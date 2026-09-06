//! The hosted read surface — "any token on demand", flow-explorer style.
//!
//! `GET /api/token/<policy_hex>.<asset_name_hex>` either returns where the
//! artifacts already live on R2, or enqueues the pipeline that makes them:
//! **sieve walk → export → push**. The client polls the same URL and watches
//! the state advance; measured end-to-end a 2-year token is under two
//! minutes cold, and a re-request is an incremental resume measured in
//! seconds. Follows wallet-sieve's `serve` discipline: axum on loopback,
//! tokio confined to this module, ONE sync worker so walks never contend
//! for the snapshot's disk bandwidth with each other.
//!
//! Registered tokens are recognised by unit and reuse their curated entry
//! (floor slot, decimals, nickname-keyed db). Unknown units get a synthetic
//! entry and a genesis-floor sieve walk — the gate makes even that
//! tolerable, and the ledger db it leaves behind makes the next refresh
//! incremental.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};

use anyhow::{Context, Result};
use axum::Json;
use axum::extract::{Path as AxPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use serde::Serialize;
use tower_http::cors::{Any, CorsLayer};

use crate::store::Ledger;
use crate::{export, registry, walk};

#[derive(clap::Args, Clone)]
pub struct ServeArgs {
    /// Data dir holding the immutable DB (expects `<data-dir>/immutable`).
    #[arg(long)]
    pub data_dir: PathBuf,

    /// Token registry TOML.
    #[arg(long, default_value = "tokens.toml")]
    pub tokens: PathBuf,

    /// Where per-token ledger dbs live. Registered tokens use
    /// `<name>.db`, unknown units `<unit>.db`.
    #[arg(long)]
    pub db_dir: PathBuf,

    /// Where exports are written before pushing.
    #[arg(long)]
    pub export_dir: PathBuf,

    /// Root of the POLICY archives the reverse passes write and the
    /// `/policy/*` routes read (`<archive-dir>/<policy_hex>/manifest.json`).
    /// Parquet and a manifest, no database — see `archive.rs`.
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,

    /// `push-artifacts.sh`. Omit to skip pushing (artifacts stay local).
    #[arg(long)]
    pub push_script: Option<PathBuf>,

    /// Publish every landed policy archive — Parquet and manifest to R2,
    /// the bundle to Workers KV — from the environment the unit loads
    /// (`R2_*`, `CF_KV_TOKEN`, `KV_NAMESPACE_IDS`). In process, with a
    /// per-step record; see `publish.rs`. Omit to keep archives local.
    #[arg(long)]
    pub publish_archive: bool,

    /// A tx-index over the same snapshot (`base.idx` + `segments/`). With
    /// it, a SEEK — the window a reader asked for — resolves its inputs on
    /// the spot; without it those rows arrive as arrivals and the descent
    /// corrects them later.
    #[arg(long)]
    pub tx_index_dir: Option<PathBuf>,

    /// Walk workers: how many policies descend at once. Measured on
    /// cardano-infra: eight concurrent 60-day walks cost what one costs
    /// (the sieve gate is CPU-bound, twelve cores); four is the co-tenant's
    /// number. See `docs/design/POLICY_WALK_SCHEDULER.md`.
    #[arg(long, default_value_t = 4)]
    pub walk_workers: usize,

    /// Seek workers: bounded windows readers asked for, seconds each,
    /// mostly idle.
    #[arg(long, default_value_t = 20)]
    pub seek_workers: usize,

    /// Public base clients should fetch artifacts from.
    #[arg(long, default_value = "https://tokendata.hodlcroft.com")]
    pub public_base: String,

    #[arg(long, default_value = "127.0.0.1:8185")]
    pub listen: String,

    /// The volatile tail: one chainsync follower for the box, keeping the
    /// stretch above the immutable tip that no snapshot can hold. See
    /// `crate::tip`.
    #[command(flatten)]
    pub tip: crate::tip::TipArgs,
}

/// One unit's place in the pipeline. Serialized as the poll response body.
#[derive(Clone, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum JobState {
    Queued,
    Walking,
    Exporting,
    Pushing,
    Ready {
        unit: String,
        slot: u64,
        /// `<public_base>/<unit>/<slot>` — append `/spine.bin.gz` etc.
        base: String,
    },
    Failed {
        error: String,
    },
}

struct Hub {
    args: ServeArgs,
    jobs: Mutex<HashMap<String, JobState>>,
    queue: mpsc::Sender<String>,
    /// `TOKEN_LEDGER_SERVE_TOKEN` — same posture as wallet-sieve: data
    /// routes Bearer-gated (the fronting worker holds the token and does
    /// user-level gating), `/health` open. Unset = open, for local dev.
    bearer: Option<String>,
}

pub fn run(args: ServeArgs) -> Result<()> {
    std::fs::create_dir_all(&args.db_dir)?;
    std::fs::create_dir_all(&args.export_dir)?;
    std::fs::create_dir_all(&args.archive_dir)?;

    let (tx, rx) = mpsc::channel::<String>();
    let bearer = std::env::var("TOKEN_LEDGER_SERVE_TOKEN").ok();
    if bearer.is_none() {
        tracing::warn!("serve: TOKEN_LEDGER_SERVE_TOKEN unset — data routes are OPEN");
    }
    let hub = Arc::new(Hub {
        args: args.clone(),
        jobs: Mutex::new(HashMap::new()),
        queue: tx,
        bearer,
    });

    // ONE worker: walks are disk-bound against a shared snapshot, and two of
    // them interleaving seeks would slow both. Queued requests state as much.
    {
        let hub = Arc::clone(&hub);
        std::thread::spawn(move || {
            for unit in rx {
                let outcome = build(&hub, &unit);
                let mut jobs = hub.jobs.lock().expect("jobs");
                match outcome {
                    Ok(state) => jobs.insert(unit, state),
                    Err(e) => {
                        tracing::error!(unit, error = %format!("{e:#}"), "serve: job failed");
                        jobs.insert(
                            unit,
                            JobState::Failed {
                                error: format!("{e:#}"),
                            },
                        )
                    }
                };
            }
        });
    }

    // The POLICY surface, mounted alongside rather than replacing the artifact
    // path above. They answer different questions: that one builds an immutable
    // snapshot for a finished token, this one serves a live, correcting feed.
    // token-explorer depends on the first and is untouched.
    let policy_hub = crate::policy_api::PolicyHub::new(
        args.data_dir.clone(),
        args.tokens.clone(),
        args.archive_dir.clone(),
        args.publish_archive.then(crate::publish::Targets::from_env),
        args.tx_index_dir.clone(),
        crate::scheduler::Pool {
            walk_workers: args.walk_workers.max(1),
            seek_workers: args.seek_workers.max(1),
        },
        std::env::var("TOKEN_LEDGER_SERVE_TOKEN").ok(),
    );

    // THE VOLATILE TAIL, from the box's chain-tail spool. No-op without
    // `--tail-db`; token-ledger follows nothing itself.
    crate::tip::spawn(
        Arc::clone(&policy_hub),
        args.tip.clone(),
        args.publish_archive.then(crate::publish::Targets::from_env),
    );

    let listen = args.listen.clone();
    // Each half resolves its OWN state before the merge — the two hubs are
    // different types, so they cannot share one `with_state`. CORS goes on
    // last, over both.
    let app = axum::Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/api/token/{unit}", get(token))
        .with_state(hub)
        .merge(crate::policy_api::router(policy_hub))
        // Permissive CORS: the consumers are wasm frontends on other
        // origins, and everything here is public chain data behind the
        // bearer gate.
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    tracing::info!(%listen, "serve: listening");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let listener = tokio::net::TcpListener::bind(&listen).await?;
            axum::serve(listener, app).await?;
            Ok(())
        })
}

async fn token(
    State(hub): State<Arc<Hub>>,
    headers: HeaderMap,
    AxPath(unit): AxPath<String>,
) -> (StatusCode, Json<JobState>) {
    if let Some(expected) = &hub.bearer {
        let ok = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| t == expected);
        if !ok {
            return (
                StatusCode::UNAUTHORIZED,
                Json(JobState::Failed {
                    error: "unauthorized".to_string(),
                }),
            );
        }
    }
    let unit = unit.to_lowercase();
    let mut jobs = hub.jobs.lock().expect("jobs");
    if let Some(state) = jobs.get(&unit) {
        return (StatusCode::OK, Json(state.clone()));
    }
    // Cheap validation before queueing anything.
    if registry::load_or_unit(&hub.args.tokens, &unit).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(JobState::Failed {
                error: format!("`{unit}` is not a valid <policy_hex>.<asset_name_hex> unit"),
            }),
        );
    }
    jobs.insert(unit.clone(), JobState::Queued);
    let _ = hub.queue.send(unit);
    (StatusCode::OK, Json(JobState::Queued))
}

fn set(hub: &Hub, unit: &str, state: JobState) {
    hub.jobs
        .lock()
        .expect("jobs")
        .insert(unit.to_string(), state);
}

/// The pipeline body: sieve walk (resuming any existing ledger) → export →
/// push. Returns the Ready state on success.
fn build(hub: &Hub, unit: &str) -> Result<JobState> {
    let a = &hub.args;
    // A registered token keeps its curated entry and nickname-keyed db.
    let token = match registry::find_by_unit(&a.tokens, unit)? {
        Some(entry) => entry,
        None => registry::load_or_unit(&a.tokens, unit)?,
    };
    let db = a.db_dir.join(format!("{}.db", token.name));

    set(hub, unit, JobState::Walking);
    walk::run(walk::WalkArgs {
        data_dir: a.data_dir.clone(),
        tokens: a.tokens.clone(),
        token: token.name.clone(),
        db: Some(db.clone()),
        from_slot: None,
        fresh: false,
        buffer_every: 20_000,
        max_blocks: None,
        sieve: true,
    })
    .context("sieve walk")?;

    set(hub, unit, JobState::Exporting);
    export::run(export::ExportArgs {
        db: db.clone(),
        tokens: a.tokens.clone(),
        token: token.name.clone(),
        out_dir: a.export_dir.clone(),
        stride: 1024,
        curve_eps_bps: 10,
    })
    .context("export")?;

    if let Some(script) = &a.push_script {
        set(hub, unit, JobState::Pushing);
        let out = std::process::Command::new(script)
            .arg(&token.name)
            .arg(&a.export_dir)
            // The script derives unit + slot from the ledger itself; point it
            // at OUR db rather than its default location.
            .env("TOKEN_LEDGER_DB", &db)
            .output()
            .context("running push script")?;
        if !out.status.success() {
            anyhow::bail!(
                "push failed: {}",
                String::from_utf8_lossy(&out.stderr)
                    .lines()
                    .last()
                    .unwrap_or("?")
            );
        }
    }

    // The walk's high-water slot IS the artifact version — read it from the
    // ledger's own cursor, the same row the push script keys R2 by.
    let slot = Ledger::open(&db)?
        .resume_slot()?
        .context("walked ledger has no cursor")?;
    Ok(JobState::Ready {
        unit: token.unit(),
        slot,
        base: format!("{}/{}/{}", a.public_base, token.unit(), slot),
    })
}
