//! Bundle: owns the domain, registers indexers, runs the framework.
//!
//! Replaces the hand-rolled composition that lived inline in
//! `bundles/default/src/main.rs` during Phase 1. Bundle authors now
//! write:
//!
//! ```ignore
//! let mut bundle = Bundle::new(domain, config, listen_addr);
//! bundle.add_indexer(JpgCoIndexer::new()?);
//! bundle.add_indexer(OwnershipIndexer::new()?);
//! bundle.run(exit).await?;
//! ```
//!
//! The generic `add_indexer<I: Indexer<DomainAdapter>>` keeps each
//! indexer's associated `Scope`/`Change` types at the registration
//! boundary; internally `IndexerAdapter<I>` wraps it into the
//! object-safe `IndexerHandle` so the bundle can store a
//! heterogeneous collection. See `docs/design/CF_REPLICATION.md` for
//! why the trait is non-object-safe.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, get};
use dolos::adapters::DomainAdapter;
use dolos_core::Domain;
use dolos_core::config::RootConfig;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::auth::AuthToken;
use crate::handle::{IndexerAdapter, IndexerHandle};
use crate::indexer::Indexer;
use crate::replicate::replicate_router;
use crate::replicator::{ConnState, Replicator, Subscription, SubscriptionId};
use crate::{run_dispatcher, spawn_sync_pipeline};

pub struct Bundle {
    domain: DomainAdapter,
    config: RootConfig,
    listen: SocketAddr,
    data_dir: PathBuf,
    indexers: Vec<Arc<dyn IndexerHandle>>,
    /// Filesystem path for wasm-module artifacts. `None` = wasm
    /// hosting disabled; the bundle runs as a pure
    /// statically-composed indexer host (today's behaviour).
    /// `Some(path)` = enable `/_admin/modules/*` + auto-resume
    /// any modules already activated under that path.
    modules_dir: Option<PathBuf>,
}

impl Bundle {
    /// Construct a bundle.
    ///
    /// `data_dir` is where mitos stores its own state (subscription
    /// registry, future per-indexer materialized views). Independent
    /// of the Dolos data dir referenced by `config` — Dolos's data
    /// dir is under its own ownership, mitos doesn't write there.
    pub fn new(
        domain: DomainAdapter,
        config: RootConfig,
        listen: SocketAddr,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            domain,
            config,
            listen,
            data_dir,
            indexers: Vec::new(),
            modules_dir: None,
        }
    }

    /// Register an indexer with the bundle. The indexer's `Scope`
    /// and `Change` types are erased into CBOR-bytes-at-the-boundary
    /// inside the adapter; bundle code never sees the erased form.
    pub fn add_indexer<I>(&mut self, indexer: I)
    where
        I: Indexer<DomainAdapter> + 'static,
    {
        let adapter = IndexerAdapter::<I>::new(indexer);
        self.indexers.push(Arc::new(adapter));
    }

    /// Enable wasm-module hosting. After this is called:
    /// - `/_admin/modules/*` routes are mounted
    /// - any modules already registered under `modules_dir`
    ///   get their followers auto-started on `run` (resume)
    /// - successful uploads via `POST /_admin/modules/{id}`
    ///   start (or replace) a running follower for that module
    ///
    /// Per `MITOS_PLATFORM_DEPLOYMENT.md`: the modules dir is
    /// the canonical artifact storage location, e.g.
    /// `/var/lib/mitos/modules/`. Layout is one subdir per
    /// module id: `<id>/current.wasm`, `<id>/manifest.toml`,
    /// `<id>/cursor.cbor`, `<id>/<sha>.wasm` (rollback target).
    pub fn enable_modules(&mut self, modules_dir: PathBuf) {
        self.modules_dir = Some(modules_dir);
    }

    /// Run the bundle: spawn the chain-sync pipeline, bootstrap each
    /// indexer, start its dispatcher, mount HTTP routes (per-indexer +
    /// `/replicate/{indexer}` test surface + `/_admin/subscriptions`),
    /// and serve until `exit` is cancelled.
    pub async fn run(self, exit: CancellationToken) -> anyhow::Result<()> {
        let Bundle {
            domain,
            config,
            listen,
            data_dir,
            indexers,
            modules_dir,
        } = self;

        let sync_handle = spawn_sync_pipeline(domain.clone(), &config, exit.clone())?;
        info!("chain-sync pipeline spawned");

        let started_at = SystemTime::now();
        let indexer_names: Vec<String> = indexers.iter().map(|h| h.name().to_string()).collect();

        let mut dispatcher_handles: Vec<JoinHandle<()>> = Vec::with_capacity(indexers.len());
        let mut app = axum::Router::new();

        for ix in &indexers {
            let name = ix.name();

            let from = ix.bootstrap(&domain).await?;
            info!(indexer = %name, ?from, "indexer bootstrapped");

            let subscription = domain
                .watch_tip(Some(from.clone()))
                .map_err(|e| anyhow::anyhow!("watch_tip for {name}: {e:?}"))?;

            let ix_clone = ix.clone();
            let domain_clone = domain.clone();
            let handle = tokio::spawn(async move {
                run_dispatcher(ix_clone, domain_clone, subscription).await;
            });
            dispatcher_handles.push(handle);

            app = app.nest(&format!("/{name}"), ix.routes());
        }

        let auth = AuthToken::from_env();

        // Mount CF replication test surface (server-accepted WS) and
        // admin endpoints for managing outbound `Replicator`
        // subscriptions (production WS-client direction). Both gated
        // by Bearer-token auth (see auth.rs).
        app = app.merge(replicate_router(&indexers, domain.clone(), auth.clone()));

        let replicator_path = data_dir.join("subscriptions.redb");
        let replicator = Arc::new(
            Replicator::new(&indexers, domain.clone(), &replicator_path, auth.clone()).await?,
        );
        info!(path = %replicator_path.display(), "replicator registry opened");
        app = app.merge(admin_router(replicator.clone(), auth.clone()));

        // Wasm-module hosting (Bundle::enable_modules). Mounts
        // /_admin/modules/* and auto-starts followers for any
        // modules already registered on disk. The host handle
        // is captured outside the scope so the shutdown path
        // can call `stop_all` — without that, follower tasks
        // keep tokio's runtime alive past the bundle's serve
        // future and systemd hits its 90s graceful-shutdown
        // timeout.
        let mut module_host: Option<Arc<dyn mitos_platform::host::ModuleHostHandle>> = None;
        if let Some(modules_dir) = modules_dir.as_ref() {
            std::fs::create_dir_all(modules_dir).map_err(|e| {
                anyhow::anyhow!(
                    "creating modules dir {}: {e}",
                    modules_dir.display()
                )
            })?;
            let storage = mitos_platform::storage::ModuleStorage::new(modules_dir.clone());
            let engine = mitos_platform::registry::ModuleRegistry::build_engine()
                .map_err(|e| anyhow::anyhow!("module engine: {e}"))?;
            let dp: Arc<dyn mitos_platform::host_fns::DataPlaneFacade> = Arc::new(
                mitos_platform::host_fns::DomainDataPlane::new(domain.clone()),
            );

            // Subscription factory: each follower gets its own
            // tip-watch. Resume from persisted cursor when
            // available; on fresh deploy fall back to the
            // domain's current chain tip — NOT to None, because
            // dolos's `watch_tip(None)` triggers a full
            // WAL-replay of the entire retention window
            // (~10k+ historical slots), which is wrong default
            // for wasm-module followers (backfill should happen
            // via `read_utxos` data-plane queries, not chain
            // replay).
            let domain_for_factory = domain.clone();
            let sub_factory: mitos_platform::host::SubscriptionFactory<
                <DomainAdapter as Domain>::TipSubscription,
            > = Arc::new(move |cursor: Option<dolos_core::ChainPoint>| {
                use dolos_core::StateStore;
                let from = cursor.or_else(|| {
                    domain_for_factory
                        .state()
                        .read_cursor()
                        .ok()
                        .flatten()
                });
                domain_for_factory
                    .watch_tip(from)
                    .expect("watch_tip subscribe failed")
            });

            // Per-module redb-backed KV — crash-safe across
            // restarts. Each module gets its own `kv.redb`
            // alongside `cursor.redb` + `current.wasm`.
            let storage_for_kv = storage.clone();
            let kv_factory: mitos_platform::host::KvFactory = Arc::new(move |id: &str| {
                let path = storage_for_kv.kv_path(id);
                match mitos_platform::host_fns::state_kv::ModuleKv::open_redb(&path, None) {
                    Ok(kv) => kv,
                    Err(e) => {
                        // Fall back to in-memory rather than refusing
                        // to start. The error logs loudly; cursor
                        // resume still works (cursor lives in its own
                        // file), so the operator can investigate
                        // without taking the host down. In practice
                        // open() failure here means a permissions or
                        // disk-full issue that affects the whole
                        // modules dir.
                        error!(
                            module = %id,
                            path = %path.display(),
                            error = %e,
                            "redb KV open failed; falling back to in-memory"
                        );
                        mitos_platform::host_fns::state_kv::ModuleKv::new_in_memory()
                    }
                }
            });
            let emitter_factory: mitos_platform::host::EmitterFactory =
                Arc::new(mitos_platform::host_fns::emit::EventSink::new);

            let host = Arc::new(mitos_platform::host::ModuleHost::new(
                storage.clone(),
                engine,
                dp,
                sub_factory,
                kv_factory,
                emitter_factory,
                mitos_platform::registry::ResourceBudget::default(),
            ));

            // Auto-resume: scan modules dir, start a follower
            // for each registered module. Failures here are
            // logged but don't abort bundle startup — a single
            // bad module shouldn't take the host down.
            for module_id in storage.list_modules().unwrap_or_default() {
                match host.start(&module_id).await {
                    Ok(()) => info!(module = %module_id, "auto-resumed wasm module"),
                    Err(e) => {
                        error!(module = %module_id, error = %e, "auto-resume failed; module not running");
                    }
                }
            }

            // The platform admin router has its own AuthToken
            // type — same shape, same env var, separate crate.
            let platform_auth = mitos_platform::admin::AuthToken::from_env();
            let host_for_admin = host.clone();
            app = app.merge(mitos_platform::admin::admin_router_with_host(
                storage,
                host_for_admin,
                platform_auth,
            ));
            module_host = Some(host);
            info!(
                modules_dir = %modules_dir.display(),
                "wasm-module hosting enabled"
            );
        }

        // /health surfaces replicator state — open (no auth) so a
        // status page or LB health check can hit it without
        // credentials. No sensitive data is exposed.
        let health_state = HealthState {
            started_at,
            indexers: indexer_names,
            replicator: replicator.clone(),
        };
        app = app.route("/health", get(handle_health).with_state(health_state));

        let listener = tokio::net::TcpListener::bind(listen).await?;
        info!(addr = %listen, "HTTP server listening");

        let serve = axum::serve(listener, app)
            .with_graceful_shutdown(async move { exit.cancelled().await });

        tokio::select! {
            result = serve => {
                if let Err(e) = result {
                    error!(error = %e, "HTTP server exited with error");
                }
            }
            _ = sync_handle => {
                info!("sync pipeline exited");
            }
        }

        for h in dispatcher_handles {
            h.abort();
        }

        // Stop wasm-module followers cleanly so cursor checkpoints
        // get their final write before tokio's runtime shuts down.
        // Each `stop` cancels its slot's CancellationToken and awaits
        // task termination; loop bounded by however many slots are
        // running.
        if let Some(host) = module_host {
            tracing::info!("stopping wasm-module followers");
            host.stop_all().await;
        }

        Ok(())
    }
}

/// Print a summary of the loaded configuration to stdout without
/// opening the Dolos data stores or spawning anything. Used by
/// `mitos --print-config-only` to validate startup state offline
/// (env vars, paths, persisted subscriptions) — crucially, this
/// does NOT acquire the WAL lock, so it's safe to run while
/// another mitos instance is live against the same data dir.
pub fn print_config_summary(
    config: &RootConfig,
    listen: SocketAddr,
    data_dir: &std::path::Path,
    indexer_names: &[&'static str],
) -> anyhow::Result<()> {
    println!("# mitos config summary");
    println!();
    println!("listen:        {listen}");
    println!("data_dir:      {}", data_dir.display());
    println!("storage.wal:   {:?}", config.storage.wal.path());
    println!("storage.state: {:?}", config.storage.state.path());
    println!();
    println!("indexers ({}):", indexer_names.len());
    for n in indexer_names {
        println!("  - {n}");
    }
    println!();

    let auth = AuthToken::from_env();
    println!(
        "auth:          {}",
        if auth.is_open() { "OPEN" } else { "set" }
    );
    println!();

    let replicator_path = data_dir.join("subscriptions.redb");
    let persisted = Replicator::list_persisted(&replicator_path)?;
    println!(
        "persisted subscriptions ({}, from {}):",
        persisted.len(),
        replicator_path.display()
    );
    for (id, sub) in persisted {
        println!(
            "  [{id}] indexer={} target={} cursor={:?} scope_bytes={}",
            sub.indexer,
            sub.target_url,
            sub.cursor,
            sub.scope.len()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Admin endpoints: /_admin/subscriptions
// ---------------------------------------------------------------------

#[derive(Clone)]
struct AdminState {
    replicator: Arc<Replicator>,
}

fn admin_router(replicator: Arc<Replicator>, auth: AuthToken) -> axum::Router {
    let state = AdminState { replicator };
    axum::Router::new()
        .route(
            "/_admin/subscriptions",
            get(list_subscriptions).post(add_subscription),
        )
        .route("/_admin/subscriptions/{id}", delete(remove_subscription))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::require_auth,
        ))
}

#[derive(Serialize)]
struct SubscriptionEntry {
    id: SubscriptionId,
    sub: Subscription,
    state: ConnState,
}

async fn list_subscriptions(State(state): State<AdminState>) -> Json<Vec<SubscriptionEntry>> {
    let entries = state
        .replicator
        .list()
        .await
        .into_iter()
        .map(|(id, sub, state)| SubscriptionEntry { id, sub, state })
        .collect();
    Json(entries)
}

/// Friendly admin-API shape. The framework looks up the indexer by
/// name to convert `scope` (JSON) → typed `Scope` → CBOR, and
/// parses `cursor` from a friendly string. Avoids forcing admin
/// clients to know the indexer's CBOR encoding or the dolos-core
/// `ChainPoint` JSON shape.
///
/// Examples:
/// ```json
/// {
///   "indexer": "collection-ownership",
///   "target_url": "wss://collections-mitos.<acct>.workers.dev/_internal/replicate?policy_id=abc...",
///   "scope": {"policy_id": "abc..."},
///   "cursor": "origin"
/// }
/// ```
///
/// `cursor` accepts:
/// - `"origin"` — start from chain origin
/// - `"<slot>"` — start from a slot (no hash)
/// - `"<slot>:<hash_hex>"` — specific block at a slot
#[derive(Deserialize)]
struct AddSubscription {
    indexer: String,
    target_url: String,
    scope: serde_json::Value,
    cursor: String,
}

#[derive(Serialize)]
struct AddedResponse {
    id: SubscriptionId,
}

async fn add_subscription(
    State(state): State<AdminState>,
    Json(body): Json<AddSubscription>,
) -> Result<Json<AddedResponse>, (StatusCode, String)> {
    // Look up the indexer so we can encode the scope using its
    // typed `Scope` shape. Returns 400 if the indexer is unknown.
    let handle = state
        .replicator
        .indexer_handle(&body.indexer)
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("unknown indexer: {}", body.indexer),
            )
        })?;

    let scope_cbor = handle
        .encode_scope_from_json(&body.scope)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let cursor = parse_friendly_cursor(&body.cursor)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let sub = Subscription {
        indexer: body.indexer,
        target_url: body.target_url,
        scope: scope_cbor,
        cursor,
    };
    state
        .replicator
        .add(sub)
        .await
        .map(|id| Json(AddedResponse { id }))
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

fn parse_friendly_cursor(s: &str) -> anyhow::Result<dolos_core::ChainPoint> {
    if s.eq_ignore_ascii_case("origin") {
        return Ok(dolos_core::ChainPoint::Origin);
    }
    if let Some((slot, hash_hex)) = s.split_once(':') {
        let slot: u64 = slot.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
        let hash_bytes = hex::decode(hash_hex).map_err(|e| anyhow::anyhow!("bad hash hex: {e}"))?;
        if hash_bytes.len() != 32 {
            anyhow::bail!("hash must be 32 bytes; got {}", hash_bytes.len());
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        return Ok(dolos_core::ChainPoint::Specific(
            slot,
            dolos_core::BlockHash::from(arr),
        ));
    }
    let slot: u64 = s.parse().map_err(|e| anyhow::anyhow!("bad slot: {e}"))?;
    Ok(dolos_core::ChainPoint::Slot(slot))
}

async fn remove_subscription(
    State(state): State<AdminState>,
    Path(id): Path<SubscriptionId>,
) -> impl IntoResponse {
    if state.replicator.remove(id).await {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[derive(Clone)]
struct HealthState {
    started_at: SystemTime,
    indexers: Vec<String>,
    replicator: Arc<Replicator>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    uptime_secs: u64,
    indexers: Vec<String>,
    replicator: crate::replicator::ReplicatorSummary,
}

async fn handle_health(State(state): State<HealthState>) -> Json<HealthResponse> {
    let uptime_secs = SystemTime::now()
        .duration_since(state.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let summary = state.replicator.summary().await;
    Json(HealthResponse {
        status: "ok",
        uptime_secs,
        indexers: state.indexers,
        replicator: summary,
    })
}
