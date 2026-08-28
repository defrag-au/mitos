//! `serve` — the hosted read surface over the sieve (the market-ledger serve
//! pattern: axum, loopback listen, CF tunnel in front, bearer-gated data
//! routes, `/health` open, tokio confined to [`run`]).
//!
//! Surface:
//! - `GET  /health` — open.
//! - `GET  /flows/{target}` — cached rows newest-first (`?limit`,
//!   `?before_slot` pagination) + wallet meta + any job state.
//! - `POST /flows/{target}/refresh` — start (or join) an excavation.
//! - `GET  /flows/{target}/events` — SSE job progress until terminal.
//!
//! The intended consumer is a CF Worker holding the bearer token and doing
//! user-level gating (wallet signature + holding check) on its own side —
//! this service never sees end users.

mod auth;
mod db;
mod handlers;
mod jobs;

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use axum::Router;
use axum::http::{Method, header};
use axum::routing::{get, post};
use tower_http::cors::{Any, CorsLayer};

#[derive(clap::Args, Debug)]
pub struct ServeArgs {
    /// Listen address. Keep it loopback; external access goes through a CF
    /// tunnel (market-ledger.defrag.cc pattern).
    #[arg(long, env = "WALLET_SIEVE_LISTEN", default_value = "127.0.0.1:8184")]
    listen: String,

    /// Cache sqlite path (created if missing).
    #[arg(long, default_value = "sieve-cache.db")]
    db: PathBuf,

    /// Immutable DB dir the excavations scan.
    #[arg(long)]
    immutable: PathBuf,

    /// Threads per excavation (one excavation runs at a time).
    #[arg(long, default_value_t = 10)]
    scan_threads: usize,

    /// Chain-tail spool sqlite (raw block CBOR beyond the last complete
    /// chunk, maintained by the built-in follower).
    #[arg(long, default_value = "tail.db")]
    tail_db: PathBuf,

    /// N2N peer for the tail follower.
    #[arg(
        long,
        env = "WALLET_SIEVE_PEER",
        default_value = "backbone.cardano.iog.io:3001"
    )]
    peer: String,

    /// Network magic (mainnet).
    #[arg(long, default_value_t = 764_824_073)]
    magic: u64,

    /// market-ledger sqlite (read-only) for venue/sale labels. A path that
    /// doesn't exist simply disables the enrichment.
    #[arg(long, default_value = "/opt/market-ledger/market-ledger.db")]
    market_db: PathBuf,

    /// Days of history a COLD excavation publishes before backfilling the
    /// rest. The chain is heavily front-loaded — a year is ~13 GB against
    /// 219 GB total — so this is the difference between rows in seconds and
    /// rows in a minute. `0` disables the split and sweeps everything once.
    #[arg(long, default_value_t = 365)]
    initial_window_days: u64,

    /// Longest window still served by the SHALLOW lane.
    ///
    /// Scans run in two lanes so a newcomer wanting 30 days never queues
    /// behind somebody's full-chain backfill. A sweep is chain-size-bound —
    /// ~3 GB for 90 days against 219 GB for everything — so the deep lane can
    /// hold a batch for minutes while the shallow one turns requests around in
    /// seconds. Both lanes still coalesce, so concurrency costs sweeps, not
    /// users.
    ///
    /// `0` puts every request in one lane (the old behaviour).
    #[arg(long, default_value_t = 90)]
    shallow_max_days: u64,
}

#[derive(Clone)]
pub struct AppState {
    pub db_path: PathBuf,
    pub registry: jobs::Registry,
    pub started: Instant,
}

fn router(state: AppState, token: auth::AuthToken) -> Router {
    let gated = Router::new()
        .route("/flows/{target}", get(handlers::flows))
        .route("/flows/{target}/refresh", post(handlers::refresh))
        .route("/flows/{target}/events", get(handlers::events))
        .layer(axum::middleware::from_fn_with_state(
            token,
            auth::require_auth,
        ));
    Router::new()
        .route("/health", get(handlers::health))
        .merge(gated)
        .layer(
            // Wildcard origin is fine: bearer-in-header, no cookies.
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]),
        )
        .with_state(state)
}

pub fn run(args: ServeArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let token = auth::AuthToken::from_env();
    let registry = jobs::Registry::start(jobs::Config {
        db_path: args.db.clone(),
        immutable: args.immutable.clone(),
        tail_db: args.tail_db.clone(),
        market_db: args.market_db.clone(),
        // Slots are seconds on Shelley, so a day is 86,400 of them.
        initial_window_slots: (args.initial_window_days > 0)
            .then(|| args.initial_window_days * 86_400),
        threads: args.scan_threads,
        shallow_max_days: args.shallow_max_days,
    })?;
    crate::tail::spawn(
        args.immutable.clone(),
        args.tail_db.clone(),
        args.peer.clone(),
        args.magic,
    )?;
    // Batch refresh: when the chunk store advances (Mithril refresh landed),
    // queue every cached wallet — the worker drains them into ONE sweep of
    // the new chunks, advancing every cursor together.
    {
        let registry = registry.clone();
        let db_path = args.db.clone();
        let tail_db = args.tail_db.clone();
        let immutable = args.immutable.clone();
        std::thread::Builder::new()
            .name("refresh-watcher".into())
            .spawn(move || {
                // Coverage = max(chunk end, spool tip). Whenever it advances
                // meaningfully — a Mithril refresh OR just the spool riding
                // the tip — one batch sweep updates every cached wallet.
                const MIN_ADVANCE_SLOTS: u64 = 600;
                let mut last_cover: Option<u64> = None;
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(600));
                    let chunk_end = crate::scan::list_chunks(&immutable, 0)
                        .ok()
                        .and_then(|c| c.last().copied())
                        .map(|n| (n + 1) * mitos_chain_walk::mithril::CHUNK_SLOTS - 1);
                    let spool_tip = crate::tail::open_ro(&tail_db).ok().and_then(|c| {
                        c.query_row("SELECT MAX(slot) FROM tail_blocks", [], |r| {
                            r.get::<_, Option<u64>>(0)
                        })
                        .ok()
                        .flatten()
                    });
                    let Some(cover) = chunk_end.max(spool_tip) else {
                        continue;
                    };
                    match last_cover {
                        None => last_cover = Some(cover),
                        Some(prev) if cover >= prev + MIN_ADVANCE_SLOTS => {
                            last_cover = Some(cover);
                            let wallets = db::open_ro(&db_path)
                                .and_then(|c| db::list_wallets(&c))
                                .unwrap_or_default();
                            tracing::info!(
                                cover,
                                wallets = wallets.len(),
                                "coverage advanced — queueing batch refresh"
                            );
                            for (canonical, display) in wallets {
                                let _ = registry.enqueue(&display, &canonical);
                            }
                        }
                        _ => {}
                    }
                }
            })
            .context("spawning refresh watcher")?;
    }

    let state = AppState {
        db_path: args.db.clone(),
        registry,
        started: Instant::now(),
    };
    let rt = tokio::runtime::Runtime::new().context("building tokio runtime")?;
    rt.block_on(async move {
        let app = router(state, token);
        let listener = tokio::net::TcpListener::bind(&args.listen)
            .await
            .with_context(|| format!("binding {}", args.listen))?;
        tracing::info!(
            listen = %args.listen,
            db = %args.db.display(),
            immutable = %args.immutable.display(),
            "wallet-sieve serve up"
        );
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("shutting down");
            })
            .await
            .context("server error")
    })
}
