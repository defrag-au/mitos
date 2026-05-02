//! Default bundle: Dolos data plane + JpgCoIndexer.
//!
//! Phase 1: setup_domain wired up via mitos-core. Bundle constructs the
//! domain from a Dolos config file, spawns the chain-sync pipeline,
//! bootstraps each indexer, and starts a per-indexer dispatcher loop.
//! HTTP server stub exposes `/health` plus each indexer's nested routes.

use std::sync::Arc;

use clap::Parser;
use jpg_co_indexer::JpgCoIndexer;
use mitos_core::{Domain, DomainAdapter, Indexer};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(version, about = "mitos default bundle")]
struct Args {
    /// Path to the Dolos config file (same schema as stock dolos.toml).
    /// The data dir referenced by this config must already be bootstrapped
    /// (run `dolos bootstrap mithril ...` first).
    #[arg(long, env = "DOLOS_CONFIG", default_value = "dolos.toml")]
    config: String,

    /// HTTP listen address for indexer routes.
    #[arg(long, env = "BUNDLE_LISTEN", default_value = "127.0.0.1:8080")]
    listen: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    info!(config = %args.config, listen = %args.listen, "mitos starting");

    // Load Dolos config + open the embedded data plane.
    let config = mitos_core::load_config(&args.config)?;
    let domain = mitos_core::setup_domain(&config)?;
    info!("domain initialized");

    // Cancellation token wires SIGINT/SIGTERM through to chain-sync teardown.
    let exit = install_exit_handler();

    // Start Dolos's chain-sync pipeline in the background. Without this
    // the WAL/state never advances and indexers receive no events.
    let sync_handle = mitos_core::spawn_sync_pipeline(domain.clone(), &config, exit.clone())?;
    info!("chain-sync pipeline spawned");

    // Build, bootstrap, and start dispatchers for each indexer in this bundle.
    let mut indexers: Vec<Arc<Mutex<dyn Indexer<DomainAdapter>>>> = vec![];
    let jpg_co = Arc::new(Mutex::new(JpgCoIndexer::new()?));
    indexers.push(jpg_co.clone());

    for ix in &indexers {
        let name = {
            let guard = ix.lock().await;
            guard.name()
        };

        let from = {
            let mut guard = ix.lock().await;
            guard.bootstrap(&domain).await?
        };
        info!(indexer = %name, ?from, "indexer bootstrapped");

        let subscription = domain
            .watch_tip(Some(from.clone()))
            .map_err(|e| anyhow::anyhow!("watch_tip for {name}: {e:?}"))?;

        let ix_clone = ix.clone();
        let domain_clone = domain.clone();
        tokio::spawn(async move {
            mitos_core::run_dispatcher(ix_clone, domain_clone, subscription).await;
        });
    }

    // HTTP routes: /health plus each indexer's nested routes.
    let mut app = axum::Router::new().route("/health", axum::routing::get(handle_health));
    for ix in &indexers {
        let guard = ix.lock().await;
        app = app.nest(&format!("/{}", guard.name()), guard.routes());
    }

    let listener = tokio::net::TcpListener::bind(&args.listen).await?;
    info!(addr = %args.listen, "HTTP server listening");

    let serve =
        axum::serve(listener, app).with_graceful_shutdown(async move { exit.cancelled().await });

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

    info!("mitos shutting down");
    Ok(())
}

async fn handle_health() -> &'static str {
    // TODO(phase-2): aggregate per-indexer cursor lag here.
    "ok"
}

fn install_exit_handler() -> CancellationToken {
    let token = CancellationToken::new();
    let token_clone = token.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::warn!("SIGINT received");
            }
            _ = wait_sigterm() => {
                tracing::warn!("SIGTERM received");
            }
        }
        token_clone.cancel();
    });
    token
}

#[cfg(unix)]
async fn wait_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    if let Ok(mut s) = signal(SignalKind::terminate()) {
        s.recv().await;
    } else {
        std::future::pending::<()>().await;
    }
}

#[cfg(not(unix))]
async fn wait_sigterm() {
    std::future::pending::<()>().await;
}

fn init_tracing() {
    use tracing_subscriber::{EnvFilter, fmt, prelude::*};
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,mitos=debug,jpg_co_indexer=debug")),
        )
        .with(fmt::layer().compact())
        .init();
}
