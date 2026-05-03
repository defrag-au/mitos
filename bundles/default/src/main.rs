//! Default bundle: Dolos data plane + JpgCoIndexer.
//!
//! Phase 1+: composition logic now lives in `mitos_core::Bundle`. This
//! file just loads config, constructs the domain, instantiates each
//! indexer the bundle wants to include, and hands off to `Bundle::run`.

use clap::Parser;
use collection_ownership_indexer::OwnershipIndexer;
use jpg_co_indexer::JpgCoIndexer;
use marketplace_indexer::MarketplaceIndexer;
use mitos_core::Bundle;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    listen: std::net::SocketAddr,

    /// Where mitos stores its own state — the subscription registry
    /// today, per-indexer materialized views in the future.
    /// Independent of Dolos's data dir.
    #[arg(long, env = "BUNDLE_DATA_DIR", default_value = "./mitos-data")]
    data_dir: std::path::PathBuf,

    /// Print the loaded configuration + persisted subscriptions to
    /// stdout, then exit 0 without starting the chain follower or
    /// HTTP server. Useful for verifying env vars, paths, and the
    /// state of the redb registry before committing to a long-
    /// running process.
    #[arg(long)]
    print_config_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    info!(config = %args.config, listen = %args.listen, "mitos starting");

    let config = mitos_core::load_config(&args.config)?;

    // Early exit BEFORE opening data stores (which acquires the
    // WAL lock). Lets `--print-config-only` run safely against a
    // dir that's actively being used by another mitos instance.
    if args.print_config_only {
        mitos_core::print_config_summary(
            &config,
            args.listen,
            &args.data_dir,
            &["jpg-co", "collection-ownership"],
        )?;
        return Ok(());
    }

    let domain = mitos_core::setup_domain(&config)?;
    info!("domain initialized");

    let exit = install_exit_handler();

    let mut bundle = Bundle::new(domain, config, args.listen, args.data_dir);
    bundle.add_indexer(JpgCoIndexer::new()?);
    bundle.add_indexer(OwnershipIndexer::new()?);
    bundle.add_indexer(MarketplaceIndexer::new()?);

    bundle.run(exit).await?;

    info!("mitos shutting down");
    Ok(())
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
