//! Default bundle: Dolos data plane + JpgCoIndexer.
//!
//! Phase 0 status: composes the trait + dispatcher + indexer modules,
//! but does NOT yet wire up `setup_domain` (that's phase-1 work — see
//! `docs/design/ROADMAP.md`). This main is structured so phase-1 only
//! needs to fill in `setup_domain` and the surrounding tokio orchestration.

use std::sync::Arc;

use clap::Parser;
use mitos_core::Indexer;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(version, about = "mitos default bundle")]
struct Args {
    /// Path to the Dolos config file (same schema as stock dolos.toml).
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

    // ---- TODO(phase-1): set up the Dolos domain ----
    //
    // Replicate the wiring from
    //   ~/code/github/dolos/src/bin/dolos/common.rs::setup_domain
    // verbatim into this bundle. Roughly:
    //
    //   1. Load `RootConfig` from `args.config`.
    //   2. `open_data_stores(&config)` → wal/state/archive/indexes/mempool
    //   3. `open_genesis_files(&config.genesis)` → Arc<Genesis>
    //   4. Initialize `dolos_cardano::CardanoLogic` from the genesis +
    //      state + the chain config.
    //   5. Construct a `dolos::adapters::DomainAdapter { ... }` with all
    //      stores + chain logic + tip broadcast channel.
    //   6. Call `domain.bootstrap()` to put it in a valid state.
    //
    // Once that's done, the rest of this main should compose cleanly.
    //
    // After setup_domain works:
    //   - Spawn `dolos::sync` stage as a tokio task to drive chain sync
    //   - For each indexer, call bootstrap() then spawn a dispatcher
    //   - Mount their routes under axum
    //
    // None of that exists yet — the code below is the SHAPE we want;
    // it does not currently compile end-to-end without the domain.

    warn!(
        "phase-0 stub: setup_domain not yet implemented; \
         bundle exits without starting chain sync or HTTP server"
    );

    // -- shape of phase-1+ orchestration (kept here as a compile-checked
    //    placeholder once we plug in `setup_domain`) --
    if false {
        let domain = setup_domain_placeholder().await?;
        let indexers = build_indexers(&domain).await?;
        let app = compose_routes(&indexers);
        run(domain, indexers, app, &args.listen).await?;
    }

    Ok(())
}

/// Stand-in for the real `setup_domain`. Phase-1 will replace this with
/// the actual Dolos wiring.
async fn setup_domain_placeholder() -> anyhow::Result<()> {
    anyhow::bail!("setup_domain not yet implemented (phase-1 work)")
}

/// Construct each indexer this bundle includes, run its `bootstrap`,
/// and return them ready for dispatcher attachment.
///
/// Once `setup_domain_placeholder` returns a real `DomainAdapter`,
/// this function's signature becomes
///   `async fn build_indexers(domain: &DomainAdapter)
///        -> anyhow::Result<Vec<Arc<Mutex<dyn Indexer<DomainAdapter>>>>>`
/// and we can call `bootstrap` + `spawn_dispatcher` on each entry.
async fn build_indexers(_domain: &()) -> anyhow::Result<Vec<()>> {
    // Phase-1 sketch:
    //
    // let mut indexers: Vec<Arc<Mutex<dyn Indexer<D>>>> = vec![
    //     Arc::new(Mutex::new(jpg_co_indexer::JpgCoIndexer::new()?)),
    //     // additional indexers here as the bundle grows
    // ];
    // for ix in &indexers {
    //     let from = ix.lock().await.bootstrap(domain).await?;
    //     let sub = domain.watch_tip(Some(from))?;
    //     tokio::spawn(mitos_core::run_dispatcher(
    //         ix.clone(), domain.clone(), sub,
    //     ));
    // }
    Ok(vec![])
}

/// Merge each indexer's routes into a single axum router, conventionally
/// nested under `/<name>/`.
fn compose_routes(_indexers: &[()]) -> axum::Router {
    // Phase-1 sketch:
    //
    // indexers.iter().fold(axum::Router::new(), |router, ix| {
    //     let ix = futures::executor::block_on(ix.lock());
    //     router.nest(&format!("/{}", ix.name()), ix.routes())
    // })
    axum::Router::new()
}

async fn run(
    _domain: (),
    _indexers: Vec<()>,
    app: axum::Router,
    listen: &str,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!(addr = %listen, "HTTP server listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().compact())
        .init();
}

// Suppress unused-import warning until phase-1 replaces the placeholder
// orchestration with real calls.
#[allow(dead_code)]
fn _silence_unused_imports() {
    let _ = std::any::type_name::<Arc<Mutex<dyn Indexer<dolos::adapters::DomainAdapter>>>>();
}
