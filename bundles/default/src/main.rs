//! Default bundle: Dolos data plane + wasm-module hosting + the
//! none-match residual-pass indexer.
//!
//! Composition logic lives in `mitos_core::Bundle`. This file just
//! loads config, constructs the domain, registers the residual
//! indexer + wasm-module hosting, and hands off to `Bundle::run`.
//!
//! The three legacy in-tree indexers (collection-ownership,
//! marketplace, mint-burn) and the outbound `Replicator` subscription
//! model were retired once their consumers cut over to platform-v2
//! community modules. `NoneMatchIndexer` stays as the dispatcher's
//! residual-pass coordinator (see
//! `docs/design/DOMAIN_REFACTOR.md`).

use axum::middleware::from_fn_with_state;
use clap::Parser;
use mitos_core::{Bundle, require_auth};
use none_match_indexer::NoneMatchIndexer;
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

    /// Where mitos stores its own state. Independent of Dolos's
    /// data dir. Currently used by platform-v2 hosting paths (the
    /// emissions log + cursor checkpoint per wasm module live
    /// under `--modules-dir`, which is a separate path).
    #[arg(long, env = "BUNDLE_DATA_DIR", default_value = "./mitos-data")]
    data_dir: std::path::PathBuf,

    /// Wasm-module artifact storage. Setting this enables the
    /// `/_admin/modules/*` admin surface + auto-resume of any
    /// modules already registered under the path. Unset =
    /// classic statically-composed bundle (today's behaviour).
    /// See `docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md`.
    #[arg(long, env = "BUNDLE_MODULES_DIR")]
    modules_dir: Option<std::path::PathBuf>,

    /// Community-modules source tree. When set + `--modules-dir`
    /// is enabled, the bundle activates any pre-built community
    /// module under `<dir>/<name>/build/` whose sha differs from
    /// the currently-active artifact in `--modules-dir`. Typical
    /// value: `<mitos-checkout>/community-modules`. See
    /// `docs/strategy/COMMUNITY_MODULES.md`.
    #[arg(long, env = "BUNDLE_COMMUNITY_MODULES_DIR")]
    community_modules_dir: Option<std::path::PathBuf>,

    /// Print the loaded configuration to stdout, then exit 0
    /// without starting the chain follower or HTTP server.
    /// Useful for verifying env vars and paths before committing
    /// to a long-running process.
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
        mitos_core::print_config_summary(&config, args.listen, &args.data_dir, &["none-match"])?;
        return Ok(());
    }

    let domain = mitos_core::setup_domain(&config)?;
    info!("domain initialized");

    let exit = install_exit_handler();

    // Clone minibf config + domain before they're moved into Bundle::new.
    // listen_address inside MinibfConfig is vestigial here — we mount the
    // router on the bundle's existing listener, not a separate bind.
    let minibf_cfg = config.serve.minibf.clone();
    let minibf_domain = minibf_cfg.as_ref().map(|_| domain.clone());

    let mut bundle = Bundle::new(domain, config, args.listen, args.data_dir);

    // Residual pass: emits `Domain::AssetMovement` for asset
    // transfers no specific-domain indexer claimed (the chain-
    // recognition surface that used to live in
    // collection-ownership / marketplace / mint-burn indexers now
    // lives in wasm community modules; `none-match` catches the
    // residual so consumers tracking generic ownership get
    // complete coverage). Switches the dispatcher to synchronised
    // mode — see `docs/design/DOMAIN_REFACTOR.md`.
    let claim_coordinator = bundle.enable_residual_pass();
    bundle.add_indexer(NoneMatchIndexer::new(claim_coordinator));

    if let Some(modules_dir) = args.modules_dir {
        info!(modules_dir = %modules_dir.display(), "wasm-module hosting enabled");
        bundle.enable_modules(modules_dir);
        if let Some(cm_dir) = args.community_modules_dir {
            info!(
                community_modules_dir = %cm_dir.display(),
                "community-modules auto-load enabled"
            );
            bundle.enable_community_modules(cm_dir);
        }
    } else {
        info!("wasm-module hosting disabled (set --modules-dir to enable)");
        if args.community_modules_dir.is_some() {
            tracing::warn!(
                "--community-modules-dir set but --modules-dir is not; \
                 community-modules auto-load requires wasm-module hosting"
            );
        }
    }

    if let (Some(cfg), Some(minibf_domain)) = (minibf_cfg, minibf_domain) {
        let auth = bundle.auth().clone();
        let router = dolos_minibf::build_router(cfg, minibf_domain)
            .layer(from_fn_with_state(auth, require_auth));
        bundle.nest_extra("/minibf", router);
        info!("minibf bridge enabled at /minibf");
    }

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
                .unwrap_or_else(|_| EnvFilter::new("info,mitos=debug")),
        )
        .with(fmt::layer().compact())
        .init();
}
