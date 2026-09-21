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

mod cli;
mod doctor;
mod init;

use std::path::Path;

use anyhow::Context;
use axum::middleware::from_fn_with_state;
use clap::{CommandFactory, FromArgMatches};
use mitos_core::{Bundle, ConfigMode, authenticate, require_read_chain};
use none_match_indexer::NoneMatchIndexer;
use tokio_util::sync::CancellationToken;
use tracing::info;

use cli::{Args, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let matches = Args::command().get_matches();
    let mut args = Args::from_arg_matches(&matches).unwrap_or_else(|e| e.exit());
    match args.command.take() {
        Some(Command::Init(init)) => init::run(init),
        Some(Command::Doctor(doc)) => doctor::run(doc),
        None => run_node(args, matches).await,
    }
}

async fn run_node(args: Args, matches: clap::ArgMatches) -> anyhow::Result<()> {
    let overrides = cli::command_line_overrides(&matches);
    let mode = mitos_core::select_mode(
        args.config_file.as_deref(),
        Path::new(&args.config),
        mitos_core::default_mitos_toml_path(),
        |p| p.exists(),
    );

    let (config, listen, data_dir, modules_dir, community_modules_dir) = match mode {
        ConfigMode::Legacy { dolos_toml } => {
            info!(config = %dolos_toml.display(), listen = %args.listen, "mitos starting (legacy dolos.toml)");
            let config = mitos_core::load_legacy_dolos_toml(&dolos_toml)?;
            (
                config,
                args.listen,
                args.data_dir,
                args.modules_dir,
                args.community_modules_dir,
            )
        }
        ConfigMode::Mitos { path } => {
            info!(config = %path.display(), "mitos starting (mitos.toml)");
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let lookup = |k: &str| {
                overrides
                    .iter()
                    .find(|(n, _)| n == k)
                    .map(|(_, v)| v.clone())
                    .or_else(|| std::env::var(k).ok())
            };
            let loaded = mitos_core::load_mitos_toml(&text, &lookup)?;
            (
                loaded.root,
                loaded.resolved.listen,
                loaded.resolved.bundle_data_dir,
                loaded.resolved.modules_dir,
                loaded.resolved.community_modules_dir,
            )
        }
    };

    // Early exit BEFORE opening data stores (which acquires the
    // WAL lock). Lets `--print-config-only` run safely against a
    // dir that's actively being used by another mitos instance.
    if args.print_config_only {
        mitos_core::print_config_summary(&config, listen, &data_dir, &["none-match"])?;
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

    // Fallible: this is where `MITOS_NAMESPACES_FILE` is loaded, and a host
    // that declares a security boundary it cannot enforce must not serve.
    let mut bundle = Bundle::new(domain, config, listen, data_dir)?;

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

    if let Some(modules_dir) = modules_dir {
        info!(modules_dir = %modules_dir.display(), "wasm-module hosting enabled");
        bundle.enable_modules(modules_dir);
        if let Some(cm_dir) = community_modules_dir {
            info!(
                community_modules_dir = %cm_dir.display(),
                "community-modules auto-load enabled"
            );
            bundle.enable_community_modules(cm_dir);
        }
    } else {
        info!("wasm-module hosting disabled (set --modules-dir to enable)");
        if community_modules_dir.is_some() {
            tracing::warn!(
                "--community-modules-dir set but --modules-dir is not; \
                 community-modules auto-load requires wasm-module hosting"
            );
        }
    }

    if let (Some(cfg), Some(minibf_domain)) = (minibf_cfg, minibf_domain) {
        // `ReadChain`, NOT an admin attempt: minibf is a read API over PUBLIC
        // chain data, and `wallet-viewer` calls it with the same
        // `MITOS_AUTH_TOKEN` secret the companions use while subscribing to
        // nothing. Gating it as admin would either 403 that worker the day
        // its secret becomes a namespace token, or leave it holding the
        // operator token — the one that evicts modules — for ever.
        let auth = bundle.authenticator().clone();
        let router = dolos_minibf::build_router(cfg, minibf_domain)
            .layer(from_fn_with_state(auth.clone(), require_read_chain))
            .layer(from_fn_with_state(auth, authenticate));
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
