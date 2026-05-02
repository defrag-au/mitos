//! Embedding glue: construct a Dolos `DomainAdapter` and start its
//! chain-sync pipeline from external code.
//!
//! Mirrors `dolos/src/bin/dolos/common.rs::setup_domain` — Dolos's wiring
//! lives in its binary crate (not exported as a library function), so we
//! replicate it here. Kept deliberately close to upstream so that bumping
//! the Dolos tag is a reviewable diff rather than a redesign.

use std::path::Path;
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos::core::Genesis;
use dolos::storage;
use dolos_core::config::{ChainConfig, GenesisConfig, RootConfig};
use dolos_core::{BootstrapExt, ChainLogic};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Load a `RootConfig` from a TOML file, with `DOLOS_*` env-var overrides.
///
/// Compatible with stock `dolos.toml` files — bundles and Dolos can share
/// the same config file when they're operating on the same data dir.
pub fn load_config(path: impl AsRef<Path>) -> anyhow::Result<RootConfig> {
    let path = path.as_ref();
    let cfg = ::config::Config::builder()
        .add_source(::config::File::with_name(
            path.to_str()
                .ok_or_else(|| anyhow::anyhow!("config path is not valid UTF-8"))?,
        ))
        .add_source(::config::Environment::with_prefix("DOLOS").separator("_"))
        .build()
        .map_err(|e| anyhow::anyhow!("loading config from {}: {e}", path.display()))?;
    cfg.try_deserialize::<RootConfig>()
        .map_err(|e| anyhow::anyhow!("deserializing config: {e}"))
}

/// Open all data stores + initialize the Cardano chain logic + assemble
/// a `DomainAdapter`. Replicates `setup_domain` from Dolos's binary.
///
/// Requires the data dir to already exist (i.e. Dolos has been bootstrapped
/// against this config). Mitos doesn't run the bootstrap itself yet —
/// invoke `dolos bootstrap mithril ...` first, then point a bundle at the
/// resulting data dir.
pub fn setup_domain(config: &RootConfig) -> anyhow::Result<DomainAdapter> {
    let stores = storage::open_data_stores::<dolos_cardano::CardanoDelta>(config)
        .map_err(|e| anyhow::anyhow!("opening data stores: {e}"))?;

    let genesis = Arc::new(open_genesis_files(&config.genesis)?);
    let mempool = stores.mempool.clone();
    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

    let ChainConfig::Cardano(chain_config) = config.chain.clone();

    let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
        chain_config,
        &stores.state,
        &genesis,
    )
    .map_err(|e| anyhow::anyhow!("initializing CardanoLogic: {e}"))?;

    let domain = DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        sync_config: Arc::new(config.sync.clone()),
        genesis,
        chain: Arc::new(std::sync::RwLock::new(chain)),
        wal: stores.wal,
        state: stores.state,
        archive: stores.archive,
        indexes: stores.indexes,
        mempool,
        tip_broadcast,
    };

    domain
        .bootstrap()
        .map_err(|e| anyhow::anyhow!("domain bootstrap: {e:?}"))?;

    Ok(domain)
}

fn open_genesis_files(config: &GenesisConfig) -> anyhow::Result<Genesis> {
    Genesis::from_file_paths(
        &config.byron_path,
        &config.shelley_path,
        &config.alonzo_path,
        &config.conway_path,
        config.force_protocol,
    )
    .map_err(|e| anyhow::anyhow!("loading genesis files: {e}"))
}

/// Spawn Dolos's chain-sync pipeline as a background task. The pipeline
/// consumes blocks from the upstream peer (per `config.upstream`) and
/// writes them to WAL + state + archive + indexes, which causes the
/// `DomainAdapter`'s tip broadcast channel to emit `TipEvent`s that
/// indexer dispatchers consume.
///
/// The returned `JoinHandle` resolves when the pipeline shuts down;
/// `exit` is the cancellation token to trigger shutdown from outside.
pub fn spawn_sync_pipeline(
    domain: DomainAdapter,
    config: &RootConfig,
    exit: CancellationToken,
) -> anyhow::Result<JoinHandle<()>> {
    let pipeline = dolos::sync::pipeline(
        &config.sync,
        &config.chain,
        &config.upstream,
        domain,
        &config.retries,
    )
    .map_err(|e| anyhow::anyhow!("building sync pipeline: {e}"))?;

    let daemon = gasket::daemon::Daemon::new(pipeline);

    let handle = tokio::spawn(async move {
        run_pipeline_loop(daemon, exit).await;
    });

    Ok(handle)
}

/// Mirror of `dolos::common::run_pipeline` — polls the pipeline for stop
/// conditions and forwards external cancellation to gasket teardown.
async fn run_pipeline_loop(pipeline: gasket::daemon::Daemon, exit: CancellationToken) {
    use std::time::Duration;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if pipeline.should_stop() {
                    tracing::debug!("sync pipeline reported should_stop");
                    exit.cancel();
                    break;
                }
            }
            _ = exit.cancelled() => {
                tracing::debug!("exit requested; tearing down sync pipeline");
                break;
            }
        }
    }
    pipeline.teardown();
}
