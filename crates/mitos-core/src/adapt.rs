//! Adapter: `mitos_config::DerivedDolos` → dolos `RootConfig`.
//!
//! The only place in the host a dolos config type is constructed from a
//! mitos.toml. Fields mitos does not read are left at Default (vestigial
//! `[serve.grpc]` etc. are not derived).

use std::path::PathBuf;

use dolos_core::config::{
    ArchiveStoreConfig, CardanoConfig, ChainConfig, FjallStateConfig, GenesisConfig, MinibfConfig,
    MithrilConfig, PeerConfig, RedbArchiveConfig, RootConfig, ServeConfig, StateStoreConfig,
    StorageConfig, StorageVersion, SyncConfig, UpstreamConfig,
};
use mitos_config::network::{ArchiveBackend, StateBackend};
use mitos_config::{DerivedDolos, DerivedGenesis};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenesisPaths {
    pub byron: PathBuf,
    pub shelley: PathBuf,
    pub alonzo: PathBuf,
    pub conway: PathBuf,
}

impl GenesisPaths {
    pub fn from_derived_beside(dir: &std::path::Path, genesis: &DerivedGenesis) -> Self {
        Self {
            byron: dir.join(&genesis.byron_path),
            shelley: dir.join(&genesis.shelley_path),
            alonzo: dir.join(&genesis.alonzo_path),
            conway: dir.join(&genesis.conway_path),
        }
    }
}

pub fn to_root_config(derived: &DerivedDolos, genesis: GenesisPaths) -> RootConfig {
    let state = match derived.storage.state_backend {
        StateBackend::Fjall => StateStoreConfig::Fjall(FjallStateConfig {
            cache: Some(derived.storage.state_cache_mb),
            max_history: Some(derived.storage.state_max_history),
            ..Default::default()
        }),
    };
    let archive = match derived.storage.archive_backend {
        ArchiveBackend::Redb => ArchiveStoreConfig::Redb(RedbArchiveConfig::default()),
    };

    let storage = StorageConfig {
        // mitos-config's StorageSpec.version is the string "v3", matching
        // production's dolos.toml and dolos v1.2.0's default.
        version: {
            debug_assert_eq!(derived.storage.version, "v3");
            StorageVersion::V3
        },
        path: derived.storage.path.clone(),
        state,
        archive,
        ..Default::default()
    };

    // pull_batch_size is private, so this cannot be a struct update.
    #[allow(clippy::field_reassign_with_default)]
    let sync = {
        let mut sync = SyncConfig::default();
        sync.max_history = Some(derived.sync.max_history);
        sync.max_rollback = Some(derived.sync.max_rollback);
        sync
    };

    let serve = ServeConfig {
        minibf: Some(MinibfConfig::new(derived.minibf.listen_address)),
        ..Default::default()
    };

    RootConfig {
        upstream: UpstreamConfig::Peer(PeerConfig {
            peer_address: derived.upstream_peer_address.clone(),
        }),
        storage,
        genesis: GenesisConfig {
            byron_path: genesis.byron,
            shelley_path: genesis.shelley,
            alonzo_path: genesis.alonzo,
            conway_path: genesis.conway,
            force_protocol: derived.genesis.force_protocol,
        },
        sync,
        serve,
        mithril: Some(MithrilConfig {
            aggregator: derived.mithril.aggregator.clone(),
            genesis_key: derived.mithril.genesis_key.clone(),
            ancillary_key: Some(derived.mithril.ancillary_key.clone()),
        }),
        chain: ChainConfig::Cardano(CardanoConfig {
            magic: derived.chain.magic,
            is_testnet: derived.chain.is_testnet,
            ..Default::default()
        }),
        submit: Default::default(),
        relay: None,
        retries: None,
        snapshot: None,
        logging: Default::default(),
        telemetry: Default::default(),
    }
}

/// Materialise embedded genesis bytes next to `dir` (the node data_dir).
/// Existing files with matching bytes are left alone; a mismatch writes
/// `*.new` and returns that path so the caller can refuse.
pub fn materialise_genesis(
    dir: &std::path::Path,
    genesis: &DerivedGenesis,
) -> anyhow::Result<GenesisPaths> {
    use crate::atomic::{WritePlan, write_unless_edited};

    let paths = GenesisPaths::from_derived_beside(dir, genesis);
    let files = [
        (&paths.byron, genesis.byron, "byron.json"),
        (&paths.shelley, genesis.shelley, "shelley.json"),
        (&paths.alonzo, genesis.alonzo, "alonzo.json"),
        (&paths.conway, genesis.conway, "conway.json"),
    ];
    for (path, bytes, name) in files {
        match write_unless_edited(path, bytes, None)? {
            WritePlan::Wrote | WritePlan::AlreadyComplete => {}
            WritePlan::LeftNew { new_path } => {
                anyhow::bail!(
                    "genesis {name} at {} differs from the bytes embedded in this binary; \
                     left the new content at {} and left the original untouched",
                    path.display(),
                    new_path.display()
                );
            }
        }
    }
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::{GenesisPaths, to_root_config};
    use dolos_core::config::{
        ArchiveStoreConfig, ChainConfig, StateStoreConfig, StorageVersion, UpstreamConfig,
    };
    use mitos_config::network::{ArchiveBackend, StateBackend};
    use mitos_config::{MitosFile, derive_dolos, resolve};
    use std::path::{Path, PathBuf};

    const MAINNET_TOML: &str = include_str!("../../mitos-config/tests/fixtures/mainnet-dolos.toml");
    const PREPROD_TOML: &str = include_str!("../../mitos-config/tests/fixtures/preprod-dolos.toml");

    fn derived_root(
        network: &str,
        data_dir: &str,
    ) -> (mitos_config::DerivedDolos, dolos_core::config::RootConfig) {
        let file = MitosFile::parse(&format!("network = \"{network}\"\n")).unwrap();
        let env = |_: &str| -> Option<String> { None };
        let mut resolved = resolve(&file, &env).unwrap();
        resolved.data_dir = PathBuf::from(data_dir);
        let derived = derive_dolos(&resolved).unwrap();
        let genesis = GenesisPaths::from_derived_beside(Path::new(data_dir), &derived.genesis);
        let root = to_root_config(&derived, genesis);
        (derived, root)
    }

    fn assert_read_fields(network: &str, fixture: &str, data_dir: &str) {
        let prod: toml::Value = toml::from_str(fixture).unwrap();
        let (derived, root) = derived_root(network, data_dir);

        match &root.upstream {
            UpstreamConfig::Peer(p) => {
                assert_eq!(p.peer_address, derived.upstream_peer_address);
                assert_eq!(
                    p.peer_address,
                    prod["upstream"]["peer_address"].as_str().unwrap()
                );
            }
            UpstreamConfig::Emulator(_) => panic!("expected Peer, got emulator"),
        }

        assert!(matches!(root.storage.version, StorageVersion::V3));
        assert_eq!(root.storage.path, PathBuf::from(data_dir).join("data"));
        assert_eq!(prod["storage"]["path"].as_str().unwrap(), "data");
        match &root.storage.state {
            StateStoreConfig::Fjall(cfg) => {
                assert!(matches!(derived.storage.state_backend, StateBackend::Fjall));
                assert_eq!(cfg.cache, Some(derived.storage.state_cache_mb));
                assert_eq!(cfg.max_history, Some(derived.storage.state_max_history));
                assert_eq!(
                    cfg.cache.unwrap() as i64,
                    prod["storage"]["state"]["cache"].as_integer().unwrap()
                );
            }
            _ => panic!("expected fjall state"),
        }
        match &root.storage.archive {
            ArchiveStoreConfig::Redb(_) => {
                assert!(matches!(
                    derived.storage.archive_backend,
                    ArchiveBackend::Redb
                ));
            }
            _ => panic!("expected redb archive"),
        }

        assert_eq!(root.sync.max_history, Some(derived.sync.max_history));
        assert_eq!(root.sync.max_rollback, Some(derived.sync.max_rollback));
        assert_eq!(
            root.sync.max_history.unwrap() as i64,
            prod["sync"]["max_history"].as_integer().unwrap()
        );

        match &root.chain {
            ChainConfig::Cardano(c) => {
                assert_eq!(c.magic, derived.chain.magic);
                assert_eq!(c.is_testnet, derived.chain.is_testnet);
                assert_eq!(c.magic as i64, prod["chain"]["magic"].as_integer().unwrap());
                assert_eq!(c.is_testnet, prod["chain"]["is_testnet"].as_bool().unwrap());
            }
        }

        let mithril = root.mithril.as_ref().expect("mithril derived");
        assert_eq!(mithril.aggregator, derived.mithril.aggregator);
        assert_eq!(mithril.genesis_key, derived.mithril.genesis_key);
        assert_eq!(
            mithril.ancillary_key.as_deref(),
            Some(derived.mithril.ancillary_key.as_str())
        );
        assert_eq!(
            mithril.aggregator,
            prod["mithril"]["aggregator"].as_str().unwrap()
        );
        assert_eq!(
            mithril.ancillary_key.as_deref(),
            prod["mithril"]["ancillary_key"].as_str()
        );

        let minibf = root.serve.minibf.as_ref().expect("minibf derived");
        assert_eq!(minibf.listen_address, derived.minibf.listen_address);
        // vestigial grpc/trp/minikupo/ouroboros are NOT derived
        assert!(root.serve.grpc.is_none());
        assert!(root.serve.trp.is_none());
        assert!(root.serve.minikupo.is_none());
        assert!(root.serve.ouroboros.is_none());
        let _ = minibf;
    }

    #[test]
    fn mainnet_rootconfig_matches_infra_fixture_in_every_field_read() {
        assert_read_fields("mainnet", MAINNET_TOML, "/opt/mitos/mainnet");
    }

    #[test]
    fn preprod_rootconfig_matches_infra_fixture_in_every_field_read() {
        assert_read_fields("preprod", PREPROD_TOML, "/opt/mitos/preprod");
    }
}
