//! Derive the dolos-shaped config mitos actually reads. Plain structs —
//! no dolos types. `mitos-core` is the adapter.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::error::Error;
use crate::file::Profile;
use crate::network::{ArchiveBackend, StateBackend, spec};
use crate::resolve::Resolved;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedDolos {
    pub upstream_peer_address: String,
    pub storage: DerivedStorage,
    pub genesis: DerivedGenesis,
    pub sync: DerivedSync,
    pub chain: DerivedChain,
    pub mithril: DerivedMithril,
    /// Present because mitos mounts minibf when `serve.minibf` is Some.
    /// `listen_address` is vestigial (mounted on BUNDLE_LISTEN).
    pub minibf: DerivedMinibf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedStorage {
    pub version: &'static str,
    /// Absolute: `{data_dir}/data`. Production's file says `path = "data"`
    /// relative to WorkingDirectory; same resolved location when
    /// `data_dir` is that working directory.
    pub path: PathBuf,
    pub state_backend: StateBackend,
    pub state_cache_mb: usize,
    pub state_max_history: u64,
    pub archive_backend: ArchiveBackend,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedGenesis {
    pub byron_path: PathBuf,
    pub shelley_path: PathBuf,
    pub alonzo_path: PathBuf,
    pub conway_path: PathBuf,
    pub byron: &'static [u8],
    pub shelley: &'static [u8],
    pub alonzo: &'static [u8],
    pub conway: &'static [u8],
    pub force_protocol: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedSync {
    pub max_history: u64,
    pub max_rollback: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedChain {
    pub chain_type: &'static str,
    pub magic: u64,
    pub is_testnet: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedMithril {
    pub aggregator: String,
    pub genesis_key: String,
    pub ancillary_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DerivedMinibf {
    pub listen_address: SocketAddr,
}

pub fn derive_dolos(resolved: &Resolved) -> Result<DerivedDolos, Error> {
    if resolved.profile != Profile::Full {
        return Err(Error::ProfileNotImplemented {
            profile: resolved.profile,
        });
    }
    let net = spec(resolved.network);
    Ok(DerivedDolos {
        upstream_peer_address: net.peer_address.to_owned(),
        storage: DerivedStorage {
            version: net.storage.version,
            path: resolved.data_dir.join("data"),
            state_backend: net.storage.state_backend,
            state_cache_mb: net.storage.state_cache_mb,
            state_max_history: net.storage.state_max_history,
            archive_backend: net.storage.archive_backend,
        },
        genesis: DerivedGenesis {
            byron_path: PathBuf::from("byron.json"),
            shelley_path: PathBuf::from("shelley.json"),
            alonzo_path: PathBuf::from("alonzo.json"),
            conway_path: PathBuf::from("conway.json"),
            byron: net.genesis.byron,
            shelley: net.genesis.shelley,
            alonzo: net.genesis.alonzo,
            conway: net.genesis.conway,
            force_protocol: None,
        },
        sync: DerivedSync {
            max_history: net.sync.max_history,
            max_rollback: net.sync.max_rollback,
        },
        chain: DerivedChain {
            chain_type: "cardano",
            magic: net.magic,
            is_testnet: net.is_testnet,
        },
        mithril: DerivedMithril {
            aggregator: net.mithril.aggregator.to_owned(),
            genesis_key: net.mithril.genesis_key.to_owned(),
            ancillary_key: net.mithril.ancillary_key.to_owned(),
        },
        minibf: DerivedMinibf {
            listen_address: net.minibf_listen,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::derive_dolos;
    use crate::file::MitosFile;
    use crate::network::{ArchiveBackend, StateBackend};
    use crate::resolve::resolve;
    use serde::Deserialize;
    use std::net::SocketAddr;
    use std::path::PathBuf;

    /// Fields of a production `dolos.toml` that mitos actually reads (plus
    /// mithril, which bootstrap will read). Unknown / vestigial keys are
    /// ignored so the fixture can be the real file.
    #[derive(Debug, Deserialize)]
    struct ProductionDolosFile {
        upstream: ProductionUpstream,
        storage: ProductionStorage,
        genesis: ProductionGenesis,
        sync: ProductionSync,
        mithril: ProductionMithril,
        chain: ProductionChain,
        serve: ProductionServe,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionUpstream {
        peer_address: String,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionStorage {
        version: String,
        path: PathBuf,
        state: ProductionState,
        archive: ProductionArchive,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionState {
        backend: String,
        cache: usize,
        max_history: u64,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionArchive {
        backend: String,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionGenesis {
        byron_path: PathBuf,
        shelley_path: PathBuf,
        alonzo_path: PathBuf,
        conway_path: PathBuf,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionSync {
        max_history: u64,
        max_rollback: u64,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionMithril {
        aggregator: String,
        genesis_key: String,
        #[serde(default)]
        ancillary_key: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionChain {
        #[serde(rename = "type")]
        chain_type: String,
        magic: u64,
        is_testnet: bool,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionServe {
        minibf: ProductionMinibf,
    }

    #[derive(Debug, Deserialize)]
    struct ProductionMinibf {
        listen_address: SocketAddr,
    }

    const MAINNET_TOML: &str = include_str!("../tests/fixtures/mainnet-dolos.toml");
    const PREPROD_TOML: &str = include_str!("../tests/fixtures/preprod-dolos.toml");

    fn derived(network: &str, data_dir: &str) -> crate::derive::DerivedDolos {
        let file = MitosFile::parse(&format!("network = \"{network}\"\n")).unwrap();
        let env = |_: &str| -> Option<String> { None };
        let mut resolved = resolve(&file, &env).unwrap();
        resolved.data_dir = PathBuf::from(data_dir);
        derive_dolos(&resolved).unwrap()
    }

    fn assert_read_fields_match(network: &str, fixture: &str, data_dir: &str) {
        let prod: ProductionDolosFile = toml::from_str(fixture).unwrap();
        let der = derived(network, data_dir);

        assert_eq!(der.upstream_peer_address, prod.upstream.peer_address);
        assert_eq!(der.storage.version, prod.storage.version);
        assert_eq!(der.storage.path, PathBuf::from(data_dir).join("data"));
        assert_eq!(prod.storage.path, PathBuf::from("data"));
        match der.storage.state_backend {
            StateBackend::Fjall => assert_eq!(prod.storage.state.backend, "fjall"),
        }
        assert_eq!(der.storage.state_cache_mb, prod.storage.state.cache);
        assert_eq!(
            der.storage.state_max_history,
            prod.storage.state.max_history
        );
        match der.storage.archive_backend {
            ArchiveBackend::Redb => assert_eq!(prod.storage.archive.backend, "redb"),
        }
        assert_eq!(der.genesis.byron_path, prod.genesis.byron_path);
        assert_eq!(der.genesis.shelley_path, prod.genesis.shelley_path);
        assert_eq!(der.genesis.alonzo_path, prod.genesis.alonzo_path);
        assert_eq!(der.genesis.conway_path, prod.genesis.conway_path);
        assert_eq!(der.sync.max_history, prod.sync.max_history);
        assert_eq!(der.sync.max_rollback, prod.sync.max_rollback);
        assert_eq!(der.mithril.aggregator, prod.mithril.aggregator);
        assert_eq!(der.mithril.genesis_key, prod.mithril.genesis_key);
        assert_eq!(
            Some(der.mithril.ancillary_key.as_str()),
            prod.mithril.ancillary_key.as_deref(),
            "{network} ancillary_key: infra's dolos.toml must carry the dolos v1.2.0 key"
        );
        assert_eq!(der.chain.chain_type, prod.chain.chain_type);
        assert_eq!(der.chain.magic, prod.chain.magic);
        assert_eq!(der.chain.is_testnet, prod.chain.is_testnet);
        assert_eq!(der.minibf.listen_address, prod.serve.minibf.listen_address);
    }

    #[test]
    fn mainnet_derivation_matches_infra_dolos_toml_in_every_field_read() {
        assert_read_fields_match("mainnet", MAINNET_TOML, "/opt/mitos/mainnet");
    }

    #[test]
    fn preprod_derivation_matches_infra_dolos_toml_in_every_field_read() {
        assert_read_fields_match("preprod", PREPROD_TOML, "/opt/mitos/preprod");
    }

    #[test]
    fn vestigial_serve_sections_are_not_on_derived_config() {
        // If this struct grows a grpc/ouroboros/trp field, the equivalence
        // test would start requiring them. Keep the derived surface to what
        // mitos_core::setup_domain + minibf actually read.
        let der = derived("mainnet", "/opt/mitos/mainnet");
        let _ = der.minibf;
    }
}
