//! Known networks. Magic, Mithril keys, and genesis bytes live here so a
//! node does not copy four JSON files between repositories.
//!
//! Constants were read from dolos **v1.2.0** `src/bin/dolos/init.rs`
//! (`KnownNetwork`). One deliberate divergence: dolos's `From<&KnownNetwork>
//! for PeerConfig` uses the Demeter relay; production and
//! `infra/services/mitos/*/dolos.toml` use the Cardano Foundation backbone,
//! and that is the peer we derive.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::file::NetworkId;

/// Tag these values were copied from. Bump this string when re-reading.
pub const DOLOS_SOURCE_TAG: &str = "v1.2.0";

pub const MAINNET_BYRON_SHA256: &str =
    "4f28b3b437b2c4f6ee26cc70964b3a5f1a274b0b3909c31535091c00316c13aa";
pub const MAINNET_SHELLEY_SHA256: &str =
    "59cd3932c6dd792bc5020ca3336064a8faabde4e4a8dc7d143ff4df6eec36961";
pub const MAINNET_ALONZO_SHA256: &str =
    "3511a116de9496a84f956ac50c90dbe92e95d0fbee7b2d7134fb4d407efb9d0e";
pub const MAINNET_CONWAY_SHA256: &str =
    "ae9d00e4dd9a60814f541005e061ceeb93248712345cb7bbf6a15b887143acdb";

pub const PREPROD_BYRON_SHA256: &str =
    "d88fbffdf78daaccfaddf504e95840c73ce527c06fa4140aefb55d3f91c00cef";
pub const PREPROD_SHELLEY_SHA256: &str =
    "4b9d32c09159c2948e4386ba1f59db5a249a89b43b84dfd8368f465e650095de";
pub const PREPROD_ALONZO_SHA256: &str =
    "7333bfafe311589fa09e8bf59a47ec0d85a1959f00748cc0800591d2c7646408";
pub const PREPROD_CONWAY_SHA256: &str =
    "dbd8fbf16aafaf97a0894cb69db477fb658b3fec744ef89e8bd06e77aa482753";

const MAINNET_BYRON: &[u8] = include_bytes!("../../../networks/mainnet/byron.json");
const MAINNET_SHELLEY: &[u8] = include_bytes!("../../../networks/mainnet/shelley.json");
const MAINNET_ALONZO: &[u8] = include_bytes!("../../../networks/mainnet/alonzo.json");
const MAINNET_CONWAY: &[u8] = include_bytes!("../../../networks/mainnet/conway.json");

const PREPROD_BYRON: &[u8] = include_bytes!("../../../networks/preprod/byron.json");
const PREPROD_SHELLEY: &[u8] = include_bytes!("../../../networks/preprod/shelley.json");
const PREPROD_ALONZO: &[u8] = include_bytes!("../../../networks/preprod/alonzo.json");
const PREPROD_CONWAY: &[u8] = include_bytes!("../../../networks/preprod/conway.json");

/// CF backbone — what we run. dolos's KnownNetwork default is Demeter.
pub const MAINNET_PEER: &str = "backbone.mainnet.cardanofoundation.org:3001";
pub const PREPROD_PEER: &str = "preprod-node.world.dev.cardano.org:30000";

pub const MAINNET_MAGIC: u64 = 764824073;
pub const PREPROD_MAGIC: u64 = 1;

pub const MAINNET_MITHRIL_AGGREGATOR: &str =
    "https://aggregator.release-mainnet.api.mithril.network/aggregator";
pub const PREPROD_MITHRIL_AGGREGATOR: &str =
    "https://aggregator.release-preprod.api.mithril.network/aggregator";

pub const MAINNET_MITHRIL_GENESIS_KEY: &str = "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d";
pub const PREPROD_MITHRIL_GENESIS_KEY: &str = "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d";

pub const MAINNET_MITHRIL_ANCILLARY_KEY: &str = "5b32332c37312c39362c3133332c34372c3235332c3232362c3133362c3233352c35372c3136342c3130362c3138362c322c32312c32392c3132302c3136332c38392c3132312c3137372c3133382c3230382c3133382c3231342c39392c35382c32322c302c35382c332c36395d";
pub const PREPROD_MITHRIL_ANCILLARY_KEY: &str = "5b3138392c3139322c3231362c3135302c3131342c3231362c3233372c3231302c34352c31382c32312c3139362c3230382c3234362c3134362c322c3235322c3234332c3235312c3139372c32382c3135372c3230342c3134352c33302c31342c3232382c3136382c3132392c38332c3133362c33365d";

/// Vestigial minibf bind in production's dolos.toml. mitos mounts minibf
/// on `BUNDLE_LISTEN`; this address is kept so a derived config matches
/// the field the file still carries.
pub const MAINNET_MINIBF_LISTEN: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3000);
pub const PREPROD_MINIBF_LISTEN: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 3100);

pub const MAINNET_LISTEN: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8181);
pub const PREPROD_LISTEN: SocketAddr =
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8182);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateBackend {
    Fjall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveBackend {
    Redb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageSpec {
    pub version: &'static str,
    pub state_backend: StateBackend,
    pub state_cache_mb: usize,
    pub state_max_history: u64,
    pub archive_backend: ArchiveBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncSpec {
    pub max_history: u64,
    pub max_rollback: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MithrilSpec {
    pub aggregator: &'static str,
    pub genesis_key: &'static str,
    pub ancillary_key: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenesisFiles {
    pub byron: &'static [u8],
    pub shelley: &'static [u8],
    pub alonzo: &'static [u8],
    pub conway: &'static [u8],
    pub byron_sha256: &'static str,
    pub shelley_sha256: &'static str,
    pub alonzo_sha256: &'static str,
    pub conway_sha256: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetworkSpec {
    pub id: NetworkId,
    pub magic: u64,
    pub is_testnet: bool,
    pub peer_address: &'static str,
    pub mithril: MithrilSpec,
    pub genesis: GenesisFiles,
    /// Storage knobs we actually run, from infra's dolos.toml — not from
    /// dolos KnownNetwork, which does not carry them.
    pub storage: StorageSpec,
    pub sync: SyncSpec,
    pub default_listen: SocketAddr,
    pub minibf_listen: SocketAddr,
    /// `KOIOS_NETWORK` production sets: `api` on mainnet, `preprod` on preprod.
    pub koios_network: &'static str,
}

pub fn spec(id: NetworkId) -> &'static NetworkSpec {
    match id {
        NetworkId::Mainnet => &MAINNET,
        NetworkId::Preprod => &PREPROD,
    }
}

pub static MAINNET: NetworkSpec = NetworkSpec {
    id: NetworkId::Mainnet,
    magic: MAINNET_MAGIC,
    is_testnet: false,
    peer_address: MAINNET_PEER,
    mithril: MithrilSpec {
        aggregator: MAINNET_MITHRIL_AGGREGATOR,
        genesis_key: MAINNET_MITHRIL_GENESIS_KEY,
        ancillary_key: MAINNET_MITHRIL_ANCILLARY_KEY,
    },
    genesis: GenesisFiles {
        byron: MAINNET_BYRON,
        shelley: MAINNET_SHELLEY,
        alonzo: MAINNET_ALONZO,
        conway: MAINNET_CONWAY,
        byron_sha256: MAINNET_BYRON_SHA256,
        shelley_sha256: MAINNET_SHELLEY_SHA256,
        alonzo_sha256: MAINNET_ALONZO_SHA256,
        conway_sha256: MAINNET_CONWAY_SHA256,
    },
    storage: StorageSpec {
        version: "v3",
        state_backend: StateBackend::Fjall,
        state_cache_mb: 500,
        state_max_history: 604800,
        archive_backend: ArchiveBackend::Redb,
    },
    sync: SyncSpec {
        max_history: 604800,
        max_rollback: 129600,
    },
    default_listen: MAINNET_LISTEN,
    minibf_listen: MAINNET_MINIBF_LISTEN,
    koios_network: "api",
};

pub static PREPROD: NetworkSpec = NetworkSpec {
    id: NetworkId::Preprod,
    magic: PREPROD_MAGIC,
    is_testnet: true,
    peer_address: PREPROD_PEER,
    mithril: MithrilSpec {
        aggregator: PREPROD_MITHRIL_AGGREGATOR,
        genesis_key: PREPROD_MITHRIL_GENESIS_KEY,
        ancillary_key: PREPROD_MITHRIL_ANCILLARY_KEY,
    },
    genesis: GenesisFiles {
        byron: PREPROD_BYRON,
        shelley: PREPROD_SHELLEY,
        alonzo: PREPROD_ALONZO,
        conway: PREPROD_CONWAY,
        byron_sha256: PREPROD_BYRON_SHA256,
        shelley_sha256: PREPROD_SHELLEY_SHA256,
        alonzo_sha256: PREPROD_ALONZO_SHA256,
        conway_sha256: PREPROD_CONWAY_SHA256,
    },
    storage: StorageSpec {
        version: "v3",
        state_backend: StateBackend::Fjall,
        state_cache_mb: 200,
        state_max_history: 86400,
        archive_backend: ArchiveBackend::Redb,
    },
    sync: SyncSpec {
        max_history: 604800,
        max_rollback: 129600,
    },
    default_listen: PREPROD_LISTEN,
    minibf_listen: PREPROD_MINIBF_LISTEN,
    koios_network: "preprod",
};

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::{DOLOS_SOURCE_TAG, MAINNET, PREPROD, spec};
    use crate::file::NetworkId;

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    #[test]
    fn genesis_bytes_match_pinned_digests() {
        for net in [NetworkId::Mainnet, NetworkId::Preprod] {
            let g = spec(net).genesis;
            assert_eq!(sha256_hex(g.byron), g.byron_sha256, "{net} byron");
            assert_eq!(sha256_hex(g.shelley), g.shelley_sha256, "{net} shelley");
            assert_eq!(sha256_hex(g.alonzo), g.alonzo_sha256, "{net} alonzo");
            assert_eq!(sha256_hex(g.conway), g.conway_sha256, "{net} conway");
        }
    }

    #[test]
    fn source_tag_is_the_dolos_tag_we_embed() {
        assert_eq!(DOLOS_SOURCE_TAG, "v1.2.0");
    }

    #[test]
    fn magics_match_dolos_known_network() {
        assert_eq!(MAINNET.magic, 764824073);
        assert_eq!(PREPROD.magic, 1);
        assert!(!MAINNET.is_testnet);
        assert!(PREPROD.is_testnet);
    }
}
