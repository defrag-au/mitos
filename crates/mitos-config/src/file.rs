//! The authored `/etc/mitos/mitos.toml`. Secrets are file paths, never values.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Default node root. Dolos's data dir is `{data_dir}/data`; mitos's own
/// state is `{data_dir}/mitos-data`. Production today uses a working
/// directory of `/opt/mitos/mainnet` and a relative `storage.path = "data"`.
pub fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/mitos")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NetworkId {
    Mainnet,
    Preprod,
}

impl fmt::Display for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mainnet => "mainnet",
            Self::Preprod => "preprod",
        })
    }
}

/// Footprint profiles. Only [`Profile::Full`] has behaviour; the others
/// parse so a file that names them is valid, and are refused by resolve /
/// doctor with "not implemented yet".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    Lean,
    Standard,
    #[default]
    Full,
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Lean => "lean",
            Self::Standard => "standard",
            Self::Full => "full",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MitosFile {
    pub network: NetworkId,
    #[serde(default)]
    pub profile: Profile,
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub listen: Option<String>,

    #[serde(default)]
    pub auth_token_file: Option<PathBuf>,
    #[serde(default)]
    pub readonly_token_file: Option<PathBuf>,
    #[serde(default)]
    pub namespaces_file: Option<PathBuf>,
    #[serde(default)]
    pub koios_api_key_file: Option<PathBuf>,

    /// Directory that contains `immutable/`. WO-F5's `--chunk-store`.
    #[serde(default)]
    pub chunk_store: Option<PathBuf>,

    #[serde(default)]
    pub modules: ModulesSection,
    #[serde(default)]
    pub caches: CachesSection,
    #[serde(default)]
    pub tx_index: TxIndexSection,
    #[serde(default)]
    pub fallback: FallbackSection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ModulesSection {
    #[serde(default)]
    pub catalogue: Option<String>,
    #[serde(default)]
    pub install: Vec<String>,
}

/// WO-F2's knobs, stored so a file can name them. Only `full` has
/// behaviour, and even then this crate does not invent cache sizes —
/// the host keeps today's defaults until WO-F2 lands.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CachesSection {
    #[serde(default)]
    pub kv_mb: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TxIndexSection {
    #[serde(default)]
    pub dir: Option<PathBuf>,
    #[serde(default)]
    pub immutable: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum FallbackProvider {
    #[default]
    Koios,
    Maestro,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum FallbackGate {
    Off,
    #[default]
    Shadow,
    On,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct FallbackSection {
    #[serde(default)]
    pub provider: Option<FallbackProvider>,
    #[serde(default)]
    pub gate: Option<FallbackGate>,
    /// `api` on mainnet, `preprod` on preprod — production's `KOIOS_NETWORK`.
    #[serde(default)]
    pub koios_network: Option<String>,
}

impl MitosFile {
    pub fn parse(text: &str) -> Result<Self, Error> {
        toml::from_str(text).map_err(|e| Error::from_toml("mitos.toml", e))
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }

    pub fn listen_or_parse(&self) -> Result<Option<SocketAddr>, Error> {
        match self.listen.as_deref() {
            None => Ok(None),
            Some(raw) => raw.parse().map(Some).map_err(|_| Error::InvalidListen {
                value: raw.to_owned(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MitosFile, NetworkId, Profile};

    #[test]
    fn minimal_file_round_trips() {
        let text = "network = \"mainnet\"\n";
        let file = MitosFile::parse(text).unwrap();
        assert_eq!(file.network, NetworkId::Mainnet);
        assert_eq!(file.profile, Profile::Full);
        let again = MitosFile::parse(&file.to_toml().unwrap()).unwrap();
        assert_eq!(file, again);
    }

    #[test]
    fn lean_and_standard_parse() {
        for profile in ["lean", "standard"] {
            let text = format!("network = \"preprod\"\nprofile = \"{profile}\"\n");
            let file = MitosFile::parse(&text).unwrap();
            assert_eq!(file.profile.to_string(), profile);
        }
    }

    #[test]
    fn typo_d_key_names_the_key() {
        let err = MitosFile::parse("network = \"mainnet\"\nnetwrok = \"x\"\n").unwrap_err();
        match err {
            crate::error::Error::UnknownField { file, key } => {
                assert_eq!(file, "mitos.toml");
                assert_eq!(key, "netwrok");
            }
            other => panic!("expected UnknownField, got {other}"),
        }
    }

    #[test]
    fn unknown_nested_key_is_refused() {
        let err = MitosFile::parse("network = \"mainnet\"\n[caches]\nkv_mb = 256\nmystery = 1\n")
            .unwrap_err();
        match err {
            crate::error::Error::UnknownField { key, .. } => assert_eq!(key, "mystery"),
            other => panic!("expected UnknownField, got {other}"),
        }
    }
}
