//! Precedence: env var > file > default. The lookup is a function so no
//! test depends on process-global env.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::env;
use crate::error::Error;
use crate::file::{
    CachesSection, FallbackGate, FallbackProvider, FallbackSection, MitosFile, ModulesSection,
    NetworkId, Profile, TxIndexSection, default_data_dir,
};
use crate::network::spec;

/// A lookup over env var names. Never `std::env`.
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub network: NetworkId,
    pub profile: Profile,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// mitos's own state (`BUNDLE_DATA_DIR`). Distinct from dolos `data/`.
    pub bundle_data_dir: PathBuf,
    pub modules_dir: Option<PathBuf>,
    pub community_modules_dir: Option<PathBuf>,
    pub auth_token_file: Option<PathBuf>,
    pub readonly_token_file: Option<PathBuf>,
    pub namespaces_file: Option<PathBuf>,
    pub koios_api_key_file: Option<PathBuf>,
    /// Env overrode the file: the host must read `MITOS_AUTH_TOKEN`.
    pub auth_token_from_env: bool,
    pub readonly_token_from_env: bool,
    pub koios_api_key_from_env: bool,
    pub modules: ModulesSection,
    pub caches: CachesSection,
    pub tx_index: Option<TxIndexPaths>,
    pub fallback: ResolvedFallback,
    pub chunk_store: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxIndexPaths {
    pub dir: PathBuf,
    pub immutable: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFallback {
    pub provider: FallbackProvider,
    pub gate: FallbackGate,
    pub koios_network: String,
}

pub fn resolve(file: &MitosFile, env: EnvLookup<'_>) -> Result<Resolved, Error> {
    if file.profile != Profile::Full {
        return Err(Error::ProfileNotImplemented {
            profile: file.profile,
        });
    }
    let net = spec(file.network);
    let data_dir = overlay_path(env, None, file.data_dir.clone(), default_data_dir());
    let listen = match non_empty(env, env::BUNDLE_LISTEN) {
        Some(raw) => parse_listen(&raw)?,
        None => file.listen_or_parse()?.unwrap_or(net.default_listen),
    };

    let bundle_data_dir = overlay_path(
        env,
        Some(env::BUNDLE_DATA_DIR),
        None,
        data_dir.join("mitos-data"),
    );
    let modules_dir = overlay_optional_path(
        env,
        env::BUNDLE_MODULES_DIR,
        None,
        Some(data_dir.join("modules")),
    );
    let community_modules_dir =
        overlay_optional_path(env, env::BUNDLE_COMMUNITY_MODULES_DIR, None, None);

    let auth_token_from_env = non_empty(env, env::MITOS_AUTH_TOKEN).is_some();
    let readonly_token_from_env = non_empty(env, env::MITOS_READONLY_TOKEN).is_some();
    let koios_api_key_from_env = non_empty(env, env::KOIOS_API_KEY).is_some();

    let chunk_store = match non_empty(env, env::MITOS_CHUNK_STORE) {
        Some(raw) => {
            let p = PathBuf::from(raw);
            if p.as_os_str().is_empty() {
                return Err(Error::EmptyChunkStore(p));
            }
            Some(p)
        }
        None => file.chunk_store.clone(),
    };

    Ok(Resolved {
        network: file.network,
        profile: file.profile,
        data_dir: data_dir.clone(),
        listen,
        bundle_data_dir,
        modules_dir,
        community_modules_dir,
        auth_token_file: file.auth_token_file.clone(),
        readonly_token_file: file.readonly_token_file.clone(),
        namespaces_file: overlay_optional_path(
            env,
            env::MITOS_NAMESPACES_FILE,
            file.namespaces_file.clone(),
            None,
        ),
        koios_api_key_file: file.koios_api_key_file.clone(),
        auth_token_from_env,
        readonly_token_from_env,
        koios_api_key_from_env,
        modules: file.modules.clone(),
        caches: file.caches.clone(),
        tx_index: resolve_tx_index(&file.tx_index, env)?,
        fallback: resolve_fallback(&file.fallback, env, net.koios_network)?,
        chunk_store,
    })
}

fn resolve_tx_index(
    file: &TxIndexSection,
    env: EnvLookup<'_>,
) -> Result<Option<TxIndexPaths>, Error> {
    let dir = non_empty(env, env::MITOS_TX_INDEX_DIR).map(PathBuf::from);
    let imm = non_empty(env, env::MITOS_TX_INDEX_IMMUTABLE).map(PathBuf::from);
    match (dir, imm, file.dir.clone(), file.immutable.clone()) {
        (Some(dir), Some(immutable), ..) => Ok(Some(TxIndexPaths { dir, immutable })),
        (None, None, Some(dir), Some(immutable)) => Ok(Some(TxIndexPaths { dir, immutable })),
        (None, None, None, None) => Ok(None),
        (Some(_), None, ..) => Err(Error::TxIndexHalfConfigured {
            which: env::MITOS_TX_INDEX_DIR,
        }),
        (None, Some(_), ..) => Err(Error::TxIndexHalfConfigured {
            which: env::MITOS_TX_INDEX_IMMUTABLE,
        }),
        (None, None, Some(_), None) => Err(Error::TxIndexHalfConfigured {
            which: "tx_index.dir",
        }),
        (None, None, None, Some(_)) => Err(Error::TxIndexHalfConfigured {
            which: "tx_index.immutable",
        }),
    }
}

fn resolve_fallback(
    file: &FallbackSection,
    env: EnvLookup<'_>,
    default_koios_network: &'static str,
) -> Result<ResolvedFallback, Error> {
    let provider = match non_empty(env, env::MITOS_FALLBACK_PROVIDER) {
        Some(raw) => parse_provider(&raw),
        None => file.provider.unwrap_or(FallbackProvider::Koios),
    };
    let gate = match non_empty(env, env::MITOS_FALLBACK_GATE) {
        Some(raw) => parse_gate(&raw),
        None => file.gate.unwrap_or(FallbackGate::Shadow),
    };
    let koios_network = match non_empty(env, env::KOIOS_NETWORK) {
        Some(raw) => raw,
        None => file
            .koios_network
            .clone()
            .unwrap_or_else(|| default_koios_network.to_owned()),
    };
    Ok(ResolvedFallback {
        provider,
        gate,
        koios_network,
    })
}

fn parse_provider(raw: &str) -> FallbackProvider {
    // Same rule as mitos-platform: unrecognised → Koios, never Maestro.
    match raw.trim().to_ascii_lowercase().as_str() {
        "koios" | "" => FallbackProvider::Koios,
        "maestro" => FallbackProvider::Maestro,
        "none" => FallbackProvider::None,
        _ => FallbackProvider::Koios,
    }
}

fn parse_gate(raw: &str) -> FallbackGate {
    match raw.trim().to_ascii_lowercase().as_str() {
        "shadow" => FallbackGate::Shadow,
        "on" => FallbackGate::On,
        _ => FallbackGate::Off,
    }
}

fn parse_listen(raw: &str) -> Result<SocketAddr, Error> {
    raw.parse().map_err(|_| Error::InvalidListen {
        value: raw.to_owned(),
    })
}

fn non_empty(env: EnvLookup<'_>, key: &str) -> Option<String> {
    env(key).filter(|s| !s.is_empty())
}

fn overlay_path(
    env: EnvLookup<'_>,
    env_key: Option<&str>,
    file: Option<PathBuf>,
    default: PathBuf,
) -> PathBuf {
    if let Some(key) = env_key
        && let Some(v) = non_empty(env, key)
    {
        return PathBuf::from(v);
    }
    file.unwrap_or(default)
}

fn overlay_optional_path(
    env: EnvLookup<'_>,
    env_key: &str,
    file: Option<PathBuf>,
    default: Option<PathBuf>,
) -> Option<PathBuf> {
    if let Some(v) = non_empty(env, env_key) {
        return Some(PathBuf::from(v));
    }
    file.or(default)
}

#[cfg(test)]
mod tests {
    use super::resolve;
    use crate::error::Error;
    use crate::file::{MitosFile, Profile};
    use crate::network::{MAINNET_LISTEN, PREPROD_LISTEN};
    use std::collections::BTreeMap;

    fn lookup<'a>(map: &'a BTreeMap<&str, &str>) -> impl Fn(&str) -> Option<String> + 'a {
        |k| map.get(k).map(|s| (*s).to_owned())
    }

    fn parse(text: &str) -> MitosFile {
        MitosFile::parse(text).unwrap()
    }

    #[test]
    fn empty_env_uses_file_and_network_defaults() {
        let file = parse("network = \"mainnet\"\n");
        let empty = BTreeMap::new();
        let env = lookup(&empty);
        let r = resolve(&file, &env).unwrap();
        assert_eq!(r.listen, MAINNET_LISTEN);
        assert_eq!(r.data_dir, std::path::PathBuf::from("/var/lib/mitos"));
        assert_eq!(r.bundle_data_dir, r.data_dir.join("mitos-data"));
        assert_eq!(r.fallback.koios_network, "api");
        assert!(r.tx_index.is_none());
    }

    #[test]
    fn preprod_default_listen_is_8182() {
        let file = parse("network = \"preprod\"\n");
        let empty = BTreeMap::new();
        let env = lookup(&empty);
        let r = resolve(&file, &env).unwrap();
        assert_eq!(r.listen, PREPROD_LISTEN);
        assert_eq!(r.fallback.koios_network, "preprod");
    }

    #[test]
    fn env_overrides_listen_and_data_dir() {
        let file = parse("network = \"mainnet\"\nlisten = \"127.0.0.1:9999\"\n");
        let mut map = BTreeMap::new();
        map.insert(crate::env::BUNDLE_LISTEN, "0.0.0.0:8181");
        map.insert(crate::env::BUNDLE_DATA_DIR, "/opt/mitos/mainnet/mitos-data");
        let env = lookup(&map);
        let r = resolve(&file, &env).unwrap();
        assert_eq!(r.listen, "0.0.0.0:8181".parse().unwrap());
        assert_eq!(
            r.bundle_data_dir.as_os_str(),
            "/opt/mitos/mainnet/mitos-data"
        );
    }

    #[test]
    fn lean_is_refused_by_resolve() {
        let file = parse("network = \"mainnet\"\nprofile = \"lean\"\n");
        let empty = BTreeMap::new();
        let env = lookup(&empty);
        match resolve(&file, &env) {
            Err(Error::ProfileNotImplemented { profile }) => {
                assert_eq!(profile, Profile::Lean);
            }
            other => panic!("expected ProfileNotImplemented, got {other:?}"),
        }
    }

    #[test]
    fn tx_index_half_configured_is_refused() {
        let file = parse("network = \"mainnet\"\n");
        let mut map = BTreeMap::new();
        map.insert(crate::env::MITOS_TX_INDEX_DIR, "/opt/tx-index/mainnet");
        let env = lookup(&map);
        match resolve(&file, &env) {
            Err(Error::TxIndexHalfConfigured { which }) => {
                assert_eq!(which, crate::env::MITOS_TX_INDEX_DIR);
            }
            other => panic!("expected TxIndexHalfConfigured, got {other:?}"),
        }
    }

    #[test]
    fn tx_index_both_env_vars_enable_the_tier() {
        let file = parse("network = \"mainnet\"\n");
        let mut map = BTreeMap::new();
        map.insert(crate::env::MITOS_TX_INDEX_DIR, "/opt/tx-index/mainnet");
        map.insert(
            crate::env::MITOS_TX_INDEX_IMMUTABLE,
            "/opt/market-ledger/snapshot-full/db/immutable",
        );
        let env = lookup(&map);
        let r = resolve(&file, &env).unwrap();
        let tx = r.tx_index.expect("tier on");
        assert_eq!(tx.dir.as_os_str(), "/opt/tx-index/mainnet");
    }

    #[test]
    fn auth_token_env_is_flagged_not_stored() {
        let file = parse("network = \"mainnet\"\nauth_token_file = \"/etc/mitos/auth.token\"\n");
        let mut map = BTreeMap::new();
        map.insert(crate::env::MITOS_AUTH_TOKEN, "not-a-value-we-keep");
        let env = lookup(&map);
        let r = resolve(&file, &env).unwrap();
        assert!(r.auth_token_from_env);
        assert_eq!(
            r.auth_token_file.as_deref().map(|p| p.as_os_str()),
            Some(std::ffi::OsStr::new("/etc/mitos/auth.token"))
        );
    }
}
