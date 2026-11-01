//! Env var names the units set today. The resolver takes a lookup
//! function over these names; it never reads `std::env`.

/// Path to `mitos.toml`. Host-only: this crate does not open files.
pub const MITOS_CONFIG: &str = "MITOS_CONFIG";

/// Default on-disk location when `MITOS_CONFIG` is unset and `--config-file`
/// is not passed. Presence of this file is what switches the binary off
/// today's env/`dolos.toml` path.
pub const DEFAULT_MITOS_TOML: &str = "/etc/mitos/mitos.toml";

pub const BUNDLE_LISTEN: &str = "BUNDLE_LISTEN";
pub const BUNDLE_DATA_DIR: &str = "BUNDLE_DATA_DIR";
pub const BUNDLE_MODULES_DIR: &str = "BUNDLE_MODULES_DIR";
pub const BUNDLE_COMMUNITY_MODULES_DIR: &str = "BUNDLE_COMMUNITY_MODULES_DIR";

pub const MITOS_AUTH_TOKEN: &str = "MITOS_AUTH_TOKEN";
pub const MITOS_READONLY_TOKEN: &str = "MITOS_READONLY_TOKEN";
pub const MITOS_NAMESPACES_FILE: &str = "MITOS_NAMESPACES_FILE";

pub const MITOS_TX_INDEX_DIR: &str = "MITOS_TX_INDEX_DIR";
pub const MITOS_TX_INDEX_IMMUTABLE: &str = "MITOS_TX_INDEX_IMMUTABLE";

pub const MITOS_FALLBACK_PROVIDER: &str = "MITOS_FALLBACK_PROVIDER";
pub const MITOS_FALLBACK_GATE: &str = "MITOS_FALLBACK_GATE";
pub const KOIOS_NETWORK: &str = "KOIOS_NETWORK";
pub const KOIOS_API_KEY: &str = "KOIOS_API_KEY";

/// WO-F5: directory that CONTAINS `immutable/`.
pub const MITOS_CHUNK_STORE: &str = "MITOS_CHUNK_STORE";

/// Legacy. Only used when no `mitos.toml` is in play; the host keeps today's
/// `load_config(DOLOS_CONFIG)` path in that case.
pub const DOLOS_CONFIG: &str = "DOLOS_CONFIG";

/// Every env var a production unit file actually sets (plus the two this
/// crate adds: `MITOS_CONFIG`, `MITOS_CHUNK_STORE`). Used by tests so a
/// newly-documented knob cannot silently miss the resolver.
pub const UNIT_ENV_KEYS: &[&str] = &[
    MITOS_AUTH_TOKEN,
    MITOS_READONLY_TOKEN,
    DOLOS_CONFIG,
    BUNDLE_LISTEN,
    BUNDLE_DATA_DIR,
    BUNDLE_MODULES_DIR,
    BUNDLE_COMMUNITY_MODULES_DIR,
    MITOS_FALLBACK_PROVIDER,
    KOIOS_NETWORK,
    MITOS_FALLBACK_GATE,
    MITOS_TX_INDEX_DIR,
    MITOS_TX_INDEX_IMMUTABLE,
    KOIOS_API_KEY,
    MITOS_NAMESPACES_FILE,
];
