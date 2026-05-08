//! Shared `state-kv` store types — module-private KV state.
//!
//! Two backing modes:
//! - `InMemory` — `HashMap` for tests + dev. Lost on restart.
//! - `Redb` — vendored from Balius (`vendored::balius::kv::RedbKv`),
//!   redb-backed, per-module-keyed (`{module_id}-{key}`), durable
//!   across restarts.
//!
//! The v2 `StateKvHost` impl lives in `host_fns_v2/state_kv.rs`;
//! this module only carries the store enum the v2 impl + bundle
//! both consume.

use std::path::Path;

use crate::vendored::balius::kv::{KvError, RedbKv};

pub enum ModuleKv {
    InMemory(std::collections::HashMap<String, Vec<u8>>),
    Redb(RedbKv),
}

impl ModuleKv {
    pub fn new_in_memory() -> Self {
        Self::InMemory(std::collections::HashMap::new())
    }

    /// Open or create a redb file at `path`. Cache size is
    /// optional (Balius default of 10_000 MiB applies if None).
    pub fn open_redb(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, KvError> {
        Ok(Self::Redb(RedbKv::try_new(path, cache_size)?))
    }
}
