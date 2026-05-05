//! `state-kv` interface — module-private KV state.
//!
//! Two backing modes:
//! - `InMemory` — `HashMap` for tests + dev. Lost on restart.
//! - `Redb` — vendored from Balius (`vendored::balius::kv::RedbKv`),
//!   redb-backed, per-module-keyed (`{module_id}-{key}`), durable
//!   across restarts.
//!
//! Module ID prefixing means multiple modules sharing the same
//! redb file cannot reach each other's state. V1 doesn't actually
//! share files (one module per host), but the prefixing is free
//! and forward-compatible.

use std::path::Path;

use crate::bindings::StateKvHost;
use crate::host_fns::HostState;
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
    /// optional (Balius default of 10_000 MiB applies if None;
    /// reasonable for v1 single-tenant). Errors propagate as
    /// `KvError`.
    pub fn open_redb(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, KvError> {
        Ok(Self::Redb(RedbKv::try_new(path, cache_size)?))
    }
}

impl StateKvHost for HostState {
    async fn get_value(&mut self, key: String) -> wasmtime::Result<Option<Vec<u8>>> {
        match &self.kv {
            ModuleKv::InMemory(map) => Ok(map.get(&key).cloned()),
            ModuleKv::Redb(redb) => match redb.get_value(&self.module_id, &key) {
                Ok(v) => Ok(Some(v)),
                Err(KvError::NotFound(_)) => Ok(None),
                Err(e) => Err(wasmtime::Error::msg(e.to_string())),
            },
        }
    }

    async fn set_value(&mut self, key: String, value: Vec<u8>) -> wasmtime::Result<()> {
        match &mut self.kv {
            ModuleKv::InMemory(map) => {
                map.insert(key, value);
                Ok(())
            }
            ModuleKv::Redb(redb) => redb
                .set_value(&self.module_id, &key, value)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }

    async fn delete_value(&mut self, key: String) -> wasmtime::Result<()> {
        match &mut self.kv {
            ModuleKv::InMemory(map) => {
                map.remove(&key);
                Ok(())
            }
            ModuleKv::Redb(redb) => redb
                .delete_value(&self.module_id, &key)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }
}
