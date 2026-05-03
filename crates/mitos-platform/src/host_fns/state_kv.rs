//! `state-kv` interface — module-private KV state backed by a
//! per-module redb table.
//!
//! V1: one module → one table → one redb file (or one table
//! inside a shared file, TBD during impl). Keys are
//! namespaced under the module ID; modules cannot reach each
//! other's state.
//!
//! Will be backed by the vendored `kv/redb.rs` from Balius (see
//! `vendored/balius/`); for now this stub keeps the host trait
//! impl shape so the rest of the crate compiles.

use crate::bindings::StateKvHost;
use crate::host_fns::HostState;

/// Per-module KV handle. V1 stub — wraps an in-memory map until
/// the vendored redb-backed impl lands. See
/// `vendored/balius/README.md` for the vendoring plan.
pub struct ModuleKv {
    inner: std::collections::HashMap<String, Vec<u8>>,
}

impl ModuleKv {
    pub fn new_in_memory() -> Self {
        Self {
            inner: std::collections::HashMap::new(),
        }
    }
}

impl StateKvHost for HostState {
    async fn get_value(&mut self, key: String) -> wasmtime::Result<Option<Vec<u8>>> {
        Ok(self.kv.inner.get(&key).cloned())
    }

    async fn set_value(&mut self, key: String, value: Vec<u8>) -> wasmtime::Result<()> {
        self.kv.inner.insert(key, value);
        Ok(())
    }

    async fn delete_value(&mut self, key: String) -> wasmtime::Result<()> {
        self.kv.inner.remove(&key);
        Ok(())
    }
}
