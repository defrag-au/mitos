//! v2 `state-kv` interface — same semantics as v1, plumbed
//! against v2 bindings. Reuses v1's `ModuleKv` store type
//! (one redb file per module, namespaced keys).

use crate::bindings_v2::StateKvHost;
use crate::host_fns::state_kv::ModuleKv;
use crate::host_fns_v2::HostStateV2;
use crate::vendored::balius::kv::KvError;

impl StateKvHost for HostStateV2 {
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
