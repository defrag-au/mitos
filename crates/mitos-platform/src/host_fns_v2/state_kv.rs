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

    async fn list_values(&mut self, prefix: String) -> wasmtime::Result<Vec<String>> {
        match &self.kv {
            // InMemory stores logical keys directly (no namespace
            // prefix). Filter + sort to match redb's ordered range.
            ModuleKv::InMemory(map) => {
                let mut keys: Vec<String> = map
                    .keys()
                    .filter(|k| k.starts_with(&prefix))
                    .cloned()
                    .collect();
                keys.sort();
                Ok(keys)
            }
            // RedbKv::list_values returns full `{module_id}-{key}`
            // storage keys in sorted order; strip the namespace so
            // the module sees its own logical keys.
            ModuleKv::Redb(redb) => {
                let namespace = format!("{}-", self.module_id);
                let logical = redb
                    .list_values(&self.module_id, &prefix)
                    .map_err(|e| wasmtime::Error::msg(e.to_string()))?
                    .into_iter()
                    .map(|k| k.strip_prefix(&namespace).map(String::from).unwrap_or(k))
                    .collect();
                Ok(logical)
            }
        }
    }

    async fn kv_scan(
        &mut self,
        prefix: String,
        after: Option<String>,
        limit: u32,
    ) -> wasmtime::Result<Vec<(String, Vec<u8>)>> {
        match &self.kv {
            // InMemory stores logical keys. Filter (prefix + > after),
            // sort, truncate — matching redb's ordered range semantics.
            ModuleKv::InMemory(map) => {
                let mut pairs: Vec<(String, Vec<u8>)> = map
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .filter(|(k, _)| after.as_deref().map(|a| k.as_str() > a).unwrap_or(true))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs.truncate(limit as usize);
                Ok(pairs)
            }
            ModuleKv::Redb(redb) => redb
                .scan(&self.module_id, &prefix, after.as_deref(), limit as usize)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }

    async fn get_many(&mut self, keys: Vec<String>) -> wasmtime::Result<Vec<Option<Vec<u8>>>> {
        match &self.kv {
            ModuleKv::InMemory(map) => Ok(keys.iter().map(|k| map.get(k).cloned()).collect()),
            ModuleKv::Redb(redb) => redb
                .get_many(&self.module_id, &keys)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }

    async fn set_many(&mut self, entries: Vec<(String, Vec<u8>)>) -> wasmtime::Result<()> {
        match &mut self.kv {
            ModuleKv::InMemory(map) => {
                for (k, v) in entries {
                    map.insert(k, v);
                }
                Ok(())
            }
            ModuleKv::Redb(redb) => redb
                .set_many(&self.module_id, &entries)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }

    async fn delete_prefix(&mut self, prefix: String) -> wasmtime::Result<()> {
        match &mut self.kv {
            ModuleKv::InMemory(map) => {
                map.retain(|k, _| !k.starts_with(&prefix));
                Ok(())
            }
            ModuleKv::Redb(redb) => redb
                .delete_prefix(&self.module_id, &prefix)
                .map_err(|e| wasmtime::Error::msg(e.to_string())),
        }
    }
}
