// Vendored from github.com/txpipe/balius @ e9c8cd50c7d7074dc6db75633bb26c68d41da187
// Original path: balius-runtime/src/kv/redb.rs
// Apache-2.0 — see LICENSE-APACHE-2.0 (vendored alongside in this directory)
//
// Local modifications:
// - Removed `KvProvider` trait impl and the `super::KvProvider`
//   import (we don't have that trait; mitos calls the inherent
//   methods directly from `state_kv::ModuleKv::Redb`).
// - Replaced `KvError` (Balius's WIT-generated type) with a
//   local `KvError` enum carrying the same NotFound + Internal
//   distinctions; `Payload` (Balius alias for `Vec<u8>`) is now
//   spelled `Vec<u8>` directly.
// - Replaced `crate::Error::KvError(...)` mapping with our local
//   `KvError::Internal(...)` since we don't share Balius's
//   top-level error enum.
// - Removed `async_trait::async_trait` decoration on the impl
//   (we use native async fn — bindgen-generated trait shape
//   matches that).
// - Methods are now inherent on `RedbKv`, not trait-method.
// - `into_ephemeral` retained verbatim — useful for in-memory
//   mode when a write-mode db is needed without a real file.
// - Renamed `worker_id` to `module_id` in public APIs for
//   internal consistency; storage layout (`{id}-{key}` keys) is
//   unchanged so a Balius-format db round-trips.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, Durability, ReadableTable, TableDefinition};
use tracing::warn;

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("kv key not found: {0}")]
    NotFound(String),
    #[error("kv internal error: {0}")]
    Internal(String),
}

#[derive(Clone)]
pub struct RedbKv {
    db: Arc<Database>,
}

impl RedbKv {
    pub const DEF: TableDefinition<'static, String, Vec<u8>> = TableDefinition::new("kv");

    pub fn try_new(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, KvError> {
        // Sole open site for kv.redb. Routed exclusively through
        // `ModuleStorage::kv_store` which caches by path; see
        // clippy.toml for the workspace lint.
        #[allow(clippy::disallowed_methods)]
        let db = Database::builder()
            .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "db is repairing"))
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(10_000))
            .create(path)
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let mut wx = db
            .begin_write()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        wx.set_durability(Durability::Immediate);
        wx.open_table(Self::DEF)
            .map_err(|err| KvError::Internal(err.to_string()))?;
        wx.commit()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn key_for_module(module_id: &str, key: &str) -> String {
        format!("{module_id}-{key}")
    }

    pub fn into_ephemeral(&mut self) -> Result<Self, KvError> {
        // In-memory backend — no file lock; the workspace lint
        // is about preventing duplicate file opens, not in-memory
        // databases. Allow locally.
        #[allow(clippy::disallowed_methods)]
        let new_db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| KvError::Internal(e.to_string()))?;

        let rx = self
            .db
            .begin_read()
            .map_err(|e| KvError::Internal(e.to_string()))?;
        let wx = new_db
            .begin_write()
            .map_err(|e| KvError::Internal(e.to_string()))?;

        {
            if let Ok(source) = rx.open_table(Self::DEF) {
                let mut target = wx
                    .open_table(Self::DEF)
                    .map_err(|e| KvError::Internal(e.to_string()))?;

                for entry in source
                    .iter()
                    .map_err(|e| KvError::Internal(e.to_string()))?
                {
                    let (k, v) = entry.map_err(|e| KvError::Internal(e.to_string()))?;
                    target
                        .insert(k.value(), v.value())
                        .map_err(|e| KvError::Internal(e.to_string()))?;
                }
            };
        }

        wx.commit().map_err(|e| KvError::Internal(e.to_string()))?;

        Ok(Self {
            db: Arc::new(new_db),
        })
    }

    pub fn get_value(&self, module_id: &str, key: &str) -> Result<Vec<u8>, KvError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let table = rx
            .open_table(Self::DEF)
            .map_err(|err| KvError::Internal(err.to_string()))?;
        match table
            .get(Self::key_for_module(module_id, key))
            .map_err(|err| KvError::Internal(err.to_string()))?
        {
            Some(value) => Ok(value.value()),
            None => Err(KvError::NotFound(key.to_owned())),
        }
    }

    pub fn set_value(&self, module_id: &str, key: &str, value: Vec<u8>) -> Result<(), KvError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        {
            let mut table = wx
                .open_table(Self::DEF)
                .map_err(|err| KvError::Internal(err.to_string()))?;

            table
                .insert(Self::key_for_module(module_id, key), value)
                .map_err(|err| KvError::Internal(err.to_string()))?;
        }

        wx.commit()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        Ok(())
    }

    pub fn delete_value(&self, module_id: &str, key: &str) -> Result<(), KvError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        {
            let mut table = wx
                .open_table(Self::DEF)
                .map_err(|err| KvError::Internal(err.to_string()))?;

            table
                .remove(Self::key_for_module(module_id, key))
                .map_err(|err| KvError::Internal(err.to_string()))?;
        }

        wx.commit()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        Ok(())
    }

    pub fn list_values(&self, module_id: &str, prefix: &str) -> Result<Vec<String>, KvError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let table = rx
            .open_table(Self::DEF)
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let mut result = vec![];
        let range = table
            .range(Self::key_for_module(module_id, prefix)..)
            .map_err(|err| KvError::Internal(err.to_string()))?;

        for item in range {
            let (k, _) = item.map_err(|err| KvError::Internal(err.to_string()))?;
            if k.value()
                .starts_with(&Self::key_for_module(module_id, prefix))
            {
                result.push(k.value());
            } else {
                break;
            }
        }
        Ok(result)
    }

    // ── mitos additions: batched / paginated ops for sharded module
    // state at scale (see docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md).
    // Not part of the vendored upstream.

    /// Paginated `(logical_key, value)` range scan under `prefix`,
    /// keys sorted, `after` (a logical key) **exclusive**, up to
    /// `limit` rows. Values are returned alongside keys (redb range
    /// yields both) so a chunked emit needs no follow-up gets.
    /// Logical keys have the `{module_id}-` namespace stripped.
    pub fn scan(
        &self,
        module_id: &str,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, KvError> {
        use std::ops::Bound;
        let rx = self
            .db
            .begin_read()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        let table = rx
            .open_table(Self::DEF)
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let storage_prefix = Self::key_for_module(module_id, prefix);
        let lower = match after {
            Some(a) => Bound::Excluded(Self::key_for_module(module_id, a)),
            None => Bound::Included(storage_prefix.clone()),
        };
        let range = table
            .range::<String>((lower, Bound::Unbounded))
            .map_err(|err| KvError::Internal(err.to_string()))?;

        let namespace = Self::key_for_module(module_id, "");
        let mut out = Vec::with_capacity(limit.min(1024));
        for item in range {
            if out.len() >= limit {
                break;
            }
            let (k, v) = item.map_err(|err| KvError::Internal(err.to_string()))?;
            let full = k.value();
            if !full.starts_with(&storage_prefix) {
                break;
            }
            let logical = full
                .strip_prefix(&namespace)
                .map(String::from)
                .unwrap_or(full);
            out.push((logical, v.value()));
        }
        Ok(out)
    }

    /// Batched random-access get — one read txn for many keys.
    /// Result is parallel to `keys` (`None` ⇒ absent).
    pub fn get_many(
        &self,
        module_id: &str,
        keys: &[String],
    ) -> Result<Vec<Option<Vec<u8>>>, KvError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        let table = rx
            .open_table(Self::DEF)
            .map_err(|err| KvError::Internal(err.to_string()))?;
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let v = table
                .get(Self::key_for_module(module_id, key))
                .map_err(|err| KvError::Internal(err.to_string()))?
                .map(|g| g.value());
            out.push(v);
        }
        Ok(out)
    }

    /// Batched write — one write txn (one fsync) for many keys.
    pub fn set_many(
        &self,
        module_id: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), KvError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        {
            let mut table = wx
                .open_table(Self::DEF)
                .map_err(|err| KvError::Internal(err.to_string()))?;
            for (key, value) in entries {
                table
                    .insert(Self::key_for_module(module_id, key), value.clone())
                    .map_err(|err| KvError::Internal(err.to_string()))?;
            }
        }
        wx.commit()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        Ok(())
    }

    /// Delete every key under `prefix` in one write txn.
    pub fn delete_prefix(&self, module_id: &str, prefix: &str) -> Result<(), KvError> {
        // `list_values` returns full storage keys (`{module_id}-{logical}`).
        let storage_keys = self.list_values(module_id, prefix)?;
        if storage_keys.is_empty() {
            return Ok(());
        }
        let wx = self
            .db
            .begin_write()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        {
            let mut table = wx
                .open_table(Self::DEF)
                .map_err(|err| KvError::Internal(err.to_string()))?;
            for full_key in storage_keys {
                table
                    .remove(full_key)
                    .map_err(|err| KvError::Internal(err.to_string()))?;
            }
        }
        wx.commit()
            .map_err(|err| KvError::Internal(err.to_string()))?;
        Ok(())
    }
}

// Tests below are mitos-side additions, not part of the
// vendored upstream content.
#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir_path(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "mitos-platform-kv-test-{}-{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn round_trip_get_set() {
        let path = tempdir_path("round_trip");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        kv.set_value("mod-a", "key1", b"hello".to_vec()).unwrap();
        let got = kv.get_value("mod-a", "key1").unwrap();
        assert_eq!(got, b"hello");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn module_id_isolation() {
        let path = tempdir_path("isolation");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        kv.set_value("mod-a", "shared", b"a-value".to_vec())
            .unwrap();
        kv.set_value("mod-b", "shared", b"b-value".to_vec())
            .unwrap();
        assert_eq!(kv.get_value("mod-a", "shared").unwrap(), b"a-value");
        assert_eq!(kv.get_value("mod-b", "shared").unwrap(), b"b-value");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_key_returns_not_found() {
        let path = tempdir_path("missing");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        let err = kv.get_value("mod-a", "nope").unwrap_err();
        assert!(matches!(err, KvError::NotFound(_)));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_then_get_returns_not_found() {
        let path = tempdir_path("delete");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        kv.set_value("mod-a", "k", b"v".to_vec()).unwrap();
        kv.delete_value("mod-a", "k").unwrap();
        assert!(matches!(
            kv.get_value("mod-a", "k").unwrap_err(),
            KvError::NotFound(_)
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn set_many_get_many_round_trip() {
        let path = tempdir_path("set_get_many");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        kv.set_many(
            "mod-a",
            &[
                ("p:1".into(), b"one".to_vec()),
                ("p:2".into(), b"two".to_vec()),
            ],
        )
        .unwrap();
        let got = kv
            .get_many("mod-a", &["p:2".into(), "p:missing".into(), "p:1".into()])
            .unwrap();
        assert_eq!(
            got,
            vec![Some(b"two".to_vec()), None, Some(b"one".to_vec())]
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn scan_paginates_sorted_after_exclusive() {
        let path = tempdir_path("scan");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        // Two prefixes + another module sharing key tails.
        kv.set_many(
            "mod-a",
            &[
                ("e:b".into(), b"B".to_vec()),
                ("e:a".into(), b"A".to_vec()),
                ("e:c".into(), b"C".to_vec()),
                ("other:z".into(), b"Z".to_vec()),
            ],
        )
        .unwrap();
        kv.set_value("mod-b", "e:a", b"OTHER-MOD".to_vec()).unwrap();

        // First page: sorted, prefix-scoped, module-scoped.
        let page1 = kv.scan("mod-a", "e:", None, 2).unwrap();
        assert_eq!(
            page1,
            vec![("e:a".into(), b"A".to_vec()), ("e:b".into(), b"B".to_vec())]
        );
        // Next page: after the last key, exclusive.
        let page2 = kv.scan("mod-a", "e:", Some("e:b"), 2).unwrap();
        assert_eq!(page2, vec![("e:c".into(), b"C".to_vec())]);
        // Exhausted.
        assert!(kv.scan("mod-a", "e:", Some("e:c"), 2).unwrap().is_empty());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_prefix_clears_only_matching() {
        let path = tempdir_path("delete_prefix");
        let kv = RedbKv::try_new(&path, Some(1)).unwrap();
        kv.set_many(
            "mod-a",
            &[
                ("e:1".into(), b"1".to_vec()),
                ("e:2".into(), b"2".to_vec()),
                ("keep:1".into(), b"k".to_vec()),
            ],
        )
        .unwrap();
        kv.delete_prefix("mod-a", "e:").unwrap();
        assert!(kv.scan("mod-a", "e:", None, 10).unwrap().is_empty());
        assert_eq!(kv.get_value("mod-a", "keep:1").unwrap(), b"k");
        let _ = std::fs::remove_file(&path);
    }
}
