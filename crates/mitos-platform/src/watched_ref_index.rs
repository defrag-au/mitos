//! Per-module index of the live UTxOs that match a module's
//! interest — the "watched-UTxO set."
//!
//! Design: `docs/design/WATCHED_UTXO_INDEX.md`.
//!
//! The platform already computes "does this output match interest"
//! for every `Produced` event during dispatch. This index simply
//! *remembers the answer*: every produced output that matched is
//! inserted (on the way in), every consumed ref is removed (on the
//! way out), and the live set at startup is seeded from the bootstrap
//! scan. Because a UTxO's content is immutable, "did it match when
//! produced" and "does it match now that it's consumed" are the same
//! question with the same answer — so membership here is an exact,
//! lookup-free substitute for re-resolving an old prior output just
//! to run the interest check.
//!
//! First consumer: [`crate::maestro_fallback_plane`] gates its
//! archive-horizon Maestro fallback on membership, so it only pays
//! for prior-output lookups that can actually match interest instead
//! of every chain-wide spend of a >7-day-old UTxO.
//!
//! ## Storage
//!
//! One redb file per module at `<module_dir>/watched.redb`, a single
//! `watched_refs` table keyed by the 36-byte `(tx_hash ++ index)`
//! encoding with an empty value (set semantics). A process-lifetime
//! in-memory `HashSet` mirror serves the hot `contains()` path with
//! no redb read; redb is only touched to persist deltas so the set
//! survives restart. Opened once per module and held (redb is
//! single-writer-per-process — a second open of the same file fails),
//! mirroring `cursor.redb` / `emissions.redb` / `kv.redb`.

use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};

use mitos_data_plane::OutputRef;
use redb::{ReadableTable, TableDefinition};

/// `(tx_hash ++ index) → ()` — set membership for watched output refs.
const WATCHED_TABLE: TableDefinition<'_, &[u8], ()> = TableDefinition::new("watched_refs");

#[derive(Debug, thiserror::Error)]
pub enum WatchedRefIndexError {
    #[error("redb: {0}")]
    Redb(String),
}

/// 36-byte key: 32-byte tx hash followed by the big-endian output
/// index. Fixed width keeps redb's ordering stable and lookups
/// allocation-free.
fn encode_key(oref: &OutputRef) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(oref.tx_hash.as_slice());
    k[32..].copy_from_slice(&oref.index.to_be_bytes());
    k
}

fn decode_key(bytes: &[u8]) -> Option<OutputRef> {
    if bytes.len() != 36 {
        return None;
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes[..32]);
    let index = u32::from_be_bytes(bytes[32..36].try_into().ok()?);
    Some(OutputRef::from_bytes(hash, index))
}

/// Persisted set of live output refs matching a module's interest.
///
/// `Clone` is not derived — callers share a single instance via
/// `Arc<WatchedRefIndex>` so the plane (reader) and the driver
/// (writer) observe the same mirror.
pub struct WatchedRefIndex {
    mem: RwLock<HashSet<OutputRef>>,
    /// `None` = in-memory only (tests, or a redb open failure where we
    /// degrade rather than refuse to start; the set re-seeds at the
    /// next bootstrap).
    db: Option<Arc<redb::Database>>,
}

impl WatchedRefIndex {
    /// Open (or create) the per-module index file and load its
    /// contents into the in-memory mirror. Safe to call once at
    /// startup and hold for the process lifetime.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WatchedRefIndexError> {
        // Sole open site for watched.redb. Routed through
        // `ModuleStorage::watched_index` which caches by module so the
        // single-writer lock isn't tripped by a second open.
        #[allow(clippy::disallowed_methods)]
        let db = redb::Database::builder()
            .create(path)
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        // Ensure the table exists so the initial read never fails with
        // "table does not exist."
        let wx = db
            .begin_write()
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        wx.open_table(WATCHED_TABLE)
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        wx.commit()
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;

        // Load the persisted set into the mirror.
        let mut mem = HashSet::new();
        let rx = db
            .begin_read()
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        let table = rx
            .open_table(WATCHED_TABLE)
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        let iter = table
            .iter()
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        for entry in iter {
            let (key, _) = entry.map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
            if let Some(oref) = decode_key(key.value()) {
                mem.insert(oref);
            }
        }

        Ok(Self {
            mem: RwLock::new(mem),
            db: Some(Arc::new(db)),
        })
    }

    /// In-memory-only index. Used by tests and as the graceful
    /// fallback when the redb file can't be opened.
    pub fn in_memory() -> Self {
        Self {
            mem: RwLock::new(HashSet::new()),
            db: None,
        }
    }

    /// Is this ref currently watched? Hot path — reads the mirror
    /// only, never redb.
    pub fn contains(&self, oref: &OutputRef) -> bool {
        self.mem
            .read()
            .map(|m| m.contains(oref))
            .unwrap_or(false)
    }

    /// Number of refs currently watched (mirror size).
    pub fn len(&self) -> usize {
        self.mem.read().map(|m| m.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Apply an insert/remove delta in a single redb transaction.
    /// The in-memory mirror is updated first (correctness within the
    /// process never depends on the redb write succeeding); a redb
    /// failure is logged and means at most a re-seed of those refs at
    /// the next bootstrap.
    pub fn apply(&self, insert: &[OutputRef], remove: &[OutputRef]) {
        if insert.is_empty() && remove.is_empty() {
            return;
        }
        if let Ok(mut mem) = self.mem.write() {
            for oref in insert {
                mem.insert(*oref);
            }
            for oref in remove {
                mem.remove(oref);
            }
        }
        if let Some(db) = &self.db
            && let Err(e) = persist(db, insert, remove)
        {
            tracing::warn!("watched_ref_index persist failed: {e}");
        }
    }

    /// Seed the index with refs known to match interest (the
    /// bootstrap / recapture live-UTxO scan). Insert-only; safe to
    /// call repeatedly (idempotent set insert).
    pub fn seed(&self, refs: impl IntoIterator<Item = OutputRef>) {
        let batch: Vec<OutputRef> = refs.into_iter().collect();
        self.apply(&batch, &[]);
    }
}

fn persist(
    db: &redb::Database,
    insert: &[OutputRef],
    remove: &[OutputRef],
) -> Result<(), WatchedRefIndexError> {
    let wx = db
        .begin_write()
        .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
    {
        let mut table = wx
            .open_table(WATCHED_TABLE)
            .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        for oref in insert {
            table
                .insert(encode_key(oref).as_slice(), ())
                .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        }
        for oref in remove {
            table
                .remove(encode_key(oref).as_slice())
                .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
        }
    }
    wx.commit()
        .map_err(|e| WatchedRefIndexError::Redb(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_primitives::Hash;

    fn oref(byte: u8, index: u32) -> OutputRef {
        OutputRef::new(Hash::new([byte; 32]), index)
    }

    #[test]
    fn in_memory_insert_remove_contains() {
        let idx = WatchedRefIndex::in_memory();
        let a = oref(0x01, 0);
        let b = oref(0x02, 3);
        assert!(!idx.contains(&a));
        idx.apply(&[a, b], &[]);
        assert!(idx.contains(&a));
        assert!(idx.contains(&b));
        assert_eq!(idx.len(), 2);
        idx.apply(&[], &[a]);
        assert!(!idx.contains(&a));
        assert!(idx.contains(&b));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn key_codec_round_trips() {
        let o = oref(0xAB, 7);
        assert_eq!(decode_key(&encode_key(&o)), Some(o));
        assert_eq!(decode_key(&[0u8; 35]), None);
    }

    #[test]
    fn redb_round_trips_across_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "watched_ref_index_test_{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("watched.redb");
        let _ = std::fs::remove_file(&path);

        let a = oref(0x10, 1);
        let b = oref(0x20, 2);
        {
            let idx = WatchedRefIndex::open(&path).expect("open");
            idx.seed([a, b]);
            idx.apply(&[], &[a]); // drop a, keep b
            assert!(idx.contains(&b));
            assert!(!idx.contains(&a));
        }
        // Reopen: mirror reloads from redb.
        let idx = WatchedRefIndex::open(&path).expect("reopen");
        assert!(idx.contains(&b));
        assert!(!idx.contains(&a));
        assert_eq!(idx.len(), 1);

        let _ = std::fs::remove_file(&path);
    }
}
