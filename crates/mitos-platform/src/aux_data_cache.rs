//! Platform-wide auxiliary-data cache.
//!
//! TX auxiliary-data CBOR (labels-50+ datum bytes, etc.) is
//! keyed by the 32-byte TX hash. The cache outlives the dolos
//! archive window — once an entry is written it persists
//! indefinitely in the redb file at `<storage_root>/aux_data.redb`.
//!
//! Two population paths:
//! - **Proactive** (follower): every applied block is scanned;
//!   any TX with aux_data gets an entry immediately — free,
//!   since the block CBOR is already in memory.
//! - **Write-through** (host): `CachingDataPlane::tx_metadata`
//!   writes back on archive hits so future bootstraps don't
//!   need to re-query dolos (or Maestro in Phase 2).
//!
//! **Thread safety**: `Arc<redb::Database>` internally; `Clone`
//! is cheap (one Arc increment). Concurrent reads via separate
//! read transactions are fine; concurrent writes are serialised
//! by redb's single-writer lock with no external coordination.

use std::path::Path;
use std::sync::Arc;

use redb::TableDefinition;

const TABLE: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("aux_data");

#[derive(Debug, thiserror::Error)]
pub enum AuxDataCacheError {
    #[error("redb: {0}")]
    Redb(String),
}

/// Persistent tx_hash → aux_cbor cache shared across all modules.
/// Stored at `<storage_root>/aux_data.redb` (platform-wide, not
/// per-module — aux_data is a chain fact, not module state).
#[derive(Clone)]
pub struct AuxDataCache {
    db: Arc<redb::Database>,
}

impl AuxDataCache {
    /// Open (or create) the cache file. Safe to call once at
    /// startup and hold for the process lifetime.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, AuxDataCacheError> {
        // Sole open site for aux_data.redb. Routed through
        // `ModuleStorage::aux_data_cache` which caches by path;
        // see clippy.toml for the workspace lint.
        #[allow(clippy::disallowed_methods)]
        let db = redb::Database::builder()
            .create(path)
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        // Ensure the table exists on first open so reads never
        // fail with "table does not exist."
        let wx = db
            .begin_write()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        wx.open_table(TABLE)
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        wx.commit()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Look up aux_cbor by TX hash. Returns `None` on miss.
    /// DB errors are silently mapped to `None` — a cache miss
    /// degrades gracefully to the archive / Maestro fallback.
    pub fn get(&self, tx_hash: &[u8; 32]) -> Option<Vec<u8>> {
        let rx = self.db.begin_read().ok()?;
        let table = rx.open_table(TABLE).ok()?;
        table
            .get(tx_hash.as_slice())
            .ok()?
            .map(|v| v.value().to_vec())
    }

    /// Write a single entry. Cache-write failures are logged as
    /// warnings — a miss on next access is the only consequence.
    pub fn insert(&self, tx_hash: &[u8; 32], aux_cbor: &[u8]) {
        if let Err(e) = self.try_insert(tx_hash, aux_cbor) {
            tracing::warn!("aux_data_cache insert failed: {e}");
        }
    }

    fn try_insert(&self, tx_hash: &[u8; 32], aux_cbor: &[u8]) -> Result<(), AuxDataCacheError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        {
            let mut table = wx
                .open_table(TABLE)
                .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
            table
                .insert(tx_hash.as_slice(), aux_cbor)
                .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        }
        wx.commit()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        Ok(())
    }

    /// Write multiple entries in a single transaction. Used during
    /// block apply to batch all aux_data entries from one block.
    /// No-op when `entries` is empty.
    pub fn insert_batch(&self, entries: &[([u8; 32], Vec<u8>)]) {
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.try_insert_batch(entries) {
            tracing::warn!("aux_data_cache insert_batch failed: {e}");
        }
    }

    fn try_insert_batch(
        &self,
        entries: &[([u8; 32], Vec<u8>)],
    ) -> Result<(), AuxDataCacheError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        {
            let mut table = wx
                .open_table(TABLE)
                .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
            for (hash, cbor) in entries {
                table
                    .insert(hash.as_slice(), cbor.as_slice())
                    .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
            }
        }
        wx.commit()
            .map_err(|e| AuxDataCacheError::Redb(e.to_string()))?;
        Ok(())
    }
}
