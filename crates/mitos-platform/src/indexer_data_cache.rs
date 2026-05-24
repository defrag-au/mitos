//! Platform-wide indexer-data cache.
//!
//! A persistent redb store for hash-addressable chain facts that
//! outlive the dolos archive window. Two namespaces share one
//! file at `<storage_root>/indexer_data.redb`:
//!
//! - **`aux_data`** (tx_hash → aux CBOR): transaction
//!   auxiliary-data (CIP-25 metadata, labels-50+ datum bytes).
//!   The dolos archive prunes it past the ~7-day horizon; this
//!   cache keeps it indefinitely.
//! - **`datums`** (datum_hash → datum CBOR): Plutus datum
//!   preimages. dolos keeps these in its refcounted `DATUM_NS`
//!   index only while a referencing UTxO is unspent *and* the
//!   witnessing block was replayed — snapshot-bootstrapped nodes
//!   miss the preimages for pre-snapshot ref UTxOs (the CIP-68
//!   hash-only-datum gap). Maestro backfills the miss; this cache
//!   makes that a one-time cost per datum.
//!
//! Population paths:
//! - aux_data: **proactive** (every applied block is scanned;
//!   any TX with aux_data gets an entry — free, the block CBOR is
//!   already in memory) + **write-through** on both resolution
//!   tiers of `CachingDataPlane::tx_metadata` (the dolos archive
//!   hit *and* the Maestro fallback).
//! - datums: **write-through** on both resolution tiers of
//!   `CachingDataPlane::datum_by_hash` — the dolos `DATUM_NS` hit
//!   *and* the Maestro fallback. There's no proactive per-block
//!   harvest (unlike aux_data) because live block apply already
//!   populates dolos `DATUM_NS`; the cache only needs to capture
//!   datums as they're resolved. Caching the `DATUM_NS` hit (not
//!   just the Maestro miss) is deliberate: a datum then survives
//!   here even after dolos drops it from its refcounted index when
//!   the last referencing UTxO is spent — the same "outlive
//!   pruning" guarantee aux_data gets.
//!
//! **Thread safety**: `Arc<redb::Database>` internally; `Clone`
//! is cheap (one Arc increment). Concurrent reads via separate
//! read transactions are fine; concurrent writes are serialised
//! by redb's single-writer lock with no external coordination.
//! The two tables share that one writer lock — fine, since both
//! population paths write infrequently (per-block batch / cache
//! miss).

use std::path::Path;
use std::sync::Arc;

use redb::TableDefinition;

/// `tx_hash → aux_data CBOR`.
const AUX_TABLE: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("aux_data");
/// `datum_hash → datum CBOR`.
const DATUM_TABLE: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("datums");

#[derive(Debug, thiserror::Error)]
pub enum IndexerDataCacheError {
    #[error("redb: {0}")]
    Redb(String),
}

/// Persistent cache of hash-addressable chain facts shared across
/// all modules. Stored at `<storage_root>/indexer_data.redb`
/// (platform-wide, not per-module — these are chain facts, not
/// module state).
#[derive(Clone)]
pub struct IndexerDataCache {
    db: Arc<redb::Database>,
}

impl IndexerDataCache {
    /// Open (or create) the cache file. Safe to call once at
    /// startup and hold for the process lifetime.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, IndexerDataCacheError> {
        // Sole open site for indexer_data.redb. Routed through
        // `ModuleStorage::indexer_data_cache` which caches by path;
        // see clippy.toml for the workspace lint.
        #[allow(clippy::disallowed_methods)]
        let db = redb::Database::builder()
            .create(path)
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        // Ensure both tables exist on first open so reads never
        // fail with "table does not exist."
        let wx = db
            .begin_write()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        wx.open_table(AUX_TABLE)
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        wx.open_table(DATUM_TABLE)
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        wx.commit()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    // ----- aux_data namespace (tx_hash → aux CBOR) -----

    /// Look up aux_cbor by TX hash. Returns `None` on miss. DB
    /// errors are silently mapped to `None` — a cache miss
    /// degrades gracefully to the archive / Maestro fallback.
    pub fn get_aux(&self, tx_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.get_in(AUX_TABLE, tx_hash)
    }

    /// Write a single aux_data entry. Cache-write failures are
    /// logged as warnings — a miss on next access is the only
    /// consequence.
    pub fn insert_aux(&self, tx_hash: &[u8; 32], aux_cbor: &[u8]) {
        if let Err(e) = self.insert_in(AUX_TABLE, tx_hash, aux_cbor) {
            tracing::warn!("indexer_data_cache aux insert failed: {e}");
        }
    }

    /// Write multiple aux_data entries in a single transaction.
    /// Used during block apply to batch all aux_data entries from
    /// one block. No-op when `entries` is empty.
    pub fn insert_aux_batch(&self, entries: &[([u8; 32], Vec<u8>)]) {
        if entries.is_empty() {
            return;
        }
        if let Err(e) = self.insert_batch_in(AUX_TABLE, entries) {
            tracing::warn!("indexer_data_cache aux insert_batch failed: {e}");
        }
    }

    // ----- datums namespace (datum_hash → datum CBOR) -----

    /// Look up datum CBOR by datum hash. Returns `None` on miss.
    pub fn get_datum(&self, datum_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.get_in(DATUM_TABLE, datum_hash)
    }

    /// Write a single datum entry. Cache-write failures are
    /// logged as warnings — a miss on next access just re-fetches
    /// from Maestro.
    pub fn insert_datum(&self, datum_hash: &[u8; 32], datum_cbor: &[u8]) {
        if let Err(e) = self.insert_in(DATUM_TABLE, datum_hash, datum_cbor) {
            tracing::warn!("indexer_data_cache datum insert failed: {e}");
        }
    }

    // ----- shared redb helpers -----

    fn get_in(&self, table: TableDefinition<'_, &[u8], &[u8]>, key: &[u8; 32]) -> Option<Vec<u8>> {
        let rx = self.db.begin_read().ok()?;
        let table = rx.open_table(table).ok()?;
        table.get(key.as_slice()).ok()?.map(|v| v.value().to_vec())
    }

    fn insert_in(
        &self,
        table: TableDefinition<'_, &[u8], &[u8]>,
        key: &[u8; 32],
        value: &[u8],
    ) -> Result<(), IndexerDataCacheError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        {
            let mut table = wx
                .open_table(table)
                .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
            table
                .insert(key.as_slice(), value)
                .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        }
        wx.commit()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        Ok(())
    }

    fn insert_batch_in(
        &self,
        table: TableDefinition<'_, &[u8], &[u8]>,
        entries: &[([u8; 32], Vec<u8>)],
    ) -> Result<(), IndexerDataCacheError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        {
            let mut table = wx
                .open_table(table)
                .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
            for (key, value) in entries {
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
            }
        }
        wx.commit()
            .map_err(|e| IndexerDataCacheError::Redb(e.to_string()))?;
        Ok(())
    }
}
