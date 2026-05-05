//! Per-event handler context.
//!
//! `Ctx` is what the runtime hands to each `MitosChannel::apply_event`
//! call. It exposes the synchronous `sql.exec` interface the dApp
//! needs to write rows inside the output-gate window (see Q3 of the
//! design doc's "Atomicity: output-gate" section), plus a logger
//! and the chain point of the event being applied for diagnostics.

use serde::de::DeserializeOwned;
use worker::SqlStorage;
pub use worker::SqlStorageValue;

use crate::error::{CompanionError, Result};
use crate::wire::ChainPoint;

/// Handler context. Carries the synchronous SQL surface + diagnostic
/// metadata.
///
/// Constructed by the runtime once per Apply frame; passed to the
/// channel's `apply_event`. Owned (not borrowed) for ergonomic trait
/// signatures.
#[derive(Clone, Debug)]
pub struct Ctx {
    /// Chain point of the event currently being applied. The dApp can
    /// use this in row keys / logs without pulling it out of the event
    /// payload.
    pub point: ChainPoint,

    /// Channel name the event arrived on. Useful for cross-channel
    /// dApps that share an `apply_event` body.
    pub channel: String,

    /// SQL storage handle — synchronous exec interface.
    sql: SqlStorage,
}

impl Ctx {
    /// Construct a new Ctx. Called by the runtime; dApp code never
    /// constructs one directly.
    pub fn new(point: ChainPoint, channel: String, sql: SqlStorage) -> Self {
        Self {
            point,
            channel,
            sql,
        }
    }

    /// Synchronous SQL exec. Runs inside the DO's output gate; safe
    /// to call multiple times in sequence with no `.await` between
    /// — those calls coalesce into a single atomic implicit
    /// transaction (per Q3).
    ///
    /// Returns the cursor for queries that return rows; for INSERT /
    /// UPDATE / DELETE the cursor is typically discarded.
    pub fn exec(&self, sql: &str, bindings: Vec<SqlStorageValue>) -> Result<()> {
        self.sql
            .exec(sql, Some(bindings))
            .map_err(|e| CompanionError::Storage(e.to_string()))?;
        Ok(())
    }

    /// Query for a single row deserialised into `T`. Returns
    /// `Ok(None)` if no rows match.
    pub fn query_row<T: DeserializeOwned>(
        &self,
        sql: &str,
        bindings: Vec<SqlStorageValue>,
    ) -> Result<Option<T>> {
        let cursor = self
            .sql
            .exec(sql, Some(bindings))
            .map_err(|e| CompanionError::Storage(e.to_string()))?;
        let mut iter = cursor.next::<T>();
        match iter.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(CompanionError::Storage(e.to_string())),
            None => Ok(None),
        }
    }

    /// Query for a single BLOB column from a single row. Used for
    /// reading the CBOR-encoded `cursor_chain_point` row.
    pub fn query_blob(&self, sql: &str, bindings: Vec<SqlStorageValue>) -> Result<Option<Vec<u8>>> {
        let cursor = self
            .sql
            .exec(sql, Some(bindings))
            .map_err(|e| CompanionError::Storage(e.to_string()))?;
        let mut iter = cursor.raw();
        match iter.next() {
            Some(Ok(row)) => match row.into_iter().next() {
                Some(SqlStorageValue::Blob(bytes)) => Ok(Some(bytes)),
                Some(other) => Err(CompanionError::Storage(format!(
                    "expected BLOB, got {other:?}"
                ))),
                None => Ok(None),
            },
            Some(Err(e)) => Err(CompanionError::Storage(e.to_string())),
            None => Ok(None),
        }
    }
}
