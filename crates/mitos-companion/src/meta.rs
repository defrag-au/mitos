//! Runtime-owned schema + cursor + meta-row helpers.
//!
//! The runtime always creates three tables on first wake and migrates
//! from older shapes if necessary:
//!
//! - `mitos_companion_meta` — KV-shaped (TEXT key, BLOB value). Holds
//!   the CBOR-encoded `cursor_chain_point`, the schema version row,
//!   and `last_subscribe_at` timestamp.
//! - `mitos_companion_interest` — the canonical interest set (Q6/Q7).
//! - `mitos_companion_registration` — the cached subscribe-call config
//!   so the runtime can skip re-subscribing on subsequent wakes when
//!   nothing has changed.
//!
//! See the design doc's "Cursor coordination" + "Addressing & wake-up"
//! + "Dynamic interest mechanics" sections.

use mitos_protocol::SubscribeTarget;
use serde::{Deserialize, Serialize};
use worker::SqlStorage;

use crate::ctx::SqlStorageValue;
use crate::error::{CompanionError, Result};
use crate::wire::ChainPoint;

/// Schema version the runtime currently knows how to handle. Bumped
/// when an additive change goes out; a mismatch on first wake triggers
/// the runtime's migration path (currently just the split-row → CBOR
/// cursor migration).
pub const RUNTIME_SCHEMA_VERSION: i64 = 1;

const META_KEY_CURSOR: &str = "cursor_chain_point";
const META_KEY_SCHEMA_VERSION: &str = "schema_version";
const META_KEY_LAST_SUBSCRIBE: &str = "last_subscribe_at";
const META_KEY_REGISTRATION: &str = "registration";

// ============================================================================
// Schema setup
// ============================================================================

/// Create runtime-owned tables if they don't exist. Idempotent;
/// re-running this is a no-op once tables are present.
pub fn ensure_schema(sql: &SqlStorage) -> Result<()> {
    sql.exec(
        "CREATE TABLE IF NOT EXISTS mitos_companion_meta (
            key   TEXT PRIMARY KEY,
            value BLOB NOT NULL
        )",
        None,
    )
    .map_err(|e| CompanionError::Storage(format!("create meta table: {e}")))?;

    sql.exec(
        "CREATE TABLE IF NOT EXISTS mitos_companion_interest (
            kind     TEXT NOT NULL,
            value    TEXT NOT NULL,
            channel  TEXT NOT NULL DEFAULT '',
            added_at TEXT NOT NULL,
            PRIMARY KEY (kind, value, channel)
        )",
        None,
    )
    .map_err(|e| CompanionError::Storage(format!("create interest table: {e}")))?;

    // Stamp the current schema version if the row is missing.
    let exists = sql
        .exec(
            "SELECT 1 FROM mitos_companion_meta WHERE key = ?",
            Some(vec![SqlStorageValue::from(META_KEY_SCHEMA_VERSION)]),
        )
        .map_err(|e| CompanionError::Storage(format!("schema version probe: {e}")))?
        .raw()
        .next()
        .is_some();
    if !exists {
        sql.exec(
            "INSERT INTO mitos_companion_meta (key, value) VALUES (?, ?)",
            Some(vec![
                SqlStorageValue::from(META_KEY_SCHEMA_VERSION),
                SqlStorageValue::Blob(RUNTIME_SCHEMA_VERSION.to_le_bytes().to_vec()),
            ]),
        )
        .map_err(|e| CompanionError::Storage(format!("stamp schema version: {e}")))?;
    }

    Ok(())
}

// ============================================================================
// Cursor read / write
// ============================================================================

/// Read the persisted cursor. Returns `None` if no cursor row exists
/// yet (fresh DO; first subscribe will use `Origin`).
pub fn read_cursor(sql: &SqlStorage) -> Result<Option<ChainPoint>> {
    let cursor = sql
        .exec(
            "SELECT value FROM mitos_companion_meta WHERE key = ?",
            Some(vec![SqlStorageValue::from(META_KEY_CURSOR)]),
        )
        .map_err(|e| CompanionError::Storage(format!("read cursor: {e}")))?;
    let mut iter = cursor.raw();
    match iter.next() {
        Some(Ok(row)) => match row.into_iter().next() {
            Some(SqlStorageValue::Blob(bytes)) => {
                let point: ChainPoint = ciborium::de::from_reader(&bytes[..])
                    .map_err(|e| CompanionError::Storage(format!("decode cursor: {e}")))?;
                Ok(Some(point))
            }
            Some(other) => Err(CompanionError::Storage(format!(
                "expected BLOB cursor, got {other:?}"
            ))),
            None => Ok(None),
        },
        Some(Err(e)) => Err(CompanionError::Storage(e.to_string())),
        None => Ok(None),
    }
}

/// Write the cursor as a CBOR-encoded BLOB. Synchronous — designed to
/// run inside the output gate after the dApp's `apply_event` writes
/// (per Q3 "Atomicity: output-gate"). Idempotent (`INSERT OR REPLACE`)
/// so replaying the same event lands the same row.
pub fn write_cursor(sql: &SqlStorage, point: &ChainPoint) -> Result<()> {
    let mut buf = Vec::with_capacity(64);
    ciborium::ser::into_writer(point, &mut buf)
        .map_err(|e| CompanionError::Storage(format!("encode cursor: {e}")))?;
    sql.exec(
        "INSERT OR REPLACE INTO mitos_companion_meta (key, value) VALUES (?, ?)",
        Some(vec![
            SqlStorageValue::from(META_KEY_CURSOR),
            SqlStorageValue::Blob(buf),
        ]),
    )
    .map_err(|e| CompanionError::Storage(format!("write cursor: {e}")))?;
    Ok(())
}

// ============================================================================
// Split-row → CBOR migration
// ============================================================================

/// One-shot migration from collections-mitos's historical split-row
/// format (`cursor_slot` + `cursor_hash` as TEXT rows) to the
/// CBOR-encoded `cursor_chain_point` BLOB row.
///
/// Idempotent — if the new row exists, returns immediately. If only
/// the legacy rows exist, reconstructs `ChainPoint::Specific(slot,
/// hash)`, writes the new row, deletes the old. Runs on every
/// migration pass so legacy DOs auto-upgrade on first contact.
pub fn migrate_split_row_cursor(sql: &SqlStorage) -> Result<bool> {
    if read_cursor(sql)?.is_some() {
        return Ok(false);
    }
    let slot_row = sql
        .exec(
            "SELECT value FROM mitos_companion_meta WHERE key = 'cursor_slot'",
            None,
        )
        .map_err(|e| CompanionError::Storage(e.to_string()))?
        .raw()
        .next();
    let hash_row = sql
        .exec(
            "SELECT value FROM mitos_companion_meta WHERE key = 'cursor_hash'",
            None,
        )
        .map_err(|e| CompanionError::Storage(e.to_string()))?
        .raw()
        .next();
    let (Some(Ok(slot_row)), Some(Ok(hash_row))) = (slot_row, hash_row) else {
        return Ok(false);
    };
    let slot = match slot_row.into_iter().next() {
        Some(SqlStorageValue::String(s)) => s
            .parse::<u64>()
            .map_err(|e| CompanionError::Storage(format!("parse legacy slot: {e}")))?,
        Some(SqlStorageValue::Integer(i)) => i as u64,
        Some(SqlStorageValue::Blob(b)) => String::from_utf8(b)
            .map_err(|e| CompanionError::Storage(format!("legacy slot utf8: {e}")))?
            .parse::<u64>()
            .map_err(|e| CompanionError::Storage(format!("parse legacy slot: {e}")))?,
        Some(other) => {
            return Err(CompanionError::Storage(format!(
                "unexpected legacy slot shape: {other:?}"
            )));
        }
        None => return Ok(false),
    };
    let hash = match hash_row.into_iter().next() {
        Some(SqlStorageValue::String(s)) => s,
        Some(SqlStorageValue::Blob(b)) => String::from_utf8(b)
            .map_err(|e| CompanionError::Storage(format!("legacy hash utf8: {e}")))?,
        Some(other) => {
            return Err(CompanionError::Storage(format!(
                "unexpected legacy hash shape: {other:?}"
            )));
        }
        None => return Ok(false),
    };
    write_cursor(sql, &ChainPoint::Specific(slot, hash))?;
    sql.exec(
        "DELETE FROM mitos_companion_meta WHERE key IN ('cursor_slot', 'cursor_hash')",
        None,
    )
    .map_err(|e| CompanionError::Storage(format!("clean up legacy rows: {e}")))?;
    tracing::info!("migrated cursor from split-row to CBOR-encoded format");
    Ok(true)
}

// ============================================================================
// Registration cache
// ============================================================================

/// Cached subscribe-call config. Stored as CBOR in the meta table
/// under `META_KEY_REGISTRATION`. Compared on subsequent wakes; if
/// unchanged, the runtime skips the HTTPS subscribe round-trip.
///
/// Changed by the unified-subscribe wire refactor (see
/// `docs/design/UNIFIED_SUBSCRIBE.md`): `module_name: String` →
/// `targets: Vec<SubscribeTarget>`. Old CBOR-encoded caches that
/// predate this shape will fail to decode in `read_registration`,
/// which downgrades the failure to `Ok(None)` so the next wake
/// just re-registers cleanly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistrationCache {
    pub targets: Vec<SubscribeTarget>,
    pub companion_key: String,
    pub mitos_host_url: String,
    pub interests_hash: u64,
}

pub fn read_registration(sql: &SqlStorage) -> Result<Option<RegistrationCache>> {
    let cursor = sql
        .exec(
            "SELECT value FROM mitos_companion_meta WHERE key = ?",
            Some(vec![SqlStorageValue::from(META_KEY_REGISTRATION)]),
        )
        .map_err(|e| CompanionError::Storage(format!("read registration: {e}")))?;
    let mut iter = cursor.raw();
    match iter.next() {
        Some(Ok(row)) => match row.into_iter().next() {
            Some(SqlStorageValue::Blob(bytes)) => {
                match ciborium::de::from_reader::<RegistrationCache, _>(&bytes[..]) {
                    Ok(cache) => Ok(Some(cache)),
                    Err(e) => {
                        // Cache schema mismatch (e.g. pre-unified-
                        // subscribe rows still on disk). Treat as
                        // no-cache so the runtime re-registers
                        // cleanly on this wake. Log so an operator
                        // can see the migration happen.
                        tracing::warn!(
                            error = %e,
                            "registration cache failed to decode; treating as absent (will re-subscribe)"
                        );
                        Ok(None)
                    }
                }
            }
            _ => Ok(None),
        },
        Some(Err(e)) => Err(CompanionError::Storage(e.to_string())),
        None => Ok(None),
    }
}

pub fn write_registration(sql: &SqlStorage, cache: &RegistrationCache) -> Result<()> {
    let mut buf = Vec::with_capacity(128);
    ciborium::ser::into_writer(cache, &mut buf)
        .map_err(|e| CompanionError::Storage(format!("encode registration: {e}")))?;
    sql.exec(
        "INSERT OR REPLACE INTO mitos_companion_meta (key, value) VALUES (?, ?)",
        Some(vec![
            SqlStorageValue::from(META_KEY_REGISTRATION),
            SqlStorageValue::Blob(buf),
        ]),
    )
    .map_err(|e| CompanionError::Storage(format!("write registration: {e}")))?;
    Ok(())
}

// ============================================================================
// Last-subscribe diagnostic timestamp
// ============================================================================

pub fn write_last_subscribe_at(sql: &SqlStorage, rfc3339: &str) -> Result<()> {
    sql.exec(
        "INSERT OR REPLACE INTO mitos_companion_meta (key, value) VALUES (?, ?)",
        Some(vec![
            SqlStorageValue::from(META_KEY_LAST_SUBSCRIBE),
            SqlStorageValue::Blob(rfc3339.as_bytes().to_vec()),
        ]),
    )
    .map_err(|e| CompanionError::Storage(format!("write last_subscribe_at: {e}")))?;
    Ok(())
}
