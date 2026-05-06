//! Per-module write-forward emissions log.
//!
//! See the design doc's "Emissions log on the host" section
//! (`docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`). The host
//! writes one row per matched event per receiving companion;
//! status tracks the row through its delivery lifecycle:
//!
//! ```text
//!                                +-(deliver now)----> Pending -+- ack -> Acked
//!  match arrives -+-(WS open)----+                              +- nack -> Nacked
//!                 |                                              +- 24h -> Timeout
//!                 +-(WS closed)-> Queued -(reconnect, drain)-> Pending -+
//! ```
//!
//! Storage shape: per-module redb file at
//! `<storage>/<module_id>/emissions.redb`. Single table
//! `emissions` keyed by monotonic `u64` ID. Row payload is
//! CBOR-encoded `EmissionRecord`.

use std::path::Path;
use std::sync::Arc;

use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use mitos_protocol::ChainPoint;

const EMISSIONS_TABLE: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("emissions");
const NEXT_ID_KEY: &str = "__next_id";
const META_TABLE: TableDefinition<'_, &str, u64> = TableDefinition::new("meta");

#[derive(Debug, thiserror::Error)]
pub enum EmissionsError {
    #[error("redb: {0}")]
    Redb(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
}

/// Status of a row in `module_emissions`. Lifecycle described
/// in the module-level docs above.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmissionStatus {
    /// Match found; companion offline (no active WS). Row
    /// buffered for future delivery on reconnect.
    Queued,
    /// Frame sent over WS; awaiting `Ack` or `Nack`.
    Pending,
    /// Companion confirmed successful `apply_event`.
    Acked,
    /// Companion confirmed `apply_event` errored.
    Nacked,
    /// Frame was sent but no Ack/Nack within the timeout
    /// (typically WS drop or DO crash mid-handler).
    Timeout,
}

impl EmissionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Pending => "pending",
            Self::Acked => "acked",
            Self::Nacked => "nacked",
            Self::Timeout => "timeout",
        }
    }
}

/// One row in `module_emissions`. CBOR-encoded as the redb
/// value bytes; the `id` is the redb key (also embedded here
/// for convenience when the row is read out).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmissionRecord {
    pub id: u64,
    /// Wall-clock timestamp (RFC 3339) when the host saw the
    /// match. Diagnostic only — operators use this in the
    /// `mitos-admin emissions list` UI.
    pub matched_at: String,
    /// Set when the row's status moves out of `Queued`. None
    /// while still queued.
    pub sent_at: Option<String>,
    pub chain_point: ChainPoint,
    pub channel: String,
    /// CBOR-encoded change payload. Same bytes that go on the
    /// wire as `ServerMessage::Apply.change`.
    pub payload: Vec<u8>,
    /// Companion key. Match the companion's
    /// `id_from_name(companion_key)` — see Q8 of the design doc.
    pub companion_id: String,
    pub status: EmissionStatus,
    pub status_at: String,
    /// Populated only when `status == Nacked`.
    pub error: Option<String>,
}

/// Per-module emissions log. Wraps a redb database file with
/// helpers for the lifecycle the design doc specifies. Cheap
/// to clone (Arc-wrapped redb handle).
#[derive(Clone)]
pub struct EmissionsStore {
    db: Arc<redb::Database>,
}

impl EmissionsStore {
    /// Open or create the emissions database at the given path.
    /// Initialises the tables on first open so subsequent reads
    /// don't race-fail on a not-yet-created table.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, EmissionsError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Sole open site for emissions.redb. Routed exclusively
        // through `ModuleStorage::emissions_store` which caches
        // by path; see clippy.toml for the workspace lint.
        #[allow(clippy::disallowed_methods)]
        let db = redb::Database::builder()
            .create(path)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let wx = db
            .begin_write()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        wx.open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        wx.open_table(META_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        wx.commit()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Append a new row with status `Queued` (companion offline)
    /// or `Pending` (caller will dispatch immediately).
    /// Auto-assigns the next monotonic ID and returns it.
    pub fn append(
        &self,
        companion_id: &str,
        channel: &str,
        chain_point: ChainPoint,
        payload: Vec<u8>,
        initial_status: EmissionStatus,
        now_rfc3339: &str,
    ) -> Result<u64, EmissionsError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let id = {
            let mut meta = wx
                .open_table(META_TABLE)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let next = meta
                .get(NEXT_ID_KEY)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?
                .map(|v| v.value())
                .unwrap_or(1);
            meta.insert(NEXT_ID_KEY, next + 1)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            next
        };

        let sent_at = match initial_status {
            EmissionStatus::Queued => None,
            _ => Some(now_rfc3339.to_string()),
        };

        let record = EmissionRecord {
            id,
            matched_at: now_rfc3339.to_string(),
            sent_at,
            chain_point,
            channel: channel.to_string(),
            payload,
            companion_id: companion_id.to_string(),
            status: initial_status,
            status_at: now_rfc3339.to_string(),
            error: None,
        };

        let mut buf = Vec::new();
        ciborium::ser::into_writer(&record, &mut buf)
            .map_err(|e| EmissionsError::Encode(e.to_string()))?;
        let mut emissions = wx
            .open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        emissions
            .insert(id, buf.as_slice())
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        drop(emissions);
        wx.commit()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        Ok(id)
    }

    /// Read a single row by ID. `Ok(None)` if the ID doesn't
    /// exist (already purged or never existed).
    pub fn get(&self, id: u64) -> Result<Option<EmissionRecord>, EmissionsError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let table = rx
            .open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let Some(value) = table
            .get(id)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?
        else {
            return Ok(None);
        };
        let bytes = value.value();
        let record: EmissionRecord =
            ciborium::de::from_reader(bytes).map_err(|e| EmissionsError::Decode(e.to_string()))?;
        Ok(Some(record))
    }

    /// Update an existing row's status. Used on Ack/Nack
    /// (`Pending → Acked/Nacked`), pending-aging
    /// (`Pending → Timeout`), and queued-drain
    /// (`Queued → Pending`). Idempotent — re-applying the same
    /// status is a no-op rewrite.
    pub fn update_status(
        &self,
        id: u64,
        new_status: EmissionStatus,
        now_rfc3339: &str,
        error: Option<String>,
    ) -> Result<(), EmissionsError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let updated = {
            let table = wx
                .open_table(EMISSIONS_TABLE)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let Some(value) = table
                .get(id)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?
            else {
                return Ok(()); // missing row — silently noop, caller may have purged
            };
            let mut record: EmissionRecord = ciborium::de::from_reader(value.value())
                .map_err(|e| EmissionsError::Decode(e.to_string()))?;
            // Set sent_at on the queued → anything-else transition.
            if record.status == EmissionStatus::Queued
                && new_status != EmissionStatus::Queued
                && record.sent_at.is_none()
            {
                record.sent_at = Some(now_rfc3339.to_string());
            }
            record.status = new_status;
            record.status_at = now_rfc3339.to_string();
            if matches!(new_status, EmissionStatus::Nacked) {
                record.error = error;
            }
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&record, &mut buf)
                .map_err(|e| EmissionsError::Encode(e.to_string()))?;
            buf
        };
        let mut table = wx
            .open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        table
            .insert(id, updated.as_slice())
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        drop(table);
        wx.commit()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        Ok(())
    }

    /// Read all queued rows for a specific companion in
    /// monotonic ID order. Used on companion reconnect to drain
    /// buffered emissions before live stream resumes.
    pub fn list_queued_for_companion(
        &self,
        companion_id: &str,
    ) -> Result<Vec<EmissionRecord>, EmissionsError> {
        self.list_filtered(|r| {
            r.companion_id == companion_id && matches!(r.status, EmissionStatus::Queued)
        })
    }

    /// Generic filter over all rows. The closure receives each
    /// decoded record and returns `true` to keep, `false` to
    /// skip. Used by `mitos-admin emissions list` operator surface.
    pub fn list_filtered<F: Fn(&EmissionRecord) -> bool>(
        &self,
        filter: F,
    ) -> Result<Vec<EmissionRecord>, EmissionsError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let table = rx
            .open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let iter = table
            .iter()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let mut out = Vec::new();
        for entry in iter {
            let (_, value) = entry.map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let record: EmissionRecord = ciborium::de::from_reader(value.value())
                .map_err(|e| EmissionsError::Decode(e.to_string()))?;
            if filter(&record) {
                out.push(record);
            }
        }
        Ok(out)
    }

    /// Drop rows matching the predicate. Used by the operator
    /// surface (`mitos-admin emissions purge`) for compaction +
    /// abandoned-companion cleanup.
    pub fn purge<F: Fn(&EmissionRecord) -> bool>(
        &self,
        predicate: F,
    ) -> Result<usize, EmissionsError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let to_drop: Vec<u64> = {
            let table = wx
                .open_table(EMISSIONS_TABLE)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let iter = table
                .iter()
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let mut acc = Vec::new();
            for entry in iter {
                let (key, value) = entry.map_err(|e| EmissionsError::Redb(e.to_string()))?;
                let record: EmissionRecord = ciborium::de::from_reader(value.value())
                    .map_err(|e| EmissionsError::Decode(e.to_string()))?;
                if predicate(&record) {
                    acc.push(key.value());
                }
            }
            acc
        };
        let mut table = wx
            .open_table(EMISSIONS_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        for id in &to_drop {
            table
                .remove(*id)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        }
        drop(table);
        wx.commit()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        Ok(to_drop.len())
    }

    /// Periodic compaction. Walks every row once and applies
    /// two policies in one pass:
    ///
    /// - **Acked rows** older than `acked_max_age_secs` are
    ///   purged outright (the consumer confirmed delivery; we
    ///   keep them around briefly for diagnostics, then drop).
    /// - **Pending rows** older than `pending_max_age_secs`
    ///   flip to `Timeout` (delivery uncertain — likely the WS
    ///   dropped before the Ack arrived). The operator decides
    ///   whether to `mitos-admin emissions-replay`. Timeout
    ///   rows are otherwise terminal and stay around as a
    ///   diagnostic signal.
    ///
    /// `now_secs` is the current Unix seconds; tests pass a
    /// fixed value for determinism. Rows with timestamps that
    /// can't be parsed as `unix:N` are skipped (keeps the
    /// sweep safe across legacy / future timestamp formats).
    ///
    /// Returns `(timed_out_pending, purged_acked)` for logging.
    pub fn compact(
        &self,
        now_secs: u64,
        acked_max_age_secs: u64,
        pending_max_age_secs: u64,
    ) -> Result<(usize, usize), EmissionsError> {
        // First pass: classify.
        let acked_threshold = now_secs.saturating_sub(acked_max_age_secs);
        let pending_threshold = now_secs.saturating_sub(pending_max_age_secs);
        let mut to_purge: Vec<u64> = Vec::new();
        let mut to_timeout: Vec<u64> = Vec::new();
        for record in self.list_filtered(|_| true)? {
            let Some(ts) = parse_unix_secs(&record.status_at) else {
                continue;
            };
            match record.status {
                EmissionStatus::Acked if ts < acked_threshold => {
                    to_purge.push(record.id);
                }
                EmissionStatus::Pending if ts < pending_threshold => {
                    to_timeout.push(record.id);
                }
                _ => {}
            }
        }

        // Second pass: apply. update_status touches one redb
        // tx per call — fine for the volumes we expect (single
        // digits to low hundreds per sweep). Batch into one
        // transaction if this becomes hot.
        let timed_out_count = to_timeout.len();
        let now_str = format!("unix:{now_secs}");
        for id in to_timeout {
            self.update_status(id, EmissionStatus::Timeout, &now_str, None)?;
        }
        let purged_count = self.purge(|r| to_purge.contains(&r.id))?;
        Ok((timed_out_count, purged_count))
    }

    /// Return the next emission_id that would be assigned. Used
    /// in `SubscribeResponse.next_emission_id` so companions can
    /// sync against the host's view.
    pub fn peek_next_id(&self) -> Result<u64, EmissionsError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        let meta = rx
            .open_table(META_TABLE)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        Ok(meta
            .get(NEXT_ID_KEY)
            .map_err(|e| EmissionsError::Redb(e.to_string()))?
            .map(|v| v.value())
            .unwrap_or(1))
    }
}

/// Parse the `unix:N` timestamp shape produced by the bundle's
/// `now_rfc3339` helpers. Returns `None` for legacy / future
/// formats so the compaction sweep treats those rows as
/// "leave alone."
fn parse_unix_secs(s: &str) -> Option<u64> {
    s.strip_prefix("unix:").and_then(|n| n.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_store() -> (tempfile::TempDir, EmissionsStore) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("emissions.redb");
        let store = EmissionsStore::open(&path).unwrap();
        (tmp, store)
    }

    fn fixed_point() -> ChainPoint {
        ChainPoint::Specific(123, "abc".into())
    }

    #[test]
    fn append_assigns_monotonic_ids() {
        let (_t, store) = fresh_store();
        let id1 = store
            .append(
                "c1",
                "ownership",
                fixed_point(),
                vec![1],
                EmissionStatus::Queued,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        let id2 = store
            .append(
                "c1",
                "ownership",
                fixed_point(),
                vec![2],
                EmissionStatus::Queued,
                "2026-05-05T00:00:01Z",
            )
            .unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(store.peek_next_id().unwrap(), 3);
    }

    #[test]
    fn append_sets_sent_at_when_pending_initial() {
        let (_t, store) = fresh_store();
        let id = store
            .append(
                "c1",
                "ownership",
                fixed_point(),
                vec![1],
                EmissionStatus::Pending,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        let row = store.get(id).unwrap().unwrap();
        assert_eq!(row.status, EmissionStatus::Pending);
        assert_eq!(row.sent_at.as_deref(), Some("2026-05-05T00:00:00Z"));
    }

    #[test]
    fn update_status_transitions_through_lifecycle() {
        let (_t, store) = fresh_store();
        let id = store
            .append(
                "c1",
                "ownership",
                fixed_point(),
                vec![1],
                EmissionStatus::Queued,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        // Queued → Pending: sent_at populated.
        store
            .update_status(id, EmissionStatus::Pending, "2026-05-05T00:00:01Z", None)
            .unwrap();
        let row = store.get(id).unwrap().unwrap();
        assert_eq!(row.status, EmissionStatus::Pending);
        assert_eq!(row.sent_at.as_deref(), Some("2026-05-05T00:00:01Z"));

        // Pending → Acked.
        store
            .update_status(id, EmissionStatus::Acked, "2026-05-05T00:00:02Z", None)
            .unwrap();
        let row = store.get(id).unwrap().unwrap();
        assert_eq!(row.status, EmissionStatus::Acked);
    }

    #[test]
    fn update_status_records_error_only_on_nacked() {
        let (_t, store) = fresh_store();
        let id = store
            .append(
                "c1",
                "ownership",
                fixed_point(),
                vec![1],
                EmissionStatus::Pending,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        store
            .update_status(
                id,
                EmissionStatus::Nacked,
                "2026-05-05T00:00:01Z",
                Some("apply failed: foo".into()),
            )
            .unwrap();
        let row = store.get(id).unwrap().unwrap();
        assert_eq!(row.status, EmissionStatus::Nacked);
        assert_eq!(row.error.as_deref(), Some("apply failed: foo"));
    }

    #[test]
    fn list_queued_filters_by_companion() {
        let (_t, store) = fresh_store();
        for c in &["c1", "c2", "c1"] {
            store
                .append(
                    c,
                    "ownership",
                    fixed_point(),
                    vec![],
                    EmissionStatus::Queued,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        // Promote one of c1's rows to Pending so we know the
        // queued filter works.
        store
            .update_status(1, EmissionStatus::Pending, "2026-05-05T00:00:01Z", None)
            .unwrap();
        let queued = store.list_queued_for_companion("c1").unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, 3);
        assert_eq!(queued[0].companion_id, "c1");
    }

    #[test]
    fn purge_drops_matching_rows() {
        let (_t, store) = fresh_store();
        for _ in 0..5 {
            store
                .append(
                    "c1",
                    "ownership",
                    fixed_point(),
                    vec![],
                    EmissionStatus::Queued,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        let dropped = store
            .purge(|r| matches!(r.status, EmissionStatus::Queued))
            .unwrap();
        assert_eq!(dropped, 5);
        let remaining = store.list_filtered(|_| true).unwrap();
        assert!(remaining.is_empty());
    }

    #[test]
    fn missing_row_update_is_silent_noop() {
        let (_t, store) = fresh_store();
        store
            .update_status(999, EmissionStatus::Acked, "2026-05-05T00:00:00Z", None)
            .unwrap();
        // No panic; row never existed.
    }
}
