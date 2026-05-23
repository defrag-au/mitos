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

/// Sentinel `companion_id` used by `drain_one` to persist
/// emissions that fire before any companion has subscribed.
/// The leading colon is invalid in real companion keys
/// (validated elsewhere to be `[A-Za-z0-9_-]+`) so this can't
/// collide with a legitimate key. Subscribed companions claim
/// these rows via [`EmissionsStore::retarget_companion`].
///
/// See `docs/design/EVENT_DELIVERY_RESILIENCE.md` drop site #3.
pub const UNSUBSCRIBED_COMPANION_ID: &str = ":unsubscribed";

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
    /// Client instance identifier — disambiguates two companions
    /// that share the same `companion_id` (e.g. dev + prod workers
    /// consuming the same policy). One emission row per
    /// `(companion_id, client_id)` pair. `#[serde(default)]` so
    /// pre-multi-client rows deserialise as the empty string;
    /// queries that filter on `client_id` should treat "" as
    /// "legacy-pre-fix" and not match modern subscribers.
    ///
    /// See `docs/design/MULTI_CLIENT_COMPANIONS.md`.
    #[serde(default)]
    pub client_id: String,
    pub status: EmissionStatus,
    pub status_at: String,
    /// Populated only when `status == Nacked`.
    pub error: Option<String>,
    /// Dialer partition key chosen by the module via
    /// `emit-event-keyed`. Empty = global lane. Opaque to the
    /// platform; the dialer uses it as a hash input only.
    /// `#[serde(default)]` so existing CBOR rows on disk (pre-
    /// dialer-concurrency) deserialise cleanly as empty.
    #[serde(default)]
    pub partition_key: Vec<u8>,
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
    ///
    /// `partition_key` is the dialer's lane identifier (see
    /// `docs/design/DIALER_CONCURRENCY.md`). Empty = global lane,
    /// equivalent to legacy single-lane drain. Callers from the
    /// legacy `emit-event` path pass empty; `emit-event-keyed`
    /// callers pass the module-declared key.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &self,
        companion_id: &str,
        client_id: &str,
        channel: &str,
        chain_point: ChainPoint,
        payload: Vec<u8>,
        partition_key: Vec<u8>,
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
            client_id: client_id.to_string(),
            status: initial_status,
            status_at: now_rfc3339.to_string(),
            error: None,
            partition_key,
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
    /// (`Pending → Timeout`), queued-drain (`Queued → Pending`),
    /// and reconnect-requeue (`Pending → Queued`, see
    /// [`Self::requeue_pending_for_companion`]). Idempotent —
    /// re-applying the same status is a no-op rewrite. `sent_at`
    /// is only set on the initial `Queued → non-Queued`
    /// transition, so a `Pending → Queued → Pending` round-trip
    /// preserves the original send timestamp as an audit trail.
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

    /// Rewrite `companion_id` and `client_id` on every row that
    /// currently holds `from` (with the sentinel empty `client_id`)
    /// to instead hold `(to, client_id)`. Used by the subscribe
    /// handler to claim the [`UNSUBSCRIBED_COMPANION_ID`]
    /// sentinel rows that `drain_one` wrote during the window
    /// between module-activation and first companion-subscribe
    /// (drop site #3 in
    /// `docs/design/EVENT_DELIVERY_RESILIENCE.md`).
    ///
    /// Status, payload, timestamps, and chain point are left
    /// alone — only the routing target changes. Once the
    /// rewrite commits, the dialer's `drain_queued` picks the
    /// rows up on the next reconnect and the new companion
    /// receives them in id order.
    ///
    /// `client_id` is required (the multi-client identity work
    /// made it part of every companion's routing key — see
    /// `docs/design/MULTI_CLIENT_COMPANIONS.md`). Sentinel rows
    /// land with an empty `client_id`; rewriting it here is what
    /// makes them surface to a subscriber's
    /// `list_queued_for_companion(to, client_id)` lookup.
    ///
    /// **Single-claim semantics.** First subscriber to a
    /// module claims all sentinel rows; later subscribers see
    /// nothing to retarget. Acceptable for single-companion
    /// modules (the current shape of every production module);
    /// see the design doc's "What this doesn't address" for
    /// the multi-companion variant.
    ///
    /// Returns the number of rows that were rewritten.
    pub fn retarget_companion(
        &self,
        from: &str,
        to: &str,
        client_id: &str,
    ) -> Result<usize, EmissionsError> {
        let rows = self.list_filtered(|r| r.companion_id == from)?;
        let count = rows.len();
        if count == 0 {
            return Ok(0);
        }
        // Re-encode each row with the new companion_id +
        // client_id. Same open-then-write pattern as
        // `update_status`, scoped to the routing fields only.
        for row in rows {
            let wx = self
                .db
                .begin_write()
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let updated = {
                let table = wx
                    .open_table(EMISSIONS_TABLE)
                    .map_err(|e| EmissionsError::Redb(e.to_string()))?;
                let Some(value) = table
                    .get(row.id)
                    .map_err(|e| EmissionsError::Redb(e.to_string()))?
                else {
                    // Row vanished between the scan and the
                    // write (e.g. concurrent purge). Skip and
                    // continue — the iteration result becomes a
                    // best-effort approximation of "rows claimed."
                    continue;
                };
                let mut record: EmissionRecord = ciborium::de::from_reader(value.value())
                    .map_err(|e| EmissionsError::Decode(e.to_string()))?;
                record.companion_id = to.to_string();
                record.client_id = client_id.to_string();
                let mut buf = Vec::new();
                ciborium::ser::into_writer(&record, &mut buf)
                    .map_err(|e| EmissionsError::Encode(e.to_string()))?;
                buf
            };
            let mut table = wx
                .open_table(EMISSIONS_TABLE)
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            table
                .insert(row.id, updated.as_slice())
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
            drop(table);
            wx.commit()
                .map_err(|e| EmissionsError::Redb(e.to_string()))?;
        }
        Ok(count)
    }

    /// Read all queued rows for a specific `(companion_id,
    /// client_id)` pair in monotonic ID order. Used on companion
    /// reconnect to drain buffered emissions before live stream
    /// resumes. Two consumers sharing a `companion_id` but with
    /// different `client_id`s drain independently.
    pub fn list_queued_for_companion(
        &self,
        companion_id: &str,
        client_id: &str,
    ) -> Result<Vec<EmissionRecord>, EmissionsError> {
        self.list_filtered(|r| {
            r.companion_id == companion_id
                && r.client_id == client_id
                && matches!(r.status, EmissionStatus::Queued)
        })
    }

    /// Read **every** queued row in the store in one read txn,
    /// grouped by `(companion_id, client_id)`. Rows within each
    /// group are id-ordered (= chain order, since `table.iter()`
    /// yields ascending keys and `host_v2::drain_one` emits in
    /// chain order).
    ///
    /// This is the per-module-drain entry point: one full table
    /// scan per poll tick fans out to all the module's companions,
    /// replacing the previous one-scan-per-companion model (which
    /// was O(companions × rows) per second). See
    /// `docs/design/DIALER_CONCURRENCY.md` ("Per-module drain").
    ///
    /// Group order is the `BTreeMap` order of the
    /// `(companion_id, client_id)` key — deterministic but not
    /// otherwise meaningful; the dialer looks each group up in its
    /// registry independently.
    #[allow(clippy::type_complexity)]
    pub fn list_queued_grouped_by_companion(
        &self,
    ) -> Result<Vec<((String, String), Vec<EmissionRecord>)>, EmissionsError> {
        use std::collections::BTreeMap;
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
        let mut groups: BTreeMap<(String, String), Vec<EmissionRecord>> = BTreeMap::new();
        for entry in iter {
            let (_, value) = entry.map_err(|e| EmissionsError::Redb(e.to_string()))?;
            let record: EmissionRecord = ciborium::de::from_reader(value.value())
                .map_err(|e| EmissionsError::Decode(e.to_string()))?;
            if matches!(record.status, EmissionStatus::Queued) {
                groups
                    .entry((record.companion_id.clone(), record.client_id.clone()))
                    .or_default()
                    .push(record);
            }
        }
        Ok(groups.into_iter().collect())
    }

    /// Read all queued rows for `(companion_id, client_id)`,
    /// grouped by `partition_key`. Within each group, rows are
    /// id-ordered (= slot-ordered, since `host_v2::drain_one` emits
    /// in chain order). Group order is unspecified — the dialer
    /// dispatches groups to lane workers by hash-of-key.
    #[allow(clippy::type_complexity)]
    pub fn list_queued_for_companion_grouped(
        &self,
        companion_id: &str,
        client_id: &str,
    ) -> Result<Vec<(Vec<u8>, Vec<EmissionRecord>)>, EmissionsError> {
        use std::collections::BTreeMap;
        let mut groups: BTreeMap<Vec<u8>, Vec<EmissionRecord>> = BTreeMap::new();
        let rows = self.list_queued_for_companion(companion_id, client_id)?;
        for row in rows {
            groups
                .entry(row.partition_key.clone())
                .or_default()
                .push(row);
        }
        Ok(groups.into_iter().collect())
    }

    /// Flip `Pending` rows older than `max_pending_age_secs` for
    /// this companion back to `Queued`. Called periodically by
    /// the dialer's pump loop while the WS is still alive — it
    /// catches the "consumer silently died" case where the WS
    /// keepalive holds the socket open but `apply_event` Ack
    /// never arrives. The age-bound (vs the unconditional
    /// [`Self::requeue_pending_for_companion`]) keeps the path
    /// safe to run on a hot loop: only rows that have been
    /// in-flight longer than a real Ack round-trip get retried.
    ///
    /// `now_secs` and the row's `status_at` are both in `unix:N`
    /// form (produced by the dialer's `now_rfc3339` helper);
    /// rows whose `status_at` can't be parsed are skipped, same
    /// pattern as [`Self::compact`].
    ///
    /// Returns the number of rows that were flipped, for
    /// pump-time logging.
    pub fn requeue_stale_pending_for_companion(
        &self,
        companion_id: &str,
        client_id: &str,
        now_secs: u64,
        max_pending_age_secs: u64,
        now_status_at: &str,
    ) -> Result<usize, EmissionsError> {
        let threshold = now_secs.saturating_sub(max_pending_age_secs);
        let stale = self.list_filtered(|r| {
            if r.companion_id != companion_id
                || r.client_id != client_id
                || r.status != EmissionStatus::Pending
            {
                return false;
            }
            parse_unix_secs(&r.status_at)
                .map(|ts| ts < threshold)
                .unwrap_or(false)
        })?;
        let count = stale.len();
        for row in stale {
            self.update_status(row.id, EmissionStatus::Queued, now_status_at, None)?;
        }
        Ok(count)
    }

    /// Flip every `Pending` row for this companion back to
    /// `Queued`. Called by the dialer on reconnect, before
    /// [`Self::list_queued_for_companion`] / `drain_queued`, to
    /// recover rows that were in flight when the previous WS
    /// died — a dirty close (CF DO hibernation, network drop)
    /// tears the socket without surfacing the unacked frames as
    /// `Nack`, so without this they would stay `Pending` forever
    /// and never appear in the drain.
    ///
    /// Safe because consumer `apply_event` is required to be
    /// idempotent (recapture's bootstrap-refill already relies
    /// on this). The worst case is double-application of an
    /// emission whose `Ack` was lost in flight — the consumer
    /// absorbs it, the new `Ack` arrives, and the row settles.
    ///
    /// `sent_at` is intentionally preserved by `update_status`
    /// (which only writes it on the initial `Queued → non-Queued`
    /// transition), so the audit trail shows "this was sent at
    /// T1, requeued at T2."
    ///
    /// Returns the number of rows that were flipped, for
    /// reconnect-time logging.
    pub fn requeue_pending_for_companion(
        &self,
        companion_id: &str,
        client_id: &str,
        now_rfc3339: &str,
    ) -> Result<usize, EmissionsError> {
        let pending = self.list_filtered(|r| {
            r.companion_id == companion_id
                && r.client_id == client_id
                && matches!(r.status, EmissionStatus::Pending)
        })?;
        let count = pending.len();
        for row in pending {
            self.update_status(row.id, EmissionStatus::Queued, now_rfc3339, None)?;
        }
        Ok(count)
    }

    /// Flip **every** `Pending` row in the store back to `Queued`,
    /// regardless of companion. Called once at per-module-drain
    /// task start to recover rows left `Pending` by a prior host
    /// process that died mid-POST — the per-module analog of the
    /// per-companion [`Self::requeue_pending_for_companion`] the
    /// old one-task-per-companion dialer ran on task start.
    ///
    /// Safe because consumer `apply_event` is required to be
    /// idempotent (see [`Self::requeue_pending_for_companion`] for
    /// the full argument). Returns the number of rows flipped.
    pub fn requeue_all_pending(&self, now_rfc3339: &str) -> Result<usize, EmissionsError> {
        let pending = self.list_filtered(|r| matches!(r.status, EmissionStatus::Pending))?;
        let count = pending.len();
        for row in pending {
            self.update_status(row.id, EmissionStatus::Queued, now_rfc3339, None)?;
        }
        Ok(count)
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
                "client_a",
                "ownership",
                fixed_point(),
                vec![1],
                vec![],
                EmissionStatus::Queued,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        let id2 = store
            .append(
                "c1",
                "client_a",
                "ownership",
                fixed_point(),
                vec![2],
                vec![],
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
                "client_a",
                "ownership",
                fixed_point(),
                vec![1],
                vec![],
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
                "client_a",
                "ownership",
                fixed_point(),
                vec![1],
                vec![],
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
                "client_a",
                "ownership",
                fixed_point(),
                vec![1],
                vec![],
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
    fn list_queued_grouped_buckets_by_partition_key() {
        let (_t, store) = fresh_store();
        // Three queued rows for c1:
        //  - id=1: empty key (global lane)
        //  - id=2: key=b"policy_a"
        //  - id=3: key=b"policy_a"   (same lane as id=2)
        //  - id=4: key=b"policy_b"
        //  - id=5: empty key
        // Plus one row for c2 we want filtered out.
        let cases: &[(&str, &[u8])] = &[
            ("c1", b""),
            ("c1", b"policy_a"),
            ("c1", b"policy_a"),
            ("c1", b"policy_b"),
            ("c1", b""),
            ("c2", b"policy_a"),
        ];
        for (c, key) in cases {
            store
                .append(
                    c,
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
                    key.to_vec(),
                    EmissionStatus::Queued,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }

        let grouped = store
            .list_queued_for_companion_grouped("c1", "client_a")
            .unwrap();
        assert_eq!(grouped.len(), 3, "three distinct keys for c1");

        // Empty key (global lane) sorts first under BTreeMap order.
        assert_eq!(grouped[0].0, b"" as &[u8]);
        let ids: Vec<u64> = grouped[0].1.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![1, 5], "global lane preserves id order");

        assert_eq!(grouped[1].0, b"policy_a" as &[u8]);
        let ids: Vec<u64> = grouped[1].1.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![2, 3], "same-key rows stay together in id order");

        assert_eq!(grouped[2].0, b"policy_b" as &[u8]);
        let ids: Vec<u64> = grouped[2].1.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![4]);
    }

    #[test]
    fn list_queued_grouped_by_companion_buckets_per_companion() {
        let (_t, store) = fresh_store();
        // Two companions on this module's store, each with two
        // queued rows, plus one Acked row that must be excluded.
        let cases: &[(&str, &str, EmissionStatus)] = &[
            ("policy_a", "client_x", EmissionStatus::Queued),
            ("policy_b", "client_x", EmissionStatus::Queued),
            ("policy_a", "client_x", EmissionStatus::Queued),
            ("policy_b", "client_x", EmissionStatus::Acked),
            ("policy_b", "client_x", EmissionStatus::Queued),
        ];
        for (companion, client, status) in cases {
            store
                .append(
                    companion,
                    client,
                    "collection-holders",
                    fixed_point(),
                    vec![],
                    vec![],
                    *status,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }

        let grouped = store.list_queued_grouped_by_companion().unwrap();
        assert_eq!(grouped.len(), 2, "two distinct companions");

        // BTreeMap key order: policy_a sorts before policy_b.
        assert_eq!(
            grouped[0].0,
            ("policy_a".to_string(), "client_x".to_string())
        );
        let a_ids: Vec<u64> = grouped[0].1.iter().map(|r| r.id).collect();
        assert_eq!(a_ids, vec![1, 3], "policy_a queued rows in id order");

        assert_eq!(
            grouped[1].0,
            ("policy_b".to_string(), "client_x".to_string())
        );
        let b_ids: Vec<u64> = grouped[1].1.iter().map(|r| r.id).collect();
        assert_eq!(
            b_ids,
            vec![2, 5],
            "policy_b queued rows in id order; Acked id=4 excluded"
        );
    }

    #[test]
    fn requeue_all_pending_flips_every_pending_companion() {
        let (_t, store) = fresh_store();
        // Pending rows across two companions + one Queued row that
        // must be left alone.
        for (companion, status) in [
            ("c1", EmissionStatus::Pending),
            ("c2", EmissionStatus::Pending),
            ("c1", EmissionStatus::Queued),
        ] {
            store
                .append(
                    companion,
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
                    vec![],
                    status,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        let count = store.requeue_all_pending("2026-05-05T00:00:10Z").unwrap();
        assert_eq!(count, 2, "both companions' Pending rows flipped");
        // All three rows are now Queued.
        let queued = store
            .list_filtered(|r| r.status == EmissionStatus::Queued)
            .unwrap();
        assert_eq!(queued.len(), 3);
        // Second call is a no-op.
        assert_eq!(
            store.requeue_all_pending("2026-05-05T00:00:20Z").unwrap(),
            0
        );
    }

    #[test]
    fn list_queued_filters_by_companion() {
        let (_t, store) = fresh_store();
        for c in &["c1", "c2", "c1"] {
            store
                .append(
                    c,
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
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
        let queued = store.list_queued_for_companion("c1", "client_a").unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].id, 3);
        assert_eq!(queued[0].companion_id, "c1");
    }

    #[test]
    fn requeue_pending_flips_only_named_companion() {
        let (_t, store) = fresh_store();
        // c1 gets two rows, c2 gets one. All start Pending so we
        // can verify the requeue is scoped to c1.
        for c in &["c1", "c1", "c2"] {
            store
                .append(
                    c,
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
                    vec![],
                    EmissionStatus::Pending,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        let count = store
            .requeue_pending_for_companion("c1", "client_a", "2026-05-05T00:00:10Z")
            .unwrap();
        assert_eq!(count, 2);
        // c1's two rows back to Queued, c2's row untouched.
        let c1_queued = store.list_queued_for_companion("c1", "client_a").unwrap();
        assert_eq!(c1_queued.len(), 2);
        let c2_pending = store
            .list_filtered(|r| r.companion_id == "c2" && r.status == EmissionStatus::Pending)
            .unwrap();
        assert_eq!(c2_pending.len(), 1);
    }

    #[test]
    fn requeue_pending_preserves_sent_at_audit_trail() {
        let (_t, store) = fresh_store();
        // Initial Pending append populates sent_at to T0.
        let id = store
            .append(
                "c1",
                "client_a",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Pending,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        // Requeue at T1; sent_at should *not* be cleared (only
        // status_at moves).
        store
            .requeue_pending_for_companion("c1", "client_a", "2026-05-05T00:00:10Z")
            .unwrap();
        let row = store.get(id).unwrap().unwrap();
        assert_eq!(row.status, EmissionStatus::Queued);
        assert_eq!(row.sent_at.as_deref(), Some("2026-05-05T00:00:00Z"));
        assert_eq!(row.status_at, "2026-05-05T00:00:10Z");
    }

    #[test]
    fn requeue_stale_pending_respects_threshold() {
        let (_t, store) = fresh_store();
        // Two Pending rows: one at T=100 (stale), one at T=190
        // (still fresh at threshold 60s relative to now=200).
        let stale_id = store
            .append(
                "c1",
                "client_a",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Pending,
                "unix:100",
            )
            .unwrap();
        let fresh_id = store
            .append(
                "c1",
                "client_a",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Pending,
                "unix:190",
            )
            .unwrap();
        let now_secs = 200;
        let max_age = 60;
        let count = store
            .requeue_stale_pending_for_companion("c1", "client_a", now_secs, max_age, "unix:200")
            .unwrap();
        assert_eq!(count, 1, "only the stale row should requeue");
        assert_eq!(
            store.get(stale_id).unwrap().unwrap().status,
            EmissionStatus::Queued
        );
        assert_eq!(
            store.get(fresh_id).unwrap().unwrap().status,
            EmissionStatus::Pending
        );
    }

    #[test]
    fn requeue_stale_pending_skips_unparseable_timestamps() {
        let (_t, store) = fresh_store();
        // Legacy RFC3339-style status_at; parse_unix_secs returns
        // None → row is left alone rather than treated as
        // age-zero or age-infinity.
        store
            .append(
                "c1",
                "client_a",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Pending,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        let count = store
            .requeue_stale_pending_for_companion("c1", "client_a", 200, 60, "unix:200")
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn requeue_pending_ignores_non_pending_rows() {
        let (_t, store) = fresh_store();
        // Mix of statuses; only the Pending one should flip.
        for status in [
            EmissionStatus::Queued,
            EmissionStatus::Pending,
            EmissionStatus::Acked,
            EmissionStatus::Nacked,
            EmissionStatus::Timeout,
        ] {
            store
                .append(
                    "c1",
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
                    vec![],
                    status,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        let count = store
            .requeue_pending_for_companion("c1", "client_a", "2026-05-05T00:00:10Z")
            .unwrap();
        assert_eq!(count, 1);
        // Second call is a no-op: nothing left in Pending.
        let count = store
            .requeue_pending_for_companion("c1", "client_a", "2026-05-05T00:00:20Z")
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn retarget_companion_claims_sentinel_rows() {
        let (_t, store) = fresh_store();
        // 3 sentinel rows from the "no subscribers yet" window,
        // plus 1 row that already belongs to a different
        // companion. Only the sentinel rows should retarget.
        for _ in 0..3 {
            store
                .append(
                    UNSUBSCRIBED_COMPANION_ID,
                    "",
                    "ownership",
                    fixed_point(),
                    vec![],
                    vec![],
                    EmissionStatus::Queued,
                    "2026-05-05T00:00:00Z",
                )
                .unwrap();
        }
        store
            .append(
                "other_companion",
                "client_a",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Queued,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        let count = store
            .retarget_companion(UNSUBSCRIBED_COMPANION_ID, "subscriber_a", "client_a")
            .unwrap();
        assert_eq!(count, 3);
        // After retarget: subscriber_a has the 3 rows ready to
        // drain; sentinel has none; the other companion is
        // untouched.
        let claimed = store
            .list_queued_for_companion("subscriber_a", "client_a")
            .unwrap();
        assert_eq!(claimed.len(), 3);
        let remaining_sentinel = store
            .list_queued_for_companion(UNSUBSCRIBED_COMPANION_ID, "")
            .unwrap();
        assert!(remaining_sentinel.is_empty());
        let other = store
            .list_queued_for_companion("other_companion", "client_a")
            .unwrap();
        assert_eq!(other.len(), 1);
    }

    #[test]
    fn retarget_companion_second_claim_is_noop() {
        let (_t, store) = fresh_store();
        store
            .append(
                UNSUBSCRIBED_COMPANION_ID,
                "",
                "ownership",
                fixed_point(),
                vec![],
                vec![],
                EmissionStatus::Queued,
                "2026-05-05T00:00:00Z",
            )
            .unwrap();
        // First subscriber claims.
        assert_eq!(
            store
                .retarget_companion(UNSUBSCRIBED_COMPANION_ID, "subscriber_a", "client_a")
                .unwrap(),
            1
        );
        // Second subscriber sees nothing to claim — single-claim
        // semantics documented on the method.
        assert_eq!(
            store
                .retarget_companion(UNSUBSCRIBED_COMPANION_ID, "subscriber_b", "client_b")
                .unwrap(),
            0
        );
    }

    #[test]
    fn purge_drops_matching_rows() {
        let (_t, store) = fresh_store();
        for _ in 0..5 {
            store
                .append(
                    "c1",
                    "client_a",
                    "ownership",
                    fixed_point(),
                    vec![],
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
