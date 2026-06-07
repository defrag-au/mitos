//! Lane-aware delivery pool.
//!
//! Implements the dispatch + execution machinery described in
//! `docs/design/DIALER_CONCURRENCY.md`. The pool partitions the
//! queued emissions by `partition_key`, hashes each partition to
//! one of N worker slots, and dispatches lane batches in parallel.
//! Within a lane, rows drain serially in id-order (= slot-order,
//! since `host_v2::drain_one` emits in chain order).
//!
//! ## Why a pool instead of N persistent tasks
//!
//! Each companion drain (`run_tick`, one per active companion per
//! module tick) drains a snapshot of that companion's queued set.
//! Spawning one short-lived task per
//! lane per tick is cheaper than maintaining N long-lived worker
//! tasks plus their channels: less plumbing, no work-stealing
//! between idle workers, natural backpressure (we don't dispatch
//! the next tick's work until this tick's lanes finish).
//!
//! ## Default at N=8
//!
//! `LaneConfig::default()` returns `lanes = 8`. Operators
//! override via the `MITOS_DIALER_LANES` env var — set to `1`
//! to fall back to the pre-pool strictly-serial drain. See
//! `docs/design/DIALER_CONCURRENCY.md` for the rollout shape.
//!
//! Cursor behaviour at N>1: see the caveat on [`LaneConfig`].
//!
//! ## Status writes
//!
//! Lane workers don't write status updates directly to redb —
//! redb is single-writer, and parallel writes from N tasks would
//! contend. A `StatusWriter` task owns the store, drains a
//! single mpsc, and batches updates per tick. Lane workers send
//! `StatusUpdate` messages; the writer is the only thread that
//! touches the table.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use mitos_protocol::{
    ApplyBody, ApplyBulkRequest, BulkEmission, BulkEmissionResult, HTTP_DELIVERY_MIME, UndoBody,
    encode_apply, encode_apply_bulk, encode_undo,
};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::emissions::{EmissionRecord, EmissionStatus, EmissionsStore};

/// Bulk-apply capability cache states. One `AtomicU8` per
/// (companion, target) dial task, shared across its lanes.
/// `UNKNOWN` until the first bulk POST resolves it; `SUPPORTED`
/// after a 2xx (or transient 5xx — those keep retrying bulk);
/// `UNSUPPORTED` after a 404/415 (companion has no bulk route),
/// after which the task drains per-row for its lifetime.
pub(crate) const BULK_UNKNOWN: u8 = 0;
const BULK_SUPPORTED: u8 = 1;
const BULK_UNSUPPORTED: u8 = 2;

/// Bulk-apply batch sizing. The lane's per-tick row snapshot is the
/// implicit flush window (the doc's W); `max` is the per-POST cap M.
#[derive(Debug, Clone, Copy)]
pub struct BulkConfig {
    /// Max emissions per bulk POST. `1` disables bulk (each POST is
    /// one row — semantically the per-row path). Default 50.
    pub max: usize,
}

impl BulkConfig {
    /// Read `MITOS_BULK_BATCH_MAX` (default 50). Set to `1` to roll
    /// out the new path without changing throughput characteristics
    /// (per `DIALER_BULK_APPLY.md` Phase 2 step 9).
    pub fn from_env() -> Self {
        let max = std::env::var("MITOS_BULK_BATCH_MAX")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(50);
        Self { max }
    }
}

/// Pool configuration. Loaded from `mitos.toml` (see
/// [`Self::from_env`]) or constructed inline for tests.
#[derive(Debug, Clone, Copy)]
pub struct LaneConfig {
    /// Number of parallel lane workers per `(companion, target)`.
    /// `1` = strictly serial (pre-pool behaviour); higher =
    /// parallel dispatch by hash-of-partition-key.
    ///
    /// **Cursor caveat at N>1**: each row's `ApplyBody.cursor` is
    /// stamped with the row's own `chain_point`. With multiple
    /// lanes running in parallel, a fast lane may ack a high-slot
    /// row before a slow lane acks a lower-slot row, briefly
    /// regressing the companion's persisted cursor. The
    /// idempotent-`apply_event` contract absorbs this on host
    /// restart (the host re-emits anything past the last-persisted
    /// cursor; the companion re-applies those rows safely). A
    /// follow-up will add cursor-floor stamping to remove the
    /// regression — see `docs/design/DIALER_CONCURRENCY.md`.
    pub lanes: usize,
}

impl Default for LaneConfig {
    /// 8 lanes by default. Picked as a balance between
    /// parallelism (~8x speedup on recapture-shaped workloads
    /// where many keys are active) and not flooding any single
    /// companion's Worker endpoint with simultaneous POSTs. CF
    /// Workers handles this comfortably at the request budgets
    /// we run with.
    ///
    /// Operators override via the `MITOS_DIALER_LANES` env var
    /// (see [`Self::from_env`]); set to `1` to fall back to the
    /// pre-pool strictly-serial drain behaviour.
    fn default() -> Self {
        Self { lanes: 8 }
    }
}

impl LaneConfig {
    /// Read from the `MITOS_DIALER_LANES` env var if set; fall
    /// back to default (`lanes = 1`). Invalid values log a warn
    /// and fall back. Keeping the override env-var-shaped for
    /// now means no `mitos.toml` schema change is needed for this
    /// landing — the toml plumbing can come in a follow-up.
    pub fn from_env() -> Self {
        let raw = match std::env::var("MITOS_DIALER_LANES") {
            Ok(v) => v,
            Err(_) => return Self::default(),
        };
        match raw.trim().parse::<usize>() {
            Ok(n) if n >= 1 => Self { lanes: n },
            _ => {
                warn!(
                    raw = %raw,
                    "MITOS_DIALER_LANES is not a positive integer; using default (1)"
                );
                Self::default()
            }
        }
    }
}

/// One status transition to commit to the emissions store. The
/// status-writer task drains an mpsc of these.
#[derive(Debug)]
pub enum StatusUpdate {
    Pending(u64, String),
    Acked(u64, String),
    Queued(u64, String),
    Nacked(u64, String, String),
}

/// Handle to a running `StatusWriter` task.
///
/// `send` is best-effort — if the writer task has shut down the
/// channel closes and updates silently drop. That's the same
/// failure mode the pre-pool path had on store errors (warnings
/// logged, drain continues). Status drift is recovered by
/// `requeue_*_pending_for_companion` on next reconnect.
pub struct StatusWriterHandle {
    tx: mpsc::UnboundedSender<StatusUpdate>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl StatusWriterHandle {
    /// Clone the send side so a per-companion drain can enqueue
    /// status transitions. The per-module drain holds one writer
    /// for the whole module store and hands each spawned
    /// companion-drain its own cloned sender.
    pub fn sender(&self) -> mpsc::UnboundedSender<StatusUpdate> {
        self.tx.clone()
    }

    /// Drop the sender so the writer task drains and exits, then
    /// await its termination. Called from `run_module_drain` on
    /// cancellation so we don't leak the task.
    pub async fn shutdown(mut self) {
        drop(self.tx);
        if let Some(handle) = self.join.take() {
            let _ = handle.await;
        }
    }
}

/// Spawn the status-writer task. Returns a handle the lane
/// workers use to enqueue updates. The task owns the
/// [`EmissionsStore`] handle for writes — readers (the tick-time
/// queue scan) clone the store separately, since redb readers
/// don't contend with writers.
pub fn spawn_status_writer(store: EmissionsStore, cancel: CancellationToken) -> StatusWriterHandle {
    let (tx, mut rx) = mpsc::unbounded_channel::<StatusUpdate>();
    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                msg = rx.recv() => {
                    let Some(msg) = msg else {
                        return; // sender dropped on companion shutdown
                    };
                    apply_status_update(&store, msg);
                    // Drain any other updates queued behind this
                    // one in the same poll without re-entering
                    // the select! — keeps redb txn count low
                    // under bursty load.
                    while let Ok(extra) = rx.try_recv() {
                        apply_status_update(&store, extra);
                    }
                }
            }
        }
    });
    StatusWriterHandle {
        tx,
        join: Some(join),
    }
}

fn apply_status_update(store: &EmissionsStore, msg: StatusUpdate) {
    let (id, status, ts, err) = match msg {
        StatusUpdate::Pending(id, ts) => (id, EmissionStatus::Pending, ts, None),
        StatusUpdate::Acked(id, ts) => (id, EmissionStatus::Acked, ts, None),
        StatusUpdate::Queued(id, ts) => (id, EmissionStatus::Queued, ts, None),
        StatusUpdate::Nacked(id, ts, err) => (id, EmissionStatus::Nacked, ts, Some(err)),
    };
    if let Err(e) = store.update_status(id, status, &ts, err) {
        warn!(id, ?status, error = %e, "status writer redb update failed");
    }
}

/// Run one drain pass for a single companion: take its
/// pre-fetched, partition-key-grouped queued rows, hash-assign
/// each group to a lane slot, and drain the lanes in parallel.
///
/// Returns `Ok(())` if every dispatched lane completed without a
/// transport failure (422 nacks count as success — they're
/// application-level). Returns `Err` if *any* lane saw a 5xx /
/// transport error so the caller can apply per-companion backoff.
///
/// The rows are supplied by the per-module drain loop's single
/// store scan (`EmissionsStore::list_queued_grouped_by_companion`),
/// so this function does no redb reads itself — it's spawnable
/// (all fields owned + `'static`) and the module loop runs one per
/// active companion concurrently.
///
/// At `lanes = 1`, the function still groups (cheap) but ends up
/// dispatching every group into the single worker slot in a
/// deterministic id order — bit-exact with the pre-pool serial
/// drain.
pub struct TickArgs {
    pub client: reqwest::Client,
    pub apply_url: String,
    /// Bulk-apply URL (`apply_url` with `-bulk` before the query).
    /// Empty disables the bulk path.
    pub bulk_url: String,
    /// Undo URL (`POST /_internal/undo-<target>`) — delivers
    /// chain-rollback `is_undo` rows.
    pub undo_url: String,
    /// Per-companion bulk capability cache, shared across this
    /// companion's lanes (and across ticks). See [`BULK_UNKNOWN`].
    pub bulk_capability: Arc<AtomicU8>,
    /// Max emissions per bulk POST; `<= 1` disables bulk.
    pub bulk_max: usize,
    /// Channel / module name carried in the `ApplyBulkRequest`
    /// (informational — the consumer routes by the URL path).
    pub channel: String,
    pub header_name: Option<String>,
    pub header_value: Option<String>,
    /// Pre-fetched queued rows for this companion, grouped by
    /// `partition_key` (the form produced per-companion from the
    /// module-wide scan). Empty groups are skipped.
    pub grouped: Vec<(Vec<u8>, Vec<EmissionRecord>)>,
    pub companion_key: String,
    /// Cloned send side of the module's single status writer.
    pub status_tx: mpsc::UnboundedSender<StatusUpdate>,
    pub lanes: usize,
    pub now: fn() -> String,
}

pub async fn run_tick(args: TickArgs) -> anyhow::Result<()> {
    let grouped = args.grouped;
    if grouped.is_empty() {
        return Ok(());
    }
    let total_rows: usize = grouped.iter().map(|(_, v)| v.len()).sum();
    debug!(
        companion_key = %args.companion_key,
        lanes = args.lanes,
        keys = grouped.len(),
        rows = total_rows,
        "companion drain: applying queued emissions"
    );

    let n = args.lanes.max(1);
    // Bucket groups by slot. Within a bucket, rows from different
    // partition keys still stay grouped by their original key —
    // but since they end up on the same worker, they're drained
    // sequentially. Order across keys within a bucket is the
    // BTreeMap insertion order from the per-companion grouping,
    // which sorts by key bytes (empty key first).
    let mut buckets: Vec<Vec<EmissionRecord>> = (0..n).map(|_| Vec::new()).collect();
    for (key, rows) in grouped {
        let slot_idx = assign_lane(&key, n);
        buckets[slot_idx].extend(rows);
    }

    // Spawn one task per non-empty bucket. join_set so we can
    // await all in parallel + collect transport failures.
    let mut tasks = JoinSet::new();
    for (slot_idx, rows) in buckets.into_iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let client = args.client.clone();
        let apply_url = args.apply_url.clone();
        let bulk_url = args.bulk_url.clone();
        let undo_url = args.undo_url.clone();
        let bulk_capability = args.bulk_capability.clone();
        let bulk_max = args.bulk_max;
        let channel = args.channel.clone();
        let header_name = args.header_name.clone();
        let header_value = args.header_value.clone();
        let writer_tx = StatusWriterSender {
            inner: args.status_tx.clone(),
        };
        let companion_key = args.companion_key.clone();
        let now_fn = args.now;
        tasks.spawn(async move {
            drain_lane(LaneArgs {
                slot_idx,
                rows,
                client,
                apply_url,
                bulk_url,
                undo_url,
                bulk_capability,
                bulk_max,
                channel,
                header_name,
                header_value,
                companion_key,
                writer_tx,
                now_fn,
            })
            .await
        });
    }

    let mut transport_err: Option<anyhow::Error> = None;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                // Keep the first error; the others are likely the
                // same outage and noisier than informative.
                if transport_err.is_none() {
                    transport_err = Some(e);
                }
            }
            Err(join_err) => {
                if transport_err.is_none() {
                    transport_err = Some(anyhow::anyhow!("lane join: {join_err}"));
                }
            }
        }
    }

    match transport_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Lightweight clone-by-channel of the status writer's send side
/// so we don't have to make the whole `StatusWriterHandle` Send +
/// Clone (the `join` field would need an Arc + Mutex). Workers
/// only need the sender.
#[derive(Clone)]
struct StatusWriterSender {
    inner: mpsc::UnboundedSender<StatusUpdate>,
}

impl StatusWriterSender {
    fn send(&self, update: StatusUpdate) {
        let _ = self.inner.send(update);
    }
}

struct LaneArgs {
    slot_idx: usize,
    rows: Vec<EmissionRecord>,
    client: reqwest::Client,
    apply_url: String,
    bulk_url: String,
    undo_url: String,
    bulk_capability: Arc<AtomicU8>,
    bulk_max: usize,
    channel: String,
    header_name: Option<String>,
    header_value: Option<String>,
    companion_key: String,
    writer_tx: StatusWriterSender,
    now_fn: fn() -> String,
}

async fn drain_lane(args: LaneArgs) -> anyhow::Result<()> {
    let LaneArgs {
        slot_idx,
        rows,
        client,
        apply_url,
        bulk_url,
        undo_url,
        bulk_capability,
        bulk_max,
        channel,
        header_name,
        header_value,
        companion_key,
        writer_tx,
        now_fn,
    } = args;

    debug!(
        slot_idx,
        companion_key = %companion_key,
        rows = rows.len(),
        "lane drain start"
    );

    // Bulk path is taken unless M<=1, no bulk URL, the companion was
    // already found not to support bulk (cached UNSUPPORTED), or the
    // lane carries any undo row (undo can't be bulk-batched and must
    // keep strict id-order against the surrounding applies).
    let has_undo = rows.iter().any(|r| r.is_undo);
    let bulk_enabled = bulk_max > 1
        && !bulk_url.is_empty()
        && bulk_capability.load(Ordering::Relaxed) != BULK_UNSUPPORTED
        && !has_undo;

    if !bulk_enabled {
        return drain_rows_single(
            &rows,
            &client,
            &apply_url,
            &undo_url,
            header_name.as_deref(),
            header_value.as_deref(),
            &companion_key,
            &writer_tx,
            now_fn,
            slot_idx,
        )
        .await;
    }

    // Chunk the lane's rows into batches of `bulk_max`, one POST per
    // batch. Within-key order is preserved (rows arrive key-grouped
    // from `run_tick`), which is the only ordering that matters —
    // cross-key/cross-partition order is unconstrained by design.
    let mut start = 0;
    while start < rows.len() {
        let end = (start + bulk_max).min(rows.len());
        let chunk = &rows[start..end];
        match bulk_post_chunk(
            &client,
            &bulk_url,
            header_name.as_deref(),
            header_value.as_deref(),
            &channel,
            chunk,
            &writer_tx,
            now_fn,
        )
        .await
        {
            BulkOutcome::Applied => {
                bulk_capability.store(BULK_SUPPORTED, Ordering::Relaxed);
                start = end;
            }
            BulkOutcome::NoBulk => {
                bulk_capability.store(BULK_UNSUPPORTED, Ordering::Relaxed);
                debug!(
                    companion_key = %companion_key,
                    "companion has no bulk route; falling back to per-row drain for the lane remainder"
                );
                // Drain the current chunk + everything after it
                // per-row (the bulk POST left these rows Pending; the
                // single path re-marks + applies them).
                return drain_rows_single(
                    &rows[start..],
                    &client,
                    &apply_url,
                    &undo_url,
                    header_name.as_deref(),
                    header_value.as_deref(),
                    &companion_key,
                    &writer_tx,
                    now_fn,
                    slot_idx,
                )
                .await;
            }
            BulkOutcome::Transport(e) => return Err(e),
        }
    }
    Ok(())
}

/// What a per-row drain is delivering. Derived from
/// [`EmissionRecord::is_undo`]; selects the destination URL, the wire
/// body shape, and the log label. Forward (`Apply`) is the common path;
/// `Undo` carries a chain-rollback to a companion's `undo` hook.
///
/// Only the bulk-incompatible per-row path uses this — `Undo` rows force
/// a lane to drain per-row (see [`drain_lane`]) precisely because they
/// can't be `ApplyBulkRequest`-batched.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryKind {
    Apply,
    Undo,
}

impl DeliveryKind {
    fn of(row: &EmissionRecord) -> Self {
        if row.is_undo {
            Self::Undo
        } else {
            Self::Apply
        }
    }

    /// Lowercase label for log / error context.
    fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::Undo => "undo",
        }
    }

    /// Destination URL for this delivery.
    fn url<'a>(self, apply_url: &'a str, undo_url: &'a str) -> &'a str {
        match self {
            Self::Apply => apply_url,
            Self::Undo => undo_url,
        }
    }

    /// CBOR-encode `row` as the body for this delivery.
    fn encode_body(self, row: &EmissionRecord) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::Apply => encode_apply(&ApplyBody {
                emission_id: row.id,
                cursor: row.chain_point.clone(),
                change: row.payload.clone(),
            })
            .map_err(|e| anyhow::anyhow!("encode ApplyBody: {e}")),
            Self::Undo => encode_undo(&UndoBody {
                cursor: row.chain_point.clone(),
            })
            .map_err(|e| anyhow::anyhow!("encode UndoBody: {e}")),
        }
    }
}

/// Per-row drain — one HTTP POST per emission. The pre-bulk path,
/// retained as the fallback when a companion has no bulk route
/// (404/415) and the `bulk_max <= 1` opt-out.
#[allow(clippy::too_many_arguments)]
async fn drain_rows_single(
    rows: &[EmissionRecord],
    client: &reqwest::Client,
    apply_url: &str,
    undo_url: &str,
    header_name: Option<&str>,
    header_value: Option<&str>,
    companion_key: &str,
    writer_tx: &StatusWriterSender,
    now_fn: fn() -> String,
    slot_idx: usize,
) -> anyhow::Result<()> {
    for row in rows {
        let now = (now_fn)();
        writer_tx.send(StatusUpdate::Pending(row.id, now));

        // Forward rows deliver as `ApplyBody` to the apply URL; rollback
        // rows as `UndoBody` to the undo URL. A lane carrying any undo
        // row drains here (per-row) so id-order is preserved across the
        // apply→undo→apply boundary (bulk would reorder + can't mix).
        let kind = DeliveryKind::of(row);
        let body_bytes = kind.encode_body(row)?;
        let mut req_builder = client
            .post(kind.url(apply_url, undo_url))
            .header(reqwest::header::CONTENT_TYPE, HTTP_DELIVERY_MIME)
            .body(body_bytes);
        if let (Some(name), Some(value)) = (header_name, header_value) {
            req_builder = req_builder.header(name, value);
        }
        let resp = match req_builder.send().await {
            Ok(r) => r,
            Err(e) => {
                let now = (now_fn)();
                writer_tx.send(StatusUpdate::Queued(row.id, now));
                return Err(anyhow::anyhow!("{} POST send: {e}", kind.as_str()));
            }
        };
        let status = resp.status();
        let now = (now_fn)();
        if status.is_success() {
            writer_tx.send(StatusUpdate::Acked(row.id, now));
            continue;
        }
        // A companion with no `/_internal/undo-<channel>` route 404s undo
        // POSTs. That's expected for re-derivable consumers (they never
        // need undo — the re-applied forward frames reconverge them). Ack
        // it so it doesn't wedge the lane retrying forever. Only latching
        // consumers implement the route; for them a 200/422 comes back.
        if kind == DeliveryKind::Undo && status == reqwest::StatusCode::NOT_FOUND {
            debug!(
                id = row.id,
                slot_idx,
                companion_key = %companion_key,
                "companion has no undo route; skipping undo (re-derivable consumer)"
            );
            writer_tx.send(StatusUpdate::Acked(row.id, now));
            continue;
        }
        if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            let error = resp.text().await.unwrap_or_else(|_| String::new());
            warn!(
                id = row.id,
                slot_idx,
                companion_key = %companion_key,
                op = kind.as_str(),
                error = %error,
                "delivery returned 422; marking emission as Nacked"
            );
            writer_tx.send(StatusUpdate::Nacked(row.id, now, error));
            continue;
        }
        let body_text = resp.text().await.unwrap_or_default();
        writer_tx.send(StatusUpdate::Queued(row.id, now));
        return Err(anyhow::anyhow!(
            "{} POST returned status {status}: {body_text}",
            kind.as_str()
        ));
    }
    Ok(())
}

/// Pure demux of a bulk response onto the chunk's emission ids, in
/// chunk order. The load-bearing partial-success logic:
/// - `applied: true`  → `Acked`
/// - `applied: false` → `Nacked` (carries the rejection error)
/// - id **missing** from `results` → `Queued` (retry next tick;
///   covers a companion that truncated its response mid-batch)
/// - extra ids in `results` (not in the chunk) → ignored
fn demux_bulk_results(
    chunk_ids: &[u64],
    results: &[BulkEmissionResult],
    now: &str,
) -> Vec<StatusUpdate> {
    let by_id: HashMap<u64, &BulkEmissionResult> =
        results.iter().map(|r| (r.emission_id, r)).collect();
    chunk_ids
        .iter()
        .map(|id| match by_id.get(id) {
            Some(r) if r.applied => StatusUpdate::Acked(*id, now.to_string()),
            Some(r) => {
                StatusUpdate::Nacked(*id, now.to_string(), r.error.clone().unwrap_or_default())
            }
            None => StatusUpdate::Queued(*id, now.to_string()),
        })
        .collect()
}

/// Outcome of one bulk POST.
enum BulkOutcome {
    /// 2xx — per-emission results demuxed into the status writer.
    Applied,
    /// 404/415 — companion has no bulk route. Caller flips capability
    /// to UNSUPPORTED and drains per-row. Rows left Pending.
    NoBulk,
    /// 5xx / network / encode-decode — all rows re-Queued; lane
    /// returns this error so the caller backs off.
    Transport(anyhow::Error),
}

/// POST one batch of emissions to the companion's bulk endpoint and
/// demux the per-emission results into the status writer.
#[allow(clippy::too_many_arguments)]
async fn bulk_post_chunk(
    client: &reqwest::Client,
    bulk_url: &str,
    header_name: Option<&str>,
    header_value: Option<&str>,
    channel: &str,
    chunk: &[EmissionRecord],
    writer_tx: &StatusWriterSender,
    now_fn: fn() -> String,
) -> BulkOutcome {
    // Mark all Pending up front: a host crash mid-POST leaves them
    // Pending → requeued on task restart (same crash-safety the
    // per-row path gets).
    for row in chunk {
        writer_tx.send(StatusUpdate::Pending(row.id, (now_fn)()));
    }

    let emissions: Vec<BulkEmission> = chunk
        .iter()
        .map(|r| BulkEmission {
            emission_id: r.id,
            cursor: r.chain_point.clone(),
            change: r.payload.clone(),
        })
        .collect();
    let body = ApplyBulkRequest {
        channel: channel.to_string(),
        emissions,
    };
    let body_bytes = match encode_apply_bulk(&body) {
        Ok(b) => b,
        Err(e) => {
            for row in chunk {
                writer_tx.send(StatusUpdate::Queued(row.id, (now_fn)()));
            }
            return BulkOutcome::Transport(anyhow::anyhow!("encode ApplyBulkRequest: {e}"));
        }
    };

    let mut req_builder = client
        .post(bulk_url)
        .header(reqwest::header::CONTENT_TYPE, HTTP_DELIVERY_MIME)
        .body(body_bytes);
    if let (Some(name), Some(value)) = (header_name, header_value) {
        req_builder = req_builder.header(name, value);
    }
    let resp = match req_builder.send().await {
        Ok(r) => r,
        Err(e) => {
            for row in chunk {
                writer_tx.send(StatusUpdate::Queued(row.id, (now_fn)()));
            }
            return BulkOutcome::Transport(anyhow::anyhow!("bulk POST send: {e}"));
        }
    };

    let status = resp.status();
    // No bulk route (or wrong media type) → fall back to per-row.
    if status == reqwest::StatusCode::NOT_FOUND
        || status == reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE
    {
        return BulkOutcome::NoBulk;
    }
    if status.is_success() {
        let bytes = match resp.bytes().await {
            Ok(b) => b,
            Err(e) => {
                for row in chunk {
                    writer_tx.send(StatusUpdate::Queued(row.id, (now_fn)()));
                }
                return BulkOutcome::Transport(anyhow::anyhow!("read bulk response: {e}"));
            }
        };
        let parsed = match mitos_protocol::decode_apply_bulk_response(&bytes) {
            Ok(p) => p,
            Err(e) => {
                for row in chunk {
                    writer_tx.send(StatusUpdate::Queued(row.id, (now_fn)()));
                }
                return BulkOutcome::Transport(anyhow::anyhow!("decode bulk response: {e}"));
            }
        };
        let chunk_ids: Vec<u64> = chunk.iter().map(|r| r.id).collect();
        for update in demux_bulk_results(&chunk_ids, &parsed.results, &(now_fn)()) {
            writer_tx.send(update);
        }
        return BulkOutcome::Applied;
    }

    // 5xx / other non-2xx → all back to Queued, surface as transport
    // error so the caller applies backoff.
    let body_text = resp.text().await.unwrap_or_default();
    for row in chunk {
        writer_tx.send(StatusUpdate::Queued(row.id, (now_fn)()));
    }
    BulkOutcome::Transport(anyhow::anyhow!(
        "bulk POST returned status {status}: {body_text}"
    ))
}

/// Hash a partition key to a worker slot in `[0, n)`. Empty key
/// (= global lane) hashes to `0` deterministically so all global-
/// lane events land on the same slot at `n > 1`.
fn assign_lane(key: &[u8], n: usize) -> usize {
    debug_assert!(n >= 1);
    if key.is_empty() {
        return 0;
    }
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign_lane_n_eq_one_always_zero() {
        for key in [&b""[..], b"policy_a", b"policy_b"] {
            assert_eq!(assign_lane(key, 1), 0);
        }
    }

    #[test]
    fn assign_lane_empty_key_is_zero() {
        assert_eq!(assign_lane(b"", 4), 0);
        assert_eq!(assign_lane(b"", 16), 0);
    }

    #[test]
    fn assign_lane_deterministic() {
        let a = assign_lane(b"policy_a", 8);
        let b = assign_lane(b"policy_a", 8);
        assert_eq!(a, b, "same key always hashes to same slot");
    }

    #[test]
    fn assign_lane_spreads_across_slots() {
        let mut seen = std::collections::HashSet::new();
        for i in 0u32..1000 {
            let key = i.to_be_bytes();
            seen.insert(assign_lane(&key, 16));
        }
        // 1000 random-ish keys into 16 slots should cover every
        // slot. If hashing degenerated, this would fail.
        assert_eq!(seen.len(), 16);
    }

    #[test]
    fn lane_config_from_env_defaults() {
        // SAFETY: tests in same process share env; this test sets
        // and clears its own var to avoid bleeding into siblings.
        // (cargo test runs tests in threads by default, but they
        // share env — keep these isolated by var name.)
        // SAFETY: single-threaded var manipulation, no concurrent reads.
        unsafe {
            std::env::remove_var("MITOS_DIALER_LANES");
        }
        let cfg = LaneConfig::from_env();
        // Tracks `LaneConfig::default()`. If the default ever
        // changes, update both together.
        assert_eq!(cfg.lanes, 8);
    }

    #[test]
    fn lane_config_from_env_parses_valid() {
        // SAFETY: single-threaded var manipulation; restore default after.
        unsafe {
            std::env::set_var("MITOS_DIALER_LANES_TEST_VALID", "8");
        }
        // Pull manually so we don't depend on the canonical name
        // (other tests may race on it).
        let raw = std::env::var("MITOS_DIALER_LANES_TEST_VALID").unwrap();
        let parsed: usize = raw.parse().unwrap();
        assert_eq!(parsed, 8);
        // SAFETY: cleanup.
        unsafe {
            std::env::remove_var("MITOS_DIALER_LANES_TEST_VALID");
        }
    }

    fn result(id: u64, applied: bool, error: Option<&str>) -> BulkEmissionResult {
        BulkEmissionResult {
            emission_id: id,
            applied,
            error: error.map(|s| s.to_string()),
        }
    }

    #[test]
    fn demux_all_applied() {
        let updates = demux_bulk_results(
            &[1, 2, 3],
            &[
                result(1, true, None),
                result(2, true, None),
                result(3, true, None),
            ],
            "t",
        );
        assert!(
            updates
                .iter()
                .all(|u| matches!(u, StatusUpdate::Acked(_, _)))
        );
        assert_eq!(updates.len(), 3);
    }

    #[test]
    fn demux_mixed_applied_and_rejected() {
        let updates = demux_bulk_results(
            &[1, 2, 3],
            &[
                result(1, true, None),
                result(2, false, Some("datum mismatch")),
                result(3, true, None),
            ],
            "t",
        );
        assert!(matches!(updates[0], StatusUpdate::Acked(1, _)));
        match &updates[1] {
            StatusUpdate::Nacked(id, _, err) => {
                assert_eq!(*id, 2);
                assert_eq!(err, "datum mismatch");
            }
            other => panic!("expected Nacked, got {other:?}"),
        }
        assert!(matches!(updates[2], StatusUpdate::Acked(3, _)));
    }

    #[test]
    fn demux_missing_id_is_requeued() {
        // Companion truncated — id 2 omitted from results. It must be
        // re-Queued (retry), not silently lost or marked applied.
        let updates = demux_bulk_results(
            &[1, 2, 3],
            &[result(1, true, None), result(3, true, None)],
            "t",
        );
        assert!(matches!(updates[0], StatusUpdate::Acked(1, _)));
        assert!(matches!(updates[1], StatusUpdate::Queued(2, _)));
        assert!(matches!(updates[2], StatusUpdate::Acked(3, _)));
    }

    #[test]
    fn demux_extra_id_is_ignored() {
        // Result for id 99 (not in the chunk) is ignored; output is
        // exactly one update per chunk id, in chunk order.
        let updates = demux_bulk_results(
            &[1, 2],
            &[
                result(1, true, None),
                result(99, true, None),
                result(2, false, Some("x")),
            ],
            "t",
        );
        assert_eq!(updates.len(), 2);
        assert!(matches!(updates[0], StatusUpdate::Acked(1, _)));
        assert!(matches!(updates[1], StatusUpdate::Nacked(2, _, _)));
    }

    #[test]
    fn bulk_config_defaults_to_50() {
        // SAFETY: single-threaded var manipulation in test.
        unsafe {
            std::env::remove_var("MITOS_BULK_BATCH_MAX");
        }
        assert_eq!(BulkConfig::from_env().max, 50);
    }

    #[test]
    fn status_update_variants_translate_cleanly() {
        // Compile-time check that the conversion in
        // `apply_status_update` covers every variant. If a new
        // variant is added to `StatusUpdate`, this match goes
        // non-exhaustive at compile time.
        fn _exhaustive(u: StatusUpdate) {
            match u {
                StatusUpdate::Pending(_, _) => {}
                StatusUpdate::Acked(_, _) => {}
                StatusUpdate::Queued(_, _) => {}
                StatusUpdate::Nacked(_, _, _) => {}
            }
        }
    }
}
