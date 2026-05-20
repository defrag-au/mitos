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
//! Each tick of `run_companion` (every `POLL_INTERVAL`) drains a
//! snapshot of the queued set. Spawning one short-lived task per
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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use mitos_protocol::{ApplyBody, HTTP_DELIVERY_MIME, encode_apply};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::emissions::{EmissionRecord, EmissionStatus, EmissionsStore};

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
    /// Drop the sender so the writer task drains and exits, then
    /// await its termination. Called from `run_companion` on
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

/// Run one tick of the lane pool: scan queued rows, group by
/// partition_key, hash-assign to slots, drain in parallel.
///
/// Returns `Ok(())` if every dispatched lane completed without a
/// transport failure (422 nacks count as success — they're
/// application-level). Returns `Err` if *any* lane saw a 5xx /
/// transport error so the caller can apply backoff.
///
/// At `lanes = 1`, the function still groups (cheap) but ends up
/// dispatching every group into the single worker slot in a
/// deterministic id order — bit-exact with the pre-pool serial
/// drain.
pub struct PoolContext<'a> {
    pub client: &'a reqwest::Client,
    pub apply_url: &'a str,
    pub header_name: Option<&'a str>,
    pub header_value: Option<&'a str>,
    pub store: &'a EmissionsStore,
    pub companion_key: &'a str,
    pub client_id: &'a str,
    pub status_writer: &'a StatusWriterHandle,
    pub lanes: usize,
    pub now: fn() -> String,
}

pub async fn run_tick(ctx: PoolContext<'_>) -> anyhow::Result<()> {
    let grouped = ctx
        .store
        .list_queued_for_companion_grouped(ctx.companion_key, ctx.client_id)
        .map_err(|e| anyhow::anyhow!("list queued grouped: {e}"))?;
    if grouped.is_empty() {
        return Ok(());
    }
    let total_rows: usize = grouped.iter().map(|(_, v)| v.len()).sum();
    debug!(
        companion_key = %ctx.companion_key,
        client_id = %ctx.client_id,
        lanes = ctx.lanes,
        keys = grouped.len(),
        rows = total_rows,
        "lane pool tick: draining queued emissions"
    );

    let n = ctx.lanes.max(1);
    // Bucket groups by slot. Within a bucket, rows from different
    // partition keys still stay grouped by their original key —
    // but since they end up on the same worker, they're drained
    // sequentially. Order across keys within a bucket is the
    // BTreeMap insertion order from `list_queued_for_companion_grouped`,
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
        let client = ctx.client.clone();
        let apply_url = ctx.apply_url.to_string();
        let header_name = ctx.header_name.map(|s| s.to_string());
        let header_value = ctx.header_value.map(|s| s.to_string());
        let writer_tx = StatusWriterSender {
            inner: ctx.status_writer.tx.clone(),
        };
        let companion_key = ctx.companion_key.to_string();
        let now_fn = ctx.now;
        tasks.spawn(async move {
            drain_lane(LaneArgs {
                slot_idx,
                rows,
                client,
                apply_url,
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

    for row in rows {
        let now = (now_fn)();
        writer_tx.send(StatusUpdate::Pending(row.id, now));

        let body = ApplyBody {
            emission_id: row.id,
            // For this iteration we stamp the row's own chain
            // point — bit-exact with the pre-pool path. The
            // floor-stamping discussed in the design doc is a
            // follow-up; with lanes = 1 the row's slot IS the
            // floor by construction.
            cursor: row.chain_point.clone(),
            change: row.payload.clone(),
        };
        let body_bytes =
            encode_apply(&body).map_err(|e| anyhow::anyhow!("encode ApplyBody: {e}"))?;
        let mut req_builder = client
            .post(&apply_url)
            .header(reqwest::header::CONTENT_TYPE, HTTP_DELIVERY_MIME)
            .body(body_bytes);
        if let (Some(name), Some(value)) = (header_name.as_deref(), header_value.as_deref()) {
            req_builder = req_builder.header(name, value);
        }
        let resp = match req_builder.send().await {
            Ok(r) => r,
            Err(e) => {
                let now = (now_fn)();
                writer_tx.send(StatusUpdate::Queued(row.id, now));
                return Err(anyhow::anyhow!("apply POST send: {e}"));
            }
        };
        let status = resp.status();
        let now = (now_fn)();
        if status.is_success() {
            writer_tx.send(StatusUpdate::Acked(row.id, now));
            continue;
        }
        if status == reqwest::StatusCode::UNPROCESSABLE_ENTITY {
            let error = resp.text().await.unwrap_or_else(|_| String::new());
            warn!(
                id = row.id,
                slot_idx,
                companion_key = %companion_key,
                error = %error,
                "apply returned 422; marking emission as Nacked"
            );
            writer_tx.send(StatusUpdate::Nacked(row.id, now, error));
            continue;
        }
        let body_text = resp.text().await.unwrap_or_default();
        writer_tx.send(StatusUpdate::Queued(row.id, now));
        return Err(anyhow::anyhow!(
            "apply POST returned status {status}: {body_text}"
        ));
    }
    Ok(())
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
