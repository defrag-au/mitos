//! In-memory ring of operationally-interesting events.
//!
//! The structured equivalent of tailing the journal for the lines an
//! operator actually cares about (recaptures, traps). A bounded ring
//! of typed events, read by `GET /_admin/events` and surfaced as
//! `mitos-admin tail` — so "did that recapture finish / did anything
//! trap?" is a query, not a `journalctl | grep`.
//!
//! Bounded + lossy by design: it retains the most recent
//! [`RING_CAPACITY`] events and drops the oldest. It is NOT an audit
//! log (that's the journal) and NOT a source of cumulative counters
//! (those are separate atomics for `/metrics`) — it's the recent feed.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

/// Max events retained. ~512 covers a long operator session without
/// unbounded growth; older events age out FIFO.
const RING_CAPACITY: usize = 512;

/// A typed operational event. Internally tagged on `kind` and
/// flattened into [`AdminEvent`], so the wire shape is
/// `{seq, ts_unix, module, kind, <variant fields>}`.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventKind {
    /// An admin-triggered recapture began.
    RecaptureStarted { reason: Option<String> },
    /// A recapture finished successfully.
    RecaptureCompleted {
        companions_targeted: usize,
        duration_ms: u64,
    },
    /// A recapture errored (timeout / coordination failure).
    RecaptureFailed { error: String, duration_ms: u64 },
    /// A wasm module trapped during dispatch (out-of-fuel, realloc,
    /// or a module-side panic). `reason` is the host error text; the
    /// full replay fixture lands at `last-trap.toml` (see `last-trap`).
    Trap { reason: String },
}

impl EventKind {
    /// Stable lowercase tag for `?kind=` filtering, matching the
    /// serialized `kind` field.
    pub fn tag(&self) -> &'static str {
        match self {
            EventKind::RecaptureStarted { .. } => "recapture_started",
            EventKind::RecaptureCompleted { .. } => "recapture_completed",
            EventKind::RecaptureFailed { .. } => "recapture_failed",
            EventKind::Trap { .. } => "trap",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AdminEvent {
    /// Monotonic sequence number. Poll cursor for `?after=`.
    pub seq: u64,
    /// Unix epoch seconds when recorded.
    pub ts_unix: u64,
    /// Module the event pertains to.
    pub module: String,
    #[serde(flatten)]
    pub kind: EventKind,
}

/// Clone-able handle to the shared ring. One instance is created at
/// bundle startup and shared (Arc-backed) between the admin router
/// (reads via `GET /_admin/events`) and the host/follower (record).
#[derive(Clone)]
pub struct EventRing {
    events: Arc<Mutex<VecDeque<AdminEvent>>>,
    seq: Arc<AtomicU64>,
}

impl Default for EventRing {
    fn default() -> Self {
        Self {
            events: Arc::new(Mutex::new(VecDeque::with_capacity(RING_CAPACITY))),
            seq: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl EventRing {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event: assign a monotonic `seq` + unix timestamp,
    /// dropping the oldest event when at capacity. Lock poisoning is
    /// swallowed (observability must never break the hot path).
    pub fn record(&self, module: impl Into<String>, kind: EventKind) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let ts_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let event = AdminEvent {
            seq,
            ts_unix,
            module: module.into(),
            kind,
        };
        if let Ok(mut q) = self.events.lock() {
            if q.len() >= RING_CAPACITY {
                q.pop_front();
            }
            q.push_back(event);
        }
    }

    /// Highest `seq` assigned so far — the cursor a poller passes back
    /// as `?after=` to receive only strictly-newer events.
    pub fn latest_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Events with `seq >= after`, optionally filtered by `module` +
    /// `kind` tag, oldest-first, capped at `limit`.
    pub fn snapshot(
        &self,
        after: u64,
        module: Option<&str>,
        kind: Option<&str>,
        limit: usize,
    ) -> Vec<AdminEvent> {
        let Ok(q) = self.events.lock() else {
            return Vec::new();
        };
        q.iter()
            .filter(|e| e.seq >= after)
            .filter(|e| module.is_none_or(|m| e.module == m))
            .filter(|e| kind.is_none_or(|k| e.kind.tag() == k))
            .take(limit)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_snapshots_in_order() {
        let ring = EventRing::new();
        ring.record("m1", EventKind::Trap { reason: "boom".into() });
        ring.record(
            "m2",
            EventKind::RecaptureCompleted {
                companions_targeted: 3,
                duration_ms: 12,
            },
        );
        let all = ring.snapshot(0, None, None, 100);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].seq, 0);
        assert_eq!(all[0].module, "m1");
        assert_eq!(all[1].seq, 1);
        assert_eq!(ring.latest_seq(), 2);
    }

    #[test]
    fn after_cursor_returns_only_newer() {
        let ring = EventRing::new();
        ring.record("m", EventKind::Trap { reason: "a".into() });
        let cursor = ring.latest_seq();
        ring.record("m", EventKind::Trap { reason: "b".into() });
        let newer = ring.snapshot(cursor, None, None, 100);
        assert_eq!(newer.len(), 1);
        assert_eq!(newer[0].seq, 1);
    }

    #[test]
    fn filters_by_module_and_kind() {
        let ring = EventRing::new();
        ring.record("a", EventKind::Trap { reason: "x".into() });
        ring.record(
            "b",
            EventKind::RecaptureStarted { reason: None },
        );
        assert_eq!(ring.snapshot(0, Some("a"), None, 100).len(), 1);
        assert_eq!(ring.snapshot(0, None, Some("trap"), 100).len(), 1);
        assert_eq!(ring.snapshot(0, Some("a"), Some("recapture_started"), 100).len(), 0);
    }

    #[test]
    fn evicts_oldest_at_capacity() {
        let ring = EventRing::new();
        for _ in 0..RING_CAPACITY + 10 {
            ring.record("m", EventKind::Trap { reason: "x".into() });
        }
        let all = ring.snapshot(0, None, None, RING_CAPACITY * 2);
        assert_eq!(all.len(), RING_CAPACITY);
        // Oldest 10 evicted: lowest retained seq is 10.
        assert_eq!(all[0].seq, 10);
    }
}
