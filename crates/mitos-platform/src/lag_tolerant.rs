//! `LagTolerantSubscription` — a `TipSubscription` that doesn't
//! panic when the underlying broadcast::Receiver lags.
//!
//! Why this exists:
//!
//! Dolos's `TipSubscription::next_tip` does
//! `self.receiver.recv().await.unwrap()` (`dolos
//! v1.0.3/src/adapters/mod.rs`). `tokio::sync::broadcast::Receiver::recv`
//! returns `Err(RecvError::Lagged(n))` when the receiver falls
//! more than `channel_capacity` events behind the sender — a
//! recoverable condition (semantically: "you missed n events,
//! re-fetch from your last-known position"). The `unwrap`
//! converts it into a panic, killing the consumer task.
//!
//! For wasm-module followers this fires when the initial WAL
//! replay (`watch_tip(Some(cursor))` with `cursor` more than
//! ~100 slots behind tip) takes longer than the broadcast's
//! buffer can hold.
//!
//! This wrapper:
//! - Subscribes to `tip_broadcast` directly (skipping dolos's
//!   broken adapter wrapper)
//! - Holds an optional replay queue (initial WAL gap or
//!   lag-recovery refill)
//! - On `Lagged(n)`, queries the WAL for the missed range
//!   starting from the last-seen cursor and refills the replay
//!   queue
//! - On `Closed` (sender dropped — happens during Domain
//!   shutdown), blocks forever so the caller's cancel token
//!   handles termination cleanly
//!
//! The trade-off vs. fixing dolos upstream: this is a focused
//! ~80-line shim that lives entirely in our crate. Upstream PR
//! to dolos is the right long-term fix; this unblocks the
//! deployment story today.

use std::collections::VecDeque;
use std::sync::Arc;

use dolos_core::{ChainPoint, Domain, TipEvent, TipSubscription, WalStore};
use tokio::sync::broadcast::{self, error::RecvError};

/// Raw block bytes are arc-wrapped in dolos's wire format.
type ArcBlock = Arc<Vec<u8>>;

/// A `TipSubscription` impl that handles `broadcast::RecvError`
/// gracefully instead of panicking. Generic over the `Domain`
/// so it can refetch missed blocks from the WAL on lag.
pub struct LagTolerantSubscription<D: Domain> {
    domain: Arc<D>,
    replay: VecDeque<(ChainPoint, ArcBlock)>,
    receiver: broadcast::Receiver<TipEvent>,
    last_seen: Option<ChainPoint>,
}

impl<D: Domain> LagTolerantSubscription<D> {
    /// Construct a fresh subscription. Mirrors the work
    /// `Domain::watch_tip` does upstream (subscribe + collect
    /// replay) but with the panic-prone `unwrap` swapped for
    /// proper recovery.
    ///
    /// `tip_broadcast` is the `broadcast::Sender` from
    /// `DomainAdapter`. Caller passes `tip_broadcast.subscribe()`
    /// directly — keeps this generic and avoids depending on
    /// `DomainAdapter`'s concrete type.
    pub fn new(
        domain: Arc<D>,
        tip_broadcast: &broadcast::Sender<TipEvent>,
        from: Option<ChainPoint>,
    ) -> Result<Self, dolos_core::WalError> {
        // Subscribe FIRST so we don't miss any events that fire
        // while we're collecting the replay — same ordering as
        // dolos's adapter (and same race-window caveat documented
        // there).
        let receiver = tip_broadcast.subscribe();
        let replay: VecDeque<(ChainPoint, ArcBlock)> = domain
            .wal()
            .iter_blocks(from.clone(), None)?
            .collect();
        Ok(Self {
            domain,
            replay,
            receiver,
            last_seen: from,
        })
    }

    /// Refill the replay queue from the WAL after a lag event.
    /// Called when the broadcast receiver returned
    /// `RecvError::Lagged(n)` — we know we missed `n` events,
    /// the WAL has them, fetch from `last_seen` forward.
    fn refill_after_lag(&mut self, missed: u64) {
        match self
            .domain
            .wal()
            .iter_blocks(self.last_seen.clone(), None)
        {
            Ok(iter) => {
                let collected: VecDeque<_> = iter.collect();
                tracing::warn!(
                    missed,
                    refilled = collected.len(),
                    last_seen = ?self.last_seen,
                    "broadcast lagged; refilled replay queue from WAL"
                );
                self.replay = collected;
            }
            Err(e) => {
                tracing::error!(
                    missed,
                    error = %e,
                    "broadcast lagged AND WAL refill failed; will resync on next event"
                );
            }
        }
    }
}

impl<D: Domain + 'static> TipSubscription for LagTolerantSubscription<D> {
    async fn next_tip(&mut self) -> TipEvent {
        loop {
            // Replay queue first (initial-from-cursor or
            // post-lag refill).
            if let Some((point, block)) = self.replay.pop_front() {
                self.last_seen = Some(point.clone());
                return TipEvent::Apply(point, block);
            }

            match self.receiver.recv().await {
                Ok(event) => {
                    // Track last-seen cursor for future lag
                    // recovery. `Apply` and `Undo` carry the
                    // chain point; `Mark` is just a checkpoint.
                    if let Some(p) = event_point(&event) {
                        self.last_seen = Some(p);
                    }
                    return event;
                }
                Err(RecvError::Lagged(n)) => {
                    // Don't panic. Log + refill from WAL +
                    // continue the loop so the caller sees the
                    // refilled events as Apply.
                    self.refill_after_lag(n);
                    continue;
                }
                Err(RecvError::Closed) => {
                    // Sender (DomainAdapter::tip_broadcast) was
                    // dropped — Domain is shutting down. We
                    // can't return a "None" variant from
                    // next_tip; block forever and let the
                    // caller's cancel token terminate the task.
                    tracing::warn!("tip_broadcast sender closed; blocking — expect cancel");
                    return std::future::pending().await;
                }
            }
        }
    }
}

fn event_point(event: &TipEvent) -> Option<ChainPoint> {
    match event {
        TipEvent::Apply(p, _) => Some(p.clone()),
        TipEvent::Undo(p, _) => Some(p.clone()),
        TipEvent::Mark(p) => Some(p.clone()),
    }
}
