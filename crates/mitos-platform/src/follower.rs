//! Chain-follower driver: pumps a `TipSubscription` (dolos's
//! tip-event source) into a wasm-module `Driver`.
//!
//! Mirrors the existing `mitos-core::dispatcher::run_dispatcher`
//! pattern but for the wasm-module path:
//!   - `Apply(point, cbor)` → `Driver::apply_cbor`
//!   - `Undo(point, cbor)` → log + skip (out of v1 scope; v2
//!     wires undo into the supervisor)
//!   - `Mark(point)` → cursor checkpoint, no dispatch
//!
//! Generic over `TipSubscription` rather than concretely typed
//! to `DomainAdapter` so callers can wire to any `Domain` impl
//! (production: `dolos::adapters::DomainAdapter`; tests: a
//! synthetic mpsc-backed fake).
//!
//! `RestartedRetry` outcomes are handled by re-feeding the same
//! event in a tight inner loop; the supervisor's bounded-retry
//! eventually transitions to `RestartAndReplay` (which does a
//! single instance-restart-and-retry cycle), or to
//! `Skipped`/`Quarantined`. The outer event loop only sees
//! "this event is done one way or another" outcomes.

use std::sync::Arc;

use dolos_core::{TipEvent, TipSubscription};

use crate::driver::{ApplyOutcome, Driver};
use crate::host::InterestUpdate;
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::registry::ModuleRegistry;
use crate::{PlatformError, PlatformResult};

/// Drive `Driver` from a `TipSubscription` indefinitely,
/// multiplexed with dynamic-interest updates from the
/// `InterestRouter` control channel.
///
/// `interest_rx` carries `Vec<Interest>` blobs forwarded by the
/// dialer when a `ClientMessage::Interest` frame arrives over a
/// companion's WS. The follower drains it via `tokio::select!`
/// so interest updates land between block applies (never
/// preempting an in-flight dispatch — `select!` arms run
/// sequentially in the same task).
///
/// Returns:
/// - `Ok(())` on subscription channel close OR on
///   `interest_rx` close (sender dropped — happens in
///   `ModuleHost::stop`)
/// - `Err(PlatformError::Quarantined)` if the supervisor
///   decides the module is unusable; caller surfaces to the
///   operator
///
/// Cursor advancement is owned by the `Driver`; this loop is
/// the dispatch fabric only.
pub async fn run_chain_follower<S>(
    mut driver: Driver,
    mut subscription: S,
    mut interest_rx: tokio::sync::mpsc::UnboundedReceiver<InterestUpdate>,
    registry: &ModuleRegistry,
    data_plane: Arc<dyn DataPlaneFacade>,
    kv_factory: impl Fn() -> state_kv::ModuleKv + Clone,
    emitter_factory: impl Fn() -> emit::EventSink + Clone,
) -> PlatformResult<()>
where
    S: TipSubscription,
{
    tracing::info!("chain-follower started");
    loop {
        // Multiplex tip events with interest updates. `biased`
        // gives tip events priority — dynamic interest is rare
        // (companion subscribe / unsubscribe), chain blocks
        // are the steady-state load.
        let event = tokio::select! {
            biased;
            event = subscription.next_tip() => event,
            update = interest_rx.recv() => {
                match update {
                    Some(update) => {
                        if let Err(e) = driver
                            .update_interest(update.op, update.items)
                            .await
                        {
                            tracing::warn!(
                                error = %e,
                                "module update_interest failed; interest set may be stale until next update",
                            );
                        }
                        continue;
                    }
                    None => {
                        // Sender dropped — `ModuleHost::stop`
                        // signals shutdown by closing the channel.
                        tracing::info!("interest channel closed; follower exiting");
                        return Ok(());
                    }
                }
            }
        };
        match event {
            TipEvent::Apply(point, block_cbor) => {
                // `block_cbor` is `Arc<Vec<u8>>` — borrow as a slice
                // for the inner re-feed loop (no clone per iteration).
                let block_bytes: &[u8] = block_cbor.as_ref();
                // Inner re-feed loop: the supervisor's `Retry`
                // and `RestartAndReplay` outcomes both surface as
                // `ApplyOutcome::RestartedRetry` and the contract
                // is "re-feed the same block". Loop until we get
                // a terminal outcome.
                loop {
                    let outcome = match driver
                        .apply_cbor(
                            registry,
                            data_plane.clone(),
                            kv_factory.clone(),
                            emitter_factory.clone(),
                            block_bytes,
                            point.clone(),
                        )
                        .await
                    {
                        Ok(o) => o,
                        // Host-side decode failure — match the
                        // static indexers' posture: skip + warn,
                        // keep the follower alive. The
                        // alternative (exit + cursor-stuck) is
                        // worse because mitos has at least one
                        // block format in retention pallas can't
                        // decode (logged at WARN by marketplace_indexer
                        // since 2026-05-02). Decode is a host
                        // concern, not the module's — supervisor
                        // policy doesn't apply.
                        Err(crate::PlatformError::Decode(e)) => {
                            tracing::warn!(
                                ?point,
                                error = %e,
                                "host-side block decode failed; skipping",
                            );
                            break;
                        }
                        Err(e) => return Err(e),
                    };
                    match outcome {
                        ApplyOutcome::Applied => {
                            tracing::trace!(?point, "applied");
                            break;
                        }
                        ApplyOutcome::Skipped => {
                            tracing::warn!(?point, "skipped failing block");
                            break;
                        }
                        ApplyOutcome::RestartedRetry => {
                            tracing::debug!(?point, "supervisor retry; re-feeding");
                            continue;
                        }
                        ApplyOutcome::Quarantined => {
                            return Err(PlatformError::Quarantined { failures: 1 });
                        }
                    }
                }
            }
            TipEvent::Undo(point, _block) => {
                // V1 doesn't model undo to the module — the WIT
                // doesn't expose an undo channel, and the
                // ownership module's emit shape is forward-only.
                // V2 will plumb `TipEvent::Undo` through to a
                // `handle-event(channel=undo, ...)` dispatch.
                tracing::warn!(
                    ?point,
                    "Undo received — out of v1 scope, cursor not rolled back"
                );
            }
            TipEvent::Mark(point) => {
                // Cursor checkpoint with no content. V1 just
                // logs; a future enhancement would call a
                // `Driver::mark(cursor)` to persist the
                // checkpoint without dispatching.
                tracing::trace!(?point, "Mark");
            }
        }
    }
}
