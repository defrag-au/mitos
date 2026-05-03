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
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::registry::ModuleRegistry;
use crate::{PlatformError, PlatformResult};

/// Drive `Driver` from a `TipSubscription` indefinitely.
///
/// Returns:
/// - `Ok(())` only on subscription channel close (sender dropped)
/// - `Err(PlatformError::Quarantined)` if the supervisor decides
///   the module is unusable; caller surfaces to the operator
///
/// Cursor advancement is owned by the `Driver`; this loop is
/// the dispatch fabric only.
pub async fn run_chain_follower<S>(
    mut driver: Driver,
    mut subscription: S,
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
        let event = subscription.next_tip().await;
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
                    let outcome = driver
                        .apply_cbor(
                            registry,
                            data_plane.clone(),
                            kv_factory.clone(),
                            emitter_factory.clone(),
                            block_bytes,
                            point.clone(),
                        )
                        .await?;
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
