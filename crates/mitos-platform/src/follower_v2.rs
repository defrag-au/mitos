//! v2 chain follower — pumps `TipEvent`s through `DriverV2`.
//!
//! Sister to `follower.rs` (v1). Same shape, different driver:
//! - `Apply(point, cbor)` → `DriverV2::apply_block(cbor, plane)`
//! - `Undo(point, _)` → `DriverV2::dispatch_rollback(point)`
//! - `Mark(point)` → cursor checkpoint only (no module dispatch)
//!
//! v2 dynamic interest (companion → host via WS → module) is a
//! follow-up: the v1 wire enum (`mitos_protocol::Interest`) was
//! designed around protocol-event filtering and doesn't map
//! cleanly onto v2's `InterestPredicate` vocabulary. For now
//! interest is set once at follower start (from manifest config
//! or programmatically by tools like `mitos-run`); the runtime-
//! mutable update path lands when the v2 wire protocol does.

use std::sync::Arc;

use dolos_core::{TipEvent, TipSubscription};
use mitos_data_plane::ChainDataPlane;
use tokio_util::sync::CancellationToken;

use crate::driver_v2::DriverV2;
use crate::storage::ModuleStorage;
use crate::PlatformResult;

/// Run the v2 chain follower until cancelled or the tip
/// channel closes.
///
/// `data_plane` is the `ChainDataPlane` impl the dispatch path
/// uses to resolve prior outputs during event-batch building.
/// Same plane the host was instantiated with.
///
/// `storage` + `module_id` are used to flush the driver's
/// post-apply cursor to disk after every successful Apply /
/// Mark / Undo. Without this, restart loses progress and
/// `auto_resume` resumes from `None` (= origin / WAL replay).
pub async fn run_chain_follower_v2<S, P>(
    mut driver: DriverV2,
    mut subscription: S,
    cancel: CancellationToken,
    data_plane: Arc<P>,
    storage: ModuleStorage,
    module_id: String,
) -> PlatformResult<()>
where
    S: TipSubscription,
    P: ChainDataPlane + Sync + Send + 'static,
{
    tracing::info!("v2 chain-follower started");
    loop {
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("v2 follower: cancelled");
                return Ok(());
            }
            event = subscription.next_tip() => event,
        };
        match event {
            TipEvent::Apply(point, block_cbor) => {
                let block_bytes: &[u8] = block_cbor.as_ref();
                match driver.apply_block(block_bytes, data_plane.as_ref()).await {
                    Ok(outcome) => {
                        tracing::trace!(
                            ?point,
                            ?outcome,
                            "v2 follower: applied",
                        );
                        flush_cursor(&storage, &module_id, &driver);
                    }
                    Err(crate::PlatformError::Decode(e)) => {
                        // Match v1's posture: host-side decode
                        // failure is a bad block, not a module
                        // bug. Skip + warn, keep follower alive.
                        tracing::warn!(
                            ?point,
                            error = %e,
                            "v2 follower: host-side block decode failed; skipping",
                        );
                    }
                    Err(e) => {
                        // Wasm trap from the module — supervisor
                        // wiring is a v2.x follow-up; for now
                        // surface to the spawning task.
                        return Err(e);
                    }
                }
            }
            TipEvent::Undo(point, _) => {
                let to_cursor: mitos_data_plane::ChainPoint = point.into();
                if let Err(e) = driver.dispatch_rollback(to_cursor.clone()).await {
                    tracing::error!(
                        ?to_cursor,
                        error = %e,
                        "v2 follower: rollback dispatch trapped",
                    );
                    return Err(crate::PlatformError::Wasmtime(e));
                }
                tracing::info!(?to_cursor, "v2 follower: rollback dispatched");
                flush_cursor(&storage, &module_id, &driver);
            }
            TipEvent::Mark(point) => {
                tracing::trace!(?point, "v2 follower: mark");
                // No dispatch on Mark, but still flush — Mark is
                // the cursor-only checkpoint that lets a restart
                // resume from a known point even when no module-
                // visible blocks have arrived.
                flush_cursor(&storage, &module_id, &driver);
            }
        }
    }
}

/// Persist the driver's post-apply cursor to module storage.
/// Best-effort: a transient redb error is logged but doesn't
/// kill the follower (next successful flush will overwrite).
fn flush_cursor(storage: &ModuleStorage, module_id: &str, driver: &DriverV2) {
    let Some(cursor) = driver.cursor() else {
        return;
    };
    if let Err(e) = storage.write_cursor(module_id, cursor) {
        tracing::warn!(
            module = %module_id,
            error = %e,
            "v2 follower: cursor flush failed; will retry on next event",
        );
    }
}
