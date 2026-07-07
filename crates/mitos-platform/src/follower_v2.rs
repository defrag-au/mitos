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
use pallas_traverse::MultiEraBlock;
use tokio_util::sync::CancellationToken;

use crate::PlatformResult;
use crate::driver_v2::DriverV2;
use crate::host_v2::{InterestUpdate, KvFactory};
use crate::storage::ModuleStorage;
use crate::trap_context::{TrapContextLogger, write_fixture};

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
///
/// `interest_rx` carries dynamic-interest updates from the
/// companion-WS dialer (one frame per `ClientMessage::Interest`
/// arrival). The follower multiplexes these with tip events so
/// new predicates take effect on the next block boundary.
///
/// `kv_factory` is used by the dynamic-interest path to open a
/// short-lived `ModuleKv` handle for bootstrap completion flags
/// when an Add op brings a new address/policy into the interest
/// set. The factory's underlying redb handle is cached
/// (`ModuleStorage::kv_store`) so the multiple opens across
/// follower lifetime don't fault on redb's single-writer rule.
#[allow(clippy::too_many_arguments)]
pub async fn run_chain_follower_v2<S, P>(
    mut driver: DriverV2,
    mut subscription: S,
    mut interest_rx: tokio::sync::mpsc::UnboundedReceiver<InterestUpdate>,
    cancel: CancellationToken,
    data_plane: Arc<P>,
    storage: ModuleStorage,
    module_id: String,
    kv_factory: KvFactory,
    trap_logger: Arc<TrapContextLogger>,
    event_ring: Option<crate::events::EventRing>,
    chunked_cold_start: bool,
    backfill: crate::backfill_v2::BackfillFactory,
) -> PlatformResult<()>
where
    S: TipSubscription,
    P: ChainDataPlane + Sync + Send + 'static,
{
    tracing::info!("v2 chain-follower started");
    // Open the platform-wide aux-data cache once before the loop.
    // Blocks with aux_data are harvested proactively after each
    // successful apply so future bootstraps can resolve without
    // the dolos archive. Failure here is non-fatal — the fallback
    // is the dolos archive (or Maestro in Phase 2).
    let aux_cache = storage.indexer_data_cache().ok();
    loop {
        // Multiplex tip events with interest updates. `biased`
        // gives tip events priority — dynamic interest is rare
        // (companion subscribe / unsubscribe), chain blocks
        // are the steady-state load.
        let event = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("v2 follower: cancelled");
                return Ok(());
            }
            update = interest_rx.recv() => {
                match update {
                    Some(update) => {
                        apply_interest_update(
                            &mut driver,
                            &module_id,
                            update,
                            data_plane.as_ref(),
                            &kv_factory,
                            chunked_cold_start,
                            &backfill,
                            event_ring.as_ref(),
                        )
                        .await;
                        continue;
                    }
                    None => {
                        // Sender dropped — `ModuleHostV2::stop`
                        // signals shutdown by closing the
                        // channel. Clean exit.
                        tracing::info!(
                            module = %module_id,
                            "v2 interest channel closed; follower exiting",
                        );
                        return Ok(());
                    }
                }
            }
            event = subscription.next_tip() => event,
        };
        match event {
            TipEvent::Apply(point, block_cbor) => {
                let block_bytes: &[u8] = block_cbor.as_ref();
                // Pin the in-flight block CBOR + slot to the trap
                // log so a wasm trap during this dispatch can be
                // replayed locally via
                // `mitos-run --fixture last-trap.toml`. Clear
                // first so successful prior blocks don't pollute
                // this batch's trap context.
                trap_logger.clear();
                let cp: mitos_data_plane::ChainPoint = point.clone().into();
                trap_logger.set_block(cp.slot(), block_bytes.to_vec());
                match driver.apply_block(block_bytes, data_plane.as_ref()).await {
                    Ok(outcome) => {
                        tracing::trace!(?point, ?outcome, "v2 follower: applied",);
                        // Proactively cache aux_data from this block
                        // while the CBOR is still in scope.
                        if let Some(ref cache) = aux_cache {
                            harvest_block_aux_data(block_bytes, cache);
                        }
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
                        // Wasm trap from the module during
                        // steady-state dispatch. Same fixture-
                        // write path init uses: snapshot the
                        // logger (block CBOR + data-plane call
                        // log) and write to `last-trap.toml` so
                        // the operator can replay locally.
                        let snap = trap_logger.snapshot();
                        let path = storage.last_trap_path(&module_id);
                        match write_fixture(&snap, &module_id, &path) {
                            Ok(()) => tracing::error!(
                                module = %module_id,
                                fixture = %path.display(),
                                ?point,
                                "v2 dispatch trapped; fixture written for local replay (mitos-run --fixture)",
                            ),
                            Err(write_err) => tracing::warn!(
                                module = %module_id,
                                error = %write_err,
                                ?point,
                                "v2 dispatch trapped; failed to write trap fixture",
                            ),
                        }
                        // Surface the trap on the operational events
                        // ring so `mitos-admin tail` / `GET
                        // /_admin/events` see it without a journal grep.
                        if let Some(ring) = &event_ring {
                            ring.record(
                                module_id.clone(),
                                crate::events::EventKind::Trap {
                                    reason: e.to_string(),
                                },
                            );
                        }
                        // Supervisor wiring is a v2.x follow-up;
                        // for now surface to the spawning task.
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

/// Scan a raw block CBOR and batch-insert all TX aux_data entries
/// into the cache. Errors are silently discarded — a cache miss
/// on the next bootstrap is the only consequence.
fn harvest_block_aux_data(block_bytes: &[u8], cache: &crate::indexer_data_cache::IndexerDataCache) {
    let block = match MultiEraBlock::decode(block_bytes) {
        Ok(b) => b,
        Err(_) => return,
    };
    let entries: Vec<([u8; 32], Vec<u8>)> = block
        .txs()
        .into_iter()
        .filter_map(|tx| {
            let cbor = mitos_data_plane::extract_aux_cbor(&tx)?;
            let hash: [u8; 32] = tx.hash().as_ref().try_into().ok()?;
            Some((hash, cbor))
        })
        .collect();
    cache.insert_aux_batch(&entries);
}

/// Apply one dynamic-interest update: mutate the driver's
/// `InterestSet` per the op, fire bootstrap for any newly-added
/// predicates that have a current-state hydration path, then
/// forward the same predicates (already CBOR-encoded by the
/// router) to the module's `update-interest` export so it can
/// persist via state-kv.
///
/// Errors from the module's export are logged but don't kill
/// the follower — the host's filter is the source of truth for
/// dispatch, the module's persisted copy is a best-effort
/// resilience aid.
#[allow(clippy::too_many_arguments)]
async fn apply_interest_update<P: ChainDataPlane + Sync + Send + 'static>(
    driver: &mut DriverV2,
    module_id: &str,
    update: InterestUpdate,
    plane: &P,
    kv_factory: &KvFactory,
    chunked_cold_start: bool,
    backfill: &crate::backfill_v2::BackfillFactory,
    event_ring: Option<&crate::events::EventRing>,
) {
    use mitos_protocol::InterestOp as WireOp;

    // 1. Apply the op to the driver's InterestSet so subsequent
    //    block-dispatch filtering reflects the new predicates.
    let mut current = driver.interest();
    match update.op {
        // Resync leaves the host filter net-identical to Add: the
        // "treat as new" semantics live entirely in the module-export
        // call sequence below.
        WireOp::Add | WireOp::Resync => {
            for p in &update.predicates {
                // Dedupe: a re-subscribe of an already-watched
                // predicate is a no-op rather than a duplicate
                // entry. Keeps `predicates.iter().any(...)`
                // matching cheap.
                if !current.predicates.iter().any(|existing| existing == p) {
                    current.add(p.clone());
                }
            }
        }
        WireOp::Remove => {
            for p in &update.predicates {
                current.remove(p);
            }
        }
        WireOp::Replace => {
            current.replace(update.predicates.clone());
        }
    }
    driver.set_interest(current);

    // 2. Forward to the module's update-interest export FIRST, so a
    //    page-oriented module records the newly-added policies as a
    //    scoped cold-start (read back when we pump `rebootstrap`
    //    below). Runs for every op to keep the module's persisted
    //    interest in sync. Map the wire op to the bindgen call
    //    sequence (same variants, different generated types).
    //
    //    `Resync` composes `Remove` ∘ `Add`: the Remove makes the
    //    module drop the scope from its persisted tracked set, so the
    //    immediately-following Add sees it as genuinely new and seeds
    //    the scoped-onboard set — the Onboard pump below then re-walks
    //    the scope and re-emits its snapshot to every subscriber
    //    (snapshot apply is an idempotent full-replace on consumers).
    //    Both calls happen inside this one InterestUpdate, which the
    //    follower processes between blocks — no dispatch interleaves,
    //    so there is no missed-event window. This is the same
    //    sequence the manual per-policy resync dance exercised via
    //    two HTTP round-trips, made atomic. No WIT change: the module
    //    only ever sees the existing Add/Remove ops.
    let call_ops: &[crate::bindings_v2::InterestOp] = match update.op {
        WireOp::Add => &[crate::bindings_v2::InterestOp::Add],
        WireOp::Remove => &[crate::bindings_v2::InterestOp::Remove],
        WireOp::Replace => &[crate::bindings_v2::InterestOp::Replace],
        WireOp::Resync => &[
            crate::bindings_v2::InterestOp::Remove,
            crate::bindings_v2::InterestOp::Add,
        ],
    };
    for bindgen_op in call_ops {
        match driver
            .call_update_interest(*bindgen_op, &update.items_cbor)
            .await
        {
            Ok(Ok(())) => {
                tracing::debug!(
                    module = %module_id,
                    op = ?update.op,
                    call = ?bindgen_op,
                    "v2 interest update applied",
                );
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    module = %module_id,
                    op = ?update.op,
                    call = ?bindgen_op,
                    error = %e,
                    "module returned Err from update-interest; host filter still updated",
                );
            }
            Err(e) => {
                tracing::warn!(
                    module = %module_id,
                    op = ?update.op,
                    call = ?bindgen_op,
                    error = %e,
                    "update-interest trapped or fuel-exhausted; host filter still updated",
                );
            }
        }
    }

    // 3. Cold-start newly-added predicates. `Remove` never hydrates.
    //    Both `Add` and `Replace` can introduce genuinely-new policies
    //    (Replace is what the boot-time reconcile routes to drop
    //    orphans + assert the live companion set). The module's
    //    `update-interest` seeds ONLY the newly-added policies into the
    //    onboard scope, so a re-assert of already-tracked policies
    //    seeds nothing and the Onboard pump below is a no-op. Two
    //    paths, gated by `chunked_cold_start`:
    if matches!(update.op, WireOp::Add | WireOp::Replace | WireOp::Resync) {
        // Seed the watched-UTxO index for the added/asserted scopes
        // (refs-only, flag-gated, once per scope). This MUST happen on
        // the live `driver` here — the chunked cold-start below runs on
        // a disposable backfill instance that never touches this index,
        // and the per-scope bootstrap flag means the synthetic-dispatch
        // path is skipped on restart re-subscribe. Without this, a
        // dynamic-interest module's pre-existing live UTxOs stay
        // unindexed and the gate would wrongly skip them. See
        // `docs/design/WATCHED_UTXO_INDEX.md`.
        if let Some(index) = driver.watched_index() {
            let mut seed_kv = kv_factory(module_id);
            for predicate in &update.predicates {
                crate::bootstrap_v2::seed_predicate_watched(
                    index.as_ref(),
                    predicate,
                    &mut seed_kv,
                    module_id,
                    plane,
                )
                .await;
            }
        }
        if chunked_cold_start {
            // Page-oriented modules (collection-holders/metadata)
            // seeded a scoped onboard set in update-interest above
            // (on the live `driver`, persisted to the shared state-kv).
            // The chunked `rebootstrap` cold-start runs on a DISPOSABLE
            // backfill instance, NOT this live `driver` — the scan is
            // unbounded and may trap, and a trap poisons the wasm
            // instance, which would brick live dispatch for every
            // collection on this module. The backfill instance reads
            // the onboard scope + writes shards through the shared
            // `Arc<Database>` state-kv, so the live driver sees the
            // refilled holder map on the next event. See
            // `COLD_START_TRAP_ISOLATION.md`.
            //
            // ONBOARD mode: the pump scans ONLY the just-seeded onboard
            // scope. An empty scope (a no-op re-assert) is a no-op,
            // NEVER a full re-scan of every tracked policy — that's
            // recapture's job (`RebootstrapMode::Full`). This is what
            // stops a boot reconcile / re-subscribe from re-scanning
            // (and re-resolving via Maestro) every collection.
            let outcome = crate::backfill_v2::run_rebootstrap_pump(
                backfill,
                crate::bindings_v2::RebootstrapMode::Onboard,
            )
            .await;
            if outcome.ingested > 0 {
                tracing::info!(
                    module = %module_id,
                    utxos = outcome.ingested,
                    steps = outcome.steps,
                    rebuilds = outcome.rebuilds,
                    outcome = outcome.outcome,
                    "v2 onboard cold-start complete (backfill plane)",
                );
            }
            // Record onboard health to the events ring (the recapture
            // path already records; onboard was previously blind to
            // `/_admin/events` + `/metrics`, only in journald). Records
            // every run, including no-ops, so the cadence is visible.
            if let Some(ring) = event_ring {
                ring.record(
                    module_id,
                    crate::events::EventKind::OnboardCompleted {
                        utxos_ingested: outcome.ingested,
                        outcome: outcome.outcome.to_owned(),
                    },
                );
            }
        } else if matches!(update.op, WireOp::Add | WireOp::Resync) {
            // Resync included so a first-ever subscribe (which now
            // routes Resync — `resume_from: None`) still fires the
            // synthetic bootstrap for non-chunked modules. For an
            // already-hydrated scope the per-scope state-kv flag
            // makes this a no-op: non-chunked modules emit per-TX
            // events, not snapshots, so there is no baseline to
            // replay and re-firing history would duplicate
            // change-log rows on consumers.
            // Every other module: the host's synthetic-event
            // bootstrap (unchanged). Pumping `rebootstrap` here would
            // double-fire a module that also cold-starts inline (e.g.
            // vesting-tracker), so it's opt-in via the manifest flag.
            // Idempotent via the per-scope state-kv flag.
            let mut bootstrap_kv = kv_factory(module_id);
            for predicate in &update.predicates {
                match crate::bootstrap_v2::bootstrap_one_predicate(
                    driver,
                    module_id,
                    &mut bootstrap_kv,
                    predicate,
                    plane,
                )
                .await
                {
                    Ok(stats) => {
                        if stats.utxos_dispatched > 0 {
                            tracing::info!(
                                module = %module_id,
                                predicate = ?predicate,
                                utxos = stats.utxos_dispatched,
                                batches = stats.batches_dispatched,
                                "v2 dynamic-interest bootstrap complete (synthetic)",
                            );
                        }
                    }
                    Err(e) => tracing::error!(
                        module = %module_id,
                        predicate = ?predicate,
                        error = %e,
                        "v2 dynamic-interest bootstrap failed; live dispatch still applies",
                    ),
                }
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
