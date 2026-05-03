//! End-to-end integration test for the platform's load +
//! instantiate + dispatch path.
//!
//! Reuses the spike guest at `spikes/wit-spike/` rather than
//! building a dedicated test module — the spike's WIT was
//! deliberately compatible with the platform WIT (same
//! `mitos:platform`-style shape, same exports). If this test
//! ever drifts because the platform WIT moves ahead of the
//! spike's, the right fix is to rebuild the spike against the
//! platform WIT, not to fork.
//!
//! What this proves:
//! - `ModuleRegistry::load_from_path` compiles a real component
//! - `instantiate` performs the ABI handshake + reads trap policy
//! - the bindgen-generated `call_init` + `call_handle_event`
//!   actually dispatch into a guest
//! - the `ResourceTable`-backed `ResolvedBlock` round-trips
//!   through `borrow<>` correctly
//! - the supervisor wires up cleanly
//!
//! Skipped automatically if the spike artifact isn't built —
//! avoids a hard test-time dep. Run the spike first:
//!
//! ```bash
//! cd spikes/wit-spike && cargo build --target wasm32-wasip2 --release
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dolos_core::ChainPoint;
use mitos_data_plane::{DataPlaneResult, DecodeLevel, OutputRef, TypedOutput};
use mitos_platform::bindings::OutputRef as WitOutputRef;
use mitos_platform::driver::{ApplyOutcome, BlockEvent, Driver};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::registry::{ModuleRegistry, ResourceBudget};
use mitos_platform::resolved_block::TxView;
use mitos_platform::ResolvedBlock;

/// Stub data plane that records calls but returns nothing. The
/// spike guest's `handle-event` doesn't currently call read-utxos,
/// so this is enough to prove the wiring.
struct NullDataPlane;

#[async_trait]
impl DataPlaneFacade for NullDataPlane {
    async fn read_utxos(
        &self,
        _refs: &[OutputRef],
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        Ok(Vec::new())
    }
}

fn spike_guest_wasm() -> Option<PathBuf> {
    // From `crates/mitos-platform/tests/integration.rs` up to
    // the workspace root, then over to the spike's release dir.
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join("spikes/wit-spike/target/wasm32-wasip2/release/mitos_platform_spike_guest.wasm");
    if candidate.exists() {
        Some(candidate)
    } else {
        None
    }
}

#[tokio::test]
async fn end_to_end_load_instantiate_dispatch() {
    let Some(wasm) = spike_guest_wasm() else {
        eprintln!(
            "skipping: spike guest .wasm not built. \
             Run `cd spikes/wit-spike && cargo build --target wasm32-wasip2 --release` first."
        );
        return;
    };

    let engine = ModuleRegistry::build_engine().expect("engine build");
    let registry = ModuleRegistry::load_from_path(engine, "spike-guest".to_owned(), &wasm)
        .expect("component load");

    let (sink, mut events) = emit::EventSink::new();
    let kv = state_kv::ModuleKv::new_in_memory();
    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);

    let mut instance = registry
        .instantiate(dp, kv, sink, ResourceBudget::default())
        .await
        .expect("instantiate");

    // Init handshake.
    instance
        .bindings
        .call_init(&mut instance.store, &[])
        .await
        .expect("init");

    // Push a synthetic ResolvedBlock with one tx carrying one
    // input ref so the spike guest's `get_consumed_input(0, 0)`
    // call has a slot to address. NullDataPlane returns nothing,
    // so the lookup memoises `None` — exercises the cache miss
    // path without needing a real chain backend.
    let synthetic_input = WitOutputRef {
        tx_hash: vec![0xAB; 32],
        index: 0,
    };
    let block = ResolvedBlock::from_views(
        12_345_678,
        vec![TxView {
            consumed_input_refs: vec![synthetic_input],
        }],
    );
    let block_id = instance
        .store
        .data_mut()
        .table
        .push(block)
        .expect("push resource");

    instance
        .bindings
        .call_handle_event(&mut instance.store, 0, block_id)
        .await
        .expect("handle-event");

    // Drain any emitted events. Spike guest doesn't emit; assert
    // empty so a regression that wires up emission would surface.
    assert!(events.try_recv().is_err(), "spike guest must not emit");
}

/// Driver-level test: drive two consecutive blocks through the
/// dispatch loop and assert the cursor advances on each. Proves
/// the per-call refuel + cursor-advance path works end-to-end.
#[tokio::test]
async fn driver_advances_cursor_across_blocks() {
    let Some(wasm) = spike_guest_wasm() else {
        eprintln!(
            "skipping: spike guest .wasm not built. \
             Run `cd spikes/wit-spike && cargo build --target wasm32-wasip2 --release` first."
        );
        return;
    };

    let engine = ModuleRegistry::build_engine().expect("engine build");
    let registry = ModuleRegistry::load_from_path(engine, "spike-guest".to_owned(), &wasm)
        .expect("component load");

    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let budget = ResourceBudget::default();

    let kv_factory = state_kv::ModuleKv::new_in_memory;
    let emitter_factory = || emit::EventSink::new().0;

    let instance = registry
        .instantiate(dp.clone(), kv_factory(), emitter_factory(), budget)
        .await
        .expect("instantiate");

    // Init handshake (driver itself doesn't gate this; in real
    // wiring it'll happen at registration).
    let mut instance = instance;
    instance
        .bindings
        .call_init(&mut instance.store, &[])
        .await
        .expect("init");

    let mut driver = Driver::new(instance, budget);

    let synthetic_input = WitOutputRef {
        tx_hash: vec![0xAB; 32],
        index: 0,
    };

    for slot in [12_345_678u64, 12_345_679u64] {
        let event = BlockEvent::Apply {
            slot,
            cursor_after: ChainPoint::Slot(slot),
            txs: vec![TxView {
                consumed_input_refs: vec![synthetic_input.clone()],
            }],
        };
        let outcome = driver
            .apply(&registry, dp.clone(), kv_factory, emitter_factory, event)
            .await
            .expect("apply");
        assert_eq!(outcome, ApplyOutcome::Applied);
        assert!(matches!(driver.cursor(), Some(ChainPoint::Slot(s)) if *s == slot));
    }
}
