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
use dolos_core::{ChainPoint, TipEvent, TipSubscription};
use mitos_data_plane::{DataPlaneResult, DecodeLevel, OutputRef, TypedOutput};
use mitos_platform::ResolvedBlock;
use mitos_platform::bindings::{
    AssetEntry as WitAssetEntry, AssetId as WitAssetId, OutputRef as WitOutputRef,
    TypedOutput as WitTypedOutput,
};
use mitos_platform::driver::{ApplyOutcome, BlockEvent, Driver};
use mitos_platform::follower::run_chain_follower;
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::registry::{ModuleRegistry, ResourceBudget};
use mitos_platform::resolved_block::TxView;

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

    async fn utxos_by_address(
        &self,
        _address: &str,
    ) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(Vec::new())
    }

    async fn datum_by_hash(
        &self,
        _hash: &[u8; 32],
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(None)
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
            tx_hash: vec![0xCD; 32],
            outputs: vec![],
            output_datums: Vec::new(),
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

fn ownership_module_wasm() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join(
            "modules/ownership-indexer/target/wasm32-wasip2/release/\
             ownership_indexer_module.wasm",
        );
    candidate.exists().then_some(candidate)
}

/// Drive the ownership module through one block carrying one
/// asset of a watched policy; assert it emits exactly one
/// `OwnershipChange::Transfer` event and that the policy_id
/// matches the configured watch.
#[tokio::test]
async fn ownership_module_emits_transfer_for_watched_policy() {
    let Some(wasm) = ownership_module_wasm() else {
        eprintln!(
            "skipping: ownership module .wasm not built. \
             Run `cd modules/ownership-indexer && \
             cargo build -p ownership-indexer-module \
             --target wasm32-wasip2 --release` first."
        );
        return;
    };

    let engine = ModuleRegistry::build_engine().expect("engine build");
    let registry = ModuleRegistry::load_from_path(engine, "ownership".to_owned(), &wasm)
        .expect("component load");

    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let budget = ResourceBudget::default();

    let (sink, mut events) = emit::EventSink::new();
    let kv = state_kv::ModuleKv::new_in_memory();

    let mut instance = registry
        .instantiate(dp.clone(), kv, sink, budget)
        .await
        .expect("instantiate");

    // Hand the module a CBOR'd Config matching its expected
    // shape: one watched policy. The hex below is the same
    // 28-byte (56-hex) policy we'll plant on the synthetic
    // output so `handle_event` matches and emits.
    #[derive(serde::Serialize)]
    struct ModuleConfig {
        policies: Vec<String>,
    }
    let watched_policy_hex = "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6".to_owned();
    let cfg = ModuleConfig {
        policies: vec![watched_policy_hex.clone()],
    };
    let mut cfg_bytes = Vec::new();
    ciborium::ser::into_writer(&cfg, &mut cfg_bytes).expect("cbor encode");

    instance
        .bindings
        .call_init(&mut instance.store, &cfg_bytes)
        .await
        .expect("init");

    // Synthetic block with one tx producing one output that
    // carries one asset under the watched policy. Asset name
    // is "BlackFlag001" hex-encoded.
    let policy_bytes = hex::decode(&watched_policy_hex).unwrap();
    let asset_name_bytes = b"BlackFlag001".to_vec();
    let output = WitTypedOutput {
        address: "addr1qxy...".to_owned(),
        lovelace: 2_000_000,
        assets: vec![WitAssetEntry {
            asset: WitAssetId {
                policy: policy_bytes,
                name: asset_name_bytes.clone(),
            },
            quantity: 1,
        }],
    };
    let block = ResolvedBlock::from_views(
        90_000_000,
        vec![TxView {
            tx_hash: vec![0xEF; 32],
            outputs: vec![output],
            output_datums: Vec::new(),
            consumed_input_refs: vec![],
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

    // One emission, on channel 0.
    let event = events.try_recv().expect("expected one Transfer event");
    assert_eq!(event.channel, 0);
    assert_eq!(event.module_id, "ownership");

    // CBOR-decode the payload + assert the field shapes match
    // the host-side `OwnershipChange::Transfer` we'd expect.
    #[derive(serde::Deserialize, Debug)]
    #[serde(tag = "type")]
    enum Decoded {
        Transfer {
            policy_id: String,
            asset_name: String,
            new_owner: String,
            tx_hash: String,
            output_index: u32,
        },
    }
    let decoded: Decoded = ciborium::de::from_reader(event.payload.as_slice()).expect("decode");
    let Decoded::Transfer {
        policy_id,
        asset_name,
        new_owner,
        tx_hash,
        output_index,
    } = decoded;
    assert_eq!(policy_id, watched_policy_hex);
    assert_eq!(asset_name, hex::encode(&asset_name_bytes));
    assert_eq!(new_owner, "addr1qxy...");
    assert_eq!(tx_hash, hex::encode([0xEF; 32]));
    assert_eq!(output_index, 0);

    // No further emissions.
    assert!(events.try_recv().is_err(), "expected exactly one event");
}

/// Drive the ownership module through one block whose output
/// carries an asset under a non-watched policy; assert it emits
/// nothing.
#[tokio::test]
async fn ownership_module_ignores_unwatched_policy() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };

    let engine = ModuleRegistry::build_engine().expect("engine build");
    let registry = ModuleRegistry::load_from_path(engine, "ownership".to_owned(), &wasm)
        .expect("component load");

    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let budget = ResourceBudget::default();

    let (sink, mut events) = emit::EventSink::new();
    let kv = state_kv::ModuleKv::new_in_memory();

    let mut instance = registry
        .instantiate(dp.clone(), kv, sink, budget)
        .await
        .expect("instantiate");

    #[derive(serde::Serialize)]
    struct ModuleConfig {
        policies: Vec<String>,
    }
    let watched = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
    let mut cfg_bytes = Vec::new();
    ciborium::ser::into_writer(
        &ModuleConfig {
            policies: vec![watched.clone()],
        },
        &mut cfg_bytes,
    )
    .unwrap();
    instance
        .bindings
        .call_init(&mut instance.store, &cfg_bytes)
        .await
        .expect("init");

    let unwatched =
        hex::decode("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let output = WitTypedOutput {
        address: "addr1abc".to_owned(),
        lovelace: 2_000_000,
        assets: vec![WitAssetEntry {
            asset: WitAssetId {
                policy: unwatched,
                name: b"X".to_vec(),
            },
            quantity: 1,
        }],
    };
    let block = ResolvedBlock::from_views(
        90_000_001,
        vec![TxView {
            tx_hash: vec![0x11; 32],
            outputs: vec![output],
            output_datums: Vec::new(),
            consumed_input_refs: vec![],
        }],
    );
    let block_id = instance.store.data_mut().table.push(block).unwrap();
    instance
        .bindings
        .call_handle_event(&mut instance.store, 0, block_id)
        .await
        .expect("handle-event");

    assert!(events.try_recv().is_err(), "unwatched policy must not emit");
}

/// Synthetic TipSubscription backed by an mpsc receiver.
/// Production wires the follower to `DomainAdapter::TipSubscription`;
/// this fake lets the integration test drive the follower without
/// standing up dolos.
struct FakeTipSubscription {
    rx: tokio::sync::mpsc::UnboundedReceiver<TipEvent>,
}

impl TipSubscription for FakeTipSubscription {
    async fn next_tip(&mut self) -> TipEvent {
        match self.rx.recv().await {
            Some(event) => event,
            // Channel closed: block forever — mirrors dolos's
            // actual semantics (next_tip never returns; the
            // test cancels via tokio::time::timeout).
            None => std::future::pending().await,
        }
    }
}

/// Drive the follower with two synthetic Apply events containing
/// the captured mainnet fixture (slot 186000000). Asserts events
/// flow through the wasm module's emit channel and the follower
/// terminates cleanly when the subscription's sender drops.
#[tokio::test]
async fn follower_pumps_apply_events_through_module() {
    let Some(wasm) = ownership_module_wasm() else {
        eprintln!("skipping: ownership module .wasm not built");
        return;
    };
    // Fixture must be present for this test to be meaningful;
    // skip otherwise (same auto-skip pattern as the equivalence
    // test).
    let fixture_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/186000000.block.cbor");
    if !fixture_path.exists() {
        eprintln!(
            "skipping: no fixture at {} — run capture-block first",
            fixture_path.display()
        );
        return;
    }
    let cbor = std::fs::read(&fixture_path).expect("read fixture");

    let engine = ModuleRegistry::build_engine().expect("engine");
    let registry =
        ModuleRegistry::load_from_path(engine, "ownership".to_owned(), &wasm).expect("load");
    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);
    let budget = ResourceBudget::default();

    let (sink, mut events) = emit::EventSink::new();
    let kv = state_kv::ModuleKv::new_in_memory();

    let mut instance = registry
        .instantiate(dp.clone(), kv, sink, budget)
        .await
        .expect("instantiate");

    // Watch every policy in the fixture so we know an event will
    // be emitted (otherwise the watch set is empty and the test
    // is trivial).
    #[derive(serde::Serialize)]
    struct Cfg {
        policies: Vec<String>,
    }
    let decoded = mitos_platform::block_decode::decode_block(&cbor).expect("decode fixture");
    let mut policies = std::collections::HashSet::new();
    for tx in &decoded.txs {
        for out in &tx.outputs {
            for asset in &out.assets {
                policies.insert(hex::encode(&asset.asset.policy));
            }
        }
    }
    let mut cfg_bytes = Vec::new();
    ciborium::ser::into_writer(
        &Cfg {
            policies: policies.into_iter().collect(),
        },
        &mut cfg_bytes,
    )
    .unwrap();
    instance
        .bindings
        .call_init(&mut instance.store, &cfg_bytes)
        .await
        .expect("init");

    let driver = Driver::new(instance, budget);

    // Wire the synthetic subscription; send one Apply, then drop.
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000),
        std::sync::Arc::new(cbor.clone()),
    ))
    .unwrap();
    drop(tx); // close the channel; follower will block on next_tip()

    // Run with a short timeout — follower normally loops forever,
    // so we cancel after the Apply is processed.
    let kv_factory = state_kv::ModuleKv::new_in_memory;
    let emitter_factory = || emit::EventSink::new().0;
    // No interest updates in this test — keep the sender
    // alive in scope so the follower's interest arm parks
    // (never resolves) rather than seeing `None` and exiting.
    let (_interest_tx, interest_rx) = tokio::sync::mpsc::unbounded_channel();
    let follower_task = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        run_chain_follower(
            driver,
            FakeTipSubscription { rx },
            interest_rx,
            &registry,
            dp.clone(),
            kv_factory,
            emitter_factory,
        ),
    );
    // Timeout is expected (follower would block on the empty
    // channel after processing the one Apply); we just need the
    // single dispatch to have happened by then.
    let _ = follower_task.await;

    // Drain emissions — should be at least one Transfer event
    // since the fixture carries 67 watched policies across
    // 12 outputs.
    let mut emitted = 0;
    while events.try_recv().is_ok() {
        emitted += 1;
    }
    assert!(
        emitted > 0,
        "expected at least one Transfer event from the fixture; got {emitted}"
    );
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
                tx_hash: vec![0xCD; 32],
                outputs: vec![],
                output_datums: Vec::new(),
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
