//! Dynamic interest end-to-end test for the v2 platform.
//!
//! Verifies the full path: companion-shaped wire frame
//! (`Vec<mitos_protocol::Interest>`) arrives at the host's
//! `InterestRouter::route_interest`, gets translated to v2
//! `InterestPredicate`s, applied to the driver's `InterestSet`,
//! and the *next* block-dispatch sees emissions for the newly-
//! added policy that the *previous* block-dispatch wouldn't
//! have produced (because the policy wasn't watched yet).
//!
//! Skips cleanly when the test-indexer wasm fixture isn't built.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use cardano_assets::PolicyId;
use dolos_core::TipEvent;
use mitos_data_plane::ChainPoint;
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{
    EmitterFactory, InterestRouter, KvFactory, ModuleHostV2, SubscriptionFactory,
};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::{AssetSelector, Interest as WireInterest, InterestOp as WireInterestOp};
use tokio::sync::Mutex;

use common::{
    NullChainDataPlane, OneShotSub, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dynamic_interest_changes_filter_mid_stream() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    // Pre-scan the block for the highest-frequency policy to
    // assert against. Same self-tuning approach `dispatch_v2`
    // uses — the test stays robust across fixture changes.
    let decoded =
        mitos_data_plane::block_events::decode_block_v2(&cbor).expect("decode block fixture");
    let mut counts: HashMap<String, usize> = HashMap::new();
    for tx in &decoded.txs {
        for out in &tx.outputs {
            for asset in &out.output.assets {
                *counts.entry(asset.policy_id.to_string()).or_insert(0) += 1;
            }
        }
    }
    let (watched_policy, _) = counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .expect("block fixture has at least one asset");

    // Manifest declares NO interest at start — the only way the
    // module sees events at all is via the dynamic-interest path.
    // This isolates the test from manifest-driven bootstrap.
    let wasm = std::fs::read(&wasm_path).expect("read wasm");
    let manifest = manifest_v2(&wasm);
    assert!(
        manifest.interest.is_empty(),
        "manifest_v2 helper should produce empty interest by default",
    );

    let storage_dir = tempdir("dynamic-interest-v2");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");

    // Fake companion subscription so the drain task records
    // rows for our assertion. Same pattern dispatch_v2 uses.
    let companions_dir = storage.module_dir_for_companions("test-indexer");
    std::fs::create_dir_all(&companions_dir).expect("companions dir");
    std::fs::write(companions_dir.join("test-companion.cbor"), b"").expect("companion stub");

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let chain_plane = Arc::new(NullChainDataPlane);
    let dp: Arc<dyn DataPlaneFacade> = chain_plane.clone();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let rx_holder = Arc::new(Mutex::new(rx));
    let sub_factory: SubscriptionFactory<OneShotSub> = Arc::new({
        let rx_holder = rx_holder.clone();
        move |_resume_cursor: Option<ChainPoint>| OneShotSub {
            rx: rx_holder.clone(),
        }
    });
    let kv_factory: KvFactory = Arc::new(|_id: &str| state_kv::ModuleKv::new_in_memory());
    let emitter_factory: EmitterFactory = Arc::new(emit::EventSink::new);

    let host = ModuleHostV2::new(
        storage.clone(),
        engine,
        dp,
        chain_plane,
        sub_factory,
        kv_factory,
        emitter_factory,
        ResourceBudget::default(),
    );

    host.start("test-indexer", false).await.expect("start");

    // ---- Block #1: empty interest. Should emit nothing.

    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor.clone()),
    ))
    .expect("send first block");

    // Settle.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let emissions = storage
        .emissions_store("test-indexer")
        .expect("emissions store");
    let baseline_rows = emissions
        .list_queued_for_companion("test-companion", "test-client")
        .expect("list emissions");
    assert!(
        baseline_rows.is_empty(),
        "block dispatched against empty interest set should produce 0 emissions, got {}",
        baseline_rows.len(),
    );

    // ---- Inject a dynamic-interest update for the watched
    // policy. The host's InterestRouter translates v1 →
    // v2 InterestPredicate and pushes through the
    // per-module follower channel.

    let policy_id = PolicyId::new(watched_policy.clone()).expect("valid policy id");
    let mut wire_interest = WireInterest::any();
    wire_interest.asset = AssetSelector::Policy(policy_id);
    host.route_interest("test-indexer", WireInterestOp::Add, vec![wire_interest])
        .await
        .expect("route_interest");

    // Give the follower a tick to drain the interest channel
    // and apply the update before we send the next block.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // ---- Block #2: same fixture, but interest now matches
    // the watched policy. Module should emit one event per
    // asset on outputs that hold the policy.

    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_001).into(),
        Arc::new(cbor.clone()),
    ))
    .expect("send second block");

    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    host.stop("test-indexer").await.expect("stop");

    let post_update_rows = emissions
        .list_queued_for_companion("test-companion", "test-client")
        .expect("list emissions");
    assert!(
        !post_update_rows.is_empty(),
        "block re-dispatched after route_interest(Add, watched_policy) should produce > 0 emissions; got 0 — \
         dynamic-interest path is broken",
    );

    drop(tx);
    std::fs::remove_dir_all(&storage_dir).ok();
}
