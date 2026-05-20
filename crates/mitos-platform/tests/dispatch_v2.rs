//! v2 dispatch test — feed a real block CBOR through the host
//! follower and assert per-asset emissions land in the per-
//! companion emissions store with the expected CBOR shape.
//!
//! The test self-tunes: it scans the block fixture for the
//! most-frequent policy, declares that policy as the manifest's
//! `[interest]`, and asserts the test-indexer emits exactly one
//! event per asset under that policy across the block.
//!
//! Coverage:
//! - `decode_block_v2` round-trips the fixture's CBOR
//! - The dispatch composer's policy filter actually narrows
//!   the per-tx batches before delivery
//! - The wasm module receives `Produced` events at the WIT
//!   boundary and emits CBOR through `emit-event`
//! - The drain task fans emissions to a registered companion's
//!   row in the redb-backed `EmissionsStore`
//! - The CBOR shape the module produces decodes cleanly into
//!   the assertion struct
//!
//! Skips cleanly when the test fixture wasm or block CBOR
//! aren't available.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::TipEvent;
use mitos_data_plane::ChainPoint;
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{EmitterFactory, KvFactory, ModuleHostV2, SubscriptionFactory};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use tokio::sync::Mutex;

use common::{
    NullChainDataPlane, OneShotSub, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm,
};

/// Shape the test-indexer emits per asset. Mirror of the
/// `TestEvent` struct in `modules/test_indexer.rs` — keep in
/// sync if either side changes. `out` is unused here because
/// the test asserts on policy_id + tx_hash, but it's part of
/// the wire shape so it stays declared.
#[derive(Debug, serde::Deserialize)]
struct EmittedShape {
    policy_id: String,
    tx_hash: String,
    #[allow(dead_code)]
    out: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dispatch_emits_per_asset_under_watched_policy() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    // Pre-scan the block for the highest-frequency policy.
    // Picking the top one keeps the test self-tuning if the
    // fixture changes.
    //
    // The expected emission count is *not* the asset count under
    // that policy. The host's interest filter is loose: when an
    // output holds at least one asset under a watched policy,
    // the entire output reaches the module — including any
    // co-resident assets under unwatched policies. The module
    // (`test_indexer`) emits one event per asset on the
    // delivered output, so the expected count is the sum of
    // *all* assets on outputs that have at least one asset
    // under the watched policy.
    let decoded =
        mitos_data_plane::block_events::decode_block_v2(&cbor).expect("decode block fixture");
    let mut policy_outputs: HashMap<String, usize> = HashMap::new();
    for tx in &decoded.txs {
        for out in &tx.outputs {
            for asset in &out.output.assets {
                *policy_outputs
                    .entry(asset.policy_id.to_string())
                    .or_insert(0) += 1;
            }
        }
    }
    let (watched_policy, _) = policy_outputs
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .expect("block fixture has at least one asset");
    let mut expected_count = 0usize;
    for tx in &decoded.txs {
        for out in &tx.outputs {
            let has_watched = out
                .output
                .assets
                .iter()
                .any(|a| a.policy_id.to_string() == watched_policy);
            if has_watched {
                expected_count += out.output.assets.len();
            }
        }
    }

    let wasm = std::fs::read(&wasm_path).expect("read wasm");
    let mut manifest = manifest_v2(&wasm);
    manifest.interest.policies = vec![watched_policy.clone()];

    let storage_dir = tempdir("dispatch-v2");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");

    // Register a synthetic companion so the host's drain task
    // appends emission rows to the redb store (drain skips when
    // the companions dir is empty — see `host_v2::drain_one`).
    // The drain only reads file stems for the companion key;
    // contents are irrelevant for this test.
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

    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor),
    ))
    .expect("send tip event");

    // Give the follower + drain tasks time to dispatch the block,
    // run the module, and flush emission rows to the redb store.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    host.stop("test-indexer").await.expect("stop");

    // Inspect the emissions store directly.
    let emissions = storage
        .emissions_store("test-indexer")
        .expect("emissions store");
    let rows = emissions
        .list_queued_for_companion("test-companion", "test-client")
        .expect("list emissions");

    assert_eq!(
        rows.len(),
        expected_count,
        "module should emit one event per asset under {watched_policy} \
         (block-fixture asset-under-policy count = {expected_count}, got {})",
        rows.len(),
    );

    // Spot-check: every emission decodes cleanly into the
    // expected shape, and at least one is for the watched
    // policy. (Co-resident assets on the same output are
    // expected to appear too — see the comment on
    // `expected_count` for why.)
    let mut watched_seen = 0usize;
    for record in &rows {
        let shape: EmittedShape =
            ciborium::de::from_reader(record.payload.as_slice()).expect("decode emitted CBOR");
        if shape.policy_id == watched_policy {
            watched_seen += 1;
        }
        assert_eq!(
            shape.tx_hash.len(),
            64,
            "tx_hash should be 64-char hex (32 bytes hex-encoded)",
        );
    }
    assert!(
        watched_seen > 0,
        "no emissions matched the watched policy {watched_policy} — \
         interest filter may be broken",
    );

    drop(tx);
    std::fs::remove_dir_all(&storage_dir).ok();
}
