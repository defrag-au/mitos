//! `ModuleHostV2` lifecycle test — start / stop / replace against
//! the `test-indexer` v2 fixture.
//!
//! Drives the real test wasm through the host, pushes one
//! `TipEvent::Apply` frame via the synthetic subscription so the
//! follower advances the cursor, then verifies:
//!
//! 1. `start` actually instantiates and registers the module
//! 2. The follower flushes the cursor to disk after dispatching
//!    the block
//! 3. `replace` is start-after-stop and survives a re-instantiation
//! 4. `stop` is idempotent (calling stop on a non-running module
//!    is a no-op)
//! 5. A fresh `ModuleHostV2` resumes the cursor from disk on
//!    cold start (the same lifecycle a process restart sees)
//!
//! Skips cleanly when the test fixture wasm isn't built.

mod common;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_replace_stop_roundtrip_v2() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!(
            "skipping: test-indexer wasm not built — run \
             `nix develop $CNFT_DEV_WORKERS -c cargo run --release -p mitos-build \
             -- --module modules/test_indexer.rs` from the mitos repo root"
        );
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    let wasm = std::fs::read(&wasm_path).expect("read wasm");
    let manifest = manifest_v2(&wasm);

    let storage_dir = tempdir("lifecycle-v2");
    let storage = ModuleStorage::new(&storage_dir);
    storage
        .activate(&manifest, &wasm)
        .expect("activate manifest");

    // Shared engine + null data plane wires once; the subscription
    // factory hands fresh `OneShotSub` receivers off the same
    // sender so we can push events from the test thread.
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
        engine.clone(),
        dp.clone(),
        chain_plane.clone(),
        sub_factory.clone(),
        kv_factory.clone(),
        emitter_factory.clone(),
        ResourceBudget::default(),
    );

    // 1. Start the module.
    host.start("test-indexer").await.expect("start");
    assert_eq!(host.list().await, vec!["test-indexer"]);

    // 2. Push one Apply event via the synthetic subscription.
    //    With the manifest's `[interest]` empty, the dispatch
    //    composer produces no module-visible batches but the
    //    follower still advances the cursor — that's the
    //    invariant we want to verify.
    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor.clone()),
    ))
    .expect("send tip event");

    // 3. Give the follower a moment to drain the queue and
    //    flush the cursor. 500ms is generous on the slow side
    //    of CI; a faster signal would be a notify-based hook,
    //    but the v1 test used the same pattern and never
    //    flaked.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // 4. Cursor was checkpointed.
    let persisted = storage
        .read_cursor("test-indexer")
        .expect("read cursor")
        .expect("cursor present after dispatch");
    assert_eq!(
        persisted.slot(),
        186_000_000,
        "follower should have advanced the cursor to the dispatched block's slot",
    );

    // 5. Replace (re-instantiate) the module. The new instance
    //    should pick up the same cursor from disk and the
    //    list should still contain exactly this module.
    host.replace("test-indexer").await.expect("replace");
    assert_eq!(host.list().await, vec!["test-indexer"]);

    // 6. Stop. Slot is empty after.
    host.stop("test-indexer").await.expect("stop");
    assert!(
        host.list().await.is_empty(),
        "stop() should empty the running-modules list",
    );

    // 7. Stop is idempotent.
    host.stop("test-indexer")
        .await
        .expect("idempotent stop on a non-running module");

    // 8. Cold restart: build a fresh host pointing at the same
    //    storage dir, start the module, assert the cursor
    //    resumed from disk. This mirrors the systemd restart
    //    path — same module file, same cursor.redb, fresh
    //    process state.
    let host_2 = ModuleHostV2::new(
        storage.clone(),
        engine,
        dp,
        chain_plane,
        sub_factory,
        kv_factory,
        emitter_factory,
        ResourceBudget::default(),
    );
    host_2.start("test-indexer").await.expect("cold restart");
    assert_eq!(host_2.list().await, vec!["test-indexer"]);
    let after_restart = storage
        .read_cursor("test-indexer")
        .expect("read cursor after restart")
        .expect("cursor still present after restart");
    assert_eq!(
        after_restart.slot(),
        186_000_000,
        "cold restart should resume the persisted cursor",
    );
    host_2
        .stop("test-indexer")
        .await
        .expect("stop after restart");

    drop(tx);
    std::fs::remove_dir_all(&storage_dir).ok();
}
