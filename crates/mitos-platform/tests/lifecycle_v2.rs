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
use mitos_platform::host_v2::{
    EmitterFactory, FallbackSource, KvFactory, ModuleHostV2, SubscriptionFactory,
};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use tokio::sync::Mutex;

use common::{
    NullChainDataPlane, OneShotSub, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm,
    wait_for,
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
    )
    .with_fallback_source(FallbackSource::Disabled);

    // 1. Start the module.
    host.start("test-indexer", false).await.expect("start");
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

    // 3. Wait for the follower to drain the queue and flush the
    //    cursor. Polled rather than slept: the old fixed 500ms
    //    encoded a guess, and when the guess was wrong the failure
    //    read as "cursor absent" (a logic bug) rather than "not yet"
    //    (a timing one).
    //
    // 4. Cursor was checkpointed.
    let persisted = wait_for("follower to flush the cursor", || {
        storage.read_cursor("test-indexer").ok().flatten()
    })
    .await;
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
    )
    .with_fallback_source(FallbackSource::Disabled);
    host_2
        .start("test-indexer", false)
        .await
        .expect("cold restart");
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

/// A module already activated under an older host must not keep
/// running once the host's WIT moves underneath it.
///
/// Auto-load can refuse to *activate* a skewed artifact, but by the
/// time the skew exists the module is already in storage — refusing
/// activation there leaves it running. `start` is the chokepoint that
/// actually stops it, and it covers auto-resume, admin restart and
/// recapture alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_refuses_a_module_built_against_a_different_wit() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let wasm = std::fs::read(&wasm_path).expect("read wasm");

    // Activate directly, as an older host would have: storage
    // performs no validation, so this is exactly the state a
    // previously-legitimate module is left in after a WIT change.
    let mut manifest = manifest_v2(&wasm);
    manifest.abi.wit_sha = Some("11".repeat(32));

    let storage_dir = tempdir("lifecycle-v2-wit-skew");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let chain_plane = Arc::new(NullChainDataPlane);
    let dp: Arc<dyn DataPlaneFacade> = chain_plane.clone();
    let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
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
    )
    .with_fallback_source(FallbackSource::Disabled);

    let err = host
        .start("test-indexer", false)
        .await
        .expect_err("start must refuse a WIT-skewed module");
    let msg = err.to_string();
    assert!(
        msg.contains("wit revision mismatch"),
        "refusal should name the WIT revision skew, got: {msg}"
    );
    assert!(
        host.list().await.is_empty(),
        "a refused module must not be running"
    );

    // And auto-resume — the path a process restart actually takes —
    // must leave it stopped rather than starting it anyway.
    host.auto_resume().await;
    assert!(
        host.list().await.is_empty(),
        "auto-resume must not start a refused module"
    );

    std::fs::remove_dir_all(&storage_dir).ok();
}
