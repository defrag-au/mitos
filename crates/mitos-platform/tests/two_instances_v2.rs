//! Two instances of one module, side by side, on a real `ModuleHostV2`.
//!
//! Everything else in this directory drives a module through the `&str`
//! facade, which can only ever mean ONE instance. These tests use the
//! instance-addressed API underneath it (`start_instance`, `stop_instance`,
//! `running_instances_of`) to assert the properties that only exist once a
//! module can be forked:
//!
//! 1. Two followers advance two cursors and write two KVs.
//! 2. Stopping one leaves the other running.
//! 3. `replace` — the `&str` path — stops the instance the facade STARTED,
//!    not the one `current.toml` now names, and leaves exactly one slot.
//! 4. A dynamic-interest update reaches every running instance, observed
//!    through two clients placed on different instances.
//! 5. `recapture` and `evict` — whole-module operations with no defined
//!    meaning across two instances — refuse rather than guess.
//!
//! # The second instance
//!
//! There is one test wasm, and `put_artifact` of the same bytes is a no-op,
//! so a second instance is made the other way its key allows: same artifact,
//! an explicit CONFIG. The config bytes are empty, which the test-indexer's
//! `init` treats exactly as no config at all — so the second instance differs
//! from the first in identity and in nothing else, which is what these tests
//! are about. Creating instances properly is WO-08's; `put_config` +
//! `write_instance_marker` is what is public today.
//!
//! Skips cleanly when the test fixture wasm isn't built.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use cardano_assets::PolicyId;
use dolos_core::{TipEvent, TipSubscription};
use mitos_data_plane::ChainPoint;
use mitos_lifecycle::client_move::{ClientRow, Placement};
use mitos_lifecycle::ids::{ClientId, ClientKey, InstanceKey, ModuleId, Namespace, Revision};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{
    EmitterFactory, FallbackSource, InterestRouter, KvFactory, ModuleHostV2, SubscriptionFactory,
};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::{AssetSelector, Interest, InterestOp};

use common::{
    NullChainDataPlane, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm, wait_for,
};

const MODULE: &str = "test-indexer";

// ---------------------------------------------------------------------------
// A subscription every follower gets its own copy of
// ---------------------------------------------------------------------------

/// `common::OneShotSub` shares ONE receiver between every subscription it
/// hands out, so a `TipEvent` sent while two followers are running reaches
/// whichever of them wakes first. That is invisible with one instance and
/// fatal here: "both instances advanced their cursor" would depend on the
/// scheduler.
///
/// A broadcast channel gives each follower its own receiver and every
/// follower every event, which is what a real chain subscription does.
struct BroadcastSub {
    rx: tokio::sync::broadcast::Receiver<TipEvent>,
}

impl TipSubscription for BroadcastSub {
    async fn next_tip(&mut self) -> TipEvent {
        loop {
            match self.rx.recv().await {
                Ok(event) => return event,
                // A slow follower that missed frames is not something these
                // tests produce (the channel is far larger than what they
                // send), but dropping to `pending` on it would look like a
                // hang. Keep reading.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                // Closed — block forever rather than return a sentinel; the
                // follower runs until the host's cancellation token fires.
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    std::future::pending().await
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

struct Harness {
    dir: std::path::PathBuf,
    storage: ModuleStorage,
    host: ModuleHostV2<BroadcastSub, NullChainDataPlane>,
    tx: tokio::sync::broadcast::Sender<TipEvent>,
    /// What `activate` produced — the module's current instance.
    first: InstanceKey,
    /// Same artifact, an explicit (empty) config. A distinct instance
    /// directory, and nothing else distinct about it.
    second: InstanceKey,
}

impl Harness {
    /// `None` when the test-indexer wasm isn't built — callers skip.
    fn build(
        name: &str,
        manifest_tweak: impl FnOnce(&mut mitos_platform::manifest::Manifest),
    ) -> Option<Self> {
        let wasm_path = test_indexer_wasm()?;
        let wasm = std::fs::read(&wasm_path).expect("read wasm");
        let mut manifest = manifest_v2(&wasm);
        manifest_tweak(&mut manifest);

        let dir = tempdir(name);
        let storage = ModuleStorage::new(&dir);
        storage.activate(&manifest, &wasm).expect("activate");
        let first = storage
            .current_instance(MODULE)
            .expect("read current")
            .expect("activated module has a current instance");

        let config = storage.put_config(b"").expect("put config");
        let second = InstanceKey {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: first.wasm.clone(),
            config: Some(config),
        };
        storage
            .write_instance_marker(&second)
            .expect("second instance marker");
        assert_ne!(first, second);

        let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
        let chain_plane = Arc::new(NullChainDataPlane);
        let dp: Arc<dyn DataPlaneFacade> = chain_plane.clone();

        let (tx, _rx) = tokio::sync::broadcast::channel(256);
        let sub_factory: SubscriptionFactory<BroadcastSub> = Arc::new({
            let tx = tx.clone();
            move |_resume: Option<ChainPoint>| BroadcastSub { rx: tx.subscribe() }
        });
        // A REAL, storage-backed KV factory — the in-memory one every other
        // test uses would make "each instance has its own KV" vacuously true.
        let kv_factory: KvFactory = Arc::new({
            let storage = storage.clone();
            move |key: &InstanceKey| match storage.kv_store_at(key, None) {
                Ok(kv) => state_kv::ModuleKv::Redb(kv),
                Err(e) => {
                    eprintln!("kv_store_at({}) failed: {e}", key.dir_name());
                    state_kv::ModuleKv::new_in_memory()
                }
            }
        });
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

        Some(Harness {
            dir,
            storage,
            host,
            tx,
            first,
            second,
        })
    }

    fn cleanup(self) {
        std::fs::remove_dir_all(&self.dir).ok();
    }
}

/// The policy that appears on the most outputs in the block fixture — the
/// same self-tuning pick `dispatch_v2` and `dynamic_interest_v2` make, so
/// these tests stay honest if the fixture changes.
fn busiest_policy(cbor: &[u8]) -> String {
    let decoded =
        mitos_data_plane::block_events::decode_block_v2(cbor).expect("decode block fixture");
    let mut counts: HashMap<String, usize> = HashMap::new();
    for tx in &decoded.txs {
        for out in &tx.outputs {
            for asset in &out.output.assets {
                *counts.entry(asset.policy_id.to_string()).or_insert(0) += 1;
            }
        }
    }
    counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .expect("block fixture has at least one asset")
        .0
}

/// Place `client` on `instance` by writing its row — what a committed move
/// leaves behind, and the only thing the fan-out reads.
fn place(storage: &ModuleStorage, client: &str, instance: &InstanceKey) {
    let row = ClientRow::adopt(
        ClientKey {
            namespace: Namespace::parse("prod").unwrap(),
            client: ClientId::parse(client).unwrap(),
        },
        ModuleId::parse(MODULE).unwrap(),
        Placement {
            instance: instance.clone(),
            revision: Revision(0),
        },
    );
    storage.write_row(MODULE, &row).expect("write row");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Two followers, two cursors, two KVs. If the data plane were still
/// module-addressed, the two instances would share one `cursor.redb` and one
/// `state.redb` — and redb is single-writer, so the second open would fail
/// outright rather than quietly interleave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_instance_follows_beside_the_first() {
    let Some(h) = Harness::build("two-instances-follow", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    h.host
        .start_instance(&h.first, false)
        .await
        .expect("start first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("start second");

    let mut running = h.host.running_instances_of(MODULE).await;
    running.sort_by_key(|k| k.dir_name());
    let mut expected = vec![h.first.clone(), h.second.clone()];
    expected.sort_by_key(|k| k.dir_name());
    assert_eq!(running, expected, "both instances should have a live slot");

    h.tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor),
    ))
    .expect("send tip event");

    // Each instance flushes its OWN cursor. Waiting on both separately is the
    // assertion: a shared cursor would let one instance satisfy both.
    for key in [&h.first, &h.second] {
        let cursor = wait_for(&format!("cursor for {}", key.dir_name()), || {
            h.storage.read_cursor_at(key).ok().flatten()
        })
        .await;
        assert_eq!(cursor.slot(), 186_000_000);
    }

    // …and two distinct KV databases, which is what makes two followers over
    // the same blocks safe at all.
    assert_ne!(
        h.storage.instance_dir(&h.first),
        h.storage.instance_dir(&h.second),
    );
    for key in [&h.first, &h.second] {
        assert!(
            h.storage.instance_dir(key).join("kv.redb").exists(),
            "{} should have its own kv.redb",
            key.dir_name(),
        );
    }

    h.host.stop_all().await;
    h.cleanup();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopping_one_instance_leaves_the_other_running() {
    let Some(h) = Harness::build("two-instances-stop-one", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };

    h.host.start_instance(&h.first, false).await.expect("first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("second");
    assert_eq!(h.host.running_instances_of(MODULE).await.len(), 2);

    h.host.stop_instance(&h.first).await.expect("stop first");

    assert_eq!(
        h.host.running_instances_of(MODULE).await,
        vec![h.second.clone()],
        "stopping one instance must not touch the other",
    );
    // The module is still running as far as the `&str` view is concerned —
    // a module with any live instance is running.
    assert_eq!(h.host.list_running().await, vec![MODULE.to_owned()]);

    h.host.stop_all().await;
    h.cleanup();
}

/// The `facade_slots` case, and the reason that map exists.
///
/// `write_config` RENAMES the live instance directory to the new key — v2's
/// upload semantics, preserved. So after it, the instance the facade started
/// is not the instance `current.toml` names, and `replace` must stop the
/// former and start the latter. Stopping "the current instance" would leave
/// the renamed-away slot running forever, on a directory that no longer
/// exists under that name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replace_after_an_upload_stops_the_renamed_instance_and_starts_the_new_key() {
    let Some(h) = Harness::build("two-instances-replace", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };

    // Start through the facade, so `facade_slots` records `first`.
    h.host.start(MODULE, false).await.expect("start");
    assert_eq!(
        h.host.running_instances_of(MODULE).await,
        vec![h.first.clone()]
    );

    // The upload: `first` is RENAMED to a key carrying a config. Different
    // bytes from the harness's second instance on purpose — repointing onto
    // a key whose directory already exists is a collision, not an upload.
    // (`a0` is a CBOR empty map; the test-indexer's `init` reads it as no
    // configuration, so nothing about the running module changes — only its
    // identity, which is the whole point.)
    h.storage
        .write_config(MODULE, b"\xa0")
        .expect("write config (the rename)");
    let after = h
        .storage
        .current_instance(MODULE)
        .unwrap()
        .expect("still activated");
    assert_ne!(after, h.first, "write_config should have repointed");

    // The running follower's handles were re-keyed to `after` by the rename.
    // A `Weak`, so holding it cannot itself keep the old handle alive.
    let old_handle = std::sync::Arc::downgrade(&h.storage.watched_index_at(&after));

    h.host.replace(MODULE).await.expect("replace");

    // v2 re-opened redb on every replace. Stopping `first` closes `first`'s
    // cache entries — of which there are none, they moved — so unless the
    // start of `after` closes them too, every deploy hands the new follower
    // the old follower's handles.
    assert!(
        old_handle.upgrade().is_none(),
        "the replaced follower's handles outlived it: the new follower is reusing them",
    );

    assert_eq!(
        h.host.running_instances_of(MODULE).await,
        vec![after.clone()],
        "replace must leave exactly one slot, on the key current.toml names",
    );

    h.host.stop_all().await;
    h.cleanup();
}

/// One `route_interest` call, two instances, two clients — each placed on a
/// different instance. If the update reached only one follower, the other
/// instance's client gets nothing and the test fails on THAT client rather
/// than on a channel count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interest_update_reaches_every_running_instance() {
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };
    let watched = busiest_policy(&cbor);
    // No manifest interest: the ONLY way either instance sees an event is
    // the dynamic-interest path, which is what this test is about.
    let Some(h) = Harness::build("two-instances-interest", |m| {
        assert!(
            m.interest.is_empty(),
            "fixture manifest declares no interest"
        );
    }) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };

    common::write_companion_subscription(
        &h.storage,
        MODULE,
        "client-a",
        "comp",
        vec![Interest::any()],
    );
    common::write_companion_subscription(
        &h.storage,
        MODULE,
        "client-b",
        "comp",
        vec![Interest::any()],
    );
    // `client-a` has no row, so it follows the module's current instance
    // (`first`). `client-b` is placed on `second`.
    place(&h.storage, "client-b", &h.second);

    h.host.start_instance(&h.first, false).await.expect("first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("second");

    let mut interest = Interest::any();
    interest.asset = AssetSelector::Policy(PolicyId::new(watched.clone()).expect("valid policy"));
    h.host
        .route_interest(MODULE, InterestOp::Add, vec![interest])
        .await
        .expect("route_interest");

    // Applied interest isn't observable from out here — same constraint
    // `dynamic_interest_v2` documents. The block is sent after a short
    // settle; if this ever flakes, the fix is to expose the driver's applied
    // set, not to raise the number.
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    h.tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor),
    ))
    .expect("send tip event");

    // Each client's rows are in the store of the instance that serves it —
    // and in no other.
    let rows_for = |key: &InstanceKey, client: &str| {
        let store = h.storage.emissions_store_at(key).ok()?;
        let rows = store.list_queued_for_companion("comp", client).ok()?;
        (!rows.is_empty()).then_some(rows.len())
    };
    let a = wait_for("client-a's rows on the first instance", || {
        rows_for(&h.first, "client-a")
    })
    .await;
    let b = wait_for("client-b's rows on the second instance", || {
        rows_for(&h.second, "client-b")
    })
    .await;
    assert!(a > 0 && b > 0);

    // And nothing crossed over: an instance that does not serve a client
    // must never write its rows, or the companion receives every event twice.
    assert_eq!(rows_for(&h.first, "client-b"), None);
    assert_eq!(rows_for(&h.second, "client-a"), None);

    h.host.stop_all().await;
    h.cleanup();
}

/// Both operations were written when a module WAS an instance: recapture
/// wipes the bootstrap flags and restarts, evict removes the module
/// directory. Across two instances each would act on one and mean the other,
/// so both refuse. Making them instance-aware is the control plane's job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recapture_and_evict_refuse_a_module_with_two_instances() {
    let Some(h) = Harness::build("two-instances-refuse", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };

    h.host.start_instance(&h.first, false).await.expect("first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("second");

    let recapture = h
        .host
        .recapture_module(MODULE, None, std::time::Duration::from_secs(1))
        .await
        .expect_err("recapture must refuse a forked module");
    assert!(
        matches!(
            recapture,
            mitos_platform::PlatformError::MultipleInstances { found: 2, .. }
        ),
        "expected MultipleInstances, got: {recapture}",
    );

    let evict = h
        .host
        .evict_module(MODULE)
        .await
        .expect_err("evict must refuse a forked module");
    assert!(
        matches!(
            evict,
            mitos_platform::PlatformError::MultipleInstances { found: 2, .. }
        ),
        "expected MultipleInstances, got: {evict}",
    );

    // A refusal that stopped something would be worse than the guess it
    // avoided.
    assert_eq!(h.host.running_instances_of(MODULE).await.len(), 2);
    assert!(h.storage.instance_dir(&h.first).is_dir());
    assert!(h.storage.instance_dir(&h.second).is_dir());

    h.host.stop_all().await;
    h.cleanup();
}
