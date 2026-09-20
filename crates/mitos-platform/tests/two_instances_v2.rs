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

/// Wait until `n` followers have subscribed to the synthetic tip channel.
///
/// `broadcast::send` reaches the receivers that exist at that instant, and
/// `start_instance` returns once the follower task is SPAWNED — it subscribes
/// a moment later. A send in that gap is silently dropped, which reads as "a
/// follower did not advance" and is nothing of the kind.
async fn subscribed(tx: &tokio::sync::broadcast::Sender<TipEvent>, n: usize) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while tx.receiver_count() < n {
        assert!(
            std::time::Instant::now() < deadline,
            "only {} of {n} followers subscribed",
            tx.receiver_count(),
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
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

/// `evict_instance` on the REAL host: it stops the slot, releases the
/// dialer's hold, closes the handles and removes the directory — and stops
/// there, leaving the module and its sibling alone.
///
/// `FakeHost` does the storage half of this for the lifecycle's unit tests,
/// so without a test here the real teardown would have nothing but the fake
/// vouching for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evicting_one_instance_removes_its_directory_and_leaves_the_module() {
    let Some(h) = Harness::build("two-instances-evict", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };

    h.host.start_instance(&h.first, false).await.expect("first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("second");

    // The module's CURRENT instance cannot be reaped while a sibling exists:
    // `current.toml` would name nothing.
    let refused = h
        .host
        .evict_instance(&h.first)
        .await
        .expect_err("reaping the current instance must be refused");
    assert!(
        matches!(
            refused,
            mitos_platform::PlatformError::CurrentInstanceHasSiblings { .. }
        ),
        "expected CurrentInstanceHasSiblings, got: {refused}",
    );
    assert!(h.storage.instance_dir(&h.first).is_dir());
    assert_eq!(h.host.running_instances_of(MODULE).await.len(), 2);

    // The sibling can.
    h.host
        .evict_instance(&h.second)
        .await
        .expect("evicting a non-current instance");

    assert!(
        !h.storage.instance_dir(&h.second).is_dir(),
        "the reaped instance's directory survived",
    );
    assert!(h.storage.instance_dir(&h.first).is_dir());
    assert_eq!(
        h.storage.current_instance(MODULE).unwrap(),
        Some(h.first.clone()),
        "reaping a sibling moved the module's pointer",
    );
    assert_eq!(
        h.host.running_instances_of(MODULE).await,
        vec![h.first.clone()]
    );

    // …and now that it is the only one, reaping it takes the module, exactly
    // as `evict_module` always did.
    h.host
        .evict_instance(&h.first)
        .await
        .expect("evicting the last instance");
    assert!(h.storage.current_instance(MODULE).unwrap().is_none());
    assert!(!h.storage.root().join(MODULE).exists());

    h.host.stop_all().await;
    h.cleanup();
}

/// A staging instance built the way a client move builds one: a REAL copy of
/// a REAL running parent's state, taken with that parent stopped, on a real
/// `ModuleHostV2`.
///
/// The `FakeHost` tests say the right files are carried. Only this one says
/// the copy is something a follower can actually resume from — that the
/// parent's redb handles were let go before the bytes were taken, and that
/// both followers advance afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_staging_instance_copied_from_a_running_parent_resumes_from_its_cursor() {
    use mitos_platform::auth::Authenticator;
    use mitos_platform::events::EventRing;
    use mitos_platform::host_v2::ModuleHostHandle;
    use mitos_platform::lifecycle::{LifecycleManager, system_clock};

    let Some(h) = Harness::build("two-instances-staging", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };
    let Harness {
        dir,
        storage,
        host,
        tx,
        first,
        second: _unused,
    } = h;
    let host = Arc::new(host);
    let handle: Arc<dyn ModuleHostHandle> = host.clone();
    let manager = Arc::new(LifecycleManager::new(
        storage.clone(),
        handle,
        Arc::new(Authenticator::legacy(Some("t".repeat(48)), None)),
        EventRing::new(),
        system_clock(),
    ));

    // The parent, running and caught up to a block.
    manager.boot().await;
    assert_eq!(host.running_instances_of(MODULE).await, vec![first.clone()]);
    // A `broadcast` reaches the receivers that exist AT SEND TIME, and
    // `start_instance` returns once the follower task is spawned — the task
    // subscribes a moment later. Waiting for the subscription is the test's
    // own business; nothing about the host depends on it.
    subscribed(&tx, 1).await;
    tx.send(TipEvent::Apply(
        ChainPoint::Slot(186_000_000).into(),
        Arc::new(cbor.clone()),
    ))
    .expect("send the first block");
    let parent_cursor = wait_for("the parent to reach the block", || {
        storage.read_cursor_at(&first).ok().flatten()
    })
    .await;
    assert_eq!(parent_cursor.slot(), 186_000_000);

    // A target the harness did NOT plant — `create_staging` refuses a
    // directory it did not make, and building one is the whole point here.
    let target = InstanceKey {
        module: ModuleId::parse(MODULE).unwrap(),
        wasm: first.wasm.clone(),
        config: Some(storage.put_config(b"\xa0").expect("put config")),
    };
    assert!(!storage.instance_dir(&target).is_dir());

    manager
        .create_staging(&target, Some(&first))
        .await
        .expect("the staging instance is built and started");

    assert_eq!(
        storage
            .read_cursor_at(&target)
            .unwrap()
            .expect("the staging instance has a cursor")
            .slot(),
        186_000_000,
        "the copy did not carry the parent's cursor",
    );
    assert!(
        storage.instance_dir(&target).join("kv.redb").is_file(),
        "the copy did not carry the parent's KV",
    );

    // BOTH follow from here. A copy taken with the parent's redb handles
    // still open is the kind that opens and then fails on the first write —
    // and a parent whose handles were not given back cannot restart at all.
    let mut running = host.running_instances_of(MODULE).await;
    running.sort_by_key(|key| key.dir_name());
    let mut expected = vec![first.clone(), target.clone()];
    expected.sort_by_key(|key| key.dir_name());
    assert_eq!(running, expected, "one of the two followers is not running");

    // One more chain event, and both write their OWN cursor from it.
    //
    // A rollback rather than another `Apply`: the driver takes its cursor
    // from the BLOCK it decoded, and there is one block fixture — so
    // re-applying it could not move either cursor and would prove nothing.
    // `Undo` sets the cursor to the point it carries, which each follower
    // must then flush to its own file.
    subscribed(&tx, 2).await;
    tx.send(TipEvent::Undo(
        ChainPoint::Slot(185_999_000).into(),
        Arc::new(cbor),
    ))
    .expect("send the rollback");
    for key in [&first, &target] {
        let cursor = wait_for(&format!("{} to move its cursor", key.dir_name()), || {
            storage
                .read_cursor_at(key)
                .ok()
                .flatten()
                .filter(|c| c.slot() == 185_999_000)
        })
        .await;
        assert_eq!(cursor.slot(), 185_999_000);
    }
    assert_eq!(
        host.running_instances_of(MODULE).await.len(),
        2,
        "a follower died dispatching after the copy",
    );

    host.stop_all().await;
    std::fs::remove_dir_all(&dir).ok();
}

/// Boot and the watchdog, per INSTANCE, through the real manager over a real
/// host — `host.start_instance` is never called by this test, only by the
/// lifecycle acting on the records.
///
/// The watchdog half is the one that could not be asserted before: with two
/// instances, `list_running` says the module is up while one of its followers
/// is dead, so a module-addressed watchdog leaves it dead for ever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_instance_is_started_by_boot_and_revived_by_the_watchdog() {
    use mitos_lifecycle::instance::Instance;
    use mitos_platform::admin::AuthToken;
    use mitos_platform::auth::Authenticator;
    use mitos_platform::dialer::CompanionDialer;
    use mitos_platform::events::EventRing;
    use mitos_platform::host_v2::ModuleHostHandle;
    use mitos_platform::lifecycle::{LifecycleManager, system_clock};
    use std::collections::BTreeMap;
    use std::time::Duration;

    let Some(h) = Harness::build("two-instances-lifecycle", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    // Destructured rather than reshaped: the manager needs the host behind
    // an `Arc<dyn ModuleHostHandle>`, and every other test in this file holds
    // it by value.
    let Harness {
        dir,
        storage,
        host,
        tx: _tx,
        first,
        second,
    } = h;
    let host = Arc::new(host);
    let handle: Arc<dyn ModuleHostHandle> = host.clone();

    // The second instance's FIRST record, written with its directory — the
    // shape WO-08 will create. Without one it is `Unmanaged`, and the whole
    // point of that is that boot leaves it alone.
    storage
        .write_lifecycle_at(
            &second,
            &Instance::adopt(second.clone(), &BTreeMap::new()).record(),
        )
        .expect("write the second instance's record");

    let events = EventRing::new();
    let manager = Arc::new(LifecycleManager::new(
        storage.clone(),
        handle.clone(),
        Arc::new(Authenticator::legacy(Some("t".repeat(48)), None)),
        events.clone(),
        system_clock(),
    ));

    manager.boot().await;
    let mut running = host.running_instances_of(MODULE).await;
    running.sort_by_key(|key| key.dir_name());
    let mut expected = vec![first.clone(), second.clone()];
    expected.sort_by_key(|key| key.dir_name());
    assert_eq!(running, expected, "boot started only one of two instances");

    // The second follower dies. The MODULE is still running as far as every
    // `&str` caller is concerned, which is exactly the trap.
    host.stop_instance(&second).await.expect("stop the second");
    assert_eq!(host.list_running().await, vec![MODULE.to_owned()]);

    let cancel = tokio_util::sync::CancellationToken::new();
    let watchdog = mitos_platform::watchdog::spawn_with_config(
        storage.clone(),
        handle,
        manager.clone(),
        CompanionDialer::new(storage.clone(), AuthToken(None), None),
        events,
        cancel.clone(),
        Duration::from_millis(100),
        Duration::from_millis(100),
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if host.running_instances_of(MODULE).await.len() == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the watchdog left a dead second follower dead",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cancel.cancel();
    let _ = watchdog.await;
    host.stop_all().await;
    std::fs::remove_dir_all(&dir).ok();
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

/// **The property the whole switch exists for**, on the real host: across a
/// hand-off, every block reaches the moving client exactly once.
///
/// The instance taking a client over resumes from its OWN cursor, which is at
/// or below the one its predecessor reached — so without a watermark it
/// re-emits blocks the client has already had. With one, it withholds them,
/// and a ROLLBACK below the watermark lowers it again, because the blocks
/// that replace the abandoned fork are new blocks nobody has delivered.
///
/// The old follower stays RUNNING throughout, which is what makes "exactly
/// once" mean anything: if the row were not the switch, it would keep writing
/// the client's rows too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_block_reaches_a_moving_client_exactly_once() {
    use mitos_lifecycle::ids::Slot;

    /// Wait until a count stops changing.
    ///
    /// One block becomes several emissions, written one row at a time by the
    /// drain task, and there is no single "this block is fully fanned out"
    /// signal from out here — a cursor flush and a row write are independent.
    /// Taking the first non-zero reading would capture a partial count.
    async fn settled(what: &str, count: impl Fn() -> usize) -> usize {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            let before = count();
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let after = count();
            if after > 0 && before == after {
                return after;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{what}: the count never settled (last {after})",
            );
        }
    }

    const CLIENT: &str = "client-a";
    const SLOT: u64 = 186_000_000;
    /// Below the block — a rollback that voids it.
    const ROLLED_BACK_TO: u64 = 185_999_000;

    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };
    let watched = busiest_policy(&cbor);
    let Some(h) = Harness::build("two-instances-exactly-once", |_| {}) else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    common::write_companion_subscription(&h.storage, MODULE, CLIENT, "comp", vec![Interest::any()]);

    let forward = |key: &InstanceKey| {
        h.storage
            .emissions_store_at(key)
            .expect("emissions store")
            .list_filtered(|row| row.client_id == CLIENT && !row.is_undo)
            .expect("list")
            .len()
    };
    let undos = |key: &InstanceKey| {
        h.storage
            .emissions_store_at(key)
            .expect("emissions store")
            .list_filtered(|row| row.client_id == CLIENT && row.is_undo)
            .expect("list")
            .len()
    };
    let apply = |tx: &tokio::sync::broadcast::Sender<TipEvent>| {
        tx.send(TipEvent::Apply(
            ChainPoint::Slot(SLOT).into(),
            Arc::new(cbor.clone()),
        ))
        .expect("send the block");
    };

    // 1. THE watermark the commit writes — the live instance's cursor — put
    //    on `second` BEFORE its follower starts, because the drain task loads
    //    the watermarks once and owns them for as long as it runs. That is
    //    exactly the guarantee `quiesce_pair` gives the real commit: both
    //    followers are stopped across it.
    let mut handoffs = h.storage.read_handoffs_at(&h.second).expect("watermarks");
    handoffs.begin(ClientId::parse(CLIENT).unwrap(), Slot(SLOT));
    h.storage
        .write_handoffs_at(&h.second, &handoffs)
        .expect("write the watermark");

    // 2. Both followers, and the dynamic interest that is the only reason
    //    either of them emits anything — routed with both RUNNING, so the
    //    update reaches both.
    h.host.start_instance(&h.first, false).await.expect("first");
    h.host
        .start_instance(&h.second, false)
        .await
        .expect("second");
    let mut interest = Interest::any();
    interest.asset = AssetSelector::Policy(PolicyId::new(watched).expect("valid policy"));
    h.host
        .route_interest(MODULE, InterestOp::Add, vec![interest])
        .await
        .expect("route_interest");
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    subscribed(&h.tx, 2).await;

    // 3. Before the switch: `client-a` has no row, so it follows the module's
    //    current instance and `first` delivers the block. `second` serves
    //    nobody and writes nothing — it is following, and that is all.
    apply(&h.tx);
    let delivered = settled("the first instance delivering the block", || {
        forward(&h.first)
    })
    .await;
    wait_for("the second instance to process the block", || {
        h.storage
            .read_cursor_at(&h.second)
            .ok()
            .flatten()
            .filter(|c| c.slot() == SLOT)
    })
    .await;
    assert_eq!(
        forward(&h.second),
        0,
        "an instance nobody is placed on wrote a client's rows",
    );

    // 4. THE switch: the row write, which is the routing change.
    place(&h.storage, CLIENT, &h.second);

    // 5. The same block again — which is what the new instance replaying from
    //    its own (lower) cursor looks like, and what the watermark exists to
    //    withhold.
    apply(&h.tx);
    // No cursor to wait on: `second` is already at this slot. The rows are
    // what must not appear, so give them every chance to.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        forward(&h.second),
        0,
        "the client was sent a block its previous instance had already delivered",
    );
    assert_eq!(
        forward(&h.first),
        delivered,
        "the instance the client has LEFT is still writing its rows",
    );

    // 6. A rollback BELOW the watermark. It voids what `first` delivered on
    //    the abandoned fork, so the watermark drops — and the rollback itself
    //    is never withheld.
    h.tx.send(TipEvent::Undo(
        ChainPoint::Slot(ROLLED_BACK_TO).into(),
        Arc::new(cbor.clone()),
    ))
    .expect("send the rollback");
    wait_for("the second instance to deliver the rollback", || {
        (undos(&h.second) > 0).then_some(())
    })
    .await;
    assert_eq!(
        undos(&h.first),
        0,
        "the instance the client has left delivered the rollback too",
    );

    // 7. …and the replacement blocks are new blocks nobody has delivered, so
    //    the same block now arrives from the new instance.
    apply(&h.tx);
    let redelivered = settled("the second instance delivering the replacement", || {
        forward(&h.second)
    })
    .await;
    assert_eq!(
        redelivered, delivered,
        "the block that replaced the rolled-back one was partly withheld",
    );
    assert_eq!(
        forward(&h.first),
        delivered,
        "the old instance delivered twice"
    );

    h.host.stop_all().await;
    h.cleanup();
}
