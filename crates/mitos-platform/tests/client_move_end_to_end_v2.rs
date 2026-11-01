//! One real client move, end to end, on a real `ModuleHostV2`.
//!
//! The unit tests in `moves.rs` drive the reducer and the host facade; the
//! tests in `two_instances_v2.rs` drive two real followers over a synthetic
//! tip. Neither wires the two together. This test does: a live instance is
//! following, a client is subscribed, a worker announces a new build, and the
//! host must pull it, create the staging instance, catch it up, quiesce both
//! followers and commit the row — with no Dolos and no network.
//!
//! `MapFetcher` stands in for the worker's `/.well-known/mitos/...` surface;
//! it is the same `ArtifactFetcher` the production `HttpFetcher` implements,
//! so the puller's seven checks and the CAS write are the real ones.
//!
//! Skips cleanly when the test-indexer wasm isn't built (see
//! `tests/common::test_indexer_wasm`).

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dolos_core::{TipEvent, TipSubscription};
use mitos_data_plane::ChainPoint;
use mitos_lifecycle::artifact::ArtifactSource;
use mitos_lifecycle::ids::{ClientId, InstanceKey, ModuleId, Revision, Sha256Hex};
use mitos_platform::auth::Authenticator;
use mitos_platform::events::EventRing;
use mitos_platform::fetch::{ArtifactFetcher, ArtifactPuller, FetchError};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{
    EmitterFactory, FallbackSource, KvFactory, ModuleHostHandle, ModuleHostV2, SubscriptionFactory,
};
use mitos_platform::lifecycle::LifecycleManager;
use mitos_platform::moves::MoveManager;
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::{AnnounceResult, AnnouncedModule};

use common::{
    NullChainDataPlane, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm,
    write_companion_subscription,
};

const MODULE: &str = "test-indexer";
const CLIENT: &str = "ownership.cnft.dev";
const TIP_SLOT: u64 = 186_000_000;

/// A namespace registry with this test's client in it. Without one the host is
/// in legacy mode and refuses every announcement (`Registry::may_announce`),
/// which is a separate test in `moves.rs`; here the announcement must be
/// accepted.
const NAMESPACES: &str = r#"
operator_token_as_client = "allow-with-warning"

[[namespace]]
name = "prod"
token_env = "MITOS_TOKEN_PROD"
clients = ["ownership.cnft.dev"]

[namespace.profile]
tier = "production"
revision = "explicit-rollback-only"
retain_superseded_secs = 259200

[namespace.profile.eject]
kind = "alert-only"

[namespace.profile.resources]
fuel_per_call = 100000000
backfill_concurrency = 4
"#;

// ---------------------------------------------------------------------------
// The synthetic tip channel, one receiver per follower
// ---------------------------------------------------------------------------

/// `common::OneShotSub` shares one receiver between every subscription, so two
/// followers would race for each `TipEvent`. A broadcast gives every follower
/// its own receiver, which is what a real chain subscription does. Same shape
/// as `two_instances_v2::BroadcastSub`.
struct BroadcastSub {
    rx: tokio::sync::broadcast::Receiver<TipEvent>,
}

impl TipSubscription for BroadcastSub {
    async fn next_tip(&mut self) -> TipEvent {
        loop {
            match self.rx.recv().await {
                Ok(event) => return event,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    std::future::pending().await
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The worker's artifact host
// ---------------------------------------------------------------------------

/// Serves canned bytes and counts calls. The production `HttpFetcher` is the
/// same trait, so the puller cannot tell the difference.
#[derive(Default)]
struct MapFetcher {
    responses: Mutex<HashMap<String, Vec<u8>>>,
    calls: AtomicUsize,
}

impl MapFetcher {
    fn serve(&self, url: &str, bytes: Vec<u8>) {
        self.responses
            .lock()
            .expect("responses mutex")
            .insert(url.to_owned(), bytes);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ArtifactFetcher for MapFetcher {
    async fn fetch(&self, url: &str, max: usize) -> Result<Vec<u8>, FetchError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let bytes = self
            .responses
            .lock()
            .expect("responses mutex")
            .get(url)
            .cloned()
            .ok_or(FetchError::Status(404))?;
        if bytes.len() > max {
            return Err(FetchError::TooLarge { max });
        }
        Ok(bytes)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A second, real component with a different sha and identical behaviour.
///
/// A custom section is legal anywhere a section is, and the component model
/// allows it; `ArtifactPuller`'s `validate_component` is the check that this
/// stayed true.
fn with_custom_section(wasm: &[u8], tag: u8) -> Vec<u8> {
    let mut out = wasm.to_vec();
    // section id 0, payload length 3, name length 1, name "v", payload tag.
    out.extend_from_slice(&[0x00, 0x03, 0x01, b'v', tag]);
    out
}

async fn wait_until<F, Fut>(what: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if condition().await {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workers_announcement_is_pulled_and_the_client_moves_to_the_new_instance() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(block) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    let live_wasm = std::fs::read(&wasm_path).expect("read test-indexer wasm");
    let new_wasm = with_custom_section(&live_wasm, 0x01);
    assert_ne!(live_wasm, new_wasm, "the announced build must be a new sha");

    let dir = tempdir("client-move-end-to-end");
    let storage = ModuleStorage::new(&dir);

    // ── a running host, with a real engine and a null chain plane ─────────
    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let chain_plane = Arc::new(NullChainDataPlane);
    let dp: Arc<dyn DataPlaneFacade> = chain_plane.clone();

    let (tx, _rx) = tokio::sync::broadcast::channel(256);
    let sub_factory: SubscriptionFactory<BroadcastSub> = Arc::new({
        let tx = tx.clone();
        move |_resume: Option<ChainPoint>| BroadcastSub { rx: tx.subscribe() }
    });
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

    let host = Arc::new(
        ModuleHostV2::new(
            storage.clone(),
            engine,
            dp,
            chain_plane,
            sub_factory,
            kv_factory,
            emitter_factory,
            ResourceBudget::default(),
        )
        .with_fallback_source(FallbackSource::Disabled),
    );
    let host_handle: Arc<dyn ModuleHostHandle> = host.clone();

    // ── the host's control plane ──────────────────────────────────────────
    let auth = Arc::new(
        Authenticator::from_registry_file("test-namespaces.toml", NAMESPACES, &|name| {
            Some(format!("{name}-{}", "x".repeat(48)))
        })
        .expect("the test registry parses"),
    );
    let events = EventRing::new();
    let clock = mitos_platform::lifecycle::system_clock();
    let lifecycle = Arc::new(LifecycleManager::new(
        storage.clone(),
        host_handle,
        auth.clone(),
        events.clone(),
        clock.clone(),
    ));
    let fetcher = Arc::new(MapFetcher::default());
    let puller = Arc::new(ArtifactPuller::new(storage.clone(), fetcher.clone()));
    let moves = MoveManager::new(
        storage.clone(),
        puller,
        auth.clone(),
        lifecycle.clone(),
        events.clone(),
        clock,
    );

    // ── the live build, and a client attached to it ───────────────────────
    let live_manifest = manifest_v2(&live_wasm);
    storage
        .activate(&live_manifest, &live_wasm)
        .expect("activate the live build");
    let live = storage
        .current_instance(MODULE)
        .expect("read current instance")
        .expect("activation points current.toml at the instance");
    write_companion_subscription(&storage, MODULE, CLIENT, "policy_a", Vec::new());

    lifecycle.boot().await;
    moves.boot().await;
    wait_until("the live follower to start", || {
        let host = host.clone();
        let live = live.clone();
        async move { host.running_instances_of(MODULE).await.contains(&live) }
    })
    .await;

    // One block, so the live instance has a cursor for the staging to copy.
    tx.send(TipEvent::Apply(
        ChainPoint::Slot(TIP_SLOT).into(),
        Arc::new(block),
    ))
    .expect("send tip event");
    wait_until("the live cursor to flush the block", || {
        let storage = storage.clone();
        let live = live.clone();
        async move {
            storage
                .read_cursor_at(&live)
                .ok()
                .flatten()
                .map(|c| c.slot() == TIP_SLOT)
                .unwrap_or(false)
        }
    })
    .await;

    // ── the worker announces a new build ──────────────────────────────────
    let mut new_manifest = manifest_v2(&new_wasm);
    new_manifest.lifecycle.revision = Revision(2);
    let new_manifest_bytes = new_manifest.to_toml().expect("manifest toml");

    let announced = AnnouncedModule {
        module: ModuleId::parse(MODULE).expect("module id"),
        wasm: Sha256Hex::parse(&mitos_platform::manifest::sha256_hex(&new_wasm)).expect("wasm sha"),
        manifest: Sha256Hex::parse(&mitos_platform::manifest::sha256_hex(
            new_manifest_bytes.as_bytes(),
        ))
        .expect("manifest sha"),
        revision: Revision(2),
        config: None,
    };
    let client = ClientId::parse(CLIENT).expect("client id");
    let source = ArtifactSource::for_client(&client, &announced.module, &announced.wasm)
        .expect("the client is a hostname");
    fetcher.serve(&source.wasm_url, new_wasm);
    fetcher.serve(&source.manifest_url, new_manifest_bytes.into_bytes());

    assert_eq!(
        moves.announce(&client, &announced.module, &announced).await,
        AnnounceResult::Accepted,
    );
    moves.settle().await;

    let row = storage
        .read_row(MODULE, &client)
        .expect("read row")
        .expect("the announcement creates a row");
    let staging = row
        .pending
        .as_ref()
        .expect("an accepted announcement leaves a pending move")
        .target
        .instance
        .clone();
    assert_ne!(staging, live, "the target must be a second instance");

    wait_until("the staging instance to be built", || {
        let storage = storage.clone();
        let module = announced.module.clone();
        let staging = staging.clone();
        async move {
            storage
                .list_instances(&module)
                .map(|instances| instances.contains(&staging))
                .unwrap_or(false)
        }
    })
    .await;

    // ── the tick carries the move to the switch ───────────────────────────
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        moves.tick().await;
        moves.settle().await;

        let row = storage
            .read_row(MODULE, &client)
            .expect("read row")
            .expect("the row survives the commit");
        if row.pending.is_none() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the move did not commit; row: {row:?}",
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ── the row is the switch, and delivery follows it ────────────────────
    let row = storage
        .read_row(MODULE, &client)
        .expect("read row")
        .expect("the row survives the commit");
    let current = row.current.as_ref().expect("a committed row has a current");
    assert_eq!(current.instance, staging);
    assert_eq!(current.revision, Revision(2));
    assert_eq!(
        storage
            .current_instance(MODULE)
            .expect("read current")
            .expect("succession moves current.toml to the new instance"),
        staging,
        "the client that emptied the old instance takes current.toml with it",
    );
    assert!(
        host.running_instances_of(MODULE).await.contains(&staging),
        "the instance the client moved onto must be following",
    );
    assert_eq!(
        storage
            .read_cursor_at(&staging)
            .expect("read staging cursor")
            .map(|c| c.slot()),
        Some(TIP_SLOT),
        "the staging instance resumes from the parent's cursor",
    );

    // The pull happened once: wasm and manifest, and nothing else.
    assert_eq!(fetcher.calls(), 2, "one wasm and one manifest fetch");
    assert!(
        storage.artifact_path(&announced.wasm).exists(),
        "the verified artifact is in the CAS",
    );

    // The old instance is retained, not deleted — rollback is a row move back.
    assert!(
        storage
            .list_instances(&announced.module)
            .expect("list instances")
            .contains(&live),
        "the superseded instance is retained for rollback",
    );

    host.stop_all().await;
    std::fs::remove_dir_all(&dir).ok();
}
