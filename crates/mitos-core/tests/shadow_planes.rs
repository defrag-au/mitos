//! Shadow diff: one module artifact, one interest, **two planes**.
//!
//! The M2 gate is "the cohort runs on the book in shadow, then default". This is
//! the inner-loop version of that: run the same wasm against a Dolos domain and
//! against a published live book, over the same chunks, and compare what the
//! module emits.
//!
//! Both planes are built from the same chunk store, so they answer for the same
//! instant. The domain is opened with `setup_domain` and **never** given a sync
//! pipeline — that is deliberate: if either plane moved during the run, a
//! difference could be drift rather than a disagreement, and the diff would be
//! worthless.
//!
//! Comparison is byte equality of the emission payloads, in order. An ordering
//! difference is reported separately from a content difference, because the two
//! mean different things: reordered pages are a plane property a consumer sees,
//! different bytes are a wrong answer.
//!
//! ```sh
//! MITOS_SHADOW_BOOK_DIR=.tmp/shadow/book-full \
//! MITOS_SHADOW_DOLOS_CONFIG=.tmp/shadow/dolos-mainnet/mitos.toml \
//! MITOS_SHADOW_POLICY=0a85dd1543465407852c90e66c074a3b52ea2d7c77a2346ddc20550a \
//!   cargo test -p mitos-core --test shadow_planes -- --nocapture
//! ```
//!
//! `MITOS_SHADOW_MODULE` selects the artifact (default `collection-holders`) and
//! `MITOS_SHADOW_TIMEOUT_SECS` bounds each run (default 300).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cardano_assets::PolicyId;
use mitos_core::{load_mitos_toml, setup_domain};
use mitos_platform::host_fns::{DataPlaneFacade, DomainDataPlane, emit, state_kv};
use mitos_platform::host_v2::{InterestRouter, KvFactory, ModuleHostV2, SubscriptionFactory};
use mitos_platform::manifest::Manifest;
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_platform::{BookDataPlane, ChainDataPlane};
use mitos_protocol::{
    AssetSelector, Interest as WireInterest, InterestOp as WireInterestOp, SubscribeRequest,
    SubscribeTarget,
};
use tokio::sync::Mutex;

const CLIENT_ID: &str = "shadow-test";
const COMPANION_KEY: &str = "shadow-companion";

/// One run's emissions, in the order the host queued them.
struct Run {
    events: Vec<Vec<u8>>,
    elapsed: Duration,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_module_emits_the_same_bytes_from_both_planes() {
    let Ok(book_dir) = std::env::var("MITOS_SHADOW_BOOK_DIR") else {
        eprintln!("skipping: MITOS_SHADOW_BOOK_DIR is not set");
        return;
    };
    let Ok(dolos_config) = std::env::var("MITOS_SHADOW_DOLOS_CONFIG") else {
        eprintln!("skipping: MITOS_SHADOW_DOLOS_CONFIG is not set");
        return;
    };
    let policy_hex = std::env::var("MITOS_SHADOW_POLICY")
        .expect("MITOS_SHADOW_POLICY (28-byte hex) is required");
    let module_id =
        std::env::var("MITOS_SHADOW_MODULE").unwrap_or_else(|_| "collection-holders".to_owned());
    let timeout = std::env::var("MITOS_SHADOW_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300u64);

    let artifact = artifact_dir(&module_id);
    let wasm = std::fs::read(artifact.join(format!("{module_id}.wasm"))).unwrap_or_else(|e| {
        panic!(
            "{} could not be read ({e}) — build it with \
             `cargo run --release -p mitos-build -- --module community-modules/{module_id}/{}.rs`",
            artifact.display(),
            module_id.replace('-', "_"),
        )
    });
    let manifest = Manifest::parse(
        &std::fs::read_to_string(artifact.join("manifest.toml")).expect("manifest.toml"),
    )
    .expect("parse manifest");

    let book = Arc::new(BookDataPlane::open(Path::new(&book_dir)).expect("open the book"));
    let book_tip = ChainDataPlane::tip(&*book)
        .await
        .expect("the book must be stamped")
        .slot();

    let config_text = std::fs::read_to_string(&dolos_config).expect("read the dolos config");
    let lookup = |k: &str| std::env::var(k).ok();
    let loaded = load_mitos_toml(&config_text, &lookup).expect("parse the dolos config");
    let domain = setup_domain(&loaded.root).expect("open the dolos domain");
    let dolos = Arc::new(DomainDataPlane::new(domain));
    let dolos_tip = ChainDataPlane::tip(&*dolos)
        .await
        .expect("the dolos domain must have a tip")
        .slot();

    println!("module     {module_id}");
    println!("policy     {policy_hex}");
    println!("book       slot {book_tip}");
    println!("dolos      slot {dolos_tip}");
    if book_tip != dolos_tip {
        // Not fatal: the two repositories are built from the same chunks, so a
        // difference here means one of them was imported at a different point —
        // which makes every downstream answer suspect. Say so loudly.
        println!(
            "WARNING    the planes are at different slots ({dolos_tip} vs {book_tip}); \
             a difference below may be drift rather than disagreement"
        );
    }

    let book_dp: Arc<dyn DataPlaneFacade> = book.clone();
    let dolos_dp: Arc<dyn DataPlaneFacade> = dolos.clone();

    let book_run = run_module(
        "book",
        book.clone(),
        book_dp,
        &manifest,
        &wasm,
        &policy_hex,
        timeout,
    )
    .await;
    let dolos_run = run_module(
        "dolos",
        dolos.clone(),
        dolos_dp,
        &manifest,
        &wasm,
        &policy_hex,
        timeout,
    )
    .await;

    println!(
        "book       {} events · {} bytes · {:.1}s",
        book_run.events.len(),
        byte_len(&book_run.events),
        book_run.elapsed.as_secs_f64()
    );
    println!(
        "dolos      {} events · {} bytes · {:.1}s",
        dolos_run.events.len(),
        byte_len(&dolos_run.events),
        dolos_run.elapsed.as_secs_f64()
    );

    compare(&dolos_run.events, &book_run.events);
}

/// Run the module once against one plane and collect the payload bytes.
async fn run_module<P>(
    label: &str,
    plane: Arc<P>,
    dp: Arc<dyn DataPlaneFacade>,
    manifest: &Manifest,
    wasm: &[u8],
    policy_hex: &str,
    timeout_secs: u64,
) -> Run
where
    P: ChainDataPlane + Send + Sync + 'static,
{
    let module_id = manifest.module.id.clone();
    let started = Instant::now();

    let dir = tempfile::tempdir().expect("tempdir");
    let storage = ModuleStorage::new(dir.path());
    storage.activate(manifest, wasm).expect("activate");
    write_companion_subscription(&storage, &module_id);

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let rx_holder = Arc::new(Mutex::new(rx));
    let sub_factory: SubscriptionFactory<OneShotSub> = Arc::new(move |_resume| OneShotSub {
        rx: rx_holder.clone(),
    });

    let storage_for_kv = storage.clone();
    let kv_factory: KvFactory = Arc::new(move |key| match storage_for_kv.kv_store_at(key, None) {
        Ok(kv) => state_kv::ModuleKv::Redb(kv),
        Err(e) => panic!("kv_store_at: {e}"),
    });
    let emitter_factory = Arc::new(emit::EventSink::new);

    let host = ModuleHostV2::new(
        storage.clone(),
        engine,
        dp,
        plane,
        sub_factory,
        kv_factory,
        emitter_factory,
        ResourceBudget::default(),
    );
    host.start(&module_id, false).await.expect("start");

    let mut interest = WireInterest::any();
    interest.asset =
        AssetSelector::Policy(PolicyId::new(policy_hex.to_owned()).expect("policy id"));
    host.route_interest(&module_id, WireInterestOp::Add, vec![interest])
        .await
        .expect("route_interest");

    // The module's own schema decides when a snapshot ends, and this harness
    // deliberately does not know it: drain until the stream has been quiet for
    // ten seconds, or until nothing has arrived at all. The window is generous
    // because a chunked cold start can pause between pages, and cutting a
    // snapshot short would show up as a count difference between the planes.
    let deadline = started + Duration::from_secs(timeout_secs);
    let mut events: Vec<Vec<u8>> = Vec::new();
    let mut quiet_since = Instant::now();
    loop {
        if Instant::now() >= deadline {
            eprintln!("{label}: timed out with {} events", events.len());
            break;
        }
        let rows = storage
            .emissions_store(&module_id)
            .expect("emissions store")
            .list_queued_for_companion(COMPANION_KEY, CLIENT_ID)
            .expect("list emissions");
        if rows.len() > events.len() {
            for row in rows.iter().skip(events.len()) {
                events.push(row.payload.clone());
            }
            quiet_since = Instant::now();
        } else if (!events.is_empty() && quiet_since.elapsed() > Duration::from_secs(10))
            || (events.is_empty() && started.elapsed() > Duration::from_secs(30))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    host.stop(&module_id).await.expect("stop");
    drop(tx);

    Run {
        events,
        elapsed: started.elapsed(),
    }
}

/// Byte equality, with the ordering case named separately.
fn compare(dolos: &[Vec<u8>], book: &[Vec<u8>]) {
    assert!(
        !dolos.is_empty() || !book.is_empty(),
        "neither plane emitted anything — the interest matched nothing on either side, so this \
         run proves nothing"
    );
    assert_eq!(
        dolos.len(),
        book.len(),
        "event count differs: dolos {} vs book {}",
        dolos.len(),
        book.len()
    );

    for (i, (a, b)) in dolos.iter().zip(book).enumerate() {
        if a != b {
            let mut sorted_dolos: Vec<&Vec<u8>> = dolos.iter().collect();
            let mut sorted_book: Vec<&Vec<u8>> = book.iter().collect();
            sorted_dolos.sort();
            sorted_book.sort();
            assert_eq!(
                sorted_dolos,
                sorted_book,
                "event {i} differs and the streams are not a reordering of each other \
                 (dolos {} B, book {} B)",
                a.len(),
                b.len()
            );
            panic!(
                "the same {} events arrived in a different order (first difference at {i}): \
                 the planes answer with the same content but not the same sequence",
                dolos.len()
            );
        }
    }
    println!("OK          {} events byte-identical", dolos.len());
}

fn artifact_dir(module_id: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("mitos/")
        .join("community-modules")
        .join(module_id)
        .join("target/mitos")
        .join(module_id)
}

fn byte_len(events: &[Vec<u8>]) -> usize {
    events.iter().map(Vec::len).sum()
}

// ── the two test-support pieces this harness needs ──────────────────────────

/// A subscription that never yields: both planes are frozen, so nothing follows.
struct OneShotSub {
    rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<dolos_core::TipEvent>>>,
}

impl dolos_core::TipSubscription for OneShotSub {
    async fn next_tip(&mut self) -> dolos_core::TipEvent {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(event) => event,
            None => std::future::pending().await,
        }
    }
}

/// Register the companion interest the host reads at start, so the module runs
/// the way a subscribed companion makes it run.
fn write_companion_subscription(storage: &ModuleStorage, module_id: &str) {
    let dir = storage.module_dir_for_companions(module_id).join(CLIENT_ID);
    std::fs::create_dir_all(&dir).expect("companion client dir");
    let request = SubscribeRequest {
        targets: vec![SubscribeTarget::Module {
            name: module_id.to_owned(),
        }],
        companion_key: COMPANION_KEY.to_owned(),
        client_id: CLIENT_ID.to_owned(),
        resume_from: None,
        interests: vec![WireInterest::any()],
        dial_back: None,
        announced: Vec::new(),
    };
    std::fs::write(
        dir.join(format!("{COMPANION_KEY}.cbor")),
        request.encode().expect("encode subscribe"),
    )
    .expect("write companion cbor");
}
