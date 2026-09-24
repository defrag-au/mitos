//! `collection-holders` cold-started against a published live book.
//!
//! This is `PHONEBOOK_COVERAGE.md` §11's item 3 — the first time a **shipped**
//! module runs against the phonebook rather than against a fixture, a stub plane
//! or dolos. Everything else in that document is a reading of code against the
//! artifact's shape; this is the execution test that finds the gaps reading has
//! not found.
//!
//! # What it does
//!
//! Boots the real `mitos-platform` host with a [`BookDataPlane`] over a published
//! book, activates the real built `collection-holders` artifact, subscribes a
//! companion to one policy via dynamic interest, and lets the host's chunked
//! cold-start pump run the module's `rebootstrap` to completion. The module walks
//! `utxos_by_policy` → `read_utxos` through the host functions, folds a
//! `(asset_name, holder)` ledger, and emits `SnapshotBegin` → `SnapshotChunk`* →
//! `SnapshotEnd`.
//!
//! # What it asserts
//!
//! The emission is checked against the book through the **same trait the module
//! uses**, computed independently in this file:
//!
//! - the snapshot sequence is complete — one `Begin`, ≥0 `Chunk`, one `End`;
//! - `SnapshotEnd.holding_count` equals the holdings the chunks carried, which is
//!   the protocol's own invariant and the one a consumer trusts;
//! - the set of asset names emitted is exactly the set the book holds under that
//!   policy, *restricted to outputs the module can categorise* (see below);
//! - **Σ quantity per asset name is equal**, not merely bounded. That is the sum
//!   over every UTxO of a policy, through the scan, the projection, the module's
//!   SUM merge and the emit path.
//!
//! # The one rule that is duplicated on purpose
//!
//! The module categorises holders by address shape and **drops** what it cannot
//! (`extract_holder_key`): Byron/bootstrap, and Shelley types 4–5 whose delegation
//! is a pointer rather than a credential. So the book-side sum here is taken over
//! `header >> 4 ∈ {0,1,2,3,6,7}` and nothing else. Writing that rule a second time
//! in this file is the point — it is the specification, and the module is the
//! implementation. If they ever disagree, this test fails rather than one of them
//! being quietly right.
//!
//! # Why the event types are mirrored rather than imported
//!
//! `mitos-community-events` is not a dependency of this crate. Importing it would
//! mean editing `Cargo.toml`, and the mirror costs forty lines: it decodes the
//! documented wire shape with an independent transcription, and a rename upstream
//! fails here loudly instead of being absorbed. If the crate is ever added as a
//! dev-dependency this file should use it and delete the mirror.
//!
//! # Running it
//!
//! ```sh
//! MITOS_BOOK_DIR=/srv/data/fold-book/full3 cargo test -p mitos-platform \
//!     --test collection_holders_over_book -- --nocapture
//! ```
//!
//! `MITOS_BOOK_DIR` unset ⇒ the test **skips**, which is the CI shape: the book is
//! gigabytes and CI has none. Set ⇒ every missing piece is a failure. There is no
//! third state, because a test that reports green after doing nothing is worse
//! than one that is absent.
//!
//! `MITOS_BOOK_POLICY` (56 hex) pins the policy; unset ⇒ the first policy in
//! `policy.tbl` with at least [`MIN_ROWS`] live rows, which is deterministic and
//! cheap.
//!
//! # It passes, and how it got there is the useful part
//!
//! **First run, 2026-09-23.** Everything ran except the module: the host booted, the
//! artifact loaded, dynamic interest routed, the onboard scope was shared from the
//! live instance to the pump's disposable one — and then the cold start **trapped on
//! its first page scan, emitting nothing**:
//!
//! ```text
//! backfill: rebootstrap trapped (non-retryable); refill may be partial
//! emitted 0 begin · 0 chunk(s) · 0 end · 0 holding(s)
//! ```
//!
//! The cause was one missing number. `HostStateV2::utxos_by_policy` freezes a fresh
//! scan as of a tip — `scan_anchor_slot` → `tip().slot()`
//! (`host_fns_v2/chain_data.rs`) — and `BookDataPlane::tip` refused, because the
//! artifact recorded no chain position. The `?` became a host-fn error the guest
//! calls without a `Result` in front of it, so the first page scan killed the whole
//! cold start.
//!
//! That was `PHONEBOOK_COVERAGE.md` §12's gap, and it closed the same day:
//! `book.meta` gained an optional `base_slot`, a build writes it from the walk,
//! `fold-probe stamp-boundary` fills it in for an artifact built before it existed,
//! and `tip()` answers `ChainPoint::Slot`. **Observed after the fix, same book**
//! (chunks 1848–1851, policy `4b9c44c5…`, 706 live refs):
//!
//! ```text
//! backfill: rebootstrap pump complete utxos_ingested=706 steps=6 outcome="completed"
//! emitted    1 begin · 1 chunk(s) · 1 end · 839 holding(s) · 839 asset names · 1.1 s
//! ✓ 839 asset names · Σ 839 units · 839 (asset, holder) entries over 706 live refs
//! ```
//!
//! 839 names from 706 refs — the policy puts more than one asset in some outputs, so
//! the module's SUM merge across a page is exercised rather than assumed.
//!
//! One warning remains and it is a **different** gap, left open on purpose: the
//! host's watched-UTxO seeding calls `search_utxos`, which the book still refuses for
//! want of a predicate algebra and a `Page` cursor. Non-fatal here — the seed logs
//! and carries on — but it is the next thing this test will notice when someone
//! implements it.

mod common;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cardano_assets::PolicyId;
use mitos_data_plane::{BookDataPlane, ChainDataPlane, DecodeLevel, OutputRef};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{InterestRouter, KvFactory, ModuleHostV2, SubscriptionFactory};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::{AssetSelector, Interest as WireInterest, InterestOp as WireInterestOp};
use pallas_addresses::{Address, ShelleyDelegationPart};
use serde::Deserialize;
use tokio::sync::Mutex;

use common::{OneShotSub, tempdir, write_companion_subscription};

const MODULE_ID: &str = "collection-holders";
const CLIENT_ID: &str = "book-test";
const COMPANION_KEY: &str = "book-companion";

/// The smallest policy worth running: below this the scan is a single page and the
/// test would pass without exercising the chunk loop.
const MIN_ROWS: usize = 200;
/// How many `policy.tbl` entries to examine before giving up on finding one.
const MAX_POLICY_CANDIDATES: usize = 400;

// ── The wire shape, transcribed from `mitos-community-events::collection_holders`
// ── (see the module doc for why it is not imported).

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CollectionEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    /// Stepped over rather than modelled: this test drives a cold start, so a
    /// delta would mean the module emitted outside the snapshot sequence it was
    /// asked for. The variant still has to be named or the decoder would fail.
    Delta(serde::de::IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct SnapshotBegin {
    policy: String,
    #[allow(dead_code)]
    cursor_slot: u64,
}

#[derive(Debug, Deserialize)]
struct SnapshotChunk {
    policy: String,
    holdings: Vec<Holding>,
}

#[derive(Debug, Deserialize)]
struct SnapshotEnd {
    policy: String,
    holding_count: u64,
}

#[derive(Debug, Deserialize)]
struct Holding {
    asset_name_hex: String,
    holder: HolderRef,
    quantity: u64,
}

/// The holder identity, as the module emits it. `Script`'s `label` is the
/// address-registry tag and is not asserted — it is `None` until that lookup
/// lands; the variant is modelled so the decoder sees the shape it documents.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HolderRef {
    /// 56-char lowercase hex of the 28-byte stake credential.
    Stake(String),
    /// Bech32 enterprise / no-stake address.
    Payment(String),
    /// Bech32 script address.
    Script {
        addr: String,
        #[allow(dead_code)]
        label: Option<String>,
    },
}

/// The built artifact directory, or the directory it would be in.
fn holders_artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("mitos/")
        .join("community-modules/collection-holders/target/mitos/collection-holders")
}

/// `Some(book)` when this run is meant to execute, `None` when it should skip.
fn book_dir() -> Option<PathBuf> {
    let dir = PathBuf::from(std::env::var_os("MITOS_BOOK_DIR")?);
    assert!(
        dir.join("book.meta").exists(),
        "MITOS_BOOK_DIR is set to {} but there is no book.meta there — set it or unset it, \
         do not let this test report green without running",
        dir.display()
    );
    Some(dir)
}

/// A policy from the book: `MITOS_BOOK_POLICY` when set, else the first entry of
/// `policy.tbl` with at least [`MIN_ROWS`] live rows.
///
/// The trait methods are called fully qualified because `BookDataPlane` implements
/// both `ChainDataPlane` and — through the blanket impl — `DataPlaneFacade`, and
/// both are in scope here.
async fn pick_policy(plane: &BookDataPlane) -> ([u8; 28], Vec<OutputRef>) {
    if let Ok(hex) = std::env::var("MITOS_BOOK_POLICY") {
        let bytes = hex::decode(&hex).expect("MITOS_BOOK_POLICY must be 56 hex characters");
        let policy: [u8; 28] = bytes
            .try_into()
            .expect("MITOS_BOOK_POLICY must be 28 bytes");
        let refs = ChainDataPlane::utxos_by_policy(plane, &policy)
            .await
            .expect("utxos_by_policy");
        return (policy, refs);
    }

    let table = plane.base().policies();
    for id in 0..table.len().min(MAX_POLICY_CANDIDATES) {
        let policy = *table.get(id as u32).expect("id in range");
        let refs = ChainDataPlane::utxos_by_policy(plane, &policy)
            .await
            .expect("utxos_by_policy");
        if refs.len() >= MIN_ROWS {
            return (policy, refs);
        }
    }
    panic!(
        "no policy among the first {MAX_POLICY_CANDIDATES} holds ≥ {MIN_ROWS} live rows; \
         set MITOS_BOOK_POLICY"
    );
}

/// Is this address one the module can categorise?
///
/// The same decision `extract_holder_key` makes, asked through pallas's typed
/// accessors rather than by poking at the header byte: types 0–3 delegate to a
/// credential (Key or Script) and types 6–7 carry none (`Null`), and both are
/// kept. A **pointer** delegation (types 4–5) is dropped, and Byron is not
/// Shelley at all so `from_bech32` does not produce one.
fn categorisable(address_bech32: &str) -> bool {
    let Ok(Address::Shelley(addr)) = Address::from_bech32(address_bech32) else {
        return false;
    };
    matches!(
        addr.delegation(),
        ShelleyDelegationPart::Key(_)
            | ShelleyDelegationPart::Script(_)
            | ShelleyDelegationPart::Null
    )
}

/// What the book says, through the trait the module uses: `asset_name_hex →
/// Σ quantity` over the outputs the module can categorise.
async fn expected(
    plane: &BookDataPlane,
    policy: &[u8; 28],
    refs: &[OutputRef],
) -> (BTreeMap<String, u64>, u64) {
    let policy_hex = hex::encode(policy);
    let outputs = ChainDataPlane::read_utxos(plane, refs, DecodeLevel::Lean)
        .await
        .expect("read_utxos");

    let mut sums: BTreeMap<String, u64> = BTreeMap::new();
    let mut dropped = 0u64;
    for (_oref, out) in &outputs {
        if !categorisable(&out.address) {
            dropped += 1;
            continue;
        }
        for asset in &out.assets {
            if asset.policy_id.as_ref() != policy_hex {
                continue;
            }
            *sums.entry(asset.asset_name_hex.clone()).or_insert(0) += asset.quantity;
        }
    }
    (sums, dropped)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collection_holders_cold_starts_against_a_published_book() {
    let Some(book) = book_dir() else {
        eprintln!("skipping: MITOS_BOOK_DIR is not set");
        return;
    };

    // The module logs through `logging::log` → the host fn → `tracing`, and nothing
    // else in this suite installs a subscriber. Without one, a cold start that
    // emits nothing is indistinguishable from one that never ran — which is the
    // first thing this test found.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    let artifact = holders_artifact_dir();
    let wasm_path = artifact.join(format!("{MODULE_ID}.wasm"));
    let wasm = std::fs::read(&wasm_path).unwrap_or_else(|e| {
        panic!(
            "{} could not be read ({e}) — build it with \
             `cargo run --release -p mitos-build -- --module collection-holders`",
            wasm_path.display()
        )
    });
    let manifest = mitos_platform::manifest::Manifest::parse(
        &std::fs::read_to_string(artifact.join("manifest.toml")).expect("manifest.toml"),
    )
    .expect("parse manifest");
    assert_eq!(manifest.module.id, MODULE_ID);

    let plane = Arc::new(BookDataPlane::open(&book).expect("open book"));
    println!(
        "book       {} · {} rows · {} policies",
        book.display(),
        plane.rows(),
        plane.base().policies().len()
    );

    // The pre-flight: a fresh page scan is frozen as of a tip, and a plane that
    // cannot name one refuses a host fn the module calls without a `Result` in
    // front of it — so the module traps rather than degrading. Check it here, where
    // the failure is one line and names the fix, instead of after a timeout with
    // zero emissions. See the module doc.
    match ChainDataPlane::tip(&*plane).await {
        Ok(tip) => println!("tip        slot {}", tip.slot()),
        Err(e) => panic!(
            "this book cannot answer `tip()` ({e}), and a cold-start page scan needs the anchor \
             it carries: `HostStateV2::utxos_by_policy` → `scan_anchor_slot` → `tip().slot()` \
             (host_fns_v2/chain_data.rs), so the host fn errors and the guest's \
             `chain_data::utxos_by_policy` call traps. The module cannot emit a snapshot. \
             The gap is one `u64` in `book.meta` — the slot at the fold's boundary, which \
             `ChainPoint::Slot` already represents and the fold already walks past. See \
             PHONEBOOK_COVERAGE.md §12 / DELTA_CHAIN.md §7."
        ),
    }

    let (policy, refs) = pick_policy(&plane).await;
    let policy_hex = hex::encode(policy);
    println!("policy     {policy_hex} · {} live refs", refs.len());

    let started = Instant::now();
    let (want, dropped) = expected(&plane, &policy, &refs).await;
    println!(
        "expected   {} asset names · {} rows at addresses the module cannot categorise · {:.1} s",
        want.len(),
        dropped,
        started.elapsed().as_secs_f64()
    );
    assert!(
        !want.is_empty(),
        "the book holds no categorisable output for {policy_hex}; pick another policy"
    );

    // ── the host, exactly as the bundle builds it ───────────────────────────

    let storage_dir = tempdir("collection-holders-over-book");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");
    write_companion_subscription(
        &storage,
        MODULE_ID,
        CLIENT_ID,
        COMPANION_KEY,
        vec![WireInterest::any()],
    );

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let dp: Arc<dyn DataPlaneFacade> = plane.clone();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let rx_holder = Arc::new(Mutex::new(rx));
    let sub_factory: SubscriptionFactory<OneShotSub> = Arc::new(move |_resume| OneShotSub {
        rx: rx_holder.clone(),
    });

    // The production KV shape, not `new_in_memory()`: `ModuleStorage::kv_store_at`
    // hands every call the *same* `Arc<Database>` per instance, and the chunked
    // cold start depends on that — the live instance's `update_interest` seeds the
    // onboard scope, and the pump's disposable backfill instance reads it back. A
    // fresh in-memory map per call would make the scope invisible and the pump a
    // silent no-op.
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
        plane.clone(),
        sub_factory,
        kv_factory,
        emitter_factory,
        ResourceBudget::default(),
    );
    host.start(MODULE_ID, false).await.expect("start");

    // ── subscribe ───────────────────────────────────────────────────────────

    let mut interest = WireInterest::any();
    interest.asset = AssetSelector::Policy(PolicyId::new(policy_hex.clone()).expect("policy id"));
    host.route_interest(MODULE_ID, WireInterestOp::Add, vec![interest])
        .await
        .expect("route_interest");

    // ── collect the snapshot ────────────────────────────────────────────────

    let deadline = Instant::now()
        + Duration::from_secs(
            std::env::var("MITOS_BOOK_TEST_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
        );
    let mut events: Vec<CollectionEvent> = Vec::new();
    let mut polls = 0u64;
    let mut saw_end = false;
    while Instant::now() < deadline {
        polls += 1;
        let rows = storage
            .emissions_store(MODULE_ID)
            .expect("emissions store")
            .list_queued_for_companion(COMPANION_KEY, CLIENT_ID)
            .expect("list emissions");
        if rows.len() as u64 > events.len() as u64 {
            for row in rows.iter().skip(events.len()) {
                match ciborium::de::from_reader::<CollectionEvent, _>(row.payload.as_slice()) {
                    Ok(event) => events.push(event),
                    Err(e) => panic!("emission {} is not a CollectionEvent: {e}", row.id),
                }
            }
            if matches!(events.last(), Some(CollectionEvent::SnapshotEnd(_))) {
                saw_end = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = started.elapsed();
    host.stop(MODULE_ID).await.expect("stop");
    drop(tx);

    // ── assert ──────────────────────────────────────────────────────────────

    let mut chunks: Vec<&SnapshotChunk> = Vec::new();
    let mut end: Option<&SnapshotEnd> = None;
    let mut begins = 0usize;
    for event in &events {
        match event {
            CollectionEvent::SnapshotBegin(b) => {
                begins += 1;
                assert_eq!(b.policy, policy_hex, "snapshot is for another policy");
            }
            CollectionEvent::SnapshotChunk(c) => {
                assert_eq!(c.policy, policy_hex, "chunk is for another policy");
                chunks.push(c);
            }
            CollectionEvent::SnapshotEnd(e) => {
                assert_eq!(e.policy, policy_hex);
                end = Some(e);
            }
            CollectionEvent::Delta(_) => {}
        }
    }

    let held: u64 = chunks.iter().map(|c| c.holdings.len() as u64).sum();
    let mut got: BTreeMap<String, u64> = BTreeMap::new();
    for chunk in &chunks {
        for holding in &chunk.holdings {
            // The holder identity is not cross-checked against the book here — that
            // would be the same categorisation rule a third time — but its *shape*
            // is the module's public contract and costs nothing to hold to it.
            match &holding.holder {
                HolderRef::Stake(cred) => assert!(
                    cred.len() == 56
                        && cred
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
                    "stake holder `{cred}` is not 56 lowercase hex characters",
                ),
                HolderRef::Payment(addr) | HolderRef::Script { addr, .. } => assert!(
                    addr.starts_with("addr1") || addr.starts_with("addr_test1"),
                    "holder address `{addr}` is not bech32",
                ),
            }
            *got.entry(holding.asset_name_hex.clone()).or_insert(0) += holding.quantity;
        }
    }

    println!(
        "emitted    {begins} begin · {} chunk(s) · {} end · {held} holding(s) · {} asset names · \
         {:.1} s",
        chunks.len(),
        usize::from(end.is_some()),
        got.len(),
        elapsed.as_secs_f64(),
    );
    println!("           {polls} poll(s) of the emissions store");

    assert!(
        saw_end,
        "no SnapshotEnd within the deadline — got {} emission(s): the cold start did not \
         complete",
        events.len()
    );
    assert_eq!(begins, 1, "exactly one SnapshotBegin opens a snapshot");
    let end = end.expect("SnapshotEnd seen above");
    assert_eq!(
        end.holding_count, held,
        "`holding_count` must equal the holdings the chunks carried"
    );

    // The two assertion bodies, reported side by side so a failure is readable
    // rather than a diff of two maps.
    let names_match = got.keys().eq(want.keys());
    let sums_match = got == want;
    if !names_match || !sums_match {
        let mut only_book: Vec<&String> = want.keys().filter(|k| !got.contains_key(*k)).collect();
        let mut only_module: Vec<&String> = got.keys().filter(|k| !want.contains_key(*k)).collect();
        only_book.truncate(10);
        only_module.truncate(10);
        for (name, expected_qty) in &want {
            if got.get(name) != Some(expected_qty) {
                println!(
                    "  {}  book {expected_qty} · module {}",
                    if name.is_empty() {
                        "<empty name>"
                    } else {
                        name
                    },
                    got.get(name)
                        .map_or("absent".to_string(), |q| q.to_string()),
                );
            }
        }
        panic!(
            "the snapshot does not match the book: {} asset name(s) only in the book {only_book:?}, \
             {} only in the module {only_module:?}, {} mismatch(es) listed above",
            want.len() - want.keys().filter(|k| got.contains_key(*k)).count(),
            got.len() - got.keys().filter(|k| want.contains_key(*k)).count(),
            want.iter().filter(|(k, v)| got.get(*k) != Some(v)).count(),
        );
    }

    // Report what the module actually did, so a green run is also evidence.
    let entries: u64 = chunks.iter().map(|c| c.holdings.len() as u64).sum();
    println!(
        "  ✓ {} asset names · Σ {} units · {entries} (asset, holder) entries over {} live refs",
        got.len(),
        got.values().sum::<u64>(),
        refs.len(),
    );

    std::fs::remove_dir_all(&storage_dir).ok();
}
