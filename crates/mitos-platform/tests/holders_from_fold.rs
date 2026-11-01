//! `holders-from-fold` serving a published fold artifact.
//!
//! This is `docs/design/FOLD_PLANE.md` §8 gate item 4, the half that does not need a
//! chain: the host serves an artifact, the **shipped** module reads it through the
//! `fold-data` interface, and the emissions are checked against the artifact's own
//! bytes.
//!
//! # What it runs
//!
//! Boots the real `mitos-platform` host with [`NullChainDataPlane`] — the module must
//! never call a chain host fn, and a stub plane is how that is enforced rather than
//! trusted — activates the real built `holders-from-fold` artifact, and calls
//! `start(module, rebootstrap = true)`, which drives the module's `rebootstrap` on a
//! disposable backfill instance to completion. The module reads `artifact.meta`, pages
//! `holders.tsv` through `fold-data::read`, parses it, and emits
//! `SnapshotBegin` → `SnapshotChunk`* → `SnapshotEnd`.
//!
//! # What it asserts
//!
//! The rows file is parsed **in this file**, independently of `mitos-holder-table`, and
//! the two are compared as maps:
//!
//! - one `Begin`, at least one `Chunk`, one `End`, and no `Delta`;
//! - every event names the policy the manifest's extension names;
//! - `SnapshotEnd.holding_count` equals the holdings the chunks carried;
//! - `(asset_name_hex, holder) → quantity` is equal, with the holder's wire variant
//!   (`Stake` / `Payment` / `Script`) derived here from the address;
//! - `SnapshotBegin.cursor_slot` is the manifest's `base_slot`.
//!
//! Writing the holder rule a second time is the point, exactly as in
//! `collection_holders_over_book.rs`: this file is the specification, the module is the
//! implementation, and a divergence fails here rather than one of them being quietly
//! right.
//!
//! # Running it
//!
//! ```sh
//! cargo test -p mitos-platform --test holders_from_fold -- --nocapture
//! ```
//!
//! `MITOS_FOLD_ARTIFACT` names the artifact; unset ⇒ `.tmp/pilot/full` when it holds an
//! `artifact.meta`, otherwise the test **skips** (the CI shape — CI has no pilot). Set
//! ⇒ every missing piece is a failure, including a module that has not been built.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{KvFactory, ModuleHostV2, SubscriptionFactory};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::Interest as WireInterest;
use pallas_addresses::Address;
use serde::Deserialize;
use tokio::sync::Mutex;

use common::{NullChainDataPlane, OneShotSub, tempdir, write_companion_subscription};

const MODULE_ID: &str = "holders-from-fold";
const CLIENT_ID: &str = "fold-test";
const COMPANION_KEY: &str = "fold-companion";

/// The rows file's header, as `HolderTable::ROWS_HEADER` writes it. Repeated here
/// because this file is the reader's specification, not a consumer of it.
const ROWS_HEADER: &str = "holder\taddress\tcohort\tasset_name_hex\tquantity";

// ── The wire shape, transcribed from `mitos-community-events::collection_holders`
// ── (mirrored rather than imported, as in `collection_holders_over_book.rs`: the
// ── crate is not a dependency of this one, and an independent transcription fails
// ── loudly on a rename instead of absorbing it).

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CollectionEvent {
    SnapshotBegin(SnapshotBegin),
    SnapshotChunk(SnapshotChunk),
    SnapshotEnd(SnapshotEnd),
    /// Modelled so the decoder accepts the variant; a fold round must never produce
    /// one, which the assertion below holds it to.
    Delta(serde::de::IgnoredAny),
}

#[derive(Debug, Deserialize)]
struct SnapshotBegin {
    policy: String,
    cursor_slot: u64,
    cursor_hash_hex: String,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum HolderRef {
    Stake(String),
    Payment(String),
    Script {
        addr: String,
        #[allow(dead_code)]
        label: Option<String>,
    },
}

// ── Locating the two artifacts ──────────────────────────────────────────────

/// The artifact directory, or `None` when this run should skip.
fn artifact_dir() -> Option<PathBuf> {
    if let Some(dir) = std::env::var_os("MITOS_FOLD_ARTIFACT") {
        let dir = PathBuf::from(dir);
        assert!(
            dir.join("artifact.meta").exists(),
            "MITOS_FOLD_ARTIFACT is set to {} but there is no artifact.meta there — set it \
             or unset it, do not let this test report green without running",
            dir.display()
        );
        return Some(dir);
    }
    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".tmp/pilot/full");
    fallback.join("artifact.meta").exists().then_some(fallback)
}

/// The built module artifact directory, or the directory it would be in.
fn module_artifact_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("mitos/")
        .join("community-modules/holders-from-fold/target/mitos/holders-from-fold")
}

// ── The expected snapshot, parsed here ──────────────────────────────────────

/// A holder as a comparable string: the wire variant plus its identity.
///
/// `Stake` is the 28-byte credential; an enterprise address keeps its bech32 and the
/// payment credential's kind decides `Payment` vs `Script`. This is the rule the module
/// documents — written a second time on purpose.
fn expected_holder(holder: &str, address: &str) -> String {
    if holder.len() == 56
        && holder
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
    {
        return format!("stake:{holder}");
    }
    match Address::from_bech32(address) {
        Ok(Address::Shelley(shelley)) if shelley.payment().is_script() => {
            format!("script:{address}")
        }
        _ => format!("payment:{address}"),
    }
}

/// `(asset_name_hex, holder) → quantity`, and the number of distinct holders.
fn expected(table: &str) -> (BTreeMap<(String, String), u64>, usize) {
    let mut lines = table.lines();
    assert_eq!(lines.next(), Some(ROWS_HEADER), "the rows file's header");

    let mut holdings: BTreeMap<(String, String), u64> = BTreeMap::new();
    let mut holders = std::collections::BTreeSet::new();
    for (index, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let [holder, address, _cohort, asset_name, quantity] = fields.as_slice() else {
            panic!(
                "line {}: expected 5 columns, found {}",
                index + 2,
                fields.len()
            );
        };
        let quantity: u64 = quantity
            .parse()
            .unwrap_or_else(|_| panic!("line {}: quantity {quantity:?}", index + 2));
        let key = expected_holder(holder, address);
        holders.insert(key.clone());
        *holdings.entry(((*asset_name).to_owned(), key)).or_insert(0) += quantity;
    }
    (holdings, holders.len())
}

fn emitted_holder(holder: &HolderRef) -> String {
    match holder {
        HolderRef::Stake(credential) => format!("stake:{credential}"),
        HolderRef::Payment(address) => format!("payment:{address}"),
        HolderRef::Script { addr, .. } => format!("script:{addr}"),
    }
}

// ── The test ────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn holders_from_fold_serves_the_artifact_it_is_given() {
    let Some(dir) = artifact_dir() else {
        eprintln!("skipping: no fold artifact (set MITOS_FOLD_ARTIFACT or run `fold-probe pilot`)");
        return;
    };

    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    // The host resolves its artifact once, from the environment, the first time a
    // module asks. No other test in this binary reads `MITOS_FOLD_ARTIFACT`, and the
    // read happens after `start` below, so nothing races this write.
    //
    // SAFETY: `set_var` is unsafe because a concurrent `getenv` is UB on some
    // platforms. This is the only writer and the only reader in this process.
    unsafe {
        std::env::set_var("MITOS_FOLD_ARTIFACT", &dir);
    }

    // ── expected, from the artifact's own bytes ─────────────────────────────
    let rows = std::fs::read_to_string(dir.join("holders.tsv")).expect("holders.tsv");
    let (want, holders) = expected(&rows);
    assert!(
        !want.is_empty(),
        "{} holds no holdings; build a pilot with a policy that has some",
        dir.display()
    );
    println!(
        "artifact   {} · {} holdings · {holders} holder(s)",
        dir.display(),
        want.len(),
    );

    // ── the host, exactly as the bundle builds it ───────────────────────────
    let artifact = module_artifact_dir();
    let wasm_path = artifact.join(format!("{MODULE_ID}.wasm"));
    let wasm = std::fs::read(&wasm_path).unwrap_or_else(|error| {
        panic!(
            "{} could not be read ({error}) — build it with \
             `cargo run -q -p mitos-build -- --module \
             community-modules/holders-from-fold/holders_from_fold.rs`",
            wasm_path.display()
        )
    });
    let manifest = mitos_platform::manifest::Manifest::parse(
        &std::fs::read_to_string(artifact.join("manifest.toml")).expect("manifest.toml"),
    )
    .expect("parse manifest");
    assert_eq!(manifest.module.id, MODULE_ID);

    let storage_dir = tempdir("holders-from-fold");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");
    write_companion_subscription(
        &storage,
        MODULE_ID,
        CLIENT_ID,
        COMPANION_KEY,
        // The module emits on the policy lane; a companion that matches everything
        // receives the whole sequence.
        vec![WireInterest::any()],
    );

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let plane = Arc::new(NullChainDataPlane);
    let dp: Arc<dyn DataPlaneFacade> = plane.clone();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let rx_holder = Arc::new(Mutex::new(rx));
    let sub_factory: SubscriptionFactory<OneShotSub> = Arc::new(move |_resume| OneShotSub {
        rx: rx_holder.clone(),
    });

    let storage_for_kv = storage.clone();
    let kv_factory: KvFactory = Arc::new(move |key| match storage_for_kv.kv_store_at(key, None) {
        Ok(kv) => state_kv::ModuleKv::Redb(kv),
        Err(error) => panic!("kv_store_at: {error}"),
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

    // `rebootstrap = true` is the recapture shape: the host drives the module's
    // `rebootstrap` to `done` on a disposable instance. The emissions land in the sink
    // the drain task writes from, and that task is spawned alongside the follower — so
    // the store is read until the sequence closes rather than once `start` returns.
    let ingested = host.start(MODULE_ID, true).await.expect("start");
    println!("rebootstrap ingested {ingested} holding(s)");

    let store = storage.emissions_store(MODULE_ID).expect("emissions store");
    let deadline = Instant::now() + Duration::from_secs(30);
    let rows = loop {
        let rows = store
            .list_queued_for_companion(COMPANION_KEY, CLIENT_ID)
            .expect("list emissions");
        if rows.iter().any(|row| {
            matches!(
                ciborium::de::from_reader::<CollectionEvent, _>(row.payload.as_slice()),
                Ok(CollectionEvent::SnapshotEnd(_))
            )
        }) {
            break rows;
        }
        assert!(
            Instant::now() < deadline,
            "no SnapshotEnd within 30s — {} emission(s) queued for {CLIENT_ID}/{COMPANION_KEY}",
            rows.len(),
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    host.stop(MODULE_ID).await.expect("stop");
    drop(tx);

    let mut events: Vec<CollectionEvent> = Vec::with_capacity(rows.len());
    for row in &rows {
        match ciborium::de::from_reader::<CollectionEvent, _>(row.payload.as_slice()) {
            Ok(event) => events.push(event),
            Err(error) => panic!("emission {} is not a CollectionEvent: {error}", row.id),
        }
    }

    // ── assert ──────────────────────────────────────────────────────────────
    let manifest_policy = policy_hex(&dir);
    let mut begins = 0usize;
    let mut ends = 0usize;
    let mut chunks = 0usize;
    let mut holding_count = 0u64;
    let mut got: BTreeMap<(String, String), u64> = BTreeMap::new();

    for event in &events {
        match event {
            CollectionEvent::SnapshotBegin(begin) => {
                begins += 1;
                assert_eq!(
                    begin.policy, manifest_policy,
                    "snapshot is for another policy"
                );
                assert_eq!(
                    begin.cursor_slot,
                    base_slot(&dir),
                    "the snapshot must be stamped with the manifest's base slot",
                );
                assert_eq!(
                    begin.cursor_hash_hex,
                    base_hash(&dir),
                    "the snapshot must carry the manifest's base hash",
                );
            }
            CollectionEvent::SnapshotChunk(chunk) => {
                chunks += 1;
                assert_eq!(chunk.policy, manifest_policy, "chunk is for another policy");
                for holding in &chunk.holdings {
                    *got.entry((
                        holding.asset_name_hex.clone(),
                        emitted_holder(&holding.holder),
                    ))
                    .or_insert(0) += holding.quantity;
                }
            }
            CollectionEvent::SnapshotEnd(end) => {
                ends += 1;
                assert_eq!(end.policy, manifest_policy);
                holding_count = end.holding_count;
            }
            CollectionEvent::Delta(_) => panic!("a fold round must never emit a delta"),
        }
    }

    let carried: u64 = got.values().sum();
    println!(
        "emitted    {begins} begin · {chunks} chunk(s) · {ends} end · {carried} unit(s) over \
         {} holding(s)",
        got.len()
    );

    assert_eq!(begins, 1, "exactly one SnapshotBegin opens a snapshot");
    assert_eq!(ends, 1, "exactly one SnapshotEnd closes it");
    assert!(chunks >= 1, "the snapshot carried no chunks");
    assert_eq!(
        holding_count,
        got.len() as u64,
        "`holding_count` must equal the holdings the chunks carried",
    );
    assert_eq!(
        holding_count,
        want.len() as u64,
        "`holding_count` must equal the holdings the artifact holds",
    );

    if got != want {
        let only_artifact: Vec<_> = want.keys().filter(|key| !got.contains_key(*key)).collect();
        let only_module: Vec<_> = got.keys().filter(|key| !want.contains_key(*key)).collect();
        for (key, expected_quantity) in &want {
            if got.get(key) != Some(expected_quantity) {
                println!(
                    "  {} · {}  artifact {expected_quantity} · module {}",
                    key.0,
                    key.1,
                    got.get(key).map_or("absent".to_owned(), |q| q.to_string()),
                );
            }
        }
        panic!(
            "the snapshot does not match the artifact: {} holding(s) only in the artifact \
             (first: {:?}), {} only in the module (first: {:?})",
            only_artifact.len(),
            only_artifact.first(),
            only_module.len(),
            only_module.first(),
        );
    }

    println!(
        "  ✓ {} holdings · {holders} holder(s) · Σ {} unit(s)",
        got.len(),
        got.values().sum::<u64>(),
    );

    std::fs::remove_dir_all(&storage_dir).ok();
}

// ── The manifest fields the snapshot is stamped with ────────────────────────

/// The manifest, parsed here rather than through `mitos-fold`: three fields, and the
/// point of the assertion is that the module reports what the file says.
fn manifest_field(dir: &Path, field: &str) -> String {
    let manifest = std::fs::read_to_string(dir.join("artifact.meta")).expect("artifact.meta");
    for line in manifest.lines() {
        if let Some(value) = line.strip_prefix(&format!("{field} ")) {
            return value.to_owned();
        }
    }
    panic!("the manifest has no `{field}`");
}

fn policy_hex(dir: &Path) -> String {
    let manifest = std::fs::read_to_string(dir.join("artifact.meta")).expect("artifact.meta");
    for line in manifest.lines() {
        if let Some(value) = line.strip_prefix("extension 1 ") {
            return value.to_owned();
        }
    }
    panic!("the manifest carries no policy extension (tag 1), so no subject to check");
}

fn base_slot(dir: &Path) -> u64 {
    manifest_field(dir, "base_slot")
        .parse()
        .expect("base_slot is a number")
}

fn base_hash(dir: &Path) -> String {
    manifest_field(dir, "base_hash")
}
