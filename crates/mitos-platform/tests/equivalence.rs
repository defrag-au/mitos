//! Observable-equivalence harness.
//!
//! Runs the wasm-module port of `OwnershipIndexer` (built under
//! `modules/ownership-indexer/`) and a pure-Rust **reference
//! emitter** mirroring the host-side `crates/collection-ownership-indexer/`
//! against the same input, then diffs the emitted events.
//!
//! Two test shapes:
//! 1. **Synthetic-`TxView` equivalence** — hand-built inputs
//!    cover the emit-shape parity matrix (empty assets, watched
//!    + unwatched policies, multi-output, multi-tx).
//! 2. **Mainnet fixture equivalence** — looks for `*.block.cbor`
//!    files under `tests/fixtures/`; if any are present, decodes
//!    each via `block_decode::decode_block` and runs the same
//!    diff. Auto-skips when no fixtures are checked in.
//!
//! V1 emission-shape note: the wasm module drops `role` and
//! `asset_fingerprint` from its events (the WIT doesn't expose
//! the host primitives needed to derive them). The reference
//! emitter mirrors that limitation so the diff is apples-to-
//! apples; a future ABI bump that surfaces those fields would
//! warrant updating both halves of the diff at once.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use mitos_data_plane::{DataPlaneResult, DecodeLevel, OutputRef, TypedOutput};
use mitos_platform::ResolvedBlock;
use mitos_platform::bindings::{
    AssetEntry as WitAssetEntry, AssetId as WitAssetId, TypedOutput as WitTypedOutput,
};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::registry::{ModuleRegistry, ResourceBudget};
use mitos_platform::resolved_block::TxView;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
enum OwnershipChangeV1Wire {
    Transfer {
        policy_id: String,
        asset_name: String,
        new_owner: String,
        tx_hash: String,
        output_index: u32,
    },
}

/// Pure-Rust reference emitter. Mirrors the host-side
/// `OwnershipIndexer::handle_event` logic; takes pre-projected
/// `TxView`s so the test bypasses pallas decode entirely (which
/// is exercised by the fixture-driven test).
fn reference_emit(watched: &HashSet<String>, txs: &[TxView]) -> Vec<OwnershipChangeV1Wire> {
    if watched.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for tx in txs {
        let tx_hash_hex = hex::encode(&tx.tx_hash);
        for (output_idx, output) in tx.outputs.iter().enumerate() {
            for asset in &output.assets {
                let policy_hex = hex::encode(&asset.asset.policy);
                if !watched.contains(&policy_hex) {
                    continue;
                }
                let asset_name_hex = hex::encode(&asset.asset.name);
                out.push(OwnershipChangeV1Wire::Transfer {
                    policy_id: policy_hex,
                    asset_name: asset_name_hex,
                    new_owner: output.address.clone(),
                    tx_hash: tx_hash_hex.clone(),
                    output_index: output_idx as u32,
                });
            }
        }
    }
    out
}

/// Stub data plane — equivalence tests don't exercise consumed
/// inputs (ownership is pure-output) so this never fires.
struct NullDataPlane;

#[async_trait]
impl DataPlaneFacade for NullDataPlane {
    async fn read_utxos(
        &self,
        _refs: &[OutputRef],
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        Ok(Vec::new())
    }
}

fn ownership_module_wasm() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join(
            "modules/ownership-indexer/target/wasm32-wasip2/release/\
             ownership_indexer_module.wasm",
        );
    candidate.exists().then_some(candidate)
}

/// Drive `txs` through the wasm module configured to watch
/// `watched`; collect emitted events.
async fn wasm_emit(
    wasm: &Path,
    watched: &HashSet<String>,
    txs: Vec<TxView>,
) -> Vec<OwnershipChangeV1Wire> {
    let engine = ModuleRegistry::build_engine().expect("engine");
    let registry =
        ModuleRegistry::load_from_path(engine, "ownership".to_owned(), wasm).expect("load");
    let dp: Arc<dyn DataPlaneFacade> = Arc::new(NullDataPlane);

    let (sink, mut events) = emit::EventSink::new();
    let kv = state_kv::ModuleKv::new_in_memory();

    let mut instance = registry
        .instantiate(dp, kv, sink, ResourceBudget::default())
        .await
        .expect("instantiate");

    #[derive(serde::Serialize)]
    struct Cfg<'a> {
        policies: Vec<&'a str>,
    }
    let mut cfg_bytes = Vec::new();
    ciborium::ser::into_writer(
        &Cfg {
            policies: watched.iter().map(String::as_str).collect(),
        },
        &mut cfg_bytes,
    )
    .unwrap();
    instance
        .bindings
        .call_init(&mut instance.store, &cfg_bytes)
        .await
        .expect("init");

    let block = ResolvedBlock::from_views(0, txs);
    let block_id = instance.store.data_mut().table.push(block).unwrap();
    instance
        .bindings
        .call_handle_event(&mut instance.store, 0, block_id)
        .await
        .expect("handle-event");

    let mut emitted = Vec::new();
    while let Ok(event) = events.try_recv() {
        let decoded: OwnershipChangeV1Wire =
            ciborium::de::from_reader(event.payload.as_slice()).expect("decode");
        emitted.push(decoded);
    }
    emitted
}

fn watched_set(policies: &[&str]) -> HashSet<String> {
    policies.iter().map(|s| (*s).to_string()).collect()
}

fn make_output(address: &str, assets: Vec<(Vec<u8>, Vec<u8>, u64)>) -> WitTypedOutput {
    WitTypedOutput {
        address: address.to_owned(),
        lovelace: 2_000_000,
        assets: assets
            .into_iter()
            .map(|(policy, name, qty)| WitAssetEntry {
                asset: WitAssetId { policy, name },
                quantity: qty,
            })
            .collect(),
    }
}

#[tokio::test]
async fn equivalence_empty_watch_emits_nothing() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };
    let watched = watched_set(&[]);
    let txs = vec![TxView {
        tx_hash: vec![0x01; 32],
        outputs: vec![make_output(
            "addr1abc",
            vec![(vec![0xAA; 28], b"X".to_vec(), 1)],
        )],
        consumed_input_refs: vec![],
    }];
    let actual = wasm_emit(&wasm, &watched, txs.clone()).await;
    let expected = reference_emit(&watched, &txs);
    assert_eq!(actual, expected, "empty-watch must emit nothing");
}

#[tokio::test]
async fn equivalence_single_watched_asset() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };
    let policy = vec![0xAA; 28];
    let policy_hex = hex::encode(&policy);
    let watched = watched_set(&[&policy_hex]);
    let txs = vec![TxView {
        tx_hash: vec![0x01; 32],
        outputs: vec![make_output(
            "addr1abc",
            vec![(policy.clone(), b"BlackFlag001".to_vec(), 1)],
        )],
        consumed_input_refs: vec![],
    }];
    let actual = wasm_emit(&wasm, &watched, txs.clone()).await;
    let expected = reference_emit(&watched, &txs);
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn equivalence_multi_output_multi_asset() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };
    let policy_a = vec![0xAA; 28];
    let policy_b = vec![0xBB; 28];
    let policy_a_hex = hex::encode(&policy_a);
    let watched = watched_set(&[&policy_a_hex]);

    // Two txs. tx0 has two outputs: one with two watched-policy
    // assets, one with one unwatched + one watched. tx1 has one
    // output with no assets.
    let txs = vec![
        TxView {
            tx_hash: vec![0xAA; 32],
            outputs: vec![
                make_output(
                    "addr1qxy",
                    vec![
                        (policy_a.clone(), b"asset-1".to_vec(), 1),
                        (policy_a.clone(), b"asset-2".to_vec(), 1),
                    ],
                ),
                make_output(
                    "addr1qzz",
                    vec![
                        (policy_b.clone(), b"unwatched".to_vec(), 5),
                        (policy_a.clone(), b"asset-3".to_vec(), 1),
                    ],
                ),
            ],
            consumed_input_refs: vec![],
        },
        TxView {
            tx_hash: vec![0xBB; 32],
            outputs: vec![make_output("addr1qpp", vec![])],
            consumed_input_refs: vec![],
        },
    ];

    let actual = wasm_emit(&wasm, &watched, txs.clone()).await;
    let expected = reference_emit(&watched, &txs);
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 3, "three watched-asset emissions");
}

#[tokio::test]
async fn equivalence_unwatched_policy_emits_nothing() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };
    let watched = watched_set(&["aa".repeat(28).as_str()]);
    let txs = vec![TxView {
        tx_hash: vec![0x01; 32],
        outputs: vec![make_output(
            "addr1abc",
            vec![(vec![0xBB; 28], b"X".to_vec(), 1)],
        )],
        consumed_input_refs: vec![],
    }];
    let actual = wasm_emit(&wasm, &watched, txs.clone()).await;
    let expected = reference_emit(&watched, &txs);
    assert_eq!(actual, expected);
    assert!(actual.is_empty());
}

/// Decode any `*.block.cbor` fixtures and run the same diff.
/// Auto-skips when no fixtures are present — see
/// `tests/fixtures/README.md` for how to add one. When a fixture
/// IS present it asserts bit-for-bit emission parity between the
/// wasm module and the reference emitter on real mainnet data.
#[tokio::test]
async fn mainnet_fixture_emission_equivalence() {
    let Some(wasm) = ownership_module_wasm() else {
        return;
    };
    let fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let entries = std::fs::read_dir(&fixtures_dir).expect("fixtures dir");
    let cbors: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.ends_with(".block.cbor"))
                .unwrap_or(false)
        })
        .collect();
    if cbors.is_empty() {
        eprintln!(
            "skipping: no *.block.cbor fixtures in {}. \
             See README.md in that dir to add one.",
            fixtures_dir.display()
        );
        return;
    }
    // Watch every policy that shows up in the fixture so every
    // emission gets compared. The reference emitter and the wasm
    // module both apply the same watch logic — if either drifts,
    // the diff fires.
    for cbor_path in cbors {
        eprintln!("equivalence check: {}", cbor_path.display());
        let cbor = std::fs::read(&cbor_path).expect("read fixture");
        let decoded = mitos_platform::block_decode::decode_block(&cbor).expect("decode");
        let mut watched = HashSet::new();
        for tx in &decoded.txs {
            for out in &tx.outputs {
                for asset in &out.assets {
                    watched.insert(hex::encode(&asset.asset.policy));
                }
            }
        }
        let actual = wasm_emit(&wasm, &watched, decoded.txs.clone()).await;
        let expected = reference_emit(&watched, &decoded.txs);
        assert_eq!(
            actual,
            expected,
            "wasm module diverged from reference for {}",
            cbor_path.display()
        );
    }
}
