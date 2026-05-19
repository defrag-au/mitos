//! Bootstrap-by-policy integration test.
//!
//! Verifies the path: dynamic-interest `Add(HoldsPolicy(P))`
//! arrives at the host → follower hooks `bootstrap_one_predicate`
//! → `search_utxos(holds_policy(P))` pages the data plane →
//! synthetic `UtxoEvent::Produced` batches dispatch through the
//! driver → module's host filter passes the events (the predicate
//! was just added) → test-indexer emits.
//!
//! Critically: NO `TipEvent` is ever sent. Emissions can only come
//! from the bootstrap path. If the test sees > 0 emissions, the
//! whole bootstrap-by-policy chain is wired correctly.
//!
//! Skips cleanly when the test-indexer wasm fixture isn't built.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cardano_assets::PolicyId;
use mitos_data_plane::types::ProtocolParameters;
use mitos_data_plane::{
    AssetPattern, ChainDataPlane, ChainPoint, ChainTip, DataPlaneResult, DecodeLevel, OutputRef,
    Page, PageRequest, TypedDatum, TypedOutput, TypedScript, UtxoPattern, UtxoPredicate,
};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::host_v2::{
    EmitterFactory, InterestRouter, KvFactory, ModuleHostV2, SubscriptionFactory,
};
use mitos_platform::registry_v2::ResourceBudget;
use mitos_platform::storage::ModuleStorage;
use mitos_protocol::{AssetSelector, Interest as WireInterest, InterestOp as WireInterestOp};
use pallas_primitives::Hash;
use tokio::sync::Mutex;

use common::{OneShotSub, fixture_block_cbor, manifest_v2, tempdir, test_indexer_wasm};

/// `ChainDataPlane` impl that knows how to answer
/// `search_utxos(holds_policy(P))` for one specific policy. Every
/// other method is the null shape — same as `NullChainDataPlane`.
/// Backs the bootstrap path with real UTxOs without standing up
/// dolos.
struct FixturePolicyDataPlane {
    target_policy: PolicyId,
    /// Pre-decoded `(OutputRef, TypedOutput)` pairs the plane
    /// returns when the predicate matches the target policy.
    fixture_outputs: Vec<(OutputRef, TypedOutput)>,
}

#[async_trait]
impl ChainDataPlane for FixturePolicyDataPlane {
    async fn read_utxo(
        &self,
        _oref: &OutputRef,
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        Ok(None)
    }

    async fn read_utxos(
        &self,
        _orefs: &[OutputRef],
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        Ok(Vec::new())
    }

    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        _decode: DecodeLevel,
        _page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        let matches_target = match predicate {
            UtxoPredicate::Match(UtxoPattern {
                asset: Some(AssetPattern::Policy(p)),
                address: None,
                output_ref: None,
            }) => *p == self.target_policy,
            _ => false,
        };
        if matches_target {
            Ok(Page {
                items: self.fixture_outputs.clone(),
                next_token: None,
                tip: ChainTip::origin(),
            })
        } else {
            Ok(Page {
                items: Vec::new(),
                next_token: None,
                tip: ChainTip::origin(),
            })
        }
    }

    async fn utxos_by_address(&self, _address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(Vec::new())
    }

    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        // Surface the fixture outputs' refs when the target
        // policy is queried — mirrors what dolos's `BY_POLICY`
        // index would return in production.
        let target_bytes = hex::decode(self.target_policy.as_ref()).unwrap_or_default();
        if policy == target_bytes.as_slice() {
            Ok(self.fixture_outputs.iter().map(|(r, _)| *r).collect())
        } else {
            Ok(Vec::new())
        }
    }

    async fn utxos_by_payment_cred(&self, _cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(Vec::new())
    }

    async fn tx_metadata(&self, _tx_hash: &Hash<32>) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn read_datum(&self, _hash: &Hash<32>) -> DataPlaneResult<Option<TypedDatum>> {
        Ok(None)
    }

    async fn read_script(&self, _hash: &Hash<28>) -> DataPlaneResult<Option<TypedScript>> {
        Ok(None)
    }

    async fn total_supply(
        &self,
        _policy: &PolicyId,
        _asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn holder_count(&self, _policy: &PolicyId) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        Ok(ChainTip::origin())
    }

    async fn protocol_params(&self) -> DataPlaneResult<ProtocolParameters> {
        Ok(ProtocolParameters {
            coins_per_utxo_byte: 0,
            min_fee_a: 0,
            min_fee_b: 0,
            max_tx_size: 0,
            price_mem: 0.0,
            price_step: 0.0,
            max_tx_ex_mem: 0,
            max_tx_ex_steps: 0,
            epoch: 0,
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_interest_add_policy_triggers_bootstrap_emissions() {
    let Some(wasm_path) = test_indexer_wasm() else {
        eprintln!("skipping: test-indexer wasm not built");
        return;
    };
    let Some(cbor) = fixture_block_cbor() else {
        eprintln!("skipping: tests/fixtures/186000000.block.cbor missing");
        return;
    };

    // Pre-scan the block: pick the highest-frequency policy +
    // collect every (OutputRef, TypedOutput) pair that holds it.
    // The fixture data plane returns these pairs from
    // `search_utxos(holds_policy(P))` so the bootstrap path has
    // real synthesised events to dispatch.
    let decoded =
        mitos_data_plane::block_events::decode_block_v2(&cbor).expect("decode block fixture");
    let mut counts: HashMap<String, usize> = HashMap::new();
    for tx in &decoded.txs {
        for out in &tx.outputs {
            for asset in &out.output.assets {
                *counts.entry(asset.policy_id.to_string()).or_insert(0) += 1;
            }
        }
    }
    let (watched_policy_hex, _) = counts
        .into_iter()
        .max_by_key(|(_, n)| *n)
        .expect("block fixture has at least one asset");
    let watched_policy = PolicyId::new(watched_policy_hex.clone()).expect("valid policy id");

    let mut fixture_outputs: Vec<(OutputRef, TypedOutput)> = Vec::new();
    for tx in &decoded.txs {
        for (idx, out) in tx.outputs.iter().enumerate() {
            let holds_target = out
                .output
                .assets
                .iter()
                .any(|a| a.policy_id == watched_policy);
            if holds_target {
                fixture_outputs.push((OutputRef::new(tx.tx_hash, idx as u32), out.output.clone()));
            }
        }
    }
    assert!(
        !fixture_outputs.is_empty(),
        "block fixture should hold at least one output under the most-frequent policy",
    );

    // Manifest declares NO interest at start. The only path to
    // emissions is the dynamic-interest Add we'll fire below.
    let wasm = std::fs::read(&wasm_path).expect("read wasm");
    let manifest = manifest_v2(&wasm);
    assert!(
        manifest.interest.is_empty(),
        "manifest_v2 helper should produce empty interest by default",
    );

    let storage_dir = tempdir("bootstrap-by-policy-v2");
    let storage = ModuleStorage::new(&storage_dir);
    storage.activate(&manifest, &wasm).expect("activate");

    // Companion stub so the emissions store records rows for our
    // assertion. Same pattern dispatch_v2 / dynamic_interest_v2 use.
    let companions_dir = storage.module_dir_for_companions("test-indexer");
    std::fs::create_dir_all(&companions_dir).expect("companions dir");
    std::fs::write(companions_dir.join("test-companion.cbor"), b"").expect("companion stub");

    let engine = mitos_platform::registry_v2::ModuleRegistryV2::build_engine().expect("engine");
    let chain_plane = Arc::new(FixturePolicyDataPlane {
        target_policy: watched_policy.clone(),
        fixture_outputs,
    });
    let dp: Arc<dyn DataPlaneFacade> = chain_plane.clone();

    // Tip channel exists for the host's required wiring but we
    // never send a TipEvent — the only emissions in this test
    // come from the bootstrap-on-Add path.
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

    // Inject the dynamic-interest Add. The follower's interest
    // multiplexer should: (1) add HoldsPolicy(P) to the driver's
    // InterestSet, (2) call `bootstrap_one_predicate` which pages
    // `search_utxos(holds_policy(P))` and dispatches synthetic
    // batches, (3) forward the update to the module's
    // `update-interest` export.
    let mut wire_interest = WireInterest::any();
    wire_interest.asset = AssetSelector::Policy(watched_policy);
    host.route_interest("test-indexer", WireInterestOp::Add, vec![wire_interest])
        .await
        .expect("route_interest");

    // Bootstrap is a fire-and-forget hop on the follower's
    // interest channel. Wait long enough for the page walk +
    // dispatch_synthetic_batch + emission flush to complete.
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;

    host.stop("test-indexer").await.expect("stop");

    let emissions = storage
        .emissions_store("test-indexer")
        .expect("emissions store");
    let rows = emissions
        .list_queued_for_companion("test-companion")
        .expect("list emissions");
    assert!(
        !rows.is_empty(),
        "route_interest(Add, holds_policy) without any block dispatch should still produce \
         emissions via the bootstrap-by-policy path; got 0 — bootstrap-on-Add is broken",
    );

    drop(tx);
    std::fs::remove_dir_all(&storage_dir).ok();
}
