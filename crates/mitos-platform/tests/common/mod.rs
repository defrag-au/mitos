//! Shared helpers for the v2 platform integration tests.
//!
//! - `NullChainDataPlane` — implements every `ChainDataPlane`
//!   method as a no-op (empty Vec / None / synthetic ChainTip).
//!   Tests that don't drive the data plane wire this in to
//!   satisfy the host's required parameter without setting up
//!   a real archive.
//! - `OneShotSub` — synthetic `TipSubscription` backed by an
//!   `mpsc::UnboundedReceiver`. Tests pump `TipEvent::Apply`
//!   frames in by sending on the matching sender.
//! - `module_artifact_dir()` — locate the built test-indexer
//!   wasm artifact, returning `None` so tests can skip cleanly
//!   when the fixture isn't built.

#![allow(dead_code)] // not all helpers are used by every test

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use dolos_core::{TipEvent, TipSubscription};
use mitos_data_plane::types::ProtocolParameters;
use mitos_data_plane::{
    AssetEntry, ChainDataPlane, ChainTip, DataPlaneResult, DecodeLevel, InterestSet, OutputRef,
    Page, PageRequest, TxRecord, TypedDatum, TypedOutput, TypedScript, UtxoPredicate,
};
use mitos_platform::manifest::{
    AbiSection, BuildSection, InterestSection, Manifest, ModuleSection, TrapPolicySection,
    sha256_hex,
};
use pallas_primitives::Hash;
use tokio::sync::Mutex;

// ----- ChainDataPlane stub -------------------------------------------------

/// Null `ChainDataPlane` impl — every method returns the
/// empty / `None` shape. Suitable for tests where the module
/// doesn't call `chain-data` host fns and the manifest declares
/// no `[interest]` addresses (so bootstrap is also a no-op).
pub struct NullChainDataPlane;

#[async_trait]
impl ChainDataPlane for NullChainDataPlane {
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
        _predicate: &UtxoPredicate,
        _decode: DecodeLevel,
        _page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        Ok(Page {
            items: Vec::new(),
            next_token: None,
            tip: ChainTip::origin(),
        })
    }

    async fn utxos_by_address(&self, _address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(Vec::new())
    }

    async fn utxos_by_policy(&self, _policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(Vec::new())
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
        _policy: &cardano_assets::PolicyId,
        _asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn holder_count(&self, _policy: &cardano_assets::PolicyId) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        Ok(ChainTip::origin())
    }

    async fn protocol_params(&self) -> DataPlaneResult<ProtocolParameters> {
        // Tests don't drive the params path; placeholder values
        // keep the trait satisfied without dragging in any real
        // protocol numbers.
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

// ----- TipSubscription stub ------------------------------------------------

/// `TipSubscription` impl backed by an `Arc<Mutex<UnboundedReceiver>>`.
/// Each call to a `SubscriptionFactory` gets a fresh receiver wired to
/// the same shared channel — tests push `TipEvent::Apply` frames via
/// the sender and the follower pumps them through the dispatch path.
pub struct OneShotSub {
    pub rx: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<TipEvent>>>,
}

impl TipSubscription for OneShotSub {
    async fn next_tip(&mut self) -> TipEvent {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(event) => event,
            // Channel closed — block forever rather than returning
            // a sentinel. The follower task is meant to run until
            // the host's cancellation token fires.
            None => std::future::pending().await,
        }
    }
}

// ----- Fixture artifact resolution -----------------------------------------

/// Locate the built `test-indexer` wasm artifact.
///
/// Build with `nix develop /path/to/cnft.dev-workers -c cargo run
/// --release -p mitos-build -- --module modules/test_indexer.rs`
/// from this repo root. Returns `None` so tests can skip cleanly
/// when the artifact isn't built (CI is opt-in).
pub fn test_indexer_wasm() -> Option<PathBuf> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = manifest
        .parent()? // crates/
        .parent()? // mitos/
        .join("modules/target/mitos/test-indexer/test-indexer.wasm");
    candidate.exists().then_some(candidate)
}

/// Read the block CBOR fixture used by `dispatch_v2`. Same fixture
/// the v1 equivalence test used; we keep it because the platform's
/// dispatch composer still produces typed events from the same
/// block bytes.
pub fn fixture_block_cbor() -> Option<Vec<u8>> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest.join("tests/fixtures/186000000.block.cbor");
    path.exists().then(|| std::fs::read(path).ok()).flatten()
}

/// Build a v2 `Manifest` for a wasm whose bytes are passed in.
/// Tests that need bespoke interest predicates clone + tweak the
/// `interest` field after.
pub fn manifest_v2(wasm: &[u8]) -> Manifest {
    Manifest {
        module: ModuleSection {
            id: "test-indexer".to_owned(),
            sha256: sha256_hex(wasm),
            size_bytes: wasm.len() as u64,
        },
        abi: AbiSection {
            version_major: 2,
            version_minor: 0,
            wit_package: "mitos:platform-v2".to_owned(),
            wit_world: "mitos-module-v2".to_owned(),
        },
        trap_policy: TrapPolicySection {
            strategy: "replay".to_owned(),
            max_retries: 3,
            backoff_cap_ms: 1_000,
        },
        build: BuildSection {
            rust_version: "1.95.0".to_owned(),
            target: "wasm32-wasip2".to_owned(),
            profile: "release".to_owned(),
            build_id: "test-fixture".to_owned(),
            git_sha: None,
            crate_version: "0.0.0".to_owned(),
        },
        interest: InterestSection::default(),
    }
}

// ----- Misc ----------------------------------------------------------------

/// Per-test storage dir under the OS tempdir — keyed on test name +
/// PID so concurrent tests don't collide. Caller cleans up via
/// `std::fs::remove_dir_all` at end.
pub fn tempdir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "mitos-platform-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).expect("create tempdir");
    p
}

/// Marker type to silence the rustc warning for unused `Arc` /
/// types pulled in by re-exports the test module references but
/// the file itself doesn't directly use.
#[allow(dead_code)]
pub type _Touch = (Arc<NullChainDataPlane>, InterestSet, AssetEntry, TxRecord);
