//! `mitos-run` — local module test runner.
//!
//! Loads a built `mitos-build` artifact and drives `init()`
//! against a fixture-driven data plane. Surfaces logs +
//! emissions + full trap backtraces with debug symbols intact —
//! the local debugging path that doesn't require the production
//! mitos host or a Dolos snapshot.
//!
//! Usage:
//!
//! ```bash
//! mitos-run --artifact path/to/target/mitos/<id> \
//!           --fixture path/to/fixture.toml
//! ```

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use clap::Parser;
use mitos_data_plane::{
    DataPlaneError, DataPlaneResult, DecodeLevel, OutputRef, TypedOutput,
};
use mitos_platform::host_fns::{DataPlaneFacade, emit, state_kv};
use mitos_platform::registry::{ModuleRegistry, ResourceBudget};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(version, about = "run a mitos module locally with fixture data")]
struct Args {
    /// Path to a `mitos-build` artifact directory (contains
    /// `<id>.wasm`, `manifest.toml`, `config.cbor`).
    #[arg(long)]
    artifact: PathBuf,

    /// Path to a fixture TOML file with canned data-plane
    /// responses. Optional — without it the data plane returns
    /// nothing for every query (useful for testing modules that
    /// don't need bootstrap data).
    #[arg(long)]
    fixture: Option<PathBuf>,

    /// Override module id (defaults to `manifest.module.id`).
    #[arg(long)]
    module_id: Option<String>,

    /// Path to a Cardano block CBOR file. v2 modules only — the
    /// platform decodes the block, builds per-TX event batches
    /// against the module's interest set (loaded from the
    /// fixture's `[interest]` section), and dispatches each
    /// batch through `handle-events`. Repeatable for multi-block
    /// fixtures; specify in chain order.
    #[arg(long)]
    block: Vec<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Verbose tracing — surface every `logging::log` from the
    // module plus the platform's own info traces. Module logs
    // come through the `tracing` subscriber at whatever level
    // the module passed; platform-internal traces at info+.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
            |_| tracing_subscriber::EnvFilter::new("info,jpg_co_module=debug,mitos_platform=info"),
        ))
        .with_target(true)
        .init();

    let args = Args::parse();

    let manifest_path = args.artifact.join("manifest.toml");
    let manifest_str = fs::read_to_string(&manifest_path)
        .with_context(|| format!("read manifest at {}", manifest_path.display()))?;
    let manifest: ManifestSummary = toml::from_str(&manifest_str)
        .with_context(|| format!("parse manifest at {}", manifest_path.display()))?;

    let module_id = args.module_id.unwrap_or(manifest.module.id.clone());
    let wasm_path = args.artifact.join(format!("{module_id}.wasm"));
    let config_path = args.artifact.join("config.cbor");

    if !wasm_path.exists() {
        bail!("wasm not found at {}", wasm_path.display());
    }

    let config_bytes = if config_path.exists() {
        fs::read(&config_path)
            .with_context(|| format!("read config at {}", config_path.display()))?
    } else {
        Vec::new()
    };

    let fixture = match &args.fixture {
        Some(path) => {
            let s = fs::read_to_string(path)
                .with_context(|| format!("read fixture at {}", path.display()))?;
            toml::from_str::<Fixture>(&s)
                .with_context(|| format!("parse fixture at {}", path.display()))?
        }
        None => Fixture::default(),
    };

    let abi_major = manifest.abi.version_major;
    println!("▸ artifact:    {}", args.artifact.display());
    println!("  module id:   {module_id}");
    println!("  abi:         v{abi_major}");
    println!("  wasm:        {} bytes", fs::metadata(&wasm_path)?.len());
    println!("  config:      {} bytes", config_bytes.len());
    println!(
        "  fixture:     {} utxo(s), {} tx_metadata entry(ies), {} interest pred(s)",
        fixture.utxo.len(),
        fixture.tx_metadata.len(),
        fixture.interest.len(),
    );
    if !args.block.is_empty() {
        println!("  blocks:      {}", args.block.len());
    }

    // Branch on ABI version. v2 modules go through
    // ModuleRegistryV2 + DriverV2 with optional block dispatch
    // after init; v1 modules keep the existing init-only test
    // shape.
    if abi_major == 2 {
        return run_v2(&args.block, &fixture, &module_id, &wasm_path, &config_bytes).await;
    }

    let dp: Arc<dyn DataPlaneFacade> = Arc::new(FixtureDataPlane::from_fixture(fixture)?);
    let kv = state_kv::ModuleKv::new_in_memory();
    let (sink, mut events_rx) = emit::EventSink::new();

    let engine = ModuleRegistry::build_engine().map_err(|e| anyhow::anyhow!("build engine: {e}"))?;
    let registry = ModuleRegistry::load_from_path(engine, module_id.clone(), &wasm_path)
        .map_err(|e| anyhow::anyhow!("load module: {e}"))?;

    let budget = ResourceBudget::default();
    let mut instance = registry
        .instantiate(dp, kv, sink, budget)
        .await
        .map_err(|e| anyhow::anyhow!("instantiate: {e}"))?;

    // Mirror `ModuleHost::start`: refuel with the init-class
    // budget before `call_init`. Bootstrap-style modules can
    // burn an order of magnitude more than `fuel_per_call`
    // would allow.
    instance
        .store
        .set_fuel(budget.init_fuel)
        .map_err(|e| anyhow::anyhow!("set init fuel: {e}"))?;
    println!(
        "▸ init(): calling with config_bytes.len()={}, fuel={}",
        config_bytes.len(),
        budget.init_fuel
    );
    let init_result = instance
        .bindings
        .call_init(&mut instance.store, &config_bytes)
        .await;

    // Drain any emissions produced during init (the module may
    // emit during bootstrap).
    let mut emitted = 0;
    while let Ok(event) = events_rx.try_recv() {
        emitted += 1;
        println!(
            "  emit channel={} payload={} bytes",
            event.channel,
            event.payload.len()
        );
    }
    if emitted > 0 {
        println!("▸ {emitted} event(s) emitted during init");
    }

    match init_result {
        Ok(()) => {
            println!("✓ init() returned cleanly");
            Ok(())
        }
        Err(e) => {
            // wasmtime::Error's Debug formatter includes the
            // trap backtrace; with the line-tables-only debug
            // info baked in by mitos-build's release profile,
            // each frame names its source function.
            eprintln!("✗ init() failed:\n{e:?}");
            std::process::exit(1);
        }
    }
}

// -----------------------------------------------------------------------------
// Fixture schema (TOML)
// -----------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Deserialize)]
struct Fixture {
    /// Schema version. Bump on breaking changes; we don't read
    /// it today but parsing it forward-proofs the file.
    #[serde(default = "default_version")]
    #[allow(dead_code)]
    version: u32,

    /// UTxOs the data plane will surface. Indexed by address for
    /// `utxos_by_address`, by ref for `read_utxos` /
    /// `read_output_datums` / `read_output_hashes`.
    #[serde(default)]
    utxo: Vec<FixtureUtxo>,

    /// Auxiliary-data CBOR keyed by tx_hash. Hex-encoded.
    #[serde(default)]
    tx_metadata: Vec<FixtureTxMetadata>,

    /// v2-only: the module's interest set. Applied before any
    /// `handle-events` dispatch so the platform can filter
    /// matching events. Ignored for v1 modules.
    #[serde(default)]
    interest: Vec<FixtureInterestPredicate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum FixtureInterestPredicate {
    AtAddress { address: String },
    AtStakeKeyHash { hash: String },
    AtStakeScriptHash { hash: String },
    HoldsPolicy { policy: String },
    HoldsAsset { policy: String, asset_name: String },
    TickEvery { seconds: u32 },
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureUtxo {
    /// 64-hex tx_hash.
    tx_hash: String,
    /// 0-based output index.
    index: u32,
    /// bech32 address (`addr1...`).
    address: String,
    lovelace: u64,
    /// 64-hex datum hash. `None` when the output has no datum.
    #[serde(default)]
    datum_hash: Option<String>,
    /// Hex-encoded resolved datum payload bytes. Set when the
    /// host can resolve (inline or witness-set); leave absent to
    /// simulate "host couldn't resolve, fall back to metadata".
    #[serde(default)]
    datum_payload_hex: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureTxMetadata {
    /// 64-hex tx_hash.
    tx_hash: String,
    /// Hex-encoded auxiliary-data CBOR.
    aux_cbor_hex: String,
}

// -----------------------------------------------------------------------------
// Data plane backed by the fixture
// -----------------------------------------------------------------------------

struct FixtureDataPlane {
    by_ref: HashMap<(Vec<u8>, u32), ResolvedUtxo>,
    by_address: HashMap<String, Vec<OutputRef>>,
    aux_by_tx: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
struct ResolvedUtxo {
    output: TypedOutput,
    datum_hash: Option<[u8; 32]>,
    datum_payload: Option<Vec<u8>>,
}

impl FixtureDataPlane {
    fn from_fixture(fixture: Fixture) -> Result<Self> {
        let mut by_ref = HashMap::new();
        let mut by_address: HashMap<String, Vec<OutputRef>> = HashMap::new();
        let mut aux_by_tx = HashMap::new();

        for u in fixture.utxo {
            let tx_hash = decode_32(&u.tx_hash)
                .with_context(|| format!("utxo tx_hash {}", u.tx_hash))?;
            let oref = OutputRef::from_bytes(tx_hash, u.index);

            let datum_hash = u
                .datum_hash
                .as_deref()
                .map(decode_32)
                .transpose()
                .with_context(|| format!("utxo datum_hash for {}#{}", u.tx_hash, u.index))?;
            let datum_payload = u
                .datum_payload_hex
                .as_deref()
                .map(hex::decode)
                .transpose()
                .with_context(|| {
                    format!("utxo datum_payload_hex for {}#{}", u.tx_hash, u.index)
                })?;

            by_address
                .entry(u.address.clone())
                .or_default()
                .push(oref);
            by_ref.insert(
                (tx_hash.to_vec(), u.index),
                ResolvedUtxo {
                    output: TypedOutput {
                        address: u.address,
                        lovelace: u.lovelace,
                        assets: Vec::new(),
                        datum: None,
                        script_ref: None,
                        original_cbor: None,
                        decoded_at: DecodeLevel::Lean,
                    },
                    datum_hash,
                    datum_payload,
                },
            );
        }

        for tm in fixture.tx_metadata {
            let tx_hash = decode_32(&tm.tx_hash)
                .with_context(|| format!("tx_metadata tx_hash {}", tm.tx_hash))?;
            let bytes = hex::decode(&tm.aux_cbor_hex)
                .with_context(|| format!("tx_metadata aux_cbor_hex for {}", tm.tx_hash))?;
            aux_by_tx.insert(tx_hash.to_vec(), bytes);
        }

        Ok(Self {
            by_ref,
            by_address,
            aux_by_tx,
        })
    }
}

// `FixtureDataPlane` impls `ChainDataPlane` directly; the
// `DataPlaneFacade` impl comes for free via the blanket impl
// in `mitos-platform`. This lets the same fixture serve both
// v1 (DataPlaneFacade-shaped) and v2 (ChainDataPlane-shaped)
// dispatch paths.
#[async_trait]
impl mitos_data_plane::ChainDataPlane for FixtureDataPlane {
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        let key = (oref.tx_hash.as_ref().to_vec(), oref.index);
        Ok(self.by_ref.get(&key).map(|u| u.output.clone()))
    }

    async fn read_utxos(
        &self,
        refs: &[OutputRef],
        _decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let key = (r.tx_hash.as_ref().to_vec(), r.index);
            if let Some(u) = self.by_ref.get(&key) {
                out.push((*r, u.output.clone()));
            }
        }
        Ok(out)
    }

    async fn search_utxos(
        &self,
        _predicate: &mitos_data_plane::UtxoPredicate,
        _decode: DecodeLevel,
        _page: mitos_data_plane::PageRequest,
    ) -> DataPlaneResult<mitos_data_plane::Page<(OutputRef, TypedOutput)>> {
        Ok(mitos_data_plane::Page {
            items: Vec::new(),
            next_token: None,
            tip: mitos_data_plane::ChainTip::origin(),
        })
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(self.by_address.get(address).cloned().unwrap_or_default())
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(self.aux_by_tx.get(tx_hash.as_ref() as &[u8]).cloned())
    }

    async fn read_datum(
        &self,
        _hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<mitos_data_plane::TypedDatum>> {
        Ok(None)
    }

    async fn read_script(
        &self,
        _hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<mitos_data_plane::TypedScript>> {
        Ok(None)
    }

    async fn total_supply(
        &self,
        _policy: &cardano_assets::PolicyId,
        _asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn holder_count(
        &self,
        _policy: &cardano_assets::PolicyId,
    ) -> DataPlaneResult<u64> {
        Ok(0)
    }

    async fn tip(&self) -> DataPlaneResult<mitos_data_plane::ChainTip> {
        Ok(mitos_data_plane::ChainTip::origin())
    }

    async fn protocol_params(
        &self,
    ) -> DataPlaneResult<mitos_data_plane::types::ProtocolParameters> {
        Err(DataPlaneError::NotYetImplemented(
            "fixture has no protocol params",
        ))
    }
}

// -----------------------------------------------------------------------------
// Mini manifest parser — we only need module.id locally; the
// platform's own loader runs full sha/abi validation downstream.
// -----------------------------------------------------------------------------

#[derive(Deserialize)]
struct ManifestSummary {
    module: ManifestModule,
    #[serde(default)]
    abi: ManifestAbi,
}

#[derive(Deserialize)]
struct ManifestModule {
    id: String,
}

#[derive(Default, Deserialize)]
struct ManifestAbi {
    #[serde(default = "default_abi_major")]
    version_major: u32,
}

fn default_abi_major() -> u32 {
    1
}

// ============================================================
// v2 dispatch path
// ============================================================
//
// Boots a v2 module against the same fixture shape v1 uses, plus
// optional block CBORs via `--block`. After init, the fixture's
// `[interest]` predicates are pushed into the host state, then
// each block is decoded + filtered + dispatched per `handle-events`
// in chain order.

async fn run_v2(
    blocks: &[PathBuf],
    fixture: &Fixture,
    module_id: &str,
    wasm_path: &std::path::Path,
    config_bytes: &[u8],
) -> Result<()> {
    use mitos_platform::driver_v2::{ApplyOutcomeV2, DriverV2};
    use mitos_platform::registry_v2::ModuleRegistryV2;

    let interest = build_interest_set(&fixture.interest)?;
    println!("▸ interest:    {} predicate(s)", interest.predicates.len());

    let dp: Arc<FixtureDataPlane> = Arc::new(FixtureDataPlane::from_fixture(fixture.clone())?);
    let dp_facade: Arc<dyn DataPlaneFacade> = dp.clone();
    let kv = state_kv::ModuleKv::new_in_memory();
    let (sink, mut events_rx) = emit::EventSink::new();

    let engine =
        ModuleRegistryV2::build_engine().map_err(|e| anyhow::anyhow!("v2 engine: {e}"))?;
    let registry = ModuleRegistryV2::load_from_path(engine, module_id.to_owned(), wasm_path)
        .map_err(|e| anyhow::anyhow!("v2 load: {e}"))?;

    let budget = ResourceBudget::default();
    let mut instance = registry
        .instantiate(dp_facade, kv, sink, budget)
        .await
        .map_err(|e| anyhow::anyhow!("v2 instantiate: {e}"))?;

    // Refuel before init with the larger init budget, mirroring
    // ModuleHostV2's eventual behaviour. v2 init should be light
    // (no bootstrap work) but the budget gives headroom for any
    // one-shot setup the module wants to do.
    instance
        .store
        .set_fuel(budget.init_fuel)
        .map_err(|e| anyhow::anyhow!("set init fuel: {e}"))?;
    println!(
        "▸ init(): calling with config_bytes.len()={}, fuel={}",
        config_bytes.len(),
        budget.init_fuel
    );
    if let Err(e) = instance
        .bindings
        .call_init(&mut instance.store, config_bytes)
        .await
    {
        eprintln!("✗ init() failed:\n{e:?}");
        std::process::exit(1);
    }

    // Drain any emissions produced during init.
    let mut emitted = 0;
    while let Ok(event) = events_rx.try_recv() {
        emitted += 1;
        println!(
            "  emit channel={} payload={} bytes",
            event.channel,
            event.payload.len()
        );
    }
    if emitted > 0 {
        println!("▸ {emitted} event(s) emitted during init");
    }
    println!("✓ init() returned cleanly");

    // Push the fixture's interest set into the host state so the
    // block-dispatch path can filter against it.
    let mut driver = DriverV2::new(instance, budget);
    driver.set_interest(interest);

    if blocks.is_empty() {
        println!("▸ no --block flags; skipping handle-events dispatch");
        return Ok(());
    }

    for path in blocks {
        let cbor = fs::read(path)
            .with_context(|| format!("read block {}", path.display()))?;
        println!(
            "▸ apply_block: {} ({} bytes)",
            path.display(),
            cbor.len()
        );
        match driver.apply_block(&cbor, dp.as_ref()).await {
            Ok(ApplyOutcomeV2::Applied) => {
                println!("  ✓ Applied (events dispatched)");
            }
            Ok(ApplyOutcomeV2::AppliedEmpty) => {
                println!("  ✓ AppliedEmpty (no matching events; cursor advanced)");
            }
            Err(e) => {
                eprintln!("  ✗ apply_block failed:\n{e:?}");
                std::process::exit(1);
            }
        }
        // Drain emissions per block so they're attributed clearly.
        let mut block_emitted = 0;
        while let Ok(event) = events_rx.try_recv() {
            block_emitted += 1;
            println!(
                "  emit channel={} payload={} bytes",
                event.channel,
                event.payload.len()
            );
        }
        if block_emitted > 0 {
            println!("  ▸ {block_emitted} event(s) emitted");
        }
    }

    Ok(())
}

fn build_interest_set(
    preds: &[FixtureInterestPredicate],
) -> Result<mitos_data_plane::InterestSet> {
    use cardano_assets::PolicyId;
    use mitos_data_plane::{InterestPredicate, InterestSet, StakeCred};

    let mut set = InterestSet::default();
    for p in preds {
        let pred = match p {
            FixtureInterestPredicate::AtAddress { address } => {
                InterestPredicate::AtAddress(address.clone())
            }
            FixtureInterestPredicate::AtStakeKeyHash { hash } => {
                let bytes = hex::decode(hash).context("at_stake_key_hash hex")?;
                if bytes.len() != 28 {
                    bail!("stake key hash must be 28 bytes");
                }
                let mut arr = [0u8; 28];
                arr.copy_from_slice(&bytes);
                InterestPredicate::AtStakeCred(StakeCred::KeyHash(arr))
            }
            FixtureInterestPredicate::AtStakeScriptHash { hash } => {
                let bytes = hex::decode(hash).context("at_stake_script_hash hex")?;
                if bytes.len() != 28 {
                    bail!("stake script hash must be 28 bytes");
                }
                let mut arr = [0u8; 28];
                arr.copy_from_slice(&bytes);
                InterestPredicate::AtStakeCred(StakeCred::ScriptHash(arr))
            }
            FixtureInterestPredicate::HoldsPolicy { policy } => {
                let p = PolicyId::new(policy.clone()).context("holds_policy hex")?;
                InterestPredicate::HoldsPolicy(p)
            }
            FixtureInterestPredicate::HoldsAsset { policy, asset_name } => {
                let p = PolicyId::new(policy.clone()).context("holds_asset.policy hex")?;
                let name = hex::decode(asset_name).context("holds_asset.asset_name hex")?;
                InterestPredicate::HoldsAsset {
                    policy: p,
                    asset_name: name,
                }
            }
            FixtureInterestPredicate::TickEvery { seconds } => {
                InterestPredicate::TickEvery(*seconds)
            }
        };
        set.add(pred);
    }
    Ok(set)
}

fn decode_32(s: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(s).context("hex decode")?;
    if bytes.len() != 32 {
        bail!("expected 32-byte hex, got {} bytes from {s}", bytes.len());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}
