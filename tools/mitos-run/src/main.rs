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
use mitos_data_plane::{DataPlaneError, DataPlaneResult, DecodeLevel, OutputRef, TypedOutput};
use mitos_platform::host_fns::DataPlaneFacade;
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

    /// Golden-test mode. After all `--block`s are dispatched,
    /// the JSON-decoded emissions (in emission order, one array
    /// element per `emit_event` call) are compared against the
    /// JSON array in this file. Mismatch → exit 1 with a diff
    /// report. Match → exit 0 (and we run quietly).
    ///
    /// Set `UPDATE_GOLDEN=1` in the env to overwrite the expected
    /// file with the actual emissions instead of asserting —
    /// useful when intentionally evolving a module's wire shape.
    /// Always re-review the diff before committing the new
    /// expected file.
    #[arg(long)]
    expected: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // Verbose tracing — surface every `logging::log` from the
    // module plus the platform's own info traces. Module logs
    // come through the `tracing` subscriber at whatever level
    // the module passed; platform-internal traces at info+.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,jpg_co_module=debug,mitos_platform=info")
            }),
        )
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

    // v2 is the only supported ABI; v1 was retired
    // (see MITOS_PLATFORM_V2.md). Mismatched manifests are a
    // build error rather than a silent fallback.
    if abi_major != 2 {
        bail!(
            "unsupported ABI v{abi_major} — mitos-run only handles v2 modules; \
             rebuild with `mitos-build` (v2 is the default)"
        );
    }
    run_v2(
        &args.block,
        &fixture,
        &module_id,
        &wasm_path,
        &config_bytes,
        args.expected.as_deref(),
    )
    .await
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
    AtPaymentCred { hash: String },
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
    /// Native assets at the output. Set when the synthesized
    /// UTxO needs to carry NFTs (e.g. listed assets at a
    /// marketplace script address) for module emit-paths that
    /// iterate `prior_output.assets`.
    #[serde(default)]
    assets: Vec<FixtureAsset>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureAsset {
    /// 56-hex policy id.
    policy: String,
    /// Lowercase hex asset name.
    asset_name_hex: String,
    #[serde(default = "default_asset_quantity")]
    quantity: u64,
}

fn default_asset_quantity() -> u64 {
    1
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
    /// 28-byte payment credential → refs at addresses sharing
    /// that payment cred (regardless of staking part). Populated
    /// from `[[utxo]]` entries by decoding each address's
    /// payment part.
    by_payment_cred: HashMap<[u8; 28], Vec<OutputRef>>,
    aux_by_tx: HashMap<Vec<u8>, Vec<u8>>,
    /// Hash → datum-CBOR map. Populated from block witness-data
    /// during harvest so modules calling
    /// `chain_data::datum_by_hash` (CIP-68 reference-output
    /// resolution being the canonical case) resolve correctly
    /// from the fixture.
    datums_by_hash: HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
struct ResolvedUtxo {
    output: TypedOutput,
}

impl FixtureDataPlane {
    fn from_fixture(fixture: Fixture) -> Result<Self> {
        let mut by_ref = HashMap::new();
        let mut by_address: HashMap<String, Vec<OutputRef>> = HashMap::new();
        let mut by_payment_cred: HashMap<[u8; 28], Vec<OutputRef>> = HashMap::new();
        let mut aux_by_tx = HashMap::new();

        for u in fixture.utxo {
            let tx_hash =
                decode_32(&u.tx_hash).with_context(|| format!("utxo tx_hash {}", u.tx_hash))?;
            let oref = OutputRef::from_bytes(tx_hash, u.index);

            // Project fixture datum fields onto the TypedOutput
            // so the data plane's `read_utxos` (and through it
            // the v2 dispatch composer) can see them. Hash bytes
            // come back as `pallas::Hash<32>`; payload becomes
            // `original_cbor` per the data-plane convention.
            let datum_hash_bytes = u
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
                .with_context(|| format!("utxo datum_payload_hex for {}#{}", u.tx_hash, u.index))?;
            let datum = datum_hash_bytes.map(|h| mitos_data_plane::TypedDatum {
                hash: pallas_primitives::Hash::new(h),
                payload: None,
                original_cbor: datum_payload,
            });

            let assets = u
                .assets
                .into_iter()
                .map(|a| {
                    let policy = cardano_assets::PolicyId::new(a.policy.clone())
                        .with_context(|| format!("utxo asset policy {}", a.policy))?;
                    Ok::<_, anyhow::Error>(mitos_data_plane::AssetEntry {
                        policy_id: policy,
                        asset_name_hex: a.asset_name_hex,
                        quantity: a.quantity,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            by_address.entry(u.address.clone()).or_default().push(oref);
            // Derive payment cred from the address so
            // `utxos_by_payment_cred` lookups resolve. Silently
            // skip non-Shelley shapes (Byron has no Shelley
            // payment part).
            if let Ok(addr) = pallas_addresses::Address::from_bech32(&u.address)
                && let pallas_addresses::Address::Shelley(s) = addr
            {
                let cred_bytes: [u8; 28] = match s.payment() {
                    pallas_addresses::ShelleyPaymentPart::Key(h) => **h,
                    pallas_addresses::ShelleyPaymentPart::Script(h) => **h,
                };
                by_payment_cred.entry(cred_bytes).or_default().push(oref);
            }
            by_ref.insert(
                (tx_hash.to_vec(), u.index),
                ResolvedUtxo {
                    output: TypedOutput {
                        address: u.address,
                        lovelace: u.lovelace,
                        assets,
                        datum,
                        script_ref: None,
                        original_cbor: None,
                        decoded_at: DecodeLevel::Lean,
                    },
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
            by_payment_cred,
            aux_by_tx,
            datums_by_hash: HashMap::new(),
        })
    }

    /// Walk every TX in the given block CBOR and harvest three
    /// kinds of resolution data from it into the fixture:
    /// - **aux-data CBOR** keyed by tx hash — so
    ///   `chain_data::tx_metadata` lookups against TXs in the
    ///   dispatched block resolve without operator hand-authoring
    ///   `[[tx_metadata]]` entries.
    /// - **witness datums** keyed by Plutus-data hash — so
    ///   `chain_data::datum_by_hash` resolves hash-only output
    ///   datums (the typical CIP-68 reference-output shape,
    ///   where the datum lives in the witness set rather than
    ///   inline on the output).
    /// - **UTxOs produced by each TX** keyed by output ref — so
    ///   the v2 dispatcher's `read_utxos` lookup resolves prior
    ///   outputs for Consumed/Referenced events in later blocks
    ///   (CIP-68 metadata updates spend a reference UTxO
    ///   produced by a prior block; without harvest the
    ///   Consumed event silently drops).
    ///
    /// Explicit fixture entries take precedence: if a key is
    /// already populated, harvest skips it.
    fn harvest_block_aux(&mut self, block_cbor: &[u8]) -> Result<usize> {
        use pallas::ledger::traverse::MultiEraBlock;
        let block = MultiEraBlock::decode(block_cbor)
            .map_err(|e| anyhow::anyhow!("MultiEraBlock decode: {e}"))?;
        let mut harvested = 0;
        for tx in block.txs() {
            let tx_hash_bytes = tx.hash().as_slice().to_vec();
            if !self.aux_by_tx.contains_key(&tx_hash_bytes)
                && let Some(aux) = mitos_data_plane::extract_aux_cbor(&tx)
            {
                self.aux_by_tx.insert(tx_hash_bytes, aux);
                harvested += 1;
            }
            // Each plutus_data witness on the TX carries
            // `(hash, raw_cbor_bytes)`. Index by hash. The
            // KeepRaw wrapper preserves the original CBOR
            // bytes; we use those (not pallas's re-encoded form)
            // so the hash matches what's referenced in outputs.
            for plutus_data in tx.plutus_data() {
                use pallas::ledger::traverse::OriginalHash;
                let hash_bytes = plutus_data.original_hash().to_vec();
                if let std::collections::hash_map::Entry::Vacant(e) =
                    self.datums_by_hash.entry(hash_bytes)
                {
                    e.insert(plutus_data.raw_cbor().to_vec());
                    harvested += 1;
                }
            }
            // Outputs: project each into a TypedOutput so the
            // dispatcher's prior-output resolution for
            // Consumed/Referenced events in subsequent blocks
            // finds them. Datum bytes (if hash-only on the
            // output) come via the witness-datum index above.
            let tx_hash_bytes_owned: [u8; 32] = *tx.hash();
            for (idx, output) in tx.outputs().iter().enumerate() {
                let key = (tx_hash_bytes_owned.to_vec(), idx as u32);
                if self.by_ref.contains_key(&key) {
                    continue;
                }
                let mut typed_output = mitos_data_plane::project_typed_output(output);
                // Populate the datum field — `project_typed_output`
                // leaves it `None` by convention; we want
                // hash-only or inline bytes so the dispatcher's
                // `backfill_prior_datum` step on Consumed events
                // can fill the witness payload.
                let (datum_hash, inline_bytes) =
                    mitos_data_plane::block_events::extract_datum_info(output);
                if let Some(hash) = datum_hash {
                    typed_output.datum = Some(mitos_data_plane::TypedDatum {
                        hash,
                        payload: None,
                        original_cbor: inline_bytes,
                    });
                }
                let oref = mitos_data_plane::OutputRef::from_bytes(tx_hash_bytes_owned, idx as u32);
                self.by_address
                    .entry(typed_output.address.clone())
                    .or_default()
                    .push(oref);
                if let Ok(addr) = pallas_addresses::Address::from_bech32(&typed_output.address)
                    && let pallas_addresses::Address::Shelley(s) = addr
                {
                    let cred_bytes: [u8; 28] = match s.payment() {
                        pallas_addresses::ShelleyPaymentPart::Key(h) => **h,
                        pallas_addresses::ShelleyPaymentPart::Script(h) => **h,
                    };
                    self.by_payment_cred
                        .entry(cred_bytes)
                        .or_default()
                        .push(oref);
                }
                self.by_ref.insert(
                    key,
                    ResolvedUtxo {
                        output: typed_output,
                    },
                );
                harvested += 1;
            }
        }
        Ok(harvested)
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

    async fn utxos_by_policy(&self, _policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        // Fixture replay doesn't index by policy; modules that
        // depend on this host-fn need to pre-stash refs via
        // explicit `[[utxo]]` entries the runner already
        // surfaces through `read_utxos`.
        Ok(Vec::new())
    }

    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        if cred.len() != 28 {
            return Ok(Vec::new());
        }
        let mut key = [0u8; 28];
        key.copy_from_slice(cred);
        Ok(self.by_payment_cred.get(&key).cloned().unwrap_or_default())
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(self.aux_by_tx.get(tx_hash.as_ref() as &[u8]).cloned())
    }

    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<mitos_data_plane::TypedDatum>> {
        // Resolve from witness datums harvested off `--block`
        // arguments. The TypedDatum we return mirrors what
        // `LocalDataPlane` produces from the dolos archive: the
        // raw CBOR sits in `original_cbor`, and the WIT→bindgen
        // conversion in `mitos-platform::event_bindings` plucks
        // it into `payload` for the wasm side.
        Ok(self
            .datums_by_hash
            .get(hash.as_slice())
            .cloned()
            .map(|bytes| mitos_data_plane::TypedDatum {
                hash: *hash,
                payload: None,
                original_cbor: Some(bytes),
            }))
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

    async fn holder_count(&self, _policy: &cardano_assets::PolicyId) -> DataPlaneResult<u64> {
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
    expected_path: Option<&std::path::Path>,
) -> Result<()> {
    use mitos_platform::driver_v2::{ApplyOutcomeV2, DriverV2};
    use mitos_platform::host_fns::{emit, state_kv};
    use mitos_platform::registry_v2::{ModuleRegistryV2, ResourceBudget};

    let interest = build_interest_set(&fixture.interest)?;
    println!("▸ interest:    {} predicate(s)", interest.predicates.len());

    // Build the fixture-backed data plane and pre-harvest aux-data
    // from every `--block` arg so the module's `tx_metadata`
    // lookups against TXs in the dispatched block resolve
    // automatically. Operators only need to hand-author
    // `[[tx_metadata]]` entries for TXs OUTSIDE the dispatched
    // blocks (rare).
    let mut dp_inner = FixtureDataPlane::from_fixture(fixture.clone())?;
    let mut total_harvested = 0usize;
    for path in blocks {
        let cbor = fs::read(path)
            .with_context(|| format!("read block for aux harvest {}", path.display()))?;
        total_harvested += dp_inner
            .harvest_block_aux(&cbor)
            .with_context(|| format!("harvest aux-data from {}", path.display()))?;
    }
    if total_harvested > 0 {
        println!(
            "▸ harvested aux-data from {} TX(es) across {} block(s)",
            total_harvested,
            blocks.len()
        );
    }
    let dp: Arc<FixtureDataPlane> = Arc::new(dp_inner);
    let dp_facade: Arc<dyn DataPlaneFacade> = dp.clone();
    let kv = state_kv::ModuleKv::new_in_memory();
    let (sink, mut events_rx) = emit::EventSink::new();

    let engine = ModuleRegistryV2::build_engine().map_err(|e| anyhow::anyhow!("v2 engine: {e}"))?;
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

    // Emissions collected across init + update_interest + each
    // dispatched block. Cold-start-only fixtures (no --block)
    // still produce content here via `update_interest`'s
    // cold-start path.
    let mut collected_emits: Vec<serde_json::Value> = Vec::new();

    let drain = |rx: &mut tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
                 collected: &mut Vec<serde_json::Value>,
                 phase: &str| {
        let mut count = 0;
        while let Ok(event) = rx.try_recv() {
            count += 1;
            let decoded =
                mitos_community_events::decode_emit(module_id, event.channel, &event.payload);
            match &decoded {
                Some(json) => {
                    println!(
                        "  [{phase}] emit channel={} payload={} bytes",
                        event.channel,
                        event.payload.len()
                    );
                    for line in json.lines() {
                        println!("    {line}");
                    }
                }
                None => println!(
                    "  [{phase}] emit channel={} payload={} bytes (no typed decoder)",
                    event.channel,
                    event.payload.len()
                ),
            }
            let value = decoded
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_else(|| {
                    serde_json::json!({
                        "channel": event.channel,
                        "payload_bytes": event.payload.len(),
                    })
                });
            collected.push(value);
        }
        count
    };

    let init_emitted = drain(&mut events_rx, &mut collected_emits, "init");
    if init_emitted > 0 {
        println!("▸ {init_emitted} event(s) emitted during init");
    }
    println!("✓ init() returned cleanly");

    // Push the fixture's interest set into the host state so the
    // block-dispatch path can filter against it.
    let mut driver = DriverV2::new(instance, budget);
    driver.set_interest(interest.clone());

    // Also call the module's `update_interest` export with a
    // Replace op carrying the CBOR-encoded predicate list — same
    // shape the production follower uses. Modules whose filter
    // logic needs to know its own interest set (e.g.
    // burn-address tracking watched bech32s for per-output
    // filtering) only work end-to-end if this call is wired.
    let mut items_cbor = Vec::with_capacity(64);
    ciborium::ser::into_writer(&interest.predicates, &mut items_cbor)
        .map_err(|e| anyhow::anyhow!("encode interest predicates as CBOR: {e}"))?;
    match driver
        .call_update_interest(
            mitos_platform::bindings_v2::InterestOp::Replace,
            &items_cbor,
        )
        .await
    {
        Ok(Ok(())) => {
            println!(
                "✓ update_interest(Replace, {} bytes) returned cleanly",
                items_cbor.len()
            );
        }
        Ok(Err(e)) => {
            eprintln!("✗ update_interest returned Err: {e}");
        }
        Err(e) => {
            eprintln!("✗ update_interest failed: {e:?}");
        }
    }

    // Drain cold-start emissions produced by `update_interest`'s
    // bootstrap path. Modules that subscribe-then-snapshot (e.g.
    // vesting-tracker) emit here without any block dispatch.
    let cold_start_emitted = drain(&mut events_rx, &mut collected_emits, "update_interest");
    if cold_start_emitted > 0 {
        println!("▸ {cold_start_emitted} event(s) emitted during update_interest");
    }

    if blocks.is_empty() {
        println!("▸ no --block flags; cold-start-only run");
    }

    for path in blocks {
        let cbor = fs::read(path).with_context(|| format!("read block {}", path.display()))?;
        println!("▸ apply_block: {} ({} bytes)", path.display(), cbor.len());
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
        let block_emitted = drain(&mut events_rx, &mut collected_emits, "block");
        if block_emitted > 0 {
            println!("  ▸ {block_emitted} event(s) emitted");
        }
    }

    if let Some(path) = expected_path {
        let actual = serde_json::Value::Array(collected_emits);
        let update = std::env::var("UPDATE_GOLDEN").is_ok();
        if update {
            let pretty = serde_json::to_string_pretty(&actual)
                .map_err(|e| anyhow::anyhow!("encode actual emits as JSON: {e}"))?;
            fs::write(path, format!("{pretty}\n"))
                .with_context(|| format!("write golden file {}", path.display()))?;
            println!(
                "▸ UPDATE_GOLDEN=1 — overwrote {} with {} emission(s)",
                path.display(),
                actual.as_array().map(Vec::len).unwrap_or(0)
            );
            return Ok(());
        }
        if !path.exists() {
            bail!(
                "golden file {} doesn't exist; set UPDATE_GOLDEN=1 to create it",
                path.display()
            );
        }
        let expected_str = fs::read_to_string(path)
            .with_context(|| format!("read golden file {}", path.display()))?;
        let expected: serde_json::Value = serde_json::from_str(&expected_str)
            .with_context(|| format!("parse golden file {} as JSON", path.display()))?;
        if expected == actual {
            println!(
                "✓ golden match ({} emission(s)) against {}",
                actual.as_array().map(Vec::len).unwrap_or(0),
                path.display()
            );
        } else {
            eprintln!("✗ golden mismatch against {}", path.display());
            eprintln!(
                "  re-run with UPDATE_GOLDEN=1 to accept the new shape (review the diff first)"
            );
            eprintln!("---- expected ----");
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&expected).unwrap_or_default()
            );
            eprintln!("---- actual ----");
            eprintln!(
                "{}",
                serde_json::to_string_pretty(&actual).unwrap_or_default()
            );
            std::process::exit(1);
        }
    }

    Ok(())
}

fn build_interest_set(preds: &[FixtureInterestPredicate]) -> Result<mitos_data_plane::InterestSet> {
    use cardano_assets::PolicyId;
    use mitos_data_plane::{InterestPredicate, InterestSet, StakeCred};

    let mut set = InterestSet::default();
    for p in preds {
        let pred = match p {
            FixtureInterestPredicate::AtAddress { address } => {
                InterestPredicate::AtAddress(address.clone())
            }
            FixtureInterestPredicate::AtPaymentCred { hash } => {
                let bytes = hex::decode(hash).context("at_payment_cred hex")?;
                if bytes.len() != 28 {
                    bail!("payment cred must be 28 bytes");
                }
                let mut arr = [0u8; 28];
                arr.copy_from_slice(&bytes);
                InterestPredicate::AtPaymentCred(arr)
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
