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

    println!("▸ artifact:    {}", args.artifact.display());
    println!("  module id:   {module_id}");
    println!("  wasm:        {} bytes", fs::metadata(&wasm_path)?.len());
    println!("  config:      {} bytes", config_bytes.len());
    println!(
        "  fixture:     {} utxo(s), {} tx_metadata entry(ies)",
        fixture.utxo.len(),
        fixture.tx_metadata.len()
    );

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

#[derive(Debug, Default, Deserialize)]
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
}

fn default_version() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Deserialize)]
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

#[async_trait]
impl DataPlaneFacade for FixtureDataPlane {
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

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        Ok(self.by_address.get(address).cloned().unwrap_or_default())
    }

    async fn datum_by_hash(&self, _hash: &[u8; 32]) -> DataPlaneResult<Option<Vec<u8>>> {
        Err(DataPlaneError::NotYetImplemented(
            "datum_by_hash not surfaced via fixtures",
        ))
    }

    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<(Vec<u8>, Vec<u8>)>>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let key = (r.tx_hash.as_ref().to_vec(), r.index);
            let entry = self.by_ref.get(&key).and_then(|u| {
                let payload = u.datum_payload.as_ref()?.clone();
                let hash = u.datum_hash?;
                Some((hash.to_vec(), payload))
            });
            out.push(entry);
        }
        Ok(out)
    }

    async fn read_output_hashes(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<Vec<u8>>>> {
        let mut out = Vec::with_capacity(refs.len());
        for r in refs {
            let key = (r.tx_hash.as_ref().to_vec(), r.index);
            let entry = self
                .by_ref
                .get(&key)
                .and_then(|u| u.datum_hash.map(|h| h.to_vec()));
            out.push(entry);
        }
        Ok(out)
    }

    async fn tx_metadata(&self, tx_hash: &[u8; 32]) -> DataPlaneResult<Option<Vec<u8>>> {
        Ok(self.aux_by_tx.get(tx_hash.as_slice()).cloned())
    }
}

// -----------------------------------------------------------------------------
// Mini manifest parser — we only need module.id locally; the
// platform's own loader runs full sha/abi validation downstream.
// -----------------------------------------------------------------------------

#[derive(Deserialize)]
struct ManifestSummary {
    module: ManifestModule,
}

#[derive(Deserialize)]
struct ManifestModule {
    id: String,
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
