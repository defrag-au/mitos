//! Trap-time fixture capture.
//!
//! `TrapContextLogger` wraps a `DataPlaneFacade` and records the
//! inputs + outputs of every host-fn call a module makes during
//! `init()` or `handle_event()`. When the wasm guest traps,
//! `ModuleHost` snapshots the log + serialises a fixture TOML
//! to `<modules-dir>/<id>/last-trap.toml`. Operators pull that
//! via `/_admin/modules/<id>/last-trap` and replay locally with
//! `tools/mitos-run` — same data the guest saw, full debug
//! symbols, no need for a Dolos snapshot.
//!
//! Fixture shape mirrors the TOML format `tools/mitos-run` reads
//! (`[[utxo]]` + `[[tx_metadata]]`). The logger captures call-by-
//! call, then denormalises into the per-output / per-tx records
//! `mitos-run`'s `FixtureDataPlane` looks up.

use std::sync::Arc;
use std::sync::Mutex;

use crate::host_fns::DataPlaneFacade;
use mitos_data_plane::{
    DataPlaneError, DataPlaneResult, DecodeLevel, OutputRef, StakeCred, TypedOutput,
};

/// Snapshot of one module's data-plane traffic during the most
/// recent dispatch attempt. Reset by `clear()` between calls so
/// each `init` / `handle_event` gets a fresh slate; only the
/// log of the *trapping* call survives, which is exactly what
/// the operator needs to repro.
#[derive(Default, Debug, Clone)]
pub struct TrapContextLog {
    /// `utxos_by_address(addr) -> refs`.
    pub utxos_by_address: Vec<UtxosByAddressCall>,
    /// `utxos_by_policy(policy) -> refs`.
    pub utxos_by_policy: Vec<UtxosByPolicyCall>,
    /// `utxos_by_payment_cred(cred) -> refs`.
    pub utxos_by_payment_cred: Vec<UtxosByPaymentCredCall>,
    /// `resolve_stake_for_payment_pkh(pkh) -> Option<stake-cred>`.
    pub resolve_stake_for_payment_pkh: Vec<ResolveStakeForPkhCall>,
    /// `read_utxos(refs, decode) -> [(ref, output)]`.
    pub read_utxos: Vec<ReadUtxosCall>,
    /// `read_output_datums(refs) -> [Option<(hash, payload)>]`.
    pub read_output_datums: Vec<ReadOutputDatumsCall>,
    /// `read_output_hashes(refs) -> [Option<hash>]`.
    pub read_output_hashes: Vec<ReadOutputHashesCall>,
    /// `tx_metadata(tx_hash) -> Option<aux_cbor>`.
    pub tx_metadata: Vec<TxMetadataCall>,
    /// `datum_by_hash(hash) -> Option<bytes>`.
    pub datum_by_hash: Vec<DatumByHashCall>,
    /// CBOR of the block being dispatched, if this is a
    /// `handle_event` trap. `None` for `init` traps.
    pub block_cbor: Option<Vec<u8>>,
    /// Slot of the block being dispatched (paired with
    /// `block_cbor`).
    pub block_slot: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct UtxosByAddressCall {
    pub address: String,
    pub refs: Vec<OutputRef>,
}

#[derive(Debug, Clone)]
pub struct UtxosByPolicyCall {
    pub policy: Vec<u8>,
    pub refs: Vec<OutputRef>,
}

#[derive(Debug, Clone)]
pub struct UtxosByPaymentCredCall {
    pub cred: Vec<u8>,
    pub refs: Vec<OutputRef>,
}

#[derive(Debug, Clone)]
pub struct ResolveStakeForPkhCall {
    pub pkh: Vec<u8>,
    pub result: Option<StakeCred>,
}

#[derive(Debug, Clone)]
pub struct ReadUtxosCall {
    pub requested: Vec<OutputRef>,
    pub decode: DecodeLevel,
    pub returned: Vec<(OutputRef, TypedOutput)>,
}

#[derive(Debug, Clone)]
pub struct ReadOutputDatumsCall {
    pub requested: Vec<OutputRef>,
    /// `Some((hash, payload))` per requested ref when the host
    /// could resolve; `None` when the host returned no datum.
    pub results: Vec<Option<(Vec<u8>, Vec<u8>)>>,
}

#[derive(Debug, Clone)]
pub struct ReadOutputHashesCall {
    pub requested: Vec<OutputRef>,
    pub results: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct TxMetadataCall {
    pub tx_hash: [u8; 32],
    pub aux_cbor: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct DatumByHashCall {
    pub hash: [u8; 32],
    pub bytes: Option<Vec<u8>>,
}

/// Wraps an inner data plane and records every call. Cloning
/// the `Arc` shares the same log — multiple wasmtime instances
/// share one logger when the host wires it that way (current
/// host wires one logger per module slot).
pub struct TrapContextLogger {
    inner: Arc<dyn DataPlaneFacade>,
    log: Mutex<TrapContextLog>,
}

impl TrapContextLogger {
    pub fn new(inner: Arc<dyn DataPlaneFacade>) -> Self {
        Self {
            inner,
            log: Mutex::new(TrapContextLog::default()),
        }
    }

    /// Snapshot the current log without mutating it. Called by
    /// the host on trap to extract the fixture context.
    pub fn snapshot(&self) -> TrapContextLog {
        self.log.lock().expect("trap log mutex").clone()
    }

    /// Reset the log. Called by the host before each dispatch
    /// so a successful previous block doesn't pollute the trap
    /// context for the current one.
    pub fn clear(&self) {
        let mut log = self.log.lock().expect("trap log mutex");
        *log = TrapContextLog::default();
    }

    /// Stash the in-flight block's CBOR + slot. Called by the
    /// follower immediately before `call_handle_event` so a
    /// trap during dispatch can pair the data-plane call log
    /// with the exact block bytes.
    pub fn set_block(&self, slot: u64, cbor: Vec<u8>) {
        let mut log = self.log.lock().expect("trap log mutex");
        log.block_slot = Some(slot);
        log.block_cbor = Some(cbor);
    }
}

#[async_trait::async_trait]
impl DataPlaneFacade for TrapContextLogger {
    async fn read_utxos(
        &self,
        refs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        let result = self.inner.read_utxos(refs, decode).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .read_utxos
            .push(ReadUtxosCall {
                requested: refs.to_vec(),
                decode,
                returned: result.clone(),
            });
        Ok(result)
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        let result = self.inner.utxos_by_address(address).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .utxos_by_address
            .push(UtxosByAddressCall {
                address: address.to_owned(),
                refs: result.clone(),
            });
        Ok(result)
    }

    async fn utxos_by_policy(&self, policy: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        let result = self.inner.utxos_by_policy(policy).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .utxos_by_policy
            .push(UtxosByPolicyCall {
                policy: policy.to_vec(),
                refs: result.clone(),
            });
        Ok(result)
    }

    async fn utxos_by_payment_cred(&self, cred: &[u8]) -> DataPlaneResult<Vec<OutputRef>> {
        let result = self.inner.utxos_by_payment_cred(cred).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .utxos_by_payment_cred
            .push(UtxosByPaymentCredCall {
                cred: cred.to_vec(),
                refs: result.clone(),
            });
        Ok(result)
    }

    async fn resolve_stake_for_payment_pkh(
        &self,
        pkh: &[u8],
    ) -> DataPlaneResult<Option<StakeCred>> {
        let result = self.inner.resolve_stake_for_payment_pkh(pkh).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .resolve_stake_for_payment_pkh
            .push(ResolveStakeForPkhCall {
                pkh: pkh.to_vec(),
                result: result.clone(),
            });
        Ok(result)
    }

    async fn datum_by_hash(&self, hash: &[u8; 32]) -> DataPlaneResult<Option<Vec<u8>>> {
        let result = self.inner.datum_by_hash(hash).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .datum_by_hash
            .push(DatumByHashCall {
                hash: *hash,
                bytes: result.clone(),
            });
        Ok(result)
    }

    async fn read_output_datums(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<(Vec<u8>, Vec<u8>)>>> {
        let result = self.inner.read_output_datums(refs).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .read_output_datums
            .push(ReadOutputDatumsCall {
                requested: refs.to_vec(),
                results: result.clone(),
            });
        Ok(result)
    }

    async fn read_output_hashes(
        &self,
        refs: &[OutputRef],
    ) -> DataPlaneResult<Vec<Option<Vec<u8>>>> {
        let result = self.inner.read_output_hashes(refs).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .read_output_hashes
            .push(ReadOutputHashesCall {
                requested: refs.to_vec(),
                results: result.clone(),
            });
        Ok(result)
    }

    async fn tx_metadata(&self, tx_hash: &[u8; 32]) -> DataPlaneResult<Option<Vec<u8>>> {
        let result = self.inner.tx_metadata(tx_hash).await?;
        self.log
            .lock()
            .expect("trap log mutex")
            .tx_metadata
            .push(TxMetadataCall {
                tx_hash: *tx_hash,
                aux_cbor: result.clone(),
            });
        Ok(result)
    }
}

/// Render a captured log as a TOML fixture matching the format
/// `tools/mitos-run` reads (`[[utxo]]` + `[[tx_metadata]]`).
///
/// Denormalisation rules:
/// - Each `OutputRef` ever returned by `utxos_by_address` /
///   `read_utxos` / `read_output_datums` / `read_output_hashes`
///   becomes one `[[utxo]]` entry. Fields are filled from the
///   richest call that touched it: `read_utxos` provides
///   address+lovelace, `read_output_hashes` provides datum_hash,
///   `read_output_datums` provides datum_payload_hex.
/// - Each unique `tx_metadata` call → one `[[tx_metadata]]`
///   entry. Calls returning `None` are skipped (replayer
///   defaults to `None` for unknown tx_hashes anyway).
/// - The block CBOR (when present) is emitted as a top-level
///   `[block]` table with `slot` + `cbor_hex`. `mitos-run`
///   doesn't consume this yet (`handle_event` replay is a
///   follow-up), but capturing it here means the data is
///   available the moment that path lands.
pub fn render_fixture_toml(log: &TrapContextLog, module_id: &str) -> String {
    use std::collections::BTreeMap;
    use std::fmt::Write;

    // Index utxos by ref. BTreeMap → deterministic ordering in
    // the output (helps when diffing fixtures).
    let mut utxo_index: BTreeMap<(Vec<u8>, u32), UtxoRecord> = BTreeMap::new();

    // First pass: read_utxos provides address + lovelace.
    for call in &log.read_utxos {
        for (oref, out) in &call.returned {
            let key = (oref.tx_hash.as_ref().to_vec(), oref.index);
            let rec = utxo_index.entry(key).or_default();
            rec.address = Some(out.address.clone());
            rec.lovelace = Some(out.lovelace);
        }
    }
    // utxos_by_address: contributes refs only; address comes
    // from the call's input.
    for call in &log.utxos_by_address {
        for oref in &call.refs {
            let key = (oref.tx_hash.as_ref().to_vec(), oref.index);
            let rec = utxo_index.entry(key).or_default();
            // Only fill if read_utxos didn't already.
            if rec.address.is_none() {
                rec.address = Some(call.address.clone());
            }
        }
    }
    // read_output_hashes: per-ref datum hash. Aligned with
    // requested refs.
    for call in &log.read_output_hashes {
        for (oref, hash_opt) in call.requested.iter().zip(call.results.iter()) {
            if let Some(hash) = hash_opt {
                let key = (oref.tx_hash.as_ref().to_vec(), oref.index);
                let rec = utxo_index.entry(key).or_default();
                rec.datum_hash = Some(hex::encode(hash));
            }
        }
    }
    // read_output_datums: per-ref resolved (hash, payload).
    // Sets datum_payload_hex when host could resolve.
    for call in &log.read_output_datums {
        for (oref, entry) in call.requested.iter().zip(call.results.iter()) {
            if let Some((hash, payload)) = entry {
                let key = (oref.tx_hash.as_ref().to_vec(), oref.index);
                let rec = utxo_index.entry(key).or_default();
                if rec.datum_hash.is_none() {
                    rec.datum_hash = Some(hex::encode(hash));
                }
                rec.datum_payload_hex = Some(hex::encode(payload));
            }
        }
    }

    let mut out = String::new();
    let _ = writeln!(out, "# Trap-time fixture for module `{module_id}`.");
    let _ = writeln!(
        out,
        "# Captured by mitos-platform's TrapContextLogger; replay with"
    );
    let _ = writeln!(out, "#   mitos-run --artifact <dir> --fixture <this-file>");
    let _ = writeln!(out);
    let _ = writeln!(out, "version = 1");

    // Block first if present — concise top-of-file.
    if let (Some(slot), Some(cbor)) = (log.block_slot, log.block_cbor.as_ref()) {
        let _ = writeln!(out);
        let _ = writeln!(out, "[block]");
        let _ = writeln!(out, "slot = {slot}");
        let _ = writeln!(out, "cbor_hex = \"{}\"", hex::encode(cbor));
    }

    // UTxO table — emit only entries we have at least an
    // address for; partial entries (just a ref, no fields)
    // would confuse the replayer. Skip those.
    for ((tx_hash, index), rec) in &utxo_index {
        let Some(address) = rec.address.as_ref() else {
            continue;
        };
        let _ = writeln!(out);
        let _ = writeln!(out, "[[utxo]]");
        let _ = writeln!(out, "tx_hash = \"{}\"", hex::encode(tx_hash));
        let _ = writeln!(out, "index = {index}");
        let _ = writeln!(out, "address = \"{address}\"");
        if let Some(lovelace) = rec.lovelace {
            let _ = writeln!(out, "lovelace = {lovelace}");
        }
        if let Some(hash) = &rec.datum_hash {
            let _ = writeln!(out, "datum_hash = \"{hash}\"");
        }
        if let Some(payload) = &rec.datum_payload_hex {
            let _ = writeln!(out, "datum_payload_hex = \"{payload}\"");
        }
    }

    // tx_metadata — one entry per unique tx_hash with an
    // aux-cbor response. Multiple calls to the same tx_hash
    // dedupe to the last seen response (consistent with what
    // `mitos-run`'s replayer does).
    let mut seen = std::collections::BTreeMap::<[u8; 32], &Vec<u8>>::new();
    for call in &log.tx_metadata {
        if let Some(bytes) = &call.aux_cbor {
            seen.insert(call.tx_hash, bytes);
        }
    }
    for (tx_hash, bytes) in seen {
        let _ = writeln!(out);
        let _ = writeln!(out, "[[tx_metadata]]");
        let _ = writeln!(out, "tx_hash = \"{}\"", hex::encode(tx_hash));
        let _ = writeln!(out, "aux_cbor_hex = \"{}\"", hex::encode(bytes));
    }

    out
}

#[derive(Default)]
struct UtxoRecord {
    address: Option<String>,
    lovelace: Option<u64>,
    datum_hash: Option<String>,
    datum_payload_hex: Option<String>,
}

/// Surface a fixture-write failure separately from the
/// underlying wasm error so callers can choose to log without
/// failing the whole start path.
#[derive(Debug, thiserror::Error)]
pub enum FixtureWriteError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// Write a captured log as a fixture TOML to `path`. Caller
/// supplies the path (the host knows where the per-module
/// directory is via `ModuleStorage`).
pub fn write_fixture(
    log: &TrapContextLog,
    module_id: &str,
    path: &std::path::Path,
) -> Result<(), FixtureWriteError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let toml = render_fixture_toml(log, module_id);
    std::fs::write(path, toml)?;
    Ok(())
}

// Keep the `DataPlaneError` import used by the trait; suppress
// unused-import warning if no method needs it directly.
#[allow(dead_code)]
fn _data_plane_error_marker(e: DataPlaneError) -> DataPlaneError {
    e
}
