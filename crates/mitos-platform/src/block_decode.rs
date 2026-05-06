//! Pallas → platform `TxView` projection.
//!
//! The host pre-decodes blocks (modules never touch pallas) and
//! hands typed views to the wasm runtime via the `block-context`
//! resource. This module is the projection function.
//!
//! v1 surfaces:
//! - `tx_hash` (32 bytes)
//! - produced outputs as `bindings::TypedOutput` (address +
//!   lovelace + assets)
//! - `consumed_input_refs` for lazy on-demand resolution
//! - per-output datum info (hash + inline-bytes-when-present)
//!   stashed on `TxView` for the `get-output-datum` host fn
//!
//! v1 deliberately drops:
//! - script_ref (not on the WIT yet — bump ABI when it lands)
//! - `original_cbor` passthrough (modules requiring raw CBOR
//!   are out of v1 scope)
//!
//! Address projection is bech32-encoded `String` because that's
//! what `mitos_data_plane::TypedOutput` already emits and the
//! WIT shape mirrors. Outputs whose address fails to parse are
//! emitted with an empty address string and a warn log; the
//! existing host-side ownership indexer does the same — a
//! single bad output shouldn't poison the block.

use pallas::ledger::primitives::conway::DatumOption;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx, OriginalHash};

use crate::bindings::{AssetEntry, AssetId, TypedOutput};
use crate::resolved_block::{OutputDatumInfo, TxView};

/// Decode a CBOR-encoded block into the platform's typed view.
/// Returns `Err` on undecoded CBOR; per-tx and per-output
/// projection failures are logged + dropped (the same
/// best-effort posture the existing host-side indexer uses).
pub fn decode_block(cbor: &[u8]) -> Result<DecodedBlock, anyhow::Error> {
    let block =
        MultiEraBlock::decode(cbor).map_err(|e| anyhow::anyhow!("MultiEraBlock decode: {e}"))?;
    let slot = block.slot();
    let txs = block.txs().iter().map(project_tx).collect();
    Ok(DecodedBlock { slot, txs })
}

/// Output of `decode_block` — directly feeds
/// `BlockEvent::Apply { slot, cursor_after, txs }`.
pub struct DecodedBlock {
    pub slot: u64,
    pub txs: Vec<TxView>,
}

fn project_tx(tx: &MultiEraTx<'_>) -> TxView {
    let tx_hash = tx.hash().as_slice().to_vec();
    let raw_outputs = tx.outputs();
    let outputs: Vec<TypedOutput> = raw_outputs.iter().map(project_output).collect();
    let output_datums: Vec<Option<OutputDatumInfo>> =
        raw_outputs.iter().map(extract_datum_info).collect();
    let consumed_input_refs = tx
        .consumes()
        .iter()
        .map(|input| crate::bindings::OutputRef {
            tx_hash: input.hash().as_slice().to_vec(),
            index: input.index() as u32,
        })
        .collect();
    TxView {
        tx_hash,
        outputs,
        output_datums,
        consumed_input_refs,
    }
}

fn project_output(output: &MultiEraOutput<'_>) -> TypedOutput {
    let address = match output.address() {
        Ok(addr) => addr.to_string(),
        Err(e) => {
            tracing::debug!(error = %e, "output address parse failed");
            String::new()
        }
    };
    let value = output.value();
    let lovelace = value.coin();
    let assets: Vec<AssetEntry> = value
        .assets()
        .iter()
        .flat_map(|policy_assets| {
            let policy = policy_assets.policy().as_slice().to_vec();
            policy_assets
                .assets()
                .iter()
                .map(|asset| AssetEntry {
                    asset: AssetId {
                        policy: policy.clone(),
                        name: asset.name().to_vec(),
                    },
                    quantity: asset.any_coin().max(0) as u64,
                })
                .collect::<Vec<_>>()
        })
        .collect();
    TypedOutput {
        address,
        lovelace,
        assets,
    }
}

/// Extract per-output datum info during block decode. Inline
/// datums carry their bytes directly here; hash-attached datums
/// record only the hash (the host resolves via Dolos's datum
/// state index later, when the guest asks via
/// `block-context::get-output-datum`).
///
/// This is the hook for the caller-blind datum resolution
/// principle: when the guest asks for the datum on `(tx_idx,
/// output_idx)`, the host has the inline payload pre-extracted
/// here OR resolves the hash transparently via the data plane.
/// The guest never sees the source distinction.
fn extract_datum_info(output: &MultiEraOutput<'_>) -> Option<OutputDatumInfo> {
    let datum_opt = output.datum()?;
    match datum_opt {
        DatumOption::Hash(h) => Some(OutputDatumInfo {
            hash: h,
            inline_bytes: None,
        }),
        DatumOption::Data(cbor_wrap) => {
            // `cbor_wrap` is `CborWrap<KeepRaw<'_, PlutusData<'_>>>`.
            // The original CBOR of the inner PlutusData is what we
            // want to ship as `payload` to the guest. `KeepRaw`
            // stashes the original bytes during decode.
            let raw = cbor_wrap.0.raw_cbor().to_vec();
            let hash = cbor_wrap.0.original_hash();
            Some(OutputDatumInfo {
                hash,
                inline_bytes: Some(raw),
            })
        }
    }
}
