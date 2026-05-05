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
//!
//! v1 deliberately drops:
//! - datum + script_ref (not on the WIT yet — bump ABI when
//!   they land)
//! - `original_cbor` passthrough (modules requiring raw CBOR
//!   are out of v1 scope)
//!
//! Address projection is bech32-encoded `String` because that's
//! what `mitos_data_plane::TypedOutput` already emits and the
//! WIT shape mirrors. Outputs whose address fails to parse are
//! emitted with an empty address string and a warn log; the
//! existing host-side ownership indexer does the same — a
//! single bad output shouldn't poison the block.

use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};

use crate::bindings::{AssetEntry, AssetId, TypedOutput};
use crate::resolved_block::TxView;

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
    let outputs: Vec<TypedOutput> = tx.outputs().iter().map(project_output).collect();
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
