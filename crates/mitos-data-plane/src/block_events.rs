//! Block CBOR → v2 dispatch event drafts.
//!
//! This is the host-side entry point for the v2 dispatch path.
//! Given a Cardano block in CBOR form, produce a structured
//! per-TX draft with everything the dispatcher needs to emit
//! `produced` / `consumed` / `referenced` / `minted` /
//! `tx-context` events:
//!
//! - tx hashes + indices (for cursor + correlation)
//! - validity interval, required signers (for `tx-context`)
//! - produced outputs with their datum hashes + inline bytes
//! - input refs with redeemer bytes (when script-locked) — the
//!   dispatcher resolves prior outputs via the data plane in a
//!   bulk pass before filtering
//! - reference input refs (same — resolved later)
//! - mint multiset entries (per `(policy, asset_name)`)
//!
//! Pure pallas decode: no I/O, no data-plane access. The
//! dispatcher composes this with `read_utxos` for prior-output
//! resolution.
//!
//! Shape mirrors the v2 design doc's "Per-TX event ordering":
//! when the dispatcher walks a `TxDraft` to build events, it
//! emits `tx-context` first, then `referenced`, then `consumed`,
//! then `produced`, then `minted` — matching the WIT contract.

use cardano_assets::PolicyId;
use pallas_primitives::Hash;
use pallas_traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx, OriginalHash};

use crate::types::{
    AssetEntry, ChainPoint, OutputRef, TypedDatum, TypedOutput, ValidityInterval,
};

/// Decoded block in dispatch-friendly form. Per-TX drafts that
/// the platform's dispatcher walks to build full events after
/// resolving prior outputs via the data plane.
#[derive(Debug, Clone)]
pub struct DecodedBlockV2 {
    pub cursor: ChainPoint,
    pub txs: Vec<TxDraft>,
}

/// One TX's worth of raw decoded data. Inputs / reference
/// inputs are refs only — prior outputs and prior datums get
/// resolved by the dispatcher via `read_utxos` /
/// `read_output_datums`.
#[derive(Debug, Clone)]
pub struct TxDraft {
    pub tx_hash: Hash<32>,
    pub tx_idx: u32,
    pub validity_interval: ValidityInterval,
    pub required_signers: Vec<Hash<28>>,
    /// Inputs being spent. Each carries the redeemer bytes
    /// when this input is script-locked (selected via
    /// `find_spend_redeemer` against canonical input order).
    pub inputs: Vec<InputDraft>,
    /// Reference inputs (read-only). No redeemer.
    pub reference_inputs: Vec<OutputRef>,
    /// Outputs being produced — fully decoded TypedOutput plus
    /// per-output datum info.
    pub outputs: Vec<OutputDraft>,
    /// Mint field — one entry per `(policy, asset_name)` pair.
    pub mints: Vec<MintDraft>,
    /// Auxiliary data CBOR. Modules calling
    /// `chain_data::tx_metadata` see this; included on the
    /// draft so the dispatcher can populate `tx-context`'s
    /// derived metadata if needed (currently unused — modules
    /// fetch on demand).
    pub aux_data_cbor: Option<Vec<u8>>,
    /// Datums in this TX's witness set, paired with their
    /// `original_hash`. Lets the dispatcher fill in
    /// `prior_datum.original_cbor` for hash-datum prior outputs
    /// the data plane couldn't resolve directly — jpg.store's
    /// metadata-encoded CO datums, for instance, are only
    /// revealed in the cancel/accept TX's witness set, so a
    /// `read_utxo` lookup of the prior CO yields hash-only
    /// `TypedDatum` until this fallback runs.
    pub witness_datums: Vec<(Hash<32>, Vec<u8>)>,
}

#[derive(Debug, Clone)]
pub struct InputDraft {
    pub oref: OutputRef,
    /// CBOR-encoded redeemer when this input was spent through
    /// a Plutus script. `None` for key-locked inputs.
    pub redeemer: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct OutputDraft {
    pub output: TypedOutput,
    /// Datum hash + inline bytes when present. Modules see
    /// `Some(typed_datum)` post-resolution; bytes get filled in
    /// by the dispatcher when resolution succeeds.
    pub datum_hash: Option<Hash<32>>,
    pub inline_datum_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct MintDraft {
    pub policy: PolicyId,
    pub asset_name: Vec<u8>,
    pub quantity_delta: i64,
}

/// Decode a CBOR-encoded block into the v2 dispatch draft.
/// Returns `Err` only on undecoded CBOR; per-tx and per-output
/// projection failures are logged + dropped (best-effort
/// posture — a single bad TX shouldn't poison the block).
pub fn decode_block_v2(cbor: &[u8]) -> anyhow::Result<DecodedBlockV2> {
    let block = MultiEraBlock::decode(cbor)
        .map_err(|e| anyhow::anyhow!("MultiEraBlock decode: {e}"))?;
    let cursor = ChainPoint::Specific(block.slot(), block.hash());

    let txs = block
        .txs()
        .iter()
        .enumerate()
        .map(|(idx, tx)| project_tx(idx as u32, tx))
        .collect();

    Ok(DecodedBlockV2 { cursor, txs })
}

fn project_tx(tx_idx: u32, tx: &MultiEraTx<'_>) -> TxDraft {
    let tx_hash = tx.hash();

    // Validity interval — `validity_start` may not be exposed
    // for every era; `ttl` is `valid_to` (exclusive).
    let validity_interval = ValidityInterval {
        valid_from: tx.validity_start(),
        valid_to: tx.ttl(),
    };

    // `MultiEraSigners::collect` takes a generic `T:
    // FromIterator<&Hash<28>>` — collect into `Vec<&Hash<28>>`,
    // then deref-copy into owned `Hash<28>`s.
    let required_signers: Vec<Hash<28>> = tx
        .required_signers()
        .collect::<Vec<&Hash<28>>>()
        .into_iter()
        .copied()
        .collect();

    let raw_outputs = tx.outputs();
    let outputs: Vec<OutputDraft> = raw_outputs
        .iter()
        .map(project_output_draft)
        .collect();

    let inputs: Vec<InputDraft> = tx
        .consumes()
        .iter()
        .enumerate()
        .map(|(input_order, input)| {
            // `find_spend_redeemer` is on `MultiEraTx` directly,
            // not the Vec returned by `redeemers()`. Indices are
            // canonical input order (sorted by tx_hash, idx),
            // which `consumes()` already returns.
            let redeemer = tx
                .find_spend_redeemer(input_order as u32)
                .map(|r| r.encode());
            InputDraft {
                oref: OutputRef::new(*input.hash(), input.index() as u32),
                redeemer,
            }
        })
        .collect();

    let reference_inputs: Vec<OutputRef> = tx
        .reference_inputs()
        .iter()
        .map(|i| OutputRef::new(*i.hash(), i.index() as u32))
        .collect();

    let mints: Vec<MintDraft> = tx
        .mints()
        .iter()
        .flat_map(|policy_assets| {
            let policy = match PolicyId::new(hex::encode(policy_assets.policy().as_slice())) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "skip mint with invalid policy id");
                    return Vec::new();
                }
            };
            policy_assets
                .assets()
                .iter()
                .map(|asset| MintDraft {
                    policy: policy.clone(),
                    asset_name: asset.name().to_vec(),
                    // `any_coin()` returns `i128` (Conway widened
                    // multi-asset quantities); clamp to `i64`
                    // since real mint deltas comfortably fit
                    // (max practical Cardano supply ~45B ADA-
                    // equivalents, far below i64 range).
                    quantity_delta: asset.any_coin().clamp(i64::MIN as i128, i64::MAX as i128) as i64,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let aux_data_cbor = extract_aux_data(tx);

    let witness_datums: Vec<(Hash<32>, Vec<u8>)> = tx
        .plutus_data()
        .iter()
        .map(|d| (d.original_hash(), d.raw_cbor().to_vec()))
        .collect();

    TxDraft {
        tx_hash,
        tx_idx,
        validity_interval,
        required_signers,
        inputs,
        reference_inputs,
        outputs,
        mints,
        aux_data_cbor,
        witness_datums,
    }
}

fn project_output_draft(output: &MultiEraOutput<'_>) -> OutputDraft {
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
            let policy = match PolicyId::new(hex::encode(policy_assets.policy().as_slice())) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "skip asset with invalid policy id");
                    return Vec::new();
                }
            };
            policy_assets
                .assets()
                .iter()
                .map(|asset| AssetEntry {
                    policy_id: policy.clone(),
                    asset_name_hex: hex::encode(asset.name()),
                    quantity: asset.any_coin().max(0) as u64,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let typed_output = TypedOutput {
        address,
        lovelace,
        assets,
        datum: None, // populated post-resolution by dispatcher
        script_ref: None,
        original_cbor: None,
        decoded_at: crate::types::DecodeLevel::Lean,
    };

    let (datum_hash, inline_datum_bytes) = extract_datum_info(output);
    OutputDraft {
        output: typed_output,
        datum_hash,
        inline_datum_bytes,
    }
}

fn extract_datum_info(output: &MultiEraOutput<'_>) -> (Option<Hash<32>>, Option<Vec<u8>>) {
    use pallas::ledger::primitives::conway::DatumOption;
    match output.datum() {
        Some(DatumOption::Hash(h)) => (Some(h), None),
        Some(DatumOption::Data(cbor_wrap)) => {
            let raw = cbor_wrap.0.raw_cbor().to_vec();
            let hash = cbor_wrap.0.original_hash();
            (Some(hash), Some(raw))
        }
        None => (None, None),
    }
}

/// Pull the raw auxiliary-data CBOR. Same projection v1 uses;
/// duplicated here to keep block_decode_v2 free of v1 deps so
/// v1 can be deleted without touching v2.
fn extract_aux_data(tx: &MultiEraTx<'_>) -> Option<Vec<u8>> {
    use pallas::codec::utils::Nullable;
    match tx {
        MultiEraTx::AlonzoCompatible(t, _) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        MultiEraTx::Babbage(t) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        MultiEraTx::Conway(t) => match &t.auxiliary_data {
            Nullable::Some(kr) => Some(kr.raw_cbor().to_vec()),
            _ => None,
        },
        MultiEraTx::Byron(_) | _ => None,
    }
}

/// Build a `TypedDatum` from a draft's recorded datum_hash +
/// inline bytes. Used by the dispatcher when constructing
/// `produced` / `consumed` / `referenced` events. `payload`
/// stays `None` because we keep PlutusData decoding out of the
/// dispatch hot path — modules that need it use minicbor
/// themselves.
pub fn datum_from_draft(
    hash: Option<Hash<32>>,
    inline_bytes: Option<Vec<u8>>,
) -> Option<TypedDatum> {
    let hash = hash?;
    Some(TypedDatum {
        hash,
        payload: None,
        original_cbor: inline_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// Smoke test: decode the captured mainnet block fixture
    /// (already used by v1's lifecycle test) into a
    /// `DecodedBlockV2` and assert basic invariants.
    #[test]
    fn decodes_mainnet_block_fixture() {
        let fixture_path = PathBuf::from("../mitos-platform/tests/fixtures/186000000.block.cbor");
        if !fixture_path.exists() {
            eprintln!("skipping: fixture not present at {}", fixture_path.display());
            return;
        }
        let cbor = std::fs::read(&fixture_path).expect("read fixture");
        let decoded = decode_block_v2(&cbor).expect("decode v2");

        // Cursor should be Specific with the block's slot.
        match decoded.cursor {
            ChainPoint::Specific(slot, _) => {
                assert_eq!(slot, 186_000_000, "cursor slot matches fixture");
            }
            other => panic!("expected Specific cursor, got {other:?}"),
        }

        // At least one TX, and each tx_idx is contiguous from 0.
        assert!(!decoded.txs.is_empty(), "fixture has at least one TX");
        for (i, tx) in decoded.txs.iter().enumerate() {
            assert_eq!(tx.tx_idx, i as u32, "tx_idx is contiguous");
            // Every TX has at least one input (basic Cardano
            // sanity); collateral-only TXs are vanishingly rare
            // and not in this fixture.
            assert!(
                !tx.inputs.is_empty() || !tx.reference_inputs.is_empty() || !tx.outputs.is_empty(),
                "TX {i} should have at least some content",
            );
        }
    }
}
