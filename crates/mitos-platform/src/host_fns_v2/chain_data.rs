//! v2 `chain-data` interface — proxies into the data plane.
//!
//! Extended over v1 with `read-tx` (full TX rollup) and
//! `datum-by-hash` (side-door for hash-only lookups). The
//! v2 WIT drops `decode-level` from `read-utxos` since the
//! returned shape is a fixed lean projection.

use crate::bindings_v2::{
    self, AssetEntry as WitAssetEntry, AssetId as WitAssetId, ChainDataHost,
    OutputRef as WitOutputRef, TypedDatum as WitTypedDatum, TypedOutput as WitTypedOutput,
};
use crate::host_fns_v2::HostStateV2;

impl ChainDataHost for HostStateV2 {
    async fn read_utxos(
        &mut self,
        refs: Vec<WitOutputRef>,
    ) -> wasmtime::Result<Vec<WitTypedOutput>> {
        let dp_refs: Vec<mitos_data_plane::OutputRef> = refs
            .iter()
            .map(into_dp_ref_owned)
            .collect::<wasmtime::Result<Vec<_>>>()?;
        let pairs = self
            .data_plane
            .read_utxos(&dp_refs, mitos_data_plane::DecodeLevel::Lean)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(pairs
            .into_iter()
            .map(|(_, out)| from_dp_output(out))
            .collect())
    }

    async fn utxos_by_address(&mut self, address: String) -> wasmtime::Result<Vec<WitOutputRef>> {
        let refs = self
            .data_plane
            .utxos_by_address(&address)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(refs.into_iter().map(from_dp_ref).collect())
    }

    async fn utxos_by_policy(&mut self, policy: Vec<u8>) -> wasmtime::Result<Vec<WitOutputRef>> {
        let refs = self
            .data_plane
            .utxos_by_policy(&policy)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(refs.into_iter().map(from_dp_ref).collect())
    }

    async fn read_output_datums(
        &mut self,
        refs: Vec<WitOutputRef>,
    ) -> wasmtime::Result<Vec<Option<WitTypedDatum>>> {
        let dp_refs: Vec<mitos_data_plane::OutputRef> = refs
            .iter()
            .map(into_dp_ref_owned)
            .collect::<wasmtime::Result<Vec<_>>>()?;
        let resolved = self
            .data_plane
            .read_output_datums(&dp_refs)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(resolved
            .into_iter()
            .map(|opt| opt.map(|(hash, payload)| WitTypedDatum { hash, payload }))
            .collect())
    }

    async fn tx_metadata(&mut self, tx_hash: Vec<u8>) -> wasmtime::Result<Option<Vec<u8>>> {
        let bytes: [u8; 32] = tx_hash
            .as_slice()
            .try_into()
            .map_err(|_| wasmtime::Error::msg("tx_hash must be 32 bytes"))?;
        self.data_plane
            .tx_metadata(&bytes)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))
    }

    async fn datum_by_hash(&mut self, hash: Vec<u8>) -> wasmtime::Result<Option<Vec<u8>>> {
        let bytes: [u8; 32] = hash
            .as_slice()
            .try_into()
            .map_err(|_| wasmtime::Error::msg("datum hash must be 32 bytes"))?;
        // CIP-68 hash-only outputs and any reference-datum
        // resolution path lands here. The facade's
        // `datum_by_hash` delegates to `ChainDataPlane::
        // read_datum` and extracts `original_cbor` — what
        // modules actually want.
        self.data_plane
            .datum_by_hash(&bytes)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))
    }

    async fn read_tx(
        &mut self,
        tx_hash: Vec<u8>,
    ) -> wasmtime::Result<Option<bindings_v2::TxRecord>> {
        let bytes: [u8; 32] = tx_hash
            .as_slice()
            .try_into()
            .map_err(|_| wasmtime::Error::msg("tx_hash must be 32 bytes"))?;
        let phash = pallas_primitives::Hash::<32>::from(bytes);
        let record = self
            .data_plane
            .read_tx(&phash)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(record.map(tx_record_to_wit))
    }
}

fn tx_record_to_wit(r: mitos_data_plane::TxRecord) -> bindings_v2::TxRecord {
    bindings_v2::TxRecord {
        tx_hash: r.tx_hash.as_ref().to_vec(),
        tx_idx: r.tx_idx,
        cursor: chain_point_to_wit(r.cursor),
        inputs: r.inputs.into_iter().map(consumed_input_to_wit).collect(),
        reference_inputs: r
            .reference_inputs
            .into_iter()
            .map(referenced_input_to_wit)
            .collect(),
        outputs: r.outputs.into_iter().map(typed_output_to_wit).collect(),
        mint: r.mint.into_iter().map(mint_entry_to_wit).collect(),
        required_signers: r
            .required_signers
            .into_iter()
            .map(|h| h.as_ref().to_vec())
            .collect(),
        validity_interval: bindings_v2::ValidityInterval {
            valid_from: r.validity_interval.valid_from,
            valid_to: r.validity_interval.valid_to,
        },
        aux_data: r.aux_data,
    }
}

/// Sentinel empty `TypedOutput` returned when the host couldn't
/// resolve a prior output (already-spent input). Modules detect
/// via `prior_output.address.is_empty()`.
fn empty_typed_output() -> bindings_v2::TypedOutput {
    bindings_v2::TypedOutput {
        address: String::new(),
        lovelace: 0,
        assets: Vec::new(),
    }
}

fn consumed_input_to_wit(c: mitos_data_plane::ConsumedInput) -> bindings_v2::ConsumedInput {
    bindings_v2::ConsumedInput {
        oref: WitOutputRef {
            tx_hash: c.oref.tx_hash.as_ref().to_vec(),
            index: c.oref.index,
        },
        prior_output: c
            .prior_output
            .map(typed_output_to_wit)
            .unwrap_or_else(empty_typed_output),
        prior_datum: c.prior_datum.map(typed_datum_to_wit),
        redeemer: c.redeemer,
    }
}

fn referenced_input_to_wit(r: mitos_data_plane::ReferencedInput) -> bindings_v2::ReferencedInput {
    bindings_v2::ReferencedInput {
        oref: WitOutputRef {
            tx_hash: r.oref.tx_hash.as_ref().to_vec(),
            index: r.oref.index,
        },
        prior_output: r
            .prior_output
            .map(typed_output_to_wit)
            .unwrap_or_else(empty_typed_output),
        prior_datum: r.prior_datum.map(typed_datum_to_wit),
    }
}

fn mint_entry_to_wit(m: mitos_data_plane::MintEntry) -> bindings_v2::MintEntry {
    bindings_v2::MintEntry {
        policy: m.policy.as_bytes().unwrap_or([0u8; 28]).to_vec(),
        asset_name: m.asset_name,
        quantity_delta: m.quantity_delta,
    }
}

fn typed_output_to_wit(o: mitos_data_plane::TypedOutput) -> bindings_v2::TypedOutput {
    bindings_v2::TypedOutput {
        address: o.address,
        lovelace: o.lovelace,
        assets: o
            .assets
            .into_iter()
            .map(|a| WitAssetEntry {
                asset: WitAssetId {
                    policy: a.policy_id.as_bytes().unwrap_or([0u8; 28]).to_vec(),
                    name: hex::decode(&a.asset_name_hex).unwrap_or_default(),
                },
                quantity: a.quantity,
            })
            .collect(),
    }
}

fn typed_datum_to_wit(d: mitos_data_plane::TypedDatum) -> WitTypedDatum {
    WitTypedDatum {
        hash: d.hash.as_ref().to_vec(),
        payload: d.original_cbor.unwrap_or_default(),
    }
}

fn chain_point_to_wit(cp: mitos_data_plane::ChainPoint) -> bindings_v2::ChainPoint {
    match cp {
        mitos_data_plane::ChainPoint::Origin => bindings_v2::ChainPoint::Origin,
        mitos_data_plane::ChainPoint::Slot(s) => bindings_v2::ChainPoint::SlotOnly(s),
        mitos_data_plane::ChainPoint::Specific(s, h) => {
            bindings_v2::ChainPoint::Specific(bindings_v2::SpecificPoint {
                slot: s,
                block_hash: h.as_ref().to_vec(),
            })
        }
    }
}

fn from_dp_ref(r: mitos_data_plane::OutputRef) -> WitOutputRef {
    WitOutputRef {
        tx_hash: r.tx_hash.as_ref().to_vec(),
        index: r.index,
    }
}

fn into_dp_ref_owned(r: &WitOutputRef) -> wasmtime::Result<mitos_data_plane::OutputRef> {
    let bytes: [u8; 32] = r
        .tx_hash
        .as_slice()
        .try_into()
        .map_err(|_| wasmtime::Error::msg("output-ref tx_hash must be 32 bytes"))?;
    Ok(mitos_data_plane::OutputRef::new(
        pallas_primitives::Hash::new(bytes),
        r.index,
    ))
}

fn from_dp_output(o: mitos_data_plane::TypedOutput) -> WitTypedOutput {
    WitTypedOutput {
        address: o.address,
        lovelace: o.lovelace,
        assets: o
            .assets
            .into_iter()
            .map(|a| WitAssetEntry {
                asset: WitAssetId {
                    policy: a.policy_id.as_bytes().unwrap_or([0u8; 28]).to_vec(),
                    name: hex::decode(&a.asset_name_hex).unwrap_or_default(),
                },
                quantity: a.quantity,
            })
            .collect(),
    }
}
