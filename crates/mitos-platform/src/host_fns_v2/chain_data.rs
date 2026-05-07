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

    async fn utxos_by_address(
        &mut self,
        address: String,
    ) -> wasmtime::Result<Vec<WitOutputRef>> {
        let refs = self
            .data_plane
            .utxos_by_address(&address)
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
            .map(|opt| {
                opt.map(|(hash, payload)| WitTypedDatum {
                    hash,
                    payload,
                })
            })
            .collect())
    }

    async fn tx_metadata(
        &mut self,
        tx_hash: Vec<u8>,
    ) -> wasmtime::Result<Option<Vec<u8>>> {
        let bytes: [u8; 32] = tx_hash
            .as_slice()
            .try_into()
            .map_err(|_| wasmtime::Error::msg("tx_hash must be 32 bytes"))?;
        self.data_plane
            .tx_metadata(&bytes)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))
    }

    async fn datum_by_hash(
        &mut self,
        hash: Vec<u8>,
    ) -> wasmtime::Result<Option<Vec<u8>>> {
        let _: [u8; 32] = hash
            .as_slice()
            .try_into()
            .map_err(|_| wasmtime::Error::msg("datum hash must be 32 bytes"))?;
        // The v1 DataPlaneFacade only exposes the bulk
        // `read_output_datums` shape — there's no per-hash
        // accessor today. Wire a `datum_by_hash` method onto
        // the facade in a follow-up; for now return `None`
        // (matches the contract: hash not in witness-set
        // index → caller falls back to `tx-metadata`).
        Ok(None)
    }

    async fn read_tx(
        &mut self,
        _tx_hash: Vec<u8>,
    ) -> wasmtime::Result<Option<bindings_v2::TxRecord>> {
        // `read-tx` requires composing several data-plane
        // calls (block-by-tx-hash, decode, project to typed
        // shape). Wired in a follow-up step alongside the
        // bootstrap orchestrator. For now: surface as `None`
        // so modules calling it gracefully degrade.
        Ok(None)
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
