//! `chain-data` interface — proxies into the data plane.
//!
//! Bulk shape only (`read-utxos`). Single-utxo lookups go via
//! the `block-context` resource so they can be lazy-cached per
//! block.

use crate::bindings::{
    AssetEntry as WitAssetEntry, AssetId as WitAssetId, ChainDataHost,
    DecodeLevel as WitDecodeLevel, OutputRef as WitOutputRef, TypedOutput as WitTypedOutput,
};
use crate::host_fns::HostState;

impl ChainDataHost for HostState {
    async fn read_utxos(
        &mut self,
        refs: Vec<WitOutputRef>,
        decode: WitDecodeLevel,
    ) -> wasmtime::Result<Vec<WitTypedOutput>> {
        let dp_refs: Vec<mitos_data_plane::OutputRef> = refs
            .iter()
            .map(into_dp_ref_owned)
            .collect::<wasmtime::Result<Vec<_>>>()?;
        let dp_decode = into_dp_decode(decode);
        let pairs = self
            .data_plane
            .read_utxos(&dp_refs, dp_decode)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        // Return outputs in the same order data plane gave us;
        // missing entries are silently omitted by the data plane,
        // matching our WIT contract (modules that need order-
        // preserving lookups use `block-context` instead).
        Ok(pairs
            .into_iter()
            .map(|(_, out)| from_dp_output(out))
            .collect())
    }
}

/// WIT `output-ref` → data-plane `OutputRef`. Fails if `tx_hash`
/// is not exactly 32 bytes (guest sent a malformed value).
/// `pub(crate)` so the `block-context` lazy resolution path can
/// reuse the conversion.
pub(crate) fn into_dp_ref_owned(r: &WitOutputRef) -> wasmtime::Result<mitos_data_plane::OutputRef> {
    let bytes: [u8; 32] = r
        .tx_hash
        .as_slice()
        .try_into()
        .map_err(|_| wasmtime::Error::msg("output-ref tx_hash must be 32 bytes"))?;
    Ok(mitos_data_plane::OutputRef::from_bytes(bytes, r.index))
}

fn into_dp_decode(d: WitDecodeLevel) -> mitos_data_plane::DecodeLevel {
    match d {
        WitDecodeLevel::Lean => mitos_data_plane::DecodeLevel::Lean,
        WitDecodeLevel::WithDatum => mitos_data_plane::DecodeLevel::WithDatum,
        WitDecodeLevel::Full => mitos_data_plane::DecodeLevel::Full,
    }
}

/// Project the data-plane's rich `TypedOutput` into the lean
/// WIT shape. v1 surfaces address + lovelace + asset multiset;
/// datum and script_ref aren't on the WIT yet — when they are,
/// expand here and bump the ABI minor version. `pub(crate)`
/// so the `block-context` lazy resolution path can reuse it.
pub(crate) fn from_dp_output(o: mitos_data_plane::TypedOutput) -> WitTypedOutput {
    WitTypedOutput {
        address: o.address,
        lovelace: o.lovelace,
        assets: o.assets.into_iter().map(from_dp_asset).collect(),
    }
}

fn from_dp_asset(a: mitos_data_plane::AssetEntry) -> WitAssetEntry {
    // `PolicyId` is hex-typed; hex-decode for the byte form WIT
    // expects. `as_bytes` is fallible only on internal corruption
    // (a non-hex `PolicyId` shouldn't exist) — fall back to empty
    // rather than trap, since this is on the success path of a
    // bulk lookup and a single bad asset shouldn't poison the
    // batch. Asset name is already hex-encoded.
    let policy = a.policy_id.as_bytes().unwrap_or([0u8; 28]).to_vec();
    let name = hex::decode(&a.asset_name_hex).unwrap_or_default();
    WitAssetEntry {
        asset: WitAssetId { policy, name },
        quantity: a.quantity,
    }
}
