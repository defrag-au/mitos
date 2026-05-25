//! v2 `chain-data` interface — proxies into the data plane.
//!
//! Extended over v1 with `read-tx` (full TX rollup) and
//! `datum-by-hash` (side-door for hash-only lookups). The
//! v2 WIT drops `decode-level` from `read-utxos` since the
//! returned shape is a fixed lean projection.

use crate::bindings_v2::{
    self, AssetEntry as WitAssetEntry, AssetId as WitAssetId, ChainDataHost,
    OutputRef as WitOutputRef, StakeCred as WitStakeCred, TypedDatum as WitTypedDatum,
    TypedOutput as WitTypedOutput, UtxoPage as WitUtxoPage,
};
use crate::host_fns_v2::HostStateV2;
use crate::host_fns_v2::scan_cache::ScanPage;

impl ChainDataHost for HostStateV2 {
    async fn read_utxos(
        &mut self,
        refs: Vec<WitOutputRef>,
    ) -> wasmtime::Result<Vec<(WitOutputRef, WitTypedOutput)>> {
        let dp_refs: Vec<mitos_data_plane::OutputRef> = refs
            .iter()
            .map(into_dp_ref_owned)
            .collect::<wasmtime::Result<Vec<_>>>()?;
        let pairs = self
            .data_plane
            .read_utxos(&dp_refs, mitos_data_plane::DecodeLevel::Lean)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        // Each output carries its own ref — the data plane
        // resolves from a UTxO map, so the result is neither in
        // `refs` order nor 1:1. Guests correlate via the ref.
        Ok(pairs
            .into_iter()
            .map(|(oref, out)| (from_dp_ref(oref), from_dp_output(out)))
            .collect())
    }

    async fn utxos_by_address(
        &mut self,
        address: String,
        after: Option<Vec<u8>>,
        limit: u32,
    ) -> wasmtime::Result<WitUtxoPage> {
        match after {
            Some(token) => self.resume_scan(&token, limit),
            None => {
                let dp = self.data_plane.clone();
                let refs = dp
                    .utxos_by_address(&address)
                    .await
                    .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
                let anchor = scan_anchor_slot(dp.as_ref()).await?;
                Ok(self.begin_scan(refs, anchor, limit))
            }
        }
    }

    async fn utxos_by_policy(
        &mut self,
        policy: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: u32,
    ) -> wasmtime::Result<WitUtxoPage> {
        match after {
            Some(token) => self.resume_scan(&token, limit),
            None => {
                let dp = self.data_plane.clone();
                let refs = dp
                    .utxos_by_policy(&policy)
                    .await
                    .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
                let anchor = scan_anchor_slot(dp.as_ref()).await?;
                Ok(self.begin_scan(refs, anchor, limit))
            }
        }
    }

    async fn utxos_by_payment_cred(
        &mut self,
        cred: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: u32,
    ) -> wasmtime::Result<WitUtxoPage> {
        match after {
            Some(token) => self.resume_scan(&token, limit),
            None => {
                let dp = self.data_plane.clone();
                let refs = dp
                    .utxos_by_payment_cred(&cred)
                    .await
                    .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
                let anchor = scan_anchor_slot(dp.as_ref()).await?;
                Ok(self.begin_scan(refs, anchor, limit))
            }
        }
    }

    async fn resolve_stake_for_payment_pkh(
        &mut self,
        pkh: Vec<u8>,
    ) -> wasmtime::Result<Option<WitStakeCred>> {
        let cred = self
            .data_plane
            .resolve_stake_for_payment_pkh(&pkh)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(cred.map(stake_cred_to_wit))
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

    async fn asset_state(
        &mut self,
        policy: Vec<u8>,
        asset_name: Vec<u8>,
    ) -> wasmtime::Result<Option<bindings_v2::AssetMintState>> {
        let state = self
            .data_plane
            .asset_state(&policy, &asset_name)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(state.map(asset_mint_state_to_wit))
    }

    async fn cip25_metadata(
        &mut self,
        policy: Vec<u8>,
        asset_name: Vec<u8>,
    ) -> wasmtime::Result<Option<bindings_v2::Cip25MetadataResult>> {
        let resolution = self
            .data_plane
            .cip25_metadata(&policy, &asset_name)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(resolution.map(|r| bindings_v2::Cip25MetadataResult {
            metadata_json: r.metadata_json,
            source_tx: r.source_tx,
        }))
    }

    async fn cip25_metadata_batch(
        &mut self,
        policy: Vec<u8>,
        asset_names: Vec<Vec<u8>>,
    ) -> wasmtime::Result<Vec<Option<bindings_v2::Cip25MetadataResult>>> {
        let resolutions = self
            .data_plane
            .cip25_metadata_batch(&policy, &asset_names)
            .await
            .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
        Ok(resolutions
            .into_iter()
            .map(|opt| {
                opt.map(|r| bindings_v2::Cip25MetadataResult {
                    metadata_json: r.metadata_json,
                    source_tx: r.source_tx,
                })
            })
            .collect())
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

/// Paged-scan helpers for the `utxos-by-*` host-fns. Kept off
/// the `ChainDataHost` impl so the WIT-trait block stays a thin
/// shim over these.
impl HostStateV2 {
    /// Materialise a fresh scan over `refs` and return its first
    /// page. `refs` is dolos's dump-all; the scan cache freezes +
    /// sorts it. The page is clamped to the adaptive sizer.
    fn begin_scan(
        &mut self,
        refs: Vec<mitos_data_plane::OutputRef>,
        anchor_slot: u64,
        limit: u32,
    ) -> WitUtxoPage {
        let clamp = self.adaptive.page_limit(limit);
        scan_page_to_wit(self.scan_cache.begin(refs, anchor_slot, clamp))
    }

    /// Continue a scan from a prior page's opaque token.
    fn resume_scan(&mut self, token: &[u8], limit: u32) -> wasmtime::Result<WitUtxoPage> {
        let clamp = self.adaptive.page_limit(limit);
        self.scan_cache
            .resume(token, clamp)
            .map(scan_page_to_wit)
            .map_err(|e| wasmtime::Error::msg(format!("paged utxo scan: {e}")))
    }
}

/// Tip slot to freeze a fresh scan as-of. dolos exposes the tip
/// as an O(1) cursor-table read, so this is cheap to take once
/// per scan-start.
async fn scan_anchor_slot(dp: &dyn crate::host_fns::DataPlaneFacade) -> wasmtime::Result<u64> {
    let tip = dp
        .tip()
        .await
        .map_err(|e| wasmtime::Error::msg(e.to_string()))?;
    Ok(tip.slot())
}

fn scan_page_to_wit(p: ScanPage) -> WitUtxoPage {
    WitUtxoPage {
        refs: p.refs.into_iter().map(from_dp_ref).collect(),
        anchor_slot: p.anchor_slot,
        next: p.next,
    }
}

fn asset_mint_state_to_wit(s: mitos_data_plane::AssetMintState) -> bindings_v2::AssetMintState {
    bindings_v2::AssetMintState {
        initial_tx: s.initial_tx.map(|h| h.to_vec()),
        initial_slot: s.initial_slot,
        mint_tx_count: s.mint_tx_count,
        metadata_tx: s.metadata_tx.map(|h| h.to_vec()),
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

fn stake_cred_to_wit(c: mitos_data_plane::StakeCred) -> WitStakeCred {
    match c {
        mitos_data_plane::StakeCred::KeyHash(b) => WitStakeCred::KeyHash(b.to_vec()),
        mitos_data_plane::StakeCred::ScriptHash(b) => WitStakeCred::ScriptHash(b.to_vec()),
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
