//! `LocalDataPlane` — in-process implementation atop
//! `dolos_core::Domain`.
//!
//! Zero serialisation cost: trait calls dispatch directly to
//! dolos's state store and indexes, results return as typed
//! Rust values. The reference implementation other transports
//! validate against.
//!
//! Phase A scope (this file): point lookups + simple
//! predicate-driven search. Aggregates (`total_supply`,
//! `holder_count`) and full predicate algebra (Not / nested
//! AnyOf+AllOf evaluation against indexes) come later — Phase A
//! ships the surface needed to port the OwnershipIndexer
//! backfill, and that's a single-policy scan.

use async_trait::async_trait;
use cardano_assets::PolicyId;
use dolos_cardano::model::{DATUM_NS, DatumState};
use dolos_core::{
    ArchiveStore, ChainPoint, Domain, EntityKey, EraCbor, IndexStore, StateStore, TxoRef,
};
use pallas::ledger::primitives::PlutusData;
use pallas::ledger::primitives::conway::DatumOption;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, OriginalHash};
use pallas_traverse::MultiEraOutput;

use crate::ChainDataPlane;
use crate::types::{
    AssetEntry, AssetPattern, ChainTip, DataPlaneError, DataPlaneResult, DecodeLevel, OutputRef,
    Page, PageRequest, ScriptLanguage, TypedDatum, TypedOutput, TypedScript, UtxoPattern,
    UtxoPredicate,
};

/// In-process data plane wrapping a `dolos_core::Domain`.
///
/// Holds a reference to the domain for the lifetime of the
/// plane — typical use is to construct one ad-hoc inside a
/// single async fn (e.g. inside an indexer's `subscribe`
/// callback that has `&domain` available). For longer-lived
/// usage, callers can wrap a `&Arc<D>` by dereferencing.
pub struct LocalDataPlane<'a, D: Domain> {
    domain: &'a D,
}

impl<'a, D: Domain> LocalDataPlane<'a, D> {
    pub fn new(domain: &'a D) -> Self {
        Self { domain }
    }

    /// Project a hydrated UTxO (TxoRef + EraCbor) into a
    /// `TypedOutput` at the requested `DecodeLevel`.
    ///
    /// Datum handling per `DecodeLevel`:
    /// - `Lean` — `datum: None` regardless of on-chain shape.
    /// - `WithDatum` / `Full` — extract inline datums directly,
    ///   resolve hash-attached datums via the host's state store
    ///   (`DATUM_NS`) which Dolos populates from witness sets
    ///   during block apply. Caller-blind: the guest sees only
    ///   the resolved bytes, never the inline-vs-hash source.
    fn project_output(
        &self,
        cbor: &EraCbor,
        level: DecodeLevel,
        include_raw_cbor: bool,
    ) -> DataPlaneResult<TypedOutput> {
        let era = pallas_traverse::Era::try_from(cbor.0)
            .map_err(|_| DataPlaneError::Decode("invalid era tag".into()))?;
        let output = MultiEraOutput::decode(era, &cbor.1)
            .map_err(|e| DataPlaneError::Decode(format!("output decode: {e}")))?;
        let original_cbor = if include_raw_cbor {
            Some(cbor.1.clone())
        } else {
            None
        };
        self.project_multi_era_output(&output, level, original_cbor)
    }

    /// Build a `TypedOutput` from an already-decoded `MultiEraOutput`.
    /// Used both by the era-cbor entry (`project_output`) and the
    /// archive-fallback path (`read_utxo_from_archive`), which has a
    /// `MultiEraOutput` in hand from decoding the historical block.
    fn project_multi_era_output(
        &self,
        output: &MultiEraOutput<'_>,
        level: DecodeLevel,
        original_cbor: Option<Vec<u8>>,
    ) -> DataPlaneResult<TypedOutput> {
        let address = output
            .address()
            .map_err(|e| DataPlaneError::Decode(format!("address parse: {e}")))?
            .to_string();

        let lovelace = output.value().coin();

        // Collect inner per-policy entries first (the iterator
        // borrows from `policy_assets`, which gets consumed by
        // the outer iterator). Slightly less elegant than chained
        // `.flat_map` but borrow-checker happy.
        let mut assets: Vec<AssetEntry> = Vec::new();
        for policy_assets in output.value().assets() {
            let policy_hex = hex::encode(policy_assets.policy());
            let Ok(policy_id) = PolicyId::new(&policy_hex) else {
                continue;
            };
            for a in policy_assets.assets() {
                let asset_name_hex = hex::encode(a.name());
                // `output_coin` for native tokens is positive
                // (we're looking at outputs, not minting deltas).
                let qty = a.output_coin().unwrap_or_default();
                assets.push(AssetEntry {
                    policy_id: policy_id.clone(),
                    asset_name_hex,
                    quantity: qty,
                });
            }
        }

        let datum = if level.includes_datum() {
            self.resolve_output_datum(output)
        } else {
            None
        };
        let _ = level.includes_script();
        let script_ref = None;

        Ok(TypedOutput {
            address,
            lovelace,
            assets,
            datum,
            script_ref,
            original_cbor,
            decoded_at: level,
        })
    }

    /// Fallback path for `read_utxo` / `read_utxos` when the current-
    /// state lookup misses. Walks Dolos's archive: tx-hash → slot →
    /// block CBOR → decode → find tx → pick output by index.
    ///
    /// This is the path that lets the dispatcher resolve prior
    /// outputs for inputs being consumed in the very block that's
    /// being applied — by the time per-module dispatch runs, those
    /// UTxOs have already been removed from current state, but they
    /// still live in the archive's block body. Without this fallback
    /// the dispatcher's `consumed` event filter silently drops every
    /// same-block consumption, which is the entire point of running
    /// a follower-mode indexer in the first place.
    ///
    /// Heavier than `state.get_utxos` (decodes a whole block per
    /// miss), so only worth invoking when the cheap state lookup
    /// already came back empty.
    async fn read_utxo_from_archive(
        &self,
        oref: &OutputRef,
        level: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        let slot = self
            .domain
            .indexes()
            .slot_by_tx_hash(oref.tx_hash.as_slice())
            .map_err(|e| DataPlaneError::Storage(format!("slot_by_tx_hash: {e:?}")))?;
        let Some(slot) = slot else {
            return Ok(None);
        };
        let block_body = self
            .domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|e| DataPlaneError::Storage(format!("get_block_by_slot: {e:?}")))?;
        let Some(block_body) = block_body else {
            return Ok(None);
        };
        let block = MultiEraBlock::decode(block_body.as_slice())
            .map_err(|e| DataPlaneError::Decode(format!("MultiEraBlock decode: {e}")))?;
        let tx = block
            .txs()
            .into_iter()
            .find(|t| t.hash().as_slice() == oref.tx_hash.as_slice());
        let Some(tx) = tx else {
            return Ok(None);
        };
        let outputs = tx.outputs();
        let Some(output) = outputs.get(oref.index as usize) else {
            return Ok(None);
        };
        let typed = self.project_multi_era_output(output, level, None)?;
        Ok(Some(typed))
    }

    /// Caller-blind datum resolution for one output.
    ///
    /// - `None` if the output has no datum on chain.
    /// - `Some(TypedDatum { hash, payload, original_cbor })`
    ///   when a datum exists. `payload` and `original_cbor` are
    ///   populated when the plane could resolve the bytes (always
    ///   for inline; for hash-attached when present in
    ///   `DATUM_NS`). `payload` falls back to `None` if the bytes
    ///   exist but minicbor decode fails (rare; logged debug).
    fn resolve_output_datum(&self, output: &MultiEraOutput<'_>) -> Option<TypedDatum> {
        let datum_opt = output.datum()?;
        let (hash, bytes): (pallas_primitives::Hash<32>, Option<Vec<u8>>) = match datum_opt {
            DatumOption::Data(cbor_wrap) => {
                let raw = cbor_wrap.0.raw_cbor().to_vec();
                let h = cbor_wrap.0.original_hash();
                (h, Some(raw))
            }
            DatumOption::Hash(h) => {
                let key = EntityKey::from(h);
                let bytes = self
                    .domain
                    .state()
                    .read_entity_typed::<DatumState>(DATUM_NS, &key)
                    .ok()
                    .flatten()
                    .map(|s| s.bytes);
                (h, bytes)
            }
        };

        let payload = bytes.as_ref().and_then(|b| {
            match pallas::codec::minicbor::decode::<PlutusData>(b) {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::debug!(?hash, error = %e, "datum payload minicbor decode failed");
                    None
                }
            }
        });

        Some(TypedDatum {
            hash,
            payload,
            original_cbor: bytes,
        })
    }

    /// Read current chain tip from the domain's state cursor.
    /// `read_cursor` surfaces a `dolos_core::ChainPoint`; we
    /// convert at this boundary into mitos's owned type before
    /// handing back to the data-plane caller.
    fn current_tip(&self) -> DataPlaneResult<ChainTip> {
        let point = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| DataPlaneError::Storage(format!("read_cursor: {e:?}")))?
            .unwrap_or(ChainPoint::Origin);
        Ok(ChainTip::at(point.into()))
    }
}

#[async_trait]
impl<D: Domain> ChainDataPlane for LocalDataPlane<'_, D> {
    async fn read_utxo(
        &self,
        oref: &OutputRef,
        decode: DecodeLevel,
    ) -> DataPlaneResult<Option<TypedOutput>> {
        let txo_ref: TxoRef = (*oref).into();
        let utxos = self
            .domain
            .state()
            .get_utxos(vec![txo_ref])
            .map_err(|e| DataPlaneError::Storage(format!("get_utxos: {e:?}")))?;

        if let Some((_, era_cbor)) = utxos.into_iter().next() {
            return self.project_output(&era_cbor, decode, false).map(Some);
        }
        // Archive fallback — see `read_utxo_from_archive` doc.
        self.read_utxo_from_archive(oref, decode).await
    }

    async fn read_utxos(
        &self,
        orefs: &[OutputRef],
        decode: DecodeLevel,
    ) -> DataPlaneResult<Vec<(OutputRef, TypedOutput)>> {
        let txo_refs: Vec<TxoRef> = orefs.iter().map(|o| (*o).into()).collect();
        let utxos = self
            .domain
            .state()
            .get_utxos(txo_refs)
            .map_err(|e| DataPlaneError::Storage(format!("get_utxos: {e:?}")))?;

        let mut out = Vec::with_capacity(orefs.len());
        let mut found: std::collections::HashSet<(pallas_primitives::Hash<32>, u32)> =
            std::collections::HashSet::new();
        for (txo_ref, era_cbor) in utxos {
            match self.project_output(&era_cbor, decode, false) {
                Ok(typed) => {
                    let oref = OutputRef::from(txo_ref);
                    found.insert((oref.tx_hash, oref.index));
                    out.push((oref, typed));
                }
                Err(e) => {
                    tracing::debug!(?txo_ref, error = ?e, "skipping output that failed to project");
                }
            }
        }
        // Archive fallback for refs the current-state lookup didn't
        // find — typically outputs consumed in the same block whose
        // events we're building. Per-miss block decode, so the cost
        // only applies when the cheap path missed.
        for oref in orefs {
            if found.contains(&(oref.tx_hash, oref.index)) {
                continue;
            }
            match self.read_utxo_from_archive(oref, decode).await {
                Ok(Some(typed)) => out.push((*oref, typed)),
                Ok(None) => {
                    tracing::debug!(?oref, "archive fallback: utxo not found");
                }
                Err(e) => {
                    tracing::debug!(?oref, error = %e, "archive fallback: lookup failed");
                }
            }
        }
        Ok(out)
    }

    async fn search_utxos(
        &self,
        predicate: &UtxoPredicate,
        decode: DecodeLevel,
        page: PageRequest,
    ) -> DataPlaneResult<Page<(OutputRef, TypedOutput)>> {
        // Phase A scope: only the simple predicate shapes that
        // map directly to dolos's existing indexes.
        // Specifically, `UtxoPredicate::Match(UtxoPattern { asset:
        // Some(AssetPattern::Policy(p)), ... })` — the case the
        // OwnershipIndexer backfill needs. Other shapes return
        // `NotYetImplemented` — the predicate algebra is sound;
        // the index-driven query planning isn't built out yet.
        let policy = match predicate {
            UtxoPredicate::Match(UtxoPattern {
                asset: Some(AssetPattern::Policy(p)),
                address: None,
                output_ref: None,
            }) => p,
            _ => {
                return Err(DataPlaneError::NotYetImplemented(
                    "Phase A search_utxos supports only `Match(UtxoPattern { asset: Some(Policy(_)), .. })`",
                ));
            }
        };

        // Resolve via dolos's by-policy index.
        let policy_bytes = policy.as_bytes().map_err(|e| {
            DataPlaneError::InvalidRequest(format!("policy id not valid hex: {e:?}"))
        })?;

        // dolos's CardanoIndexExt is needed for this. For Phase
        // A the LocalDataPlane caller is mitos's ownership
        // indexer which already has access — we expose it via
        // the trait constraint on D.
        // Note: this is one place where Phase A is leaning on
        // mitos-internal details; future Phase B+ should expose
        // a more generic index abstraction.
        use dolos_cardano::indexes::CardanoIndexExt;
        let utxo_set = self
            .domain
            .indexes()
            .utxos_by_policy(&policy_bytes)
            .map_err(|e| DataPlaneError::Storage(format!("utxos_by_policy: {e:?}")))?;

        // Phase A pagination: in-memory (read whole set, slice).
        // For collections in the millions this would need a
        // proper cursor; for typical PFP collections (~10K
        // outputs) it's fine. Hard-cap the result at the
        // configurable max so we don't accidentally return
        // 100k items if this is misused.
        let cap = (page.max_items as usize).min(1000);
        let txo_refs: Vec<TxoRef> = utxo_set.into_iter().take(cap).collect();

        if txo_refs.is_empty() {
            let tip = self.current_tip()?;
            return Ok(Page::empty(tip));
        }

        let utxo_map = self
            .domain
            .state()
            .get_utxos(txo_refs.clone())
            .map_err(|e| DataPlaneError::Storage(format!("get_utxos: {e:?}")))?;

        let mut items = Vec::with_capacity(utxo_map.len());
        for (txo_ref, era_cbor) in utxo_map {
            match self.project_output(&era_cbor, decode, false) {
                Ok(typed) => items.push((OutputRef::from(txo_ref), typed)),
                Err(e) => {
                    tracing::debug!(?txo_ref, error = ?e, "skipping output that failed to project");
                }
            }
        }

        let tip = self.current_tip()?;

        // Phase A: no pagination tokens (whole set fit in one
        // page or was capped). Phase B+ adds proper cursor
        // encoding.
        Ok(Page {
            items,
            next_token: None,
            tip,
        })
    }

    async fn utxos_by_address(&self, address: &str) -> DataPlaneResult<Vec<OutputRef>> {
        use dolos_cardano::indexes::CardanoIndexExt;

        // Bech32 → raw address bytes. Dolos's per-address index
        // is keyed on the binary address, not the bech32 string.
        // `pallas_addresses::Address::from_bech32` handles both
        // mainnet (`addr1...`) and testnet (`addr_test1...`)
        // forms, plus Byron (`Ae2.../DdzFF...`) which round-trip
        // through `to_vec()` to their canonical encoding too.
        let addr = pallas_addresses::Address::from_bech32(address).map_err(|e| {
            DataPlaneError::InvalidRequest(format!("address not bech32: {e:?}"))
        })?;
        let addr_bytes = addr.to_vec();

        let utxo_set = self
            .domain
            .indexes()
            .utxos_by_address(&addr_bytes)
            .map_err(|e| DataPlaneError::Storage(format!("utxos_by_address: {e:?}")))?;

        // Cap at 100K refs to bound host-side memory; addresses
        // with more UTxOs warrant the predicate-based
        // `search_utxos` flow with proper pagination (Phase B+).
        const HARD_CAP: usize = 100_000;
        let total: Vec<TxoRef> = utxo_set.into_iter().collect();
        if total.len() > HARD_CAP {
            tracing::warn!(
                address = %address,
                returned = HARD_CAP,
                total = total.len(),
                "utxos_by_address result truncated at hard cap"
            );
        }
        Ok(total.into_iter().take(HARD_CAP).map(OutputRef::from).collect())
    }

    async fn read_datum(
        &self,
        hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TypedDatum>> {
        // Dolos maintains a refcounted hash-keyed datum index in
        // its state store under the `DATUM_NS` namespace,
        // populated from witness sets during block apply (see
        // `dolos-cardano/src/roll/datums.rs`). Inline datums are
        // *not* in this index — they're carried on the output
        // itself and surfaced by `project_output` directly.
        //
        // This side-door is for callers that have a hash but no
        // containing UTxO context (rare). Most consumers should
        // call `read_utxo*` with `DecodeLevel::WithDatum` and let
        // the plane do caller-blind resolution transparently.
        let key = EntityKey::from(*hash);
        let bytes = self
            .domain
            .state()
            .read_entity_typed::<DatumState>(DATUM_NS, &key)
            .map_err(|e| DataPlaneError::Storage(format!("read_entity_typed(datum): {e:?}")))?
            .map(|s| s.bytes);
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let payload = match pallas::codec::minicbor::decode::<PlutusData>(&bytes) {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::debug!(?hash, error = %e, "datum payload minicbor decode failed");
                None
            }
        };
        Ok(Some(TypedDatum {
            hash: *hash,
            payload,
            original_cbor: Some(bytes),
        }))
    }

    async fn read_script(
        &self,
        _hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<TypedScript>> {
        Err(DataPlaneError::NotYetImplemented(
            "read_script requires a hash-keyed script index (Phase B follow-up)",
        ))
    }

    async fn read_block(&self, slot: u64) -> DataPlaneResult<Option<Vec<u8>>> {
        // Direct archive lookup — same call the `read_tx` /
        // `tx_metadata` paths use, just exposed at the block
        // granularity for admin fixture export.
        self.domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|e| DataPlaneError::Storage(format!("get_block_by_slot: {e:?}")))
    }

    async fn slot_by_tx_hash(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<u64>> {
        self.domain
            .indexes()
            .slot_by_tx_hash(tx_hash.as_slice())
            .map_err(|e| DataPlaneError::Storage(format!("slot_by_tx_hash: {e:?}")))
    }

    async fn tx_metadata(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<Vec<u8>>> {
        // Dolos's tx index gives us the slot; the archive
        // holds the block CBOR. Decode the block via pallas,
        // find the matching tx by hash, return its
        // auxiliary_data CBOR. Sync I/O — both lookups are
        // in-process redb reads.
        let slot = self
            .domain
            .indexes()
            .slot_by_tx_hash(tx_hash.as_slice())
            .map_err(|e| DataPlaneError::Storage(format!("slot_by_tx_hash: {e:?}")))?;
        let Some(slot) = slot else {
            return Ok(None);
        };
        let block_body = self
            .domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|e| DataPlaneError::Storage(format!("get_block_by_slot: {e:?}")))?;
        let Some(block_body) = block_body else {
            return Ok(None);
        };
        let block = MultiEraBlock::decode(block_body.as_slice()).map_err(|e| {
            DataPlaneError::Decode(format!("MultiEraBlock decode: {e}"))
        })?;
        let tx = block
            .txs()
            .into_iter()
            .find(|t| t.hash().as_slice() == tx_hash.as_slice());
        let Some(tx) = tx else {
            return Ok(None);
        };
        Ok(extract_aux_cbor(&tx))
    }

    async fn read_tx(
        &self,
        tx_hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<crate::types::TxRecord>> {
        // Same archive lookup as `tx_metadata` — slot index +
        // block-by-slot + decode + find by hash.
        let slot = self
            .domain
            .indexes()
            .slot_by_tx_hash(tx_hash.as_slice())
            .map_err(|e| DataPlaneError::Storage(format!("slot_by_tx_hash: {e:?}")))?;
        let Some(slot) = slot else {
            return Ok(None);
        };
        let block_body = self
            .domain
            .archive()
            .get_block_by_slot(&slot)
            .map_err(|e| DataPlaneError::Storage(format!("get_block_by_slot: {e:?}")))?;
        let Some(block_body) = block_body else {
            return Ok(None);
        };
        let block = MultiEraBlock::decode(block_body.as_slice()).map_err(|e| {
            DataPlaneError::Decode(format!("MultiEraBlock decode: {e}"))
        })?;
        let block_hash = block.hash();
        let cursor = crate::types::ChainPoint::Specific(block.slot(), block_hash);

        // Find tx + capture its index in the block.
        let mut tx_idx_found: Option<u32> = None;
        let mut tx_found: Option<MultiEraTx<'_>> = None;
        for (idx, tx) in block.txs().into_iter().enumerate() {
            if tx.hash().as_slice() == tx_hash.as_slice() {
                tx_idx_found = Some(idx as u32);
                tx_found = Some(tx);
                break;
            }
        }
        let (Some(tx_idx), Some(tx)) = (tx_idx_found, tx_found) else {
            return Ok(None);
        };

        // Project the tx into the typed rollup. Prior outputs
        // for inputs are looked up via current-state UTxOs;
        // already-spent inputs return `None` (per the
        // `TxRecord` doc comment's resolution caveat).
        Ok(Some(project_tx_record(&tx, tx_idx, cursor, self).await?))
    }

    async fn total_supply(
        &self,
        _policy: &PolicyId,
        _asset_name_hex: Option<&str>,
    ) -> DataPlaneResult<u64> {
        // Phase A: not implemented. Total supply is
        // mint-history-driven, not current-state derived;
        // requires a different index (per-policy mint sum).
        Err(DataPlaneError::NotYetImplemented(
            "total_supply requires a mint-aggregation index (Phase B follow-up)",
        ))
    }

    async fn holder_count(&self, policy: &PolicyId) -> DataPlaneResult<u64> {
        // Phase A: do this the slow way — enumerate all UTxOs
        // for the policy, count distinct addresses. Acceptable
        // for the small policies we care about; not for popular
        // ones. Phase B+ adds an aggregation index.
        use dolos_cardano::indexes::CardanoIndexExt;
        let policy_bytes = policy.as_bytes().map_err(|e| {
            DataPlaneError::InvalidRequest(format!("policy id not valid hex: {e:?}"))
        })?;
        let utxo_set = self
            .domain
            .indexes()
            .utxos_by_policy(&policy_bytes)
            .map_err(|e| DataPlaneError::Storage(format!("utxos_by_policy: {e:?}")))?;
        let txo_refs: Vec<TxoRef> = utxo_set.into_iter().collect();
        if txo_refs.is_empty() {
            return Ok(0);
        }
        let utxo_map = self
            .domain
            .state()
            .get_utxos(txo_refs)
            .map_err(|e| DataPlaneError::Storage(format!("get_utxos: {e:?}")))?;

        let mut addresses = std::collections::HashSet::new();
        for (_, era_cbor) in utxo_map {
            if let Ok(era) = pallas_traverse::Era::try_from(era_cbor.0)
                && let Ok(output) = MultiEraOutput::decode(era, &era_cbor.1)
                && let Ok(addr) = output.address()
            {
                addresses.insert(addr.to_string());
            }
        }
        Ok(addresses.len() as u64)
    }

    async fn tip(&self) -> DataPlaneResult<ChainTip> {
        self.current_tip()
    }

    async fn protocol_params(&self) -> DataPlaneResult<crate::types::ProtocolParameters> {
        // Phase A: not implemented. Pulling current ProtocolParams
        // from dolos requires walking the era summaries / governance
        // state; deferred until a consumer needs it.
        Err(DataPlaneError::NotYetImplemented(
            "protocol_params requires era-state walking (Phase B follow-up)",
        ))
    }
}

// Suppress unused-warning for the placeholder enum variants
// during Phase A — they're part of the public API but the local
// impl doesn't yet exercise all of them.
#[allow(dead_code)]
const _: () = {
    let _ = ScriptLanguage::Native;
    let _ = ScriptLanguage::PlutusV1;
    let _ = ScriptLanguage::PlutusV2;
    let _ = ScriptLanguage::PlutusV3;
};

/// Pull the raw auxiliary-data CBOR out of a `MultiEraTx`.
/// `MultiEraTx::aux_data()` is `pub(crate)` upstream, so we
/// pattern-match on the variant ourselves and access the
/// public `auxiliary_data` field. Each era's `KeepRaw` wrapper
/// preserves the original CBOR bytes via `raw_cbor()`.
///
/// Re-exported via `mitos_data_plane::extract_aux_cbor` so
/// fixture-driven test harnesses (mitos-run) can pre-harvest
/// aux-data from `--block` arguments without re-implementing
/// the era pattern match.
pub fn extract_aux_cbor(tx: &MultiEraTx<'_>) -> Option<Vec<u8>> {
    use pallas_codec::utils::Nullable;
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
        MultiEraTx::Byron(_) => None,
        // `MultiEraTx` is `#[non_exhaustive]`. Future eras land
        // here until we add matching arms — return None defensively
        // so an unknown era doesn't break aux-data lookups.
        _ => None,
    }
}

/// Project a `MultiEraTx` into the typed `TxRecord`. Inputs +
/// reference inputs get a best-effort prior-output lookup
/// against current state via `read_utxo`; already-spent inputs
/// surface as `prior_output: None`.
async fn project_tx_record<'a, D: dolos_core::Domain>(
    tx: &MultiEraTx<'a>,
    tx_idx: u32,
    cursor: crate::types::ChainPoint,
    plane: &LocalDataPlane<'_, D>,
) -> DataPlaneResult<crate::types::TxRecord> {
    let tx_hash = tx.hash();

    // Collect input refs first so we can do bulk lookups.
    let input_refs: Vec<(OutputRef, Option<Vec<u8>>)> = tx
        .consumes()
        .iter()
        .enumerate()
        .map(|(input_order, input)| {
            let redeemer = tx
                .find_spend_redeemer(input_order as u32)
                .map(|r| r.encode());
            (
                OutputRef::new(*input.hash(), input.index() as u32),
                redeemer,
            )
        })
        .collect();
    let ref_input_refs: Vec<OutputRef> = tx
        .reference_inputs()
        .iter()
        .map(|i| OutputRef::new(*i.hash(), i.index() as u32))
        .collect();

    // Snapshot this TX's witness-set datums so we can backfill
    // `prior_datum.original_cbor` for hash-datum prior outputs
    // the data plane couldn't resolve directly (e.g. jpg.store's
    // metadata-encoded CO datums revealed only on cancel/accept).
    // Mirrors `block_events::project_tx`.
    let witness_datums: Vec<(pallas_primitives::Hash<32>, Vec<u8>)> = tx
        .plutus_data()
        .iter()
        .map(|d| (d.original_hash(), d.raw_cbor().to_vec()))
        .collect();

    // Best-effort prior-output resolution. Loop per-input
    // rather than `read_utxos` because the bulk method
    // silently omits unresolved entries; we want explicit
    // None for absent inputs.
    let mut inputs = Vec::with_capacity(input_refs.len());
    for (oref, redeemer) in input_refs {
        let prior = ChainDataPlane::read_utxo(plane, &oref, DecodeLevel::WithDatum)
            .await
            .unwrap_or(None);
        let (prior_output, prior_datum) = match prior {
            Some(mut o) => {
                if let Some(d) = o.datum.as_mut() {
                    d.fill_from_witness(&witness_datums);
                }
                let datum = o.datum.clone();
                (Some(o), datum)
            }
            None => (None, None),
        };
        inputs.push(crate::types::ConsumedInput {
            oref,
            prior_output,
            prior_datum,
            redeemer,
        });
    }

    let mut reference_inputs = Vec::with_capacity(ref_input_refs.len());
    for oref in ref_input_refs {
        let prior = ChainDataPlane::read_utxo(plane, &oref, DecodeLevel::WithDatum)
            .await
            .unwrap_or(None);
        let (prior_output, prior_datum) = match prior {
            Some(mut o) => {
                if let Some(d) = o.datum.as_mut() {
                    d.fill_from_witness(&witness_datums);
                }
                let datum = o.datum.clone();
                (Some(o), datum)
            }
            None => (None, None),
        };
        reference_inputs.push(crate::types::ReferencedInput {
            oref,
            prior_output,
            prior_datum,
        });
    }

    // Outputs go through the existing per-output projection.
    // Eras differ on output CBOR shape; we re-encode the era-
    // specific output to bytes via the multi-era variant's
    // `encode()` (via project_output's existing path).
    let outputs: Vec<TypedOutput> = tx
        .outputs()
        .iter()
        .map(|o| project_typed_output(o))
        .collect();

    // Mints from the multi-asset field.
    let mint: Vec<crate::types::MintEntry> = tx
        .mints()
        .iter()
        .flat_map(|policy_assets| {
            let policy = match cardano_assets::PolicyId::new(hex::encode(
                policy_assets.policy().as_slice(),
            )) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "skip mint with invalid policy id");
                    return Vec::new();
                }
            };
            policy_assets
                .assets()
                .iter()
                .map(|asset| crate::types::MintEntry {
                    policy: policy.clone(),
                    asset_name: asset.name().to_vec(),
                    quantity_delta: asset
                        .any_coin()
                        .clamp(i64::MIN as i128, i64::MAX as i128)
                        as i64,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let required_signers: Vec<pallas_primitives::Hash<28>> = tx
        .required_signers()
        .collect::<Vec<&pallas_primitives::Hash<28>>>()
        .into_iter()
        .copied()
        .collect();

    let validity_interval = crate::types::ValidityInterval {
        valid_from: tx.validity_start(),
        valid_to: tx.ttl(),
    };

    let aux_data = extract_aux_cbor(tx);

    Ok(crate::types::TxRecord {
        tx_hash,
        tx_idx,
        cursor,
        inputs,
        reference_inputs,
        outputs,
        mint,
        required_signers,
        validity_interval,
        aux_data,
    })
}

/// Lean projection of a `MultiEraOutput` into mitos's owned
/// `TypedOutput`. Used by `project_tx_record`'s output walk.
/// Same shape `read_utxos(_, Lean)` already produces, hand-
/// rolled here to avoid an extra data-plane round-trip when
/// we already have the raw `MultiEraOutput` in scope.
fn project_typed_output(output: &pallas_traverse::MultiEraOutput<'_>) -> TypedOutput {
    let address = match output.address() {
        Ok(addr) => addr.to_string(),
        Err(_) => String::new(),
    };
    let value = output.value();
    let lovelace = value.coin();
    let assets: Vec<AssetEntry> = value
        .assets()
        .iter()
        .flat_map(|policy_assets| {
            let policy = match cardano_assets::PolicyId::new(hex::encode(
                policy_assets.policy().as_slice(),
            )) {
                Ok(p) => p,
                Err(_) => return Vec::new(),
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
    TypedOutput {
        address,
        lovelace,
        assets,
        datum: None,
        script_ref: None,
        original_cbor: None,
        decoded_at: DecodeLevel::Lean,
    }
}
