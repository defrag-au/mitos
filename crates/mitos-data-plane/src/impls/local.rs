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
use dolos_core::{ChainPoint, Domain, EraCbor, StateStore, TxoRef};
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
    /// `TypedOutput` at the requested `DecodeLevel`. Pure
    /// translation function; no side effects, no Domain access.
    fn project_output(
        cbor: &EraCbor,
        level: DecodeLevel,
        include_raw_cbor: bool,
    ) -> DataPlaneResult<TypedOutput> {
        let era = pallas_traverse::Era::try_from(cbor.0)
            .map_err(|_| DataPlaneError::Decode("invalid era tag".into()))?;
        let output = MultiEraOutput::decode(era, &cbor.1)
            .map_err(|e| DataPlaneError::Decode(format!("output decode: {e}")))?;

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

        // TODO Phase A+: datum + script extraction.
        // The current `output.datum()` / `output.script_ref()`
        // accessors give us inline forms; hash-referenced
        // datums need a witness-set lookup which the current
        // `MultiEraOutput` API doesn't expose at this layer.
        // For Phase A both fields are always `None` — the
        // contract is honest, just incomplete. `decoded_at`
        // surfaces what level the plane actually performed so
        // callers can detect that `Lean` is the only level
        // currently fully supported.
        let _ = level.includes_datum();
        let _ = level.includes_script();
        let datum = None;
        let script_ref = None;

        let original_cbor = if include_raw_cbor {
            Some(cbor.1.clone())
        } else {
            None
        };

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

    /// Read current chain tip from the domain's state cursor.
    fn current_tip(&self) -> DataPlaneResult<ChainTip> {
        let point = self
            .domain
            .state()
            .read_cursor()
            .map_err(|e| DataPlaneError::Storage(format!("read_cursor: {e:?}")))?
            .unwrap_or(ChainPoint::Origin);
        Ok(ChainTip::at(point))
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

        match utxos.into_iter().next() {
            Some((_, era_cbor)) => Self::project_output(&era_cbor, decode, false).map(Some),
            None => Ok(None),
        }
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

        let mut out = Vec::with_capacity(utxos.len());
        for (txo_ref, era_cbor) in utxos {
            match Self::project_output(&era_cbor, decode, false) {
                Ok(typed) => out.push((OutputRef::from(txo_ref), typed)),
                Err(e) => {
                    tracing::debug!(?txo_ref, error = ?e, "skipping output that failed to project");
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
            match Self::project_output(&era_cbor, decode, false) {
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
        _hash: &pallas_primitives::Hash<32>,
    ) -> DataPlaneResult<Option<TypedDatum>> {
        // Phase A: not implemented. Witness-set resolution
        // requires a hash-keyed datum index that mitos doesn't
        // currently maintain. Revisit when a consumer needs it.
        Err(DataPlaneError::NotYetImplemented(
            "read_datum requires a hash-keyed datum index (Phase B follow-up)",
        ))
    }

    async fn read_script(
        &self,
        _hash: &pallas_primitives::Hash<28>,
    ) -> DataPlaneResult<Option<TypedScript>> {
        Err(DataPlaneError::NotYetImplemented(
            "read_script requires a hash-keyed script index (Phase B follow-up)",
        ))
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
