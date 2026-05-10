//! Mint/burn indexer.
//!
//! Walks each tx's `mint` field directly and emits typed
//! `Domain::Mint` / `Domain::Burn` events. No script-recognition
//! needed — minting and burning are Cardano primitives, available
//! on every tx via `tx.mints()`.
//!
//! ## Attribution
//!
//! - **Mints** walk the tx's produced outputs to find the
//!   recipient address(es). For each output containing a
//!   minted asset, one `Mint` event is emitted with that
//!   output's quantity. Multi-recipient mints (rare but valid)
//!   produce one event per recipient.
//! - **Burns** resolve the tx's consumed inputs via dolos's
//!   state store to find the previous-owner address(es). For
//!   each consumed input containing a burned asset, one `Burn`
//!   event is emitted with that input's quantity. State lookup
//!   cost is paid only when a burn is detected — pure-mint txs
//!   skip the resolution path entirely.
//!
//! ## Known v1 imprecision
//!
//! For mixed-context txs (e.g. an asset partially minted while
//! also being held in inputs, or a partial burn where some
//! quantity continues forward), the per-output / per-input
//! emission can over-count vs. the net `tx.mint` quantity. The
//! `Domain::AssetMovement` residual pass corrects for the diff
//! at the residual layer; consumers needing exact net quantities
//! can compute from the raw events. Most production mints (NFTs
//! with first-appearance semantics) and burns (full-UTxO
//! consumption) are clean under per-output/per-input attribution.
//!
//! ## Claims
//!
//! Per `docs/design/DOMAIN_REFACTOR.md`, mint-burn-indexer
//! claims nothing — mints and burns aren't movements, and the
//! residual pass's diff logic separately accounts for `tx.mint`
//! quantities so freshly-minted UTxOs and burned inputs don't
//! surface as `AssetMovement`.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use cardano_assets::PolicyId;
use dolos_core::{ChainPoint, Domain, StateStore, TipEvent, TxoRef};
use mitos_core::{Emitter, Indexer};
use mitos_protocol::{
    BurnPayload, Domain as ProtocolDomain, Interest, MintPayload, MovementClaim, ProtocolEvent,
    any_interest_matches_event,
};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};
use tracing::{debug, info, warn};

/// Mint/burn indexer. No state, no per-scope tracking — every tx
/// flows through, emits events when `tx.mint` is non-empty.
#[derive(Debug, Default)]
pub struct MintBurnIndexer;

impl MintBurnIndexer {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self)
    }
}

#[async_trait]
impl<D: dolos_core::Domain> Indexer<D> for MintBurnIndexer {
    /// Per-consumer subscription scope. Same shape as
    /// `marketplace-indexer` — `Vec<Interest>` so consumers can
    /// filter by asset axis (policy, asset, fingerprint) and
    /// domain axis (`Mint(Any)`, `Burn(Any)`, or both via
    /// `DomainSelector::Any`).
    type Scope = Vec<Interest>;
    type Change = ProtocolEvent;

    fn name(&self) -> &'static str {
        "mint-burn"
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        info!("mint-burn-indexer bootstrap (no materialized state; emits forward-only)");
        Ok(ChainPoint::Origin)
    }

    async fn handle_event(
        &mut self,
        domain: &D,
        event: &TipEvent,
        emitter: &Emitter<Self::Change>,
    ) -> anyhow::Result<Vec<MovementClaim>> {
        match event {
            TipEvent::Apply(point, block) => {
                let parsed = match MultiEraBlock::decode(block.as_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "block decode failed; skipping");
                        return Ok(Vec::new());
                    }
                };
                let slot = match point {
                    ChainPoint::Specific(s, _) => *s,
                    ChainPoint::Slot(s) => *s,
                    ChainPoint::Origin => 0,
                };
                for tx in parsed.txs() {
                    if let Err(e) = self.process_tx(domain, &tx, slot, emitter) {
                        debug!(
                            tx_hash = %hex::encode(tx.hash()),
                            error = %e,
                            "mint/burn processing failed; skipping"
                        );
                    }
                }
            }
            // Undo + Mark are auto-emitted by the framework. The
            // indexer holds no per-tx state to revert; consumer-
            // side projections roll back via the framework's
            // Undo signal (remove events by tx_hash).
            TipEvent::Undo(_, _) | TipEvent::Mark(_) => {}
        }
        // No claims — mints/burns aren't movements; the residual
        // pass's diff logic accounts for `tx.mint` quantities
        // separately so freshly-minted outputs and burned inputs
        // don't surface as `AssetMovement`.
        Ok(Vec::new())
    }

    fn routes(&self) -> Router {
        Router::new()
    }

    fn change_matches_scope(scope: &Self::Scope, change: &Self::Change) -> bool {
        any_interest_matches_event(scope, change)
    }

    // `subscribe` / `unsubscribe` keep the trait defaults — no
    // per-scope materialised state.
}

impl MintBurnIndexer {
    /// Process one tx: read mint quantities, partition into
    /// mint vs burn unit sets, then emit per-recipient (mints)
    /// and per-source (burns) events.
    fn process_tx<D: Domain>(
        &self,
        domain: &D,
        tx: &MultiEraTx<'_>,
        slot: u64,
        emitter: &Emitter<ProtocolEvent>,
    ) -> anyhow::Result<()> {
        // Single pass over `tx.mints()` to learn which (policy,
        // asset_name) pairs were minted vs. burned. Empty mint
        // field → fast-path return; the vast majority of txs
        // have no mints/burns.
        let mut mint_units_signed: Vec<(String, i64)> = Vec::new();
        for policy_assets in tx.mints() {
            let policy_hex = hex::encode(policy_assets.policy());
            for asset in policy_assets.assets() {
                let unit = format!("{policy_hex}{}", hex::encode(asset.name()));
                let amount = asset.mint_coin().unwrap_or(0);
                mint_units_signed.push((unit, amount));
            }
        }

        if mint_units_signed.is_empty() {
            return Ok(());
        }

        let tx_hash = hex::encode(tx.hash());

        // Split into mint vs burn unit sets. Per-event amounts
        // come from the matched output / input itself, not from
        // the aggregate `tx.mint` value (see module docs on
        // attribution).
        let mut mint_units: HashSet<&str> = HashSet::new();
        let mut burn_units: HashSet<&str> = HashSet::new();
        for (unit, amount) in &mint_units_signed {
            if *amount > 0 {
                mint_units.insert(unit.as_str());
            } else if *amount < 0 {
                burn_units.insert(unit.as_str());
            }
        }

        if !mint_units.is_empty() {
            self.emit_mints(tx, slot, &tx_hash, &mint_units, emitter);
        }

        if !burn_units.is_empty() {
            self.emit_burns(domain, tx, slot, &tx_hash, &burn_units, emitter)?;
        }

        Ok(())
    }

    /// For each produced output, emit one `Mint` event per
    /// (asset, recipient) pair where the asset matches a minted
    /// unit. Quantity is taken from the output itself.
    fn emit_mints(
        &self,
        tx: &MultiEraTx<'_>,
        slot: u64,
        tx_hash: &str,
        mint_units: &HashSet<&str>,
        emitter: &Emitter<ProtocolEvent>,
    ) {
        for (_idx, output) in tx.produces() {
            let address = match output.address() {
                Ok(a) => a.to_string(),
                Err(e) => {
                    debug!(error = %e, "output address parse failed");
                    continue;
                }
            };
            for policy_assets in output.value().assets() {
                let policy_hex = hex::encode(policy_assets.policy());
                for asset in policy_assets.assets() {
                    let asset_name_hex = hex::encode(asset.name());
                    let unit = format!("{policy_hex}{asset_name_hex}");
                    if !mint_units.contains(unit.as_str()) {
                        continue;
                    }
                    let amount = asset.output_coin().unwrap_or(0);
                    let policy_id = match PolicyId::new(policy_hex.clone()) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    emitter.apply(ProtocolEvent {
                        policy_id,
                        asset_name_hex: Some(asset_name_hex),
                        tx_hash: tx_hash.to_string(),
                        slot,
                        domain: ProtocolDomain::Mint(MintPayload {
                            minter: address.clone(),
                            amount,
                        }),
                    });
                }
            }
        }
    }

    /// Resolve consumed inputs via dolos state, then emit one
    /// `Burn` event per (asset, source) pair where the asset
    /// matches a burned unit. Quantity is taken from the input
    /// itself.
    fn emit_burns<D: Domain>(
        &self,
        domain: &D,
        tx: &MultiEraTx<'_>,
        slot: u64,
        tx_hash: &str,
        burn_units: &HashSet<&str>,
        emitter: &Emitter<ProtocolEvent>,
    ) -> anyhow::Result<()> {
        let consumed_refs: Vec<TxoRef> = tx
            .consumes()
            .into_iter()
            .map(|i| TxoRef(*i.hash(), i.index() as u32))
            .collect();
        if consumed_refs.is_empty() {
            return Ok(());
        }

        let utxo_map = match domain.state().get_utxos(consumed_refs) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = ?e, "burn input state lookup failed; skipping");
                return Ok(());
            }
        };
        // Stable backing for the Arcs the decoded outputs borrow
        // from. Same pattern as `marketplace-indexer::classify_tx`.
        let utxo_pairs: Vec<(TxoRef, Arc<dolos_core::EraCbor>)> = utxo_map.into_iter().collect();

        for (_txo_ref, era_cbor) in &utxo_pairs {
            let era = match pallas::ledger::traverse::Era::try_from(era_cbor.0) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let output = match MultiEraOutput::decode(era, &era_cbor.1) {
                Ok(o) => o,
                Err(_) => continue,
            };
            let address = match output.address() {
                Ok(a) => a.to_string(),
                Err(_) => continue,
            };
            for policy_assets in output.value().assets() {
                let policy_hex = hex::encode(policy_assets.policy());
                for asset in policy_assets.assets() {
                    let asset_name_hex = hex::encode(asset.name());
                    let unit = format!("{policy_hex}{asset_name_hex}");
                    if !burn_units.contains(unit.as_str()) {
                        continue;
                    }
                    let amount = asset.output_coin().unwrap_or(0);
                    let policy_id = match PolicyId::new(policy_hex.clone()) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    emitter.apply(ProtocolEvent {
                        policy_id,
                        asset_name_hex: Some(asset_name_hex),
                        tx_hash: tx_hash.to_string(),
                        slot,
                        domain: ProtocolDomain::Burn(BurnPayload {
                            previous_owner: address.clone(),
                            amount,
                        }),
                    });
                }
            }
        }

        Ok(())
    }
}
