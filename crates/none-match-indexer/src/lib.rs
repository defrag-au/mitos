//! Residual `none_match-indexer` — emits `Domain::AssetMovement`
//! for asset transfers that no specific-domain indexer claimed.
//!
//! Reserved name. The dispatcher's residual pass (see
//! `docs/design/DOMAIN_REFACTOR.md` "Dispatch mechanism") looks
//! for an indexer registered with this name and runs it AFTER
//! all specific-domain indexers have processed a tx, with the
//! accumulated `MovementClaim`s available via the shared
//! `TxClaimCoordinator`.
//!
//! ## What it emits
//!
//! For each tx with asset transfers between addresses:
//!
//! - Walks `tx.consumes()` (resolved via dolos state) to find
//!   which addresses held which assets in inputs
//! - Walks `tx.produces()` to find which addresses receive which
//!   assets in outputs
//! - For each (asset, output_address) pair where the asset is
//!   also in some input, emits `Domain::AssetMovement` from the
//!   first matching input address — UNLESS:
//!   - same-wallet rule matches (HD-wallet change → suppress)
//!   - the `(asset, from, to)` triplet is already claimed by a
//!     specific-domain indexer (Sale, ListingCreate, etc.)
//!
//! ## What it doesn't emit
//!
//! - Pure mint outputs (asset doesn't appear in any input — that's
//!   `mint-burn-indexer`'s responsibility)
//! - Self-transfers (HD-wallet change UTxOs going back to the
//!   same wallet, per the `same_wallet` suppression rule)
//! - Movements claimed by specific-domain indexers (Sale,
//!   Listing*, Unlisting, OfferAccept)
//! - Lovelace flow (out of scope — only native assets)
//!
//! ## Known v1 imprecision
//!
//! Per-output emission with first-matching-input attribution.
//! Multi-source / multi-destination txs of the same asset can
//! be ambiguously attributed (Cardano's UTxO model has no
//! canonical "this input's tokens went to that output" mapping).
//! Most production transfers (1 input → 1 output for an NFT;
//! marketplace sales fully claimed by the marketplace indexer)
//! are clean under this model.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use cardano_assets::AssetId;
use dolos_core::{ChainPoint, Domain, StateStore, TipEvent, TxoRef};
use mitos_core::helpers::same_wallet;
use mitos_core::{Emitter, Indexer, TxClaimCoordinator};
use mitos_protocol::{
    AssetMovementPayload, Domain as ProtocolDomain, Interest, MovementClaim, ProtocolEvent,
    any_interest_matches_event,
};
use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraTx};
use tracing::{debug, info, warn};

/// Reserved indexer name. The dispatcher recognises this name to
/// route residual-pass dispatch (see
/// `docs/design/DOMAIN_REFACTOR.md`).
pub const NONE_MATCH_INDEXER_NAME: &str = "none-match";

/// Residual indexer. Holds a clone of the per-Apply
/// `TxClaimCoordinator` so its `handle_event` can read the claim
/// set without needing extra parameters in the trait.
#[derive(Debug)]
pub struct NoneMatchIndexer {
    coordinator: TxClaimCoordinator,
}

impl NoneMatchIndexer {
    pub fn new(coordinator: TxClaimCoordinator) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<D: dolos_core::Domain> Indexer<D> for NoneMatchIndexer {
    type Scope = Vec<Interest>;
    type Change = ProtocolEvent;

    fn name(&self) -> &'static str {
        NONE_MATCH_INDEXER_NAME
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        info!("none-match-indexer bootstrap (residual emitter; forward-only)");
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
                            "residual processing failed; skipping"
                        );
                    }
                }
            }
            // Undo / Mark are auto-emitted by the framework. The
            // residual pass holds no per-tx state to revert.
            TipEvent::Undo(_, _) | TipEvent::Mark(_) => {}
        }
        // Residual itself never claims — it's the consumer of
        // claims, not a producer.
        Ok(Vec::new())
    }

    fn routes(&self) -> Router {
        Router::new()
    }

    fn change_matches_scope(scope: &Self::Scope, change: &Self::Change) -> bool {
        any_interest_matches_event(scope, change)
    }
}

impl NoneMatchIndexer {
    fn process_tx<D: Domain>(
        &self,
        domain: &D,
        tx: &MultiEraTx<'_>,
        slot: u64,
        emitter: &Emitter<ProtocolEvent>,
    ) -> anyhow::Result<()> {
        // No inputs = no asset movements (mint-only tx, handled
        // by mint-burn-indexer).
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
                debug!(error = ?e, "input state lookup failed; skipping");
                return Ok(());
            }
        };
        let utxo_pairs: Vec<(TxoRef, Arc<dolos_core::EraCbor>)> = utxo_map.into_iter().collect();

        // Build map: unit (policy_hex + asset_name_hex) → Vec<(input_addr,
        // input_amount)>. One entry per (asset, input_holder) pair.
        let mut input_holders: HashMap<String, Vec<String>> = HashMap::new();
        for (_txo_ref, era_cbor) in &utxo_pairs {
            let era = match Era::try_from(era_cbor.0) {
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
                    input_holders.entry(unit).or_default().push(address.clone());
                }
            }
        }

        if input_holders.is_empty() {
            return Ok(());
        }

        let tx_hash = hex::encode(tx.hash());

        // Walk produced outputs; for each output asset that's also
        // held in some input, emit `AssetMovement` from the first
        // non-same-wallet, non-claimed input — unless suppressed.
        for (_idx, output) in tx.produces() {
            let to_addr = match output.address() {
                Ok(a) => a.to_string(),
                Err(_) => continue,
            };
            for policy_assets in output.value().assets() {
                let policy_hex = hex::encode(policy_assets.policy());
                for asset in policy_assets.assets() {
                    let asset_name_hex = hex::encode(asset.name());
                    let unit = format!("{policy_hex}{asset_name_hex}");
                    let Some(holders) = input_holders.get(&unit) else {
                        // Not in any input → fresh mint output,
                        // mint-burn-indexer handles this case.
                        continue;
                    };
                    let output_amount = asset.output_coin().unwrap_or(0);
                    let asset_id = match AssetId::new(policy_hex.clone(), asset_name_hex.clone()) {
                        Ok(a) => a,
                        Err(_) => continue,
                    };
                    let policy_id = match asset_id.policy_id_typed() {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // First non-same-wallet, non-claimed input wins.
                    // Pre-v1 design choice: per-output emission with
                    // first-input attribution. See module docs.
                    for from_addr in holders {
                        if same_wallet(from_addr, &to_addr) {
                            continue;
                        }
                        let claim = MovementClaim {
                            asset: asset_id.clone(),
                            from: from_addr.clone(),
                            to: to_addr.clone(),
                        };
                        if self.coordinator.contains(&claim) {
                            // Already classified by a specific-
                            // domain indexer (Sale, Listing*, etc.).
                            continue;
                        }
                        emitter.apply(ProtocolEvent {
                            policy_id: policy_id.clone(),
                            asset_name_hex: Some(asset_name_hex.clone()),
                            tx_hash: tx_hash.clone(),
                            slot,
                            domain: ProtocolDomain::AssetMovement(AssetMovementPayload {
                                previous_owner: from_addr.clone(),
                                new_owner: to_addr.clone(),
                                amount: output_amount,
                            }),
                        });
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}
