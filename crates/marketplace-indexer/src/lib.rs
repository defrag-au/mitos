//! Marketplace events indexer (sales, listings, offers).
//!
//! This is the second mitos indexer, alongside `OwnershipIndexer`,
//! but with a fundamentally different topology. Where ownership is
//! a per-policy stream (one consumer per policy, scope-filtered at
//! the indexer), marketplace events are emitted **as a single
//! global feed** — every marketplace tx on chain produces a record
//! regardless of policy.
//!
//! The CF-side worker subscribes once, receives every event, and
//! is responsible for per-policy fan-out: extract `policy_id` from
//! each event, route to a per-policy DO via `idFromName(policy_id)`.
//! For policies no consumer cares about, the worker drops the
//! event (or routes to a "missing" DO that 404s on read). The
//! marketplace indexer doesn't try to know which policies anyone
//! is watching; that's the consumer worker's concern.
//!
//! This shape avoids the awkwardness of "you have to subscribe to
//! every policy you might care about ahead of time" — sales
//! happen across thousands of policies, and pre-registering all of
//! them is the wrong default. Instead: emit everything, route
//! cheaply.
//!
//! Design + rationale: see `mitos/docs/design/MARKETPLACE_INDEXER.md`.
//!
//! Phase 9 (this crate, current state): skeleton.
//!
//! - `Scope = ()` — no per-consumer scope. The single subscriber
//!   gets everything.
//! - `handle_event(Apply)` walks each tx in the block, resolves
//!   its consumed inputs via `domain.state().get_utxos`, builds a
//!   `RawTxData` via the shared-crates `pallas-adapter` feature,
//!   runs `RuleEngine::classify`, and emits one `MarketplaceEvent`
//!   for each marketplace-relevant `TxType` it finds. No watch
//!   set, no policy filtering at the indexer.
//!
//! Phase 10+ (deferred): typed per-variant change records (vs.
//! the current "wrap a TxClassification" approach), trait-filtered
//! collection offers, royalty resolution, parallel-run validation
//! against the existing classifier worker.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use dolos_core::{ChainPoint, Domain, StateStore, TipEvent, TxoRef};
use mitos_core::{Emitter, Indexer};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use address_registry::SmartContractRegistry;
use transactions::pallas_adapter::{OutputRef, raw_tx_data_from_pallas};
use tx_classifier::{RuleEngine, TxClassification, TxType};

/// Single marketplace event record.
///
/// For the skeleton we wrap the classifier's full
/// `TxClassification` rather than re-defining a typed taxonomy of
/// variants. Pros: zero risk of drift from the upstream decoder;
/// changes to `TxType` flow through automatically. Cons: consumers
/// see a richer type than they need (mints, transfers, dex swaps
/// all included alongside marketplace events) and have to extract
/// what they care about.
///
/// `policy_id` is surfaced at the top level so the CF-side worker
/// can route to per-policy DOs without parsing the embedded
/// classification. A single tx that touches multiple policies
/// (rare but valid) emits multiple records — one per policy.
///
/// Phase 10 work: replace with a narrowed `MarketplaceEventType`
/// enum covering only sales / listings / offers / collection
/// offers, with explicit conversion at the emit site.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceEvent {
    /// Policy this event references. Used by the CF worker to
    /// route to a per-policy DO. A tx that touches N policies
    /// produces N MarketplaceEvent records, one per policy.
    pub policy_id: String,
    /// Transaction hash this classification belongs to. Same as
    /// `classification.tx_hash` but lifted out for cheap access.
    pub tx_hash: String,
    /// Slot the block was at when the tx was applied. Useful for
    /// downstream chronological ordering and reorg correlation.
    pub slot: u64,
    /// Full classifier output. Includes all `TxType` variants the
    /// rule engine identified — consumers filter to whichever they
    /// care about (e.g. only `Sale` for floor tracking).
    pub classification: TxClassification,
}

pub struct MarketplaceIndexer {
    /// Address-aware classifier engine. Built once at construction
    /// (mainnet registry); the rule logic + smart-contract
    /// addresses stay constant for the lifetime of the indexer.
    rule_engine: Arc<RuleEngine>,
}

impl MarketplaceIndexer {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Box::new(SmartContractRegistry::new());
        let rule_engine = Arc::new(RuleEngine::new(registry));
        Ok(Self { rule_engine })
    }
}

impl Default for MarketplaceIndexer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

#[async_trait]
impl<D: Domain> Indexer<D> for MarketplaceIndexer {
    /// No per-consumer scope — marketplace events are a global
    /// feed. The CF worker subscribes once and routes to per-policy
    /// DOs internally.
    type Scope = ();
    type Change = MarketplaceEvent;

    fn name(&self) -> &'static str {
        "marketplace"
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        info!(
            indexer = "marketplace",
            "bootstrap: emitting global marketplace event feed (no per-policy scope)"
        );
        Ok(ChainPoint::Origin)
    }

    async fn handle_event(
        &mut self,
        domain: &D,
        event: &TipEvent,
        emitter: &Emitter<Self::Change>,
    ) -> anyhow::Result<()> {
        match event {
            TipEvent::Apply(point, block) => {
                let parsed = match MultiEraBlock::decode(block.as_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "block decode failed; skipping");
                        return Ok(());
                    }
                };

                let slot = match point {
                    ChainPoint::Specific(s, _) => *s,
                    ChainPoint::Slot(s) => *s,
                    ChainPoint::Origin => 0,
                };

                for tx in parsed.txs() {
                    if let Err(e) = self.classify_tx(domain, &tx, slot, emitter) {
                        debug!(tx_hash = %hex::encode(tx.hash()), error = %e, "tx classification failed; skipping");
                    }
                }
            }
            // Undo + Mark are auto-emitted by the framework.
            // Marketplace events are append-only on the consumer
            // side (sales table, etc.) — Undo signals a rollback,
            // consumer DO removes by tx_hash. The indexer doesn't
            // track its own state to revert.
            TipEvent::Undo(_, _) | TipEvent::Mark(_) => {}
        }
        Ok(())
    }

    fn routes(&self) -> Router {
        Router::new()
    }

    // `subscribe`, `unsubscribe`, and `change_matches_scope` use
    // the trait defaults: no-op subscribe (returns Resume), no-op
    // unsubscribe, every change matches every scope (since scope
    // is `()`, there's nothing to match).
}

impl MarketplaceIndexer {
    /// Classify one tx and emit any marketplace-relevant events.
    fn classify_tx<D: Domain>(
        &self,
        domain: &D,
        tx: &pallas::ledger::traverse::MultiEraTx<'_>,
        slot: u64,
        emitter: &Emitter<MarketplaceEvent>,
    ) -> anyhow::Result<()> {
        // Resolve consumed inputs via dolos's state store.
        // Reference inputs too — their datums often carry the
        // marketplace pricing context.
        let mut needed_refs: Vec<TxoRef> = tx
            .consumes()
            .into_iter()
            .map(|i| TxoRef(*i.hash(), i.index() as u32))
            .collect();
        for r in tx.reference_inputs() {
            needed_refs.push(TxoRef(*r.hash(), r.index() as u32));
        }

        let utxo_map = match domain.state().get_utxos(needed_refs) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = ?e, "state lookup failed; skipping");
                return Ok(());
            }
        };

        // Stable backing for the decoded MultiEraOutputs. The
        // outputs borrow from the CBOR bytes inside the Arcs;
        // keeping the Vec alive for the duration of this fn keeps
        // those refs valid.
        let utxo_pairs: Vec<(TxoRef, std::sync::Arc<dolos_core::EraCbor>)> =
            utxo_map.into_iter().collect();

        let resolved: HashMap<OutputRef, MultiEraOutput<'_>> = utxo_pairs
            .iter()
            .filter_map(|(txo_ref, era_cbor)| {
                let era = pallas::ledger::traverse::Era::try_from(era_cbor.0).ok()?;
                let output = MultiEraOutput::decode(era, &era_cbor.1).ok()?;
                Some((OutputRef::new(*txo_ref.0, txo_ref.1), output))
            })
            .collect();

        let raw = raw_tx_data_from_pallas(tx, &resolved, Some(slot))
            .map_err(|e| anyhow::anyhow!("build RawTxData: {e}"))?;
        let mut classification = self.rule_engine.classify(&raw);
        classification.tx_hash = raw.tx_hash.clone();

        // Per-policy emission: one `MarketplaceEvent` per policy
        // referenced by the classification. The CF worker uses the
        // top-level `policy_id` to route. Non-marketplace `TxType`
        // variants (Mint, Transfer, DexSwap) contribute nothing to
        // the policy set — they ride along inside `classification`
        // for context but don't trigger a fan-out by themselves.
        let policies = policies_in_classification(&classification);
        for policy_id in policies {
            emitter.apply(MarketplaceEvent {
                policy_id,
                tx_hash: classification.tx_hash.clone(),
                slot,
                classification: classification.clone(),
            });
        }

        Ok(())
    }
}

/// Walk a `TxClassification` and collect every distinct policy_id
/// referenced by any marketplace-relevant `TxType`.
fn policies_in_classification(c: &TxClassification) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for ty in &c.tx_types {
        match ty {
            // Sale carries a `PricedAsset` (asset + price + delta).
            // The nested `asset.asset` is the typed `AssetId`.
            TxType::Sale { asset, .. } => {
                out.insert(asset.asset.policy_id.clone());
            }
            // OfferAccept carries a bare `AssetId`.
            TxType::OfferAccept { asset, .. } => {
                out.insert(asset.policy_id.clone());
            }
            // Listing create/update carry `Vec<PricedAsset>` for
            // bundles.
            TxType::ListingCreate { assets, .. } | TxType::ListingUpdate { assets, .. } => {
                for a in assets {
                    out.insert(a.asset.policy_id.clone());
                }
            }
            // Unlisting carries `Vec<AssetId>` (no price on
            // unlist).
            TxType::Unlisting { assets, .. } => {
                for a in assets {
                    out.insert(a.policy_id.clone());
                }
            }
            // Offer create/update/cancel carry the policy_id at
            // the top level (works for both per-asset and
            // collection-wide offers — `encoded_asset_name` on the
            // variant tells them apart).
            TxType::CreateOffer { policy_id, .. }
            | TxType::OfferUpdate { policy_id, .. }
            | TxType::OfferCancel { policy_id, .. } => {
                out.insert(policy_id.clone());
            }
            // Non-marketplace variants — skipped at the policy
            // extraction phase. The classification still includes
            // them in tx_types for downstream context, but this
            // indexer doesn't fan them out.
            _ => {}
        }
    }
    out
}
