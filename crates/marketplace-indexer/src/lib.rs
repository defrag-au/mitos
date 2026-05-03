//! Marketplace events indexer (sales, listings, offers).
//!
//! This is the second mitos indexer, alongside `OwnershipIndexer`,
//! but with a fundamentally different topology. Where ownership is
//! a per-policy stream (one consumer per policy, scope-filtered at
//! the indexer), marketplace events are emitted **as a single
//! global feed** — every marketplace tx on chain produces one or
//! more typed `ProtocolEvent` records.
//!
//! The CF-side worker subscribes once, receives every event, and
//! is responsible for per-policy fan-out: read `policy_id` /
//! `asset_name_hex` off the event, route to a per-policy DO via
//! `idFromName(policy_id)`. For policies no consumer cares about,
//! the worker drops the event (or routes to a "missing" DO that
//! 404s on read). The marketplace indexer doesn't try to know
//! which policies anyone is watching; that's the consumer worker's
//! concern.
//!
//! Design + rationale: see
//! `mitos/docs/design/MARKETPLACE_INDEXER.md` and
//! `mitos/docs/design/SUBSCRIPTION_MECHANICS.md`.
//!
//! Phase 3 (this crate, current state): typed event emission via
//! the `mitos-protocol` taxonomy.
//!
//! - `Scope = ()` — no per-consumer scope. The single subscriber
//!   gets everything; the framework `Interest` filtering machinery
//!   is wired in Phase 4 when the trait surgery lands.
//! - `Change = mitos_protocol::ProtocolEvent` — kind-as-outer
//!   `Marketplace` payloads with brand-as-data. One event per
//!   `(policy, marketplace_event)` pair: a tx that touches N
//!   policies emits N records.
//! - `handle_event(Apply)` walks each tx in the block, resolves
//!   its consumed inputs via `domain.state().get_utxos`, builds a
//!   `RawTxData` via the shared-crates `pallas-adapter` feature,
//!   runs `RuleEngine::classify`, then hands the result to
//!   `classification_to_events` to translate into typed events.
//!
//! Phase 4+ (deferred): `Scope = Interest` (server-side filtering
//! by asset/brand/kind), trait-filtered collection offers, cancel-
//! payload redeemer/script-ref decoding, royalty resolution,
//! parallel-run validation against the existing classifier worker.

mod brand_resolver;
mod translator;

#[cfg(test)]
mod translator_tests;

pub use brand_resolver::marketplace_brand_from_address;
pub use translator::classification_to_events;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use dolos_core::{ChainPoint, Domain, StateStore, TipEvent, TxoRef};
use mitos_core::{Emitter, Indexer};
use mitos_protocol::{Interest, ProtocolEvent, any_interest_matches_event};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use tracing::{debug, info, warn};

use address_registry::SmartContractRegistry;
use transactions::pallas_adapter::{OutputRef, raw_tx_data_from_pallas};
use tx_classifier::RuleEngine;

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
    /// Per-consumer scope is a `Vec<Interest>` — a list of
    /// disjoint subscription clauses, ORed at match time. Each
    /// emitted `ProtocolEvent` is delivered only to consumers
    /// whose interest list contains a clause that matches it.
    /// Consumers wanting "all marketplace events" send a single
    /// `Interest::any()`.
    type Scope = Vec<Interest>;
    type Change = ProtocolEvent;

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

    /// Per-consumer filter: `change` is delivered only when at
    /// least one `Interest` in `scope` matches. `Interest::matches`
    /// ANDs the asset / domain / value axes; the OR across the
    /// vec is what `any_interest_matches_event` performs.
    fn change_matches_scope(scope: &Self::Scope, change: &Self::Change) -> bool {
        any_interest_matches_event(scope, change)
    }

    // `subscribe`/`unsubscribe` keep the trait defaults: this
    // indexer doesn't track per-scope state (the rule engine + the
    // chain are everything; filtering is post-emit).
}

impl MarketplaceIndexer {
    /// Classify one tx and emit any marketplace-relevant events.
    fn classify_tx<D: Domain>(
        &self,
        domain: &D,
        tx: &pallas::ledger::traverse::MultiEraTx<'_>,
        slot: u64,
        emitter: &Emitter<ProtocolEvent>,
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

        // Translate the classification into typed protocol events.
        // One event per `(policy, marketplace_event)` pair —
        // multi-policy listings emit one event per affected policy;
        // non-marketplace `TxType` variants (Mint, Transfer,
        // DexSwap) contribute nothing.
        for event in classification_to_events(&classification, slot) {
            emitter.apply(event);
        }

        Ok(())
    }
}
