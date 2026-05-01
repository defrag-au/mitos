//! jpg.store collection-offer indexer.
//!
//! Phase 0: stub that logs every chain event without doing anything yet.
//! Phase 2 will fill in:
//!   - bootstrap: enumerate UTxOs at the CO contract via Dolos's
//!     by-address index, decode datums, populate redb materialized view
//!   - handle_event Apply/Undo: parse blocks via pallas-traverse, scan
//!     for outputs at the contract, decode datums (inline or from
//!     witness set in the same TX), apply deltas
//!   - HTTP routes: /by-creator/{pkh}, /by-policy/{policy}, /{tx}/{idx}
//!
//! Datum format and contract addresses come from
//! `~/code/defrag/shared-crates/cardano-tx/src/builder/collection_offer.rs`.

use async_trait::async_trait;
use axum::Router;
use dolos_core::{ChainPoint, Domain, TipEvent};
use mitos_core::Indexer;
use tracing::info;

/// Known CO script addresses (jpg.store V2 + V3). Mirrors
/// `workers/jpg-store-mirror/src/api.rs:737-740` in cnft.dev-workers.
pub const KNOWN_CO_CONTRACTS: &[&str] = &[
    "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd",
    "addr1xxzvcf02fs5e282qk3pmjkau2emtcsj5wrukxak3np90n2evjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eksg6pw3p",
];

pub struct JpgCoIndexer {
    // TODO(phase-2): redb handle + cursor + per-CO materialized view
}

impl JpgCoIndexer {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {})
    }
}

impl Default for JpgCoIndexer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

#[async_trait]
impl<D: Domain> Indexer<D> for JpgCoIndexer {
    fn name(&self) -> &'static str {
        "jpg-co"
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        // TODO(phase-2): enumerate UTxOs at KNOWN_CO_CONTRACTS via
        // domain.indexes(), hydrate via domain.state(), decode datums,
        // populate materialized view.
        info!(indexer = "jpg-co", "bootstrap stub — phase-2 work pending");
        // Until we have a real catch-up point: tell the dispatcher to
        // start from the current tip. Phase 2 will return the actual
        // chain point we bootstrapped to.
        Ok(ChainPoint::Origin)
    }

    async fn handle_event(
        &mut self,
        _domain: &D,
        event: &TipEvent,
    ) -> anyhow::Result<()> {
        match event {
            TipEvent::Mark(point) => {
                info!(indexer = "jpg-co", ?point, "mark");
            }
            TipEvent::Apply(point, _block) => {
                // TODO(phase-2): pallas-traverse the block, scan TX outputs
                // at KNOWN_CO_CONTRACTS, decode datums, upsert into view.
                info!(indexer = "jpg-co", ?point, "apply (stub)");
            }
            TipEvent::Undo(point, _block) => {
                // TODO(phase-2): reverse the same block's deltas.
                info!(indexer = "jpg-co", ?point, "undo (stub)");
            }
        }
        Ok(())
    }

    fn routes(&self) -> Router {
        // TODO(phase-2): /by-creator/{pkh}, /by-policy/{policy}, etc.
        // For now, an empty router so the bundle's nest("/jpg-co", ...)
        // doesn't break.
        Router::new()
    }
}
