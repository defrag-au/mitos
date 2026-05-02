//! Collection ownership indexer: per-policy NFT ownership tracking.
//!
//! This is the first migration target for the CF replication
//! prototype, mirroring `cnft.dev-workers/workers/collection-ownership/`.
//! See `docs/design/CF_REPLICATION.md` Phase 4.5 for the build order.
//!
//! Phase 3 (this crate, current state): watch-set-only skeleton.
//! - `subscribe(policy_id)` adds the policy to the in-memory watch set
//! - `handle_event(Apply)` decodes the block, scans tx outputs at
//!   watched policies, and emits an `OwnershipChange::Transfer` for
//!   each output
//! - Cold subscribes return `Resume { cursor }` from the current
//!   dispatcher position — no historical backfill yet.
//!
//! Phase 5 (next): backfill via `domain.indexes().utxos_by_policy()`
//! at subscribe time, so a fresh consumer gets the full current
//! ownership state for `policy_id` before live tail begins.

use std::collections::HashSet;

use async_trait::async_trait;
use axum::Router;
use dolos_core::{ChainPoint, Domain, TipEvent};
use mitos_core::{Emitter, Indexer, SubscribeReply};
use pallas::ledger::traverse::MultiEraBlock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// CF subscription scope: a single Cardano policy ID.
///
/// Encoded as lowercase hex on the wire; that's what the
/// cnft.dev-workers ecosystem uses everywhere. Could be a `[u8; 28]`
/// internally for slight efficiency, but the saving is irrelevant at
/// our scale and the hex form makes logs and debugging trivial.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct OwnershipScope {
    pub policy_id: String,
}

/// Single ownership change record.
///
/// Phase 3 only emits `Transfer` (an output containing an asset of a
/// watched policy was produced). Phase 5+ may add `Burn` once the
/// indexer tracks consumed-with-no-replacement; for now, a transfer
/// to a script-locked address handles the common cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OwnershipChange {
    Transfer {
        policy_id: String,
        asset_name: String,
        new_owner: String,
        tx_hash: String,
        output_index: u32,
    },
}

pub struct OwnershipIndexer {
    /// Set of policy IDs (hex) the indexer is currently emitting
    /// records for. Mutated by `subscribe`/`unsubscribe`; read on
    /// every Apply.
    watch_set: HashSet<String>,
}

impl OwnershipIndexer {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            watch_set: HashSet::new(),
        })
    }
}

impl Default for OwnershipIndexer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

#[async_trait]
impl<D: Domain> Indexer<D> for OwnershipIndexer {
    type Scope = OwnershipScope;
    type Change = OwnershipChange;

    fn name(&self) -> &'static str {
        "collection-ownership"
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        info!(
            indexer = "collection-ownership",
            "bootstrap: watch set empty until first subscribe"
        );
        Ok(ChainPoint::Origin)
    }

    async fn handle_event(
        &mut self,
        _domain: &D,
        event: &TipEvent,
        emitter: &Emitter<Self::Change>,
    ) -> anyhow::Result<()> {
        match event {
            TipEvent::Apply(_, block) => {
                if self.watch_set.is_empty() {
                    return Ok(());
                }
                let parsed = match MultiEraBlock::decode(block.as_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "block decode failed; skipping");
                        return Ok(());
                    }
                };
                for tx in parsed.txs() {
                    let tx_hash = hex::encode(tx.hash());
                    for (idx, output) in tx.produces() {
                        let address = match output.address() {
                            Ok(a) => a.to_string(),
                            Err(e) => {
                                debug!(error = %e, tx = %tx_hash, "output address parse failed");
                                continue;
                            }
                        };
                        for policy_assets in output.value().assets() {
                            let policy = hex::encode(policy_assets.policy());
                            if !self.watch_set.contains(&policy) {
                                continue;
                            }
                            for asset in policy_assets.assets() {
                                emitter.apply(OwnershipChange::Transfer {
                                    policy_id: policy.clone(),
                                    asset_name: hex::encode(asset.name()),
                                    new_owner: address.clone(),
                                    tx_hash: tx_hash.clone(),
                                    output_index: idx as u32,
                                });
                            }
                        }
                    }
                }
            }
            // Undo + Mark are auto-emitted by the framework. The
            // indexer's own state (the watch set) is unaffected by
            // chain reorgs — only consumer-side materialized views
            // need to roll back, and the framework's Undo signal
            // handles that.
            TipEvent::Undo(_, _) | TipEvent::Mark(_) => {}
        }
        Ok(())
    }

    fn routes(&self) -> Router {
        // Phase 5+: expose a watch-set introspection endpoint here
        // (e.g. `GET /watched` returning current policies). For now,
        // empty Router — the bundle still nests it under
        // `/collection-ownership/`.
        Router::new()
    }

    async fn subscribe(
        &mut self,
        _domain: &D,
        scope: Self::Scope,
        cursor: ChainPoint,
    ) -> anyhow::Result<SubscribeReply> {
        let added = self.watch_set.insert(scope.policy_id.clone());
        info!(
            indexer = "collection-ownership",
            policy_id = %scope.policy_id,
            new = added,
            watch_set_size = self.watch_set.len(),
            "subscribe"
        );
        // Phase 5: replace with snapshot-or-resume decision based on
        // gap between consumer cursor and current tip. For now
        // always Resume; cold subscribes (`Origin` cursor) just get
        // live tail with no backfill.
        Ok(SubscribeReply::Resume { cursor })
    }

    async fn unsubscribe(&mut self, scope: Self::Scope) -> anyhow::Result<()> {
        let removed = self.watch_set.remove(&scope.policy_id);
        info!(
            indexer = "collection-ownership",
            policy_id = %scope.policy_id,
            removed,
            watch_set_size = self.watch_set.len(),
            "unsubscribe"
        );
        Ok(())
    }

    fn change_matches_scope(scope: &Self::Scope, change: &Self::Change) -> bool {
        match change {
            OwnershipChange::Transfer { policy_id, .. } => *policy_id == scope.policy_id,
        }
    }
}
