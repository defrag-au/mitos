//! Per-Apply movement-claim coordinator for the dispatcher's
//! residual pass.
//!
//! Specific-domain indexers (marketplace, mint-burn, future dex /
//! lending) return `Vec<MovementClaim>` from `handle_event` for
//! each tx they classify. The synchronised dispatcher accumulates
//! those claims into a `TxClaimCoordinator` for the current Apply
//! event, then runs `none_match-indexer` (the residual pass) which
//! consults the coordinator to decide which asset transfers in
//! the in/out diff are *not* already classified — those become
//! `Domain::AssetMovement` emissions.
//!
//! Per `docs/design/DOMAIN_REFACTOR.md` "Implementation notes
//! (v1)":
//!
//! - **Ephemeral per Apply event** — the coordinator is created
//!   and discarded per Apply; claims are not persisted, not
//!   carried across events. `Undo` and `Mark` need no claim
//!   handling. Multi-block reorgs work without special handling.
//! - **`DashMap`-backed concurrent insertion** — specific-domain
//!   indexers (will eventually) run in parallel within a single
//!   Apply; lock-free insertion avoids the bottleneck a
//!   `Mutex<HashSet>` would create on busy blocks.

use std::sync::Arc;

use dashmap::DashMap;
use mitos_protocol::MovementClaim;

/// A per-Apply claim coordinator. Cheap to construct, cheap to
/// clone (the inner `Arc<DashMap>` is shared); hand it to
/// `none_match-indexer` at construction time so the residual pass
/// can read claim state when emitting movements.
///
/// `clear` between Apply events is the canonical way to reset.
/// Don't share a coordinator between block boundaries — claims
/// are per-Apply.
#[derive(Debug, Default, Clone)]
pub struct TxClaimCoordinator {
    inner: Arc<DashMap<MovementClaim, ()>>,
}

impl TxClaimCoordinator {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Insert a batch of claims. Concurrent-safe; multiple
    /// indexers can call this in parallel. Duplicate claims
    /// (same `(asset, from, to)` triplet from two indexers — a
    /// classification bug in production) are silently
    /// deduplicated; the dispatcher logs a warning when it
    /// detects them upstream of this method.
    pub fn add_all(&self, claims: Vec<MovementClaim>) {
        for claim in claims {
            self.inner.insert(claim, ());
        }
    }

    /// Check whether a given `(asset, from, to)` triplet was
    /// claimed by any indexer for the current tx. Used by the
    /// residual pass to skip emission for already-classified
    /// movements.
    pub fn contains(&self, claim: &MovementClaim) -> bool {
        self.inner.contains_key(claim)
    }

    /// Reset between Apply events — the dispatcher calls this
    /// before invoking specific-domain indexers for the next
    /// event. Cheap (just clears the inner map).
    pub fn clear(&self) {
        self.inner.clear();
    }

    /// Number of claims currently held. Diagnostic / logging.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// True iff no claims have been registered for the current
    /// Apply event.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cardano_assets::AssetId;

    fn sample_claim(asset_name: &str, from: &str, to: &str) -> MovementClaim {
        MovementClaim {
            asset: AssetId::new(
                "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6".into(),
                asset_name.into(),
            )
            .unwrap(),
            from: from.into(),
            to: to.into(),
        }
    }

    #[test]
    fn add_and_contains() {
        let coord = TxClaimCoordinator::new();
        let c = sample_claim("deadbeef", "addr_a", "addr_b");
        assert!(!coord.contains(&c));
        coord.add_all(vec![c.clone()]);
        assert!(coord.contains(&c));
    }

    #[test]
    fn duplicate_claim_silently_deduplicated() {
        let coord = TxClaimCoordinator::new();
        let c = sample_claim("deadbeef", "addr_a", "addr_b");
        coord.add_all(vec![c.clone(), c.clone()]);
        assert_eq!(coord.len(), 1);
    }

    #[test]
    fn clear_resets_state() {
        let coord = TxClaimCoordinator::new();
        coord.add_all(vec![sample_claim("deadbeef", "a", "b")]);
        assert_eq!(coord.len(), 1);
        coord.clear();
        assert!(coord.is_empty());
    }

    #[test]
    fn clones_share_state() {
        // Two clones backed by the same `Arc<DashMap>` — what one
        // sees the other sees. Critical for the runtime: the
        // dispatcher inserts via one clone, none-match reads via
        // another.
        let coord_a = TxClaimCoordinator::new();
        let coord_b = coord_a.clone();
        let c = sample_claim("deadbeef", "a", "b");
        coord_a.add_all(vec![c.clone()]);
        assert!(coord_b.contains(&c));
    }
}
