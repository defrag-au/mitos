use async_trait::async_trait;
use dolos_core::{ChainPoint, TipEvent};
use serde::{Serialize, de::DeserializeOwned};

/// Reply to a `subscribe` request from a CF replication consumer.
///
/// `Resume` is the live-tail path: the consumer's last cursor is recent
/// enough that mitos can stream change records from `cursor` forward.
///
/// `SnapshotRedirect` is the cold-subscribe / large-gap path: mitos has
/// frozen a view at a known cursor and tells the consumer to fetch that
/// snapshot before resuming. The consumer applies the snapshot, then
/// re-subscribes at `snapshot_cursor`.
///
/// `Fork` is reorg recognition: the consumer's last cursor references a
/// block mitos has since rolled back. Mitos will deliver `Undo` records
/// back to the common ancestor over the live channel, then `Apply`
/// records along the new chain. `common_ancestor` is informational —
/// the consumer doesn't need to act on it directly, the Apply/Undo
/// stream handles correctness.
#[derive(Debug, Clone)]
pub enum SubscribeReply {
    Resume {
        cursor: ChainPoint,
    },
    SnapshotRedirect {
        snapshot_url: String,
        snapshot_cursor: ChainPoint,
    },
    Fork {
        common_ancestor: ChainPoint,
    },
}

/// Contract every indexer module implements. See
/// `docs/design/INDEXER_TRAIT.md` for the full lifecycle and idempotency
/// requirements, and `docs/design/CF_REPLICATION.md` for the
/// subscription model.
///
/// `D` is the concrete `dolos_core::Domain` type the bundle has wired up.
/// Indexers are typed against it rather than `dyn Domain` so they can use
/// the associated types (`D::State`, `D::Indexes`, etc.) without
/// dynamic dispatch.
#[async_trait]
pub trait Indexer<D: dolos_core::Domain>: Send + Sync {
    /// Indexer-defined scope payload for CF subscriptions. Each indexer
    /// declares its own type — `()` for indexers with no per-scope
    /// concept (e.g. fixed contract addresses), a struct like
    /// `OwnershipScope { policy_id }` for indexers that watch a dynamic
    /// set keyed on chain entities.
    ///
    /// The framework decodes wire bytes (CBOR) into this type once at
    /// the subscribe boundary; everything inside the indexer is the
    /// typed value.
    type Scope: DeserializeOwned + Serialize + Send + Sync + 'static;

    /// Stable identifier. Used for log scoping, storage path naming
    /// (under `<bundle-data-dir>/indexers/<name>/`), and HTTP route
    /// prefix by convention. Must be valid as a filesystem directory.
    fn name(&self) -> &'static str;

    /// One-time pull of current chain state into the indexer's
    /// materialized view. Called before any chain events are dispatched.
    /// Returns the chain point we caught up to — the dispatcher will
    /// start streaming events from this point forward.
    async fn bootstrap(&mut self, domain: &D) -> anyhow::Result<ChainPoint>;

    /// Single chain event. The dispatcher calls this for every
    /// subscribed event in order. Implementations MUST be idempotent
    /// against re-delivery.
    async fn handle_event(&mut self, domain: &D, event: &TipEvent) -> anyhow::Result<()>;

    /// HTTP routes this indexer exposes. The bundle merges all
    /// indexers' routes under a shared `axum::Router`, conventionally
    /// nested under `/<name>/`.
    fn routes(&self) -> axum::Router;

    /// Add a CF replication scope to this indexer's watch set.
    ///
    /// Default implementation: no-op subscribe that returns
    /// `Resume { cursor }`, suitable for indexers whose view is fully
    /// determined by the indexer config (e.g. fixed contract
    /// addresses) and don't need per-consumer scope tracking.
    /// Indexers with dynamic watch sets (e.g. ownership keyed by
    /// policy) override this.
    async fn subscribe(
        &mut self,
        _domain: &D,
        _scope: Self::Scope,
        cursor: ChainPoint,
    ) -> anyhow::Result<SubscribeReply> {
        Ok(SubscribeReply::Resume { cursor })
    }

    /// Drop a previously-subscribed scope. Default: no-op.
    async fn unsubscribe(&mut self, _scope: Self::Scope) -> anyhow::Result<()> {
        Ok(())
    }
}
