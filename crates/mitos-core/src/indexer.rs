use async_trait::async_trait;
use dolos_core::{ChainPoint, TipEvent};

/// Contract every indexer module implements. See
/// `docs/design/INDEXER_TRAIT.md` for the full lifecycle and idempotency
/// requirements.
///
/// `D` is the concrete `dolos_core::Domain` type the bundle has wired up.
/// Indexers are typed against it rather than `dyn Domain` so they can use
/// the associated types (`D::State`, `D::Indexes`, etc.) without
/// dynamic dispatch.
#[async_trait]
pub trait Indexer<D: dolos_core::Domain>: Send + Sync {
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
    async fn handle_event(
        &mut self,
        domain: &D,
        event: &TipEvent,
    ) -> anyhow::Result<()>;

    /// HTTP routes this indexer exposes. The bundle merges all
    /// indexers' routes under a shared `axum::Router`, conventionally
    /// nested under `/<name>/`.
    fn routes(&self) -> axum::Router;
}
