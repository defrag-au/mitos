//! Type-erasure adapter for `Indexer<D>`.
//!
//! The `Indexer` trait carries an associated `Scope` type so per-consumer
//! subscriptions are typed at the indexer's boundary rather than passed
//! around as `serde_json::Value`. The associated type makes the trait
//! non-object-safe, so the bundle can't store `Vec<Arc<Mutex<dyn
//! Indexer<D>>>>` directly.
//!
//! `IndexerHandle` is an object-safe trait that erases the `Scope` type
//! by accepting CBOR bytes at the subscribe boundary and decoding inside
//! the per-indexer impl. This is the same shape axum uses for handlers
//! (`Handler<T>` is generic; `BoxedHandler` is the erased form stored in
//! the router). Bundle authors never see the erased form — they just
//! call `Bundle::add_indexer(MyIndexer::new()?)` and the framework wraps
//! it.

use std::sync::Arc;

use async_trait::async_trait;
use dolos_core::{ChainPoint, Domain, TipEvent};
use tokio::sync::Mutex;

use crate::indexer::{Indexer, SubscribeReply};

/// Object-safe view of an `Indexer<D>` for storage in heterogeneous
/// collections (e.g. `Vec<Box<dyn IndexerHandle<D>>>`). All methods
/// take only `Send + Sync` types — the indexer's own `Scope` is
/// erased behind CBOR bytes.
#[async_trait]
pub trait IndexerHandle<D: Domain>: Send + Sync {
    fn name(&self) -> &'static str;

    /// HTTP routes for this indexer. Captured at adapter construction
    /// (a clone of the original `Router`), so this is cheap to call
    /// from synchronous bundle-setup code.
    fn routes(&self) -> axum::Router;

    async fn bootstrap(&self, domain: &D) -> anyhow::Result<ChainPoint>;

    async fn handle_event(&self, domain: &D, event: &TipEvent) -> anyhow::Result<()>;

    /// Decode `scope_cbor` into the indexer's `Scope` type and call
    /// `Indexer::subscribe`. Returns the framework-shared
    /// `SubscribeReply` regardless of the indexer's scope vocabulary.
    async fn subscribe(
        &self,
        domain: &D,
        scope_cbor: &[u8],
        cursor: ChainPoint,
    ) -> anyhow::Result<SubscribeReply>;

    /// Decode `scope_cbor` and call `Indexer::unsubscribe`.
    async fn unsubscribe(&self, scope_cbor: &[u8]) -> anyhow::Result<()>;
}

/// Concrete adapter. Captures `name` and `routes` at construction (both
/// are `&self` methods on `Indexer` returning owned values), and holds
/// an `Arc<Mutex<I>>` for the methods that need `&mut self`.
pub struct IndexerAdapter<I> {
    name: &'static str,
    routes: axum::Router,
    inner: Arc<Mutex<I>>,
}

impl<I> IndexerAdapter<I> {
    pub fn new<D>(indexer: I) -> Self
    where
        D: Domain,
        I: Indexer<D>,
    {
        let name = indexer.name();
        let routes = indexer.routes();
        Self {
            name,
            routes,
            inner: Arc::new(Mutex::new(indexer)),
        }
    }
}

#[async_trait]
impl<D, I> IndexerHandle<D> for IndexerAdapter<I>
where
    D: Domain,
    I: Indexer<D> + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn routes(&self) -> axum::Router {
        self.routes.clone()
    }

    async fn bootstrap(&self, domain: &D) -> anyhow::Result<ChainPoint> {
        let mut guard = self.inner.lock().await;
        guard.bootstrap(domain).await
    }

    async fn handle_event(&self, domain: &D, event: &TipEvent) -> anyhow::Result<()> {
        let mut guard = self.inner.lock().await;
        guard.handle_event(domain, event).await
    }

    async fn subscribe(
        &self,
        domain: &D,
        scope_cbor: &[u8],
        cursor: ChainPoint,
    ) -> anyhow::Result<SubscribeReply> {
        let scope: I::Scope = ciborium::from_reader(scope_cbor)
            .map_err(|e| anyhow::anyhow!("decoding scope CBOR: {e}"))?;
        let mut guard = self.inner.lock().await;
        guard.subscribe(domain, scope, cursor).await
    }

    async fn unsubscribe(&self, scope_cbor: &[u8]) -> anyhow::Result<()> {
        let scope: I::Scope = ciborium::from_reader(scope_cbor)
            .map_err(|e| anyhow::anyhow!("decoding scope CBOR: {e}"))?;
        let mut guard = self.inner.lock().await;
        guard.unsubscribe(scope).await
    }
}
