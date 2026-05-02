//! Type-erasure adapter for `Indexer<D>`.
//!
//! The `Indexer` trait carries associated `Scope` and `Change` types so
//! per-consumer subscriptions and change payloads are typed at the
//! indexer's boundary rather than passed around as `serde_json::Value`.
//! Those associated types make the trait non-object-safe, so the bundle
//! can't store `Vec<Arc<dyn Indexer<D>>>` directly.
//!
//! `IndexerHandle` is an object-safe trait that erases the typed
//! interface by accepting CBOR bytes at the subscribe boundary and
//! decoding inside the per-indexer impl. This is the same shape axum
//! uses for handlers (`Handler<T>` is generic; `BoxedHandler` is the
//! erased form stored in the router). Bundle authors never see the
//! erased form — they just call `Bundle::add_indexer(MyIndexer::new()?)`
//! and the framework wraps it.

use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::ws::{Message, WebSocket};
use dolos::adapters::DomainAdapter;
use dolos_core::{ChainPoint, TipEvent};
use futures_util::StreamExt;
use tokio::sync::{Mutex, broadcast};
use tracing::warn;

use crate::emitter::{EmittedRecord, Emitter};
use crate::indexer::Indexer;
use crate::replicate::{ClientMessage, ServerMessage, decode_client, send_server};

/// How many records to buffer per indexer in the broadcast channel.
/// Slow consumers that fall behind get a `Lagged` error and the pump
/// drops their connection — they reconnect via the resume / snapshot
/// path. Sized for one block's worth of busy chain activity (~hundreds
/// of changes) plus headroom; production tuning is Phase 4.5 work.
const BROADCAST_CAPACITY: usize = 4096;

/// Object-safe view of an `Indexer<DomainAdapter>` for storage in
/// heterogeneous collections. All methods take only `Send + Sync`
/// types — the indexer's `Scope` and `Change` are erased behind CBOR
/// bytes. Concrete on `DomainAdapter` because the framework only
/// runs against the Dolos-backed domain in production; indexers
/// remain generic over `D: Domain` for testability.
#[async_trait]
pub trait IndexerHandle: Send + Sync {
    fn name(&self) -> &'static str;

    /// HTTP routes for this indexer. Captured at adapter construction
    /// (a clone of the original `Router`), so this is cheap to call
    /// from synchronous bundle-setup code.
    fn routes(&self) -> axum::Router;

    async fn bootstrap(&self, domain: &DomainAdapter) -> anyhow::Result<ChainPoint>;

    async fn handle_event(
        &self,
        domain: &DomainAdapter,
        event: &TipEvent,
    ) -> anyhow::Result<()>;

    /// Run the per-consumer WebSocket pump until the consumer
    /// disconnects (or the indexer drops). Owns the subscribe
    /// handshake, broadcast→ws forwarding, scope filtering, and
    /// (Phase 4.5) ack-driven retransmit-buffer trim.
    async fn run_subscriber(
        &self,
        socket: WebSocket,
        domain: DomainAdapter,
    ) -> anyhow::Result<()>;
}

/// Concrete adapter. Captures `name` and `routes` at construction (both
/// are `&self` methods on `Indexer` returning owned values), holds an
/// `Arc<Mutex<I>>` for the methods that need `&mut self`, and owns the
/// broadcast channel emissions flow through.
pub struct IndexerAdapter<I>
where
    I: Indexer<DomainAdapter>,
{
    name: &'static str,
    routes: axum::Router,
    inner: Arc<Mutex<I>>,
    changes: broadcast::Sender<EmittedRecord<<I as Indexer<DomainAdapter>>::Change>>,
}

impl<I> IndexerAdapter<I>
where
    I: Indexer<DomainAdapter> + 'static,
{
    pub fn new(indexer: I) -> Self {
        let name = indexer.name();
        let routes = indexer.routes();
        let (changes, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            name,
            routes,
            inner: Arc::new(Mutex::new(indexer)),
            changes,
        }
    }
}

#[async_trait]
impl<I> IndexerHandle for IndexerAdapter<I>
where
    I: Indexer<DomainAdapter> + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn routes(&self) -> axum::Router {
        self.routes.clone()
    }

    async fn bootstrap(&self, domain: &DomainAdapter) -> anyhow::Result<ChainPoint> {
        let mut guard = self.inner.lock().await;
        guard.bootstrap(domain).await
    }

    async fn handle_event(
        &self,
        domain: &DomainAdapter,
        event: &TipEvent,
    ) -> anyhow::Result<()> {
        let cursor = event_cursor(event);
        let emitter = Emitter::new(self.changes.clone(), cursor.clone());
        let mut guard = self.inner.lock().await;
        let result = guard.handle_event(domain, event, &emitter).await;
        // Mark heartbeats are auto-emitted; indexers don't call
        // emitter.mark() themselves.
        if matches!(event, TipEvent::Mark(_)) {
            let _ = self.changes.send(EmittedRecord::Mark { cursor });
        }
        result
    }

    async fn run_subscriber(
        &self,
        socket: WebSocket,
        domain: DomainAdapter,
    ) -> anyhow::Result<()> {
        let mut socket = socket;

        // Phase A: subscribe handshake.
        let (scope, cursor) = match read_subscribe::<I>(&mut socket).await? {
            Some(pair) => pair,
            None => return Ok(()), // peer closed before subscribing
        };

        let reply = {
            let mut guard = self.inner.lock().await;
            guard.subscribe(&domain, scope.clone_for_subscribe(), cursor).await?
        };
        send_server(&mut socket, &ServerMessage::SubscribeReply(reply)).await?;

        // Phase B: forward broadcast records, filtered by scope, until
        // the consumer disconnects or lags.
        let rx = self.changes.subscribe();
        forward_records::<I>(&mut socket, rx, scope.into_inner()).await
    }
}

/// Read the first message from a freshly-upgraded socket and decode
/// it as a Subscribe. Returns `None` if the peer closed before
/// subscribing.
async fn read_subscribe<I>(
    socket: &mut WebSocket,
) -> anyhow::Result<Option<(TypedScope<I>, ChainPoint)>>
where
    I: Indexer<DomainAdapter>,
{
    let bytes = match socket.next().await {
        Some(Ok(Message::Binary(b))) => b,
        Some(Ok(Message::Close(_))) | None => return Ok(None),
        Some(Ok(other)) => {
            let _ = send_server(
                socket,
                &ServerMessage::Error {
                    code: "expected_binary".into(),
                    message: format!("expected Binary frame, got {other:?}"),
                },
            )
            .await;
            return Ok(None);
        }
        Some(Err(e)) => return Err(anyhow::anyhow!("ws recv: {e}")),
    };

    match decode_client(&bytes)? {
        ClientMessage::Subscribe { scope, cursor } => {
            let typed: <I as Indexer<DomainAdapter>>::Scope =
                ciborium::from_reader(scope.as_slice())
                    .map_err(|e| anyhow::anyhow!("decoding scope CBOR: {e}"))?;
            Ok(Some((TypedScope(typed), cursor)))
        }
        ClientMessage::Ack { .. } | ClientMessage::Unsubscribe => {
            let _ = send_server(
                socket,
                &ServerMessage::Error {
                    code: "subscribe_required".into(),
                    message: "first message must be Subscribe".into(),
                },
            )
            .await;
            Ok(None)
        }
    }
}

/// Wrapper letting us hand the same `Scope` value to both
/// `Indexer::subscribe` (consumes by value) and the broadcast pump
/// filter (needs to keep its own copy). We require `Scope: Clone`
/// implicitly by going through CBOR re-encode; this avoids forcing
/// a `Clone` bound on every indexer's scope type.
struct TypedScope<I>(<I as Indexer<DomainAdapter>>::Scope)
where
    I: Indexer<DomainAdapter>;

impl<I> TypedScope<I>
where
    I: Indexer<DomainAdapter>,
{
    fn clone_for_subscribe(&self) -> <I as Indexer<DomainAdapter>>::Scope {
        // Re-encode → re-decode round-trip. Cheap (scopes are small)
        // and avoids requiring Clone on every indexer's Scope.
        let mut buf = Vec::with_capacity(32);
        ciborium::into_writer(&self.0, &mut buf).expect("scope re-encode");
        ciborium::from_reader(buf.as_slice()).expect("scope re-decode")
    }

    fn into_inner(self) -> <I as Indexer<DomainAdapter>>::Scope {
        self.0
    }
}

async fn forward_records<I>(
    socket: &mut WebSocket,
    mut rx: broadcast::Receiver<EmittedRecord<<I as Indexer<DomainAdapter>>::Change>>,
    scope: <I as Indexer<DomainAdapter>>::Scope,
) -> anyhow::Result<()>
where
    I: Indexer<DomainAdapter>,
{
    loop {
        let record = match rx.recv().await {
            Ok(r) => r,
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(skipped = n, "consumer lagged, dropping connection");
                let _ = send_server(
                    socket,
                    &ServerMessage::Error {
                        code: "lagged".into(),
                        message: format!("consumer lagged by {n} records; reconnect"),
                    },
                )
                .await;
                return Ok(());
            }
            Err(broadcast::error::RecvError::Closed) => return Ok(()),
        };

        let msg = match record {
            EmittedRecord::Apply { cursor, change } => {
                if !I::change_matches_scope(&scope, &change) {
                    continue;
                }
                let mut buf = Vec::with_capacity(64);
                ciborium::into_writer(&change, &mut buf)
                    .map_err(|e| anyhow::anyhow!("encode change: {e}"))?;
                ServerMessage::Apply { cursor, change: buf }
            }
            EmittedRecord::Undo { cursor } => ServerMessage::Undo { cursor },
            EmittedRecord::Mark { cursor } => ServerMessage::Mark { cursor },
        };

        send_server(socket, &msg).await?;
    }
}

fn event_cursor(event: &TipEvent) -> ChainPoint {
    match event {
        TipEvent::Apply(c, _) => c.clone(),
        TipEvent::Undo(c, _) => c.clone(),
        TipEvent::Mark(c) => c.clone(),
    }
}
