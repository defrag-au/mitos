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
//! decoding inside the per-indexer impl. Bundle authors never see the
//! erased form — they just call `Bundle::add_indexer(MyIndexer::new()?)`
//! and the framework wraps it.
//!
//! `run_subscriber` accepts a `Box<dyn WsTransport>` so the same
//! protocol logic works regardless of who initiated the WebSocket.
//! The server-side `/replicate/{indexer}` endpoint wraps an axum
//! WebSocket; the client-side `Replicator` wraps a tokio-tungstenite
//! stream. See `docs/design/CF_REPLICATION.md` § Connection direction.

use std::sync::Arc;

use async_trait::async_trait;
use dolos::adapters::DomainAdapter;
use dolos_core::{ChainPoint, TipEvent};
use mitos_protocol::MovementClaim;
use tokio::sync::{Mutex, broadcast};
use tracing::warn;

use crate::emitter::{EmittedRecord, Emitter};
use crate::indexer::Indexer;
use crate::replicate::{
    ClientMessage, ServerMessage, chain_point_from_wire, chain_point_to_wire, decode_client,
    encode_server, subscribe_reply_to_wire,
};
use crate::transport::WsTransport;

/// How many records to buffer per indexer in the broadcast channel.
/// Slow consumers that fall behind get a `Lagged` error and the pump
/// drops their connection — they reconnect via the resume / snapshot
/// path. Sized for one block's worth of busy chain activity (~hundreds
/// of changes) plus headroom; revisit if production traffic patterns
/// regularly hit this ceiling.
const BROADCAST_CAPACITY: usize = 4096;

/// Object-safe view of an `Indexer<DomainAdapter>` for storage in
/// heterogeneous collections. All methods take only `Send + Sync`
/// types — the indexer's `Scope` and `Change` are erased behind CBOR
/// bytes.
#[async_trait]
pub trait IndexerHandle: Send + Sync {
    fn name(&self) -> &'static str;

    /// Mirrors `Indexer::is_internal()`. Captured at adapter
    /// construction so the host's unified-subscribe handler can
    /// reject `SubscribeTarget::Indexer { name }` requests for
    /// internal indexers without locking the inner mutex.
    fn is_internal(&self) -> bool;

    /// HTTP routes for this indexer. Captured at adapter construction
    /// (a clone of the original `Router`), so this is cheap to call
    /// from synchronous bundle-setup code.
    fn routes(&self) -> axum::Router;

    async fn bootstrap(&self, domain: &DomainAdapter) -> anyhow::Result<ChainPoint>;

    async fn handle_event(
        &self,
        domain: &DomainAdapter,
        event: &TipEvent,
    ) -> anyhow::Result<Vec<MovementClaim>>;

    /// Run the per-consumer pump until the consumer disconnects or
    /// drops. Owns the subscribe handshake, broadcast→ws forwarding,
    /// and scope filtering. Ack-driven retransmit-buffer trim is
    /// future work — today the broadcast channel's bounded capacity
    /// applies backpressure without per-consumer trim.
    ///
    /// `transport` is direction-agnostic — server-accepted axum
    /// WebSocket or client-dialed tungstenite stream both work.
    async fn run_subscriber(
        &self,
        transport: Box<dyn WsTransport>,
        domain: DomainAdapter,
        inbound_handler: Option<Arc<dyn InboundFrameHandler>>,
    ) -> anyhow::Result<()>;

    /// Convert a JSON-shaped scope value into the CBOR bytes that
    /// flow on the wire. Lets admin clients (and the `Replicator`
    /// registration API) hand in scope as ordinary JSON without
    /// needing to know the indexer's CBOR encoding details.
    ///
    /// JSON → indexer's `Scope` (via serde) → CBOR. Errors if the
    /// JSON shape doesn't match the indexer's type.
    fn encode_scope_from_json(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>>;
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
    is_internal: bool,
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
        let is_internal = indexer.is_internal();
        let routes = indexer.routes();
        let (changes, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            name,
            is_internal,
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

    fn is_internal(&self) -> bool {
        self.is_internal
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
    ) -> anyhow::Result<Vec<MovementClaim>> {
        let cursor = event_cursor(event);
        let emitter = Emitter::new(self.changes.clone(), cursor.clone());
        let mut guard = self.inner.lock().await;
        let result = guard.handle_event(domain, event, &emitter).await;
        // `Undo` and `Mark` are chain-level signals, not
        // indexer-specific. The framework auto-emits them after the
        // indexer has had a chance to roll back its own materialized
        // view (Undo) or update its cursor (Mark). Indexers only
        // emit explicit `Apply` records.
        match event {
            TipEvent::Undo(_, _) => {
                let _ = self.changes.send(EmittedRecord::Undo { cursor });
            }
            TipEvent::Mark(_) => {
                let _ = self.changes.send(EmittedRecord::Mark { cursor });
            }
            TipEvent::Apply(_, _) => {}
        }
        result
    }

    fn encode_scope_from_json(&self, value: &serde_json::Value) -> anyhow::Result<Vec<u8>> {
        let scope: <I as Indexer<DomainAdapter>>::Scope = serde_json::from_value(value.clone())
            .map_err(|e| anyhow::anyhow!("scope JSON does not match indexer scope type: {e}"))?;
        let mut buf = Vec::with_capacity(64);
        ciborium::into_writer(&scope, &mut buf)
            .map_err(|e| anyhow::anyhow!("encode scope CBOR: {e}"))?;
        Ok(buf)
    }

    async fn run_subscriber(
        &self,
        mut transport: Box<dyn WsTransport>,
        domain: DomainAdapter,
        inbound_handler: Option<Arc<dyn InboundFrameHandler>>,
    ) -> anyhow::Result<()> {
        // Phase A: subscribe handshake.
        let (scope, cursor) = match read_subscribe::<I>(&mut transport).await? {
            Some(pair) => pair,
            None => return Ok(()), // peer closed before subscribing
        };

        // Subscribe to broadcast *before* acquiring the indexer
        // Mutex. The dispatcher acquires the same Mutex during
        // `handle_event`, so any live record it produces while we
        // hold the lock for `subscribe` arrives in `rx` afterward.
        // Subscribing first guarantees we don't miss a record that
        // fires between Mutex release and pump start.
        let rx = self.changes.subscribe();

        // Run the indexer's subscribe under the Mutex. The indexer
        // mutates its watch set and populates `backfill` with
        // synthetic Apply records reflecting current state at the
        // cursor returned in the reply.
        let mut backfill: Vec<<I as Indexer<DomainAdapter>>::Change> = Vec::new();
        let reply = {
            let mut guard = self.inner.lock().await;
            guard
                .subscribe(&domain, scope.clone_for_subscribe(), cursor, &mut backfill)
                .await?
        };

        // Send the SubscribeReply, then deliver every backfill
        // record as an Apply at the reply's cursor (so the consumer
        // advances its last-applied cursor atomically once the
        // batch is fully applied). Live tail follows.
        //
        // Apply the same `change_matches_scope` filter the live-tail
        // pump uses — backfill records are equivalent to synthetic
        // Apply events at startup, so they should respect the
        // subscriber's scope identically.
        let resume_cursor = reply_cursor(&reply);
        let wire_reply = subscribe_reply_to_wire(&reply);
        send(&mut transport, &ServerMessage::SubscribeReply(wire_reply)).await?;

        for change in backfill {
            if !I::change_matches_scope(&scope.0, &change) {
                continue;
            }
            let mut buf = Vec::with_capacity(64);
            ciborium::into_writer(&change, &mut buf)
                .map_err(|e| anyhow::anyhow!("encode backfill change: {e}"))?;
            send(
                &mut transport,
                &ServerMessage::Apply {
                    // emission_id = 0 on the legacy replicator lane
                    // — the emissions log is per-companion and
                    // doesn't apply to direct in-tree-indexer
                    // subscribers. Companions tolerate the default
                    // via `#[serde(default)]`.
                    emission_id: 0,
                    cursor: chain_point_to_wire(&resume_cursor),
                    change: buf,
                },
            )
            .await?;
        }

        // Phase B: forward broadcast records, filtered by scope.
        forward_records::<I>(&mut transport, rx, scope.into_inner(), inbound_handler).await
    }
}

/// Resume cursor extracted from a `SubscribeReply` for stamping
/// backfill records. For `SnapshotRedirect` the consumer fetches
/// the snapshot from R2 — we still stamp any inline backfill at
/// the snapshot cursor as a safety net.
fn reply_cursor(reply: &crate::indexer::SubscribeReply) -> ChainPoint {
    use crate::indexer::SubscribeReply;
    match reply {
        SubscribeReply::Resume { cursor } => cursor.clone(),
        SubscribeReply::SnapshotRedirect {
            snapshot_cursor, ..
        } => snapshot_cursor.clone(),
        SubscribeReply::Fork { common_ancestor } => common_ancestor.clone(),
    }
}

async fn send(transport: &mut Box<dyn WsTransport>, msg: &ServerMessage) -> anyhow::Result<()> {
    let bytes = encode_server(msg).map_err(|e| anyhow::anyhow!("{e}"))?;
    transport.send_binary(bytes).await
}

/// Read the first message from a freshly-upgraded socket and decode
/// it as a Subscribe. Returns `None` if the peer closed before
/// subscribing.
async fn read_subscribe<I>(
    transport: &mut Box<dyn WsTransport>,
) -> anyhow::Result<Option<(TypedScope<I>, ChainPoint)>>
where
    I: Indexer<DomainAdapter>,
{
    let bytes = match transport.recv_binary().await? {
        Some(b) => b,
        None => return Ok(None),
    };

    let msg = decode_client(&bytes).map_err(|e| anyhow::anyhow!("{e}"))?;
    match msg {
        ClientMessage::Subscribe { scope, cursor } => {
            let typed: <I as Indexer<DomainAdapter>>::Scope =
                ciborium::from_reader(scope.as_slice())
                    .map_err(|e| anyhow::anyhow!("decoding scope CBOR: {e}"))?;
            let dolos_cursor = chain_point_from_wire(&cursor)?;
            Ok(Some((TypedScope(typed), dolos_cursor)))
        }
        ClientMessage::Ack { .. }
        | ClientMessage::Nack { .. }
        | ClientMessage::Interest { .. }
        | ClientMessage::Unsubscribe
        | ClientMessage::RecaptureReady => {
            // RecaptureReady is part of the recapture protocol (see
            // `docs/design/RECAPTURE.md`); it's only meaningful
            // mid-subscription after the host has sent a `Recapture`
            // frame. Receiving it before Subscribe is the same shape
            // of error as Ack/Nack/etc — handle uniformly.
            let _ = send(
                transport,
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
        let mut buf = Vec::with_capacity(32);
        ciborium::into_writer(&self.0, &mut buf).expect("scope re-encode");
        ciborium::from_reader(buf.as_slice()).expect("scope re-decode")
    }

    fn into_inner(self) -> <I as Indexer<DomainAdapter>>::Scope {
        self.0
    }
}

/// Bidirectional pump after the subscribe handshake. Multiplexes
/// outbound broadcast→WS sends with inbound WS→handler reads via
/// `tokio::select!`. The two arms share `&mut transport` but
/// `select!` runs them sequentially — only one branch holds the
/// borrow at a time, so this is safe without a transport split.
///
/// Inbound frames handled:
/// - `ClientMessage::Interest { op, items }` — forwarded to the
///   `inbound_handler` if one is wired. Bundle wires this to the
///   running module's `update-interest` host call.
/// - `ClientMessage::Ack { emission_id }` /
///   `Nack { emission_id, error }` — forwarded to the
///   `inbound_handler`. Bundle wires this to the
///   `EmissionsStore::update_status` path.
/// - `ClientMessage::Unsubscribe` — graceful close; pump returns
///   `Ok(())` to drop the WS.
/// - `ClientMessage::Subscribe { .. }` after the initial handshake
///   is unexpected — log + ignore (single-subscribe per session).
///
/// `inbound_handler` is optional so the legacy test surface
/// (`replicate_router`) without a wired bundle keeps working —
/// it just logs frames it can't act on.
async fn forward_records<I>(
    transport: &mut Box<dyn WsTransport>,
    mut rx: broadcast::Receiver<EmittedRecord<<I as Indexer<DomainAdapter>>::Change>>,
    scope: <I as Indexer<DomainAdapter>>::Scope,
    inbound_handler: Option<Arc<dyn InboundFrameHandler>>,
) -> anyhow::Result<()>
where
    I: Indexer<DomainAdapter>,
{
    loop {
        tokio::select! {
            biased; // outbound first — keeps the live stream moving

            record = rx.recv() => {
                let record = match record {
                    Ok(r) => r,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "consumer lagged, dropping connection");
                        let _ = send(
                            transport,
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
                        // emission_id is assigned by the inbound_handler's
                        // emit-interception path when it appends to the
                        // EmissionsStore. The pump itself doesn't write
                        // the log; we send `0` here as a placeholder. Bundle
                        // wires the real id assignment via a future
                        // intercept-then-forward pump if it cares.
                        ServerMessage::Apply {
                            emission_id: 0,
                            cursor: chain_point_to_wire(&cursor),
                            change: buf,
                        }
                    }
                    EmittedRecord::Undo { cursor } => ServerMessage::Undo {
                        cursor: chain_point_to_wire(&cursor),
                    },
                    EmittedRecord::Mark { cursor } => ServerMessage::Mark {
                        cursor: chain_point_to_wire(&cursor),
                    },
                };

                send(transport, &msg).await?;
            }

            inbound = transport.recv_binary() => {
                let bytes = match inbound? {
                    Some(b) => b,
                    None => return Ok(()), // peer closed cleanly
                };
                match decode_client(&bytes) {
                    Ok(ClientMessage::Interest { op, items }) => {
                        if let Some(h) = &inbound_handler {
                            if let Err(e) = h.on_interest(op, items).await {
                                warn!(error = %e, "inbound Interest handler failed");
                            }
                        } else {
                            // Legacy Replicator lane never wires
                            // an inbound_handler; the new
                            // companion-runtime path goes through
                            // `mitos_platform::dialer` which has
                            // its own handler. Trace, not debug.
                            tracing::trace!(
                                "legacy lane: Interest frame received with no handler; ignoring"
                            );
                        }
                    }
                    Ok(ClientMessage::Ack { emission_id }) => {
                        if let Some(h) = &inbound_handler
                            && let Err(e) = h.on_ack(emission_id).await {
                                warn!(error = %e, "inbound Ack handler failed");
                            }
                    }
                    Ok(ClientMessage::Nack { emission_id, error }) => {
                        if let Some(h) = &inbound_handler {
                            if let Err(e) = h.on_nack(emission_id, &error).await {
                                warn!(error = %e, "inbound Nack handler failed");
                            }
                        } else {
                            // Same legacy-lane case as Interest;
                            // demote to trace so the deploy log
                            // stays clean. Genuine unrouted Nacks
                            // still surface at trace + the row
                            // staying Pending in the EmissionsStore
                            // is the durable signal.
                            tracing::trace!(
                                emission_id, error = %error,
                                "Nack received but no inbound_handler wired",
                            );
                        }
                    }
                    Ok(ClientMessage::Unsubscribe) => {
                        tracing::info!("client unsubscribed; closing pump");
                        return Ok(());
                    }
                    Ok(ClientMessage::Subscribe { .. }) => {
                        tracing::warn!("unexpected Subscribe after initial handshake; ignoring");
                    }
                    Ok(ClientMessage::RecaptureReady) => {
                        // Recapture protocol's host-side driver
                        // lands in a later commit (see
                        // `docs/design/RECAPTURE.md`). The legacy
                        // replicate pump never sends `Recapture`,
                        // so a companion should never reply with
                        // `RecaptureReady` here. Log and drop.
                        tracing::warn!(
                            "RecaptureReady received on legacy pump; ignoring \
                             (recapture not yet wired)"
                        );
                    }
                    Err(e) => {
                        // Legacy pre-PR-3 consumers send Ack/Nack frames
                        // without `emission_id`; that surfaces here as
                        // a `missing field` decode error. Demote those
                        // to debug to avoid log-flood — once those
                        // consumers migrate to companion runtime, the
                        // case disappears. Other decode failures stay
                        // at warn so genuinely-malformed frames remain
                        // visible.
                        let msg = e.to_string();
                        if msg.contains("missing field `emission_id`") {
                            tracing::debug!(error = %msg, "legacy client frame (no emission_id); ignoring");
                        } else {
                            tracing::warn!(error = %msg, "client frame decode failed; ignoring");
                        }
                    }
                }
            }
        }
    }
}

/// Inbound-frame handler trait — bundle implements this to wire
/// Interest frames into the running module's `update-interest`
/// call and Ack/Nack into the emissions log status update path.
/// Object-safe so `Box<dyn InboundFrameHandler>` flows through
/// the indexer-handle / run_subscriber surface.
#[async_trait]
pub trait InboundFrameHandler: Send + Sync {
    /// Called on `ClientMessage::Interest`. `items` is the typed
    /// `Vec<mitos_protocol::Interest>` decoded from the frame.
    /// Bundle implementations re-encode as CBOR and call the
    /// module's `update-interest` export via the follower
    /// control channel.
    async fn on_interest(
        &self,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> anyhow::Result<()>;

    /// Called on `ClientMessage::Ack`. Bundle implementations
    /// move the emission row's status to `Acked`.
    async fn on_ack(&self, emission_id: u64) -> anyhow::Result<()>;

    /// Called on `ClientMessage::Nack`. Bundle implementations
    /// move the emission row's status to `Nacked` and record the
    /// error.
    async fn on_nack(&self, emission_id: u64, error: &str) -> anyhow::Result<()>;
}

fn event_cursor(event: &TipEvent) -> ChainPoint {
    match event {
        TipEvent::Apply(c, _) => c.clone(),
        TipEvent::Undo(c, _) => c.clone(),
        TipEvent::Mark(c) => c.clone(),
    }
}
