//! CF replication wire protocol + axum mount (test/admin surface).
//!
//! Defines the CBOR envelopes consumers exchange with mitos and
//! provides `replicate_router` which mounts `/replicate/{indexer}` for
//! each registered indexer. **This server endpoint is a test surface,
//! not the production path** — production uses `Replicator` to dial
//! outbound to CF DOs that accept the WebSocket via the Hibernation
//! API. See `docs/design/CF_REPLICATION.md` § Connection direction.
//!
//! The shared per-consumer protocol logic lives in
//! `IndexerHandle::run_subscriber` and is direction-agnostic via the
//! `WsTransport` trait — both this server endpoint and the outbound
//! `Replicator` feed it the same protocol.

use std::sync::Arc;

use axum::Router;
use axum::extract::{State, WebSocketUpgrade, ws::WebSocket};
use axum::response::Response;
use axum::routing::get;
use dolos::adapters::DomainAdapter;
use dolos_core::ChainPoint;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::SubscribeReply;
use crate::handle::IndexerHandle;
use crate::transport::AxumWs;

/// Messages the consumer (CF DO) sends to mitos over the WebSocket.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Open a subscription against the connected indexer with the
    /// given typed scope (CBOR-encoded for the indexer's `Scope`
    /// type) and the consumer's last-applied cursor.
    Subscribe {
        #[serde(with = "serde_bytes")]
        scope: Vec<u8>,
        cursor: ChainPoint,
    },
    /// Acknowledge that the consumer has durably persisted state up
    /// to and including `cursor`. Mitos uses this to trim the
    /// per-consumer retransmit buffer (Phase 4.5 work).
    Ack { cursor: ChainPoint },
    /// Drop the previously-subscribed scope. Mitos may also remove
    /// the scope from its watch set if no other consumer is tracking
    /// it.
    Unsubscribe,
}

/// Messages mitos sends to the consumer.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    SubscribeReply(SubscribeReply),
    Apply {
        cursor: ChainPoint,
        #[serde(with = "serde_bytes")]
        change: Vec<u8>,
    },
    Undo {
        cursor: ChainPoint,
    },
    Mark {
        cursor: ChainPoint,
    },
    Error {
        code: String,
        message: String,
    },
}

#[derive(Clone)]
struct ReplicateState {
    handle: Arc<dyn IndexerHandle>,
    domain: DomainAdapter,
}

/// Mount `/replicate/{indexer}` for each registered indexer (test
/// surface — see module-level docs). Each route's typed `State`
/// resolves directly to the indexer it serves; no name lookup at
/// runtime.
///
/// `auth` gates every upgrade request via the standard axum
/// middleware (Bearer token in `Authorization` header). Open mode
/// when `auth` is `None`-equivalent.
pub fn replicate_router(
    handles: &[Arc<dyn IndexerHandle>],
    domain: DomainAdapter,
    auth: crate::auth::AuthToken,
) -> Router {
    let mut router = Router::new();
    for handle in handles {
        let name = handle.name();
        let endpoint = format!("/replicate/{name}");
        let state = ReplicateState {
            handle: handle.clone(),
            domain: domain.clone(),
        };
        router = router.route(&endpoint, get(handle_upgrade).with_state(state));
        debug!(indexer = %name, endpoint = %endpoint, "replicate route mounted");
    }
    router.layer(axum::middleware::from_fn_with_state(
        auth,
        crate::auth::require_auth,
    ))
}

async fn handle_upgrade(
    upgrade: WebSocketUpgrade,
    State(state): State<ReplicateState>,
) -> Response {
    upgrade.on_upgrade(move |socket| run_socket(socket, state))
}

async fn run_socket(socket: WebSocket, state: ReplicateState) {
    let name = state.handle.name();
    info!(indexer = %name, "test consumer connected (server-accepted)");
    let transport = Box::new(AxumWs(socket));
    match state.handle.run_subscriber(transport, state.domain).await {
        Ok(()) => info!(indexer = %name, "consumer disconnected"),
        Err(e) => warn!(indexer = %name, error = %e, "subscriber loop exited with error"),
    }
}

/// Encode a `ServerMessage` to CBOR for transport.
pub fn encode_server(msg: &ServerMessage) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(msg, &mut buf).map_err(|e| anyhow::anyhow!("CBOR encode: {e}"))?;
    Ok(buf)
}

/// Encode a `ClientMessage` to CBOR for transport.
pub fn encode_client(msg: &ClientMessage) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(msg, &mut buf).map_err(|e| anyhow::anyhow!("CBOR encode: {e}"))?;
    Ok(buf)
}

/// Decode a `ClientMessage` from CBOR.
pub fn decode_client(bytes: &[u8]) -> anyhow::Result<ClientMessage> {
    ciborium::from_reader(bytes).map_err(|e| anyhow::anyhow!("CBOR decode: {e}"))
}

/// Decode a `ServerMessage` from CBOR.
pub fn decode_server(bytes: &[u8]) -> anyhow::Result<ServerMessage> {
    ciborium::from_reader(bytes).map_err(|e| anyhow::anyhow!("CBOR decode: {e}"))
}
