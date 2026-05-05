//! CF replication wire protocol + axum mount (test/admin surface).
//!
//! Wire types (`ClientMessage`, `ServerMessage`, `SubscribeReply`,
//! `ChainPoint`) live in `mitos-protocol` so the host (here) and the
//! CF companion (cnft.dev-workers/types/mitos-companion) share a
//! single source of truth. mitos-core internally still uses
//! `dolos_core::ChainPoint` for everything that integrates with
//! dolos's APIs; the conversion to/from `mitos_protocol::ChainPoint`
//! happens at the WS boundary via the `From` impls in this file.
//!
//! The `replicate_router` function below mounts
//! `/replicate/{indexer}` for each registered indexer. **This server
//! endpoint is a test surface, not the production path** —
//! production uses `Replicator` to dial outbound to CF DOs that
//! accept the WebSocket via the Hibernation API. See
//! `docs/design/CF_REPLICATION.md` § Connection direction.

use std::sync::Arc;

use axum::Router;
use axum::extract::{State, WebSocketUpgrade, ws::WebSocket};
use axum::response::Response;
use axum::routing::get;
use dolos::adapters::DomainAdapter;
use dolos_core::{BlockHash, ChainPoint};
use tracing::{debug, info, warn};

// Re-export the canonical wire types so existing
// `crate::ClientMessage` / `crate::ServerMessage` imports still work.
pub use mitos_protocol::wire::{
    ClientMessage, ServerMessage, decode_client, decode_server, encode_client, encode_server,
};
pub use mitos_protocol::{ChainPoint as WireChainPoint, SubscribeReply};

use crate::handle::IndexerHandle;
use crate::transport::AxumWs;

/// Convert a dolos `ChainPoint` (host-internal type with pallas
/// `Hash<32>`) to the wire-format `ChainPoint` (string-hash) used in
/// the CBOR envelopes. Round-trips identically because pallas's
/// `Hash<32>` serde impl emits hex text.
pub fn chain_point_to_wire(point: &ChainPoint) -> WireChainPoint {
    match point {
        ChainPoint::Origin => WireChainPoint::Origin,
        ChainPoint::Slot(s) => WireChainPoint::Slot(*s),
        ChainPoint::Specific(s, h) => WireChainPoint::Specific(*s, h.to_string()),
    }
}

/// Convert a wire `ChainPoint` (string-hash) back into the
/// dolos-shaped one. Errors if the hash text isn't valid hex of the
/// expected length.
pub fn chain_point_from_wire(point: &WireChainPoint) -> anyhow::Result<ChainPoint> {
    match point {
        WireChainPoint::Origin => Ok(ChainPoint::Origin),
        WireChainPoint::Slot(s) => Ok(ChainPoint::Slot(*s)),
        WireChainPoint::Specific(s, h) => {
            let hash: BlockHash = h
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid block hash hex {h}: {e}"))?;
            Ok(ChainPoint::Specific(*s, hash))
        }
    }
}

/// Convert a dolos `SubscribeReply` (which uses
/// `dolos_core::ChainPoint` internally) into the wire shape.
pub fn subscribe_reply_to_wire(reply: &crate::indexer::SubscribeReply) -> SubscribeReply {
    use crate::indexer::SubscribeReply as Inner;
    match reply {
        Inner::Resume { cursor } => SubscribeReply::Resume {
            cursor: chain_point_to_wire(cursor),
        },
        Inner::SnapshotRedirect {
            snapshot_url,
            snapshot_cursor,
        } => SubscribeReply::SnapshotRedirect {
            snapshot_url: snapshot_url.clone(),
            snapshot_cursor: chain_point_to_wire(snapshot_cursor),
        },
        Inner::Fork { common_ancestor } => SubscribeReply::Fork {
            common_ancestor: chain_point_to_wire(common_ancestor),
        },
    }
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
/// middleware (Bearer token in `Authorization` header).
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
