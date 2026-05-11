//! Wire-format types for the mitos ↔ companion WS protocol.
//!
//! These are the **canonical** definitions — mitos host (mitos-core)
//! and CF companion (cnft.dev-workers/types/mitos-companion) both
//! import them from here. Eliminates the historical mirror between
//! `mitos-core/src/replicate.rs` and the CF-side mirrors.
//!
//! ## ChainPoint compatibility note
//!
//! `mitos_protocol::ChainPoint` is **type-distinct** from
//! `dolos_core::ChainPoint` but **CBOR wire-compatible**: Pallas's
//! `Hash<32>` serializes as hex text under CBOR, matching the
//! `String` here. mitos-core wires the conversion at the WS boundary
//! via `From<dolos_core::ChainPoint>` impls (see
//! `mitos-core/src/replicate.rs`).
//!
//! ## Frame inventory
//!
//! - `ClientMessage` — companion → mitos. `Subscribe` (in-session
//!   re-assertion), `Interest` (Q6), `Ack`/`Nack` (Q5),
//!   `Unsubscribe`, `RecaptureReady` (recapture ack — see
//!   `docs/design/RECAPTURE.md`).
//! - `ServerMessage` — mitos → companion. `Connected` (Q8 readiness),
//!   `SubscribeReply`, `Apply` (with `emission_id`), `Undo`, `Mark`,
//!   `Error`, `Recapture` / `RecaptureDone` (per-module state-rebuild
//!   protocol — see `docs/design/RECAPTURE.md`).
//!
//! See `mitos/docs/strategy/MITOS_COMPANION_RUNTIME_V1.md` for the
//! design rationale.

use serde::{Deserialize, Serialize};

use crate::interest::Interest;

// ============================================================================
// ChainPoint
// ============================================================================

/// Cardano chain point.
///
/// **Hash is hex-text, not bytes.** `Specific` is `[u64, text]` on
/// the wire — using `Vec<u8>` would fail to decode every frame
/// produced by mitos-core's serializer (which uses pallas's hex-text
/// `Hash<32>` impl).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChainPoint {
    Origin,
    Slot(u64),
    Specific(u64, String),
}

impl ChainPoint {
    /// Slot number, if any. `Origin` returns `None`.
    pub fn slot(&self) -> Option<u64> {
        match self {
            ChainPoint::Origin => None,
            ChainPoint::Slot(s) => Some(*s),
            ChainPoint::Specific(s, _) => Some(*s),
        }
    }

    /// Hash hex string, if any. `Origin` and `Slot` return `None`.
    pub fn hash(&self) -> Option<&str> {
        match self {
            ChainPoint::Origin | ChainPoint::Slot(_) => None,
            ChainPoint::Specific(_, h) => Some(h.as_str()),
        }
    }
}

// ============================================================================
// SubscribeReply
// ============================================================================

/// Reply to a `Subscribe` from the companion.
///
/// `Resume` is the live-tail path: companion's last cursor is recent
/// enough that mitos can stream from there forward.
///
/// `SnapshotRedirect` is the cold-subscribe / large-gap path: mitos
/// has a frozen view and tells the companion to fetch that snapshot
/// before resuming.
///
/// `Fork` is reorg recognition: companion's last cursor references a
/// block mitos has rolled back. Mitos delivers `Undo` records back to
/// the common ancestor, then `Apply` records along the new chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

// ============================================================================
// InterestOp
// ============================================================================

/// Interest mutation operation. See Q6 of the design doc.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum InterestOp {
    Add,
    Remove,
    /// Full re-sync — host replaces its filter set with the supplied
    /// items. Used on reconnect to re-assert state.
    Replace,
}

// ============================================================================
// ClientMessage — companion → mitos
// ============================================================================

/// Frames the companion sends to mitos over the held WS.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Open a subscription with the supplied scope (CBOR-encoded for
    /// the indexer's typed `Scope`) and the consumer's last-applied
    /// cursor.
    ///
    /// Initial registration for companion-runtime users happens
    /// out-of-band over HTTPS via `/api/companions/subscribe`; this
    /// frame is for in-session re-assertion (drift detection / full
    /// re-sync).
    Subscribe {
        #[serde(with = "serde_bytes")]
        scope: Vec<u8>,
        cursor: ChainPoint,
    },

    /// Drop the previously-subscribed scope.
    Unsubscribe,

    /// Dynamic interest mutation (Q6). Default path is one frame per
    /// RPC call; the `items` Vec is reserved for a future
    /// `bulk_subscribe` RPC variant.
    Interest {
        op: InterestOp,
        items: Vec<Interest>,
    },

    /// Emission delivery acknowledgement (Q5). Sent after successful
    /// `apply_event` + cursor advance.
    Ack { emission_id: u64 },

    /// Emission delivery negative-acknowledgement (Q5). Sent when the
    /// dApp's `apply_event` returned `Err`. Cursor still advances on
    /// the companion side; host records the error in `module_emissions`.
    Nack { emission_id: u64, error: String },

    /// Acknowledgement that the companion has finished its
    /// `on_recapture` hook (dApp-state cleanup scoped to the
    /// module name carried in the preceding `Recapture` frame)
    /// and is ready to receive the refill stream. Mitos waits
    /// for this before clearing the module's bootstrap-done
    /// flags and re-running bootstrap. No payload — the WS
    /// conversation is per-companion so mitos knows the source.
    /// See `docs/design/RECAPTURE.md`.
    RecaptureReady,
}

// ============================================================================
// ServerMessage — mitos → companion
// ============================================================================

/// Frames mitos sends to the companion over the held WS.
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    SubscribeReply(SubscribeReply),
    Apply {
        /// Q5 — opaque to companion; echoed back in `Ack` / `Nack` so
        /// the host can correlate against `module_emissions`.
        #[serde(default)]
        emission_id: u64,
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
    /// Q8 — readiness signal; first frame after mitos completes the
    /// dial. `last_emission_id` is the host's view of the latest
    /// emission_id for this companion as a sync point.
    Connected {
        last_emission_id: u64,
    },

    /// Per-module state-rebuild request from the host. Companions
    /// subscribed to multiple community modules MUST scope their
    /// `on_recapture` cleanup by `module` — blindly dropping
    /// shared dApp tables takes out rows from other subscriptions
    /// whose state isn't being refilled. See
    /// `docs/design/RECAPTURE.md` for the schema contract.
    ///
    /// After the companion's `on_recapture` completes, the runtime
    /// sends `ClientMessage::RecaptureReady` and the host begins
    /// re-emitting Apply frames for the module's current
    /// bootstrap-walk state.
    Recapture {
        /// The source module being recaptured (matches a
        /// `SubscribeTarget::Module { name }` the companion is
        /// subscribed to). Companions use this to scope their
        /// cleanup.
        module: String,
        /// Operator-supplied free-form label. Surfaced in the
        /// companion's `on_recapture` callback for logging; not
        /// load-bearing for the protocol.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

    /// End-of-refill marker emitted after the host's bootstrap
    /// walk completes for a `Recapture`. Apply frames preceding
    /// this carry the refill payload; live chain events resume
    /// from `cursor`.
    ///
    /// Informational — the protocol is correct without this frame
    /// (the Apply stream does all the load-bearing work), but
    /// the marker gives companions a clean boundary for any
    /// post-refill housekeeping and the admin endpoint a precise
    /// finish point for its response.
    RecaptureDone {
        /// The chain cursor at the moment the bootstrap walk
        /// finished. Live events resume from here.
        cursor: ChainPoint,
        /// The source module whose recapture is now complete.
        /// Matches the `module` carried in the preceding
        /// `Recapture` frame.
        module: String,
        /// Number of refill events emitted for this companion's
        /// view during the recapture. Useful for logs +
        /// admin-endpoint response. Best-effort counter; if
        /// counting proves expensive, future revisions may set
        /// this to 0 — companions MUST NOT depend on the value
        /// for correctness.
        #[serde(default)]
        events_emitted: u64,
    },
}

// ============================================================================
// Encode / decode helpers
// ============================================================================

/// CBOR-encode a `ClientMessage`.
pub fn encode_client(msg: &ClientMessage) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(msg, &mut buf).map_err(|e| format!("encode_client: {e}"))?;
    Ok(buf)
}

/// CBOR-encode a `ServerMessage`.
pub fn encode_server(msg: &ServerMessage) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(msg, &mut buf).map_err(|e| format!("encode_server: {e}"))?;
    Ok(buf)
}

/// CBOR-decode a `ClientMessage`.
pub fn decode_client(bytes: &[u8]) -> Result<ClientMessage, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_client: {e}"))
}

/// CBOR-decode a `ServerMessage`.
pub fn decode_server(bytes: &[u8]) -> Result<ServerMessage, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_server: {e}"))
}
