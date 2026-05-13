//! HTTP body shapes for the mitos ↔ companion delivery transport.
//!
//! After the WS → HTTP migration (see
//! `docs/design/HTTP_DELIVERY_TRANSPORT.md`), event delivery from
//! mitos to a companion happens via plain HTTP POST instead of an
//! outbound WebSocket. The bodies defined here are CBOR-encoded
//! over the wire — same codec as the legacy `ServerMessage` /
//! `ClientMessage` frames, just without the enum tag overhead.
//!
//! ## Endpoint mapping
//!
//! - `POST /_internal/apply-<channel>?key=<companion_key>`
//!   - body: [`ApplyBody`] (CBOR)
//!   - 200 OK with empty body — equivalent to the legacy `Ack`
//!   - 422 Unprocessable Entity with text body — equivalent to
//!     the legacy `Nack` (apply errored; retry won't help)
//!   - 5xx — transport / runtime error; dialer backs off + retries
//!
//! - `POST /_internal/recapture-<channel>?key=<companion_key>`
//!   - body: [`RecaptureBody`] (CBOR)
//!   - 200 OK with empty body — equivalent to the legacy
//!     `RecaptureReady`
//!   - 5xx — `on_recapture` failed; admin endpoint surfaces the
//!     error to the operator
//!
//! `ApplyBody` and `RecaptureBody` mirror the `ServerMessage::Apply`
//! and `ServerMessage::Recapture` variants exactly. They're
//! redeclared as standalone structs so HTTP callers don't have to
//! deal with the enum-tag CBOR overhead or accidentally serialise
//! the wrong variant.

use serde::{Deserialize, Serialize};

use crate::wire::ChainPoint;

/// MIME type both directions use for HTTP delivery bodies.
pub const HTTP_DELIVERY_MIME: &str = "application/cbor";

/// Body of `POST /_internal/apply-<channel>?key=<companion_key>`.
///
/// Carries everything the consumer's `apply_event` needs to run:
/// the chain cursor of the event, the emission id (for log
/// correlation), and the CBOR-encoded event payload that the
/// channel handler decodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyBody {
    /// Monotonic emission id from the host's per-module
    /// `EmissionsStore`. Opaque to the consumer; surfaced in
    /// logs for correlation with mitos's `mitos-admin emissions`
    /// view.
    pub emission_id: u64,
    /// Chain point of the event. The consumer's runtime
    /// advances its persisted cursor to this point after the
    /// channel handler returns.
    pub cursor: ChainPoint,
    /// CBOR-encoded event payload — same bytes the channel
    /// handler's `apply_bytes` expects to decode.
    #[serde(with = "serde_bytes")]
    pub change: Vec<u8>,
}

/// Body of `POST /_internal/recapture-<channel>?key=<companion_key>`.
///
/// Replaces the legacy `ServerMessage::Recapture` frame. The
/// consumer's `on_recapture` hook runs synchronously inside the
/// request; the 200 response is the "ready for refill" signal
/// that the legacy `ClientMessage::RecaptureReady` carried.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecaptureBody {
    /// Source module being recaptured. Matches a
    /// `SubscribeTarget::Module { name }` the consumer is
    /// subscribed to. Consumers scope their cleanup by this.
    pub module: String,
    /// Operator-supplied free-form label. Surfaced in
    /// `on_recapture` for logging.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// CBOR-encode an [`ApplyBody`].
pub fn encode_apply(body: &ApplyBody) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(128);
    ciborium::into_writer(body, &mut buf).map_err(|e| format!("encode_apply: {e}"))?;
    Ok(buf)
}

/// CBOR-decode an [`ApplyBody`].
pub fn decode_apply(bytes: &[u8]) -> Result<ApplyBody, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_apply: {e}"))
}

/// CBOR-encode a [`RecaptureBody`].
pub fn encode_recapture(body: &RecaptureBody) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(body, &mut buf).map_err(|e| format!("encode_recapture: {e}"))?;
    Ok(buf)
}

/// CBOR-decode a [`RecaptureBody`].
pub fn decode_recapture(bytes: &[u8]) -> Result<RecaptureBody, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_recapture: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_body_round_trip() {
        let body = ApplyBody {
            emission_id: 42,
            cursor: ChainPoint::Specific(123, "deadbeef".into()),
            change: vec![0xd8, 0x79, 0x9f, 0xff],
        };
        let bytes = encode_apply(&body).unwrap();
        let decoded = decode_apply(&bytes).unwrap();
        assert_eq!(decoded.emission_id, 42);
        assert_eq!(decoded.cursor.slot(), Some(123));
        assert_eq!(decoded.cursor.hash(), Some("deadbeef"));
        assert_eq!(decoded.change, vec![0xd8, 0x79, 0x9f, 0xff]);
    }

    #[test]
    fn recapture_body_round_trip() {
        let body = RecaptureBody {
            module: "jpg-store-offer".into(),
            reason: Some("operator triage".into()),
        };
        let bytes = encode_recapture(&body).unwrap();
        let decoded = decode_recapture(&bytes).unwrap();
        assert_eq!(decoded.module, "jpg-store-offer");
        assert_eq!(decoded.reason.as_deref(), Some("operator triage"));
    }

    #[test]
    fn recapture_body_round_trip_no_reason() {
        let body = RecaptureBody {
            module: "asset-transfer".into(),
            reason: None,
        };
        let bytes = encode_recapture(&body).unwrap();
        let decoded = decode_recapture(&bytes).unwrap();
        assert!(decoded.reason.is_none());
    }
}
