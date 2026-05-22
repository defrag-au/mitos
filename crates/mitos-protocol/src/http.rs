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

use crate::interest::Interest;
use crate::wire::{ChainPoint, InterestOp};

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

/// One emission inside an [`ApplyBulkRequest`]. Mirrors the
/// per-row fields of [`ApplyBody`] minus `channel`, which is shared
/// at the batch level (all emissions in a bulk POST target one
/// channel / one partition lane).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkEmission {
    /// Monotonic emission id from the host's `EmissionsStore`. The
    /// host uses it to demux the per-emission result back into the
    /// store; opaque to the consumer (NOT a dedup key — applies are
    /// chain-point-idempotent; see `docs/design/DIALER_BULK_APPLY.md`).
    pub emission_id: u64,
    /// Chain point of this emission.
    pub cursor: ChainPoint,
    /// CBOR-encoded event payload — same bytes `apply_bytes` decodes.
    #[serde(with = "serde_bytes")]
    pub change: Vec<u8>,
}

/// Body of `POST /_internal/apply-<channel>-bulk?key=<companion_key>`.
///
/// Carries up to M emissions for one channel (one partition lane),
/// applied by the consumer **in slice order**. Collapses M HTTP
/// round-trips into one. See `docs/design/DIALER_BULK_APPLY.md`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyBulkRequest {
    /// Channel tag — same value the per-row `apply-<channel>` URL
    /// carries. All `emissions` belong to this channel.
    pub channel: String,
    /// Emissions to apply, in order.
    pub emissions: Vec<BulkEmission>,
}

/// Per-emission outcome in an [`ApplyBulkResponse`]. `applied =
/// false` carries the rejection reason in `error` (the bulk
/// analogue of a per-row 422 Nack); the lane keeps moving.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkEmissionResult {
    pub emission_id: u64,
    /// `true` → Ack-equivalent; `false` → Nack-equivalent (terminal,
    /// won't help to retry).
    pub applied: bool,
    /// Rejection reason when `applied == false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response body for `POST /_internal/apply-<channel>-bulk`.
///
/// HTTP 200 carries one [`BulkEmissionResult`] per applied/rejected
/// emission. The host demuxes by `emission_id`: `applied` → Acked,
/// `!applied` → Nacked, and any requested id **missing** from
/// `results` is left Queued for the next tick (defensive: covers a
/// companion that truncated mid-batch). A 5xx on the whole POST
/// means none applied — all rows go back to Queued.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyBulkResponse {
    pub results: Vec<BulkEmissionResult>,
}

/// CBOR-encode an [`ApplyBulkRequest`].
pub fn encode_apply_bulk(body: &ApplyBulkRequest) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(256);
    ciborium::into_writer(body, &mut buf).map_err(|e| format!("encode_apply_bulk: {e}"))?;
    Ok(buf)
}

/// CBOR-decode an [`ApplyBulkRequest`].
pub fn decode_apply_bulk(bytes: &[u8]) -> Result<ApplyBulkRequest, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_apply_bulk: {e}"))
}

/// CBOR-encode an [`ApplyBulkResponse`].
pub fn encode_apply_bulk_response(body: &ApplyBulkResponse) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(128);
    ciborium::into_writer(body, &mut buf).map_err(|e| format!("encode_apply_bulk_response: {e}"))?;
    Ok(buf)
}

/// CBOR-decode an [`ApplyBulkResponse`].
pub fn decode_apply_bulk_response(bytes: &[u8]) -> Result<ApplyBulkResponse, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_apply_bulk_response: {e}"))
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

/// Body of `POST /api/companions/{key}/interest`.
///
/// Targeted interest mutation — Add or Remove a small set of
/// predicates without re-running the full
/// `/api/companions/subscribe` path. Updates the persisted CBOR
/// registration for every module the companion is subscribed to
/// AND propagates to each running follower's live filter via the
/// host's `InterestRouter`. The combination of "update the
/// persisted set" + "update the in-memory filter" preserves the
/// invariant that the next restart-time resume produces the same
/// dispatch behaviour as the post-mutation live state.
///
/// `Replace` is intentionally not supported — full-set rewrites
/// go through the existing `/api/companions/subscribe` flow,
/// which also recomputes `dial_back` and target lists. The
/// mutation endpoint stays single-purpose: just the filter delta.
///
/// Empty `items` is rejected as a 400 — no useful operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestMutationBody {
    /// `Add` appends new predicates (deduped against the
    /// existing set). `Remove` strips matching predicates.
    /// `Replace` is rejected — use `/api/companions/subscribe`
    /// for that.
    pub op: InterestOp,
    /// Predicates to apply. Must be non-empty.
    pub items: Vec<Interest>,
}

/// CBOR-encode an [`InterestMutationBody`].
pub fn encode_interest_mutation(body: &InterestMutationBody) -> Result<Vec<u8>, String> {
    let mut buf = Vec::with_capacity(128);
    ciborium::into_writer(body, &mut buf).map_err(|e| format!("encode_interest_mutation: {e}"))?;
    Ok(buf)
}

/// CBOR-decode an [`InterestMutationBody`].
pub fn decode_interest_mutation(bytes: &[u8]) -> Result<InterestMutationBody, String> {
    ciborium::from_reader(bytes).map_err(|e| format!("decode_interest_mutation: {e}"))
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
    fn apply_bulk_round_trip() {
        let req = ApplyBulkRequest {
            channel: "collection-holders".into(),
            emissions: vec![
                BulkEmission {
                    emission_id: 10,
                    cursor: ChainPoint::Slot(100),
                    change: vec![1, 2, 3],
                },
                BulkEmission {
                    emission_id: 11,
                    cursor: ChainPoint::Specific(101, "abcd".into()),
                    change: vec![4, 5],
                },
            ],
        };
        let bytes = encode_apply_bulk(&req).unwrap();
        let decoded = decode_apply_bulk(&bytes).unwrap();
        assert_eq!(decoded.channel, "collection-holders");
        assert_eq!(decoded.emissions.len(), 2);
        assert_eq!(decoded.emissions[0].emission_id, 10);
        assert_eq!(decoded.emissions[1].cursor.hash(), Some("abcd"));
        assert_eq!(decoded.emissions[1].change, vec![4, 5]);
    }

    #[test]
    fn apply_bulk_response_round_trip() {
        let resp = ApplyBulkResponse {
            results: vec![
                BulkEmissionResult {
                    emission_id: 10,
                    applied: true,
                    error: None,
                },
                BulkEmissionResult {
                    emission_id: 11,
                    applied: false,
                    error: Some("datum hash mismatch".into()),
                },
            ],
        };
        let bytes = encode_apply_bulk_response(&resp).unwrap();
        let decoded = decode_apply_bulk_response(&bytes).unwrap();
        assert_eq!(decoded.results.len(), 2);
        assert!(decoded.results[0].applied);
        assert!(!decoded.results[1].applied);
        assert_eq!(decoded.results[1].error.as_deref(), Some("datum hash mismatch"));
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
    fn interest_mutation_round_trip_add() {
        use crate::interest::Interest;
        let body = InterestMutationBody {
            op: InterestOp::Add,
            items: vec![Interest::any()],
        };
        let bytes = encode_interest_mutation(&body).unwrap();
        let decoded = decode_interest_mutation(&bytes).unwrap();
        assert!(matches!(decoded.op, InterestOp::Add));
        assert_eq!(decoded.items.len(), 1);
    }

    #[test]
    fn interest_mutation_round_trip_remove() {
        use crate::interest::Interest;
        let body = InterestMutationBody {
            op: InterestOp::Remove,
            items: vec![Interest::any()],
        };
        let bytes = encode_interest_mutation(&body).unwrap();
        let decoded = decode_interest_mutation(&bytes).unwrap();
        assert!(matches!(decoded.op, InterestOp::Remove));
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
