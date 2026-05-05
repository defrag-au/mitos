//! HTTPS subscribe call — companion → mitos host registration.
//!
//! Per the design doc's "Addressing & wake-up: mitos dials companions"
//! section, the companion's first action on DO wake is to POST a
//! single subscribe call to the mitos host:
//!
//! ```text
//! POST {MITOS_HOST_URL}/api/companions/subscribe
//! Authorization: Bearer <module_auth_token>
//! Content-Type: application/cbor
//!
//! { module_name, companion_key, resume_from, interests, dial_back? }
//! ```
//!
//! Mitos persists the registration in its module redb so it can dial
//! back over WS to deliver emissions (the dial-back path lands in
//! PR 3 alongside the emissions log; for PR 1, the host endpoint
//! just persists).
//!
//! ## Type sourcing
//!
//! `SubscribeRequest`, `SubscribeResponse`, `DialBackOverride` all
//! live in `mitos-protocol` so the host and companion share a single
//! source of truth — no manual mirroring. This module re-exports
//! them and adds the companion-side `post_subscribe` HTTPS helper.
//!
//! ## Idempotency
//!
//! Calling subscribe again is idempotent — host overwrites the stored
//! config + interest set with the new payload. The runtime caches the
//! last successful registration in DO SQLite and skips the round-trip
//! on subsequent wakes when nothing has changed (see [`crate::meta::
//! RegistrationCache`]).

use crate::error::{CompanionError, Result};

pub use mitos_protocol::{DialBackOverride, SubscribeRequest, SubscribeResponse};

/// Environment variable name the runtime reads to determine where
/// mitos lives. dApps set this in their `wrangler.toml`.
pub const MITOS_HOST_URL_ENV: &str = "MITOS_HOST_URL";

/// Environment variable name for the per-module shared bearer token.
/// dApps populate this via CF Secrets Store. Same shape mitos's
/// admin endpoints use; same value rotates them in lockstep.
pub const MITOS_AUTH_TOKEN_ENV: &str = "MITOS_AUTH_TOKEN";

/// Environment variable name for the dial-back URL template the
/// host should use when opening its outbound WS to this companion.
/// Carries `{key}` as a placeholder that mitos substitutes with
/// the companion key at dial time.
///
/// Example: `wss://collections-mitos.cnft.dev/_internal/replicate?policy_id={key}`
pub const MITOS_REPLICATE_URL_ENV: &str = "MITOS_REPLICATE_URL";

/// CBOR-encode a `SubscribeRequest` for transport.
pub fn encode_subscribe(req: &SubscribeRequest) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);
    ciborium::ser::into_writer(req, &mut buf)
        .map_err(|e| CompanionError::Wire(format!("encode_subscribe: {e}")))?;
    Ok(buf)
}

/// CBOR-decode a `SubscribeRequest`. Used by the host endpoint;
/// also useful in tests.
pub fn decode_subscribe(bytes: &[u8]) -> Result<SubscribeRequest> {
    ciborium::de::from_reader(bytes)
        .map_err(|e| CompanionError::Wire(format!("decode_subscribe: {e}")))
}

/// CBOR-decode a `SubscribeResponse`. Used by the companion runtime
/// to interpret the host's reply.
pub fn decode_subscribe_response(bytes: &[u8]) -> Result<SubscribeResponse> {
    ciborium::de::from_reader(bytes)
        .map_err(|e| CompanionError::Wire(format!("decode_subscribe_response: {e}")))
}

/// Post the subscribe call to the mitos host.
///
/// Reads `MITOS_HOST_URL` and `MITOS_AUTH_TOKEN` from the supplied
/// `Env` (CF Secrets Store / wrangler.toml), builds a CBOR-encoded
/// request, and POSTs to `{host_url}/api/companions/subscribe`.
///
/// Returns the parsed response on 200 OK; surfaces transport / decode
/// errors as `CompanionError::Wire` for the caller to log.
#[cfg(target_arch = "wasm32")]
pub async fn post_subscribe(
    env: &worker::Env,
    request: &SubscribeRequest,
) -> Result<SubscribeResponse> {
    use js_sys::Uint8Array;
    use worker::{Fetch, Headers, Method, Request, RequestInit};

    let host_url = env
        .var(MITOS_HOST_URL_ENV)
        .map_err(|e| CompanionError::Wire(format!("read {MITOS_HOST_URL_ENV}: {e}")))?
        .to_string();
    let token = env
        .secret(MITOS_AUTH_TOKEN_ENV)
        .or_else(|_| env.var(MITOS_AUTH_TOKEN_ENV))
        .map_err(|e| CompanionError::Wire(format!("read {MITOS_AUTH_TOKEN_ENV}: {e}")))?
        .to_string();

    let body_bytes = encode_subscribe(request)?;
    let body_array = Uint8Array::from(body_bytes.as_slice());

    let headers = Headers::new();
    headers
        .set("authorization", &format!("Bearer {token}"))
        .map_err(|e| CompanionError::Wire(format!("set auth header: {e}")))?;
    headers
        .set("content-type", "application/cbor")
        .map_err(|e| CompanionError::Wire(format!("set content-type: {e}")))?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(body_array.into()));

    let url = format!(
        "{}/api/companions/subscribe",
        host_url.trim_end_matches('/')
    );
    let req = Request::new_with_init(&url, &init)
        .map_err(|e| CompanionError::Wire(format!("build subscribe request: {e}")))?;

    let mut response = Fetch::Request(req)
        .send()
        .await
        .map_err(|e| CompanionError::Wire(format!("subscribe send: {e}")))?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        let body = response.text().await.unwrap_or_default();
        return Err(CompanionError::Wire(format!(
            "subscribe non-2xx ({status}): {body}"
        )));
    }

    let body_bytes = response
        .bytes()
        .await
        .map_err(|e| CompanionError::Wire(format!("read subscribe response: {e}")))?;
    decode_subscribe_response(&body_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitos_protocol::{ChainPoint, Interest};

    #[test]
    fn round_trip_subscribe_request() {
        let req = SubscribeRequest {
            module_name: "ownership-indexer".into(),
            companion_key: "customer_42".into(),
            resume_from: Some(ChainPoint::Specific(123, "abcd".into())),
            interests: vec![Interest::any()],
            dial_back: None,
        };
        let bytes = encode_subscribe(&req).unwrap();
        let decoded = decode_subscribe(&bytes).unwrap();
        assert_eq!(decoded.module_name, "ownership-indexer");
        assert_eq!(decoded.companion_key, "customer_42");
        assert_eq!(decoded.interests.len(), 1);
        assert!(decoded.dial_back.is_none());
    }

    #[test]
    fn round_trip_subscribe_response() {
        let resp = SubscribeResponse {
            status: "subscribed".into(),
            next_emission_id: 42,
        };
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&resp, &mut buf).unwrap();
        let decoded = decode_subscribe_response(&buf).unwrap();
        assert_eq!(decoded.status, "subscribed");
        assert_eq!(decoded.next_emission_id, 42);
    }

    #[test]
    fn round_trip_with_dial_back_override() {
        let req = SubscribeRequest {
            module_name: "tenant-saas".into(),
            companion_key: "tenant_001".into(),
            resume_from: None,
            interests: vec![],
            dial_back: Some(DialBackOverride {
                url: Some("wss://tenant001.example.com/replicate?key={key}".into()),
                auth_header: Some("X-Custom-Auth".into()),
                auth_value: Some("token-abc".into()),
            }),
        };
        let bytes = encode_subscribe(&req).unwrap();
        let decoded = decode_subscribe(&bytes).unwrap();
        let dial = decoded.dial_back.unwrap();
        assert_eq!(dial.auth_header.as_deref(), Some("X-Custom-Auth"));
    }
}
