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
//! back over WS to deliver emissions. The dial-back is driven by
//! `mitos-platform::dialer::CompanionDialer::start_all` at host
//! startup, and on every fresh subscribe via `register`.
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
/// host should use when POSTing emissions to this companion.
///
/// Template placeholders (all optional but typically all three are
/// present in production wrangler config):
/// - `{key}` → the companion key (= the DO's `id_from_name`)
/// - `{target}` → the subscribe target name (i.e. the module the
///   companion subscribes to)
/// - `{op}` → `apply` or `recapture`, the per-request operation
///
/// Example (HTTP delivery, post-migration):
///   `https://jpgsm.cnft.dev/_internal/{op}-{target}?key={key}`
///
/// Multi-target companions MUST include `{target}` so each target
/// resolves distinctly. `{op}` MUST be present somewhere in the
/// path so apply and recapture URLs differ.
pub const MITOS_REPLICATE_URL_ENV: &str = "MITOS_REPLICATE_URL";

/// Read the auth token. dApps configure this in `wrangler.toml`
/// as a CF Secrets Store binding (`secrets_store_secrets`,
/// preferred) or as a legacy worker secret. `env.var()` is
/// intentionally not consulted — auth tokens belong in secret
/// storage, not plaintext wrangler vars.
#[cfg(target_arch = "wasm32")]
async fn read_auth_token(env: &worker::Env) -> Result<String> {
    if let Ok(store) = env.secret_store(MITOS_AUTH_TOKEN_ENV) {
        return match store.get().await {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(CompanionError::Wire(format!(
                "{MITOS_AUTH_TOKEN_ENV} secret store binding is empty"
            ))),
            Err(e) => Err(CompanionError::Wire(format!(
                "{MITOS_AUTH_TOKEN_ENV} secret store get(): {e}"
            ))),
        };
    }
    env.secret(MITOS_AUTH_TOKEN_ENV)
        .map(|s| s.to_string())
        .map_err(|e| CompanionError::Wire(format!("read {MITOS_AUTH_TOKEN_ENV}: {e}")))
}

/// Parse the host portion out of a URL. Used by the runtime as
/// the default `client_id` derivation source (see
/// `docs/design/MULTI_CLIENT_COMPANIONS.md` — "Worker-side
/// change"). Lightweight parser: skips an optional `<scheme>://`,
/// strips optional `userinfo@`, then takes everything up to the
/// next `/`, `?`, `#`, or end-of-string. The URL may carry the
/// unresolved `{op}` / `{target}` / `{key}` template placeholders
/// in its path; those sit after the host portion, so the result is
/// unaffected.
///
/// Returns `None` for inputs we can't usefully extract a host from
/// (empty, no host at all).
pub fn host_of_url(url: &str) -> Option<&str> {
    let after_scheme = url.find("://").map(|i| &url[i + 3..]).unwrap_or(url);
    let after_userinfo = after_scheme
        .find('@')
        .map(|i| &after_scheme[i + 1..])
        .unwrap_or(after_scheme);
    let end = after_userinfo
        .find(['/', '?', '#'])
        .unwrap_or(after_userinfo.len());
    let host = &after_userinfo[..end];
    if host.is_empty() { None } else { Some(host) }
}

#[cfg(test)]
mod host_of_url_tests {
    use super::host_of_url;

    #[test]
    fn extracts_host_from_https_url() {
        assert_eq!(
            host_of_url("https://hooks.epochify.space/_internal/apply-x?key=y"),
            Some("hooks.epochify.space"),
        );
    }

    #[test]
    fn extracts_host_with_unsubstituted_template() {
        assert_eq!(
            host_of_url("https://hooks.dev.epochify.space/_internal/{op}-{target}?key={key}"),
            Some("hooks.dev.epochify.space"),
        );
    }

    #[test]
    fn strips_userinfo() {
        assert_eq!(
            host_of_url("https://user:pass@hooks.example.com/path"),
            Some("hooks.example.com"),
        );
    }

    #[test]
    fn handles_no_path() {
        assert_eq!(host_of_url("https://example.com"), Some("example.com"));
    }

    #[test]
    fn handles_no_scheme() {
        // Falls through to "everything up to /". Cheap-and-cheerful;
        // the runtime caller always passes a fully-qualified URL in
        // practice, this is just defensive.
        assert_eq!(host_of_url("example.com/path"), Some("example.com"));
    }

    #[test]
    fn empty_returns_none() {
        assert_eq!(host_of_url(""), None);
    }

    #[test]
    fn only_scheme_returns_none() {
        assert_eq!(host_of_url("https://"), None);
    }
}

/// CBOR-encode a `SubscribeRequest` for transport. Thin wrapper
/// over `SubscribeRequest::encode` that surfaces the codec error
/// as a `CompanionError`.
pub fn encode_subscribe(req: &SubscribeRequest) -> Result<Vec<u8>> {
    req.encode()
        .map_err(|e| CompanionError::Wire(format!("encode_subscribe: {e}")))
}

/// CBOR-decode a `SubscribeRequest`. Mirror of `encode_subscribe`
/// for completeness — host calls `SubscribeRequest::decode` directly.
pub fn decode_subscribe(bytes: &[u8]) -> Result<SubscribeRequest> {
    SubscribeRequest::decode(bytes)
        .map_err(|e| CompanionError::Wire(format!("decode_subscribe: {e}")))
}

/// CBOR-decode a `SubscribeResponse`. Both directions ride CBOR
/// — the encoding is fixed by `SubscribeResponse::decode` /
/// `encode` co-located with the type in `mitos-protocol`, so
/// host and consumer can never disagree on encoding.
pub fn decode_subscribe_response(bytes: &[u8]) -> Result<SubscribeResponse> {
    SubscribeResponse::decode(bytes)
        .map_err(|e| CompanionError::Wire(format!("decode_subscribe_response: {e}")))
}

/// Post an interest mutation (`Add` / `Remove`) to the mitos host.
///
/// Targets `POST {MITOS_HOST_URL}/api/companions/{key}/interest`
/// with a CBOR-encoded [`mitos_protocol::InterestMutationBody`].
/// Used by the runtime's `handle_interest_subscribe` /
/// `handle_interest_unsubscribe` handlers to push deltas to
/// mitos without the full re-subscribe round-trip — keeps the
/// running follower's filter in sync with the companion's local
/// SQL canonical interest set.
///
/// Best-effort from the caller's perspective: the SQL row is the
/// source of truth, so a transient host-side failure here leaves
/// the companion correct (the next reconnect-time `Replace` via
/// `/api/companions/subscribe` rehydrates).
#[cfg(target_arch = "wasm32")]
pub async fn post_interest_mutation(
    env: &worker::Env,
    companion_key: &str,
    op: mitos_protocol::InterestOp,
    items: Vec<mitos_protocol::Interest>,
) -> Result<()> {
    use js_sys::Uint8Array;
    use worker::{Fetch, Headers, Method, Request, RequestInit};

    let host_url = env
        .var(MITOS_HOST_URL_ENV)
        .map_err(|e| CompanionError::Wire(format!("read {MITOS_HOST_URL_ENV}: {e}")))?
        .to_string();
    let token = read_auth_token(env).await?;

    let body = mitos_protocol::InterestMutationBody { op, items };
    let body_bytes = mitos_protocol::encode_interest_mutation(&body)
        .map_err(|e| CompanionError::Wire(format!("encode interest mutation: {e}")))?;
    let body_array = Uint8Array::from(body_bytes.as_slice());

    let headers = Headers::new();
    headers
        .set("authorization", &format!("Bearer {token}"))
        .map_err(|e| CompanionError::Wire(format!("set auth header: {e}")))?;
    headers
        .set("content-type", mitos_protocol::HTTP_DELIVERY_MIME)
        .map_err(|e| CompanionError::Wire(format!("set content-type: {e}")))?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post)
        .with_headers(headers)
        .with_body(Some(body_array.into()));

    let url = format!(
        "{}/api/companions/{}/interest",
        host_url.trim_end_matches('/'),
        companion_key,
    );
    let req = Request::new_with_init(&url, &init)
        .map_err(|e| CompanionError::Wire(format!("build mutation request: {e}")))?;

    let mut response = Fetch::Request(req)
        .send()
        .await
        .map_err(|e| CompanionError::Wire(format!("mutation send: {e}")))?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        let body = response.text().await.unwrap_or_default();
        return Err(CompanionError::Wire(format!(
            "interest mutation non-2xx ({status}): {body}"
        )));
    }
    Ok(())
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
    let token = read_auth_token(env).await?;

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
    use mitos_protocol::{ChainPoint, Interest, SubscribeTarget};

    #[test]
    fn round_trip_subscribe_request() {
        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "ownership-indexer".into(),
            }],
            companion_key: "customer_42".into(),
            client_id: "test-client".into(),
            resume_from: Some(ChainPoint::Specific(123, "abcd".into())),
            interests: vec![Interest::any()],
            dial_back: None,
        };
        let bytes = encode_subscribe(&req).unwrap();
        let decoded = decode_subscribe(&bytes).unwrap();
        assert_eq!(decoded.targets.len(), 1);
        assert_eq!(decoded.single_module_target(), Some("ownership-indexer"));
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
            targets: vec![SubscribeTarget::Module {
                name: "tenant-saas".into(),
            }],
            companion_key: "tenant_001".into(),
            client_id: "tenant001.example.com".into(),
            resume_from: None,
            interests: vec![],
            dial_back: Some(DialBackOverride {
                url: Some("https://tenant001.example.com/_internal/{op}-{target}?key={key}".into()),
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
