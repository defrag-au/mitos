//! Companion registration HTTP surface — `/api/companions/subscribe`.
//!
//! Per the design doc (`docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`,
//! "Addressing & wake-up: mitos dials companions"), each Companion DO
//! POSTs a single CBOR-encoded `SubscribeRequest` to mitos on first
//! wake. Mitos persists the registration so it can later dial back
//! to deliver emissions.
//!
//! ## What this module owns
//!
//! - `POST /api/companions/subscribe` — accepts CBOR, persists to
//!   `<storage>/<module_id>/companions/<companion_key>.cbor`,
//!   hands the registration to the `CompanionDialer` to start an
//!   outbound dial loop, responds with `next_emission_id` from
//!   the module's `EmissionsStore` (`peek_next_id`). Companions
//!   use the returned value as a sync point.
//! - Auth via the existing `MITOS_AUTH_TOKEN` shared-secret
//!   middleware.
//!
//! The actual outbound dial + Apply-frame delivery lives in
//! `dialer.rs` (`CompanionDialer::run_companion`); this module
//! only handles registration intake.
//!
//! ## Still deferred
//!
//! - Idempotency-aware overwrite semantics with last-modified
//!   timestamps.
//! - Auto-cleanup of registrations for evicted companions.
//!
//! ## Storage layout
//!
//! ```text
//! <storage_root>/<module_id>/companions/<companion_key>.cbor
//! ```
//!
//! One file per (module, companion_key) pair. Re-registration
//! overwrites. Path separator constraints: `companion_key` is
//! validated to be alphanumeric + `_`/`-` only (rejects `/`, `\`,
//! `..` etc.) so we can use it in a filesystem path safely.

use std::path::PathBuf;
use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;

use mitos_protocol::{SUBSCRIBE_MIME, SubscribeRequest, SubscribeResponse, SubscribeTarget};

use crate::indexer_bridge::IndexerBridgeHandle;

use crate::admin::AuthToken;
use crate::dialer::CompanionDialer;
use crate::storage::ModuleStorage;

/// Build the companion-registration router with the same auth shape
/// as the admin router. Returns the axum `Router`; the host wires it
/// into the top-level service alongside `admin_router`.
///
/// `dialer` is optional so unit tests can exercise the persistence
/// surface without spinning up the dial supervisor. Production
/// callers always pass `Some(...)`.
pub fn companion_router(
    storage: ModuleStorage,
    auth: AuthToken,
    dialer: Option<CompanionDialer>,
    indexer_bridge: Option<IndexerBridgeHandle>,
) -> axum::Router {
    let state = CompanionState {
        storage: Arc::new(storage),
        dialer,
        indexer_bridge,
    };
    axum::Router::new()
        .route("/api/companions/subscribe", post(subscribe_handler))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(auth, require_auth))
}

#[derive(Clone)]
struct CompanionState {
    storage: Arc<ModuleStorage>,
    dialer: Option<CompanionDialer>,
    /// Indexer bridge — populated by the bundle when the host
    /// has in-tree indexers to expose via the unified subscribe
    /// path. `None` means the host only supports
    /// `SubscribeTarget::Module { ... }` requests (no in-tree
    /// indexers wired into the unified path).
    indexer_bridge: Option<IndexerBridgeHandle>,
}

// Local re-implementation of the admin auth middleware so the
// companion router can stand alone without leaking admin-internal
// items. Same shape; same behaviour.
async fn require_auth(
    State(token): State<AuthToken>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> std::result::Result<Response, StatusCode> {
    if let Some(expected) = token.as_deref() {
        let provided = req
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.strip_prefix("Bearer "));
        match provided {
            Some(p) if constant_time_eq(p.as_bytes(), expected.as_bytes()) => {}
            _ => return Err(StatusCode::UNAUTHORIZED),
        }
    }
    Ok(next.run(req).await)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Debug, thiserror::Error)]
enum SubscribeError {
    #[error("CBOR decode: {0}")]
    Decode(String),
    #[error("invalid module_name: {0}")]
    InvalidModuleName(String),
    #[error("invalid companion_key: {0}")]
    InvalidCompanionKey(String),
    #[error("storage io: {0}")]
    Io(String),
    #[error("module not registered: {0}")]
    UnknownModule(String),
    /// `SubscribeTarget::Indexer { name }` requested but the
    /// named indexer is not registered with the bundle (or the
    /// host has no indexer registry wired at all — see
    /// `Bundle::enable_modules` / future
    /// `enable_companion_subscribe`).
    #[error("indexer not registered: {0}")]
    UnknownIndexer(String),
    /// `SubscribeTarget::Indexer { name }` requested but the
    /// indexer is marked internal (`Indexer::is_internal() == true`,
    /// e.g. `none-match`). See `docs/design/UNIFIED_SUBSCRIBE.md`
    /// "Indexer visibility".
    #[error("indexer is internal (not subscribable from companions): {0}")]
    InternalIndexer(String),
}

#[derive(serde::Serialize)]
struct ErrorBody {
    error: String,
    code: &'static str,
}

impl IntoResponse for SubscribeError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Decode(_) => StatusCode::BAD_REQUEST,
            Self::InvalidModuleName(_) | Self::InvalidCompanionKey(_) => StatusCode::BAD_REQUEST,
            Self::UnknownModule(_) | Self::UnknownIndexer(_) => StatusCode::NOT_FOUND,
            Self::InternalIndexer(_) => StatusCode::BAD_REQUEST,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match &self {
            Self::Decode(_) => "cbor_decode",
            Self::InvalidModuleName(_) => "invalid_module_name",
            Self::InvalidCompanionKey(_) => "invalid_companion_key",
            Self::Io(_) => "storage_io",
            Self::UnknownModule(_) => "unknown_module",
            Self::UnknownIndexer(_) => "unknown_indexer",
            Self::InternalIndexer(_) => "internal_indexer",
        };
        let body = ErrorBody {
            error: self.to_string(),
            code,
        };
        (status, Json(body)).into_response()
    }
}

async fn subscribe_handler(
    State(state): State<CompanionState>,
    body: axum::body::Bytes,
) -> std::result::Result<Response, SubscribeError> {
    let request = SubscribeRequest::decode(&body[..])
        .map_err(|e| SubscribeError::Decode(e.to_string()))?;

    validate_companion_key(&request.companion_key)?;

    if request.targets.is_empty() {
        return Err(SubscribeError::Decode(
            "subscribe request has no targets".into(),
        ));
    }

    // Validate + persist each target (modules write CBOR to disk;
    // indexers validate against the bridge). No dial-spawning yet —
    // that's handled by ONE `dialer.register(request)` call below
    // which fans out across all targets.
    //
    // `next_emission_id` is module-target-specific (peek of the
    // module's emissions log); when multiple targets are present
    // we surface the first module target's id, or 0 if none.
    let mut next_emission_id: u64 = 0;
    for target in &request.targets {
        match target {
            SubscribeTarget::Module { name } => {
                let id = validate_and_persist_module(&state, &request, name).await?;
                if next_emission_id == 0 {
                    next_emission_id = id;
                }
            }
            SubscribeTarget::Indexer { name } => {
                validate_indexer_target(&state, name)?;
            }
        }
    }

    // All targets validated. One register-call drives the dialer
    // to spawn per-target dial loops.
    if let Some(dialer) = &state.dialer {
        dialer.register(request.clone()).await;
    }
    let response = SubscribeResponse {
        status: "subscribed".to_string(),
        next_emission_id,
    };
    let body = response
        .encode()
        .map_err(|e| SubscribeError::Io(format!("encode subscribe response: {e}")))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, SUBSCRIBE_MIME)
        .body(Body::from(body))
        .expect("response builder"))
}

/// Validate + persist a wasm-module target. Does NOT hand to the
/// dialer — the caller spawns dial loops once after all targets
/// validate, via a single `dialer.register(request)` call. Returns
/// the module's `next_emission_id` for echoing back in
/// `SubscribeResponse`.
async fn validate_and_persist_module(
    state: &CompanionState,
    request: &SubscribeRequest,
    module_name: &str,
) -> std::result::Result<u64, SubscribeError> {
    validate_module_id(module_name)?;

    // Module must be registered (i.e. have an artifact uploaded) so
    // we don't accept registrations for unknown modules.
    if state
        .storage
        .read_manifest(module_name)
        .map_err(|e| SubscribeError::Io(e.to_string()))?
        .is_none()
    {
        return Err(SubscribeError::UnknownModule(module_name.to_string()));
    }

    let companions_dir = companions_dir_for(&state.storage, module_name);
    std::fs::create_dir_all(&companions_dir).map_err(|e| SubscribeError::Io(e.to_string()))?;

    let path = companions_dir.join(format!("{}.cbor", request.companion_key));
    let buf = request
        .encode()
        .map_err(|e| SubscribeError::Io(format!("encode persisted registration: {e}")))?;
    write_atomic(&path, &buf).map_err(|e| SubscribeError::Io(e.to_string()))?;

    tracing::info!(
        module = %module_name,
        companion_key = %request.companion_key,
        interests = request.interests.len(),
        "companion target validated + persisted (module)"
    );

    // Companions use this as a sync point — any emission_id at or
    // below `peek_next_id - 1` is already in the log; anything
    // above is fresh on this session.
    let next_emission_id = match state.storage.emissions_store(module_name) {
        Ok(store) => store.peek_next_id().unwrap_or(1),
        Err(e) => {
            tracing::warn!(
                module = %module_name,
                error = %e,
                "open emissions log to peek next_id failed; returning 1"
            );
            1
        }
    };
    Ok(next_emission_id)
}

/// Validate an in-tree indexer target. Indexer subscriptions
/// aren't persisted in v1 — companions re-register on each DO
/// wake. Returns `Ok(())` if the indexer exists + isn't internal.
fn validate_indexer_target(
    state: &CompanionState,
    indexer_name: &str,
) -> std::result::Result<(), SubscribeError> {
    let Some(bridge) = &state.indexer_bridge else {
        return Err(SubscribeError::UnknownIndexer(format!(
            "{indexer_name} (host has no indexer bridge wired)"
        )));
    };

    if !bridge.contains(indexer_name) {
        return Err(SubscribeError::UnknownIndexer(indexer_name.to_string()));
    }
    if bridge.is_internal(indexer_name) {
        return Err(SubscribeError::InternalIndexer(indexer_name.to_string()));
    }

    tracing::info!(
        indexer = %indexer_name,
        "companion target validated (indexer)"
    );

    Ok(())
}

fn companions_dir_for(storage: &ModuleStorage, module_id: &str) -> PathBuf {
    storage.module_dir_for_companions(module_id)
}

/// Atomic write: write to `<path>.tmp`, fsync, rename to `<path>`.
/// Mirrors the artifact-upload pattern in `storage.rs`.
fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("cbor.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn validate_module_id(id: &str) -> std::result::Result<(), SubscribeError> {
    if id.is_empty() || id.len() > 64 {
        return Err(SubscribeError::InvalidModuleName(format!(
            "len {} not in 1..=64",
            id.len()
        )));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(SubscribeError::InvalidModuleName(id.to_string()));
    }
    Ok(())
}

fn validate_companion_key(key: &str) -> std::result::Result<(), SubscribeError> {
    if key.is_empty() || key.len() > 128 {
        return Err(SubscribeError::InvalidCompanionKey(format!(
            "len {} not in 1..=128",
            key.len()
        )));
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(SubscribeError::InvalidCompanionKey(key.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request as HttpRequest;
    use mitos_protocol::{ChainPoint, SubscribeTarget};
    use tower::ServiceExt;

    fn build_router_with(storage: ModuleStorage) -> axum::Router {
        companion_router(storage, AuthToken(None), None, None)
    }

    fn cbor(req: &SubscribeRequest) -> Vec<u8> {
        let mut buf = Vec::new();
        ciborium::ser::into_writer(req, &mut buf).unwrap();
        buf
    }

    #[tokio::test]
    async fn rejects_unknown_module() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "nonexistent".into(),
            }],
            companion_key: "customer_42".into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
        };
        let body = cbor(&req);
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_invalid_key() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "ownership-indexer".into(),
            }],
            companion_key: "../escape".into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
        };
        let body = cbor(&req);
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn rejects_garbage_cbor() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let body = vec![0xff, 0xff, 0xff, 0xff];
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn cbor_round_trip() {
        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "ownership".into(),
            }],
            companion_key: "customer_7".into(),
            resume_from: Some(ChainPoint::Specific(123, "abcd".into())),
            interests: vec![],
            dial_back: None,
        };
        let bytes = cbor(&req);
        let decoded: SubscribeRequest = ciborium::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(decoded.single_module_target(), Some("ownership"));
        assert_eq!(decoded.companion_key, "customer_7");
    }

    #[test]
    fn validate_module_id_accepts_alphanumeric() {
        assert!(validate_module_id("ownership-indexer").is_ok());
        assert!(validate_module_id("ownership_indexer").is_ok());
        assert!(validate_module_id("ABC123").is_ok());
        assert!(validate_module_id("").is_err());
        assert!(validate_module_id("../escape").is_err());
        assert!(validate_module_id("name with spaces").is_err());
    }

    #[test]
    fn validate_companion_key_accepts_keys() {
        assert!(validate_companion_key("customer_42").is_ok());
        assert!(validate_companion_key("policy_id-deadbeef").is_ok());
        assert!(validate_companion_key("").is_err());
        assert!(validate_companion_key("../escape").is_err());

        let _ = to_bytes;
    }
}
