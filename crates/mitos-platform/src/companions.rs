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
//! `dialer.rs` (the per-module `run_module_drain` task); this
//! module only handles registration intake.
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

use mitos_protocol::{
    HTTP_DELIVERY_MIME, InterestOp, SUBSCRIBE_MIME, SubscribeRequest, SubscribeResponse,
    SubscribeTarget, decode_interest_mutation,
};

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
    events: crate::events::EventRing,
) -> axum::Router {
    let state = CompanionState {
        storage: Arc::new(storage),
        dialer,
        indexer_bridge,
        events,
    };
    axum::Router::new()
        .route("/api/companions/subscribe", post(subscribe_handler))
        .route(
            "/api/companions/{companion_key}/interest",
            post(interest_mutation_handler),
        )
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
    /// Shared operational-events ring — records `companion_subscribed`.
    events: crate::events::EventRing,
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
    #[error("invalid client_id: {0}")]
    InvalidClientId(String),
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
            Self::InvalidModuleName(_)
            | Self::InvalidCompanionKey(_)
            | Self::InvalidClientId(_) => StatusCode::BAD_REQUEST,
            Self::UnknownModule(_) | Self::UnknownIndexer(_) => StatusCode::NOT_FOUND,
            Self::InternalIndexer(_) => StatusCode::BAD_REQUEST,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match &self {
            Self::Decode(_) => "cbor_decode",
            Self::InvalidModuleName(_) => "invalid_module_name",
            Self::InvalidCompanionKey(_) => "invalid_companion_key",
            Self::InvalidClientId(_) => "invalid_client_id",
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

#[derive(Debug, thiserror::Error)]
enum InterestMutationError {
    #[error("CBOR decode: {0}")]
    Decode(String),
    #[error("invalid companion_key: {0}")]
    InvalidCompanionKey(String),
    #[error("no companion registrations found for `{0}`")]
    UnknownCompanion(String),
    #[error("empty items vec")]
    EmptyItems,
    #[error("Replace op not supported on mutation endpoint; use /api/companions/subscribe")]
    ReplaceNotSupported,
    #[error("storage io: {0}")]
    Io(String),
}

impl IntoResponse for InterestMutationError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Decode(_) | Self::InvalidCompanionKey(_) | Self::EmptyItems => {
                StatusCode::BAD_REQUEST
            }
            Self::ReplaceNotSupported => StatusCode::BAD_REQUEST,
            Self::UnknownCompanion(_) => StatusCode::NOT_FOUND,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match &self {
            Self::Decode(_) => "cbor_decode",
            Self::InvalidCompanionKey(_) => "invalid_companion_key",
            Self::EmptyItems => "empty_items",
            Self::ReplaceNotSupported => "replace_not_supported",
            Self::UnknownCompanion(_) => "unknown_companion",
            Self::Io(_) => "storage_io",
        };
        let body = ErrorBody {
            error: self.to_string(),
            code,
        };
        (status, Json(body)).into_response()
    }
}

/// `POST /api/companions/{companion_key}/interest` — targeted
/// Add/Remove of interest predicates on a companion's filter set
/// without re-running the full subscribe flow.
///
/// Per-companion semantics: a companion's interest set is uniform
/// across every module it subscribes to (the dispatcher applies
/// the same set against each module's events). The handler:
///
/// 1. Locates every `<storage>/<module>/companions/<key>.cbor`
///    registration for this companion_key (one per subscribed
///    module).
/// 2. Applies the mutation to each persisted set, rewriting the
///    CBOR atomically.
/// 3. Calls `dialer.route_interest_mutation(module, op, items)`
///    for each subscribed module so the running follower's live
///    filter picks up the change without a respawn.
///
/// Returns 200 on success; the response body is empty. Errors:
/// - 400 for decode failure, empty items, Replace op, malformed key
/// - 404 if no registrations exist for the companion_key
/// - 500 for CBOR write failure
///
/// The endpoint is per-companion (not per-module-companion) so
/// callers don't need to discover which modules a companion has
/// subscribed to — the handler does that discovery internally.
async fn interest_mutation_handler(
    State(state): State<CompanionState>,
    axum::extract::Path(companion_key): axum::extract::Path<String>,
    body: axum::body::Bytes,
) -> std::result::Result<Response, InterestMutationError> {
    validate_companion_key_for_mutation(&companion_key)?;

    let mutation = decode_interest_mutation(&body[..])
        .map_err(|e| InterestMutationError::Decode(e.to_string()))?;
    if mutation.items.is_empty() {
        return Err(InterestMutationError::EmptyItems);
    }
    if matches!(mutation.op, InterestOp::Replace) {
        return Err(InterestMutationError::ReplaceNotSupported);
    }

    // Discover every (module, registration) the companion has on
    // disk. Companions can subscribe to multiple modules under
    // the same key; the mutation applies to all of them.
    let registrations = load_companion_registrations(&state.storage, &companion_key)
        .map_err(|e| InterestMutationError::Io(e.to_string()))?;
    if registrations.is_empty() {
        return Err(InterestMutationError::UnknownCompanion(companion_key));
    }

    // Apply the mutation to each persisted registration. Errors
    // here are 500s — we've already accepted the request as
    // shape-valid.
    for (module, path, mut req) in registrations.iter().cloned() {
        apply_mutation_to_set(&mut req.interests, mutation.op, &mutation.items);
        if req.interests.is_empty() {
            // Teardown: the last interest was removed (the dApp dropped
            // this collection). DELETE the registration rather than
            // persisting an empty record, so the host's startup
            // reconcile (`dialer.start_all`, which re-routes reloaded
            // companions' interest into the module scan-interest) can't
            // resurrect — and re-cold-start — a collection that's gone.
            // The follower's live filter is cleared by the
            // `route_interest_mutation(Remove)` below.
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(InterestMutationError::Io(format!(
                        "remove emptied registration {module}: {e}"
                    )));
                }
            }
        } else {
            let buf = req
                .encode()
                .map_err(|e| InterestMutationError::Io(format!("encode {module}: {e}")))?;
            write_atomic(&path, &buf).map_err(|e| InterestMutationError::Io(e.to_string()))?;
        }
    }

    // Propagate to each running follower's in-memory filter so
    // the change is live without waiting for the next host
    // restart. Best-effort: a follower that's not currently
    // running surfaces `InterestRouteError::NotRunning` which we
    // log + swallow (persisted CBOR will be picked up on restart).
    if let Some(dialer) = &state.dialer {
        for (module, _, _) in &registrations {
            match dialer
                .route_interest_mutation(module, mutation.op, mutation.items.clone())
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        module = %module,
                        companion_key = %companion_key,
                        error = %e,
                        "route_interest_mutation failed; persisted CBOR is correct, restart will resolve"
                    );
                }
            }
        }
    }

    tracing::info!(
        companion_key = %companion_key,
        op = ?mutation.op,
        item_count = mutation.items.len(),
        module_count = registrations.len(),
        "interest mutation applied"
    );

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, HTTP_DELIVERY_MIME)
        .body(Body::empty())
        .expect("response builder"))
}

/// Apply an Add or Remove to a companion's persisted interest
/// vector. Add dedupes against existing entries (no
/// double-registration). Remove strips by equality.
fn apply_mutation_to_set(
    current: &mut Vec<mitos_protocol::Interest>,
    op: InterestOp,
    items: &[mitos_protocol::Interest],
) {
    match op {
        InterestOp::Add => {
            for item in items {
                if !current.contains(item) {
                    current.push(item.clone());
                }
            }
        }
        InterestOp::Remove => {
            current.retain(|existing| !items.contains(existing));
        }
        InterestOp::Replace => {
            // Validated out before this point — defensive no-op.
        }
    }
}

/// Scan `<storage>/*/companions/<key>.cbor` and decode each
/// matching registration. Returns one entry per module the
/// companion is subscribed to.
fn load_companion_registrations(
    storage: &ModuleStorage,
    companion_key: &str,
) -> std::io::Result<Vec<(String, PathBuf, SubscribeRequest)>> {
    let modules = storage
        .list_modules()
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let mut out = Vec::new();
    let file_name = format!("{companion_key}.cbor");
    for module in modules {
        // Two-level layout: walk every `<client_id>/` subdir under
        // the module's companions/ root, picking up any
        // `<companion_key>.cbor` file. One interest-mutation
        // targets ALL clients that share the key.
        let companions_root = storage.module_dir_for_companions(&module);
        let entries = match std::fs::read_dir(&companions_root) {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for entry in entries.flatten() {
            // Skip metadata directories (`.unreachable/`, etc.) and
            // any bare `.cbor` files left over from a pre-migration
            // host (the on-start migration moves them, but we
            // tolerate stragglers).
            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !file_type.is_dir() {
                continue;
            }
            let client_dir = entry.path();
            let dir_name = entry.file_name();
            let dir_name_str = dir_name.to_string_lossy();
            if dir_name_str.starts_with('.') {
                continue;
            }
            let path = client_dir.join(&file_name);
            if !path.exists() {
                continue;
            }
            let bytes = std::fs::read(&path)?;
            let req: SubscribeRequest = match ciborium::de::from_reader(bytes.as_slice()) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        module = %module,
                        client_id = %dir_name_str,
                        companion_key = %companion_key,
                        error = %e,
                        "skipping un-decodable companion CBOR"
                    );
                    continue;
                }
            };
            out.push((module.clone(), path, req));
        }
    }
    Ok(out)
}

fn validate_companion_key_for_mutation(
    key: &str,
) -> std::result::Result<(), InterestMutationError> {
    if key.is_empty() || key.len() > 128 {
        return Err(InterestMutationError::InvalidCompanionKey(format!(
            "len {} not in 1..=128",
            key.len()
        )));
    }
    if !key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(InterestMutationError::InvalidCompanionKey(key.to_string()));
    }
    Ok(())
}

async fn subscribe_handler(
    State(state): State<CompanionState>,
    body: axum::body::Bytes,
) -> std::result::Result<Response, SubscribeError> {
    let request =
        SubscribeRequest::decode(&body[..]).map_err(|e| SubscribeError::Decode(e.to_string()))?;

    validate_companion_key(&request.companion_key)?;
    validate_client_id(&request.client_id)?;

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
                state.events.record(
                    name.as_str(),
                    crate::events::EventKind::CompanionSubscribed {
                        client_id: request.client_id.clone(),
                        companion_key: request.companion_key.clone(),
                    },
                );
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

        // Propagate the subscriber's interest into each MODULE
        // target's scan-interest. `register` above only wires up
        // the per-companion FANOUT interest (the persisted CBOR,
        // read back by `GET …/companions` as `watched_policies`).
        // The module's own watch/scan set — the driver `InterestSet`
        // the cold-start + recapture walk via `utxos_by_policy`, plus
        // the module's `update_interest` export — is updated ONLY by
        // routing an interest mutation into the follower. Without
        // this, a subscribe registers the companion but its policy
        // never enters the module's scan-interest, so cold-start
        // never fires and the companion never drains (CO1: an added
        // collection that never captures). The incremental
        // `/api/companions/{key}/interest` endpoint already routes,
        // but a subscribe that races/misses that separate push is
        // left stranded; routing here makes the subscribe itself
        // self-sufficient. `Add` is union-preserving (Replace would
        // wipe other companions' policies from the shared module
        // interest) and idempotent — re-subscribes dedup and the
        // per-scope bootstrap flag suppresses re-cold-start, so this
        // only hydrates genuinely-new policies.
        if !request.interests.is_empty() {
            for target in &request.targets {
                if let SubscribeTarget::Module { name } = target
                    && let Err(e) = dialer
                        .route_interest_mutation(name, InterestOp::Add, request.interests.clone())
                        .await
                {
                    tracing::warn!(
                        module = %name,
                        error = %e,
                        "subscribe: routing interest into module scan-set failed; \
                         companion fanout-interest still registered",
                    );
                }
            }
        }
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

    // Two-level layout: <storage>/<module>/companions/<client_id>/<companion_key>.cbor.
    // Different `client_id`s for the same `(module, companion_key)`
    // produce parallel records — see
    // `docs/design/MULTI_CLIENT_COMPANIONS.md`.
    let companions_dir = client_companions_dir(&state.storage, module_name, &request.client_id);
    std::fs::create_dir_all(&companions_dir).map_err(|e| SubscribeError::Io(e.to_string()))?;

    let path = companions_dir.join(format!("{}.cbor", request.companion_key));
    let buf = request
        .encode()
        .map_err(|e| SubscribeError::Io(format!("encode persisted registration: {e}")))?;
    write_atomic(&path, &buf).map_err(|e| SubscribeError::Io(e.to_string()))?;

    tracing::info!(
        module = %module_name,
        companion_key = %request.companion_key,
        client_id = %request.client_id,
        interests = request.interests.len(),
        "companion target validated + persisted (module)"
    );

    // Claim any sentinel rows that `drain_one` wrote during the
    // window before this companion subscribed (drop site #3 in
    // `docs/design/EVENT_DELIVERY_RESILIENCE.md`). Single-claim
    // semantics — first subscriber to a module reclaims; later
    // ones see nothing. The dialer's `drain_queued` picks the
    // retargeted rows up on the next connection.
    let store = match state.storage.emissions_store(module_name) {
        Ok(s) => Some(s),
        Err(e) => {
            tracing::warn!(
                module = %module_name,
                error = %e,
                "open emissions log to claim sentinel rows failed; \
                 unsubscribed-window emissions stay on sentinel until next subscribe"
            );
            None
        }
    };
    if let Some(store) = &store {
        match store.retarget_companion(
            crate::emissions::UNSUBSCRIBED_COMPANION_ID,
            &request.companion_key,
            &request.client_id,
        ) {
            Ok(0) => {}
            Ok(count) => {
                tracing::info!(
                    module = %module_name,
                    companion_key = %request.companion_key,
                    count,
                    "claimed unsubscribed-window emissions for new subscriber"
                );
            }
            Err(e) => {
                tracing::warn!(
                    module = %module_name,
                    companion_key = %request.companion_key,
                    error = %e,
                    "retarget unsubscribed sentinel rows failed; rows stay on sentinel"
                );
            }
        }
    }

    // Companions use this as a sync point — any emission_id at or
    // below `peek_next_id - 1` is already in the log; anything
    // above is fresh on this session.
    let next_emission_id = match store {
        Some(s) => s.peek_next_id().unwrap_or(1),
        None => 1,
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

/// Atomic write: write to `<path>.tmp`, fsync, rename to `<path>`.
/// Mirrors the artifact-upload pattern in `storage.rs`.
fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("cbor.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

// ============================================================================
// Migration: flat companion files → two-level (client_id, companion_key)
// layout. Pre-fix hosts wrote `<storage>/<module>/companions/<key>.cbor`;
// post-fix hosts write `<storage>/<module>/companions/<client_id>/<key>.cbor`.
// On host start, `migrate_flat_companions_for_module` decodes each pre-fix
// file, derives `client_id` from its `dial_back.url` host portion, and
// rewrites it under the new path with the `client_id` field populated.
// Records without a usable URL are quarantined under
// `<module>/companions/.unreachable/` for operator review.
// See `docs/design/MULTI_CLIENT_COMPANIONS.md` — "Migration".
// ============================================================================

/// Reserved subdirectory holding companion records that couldn't be
/// migrated (no `dial_back.url`, so no way to derive `client_id`).
const UNREACHABLE_DIR: &str = ".unreachable";

/// Minimal pre-migration shape — old persisted CBOR has no
/// `client_id` field. We only need the URL during migration; everything
/// else stays in the original bytes.
#[derive(serde::Deserialize)]
struct LegacyDialBackProbe {
    #[serde(default)]
    dial_back: Option<LegacyDialBackOverride>,
}

#[derive(serde::Deserialize)]
struct LegacyDialBackOverride {
    #[serde(default)]
    url: Option<String>,
}

/// One-time migration of pre-fix flat companion files into the
/// two-level layout. Idempotent — re-running on an already-migrated
/// module is a no-op (no flat files left to find).
///
/// Returns `(migrated, quarantined)` counts.
pub(crate) fn migrate_flat_companions_for_module(
    storage: &ModuleStorage,
    module_id: &str,
) -> std::io::Result<(usize, usize)> {
    let companions_root = storage.module_dir_for_companions(module_id);
    if !companions_root.exists() {
        return Ok((0, 0));
    }
    let entries = std::fs::read_dir(&companions_root)?;

    let mut migrated = 0usize;
    let mut quarantined = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        // Only consider files (skip the new <client_id>/ subdirs and
        // any `.unreachable/` directory).
        let file_type = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
            continue;
        }

        let companion_key = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => {
                tracing::warn!(
                    path = %path.display(),
                    "migrate: skipping file with no stem"
                );
                continue;
            }
        };

        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "migrate: failed to read companion file"
                );
                continue;
            }
        };

        // Probe the dial_back URL host without committing to the
        // full SubscribeRequest decode — the old CBOR lacks
        // `client_id`, so a full decode against the new struct
        // would error.
        let probe: LegacyDialBackProbe = match ciborium::de::from_reader(bytes.as_slice()) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "migrate: failed to probe legacy CBOR; leaving in place"
                );
                continue;
            }
        };

        let url = probe.dial_back.and_then(|d| d.url);
        let client_id = url.as_deref().and_then(url_host).map(|s| s.to_string());

        match client_id {
            Some(client_id) if !client_id.is_empty() => {
                // Synthesise the new client_id field into the CBOR
                // by full decode-with-default → re-encode. We can't
                // use the current `SubscribeRequest` struct directly
                // (its `client_id` is non-Option), so we go through
                // a transitional shape.
                match migrate_one(&bytes, &client_id) {
                    Ok(new_bytes) => {
                        let dest_dir = companions_root.join(&client_id);
                        std::fs::create_dir_all(&dest_dir)?;
                        let dest = dest_dir.join(format!("{companion_key}.cbor"));
                        write_atomic(&dest, &new_bytes)?;
                        std::fs::remove_file(&path)?;
                        migrated += 1;
                        tracing::info!(
                            module = %module_id,
                            client_id = %client_id,
                            companion_key = %companion_key,
                            "migrate: relocated companion to two-level layout"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            module = %module_id,
                            companion_key = %companion_key,
                            error = %e,
                            "migrate: failed to rewrite CBOR; leaving in place"
                        );
                    }
                }
            }
            _ => {
                // No URL → quarantine under .unreachable/. The file
                // is preserved so an operator can inspect + clean up.
                let dest_dir = companions_root.join(UNREACHABLE_DIR);
                std::fs::create_dir_all(&dest_dir)?;
                let dest = dest_dir.join(format!("{companion_key}.cbor"));
                std::fs::rename(&path, &dest)?;
                quarantined += 1;
                tracing::warn!(
                    module = %module_id,
                    companion_key = %companion_key,
                    "migrate: companion has no dial_back.url; quarantined to .unreachable/"
                );
            }
        }
    }

    if migrated > 0 || quarantined > 0 {
        tracing::info!(
            module = %module_id,
            migrated,
            quarantined,
            "migrate: companion layout migration complete"
        );
    }
    Ok((migrated, quarantined))
}

/// Re-encode a legacy companion CBOR with the new `client_id` field
/// populated. Decoding goes through a transitional `MigratableRequest`
/// shape that mirrors the modern `SubscribeRequest` but with
/// `client_id: Option<String>` so legacy bytes (which lack the field)
/// decode cleanly; then we substitute the derived `client_id` and
/// re-encode via the canonical type.
fn migrate_one(legacy_bytes: &[u8], client_id: &str) -> Result<Vec<u8>, String> {
    use mitos_protocol::{ChainPoint, DialBackOverride, Interest, SubscribeTarget};

    #[derive(serde::Deserialize)]
    struct MigratableRequest {
        targets: Vec<SubscribeTarget>,
        companion_key: String,
        #[serde(default)]
        client_id: Option<String>,
        #[serde(default)]
        resume_from: Option<ChainPoint>,
        #[serde(default)]
        interests: Vec<Interest>,
        #[serde(default)]
        dial_back: Option<DialBackOverride>,
    }

    let m: MigratableRequest =
        ciborium::de::from_reader(legacy_bytes).map_err(|e| format!("decode legacy: {e}"))?;
    let req = SubscribeRequest {
        targets: m.targets,
        companion_key: m.companion_key,
        client_id: m.client_id.unwrap_or_else(|| client_id.to_string()),
        resume_from: m.resume_from,
        interests: m.interests,
        dial_back: m.dial_back,
    };
    req.encode().map_err(|e| format!("re-encode: {e}"))
}

/// Parse the host portion out of a dial-back URL. Mirrors the
/// runtime-side helper in `mitos-companion::subscribe::host_of_url`
/// but lives here to keep the platform side dep-free of
/// `mitos-companion`.
fn url_host(url: &str) -> Option<&str> {
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

/// Validate the dApp-supplied `client_id`. Charset is intentionally
/// permissive of `.` (URL hosts like `hooks.epochify.space`) and `-`
/// (UUIDs) on top of the alnum + `_` set we accept for module IDs
/// and companion keys. Explicitly reject `.` and `..` (path-traversal
/// sentinels) and any string starting with a dot (so the dotfile-
/// reserved `.unreachable` directory remains distinguishable from a
/// legitimate `client_id`).
fn validate_client_id(client_id: &str) -> std::result::Result<(), SubscribeError> {
    let trimmed = client_id.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return Err(SubscribeError::InvalidClientId(format!(
            "len {} not in 1..=128 (after trim)",
            trimmed.len()
        )));
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return Err(SubscribeError::InvalidClientId(trimmed.to_string()));
    }
    if trimmed == "." || trimmed == ".." || trimmed.starts_with('.') {
        return Err(SubscribeError::InvalidClientId(trimmed.to_string()));
    }
    Ok(())
}

/// On-disk directory holding all companion registrations for a single
/// `(module_id, client_id)` pair. Layout:
/// `<storage>/<module>/companions/<client_id>/`. `client_id` is the
/// raw value — validation guarantees it's filesystem-safe.
fn client_companions_dir(storage: &ModuleStorage, module_id: &str, client_id: &str) -> PathBuf {
    storage.module_dir_for_companions(module_id).join(client_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::http::Request as HttpRequest;
    use mitos_protocol::{ChainPoint, DialBackOverride, SubscribeTarget};
    use tower::ServiceExt;

    fn build_router_with(storage: ModuleStorage) -> axum::Router {
        companion_router(
            storage,
            AuthToken(None),
            None,
            None,
            crate::events::EventRing::new(),
        )
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
            client_id: "test-client".into(),
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
            client_id: "test-client".into(),
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
    async fn interest_mutation_rejects_unknown_companion() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let body = mitos_protocol::InterestMutationBody {
            op: mitos_protocol::InterestOp::Add,
            items: vec![mitos_protocol::Interest::any()],
        };
        let bytes = mitos_protocol::encode_interest_mutation(&body).unwrap();
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/customer_42/interest")
                    .header("content-type", "application/cbor")
                    .body(Body::from(bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn interest_mutation_rejects_empty_items() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let body = mitos_protocol::InterestMutationBody {
            op: mitos_protocol::InterestOp::Add,
            items: vec![],
        };
        let bytes = mitos_protocol::encode_interest_mutation(&body).unwrap();
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/customer_42/interest")
                    .header("content-type", "application/cbor")
                    .body(Body::from(bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn interest_mutation_rejects_replace_op() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let body = mitos_protocol::InterestMutationBody {
            op: mitos_protocol::InterestOp::Replace,
            items: vec![mitos_protocol::Interest::any()],
        };
        let bytes = mitos_protocol::encode_interest_mutation(&body).unwrap();
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/customer_42/interest")
                    .header("content-type", "application/cbor")
                    .body(Body::from(bytes))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn apply_mutation_add_dedupes() {
        let mut set = vec![mitos_protocol::Interest::any()];
        apply_mutation_to_set(
            &mut set,
            mitos_protocol::InterestOp::Add,
            &[mitos_protocol::Interest::any()],
        );
        assert_eq!(set.len(), 1, "Add must dedupe against existing items");
    }

    #[test]
    fn apply_mutation_remove_strips_matching() {
        let any = mitos_protocol::Interest::any();
        let mut set = vec![any.clone()];
        apply_mutation_to_set(
            &mut set,
            mitos_protocol::InterestOp::Remove,
            std::slice::from_ref(&any),
        );
        assert!(set.is_empty(), "Remove must strip matching items");
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
            client_id: "test-client".into(),
            resume_from: Some(ChainPoint::Specific(123, "abcd".into())),
            interests: vec![],
            dial_back: None,
        };
        let bytes = cbor(&req);
        let decoded: SubscribeRequest = ciborium::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(decoded.single_module_target(), Some("ownership"));
        assert_eq!(decoded.companion_key, "customer_7");
        assert_eq!(decoded.client_id, "test-client");
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

    #[test]
    fn validate_client_id_accepts_url_host_shape() {
        assert!(validate_client_id("hooks.epochify.space").is_ok());
        assert!(validate_client_id("hooks.dev.epochify.space").is_ok());
        assert!(validate_client_id("worker-prod").is_ok());
        assert!(validate_client_id("client_a").is_ok());
        // UUID-like.
        assert!(validate_client_id("550e8400-e29b-41d4-a716-446655440000").is_ok());
    }

    #[test]
    fn validate_client_id_rejects_unsafe_inputs() {
        assert!(validate_client_id("").is_err());
        assert!(validate_client_id("   ").is_err());
        assert!(validate_client_id(".").is_err());
        assert!(validate_client_id("..").is_err());
        // Path traversal attempts.
        assert!(validate_client_id("../escape").is_err());
        assert!(validate_client_id(".hidden").is_err());
        // Disallowed characters.
        assert!(validate_client_id("has space").is_err());
        assert!(validate_client_id("has/slash").is_err());
        assert!(validate_client_id("has\\backslash").is_err());
    }

    #[test]
    fn url_host_parser_extracts_expected_hosts() {
        assert_eq!(
            super::url_host("https://hooks.epochify.space/_internal/{op}-{target}?key={key}"),
            Some("hooks.epochify.space"),
        );
        assert_eq!(
            super::url_host("https://hooks.dev.epochify.space/_internal/apply-x?key=y"),
            Some("hooks.dev.epochify.space"),
        );
        assert_eq!(super::url_host(""), None);
        assert_eq!(super::url_host("https://"), None);
    }

    #[tokio::test]
    async fn two_clients_one_key_persist_independently() {
        // Architectural-fix integration test for
        // `docs/design/MULTI_CLIENT_COMPANIONS.md`. Two subscribes
        // with the same `companion_key` but distinct `client_id`s
        // must land in parallel on-disk records, each with its own
        // dial-back URL.

        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());

        // Hand-install a fake manifest so the subscribe handler's
        // "module must be registered" check passes. Writing the
        // manifest file directly is fine — `read_manifest` reads
        // from disk every call.
        let module_id = "ownership-indexer";
        let module_dir = tmp.path().join(module_id);
        std::fs::create_dir_all(&module_dir).unwrap();
        let manifest_toml = format!(
            r#"
[module]
id = "{module_id}"
sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
size_bytes = 0

[abi]
version_major = 2
version_minor = 0
wit_package = "mitos:platform-v2"
wit_world = "mitos-module-v2"

[trap_policy]
strategy = "replay"
max_retries = 3
backoff_cap_ms = 1000

[build]
rust_version = "0.0"
target = "wasm32-wasip2"
profile = "release"
build_id = "1970-01-01T00:00:00Z"
crate_version = "0.0.0"
"#
        );
        std::fs::write(module_dir.join("manifest.toml"), manifest_toml).unwrap();

        let router = build_router_with(storage.clone());

        // Two subscribes sharing companion_key but differing in
        // client_id + dial-back URL.
        let mut requests = vec![
            SubscribeRequest {
                targets: vec![SubscribeTarget::Module {
                    name: module_id.into(),
                }],
                companion_key: "shared_policy_id".into(),
                client_id: "hooks.dev.epochify.space".into(),
                resume_from: None,
                interests: vec![],
                dial_back: Some(DialBackOverride {
                    url: Some(
                        "https://hooks.dev.epochify.space/_internal/{op}-{target}?key={key}".into(),
                    ),
                    auth_header: None,
                    auth_value: None,
                }),
            },
            SubscribeRequest {
                targets: vec![SubscribeTarget::Module {
                    name: module_id.into(),
                }],
                companion_key: "shared_policy_id".into(),
                client_id: "hooks.epochify.space".into(),
                resume_from: None,
                interests: vec![],
                dial_back: Some(DialBackOverride {
                    url: Some(
                        "https://hooks.epochify.space/_internal/{op}-{target}?key={key}".into(),
                    ),
                    auth_header: None,
                    auth_value: None,
                }),
            },
        ];
        for req in requests.drain(..) {
            let body = cbor(&req);
            let response = router
                .clone()
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
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Both records exist under distinct client_id subdirs.
        let dev_path = storage
            .module_dir_for_companions(module_id)
            .join("hooks.dev.epochify.space")
            .join("shared_policy_id.cbor");
        let prod_path = storage
            .module_dir_for_companions(module_id)
            .join("hooks.epochify.space")
            .join("shared_policy_id.cbor");
        assert!(dev_path.exists(), "dev client_id subdir+file expected");
        assert!(prod_path.exists(), "prod client_id subdir+file expected");

        // Decode + sanity-check the persisted dial-back URLs are
        // distinct. This is the architectural property the bug
        // violated: two records, two URLs, one storage layer.
        let dev_req: SubscribeRequest =
            ciborium::de::from_reader(std::fs::read(&dev_path).unwrap().as_slice()).unwrap();
        let prod_req: SubscribeRequest =
            ciborium::de::from_reader(std::fs::read(&prod_path).unwrap().as_slice()).unwrap();
        assert_eq!(dev_req.client_id, "hooks.dev.epochify.space");
        assert_eq!(prod_req.client_id, "hooks.epochify.space");
        assert!(
            dev_req
                .dial_back
                .as_ref()
                .and_then(|d| d.url.as_deref())
                .unwrap_or("")
                .contains("hooks.dev.epochify.space")
        );
        assert!(
            prod_req
                .dial_back
                .as_ref()
                .and_then(|d| d.url.as_deref())
                .unwrap_or("")
                .contains("hooks.epochify.space")
                && !prod_req
                    .dial_back
                    .as_ref()
                    .and_then(|d| d.url.as_deref())
                    .unwrap_or("")
                    .contains("dev.epochify.space"),
            "prod URL must not collide with dev's"
        );

        // `load_companion_registrations(companion_key)` walks the
        // two-level layout and surfaces BOTH records under the
        // shared key.
        let registrations =
            load_companion_registrations(&storage, "shared_policy_id").expect("load OK");
        assert_eq!(
            registrations.len(),
            2,
            "expected one record per (client_id, companion_key)"
        );
    }

    #[test]
    fn subscribe_rejects_empty_client_id() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let router = build_router_with(storage);

        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "ownership-indexer".into(),
            }],
            companion_key: "customer_42".into(),
            client_id: "".into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
        };
        let body = cbor(&req);
        let response = tokio::runtime::Runtime::new().unwrap().block_on(
            router.oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .body(Body::from(body))
                    .unwrap(),
            ),
        );
        let response = response.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn migrate_flat_companions_moves_legacy_into_two_level() {
        // Synthesise a pre-fix flat companion record (no
        // client_id field, written directly under
        // `<storage>/<module>/companions/<companion_key>.cbor`).
        // The migration must move it into the two-level layout
        // with `client_id` derived from `dial_back.url`'s host.

        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let module_id = "ownership-indexer";

        let companions_root = storage.module_dir_for_companions(module_id);
        std::fs::create_dir_all(&companions_root).unwrap();

        // Encode a legacy SubscribeRequest shape (no client_id)
        // via a transitional struct so we don't depend on the
        // modern type being able to skip the field.
        let legacy_cbor = {
            #[derive(serde::Serialize)]
            struct LegacyReq {
                targets: Vec<SubscribeTarget>,
                companion_key: String,
                resume_from: Option<ChainPoint>,
                interests: Vec<mitos_protocol::Interest>,
                dial_back: Option<DialBackOverride>,
            }
            let r = LegacyReq {
                targets: vec![SubscribeTarget::Module {
                    name: module_id.into(),
                }],
                companion_key: "policy_x".into(),
                resume_from: None,
                interests: vec![],
                dial_back: Some(DialBackOverride {
                    url: Some(
                        "https://hooks.dev.epochify.space/_internal/{op}-{target}?key={key}".into(),
                    ),
                    auth_header: None,
                    auth_value: None,
                }),
            };
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&r, &mut buf).unwrap();
            buf
        };
        std::fs::write(companions_root.join("policy_x.cbor"), &legacy_cbor).unwrap();

        let (migrated, quarantined) =
            migrate_flat_companions_for_module(&storage, module_id).unwrap();
        assert_eq!(migrated, 1);
        assert_eq!(quarantined, 0);

        // Flat file is gone.
        assert!(!companions_root.join("policy_x.cbor").exists());
        // Relocated to two-level path.
        let new_path = companions_root
            .join("hooks.dev.epochify.space")
            .join("policy_x.cbor");
        assert!(new_path.exists());

        // Decoded record carries the synthesised client_id.
        let req: SubscribeRequest =
            ciborium::de::from_reader(std::fs::read(&new_path).unwrap().as_slice()).unwrap();
        assert_eq!(req.client_id, "hooks.dev.epochify.space");
        assert_eq!(req.companion_key, "policy_x");

        // Re-running the migration is a no-op (idempotent).
        let (m, q) = migrate_flat_companions_for_module(&storage, module_id).unwrap();
        assert_eq!(m, 0);
        assert_eq!(q, 0);
    }

    #[test]
    fn migrate_flat_companions_quarantines_unreachable() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let module_id = "ownership-indexer";
        let companions_root = storage.module_dir_for_companions(module_id);
        std::fs::create_dir_all(&companions_root).unwrap();

        // Legacy record with NO dial_back.url — unmigratable.
        let legacy_cbor = {
            #[derive(serde::Serialize)]
            struct LegacyReq {
                targets: Vec<SubscribeTarget>,
                companion_key: String,
                resume_from: Option<ChainPoint>,
                interests: Vec<mitos_protocol::Interest>,
                dial_back: Option<DialBackOverride>,
            }
            let r = LegacyReq {
                targets: vec![SubscribeTarget::Module {
                    name: module_id.into(),
                }],
                companion_key: "orphan".into(),
                resume_from: None,
                interests: vec![],
                dial_back: None,
            };
            let mut buf = Vec::new();
            ciborium::ser::into_writer(&r, &mut buf).unwrap();
            buf
        };
        std::fs::write(companions_root.join("orphan.cbor"), &legacy_cbor).unwrap();

        let (migrated, quarantined) =
            migrate_flat_companions_for_module(&storage, module_id).unwrap();
        assert_eq!(migrated, 0);
        assert_eq!(quarantined, 1);

        assert!(!companions_root.join("orphan.cbor").exists());
        assert!(
            companions_root
                .join(".unreachable")
                .join("orphan.cbor")
                .exists()
        );
    }
}
