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
use axum::extract::{Extension, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;

use mitos_lifecycle::access::{Attempt, Denied, Granted, Principal};
use mitos_lifecycle::ids::ClientId;
use mitos_protocol::{
    AnnounceOutcome, AnnounceResult, HTTP_DELIVERY_MIME, InterestOp, SUBSCRIBE_MIME,
    SubscribeRequest, SubscribeResponse, SubscribeTarget, decode_interest_mutation,
};

use crate::indexer_bridge::IndexerBridgeHandle;

use crate::auth::Authenticator;
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
    auth: Arc<Authenticator>,
    dialer: Option<CompanionDialer>,
    indexer_bridge: Option<IndexerBridgeHandle>,
    events: crate::events::EventRing,
    lifecycle: Option<Arc<crate::lifecycle::LifecycleManager>>,
    moves: Option<Arc<crate::moves::MoveManager>>,
) -> axum::Router {
    let state = CompanionState {
        storage: Arc::new(storage),
        dialer,
        indexer_bridge,
        events,
        resync_recently: ResyncDebounce::default(),
        auth: auth.clone(),
        operator_as_client_recently: OperatorAsClientDebounce::default(),
        lifecycle,
        moves,
    };
    axum::Router::new()
        .route("/api/companions/subscribe", post(subscribe_handler))
        .route(
            "/api/companions/{companion_key}/interest",
            post(interest_mutation_handler),
        )
        .with_state(state)
        // Authentication only. Neither of these routes has a single
        // `Attempt` a layer could build: both name a CLIENT, and for the
        // interest route the clients are not known until the registrations
        // are read off disk. Each handler asks `authorize` itself.
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::authenticate,
        ))
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
    /// Per-(module, companion_key) debounce for subscribe-time
    /// auto-resync — see `subscribe_handler`.
    resync_recently: ResyncDebounce,
    /// The host's authenticator. The router's layer proved WHO is calling;
    /// these handlers are where it is decided whether that principal may act
    /// as the client the request names.
    auth: Arc<Authenticator>,
    /// Per-client debounce for `OperatorTokenUsedAsClient` — see
    /// `record_operator_token_used_as_client`.
    operator_as_client_recently: OperatorAsClientDebounce,
    /// The instance lifecycle. Told to reconcile after a registration is
    /// written or removed, so an attach or detach lands on the record
    /// immediately rather than waiting for the 60s tick. `None` in tests that
    /// exercise persistence alone, and in artifact-only routers.
    lifecycle: Option<Arc<crate::lifecycle::LifecycleManager>>,
    /// Client rows and artifact pulls. `None` means the host does not accept
    /// announcements at all, and every one is refused saying so.
    moves: Option<Arc<crate::moves::MoveManager>>,
}

/// Tell the lifecycle the registrations changed.
///
/// Best-effort on purpose: a failure here is logged and the request still
/// succeeds, because the 60s tick reconciles again. This is a hint that saves
/// a minute, not the mechanism — `Instance::reconcile` diffs the disk, so a
/// call that never happens is corrected rather than lost.
async fn nudge_lifecycle(state: &CompanionState, module: &str) {
    let Some(lifecycle) = state.lifecycle.as_ref() else {
        return;
    };
    if let Err(e) = lifecycle.reconcile(module).await {
        tracing::warn!(
            module = %module,
            error = %e,
            "lifecycle reconcile after a registration change failed; the next tick retries",
        );
    }
}

/// Debounce for the `operator_token_used_as_client` event.
///
/// ~91 companion DOs re-subscribe at once after a host restart, and while the
/// rollout is on `allow-with-warning` every one of them is still carrying the
/// operator token. Without this the event ring — 512 entries — would be
/// flushed of everything else by a single restart, which is precisely when an
/// operator most wants to read it. One event per client per window is all the
/// signal there is: the question the event answers is "which workers are still
/// on the old token", not "how many times did they ask".
#[derive(Clone, Default)]
struct OperatorAsClientDebounce(
    Arc<std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>>,
);

impl OperatorAsClientDebounce {
    const WINDOW: std::time::Duration = std::time::Duration::from_secs(600);

    /// True when an event may be recorded for this client. Records the grant,
    /// so the next call inside the window is refused. Degrades open (allow)
    /// on lock poisoning — a duplicated warning beats a lost one.
    fn allow(&self, client_id: &str) -> bool {
        let Ok(mut map) = self.0.lock() else {
            return true;
        };
        let now = std::time::Instant::now();
        match map.get(client_id) {
            Some(last) if now.duration_since(*last) < Self::WINDOW => false,
            _ => {
                map.insert(client_id.to_owned(), now);
                map.retain(|_, t| now.duration_since(*t) < Self::WINDOW);
                true
            }
        }
    }
}

/// Debounce guard for subscribe-time auto-resync. A companion that
/// wakes repeatedly before consuming its first emission re-sends
/// `resume_from: None` each time; without this, every wake would
/// route another `Resync` and re-walk the scope. One resync per
/// (module, companion_key) per window is plenty — the baseline only
/// needs to stream once, and an operator who genuinely needs an
/// immediate second pass has the per-companion admin recapture.
#[derive(Clone, Default)]
struct ResyncDebounce(
    Arc<std::sync::Mutex<std::collections::HashMap<(String, String), std::time::Instant>>>,
);

impl ResyncDebounce {
    const WINDOW: std::time::Duration = std::time::Duration::from_secs(120);

    /// True when a resync may fire for this (module, companion_key).
    /// Records the grant, so the next call inside the window is
    /// refused. Degrades open (allow) on lock poisoning.
    fn allow(&self, module: &str, companion_key: &str) -> bool {
        let Ok(mut map) = self.0.lock() else {
            return true;
        };
        let key = (module.to_owned(), companion_key.to_owned());
        let now = std::time::Instant::now();
        match map.get(&key) {
            Some(last) if now.duration_since(*last) < Self::WINDOW => false,
            _ => {
                map.insert(key, now);
                // Opportunistic sweep so the map can't grow unbounded
                // across a long host lifetime (91 policies × restarts).
                map.retain(|_, t| now.duration_since(*t) < Self::WINDOW);
                true
            }
        }
    }
}

/// Authorize `principal` to act as `client_id`, and record the rollout
/// warning when the operator token is what is being used.
///
/// Both companion routes funnel through this so the `Granted` cases are
/// handled in exactly one place — `OperatorTokenUsedAsClient` is easy to
/// pattern-match past, and the whole rollout depends on it being seen.
fn authorize_as_client(
    state: &CompanionState,
    principal: &Principal,
    client_id: &ClientId,
    module_for_event: &str,
) -> std::result::Result<(), Denied> {
    match state.auth.authorize(principal, Attempt::ActAs(client_id))? {
        Granted::Plain => Ok(()),
        Granted::OperatorTokenUsedAsClient => {
            if state.operator_as_client_recently.allow(client_id.as_str()) {
                tracing::warn!(
                    client_id = %client_id,
                    "client action authorised with the OPERATOR token; rotate this worker's \
                     MITOS_AUTH_TOKEN secret to its namespace token before switching \
                     operator_token_as_client to `refuse`",
                );
                state.events.record(
                    module_for_event,
                    crate::events::EventKind::OperatorTokenUsedAsClient {
                        client_id: client_id.to_string(),
                    },
                );
            }
            Ok(())
        }
    }
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
    /// The caller is authenticated but may not act as the client it named.
    /// 403, not 401: the token IS somebody, just not this client's namespace.
    #[error("{0}")]
    Denied(Denied),
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
            Self::Denied(_) => StatusCode::FORBIDDEN,
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
            Self::Denied(_) => "denied",
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
    /// Registrations exist under this companion key, but the caller may act
    /// as NONE of their clients. 403 rather than the 404 an unknown key
    /// gets: the difference between "there is nothing here" and "there is
    /// something here that is not yours" is the whole point of the route's
    /// per-record check.
    #[error("{0}")]
    Denied(Denied),
    /// Same 403, for the case where every registration was skipped without
    /// producing a `Denied` — a persisted client id that no longer parses
    /// belongs to no namespace, so there is nobody it could have been
    /// refused AS.
    #[error("no registration under `{0}` names a client you may act as")]
    NoneGranted(String),
}

impl IntoResponse for InterestMutationError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Decode(_) | Self::InvalidCompanionKey(_) | Self::EmptyItems => {
                StatusCode::BAD_REQUEST
            }
            Self::ReplaceNotSupported => StatusCode::BAD_REQUEST,
            Self::UnknownCompanion(_) => StatusCode::NOT_FOUND,
            Self::Denied(_) | Self::NoneGranted(_) => StatusCode::FORBIDDEN,
            Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match &self {
            Self::Decode(_) => "cbor_decode",
            Self::InvalidCompanionKey(_) => "invalid_companion_key",
            Self::EmptyItems => "empty_items",
            Self::ReplaceNotSupported => "replace_not_supported",
            Self::UnknownCompanion(_) => "unknown_companion",
            Self::Denied(_) | Self::NoneGranted(_) => "denied",
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
    Extension(principal): Extension<Principal>,
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

    // The route names a companion_key and NO client — and `dev` and `prod`
    // workers share companion keys, because the key is the policy-id DO name.
    // So a mutation is authorized per REGISTRATION, and only the granted ones
    // are touched: the rest must be byte-identical afterwards. That is the
    // whole point of this order.
    let mut denial: Option<Denied> = None;
    let mut granted = Vec::with_capacity(registrations.len());
    for (module, path, req) in registrations {
        // A persisted client id that no longer parses belongs to no
        // namespace, so nobody may act as it. Skip it rather than fail the
        // request — it is somebody else's record either way.
        let Ok(client) = ClientId::parse(&req.client_id) else {
            tracing::warn!(
                companion_key = %companion_key,
                module = %module,
                client_id = %req.client_id,
                "interest mutation: registration has an unparseable client id; leaving it alone",
            );
            continue;
        };
        match authorize_as_client(&state, &principal, &client, &module) {
            Ok(()) => granted.push((module, path, req)),
            Err(e) => {
                tracing::warn!(
                    companion_key = %companion_key,
                    module = %module,
                    client_id = %client,
                    denied = %e,
                    "interest mutation: leaving this registration untouched",
                );
                denial.get_or_insert(e);
            }
        }
    }
    if granted.is_empty() {
        // Registrations exist, just none this caller may touch. 403, not the
        // 404 an unknown key gets.
        return Err(match denial {
            Some(denied) => InterestMutationError::Denied(denied),
            None => InterestMutationError::NoneGranted(companion_key),
        });
    }
    let registrations = granted;

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

    // A mutation that emptied a record DELETED it above, which is a detach.
    // Reconcile the affected modules once each rather than per registration:
    // it is a diff, so one call covers every change to that module.
    let touched: std::collections::BTreeSet<&str> =
        registrations.iter().map(|(m, _, _)| m.as_str()).collect();
    for module in &touched {
        nudge_lifecycle(&state, module).await;
    }

    // Propagate to each running follower's in-memory filter so
    // the change is live without waiting for the next host
    // restart. Best-effort: a follower that's not currently
    // running surfaces `InterestRouteError::NotRunning` which we
    // log + swallow (persisted CBOR will be picked up on restart).
    //
    // Module names dedupe first: `registrations` has one entry per
    // (module, client) pair sharing the key, and routing the same
    // mutation twice into one follower is wasted work.
    if let Some(dialer) = &state.dialer {
        let modules: std::collections::BTreeSet<&str> =
            registrations.iter().map(|(m, _, _)| m.as_str()).collect();
        for module in modules {
            // Cross-companion underflow guard: an item is only
            // removed from the module's scan-set when NO persisted
            // registration still holds it. Without this, one
            // companion unsubscribing a policy silently stopped the
            // module watching it for every OTHER companion (e.g. a
            // prod worker dropping a policy the dev worker still
            // tails). The union is read back from disk after the
            // mutation writes above, so it reflects the
            // post-mutation world — including this companion's own
            // surviving interests.
            let items_to_route: Vec<mitos_protocol::Interest> =
                if matches!(mutation.op, InterestOp::Remove) {
                    let still_held = load_module_interest_union(&state.storage, module);
                    let (route, kept): (Vec<_>, Vec<_>) = mutation
                        .items
                        .iter()
                        .cloned()
                        .partition(|item| !still_held.contains(item));
                    if !kept.is_empty() {
                        tracing::info!(
                            module = %module,
                            companion_key = %companion_key,
                            kept = kept.len(),
                            "interest remove: item(s) still held by another companion — \
                             kept in module scan-set",
                        );
                    }
                    route
                } else {
                    mutation.items.clone()
                };
            if items_to_route.is_empty() {
                continue;
            }
            match dialer
                .route_interest_mutation(module, mutation.op, items_to_route)
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
        // Resync persists like Add — the "treat as new" semantics
        // apply to the module's scan-set (follower-side), not to the
        // registration record.
        InterestOp::Add | InterestOp::Resync => {
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

/// Union of every persisted registration's interests for one module —
/// all client_ids, all companion_keys. Callers read this AFTER
/// writing the current mutation's registrations so it reflects the
/// post-mutation world. Un-decodable records are skipped (same
/// tolerance as `load_companion_registrations`).
fn load_module_interest_union(
    storage: &ModuleStorage,
    module_id: &str,
) -> Vec<mitos_protocol::Interest> {
    let mut union: Vec<mitos_protocol::Interest> = Vec::new();
    let companions_root = storage.module_dir_for_companions(module_id);
    let clients = match std::fs::read_dir(&companions_root) {
        Ok(e) => e,
        Err(_) => return union,
    };
    for client in clients.flatten() {
        if !client.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        if client.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let records = match std::fs::read_dir(client.path()) {
            Ok(e) => e,
            Err(_) => continue,
        };
        for record in records.flatten() {
            if record.path().extension().and_then(|e| e.to_str()) != Some("cbor") {
                continue;
            }
            let Ok(bytes) = std::fs::read(record.path()) else {
                continue;
            };
            let Ok(req) = ciborium::de::from_reader::<SubscribeRequest, _>(bytes.as_slice()) else {
                continue;
            };
            for interest in req.interests {
                if !union.contains(&interest) {
                    union.push(interest);
                }
            }
        }
    }
    union
}

/// Scan `<storage>/*/companions/<key>.cbor` and decode each
/// matching registration. Returns one entry per module the
/// companion is subscribed to. `pub(crate)` for the admin
/// router's per-companion recapture (scoped resync).
pub(crate) fn load_companion_registrations(
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
    Extension(principal): Extension<Principal>,
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

    // `client_id` arrives in the request BODY, so until this check it was
    // self-asserted: any token holder could subscribe as
    // `ownership.cnft.dev`. Authorize before anything is persisted or handed
    // to the dialer — a refusal must leave no registration on disk and no
    // dial loop running.
    let client_id = ClientId::parse(&request.client_id)
        .map_err(|e| SubscribeError::InvalidClientId(e.to_string()))?;
    let module_for_event = request
        .targets
        .iter()
        .find_map(|t| match t {
            SubscribeTarget::Module { name } => Some(name.as_str()),
            SubscribeTarget::Indexer { .. } => None,
        })
        .unwrap_or("-");
    authorize_as_client(&state, &principal, &client_id, module_for_event)
        .map_err(SubscribeError::Denied)?;

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
                // The registration is now on disk, so the client can attach.
                // On an instance an operator STOPPED this records the client
                // and does NOT start it — the attach is remembered and served
                // the moment the operator starts it.
                nudge_lifecycle(&state, name).await;
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
            // `resume_from: None` is the companion declaring it holds
            // no state (fresh DO, or reset by a dApp-side recapture).
            // For a scope the module ALREADY tracks, a plain `Add` is
            // deduped module-side and no snapshot ever streams — the
            // companion would tail live deltas over an empty ledger
            // forever (the CO1 "added collection never captures"
            // gap). Route `Resync` instead: the follower re-seeds the
            // scope as genuinely new and the Onboard pump re-emits
            // its snapshot. Idempotent for every other subscriber
            // (snapshot apply is a full-replace), debounced per
            // (module, companion_key) against wake storms.
            let declares_no_state = request.resume_from.is_none();
            for target in &request.targets {
                let SubscribeTarget::Module { name } = target else {
                    continue;
                };
                let op = if declares_no_state
                    && state.resync_recently.allow(name, &request.companion_key)
                {
                    state.events.record(
                        name.as_str(),
                        crate::events::EventKind::ResyncRouted {
                            client_id: request.client_id.clone(),
                            companion_key: request.companion_key.clone(),
                        },
                    );
                    InterestOp::Resync
                } else {
                    InterestOp::Add
                };
                if let Err(e) = dialer
                    .route_interest_mutation(name, op, request.interests.clone())
                    .await
                {
                    tracing::warn!(
                        module = %name,
                        op = ?op,
                        error = %e,
                        "subscribe: routing interest into module scan-set failed; \
                         companion fanout-interest still registered",
                    );
                }
            }
        }
    }
    // Announcements are handled LAST, after the registration is on disk and
    // `reconcile` has run — so a client that is about to be adopted onto its
    // instance already is one. An announcement NEVER fails the subscribe: a
    // refusal is reported here and the companion stays registered and served
    // by the instance it is on.
    let announced = process_announcements(&state, &principal, &client_id, &request).await;

    let response = SubscribeResponse {
        status: "subscribed".to_string(),
        next_emission_id,
        announced,
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

/// One outcome per announcement on the request.
///
/// An entry for a module this request does not TARGET is refused: a client
/// announcing a build of something it is not subscribing to has nothing the
/// host can act on, and accepting it would let one subscribe drive a move for
/// any module on the box.
async fn process_announcements(
    state: &CompanionState,
    principal: &Principal,
    client_id: &ClientId,
    request: &SubscribeRequest,
) -> Vec<AnnounceOutcome> {
    if request.announced.is_empty() {
        return Vec::new();
    }
    let Some(moves) = state.moves.as_ref() else {
        return request
            .announced
            .iter()
            .map(|a| AnnounceOutcome {
                module: a.module.clone(),
                result: AnnounceResult::Refused {
                    reason: "this host does not accept announcements".to_owned(),
                },
            })
            .collect();
    };

    let targeted: std::collections::BTreeSet<&str> = request
        .targets
        .iter()
        .filter_map(|t| match t {
            SubscribeTarget::Module { name } => Some(name.as_str()),
            SubscribeTarget::Indexer { .. } => None,
        })
        .collect();

    let mut outcomes = Vec::with_capacity(request.announced.len());
    for announced in &request.announced {
        let result = if !targeted.contains(announced.module.as_str()) {
            AnnounceResult::Refused {
                reason: format!(
                    "this subscribe does not target module `{}`",
                    announced.module
                ),
            }
        } else {
            moves
                .announce(client_id, &announced.module, announced)
                .await
        };
        if let AnnounceResult::Refused { reason } = &result {
            tracing::warn!(
                client_id = %client_id,
                module = %announced.module,
                ?principal,
                %reason,
                "announcement refused; the companion is still registered and served",
            );
        }
        outcomes.push(AnnounceOutcome {
            module: announced.module.clone(),
            result,
        });
    }
    outcomes
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

/// Whether a module still has legacy FLAT registrations — `<key>.cbor` files
/// directly under `companions/` rather than under a `<client_id>/` directory.
///
/// Lifecycle adoption counts client DIRECTORIES, and what it concludes is
/// written down and sticky. So it asks this first: adopting while flat files
/// remain would record a subscribed module as client-less ⇒ `Pinned` ⇒ never
/// retiring when its clients later move. The migration below normally clears
/// them, but it can FAIL, and "it was called" is not "it worked". Uses exactly
/// the migration's own test for what a flat record is.
pub(crate) fn has_flat_companions(storage: &ModuleStorage, module_id: &str) -> bool {
    let Ok(entries) = std::fs::read_dir(storage.module_dir_for_companions(module_id)) else {
        return false;
    };
    entries.flatten().any(|entry| {
        entry.file_type().map(|t| t.is_file()).unwrap_or(false)
            && entry.path().extension().and_then(|s| s.to_str()) == Some("cbor")
    })
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
        // A legacy flat record predates announcements entirely.
        announced: Vec::new(),
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
///
/// The rules are [`ClientId::parse`]'s, and this DELEGATES to it rather than
/// restating them. It used to validate the TRIMMED id while
/// `client_companions_dir` built a path from the RAW one, so a padded id
/// passed validation and produced a directory that never round-tripped. Two
/// validators that agree today are two that can disagree tomorrow; the id
/// that becomes a path and the id that is authorized must be one check.
fn validate_client_id(client_id: &str) -> std::result::Result<(), SubscribeError> {
    ClientId::parse(client_id)
        .map(|_| ())
        .map_err(|e| SubscribeError::InvalidClientId(e.to_string()))
}

/// On-disk directory holding all companion registrations for a single
/// `(module_id, client_id)` pair. Layout:
/// `<storage>/<module>/companions/<client_id>/`. `client_id` is the
/// raw value — validation guarantees it's filesystem-safe.
fn client_companions_dir(storage: &ModuleStorage, module_id: &str, client_id: &str) -> PathBuf {
    storage.module_dir_for_companions(module_id).join(client_id)
}

/// Fixtures shared with `admin`'s router tests.
#[cfg(test)]
pub(crate) mod testing {
    use crate::manifest::{
        AbiSection, BuildSection, InterestSection, Manifest, ModuleSection, TrapPolicySection,
        host_wit_sha, sha256_hex,
    };

    /// A manifest that `ModuleStorage::activate` will accept for `wasm`.
    /// Registration is what `current.toml` means now, and only `activate`
    /// writes one — so a test that needs a registered module goes through it
    /// rather than hand-writing files.
    pub(crate) fn manifest_for(module_id: &str, wasm: &[u8]) -> Manifest {
        Manifest {
            module: ModuleSection {
                id: module_id.to_owned(),
                sha256: sha256_hex(wasm),
                size_bytes: wasm.len() as u64,
            },
            abi: AbiSection {
                version_major: 2,
                version_minor: 0,
                wit_package: "mitos:platform-v2".to_owned(),
                wit_world: "mitos-module-v2".to_owned(),
                wit_sha: Some(host_wit_sha().to_owned()),
            },
            trap_policy: TrapPolicySection {
                strategy: "replay".to_owned(),
                max_retries: 3,
                backoff_cap_ms: 1000,
            },
            build: BuildSection {
                rust_version: "0.0".to_owned(),
                target: "wasm32-wasip2".to_owned(),
                profile: "release".to_owned(),
                build_id: "1970-01-01T00:00:00Z".to_owned(),
                git_sha: None,
                crate_version: "0.0.0".to_owned(),
            },
            interest: InterestSection::default(),
            lifecycle: Default::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::manifest_for;
    use super::*;
    use crate::auth::testing::{
        DEV, DEV_CLIENT, FULL, PROD, PROD_CLIENT, READONLY, configured_authenticator,
    };
    use axum::body::{Body, to_bytes};
    use axum::http::Request as HttpRequest;
    use mitos_lifecycle::access::OperatorTokenAsClient;
    use mitos_protocol::{ChainPoint, DialBackOverride, SubscribeTarget};
    use tower::ServiceExt;

    /// The legacy (v2) posture: one implicit namespace, open mode. Every
    /// pre-existing test in this module runs against it unchanged, which is
    /// the proof that legacy mode is byte-for-byte v2.
    fn build_router_with(storage: ModuleStorage) -> axum::Router {
        companion_router(
            storage,
            Arc::new(crate::auth::Authenticator::legacy(None, None)),
            None,
            None,
            crate::events::EventRing::new(),
            None,
            None,
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
            announced: Vec::new(),
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
            announced: Vec::new(),
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
            announced: Vec::new(),
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
        // "module must be registered" check passes. Goes through
        // `activate` rather than hand-writing files: registration is what
        // `current.toml` means now, and only `activate` writes one.
        let module_id = "ownership-indexer";
        let wasm = b"fake wasm for the registration check";
        storage
            .activate(&manifest_for(module_id, wasm), wasm)
            .unwrap();

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
                announced: Vec::new(),
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
                announced: Vec::new(),
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
            announced: Vec::new(),
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

    #[test]
    fn resync_debounce_one_grant_per_window() {
        let debounce = ResyncDebounce::default();
        assert!(debounce.allow("collection-holders", "policy_a"));
        // Same (module, key) inside the window: refused.
        assert!(!debounce.allow("collection-holders", "policy_a"));
        // Different key / different module: independent grants.
        assert!(debounce.allow("collection-holders", "policy_b"));
        assert!(debounce.allow("collection-metadata", "policy_a"));
    }

    #[test]
    fn apply_mutation_resync_persists_like_add() {
        let p1 = mitos_protocol::Interest::any();
        let mut current = vec![];
        apply_mutation_to_set(&mut current, InterestOp::Resync, std::slice::from_ref(&p1));
        assert_eq!(current, vec![p1.clone()]);
        // Re-asserting the same interest stays deduped.
        apply_mutation_to_set(&mut current, InterestOp::Resync, std::slice::from_ref(&p1));
        assert_eq!(current, vec![p1]);
    }

    /// The union guard's input: `load_module_interest_union` must see
    /// every persisted registration's interests — across client_ids
    /// AND companion_keys — so a Remove only reaches the module for
    /// items nobody else still holds.
    #[tokio::test]
    async fn module_interest_union_spans_clients_and_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let module_id = "ownership-indexer";

        let shared = mitos_protocol::Interest::any();
        let write_reg = |client: &str, key: &str, interests: Vec<mitos_protocol::Interest>| {
            let dir = client_companions_dir(&storage, module_id, client);
            std::fs::create_dir_all(&dir).unwrap();
            let req = SubscribeRequest {
                targets: vec![SubscribeTarget::Module {
                    name: module_id.into(),
                }],
                companion_key: key.into(),
                client_id: client.into(),
                resume_from: None,
                interests,
                dial_back: None,
                announced: Vec::new(),
            };
            std::fs::write(dir.join(format!("{key}.cbor")), req.encode().unwrap()).unwrap();
        };

        // prod + dev clients sharing one key, plus a second key on a
        // third record — all holding the same interest.
        write_reg("ownership.cnft.dev", "policy_a", vec![shared.clone()]);
        write_reg("ownership.dev.cnft.dev", "policy_a", vec![shared.clone()]);
        write_reg("ownership.cnft.dev", "policy_b", vec![shared.clone()]);

        let union = load_module_interest_union(&storage, module_id);
        // Deduped: one interest held by three records.
        assert_eq!(union, vec![shared.clone()]);

        // Drop two of the three records; the union must still hold
        // the interest via the survivor — this is exactly the state
        // the Remove guard consults after the mutation writes.
        std::fs::remove_file(
            client_companions_dir(&storage, module_id, "ownership.cnft.dev").join("policy_a.cbor"),
        )
        .unwrap();
        std::fs::remove_file(
            client_companions_dir(&storage, module_id, "ownership.dev.cnft.dev")
                .join("policy_a.cbor"),
        )
        .unwrap();
        assert_eq!(
            load_module_interest_union(&storage, module_id),
            vec![shared]
        );

        // Last record gone → empty union → Remove may reach the module.
        std::fs::remove_file(
            client_companions_dir(&storage, module_id, "ownership.cnft.dev").join("policy_b.cbor"),
        )
        .unwrap();
        assert!(load_module_interest_union(&storage, module_id).is_empty());
    }

    // -------------------------------------------------------------------
    // Namespaces, through the REAL router. The pure crate already tests
    // `authorize`; what these prove is that these two routes actually ASK —
    // and, for the interest route, that a refusal leaves the other client's
    // bytes alone.
    // -------------------------------------------------------------------

    const MODULE: &str = "ownership-indexer";

    /// A configured two-namespace host with `MODULE` registered.
    fn namespaced_router(
        storage: &ModuleStorage,
        mode: OperatorTokenAsClient,
    ) -> (axum::Router, crate::events::EventRing) {
        let wasm = b"namespaced test wasm";
        storage.activate(&manifest_for(MODULE, wasm), wasm).unwrap();
        let events = crate::events::EventRing::new();
        let router = companion_router(
            storage.clone(),
            Arc::new(configured_authenticator(mode)),
            None,
            None,
            events.clone(),
            None,
            None,
        );
        (router, events)
    }

    /// Two registrations must differ in CONTENT for "the prod bytes did not
    /// move" to mean anything — `Interest::any()` twice would dedupe and the
    /// mutation would be a no-op for both.
    fn policy_interest(policy: &str) -> mitos_protocol::Interest {
        mitos_protocol::Interest {
            asset: mitos_protocol::AssetSelector::Policy(
                cardano_assets::PolicyId::new(policy).unwrap(),
            ),
            ..mitos_protocol::Interest::any()
        }
    }

    fn add_mutation(policy: &str) -> Vec<u8> {
        mitos_protocol::encode_interest_mutation(&mitos_protocol::InterestMutationBody {
            op: InterestOp::Add,
            items: vec![policy_interest(policy)],
        })
        .unwrap()
    }

    fn subscribe_request(client_id: &str) -> SubscribeRequest {
        SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: MODULE.into(),
            }],
            companion_key: "shared_policy_id".into(),
            client_id: client_id.into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
            announced: Vec::new(),
        }
    }

    async fn post_subscribe(
        router: &axum::Router,
        token: Option<&str>,
        request: &SubscribeRequest,
    ) -> StatusCode {
        let mut req = HttpRequest::builder()
            .method("POST")
            .uri("/api/companions/subscribe")
            .header("content-type", "application/cbor");
        if let Some(token) = token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        router
            .clone()
            .oneshot(req.body(Body::from(cbor(request))).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn a_dev_token_cannot_subscribe_as_a_prod_client() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        let status = post_subscribe(&router, Some(DEV), &subscribe_request(PROD_CLIENT)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        // A refusal must leave NOTHING behind: no registration on disk, and
        // (the router was built with `dialer: None`) no dial loop to start.
        // A directory for the client would be the visible half of a dial the
        // host would resume on the next boot.
        assert!(
            !client_companions_dir(&storage, MODULE, PROD_CLIENT).exists(),
            "a refused subscribe wrote a registration",
        );
        assert_eq!(
            std::fs::read_dir(storage.module_dir_for_companions(MODULE))
                .map(|d| d.count())
                .unwrap_or(0),
            0,
        );
    }

    #[tokio::test]
    async fn a_dev_token_subscribes_as_its_own_client() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        let status = post_subscribe(&router, Some(DEV), &subscribe_request(DEV_CLIENT)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            client_companions_dir(&storage, MODULE, DEV_CLIENT)
                .join("shared_policy_id.cbor")
                .is_file(),
        );
    }

    #[tokio::test]
    async fn an_unknown_token_is_401_and_a_wrong_namespace_is_403() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        assert_eq!(
            post_subscribe(&router, None, &subscribe_request(DEV_CLIENT)).await,
            StatusCode::UNAUTHORIZED,
        );
        assert_eq!(
            post_subscribe(&router, Some("who-even"), &subscribe_request(DEV_CLIENT)).await,
            StatusCode::UNAUTHORIZED,
        );
        assert_eq!(
            post_subscribe(&router, Some(PROD), &subscribe_request(DEV_CLIENT)).await,
            StatusCode::FORBIDDEN,
        );
        // A client no namespace lists is refused, not adopted — even for the
        // operator.
        assert_eq!(
            post_subscribe(
                &router,
                Some(FULL),
                &subscribe_request("preview-1234.cnft.dev")
            )
            .await,
            StatusCode::FORBIDDEN,
        );
    }

    #[tokio::test]
    async fn the_operator_token_as_a_client_is_granted_and_recorded() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, events) = namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        // ~91 companion DOs re-subscribe at once after a host restart. The
        // event must survive that as ONE entry, or a single restart flushes
        // the 512-entry ring of everything else.
        for _ in 0..91 {
            assert_eq!(
                post_subscribe(&router, Some(FULL), &subscribe_request(PROD_CLIENT)).await,
                StatusCode::OK,
            );
        }

        let recorded = events.snapshot(0, None, Some("operator_token_used_as_client"), 512);
        assert_eq!(
            recorded.len(),
            1,
            "91 subscribes from one client must produce ONE event, not 91",
        );
        match &recorded[0].kind {
            crate::events::EventKind::OperatorTokenUsedAsClient { client_id } => {
                assert_eq!(client_id, PROD_CLIENT);
            }
            other => panic!("wrong event recorded: {other:?}"),
        }
    }

    #[tokio::test]
    async fn refuse_mode_turns_that_into_403() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) = namespaced_router(&storage, OperatorTokenAsClient::Refuse);

        assert_eq!(
            post_subscribe(&router, Some(FULL), &subscribe_request(PROD_CLIENT)).await,
            StatusCode::FORBIDDEN,
        );
        // Refusing it as a CLIENT credential never touches the namespace
        // tokens the rotation moved everyone onto.
        assert_eq!(
            post_subscribe(&router, Some(PROD), &subscribe_request(PROD_CLIENT)).await,
            StatusCode::OK,
        );
    }

    /// Register the SAME companion_key under a prod and a dev client, mutate
    /// with the dev token, and assert the prod record's BYTES did not move.
    /// `dev` and `prod` workers share companion keys — the key is the
    /// policy-id DO name — so this is the concrete exposure the order exists
    /// to close.
    #[tokio::test]
    async fn a_dev_token_cannot_rewrite_a_prod_clients_interest() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        let policy = "a".repeat(56);
        let mut prod_req = subscribe_request(PROD_CLIENT);
        prod_req.interests = vec![policy_interest(&policy)];
        let mut dev_req = subscribe_request(DEV_CLIENT);
        dev_req.interests = vec![policy_interest(&policy)];
        assert_eq!(
            post_subscribe(&router, Some(PROD), &prod_req).await,
            StatusCode::OK
        );
        assert_eq!(
            post_subscribe(&router, Some(DEV), &dev_req).await,
            StatusCode::OK
        );

        let prod_path =
            client_companions_dir(&storage, MODULE, PROD_CLIENT).join("shared_policy_id.cbor");
        let dev_path =
            client_companions_dir(&storage, MODULE, DEV_CLIENT).join("shared_policy_id.cbor");
        let prod_before = std::fs::read(&prod_path).unwrap();
        let dev_before = std::fs::read(&dev_path).unwrap();

        let mutation = add_mutation(&"b".repeat(56));
        let response = router
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/shared_policy_id/interest")
                    .header("content-type", "application/cbor")
                    .header(header::AUTHORIZATION, format!("Bearer {DEV}"))
                    .body(Body::from(mutation))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        assert_eq!(
            std::fs::read(&prod_path).unwrap(),
            prod_before,
            "the prod registration's bytes must be untouched",
        );
        assert_ne!(
            std::fs::read(&dev_path).unwrap(),
            dev_before,
            "the dev registration must have been mutated",
        );
    }

    #[tokio::test]
    async fn interest_mutation_with_nothing_granted_is_403_not_404() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);

        let mut prod_req = subscribe_request(PROD_CLIENT);
        prod_req.interests = vec![policy_interest(&"a".repeat(56))];
        assert_eq!(
            post_subscribe(&router, Some(PROD), &prod_req).await,
            StatusCode::OK
        );

        let mutation = add_mutation(&"b".repeat(56));
        let status = |token: &'static str, uri: &'static str| {
            let router = router.clone();
            let mutation = mutation.clone();
            async move {
                router
                    .oneshot(
                        HttpRequest::builder()
                            .method("POST")
                            .uri(uri)
                            .header("content-type", "application/cbor")
                            .header(header::AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::from(mutation))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
                    .status()
            }
        };

        // Registrations exist, none this caller may touch → 403.
        assert_eq!(
            status(DEV, "/api/companions/shared_policy_id/interest").await,
            StatusCode::FORBIDDEN,
        );
        // No registrations at all → the existing 404, unchanged.
        assert_eq!(
            status(DEV, "/api/companions/nothing_here/interest").await,
            StatusCode::NOT_FOUND,
        );
    }

    #[tokio::test]
    async fn the_readonly_token_may_not_act_as_a_client() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let (router, _events) =
            namespaced_router(&storage, OperatorTokenAsClient::AllowWithWarning);
        assert_eq!(
            post_subscribe(&router, Some(READONLY), &subscribe_request(PROD_CLIENT)).await,
            StatusCode::FORBIDDEN,
        );
    }
}
