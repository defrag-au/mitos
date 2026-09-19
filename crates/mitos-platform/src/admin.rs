//! `/_admin/modules/*` HTTP surface.
//!
//! Mounted by `admin_router_with_host` (production wiring) or
//! `admin_router` (artifact-only / inspector deployments). See
//! `docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md` for the operator
//! workflow.
//!
//! Routes:
//! - `POST /_admin/modules/{id}` — multipart upload + activate
//! - `GET  /_admin/modules` — list registered modules
//! - `GET  /_admin/modules/{id}` — single module status
//! - `DELETE /_admin/modules/{id}` — stop module + drop slot
//!   (artifact stays on disk for rollback)
//! - `POST /_admin/modules/{id}/restart` — re-instantiate
//! - `POST /_admin/modules/{id}/recapture` — coordinated state
//!   rebuild for subscribed companions (see `docs/design/RECAPTURE.md`)
//! - `POST /_admin/modules/{id}/evict` — full retirement:
//!   stop slot, drop dialer state, remove artifact directory.
//!   Refuses if companion records exist for the module unless
//!   `?force=true`. Idempotent across re-invocation.
//! - `GET  /_admin/modules/{id}/last-trap` — trap-context fixture
//! - `GET  /_admin/modules/{id}/emissions` — emissions log
//! - `DELETE /_admin/modules/{id}/emissions` — purge
//! - `POST /_admin/modules/{id}/emissions/{emission_id}/replay` —
//!   re-queue a single row
//! - `GET  /_admin/blocks/{slot}` — raw block CBOR from the
//!   dolos archive; produces `mitos-run --block` fixtures
//!   without the `capture-block` writer-lock dance.
//! - `GET  /_admin/blocks/by-tx/{tx_hash}` — same, resolved by
//!   tx hash via the archive's tx-index. Response carries the
//!   slot in `X-Mitos-Block-Slot` so the operator can label the
//!   fixture without a second round trip.
//!
//! Auth lives in [`crate::auth`] — one authenticator for all three
//! routers, resolving a bearer token to a
//! `mitos_lifecycle::access::Principal` and asking `authorize` the
//! rest. This router asks for `Admin(Read)` on `GET` and
//! `Admin(Mutate)` otherwise, with ONE exception: recapture, which
//! a worker calls (see `Attempt::RecaptureModule`).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::Json;
use axum::extract::{Multipart, Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};

use mitos_lifecycle::access::{ClientSet, OperatorTokenAsClient};
use mitos_lifecycle::ids::Revision;
use mitos_lifecycle::instance::{
    BootAction, DesiredState, Effect, Event, EvictMode, Instance, OperatorCommand, Phase, Rejected,
};
use mitos_lifecycle::policy::Tier;
use mitos_lifecycle::state_prep::StateSchema;

use crate::auth::{AuthMode, Authenticator};
use crate::lifecycle::LifecycleError;
use crate::manifest::{Manifest, ManifestError};
use crate::storage::{ModuleStorage, StorageError};

/// A shared-secret bearer token, as an OUTBOUND credential.
///
/// No longer an inbound gate — that is [`crate::auth::Authenticator`]'s job.
/// What is left is the dial-back side: the companion dialer and the in-tree
/// indexer bridge present this token when they call a worker, so the worker
/// can tell the connection came from mitos rather than from somebody who
/// guessed the URL.
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    /// Load from `MITOS_AUTH_TOKEN`. `None` means the host has no
    /// shared secret, so dial-backs go out unauthenticated — the
    /// same posture v2 had.
    pub fn from_env() -> Self {
        match std::env::var("MITOS_AUTH_TOKEN") {
            Ok(t) if !t.is_empty() => Self(Some(t)),
            _ => Self(None),
        }
    }

    pub fn as_deref(&self) -> Option<&str> {
        self.0.as_deref()
    }
}

/// Shared state passed to every handler.
#[derive(Clone)]
struct AdminState {
    storage: ModuleStorage,
    host: Option<Arc<dyn crate::host_v2::ModuleHostHandle>>,
    /// Names of in-tree indexers registered with the bundle.
    /// `upload_module` rejects (400 reserved_name) any module ID
    /// that collides with one of these — see
    /// `docs/design/UNIFIED_SUBSCRIBE.md` step 4.
    reserved_names: Vec<String>,
    /// Optional chain-data handle for the read-only block-fetch
    /// route (`GET /_admin/blocks/{slot}`). `None` when the admin
    /// router is mounted without a dolos archive in scope
    /// (artifact-only deployments, tests).
    chain_data: Option<Arc<dyn mitos_data_plane::ChainDataPlane>>,
    /// Process start time, threaded from the bundle so `GET
    /// /_admin/status` reports the same `uptime_secs` as the open
    /// `/health` endpoint. Artifact-only routers capture their own
    /// build time, which is process start for those deployments.
    started_at: SystemTime,
    /// Shared operational-events ring. The admin router reads it for
    /// `GET /_admin/events` + records recapture events; the host /
    /// follower record traps into the same instance.
    events: crate::events::EventRing,
    /// The host's authenticator. Handlers do not authorize with it — the
    /// layers already did — but `GET /_admin/status` reports the posture
    /// (mode, namespaces, unowned clients) from it.
    auth: Arc<Authenticator>,
    /// The instance lifecycle. Every handler that stops, starts, uploads or
    /// retires a module goes through this rather than calling the host
    /// directly, because the host has no idea what an operator wants.
    /// `None` in artifact-only routers, where there is no host either.
    lifecycle: Option<Arc<crate::lifecycle::LifecycleManager>>,
}

/// Build the admin router with artifact-only behaviour. Uploads
/// land on disk but no follower is started. Used by:
/// - read-only / inspector deployments
/// - tests that only exercise the artifact pipeline
///
/// Production wires the lifecycle-aware variant
/// `admin_router_with_host` which actually starts running
/// modules after upload.
pub fn admin_router(storage: ModuleStorage, auth: Arc<Authenticator>) -> axum::Router {
    admin_router_inner(
        storage,
        None,
        Vec::new(),
        None,
        SystemTime::now(),
        crate::events::EventRing::new(),
        auth,
        None,
    )
}

/// Build the admin router with the running-instance lifecycle
/// wired in. Uploads trigger `host.replace(id)` so the new sha
/// starts running immediately; DELETE + restart routes are
/// available for admin operators.
///
/// `reserved_names` is the set of in-tree indexer names the host
/// has registered — `upload_module` rejects collisions to prevent
/// a wasm module from shadowing an indexer subscribable via the
/// unified-subscribe path. Pass an empty `Vec` when the bundle
/// has no in-tree indexers (artifact-only deployments).
///
/// `chain_data` enables the `GET /_admin/blocks/{slot}` block-
/// fetch route. Pass `None` to disable that route (the handler
/// returns 503 in that case).
#[allow(clippy::too_many_arguments)]
pub fn admin_router_with_host(
    storage: ModuleStorage,
    host: Arc<dyn crate::host_v2::ModuleHostHandle>,
    reserved_names: Vec<String>,
    chain_data: Option<Arc<dyn mitos_data_plane::ChainDataPlane>>,
    started_at: SystemTime,
    events: crate::events::EventRing,
    auth: Arc<Authenticator>,
    lifecycle: Arc<crate::lifecycle::LifecycleManager>,
) -> axum::Router {
    admin_router_inner(
        storage,
        Some(host),
        reserved_names,
        chain_data,
        started_at,
        events,
        auth,
        Some(lifecycle),
    )
}

#[allow(clippy::too_many_arguments)]
fn admin_router_inner(
    storage: ModuleStorage,
    host: Option<Arc<dyn crate::host_v2::ModuleHostHandle>>,
    reserved_names: Vec<String>,
    chain_data: Option<Arc<dyn mitos_data_plane::ChainDataPlane>>,
    started_at: SystemTime,
    events: crate::events::EventRing,
    auth: Arc<Authenticator>,
    lifecycle: Option<Arc<crate::lifecycle::LifecycleManager>>,
) -> axum::Router {
    let state = AdminState {
        storage,
        host,
        reserved_names,
        chain_data,
        started_at,
        events,
        auth: auth.clone(),
        lifecycle,
    };

    // Recapture is its OWN sub-router because it is the one admin route a
    // WORKER calls: `holder-map`'s production cron recaptures
    // `holder-distribution` with the same secret it subscribes with. Under
    // `Admin(Mutate)` — which every namespace token is refused — that cron
    // would 403 the day its secret is rotated.
    let recapture = axum::Router::new()
        .route("/_admin/modules/{id}/recapture", post(recapture_module))
        .layer(axum::middleware::from_fn_with_state(
            auth.clone(),
            crate::auth::require_recapture,
        ));

    // Everything else: `Admin(Read)` on `GET`, `Admin(Mutate)` otherwise,
    // decided once in the layer rather than in twenty handlers.
    let authed = axum::Router::new()
        .route("/_admin", get(admin_index))
        .route("/_admin/status", get(status))
        .route("/_admin/events", get(list_events))
        .route("/_admin/modules", get(list_modules))
        .route(
            "/_admin/modules/{id}",
            get(get_module).post(upload_module).delete(delete_module),
        )
        .route("/_admin/modules/{id}/restart", post(restart_module))
        .route("/_admin/modules/{id}/start", post(start_module))
        .route("/_admin/modules/{id}/pin", post(pin_module))
        .route("/_admin/modules/{id}/unpin", post(unpin_module))
        .route("/_admin/modules/{id}/evict", post(evict_module))
        .route("/_admin/modules/{id}/companions", get(list_companions))
        .route(
            "/_admin/modules/{id}/companions/{client_id}/{companion_key}",
            axum::routing::delete(delete_companion),
        )
        .route("/_admin/modules/{id}/last-trap", get(last_trap))
        .route(
            "/_admin/modules/{id}/emissions",
            get(list_emissions).delete(purge_emissions),
        )
        .route(
            "/_admin/modules/{id}/emissions/{emission_id}/replay",
            post(replay_emission),
        )
        .route("/_admin/utxos", get(get_utxos_by_address))
        .route("/_admin/blocks/{slot}", get(get_block_by_slot))
        .route("/_admin/blocks/by-tx/{tx_hash}", get(get_block_by_tx))
        .layer(axum::middleware::from_fn_with_state(
            auth.clone(),
            crate::auth::require_admin,
        ));

    // Authentication wraps BOTH authorized sub-routers, outermost, so a
    // handler can only ever run with a `Principal` in its extensions and an
    // unknown token is a 401 before any `authorize` call is reached.
    let gated = recapture
        .merge(authed)
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::authenticate,
        ));

    // `/metrics` is OPEN (no auth), like `/health` — operational
    // gauges/counters for Prometheus, no secrets. Kept in this router
    // (rather than the bundle) so it shares `AdminState` + the
    // emission-scan helpers. Merged AFTER the auth layers so it is exempt
    // from both.
    let open = axum::Router::new().route("/metrics", get(metrics));

    gated.merge(open).with_state(state)
}

// -----------------------------------------------------------------------------
// Response shapes
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub struct ModuleSummary {
    pub id: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub abi_version: String,
    pub trap_strategy: String,
    pub crate_version: String,
    /// What the module declares about itself. `0` for a pre-v3 artifact,
    /// which is informative rather than missing — every artifact built before
    /// `[lifecycle]` existed IS revision 0, schema 0.
    ///
    /// `revision` is NOT identity: bumping it alone moves nobody. It is what
    /// a client move consults to decide which upgrade actions it crosses.
    pub revision: Revision,
    /// The kv layout the module expects. A bump means the state is not
    /// copy-compatible; with no `migrate` export in the WIT it means rebuild.
    pub state_schema: StateSchema,
    /// The instance lifecycle, on the READ routes (`GET /_admin/modules` and
    /// `GET /_admin/modules/{id}`). Absent from an upload response, which
    /// describes the artifact rather than what is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<StatusLifecycle>,
}

impl From<&Manifest> for ModuleSummary {
    fn from(m: &Manifest) -> Self {
        Self {
            id: m.module.id.clone(),
            sha256: m.module.sha256.clone(),
            size_bytes: m.module.size_bytes,
            abi_version: format!("{}.{}", m.abi.version_major, m.abi.version_minor),
            trap_strategy: m.trap_policy.strategy.clone(),
            crate_version: m.build.crate_version.clone(),
            revision: m.lifecycle.revision,
            state_schema: m.lifecycle.state_schema,
            lifecycle: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadResponse {
    pub ok: bool,
    pub module: ModuleSummary,
}

/// Whole-host health snapshot for `GET /_admin/status`. One call an
/// agent can poll for version / liveness / chain position / per-
/// module summary. Read-only and side-effect free (safe to poll).
/// LLM-decodable: stable keys, decision-oriented fields, `*_secs_ago`
/// deltas rather than raw timestamps the caller must diff.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Crate version (`CARGO_PKG_VERSION`). Currently the static
    /// workspace `0.0.1`; `build_sha` is the meaningful identifier.
    pub version: String,
    /// Git short SHA stamped at build time (`build.rs`). `"unknown"`
    /// for non-checkout builds.
    pub build_sha: String,
    /// Seconds since process start (same clock as `/health`).
    pub uptime_secs: u64,
    /// Chain tip the data plane is reading from. `None` when no
    /// chain-data handle is wired (artifact-only) or the query fails.
    pub tip: Option<StatusTip>,
    /// Oldest slot whose block body is still in the archive. Below
    /// this, `datum_by_hash` / block fetch fail (explains empty
    /// CIP-68 metadata for old assets). `None` until wired to dolos.
    pub archive_horizon_slot: Option<u64>,
    /// Per-module summary.
    pub modules: Vec<StatusModule>,
    /// Which auth posture the host booted into — `legacy` when
    /// `MITOS_NAMESPACES_FILE` is unset, `namespaces` otherwise. Lets an
    /// operator confirm a rollout step took effect without reading the unit
    /// file.
    #[serde(default = "AuthMode::legacy_default")]
    pub auth_mode: AuthMode,
    /// The configured namespaces. Names, tiers and COUNTS only — never a
    /// token, never a `token_env` value, never a client list that would let
    /// this endpoint enumerate the deployment.
    #[serde(default)]
    pub namespaces: Vec<StatusNamespace>,
    /// Client ids with a registration on disk that no namespace owns. They
    /// keep running and keep being dialled; their next subscribe is a 403,
    /// which is the signal to add them to a namespace.
    #[serde(default)]
    pub unowned_clients: Vec<String>,
}

impl AuthMode {
    /// A status response from a host that predates this field came from a
    /// host with no namespaces.
    fn legacy_default() -> Self {
        Self::Legacy
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusNamespace {
    pub name: String,
    /// The queue order for staging catch-ups, and (today) the rule for
    /// whether this namespace may run a whole-module recapture. The lifecycle
    /// crate's own enum, not a rendering of it — a new tier is then a compile
    /// error here rather than a string nobody updated.
    pub tier: Tier,
    /// How many clients the namespace lists. A count, not the list: the
    /// deployment's client roster is not something a read-only dashboard
    /// token should be able to enumerate.
    pub clients: usize,
    /// Where the rollout has got to: still accepting the operator token as a
    /// client credential, or refusing it.
    pub operator_token_as_client: OperatorTokenAsClient,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusTip {
    pub slot: u64,
    /// Block hash hex; `None` for `Origin` / slot-only tips.
    pub hash: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusModule {
    pub id: String,
    /// Number of registered companions across the two-level
    /// `<client_id>/<companion_key>` layout.
    pub companions: usize,
    /// Undelivered emissions waiting to be dialed. The headline
    /// "is this module backed up?" signal — a non-draining `queued`
    /// is a stalled lane (see the dialer-backoff incident).
    pub queued: usize,
    /// Emissions dialed but not yet acked (in flight).
    pub pending: usize,
    /// The module's follower task is alive (truthful liveness — a
    /// follower that exited on error reports `false` even while its
    /// slot lingers in the host map). `companions > 0` with
    /// `running: false` means the module's consumers are getting
    /// nothing — the watchdog restarts it; alerting should page on it.
    pub running: bool,
    /// A recapture is currently in flight for this module (the
    /// per-module mutex is held). Answers "is a recapture still
    /// running?" without a journal grep.
    pub recapture_in_progress: bool,
    /// A bootstrap/rebootstrap pass (`start`) is currently running.
    pub bootstrap_in_progress: bool,
    /// Result of the module's last rebootstrap (survives the bounded
    /// events ring). `None` if none has run this process.
    pub last_result: Option<StatusLastResult>,
    /// Age of the last captured trap fixture, or `None` if none.
    /// A small value means a module recently trapped.
    pub last_trap_secs_ago: Option<u64>,
    /// The instance lifecycle. `None` only on an artifact-only router, which
    /// has no lifecycle wired.
    ///
    /// Read `lifecycle.desired` against `running` to tell the two failures
    /// apart: `Running`/`false` is an outage the watchdog is about to fix;
    /// `Stopped`/`false` is an operator's decision and must be left alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<StatusLifecycle>,
}

/// An instance's lifecycle, as the admin API reports it. The lifecycle
/// crate's own enums, not renderings of them — a new phase is then a compile
/// error here rather than a string nobody updated.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusLifecycle {
    /// What IS: `active`, `draining`, `stopped`, `staging`, `reaped`.
    pub phase: Phase,
    /// What is WANTED: `running`, `stopped`, `pinned`. Orthogonal to `phase`.
    pub desired: DesiredState,
    /// Attached clients as `namespace/client`. These are what keeps the
    /// instance alive; when the last one leaves, WHY it left decides whether
    /// the instance drains, stops or alerts.
    pub attached: Vec<String>,
    /// The tier governing this instance — the most protected of every
    /// namespace that has EVER attached, never relaxed on detach. `None`
    /// before any client has attached.
    pub governing_tier: Option<Tier>,
}

impl From<&Instance> for StatusLifecycle {
    fn from(instance: &Instance) -> Self {
        Self {
            phase: instance.phase,
            desired: instance.desired,
            attached: crate::lifecycle::attached_labels(instance),
            governing_tier: instance.governing.map(|p| p.tier),
        }
    }
}

/// Best-effort: a read surface must not 500 because one module's record is
/// unreadable, and the operator has `/_admin/events` for the reason.
fn lifecycle_of(state: &AdminState, id: &str) -> Option<StatusLifecycle> {
    let instance = state.lifecycle.as_ref()?.instance(id);
    match instance {
        Ok(instance) => instance.as_ref().map(StatusLifecycle::from),
        Err(e) => {
            tracing::warn!(module = %id, error = %e, "status: lifecycle unreadable");
            None
        }
    }
}

/// Serialized view of a module's last rebootstrap result.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusLastResult {
    /// What ran (`rebootstrap`).
    pub kind: String,
    pub utxos_ingested: u64,
    pub duration_ms: u64,
    /// `completed` | `partial` | `failed`.
    pub outcome: String,
    pub secs_ago: u64,
}

/// Per-companion detail for `GET /_admin/modules/{id}/companions`.
/// Replaces the journal greps for "which companions are subscribed +
/// draining" and "where is companion X up to". LLM-decodable: the
/// `queued`/`last_drain_secs_ago` pair is the per-companion stall
/// signal; the source for `/metrics` per-companion gauges.
#[derive(Debug, Serialize, Deserialize)]
pub struct CompanionDetail {
    pub client_id: String,
    pub companion_key: String,
    /// Subscribed targets (module / indexer names).
    pub targets: Vec<String>,
    /// Watched policy hexes. Empty when interest is unbounded (see
    /// `unbounded_interest`) or no policy narrowing was declared.
    pub watched_policies: Vec<String>,
    /// Interest matches every policy (an `Any`/`Fingerprint`
    /// selector, or no policy narrowing) rather than the
    /// `watched_policies` list.
    pub unbounded_interest: bool,
    /// Host's resume cursor slot for this companion (last applied
    /// chain point it reported at subscribe). `None` for a fresh
    /// companion that hasn't checkpointed.
    pub resume_slot: Option<u64>,
    /// Per-status emission counts scoped to this companion.
    pub queued: usize,
    pub pending: usize,
    pub acked: usize,
    pub nacked: usize,
    pub timeout: usize,
    /// Age of the most recent dial (max `sent_at`). `None` if never
    /// dialed. Large/growing with non-zero `queued` = a stalled lane.
    pub last_drain_secs_ago: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompanionsResponse {
    pub module: String,
    /// A recapture is currently in flight for this module.
    pub recapture_in_progress: bool,
    pub companions: Vec<CompanionDetail>,
}

/// Query for `GET /_admin/utxos`.
#[derive(Debug, Deserialize)]
struct UtxosQuery {
    /// Bech32 address whose unspent set to list.
    address: String,
}

/// One unspent output reference.
#[derive(Debug, Serialize)]
struct UtxoRefEntry {
    tx_hash: String,
    index: u32,
}

/// Response for `GET /_admin/utxos`.
#[derive(Debug, Serialize)]
struct UtxosResponse {
    address: String,
    count: usize,
    /// The data plane capped this result, so it is a PREFIX of the unspent set
    /// and not the set itself.
    ///
    /// Reported because the difference is invisible otherwise, and the
    /// consequence is severe for the obvious consumer: a reconciler that
    /// treats "not in this list" as "not on chain" will delete live rows. The
    /// jpg ask-book walker did precisely that against the V1 listing address,
    /// which holds more than the cap.
    truncated: bool,
    utxos: Vec<UtxoRefEntry>,
}

/// Query for `GET /_admin/events`.
#[derive(Debug, Deserialize)]
struct EventsQuery {
    /// Only events with `seq >= after`. Poll cursor: pass the prior
    /// response's `latest_seq` to get strictly-newer events.
    #[serde(default)]
    after: Option<u64>,
    /// Filter to one module.
    #[serde(default)]
    module: Option<String>,
    /// Filter to one event kind (e.g. `trap`, `recapture_completed`).
    #[serde(default)]
    kind: Option<String>,
    /// Max events returned (default 200, capped 1000).
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct EventsResponse {
    events: Vec<crate::events::AdminEvent>,
    /// Highest seq assigned so far — the cursor to poll with as
    /// `?after=` for newer events (`mitos-admin tail --follow`).
    latest_seq: u64,
}

#[derive(Debug, Serialize)]
struct AdminError {
    error: String,
    code: &'static str,
}

#[derive(Debug, thiserror::Error)]
enum HandlerError {
    #[error("module id in URL ({url}) doesn't match manifest ({manifest})")]
    IdMismatch { url: String, manifest: String },
    #[error("missing multipart field `{0}`")]
    MissingField(&'static str),
    #[error("multipart parse: {0}")]
    Multipart(String),
    #[error("manifest: {0}")]
    Manifest(#[from] ManifestError),
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("wasmtime: {0}")]
    Wasmtime(String),

    /// Chain-data plane returned an error servicing an admin
    /// query (today: only the block-by-slot route). Operator
    /// retries; persistent failures point to the dolos archive.
    #[error("chain-data: {0}")]
    ChainData(String),

    /// Block-by-tx route: the `{tx_hash}` path segment couldn't
    /// be parsed as 32 bytes of lowercase hex (64 chars).
    #[error("invalid tx hash `{0}`: must be 64 lowercase hex chars")]
    BadTxHash(String),
    /// Module ID shadows an in-tree indexer's name (e.g. uploading
    /// a module called `mint-burn` while the host has the
    /// `mint-burn` indexer registered). Rejected to keep the
    /// unified-subscribe routing unambiguous — see
    /// `docs/design/UNIFIED_SUBSCRIBE.md`.
    #[error("module id `{0}` shadows reserved in-tree indexer name")]
    ReservedName(String),

    /// Per-companion recapture: the named companion has no
    /// registration for this module, so there is nothing to resync.
    #[error("companion `{0}` has no registration for this module")]
    RecaptureCompanionUnknown(String),

    /// Recapture: per-module mutex held by another in-flight call.
    /// Operator can retry once the first completes.
    #[error("recapture already in progress for module `{0}`")]
    RecaptureInProgress(String),

    /// Recapture: the host's dialer isn't wired in this bundle
    /// (e.g. an artifact-only deployment). Recapture is
    /// unavailable until the bundle calls `host.set_dialer`.
    #[error("recapture unavailable: {0}")]
    RecaptureUnavailable(String),

    /// Recapture: module not registered. Distinct from
    /// "module exists but no subscribers" — that surfaces as a
    /// `recapture_failed` event from the detached recapture task.
    #[error("module `{0}` not registered")]
    ModuleNotFound(String),

    /// The instance lifecycle refused the operator's command. A legitimate
    /// state, not a malformed request — `StillAttached` is the evict
    /// safety check, and `Reaped` is a module that is already gone.
    #[error("{0}")]
    Lifecycle(#[from] crate::lifecycle::LifecycleError),

    /// An operator command on a router with no lifecycle wired (artifact-only
    /// deployments and the inspector). Same posture as `RecaptureUnavailable`.
    #[error("lifecycle unavailable: no host wired in this admin router")]
    LifecycleUnavailable,

    /// An operation that would START a follower, on an instance that is not
    /// supposed to be running. Recapture is the one that does: it wipes the
    /// bootstrap flags and restarts the module, so allowing it here would be
    /// a fourth way an operator's stop gets undone.
    #[error("module `{0}` is not running; start it first (POST …/start)")]
    NotRunnable(String),
}

impl HandlerError {
    fn code(&self) -> &'static str {
        match self {
            Self::IdMismatch { .. } => "id_mismatch",
            Self::MissingField(_) => "missing_field",
            Self::Multipart(_) => "multipart_parse",
            Self::Manifest(ManifestError::AbiMismatch { .. }) => "abi_mismatch",
            Self::Manifest(ManifestError::WitMismatch { .. }) => "wit_mismatch",
            Self::Manifest(ManifestError::WitShaMismatch { .. }) => "wit_sha_mismatch",
            Self::Manifest(ManifestError::WitShaMissing) => "wit_sha_missing",
            Self::Manifest(ManifestError::ShaMismatch { .. }) => "sha_mismatch",
            Self::Manifest(ManifestError::SizeMismatch { .. }) => "size_mismatch",
            Self::Manifest(ManifestError::InvalidModuleId(_)) => "invalid_module_id",
            Self::Manifest(ManifestError::InvalidTrapStrategy(_)) => "invalid_trap_strategy",
            Self::Manifest(ManifestError::Parse(_)) => "manifest_parse",
            Self::Manifest(ManifestError::Lifecycle(_)) => "lifecycle_ledger",
            Self::Storage(StorageError::UploadInProgress(_)) => "upload_in_progress",
            Self::Storage(_) => "storage_io",
            Self::Wasmtime(_) => "wasm_invalid",
            Self::ChainData(_) => "chain_data",
            Self::BadTxHash(_) => "bad_tx_hash",
            Self::ReservedName(_) => "reserved_name",
            Self::RecaptureCompanionUnknown(_) => "recapture_companion_unknown",
            Self::RecaptureInProgress(_) => "recapture_in_progress",
            Self::RecaptureUnavailable(_) => "recapture_unavailable",
            Self::ModuleNotFound(_) => "not_found",
            Self::Lifecycle(LifecycleError::NotRegistered(_)) => "not_found",
            Self::Lifecycle(LifecycleError::Rejected(_)) => "lifecycle_rejected",
            Self::Lifecycle(LifecycleError::Storage(_)) => "storage_io",
            Self::LifecycleUnavailable => "lifecycle_unavailable",
            Self::NotRunnable(_) => "not_runnable",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Storage(StorageError::UploadInProgress(_)) => StatusCode::CONFLICT,
            Self::Storage(StorageError::Io(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ChainData(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ReservedName(_) => StatusCode::CONFLICT,
            Self::RecaptureInProgress(_) => StatusCode::CONFLICT,
            Self::RecaptureUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::RecaptureCompanionUnknown(_) => StatusCode::NOT_FOUND,
            Self::ModuleNotFound(_) => StatusCode::NOT_FOUND,
            // A refusal is a state, not a bad request: `StillAttached` is the
            // evict safety check an operator overrides with `?force=true`.
            Self::Lifecycle(LifecycleError::Rejected(_)) => StatusCode::CONFLICT,
            Self::Lifecycle(LifecycleError::NotRegistered(_)) => StatusCode::NOT_FOUND,
            Self::Lifecycle(LifecycleError::Storage(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::LifecycleUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::NotRunnable(_) => StatusCode::CONFLICT,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for HandlerError {
    fn into_response(self) -> Response {
        let body = AdminError {
            error: self.to_string(),
            code: self.code(),
        };
        (self.status(), Json(body)).into_response()
    }
}

// -----------------------------------------------------------------------------
// Handlers
// -----------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct AdminIndex {
    service: &'static str,
    version: &'static str,
    build_sha: &'static str,
    /// One-line auth summary.
    auth: &'static str,
    endpoints: Vec<EndpointDoc>,
}

#[derive(Debug, Serialize)]
struct EndpointDoc {
    method: &'static str,
    path: &'static str,
    /// `full` (any token), `read-only` (GET, full or read-only token),
    /// or `open` (no auth).
    auth: &'static str,
    description: &'static str,
}

/// The admin surface, source of truth for `GET /_admin`. Keep in sync
/// with the routes in `admin_router_inner`.
const ADMIN_ENDPOINTS: &[(&str, &str, &str, &str)] = &[
    (
        "GET",
        "/_admin",
        "read-only",
        "This self-describing endpoint index.",
    ),
    (
        "GET",
        "/_admin/status",
        "read-only",
        "Whole-host health: version, uptime, tip, per-module backlog/companions/trap-age.",
    ),
    (
        "GET",
        "/_admin/events",
        "read-only",
        "Recent operational events ring (recapture/trap). Query: after, module, kind, limit.",
    ),
    (
        "GET",
        "/_admin/modules",
        "read-only",
        "List registered modules.",
    ),
    (
        "GET",
        "/_admin/modules/{id}",
        "read-only",
        "One module's manifest summary.",
    ),
    (
        "POST",
        "/_admin/modules/{id}",
        "full",
        "Upload + activate a mitos-build artifact (multipart).",
    ),
    (
        "DELETE",
        "/_admin/modules/{id}",
        "full",
        "Stop the module + drop its slot.",
    ),
    (
        "POST",
        "/_admin/modules/{id}/restart",
        "full",
        "Re-instantiate the running module.",
    ),
    (
        "POST",
        "/_admin/modules/{id}/recapture",
        "full",
        "Coordinated state-rebuild. Body: {\"companion\":\"*\",\"reason\":...}. companion=* → full module rebuild (all subscribers); companion=<key> → scoped resync (re-emit that companion's scopes' snapshots, no module restart). Async: returns 202 immediately; poll /_admin/events (recapture_completed/failed | resync_routed + onboard_completed).",
    ),
    (
        "POST",
        "/_admin/modules/{id}/evict",
        "full",
        "Full retirement: stop + clear dialer state + remove artifact.",
    ),
    (
        "GET",
        "/_admin/modules/{id}/companions",
        "read-only",
        "Per-companion interest, cursor, emission counts, last-drain age.",
    ),
    (
        "DELETE",
        "/_admin/modules/{id}/companions/{client_id}/{companion_key}",
        "full",
        "Remove one companion record.",
    ),
    (
        "GET",
        "/_admin/modules/{id}/last-trap",
        "read-only",
        "Most recent trap fixture (TOML) for local replay.",
    ),
    (
        "GET",
        "/_admin/modules/{id}/emissions",
        "read-only",
        "Emissions log. Query: status, companion, limit, after_id.",
    ),
    (
        "DELETE",
        "/_admin/modules/{id}/emissions",
        "full",
        "Purge emissions (requires explicit ?status=).",
    ),
    (
        "POST",
        "/_admin/modules/{id}/emissions/{emission_id}/replay",
        "full",
        "Re-queue one emission for delivery.",
    ),
    (
        "GET",
        "/_admin/blocks/{slot}",
        "read-only",
        "Raw block CBOR at a slot (fixture capture).",
    ),
    (
        "GET",
        "/_admin/blocks/by-tx/{tx_hash}",
        "read-only",
        "Raw block CBOR resolved by tx hash.",
    ),
    (
        "GET",
        "/metrics",
        "open",
        "Prometheus text exposition (gauges + counters).",
    ),
    (
        "GET",
        "/health",
        "open",
        "Liveness + uptime + indexer list.",
    ),
];

/// `GET /_admin` — self-describing index of the admin surface so a
/// fresh operator/agent can discover endpoints + their auth scope
/// without reading source. Read-only.
async fn admin_index() -> Json<AdminIndex> {
    Json(AdminIndex {
        service: "mitos-admin",
        version: env!("CARGO_PKG_VERSION"),
        build_sha: env!("MITOS_BUILD_SHA"),
        auth: "Bearer MITOS_AUTH_TOKEN = full (any method); MITOS_READONLY_TOKEN = GET-only; \
               a namespace token has no admin access except POST .../recapture; \
               /metrics + /health are open",
        endpoints: ADMIN_ENDPOINTS
            .iter()
            .map(|&(method, path, auth, description)| EndpointDoc {
                method,
                path,
                auth,
                description,
            })
            .collect(),
    })
}

/// `GET /_admin/status` — whole-host health for `mitos-admin status`
/// and polling agents. Read-only; aggregates already-tracked state
/// (build stamp, uptime, chain tip, module/companion/trap summary).
async fn status(State(state): State<AdminState>) -> Result<Json<StatusResponse>, HandlerError> {
    let uptime_secs = SystemTime::now()
        .duration_since(state.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Tip + archive horizon are best-effort: a failed query (or no
    // chain-data handle) degrades to `None` rather than failing the
    // whole status call.
    let (tip, archive_horizon_slot) = match &state.chain_data {
        Some(cd) => {
            let tip = match cd.tip().await {
                Ok(t) => Some(StatusTip {
                    slot: t.slot(),
                    hash: t.point.hash().map(hex::encode),
                }),
                Err(e) => {
                    tracing::warn!(error = %e, "status: chain tip query failed");
                    None
                }
            };
            let horizon = match cd.archive_horizon_slot().await {
                Ok(h) => h,
                Err(e) => {
                    tracing::warn!(error = %e, "status: archive horizon query failed");
                    None
                }
            };
            (tip, horizon)
        }
        None => (None, None),
    };

    // Snapshots of host-side state for the whole summary.
    let (recapturing, bootstrapping, last_results, running_set): (
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
        std::collections::HashMap<String, crate::host_v2::LastResult>,
        std::collections::HashSet<String>,
    ) = match &state.host {
        Some(host) => (
            host.recapture_in_flight().await.into_iter().collect(),
            host.bootstrap_in_flight().await.into_iter().collect(),
            host.last_results().await.into_iter().collect(),
            host.list_running().await.into_iter().collect(),
        ),
        None => Default::default(),
    };

    let now = SystemTime::now();
    let now_unix = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ids = state.storage.list_modules()?;
    let mut modules = Vec::with_capacity(ids.len());
    for id in ids {
        let last_trap_secs_ago = state
            .storage
            .last_trap_path(&id)
            .ok()
            .and_then(|path| std::fs::metadata(path).ok())
            .and_then(|m| m.modified().ok())
            .and_then(|mtime| now.duration_since(mtime).ok())
            .map(|d| d.as_secs());
        let companions = state.storage.count_companions(&id);
        // Backlog depth. Best-effort full-scan of the emissions log
        // (O(rows)) — fine for a polled status call at current sizes;
        // if scrape latency bites, cache with a short TTL or maintain
        // atomic counters on the emit/drain path. Degrades to 0/0 if
        // the store can't be opened (e.g. no emissions yet).
        let (queued, pending) = emission_backlog(&state, &id);
        let last_result = last_results.get(&id).map(|r| StatusLastResult {
            kind: r.kind.to_string(),
            utxos_ingested: r.utxos_ingested,
            duration_ms: r.duration_ms,
            outcome: r.outcome.to_string(),
            secs_ago: now_unix.saturating_sub(r.at_unix),
        });
        modules.push(StatusModule {
            id: id.clone(),
            companions,
            queued,
            pending,
            running: running_set.contains(&id),
            recapture_in_progress: recapturing.contains(&id),
            bootstrap_in_progress: bootstrapping.contains(&id),
            last_result,
            last_trap_secs_ago,
            lifecycle: lifecycle_of(&state, &id),
        });
    }

    // Auth posture. Names, tiers and counts — nothing derived from a token.
    let registry = state.auth.registry();
    let namespaces = registry
        .namespaces()
        .iter()
        .map(|def| StatusNamespace {
            name: def.name.to_string(),
            tier: def.profile.tier,
            clients: match &def.clients {
                ClientSet::Only(clients) => clients.len(),
                // The implicit legacy namespace owns every client there is,
                // so there is no roster to count.
                ClientSet::Any => 0,
            },
            operator_token_as_client: registry.operator_token_as_client(),
        })
        .collect();

    Ok(Json(StatusResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        build_sha: env!("MITOS_BUILD_SHA").to_string(),
        uptime_secs,
        tip,
        archive_horizon_slot,
        modules,
        auth_mode: state.auth.mode(),
        namespaces,
        unowned_clients: state.auth.unowned_clients(&state.storage),
    }))
}

/// `GET /_admin/events` — recent operational events (recapture,
/// trap) from the in-memory ring. Read-only; safe to poll. The
/// structured replacement for `journalctl | grep`. Surfaced as
/// `mitos-admin tail`.
async fn list_events(
    State(state): State<AdminState>,
    axum::extract::Query(q): axum::extract::Query<EventsQuery>,
) -> Json<EventsResponse> {
    let after = q.after.unwrap_or(0);
    let limit = q.limit.unwrap_or(200).min(1000);
    let module = q.module.as_deref().filter(|s| !s.is_empty());
    let kind = q.kind.as_deref().filter(|s| !s.is_empty());
    let events = state.events.snapshot(after, module, kind, limit);
    Json(EventsResponse {
        events,
        latest_seq: state.events.latest_seq(),
    })
}

/// Escape a Prometheus label value (`\`, `"`, newline).
fn esc_label(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Write a metric family header (`# HELP` + `# TYPE`).
fn metric_header(out: &mut String, name: &str, help: &str, kind: &str) {
    use std::fmt::Write as _;
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// `GET /metrics` — Prometheus text exposition. **Open** (no auth,
/// like `/health`): operational gauges + counters for a dashboard /
/// alerting, no secrets. Computed at scrape time from the same
/// sources as the JSON endpoints — one emissions-store scan per
/// module (see the cost note on `emission_backlog`); cache or move to
/// atomic counters if scrape latency becomes a problem.
async fn metrics(State(state): State<AdminState>) -> Response {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(8192);

    // ----- global -----
    let uptime = SystemTime::now()
        .duration_since(state.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    metric_header(
        &mut out,
        "mitos_build_info",
        "Build version + git sha (value always 1).",
        "gauge",
    );
    let _ = writeln!(
        out,
        "mitos_build_info{{version=\"{}\",build_sha=\"{}\"}} 1",
        esc_label(env!("CARGO_PKG_VERSION")),
        esc_label(env!("MITOS_BUILD_SHA")),
    );
    metric_header(
        &mut out,
        "mitos_uptime_seconds",
        "Seconds since process start.",
        "gauge",
    );
    let _ = writeln!(out, "mitos_uptime_seconds {uptime}");

    if let Some(cd) = &state.chain_data {
        if let Ok(tip) = cd.tip().await {
            metric_header(
                &mut out,
                "mitos_chain_tip_slot",
                "Current chain tip slot.",
                "gauge",
            );
            let _ = writeln!(out, "mitos_chain_tip_slot {}", tip.slot());
        }
        if let Ok(Some(horizon)) = cd.archive_horizon_slot().await {
            metric_header(
                &mut out,
                "mitos_archive_horizon_slot",
                "Oldest slot still in the archive; datum/input lookups below this miss (empty CIP-68 traits).",
                "gauge",
            );
            let _ = writeln!(out, "mitos_archive_horizon_slot {horizon}");
        }
    }

    // ----- gather per-module + per-companion -----
    let (recapturing, bootstrapping, last_results, running): (
        std::collections::HashSet<String>,
        std::collections::HashSet<String>,
        std::collections::HashMap<String, crate::host_v2::LastResult>,
        std::collections::HashSet<String>,
    ) = match &state.host {
        Some(host) => (
            host.recapture_in_flight().await.into_iter().collect(),
            host.bootstrap_in_flight().await.into_iter().collect(),
            host.last_results().await.into_iter().collect(),
            host.list_running().await.into_iter().collect(),
        ),
        None => Default::default(),
    };
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let ids = state.storage.list_modules().unwrap_or_default();

    struct ModMetrics {
        module: String,
        companions: usize,
        running: bool,
        recapture: bool,
        bootstrap: bool,
        last_rebootstrap: Option<(u64, u64)>, // (utxos_ingested, secs_ago)
        last_trap_age: Option<u64>,
        status_totals: [usize; 5], // queued, pending, acked, nacked, timeout
        comps: Vec<CompMetrics>,
    }
    struct CompMetrics {
        companion: String,
        client: String,
        queued: usize,
        pending: usize,
        last_drain_age: Option<u64>,
        oldest_queued_age: Option<u64>,
    }

    let mut mods = Vec::with_capacity(ids.len());
    for id in &ids {
        let last_trap_age = state
            .storage
            .last_trap_path(id)
            .ok()
            .and_then(|path| std::fs::metadata(path).ok())
            .and_then(|m| m.modified().ok())
            .and_then(|mt| SystemTime::now().duration_since(mt).ok())
            .map(|d| d.as_secs());
        let stats = companion_emission_stats(&state, id);
        let mut status_totals = [0usize; 5];
        let mut comps = Vec::with_capacity(stats.len());
        for ((companion, client), s) in &stats {
            status_totals[0] += s.queued;
            status_totals[1] += s.pending;
            status_totals[2] += s.acked;
            status_totals[3] += s.nacked;
            status_totals[4] += s.timeout;
            comps.push(CompMetrics {
                companion: companion.clone(),
                client: client.clone(),
                queued: s.queued,
                pending: s.pending,
                last_drain_age: s.last_sent_unix.map(|u| now.saturating_sub(u)),
                oldest_queued_age: s.oldest_queued_unix.map(|u| now.saturating_sub(u)),
            });
        }
        let last_rebootstrap = last_results
            .get(id)
            .map(|r| (r.utxos_ingested, now.saturating_sub(r.at_unix)));
        mods.push(ModMetrics {
            module: id.clone(),
            companions: state.storage.count_companions(id),
            running: running.contains(id),
            recapture: recapturing.contains(id),
            bootstrap: bootstrapping.contains(id),
            last_rebootstrap,
            last_trap_age,
            status_totals,
            comps,
        });
    }

    // ----- emit, grouped by metric family -----
    const STATUS_NAMES: [&str; 5] = ["queued", "pending", "acked", "nacked", "timeout"];

    metric_header(
        &mut out,
        "mitos_module_companions",
        "Registered companions per module.",
        "gauge",
    );
    for m in &mods {
        let _ = writeln!(
            out,
            "mitos_module_companions{{module=\"{}\"}} {}",
            esc_label(&m.module),
            m.companions,
        );
    }

    metric_header(
        &mut out,
        "mitos_module_running",
        "1 if the module's follower task is alive (truthful liveness — a \
         follower that exited on error reports 0 even while its slot \
         lingers). companions>0 with running=0 is the page-worthy state.",
        "gauge",
    );
    for m in &mods {
        let _ = writeln!(
            out,
            "mitos_module_running{{module=\"{}\"}} {}",
            esc_label(&m.module),
            u8::from(m.running),
        );
    }

    metric_header(
        &mut out,
        "mitos_recapture_in_progress",
        "1 if a recapture is currently in flight for the module.",
        "gauge",
    );
    for m in &mods {
        let _ = writeln!(
            out,
            "mitos_recapture_in_progress{{module=\"{}\"}} {}",
            esc_label(&m.module),
            u8::from(m.recapture),
        );
    }

    metric_header(
        &mut out,
        "mitos_bootstrap_in_progress",
        "1 if a bootstrap/rebootstrap pass is currently running for the module.",
        "gauge",
    );
    for m in &mods {
        let _ = writeln!(
            out,
            "mitos_bootstrap_in_progress{{module=\"{}\"}} {}",
            esc_label(&m.module),
            u8::from(m.bootstrap),
        );
    }

    metric_header(
        &mut out,
        "mitos_last_rebootstrap_utxos",
        "UTxOs ingested by the module's most recent rebootstrap (omitted if none).",
        "gauge",
    );
    for m in &mods {
        if let Some((utxos, _age)) = m.last_rebootstrap {
            let _ = writeln!(
                out,
                "mitos_last_rebootstrap_utxos{{module=\"{}\"}} {utxos}",
                esc_label(&m.module),
            );
        }
    }

    metric_header(
        &mut out,
        "mitos_last_rebootstrap_age_seconds",
        "Seconds since the module's most recent rebootstrap (omitted if none).",
        "gauge",
    );
    for m in &mods {
        if let Some((_utxos, age)) = m.last_rebootstrap {
            let _ = writeln!(
                out,
                "mitos_last_rebootstrap_age_seconds{{module=\"{}\"}} {age}",
                esc_label(&m.module),
            );
        }
    }

    metric_header(
        &mut out,
        "mitos_module_last_trap_age_seconds",
        "Age of the module's last trap fixture (omitted if none).",
        "gauge",
    );
    for m in &mods {
        if let Some(age) = m.last_trap_age {
            let _ = writeln!(
                out,
                "mitos_module_last_trap_age_seconds{{module=\"{}\"}} {age}",
                esc_label(&m.module),
            );
        }
    }

    metric_header(
        &mut out,
        "mitos_module_emissions",
        "Emission count by status per module (queued+pending = backlog).",
        "gauge",
    );
    for m in &mods {
        for (i, status) in STATUS_NAMES.iter().enumerate() {
            let _ = writeln!(
                out,
                "mitos_module_emissions{{module=\"{}\",status=\"{status}\"}} {}",
                esc_label(&m.module),
                m.status_totals[i],
            );
        }
    }

    metric_header(
        &mut out,
        "mitos_companion_backlog",
        "Undelivered emissions per companion by status (queued/pending).",
        "gauge",
    );
    for m in &mods {
        for c in &m.comps {
            for (status, val) in [("queued", c.queued), ("pending", c.pending)] {
                let _ = writeln!(
                    out,
                    "mitos_companion_backlog{{module=\"{}\",companion=\"{}\",client=\"{}\",status=\"{status}\"}} {val}",
                    esc_label(&m.module),
                    esc_label(&c.companion),
                    esc_label(&c.client),
                );
            }
        }
    }

    metric_header(
        &mut out,
        "mitos_companion_last_drain_age_seconds",
        "Seconds since the companion's most recent dial (omitted if never).",
        "gauge",
    );
    for m in &mods {
        for c in &m.comps {
            if let Some(age) = c.last_drain_age {
                let _ = writeln!(
                    out,
                    "mitos_companion_last_drain_age_seconds{{module=\"{}\",companion=\"{}\",client=\"{}\"}} {age}",
                    esc_label(&m.module),
                    esc_label(&c.companion),
                    esc_label(&c.client),
                );
            }
        }
    }

    metric_header(
        &mut out,
        "mitos_companion_oldest_queued_age_seconds",
        "Age of the companion's oldest undelivered emission (a stalled lane signal; omitted if none queued).",
        "gauge",
    );
    for m in &mods {
        for c in &m.comps {
            if let Some(age) = c.oldest_queued_age {
                let _ = writeln!(
                    out,
                    "mitos_companion_oldest_queued_age_seconds{{module=\"{}\",companion=\"{}\",client=\"{}\"}} {age}",
                    esc_label(&m.module),
                    esc_label(&c.companion),
                    esc_label(&c.client),
                );
            }
        }
    }

    metric_header(
        &mut out,
        "mitos_events_total",
        "Cumulative operational events since start, by kind (e.g. trap, recapture_completed).",
        "counter",
    );
    for (module, kind, count) in state.events.totals() {
        let _ = writeln!(
            out,
            "mitos_events_total{{module=\"{}\",kind=\"{kind}\"}} {count}",
            esc_label(&module),
        );
    }

    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        out,
    )
        .into_response()
}

/// Best-effort count of a module's `Queued` + `Pending` emissions —
/// the backlog depth surfaced in `/_admin/status`. A module that has
/// never emitted (no store) counts as `(0, 0)`. Shared with the
/// `/metrics` exposition once that lands.
fn emission_backlog(state: &AdminState, id: &str) -> (usize, usize) {
    use crate::emissions::EmissionStatus::{Pending, Queued};
    let Ok(store) = state.storage.emissions_store(id) else {
        return (0, 0);
    };
    let Ok(rows) = store.list_filtered(|r| matches!(r.status, Queued | Pending)) else {
        return (0, 0);
    };
    let mut queued = 0;
    let mut pending = 0;
    for r in &rows {
        match r.status {
            Queued => queued += 1,
            Pending => pending += 1,
            _ => {}
        }
    }
    (queued, pending)
}

/// Per-companion emission accumulator, built from one scan of a
/// module's emissions store keyed by `(companion_id, client_id)`.
#[derive(Default)]
struct CompanionEmissionStat {
    queued: usize,
    pending: usize,
    acked: usize,
    nacked: usize,
    timeout: usize,
    /// Max `sent_at` (unix secs) across the companion's rows.
    last_sent_unix: Option<u64>,
    /// Min `matched_at` (unix secs) across the companion's still-
    /// `Queued` rows — the age of the oldest undelivered emission.
    /// Distinguishes a transient burst from a genuine stall.
    oldest_queued_unix: Option<u64>,
}

/// Parse an emissions-store timestamp (`"unix:<secs>"`) to epoch secs.
fn parse_emission_unix(s: &str) -> Option<u64> {
    s.strip_prefix("unix:")
        .and_then(|n| n.trim().parse::<u64>().ok())
}

/// Scan a module's emissions store once, bucketing per-status counts
/// (and the latest dial time) per `(companion_id, client_id)`. Empty
/// map if the store can't be opened. The store's `companion_id` is
/// the raw companion key, so it joins directly against the companion
/// record's `companion_key`.
fn companion_emission_stats(
    state: &AdminState,
    id: &str,
) -> std::collections::HashMap<(String, String), CompanionEmissionStat> {
    use crate::emissions::EmissionStatus::{Acked, Nacked, Pending, Queued, Timeout};
    let mut map: std::collections::HashMap<(String, String), CompanionEmissionStat> =
        std::collections::HashMap::new();
    let Ok(store) = state.storage.emissions_store(id) else {
        return map;
    };
    let Ok(rows) = store.list_filtered(|_| true) else {
        return map;
    };
    for r in &rows {
        let entry = map
            .entry((r.companion_id.clone(), r.client_id.clone()))
            .or_default();
        match r.status {
            Queued => entry.queued += 1,
            Pending => entry.pending += 1,
            Acked => entry.acked += 1,
            Nacked => entry.nacked += 1,
            Timeout => entry.timeout += 1,
        }
        if let Some(sent) = r.sent_at.as_deref().and_then(parse_emission_unix) {
            entry.last_sent_unix = Some(entry.last_sent_unix.map_or(sent, |cur| cur.max(sent)));
        }
        if matches!(r.status, Queued)
            && let Some(matched) = parse_emission_unix(&r.matched_at)
        {
            entry.oldest_queued_unix = Some(
                entry
                    .oldest_queued_unix
                    .map_or(matched, |cur| cur.min(matched)),
            );
        }
    }
    map
}

/// `GET /_admin/modules/{id}/companions` — per-companion interest,
/// resume cursor, emission counts, and last-drain age. Read-only.
async fn list_companions(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }

    let recapture_in_progress = match &state.host {
        Some(host) => host.recapture_in_flight().await.iter().any(|m| m == &id),
        None => false,
    };

    let stats = companion_emission_stats(&state, &id);
    let now_unix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut companions = Vec::new();
    let root = state.storage.module_dir_for_companions(&id);
    if let Ok(clients) = std::fs::read_dir(&root) {
        for client in clients.flatten() {
            // Two-level layout: skip non-dirs + metadata dirs.
            if !client.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            if client.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let Ok(files) = std::fs::read_dir(client.path()) else {
                continue;
            };
            for f in files.flatten() {
                let path = f.path();
                if path.extension().is_none_or(|e| e != "cbor") {
                    continue;
                }
                let Ok(bytes) = std::fs::read(&path) else {
                    continue;
                };
                let Ok(req) = mitos_protocol::SubscribeRequest::decode(&bytes) else {
                    continue;
                };

                let (watched_policies, unbounded_interest) =
                    match mitos_protocol::watched_policies(&req.interests) {
                        // Unbounded (Any/Fingerprint) or no narrowing
                        // declared → matches every policy.
                        None => (Vec::new(), true),
                        Some(s) if s.is_empty() => (Vec::new(), true),
                        Some(s) => (
                            s.into_iter().map(|p| p.as_str().to_string()).collect(),
                            false,
                        ),
                    };
                let targets = req.targets.iter().map(|t| t.name().to_string()).collect();
                let resume_slot = req.resume_from.as_ref().and_then(|c| c.slot());

                let stat = stats.get(&(req.companion_key.clone(), req.client_id.clone()));
                let last_drain_secs_ago = stat
                    .and_then(|s| s.last_sent_unix)
                    .map(|sent| now_unix.saturating_sub(sent));

                companions.push(CompanionDetail {
                    client_id: req.client_id,
                    companion_key: req.companion_key,
                    targets,
                    watched_policies,
                    unbounded_interest,
                    resume_slot,
                    queued: stat.map_or(0, |s| s.queued),
                    pending: stat.map_or(0, |s| s.pending),
                    acked: stat.map_or(0, |s| s.acked),
                    nacked: stat.map_or(0, |s| s.nacked),
                    timeout: stat.map_or(0, |s| s.timeout),
                    last_drain_secs_ago,
                });
            }
        }
    }

    // Stable order for diff-friendly output across polls.
    companions.sort_by(|a, b| {
        (a.companion_key.as_str(), a.client_id.as_str())
            .cmp(&(b.companion_key.as_str(), b.client_id.as_str()))
    });

    Ok(Json(CompanionsResponse {
        module: id,
        recapture_in_progress,
        companions,
    })
    .into_response())
}

async fn list_modules(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ModuleSummary>>, HandlerError> {
    let ids = state.storage.list_modules()?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(m) = state.storage.read_manifest(&id)? {
            let mut summary = ModuleSummary::from(&m);
            summary.lifecycle = lifecycle_of(&state, &id);
            out.push(summary);
        }
    }
    Ok(Json(out))
}

async fn get_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    match state.storage.read_manifest(&id)? {
        Some(m) => {
            let mut summary = ModuleSummary::from(&m);
            summary.lifecycle = lifecycle_of(&state, &id);
            Ok(Json(summary).into_response())
        }
        None => Ok((StatusCode::NOT_FOUND, "module not registered").into_response()),
    }
}

async fn upload_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, HandlerError> {
    // Reserved-name guard: reject uploads whose ID collides with
    // an in-tree indexer's name (e.g. uploading a module called
    // `mint-burn` while the host has the `mint-burn` indexer
    // registered). Done before acquiring the upload lock — cheap
    // check, no point holding the lock for a request that's about
    // to fail.
    if state.reserved_names.iter().any(|n| n == &id) {
        return Err(HandlerError::ReservedName(id));
    }

    // Acquire upload lock first — fail-fast on concurrent uploads.
    let _lock = state.storage.acquire_upload_lock(&id)?;

    // Drain the multipart payload. Required: manifest + wasm.
    // Optional: config (CBOR-encoded module config — written to
    // `<storage>/<id>/config.cbor` for the host to pass to
    // `init` on follower start).
    let mut manifest_bytes: Option<Vec<u8>> = None;
    let mut wasm_bytes: Option<Vec<u8>> = None;
    let mut config_bytes: Option<Vec<u8>> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| HandlerError::Multipart(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_owned();
        let bytes = field
            .bytes()
            .await
            .map_err(|e| HandlerError::Multipart(e.to_string()))?;
        match name.as_str() {
            "manifest" => manifest_bytes = Some(bytes.to_vec()),
            "wasm" => wasm_bytes = Some(bytes.to_vec()),
            "config" => config_bytes = Some(bytes.to_vec()),
            other => {
                tracing::debug!(field = %other, "ignoring unrecognised multipart field");
            }
        }
    }

    let manifest_bytes = manifest_bytes.ok_or(HandlerError::MissingField("manifest"))?;
    let wasm_bytes = wasm_bytes.ok_or(HandlerError::MissingField("wasm"))?;

    // Parse + validate manifest against the wasm bytes.
    let manifest_str = String::from_utf8(manifest_bytes)
        .map_err(|e| HandlerError::Manifest(ManifestError::Parse(e.to_string())))?;
    let manifest = Manifest::parse(&manifest_str)?;

    if manifest.module.id != id {
        return Err(HandlerError::IdMismatch {
            url: id.clone(),
            manifest: manifest.module.id.clone(),
        });
    }

    // v2 is the only supported ABI; v1 was retired (see
    // MITOS_PLATFORM_V2.md). A manifest claiming any other major
    // is rejected.
    manifest.validate_against_host(
        &wasm_bytes,
        &[(
            crate::registry_v2::HOST_ABI_MAJOR_V2,
            "mitos:platform-v2/mitos-module-v2",
        )],
    )?;

    // Independent wasmtime validation: even if the manifest claims
    // valid shape, we re-load the bytes to confirm wasmtime
    // accepts the component. Cheap, catches tampered manifests.
    validate_with_wasmtime(&wasm_bytes)?;

    // Activate, then start ONLY IF the instance expects a follower.
    //
    // v2 called `host.replace` unconditionally here, which was the third way
    // an operator's stop got undone — deploy a new sha and a module somebody
    // had deliberately stopped came back. The artifact still lands on disk
    // either way; what the lifecycle decides is whether it RUNS.
    //
    // Config absence is meaningful — the host distinguishes "no config"
    // (`init(&[])`) from "empty CBOR config" (`init(b'')`) — so it is only
    // written when the field was present.
    match state.lifecycle.as_ref() {
        Some(lifecycle) => {
            let action = lifecycle
                .upload(&manifest, &wasm_bytes, config_bytes.as_deref())
                .await?;
            tracing::info!(
                module = %manifest.module.id,
                sha = %manifest.module.sha256,
                size = manifest.module.size_bytes,
                ?action,
                "module uploaded + activated",
            );
        }
        // Artifact-only mode (test harness, read-only deployments): the
        // artifact is on disk, and there is no follower to start.
        None => {
            state.storage.activate(&manifest, &wasm_bytes)?;
            if let Some(cfg) = config_bytes.as_ref() {
                state.storage.write_config(&id, cfg)?;
            }
            tracing::info!(
                module = %manifest.module.id,
                sha = %manifest.module.sha256,
                size = manifest.module.size_bytes,
                "module uploaded + activated (artifact-only)",
            );
        }
    }

    Ok(Json(UploadResponse {
        ok: true,
        module: ModuleSummary::from(&manifest),
    }))
}

async fn delete_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    // `Operator(Stop)` rather than `host.stop`. The difference is that the
    // intent is PERSISTED, so the next restart, the watchdog and the next
    // upload all leave it alone — in v2 each of those undid this within a
    // tick, a deploy, or a reboot.
    operator(&state, &id, OperatorCommand::Stop).await?;
    // The artifact directory stays: a stop is not a retirement, and rollback
    // wants the state. `POST …/evict` is the destructive one.
    tracing::info!(module = %id, "module stopped via DELETE");
    Ok((StatusCode::NO_CONTENT, ()).into_response())
}

/// Feed one operator command to the lifecycle and return its effects.
async fn operator(
    state: &AdminState,
    id: &str,
    command: OperatorCommand,
) -> Result<Vec<Effect>, HandlerError> {
    let lifecycle = state
        .lifecycle
        .as_ref()
        .ok_or(HandlerError::LifecycleUnavailable)?;
    Ok(lifecycle.apply(id, Event::Operator(command)).await?)
}

/// `POST /_admin/modules/{id}/start` — undo a stop. Also un-pins, so a
/// started instance with no clients drains like any other.
async fn start_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Err(HandlerError::ModuleNotFound(id));
    }
    operator(&state, &id, OperatorCommand::Start).await?;
    Ok((StatusCode::OK, "started").into_response())
}

/// `POST /_admin/modules/{id}/pin` — "run this with no subscribers, and
/// neither alert nor eject." For a module whose consumers are coming.
async fn pin_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Err(HandlerError::ModuleNotFound(id));
    }
    operator(&state, &id, OperatorCommand::Pin).await?;
    Ok((StatusCode::OK, "pinned").into_response())
}

/// `POST /_admin/modules/{id}/unpin` — hand it back to its clients. With
/// none attached it starts draining under its governing policy.
async fn unpin_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Err(HandlerError::ModuleNotFound(id));
    }
    operator(&state, &id, OperatorCommand::Unpin).await?;
    Ok((StatusCode::OK, "unpinned").into_response())
}

/// Query params for `POST /_admin/modules/{id}/evict`.
///
/// `force=true` overrides the "refuse if companions still
/// registered" safety check. Use when consumer-side cleanup has
/// already removed the companion records but the on-disk
/// `companions/<key>.cbor` files still exist (e.g. the dialer
/// crashed mid-unregister). Logged loudly.
#[derive(Debug, Default, Deserialize)]
struct EvictQuery {
    #[serde(default)]
    force: bool,
}

/// Response body for a successful evict.
///
/// `cancelled_companions` is the set of `companion_key`s whose
/// dial loops got reaped from the dialer's in-memory state.
/// Companion records on disk get removed as part of the
/// artifact-directory teardown — they're not enumerated
/// separately because the dir-removal covers them transitively.
#[derive(Debug, Serialize)]
struct EvictResponse {
    module: String,
    cancelled_companions: Vec<String>,
    artifact_removed: bool,
}

/// Conflict response body when companions are still registered
/// and `?force=true` wasn't passed. Surfaces the offending keys
/// so the operator knows which consumers to retire first.
#[derive(Debug, Serialize)]
struct EvictConflictBody {
    error: &'static str,
    companion_keys: Vec<String>,
    hint: &'static str,
}

async fn evict_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<EvictQuery>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let Some(lifecycle) = state.lifecycle.as_ref() else {
        return Err(HandlerError::LifecycleUnavailable);
    };

    // Reconcile FIRST, so the safety check below is asked of an attached set
    // that matches the disk. Without this, a client whose registration was
    // removed while the host was down would still be counted as attached and
    // an evict would 409 on a module nobody is using.
    lifecycle.reconcile(&id).await?;

    let mode = if query.force {
        EvictMode::Force
    } else {
        EvictMode::RefuseIfAttached
    };

    // The lifecycle owns the safety check now: `Rejected::StillAttached` IS
    // "companions still registered", asked of the attached set rather than a
    // directory walk. Keeping both would mean two answers to one question.
    let attached = lifecycle
        .instance(&id)?
        .map(|i| crate::lifecycle::attached_labels(&i))
        .unwrap_or_default();
    match lifecycle
        .apply(&id, Event::Operator(OperatorCommand::Evict(mode)))
        .await
    {
        Ok(_) => {}
        Err(LifecycleError::Rejected(Rejected::StillAttached(_))) => {
            return Ok((
                StatusCode::CONFLICT,
                Json(EvictConflictBody {
                    error: "clients still attached",
                    companion_keys: attached,
                    hint: "retire consumers + remove their companion records, or pass ?force=true to override",
                }),
            )
                .into_response());
        }
        Err(e) => return Err(e.into()),
    }

    if query.force && !attached.is_empty() {
        tracing::warn!(
            module = %id,
            clients = ?attached,
            "evict --force retired an instance with clients still attached",
        );
    }

    tracing::info!(
        module = %id,
        attached = attached.len(),
        force = query.force,
        "module evicted (slot stopped, dialer state cleared, artifact removed)",
    );
    Ok((
        StatusCode::OK,
        Json(EvictResponse {
            module: id,
            cancelled_companions: attached,
            artifact_removed: true,
        }),
    )
        .into_response())
}

/// `DELETE /_admin/modules/{id}/companions/{client_id}/{companion_key}`
/// — surgically remove a single companion record from the host's
/// store. Leaves other clients sharing the same `companion_key`
/// (or the same `client_id` with other keys) untouched. The
/// dialer's in-memory task for that exact tuple is also
/// cancelled. Returns:
///
/// - 204 No Content on success (record removed).
/// - 404 Not Found if the module or record doesn't exist.
/// - 500 on storage errors.
///
/// See `docs/design/MULTI_CLIENT_COMPANIONS.md` — "Admin
/// operations".
#[derive(Debug, Deserialize)]
struct DeleteCompanionParams {
    id: String,
    client_id: String,
    companion_key: String,
}

async fn delete_companion(
    State(state): State<AdminState>,
    Path(params): Path<DeleteCompanionParams>,
) -> Result<Response, HandlerError> {
    let DeleteCompanionParams {
        id,
        client_id,
        companion_key,
    } = params;

    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }

    let path = state
        .storage
        .module_dir_for_companions(&id)
        .join(&client_id)
        .join(format!("{companion_key}.cbor"));
    if !path.exists() {
        return Ok((StatusCode::NOT_FOUND, "no such companion record").into_response());
    }

    // Cancel the in-memory dialer task before removing the file —
    // otherwise the next drain tick might re-spawn it from a
    // not-yet-deleted record. Best-effort: nothing breaks if the
    // task wasn't running.
    if let Some(host) = &state.host {
        host.cancel_companion_task(&id, &client_id, &companion_key)
            .await;
    }

    std::fs::remove_file(&path).map_err(|e| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(format!(
            "remove {}: {e}",
            path.display()
        ))))
    })?;

    // The client's LAST record being gone is a detach. Best-effort: the 60s
    // tick reconciles again, and `reconcile` is a diff so nothing is lost.
    if let Some(lifecycle) = state.lifecycle.as_ref()
        && let Err(e) = lifecycle.reconcile(&id).await
    {
        tracing::warn!(
            module = %id,
            error = %e,
            "lifecycle reconcile after delete-companion failed; the next tick retries",
        );
    }

    tracing::info!(
        module = %id,
        client_id = %client_id,
        companion_key = %companion_key,
        "companion record deleted"
    );
    state.events.record(
        id.as_str(),
        crate::events::EventKind::CompanionEvicted {
            client_id,
            companion_key,
        },
    );
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

async fn restart_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let Some(host) = &state.host else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "no host wired in this admin router",
        )
            .into_response());
    };
    // `Operator(Start)` first, because restart on a STOPPED module must mean
    // "start it", and that has to change the persisted intent or the watchdog
    // would stop it again. If the instance was already running, `Start`
    // produces no `StartFollower` — and then a restart means what it always
    // meant: cycle the follower in place.
    let effects = operator(&state, &id, OperatorCommand::Start).await?;
    if !effects.contains(&Effect::StartFollower) {
        host.replace(&id)
            .await
            .map_err(|e| HandlerError::Wasmtime(format!("host.replace: {e}")))?;
    }
    tracing::info!(module = %id, "module restarted");
    Ok((StatusCode::OK, "restarted").into_response())
}

/// Request body for `POST /_admin/modules/{id}/recapture`. Both
/// fields optional — empty body means `{ "companion": "*",
/// "reason": null }`. v1 only accepts `companion = "*"`;
/// per-companion targeting is a deferred follow-up tracked in
/// `docs/design/RECAPTURE.md`.
#[derive(Debug, Default, Deserialize)]
struct RecaptureRequest {
    /// Subscriber filter. v1 only accepts `"*"` (= all
    /// subscribed companions). Optional with default `"*"`.
    #[serde(default = "default_companion_filter")]
    companion: String,
    /// Free-form operator label, surfaced in the companion's
    /// `on_recapture` callback for logging.
    #[serde(default)]
    reason: Option<String>,
}

fn default_companion_filter() -> String {
    "*".to_owned()
}

/// Response body for an accepted (dispatched) recapture. The
/// recapture itself runs detached — completion/failure is observable
/// via `GET /_admin/events` (`recapture_completed` /
/// `recapture_failed`) and the `recapture_in_progress` field on
/// `/_admin/status`.
#[derive(Debug, Serialize)]
struct RecaptureAccepted {
    module: String,
    /// Always `"started"` — the 202 contract marker.
    status: &'static str,
}

/// Per-companion `RecaptureReady` ACK budget. Matches the design
/// doc's open question 3 starting guess; revisit once we've
/// measured `on_recapture` against a populated companion table.
const RECAPTURE_COMPANION_TIMEOUT: Duration = Duration::from_secs(30);

async fn recapture_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    body: Option<Json<RecaptureRequest>>,
) -> Result<Response, HandlerError> {
    // Empty body is treated as the default request.
    let req = body.map(|Json(b)| b).unwrap_or_default();

    // 404 when the module's never been activated.
    if state.storage.read_manifest(&id)?.is_none() {
        return Err(HandlerError::ModuleNotFound(id));
    }

    let Some(host) = &state.host else {
        return Err(HandlerError::RecaptureUnavailable(
            "no host wired in this admin router".to_owned(),
        ));
    };

    // A recapture on an instance that is not supposed to be running would
    // START it — recapture's whole job is to wipe the bootstrap flags and
    // restart. That is a fourth way an operator's stop gets undone, so it is
    // a 409 instead.
    if let Some(lifecycle) = state.lifecycle.as_ref()
        && lifecycle.expects_follower(&id)? == BootAction::LeaveStopped
    {
        return Err(HandlerError::NotRunnable(id));
    }

    // `companion=<key>` — scoped resync, not the coordinated
    // module-wide rebuild. Resolve the companion's persisted
    // interests for THIS module and route `InterestOp::Resync` into
    // the follower: the module drops + re-seeds those scopes as
    // genuinely new, the Onboard pump re-walks them, and the
    // re-emitted snapshots reach every subscriber (idempotent
    // full-replace on consumers). No bootstrap-flag wipe, no
    // follower restart, no `RecaptureReady` coordination — the big
    // hammer stays `companion=*`.
    if req.companion != "*" {
        let interests: Vec<mitos_protocol::Interest> =
            crate::companions::load_companion_registrations(&state.storage, &req.companion)
                .map_err(|e| HandlerError::RecaptureUnavailable(e.to_string()))?
                .into_iter()
                .filter(|(module, _, _)| module == &id)
                .flat_map(|(_, _, r)| r.interests)
                .fold(Vec::new(), |mut acc, i| {
                    if !acc.contains(&i) {
                        acc.push(i);
                    }
                    acc
                });
        if interests.is_empty() {
            return Err(HandlerError::RecaptureCompanionUnknown(req.companion));
        }
        tracing::info!(
            module = %id,
            companion_key = %req.companion,
            scopes = interests.len(),
            reason = ?req.reason,
            "recapture: scoped resync dispatching"
        );
        state.events.record(
            id.as_str(),
            crate::events::EventKind::ResyncRouted {
                client_id: "*".to_owned(),
                companion_key: req.companion.clone(),
            },
        );
        host.route_interest(&id, mitos_protocol::InterestOp::Resync, interests)
            .await
            .map_err(|e| HandlerError::RecaptureUnavailable(e.to_string()))?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(RecaptureAccepted {
                module: id,
                status: "resync_routed",
            }),
        )
            .into_response());
    }

    // Synchronous duplicate check so the caller gets an immediate
    // 409. The authoritative guard is the mutex inside
    // `recapture_module` (which the detached task still hits) — this
    // pre-check just keeps the common repeat-POST path from spawning
    // a task destined to 409, and keeps the event ring free of
    // started/failed noise for duplicates.
    if host.recapture_in_flight().await.iter().any(|m| m == &id) {
        return Err(HandlerError::RecaptureInProgress(id));
    }

    tracing::info!(
        module = %id,
        reason = ?req.reason,
        "recapture: admin endpoint dispatching (detached)"
    );
    state.events.record(
        id.as_str(),
        crate::events::EventKind::RecaptureStarted {
            reason: req.reason.clone(),
        },
    );

    // Run the recapture DETACHED from this request. The old inline
    // `.await` tied the whole coordinate→wipe→restart pipeline to the
    // HTTP connection's lifetime; the CF tunnel kills idle responses
    // at ~100s, dropping the handler future and cancelling the
    // recapture mid-`start()` — which is stop-first, so the module
    // was left STOPPED (the 2026-06/07 holder-distribution outage).
    // A spawned task is owned by the runtime, not the connection, so
    // client/tunnel behaviour can no longer corrupt module lifecycle.
    // Completion/failure lands on the event ring exactly as before.
    let task_host = host.clone();
    let events = state.events.clone();
    let module_id = id.clone();
    let reason = req.reason.clone();
    tokio::spawn(async move {
        let started = std::time::Instant::now();
        match task_host
            .recapture_module(&module_id, reason, RECAPTURE_COMPANION_TIMEOUT)
            .await
        {
            Ok(outcome) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                tracing::info!(
                    module = %module_id,
                    companions_targeted = outcome.companion_count,
                    duration_ms,
                    "recapture: complete"
                );
                events.record(
                    module_id.as_str(),
                    crate::events::EventKind::RecaptureCompleted {
                        companions_targeted: outcome.companion_count,
                        duration_ms,
                    },
                );
            }
            // Lost the pre-check race with a concurrent POST — the
            // other flight is doing the work; nothing to record.
            Err(crate::PlatformError::RecaptureInProgress(_)) => {
                tracing::info!(
                    module = %module_id,
                    "recapture: duplicate dispatch lost the in-flight race; ignoring"
                );
            }
            Err(other) => {
                let duration_ms = started.elapsed().as_millis() as u64;
                tracing::error!(
                    module = %module_id,
                    error = %other,
                    duration_ms,
                    "recapture: failed"
                );
                events.record(
                    module_id.as_str(),
                    crate::events::EventKind::RecaptureFailed {
                        error: other.to_string(),
                        duration_ms,
                    },
                );
            }
        }
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RecaptureAccepted {
            module: id,
            status: "started",
        }),
    )
        .into_response())
}

/// Return the most recently captured trap fixture for a module.
/// Written by the host's `TrapContextLogger` on every wasm trap
/// during `init` (and, in a follow-up, `handle_event`); the
/// fixture is `mitos-run --fixture` compatible TOML so the
/// operator can reproduce the trap locally with full debug
/// symbols, no SSH or Dolos snapshot needed.
///
/// 404 when the module has never trapped (or `last-trap.toml`
/// has been manually cleared). Plain-text body so curl can
/// `>` it straight to disk.
async fn last_trap(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let path = state.storage.last_trap_path(&id)?;
    match std::fs::read_to_string(&path) {
        Ok(body) => Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "text/x-toml")],
            body,
        )
            .into_response()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((
            StatusCode::NOT_FOUND,
            "no trap fixture captured for this module",
        )
            .into_response()),
        Err(e) => Err(HandlerError::Storage(StorageError::Io(e))),
    }
}

// -----------------------------------------------------------------------------
// Blocks: read-only fetch-by-slot
// -----------------------------------------------------------------------------

/// Return the raw block CBOR for `slot`. Used to materialise
/// real-chain fixtures for `mitos-run` without the writer-lock
/// dance the offline `capture-block` tool requires. Body is
/// `application/cbor`; the bytes are exactly what the consensus
/// follower received.
///
/// `503 Service Unavailable` when the admin router was mounted
/// without a chain-data handle (artifact-only deployments).
/// `404 Not Found` when the slot isn't in the archive.
/// `GET /_admin/utxos?address=<bech32>` — the current unspent set at an
/// address, as bare output references.
///
/// Read-only, and deliberately REFS ONLY. Decoding 170k outputs into one
/// response would be a multi-hundred-megabyte body dominated by datum
/// payloads; a caller that wants the outputs resolves the refs itself, and on
/// this box `tx-index` does that from the chunk store in well under a
/// millisecond each. The split is the point: mitos knows what is UNSPENT
/// (which needs the whole chain's spend history), tx-index knows what any
/// output CONTAINS. Neither can answer the other's question.
///
/// The motivating caller is the jpg ask-book backfill, which needs the
/// residual listing set — see the mirror's `/_admin/asks/backfill`.
async fn get_utxos_by_address(
    State(state): State<AdminState>,
    axum::extract::Query(q): axum::extract::Query<UtxosQuery>,
) -> Result<Response, HandlerError> {
    let Some(chain_data) = state.chain_data.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "admin router mounted without chain-data handle; utxo query unavailable",
        )
            .into_response());
    };
    let address = q.address.trim();
    if address.is_empty() {
        return Ok((StatusCode::BAD_REQUEST, "address is required").into_response());
    }
    match chain_data.utxos_by_address(address).await {
        Ok(refs) => {
            let utxos: Vec<UtxoRefEntry> = refs
                .into_iter()
                .map(|r| UtxoRefEntry {
                    tx_hash: hex::encode(r.tx_hash),
                    index: r.index,
                })
                .collect();
            Ok(Json(UtxosResponse {
                address: address.to_owned(),
                count: utxos.len(),
                truncated: utxos.len() >= mitos_data_plane::UTXOS_BY_ADDRESS_HARD_CAP,
                utxos,
            })
            .into_response())
        }
        Err(e) => Err(HandlerError::ChainData(format!("utxos_by_address: {e}"))),
    }
}

async fn get_block_by_slot(
    State(state): State<AdminState>,
    Path(slot): Path<u64>,
) -> Result<Response, HandlerError> {
    let Some(chain_data) = state.chain_data.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "admin router mounted without chain-data handle; block fetch unavailable",
        )
            .into_response());
    };
    match chain_data.read_block(slot).await {
        Ok(Some(bytes)) => Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/cbor")],
            bytes,
        )
            .into_response()),
        Ok(None) => Ok((
            StatusCode::NOT_FOUND,
            "no block at this slot in the archive",
        )
            .into_response()),
        Err(e) => Err(HandlerError::ChainData(format!("read_block: {e}"))),
    }
}

/// Same as `get_block_by_slot` but resolves by tx hash first
/// via the archive's tx-index. The slot the tx landed in goes
/// back in the `X-Mitos-Block-Slot` response header so the
/// operator can label the saved fixture without a follow-up
/// round trip.
async fn get_block_by_tx(
    State(state): State<AdminState>,
    Path(tx_hash_hex): Path<String>,
) -> Result<Response, HandlerError> {
    let Some(chain_data) = state.chain_data.as_ref() else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            "admin router mounted without chain-data handle; block fetch unavailable",
        )
            .into_response());
    };

    let bytes =
        hex::decode(&tx_hash_hex).map_err(|_| HandlerError::BadTxHash(tx_hash_hex.clone()))?;
    if bytes.len() != 32 {
        return Err(HandlerError::BadTxHash(tx_hash_hex));
    }
    let tx_hash: pallas_primitives::Hash<32> = bytes.as_slice().into();

    let slot = match chain_data.slot_by_tx_hash(&tx_hash).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Ok((StatusCode::NOT_FOUND, "tx hash not found in archive").into_response());
        }
        Err(e) => return Err(HandlerError::ChainData(format!("slot_by_tx_hash: {e}"))),
    };

    match chain_data.read_block(slot).await {
        Ok(Some(bytes)) => Ok((
            StatusCode::OK,
            [
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/cbor".to_owned(),
                ),
                (
                    axum::http::HeaderName::from_static("x-mitos-block-slot"),
                    slot.to_string(),
                ),
            ],
            bytes,
        )
            .into_response()),
        Ok(None) => Ok((
            StatusCode::NOT_FOUND,
            format!("tx hash resolved to slot {slot} but archive has no block at that slot"),
        )
            .into_response()),
        Err(e) => Err(HandlerError::ChainData(format!("read_block: {e}"))),
    }
}

// -----------------------------------------------------------------------------
// Emissions: list / replay / purge
// -----------------------------------------------------------------------------

#[derive(Debug, Default, Deserialize)]
struct EmissionsListQuery {
    /// Filter by lifecycle status. Comma-separated; case-insensitive.
    /// Default = "queued,pending" (the actionable rows). Use
    /// "all" to skip the status filter.
    #[serde(default)]
    status: Option<String>,
    /// Filter to a specific companion. Empty / missing = all.
    #[serde(default)]
    companion_key: Option<String>,
    /// Cap the response size. Default 50.
    #[serde(default)]
    limit: Option<usize>,
    /// Skip rows with id <= this (cursor pagination).
    #[serde(default)]
    after_id: Option<u64>,
}

#[derive(Debug, Serialize)]
struct EmissionView {
    id: u64,
    matched_at: String,
    sent_at: Option<String>,
    chain_point: mitos_protocol::ChainPoint,
    channel: String,
    companion_id: String,
    status: &'static str,
    status_at: String,
    error: Option<String>,
    payload_bytes: usize,
}

impl From<&crate::emissions::EmissionRecord> for EmissionView {
    fn from(r: &crate::emissions::EmissionRecord) -> Self {
        Self {
            id: r.id,
            matched_at: r.matched_at.clone(),
            sent_at: r.sent_at.clone(),
            chain_point: r.chain_point.clone(),
            channel: r.channel.clone(),
            companion_id: r.companion_id.clone(),
            status: r.status.as_str(),
            status_at: r.status_at.clone(),
            error: r.error.clone(),
            payload_bytes: r.payload.len(),
        }
    }
}

#[derive(Debug, Serialize)]
struct EmissionsListResponse {
    rows: Vec<EmissionView>,
    /// Total rows matching the filter (before `limit`). Useful
    /// for paginators that want to know how many remain.
    total: usize,
    /// Aggregate counts by status across the filter — handy for
    /// operator triage. Lower-case keys: queued, pending, acked,
    /// nacked, timeout.
    counts: std::collections::BTreeMap<String, usize>,
}

#[derive(Debug, Serialize)]
struct EmissionsPurgeResponse {
    purged: usize,
    matched_status: String,
}

#[derive(Debug, Serialize)]
struct EmissionsReplayResponse {
    replayed_id: u64,
    previous_status: String,
    new_status: &'static str,
}

/// Parse a comma-separated status filter into a set of
/// `EmissionStatus`. `None`/empty defaults to actionable rows
/// (queued + pending). `"all"` matches everything.
fn parse_status_filter(s: Option<&str>) -> Option<Vec<crate::emissions::EmissionStatus>> {
    use crate::emissions::EmissionStatus::*;
    let s = s.unwrap_or("queued,pending").trim().to_ascii_lowercase();
    if s.is_empty() || s == "all" {
        return None;
    }
    let mut out = Vec::new();
    for tok in s.split(',') {
        match tok.trim() {
            "queued" => out.push(Queued),
            "pending" => out.push(Pending),
            "acked" => out.push(Acked),
            "nacked" => out.push(Nacked),
            "timeout" => out.push(Timeout),
            _ => {} // silently skip unknown tokens
        }
    }
    Some(out)
}

async fn list_emissions(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<EmissionsListQuery>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let store = state.storage.emissions_store(&id).map_err(|e| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
    })?;

    let status_filter = parse_status_filter(q.status.as_deref());
    let companion_filter: Option<String> = q
        .companion_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned());
    let limit = q.limit.unwrap_or(50);
    let after_id = q.after_id.unwrap_or(0);

    let all_matching = store
        .list_filtered(|r| {
            if r.id <= after_id {
                return false;
            }
            if let Some(key) = &companion_filter
                && &r.companion_id != key
            {
                return false;
            }
            if let Some(statuses) = &status_filter
                && !statuses.contains(&r.status)
            {
                return false;
            }
            true
        })
        .map_err(|e| {
            HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
        })?;

    // Aggregate-by-status across the FULL match set (before
    // limit-truncation) so operators can see "10 acked, 3
    // pending" even when only the first page is returned.
    let mut counts: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for r in &all_matching {
        *counts.entry(r.status.as_str().to_owned()).or_insert(0) += 1;
    }

    let total = all_matching.len();
    let rows: Vec<EmissionView> = all_matching
        .iter()
        .take(limit)
        .map(EmissionView::from)
        .collect();
    Ok(Json(EmissionsListResponse {
        rows,
        total,
        counts,
    })
    .into_response())
}

async fn purge_emissions(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<EmissionsListQuery>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let store = state.storage.emissions_store(&id).map_err(|e| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
    })?;

    // Refuse to operate without an explicit status filter.
    // Default ("queued,pending") is the actionable backlog —
    // accidentally purging Pending rows would orphan in-flight
    // deliveries. Operators must opt in to which statuses they
    // mean to purge.
    let raw = q
        .status
        .as_deref()
        .ok_or_else(|| {
            HandlerError::Storage(StorageError::Io(std::io::Error::other(
                "purge requires explicit ?status=<list> (e.g. status=acked or status=nacked,timeout)",
            )))
        })?
        .to_owned();
    let status_filter = parse_status_filter(Some(&raw)).ok_or_else(|| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(
            "purge requires a non-`all` status filter; reject blast-radius purges",
        )))
    })?;
    let companion_filter: Option<String> = q
        .companion_key
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned());

    let purged = store
        .purge(|r| {
            if let Some(key) = &companion_filter
                && &r.companion_id != key
            {
                return false;
            }
            status_filter.contains(&r.status)
        })
        .map_err(|e| {
            HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
        })?;

    tracing::info!(
        module = %id,
        purged,
        status_filter = %raw,
        companion = ?companion_filter,
        "emissions purged"
    );
    Ok(Json(EmissionsPurgeResponse {
        purged,
        matched_status: raw,
    })
    .into_response())
}

async fn replay_emission(
    State(state): State<AdminState>,
    Path((id, emission_id)): Path<(String, u64)>,
) -> Result<Response, HandlerError> {
    if state.storage.read_manifest(&id)?.is_none() {
        return Ok((StatusCode::NOT_FOUND, "module not registered").into_response());
    }
    let store = state.storage.emissions_store(&id).map_err(|e| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
    })?;

    let Some(record) = store.get(emission_id).map_err(|e| {
        HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
    })?
    else {
        return Ok((
            StatusCode::NOT_FOUND,
            format!("emission_id {emission_id} not found"),
        )
            .into_response());
    };

    let previous = record.status.as_str().to_owned();
    let now = format!(
        "unix:{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    store
        .update_status(
            emission_id,
            crate::emissions::EmissionStatus::Queued,
            &now,
            None,
        )
        .map_err(|e| {
            HandlerError::Storage(StorageError::Io(std::io::Error::other(e.to_string())))
        })?;

    tracing::info!(
        module = %id,
        emission_id,
        previous_status = %previous,
        "emission replayed (status flipped to Queued)"
    );
    Ok(Json(EmissionsReplayResponse {
        replayed_id: emission_id,
        previous_status: previous,
        new_status: "queued",
    })
    .into_response())
}

/// Independent wasmtime-side validation. Confirms the component
/// parses and the world declared by the manifest is what wasmtime
/// sees in the binary's metadata. Dry-instantiation + version-export
/// round-trip is future work — the actual host's `start()` path
/// catches those on first activation; the cost of doing it here too
/// hasn't earned its keep.
fn validate_with_wasmtime(wasm_bytes: &[u8]) -> Result<(), HandlerError> {
    // Build a minimal engine just for parse-validation. Cheap to
    // construct; doesn't need the platform's full Config.
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    let engine = wasmtime::Engine::new(&config)
        .map_err(|e| HandlerError::Wasmtime(format!("engine: {e}")))?;
    let _component = wasmtime::component::Component::from_binary(&engine, wasm_bytes)
        .map_err(|e| HandlerError::Wasmtime(format!("component: {e}")))?;
    Ok(())
}

/// The admin router's auth, proven through the REAL router.
///
/// The four unit tests that used to live here exercised a private
/// `authorize(full, readonly, method, provided)`. That function is gone and
/// its rule moved to `mitos_lifecycle::access`, where a decision-table test
/// already covers it — so these prove the thing the pure crate cannot: that
/// every route actually ASKS, and asks with the right attempt.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::testing::{DEV, FULL, PROD, READONLY, configured_authenticator};
    use axum::body::Body;
    use axum::http::{Method, Request as HttpRequest, StatusCode};
    use mitos_lifecycle::access::OperatorTokenAsClient;
    use tower::ServiceExt;

    /// Every mutating admin route, by method and path. Iterated rather than
    /// hand-picked: a route added without a thought about who may reach it
    /// should fail here, not in production.
    const MUTATING_ROUTES: &[(Method, &str)] = &[
        (Method::POST, "/_admin/modules/m"),
        (Method::DELETE, "/_admin/modules/m"),
        (Method::POST, "/_admin/modules/m/restart"),
        (Method::POST, "/_admin/modules/m/start"),
        (Method::POST, "/_admin/modules/m/pin"),
        (Method::POST, "/_admin/modules/m/unpin"),
        (Method::POST, "/_admin/modules/m/evict"),
        (Method::DELETE, "/_admin/modules/m/companions/c/k"),
        (Method::DELETE, "/_admin/modules/m/emissions"),
        (Method::POST, "/_admin/modules/m/emissions/1/replay"),
    ];

    /// Every `GET` on the admin router. Together with `MUTATING_ROUTES` and
    /// the recapture sub-router this is the router's whole surface — if you
    /// add a route, add it here, because "who may reach it" is not something
    /// to find out in production.
    const READ_ROUTES: &[&str] = &[
        "/_admin",
        "/_admin/status",
        "/_admin/events",
        "/_admin/modules",
        "/_admin/modules/m",
        "/_admin/modules/m/companions",
        "/_admin/modules/m/last-trap",
        "/_admin/modules/m/emissions",
        "/_admin/utxos",
        "/_admin/blocks/1",
        "/_admin/blocks/by-tx/aa",
    ];

    /// The tripwire for the two lists above. axum exposes no route table, so
    /// the lists are hand-maintained — and a route added to the router but not
    /// to a list is silently absent from every "who may reach it" test. This
    /// reads `admin_router_inner`'s own SOURCE and fails when the router has a
    /// route, or a handler method, that the lists do not account for.
    ///
    /// It is a tripwire, not a parser: it counts `get(` / `post(` /
    /// `delete(` calls and route-path literals in that one function. If it
    /// fails after you touched the router, the fix is almost always to add the
    /// new route to `MUTATING_ROUTES` or `READ_ROUTES`.
    #[test]
    fn the_route_lists_cover_the_whole_router() {
        let source = include_str!("admin.rs");
        let start = source
            .find("fn admin_router_inner(")
            .expect("admin_router_inner exists");
        let body = &source[start..];
        let end = body.find("\n}\n").expect("function has an end");
        let code: String = body[..end]
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");

        // Every path template in the router, made concrete the way the lists
        // spell it.
        let mut templates = Vec::new();
        let mut rest = code.as_str();
        while let Some(at) = rest.find("\"/") {
            let tail = &rest[at + 1..];
            let close = tail.find('"').expect("string literal closes");
            templates.push(&tail[..close]);
            rest = &tail[close + 1..];
        }
        let concrete = |template: &str| {
            template
                .replace("{id}", "m")
                .replace("{client_id}", "c")
                .replace("{companion_key}", "k")
                .replace("{emission_id}", "1")
                .replace("{slot}", "1")
                .replace("{tx_hash}", "aa")
        };
        // The two routes deliberately outside both lists, each with its own
        // test: recapture (`RecaptureModule`) and the open `/metrics`.
        let elsewhere = ["/_admin/modules/m/recapture", "/metrics"];
        for template in &templates {
            let path = concrete(template);
            assert!(
                !path.contains('{'),
                "route `{template}` has a path parameter this test cannot make concrete — teach \
                 `concrete` about it"
            );
            let listed = READ_ROUTES.contains(&path.as_str())
                || MUTATING_ROUTES.iter().any(|(_, p)| *p == path)
                || elsewhere.contains(&path.as_str());
            assert!(
                listed,
                "`{template}` is on the admin router but in neither route list, so no test \
                 checks who may reach it"
            );
        }

        // …and every METHOD, because a `.post(..)` added to an existing path
        // adds no new template.
        let count = |needle: &str| code.matches(needle).count();
        assert_eq!(
            count("get("),
            READ_ROUTES.len() + 1,
            "GET handlers on the router vs READ_ROUTES (+1 for the open /metrics)"
        );
        assert_eq!(
            count("post(") + count("delete("),
            MUTATING_ROUTES.len() + 1,
            "POST/DELETE handlers on the router vs MUTATING_ROUTES (+1 for recapture)"
        );
    }

    fn tempdir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("mitos-admin-auth-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn router_at(dir: &std::path::Path, mode: OperatorTokenAsClient) -> axum::Router {
        admin_router(
            ModuleStorage::new(dir),
            Arc::new(configured_authenticator(mode)),
        )
    }

    async fn call(
        router: &axum::Router,
        method: Method,
        path: &str,
        token: Option<&str>,
    ) -> StatusCode {
        let mut req = HttpRequest::builder().method(method).uri(path);
        if let Some(token) = token {
            req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        router
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    /// A router with a FAKE host and a real lifecycle over `dir`, plus the
    /// pieces a companion router needs to share the same manager.
    struct Wired {
        router: axum::Router,
        storage: ModuleStorage,
        host: crate::host_v2::testing::FakeHost,
        lifecycle: Arc<crate::lifecycle::LifecycleManager>,
        events: crate::events::EventRing,
        auth: Arc<Authenticator>,
    }

    fn wired(dir: &std::path::Path) -> Wired {
        let storage = ModuleStorage::new(dir);
        let host = crate::host_v2::testing::FakeHost::new();
        let handle: Arc<dyn crate::host_v2::ModuleHostHandle> = Arc::new(host.clone());
        let auth = Arc::new(configured_authenticator(
            OperatorTokenAsClient::AllowWithWarning,
        ));
        let events = crate::events::EventRing::new();
        let lifecycle = Arc::new(crate::lifecycle::LifecycleManager::new(
            storage.clone(),
            handle.clone(),
            auth.clone(),
            events.clone(),
            crate::lifecycle::system_clock(),
        ));
        let router = admin_router_with_host(
            storage.clone(),
            handle,
            Vec::new(),
            None,
            SystemTime::now(),
            events.clone(),
            auth.clone(),
            lifecycle.clone(),
        );
        Wired {
            router,
            storage,
            host,
            lifecycle,
            events,
            auth,
        }
    }

    /// `POST …/evict` with the lifecycle wired: the safety check is now the
    /// attached set, not a directory walk.
    #[tokio::test]
    async fn evict_refuses_while_attached_and_force_overrides() {
        let dir = tempdir("evict-attached");
        let w = wired(&dir);
        let wasm = b"evict test wasm";
        w.storage
            .activate(&crate::companions::testing::manifest_for("m", wasm), wasm)
            .unwrap();
        crate::lifecycle::testing::write_registration(
            &w.storage,
            "m",
            crate::auth::testing::PROD_CLIENT,
            "policy_a",
        );
        w.lifecycle.boot().await;

        assert_eq!(
            call(
                &w.router,
                Method::POST,
                "/_admin/modules/m/evict",
                Some(FULL)
            )
            .await,
            StatusCode::CONFLICT,
        );
        assert!(!w.host.evicted("m"));

        assert_eq!(
            call(
                &w.router,
                Method::POST,
                "/_admin/modules/m/evict?force=true",
                Some(FULL)
            )
            .await,
            StatusCode::OK,
        );
        assert!(w.host.evicted("m"));

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Recapture wipes the bootstrap flags and RESTARTS the module, so on a
    /// stopped instance it would be a fourth way an operator's stop gets
    /// undone.
    #[tokio::test]
    async fn recapture_on_a_stopped_module_is_409() {
        let dir = tempdir("recapture-stopped");
        let w = wired(&dir);
        let wasm = b"recapture stopped wasm";
        w.storage
            .activate(&crate::companions::testing::manifest_for("m", wasm), wasm)
            .unwrap();
        w.lifecycle.boot().await;

        // Running: it gets past the lifecycle guard.
        assert_ne!(
            call(
                &w.router,
                Method::POST,
                "/_admin/modules/m/recapture",
                Some(FULL)
            )
            .await,
            StatusCode::CONFLICT,
        );

        assert_eq!(
            call(&w.router, Method::DELETE, "/_admin/modules/m", Some(FULL)).await,
            StatusCode::NO_CONTENT,
        );
        assert_eq!(
            call(
                &w.router,
                Method::POST,
                "/_admin/modules/m/recapture",
                Some(FULL)
            )
            .await,
            StatusCode::CONFLICT,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Subscribe → attached; the last companion record deleted → detached.
    /// Through the REAL routers, sharing one manager, because the hooks that
    /// call `reconcile` live in the handlers.
    #[tokio::test]
    async fn a_subscribe_attaches_and_the_last_delete_detaches() {
        use crate::auth::testing::{PROD, PROD_CLIENT};

        let dir = tempdir("attach-detach");
        let w = wired(&dir);
        let wasm = b"attach detach wasm";
        w.storage
            .activate(&crate::companions::testing::manifest_for("m", wasm), wasm)
            .unwrap();
        w.lifecycle.boot().await;
        assert!(
            w.lifecycle
                .instance("m")
                .unwrap()
                .unwrap()
                .attached
                .is_empty(),
            "nothing is attached before anyone subscribes",
        );

        let companions = crate::companions::companion_router(
            w.storage.clone(),
            w.auth.clone(),
            None,
            None,
            w.events.clone(),
            Some(w.lifecycle.clone()),
        );
        let request = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module { name: "m".into() }],
            companion_key: "policy_a".into(),
            client_id: PROD_CLIENT.into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
        };
        let mut body = Vec::new();
        ciborium::ser::into_writer(&request, &mut body).unwrap();
        let response = companions
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .header(header::AUTHORIZATION, format!("Bearer {PROD}"))
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let attached =
            crate::lifecycle::attached_labels(&w.lifecycle.instance("m").unwrap().unwrap());
        assert_eq!(attached, vec!["prod/ownership.cnft.dev"]);

        // …and the admin delete of that client's last record detaches it.
        assert_eq!(
            call(
                &w.router,
                Method::DELETE,
                &format!("/_admin/modules/m/companions/{PROD_CLIENT}/policy_a"),
                Some(FULL),
            )
            .await,
            StatusCode::NO_CONTENT,
        );
        assert!(
            w.lifecycle
                .instance("m")
                .unwrap()
                .unwrap()
                .attached
                .is_empty(),
            "deleting the last companion record did not detach the client",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_namespace_token_gets_403_on_every_admin_route() {
        let dir = tempdir("namespace-403");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);

        for token in [PROD, DEV] {
            for path in READ_ROUTES {
                assert_eq!(
                    call(&router, Method::GET, path, Some(token)).await,
                    StatusCode::FORBIDDEN,
                    "GET {path} with a namespace token",
                );
            }
            for (method, path) in MUTATING_ROUTES {
                assert_eq!(
                    call(&router, method.clone(), path, Some(token)).await,
                    StatusCode::FORBIDDEN,
                    "{method} {path} with a namespace token — even a PROD worker's token \
                     must not be able to evict a module",
                );
            }
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The `readonly_token_authorizes_only_get` rule, now proven where the
    /// method-to-access mapping actually lives.
    #[tokio::test]
    async fn the_readonly_token_reads_and_does_not_write() {
        let dir = tempdir("readonly");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);

        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some(READONLY)).await,
            StatusCode::OK,
        );
        for (method, path) in MUTATING_ROUTES {
            assert_eq!(
                call(&router, method.clone(), path, Some(READONLY)).await,
                StatusCode::FORBIDDEN,
                "{method} {path} with the read-only token",
            );
        }
        // …and the full token still reaches both.
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some(FULL)).await,
            StatusCode::OK,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_unknown_token_is_401_and_a_wrong_principal_is_403() {
        let dir = tempdir("401-vs-403");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);

        // 401 — "who are you".
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", None).await,
            StatusCode::UNAUTHORIZED,
        );
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some("not-a-token")).await,
            StatusCode::UNAUTHORIZED,
        );
        // 403 — "not you". A stale token and the wrong namespace's token are
        // different incidents; the status code is how they are told apart.
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some(PROD)).await,
            StatusCode::FORBIDDEN,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn metrics_stays_open() {
        let dir = tempdir("metrics-open");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);
        assert_eq!(
            call(&router, Method::GET, "/metrics", None).await,
            StatusCode::OK,
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `holder-map`'s production cron recaptures `holder-distribution` with
    /// the secret it subscribes with. Under `Admin(Mutate)` — which every
    /// namespace token is refused — that cron would 403 the day its secret is
    /// rotated, which is the whole reason recapture has its own attempt.
    #[tokio::test]
    async fn a_prod_token_may_recapture_and_a_dev_token_may_not() {
        let dir = tempdir("recapture-attempt");
        let storage = ModuleStorage::new(&dir);
        let wasm = b"recapture-attempt wasm";
        let manifest = crate::companions::testing::manifest_for("m", wasm);
        storage.activate(&manifest, wasm).unwrap();
        let router = admin_router(
            storage,
            Arc::new(configured_authenticator(
                OperatorTokenAsClient::AllowWithWarning,
            )),
        );

        // A prod namespace token gets PAST authorization and into the
        // handler, which then fails on "no host wired in this admin router".
        // That is `holder-map`'s production cron continuing to work after its
        // secret becomes a namespace token — and because this router has
        // `host: None`, the recapture provably never ran.
        let reached = call(
            &router,
            Method::POST,
            "/_admin/modules/m/recapture",
            Some(PROD),
        )
        .await;
        assert_ne!(
            reached,
            StatusCode::FORBIDDEN,
            "a prod namespace token must reach recapture",
        );
        assert_ne!(reached, StatusCode::NOT_FOUND, "the module is registered");

        // A dev token may NOT: recapture is whole-module today, so it would
        // make production's companions drop their state. Refused BEFORE the
        // handler, so nothing was invoked.
        assert_eq!(
            call(
                &router,
                Method::POST,
                "/_admin/modules/m/recapture",
                Some(DEV)
            )
            .await,
            StatusCode::FORBIDDEN,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The route must go through `RecaptureModule`, not `Admin(Mutate)` — the
    /// same prod token that reaches recapture is still refused on evict.
    #[tokio::test]
    async fn recapture_is_not_reachable_as_admin_mutate() {
        let dir = tempdir("recapture-vs-evict");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);
        assert_eq!(
            call(&router, Method::POST, "/_admin/modules/m/evict", Some(PROD)).await,
            StatusCode::FORBIDDEN,
        );
        assert_ne!(
            call(
                &router,
                Method::POST,
                "/_admin/modules/m/recapture",
                Some(PROD)
            )
            .await,
            StatusCode::FORBIDDEN,
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn status_never_leaks_a_token() {
        let dir = tempdir("status-leak");
        let router = router_at(&dir, OperatorTokenAsClient::AllowWithWarning);

        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method(Method::GET)
                    .uri("/_admin/status")
                    .header(header::AUTHORIZATION, format!("Bearer {FULL}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        for secret in [FULL, READONLY, PROD, DEV] {
            assert!(
                !body.contains(secret),
                "status response contains a configured token",
            );
        }
        // The env var NAMES are fine; their values are not. Neither appears
        // in the response at all today, and neither should.
        assert!(!body.contains("MITOS_TOKEN_PROD"));

        let status: StatusResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(status.auth_mode, AuthMode::Namespaces);
        assert_eq!(status.namespaces.len(), 2);
        assert!(
            status.namespaces.iter().any(|n| n.tier == Tier::Production),
            "the prod namespace's tier must be reported",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn unowned_registrations_are_reported_and_left_alone() {
        let dir = tempdir("unowned");
        let storage = ModuleStorage::new(&dir);

        // A registration for a client no namespace lists, beside one for a
        // client that is listed.
        let wasm = b"unowned-test wasm";
        let manifest = crate::companions::testing::manifest_for("mod-a", wasm);
        storage.activate(&manifest, wasm).unwrap();
        for client in ["ownership.cnft.dev", "preview-1234.cnft.dev"] {
            let client_dir = storage.module_dir_for_companions("mod-a").join(client);
            std::fs::create_dir_all(&client_dir).unwrap();
            std::fs::write(client_dir.join("customer_42.cbor"), b"sub").unwrap();
        }
        let before = std::fs::read(
            storage
                .module_dir_for_companions("mod-a")
                .join("preview-1234.cnft.dev")
                .join("customer_42.cbor"),
        )
        .unwrap();

        let router = admin_router(
            storage.clone(),
            Arc::new(configured_authenticator(
                OperatorTokenAsClient::AllowWithWarning,
            )),
        );
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .uri("/_admin/status")
                    .header(header::AUTHORIZATION, format!("Bearer {FULL}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: StatusResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(status.unowned_clients, vec!["preview-1234.cnft.dev"]);
        // Reported, NOT removed. This order takes nothing away.
        assert_eq!(
            std::fs::read(
                storage
                    .module_dir_for_companions("mod-a")
                    .join("preview-1234.cnft.dev")
                    .join("customer_42.cbor")
            )
            .unwrap(),
            before,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn legacy_mode_is_unchanged_v2() {
        let dir = tempdir("legacy");
        let router = admin_router(
            ModuleStorage::new(&dir),
            Arc::new(crate::auth::Authenticator::legacy(
                Some(FULL.to_owned()),
                Some(READONLY.to_owned()),
            )),
        );

        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some(FULL)).await,
            StatusCode::OK,
        );
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", Some(READONLY)).await,
            StatusCode::OK,
        );
        assert_eq!(
            call(
                &router,
                Method::POST,
                "/_admin/modules/m/evict",
                Some(READONLY)
            )
            .await,
            StatusCode::FORBIDDEN,
        );
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", None).await,
            StatusCode::UNAUTHORIZED,
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn open_mode_allows_an_unauthenticated_request() {
        let dir = tempdir("open-mode");
        let router = admin_router(
            ModuleStorage::new(&dir),
            Arc::new(crate::auth::Authenticator::legacy(None, None)),
        );
        assert_eq!(
            call(&router, Method::GET, "/_admin/status", None).await,
            StatusCode::OK,
            "a host with no MITOS_AUTH_TOKEN runs open, exactly as v2 did",
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
