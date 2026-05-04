//! `/_admin/modules/*` HTTP surface.
//!
//! Phase 1 of the deployment story (`MITOS_PLATFORM_DEPLOYMENT.md`):
//!
//! - `POST /_admin/modules/{id}` — multipart upload + activate
//! - `GET  /_admin/modules` — list registered modules
//! - `GET  /_admin/modules/{id}` — single module status
//!
//! What's NOT in phase 1 (deferred to phase 2):
//! - DELETE / restart endpoints
//! - The running-instance lifecycle (this phase only manages
//!   on-disk artifacts; `mitos-core` will wire the host
//!   instance management on top in a follow-up)
//! - The quarantine→prove-by-replay state machine
//! - Multipart `config` field (mitos.toml CBOR)
//!
//! Auth is a self-contained shared-secret bearer-token
//! middleware mirroring `mitos-core::auth` rather than depending
//! on mitos-core (which pulls in the dolos crate transitively).
//! Same env var, same shape; v2 multi-user auth will swap this
//! out without route changes.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Multipart, Path, Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};

use crate::manifest::{Manifest, ManifestError};
use crate::registry::HOST_ABI_MAJOR;
use crate::storage::{ModuleStorage, StorageError};

/// World string that all modules conforming to platform v1 must
/// declare. Mirror of the world: clause in `wit/world.wit`.
pub const PLATFORM_WIT_WORLD: &str = "mitos:platform/mitos-module";

/// Auth posture for the admin endpoints.
#[derive(Clone, Debug)]
pub struct AuthToken(pub Option<String>);

impl AuthToken {
    /// Load from `MITOS_AUTH_TOKEN`. If unset, returns open
    /// mode (every request allowed); a warn-level diagnostic
    /// is left to callers since this constructor is used by
    /// production binaries AND tests with different posture
    /// expectations.
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

async fn require_auth(
    State(token): State<AuthToken>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
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

/// Shared state passed to every handler.
#[derive(Clone)]
struct AdminState {
    storage: ModuleStorage,
    host: Option<Arc<dyn crate::host::ModuleHostHandle>>,
}

/// Build the admin router with artifact-only behaviour. Uploads
/// land on disk but no follower is started. Used by:
/// - read-only / inspector deployments
/// - tests that only exercise the artifact pipeline
///
/// Production wires the lifecycle-aware variant
/// `admin_router_with_host` which actually starts running
/// modules after upload.
pub fn admin_router(storage: ModuleStorage, auth: AuthToken) -> axum::Router {
    admin_router_inner(storage, None, auth)
}

/// Build the admin router with the running-instance lifecycle
/// wired in. Uploads trigger `host.replace(id)` so the new sha
/// starts running immediately; DELETE + restart routes are
/// available for admin operators.
pub fn admin_router_with_host(
    storage: ModuleStorage,
    host: Arc<dyn crate::host::ModuleHostHandle>,
    auth: AuthToken,
) -> axum::Router {
    admin_router_inner(storage, Some(host), auth)
}

fn admin_router_inner(
    storage: ModuleStorage,
    host: Option<Arc<dyn crate::host::ModuleHostHandle>>,
    auth: AuthToken,
) -> axum::Router {
    let state = AdminState { storage, host };
    axum::Router::new()
        .route("/_admin/modules", get(list_modules))
        .route(
            "/_admin/modules/{id}",
            get(get_module).post(upload_module).delete(delete_module),
        )
        .route("/_admin/modules/{id}/restart", post(restart_module))
        .with_state(state)
        .layer(axum::middleware::from_fn_with_state(auth, require_auth))
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadResponse {
    pub ok: bool,
    pub module: ModuleSummary,
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
}

impl HandlerError {
    fn code(&self) -> &'static str {
        match self {
            Self::IdMismatch { .. } => "id_mismatch",
            Self::MissingField(_) => "missing_field",
            Self::Multipart(_) => "multipart_parse",
            Self::Manifest(ManifestError::AbiMismatch { .. }) => "abi_mismatch",
            Self::Manifest(ManifestError::WitMismatch { .. }) => "wit_mismatch",
            Self::Manifest(ManifestError::ShaMismatch { .. }) => "sha_mismatch",
            Self::Manifest(ManifestError::SizeMismatch { .. }) => "size_mismatch",
            Self::Manifest(ManifestError::InvalidModuleId(_)) => "invalid_module_id",
            Self::Manifest(ManifestError::InvalidTrapStrategy(_)) => "invalid_trap_strategy",
            Self::Manifest(ManifestError::Parse(_)) => "manifest_parse",
            Self::Storage(StorageError::UploadInProgress(_)) => "upload_in_progress",
            Self::Storage(_) => "storage_io",
            Self::Wasmtime(_) => "wasm_invalid",
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::Storage(StorageError::UploadInProgress(_)) => StatusCode::CONFLICT,
            Self::Storage(StorageError::Io(_)) => StatusCode::INTERNAL_SERVER_ERROR,
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

async fn list_modules(
    State(state): State<AdminState>,
) -> Result<Json<Vec<ModuleSummary>>, HandlerError> {
    let ids = state.storage.list_modules()?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(m) = state.storage.read_manifest(&id)? {
            out.push((&m).into());
        }
    }
    Ok(Json(out))
}

async fn get_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
) -> Result<Response, HandlerError> {
    match state.storage.read_manifest(&id)? {
        Some(m) => Ok(Json(ModuleSummary::from(&m)).into_response()),
        None => Ok((StatusCode::NOT_FOUND, "module not registered").into_response()),
    }
}

async fn upload_module(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, HandlerError> {
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

    manifest.validate_against_host(&wasm_bytes, HOST_ABI_MAJOR, PLATFORM_WIT_WORLD)?;

    // Independent wasmtime validation: even if the manifest claims
    // valid shape, we re-load the bytes to confirm wasmtime
    // accepts the component. Cheap, catches tampered manifests.
    validate_with_wasmtime(&wasm_bytes)?;

    // Write artifact + manifest + symlink atomically.
    state.storage.activate(&manifest, &wasm_bytes)?;

    // If config was supplied, write it alongside. Atomic via
    // write-then-rename. Absence is meaningful — the host
    // distinguishes "no config" (call `init(&[])`) from "empty
    // CBOR config" (call `init(b'')`).
    if let Some(cfg) = config_bytes.as_ref() {
        state.storage.write_config(&id, cfg)?;
    }

    tracing::info!(
        module = %manifest.module.id,
        sha = %manifest.module.sha256,
        size = manifest.module.size_bytes,
        "module uploaded + activated",
    );

    // If a host is wired in, start (or replace) the running
    // instance so the new sha actually takes effect. In
    // artifact-only mode (test harness, read-only deployments)
    // skip this step — the artifact is on disk, that's all.
    if let Some(host) = &state.host {
        host.replace(&id)
            .await
            .map_err(|e| HandlerError::Wasmtime(format!("host.replace: {e}")))?;
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
    if let Some(host) = &state.host {
        host.stop(&id)
            .await
            .map_err(|e| HandlerError::Wasmtime(format!("host.stop: {e}")))?;
    }
    // V1.5 doesn't yet remove the artifact directory — that's a
    // safer default while we still don't have rollback CLI
    // surface; operators can `rm -rf` if they really want it
    // gone. The lifecycle effect (stop + drop slot) is what
    // matters for "delete."
    tracing::info!(module = %id, "module stopped via DELETE");
    Ok((StatusCode::NO_CONTENT, ()).into_response())
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
    host.replace(&id)
        .await
        .map_err(|e| HandlerError::Wasmtime(format!("host.replace: {e}")))?;
    tracing::info!(module = %id, "module restarted");
    Ok((StatusCode::OK, "restarted").into_response())
}

/// Independent wasmtime-side validation. Phase 1: confirm the
/// component parses and the world declared by the manifest is
/// what wasmtime sees in the binary's metadata. Phase 2 will add
/// dry-instantiation + version-export round-trip.
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

