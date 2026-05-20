//! Companion dial loop (HTTP delivery).
//!
//! For each persisted companion (one CBOR file per
//! `<storage>/<module>/companions/<key>.cbor`), the dialer
//! maintains a per-companion async task that drains the
//! per-module `EmissionsStore` via HTTP POST against the
//! companion's Worker URL.
//!
//! ## Per-task lifecycle
//!
//! 1. **Pending requeue** — on task start, flip any rows left
//!    `Pending` from a prior host process back to `Queued`.
//!    Catches the host-crash-mid-request case (analog of the
//!    legacy reconnect-time requeue).
//! 2. **Pump loop**:
//!    - `outbound_rx` carries control frames (currently just
//!      `Recapture`). When one arrives, POST to the companion's
//!      `/_internal/recapture-<channel>` endpoint and signal the
//!      `pending_recaptures` oneshot on 2xx.
//!    - Poll tick (every `POLL_INTERVAL`) drains the
//!      `EmissionsStore` for new `Queued` rows. Each row → one
//!      HTTP POST to `/_internal/apply-<channel>`. Response status
//!      maps to:
//!      - `2xx` → `Acked`
//!      - `422` → `Nacked` (apply errored; won't help to retry)
//!      - `5xx` / network error → leave `Queued`, back off + retry
//! 3. **Backoff** — transport errors trigger exponential backoff
//!    up to `MAX_BACKOFF`. Per-task `CancellationToken` cuts the
//!    loop and returns.
//!
//! ## Recapture flow
//!
//! `recapture_module` (called by the admin endpoint) pushes a
//! `ServerMessage::Recapture` frame onto each subscribed
//! companion's `outbound_tx`. The per-task loop receives it,
//! POSTs to the companion, signals the oneshot on 2xx. The
//! admin endpoint awaits each oneshot with `per_companion_timeout`.
//!
//! Pause semantics: while the per-task loop is processing a
//! Recapture POST, it's not draining Apply rows. The companion's
//! `on_recapture` finishes wiping its table before the loop
//! resumes draining (which will then deliver the refill events).
//!
//! ## Auth
//!
//! Each request carries the module-level `MITOS_AUTH_TOKEN` as a
//! Bearer header by default. Per-companion overrides via
//! `DialBackOverride.auth_header` / `auth_value` are supported
//! but rare (multi-tenant SaaS only).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use mitos_protocol::{
    HTTP_DELIVERY_MIME, RecaptureBody, ServerMessage, SubscribeRequest, SubscribeTarget,
    encode_recapture,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::admin::AuthToken;
use crate::host_v2::InterestRouter;
use crate::indexer_bridge::IndexerBridgeHandle;
use crate::storage::ModuleStorage;

mod pool;
pub use pool::LaneConfig;

const POLL_INTERVAL: Duration = Duration::from_millis(1_000);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Per-request HTTP timeout. Apply and recapture POSTs share
/// this — both run synchronously on the worker side and should
/// complete within seconds. 30s is comfortably above any sane
/// `apply_event` / `on_recapture` body.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Identifier for one `(module, client_id, companion_key)` tuple.
/// Hashable + cloneable for use as map key. `client_id`
/// disambiguates two consumers that share the same
/// `companion_key` — see
/// `docs/design/MULTI_CLIENT_COMPANIONS.md`.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CompanionId {
    pub module_id: String,
    pub client_id: String,
    pub companion_key: String,
}

impl CompanionId {
    pub fn new(
        module_id: impl Into<String>,
        client_id: impl Into<String>,
        companion_key: impl Into<String>,
    ) -> Self {
        Self {
            module_id: module_id.into(),
            client_id: client_id.into(),
            companion_key: companion_key.into(),
        }
    }
}

/// Successful outcome of `CompanionDialer::recapture_module` —
/// every targeted companion ACKed (2xx) within timeout.
/// See `docs/design/RECAPTURE.md`.
#[derive(Debug, Clone)]
pub struct RecaptureSummary {
    pub module: String,
    /// Companion keys that ACKed, in iteration order. Same as the
    /// set of subscribers at recapture start (no partial success
    /// returns this variant).
    pub ready_companions: Vec<String>,
}

/// Failure modes for `CompanionDialer::recapture_module`.
#[derive(Debug, thiserror::Error)]
pub enum RecaptureError {
    /// No companions subscribed to this module — the recapture
    /// has nothing to coordinate. The admin endpoint returns 404
    /// for this case (distinguishable from "module exists but no
    /// subscribers" which the host's outer wrapper handles).
    #[error("no companions subscribed to module `{0}`")]
    NoSubscribers(String),

    /// One or more companions failed to ACK within the
    /// per-companion timeout. The bootstrap-refill MUST NOT
    /// proceed — running it with some companions still
    /// mid-cleanup would seed ghost rows.
    #[error(
        "recapture for `{module}` timed out: {} ready, {} timed out",
        ready_companions.len(),
        timed_out_companions.len(),
    )]
    Timeout {
        module: String,
        ready_companions: Vec<String>,
        timed_out_companions: Vec<String>,
    },

    /// Pushing the Recapture frame into the companion's outbound
    /// channel failed — the dial task is gone. The recapture is
    /// aborted; operator should retry once the companion has been
    /// re-registered.
    #[error("send Recapture to {companion:?}: {detail}")]
    FrameSend {
        companion: CompanionId,
        detail: String,
    },
}

/// Per-companion dial supervisor entry. Owns the task handle,
/// cancellation token, and an unbounded mpsc sender the recapture
/// driver uses to push control frames (`Recapture`) into the
/// running task. Unbounded so the driver never has to await
/// capacity; volume is O(1) per recapture.
struct ActiveCompanion {
    cancel: CancellationToken,
    task: JoinHandle<()>,
    outbound_tx: mpsc::UnboundedSender<ServerMessage>,
}

/// Dial supervisor — one instance per running mitos host.
/// Spawns/cancels per-companion tasks as the registry
/// (the on-disk `companions/*.cbor` set) changes.
#[derive(Clone)]
pub struct CompanionDialer {
    storage: ModuleStorage,
    auth: AuthToken,
    /// Routes companion-initiated interest updates into the
    /// matching module's follower task. Kept on the supervisor
    /// for use by future inbound paths (e.g. dynamic-interest
    /// HTTP endpoint on mitos). Not consulted by the per-task
    /// HTTP drain loop today — interest changes flow via the
    /// companion's re-subscribe path.
    #[allow(dead_code)]
    interest_router: Option<Arc<dyn InterestRouter>>,
    /// In-tree indexer bridge — handles `SubscribeTarget::Indexer`
    /// dispatch. Provided by `mitos-core::Bundle` at runtime when
    /// the host has in-tree indexers. `None` means the dialer
    /// only handles `Module` targets.
    indexer_bridge: Option<IndexerBridgeHandle>,
    tasks: Arc<Mutex<HashMap<CompanionId, ActiveCompanion>>>,
    /// One-shot senders the recapture driver uses to await
    /// "ready" from each targeted companion. Registered before
    /// the Recapture frame is pushed; fired by the per-task loop
    /// when its recapture POST returns 2xx. See
    /// `docs/design/RECAPTURE.md`.
    pending_recaptures: Arc<Mutex<HashMap<CompanionId, oneshot::Sender<()>>>>,
}

impl CompanionDialer {
    pub fn new(
        storage: ModuleStorage,
        auth: AuthToken,
        interest_router: Option<Arc<dyn InterestRouter>>,
    ) -> Self {
        Self {
            storage,
            auth,
            interest_router,
            indexer_bridge: None,
            tasks: Arc::new(Mutex::new(HashMap::new())),
            pending_recaptures: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Attach an in-tree indexer bridge. The dialer will dispatch
    /// `SubscribeTarget::Indexer` requests through the bridge's
    /// `spawn_dial` method.
    pub fn with_indexer_bridge(mut self, bridge: IndexerBridgeHandle) -> Self {
        self.indexer_bridge = Some(bridge);
        self
    }

    /// Forward an interest mutation to the running follower's
    /// live filter. Called from the
    /// `POST /api/companions/<key>/interest` handler — the
    /// persistence side (rewriting the CBOR registration) is the
    /// caller's responsibility; this method only propagates the
    /// delta to the in-memory follower so the filter takes effect
    /// without a subscribe round-trip.
    ///
    /// `Ok(false)` indicates no `InterestRouter` is wired (test /
    /// dev build). Production callers should treat that as a soft
    /// success — the persisted CBOR is still correct, and the
    /// filter will resolve on next host restart.
    pub async fn route_interest_mutation(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> std::result::Result<bool, crate::host_v2::InterestRouteError> {
        let Some(router) = &self.interest_router else {
            return Ok(false);
        };
        router.route_interest(module_id, op, items).await?;
        Ok(true)
    }

    /// Scan `<storage_root>/*/companions/*.cbor` and start a
    /// drain task for each persisted companion. Failures on
    /// individual companion files are logged but don't abort
    /// the scan.
    pub async fn start_all(&self) {
        let modules = match self.storage.list_modules() {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "list_modules failed; no companion drain tasks started");
                return;
            }
        };
        for module_id in modules {
            // One-time migration of pre-fix flat companion files
            // into the two-level `<client_id>/<companion_key>.cbor`
            // layout. Idempotent — no-op once migrated. See
            // `docs/design/MULTI_CLIENT_COMPANIONS.md`.
            if let Err(e) =
                crate::companions::migrate_flat_companions_for_module(&self.storage, &module_id)
            {
                warn!(module = %module_id, error = %e, "companion layout migration failed; continuing");
            }

            let dir = self.storage.module_dir_for_companions(&module_id);
            if !dir.exists() {
                continue;
            }
            let read = match std::fs::read_dir(&dir) {
                Ok(r) => r,
                Err(e) => {
                    warn!(module = %module_id, dir = %dir.display(), error = %e, "read companions dir failed");
                    continue;
                }
            };
            // Two-level walk: each entry under `<module>/companions/`
            // is a `<client_id>/` subdirectory; each file under that
            // is a `<companion_key>.cbor`. Skip the reserved
            // `.unreachable/` quarantine dir + any leftover flat
            // `.cbor` files that survived migration.
            for entry in read.flatten() {
                let path = entry.path();
                let file_type = match entry.file_type() {
                    Ok(t) => t,
                    Err(e) => {
                        warn!(path = %path.display(), error = %e, "file type probe failed");
                        continue;
                    }
                };
                if !file_type.is_dir() {
                    continue;
                }
                let dir_name = entry.file_name();
                let dir_name_str = dir_name.to_string_lossy();
                if dir_name_str.starts_with('.') {
                    continue;
                }
                let client_files = match std::fs::read_dir(&path) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(client_dir = %path.display(), error = %e, "read client-dir failed");
                        continue;
                    }
                };
                for client_entry in client_files.flatten() {
                    let cpath = client_entry.path();
                    if cpath.extension().and_then(|s| s.to_str()) != Some("cbor") {
                        continue;
                    }
                    match load_companion(&cpath) {
                        Ok(req) => self.spawn(req).await,
                        Err(e) => {
                            warn!(path = %cpath.display(), error = %e, "load companion failed")
                        }
                    }
                }
            }
        }
    }

    /// Register (or re-register) a companion. Called from the
    /// `/api/companions/subscribe` handler after persistence +
    /// validation. Spawns one drain task per target in the
    /// request; multi-target subscribe results in N independent
    /// drain loops. Cancels any existing drain task for each
    /// `(target_name, companion_key)` pair before spawning fresh
    /// ones — register() is idempotent re-registration.
    pub async fn register(&self, req: SubscribeRequest) {
        {
            let mut tasks = self.tasks.lock().await;
            for target in &req.targets {
                let id = CompanionId::new(target.name(), &req.client_id, &req.companion_key);
                if let Some(prev) = tasks.remove(&id) {
                    prev.cancel.cancel();
                    drop(prev.task);
                }
            }
        }
        self.spawn(req).await;
    }

    /// Cancel the drain task for a companion. The on-disk
    /// `companions/<key>.cbor` is left in place — caller is
    /// responsible for that.
    pub async fn unregister(&self, id: &CompanionId) {
        let mut tasks = self.tasks.lock().await;
        if let Some(prev) = tasks.remove(id) {
            prev.cancel.cancel();
        }
    }

    /// Cancel every drain task whose `CompanionId.module_id`
    /// matches. Returns the `companion_key`s that were active +
    /// got cancelled — useful for the admin evict path to report
    /// which subscribers got reaped.
    pub async fn unregister_module(&self, module_id: &str) -> Vec<String> {
        let mut tasks = self.tasks.lock().await;
        let to_remove: Vec<CompanionId> = tasks
            .keys()
            .filter(|id| id.module_id == module_id)
            .cloned()
            .collect();
        let mut cancelled_keys = Vec::with_capacity(to_remove.len());
        for cid in to_remove {
            if let Some(prev) = tasks.remove(&cid) {
                prev.cancel.cancel();
                cancelled_keys.push(cid.companion_key);
            }
        }
        cancelled_keys
    }

    /// Coordinate a per-module recapture across every subscribed
    /// companion. Pushes a `ServerMessage::Recapture` frame onto
    /// each companion's outbound channel (the per-task drain
    /// loop receives it and runs the HTTP POST), then awaits the
    /// matching oneshot signal for each companion with
    /// `per_companion_timeout`.
    pub async fn recapture_module(
        &self,
        module_id: &str,
        reason: Option<String>,
        per_companion_timeout: Duration,
    ) -> Result<RecaptureSummary, RecaptureError> {
        let targets: Vec<(CompanionId, mpsc::UnboundedSender<ServerMessage>)> = {
            let tasks = self.tasks.lock().await;
            tasks
                .iter()
                .filter(|(id, _)| id.module_id == module_id)
                .map(|(id, slot)| (id.clone(), slot.outbound_tx.clone()))
                .collect()
        };
        if targets.is_empty() {
            return Err(RecaptureError::NoSubscribers(module_id.to_owned()));
        }
        let companion_count = targets.len();
        info!(
            module = %module_id,
            companion_count,
            timeout_secs = per_companion_timeout.as_secs(),
            "recapture: dispatching Recapture frames to subscribed companions"
        );

        // Register oneshots + push Recapture frames in two passes
        // so receivers are all in place before any frame is sent.
        let mut waiters: Vec<(CompanionId, oneshot::Receiver<()>)> =
            Vec::with_capacity(companion_count);
        {
            let mut pending = self.pending_recaptures.lock().await;
            for (id, _) in &targets {
                let (tx, rx) = oneshot::channel();
                pending.insert(id.clone(), tx);
                waiters.push((id.clone(), rx));
            }
        }
        for (id, tx) in &targets {
            let frame = ServerMessage::Recapture {
                module: module_id.to_owned(),
                reason: reason.clone(),
            };
            if let Err(e) = tx.send(frame) {
                let mut pending = self.pending_recaptures.lock().await;
                pending.remove(id);
                return Err(RecaptureError::FrameSend {
                    companion: id.clone(),
                    detail: e.to_string(),
                });
            }
        }

        let mut ready: Vec<String> = Vec::with_capacity(companion_count);
        let mut timed_out: Vec<String> = Vec::new();
        for (id, rx) in waiters {
            match tokio::time::timeout(per_companion_timeout, rx).await {
                Ok(Ok(())) => ready.push(id.companion_key.clone()),
                Ok(Err(_recv_err)) => {
                    timed_out.push(id.companion_key.clone());
                }
                Err(_elapsed) => {
                    timed_out.push(id.companion_key.clone());
                    let mut pending = self.pending_recaptures.lock().await;
                    pending.remove(&id);
                }
            }
        }

        if timed_out.is_empty() {
            info!(
                module = %module_id,
                companion_count,
                "recapture: all companions acked"
            );
            Ok(RecaptureSummary {
                module: module_id.to_owned(),
                ready_companions: ready,
            })
        } else {
            warn!(
                module = %module_id,
                timed_out_count = timed_out.len(),
                ready_count = ready.len(),
                "recapture: some companions failed to ack within timeout"
            );
            Err(RecaptureError::Timeout {
                module: module_id.to_owned(),
                ready_companions: ready,
                timed_out_companions: timed_out,
            })
        }
    }

    async fn spawn(&self, req: SubscribeRequest) {
        for target in req.targets.clone() {
            let id = CompanionId::new(target.name(), &req.client_id, &req.companion_key);
            let cancel = CancellationToken::new();
            let cancel_for_task = cancel.clone();
            let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<ServerMessage>();

            let task = match &target {
                SubscribeTarget::Module { .. } => {
                    let storage = self.storage.clone();
                    let auth = self.auth.clone();
                    let target_clone = target.clone();
                    let req_clone = req.clone();
                    let pending_recaptures = self.pending_recaptures.clone();
                    let id_for_task = id.clone();
                    tokio::spawn(async move {
                        run_companion(
                            req_clone,
                            target_clone,
                            storage,
                            auth,
                            cancel_for_task,
                            outbound_rx,
                            pending_recaptures,
                            id_for_task,
                        )
                        .await;
                    })
                }
                SubscribeTarget::Indexer { name } => {
                    let Some(bridge) = self.indexer_bridge.clone() else {
                        warn!(
                            indexer = %name,
                            companion_key = %req.companion_key,
                            "indexer-target subscribe but no bridge wired on dialer; skipping"
                        );
                        continue;
                    };
                    drop(outbound_rx);
                    bridge.spawn_dial(req.clone(), target.clone(), cancel_for_task)
                }
            };

            let mut tasks = self.tasks.lock().await;
            tasks.insert(
                id,
                ActiveCompanion {
                    cancel,
                    task,
                    outbound_tx,
                },
            );
        }
    }
}

fn load_companion(path: &std::path::Path) -> anyhow::Result<SubscribeRequest> {
    let bytes = std::fs::read(path)?;
    let req: SubscribeRequest = ciborium::de::from_reader(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("decode {}: {e}", path.display()))?;
    Ok(req)
}

/// Per-companion drain task. Owns one `reqwest::Client` (for
/// connection pooling across requests to this companion), resolves
/// the per-`{op}` URLs once, then enters the pump loop.
#[allow(clippy::too_many_arguments)]
async fn run_companion(
    req: SubscribeRequest,
    target: SubscribeTarget,
    storage: ModuleStorage,
    auth: AuthToken,
    cancel: CancellationToken,
    mut outbound_rx: mpsc::UnboundedReceiver<ServerMessage>,
    pending_recaptures: Arc<Mutex<HashMap<CompanionId, oneshot::Sender<()>>>>,
    companion_id: CompanionId,
) {
    let module_name = target.name().to_string();
    let apply_url = match resolve_op_url(&req, &target, "apply") {
        Ok(u) => u,
        Err(e) => {
            error!(
                module = %module_name,
                companion_key = %req.companion_key,
                error = %e,
                "no apply URL resolvable; companion task exiting"
            );
            return;
        }
    };
    let recapture_url = match resolve_op_url(&req, &target, "recapture") {
        Ok(u) => u,
        Err(e) => {
            error!(
                module = %module_name,
                companion_key = %req.companion_key,
                error = %e,
                "no recapture URL resolvable; companion task exiting"
            );
            return;
        }
    };
    if Url::parse(&apply_url).is_err() {
        error!(
            module = %module_name,
            url = %apply_url,
            "apply URL is malformed; companion task exiting"
        );
        return;
    }

    let store = match storage.emissions_store(&module_name) {
        Ok(s) => s,
        Err(e) => {
            error!(
                module = %module_name,
                error = %e,
                "open EmissionsStore failed; companion task exiting"
            );
            return;
        }
    };

    let client = match build_http_client() {
        Ok(c) => c,
        Err(e) => {
            error!(
                module = %module_name,
                error = %e,
                "build reqwest client failed; companion task exiting"
            );
            return;
        }
    };

    let (header_name, header_value) = resolve_auth_header(&req, &auth);

    // Reclaim Pending rows from a previous host process. Catches
    // the host-crash-mid-request case — rows marked Pending then
    // never Acked because the host died mid-POST.
    match store.requeue_pending_for_companion(
        &req.companion_key,
        &req.client_id,
        &now_rfc3339(),
    ) {
        Ok(0) => {}
        Ok(count) => {
            info!(
                module = %module_name,
                companion_key = %req.companion_key,
                count,
                "requeued Pending emissions on task start"
            );
        }
        Err(e) => {
            warn!(
                module = %module_name,
                companion_key = %req.companion_key,
                error = %e,
                "requeue Pending failed on task start"
            );
        }
    }

    let lane_config = LaneConfig::from_env();
    info!(
        module = %module_name,
        companion_key = %req.companion_key,
        apply_url = %apply_url,
        lanes = lane_config.lanes,
        "companion drain task started"
    );

    // The status writer owns redb writes for this companion's
    // emissions table so parallel lane workers don't contend on
    // the single-writer txn. At `lanes = 1` the contention is
    // theoretical, but routing all status writes through the
    // writer here keeps the path identical at every N — the
    // lift to N>1 doesn't introduce a new code path on the hot
    // loop.
    let status_writer = pool::spawn_status_writer(store.clone(), cancel.clone());

    let mut backoff = INITIAL_BACKOFF;
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate-fire first tick

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                status_writer.shutdown().await;
                return;
            }

            outbound = outbound_rx.recv() => {
                let Some(frame) = outbound else {
                    status_writer.shutdown().await;
                    return; // sender dropped — task deregistered
                };
                match frame {
                    ServerMessage::Recapture { module, reason } => {
                        let result = post_recapture(
                            &client,
                            &recapture_url,
                            header_name.as_deref(),
                            header_value.as_deref(),
                            &module,
                            reason.as_deref(),
                        )
                        .await;
                        match result {
                            Ok(()) => {
                                info!(
                                    module = %module,
                                    companion_key = %companion_id.companion_key,
                                    "Recapture POST succeeded; firing pending_recaptures oneshot"
                                );
                                fire_recapture_ready(&pending_recaptures, &companion_id).await;
                            }
                            Err(e) => {
                                error!(
                                    module = %module,
                                    companion_key = %companion_id.companion_key,
                                    error = %e,
                                    "Recapture POST failed; admin endpoint will time out"
                                );
                                // Don't fire the oneshot — admin endpoint's
                                // timeout is the safer failure mode.
                            }
                        }
                    }
                    other => {
                        debug!(?other, "ignoring non-Recapture outbound frame");
                    }
                }
            }

            _ = tick.tick() => {
                let result = pool::run_tick(pool::PoolContext {
                    client: &client,
                    apply_url: &apply_url,
                    header_name: header_name.as_deref(),
                    header_value: header_value.as_deref(),
                    store: &store,
                    companion_key: &req.companion_key,
                    client_id: &req.client_id,
                    status_writer: &status_writer,
                    lanes: lane_config.lanes,
                    now: now_rfc3339,
                }).await;
                match result {
                    Ok(()) => {
                        backoff = INITIAL_BACKOFF;
                    }
                    Err(e) => {
                        warn!(
                            module = %module_name,
                            companion_key = %req.companion_key,
                            error = %e,
                            backoff_secs = backoff.as_secs(),
                            "apply drain hit transport error; backing off"
                        );
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                status_writer.shutdown().await;
                                return;
                            }
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                }
            }
        }
    }
}

/// POST a single Recapture body to the companion. Returns Ok on
/// 2xx, Err on any non-2xx or transport error.
async fn post_recapture(
    client: &reqwest::Client,
    url: &str,
    header_name: Option<&str>,
    header_value: Option<&str>,
    module: &str,
    reason: Option<&str>,
) -> anyhow::Result<()> {
    let body = RecaptureBody {
        module: module.to_owned(),
        reason: reason.map(|s| s.to_owned()),
    };
    let body_bytes = encode_recapture(&body).map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    let mut builder = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, HTTP_DELIVERY_MIME)
        .body(body_bytes);
    if let (Some(name), Some(value)) = (header_name, header_value) {
        builder = builder.header(name, value);
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("recapture POST send: {e}"))?;
    let status = resp.status();
    if status.is_success() {
        return Ok(());
    }
    let body_text = resp.text().await.unwrap_or_default();
    Err(anyhow::anyhow!(
        "recapture POST returned status {status}: {body_text}"
    ))
}

async fn fire_recapture_ready(
    pending: &Arc<Mutex<HashMap<CompanionId, oneshot::Sender<()>>>>,
    companion_id: &CompanionId,
) {
    let sender = {
        let mut map = pending.lock().await;
        map.remove(companion_id)
    };
    match sender {
        Some(tx) => {
            if tx.send(()).is_err() {
                debug!(
                    module = %companion_id.module_id,
                    companion_key = %companion_id.companion_key,
                    "recapture ack signal arrived after driver gave up; dropping"
                );
            }
        }
        None => {
            warn!(
                module = %companion_id.module_id,
                companion_key = %companion_id.companion_key,
                "recapture ack signal but no pending recapture registered; ignoring"
            );
        }
    }
}

/// Resolve the per-op URL for one target. The URL template from
/// `dial_back.url` is substituted with three tokens:
/// - `{key}` → the companion_key
/// - `{target}` → the target's name
/// - `{op}` → `apply` or `recapture` (passed by caller)
///
/// Multi-target companions MUST include `{target}` so each
/// target's URL resolves distinctly. The `{op}` token MUST be
/// present somewhere in the path so apply and recapture URLs
/// differ — typically `https://.../_internal/{op}-{target}?key={key}`.
fn resolve_op_url(
    req: &SubscribeRequest,
    target: &SubscribeTarget,
    op: &str,
) -> anyhow::Result<String> {
    let url = req
        .dial_back
        .as_ref()
        .and_then(|d| d.url.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "subscribe request from {}/{} has no dial_back.url",
                target.name(),
                req.companion_key
            )
        })?;
    Ok(url
        .replace("{key}", &req.companion_key)
        .replace("{target}", target.name())
        .replace("{op}", op))
}

fn resolve_auth_header(
    req: &SubscribeRequest,
    auth: &AuthToken,
) -> (Option<String>, Option<String>) {
    if let Some(d) = &req.dial_back
        && let (Some(name), Some(value)) = (d.auth_header.clone(), d.auth_value.clone())
    {
        return (Some(name), Some(value));
    }
    if let Some(token) = auth.as_deref() {
        return (
            Some("Authorization".to_string()),
            Some(format!("Bearer {token}")),
        );
    }
    (None, None)
}

fn build_http_client() -> anyhow::Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        // Keep-alive defaults are fine; reqwest pools connections
        // per-host automatically via hyper's pool.
        .build()
        .map_err(|e| anyhow::anyhow!("build reqwest client: {e}"))
}

fn now_rfc3339() -> String {
    // Diagnostic-only timestamp; emissions store treats this as
    // an opaque string. Unix-seconds gives us a sortable label
    // without pulling chrono / humantime into the dep graph.
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix:{secs}")
}

/// Internal path helper. Resolves to the two-level layout:
/// `<storage>/<module>/companions/<client_id>/<companion_key>.cbor`.
/// See `docs/design/MULTI_CLIENT_COMPANIONS.md`.
pub fn companion_path_for(storage: &ModuleStorage, id: &CompanionId) -> PathBuf {
    storage
        .module_dir_for_companions(&id.module_id)
        .join(&id.client_id)
        .join(format!("{}.cbor", id.companion_key))
}
