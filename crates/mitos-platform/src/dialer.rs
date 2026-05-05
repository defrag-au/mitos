//! Companion dial loop.
//!
//! For each persisted companion (one CBOR file per
//! `<storage>/<module>/companions/<key>.cbor`), the dialer
//! maintains an outbound WebSocket back to that companion's
//! Worker URL. On connect:
//!
//! 1. Send `ServerMessage::Connected { last_emission_id }` as
//!    the readiness signal (companion logs it; nothing more).
//! 2. Drain `EmissionStatus::Queued` rows for this companion
//!    from the per-module `EmissionsStore`, sending one
//!    `ServerMessage::Apply` per row and flipping the row to
//!    `Pending`. Drain proceeds in monotonic-id order.
//! 3. Tail loop: poll the emissions store for new `Queued` rows
//!    every `POLL_INTERVAL`, and parse inbound
//!    `ClientMessage::{Ack, Nack, Unsubscribe, Interest}`
//!    frames as they arrive (Interest is logged for now —
//!    follower control-channel wiring is a separate task).
//!
//! On disconnect or transport error: exponential backoff up to
//! `MAX_BACKOFF`, then redial. Cancellation via the supervisor
//! `CancellationToken` (held in `CompanionDialer.tasks`) cuts
//! the loop and returns.
//!
//! ## Why polling, not broadcast
//!
//! The drain pump on the bundle side writes new emissions to
//! the per-module `EmissionsStore` as `Queued`. Per-companion
//! dial tasks poll for new queued rows. This avoids the
//! complexity of a per-module broadcast channel + offline-
//! companion queue split, at the cost of poll latency
//! (`POLL_INTERVAL` = 1s). Cardano blocks land every 20s on
//! mainnet, so 1s poll latency is well below the chain cadence
//! and not visible end-to-end.
//!
//! ## Auth
//!
//! Each dial carries the module-level `MITOS_AUTH_TOKEN` as a
//! Bearer header by default. Per-companion overrides via
//! `DialBackOverride.auth_header` / `auth_value` are
//! supported but rare (multi-tenant SaaS only).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::{SinkExt, StreamExt};
use mitos_protocol::{
    ClientMessage, ServerMessage, SubscribeRequest, decode_client, encode_server,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::admin::AuthToken;
use crate::emissions::{EmissionStatus, EmissionsStore};
use crate::storage::ModuleStorage;

const POLL_INTERVAL: Duration = Duration::from_millis(1_000);
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Identifier for one (module, companion_key) pair. Hashable
/// + cloneable for use as map key.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct CompanionId {
    pub module_id: String,
    pub companion_key: String,
}

impl CompanionId {
    pub fn new(module_id: impl Into<String>, companion_key: impl Into<String>) -> Self {
        Self {
            module_id: module_id.into(),
            companion_key: companion_key.into(),
        }
    }
}

/// Per-companion dial supervisor entry. Owns the task handle
/// + cancellation token.
struct ActiveCompanion {
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

/// Dial supervisor — one instance per running mitos host.
/// Spawns/cancels per-companion tasks as the registry
/// (the on-disk `companions/*.cbor` set) changes.
#[derive(Clone)]
pub struct CompanionDialer {
    storage: ModuleStorage,
    auth: AuthToken,
    tasks: Arc<Mutex<HashMap<CompanionId, ActiveCompanion>>>,
}

impl CompanionDialer {
    pub fn new(storage: ModuleStorage, auth: AuthToken) -> Self {
        Self {
            storage,
            auth,
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Scan `<storage_root>/*/companions/*.cbor` and start a
    /// dial loop for each persisted companion. Failures on
    /// individual companion files are logged but don't abort
    /// the scan.
    pub async fn start_all(&self) {
        let modules = match self.storage.list_modules() {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "list_modules failed; no companion dial loops started");
                return;
            }
        };
        for module_id in modules {
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
            for entry in read.flatten() {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
                    continue;
                }
                match load_companion(&path) {
                    Ok(req) => self.spawn(req).await,
                    Err(e) => warn!(path = %path.display(), error = %e, "load companion failed"),
                }
            }
        }
    }

    /// Register (or re-register) a companion. Called from the
    /// `/api/companions/subscribe` handler after the CBOR file
    /// has been written to disk. Cancels any existing dial
    /// loop for the same `(module_id, companion_key)` and
    /// spawns a fresh one with the updated registration.
    pub async fn register(&self, req: SubscribeRequest) {
        let id = CompanionId::new(&req.module_name, &req.companion_key);
        // Cancel any existing task for this id before spawning
        // a fresh one — register() is idempotent re-registration.
        {
            let mut tasks = self.tasks.lock().await;
            if let Some(prev) = tasks.remove(&id) {
                prev.cancel.cancel();
                let _ = prev.task; // task drains itself on cancel
            }
        }
        self.spawn(req).await;
    }

    /// Cancel the dial loop for a companion. The on-disk
    /// `companions/<key>.cbor` is left in place — caller is
    /// responsible for that.
    pub async fn unregister(&self, id: &CompanionId) {
        let mut tasks = self.tasks.lock().await;
        if let Some(prev) = tasks.remove(id) {
            prev.cancel.cancel();
        }
    }

    async fn spawn(&self, req: SubscribeRequest) {
        let id = CompanionId::new(&req.module_name, &req.companion_key);
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let storage = self.storage.clone();
        let auth = self.auth.clone();
        let task = tokio::spawn(async move {
            run_companion(req, storage, auth, cancel_for_task).await;
        });
        let mut tasks = self.tasks.lock().await;
        tasks.insert(id, ActiveCompanion { cancel, task });
    }
}

fn load_companion(path: &std::path::Path) -> anyhow::Result<SubscribeRequest> {
    let bytes = std::fs::read(path)?;
    let req: SubscribeRequest = ciborium::de::from_reader(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("decode {}: {e}", path.display()))?;
    Ok(req)
}

/// Per-companion supervisor: dial → loop with reconnect/backoff
/// until cancelled.
async fn run_companion(
    req: SubscribeRequest,
    storage: ModuleStorage,
    auth: AuthToken,
    cancel: CancellationToken,
) {
    let url_str = match resolve_dial_url(&req) {
        Ok(u) => u,
        Err(e) => {
            error!(
                module = %req.module_name,
                companion_key = %req.companion_key,
                error = %e,
                "no dial-back URL configured; companion will not receive emissions"
            );
            return;
        }
    };
    let parsed = match Url::parse(&url_str) {
        Ok(u) => u,
        Err(e) => {
            error!(
                module = %req.module_name,
                companion_key = %req.companion_key,
                url = %url_str,
                error = %e,
                "invalid dial-back URL; companion task exiting"
            );
            return;
        }
    };

    let store = match storage.emissions_store(&req.module_name) {
        Ok(s) => s,
        Err(e) => {
            error!(
                module = %req.module_name,
                error = %e,
                "open EmissionsStore failed; companion task exiting"
            );
            return;
        }
    };

    let mut backoff = INITIAL_BACKOFF;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        match dial_and_pump(&parsed, &req, &auth, &store, &cancel).await {
            Ok(()) => {
                info!(
                    module = %req.module_name,
                    companion_key = %req.companion_key,
                    "companion dial loop exited cleanly; redialing"
                );
                backoff = INITIAL_BACKOFF;
            }
            Err(e) => {
                warn!(
                    module = %req.module_name,
                    companion_key = %req.companion_key,
                    target = %url_str,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "companion dial errored; backing off",
                );
            }
        }
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Resolve the dial-back URL. v1 requires `dial_back.url` to
/// be set on the subscribe request. Module-level
/// `[companion] replicate_url` defaults are wired in a
/// follow-up.
fn resolve_dial_url(req: &SubscribeRequest) -> anyhow::Result<String> {
    let url = req
        .dial_back
        .as_ref()
        .and_then(|d| d.url.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "subscribe request from {}/{} has no dial_back.url",
                req.module_name,
                req.companion_key
            )
        })?;
    Ok(url.replace("{key}", &req.companion_key))
}

async fn dial_and_pump(
    url: &Url,
    req: &SubscribeRequest,
    auth: &AuthToken,
    store: &EmissionsStore,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    // Build request with auth header. Per-companion override
    // takes precedence over the module-level token.
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|e| anyhow::anyhow!("build ws request: {e}"))?;
    let (header_name, header_value) = resolve_auth_header(req, auth);
    if let (Some(name), Some(value)) = (header_name, header_value) {
        let header_name: tokio_tungstenite::tungstenite::http::HeaderName =
            name.parse().map_err(|e| anyhow::anyhow!("auth header name: {e}"))?;
        let header_value = value
            .parse()
            .map_err(|e| anyhow::anyhow!("auth header value (must be ASCII): {e}"))?;
        request.headers_mut().insert(header_name, header_value);
    }

    let (stream, _resp) = connect_async(request)
        .await
        .map_err(|e| anyhow::anyhow!("ws connect: {e}"))?;
    info!(
        module = %req.module_name,
        companion_key = %req.companion_key,
        target = %url,
        "companion ws connected"
    );

    let (mut sink, mut source) = stream.split();

    // Send Connected as readiness signal. last_emission_id =
    // peek_next_id - 1 (the highest assigned id so far).
    let next_id = store.peek_next_id().unwrap_or(1);
    let last_emission_id = next_id.saturating_sub(1);
    send_msg(
        &mut sink,
        &ServerMessage::Connected { last_emission_id },
    )
    .await?;

    // Drain queued rows in id order, send Apply, flip to
    // Pending. New rows arriving while we drain will show up
    // on the next poll iteration in the main loop.
    drain_queued(&mut sink, store, &req.companion_key).await?;

    // Main pump: tokio::select! over inbound frames + a poll
    // tick that re-checks for new Queued rows.
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate-fire first tick

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => return Ok(()),

            inbound = source.next() => {
                match inbound {
                    Some(Ok(Message::Binary(bytes))) => {
                        handle_inbound_frame(&bytes, store).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        info!(?frame, "peer closed ws");
                        return Ok(());
                    }
                    Some(Ok(_)) => {} // text/ping/pong/etc — ignore
                    Some(Err(e)) => {
                        return Err(anyhow::anyhow!("ws recv: {e}"));
                    }
                    None => return Ok(()), // stream exhausted
                }
            }

            _ = tick.tick() => {
                drain_queued(&mut sink, store, &req.companion_key).await?;
            }
        }
    }
}

fn resolve_auth_header(
    req: &SubscribeRequest,
    auth: &AuthToken,
) -> (Option<String>, Option<String>) {
    if let Some(d) = &req.dial_back {
        if let (Some(name), Some(value)) = (d.auth_header.clone(), d.auth_value.clone()) {
            return (Some(name), Some(value));
        }
    }
    if let Some(token) = auth.as_deref() {
        return (
            Some("Authorization".to_string()),
            Some(format!("Bearer {token}")),
        );
    }
    (None, None)
}

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    Message,
>;

async fn send_msg(sink: &mut WsSink, msg: &ServerMessage) -> anyhow::Result<()> {
    let bytes = encode_server(msg).map_err(|e| anyhow::anyhow!("encode: {e}"))?;
    sink.send(Message::Binary(bytes.into()))
        .await
        .map_err(|e| anyhow::anyhow!("ws send: {e}"))
}

/// Drain `Queued` rows for this companion in id order. Each
/// row → one `ServerMessage::Apply`; status flips to
/// `Pending` after the send.
async fn drain_queued(
    sink: &mut WsSink,
    store: &EmissionsStore,
    companion_key: &str,
) -> anyhow::Result<()> {
    let queued = store
        .list_queued_for_companion(companion_key)
        .map_err(|e| anyhow::anyhow!("list queued: {e}"))?;
    if queued.is_empty() {
        return Ok(());
    }
    debug!(
        companion_key = %companion_key,
        count = queued.len(),
        "draining queued emissions"
    );
    for row in queued {
        let msg = ServerMessage::Apply {
            emission_id: row.id,
            cursor: row.chain_point.clone(),
            change: row.payload.clone(),
        };
        send_msg(sink, &msg).await?;
        let now = now_rfc3339();
        if let Err(e) = store.update_status(row.id, EmissionStatus::Pending, &now, None) {
            warn!(id = row.id, error = %e, "update_status Queued→Pending failed");
        }
    }
    Ok(())
}

async fn handle_inbound_frame(bytes: &[u8], store: &EmissionsStore) {
    let msg = match decode_client(bytes) {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, "decode client frame failed; ignoring");
            return;
        }
    };
    match msg {
        ClientMessage::Ack { emission_id } => {
            let now = now_rfc3339();
            if let Err(e) = store.update_status(emission_id, EmissionStatus::Acked, &now, None) {
                warn!(emission_id, error = %e, "update_status Pending→Acked failed");
            }
        }
        ClientMessage::Nack { emission_id, error } => {
            let now = now_rfc3339();
            if let Err(e) =
                store.update_status(emission_id, EmissionStatus::Nacked, &now, Some(error))
            {
                warn!(emission_id, error = %e, "update_status Pending→Nacked failed");
            }
        }
        ClientMessage::Interest { op, items } => {
            // TODO: forward to follower's update-interest call
            // via a control channel (separate work item).
            debug!(?op, count = items.len(), "Interest frame received; not yet wired");
        }
        ClientMessage::Unsubscribe => {
            info!("companion sent Unsubscribe; closing pump");
        }
        ClientMessage::Subscribe { .. } => {
            warn!("companion sent unexpected Subscribe frame; ignoring");
        }
    }
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

/// Internal path helper (re-exported via storage).
pub fn companion_path_for(storage: &ModuleStorage, id: &CompanionId) -> PathBuf {
    storage
        .module_dir_for_companions(&id.module_id)
        .join(format!("{}.cbor", id.companion_key))
}
