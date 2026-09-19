//! Companion dial loop (HTTP delivery).
//!
//! The dialer runs **one drain task per module** (not one per
//! companion). The per-module task scans the module's shared
//! `EmissionsStore` once per `POLL_INTERVAL` and fans the queued
//! rows to every subscribed companion via HTTP POST against each
//! companion's Worker URL. This replaces the previous
//! one-task-per-companion model, where N companions each
//! full-scanned the shared store every tick — O(companions × rows)
//! redb reads per second. With per-policy companions (1K+ on a
//! single module), that is the load that bites; the per-module
//! drain collapses it to one scan per module per tick. See
//! `docs/design/DIALER_CONCURRENCY.md` ("Per-module drain").
//!
//! ## Registry
//!
//! Each module task shares a `registry` (`HashMap<CompanionId,
//! CompanionDial>`) with the supervisor. The supervisor mutates it
//! on subscribe/unsubscribe (`register` / `unregister`); the task
//! reads a snapshot each tick. A [`CompanionDial`] holds the
//! once-resolved apply/recapture URLs + auth + per-companion bulk
//! capability cache.
//!
//! ## One drain per MODULE, every INSTANCE's store
//!
//! A module can have more than one instance (WO-06), and each keeps its own
//! `emissions.redb`. The drain still runs one task per module and reads them
//! all — NOT one task per instance, which would let two tasks POST to the
//! same companion at once and would split the recapture wipe's "never
//! overlaps an apply" guarantee across tasks that cannot see each other. See
//! `mitos_lifecycle::routing::DrainPass`.
//!
//! ## Per-tick lifecycle
//!
//! 1. **Reconcile the open stores** — `list_instances` for the module; shut
//!    down and drop the writers of keys that are gone (an upload RENAMES the
//!    directory, so `K1` disappears and `K2` appears with the same file),
//!    then for keys newly seen in this process run `requeue_all_pending`,
//!    which flips every row left `Pending` by a prior host process back to
//!    `Queued`. Only at the START of a tick: between ticks is the one moment
//!    no in-flight send is holding a status writer.
//! 2. **Scan once per store** — `list_queued_grouped_by_companion` reads all
//!    `Queued` rows in one txn, grouped by `(companion, client)`.
//! 3. **Plan** — [`plan_module_tick`], a pure function: each companion's
//!    batch is tagged `Leftover` or `Serving` by `routing::drain_pass`, and
//!    companions that are backing off or mid-recapture are dropped.
//! 4. **Fan out** — run every `Leftover` batch to completion (bounded by
//!    `MITOS_DIALER_MODULE_CONCURRENCY`), update `retry`, THEN run the
//!    `Serving` batches, minus any companion whose leftovers just failed.
//!    Each row → one HTTP POST to `/_internal/apply-<channel>`
//!    (or the bulk route). Response status maps to:
//!      - `2xx` → `Acked`
//!      - `422` → `Nacked` (apply errored; won't help to retry)
//!      - `5xx` / network error → leave `Queued`, back off + retry
//! 5. **Backoff** — a companion's transport errors trigger
//!    per-companion exponential backoff up to `MAX_BACKOFF`
//!    (tracked in the task's `retry` map; the loop keeps ticking
//!    for the other companions). A module-level `CancellationToken`
//!    cuts the task on module retirement.
//!
//! ## Recapture flow
//!
//! `recapture_module` (called by the admin endpoint) snapshots the
//! module's companions, registers a `pending_recaptures` oneshot
//! per companion, and pushes a `ModuleControl::Recapture { id }`
//! frame per companion onto the module task's control channel. The
//! task handles each frame in its `select!` loop — mutually
//! exclusive with the tick-drain, which awaits its full batch
//! before yielding back to `select!`. On a Recapture frame the
//! task sets the companion's `recapturing` flag (so the tick skips
//! it), then spawns the recapture POST; on completion the flag
//! clears and the `pending_recaptures` oneshot fires on 2xx. The
//! admin endpoint awaits each oneshot with `per_companion_timeout`.
//!
//! Pause semantics: an apply POST never overlaps a companion's
//! recapture (wipe) POST — the `recapturing` flag gates the tick,
//! and the flag is set in the same single-threaded loop that runs
//! the tick. The companion's `on_recapture` finishes wiping its
//! table before draining resumes (which then delivers the refill
//! events).
//!
//! ## Indexer targets
//!
//! `SubscribeTarget::Indexer` requests keep the legacy
//! one-task-per-companion shape (dispatched through the in-tree
//! indexer bridge) — in-tree indexers are a small fixed set, not
//! the per-policy fan-out the module drain optimises.
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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant, SystemTime};

use mitos_lifecycle::ids::InstanceKey;
use mitos_lifecycle::routing::{self, DrainPass};
use mitos_protocol::{
    HTTP_DELIVERY_MIME, RecaptureBody, SubscribeRequest, SubscribeTarget, encode_recapture,
};
use tokio::sync::{Mutex, Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::admin::AuthToken;
use crate::emissions::EmissionRecord;
use crate::host_v2::InterestRouter;
use crate::indexer_bridge::IndexerBridgeHandle;
use crate::rows::{RowCache, instance_dir_of};
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

/// Max companions a module drain delivers to concurrently within a
/// single tick. Bounds the thundering-herd during a refill burst
/// (e.g. a whole-module recapture re-emitting for 1K companions)
/// without serialising idle ticks — most ticks have a handful of
/// active companions and never approach the cap. Overridable via
/// `MITOS_DIALER_MODULE_CONCURRENCY`.
const DEFAULT_MODULE_DRAIN_CONCURRENCY: usize = 64;

fn module_drain_concurrency() -> usize {
    std::env::var("MITOS_DIALER_MODULE_CONCURRENCY")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .unwrap_or(DEFAULT_MODULE_DRAIN_CONCURRENCY)
}

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

/// Per-companion resolved dial configuration, stored in a module
/// drain's `registry`. Built once at register time (URL + auth
/// resolution), then read by every poll tick to deliver that
/// companion's queued rows. Cheap to clone — owned strings + Arcs.
#[derive(Clone)]
struct CompanionDial {
    id: CompanionId,
    companion_key: String,
    apply_url: String,
    /// Bulk-apply URL (`apply_url` with `-bulk` before the query).
    bulk_url: String,
    recapture_url: String,
    /// Undo URL (`{op}` = `undo`) — `POST /_internal/undo-<target>`,
    /// used to deliver chain-rollback `is_undo` rows.
    undo_url: String,
    header_name: Option<String>,
    header_value: Option<String>,
    /// Per-companion bulk-route capability cache
    /// (`BULK_UNKNOWN` → `SUPPORTED`/`UNSUPPORTED`), shared across
    /// this companion's lanes + across ticks. See `pool::BULK_UNKNOWN`.
    bulk_capability: Arc<AtomicU8>,
    /// Set while a recapture (table-wipe) POST is in flight for
    /// this companion; the poll tick skips flagged companions so an
    /// apply POST never races a wipe POST. Cleared when the wipe
    /// settles. See the module-level "Recapture flow" docs.
    recapturing: Arc<AtomicBool>,
}

/// Per-companion exponential-backoff state, owned by a module
/// drain task. Absent = healthy (next failure starts at
/// `INITIAL_BACKOFF`); present = the companion's endpoint is
/// erroring and the tick skips it until `next_retry_at`.
struct RetryState {
    backoff: Duration,
    next_retry_at: Instant,
}

/// Control frame to a running module-drain task. Registry mutation
/// (register/unregister) happens directly on the shared registry
/// under its lock; the channel carries only operations that must
/// be serialised against the poll tick — today just recapture,
/// whose wipe POST must not overlap an apply POST to the same
/// companion (`docs/design/RECAPTURE.md`).
enum ModuleControl {
    Recapture {
        id: CompanionId,
        reason: Option<String>,
    },
    /// Let go of one instance's emissions store — shut down its status
    /// writer and drop the handle — and say when that is done.
    ///
    /// A reap removes the instance directory, and a status writer holding a
    /// clone of its `emissions.redb` is exactly what makes `remove_dir_all`
    /// fail. `evict_instance` sends this and awaits the reply before it
    /// closes anything.
    ReleaseInstance {
        key: InstanceKey,
        done: oneshot::Sender<()>,
    },
}

/// Supervisor handle for one module's drain task. One per module
/// with ≥1 subscribed `Module`-target companion.
struct ModuleDrain {
    cancel: CancellationToken,
    #[allow(dead_code)]
    task: JoinHandle<()>,
    control_tx: mpsc::UnboundedSender<ModuleControl>,
    /// Shared with the drain task: the supervisor mutates it on
    /// register/unregister + reads it to enumerate recapture
    /// targets; the task reads a snapshot each poll tick.
    registry: Arc<Mutex<HashMap<CompanionId, CompanionDial>>>,
}

/// Per-companion task for an `Indexer` target. These keep the
/// legacy one-task-per-companion shape (dispatched through the
/// in-tree indexer bridge); in-tree indexers are a small fixed
/// set, not the per-policy fan-out the module drain optimises.
struct ActiveIndexerTask {
    cancel: CancellationToken,
    #[allow(dead_code)]
    task: JoinHandle<()>,
}

/// Dial supervisor — one instance per running mitos host. Owns one
/// drain task per module (for `Module` targets) plus per-companion
/// tasks for `Indexer` targets, spawning/cancelling them as the
/// registry (the on-disk `companions/*.cbor` set) changes.
#[derive(Clone)]
pub struct CompanionDialer {
    storage: ModuleStorage,
    auth: AuthToken,
    /// Routes companion-initiated interest updates into the
    /// matching module's follower task. Kept on the supervisor
    /// for use by future inbound paths (e.g. dynamic-interest
    /// HTTP endpoint on mitos). Not consulted by the
    /// HTTP drain loop today — interest changes flow via the
    /// companion's re-subscribe path.
    #[allow(dead_code)]
    interest_router: Option<Arc<dyn InterestRouter>>,
    /// In-tree indexer bridge — handles `SubscribeTarget::Indexer`
    /// dispatch. Provided by `mitos-core::Bundle` at runtime when
    /// the host has in-tree indexers. `None` means the dialer
    /// only handles `Module` targets.
    indexer_bridge: Option<IndexerBridgeHandle>,
    /// One drain task per module (`Module` targets).
    module_drains: Arc<Mutex<HashMap<String, ModuleDrain>>>,
    /// Per-companion tasks for `Indexer` targets.
    indexer_tasks: Arc<Mutex<HashMap<CompanionId, ActiveIndexerTask>>>,
    /// One-shot senders the recapture driver uses to await
    /// "ready" from each targeted companion. Registered before
    /// the Recapture frame is pushed; fired by the module task
    /// when the companion's recapture POST returns 2xx. See
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
            module_drains: Arc::new(Mutex::new(HashMap::new())),
            indexer_tasks: Arc::new(Mutex::new(HashMap::new())),
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
                        Ok(req) => self.register(req).await,
                        Err(e) => {
                            warn!(path = %cpath.display(), error = %e, "load companion failed")
                        }
                    }
                }
            }

            // Reconcile THIS module's scan-interest from the union of
            // its persisted companions (see `reconcile_module_interest`
            // for why — CO1-class stranding otherwise persists across
            // every restart).
            self.reconcile_module_interest(&module_id).await;
        }
    }

    /// Re-assert `module_id`'s scan-interest from the union of its
    /// persisted companions' interests.
    ///
    /// `register` only restores the per-companion FANOUT interest; the
    /// module's SCAN-interest (walked by cold-start + recapture via
    /// `utxos_by_policy`, persisted in module state-kv via
    /// `update_interest`) is updated only by routing a mutation into
    /// the follower — what `subscribe_handler` does live. A companion
    /// whose policy never entered the scan-set (a subscribe that
    /// raced/predated interest-routing, or — the 2026-06/07 outage —
    /// one that landed while the module was STOPPED and had its
    /// mutation dropped with `NotRunning`) otherwise stays stranded:
    /// fanout registered, scan-set never updated, so cold-start +
    /// recapture skip it and it never captures (CO1).
    ///
    /// Callers: `start_all` (boot reload) and the watchdog after it
    /// revives a stopped module. One union mutation per module —
    /// applied once, not per companion, to avoid re-triggering the
    /// module's cold-start machinery per companion.
    ///
    /// Reconcile op: for the chunked cold-start modules (whose
    /// scan-interest IS the durable tracked-policy set) use `Replace`
    /// so the call asserts EXACTLY the live companion set: a policy
    /// whose companion was deleted is dropped from the module's
    /// tracked set + onboard scope + shards, instead of lingering as
    /// an orphan that gets re-scanned (and, for CIP-25, re-resolved
    /// via Maestro) on every restart. `Replace` with no genuinely-new
    /// policies seeds no onboard scope, so the follower's Onboard pump
    /// is a no-op. Other modules keep `Add` (their update-interest
    /// isn't a full-set authority and their bootstrap is idempotent +
    /// cheap).
    pub async fn reconcile_module_interest(&self, module_id: &str) {
        let dir = self.storage.module_dir_for_companions(module_id);
        if !dir.exists() {
            return;
        }
        let read = match std::fs::read_dir(&dir) {
            Ok(r) => r,
            Err(e) => {
                warn!(
                    module = %module_id,
                    dir = %dir.display(),
                    error = %e,
                    "reconcile: read companions dir failed"
                );
                return;
            }
        };
        let mut module_interests: Vec<mitos_protocol::Interest> = Vec::new();
        for entry in read.flatten() {
            let path = entry.path();
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            if !is_dir || entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            let Ok(client_files) = std::fs::read_dir(&path) else {
                continue;
            };
            for client_entry in client_files.flatten() {
                let cpath = client_entry.path();
                if cpath.extension().and_then(|s| s.to_str()) != Some("cbor") {
                    continue;
                }
                if let Ok(req) = load_companion(&cpath) {
                    for i in &req.interests {
                        if !module_interests.contains(i) {
                            module_interests.push(i.clone());
                        }
                    }
                }
            }
        }
        let reconcile_op = if crate::manifest::is_chunked_cold_start_module(module_id) {
            mitos_protocol::InterestOp::Replace
        } else {
            mitos_protocol::InterestOp::Add
        };
        if !module_interests.is_empty()
            && let Err(e) = self
                .route_interest_mutation(module_id, reconcile_op, module_interests)
                .await
        {
            warn!(
                module = %module_id,
                error = %e,
                "reconcile: routing module scan-interest from persisted \
                 companions failed; fanout-interest still registered",
            );
        }
    }

    /// Register (or re-register) a companion. Called from the
    /// `/api/companions/subscribe` handler after persistence +
    /// validation. Each `Module` target is resolved into a
    /// [`CompanionDial`] and inserted into its module drain's
    /// registry (spawning the module drain task on first use);
    /// each `Indexer` target spawns a per-companion bridge task.
    /// Idempotent re-registration — a re-register overwrites the
    /// existing dial / replaces the indexer task.
    pub async fn register(&self, req: SubscribeRequest) {
        for target in req.targets.clone() {
            match &target {
                SubscribeTarget::Module { .. } => {
                    self.register_module_companion(&req, &target).await;
                }
                SubscribeTarget::Indexer { name } => {
                    self.register_indexer_companion(&req, &target, name).await;
                }
            }
        }
    }

    /// Resolve a `Module`-target companion's dial config and add it
    /// to the module drain's registry, spawning the drain task if
    /// this is the module's first companion. Resolution failures
    /// (no dial-back URL, malformed apply URL) log + skip — the
    /// companion isn't registered, matching the old behaviour where
    /// its per-companion task exited early.
    async fn register_module_companion(&self, req: &SubscribeRequest, target: &SubscribeTarget) {
        let module_id = target.name();
        let dial = match resolve_companion_dial(req, target, &self.auth) {
            Ok(d) => d,
            Err(e) => {
                error!(
                    module = %module_id,
                    companion_key = %req.companion_key,
                    error = %e,
                    "resolve companion dial failed; companion not registered"
                );
                return;
            }
        };
        let registry = self.ensure_module_task(module_id).await;
        registry.lock().await.insert(dial.id.clone(), dial);
    }

    /// Spawn a per-companion `Indexer`-target task through the
    /// bridge, replacing any existing task for the same id.
    async fn register_indexer_companion(
        &self,
        req: &SubscribeRequest,
        target: &SubscribeTarget,
        name: &str,
    ) {
        let Some(bridge) = self.indexer_bridge.clone() else {
            warn!(
                indexer = %name,
                companion_key = %req.companion_key,
                "indexer-target subscribe but no bridge wired on dialer; skipping"
            );
            return;
        };
        let id = CompanionId::new(target.name(), &req.client_id, &req.companion_key);
        {
            let mut tasks = self.indexer_tasks.lock().await;
            if let Some(prev) = tasks.remove(&id) {
                prev.cancel.cancel();
            }
        }
        let cancel = CancellationToken::new();
        let task = bridge.spawn_dial(req.clone(), target.clone(), cancel.clone());
        self.indexer_tasks
            .lock()
            .await
            .insert(id, ActiveIndexerTask { cancel, task });
    }

    /// Ensure a drain task exists for `module_id`, returning a
    /// handle to its shared registry. Spawns the task on first use.
    async fn ensure_module_task(
        &self,
        module_id: &str,
    ) -> Arc<Mutex<HashMap<CompanionId, CompanionDial>>> {
        let mut drains = self.module_drains.lock().await;
        if let Some(existing) = drains.get(module_id) {
            return existing.registry.clone();
        }
        let cancel = CancellationToken::new();
        let (control_tx, control_rx) = mpsc::unbounded_channel::<ModuleControl>();
        let registry: Arc<Mutex<HashMap<CompanionId, CompanionDial>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let task = {
            let module_id = module_id.to_string();
            let storage = self.storage.clone();
            let cancel = cancel.clone();
            let registry = registry.clone();
            let pending = self.pending_recaptures.clone();
            tokio::spawn(async move {
                run_module_drain(module_id, storage, cancel, control_rx, registry, pending).await;
            })
        };
        drains.insert(
            module_id.to_string(),
            ModuleDrain {
                cancel,
                task,
                control_tx,
                registry: registry.clone(),
            },
        );
        registry
    }

    /// Deregister a companion. For a `Module` target this removes
    /// its dial from the module drain's registry (the drain task
    /// keeps running for the module's other companions); for an
    /// `Indexer` target it cancels the per-companion task. The
    /// on-disk `companions/<key>.cbor` is left in place — caller is
    /// responsible for that.
    pub async fn unregister(&self, id: &CompanionId) {
        let registry = self
            .module_drains
            .lock()
            .await
            .get(&id.module_id)
            .map(|d| d.registry.clone());
        if let Some(registry) = registry {
            registry.lock().await.remove(id);
        }
        let mut tasks = self.indexer_tasks.lock().await;
        if let Some(prev) = tasks.remove(id) {
            prev.cancel.cancel();
        }
    }

    /// Cancel a module's entire drain (and any indexer companions
    /// on the same `module_id`). Returns the `companion_key`s that
    /// were active + got reaped — useful for the admin evict path
    /// to report which subscribers got cut.
    pub async fn unregister_module(&self, module_id: &str) -> Vec<String> {
        let mut cancelled_keys = Vec::new();
        let drain = self.module_drains.lock().await.remove(module_id);
        if let Some(drain) = drain {
            drain.cancel.cancel();
            let reg = drain.registry.lock().await;
            cancelled_keys.extend(reg.keys().map(|id| id.companion_key.clone()));
        }
        let mut tasks = self.indexer_tasks.lock().await;
        let to_remove: Vec<CompanionId> = tasks
            .keys()
            .filter(|id| id.module_id == module_id)
            .cloned()
            .collect();
        for cid in to_remove {
            if let Some(prev) = tasks.remove(&cid) {
                prev.cancel.cancel();
                cancelled_keys.push(cid.companion_key);
            }
        }
        cancelled_keys
    }

    /// Ask the module's drain to let go of ONE instance's emissions store,
    /// and wait until it has.
    ///
    /// `evict_instance` calls this before it closes handles and removes the
    /// directory: the drain's status writer owns a clone of that store, and a
    /// live redb handle is what makes `remove_dir_all` fail. The reply comes
    /// after the writer has DRAINED, so a status update in flight is written
    /// to the file rather than lost with it.
    ///
    /// Returns at once when the module has no drain — nothing holds the store
    /// and there is nothing to wait for.
    ///
    /// **The wait is BOUNDED.** The drain handles control frames between
    /// ticks, and a tick lasts as long as its slowest send — up to
    /// `REQUEST_TIMEOUT` when a companion is hanging. The caller is a `Reap`
    /// effect running under the module's lifecycle lock, which every
    /// subscribe to that module also takes; an unbounded wait here would make
    /// a subscribe hang behind somebody else's unreachable worker. So past
    /// the bound this answers [`Released::StillBusy`] and the reap fails,
    /// typed, into the retry it already has (`pending_cleanup`). The frame
    /// stays queued: the drain handles it when its tick ends, and the retry
    /// finds the store already released.
    pub async fn release_instance(&self, key: &InstanceKey) -> Released {
        let control_tx = {
            let drains = self.module_drains.lock().await;
            drains
                .get(key.module.as_str())
                .map(|d| d.control_tx.clone())
        };
        let Some(control_tx) = control_tx else {
            return Released::Done;
        };
        let (done, wait) = oneshot::channel();
        if control_tx
            .send(ModuleControl::ReleaseInstance {
                key: key.clone(),
                done,
            })
            .is_err()
        {
            // The drain task is gone, which means its stores went with it.
            return Released::Done;
        }
        match tokio::time::timeout(RELEASE_WAIT, wait).await {
            // `Err` inside is the drain dropping the sender as it exits —
            // its stores went with it.
            Ok(_) => Released::Done,
            Err(_) => Released::StillBusy,
        }
    }

    /// Coordinate a per-module recapture across every subscribed
    /// companion. Snapshots the module's companions, registers a
    /// `pending_recaptures` oneshot per companion, then pushes one
    /// `ModuleControl::Recapture` frame per companion onto the
    /// module drain's control channel (the drain task runs each
    /// recapture POST, mutually exclusive with its tick-drain), and
    /// awaits the matching oneshot per companion with
    /// `per_companion_timeout`.
    pub async fn recapture_module(
        &self,
        module_id: &str,
        reason: Option<String>,
        per_companion_timeout: Duration,
    ) -> Result<RecaptureSummary, RecaptureError> {
        // Snapshot the module's companions + grab its control sender
        // under one module_drains lock, then release before any await.
        let (control_tx, ids): (mpsc::UnboundedSender<ModuleControl>, Vec<CompanionId>) = {
            let drains = self.module_drains.lock().await;
            let Some(drain) = drains.get(module_id) else {
                return Err(RecaptureError::NoSubscribers(module_id.to_owned()));
            };
            let ids: Vec<CompanionId> = drain.registry.lock().await.keys().cloned().collect();
            (drain.control_tx.clone(), ids)
        };
        if ids.is_empty() {
            return Err(RecaptureError::NoSubscribers(module_id.to_owned()));
        }
        let companion_count = ids.len();
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
            for id in &ids {
                let (tx, rx) = oneshot::channel();
                pending.insert(id.clone(), tx);
                waiters.push((id.clone(), rx));
            }
        }
        for id in &ids {
            let frame = ModuleControl::Recapture {
                id: id.clone(),
                reason: reason.clone(),
            };
            if let Err(e) = control_tx.send(frame) {
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
}

fn load_companion(path: &std::path::Path) -> anyhow::Result<SubscribeRequest> {
    let bytes = std::fs::read(path)?;
    let req: SubscribeRequest = ciborium::de::from_reader(bytes.as_slice())
        .map_err(|e| anyhow::anyhow!("decode {}: {e}", path.display()))?;
    Ok(req)
}

/// Resolve a `Module`-target companion's dial config from its
/// subscribe request. Errors if no dial-back URL is set or the
/// apply URL is malformed (the same conditions that made the old
/// per-companion task exit early before entering its loop).
fn resolve_companion_dial(
    req: &SubscribeRequest,
    target: &SubscribeTarget,
    auth: &AuthToken,
) -> anyhow::Result<CompanionDial> {
    let apply_url = resolve_op_url(req, target, "apply")?;
    if Url::parse(&apply_url).is_err() {
        anyhow::bail!("apply URL is malformed: {apply_url}");
    }
    let recapture_url = resolve_op_url(req, target, "recapture")?;
    let undo_url = resolve_op_url(req, target, "undo")?;
    // `bulk_url` is `apply_url` with `-bulk` spliced before the
    // query; the capability cache lazily resolves whether the
    // companion has the bulk route (404/415 → fall back to per-row
    // for its lifetime). See `docs/design/DIALER_BULK_APPLY.md`.
    let bulk_url = bulk_url_from_apply(&apply_url);
    let (header_name, header_value) = resolve_auth_header(req, auth);
    Ok(CompanionDial {
        id: CompanionId::new(target.name(), &req.client_id, &req.companion_key),
        companion_key: req.companion_key.clone(),
        apply_url,
        bulk_url,
        recapture_url,
        undo_url,
        header_name,
        header_value,
        bulk_capability: Arc::new(AtomicU8::new(pool::BULK_UNKNOWN)),
        recapturing: Arc::new(AtomicBool::new(false)),
    })
}

/// How long a reap waits for the drain to let go of a store — a little over
/// one quiet tick (`POLL_INTERVAL`), and far under one hung send.
const RELEASE_WAIT: Duration = Duration::from_secs(3);

/// Whether the drain let go of an instance's store within [`RELEASE_WAIT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Released {
    Done,
    /// Mid-tick. The request is still queued and WILL be honoured; ask again.
    StillBusy,
}

/// One instance's half of a module's drain: its emissions store and the
/// status writer that owns redb writes to it.
struct OpenStore {
    store: crate::emissions::EmissionsStore,
    status_writer: pool::StatusWriterHandle,
}

/// Per-module drain task. Holds one open store per INSTANCE of the module
/// plus a shared `reqwest::Client`, then loops: each `POLL_INTERVAL` it
/// reconciles the open set against the instance directories, scans each store
/// once, and fans the queued rows to every registered companion (bounded
/// concurrency, leftovers before serving). A control channel carries
/// recapture frames and instance releases, serialised against the tick-drain
/// so a wipe POST never overlaps an apply POST for the same companion. See
/// the module-level docs.
async fn run_module_drain(
    module_id: String,
    storage: ModuleStorage,
    cancel: CancellationToken,
    mut control_rx: mpsc::UnboundedReceiver<ModuleControl>,
    registry: Arc<Mutex<HashMap<CompanionId, CompanionDial>>>,
    pending_recaptures: Arc<Mutex<HashMap<CompanionId, oneshot::Sender<()>>>>,
) {
    let module = match mitos_lifecycle::ids::ModuleId::parse(&module_id) {
        Ok(module) => module,
        Err(e) => {
            error!(module = %module_id, error = %e, "not a module id; drain exiting");
            return;
        }
    };
    // One shared client per module: companions of a module typically
    // share a Worker host, so a single client pools connections to
    // it across all of them (fewer total sockets than one client per
    // companion). Auth is per-request, so sharing is safe.
    let client = match build_http_client() {
        Ok(c) => c,
        Err(e) => {
            error!(module = %module_id, error = %e, "build reqwest client failed; module drain exiting");
            return;
        }
    };

    let lane_config = LaneConfig::from_env();
    let bulk_config = pool::BulkConfig::from_env();
    let concurrency = module_drain_concurrency();
    info!(
        module = %module_id,
        lanes = lane_config.lanes,
        bulk_max = bulk_config.max,
        concurrency,
        "module drain task started"
    );

    // One status writer per INSTANCE store: each owns redb writes to its own
    // `emissions.redb`, and every spawned companion drain sends through a
    // clone of the sender belonging to the store its rows came from.
    let mut open: HashMap<InstanceKey, OpenStore> = HashMap::new();
    let mut rows = RowCache::default();
    let mut retry: HashMap<CompanionId, RetryState> = HashMap::new();
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // consume the immediate-fire first tick

    loop {
        tokio::select! {
            biased;

            _ = cancel.cancelled() => {
                shutdown_open(&mut open).await;
                return;
            }

            ctrl = control_rx.recv() => {
                let Some(ctrl) = ctrl else {
                    shutdown_open(&mut open).await;
                    return; // all senders dropped — module retired
                };
                match ctrl {
                    ModuleControl::Recapture { id, reason } => {
                        handle_recapture_frame(
                            &module_id,
                            &registry,
                            &client,
                            &pending_recaptures,
                            id,
                            reason,
                        )
                        .await;
                    }
                    ModuleControl::ReleaseInstance { key, done } => {
                        if let Some(entry) = open.remove(&key) {
                            // Drains the queue before it returns, so a
                            // status update in flight is written before the
                            // caller removes the file it would have gone to.
                            entry.status_writer.shutdown().await;
                            info!(
                                module = %module_id,
                                instance = %key.dir_name(),
                                "drain released an instance's emissions store",
                            );
                        }
                        let _ = done.send(());
                    }
                }
            }

            _ = tick.tick() => {
                reconcile_open_stores(&module, &storage, &cancel, &mut open).await;
                if let Err(e) = run_module_tick(
                    &storage,
                    &module_id,
                    &open,
                    &registry,
                    &client,
                    &mut rows,
                    &mut retry,
                    lane_config,
                    bulk_config,
                    concurrency,
                )
                .await
                {
                    // Only a store-scan failure surfaces here;
                    // per-companion transport errors are handled
                    // inside via `retry`. Log + let the next tick
                    // retry the scan.
                    warn!(module = %module_id, error = %e, "module drain scan failed; retrying next tick");
                }
            }
        }
    }
}

async fn shutdown_open(open: &mut HashMap<InstanceKey, OpenStore>) {
    for (_, entry) in open.drain() {
        entry.status_writer.shutdown().await;
    }
}

/// Bring the set of open per-instance stores into line with the instance
/// directories on disk.
///
/// **At the START of a tick and nowhere else.** Every send a tick spawns
/// holds a clone of its store's status sender and the tick joins them all
/// before it returns, so between ticks is the one moment no send can be
/// holding a writer that is about to be shut down.
async fn reconcile_open_stores(
    module: &mitos_lifecycle::ids::ModuleId,
    storage: &ModuleStorage,
    cancel: &CancellationToken,
    open: &mut HashMap<InstanceKey, OpenStore>,
) {
    let listed = match storage.list_instances(module) {
        Ok(listed) => listed,
        Err(e) => {
            warn!(
                module = %module,
                error = %e,
                "drain: list_instances failed; keeping the stores already open",
            );
            return;
        }
    };

    // Departed FIRST, and AWAITED, before anything is requeued. An upload
    // renames the directory, so `K1` disappears and `K2` appears — and `K2`'s
    // store IS `K1`'s file. Requeue it while `K1`'s writer still holds an
    // unwritten `Acked` and that row goes back to `Queued`, is listed by this
    // very tick, and is sent twice.
    let departed: Vec<InstanceKey> = open
        .keys()
        .filter(|key| !listed.contains(key))
        .cloned()
        .collect();
    for key in departed {
        if let Some(entry) = open.remove(&key) {
            entry.status_writer.shutdown().await;
            debug!(
                module = %module,
                instance = %key.dir_name(),
                "drain: instance directory gone; its store is released",
            );
        }
    }

    for key in listed {
        if open.contains_key(&key) {
            continue;
        }
        let store = match storage.emissions_store_at(&key) {
            Ok(store) => store,
            Err(e) => {
                warn!(
                    module = %module,
                    instance = %key.dir_name(),
                    error = %e,
                    "drain: open EmissionsStore failed; skipping this instance until next tick",
                );
                continue;
            }
        };
        // Crash recovery, once per store per process: flip every row left
        // `Pending` by a prior host process back to `Queued`.
        match store.requeue_all_pending(&now_rfc3339()) {
            Ok(0) => {}
            Ok(count) => info!(
                module = %module,
                instance = %key.dir_name(),
                count,
                "requeued Pending emissions on first sight of this instance's store",
            ),
            Err(e) => warn!(
                module = %module,
                instance = %key.dir_name(),
                error = %e,
                "requeue Pending failed",
            ),
        }
        let status_writer = pool::spawn_status_writer(store.clone(), cancel.clone());
        open.insert(
            key,
            OpenStore {
                store,
                status_writer,
            },
        );
    }
}

/// One instance's queued rows, as scanned this tick.
struct InstanceScan {
    key: InstanceKey,
    /// `((companion_key, client_id), rows)` — the shape the store's scan
    /// returns.
    queued: Vec<((String, String), Vec<EmissionRecord>)>,
    /// Rows delivered from this instance report their status through THIS
    /// store's writer. A clone of another's would write to the wrong file.
    status_tx: mpsc::UnboundedSender<pool::StatusUpdate>,
}

/// One companion's work for one pass: its batches, one per instance that
/// holds rows for it, run IN SEQUENCE.
///
/// Sequence, not concurrency: two apply POSTs to one companion at once is
/// exactly what one-drain-per-module exists to prevent, and a client can be
/// owed rows by more than one instance it has left.
struct CompanionWork {
    id: CompanionId,
    batches: Vec<pool::TickArgs>,
}

/// One tick's work, in the order it must run.
#[derive(Default)]
struct TickPlan {
    leftover: Vec<CompanionWork>,
    serving: Vec<CompanionWork>,
}

/// Whether a companion's backoff window is still open.
fn backing_off(retry: &HashMap<CompanionId, RetryState>, id: &CompanionId, now: Instant) -> bool {
    matches!(retry.get(id), Some(rs) if rs.next_retry_at > now)
}

/// Decide WHAT this tick sends and IN WHICH ORDER — with no network and no
/// clock beyond the `now` it is handed.
///
/// Split out of [`run_module_tick`] because every decision here is worth a
/// test and none of them needs an HTTP receiver: which companions are skipped
/// (unregistered, mid-recapture, backing off) and which of their batches are
/// `Leftover` — rows an instance the client has LEFT still owes it, older
/// than anything its current instance holds, so they go first.
#[allow(clippy::too_many_arguments)]
fn plan_module_tick(
    storage: &ModuleStorage,
    module_id: &str,
    scanned: Vec<InstanceScan>,
    dials: &HashMap<CompanionId, CompanionDial>,
    module_current: Option<&InstanceKey>,
    retry: &HashMap<CompanionId, RetryState>,
    now: Instant,
    rows: &mut RowCache,
    client: &reqwest::Client,
    lane_config: LaneConfig,
    bulk_config: pool::BulkConfig,
) -> TickPlan {
    let mut leftover: Vec<CompanionWork> = Vec::new();
    let mut serving: Vec<CompanionWork> = Vec::new();
    for scan in scanned {
        for ((companion_key, client_id), queued) in scan.queued {
            let id = CompanionId::new(module_id, &client_id, &companion_key);
            let Some(dial) = dials.get(&id) else {
                // Rows for an unregistered companion (e.g. the
                // `:unsubscribed` sentinel before a subscribe claims
                // them) — nothing to deliver to yet.
                continue;
            };
            if dial.recapturing.load(Ordering::Relaxed) {
                continue;
            }
            if backing_off(retry, &id, now) {
                continue;
            }
            let pass = match module_current {
                Some(current) => routing::drain_pass(
                    rows.get(storage, module_id, &client_id),
                    &scan.key,
                    current,
                    |other| instance_dir_of(storage, other),
                ),
                // Nothing to compare against — an unactivated module, or a
                // pointer that would not read. Fail open, as the fan-out
                // does: everything is this instance's to serve.
                None => DrainPass::Serving,
            };
            let args = pool::TickArgs {
                client: client.clone(),
                apply_url: dial.apply_url.clone(),
                bulk_url: dial.bulk_url.clone(),
                undo_url: dial.undo_url.clone(),
                bulk_capability: dial.bulk_capability.clone(),
                bulk_max: bulk_config.max,
                channel: module_id.to_string(),
                header_name: dial.header_name.clone(),
                header_value: dial.header_value.clone(),
                grouped: group_by_partition(queued),
                companion_key: dial.companion_key.clone(),
                status_tx: scan.status_tx.clone(),
                lanes: lane_config.lanes,
                now: now_rfc3339,
            };
            let into = match pass {
                DrainPass::Leftover => &mut leftover,
                DrainPass::Serving => &mut serving,
            };
            match into.iter_mut().find(|work| work.id == id) {
                Some(work) => work.batches.push(args),
                None => into.push(CompanionWork {
                    id,
                    batches: vec![args],
                }),
            }
        }
    }
    TickPlan { leftover, serving }
}

/// One drain pass for a module: scan every open store once, plan, then run
/// the leftovers to completion before the serving batches. Updates `retry`
/// from the per-companion results. Returns `Err` only if a store scan itself
/// fails.
#[allow(clippy::too_many_arguments)]
async fn run_module_tick(
    storage: &ModuleStorage,
    module_id: &str,
    open: &HashMap<InstanceKey, OpenStore>,
    registry: &Arc<Mutex<HashMap<CompanionId, CompanionDial>>>,
    client: &reqwest::Client,
    rows: &mut RowCache,
    retry: &mut HashMap<CompanionId, RetryState>,
    lane_config: LaneConfig,
    bulk_config: pool::BulkConfig,
    concurrency: usize,
) -> anyhow::Result<()> {
    let mut scanned: Vec<InstanceScan> = Vec::new();
    // With one store a failed scan WAS the tick. With several it is one
    // instance's problem: a staging store that will not read must not stop
    // the live instance beside it from delivering. Only when every scan
    // failed is there nothing to do but report it.
    let mut scan_failure = None;
    let mut scans_ok = 0_usize;
    for (key, entry) in open {
        let queued = match entry.store.list_queued_grouped_by_companion() {
            Ok(queued) => queued,
            Err(e) => {
                warn!(
                    module = %module_id,
                    instance = %key.dir_name(),
                    error = %e,
                    "drain: scan of this instance's store failed; draining the others",
                );
                scan_failure = Some(anyhow::anyhow!(
                    "list queued grouped by companion for {}: {e}",
                    key.dir_name()
                ));
                continue;
            }
        };
        scans_ok += 1;
        if queued.is_empty() {
            continue;
        }
        scanned.push(InstanceScan {
            key: key.clone(),
            queued,
            status_tx: entry.status_writer.sender(),
        });
    }
    if scanned.is_empty() {
        return match scan_failure {
            Some(e) if scans_ok == 0 => Err(e),
            _ => Ok(()),
        };
    }
    // Deterministic, so a companion owed by two instances is always served
    // in the same sequence rather than in `HashMap` iteration order.
    scanned.sort_by_key(|scan| scan.key.dir_name());

    let dials = { registry.lock().await.clone() };
    let now = Instant::now();
    // ONCE per tick, not once per row. An upload can move it mid-tick; the
    // next tick sees the new one.
    let module_current = match storage.current_instance(module_id) {
        Ok(current) => current,
        Err(e) => {
            warn!(
                module = %module_id,
                error = %e,
                "drain: current instance unreadable; treating every batch as this tick's to serve",
            );
            None
        }
    };

    let plan = plan_module_tick(
        storage,
        module_id,
        scanned,
        &dials,
        module_current.as_ref(),
        retry,
        now,
        rows,
        client,
        lane_config,
        bulk_config,
    );

    // Leftovers first, to completion, updating `retry` from their results…
    run_pass(module_id, plan.leftover, concurrency, retry).await;
    // …so a companion whose leftovers just failed is already backing off when
    // its serving batch would have been sent. Sending it anyway would put
    // rows the old instance still owed BEHIND rows the new one produced.
    let now = Instant::now();
    let serving: Vec<CompanionWork> = plan
        .serving
        .into_iter()
        .filter(|work| !backing_off(retry, &work.id, now))
        .collect();
    run_pass(module_id, serving, concurrency, retry).await;
    Ok(())
}

/// Deliver one pass's work list concurrently, bounded by `concurrency`, and
/// fold the outcomes into `retry`.
async fn run_pass(
    module_id: &str,
    work: Vec<CompanionWork>,
    concurrency: usize,
    retry: &mut HashMap<CompanionId, RetryState>,
) {
    if work.is_empty() {
        return;
    }
    let started = Instant::now();
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut set: JoinSet<(CompanionId, anyhow::Result<()>)> = JoinSet::new();
    for CompanionWork { id, batches } in work {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("module drain semaphore never closed");
        set.spawn(async move {
            let _permit = permit;
            // In sequence — see `CompanionWork`. The first failure ends this
            // companion's pass: a later batch delivered past a failed earlier
            // one would arrive out of order.
            let mut outcome = Ok(());
            for args in batches {
                outcome = pool::run_tick(args).await;
                if outcome.is_err() {
                    break;
                }
            }
            (id, outcome)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((id, Ok(()))) => {
                // Healthy — clear any backoff so the next failure
                // restarts at INITIAL_BACKOFF.
                retry.remove(&id);
            }
            Ok((id, Err(e))) => {
                let rs = retry.entry(id.clone()).or_insert(RetryState {
                    backoff: INITIAL_BACKOFF,
                    next_retry_at: started,
                });
                let used = rs.backoff;
                rs.next_retry_at = Instant::now() + used;
                rs.backoff = (rs.backoff * 2).min(MAX_BACKOFF);
                warn!(
                    module = %module_id,
                    companion_key = %id.companion_key,
                    error = %e,
                    backoff_secs = used.as_secs(),
                    "companion apply drain hit transport error; backing off"
                );
            }
            Err(join_err) => {
                warn!(module = %module_id, error = %join_err, "companion drain task panicked");
            }
        }
    }
}

/// Handle one recapture frame in the module drain loop. Sets the
/// companion's `recapturing` flag (so the tick stops delivering to
/// it), then spawns the wipe POST; on completion the flag clears
/// and — on 2xx — the `pending_recaptures` oneshot fires. The flag
/// is set synchronously in the single-threaded loop before any tick
/// can run, so an apply POST never overlaps the wipe.
async fn handle_recapture_frame(
    module_id: &str,
    registry: &Arc<Mutex<HashMap<CompanionId, CompanionDial>>>,
    client: &reqwest::Client,
    pending_recaptures: &Arc<Mutex<HashMap<CompanionId, oneshot::Sender<()>>>>,
    id: CompanionId,
    reason: Option<String>,
) {
    let dial = { registry.lock().await.get(&id).cloned() };
    let Some(dial) = dial else {
        warn!(
            module = %module_id,
            companion_key = %id.companion_key,
            "recapture frame for unregistered companion; ignoring (admin will time out)"
        );
        return;
    };
    // Pause apply delivery for this companion while the wipe runs.
    dial.recapturing.store(true, Ordering::Relaxed);

    let client = client.clone();
    let recapture_url = dial.recapture_url.clone();
    let header_name = dial.header_name.clone();
    let header_value = dial.header_value.clone();
    let recapturing = dial.recapturing.clone();
    let pending = pending_recaptures.clone();
    let module_id = module_id.to_string();
    tokio::spawn(async move {
        let result = post_recapture(
            &client,
            &recapture_url,
            header_name.as_deref(),
            header_value.as_deref(),
            &module_id,
            reason.as_deref(),
        )
        .await;
        // Resume apply delivery regardless of outcome.
        recapturing.store(false, Ordering::Relaxed);
        match result {
            Ok(()) => {
                info!(
                    module = %module_id,
                    companion_key = %id.companion_key,
                    "Recapture POST succeeded; firing pending_recaptures oneshot"
                );
                fire_recapture_ready(&pending, &id).await;
            }
            Err(e) => {
                error!(
                    module = %module_id,
                    companion_key = %id.companion_key,
                    error = %e,
                    "Recapture POST failed; admin endpoint will time out"
                );
                // Don't fire the oneshot — admin endpoint's timeout
                // is the safer failure mode.
            }
        }
    });
}

/// Group a companion's queued rows by `partition_key`, id-ordered
/// within each group (input is already id-ordered from the scan).
/// `BTreeMap` key order puts the empty (global) lane first — the
/// shape `pool::run_tick` expects.
fn group_by_partition(rows: Vec<EmissionRecord>) -> Vec<(Vec<u8>, Vec<EmissionRecord>)> {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<Vec<u8>, Vec<EmissionRecord>> = BTreeMap::new();
    for row in rows {
        groups
            .entry(row.partition_key.clone())
            .or_default()
            .push(row);
    }
    groups.into_iter().collect()
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

/// Derive the bulk-apply URL from the resolved per-row apply URL by
/// splicing `-bulk` onto the path segment, before any query string.
/// `…/_internal/apply-collection-holders?policy_id=X` →
/// `…/_internal/apply-collection-holders-bulk?policy_id=X`.
fn bulk_url_from_apply(apply_url: &str) -> String {
    match apply_url.split_once('?') {
        Some((path, query)) => format!("{path}-bulk?{query}"),
        None => format!("{apply_url}-bulk"),
    }
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

#[cfg(test)]
mod tests {
    use super::{bulk_url_from_apply, group_by_partition};
    use crate::emissions::{EmissionRecord, EmissionStatus};
    use mitos_protocol::ChainPoint;

    fn row(id: u64, partition_key: &[u8]) -> EmissionRecord {
        EmissionRecord {
            id,
            matched_at: "unix:0".into(),
            sent_at: None,
            chain_point: ChainPoint::Specific(id, "h".into()),
            channel: "collection-holders".into(),
            payload: vec![],
            companion_id: "policy_a".into(),
            client_id: "client_x".into(),
            status: EmissionStatus::Queued,
            status_at: "unix:0".into(),
            error: None,
            partition_key: partition_key.to_vec(),
            is_undo: false,
        }
    }

    // -----------------------------------------------------------------
    // Planning: what a tick sends and in which order. No network, no
    // clock beyond the `Instant` it is handed.
    // -----------------------------------------------------------------

    mod planning {
        use super::super::*;
        use mitos_lifecycle::ids::{ModuleId, Sha256Hex};
        use std::collections::HashSet;

        const MODULE: &str = "ownership-indexer";
        const CLIENT: &str = "ownership.cnft.dev";
        const COMPANION: &str = "policy_a";

        struct Fixture {
            _tmp: tempfile::TempDir,
            storage: ModuleStorage,
            current: InstanceKey,
            other: InstanceKey,
            client: reqwest::Client,
        }

        fn fixture() -> Fixture {
            let tmp = tempfile::tempdir().unwrap();
            let storage = ModuleStorage::new(tmp.path());
            let wasm = b"planner fixture";
            storage
                .activate(
                    &crate::companions::testing::manifest_for(MODULE, wasm),
                    wasm,
                )
                .unwrap();
            let current = storage.current_instance(MODULE).unwrap().unwrap();
            let other = plant(&storage, b"a second build");
            Fixture {
                _tmp: tmp,
                storage,
                current,
                other,
                client: build_http_client().unwrap(),
            }
        }

        fn plant(storage: &ModuleStorage, wasm: &[u8]) -> InstanceKey {
            let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
            storage.put_artifact(&manifest, wasm).unwrap();
            let key = InstanceKey {
                module: ModuleId::parse(MODULE).unwrap(),
                wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(wasm)).unwrap(),
                config: None,
            };
            storage.write_instance_marker(&key).unwrap();
            key
        }

        fn dial() -> (CompanionId, HashMap<CompanionId, CompanionDial>) {
            let id = CompanionId::new(MODULE, CLIENT, COMPANION);
            let dial = CompanionDial {
                id: id.clone(),
                companion_key: COMPANION.to_owned(),
                apply_url: "http://127.0.0.1:1/apply".to_owned(),
                bulk_url: "http://127.0.0.1:1/apply-bulk".to_owned(),
                recapture_url: "http://127.0.0.1:1/recapture".to_owned(),
                undo_url: "http://127.0.0.1:1/undo".to_owned(),
                header_name: None,
                header_value: None,
                bulk_capability: Arc::new(AtomicU8::new(pool::BULK_UNKNOWN)),
                recapturing: Arc::new(AtomicBool::new(false)),
            };
            (id.clone(), HashMap::from([(id, dial)]))
        }

        /// One instance's scan result. The status receiver is dropped: these
        /// tests only PLAN, and a send on a closed channel is the same
        /// best-effort no-op a shut-down writer gives in production.
        fn scan(key: &InstanceKey, ids: &[u64]) -> InstanceScan {
            let (tx, _rx) = mpsc::unbounded_channel();
            InstanceScan {
                key: key.clone(),
                queued: vec![(
                    (COMPANION.to_owned(), CLIENT.to_owned()),
                    ids.iter().map(|id| super::row(*id, b"")).collect(),
                )],
                status_tx: tx,
            }
        }

        fn place(storage: &ModuleStorage, instance: &InstanceKey) {
            use mitos_lifecycle::client_move::{ClientRow, Placement};
            use mitos_lifecycle::ids::{ClientId, ClientKey, Namespace, Revision};
            let row = ClientRow::adopt(
                ClientKey {
                    namespace: Namespace::parse("prod").unwrap(),
                    client: ClientId::parse(CLIENT).unwrap(),
                },
                ModuleId::parse(MODULE).unwrap(),
                Placement {
                    instance: instance.clone(),
                    revision: Revision(0),
                },
            );
            storage.write_row(MODULE, &row).unwrap();
        }

        fn ids_of(work: &[CompanionWork]) -> Vec<Vec<u64>> {
            work.iter()
                .flat_map(|w| w.batches.iter())
                .map(|args| {
                    args.grouped
                        .iter()
                        .flat_map(|(_, rows)| rows.iter().map(|r| r.id))
                        .collect()
                })
                .collect()
        }

        /// A client owed rows by the instance it has LEFT gets those first:
        /// they are older than anything its current instance holds.
        #[test]
        fn leftover_batches_are_planned_ahead_of_serving_ones() {
            let f = fixture();
            // The row places the client on the OTHER instance, so what sits
            // in the current instance's store is a leftover.
            place(&f.storage, &f.other);
            let (_, dials) = dial();
            let scanned = vec![scan(&f.current, &[1, 2]), scan(&f.other, &[7, 8])];

            let plan = plan_module_tick(
                &f.storage,
                MODULE,
                scanned,
                &dials,
                Some(&f.current),
                &HashMap::new(),
                Instant::now(),
                &mut RowCache::default(),
                &f.client,
                LaneConfig::default(),
                pool::BulkConfig { max: 50 },
            );

            assert_eq!(ids_of(&plan.leftover), vec![vec![1, 2]]);
            assert_eq!(ids_of(&plan.serving), vec![vec![7, 8]]);
        }

        /// A companion inside its backoff window is skipped entirely — not
        /// moved to the other pass, which would send it anyway.
        #[test]
        fn a_companion_backing_off_is_in_neither_list() {
            let f = fixture();
            place(&f.storage, &f.other);
            let (id, dials) = dial();
            let scanned = vec![scan(&f.current, &[1]), scan(&f.other, &[2])];
            let now = Instant::now();
            let retry = HashMap::from([(
                id,
                RetryState {
                    backoff: INITIAL_BACKOFF,
                    next_retry_at: now + Duration::from_secs(30),
                },
            )]);

            let plan = plan_module_tick(
                &f.storage,
                MODULE,
                scanned,
                &dials,
                Some(&f.current),
                &retry,
                now,
                &mut RowCache::default(),
                &f.client,
                LaneConfig::default(),
                pool::BulkConfig { max: 50 },
            );

            assert!(plan.leftover.is_empty());
            assert!(plan.serving.is_empty());
        }

        /// No pointer to compare against — an unactivated module, or one
        /// whose `current.toml` would not read. Fail OPEN, as the fan-out
        /// does: silence is worse than a duplicate a companion can absorb.
        #[test]
        fn an_unresolvable_current_instance_plans_everything_as_serving() {
            let f = fixture();
            place(&f.storage, &f.other);
            let (_, dials) = dial();
            let scanned = vec![scan(&f.current, &[1]), scan(&f.other, &[2])];

            let plan = plan_module_tick(
                &f.storage,
                MODULE,
                scanned,
                &dials,
                None,
                &HashMap::new(),
                Instant::now(),
                &mut RowCache::default(),
                &f.client,
                LaneConfig::default(),
                pool::BulkConfig { max: 50 },
            );

            assert!(plan.leftover.is_empty());
            assert_eq!(
                ids_of(&plan.serving)
                    .into_iter()
                    .flatten()
                    .collect::<HashSet<_>>(),
                HashSet::from([1, 2]),
            );
        }

        /// Rows for a companion nobody has registered — the `:unsubscribed`
        /// sentinel before a subscribe claims them — are not work.
        #[test]
        fn an_unregistered_companion_is_not_planned() {
            let f = fixture();
            let scanned = vec![scan(&f.current, &[1])];
            let plan = plan_module_tick(
                &f.storage,
                MODULE,
                scanned,
                &HashMap::new(),
                Some(&f.current),
                &HashMap::new(),
                Instant::now(),
                &mut RowCache::default(),
                &f.client,
                LaneConfig::default(),
                pool::BulkConfig { max: 50 },
            );
            assert!(plan.leftover.is_empty() && plan.serving.is_empty());
        }

        /// Two instances a client has left both owe it rows. They go in ONE
        /// `CompanionWork`, run in sequence — two concurrent POSTs to one
        /// companion is what one-drain-per-module exists to prevent.
        #[test]
        fn batches_from_two_instances_are_one_companions_work() {
            let f = fixture();
            let third = plant(&f.storage, b"a third build");
            place(&f.storage, &third);
            let (_, dials) = dial();
            let scanned = vec![scan(&f.current, &[1]), scan(&f.other, &[2])];

            let plan = plan_module_tick(
                &f.storage,
                MODULE,
                scanned,
                &dials,
                Some(&f.current),
                &HashMap::new(),
                Instant::now(),
                &mut RowCache::default(),
                &f.client,
                LaneConfig::default(),
                pool::BulkConfig { max: 50 },
            );

            assert_eq!(plan.leftover.len(), 1, "one companion, one work item");
            assert_eq!(plan.leftover[0].batches.len(), 2);
            assert!(plan.serving.is_empty());
        }
    }

    #[test]
    fn group_by_partition_buckets_and_preserves_id_order() {
        // Mixed partition keys, interleaved ids. Within each key the
        // rows must stay in id order; the empty (global) key sorts
        // first under BTreeMap order.
        let rows = vec![
            row(1, b""),
            row(2, b"policy_a"),
            row(3, b"policy_a"),
            row(4, b""),
            row(5, b"policy_b"),
        ];
        let grouped = group_by_partition(rows);
        assert_eq!(grouped.len(), 3);

        assert_eq!(grouped[0].0, b"" as &[u8]);
        assert_eq!(
            grouped[0].1.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![1, 4],
            "global lane stays id-ordered"
        );

        assert_eq!(grouped[1].0, b"policy_a" as &[u8]);
        assert_eq!(
            grouped[1].1.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![2, 3]
        );

        assert_eq!(grouped[2].0, b"policy_b" as &[u8]);
        assert_eq!(
            grouped[2].1.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![5]
        );
    }

    #[test]
    fn bulk_url_splices_before_query() {
        assert_eq!(
            bulk_url_from_apply(
                "https://ownership.dev.cnft.dev/_internal/apply-collection-holders?policy_id=abc"
            ),
            "https://ownership.dev.cnft.dev/_internal/apply-collection-holders-bulk?policy_id=abc"
        );
    }

    #[test]
    fn bulk_url_no_query_appends() {
        assert_eq!(
            bulk_url_from_apply("https://x/_internal/apply-collection-holders"),
            "https://x/_internal/apply-collection-holders-bulk"
        );
    }

    #[test]
    fn bulk_url_preserves_multi_param_query() {
        assert_eq!(
            bulk_url_from_apply("https://x/_internal/apply-mod?key=k&policy_id=p"),
            "https://x/_internal/apply-mod-bulk?key=k&policy_id=p"
        );
    }

    // -----------------------------------------------------------------
    // Delivery, against a real receiver.
    //
    // Nothing in this crate stood up an HTTP receiver before, so the drain
    // loop itself — the thing that actually POSTs — had no test at all. One
    // small `axum` router is enough, and it answers the questions that the
    // planner cannot: what ARRIVED, in what ORDER, and how many times.
    // -----------------------------------------------------------------

    mod delivery {
        use super::super::*;
        use mitos_lifecycle::ids::{ModuleId, Sha256Hex};
        use std::collections::HashSet;
        use std::sync::Mutex as StdMutex;

        const MODULE: &str = "ownership-indexer";
        const CLIENT: &str = "ownership.cnft.dev";

        /// One POST, as the receiver saw it.
        #[derive(Debug, Clone)]
        struct Delivery {
            #[allow(dead_code)] // in the failure message, not in an assertion
            path: String,
            ids: Vec<u64>,
            /// The rows' payloads, read as text. **Emission ids are unique
            /// per STORE, not per module** — two instances both start at 1 —
            /// so an id cannot say which instance a row came from. The
            /// payload can, and every test here labels its rows.
            labels: Vec<String>,
        }

        /// A companion, as a receiver.
        ///
        /// Records each POST in ARRIVAL ORDER — order is the whole point of
        /// the leftover/serving split, and a set would not show it.
        /// `failing` makes one companion answer 500, which is what a Worker
        /// having a bad day looks like to the drain.
        #[derive(Clone, Default)]
        struct Receiver(Arc<StdMutex<ReceiverState>>);

        #[derive(Default)]
        struct ReceiverState {
            seen: Vec<Delivery>,
            failing: HashSet<String>,
            /// Companions that take `HANG` to answer — a Worker that has
            /// stopped responding, which holds the drain's tick open.
            hanging: HashSet<String>,
        }

        const HANG: Duration = Duration::from_secs(5);

        impl Receiver {
            fn hang(&self, companion_key: &str) {
                self.0
                    .lock()
                    .unwrap()
                    .hanging
                    .insert(companion_key.to_owned());
            }

            fn recover(&self, companion_key: &str) {
                self.0.lock().unwrap().hanging.remove(companion_key);
            }

            fn fail(&self, companion_key: &str) {
                self.0
                    .lock()
                    .unwrap()
                    .failing
                    .insert(companion_key.to_owned());
            }

            fn seen(&self) -> Vec<Delivery> {
                self.0.lock().unwrap().seen.clone()
            }

            /// Every row that arrived, by label, in arrival order —
            /// duplicates included, because "exactly once" is a claim about
            /// the duplicates.
            fn labels(&self) -> Vec<String> {
                self.seen().into_iter().flat_map(|d| d.labels).collect()
            }

            /// Every emission id that arrived, in arrival order. Only
            /// meaningful within ONE store.
            fn ids(&self) -> Vec<u64> {
                self.seen().into_iter().flat_map(|d| d.ids).collect()
            }

            /// Serve on an ephemeral port; returns the dial-back template.
            async fn serve(&self) -> String {
                let app = axum::Router::new()
                    .fallback(axum::routing::any(handle))
                    .with_state(self.clone());
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                tokio::spawn(async move {
                    let _ = axum::serve(listener, app).await;
                });
                format!("http://{addr}/_internal/{{op}}-{{target}}?key={{key}}")
            }
        }

        async fn handle(
            axum::extract::State(receiver): axum::extract::State<Receiver>,
            axum::extract::RawQuery(query): axum::extract::RawQuery,
            request: axum::extract::Request,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            let path = request.uri().path().to_owned();
            let companion_key = query
                .unwrap_or_default()
                .split('&')
                .find_map(|pair| pair.strip_prefix("key=").map(|k| k.to_owned()))
                .unwrap_or_default();
            let body = axum::body::to_bytes(request.into_body(), usize::MAX)
                .await
                .unwrap_or_default();

            // Bulk first: the drain tries it before falling back per-row, so
            // a receiver that only understood one shape would silently test
            // the other path.
            let label = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
            let (ids, labels, bulk) = match mitos_protocol::decode_apply_bulk(&body) {
                Ok(batch) => (
                    batch.emissions.iter().map(|e| e.emission_id).collect(),
                    batch.emissions.iter().map(|e| label(&e.change)).collect(),
                    true,
                ),
                Err(_) => match mitos_protocol::decode_apply(&body) {
                    Ok(one) => (vec![one.emission_id], vec![label(&one.change)], false),
                    // A recapture or undo body — recorded with no rows, so
                    // the arrival order still shows it.
                    Err(_) => (Vec::new(), Vec::new(), false),
                },
            };
            let hangs = {
                let mut state = receiver.0.lock().unwrap();
                state.seen.push(Delivery {
                    path,
                    ids: ids.clone(),
                    labels,
                });
                if state.failing.contains(&companion_key) {
                    return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "no").into_response();
                }
                state.hanging.contains(&companion_key)
            };
            if hangs {
                tokio::time::sleep(HANG).await;
            }
            if bulk {
                let response = mitos_protocol::ApplyBulkResponse {
                    results: ids
                        .iter()
                        .map(|id| mitos_protocol::BulkEmissionResult {
                            emission_id: *id,
                            applied: true,
                            error: None,
                        })
                        .collect(),
                };
                return (
                    axum::http::StatusCode::OK,
                    mitos_protocol::encode_apply_bulk_response(&response).unwrap(),
                )
                    .into_response();
            }
            (axum::http::StatusCode::OK, "ok").into_response()
        }

        struct Harness {
            _tmp: tempfile::TempDir,
            storage: ModuleStorage,
            dialer: CompanionDialer,
            receiver: Receiver,
            url: String,
            current: InstanceKey,
        }

        async fn harness() -> Harness {
            let tmp = tempfile::tempdir().unwrap();
            let storage = ModuleStorage::new(tmp.path());
            let wasm = b"delivery fixture";
            storage
                .activate(
                    &crate::companions::testing::manifest_for(MODULE, wasm),
                    wasm,
                )
                .unwrap();
            let current = storage.current_instance(MODULE).unwrap().unwrap();
            let receiver = Receiver::default();
            let url = receiver.serve().await;
            Harness {
                _tmp: tmp,
                dialer: CompanionDialer::new(storage.clone(), crate::admin::AuthToken(None), None),
                storage,
                receiver,
                url,
                current,
            }
        }

        impl Harness {
            /// Register a companion — which is also what spawns the module's
            /// drain task, exactly as a subscribe does.
            async fn subscribe(&self, companion_key: &str) {
                self.dialer
                    .register(SubscribeRequest {
                        targets: vec![SubscribeTarget::Module {
                            name: MODULE.to_owned(),
                        }],
                        companion_key: companion_key.to_owned(),
                        client_id: CLIENT.to_owned(),
                        resume_from: None,
                        interests: Vec::new(),
                        dial_back: Some(mitos_protocol::DialBackOverride {
                            url: Some(self.url.clone()),
                            auth_header: None,
                            auth_value: None,
                        }),
                        announced: Vec::new(),
                    })
                    .await;
            }

            fn plant(&self, wasm: &[u8]) -> InstanceKey {
                let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
                self.storage.put_artifact(&manifest, wasm).unwrap();
                let key = InstanceKey {
                    module: ModuleId::parse(MODULE).unwrap(),
                    wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(wasm)).unwrap(),
                    config: None,
                };
                self.storage.write_instance_marker(&key).unwrap();
                key
            }

            /// Queue one row in an instance's store. `label` is the payload,
            /// and the only thing that identifies the row across stores.
            fn queue(&self, instance: &InstanceKey, companion_key: &str, label: &str) -> u64 {
                self.append(
                    instance,
                    companion_key,
                    label,
                    crate::emissions::EmissionStatus::Queued,
                )
            }

            fn append(
                &self,
                instance: &InstanceKey,
                companion_key: &str,
                label: &str,
                status: crate::emissions::EmissionStatus,
            ) -> u64 {
                self.storage
                    .emissions_store_at(instance)
                    .unwrap()
                    .append(
                        companion_key,
                        CLIENT,
                        MODULE,
                        mitos_protocol::ChainPoint::Slot(1),
                        label.as_bytes().to_vec(),
                        Vec::new(),
                        status,
                        "unix:0",
                    )
                    .unwrap()
            }

            fn place(&self, instance: &InstanceKey) {
                use mitos_lifecycle::client_move::{ClientRow, Placement};
                use mitos_lifecycle::ids::{ClientId, ClientKey, Namespace, Revision};
                let row = ClientRow::adopt(
                    ClientKey {
                        namespace: Namespace::parse("prod").unwrap(),
                        client: ClientId::parse(CLIENT).unwrap(),
                    },
                    ModuleId::parse(MODULE).unwrap(),
                    Placement {
                        instance: instance.clone(),
                        revision: Revision(0),
                    },
                );
                self.storage.write_row(MODULE, &row).unwrap();
            }
        }

        /// Poll until `cond` holds, or fail. The drain ticks once a second,
        /// so anything here is a few ticks; a fixed sleep would encode a
        /// guess and read as a logic bug when the guess was wrong.
        async fn wait_for(what: &str, mut cond: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if cond() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            panic!("timed out waiting for: {what}");
        }

        /// A few ticks' worth of "and then nothing else happened". Used only
        /// for negative assertions, where there is nothing to poll on.
        async fn settle() {
            tokio::time::sleep(POLL_INTERVAL * 3).await;
        }

        /// **The baseline.** One instance, one companion: every queued row
        /// arrives, in id order, exactly once. If this ever fails, a deploy
        /// of the instance-addressed drain changes who receives what.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn with_one_instance_the_drain_delivers_as_it_always_did() {
            let h = harness().await;
            let ids: Vec<u64> = (0..5)
                .map(|n| h.queue(&h.current, "policy_a", &format!("row-{n}")))
                .collect();
            h.subscribe("policy_a").await;

            wait_for("every row to arrive", || {
                h.receiver.ids().len() >= ids.len()
            })
            .await;
            settle().await;

            assert_eq!(h.receiver.ids(), ids, "delivered out of order, or twice");
            assert_eq!(
                h.receiver.labels(),
                vec!["row-0", "row-1", "row-2", "row-3", "row-4"],
            );
        }

        /// The whole point of WO-06's storage split, seen from the outside:
        /// a second instance keeps its own emissions log, and nothing was
        /// reading it.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn rows_in_a_second_instances_store_are_delivered() {
            let h = harness().await;
            let second = h.plant(b"a second build");
            // The client has moved onto the second instance, so that store is
            // the one serving it.
            h.place(&second);
            for n in 0..3 {
                h.queue(&second, "policy_a", &format!("second-{n}"));
            }
            h.subscribe("policy_a").await;

            wait_for("the second instance's rows to arrive", || {
                h.receiver.labels().len() >= 3
            })
            .await;
            settle().await;

            assert_eq!(
                h.receiver.labels(),
                vec!["second-0", "second-1", "second-2"],
            );
        }

        /// Rows the instance a client has LEFT still owes it are older than
        /// anything its new instance holds, so they go first. Asserted on the
        /// ORDER the receiver saw, not the set.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn leftovers_are_sent_before_the_serving_instance_speaks() {
            let h = harness().await;
            let second = h.plant(b"a second build");
            h.place(&second);
            // Queued on the instance the client has left…
            for n in 0..2 {
                h.queue(&h.current, "policy_a", &format!("left-{n}"));
            }
            // …and on the one now serving it. The two stores number their
            // rows independently — both start at 1 — so only the payload can
            // say which instance a delivery came from.
            for n in 0..2 {
                h.queue(&second, "policy_a", &format!("serve-{n}"));
            }
            h.subscribe("policy_a").await;

            wait_for("all four rows to arrive", || h.receiver.labels().len() >= 4).await;
            settle().await;

            assert_eq!(
                h.receiver.labels(),
                vec!["left-0", "left-1", "serve-0", "serve-1"],
                "the new instance spoke before the old one had finished",
            );
        }

        /// A companion whose leftovers failed is already backing off when its
        /// serving batch would have been sent. Sending it anyway would put
        /// rows the old instance still owed BEHIND rows the new one produced.
        ///
        /// Asserted with a receiver that fails FOREVER, so the property holds
        /// over every tick in the window rather than racing one of them.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_companion_whose_leftovers_failed_is_not_sent_its_serving_batch_that_tick() {
            let h = harness().await;
            let second = h.plant(b"a second build");
            h.place(&second);
            h.queue(&h.current, "policy_a", "left");
            h.queue(&second, "policy_a", "serve");
            h.receiver.fail("policy_a");
            h.subscribe("policy_a").await;

            wait_for("the leftover to be attempted", || {
                h.receiver.labels().iter().any(|l| l == "left")
            })
            .await;
            settle().await;

            assert!(
                !h.receiver.labels().iter().any(|l| l == "serve"),
                "the serving batch was sent while the leftovers were still failing: {:?}",
                h.receiver.seen(),
            );
        }

        /// An upload RENAMES the instance directory: `K1` disappears and `K2`
        /// appears, with the same file behind it. The drain must notice, and
        /// must not resend what `K1`'s writer had already acked.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn an_upload_mid_drain_keeps_delivering() {
            let h = harness().await;
            for n in 0..3 {
                h.queue(&h.current, "policy_a", &format!("before-{n}"));
            }
            h.subscribe("policy_a").await;
            wait_for("the pre-upload rows to arrive", || {
                h.receiver.labels().len() >= 3
            })
            .await;

            // K1 → K2. `write_config` is the same repoint `activate` does,
            // without needing a second artifact.
            h.storage.write_config(MODULE, b"\xa0").unwrap();
            let after_key = h.storage.current_instance(MODULE).unwrap().unwrap();
            assert_ne!(after_key, h.current);
            for n in 0..3 {
                h.queue(&after_key, "policy_a", &format!("after-{n}"));
            }

            wait_for("the post-upload rows to arrive", || {
                h.receiver.labels().len() >= 6
            })
            .await;
            settle().await;

            assert_eq!(
                h.receiver.labels(),
                vec![
                    "before-0", "before-1", "before-2", "after-0", "after-1", "after-2"
                ],
                "a row was resent across the rename, or lost",
            );
        }

        /// `evict_instance` asks for this and waits: a status writer holding
        /// a clone of the store is what makes the directory removal fail.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn releasing_an_instance_lets_its_directory_be_removed() {
            let h = harness().await;
            let doomed = h.plant(b"a second build");
            h.subscribe("policy_a").await;
            // Get the drain to open BOTH stores, so there is something to
            // release — an unopened store would make this vacuous.
            h.queue(&h.current, "policy_a", "on-current");
            h.place(&doomed);
            h.queue(&doomed, "policy_a", "on-doomed");
            wait_for("both stores to be drained", || {
                let labels = h.receiver.labels();
                labels.iter().any(|l| l == "on-current") && labels.iter().any(|l| l == "on-doomed")
            })
            .await;

            assert_eq!(h.dialer.release_instance(&doomed).await, Released::Done);
            h.storage.close_emissions_at(&doomed);
            h.storage
                .remove_instance_dir(&doomed)
                .expect("the drain was still holding the store");
            assert!(!h.storage.instance_dir(&doomed).is_dir());

            // …and the sibling keeps being served. A release that took the
            // whole drain down would pass every assertion above.
            h.storage
                .remove_row(
                    MODULE,
                    &mitos_lifecycle::ids::ClientId::parse(CLIENT).unwrap(),
                )
                .unwrap();
            h.queue(&h.current, "policy_a", "after-release");
            wait_for("the sibling to keep delivering", || {
                h.receiver.labels().iter().any(|l| l == "after-release")
            })
            .await;
        }

        /// The caller is a `Reap` under the module's lifecycle lock, and every
        /// subscribe to that module takes the same lock. The drain only lets
        /// go BETWEEN ticks, and a tick lasts as long as its slowest send — so
        /// an unbounded wait would hang a subscribe behind somebody else's
        /// unresponsive Worker.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn a_release_does_not_wait_out_a_hanging_companion() {
            let h = harness().await;
            let doomed = h.plant(b"a second build");
            h.subscribe("policy_a").await;
            h.place(&doomed);
            h.queue(&doomed, "policy_a", "opens-the-store");
            wait_for("the doomed store to be opened", || {
                h.receiver.labels().iter().any(|l| l == "opens-the-store")
            })
            .await;

            // Hold a tick open: the companion accepts the POST and then says
            // nothing for `HANG`.
            h.receiver.hang("policy_a");
            h.queue(&doomed, "policy_a", "hangs");
            wait_for("the hanging send to be in flight", || {
                h.receiver.labels().iter().any(|l| l == "hangs")
            })
            .await;

            let asked = Instant::now();
            assert_eq!(
                h.dialer.release_instance(&doomed).await,
                Released::StillBusy
            );
            assert!(
                asked.elapsed() < HANG,
                "the release waited out the hanging send: {:?}",
                asked.elapsed(),
            );

            // The request was not dropped: once the tick ends the drain
            // honours it, so the reap's RETRY is answered at once.
            h.receiver.recover("policy_a");
            tokio::time::sleep(HANG).await;
            assert_eq!(h.dialer.release_instance(&doomed).await, Released::Done);
        }

        /// The requeue is crash recovery — "rows a PRIOR host process left
        /// `Pending`" — so it runs once per store, on first sight. Every tick
        /// would flip rows that THIS process has in flight back to `Queued`
        /// and send them twice.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn pending_rows_are_requeued_once_per_store_not_once_per_tick() {
            let h = harness().await;
            // Left behind by a prior process.
            h.append(
                &h.current,
                "policy_a",
                "stale",
                crate::emissions::EmissionStatus::Pending,
            );
            h.subscribe("policy_a").await;
            wait_for("the stale Pending row to be requeued and sent", || {
                h.receiver.labels().iter().any(|l| l == "stale")
            })
            .await;

            // A row that goes Pending now belongs to this process's status
            // writer. Nothing may take it back.
            let in_flight = h.append(
                &h.current,
                "policy_a",
                "in-flight",
                crate::emissions::EmissionStatus::Pending,
            );
            settle().await;

            assert!(
                !h.receiver.labels().iter().any(|l| l == "in-flight"),
                "a Pending row this process owns was requeued by a later tick",
            );
            assert_eq!(
                h.storage
                    .emissions_store_at(&h.current)
                    .unwrap()
                    .list_filtered(|r| r.id == in_flight)
                    .unwrap()[0]
                    .status,
                crate::emissions::EmissionStatus::Pending,
            );
        }

        /// Rows for a companion nobody registered sit where they are.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn an_unregistered_companions_rows_are_not_delivered() {
            let h = harness().await;
            h.queue(
                &h.current,
                crate::emissions::UNSUBSCRIBED_COMPANION_ID,
                "sentinel",
            );
            h.queue(&h.current, "policy_a", "mine");
            h.subscribe("policy_a").await;

            wait_for("my row to arrive", || {
                h.receiver.labels().iter().any(|l| l == "mine")
            })
            .await;
            settle().await;

            assert_eq!(h.receiver.labels(), vec!["mine"]);
        }
    }
}
