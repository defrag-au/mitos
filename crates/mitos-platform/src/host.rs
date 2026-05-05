//! `ModuleHost` — running-instance lifecycle for wasm modules.
//!
//! Owns one follower task per active module. Wires
//! `ModuleStorage` (artifact + cursor persistence) +
//! `ModuleRegistry` (wasmtime engine, linker, instantiation) +
//! a caller-provided `TipSubscription` factory so the host
//! stays decoupled from any specific dolos `Domain` impl.
//!
//! Lifecycle operations:
//! - `start(id)` — read `current.wasm`, instantiate, spawn
//!   follower against a fresh `TipSubscription`, resume cursor
//!   from `read_cursor` if present
//! - `stop(id)` — cancel the follower, await its termination,
//!   release the slot
//! - `replace(id)` — atomic stop-then-start (used after a
//!   successful upload to swap in the new sha)
//! - `list()` — running module ids
//!
//! Cursor persistence is best-effort via `ModuleStorage::write_cursor`
//! fired from the driver's checkpoint hook on every successful
//! advance. Crash-safe redb-backed cursor + WAL lands when
//! `store.rs` is vendored from Balius.
//!
//! V1.5 scope (this file): the lifecycle primitive. Production
//! integration with mitos's bundle binary + the existing
//! replication WS happens separately — `ModuleHost` is generic
//! over the `TipSubscription` source so a future mitos bundle
//! can wire it to dolos's tip broadcast and the existing
//! event-fan-out infrastructure without touching this code.

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::{ChainPoint, TipSubscription};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::driver::Driver;
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::registry::{ModuleRegistry, ResourceBudget};
use crate::storage::ModuleStorage;
use crate::{PlatformError, PlatformResult, follower::run_chain_follower};

/// Object-safe lifecycle surface for the admin router.
/// `ModuleHost<S>` is generic over the `TipSubscription` type;
/// the admin router doesn't care about that — it just needs
/// "start a module" / "stop a module" / "what's running."
/// Trait keeps the generic out of the route signatures.
#[async_trait::async_trait]
pub trait ModuleHostHandle: Send + Sync {
    async fn replace(&self, id: &str) -> PlatformResult<()>;
    async fn stop(&self, id: &str) -> PlatformResult<()>;
    async fn stop_all(&self);
    async fn list_running(&self) -> Vec<String>;
}

#[async_trait::async_trait]
impl<S> ModuleHostHandle for ModuleHost<S>
where
    S: TipSubscription + 'static,
{
    async fn replace(&self, id: &str) -> PlatformResult<()> {
        ModuleHost::replace(self, id).await
    }
    async fn stop(&self, id: &str) -> PlatformResult<()> {
        ModuleHost::stop(self, id).await
    }
    async fn stop_all(&self) {
        ModuleHost::stop_all(self).await
    }
    async fn list_running(&self) -> Vec<String> {
        ModuleHost::list(self).await
    }
}

/// Factory for fresh `TipSubscription`s. Called once per
/// `start`/`replace` to spin up an isolated subscription for
/// each follower.
///
/// Takes the persisted cursor (if any) so the production wiring
/// can choose: resume from cursor on restart, or fall back to
/// current chain tip on fresh deploy. Passing `None` to
/// `dolos::Domain::watch_tip` triggers a full WAL-replay from
/// the WAL's earliest retained slot — wrong default for
/// wasm-module followers, which want live-tail semantics with
/// backfill happening via `read_utxos` data-plane queries.
pub type SubscriptionFactory<S> = Arc<dyn Fn(Option<ChainPoint>) -> S + Send + Sync>;

/// Factory for fresh in-memory KVs. V1.5 default; v2 will use
/// the redb-backed `ModuleKv::open_redb` per module.
pub type KvFactory = Arc<dyn Fn(&str) -> state_kv::ModuleKv + Send + Sync>;

/// Factory for fresh `EventSink` pairs. Each follower gets one;
/// the host wires the receiving end into its preferred fan-out
/// (replication WS, queue, log). V1.5 just stashes them on the
/// slot for callers to drain manually.
pub type EmitterFactory = Arc<
    dyn Fn() -> (
            emit::EventSink,
            tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
        ) + Send
        + Sync,
>;

/// Per-running-module state held inside the host. Cancel the
/// `cancel` token to cooperatively stop the follower; await
/// `task` to confirm it's actually done before doing anything
/// state-mutating.
struct RunningSlot {
    sha: String,
    task: JoinHandle<PlatformResult<()>>,
    cancel: CancellationToken,
    /// Drain task that pulls from the module's `events_rx`
    /// mpsc and appends one `Queued` row per registered
    /// companion to the per-module `EmissionsStore`. Owned
    /// alongside the follower task so `stop()` can cancel
    /// both in lockstep.
    drain_task: JoinHandle<()>,
}

/// Running-module lifecycle manager.
///
/// `S` is the `TipSubscription` type the host pumps from. Same
/// concrete type for every running slot — production wires
/// `dolos::adapters::DomainAdapter::TipSubscription`, tests use
/// a `FakeTipSubscription` mpsc shim.
pub struct ModuleHost<S>
where
    S: TipSubscription,
{
    storage: ModuleStorage,
    /// Shared wasmtime engine. Built once; reused across all
    /// running modules + replacement instances.
    engine: wasmtime::Engine,
    data_plane: Arc<dyn DataPlaneFacade>,
    subscription_factory: SubscriptionFactory<S>,
    kv_factory: KvFactory,
    emitter_factory: EmitterFactory,
    budget: ResourceBudget,
    slots: Arc<Mutex<HashMap<String, RunningSlot>>>,
}

impl<S> ModuleHost<S>
where
    S: TipSubscription + 'static,
{
    pub fn new(
        storage: ModuleStorage,
        engine: wasmtime::Engine,
        data_plane: Arc<dyn DataPlaneFacade>,
        subscription_factory: SubscriptionFactory<S>,
        kv_factory: KvFactory,
        emitter_factory: EmitterFactory,
        budget: ResourceBudget,
    ) -> Self {
        Self {
            storage,
            engine,
            data_plane,
            subscription_factory,
            kv_factory,
            emitter_factory,
            budget,
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start (or restart) the module. If a slot already exists,
    /// it's stopped first — `start` is the unified
    /// "establish state where this module is running against
    /// the chain" call. `replace` is an alias for callers
    /// reading at admin-route level.
    pub async fn start(&self, id: &str) -> PlatformResult<()> {
        // Stop any existing slot first; the new instance must
        // own the resource table + KV + emitter exclusively.
        self.stop(id).await?;

        // Read manifest + current.wasm.
        let manifest = self
            .storage
            .read_manifest(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;
        let wasm_path = self
            .storage
            .current_wasm_path(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no current.wasm for {id}")))?;

        // Build a fresh registry pointed at this module's wasm.
        let registry =
            ModuleRegistry::load_from_path(self.engine.clone(), id.to_owned(), &wasm_path)?;

        // Per-slot state: kv + emitter pair.
        let kv = (self.kv_factory)(id);
        let (sink, events_rx) = (self.emitter_factory)();

        // Build the driver with the resumed cursor + checkpoint
        // hook so future advances persist back through storage.
        let mut instance = registry
            .instantiate(self.data_plane.clone(), kv, sink, self.budget)
            .await?;
        // Init with persisted config if any; empty bytes
        // otherwise. The module decides what to do with empty
        // config — ownership-indexer-module treats it as
        // "no policies watched, no-op."
        let config = self.storage.read_config(id)?.unwrap_or_default();
        instance
            .bindings
            .call_init(&mut instance.store, &config)
            .await
            .map_err(PlatformError::Wasmtime)?;
        let persisted_cursor = self.storage.read_cursor(id)?;
        let mut driver = Driver::new(instance, self.budget);
        if let Some(cursor) = persisted_cursor.as_ref() {
            tracing::info!(module = %id, ?cursor, "resuming from persisted cursor");
            driver = driver.with_initial_cursor(cursor.clone());
        } else {
            tracing::info!(module = %id, "no persisted cursor; factory will pick start point");
        }
        let storage_for_hook = self.storage.clone();
        let id_for_hook = id.to_owned();
        let hook = Arc::new(move |cursor: &ChainPoint| {
            if let Err(e) = storage_for_hook.write_cursor(&id_for_hook, cursor) {
                tracing::warn!(error = %e, module = %id_for_hook, "checkpoint write failed");
            }
        });
        driver = driver.with_checkpoint_hook(hook);

        // Spawn the follower.
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let subscription = (self.subscription_factory)(persisted_cursor);
        let storage = self.storage.clone();
        let engine = self.engine.clone();
        let data_plane = self.data_plane.clone();
        let kv_factory = self.kv_factory.clone();
        let emitter_factory_inner = self.emitter_factory.clone();
        let id_for_task = id.to_owned();
        let id_for_log = id.to_owned();
        let task = tokio::spawn(async move {
            // Build a registry handle that the follower can use
            // to re-instantiate on RestartAndReplay outcomes.
            let registry = ModuleRegistry::load_from_path(
                engine,
                id_for_task.clone(),
                &storage.current_wasm_path(&id_for_task)?.ok_or_else(|| {
                    PlatformError::Decode(format!("no current.wasm for {id_for_task}"))
                })?,
            )?;
            let kv_factory_for_run = move || kv_factory(&id_for_task);
            let emitter_factory_for_run = move || emitter_factory_inner().0;
            let result = tokio::select! {
                _ = cancel_for_task.cancelled() => {
                    tracing::info!(module = %registry.module_id, "follower cancelled");
                    Ok(())
                }
                r = run_chain_follower(
                    driver,
                    subscription,
                    &registry,
                    data_plane,
                    kv_factory_for_run,
                    emitter_factory_for_run,
                ) => r,
            };
            // Task-death detection: log the outcome so a silent
            // panic from upstream code (e.g. dolos's
            // `TipSubscription::next_tip` unwrap on lagged
            // broadcast::Receiver) surfaces as an ERROR line
            // rather than a stuck follower with no journal trace.
            if let Err(e) = &result {
                tracing::error!(
                    module = %id_for_log,
                    error = %e,
                    "follower task exited with error",
                );
            } else {
                tracing::info!(
                    module = %id_for_log,
                    "follower task exited cleanly",
                );
            }
            result
        });

        // Drain task: pull EmittedEvents off events_rx, look
        // up registered companions for this module, and append
        // one `Queued` row per companion to the EmissionsStore.
        // The dial loop polls EmissionsStore for queued rows
        // and converts each to a `ServerMessage::Apply` over the
        // outbound WS — see `dialer::run_companion`.
        let drain_storage = self.storage.clone();
        let drain_module_id = id.to_owned();
        let drain_cancel = cancel.clone();
        let drain_task = tokio::spawn(async move {
            run_emit_drain(drain_storage, drain_module_id, events_rx, drain_cancel).await;
        });

        let mut slots = self.slots.lock().await;
        slots.insert(
            id.to_owned(),
            RunningSlot {
                sha: manifest.module.sha256,
                task,
                cancel,
                drain_task,
            },
        );
        tracing::info!(module = %id, "follower started");
        Ok(())
    }

    /// Alias for `start` — semantically clearer at the
    /// admin-route level where "the module that's currently
    /// running gets replaced by the new sha" is the operator's
    /// mental model.
    pub async fn replace(&self, id: &str) -> PlatformResult<()> {
        self.start(id).await
    }

    /// Stop the running follower for `id`, if any. No-op when
    /// `id` isn't running. Awaits the task to confirm cleanup.
    pub async fn stop(&self, id: &str) -> PlatformResult<()> {
        let mut slots = self.slots.lock().await;
        if let Some(slot) = slots.remove(id) {
            slot.cancel.cancel();
            // Best-effort: don't panic if the task already
            // panicked — log + drop. Real production might
            // want to surface this, but for v1.5 the lifecycle
            // is "stopped no matter what."
            match slot.task.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(module = %id, error = %e, "follower exited with error")
                }
                Err(join_err) if join_err.is_cancelled() => {}
                Err(join_err) => {
                    tracing::warn!(module = %id, error = %join_err, "follower task panicked")
                }
            }
            // Drain task shares the cancel token with the
            // follower, so cancelling above also signals it.
            // Await to ensure any in-flight EmissionsStore
            // append commits before stop returns.
            if let Err(join_err) = slot.drain_task.await {
                if !join_err.is_cancelled() {
                    tracing::warn!(module = %id, error = %join_err, "emit drain task panicked")
                }
            }
            // Drop the cached cursor-store handle so the next
            // start re-opens redb cleanly. Without this, a
            // crash during the just-stopped task could leave a
            // half-committed transaction visible to the new
            // task via the cached handle.
            self.storage.close_cursor(id);
            tracing::info!(module = %id, sha = %slot.sha, "follower stopped");
        }
        Ok(())
    }

    /// List currently-running module ids.
    pub async fn list(&self) -> Vec<String> {
        let slots = self.slots.lock().await;
        let mut out: Vec<String> = slots.keys().cloned().collect();
        out.sort();
        out
    }

    /// Stop every running follower. Used during bundle shutdown
    /// — without this, follower tasks keep tokio's runtime alive
    /// past the bundle's `serve` future and systemd hits its
    /// 90s graceful-shutdown timeout, SIGKILL's the process,
    /// and we lose any in-flight cursor checkpoints.
    pub async fn stop_all(&self) {
        let ids = self.list().await;
        for id in &ids {
            // stop() is idempotent + best-effort logged on
            // failure; we drive every slot regardless of any
            // single one's outcome.
            let _ = self.stop(id).await;
        }
    }

}

/// Per-module drain task: pulls EmittedEvents off `events_rx`
/// and writes one `Queued` row per registered companion to the
/// per-module `EmissionsStore`. Each companion gets its own row
/// so the dial loop can drain per-companion in id order.
///
/// Companions are discovered by listing
/// `<storage>/<module_id>/companions/*.cbor` on every event —
/// inefficient but trivially correct in the face of
/// register/unregister churn. v2 caches the companion list
/// behind a registered-companions broadcast.
async fn run_emit_drain(
    storage: ModuleStorage,
    module_id: String,
    mut events_rx: tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
    cancel: CancellationToken,
) {
    let store = match crate::emissions::EmissionsStore::open(storage.emissions_path(&module_id)) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                module = %module_id,
                error = %e,
                "open EmissionsStore for drain failed; emit interception disabled for this module"
            );
            return;
        }
    };
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            event = events_rx.recv() => {
                let Some(event) = event else { return };
                drain_one(&storage, &store, &module_id, event);
            }
        }
    }
}

fn drain_one(
    storage: &ModuleStorage,
    store: &crate::emissions::EmissionsStore,
    module_id: &str,
    event: emit::EmittedEvent,
) {
    use crate::emissions::EmissionStatus;
    let companions_dir = storage.module_dir_for_companions(module_id);
    if !companions_dir.exists() {
        return; // no registered companions
    }
    let read = match std::fs::read_dir(&companions_dir) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(
                module = %module_id,
                dir = %companions_dir.display(),
                error = %e,
                "read companions dir failed; emission dropped"
            );
            return;
        }
    };
    let now = format!(
        "unix:{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    // Channel as a string — the WIT ABI uses u32; companion-side
    // dispatch is by string tag. v1 stringifies; v2 will plumb
    // the name through manifest metadata.
    let channel = event.channel.to_string();
    for entry in read.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
            continue;
        }
        let companion_key = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        if let Err(e) = store.append(
            &companion_key,
            &channel,
            event.chain_point.clone(),
            event.payload.clone(),
            EmissionStatus::Queued,
            &now,
        ) {
            tracing::warn!(
                module = %module_id,
                companion_key = %companion_key,
                error = %e,
                "append emission row failed"
            );
        }
    }
}
