//! v2 module host lifecycle — running-instance management for
//! v2 wasm modules.
//!
//! Sister to `host.rs` (v1). Same shape:
//! - `start(id)` reads manifest + wasm, instantiates via
//!   `ModuleRegistryV2`, calls init, spawns a v2 follower
//! - `stop(id)` cancels the follower, awaits clean shutdown
//! - `replace(id)` is start-after-stop; same alias semantics
//!
//! Differences from v1:
//! - Uses v2 bindings + `DriverV2` + `run_chain_follower_v2`.
//! - Dynamic interest (runtime-mutable predicates from the
//!   companion WS) is not wired yet — see follower_v2 docs.
//!   Initial interest is empty until a v2-flavoured WS arrives;
//!   the bootstrap orchestrator (step 6) sets it from manifest
//!   `[interest]` config.
//! - Trap-context fixture capture is wired the same way as v1
//!   so the existing `/_admin/modules/<id>/last-trap` endpoint
//!   serves both.

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::TipSubscription;
use mitos_data_plane::{ChainDataPlane, ChainPoint};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::bootstrap_v2::{interest_from_manifest, run_bootstrap};
use crate::driver_v2::DriverV2;
use crate::follower_v2::run_chain_follower_v2;
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::registry_v2::{ModuleRegistryV2, ResourceBudget};
use crate::storage::ModuleStorage;
use crate::trap_context::{TrapContextLogger, write_fixture};
use crate::{PlatformError, PlatformResult};

// ============================================================================
// Public lifecycle traits + factory types (moved from `host.rs` when the v1
// dispatch path was retired — see MITOS_PLATFORM_V2.md).
// ============================================================================

/// Object-safe lifecycle surface for the admin router. The concrete
/// `ModuleHostV2<S, P>` is generic over the `TipSubscription` and
/// `ChainDataPlane` types; the admin router doesn't care about that
/// — it just needs "start a module" / "stop a module" / "what's
/// running." The trait keeps the generics out of route signatures.
#[async_trait::async_trait]
pub trait ModuleHostHandle: Send + Sync {
    async fn replace(&self, id: &str) -> PlatformResult<()>;
    async fn stop(&self, id: &str) -> PlatformResult<()>;
    async fn stop_all(&self);
    async fn list_running(&self) -> Vec<String>;
}

/// One dynamic-interest update queued for delivery to a running
/// module's `update-interest` export. Pushed by the dialer when a
/// `ClientMessage::Interest` frame arrives over the companion's WS.
#[derive(Debug)]
pub struct InterestUpdate {
    pub op: mitos_protocol::InterestOp,
    pub items: Vec<mitos_protocol::Interest>,
}

/// Routes inbound `Interest` frames to the right running module's
/// follower task. Only the follower task can call the wasmtime
/// instance's exports (the wasmtime Store isn't safely shareable
/// across tasks), so the dialer can't invoke `update-interest`
/// directly — it pushes through this trait and the follower drains.
#[async_trait::async_trait]
pub trait InterestRouter: Send + Sync {
    async fn route_interest(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError>;
}

#[derive(Debug, thiserror::Error)]
pub enum InterestRouteError {
    #[error("module `{0}` is not running; interest update dropped")]
    NotRunning(String),
    #[error("module `{0}` follower channel closed; interest update dropped")]
    ChannelClosed(String),
}

/// Factory for fresh `TipSubscription`s. Called once per
/// `start`/`replace` to spin up an isolated subscription for each
/// follower. Takes the persisted cursor (if any) so the production
/// wiring can choose: resume from cursor on restart, or fall back
/// to the current chain tip on fresh deploy.
pub type SubscriptionFactory<S> = Arc<dyn Fn(Option<ChainPoint>) -> S + Send + Sync>;

/// Factory for fresh in-memory or redb-backed KVs. Called per
/// module on start/replace.
pub type KvFactory = Arc<dyn Fn(&str) -> state_kv::ModuleKv + Send + Sync>;

/// Factory for fresh `EventSink` pairs. Each follower gets one;
/// the host wires the receiving end into the emissions store /
/// replication WS.
pub type EmitterFactory = Arc<
    dyn Fn() -> (
            emit::EventSink,
            tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
        ) + Send
        + Sync,
>;

/// Per-running-module state held inside the host.
struct RunningSlotV2 {
    sha: String,
    task: JoinHandle<PlatformResult<()>>,
    cancel: CancellationToken,
    drain_task: JoinHandle<()>,
}

/// Running-module lifecycle manager for v2 modules.
///
/// `S` is the `TipSubscription` type the host pumps from. Same
/// concrete type for every running slot — production wires
/// `dolos`'s broadcast subscription via the `SubscriptionFactory`.
pub struct ModuleHostV2<S, P>
where
    S: TipSubscription,
    P: ChainDataPlane + Send + Sync + 'static,
{
    storage: ModuleStorage,
    engine: wasmtime::Engine,
    /// `DataPlaneFacade` — used by the wasmtime host fns when
    /// the module calls `chain-data::*` from inside
    /// `handle-events`. Same trait v1 uses.
    data_plane: Arc<dyn DataPlaneFacade>,
    /// `ChainDataPlane` — used by the dispatch composer's
    /// `build_event_batches` to bulk-resolve prior outputs.
    /// Concrete type kept alongside the `dyn DataPlaneFacade`
    /// because v2's dispatch composer takes a generic
    /// `&P: ChainDataPlane` rather than a trait object —
    /// specialising avoids the per-call vtable dispatch.
    chain_plane: Arc<P>,
    subscription_factory: SubscriptionFactory<S>,
    kv_factory: KvFactory,
    emitter_factory: EmitterFactory,
    budget: ResourceBudget,
    slots: Arc<Mutex<HashMap<String, RunningSlotV2>>>,
}

impl<S, P> ModuleHostV2<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    // Lifecycle constructor takes one parameter per platform
    // capability the host needs to spin up a v2 module slot
    // (storage, engine, data planes, factories, budget). They
    // don't naturally collapse into a config struct without
    // adding a new struct that's just a parameter bag, so
    // we accept the count over clippy's heuristic.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage: ModuleStorage,
        engine: wasmtime::Engine,
        data_plane: Arc<dyn DataPlaneFacade>,
        chain_plane: Arc<P>,
        subscription_factory: SubscriptionFactory<S>,
        kv_factory: KvFactory,
        emitter_factory: EmitterFactory,
        budget: ResourceBudget,
    ) -> Self {
        Self {
            storage,
            engine,
            data_plane,
            chain_plane,
            subscription_factory,
            kv_factory,
            emitter_factory,
            budget,
            slots: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start (or restart) the v2 module. Mirrors v1's
    /// `ModuleHost::start` shape: stop existing slot if any →
    /// instantiate → init (with refuel + trap-context capture)
    /// → spawn follower + emit-drain task.
    pub async fn start(&self, id: &str) -> PlatformResult<()> {
        self.stop(id).await?;

        let manifest = self
            .storage
            .read_manifest(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;
        let wasm_path = self
            .storage
            .current_wasm_path(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no current.wasm for {id}")))?;

        let registry =
            ModuleRegistryV2::load_from_path(self.engine.clone(), id.to_owned(), &wasm_path)?;

        let kv = (self.kv_factory)(id);
        let (sink, events_rx) = (self.emitter_factory)();

        // Trap-context logger wraps the data plane facade so
        // host-fn calls during init or dispatch get captured.
        // Same `last-trap.toml` write path v1 uses.
        let trap_logger = Arc::new(TrapContextLogger::new(self.data_plane.clone()));

        let mut instance = registry
            .instantiate(trap_logger.clone(), kv, sink, self.budget)
            .await?;

        let config = self.storage.read_config(id)?.unwrap_or_default();
        instance.store.set_fuel(self.budget.init_fuel)?;
        if let Err(e) = instance
            .bindings
            .call_init(&mut instance.store, &config)
            .await
        {
            let snap = trap_logger.snapshot();
            let path = self.storage.last_trap_path(id);
            match write_fixture(&snap, id, &path) {
                Ok(()) => tracing::error!(
                    module = %id,
                    fixture = %path.display(),
                    "v2 init trapped; fixture written for local replay (mitos-run --fixture)",
                ),
                Err(write_err) => tracing::warn!(
                    module = %id,
                    error = %write_err,
                    "v2 init trapped; failed to write trap fixture",
                ),
            }
            return Err(PlatformError::Wasmtime(e));
        }

        let mut driver = DriverV2::new(instance, self.budget);

        // Bootstrap: hydrate state at watched addresses
        // declared in the manifest's `[interest]` section.
        // Per-address state-kv flags make this a no-op for
        // already-bootstrapped addresses (idempotent), so the
        // path runs cheap on every restart. Policy-scoped
        // interest predicates participate in runtime
        // filtering but don't trigger a bootstrap pass today.
        if !manifest.interest.is_empty() {
            // Build the InterestSet from the manifest's
            // declarative addresses + policies. Push it onto
            // the driver so subsequent block dispatch filters
            // correctly even if no companion-driven
            // update-interest arrives.
            let interest = interest_from_manifest(
                &manifest.interest.addresses,
                &manifest.interest.policies,
            );
            driver.set_interest(interest.clone());

            // Hand the bootstrap orchestrator a mutable
            // reference to the module's state-kv (for the
            // per-address completion flags) and the data
            // plane facade (for current-state lookups).
            // Synthetic events flow through the driver with
            // refuel-per-batch so per-call fuel stays
            // bounded.
            let mut bootstrap_kv = (self.kv_factory)(id);
            let bootstrap_chain = self.chain_plane.clone();
            match run_bootstrap(
                &mut driver,
                id,
                &mut bootstrap_kv,
                &interest,
                bootstrap_chain.as_ref(),
            )
            .await
            {
                Ok(stats) => tracing::info!(
                    module = %id,
                    addresses_seen = stats.addresses_seen,
                    addresses_scanned = stats.addresses_scanned,
                    utxos_dispatched = stats.utxos_dispatched,
                    batches_dispatched = stats.batches_dispatched,
                    "v2 bootstrap complete",
                ),
                Err(e) => {
                    // Bootstrap failure surfaces but doesn't
                    // abort the host start — the follower
                    // will pick up live chain activity from
                    // here regardless. Operators see the
                    // error and can re-trigger bootstrap by
                    // clearing the per-address state-kv flag.
                    tracing::error!(
                        module = %id,
                        error = %e,
                        "v2 bootstrap failed; continuing without full hydration",
                    );
                }
            }
        }

        // Spawn the v2 follower.
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let persisted_cursor = self.storage.read_cursor(id)?;
        let subscription = (self.subscription_factory)(persisted_cursor);
        let chain_plane = self.chain_plane.clone();
        let id_for_log = id.to_owned();

        let task = tokio::spawn(async move {
            let result =
                run_chain_follower_v2(driver, subscription, cancel_for_task, chain_plane).await;
            match &result {
                Err(e) => tracing::error!(
                    module = %id_for_log,
                    error = %e,
                    "v2 follower task exited with error",
                ),
                Ok(_) => tracing::info!(
                    module = %id_for_log,
                    "v2 follower task exited cleanly",
                ),
            }
            result
        });

        // Drain task — same shape as v1, pulls EmittedEvents off
        // the events channel and forwards to the EmissionsStore
        // for each registered companion.
        let drain_storage = self.storage.clone();
        let drain_module_id = id.to_owned();
        let drain_cancel = cancel.clone();
        let drain_task = tokio::spawn(async move {
            run_emit_drain(drain_storage, drain_module_id, events_rx, drain_cancel)
                .await;
        });

        let mut slots = self.slots.lock().await;
        slots.insert(
            id.to_owned(),
            RunningSlotV2 {
                sha: manifest.module.sha256,
                task,
                cancel,
                drain_task,
            },
        );
        tracing::info!(module = %id, "v2 follower started");
        Ok(())
    }

    pub async fn replace(&self, id: &str) -> PlatformResult<()> {
        self.start(id).await
    }

    pub async fn stop(&self, id: &str) -> PlatformResult<()> {
        let slot = {
            let mut slots = self.slots.lock().await;
            slots.remove(id)
        };
        let Some(slot) = slot else {
            return Ok(());
        };
        slot.cancel.cancel();
        let _ = slot.task.await;
        let _ = slot.drain_task.await;
        // Mirror v1's storage-cache eviction so a future start()
        // re-opens redb cleanly. The `close_*` set is per-module
        // and matches what v1 does — kept identical for behaviour
        // parity during the v1+v2 coexistence window.
        self.storage.close_cursor(id);
        self.storage.close_kv(id);
        tracing::info!(
            module = %id,
            sha = %slot.sha,
            "v2 module stopped",
        );
        Ok(())
    }

    pub async fn list(&self) -> Vec<String> {
        let slots = self.slots.lock().await;
        slots.keys().cloned().collect()
    }

    pub async fn stop_all(&self) {
        let ids: Vec<String> = {
            let slots = self.slots.lock().await;
            slots.keys().cloned().collect()
        };
        for id in ids {
            let _ = self.stop(&id).await;
        }
    }

    /// Resume any modules whose manifests already sit on disk.
    /// Called once at host start; logs + skips on per-module
    /// failure so a single bad module can't abort bundle startup.
    /// Replaces the v1 unified host's auto-resume that previously
    /// routed by ABI version.
    pub async fn auto_resume(&self) {
        let module_ids = match self.storage.list_modules() {
            Ok(ids) => ids,
            Err(e) => {
                tracing::error!(error = %e, "auto-resume: list_modules failed");
                return;
            }
        };
        for id in module_ids {
            match self.start(&id).await {
                Ok(()) => tracing::info!(module = %id, "auto-resumed"),
                Err(e) => tracing::error!(
                    module = %id,
                    error = %e,
                    "auto-resume failed; module not running",
                ),
            }
        }
    }
}

#[async_trait::async_trait]
impl<S, P> ModuleHostHandle for ModuleHostV2<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    async fn replace(&self, id: &str) -> PlatformResult<()> {
        ModuleHostV2::replace(self, id).await
    }
    async fn stop(&self, id: &str) -> PlatformResult<()> {
        ModuleHostV2::stop(self, id).await
    }
    async fn stop_all(&self) {
        ModuleHostV2::stop_all(self).await
    }
    async fn list_running(&self) -> Vec<String> {
        ModuleHostV2::list(self).await
    }
}

/// Drain task — pull events off the per-module emit channel and
/// fan them out into the per-companion emissions store. Spawned
/// alongside the follower task in `start()` and cancelled in
/// `stop()`. (Moved here from the retired v1 `host.rs`.)
pub(crate) async fn run_emit_drain(
    storage: ModuleStorage,
    module_id: String,
    mut events_rx: tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
    cancel: CancellationToken,
) {
    let store = match storage.emissions_store(&module_id) {
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
    // dispatch is by string tag. We stringify here; future work
    // could plumb the name through manifest metadata.
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

/// Interest-update router for v2 modules.
///
/// The companion-WS dialer in `dialer.rs` doesn't know which ABI
/// version a module runs under, so it routes every inbound
/// `ClientMessage::Interest` frame through a `dyn InterestRouter`.
/// v2's wire-interest path is not wired through to the follower
/// yet (manifest-declared interest is the source of truth at
/// runtime); accept + log so the dialer doesn't error.
#[async_trait::async_trait]
impl<S, P> InterestRouter for ModuleHostV2<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    async fn route_interest(
        &self,
        module_id: &str,
        _op: mitos_protocol::InterestOp,
        _items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError> {
        tracing::debug!(
            module = %module_id,
            "v2 interest update dropped (wire path TBD; manifest [interest] is source of truth)",
        );
        Ok(())
    }
}
