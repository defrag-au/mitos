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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

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

    /// Drive a coordinated state-rebuild for `id`. The protocol
    /// is in `docs/design/RECAPTURE.md`. v1 always targets every
    /// subscribed companion (`companion=*`); the `RecaptureRequest`
    /// type lives at the admin-router boundary.
    ///
    /// `companion_timeout` is the per-companion `RecaptureReady`
    /// ACK budget; reasonable default is `Duration::from_secs(30)`.
    ///
    /// Returns errors mappable to admin HTTP statuses:
    /// - `RecaptureInProgress` → 409
    /// - `RecaptureCoordination` → 504 (timeout) / 500 (other)
    /// - `Decode` (unknown module) → 404
    async fn recapture_module(
        &self,
        id: &str,
        reason: Option<String>,
        companion_timeout: Duration,
    ) -> PlatformResult<RecaptureOutcome>;

    /// Full module retirement: stop the running slot, drop the
    /// dialer's in-memory companion connections for this module,
    /// and surrender the artifact directory back to the
    /// admin handler for removal. The handler is responsible
    /// for the final filesystem step (it has the `ModuleStorage`
    /// reference directly).
    ///
    /// Unlike `stop`, this is intended to be permanent — `replace`
    /// on an evicted module is a fresh upload + activate, not a
    /// resume.
    ///
    /// Returns the list of `companion_key`s whose dial loops got
    /// cancelled (empty when no companions were active). Caller
    /// can surface those in the admin response so the operator
    /// can spot-check what got reaped.
    async fn evict_module(&self, id: &str) -> PlatformResult<Vec<String>>;
}

/// One dynamic-interest update queued for delivery to a running
/// module's `update-interest` export. Pushed by the dialer when a
/// `ClientMessage::Interest` frame arrives over the companion's WS.
///
/// Carries *both* shapes:
/// - `op` + `predicates` are what the host applies to its own
///   driver-level `InterestSet` for runtime filtering.
/// - `items_cbor` is the raw bytes the WIT `update-interest` export
///   takes — encoded `Vec<InterestPredicate>` so modules can decode
///   for their own state-kv persistence if they want.
///
/// The host owns the dispatch filter; module-side persistence is
/// optional. Encoding once and shipping bytes through avoids
/// double-encoding at the follower hop.
#[derive(Debug)]
pub struct InterestUpdate {
    pub op: mitos_protocol::InterestOp,
    pub predicates: Vec<mitos_data_plane::InterestPredicate>,
    pub items_cbor: Vec<u8>,
}

/// Translate a v1-shaped wire `Interest` (multi-axis, designed for
/// protocol-event filtering) into a v2 `InterestPredicate`
/// (single-purpose, designed for utxo-event filtering).
///
/// Today the companion-runtime SDK only emits `policy` rows
/// (`mitos_companion::interest::rows_to_interests`), which map to
/// `Interest::any().asset = AssetSelector::Policy(p)`. So the
/// projection collapses to extracting the policy id from the asset
/// axis. Roles / domain / value axes are v1 protocol-event concerns
/// the v2 utxo-dispatch path doesn't apply.
///
/// Returns `None` for items the v2 vocabulary can't represent
/// (asset selector `Any`, address-only selectors that v1 didn't
/// distinguish, etc.) — caller logs + drops those.
fn v1_interest_to_predicate(
    item: &mitos_protocol::Interest,
) -> Option<mitos_data_plane::InterestPredicate> {
    use mitos_data_plane::InterestPredicate;
    use mitos_protocol::AssetSelector;
    match &item.asset {
        AssetSelector::Policy(p) => Some(InterestPredicate::HoldsPolicy(p.clone())),
        AssetSelector::Asset { policy, name_hex } => Some(InterestPredicate::HoldsAsset {
            policy: policy.clone(),
            asset_name: hex::decode(name_hex).ok()?,
        }),
        // v2 doesn't have a wildcard predicate; "match everything"
        // means an empty `InterestSet`. v1 `Any` rows on the wire
        // should never reach the host (the SDK doesn't emit them),
        // but if they did we drop rather than synthesize a no-op.
        // `Fingerprint` and `Trait` are v1 protocol-event filters
        // with no v2 utxo-dispatch analogue — also drop.
        _ => None,
    }
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
    /// Per-running-module dynamic-interest channel senders. The
    /// matching receiver lives inside each module's follower task
    /// where it's multiplexed with tip events. Held in a
    /// `std::sync::Mutex` (not `tokio::sync`) because every
    /// access is a quick map insert/remove/get — no `.await` ever
    /// held across the guard.
    interest_senders:
        Arc<std::sync::Mutex<HashMap<String, tokio::sync::mpsc::UnboundedSender<InterestUpdate>>>>,
    /// Set of module ids with an in-flight recapture. Mutual
    /// exclusion is per-module: a second `recapture_module(id)`
    /// while the first is mid-flight returns
    /// `PlatformError::RecaptureInProgress`. Uses a `HashSet` +
    /// std `Mutex` rather than per-key mutex because the only
    /// operations are check-insert-or-reject and remove — no
    /// awaiting needed.
    recapture_in_flight: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Companion dialer the recapture orchestrator drives.
    /// Wired post-construction via `set_dialer` because the
    /// dialer borrows the host as its `InterestRouter` — host
    /// must exist (and be `Arc`-wrapped) before the dialer can
    /// be built. `OnceLock` lets us inject behind a `&self`
    /// without giving up Arc-sharing of the host. `None` (i.e.
    /// the lock unset) means recapture is unavailable; the
    /// admin endpoint returns a clear error rather than
    /// panicking.
    dialer: std::sync::OnceLock<crate::dialer::CompanionDialer>,
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
            interest_senders: Arc::new(std::sync::Mutex::new(HashMap::new())),
            recapture_in_flight: Arc::new(std::sync::Mutex::new(HashSet::new())),
            dialer: std::sync::OnceLock::new(),
        }
    }

    /// Wire the companion dialer into the host. Must be called
    /// before the `recapture_module` flow can run; subsequent
    /// calls are silent no-ops (the `OnceLock` semantics).
    ///
    /// Construction order in `mitos-core::Bundle`:
    /// 1. `let host = Arc::new(ModuleHostV2::new(...))`
    /// 2. `let dialer = CompanionDialer::new(storage, auth, host.clone() as Arc<dyn InterestRouter>)`
    /// 3. `host.set_dialer(dialer.clone())`
    ///
    /// The dialer needs the host (as `InterestRouter`), and the
    /// host needs the dialer (for `recapture_module`); `set_dialer`
    /// breaks the circular construction without needing interior
    /// mutability beyond `OnceLock`.
    pub fn set_dialer(&self, dialer: crate::dialer::CompanionDialer) {
        if self.dialer.set(dialer).is_err() {
            tracing::warn!("set_dialer called twice; second call ignored");
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
        // Wrap the shared data plane with the aux-data cache so
        // `tx_metadata` calls from the module check the persistent
        // cache before hitting the dolos archive (7-day window).
        // Failures to open the cache degrade gracefully to a plain
        // passthrough — same behaviour as before this was added.
        let cache_opt = match self.storage.aux_data_cache() {
            Ok(c) => {
                tracing::debug!(module = %id, "aux_data_cache opened");
                Some(Arc::new(c))
            }
            Err(e) => {
                tracing::warn!(
                    module = %id,
                    error = %e,
                    "aux_data_cache unavailable; tx_metadata bypasses cache",
                );
                None
            }
        };
        let maestro_opt = crate::maestro::MaestroClient::shared();
        if maestro_opt.is_some() {
            tracing::info!(module = %id, "Maestro aux_data fallback enabled");
        }

        let caching_plane: Arc<dyn DataPlaneFacade> = Arc::new(
            crate::host_fns::CachingDataPlane::new(self.data_plane.clone(), cache_opt, maestro_opt),
        );
        let trap_logger = Arc::new(TrapContextLogger::new(caching_plane));

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
            let interest =
                interest_from_manifest(&manifest.interest.addresses, &manifest.interest.policies);
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
        // Wrap the chain plane so prior-output lookups for inputs
        // older than the dolos archive horizon fall back to
        // Maestro. Without this, `build_event_batches` emits
        // `TypedOutput::unresolved()` for archive-pruned create-
        // TXs, the InterestSet doesn't match, and the consuming
        // module sees no event — see
        // `maestro_fallback_plane.rs` for the failure mode this
        // fixes. When no Maestro key is configured the wrapper
        // collapses to a pass-through.
        let chain_plane: Arc<crate::maestro_fallback_plane::MaestroFallbackPlane<P>> = Arc::new(
            crate::maestro_fallback_plane::MaestroFallbackPlane::new(
                self.chain_plane.clone(),
                crate::maestro::MaestroClient::shared(),
            ),
        );
        let follower_storage = self.storage.clone();
        let follower_module_id = id.to_owned();
        let id_for_log = id.to_owned();

        // Dynamic-interest channel — sender owned by
        // `interest_senders` (cloned for the dialer via
        // `InterestRouter::route_interest`); receiver moves into
        // the follower task and is multiplexed with tip events
        // via `tokio::select!`.
        let (interest_tx, interest_rx) = tokio::sync::mpsc::unbounded_channel::<InterestUpdate>();
        {
            let mut senders = self
                .interest_senders
                .lock()
                .expect("interest_senders mutex");
            senders.insert(id.to_owned(), interest_tx);
        }

        let follower_kv_factory = self.kv_factory.clone();
        let follower_trap_logger = trap_logger.clone();
        let task = tokio::spawn(async move {
            let result = run_chain_follower_v2(
                driver,
                subscription,
                interest_rx,
                cancel_for_task,
                chain_plane,
                follower_storage,
                follower_module_id,
                follower_kv_factory,
                follower_trap_logger,
            )
            .await;
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
            run_emit_drain(drain_storage, drain_module_id, events_rx, drain_cancel).await;
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
        // Drop the interest sender first so the follower's
        // `tokio::select!` sees a `None` on `interest_rx.recv()`
        // alongside cancellation. Mirrors v1's stop semantics —
        // sender drop is the clean-exit signal for the multiplexed
        // loop.
        {
            let mut senders = self
                .interest_senders
                .lock()
                .expect("interest_senders mutex");
            senders.remove(id);
        }
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

    /// Recapture: coordinate every subscribed companion through a
    /// clean state-rebuild for `id`. The protocol exchange is in
    /// `docs/design/RECAPTURE.md`.
    ///
    /// Sequence:
    ///
    /// 1. Acquire per-module recapture mutex (409 on conflict).
    /// 2. Through `dialer.recapture_module`: send
    ///    `ServerMessage::Recapture { module, reason }` to every
    ///    subscribed companion, await `RecaptureReady` from each
    ///    with `companion_timeout`. Any timeout aborts the rest of
    ///    this flow — no bootstrap-refill fires, dApp-state is
    ///    whatever the companions partially produced, operator
    ///    investigates.
    /// 3. Clear the module's bootstrap-done flags via
    ///    `bootstrap_v2::clear_bootstrap_flags`. Without this the
    ///    follower's bootstrap pass would short-circuit and no
    ///    refill events would flow.
    /// 4. Restart the follower via `self.start(id)`. The fresh
    ///    follower's `init` runs, then `run_bootstrap` walks every
    ///    `[interest].address` + `[interest].policy` (now flagless)
    ///    and synthesises events that flow through the broadcast
    ///    channel + outbound WSes to each companion's
    ///    `apply_event` for re-INSERT.
    /// 5. Through `dialer.send_recapture_done`: push
    ///    `ServerMessage::RecaptureDone { cursor, module,
    ///    events_emitted }` to each companion. Informational — the
    ///    Apply stream already did the load-bearing work; this is
    ///    a clean boundary marker.
    ///
    /// `events_emitted` is currently `0` — counting per-companion
    /// requires bootstrap_v2 to thread a counter back, which is a
    /// follow-up refinement noted in `RECAPTURE.md` open
    /// question 4.
    ///
    /// `companion_timeout` is the per-companion budget for the
    /// `RecaptureReady` ACK. Reasonable default at the call site
    /// is 30s (matches the design doc's open question 3).
    pub async fn recapture_module(
        &self,
        id: &str,
        reason: Option<String>,
        companion_timeout: Duration,
    ) -> PlatformResult<RecaptureOutcome> {
        let dialer = self.dialer.get().ok_or_else(|| {
            PlatformError::RecaptureCoordination(
                "dialer not wired (host.set_dialer never called); recapture unavailable".to_owned(),
            )
        })?;

        // 1. Acquire mutex.
        {
            let mut in_flight = self
                .recapture_in_flight
                .lock()
                .expect("recapture_in_flight mutex poisoned");
            if !in_flight.insert(id.to_owned()) {
                return Err(PlatformError::RecaptureInProgress(id.to_owned()));
            }
        }
        // Drop-guard ensures the mutex set is cleaned up even on
        // early-return / panic. Using an explicit struct rather
        // than `defer!` to keep deps minimal.
        let _release = RecaptureGuard {
            set: self.recapture_in_flight.clone(),
            id: id.to_owned(),
        };

        tracing::info!(
            module = %id,
            reason = ?reason,
            "recapture: starting"
        );

        // 2. Send Recapture + await RecaptureReady from each.
        let ready = match dialer
            .recapture_module(id, reason.clone(), companion_timeout)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    module = %id,
                    error = %e,
                    "recapture: companion coordination failed; aborting before bootstrap-refill"
                );
                return Err(PlatformError::RecaptureCoordination(e.to_string()));
            }
        };
        let companion_count = ready.ready_companions.len();
        tracing::info!(
            module = %id,
            companion_count,
            "recapture: all companions ready; wiping bootstrap-done flags"
        );

        // 3. Clear bootstrap-done flags.
        {
            let mut kv = (self.kv_factory)(id);
            match crate::bootstrap_v2::clear_bootstrap_flags(&mut kv, id) {
                Ok(n) => tracing::info!(
                    module = %id,
                    flags_cleared = n,
                    "recapture: bootstrap-done flags cleared"
                ),
                Err(e) => {
                    tracing::error!(
                        module = %id,
                        error = %e,
                        "recapture: clear_bootstrap_flags failed; aborting before follower restart"
                    );
                    return Err(PlatformError::RecaptureCoordination(format!(
                        "clear_bootstrap_flags: {e}"
                    )));
                }
            }
        }

        // 4. Restart follower. `start(id)` stops the existing
        //    follower (if running), re-instantiates wasmtime, calls
        //    init, runs bootstrap (which now finds no flags and
        //    walks the interest set), then spawns the follower
        //    task. Bootstrap-synthesised events flow through the
        //    broadcast → outbound WSes → companions while this
        //    call is returning.
        if let Err(e) = self.start(id).await {
            tracing::error!(
                module = %id,
                error = %e,
                "recapture: follower restart failed; refill is partial"
            );
            return Err(e);
        }

        // 5. (Legacy `RecaptureDone` notification dropped — the
        //    HTTP delivery transport doesn't push informational
        //    frames to companions. The admin endpoint's success
        //    response is the canonical "recapture complete" signal
        //    for operators; companions observe completion via the
        //    refill Apply requests landing.)

        tracing::info!(
            module = %id,
            companion_count,
            "recapture: complete"
        );
        Ok(RecaptureOutcome {
            module: id.to_owned(),
            companion_count,
            // Best-effort counter; populated by future bootstrap
            // instrumentation. See `RECAPTURE.md` open question 4.
            events_emitted: 0,
        })
    }

    /// Full retirement of a running module:
    ///
    /// 1. Stop the running slot (calls into `Self::stop`).
    /// 2. Cancel any in-memory dialer state for this module so
    ///    stale WS connections to deleted companions get reaped
    ///    immediately rather than waiting for a service restart.
    /// 3. Close cached DB handles (`kv.redb`, `emissions.redb`,
    ///    `cursor.redb`) so the OS file locks release before the
    ///    admin handler tries to `remove_dir_all` the artifact.
    ///
    /// The admin handler is responsible for the filesystem step
    /// (calling `ModuleStorage::remove_module_dir`) after this
    /// returns — keeps the storage concern at the storage
    /// boundary rather than threading the storage handle through
    /// the host.
    ///
    /// Returns the `companion_key`s whose dial loops got
    /// cancelled in step 2, so the admin response can surface
    /// what was reaped.
    ///
    /// Returns `Ok(vec![])` (without error) when the module isn't
    /// currently running — idempotent for re-tries against a
    /// half-retired module.
    pub async fn evict_module(&self, id: &str) -> PlatformResult<Vec<String>> {
        // 1. Stop the running slot. Safe to call against a
        //    not-running id (no-op).
        self.stop(id).await?;

        // 2. Drop dialer in-memory state for the module's
        //    companions. The dialer is wired post-construction
        //    via `set_dialer`; in artifact-only test deployments
        //    it's `None`, in which case there are no in-memory
        //    connections to drop and we report empty.
        let cancelled = if let Some(dialer) = self.dialer.get() {
            dialer.unregister_module(id).await
        } else {
            Vec::new()
        };

        // 3. Close cached DB handles so the redb file locks
        //    release before the admin handler removes the dir.
        //    `close_*` is idempotent — no-op when nothing's
        //    cached for `id`.
        self.storage.close_kv(id);
        self.storage.close_emissions(id);
        self.storage.close_cursor(id);

        tracing::info!(
            module = %id,
            cancelled_companions = cancelled.len(),
            "module evicted (slot stopped, dialer state cleared, DB handles closed)"
        );
        Ok(cancelled)
    }
}

/// Drop-guard for the per-module recapture mutex set. Ensures
/// the module is removed from `recapture_in_flight` even if the
/// `recapture_module` body panics or early-returns.
struct RecaptureGuard {
    set: Arc<std::sync::Mutex<HashSet<String>>>,
    id: String,
}

impl Drop for RecaptureGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.id);
        }
    }
}

/// Outcome of a successful `recapture_module` call. Surfaced
/// through the admin endpoint's response body.
#[derive(Debug, Clone)]
pub struct RecaptureOutcome {
    pub module: String,
    /// Number of companions whose state was refilled.
    pub companion_count: usize,
    /// Per `RECAPTURE.md` open question 4: best-effort counter.
    /// v1 reports `0` — implementation pending.
    pub events_emitted: u64,
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
    async fn recapture_module(
        &self,
        id: &str,
        reason: Option<String>,
        companion_timeout: Duration,
    ) -> PlatformResult<RecaptureOutcome> {
        ModuleHostV2::recapture_module(self, id, reason, companion_timeout).await
    }
    async fn evict_module(&self, id: &str) -> PlatformResult<Vec<String>> {
        ModuleHostV2::evict_module(self, id).await
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
    use crate::emissions::{EmissionStatus, UNSUBSCRIBED_COMPANION_ID};
    let companions_dir = storage.module_dir_for_companions(module_id);
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

    // Track whether we wrote at least one per-companion row. If
    // not (no dir, empty dir, or all entries skipped), fall back
    // to the sentinel companion-id so the emission isn't silently
    // dropped during the window before any companion subscribes
    // (drop site #3 in `docs/design/EVENT_DELIVERY_RESILIENCE.md`).
    // The first subscribe call for this module reclaims the
    // sentinel rows via `EmissionsStore::retarget_companion`.
    let mut wrote_per_companion = false;
    if companions_dir.exists() {
        match std::fs::read_dir(&companions_dir) {
            Ok(read) => {
                for entry in read.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
                        continue;
                    }
                    let companion_key = match path.file_stem().and_then(|s| s.to_str()) {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    match store.append(
                        &companion_key,
                        &channel,
                        event.chain_point.clone(),
                        event.payload.clone(),
                        event.partition_key.clone(),
                        EmissionStatus::Queued,
                        &now,
                    ) {
                        Ok(_) => wrote_per_companion = true,
                        Err(e) => {
                            tracing::warn!(
                                module = %module_id,
                                companion_key = %companion_key,
                                error = %e,
                                "append emission row failed"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    module = %module_id,
                    dir = %companions_dir.display(),
                    error = %e,
                    "read companions dir failed; falling back to sentinel"
                );
            }
        }
    }

    if !wrote_per_companion {
        if let Err(e) = store.append(
            UNSUBSCRIBED_COMPANION_ID,
            &channel,
            event.chain_point.clone(),
            event.payload.clone(),
            event.partition_key.clone(),
            EmissionStatus::Queued,
            &now,
        ) {
            tracing::warn!(
                module = %module_id,
                error = %e,
                "append unsubscribed sentinel emission failed; emission lost"
            );
        } else {
            tracing::debug!(
                module = %module_id,
                channel = %channel,
                "no subscribed companion; emission persisted to sentinel"
            );
        }
    }
}

/// Interest-update router for v2 modules.
///
/// The companion-WS dialer in `dialer.rs` forwards every inbound
/// `ClientMessage::Interest` frame here. We translate v1-shaped
/// `Interest` items into v2 `InterestPredicate`s, encode them as
/// CBOR (the WIT `update-interest` export takes opaque bytes) and
/// push an `InterestUpdate` through the per-module follower
/// channel. The follower applies the op to the driver's
/// `InterestSet` between blocks and calls the module's
/// `update-interest` export so it can persist if it wants.
#[async_trait::async_trait]
impl<S, P> InterestRouter for ModuleHostV2<S, P>
where
    S: TipSubscription + 'static,
    P: ChainDataPlane + Send + Sync + 'static,
{
    async fn route_interest(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError> {
        // Project v1 wire items into v2 predicates. Items the v2
        // vocabulary can't represent (Any, Fingerprint, Trait)
        // get logged + dropped — caller continues with the
        // remainder.
        let mut predicates = Vec::with_capacity(items.len());
        for item in &items {
            match v1_interest_to_predicate(item) {
                Some(p) => predicates.push(p),
                None => tracing::debug!(
                    module = %module_id,
                    "v1 interest item with no v2 analogue — dropped",
                ),
            }
        }
        if predicates.is_empty() && !items.is_empty() {
            // Every item dropped — operator wanted to update
            // interest but nothing landed. Surface as a log; not
            // an error since the SDK only emits policy rows
            // today and those translate cleanly.
            tracing::warn!(
                module = %module_id,
                "v1 interest update with {} item(s) projected to 0 v2 predicates; nothing applied",
                items.len(),
            );
            return Ok(());
        }

        // CBOR-encode the v2 predicates so the follower can hand
        // the same bytes to the module's `update-interest` export
        // — module decodes once on its side if it cares.
        let mut items_cbor = Vec::with_capacity(64);
        if let Err(e) = ciborium::ser::into_writer(&predicates, &mut items_cbor) {
            tracing::error!(
                module = %module_id,
                error = %e,
                "encode v2 interest predicates as CBOR failed; update dropped",
            );
            return Ok(());
        }

        let sender = {
            let senders = self
                .interest_senders
                .lock()
                .expect("interest_senders mutex");
            senders.get(module_id).cloned()
        };
        let sender = sender.ok_or_else(|| InterestRouteError::NotRunning(module_id.to_owned()))?;
        sender
            .send(InterestUpdate {
                op,
                predicates,
                items_cbor,
            })
            .map_err(|_| InterestRouteError::ChannelClosed(module_id.to_owned()))?;
        Ok(())
    }
}
