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
use mitos_lifecycle::ids::InstanceKey;
use mitos_lifecycle::routing;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::bootstrap_v2::{interest_from_manifest, run_bootstrap};
use crate::follower_v2::run_chain_follower_v2;
use crate::host_fns::{DataPlaneFacade, emit, state_kv};
use crate::registry_v2::{ModuleRegistryV2, ResourceBudget};
use crate::rows::{RowCache, instance_dir_of};
use crate::storage::ModuleStorage;
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

    /// Which instances of `module` have a live slot. `list_running` answers
    /// the same question module-at-a-time and cannot distinguish two
    /// instances of one module; this is what the instances admin route reads.
    /// Cheap and side-effect free.
    async fn running_instances(&self, module: &str) -> Vec<InstanceKey>;

    /// Start ONE instance.
    ///
    /// The three calls below are what the `LifecycleManager` uses, and it
    /// uses NOTHING else: it always knows the key it means, and a manager
    /// that mixed the two addressings is how `facade_slots` came to exist.
    /// `replace` / `stop` / `evict_module` remain for the `&str` operator
    /// tools, which mean "the module's current instance".
    async fn start_instance(&self, key: &InstanceKey) -> PlatformResult<()>;

    async fn stop_instance(&self, key: &InstanceKey) -> PlatformResult<()>;

    /// Retire ONE instance: stop it, release the dialer's hold on its
    /// emissions store, close its handles and remove its directory. When it
    /// was the module's LAST instance this also does what `evict_module`
    /// does at the module level, so reaping a module's only instance is
    /// exactly today's evict.
    ///
    /// Returns the companion keys whose dial loops were cancelled — empty
    /// unless the module went with it.
    async fn evict_instance(&self, key: &InstanceKey) -> PlatformResult<Vec<String>>;

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

    /// Route a dynamic interest mutation into a running module's
    /// follower — the same path a companion subscribe uses. The
    /// admin router's per-companion recapture drives this with
    /// `InterestOp::Resync` to re-emit a companion's scopes'
    /// snapshots without the coordinated module-wide rebuild.
    async fn route_interest(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError>;

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

    /// Cancel the in-memory dialer task for a single `(module,
    /// client_id, companion_key)` tuple. Used by the admin
    /// `delete-companion` handler before removing the on-disk
    /// record so the next drain tick doesn't re-spawn it from a
    /// not-yet-deleted file. No-op when the task isn't running.
    async fn cancel_companion_task(&self, module_id: &str, client_id: &str, companion_key: &str);

    /// Module ids with a recapture currently in flight (the
    /// per-module mutex set guarding `recapture_module`). Cheap and
    /// side-effect free — backs the `recapture_in_progress` field in
    /// `GET /_admin/status` + the companions endpoint so "is a
    /// recapture still running?" is a field, not a journal grep.
    async fn recapture_in_flight(&self) -> Vec<String>;

    /// Module ids whose `start` (bootstrap/rebootstrap) pass is
    /// currently running. Backs `bootstrap_in_progress`.
    async fn bootstrap_in_flight(&self) -> Vec<String>;

    /// Last rebootstrap result per module (`(module_id, result)`).
    /// Backs the `last_result` field in `/_admin/status`.
    async fn last_results(&self) -> Vec<(String, LastResult)>;
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
    use mitos_protocol::{AssetSelector, DomainSelector};
    // Raw output-address watch travels on the domain axis (it's a UTxO-output
    // subscription, not a protocol-event filter). Take it first — an address
    // interest carries `asset = Any`, which the asset match below would drop.
    if let DomainSelector::AtAddress(addr) = &item.domain {
        return Some(InterestPredicate::AtAddress(addr.clone()));
    }
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
/// Builds the state KV an instance runs against.
///
/// Takes an `InstanceKey`, not a module id: two instances of one module have
/// two KVs, and handing both the same one is exactly the overwrite this order
/// exists to make impossible.
pub type KvFactory = Arc<dyn Fn(&InstanceKey) -> state_kv::ModuleKv + Send + Sync>;

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
/// Where a host gets its chain-data fallback provider.
///
/// A named choice rather than an ambient lookup, because "did this
/// host just make a network call?" is not something a caller should
/// have to infer from process environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackSource {
    /// Process configuration — `MITOS_FALLBACK_PROVIDER`, which
    /// defaults to Koios. Production.
    FromEnv,
    /// No fallback: the wired data plane is the only source of chain
    /// data.
    ///
    /// Exists for tests. A test that wires a stub plane is asserting
    /// "no chain data is available"; inheriting the production default
    /// meant the host quietly resolved prior outputs over the real
    /// network instead — ~0.5s per lookup, which turned three
    /// integration tests into network-dependent 7-second affairs that
    /// failed against their own fixed sleeps.
    Disabled,
}

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
    /// A slot is an INSTANCE, not a module. Two instances of one module run
    /// side by side with separate followers, cursors and KVs.
    slots: Arc<Mutex<HashMap<InstanceKey, RunningSlotV2>>>,
    /// Which instance the `&str` API last STARTED for each module id.
    ///
    /// Not derivable from `current.toml`: `replace(id)` runs AFTER
    /// `activate` has renamed K1 → K2, so "stop the current instance" would
    /// look for K2 and find nothing running while K1's follower carried on.
    /// The facade records what it started, and that is what it stops.
    facade_slots: Arc<Mutex<HashMap<String, InstanceKey>>>,
    /// Per-running-INSTANCE dynamic-interest channel senders. The
    /// matching receiver lives inside each instance's follower task
    /// where it's multiplexed with tip events. Held in a
    /// `std::sync::Mutex` (not `tokio::sync`) because every
    /// access is a quick map insert/remove/get — no `.await` ever
    /// held across the guard.
    interest_senders: Arc<
        std::sync::Mutex<HashMap<InstanceKey, tokio::sync::mpsc::UnboundedSender<InterestUpdate>>>,
    >,
    /// Where `start` resolves the chain-data fallback provider from.
    fallback_source: FallbackSource,
    /// Set of module ids with an in-flight recapture. Mutual
    /// exclusion is per-module: a second `recapture_module(id)`
    /// while the first is mid-flight returns
    /// `PlatformError::RecaptureInProgress`. Uses a `HashSet` +
    /// std `Mutex` rather than per-key mutex because the only
    /// operations are check-insert-or-reject and remove — no
    /// awaiting needed.
    recapture_in_flight: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Module ids whose `start` (bootstrap/rebootstrap pass) is in
    /// flight. Backs `bootstrap_in_progress` in `/_admin/status`.
    bootstrap_in_flight: Arc<std::sync::Mutex<HashSet<String>>>,
    /// Last rebootstrap result per module (survives across the bounded
    /// events ring + host restarts within a process). Backs the
    /// `last_result` field in `/_admin/status`.
    last_results: Arc<std::sync::Mutex<HashMap<String, LastResult>>>,
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
    /// Shared operational-events ring (recapture/trap). Set after
    /// construction via `set_event_ring` (OnceLock, like `dialer`).
    /// `None` (unset) → events aren't recorded (artifact-only / tests).
    event_ring: std::sync::OnceLock<crate::events::EventRing>,
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
            facade_slots: Arc::new(Mutex::new(HashMap::new())),
            interest_senders: Arc::new(std::sync::Mutex::new(HashMap::new())),
            recapture_in_flight: Arc::new(std::sync::Mutex::new(HashSet::new())),
            bootstrap_in_flight: Arc::new(std::sync::Mutex::new(HashSet::new())),
            last_results: Arc::new(std::sync::Mutex::new(HashMap::new())),
            dialer: std::sync::OnceLock::new(),
            event_ring: std::sync::OnceLock::new(),
            fallback_source: FallbackSource::FromEnv,
        }
    }

    /// Choose where this host gets its chain-data fallback provider.
    /// Defaults to [`FallbackSource::FromEnv`]; see that type for why
    /// the override exists.
    pub fn with_fallback_source(mut self, source: FallbackSource) -> Self {
        self.fallback_source = source;
        self
    }

    /// Inject the shared operational-events ring. `&self` via
    /// `OnceLock` (like `set_dialer`) so the lifecycle constructor's
    /// signature stays put. Call once at bundle startup **before**
    /// `auto_resume` so spawned followers capture the same ring the
    /// admin router reads. Unset → events simply aren't recorded.
    pub fn set_event_ring(&self, ring: crate::events::EventRing) {
        let _ = self.event_ring.set(ring);
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
    ///
    /// `rebootstrap`: when `true`, invoke the module's
    /// `rebootstrap` export after the manifest bootstrap pass —
    /// used by the recapture flow so a self-bootstrapping module
    /// re-emits its own state. All non-recapture callers pass
    /// `false`.
    ///
    /// Returns the count of interest predicates the module
    /// re-bootstrapped (`0` unless `rebootstrap` was `true` and
    /// the module is self-bootstrapping) — the recapture flow
    /// surfaces it as `events_emitted`.
    pub async fn start(&self, id: &str, rebootstrap: bool) -> PlatformResult<u64> {
        // Stop whatever the facade last started for this module id. After an
        // upload that is K1, while `current.toml` already says K2 — see
        // `facade_slots`.
        self.stop(id).await?;
        let key = self
            .storage
            .current_instance(id)?
            .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;
        let started = self.start_instance(&key, rebootstrap).await?;
        self.facade_slots.lock().await.insert(id.to_owned(), key);
        Ok(started)
    }

    /// Refuse a whole-module operation on a module that has more than one
    /// instance directory.
    ///
    /// `recapture_module` and `evict_module` were both written when a module
    /// WAS an instance: one wipes bootstrap flags and restarts, the other
    /// removes the module directory. Neither has a defined meaning across
    /// two instances, and guessing would either restart the wrong follower
    /// or delete state nobody asked about.
    fn refuse_if_forked(&self, id: &str) -> PlatformResult<()> {
        let module = mitos_lifecycle::ids::ModuleId::parse(id)
            .map_err(|e| PlatformError::Decode(e.to_string()))?;
        let found = self.storage.list_instances(&module)?.len();
        if found > 1 {
            return Err(PlatformError::MultipleInstances {
                module: id.to_owned(),
                found,
            });
        }
        Ok(())
    }

    /// Start ONE instance. The `&str` API is a thin resolver over this.
    pub async fn start_instance(
        &self,
        key: &InstanceKey,
        rebootstrap: bool,
    ) -> PlatformResult<u64> {
        let id: &str = key.module.as_str();
        self.stop_instance(key).await?;

        // Mark the module as bootstrapping for the duration of `start`
        // (bootstrap + optional rebootstrap pass). Drop-guard clears it
        // on any exit. Backs `bootstrap_in_progress` in the admin API.
        // Keyed by module id, like `/_admin/status` itself.
        {
            let mut bf = self
                .bootstrap_in_flight
                .lock()
                .expect("bootstrap_in_flight mutex poisoned");
            bf.insert(id.to_owned());
        }
        let _bootstrap_guard = InFlightGuard {
            set: self.bootstrap_in_flight.clone(),
            id: id.to_owned(),
        };

        // From the CAS, by the instance's OWN digests — not through the
        // module's current pointer, which may already name another
        // instance.
        let manifest = self
            .storage
            .read_artifact_manifest(&key.wasm)?
            .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;

        // Refuse to START a module whose manifest doesn't satisfy this
        // host's ABI — including the WIT revision its bindings were
        // generated from.
        //
        // Activation already checks this, but activation happened
        // against whatever host was running at the time. A module
        // activated legitimately last week must not keep running after
        // the host's WIT moves underneath it, and auto-load can't undo
        // that: by then the module is already in storage, and the only
        // lever there is destructive removal (losing the module's
        // cursor and state). Enforcing at start is the non-destructive
        // equivalent — the artifact stays put, it just doesn't run, and
        // rebuilding it is the whole remedy.
        manifest.validate_abi_against_host(&[(
            crate::registry_v2::HOST_ABI_MAJOR_V2,
            "mitos:platform-v2/mitos-module-v2",
        )])?;

        let wasm_path = self.storage.artifact_path(&key.wasm);
        if !wasm_path.exists() {
            return Err(PlatformError::Decode(format!(
                "no artifact in the content store for {id} ({})",
                key.wasm
            )));
        }

        let registry = Arc::new(ModuleRegistryV2::load_from_path(
            self.engine.clone(),
            id.to_owned(),
            &wasm_path,
        )?);

        let (sink, events_rx) = (self.emitter_factory)();

        // Wrap the shared data plane with the aux-data cache so
        // `tx_metadata` calls from the module check the persistent
        // cache before hitting the dolos archive (7-day window).
        // Failures to open the cache degrade gracefully to a plain
        // passthrough — same behaviour as before this was added.
        let cache_opt = match self.storage.indexer_data_cache() {
            Ok(c) => {
                tracing::debug!(module = %id, "indexer_data_cache opened");
                Some(Arc::new(c))
            }
            Err(e) => {
                tracing::warn!(
                    module = %id,
                    error = %e,
                    "indexer_data_cache unavailable; tx_metadata/datum bypass cache",
                );
                None
            }
        };
        let fallback_opt = match self.fallback_source {
            FallbackSource::FromEnv => crate::fallback::shared(),
            FallbackSource::Disabled => None,
        };
        if fallback_opt.is_some() {
            tracing::info!(module = %id, "chain-data fallback provider enabled");
        }
        // The local chunk-store index sits ahead of the remote fallback for
        // aux-data; absent, aux-data lookups just take the remote path.
        let local_index_opt = crate::local_tx_index::shared();
        let caching_plane: Arc<dyn DataPlaneFacade> =
            Arc::new(crate::host_fns::CachingDataPlane::new(
                self.data_plane.clone(),
                cache_opt,
                local_index_opt,
                fallback_opt.clone(),
            ));
        // The config is already in the CAS under the instance's own digest —
        // it is half of what identifies this instance, so it is read from the
        // key rather than from the module's current pointer. `None` means the
        // instance runs with no config, which `init(&[])` distinguishes from
        // an empty one.
        let config = match &key.config {
            Some(sha) => self.storage.read_config_bytes(sha)?.unwrap_or_default(),
            None => Vec::new(),
        };

        // The backfill plane (`COLD_START_TRAP_ISOLATION.md`): a factory
        // that builds fresh, *disposable* instances for unbounded
        // cold-start work — recapture rebootstrap here, onboard
        // cold-starts in the follower. It shares the live `sink` (so
        // backfill snapshots land on the same drain task) and state-kv
        // (a shared `Arc<Database>` per module), so a disposable
        // instance's output reaches companions and the live instance
        // sees the refilled state.
        let backfill = crate::backfill_v2::BackfillFactory {
            instance: key.clone(),
            registry: registry.clone(),
            caching_plane: caching_plane.clone(),
            config: config.clone(),
            sink: sink.clone(),
            kv_factory: self.kv_factory.clone(),
            storage: self.storage.clone(),
            budget: self.budget,
        };

        // Live dispatch instance — born clean and never runs unbounded
        // backfill work (Fix D, the two-plane split). All cold-start /
        // recapture scans run on the backfill plane's disposable
        // instances, so a scan trap can never poison this instance and
        // brick live holder tracking. The trap-context logger is
        // per-instance; the follower gets the one matching this driver.
        let (mut driver, trap_logger) = backfill.instantiate(None).await?;

        // Watched-UTxO index for this module — shared between the
        // driver (writer: insert matched produced / remove consumed)
        // and the follower's fallback plane (reader: scope the Maestro
        // prior-output fallback). One `Arc` per module, persisted to
        // `watched.redb`. See `docs/design/WATCHED_UTXO_INDEX.md`.
        let watched_index = self.storage.watched_index_at(key);
        driver.set_watched(Some(watched_index.clone()));

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
                &manifest.interest.payment_credentials,
            );
            driver.set_interest(interest.clone());

            // Hand the bootstrap orchestrator a mutable
            // reference to the module's state-kv (for the
            // per-address completion flags) and the data
            // plane facade (for current-state lookups).
            // Synthetic events flow through the driver with
            // refuel-per-batch so per-call fuel stays
            // bounded.
            let mut bootstrap_kv = (self.kv_factory)(key);
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

        // Recapture re-bootstrap: after the host-side manifest
        // bootstrap pass, let a self-bootstrapping module re-emit
        // its own state by re-running its cold-start over its
        // restored interest. No-op for event-driven modules
        // (their refill is `run_bootstrap` above). Only the
        // recapture flow passes `rebootstrap = true`.
        //
        // This runs on a DISPOSABLE backfill instance, NOT the live
        // `driver` above (Fix D, `COLD_START_TRAP_ISOLATION.md`): the
        // cold-start scan is unbounded and may trap, and a trap poisons
        // the wasm instance — so it must never touch the instance that
        // becomes the live follower. The pump re-instantiates +
        // recycles internally to grind a large policy to completion
        // without exhausting a fixed re-instantiation budget.
        let mut rebootstrap_count: u64 = 0;
        if rebootstrap {
            let rebootstrap_started = std::time::Instant::now();
            // Recapture = full re-scan of every tracked policy,
            // ignoring any pending onboard scope.
            let result = crate::backfill_v2::run_rebootstrap_pump(
                &backfill,
                crate::bindings_v2::RebootstrapMode::Full,
            )
            .await;
            rebootstrap_count = result.ingested;
            if let Some(ring) = self.event_ring.get() {
                ring.record(
                    id,
                    crate::events::EventKind::RebootstrapCompleted {
                        utxos_ingested: result.ingested,
                    },
                );
            }
            if let Ok(mut results) = self.last_results.lock() {
                results.insert(
                    id.to_owned(),
                    LastResult {
                        kind: "rebootstrap",
                        utxos_ingested: result.ingested,
                        duration_ms: rebootstrap_started.elapsed().as_millis() as u64,
                        outcome: result.outcome,
                        at_unix: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0),
                    },
                );
            }
        }

        // Spawn the v2 follower.
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();
        let persisted_cursor = self.storage.read_cursor_at(key)?;
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
        let chain_plane: Arc<crate::maestro_fallback_plane::MaestroFallbackPlane<P>> =
            Arc::new(crate::maestro_fallback_plane::MaestroFallbackPlane::new(
                self.chain_plane.clone(),
                fallback_opt,
                Some(watched_index.clone()),
                crate::maestro_fallback_plane::GateMode::from_env(),
            ));
        let follower_storage = self.storage.clone();
        // The follower carries the INSTANCE it runs as, so its cursor writes
        // and its KV opens address that instance and not "the module".
        let follower_key = key.clone();
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
            senders.insert(key.clone(), interest_tx);
        }

        let follower_kv_factory = self.kv_factory.clone();
        let follower_trap_logger = trap_logger.clone();
        let follower_event_ring = self.event_ring.get().cloned();
        // Page-oriented modules cold-start a freshly-added predicate
        // via the budget-safe chunked `rebootstrap` pump; others use
        // the host's inline synthetic bootstrap. See
        // `manifest::is_chunked_cold_start_module` +
        // `follower_v2::apply_interest_update`.
        let follower_chunked_cold_start = crate::manifest::is_chunked_cold_start_module(id);
        let exit_event_ring = self.event_ring.get().cloned();
        let task = tokio::spawn(async move {
            let result = run_chain_follower_v2(
                driver,
                subscription,
                interest_rx,
                cancel_for_task,
                chain_plane,
                follower_storage,
                follower_key,
                follower_kv_factory,
                follower_trap_logger,
                follower_event_ring,
                follower_chunked_cold_start,
                backfill,
            )
            .await;
            match &result {
                Err(e) => {
                    tracing::error!(
                        module = %id_for_log,
                        error = %e,
                        "v2 follower task exited with error",
                    );
                    // Surface the death on the event ring so
                    // `/_admin/events` + alerting see it — a dead
                    // follower otherwise looks like a quiet module
                    // (queued=0). The watchdog restarts it on the
                    // next tick; `list_running` reports truthfully
                    // in the meantime via `is_finished()`.
                    if let Some(ring) = &exit_event_ring {
                        ring.record(
                            id_for_log.as_str(),
                            crate::events::EventKind::FollowerExited {
                                error: e.to_string(),
                            },
                        );
                    }
                }
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
        let drain_key = key.clone();
        let drain_cancel = cancel.clone();
        let drain_task = tokio::spawn(async move {
            run_emit_drain(drain_storage, drain_key, events_rx, drain_cancel).await;
        });

        let mut slots = self.slots.lock().await;
        slots.insert(
            key.clone(),
            RunningSlotV2 {
                sha: manifest.module.sha256,
                task,
                cancel,
                drain_task,
            },
        );
        tracing::info!(module = %id, instance = %key.dir_name(), "v2 follower started");
        Ok(rebootstrap_count)
    }

    pub async fn replace(&self, id: &str) -> PlatformResult<()> {
        self.start(id, false).await.map(|_| ())
    }

    /// Stop whatever the `&str` API last started for this module id.
    ///
    /// NOT "the current instance": after an upload's rename the current
    /// instance is K2 while the running follower is still K1, and looking up
    /// K2 would leave K1 running for ever. See `facade_slots`.
    pub async fn stop(&self, id: &str) -> PlatformResult<()> {
        let key = self.facade_slots.lock().await.remove(id);
        match key {
            Some(key) => self.stop_instance(&key).await,
            None => Ok(()),
        }
    }

    /// Stop ONE instance, leaving every other instance of the module running.
    pub async fn stop_instance(&self, key: &InstanceKey) -> PlatformResult<()> {
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
            senders.remove(key);
        }
        let slot = {
            let mut slots = self.slots.lock().await;
            slots.remove(key)
        };
        if let Some(slot) = &slot {
            slot.cancel.cancel();
        }
        let sha = match slot {
            Some(slot) => {
                let _ = slot.task.await;
                let _ = slot.drain_task.await;
                Some(slot.sha)
            }
            None => None,
        };
        // Evict THIS instance's cached handles so a future start re-opens
        // redb cleanly — and only this instance's, or stopping one would
        // close the other's files underneath it.
        //
        // WITH OR WITHOUT a slot, and the second case is the common one:
        // after an upload the running follower is K1, but `repoint_locked`
        // has already re-keyed its handles to K2. `stop(id)` stops K1 and
        // closes K1's entries — there are none — and then `start_instance(K2)`
        // arrives here with no K2 slot. Returning early would hand the new
        // follower the old follower's handles on every deploy, where v2
        // re-opened them. Nothing else can be holding them: the only follower
        // that did has just been awaited above, by whichever call stopped it.
        self.storage.close_cursor_at(key);
        self.storage.close_kv_at(key);
        self.storage.close_watched_at(key);
        let Some(sha) = sha else {
            return Ok(());
        };
        tracing::info!(
            module = %key.module,
            instance = %key.dir_name(),
            sha = %sha,
            "v2 module stopped",
        );
        Ok(())
    }

    pub async fn list(&self) -> Vec<String> {
        let slots = self.slots.lock().await;
        let mut ids: Vec<String> = slots.keys().map(|k| k.module.to_string()).collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Every instance with a slot, running or not.
    pub async fn list_instances(&self) -> Vec<InstanceKey> {
        let slots = self.slots.lock().await;
        slots.keys().cloned().collect()
    }

    /// Module ids whose follower task is actually alive. Unlike
    /// [`list`], a slot whose follower exited (dispatch error, trap
    /// cascade) is excluded — the slot lingers in the map (nothing
    /// polls the `JoinHandle`), so raw key listing over-reports
    /// liveness. This is the signal `/_admin/status.running`, the
    /// watchdog, and `mitos_module_running` are built on.
    pub async fn list_running(&self) -> Vec<String> {
        let slots = self.slots.lock().await;
        // Module ids, deduplicated: every `&str` consumer — the watchdog,
        // `/_admin/status`, `mitos_module_running` — asks "is this module
        // running", and two instances of it are still one module running.
        let mut ids: Vec<String> = slots
            .iter()
            .filter(|(_, slot)| !slot.task.is_finished())
            .map(|(key, _)| key.module.to_string())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Running instances of one module, truthfully (a follower that exited
    /// is excluded, as in `list_running`).
    pub async fn running_instances_of(&self, module: &str) -> Vec<InstanceKey> {
        let slots = self.slots.lock().await;
        slots
            .iter()
            .filter(|(key, slot)| key.module.as_str() == module && !slot.task.is_finished())
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub async fn stop_all(&self) {
        let keys: Vec<InstanceKey> = {
            let slots = self.slots.lock().await;
            slots.keys().cloned().collect()
        };
        for key in keys {
            let _ = self.stop_instance(&key).await;
        }
        self.facade_slots.lock().await.clear();
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
            match self.start(&id, false).await {
                Ok(_) => tracing::info!(module = %id, "auto-resumed"),
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
    /// Snapshot of module ids with an in-flight recapture. Reads the
    /// per-module mutex set; degrades to empty on lock poisoning.
    pub fn recaptures_in_flight(&self) -> Vec<String> {
        self.recapture_in_flight
            .lock()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Snapshot of module ids whose bootstrap/rebootstrap is running.
    pub fn bootstraps_in_flight(&self) -> Vec<String> {
        self.bootstrap_in_flight
            .lock()
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Snapshot of the last rebootstrap result per module.
    pub fn last_results_snapshot(&self) -> Vec<(String, LastResult)> {
        self.last_results
            .lock()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default()
    }

    pub async fn recapture_module(
        &self,
        id: &str,
        reason: Option<String>,
        companion_timeout: Duration,
    ) -> PlatformResult<RecaptureOutcome> {
        // Recapture is a WHOLE-MODULE operation written for one instance: it
        // wipes the bootstrap flags and restarts, which with two instances
        // would mean wiping one's flags and restarting the other. Refuse
        // rather than guess; making it instance-aware is the control plane's.
        // With one instance — every module in production — nothing changes.
        //
        // Asked BEFORE the dialer: "this module is forked" is the true reason
        // the call is refused, and it must not be masked by whether an
        // unrelated subsystem happens to be wired.
        self.refuse_if_forked(id)?;

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
        let _release = InFlightGuard {
            set: self.recapture_in_flight.clone(),
            id: id.to_owned(),
        };

        tracing::info!(
            module = %id,
            reason = ?reason,
            "recapture: starting"
        );

        // 2. Send Recapture + await RecaptureReady from each.
        //
        // A companion that doesn't ack in time (dead registration, or
        // a cold/hibernated CF Durable Object slow to wake) must NOT
        // abort the whole recapture — that lets one stale collection
        // wedge every other collection's refill. Proceed with the
        // companions that DID ack; the laggards converge on their next
        // snapshot (re-emitted to all via the broadcast in step 4) or
        // their next reconnect — snapshots are chain-point idempotent.
        // Only hard transport errors (no subscribers, frame-send
        // failure) abort.
        let ready = match dialer
            .recapture_module(id, reason.clone(), companion_timeout)
            .await
        {
            Ok(s) => s,
            Err(crate::dialer::RecaptureError::Timeout {
                ready_companions,
                timed_out_companions,
                ..
            }) => {
                tracing::warn!(
                    module = %id,
                    ready_count = ready_companions.len(),
                    timed_out = ?timed_out_companions,
                    "recapture: some companions didn't ack in time; proceeding with \
                     bootstrap-refill for the rest (laggards converge on next snapshot)"
                );
                crate::dialer::RecaptureSummary {
                    module: id.to_owned(),
                    ready_companions,
                }
            }
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
            // One instance, guaranteed by `refuse_if_forked` above.
            let key = self
                .storage
                .current_instance(id)?
                .ok_or_else(|| PlatformError::Decode(format!("no manifest for {id}")))?;
            let mut kv = (self.kv_factory)(&key);
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
        let events_emitted = match self.start(id, true).await {
            Ok(n) => n,
            Err(e) => {
                tracing::error!(
                    module = %id,
                    error = %e,
                    "recapture: follower restart failed; attempting recovery start"
                );
                // `start` is stop-first: at this point the old
                // follower is already gone, so returning the error
                // as-is would leave the module STOPPED (silently
                // dead until a restart — the 2026-06/07
                // holder-distribution outage). Salvage with a plain
                // start: the bootstrap-done flags were cleared in
                // step 3, so a recovery start still performs the
                // full refill through the normal bootstrap pass.
                // The original error is still returned so the
                // recapture reports failed and alerting fires.
                match self.start(id, false).await {
                    Ok(_) => tracing::warn!(
                        module = %id,
                        "recapture: recovery start succeeded — module is live and \
                         the refill ran via bootstrap (flags were cleared); the \
                         recapture itself is still reported failed"
                    ),
                    Err(re) => tracing::error!(
                        module = %id,
                        error = %re,
                        "recapture: recovery start ALSO failed; module left \
                         stopped — watchdog will retry on its next tick"
                    ),
                }
                return Err(e);
            }
        };

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
            // Interest predicates the module re-emitted under
            // `rebootstrap` — 0 for event-driven modules, whose
            // refill is the host-side `run_bootstrap` pass.
            events_emitted,
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
        // Whole-module, like recapture: it removes the module directory,
        // which is every instance's state at once. Refuse a forked module
        // rather than destroy an instance nobody asked about.
        self.refuse_if_forked(id)?;

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
        self.storage.close_watched(id);

        tracing::info!(
            module = %id,
            cancelled_companions = cancelled.len(),
            "module evicted (slot stopped, dialer state cleared, DB handles closed)"
        );
        Ok(cancelled)
    }

    /// Retire ONE instance, leaving its siblings — and the module — alone.
    ///
    /// Unlike `evict_module`, this removes the directory itself: the
    /// instance-scoped teardown has no equivalent of the admin handler that
    /// used to do it, and splitting it would mean a caller that could forget.
    pub async fn evict_instance(&self, key: &InstanceKey) -> PlatformResult<Vec<String>> {
        let id: &str = key.module.as_str();
        let before = self.storage.list_instances(&key.module)?;
        if before.len() > 1 && self.storage.current_instance(id)?.as_ref() == Some(key) {
            return Err(PlatformError::CurrentInstanceHasSiblings {
                module: id.to_owned(),
                instance: key.dir_name(),
                siblings: before.len() - 1,
            });
        }

        // 1. Stop the follower, and stop calling this the facade's slot if
        //    that is what it was — otherwise a later `stop(id)` would try to
        //    stop a key whose directory has gone.
        self.stop_instance(key).await?;
        {
            let mut facade = self.facade_slots.lock().await;
            if facade.get(id) == Some(key) {
                facade.remove(id);
            }
        }

        // 2. The dialer's drain holds this instance's emissions store for the
        //    life of the process, through a status writer that owns a clone.
        //    A live redb handle is exactly what makes `remove_dir_all` fail,
        //    so ask it to let go and wait — `release_instance` drains the
        //    writer's queue before it replies. The wait is BOUNDED (this runs
        //    under the module's lifecycle lock): a drain stuck behind a
        //    hanging companion fails this reap into its retry instead. Every
        //    step above is a no-op the second time, and by then the queued
        //    release has been honoured.
        if let Some(dialer) = self.dialer.get() {
            match dialer.release_instance(key).await {
                crate::dialer::Released::Done => {}
                crate::dialer::Released::StillBusy => {
                    return Err(PlatformError::DrainBusy {
                        module: id.to_owned(),
                        instance: key.dir_name(),
                    });
                }
            }
        }

        // 3. Whatever this instance still owed its companions dies with the
        //    directory. Until WO-09 owns "what a departing instance still
        //    owes", this line is the only trace that it did.
        self.report_undelivered(key);

        // 4. Close the cached handles and remove the directory. Handles
        //    first: unlinking files under an open redb handle leaves a
        //    writer addressing an inode nothing can reach.
        self.storage.close_kv_at(key);
        self.storage.close_emissions_at(key);
        self.storage.close_cursor_at(key);
        self.storage.close_watched_at(key);
        self.storage.remove_instance_dir(key)?;

        // 5. If that was the module's last instance, the module goes too —
        //    the same teardown `evict_module` does, so reaping a module's
        //    only instance is byte-for-byte today's evict.
        if !self.storage.list_instances(&key.module)?.is_empty() {
            tracing::info!(
                module = %id,
                instance = %key.dir_name(),
                "instance evicted; the module and its siblings stand",
            );
            return Ok(Vec::new());
        }
        let cancelled = match self.dialer.get() {
            Some(dialer) => dialer.unregister_module(id).await,
            None => Vec::new(),
        };
        self.storage.close_kv(id);
        self.storage.close_emissions(id);
        self.storage.close_cursor(id);
        self.storage.close_watched(id);
        self.storage.remove_module_dir(id)?;
        tracing::info!(
            module = %id,
            instance = %key.dir_name(),
            cancelled_companions = cancelled.len(),
            "module's last instance evicted; the module went with it",
        );
        Ok(cancelled)
    }

    /// Count what is still `Queued` in an instance's emissions store and say
    /// so, loudly, before it is deleted. Best-effort: a store that will not
    /// open is reported as unknown rather than failing the reap, because the
    /// alternative is an instance that can never be retired.
    fn report_undelivered(&self, key: &InstanceKey) {
        let id: &str = key.module.as_str();
        // Do not OPEN a store that was never written — `EmissionsStore::open`
        // creates the file, and creating one to count zero rows in it,
        // moments before deleting the directory, is silly.
        if !self
            .storage
            .instance_dir(key)
            .join("emissions.redb")
            .exists()
        {
            return;
        }
        let queued = self
            .storage
            .emissions_store_at(key)
            .and_then(|store| {
                store
                    .list_filtered(|r| matches!(r.status, crate::emissions::EmissionStatus::Queued))
            })
            .map(|rows| rows.len());
        match queued {
            Ok(0) => {}
            Ok(queued) => {
                tracing::warn!(
                    module = %id,
                    instance = %key.dir_name(),
                    queued,
                    "reaping an instance with undelivered emissions; they are being DELETED",
                );
                if let Some(ring) = self.event_ring.get() {
                    ring.record(
                        id,
                        crate::events::EventKind::InstanceReapedWithQueuedRows {
                            instance: key.dir_name(),
                            queued,
                        },
                    );
                }
            }
            Err(e) => tracing::warn!(
                module = %id,
                instance = %key.dir_name(),
                error = %e,
                "reaping an instance whose emissions store could not be counted",
            ),
        }
    }
}

/// Drop-guard for a per-module in-flight set. Ensures the module is
/// removed from the set even if the guarded body panics or
/// early-returns. Used for both `recapture_in_flight` and
/// `bootstrap_in_flight`.
struct InFlightGuard {
    set: Arc<std::sync::Mutex<HashSet<String>>>,
    id: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut set) = self.set.lock() {
            set.remove(&self.id);
        }
    }
}

/// Last rebootstrap result for a module, retained by the host so
/// `/_admin/status` can answer "how did the last (re)bootstrap go?"
/// without depending on the bounded events ring (which loses old
/// entries + resets on restart).
#[derive(Debug, Clone)]
pub struct LastResult {
    /// What ran (`"rebootstrap"`).
    pub kind: &'static str,
    pub utxos_ingested: u64,
    pub duration_ms: u64,
    /// `"completed"` (drained to `done`), `"partial"` (hit a cap /
    /// trap-abort), or `"failed"` (module returned an error).
    pub outcome: &'static str,
    /// Unix seconds when it finished.
    pub at_unix: u64,
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

/// A recording [`ModuleHostHandle`] for tests that exercise the lifecycle
/// without a wasmtime engine or a data plane.
///
/// ONE fake, shared by every test module that needs one. `ModuleHostV2` is the
/// trait's only real implementor and it needs an engine and a chain, so
/// before this the watchdog and the admin evict path had no test coverage at
/// all. Three copies of a fake is three chances for one of them to quietly
/// stop matching the trait.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Mutex as StdMutex;

    /// One recorded call. The lifecycle's whole job is deciding WHICH of
    /// these happen and when, so the assertions are almost always "was this
    /// module's follower touched, and how".
    ///
    /// The `&str` trio is the operator surface (`POST …/restart`, the CLI).
    /// The instance trio is what the `LifecycleManager` uses, and after WO-07
    /// it uses nothing else — `an_upload_never_uses_the_str_host_api` is the
    /// test that holds that.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum HostCall {
        Replace(String),
        Stop(String),
        Evict(String),
        StartInstance(InstanceKey),
        StopInstance(InstanceKey),
        EvictInstance(InstanceKey),
    }

    impl HostCall {
        /// Which module this touched. Every `FakeHost` helper is
        /// module-scoped — "was this module's follower started", not "which
        /// of its instances" — so the ~26 assertions written against them
        /// stand unchanged now that the manager addresses instances.
        pub(crate) fn module(&self) -> &str {
            match self {
                HostCall::Replace(id) | HostCall::Stop(id) | HostCall::Evict(id) => id,
                HostCall::StartInstance(key)
                | HostCall::StopInstance(key)
                | HostCall::EvictInstance(key) => key.module.as_str(),
            }
        }
    }

    /// An [`InterestRouter`] that records which modules had their
    /// scan-interest re-asserted.
    ///
    /// A REBUILT instance starts with an empty KV, and the module's
    /// scan-interest lives in that KV — so "starting one re-asserts it" is
    /// load-bearing, and needs something that can say whether it happened.
    #[derive(Default)]
    pub(crate) struct RecordingRouter(StdMutex<Vec<String>>);

    impl RecordingRouter {
        pub(crate) fn routed(&self) -> Vec<String> {
            self.0.lock().expect("recording router mutex").clone()
        }
    }

    #[async_trait::async_trait]
    impl InterestRouter for RecordingRouter {
        async fn route_interest(
            &self,
            module_id: &str,
            _op: mitos_protocol::InterestOp,
            _items: Vec<mitos_protocol::Interest>,
        ) -> Result<(), InterestRouteError> {
            self.0
                .lock()
                .expect("recording router mutex")
                .push(module_id.to_owned());
            Ok(())
        }
    }

    /// What the fake believes is running.
    ///
    /// The `&str` API cannot name an instance — the fake resolves nothing —
    /// so a module-addressed start records a module-addressed slot, and
    /// `list_running` flattens both, which is all any `&str` caller asks.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum Slot {
        Module(String),
        Instance(InstanceKey),
    }

    impl Slot {
        fn module(&self) -> &str {
            match self {
                Slot::Module(id) => id,
                Slot::Instance(key) => key.module.as_str(),
            }
        }
    }

    #[derive(Default)]
    struct Inner {
        calls: Vec<HostCall>,
        running: HashSet<Slot>,
        /// Modules whose `replace` fails — an effect that fails must not roll
        /// the lifecycle record back.
        fail_replace: HashSet<String>,
        /// Modules whose `evict_module` fails — a `Reap` whose host-side
        /// teardown failed must leave the directory alone and be retried.
        fail_evict: HashSet<String>,
        /// Instances whose `stop_instance` fails and leaves the follower
        /// RUNNING — so anything that needed it stopped must not proceed.
        fail_stop: HashSet<InstanceKey>,
        /// Runs inside `stop`, so a test can record what the WORLD looked
        /// like at the moment a follower was stopped — an ordering against
        /// something that is not a host call.
        on_stop: Option<Arc<dyn Fn() + Send + Sync>>,
    }

    #[derive(Clone)]
    pub(crate) struct FakeHost {
        inner: Arc<StdMutex<Inner>>,
        /// `evict_instance` REMOVES the instance directory on the real host —
        /// that is why a reap whose teardown failed must leave it alone. A
        /// fake that only recorded the call would make
        /// `a_reap_that_could_not_finish_is_retried_on_the_next_tick` and
        /// `an_abandoned_dev_only_instance_drains_stops_and_is_reaped` assert
        /// nothing, so the fake does the storage half for real.
        storage: ModuleStorage,
    }

    impl FakeHost {
        pub(crate) fn new(storage: ModuleStorage) -> Self {
            Self {
                inner: Arc::new(StdMutex::new(Inner::default())),
                storage,
            }
        }

        pub(crate) fn calls(&self) -> Vec<HostCall> {
            self.inner.lock().expect("fake host mutex").calls.clone()
        }

        /// True when this module's follower was started, however addressed.
        pub(crate) fn started(&self, module: &str) -> bool {
            self.saw(module, |c| {
                matches!(c, HostCall::Replace(_) | HostCall::StartInstance(_))
            })
        }

        pub(crate) fn stopped(&self, module: &str) -> bool {
            self.saw(module, |c| {
                matches!(c, HostCall::Stop(_) | HostCall::StopInstance(_))
            })
        }

        pub(crate) fn evicted(&self, module: &str) -> bool {
            self.saw(module, |c| {
                matches!(c, HostCall::Evict(_) | HostCall::EvictInstance(_))
            })
        }

        fn saw(&self, module: &str, which: impl Fn(&HostCall) -> bool) -> bool {
            self.calls()
                .iter()
                .any(|call| call.module() == module && which(call))
        }

        pub(crate) fn clear(&self) {
            self.inner.lock().expect("fake host mutex").calls.clear();
        }

        /// Make starting this module's follower fail, as a crash-looping one
        /// does. Module-scoped, so it covers both addressings.
        pub(crate) fn fail_replace(&self, module: &str) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .fail_replace
                .insert(module.to_owned());
        }

        /// Make evicting this module — or any instance of it — fail, as a
        /// teardown that could not close its redb handles does.
        pub(crate) fn fail_evict(&self, module: &str) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .fail_evict
                .insert(module.to_owned());
        }

        /// Make `stop_instance` fail for `key`, leaving its follower running.
        pub(crate) fn fail_stop_instance(&self, key: &InstanceKey) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .fail_stop
                .insert(key.clone());
        }

        pub(crate) fn on_stop(&self, observe: impl Fn() + Send + Sync + 'static) {
            self.inner.lock().expect("fake host mutex").on_stop = Some(Arc::new(observe));
        }

        /// The teardown that was failing now works.
        pub(crate) fn heal_evict(&self, module: &str) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .fail_evict
                .remove(module);
        }

        /// The follower task exited on its own — no `stop` call, it is just
        /// gone. This is the class the watchdog exists to close: a dispatch
        /// error killing the task, or a cancelled `start` inside a recapture
        /// leaving the slot empty (the 2026-06 holder-distribution outage).
        ///
        /// Every instance of the module, because the callers that predate
        /// instances mean "this module's follower died".
        pub(crate) fn follower_died(&self, module: &str) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .retain(|slot| slot.module() != module);
        }

        /// One instance's follower exited on its own, leaving its siblings
        /// running.
        pub(crate) fn instance_died(&self, key: &InstanceKey) {
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .remove(&Slot::Instance(key.clone()));
        }

        fn record(&self, call: HostCall) {
            self.inner.lock().expect("fake host mutex").calls.push(call);
        }

        /// `fail_*` sets are keyed by module: a test says "this module's
        /// follower will not start", not "this digest's".
        fn refuses(&self, which: impl Fn(&Inner) -> &HashSet<String>, module: &str) -> bool {
            which(&self.inner.lock().expect("fake host mutex")).contains(module)
        }

        /// Drop the slots a stop covers, then run the `on_stop` observer —
        /// OUTSIDE the mutex, because the observer reads the world (that is
        /// what it is for) and one of its readers is this fake.
        fn settle_stop(&self, covers: impl Fn(&Slot) -> bool) {
            let observe = {
                let mut inner = self.inner.lock().expect("fake host mutex");
                inner.running.retain(|slot| !covers(slot));
                inner.on_stop.clone()
            };
            if let Some(observe) = observe {
                observe();
            }
        }
    }

    #[async_trait::async_trait]
    impl ModuleHostHandle for FakeHost {
        async fn replace(&self, id: &str) -> PlatformResult<()> {
            self.record(HostCall::Replace(id.to_owned()));
            if self.refuses(|i| &i.fail_replace, id) {
                return Err(PlatformError::Decode(format!("fake replace failure: {id}")));
            }
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .insert(Slot::Module(id.to_owned()));
            Ok(())
        }

        async fn stop(&self, id: &str) -> PlatformResult<()> {
            self.record(HostCall::Stop(id.to_owned()));
            self.settle_stop(|slot| slot.module() == id);
            Ok(())
        }

        async fn stop_all(&self) {}

        async fn list_running(&self) -> Vec<String> {
            let mut ids: Vec<String> = self
                .inner
                .lock()
                .expect("fake host mutex")
                .running
                .iter()
                .map(|slot| slot.module().to_owned())
                .collect();
            ids.sort();
            ids.dedup();
            ids
        }

        async fn running_instances(&self, module: &str) -> Vec<InstanceKey> {
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .iter()
                .filter_map(|slot| match slot {
                    Slot::Instance(key) if key.module.as_str() == module => Some(key.clone()),
                    // A `&str`-started slot names no instance, so it cannot
                    // appear here. Nothing in the manager starts one after
                    // WO-07; the operator routes still can.
                    Slot::Instance(_) | Slot::Module(_) => None,
                })
                .collect()
        }

        async fn start_instance(&self, key: &InstanceKey) -> PlatformResult<()> {
            self.record(HostCall::StartInstance(key.clone()));
            let id = key.module.as_str();
            if self.refuses(|i| &i.fail_replace, id) {
                return Err(PlatformError::Decode(format!("fake replace failure: {id}")));
            }
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .insert(Slot::Instance(key.clone()));
            Ok(())
        }

        async fn stop_instance(&self, key: &InstanceKey) -> PlatformResult<()> {
            self.record(HostCall::StopInstance(key.clone()));
            if self
                .inner
                .lock()
                .expect("fake host mutex")
                .fail_stop
                .contains(key)
            {
                return Err(PlatformError::Decode(format!(
                    "fake stop failure: {}",
                    key.dir_name()
                )));
            }
            self.settle_stop(|slot| slot == &Slot::Instance(key.clone()));
            // The storage half, as the real `stop_instance` does it: a
            // follower that has stopped has let go of its redb handles.
            // WITHOUT this, a test that copies a stopped parent's state would
            // be copying a file whose writer is still open — which is exactly
            // the thing `state_prep`'s rule 1 exists to prevent, and a fake
            // that did not model it could not fail on it.
            self.storage.close_kv_at(key);
            self.storage.close_cursor_at(key);
            self.storage.close_watched_at(key);
            Ok(())
        }

        async fn evict_instance(&self, key: &InstanceKey) -> PlatformResult<Vec<String>> {
            self.record(HostCall::EvictInstance(key.clone()));
            let id = key.module.as_str();
            // Both refusals BEFORE anything is touched — a reap that says no
            // must leave the world exactly as it found it, which is the whole
            // point of `CurrentInstanceHasSiblings`.
            if self.refuses(|i| &i.fail_evict, id) {
                return Err(PlatformError::Decode(format!("fake evict failure: {id}")));
            }
            let before = self.storage.list_instances(&key.module)?;
            if before.len() > 1 && self.storage.current_instance(id)?.as_ref() == Some(key) {
                return Err(PlatformError::CurrentInstanceHasSiblings {
                    module: id.to_owned(),
                    instance: key.dir_name(),
                    siblings: before.len() - 1,
                });
            }
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .remove(&Slot::Instance(key.clone()));
            // The storage half, for real — see the field comment.
            self.storage.close_kv_at(key);
            self.storage.close_emissions_at(key);
            self.storage.close_cursor_at(key);
            self.storage.close_watched_at(key);
            self.storage.remove_instance_dir(key)?;
            if self.storage.list_instances(&key.module)?.is_empty() {
                self.storage.remove_module_dir(id)?;
            }
            Ok(Vec::new())
        }

        async fn recapture_module(
            &self,
            id: &str,
            _reason: Option<String>,
            _companion_timeout: Duration,
        ) -> PlatformResult<RecaptureOutcome> {
            Ok(RecaptureOutcome {
                module: id.to_owned(),
                companion_count: 0,
                events_emitted: 0,
            })
        }

        async fn route_interest(
            &self,
            _module_id: &str,
            _op: mitos_protocol::InterestOp,
            _items: Vec<mitos_protocol::Interest>,
        ) -> Result<(), InterestRouteError> {
            Ok(())
        }

        async fn evict_module(&self, id: &str) -> PlatformResult<Vec<String>> {
            self.record(HostCall::Evict(id.to_owned()));
            if self.refuses(|i| &i.fail_evict, id) {
                return Err(PlatformError::Decode(format!("fake evict failure: {id}")));
            }
            self.inner
                .lock()
                .expect("fake host mutex")
                .running
                .retain(|slot| slot.module() != id);
            Ok(Vec::new())
        }

        async fn cancel_companion_task(
            &self,
            _module_id: &str,
            _client_id: &str,
            _companion_key: &str,
        ) {
        }

        async fn recapture_in_flight(&self) -> Vec<String> {
            Vec::new()
        }

        async fn bootstrap_in_flight(&self) -> Vec<String> {
            Vec::new()
        }

        async fn last_results(&self) -> Vec<(String, LastResult)> {
            Vec::new()
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
        ModuleHostV2::list_running(self).await
    }
    async fn running_instances(&self, module: &str) -> Vec<InstanceKey> {
        ModuleHostV2::running_instances_of(self, module).await
    }
    async fn start_instance(&self, key: &InstanceKey) -> PlatformResult<()> {
        ModuleHostV2::start_instance(self, key, false)
            .await
            .map(drop)
    }
    async fn stop_instance(&self, key: &InstanceKey) -> PlatformResult<()> {
        ModuleHostV2::stop_instance(self, key).await
    }
    async fn evict_instance(&self, key: &InstanceKey) -> PlatformResult<Vec<String>> {
        ModuleHostV2::evict_instance(self, key).await
    }
    async fn route_interest(
        &self,
        module_id: &str,
        op: mitos_protocol::InterestOp,
        items: Vec<mitos_protocol::Interest>,
    ) -> Result<(), InterestRouteError> {
        <Self as InterestRouter>::route_interest(self, module_id, op, items).await
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
    async fn recapture_in_flight(&self) -> Vec<String> {
        ModuleHostV2::recaptures_in_flight(self)
    }
    async fn bootstrap_in_flight(&self) -> Vec<String> {
        ModuleHostV2::bootstraps_in_flight(self)
    }
    async fn last_results(&self) -> Vec<(String, LastResult)> {
        ModuleHostV2::last_results_snapshot(self)
    }
    async fn cancel_companion_task(&self, module_id: &str, client_id: &str, companion_key: &str) {
        if let Some(dialer) = self.dialer.get() {
            let cid = crate::dialer::CompanionId::new(module_id, client_id, companion_key);
            dialer.unregister(&cid).await;
        }
    }
}

/// Drain task — pull events off the per-module emit channel and
/// fan them out into the per-companion emissions store. Spawned
/// alongside the follower task in `start()` and cancelled in
/// `stop()`. (Moved here from the retired v1 `host.rs`.)
pub(crate) async fn run_emit_drain(
    storage: ModuleStorage,
    instance: InstanceKey,
    mut events_rx: tokio::sync::mpsc::UnboundedReceiver<emit::EmittedEvent>,
    cancel: CancellationToken,
) {
    let module_id = instance.module.to_string();
    let store = match storage.emissions_store_at(&instance) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(
                module = %module_id,
                instance = %instance.dir_name(),
                error = %e,
                "open EmissionsStore for drain failed; emit interception disabled for this instance"
            );
            return;
        }
    };
    // Per-companion interest projection cache. Lives for the
    // lifetime of the drain task so the fan-out filter doesn't
    // re-read + CBOR-decode every companion `.cbor` on each
    // emission. Refreshed per-file on mtime change (re-subscribe
    // rewrites the file). See `InterestCache`.
    let mut interest_cache = InterestCache::default();
    // …and the same shape for client rows, which the fan-out now consults
    // per emitted event.
    let mut row_cache = RowCache::default();
    // The hand-off watermarks, OWNED by this task for as long as it runs.
    //
    // Its only other writer is the lifecycle manager at a commit, and only
    // with this follower stopped — `stop_instance` awaits this very task, so
    // the two can never overlap. Loading it once and keeping it is therefore
    // not a cache that can go stale: while this task lives, nothing else
    // touches the file.
    //
    // A file that will not parse is reported and treated as NO watermarks.
    // That can deliver a block twice, which a companion tolerates (applies
    // are chain-point-idempotent); refusing to start the drain would lose
    // every block instead.
    let mut handoffs = match storage.read_handoffs_at(&instance) {
        Ok(handoffs) => handoffs,
        Err(e) => {
            tracing::error!(
                module = %module_id,
                instance = %instance.dir_name(),
                error = %e,
                "hand-off watermarks unreadable; this instance will withhold nothing, so a \
                 client it took over may be re-sent blocks its predecessor already delivered",
            );
            mitos_lifecycle::handoff::Handoffs::default()
        }
    };
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            event = events_rx.recv() => {
                let Some(event) = event else { return };
                drain_event(
                    &storage,
                    &store,
                    &instance,
                    event,
                    &mut interest_cache,
                    &mut row_cache,
                    &mut handoffs,
                );
            }
        }
    }
}

/// One emission, all the way through: watermarks, fan-out, watermarks again.
///
/// Its own function, and not the body of the `select!` arm, because the ORDER
/// of the three steps is the whole of the watermark's correctness and it is
/// the only part of the drain a test can reach without a chain.
fn drain_event(
    storage: &ModuleStorage,
    store: &crate::emissions::EmissionsStore,
    instance: &InstanceKey,
    event: emit::EmittedEvent,
    interest_cache: &mut InterestCache,
    row_cache: &mut RowCache,
    handoffs: &mut mitos_lifecycle::handoff::Handoffs,
) {
    let module_id = instance.module.as_str();
    let at = slot_of(&event.chain_point);
    // Resolved per EVENT, not once per task: an upload moves the module's
    // current instance under a running drain, and a row naming a renamed-away
    // instance follows whatever it is NOW.
    //
    // An ERROR here is not "unactivated" — this task is running an instance,
    // so the module plainly is. It fails OPEN, as the interest filter does:
    // with one instance that is exactly v2, and with two it is a duplicate
    // rather than silence. But it says so, per event, for as long as the
    // pointer is broken.
    let module_current = match storage.current_instance(module_id) {
        Ok(current) => current,
        Err(e) => {
            tracing::warn!(
                module = %module_id,
                instance = %instance.dir_name(),
                error = %e,
                "cannot resolve the module's current instance; delivering to every \
                 companion from this instance",
            );
            None
        }
    };
    if event.is_undo {
        // BEFORE the fan-out. A reorg below a watermark voids what the
        // client's PREVIOUS instance delivered on the abandoned fork, so the
        // blocks that replace `(to, watermark]` are new blocks nobody has
        // delivered — the watermark drops to the rollback point, and
        // everything above it must be admitted from here on.
        let changed = handoffs.rolled_back(at);
        persist_handoffs(storage, instance, handoffs, changed);
        // Chain rollback: broadcast an undo emission to every subscribed
        // companion (no interest filter — a reorg is unconditional; each
        // companion reverts its own state). An undo is NEVER withheld: it is
        // about state the companion already applied, whoever delivered it.
        fan_out_undo(
            storage,
            store,
            instance,
            module_current.as_ref(),
            event,
            row_cache,
        );
    } else {
        drain_one(
            storage,
            store,
            instance,
            module_current.as_ref(),
            event,
            interest_cache,
            row_cache,
            handoffs,
        );
    }
    // A watermark the chain has moved a whole stability window past can never
    // be lowered and never withholds anything again — it is only a line in a
    // file.
    let changed = handoffs.settle(at);
    persist_handoffs(storage, instance, handoffs, changed);
}

/// An emission's slot, as a hand-off watermark measures it.
///
/// `ChainPoint::Origin` is slot 0 — before every block — which is what both
/// callers want: a forward emission at Origin is one the client's previous
/// instance necessarily delivered, and a rollback to Origin voids everything.
fn slot_of(point: &mitos_protocol::ChainPoint) -> mitos_lifecycle::ids::Slot {
    mitos_lifecycle::ids::Slot(point.slot().unwrap_or(0))
}

/// Write the watermarks back when — and only when — they changed.
///
/// A failure is reported and carried past: the in-memory copy is what this
/// task will go on using, and it is correct. What is lost is durability
/// across a restart, which costs at most a re-delivery.
fn persist_handoffs(
    storage: &ModuleStorage,
    instance: &InstanceKey,
    handoffs: &mitos_lifecycle::handoff::Handoffs,
    changed: mitos_lifecycle::handoff::Changed,
) {
    // `match`, never `== Yes`: a third answer would arrive as a compile
    // error here rather than as a silent skip.
    match changed {
        mitos_lifecycle::handoff::Changed::No => {}
        mitos_lifecycle::handoff::Changed::Yes => {
            if let Err(e) = storage.write_handoffs_at(instance, handoffs) {
                tracing::error!(
                    module = %instance.module,
                    instance = %instance.dir_name(),
                    error = %e,
                    "persisting hand-off watermarks failed; a restart may re-deliver blocks \
                     this instance has already withheld",
                );
            }
        }
    }
}

/// Per-companion interest projection cache for the emit fan-out
/// filter (`drain_one`). Keyed by companion `.cbor` path; an entry
/// is refreshed when the file's mtime changes (a re-subscribe
/// rewrites the file with a new interest set).
///
/// `watched == None` means **unbounded** — deliver everything. That
/// covers both an `Any`/`Fingerprint` interest *and* the
/// no-declared-interest case, so the filter is never *narrower* than
/// the pre-filter broadcast: a companion only loses an emission when
/// it has explicitly declared one or more bounded policy interests
/// that the event's policy isn't in. Any read/decode failure also
/// resolves to `None` (fail open — never drop a delivery because the
/// filter couldn't load).
#[derive(Default)]
pub(crate) struct InterestCache {
    entries: HashMap<std::path::PathBuf, CachedInterest>,
}

struct CachedInterest {
    /// File mtime at cache time; `None` if unstattable. A changed
    /// (or newly-readable) mtime invalidates the entry.
    mtime: Option<std::time::SystemTime>,
    /// Watched policy hex set, or `None` for unbounded interest.
    watched: Option<HashSet<String>>,
}

impl InterestCache {
    /// Whether the companion persisted at `path` wants an event
    /// keyed by `event_policy_hex`.
    ///
    /// - `event_policy_hex == None` (event carries no policy key):
    ///   always yes — the host can't route it, so deliver as before.
    /// - companion interest unbounded (`watched == None`): always yes.
    /// - otherwise: yes iff the policy is in the companion's set.
    fn companion_wants(&mut self, path: &std::path::Path, event_policy_hex: Option<&str>) -> bool {
        let Some(policy) = event_policy_hex else {
            return true;
        };
        let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        let needs_refresh = self
            .entries
            .get(path)
            .map(|c| c.mtime != mtime)
            .unwrap_or(true);
        if needs_refresh {
            self.entries.insert(
                path.to_path_buf(),
                CachedInterest {
                    mtime,
                    watched: load_watched_policies(path),
                },
            );
        }
        match &self.entries.get(path).expect("just inserted").watched {
            None => true,
            Some(set) => set.contains(policy),
        }
    }
}

/// Read a companion `.cbor` and project its declared interest to a
/// watched-policy hex set. Returns `None` (unbounded) when the
/// interest is unbounded, when no policy interest is declared, or on
/// any read/decode error — all "deliver everything" outcomes.
fn load_watched_policies(path: &std::path::Path) -> Option<HashSet<String>> {
    let bytes = std::fs::read(path).ok()?;
    let req = mitos_protocol::SubscribeRequest::decode(&bytes).ok()?;
    match mitos_protocol::watched_policies(&req.interests) {
        // Unbounded interest (Any/Fingerprint present) → all.
        None => None,
        // No declared policy interest → don't narrow (back-compat).
        Some(s) if s.is_empty() => None,
        Some(s) => Some(s.into_iter().map(|p| p.as_str().to_string()).collect()),
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_one(
    storage: &ModuleStorage,
    store: &crate::emissions::EmissionsStore,
    instance: &InstanceKey,
    module_current: Option<&InstanceKey>,
    event: emit::EmittedEvent,
    interest_cache: &mut InterestCache,
    row_cache: &mut RowCache,
    handoffs: &mitos_lifecycle::handoff::Handoffs,
) {
    use crate::emissions::{EmissionStatus, UNSUBSCRIBED_COMPANION_ID};
    use mitos_lifecycle::handoff::Admit;
    let module_id: &str = instance.module.as_str();
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

    // Per-companion interest filter routing key. By convention a
    // non-empty `partition_key` carrying valid UTF-8 is the policy
    // hex the event pertains to (collection-holders /
    // collection-metadata / jpg-store-offer emit this; the dialer
    // also uses it as a concurrency-lane hash, which is compatible).
    // A module emitting a binary or empty key yields `None` here →
    // the filter broadcasts (unchanged behaviour). See the
    // recapture cross-contamination fix in
    // `docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md` §5.
    let event_policy_hex: Option<&str> = if event.partition_key.is_empty() {
        None
    } else {
        std::str::from_utf8(&event.partition_key).ok()
    };

    // Two-level walk: `<module>/companions/<client_id>/<companion_key>.cbor`.
    // Each `(client_id, companion_key)` pair is a distinct
    // subscriber and gets its own emission row — *if* its declared
    // interest matches this event's policy (see `event_policy_hex` +
    // `InterestCache`). See
    // `docs/design/MULTI_CLIENT_COMPANIONS.md` — "Dispatch fan-out".
    //
    // Sentinel fallback (drop site #3 in
    // `docs/design/EVENT_DELIVERY_RESILIENCE.md`): only when *no*
    // companion is subscribed at all (`!saw_companion`) — so an
    // emission isn't lost in the window before the first subscribe;
    // the first subscribe reclaims sentinel rows via
    // `EmissionsStore::retarget_companion`. When companions exist but
    // none are interested in this policy, the event is genuinely
    // unwanted and is dropped — sentineling it would let a future
    // unrelated subscriber wrongly reclaim another policy's emission.
    let mut wrote_per_companion = false;
    let mut saw_companion = false;
    if companions_dir.exists() {
        match std::fs::read_dir(&companions_dir) {
            Ok(read) => {
                for entry in read.flatten() {
                    let file_type = match entry.file_type() {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    if !file_type.is_dir() {
                        continue;
                    }
                    let dir_name = entry.file_name();
                    let client_id = match dir_name.to_str() {
                        Some(s) => s,
                        None => continue,
                    };
                    // Skip metadata dirs (`.unreachable/` etc.).
                    if client_id.starts_with('.') {
                        continue;
                    }
                    // Does THIS instance serve this client? With one
                    // instance — every module in production — the answer is
                    // always `Deliver` and the walk is what it always was.
                    //
                    // A `Skip` is NOT "no subscriber". The client IS
                    // subscribed; another instance is writing its row. So the
                    // walk continues far enough to set `saw_companion`, and
                    // only the `append` is withheld — skipping the whole
                    // client dir would leave `saw_companion` false and this
                    // instance would sentinel an event that was in fact
                    // delivered, which a later unrelated subscriber could
                    // then reclaim.
                    let serves = match module_current {
                        Some(current) => routing::fan_out(
                            row_cache.get(storage, module_id, client_id),
                            instance,
                            current,
                            |other| instance_dir_of(storage, other),
                        ),
                        // Nothing to compare against (an unactivated module):
                        // behave as v2 did and deliver.
                        None => routing::FanOut::Deliver,
                    };
                    // …and has this instance ALREADY had this block delivered
                    // to this client, by the instance it took the client
                    // over from? Asked per client, like `serves`, because a
                    // watermark is per client.
                    //
                    // A client id that will not parse has no watermark it
                    // could be under — `RowCache::get` reads the same name
                    // the same way.
                    let admits = match mitos_lifecycle::ids::ClientId::parse(client_id) {
                        Ok(client) => handoffs.admits(&client, slot_of(&event.chain_point)),
                        Err(_) => Admit::Deliver,
                    };
                    let client_dir = entry.path();
                    let client_files = match std::fs::read_dir(&client_dir) {
                        Ok(r) => r,
                        Err(e) => {
                            tracing::warn!(
                                module = %module_id,
                                client_dir = %client_dir.display(),
                                error = %e,
                                "read client-dir failed"
                            );
                            continue;
                        }
                    };
                    for client_entry in client_files.flatten() {
                        let path = client_entry.path();
                        if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
                            continue;
                        }
                        let companion_key = match path.file_stem().and_then(|s| s.to_str()) {
                            Some(s) => s.to_string(),
                            None => continue,
                        };
                        saw_companion = true;
                        // `match`, never `== Deliver`: a `Hold` variant is
                        // coming for the staging diff, and it must arrive as
                        // a compile error here rather than as a silent skip.
                        match serves {
                            routing::FanOut::Deliver => {}
                            routing::FanOut::Skip => continue,
                        }
                        // Withheld for the same reason a `Skip` is: somebody
                        // else has this client's copy of this block — there,
                        // the instance it moved to; here, the instance it
                        // moved FROM, before the switch. And for the same
                        // reason it is checked AFTER `saw_companion`: the
                        // companion is subscribed, so sentineling this event
                        // would keep one that was in fact delivered (WO-06).
                        match admits {
                            Admit::Deliver => {}
                            Admit::Withheld => {
                                tracing::trace!(
                                    module = %module_id,
                                    instance = %instance.dir_name(),
                                    client_id = %client_id,
                                    slot = event.chain_point.slot(),
                                    "withheld: the client's previous instance delivered this block",
                                );
                                continue;
                            }
                        }
                        // Per-companion interest filter: skip companions
                        // whose declared interest doesn't cover this
                        // event's policy. Unbounded / no-interest /
                        // keyless events always pass (see InterestCache).
                        if !interest_cache.companion_wants(&path, event_policy_hex) {
                            tracing::trace!(
                                module = %module_id,
                                client_id = %client_id,
                                companion_key = %companion_key,
                                policy = event_policy_hex.unwrap_or(""),
                                "emission filtered out — companion not interested in policy"
                            );
                            continue;
                        }
                        match store.append(
                            &companion_key,
                            client_id,
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
                                    client_id = %client_id,
                                    companion_key = %companion_key,
                                    error = %e,
                                    "append emission row failed"
                                );
                            }
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

    // Sentinel only when there are no subscribers at all. If
    // companions exist but the interest filter dropped this event
    // for all of them, it's genuinely unwanted — don't sentinel
    // (a later unrelated subscribe must not reclaim it).
    //
    // And only on the CURRENT instance. Every instance sees the same events,
    // so without this each would keep its own copy of the same unclaimed row
    // — and the first subscriber, placed on the current instance, could only
    // ever reclaim one. The rest would sit in stores nothing reads.
    let keep_unclaimed = match module_current {
        Some(current) => routing::unclaimed(instance, current) == routing::Unclaimed::Keep,
        // No pointer to compare against (an unactivated module): behave as
        // v2 did, which is to keep it.
        None => true,
    };
    if keep_unclaimed && !saw_companion && !wrote_per_companion {
        if let Err(e) = store.append(
            UNSUBSCRIBED_COMPANION_ID,
            "",
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

/// Fan a chain-rollback **undo** out to every companion subscribed to
/// this module. Unlike [`drain_one`], there is no interest filter and
/// no sentinel: a reorg is unconditional, so each subscribed companion
/// gets one `is_undo` emission carrying the rollback `ChainPoint`
/// (`event.chain_point`). The dialer delivers it via
/// `POST /_internal/undo-<target>` and the companion's `undo()` hook
/// reverts any latching state at or after that point. Re-derivable
/// companions whose `undo` is the default no-op simply ignore it; the
/// re-applied forward frames reconverge them.
///
/// No subscribers → nothing to undo (a sentinel undo would be
/// meaningless — undo targets state a companion already applied).
fn fan_out_undo(
    storage: &ModuleStorage,
    store: &crate::emissions::EmissionsStore,
    instance: &InstanceKey,
    module_current: Option<&InstanceKey>,
    event: emit::EmittedEvent,
    row_cache: &mut RowCache,
) {
    let module_id: &str = instance.module.as_str();
    let companions_dir = storage.module_dir_for_companions(module_id);
    let now = format!(
        "unix:{}",
        std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    );
    // The undo row's channel = the module/target name (= the companion
    // channel `NAME`). The dialer routes by the companion's pre-resolved
    // per-target URL, but we stamp it here for log clarity + the
    // companion's `/_internal/undo-<channel>` lookup.
    let channel = module_id.to_string();

    let Ok(read) = std::fs::read_dir(&companions_dir) else {
        // No companions dir → no subscribers → nothing to revert.
        return;
    };
    let mut delivered = 0u64;
    for entry in read.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let dir_name = entry.file_name();
        let Some(client_id) = dir_name.to_str() else {
            continue;
        };
        if client_id.starts_with('.') {
            continue; // metadata dirs (`.unreachable/` etc.)
        }
        // An undo follows the SAME rule as a forward emission: it is for the
        // client, and the client is served by one instance. Delivering a
        // reorg from both would make a companion revert twice.
        if let Some(current) = module_current {
            let row = row_cache.get(storage, module_id, client_id);
            match routing::fan_out(row, instance, current, |other| {
                instance_dir_of(storage, other)
            }) {
                routing::FanOut::Deliver => {}
                routing::FanOut::Skip => continue,
            }
        }
        let Ok(client_files) = std::fs::read_dir(entry.path()) else {
            continue;
        };
        for client_entry in client_files.flatten() {
            let path = client_entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("cbor") {
                continue;
            }
            let Some(companion_key) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            match store.append_undo(
                companion_key,
                client_id,
                &channel,
                event.chain_point.clone(),
                event.partition_key.clone(),
                &now,
            ) {
                Ok(_) => delivered += 1,
                Err(e) => tracing::warn!(
                    module = %module_id,
                    client_id = %client_id,
                    companion_key = %companion_key,
                    error = %e,
                    "append undo emission failed"
                ),
            }
        }
    }
    tracing::info!(
        module = %module_id,
        cursor = ?event.chain_point,
        companions = delivered,
        "rollback fanned out as undo emissions"
    );
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

        // EVERY running instance of the module.
        //
        // Interest belongs to a companion, at module level — an instance
        // that is not serving that companion watching a few extra refs costs
        // a little and breaks nothing, while one that IS serving it and
        // missed the update is silent loss (the companion subscribes, the
        // module never widens its scan, and nothing ever captures).
        let senders: Vec<_> = {
            let senders = self
                .interest_senders
                .lock()
                .expect("interest_senders mutex");
            senders
                .iter()
                .filter(|(key, _)| key.module.as_str() == module_id)
                .map(|(_, tx)| tx.clone())
                .collect()
        };
        if senders.is_empty() {
            return Err(InterestRouteError::NotRunning(module_id.to_owned()));
        }
        let mut delivered = 0usize;
        for sender in &senders {
            if sender
                .send(InterestUpdate {
                    op,
                    predicates: predicates.clone(),
                    items_cbor: items_cbor.clone(),
                })
                .is_ok()
            {
                delivered += 1;
            }
        }
        if delivered == 0 {
            return Err(InterestRouteError::ChannelClosed(module_id.to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod interest_filter_tests {
    //! Unit coverage for the per-companion emit fan-out filter
    //! (`InterestCache` / `companion_wants`) — the host side of the
    //! recapture cross-contamination fix
    //! (`docs/design/HOLDER_DISTRIBUTION_DRIVER_MIGRATION.md` §5).

    use super::{CachedInterest, InterestCache};
    use cardano_assets::PolicyId;
    use mitos_protocol::{
        AssetSelector, DialBackOverride, Interest, SubscribeRequest, SubscribeTarget,
    };
    use std::collections::HashSet;
    use std::path::Path;

    // Two distinct, valid (56 lowercase-hex) policies.
    const POLICY_A: &str = "43a056aabbccddeeff00112233445566778899aabbccddeeff001122";
    const POLICY_B: &str = "de79250aabbccddeeff00112233445566778899aabbccddeeff001122";

    fn write_companion(dir: &Path, interests: Vec<Interest>) -> std::path::PathBuf {
        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: "collection-holders".into(),
            }],
            companion_key: "k".into(),
            client_id: "c".into(),
            resume_from: None,
            interests,
            dial_back: Some(DialBackOverride {
                url: Some("https://x/_internal/{op}-{target}?key={key}".into()),
                auth_header: None,
                auth_value: None,
            }),
            announced: Vec::new(),
        };
        let path = dir.join("companion.cbor");
        std::fs::write(&path, req.encode().expect("encode")).expect("write");
        path
    }

    fn policy_interest(hex: &str) -> Interest {
        Interest {
            asset: AssetSelector::Policy(PolicyId::new(hex).expect("valid policy")),
            ..Interest::any()
        }
    }

    #[test]
    fn bounded_interest_routes_only_matching_policy() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_companion(tmp.path(), vec![policy_interest(POLICY_A)]);
        let mut cache = InterestCache::default();
        assert!(cache.companion_wants(&path, Some(POLICY_A)));
        assert!(!cache.companion_wants(&path, Some(POLICY_B)));
    }

    #[test]
    fn keyless_event_always_delivered() {
        let tmp = tempfile::tempdir().unwrap();
        let path = write_companion(tmp.path(), vec![policy_interest(POLICY_A)]);
        let mut cache = InterestCache::default();
        // No partition key → host can't route → deliver (unchanged).
        assert!(cache.companion_wants(&path, None));
    }

    #[test]
    fn unbounded_interest_delivers_every_policy() {
        let tmp = tempfile::tempdir().unwrap();
        // `Interest::any()` → `AssetSelector::Any` → unbounded.
        let path = write_companion(tmp.path(), vec![Interest::any()]);
        let mut cache = InterestCache::default();
        assert!(cache.companion_wants(&path, Some(POLICY_A)));
        assert!(cache.companion_wants(&path, Some(POLICY_B)));
    }

    #[test]
    fn no_declared_interest_is_not_narrower_than_broadcast() {
        let tmp = tempfile::tempdir().unwrap();
        // Empty interest set must NOT drop everything — back-compat:
        // a companion that subscribed without declaring interest still
        // receives all policies (the pre-filter broadcast behaviour).
        let path = write_companion(tmp.path(), vec![]);
        let mut cache = InterestCache::default();
        assert!(cache.companion_wants(&path, Some(POLICY_A)));
        assert!(cache.companion_wants(&path, Some(POLICY_B)));
    }

    #[test]
    fn unreadable_file_fails_open() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist.cbor");
        let mut cache = InterestCache::default();
        // Read failure → unbounded → deliver. Never drop a delivery
        // because the filter couldn't load the interest.
        assert!(cache.companion_wants(&missing, Some(POLICY_A)));
    }

    #[test]
    fn cache_hit_uses_cached_entry_without_reread() {
        let tmp = tempfile::tempdir().unwrap();
        // On-disk says POLICY_A …
        let path = write_companion(tmp.path(), vec![policy_interest(POLICY_A)]);
        let mtime = std::fs::metadata(&path).unwrap().modified().ok();
        let mut cache = InterestCache::default();
        // … but we pre-seed the cache with the *current* mtime and a
        // POLICY_B watch-set. Same mtime ⇒ cache hit ⇒ no re-read, so
        // the cached B set wins.
        let mut set = HashSet::new();
        set.insert(POLICY_B.to_string());
        cache.entries.insert(
            path.clone(),
            CachedInterest {
                mtime,
                watched: Some(set),
            },
        );
        assert!(cache.companion_wants(&path, Some(POLICY_B)));
        assert!(!cache.companion_wants(&path, Some(POLICY_A)));
    }

    #[test]
    fn stale_mtime_triggers_refresh() {
        let tmp = tempfile::tempdir().unwrap();
        // On-disk says POLICY_A …
        let path = write_companion(tmp.path(), vec![policy_interest(POLICY_A)]);
        let mut cache = InterestCache::default();
        // … but the cached entry carries a bogus (old) mtime and a
        // POLICY_B watch-set. mtime mismatch ⇒ refresh from disk ⇒
        // the real POLICY_A set wins.
        let mut set = HashSet::new();
        set.insert(POLICY_B.to_string());
        cache.entries.insert(
            path.clone(),
            CachedInterest {
                mtime: Some(std::time::UNIX_EPOCH),
                watched: Some(set),
            },
        );
        assert!(cache.companion_wants(&path, Some(POLICY_A)));
        assert!(!cache.companion_wants(&path, Some(POLICY_B)));
    }
}

/// The emission fan-out — `drain_one` and `fan_out_undo`.
///
/// Neither had a test before this order. Both are plain synchronous
/// functions over a `ModuleStorage` and an `EmissionsStore`, so they need no
/// host, no engine and no chain.
#[cfg(test)]
mod fan_out_tests {
    use super::*;
    use crate::emissions::UNSUBSCRIBED_COMPANION_ID;
    use cardano_assets::PolicyId;
    use mitos_lifecycle::handoff::Handoffs;
    use mitos_lifecycle::ids::{ClientId, Slot};
    use mitos_protocol::{AssetSelector, Interest, SubscribeRequest, SubscribeTarget};

    const MODULE: &str = "ownership-indexer";
    // 56 lowercase hex each. NOTE: `interest_filter_tests`'s `POLICY_B` is
    // 57 characters — a typo that never surfaced there because that module
    // only ever passes it as a raw `&str`, never through `PolicyId::new`.
    const POLICY_A: &str = "43a056aabbccddeeff00112233445566778899aabbccddeeff001122";
    const POLICY_B: &str = "de7925aabbccddeeff00112233445566778899aabbccddeeff001122";

    struct Fixture {
        _tmp: tempfile::TempDir,
        storage: ModuleStorage,
        /// The module's one instance — and, in the one-instance case, also
        /// what `current.toml` resolves to.
        instance: InstanceKey,
    }

    fn fixture() -> Fixture {
        let tmp = tempfile::tempdir().unwrap();
        let storage = ModuleStorage::new(tmp.path());
        let wasm = b"fan-out test wasm";
        storage
            .activate(
                &crate::companions::testing::manifest_for(MODULE, wasm),
                wasm,
            )
            .unwrap();
        let instance = storage.current_instance(MODULE).unwrap().unwrap();
        Fixture {
            _tmp: tmp,
            storage,
            instance,
        }
    }

    /// A second instance, built the only way one can be outside `activate`.
    fn second_instance(storage: &ModuleStorage, wasm: &[u8]) -> InstanceKey {
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        storage.put_artifact(&manifest, wasm).unwrap();
        let key = InstanceKey {
            module: mitos_lifecycle::ids::ModuleId::parse(MODULE).unwrap(),
            wasm: mitos_lifecycle::ids::Sha256Hex::parse(&crate::manifest::sha256_hex(wasm))
                .unwrap(),
            config: None,
        };
        storage.write_instance_marker(&key).unwrap();
        key
    }

    /// One registration. `interests` empty ⇒ unbounded, which the filter
    /// always passes.
    fn register(storage: &ModuleStorage, client: &str, key: &str, policies: &[&str]) {
        let dir = storage.module_dir_for_companions(MODULE).join(client);
        std::fs::create_dir_all(&dir).unwrap();
        let interests: Vec<Interest> = policies
            .iter()
            .map(|p| Interest {
                asset: AssetSelector::Policy(PolicyId::new(*p).unwrap()),
                ..Interest::any()
            })
            .collect();
        let req = SubscribeRequest {
            targets: vec![SubscribeTarget::Module {
                name: MODULE.into(),
            }],
            companion_key: key.into(),
            client_id: client.into(),
            resume_from: None,
            interests,
            dial_back: None,
            announced: Vec::new(),
        };
        std::fs::write(dir.join(format!("{key}.cbor")), req.encode().unwrap()).unwrap();
    }

    fn event(policy: Option<&str>) -> emit::EmittedEvent {
        emit::EmittedEvent {
            module_id: MODULE.to_owned(),
            channel: 1,
            payload: b"payload".to_vec(),
            chain_point: mitos_protocol::ChainPoint::Slot(42),
            partition_key: policy.map(|p| p.as_bytes().to_vec()).unwrap_or_default(),
            is_undo: false,
        }
    }

    /// Every row in a store, as `(client_id, companion_key)`.
    fn rows(store: &crate::emissions::EmissionsStore) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = store
            .list_filtered(|_| true)
            .unwrap()
            .into_iter()
            .map(|r| (r.client_id, r.companion_id))
            .collect();
        out.sort();
        out
    }

    /// **Pins today's behaviour, written and run BEFORE the instance
    /// addressing went in.** Two clients, three companions, one bounded
    /// interest that filters one of them out, plus the no-companion
    /// sentinel case.
    #[test]
    fn with_one_instance_and_no_rows_the_fan_out_is_unchanged() {
        let f = fixture();
        let store = f.storage.emissions_store(MODULE).unwrap();

        // No companions at all ⇒ the sentinel keeps the event so the first
        // subscriber can reclaim it.
        drain_one(
            &f.storage,
            &store,
            &f.instance,
            Some(&f.instance),
            event(Some(POLICY_A)),
            &mut InterestCache::default(),
            &mut RowCache::default(),
            &Handoffs::default(),
        );
        assert_eq!(
            rows(&store),
            vec![(String::new(), UNSUBSCRIBED_COMPANION_ID.to_owned())],
        );

        // Two clients, three companions. `jpgsm`'s is bounded to POLICY_B,
        // so an event for POLICY_A must reach the other two only.
        let f = fixture();
        let store = f.storage.emissions_store(MODULE).unwrap();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        register(&f.storage, "ownership.cnft.dev", "policy_b", &[]);
        register(&f.storage, "jpgsm.cnft.dev", "policy_a", &[POLICY_B]);

        drain_one(
            &f.storage,
            &store,
            &f.instance,
            Some(&f.instance),
            event(Some(POLICY_A)),
            &mut InterestCache::default(),
            &mut RowCache::default(),
            &Handoffs::default(),
        );
        assert_eq!(
            rows(&store),
            vec![
                ("ownership.cnft.dev".to_owned(), "policy_a".to_owned()),
                ("ownership.cnft.dev".to_owned(), "policy_b".to_owned()),
            ],
            "the bounded companion must be filtered out, and no sentinel written",
        );

        // An undo is unconditional: no interest filter, no sentinel.
        let f = fixture();
        let store = f.storage.emissions_store(MODULE).unwrap();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        register(&f.storage, "jpgsm.cnft.dev", "policy_a", &[POLICY_B]);
        fan_out_undo(
            &f.storage,
            &store,
            &f.instance,
            Some(&f.instance),
            event(Some(POLICY_A)),
            &mut RowCache::default(),
        );
        assert_eq!(
            rows(&store),
            vec![
                ("jpgsm.cnft.dev".to_owned(), "policy_a".to_owned()),
                ("ownership.cnft.dev".to_owned(), "policy_a".to_owned()),
            ],
            "an undo reaches every subscriber regardless of interest",
        );
    }

    /// Write a client row placing `client` on `instance`.
    fn place(storage: &ModuleStorage, client: &str, instance: &InstanceKey) {
        use mitos_lifecycle::client_move::{ClientRow, Placement};
        use mitos_lifecycle::ids::{ClientId, ClientKey, Namespace, Revision};

        let row = ClientRow::adopt(
            ClientKey {
                namespace: Namespace::parse("prod").unwrap(),
                client: ClientId::parse(client).unwrap(),
            },
            mitos_lifecycle::ids::ModuleId::parse(MODULE).unwrap(),
            Placement {
                instance: instance.clone(),
                revision: Revision(0),
            },
        );
        storage.write_row(MODULE, &row).unwrap();
    }

    fn drain(f: &Fixture, store: &crate::emissions::EmissionsStore, from: &InstanceKey) {
        drain_at(f, store, from, &Handoffs::default(), event(Some(POLICY_A)));
    }

    /// As `drain`, with watermarks and an event of the caller's choosing.
    fn drain_at(
        f: &Fixture,
        store: &crate::emissions::EmissionsStore,
        from: &InstanceKey,
        handoffs: &Handoffs,
        event: emit::EmittedEvent,
    ) {
        let current = f.storage.current_instance(MODULE).unwrap();
        drain_one(
            &f.storage,
            store,
            from,
            current.as_ref(),
            event,
            &mut InterestCache::default(),
            &mut RowCache::default(),
            handoffs,
        );
    }

    /// A second instance serves nobody until a row says so — and must not
    /// keep its own copy of the unclaimed event either, or the first
    /// subscriber (placed on the current instance) could never reclaim it.
    #[test]
    fn a_second_instance_writes_no_rows_and_no_sentinel() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        let second = second_instance(&f.storage, b"second instance wasm");
        let second_store = f.storage.emissions_store_at(&second).unwrap();

        drain(&f, &second_store, &second);
        assert!(
            rows(&second_store).is_empty(),
            "a second instance nobody is placed on must write nothing",
        );

        // …and with NO companions at all, still nothing: `unclaimed` drops
        // it on a non-current instance.
        let f = fixture();
        let second = second_instance(&f.storage, b"second instance wasm");
        let second_store = f.storage.emissions_store_at(&second).unwrap();
        drain(&f, &second_store, &second);
        assert!(
            rows(&second_store).is_empty(),
            "only the current instance keeps an unclaimed event",
        );
    }

    /// One emitted event through BOTH drains ⇒ exactly one row, in the
    /// instance the client's row names.
    #[test]
    fn a_client_whose_row_names_the_second_instance_is_served_by_it_alone() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        let second = second_instance(&f.storage, b"second instance wasm");
        place(&f.storage, "ownership.cnft.dev", &second);

        // Preconditions — both are what makes the assertions below mean
        // anything. A row that did not round-trip, or a second instance with
        // no directory, would make the first instance deliver and this test
        // pass for the wrong reason (see the `Gone` arm of `fan_out`).
        assert!(f.storage.instance_dir(&second).is_dir());
        let parsed = f
            .storage
            .read_row(MODULE, &ClientId::parse("ownership.cnft.dev").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(parsed.current.unwrap().instance, second);

        let first_store = f.storage.emissions_store_at(&f.instance).unwrap();
        let second_store = f.storage.emissions_store_at(&second).unwrap();
        drain(&f, &first_store, &f.instance);
        drain(&f, &second_store, &second);

        assert!(
            rows(&first_store).is_empty(),
            "the instance the client left must not deliver; that is the duplicate",
        );
        assert_eq!(
            rows(&second_store),
            vec![("ownership.cnft.dev".to_owned(), "policy_a".to_owned())],
        );
    }

    /// THE trap this is shaped around. An upload RENAMES the live instance,
    /// so a row written while the module was K1 still says K1 after the
    /// module has become K2 — and K1 exists nowhere. That client has not
    /// been moved by anything; it follows the module's current instance.
    #[test]
    fn a_row_naming_a_renamed_away_instance_is_still_delivered() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        place(&f.storage, "ownership.cnft.dev", &f.instance);

        // The upload: K1 → K2, and K1 is gone.
        let wasm_b = b"an operator upload";
        f.storage
            .activate(
                &crate::companions::testing::manifest_for(MODULE, wasm_b),
                wasm_b,
            )
            .unwrap();
        let k2 = f.storage.current_instance(MODULE).unwrap().unwrap();
        assert_ne!(k2, f.instance);
        assert!(!f.storage.instance_dir(&f.instance).is_dir());

        let store = f.storage.emissions_store_at(&k2).unwrap();
        drain(&f, &store, &k2);
        assert_eq!(
            rows(&store),
            vec![("ownership.cnft.dev".to_owned(), "policy_a".to_owned())],
            "every deploy does this; a stale row must not stop delivery",
        );
    }

    #[test]
    fn an_unreadable_row_does_not_stop_delivery() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        let dir = f.storage.module_dir_for_rows(MODULE);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("ownership.cnft.dev.toml"), "this is not = = toml").unwrap();

        let store = f.storage.emissions_store_at(&f.instance).unwrap();
        drain(&f, &store, &f.instance);
        assert_eq!(
            rows(&store),
            vec![("ownership.cnft.dev".to_owned(), "policy_a".to_owned())],
            "a row that cannot be read must not stop delivery",
        );
    }

    #[test]
    fn an_undo_follows_the_same_rule() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        let second = second_instance(&f.storage, b"second instance wasm");
        place(&f.storage, "ownership.cnft.dev", &second);

        let current = f.storage.current_instance(MODULE).unwrap();
        let first_store = f.storage.emissions_store_at(&f.instance).unwrap();
        let second_store = f.storage.emissions_store_at(&second).unwrap();
        for (from, store) in [(&f.instance, &first_store), (&second, &second_store)] {
            fan_out_undo(
                &f.storage,
                store,
                from,
                current.as_ref(),
                event(Some(POLICY_A)),
                &mut RowCache::default(),
            );
        }

        assert!(
            rows(&first_store).is_empty(),
            "reverting twice is exactly as wrong as delivering twice",
        );
        assert_eq!(
            rows(&second_store),
            vec![("ownership.cnft.dev".to_owned(), "policy_a".to_owned())],
        );
    }

    /// `module_current` is resolved per EVENT, not once per drain task: an
    /// upload moves it under a task that is already running.
    #[test]
    fn an_upload_mid_task_moves_module_current() {
        let f = fixture();
        register(&f.storage, "ownership.cnft.dev", "policy_a", &[]);
        let second = second_instance(&f.storage, b"second instance wasm");
        let second_store = f.storage.emissions_store_at(&second).unwrap();

        // While `second` is not current, it delivers nothing.
        drain(&f, &second_store, &second);
        assert!(rows(&second_store).is_empty());

        // Point the module at it — what an upload's repoint does — and the
        // very next event is delivered by it.
        f.storage.set_current(&second).unwrap();
        drain(&f, &second_store, &second);
        assert_eq!(
            rows(&second_store),
            vec![("ownership.cnft.dev".to_owned(), "policy_a".to_owned())],
            "the drain task must re-resolve current per event",
        );
    }

    // -----------------------------------------------------------------
    // Hand-off watermarks
    // -----------------------------------------------------------------

    const CLIENT: &str = "ownership.cnft.dev";

    fn client() -> ClientId {
        ClientId::parse(CLIENT).unwrap()
    }

    /// A forward emission carrying a chosen slot.
    fn at(slot: u64) -> emit::EmittedEvent {
        emit::EmittedEvent {
            chain_point: mitos_protocol::ChainPoint::Slot(slot),
            ..event(Some(POLICY_A))
        }
    }

    /// A rollback TO a chosen slot.
    fn undo_to(slot: u64) -> emit::EmittedEvent {
        emit::EmittedEvent {
            is_undo: true,
            ..at(slot)
        }
    }

    /// The watermark's whole job. The instance taking a client over resumes
    /// from its OWN cursor, which is at or below the one its predecessor had
    /// reached, so it re-emits blocks the client has already been sent.
    #[test]
    fn blocks_at_or_before_the_watermark_are_withheld_and_later_ones_delivered() {
        let f = fixture();
        register(&f.storage, CLIENT, "policy_a", &[]);
        let second = second_instance(&f.storage, b"second instance wasm");
        place(&f.storage, CLIENT, &second);
        let second_store = f.storage.emissions_store_at(&second).unwrap();

        let mut handoffs = Handoffs::default();
        handoffs.begin(client(), Slot(100));

        // AT the watermark, and below it.
        drain_at(&f, &second_store, &second, &handoffs, at(100));
        drain_at(&f, &second_store, &second, &handoffs, at(7));
        assert!(
            rows(&second_store).is_empty(),
            "a block its predecessor delivered must not be delivered again",
        );

        // The first block PAST it.
        drain_at(&f, &second_store, &second, &handoffs, at(101));
        assert_eq!(
            rows(&second_store),
            vec![(CLIENT.to_owned(), "policy_a".to_owned())],
        );
    }

    /// A withheld block is DELIVERED, by someone, and must not also be kept
    /// for a future subscriber. The instance is the module's current one
    /// here, which is the only case where `unclaimed` would keep anything —
    /// so this is the case that can go wrong.
    #[test]
    fn a_withheld_block_is_not_kept_as_unclaimed() {
        let f = fixture();
        register(&f.storage, CLIENT, "policy_a", &[]);
        let store = f.storage.emissions_store(MODULE).unwrap();
        assert_eq!(
            f.storage.current_instance(MODULE).unwrap().as_ref(),
            Some(&f.instance),
            "precondition: only the CURRENT instance keeps an unclaimed event",
        );

        let mut handoffs = Handoffs::default();
        handoffs.begin(client(), Slot(100));
        drain_at(&f, &store, &f.instance, &handoffs, at(50));

        assert!(
            rows(&store).is_empty(),
            "the only companion was withheld, not absent; a sentinel here would be \
             handed to the next subscriber as a block nobody had had",
        );
    }

    /// A reorg below a watermark voids what the PREVIOUS instance delivered
    /// on the abandoned fork: the blocks that replace `(to, watermark]` are
    /// new blocks nobody has sent. So the watermark drops — durably, because
    /// the drain task does not survive a restart and the lowered watermark
    /// must.
    #[test]
    fn a_rollback_below_the_watermark_lowers_it_and_is_itself_delivered() {
        let f = fixture();
        register(&f.storage, CLIENT, "policy_a", &[]);
        let store = f.storage.emissions_store(MODULE).unwrap();

        let mut handoffs = Handoffs::default();
        handoffs.begin(client(), Slot(100));
        f.storage.write_handoffs_at(&f.instance, &handoffs).unwrap();

        drain_event(
            &f.storage,
            &store,
            &f.instance,
            undo_to(50),
            &mut InterestCache::default(),
            &mut RowCache::default(),
            &mut handoffs,
        );

        assert_eq!(handoffs.watermark(&client()), Some(Slot(50)));
        assert_eq!(
            f.storage
                .read_handoffs_at(&f.instance)
                .unwrap()
                .watermark(&client()),
            Some(Slot(50)),
            "a lowered watermark that only lived in memory would re-withhold the \
             replacement blocks after a restart",
        );
        assert_eq!(
            rows(&store),
            vec![(CLIENT.to_owned(), "policy_a".to_owned())],
            "the rollback itself is never withheld",
        );

        // And the replacement blocks are now admitted.
        drain_at(&f, &store, &f.instance, &handoffs, at(60));
        assert_eq!(rows(&store).len(), 2);
    }

    /// The trade-off, pinned. A watermark file that will not parse is a
    /// client that may be sent a block twice — which a companion tolerates,
    /// because applies are chain-point-idempotent. Refusing to start the
    /// drain would lose every block instead.
    #[tokio::test]
    async fn an_unreadable_watermark_file_delivers_rather_than_refusing_to_start() {
        let f = fixture();
        register(&f.storage, CLIENT, "policy_a", &[]);
        // `storage::HANDOFFS` is private; this is the name it holds.
        std::fs::write(
            f.storage.instance_dir(&f.instance).join("handoffs.toml"),
            b"after = 3\n",
        )
        .unwrap();
        assert!(
            f.storage.read_handoffs_at(&f.instance).is_err(),
            "precondition: the file must be unparseable",
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let cancel = CancellationToken::new();
        let drain = tokio::spawn(run_emit_drain(
            f.storage.clone(),
            f.instance.clone(),
            rx,
            cancel.clone(),
        ));
        tx.send(at(101)).unwrap();

        let store = f.storage.emissions_store_at(&f.instance).unwrap();
        for _ in 0..100 {
            if !rows(&store).is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        cancel.cancel();
        drain.await.unwrap();

        assert_eq!(
            rows(&store),
            vec![(CLIENT.to_owned(), "policy_a".to_owned())],
            "an unreadable watermark file must withhold nothing, not stop the drain",
        );
    }
}
