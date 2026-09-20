//! The instance lifecycle, wired — D10.
//!
//! One component owns it. Everything that used to decide for itself whether a
//! module should be running — `auto_resume`, the watchdog, the admin
//! stop/restart/evict handlers, the upload handler — now feeds an [`Event`]
//! through here and executes the [`Effect`]s it gets back.
//!
//! **Nothing in this file decides anything.** `Instance::step`, `on_boot`,
//! `recover` and `reconcile` are the decision; they live in
//! `mitos_lifecycle::instance` and are tested there against a decision table.
//! If a change here needs an `if` about whether an instance should be running,
//! draining, stopped or reaped, it belongs in that crate instead.
//!
//! # What this fixes
//!
//! v2 had no notion of what an operator WANTED. `DELETE /_admin/modules/{id}`
//! stopped the follower and three things then undid it: `auto_resume` on the
//! next restart, the watchdog within ONE TICK on a subscribed module, and an
//! upload calling `host.replace` unconditionally. The operator's intent is now
//! persisted as `DesiredState` and all three paths read it.
//!
//! # Persist, THEN execute
//!
//! [`LifecycleManager::apply`] locks the module, loads the record, steps the
//! machine, WRITES the new record, and only then executes effects in order. A
//! crash between the write and the effects is what `Instance::recover` is for.
//!
//! An effect that FAILS does not roll the record back. The record is what
//! SHOULD be true; the watchdog is what closes the gap. Rolling back would
//! mean a module whose follower failed to start is recorded as stopped, and
//! then nothing ever retries it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};

use mitos_lifecycle::UnixSecs;
use mitos_lifecycle::ids::{ClientId, ClientKey, InstanceKey, Sha256Hex};
use mitos_lifecycle::instance::{BootAction, Effect, Event, Instance, Rejected};
use mitos_lifecycle::policy::NamespaceProfile;
use mitos_lifecycle::routing::{self, Subscriber};
use mitos_lifecycle::{state_prep, upgrade};

use crate::auth::Authenticator;
use crate::dialer::CompanionDialer;
use crate::events::{EventKind, EventRing};
use crate::host_v2::ModuleHostHandle;
use crate::manifest::Manifest;
use crate::rows::instance_dir_of;
use crate::storage::{ModuleStorage, PartialInstance, StateFile, StorageError};

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    /// The module has no `current.toml`, so there is no instance to govern.
    #[error("module {0} is not registered")]
    NotRegistered(String),
    /// An instance directory with no `lifecycle.toml` that is not the
    /// module's current one — see [`InstanceView::Unmanaged`]. Not an error
    /// anyone has to handle in production; it is how the manager refuses to
    /// adopt a directory nothing here made.
    #[error("instance {instance} of module {module} is not managed by the lifecycle")]
    Unmanaged { module: String, instance: String },
    /// The machine refused the event. The admin layer maps this to 409 — it
    /// is a legitimate state, not a malformed request.
    #[error("{0}")]
    Rejected(#[from] Rejected),
    /// The host refused something the caller asked for DIRECTLY (an
    /// operator's restart). Effects do not use this: a failed effect is
    /// logged and left for the watchdog, never returned.
    #[error("host: {0}")]
    Host(String),
}

impl LifecycleError {
    /// Whether this means "there is nothing there any more" — a reap that
    /// finished and took the instance, or the module, with it. The tick
    /// reloads after every effect and must stay quiet about that.
    fn is_gone(&self) -> bool {
        matches!(
            self,
            LifecycleError::NotRegistered(_)
                | LifecycleError::Storage(StorageError::NoSuchInstance { .. })
        )
    }
}

/// Why a staging instance could not be created. Every one of these fails the
/// MOVE (`MoveFailure::StagingFailed`) and leaves the client exactly where it
/// was being served.
#[derive(Debug, thiserror::Error)]
pub enum StagingError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("lifecycle: {0}")]
    Lifecycle(#[from] LifecycleError),
    /// A directory is already there with no `lifecycle.toml`. Nothing here
    /// made it, so nothing here will adopt whatever state is in it.
    #[error("instance {0} has a directory but no record; refusing it as a move's target")]
    Unmanaged(String),
    #[error("no manifest in the content store for {0}")]
    NoManifest(String),
    /// Unreachable while the WIT has no `migrate` export — see
    /// `create_staging`. Never "copy and hope".
    #[error("the state plan needs a `migrate` export, and the host cannot run one")]
    MigrateUnsupported,
    /// `copy_set` asked to carry a file with no parent to carry it from —
    /// a contradiction in the pure crate, not a state the host can reach.
    #[error("the copy plan names files but the move has no parent instance")]
    CopyWithoutParent,
    /// The parent's directory is not there. An upload renamed it away and
    /// the row naming it was not rebased first — copying from it would
    /// silently produce an EMPTY instance, because a file that is not there
    /// carries as zero bytes.
    #[error("parent instance {0} is not on disk; refusing to copy nothing and call it a copy")]
    ParentGone(String),
    /// The parent's follower could not be stopped, so its files cannot be
    /// copied: a byte copy under a live writer is a torn one.
    #[error("parent instance {0}'s follower would not stop; refusing to copy live state files")]
    ParentWouldNotStop(String),
    #[error("the staging instance was built but its follower did not start")]
    StartFailed,
}

/// What [`LifecycleManager::revive`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Revival {
    Restarted,
    /// By the time the lock was held there was nothing to do: the follower
    /// was back, the record no longer wanted one, or the directory had moved.
    NotNeeded,
    Failed(String),
}

/// What the manager knows about one instance directory.
///
/// The distinction is the whole of "adoption is for the module's CURRENT
/// instance only". Adoption exists so the first v3 boot of a v2 tree is a
/// no-op, and a v2 tree has exactly one instance per module. A record-less
/// SIBLING is something else entirely — WO-08 writes `Instance::staging`'s
/// record in the same breath as creating the directory, so a sibling with no
/// record is a directory nothing here made. Adopting it would conclude
/// `Active` + `Pinned` (no clients ⇒ pinned) and it would follow the chain
/// for ever, unasked.
#[derive(Debug, Clone)]
pub enum InstanceView {
    Managed(Instance),
    Unmanaged(InstanceKey),
}

impl InstanceView {
    pub fn key(&self) -> &InstanceKey {
        match self {
            InstanceView::Managed(instance) => &instance.key,
            InstanceView::Unmanaged(key) => key,
        }
    }

    pub fn managed(&self) -> Option<&Instance> {
        match self {
            InstanceView::Managed(instance) => Some(instance),
            InstanceView::Unmanaged(_) => None,
        }
    }

    /// What a restart does with this instance. An `Unmanaged` one is left
    /// alone: it is not started, not reaped, and not adopted on the way past.
    pub fn on_boot(&self) -> BootAction {
        match self {
            InstanceView::Managed(instance) => instance.on_boot(),
            InstanceView::Unmanaged(_) => BootAction::LeaveStopped,
        }
    }
}

/// Reads the wall clock. The pure crate never does, and neither may a test of
/// this: `an_abandoned_dev_only_instance_drains_stops_and_is_reaped` moves a
/// day forward without sleeping for one.
pub type Clock = Arc<dyn Fn() -> UnixSecs + Send + Sync>;

/// The system clock, for production.
pub fn system_clock() -> Clock {
    Arc::new(|| {
        UnixSecs(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        )
    })
}

pub struct LifecycleManager {
    storage: ModuleStorage,
    host: Arc<dyn ModuleHostHandle>,
    auth: Arc<Authenticator>,
    events: EventRing,
    clock: Clock,
    /// Set after construction, as `host.set_dialer` is — the dialer needs the
    /// host, which needs the storage, which this also holds. At boot it is
    /// deliberately still unset: `dialer.start_all()` reconciles every
    /// module's interest itself, so a `StartFollower` during `boot()` must not
    /// duplicate that work.
    dialer: OnceLock<CompanionDialer>,
    /// One lock per module, held across load → step → write → execute, so two
    /// admin calls and a tick cannot interleave on one instance.
    locks: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl LifecycleManager {
    pub fn new(
        storage: ModuleStorage,
        host: Arc<dyn ModuleHostHandle>,
        auth: Arc<Authenticator>,
        events: EventRing,
        clock: Clock,
    ) -> Self {
        Self {
            storage,
            host,
            auth,
            events,
            clock,
            dialer: OnceLock::new(),
            locks: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Wire the dialer in once it exists. After this, a `StartFollower` also
    /// re-asserts the module's scan-interest from the persisted companion
    /// set — subscribes that landed while the module was down had their
    /// interest mutation dropped with `NotRunning`.
    pub fn set_dialer(&self, dialer: CompanionDialer) {
        let _ = self.dialer.set(dialer);
    }

    fn lock_for(&self, module: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().expect("lifecycle locks mutex");
        locks.entry(module.to_owned()).or_default().clone()
    }

    fn now(&self) -> UnixSecs {
        (self.clock)()
    }

    // -----------------------------------------------------------------
    // Loading
    // -----------------------------------------------------------------

    /// The clients with a registration ON DISK right now, with the profile of
    /// the namespace that owns each.
    ///
    /// A client no namespace owns is SKIPPED: it has no `ClientKey`, so there
    /// is nothing to attach. It is not silently lost — WO-02 already reports
    /// it as `unowned_clients` on `/_admin/status` and warns once at boot.
    pub fn on_disk_clients(&self, module: &str) -> BTreeMap<ClientKey, NamespaceProfile> {
        let registry = self.auth.registry();
        let root = self.storage.module_dir_for_companions(module);
        let Ok(entries) = std::fs::read_dir(&root) else {
            return BTreeMap::new();
        };
        let mut out = BTreeMap::new();
        for entry in entries.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            // `.unreachable/` and friends are dialer metadata, not clients.
            if name.starts_with('.') {
                continue;
            }
            // A directory with no `.cbor` in it is a client whose last
            // registration was removed; the directory itself lingers.
            let has_record = std::fs::read_dir(entry.path())
                .map(|files| {
                    files
                        .flatten()
                        .any(|f| f.path().extension().is_some_and(|e| e == "cbor"))
                })
                .unwrap_or(false);
            if !has_record {
                continue;
            }
            let Ok(client) = ClientId::parse(&name) else {
                continue;
            };
            let (Some(key), Some(def)) =
                (registry.client_key(&client), registry.namespace_of(&client))
            else {
                continue;
            };
            out.insert(key, def.profile);
        }
        out
    }

    fn current_key(&self, module: &str) -> Result<InstanceKey, LifecycleError> {
        self.storage
            .current_instance(module)?
            .ok_or_else(|| LifecycleError::NotRegistered(module.to_owned()))
    }

    /// The instance, from its record — or ADOPTED and written if it has none.
    ///
    /// Adoption is what makes the first v3 boot a no-op on mainnet: a module
    /// with clients becomes `Active`, one without becomes `Active` + `Pinned`.
    /// See `Instance::adopt` for why pinning the client-ful case would leave a
    /// zombie follower behind per deploy — and [`InstanceView`] for why only
    /// the module's CURRENT instance is ever adopted.
    ///
    /// **Caller must hold this module's lock.** Adoption WRITES, and every
    /// other write to the record is a read-modify-write under that lock. An
    /// unlocked adoption landing after a locked `apply` would overwrite the
    /// transition it had just persisted — silently discarding, say, an
    /// operator's stop. Callers without the lock use [`Self::peek_at`].
    fn load_or_adopt_at(&self, key: &InstanceKey) -> Result<Instance, LifecycleError> {
        let module = key.module.as_str();
        if let Some(record) = self.storage.read_lifecycle_at(key)? {
            return Ok(Instance::from_record(key.clone(), record));
        }
        if self.storage.current_instance(module)?.as_ref() != Some(key) {
            return Err(LifecycleError::Unmanaged {
                module: module.to_owned(),
                instance: key.dir_name(),
            });
        }
        let instance = self.adopted(module, key.clone());
        self.storage.write_lifecycle_at(key, &instance.record())?;
        tracing::info!(
            module = %module,
            instance = %key.dir_name(),
            desired = ?instance.desired,
            attached = instance.attached.len(),
            "instance adopted into the lifecycle",
        );
        // Adoption counts client DIRECTORIES. The flat-file migration
        // runs first, but it leaves a record it cannot decode "in
        // place", and it can fail outright — and what adoption
        // concludes is written down and sticky: a subscribed module
        // recorded as client-less is `Pinned`, and never retires when
        // its clients later move. Refusing to adopt would be worse (a
        // module with one corrupt legacy file could never start), so
        // this is made LOUD instead: it lands on the event ring, where
        // the deploy check looks.
        if crate::companions::has_flat_companions(&self.storage, module) {
            self.effect_failed(
                module,
                "adopt",
                "legacy flat companion records remain after migration; this module's \
                 clients may be under-counted — if it shows `pinned` but is subscribed, \
                 fix the records and `unpin` it",
            );
        }
        Ok(instance)
    }

    fn adopted(&self, module: &str, key: InstanceKey) -> Instance {
        Instance::adopt(key, &self.on_disk_clients(module))
    }

    /// The instance as it IS or WOULD BE adopted — without writing anything.
    ///
    /// For callers that do not hold the module lock: the watchdog and the
    /// admin API's read surfaces. Deriving the adoption instead of persisting
    /// it is safe because every reader derives the same answer from the same
    /// directory listing, and the next locked caller writes it down. The same
    /// rule as WO-01's crash repair: a path that is not holding the write lock
    /// never writes.
    fn peek_at(&self, key: &InstanceKey) -> Result<InstanceView, LifecycleError> {
        let module = key.module.as_str();
        Ok(match self.storage.read_lifecycle_at(key)? {
            Some(record) => InstanceView::Managed(Instance::from_record(key.clone(), record)),
            None if self.storage.current_instance(module)?.as_ref() == Some(key) => {
                InstanceView::Managed(self.adopted(module, key.clone()))
            }
            None => InstanceView::Unmanaged(key.clone()),
        })
    }

    /// The module's CURRENT instance, as `peek_at` sees it. What every
    /// `&str`-addressed caller means by "the module".
    fn peek(&self, module: &str) -> Result<Instance, LifecycleError> {
        let key = self.current_key(module)?;
        match self.peek_at(&key)? {
            InstanceView::Managed(instance) => Ok(instance),
            // The current instance is adopted if it has no record, so this
            // arm is unreachable — `peek_at` only returns `Unmanaged` for a
            // key that is not the current one.
            InstanceView::Unmanaged(key) => Err(LifecycleError::Unmanaged {
                module: module.to_owned(),
                instance: key.dir_name(),
            }),
        }
    }

    /// Every instance directory of the module, with what governs each.
    ///
    /// One unreadable instance is logged and left out rather than failing the
    /// listing — the same posture `boot` has always had for one bad module,
    /// one level down.
    pub fn instances(&self, module: &str) -> Result<Vec<InstanceView>, LifecycleError> {
        let module_id = mitos_lifecycle::ids::ModuleId::parse(module)
            .map_err(|_| LifecycleError::NotRegistered(module.to_owned()))?;
        let mut out = Vec::new();
        for key in self.storage.list_instances(&module_id)? {
            match self.peek_at(&key) {
                Ok(view) => out.push(view),
                Err(e) => tracing::warn!(
                    module = %module,
                    instance = %key.dir_name(),
                    error = %e,
                    "lifecycle: instance unreadable; leaving it alone",
                ),
            }
        }
        Ok(out)
    }

    /// Whether this module's CURRENT instance's follower should be running.
    pub fn expects_follower(&self, module: &str) -> Result<BootAction, LifecycleError> {
        Ok(self.peek(module)?.on_boot())
    }

    /// Whether THIS instance's follower should be running. The watchdog's
    /// whole decision. Reads only — the watchdog does not hold the module
    /// lock.
    pub fn expects_follower_at(&self, key: &InstanceKey) -> Result<BootAction, LifecycleError> {
        Ok(self.peek_at(key)?.on_boot())
    }

    /// The persisted record for the module's CURRENT instance, for the admin
    /// API's read surfaces. `Ok(None)` when the module has no instance at all.
    pub fn instance(&self, module: &str) -> Result<Option<Instance>, LifecycleError> {
        match self.peek(module) {
            Ok(instance) => Ok(Some(instance)),
            Err(e) if e.is_gone() => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// The record for ONE instance. `Ok(None)` for an `Unmanaged` one or an
    /// instance that is not there.
    pub fn instance_at(&self, key: &InstanceKey) -> Result<Option<Instance>, LifecycleError> {
        match self.peek_at(key) {
            Ok(view) => Ok(view.managed().cloned()),
            Err(e) if e.is_gone() => Ok(None),
            Err(e) => Err(e),
        }
    }

    // -----------------------------------------------------------------
    // Stepping
    // -----------------------------------------------------------------

    /// Feed one event to the module's CURRENT instance: load → step →
    /// PERSIST → execute.
    pub async fn apply(&self, module: &str, event: Event) -> Result<Vec<Effect>, LifecycleError> {
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;
        let key = self.current_key(module)?;
        self.apply_at_locked(&key, event).await
    }

    /// Feed one event to ONE instance.
    pub async fn apply_at(
        &self,
        key: &InstanceKey,
        event: Event,
    ) -> Result<Vec<Effect>, LifecycleError> {
        let lock = self.lock_for(key.module.as_str());
        let _guard = lock.lock().await;
        self.apply_at_locked(key, event).await
    }

    async fn apply_at_locked(
        &self,
        key: &InstanceKey,
        event: Event,
    ) -> Result<Vec<Effect>, LifecycleError> {
        let instance = self.load_or_adopt_at(key)?;
        let transition = instance.step(event, self.now())?;
        // The record goes to disk BEFORE any effect runs. A crash in between
        // leaves a record describing a world that has not happened yet, which
        // is exactly what `recover` re-issues on the next boot.
        self.storage
            .write_lifecycle_at(key, &transition.next.record())?;
        self.execute_all(key, &transition.effects).await;
        Ok(transition.effects)
    }

    /// Bring every instance's `attached` set into line with the registrations
    /// on disk.
    ///
    /// EVERY instance, not just the current one: a single registration
    /// change can alter two instances' sets — the client that arrives is
    /// attached to whichever serves it, and the one that leaves is detached
    /// from wherever it was.
    ///
    /// A diff, not a hook. There are at least four paths that add or remove a
    /// registration and the next one somebody adds would be missed silently —
    /// so every one of them calls this, and so does the tick. Being a diff is
    /// why a path that forgets is corrected a minute later instead of never.
    pub async fn reconcile(&self, module: &str) -> Result<(), LifecycleError> {
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;
        self.reconcile_locked(module).await
    }

    async fn reconcile_locked(&self, module: &str) -> Result<(), LifecycleError> {
        // The current instance, and the subscriber list, are read ONCE. Two
        // reads could see two different worlds, and then two instances'
        // records would disagree about who is attached to what.
        let current = self.current_key(module)?;
        let subscribers = self.subscribers_of(module);
        // One instance's failure must not strand its siblings: a client that
        // left A for B is detached from one and attached to the other, and
        // stopping half-way leaves it counted by both or by neither. The
        // first error is still the caller's answer.
        let mut first_error = None;
        for view in self.instances(module)? {
            if view.managed().is_none() {
                continue;
            }
            // Through `load_or_adopt_at`, not the peeked view: this is a
            // LOCKED caller, and it is the one that writes down what every
            // unlocked reader has been deriving.
            let instance = match self.load_or_adopt_at(view.key()) {
                Ok(instance) => instance,
                Err(e) => {
                    tracing::warn!(
                        module = %module,
                        instance = %view.key().dir_name(),
                        error = %e,
                        "reconcile: instance unreadable; leaving it alone",
                    );
                    continue;
                }
            };
            let attached = routing::attached_to(
                &instance.key,
                &current,
                subscribers.iter().map(|(client, profile, row)| Subscriber {
                    client: client.clone(),
                    profile: *profile,
                    row: row.as_ref(),
                }),
                |other| instance_dir_of(&self.storage, other),
            );
            for event in instance.reconcile(&attached) {
                // Each event re-loads, so a rejection on one (a client that
                // raced away between the diff and the step) does not abandon
                // the rest.
                match self.apply_at_locked(&instance.key, event.clone()).await {
                    Ok(_) => {}
                    Err(LifecycleError::Rejected(r)) => {
                        tracing::debug!(
                            module = %module,
                            instance = %instance.key.dir_name(),
                            ?event,
                            rejected = %r,
                            "reconcile: stale event",
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            module = %module,
                            instance = %instance.key.dir_name(),
                            ?event,
                            error = %e,
                            "reconcile: event failed; continuing with the module's other instances",
                        );
                        first_error.get_or_insert(e);
                        break;
                    }
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// The module's clients as `routing::attached_to` wants them: who, under
    /// which profile, and their row if they have one.
    ///
    /// A row that will not read is `None`, which `routing::fan_out` treats as
    /// "follows the module's current instance" — what that client was doing
    /// anyway. Refusing to reconcile because one row is corrupt would freeze
    /// every other client's attachment along with it.
    fn subscribers_of(
        &self,
        module: &str,
    ) -> Vec<(
        ClientKey,
        NamespaceProfile,
        Option<mitos_lifecycle::client_move::ClientRow>,
    )> {
        self.on_disk_clients(module)
            .into_iter()
            .map(|(client, profile)| {
                let row = match self.storage.read_row(module, &client.client) {
                    Ok(row) => row,
                    Err(e) => {
                        tracing::warn!(
                            module = %module,
                            client = %client,
                            error = %e,
                            "reconcile: client row unreadable; this client follows the module's \
                             current instance",
                        );
                        None
                    }
                };
                (client, profile, row)
            })
            .collect()
    }

    // -----------------------------------------------------------------
    // Effects
    // -----------------------------------------------------------------

    async fn execute_all(&self, key: &InstanceKey, effects: &[Effect]) {
        for effect in effects {
            self.execute(key, *effect).await;
        }
    }

    /// Start ONE instance's follower — the only way anything in this manager
    /// starts one.
    ///
    /// The dialer reconcile is half of it, not an extra: a subscribe that
    /// landed while the module was down had its interest mutation dropped
    /// with `NotRunning`, and a REBUILT instance has an empty KV, so the
    /// module's scan-interest (which `update-interest` persists in that KV)
    /// is gone with it. Without the re-assert such an instance would
    /// bootstrap to tip having scanned for nothing.
    ///
    /// Returns the failure rather than logging it, so a caller that must
    /// report it — `create_staging` — can, while `execute` keeps the "log it
    /// and let the watchdog close the gap" posture every effect has.
    async fn start_follower(&self, key: &InstanceKey) -> Result<(), String> {
        self.host
            .start_instance(key)
            .await
            .map_err(|e| e.to_string())?;
        if let Some(dialer) = self.dialer.get() {
            dialer.reconcile_module_interest(key.module.as_str()).await;
        }
        tracing::info!(
            module = %key.module,
            instance = %key.dir_name(),
            "lifecycle: follower started",
        );
        Ok(())
    }

    /// Every effect is idempotent (see the lifecycle crate's docs), so a
    /// failure here is logged and left for the watchdog rather than retried
    /// in place or rolled back.
    ///
    /// Every call here is INSTANCE-addressed. The manager always knows the
    /// key it means, and a manager that reached for `replace(id)` would be
    /// asking the host to resolve `current.toml` a second time — which after
    /// an upload names a different instance than the one it just stepped.
    async fn execute(&self, key: &InstanceKey, effect: Effect) {
        let module = key.module.as_str();
        match effect {
            Effect::StartFollower => {
                if let Err(e) = self.start_follower(key).await {
                    self.effect_failed(module, "start_follower", &e);
                }
            }
            Effect::StopFollower => match self.host.stop_instance(key).await {
                Ok(()) => tracing::info!(
                    module = %module,
                    instance = %key.dir_name(),
                    "lifecycle: follower stopped",
                ),
                Err(e) => self.effect_failed(module, "stop_follower", &e.to_string()),
            },
            Effect::Reap => self.reap(key).await,
            Effect::Alert(alert) => {
                tracing::error!(module = %module, ?alert, "lifecycle: instance alert");
                self.events
                    .record(module, EventKind::InstanceAlert { alert });
            }
        }
    }

    /// The host owns the whole teardown now — including removing the
    /// directory, which `evict_module`'s caller used to do. It has to: only
    /// the host knows whether its own handle-closing succeeded, and unlinking
    /// files under an open redb handle is how a follower ends up writing to
    /// an inode nothing can reach.
    ///
    /// A failure leaves the record saying `Reaped` with the directory still
    /// there, which is what `Instance::pending_cleanup` re-issues on every
    /// tick until it takes.
    async fn reap(&self, key: &InstanceKey) {
        let module = key.module.as_str();
        match self.host.evict_instance(key).await {
            Ok(cancelled) => {
                if !cancelled.is_empty() {
                    tracing::warn!(
                        module = %module,
                        companions = ?cancelled,
                        "lifecycle: reap cancelled live companion dial loops",
                    );
                }
                tracing::info!(
                    module = %module,
                    instance = %key.dir_name(),
                    "lifecycle: instance reaped",
                );
            }
            Err(e) => self.effect_failed(module, "reap", &e.to_string()),
        }
    }

    fn effect_failed(&self, module: &str, effect: &'static str, error: &str) {
        tracing::error!(
            module = %module,
            effect = %effect,
            error = %error,
            "lifecycle: effect failed; the record stands and the watchdog will retry",
        );
        self.events.record(
            module,
            EventKind::LifecycleEffectFailed {
                effect: effect.to_owned(),
                error: error.to_owned(),
            },
        );
    }

    // -----------------------------------------------------------------
    // Boot and tick
    // -----------------------------------------------------------------

    /// Adopt every module, reconcile it, and re-issue whatever its record
    /// says should already be true. REPLACES `host.auto_resume()`.
    ///
    /// One bad module is logged and skipped — `auto_resume` aborted the whole
    /// walk, which is how a single unreadable directory could leave every
    /// other module dead.
    pub async fn boot(&self) {
        let modules = match self.storage.list_modules() {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "lifecycle boot: list_modules failed; no module resumed");
                return;
            }
        };
        for module in modules {
            // BEFORE adoption, not after. Adoption counts client DIRECTORIES,
            // so a module whose registrations are still legacy flat files
            // would look client-less, be adopted as `Pinned`, and then never
            // retire when its clients later move — one zombie follower per
            // deploy, for ever. Idempotent, and `dialer.start_all()` runs it
            // again later without harm.
            match crate::companions::migrate_flat_companions_for_module(&self.storage, &module) {
                Ok((0, 0)) => {}
                Ok((migrated, quarantined)) => tracing::info!(
                    module = %module,
                    migrated,
                    quarantined,
                    "lifecycle boot: migrated legacy flat companion records",
                ),
                Err(e) => tracing::warn!(
                    module = %module,
                    error = %e,
                    "lifecycle boot: flat-companion migration failed; adoption may under-count \
                     this module's clients",
                ),
            }

            if let Err(e) = self.boot_module(&module).await {
                tracing::error!(
                    module = %module,
                    error = %e,
                    "lifecycle boot: module skipped; the others still start",
                );
            }
        }
    }

    async fn boot_module(&self, module: &str) -> Result<(), LifecycleError> {
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;
        // An unfinished instance build. Its writer died with the process, and
        // "resume it" is not available: half a copied `kv.redb` looks exactly
        // like a whole one. The move that wanted it re-queues (`recover`) and
        // `create_staging` builds it again from scratch.
        if let Ok(module_id) = mitos_lifecycle::ids::ModuleId::parse(module) {
            self.storage.sweep_partial_instances(&module_id);
        }
        self.reconcile_locked(module).await?;
        for view in self.instances(module)? {
            let key = view.key().clone();
            match view {
                InstanceView::Managed(_) => {}
                InstanceView::Unmanaged(_) => {
                    // Once, at boot, and then never again: an operator has a
                    // directory here that no move made and no record governs.
                    // It is not started, not reaped and not adopted.
                    tracing::warn!(
                        module = %module,
                        instance = %key.dir_name(),
                        "lifecycle boot: instance directory has no record and is not the \
                         module's current instance; leaving it untouched",
                    );
                    continue;
                }
            }
            // Re-load under the lock so the adoption is PERSISTED — `peek_at`
            // derived it without writing.
            let instance = match self.load_or_adopt_at(&key) {
                Ok(instance) => instance,
                Err(e) => {
                    tracing::error!(
                        module = %module,
                        instance = %key.dir_name(),
                        error = %e,
                        "lifecycle boot: instance skipped; the others still start",
                    );
                    continue;
                }
            };
            // `recover` re-issues what a restart must re-do: follower
            // liveness is in memory, so nothing is running and `on_boot`
            // says what should be.
            self.execute_all(&key, &instance.recover()).await;
        }
        Ok(())
    }

    /// One pass of the 60s timer: reconcile, then `Tick`, for every module.
    pub async fn tick(&self) {
        let modules = match self.storage.list_modules() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "lifecycle tick: list_modules failed; skipping");
                return;
            }
        };
        for module in modules {
            let lock = self.lock_for(&module);
            let _guard = lock.lock().await;
            if let Err(e) = self.reconcile_locked(&module).await {
                tracing::warn!(module = %module, error = %e, "lifecycle tick: reconcile failed");
                continue;
            }
            let instances = match self.instances(&module) {
                Ok(instances) => instances,
                Err(e) => {
                    tracing::warn!(module = %module, error = %e, "lifecycle tick: list failed");
                    continue;
                }
            };
            for view in instances {
                // An `Unmanaged` instance has no machine to tick.
                if view.managed().is_none() {
                    continue;
                }
                let key = view.key().clone();
                // A `Staging` instance NO move wants is released here, every
                // tick — not only by the `ReleaseStaging` effect. That effect
                // is issued once, by a transition whose row is persisted
                // BEFORE it runs and which leaves the row with no pending
                // move: crash in between, or let the release fail, and
                // `recover()` has nothing to re-issue. The instance would
                // then follow the chain for ever with nobody to speak to, on
                // a box whose binding constraint is memory. The rule is the
                // effect's own ("still `Staging` and no move wants it"), so
                // re-applying it is executing, not deciding — and a staging
                // instance is never unwanted by accident: its row is written
                // before `EnsureInstance` creates it.
                if view
                    .managed()
                    .is_some_and(|i| i.phase == mitos_lifecycle::instance::Phase::Staging)
                    && let Err(e) = self.release_staging_locked(&key).await
                {
                    tracing::warn!(
                        module = %module,
                        instance = %key.dir_name(),
                        error = %e,
                        "lifecycle tick: releasing an unwanted staging instance failed",
                    );
                }
                match self.apply_at_locked(&key, Event::Tick).await {
                    Ok(_) | Err(LifecycleError::Rejected(_)) => {}
                    Err(e) => tracing::warn!(
                        module = %module,
                        instance = %key.dir_name(),
                        error = %e,
                        "lifecycle tick failed",
                    ),
                }
                // `Tick` on a reaped instance is REJECTED, so a `Reap` that
                // could not finish — the host refuses to remove a directory
                // when its own teardown failed — would otherwise wait for the
                // next host restart. `pending_cleanup` is the machine's list
                // of what is safe to re-issue while the host is up; it is
                // empty for every phase but `Reaped`.
                match self.load_or_adopt_at(&key) {
                    Ok(instance) => self.execute_all(&key, &instance.pending_cleanup()).await,
                    // The reap finished and took the instance with it.
                    Err(e) if e.is_gone() => {}
                    Err(e) => tracing::warn!(
                        module = %module,
                        instance = %key.dir_name(),
                        error = %e,
                        "lifecycle tick: reload failed",
                    ),
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Revive
    // -----------------------------------------------------------------

    /// Restart an instance whose follower died — the watchdog's one action.
    ///
    /// Under the module lock, and it RE-CHECKS there. The watchdog works from
    /// a snapshot taken with no lock, and `upload` deliberately holds a
    /// follower stopped for as long as its block in flight takes to finish,
    /// then renames its directory. A revive acting on the snapshot would
    /// start `K1` in that gap: before the rename it runs across it (no cursor
    /// writes, no fan-out — WO-06's review), and the upload's own
    /// `start_instance(K2)` then fails because `K1` still holds the redb
    /// files. The same gap is how an operator's stop could be undone for a
    /// sweep. Waiting for the lock and asking again closes both: by then the
    /// record says what is wanted and the directory is where it will stay.
    pub async fn revive(&self, key: &InstanceKey) -> Revival {
        let module = key.module.as_str();
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;

        if !self.storage.instance_dir(key).is_dir() {
            // Renamed away (an upload) or reaped while we waited.
            return Revival::NotNeeded;
        }
        if self.host.running_instances(module).await.contains(key) {
            return Revival::NotNeeded;
        }
        match self.peek_at(key).map(|view| view.on_boot()) {
            Ok(BootAction::StartFollower) => {}
            Ok(BootAction::LeaveStopped) => return Revival::NotNeeded,
            Err(e) => return Revival::Failed(e.to_string()),
        }
        match self.host.start_instance(key).await {
            Ok(()) => Revival::Restarted,
            Err(e) => Revival::Failed(e.to_string()),
        }
    }

    /// Cycle the current instance's follower in place — an operator's
    /// `restart` of a module that is already running. Under the module lock
    /// for the same reason as [`Self::revive`], and by KEY: the host's `&str`
    /// `replace` stops "whatever the facade last started", which is nothing
    /// now that this manager starts every follower itself.
    pub async fn cycle_follower(&self, module: &str) -> Result<(), LifecycleError> {
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;
        let key = self.current_key(module)?;
        // `start_instance` stops the key's slot first.
        self.host
            .start_instance(&key)
            .await
            .map_err(|e| LifecycleError::Host(e.to_string()))
    }

    // -----------------------------------------------------------------
    // Creating a staging instance — `client_move::Effect::EnsureInstance`
    // -----------------------------------------------------------------

    /// Make `key` exist, with its state prepared from `parent`, and follow.
    ///
    /// **Everything here runs under the MODULE's lifecycle lock**, because it
    /// stops and starts a follower. WO-07's review is what happens when
    /// something does that from outside: an unlocked sweep started the old
    /// instance in the middle of its own upload, and the upload's start of
    /// the new one then failed on redb "already open".
    ///
    /// The caller — `MoveManager` executing `EnsureInstance` — is holding a
    /// ROW lock, so it must call this from a spawned task. The copy takes as
    /// long as the parent's `kv.redb` is big.
    pub async fn create_staging(
        &self,
        key: &InstanceKey,
        parent: Option<&InstanceKey>,
    ) -> Result<(), StagingError> {
        let lock = self.lock_for(key.module.as_str());
        let _guard = lock.lock().await;
        self.create_staging_locked(key, parent).await
    }

    async fn create_staging_locked(
        &self,
        key: &InstanceKey,
        parent: Option<&InstanceKey>,
    ) -> Result<(), StagingError> {
        // 1. ALREADY THERE. Both the recovery path (a crash after the rename,
        //    re-driven by `ClientRow::recover`) and D4's "B already exists" —
        //    a second client announcing the build a first one is moving to.
        //    Its cursor is the whole point: it has been following, and
        //    rebuilding it would throw that away.
        if self.storage.instance_dir(key).is_dir() {
            return match self.peek_at(key)? {
                InstanceView::Managed(instance) => {
                    if instance.on_boot() == BootAction::StartFollower
                        && !self
                            .host
                            .running_instances(key.module.as_str())
                            .await
                            .contains(key)
                    {
                        self.start_follower(key)
                            .await
                            .map_err(|_| StagingError::StartFailed)?;
                    }
                    Ok(())
                }
                // A directory with no record is one nothing here made. Taking
                // it as a move's target would adopt whatever state an
                // operator left in it.
                InstanceView::Unmanaged(key) => Err(StagingError::Unmanaged(key.dir_name())),
            };
        }

        // 2. THE PLAN, from pure functions only.
        let target = self.artifact_manifest(&key.wasm)?;
        let parent_manifest = match parent {
            Some(parent) => Some(self.artifact_manifest(&parent.wasm)?),
            None => None,
        };
        let plan = state_prep::plan(
            parent_manifest.as_ref().map(|m| m.lifecycle.state_schema),
            target.lifecycle.state_schema,
            // ALWAYS: there is no `migrate` export in the WIT yet (WO-04), so
            // the host has no way to run one.
            state_prep::MigrateExport::Absent,
        );
        if matches!(plan, state_prep::StatePlan::CopyThenMigrate { .. }) {
            // Unreachable while `MigrateExport::Absent` is a constant. If it
            // ever is reached, the copy would be at the parent's schema with
            // nothing to bring it forward — refuse rather than copy and hope.
            return Err(StagingError::MigrateUnsupported);
        }
        let instance_plan = upgrade::resolve_instance(
            &target.lifecycle.upgrade,
            parent_manifest.as_ref().map(|m| m.lifecycle.revision),
            target.lifecycle.revision,
        );
        let carry = state_prep::copy_set(plan, instance_plan.watched);
        tracing::info!(
            module = %key.module,
            instance = %key.dir_name(),
            parent = parent.map(|p| p.dir_name()).unwrap_or_else(|| "-".to_owned()),
            ?plan,
            watched = ?instance_plan.watched,
            "creating a staging instance",
        );

        // 3. BUILD UNDER A TEMP NAME. Nothing on the host can see a
        //    `.partial`, so every crash window below leaves a tree that is
        //    either complete or invisible.
        let partial = self.storage.begin_instance(key)?;
        if let Err(e) = self.build_partial(&partial, parent, carry).await {
            // The host is still up, so do not leave a parent's worth of disk
            // behind for boot's sweep to find.
            partial.discard();
            return Err(e);
        }

        // 4. Start it. A failure here leaves a complete, MANAGED, `Staging`
        //    instance — so the watchdog will retry it, and `ReleaseStaging`
        //    from the move this failure fails is what cleans it up.
        self.start_follower(key).await.map_err(|e| {
            self.effect_failed(key.module.as_str(), "start_staging", &e);
            StagingError::StartFailed
        })
    }

    /// Fill a partial and commit it. Split out so ONE caller can discard the
    /// partial on any failure in here.
    async fn build_partial(
        &self,
        partial: &PartialInstance,
        parent: Option<&InstanceKey>,
        carry: state_prep::CopySet,
    ) -> Result<(), StagingError> {
        let wanted: Vec<StateFile> = [
            (StateFile::Kv, carry.kv),
            (StateFile::Cursor, carry.cursor),
            (StateFile::Watched, carry.watched),
        ]
        .into_iter()
        .filter(|(_, carry)| *carry == state_prep::Carry::Copy)
        .map(|(file, _)| file)
        .collect();
        if !wanted.is_empty() {
            // `copy_set` only ever says `Copy` for a plan that HAS a parent
            // (`Fresh` and `Rebuild` are `StartEmpty` throughout), so this is
            // a contradiction in the pure crate rather than a host state.
            let parent = parent.ok_or(StagingError::CopyWithoutParent)?;
            self.copy_from_parent(partial, parent, wanted).await?;
        }
        // The record BEFORE the marker, and the marker inside `commit`: a
        // directory is only an instance once it has both, and a managed one
        // only once it has a record.
        partial.write_lifecycle(&Instance::staging(partial.key().clone()).record())?;
        partial.clone().commit()?;
        Ok(())
    }

    /// Stop the parent's follower, carry its files across, and START IT AGAIN
    /// — on every path out, success or failure.
    ///
    /// `stop_instance` awaits the block in flight and flushes the cursor, so
    /// `kv`, `cursor` and `watched` all describe the same block; that is
    /// `state_prep`'s rule 1, and it is also why the copy IS the parent's
    /// downtime. The parent's RECORD is untouched — nobody decided it should
    /// stop, exactly as in `upload` — so `on_boot` is what says whether to
    /// put it back, and an operator's stop survives a move it knew nothing
    /// about.
    async fn copy_from_parent(
        &self,
        partial: &PartialInstance,
        parent: &InstanceKey,
        files: Vec<StateFile>,
    ) -> Result<(), StagingError> {
        let module = parent.module.as_str();
        // A parent whose directory is GONE — an upload renamed it away —
        // would carry no files at all, because a file that is not there is
        // `Ok(0)`: the move would silently get an EMPTY instance where it
        // asked for a copy. `ClientRow::rebase` is what keeps this from
        // happening (`moves.rs` §1), and this is the second lock on the
        // door: a rebuild is a decision `state_prep` makes, never something
        // a missing directory decides for it.
        if !self.storage.instance_dir(parent).is_dir() {
            return Err(StagingError::ParentGone(parent.dir_name()));
        }

        let started = std::time::Instant::now();
        // If the follower will not stop, there is NO copy. `upload` can carry
        // on past a failed quiesce because a rename moves the files whole; a
        // byte copy of redb files with their writer still running is a torn
        // `kv` beside a `cursor` from a different block, which is the one
        // thing `state_prep`'s rule 1 exists to forbid — and it would not
        // fail here but weeks later, as a module whose state is quietly
        // wrong. Nothing was stopped, so there is nothing to restart.
        if let Err(e) = self.host.stop_instance(parent).await {
            self.effect_failed(module, "quiesce_for_staging", &e.to_string());
            return Err(StagingError::ParentWouldNotStop(parent.dir_name()));
        }

        // Off the async workers: this is synchronous file IO, seconds of it
        // for the largest module on a filesystem without reflink, and every
        // follower on the host dispatches on the runtime it would block.
        let (copier, from) = (partial.clone(), parent.clone());
        let copied = tokio::task::spawn_blocking(move || {
            let mut bytes = 0u64;
            for file in files {
                bytes += copier.carry(&from, file)?;
            }
            Ok::<u64, StorageError>(bytes)
        })
        .await;
        let (bytes, outcome) = match copied {
            Ok(Ok(bytes)) => (bytes, Ok(())),
            Ok(Err(e)) => (0, Err(StagingError::Storage(e))),
            Err(join) => (
                0,
                Err(StagingError::Storage(StorageError::Io(
                    std::io::Error::other(format!("staging copy task: {join}")),
                ))),
            ),
        };

        // ALWAYS, and before the `?`. Leaving a live client's instance
        // stopped because a copy for somebody else's move failed would be
        // the worst outcome this whole path has.
        if matches!(
            self.peek_at(parent).map(|view| view.on_boot()),
            Ok(BootAction::StartFollower)
        ) && let Err(e) = self.start_follower(parent).await
        {
            self.effect_failed(module, "restart_after_staging_copy", &e);
        }

        // The parent's downtime IS this number. The largest `kv.redb` today
        // is 1.3 GB, and "a dev worker's announcement paused prod's follower
        // for N seconds" is a thing an operator must be able to see after the
        // fact — so it goes on the ring, not only in the journal.
        let millis = started.elapsed().as_millis() as u64;
        tracing::info!(
            module = %module,
            parent = %parent.dir_name(),
            instance = %partial.key().dir_name(),
            bytes,
            millis,
            ok = outcome.is_ok(),
            "staging copy finished; the parent's follower was stopped for its duration",
        );
        self.events.record(
            module,
            EventKind::StagingCopy {
                instance: partial.key().dir_name(),
                parent: parent.dir_name(),
                bytes,
                millis,
            },
        );
        outcome
    }

    fn artifact_manifest(&self, wasm: &Sha256Hex) -> Result<Manifest, StagingError> {
        self.storage
            .read_artifact_manifest(wasm)?
            .ok_or_else(|| StagingError::NoManifest(wasm.to_string()))
    }

    /// `client_move::Effect::ReleaseStaging` — this move no longer wants
    /// `key`.
    ///
    /// TWO clients can be moving to one build (they announced the same one),
    /// so one giving up must not take the instance the other is still
    /// catching up on. The rows are the record of who wants it.
    pub async fn release_staging(&self, key: &InstanceKey) -> Result<(), LifecycleError> {
        let lock = self.lock_for(key.module.as_str());
        let _guard = lock.lock().await;
        self.release_staging_locked(key).await
    }

    /// Caller holds the module lock. Shared with the tick's sweep, which is
    /// what makes `ReleaseStaging` safe to lose — see `tick`.
    async fn release_staging_locked(&self, key: &InstanceKey) -> Result<(), LifecycleError> {
        let module = key.module.as_str();
        if self.a_move_still_wants(key) {
            tracing::debug!(
                module = %module,
                instance = %key.dir_name(),
                "staging instance released by one move; another still wants it",
            );
            return Ok(());
        }
        match self.apply_at_locked(key, Event::StagingReleased).await {
            Ok(_) => Ok(()),
            // `StagingReleased` on an instance that has since become `Active`
            // is REFUSED, and that is the point: a client attached to it
            // between the release and now, so it is not staging any more.
            Err(LifecycleError::Rejected(rejected)) => {
                tracing::debug!(
                    module = %module,
                    instance = %key.dir_name(),
                    rejected = %rejected,
                    "staging release refused; the instance is no longer staging",
                );
                Ok(())
            }
            // The reap already finished and took it.
            Err(e) if e.is_gone() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Whether ANY row of the module has a pending move targeting `key`.
    ///
    /// A row that will not read counts as WANTING it: refusing to reap is the
    /// recoverable mistake, reaping an instance a client is catching up on is
    /// not.
    fn a_move_still_wants(&self, key: &InstanceKey) -> bool {
        match self.storage.list_rows(key.module.as_str()) {
            Ok(rows) => rows.iter().any(|row| {
                row.pending
                    .as_ref()
                    .is_some_and(|p| &p.target.instance == key)
            }),
            Err(e) => {
                tracing::warn!(
                    module = %key.module,
                    instance = %key.dir_name(),
                    error = %e,
                    "cannot read this module's rows; keeping the staging instance",
                );
                true
            }
        }
    }

    // -----------------------------------------------------------------
    // Upload
    // -----------------------------------------------------------------

    /// Activate an artifact and start it ONLY IF the instance expects a
    /// follower.
    ///
    /// Today's handler calls `host.replace` unconditionally, which is the
    /// third way an operator's stop gets undone. The record survives the
    /// activation untouched: `activate` RENAMES the instance directory to the
    /// new key and `lifecycle.toml` moves with it.
    pub async fn upload(
        &self,
        manifest: &Manifest,
        wasm: &[u8],
        config: Option<&[u8]>,
    ) -> Result<BootAction, LifecycleError> {
        let module = manifest.module.id.clone();
        let lock = self.lock_for(&module);
        let _guard = lock.lock().await;

        // QUIESCE FIRST. `activate` and `write_config` each RENAME the
        // instance directory, and a follower that is running across a rename
        // is holding a key that no longer exists: its cursor write is refused
        // (`NoSuchInstance` — creating the directory would resurrect it), and
        // the fan-out sees an instance that is not the module's current one
        // and writes no rows. A follower mid-block when the upload lands would
        // lose that block's emissions, and the replay would run against KV
        // state that had already absorbed the block — so a module that emits
        // on CHANGE would not emit them again. Stopping awaits the follower,
        // which finishes its block and flushes its cursor under the key it
        // was started with.
        //
        // Not a lifecycle event: nobody decided this instance should stop.
        // The record is untouched, and `on_boot` below restarts it.
        //
        // Resolved BEFORE the rename, and stopped BY KEY. After the rename
        // `current.toml` names K2 and the running follower is still K1; a
        // `stop(id)` that resolved the pointer a second time would stop
        // nothing at all.
        let before = match self.storage.current_instance(&module) {
            Ok(before) => before,
            Err(e) => {
                // A pointer that will not resolve — a crash the repair could
                // not fix, or a tree we did not write. There is no follower
                // to quiesce, and an upload is one of the things that REPAIRS
                // such a module, so this must not refuse one.
                self.effect_failed(&module, "quiesce_for_upload", &e.to_string());
                None
            }
        };
        if let Some(before) = &before
            && let Err(e) = self.host.stop_instance(before).await
        {
            self.effect_failed(&module, "quiesce_for_upload", &e.to_string());
        }

        let activated = self.storage.activate(manifest, wasm).and_then(|()| {
            config.map_or(Ok(()), |config| self.storage.write_config(&module, config))
        });
        if let Err(e) = activated {
            // The follower was stopped for an upload that did not happen.
            // Put it back — on whatever `current.toml` names now, which is
            // the old instance, or the half-way one if only the config
            // failed.
            if let Ok(key) = self.current_key(&module)
                && matches!(
                    self.peek_at(&key).map(|view| view.on_boot()),
                    Ok(BootAction::StartFollower)
                )
            {
                self.execute(&key, Effect::StartFollower).await;
            }
            return Err(e.into());
        }

        // Resolved AGAIN, after the rename: this is K2.
        let key = self.current_key(&module)?;
        let action = self.load_or_adopt_at(&key)?.on_boot();
        if action == BootAction::StartFollower {
            self.execute(&key, Effect::StartFollower).await;
        } else {
            tracing::info!(
                module = %module,
                "upload activated but NOT started: the instance is stopped by an operator",
            );
        }
        Ok(action)
    }
}

/// Cadence of the lifecycle timer. Every timer in the machine is measured in
/// minutes or hours (a 10-minute dev grace, a 24-hour retain), so a minute is
/// a fine resolution and the sweep itself is a directory listing per module.
pub const TICK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// The 60s tick task. Cancelled on shutdown like the watchdog and compaction,
/// and awaited there — a `Reap` mid-flight removes a module directory and
/// must not race the follower teardown.
pub fn spawn_tick(
    manager: Arc<LifecycleManager>,
    cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick =
            tokio::time::interval_at(tokio::time::Instant::now() + TICK_INTERVAL, TICK_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("lifecycle tick cancelled");
                    return;
                }
                _ = tick.tick() => manager.tick().await,
            }
        }
    })
}

/// The instance's `attached` set as the admin API reports it — sorted
/// `namespace/client` labels. One renderer, so `/_admin/status`,
/// `/_admin/modules` and the per-module route cannot drift apart.
pub fn attached_labels(instance: &Instance) -> Vec<String> {
    instance
        .attached
        .iter()
        .map(|key| format!("{}/{}", key.namespace, key.client))
        .collect()
}

/// Fixtures shared with the admin and companion router tests.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A clock a test moves by hand. The pure crate never reads one, and
    /// neither may a test of this: draining, stopping and reaping are hours
    /// apart, and none of it is worth sleeping for.
    #[derive(Clone)]
    pub(crate) struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        pub(crate) fn new(now: u64) -> Self {
            Self(Arc::new(AtomicU64::new(now)))
        }

        pub(crate) fn advance(&self, secs: u64) {
            self.0.fetch_add(secs, Ordering::Relaxed);
        }

        pub(crate) fn clock(&self) -> Clock {
            let inner = self.0.clone();
            Arc::new(move || UnixSecs(inner.load(Ordering::Relaxed)))
        }
    }

    /// Write a companion registration for `(module, client, key)`. The bytes
    /// are a real `SubscribeRequest`, because the flat-file migration and the
    /// interest union both decode them.
    pub(crate) fn write_registration(
        storage: &ModuleStorage,
        module: &str,
        client: &str,
        key: &str,
    ) {
        write_registration_with_interest(storage, module, client, key, Vec::new());
    }

    /// As [`write_registration`], with a declared interest.
    ///
    /// `reconcile_module_interest` routes the UNION of what a module's
    /// companions declare, so a registration with none is nothing to route —
    /// which is invisible unless a test says what it wanted routed.
    pub(crate) fn write_registration_with_interest(
        storage: &ModuleStorage,
        module: &str,
        client: &str,
        key: &str,
        interests: Vec<mitos_protocol::Interest>,
    ) {
        let dir = storage.module_dir_for_companions(module).join(client);
        std::fs::create_dir_all(&dir).expect("companion dir");
        let request = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module {
                name: module.to_owned(),
            }],
            companion_key: key.to_owned(),
            client_id: client.to_owned(),
            resume_from: None,
            interests,
            dial_back: None,
            announced: Vec::new(),
        };
        std::fs::write(dir.join(format!("{key}.cbor")), request.encode().unwrap())
            .expect("write registration");
    }

    pub(crate) fn remove_registration(
        storage: &ModuleStorage,
        module: &str,
        client: &str,
        key: &str,
    ) {
        let path = storage
            .module_dir_for_companions(module)
            .join(client)
            .join(format!("{key}.cbor"));
        std::fs::remove_file(path).expect("remove registration");
    }

    /// Activate `module` so it has an instance for the lifecycle to govern.
    pub(crate) fn activate(storage: &ModuleStorage, module: &str, wasm: &[u8]) {
        let manifest = crate::companions::testing::manifest_for(module, wasm);
        storage.activate(&manifest, wasm).expect("activate");
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::auth::testing::{
        DEV_CLIENT, PROD_CLIENT, configured_authenticator, legacy_authenticator,
    };
    use crate::host_v2::testing::FakeHost;
    use mitos_lifecycle::access::OperatorTokenAsClient;
    use mitos_lifecycle::instance::{
        DesiredState, EvictMode, OperatorCommand, Phase, StopCause, ZeroRefCause,
    };

    const MODULE: &str = "ownership-indexer";
    const OTHER: &str = "collection-holders";
    const T0: u64 = 1_700_000_000;

    struct Harness {
        _tmp: tempfile::TempDir,
        storage: ModuleStorage,
        host: FakeHost,
        clock: TestClock,
        manager: Arc<LifecycleManager>,
        events: EventRing,
    }

    impl Harness {
        /// A host with `prod` and `dev` namespaces, the shape the real
        /// deployment has.
        fn new() -> Self {
            Self::with_auth(Arc::new(configured_authenticator(
                OperatorTokenAsClient::AllowWithWarning,
            )))
        }

        fn with_auth(auth: Arc<Authenticator>) -> Self {
            let tmp = tempfile::tempdir().expect("tempdir");
            let storage = ModuleStorage::new(tmp.path());
            let host = FakeHost::new(storage.clone());
            let clock = TestClock::new(T0);
            let events = EventRing::new();
            let manager = Arc::new(LifecycleManager::new(
                storage.clone(),
                Arc::new(host.clone()),
                auth,
                events.clone(),
                clock.clock(),
            ));
            Self {
                _tmp: tmp,
                storage,
                host,
                clock,
                manager,
                events,
            }
        }

        /// A SECOND manager over the same directory — a process restart.
        fn restart(&self) -> Arc<LifecycleManager> {
            Arc::new(LifecycleManager::new(
                self.storage.clone(),
                Arc::new(self.host.clone()),
                Arc::new(configured_authenticator(
                    OperatorTokenAsClient::AllowWithWarning,
                )),
                self.events.clone(),
                self.clock.clock(),
            ))
        }

        fn activate(&self, module: &str) {
            activate(&self.storage, module, module.as_bytes());
        }

        fn subscribe(&self, module: &str, client: &str) {
            write_registration(&self.storage, module, client, "policy_a");
        }

        fn instance(&self, module: &str) -> Instance {
            self.manager
                .instance(module)
                .expect("lifecycle readable")
                .expect("module registered")
        }

        /// A SECOND instance directory for `module`, with no record — the
        /// `Unmanaged` shape. WO-08 owns creating one for real; this is the
        /// two public calls that make a directory, and nothing more.
        fn plant(&self, module: &str, wasm: &[u8]) -> InstanceKey {
            let manifest = crate::companions::testing::manifest_for(module, wasm);
            self.storage
                .put_artifact(&manifest, wasm)
                .expect("put artifact");
            let key = InstanceKey {
                module: mitos_lifecycle::ids::ModuleId::parse(module).expect("module id"),
                wasm: mitos_lifecycle::ids::Sha256Hex::parse(&crate::manifest::sha256_hex(wasm))
                    .expect("sha"),
                config: None,
            };
            self.storage
                .write_instance_marker(&key)
                .expect("instance marker");
            key
        }

        /// …and its first record, which is what makes it MANAGED. WO-08
        /// writes `Instance::staging(key)`'s; a test that needs the instance
        /// serving passes an adopted one instead.
        fn govern(&self, instance: &Instance) {
            self.storage
                .write_lifecycle_at(&instance.key, &instance.record())
                .expect("write record");
        }

        /// A second instance that is `Active` + `Pinned` — what adoption
        /// concludes for a running instance nobody is attached to, and the
        /// only starting shape an operator command is legal on (`Staging` is
        /// controlled by its client move, not by an operator).
        fn second(&self, module: &str, wasm: &[u8]) -> InstanceKey {
            let key = self.plant(module, wasm);
            self.govern(&Instance::adopt(key.clone(), &BTreeMap::new()));
            key
        }

        fn record_at(&self, key: &InstanceKey) -> Option<Instance> {
            self.storage
                .read_lifecycle_at(key)
                .expect("record readable")
                .map(|record| Instance::from_record(key.clone(), record))
        }

        fn current(&self, module: &str) -> InstanceKey {
            self.storage
                .current_instance(module)
                .expect("pointer readable")
                .expect("module registered")
        }

        /// Write a client row placing `client` on `instance` — what a
        /// committed move leaves behind, and the only thing `routing` reads.
        fn place(&self, module: &str, client: &str, instance: &InstanceKey) {
            use mitos_lifecycle::client_move::{ClientRow, Placement};
            use mitos_lifecycle::ids::Revision;
            let row = ClientRow::adopt(
                self.manager
                    .auth
                    .registry()
                    .client_key(&ClientId::parse(client).expect("client id"))
                    .expect("a client some namespace owns"),
                mitos_lifecycle::ids::ModuleId::parse(module).expect("module id"),
                Placement {
                    instance: instance.clone(),
                    revision: Revision(0),
                },
            );
            self.storage.write_row(module, &row).expect("write row");
        }

        async fn sweep(&self) {
            let dialer = crate::dialer::CompanionDialer::new(
                self.storage.clone(),
                crate::admin::AuthToken(None),
                None,
            );
            crate::watchdog::sweep(
                &self.storage,
                &self.host,
                &self.manager,
                &dialer,
                &self.events,
            )
            .await;
        }

        async fn operator(&self, module: &str, command: OperatorCommand) -> Vec<Effect> {
            self.manager
                .apply(module, Event::Operator(command))
                .await
                .expect("operator command accepted")
        }

        fn alerts(&self) -> usize {
            self.events
                .snapshot(0, None, Some("instance_alert"), 512)
                .len()
        }

        /// Put an artifact in the CAS with a chosen `[lifecycle]`, and return
        /// the key a move to it would target. NOT an instance:
        /// `create_staging` is the only thing that makes one.
        fn artifact(
            &self,
            module: &str,
            wasm: &[u8],
            revision: u32,
            schema: u32,
            upgrade: &[(u32, mitos_lifecycle::upgrade::UpgradeAction)],
        ) -> InstanceKey {
            use mitos_lifecycle::ids::Revision;
            use mitos_lifecycle::state_prep::StateSchema;
            use mitos_lifecycle::upgrade::UpgradeEntry;
            let mut manifest = crate::companions::testing::manifest_for(module, wasm);
            manifest.lifecycle.revision = Revision(revision);
            manifest.lifecycle.state_schema = StateSchema(schema);
            manifest.lifecycle.upgrade = upgrade
                .iter()
                .map(|(since, action)| UpgradeEntry {
                    since: Revision(*since),
                    actions: std::collections::BTreeSet::from([*action]),
                })
                .collect();
            let wasm_sha = self
                .storage
                .put_artifact(&manifest, wasm)
                .expect("put artifact");
            InstanceKey {
                module: mitos_lifecycle::ids::ModuleId::parse(module).expect("module id"),
                wasm: wasm_sha,
                config: None,
            }
        }

        /// Activate `module` from an artifact with a chosen `[lifecycle]`, so
        /// a move can be PLANNED from its current instance's manifest.
        fn activate_at(
            &self,
            module: &str,
            wasm: &[u8],
            revision: u32,
            schema: u32,
        ) -> InstanceKey {
            let key = self.artifact(module, wasm, revision, schema, &[]);
            let manifest = self
                .storage
                .read_artifact_manifest(&key.wasm)
                .unwrap()
                .unwrap();
            self.storage.activate(&manifest, wasm).expect("activate");
            self.current(module)
        }

        /// Give an instance the three files a `CopySet` can carry.
        fn fill_state(&self, key: &InstanceKey, marker: &str, slot: u64) {
            self.kv_set(key, marker);
            self.storage
                .write_cursor_at(key, &mitos_data_plane::ChainPoint::Slot(slot))
                .expect("write cursor");
            self.storage
                .watched_index_at(key)
                .apply(&[watched_ref()], &[]);
        }

        fn kv_set(&self, key: &InstanceKey, marker: &str) {
            self.storage
                .kv_store_at(key, None)
                .expect("kv")
                .set_value(key.module.as_str(), "marker", marker.as_bytes().to_vec())
                .expect("set marker");
        }

        fn kv_get(&self, key: &InstanceKey) -> Option<String> {
            self.storage
                .kv_store_at(key, None)
                .ok()?
                .get_value(key.module.as_str(), "marker")
                .ok()
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        }

        fn has(&self, key: &InstanceKey, file: crate::storage::StateFile) -> bool {
            self.storage
                .instance_dir(key)
                .join(file.file_name())
                .is_file()
        }

        /// A client row that is MOVING from `parent` to `target` — what
        /// `release_staging` reads to answer "does anyone still want this".
        fn moving(&self, client: &str, parent: &InstanceKey, target: &InstanceKey) {
            use mitos_lifecycle::client_move::{ClientRow, Move, Phase as MovePhase, Stage};
            use mitos_lifecycle::ids::Revision;
            use mitos_lifecycle::policy::RevisionPolicy;
            use mitos_lifecycle::upgrade::{ClientPlan, MoveIntent};
            let mut row = ClientRow::adopt(
                self.manager
                    .auth
                    .registry()
                    .client_key(&ClientId::parse(client).expect("client id"))
                    .expect("a client some namespace owns"),
                parent.module.clone(),
                mitos_lifecycle::client_move::Placement {
                    instance: parent.clone(),
                    revision: Revision(1),
                },
            );
            row.pending = Some(Move {
                target: mitos_lifecycle::client_move::Placement {
                    instance: target.clone(),
                    revision: Revision(2),
                },
                manifest: target.wasm.clone(),
                intent: MoveIntent::Announced,
                revision_policy: RevisionPolicy::Allow,
                stage: Stage::Planned {
                    plan: ClientPlan::QUIET,
                    phase: MovePhase::CatchingUp,
                },
            });
            self.storage
                .write_row(parent.module.as_str(), &row)
                .expect("write row");
        }
    }

    fn watched_ref() -> mitos_data_plane::OutputRef {
        mitos_data_plane::OutputRef::from_bytes([7u8; 32], 0)
    }

    // -----------------------------------------------------------------
    // The bug this order exists to fix: an operator's stop STICKS.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn an_operator_stop_survives_a_restart() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        assert!(h.host.started(MODULE));

        h.operator(MODULE, OperatorCommand::Stop).await;
        assert!(h.host.stopped(MODULE));
        h.host.clear();

        // A new manager over the same directory — a process restart. v2's
        // `auto_resume` started every module with a manifest on disk.
        h.restart().boot().await;
        assert!(
            !h.host.started(MODULE),
            "a restart resumed a module an operator had stopped",
        );
    }

    #[tokio::test]
    async fn an_operator_stop_is_not_undone_by_the_watchdog() {
        let h = Harness::new();
        h.activate(MODULE);
        // The module HAS companions — which is exactly when v2's watchdog
        // undid the stop, within ONE TICK rather than on the next restart.
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.operator(MODULE, OperatorCommand::Stop).await;
        h.host.clear();

        let dialer = crate::dialer::CompanionDialer::new(
            h.storage.clone(),
            crate::admin::AuthToken(None),
            None,
        );
        crate::watchdog::sweep(&h.storage, &h.host, &h.manager, &dialer, &h.events).await;

        assert!(
            !h.host.started(MODULE),
            "the watchdog revived a module an operator had stopped",
        );

        // …and the sweep still does its job. Without this the assertion above
        // would pass on a watchdog that had simply stopped working.
        h.activate(OTHER);
        h.subscribe(OTHER, PROD_CLIENT);
        h.manager.boot().await;

        // A live follower is left alone.
        h.host.clear();
        crate::watchdog::sweep(&h.storage, &h.host, &h.manager, &dialer, &h.events).await;
        assert!(
            !h.host.started(OTHER),
            "the watchdog restarted a live follower"
        );

        // One that died is revived.
        h.host.follower_died(OTHER);
        crate::watchdog::sweep(&h.storage, &h.host, &h.manager, &dialer, &h.events).await;
        assert!(
            h.host.started(OTHER),
            "the watchdog left a dead follower dead"
        );
    }

    #[tokio::test]
    async fn an_operator_stop_is_not_undone_by_an_upload() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.operator(MODULE, OperatorCommand::Stop).await;
        h.host.clear();

        let wasm = b"a whole new build";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        let action = h
            .manager
            .upload(&manifest, wasm, None)
            .await
            .expect("upload accepted");

        assert_eq!(action, BootAction::LeaveStopped);
        assert!(
            !h.host.started(MODULE),
            "an upload started a module an operator had stopped",
        );
        // The artifact still landed — a stop is not a refusal to deploy.
        assert_eq!(
            h.storage
                .read_manifest(MODULE)
                .unwrap()
                .unwrap()
                .module
                .sha256,
            manifest.module.sha256,
        );
    }

    /// `activate` RENAMES the instance directory. A follower still running
    /// across that holds a key that no longer exists: its cursor write is
    /// refused and the fan-out, seeing an instance that is not the module's
    /// current one, writes no rows — so a block in flight loses its emissions.
    /// The follower must already be stopped when the directory moves.
    #[tokio::test]
    async fn an_upload_stops_the_follower_before_it_renames_anything() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();
        let before = h.storage.current_instance(MODULE).unwrap().unwrap();

        let seen_at_stop = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (storage, seen) = (h.storage.clone(), seen_at_stop.clone());
        h.host.on_stop(move || {
            seen.lock()
                .unwrap()
                .push(storage.current_instance(MODULE).unwrap());
        });

        let wasm = b"a whole new build";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        h.manager
            .upload(&manifest, wasm, Some(b"\xa0"))
            .await
            .expect("upload accepted");

        assert_eq!(
            *seen_at_stop.lock().unwrap(),
            vec![Some(before.clone())],
            "the follower was stopped AFTER its directory had been renamed",
        );
        let after = h.storage.current_instance(MODULE).unwrap().unwrap();
        assert_ne!(after, before);
        // By KEY, and the two keys are DIFFERENT — a stronger statement than
        // `Stop(id)` / `Replace(id)`, which could not tell the instance that
        // was quiesced from the one that was started.
        assert_eq!(
            h.host.calls(),
            vec![
                HostCall::StopInstance(before),
                HostCall::StartInstance(after)
            ],
        );
    }

    /// `upload` holds the follower STOPPED while it renames the directory, and
    /// the watchdog works from a snapshot taken with no lock. A revive acting
    /// on that snapshot starts `K1` inside the gap: a follower running across
    /// the rename again, and the upload's own start of `K2` then fails because
    /// `K1` still holds the files. So the revive waits for the module lock and
    /// ASKS AGAIN — and by then `K1` is not there to start.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_watchdog_revive_cannot_land_inside_an_uploads_quiesce() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();
        let before = h.storage.current_instance(MODULE).unwrap().unwrap();

        // The gap, held open: the follower is stopped, nothing is renamed yet.
        let (in_gap, mut wait_for_gap) = tokio::sync::mpsc::unbounded_channel();
        h.host.on_stop(move || {
            let _ = in_gap.send(());
            std::thread::sleep(std::time::Duration::from_millis(300));
        });

        // What the watchdog does once its snapshot says "K1 should be running
        // and is not" — which, inside the gap, is exactly what it says.
        let (manager, key) = (h.manager.clone(), before.clone());
        let watchdog = tokio::spawn(async move {
            wait_for_gap
                .recv()
                .await
                .expect("the upload reached its gap");
            manager.revive(&key).await
        });

        let wasm = b"a whole new build";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        h.manager
            .upload(&manifest, wasm, None)
            .await
            .expect("upload accepted");

        assert_eq!(watchdog.await.unwrap(), Revival::NotNeeded);
        let after = h.storage.current_instance(MODULE).unwrap().unwrap();
        assert_eq!(
            h.host.calls(),
            vec![
                HostCall::StopInstance(before),
                HostCall::StartInstance(after)
            ],
            "the watchdog started a follower inside the upload's quiesce",
        );
    }

    /// An operator's `restart` of a running module. It used to call the
    /// host's `&str` `replace`, which stops "whatever the facade last
    /// started" — nothing, now that this manager starts every follower by
    /// key — and took no lock.
    #[tokio::test]
    async fn cycling_a_follower_addresses_the_current_instance_by_key() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();
        let key = h.storage.current_instance(MODULE).unwrap().unwrap();

        h.manager.cycle_follower(MODULE).await.expect("cycled");

        assert_eq!(h.host.calls(), vec![HostCall::StartInstance(key)]);
    }

    /// …and when a follower really has died, it still comes back.
    #[tokio::test]
    async fn a_revive_restarts_a_follower_that_died_and_leaves_a_stopped_one() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        let key = h.storage.current_instance(MODULE).unwrap().unwrap();

        assert_eq!(h.manager.revive(&key).await, Revival::NotNeeded);

        h.host.follower_died(MODULE);
        assert_eq!(h.manager.revive(&key).await, Revival::Restarted);

        h.operator(MODULE, OperatorCommand::Stop).await;
        assert_eq!(h.manager.revive(&key).await, Revival::NotNeeded);
    }

    /// The follower is stopped FOR the upload, so an upload that then fails
    /// must not leave the module down.
    #[tokio::test]
    async fn a_failed_upload_puts_the_follower_back() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();
        let before = h.storage.current_instance(MODULE).unwrap();

        // A manifest that describes different bytes: `activate` refuses it.
        let manifest = crate::companions::testing::manifest_for(MODULE, b"what was built");
        let refused = h.manager.upload(&manifest, b"what arrived", None).await;

        assert!(refused.is_err());
        assert_eq!(h.storage.current_instance(MODULE).unwrap(), before);
        // Nothing was renamed, so the instance put back is the one quiesced.
        let key = before.expect("activated");
        assert_eq!(
            h.host.calls(),
            vec![
                HostCall::StopInstance(key.clone()),
                HostCall::StartInstance(key)
            ],
            "the old follower was not restarted",
        );
    }

    #[tokio::test]
    async fn an_operator_stop_is_not_undone_by_a_subscribe() {
        let h = Harness::new();
        h.activate(MODULE);
        h.manager.boot().await;
        h.operator(MODULE, OperatorCommand::Stop).await;
        h.host.clear();

        // A client subscribes to a stopped instance.
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.reconcile(MODULE).await.expect("reconcile");

        assert!(
            !h.host.started(MODULE),
            "a subscribe started a module an operator had stopped",
        );
        // The client IS recorded, and will be served the moment the operator
        // starts it. Losing the attach would be a different bug.
        let instance = h.instance(MODULE);
        assert_eq!(instance.attached.len(), 1);
        assert!(matches!(
            instance.phase,
            Phase::Stopped {
                cause: StopCause::Operator,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn restart_starts_a_stopped_module_and_replaces_a_running_one() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;

        // Running: `Start` changes the desired state but produces no
        // `StartFollower`, so the admin handler falls through to
        // `host.replace` — restart means "cycle it in place".
        let effects = h.operator(MODULE, OperatorCommand::Start).await;
        assert!(!effects.contains(&Effect::StartFollower));

        // Stopped: `Start` DOES produce one, and the handler must not then
        // replace a second time.
        h.operator(MODULE, OperatorCommand::Stop).await;
        h.host.clear();
        let effects = h.operator(MODULE, OperatorCommand::Start).await;
        assert!(effects.contains(&Effect::StartFollower));
        assert!(h.host.started(MODULE));
    }

    // -----------------------------------------------------------------
    // Adoption: the deploy must change nothing.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn adoption_changes_nothing() {
        let h = Harness::new();
        // A v2-shaped tree: one subscribed module, one with no subscribers.
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.activate(OTHER);

        h.manager.boot().await;

        // Both start, exactly as `auto_resume` started both.
        assert!(h.host.started(MODULE));
        assert!(h.host.started(OTHER));

        let subscribed = h.instance(MODULE);
        assert_eq!(subscribed.phase, Phase::Active);
        assert_eq!(subscribed.desired, DesiredState::Running);
        assert_eq!(
            attached_labels(&subscribed),
            vec!["prod/ownership.cnft.dev"]
        );

        // Unsubscribed ⇒ Pinned: v2 ran a module whether or not anyone
        // subscribed, and `Pinned` is the honest encoding — it neither alerts
        // nor ejects, and it is VISIBLE so an operator can retire it.
        let unsubscribed = h.instance(OTHER);
        assert_eq!(unsubscribed.phase, Phase::Active);
        assert_eq!(unsubscribed.desired, DesiredState::Pinned);

        // A YEAR of ticks. Nothing stops, nothing is reaped, nothing alerts —
        // every mainnet instance is either prod-governed or pinned.
        h.host.clear();
        for _ in 0..365 {
            h.clock.advance(24 * 3600);
            h.manager.tick().await;
        }
        assert_eq!(
            h.host.calls(),
            Vec::new(),
            "a year of ticks touched a follower"
        );
        assert_eq!(h.alerts(), 0);
    }

    #[tokio::test]
    async fn flat_registrations_are_migrated_before_adoption() {
        let h = Harness::new();
        h.activate(MODULE);

        // A pre-fix host wrote `companions/<key>.cbor` with no client
        // directory. Adoption counts DIRECTORIES, so without the migration
        // running first this module looks client-less, is adopted `Pinned`,
        // and then never retires when its clients later move — one zombie
        // follower per deploy, for ever.
        let root = h.storage.module_dir_for_companions(MODULE);
        std::fs::create_dir_all(&root).unwrap();
        let legacy = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module {
                name: MODULE.into(),
            }],
            companion_key: "policy_a".into(),
            client_id: String::new(),
            resume_from: None,
            interests: vec![],
            dial_back: Some(mitos_protocol::DialBackOverride {
                url: Some(format!("https://{PROD_CLIENT}/_internal/apply-x?key=y")),
                auth_header: None,
                auth_value: None,
            }),
            announced: Vec::new(),
        };
        std::fs::write(root.join("policy_a.cbor"), legacy.encode().unwrap()).unwrap();

        h.manager.boot().await;

        let instance = h.instance(MODULE);
        assert_eq!(
            instance.desired,
            DesiredState::Running,
            "a module whose only registration was a legacy flat file was adopted as Pinned",
        );
        assert_eq!(attached_labels(&instance), vec!["prod/ownership.cnft.dev"]);
    }

    // -----------------------------------------------------------------
    // Attach / detach
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn a_path_that_forgot_to_reconcile_is_corrected_by_the_tick() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        assert_eq!(h.instance(MODULE).attached.len(), 1);

        // Remove the registration BEHIND the manager's back — the shape of
        // any path somebody adds and forgets to hook.
        remove_registration(&h.storage, MODULE, PROD_CLIENT, "policy_a");
        h.manager.tick().await;

        assert!(
            h.instance(MODULE).attached.is_empty(),
            "reconcile is a diff; a missed hook must be corrected, not lost",
        );
    }

    // -----------------------------------------------------------------
    // Eject policy: dev reaps, prod alerts.
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn an_abandoned_dev_only_instance_drains_stops_and_is_reaped() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, DEV_CLIENT);
        h.manager.boot().await;
        h.host.clear();

        // The developer stopped riffing.
        remove_registration(&h.storage, MODULE, DEV_CLIENT, "policy_a");
        h.manager.tick().await;
        assert!(matches!(
            h.instance(MODULE).phase,
            Phase::Draining {
                cause: ZeroRefCause::Abandoned,
                ..
            }
        ));

        // +599s: inside the 10-minute grace. A quick re-subscribe would lose
        // nothing, so the follower keeps running.
        h.clock.advance(599);
        h.manager.tick().await;
        assert!(!h.host.stopped(MODULE));

        // +600s: grace expired.
        h.clock.advance(1);
        h.manager.tick().await;
        assert!(h.host.stopped(MODULE));
        assert!(!h.host.evicted(MODULE));

        // +24h: the retain window expires and the state is reaped.
        h.clock.advance(24 * 3600);
        h.manager.tick().await;
        assert!(h.host.evicted(MODULE));
        assert!(
            !h.storage.module_dir_for_companions(MODULE).exists(),
            "the module directory survived the reap",
        );
    }

    #[tokio::test]
    async fn a_prod_shared_instance_alerts_and_keeps_running() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();

        // In production a vanished client is far more likely an outage on the
        // client's side than a decision.
        remove_registration(&h.storage, MODULE, PROD_CLIENT, "policy_a");
        h.manager.tick().await;
        assert_eq!(h.alerts(), 1);

        // A year of ticks adds no second alert and stops nothing: an operator
        // decides, and until then the follower keeps its state warm.
        for _ in 0..365 {
            h.clock.advance(24 * 3600);
            h.manager.tick().await;
        }
        assert_eq!(h.alerts(), 1, "the alert repeated instead of waiting");
        assert!(!h.host.stopped(MODULE));
        assert!(!h.host.evicted(MODULE));
    }

    // -----------------------------------------------------------------
    // Crash safety
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn a_crash_between_persist_and_effects_is_recovered() {
        // `Reaped` on disk with the directory still there: the record was
        // written and the process died before the effect ran.
        let h = Harness::new();
        h.activate(MODULE);
        let mut record = h.instance(MODULE).record();
        record.phase = Phase::Reaped;
        h.storage.write_lifecycle(MODULE, &record).unwrap();

        h.manager.boot().await;
        assert!(h.host.evicted(MODULE), "a half-done reap was not finished");

        // And the ordinary case: `Active` on disk means start it, because
        // follower liveness is in memory and nothing survives a restart.
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.host.clear();
        h.restart().boot().await;
        assert!(h.host.started(MODULE));
    }

    #[tokio::test]
    async fn a_failed_effect_does_not_roll_back_the_record() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.host.fail_replace(MODULE);

        h.manager.boot().await;

        // The record is what SHOULD be true. Rolling it back would record a
        // module whose follower failed to start as stopped, and then nothing
        // would ever retry it.
        let instance = h.instance(MODULE);
        assert_eq!(instance.phase, Phase::Active);
        assert_eq!(instance.desired, DesiredState::Running);
        assert_eq!(
            h.events
                .snapshot(0, None, Some("lifecycle_effect_failed"), 16)
                .len(),
            1,
            "a failed effect must be queryable, not only in the journal",
        );

        // The watchdog is what closes the gap: it still expects a follower.
        assert_eq!(
            h.manager.expects_follower(MODULE).unwrap(),
            BootAction::StartFollower,
        );
    }

    /// `reap` refuses to remove a directory when the host-side teardown
    /// failed — unlinking files under open redb handles leaves a follower
    /// writing to an inode nothing can reach. But `Tick` on a reaped instance
    /// is REJECTED, so without `pending_cleanup` the half-reaped module would
    /// sit there, listed and dialled, until the next host restart.
    #[tokio::test]
    async fn a_reap_that_could_not_finish_is_retried_on_the_next_tick() {
        let h = Harness::new();
        h.activate(MODULE);
        h.manager.boot().await;
        h.host.fail_evict(MODULE);

        h.operator(MODULE, OperatorCommand::Evict(EvictMode::Force))
            .await;
        assert_eq!(h.instance(MODULE).phase, Phase::Reaped);
        assert!(
            h.storage.current_instance(MODULE).unwrap().is_some(),
            "the directory must survive a teardown that failed",
        );

        // Still failing: the tick retries, and still must not unlink.
        h.host.clear();
        h.manager.tick().await;
        assert!(h.host.evicted(MODULE), "the tick must retry the reap");
        assert!(h.storage.current_instance(MODULE).unwrap().is_some());

        // Teardown works now: the next tick finishes the job, with no restart.
        h.host.heal_evict(MODULE);
        h.manager.tick().await;
        assert!(
            h.storage.current_instance(MODULE).unwrap().is_none(),
            "the retried reap must remove the module",
        );
        // …and a tick over a module that is simply gone is quiet.
        h.manager.tick().await;
    }

    /// The watchdog and the admin read surfaces do NOT hold the module lock,
    /// so they must never WRITE the record. Every other write is a
    /// read-modify-write under that lock; an unlocked adoption landing after a
    /// locked `apply` would overwrite the transition it had just persisted —
    /// silently discarding, say, an operator's stop.
    #[tokio::test]
    async fn reading_the_lifecycle_never_writes_it() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);

        // No boot, no apply: the instance has no record yet.
        assert!(h.storage.read_lifecycle(MODULE).unwrap().is_none());

        // Both read surfaces answer — with what adoption WOULD conclude…
        assert_eq!(
            h.manager.expects_follower(MODULE).unwrap(),
            BootAction::StartFollower,
        );
        let peeked = h.instance(MODULE);
        assert_eq!(peeked.desired, DesiredState::Running);
        assert_eq!(peeked.attached.len(), 1);

        // …and neither wrote it down.
        assert!(
            h.storage.read_lifecycle(MODULE).unwrap().is_none(),
            "an unlocked read persisted the adoption",
        );

        // The first LOCKED caller does, and concludes the same thing.
        h.manager.reconcile(MODULE).await.unwrap();
        let record = h.storage.read_lifecycle(MODULE).unwrap().expect("adopted");
        assert_eq!(record.desired, peeked.desired);
        assert_eq!(record.attached, peeked.attached);
    }

    #[tokio::test]
    async fn the_record_survives_an_upload() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.operator(MODULE, OperatorCommand::Stop).await;

        // A new sha is a new `InstanceKey`, so `activate` RENAMES the
        // instance directory — and `lifecycle.toml` moves with it. That is
        // why the record carries no key of its own.
        let wasm = b"a second build entirely";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        h.manager.upload(&manifest, wasm, None).await.unwrap();

        let instance = h.instance(MODULE);
        assert!(matches!(
            instance.phase,
            Phase::Stopped {
                cause: StopCause::Operator,
                ..
            }
        ));
        assert_eq!(
            instance.key.wasm.as_str(),
            manifest.module.sha256,
            "load_or_adopt must rebuild the instance with the NEW key",
        );
    }

    #[tokio::test]
    async fn one_bad_module_does_not_stop_boot() {
        let h = Harness::new();
        h.activate(MODULE);
        h.activate(OTHER);
        // A directory with a pointer naming an instance that is not there.
        // `auto_resume` aborted the whole walk on a failure like this, which
        // is how one unreadable directory left every other module dead.
        std::fs::remove_dir_all(h.storage.root().join(MODULE).join("instances")).unwrap();

        h.manager.boot().await;

        assert!(!h.host.started(MODULE));
        assert!(h.host.started(OTHER), "one bad module stopped the others");
    }

    #[tokio::test]
    async fn evict_refuses_while_attached_and_force_overrides() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;

        let refused = h
            .manager
            .apply(
                MODULE,
                Event::Operator(OperatorCommand::Evict(EvictMode::RefuseIfAttached)),
            )
            .await;
        assert!(
            matches!(
                refused,
                Err(LifecycleError::Rejected(Rejected::StillAttached(1)))
            ),
            "{refused:?}",
        );
        assert!(!h.host.evicted(MODULE));

        h.operator(MODULE, OperatorCommand::Evict(EvictMode::Force))
            .await;
        assert!(h.host.evicted(MODULE));
    }

    // -----------------------------------------------------------------
    // Two instances: a second one is a full citizen.
    // -----------------------------------------------------------------

    /// The record is per INSTANCE, so an operator command on one is invisible
    /// to the other — record, follower and all.
    #[tokio::test]
    async fn each_instance_has_its_own_record() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let second = h.second(MODULE, b"a second build");
        let first = h.current(MODULE);
        h.manager.boot().await;
        assert_eq!(h.host.running_instances(MODULE).await.len(), 2);

        h.manager
            .apply_at(&second, Event::Operator(OperatorCommand::Stop))
            .await
            .expect("stop accepted");

        let stopped = h.record_at(&second).expect("second has a record");
        assert!(matches!(
            stopped.phase,
            Phase::Stopped {
                cause: StopCause::Operator,
                ..
            }
        ));
        assert_eq!(stopped.desired, DesiredState::Stopped);

        let untouched = h.record_at(&first).expect("first has a record");
        assert_eq!(untouched.phase, Phase::Active);
        assert_eq!(untouched.desired, DesiredState::Running);
        assert_eq!(attached_labels(&untouched), vec!["prod/ownership.cnft.dev"]);
        assert_eq!(
            h.host.running_instances(MODULE).await,
            vec![first],
            "stopping one instance stopped the other's follower",
        );
    }

    /// The refcount twin of the fan-out: over a module's instances the
    /// attached sets PARTITION its clients. A client in two keeps a retired
    /// instance alive for ever; a client in none lets the instance serving it
    /// be ejected as abandoned underneath it.
    #[tokio::test]
    async fn a_client_is_attached_to_exactly_one_instance() {
        // Legacy mode: one implicit namespace owning every client, so the
        // test can name three of them without touching the shared registry
        // fixture.
        let h = Harness::with_auth(Arc::new(legacy_authenticator()));
        h.activate(MODULE);
        let first = h.current(MODULE);
        let second = h.second(MODULE, b"a second build");

        for client in ["no-row.example", "on-a.example", "on-b.example"] {
            write_registration(&h.storage, MODULE, client, "policy_a");
        }
        h.place(MODULE, "on-a.example", &first);
        h.place(MODULE, "on-b.example", &second);

        h.manager.reconcile(MODULE).await.expect("reconcile");

        assert_eq!(
            attached_labels(&h.record_at(&first).expect("first")),
            vec!["prod/no-row.example", "prod/on-a.example"],
            "a client with no row follows the module's CURRENT instance",
        );
        assert_eq!(
            attached_labels(&h.record_at(&second).expect("second")),
            vec!["prod/on-b.example"],
        );
    }

    /// One registration change can alter TWO instances' sets, so `reconcile`
    /// visits every instance rather than the current one.
    #[tokio::test]
    async fn a_registration_change_reconciles_every_instance() {
        let h = Harness::with_auth(Arc::new(legacy_authenticator()));
        h.activate(MODULE);
        let first = h.current(MODULE);
        let second = h.second(MODULE, b"a second build");
        write_registration(&h.storage, MODULE, "mover.example", "policy_a");
        h.manager.boot().await;
        assert_eq!(
            attached_labels(&h.record_at(&first).expect("first")),
            vec!["prod/mover.example"],
        );

        // The row moves the client to the second instance. One write; both
        // records must change.
        h.place(MODULE, "mover.example", &second);
        h.manager.reconcile(MODULE).await.expect("reconcile");

        assert!(
            h.record_at(&first).expect("first").attached.is_empty(),
            "the instance the client left still holds a reference for it",
        );
        assert_eq!(
            attached_labels(&h.record_at(&second).expect("second")),
            vec!["prod/mover.example"],
        );
    }

    /// Every operator upload does this: `activate` RENAMES the live instance
    /// directory, so a row written before it names a key that is now nowhere.
    /// That client has not been moved by anything — it follows the module,
    /// and the instance serving it must keep its reference.
    #[tokio::test]
    async fn a_row_naming_a_renamed_away_instance_stays_attached_to_the_current_one() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let before = h.current(MODULE);
        h.place(MODULE, PROD_CLIENT, &before);
        h.manager.boot().await;

        let wasm = b"a whole new build";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        h.manager
            .upload(&manifest, wasm, None)
            .await
            .expect("upload");
        let after = h.current(MODULE);
        assert_ne!(after, before);
        assert!(!h.storage.instance_dir(&before).is_dir());

        h.manager.reconcile(MODULE).await.expect("reconcile");

        let instance = h.record_at(&after).expect("current has a record");
        assert_eq!(
            attached_labels(&instance),
            vec!["prod/ownership.cnft.dev"],
            "a stale row detached the only client from the instance serving it",
        );
        assert_eq!(
            instance.phase,
            Phase::Active,
            "the instance was drained as abandoned by a row a deploy invalidated",
        );
    }

    /// A record-less sibling is a directory nothing here made. Adopting it
    /// would conclude `Active` + `Pinned` and it would follow the chain for
    /// ever, unasked.
    #[tokio::test]
    async fn an_unmanaged_instance_is_not_adopted_started_or_reaped() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let orphan = h.plant(MODULE, b"a directory nobody claims");

        h.manager.boot().await;
        for _ in 0..365 {
            h.clock.advance(24 * 3600);
            h.manager.tick().await;
        }

        assert!(
            h.record_at(&orphan).is_none(),
            "an unmanaged instance was adopted",
        );
        assert!(
            h.storage.instance_dir(&orphan).is_dir(),
            "an unmanaged instance was reaped",
        );
        assert_eq!(
            h.host.running_instances(MODULE).await,
            vec![h.current(MODULE)],
            "an unmanaged instance was started",
        );
        // …and it is VISIBLE, which is the whole of what the host does about
        // it: the admin route lists it and boot warns once.
        let views = h.manager.instances(MODULE).expect("list");
        assert_eq!(views.len(), 2);
        assert!(
            views
                .iter()
                .any(|v| v.key() == &orphan && v.managed().is_none()),
        );
    }

    #[tokio::test]
    async fn boot_starts_each_instance_its_record_expects() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let running = h.second(MODULE, b"a second build");
        let stopped = h.plant(MODULE, b"a third build");
        // Stopped by an operator: the one thing a restart must never undo.
        h.govern(
            &Instance::adopt(stopped.clone(), &BTreeMap::new())
                .step(
                    Event::Operator(OperatorCommand::Stop),
                    mitos_lifecycle::UnixSecs(T0),
                )
                .expect("stop")
                .next,
        );

        h.manager.boot().await;

        let mut live = h.host.running_instances(MODULE).await;
        live.sort_by_key(|key| key.dir_name());
        let mut expected = vec![h.current(MODULE), running];
        expected.sort_by_key(|key| key.dir_name());
        assert_eq!(live, expected, "boot started an operator-stopped instance");
    }

    #[tokio::test]
    async fn one_bad_instance_does_not_stop_boot() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let bad = h.plant(MODULE, b"a second build");
        std::fs::write(
            h.storage.instance_dir(&bad).join("lifecycle.toml"),
            "this is not = = toml",
        )
        .unwrap();

        h.manager.boot().await;

        assert_eq!(
            h.host.running_instances(MODULE).await,
            vec![h.current(MODULE)],
            "one unreadable instance stopped the others",
        );
    }

    #[tokio::test]
    async fn the_watchdog_revives_a_dead_second_instance_and_leaves_a_stopped_one() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let second = h.second(MODULE, b"a second build");
        let third = h.second(MODULE, b"a third build");
        h.manager.boot().await;
        assert_eq!(h.host.running_instances(MODULE).await.len(), 3);

        // One an operator stopped, one whose follower simply exited. The
        // module is still "running" as `list_running` sees it — which is
        // exactly why the watchdog has to ask per instance.
        h.manager
            .apply_at(&third, Event::Operator(OperatorCommand::Stop))
            .await
            .expect("stop");
        h.host.instance_died(&second);
        h.host.clear();

        h.sweep().await;

        assert_eq!(
            h.host.calls(),
            vec![crate::host_v2::testing::HostCall::StartInstance(
                second.clone()
            )],
            "the watchdog revived the wrong set",
        );
    }

    #[tokio::test]
    async fn reaping_a_second_instance_leaves_the_module_and_its_sibling() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        let second = h.second(MODULE, b"a second build");
        let first = h.current(MODULE);
        h.manager.boot().await;

        h.manager
            .apply_at(
                &second,
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
            )
            .await
            .expect("evict accepted");

        assert!(
            !h.storage.instance_dir(&second).is_dir(),
            "the reaped instance's directory survived",
        );
        assert!(h.storage.instance_dir(&first).is_dir());
        assert!(h.record_at(&first).is_some());
        assert!(
            h.storage.module_dir_for_companions(MODULE).exists(),
            "reaping a sibling took the module's companions with it",
        );
        assert_eq!(h.host.running_instances(MODULE).await, vec![first]);
    }

    #[tokio::test]
    async fn reaping_the_only_instance_removes_the_module_as_evict_always_did() {
        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        let only = h.current(MODULE);

        h.manager
            .apply_at(
                &only,
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
            )
            .await
            .expect("evict accepted");

        assert!(
            !h.storage.root().join(MODULE).exists(),
            "the module directory survived the reap of its only instance",
        );
        assert!(h.storage.current_instance(MODULE).unwrap().is_none());
    }

    /// Removing the instance `current.toml` names, while the module has
    /// others, would leave every `&str`-addressed tool resolving to whichever
    /// sibling the crash repair happened to pick. Choosing a successor is the
    /// switch's job (WO-09).
    #[tokio::test]
    async fn reaping_the_current_instance_with_a_sibling_is_refused_and_retried() {
        let h = Harness::new();
        h.activate(MODULE);
        let sibling = h.second(MODULE, b"a second build");
        let current = h.current(MODULE);
        h.manager.boot().await;

        h.manager
            .apply_at(
                &current,
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
            )
            .await
            .expect("the machine accepts it; the HOST is what refuses");

        assert_eq!(
            h.record_at(&current).expect("record stands").phase,
            Phase::Reaped,
            "a failed effect must not roll the record back",
        );
        assert!(
            h.storage.instance_dir(&current).is_dir(),
            "the refusal removed the directory anyway",
        );
        assert_eq!(
            h.events
                .snapshot(0, None, Some("lifecycle_effect_failed"), 16)
                .len(),
            1,
            "a refused reap must be queryable, not only in the journal",
        );

        // Still refused on the next tick — and retried, which is what
        // `pending_cleanup` is for.
        h.host.clear();
        h.manager.tick().await;
        assert!(h.host.evicted(MODULE), "the tick must retry the reap");
        assert!(h.storage.instance_dir(&current).is_dir());

        // Retire the sibling and the retry takes: the current instance is now
        // the only one, so reaping it is ordinary evict.
        h.manager
            .apply_at(
                &sibling,
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
            )
            .await
            .expect("evict the sibling");
        h.manager.tick().await;
        assert!(
            !h.storage.root().join(MODULE).exists(),
            "the retried reap never finished",
        );
    }

    /// The manager always knows the key it means. Mixing the two addressings
    /// is how `facade_slots` came to exist, and after an upload a `replace(id)`
    /// would resolve `current.toml` a second time — naming a different
    /// instance than the one just stepped.
    #[tokio::test]
    async fn an_upload_never_uses_the_str_host_api() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        h.activate(MODULE);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;

        let wasm = b"a whole new build";
        let manifest = crate::companions::testing::manifest_for(MODULE, wasm);
        h.manager
            .upload(&manifest, wasm, None)
            .await
            .expect("upload");
        h.operator(MODULE, OperatorCommand::Stop).await;
        h.operator(MODULE, OperatorCommand::Start).await;
        h.operator(MODULE, OperatorCommand::Evict(EvictMode::Force))
            .await;

        let str_addressed: Vec<HostCall> = h
            .host
            .calls()
            .into_iter()
            .filter(|call| {
                matches!(
                    call,
                    HostCall::Replace(_) | HostCall::Stop(_) | HostCall::Evict(_)
                )
            })
            .collect();
        assert!(
            str_addressed.is_empty(),
            "the manager reached for the `&str` host API: {str_addressed:?}",
        );
    }

    // -----------------------------------------------------------------
    // Creating a staging instance.
    // -----------------------------------------------------------------

    use crate::storage::StateFile;
    use mitos_lifecycle::upgrade::UpgradeAction;

    /// The same schema either side: the parent's files are valid as they
    /// stand, so `kv`, `cursor` and `watched` all travel — and `emissions`
    /// never does. Every row in it is addressed to a client the PARENT is
    /// still serving.
    #[tokio::test]
    async fn a_compatible_schema_copies_kv_cursor_and_watched_but_never_emissions() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 4242);
        // An emissions log the parent owes its client.
        h.storage.emissions_store_at(&parent).unwrap();
        assert!(h.has(&parent, StateFile::Emissions));

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert_eq!(h.kv_get(&target).as_deref(), Some("from the parent"));
        assert_eq!(
            h.storage.read_cursor_at(&target).unwrap().map(|c| c.slot()),
            Some(4242),
            "state without its cursor replays blocks the state has already absorbed",
        );
        assert!(h.storage.watched_index_at(&target).contains(&watched_ref()));
        assert!(
            !h.has(&target, StateFile::Emissions),
            "the emissions log was copied; every row in it belongs to the parent's client",
        );
    }

    /// `reset-watched` is the one action that drops a file on its own.
    #[tokio::test]
    async fn reset_watched_drops_only_the_watched_index() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);

        let target = h.artifact(
            MODULE,
            b"the announced build",
            2,
            7,
            &[(2, UpgradeAction::ResetWatched)],
        );
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert_eq!(h.kv_get(&target).as_deref(), Some("from the parent"));
        assert_eq!(
            h.storage.read_cursor_at(&target).unwrap().map(|c| c.slot()),
            Some(99),
        );
        assert!(
            !h.has(&target, StateFile::Watched),
            "`reset-watched` must drop the index and nothing else",
        );
    }

    /// A schema bump with no `migrate` export is a REBUILD, and a rebuild
    /// copies nothing — so there is no reason to stop the parent at all.
    #[tokio::test]
    async fn a_schema_bump_rebuilds_from_empty_and_never_stops_the_parent() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);
        h.host.clear();

        let target = h.artifact(MODULE, b"the announced build", 2, 8, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert_eq!(h.kv_get(&target).as_deref(), None);
        assert!(!h.has(&target, StateFile::Cursor));
        assert!(!h.has(&target, StateFile::Watched));
        assert_eq!(
            h.host.calls(),
            vec![crate::host_v2::testing::HostCall::StartInstance(
                target.clone()
            )],
            "a rebuild copies nothing, so it has no reason to pause a live follower",
        );
    }

    /// A REBUILT instance has an empty KV, and the module's scan-interest
    /// lives in that KV (`update-interest` persists it there). Without the
    /// re-assert it would bootstrap to tip having scanned for nothing.
    #[tokio::test]
    async fn a_rebuilt_instance_has_the_modules_scan_interest_reasserted() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        // A registration with a real interest: `reconcile_module_interest`
        // routes the UNION of what the companions declare, and an empty
        // union is nothing to route.
        write_registration_with_interest(
            &h.storage,
            MODULE,
            PROD_CLIENT,
            "policy_a",
            vec![mitos_protocol::Interest::any()],
        );
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);

        // A dialer with a recording interest router, so "the scan-interest
        // was re-asserted" is a fact and not a hope.
        let router = Arc::new(crate::host_v2::testing::RecordingRouter::default());
        h.manager.set_dialer(crate::dialer::CompanionDialer::new(
            h.storage.clone(),
            crate::admin::AuthToken(None),
            Some(router.clone()),
        ));

        let target = h.artifact(MODULE, b"the announced build", 2, 8, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert!(
            router.routed().iter().any(|m| m == MODULE),
            "starting a staging instance must re-assert the module's scan-interest",
        );
    }

    /// The parent is a LIVE client's instance. Stopping it is how the copy
    /// gets a consistent `kv` + `cursor` + `watched`; leaving it stopped
    /// because somebody else's move failed would be the worst outcome here.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_parent_is_stopped_for_the_copy_and_always_restarted() {
        use crate::host_v2::testing::HostCall;
        use std::os::unix::fs::PermissionsExt;

        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);
        let record = h.storage.instance_dir(&parent).join("lifecycle.toml");
        let record_before = std::fs::read(&record).unwrap();
        h.host.clear();

        // The copy fails HALF-WAY: `kv` is the first file carried and it
        // cannot be read. Injected on the source rather than the
        // destination, because a read-only instances directory would fail
        // `begin_instance` — before the parent is ever stopped, which is the
        // one path this test is not about.
        let source = h
            .storage
            .instance_dir(&parent)
            .join(StateFile::Kv.file_name());
        let readable = std::fs::metadata(&source).unwrap().permissions();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o000)).unwrap();

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        let failed = h.manager.create_staging(&target, Some(&parent)).await;

        std::fs::set_permissions(&source, readable).unwrap();

        assert!(failed.is_err(), "the copy was supposed to fail");
        assert_eq!(
            h.host.calls(),
            vec![
                HostCall::StopInstance(parent.clone()),
                HostCall::StartInstance(parent.clone())
            ],
            "the parent must be put back on EVERY path out of the copy",
        );
        assert_eq!(
            std::fs::read(&record).unwrap(),
            record_before,
            "the parent's record was touched; nobody decided it should stop",
        );
        assert!(
            !h.storage.instance_dir(&target).is_dir(),
            "a failed copy left an instance behind",
        );
    }

    /// Every `*.partial` under the module's instances directory.
    fn partials(h: &Harness, any_instance: &InstanceKey) -> Vec<String> {
        let root = h.storage.instance_dir(any_instance);
        std::fs::read_dir(root.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".partial"))
            .collect()
    }

    /// `upload` can carry on past a failed quiesce: a rename moves the files
    /// whole. A COPY cannot — bytes read from redb files under a live writer
    /// are a torn `kv` beside a `cursor` from another block, and that does
    /// not fail here but weeks later, as state that is quietly wrong.
    #[tokio::test]
    async fn a_parent_that_will_not_stop_is_never_copied() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);
        h.host.clear();
        h.host.fail_stop_instance(&parent);

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        let refused = h.manager.create_staging(&target, Some(&parent)).await;

        assert!(
            matches!(refused, Err(StagingError::ParentWouldNotStop(_))),
            "{refused:?}"
        );
        assert!(!h.storage.instance_dir(&target).is_dir());
        assert!(
            partials(&h, &parent).is_empty(),
            "{:?}",
            partials(&h, &parent)
        );
        // Nothing was stopped, so nothing is restarted: a restart here would
        // cycle a follower that never went away.
        assert_eq!(h.host.calls(), vec![HostCall::StopInstance(parent)]);
    }

    /// The host is still up after a build fails, so the half-built directory
    /// — up to a parent's worth of disk — must not wait for boot's sweep.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_build_that_fails_leaves_no_partial_behind() {
        use std::os::unix::fs::PermissionsExt;

        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);
        // `cursor` is carried AFTER `kv`, so the partial already holds a file
        // when this fails.
        let source = h
            .storage
            .instance_dir(&parent)
            .join(StateFile::Cursor.file_name());
        let readable = std::fs::metadata(&source).unwrap().permissions();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o000)).unwrap();

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        let failed = h.manager.create_staging(&target, Some(&parent)).await;
        std::fs::set_permissions(&source, readable).unwrap();

        assert!(failed.is_err(), "the copy was supposed to fail");
        assert!(
            partials(&h, &parent).is_empty(),
            "{:?}",
            partials(&h, &parent)
        );
    }

    /// The copy must be taken AFTER the parent's follower has finished its
    /// block, or `kv`, `cursor` and `watched` describe three different ones.
    #[tokio::test]
    async fn the_copy_is_taken_with_the_parent_stopped() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "before the stop", 99);

        // What a follower writes as it finishes the block in flight. If the
        // copy were taken first, this would not be in it.
        let (storage, key) = (h.storage.clone(), parent.clone());
        h.host.on_stop(move || {
            storage
                .kv_store_at(&key, None)
                .expect("kv")
                .set_value(key.module.as_str(), "marker", b"the last block".to_vec())
                .expect("set marker");
        });

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert_eq!(
            h.kv_get(&target).as_deref(),
            Some("the last block"),
            "the copy was taken before the parent had finished its block",
        );
    }

    /// D4's "B already exists": a second client announces the build a first
    /// one is moving to. Its cursor is the whole point — it has been
    /// following, and rebuilding it would throw that away.
    #[tokio::test]
    async fn an_existing_staging_instance_is_found_not_rebuilt() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 10);

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("first move");
        // It has been following since.
        h.storage
            .write_cursor_at(&target, &mitos_data_plane::ChainPoint::Slot(500))
            .unwrap();
        h.host.clear();

        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("second move");

        assert_eq!(
            h.storage.read_cursor_at(&target).unwrap().map(|c| c.slot()),
            Some(500),
            "an existing staging instance was rebuilt, losing everything it had followed",
        );
        assert!(
            !h.host.stopped(MODULE),
            "finding an instance must not pause the parent: {:?}",
            h.host.calls(),
        );
    }

    /// A file that is not there carries as ZERO BYTES, so a copy from a
    /// parent whose directory an upload renamed away would silently produce
    /// an EMPTY instance — a rebuild nobody planned. `ClientRow::rebase` is
    /// what stops it reaching here; this is the second lock on the door.
    #[tokio::test]
    async fn a_parent_that_is_not_on_disk_is_refused_rather_than_copied_as_nothing() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);

        // What every operator upload does to the instance a stale row names.
        let after = h.activate_at(MODULE, b"a whole new build", 2, 7);
        assert_ne!(after, parent);
        assert!(!h.storage.instance_dir(&parent).is_dir());
        h.host.clear();

        let target = h.artifact(MODULE, b"the announced build", 3, 7, &[]);
        let refused = h
            .manager
            .create_staging(&target, Some(&parent))
            .await
            .expect_err("a parent that is gone must be refused");

        assert!(
            matches!(refused, StagingError::ParentGone(_)),
            "got {refused:?}",
        );
        assert!(
            !h.storage.instance_dir(&target).is_dir(),
            "an empty instance was built from a parent that is not there",
        );
        assert!(
            h.host.calls().is_empty(),
            "a refusal must not touch a follower: {:?}",
            h.host.calls(),
        );
    }

    #[tokio::test]
    async fn an_unmanaged_directory_is_refused() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        // A directory with no record: nothing here made it.
        h.storage.write_instance_marker(&target).unwrap();

        let refused = h
            .manager
            .create_staging(&target, Some(&parent))
            .await
            .expect_err("an unmanaged directory must be refused");
        assert!(
            matches!(refused, StagingError::Unmanaged(_)),
            "got {refused:?}",
        );
    }

    /// Crash window (a): the partial was being built and the parent is still
    /// stopped. Boot restarts the parent from its untouched record and
    /// deletes every `*.partial` it finds.
    ///
    /// (b) is the same code path — a COMPLETE partial is still a partial, and
    /// the only thing that makes one an instance is the rename.
    #[tokio::test]
    async fn a_crash_mid_build_leaves_a_partial_that_boot_sweeps() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);

        // (a) half-built, and (b) complete but unrenamed — one has the
        // marker, the other does not, and boot must take both.
        let half = h.storage.begin_instance(&target).unwrap();
        std::fs::write(half.path().join(StateFile::Kv.file_name()), b"half a file").unwrap();
        let half_path = half.path().to_owned();
        drop(half);

        let other = h.artifact(MODULE, b"a third build", 3, 7, &[]);
        let whole = h.storage.begin_instance(&other).unwrap();
        whole
            .write_lifecycle(&Instance::staging(other.clone()).record())
            .unwrap();
        let whole_path = whole.path().to_owned();
        drop(whole);

        // Nothing can SEE either of them.
        assert!(half_path.is_dir() && whole_path.is_dir());
        assert_eq!(
            h.storage
                .list_instances(&mitos_lifecycle::ids::ModuleId::parse(MODULE).unwrap())
                .unwrap(),
            vec![parent.clone()],
            "a partial was listed as an instance",
        );

        h.host.clear();
        h.restart().boot().await;

        assert!(!half_path.exists(), "boot left a half-built instance");
        assert!(!whole_path.exists(), "boot left an unrenamed instance");
        assert!(
            h.host.started(MODULE),
            "boot must restart the parent from its untouched record",
        );
    }

    /// Crash window (c): renamed, follower not started. `create_staging`
    /// step 1 finds it and starts it — that is the same path the move's
    /// `recover()` re-drives.
    #[tokio::test]
    async fn a_crash_after_the_rename_starts_the_instance_it_finds() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);

        // Exactly what a crash between the rename and the start leaves.
        let partial = h.storage.begin_instance(&target).unwrap();
        partial
            .write_lifecycle(&Instance::staging(target.clone()).record())
            .unwrap();
        partial.commit().unwrap();
        h.host.clear();

        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("the instance is found and started");

        assert_eq!(
            h.host.calls(),
            vec![crate::host_v2::testing::HostCall::StartInstance(
                target.clone()
            )],
        );
    }

    /// Between the first file and the rename, NOTHING on the host may see a
    /// half-built instance — not the listing, not `boot`, not the drain's
    /// store reconcile (which is `list_instances` too).
    #[tokio::test]
    async fn nothing_sees_a_half_built_instance() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        let module = mitos_lifecycle::ids::ModuleId::parse(MODULE).unwrap();

        let partial = h.storage.begin_instance(&target).unwrap();
        for step in ["kv", "lifecycle"] {
            match step {
                "kv" => {
                    std::fs::write(partial.path().join(StateFile::Kv.file_name()), b"x").unwrap()
                }
                _ => partial
                    .write_lifecycle(&Instance::staging(target.clone()).record())
                    .unwrap(),
            }
            assert_eq!(
                h.storage.list_instances(&module).unwrap(),
                vec![parent.clone()],
                "a partial was listed after writing {step}",
            );
            assert_eq!(
                h.manager.instances(MODULE).unwrap().len(),
                1,
                "the lifecycle saw a partial after writing {step}",
            );
            assert!(!h.storage.instance_dir(&target).is_dir());
        }

        partial.commit().unwrap();
        assert_eq!(h.storage.list_instances(&module).unwrap().len(), 2);
    }

    /// The same shape as WO-07's upload test, one order along: the watchdog
    /// works from an unlocked snapshot, and a copy holds the parent stopped.
    /// A revive landing inside it would start the parent under the copy — and
    /// the copy would then be of a file with a live writer.
    #[tokio::test]
    async fn a_watchdog_revive_cannot_land_inside_a_copy() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        h.fill_state(&parent, "from the parent", 99);

        // The gap, held open: the parent is stopped and the copy is running.
        let (in_gap, mut wait_for_gap) = tokio::sync::mpsc::unbounded_channel();
        h.host.on_stop(move || {
            let _ = in_gap.send(());
            std::thread::sleep(std::time::Duration::from_millis(300));
        });

        // What the watchdog does once its snapshot says "the parent should be
        // running and is not" — which, inside the gap, is exactly what it
        // says. On its OWN task: `revive` waits for the module lock, and
        // `create_staging` holds it for the whole copy.
        let (manager, key) = (h.manager.clone(), parent.clone());
        let watchdog = tokio::spawn(async move {
            wait_for_gap.recv().await.expect("the copy reached its gap");
            manager.revive(&key).await
        });

        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .expect("staging created");

        assert_eq!(
            watchdog.await.unwrap(),
            Revival::NotNeeded,
            "a revive acted inside the copy",
        );
        assert!(
            h.host.running_instances(MODULE).await.contains(&parent),
            "the parent must be running after the copy",
        );
    }

    // -----------------------------------------------------------------
    // Releasing a staging instance.
    // -----------------------------------------------------------------

    /// Two clients can announce the same build, and both move to the one
    /// instance. One giving up must not take it from the other.
    #[tokio::test]
    async fn a_staging_instance_two_clients_are_moving_to_survives_one_giving_up() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .unwrap();

        // The second client is still moving to it.
        h.moving(PROD_CLIENT, &parent, &target);

        h.manager.release_staging(&target).await.unwrap();

        assert!(
            h.storage.instance_dir(&target).is_dir(),
            "an instance another client is still catching up on was reaped",
        );
        assert_eq!(h.record_at(&target).map(|i| i.phase), Some(Phase::Staging),);
    }

    #[tokio::test]
    async fn a_staging_instance_nobody_wants_is_stopped_and_reaped() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .unwrap();
        h.host.clear();

        h.manager.release_staging(&target).await.unwrap();

        assert!(
            !h.storage.instance_dir(&target).is_dir(),
            "a staging instance nobody wants must be reaped",
        );
        assert!(h.storage.instance_dir(&parent).is_dir());
        assert!(h.host.stopped(MODULE) && h.host.evicted(MODULE));
    }

    /// `ReleaseStaging` is issued ONCE, by a transition that leaves its row
    /// with no pending move — so if the host dies between persisting that row
    /// and executing the effect, `recover()` has nothing to re-issue. Without
    /// a sweep the staging instance follows the chain for ever, for nobody.
    #[tokio::test]
    async fn the_tick_releases_a_staging_instance_whose_release_was_lost() {
        let h = Harness::new();
        let parent = h.activate_at(MODULE, b"the live build", 1, 7);
        h.subscribe(MODULE, PROD_CLIENT);
        h.manager.boot().await;
        let target = h.artifact(MODULE, b"the announced build", 2, 7, &[]);
        h.manager
            .create_staging(&target, Some(&parent))
            .await
            .unwrap();

        // While a move still wants it, a tick leaves it alone…
        h.moving(PROD_CLIENT, &parent, &target);
        h.manager.tick().await;
        assert_eq!(h.record_at(&target).map(|i| i.phase), Some(Phase::Staging));

        // …the move is abandoned, and the effect that should have followed
        // never runs: the crash.
        h.storage
            .remove_row(MODULE, &ClientId::parse(PROD_CLIENT).unwrap())
            .unwrap();

        h.manager.tick().await;

        assert!(
            !h.storage.instance_dir(&target).is_dir(),
            "an unwanted staging instance outlived the tick",
        );
        assert!(h.storage.instance_dir(&parent).is_dir());
        assert_eq!(
            h.record_at(&parent).map(|i| i.phase),
            Some(Phase::Active),
            "the sweep must never touch an instance that is not staging",
        );
    }

    /// Legacy mode has one implicit namespace owning every client, so a
    /// host with no namespace config adopts and governs exactly as before.
    #[tokio::test]
    async fn legacy_mode_adopts_its_clients_too() {
        let h = Harness::with_auth(Arc::new(legacy_authenticator()));
        h.activate(MODULE);
        write_registration(&h.storage, MODULE, "anything-at-all", "policy_a");
        h.manager.boot().await;

        let instance = h.instance(MODULE);
        assert_eq!(instance.desired, DesiredState::Running);
        assert_eq!(attached_labels(&instance), vec!["prod/anything-at-all"]);
    }
}
