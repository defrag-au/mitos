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
use mitos_lifecycle::ids::{ClientId, ClientKey, InstanceKey};
use mitos_lifecycle::instance::{BootAction, Effect, Event, Instance, Rejected};
use mitos_lifecycle::policy::NamespaceProfile;
use mitos_lifecycle::routing::{self, Subscriber};

use crate::auth::Authenticator;
use crate::dialer::CompanionDialer;
use crate::events::{EventKind, EventRing};
use crate::host_v2::ModuleHostHandle;
use crate::manifest::Manifest;
use crate::rows::instance_dir_of;
use crate::storage::{ModuleStorage, StorageError};

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
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(())
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
            Effect::StartFollower => match self.host.start_instance(key).await {
                Ok(()) => {
                    // Exactly what the watchdog does after a revive: a
                    // subscribe that landed while the module was down had its
                    // interest mutation dropped with `NotRunning`.
                    if let Some(dialer) = self.dialer.get() {
                        dialer.reconcile_module_interest(module).await;
                    }
                    tracing::info!(
                        module = %module,
                        instance = %key.dir_name(),
                        "lifecycle: follower started",
                    );
                }
                Err(e) => self.effect_failed(module, "start_follower", &e.to_string()),
            },
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
        let dir = storage.module_dir_for_companions(module).join(client);
        std::fs::create_dir_all(&dir).expect("companion dir");
        let request = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module {
                name: module.to_owned(),
            }],
            companion_key: key.to_owned(),
            client_id: client.to_owned(),
            resume_from: None,
            interests: vec![],
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
