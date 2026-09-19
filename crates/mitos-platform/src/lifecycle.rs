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

use crate::auth::Authenticator;
use crate::dialer::CompanionDialer;
use crate::events::{EventKind, EventRing};
use crate::host_v2::ModuleHostHandle;
use crate::manifest::Manifest;
use crate::storage::{ModuleStorage, StorageError};

#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    /// The module has no `current.toml`, so there is no instance to govern.
    #[error("module {0} is not registered")]
    NotRegistered(String),
    /// The machine refused the event. The admin layer maps this to 409 — it
    /// is a legitimate state, not a malformed request.
    #[error("{0}")]
    Rejected(#[from] Rejected),
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
    /// zombie follower behind per deploy.
    ///
    /// **Caller must hold this module's lock.** Adoption WRITES, and every
    /// other write to the record is a read-modify-write under that lock. An
    /// unlocked adoption landing after a locked `apply` would overwrite the
    /// transition it had just persisted — silently discarding, say, an
    /// operator's stop. Callers without the lock use [`Self::peek`].
    fn load_or_adopt(&self, module: &str) -> Result<Instance, LifecycleError> {
        let key = self.current_key(module)?;
        match self.storage.read_lifecycle(module)? {
            Some(record) => Ok(Instance::from_record(key, record)),
            None => {
                let instance = self.adopted(module, key);
                self.storage.write_lifecycle(module, &instance.record())?;
                tracing::info!(
                    module = %module,
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
        }
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
    fn peek(&self, module: &str) -> Result<Instance, LifecycleError> {
        let key = self.current_key(module)?;
        Ok(match self.storage.read_lifecycle(module)? {
            Some(record) => Instance::from_record(key, record),
            None => self.adopted(module, key),
        })
    }

    /// Whether this module's follower SHOULD be running. The watchdog's whole
    /// decision. Reads only — the watchdog does not hold the module lock.
    pub fn expects_follower(&self, module: &str) -> Result<BootAction, LifecycleError> {
        Ok(self.peek(module)?.on_boot())
    }

    /// The persisted record, for the admin API's read surfaces. `Ok(None)`
    /// when the module has no instance at all.
    pub fn instance(&self, module: &str) -> Result<Option<Instance>, LifecycleError> {
        match self.peek(module) {
            Ok(instance) => Ok(Some(instance)),
            Err(LifecycleError::NotRegistered(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // -----------------------------------------------------------------
    // Stepping
    // -----------------------------------------------------------------

    /// Feed one event: load → step → PERSIST → execute.
    pub async fn apply(&self, module: &str, event: Event) -> Result<Vec<Effect>, LifecycleError> {
        let lock = self.lock_for(module);
        let _guard = lock.lock().await;
        self.apply_locked(module, event).await
    }

    async fn apply_locked(
        &self,
        module: &str,
        event: Event,
    ) -> Result<Vec<Effect>, LifecycleError> {
        let instance = self.load_or_adopt(module)?;
        let transition = instance.step(event, self.now())?;
        // The record goes to disk BEFORE any effect runs. A crash in between
        // leaves a record describing a world that has not happened yet, which
        // is exactly what `recover` re-issues on the next boot.
        self.storage
            .write_lifecycle(module, &transition.next.record())?;
        self.execute_all(module, &transition.effects).await;
        Ok(transition.effects)
    }

    /// Bring `attached` into line with the registrations on disk.
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
        let on_disk = self.on_disk_clients(module);
        for event in self.load_or_adopt(module)?.reconcile(&on_disk) {
            // Each event re-loads, so a rejection on one (a client that raced
            // away between the diff and the step) does not abandon the rest.
            match self.apply_locked(module, event.clone()).await {
                Ok(_) => {}
                Err(LifecycleError::Rejected(r)) => {
                    tracing::debug!(module = %module, ?event, rejected = %r, "reconcile: stale event");
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------
    // Effects
    // -----------------------------------------------------------------

    async fn execute_all(&self, module: &str, effects: &[Effect]) {
        for effect in effects {
            self.execute(module, *effect).await;
        }
    }

    /// Every effect is idempotent (see the lifecycle crate's docs), so a
    /// failure here is logged and left for the watchdog rather than retried
    /// in place or rolled back.
    async fn execute(&self, module: &str, effect: Effect) {
        match effect {
            Effect::StartFollower => match self.host.replace(module).await {
                Ok(()) => {
                    // Exactly what the watchdog does after a revive: a
                    // subscribe that landed while the module was down had its
                    // interest mutation dropped with `NotRunning`.
                    if let Some(dialer) = self.dialer.get() {
                        dialer.reconcile_module_interest(module).await;
                    }
                    tracing::info!(module = %module, "lifecycle: follower started");
                }
                Err(e) => self.effect_failed(module, "start_follower", &e.to_string()),
            },
            Effect::StopFollower => match self.host.stop(module).await {
                Ok(()) => tracing::info!(module = %module, "lifecycle: follower stopped"),
                Err(e) => self.effect_failed(module, "stop_follower", &e.to_string()),
            },
            Effect::Reap => self.reap(module).await,
            Effect::Alert(alert) => {
                tracing::error!(module = %module, ?alert, "lifecycle: instance alert");
                self.events
                    .record(module, EventKind::InstanceAlert { alert });
            }
        }
    }

    /// v2's evict handler body, moved rather than copied: stop the slot and
    /// drop the dialer's in-memory state, then remove the directory. Both
    /// halves tolerate "already gone".
    async fn reap(&self, module: &str) {
        match self.host.evict_module(module).await {
            Ok(cancelled) => {
                if !cancelled.is_empty() {
                    tracing::warn!(
                        module = %module,
                        companions = ?cancelled,
                        "lifecycle: reap cancelled live companion dial loops",
                    );
                }
            }
            Err(e) => {
                self.effect_failed(module, "reap", &e.to_string());
                // Do NOT remove the directory when the host-side teardown
                // failed: its redb handles are still open, and removing files
                // underneath them is how a follower ends up writing to an
                // unlinked inode.
                return;
            }
        }
        if let Err(e) = self.storage.remove_module_dir(module) {
            self.effect_failed(module, "reap", &e.to_string());
            return;
        }
        tracing::info!(module = %module, "lifecycle: instance reaped");
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
        let instance = self.load_or_adopt(module)?;
        // `recover` re-issues what a restart must re-do: follower liveness is
        // in memory, so nothing is running and `on_boot` says what should be.
        self.execute_all(module, &instance.recover()).await;
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
            match self.apply_locked(&module, Event::Tick).await {
                Ok(_) | Err(LifecycleError::Rejected(_)) => {}
                Err(e) => tracing::warn!(module = %module, error = %e, "lifecycle tick failed"),
            }
            // `Tick` on a reaped instance is REJECTED, so a `Reap` that could
            // not finish — `reap` refuses to remove a directory when the
            // host-side teardown failed — would otherwise wait for the next
            // host restart. `pending_cleanup` is the machine's list of what
            // is safe to re-issue while the host is up; it is empty for every
            // phase but `Reaped`.
            match self.load_or_adopt(&module) {
                Ok(instance) => self.execute_all(&module, &instance.pending_cleanup()).await,
                // The reap finished and took the module with it.
                Err(LifecycleError::NotRegistered(_)) => {}
                Err(e) => {
                    tracing::warn!(module = %module, error = %e, "lifecycle tick: reload failed")
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

        self.storage.activate(manifest, wasm)?;
        if let Some(config) = config {
            self.storage.write_config(&module, config)?;
        }

        let action = self.load_or_adopt(&module)?.on_boot();
        if action == BootAction::StartFollower {
            self.execute(&module, Effect::StartFollower).await;
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
            let host = FakeHost::new();
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
