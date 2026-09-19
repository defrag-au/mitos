//! A worker announces its build; the host records it and pulls it — D4's
//! first three states.
//!
//! `ClientRow::step` runs `Announce → Fetching → Queued`, and this stops
//! there. Nothing here grants a catch-up slot, creates an instance, or
//! switches a client — that is WO-06. So after this a worker that announces
//! keeps being served by exactly the instance it is on, and what changes is
//! observable only: the artifact is in the CAS and `GET …/rows` shows a move
//! parked at `queued`.
//!
//! # An announcement NEVER fails the subscribe
//!
//! A refused rollback, a fork past the cap, an unreachable artifact — the
//! companion is still registered and still served by the instance it is on.
//! The outcome goes in the response, on the event ring and on the row. A
//! worker whose build the host declines is a worker running slightly older
//! code, not a worker that has lost its subscription.
//!
//! # Persist, THEN execute
//!
//! Exactly as `LifecycleManager::apply` does: lock the row, load, `step`,
//! WRITE, then execute effects in order. And the README's rule applies here
//! directly — `load_or_adopt` writes, so only a holder of the row lock may
//! call it. The admin read route and the tick's scan READ rows.

use std::collections::HashMap;
use std::sync::Arc;

use mitos_lifecycle::access::Registry;
use mitos_lifecycle::client_move::{
    AnnounceContext, ClientRow, Effect, Event, ModuleRef, Placement, Progress, Rejected,
    TargetInstance,
};
use mitos_lifecycle::ids::{ClientId, ClientKey, ModuleId};
use mitos_lifecycle::instance::DetachCause;
use mitos_lifecycle::upgrade::MoveIntent;
use mitos_protocol::{AnnounceResult, AnnouncedModule};

use crate::auth::Authenticator;
use crate::events::{EventKind, EventRing};
use crate::fetch::{Announced, ArtifactPuller, Refusal};
use crate::lifecycle::LifecycleManager;
use crate::storage::{ModuleStorage, StorageError};

/// How many live instances of one module the host will carry.
///
/// A fork past this is REFUSED with a reason rather than silently costing
/// another follower's worth of dispatch. Four is two environments' worth of
/// divergence plus headroom; the cap exists so a misconfigured worker cannot
/// fork the box to a standstill.
pub const MAX_INSTANCES_PER_MODULE: usize = 4;

/// The inline config an announcement may carry. It is part of instance
/// identity and is small — the biggest in this repo is a few hundred bytes —
/// so it travels with the announcement instead of being fetched.
pub const MAX_INLINE_CONFIG_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum MoveError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
}

pub struct MoveManager {
    storage: ModuleStorage,
    puller: Arc<ArtifactPuller>,
    auth: Arc<Authenticator>,
    lifecycle: Arc<LifecycleManager>,
    events: EventRing,
    /// One lock per `(module, client)` — a row's read-modify-write.
    locks: std::sync::Mutex<HashMap<RowKey, Arc<tokio::sync::Mutex<()>>>>,
    /// A fetch outlives the call that started it (see [`Self::spawn_fetch`]),
    /// so the task needs an owning handle back to the manager.
    me: std::sync::Weak<Self>,
    /// Fetches still running, so [`Self::settle`] can wait for them.
    fetches: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// `(module, client)` — which row a lock guards.
type RowKey = (String, String);

impl MoveManager {
    pub fn new(
        storage: ModuleStorage,
        puller: Arc<ArtifactPuller>,
        auth: Arc<Authenticator>,
        lifecycle: Arc<LifecycleManager>,
        events: EventRing,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            storage,
            puller,
            auth,
            lifecycle,
            events,
            locks: std::sync::Mutex::new(HashMap::new()),
            me: me.clone(),
            fetches: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Wait for every fetch started so far. For tests, and for an orderly
    /// shutdown; nothing on a request path may call it.
    pub async fn settle(&self) {
        loop {
            let running: Vec<_> = std::mem::take(&mut *self.fetches.lock().expect("fetches mutex"));
            if running.is_empty() {
                return;
            }
            for fetch in running {
                let _ = fetch.await;
            }
        }
    }

    fn lock_for(&self, module: &str, client: &ClientId) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().expect("move locks mutex");
        locks
            .entry((module.to_owned(), client.as_str().to_owned()))
            .or_default()
            .clone()
    }

    fn registry(&self) -> &Registry {
        self.auth.registry()
    }

    // -----------------------------------------------------------------
    // Announce
    // -----------------------------------------------------------------

    /// Record what a client says it was built against, and start pulling it.
    ///
    /// Never returns an error the subscribe should act on: every refusal is
    /// an [`AnnounceResult::Refused`] carrying the reason.
    pub async fn announce(
        &self,
        client: &ClientId,
        module: &ModuleId,
        announced: &AnnouncedModule,
    ) -> AnnounceResult {
        let lock = self.lock_for(module.as_str(), client);
        let _guard = lock.lock().await;
        match self.announce_locked(client, module, announced).await {
            Ok(result) => result,
            Err(reason) => {
                tracing::warn!(
                    client = %client,
                    module = %module,
                    reason = %reason,
                    "announcement refused",
                );
                AnnounceResult::Refused { reason }
            }
        }
    }

    async fn announce_locked(
        &self,
        client: &ClientId,
        module: &ModuleId,
        announced: &AnnouncedModule,
    ) -> Result<AnnounceResult, String> {
        // 0. FIRST, because everything after it writes or fetches. The whole
        //    rule is `may_announce`: only a client an operator NAMED in a
        //    `clients = [...]` list. That refuses legacy and open mode
        //    without mentioning either — in both, a client id is whatever the
        //    caller typed, and the host would be fetching from a hostname of
        //    the caller's choosing.
        let namespace = self
            .registry()
            .may_announce(client)
            .map_err(|e| e.to_string())?;
        let profile = namespace.profile;
        let client_key = ClientKey {
            namespace: namespace.name.clone(),
            client: client.clone(),
        };

        // 1. The inline config becomes a digest — half of the instance key.
        let config = match &announced.config {
            Some(bytes) if bytes.len() > MAX_INLINE_CONFIG_BYTES => {
                return Err(format!(
                    "config is {} bytes; at most {MAX_INLINE_CONFIG_BYTES} may be announced inline",
                    bytes.len()
                ));
            }
            Some(bytes) => Some(self.storage.put_config(bytes).map_err(|e| e.to_string())?),
            None => None,
        };

        // 2. The host derives the URL from the client's own hostname.
        let module_ref = ModuleRef::announced_by(
            client,
            module,
            announced.wasm.clone(),
            announced.manifest.clone(),
            config,
            announced.revision,
        )
        .map_err(|e| e.to_string())?;

        // 3. Load the row, or adopt one. Under the lock — `load_or_adopt`
        //    WRITES.
        let row = self
            .load_or_adopt(module, &client_key)
            .map_err(|e| e.to_string())?;

        // 4. What the pure crate cannot know.
        let target = module_ref.placement(module).instance;
        let live = self
            .storage
            .list_instances(module)
            .map_err(|e| e.to_string())?;
        let context = AnnounceContext {
            revision_policy: profile.revision,
            target: if live.contains(&target) {
                TargetInstance::Exists
            } else {
                TargetInstance::Missing
            },
            live_instances: live.len(),
            max_instances: MAX_INSTANCES_PER_MODULE,
        };

        // 5. A refusal writes NOTHING, so a pending move survives it intact.
        let transition = row
            .step(Event::Announce {
                module_ref,
                intent: MoveIntent::Announced,
                context,
            })
            .map_err(|e: Rejected| e.to_string())?;

        let result = if transition.next.pending.is_some() {
            AnnounceResult::Accepted
        } else {
            AnnounceResult::AlreadyCurrent
        };
        self.storage
            .write_row(module.as_str(), &transition.next)
            .map_err(|e| e.to_string())?;
        self.execute_all(module, client, &transition.effects).await;
        Ok(result)
    }

    /// The row on disk, or one derived for a client that is already being
    /// served.
    ///
    /// **Writes. Only a holder of the row lock may call it** — see the
    /// README's rule; this is the third place it would have bitten.
    ///
    /// A client attached to the module's current instance is ADOPTED onto it
    /// rather than started at `current: None`. `ClientRow::adopt`'s doc
    /// comment has the reason: `None` means FIRST subscribe, which skips
    /// catch-up, the pause and the drain and simply attaches — so the
    /// client's first announcement would switch it with no hand-off at all,
    /// dropping whatever the old instance still owed it.
    fn load_or_adopt(
        &self,
        module: &ModuleId,
        client: &ClientKey,
    ) -> Result<ClientRow, StorageError> {
        if let Some(row) = self.storage.read_row(module.as_str(), &client.client)? {
            return Ok(row);
        }
        let row = match self.current_placement(module, client) {
            Some(placement) => {
                tracing::info!(
                    client = %client,
                    module = %module,
                    instance = %placement.instance.dir_name(),
                    "adopting a client row onto the instance already serving it",
                );
                ClientRow::adopt(client.clone(), module.clone(), placement)
            }
            None => ClientRow::new(client.clone(), module.clone()),
        };
        self.storage.write_row(module.as_str(), &row)?;
        Ok(row)
    }

    /// Where this client is being served today, if anywhere: the module's
    /// current instance, but only when the client is actually attached to it.
    fn current_placement(&self, module: &ModuleId, client: &ClientKey) -> Option<Placement> {
        let instance = self.lifecycle.instance(module.as_str()).ok()??;
        if !instance.attached.contains(client) {
            return None;
        }
        // The revision the instance is RUNNING, read from its own manifest.
        let revision = self
            .storage
            .read_artifact_manifest(&instance.key.wasm)
            .ok()
            .flatten()
            .map(|m| m.lifecycle.revision)
            .unwrap_or_default();
        Some(Placement {
            instance: instance.key,
            revision,
        })
    }

    // -----------------------------------------------------------------
    // Effects
    // -----------------------------------------------------------------

    /// **Runs under the row lock, so nothing here may wait on the world.**
    /// Every effect is either bookkeeping or starts work that reports back
    /// later as a `Progress`.
    ///
    /// Boxed because the cycle is real: a fetch's answer is fed back in as a
    /// `Progress`, which produces effects of its own.
    fn execute_all<'a>(
        &'a self,
        module: &'a ModuleId,
        client: &'a ClientId,
        effects: &'a [Effect],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            for effect in effects {
                self.execute(module, client, effect).await;
            }
        })
    }

    async fn execute(&self, module: &ModuleId, client: &ClientId, effect: &Effect) {
        match effect {
            // The effect carries BOTH pins, so one re-issued after a restart
            // is exactly as verifiable as the first.
            Effect::EnsureArtifact { wasm, manifest } => {
                self.spawn_fetch(module, client, wasm, manifest);
            }
            // A slot is never granted in this order, so the queue is a record
            // of who is waiting and nothing more. WO-06 drains it.
            Effect::RequestCatchUpSlot | Effect::ReleaseCatchUpSlot => {}
            Effect::Alert(alert) => {
                let reason = format!("{alert:?}");
                tracing::error!(module = %module, client = %client, %reason, "move failed");
                self.events.record(
                    module.as_str(),
                    EventKind::MoveFailed {
                        client_id: client.to_string(),
                        reason,
                    },
                );
            }
            // Only reachable from `Withdraw`. WO-03's `reconcile` ALREADY
            // detached the client from the instance when its registrations
            // vanished, so acting on this would be a second detach of the
            // same client. Who owns attach/detach once rows drive moves is
            // WO-06's question; it is not answered here.
            Effect::DetachFrom { .. } => {}
            // Everything else needs a granted slot, which cannot have
            // happened. Loud, but never fatal: a reducer that surprised the
            // host is not a reason to take the host down.
            other => tracing::error!(
                module = %module,
                client = %client,
                effect = ?other,
                "move effect is not reachable before a catch-up slot is granted; ignoring",
            ),
        }
    }

    /// Start the pull on its OWN task and return at once.
    ///
    /// The caller holds the row lock and is, as often as not, a subscribe
    /// handler with a Durable Object waiting on the other end. A download is
    /// up to 16 MiB from a worker that may be slow or gone; awaited here it
    /// would make "an announcement never fails the subscribe" false by
    /// timeout, hold the row lock for the duration, and make `boot` wait on
    /// every unreachable worker in turn before the host finished starting.
    fn spawn_fetch(
        &self,
        module: &ModuleId,
        client: &ClientId,
        wasm: &mitos_lifecycle::ids::Sha256Hex,
        manifest: &mitos_lifecycle::ids::Sha256Hex,
    ) {
        // Only `None` while the last `Arc` is being dropped — shutting down.
        let Some(me) = self.me.upgrade() else {
            return;
        };
        let (module, client) = (module.clone(), client.clone());
        let (wasm, manifest) = (wasm.clone(), manifest.clone());
        let fetch = tokio::spawn(async move {
            me.ensure_artifact(&module, &client, &wasm, &manifest).await;
        });
        let mut fetches = self.fetches.lock().expect("fetches mutex");
        fetches.retain(|running| !running.is_finished());
        fetches.push(fetch);
    }

    /// Pull the artifact, then feed the answer back as `Progress`.
    ///
    /// Holds NO lock while it pulls; takes the row lock only to report.
    async fn ensure_artifact(
        &self,
        module: &ModuleId,
        client: &ClientId,
        wasm: &mitos_lifecycle::ids::Sha256Hex,
        manifest: &mitos_lifecycle::ids::Sha256Hex,
    ) {
        // A plain READ of the row: by the time this runs a newer announcement
        // may have replaced the move, and then this fetch is stale.
        let Ok(Some(row)) = self.storage.read_row(module.as_str(), client) else {
            return;
        };
        let Some(pending) = row.pending.as_ref() else {
            return;
        };
        if &pending.target.instance.wasm != wasm || &pending.manifest != manifest {
            return;
        }
        let announced = Announced {
            module: module.clone(),
            wasm: wasm.clone(),
            manifest: manifest.clone(),
            // What the rollback check ran against; verification step 5 holds
            // the fetched manifest to it.
            revision: pending.target.revision,
        };
        let target = pending.target.instance.clone();

        let step = match self.puller.ensure(client, &announced).await {
            Ok(manifest) => Progress::ArtifactReady {
                upgrades: manifest.lifecycle.upgrade,
            },
            Err(refusal) => {
                tracing::warn!(
                    module = %module,
                    client = %client,
                    refusal = %refusal,
                    "announced artifact refused",
                );
                self.record_refusal(module, client, &refusal);
                Progress::Failed(mitos_lifecycle::client_move::MoveFailure::ArtifactRefused)
            }
        };

        // A newer announcement may have landed while the fetch was in
        // flight. `Rejected::Stale` is how the machine says so, and it is
        // dropped QUIETLY — a superseded fetch is not a failure.
        let lock = self.lock_for(module.as_str(), client);
        let _guard = lock.lock().await;
        if let Err(e) = self.progress_locked(module, client, target, step).await {
            tracing::debug!(module = %module, client = %client, rejected = %e, "stale fetch answer");
        }
    }

    fn record_refusal(&self, module: &ModuleId, client: &ClientId, refusal: &Refusal) {
        self.events.record(
            module.as_str(),
            EventKind::MoveFailed {
                client_id: client.to_string(),
                reason: refusal.to_string(),
            },
        );
    }

    /// Feed one `Progress`.
    ///
    /// **The caller holds the row lock** — `tokio::sync::Mutex` is not
    /// reentrant, so this must not take it. Today the one caller is
    /// `ensure_artifact`, which takes it only once its pull has finished.
    async fn progress_locked(
        &self,
        module: &ModuleId,
        client: &ClientId,
        target: mitos_lifecycle::ids::InstanceKey,
        step: Progress,
    ) -> Result<(), String> {
        let Some(row) = self
            .storage
            .read_row(module.as_str(), client)
            .map_err(|e| e.to_string())?
        else {
            return Err("row is gone".to_owned());
        };
        let transition = row
            .step(Event::Progress { target, step })
            .map_err(|e| e.to_string())?;
        self.storage
            .write_row(module.as_str(), &transition.next)
            .map_err(|e| e.to_string())?;
        self.execute_all(module, client, &transition.effects).await;
        Ok(())
    }

    // -----------------------------------------------------------------
    // Boot and tick
    // -----------------------------------------------------------------

    /// Re-issue what every row's state says is already in flight. A
    /// `Fetching` row re-fetches; everything else re-queues.
    pub async fn boot(&self) {
        for module in self.storage.list_modules().unwrap_or_default() {
            let Ok(rows) = self.storage.list_rows(&module) else {
                continue;
            };
            let Ok(module_id) = ModuleId::parse(&module) else {
                continue;
            };
            for row in rows {
                let client = row.client.client.clone();
                let lock = self.lock_for(&module, &client);
                let _guard = lock.lock().await;
                let transition = row.recover();
                if let Err(e) = self.storage.write_row(&module, &transition.next) {
                    tracing::error!(module = %module, client = %client, error = %e, "row recover failed");
                    continue;
                }
                // Starts fetches; does not wait for them. An unreachable
                // worker must not hold up the host's start.
                self.execute_all(&module_id, &client, &transition.effects)
                    .await;
            }
        }
    }

    /// A row whose client has no registration left for the module is
    /// withdrawn; a row with neither a current placement nor a pending move
    /// is then removed.
    pub async fn tick(&self) {
        for module in self.storage.list_modules().unwrap_or_default() {
            let Ok(module_id) = ModuleId::parse(&module) else {
                continue;
            };
            // Read-only: the scan must not adopt-and-write.
            let Ok(rows) = self.storage.list_rows(&module) else {
                continue;
            };
            let attached = self.lifecycle.on_disk_clients(&module);
            for row in rows {
                if attached.contains_key(&row.client) {
                    continue;
                }
                let client = row.client.client.clone();
                let lock = self.lock_for(&module, &client);
                let _guard = lock.lock().await;
                self.withdraw(&module_id, &client, row).await;
            }
        }
    }

    async fn withdraw(&self, module: &ModuleId, client: &ClientId, row: ClientRow) {
        let next = match row.step(Event::Withdraw {
            cause: DetachCause::Unsubscribed,
        }) {
            Ok(transition) => {
                if let Err(e) = self.storage.write_row(module.as_str(), &transition.next) {
                    tracing::error!(module = %module, client = %client, error = %e, "withdraw write failed");
                    return;
                }
                self.execute_all(module, client, &transition.effects).await;
                transition.next
            }
            // `NothingToWithdraw` — the row was already empty, which is
            // exactly the row that should now go.
            Err(_) => row,
        };
        if next.current.is_none()
            && next.pending.is_none()
            && let Err(e) = self.storage.remove_row(module.as_str(), client)
        {
            tracing::warn!(module = %module, client = %client, error = %e, "removing an empty row failed");
        }
    }
}

/// The rows' timer. Same cadence as the lifecycle's, and cancelled and
/// awaited at shutdown alongside it.
pub fn spawn_tick(
    manager: Arc<MoveManager>,
    cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let interval = crate::lifecycle::TICK_INTERVAL;
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("client-row tick cancelled");
                    return;
                }
                _ = tick.tick() => manager.tick().await,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::testing::{DEV_CLIENT, PROD_CLIENT, configured_authenticator};
    use crate::fetch::testing::FakeFetcher;
    use crate::host_v2::testing::FakeHost;
    use crate::manifest::Manifest;
    use mitos_lifecycle::access::OperatorTokenAsClient;
    use mitos_lifecycle::artifact::ArtifactSource;
    use mitos_lifecycle::client_move::{Phase, Stage};
    use mitos_lifecycle::ids::{Revision, Sha256Hex};

    const MODULE: &str = "ownership-indexer";

    struct Harness {
        _tmp: tempfile::TempDir,
        storage: ModuleStorage,
        fetcher: Arc<FakeFetcher>,
        moves: Arc<MoveManager>,
        lifecycle: Arc<LifecycleManager>,
        events: EventRing,
        auth: Arc<Authenticator>,
    }

    impl Harness {
        fn new() -> Self {
            Self::with_auth(Arc::new(configured_authenticator(
                OperatorTokenAsClient::AllowWithWarning,
            )))
        }

        fn with_auth(auth: Arc<Authenticator>) -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let storage = ModuleStorage::new(tmp.path());
            let host = FakeHost::new();
            let events = EventRing::new();
            let lifecycle = Arc::new(LifecycleManager::new(
                storage.clone(),
                Arc::new(host),
                auth.clone(),
                events.clone(),
                crate::lifecycle::system_clock(),
            ));
            let fetcher = Arc::new(FakeFetcher::new());
            let puller = Arc::new(ArtifactPuller::new(storage.clone(), fetcher.clone()));
            let moves = MoveManager::new(
                storage.clone(),
                puller,
                auth.clone(),
                lifecycle.clone(),
                events.clone(),
            );
            Self {
                _tmp: tmp,
                storage,
                fetcher,
                moves,
                lifecycle,
                events,
                auth,
            }
        }

        /// Activate `MODULE` at `revision` and make it the current instance.
        fn activate(&self, wasm: &[u8], revision: u32) -> Manifest {
            let mut manifest = crate::companions::testing::manifest_for(MODULE, wasm);
            manifest.lifecycle.revision = Revision(revision);
            self.storage.activate(&manifest, wasm).unwrap();
            manifest
        }

        fn subscribe(&self, client: &str) {
            crate::lifecycle::testing::write_registration(
                &self.storage,
                MODULE,
                client,
                "policy_a",
            );
        }

        /// The announcement a worker would make for `wasm` at `revision`,
        /// with the fetcher primed to serve it from that client's hostname.
        fn announcement(&self, client: &str, wasm: &[u8], revision: u32) -> AnnouncedModule {
            let mut manifest = crate::companions::testing::manifest_for(MODULE, wasm);
            manifest.lifecycle.revision = Revision(revision);
            let manifest_bytes = manifest.to_toml().unwrap();
            let announced = AnnouncedModule {
                module: ModuleId::parse(MODULE).unwrap(),
                wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(wasm)).unwrap(),
                manifest: Sha256Hex::parse(&crate::manifest::sha256_hex(manifest_bytes.as_bytes()))
                    .unwrap(),
                revision: Revision(revision),
                config: None,
            };
            let source = ArtifactSource::for_client(
                &ClientId::parse(client).unwrap(),
                &announced.module,
                &announced.wasm,
            )
            .unwrap();
            self.fetcher.serve(&source.wasm_url, wasm.to_vec());
            self.fetcher
                .serve(&source.manifest_url, manifest_bytes.into_bytes());
            announced
        }

        /// Announce, then wait for the fetch it started — what nearly every
        /// test wants to look at. `announce_without_waiting` is the other one.
        async fn announce(&self, client: &str, announced: &AnnouncedModule) -> AnnounceResult {
            let result = self.announce_without_waiting(client, announced).await;
            self.moves.settle().await;
            result
        }

        async fn announce_without_waiting(
            &self,
            client: &str,
            announced: &AnnouncedModule,
        ) -> AnnounceResult {
            self.moves
                .announce(
                    &ClientId::parse(client).unwrap(),
                    &ModuleId::parse(MODULE).unwrap(),
                    announced,
                )
                .await
        }

        fn row(&self, client: &str) -> Option<ClientRow> {
            self.storage
                .read_row(MODULE, &ClientId::parse(client).unwrap())
                .unwrap()
        }

        fn failures(&self) -> Vec<String> {
            self.events
                .snapshot(0, None, Some("move_failed"), 512)
                .into_iter()
                .map(|e| format!("{:?}", e.kind))
                .collect()
        }
    }

    /// A real (empty) wasm component — `validate_component` is the last
    /// check and a fake byte string would fail it first.
    fn component(tag: u8) -> Vec<u8> {
        // The canonical empty component, plus a custom section so two
        // "builds" differ in bytes and therefore in sha.
        let mut bytes = vec![0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];
        bytes.extend_from_slice(&[0x00, 0x03, 0x01, b'v', tag]);
        bytes
    }

    // -----------------------------------------------------------------

    #[tokio::test]
    async fn an_attached_client_is_adopted_onto_its_instance_not_started_at_none() {
        let h = Harness::new();
        let wasm = component(1);
        h.activate(&wasm, 3);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let announced = h.announcement(PROD_CLIENT, &component(2), 4);
        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        // `current: None` would mean FIRST subscribe, which skips catch-up,
        // the pause and the drain and simply attaches — so this client's
        // first announcement would switch it with no hand-off at all.
        let row = h.row(PROD_CLIENT).expect("a row was written");
        let current = row.current.expect("the client is already being served");
        assert_eq!(current.revision, Revision(3));
        assert_eq!(
            current.instance.wasm.as_str(),
            crate::manifest::sha256_hex(&wasm)
        );
    }

    #[tokio::test]
    async fn a_move_parks_at_queued_and_no_slot_is_ever_granted() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        let row = h.row(PROD_CLIENT).unwrap();
        let pending = row.pending.expect("a move is pending");
        match pending.stage {
            Stage::Planned { phase, .. } => assert_eq!(
                phase,
                Phase::Queued,
                "this order requests a slot and never grants one",
            ),
            Stage::Fetching => panic!("the artifact should have been fetched"),
        }
        // …and the artifact really is in the CAS.
        assert!(h.storage.artifact_path(&announced.wasm).exists());
        assert!(
            h.storage
                .read_manifest_by_digest(&announced.manifest)
                .unwrap()
                .is_some()
        );
    }

    /// Every one of a client's ~91 DOs announces the same build. All but the
    /// first are recognised as already in flight.
    #[tokio::test]
    async fn the_91st_companion_of_a_client_is_a_no_op() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        for _ in 0..91 {
            h.announce(PROD_CLIENT, &announced).await;
        }
        assert_eq!(
            h.fetcher.calls(),
            2,
            "91 announcements of one build must cause ONE download (wasm + manifest)",
        );
    }

    /// `prod` is `ExplicitRollbackOnly`, so an announcement BEHIND the
    /// client's current revision is refused — and a refusal writes nothing.
    #[tokio::test]
    async fn a_rollback_is_refused_in_prod_and_the_row_is_untouched() {
        let h = Harness::new();
        h.activate(&component(1), 5);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        // Adopt the row at revision 5 by announcing the build it is on.
        let current = h.announcement(PROD_CLIENT, &component(1), 5);
        assert_eq!(
            h.announce(PROD_CLIENT, &current).await,
            AnnounceResult::AlreadyCurrent
        );
        let before = h.row(PROD_CLIENT).unwrap();

        let older = h.announcement(PROD_CLIENT, &component(2), 3);
        let result = h.announce(PROD_CLIENT, &older).await;
        match &result {
            AnnounceResult::Refused { reason } => {
                assert!(reason.contains("behind"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(
            h.row(PROD_CLIENT).unwrap(),
            before,
            "a refused announcement must leave the row exactly as it was",
        );
    }

    #[tokio::test]
    async fn a_fork_past_the_limit_is_refused() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        // Fill the module up to the cap with instances nothing is on.
        let module = ModuleId::parse(MODULE).unwrap();
        for i in 0..MAX_INSTANCES_PER_MODULE as u8 {
            let key = mitos_lifecycle::ids::InstanceKey {
                module: module.clone(),
                wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(&component(100 + i))).unwrap(),
                config: None,
            };
            h.storage.write_instance_marker(&key).unwrap();
        }

        let announced = h.announcement(PROD_CLIENT, &component(9), 2);
        match h.announce(PROD_CLIENT, &announced).await {
            AnnounceResult::Refused { reason } => {
                assert!(reason.contains("refusing to fork"), "{reason}");
            }
            other => panic!("expected a fork-limit refusal, got {other:?}"),
        }
        assert_eq!(h.fetcher.calls(), 0, "a refused fork must fetch nothing");
    }

    /// A worker that rebuilds twice in an afternoon: the second announcement
    /// replaces the first, and the first's late answer is dropped QUIETLY —
    /// a superseded fetch is not a failure and must not alert.
    #[tokio::test]
    async fn a_newer_announcement_abandons_the_pending_one() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let first = h.announcement(PROD_CLIENT, &component(2), 2);
        h.announce(PROD_CLIENT, &first).await;
        let second = h.announcement(PROD_CLIENT, &component(3), 3);
        h.announce(PROD_CLIENT, &second).await;

        let row = h.row(PROD_CLIENT).unwrap();
        let pending = row.pending.expect("the newer move is pending");
        assert_eq!(
            pending.target.instance.wasm, second.wasm,
            "the newer announcement must replace the older, never queue behind it",
        );
        assert_eq!(pending.target.revision, Revision(3));
        assert!(
            h.failures().is_empty(),
            "an abandoned move is not a failure: {:?}",
            h.failures(),
        );
    }

    #[tokio::test]
    async fn a_refused_artifact_alerts_and_leaves_the_client_where_it_was() {
        let h = Harness::new();
        let live = component(1);
        h.activate(&live, 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let current = h.announcement(PROD_CLIENT, &live, 1);
        h.announce(PROD_CLIENT, &current).await;

        // Announce a build whose served wasm is NOT what was announced.
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        let source = ArtifactSource::for_client(
            &ClientId::parse(PROD_CLIENT).unwrap(),
            &announced.module,
            &announced.wasm,
        )
        .unwrap();
        h.fetcher.serve(&source.wasm_url, b"swapped bytes".to_vec());

        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        let row = h.row(PROD_CLIENT).unwrap();
        assert!(row.pending.is_none(), "a refused artifact clears the move");
        assert_eq!(
            row.current.expect("still served").revision,
            Revision(1),
            "the client stays exactly where it was",
        );
        // Two entries, and both are wanted: the REFUSAL, naming which check
        // rejected the bytes, and the move's own `Alert(Failed)`. The first
        // is what an operator needs to fix it; the second is what says the
        // move is over.
        let failures = h.failures();
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(
            failures.iter().any(|f| f.contains("bytes served hash to")),
            "one entry must name the check that refused it: {failures:?}",
        );
    }

    /// `may_announce` refuses a client no operator NAMED — which is every
    /// client in legacy and open mode, where a client id is whatever the
    /// caller typed and the host would fetch from a hostname of their
    /// choosing.
    #[tokio::test]
    async fn a_host_without_a_namespaces_file_refuses_every_announcement() {
        let h = Harness::with_auth(Arc::new(crate::auth::testing::legacy_authenticator()));
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        match h.announce(PROD_CLIENT, &announced).await {
            AnnounceResult::Refused { reason } => {
                assert!(reason.contains("not named in any namespace"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(h.fetcher.calls(), 0, "nothing may be fetched");
        assert!(h.row(PROD_CLIENT).is_none(), "no row may be written");
        assert!(
            !h.storage.root().join("_cas").join("config").exists(),
            "not even the inline config",
        );
    }

    #[tokio::test]
    async fn a_client_that_unsubscribed_loses_its_pending_move_and_then_its_row() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        h.announce(PROD_CLIENT, &announced).await;
        assert!(h.row(PROD_CLIENT).is_some());

        // The client's registration goes away.
        crate::lifecycle::testing::remove_registration(&h.storage, MODULE, PROD_CLIENT, "policy_a");
        h.lifecycle.tick().await;
        h.moves.tick().await;

        assert!(
            h.row(PROD_CLIENT).is_none(),
            "a row with neither a placement nor a pending move must go",
        );
    }

    /// The README's rule, as a test: the read surfaces must not adopt.
    #[tokio::test]
    async fn reading_rows_never_writes_them() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        assert!(h.storage.list_rows(MODULE).unwrap().is_empty());
        assert!(
            !h.storage.module_dir_for_rows(MODULE).exists(),
            "listing rows for a module that has none must not create the directory",
        );

        // …and a tick over a module with no rows writes none either.
        h.moves.tick().await;
        assert!(h.storage.list_rows(MODULE).unwrap().is_empty());
    }

    /// A dev token cannot announce for a prod client — WO-02 already 403s
    /// the whole subscribe, so this asserts the layer below agrees.
    #[tokio::test]
    async fn a_dev_client_announcing_gets_its_own_namespaces_profile() {
        let h = Harness::new();
        h.activate(&component(1), 5);
        crate::lifecycle::testing::write_registration(&h.storage, MODULE, DEV_CLIENT, "policy_a");
        h.lifecycle.boot().await;

        // `dev` is `RevisionPolicy::Allow`, so the rollback prod refuses is
        // permitted here — the profile comes from the CLIENT's namespace.
        let current = h.announcement(DEV_CLIENT, &component(1), 5);
        h.announce(DEV_CLIENT, &current).await;
        let older = h.announcement(DEV_CLIENT, &component(2), 3);
        assert_eq!(
            h.announce(DEV_CLIENT, &older).await,
            AnnounceResult::Accepted,
            "dev allows moving down; the policy is the client's, not the host's",
        );
    }

    /// Through the REAL router: a refused announcement must still leave the
    /// companion registered. A worker whose build the host declines is a
    /// worker running slightly older code, not one that lost its
    /// subscription.
    #[tokio::test]
    async fn an_announcement_never_fails_the_subscribe() {
        use crate::auth::testing::PROD;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode, header};
        use tower::ServiceExt;

        let h = Harness::new();
        h.activate(&component(1), 1);
        h.lifecycle.boot().await;

        let router = crate::companions::companion_router(
            h.storage.clone(),
            h.auth.clone(),
            None,
            None,
            h.events.clone(),
            Some(h.lifecycle.clone()),
            Some(h.moves.clone()),
        );

        // An announcement the host will refuse: the fetcher serves nothing,
        // so the artifact is unreachable.
        let announced = AnnouncedModule {
            module: ModuleId::parse(MODULE).unwrap(),
            wasm: Sha256Hex::parse(&crate::manifest::sha256_hex(&component(7))).unwrap(),
            manifest: Sha256Hex::parse(&"cd".repeat(32)).unwrap(),
            revision: Revision(2),
            config: None,
        };
        let request = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module {
                name: MODULE.into(),
            }],
            companion_key: "policy_a".into(),
            client_id: PROD_CLIENT.into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
            announced: vec![announced],
        };
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .header(header::AUTHORIZATION, format!("Bearer {PROD}"))
                    .body(Body::from(request.encode().unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a refused announcement must not fail the subscribe",
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let decoded = mitos_protocol::SubscribeResponse::decode(&body).unwrap();
        assert_eq!(decoded.status, "subscribed");
        // `Accepted` — the MOVE was accepted; the artifact turning out to be
        // unreachable happens after that and is reported on the ring, not in
        // the response. An outcome describes the announcement's reception,
        // not the eventual fate of the move.
        assert_eq!(
            decoded.announced.first().map(|o| &o.result),
            Some(&AnnounceResult::Accepted),
            "{:?}",
            decoded.announced,
        );
        // The response came back with the pull still running.
        h.moves.settle().await;
        assert!(
            !h.failures().is_empty(),
            "the unreachable artifact must reach the event ring",
        );
        assert!(
            h.row(PROD_CLIENT).unwrap().pending.is_none(),
            "a refused artifact clears the move",
        );

        // …and the registration really is on disk.
        assert!(
            h.storage
                .module_dir_for_companions(MODULE)
                .join(PROD_CLIENT)
                .join("policy_a.cbor")
                .is_file(),
            "the companion must still be registered",
        );
    }

    /// An announcement for a module this subscribe does not target has
    /// nothing the host can act on, and accepting it would let one subscribe
    /// drive a move for any module on the box.
    #[tokio::test]
    async fn an_announcement_for_an_untargeted_module_is_refused() {
        use crate::auth::testing::PROD;
        use axum::body::Body;
        use axum::http::{Request as HttpRequest, StatusCode, header};
        use tower::ServiceExt;

        let h = Harness::new();
        h.activate(&component(1), 1);
        h.lifecycle.boot().await;

        let router = crate::companions::companion_router(
            h.storage.clone(),
            h.auth.clone(),
            None,
            None,
            h.events.clone(),
            Some(h.lifecycle.clone()),
            Some(h.moves.clone()),
        );
        let request = mitos_protocol::SubscribeRequest {
            targets: vec![mitos_protocol::SubscribeTarget::Module {
                name: MODULE.into(),
            }],
            companion_key: "policy_a".into(),
            client_id: PROD_CLIENT.into(),
            resume_from: None,
            interests: vec![],
            dial_back: None,
            announced: vec![AnnouncedModule {
                // A module the request does not target.
                module: ModuleId::parse("collection-holders").unwrap(),
                wasm: Sha256Hex::parse(&"ab".repeat(32)).unwrap(),
                manifest: Sha256Hex::parse(&"cd".repeat(32)).unwrap(),
                revision: Revision(1),
                config: None,
            }],
        };
        let response = router
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/api/companions/subscribe")
                    .header("content-type", "application/cbor")
                    .header(header::AUTHORIZATION, format!("Bearer {PROD}"))
                    .body(Body::from(request.encode().unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let decoded = mitos_protocol::SubscribeResponse::decode(&body).unwrap();
        match decoded.announced.first().map(|o| &o.result) {
            Some(AnnounceResult::Refused { reason }) => {
                assert!(reason.contains("does not target"), "{reason}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(h.fetcher.calls(), 0);
    }

    /// A row exactly as a crash mid-fetch leaves it: `Fetching`, on disk, with
    /// nothing in memory. Written directly rather than by announcing, because
    /// an announce that returns has either finished its fetch or still has
    /// the task running — neither is a restart.
    fn write_a_row_stuck_fetching(h: &Harness, announced: &AnnouncedModule) {
        let mut row = ClientRow::new(
            ClientKey {
                namespace: mitos_lifecycle::ids::Namespace::parse("prod").unwrap(),
                client: ClientId::parse(PROD_CLIENT).unwrap(),
            },
            ModuleId::parse(MODULE).unwrap(),
        );
        let transition = row
            .step(Event::Announce {
                module_ref: ModuleRef::announced_by(
                    &ClientId::parse(PROD_CLIENT).unwrap(),
                    &ModuleId::parse(MODULE).unwrap(),
                    announced.wasm.clone(),
                    announced.manifest.clone(),
                    None,
                    announced.revision,
                )
                .unwrap(),
                intent: MoveIntent::Announced,
                context: AnnounceContext {
                    revision_policy: mitos_lifecycle::policy::RevisionPolicy::Allow,
                    target: TargetInstance::Missing,
                    live_instances: 1,
                    max_instances: MAX_INSTANCES_PER_MODULE,
                },
            })
            .unwrap();
        row = transition.next;
        assert!(matches!(
            row.pending.as_ref().unwrap().stage,
            Stage::Fetching
        ));
        h.storage.write_row(MODULE, &row).unwrap();
    }

    #[tokio::test]
    async fn boot_refetches_a_row_that_was_fetching() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        write_a_row_stuck_fetching(&h, &announced);

        h.moves.boot().await;
        h.moves.settle().await;

        let after = h.row(PROD_CLIENT).unwrap();
        assert!(
            matches!(
                after.pending.as_ref().map(|p| &p.stage),
                Some(Stage::Planned {
                    phase: Phase::Queued,
                    ..
                })
            ),
            "{after:?}"
        );
        assert!(h.storage.artifact_path(&announced.wasm).exists());
        assert!(h.failures().is_empty(), "{:?}", h.failures());
    }

    /// The host restarts while a worker is down. `boot` must come back having
    /// STARTED the pull — held here, as a dead worker would hold it — and the
    /// move must then fail on its own rather than sit in `Fetching` for ever.
    #[tokio::test]
    async fn boot_does_not_wait_on_an_unreachable_worker() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        let source = ArtifactSource::for_client(
            &ClientId::parse(PROD_CLIENT).unwrap(),
            &announced.module,
            &announced.wasm,
        )
        .unwrap();
        h.fetcher.fail(&source.wasm_url, "unreachable");
        write_a_row_stuck_fetching(&h, &announced);

        let release = h.fetcher.hold();
        tokio::time::timeout(std::time::Duration::from_secs(5), h.moves.boot())
            .await
            .expect("boot waited on a fetch that was never going to answer");
        assert!(matches!(
            h.row(PROD_CLIENT).unwrap().pending.unwrap().stage,
            Stage::Fetching
        ));

        release.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
        h.moves.settle().await;
        assert!(h.row(PROD_CLIENT).unwrap().pending.is_none());
        assert!(!h.failures().is_empty());
    }

    /// Why the manifest pin is PERSISTED. Between the crash and the restart
    /// the worker starts serving a different manifest over the same wasm — a
    /// host that had forgotten the pin could only take it on trust.
    #[tokio::test]
    async fn a_refetch_after_a_restart_still_holds_the_manifest_to_its_pin() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        write_a_row_stuck_fetching(&h, &announced);

        let mut swapped = crate::companions::testing::manifest_for(MODULE, &component(2));
        swapped.lifecycle.revision = Revision(2);
        swapped.build.build_id = format!("{}-swapped", swapped.build.build_id);
        let source = ArtifactSource::for_client(
            &ClientId::parse(PROD_CLIENT).unwrap(),
            &announced.module,
            &announced.wasm,
        )
        .unwrap();
        h.fetcher.serve(
            &source.manifest_url,
            swapped.to_toml().unwrap().into_bytes(),
        );

        h.moves.boot().await;
        h.moves.settle().await;

        assert!(h.row(PROD_CLIENT).unwrap().pending.is_none());
        assert!(!h.storage.artifact_path(&announced.wasm).exists());
        assert!(!h.failures().is_empty());
    }

    /// A subscribe has a Durable Object waiting on it. The fetch is up to
    /// 16 MiB from a worker that may be slow or gone, so `announce` must
    /// return with the move RECORDED and the pull still to come.
    #[tokio::test]
    async fn an_announcement_does_not_wait_for_the_download() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let announced = h.announcement(PROD_CLIENT, &component(2), 2);

        let release = h.fetcher.hold();
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            h.announce_without_waiting(PROD_CLIENT, &announced),
        )
        .await
        .expect("the announcement waited on its own download");
        assert_eq!(first, AnnounceResult::Accepted);
        assert!(matches!(
            h.row(PROD_CLIENT).unwrap().pending.unwrap().stage,
            Stage::Fetching
        ));
        // …and the row lock is free while the pull runs: a second
        // announcement is answered, not queued behind the download.
        assert_eq!(
            h.announce_without_waiting(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        release.add_permits(tokio::sync::Semaphore::MAX_PERMITS / 2);
        h.moves.settle().await;
        assert!(matches!(
            h.row(PROD_CLIENT).unwrap().pending.unwrap().stage,
            Stage::Planned { .. }
        ));
    }
}
