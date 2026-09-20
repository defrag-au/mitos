//! A worker announces its build; the host records it, pulls it, grants it the
//! host-wide catch-up slot and CREATES the instance it is moving to — D4's
//! first five states.
//!
//! `ClientRow::step` runs `Announce → Fetching → Queued → Preparing →
//! CatchingUp`, and this stops there. Nothing feeds `CaughtUp`; nothing
//! pauses, commits or switches — that is WO-09. So a worker that announces
//! keeps being served by exactly the instance it is on, while a SECOND
//! instance follows the chain beside it and speaks to nobody
//! (`routing::fan_out` → `Skip`).
//!
//! # Which lock, and in which order
//!
//! Two locks meet here. A ROW lock (`(module, client)`) guards a row's
//! read-modify-write; the MODULE lock inside `LifecycleManager` guards
//! anything that starts or stops a follower. Everything in this file takes
//! them in that order — row, then module — and nothing anywhere takes a row
//! lock while holding a module one.
//!
//! `EnsureInstance` is the exception that proves it: creating an instance
//! copies up to a gigabyte with the parent's follower stopped, so it is
//! SPAWNED and takes the row lock again only to report. A grant is spawned
//! for a different reason: it names another row more often than not, and two
//! rows releasing the slot to each other inline would deadlock.
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
    AnnounceContext, CatchUpSlot, ClientRow, Effect, Event, Grant, ModuleRef, MoveFailure,
    Placement, Progress, QueuedMove, Rejected, TargetInstance, next_catch_up,
};
use mitos_lifecycle::ids::{ClientId, ClientKey, InstanceKey, ModuleId};
use mitos_lifecycle::instance::DetachCause;
use mitos_lifecycle::upgrade::MoveIntent;
use mitos_protocol::{AnnounceResult, AnnouncedModule};

use crate::auth::Authenticator;
use crate::events::{EventKind, EventRing};
use crate::fetch::{Announced, ArtifactPuller, Refusal};
use crate::lifecycle::{Clock, LifecycleManager};
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
    clock: Clock,
    /// One lock per `(module, client)` — a row's read-modify-write.
    locks: std::sync::Mutex<HashMap<RowKey, Arc<tokio::sync::Mutex<()>>>>,
    /// THE host-wide catch-up slot: one staging follower replaying the chain
    /// at full speed at a time, on a box whose binding constraint is memory.
    ///
    /// A plain `std::sync::Mutex` because no `.await` is ever held across it:
    /// `request` and `release` are pure, and the `Grant` they return is fed
    /// on a spawned task. In memory only — `ClientRow::recover` re-issues
    /// every `RequestCatchUpSlot` at boot, so the queue rebuilds itself from
    /// the rows and there is no second copy to disagree with them.
    slot: std::sync::Mutex<CatchUpSlot>,
    /// A fetch, a grant or an instance build outlives the call that started
    /// it, so the task needs an owning handle back to the manager.
    me: std::sync::Weak<Self>,
    /// Spawned work still running, so [`Self::settle`] can wait for it.
    tasks: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// `(module, client)` — which row a lock guards.
type RowKey = (String, String);

/// The catch-up slot as a reader sees it.
pub struct CatchUpSnapshot {
    /// The one move catching up, host-wide.
    pub holder: Option<(ClientKey, ModuleId)>,
    /// Everyone else, IN THE ORDER THEY WILL BE GRANTED.
    pub waiting: Vec<QueuedMove>,
}

impl MoveManager {
    pub fn new(
        storage: ModuleStorage,
        puller: Arc<ArtifactPuller>,
        auth: Arc<Authenticator>,
        lifecycle: Arc<LifecycleManager>,
        events: EventRing,
        clock: Clock,
    ) -> Arc<Self> {
        Arc::new_cyclic(|me| Self {
            storage,
            puller,
            auth,
            lifecycle,
            events,
            clock,
            locks: std::sync::Mutex::new(HashMap::new()),
            slot: std::sync::Mutex::new(CatchUpSlot::default()),
            me: me.clone(),
            tasks: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Wait for every task started so far — and for anything THOSE started,
    /// which is the whole point of the loop: a fetch's answer grants a slot,
    /// which builds an instance.
    ///
    /// For tests, and for an orderly shutdown; nothing on a request path may
    /// call it.
    pub async fn settle(&self) {
        loop {
            let running: Vec<_> = std::mem::take(&mut *self.tasks.lock().expect("tasks mutex"));
            if running.is_empty() {
                return;
            }
            for task in running {
                let _ = task.await;
            }
        }
    }

    /// Spawn work that outlives its caller, and remember it for `settle`.
    fn spawn(&self, work: impl std::future::Future<Output = ()> + Send + 'static) {
        let task = tokio::spawn(work);
        let mut tasks = self.tasks.lock().expect("tasks mutex");
        tasks.retain(|running| !running.is_finished());
        tasks.push(task);
    }

    /// The slot, for the admin read surface.
    ///
    /// `waiting` is ordered by asking `next_catch_up` repeatedly rather than
    /// by sorting on a key of this file's choosing — the view cannot then
    /// drift from the decision.
    pub fn catch_up(&self) -> CatchUpSnapshot {
        let slot = self.slot.lock().expect("catch-up slot mutex");
        let mut left = slot.waiting().to_vec();
        let mut waiting = Vec::with_capacity(left.len());
        while let Some(next) = next_catch_up(&left) {
            let next = next.clone();
            left.retain(|m| (&m.client, &m.module) != (&next.client, &next.module));
            waiting.push(next);
        }
        CatchUpSnapshot {
            holder: slot.holder().cloned(),
            waiting,
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

        // 3b. REBASE, before the announcement is stepped. A move is planned
        //     FROM `current`, and a row goes stale two ways: an operator
        //     upload RENAMES the module's current instance, so every row of
        //     that module names a directory that is gone; and a row made
        //     before its client was being served says `None` of a client
        //     that now is — which the machine reads as a FIRST subscribe and
        //     would switch with no hand-off at all. See `ClientRow::rebase`.
        let row = self.rebase_if_stale(module, &client_key, row).await?;

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

    /// Bring the row's `current` into line with where the client is ACTUALLY
    /// being served, before anything is planned from it.
    ///
    /// **Caller holds the row lock**, because this WRITES. A refusal
    /// (`CommitInFlight` — unreachable until WO-09 can commit) writes
    /// nothing and refuses the announcement with that reason.
    async fn rebase_if_stale(
        &self,
        module: &ModuleId,
        client: &ClientKey,
        row: ClientRow,
    ) -> Result<ClientRow, String> {
        let Some(onto) = self.served_from(module, client, &row) else {
            // Nothing is serving this client — which by the time an
            // announcement is stepped means the module has no current
            // instance, and WO-05 already refuses that upstream.
            return Ok(row);
        };
        if row.current.as_ref() == Some(&onto) {
            return Ok(row);
        }
        tracing::info!(
            client = %client,
            module = %module,
            was = ?row.current.as_ref().map(|p| p.instance.dir_name()),
            onto = %onto.instance.dir_name(),
            "rebasing a stale client row before planning its move",
        );
        let transition = row
            .step(Event::Rebase { onto })
            .map_err(|e: Rejected| e.to_string())?;
        self.storage
            .write_row(module.as_str(), &transition.next)
            .map_err(|e| e.to_string())?;
        self.execute_all(module, &client.client, &transition.effects)
            .await;
        Ok(transition.next)
    }

    /// Which instance is serving this client right now.
    ///
    /// The row's own `current` when that instance directory is still there,
    /// and otherwise the module's current instance — the same rule
    /// `routing::fan_out` derives on every read, made into a WRITE here by
    /// the holder of the row lock.
    fn served_from(
        &self,
        module: &ModuleId,
        client: &ClientKey,
        row: &ClientRow,
    ) -> Option<Placement> {
        if let Some(current) = &row.current
            && self.storage.instance_dir(&current.instance).is_dir()
        {
            return Some(current.clone());
        }
        self.current_placement(module, client)
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
            Effect::RequestCatchUpSlot => {
                if let Some(wanted) = self.queued_move(module, client) {
                    let granted = self
                        .slot
                        .lock()
                        .expect("catch-up slot mutex")
                        .request(wanted);
                    self.feed_grant(granted);
                }
            }
            Effect::ReleaseCatchUpSlot => {
                if let Some(key) = self.registry().client_key(client) {
                    let granted = self
                        .slot
                        .lock()
                        .expect("catch-up slot mutex")
                        .release(&key, module);
                    self.feed_grant(granted);
                }
            }
            Effect::EnsureInstance { key, parent } => {
                self.spawn_ensure_instance(client, key.clone(), parent.clone());
            }
            Effect::ReleaseStaging(key) => {
                // Takes the MODULE lock — the row lock is held, and that is
                // the order this file always takes them in.
                if let Err(e) = self.lifecycle.release_staging(key).await {
                    tracing::warn!(
                        module = %module,
                        client = %client,
                        instance = %key.dir_name(),
                        error = %e,
                        "releasing a staging instance failed; the lifecycle tick's sweep of unwanted \
                         staging instances will release it",
                    );
                }
            }
            // WO-07 derives an instance's `attached` set from the ROWS
            // (`routing::attached_to`), so the row write IS the attach. These
            // two say "the row changed, recount" — and being a diff, a
            // reconcile that runs once for both is exactly right.
            Effect::AttachTo(_) | Effect::DetachFrom { .. } => {
                if let Err(e) = self.lifecycle.reconcile(module.as_str()).await {
                    tracing::warn!(
                        module = %module,
                        client = %client,
                        error = %e,
                        "reconciling after a row change failed; the tick will correct it",
                    );
                }
            }
            // The staging instance already speaks to nobody: `fan_out` is
            // `Skip` for every client until a row names it. HOLDING this
            // client's emissions on it, and diffing them against the live
            // instance's, is what WO-09 adds.
            Effect::HoldAndDiff { .. } => {}
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
            // Everything else is downstream of `CaughtUp`, which nothing
            // feeds. Loud, but never fatal: a reducer that surprised the host
            // is not a reason to take the host down.
            other => tracing::error!(
                module = %module,
                client = %client,
                effect = ?other,
                "move effect is not reachable until the switch lands; ignoring",
            ),
        }
    }

    /// This client's place in the catch-up queue: its namespace's TIER, which
    /// is what puts production ahead of development, and when it asked.
    fn queued_move(&self, module: &ModuleId, client: &ClientId) -> Option<QueuedMove> {
        let key = self.registry().client_key(client)?;
        let tier = self.registry().namespace_of(client)?.profile.tier;
        Some(QueuedMove {
            client: key,
            module: module.clone(),
            tier,
            queued_at: (self.clock)(),
        })
    }

    /// Feed a `Grant` on its OWN task.
    ///
    /// A grant names ANOTHER row more often than not, and feeding it takes
    /// that row's lock — inline, under the lock of the row whose effect
    /// produced it, two rows releasing the slot to each other would deadlock.
    fn feed_grant(&self, granted: Option<Grant>) {
        let Some(grant) = granted else {
            return;
        };
        let Some(me) = self.me.upgrade() else {
            return;
        };
        self.spawn(async move { me.grant_slot(grant).await });
    }

    /// Tell the granted row it has the slot — or, if it cannot be told, give
    /// the slot straight back.
    ///
    /// A move can be abandoned between being queued and being granted. The
    /// slot would then have a holder that never releases it, and ONE leaked
    /// slot stops every move on the host.
    async fn grant_slot(&self, grant: Grant) {
        let client = grant.client.client.clone();
        let module = grant.module.clone();
        let outcome = {
            let lock = self.lock_for(module.as_str(), &client);
            let _guard = lock.lock().await;
            match self
                .storage
                .read_row(module.as_str(), &client)
                .ok()
                .flatten()
                .and_then(|row| row.pending.map(|p| p.target.instance))
            {
                Some(target) => {
                    self.progress_locked(&module, &client, target, Progress::SlotGranted)
                        .await
                }
                None => Err("the granted move is no longer pending".to_owned()),
            }
        };
        let Err(reason) = outcome else {
            return;
        };
        tracing::debug!(
            module = %module,
            client = %client,
            %reason,
            "catch-up slot granted to a move that is gone; releasing it again",
        );
        let granted = self
            .slot
            .lock()
            .expect("catch-up slot mutex")
            .release(&grant.client, &module);
        self.feed_grant(granted);
    }

    /// Build the target instance on its OWN task.
    ///
    /// `create_staging` takes the MODULE lock and copies up to a gigabyte
    /// with the parent's follower stopped. The caller holds a ROW lock and is
    /// as often as not a subscribe handler; awaited here it would hold that
    /// row lock for the length of the copy.
    fn spawn_ensure_instance(
        &self,
        client: &ClientId,
        key: InstanceKey,
        parent: Option<InstanceKey>,
    ) {
        let Some(me) = self.me.upgrade() else {
            return;
        };
        let client = client.clone();
        self.spawn(async move {
            let module = key.module.clone();
            let step = match me.lifecycle.create_staging(&key, parent.as_ref()).await {
                Ok(()) => Progress::InstanceReady,
                Err(e) => {
                    tracing::error!(
                        module = %module,
                        client = %client,
                        instance = %key.dir_name(),
                        error = %e,
                        "creating a staging instance failed; the move fails and the client \
                         stays where it is",
                    );
                    me.events.record(
                        module.as_str(),
                        EventKind::MoveFailed {
                            client_id: client.to_string(),
                            reason: e.to_string(),
                        },
                    );
                    Progress::Failed(MoveFailure::StagingFailed)
                }
            };
            // A newer announcement may have replaced the move while the copy
            // ran. `Rejected::Stale` is how the machine says so, and it is
            // dropped QUIETLY — as a superseded fetch is.
            let lock = me.lock_for(module.as_str(), &client);
            let _guard = lock.lock().await;
            if let Err(e) = me.progress_locked(&module, &client, key, step).await {
                tracing::debug!(
                    module = %module,
                    client = %client,
                    rejected = %e,
                    "stale staging answer",
                );
            }
        });
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
        self.spawn(async move {
            me.ensure_artifact(&module, &client, &wasm, &manifest).await;
        });
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
    /// Two more, because the catch-up slot is HOST-wide: one client can have
    /// at most one row per module, so "who is waiting behind whom" needs
    /// more than one module to say anything.
    const OTHER: &str = "collection-holders";
    const THIRD: &str = "collection-metadata";

    /// One move's place in the catch-up queue, as a test spells it:
    /// `(client, module)`.
    type Standing = (String, String);

    struct Harness {
        _tmp: tempfile::TempDir,
        storage: ModuleStorage,
        fetcher: Arc<FakeFetcher>,
        moves: Arc<MoveManager>,
        lifecycle: Arc<LifecycleManager>,
        host: FakeHost,
        clock: crate::lifecycle::testing::TestClock,
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
            let host = FakeHost::new(storage.clone());
            let events = EventRing::new();
            // A clock a test moves by hand. The catch-up queue orders by
            // `queued_at` within a tier, and two moves queued in the same
            // millisecond would otherwise be ordered by client name alone.
            let clock = crate::lifecycle::testing::TestClock::new(1_700_000_000);
            let lifecycle = Arc::new(LifecycleManager::new(
                storage.clone(),
                Arc::new(host.clone()),
                auth.clone(),
                events.clone(),
                clock.clock(),
            ));
            let fetcher = Arc::new(FakeFetcher::new());
            let puller = Arc::new(ArtifactPuller::new(storage.clone(), fetcher.clone()));
            let moves = MoveManager::new(
                storage.clone(),
                puller,
                auth.clone(),
                lifecycle.clone(),
                events.clone(),
                clock.clock(),
            );
            Self {
                _tmp: tmp,
                storage,
                fetcher,
                moves,
                lifecycle,
                host,
                clock,
                events,
                auth,
            }
        }

        /// Activate `MODULE` at `revision` and make it the current instance.
        fn activate(&self, wasm: &[u8], revision: u32) -> Manifest {
            self.activate_in(MODULE, wasm, revision)
        }

        fn activate_in(&self, module: &str, wasm: &[u8], revision: u32) -> Manifest {
            let mut manifest = crate::companions::testing::manifest_for(module, wasm);
            manifest.lifecycle.revision = Revision(revision);
            self.storage.activate(&manifest, wasm).unwrap();
            manifest
        }

        fn subscribe(&self, client: &str) {
            self.subscribe_in(MODULE, client);
        }

        fn subscribe_in(&self, module: &str, client: &str) {
            crate::lifecycle::testing::write_registration(
                &self.storage,
                module,
                client,
                "policy_a",
            );
        }

        /// The announcement a worker would make for `wasm` at `revision`,
        /// with the fetcher primed to serve it from that client's hostname.
        fn announcement(&self, client: &str, wasm: &[u8], revision: u32) -> AnnouncedModule {
            self.announcement_in(MODULE, client, wasm, revision)
        }

        fn announcement_in(
            &self,
            module: &str,
            client: &str,
            wasm: &[u8],
            revision: u32,
        ) -> AnnouncedModule {
            let mut manifest = crate::companions::testing::manifest_for(module, wasm);
            manifest.lifecycle.revision = Revision(revision);
            let manifest_bytes = manifest.to_toml().unwrap();
            let announced = AnnouncedModule {
                module: ModuleId::parse(module).unwrap(),
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
                    &announced.module.clone(),
                    announced,
                )
                .await
        }

        fn row(&self, client: &str) -> Option<ClientRow> {
            self.row_in(MODULE, client)
        }

        fn row_in(&self, module: &str, client: &str) -> Option<ClientRow> {
            self.storage
                .read_row(module, &ClientId::parse(client).unwrap())
                .unwrap()
        }

        /// The phase a client's move for a module is parked at.
        fn phase(&self, module: &str, client: &str) -> Option<Phase> {
            match self.row_in(module, client)?.pending?.stage {
                Stage::Planned { phase, .. } => Some(phase),
                Stage::Fetching => None,
            }
        }

        /// Set `module` up with a client on it, boot the lifecycle, and
        /// announce a NEW build — the whole path a worker deploy takes.
        /// Returns the announcement.
        async fn deploy(&self, module: &str, client: &str, wasm: &[u8]) -> AnnouncedModule {
            self.activate_in(module, &component(1), 1);
            self.subscribe_in(module, client);
            self.lifecycle.boot().await;
            let announced = self.announcement_in(module, client, wasm, 2);
            assert_eq!(
                self.announce(client, &announced).await,
                AnnounceResult::Accepted,
                "{module}/{client}",
            );
            announced
        }

        /// Who holds the catch-up slot, and who is waiting, in grant order.
        fn queue(&self) -> (Option<Standing>, Vec<Standing>) {
            let snapshot = self.moves.catch_up();
            (
                snapshot
                    .holder
                    .map(|(c, m)| (c.client.to_string(), m.to_string())),
                snapshot
                    .waiting
                    .into_iter()
                    .map(|q| (q.client.client.to_string(), q.module.to_string()))
                    .collect(),
            )
        }

        /// Announce the build the client is ALREADY on: the machine reads it
        /// as "changed its mind back" and abandons whatever was pending,
        /// which releases the slot.
        async fn give_up(&self, module: &str, client: &str) {
            let current = self.announcement_in(module, client, &component(1), 1);
            assert_eq!(
                self.announce(client, &current).await,
                AnnounceResult::AlreadyCurrent,
            );
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
    async fn a_move_parks_at_catching_up_and_nothing_feeds_caught_up() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let live = h.storage.current_instance(MODULE).unwrap().unwrap();

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        let row = h.row(PROD_CLIENT).unwrap();
        let pending = row.pending.as_ref().expect("a move is pending");
        match pending.stage {
            Stage::Planned { phase, .. } => assert_eq!(
                phase,
                Phase::CatchingUp,
                "this order grants the slot and creates the instance, and stops there",
            ),
            Stage::Fetching => panic!("the artifact should have been fetched"),
        }
        // THE bar for this order: the client is still served by the instance
        // it was on. Nothing feeds `CaughtUp`, so nothing switches.
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&live),
            "the client was switched; nothing in this order may do that",
        );
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
        // `Planned { .. }`, not a specific phase: a refetched move is granted
        // the slot and carries on from there, which is the point of this
        // order. What this test is about is the REFETCH.
        assert!(
            matches!(
                after.pending.as_ref().map(|p| &p.stage),
                Some(Stage::Planned { .. })
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

    // -----------------------------------------------------------------
    // Rebase: a move is planned from where the client REALLY is.
    // -----------------------------------------------------------------

    /// Every operator upload RENAMES the module's current instance, so a row
    /// written before it names a directory that is gone. Planned from that,
    /// the move would hand `EnsureInstance` a PARENT to copy state from that
    /// does not exist.
    #[tokio::test]
    async fn an_upload_between_announcements_rebases_the_row_before_the_next_move() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let k1 = h.storage.current_instance(MODULE).unwrap().unwrap();

        // Adopt the row at K1 by announcing the build it is on.
        h.give_up(MODULE, PROD_CLIENT).await;
        assert_eq!(
            h.row(PROD_CLIENT).unwrap().current.unwrap().instance,
            k1.clone(),
        );

        // An operator uploads K2. K1's directory is renamed away.
        let k2_wasm = component(2);
        let mut k2_manifest = crate::companions::testing::manifest_for(MODULE, &k2_wasm);
        k2_manifest.lifecycle.revision = Revision(3);
        h.lifecycle
            .upload(&k2_manifest, &k2_wasm, None)
            .await
            .expect("upload");
        let k2 = h.storage.current_instance(MODULE).unwrap().unwrap();
        assert_ne!(k2, k1);
        assert!(!h.storage.instance_dir(&k1).is_dir());
        // Something in K2's state, so "the copy came from K2" is a FACT and
        // not an inference. Un-rebased, the move's parent would be K1, whose
        // directory is gone — and a file that is not there carries as zero
        // bytes, so the staging instance would come up silently EMPTY.
        h.storage
            .kv_store_at(&k2, None)
            .unwrap()
            .set_value(MODULE, "marker", b"from K2".to_vec())
            .unwrap();

        // …and now the worker announces K3.
        let k3 = h.announcement(PROD_CLIENT, &component(3), 4);
        assert_eq!(h.announce(PROD_CLIENT, &k3).await, AnnounceResult::Accepted);

        let row = h.row(PROD_CLIENT).unwrap();
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&k2),
            "the row still names an instance the upload renamed away",
        );
        let target = row.pending.as_ref().unwrap().target.instance.clone();
        assert_eq!(
            h.storage
                .kv_store_at(&target, None)
                .unwrap()
                .get_value(MODULE, "marker")
                .ok()
                .as_deref(),
            Some(b"from K2".as_slice()),
            "the staging instance's state did not come from the instance serving the client",
        );
        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));
        assert!(h.failures().is_empty(), "{:?}", h.failures());
    }

    /// `current: None` means FIRST subscribe — no catch-up, no pause, no
    /// drain. A row made before its client was being served says `None` of a
    /// client that now is, and its first move must still be a hand-off.
    #[tokio::test]
    async fn an_unplaced_row_is_rebased_so_its_first_move_is_a_hand_off() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let live = h.storage.current_instance(MODULE).unwrap().unwrap();

        // A row from before the client was attached — what `ClientRow::new`
        // writes, and what every pre-v3 row on disk looks like.
        h.storage
            .write_row(
                MODULE,
                &ClientRow::new(
                    ClientKey {
                        namespace: mitos_lifecycle::ids::Namespace::parse("prod").unwrap(),
                        client: ClientId::parse(PROD_CLIENT).unwrap(),
                    },
                    ModuleId::parse(MODULE).unwrap(),
                ),
            )
            .unwrap();

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        h.announce(PROD_CLIENT, &announced).await;

        let row = h.row(PROD_CLIENT).unwrap();
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&live),
            "the row was not rebased onto the instance already serving it",
        );
        assert_eq!(
            h.phase(MODULE, PROD_CLIENT),
            Some(Phase::CatchingUp),
            "a first-subscribe move would have gone straight to Committing",
        );
    }

    /// The pending plan was resolved from the revision the client was
    /// THOUGHT to be on. It goes, and the instance it was building with it.
    #[tokio::test]
    async fn a_rebase_abandons_the_pending_move_and_its_staging_instance_is_released() {
        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;

        let announced = h.announcement(PROD_CLIENT, &component(2), 2);
        h.announce(PROD_CLIENT, &announced).await;
        let staging = h.row(PROD_CLIENT).unwrap().pending.unwrap().target.instance;
        assert!(h.storage.instance_dir(&staging).is_dir());

        // An upload moves the client out from under the pending move.
        let wasm = component(3);
        let mut manifest = crate::companions::testing::manifest_for(MODULE, &wasm);
        manifest.lifecycle.revision = Revision(4);
        h.lifecycle.upload(&manifest, &wasm, None).await.unwrap();

        // The next announcement rebases first, which abandons the move.
        let next = h.announcement(PROD_CLIENT, &component(4), 5);
        h.announce(PROD_CLIENT, &next).await;

        assert!(
            !h.storage.instance_dir(&staging).is_dir(),
            "the abandoned move's staging instance was left running for ever",
        );
        assert!(
            h.failures().is_empty(),
            "an abandoned move is not a failure: {:?}",
            h.failures(),
        );
    }

    // -----------------------------------------------------------------
    // The host-wide catch-up slot.
    // -----------------------------------------------------------------

    /// One staging follower replays the chain at a time, across the WHOLE
    /// host — the box's binding constraint is memory, not modules.
    #[tokio::test]
    async fn only_one_move_catches_up_at_a_time_across_modules() {
        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;

        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));
        assert_eq!(
            h.phase(OTHER, DEV_CLIENT),
            Some(Phase::Queued),
            "a second module's move caught up at the same time",
        );
        let (holder, waiting) = h.queue();
        assert_eq!(holder, Some((PROD_CLIENT.to_owned(), MODULE.to_owned())));
        assert_eq!(waiting, vec![(DEV_CLIENT.to_owned(), OTHER.to_owned())]);
    }

    /// Production ahead of development, however long development has been
    /// waiting.
    #[tokio::test]
    async fn production_is_granted_ahead_of_development() {
        let h = Harness::new();
        // The holder.
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        // Development queued FIRST…
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;
        // …and production second.
        h.clock.advance(10);
        h.deploy(THIRD, PROD_CLIENT, &component(4)).await;
        assert_eq!(
            h.queue().1,
            vec![
                (PROD_CLIENT.to_owned(), THIRD.to_owned()),
                (DEV_CLIENT.to_owned(), OTHER.to_owned()),
            ],
            "the queue is not in grant order",
        );

        h.give_up(MODULE, PROD_CLIENT).await;

        assert_eq!(
            h.phase(THIRD, PROD_CLIENT),
            Some(Phase::CatchingUp),
            "production did not jump the queue",
        );
        assert_eq!(h.phase(OTHER, DEV_CLIENT), Some(Phase::Queued));
    }

    /// A move can be abandoned between being queued and being granted. The
    /// slot would then have a holder that never releases it — and ONE leaked
    /// slot stops every move on the host.
    #[tokio::test]
    async fn a_grant_that_is_rejected_releases_the_slot_to_the_next_move() {
        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;
        h.clock.advance(10);
        h.deploy(THIRD, DEV_CLIENT, &component(4)).await;
        assert_eq!(
            h.queue().1,
            vec![
                (DEV_CLIENT.to_owned(), OTHER.to_owned()),
                (DEV_CLIENT.to_owned(), THIRD.to_owned()),
            ],
        );

        // The next-in-line's row goes away behind the manager's back — which
        // is all the manager can tell from a move that was abandoned while
        // it waited.
        h.storage
            .remove_row(OTHER, &ClientId::parse(DEV_CLIENT).unwrap())
            .unwrap();

        h.give_up(MODULE, PROD_CLIENT).await;
        h.moves.settle().await;

        assert_eq!(
            h.phase(THIRD, DEV_CLIENT),
            Some(Phase::CatchingUp),
            "the slot was granted to a move that was gone and never came back",
        );
        assert_eq!(h.queue().0, Some((DEV_CLIENT.to_owned(), THIRD.to_owned())),);
    }

    #[tokio::test]
    async fn an_abandoned_holder_frees_the_slot() {
        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;
        assert_eq!(h.phase(OTHER, DEV_CLIENT), Some(Phase::Queued));

        h.give_up(MODULE, PROD_CLIENT).await;

        assert_eq!(
            h.queue().0,
            Some((DEV_CLIENT.to_owned(), OTHER.to_owned())),
            "the abandoned holder's slot was not handed on",
        );
        assert_eq!(h.phase(OTHER, DEV_CLIENT), Some(Phase::CatchingUp));
    }

    /// The slot is in MEMORY. After a restart `ClientRow::recover` returns
    /// every planned move to `Queued` and re-issues its request, so the queue
    /// rebuilds itself from the rows — there is no second copy of "who is
    /// waiting" to disagree with them.
    #[tokio::test]
    async fn boot_rebuilds_the_queue_from_the_rows() {
        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;

        // A second manager over the same storage — a process restart, with an
        // empty slot.
        let restarted = MoveManager::new(
            h.storage.clone(),
            Arc::new(ArtifactPuller::new(h.storage.clone(), h.fetcher.clone())),
            h.auth.clone(),
            h.lifecycle.clone(),
            h.events.clone(),
            h.clock.clock(),
        );
        assert_eq!(restarted.catch_up().holder, None);

        restarted.boot().await;
        restarted.settle().await;

        let snapshot = restarted.catch_up();
        assert!(
            snapshot.holder.is_some(),
            "nothing was granted the slot after a restart",
        );
        assert_eq!(
            snapshot.waiting.len(),
            1,
            "the queue did not rebuild from the rows: {snapshot:?}",
            snapshot = (snapshot.holder, snapshot.waiting),
        );
    }

    /// A grant names ANOTHER row more often than not, and feeding it takes
    /// that row's lock. Two rows releasing the slot to each other INLINE —
    /// each under its own row lock — would deadlock. The timeout is the
    /// assertion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn granting_never_deadlocks_two_rows() {
        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;

        // Both give up AT ONCE: one's release grants the slot to the other,
        // whose own announce is holding the lock that grant needs.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(h.give_up(MODULE, PROD_CLIENT), h.give_up(OTHER, DEV_CLIENT),);
            h.moves.settle().await;
        })
        .await
        .expect("two rows releasing the slot to each other deadlocked");

        let (holder, waiting) = h.queue();
        assert_eq!(holder, None, "a slot was left held by a move that is gone");
        assert!(waiting.is_empty());
    }

    // -----------------------------------------------------------------
    // End to end.
    // -----------------------------------------------------------------

    /// The whole of this order, in one path: an announcement is fetched,
    /// granted the slot, and its instance is CREATED and STARTED — and the
    /// client is still served by the instance it was on, by an instance that
    /// speaks to nobody.
    #[tokio::test]
    async fn an_announcement_creates_a_staging_instance_that_speaks_to_nobody() {
        use mitos_lifecycle::routing::{FanOut, fan_out};

        let h = Harness::new();
        h.deploy(MODULE, PROD_CLIENT, &component(2)).await;
        let live = h.storage.current_instance(MODULE).unwrap().unwrap();
        let row = h.row(PROD_CLIENT).unwrap();
        let staging = row.pending.as_ref().unwrap().target.instance.clone();

        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&live),
            "the client was switched; nothing in this order may do that",
        );
        assert!(
            h.storage.instance_dir(&staging).is_dir(),
            "the staging instance was not created",
        );
        assert_eq!(
            h.lifecycle.instance_at(&staging).unwrap().map(|i| i.phase),
            Some(mitos_lifecycle::instance::Phase::Staging),
        );
        assert!(
            crate::host_v2::ModuleHostHandle::running_instances(&h.host, MODULE)
                .await
                .contains(&staging),
            "the staging instance is not following",
        );
        // …and it delivers to nobody, which is what makes all of the above
        // safe to run beside a live client.
        assert_eq!(
            fan_out(Some(&row), &staging, &live, |_| {
                mitos_lifecycle::routing::InstanceDir::Present
            }),
            FanOut::Skip,
            "the staging instance would deliver to the client it is staging for",
        );

        // A second client's move, for another module, waits its turn.
        h.clock.advance(10);
        h.deploy(OTHER, DEV_CLIENT, &component(3)).await;
        assert_eq!(h.phase(OTHER, DEV_CLIENT), Some(Phase::Queued));
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
