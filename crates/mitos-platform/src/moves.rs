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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use mitos_lifecycle::access::Registry;
use mitos_lifecycle::client_move::{
    AnnounceContext, CatchUpSlot, ClientQueue, ClientRow, Effect, Event, Grant, ModuleRef,
    MoveFailure, Phase, Placement, Progress, QueuedMove, Rejected, Stage, TargetInstance,
    next_catch_up,
};
use mitos_lifecycle::handoff::{self, Alignment, CatchUp};
use mitos_lifecycle::ids::{ClientId, ClientKey, InstanceKey, ModuleId, Slot};
use mitos_lifecycle::instance::DetachCause;
use mitos_lifecycle::upgrade::{MoveIntent, Refill};
use mitos_protocol::{AnnounceResult, AnnouncedModule};

use crate::auth::Authenticator;
use crate::events::{EventKind, EventRing};
use crate::fetch::{Announced, ArtifactPuller, Refusal};
use crate::lifecycle::{Clock, LifecycleManager, Quiesced};
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
    /// Both followers of a switch, stopped, with the module locked.
    ///
    /// `PauseAtBoundary` puts one here and `CommitSwitch`,
    /// `PurgeClientEmissions` and `ResumeFollowers` take it out. All of them
    /// run back-to-back on ONE task inside one `execute_all`: the map exists
    /// because they are separate effects, not because time passes between
    /// them. Anything left in it is a bug the `Quiesced`'s own `Drop`
    /// already recovers from.
    quiesced: std::sync::Mutex<HashMap<RowKey, Quiesced>>,
    /// Moves this process has already explained are waiting for WO-10, so
    /// the reason is said once and not once a minute.
    blocked_noted: std::sync::Mutex<HashSet<String>>,
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
            quiesced: std::sync::Mutex::new(HashMap::new()),
            blocked_noted: std::sync::Mutex::new(HashSet::new()),
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
            Effect::PauseAtBoundary { live, staging } => {
                self.pause_at_boundary(module, client, live, staging).await;
            }
            Effect::PurgeClientEmissions { on } => {
                self.purge_client_emissions(client, on);
            }
            Effect::CommitSwitch { from, to } => {
                self.commit_switch(module, client, from.as_ref(), to).await;
            }
            // Only ever from `recover()`, and the answer is always the same.
            // The commit IS the row write that clears the pending move, so a
            // row that still says `Committing` is a commit that did not
            // land. There is nothing to ask anyone.
            Effect::ResolveCommit => {
                self.feed(module, client, None, Progress::CommitLost).await;
            }
            Effect::ResumeFollowers { live, staging } => {
                self.resume_followers(module, client, live, staging).await;
            }
            // The staging instance already speaks to nobody until the commit,
            // and from the commit on the WATERMARK is what keeps the client
            // from being sent a block twice. Holding this client's emissions
            // on the staging instance and diffing them against the live
            // instance's is the advisory diff (D7) — it is not load-bearing
            // for the switch, and it is not built.
            Effect::HoldAndDiff { .. } | Effect::FinishDiff => {}
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
            // `RunRefill` only: §1 refuses to advance a move whose plan
            // carries one past `CatchingUp`, so nothing can reach the
            // transition that issues it. Loud, but never fatal: a reducer
            // that surprised the host is not a reason to take the host down.
            other => tracing::error!(
                module = %module,
                client = %client,
                effect = ?other,
                "move effect is not reachable until the refill order lands; ignoring",
            ),
        }
    }

    // -----------------------------------------------------------------
    // The switch
    // -----------------------------------------------------------------

    /// `(module, client)` — which row a `Quiesced` belongs to.
    fn quiesce_key(module: &ModuleId, client: &ClientId) -> RowKey {
        (module.to_string(), client.as_str().to_owned())
    }

    /// Stop both followers and keep the module locked until the commit.
    ///
    /// A failure is NOT a failed move: the row stays `Quiescing` and the
    /// tick's `Quiescing` arm re-executes this effect — see `advance`.
    async fn pause_at_boundary(
        &self,
        module: &ModuleId,
        client: &ClientId,
        live: &InstanceKey,
        staging: &InstanceKey,
    ) {
        let Some(lifecycle) = self.me.upgrade().map(|me| me.lifecycle.clone()) else {
            return;
        };
        match lifecycle.quiesce_pair(live, staging).await {
            Ok(quiesced) => {
                let after = quiesced.after();
                // A second `PauseAtBoundary` for a row that already holds one
                // would strand the first — and its `Drop` would resume the
                // followers under the second's feet. Cannot happen (the
                // effects of one transition run on one task) but the map is
                // the place it WOULD happen, so it says so.
                // Bound, so the `std::sync::Mutex` guard is dropped by the
                // end of the statement: holding one across the `.await`
                // below would make this whole future non-`Send`.
                let stranded = self
                    .quiesced
                    .lock()
                    .expect("quiesced mutex")
                    .insert(Self::quiesce_key(module, client), quiesced);
                if let Some(stranded) = stranded {
                    tracing::error!(
                        module = %module,
                        client = %client,
                        "a second quiesce replaced one still held; resuming the first",
                    );
                    stranded.resume().await;
                    return;
                }
                let queue = self.client_queue(client, live);
                tracing::info!(
                    module = %module,
                    client = %client,
                    live = %live.dir_name(),
                    staging = %staging.dir_name(),
                    after = after.0,
                    ?queue,
                    "both followers quiesced; committing the switch",
                );
                // `progress_locked`, not `feed`, because THIS caller has to
                // know whether the row moved. Every transition `Quiesced` can
                // make takes the quiesce back out of the map — `Committing`
                // through `CommitSwitch`, `WaitingForDrain` through
                // `ResumeFollowers`. If the row does NOT move, nothing does:
                // the quiesce is stranded holding the module lock with both
                // followers stopped, and the next tick's `Quiescing` arm
                // deadlocks on that lock inside `quiesce_pair` — taking the
                // whole row tick with it.
                if self
                    .progress_locked(
                        module,
                        client,
                        staging.clone(),
                        Progress::Quiesced { queue },
                    )
                    .await
                    .is_err()
                {
                    self.resume_followers(module, client, live, staging).await;
                }
            }
            Err(e) => tracing::info!(
                module = %module,
                client = %client,
                live = %live.dir_name(),
                staging = %staging.dir_name(),
                error = %e,
                "the switch could not be quiesced; the moves tick's `Quiescing` arm retries it",
            ),
        }
    }

    /// Does this client have anything undelivered on the instance it is
    /// leaving? `Pending` iff it has a `Queued` or `Pending` row there.
    ///
    /// `Pending` means a row the dialer has in flight — it is as undelivered
    /// as a `Queued` one, and the plan's `Drain` is about both.
    fn client_queue(&self, client: &ClientId, live: &InstanceKey) -> ClientQueue {
        use crate::emissions::EmissionStatus;
        let owed = self
            .storage
            .emissions_store_at(live)
            .and_then(|store| {
                store.list_filtered(|row| {
                    row.client_id == client.as_str()
                        && matches!(row.status, EmissionStatus::Queued | EmissionStatus::Pending)
                })
            })
            .map(|rows| !rows.is_empty());
        match owed {
            Ok(false) => ClientQueue::Empty,
            Ok(true) => ClientQueue::Pending,
            // A store that will not open is treated as owing the client
            // something: draining what turns out to be nothing costs a tick,
            // and committing over rows that were there loses them.
            Err(e) => {
                tracing::warn!(
                    client = %client,
                    instance = %live.dir_name(),
                    error = %e,
                    "cannot count what this client is owed; treating it as undrained",
                );
                ClientQueue::Pending
            }
        }
    }

    /// Discard the moving client's undelivered rows on the instance it is
    /// leaving — what `purge-emissions` in the upgrade ledger asks for, when
    /// the old wire shape is not worth delivering.
    ///
    /// Its follower is stopped (the `Quiesced` is held), so nothing is
    /// writing to that store while this runs.
    fn purge_client_emissions(&self, client: &ClientId, on: &InstanceKey) {
        let purged = self
            .storage
            .emissions_store_at(on)
            .and_then(|store| store.purge(|row| row.client_id == client.as_str()));
        match purged {
            Ok(0) => {}
            Ok(count) => tracing::info!(
                client = %client,
                instance = %on.dir_name(),
                count,
                "purged the moving client's undelivered emissions, as its upgrade asked",
            ),
            Err(e) => tracing::warn!(
                client = %client,
                instance = %on.dir_name(),
                error = %e,
                "purging the moving client's emissions failed; they will be delivered",
            ),
        }
    }

    /// THE switch. Two writes, and the SECOND is the commit.
    ///
    /// 1. The watermark on the instance taking over: every block at or before
    ///    the live instance's cursor was delivered by its predecessor, so the
    ///    new one withholds them when it replays from its own (lower) cursor.
    /// 2. `Progress::SwitchCommitted`, whose row `fan_out` reads. Persisting
    ///    that row IS the routing change, at one instant, for every companion
    ///    of the client.
    ///
    /// **The window between them is harmless.** A watermark for a client the
    /// row does not place on this instance is never consulted — `fan_out`
    /// sends that client's events from somewhere else entirely — and the next
    /// commit overwrites it.
    ///
    /// A first subscribe (`from: None`) does only the second write.
    async fn commit_switch(
        &self,
        module: &ModuleId,
        client: &ClientId,
        from: Option<&InstanceKey>,
        to: &Placement,
    ) {
        // A FIRST subscribe (`from: None`) pauses NOTHING and owes no
        // watermark: no instance is serving this client, so no follower was
        // stopped and every block the new instance holds is one nobody
        // delivered. It never enters the `quiesced` map, so its absence there
        // must NOT be read as a lost commit — `Committing` is reached from
        // `Preparing` in one step and `CommitLost` sends it straight back, so
        // that reading is not a slow retry but an endless one.
        let Some(from) = from else {
            // A failure here leaves the row `Committing` and the tick asks
            // again; there is nothing stopped to release.
            let _ = self.commit_row(module, client, to).await;
            return;
        };
        let held = self
            .quiesced
            .lock()
            .expect("quiesced mutex")
            .remove(&Self::quiesce_key(module, client));
        let Some(quiesced) = held else {
            // `from` names an instance that IS serving the client, and its
            // follower is not stopped — the `Quiesced` went with the process
            // that held it. This is the tick re-executing `CommitSwitch`
            // after a restart, or after one of the writes below failed.
            // Committing here would be a routing change over a running
            // instance, which is WO-06's bug. `CommitLost` resumes (a no-op
            // after a restart) and catches up again.
            tracing::info!(
                module = %module,
                client = %client,
                "commit re-issued with no quiesce in hand; the move goes back to catching up",
            );
            self.feed(
                module,
                client,
                Some(to.instance.clone()),
                Progress::CommitLost,
            )
            .await;
            return;
        };

        let mut handoffs = match self.storage.read_handoffs_at(&to.instance) {
            Ok(handoffs) => handoffs,
            Err(e) => {
                // Defaulting would drop ANOTHER client's watermark and
                // re-deliver its blocks. The row is untouched, so the move
                // stays `Committing` and the tick retries.
                tracing::error!(
                    module = %module,
                    client = %client,
                    instance = %to.instance.dir_name(),
                    error = %e,
                    "hand-off watermarks unreadable; the switch is not committed and the \
                     moves tick's `Committing` arm will retry it",
                );
                quiesced.resume().await;
                return;
            }
        };
        handoffs.begin(client.clone(), quiesced.after());
        if let Err(e) = self.storage.write_handoffs_at(&to.instance, &handoffs) {
            tracing::error!(
                module = %module,
                client = %client,
                instance = %to.instance.dir_name(),
                error = %e,
                "writing the hand-off watermark failed; the switch is not committed and \
                 the moves tick's `Committing` arm will retry it",
            );
            quiesced.resume().await;
            return;
        }

        // Back in the map so `ResumeFollowers` — the first effect of the
        // commit below — finds it.
        self.quiesced
            .lock()
            .expect("quiesced mutex")
            .insert(Self::quiesce_key(module, client), quiesced);
        if self.commit_row(module, client, to).await.is_err() {
            // The row did not move, so the commit's `ResumeFollowers` never
            // ran and the `Quiesced` is still in the map — holding the module
            // lock with both followers stopped. Leaving it there would hold
            // them until the write started working; the row still says
            // `Committing`, so the tick re-executes this whole effect.
            self.resume_followers(module, client, from, &to.instance)
                .await;
        }
    }

    /// THE row write, and the succession it causes.
    ///
    /// `progress_locked` persists the row and runs the effects the commit
    /// returns — `ResumeFollowers` first — inside it, so by the time this
    /// returns `Ok` the module lock is free again and `succeed` can take it.
    async fn commit_row(
        &self,
        module: &ModuleId,
        client: &ClientId,
        to: &Placement,
    ) -> Result<(), String> {
        if let Err(e) = self
            .progress_locked(
                module,
                client,
                to.instance.clone(),
                Progress::SwitchCommitted,
            )
            .await
        {
            tracing::error!(
                module = %module,
                client = %client,
                instance = %to.instance.dir_name(),
                rejected = %e,
                "the switch was not committed; the moves tick's `Committing` arm retries it",
            );
            return Err(e);
        }
        // Succession, AFTER the reconcile `AttachTo`/`DetachFrom` did. If the
        // client that just moved was the last one on the module's current
        // instance, `current.toml` follows it — or that instance is refused
        // its reap for ever. The lifecycle tick asks the same question for
        // every module with more than one instance, because this effect is
        // issued once by a transition that clears its own state.
        if let Err(e) = self.lifecycle.succeed(module.as_str(), &to.instance).await {
            tracing::warn!(
                module = %module,
                client = %client,
                error = %e,
                "the succession check failed; the lifecycle tick asks again every minute",
            );
        }
        Ok(())
    }

    /// Release the quiesce. Idempotent: nothing held ⇒ nothing to do.
    async fn resume_followers(
        &self,
        module: &ModuleId,
        client: &ClientId,
        live: &InstanceKey,
        staging: &InstanceKey,
    ) {
        let held = self
            .quiesced
            .lock()
            .expect("quiesced mutex")
            .remove(&Self::quiesce_key(module, client));
        match held {
            Some(quiesced) => quiesced.resume().await,
            None => tracing::debug!(
                module = %module,
                client = %client,
                live = %live.dir_name(),
                staging = %staging.dir_name(),
                "resume with nothing quiesced; both followers are already running",
            ),
        }
    }

    /// Feed one `Progress`, taking the row lock unless the caller has it.
    ///
    /// `target: None` means "whatever this row's pending move targets" —
    /// which is what `ResolveCommit` needs, since the effect carries no key.
    async fn feed(
        &self,
        module: &ModuleId,
        client: &ClientId,
        target: Option<InstanceKey>,
        step: Progress,
    ) {
        let target = match target {
            Some(target) => target,
            None => {
                match self
                    .storage
                    .read_row(module.as_str(), client)
                    .ok()
                    .flatten()
                    .and_then(|row| row.pending.map(|p| p.target.instance))
                {
                    Some(target) => target,
                    None => return,
                }
            }
        };
        if let Err(e) = self.progress_locked(module, client, target, step).await {
            tracing::debug!(
                module = %module,
                client = %client,
                rejected = %e,
                "progress refused; the move has moved on",
            );
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
    /// is then removed. Every OTHER row's pending move is advanced.
    ///
    /// **Every phase of a move past `Preparing` is swept here**, because
    /// every one of them is reached by an effect that is issued ONCE by a
    /// transition whose row is persisted before it runs. `CatchingUp` waits
    /// on the chain, `Quiescing` and `Committing` on work that can
    /// legitimately decline, and `WaitingForDrain` on a companion. Nothing
    /// else asks again.
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
                let client = row.client.client.clone();
                let lock = self.lock_for(&module, &client);
                let _guard = lock.lock().await;
                if attached.contains_key(&row.client) {
                    self.advance(&module_id, &client, row).await;
                } else {
                    self.withdraw(&module_id, &client, row).await;
                }
            }
        }
    }

    /// Carry one pending move to its next phase, if the world is ready.
    ///
    /// **Caller holds the row lock.** The row was read BEFORE the lock, so
    /// every arm re-reads what it acts on through `feed` /
    /// `progress_locked`, which load the row again under the lock.
    async fn advance(&self, module: &ModuleId, client: &ClientId, row: ClientRow) {
        let Some(pending) = row.pending.as_ref() else {
            return;
        };
        let Stage::Planned { plan, phase } = &pending.stage else {
            // `Fetching` — the pull owns it, and `boot` re-issues one that
            // was interrupted.
            return;
        };
        let staging = pending.target.instance.clone();
        // Every phase from here needs the instance the client is being served
        // BY. A row with no `current` is a first subscribe, which the machine
        // commits without any of this.
        let Some(live) = row.current.as_ref().map(|p| p.instance.clone()) else {
            return;
        };

        match phase {
            // The slot queue owns `Queued`; `create_staging`'s task owns
            // `Preparing`, and `boot` re-queues both after a restart.
            Phase::Queued | Phase::Preparing => {}

            Phase::CatchingUp => {
                // A move whose plan carries a refill is NOT advanced.
                // `RunRefill` is issued once, by the transition that clears
                // the pending move — so a crash between the row write and
                // the effect loses it, and a lost `recapture` is a client
                // whose projected state silently never got rebuilt. WO-10
                // makes the debt durable and pays it; until then the move
                // waits, visibly (`GET …/rows` reports it as blocked).
                if plan.refill != Refill::None {
                    self.note_blocked(module, client, &staging, plan.refill);
                    return;
                }
                let Some((live_slot, staging_slot)) = self.cursors(&live, &staging) else {
                    return;
                };
                // `match`, never `!=`: a third answer must arrive as a
                // compile error.
                match handoff::catch_up(live_slot, staging_slot) {
                    CatchUp::StillBehind { by } => {
                        tracing::debug!(
                            module = %module,
                            client = %client,
                            staging = %staging.dir_name(),
                            by,
                            "staging is still behind; nothing to quiesce",
                        );
                        return;
                    }
                    CatchUp::CaughtUp => {}
                }
                if !self.worth_quiescing(module, client, live_slot, staging_slot) {
                    return;
                }
                self.feed(module, client, Some(staging), Progress::CaughtUp)
                    .await;
            }

            // Re-execute `PauseAtBoundary`. **This is the retry** the
            // `quiesce_pair` failure paths are written for: the effect is
            // issued once, and a staging instance that got a block ahead
            // resumes both and comes back here.
            Phase::Quiescing => {
                let Some((live_slot, staging_slot)) = self.cursors(&live, &staging) else {
                    return;
                };
                if !self.worth_quiescing(module, client, live_slot, staging_slot) {
                    return;
                }
                self.execute(module, client, &Effect::PauseAtBoundary { live, staging })
                    .await;
            }

            Phase::WaitingForDrain => {
                // `Quiesced { queue: Pending }` with a `Drain` plan resumed
                // both followers rather than hold them paused while a
                // companion catches up on its reading. So this waits on the
                // DIALER, and there is deliberately no timeout: a client
                // whose companion is down keeps its move — and the host's
                // one catch-up slot — until it recovers. That bound is the
                // client lease's (WO-11).
                match self.client_queue(client, &live) {
                    ClientQueue::Pending => {}
                    ClientQueue::Empty => {
                        self.feed(module, client, Some(staging), Progress::QueueDrained)
                            .await;
                    }
                }
            }

            // Re-execute `CommitSwitch`. With no `Quiesced` in hand it feeds
            // `CommitLost`, which resumes and goes back to catching up — so
            // this arm covers both a commit whose write failed and one
            // interrupted by a restart.
            Phase::Committing => {
                self.execute(
                    module,
                    client,
                    &Effect::CommitSwitch {
                        from: Some(live),
                        to: pending.target.clone(),
                    },
                )
                .await;
            }
        }
    }

    /// Both cursors, or `None` if either is missing or unreadable.
    ///
    /// An instance with no cursor has followed no block, and one whose cursor
    /// will not read might be anywhere. Both are "still behind": the move
    /// waits, which costs a tick, where guessing costs a client its blocks.
    fn cursors(&self, live: &InstanceKey, staging: &InstanceKey) -> Option<(Slot, Slot)> {
        let read = |key: &InstanceKey| match self.storage.read_cursor_at(key) {
            Ok(Some(cursor)) => Some(Slot(cursor.slot())),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    instance = %key.dir_name(),
                    error = %e,
                    "cursor unreadable; treating this move as still behind",
                );
                None
            }
        };
        Some((read(live)?, read(staging)?))
    }

    /// Whether to attempt a quiesce at all.
    ///
    /// EVERY attempt stops the LIVE follower — a production client's — for as
    /// long as its block in flight takes. So the cursors as they stand,
    /// unlocked, must already say a commit is possible. It is a HINT, not the
    /// decision: `quiesce_pair` reads both again with both stopped and is the
    /// only thing that decides. Without it, a staging instance sitting a
    /// block ahead would pause production's follower once a minute for as
    /// long as it stayed there.
    fn worth_quiescing(
        &self,
        module: &ModuleId,
        client: &ClientId,
        live: Slot,
        staging: Slot,
    ) -> bool {
        match handoff::align(live, staging) {
            Alignment::Commit { .. } => true,
            Alignment::StagingAhead => {
                tracing::debug!(
                    module = %module,
                    client = %client,
                    live = live.0,
                    staging = staging.0,
                    "staging is ahead of live; not stopping anything this tick",
                );
                false
            }
        }
    }

    /// Say ONCE per move that it is waiting for the refill order — not once a
    /// minute for as long as it waits.
    fn note_blocked(
        &self,
        module: &ModuleId,
        client: &ClientId,
        staging: &InstanceKey,
        refill: Refill,
    ) {
        let key = format!("{module}/{client}/{}", staging.dir_name());
        if !self
            .blocked_noted
            .lock()
            .expect("blocked_noted mutex")
            .insert(key)
        {
            return;
        }
        tracing::info!(
            module = %module,
            client = %client,
            staging = %staging.dir_name(),
            ?refill,
            "this move's upgrade needs a refill, which the host cannot yet make durable; the \
             move waits at catching-up and the client stays on the instance it is on",
        );
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
            self.announcement_declaring(module, client, wasm, revision, &[])
        }

        /// As `announcement_in`, with an `[[lifecycle.upgrade]]` ledger — the
        /// only way a move's `ClientPlan` comes out as anything but `QUIET`.
        fn announcement_declaring(
            &self,
            module: &str,
            client: &str,
            wasm: &[u8],
            revision: u32,
            upgrade: &[mitos_lifecycle::upgrade::UpgradeEntry],
        ) -> AnnouncedModule {
            let mut manifest = crate::companions::testing::manifest_for(module, wasm);
            manifest.lifecycle.revision = Revision(revision);
            manifest.lifecycle.upgrade = upgrade.to_vec();
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

    /// A move whose upgrade ledger asks for a `recapture` is the one kind
    /// this order still parks. `RunRefill` is issued once, by the transition
    /// that clears the pending move, so a crash between the row write and the
    /// effect loses it — and a lost recapture is a client whose projected
    /// state silently never got rebuilt. WO-10 makes the debt durable; until
    /// then the move waits and the client stays where it is.
    #[tokio::test]
    async fn a_move_with_a_refill_waits_at_catching_up_until_the_refill_order_lands() {
        use mitos_lifecycle::upgrade::{UpgradeAction, UpgradeEntry};

        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let live = h.storage.current_instance(MODULE).unwrap().unwrap();

        let announced = h.announcement_declaring(
            MODULE,
            PROD_CLIENT,
            &component(2),
            2,
            &[UpgradeEntry {
                since: Revision(2),
                actions: [UpgradeAction::Recapture].into_iter().collect(),
            }],
        );
        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );
        // The tick is what carries every other move out of `CatchingUp`, so
        // it has to run for this to be about the refill.
        h.moves.tick().await;
        h.moves.settle().await;

        let row = h.row(PROD_CLIENT).unwrap();
        let pending = row.pending.as_ref().expect("a move is pending");
        match pending.stage {
            Stage::Planned { phase, .. } => assert_eq!(
                phase,
                Phase::CatchingUp,
                "a move carrying a refill may not advance past catching up",
            ),
            Stage::Fetching => panic!("the artifact should have been fetched"),
        }
        // THE bar: the client is still served by the instance it was on.
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&live),
            "the client was switched; a move whose refill cannot run may not switch",
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

    // -----------------------------------------------------------------
    // The switch
    // -----------------------------------------------------------------

    /// Move `key`'s cursor to `slot`. Every test below is about the two
    /// cursors and nothing else, so they are written by hand rather than
    /// driven through a chain.
    fn cursor(h: &Harness, key: &InstanceKey, slot: u64) {
        h.storage
            .write_cursor_at(key, &mitos_data_plane::ChainPoint::Slot(slot))
            .expect("cursor");
    }

    /// `deploy`, plus the two cursors that decide whether the move commits.
    /// Returns `(live, staging)`.
    async fn a_move_at_catching_up(
        h: &Harness,
        module: &str,
        client: &str,
        live_slot: u64,
        staging_slot: u64,
    ) -> (InstanceKey, InstanceKey) {
        h.deploy(module, client, &component(2)).await;
        assert_eq!(
            h.phase(module, client),
            Some(Phase::CatchingUp),
            "precondition: the deploy must park at catching up",
        );
        let row = h.row_in(module, client).unwrap();
        let live = row.current.expect("the client is being served").instance;
        let staging = row.pending.expect("a move is pending").target.instance;
        cursor(h, &live, live_slot);
        cursor(h, &staging, staging_slot);
        h.host.clear();
        (live, staging)
    }

    /// The whole order, end to end: two cursors that agree, one tick, and
    /// the client is served by the instance it announced.
    #[tokio::test]
    async fn a_caught_up_move_commits_and_the_client_is_served_by_the_new_instance() {
        let h = Harness::new();
        let (_live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;
        // A second move, queued behind this one. The slot is host-wide, so
        // "the commit released it" is only worth asserting if somebody was
        // waiting for it.
        h.clock.advance(1);
        h.deploy(OTHER, PROD_CLIENT, &component(3)).await;
        assert_eq!(
            h.queue().0,
            Some((PROD_CLIENT.to_owned(), MODULE.to_owned())),
            "precondition: the move under test holds the slot",
        );

        h.moves.tick().await;
        h.moves.settle().await;

        let row = h.row(PROD_CLIENT).expect("the row survives the commit");
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&staging),
            "the client is still on the instance it was leaving",
        );
        assert!(row.pending.is_none(), "the move is not finished: {row:?}");
        assert_eq!(
            h.queue().0,
            Some((PROD_CLIENT.to_owned(), OTHER.to_owned())),
            "the commit must release the catch-up slot to whoever is next",
        );
        // The watermark the new instance took over at — `live`'s cursor.
        assert_eq!(
            h.storage
                .read_handoffs_at(&staging)
                .unwrap()
                .watermark(&ClientId::parse(PROD_CLIENT).unwrap()),
            Some(Slot(1_000)),
        );
        assert!(h.failures().is_empty(), "{:?}", h.failures());
    }

    /// The ordinary case for as long as a catch-up takes: nothing to do, and
    /// — the point — nothing STOPPED. Every quiesce attempt pauses a
    /// production client's follower.
    #[tokio::test]
    async fn a_staging_instance_still_behind_is_left_alone() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        let (_live, _staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 900).await;

        h.moves.tick().await;
        h.moves.settle().await;

        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));
        assert!(
            !h.host
                .calls()
                .iter()
                .any(|c| matches!(c, HostCall::StopInstance(_))),
            "a move that cannot commit must stop nothing: {:?}",
            h.host.calls(),
        );
    }

    /// The unlocked cursors are only a HINT. Between the tick's read and the
    /// stops, the staging follower can finish a block its predecessor never
    /// reached — and then the commit would lose `(live, staging]` for every
    /// OTHER client the old instance still serves. `quiesce_pair` refuses,
    /// and the move stays where it is until live catches up.
    #[tokio::test]
    async fn staging_ahead_leaves_the_move_quiescing_and_the_next_tick_retries_it() {
        let h = Harness::new();
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;

        // The staging follower applies one more block while it is being
        // stopped — the race the lock cannot close, because the stops
        // themselves take time.
        let fires = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let storage = h.storage.clone();
        let ahead = staging.clone();
        let counter = fires.clone();
        h.host.on_stop(move || {
            if counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                storage
                    .write_cursor_at(&ahead, &mitos_data_plane::ChainPoint::Slot(1_001))
                    .expect("cursor");
            }
        });

        h.moves.tick().await;
        h.moves.settle().await;

        assert_eq!(
            h.phase(MODULE, PROD_CLIENT),
            Some(Phase::Quiescing),
            "a refused quiesce is not a failed move",
        );
        assert_eq!(
            h.row(PROD_CLIENT).unwrap().current.map(|p| p.instance),
            Some(live.clone()),
            "nothing may switch while staging is ahead",
        );

        // Live catches up and passes it — which is the only way out, since a
        // cursor never goes backwards except on a rollback.
        cursor(&h, &live, 1_001);
        h.moves.tick().await;
        h.moves.settle().await;

        let row = h.row(PROD_CLIENT).unwrap();
        assert!(row.pending.is_none(), "the retry did not commit: {row:?}");
        assert_eq!(row.current.map(|p| p.instance), Some(staging));
    }

    /// …and while the cursors as they stand say so, the tick does not even
    /// try. Without this, a staging instance sitting a block ahead would
    /// pause a production follower once a minute for as long as it stayed
    /// there.
    #[tokio::test]
    async fn a_quiesce_is_not_attempted_while_the_running_cursors_say_staging_is_ahead() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        let (_live, _staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_001).await;

        for _ in 0..3 {
            h.moves.tick().await;
            h.moves.settle().await;
        }

        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));
        assert!(
            !h.host
                .calls()
                .iter()
                .any(|c| matches!(c, HostCall::StopInstance(_))),
            "three ticks stopped a follower for a commit that could not happen: {:?}",
            h.host.calls(),
        );
    }

    /// Queue one emission for `client` on `on`, as the fan-out would.
    fn owe_the_client_one(h: &Harness, on: &InstanceKey, client: &str) {
        h.storage
            .emissions_store_at(on)
            .unwrap()
            .append(
                "policy_a",
                client,
                "1",
                mitos_protocol::ChainPoint::Slot(999),
                b"payload".to_vec(),
                Vec::new(),
                crate::emissions::EmissionStatus::Queued,
                "2026-09-20T00:00:00Z",
            )
            .unwrap();
    }

    /// What `client` is still owed on `on`.
    fn owed(h: &Harness, on: &InstanceKey, client: &str) -> usize {
        h.storage
            .emissions_store_at(on)
            .unwrap()
            .list_filtered(|row| row.client_id == client)
            .unwrap()
            .len()
    }

    /// The default hand-off. A client with a block still undelivered on the
    /// instance it is leaving must be given it there — and the followers are
    /// RESUMED while it reads, rather than held paused waiting on a companion
    /// that may be down.
    #[tokio::test]
    async fn a_client_with_rows_still_queued_drains_before_it_switches() {
        let h = Harness::new();
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;
        owe_the_client_one(&h, &live, PROD_CLIENT);

        h.moves.tick().await;
        h.moves.settle().await;

        assert_eq!(
            h.phase(MODULE, PROD_CLIENT),
            Some(Phase::WaitingForDrain),
            "a client still owed a block must not be switched away from it",
        );
        assert_eq!(
            h.row(PROD_CLIENT).unwrap().current.map(|p| p.instance),
            Some(live.clone()),
        );
        assert_eq!(owed(&h, &live, PROD_CLIENT), 1, "nothing may be discarded");

        // The dialer delivers it.
        h.storage
            .emissions_store_at(&live)
            .unwrap()
            .purge(|row| row.client_id == PROD_CLIENT)
            .unwrap();
        h.moves.tick().await;
        h.moves.settle().await;

        let row = h.row(PROD_CLIENT).unwrap();
        assert!(
            row.pending.is_none(),
            "the drained move did not commit: {row:?}"
        );
        assert_eq!(row.current.map(|p| p.instance), Some(staging));
    }

    /// …and `purge-emissions` in the upgrade ledger is how an author says the
    /// old wire shape is not worth delivering. Then there is nothing to wait
    /// for and the switch happens in the same quiesce.
    #[tokio::test]
    async fn a_client_with_rows_still_queued_has_them_purged_when_the_plan_says_so() {
        use mitos_lifecycle::upgrade::{UpgradeAction, UpgradeEntry};

        let h = Harness::new();
        h.activate(&component(1), 1);
        h.subscribe(PROD_CLIENT);
        h.lifecycle.boot().await;
        let announced = h.announcement_declaring(
            MODULE,
            PROD_CLIENT,
            &component(2),
            2,
            &[UpgradeEntry {
                since: Revision(2),
                actions: [UpgradeAction::PurgeEmissions].into_iter().collect(),
            }],
        );
        assert_eq!(
            h.announce(PROD_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );
        let row = h.row(PROD_CLIENT).unwrap();
        let live = row.current.unwrap().instance;
        let staging = row.pending.unwrap().target.instance;
        cursor(&h, &live, 1_000);
        cursor(&h, &staging, 1_000);
        owe_the_client_one(&h, &live, PROD_CLIENT);

        h.moves.tick().await;
        h.moves.settle().await;

        let row = h.row(PROD_CLIENT).unwrap();
        assert!(
            row.pending.is_none(),
            "a purging move has nothing to wait for: {row:?}",
        );
        assert_eq!(row.current.map(|p| p.instance), Some(staging));
        assert_eq!(
            owed(&h, &live, PROD_CLIENT),
            0,
            "the plan said purge; the rows are still there",
        );
    }

    /// A second set of managers over the same storage, with a host that has
    /// nothing running — a process restart.
    struct Restarted {
        lifecycle: Arc<LifecycleManager>,
        moves: Arc<MoveManager>,
        host: FakeHost,
    }

    impl Harness {
        fn restart(&self) -> Restarted {
            let host = FakeHost::new(self.storage.clone());
            let lifecycle = Arc::new(LifecycleManager::new(
                self.storage.clone(),
                Arc::new(host.clone()),
                self.auth.clone(),
                self.events.clone(),
                self.clock.clock(),
            ));
            let moves = MoveManager::new(
                self.storage.clone(),
                Arc::new(ArtifactPuller::new(
                    self.storage.clone(),
                    self.fetcher.clone(),
                )),
                self.auth.clone(),
                lifecycle.clone(),
                self.events.clone(),
                self.clock.clock(),
            );
            Restarted {
                lifecycle,
                moves,
                host,
            }
        }
    }

    /// The window between the two writes, from the wrong side: the watermark
    /// is on disk and the row is not.
    ///
    /// **Modelled by state, not by an injected failure.** The two writes are
    /// microseconds apart with nothing between them that a test can hook, and
    /// every coarser injection — a read-only rows directory — fails the row
    /// writes BEFORE the watermark too, which is a different window
    /// (`a_row_write_that_fails_after_the_stops_does_not_strand_the_quiesce`
    /// is that one). So this writes the orphan watermark by hand, which is
    /// exactly what a crash in that window leaves behind.
    #[tokio::test]
    async fn a_crash_between_the_watermark_and_the_row_commits_nothing() {
        let h = Harness::new();
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;

        let mut handoffs = h.storage.read_handoffs_at(&staging).unwrap();
        handoffs.begin(ClientId::parse(PROD_CLIENT).unwrap(), Slot(1_000));
        h.storage.write_handoffs_at(&staging, &handoffs).unwrap();

        // The row is what `fan_out` reads, so nothing has switched — and the
        // orphan watermark changes nothing, because a watermark for a client
        // the row does not place on that instance is never consulted.
        let row = h.row(PROD_CLIENT).unwrap();
        assert_eq!(
            row.current.as_ref().map(|p| &p.instance),
            Some(&live),
            "the row is the switch; it did not happen",
        );
        assert_eq!(h.phase(MODULE, PROD_CLIENT), Some(Phase::CatchingUp));

        // Boot, and the move catches up and commits again — over the top of
        // the orphan watermark, at whatever live's cursor now is.
        cursor(&h, &live, 1_100);
        cursor(&h, &staging, 1_100);
        let restarted = h.restart();
        restarted.lifecycle.boot().await;
        restarted.moves.boot().await;
        restarted.moves.settle().await;
        restarted.moves.tick().await;
        restarted.moves.settle().await;

        let row = h.row(PROD_CLIENT).unwrap();
        assert!(
            row.pending.is_none(),
            "the move did not commit after boot: {row:?}",
        );
        assert_eq!(row.current.map(|p| p.instance), Some(staging.clone()));
        assert_eq!(
            h.storage
                .read_handoffs_at(&staging)
                .unwrap()
                .watermark(&ClientId::parse(PROD_CLIENT).unwrap()),
            Some(Slot(1_100)),
            "the commit must overwrite the watermark the crash left",
        );
    }

    /// Every transition `Progress::Quiesced` can make takes the quiesce back
    /// out of the map. If the row write that carries it fails, NOTHING does —
    /// and a stranded quiesce holds the module lock with both followers
    /// stopped, so the next tick's `Quiescing` arm blocks on that lock inside
    /// `quiesce_pair`, holding the row lock, taking the whole row tick with
    /// it. **The timeout is the assertion.**
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_row_write_that_fails_after_the_stops_does_not_strand_the_quiesce() {
        use crate::host_v2::testing::HostCall;
        use std::os::unix::fs::PermissionsExt;

        let h = Harness::new();
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;

        // Injected on the DIRECTORY, because `write_row` writes a temporary
        // file and renames — there is no file to make read-only. Flipped from
        // inside `stop_instance` so the writes BEFORE the stops still land
        // and the move really does reach the quiesce.
        let rows_dir = h.storage.module_dir_for_rows(MODULE);
        let restore = std::fs::metadata(&rows_dir).unwrap().permissions();
        let readonly = rows_dir.clone();
        h.host.on_stop(move || {
            std::fs::set_permissions(&readonly, std::fs::Permissions::from_mode(0o500))
                .expect("chmod");
        });

        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for _ in 0..3 {
                h.moves.tick().await;
                h.moves.settle().await;
            }
        })
        .await
        .expect("a stranded quiesce deadlocked the row tick");

        std::fs::set_permissions(&rows_dir, restore).unwrap();
        // Nothing switched, and neither follower was left stopped.
        assert_eq!(
            h.row(PROD_CLIENT).unwrap().current.map(|p| p.instance),
            Some(live.clone()),
        );
        let calls = h.host.calls();
        for key in [&live, &staging] {
            assert!(
                matches!(
                    calls.iter().rev().find(|c| matches!(
                        c,
                        HostCall::StopInstance(k) | HostCall::StartInstance(k) if k == key
                    )),
                    Some(HostCall::StartInstance(_))
                ),
                "{} was left stopped: {calls:?}",
                key.dir_name(),
            );
        }
    }

    /// The other side of that window: the row IS written, so the switch
    /// happened, and both followers are stopped because the resume was the
    /// next thing the dead process was going to do. Boot is what brings them
    /// back — and must not re-open a move the row says is finished.
    #[tokio::test]
    async fn a_crash_after_the_row_leaves_both_followers_restarted_by_boot() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        // TWO clients on the live instance, so it is still serving somebody
        // after the move and boot has two followers to bring back.
        h.subscribe(DEV_CLIENT);
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;
        h.moves.tick().await;
        h.moves.settle().await;
        assert!(
            h.row(PROD_CLIENT).unwrap().pending.is_none(),
            "precondition"
        );

        let restarted = h.restart();
        restarted.lifecycle.boot().await;
        restarted.moves.boot().await;
        restarted.moves.settle().await;

        let started: Vec<_> = restarted
            .host
            .calls()
            .into_iter()
            .filter_map(|c| match c {
                HostCall::StartInstance(key) => Some(key),
                _ => None,
            })
            .collect();
        assert!(
            started.contains(&live) && started.contains(&staging),
            "boot must bring BOTH followers back: {started:?}",
        );
        let row = h.row(PROD_CLIENT).unwrap();
        assert!(
            row.pending.is_none(),
            "boot re-opened a move the row says is finished: {row:?}",
        );
        assert_eq!(row.current.map(|p| p.instance), Some(staging));
    }

    /// There is no such thing as mid-quiesce for an announcement: the tick
    /// holds the ROW lock from the stops to the resume, and `announce` takes
    /// the same lock. So an announcement that arrives while the followers are
    /// stopped WAITS, and finds them running.
    ///
    /// That is the whole reason the `Quiesced` can live in a map keyed by row
    /// rather than in the row itself. Without the row lock, this would
    /// abandon the move that is holding two followers stopped and strand
    /// them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_newer_announcement_mid_quiesce_resumes_both_followers() {
        use crate::host_v2::testing::HostCall;

        let h = Harness::new();
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;
        let newer = h.announcement(PROD_CLIENT, &component(3), 3);

        // Fired from inside `stop_instance`, so the announcement below is
        // issued with both followers down. The sleep holds that window open
        // long enough for it to be issued and to block on the row lock.
        let quiescing = Arc::new(tokio::sync::Notify::new());
        let signal = quiescing.clone();
        h.host.on_stop(move || {
            signal.notify_one();
            std::thread::sleep(std::time::Duration::from_millis(50));
        });

        let moves = h.moves.clone();
        let client = ClientId::parse(PROD_CLIENT).unwrap();
        let module = ModuleId::parse(MODULE).unwrap();
        let announcing = tokio::spawn(async move {
            quiescing.notified().await;
            moves.announce(&client, &module, &newer).await
        });

        h.moves.tick().await;
        h.moves.settle().await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), announcing)
            .await
            .expect("the announcement never got the row lock")
            .unwrap();
        assert_eq!(result, AnnounceResult::Accepted);
        h.moves.settle().await;

        // Both followers were restarted, and neither was left stopped.
        let calls = h.host.calls();
        let last_of = |key: &InstanceKey| {
            calls
                .iter()
                .rev()
                .find(|c| {
                    matches!(c, HostCall::StopInstance(k) | HostCall::StartInstance(k) if k == key)
                })
                .cloned()
        };
        assert!(
            matches!(last_of(&live), Some(HostCall::StartInstance(_))),
            "the live follower was left stopped: {calls:?}",
        );
        assert!(
            matches!(last_of(&staging), Some(HostCall::StartInstance(_))),
            "the staging follower was left stopped: {calls:?}",
        );
    }

    /// Two clients moving to the SAME instance. The second commit must read
    /// the watermark file before writing it, or it would drop the first
    /// client's watermark and re-deliver every block below it.
    #[tokio::test]
    async fn two_clients_moving_to_one_instance_commit_one_after_the_other() {
        let h = Harness::new();
        h.subscribe(DEV_CLIENT);
        let (live, staging) = a_move_at_catching_up(&h, MODULE, PROD_CLIENT, 1_000, 1_000).await;

        // The same build, from the other client — one staging instance, two
        // rows moving onto it. The catch-up slot is host-wide, so they cannot
        // both be catching up at once.
        h.clock.advance(1);
        let announced = h.announcement(DEV_CLIENT, &component(2), 2);
        assert_eq!(
            h.announce(DEV_CLIENT, &announced).await,
            AnnounceResult::Accepted
        );

        // Two ticks: one commit each, the second only once the first has let
        // the slot go.
        for _ in 0..2 {
            cursor(&h, &live, 1_000);
            cursor(&h, &staging, 1_000);
            h.moves.tick().await;
            h.moves.settle().await;
        }

        for client in [PROD_CLIENT, DEV_CLIENT] {
            let row = h.row_in(MODULE, client).unwrap();
            assert!(row.pending.is_none(), "{client} did not commit: {row:?}");
            assert_eq!(row.current.map(|p| p.instance), Some(staging.clone()));
        }
        let handoffs = h.storage.read_handoffs_at(&staging).unwrap();
        for client in [PROD_CLIENT, DEV_CLIENT] {
            assert_eq!(
                handoffs.watermark(&ClientId::parse(client).unwrap()),
                Some(Slot(1_000)),
                "{client}'s watermark was dropped by the other commit",
            );
        }
    }
}
