//! The client move — D4. The centre of platform v3.
//!
//! The unit of versioning is the CLIENT ROW — `(namespace, client, module)` →
//! which instance, at which revision. Not the subscription: a worker deploy is
//! atomic across its companion DOs but they re-subscribe lazily, and ~91
//! `collection-ownership` DOs each carrying their own digest would straggle
//! across every build of an afternoon. The FIRST subscribe announcing a new
//! digest moves the row, and every companion of that client moves with it.
//!
//! So there is no host-level "cutover". A deploy is one row moving from
//! instance A to instance B; when the last row leaves A, A is superseded
//! ([`crate::instance::ZeroRefCause`]). Two clients on different digests are
//! simply two rows pointing at two instances — fork on divergence — and they
//! collapse back the same way.
//!
//! ```text
//! Announce ─► Fetching ─► Queued ─► Preparing ─► CatchingUp ─► Quiescing ─► Committing ─► (moved)
//!                                       │                         │  ▲
//!                       (first subscribe: nothing to hand off) ───┘  │
//!                                                     WaitingForDrain ┘
//! ```
//!
//! # How the switch is made safe
//!
//! The invariant: **a client never receives emissions from two instances for
//! the same block.** The switch is committed with BOTH followers paused at the
//! same block boundary and the client's queue on the old instance empty. At
//! that instant "everything up to here came from A, everything after comes
//! from B" is exactly true, and the commit — row update plus delivery routing
//! — is one atomic write.
//!
//! This deliberately does NOT pick a switch slot ahead of time. A slot chosen
//! in advance can be invalidated by a rollback before it is reached, and the
//! machine would have to reason about a reorg crossing it. Committing at a
//! quiesced boundary means a rollback BEFORE the commit is ordinary processing
//! for both instances, and one AFTER it is B's alone — B followed the same
//! chain, so its rollback emissions are the ones the client needs. There is no
//! "during".
//!
//! # Rules the host must honour
//!
//! - Every `Progress` carries the target it is about. A move can be abandoned
//!   while a fetch is in flight; its late answer is [`Rejected::Stale`].
//! - Catch-up slots are in-memory. After a crash, [`ClientRow::recover`] sends
//!   every uncommitted move back to `Queued`. The staging instance itself is
//!   durable and simply carries on from its cursor.
//! - `CommitSwitch` is the one effect that is not naturally idempotent, so it
//!   has its own recovery: [`Effect::ResolveCommit`] asks the host whether the
//!   atomic write landed.

use serde::{Deserialize, Serialize};

use crate::ids::{ClientKey, InstanceKey, ModuleId, Revision, Sha256Hex};
use crate::instance::DetachCause;
use crate::policy::{RevisionPolicy, Tier};
use crate::upgrade::{
    ClientPlan, EmissionHandoff, MoveIntent, Refill, UpgradeEntry, UpgradeRefusal, resolve_client,
};
use crate::{Transition, UnixSecs};

/// What a worker announces on subscribe: the artifact it was BUILT against.
/// The wasm is public; this pointer is what is authenticated. (The wire form
/// carries the config BYTES; by the time it reaches here they are a digest.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleRef {
    pub url: String,
    pub wasm: Sha256Hex,
    /// The MANIFEST's digest, pinned alongside the wasm's. The wasm sha pins
    /// the code, but the manifest carries the upgrade actions and the interest
    /// set, and it is fetched from the same public place — a host that took an
    /// unpinned manifest could be handed one with `recapture` quietly removed.
    /// NOT part of instance identity: two manifests over one wasm and config
    /// run the same code on the same state.
    pub manifest: Sha256Hex,
    pub config: Option<Sha256Hex>,
    /// Also IN the manifest. Announced here because the rollback check runs
    /// before anything is fetched; the host must refuse an artifact whose
    /// manifest disagrees with the revision that was announced for it.
    pub revision: Revision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Placement {
    pub instance: InstanceKey,
    pub revision: Revision,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Phase {
    /// Waiting for the host-wide catch-up slot. One at a time, production
    /// first — see [`next_catch_up`].
    Queued,
    /// The target instance is being ensured: created from the parent with its
    /// state prepared ([`crate::state_prep`]), or found already running.
    Preparing,
    /// Target following toward tip, this client's emissions on it HELD and
    /// diffed against the old instance's (D7 — advisory, never blocking).
    CatchingUp,
    /// Both followers asked to pause at the next block boundary.
    Quiescing,
    /// Paused, but the old instance still owed this client emissions and the
    /// plan says drain. Followers resumed; waiting for the queue to empty.
    WaitingForDrain,
    /// The atomic switch has been requested.
    Committing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "stage")]
pub enum Stage {
    /// The artifact (and so its `[[upgrade]]` ledger) is not here yet, so
    /// there is no plan. A type, not an `Option`, so no later phase can exist
    /// without one.
    Fetching,
    Planned {
        plan: ClientPlan,
        phase: Phase,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Move {
    pub target: Placement,
    pub url: String,
    pub intent: MoveIntent,
    pub revision_policy: RevisionPolicy,
    pub stage: Stage,
}

/// `(namespace, client, module)` → where it is, and where it is going.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRow {
    pub client: ClientKey,
    pub module: ModuleId,
    /// `None` until the first move commits.
    pub current: Option<Placement>,
    /// At most ONE. A newer announce abandons it — never queues behind it.
    pub pending: Option<Move>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetInstance {
    Exists,
    Missing,
}

/// What the host knows at announce time that this crate cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnnounceContext {
    pub revision_policy: RevisionPolicy,
    pub target: TargetInstance,
    /// Live (non-reaped) instances of this module, and the configured cap. A
    /// fork past the cap is REFUSED with a reason rather than silently costing
    /// another follower's worth of dispatch.
    pub live_instances: usize,
    pub max_instances: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientQueue {
    Empty,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MoveFailure {
    /// Fetch failed, the sha did not match, or the manifest was refused by the
    /// host's ABI gate.
    ArtifactRefused,
    /// State preparation or the staging follower failed.
    StagingFailed,
    RollbackNotPermitted,
}

/// Something the host observed about the move toward `target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    ArtifactReady {
        upgrades: Vec<UpgradeEntry>,
    },
    SlotGranted,
    InstanceReady,
    CaughtUp,
    Quiesced {
        queue: ClientQueue,
    },
    QueueDrained,
    SwitchCommitted,
    /// Answer to [`Effect::ResolveCommit`]: the atomic write did NOT land.
    CommitLost,
    Failed(MoveFailure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Announce {
        module_ref: ModuleRef,
        intent: MoveIntent,
        context: AnnounceContext,
    },
    Progress {
        target: InstanceKey,
        step: Progress,
    },
    /// The client is leaving this module altogether.
    Withdraw {
        cause: DetachCause,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MoveAlert {
    Failed(MoveFailure),
}

/// All idempotent except `CommitSwitch`, which has `ResolveCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Make the artifact present in the CAS: already there ⇒ answer at once;
    /// otherwise fetch, verify the sha, validate against the host.
    EnsureArtifact {
        url: String,
        wasm: Sha256Hex,
    },
    RequestCatchUpSlot,
    ReleaseCatchUpSlot,
    /// Make the target instance exist and follow. `parent` is where its state
    /// comes from; `None` on a first subscribe.
    EnsureInstance {
        key: InstanceKey,
        parent: Option<InstanceKey>,
    },
    /// Hold this client's emissions on `staging` and diff them against
    /// `live`'s over the same blocks.
    HoldAndDiff {
        staging: InstanceKey,
        live: InstanceKey,
    },
    PauseAtBoundary {
        live: InstanceKey,
        staging: InstanceKey,
    },
    ResumeFollowers {
        live: InstanceKey,
        staging: InstanceKey,
    },
    PurgeClientEmissions {
        on: InstanceKey,
    },
    /// THE atomic write: the row now points at `to`, and delivery routing for
    /// every companion of this client follows it.
    CommitSwitch {
        from: Option<InstanceKey>,
        to: Placement,
    },
    /// After a crash in `Committing`: did the write land? Answer with
    /// `SwitchCommitted` or `CommitLost`.
    ResolveCommit,
    /// Feed [`crate::instance::Event::Attach`] to this instance.
    AttachTo(InstanceKey),
    /// Feed [`crate::instance::Event::Detach`] to this instance.
    DetachFrom {
        instance: InstanceKey,
        cause: DetachCause,
    },
    RunRefill(Refill),
    /// Close the diff, journal its summary, alert if non-empty under a
    /// production client.
    FinishDiff,
    /// This move no longer wants `0`. If it is still `Staging` and no other
    /// move does either, feed it [`crate::instance::Event::StagingReleased`].
    ReleaseStaging(InstanceKey),
    Alert(MoveAlert),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    #[error(transparent)]
    Upgrade(#[from] UpgradeRefusal),
    #[error("module already has {live} live instances (cap {max}); refusing to fork another")]
    ForkLimit { live: usize, max: usize },
    #[error("a switch is being committed; retry")]
    CommitInFlight,
    #[error("progress for a move that is no longer pending")]
    Stale,
    #[error("{step} is not valid while the move is {stage}")]
    OutOfOrder { step: String, stage: String },
    #[error("nothing to withdraw")]
    NothingToWithdraw,
}

type Step = Result<Transition<ClientRow, Effect>, Rejected>;

impl ModuleRef {
    pub fn placement(&self, module: &ModuleId) -> Placement {
        Placement {
            instance: InstanceKey {
                module: module.clone(),
                wasm: self.wasm.clone(),
                config: self.config.clone(),
            },
            revision: self.revision,
        }
    }
}

impl Move {
    fn committing(&self) -> bool {
        matches!(
            self.stage,
            Stage::Planned {
                phase: Phase::Committing,
                ..
            }
        )
    }
}

impl ClientRow {
    pub fn new(client: ClientKey, module: ModuleId) -> Self {
        Self {
            client,
            module,
            current: None,
            pending: None,
        }
    }

    /// The row for a client that is ALREADY being served by `placement` but
    /// has never announced anything — every client there is, the day a host
    /// first understands announcements.
    ///
    /// Starting such a client at `current: None` would be a lie with
    /// consequences: the machine treats `None` as a FIRST subscribe, which
    /// skips catch-up, the pause and the drain and simply attaches — so the
    /// client's first announcement would switch it to the new instance with
    /// no hand-off at all, dropping whatever the old one still owed it. It is
    /// on an instance, so the row says so.
    pub fn adopt(client: ClientKey, module: ModuleId, placement: Placement) -> Self {
        Self {
            client,
            module,
            current: Some(placement),
            pending: None,
        }
    }

    pub fn step(&self, event: Event) -> Step {
        match event {
            Event::Announce {
                module_ref,
                intent,
                context,
            } => self.announce(module_ref, intent, context),
            Event::Progress { target, step } => self.progress(target, step),
            Event::Withdraw { cause } => self.withdraw(cause),
        }
    }

    /// The effects to re-issue for a row loaded from disk after a restart.
    pub fn recover(&self) -> Transition<ClientRow, Effect> {
        let Some(pending) = &self.pending else {
            return Transition::quiet(self.clone());
        };
        let mut next = self.clone();
        let effects = match &pending.stage {
            Stage::Fetching => vec![Effect::EnsureArtifact {
                url: pending.url.clone(),
                wasm: pending.target.instance.wasm.clone(),
            }],
            Stage::Planned {
                phase: Phase::Committing,
                ..
            } => vec![Effect::ResolveCommit],
            // Slots and pauses lived in memory and are gone. Re-queue; the
            // staging instance kept its cursor and `EnsureInstance` will find
            // it rather than rebuild it.
            Stage::Planned { plan, .. } => {
                next.pending = Some(Move {
                    stage: Stage::Planned {
                        plan: *plan,
                        phase: Phase::Queued,
                    },
                    ..pending.clone()
                });
                vec![Effect::RequestCatchUpSlot]
            }
        };
        Transition::with(next, effects)
    }

    fn live(&self) -> Option<&InstanceKey> {
        self.current.as_ref().map(|p| &p.instance)
    }

    /// Effects that undo whatever the pending move is holding.
    fn abandon(&self, pending: &Move) -> Vec<Effect> {
        let target = pending.target.instance.clone();
        let Stage::Planned { phase, .. } = &pending.stage else {
            // Nothing held while fetching; a fetched artifact stays in the CAS
            // and is collected by refcount like any other.
            return vec![];
        };
        let mut effects = Vec::new();
        if matches!(phase, Phase::Quiescing | Phase::Committing)
            && let Some(live) = self.live()
        {
            effects.push(Effect::ResumeFollowers {
                live: live.clone(),
                staging: target.clone(),
            });
        }
        effects.push(Effect::ReleaseCatchUpSlot);
        if *phase != Phase::Queued {
            effects.push(Effect::ReleaseStaging(target));
        }
        effects
    }

    fn announce(
        &self,
        module_ref: ModuleRef,
        intent: MoveIntent,
        context: AnnounceContext,
    ) -> Step {
        let target = module_ref.placement(&self.module);

        if let Some(pending) = &self.pending {
            if pending.target.instance == target.instance {
                // Every one of a client's DOs re-announces. All but the first
                // land here.
                return Ok(Transition::quiet(self.clone()));
            }
            if pending.committing() {
                return Err(Rejected::CommitInFlight);
            }
        }

        if self.live() == Some(&target.instance) {
            // Already there. If a move elsewhere was pending, the client has
            // changed its mind back — abandon it.
            let mut next = self.clone();
            let effects = match next.pending.take() {
                Some(pending) => self.abandon(&pending),
                None => vec![],
            };
            return Ok(Transition::with(next, effects));
        }

        // Refusals BEFORE any state changes, so a refused announce leaves a
        // pending move exactly as it was.
        let from = self.current.as_ref().map(|p| p.revision);
        resolve_client(&[], from, target.revision, context.revision_policy, intent)?;
        if context.target == TargetInstance::Missing
            && context.live_instances >= context.max_instances
        {
            return Err(Rejected::ForkLimit {
                live: context.live_instances,
                max: context.max_instances,
            });
        }

        let mut effects = match &self.pending {
            Some(pending) => self.abandon(pending),
            None => vec![],
        };
        effects.push(Effect::EnsureArtifact {
            url: module_ref.url.clone(),
            wasm: module_ref.wasm.clone(),
        });
        let mut next = self.clone();
        next.pending = Some(Move {
            target,
            url: module_ref.url,
            intent,
            revision_policy: context.revision_policy,
            stage: Stage::Fetching,
        });
        Ok(Transition::with(next, effects))
    }

    fn withdraw(&self, cause: DetachCause) -> Step {
        if self.current.is_none() && self.pending.is_none() {
            return Err(Rejected::NothingToWithdraw);
        }
        if self.pending.as_ref().is_some_and(Move::committing) {
            return Err(Rejected::CommitInFlight);
        }
        let mut next = self.clone();
        let mut effects = match next.pending.take() {
            Some(pending) => self.abandon(&pending),
            None => vec![],
        };
        if let Some(current) = next.current.take() {
            effects.push(Effect::DetachFrom {
                instance: current.instance,
                cause,
            });
        }
        Ok(Transition::with(next, effects))
    }

    fn progress(&self, target: InstanceKey, step: Progress) -> Step {
        let Some(pending) = &self.pending else {
            return Err(Rejected::Stale);
        };
        if pending.target.instance != target {
            return Err(Rejected::Stale);
        }

        if let Progress::Failed(failure) = step {
            let mut next = self.clone();
            next.pending = None;
            let mut effects = self.abandon(pending);
            effects.push(Effect::Alert(MoveAlert::Failed(failure)));
            return Ok(Transition::with(next, effects));
        }

        let out_of_order = |step: &Progress| Rejected::OutOfOrder {
            step: format!("{step:?}"),
            stage: format!("{:?}", pending.stage),
        };
        let advance = |phase: Phase, plan: ClientPlan, effects: Vec<Effect>| {
            let mut next = self.clone();
            next.pending = Some(Move {
                stage: Stage::Planned { plan, phase },
                ..pending.clone()
            });
            Ok(Transition::with(next, effects))
        };
        let staging = target.clone();

        match (&pending.stage, step) {
            (Stage::Fetching, Progress::ArtifactReady { upgrades }) => {
                let from = self.current.as_ref().map(|p| p.revision);
                match resolve_client(
                    &upgrades,
                    from,
                    pending.target.revision,
                    pending.revision_policy,
                    pending.intent,
                ) {
                    Ok(plan) => advance(Phase::Queued, plan, vec![Effect::RequestCatchUpSlot]),
                    // Announce already checked this with the same inputs, so
                    // it cannot differ — but a reducer never panics.
                    Err(_) => {
                        self.progress(target, Progress::Failed(MoveFailure::RollbackNotPermitted))
                    }
                }
            }

            (
                Stage::Planned {
                    plan,
                    phase: Phase::Queued,
                },
                Progress::SlotGranted,
            ) => advance(
                Phase::Preparing,
                *plan,
                vec![Effect::EnsureInstance {
                    key: staging,
                    parent: self.live().cloned(),
                }],
            ),

            (
                Stage::Planned {
                    plan,
                    phase: Phase::Preparing,
                },
                Progress::InstanceReady,
            ) => match self.live() {
                Some(live) => advance(
                    Phase::CatchingUp,
                    *plan,
                    vec![Effect::HoldAndDiff {
                        staging,
                        live: live.clone(),
                    }],
                ),
                // First subscribe: nothing is being delivered from anywhere
                // else, so there is nothing to catch up to, pause or drain.
                None => advance(
                    Phase::Committing,
                    *plan,
                    vec![Effect::CommitSwitch {
                        from: None,
                        to: pending.target.clone(),
                    }],
                ),
            },

            (
                Stage::Planned {
                    plan,
                    phase: Phase::CatchingUp,
                },
                Progress::CaughtUp,
            )
            | (
                Stage::Planned {
                    plan,
                    phase: Phase::WaitingForDrain,
                },
                Progress::QueueDrained,
            ) => {
                let live = self.live().cloned().ok_or(Rejected::Stale)?;
                advance(
                    Phase::Quiescing,
                    *plan,
                    vec![Effect::PauseAtBoundary { live, staging }],
                )
            }

            (
                Stage::Planned {
                    plan,
                    phase: Phase::Quiescing,
                },
                Progress::Quiesced { queue },
            ) => {
                let live = self.live().cloned().ok_or(Rejected::Stale)?;
                let commit = Effect::CommitSwitch {
                    from: Some(live.clone()),
                    to: pending.target.clone(),
                };
                match (queue, plan.handoff) {
                    (ClientQueue::Empty, _) => advance(Phase::Committing, *plan, vec![commit]),
                    (ClientQueue::Pending, EmissionHandoff::Purge) => advance(
                        Phase::Committing,
                        *plan,
                        vec![Effect::PurgeClientEmissions { on: live }, commit],
                    ),
                    // Never hold two followers paused while a companion
                    // catches up on its reading.
                    (ClientQueue::Pending, EmissionHandoff::Drain) => advance(
                        Phase::WaitingForDrain,
                        *plan,
                        vec![Effect::ResumeFollowers { live, staging }],
                    ),
                }
            }

            (
                Stage::Planned {
                    plan,
                    phase: Phase::Committing,
                },
                Progress::SwitchCommitted,
            ) => {
                let mut next = self.clone();
                let old = next.current.replace(pending.target.clone());
                next.pending = None;
                let mut effects = Vec::new();
                if let Some(old) = &old {
                    effects.push(Effect::ResumeFollowers {
                        live: old.instance.clone(),
                        staging: staging.clone(),
                    });
                }
                // Attach BEFORE detach: the new instance must never be seen
                // with zero clients by its own lifecycle.
                effects.push(Effect::AttachTo(staging));
                if let Some(old) = old {
                    effects.push(Effect::DetachFrom {
                        instance: old.instance,
                        cause: DetachCause::Moved,
                    });
                    effects.push(Effect::FinishDiff);
                }
                if plan.refill != Refill::None {
                    effects.push(Effect::RunRefill(plan.refill));
                }
                effects.push(Effect::ReleaseCatchUpSlot);
                Ok(Transition::with(next, effects))
            }

            (
                Stage::Planned {
                    plan,
                    phase: Phase::Committing,
                },
                Progress::CommitLost,
            ) => match self.live() {
                // The pause did not survive whatever lost the commit.
                Some(live) => advance(
                    Phase::CatchingUp,
                    *plan,
                    vec![Effect::HoldAndDiff {
                        staging,
                        live: live.clone(),
                    }],
                ),
                None => advance(
                    Phase::Preparing,
                    *plan,
                    vec![Effect::EnsureInstance {
                        key: staging,
                        parent: None,
                    }],
                ),
            },

            (_, step) => Err(out_of_order(&step)),
        }
    }
}

/// A move waiting for the host-wide catch-up slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedMove {
    pub client: ClientKey,
    pub module: ModuleId,
    pub tier: Tier,
    pub queued_at: UnixSecs,
}

/// Who gets the slot next: production before development, then first come
/// first served, then by name so the choice is total and a test can pin it.
pub fn next_catch_up(waiting: &[QueuedMove]) -> Option<&QueuedMove> {
    waiting
        .iter()
        .min_by_key(|m| (m.tier, m.queued_at, &m.client, &m.module))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::ids::{ClientId, Namespace};
    use crate::upgrade::UpgradeAction;

    fn module() -> ModuleId {
        ModuleId::parse("wayup-store-offer").unwrap()
    }

    fn row() -> ClientRow {
        ClientRow::new(
            ClientKey {
                namespace: Namespace::parse("dev").unwrap(),
                client: ClientId::parse("ownership.dev.cnft.dev").unwrap(),
            },
            module(),
        )
    }

    fn module_ref(byte: &str, revision: u32) -> ModuleRef {
        ModuleRef {
            url: format!("https://example.invalid/{byte}.wasm"),
            wasm: Sha256Hex::parse(&byte.repeat(32)).unwrap(),
            manifest: Sha256Hex::parse(&"00".repeat(32)).unwrap(),
            config: None,
            revision: Revision(revision),
        }
    }

    fn key(byte: &str) -> InstanceKey {
        module_ref(byte, 0).placement(&module()).instance
    }

    fn ctx(policy: RevisionPolicy, target: TargetInstance) -> AnnounceContext {
        AnnounceContext {
            revision_policy: policy,
            target,
            live_instances: 1,
            max_instances: 4,
        }
    }

    const DEV: RevisionPolicy = RevisionPolicy::Allow;
    const PROD: RevisionPolicy = RevisionPolicy::ExplicitRollbackOnly;

    fn announce(byte: &str, revision: u32) -> Event {
        Event::Announce {
            module_ref: module_ref(byte, revision),
            intent: MoveIntent::Announced,
            context: ctx(DEV, TargetInstance::Missing),
        }
    }

    fn progress(byte: &str, step: Progress) -> Event {
        Event::Progress {
            target: key(byte),
            step,
        }
    }

    fn ready(actions: &[(u32, UpgradeAction)]) -> Progress {
        Progress::ArtifactReady {
            upgrades: actions
                .iter()
                .map(|(since, action)| UpgradeEntry {
                    since: Revision(*since),
                    actions: BTreeSet::from([*action]),
                })
                .collect(),
        }
    }

    fn run(start: ClientRow, events: Vec<Event>) -> (ClientRow, Vec<Effect>) {
        let mut state = start;
        let mut effects = Vec::new();
        for event in events {
            let t = state
                .step(event.clone())
                .unwrap_or_else(|e| panic!("{event:?} rejected: {e}"));
            state = t.next;
            effects.extend(t.effects);
        }
        (state, effects)
    }

    /// A row already living on instance `byte` at `revision`.
    fn settled(byte: &str, revision: u32) -> ClientRow {
        let (state, _) = run(
            row(),
            vec![
                announce(byte, revision),
                progress(byte, ready(&[])),
                progress(byte, Progress::SlotGranted),
                progress(byte, Progress::InstanceReady),
                progress(byte, Progress::SwitchCommitted),
            ],
        );
        assert_eq!(state.current.as_ref().unwrap().instance, key(byte));
        state
    }

    #[test]
    fn a_first_subscribe_attaches_without_pausing_or_draining_anything() {
        let (state, effects) = run(
            row(),
            vec![
                announce("aa", 1),
                progress("aa", ready(&[(1, UpgradeAction::Recapture)])),
                progress("aa", Progress::SlotGranted),
                progress("aa", Progress::InstanceReady),
                progress("aa", Progress::SwitchCommitted),
            ],
        );
        assert_eq!(state.pending, None);
        assert_eq!(state.current.unwrap().revision, Revision(1));
        assert!(
            !effects.iter().any(|e| matches!(
                e,
                Effect::PauseAtBoundary { .. }
                    | Effect::HoldAndDiff { .. }
                    | Effect::RunRefill(_)
                    | Effect::DetachFrom { .. }
            )),
            "nothing to hand off from, and nothing to upgrade FROM: {effects:?}"
        );
        assert!(effects.contains(&Effect::AttachTo(key("aa"))));
    }

    #[test]
    fn a_deploy_commits_only_at_a_quiesced_boundary_with_an_empty_queue() {
        let (state, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[(3, UpgradeAction::Recapture)])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
                progress(
                    "bb",
                    Progress::Quiesced {
                        queue: ClientQueue::Empty,
                    },
                ),
                progress("bb", Progress::SwitchCommitted),
            ],
        );
        assert_eq!(state.current.unwrap().instance, key("bb"));
        let tail: Vec<_> = effects
            .iter()
            .skip_while(|e| !matches!(e, Effect::PauseAtBoundary { .. }))
            .cloned()
            .collect();
        assert_eq!(
            tail,
            vec![
                Effect::PauseAtBoundary {
                    live: key("aa"),
                    staging: key("bb")
                },
                Effect::CommitSwitch {
                    from: Some(key("aa")),
                    to: module_ref("bb", 3).placement(&module()),
                },
                Effect::ResumeFollowers {
                    live: key("aa"),
                    staging: key("bb")
                },
                Effect::AttachTo(key("bb")),
                Effect::DetachFrom {
                    instance: key("aa"),
                    cause: DetachCause::Moved
                },
                Effect::FinishDiff,
                Effect::RunRefill(Refill::Recapture),
                Effect::ReleaseCatchUpSlot,
            ]
        );
    }

    #[test]
    fn an_undrained_queue_resumes_the_followers_rather_than_holding_the_pause() {
        let (state, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
                progress(
                    "bb",
                    Progress::Quiesced {
                        queue: ClientQueue::Pending,
                    },
                ),
            ],
        );
        assert_eq!(
            effects.last(),
            Some(&Effect::ResumeFollowers {
                live: key("aa"),
                staging: key("bb")
            })
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::CommitSwitch { .. }))
        );

        let (_, effects) = run(state, vec![progress("bb", Progress::QueueDrained)]);
        assert!(matches!(effects[..], [Effect::PauseAtBoundary { .. }]));
    }

    #[test]
    fn purge_emissions_commits_through_a_pending_queue() {
        let (_, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[(3, UpgradeAction::PurgeEmissions)])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
                progress(
                    "bb",
                    Progress::Quiesced {
                        queue: ClientQueue::Pending,
                    },
                ),
            ],
        );
        let n = effects.len();
        assert_eq!(
            effects[n - 2],
            Effect::PurgeClientEmissions { on: key("aa") }
        );
        assert!(matches!(effects[n - 1], Effect::CommitSwitch { .. }));
    }

    /// Rebuild #20 replaces the pending move; it never queues behind it.
    #[test]
    fn a_newer_announce_abandons_the_pending_move() {
        let (state, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                announce("cc", 3),
            ],
        );
        assert_eq!(state.pending.as_ref().unwrap().target.instance, key("cc"));
        let n = effects.len();
        assert_eq!(
            effects[n - 3..],
            [
                Effect::ReleaseCatchUpSlot,
                Effect::ReleaseStaging(key("bb")),
                Effect::EnsureArtifact {
                    url: module_ref("cc", 3).url,
                    wasm: module_ref("cc", 3).wasm
                },
            ]
        );
        // …and bb's late answer is recognised as stale, not applied to cc.
        assert_eq!(
            state.step(progress("bb", Progress::CaughtUp)),
            Err(Rejected::Stale)
        );
    }

    #[test]
    fn every_other_companion_of_the_client_re_announcing_is_a_no_op() {
        let (state, _) = run(settled("aa", 2), vec![announce("bb", 3)]);
        for _ in 0..91 {
            let t = state.step(announce("bb", 3)).unwrap();
            assert!(t.effects.is_empty());
            assert_eq!(t.next, state);
        }
        // A straggler still announcing what the row already runs, too.
        let t = settled("aa", 2).step(announce("aa", 2)).unwrap();
        assert!(t.effects.is_empty());
    }

    #[test]
    fn changing_your_mind_back_abandons_the_move() {
        let (state, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                announce("aa", 2),
            ],
        );
        assert_eq!(state.pending, None);
        assert_eq!(state.current.unwrap().instance, key("aa"));
        assert!(effects.contains(&Effect::ReleaseStaging(key("bb"))));
    }

    #[test]
    fn refusals_leave_the_row_exactly_as_it_was() {
        let base = settled("aa", 5);
        let down = Event::Announce {
            module_ref: module_ref("bb", 3),
            intent: MoveIntent::Announced,
            context: ctx(PROD, TargetInstance::Missing),
        };
        assert!(matches!(base.step(down), Err(Rejected::Upgrade(_))));

        let full = Event::Announce {
            module_ref: module_ref("bb", 6),
            intent: MoveIntent::Announced,
            context: AnnounceContext {
                live_instances: 4,
                ..ctx(PROD, TargetInstance::Missing)
            },
        };
        assert_eq!(
            base.step(full),
            Err(Rejected::ForkLimit { live: 4, max: 4 })
        );

        // Joining an instance that already EXISTS is not a fork.
        let join = Event::Announce {
            module_ref: module_ref("bb", 6),
            intent: MoveIntent::Announced,
            context: AnnounceContext {
                live_instances: 4,
                ..ctx(PROD, TargetInstance::Exists)
            },
        };
        assert!(base.step(join).is_ok());
    }

    #[test]
    fn a_failed_move_leaves_the_client_where_it_was_and_says_so() {
        let (state, effects) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", Progress::Failed(MoveFailure::ArtifactRefused)),
            ],
        );
        assert_eq!(state, settled("aa", 2));
        assert_eq!(
            effects.last(),
            Some(&Effect::Alert(MoveAlert::Failed(
                MoveFailure::ArtifactRefused
            )))
        );
    }

    #[test]
    fn nothing_interrupts_a_commit_in_flight() {
        let (state, _) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
                progress(
                    "bb",
                    Progress::Quiesced {
                        queue: ClientQueue::Empty,
                    },
                ),
            ],
        );
        assert_eq!(state.step(announce("cc", 4)), Err(Rejected::CommitInFlight));
        assert_eq!(
            state.step(Event::Withdraw {
                cause: DetachCause::LeaseExpired
            }),
            Err(Rejected::CommitInFlight)
        );
    }

    #[test]
    fn recovery_requeues_everything_except_a_commit_which_it_asks_about() {
        let (mid, _) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
            ],
        );
        let recovered = mid.recover();
        assert_eq!(recovered.effects, vec![Effect::RequestCatchUpSlot]);
        assert!(matches!(
            recovered.next.pending.unwrap().stage,
            Stage::Planned {
                phase: Phase::Queued,
                ..
            }
        ));

        let (committing, _) = run(
            mid,
            vec![progress(
                "bb",
                Progress::Quiesced {
                    queue: ClientQueue::Empty,
                },
            )],
        );
        assert_eq!(committing.recover().effects, vec![Effect::ResolveCommit]);
        let (lost, effects) = run(committing, vec![progress("bb", Progress::CommitLost)]);
        assert!(matches!(effects[..], [Effect::HoldAndDiff { .. }]));
        assert_eq!(
            lost.current.unwrap().instance,
            key("aa"),
            "still on the old one"
        );
    }

    /// An adopted client's first announcement must be a real HAND-OFF. Had the
    /// row started at `current: None` the machine would have treated it as a
    /// first subscribe and attached it to the new instance with no catch-up,
    /// no pause and no drain — dropping whatever the old instance still owed.
    #[test]
    fn an_adopted_clients_first_announcement_is_a_hand_off_not_a_first_subscribe() {
        let adopted = ClientRow::adopt(
            row().client,
            module(),
            module_ref("aa", 0).placement(&module()),
        );
        let (state, effects) = run(
            adopted,
            vec![
                announce("bb", 1),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
            ],
        );
        assert!(matches!(
            state.pending.unwrap().stage,
            Stage::Planned {
                phase: Phase::CatchingUp,
                ..
            }
        ));
        assert!(
            effects.contains(&Effect::HoldAndDiff {
                staging: key("bb"),
                live: key("aa"),
            }),
            "an adopted client must catch up against the instance it is on: {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::CommitSwitch { .. }))
        );
    }

    /// The row is what goes to disk, as TOML, in EVERY phase — a move can be
    /// interrupted anywhere and `recover` reads it back.
    #[test]
    fn a_row_round_trips_through_toml_in_every_phase() {
        let steps = [
            announce("bb", 3),
            progress("bb", ready(&[(3, UpgradeAction::Recapture)])),
            progress("bb", Progress::SlotGranted),
            progress("bb", Progress::InstanceReady),
            progress("bb", Progress::CaughtUp),
            progress(
                "bb",
                Progress::Quiesced {
                    queue: ClientQueue::Pending,
                },
            ),
            progress("bb", Progress::QueueDrained),
            progress(
                "bb",
                Progress::Quiesced {
                    queue: ClientQueue::Empty,
                },
            ),
            progress("bb", Progress::SwitchCommitted),
        ];
        let mut states = vec![row(), settled("aa", 2)];
        let mut state = settled("aa", 2);
        for step in steps {
            state = state.step(step).expect("accepted").next;
            states.push(state.clone());
        }
        for state in states {
            let text = toml::to_string(&state)
                .unwrap_or_else(|e| panic!("{:?} does not serialise: {e}", state.pending));
            let back: ClientRow = toml::from_str(&text)
                .unwrap_or_else(|e| panic!("does not parse back: {e}\n{text}"));
            assert_eq!(back, state, "\n{text}");
        }
    }

    #[test]
    fn production_gets_the_catch_up_slot_first() {
        let queued = |ns: &str, tier, at| QueuedMove {
            client: ClientKey {
                namespace: Namespace::parse(ns).unwrap(),
                client: ClientId::parse("c").unwrap(),
            },
            module: module(),
            tier,
            queued_at: UnixSecs(at),
        };
        let waiting = vec![
            queued("dev", Tier::Development, 10),
            queued("prod", Tier::Production, 99),
            queued("dev", Tier::Development, 5),
        ];
        assert_eq!(next_catch_up(&waiting).unwrap().tier, Tier::Production);
        assert_eq!(
            next_catch_up(&waiting[..1]).unwrap().queued_at,
            UnixSecs(10)
        );
        assert_eq!(next_catch_up(&[]), None);
    }

    // ── Exhaustive exploration ───────────────────────────────────────────
    //
    // The host's view is reconstructed ONLY from effects, then checked
    // against the row after every accepted event, over every event sequence.

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct World {
        paused: bool,
        slot: bool,
        attached: BTreeSet<InstanceKey>,
        commits: usize,
    }

    fn apply(mut world: World, effects: &[Effect]) -> World {
        for effect in effects {
            match effect {
                Effect::PauseAtBoundary { .. } => {
                    assert!(!world.paused, "paused twice");
                    world.paused = true;
                }
                Effect::ResumeFollowers { .. } => {
                    assert!(world.paused, "resumed without a pause");
                    world.paused = false;
                }
                Effect::RequestCatchUpSlot => world.slot = true,
                Effect::ReleaseCatchUpSlot => world.slot = false,
                Effect::CommitSwitch { from, .. } => {
                    assert!(
                        world.paused || from.is_none(),
                        "committed a hand-off without both followers paused"
                    );
                    world.commits += 1;
                }
                Effect::AttachTo(k) => {
                    assert!(world.attached.insert(k.clone()), "attached twice");
                }
                Effect::DetachFrom { instance, .. } => {
                    assert!(world.attached.remove(instance), "detached, never attached");
                }
                _ => {}
            }
        }
        world
    }

    fn check(row: &ClientRow, world: &World) {
        let expected: BTreeSet<_> = row.current.iter().map(|p| p.instance.clone()).collect();
        assert_eq!(
            world.attached, expected,
            "the instances the client is attached to must be exactly its current one"
        );
        if row.pending.is_none() {
            assert!(!world.paused, "followers left paused with no move pending");
            assert!(!world.slot, "catch-up slot leaked: {row:?}");
        }
    }

    fn alphabet() -> Vec<Event> {
        let mut events = vec![Event::Withdraw {
            cause: DetachCause::Unsubscribed,
        }];
        for (byte, revision) in [("aa", 1), ("bb", 2), ("cc", 3)] {
            events.push(announce(byte, revision));
            for step in [
                ready(&[]),
                ready(&[(revision, UpgradeAction::PurgeEmissions)]),
                Progress::SlotGranted,
                Progress::InstanceReady,
                Progress::CaughtUp,
                Progress::Quiesced {
                    queue: ClientQueue::Empty,
                },
                Progress::Quiesced {
                    queue: ClientQueue::Pending,
                },
                Progress::QueueDrained,
                Progress::SwitchCommitted,
                Progress::CommitLost,
                Progress::Failed(MoveFailure::StagingFailed),
            ] {
                events.push(progress(byte, step));
            }
        }
        events
    }

    #[test]
    fn invariants_hold_over_every_event_sequence() {
        let events = alphabet();
        let start = World {
            paused: false,
            slot: false,
            attached: BTreeSet::new(),
            commits: 0,
        };
        let mut frontier = vec![(row(), start)];
        let mut visited = 0_usize;
        let mut committed_a_hand_off = false;
        for _depth in 0..12 {
            let mut next_frontier = Vec::new();
            for (state, world) in &frontier {
                for event in &events {
                    let Ok(t) = state.step(event.clone()) else {
                        continue;
                    };
                    // `current` moves on SwitchCommitted and on nothing else.
                    let is_commit = matches!(
                        event,
                        Event::Progress {
                            step: Progress::SwitchCommitted,
                            ..
                        }
                    );
                    let is_withdraw = matches!(event, Event::Withdraw { .. });
                    if !is_commit && !is_withdraw {
                        assert_eq!(t.next.current, state.current, "{event:?} moved the row");
                    }
                    let world = apply(world.clone(), &t.effects);
                    check(&t.next, &world);
                    committed_a_hand_off |= world.commits >= 2;
                    visited += 1;
                    next_frontier.push((
                        t.next,
                        World {
                            commits: world.commits.min(2),
                            ..world
                        },
                    ));
                }
            }
            next_frontier.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
            next_frontier.dedup();
            frontier = next_frontier;
        }
        // De-duplication keeps the reachable set small (~3K steps), so the
        // count is a floor against a broken alphabet, not a measure of
        // coverage. The assertion that matters is the one after it.
        assert!(visited > 1_000, "exploration was vacuous: {visited} steps");
        assert!(
            committed_a_hand_off,
            "exploration never reached a real A→B hand-off, so it proved nothing about one"
        );
    }

    /// Recovery from EVERY reachable state re-issues effects that leave the
    /// host's view consistent — pauses and slots are re-acquired, not assumed.
    #[test]
    fn recover_never_assumes_in_memory_state_survived() {
        let (mid, _) = run(
            settled("aa", 2),
            vec![
                announce("bb", 3),
                progress("bb", ready(&[])),
                progress("bb", Progress::SlotGranted),
                progress("bb", Progress::InstanceReady),
                progress("bb", Progress::CaughtUp),
            ],
        );
        // Quiescing had a pause outstanding. After a crash it is gone, and the
        // recovered row must not be in a phase that believes otherwise.
        let recovered = mid.recover().next;
        assert_eq!(
            recovered.step(progress(
                "bb",
                Progress::Quiesced {
                    queue: ClientQueue::Empty
                }
            )),
            Err(Rejected::OutOfOrder {
                step: "Quiesced { queue: Empty }".to_owned(),
                stage: format!("{:?}", recovered.pending.as_ref().unwrap().stage),
            })
        );
    }
}
