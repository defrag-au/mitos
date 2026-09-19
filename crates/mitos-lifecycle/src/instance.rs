//! The instance lifecycle — D10. **Subscribe creates; its absence destroys.**
//!
//! ```text
//! Staging ──► Active ──► Draining ──► Stopped ──► Reaped
//!    │           ▲           │           │
//!    │           └───────────┴───────────┘  (a client attaches)
//!    └──(staging failed / abandoned)──────────────────► Reaped
//! ```
//!
//! An instance is kept alive by the clients attached to it. When the last one
//! leaves, WHY it left decides everything — see [`ZeroRefCause`].
//!
//! This machine also carries the fix for a v2 bug: `DELETE /_admin/modules/…`
//! only stopped the follower, the manifest stayed on disk, and `auto_resume`
//! started every directory with a manifest — so a "deleted" module came back
//! on the next restart. Here the operator's intent is persisted as
//! [`DesiredState`], and [`Instance::on_boot`] is the only thing that decides
//! what a restart resumes.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::ids::{ClientKey, InstanceKey};
use crate::policy::{EjectPolicy, NamespaceProfile};
use crate::{Transition, UnixSecs};

/// Why an instance has no clients. The distinction is load-bearing:
///
/// - `Superseded` — every client MOVED to another instance. This is what a
///   deploy looks like. It retires automatically under EVERY policy, because
///   otherwise each production deploy would leave behind an instance alerting
///   that production had abandoned it.
/// - `Abandoned` — the clients VANISHED. In `dev` that is a developer who
///   stopped riffing; in `prod` it is far more likely an outage than a
///   decision, so [`EjectPolicy::AlertOnly`] keeps it running.
///
/// Decided by how the LAST client left.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ZeroRefCause {
    Superseded,
    Abandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DetachCause {
    /// The client row moved to another instance.
    Moved,
    Unsubscribed,
    /// [`crate::lease`] expired the client.
    LeaseExpired,
}

impl DetachCause {
    fn as_zero_ref(self) -> ZeroRefCause {
        match self {
            DetachCause::Moved => ZeroRefCause::Superseded,
            DetachCause::Unsubscribed | DetachCause::LeaseExpired => ZeroRefCause::Abandoned,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "cause")]
pub enum StopCause {
    ZeroRef {
        why: ZeroRefCause,
    },
    /// An operator stopped it. Never auto-reaped, never auto-started — not by
    /// a tick, not by a client attaching, and not by a restart.
    Operator,
}

/// What the operator wants, persisted. Orthogonal to [`Phase`]: the phase is
/// what IS, this is what is WANTED.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DesiredState {
    Running,
    Stopped,
    /// Intentionally subscriber-less: runs with zero clients, neither alerts
    /// nor ejects. For a module whose consumers are coming.
    Pinned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "phase")]
pub enum Phase {
    /// Created by a client move; following from its cursor toward tip with its
    /// emissions held. Zero clients is NORMAL here — nobody has switched yet.
    Staging,
    Active,
    /// Zero clients, follower still RUNNING. A re-attach loses nothing.
    Draining {
        cause: ZeroRefCause,
        since: UnixSecs,
    },
    /// Follower stopped, handles closed, state RETAINED on disk.
    Stopped {
        cause: StopCause,
        since: UnixSecs,
    },
    /// Terminal. State, emissions and the artifact reference are gone.
    Reaped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instance {
    pub key: InstanceKey,
    pub phase: Phase,
    pub desired: DesiredState,
    pub attached: BTreeSet<ClientKey>,
    /// [`NamespaceProfile::protect`] folded over every client that has EVER
    /// attached. A high-water mark, never relaxed on detach: with zero clients
    /// there is no namespace left to ask, and an instance that once served
    /// `prod` is governed as `prod` until it is reaped.
    pub governing: Option<NamespaceProfile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictMode {
    RefuseIfAttached,
    Force,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorCommand {
    Stop,
    Start,
    Pin,
    Unpin,
    Evict(EvictMode),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Attach {
        client: ClientKey,
        profile: NamespaceProfile,
    },
    Detach {
        client: ClientKey,
        cause: DetachCause,
    },
    /// Time passed. Fed periodically; cheap and idempotent.
    Tick,
    /// The client move that created this staging instance failed, or was
    /// abandoned, and no other move wants it.
    StagingReleased,
    Operator(OperatorCommand),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceAlert {
    /// Zero clients, vanished rather than moved, under `AlertOnly`. Still
    /// running; an operator decides.
    AbandonedAwaitingOperator,
}

/// All idempotent — see the crate docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Resume the follower from its persisted cursor.
    StartFollower,
    /// Stop the follower and close its redb handles. State stays on disk.
    StopFollower,
    /// v2 evict's teardown order: dialer unregister → close handles → remove
    /// the state directory → purge emissions → release the CAS reference.
    /// Implies the follower is already stopped or is stopped first.
    Reap,
    Alert(InstanceAlert),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Rejected {
    #[error("instance is reaped")]
    Reaped,
    #[error("{0} is already attached")]
    AlreadyAttached(ClientKey),
    #[error("{0} is not attached")]
    NotAttached(ClientKey),
    #[error("{0} client(s) still attached; evict with Force to override")]
    StillAttached(usize),
    #[error("a staging instance is controlled by its client move, not by operator {0:?}")]
    StagingIsNotOperable(OperatorCommand),
}

/// What a restart does with this instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootAction {
    StartFollower,
    LeaveStopped,
}

type Step = Result<Transition<Instance, Effect>, Rejected>;

/// What the host PERSISTS for an instance — everything except its key.
///
/// The key is deliberately absent. It already lives in the instance
/// directory's marker, and a v2-style upload RENAMES that directory to a new
/// key while the instance carries on. A second copy of the key in here would
/// open a crash window between the rename and rewriting this file, in which
/// the two disagree. With one copy there is nothing to disagree with: the
/// record moves with the directory, and the key is read from the marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRecord {
    pub phase: Phase,
    pub desired: DesiredState,
    pub attached: BTreeSet<ClientKey>,
    pub governing: Option<NamespaceProfile>,
}

impl Instance {
    pub fn record(&self) -> LifecycleRecord {
        LifecycleRecord {
            phase: self.phase,
            desired: self.desired,
            attached: self.attached.clone(),
            governing: self.governing,
        }
    }

    pub fn from_record(key: InstanceKey, record: LifecycleRecord) -> Self {
        Self {
            key,
            phase: record.phase,
            desired: record.desired,
            attached: record.attached,
            governing: record.governing,
        }
    }

    /// A new instance, created by a client move.
    pub fn staging(key: InstanceKey) -> Self {
        Self {
            key,
            phase: Phase::Staging,
            desired: DesiredState::Running,
            attached: BTreeSet::new(),
            governing: None,
        }
    }

    /// Take ownership of an instance that was running BEFORE lifecycle state
    /// existed — every v2 module, the first time a v3 host boots — or that an
    /// operator installed by upload. The rule is "change nothing":
    ///
    /// - **It has clients** ⇒ `Active`, governed by them. From here on it
    ///   behaves like any other instance, which matters: when its clients later
    ///   MOVE to a newer instance it must retire as `Superseded`. Pinning it
    ///   would leave one zombie follower behind per deploy, for ever.
    /// - **It has none** ⇒ `Active` and [`DesiredState::Pinned`]. v2 ran a
    ///   module whether or not anyone subscribed, and an operator who uploads
    ///   one owns its lifetime. `Pinned` is the honest encoding of that — it
    ///   neither alerts nor ejects — and it is VISIBLE, so the operator can
    ///   unpin or evict the ones nobody wants.
    pub fn adopt(key: InstanceKey, clients: &BTreeMap<ClientKey, NamespaceProfile>) -> Self {
        Self {
            key,
            phase: Phase::Active,
            desired: if clients.is_empty() {
                DesiredState::Pinned
            } else {
                DesiredState::Running
            },
            attached: clients.keys().cloned().collect(),
            governing: clients.values().copied().reduce(NamespaceProfile::protect),
        }
    }

    /// The events that bring `attached` into line with the clients that have
    /// a registration ON DISK right now.
    ///
    /// The host does not hook each path that adds or removes a registration —
    /// there are at least four (subscribe, an interest mutation emptying a
    /// record, the admin delete, the legacy flat-file migration), and the next
    /// one someone adds would be missed silently. It calls this after any of
    /// them, at boot, and on every tick; being a diff, it is idempotent and a
    /// missed call is corrected by the next one.
    ///
    /// Attaches come FIRST, so swapping one client for another never passes
    /// through zero and drains an instance that is about to be wanted.
    /// A client whose registrations vanished is `Unsubscribed`; a lease expiry
    /// is fed directly as its own `Detach` BEFORE the files are removed.
    pub fn reconcile(&self, on_disk: &BTreeMap<ClientKey, NamespaceProfile>) -> Vec<Event> {
        if self.phase == Phase::Reaped {
            return Vec::new();
        }
        let attach = on_disk
            .iter()
            .filter(|(client, _)| !self.attached.contains(*client))
            .map(|(client, profile)| Event::Attach {
                client: client.clone(),
                profile: *profile,
            });
        let detach = self
            .attached
            .iter()
            .filter(|client| !on_disk.contains_key(*client))
            .map(|client| Event::Detach {
                client: client.clone(),
                cause: DetachCause::Unsubscribed,
            });
        attach.chain(detach).collect()
    }

    /// The effects to re-issue for a state loaded from disk after a restart.
    ///
    /// Follower liveness is in-memory, so after ANY restart nothing is running
    /// and [`Instance::on_boot`] says what should be. The one effect that can
    /// be left half-done on disk is `Reap` — the state says `Reaped` but the
    /// directory is still there — and it is idempotent, so it is simply run
    /// again.
    pub fn recover(&self) -> Vec<Effect> {
        match (self.phase, self.on_boot()) {
            (Phase::Reaped, _) => vec![Effect::Reap],
            (_, BootAction::StartFollower) => vec![Effect::StartFollower],
            (_, BootAction::LeaveStopped) => vec![],
        }
    }

    /// Whether this instance's follower SHOULD be running. The only thing that
    /// decides what a restart resumes — and what the watchdog revives: v2's
    /// watchdog restarted any module with companions and no live follower, so
    /// an operator's stop on a SUBSCRIBED module was undone within one tick,
    /// not merely on the next restart. Note what is absent: nothing here looks
    /// at whether an artifact, a manifest or a companion exists on disk.
    pub fn on_boot(&self) -> BootAction {
        match (self.phase, self.desired) {
            (Phase::Reaped | Phase::Stopped { .. }, _) | (_, DesiredState::Stopped) => {
                BootAction::LeaveStopped
            }
            (Phase::Staging | Phase::Active | Phase::Draining { .. }, _) => {
                BootAction::StartFollower
            }
        }
    }

    pub fn step(&self, event: Event, now: UnixSecs) -> Step {
        if self.phase == Phase::Reaped {
            return Err(Rejected::Reaped);
        }
        match event {
            Event::Attach { client, profile } => self.attach(client, profile),
            Event::Detach { client, cause } => self.detach(client, cause, now),
            Event::Tick => Ok(self.tick(now)),
            Event::StagingReleased => Ok(self.staging_released()),
            Event::Operator(command) => self.operator(command, now),
        }
    }

    fn attach(&self, client: ClientKey, profile: NamespaceProfile) -> Step {
        if self.attached.contains(&client) {
            return Err(Rejected::AlreadyAttached(client));
        }
        let mut next = self.clone();
        next.attached.insert(client);
        next.governing = Some(match self.governing {
            Some(existing) => existing.protect(profile),
            None => profile,
        });
        let effects = match self.phase {
            // The follower is already running in all three.
            Phase::Staging | Phase::Active | Phase::Draining { .. } => {
                next.phase = Phase::Active;
                vec![]
            }
            Phase::Stopped {
                cause: StopCause::ZeroRef { .. },
                ..
            } => {
                next.phase = Phase::Active;
                vec![Effect::StartFollower]
            }
            // The client is recorded and will be served the moment an operator
            // starts the instance. It does NOT start it — that is the bug.
            Phase::Stopped {
                cause: StopCause::Operator,
                ..
            } => vec![],
            Phase::Reaped => unreachable!("rejected above"),
        };
        Ok(Transition::with(next, effects))
    }

    fn detach(&self, client: ClientKey, cause: DetachCause, now: UnixSecs) -> Step {
        if !self.attached.contains(&client) {
            return Err(Rejected::NotAttached(client));
        }
        let mut next = self.clone();
        next.attached.remove(&client);
        Ok(next.settle(cause.as_zero_ref(), now))
    }

    /// Bring an instance with a RUNNING follower to the phase its client count
    /// implies. The one place "zero clients" is interpreted.
    fn settle(mut self, cause: ZeroRefCause, now: UnixSecs) -> Transition<Instance, Effect> {
        let running = matches!(self.phase, Phase::Active | Phase::Draining { .. });
        if !running || !self.attached.is_empty() || self.desired == DesiredState::Pinned {
            return Transition::quiet(self);
        }
        match cause {
            // Nobody is coming back to a superseded instance on their own, so
            // there is no grace to wait out — stop now, retain for rollback.
            ZeroRefCause::Superseded => {
                self.phase = Phase::Stopped {
                    cause: StopCause::ZeroRef { why: cause },
                    since: now,
                };
                Transition::with(self, vec![Effect::StopFollower])
            }
            ZeroRefCause::Abandoned => {
                let already_draining = matches!(self.phase, Phase::Draining { .. });
                self.phase = Phase::Draining { cause, since: now };
                let alert = matches!(
                    self.governing.map(|p| p.eject),
                    Some(EjectPolicy::AlertOnly)
                ) && !already_draining;
                let effects = if alert {
                    vec![Effect::Alert(InstanceAlert::AbandonedAwaitingOperator)]
                } else {
                    vec![]
                };
                Transition::with(self, effects)
            }
        }
    }

    fn tick(&self, now: UnixSecs) -> Transition<Instance, Effect> {
        let Some(profile) = self.governing else {
            // Never had a client: Staging, or pinned from birth. No timers.
            return Transition::quiet(self.clone());
        };
        let mut next = self.clone();
        match self.phase {
            Phase::Draining {
                cause: ZeroRefCause::Abandoned,
                since,
            } => match profile.eject {
                EjectPolicy::Automatic { grace_secs, .. } if now.since(since) >= grace_secs => {
                    next.phase = Phase::Stopped {
                        cause: StopCause::ZeroRef {
                            why: ZeroRefCause::Abandoned,
                        },
                        since: now,
                    };
                    Transition::with(next, vec![Effect::StopFollower])
                }
                EjectPolicy::Automatic { .. } | EjectPolicy::AlertOnly => Transition::quiet(next),
            },
            Phase::Stopped {
                cause: StopCause::ZeroRef { why },
                since,
            } => {
                let retain = match (why, profile.eject) {
                    (ZeroRefCause::Superseded, _) => Some(profile.retain_superseded_secs),
                    (ZeroRefCause::Abandoned, EjectPolicy::Automatic { retain_secs, .. }) => {
                        Some(retain_secs)
                    }
                    // Reachable only if the governing profile hardened AFTER
                    // the stop (a prod client attached and left). Then it
                    // waits for an operator like everything else under it.
                    (ZeroRefCause::Abandoned, EjectPolicy::AlertOnly) => None,
                };
                match retain {
                    Some(retain) if now.since(since) >= retain => {
                        next.phase = Phase::Reaped;
                        Transition::with(next, vec![Effect::Reap])
                    }
                    _ => Transition::quiet(next),
                }
            }
            Phase::Staging
            | Phase::Active
            | Phase::Draining {
                cause: ZeroRefCause::Superseded,
                ..
            }
            | Phase::Stopped {
                cause: StopCause::Operator,
                ..
            }
            | Phase::Reaped => Transition::quiet(next),
        }
    }

    fn staging_released(&self) -> Transition<Instance, Effect> {
        // Only a staging instance nobody switched onto is released. If a
        // client DID attach in the meantime it is Active and this is stale.
        if self.phase != Phase::Staging {
            return Transition::quiet(self.clone());
        }
        let mut next = self.clone();
        next.phase = Phase::Reaped;
        Transition::with(next, vec![Effect::StopFollower, Effect::Reap])
    }

    fn operator(&self, command: OperatorCommand, now: UnixSecs) -> Step {
        if self.phase == Phase::Staging && !matches!(command, OperatorCommand::Evict(_)) {
            return Err(Rejected::StagingIsNotOperable(command));
        }
        let mut next = self.clone();
        let running = matches!(
            self.phase,
            Phase::Staging | Phase::Active | Phase::Draining { .. }
        );
        match command {
            OperatorCommand::Stop => {
                next.desired = DesiredState::Stopped;
                if !running {
                    // Already stopped for another reason: the operator's
                    // intent now owns it, so no timer will reap it.
                    next.phase = Phase::Stopped {
                        cause: StopCause::Operator,
                        since: now,
                    };
                    return Ok(Transition::quiet(next));
                }
                next.phase = Phase::Stopped {
                    cause: StopCause::Operator,
                    since: now,
                };
                Ok(Transition::with(next, vec![Effect::StopFollower]))
            }
            OperatorCommand::Start => {
                next.desired = DesiredState::Running;
                if running {
                    // Not quiet: `Start` on a PINNED instance un-pins it, and
                    // one with no clients must then drain like any other.
                    return Ok(next.settle(ZeroRefCause::Abandoned, now));
                }
                next.phase = Phase::Active;
                let mut settled = next.settle(ZeroRefCause::Abandoned, now);
                settled.effects.insert(0, Effect::StartFollower);
                Ok(settled)
            }
            OperatorCommand::Pin => {
                next.desired = DesiredState::Pinned;
                match self.phase {
                    Phase::Draining { .. } => {
                        next.phase = Phase::Active;
                        Ok(Transition::quiet(next))
                    }
                    Phase::Stopped { .. } => {
                        next.phase = Phase::Active;
                        Ok(Transition::with(next, vec![Effect::StartFollower]))
                    }
                    _ => Ok(Transition::quiet(next)),
                }
            }
            OperatorCommand::Unpin => {
                if self.desired != DesiredState::Pinned {
                    return Ok(Transition::quiet(next));
                }
                next.desired = DesiredState::Running;
                Ok(next.settle(ZeroRefCause::Abandoned, now))
            }
            OperatorCommand::Evict(mode) => {
                if mode == EvictMode::RefuseIfAttached && !self.attached.is_empty() {
                    return Err(Rejected::StillAttached(self.attached.len()));
                }
                next.attached.clear();
                next.phase = Phase::Reaped;
                let effects = if running {
                    vec![Effect::StopFollower, Effect::Reap]
                } else {
                    vec![Effect::Reap]
                };
                Ok(Transition::with(next, effects))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{ClientId, ModuleId, Namespace, Sha256Hex};
    use crate::policy::fixtures::{dev, prod};

    fn key() -> InstanceKey {
        InstanceKey {
            module: ModuleId::parse("wayup-store-offer").unwrap(),
            wasm: Sha256Hex::parse(&"aa".repeat(32)).unwrap(),
            config: None,
        }
    }

    fn client(ns: &str, id: &str) -> ClientKey {
        ClientKey {
            namespace: Namespace::parse(ns).unwrap(),
            client: ClientId::parse(id).unwrap(),
        }
    }

    const T0: UnixSecs = UnixSecs(1_000_000);

    fn after(secs: u64) -> UnixSecs {
        UnixSecs(T0.0 + secs)
    }

    /// Apply events in order, asserting each is accepted.
    fn run(start: Instance, events: Vec<(Event, UnixSecs)>) -> (Instance, Vec<Effect>) {
        let mut state = start;
        let mut effects = Vec::new();
        for (event, now) in events {
            let t = state
                .step(event.clone(), now)
                .unwrap_or_else(|e| panic!("{event:?} rejected: {e}"));
            state = t.next;
            effects.extend(t.effects);
        }
        (state, effects)
    }

    fn attach(c: &ClientKey, p: NamespaceProfile) -> Event {
        Event::Attach {
            client: c.clone(),
            profile: p,
        }
    }

    fn detach(c: &ClientKey, cause: DetachCause) -> Event {
        Event::Detach {
            client: c.clone(),
            cause,
        }
    }

    /// A deploy: the only client moves off. Retires with no alert EVEN IN
    /// PROD, kept 72h for rollback, then reaped.
    #[test]
    fn a_superseded_prod_instance_retires_quietly() {
        let c = client("prod", "ownership.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, prod()), T0),
                (detach(&c, DetachCause::Moved), after(10)),
            ],
        );
        assert_eq!(effects, vec![Effect::StopFollower]);
        assert!(matches!(
            state.phase,
            Phase::Stopped {
                cause: StopCause::ZeroRef {
                    why: ZeroRefCause::Superseded
                },
                ..
            }
        ));

        let (state, effects) = run(state, vec![(Event::Tick, after(10 + 72 * 3600 - 1))]);
        assert!(effects.is_empty(), "retained for the full rollback window");
        let (state, effects) = run(state, vec![(Event::Tick, after(10 + 72 * 3600))]);
        assert_eq!(effects, vec![Effect::Reap]);
        assert_eq!(state.phase, Phase::Reaped);
    }

    /// The same zero-client state reached by a VANISHING client must do the
    /// opposite in prod: alert once, keep running, never time out.
    #[test]
    fn an_abandoned_prod_instance_alerts_and_keeps_running() {
        let c = client("prod", "ownership.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, prod()), T0),
                (detach(&c, DetachCause::LeaseExpired), after(10)),
                (Event::Tick, after(365 * 24 * 3600)),
            ],
        );
        assert_eq!(
            effects,
            vec![Effect::Alert(InstanceAlert::AbandonedAwaitingOperator)]
        );
        assert!(matches!(state.phase, Phase::Draining { .. }));
        assert_eq!(state.on_boot(), BootAction::StartFollower);
    }

    #[test]
    fn an_abandoned_dev_instance_drains_stops_and_is_reaped() {
        let c = client("dev", "ownership.dev.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, dev()), T0),
                (detach(&c, DetachCause::Unsubscribed), T0),
                (Event::Tick, after(599)),
            ],
        );
        assert!(effects.is_empty(), "still inside the 10 minute grace");
        let (state, effects) = run(state, vec![(Event::Tick, after(600))]);
        assert_eq!(effects, vec![Effect::StopFollower]);
        let (state, effects) = run(state, vec![(Event::Tick, after(600 + 24 * 3600))]);
        assert_eq!(effects, vec![Effect::Reap]);
        assert_eq!(state.phase, Phase::Reaped);
    }

    #[test]
    fn reattaching_inside_the_grace_loses_nothing_and_after_it_resumes() {
        let c = client("dev", "ownership.dev.cnft.dev");
        let (draining, _) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, dev()), T0),
                (detach(&c, DetachCause::Unsubscribed), T0),
            ],
        );
        let (state, effects) = run(draining.clone(), vec![(attach(&c, dev()), after(60))]);
        assert!(effects.is_empty(), "the follower never stopped");
        assert_eq!(state.phase, Phase::Active);

        let (state, effects) = run(
            draining,
            vec![(Event::Tick, after(600)), (attach(&c, dev()), after(900))],
        );
        assert_eq!(effects, vec![Effect::StopFollower, Effect::StartFollower]);
        assert_eq!(state.phase, Phase::Active);
    }

    /// The v2 bug, as a test: an operator's stop survives a restart AND a
    /// client attaching.
    #[test]
    fn an_operator_stop_is_not_resurrected_by_a_restart_or_a_subscribe() {
        let a = client("prod", "ownership.cnft.dev");
        let b = client("prod", "jpgsm.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&a, prod()), T0),
                (Event::Operator(OperatorCommand::Stop), after(5)),
                (attach(&b, prod()), after(10)),
                (Event::Tick, after(10 * 365 * 24 * 3600)),
            ],
        );
        assert_eq!(effects, vec![Effect::StopFollower]);
        assert_eq!(state.on_boot(), BootAction::LeaveStopped);
        assert_eq!(
            state.attached.len(),
            2,
            "clients are remembered, not served"
        );

        let (state, effects) = run(
            state,
            vec![(Event::Operator(OperatorCommand::Start), after(20))],
        );
        assert_eq!(effects, vec![Effect::StartFollower]);
        assert_eq!(state.phase, Phase::Active);
    }

    #[test]
    fn the_governing_profile_is_a_high_water_mark() {
        let p = client("prod", "ownership.cnft.dev");
        let d = client("dev", "ownership.dev.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&d, dev()), T0),
                (attach(&p, prod()), T0),
                (detach(&p, DetachCause::Moved), after(1)),
                // dev is the last to leave, and it VANISHED — but this
                // instance served prod, so it is governed as prod.
                (detach(&d, DetachCause::Unsubscribed), after(2)),
                (Event::Tick, after(30 * 24 * 3600)),
            ],
        );
        assert_eq!(
            effects,
            vec![Effect::Alert(InstanceAlert::AbandonedAwaitingOperator)]
        );
        assert!(matches!(state.phase, Phase::Draining { .. }));
    }

    #[test]
    fn a_pinned_instance_runs_with_no_clients_and_says_nothing() {
        let c = client("prod", "ownership.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, prod()), T0),
                (Event::Operator(OperatorCommand::Pin), T0),
                (detach(&c, DetachCause::LeaseExpired), after(1)),
                (Event::Tick, after(365 * 24 * 3600)),
            ],
        );
        assert!(effects.is_empty());
        assert_eq!(state.phase, Phase::Active);
    }

    // ── Adoption, reconcile, recovery — what WO-03 wires ─────────────────

    /// A v2 module nobody subscribes to keeps running and says nothing, which
    /// is what v2 did — but now VISIBLY, as `Pinned`.
    #[test]
    fn adopting_a_module_nobody_subscribes_to_changes_nothing() {
        let adopted = Instance::adopt(key(), &BTreeMap::new());
        assert_eq!(adopted.desired, DesiredState::Pinned);
        assert_eq!(adopted.on_boot(), BootAction::StartFollower);
        let (state, effects) = run(adopted, vec![(Event::Tick, after(10 * 365 * 24 * 3600))]);
        assert!(effects.is_empty());
        assert_eq!(state.phase, Phase::Active);
    }

    /// The reason a SUBSCRIBED module is NOT pinned on adoption: every deploy
    /// moves its clients off it, and a pinned instance would never retire.
    #[test]
    fn an_adopted_instance_with_clients_retires_when_they_move_on() {
        let p = client("prod", "ownership.cnft.dev");
        let d = client("dev", "ownership.dev.cnft.dev");
        let adopted = Instance::adopt(
            key(),
            &BTreeMap::from([(p.clone(), prod()), (d.clone(), dev())]),
        );
        assert_eq!(adopted.desired, DesiredState::Running);
        assert_eq!(adopted.governing, Some(prod().protect(dev())));

        let (state, effects) = run(
            adopted,
            vec![
                (detach(&d, DetachCause::Moved), after(1)),
                (detach(&p, DetachCause::Moved), after(2)),
            ],
        );
        assert_eq!(effects, vec![Effect::StopFollower]);
        assert!(matches!(
            state.phase,
            Phase::Stopped {
                cause: StopCause::ZeroRef {
                    why: ZeroRefCause::Superseded
                },
                ..
            }
        ));
    }

    #[test]
    fn reconcile_is_a_diff_and_applying_it_converges() {
        let a = client("prod", "ownership.cnft.dev");
        let b = client("prod", "jpgsm.cnft.dev");
        let c = client("dev", "ownership.dev.cnft.dev");
        let start = Instance::adopt(
            key(),
            &BTreeMap::from([(a.clone(), prod()), (b.clone(), prod())]),
        );

        // `b` unsubscribed and `c` arrived, by whatever path.
        let on_disk = BTreeMap::from([(a.clone(), prod()), (c.clone(), dev())]);
        let events = start.reconcile(&on_disk);
        assert_eq!(
            events,
            vec![attach(&c, dev()), detach(&b, DetachCause::Unsubscribed)],
            "attaches first, so a swap never passes through zero"
        );

        let (state, _) = run(start, events.into_iter().map(|e| (e, T0)).collect());
        assert_eq!(state.attached, BTreeSet::from([a, c]));
        assert!(
            state.reconcile(&on_disk).is_empty(),
            "idempotent once applied"
        );
    }

    /// Swapping the ONLY client for another must not drain the instance.
    #[test]
    fn reconcile_never_passes_through_zero_clients() {
        let old = client("prod", "ownership.cnft.dev");
        let new = client("prod", "jpgsm.cnft.dev");
        let start = Instance::adopt(key(), &BTreeMap::from([(old, prod())]));
        let events = start.reconcile(&BTreeMap::from([(new, prod())]));
        let (state, effects) = run(start, events.into_iter().map(|e| (e, T0)).collect());
        assert!(effects.is_empty(), "no alert, no stop: {effects:?}");
        assert_eq!(state.phase, Phase::Active);
    }

    #[test]
    fn recovery_restarts_what_should_run_and_finishes_a_half_done_reap() {
        let c = client("prod", "ownership.cnft.dev");
        let running = Instance::adopt(key(), &BTreeMap::from([(c.clone(), prod())]));
        assert_eq!(running.recover(), vec![Effect::StartFollower]);

        let (stopped, _) = run(
            running.clone(),
            vec![(Event::Operator(OperatorCommand::Stop), T0)],
        );
        assert!(
            stopped.recover().is_empty(),
            "an operator stop survives a restart"
        );

        let (reaped, _) = run(
            running,
            vec![(
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
                T0,
            )],
        );
        assert_eq!(reaped.recover(), vec![Effect::Reap]);
        assert!(reaped.reconcile(&BTreeMap::from([(c, prod())])).is_empty());
    }

    /// The record is what goes to disk, as TOML. Every phase must survive the
    /// trip — nested tagged enums and `Option`s are exactly where a format
    /// quietly refuses.
    #[test]
    fn every_phase_round_trips_through_the_persisted_record() {
        let p = client("prod", "ownership.cnft.dev");
        let clients = BTreeMap::from([(p.clone(), prod())]);
        let base = Instance::adopt(key(), &clients);
        let states = vec![
            Instance::staging(key()),
            base.clone(),
            Instance::adopt(key(), &BTreeMap::new()),
            run(
                base.clone(),
                vec![(detach(&p, DetachCause::LeaseExpired), after(5))],
            )
            .0,
            run(
                base.clone(),
                vec![(detach(&p, DetachCause::Moved), after(5))],
            )
            .0,
            run(
                base.clone(),
                vec![(Event::Operator(OperatorCommand::Stop), after(5))],
            )
            .0,
            run(
                base,
                vec![(
                    Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
                    T0,
                )],
            )
            .0,
        ];
        for state in states {
            let text = toml::to_string(&state.record())
                .unwrap_or_else(|e| panic!("{:?} does not serialise: {e}", state.phase));
            let back: LifecycleRecord = toml::from_str(&text)
                .unwrap_or_else(|e| panic!("{:?} does not parse back: {e}\n{text}", state.phase));
            assert_eq!(Instance::from_record(key(), back), state, "\n{text}");
        }
    }

    #[test]
    fn a_released_staging_instance_is_reaped_but_a_switched_one_is_not() {
        let (state, effects) = run(Instance::staging(key()), vec![(Event::StagingReleased, T0)]);
        assert_eq!(effects, vec![Effect::StopFollower, Effect::Reap]);
        assert_eq!(state.phase, Phase::Reaped);

        let c = client("dev", "ownership.dev.cnft.dev");
        let (state, effects) = run(
            Instance::staging(key()),
            vec![(attach(&c, dev()), T0), (Event::StagingReleased, T0)],
        );
        assert!(
            effects.is_empty(),
            "a stale release must not reap a live instance"
        );
        assert_eq!(state.phase, Phase::Active);
    }

    #[test]
    fn evict_refuses_while_attached_unless_forced() {
        let c = client("prod", "ownership.cnft.dev");
        let (state, _) = run(Instance::staging(key()), vec![(attach(&c, prod()), T0)]);
        assert_eq!(
            state.step(
                Event::Operator(OperatorCommand::Evict(EvictMode::RefuseIfAttached)),
                T0
            ),
            Err(Rejected::StillAttached(1))
        );
        let forced = state
            .step(
                Event::Operator(OperatorCommand::Evict(EvictMode::Force)),
                T0,
            )
            .unwrap();
        assert_eq!(forced.effects, vec![Effect::StopFollower, Effect::Reap]);
        assert_eq!(forced.next.step(Event::Tick, T0), Err(Rejected::Reaped));
    }

    // ── Exhaustive exploration ───────────────────────────────────────────
    //
    // Hand-picked scenarios prove the cases someone thought of. This walks
    // EVERY event sequence up to a depth, from every reachable state, and
    // checks invariants that must hold everywhere. Time advances by more than
    // any timer on each tick, so every timer-driven edge is taken.

    fn alphabet() -> Vec<Event> {
        let p = client("prod", "p");
        let d = client("dev", "d");
        let mut events = vec![Event::Tick, Event::StagingReleased];
        for c in [&p, &d] {
            let profile = if c.namespace.as_str() == "prod" {
                prod()
            } else {
                dev()
            };
            events.push(attach(c, profile));
            for cause in [
                DetachCause::Moved,
                DetachCause::Unsubscribed,
                DetachCause::LeaseExpired,
            ] {
                events.push(detach(c, cause));
            }
        }
        for command in [
            OperatorCommand::Stop,
            OperatorCommand::Start,
            OperatorCommand::Pin,
            OperatorCommand::Unpin,
            OperatorCommand::Evict(EvictMode::RefuseIfAttached),
            OperatorCommand::Evict(EvictMode::Force),
        ] {
            events.push(Event::Operator(command));
        }
        events
    }

    /// Whether the follower is running, reconstructed ONLY from effects — the
    /// host's view. `Staging` is born running.
    fn fold_running(running: bool, effects: &[Effect]) -> bool {
        let mut running = running;
        for effect in effects {
            match effect {
                Effect::StartFollower => {
                    assert!(!running, "StartFollower on a follower already running");
                    running = true;
                }
                Effect::StopFollower => {
                    assert!(running, "StopFollower on a follower not running");
                    running = false;
                }
                Effect::Reap => assert!(!running, "Reap while the follower still runs"),
                Effect::Alert(_) => {}
            }
        }
        running
    }

    fn check(state: &Instance, running: bool) {
        let phase_running = matches!(
            state.phase,
            Phase::Staging | Phase::Active | Phase::Draining { .. }
        );
        assert_eq!(
            running, phase_running,
            "effects and phase disagree about the follower: {state:?}"
        );
        if state.phase == Phase::Active {
            assert!(
                !state.attached.is_empty() || state.desired == DesiredState::Pinned,
                "Active with no clients and not pinned: {state:?}"
            );
        }
        if state.desired == DesiredState::Stopped {
            assert!(!running, "desired Stopped but running: {state:?}");
            assert_eq!(state.on_boot(), BootAction::LeaveStopped);
        }
        if state.phase == Phase::Reaped {
            assert!(state.attached.is_empty(), "reaped with clients: {state:?}");
        }
        if let Phase::Stopped {
            cause: StopCause::ZeroRef { .. },
            ..
        } = state.phase
        {
            assert!(
                state.attached.is_empty(),
                "stopped for having no clients, yet has clients: {state:?}"
            );
        }
    }

    #[test]
    fn invariants_hold_over_every_event_sequence() {
        let events = alphabet();
        // Three ways an instance comes to exist: created by a move, adopted
        // with clients, adopted with none. All are born with a follower.
        let with_clients = BTreeMap::from([(client("prod", "p"), prod())]);
        let mut frontier = vec![
            (Instance::staging(key()), true, T0),
            (Instance::adopt(key(), &with_clients), true, T0),
            (Instance::adopt(key(), &BTreeMap::new()), true, T0),
        ];
        let mut visited = 0_usize;
        for _depth in 0..6 {
            let mut next_frontier = Vec::new();
            for (state, running, now) in &frontier {
                for event in &events {
                    // Every step jumps past the longest timer (72h).
                    let now = UnixSecs(now.0 + 100 * 3600);
                    let Ok(t) = state.step(event.clone(), now) else {
                        continue;
                    };
                    let running = fold_running(*running, &t.effects);
                    check(&t.next, running);
                    visited += 1;
                    next_frontier.push((t.next, running, now));
                }
            }
            next_frontier.sort_by(|a, b| format!("{:?}", a.0).cmp(&format!("{:?}", b.0)));
            next_frontier.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
            frontier = next_frontier;
        }
        assert!(visited > 1_000, "exploration was vacuous: {visited} steps");
    }

    /// Under `AlertOnly`, no sequence of ticks alone ever stops or reaps an
    /// abandoned instance. Only an operator can.
    #[test]
    fn alert_only_never_times_out() {
        let c = client("prod", "p");
        let (mut state, _) = run(
            Instance::staging(key()),
            vec![
                (attach(&c, prod()), T0),
                (detach(&c, DetachCause::Unsubscribed), T0),
            ],
        );
        for year in 1..50 {
            let t = state
                .step(Event::Tick, after(year * 365 * 24 * 3600))
                .unwrap();
            assert!(t.effects.is_empty());
            state = t.next;
        }
        assert!(matches!(state.phase, Phase::Draining { .. }));
    }
}
