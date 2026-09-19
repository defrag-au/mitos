//! Upgrade actions — D6.
//!
//! A manifest declares what must happen when a client crosses a revision:
//!
//! ```toml
//! [[upgrade]]
//! since = 3
//! actions = ["recapture"]
//!
//! [[upgrade]]
//! since = 5
//! actions = ["purge-emissions", "reset-watched"]
//! ```
//!
//! The shape follows wrangler's `[[migrations]] tag = …` because it solves the
//! same problem: **environments skip builds.** `dev` takes revisions 3, 4 and
//! 5; `prod` jumps 2 → 5. A flat per-build list would lose revision 3's
//! `recapture` on prod the moment revision 5's manifest stopped repeating it.
//! So the ledger is the CLIENT ROW's revision, and a move runs the UNION of
//! every entry it crosses.
//!
//! This module turns (entries, from, to) into a plan. It executes nothing.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::ids::Revision;
use crate::policy::RevisionPolicy;

/// One thing a manifest may ask for on upgrade.
///
/// Deserialising an UNKNOWN action is an error, and must stay one: a host that
/// skipped an action it did not understand would cut a client over without the
/// `recapture` its author knew it needed. Unverifiable is not fine — the same
/// rule `abi.wit_sha` enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpgradeAction {
    /// Clear the new instance's watched-UTxO index when it is created.
    ResetWatched,
    /// Discard the moving client's undelivered emissions on the OLD instance
    /// rather than draining them first. For when the old wire shape is not
    /// worth delivering.
    PurgeEmissions,
    /// The module re-emits current state to the moving client; companions keep
    /// what they have.
    Resync,
    /// The moving client's companions DROP projected state and are refilled.
    Recapture,
}

/// Who an action applies to. This is what lets two clients share one instance
/// and still each get exactly their own `recapture`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionScope {
    /// Runs ONCE, when the instance is created from its parent. A client that
    /// later attaches to the existing instance does not re-run it.
    Instance,
    /// Runs for EACH client that crosses the revision, at its own switch.
    Client,
}

impl UpgradeAction {
    pub fn scope(self) -> ActionScope {
        match self {
            UpgradeAction::ResetWatched => ActionScope::Instance,
            UpgradeAction::PurgeEmissions | UpgradeAction::Resync | UpgradeAction::Recapture => {
                ActionScope::Client
            }
        }
    }
}

/// One `[[upgrade]]` table. `actions` is a SET: the host runs a fixed order of
/// its own ([`ClientPlan`]), never the author's, because `recapture` before
/// the switch would refill companions from the instance they are leaving.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeEntry {
    pub since: Revision,
    pub actions: BTreeSet<UpgradeAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    #[error("[[upgrade]] since = {since} is ahead of the manifest's own revision {revision}")]
    SinceAheadOfRevision { since: u32, revision: u32 },
    #[error("[[upgrade]] since = 0 can never be crossed — revisions start at 1")]
    SinceZero,
    #[error("two [[upgrade]] tables declare since = {0}; merge them")]
    DuplicateSince(u32),
}

/// Manifest-load validation. A ledger that fails this is a refused module.
pub fn validate(entries: &[UpgradeEntry], revision: Revision) -> Result<(), LedgerError> {
    let mut seen = BTreeSet::new();
    for entry in entries {
        if entry.since.0 == 0 {
            return Err(LedgerError::SinceZero);
        }
        if entry.since > revision {
            return Err(LedgerError::SinceAheadOfRevision {
                since: entry.since.0,
                revision: revision.0,
            });
        }
        if !seen.insert(entry.since) {
            return Err(LedgerError::DuplicateSince(entry.since.0));
        }
    }
    Ok(())
}

/// What happens to the moving client's undelivered emissions on the old
/// instance. A named decision because it gates the switch: `Drain` waits,
/// `Purge` does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EmissionHandoff {
    Drain,
    Purge,
}

/// What the new instance does for the client once it has switched.
///
/// One value, not two flags: `Recapture` (drop + refill) strictly subsumes
/// `Resync` (refill), so a move that crosses both runs the refill once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Refill {
    None,
    Resync,
    Recapture,
}

/// The client-scoped half of a move, in the order the host runs it:
/// `handoff` BEFORE the switch, `refill` AFTER.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientPlan {
    pub handoff: EmissionHandoff,
    pub refill: Refill,
}

impl ClientPlan {
    /// A move that crosses no declared action: drain, switch, nothing else.
    pub const QUIET: ClientPlan = ClientPlan {
        handoff: EmissionHandoff::Drain,
        refill: Refill::None,
    };
}

/// Whether a new instance carries its parent's watched-UTxO index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum WatchedIndex {
    Carry,
    Reset,
}

/// The instance-scoped half, applied once at creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstancePlan {
    pub watched: WatchedIndex,
}

/// Why this move is happening. `ExplicitRollback` is the operator's admin op;
/// everything a worker announces is `Announced`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MoveIntent {
    Announced,
    ExplicitRollback,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UpgradeRefusal {
    #[error(
        "revision {to} is behind the client's current revision {from}, and this namespace only \
         moves backwards through an explicit rollback"
    )]
    RollbackNotPermitted { from: u32, to: u32 },
}

/// The direction of a move, decided once so both resolvers agree on it.
enum Direction {
    /// First subscribe, a same-revision rebuild, or a permitted move down:
    /// nothing is crossed, so nothing runs.
    Nothing,
    Crosses {
        above: Revision,
        up_to: Revision,
    },
}

fn direction(
    from: Option<Revision>,
    to: Revision,
    policy: RevisionPolicy,
    intent: MoveIntent,
) -> Result<Direction, UpgradeRefusal> {
    let Some(from) = from else {
        return Ok(Direction::Nothing);
    };
    if to > from {
        return Ok(Direction::Crosses {
            above: from,
            up_to: to,
        });
    }
    if to < from
        && policy == RevisionPolicy::ExplicitRollbackOnly
        && intent == MoveIntent::Announced
    {
        return Err(UpgradeRefusal::RollbackNotPermitted {
            from: from.0,
            to: to.0,
        });
    }
    Ok(Direction::Nothing)
}

fn crossed(entries: &[UpgradeEntry], direction: &Direction) -> BTreeSet<UpgradeAction> {
    match direction {
        Direction::Nothing => BTreeSet::new(),
        Direction::Crosses { above, up_to } => entries
            .iter()
            .filter(|e| e.since > *above && e.since <= *up_to)
            .flat_map(|e| e.actions.iter().copied())
            .collect(),
    }
}

/// The client-scoped plan for a row moving `from` → `to`.
///
/// `from` is the CLIENT ROW's revision — `None` on a first subscribe, where
/// there is nothing to upgrade from and the ordinary subscribe-time resync
/// already covers the refill.
pub fn resolve_client(
    entries: &[UpgradeEntry],
    from: Option<Revision>,
    to: Revision,
    policy: RevisionPolicy,
    intent: MoveIntent,
) -> Result<ClientPlan, UpgradeRefusal> {
    let actions = crossed(entries, &direction(from, to, policy, intent)?);
    Ok(ClientPlan {
        handoff: if actions.contains(&UpgradeAction::PurgeEmissions) {
            EmissionHandoff::Purge
        } else {
            EmissionHandoff::Drain
        },
        refill: if actions.contains(&UpgradeAction::Recapture) {
            Refill::Recapture
        } else if actions.contains(&UpgradeAction::Resync) {
            Refill::Resync
        } else {
            Refill::None
        },
    })
}

/// The instance-scoped plan for an instance at `to` created from a parent at
/// `parent`. `None` = no parent: the state is fresh and there is nothing to
/// reset. Never refuses — whether the MOVE is permitted is the client row's
/// question, answered by [`resolve_client`].
pub fn resolve_instance(
    entries: &[UpgradeEntry],
    parent: Option<Revision>,
    to: Revision,
) -> InstancePlan {
    let direction = match parent {
        Some(parent) if to > parent => Direction::Crosses {
            above: parent,
            up_to: to,
        },
        _ => Direction::Nothing,
    };
    let actions = crossed(entries, &direction);
    InstancePlan {
        watched: if actions.contains(&UpgradeAction::ResetWatched) {
            WatchedIndex::Reset
        } else {
            WatchedIndex::Carry
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct ManifestSlice {
        #[serde(default)]
        upgrade: Vec<UpgradeEntry>,
    }

    fn ledger() -> Vec<UpgradeEntry> {
        toml::from_str::<ManifestSlice>(
            r#"
            [[upgrade]]
            since = 3
            actions = ["recapture"]

            [[upgrade]]
            since = 4
            actions = ["resync"]

            [[upgrade]]
            since = 5
            actions = ["purge-emissions", "reset-watched"]
            "#,
        )
        .expect("the documented shape parses")
        .upgrade
    }

    const PROD: RevisionPolicy = RevisionPolicy::ExplicitRollbackOnly;
    const DEV: RevisionPolicy = RevisionPolicy::Allow;

    fn client(from: Option<u32>, to: u32, policy: RevisionPolicy) -> ClientPlan {
        resolve_client(
            &ledger(),
            from.map(Revision),
            Revision(to),
            policy,
            MoveIntent::Announced,
        )
        .expect("permitted")
    }

    #[test]
    fn an_unknown_action_refuses_the_whole_manifest() {
        let parsed = toml::from_str::<ManifestSlice>(
            "[[upgrade]]\nsince = 2\nactions = [\"recapture\", \"defragment-the-moon\"]",
        );
        assert!(
            parsed.is_err(),
            "an action this host does not understand must never be skipped"
        );
    }

    /// THE reason the ledger is keyed by revision: prod jumps 2 → 5 and must
    /// still run what revision 3 declared.
    #[test]
    fn a_client_that_skips_builds_still_runs_everything_it_crossed() {
        let plan = client(Some(2), 5, PROD);
        assert_eq!(plan.refill, Refill::Recapture);
        assert_eq!(plan.handoff, EmissionHandoff::Purge);
    }

    #[test]
    fn an_action_runs_once_per_client_however_the_builds_were_taken() {
        // dev walks 2 → 3 → 4 → 5; the recapture belongs to the first hop only.
        assert_eq!(client(Some(2), 3, DEV).refill, Refill::Recapture);
        assert_eq!(client(Some(3), 4, DEV).refill, Refill::Resync);
        assert_eq!(client(Some(4), 5, DEV).refill, Refill::None);
        assert_eq!(client(Some(4), 5, DEV).handoff, EmissionHandoff::Purge);
    }

    #[test]
    fn recapture_subsumes_resync_rather_than_refilling_twice() {
        assert_eq!(client(Some(2), 4, DEV).refill, Refill::Recapture);
    }

    #[test]
    fn a_first_subscribe_and_a_same_revision_rebuild_run_nothing() {
        assert_eq!(client(None, 5, PROD), ClientPlan::QUIET);
        assert_eq!(client(Some(5), 5, PROD), ClientPlan::QUIET);
    }

    #[test]
    fn moving_down_is_refused_in_prod_and_silent_in_dev() {
        let refused = resolve_client(
            &ledger(),
            Some(Revision(5)),
            Revision(3),
            PROD,
            MoveIntent::Announced,
        );
        assert_eq!(
            refused,
            Err(UpgradeRefusal::RollbackNotPermitted { from: 5, to: 3 })
        );
        // The operator's explicit rollback is the one way down in prod, and
        // going down runs no actions anywhere — they describe moving UP.
        let explicit = resolve_client(
            &ledger(),
            Some(Revision(5)),
            Revision(3),
            PROD,
            MoveIntent::ExplicitRollback,
        );
        assert_eq!(explicit, Ok(ClientPlan::QUIET));
        assert_eq!(client(Some(5), 3, DEV), ClientPlan::QUIET);
    }

    #[test]
    fn instance_actions_key_off_the_parent_not_the_client() {
        let l = ledger();
        assert_eq!(
            resolve_instance(&l, Some(Revision(4)), Revision(5)).watched,
            WatchedIndex::Reset
        );
        assert_eq!(
            resolve_instance(&l, Some(Revision(5)), Revision(5)).watched,
            WatchedIndex::Carry
        );
        // No parent ⇒ fresh state ⇒ nothing to reset.
        assert_eq!(
            resolve_instance(&l, None, Revision(5)).watched,
            WatchedIndex::Carry
        );
    }

    #[test]
    fn scopes_are_what_let_two_clients_share_an_instance() {
        assert_eq!(UpgradeAction::ResetWatched.scope(), ActionScope::Instance);
        for a in [
            UpgradeAction::Recapture,
            UpgradeAction::Resync,
            UpgradeAction::PurgeEmissions,
        ] {
            assert_eq!(a.scope(), ActionScope::Client);
        }
    }

    #[test]
    fn a_malformed_ledger_is_caught_at_manifest_load() {
        let l = ledger();
        assert_eq!(validate(&l, Revision(5)), Ok(()));
        assert_eq!(
            validate(&l, Revision(4)),
            Err(LedgerError::SinceAheadOfRevision {
                since: 5,
                revision: 4
            })
        );
        let mut dup = l.clone();
        dup.push(l[0].clone());
        assert_eq!(
            validate(&dup, Revision(5)),
            Err(LedgerError::DuplicateSince(3))
        );
        let zero = vec![UpgradeEntry {
            since: Revision(0),
            actions: BTreeSet::new(),
        }];
        assert_eq!(validate(&zero, Revision(5)), Err(LedgerError::SinceZero));
    }
}
