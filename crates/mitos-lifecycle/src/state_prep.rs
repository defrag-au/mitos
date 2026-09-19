//! Preparing a new instance's state — D5.
//!
//! Durable Objects migrate because DO state is primary data. Module state is a
//! CACHE OF THE CHAIN, so rebuilding it is always available and migration is
//! only ever an optimisation. That inverts the usual default: an author writes
//! a migration when a rebuild is too slow, and never otherwise.
//!
//! Three rules the host must honour when executing a [`StatePlan`]:
//!
//! 1. **Never touch the parent's state.** Copy first (`reflink_or_copy`, with
//!    the parent's follower paused at a block boundary so `kv`, `cursor` and
//!    `watched` describe the same block), then work on the copy.
//! 2. **`migrate` completes before `init` or `handle-events` ever run** — the
//!    `blockConcurrencyWhile` analogue.
//! 3. **A failed migration is a rebuild, not a failed move.** Delete the copy
//!    and start empty; the parent never noticed.

use serde::{Deserialize, Serialize};

/// The `state_schema` a manifest declares. Compared, never interpreted.
///
/// `Default` is 0 — what every artifact built before this existed declares by
/// declaring nothing. Two of them are EQUAL, so moving between them is
/// `CopyCompatible`: new code resumes on the old state, exactly as v2's
/// in-place replace did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StateSchema(pub u32);

/// Whether the new artifact exports `migrate`. Read from the component's
/// exports by the host; a named value so `plan(.., true)` cannot be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateExport {
    Present,
    Absent,
}

/// Why a parent's state is being thrown away. Journalled, because "why did a
/// deploy rebootstrap 420K shards" is a question that gets asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RebuildReason {
    /// The schema moved forward and the module offers no way across.
    NoMigrateExport,
    /// The schema moved BACKWARDS. Migrations are forward-only, so an older
    /// artifact cannot read a newer layout.
    SchemaWentBackwards,
    /// `migrate` returned an error or trapped. The copy was discarded.
    MigrateFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "plan")]
pub enum StatePlan {
    /// No parent instance — a first subscribe. Empty state, ordinary
    /// bootstrap. Distinct from `Rebuild` because nothing was discarded.
    Fresh,
    /// Same schema: the parent's files are valid as they stand.
    CopyCompatible,
    /// Copy, then run the module's `migrate(from)` on the copy.
    CopyThenMigrate { from: StateSchema },
    /// Start empty although a parent exists.
    Rebuild { reason: RebuildReason },
}

pub fn plan(parent: Option<StateSchema>, target: StateSchema, migrate: MigrateExport) -> StatePlan {
    let Some(parent) = parent else {
        return StatePlan::Fresh;
    };
    if parent == target {
        return StatePlan::CopyCompatible;
    }
    if parent > target {
        return StatePlan::Rebuild {
            reason: RebuildReason::SchemaWentBackwards,
        };
    }
    match migrate {
        MigrateExport::Present => StatePlan::CopyThenMigrate { from: parent },
        MigrateExport::Absent => StatePlan::Rebuild {
            reason: RebuildReason::NoMigrateExport,
        },
    }
}

/// One call to the module's re-entrant `migrate` export. Same shape as
/// `rebootstrap`, and for the same reason: one fuel budget cannot rewrite
/// ~420K shards, so the host loops, refuelling between calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateCall {
    /// `Ok({ done, migrated })` from the guest.
    Stepped {
        progress: MigrateProgress,
        migrated: u64,
    },
    /// `Err(_)` from the guest, or a trap. Indistinguishable on purpose: both
    /// mean the copy can no longer be trusted.
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateProgress {
    MoreToDo,
    Done,
}

/// What the host does after a `migrate` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateNext {
    /// Refuel and call again.
    CallAgain { migrated_so_far: u64 },
    /// The copy is now at the target schema; start the follower on it.
    Ready { migrated: u64 },
    /// Delete the copy and proceed as `Rebuild { MigrateFailed }`.
    DiscardAndRebuild,
}

pub fn after_migrate_call(migrated_so_far: u64, call: MigrateCall) -> MigrateNext {
    match call {
        MigrateCall::Failed => MigrateNext::DiscardAndRebuild,
        MigrateCall::Stepped { progress, migrated } => {
            let total = migrated_so_far.saturating_add(migrated);
            match progress {
                MigrateProgress::MoreToDo => MigrateNext::CallAgain {
                    migrated_so_far: total,
                },
                MigrateProgress::Done => MigrateNext::Ready { migrated: total },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: StateSchema = StateSchema(1);
    const V2: StateSchema = StateSchema(2);

    #[test]
    fn the_three_cases_of_the_design_table() {
        assert_eq!(
            plan(Some(V1), V1, MigrateExport::Absent),
            StatePlan::CopyCompatible
        );
        assert_eq!(
            plan(Some(V1), V2, MigrateExport::Present),
            StatePlan::CopyThenMigrate { from: V1 }
        );
        assert_eq!(
            plan(Some(V1), V2, MigrateExport::Absent),
            StatePlan::Rebuild {
                reason: RebuildReason::NoMigrateExport
            }
        );
    }

    /// An author who never writes a migration gets a rebuild, which is the
    /// entire promise of "rebuild by default".
    #[test]
    fn no_parent_is_fresh_whatever_the_module_exports() {
        assert_eq!(plan(None, V2, MigrateExport::Present), StatePlan::Fresh);
        assert_eq!(plan(None, V2, MigrateExport::Absent), StatePlan::Fresh);
    }

    #[test]
    fn a_rollback_across_a_schema_bump_rebuilds_even_with_migrate_exported() {
        assert_eq!(
            plan(Some(V2), V1, MigrateExport::Present),
            StatePlan::Rebuild {
                reason: RebuildReason::SchemaWentBackwards
            }
        );
    }

    #[test]
    fn migrate_is_reentrant_and_a_failure_at_any_step_discards_the_copy() {
        let step = |progress, migrated| MigrateCall::Stepped { progress, migrated };
        assert_eq!(
            after_migrate_call(0, step(MigrateProgress::MoreToDo, 5_000)),
            MigrateNext::CallAgain {
                migrated_so_far: 5_000
            }
        );
        assert_eq!(
            after_migrate_call(5_000, step(MigrateProgress::Done, 1_200)),
            MigrateNext::Ready { migrated: 6_200 }
        );
        assert_eq!(
            after_migrate_call(5_000, MigrateCall::Failed),
            MigrateNext::DiscardAndRebuild
        );
    }
}
