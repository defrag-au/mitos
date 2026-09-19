//! Per-namespace policy, and how policies combine on a shared instance — D9.
//!
//! An instance is not tied to a namespace: `dev` and `prod` announcing the
//! same digest share one. So "which policy governs this instance" needs an
//! answer, and the design's first draft — "the strictest of each" — is wrong
//! for half the fields. [`NamespaceProfile::protect`] is the precise version.

use serde::{Deserialize, Serialize};

/// What happens to an instance whose clients VANISHED (unsubscribed, or their
/// lease expired). Says nothing about a SUPERSEDED instance — one every client
/// moved off — which retires automatically under every policy; see
/// [`crate::instance::ZeroRefCause`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum EjectPolicy {
    /// Keep running for `grace_secs` (a quick re-subscribe loses nothing),
    /// then stop; keep the stopped state for `retain_secs` (a slower one
    /// resumes from its cursor instead of rebootstrapping), then reap.
    Automatic { grace_secs: u64, retain_secs: u64 },
    /// Alert and wait for an operator. The instance keeps RUNNING: in
    /// production a vanished client is far more likely to be an outage on the
    /// client's side than a decision, and stopping would turn its recovery
    /// into a rebootstrap.
    AlertOnly,
}

/// Whether a client may move to a LOWER revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RevisionPolicy {
    /// Only through an explicit operator rollback, to a retained instance.
    ExplicitRollbackOnly,
    /// Freely — a developer checking out an older commit is not an incident.
    /// No upgrade actions run on the way down.
    Allow,
}

/// Queue order for staging catch-ups, which run one at a time host-wide
/// because disk IO is the contended resource on the box.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Tier {
    // Declaration order IS queue order: `Production` sorts first.
    Production,
    Development,
}

/// Resource ceilings for an instance's followers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceProfile {
    pub fuel_per_call: u64,
    /// `None` = no ceiling beyond the host's own. Production today.
    pub max_memory_bytes: Option<u64>,
    /// Concurrent backfill-plane scans this instance may run.
    pub backfill_concurrency: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceProfile {
    pub tier: Tier,
    pub eject: EjectPolicy,
    pub revision: RevisionPolicy,
    /// How long a SUPERSEDED instance's stopped state is kept, so an explicit
    /// rollback has something to roll back to.
    pub retain_superseded_secs: u64,
    pub resources: ResourceProfile,
}

impl NamespaceProfile {
    /// The profile governing an instance that serves clients under BOTH.
    ///
    /// Not "the strictest of each", because strict points in opposite
    /// directions depending on the field:
    ///
    /// - **Lifecycle fields take the more CAUTIOUS value.** An instance `prod`
    ///   relies on must not be reaped on `dev`'s ten-minute timer.
    /// - **Resource fields take the more GENEROUS value.** The same instance
    ///   must not be starved by `dev`'s fuel ceiling while it serves `prod`.
    ///
    /// Both halves protect the same thing — the most important client attached
    /// — hence the name.
    pub fn protect(self, other: NamespaceProfile) -> NamespaceProfile {
        NamespaceProfile {
            tier: self.tier.min(other.tier),
            eject: match (self.eject, other.eject) {
                (EjectPolicy::AlertOnly, _) | (_, EjectPolicy::AlertOnly) => EjectPolicy::AlertOnly,
                (
                    EjectPolicy::Automatic {
                        grace_secs: g1,
                        retain_secs: r1,
                    },
                    EjectPolicy::Automatic {
                        grace_secs: g2,
                        retain_secs: r2,
                    },
                ) => EjectPolicy::Automatic {
                    grace_secs: g1.max(g2),
                    retain_secs: r1.max(r2),
                },
            },
            revision: match (self.revision, other.revision) {
                (RevisionPolicy::Allow, RevisionPolicy::Allow) => RevisionPolicy::Allow,
                _ => RevisionPolicy::ExplicitRollbackOnly,
            },
            retain_superseded_secs: self
                .retain_superseded_secs
                .max(other.retain_superseded_secs),
            resources: ResourceProfile {
                fuel_per_call: self
                    .resources
                    .fuel_per_call
                    .max(other.resources.fuel_per_call),
                max_memory_bytes: match (
                    self.resources.max_memory_bytes,
                    other.resources.max_memory_bytes,
                ) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    // Either side unlimited ⇒ unlimited.
                    _ => None,
                },
                backfill_concurrency: self
                    .resources
                    .backfill_concurrency
                    .max(other.resources.backfill_concurrency),
            },
        }
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    /// The `prod` row of the D9 table.
    pub fn prod() -> NamespaceProfile {
        NamespaceProfile {
            tier: Tier::Production,
            eject: EjectPolicy::AlertOnly,
            revision: RevisionPolicy::ExplicitRollbackOnly,
            retain_superseded_secs: 72 * 3600,
            resources: ResourceProfile {
                fuel_per_call: 100_000_000,
                max_memory_bytes: None,
                backfill_concurrency: 4,
            },
        }
    }

    /// The `dev` row of the D9 table.
    pub fn dev() -> NamespaceProfile {
        NamespaceProfile {
            tier: Tier::Development,
            eject: EjectPolicy::Automatic {
                grace_secs: 600,
                retain_secs: 24 * 3600,
            },
            revision: RevisionPolicy::Allow,
            retain_superseded_secs: 24 * 3600,
            resources: ResourceProfile {
                fuel_per_call: 25_000_000,
                max_memory_bytes: Some(512 * 1024 * 1024),
                backfill_concurrency: 1,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{dev, prod};
    use super::*;

    #[test]
    fn a_shared_instance_is_neither_reaped_like_dev_nor_starved_like_dev() {
        let shared = dev().protect(prod());
        assert_eq!(shared.eject, EjectPolicy::AlertOnly);
        assert_eq!(shared.revision, RevisionPolicy::ExplicitRollbackOnly);
        assert_eq!(shared.retain_superseded_secs, 72 * 3600);
        assert_eq!(shared.resources.fuel_per_call, 100_000_000);
        assert_eq!(shared.resources.max_memory_bytes, None);
        assert_eq!(shared.tier, Tier::Production);
    }

    #[test]
    fn protect_is_commutative_and_idempotent() {
        assert_eq!(dev().protect(prod()), prod().protect(dev()));
        assert_eq!(dev().protect(dev()), dev());
        assert_eq!(prod().protect(prod()), prod());
    }

    #[test]
    fn production_is_queued_ahead_of_development() {
        assert!(Tier::Production < Tier::Development);
    }
}
