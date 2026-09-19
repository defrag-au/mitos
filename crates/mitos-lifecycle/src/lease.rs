//! The client lease — D10. How the host learns a client is GONE.
//!
//! v2 had no liveness at all: a dead companion was dialled at a capped 60s
//! backoff for ever, and its queued emissions were never purged. v3 cannot
//! afford that, because a subscribe can now CREATE an instance, so something
//! has to notice when the reason for it has disappeared.
//!
//! The subtlety is that silence is not death. A Durable Object hibernates; it
//! is dialled BY mitos and answers when dialled. So a lease is renewed by
//! contact in either direction, and it is only FAILURE that counts against it
//! — with the kind of failure deciding how long to wait:
//!
//! - [`DialFailureClass::Gone`] — `404` / `410`. Cloudflare is saying there is
//!   no such worker. Short fuse.
//! - [`DialFailureClass::Unavailable`] — `5xx`, timeouts, connection errors.
//!   That is an outage, and outages end. Long fuse.
//!
//! A module that never emits never dials, so a healthy lease on a quiet module
//! would otherwise never be renewed OR tested — hence the probe.

use serde::{Deserialize, Serialize};

use crate::{Transition, UnixSecs};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DialFailureClass {
    Gone,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeasePolicy {
    /// Continuous `Gone` responses for this long ⇒ expired.
    pub gone_fuse_secs: u64,
    /// ANY unbroken failure for this long ⇒ expired.
    pub unavailable_fuse_secs: u64,
    /// No contact for this long ⇒ probe.
    pub probe_after_idle_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", tag = "state")]
pub enum Lease {
    Live {
        last_contact: UnixSecs,
        /// When a probe was last ASKED for, so a slow probe is not re-issued
        /// on every tick.
        last_probe: Option<UnixSecs>,
    },
    Failing {
        /// Start of the unbroken run of failures. Drives the long fuse.
        first_failure: UnixSecs,
        /// Start of the unbroken run of `Gone` WITHIN it. Drives the short
        /// fuse. Cleared by an `Unavailable`: a `503` after a `404` means
        /// something is answering again — most likely a deploy in progress —
        /// so the short fuse restarts while the long one keeps running.
        gone_since: Option<UnixSecs>,
    },
    /// Terminal. The host removes the client's rows, which detaches it from
    /// its instances with [`crate::instance::DetachCause::LeaseExpired`].
    Expired { class: DialFailureClass },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// An authenticated subscribe, an acked delivery, or a probe answered.
    Contact,
    DialFailed(DialFailureClass),
    Tick,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// Dial the client's liveness endpoint. The answer comes back as
    /// `Contact` or `DialFailed`.
    Probe,
    /// Remove the client's rows and subscriptions.
    Expire { class: DialFailureClass },
}

impl Lease {
    pub fn granted(now: UnixSecs) -> Self {
        Lease::Live {
            last_contact: now,
            last_probe: None,
        }
    }

    /// Never rejects: an expired lease absorbs everything except `Contact`,
    /// which the host should never deliver to a removed client anyway.
    pub fn step(
        self,
        event: Event,
        policy: &LeasePolicy,
        now: UnixSecs,
    ) -> Transition<Lease, Effect> {
        match (self, event) {
            (Lease::Expired { .. }, _) => Transition::quiet(self),
            (_, Event::Contact) => Transition::quiet(Lease::granted(now)),

            (Lease::Live { .. }, Event::DialFailed(class)) => Transition::quiet(Lease::Failing {
                first_failure: now,
                gone_since: (class == DialFailureClass::Gone).then_some(now),
            }),
            (
                Lease::Failing {
                    first_failure,
                    gone_since,
                },
                Event::DialFailed(class),
            ) => {
                let gone_since = match class {
                    DialFailureClass::Gone => gone_since.or(Some(now)),
                    DialFailureClass::Unavailable => None,
                };
                Lease::Failing {
                    first_failure,
                    gone_since,
                }
                .expire_if_due(policy, now)
            }

            (
                Lease::Live {
                    last_contact,
                    last_probe,
                },
                Event::Tick,
            ) => {
                let idle = now.since(last_contact) >= policy.probe_after_idle_secs;
                let probe_outstanding =
                    last_probe.is_some_and(|asked| now.since(asked) < policy.probe_after_idle_secs);
                if idle && !probe_outstanding {
                    Transition::with(
                        Lease::Live {
                            last_contact,
                            last_probe: Some(now),
                        },
                        vec![Effect::Probe],
                    )
                } else {
                    Transition::quiet(self)
                }
            }
            (Lease::Failing { .. }, Event::Tick) => self.expire_if_due(policy, now),
        }
    }

    fn expire_if_due(self, policy: &LeasePolicy, now: UnixSecs) -> Transition<Lease, Effect> {
        let Lease::Failing {
            first_failure,
            gone_since,
        } = self
        else {
            return Transition::quiet(self);
        };
        let class = if gone_since.is_some_and(|g| now.since(g) >= policy.gone_fuse_secs) {
            Some(DialFailureClass::Gone)
        } else if now.since(first_failure) >= policy.unavailable_fuse_secs {
            Some(DialFailureClass::Unavailable)
        } else {
            None
        };
        match class {
            Some(class) => {
                Transition::with(Lease::Expired { class }, vec![Effect::Expire { class }])
            }
            None => Transition::quiet(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: LeasePolicy = LeasePolicy {
        gone_fuse_secs: 3600,
        unavailable_fuse_secs: 7 * 24 * 3600,
        probe_after_idle_secs: 6 * 3600,
    };
    const T0: UnixSecs = UnixSecs(1_000_000);

    fn at(secs: u64) -> UnixSecs {
        UnixSecs(T0.0 + secs)
    }

    fn run(start: Lease, events: &[(Event, u64)]) -> (Lease, Vec<Effect>) {
        let mut lease = start;
        let mut effects = Vec::new();
        for (event, secs) in events {
            let t = lease.step(*event, &POLICY, at(*secs));
            lease = t.next;
            effects.extend(t.effects);
        }
        (lease, effects)
    }

    use DialFailureClass::{Gone, Unavailable};

    #[test]
    fn a_deleted_worker_expires_on_the_short_fuse() {
        let (lease, effects) = run(
            Lease::granted(T0),
            &[
                (Event::DialFailed(Gone), 10),
                (Event::Tick, 3_609),
                (Event::Tick, 3_610),
            ],
        );
        assert_eq!(effects, vec![Effect::Expire { class: Gone }]);
        assert_eq!(lease, Lease::Expired { class: Gone });
    }

    #[test]
    fn an_outage_is_waited_out_for_a_week() {
        let (lease, effects) = run(
            Lease::granted(T0),
            &[
                (Event::DialFailed(Unavailable), 0),
                (Event::Tick, 6 * 24 * 3600),
            ],
        );
        assert!(effects.is_empty());
        assert!(matches!(lease, Lease::Failing { .. }));

        let (_, effects) = run(lease, &[(Event::Tick, 7 * 24 * 3600)]);
        assert_eq!(effects, vec![Effect::Expire { class: Unavailable }]);
    }

    #[test]
    fn one_successful_contact_forgives_everything() {
        let (lease, effects) = run(
            Lease::granted(T0),
            &[
                (Event::DialFailed(Gone), 0),
                (Event::Contact, 3_000),
                (Event::Tick, 3_700),
            ],
        );
        assert!(effects.is_empty());
        assert!(matches!(lease, Lease::Live { .. }));
    }

    /// A deploy in progress looks like 404 → 503 → 404. The short fuse must
    /// restart at each change, or a slow deploy reaps a healthy client.
    #[test]
    fn a_503_restarts_the_short_fuse_but_not_the_long_one() {
        let (lease, effects) = run(
            Lease::granted(T0),
            &[
                (Event::DialFailed(Gone), 0),
                (Event::DialFailed(Unavailable), 3_000),
                (Event::DialFailed(Gone), 3_100),
                (Event::Tick, 3_700),
            ],
        );
        assert!(effects.is_empty(), "only 600s of unbroken Gone");
        assert_eq!(
            lease,
            Lease::Failing {
                first_failure: T0,
                gone_since: Some(at(3_100)),
            }
        );
    }

    /// …and flapping between the two cannot dodge expiry for ever.
    #[test]
    fn flapping_still_expires_on_the_long_fuse() {
        let mut lease = Lease::granted(T0);
        let mut expired = None;
        for minute in 0..(8 * 24 * 60) {
            let class = if minute % 2 == 0 { Gone } else { Unavailable };
            let t = lease.step(Event::DialFailed(class), &POLICY, at(minute * 60));
            lease = t.next;
            if let Some(Effect::Expire { class }) = t.effects.first() {
                expired = Some((*class, minute * 60));
                break;
            }
        }
        assert_eq!(expired, Some((Unavailable, 7 * 24 * 3600)));
    }

    #[test]
    fn a_quiet_module_is_probed_once_per_interval_not_once_per_tick() {
        let (_, effects) = run(
            Lease::granted(T0),
            &[
                (Event::Tick, 6 * 3600 - 1),
                (Event::Tick, 6 * 3600),
                (Event::Tick, 6 * 3600 + 60),
                (Event::Tick, 6 * 3600 + 120),
                (Event::Tick, 12 * 3600),
            ],
        );
        assert_eq!(effects, vec![Effect::Probe, Effect::Probe]);
    }

    #[test]
    fn expiry_is_terminal_and_fires_once() {
        let (lease, effects) = run(
            Lease::Expired { class: Gone },
            &[
                (Event::Tick, 0),
                (Event::DialFailed(Gone), 1),
                (Event::Contact, 2),
            ],
        );
        assert!(effects.is_empty());
        assert_eq!(lease, Lease::Expired { class: Gone });
    }
}
