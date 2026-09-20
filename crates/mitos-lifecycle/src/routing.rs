//! Which instance speaks to which client.
//!
//! v2 has one follower per module, and everything it emits is fanned out to
//! every companion registered under that module. The moment a SECOND instance
//! of a module can run, that stops being a fact about the module and becomes
//! a decision per `(instance, client)` — and getting it wrong is silent in
//! both directions: a client served by two instances receives every event
//! twice, and a client served by none receives nothing and no error.
//!
//! The rule: **a client is served by the instance its row names; a client
//! with no placement is served by the module's CURRENT instance** — the one
//! `current.toml` points at, which is also what every `&str`-addressed
//! operator tool means by "the module".
//!
//! # The trap this is shaped around
//!
//! An operator upload (`activate`) does not create a new instance directory.
//! It RENAMES the live one to the new key, so the new code resumes on the old
//! state — v2's replace semantics, preserved on purpose until the client move
//! retires uploads as a delivery path. So a row written while the module was
//! `K1` still says `K1` after an upload has made it `K2`, and `K1` no longer
//! exists anywhere. Every deploy does this today.
//!
//! A row naming an instance that is GONE is therefore not an error and not a
//! reason to stop delivering: it is a client that has not been moved by
//! anything, and it follows the module's current instance like a client with
//! no row at all. Only a row naming ANOTHER instance that really is there
//! takes a client away from this one.
//!
//! The answer is derived on every read and never written back — the README's
//! rule about paths that do not hold the lock. The row is corrected by the
//! next thing that legitimately writes it.

use std::collections::BTreeMap;

use crate::client_move::ClientRow;
use crate::ids::{ClientKey, InstanceKey};
use crate::policy::NamespaceProfile;

/// Whether an instance's directory exists. The one thing about the world
/// [`fan_out`] needs, asked lazily: it matters only for a row that names some
/// OTHER instance, which is rare, and the host pays a `stat` only then.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstanceDir {
    Present,
    Gone,
}

/// What an instance does with an emission for one client's companions.
///
/// There is no `Hold` yet. A staging instance holds a moving client's
/// emissions for the advisory diff (D7), and that arrives WITH the order that
/// implements it — as a new variant here, so every `match` on this becomes a
/// compile error rather than a silent `Skip`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanOut {
    /// Write the row; this instance serves this client.
    Deliver,
    /// Another instance serves this client. Writing a row here would deliver
    /// the event twice.
    Skip,
}

/// Does `this` instance serve the client whose row is `row`?
///
/// `module_current` is what `current.toml` resolves to. `dir_of` is asked at
/// most once, and only about an instance other than `this`.
pub fn fan_out(
    row: Option<&ClientRow>,
    this: &InstanceKey,
    module_current: &InstanceKey,
    dir_of: impl FnOnce(&InstanceKey) -> InstanceDir,
) -> FanOut {
    let follows_current = || {
        if this == module_current {
            FanOut::Deliver
        } else {
            FanOut::Skip
        }
    };
    match row.and_then(|row| row.current.as_ref()) {
        None => follows_current(),
        Some(placement) if &placement.instance == this => FanOut::Deliver,
        Some(placement) => match dir_of(&placement.instance) {
            InstanceDir::Present => FanOut::Skip,
            InstanceDir::Gone => follows_current(),
        },
    }
}

/// What an instance does with an emission NO companion is subscribed to
/// receive.
///
/// v2 keeps such a row under a sentinel companion id so the first subscriber
/// can claim the events it would otherwise have missed. With several
/// instances, each would keep its own copy of the same event and the first
/// subscriber — placed on the current instance — could only ever claim one.
/// The others would sit in stores nothing reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unclaimed {
    Keep,
    Drop,
}

pub fn unclaimed(this: &InstanceKey, module_current: &InstanceKey) -> Unclaimed {
    if this == module_current {
        Unclaimed::Keep
    } else {
        Unclaimed::Drop
    }
}

/// One client of a module, as the host finds it: who, under which profile,
/// and its row if it has one.
pub struct Subscriber<'a> {
    pub client: ClientKey,
    pub profile: NamespaceProfile,
    pub row: Option<&'a ClientRow>,
}

/// The clients `this` instance holds a reference for — what the host hands to
/// [`crate::instance::Instance::reconcile`].
///
/// It is [`fan_out`] and nothing else, on purpose. "Whose events do I write"
/// and "who keeps me alive" must be ONE rule: were they two, an instance
/// could be delivering to a client that is not counted among its references,
/// and be ejected as abandoned underneath it — or hold a reference for a
/// client it never speaks to, and never retire.
pub fn attached_to<'a>(
    this: &InstanceKey,
    module_current: &InstanceKey,
    subscribers: impl IntoIterator<Item = Subscriber<'a>>,
    dir_of: impl Fn(&InstanceKey) -> InstanceDir,
) -> BTreeMap<ClientKey, NamespaceProfile> {
    subscribers
        .into_iter()
        .filter_map(
            |subscriber| match fan_out(subscriber.row, this, module_current, &dir_of) {
                FanOut::Deliver => Some((subscriber.client, subscriber.profile)),
                FanOut::Skip => None,
            },
        )
        .collect()
}

/// When, within one drain tick, a companion's queued rows in `this`
/// instance's store are sent.
///
/// One drain serves a whole MODULE and reads every instance's store — not one
/// drain per instance, which would let two tasks POST to the same companion
/// at once and would split the recapture wipe's "never overlaps an apply"
/// guarantee across tasks that cannot see each other.
///
/// A client is written to by exactly one instance at a time, but rows it was
/// owed by the instance it has LEFT may still be queued there. Those are
/// older than anything its new instance has for it, so they go first — and a
/// companion whose leftovers failed is backed off before its serving pass
/// runs, by the retry state the drain already keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DrainPass {
    /// Rows in the store of an instance that no longer serves this client.
    Leftover,
    Serving,
}

pub fn drain_pass(
    row: Option<&ClientRow>,
    this: &InstanceKey,
    module_current: &InstanceKey,
    dir_of: impl FnOnce(&InstanceKey) -> InstanceDir,
) -> DrainPass {
    match fan_out(row, this, module_current, dir_of) {
        FanOut::Deliver => DrainPass::Serving,
        FanOut::Skip => DrainPass::Leftover,
    }
}

/// Whether `current.toml` should follow a client that has just moved.
///
/// The module's CURRENT instance is what every unplaced client, and every
/// `&str`-addressed operator tool, means by "the module" — and it cannot be
/// reaped while siblings exist, because that would leave the pointer naming
/// nothing. So when the last client leaves it, the pointer has to leave too,
/// or the emptied instance is refused its reap on every tick for ever.
///
/// It follows the client that emptied it: that instance is, by construction,
/// live, caught up and serving somebody. An instance that still has ANY
/// client — including the unplaced ones [`attached_to`] counts on it — keeps
/// the pointer, so a dev client forking away from prod never drags "the
/// module" along with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Succession {
    Stays,
    MovesTo(InstanceKey),
}

/// `still_attached_to_current` is [`attached_to`] for the current instance,
/// evaluated AFTER the move's row was written.
pub fn succession(
    module_current: &InstanceKey,
    still_attached_to_current: &BTreeMap<ClientKey, NamespaceProfile>,
    moved_to: &InstanceKey,
) -> Succession {
    if moved_to == module_current || !still_attached_to_current.is_empty() {
        Succession::Stays
    } else {
        Succession::MovesTo(moved_to.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_move::Placement;
    use crate::ids::{ClientId, ClientKey, ModuleId, Namespace, Revision, Sha256Hex};

    fn key(byte: &str) -> InstanceKey {
        InstanceKey {
            module: ModuleId::parse("ownership-indexer").unwrap(),
            wasm: Sha256Hex::parse(&byte.repeat(32)).unwrap(),
            config: None,
        }
    }

    fn client() -> ClientKey {
        ClientKey {
            namespace: Namespace::parse("prod").unwrap(),
            client: ClientId::parse("ownership.cnft.dev").unwrap(),
        }
    }

    fn unplaced() -> ClientRow {
        ClientRow::new(client(), key("aa").module)
    }

    fn placed_on(byte: &str) -> ClientRow {
        ClientRow::adopt(
            client(),
            key(byte).module,
            Placement {
                instance: key(byte),
                revision: Revision(1),
            },
        )
    }

    fn never_asked(_: &InstanceKey) -> InstanceDir {
        panic!("the directory was checked for a row that did not need it")
    }

    fn named(host: &str) -> ClientKey {
        ClientKey {
            namespace: Namespace::parse("prod").unwrap(),
            client: ClientId::parse(host).unwrap(),
        }
    }

    /// The refcount twin of `exactly_one_live_instance_serves_a_client`: over
    /// a module's live instances the attached sets PARTITION its clients. A
    /// client in two sets keeps a retired instance alive for ever; a client in
    /// none lets the instance serving it be ejected as abandoned.
    #[test]
    fn the_attached_sets_of_a_modules_instances_partition_its_clients() {
        use crate::policy::fixtures::prod;

        let live = [key("aa"), key("bb")];
        let dir_of = |asked: &InstanceKey| {
            if live.contains(asked) {
                InstanceDir::Present
            } else {
                InstanceDir::Gone
            }
        };
        let rows = [
            (named("no-row.cnft.dev"), None),
            (named("unplaced.cnft.dev"), Some(unplaced())),
            (named("on-aa.cnft.dev"), Some(placed_on("aa"))),
            (named("on-bb.cnft.dev"), Some(placed_on("bb"))),
            (named("renamed-away.cnft.dev"), Some(placed_on("cc"))),
        ];
        for module_current in &live {
            let mut seen = Vec::new();
            for this in &live {
                let subscribers = rows.iter().map(|(client, row)| Subscriber {
                    client: client.clone(),
                    profile: prod(),
                    row: row.as_ref(),
                });
                seen.extend(attached_to(this, module_current, subscribers, dir_of).into_keys());
            }
            seen.sort();
            let mut all: Vec<_> = rows.iter().map(|(client, _)| client.clone()).collect();
            all.sort();
            assert_eq!(seen, all, "with current {module_current:?}");
        }
    }

    /// The pointer follows the client that EMPTIED the current instance, and
    /// nobody else. A dev client forking away must not drag "the module" —
    /// what every unplaced client and operator tool means — along with it.
    #[test]
    fn the_pointer_follows_only_the_client_that_empties_the_current_instance() {
        use crate::policy::fixtures::prod;
        let (current, elsewhere) = (key("aa"), key("bb"));
        let nobody = BTreeMap::new();
        let somebody = BTreeMap::from([(named("still-here.cnft.dev"), prod())]);

        assert_eq!(
            succession(&current, &somebody, &elsewhere),
            Succession::Stays
        );
        assert_eq!(
            succession(&current, &nobody, &elsewhere),
            Succession::MovesTo(elsewhere.clone())
        );
        // A move ONTO the current instance leaves the pointer where it is.
        assert_eq!(succession(&current, &nobody, &current), Succession::Stays);
    }

    /// A moved client may still be owed rows by the instance it left. They
    /// are older than anything its new instance holds, so they are sent first.
    #[test]
    fn what_the_old_instance_still_owes_is_sent_before_the_new_one_speaks() {
        let (left, now_on) = (key("aa"), key("bb"));
        let row = placed_on("bb");
        let from_old = drain_pass(Some(&row), &left, &left, |_| InstanceDir::Present);
        let from_new = drain_pass(Some(&row), &now_on, &left, never_asked);
        assert_eq!(from_old, DrainPass::Leftover);
        assert_eq!(from_new, DrainPass::Serving);
        assert!(from_old < from_new, "passes run in their `Ord` order");
    }

    /// Today, in full: one instance, no rows. If this ever fails, a deploy of
    /// the instance-addressed host changes who receives what.
    #[test]
    fn with_one_instance_and_no_rows_everything_is_delivered_as_in_v2() {
        assert_eq!(
            fan_out(None, &key("aa"), &key("aa"), never_asked),
            FanOut::Deliver
        );
        assert_eq!(
            fan_out(Some(&unplaced()), &key("aa"), &key("aa"), never_asked),
            FanOut::Deliver
        );
        assert_eq!(unclaimed(&key("aa"), &key("aa")), Unclaimed::Keep);
    }

    /// A staging instance follows the chain beside the live one. Until a row
    /// names it, it speaks to nobody.
    #[test]
    fn a_second_instance_serves_nobody_until_a_row_names_it() {
        let (live, staging) = (key("aa"), key("bb"));
        for row in [None, Some(unplaced()), Some(placed_on("aa"))] {
            assert_eq!(
                fan_out(row.as_ref(), &staging, &live, |_| InstanceDir::Present),
                FanOut::Skip,
                "{row:?}"
            );
            assert_eq!(
                fan_out(row.as_ref(), &live, &live, never_asked),
                FanOut::Deliver,
                "{row:?}"
            );
        }
        assert_eq!(unclaimed(&staging, &live), Unclaimed::Drop);
    }

    /// THE property, over every arrangement of two live instances and a row:
    /// a client is served by exactly one of them. Two is a duplicate of every
    /// event; none is silence.
    #[test]
    fn exactly_one_live_instance_serves_a_client() {
        let live = [key("aa"), key("bb")];
        let rows = [
            None,
            Some(unplaced()),
            Some(placed_on("aa")),
            Some(placed_on("bb")),
            // Named an instance that is gone — see the next test.
            Some(placed_on("cc")),
        ];
        for module_current in &live {
            for row in &rows {
                let serving: Vec<_> = live
                    .iter()
                    .filter(|this| {
                        fan_out(row.as_ref(), this, module_current, |asked| {
                            if live.contains(asked) {
                                InstanceDir::Present
                            } else {
                                InstanceDir::Gone
                            }
                        }) == FanOut::Deliver
                    })
                    .collect();
                assert_eq!(
                    serving.len(),
                    1,
                    "row {row:?} with current {module_current:?} is served by {serving:?}"
                );
            }
        }
    }

    /// Every operator upload does this: `activate` RENAMES the live instance
    /// directory to the new key. The row still names the old one, which is
    /// now nowhere. That client has not been moved by anything; it must keep
    /// being served, by whatever the module's current instance now is.
    #[test]
    fn a_row_naming_an_instance_an_upload_renamed_away_follows_the_module() {
        let (before, after) = (key("aa"), key("bb"));
        let row = placed_on("aa");
        assert_eq!(
            fan_out(Some(&row), &after, &after, |asked| {
                assert_eq!(asked, &before);
                InstanceDir::Gone
            }),
            FanOut::Deliver
        );
    }

    /// A moved client really has left: its old instance, still live and still
    /// the module's current one, must stop writing rows for it.
    #[test]
    fn a_client_placed_on_another_live_instance_is_not_served_by_the_current_one() {
        let (current, other) = (key("aa"), key("bb"));
        assert_eq!(
            fan_out(Some(&placed_on("bb")), &current, &current, |asked| {
                assert_eq!(asked, &other);
                InstanceDir::Present
            }),
            FanOut::Skip
        );
        assert_eq!(
            fan_out(Some(&placed_on("bb")), &other, &current, never_asked),
            FanOut::Deliver
        );
    }
}
