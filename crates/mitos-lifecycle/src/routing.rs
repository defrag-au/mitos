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

use crate::client_move::ClientRow;
use crate::ids::InstanceKey;

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
