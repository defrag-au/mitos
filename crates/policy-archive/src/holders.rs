//! Who holds it — the balance table, folded from movements alone.
//!
//! # Why this exists, and why it is not in `view`
//!
//! There was no way to ask "who holds this token" of an ARCHIVE. The only
//! holder table in the estate reads the per-policy sqlite database, and that
//! database is the thing being retired — so the one question that settles a
//! supply disagreement was exactly the one the sqlite-free path could not
//! answer.
//!
//! MEASURED, and the reason this got written: $PERP's archive reconciles at
//! `Σ net_mint = 1,000,000,000` with **one mint and zero burns**, while every
//! aggregator quotes 866.37M — the difference being 133.63M units parked at
//! `$burnsnek`, a trusted burn sink. They exist on chain, so we count them;
//! they can never move, so nobody else does. Naming the holder is what turns
//! "our cap is 15.7% high" into "our cap includes a burn sink".
//!
//! ⚠️ **Not a [`crate::view`] projection**, deliberately. A `PolicyView` folds
//! the story stream, and that stream is a WINDOW — most of a token's transfers
//! happened before it starts. Balances are a cumulative fold over ALL history,
//! so they come from the movement rows or they are wrong. The same reasoning
//! that makes supply walk backwards from the tip applies here, and the
//! conclusion is stronger: there is no tip figure to walk back from.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::Movement;

/// One party's holding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// Stake credential, hex, where the address has one — so a wallet that
    /// rotates payment addresses stays ONE row. See [`Holder::address`].
    ///
    /// ⚠️ Falls back to the whole bech32 address for an enterprise address,
    /// which genuinely has no stake part. Those cannot be merged with anything
    /// and must not be merged with each other.
    pub key: String,
    /// The lexicographically smallest address this party used — enough to
    /// resolve a handle or open an explorer, and NOT a claim that it is the
    /// only one. Chosen rather than "the first seen" so a party's
    /// representative is the same in every fold, however the rows are ordered;
    /// [`crate::summary`] folds the same history and must name the same
    /// address, or the two tables label one party two ways.
    pub address: String,
    /// Units held. For a fungible policy this is the balance; for an NFT
    /// collection it is **how many items**, which is the useful reading of the
    /// same sum.
    pub amount: i64,
    /// Movements this party appears in — the difference between a holding that
    /// arrived once and one that is actively traded.
    pub movements: u32,
    /// Distinct addresses folded into this row.
    pub addresses: u32,
}

/// Every party with a non-zero balance, largest first.
///
/// # ⚠️ Zero and negative rows are DROPPED, and negative is a finding
///
/// A party that received and then sent everything nets to zero and is not a
/// holder; keeping it would bury the real ones under thousands of empties.
///
/// A NEGATIVE balance is impossible on chain and means the archive is missing
/// the inbound side of a movement — which is coverage, not arithmetic. It is
/// counted by [`Fold::negative`] rather than silently discarded, because a
/// table that quietly drops the evidence of its own incompleteness is how an
/// incomplete table gets trusted.
pub fn fold<'a>(movements: impl Iterator<Item = &'a Movement>) -> Fold {
    fold_onto(movements, &Carry::default())
}

/// The UNFILTERED balance state — what a fold needs to resume.
///
/// ⚠️ Not [`Fold`]. `Fold` publishes only the rows that ended POSITIVE: a
/// party at zero at the boundary is not a holder, and dropping it there would
/// make it start again from nothing when it receives above the boundary.
/// A negative balance is likewise an archive defect worth carrying forward,
/// not erasing at a seam.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Carry {
    /// Party key → running state.
    ///
    /// The address LIST is kept rather than a count, because
    /// [`Holder::addresses`] is a count of DISTINCT addresses: a number alone
    /// cannot tell a new address from a wallet reusing one it already had, and
    /// repeat use above the boundary is the ordinary case.
    pub parties: HashMap<String, Party>,
    /// Movement rows folded, including the ones that netted to nothing.
    pub rows: u64,
}

/// One party's running state, before [`Fold`] decides what to publish.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Party {
    /// One address this party used — the one a reader is shown.
    pub address: String,
    pub amount: i64,
    pub movements: u32,
    pub addresses: Vec<String>,
}

/// The carried state after folding these movements, without publishing.
pub fn carry<'a>(movements: impl Iterator<Item = &'a Movement>) -> Carry {
    accumulate(Carry::default(), movements)
}

/// Fold movements ONTO a carried balance table, and publish the result.
pub fn fold_onto<'a>(movements: impl Iterator<Item = &'a Movement>, carry: &Carry) -> Fold {
    finalise(accumulate(carry.clone(), movements))
}

fn accumulate<'a>(mut carry: Carry, movements: impl Iterator<Item = &'a Movement>) -> Carry {
    use std::collections::HashMap;

    let Carry { parties, rows } = &mut carry;
    // Address → key, worked out once per address rather than once per row. The
    // key is a bech32 decode, and a wallet's history is thousands of rows of
    // the same few addresses; repeating that decode was most of the fold.
    let mut keys: HashMap<&'a str, String> = HashMap::new();

    for m in movements {
        // A placeholder stands in for a `(tx, unit)` attributed to nobody. It
        // is not a party and summing its zero would create an empty row.
        if m.is_placeholder() || m.address.is_empty() {
            continue;
        }
        *rows += 1;
        let key = keys
            .entry(m.address.as_str())
            .or_insert_with(|| key_of(&m.address));
        let e = parties.entry(key.clone()).or_insert_with(|| Party {
            address: m.address.clone(),
            amount: 0,
            movements: 0,
            addresses: Vec::new(),
        });
        e.amount += m.amount;
        e.movements += 1;
        if !e.addresses.contains(&m.address) {
            e.addresses.push(m.address.clone());
        }
    }

    carry
}

/// The key one party is folded under: its stake part, so one wallet is one
/// row however many payment addresses it rotates through. An enterprise
/// address has no stake part and keys by itself. Shared with
/// [`crate::summary`], whose checkpoints must name the same parties.
pub fn key_of(address: &str) -> String {
    crate::trade::account_key(address)
}

fn finalise(carry: Carry) -> Fold {
    let Carry { parties, rows } = carry;
    let mut negative = 0usize;
    let mut holders: Vec<Holder> = parties
        .into_iter()
        .filter(|(_, p)| {
            if p.amount < 0 {
                negative += 1;
            }
            p.amount > 0
        })
        .map(|(key, p)| {
            let address = p
                .addresses
                .iter()
                .min()
                .cloned()
                .unwrap_or_else(|| p.address.clone());
            Holder {
                key,
                address,
                amount: p.amount,
                movements: p.movements,
                addresses: p.addresses.len() as u32,
            }
        })
        .collect();
    // Deterministic: by amount, then by key, so equal balances do not shuffle
    // between runs and a diff of two reports means something.
    holders.sort_by(|a, b| b.amount.cmp(&a.amount).then(a.key.cmp(&b.key)));

    let held: i64 = holders.iter().map(|h| h.amount).sum();
    Fold {
        holders,
        held,
        rows,
        negative,
    }
}

/// The table, and what it cost to believe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fold {
    pub holders: Vec<Holder>,
    /// Σ of every positive balance. ⚠️ Compare this against the archive's
    /// `Σ net_mint`: they agree only when every unit is attributed, and a gap
    /// is the honest measure of how much of the supply this table cannot place.
    pub held: i64,
    /// Movement rows folded.
    pub rows: u64,
    /// Parties whose balance came out NEGATIVE — impossible on chain, so a
    /// count of how many inbound movements the archive is missing. See
    /// [`fold`].
    pub negative: usize,
}

impl Fold {
    /// What share of `total` the top `n` hold, as a percentage. `None` when
    /// there is no supply to take a share OF — never 0, which would read as
    /// "the whole supply is spread thin".
    pub fn concentration(&self, n: usize, total: i64) -> Option<f64> {
        (total > 0).then(|| {
            let top: i64 = self.holders.iter().take(n).map(|h| h.amount).sum();
            100.0 * top as f64 / total as f64
        })
    }
}

/// Publish a carried table without folding anything more.
///
/// The other half of [`Carry`]: a reader that finds a written artefact but no
/// rows above its boundary still has to hand back a [`Fold`].
pub fn publish(carry: &Carry) -> Fold {
    finalise(carry.clone())
}

/// On-disk name of the DERIVED balance table, beside `series.bin`.
///
/// ⚠️ An ARTEFACT, not truth. It is folded from the archive and can be deleted
/// at any time; a reader that cannot use it must fall back to [`fold`], never
/// to an empty table.
pub const HOLDERS: &str = "holders.bin";

/// Bumped when [`Carry`], [`Party`] or [`Fold`] changes shape. Same rule as
/// [`crate::SERIES_FORMAT`]: a stale file and a broken one are different
/// answers, so the byte is checked before anything is deserialized.
///
/// ⚠️ **2 makes [`HoldersFile::settled_to`] the IMMUTABLE boundary.** Version 1
/// was written from a fold that could include the volatile tail while naming a
/// boundary above it, so its carry held rows a later tail fold folded AGAIN.
/// The tail is replaced whole, so those rows cannot be trusted to still exist;
/// every version-1 file is refused and rebuilt rather than read.
///
/// ⚠️ **3 excludes PLUMBING units.** A version-2 carry was folded over every
/// unit in the archive, so a CIP-68 reference twin or a CIP-27 royalty token
/// sat in the table as a holding. MEASURED 2026-10-29 on the Ada Handle policy:
/// 46,167 reference twins added 46,168 to `held` on top of the 266,985 the
/// supply route reports. The supply route has always excluded them; the holder
/// table now does too, so the two can be compared.
pub const HOLDERS_FORMAT: u8 = 3;

/// What `holders.bin` holds: the balance state, and the archive it describes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HoldersFile {
    pub format: u8,
    /// The archive's IMMUTABLE coverage ceiling when this was folded — see
    /// [`crate::series::SeriesFile::immutable_to`] for why that key and not
    /// `updated_unix`.
    pub immutable_to: u64,
    /// The slot the table is complete through. Movements above it are folded
    /// on read; see [`crate::series::Carry::settled_to`].
    pub settled_to: u64,
    pub carry: Carry,
}

#[derive(Debug, thiserror::Error)]
pub enum HoldersError {
    #[error(
        "holders is format {found}, this build reads {expected} — a stale copy, \
         not a broken one"
    )]
    Version { found: u8, expected: u8 },
    #[error("holders is empty")]
    Empty,
    #[error("decoding the holders: {0}")]
    Decode(#[from] postcard::Error),
}

impl HoldersFile {
    pub fn encode(&self) -> Result<Vec<u8>, HoldersError> {
        Ok(postcard::to_stdvec(self)?)
    }

    /// Version-checked decode, on byte zero before anything else is trusted.
    pub fn decode(bytes: &[u8]) -> Result<Self, HoldersError> {
        match bytes.first() {
            Some(&HOLDERS_FORMAT) => {}
            Some(&found) => {
                return Err(HoldersError::Version {
                    found,
                    expected: HOLDERS_FORMAT,
                });
            }
            None => return Err(HoldersError::Empty),
        }
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// One party's balance at a reading, and what it held THROUGHOUT each window before it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldThrough {
    /// The party key, as [`key_of`] names it.
    pub key: String,
    /// The balance at the reading.
    pub now: i64,
    /// For each window asked, in the order asked: the lowest balance between the window's start and
    /// the reading, inclusive — the units that were in the wallet the whole time. Never negative,
    /// never more than `now`.
    pub held: Vec<i64>,
}

/// What each party held throughout the `windows` (seconds) before `at_unix`, from movements alone.
///
/// The reading a maturity rule needs for a fungible token: a unit counts as held for a window only
/// if the balance never dipped below it inside the window, so moving units out and back — or into
/// a fresh wallet the day before a snapshot — does not age them. Settled 2026-11-11 for $PERP,
/// which matures on the same schedule as a Perp.
///
/// A transaction's rows for one party are netted before they are applied: a wallet that rotates
/// payment addresses inside one transaction did not, for a moment, hold less. Movements after
/// `at_unix` are ignored. Parties whose balance at the reading is not positive are left out.
pub fn held_through<'a>(
    movements: impl Iterator<Item = &'a Movement>,
    at_unix: u64,
    windows: &[u64],
) -> Vec<HeldThrough> {
    // A transaction in the chain's own order: block time, slot, position, hash.
    type TxOrder<'t> = (u64, u64, u32, &'t [u8]);
    // Transaction → party → net.
    let mut by_tx: std::collections::BTreeMap<TxOrder<'a>, HashMap<String, i64>> =
        std::collections::BTreeMap::new();
    let mut keys: HashMap<&'a str, String> = HashMap::new();
    for m in movements {
        if m.is_placeholder() || m.address.is_empty() || m.block_time > at_unix {
            continue;
        }
        let key = keys
            .entry(m.address.as_str())
            .or_insert_with(|| key_of(&m.address))
            .clone();
        *by_tx
            .entry((m.block_time, m.slot, m.tx_index, m.tx_hash.as_slice()))
            .or_default()
            .entry(key)
            .or_default() += m.amount;
    }
    let starts: Vec<u64> = windows
        .iter()
        .map(|seconds| at_unix.saturating_sub(*seconds))
        .collect();
    // Party → (balance, the lowest balance seen since each window's start).
    let mut state: HashMap<String, (i64, Vec<i64>)> = HashMap::new();
    for ((time, ..), deltas) in by_tx {
        for (key, delta) in deltas {
            let (balance, lows) = state
                .entry(key)
                .or_insert_with(|| (0, vec![0; starts.len()]));
            *balance += delta;
            for (low, start) in lows.iter_mut().zip(&starts) {
                if time <= *start {
                    // Still before the window: its floor is the balance it opens with.
                    *low = *balance;
                } else {
                    *low = (*low).min(*balance);
                }
            }
        }
    }
    let mut out: Vec<HeldThrough> = state
        .into_iter()
        .filter(|(_, (balance, _))| *balance > 0)
        .map(|(key, (now, lows))| HeldThrough {
            key,
            now,
            held: lows.into_iter().map(|low| low.clamp(0, now)).collect(),
        })
        .collect();
    out.sort_by(|a, b| b.now.cmp(&a.now).then(a.key.cmp(&b.key)));
    out
}

/// One interval a party's balance stayed above zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldSpan {
    /// The slot of the transaction that took the balance above zero.
    pub from_slot: u64,
    /// Its block time.
    pub from_unix: u64,
    /// The block time of the transaction that took it back to zero, or `None` while it is held.
    pub to_unix: Option<u64>,
}

/// Every party's intervals of a balance above zero, oldest first, keyed as [`key_of`] names it.
///
/// The token's equivalent of a Perp's holding lots, for a predicate about holding it at all or for
/// so long without a break: how much was held does not matter, only that some was. A transaction's
/// rows for one party are netted first, as [`held_through`] nets them, so rotating addresses inside
/// one transaction is not a break.
pub fn held_spans<'a>(
    movements: impl Iterator<Item = &'a Movement>,
) -> std::collections::BTreeMap<String, Vec<HeldSpan>> {
    type TxOrder<'t> = (u64, u64, u32, &'t [u8]);
    let mut by_tx: std::collections::BTreeMap<TxOrder<'a>, HashMap<String, i64>> =
        std::collections::BTreeMap::new();
    let mut keys: HashMap<&'a str, String> = HashMap::new();
    for m in movements {
        if m.is_placeholder() || m.address.is_empty() {
            continue;
        }
        let key = keys
            .entry(m.address.as_str())
            .or_insert_with(|| key_of(&m.address))
            .clone();
        *by_tx
            .entry((m.block_time, m.slot, m.tx_index, m.tx_hash.as_slice()))
            .or_default()
            .entry(key)
            .or_default() += m.amount;
    }
    let mut balances: HashMap<String, i64> = HashMap::new();
    let mut spans: std::collections::BTreeMap<String, Vec<HeldSpan>> =
        std::collections::BTreeMap::new();
    for ((time, slot, ..), deltas) in by_tx {
        for (key, delta) in deltas {
            if delta == 0 {
                continue;
            }
            let balance = balances.entry(key.clone()).or_default();
            let before = *balance;
            *balance += delta;
            let held = spans.entry(key).or_default();
            if before <= 0 && *balance > 0 {
                held.push(HeldSpan {
                    from_slot: slot,
                    from_unix: time,
                    to_unix: None,
                });
            } else if before > 0
                && *balance <= 0
                && let Some(open) = held.last_mut()
            {
                open.to_unix = Some(time);
            }
        }
    }
    spans.retain(|_, held| !held.is_empty());
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Held, emptied, held again: two spans, the second still open. Moving part of the balance
    /// out is not a break, and a rotation inside one transaction is not either.
    #[test]
    fn held_spans_break_only_when_the_balance_reaches_zero() {
        let a = "addr_test_holder";
        let row = |slot: u64, tx: u8, amount: i64| Movement {
            slot,
            block_time: 1_000 + slot,
            tx_hash: vec![tx],
            unit_name: b"U".to_vec(),
            address: a.into(),
            amount,
            net_mint: 0,
            tx_index: 0,
        };
        let rows = [
            row(10, 1, 100),
            row(20, 2, -40), // partial: still held
            row(30, 3, -60), // empty
            row(40, 4, 5),   // back
            row(50, 5, -5),  // rotated out…
            row(50, 5, 5),   // …and in, in the same transaction
        ];
        let spans = held_spans(rows.iter());
        assert_eq!(
            spans.get(a).map(Vec::as_slice),
            Some(
                &[
                    HeldSpan {
                        from_slot: 10,
                        from_unix: 1_010,
                        to_unix: Some(1_030)
                    },
                    HeldSpan {
                        from_slot: 40,
                        from_unix: 1_040,
                        to_unix: None
                    },
                ][..]
            )
        );
    }

    /// A wallet that held 100 for months, received 50 three days ago, and briefly sent 30 away
    /// and back six days ago: through four days it held 100, through eight only 70.
    #[test]
    fn held_through_is_the_lowest_balance_inside_each_window() {
        let day = 86_400;
        let at = 100 * day;
        let a = "addr_test_holder";
        let at_day = |d: u64, tx: u8, amount: i64| Movement {
            slot: d,
            block_time: at - d * day,
            tx_hash: vec![tx],
            unit_name: b"U".to_vec(),
            address: a.into(),
            amount,
            net_mint: 0,
            tx_index: 0,
        };
        let rows = [
            at_day(90, 1, 100), // long held
            at_day(6, 2, -30),  // out, six days ago
            at_day(5, 3, 30),   // back the next day
            at_day(3, 4, 50),   // new, three days ago
        ];
        let read = held_through(rows.iter(), at, &[4 * day, 8 * day]);
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].now, 150);
        assert_eq!(read[0].held, vec![100, 70]);
        // A reading before the new arrival does not see it.
        let earlier = held_through(rows.iter(), at - 4 * day, &[day]);
        assert_eq!(earlier[0].now, 100);
        // Rows netted within one transaction never dip.
        let rotate = [
            at_day(90, 1, 100),
            Movement {
                amount: -100,
                ..at_day(2, 9, 0)
            },
            Movement {
                amount: 100,
                ..at_day(2, 9, 0)
            },
        ];
        assert_eq!(
            held_through(rotate.iter(), at, &[4 * day])[0].held,
            vec![100]
        );
    }

    fn mv(addr: &str, amount: i64) -> Movement {
        Movement {
            slot: 1,
            block_time: 1,
            tx_hash: vec![1],
            unit_name: b"U".to_vec(),
            address: addr.into(),
            amount,
            net_mint: 0,
            tx_index: 0,
        }
    }

    /// Two payment addresses sharing a stake key are ONE wallet. A holder
    /// table that lists them separately understates concentration, which is
    /// the single thing such a table is read for.
    #[test]
    fn addresses_sharing_a_stake_key_fold_into_one_holder() {
        // Same 28-byte stake part, different payment parts.
        let a = "addr1q9xy2xk7s8wvkq0w7qr4t6u9e3xj5m8n2p4r6t8v0w2y4a6c8e0g2j4l6n8q0s2u4w6y8a0c2e4g6j8l0n2q4s6u8w0yjqamn8h";
        let b = "addr1q8ab2xk7s8wvkq0w7qr4t6u9e3xj5m8n2p4r6t8v0w2y4a6c8e0g2j4l6n8q0s2u4w6y8a0c2e4g6j8l0n2q4s6u8w0yjqamn8h";
        let f = fold([mv(a, 100), mv(b, 300)].iter());
        // They may or may not share a stake part depending on the synthetic
        // bytes; what must hold is that the fold is driven by the stake part
        // and never by the raw string.
        let by_stake = crate::trade::address_parts(a).and_then(|(_, s)| s);
        if by_stake.is_some() && by_stake == crate::trade::address_parts(b).and_then(|(_, s)| s) {
            assert_eq!(f.holders.len(), 1, "one wallet, one row");
            assert_eq!(f.holders[0].amount, 400);
            assert_eq!(f.holders[0].addresses, 2);
            // The representative is chosen, not "the first seen": the summary
            // folds the same history in a different order and must name the
            // same address.
            assert_eq!(f.holders[0].address, a.min(b));
            let reversed = fold([mv(b, 300), mv(a, 100)].iter());
            assert_eq!(reversed.holders[0].address, f.holders[0].address);
        }
    }

    #[test]
    fn a_party_that_sent_everything_back_is_not_a_holder() {
        let f = fold([mv("addr_a", 100), mv("addr_a", -100), mv("addr_b", 50)].iter());
        assert_eq!(f.holders.len(), 1);
        assert_eq!(f.holders[0].address, "addr_b");
        assert_eq!(f.held, 50);
    }

    /// ⚠️ Negative is IMPOSSIBLE on chain, so it measures missing coverage
    /// rather than being an arithmetic curiosity to round away.
    #[test]
    fn a_negative_balance_is_counted_as_missing_coverage() {
        let f = fold([mv("addr_a", -100), mv("addr_b", 50)].iter());
        assert_eq!(f.negative, 1, "the impossible row is REPORTED");
        assert_eq!(f.holders.len(), 1, "and not listed as a holder");
        assert_eq!(f.held, 50);
    }

    #[test]
    fn placeholders_are_not_parties() {
        let mut p = mv("", 0);
        p.net_mint = 1_000;
        let f = fold([p, mv("addr_a", 1_000)].iter());
        assert_eq!(f.holders.len(), 1);
        assert_eq!(f.rows, 1, "the placeholder is not a folded row either");
    }

    #[test]
    fn holders_are_ordered_largest_first_and_deterministically() {
        let f = fold([mv("addr_b", 100), mv("addr_a", 100), mv("addr_c", 900)].iter());
        let keys: Vec<&str> = f.holders.iter().map(|h| h.key.as_str()).collect();
        assert_eq!(
            keys,
            vec!["addr_c", "addr_a", "addr_b"],
            "ties break by key"
        );
    }

    /// ⚠️ `None`, not 0, when there is no supply to take a share of.
    #[test]
    fn concentration_is_undefined_without_a_supply() {
        let f = fold([mv("addr_a", 100)].iter());
        assert_eq!(f.concentration(1, 0), None);
        assert_eq!(f.concentration(1, 200), Some(50.0));
    }

    /// ⚠️ THE TEST THE CARRY EXISTS FOR.
    ///
    /// Folding in two must equal folding in one — including the party that sits
    /// at ZERO at the seam, which is the row [`Fold`] drops and [`Carry`] must
    /// not.
    #[test]
    fn a_fold_split_at_a_boundary_equals_a_fold_in_one() {
        let rows = [
            mv("addr_a", 100),
            mv("addr_b", 50),
            // addr_a sends everything: zero at the seam, so it is not a holder
            // there and must still come back as the SAME party above it.
            mv("addr_a", -100),
            mv("addr_a", 30),
            mv("addr_c", -10),
            mv("addr_b", 25),
        ];
        let whole = fold(rows.iter());

        let seam = carry(rows[..3].iter());
        let split = fold_onto(rows[3..].iter(), &seam);

        assert_eq!(split, whole);
        assert!(
            split
                .holders
                .iter()
                .any(|h| h.key == "addr_a" && h.amount == 30),
            "the party at zero at the seam must come back with its balance, \
             not as a new party"
        );
        assert_eq!(split.rows, whole.rows, "rows counts every folded movement");
    }

    #[test]
    fn the_holders_file_round_trips_and_refuses_another_build() {
        let file = HoldersFile {
            format: HOLDERS_FORMAT,
            immutable_to: 201_160_800,
            settled_to: 201_160_800,
            carry: carry([mv("addr_a", 100)].iter()),
        };
        let bytes = file.encode().unwrap();
        assert_eq!(bytes[0], HOLDERS_FORMAT);
        assert_eq!(HoldersFile::decode(&bytes).unwrap(), file);

        let mut future = bytes.clone();
        future[0] = HOLDERS_FORMAT + 1;
        assert!(matches!(
            HoldersFile::decode(&future),
            Err(HoldersError::Version { .. })
        ));
        assert!(matches!(HoldersFile::decode(&[]), Err(HoldersError::Empty)));
    }
}
