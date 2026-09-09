//! The archive's self-check: does the supply it describes add up?
//!
//! ```text
//! Σ party amounts  ==  Σ per-tx net_mint          (per unit)
//! ```
//!
//! MEASURED on $PERP: both **1,000,000,000**, matching Koios `total_supply`.
//!
//! This is worth more than an assertion because **the archive carries its own
//! known total.** A reconciliation normally needs a second source to compare
//! against, and a check that needs an external call is a check that gets
//! skipped. Here both sides come out of the same file, so it costs one scan
//! and can run on every landing.
//!
//! ## Why the two sides are the same number
//!
//! Conservation, and the reverse walk is built on it. A transfer contributes
//! `−x` and `+x`, netting nothing; a mint contributes only `+x` because it has
//! no source; a burn only `−x`. So the amounts sum to exactly what was minted
//! net of what was burned — which is `Σ net_mint`, and at completeness is the
//! circulating supply.
//!
//! ## The gap is a COVERAGE number, not an error
//!
//! ⚠️ Grades rather than passing or failing, because a partial archive is the
//! normal state, not a broken one. The walk registers interest in a source
//! only where one is *missing* (`sum > minted` — see `reverse.rs` INPUTS), and
//! resolves it as the descent reaches the producing transaction. So on a
//! partial archive:
//!
//! > **`moved − minted` is exactly the supply that arrived from below the
//! > floor** — the amount whose source the walk has not yet descended to.
//!
//! Same quantity [`crate::feed::Direction::SourceBelowFloor`] names per
//! transaction, totalled. It shrinks to zero as the walk deepens, and rendering
//! it is more honest than rendering a balance that silently isn't one.
//!
//! ⚠️ A NEGATIVE gap is a real failure at any completeness. Supply cannot move
//! without having been minted, and no coverage story produces one: an
//! unresolved burn leaves `moved` short by the burn, which is a gap of `+x`,
//! not `−x`. So the sign, not the magnitude, is what separates "still walking"
//! from "the walk is wrong".
//!
//! ## Read rows, not folded rows
//!
//! [`Reconciler`] takes [`Movement`]s straight off the parquet so a whole
//! archive reconciles without holding it in memory. It mirrors
//! [`crate::feed::fold_rows`] exactly where it matters and the difference is
//! not cosmetic:
//!
//! - **amounts are SUMMED** — a later pass appends correction rows and the
//!   sum-merge is how they apply;
//! - **`net_mint` is counted ONCE per `(transaction, unit)`** — every row of a
//!   mint carries the same figure, so adding them would multiply a mint by its
//!   output count.
//!
//! Getting that backwards on either side yields a number that looks like a
//! reconciliation and is not one.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::schema::{Completeness, Movement};

/// How much the reconciler remembers — a memory bound, chosen by the caller.
///
/// [`Diagnose::Totals`] is O(units); [`Diagnose::PerTransaction`] is
/// O(transactions), which is the difference between a few kilobytes and a few
/// hundred megabytes on a policy with millions of them. So the naming of
/// offenders is opt-in rather than always-on, and a landing check on a large
/// policy can still afford to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Diagnose {
    /// Both totals, and nothing else.
    Totals,
    /// Also remember which `(transaction, unit)` failed to conserve — the
    /// difference between "this archive is wrong" and "here is where".
    PerTransaction,
}

impl Diagnose {
    pub const ALL: [Diagnose; 2] = [Diagnose::Totals, Diagnose::PerTransaction];

    pub fn as_wire(self) -> &'static str {
        match self {
            Diagnose::Totals => "totals",
            Diagnose::PerTransaction => "per-transaction",
        }
    }
}

/// One transaction that did not conserve one unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offender {
    pub tx_hash: Vec<u8>,
    pub name: Vec<u8>,
    pub net_mint: i64,
    /// Σ party amounts within this transaction.
    pub moved: i64,
}

impl Offender {
    /// Positive: a source this transaction spent was never resolved. Negative:
    /// it received supply from nowhere.
    pub fn gap(&self) -> i64 {
        self.moved - self.net_mint
    }
}

/// One unit's two totals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Balance {
    /// On-chain asset-name bytes.
    pub name: Vec<u8>,
    /// `Σ net_mint`, counted once per `(transaction, unit)`.
    pub minted: i64,
    /// `Σ amount` over every party row.
    pub moved: i64,
}

impl Balance {
    /// `moved − minted`. Zero when the archive balances; positive by exactly
    /// the supply whose source is below the floor; negative only when
    /// something is wrong.
    pub fn gap(&self) -> i64 {
        self.moved - self.minted
    }
}

/// What the totals say, graded by how far the walk reached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every unit balances. On a complete archive this is the supply invariant
    /// holding; on a partial one it additionally means nothing is outstanding.
    Balanced,
    /// A partial archive with supply still sourced below its floor. Not an
    /// error — the number shrinks to zero as the walk deepens.
    BelowFloor {
        /// Σ positive gaps across units.
        gap: i64,
        /// How many units contribute.
        units: usize,
    },
    /// The archive does not describe a consistent supply. Either a complete
    /// archive failed to balance, or supply was minted that reached nobody,
    /// which no coverage story explains.
    Failed {
        /// The units that did not balance, worst first.
        offenders: Vec<Balance>,
        /// Why this is a failure rather than a coverage number.
        because: Because,
    },
}

/// Why a [`Verdict::Failed`] is a failure. Named rather than a message,
/// because the two have different fixes: one deepens a walk, the other means
/// the walk is wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Because {
    /// The archive claims to reach the first mint, so there is no "below the
    /// floor" left to explain a gap.
    CompleteButUnbalanced,
    /// `moved` is SHORT of `minted` — supply was created that no party row
    /// ever received.
    ///
    /// ⚠️ Coverage cannot produce this, which is what makes the sign worth
    /// separating. A mint's recipients are outputs of the minting transaction
    /// itself, so they are readable the moment the walk sees it; only the
    /// SOURCE side is ever missing, and a missing source leaves `moved` LONG.
    /// An unresolved burn is the clearest case: `net_mint = −x` with no party
    /// row yet, which is a gap of `+x`.
    MintedButUnattributed,
}

impl Because {
    pub const ALL: [Because; 2] = [
        Because::CompleteButUnbalanced,
        Because::MintedButUnattributed,
    ];

    pub fn as_wire(self) -> &'static str {
        match self {
            Because::CompleteButUnbalanced => "complete-but-unbalanced",
            Because::MintedButUnattributed => "minted-but-unattributed",
        }
    }
}

/// Accumulates both sides of the invariant over a stream of rows.
#[derive(Debug)]
pub struct Reconciler {
    diagnose: Diagnose,
    moved: BTreeMap<Vec<u8>, i64>,
    minted: BTreeMap<Vec<u8>, i64>,
    /// `(tx_hash, unit)` whose `net_mint` has been counted. Only mint and burn
    /// transactions land here, so this stays small even on an archive whose
    /// movement rows do not.
    counted: HashSet<(Vec<u8>, Vec<u8>)>,
    /// `(tx_hash, unit) → (moved, net_mint)`. Populated only under
    /// [`Diagnose::PerTransaction`].
    per_tx: HashMap<(Vec<u8>, Vec<u8>), (i64, i64)>,
}

impl Reconciler {
    pub fn new(diagnose: Diagnose) -> Self {
        Self {
            diagnose,
            moved: BTreeMap::new(),
            minted: BTreeMap::new(),
            counted: HashSet::new(),
            per_tx: HashMap::new(),
        }
    }

    /// Fold one row in. Placeholder rows carry `amount = 0` and are still
    /// admitted: a mint attributed to nobody is exactly the case where
    /// `net_mint` is the only evidence there was one.
    pub fn observe(&mut self, m: &Movement) {
        if m.amount != 0 {
            *self.moved.entry(m.unit_name.clone()).or_default() += m.amount;
        }
        let first = m.net_mint != 0
            && self
                .counted
                .insert((m.tx_hash.clone(), m.unit_name.clone()));
        if first {
            *self.minted.entry(m.unit_name.clone()).or_default() += m.net_mint;
        }
        match self.diagnose {
            Diagnose::Totals => {}
            Diagnose::PerTransaction => {
                let slot = self
                    .per_tx
                    .entry((m.tx_hash.clone(), m.unit_name.clone()))
                    .or_insert((0, 0));
                slot.0 += m.amount;
                if m.net_mint != 0 {
                    // Assigned, not added — every row of a mint repeats it.
                    slot.1 = m.net_mint;
                }
            }
        }
    }

    /// Transactions that did not conserve, worst first. Empty under
    /// [`Diagnose::Totals`], which is why the mode is the caller's choice and
    /// not an inference from whether anything failed.
    pub fn offenders(&self) -> Vec<Offender> {
        let mut out: Vec<Offender> = self
            .per_tx
            .iter()
            .filter(|(_, (moved, net_mint))| moved != net_mint)
            .map(|((tx_hash, name), (moved, net_mint))| Offender {
                tx_hash: tx_hash.clone(),
                name: name.clone(),
                net_mint: *net_mint,
                moved: *moved,
            })
            .collect();
        out.sort_by_key(|o| (-o.gap().abs(), o.tx_hash.clone()));
        out
    }

    pub fn observe_all<'a>(&mut self, rows: impl IntoIterator<Item = &'a Movement>) {
        for m in rows {
            self.observe(m);
        }
    }

    /// Every unit either side saw, name-ordered.
    pub fn balances(&self) -> Vec<Balance> {
        let mut names: Vec<&Vec<u8>> = self.moved.keys().chain(self.minted.keys()).collect();
        names.sort_unstable();
        names.dedup();
        names
            .into_iter()
            .map(|name| Balance {
                name: name.clone(),
                minted: self.minted.get(name).copied().unwrap_or(0),
                moved: self.moved.get(name).copied().unwrap_or(0),
            })
            .collect()
    }

    /// The graded answer.
    ///
    /// `completeness` is the archive's own, and it is what turns an
    /// outstanding gap from a coverage number into a failure — so passing a
    /// hopeful `Complete` here defeats the whole check.
    pub fn verdict(&self, completeness: Completeness) -> Verdict {
        let balances = self.balances();
        let negative: Vec<Balance> = balances.iter().filter(|b| b.gap() < 0).cloned().collect();
        if !negative.is_empty() {
            return Verdict::Failed {
                offenders: worst_first(negative),
                because: Because::MintedButUnattributed,
            };
        }
        let outstanding: Vec<Balance> = balances.into_iter().filter(|b| b.gap() > 0).collect();
        if outstanding.is_empty() {
            return Verdict::Balanced;
        }
        let gap: i64 = outstanding.iter().map(Balance::gap).sum();
        let units = outstanding.len();
        match completeness {
            Completeness::Complete => Verdict::Failed {
                offenders: worst_first(outstanding),
                because: Because::CompleteButUnbalanced,
            },
            // An UNRECORDED archive has not claimed to reach the mint, so it
            // gets the same benefit of the doubt as a partial one. Calling it
            // a failure would fail every archive written before the stamp
            // carried the field.
            Completeness::Partial | Completeness::Unrecorded => Verdict::BelowFloor { gap, units },
        }
    }
}

fn worst_first(mut bs: Vec<Balance>) -> Vec<Balance> {
    bs.sort_by_key(|b| -b.gap().abs());
    bs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(tx: u8, unit: &str, address: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot: 1,
            block_time: 1,
            tx_hash: vec![tx],
            unit_name: unit.as_bytes().to_vec(),
            address: address.to_string(),
            amount,
            net_mint,
        }
    }

    fn reconcile(rows: &[Movement]) -> Reconciler {
        let mut r = Reconciler::new(Diagnose::PerTransaction);
        r.observe_all(rows);
        r
    }

    #[test]
    fn a_mint_then_a_transfer_balances() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            row(2, "A", "alice", -30, 0),
            row(2, "A", "bob", 30, 0),
        ]);
        assert_eq!(r.verdict(Completeness::Complete), Verdict::Balanced);
        assert_eq!(
            r.balances(),
            vec![Balance {
                name: b"A".to_vec(),
                minted: 100,
                moved: 100,
            }]
        );
    }

    /// The whole reason `net_mint` is assigned rather than added. Every row of
    /// a mint carries the transaction's figure, so summing them multiplies the
    /// mint by its output count — here 300 instead of 100.
    #[test]
    fn a_mint_split_across_outputs_is_counted_once() {
        let r = reconcile(&[
            row(1, "A", "alice", 50, 100),
            row(1, "A", "bob", 30, 100),
            row(1, "A", "carol", 20, 100),
        ]);
        assert_eq!(r.balances()[0].minted, 100);
        assert_eq!(r.verdict(Completeness::Complete), Verdict::Balanced);
    }

    /// And the mirror: amounts ARE summed, because a later pass appends
    /// correction rows and the sum-merge is how they apply.
    #[test]
    fn correction_rows_for_the_same_party_are_summed() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            row(2, "A", "alice", -40, 0),
            // A later pass resolving the other side of that spend.
            row(2, "A", "bob", 40, 0),
        ]);
        assert_eq!(r.verdict(Completeness::Complete), Verdict::Balanced);
    }

    #[test]
    fn a_burn_balances() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            row(2, "A", "alice", -100, -100),
        ]);
        assert_eq!(r.balances()[0].moved, 0);
        assert_eq!(r.verdict(Completeness::Complete), Verdict::Balanced);
    }

    /// A partial archive that has met a holder but not the mint that supplied
    /// them. The gap is the coverage number, not a fault.
    #[test]
    fn supply_from_below_the_floor_is_coverage_on_a_partial_archive() {
        let r = reconcile(&[row(9, "A", "bob", 70, 0)]);
        assert_eq!(
            r.verdict(Completeness::Partial),
            Verdict::BelowFloor { gap: 70, units: 1 }
        );
    }

    /// ⚠️ The same rows on an archive claiming to reach the first mint. There
    /// is no "below the floor" left, so the identical number is now a failure.
    #[test]
    fn the_same_gap_is_a_failure_once_the_archive_claims_completeness() {
        let r = reconcile(&[row(9, "A", "bob", 70, 0)]);
        let Verdict::Failed { because, offenders } = r.verdict(Completeness::Complete) else {
            panic!("a complete archive with an outstanding gap must fail");
        };
        assert_eq!(because, Because::CompleteButUnbalanced);
        assert_eq!(offenders[0].gap(), 70);
    }

    /// An unrecorded archive never claimed to reach the mint, so it is graded
    /// like a partial one rather than failed for a field it predates.
    #[test]
    fn an_unrecorded_archive_is_graded_like_a_partial_one() {
        let r = reconcile(&[row(9, "A", "bob", 70, 0)]);
        assert_eq!(
            r.verdict(Completeness::Unrecorded),
            Verdict::BelowFloor { gap: 70, units: 1 }
        );
    }

    /// ⚠️ THE SIGN IS THE WHOLE DISCRIMINATOR, and it is easy to get backwards
    /// — this test exists because an earlier draft did.
    ///
    /// A mint's recipients are outputs of the minting transaction, so they are
    /// never the missing side. Supply short of its mint therefore fails even
    /// while the walk is admittedly partway.
    #[test]
    fn supply_minted_to_nobody_fails_at_any_completeness() {
        // Minted 100, but only 40 ever reached a party.
        let rows = [row(1, "A", "", 0, 100), row(1, "A", "alice", 40, 100)];
        for c in Completeness::ALL {
            let Verdict::Failed {
                because, offenders, ..
            } = reconcile(&rows).verdict(c)
            else {
                panic!("{}: 40 moved against a mint of 100 must fail", c.as_wire());
            };
            assert_eq!(because, Because::MintedButUnattributed);
            assert_eq!(offenders[0].gap(), -60);
        }
    }

    /// And the mirror case, which looks similar and is NOT a failure: a burn
    /// whose source the walk has not descended to yet. `net_mint = −100` with
    /// no party row is a gap of `+100`, so it grades as coverage.
    #[test]
    fn an_unresolved_burn_is_coverage_not_a_failure() {
        let r = reconcile(&[row(1, "A", "", 0, -100)]);
        assert_eq!(r.balances()[0].gap(), 100);
        assert_eq!(
            r.verdict(Completeness::Partial),
            Verdict::BelowFloor { gap: 100, units: 1 }
        );
    }

    #[test]
    fn units_are_reconciled_independently() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            row(2, "B", "bob", 5, 0),
            row(3, "C", "carol", 7, 7),
        ]);
        let bs = r.balances();
        assert_eq!(bs.len(), 3);
        assert_eq!(
            r.verdict(Completeness::Partial),
            Verdict::BelowFloor { gap: 5, units: 1 }
        );
    }

    #[test]
    fn an_empty_archive_balances() {
        assert_eq!(
            Reconciler::new(Diagnose::Totals).verdict(Completeness::Partial),
            Verdict::Balanced
        );
    }

    /// The diagnostic that turns "this archive is wrong" into "here it is".
    #[test]
    fn an_offending_transaction_is_named_with_its_gap() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            // A spend whose source was never resolved: bob gains 30 and
            // nobody loses it.
            row(2, "A", "bob", 30, 0),
        ]);
        let offenders = r.offenders();
        assert_eq!(offenders.len(), 1);
        assert_eq!(offenders[0].tx_hash, vec![2]);
        assert_eq!(offenders[0].gap(), 30);
    }

    /// A conserving transaction is never named, however many parties it has.
    #[test]
    fn a_balanced_transaction_is_not_an_offender() {
        let r = reconcile(&[
            row(1, "A", "alice", 100, 100),
            row(2, "A", "alice", -100, 0),
            row(2, "A", "bob", 60, 0),
            row(2, "A", "carol", 40, 0),
        ]);
        assert!(r.offenders().is_empty());
    }

    /// ⚠️ Under `Totals` the check still works and the offenders are silent —
    /// so an empty list means "not tracked", never "nothing wrong". That is
    /// why the mode is the caller's to choose.
    #[test]
    fn totals_mode_reconciles_but_names_nobody() {
        let mut r = Reconciler::new(Diagnose::Totals);
        r.observe_all(&[row(1, "A", "bob", 30, 0)]);
        assert_eq!(
            r.verdict(Completeness::Partial),
            Verdict::BelowFloor { gap: 30, units: 1 }
        );
        assert!(r.offenders().is_empty());
    }

    #[test]
    fn every_diagnose_has_its_own_spelling() {
        let mut wires: Vec<&str> = Diagnose::ALL.iter().map(|d| d.as_wire()).collect();
        wires.sort_unstable();
        wires.dedup();
        assert_eq!(wires.len(), Diagnose::ALL.len());
    }

    #[test]
    fn every_because_has_its_own_spelling() {
        let mut wires: Vec<&str> = Because::ALL.iter().map(|b| b.as_wire()).collect();
        wires.sort_unstable();
        wires.dedup();
        assert_eq!(wires.len(), Because::ALL.len());
    }
}
