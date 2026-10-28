//! Who controls an address, where its stake credential does not say.
//!
//! A Shelley wallet's addresses share a stake credential, and grouping by it
//! is exact. Some addresses carry none, and a wallet behind them can still be
//! recognised from how it moves. This module holds those recognitions as
//! RULES over one transaction's movement of one unit, each naming two
//! addresses it judges to share an owner, and joins them transitively.
//!
//! # Adding a rule
//!
//! A rule is a [`Rule`]: one unit's movement in one transaction, in, and the
//! pair of addresses it judges one owner's, out. Add it to [`RULES`] with the
//! case that measured it. A rule should be NARROW: a wrong join merges two
//! people into one party for the token's whole history, and nothing
//! downstream can split them again.
//!
//! # Stability
//!
//! The address that stands for a group is the one seen FIRST in the chain's
//! order, so a group growing forward keeps its stand-in, and a party table
//! numbered from stand-ins only ever extends. Two groups that already exist
//! joining later is the one case that changes a stand-in; the counts say how
//! often that happened ([`Owners::late_joins`]).

use std::collections::HashMap;

use crate::feed::{FeedRow, UnitMove};

/// One recognition: the two addresses this movement shows to share an owner.
pub type Rule = fn(&UnitMove) -> Option<(&str, &str)>;

/// Every rule, applied in order to each unit movement.
pub const RULES: &[Rule] = &[byron_change];

/// Whether an address is a Byron one, spelled in base58 (`Ae2…`, `DdzFF…`)
/// rather than bech32.
pub fn is_byron(address: &str) -> bool {
    !address.starts_with("addr") && !address.starts_with("stake") && !address.is_empty()
}

/// A token moving whole from one Byron address to another, and nowhere else:
/// a legacy wallet spending its holdings into a fresh change address.
///
/// MEASURED on $NikePig (2026-10-27): 81,321 such hops, 65% of the token's
/// transfers, made 81,383 of its 98,468 parties. One wallet held 1,000 units
/// inside a bundle of 80 tokens and re-created it at a new `DdzFF…` address on
/// every transaction, minutes apart, while withdrawing ADA to Shelley
/// addresses. A Byron wallet derives a new address per change output and has
/// no stake credential to join them by.
///
/// The risk is two different people moving the token between Byron addresses,
/// which this would merge. On a token minted in the Shelley era, a Byron
/// address paying another is overwhelmingly one wallet's change.
pub fn byron_change(unit: &UnitMove) -> Option<(&str, &str)> {
    if unit.net_mint != 0 {
        return None;
    }
    let [a, b] = unit.parties.as_slice() else {
        return None;
    };
    let (from, to) = match (a.amount < 0, b.amount < 0) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => return None,
    };
    (from.amount == -to.amount && is_byron(&from.address) && is_byron(&to.address))
        .then_some((from.address.as_str(), to.address.as_str()))
}

/// The owners [`RULES`] recognised in a history.
#[derive(Debug, Default, Clone)]
pub struct Owners {
    /// Address → the address standing for its owner, for every address a
    /// rule joined to another. Absent means it stands for itself.
    stand_in: HashMap<String, String>,
    /// Joins of two groups that each already had more than one address.
    late_joins: usize,
}

impl Owners {
    /// Apply every rule to `rows`, in any order: they are visited oldest
    /// first, as the chain made them.
    pub fn infer(rows: &[FeedRow]) -> Self {
        // Each address's first sighting, oldest first: the order stand-ins
        // are chosen by.
        let mut order: Vec<&FeedRow> = rows.iter().collect();
        order.sort_by_key(|r| r.slot);
        let mut seen: HashMap<&str, usize> = HashMap::new();
        let mut parent: HashMap<&str, &str> = HashMap::new();
        let mut size: HashMap<&str, usize> = HashMap::new();
        let mut late_joins = 0;
        fn root<'a>(parent: &mut HashMap<&'a str, &'a str>, a: &'a str) -> &'a str {
            let mut r = a;
            while let Some(&p) = parent.get(r) {
                if p == r {
                    break;
                }
                r = p;
            }
            // Path compression, so long change chains stay cheap.
            let mut x = a;
            while let Some(&p) = parent.get(x) {
                if p == r {
                    break;
                }
                parent.insert(x, r);
                x = p;
            }
            r
        }
        for row in order {
            for unit in &row.units {
                for p in &unit.parties {
                    let n = seen.len();
                    seen.entry(p.address.as_str()).or_insert(n);
                }
                for rule in RULES {
                    let Some((a, b)) = rule(unit) else { continue };
                    let (ra, rb) = (root(&mut parent, a), root(&mut parent, b));
                    if ra == rb {
                        continue;
                    }
                    let (sa, sb) = (*size.get(ra).unwrap_or(&1), *size.get(rb).unwrap_or(&1));
                    if sa > 1 && sb > 1 {
                        late_joins += 1;
                    }
                    // The earlier-seen root stands for both.
                    let (keep, fold) = match seen[ra] <= seen[rb] {
                        true => (ra, rb),
                        false => (rb, ra),
                    };
                    parent.insert(fold, keep);
                    parent.insert(keep, keep);
                    size.insert(keep, sa + sb);
                }
            }
        }
        let addresses: Vec<&str> = parent.keys().copied().collect();
        let mut stand_in = HashMap::new();
        for a in addresses {
            let r = root(&mut parent, a);
            if r != a {
                stand_in.insert(a.to_string(), r.to_string());
            }
        }
        Self {
            stand_in,
            late_joins,
        }
    }

    /// The address standing for `address`'s owner: itself, unless a rule
    /// joined it to an earlier one.
    pub fn owner<'a>(&'a self, address: &'a str) -> &'a str {
        self.stand_in.get(address).map_or(address, String::as_str)
    }

    /// How many addresses stand for an owner other than themselves.
    pub fn joined(&self) -> usize {
        self.stand_in.len()
    }

    /// Joins of two groups that each already held several addresses: the one
    /// case that moves a stand-in after the fact.
    pub fn late_joins(&self) -> usize {
        self.late_joins
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::PartyMove;

    const B1: &str = "DdzFFzCqrhsvZkn8TUVd1xTNFaz9x1CcfQvj32Q34yqEDvAvXhEo2QnXu5g37TQDUNDZcse1tL5RwjFR5BYsT7ZQPcSFvY3JyffLxgMq";
    const B2: &str = "DdzFFzCqrhsihhE1d8tHuYruPmBNRktYrYJEV8g7bEH2a1SqziFXqQYqTtyUXMpipsZU46heo1i8TR3zHpqjiESebBwL4pLdvt5satT5";
    const B3: &str = "Ae2tdPwUPEZBHCDjH7LvfySgBetNBqmtxsz6GgHSepGp7oDF1pjJDLVREXd";
    const SHELLEY: &str = "addr1qyhpqw86efnfr5z9vheev0xjzjecrp0vu70vjwqct3z9ealaw4t04muxhuh7yxcjsnkr7aamw3ts0qymfmd746f6ugpqrsvqgh";

    fn row(slot: u64, parties: &[(&str, i64)]) -> FeedRow {
        FeedRow {
            tx_hash: vec![slot as u8],
            slot,
            block_time: slot,
            units: vec![UnitMove {
                name: b"Nike".to_vec(),
                net_mint: 0,
                parties: parties
                    .iter()
                    .map(|(a, n)| PartyMove {
                        address: a.to_string(),
                        amount: *n,
                    })
                    .collect(),
            }],
        }
    }

    /// The measured churn: the same 1,000 units hopping B1 → B2 → B3. All
    /// three stand for B1, the first seen, whatever order the rows come in.
    #[test]
    fn a_change_chain_stands_for_its_first_address() {
        let rows = vec![
            row(3, &[(B2, -1_000), (B3, 1_000)]),
            row(2, &[(B1, -1_000), (B2, 1_000)]),
            row(1, &[(SHELLEY, -1_000), (B1, 1_000)]),
        ];
        let o = Owners::infer(&rows);
        assert_eq!((o.owner(B1), o.owner(B2), o.owner(B3)), (B1, B1, B1));
        assert_eq!(o.owner(SHELLEY), SHELLEY, "a Shelley sender is not joined");
        assert_eq!((o.joined(), o.late_joins()), (2, 0));
    }

    /// Not change: a split to two receivers, a Byron address paying a
    /// Shelley one, a mint, and amounts that do not match.
    #[test]
    fn only_a_whole_one_to_one_byron_hop_is_change() {
        let mv = |parties: &[(&str, i64)], net_mint| UnitMove {
            name: vec![],
            net_mint,
            parties: parties
                .iter()
                .map(|(a, n)| PartyMove {
                    address: a.to_string(),
                    amount: *n,
                })
                .collect(),
        };
        assert!(byron_change(&mv(&[(B1, -10), (B2, 6), (B3, 4)], 0)).is_none());
        assert!(byron_change(&mv(&[(B1, -10), (SHELLEY, 10)], 0)).is_none());
        assert!(byron_change(&mv(&[(B1, -10), (B2, 10)], 5)).is_none());
        assert_eq!(byron_change(&mv(&[(B2, 10), (B1, -10)], 0)), Some((B1, B2)));
    }
}
