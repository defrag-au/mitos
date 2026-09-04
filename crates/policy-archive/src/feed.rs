//! Rows → what a feed shows: transactions with per-unit, per-party
//! movements, and the direction each unit's movement can honestly be given.
//!
//! Shared by every reader — the box's API and a Worker reading R2 — so the
//! sum rule and the direction rule have one home.

use std::collections::{BTreeMap, HashMap};

use crate::schema::Movement;

/// One transaction, as a feed reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRow {
    pub tx_hash: Vec<u8>,
    pub slot: u64,
    pub block_time: u64,
    /// One entry per unit of the policy that this transaction touched.
    pub units: Vec<UnitMove>,
}

/// One unit's movement within one transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitMove {
    /// On-chain asset-name bytes.
    pub name: Vec<u8>,
    /// Net mint for this unit here: 0 transfer, positive mint, negative burn.
    pub net_mint: i64,
    /// Every party whose balance of this unit changed — the PRIMITIVE,
    /// carried as-is. Direction is derived from it, never stored, and the
    /// derivation is allowed to decline.
    pub parties: Vec<PartyMove>,
}

/// One party's signed movement of one unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartyMove {
    pub address: String,
    pub amount: i64,
}

/// What happened to a unit, as far as its deltas can say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Direction {
    /// Exactly one loser and one gainer.
    Transfer { from: String, to: String },
    /// Created here; one recipient.
    Mint { to: String },
    /// Destroyed here; one source.
    Burn { from: String },
    /// Several parties on a side — a batched fill, a bulk move. Stated,
    /// never guessed: "the largest delta is the sender" is wrong exactly
    /// here.
    Ambiguous,
    /// The source sits below the walk's floor, so only the arrival is known.
    /// Distinct from `Ambiguous`: this resolves by walking deeper, that does
    /// not resolve at all.
    SourceBelowFloor { to: String },
}

/// Turn signed per-party deltas into a direction, or decline.
pub fn direction(unit: &UnitMove) -> Direction {
    let losers: Vec<&PartyMove> = unit.parties.iter().filter(|p| p.amount < 0).collect();
    let gainers: Vec<&PartyMove> = unit.parties.iter().filter(|p| p.amount > 0).collect();
    match (losers.as_slice(), gainers.as_slice(), unit.net_mint) {
        ([], [to], m) if m > 0 => Direction::Mint {
            to: to.address.clone(),
        },
        ([from], [], m) if m < 0 => Direction::Burn {
            from: from.address.clone(),
        },
        ([from], [to], 0) => Direction::Transfer {
            from: from.address.clone(),
            to: to.address.clone(),
        },
        ([], [to], 0) => Direction::SourceBelowFloor {
            to: to.address.clone(),
        },
        _ => Direction::Ambiguous,
    }
}

/// Rows → feed rows. Sums per `(transaction, unit, party)`, drops parties
/// that net to nothing, drops a unit whose parties all netted out and that
/// was neither minted nor burned (the asset rode through as change), keeps a
/// unit with no parties when a placeholder said it was minted or burned, and
/// orders newest-first.
pub fn fold_rows(rows: Vec<Movement>) -> Vec<FeedRow> {
    struct Acc {
        slot: u64,
        block_time: u64,
        units: BTreeMap<Vec<u8>, (i64, BTreeMap<String, i64>)>,
    }
    let mut by_tx: HashMap<Vec<u8>, Acc> = HashMap::new();
    for m in rows {
        let acc = by_tx.entry(m.tx_hash.clone()).or_insert_with(|| Acc {
            slot: m.slot,
            block_time: m.block_time,
            units: BTreeMap::new(),
        });
        let unit = acc.units.entry(m.unit_name.clone()).or_default();
        if m.net_mint != 0 {
            unit.0 = m.net_mint;
        }
        if !m.is_placeholder() {
            *unit.1.entry(m.address.clone()).or_insert(0) += m.amount;
        }
    }
    let mut out: Vec<FeedRow> = by_tx
        .into_iter()
        .filter_map(|(tx_hash, acc)| {
            let units: Vec<UnitMove> = acc
                .units
                .into_iter()
                .map(|(name, (net_mint, parties))| UnitMove {
                    name,
                    net_mint,
                    parties: parties
                        .into_iter()
                        .filter(|(_, amount)| *amount != 0)
                        .map(|(address, amount)| PartyMove { address, amount })
                        .collect(),
                })
                .filter(|u| !u.parties.is_empty() || u.net_mint != 0)
                .collect();
            (!units.is_empty()).then_some(FeedRow {
                tx_hash,
                slot: acc.slot,
                block_time: acc.block_time,
                units,
            })
        })
        .collect();
    out.sort_by(|a, b| b.slot.cmp(&a.slot).then_with(|| b.tx_hash.cmp(&a.tx_hash)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mv(tx: u8, unit: &str, addr: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot: 100 + tx as u64,
            block_time: 1_700_000_000 + tx as u64,
            tx_hash: vec![tx; 32],
            unit_name: unit.as_bytes().to_vec(),
            address: addr.to_string(),
            amount,
            net_mint,
        }
    }

    fn party(addr: &str, amount: i64) -> PartyMove {
        PartyMove {
            address: addr.into(),
            amount,
        }
    }

    fn unit(net_mint: i64, parties: Vec<PartyMove>) -> UnitMove {
        UnitMove {
            name: b"U".to_vec(),
            net_mint,
            parties,
        }
    }

    #[test]
    fn a_party_appearing_on_both_sides_nets_out() {
        let out = fold_rows(vec![
            mv(1, "A", "alice", 1, 0),
            mv(1, "A", "alice", -1, 0),
            mv(1, "B", "bob", 1, 0),
            mv(1, "B", "alice", -1, 0),
        ]);
        assert_eq!(out.len(), 1);
        assert!(!out[0].units.iter().any(|u| u.name == b"A"));
        assert_eq!(out[0].units[0].parties.len(), 2);
    }

    #[test]
    fn a_transaction_that_moved_nothing_is_not_a_row() {
        assert!(fold_rows(vec![mv(1, "A", "alice", 1, 0), mv(1, "A", "alice", -1, 0)]).is_empty());
    }

    #[test]
    fn a_placeholder_keeps_the_unit_without_a_party() {
        let out = fold_rows(vec![mv(2, "A", "", 0, -1)]);
        assert_eq!(out[0].units[0].net_mint, -1);
        assert!(out[0].units[0].parties.is_empty());
    }

    #[test]
    fn rows_come_back_newest_first() {
        let out = fold_rows(vec![
            mv(1, "A", "a", 1, 0),
            mv(3, "A", "a", 1, 0),
            mv(2, "A", "a", 1, 0),
        ]);
        assert_eq!(
            out.iter().map(|r| r.slot).collect::<Vec<_>>(),
            vec![103, 102, 101]
        );
    }

    #[test]
    fn directions_are_derived_and_decline_when_they_must() {
        assert!(matches!(
            direction(&unit(0, vec![party("alice", -1), party("bob", 1)])),
            Direction::Transfer { ref from, ref to } if from == "alice" && to == "bob"
        ));
        assert!(matches!(
            direction(&unit(1, vec![party("bob", 1)])),
            Direction::Mint { .. }
        ));
        assert!(matches!(
            direction(&unit(-1, vec![party("alice", -1)])),
            Direction::Burn { .. }
        ));
        assert!(matches!(
            direction(&unit(0, vec![party("bob", 1)])),
            Direction::SourceBelowFloor { .. }
        ));
        assert_eq!(
            direction(&unit(
                0,
                vec![party("a", -1), party("b", -1), party("c", 2)]
            )),
            Direction::Ambiguous
        );
        // A stray zero party does not turn a transfer ambiguous.
        assert!(matches!(
            direction(&unit(
                0,
                vec![party("alice", -1), party("bob", 1), party("idle", 0)]
            )),
            Direction::Transfer { .. }
        ));
    }
}
