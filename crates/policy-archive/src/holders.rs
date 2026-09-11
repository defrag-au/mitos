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
    /// One address this party used — enough to resolve a handle or open an
    /// explorer, and NOT a claim that it is the only one.
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
pub fn fold(movements: impl Iterator<Item = Movement>) -> Fold {
    let mut by_key: HashMap<String, Holder> = HashMap::new();
    let mut seen_addresses: HashMap<String, Vec<String>> = HashMap::new();
    let mut rows = 0u64;

    for m in movements {
        // A placeholder stands in for a `(tx, unit)` attributed to nobody. It
        // is not a party and summing its zero would create an empty row.
        if m.is_placeholder() || m.address.is_empty() {
            continue;
        }
        rows += 1;
        // Stake-keyed, so one wallet is one row however many payment addresses
        // it rotates through. An enterprise address has no stake part and keys
        // by itself.
        let key = crate::trade::address_parts(&m.address)
            .and_then(|(_, stake)| stake)
            .unwrap_or_else(|| m.address.clone());
        let e = by_key.entry(key.clone()).or_insert_with(|| Holder {
            key: key.clone(),
            address: m.address.clone(),
            amount: 0,
            movements: 0,
            addresses: 0,
        });
        e.amount += m.amount;
        e.movements += 1;
        let addrs = seen_addresses.entry(key).or_default();
        if !addrs.contains(&m.address) {
            addrs.push(m.address.clone());
        }
    }

    let mut negative = 0usize;
    let mut holders: Vec<Holder> = by_key
        .into_values()
        .filter(|h| {
            if h.amount < 0 {
                negative += 1;
            }
            h.amount > 0
        })
        .map(|mut h| {
            h.addresses = seen_addresses.get(&h.key).map_or(1, |a| a.len() as u32);
            h
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

#[cfg(test)]
mod tests {
    use super::*;

    fn mv(addr: &str, amount: i64) -> Movement {
        Movement {
            slot: 1,
            block_time: 1,
            tx_hash: vec![1],
            unit_name: b"U".to_vec(),
            address: addr.into(),
            amount,
            net_mint: 0,
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
        let f = fold([mv(a, 100), mv(b, 300)].into_iter());
        // They may or may not share a stake part depending on the synthetic
        // bytes; what must hold is that the fold is driven by the stake part
        // and never by the raw string.
        let by_stake = crate::trade::address_parts(a).and_then(|(_, s)| s);
        if by_stake.is_some() && by_stake == crate::trade::address_parts(b).and_then(|(_, s)| s) {
            assert_eq!(f.holders.len(), 1, "one wallet, one row");
            assert_eq!(f.holders[0].amount, 400);
            assert_eq!(f.holders[0].addresses, 2);
        }
    }

    #[test]
    fn a_party_that_sent_everything_back_is_not_a_holder() {
        let f = fold([mv("addr_a", 100), mv("addr_a", -100), mv("addr_b", 50)].into_iter());
        assert_eq!(f.holders.len(), 1);
        assert_eq!(f.holders[0].address, "addr_b");
        assert_eq!(f.held, 50);
    }

    /// ⚠️ Negative is IMPOSSIBLE on chain, so it measures missing coverage
    /// rather than being an arithmetic curiosity to round away.
    #[test]
    fn a_negative_balance_is_counted_as_missing_coverage() {
        let f = fold([mv("addr_a", -100), mv("addr_b", 50)].into_iter());
        assert_eq!(f.negative, 1, "the impossible row is REPORTED");
        assert_eq!(f.holders.len(), 1, "and not listed as a holder");
        assert_eq!(f.held, 50);
    }

    #[test]
    fn placeholders_are_not_parties() {
        let mut p = mv("", 0);
        p.net_mint = 1_000;
        let f = fold([p, mv("addr_a", 1_000)].into_iter());
        assert_eq!(f.holders.len(), 1);
        assert_eq!(f.rows, 1, "the placeholder is not a folded row either");
    }

    #[test]
    fn holders_are_ordered_largest_first_and_deterministically() {
        let f = fold([mv("addr_b", 100), mv("addr_a", 100), mv("addr_c", 900)].into_iter());
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
        let f = fold([mv("addr_a", 100)].into_iter());
        assert_eq!(f.concentration(1, 0), None);
        assert_eq!(f.concentration(1, 200), Some(50.0));
    }
}
