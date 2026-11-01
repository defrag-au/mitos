//! What an order book's fill paid, from the chain.
//!
//! An order book has no pool whose reserves move with a trade, so the story
//! knows a book fill's tokens and not its ADA. The transaction does, for a
//! book whose orders trade IN PLACE: see [`price_book_fills`].
//!
//! # Axo
//!
//! MEASURED on $NikePig (`e97fb96f…`): a trade spends two orders at Axo
//! scripts, each under its owner's stake, and both continue at their own
//! address. The buyer's order went from 19.922260 ADA and no units to
//! 9.922260 ADA and 625; the seller's from 22.223960 ADA and 1,224 units to
//! 30.562846 ADA and 599. So the buyer paid exactly 10 ADA, and the seller got
//! 8.338886: the 1.661114 between is the matcher's 1.2 ADA and the network
//! fee. Each side's fill is priced at ITS OWN order's change, which is what
//! that party paid or got.
//!
//! # Not SaturnSwap, yet
//!
//! A SaturnSwap taker sell pays its units into an escrow whose datum is a
//! 16-byte order id, and the ADA arrives in a later transaction that the
//! escrow's next ones do not obviously include (MEASURED, `6a431ebe…`): its
//! fills stay unpriced here, and a reader values them at the pools' price.

use std::collections::HashMap;

use policy_archive::story::{Kind, StoryEvent, Trader};
use tx_index::wire::ResolvedOutput;

use crate::shared_fills::Chain;

/// How a pass went.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Priced {
    pub priced: usize,
    /// Book fills the chain did not price: the index had no body, or the
    /// party's orders did not move by the fill's units.
    pub unpriced: usize,
}

/// The ADA each Axo fill's party paid (a buy) or got (a sell), by its index
/// in `events`: the change across the fill's transaction in what that
/// party's orders at Axo scripts hold, accepted only where their units moved
/// by exactly the fill's, in its direction, and the ADA the other way.
pub fn price_book_fills(
    events: &[StoryEvent],
    policy_hex: &str,
    chain: &mut impl Chain,
) -> (HashMap<usize, u64>, Priced) {
    let mut prices = HashMap::new();
    let mut out = Priced::default();
    for (i, e) in events.iter().enumerate() {
        let Kind::Fill {
            venue,
            party: Trader::Stake(stake),
            amount,
            into_pool: sell,
        } = &e.kind
        else {
            continue;
        };
        if venue != "axo" {
            continue;
        }
        match axo_fill(
            &e.tx,
            stake,
            *sell,
            amount.unsigned_abs(),
            policy_hex,
            chain,
        ) {
            Some(l) => {
                prices.insert(i, l);
                out.priced += 1;
            }
            None => out.unpriced += 1,
        }
    }
    (prices, out)
}

/// One Axo fill: see [`price_book_fills`].
fn axo_fill(
    tx: &[u8],
    stake: &str,
    sell: bool,
    units: u64,
    policy_hex: &str,
    chain: &mut impl Chain,
) -> Option<u64> {
    let tx = <[u8; 32]>::try_from(tx).ok()?;
    let stake = hex::decode(stake).ok()?;
    let mine = |o: &ResolvedOutput| {
        mitos_cohort::payment_cred(&o.address)
            .is_some_and(|c| mitos_dex_decode::axo::ORDER_CREDS.contains(&c))
            && mitos_cohort::stake_cred(&o.address).is_some_and(|s| s[..] == stake[..])
    };
    let held = |o: &ResolvedOutput| -> (i128, i128) {
        let u: u64 = o
            .assets
            .iter()
            .filter(|a| a.policy == policy_hex)
            .map(|a| a.quantity)
            .sum();
        (i128::from(o.lovelace), i128::from(u))
    };
    let (mut ada, mut held_units) = (0i128, 0i128);
    for (ptx, ix) in chain.inputs(&tx)? {
        let o = chain.output(&ptx, ix)?;
        if mine(&o) {
            let (l, u) = held(&o);
            ada -= l;
            held_units -= u;
        }
    }
    let mut ix = 0;
    while let Some(o) = chain.output(&tx, ix) {
        if mine(&o) {
            let (l, u) = held(&o);
            ada += l;
            held_units += u;
        }
        ix += 1;
    }
    let units = i128::from(units);
    let right = match sell {
        true => held_units == -units && ada > 0,
        false => held_units == units && ada < 0,
    };
    right.then(|| ada.unsigned_abs() as u64)
}

/// Print the first `per_venue` book fills of each book venue as the chain has
/// them: every input (resolved) and output, with its ADA, its units of
/// `policy_hex` and its inline datum. For reading a venue's shape.
pub fn trace_book_fills(
    events: &[StoryEvent],
    policy_hex: &str,
    chain: &mut impl Chain,
    per_venue: usize,
) {
    let mut shown: HashMap<&str, usize> = HashMap::new();
    let line = |o: &ResolvedOutput| {
        let units: u64 = o
            .assets
            .iter()
            .filter(|a| a.policy == policy_hex)
            .map(|a| a.quantity)
            .sum();
        format!(
            "{} ada {} units {units} datum {}",
            o.address,
            o.lovelace,
            o.inline_datum.as_deref().unwrap_or("-")
        )
    };
    for e in events {
        let Kind::Fill {
            venue,
            party,
            amount,
            into_pool,
        } = &e.kind
        else {
            continue;
        };
        if venue != "axo" && venue != "saturnswap" {
            continue;
        }
        let n = shown.entry(venue.as_str()).or_default();
        if *n >= per_venue {
            continue;
        }
        *n += 1;
        let Ok(tx) = <[u8; 32]>::try_from(e.tx.as_slice()) else {
            continue;
        };
        println!(
            "TRACE {venue} tx {} party {party:?} amount {amount} sell {into_pool}",
            hex::encode(tx)
        );
        for (ptx, ix) in chain.inputs(&tx).unwrap_or_default() {
            match chain.output(&ptx, ix) {
                Some(o) => println!("  in  {}", line(&o)),
                None => println!("  in  {}#{ix} unresolved", hex::encode(ptx)),
            }
        }
        let mut ix = 0;
        while let Some(o) = chain.output(&tx, ix) {
            println!("  out {}", line(&o));
            ix += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tx_index::wire::OutputAsset;

    const POLICY: &str = "c881c20e49dbaca3ff6cef365969354150983230c39520b917f5cf7c";
    /// The two orders of `e97fb96f…`, as the box's index has them.
    const BUYER: &str = "addr1z92l7rnra7sxjn5qv5fzc4fwsrrm29mgkleqj9a0y46j5l9ktxxydrvn6ljxm7xaph5cl4ywu8t27dsuegemf0q67q2stfpf4e";
    const SELLER: &str = "addr1z92l7rnra7sxjn5qv5fzc4fwsrrm29mgkleqj9a0y46j5l9756zuzp0gzptpn3lznjd08fk7n2xqce8xmf0nvcmuvl9s33qc2g";
    const MATCHER: &str = "addr1q8dcky3suahtdyjd2jk8q22jfdd7ejmxewmkq0hzra6xun36hdaguxwr4u5lmz5q5tn4mgfdkuuffy2g9xvp9grxw9us5cpm0f";

    use crate::shared_fills::OutRef;

    /// A transaction: what it spends, and what it makes.
    type Body = (Vec<OutRef>, Vec<ResolvedOutput>);

    struct Fake(HashMap<[u8; 32], Body>);

    impl Chain for Fake {
        fn inputs(&mut self, tx: &[u8; 32]) -> Option<Vec<OutRef>> {
            self.0.get(tx).map(|(i, _)| i.clone())
        }
        fn output(&mut self, tx: &[u8; 32], ix: u32) -> Option<ResolvedOutput> {
            self.0
                .get(tx)
                .and_then(|(_, o)| o.get(ix as usize).cloned())
        }
    }

    fn out(index: u32, address: &str, lovelace: u64, units: u64) -> ResolvedOutput {
        ResolvedOutput {
            index,
            address: address.to_string(),
            lovelace,
            assets: (units > 0)
                .then(|| OutputAsset {
                    policy: POLICY.to_string(),
                    name: "4e696b65".to_string(),
                    quantity: units,
                })
                .into_iter()
                .collect(),
            datum_hash: None,
            inline_datum: None,
            has_script_ref: false,
            cbor: String::new(),
        }
    }

    fn fill(stake: &str, sell: bool) -> StoryEvent {
        StoryEvent {
            slot: 1,
            unix: 1,
            tx: vec![3; 32],
            unit: Vec::new(),
            kind: Kind::Fill {
                venue: "axo".to_string(),
                party: Trader::Stake(stake.to_string()),
                amount: 625,
                into_pool: sell,
            },
        }
    }

    /// The traced trade: the buyer paid 10 ADA, the seller got 8.338886.
    #[test]
    fn each_side_of_an_axo_trade_is_its_own_orders_change() {
        let (a, b, fill_tx) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        let chain = HashMap::from([
            (a, (Vec::new(), vec![out(0, BUYER, 19_922_260, 0)])),
            (b, (Vec::new(), vec![out(0, SELLER, 22_223_960, 1_224)])),
            (
                fill_tx,
                (
                    vec![(a, 0), (b, 0)],
                    vec![
                        out(0, SELLER, 30_562_846, 599),
                        out(1, BUYER, 9_922_260, 625),
                        out(2, MATCHER, 1_200_000, 0),
                    ],
                ),
            ),
        ]);
        let stake = |addr: &str| hex::encode(mitos_cohort::stake_cred(addr).expect("a stake"));
        let events = vec![fill(&stake(SELLER), true), fill(&stake(BUYER), false)];
        let (prices, n) = price_book_fills(&events, POLICY, &mut Fake(chain.clone()));
        assert_eq!(prices.get(&0), Some(&8_338_886));
        assert_eq!(prices.get(&1), Some(&10_000_000));
        assert_eq!(
            n,
            Priced {
                priced: 2,
                unpriced: 0
            }
        );

        // A fill the orders did not move by is not priced from them.
        let wrong = vec![StoryEvent {
            kind: Kind::Fill {
                venue: "axo".to_string(),
                party: Trader::Stake(stake(SELLER)),
                amount: 600,
                into_pool: true,
            },
            ..fill("", true)
        }];
        let (prices, n) = price_book_fills(&wrong, POLICY, &mut Fake(chain));
        assert!(prices.is_empty());
        assert_eq!(n.unpriced, 1);
    }
}
