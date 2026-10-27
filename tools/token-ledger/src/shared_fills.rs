//! Name the seller behind a fill on a SHARED order address, from the chain.
//!
//! # Why
//!
//! CSwap keeps every customer's order at one address. A sell is two
//! transactions: the PLACEMENT (the seller's wallet → the order contract) and
//! the FILL (order contract → pool). The fill's token movement names nobody,
//! so the story says `NotEncodedByVenue`, and a screen drew "someone":
//! MEASURED, 34 of $PERP's 35 CSwap sells and 103 of $Aliens' 123.
//!
//! The placement does name the seller, and a reader can pair the two by
//! venue and amount, but only when both are in the page it holds, and only
//! by guessing between two orders of the same size. The chain needs no
//! guess: the fill SPENDS the order output, and that output was created by
//! the placement, whose inputs are the seller's.
//!
//! # How
//!
//! For each anonymous sell: read the fill's inputs, find the one that is an
//! order of a known venue holding this unit (the order it fills), read the
//! transaction that created it (the placement), and take the wallet among its
//! inputs that held the unit. One wallet, or nothing: two different wallets
//! funding one placement is not guessed between.
//!
//! The index covers finalised chain history only. A fill in the volatile tip
//! is left as it was, for the reader's own pairing; those are recent, so
//! their placements are almost always on the same page anyway.

use std::collections::{HashMap, HashSet};

use policy_archive::story::{Kind, StoryEvent, Trader};
use policy_archive::trade::{Role, Roles, address_parts};
use tx_index::wire::ResolvedOutput;

/// An output reference: the transaction that made it, and its index there.
pub type OutRef = ([u8; 32], u32);

/// A decoded transaction: what it spends, and what it makes.
type Decoded = (Vec<OutRef>, Vec<ResolvedOutput>);

/// What the naming reads: a transaction's inputs, and an output by outref.
/// `None` where the index cannot say (the volatile tip, a decode failure).
pub trait Chain {
    fn inputs(&mut self, tx: &[u8; 32]) -> Option<Vec<OutRef>>;
    fn output(&mut self, tx: &[u8; 32], ix: u32) -> Option<ResolvedOutput>;
}

/// The transaction index as a [`Chain`], each body decoded once per request.
pub struct IndexChain<'a> {
    index: &'a tx_index::Index,
    /// tx → what it spends and makes, or `None` where the index has no such
    /// body.
    seen: HashMap<[u8; 32], Option<Decoded>>,
}

impl<'a> IndexChain<'a> {
    pub fn new(index: &'a tx_index::Index) -> Self {
        Self {
            index,
            seen: HashMap::new(),
        }
    }

    fn body(&mut self, tx: &[u8; 32]) -> Option<&Decoded> {
        let index = self.index;
        self.seen
            .entry(*tx)
            .or_insert_with(|| {
                let body = match index.tx(tx) {
                    Ok(Some(b)) => b,
                    Ok(None) => return None,
                    Err(e) => {
                        tracing::debug!("shared fills: reading {}: {e}", hex::encode(tx));
                        return None;
                    }
                };
                match (body.inputs(), body.outputs()) {
                    (Ok(i), Ok(o)) => Some((i, o)),
                    (Err(e), _) | (_, Err(e)) => {
                        tracing::debug!("shared fills: decoding {}: {e}", hex::encode(tx));
                        None
                    }
                }
            })
            .as_ref()
    }
}

impl Chain for IndexChain<'_> {
    fn inputs(&mut self, tx: &[u8; 32]) -> Option<Vec<([u8; 32], u32)>> {
        self.body(tx).map(|(i, _)| i.clone())
    }

    fn output(&mut self, tx: &[u8; 32], ix: u32) -> Option<ResolvedOutput> {
        self.body(tx).and_then(|(_, o)| o.get(ix as usize).cloned())
    }
}

/// At most this many fills are followed back in one story: each costs a
/// handful of reads, and a story is answered while someone waits.
pub const MAX_FILLS: usize = 1_000;

/// How a pass went: sells named, and sells the chain could not name.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Named {
    pub named: usize,
    pub unnamed: usize,
}

/// Name every anonymous sell in `events` that the chain can: its `party`
/// becomes the wallet that placed the order it filled. Events of other units
/// of `policy_hex`, and of anything else, are left alone. At most `limit`
/// fills are followed; the rest count as unnamed.
pub fn name_shared_fills(
    events: &mut [StoryEvent],
    policy_hex: &str,
    roles: &Roles,
    chain: &mut impl Chain,
    limit: usize,
) -> Named {
    let mut out = Named::default();
    // Order outputs already credited to a fill: in a batch, each order is
    // one fill's and no other's.
    let mut used: HashSet<OutRef> = HashSet::new();
    for e in events.iter_mut() {
        let Kind::Fill {
            party,
            into_pool: true,
            amount,
            ..
        } = &mut e.kind
        else {
            continue;
        };
        if *party != Trader::NotEncodedByVenue {
            continue;
        }
        if out.named + out.unnamed >= limit {
            out.unnamed += 1;
            continue;
        }
        let Ok(tx) = <[u8; 32]>::try_from(e.tx.as_slice()) else {
            out.unnamed += 1;
            continue;
        };
        let unit = hex::encode(&e.unit);
        let fill = Fill {
            tx: &tx,
            policy_hex,
            unit_hex: &unit,
            amount: amount.unsigned_abs(),
        };
        match seller(&fill, roles, chain, &mut used) {
            Some(wallet) => {
                *party = Trader::Wallet(wallet);
                out.named += 1;
            }
            None => out.unnamed += 1,
        }
    }
    out
}

/// How much of the unit an output holds.
fn held(o: &ResolvedOutput, policy_hex: &str, unit_hex: &str) -> u64 {
    o.assets
        .iter()
        .filter(|a| a.policy == policy_hex && a.name == unit_hex)
        .map(|a| a.quantity)
        .sum()
}

fn holds(o: &ResolvedOutput, policy_hex: &str, unit_hex: &str) -> bool {
    held(o, policy_hex, unit_hex) > 0
}

/// One anonymous fill to follow back.
struct Fill<'a> {
    tx: &'a [u8; 32],
    policy_hex: &'a str,
    unit_hex: &'a str,
    /// What the fill moved, to tell several orders in one batch apart.
    amount: u64,
}

fn role(address: &str, roles: &Roles) -> Option<Role> {
    address_parts(address).and_then(|(cred, _)| roles.role_of(&cred))
}

/// The wallet that placed the order `fill` spent, if the chain says.
///
/// The order is the fill transaction's one unused order-contract input
/// holding the unit. Where a batch spent several, the one holding exactly the
/// fill's amount; two of the same size, or none, is not guessed between.
fn seller(
    fill: &Fill,
    roles: &Roles,
    chain: &mut impl Chain,
    used: &mut HashSet<OutRef>,
) -> Option<String> {
    let Fill {
        tx,
        policy_hex,
        unit_hex,
        amount,
    } = *fill;
    let mut orders: Vec<(OutRef, u64)> = Vec::new();
    for input in chain.inputs(tx)? {
        if used.contains(&input) {
            continue;
        }
        let Some(o) = chain.output(&input.0, input.1) else {
            continue;
        };
        if role(&o.address, roles) == Some(Role::Order) && holds(&o, policy_hex, unit_hex) {
            orders.push((input, held(&o, policy_hex, unit_hex)));
        }
    }
    let order = match orders.as_slice() {
        [(only, _)] => *only,
        _ => {
            let mut exact = orders.iter().filter(|(_, q)| *q == amount);
            match (exact.next(), exact.next()) {
                (Some((one, _)), None) => *one,
                _ => return None,
            }
        }
    };
    used.insert(order);
    let placement = order.0;
    // Who funded it: the inputs that held the unit, outside any venue.
    let mut wallets: Vec<String> = Vec::new();
    for (tx, ix) in chain.inputs(&placement)? {
        let Some(o) = chain.output(&tx, ix) else {
            continue;
        };
        if holds(&o, policy_hex, unit_hex) && role(&o.address, roles).is_none() {
            wallets.push(o.address);
        }
    }
    // One wallet: several addresses under one stake key are one wallet.
    let identity = |a: &str| match address_parts(a) {
        Some((_, Some(stake))) => stake,
        _ => a.to_string(),
    };
    let first = wallets.first()?;
    wallets
        .iter()
        .all(|w| identity(w) == identity(first))
        .then(|| first.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tx_index::wire::OutputAsset;

    const POLICY: &str = "d9d59e71eb99e653a493956a6826a406060706bd8c9a2427b3564f64";
    const UNIT: &str = "50455250";
    // Real $PERP addresses, so the credentials are the ones on chain.
    const CSWAP_ORDER: &str = "addr1z8d9k3aw6w24eyfjacy809h68dv2rwnpw0arrfau98jk6nhv88awp8sgxk65d6kry0mar3rd0dlkfljz7dv64eu39vfs38yd9p";
    const CSWAP_POOL: &str = "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";
    const SELLER: &str = "addr1q99qpegypskhugq6nss8gjkwvjlj35wd54venfynreqxjgkt55dzuk6msq4qgq4g0dmz6tkvdvdfkmgd6uyd4urms2ksl0yc60";
    const OTHER: &str = "addr1v9srzmt3s3h98jdy6e08jm2605s5yjfhjt0dg0na4e77yssehvg4s";

    fn roles() -> Roles {
        let mut r = Roles::default();
        let cred = |a: &str| address_parts(a).expect("an address").0;
        r.register(cred(CSWAP_ORDER), Role::Order, "cswap");
        r.register(cred(CSWAP_POOL), Role::Pool, "cswap");
        r
    }

    fn out(address: &str, units: u64) -> ResolvedOutput {
        ResolvedOutput {
            index: 0,
            address: address.to_string(),
            lovelace: 2_000_000,
            assets: match units {
                0 => vec![],
                q => vec![OutputAsset {
                    policy: POLICY.to_string(),
                    name: UNIT.to_string(),
                    quantity: q,
                }],
            },
            datum_hash: None,
            inline_datum: None,
            has_script_ref: false,
            cbor: String::new(),
        }
    }

    /// A chain of transactions by hash: inputs, and outputs by index.
    #[derive(Default)]
    struct Fake(HashMap<[u8; 32], Decoded>);

    impl Chain for Fake {
        fn inputs(&mut self, tx: &[u8; 32]) -> Option<Vec<([u8; 32], u32)>> {
            self.0.get(tx).map(|(i, _)| i.clone())
        }
        fn output(&mut self, tx: &[u8; 32], ix: u32) -> Option<ResolvedOutput> {
            self.0
                .get(tx)
                .and_then(|(_, o)| o.get(ix as usize).cloned())
        }
    }

    const FUNDING: [u8; 32] = [1; 32];
    const PLACEMENT: [u8; 32] = [2; 32];
    const FILL: [u8; 32] = [3; 32];
    const POOL_TX: [u8; 32] = [4; 32];

    /// A wallet's tokens, an order placed with them, and a fill spending the
    /// order beside the pool.
    fn chain(funders: Vec<ResolvedOutput>) -> Fake {
        let mut c = Fake::default();
        let n = funders.len() as u32;
        c.0.insert(FUNDING, (vec![], funders));
        c.0.insert(
            PLACEMENT,
            (
                (0..n).map(|i| (FUNDING, i)).collect(),
                vec![out(CSWAP_ORDER, 500)],
            ),
        );
        c.0.insert(POOL_TX, (vec![], vec![out(CSWAP_POOL, 90_000)]));
        c.0.insert(
            FILL,
            (
                vec![(POOL_TX, 0), (PLACEMENT, 0)],
                vec![out(CSWAP_POOL, 90_500)],
            ),
        );
        c
    }

    fn fill(party: Trader) -> StoryEvent {
        StoryEvent {
            slot: 10,
            unix: 10,
            tx: FILL.to_vec(),
            unit: hex::decode(UNIT).expect("hex"),
            kind: Kind::Fill {
                venue: "cswap".into(),
                party,
                amount: 500,
                into_pool: true,
            },
        }
    }

    fn party(e: &StoryEvent) -> &Trader {
        match &e.kind {
            Kind::Fill { party, .. } => party,
            other => panic!("not a fill: {other:?}"),
        }
    }

    #[test]
    fn a_fill_is_named_from_the_order_it_spent() {
        let mut events = vec![fill(Trader::NotEncodedByVenue)];
        let mut c = chain(vec![out(SELLER, 500), out(OTHER, 0)]);
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(
            n,
            Named {
                named: 1,
                unnamed: 0
            }
        );
        assert_eq!(party(&events[0]), &Trader::Wallet(SELLER.to_string()));
    }

    /// A batch spending two orders: each fill is named from the order holding
    /// its own amount, and neither order is credited twice. Two orders of the
    /// same size are not guessed between.
    #[test]
    fn a_batch_names_each_fill_from_its_own_order() {
        const FUNDING_B: [u8; 32] = [5; 32];
        const PLACEMENT_B: [u8; 32] = [6; 32];
        let mut c = chain(vec![out(SELLER, 500)]);
        c.0.insert(FUNDING_B, (vec![], vec![out(OTHER, 200)]));
        c.0.insert(
            PLACEMENT_B,
            (vec![(FUNDING_B, 0)], vec![out(CSWAP_ORDER, 200)]),
        );
        c.0.insert(
            FILL,
            (
                vec![(POOL_TX, 0), (PLACEMENT, 0), (PLACEMENT_B, 0)],
                vec![out(CSWAP_POOL, 90_700)],
            ),
        );
        let sized = |amount| {
            let mut e = fill(Trader::NotEncodedByVenue);
            if let Kind::Fill { amount: a, .. } = &mut e.kind {
                *a = amount;
            }
            e
        };
        let mut events = vec![sized(200), sized(500)];
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(n.named, 2);
        assert_eq!(party(&events[0]), &Trader::Wallet(OTHER.to_string()));
        assert_eq!(party(&events[1]), &Trader::Wallet(SELLER.to_string()));

        // The same two orders at the same size: neither is named.
        c.0.insert(FUNDING_B, (vec![], vec![out(OTHER, 500)]));
        c.0.insert(
            PLACEMENT_B,
            (vec![(FUNDING_B, 0)], vec![out(CSWAP_ORDER, 500)]),
        );
        let mut events = vec![sized(500), sized(500)];
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(n.named, 0);
    }

    /// Two different wallets' tokens in one placement: not guessed between.
    #[test]
    fn two_funding_wallets_are_not_guessed_between() {
        let mut events = vec![fill(Trader::NotEncodedByVenue)];
        let mut c = chain(vec![out(SELLER, 300), out(OTHER, 200)]);
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(
            n,
            Named {
                named: 0,
                unnamed: 1
            }
        );
        assert_eq!(party(&events[0]), &Trader::NotEncodedByVenue);
    }

    /// The volatile tip: the index has no such fill, and it is left alone.
    #[test]
    fn a_fill_the_index_cannot_see_is_left_alone() {
        let mut events = vec![fill(Trader::NotEncodedByVenue)];
        let mut c = Fake::default();
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(
            n,
            Named {
                named: 0,
                unnamed: 1
            }
        );
        assert_eq!(party(&events[0]), &Trader::NotEncodedByVenue);
    }

    /// Only anonymous sells are followed: a named one keeps its name, and
    /// nothing is read for it.
    #[test]
    fn a_named_fill_is_not_followed() {
        let named = Trader::Stake("stake1u…".to_string());
        let mut events = vec![fill(named.clone())];
        let mut c = Fake::default();
        let n = name_shared_fills(&mut events, POLICY, &roles(), &mut c, MAX_FILLS);
        assert_eq!(n, Named::default());
        assert_eq!(party(&events[0]), &named);
    }
}
