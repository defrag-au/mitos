//! Movements → TRADES: folding a swap's two or three transactions back into
//! the one thing a person did.
//!
//! # The problem this exists to fix
//!
//! A DEX swap on Cardano is not one transaction. The user sends the asset to an
//! order contract, a batcher later spends that order into the pool, and the
//! proceeds come back from the pool. Measured on $PERP: `wallet→order` 1,379 ·
//! `order→pool` 1,560 · `pool→wallet` 2,269 · `order→wallet` 37. So one swap
//! occupies two or three rows of any feed built straight from movements, and
//! the feed reads as "lots of transfers" — which is exactly the complaint this
//! whole design started from.
//!
//! # The fill is the trade; the placement is a detail
//!
//! `order→pool` is the moment the swap happens, and it carries everything the
//! event needs: the venue (the order's payment credential) and, on most venues,
//! the user.
//!
//! ⚠️ **"Most" is doing real work there, and it is measured.** An order
//! contract usually glues the CUSTOMER's stake credential onto one shared
//! payment script, so the user falls out of the address. On $PERP:
//!
//! | venue | distinct order stakes | also seen as a wallet party |
//! |---|---|---|
//! | Splash | 571 | **571 (100%)** |
//! | Minswap | 359 | **354 (99%)** |
//! | **CSwap** | **1** | **0 (0%)** |
//!
//! CSwap's order contract is a SINGLE address with a fixed stake part, so its
//! user cannot be read off the fill at all. That is why [`Party`] is an enum
//! rather than an `Option<String>`: "this venue does not encode the trader" and
//! "we could not work it out" are different answers, and flattening them would
//! quietly attribute every CSwap trade to one address.
//!
//! # What it will not do
//!
//! Guess. A batched fill spends several orders into one pool in one
//! transaction, and picking "the largest mover" as the trader is wrong exactly
//! there. Those become [`Event::BatchedFill`], counted and named, never
//! decomposed.

use std::collections::HashMap;

use crate::feed::{FeedRow, UnitMove};

/// What a script address is, as far as the caller's registries know.
///
/// Supplied by the caller rather than looked up here: this crate is read by
/// consumers that must not link a decode stack, and the same seam already
/// works for `mitos_cohort::classify`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Pool,
    /// A DEX order/escrow contract.
    Order,
    /// A launchpad bonding curve — trades happen against it directly, with no
    /// order leg.
    Curve,
}

/// Whether an order contract encodes its customer in the address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKeying {
    /// The stake part is the CUSTOMER's, so the trader falls out of the fill.
    CustomerStake,
    /// One fixed address for every order — the trader is not recoverable from
    /// the fill and must come from the placement leg. CSwap is this.
    Shared,
}

/// Roles by payment credential, hex, lower case.
#[derive(Debug, Clone, Default)]
pub struct Roles {
    pub roles: HashMap<String, Role>,
    /// How each ORDER credential keys its customer. Absent means
    /// [`OrderKeying::CustomerStake`], the common case.
    pub keying: HashMap<String, OrderKeying>,
}

impl Roles {
    pub fn role_of(&self, cred_hex: &str) -> Option<Role> {
        self.roles.get(cred_hex).copied()
    }
    pub fn keying_of(&self, cred_hex: &str) -> OrderKeying {
        self.keying
            .get(cred_hex)
            .copied()
            .unwrap_or(OrderKeying::CustomerStake)
    }
}

/// Who traded — and, when nobody can say, WHY not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Party {
    /// Read from the order address's stake credential.
    Stake(String),
    /// A wallet moved the asset directly, with no order contract.
    Wallet(String),
    /// This venue's order contract is one shared address; the trader is in the
    /// placement transaction, not this one.
    NotEncodedByVenue,
    /// Several parties on a side. Stated, never guessed.
    Ambiguous,
}

/// What happened, once the legs are folded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// An order was spent into a pool — the swap itself.
    Fill {
        venue_cred: String,
        party: Party,
        amount: i64,
        /// True when the asset went INTO the pool (the user sold it).
        into_pool: bool,
    },
    /// The asset moved from a wallet to an order contract. Usually the first
    /// leg of a `Fill` that lands later; on a venue with a shared order
    /// address it is the ONLY leg that names the trader.
    Placement {
        venue_cred: String,
        party: Party,
        amount: i64,
    },
    /// An order returned the asset to a wallet — the swap did NOT happen.
    /// Indistinguishable from an ordinary transfer without knowing the
    /// contract, which is why it was invisible before.
    Cancellation {
        venue_cred: String,
        party: Party,
        amount: i64,
    },
    /// Several orders spent into a pool in one transaction. Named and counted;
    /// deliberately not decomposed.
    BatchedFill {
        venue_cred: String,
        orders: usize,
        amount: i64,
    },
    /// Nothing here matched a venue — an ordinary movement.
    Transfer,
}

/// The payment credential of a bech32 Shelley address, lower-case hex, and the
/// stake part if it has one.
///
/// A tiny bech32 decode rather than a `pallas-addresses` dependency: this crate
/// is linked by wasm consumers and the whole job is splitting a known-shape
/// address into two byte ranges.
pub fn address_parts(addr: &str) -> Option<(String, Option<String>)> {
    const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    let sep = addr.rfind('1')?;
    let data = addr.get(sep + 1..)?;
    if data.len() < 6 {
        return None;
    }
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    let mut out: Vec<u8> = Vec::new();
    for c in data[..data.len() - 6].bytes() {
        let v = CHARSET.iter().position(|&x| x == c)? as u32;
        acc = (acc << 5) | v;
        bits += 5;
        while bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xff) as u8);
        }
    }
    if out.len() < 29 {
        return None;
    }
    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    Some((
        hex(&out[1..29]),
        (out.len() >= 57).then(|| hex(&out[29..57])),
    ))
}

/// Fold one transaction's unit movement into an event.
pub fn classify_move(unit: &UnitMove, roles: &Roles) -> Event {
    let losers: Vec<&crate::feed::PartyMove> =
        unit.parties.iter().filter(|p| p.amount < 0).collect();
    let gainers: Vec<&crate::feed::PartyMove> =
        unit.parties.iter().filter(|p| p.amount > 0).collect();

    let role_at = |addr: &str| -> Option<(Role, String, Option<String>)> {
        let (cred, stake) = address_parts(addr)?;
        roles.role_of(&cred).map(|r| (r, cred, stake))
    };

    // A batched fill: several order contracts into one pool. Recognised BEFORE
    // the single-sided shapes, because it is the case where picking a trader
    // would be wrong and the shape is otherwise a plain many→one.
    let order_losers: Vec<_> = losers
        .iter()
        .filter_map(|p| role_at(&p.address).map(|r| (p, r)))
        .filter(|(_, (r, _, _))| *r == Role::Order)
        .collect();
    if order_losers.len() > 1
        && gainers.len() == 1
        && role_at(&gainers[0].address).is_some_and(|(r, _, _)| r == Role::Pool)
    {
        let (_, (_, cred, _)) = &order_losers[0];
        return Event::BatchedFill {
            venue_cred: cred.clone(),
            orders: order_losers.len(),
            amount: order_losers.iter().map(|(p, _)| -p.amount).sum(),
        };
    }

    if losers.len() != 1 || gainers.len() != 1 {
        return Event::Transfer;
    }
    let (from, to) = (losers[0], gainers[0]);
    let (from_role, to_role) = (role_at(&from.address), role_at(&to.address));

    // The trader, from the order address's stake — unless the venue shares one
    // address for every order, in which case nobody can say from this tx.
    let party_from_order = |cred: &str, stake: &Option<String>| match roles.keying_of(cred) {
        OrderKeying::Shared => Party::NotEncodedByVenue,
        OrderKeying::CustomerStake => stake.clone().map_or(Party::Ambiguous, Party::Stake),
    };

    match (from_role, to_role) {
        // order → pool: the swap.
        (Some((Role::Order, cred, stake)), Some((Role::Pool | Role::Curve, _, _))) => Event::Fill {
            party: party_from_order(&cred, &stake),
            venue_cred: cred,
            amount: -from.amount,
            into_pool: true,
        },
        // pool → wallet: the proceeds coming back.
        (Some((Role::Pool | Role::Curve, cred, _)), None) => Event::Fill {
            venue_cred: cred,
            party: Party::Wallet(to.address.clone()),
            amount: to.amount,
            into_pool: false,
        },
        // wallet → order: the placement.
        (None, Some((Role::Order, cred, stake))) => Event::Placement {
            party: party_from_order(&cred, &stake),
            venue_cred: cred,
            amount: to.amount,
        },
        // order → wallet: the swap did NOT happen.
        (Some((Role::Order, cred, _)), None) => Event::Cancellation {
            venue_cred: cred,
            party: Party::Wallet(to.address.clone()),
            amount: to.amount,
        },
        // wallet → curve / wallet → pool: a direct trade, no order leg.
        (None, Some((Role::Pool | Role::Curve, cred, _))) => Event::Fill {
            venue_cred: cred,
            party: Party::Wallet(from.address.clone()),
            amount: -from.amount,
            into_pool: true,
        },
        _ => Event::Transfer,
    }
}

/// Fold a feed page. One event per `(transaction, unit)`.
pub fn fold(rows: &[FeedRow], roles: &Roles) -> Vec<Folded> {
    rows.iter()
        .flat_map(|r| {
            r.units.iter().map(move |u| Folded {
                tx_hash: r.tx_hash.clone(),
                slot: r.slot,
                block_time: r.block_time,
                unit: u.name.clone(),
                event: classify_move(u, roles),
            })
        })
        .collect()
}

/// One classified movement, with everything needed to place it on a timeline.
///
/// ⚠️ `unit` is not decoration. On a COLLECTION every event is about one
/// specific asset, and a fold that drops it can describe a policy but never an
/// NFT — which is half of what these archives are for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Folded {
    pub tx_hash: Vec<u8>,
    pub slot: u64,
    pub block_time: u64,
    /// On-chain asset-name bytes. IDENTITY only — never decode for display.
    pub unit: Vec<u8>,
    pub event: Event,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::PartyMove;

    // Real $PERP addresses, so the credentials below are the ones on chain.
    const SPLASH_POOL: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";
    const SPLASH_ORDER_A: &str = "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q5nzttqv0rf7ptwes676mpvmd9k2v2lnempz5hnnuenqc9sxcqtn2";
    const SPLASH_ORDER_B: &str = "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q3zhe74mrmvmrpvp6wf6h9mrnp55402k87mamfax54th56qawykfv";
    const CSWAP_ORDER: &str = "addr1z8d9k3aw6w24eyfjacy809h68dv2rwnpw0arrfau98jk6nhv88awp8sgxk65d6kry0mar3rd0dlkfljz7dv64eu39vfs38yd9p";
    const CSWAP_POOL: &str = "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";
    const WALLET: &str = "addr1qyhpqw86efnfr5z9vheev0xjzjecrp0vu70vjwqct3z9ealaw4t04muxhuh7yxcjsnkr7aamw3ts0qymfmd746f6ugpqrsvqgh";

    fn roles() -> Roles {
        let cred = |a: &str| address_parts(a).unwrap().0;
        let mut r = Roles::default();
        r.roles.insert(cred(SPLASH_POOL), Role::Pool);
        r.roles.insert(cred(CSWAP_POOL), Role::Pool);
        r.roles.insert(cred(SPLASH_ORDER_A), Role::Order);
        r.roles.insert(cred(CSWAP_ORDER), Role::Order);
        // MEASURED: CSwap's order contract is ONE address for every trader.
        r.keying.insert(cred(CSWAP_ORDER), OrderKeying::Shared);
        r
    }

    fn mv(pairs: &[(&str, i64)]) -> UnitMove {
        UnitMove {
            name: b"PERP COIN".to_vec(),
            net_mint: 0,
            parties: pairs
                .iter()
                .map(|(a, amt)| PartyMove {
                    address: a.to_string(),
                    amount: *amt,
                })
                .collect(),
        }
    }

    /// The whole point: three transactions that were three "transfers" become
    /// a placement and a fill, with the trader named from the order address.
    #[test]
    fn an_order_spent_into_a_pool_is_a_fill_with_a_named_trader() {
        let e = classify_move(
            &mv(&[(SPLASH_ORDER_A, -50_000), (SPLASH_POOL, 50_000)]),
            &roles(),
        );
        match e {
            Event::Fill {
                party,
                amount,
                into_pool,
                ..
            } => {
                assert_eq!(amount, 50_000);
                assert!(into_pool);
                let want = address_parts(SPLASH_ORDER_A).unwrap().1.unwrap();
                assert_eq!(party, Party::Stake(want));
            }
            other => panic!("expected a Fill, got {other:?}"),
        }
    }

    #[test]
    fn a_wallet_to_order_is_a_placement_and_order_to_wallet_is_a_cancellation() {
        assert!(matches!(
            classify_move(&mv(&[(WALLET, -900), (SPLASH_ORDER_A, 900)]), &roles()),
            Event::Placement { .. }
        ));
        // The one that was invisible: without knowing the contract this is an
        // ordinary transfer, and a refund reads as a trade.
        assert!(matches!(
            classify_move(&mv(&[(SPLASH_ORDER_A, -900), (WALLET, 900)]), &roles()),
            Event::Cancellation { .. }
        ));
    }

    /// CSwap shares ONE order address, so the trader is genuinely not in this
    /// transaction. It must say so rather than attribute every CSwap trade to
    /// the same stake.
    #[test]
    fn a_shared_order_address_refuses_to_name_a_trader() {
        let e = classify_move(&mv(&[(CSWAP_ORDER, -1_000), (CSWAP_POOL, 1_000)]), &roles());
        match e {
            Event::Fill { party, .. } => assert_eq!(party, Party::NotEncodedByVenue),
            other => panic!("expected a Fill, got {other:?}"),
        }
        // And the placement leg, which DOES name them, is where to look.
        assert!(matches!(
            classify_move(&mv(&[(WALLET, -1_000), (CSWAP_ORDER, 1_000)]), &roles()),
            Event::Placement {
                party: Party::NotEncodedByVenue,
                ..
            }
        ));
    }

    /// Several orders into one pool. "The largest mover is the trader" is
    /// wrong exactly here, so it is named and counted, never decomposed.
    #[test]
    fn a_batched_fill_is_never_decomposed() {
        let e = classify_move(
            &mv(&[
                (SPLASH_ORDER_A, -100),
                (SPLASH_ORDER_B, -400),
                (SPLASH_POOL, 500),
            ]),
            &roles(),
        );
        match e {
            Event::BatchedFill { orders, amount, .. } => {
                assert_eq!(orders, 2);
                assert_eq!(amount, 500);
            }
            other => panic!("expected a BatchedFill, got {other:?}"),
        }
    }

    #[test]
    fn a_pool_paying_a_wallet_is_the_other_side_of_a_fill() {
        match classify_move(&mv(&[(SPLASH_POOL, -700), (WALLET, 700)]), &roles()) {
            Event::Fill {
                into_pool,
                party,
                amount,
                ..
            } => {
                assert!(!into_pool);
                assert_eq!(amount, 700);
                assert_eq!(party, Party::Wallet(WALLET.into()));
            }
            other => panic!("expected a Fill, got {other:?}"),
        }
    }

    /// Nothing recognised stays a transfer. A fold that turned every movement
    /// into a trade would be worse than no fold.
    #[test]
    fn an_ordinary_transfer_is_left_alone() {
        assert_eq!(
            classify_move(
                &mv(&[(WALLET, -10), (SPLASH_ORDER_B, 10)]),
                &Roles::default()
            ),
            Event::Transfer
        );
    }

    #[test]
    fn address_parts_splits_payment_and_stake() {
        let (cred, stake) = address_parts(SPLASH_POOL).unwrap();
        assert_eq!(
            cred,
            "cb684a69e78907a9796b21fc150a758af5f2805e5ed5d5a8ce9f76f1"
        );
        assert_eq!(
            stake.unwrap(),
            "b2f6abf60ccde92eae1a2f4fdf65f2eaf6208d872c6f0e597cc10b07"
        );
        assert!(address_parts("not-an-address").is_none());
    }
}
