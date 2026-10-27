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
//!
//! A pool paying several WALLETS at once is different, and is decomposed:
//! each wallet and what it received are on chain, so a buy per wallet is a
//! reading, not a guess. See [`classify_moves`].

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::feed::{FeedRow, UnitMove};

/// What a script address is, as far as the caller's registries know.
///
/// Supplied by the caller rather than looked up here: this crate is read by
/// consumers that must not link a decode stack, and the same seam already
/// works for `mitos_cohort::classify`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Pool,
    /// A DEX order/escrow contract.
    Order,
    /// A launchpad bonding curve — trades happen against it directly, with no
    /// order leg.
    Curve,
}

/// Whether an order contract encodes its customer in the address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderKeying {
    /// The stake part is the CUSTOMER's, so the trader falls out of the fill.
    CustomerStake,
    /// One fixed address for every order — the trader is not recoverable from
    /// the fill and must come from the placement leg. CSwap is this.
    Shared,
}

/// Roles by payment credential, hex, lower case.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Roles {
    pub roles: HashMap<String, Role>,
    /// How each ORDER credential keys its customer. Absent means
    /// [`OrderKeying::CustomerStake`], the common case.
    pub keying: HashMap<String, OrderKeying>,
    /// Credential → the venue's NAME.
    ///
    /// ⚠️ Without this a fill carries a raw credential while a pool
    /// observation carries `"splash"`, so the two never join and a consumer
    /// shows named venues with no activity beside hex strings doing all the
    /// trading. That is exactly what the first token band did.
    ///
    /// Only the decode crate knows this mapping, which is why it is supplied
    /// with the roles rather than guessed at downstream.
    pub names: HashMap<String, String>,
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

    /// How ONE order address keys its customer: the credential's keying,
    /// except that an address with no stake part names nobody by stake
    /// whatever the contract usually does, so it reads as
    /// [`OrderKeying::Shared`].
    ///
    /// MEASURED on $NikePig: WingRiders V1 and V2 orders sit both at an
    /// enterprise address shared by every trader and at addresses carrying the
    /// customer's stake. Keying by credential alone either drops the customer
    /// from the staked form (declared shared) or the placing wallet from the
    /// enterprise form (declared customer-keyed). Per address, neither is lost.
    pub fn keying_at(&self, cred_hex: &str, stake: Option<&str>) -> OrderKeying {
        match stake {
            None => OrderKeying::Shared,
            Some(_) => self.keying_of(cred_hex),
        }
    }

    /// The venue's name, or `None` when nothing has registered this
    /// credential.
    ///
    /// ⚠️ `None` is a real answer and must reach the reader as one: an
    /// unnamed venue is a gap in OUR registry, not an absence of trading, and
    /// showing the credential is what lets someone go and identify it.
    pub fn name_of(&self, cred_hex: &str) -> Option<&str> {
        self.names.get(cred_hex).map(String::as_str)
    }

    /// Register a venue's credential, role and name together — the three
    /// facts that must not drift apart.
    pub fn register(&mut self, cred_hex: impl Into<String>, role: Role, venue: &str) {
        let cred = cred_hex.into();
        self.roles.insert(cred.clone(), role);
        self.names.insert(cred, venue.to_string());
    }

    /// Add every entry of `other` this set does not already hold.
    ///
    /// ⚠️ **`self` WINS.** Used to widen a table carried from the settled
    /// archive with the credentials a tail's own observations discovered. A
    /// carried entry was folded from more history than the tail can see, so a
    /// disagreement is not something a read should silently revise.
    pub fn merge_missing(&mut self, other: &Roles) {
        for (cred, role) in &other.roles {
            self.roles.entry(cred.clone()).or_insert(*role);
        }
        for (cred, keying) in &other.keying {
            self.keying.entry(cred.clone()).or_insert(*keying);
        }
        for (cred, name) in &other.names {
            self.names
                .entry(cred.clone())
                .or_insert_with(|| name.clone());
        }
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
    /// One leg of a movement of several legs: `amount` units from one address
    /// to another, stated by the chain. A wallet paying an order and an
    /// unregistered script at once, and a distribution to many wallets, are
    /// the measured cases ([`classify_moves`]).
    Moved {
        from: String,
        to: String,
        amount: i64,
    },
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

    match (from_role, to_role) {
        // order → pool: the swap.
        (Some((Role::Order, cred, stake)), Some((Role::Pool | Role::Curve, _, _))) => Event::Fill {
            party: order_party(roles, &cred, &stake),
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
        // wallet → order: the placement. On a SHARED order address the
        // order's stake is the venue's, so the trader is the wallet the asset
        // left: the one leg that names them (MEASURED: 34 of $PERP's 35 CSwap
        // sells reached the arena as "someone" while this said nobody could
        // tell). A customer-keyed address still names them by its stake.
        (None, Some((Role::Order, cred, stake))) => Event::Placement {
            party: match roles.keying_at(&cred, stake.as_deref()) {
                OrderKeying::Shared => Party::Wallet(from.address.clone()),
                OrderKeying::CustomerStake => order_party(roles, &cred, &stake),
            },
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

/// Every trade one transaction's movement of a unit holds.
///
/// [`classify_move`]'s shapes first, unchanged. Where it finds none, the
/// movement may still be trades the chain states EXACTLY, which
/// [`classify_move`] could not see because it compares addresses, not
/// wallets:
///
/// - **pools and curves paying wallets, and nobody else**: a buy per wallet,
///   of exactly what that wallet received. One wallet through two of its
///   addresses (change), a buy routed across two pools by an aggregator, and
///   a batcher paying several buyers at once all take this shape;
/// - **wallets paying pools and curves, and nobody else**: a sell per wallet,
///   likewise;
/// - **one wallet paying several order contracts** (an aggregator splitting a
///   sell across venues): a placement per order, and a sell for whatever went
///   straight into pools. The reverse, orders paying one wallet back, is a
///   cancellation per order;
/// - **a batch through one pool**: orders spent, wallets paid, the pool
///   settling the difference. A sell per order and a buy per wallet;
/// - **one source paying a venue and others besides**: the trade, and an
///   [`Event::Moved`] for each other recipient;
/// - **no venue, one sender or one receiver by wallet** (a distribution, a
///   collection): an [`Event::Moved`] per leg.
///
/// A wallet is its stake credential, or its address when it has none. This
/// is not the guessing [`Event::BatchedFill`] refuses: there each ORDER's
/// owner is the unknown; here every wallet and its amount are on chain.
/// Anything else (an order leg among several parties, wallets on both sides
/// of a pool) stays [`Event::Transfer`].
///
/// MEASURED on $NikePig: 25,262 movements of the first shape (2,429 M and
/// 2,872 M units) and 333 of the second were outside every stream.
pub fn classify_moves(unit: &UnitMove, roles: &Roles) -> Vec<Event> {
    match classify_move(unit, roles) {
        Event::Transfer => venue_trades(unit, roles)
            .or_else(|| batch_trades(unit, roles))
            .or_else(|| one_source(unit, roles))
            .or_else(|| exact_transfers(unit, roles))
            .unwrap_or_else(|| vec![Event::Transfer]),
        event => vec![event],
    }
}

/// A buy or a sell per wallet, where only pools and curves are on the other
/// side; or, with order contracts among the parties, ONE wallet's placements
/// (or cancellations) across several of them. `None` for any other shape.
fn venue_trades(unit: &UnitMove, roles: &Roles) -> Option<Vec<Event>> {
    if unit.net_mint != 0 {
        return None;
    }
    // Pools and curves by credential; orders by address (each is its own
    // order); wallets by stake. A wallet keeps the address it moved the most
    // through, to stand for it.
    let mut venues: HashMap<String, i64> = HashMap::new();
    let mut orders: HashMap<&str, (String, i64)> = HashMap::new();
    let mut wallets: HashMap<String, (i64, &str, i64)> = HashMap::new();
    for p in &unit.parties {
        let (cred, stake) = address_parts(&p.address)?;
        match roles.role_of(&cred) {
            Some(Role::Order) => {
                orders.entry(p.address.as_str()).or_insert((cred, 0)).1 += p.amount
            }
            Some(Role::Pool | Role::Curve) => *venues.entry(cred).or_default() += p.amount,
            None => {
                let id = stake.unwrap_or_else(|| p.address.clone());
                let w = wallets.entry(id).or_insert((0, p.address.as_str(), 0));
                w.0 += p.amount;
                if p.amount.abs() > w.2 {
                    (w.1, w.2) = (p.address.as_str(), p.amount.abs());
                }
            }
        }
    }
    let venues: Vec<(String, i64)> = venues.into_iter().filter(|(_, a)| *a != 0).collect();
    let mut orders: Vec<(&str, String, i64)> = orders
        .into_iter()
        .filter(|(_, (_, a))| *a != 0)
        .map(|(address, (cred, a))| (address, cred, a))
        .collect();
    let wallets: Vec<(&str, i64)> = wallets
        .into_values()
        .filter(|(a, _, _)| *a != 0)
        .map(|(a, address, _)| (address, a))
        .collect();
    // The venue a trade is credited to: the one that moved the most.
    let venue = |sign: i64| {
        venues
            .iter()
            .filter(|(_, a)| a.signum() == sign)
            .max_by_key(|(c, a)| (a.unsigned_abs(), std::cmp::Reverse(c.clone())))
            .map(|(c, _)| c.clone())
    };
    if !orders.is_empty() {
        // ONE wallet, every order and pool on the other side: an aggregator
        // splitting one sell across several venues' order contracts (or the
        // refunds of such a split). MEASURED on $NikePig: a wallet sending
        // 413,000 units to SundaeSwap V3, Minswap V2 and Minswap V1 orders in
        // one transaction.
        let [(wallet, net)] = wallets.as_slice() else {
            return None;
        };
        let paid = *net < 0;
        let against = |a: i64| match paid {
            true => a > 0,
            false => a < 0,
        };
        if !venues.iter().all(|(_, a)| against(*a)) || !orders.iter().all(|(_, _, a)| against(*a)) {
            return None;
        }
        orders.sort_by(|a, b| {
            b.2.unsigned_abs()
                .cmp(&a.2.unsigned_abs())
                .then(a.0.cmp(b.0))
        });
        let party = || Party::Wallet(wallet.to_string());
        let mut events: Vec<Event> = orders
            .into_iter()
            .map(|(_, cred, a)| match paid {
                true => Event::Placement {
                    venue_cred: cred,
                    party: party(),
                    amount: a.abs(),
                },
                false => Event::Cancellation {
                    venue_cred: cred,
                    party: party(),
                    amount: a.abs(),
                },
            })
            .collect();
        let pooled: i64 = venues.iter().map(|(_, a)| a.abs()).sum();
        if pooled > 0 {
            events.push(Event::Fill {
                venue_cred: venue(if paid { 1 } else { -1 })?,
                party: party(),
                amount: pooled,
                into_pool: paid,
            });
        }
        return Some(events);
    }
    if venues.is_empty() || wallets.is_empty() {
        return None;
    }
    let buys = venues.iter().all(|(_, a)| *a < 0) && wallets.iter().all(|(_, a)| *a > 0);
    let sells = venues.iter().all(|(_, a)| *a > 0) && wallets.iter().all(|(_, a)| *a < 0);
    let (venue_cred, into_pool) = match (buys, sells) {
        (true, _) => (venue(-1)?, false),
        (_, true) => (venue(1)?, true),
        _ => return None,
    };
    let mut events: Vec<Event> = wallets
        .into_iter()
        .map(|(address, amount)| Event::Fill {
            venue_cred: venue_cred.clone(),
            party: Party::Wallet(address.to_string()),
            amount: amount.abs(),
            into_pool,
        })
        .collect();
    // In a stable order: the largest first, then by address.
    events.sort_by(|a, b| match (a, b) {
        (
            Event::Fill {
                amount: x,
                party: Party::Wallet(p),
                ..
            },
            Event::Fill {
                amount: y,
                party: Party::Wallet(q),
                ..
            },
        ) => y.cmp(x).then_with(|| p.cmp(q)),
        _ => std::cmp::Ordering::Equal,
    });
    Some(events)
}

/// One party to a movement, grouped as [`venue_trades`] groups them.
struct Side<'a> {
    net: i64,
    role: Option<Role>,
    cred: String,
    stake: Option<String>,
    /// The address it moved the most through, to stand for it.
    address: &'a str,
    most: i64,
}

/// The order's trader, as the order address names them: its stake, unless the
/// venue shares one address for every order.
fn order_party(roles: &Roles, cred: &str, stake: &Option<String>) -> Party {
    match (roles.keying_at(cred, stake.as_deref()), stake) {
        (OrderKeying::CustomerStake, Some(stake)) => Party::Stake(stake.clone()),
        _ => Party::NotEncodedByVenue,
    }
}

/// A batcher's transaction through ONE pool: orders spent, wallets paid, and
/// the pool taking or giving the difference. Each order is its owner's sell of
/// everything it held; each wallet a buy of exactly what it received. Every leg
/// is on chain; only who traded against whom inside the batch is not, and no
/// event claims it.
///
/// MEASURED on $NikePig, Minswap V2: two orders spending 1,190,000 and 177,549
/// with the pool taking 1,287,268 and a wallet receiving 80,281. A buy order
/// there holds ADA, not the token, so its owner shows only as a wallet paid.
fn batch_trades(unit: &UnitMove, roles: &Roles) -> Option<Vec<Event>> {
    if unit.net_mint != 0 {
        return None;
    }
    let sides = group(unit, roles);
    let mut pools = sides
        .iter()
        .filter(|s| matches!(s.role, Some(Role::Pool | Role::Curve)));
    let (Some(pool), None) = (pools.next(), pools.next()) else {
        return None;
    };
    let (mut orders, mut wallets) = (Vec::new(), Vec::new());
    for s in sides.iter().filter(|s| s.net != 0) {
        match s.role {
            Some(Role::Order) if s.net < 0 => orders.push(s),
            None if s.net > 0 => wallets.push(s),
            Some(Role::Pool | Role::Curve) => {}
            _ => return None,
        }
    }
    if orders.is_empty() || wallets.is_empty() {
        return None;
    }
    let by_size = |a: &&Side, b: &&Side| {
        b.net
            .unsigned_abs()
            .cmp(&a.net.unsigned_abs())
            .then(a.address.cmp(b.address))
    };
    orders.sort_by(by_size);
    wallets.sort_by(by_size);
    let sells = orders.into_iter().map(|o| Event::Fill {
        venue_cred: o.cred.clone(),
        party: order_party(roles, &o.cred, &o.stake),
        amount: -o.net,
        into_pool: true,
    });
    let buys = wallets.into_iter().map(|w| Event::Fill {
        venue_cred: pool.cred.clone(),
        party: Party::Wallet(w.address.to_string()),
        amount: w.net,
        into_pool: false,
    });
    Some(sells.chain(buys).collect())
}

/// ONE source paying at least one venue, and others besides: the trade (a
/// placement or a sell from a wallet, a fill from an order), and a
/// [`Event::Moved`] for each other recipient.
///
/// MEASURED on $NikePig: a wallet's 161,765 going to a CSwap order with 14,141
/// to an unregistered script in the same transaction. Every leg is on chain,
/// so nothing here is guessed: the venue leg is the trade, the rest moved.
fn one_source(unit: &UnitMove, roles: &Roles) -> Option<Vec<Event>> {
    if unit.net_mint != 0 {
        return None;
    }
    let sides = group(unit, roles);
    let losers: Vec<&Side> = sides.iter().filter(|s| s.net < 0).collect();
    let [source] = losers.as_slice() else {
        return None;
    };
    let mut gainers: Vec<&Side> = sides.iter().filter(|s| s.net > 0).collect();
    gainers.sort_by(|a, b| b.net.cmp(&a.net).then(a.address.cmp(b.address)));
    if !gainers.iter().any(|g| g.role.is_some()) {
        return None;
    }
    let (source_role, source_cred, source_stake) = (source.role, &source.cred, &source.stake);
    let source = source.address;
    let mut events = Vec::new();
    for Side {
        net: amount,
        role,
        cred,
        stake,
        address,
        ..
    } in gainers
    {
        let event = match (source_role, role) {
            // A wallet: into an order is a placement, named as
            // `classify_move` names it; into a pool, a sell.
            (None, Some(Role::Order)) => Event::Placement {
                venue_cred: (*cred).clone(),
                party: match roles.keying_at(cred, stake.as_deref()) {
                    OrderKeying::Shared => Party::Wallet(source.to_string()),
                    OrderKeying::CustomerStake => order_party(roles, cred, stake),
                },
                amount: *amount,
            },
            (None, Some(Role::Pool | Role::Curve)) => Event::Fill {
                venue_cred: (*cred).clone(),
                party: Party::Wallet(source.to_string()),
                amount: *amount,
                into_pool: true,
            },
            // An order into a pool: the fill, named as the order names it.
            (Some(Role::Order), Some(Role::Pool | Role::Curve)) => Event::Fill {
                party: order_party(roles, source_cred, source_stake),
                venue_cred: source_cred.clone(),
                amount: *amount,
                into_pool: true,
            },
            // Anyone else who received: moved, not traded.
            (_, None) => Event::Moved {
                from: source.to_string(),
                to: address.to_string(),
                amount: *amount,
            },
            // A pool paying out, or order to order: another shape entirely.
            _ => return None,
        };
        events.push(event);
    }
    Some(events)
}

/// No venue among the parties, and ONE sender or ONE receiver once addresses
/// are grouped into wallets: a [`Event::Moved`] per leg, every amount stated
/// by the chain.
///
/// Only where the feed itself could not name a direction
/// ([`crate::feed::Direction::Ambiguous`]): a plain one-to-one movement keeps
/// its own path, `SourceBelowFloor` included. Several senders AND several
/// receivers stay a `Transfer`, because pairing them would be a guess.
///
/// MEASURED on $NikePig: 2,927 distributions (one sender, up to 40
/// receivers; a reward script's 1,368 among them), 1,441 collections and
/// 1,173 movements between one wallet's addresses and another's, all of it
/// previously dropped as ambiguous.
fn exact_transfers(unit: &UnitMove, roles: &Roles) -> Option<Vec<Event>> {
    if unit.net_mint != 0
        || !matches!(
            crate::feed::direction(unit),
            crate::feed::Direction::Ambiguous
        )
    {
        return None;
    }
    let sides = group(unit, roles);
    if sides.iter().any(|s| s.role.is_some()) {
        return None;
    }
    let by_size = |a: &&Side, b: &&Side| {
        b.net
            .unsigned_abs()
            .cmp(&a.net.unsigned_abs())
            .then(a.address.cmp(b.address))
    };
    let mut losers: Vec<&Side> = sides.iter().filter(|s| s.net < 0).collect();
    let mut gainers: Vec<&Side> = sides.iter().filter(|s| s.net > 0).collect();
    losers.sort_by(by_size);
    gainers.sort_by(by_size);
    let moved = |from: &Side, to: &Side, amount: i64| Event::Moved {
        from: from.address.to_string(),
        to: to.address.to_string(),
        amount,
    };
    match (losers.as_slice(), gainers.as_slice()) {
        // Every wallet nets to nothing: one wallet moving its own between
        // its addresses. Nothing changed hands, so there is nothing to say.
        // MEASURED on $NikePig: 406 of these were reported as ambiguous.
        ([], []) => Some(Vec::new()),
        ([from], to) if !to.is_empty() => Some(to.iter().map(|t| moved(from, t, t.net)).collect()),
        (from, [to]) if !from.is_empty() => {
            Some(from.iter().map(|f| moved(f, to, -f.net)).collect())
        }
        _ => None,
    }
}

/// Every party to a movement: pools and curves by credential, orders by
/// address (each is its own order), wallets by stake.
///
/// An address that does not decode (a Byron one) is a wallet of its own, by
/// its literal address: it cannot be a venue, and failing the whole movement
/// over it left distributions from Byron wallets unread (MEASURED on
/// $NikePig, about 40).
fn group<'a>(unit: &'a UnitMove, roles: &Roles) -> Vec<Side<'a>> {
    let mut sides: HashMap<String, Side> = HashMap::new();
    for p in &unit.parties {
        let (cred, stake) = address_parts(&p.address).unwrap_or_default();
        let role = roles.role_of(&cred);
        let id = match role {
            Some(Role::Order) => p.address.clone(),
            Some(_) => cred.clone(),
            None => stake.clone().unwrap_or_else(|| p.address.clone()),
        };
        let s = sides.entry(id).or_insert(Side {
            net: 0,
            role,
            cred,
            stake,
            address: &p.address,
            most: 0,
        });
        s.net += p.amount;
        if p.amount.abs() > s.most {
            (s.address, s.most) = (&p.address, p.amount.abs());
        }
    }
    // In a stable order, so nothing downstream depends on hashing.
    let mut sides: Vec<Side> = sides.into_values().collect();
    sides.sort_by(|a, b| a.address.cmp(b.address));
    sides
}

/// Fold a feed page: every event each `(transaction, unit)` holds, usually
/// one ([`classify_moves`]).
pub fn fold(rows: &[FeedRow], roles: &Roles) -> Vec<Folded> {
    rows.iter()
        .flat_map(|r| {
            r.units.iter().flat_map(move |u| {
                classify_moves(u, roles)
                    .into_iter()
                    .map(move |event| Folded {
                        tx_hash: r.tx_hash.clone(),
                        slot: r.slot,
                        block_time: r.block_time,
                        unit: u.name.clone(),
                        event,
                    })
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

    /// A wallet address with payment credential `[pay; 28]` and stake
    /// credential `[stake; 28]`. The checksum is a placeholder: nothing here
    /// verifies one.
    fn wallet(pay: u8, stake: u8) -> String {
        const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
        let mut bytes = vec![0x01];
        bytes.extend([pay; 28]);
        bytes.extend([stake; 28]);
        let (mut acc, mut bits, mut out) = (0u32, 0u32, String::from("addr1"));
        for b in bytes {
            acc = (acc << 8) | u32::from(b);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(CHARSET[((acc >> bits) & 31) as usize] as char);
            }
        }
        if bits > 0 {
            out.push(CHARSET[((acc << (5 - bits)) & 31) as usize] as char);
        }
        out + "qqqqqq"
    }

    fn fills(events: &[Event]) -> Vec<(String, i64, bool)> {
        events
            .iter()
            .map(|e| match e {
                Event::Fill {
                    party: Party::Wallet(w),
                    amount,
                    into_pool,
                    ..
                } => (w.clone(), *amount, *into_pool),
                other => panic!("expected a wallet's fill, got {other:?}"),
            })
            .collect()
    }

    /// A pool paying one wallet through two of its addresses is ONE buy.
    #[test]
    fn a_wallet_paid_at_two_addresses_is_one_buy() {
        let (a, b) = (wallet(1, 9), wallet(2, 9));
        let e = classify_moves(&mv(&[(SPLASH_POOL, -700), (&a, 500), (&b, 200)]), &roles());
        assert_eq!(fills(&e), vec![(a, 700, false)]);
        assert!(
            matches!(
                classify_move(
                    &mv(&[
                        (SPLASH_POOL, -700),
                        (&wallet(1, 9), 500),
                        (&wallet(2, 9), 200)
                    ]),
                    &roles()
                ),
                Event::Transfer
            ),
            "the single-shape fold alone cannot see it"
        );
    }

    /// Two pools paying one wallet (a routed buy) is one buy, credited to the
    /// pool that paid the most.
    #[test]
    fn a_routed_buy_is_one_buy_at_the_larger_pool() {
        let w = wallet(3, 3);
        let e = classify_moves(
            &mv(&[(SPLASH_POOL, -300), (CSWAP_POOL, -900), (&w, 1_200)]),
            &roles(),
        );
        assert_eq!(fills(&e), vec![(w, 1_200, false)]);
        let Event::Fill { venue_cred, .. } = &e[0] else {
            unreachable!()
        };
        assert_eq!(venue_cred, &address_parts(CSWAP_POOL).unwrap().0);
    }

    /// A pool paying several wallets at once is a buy each, of exactly what
    /// each received: the largest first.
    #[test]
    fn a_pool_paying_several_wallets_is_a_buy_each() {
        let (a, b, c) = (wallet(4, 4), wallet(5, 5), wallet(6, 6));
        let e = classify_moves(
            &mv(&[(SPLASH_POOL, -1_000), (&a, 100), (&b, 600), (&c, 300)]),
            &roles(),
        );
        assert_eq!(
            fills(&e),
            vec![(b, 600, false), (c, 300, false), (a, 100, false)]
        );
    }

    /// Several wallets paying a pool at once is a sell each.
    #[test]
    fn wallets_paying_a_pool_are_a_sell_each() {
        let (a, b) = (wallet(7, 7), wallet(8, 8));
        let e = classify_moves(&mv(&[(&a, -400), (&b, -100), (SPLASH_POOL, 500)]), &roles());
        assert_eq!(fills(&e), vec![(a, 400, true), (b, 100, true)]);
    }

    /// An aggregator splitting one sell across two venues' order contracts and
    /// a pool: a placement per order, the largest first, and a sell for what
    /// went straight into the pool.
    #[test]
    fn a_split_sell_is_a_placement_per_order() {
        let w = wallet(20, 20);
        let e = classify_moves(
            &mv(&[
                (&w, -1_000),
                (SPLASH_ORDER_A, 200),
                (CSWAP_ORDER, 500),
                (SPLASH_POOL, 300),
            ]),
            &roles(),
        );
        let cred = |a: &str| address_parts(a).unwrap().0;
        assert_eq!(
            e,
            vec![
                Event::Placement {
                    venue_cred: cred(CSWAP_ORDER),
                    party: Party::Wallet(w.clone()),
                    amount: 500,
                },
                Event::Placement {
                    venue_cred: cred(SPLASH_ORDER_A),
                    party: Party::Wallet(w.clone()),
                    amount: 200,
                },
                Event::Fill {
                    venue_cred: cred(SPLASH_POOL),
                    party: Party::Wallet(w.clone()),
                    amount: 300,
                    into_pool: true,
                },
            ]
        );
        // And its refunds: orders paying the one wallet back.
        let back = classify_moves(
            &mv(&[(SPLASH_ORDER_A, -200), (CSWAP_ORDER, -500), (&w, 700)]),
            &roles(),
        );
        assert!(
            back.iter().all(
                |e| matches!(e, Event::Cancellation { party: Party::Wallet(p), .. } if p == &w)
            ),
            "{back:?}"
        );
        assert_eq!(back.len(), 2);
    }

    /// A batch through one pool: an order spent, the pool taking the most of
    /// it, a wallet paid the rest. A sell of everything the order held, named
    /// as the order names it, and a buy of what the wallet received.
    #[test]
    fn a_batch_through_one_pool_is_a_sell_per_order_and_a_buy_per_wallet() {
        let buyer = wallet(30, 30);
        let e = classify_moves(
            &mv(&[
                (SPLASH_ORDER_A, -1_605_216),
                (SPLASH_POOL, 1_448_456),
                (&buyer, 156_760),
            ]),
            &roles(),
        );
        let cred = |a: &str| address_parts(a).unwrap().0;
        let order_stake = address_parts(SPLASH_ORDER_A).unwrap().1;
        assert_eq!(
            e,
            vec![
                Event::Fill {
                    venue_cred: cred(SPLASH_ORDER_A),
                    party: order_stake.map_or(Party::Ambiguous, Party::Stake),
                    amount: 1_605_216,
                    into_pool: true,
                },
                Event::Fill {
                    venue_cred: cred(SPLASH_POOL),
                    party: Party::Wallet(buyer),
                    amount: 156_760,
                    into_pool: false,
                },
            ]
        );
        // The pool paying out instead, to two wallets: the same reading.
        let (a, b) = (wallet(34, 34), wallet(35, 35));
        let e = classify_moves(
            &mv(&[
                (SPLASH_ORDER_A, -100),
                (SPLASH_POOL, -400),
                (&a, 300),
                (&b, 200),
            ]),
            &roles(),
        );
        assert_eq!(e.len(), 3, "{e:?}");
        assert_eq!(fills(&e[1..]), vec![(a, 300, false), (b, 200, false)]);
    }

    /// A wallet paying a shared order contract with a fee to an unregistered
    /// script, from two of its addresses: one placement, one movement.
    #[test]
    fn a_placement_with_a_fee_is_a_placement_and_a_move() {
        let (w1, w2, fee) = (wallet(31, 31), wallet(32, 31), wallet(33, 33));
        let e = classify_moves(
            &mv(&[
                (&w1, -150_000),
                (&w2, -25_906),
                (CSWAP_ORDER, 161_765),
                (&fee, 14_141),
            ]),
            &roles(),
        );
        let cred = |a: &str| address_parts(a).unwrap().0;
        assert_eq!(
            e,
            vec![
                Event::Placement {
                    venue_cred: cred(CSWAP_ORDER),
                    party: Party::Wallet(w1.clone()),
                    amount: 161_765,
                },
                Event::Moved {
                    from: w1,
                    to: fee,
                    amount: 14_141,
                },
            ]
        );
    }

    /// A customer-keyed order contract with a stake-less enterprise form too
    /// (WingRiders, both versions): the staked form names its customer, and
    /// the enterprise form reads as shared, naming the placing wallet and
    /// leaving the fill to the placement. Real addresses from $NikePig.
    #[test]
    fn a_stake_less_order_address_reads_as_shared() {
        const ENTERPRISE: &str = "addr1wxr2a8htmzuhj39y2gq7ftkpxv98y2g67tg8zezthgq4jkg0a4ul4";
        const STAKED: &str = "addr1zxr2a8htmzuhj39y2gq7ftkpxv98y2g67tg8zezthgq4jkw7snnjm8fjxg2nt3lhnqj8veegcyqq5tc9pdtuvw4n7r2qxnt66c";
        const POOL: &str = "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaydrn6a23ngn97d9j7cwmx0rq5mxggqvsggsnastu77f5y2sn8zx04";
        let mut r = Roles::default();
        r.roles
            .insert(address_parts(ENTERPRISE).unwrap().0, Role::Order);
        r.roles.insert(address_parts(POOL).unwrap().0, Role::Pool);
        let w = wallet(50, 50);
        let party = |e: Event| match e {
            Event::Fill { party, .. } | Event::Placement { party, .. } => party,
            other => panic!("expected a fill or a placement, got {other:?}"),
        };
        assert_eq!(
            party(classify_move(&mv(&[(&w, -10), (ENTERPRISE, 10)]), &r)),
            Party::Wallet(w.clone())
        );
        assert_eq!(
            party(classify_move(&mv(&[(ENTERPRISE, -10), (POOL, 10)]), &r)),
            Party::NotEncodedByVenue
        );
        let customer = address_parts(STAKED).unwrap().1.unwrap();
        assert_eq!(
            party(classify_move(&mv(&[(STAKED, -10), (POOL, 10)]), &r)),
            Party::Stake(customer.clone())
        );
        assert_eq!(
            party(classify_move(&mv(&[(&w, -10), (STAKED, 10)]), &r)),
            Party::Stake(customer)
        );
    }

    /// One sender to several wallets, and several to one: a movement per leg,
    /// the largest first. One wallet's two addresses still count as one.
    #[test]
    fn distributions_and_collections_are_a_move_per_leg() {
        let (a, b, c, c2) = (
            wallet(40, 40),
            wallet(41, 41),
            wallet(42, 42),
            wallet(43, 42),
        );
        let moved = |from: &str, to: &str, amount| Event::Moved {
            from: from.to_string(),
            to: to.to_string(),
            amount,
        };
        assert_eq!(
            classify_moves(
                &mv(&[(&a, -500), (&b, 200), (&c, 250), (&c2, 50)]),
                &roles()
            ),
            vec![moved(&a, &c, 300), moved(&a, &b, 200)]
        );
        assert_eq!(
            classify_moves(&mv(&[(&a, -100), (&b, -400), (&c, 500)]), &roles()),
            vec![moved(&b, &c, 400), moved(&a, &c, 100)]
        );
        // A Byron sender distributes like any other wallet.
        const BYRON: &str = "DdzFFzCqrhsrcTVhLygT24QwTnNqQqQ8mZrq5jykUzMveU26sxaH529kMpo7VhPrt5pwW3dXeB2k3EEvKcNBRmzCfcQ7dTkyGzTs658C";
        assert_eq!(
            classify_moves(&mv(&[(BYRON, -300), (&b, 200), (&c, 100)]), &roles()),
            vec![moved(BYRON, &b, 200), moved(BYRON, &c, 100)]
        );
        // One wallet moving its own between its addresses: nothing to say.
        assert_eq!(
            classify_moves(&mv(&[(&c, -300), (&c2, 200), (&c, 100)]), &roles()),
            vec![]
        );
        // A plain one-to-one keeps the feed's own reading.
        assert_eq!(
            classify_moves(&mv(&[(&a, -100), (&b, 100)]), &roles()),
            vec![Event::Transfer]
        );
    }

    /// Still not guessed: wallets on both sides of a pool, an order among
    /// several WALLETS, two orders feeding each other, and several wallets on
    /// both sides.
    #[test]
    fn other_multi_party_shapes_stay_transfers() {
        let (a, b, c) = (wallet(10, 10), wallet(11, 11), wallet(12, 12));
        let d = wallet(13, 13);
        for parties in [
            vec![(SPLASH_POOL, -500), (a.as_str(), -100), (b.as_str(), 600)],
            vec![
                (SPLASH_ORDER_A, -500),
                (CSWAP_ORDER, 300),
                (b.as_str(), 200),
            ],
            vec![
                (a.as_str(), -300),
                (b.as_str(), -200),
                (SPLASH_ORDER_A, 500),
            ],
            vec![
                (a.as_str(), -500),
                (d.as_str(), -100),
                (b.as_str(), 400),
                (c.as_str(), 200),
            ],
        ] {
            assert_eq!(
                classify_moves(&mv(&parties), &roles()),
                vec![Event::Transfer]
            );
        }
    }

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

    /// CSwap shares ONE order address, so the trader is genuinely not in the
    /// FILL. It must say so rather than attribute every CSwap trade to the
    /// same stake.
    #[test]
    fn a_shared_order_address_refuses_to_name_a_trader_in_the_fill() {
        let e = classify_move(&mv(&[(CSWAP_ORDER, -1_000), (CSWAP_POOL, 1_000)]), &roles());
        match e {
            Event::Fill { party, .. } => assert_eq!(party, Party::NotEncodedByVenue),
            other => panic!("expected a Fill, got {other:?}"),
        }
    }

    /// …and the PLACEMENT is where they are: the wallet the asset left. This
    /// said "not encoded" too, so a CSwap sell had no trader anywhere.
    #[test]
    fn a_shared_order_address_names_the_trader_in_the_placement() {
        match classify_move(&mv(&[(WALLET, -1_000), (CSWAP_ORDER, 1_000)]), &roles()) {
            Event::Placement { party, amount, .. } => {
                assert_eq!(party, Party::Wallet(WALLET.to_string()));
                assert_eq!(amount, 1_000);
            }
            other => panic!("expected a Placement, got {other:?}"),
        }
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
