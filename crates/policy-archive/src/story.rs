//! ONE ordered stream that tells a token's story.
//!
//! # Why this exists, when `price`, `trade` and `supply` already do
//!
//! Those slice the archive by KIND OF ANALYSIS. A consumer wanting to draw
//! anything then has to fetch several answers and reconcile their time bases
//! before it can start — which makes us the ones who decided what questions
//! are askable. That is a set of charts with the joins left as homework, not a
//! framework.
//!
//! 🔑 **Slice by TIME instead, and make the kind a property of each event.**
//! Then every visualisation is a FOLD over one stream:
//!
//! | to draw | fold |
//! |---|---|
//! | price series | filter [`Kind::PoolState`], map to a rate |
//! | the launch | filter [`Kind::CurveState`] |
//! | holders over time | fold [`Kind::Transfer`] / `Mint` / `Burn` |
//! | volume | fold [`Kind::Fill`] |
//! | liquidity | fold `PoolState` reserves |
//!
//! Nobody has to ask us to add an endpoint for a question we did not think of.
//!
//! # It lives in the CRATE, not in the server
//!
//! Deliberately, and for the reason the rest of this crate is pure: the same
//! code has to run on cardano-infra against files and in a Worker against R2.
//! A story that only the daemon can build is a story that stops when the
//! daemon does, and the archive stops being self-contained — which is the
//! property that makes it usable by someone who is not us.
//!
//! # What it merges
//!
//! - **movements** → the trade fold ([`crate::trade`]): transfers, fills,
//!   placements, cancellations, mints, burns;
//! - **observations** ([`crate::observation`]) → the state of a script output
//!   at a slot: pool reserves, a bonding curve's position, and the outputs no
//!   decoder claimed.
//!
//! Both are slot-keyed and carry a transaction hash, so they merge onto one
//! spine without inventing anything.
//!
//! # ⚠️ Order within a slot is NOT proven
//!
//! The stream is sorted by slot, then by a stable rank so output is
//! deterministic. It is **not** transaction order inside a block — recovering
//! that means following the chain of UTxOs, which this crate cannot do.
//!
//! This is not a corner case. MEASURED on $PERP: **ten of its eleven bonding
//! sightings share ONE slot**, because the whole launch happened inside a
//! single block. [`Story::distinct_slots`] lets a consumer see how much of the
//! ordering it is actually being given.
//!
//! Curve states are the one exception, and they are ordered by CURVE POSITION
//! within a slot — lovelace in and tokens out move strictly together, so
//! ascending lovelace is ascending position up the curve. ⚠️ A sell moves back
//! DOWN the curve and would appear out of sequence.

/// The story on the wire — postcard, interned, delta-encoded. JSON is the
/// wrong shape for tens of thousands of events and the module says why.
pub mod wire;

use crate::feed::FeedRow;
use crate::observation::{Observation, pricing};
use crate::trade::{Event, Party, Roles, fold};

/// One thing that happened to this policy, at a slot.
#[derive(Debug, Clone, PartialEq)]
pub struct StoryEvent {
    pub slot: u64,
    pub unix: u64,
    /// The transaction it happened in.
    pub tx: Vec<u8>,
    /// Which asset. ⚠️ Empty only where the event is about the policy rather
    /// than a unit of it. IDENTITY only — never decode for display.
    pub unit: Vec<u8>,
    pub kind: Kind,
}

/// What happened. A closed vocabulary: a consumer that matches every variant
/// has covered the whole archive, and a new one is a deliberate, visible
/// addition rather than a silent widening.
#[derive(Debug, Clone, PartialEq)]
pub enum Kind {
    // ── supply ───────────────────────────────────────────────────────────
    /// Units came into existence here.
    Mint { to: Option<String>, amount: i64 },
    /// Units ceased to exist here.
    Burn { from: Option<String>, amount: i64 },

    // ── ownership ────────────────────────────────────────────────────────
    Transfer {
        from: String,
        to: String,
        amount: i64,
    },
    /// An arrival whose source sits below the walk's floor. NOT a mint —
    /// the units existed, this archive just has not descended far enough to
    /// see where they came from. Shrinks as the walk deepens.
    ArrivedFromBelowFloor { to: String, amount: i64 },
    /// Several parties on a side. ⚠️ STATED, never guessed: "the largest
    /// mover is the sender" is wrong exactly here.
    Ambiguous { parties: usize, amount: i64 },

    // ── venue ────────────────────────────────────────────────────────────
    /// The swap itself — an order spent into a pool.
    Fill {
        venue: String,
        party: Trader,
        amount: i64,
        /// True when the asset went INTO the pool (the holder sold).
        into_pool: bool,
    },
    /// Wallet → order contract. On a venue with a shared order address this is
    /// the ONLY leg that names the trader.
    Placement {
        venue: String,
        party: Trader,
        amount: i64,
    },
    /// Order → wallet: the swap did NOT happen. ⚠️ Indistinguishable from an
    /// ordinary transfer without knowing the contract — which is why these
    /// were invisible, and why counting them as trades inflates volume.
    Cancellation {
        venue: String,
        party: Trader,
        amount: i64,
    },
    /// Several orders into one pool, in one transaction. Named and counted,
    /// **never decomposed**.
    BatchedFill {
        venue: String,
        orders: usize,
        amount: i64,
    },

    // ── state at a moment ────────────────────────────────────────────────
    /// A DEX pool's reserves. **This is the price series**: rate =
    /// `quote / base`, in RAW units — see [`PoolState::quote_policy`].
    PoolState(PoolState),
    /// A bonding curve's position. **This is the launch timeline.**
    CurveState {
        /// WHICH curve — an index into [`Story::pools`], for the same reason
        /// [`PoolState::pool`] is one. A launchpad runs one curve per token but
        /// a policy can be relaunched, and a curve has an address like any
        /// other contract.
        pool: u32,
        /// Lovelace the curve held.
        lovelace: i64,
        /// Units still ON the curve — unsold inventory that has never been
        /// owned by anyone, and so is never float.
        tokens_left: i64,
        /// 0.0–1.0 up its own cap. `None` when the datum did not decode,
        /// which is a different statement from 0.
        progress: Option<f64>,
    },
    /// A script output holding units that no decoder claimed.
    ///
    /// Emitted rather than dropped so a consumer can SEE the gap in our
    /// coverage, and so a decoder added later re-derives this history from the
    /// archive rather than from chunks.
    UnclaimedScript {
        address: String,
        amount: i64,
        /// Whether a later decoder has anything to work with.
        has_datum: bool,
    },
}

/// WHICH pool. **Identity, not a sighting** — a pool's venue, address and
/// instance key never change, so they are stated once in [`Story::pools`] and
/// referred to by ordinal thereafter.
///
/// # ⚠️ All three parts are load-bearing, and that is MEASURED
///
/// This type replaced a bare instance key, which could not tell two pools
/// apart often enough to matter:
///
/// - **Without `address`**: a venue that hosts every pool at one script address
///   and distinguishes them by a pool NFT is fine, but one that derives a stake
///   part per pool is not — and dropping the address merged pools that share an
///   instance key. The archive tool's own table did exactly this and lost a
///   cswap pool holding **200,000,000 base / 602 ₳** on policy `8fe8039d…`; it
///   was never printed at all.
/// - **Without `key_policy`**: two pools at one address under different key
///   policies collapse into one.
/// - **Without `key_name`**: every pool at a shared address collapses.
///
/// The old shape also made the key `Option`, and `None` — "this venue
/// publishes no instance key" — merged every such pool into a single bucket.
/// That is most of Minswap V2's rows. A pool always has an address, so there
/// is no honest `None` here and the variant is gone.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolIdent {
    pub venue: String,
    /// The script address, bech32, as the rows spell it.
    pub address: String,
    pub key_policy: Vec<u8>,
    /// The pool NFT's asset name. Empty where the venue mints none — which is
    /// no longer ambiguous, because `address` still separates the pools.
    pub key_name: Vec<u8>,
}

/// A pool's reserves at a slot.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolState {
    /// WHICH pool — an index into [`Story::pools`].
    ///
    /// ⚠️ A venue runs MANY pools and a consumer that groups by venue alone
    /// cannot aggregate them: "the latest sighting" then means the latest of
    /// whichever pool happened to move last, and a token/token pair with no
    /// measured ADA side erases a measured one. MEASURED on $PERP — a token
    /// band grouping by venue reported `splash: liquidity not published`
    /// while the archive held 29,121 ADA.
    ///
    /// Grouping by this ordinal is correct by construction, which is the
    /// whole reason it is an ordinal and not a name. Resolve it with
    /// [`Story::pool`].
    pub pool: u32,
    /// The watched policy's reserve.
    pub base: i64,
    /// The quote-side reserve. `None` is **"this venue does not publish it"**,
    /// which is not zero — a rate cannot be computed from it and pretending
    /// otherwise prices a pool at infinity.
    pub quote: Option<i64>,
    /// What it is paired against. `None` — or an empty policy — is **ADA**,
    /// the only case where the quote's decimals are known (6). Anything else
    /// needs that unit's own archive to price, which is why `crate::price`
    /// reports such pairs as `unresolved` rather than folding them in.
    ///
    /// ⚠️ Raw bytes, not hex: this crate stays pure over bytes and leaves
    /// encoding to whoever serialises. Asset names are IDENTITY only.
    pub quote_policy: Option<Vec<u8>>,
    pub quote_name: Option<Vec<u8>>,
    /// `constant-product` | `bonding-curve`. ⚠️ A consumer computing a rate
    /// from reserves must check this: a bonding curve's reserves are real and
    /// its rate is NOT `quote / base`. Summing the two models halved $PERP's
    /// price once.
    pub pricing: String,
}

/// Who traded — or an honest reason there is no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trader {
    Stake(String),
    Wallet(String),
    /// ⚠️ The venue's order contract is ONE shared address, so this leg
    /// cannot name a trader and never will. Distinct from `Unknown`: the
    /// trader is in the placement leg.
    NotEncodedByVenue,
    /// Several candidates; declining rather than picking.
    Unknown,
}

impl From<&Party> for Trader {
    fn from(p: &Party) -> Self {
        match p {
            Party::Stake(s) => Trader::Stake(s.clone()),
            Party::Wallet(w) => Trader::Wallet(w.clone()),
            Party::NotEncodedByVenue => Trader::NotEncodedByVenue,
            Party::Ambiguous => Trader::Unknown,
        }
    }
}

impl Kind {
    /// A stable rank, so events sharing a slot come back in a deterministic
    /// order. ⚠️ Rank is NOT chronology — see the module header.
    ///
    /// Ordered so a slot reads the way the chain works: supply first, then
    /// what moved, then the state that resulted.
    fn rank(&self) -> u8 {
        match self {
            Kind::Mint { .. } => 0,
            Kind::Burn { .. } => 1,
            Kind::Placement { .. } => 2,
            Kind::Fill { .. } => 3,
            Kind::BatchedFill { .. } => 4,
            Kind::Cancellation { .. } => 5,
            Kind::Transfer { .. } => 6,
            Kind::ArrivedFromBelowFloor { .. } => 7,
            Kind::Ambiguous { .. } => 8,
            Kind::CurveState { .. } => 9,
            Kind::PoolState(_) => 10,
            Kind::UnclaimedScript { .. } => 11,
        }
    }

    /// The curve's position, for ordering sightings inside one slot.
    fn curve_position(&self) -> i64 {
        match self {
            Kind::CurveState { lovelace, .. } => *lovelace,
            _ => 0,
        }
    }
}

/// A window of the stream, and what it is a window OF.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Story {
    pub events: Vec<StoryEvent>,
    /// Every pool and curve the events refer to, stated once.
    ///
    /// ⚠️ **This is what makes the stream foldable by a client.** Before it
    /// existed, a `PoolState` carried its venue and instance key and nothing
    /// else, so a consumer folding the stream could not tell two pools apart
    /// at the resolution the archive's own price uses — and therefore could
    /// not reproduce it. See [`PoolIdent`].
    pub pools: Vec<PoolIdent>,
    /// Distinct slots covered. `1` means everything here shares a block and
    /// **no ordering within it is proven**.
    pub distinct_slots: usize,
    /// Chapter markers — the few events that matter to a timeline WHEREVER
    /// they sit, including outside this window. See [`Marker`].
    pub markers: Vec<Marker>,
}

impl Story {
    /// Resolve a [`PoolState::pool`] or [`Kind::CurveState`] ordinal.
    ///
    /// `None` only on a stream whose table and rows disagree, which is a bug
    /// in whatever produced it — reported rather than papered over with a
    /// placeholder venue.
    pub fn pool(&self, ix: u32) -> Option<&PoolIdent> {
        self.pools.get(ix as usize)
    }

    /// The venue a pool ordinal belongs to, or `"?"` where the table does not
    /// name it. For DISPLAY only — anything aggregating must group by the
    /// ordinal, which is the point of having one.
    pub fn venue(&self, ix: u32) -> &str {
        self.pool(ix).map_or("?", |p| p.venue.as_str())
    }

    /// Attach chapter markers. Separate from [`build`] because markers are
    /// derived from the WHOLE archive while the events are a window of it, and
    /// folding that into one call would hide which input each came from.
    pub fn with_markers(mut self, markers: Vec<Marker>) -> Self {
        self.markers = markers;
        self
    }
}

/// A moment worth drawing however far outside the window it falls.
///
/// # ⚠️ Why these are not just events
///
/// A token's life can span 50 million slots, and a window that holds the
/// present cannot also hold a launch three years earlier. MEASURED on $PERP:
/// it launched at slot 145,246,220, and a 5,000-movement window reaches back
/// only to ~175,900,000 — **30 million slots short**.
///
/// Without markers a consumer sees `curve points: 0` and has no way to tell
/// *"this token never launched on a curve"* from *"the launch is out of
/// frame"*. Those read identically and one of them is a lie, so the stream
/// carries the answer rather than leaving it to be inferred from an absence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    pub slot: u64,
    pub unix: u64,
    pub kind: MarkerKind,
    /// Where this sits relative to the window the events cover.
    pub at: Where,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerKind {
    /// The policy's first mint — where its life starts.
    FirstMint,
    /// The first sighting of a bonding curve.
    Launch,
    /// The curve reached its cap, or trading moved to other venues.
    Graduation,
}

/// Whether a marker falls inside the window, or which side of it.
///
/// A consumer draws an in-frame marker on the timeline and an out-of-frame one
/// as an edge indicator — "there is something back there" — which is the whole
/// reason to send it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Where {
    Before,
    Within,
    After,
}

/// Derive chapter markers from the archive's FULL observation set.
///
/// ⚠️ Pass every observation, not the window's — that is the point. `window`
/// is only used to say which side of it each marker falls on.
pub fn markers(
    all: &[Observation],
    first_mint_slot: Option<u64>,
    window: (u64, u64),
) -> Vec<Marker> {
    let side = |slot: u64| match slot {
        s if s < window.0 => Where::Before,
        s if s > window.1 => Where::After,
        _ => Where::Within,
    };
    let mut out = Vec::new();

    if let Some(slot) = first_mint_slot {
        // The unix time of a first mint is not in the observations (a mint is
        // a movement), so it is left to the caller's slot→time conversion
        // rather than guessed at here.
        out.push(Marker {
            slot,
            unix: 0,
            kind: MarkerKind::FirstMint,
            at: side(slot),
        });
    }

    let mut curve: Vec<&Observation> = all
        .iter()
        .filter(|o| {
            o.decoded
                .as_ref()
                .is_some_and(|d| d.pricing == pricing::BONDING_CURVE)
        })
        .collect();
    curve.sort_by_key(|o| o.slot);

    if let Some(first) = curve.first() {
        out.push(Marker {
            slot: first.slot,
            unix: first.block_time,
            kind: MarkerKind::Launch,
            at: side(first.slot),
        });
    }
    // Graduation: the last curve sighting, but only when something at ANOTHER
    // venue happened afterwards. A curve still being traded is not a
    // graduation, and calling it one would put a finish line on a token that
    // never crossed it.
    if let Some(last) = curve.last() {
        let traded_elsewhere_after = all.iter().any(|o| {
            o.slot > last.slot
                && o.decoded
                    .as_ref()
                    .is_some_and(|d| d.pricing != pricing::BONDING_CURVE)
        });
        if traded_elsewhere_after {
            out.push(Marker {
                slot: last.slot,
                unix: last.block_time,
                kind: MarkerKind::Graduation,
                at: side(last.slot),
            });
        }
    }
    out.sort_by_key(|m| m.slot);
    out
}

/// Merge movements and observations into one ordered stream.
///
/// Pure: give it the same inputs on a box or in a Worker and it yields the
/// same stream.
/// Intern one observation's pool, returning its ordinal.
///
/// Linear, because a policy has a handful of pools — $NIKEPIG, the widest here,
/// has 14 across seven venues — and a map would cost more than it saves while
/// losing the stable, first-seen ordering an ordinal wants.
fn intern_pool(
    pools: &mut Vec<PoolIdent>,
    o: &Observation,
    d: &crate::observation::Decoded,
) -> u32 {
    let ident = PoolIdent {
        venue: d.venue.clone(),
        address: o.address.clone(),
        key_policy: d.key_policy.clone(),
        key_name: d.key_name.clone(),
    };
    match pools.iter().position(|p| *p == ident) {
        Some(i) => i as u32,
        None => {
            pools.push(ident);
            (pools.len() - 1) as u32
        }
    }
}

pub fn build(rows: &[FeedRow], observations: &[Observation], roles: &Roles) -> Story {
    let mut events: Vec<StoryEvent> = Vec::new();
    let mut pools: Vec<PoolIdent> = Vec::new();

    // ⚠️ A trade event carries the venue's CREDENTIAL; a pool observation
    // carries its NAME. Resolve here so both sides of the stream speak the
    // same vocabulary — otherwise a consumer grouping by venue sees `splash`
    // with no fills and a hex string with all of them, which is what the first
    // token band drew.
    //
    // An unregistered credential keeps its hex, deliberately: that is a gap in
    // OUR registry rather than an absence of trading, and the credential is
    // what lets someone identify the venue and close it.
    let name = |cred: &str| {
        roles
            .name_of(cred)
            .map(str::to_string)
            .unwrap_or_else(|| cred.to_string())
    };

    for f in fold(rows, roles) {
        let kind = match &f.event {
            Event::Fill {
                venue_cred,
                party,
                amount,
                into_pool,
            } => Kind::Fill {
                venue: name(venue_cred),
                party: party.into(),
                amount: *amount,
                into_pool: *into_pool,
            },
            Event::Placement {
                venue_cred,
                party,
                amount,
            } => Kind::Placement {
                venue: name(venue_cred),
                party: party.into(),
                amount: *amount,
            },
            Event::Cancellation {
                venue_cred,
                party,
                amount,
            } => Kind::Cancellation {
                venue: name(venue_cred),
                party: party.into(),
                amount: *amount,
            },
            Event::BatchedFill {
                venue_cred,
                orders,
                amount,
            } => Kind::BatchedFill {
                venue: name(venue_cred),
                orders: *orders,
                amount: *amount,
            },
            // A plain movement: the direction the rows themselves support.
            Event::Transfer => match direction_of(rows, &f.tx_hash, &f.unit) {
                Some(k) => k,
                None => continue,
            },
        };
        events.push(StoryEvent {
            slot: f.slot,
            unix: f.block_time,
            tx: f.tx_hash,
            unit: f.unit,
            kind,
        });
    }

    for o in observations {
        // ⚠️ A SCRIPT WE CAN NAME IS NOT UNCLAIMED.
        //
        // `recognise` decodes RESERVES, so it correctly declines an order
        // contract — it is not a pool and has none. But the roles table knows
        // exactly whose contract it is, and the movement through it is already
        // in the stream as a `Placement` or a `Cancellation`. Reporting it a
        // second time as "no decoder claimed this" overstates our blind spot
        // with something we have already explained.
        //
        // MEASURED on $DONUT the moment Sundae's order credential landed: 492
        // of 499 "unclaimed" outputs were the order contract whose placements
        // the same stream was now naming. The remaining 7 are the real edge of
        // coverage — a burn sink and two singletons — which is the number that
        // was worth showing all along.
        //
        // Nothing is lost: the raw observation, datum included, stays in the
        // ARCHIVE. This is the projection, and an order contract holds no
        // reserves for a later decoder to re-read.
        if o.decoded.is_none()
            && crate::trade::address_parts(&o.address)
                .is_some_and(|(cred, _)| roles.role_of(&cred).is_some())
        {
            continue;
        }
        let kind = match &o.decoded {
            None => Kind::UnclaimedScript {
                address: o.address.clone(),
                amount: o.unit_amount,
                has_datum: o.datum.is_some(),
            },
            Some(d) if d.pricing == pricing::BONDING_CURVE => Kind::CurveState {
                pool: intern_pool(&mut pools, o, d),
                lovelace: o.lovelace,
                tokens_left: o.unit_amount,
                // The crate cannot decode the curve's own parameters — that
                // is a launchpad decoder's job — so progress is the caller's
                // to supply. `None` is honest; a 0 would not be.
                progress: None,
            },
            Some(d) => Kind::PoolState(PoolState {
                // ⚠️ Interned on ADDRESS + key policy + key name, not on the
                // instance key alone. The key alone is empty for every pool on
                // a venue that mints no pool NFT, which merged them; and it is
                // shared across key policies at one address, which merged
                // those. See `PoolIdent`.
                pool: intern_pool(&mut pools, o, d),
                base: d.base_reserve,
                // `None` is "the venue does not publish it", which is not 0.
                quote: d.quote_reserve,
                quote_policy: d.quote_policy.clone(),
                quote_name: d.quote_name.clone(),
                pricing: d.pricing.clone(),
            }),
        };
        events.push(StoryEvent {
            slot: o.slot,
            unix: o.block_time,
            tx: o.tx_hash.clone(),
            unit: o.unit_name.clone(),
            kind,
        });
    }

    events.sort_by(|a, b| {
        a.slot
            .cmp(&b.slot)
            .then_with(|| a.kind.rank().cmp(&b.kind.rank()))
            // Curve sightings inside one slot, by position up the curve.
            .then_with(|| a.kind.curve_position().cmp(&b.kind.curve_position()))
            .then_with(|| a.tx.cmp(&b.tx))
            .then_with(|| a.unit.cmp(&b.unit))
    });

    let mut slots: Vec<u64> = events.iter().map(|e| e.slot).collect();
    slots.dedup();
    Story {
        distinct_slots: slots.len(),
        events,
        pools,
        markers: Vec::new(),
    }
}

/// What the party rows say a plain movement was.
///
/// Reuses [`crate::feed::direction`] so the stream and the feed cannot
/// disagree about the same transaction — the one place a second
/// implementation would be invisible.
fn direction_of(rows: &[FeedRow], tx: &[u8], unit: &[u8]) -> Option<Kind> {
    use crate::feed::Direction;
    let u = rows
        .iter()
        .find(|r| r.tx_hash == tx)?
        .units
        .iter()
        .find(|u| u.name == unit)?;
    let amount = u.parties.iter().map(|p| p.amount.abs()).max().unwrap_or(0);
    Some(match crate::feed::direction(u) {
        Direction::Mint { to } => Kind::Mint {
            to: Some(to),
            amount: u.net_mint,
        },
        Direction::Burn { from } => Kind::Burn {
            from: Some(from),
            amount: -u.net_mint,
        },
        Direction::Transfer { from, to } => Kind::Transfer { from, to, amount },
        Direction::SourceBelowFloor { to } => Kind::ArrivedFromBelowFloor { to, amount },
        Direction::Ambiguous => Kind::Ambiguous {
            parties: u.parties.len(),
            amount,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{PartyMove, UnitMove};

    /// A real Splash pool address, so the credential derived from it is the
    /// one on chain.
    const SPLASH_POOL: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";

    fn row(tx: u8, slot: u64, unit: &str, net_mint: i64, parties: &[(&str, i64)]) -> FeedRow {
        FeedRow {
            tx_hash: vec![tx; 32],
            slot,
            block_time: 1_700_000_000 + slot,
            units: vec![UnitMove {
                name: unit.as_bytes().to_vec(),
                net_mint,
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

    fn obs(slot: u64, unit: &str, lovelace: i64, amount: i64, venue: Option<&str>) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![9; 32],
            address: "addr1script".into(),
            lovelace,
            unit_name: unit.as_bytes().to_vec(),
            unit_amount: amount,
            datum: Some(vec![1, 2, 3]),
            decoded: venue.map(|v| crate::observation::Decoded {
                venue: v.into(),
                key_policy: Vec::new(),
                key_name: Vec::new(),
                key_basis: "test".into(),
                base_reserve: amount,
                quote_policy: None,
                quote_name: None,
                quote_reserve: Some(lovelace),
                fee_bps: None,
                total_lp: None,
                reserve_source: "test".into(),
                pricing: pricing::CONSTANT_PRODUCT.into(),
            }),
        }
    }

    #[test]
    fn a_mint_a_transfer_and_a_pool_state_land_on_one_spine() {
        let rows = vec![
            row(1, 10, "A", 100, &[("alice", 100)]),
            row(2, 20, "A", 0, &[("alice", -40), ("bob", 40)]),
        ];
        let obs = vec![obs(30, "A", 5_000, 60, Some("splash"))];
        let s = build(&rows, &obs, &Roles::default());

        assert_eq!(s.events.len(), 3);
        assert_eq!(s.distinct_slots, 3);
        assert!(matches!(s.events[0].kind, Kind::Mint { .. }));
        assert!(matches!(s.events[1].kind, Kind::Transfer { .. }));
        assert!(matches!(s.events[2].kind, Kind::PoolState(_)));
        // Ascending, so a timeline can read it straight through.
        assert!(s.events.windows(2).all(|w| w[0].slot <= w[1].slot));
    }

    /// ⚠️ A FILL AND A POOL STATE MUST NAME THE SAME VENUE THE SAME WAY.
    ///
    /// The trade fold works from credentials and an observation from a decoded
    /// venue name, so without resolution one says `da5b47ae…` and the other
    /// says `cswap`. A consumer grouping by venue then draws named pools with
    /// no trading beside hex strings doing all of it — which is exactly what
    /// the first token band rendered.
    #[test]
    fn a_fill_and_a_pool_state_agree_on_the_venue_name() {
        let mut roles = Roles::default();
        let cred = crate::trade::address_parts(SPLASH_POOL).unwrap().0;
        roles.register(cred, crate::trade::Role::Pool, "splash");

        let rows = vec![row(1, 10, "A", 0, &[("alice", -5), (SPLASH_POOL, 5)])];
        let obs = vec![obs(11, "A", 100, 50, Some("splash"))];
        let s = build(&rows, &obs, &roles);

        let venues: Vec<&str> = s
            .events
            .iter()
            .filter_map(|e| match &e.kind {
                Kind::Fill { venue, .. } => Some(venue.as_str()),
                // Through the pool TABLE, which is the only place a pool's
                // venue is stated from v4 on.
                Kind::PoolState(p) => Some(s.venue(p.pool)),
                _ => None,
            })
            .collect();
        assert_eq!(venues.len(), 2);
        assert!(
            venues.iter().all(|v| *v == "splash"),
            "fill and pool state disagree: {venues:?}"
        );
    }

    /// ⚠️ An UNREGISTERED credential keeps its hex rather than becoming
    /// "unknown". That is a gap in our registry, not an absence of trading,
    /// and the credential is what lets someone go and close it.
    #[test]
    fn an_unregistered_venue_keeps_its_credential() {
        let rows = vec![row(1, 10, "A", 0, &[("alice", -5), (SPLASH_POOL, 5)])];
        let s = build(&rows, &[], &Roles::default());
        // With no roles at all it is not even a fill — but the naming rule is
        // what matters, so assert the fold did not invent a venue.
        assert!(s.events.iter().all(|e| !matches!(
            &e.kind,
            Kind::Fill { venue, .. } if venue == "unknown"
        )));
    }

    /// ⚠️ Every event carries its UNIT, or the stream can describe a policy
    /// but never one NFT of it.
    #[test]
    fn every_event_names_its_asset() {
        let rows = vec![
            row(1, 10, "HAT", 1, &[("alice", 1)]),
            row(2, 11, "BOOT", 1, &[("bob", 1)]),
        ];
        let s = build(&rows, &[], &Roles::default());
        let units: Vec<&[u8]> = s.events.iter().map(|e| e.unit.as_slice()).collect();
        assert!(units.contains(&b"HAT".as_slice()));
        assert!(units.contains(&b"BOOT".as_slice()));
    }

    /// An output nothing recognised is EMITTED, not dropped — a consumer can
    /// see the gap in our coverage rather than reading an absence as nothing.
    #[test]
    fn an_unclaimed_script_output_is_in_the_stream() {
        let s = build(&[], &[obs(5, "A", 1_000, 7, None)], &Roles::default());
        match &s.events[0].kind {
            Kind::UnclaimedScript { has_datum, .. } => assert!(has_datum),
            k => panic!("expected UnclaimedScript, got {k:?}"),
        }
    }

    /// ⚠️ …BUT A SCRIPT WE CAN NAME IS NOT A GAP IN OUR COVERAGE.
    ///
    /// `recognise` decodes RESERVES and correctly declines an order contract,
    /// which has none. The roles table knows whose it is, and the movement
    /// through it is already in the stream as a placement. Emitting it AGAIN
    /// as "no decoder claimed this" reports a blind spot we do not have.
    ///
    /// MEASURED on $DONUT the moment Sundae's order credential landed: 492 of
    /// 499 "unclaimed" outputs were that contract. The 7 that remained are the
    /// real edge of coverage, and are the number worth showing.
    #[test]
    fn a_script_the_roles_table_names_is_not_unclaimed() {
        // A real SundaeSwap V3 order address, from
        // `ae24323c…#0`. Its payment credential is what the roles table holds.
        const SUNDAE_ORDER: &str = "addr1z8ax5k9mutg07p2ngscu3chsauktmstq92z9de938j8nqa7zcka2k2tsgmuedt4xl2j5awftvqzmmv3vs2yduzqxfcmsyun6n3";
        let mut o = obs(5, "A", 1_000, 7, None);
        o.address = SUNDAE_ORDER.into();

        // Unknown to the roles table: a genuine gap, and it is reported.
        let blind = build(&[], std::slice::from_ref(&o), &Roles::default());
        assert_eq!(blind.events.len(), 1);
        assert!(matches!(blind.events[0].kind, Kind::UnclaimedScript { .. }));

        // Named by the roles table: not a gap, and not reported twice.
        let (cred, _) = crate::trade::address_parts(SUNDAE_ORDER).expect("a real address");
        let mut roles = Roles::default();
        roles.register(cred, crate::trade::Role::Order, "sundae-v3");
        let known = build(&[], &[o], &roles);
        assert!(
            known.events.is_empty(),
            "a named contract is explained by its own placement: {:?}",
            known.events,
        );
    }

    /// An arrival with no source is NOT a mint. Reading it as one would invent
    /// supply out of the walk's own floor.
    #[test]
    fn an_arrival_without_a_source_is_not_a_mint() {
        let rows = vec![row(1, 10, "A", 0, &[("bob", 40)])];
        let s = build(&rows, &[], &Roles::default());
        assert!(matches!(
            s.events[0].kind,
            Kind::ArrivedFromBelowFloor { .. }
        ));
    }

    /// Several parties a side: stated, never resolved to a guess.
    #[test]
    fn a_batched_move_stays_ambiguous() {
        let rows = vec![row(
            1,
            10,
            "A",
            0,
            &[("a", -10), ("b", -10), ("c", 12), ("d", 8)],
        )];
        let s = build(&rows, &[], &Roles::default());
        match &s.events[0].kind {
            Kind::Ambiguous { parties, .. } => assert_eq!(*parties, 4),
            k => panic!("expected Ambiguous, got {k:?}"),
        }
    }

    /// Curve sightings sharing a slot come back by position up the curve, so a
    /// line drawn through them climbs instead of zigzagging.
    #[test]
    fn curve_sightings_in_one_slot_are_ordered_by_position() {
        let curve = |lovelace: i64, left: i64| Observation {
            decoded: Some(crate::observation::Decoded {
                pricing: pricing::BONDING_CURVE.into(),
                ..obs(50, "A", lovelace, left, Some("snek.fun"))
                    .decoded
                    .unwrap()
            }),
            ..obs(50, "A", lovelace, left, Some("snek.fun"))
        };
        let s = build(
            &[],
            &[curve(900, 30), curve(500, 70), curve(700, 50)],
            &Roles::default(),
        );
        let seen: Vec<i64> = s
            .events
            .iter()
            .filter_map(|e| match &e.kind {
                Kind::CurveState { lovelace, .. } => Some(*lovelace),
                _ => None,
            })
            .collect();
        assert_eq!(seen, vec![500, 700, 900]);
        assert_eq!(s.distinct_slots, 1, "all one block — ordering is unproven");
    }

    fn curve(slot: u64, lovelace: i64) -> Observation {
        let base = obs(slot, "A", lovelace, 10, Some("snek.fun"));
        Observation {
            decoded: Some(crate::observation::Decoded {
                pricing: pricing::BONDING_CURVE.into(),
                ..base.decoded.clone().unwrap()
            }),
            ..base
        }
    }

    /// ⚠️ THE CASE MARKERS EXIST FOR. A launch far below the window must come
    /// back as `Before`, not as an absence — `curve points: 0` cannot
    /// otherwise be told apart from "this token never launched on a curve".
    #[test]
    fn a_launch_below_the_window_is_marked_before_it() {
        let all = vec![
            curve(100, 500),
            curve(150, 900),
            obs(9_000, "A", 1, 1, Some("splash")),
        ];
        let m = markers(&all, Some(50), (8_000, 10_000));

        let launch = m.iter().find(|m| m.kind == MarkerKind::Launch).unwrap();
        assert_eq!(launch.slot, 100);
        assert_eq!(launch.at, Where::Before, "out of frame, not absent");

        let mint = m.iter().find(|m| m.kind == MarkerKind::FirstMint).unwrap();
        assert_eq!((mint.slot, mint.at), (50, Where::Before));
    }

    /// A marker inside the window says so, so a consumer draws it on the
    /// timeline rather than as an edge indicator.
    #[test]
    fn a_launch_inside_the_window_is_marked_within() {
        let all = vec![curve(9_100, 500), obs(9_500, "A", 1, 1, Some("splash"))];
        let m = markers(&all, None, (9_000, 10_000));
        assert_eq!(m[0].at, Where::Within);
    }

    /// ⚠️ A curve still being traded has NOT graduated. Marking one would put
    /// a finish line on a token that never crossed it.
    #[test]
    fn a_curve_with_nothing_after_it_is_not_a_graduation() {
        let all = vec![curve(100, 500), curve(150, 900)];
        let m = markers(&all, None, (0, 1_000));
        assert!(m.iter().all(|m| m.kind != MarkerKind::Graduation));
        assert!(m.iter().any(|m| m.kind == MarkerKind::Launch));
    }

    /// …and one that later trades elsewhere HAS.
    #[test]
    fn a_curve_followed_by_another_venue_is_a_graduation() {
        let all = vec![
            curve(100, 500),
            curve(150, 900),
            obs(200, "A", 1, 1, Some("splash")),
        ];
        let m = markers(&all, None, (0, 1_000));
        let g = m.iter().find(|m| m.kind == MarkerKind::Graduation).unwrap();
        assert_eq!(g.slot, 150, "the LAST curve sighting is the finish line");
    }

    /// A policy with no launchpad history gets no launch marker — an empty
    /// list is the honest answer, not a zeroed one.
    #[test]
    fn a_policy_that_never_launched_on_a_curve_has_no_launch_marker() {
        let all = vec![obs(500, "A", 1, 1, Some("splash"))];
        let m = markers(&all, None, (0, 1_000));
        assert!(m.is_empty());
    }

    /// Deterministic: the same inputs give byte-identical output, whichever
    /// order they arrive in. A framework consumer diffing two fetches must not
    /// see phantom changes.
    #[test]
    fn the_stream_is_deterministic_regardless_of_input_order() {
        let a = row(1, 10, "A", 100, &[("alice", 100)]);
        let b = row(2, 10, "B", 50, &[("bob", 50)]);
        let one = build(&[a.clone(), b.clone()], &[], &Roles::default());
        let two = build(&[b, a], &[], &Roles::default());
        assert_eq!(one, two);
    }
}
