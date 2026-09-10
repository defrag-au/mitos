//! The story on the wire: postcard, interned, delta-encoded.
//!
//! # Why not JSON — MEASURED, and not the reason you would guess
//!
//! $PERP, one 5,000-movement window, the same events both ways:
//!
//! | | raw | gzip |
//! |---|---|---|
//! | **postcard**, no tx hashes | 613 KB | **233 KB** |
//! | postcard, + tx hashes | 934 KB | 617 KB |
//! | **json**, no tx hashes | 2,895 KB | **273 KB** |
//! | json, + tx hashes | 4,126 KB | 791 KB |
//!
//! ⚠️ **Raw, postcard is 4.7× smaller. Gzipped, only 1.18×.** JSON's
//! repetition is exactly what a compressor eats, so over a compressed
//! transport the SIZE argument is weak, and an earlier draft of this header
//! overstated it. The real reasons are the other three:
//!
//! - **parse cost** — the browser blocks building ~25,000 objects out of
//!   2.9 MB of text. Postcard decodes into flat rows in a wasm consumer,
//!   which is what the frontends here are. For an interactive timeline this,
//!   not bandwidth, is the budget that matters.
//! - **precision** — a JS number cannot hold an `i64`, and it fails
//!   **SILENTLY**. A reserve of `10991175000` survives; `total_lp` on a large
//!   pool does not. That is a correctness argument, not an efficiency one.
//! - **memory** — 613 KB resident beats 2.9 MB plus the object graph, on a
//!   surface meant to hold several policies at once.
//!
//! Same approach as the rest of the workspace (`market-ledger-wire`,
//! `token-ledger-wire`, [`crate::bundle`], [`crate::graph`]): postcard, side
//! tables interned once, hashes as fixed byte arrays, slots delta-encoded.
//!
//! # Encoding contract
//!
//! **Postcard is positional — there are no field names or tags on the wire, so
//! the struct definitions here ARE the format:**
//!
//! - never reorder, remove, or insert fields; never change a field's type or
//!   `Option`-ness;
//! - no `#[serde(skip_serializing_if)]` / `default` — every field always
//!   present;
//! - enum variants are append-only (discriminant = declaration order), pinned
//!   by a test below;
//! - rows live inside a `Vec`, so even APPENDING a field to a row type is a
//!   breaking change.
//!
//! Any change bumps [`STORY_WIRE_VERSION`], which is the first field and so
//! byte 0 of every payload: a consumer peeks it and fails loudly rather than
//! decoding garbage.
//!
//! # Where the bytes actually go
//!
//! ⚠️ **Transaction hashes dominate, and MORE than expected.** 32 bytes each,
//! and they are random — so a compressor cannot help. MEASURED on the same
//! window: **gzip 233 KB → 617 KB when they are included, a 2.65× cost for
//! identity alone.**
//!
//! `crate::graph` found the same thing from the other direction (txids were
//! 57% of the detail artifact), which is why [`StoryStream::txs`] is OPTIONAL
//! and **off by default**. A chart does not need them; an interactive timeline
//! where clicking an event opens an explorer does. The caller chooses rather
//! than paying either way.
//!
//! Addresses are next, which is why they are interned: a wallet trading a
//! hundred times appears once in [`StoryStream::addresses`] and a hundred
//! `u32`s in the rows.

use serde::{Deserialize, Serialize};

use super::{Kind, PoolState, Story, StoryEvent, Trader};

/// Byte 0 of every encoded [`StoryStream`]. Bump on ANY change to the types in
/// this module — see the encoding contract above.
pub const STORY_WIRE_VERSION: u8 = 1;

/// Absent index — the `Option<u32>` postcard would otherwise cost a byte for.
pub const NONE_IDX: u32 = u32::MAX;

/// What happened, as a wire tag.
///
/// ⚠️ Discriminants are postcard varint tags in declaration order.
/// **APPEND ONLY. Never reorder.** Pinned by `tags_are_append_only`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tag {
    Mint,                  // 0
    Burn,                  // 1
    Transfer,              // 2
    ArrivedFromBelowFloor, // 3
    Ambiguous,             // 4
    Fill,                  // 5
    Placement,             // 6
    Cancellation,          // 7
    BatchedFill,           // 8
    PoolState,             // 9
    CurveState,            // 10
    UnclaimedScript,       // 11
}

impl Tag {
    pub const ALL: [Tag; 12] = [
        Tag::Mint,
        Tag::Burn,
        Tag::Transfer,
        Tag::ArrivedFromBelowFloor,
        Tag::Ambiguous,
        Tag::Fill,
        Tag::Placement,
        Tag::Cancellation,
        Tag::BatchedFill,
        Tag::PoolState,
        Tag::CurveState,
        Tag::UnclaimedScript,
    ];
}

/// How a trader is (or is not) named, on the wire.
///
/// ⚠️ APPEND ONLY. The distinction between `NotEncodedByVenue` and `Unknown`
/// is load-bearing: the first says the venue's order contract is one shared
/// address and no fill can ever name a trader, the second says we declined to
/// guess. Collapsing them attributes every trade on such a venue to one party.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraderKind {
    Stake,             // 0
    Wallet,            // 1
    NotEncodedByVenue, // 2
    Unknown,           // 3
}

/// One event. Fields are shared across kinds — `tag` says how to read them.
///
/// Deliberately one row type rather than an enum-per-kind: postcard would
/// encode a variant tag either way, and a flat row keeps the decoder a table
/// lookup instead of a match that allocates differently per arm.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRow {
    /// Slots since the previous row; the first is absolute. Ascending, so
    /// every delta is non-negative and most are 0 or small — which is exactly
    /// what postcard's varints are cheap at.
    pub slot_delta: u64,
    /// Unix seconds since the previous row, same encoding.
    pub unix_delta: u64,
    /// Index into [`StoryStream::txs`], or [`NONE_IDX`] when tx hashes were
    /// not requested.
    pub tx: u32,
    /// Index into [`StoryStream::units`].
    pub unit: u32,
    pub tag: Tag,
    /// Index into [`StoryStream::venues`], or [`NONE_IDX`].
    pub venue: u32,
    /// The primary party — sender, minter, trader, or the script's address.
    /// Index into [`StoryStream::addresses`], or [`NONE_IDX`].
    pub party_a: u32,
    /// The secondary party — a transfer's recipient. [`NONE_IDX`] otherwise.
    pub party_b: u32,
    pub trader: TraderKind,
    /// The event's main quantity: units moved, minted, burned, or a pool's
    /// base reserve.
    ///
    /// ⚠️ `i64`, not a float. Raw on-chain units — applying decimals is the
    /// consumer's job and needs the token registry.
    pub amount: i64,
    /// The second quantity, by tag: `PoolState` → quote reserve,
    /// `CurveState` → lovelace on the curve, `BatchedFill` → order count,
    /// `Ambiguous` → party count. Zero where the tag has no second quantity.
    pub extra: i64,
    /// `CurveState` → tokens still on the curve. Otherwise 0.
    pub extra2: i64,
    /// Per-tag flags. Bit 0: `Fill.into_pool`, or
    /// `UnclaimedScript.has_datum`, or `PoolState` quote reserve KNOWN.
    pub flags: u8,
}

/// Bit 0 of [`EventRow::flags`].
pub const FLAG_BIT0: u8 = 1;

/// One window of a policy's story.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoryStream {
    /// **Byte 0 of the payload.** Peek before decoding the rest.
    pub version: u8,
    pub policy: String,
    /// The window these rows cover, and what it is a window OF.
    pub from_slot: u64,
    pub to_slot: u64,
    /// Whether the archive behind it reached the policy's first mint.
    pub complete: bool,
    /// Distinct slots. `1` means everything shares a block and **no ordering
    /// within it is proven** — see the `story` module header.
    pub distinct_slots: u64,

    // ── side tables, interned once ───────────────────────────────────────
    pub addresses: Vec<String>,
    pub venues: Vec<String>,
    /// On-chain asset-name bytes. IDENTITY only.
    pub units: Vec<Vec<u8>>,
    /// ⚠️ EMPTY when the caller did not ask for hashes — 32 bytes each is the
    /// single largest cost in this format. Rows then carry [`NONE_IDX`].
    pub txs: Vec<[u8; 32]>,
    /// Quote assets referenced by `PoolState` rows, as
    /// `(policy, name)`. Empty policy = ADA.
    pub quote_units: Vec<(Vec<u8>, Vec<u8>)>,
    /// Parallel to the `PoolState` rows in order: which quote unit each used,
    /// and its pricing model. Kept out of `EventRow` so the 90% of rows that
    /// are not pool states do not carry two dead fields.
    pub pool_quote: Vec<u32>,
    /// `0` = constant-product, `1` = bonding curve. ⚠️ A consumer computing a
    /// rate from reserves MUST check this: a bonding curve's reserves are real
    /// and its rate is not `quote / base`.
    pub pool_pricing: Vec<u8>,

    pub rows: Vec<EventRow>,
}

/// Whether to spend 32 bytes per transaction on identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Txs {
    /// Include them — an interactive timeline that links to an explorer.
    Include,
    /// Omit them. A chart does not need them, and they are the largest single
    /// cost in the payload.
    Omit,
}

#[derive(Default)]
struct Intern {
    addresses: Vec<String>,
    venues: Vec<String>,
    units: Vec<Vec<u8>>,
    txs: Vec<[u8; 32]>,
    quote_units: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Intern {
    fn address(&mut self, s: &str) -> u32 {
        idx(&mut self.addresses, s.to_string())
    }
    fn venue(&mut self, s: &str) -> u32 {
        idx(&mut self.venues, s.to_string())
    }
    fn unit(&mut self, b: &[u8]) -> u32 {
        idx(&mut self.units, b.to_vec())
    }
    fn quote(&mut self, p: &Option<Vec<u8>>, n: &Option<Vec<u8>>) -> u32 {
        idx(
            &mut self.quote_units,
            (p.clone().unwrap_or_default(), n.clone().unwrap_or_default()),
        )
    }
    fn tx(&mut self, h: &[u8]) -> u32 {
        let mut a = [0u8; 32];
        let n = h.len().min(32);
        a[..n].copy_from_slice(&h[..n]);
        idx(&mut self.txs, a)
    }
}

/// Linear scan rather than a `HashMap`: these tables are hundreds of entries,
/// the scan stays in cache, and a map's iteration order would have to be
/// pinned anyway to keep the output deterministic.
fn idx<T: PartialEq>(table: &mut Vec<T>, v: T) -> u32 {
    match table.iter().position(|x| *x == v) {
        Some(i) => i as u32,
        None => {
            table.push(v);
            (table.len() - 1) as u32
        }
    }
}

/// Build the wire form. `from`/`to`/`complete` describe the window the caller
/// read, which the stream cannot know from its events alone — an empty window
/// is a fact about coverage, not an absence of data.
pub fn encode(
    story: &Story,
    policy: &str,
    from_slot: u64,
    to_slot: u64,
    complete: bool,
    txs: Txs,
) -> StoryStream {
    let mut t = Intern::default();
    let mut rows = Vec::with_capacity(story.events.len());
    let mut pool_quote = Vec::new();
    let mut pool_pricing = Vec::new();
    let (mut last_slot, mut last_unix) = (0u64, 0u64);

    for e in &story.events {
        let mut r = EventRow {
            slot_delta: e.slot.saturating_sub(last_slot),
            unix_delta: e.unix.saturating_sub(last_unix),
            tx: match txs {
                Txs::Include => t.tx(&e.tx),
                Txs::Omit => NONE_IDX,
            },
            unit: t.unit(&e.unit),
            tag: Tag::Mint,
            venue: NONE_IDX,
            party_a: NONE_IDX,
            party_b: NONE_IDX,
            trader: TraderKind::Unknown,
            amount: 0,
            extra: 0,
            extra2: 0,
            flags: 0,
        };
        last_slot = e.slot;
        last_unix = e.unix;

        match &e.kind {
            Kind::Mint { to, amount } => {
                r.tag = Tag::Mint;
                r.amount = *amount;
                if let Some(a) = to {
                    r.party_a = t.address(a);
                }
            }
            Kind::Burn { from, amount } => {
                r.tag = Tag::Burn;
                r.amount = *amount;
                if let Some(a) = from {
                    r.party_a = t.address(a);
                }
            }
            Kind::Transfer { from, to, amount } => {
                r.tag = Tag::Transfer;
                r.amount = *amount;
                r.party_a = t.address(from);
                r.party_b = t.address(to);
            }
            Kind::ArrivedFromBelowFloor { to, amount } => {
                r.tag = Tag::ArrivedFromBelowFloor;
                r.amount = *amount;
                r.party_b = t.address(to);
            }
            Kind::Ambiguous { parties, amount } => {
                r.tag = Tag::Ambiguous;
                r.amount = *amount;
                r.extra = *parties as i64;
            }
            Kind::Fill {
                venue,
                party,
                amount,
                into_pool,
            } => {
                r.tag = Tag::Fill;
                r.amount = *amount;
                r.venue = t.venue(venue);
                write_trader(&mut r, &mut t, party);
                if *into_pool {
                    r.flags |= FLAG_BIT0;
                }
            }
            Kind::Placement {
                venue,
                party,
                amount,
            } => {
                r.tag = Tag::Placement;
                r.amount = *amount;
                r.venue = t.venue(venue);
                write_trader(&mut r, &mut t, party);
            }
            Kind::Cancellation {
                venue,
                party,
                amount,
            } => {
                r.tag = Tag::Cancellation;
                r.amount = *amount;
                r.venue = t.venue(venue);
                write_trader(&mut r, &mut t, party);
            }
            Kind::BatchedFill {
                venue,
                orders,
                amount,
            } => {
                r.tag = Tag::BatchedFill;
                r.amount = *amount;
                r.extra = *orders as i64;
                r.venue = t.venue(venue);
            }
            Kind::PoolState(p) => {
                r.tag = Tag::PoolState;
                r.amount = p.base;
                r.venue = t.venue(&p.venue);
                // `None` quote is "the venue does not publish it" — encoded as
                // a cleared flag, not as 0, so a consumer cannot compute a
                // rate from a reserve that was never stated.
                if let Some(q) = p.quote {
                    r.extra = q;
                    r.flags |= FLAG_BIT0;
                }
                pool_quote.push(t.quote(&p.quote_policy, &p.quote_name));
                pool_pricing.push(match p.pricing.as_str() {
                    crate::observation::pricing::BONDING_CURVE => 1,
                    _ => 0,
                });
            }
            Kind::CurveState {
                venue,
                lovelace,
                tokens_left,
                progress: _,
            } => {
                r.tag = Tag::CurveState;
                r.venue = t.venue(venue);
                // Progress is DERIVED from the curve's own parameters, which
                // this crate cannot decode — so it is not on the wire. A
                // consumer computes it from lovelace and the cap, or does
                // without. Shipping a `None` field per row would cost bytes
                // to say nothing.
                r.extra = *lovelace;
                r.extra2 = *tokens_left;
            }
            Kind::UnclaimedScript {
                address,
                amount,
                has_datum,
            } => {
                r.tag = Tag::UnclaimedScript;
                r.amount = *amount;
                r.party_a = t.address(address);
                if *has_datum {
                    r.flags |= FLAG_BIT0;
                }
            }
        }
        rows.push(r);
    }

    StoryStream {
        version: STORY_WIRE_VERSION,
        policy: policy.to_string(),
        from_slot,
        to_slot,
        complete,
        distinct_slots: story.distinct_slots as u64,
        addresses: t.addresses,
        venues: t.venues,
        units: t.units,
        txs: t.txs,
        quote_units: t.quote_units,
        pool_quote,
        pool_pricing,
        rows,
    }
}

fn write_trader(r: &mut EventRow, t: &mut Intern, p: &Trader) {
    match p {
        Trader::Stake(s) => {
            r.trader = TraderKind::Stake;
            r.party_a = t.address(s);
        }
        Trader::Wallet(w) => {
            r.trader = TraderKind::Wallet;
            r.party_a = t.address(w);
        }
        Trader::NotEncodedByVenue => r.trader = TraderKind::NotEncodedByVenue,
        Trader::Unknown => r.trader = TraderKind::Unknown,
    }
}

/// Encode to bytes.
pub fn to_bytes(s: &StoryStream) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(s)
}

/// Decode, refusing a version this build does not know rather than reading
/// the bytes at the wrong shape.
pub fn from_bytes(b: &[u8]) -> Result<StoryStream, String> {
    match b.first() {
        None => return Err("empty story payload".into()),
        Some(&v) if v != STORY_WIRE_VERSION => {
            return Err(format!(
                "story wire version {v} (this build reads {STORY_WIRE_VERSION})"
            ));
        }
        Some(_) => {}
    }
    postcard::from_bytes(b).map_err(|e| format!("decoding story: {e}"))
}

/// Re-inflate the rows into [`StoryEvent`]s — the inverse of [`encode`], for a
/// consumer that wants the domain types back rather than the columns.
pub fn decode(s: &StoryStream) -> Vec<StoryEvent> {
    let (mut slot, mut unix) = (0u64, 0u64);
    let mut pool_at = 0usize;
    let addr = |i: u32| -> Option<String> {
        (i != NONE_IDX)
            .then(|| s.addresses.get(i as usize).cloned())
            .flatten()
    };
    let mut out = Vec::with_capacity(s.rows.len());
    for r in &s.rows {
        slot += r.slot_delta;
        unix += r.unix_delta;
        let flag = r.flags & FLAG_BIT0 != 0;
        let kind = match r.tag {
            Tag::Mint => Kind::Mint {
                to: addr(r.party_a),
                amount: r.amount,
            },
            Tag::Burn => Kind::Burn {
                from: addr(r.party_a),
                amount: r.amount,
            },
            Tag::Transfer => Kind::Transfer {
                from: addr(r.party_a).unwrap_or_default(),
                to: addr(r.party_b).unwrap_or_default(),
                amount: r.amount,
            },
            Tag::ArrivedFromBelowFloor => Kind::ArrivedFromBelowFloor {
                to: addr(r.party_b).unwrap_or_default(),
                amount: r.amount,
            },
            Tag::Ambiguous => Kind::Ambiguous {
                parties: r.extra as usize,
                amount: r.amount,
            },
            Tag::Fill => Kind::Fill {
                venue: venue_of(s, r.venue),
                party: trader_of(r, &addr),
                amount: r.amount,
                into_pool: flag,
            },
            Tag::Placement => Kind::Placement {
                venue: venue_of(s, r.venue),
                party: trader_of(r, &addr),
                amount: r.amount,
            },
            Tag::Cancellation => Kind::Cancellation {
                venue: venue_of(s, r.venue),
                party: trader_of(r, &addr),
                amount: r.amount,
            },
            Tag::BatchedFill => Kind::BatchedFill {
                venue: venue_of(s, r.venue),
                orders: r.extra as usize,
                amount: r.amount,
            },
            Tag::PoolState => {
                let q = s.pool_quote.get(pool_at).copied().unwrap_or(NONE_IDX);
                let pricing = s.pool_pricing.get(pool_at).copied().unwrap_or(0);
                pool_at += 1;
                let (qp, qn) = s.quote_units.get(q as usize).cloned().unwrap_or_default();
                Kind::PoolState(PoolState {
                    venue: venue_of(s, r.venue),
                    base: r.amount,
                    quote: flag.then_some(r.extra),
                    quote_policy: (!qp.is_empty()).then_some(qp),
                    quote_name: (!qn.is_empty()).then_some(qn),
                    pricing: match pricing {
                        1 => crate::observation::pricing::BONDING_CURVE.into(),
                        _ => crate::observation::pricing::CONSTANT_PRODUCT.into(),
                    },
                })
            }
            Tag::CurveState => Kind::CurveState {
                venue: venue_of(s, r.venue),
                lovelace: r.extra,
                tokens_left: r.extra2,
                progress: None,
            },
            Tag::UnclaimedScript => Kind::UnclaimedScript {
                address: addr(r.party_a).unwrap_or_default(),
                amount: r.amount,
                has_datum: flag,
            },
        };
        out.push(StoryEvent {
            slot,
            unix,
            tx: s
                .txs
                .get(r.tx as usize)
                .map(|h| h.to_vec())
                .unwrap_or_default(),
            unit: s.units.get(r.unit as usize).cloned().unwrap_or_default(),
            kind,
        });
    }
    out
}

fn venue_of(s: &StoryStream, i: u32) -> String {
    s.venues.get(i as usize).cloned().unwrap_or_default()
}

fn trader_of(r: &EventRow, addr: &impl Fn(u32) -> Option<String>) -> Trader {
    match r.trader {
        TraderKind::Stake => Trader::Stake(addr(r.party_a).unwrap_or_default()),
        TraderKind::Wallet => Trader::Wallet(addr(r.party_a).unwrap_or_default()),
        TraderKind::NotEncodedByVenue => Trader::NotEncodedByVenue,
        TraderKind::Unknown => Trader::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::story::Story;

    fn ev(slot: u64, kind: Kind) -> StoryEvent {
        StoryEvent {
            slot,
            unix: 1_700_000_000 + slot,
            tx: vec![7u8; 32],
            unit: b"A".to_vec(),
            kind,
        }
    }

    fn story(events: Vec<StoryEvent>) -> Story {
        let mut slots: Vec<u64> = events.iter().map(|e| e.slot).collect();
        slots.dedup();
        Story {
            distinct_slots: slots.len(),
            events,
        }
    }

    fn round_trip(s: &Story, txs: Txs) -> Vec<StoryEvent> {
        let w = encode(s, "aa", 0, 100, true, txs);
        let bytes = to_bytes(&w).unwrap();
        decode(&from_bytes(&bytes).unwrap())
    }

    #[test]
    fn every_kind_round_trips() {
        let s = story(vec![
            ev(
                1,
                Kind::Mint {
                    to: Some("alice".into()),
                    amount: 100,
                },
            ),
            ev(
                2,
                Kind::Transfer {
                    from: "alice".into(),
                    to: "bob".into(),
                    amount: 40,
                },
            ),
            ev(
                3,
                Kind::Fill {
                    venue: "splash".into(),
                    party: Trader::Stake("stake1".into()),
                    amount: 5,
                    into_pool: true,
                },
            ),
            ev(
                4,
                Kind::Cancellation {
                    venue: "cswap".into(),
                    party: Trader::NotEncodedByVenue,
                    amount: 9,
                },
            ),
            ev(
                5,
                Kind::PoolState(PoolState {
                    venue: "splash".into(),
                    base: 1_000,
                    quote: Some(2_000),
                    quote_policy: None,
                    quote_name: None,
                    pricing: crate::observation::pricing::CONSTANT_PRODUCT.into(),
                }),
            ),
            ev(
                6,
                Kind::CurveState {
                    venue: "snek.fun".into(),
                    lovelace: 9_440_340_600,
                    tokens_left: 300_000_001,
                    progress: None,
                },
            ),
            ev(
                7,
                Kind::UnclaimedScript {
                    address: "addr1script".into(),
                    amount: 3,
                    has_datum: true,
                },
            ),
            ev(
                8,
                Kind::Ambiguous {
                    parties: 4,
                    amount: 12,
                },
            ),
            ev(
                9,
                Kind::Burn {
                    from: Some("bob".into()),
                    amount: 7,
                },
            ),
            ev(
                10,
                Kind::ArrivedFromBelowFloor {
                    to: "carol".into(),
                    amount: 2,
                },
            ),
            ev(
                11,
                Kind::BatchedFill {
                    venue: "minswap-v2".into(),
                    orders: 3,
                    amount: 50,
                },
            ),
            ev(
                12,
                Kind::Placement {
                    venue: "splash".into(),
                    party: Trader::Wallet("addr1w".into()),
                    amount: 6,
                },
            ),
        ]);
        assert_eq!(s.events.len(), Tag::ALL.len(), "one event per tag");
        assert_eq!(round_trip(&s, Txs::Include), s.events);
    }

    /// ⚠️ The distinction that stops every trade on a shared-order venue being
    /// attributed to one party.
    #[test]
    fn not_encoded_by_venue_survives_the_wire() {
        let s = story(vec![ev(
            1,
            Kind::Fill {
                venue: "cswap".into(),
                party: Trader::NotEncodedByVenue,
                amount: 1,
                into_pool: false,
            },
        )]);
        match &round_trip(&s, Txs::Include)[0].kind {
            Kind::Fill { party, .. } => assert_eq!(*party, Trader::NotEncodedByVenue),
            k => panic!("{k:?}"),
        }
    }

    /// ⚠️ An unstated quote reserve must not come back as 0 — a rate computed
    /// from it would price the pool at infinity.
    #[test]
    fn an_unstated_quote_reserve_stays_unstated() {
        let s = story(vec![ev(
            1,
            Kind::PoolState(PoolState {
                venue: "v".into(),
                base: 100,
                quote: None,
                quote_policy: None,
                quote_name: None,
                pricing: crate::observation::pricing::CONSTANT_PRODUCT.into(),
            }),
        )]);
        match &round_trip(&s, Txs::Include)[0].kind {
            Kind::PoolState(p) => assert_eq!(p.quote, None),
            k => panic!("{k:?}"),
        }
    }

    /// A bonding curve's reserves are real and its rate is NOT quote/base.
    #[test]
    fn the_pricing_model_survives_the_wire() {
        let s = story(vec![ev(
            1,
            Kind::PoolState(PoolState {
                venue: "snek.fun".into(),
                base: 1,
                quote: Some(2),
                quote_policy: None,
                quote_name: None,
                pricing: crate::observation::pricing::BONDING_CURVE.into(),
            }),
        )]);
        match &round_trip(&s, Txs::Include)[0].kind {
            Kind::PoolState(p) => {
                assert_eq!(p.pricing, crate::observation::pricing::BONDING_CURVE)
            }
            k => panic!("{k:?}"),
        }
    }

    /// Omitting hashes is a SIZE decision, not a data one: everything else
    /// still round-trips.
    #[test]
    fn omitting_transaction_hashes_keeps_the_rest_intact() {
        let s = story(vec![ev(
            5,
            Kind::Transfer {
                from: "alice".into(),
                to: "bob".into(),
                amount: 3,
            },
        )]);
        let got = round_trip(&s, Txs::Omit);
        assert!(got[0].tx.is_empty());
        assert_eq!(got[0].slot, 5);
        assert_eq!(got[0].kind, s.events[0].kind);
    }

    /// The whole point of interning: a wallet trading many times costs one
    /// string and many `u32`s.
    #[test]
    fn repeated_parties_are_interned_once() {
        let events: Vec<StoryEvent> = (0..50)
            .map(|i| {
                ev(
                    i,
                    Kind::Transfer {
                        from: "alice".into(),
                        to: "bob".into(),
                        amount: 1,
                    },
                )
            })
            .collect();
        let w = encode(&story(events), "aa", 0, 100, true, Txs::Include);
        assert_eq!(
            w.addresses.len(),
            2,
            "two distinct parties across 50 events"
        );
        assert_eq!(w.txs.len(), 1, "one distinct hash");
    }

    /// ⚠️ Postcard is positional. If this fires, a deployed consumer is
    /// decoding one kind as another.
    #[test]
    fn tags_are_append_only() {
        let expect = [
            "Mint",
            "Burn",
            "Transfer",
            "ArrivedFromBelowFloor",
            "Ambiguous",
            "Fill",
            "Placement",
            "Cancellation",
            "BatchedFill",
            "PoolState",
            "CurveState",
            "UnclaimedScript",
        ];
        for (i, t) in Tag::ALL.iter().enumerate() {
            let encoded = postcard::to_allocvec(t).unwrap();
            assert_eq!(encoded[0], i as u8, "{expect:?}[{i}] moved on the wire");
        }
    }

    #[test]
    fn a_wrong_version_is_refused_rather_than_decoded() {
        let s = story(vec![ev(
            1,
            Kind::Ambiguous {
                parties: 2,
                amount: 1,
            },
        )]);
        let mut bytes = to_bytes(&encode(&s, "aa", 0, 1, true, Txs::Include)).unwrap();
        bytes[0] = STORY_WIRE_VERSION + 1;
        assert!(from_bytes(&bytes).is_err());
    }

    #[test]
    fn an_empty_payload_is_refused() {
        assert!(from_bytes(&[]).is_err());
    }

    /// Slot deltas are non-negative and the first is absolute, so a consumer
    /// re-accumulates without needing the window's floor.
    #[test]
    fn slots_re_accumulate_from_deltas_alone() {
        let s = story(vec![
            ev(
                1_000,
                Kind::Ambiguous {
                    parties: 2,
                    amount: 1,
                },
            ),
            ev(
                1_000,
                Kind::Ambiguous {
                    parties: 3,
                    amount: 1,
                },
            ),
            ev(
                9_999,
                Kind::Ambiguous {
                    parties: 4,
                    amount: 1,
                },
            ),
        ]);
        let w = encode(&s, "aa", 0, 10_000, true, Txs::Omit);
        assert_eq!(
            w.rows.iter().map(|r| r.slot_delta).collect::<Vec<_>>(),
            vec![1_000, 0, 8_999]
        );
        assert_eq!(
            decode(&w).iter().map(|e| e.slot).collect::<Vec<_>>(),
            vec![1_000, 1_000, 9_999]
        );
    }
}
