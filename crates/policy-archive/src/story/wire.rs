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
///
/// - **v2** — added [`StoryStream::markers`]. Appending a field to a struct
///   inside a `Vec` is breaking under postcard, and so is appending one to the
///   envelope, so this is a version and not a compatible addition.
/// - **v3** — added a pool-key table and [`StoryStream::pool_key`].
///   Without the pool INSTANCE a consumer cannot aggregate a venue that runs
///   several, and the first one that tried reported a venue's liquidity as
///   unpublished while the archive held 29,121 ADA of it.
///
/// # ⚠️ v4 — the change that made this stream FOLDABLE. Read before bumping.
///
/// **v3 had the ordinal machinery and interned the wrong thing in it.** Its
/// table held the instance KEY alone — the pool NFT's asset name, because that
/// was all a `PoolState` carried. The box folds by
/// [`crate::view::PoolKey`], which is `{address, key_policy, key_name}`, and
/// **all three are load-bearing**; that is not a guess, it is what the
/// archive tool's own pool table got wrong while it derived its own:
///
/// - dropping `key_policy` MERGED a cswap pool holding 200,000,000 base /
///   602 ₳ out of existence on policy `8fe8039d…` — it was never printed;
/// - the key was an `Option`, and `None` — "this venue publishes no instance
///   key" — was most of Minswap V2's rows, so every such pool collapsed into
///   one bucket and their reserves merged.
///
/// A client folding v3 therefore could not reproduce the price, which forced
/// the arrangement where the box folds and the client is handed a projection.
///
/// v4 keeps [`StoryStream::pool_key`] byte for byte and changes what it
/// indexes:
///
/// 1. the table becomes [`StoryStream::pools`] — pool IDENTITIES, with the
///    address interned against [`StoryStream::addresses`] rather than repeated
///    (a bech32 per pool state is 1,113 copies on $PERP alone);
/// 2. `pool_key` then ALWAYS resolves. Every pool has an address, so
///    [`NONE_IDX`] stops meaning "unidentifiable" and that case is gone;
/// 3. [`super::PoolState`] carries the ordinal, so `story::build` interns once
///    and the encoder stops being the only place that knows pool identity is
///    incomplete.
///
/// Two things the plan had not reckoned with, both found by building it:
///
/// - **`CurveState` needed an ordinal too.** `build` splits a bonding curve out
///   of the pool stream, so without one a client folding the stream could not
///   key the curve's reserves and the two folds disagreed on exactly the
///   liquidity `Spot::unpriceable` and `Spot::stale` are meant to report. It
///   gets [`StoryStream::curve_pool`] — its own cursor, not a share of
///   `pool_key`, so neither can drift past the other.
/// - **[`EventRow::venue`] is now unset on both tags.** A venue is a property
///   of the POOL and the table states it; a copy on the row is a copy that can
///   disagree.
///
/// [`decode`] returns a [`Story`] rather than bare events as a consequence: an
/// ordinal is meaningless without the table beside it. `view::PolicyView` can
/// now fold a `Kind` event, and its `observations_and_story_fold_identically`
/// asserts that folding the stream and folding the archive give the same
/// answer — the property this whole change was for.
pub const STORY_WIRE_VERSION: u8 = 4;

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

/// One pool's IDENTITY, stated once and referred to by ordinal thereafter.
///
/// # ⚠️ Why the address is an INDEX and not a string
///
/// A bech32 script address is ~60 bytes and a policy's pool states repeat it
/// constantly — $PERP alone carries 1,113 of them in one window. The stream
/// already interns every address it mentions for exactly this reason, so a pool
/// costs an index into that table rather than a copy of it, and a pool's
/// address is automatically the SAME string a `Transfer` to it would use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PoolRow {
    /// Into [`StoryStream::venues`].
    pub venue: u32,
    /// Into [`StoryStream::addresses`].
    pub address: u32,
    pub key_policy: Vec<u8>,
    /// The pool NFT's asset name. Empty where the venue mints none — which is
    /// not ambiguous, because `address` still separates those pools.
    pub key_name: Vec<u8>,
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
    /// Every pool and curve the rows refer to, stated once.
    ///
    /// ⚠️ **v4 replaced a table of instance KEYS with a table of pool
    /// IDENTITIES**, and that is what makes this stream foldable by a client
    /// rather than merely readable. See [`PoolRow`].
    pub pools: Vec<PoolRow>,
    /// Which pool each `PoolState` row is, indexing [`StoryStream::pools`].
    /// Parallel to the `PoolState` rows in order.
    ///
    /// ⚠️ **A venue runs MANY pools.** Grouping by venue alone and taking the
    /// latest sighting takes the latest of whichever pool moved last — which
    /// reported `splash: liquidity not published` on a policy whose splash
    /// pools held 29,121 ADA. Group by THIS, which is correct by construction.
    ///
    /// No [`NONE_IDX`]: every pool has an address, so every `PoolState`
    /// resolves. The `Option` this used to carry merged every pool on a venue
    /// that mints no pool NFT into one bucket.
    pub pool_key: Vec<u32>,
    /// Which curve each `CurveState` row is, indexing [`StoryStream::pools`].
    /// Parallel to the `CurveState` rows in order.
    ///
    /// Its own array rather than sharing `pool_key`, so neither cursor can
    /// drift past the other — the failure `pool_quote` already warns about,
    /// where reading one entry late describes pool N's reserves as pool N+1's.
    pub curve_pool: Vec<u32>,

    pub rows: Vec<EventRow>,

    /// Chapter markers — the few moments worth drawing WHEREVER they fall,
    /// including outside this window.
    ///
    /// ⚠️ Deliberately NOT merged into `rows`. The rows are an ordered window
    /// and every fold assumes that; splicing an out-of-frame launch into them
    /// would put an event at a slot the window does not cover and quietly
    /// break the contract that makes folding safe.
    pub markers: Vec<MarkerRow>,
}

/// A marker on the wire. Slots are ABSOLUTE — there are a handful of these and
/// delta-encoding against a window they may sit outside would be nonsense.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkerRow {
    pub slot: u64,
    /// `0` when unknown — a first mint's time is not in the observations.
    pub unix: u64,
    pub kind: MarkerTag,
    pub at: WhereTag,
}

/// ⚠️ APPEND ONLY, like every other tag here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarkerTag {
    FirstMint,  // 0
    Launch,     // 1
    Graduation, // 2
}

/// Which side of the window a marker falls on. **`Before`/`After` is the whole
/// point**: it is what lets a consumer draw an edge indicator instead of
/// reading an absence as "this never happened".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WhereTag {
    Before, // 0
    Within, // 1
    After,  // 2
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
    let mut pool_key: Vec<u32> = Vec::new();
    let mut curve_pool: Vec<u32> = Vec::new();
    let mut pool_pricing = Vec::new();
    let (mut last_slot, mut last_unix) = (0u64, 0u64);

    // ⚠️ INTERNED FIRST, before any row. The pool table's addresses and venues
    // index the same side tables the rows do, and building it up front means a
    // pool's address is the very same entry a `Transfer` to that address uses
    // rather than a second copy of the string.
    let pools: Vec<PoolRow> = story
        .pools
        .iter()
        .map(|p| PoolRow {
            venue: t.venue(&p.venue),
            address: t.address(&p.address),
            key_policy: p.key_policy.clone(),
            key_name: p.key_name.clone(),
        })
        .collect();

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
                // ⚠️ `r.venue` STAYS UNSET for this tag. The venue is a
                // property of the pool, and the pool table already states it —
                // a second copy on the row is a second copy that can disagree.
                // The decoder reads it from `pools[pool_key[n]]`.
                //
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
                pool_key.push(p.pool);
            }
            Kind::CurveState {
                pool,
                lovelace,
                tokens_left,
                progress: _,
            } => {
                r.tag = Tag::CurveState;
                // As `PoolState` above: venue comes from the pool table.
                curve_pool.push(*pool);
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
        pools,
        pool_key,
        curve_pool,
        rows,
        markers: story
            .markers
            .iter()
            .map(|m| MarkerRow {
                slot: m.slot,
                unix: m.unix,
                kind: match m.kind {
                    super::MarkerKind::FirstMint => MarkerTag::FirstMint,
                    super::MarkerKind::Launch => MarkerTag::Launch,
                    super::MarkerKind::Graduation => MarkerTag::Graduation,
                },
                at: match m.at {
                    super::Where::Before => WhereTag::Before,
                    super::Where::Within => WhereTag::Within,
                    super::Where::After => WhereTag::After,
                },
            })
            .collect(),
    }
}

/// Markers back as domain types.
pub fn decode_markers(s: &StoryStream) -> Vec<super::Marker> {
    s.markers
        .iter()
        .map(|m| super::Marker {
            slot: m.slot,
            unix: m.unix,
            kind: match m.kind {
                MarkerTag::FirstMint => super::MarkerKind::FirstMint,
                MarkerTag::Launch => super::MarkerKind::Launch,
                MarkerTag::Graduation => super::MarkerKind::Graduation,
            },
            at: match m.at {
                WhereTag::Before => super::Where::Before,
                WhereTag::Within => super::Where::Within,
                WhereTag::After => super::Where::After,
            },
        })
        .collect()
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

/// Re-inflate the columns into a [`Story`] — the inverse of [`encode`], for a
/// consumer that wants the domain types back.
///
/// # ⚠️ Returns a `Story`, not bare events
///
/// It used to return `Vec<StoryEvent>`, which was adequate while a `PoolState`
/// carried its own venue and key. From v4 a pool state is an ORDINAL, so the
/// events are meaningless without [`Story::pools`] — handing back events alone
/// would hand back something no consumer could resolve, and invite each of them
/// to reach into the raw `StoryStream` for the table.
pub fn decode(s: &StoryStream) -> Story {
    let (mut slot, mut unix) = (0u64, 0u64);
    let mut pool_at = 0usize;
    let mut curve_at = 0usize;
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
                // ⚠️ EVERY parallel column is read at `pool_at` BEFORE the
                // increment. Reading one after it silently shifts that column
                // by one pool — the reserves of pool N described as pool N+1,
                // which decodes cleanly and is entirely wrong.
                let q = s.pool_quote.get(pool_at).copied().unwrap_or(NONE_IDX);
                let pricing = s.pool_pricing.get(pool_at).copied().unwrap_or(0);
                let pool = s.pool_key.get(pool_at).copied().unwrap_or(NONE_IDX);
                pool_at += 1;
                let (qp, qn) = s.quote_units.get(q as usize).cloned().unwrap_or_default();
                Kind::PoolState(PoolState {
                    pool,
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
            Tag::CurveState => {
                let pool = s.curve_pool.get(curve_at).copied().unwrap_or(NONE_IDX);
                curve_at += 1;
                Kind::CurveState {
                    pool,
                    lovelace: r.extra,
                    tokens_left: r.extra2,
                    progress: None,
                }
            }
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
    Story {
        events: out,
        pools: s
            .pools
            .iter()
            .map(|p| super::PoolIdent {
                venue: venue_of(s, p.venue),
                address: s
                    .addresses
                    .get(p.address as usize)
                    .cloned()
                    .unwrap_or_default(),
                key_policy: p.key_policy.clone(),
                key_name: p.key_name.clone(),
            })
            .collect(),
        distinct_slots: s.distinct_slots as usize,
        markers: decode_markers(s),
    }
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

    /// A pool table wide enough for any ordinal the tests below use.
    fn pools(n: u32) -> Vec<super::super::PoolIdent> {
        (0..n)
            .map(|i| super::super::PoolIdent {
                venue: format!("venue{i}"),
                address: format!("addr{i}"),
                key_policy: vec![i as u8; 28],
                key_name: format!("pool{i}").into_bytes(),
            })
            .collect()
    }

    /// ⚠️ The table is sized from the events, never padded. A pool row interns
    /// an address, so a fixed-size table would add addresses to every test —
    /// including `repeated_parties_are_interned_once`, which counts them and
    /// went from 2 to 6 when this was `pools(4)`.
    fn story(events: Vec<StoryEvent>) -> Story {
        let mut slots: Vec<u64> = events.iter().map(|e| e.slot).collect();
        slots.dedup();
        let widest = events
            .iter()
            .filter_map(|e| match &e.kind {
                Kind::PoolState(p) => Some(p.pool),
                Kind::CurveState { pool, .. } => Some(*pool),
                _ => None,
            })
            .max();
        Story {
            distinct_slots: slots.len(),
            events,
            pools: widest.map(|w| pools(w + 1)).unwrap_or_default(),
            markers: Vec::new(),
        }
    }

    fn round_trip(s: &Story, txs: Txs) -> Vec<StoryEvent> {
        round_trip_story(s, txs).events
    }

    fn round_trip_story(s: &Story, txs: Txs) -> Story {
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
                    pool: 1,
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
                    pool: 2,
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

    /// ⚠️ THE PARALLEL COLUMNS MUST STAY IN STEP. Two pool states with
    /// different keys, quotes and models: if any column is read after the
    /// cursor advances, pool N comes back wearing pool N+1's identity — and it
    /// decodes cleanly, so nothing complains.
    #[test]
    fn parallel_pool_columns_stay_aligned_across_several_pools() {
        let pool = |ix: u32, base: i64, quote: Option<i64>, bonding: bool| {
            Kind::PoolState(PoolState {
                pool: ix,
                base,
                quote,
                quote_policy: None,
                quote_name: None,
                pricing: match bonding {
                    true => crate::observation::pricing::BONDING_CURVE.into(),
                    false => crate::observation::pricing::CONSTANT_PRODUCT.into(),
                },
            })
        };
        let s = story(vec![
            ev(1, pool(0, 100, Some(1_000), false)),
            ev(2, pool(1, 200, None, true)),
            ev(3, pool(2, 300, Some(3_000), false)),
        ]);
        // `Include`, so the whole-set comparison is meaningful: `Omit` drops
        // the hashes on purpose and the events would differ for that reason
        // rather than for anything this test is about.
        let got = round_trip(&s, Txs::Include);
        assert_eq!(got, s.events);
        // Spelled out, because an off-by-one here is invisible in a whole-set
        // comparison if the values happen to be similar.
        match (&got[0].kind, &got[1].kind, &got[2].kind) {
            (Kind::PoolState(a), Kind::PoolState(b), Kind::PoolState(c)) => {
                assert_eq!((a.pool, b.pool, c.pool), (0, 1, 2));
                assert_eq!((a.base, a.quote), (100, Some(1_000)));
                assert_eq!((b.base, b.quote), (200, None));
                assert_eq!(
                    b.pricing,
                    crate::observation::pricing::BONDING_CURVE,
                    "the bonding model must land on POOL_B, not a neighbour"
                );
            }
            other => panic!("expected three pool states, got {other:?}"),
        }
    }

    /// ⚠️ An unstated quote reserve must not come back as 0 — a rate computed
    /// from it would price the pool at infinity.
    #[test]
    fn an_unstated_quote_reserve_stays_unstated() {
        let s = story(vec![ev(
            1,
            Kind::PoolState(PoolState {
                pool: 0,
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
                pool: 3,
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

    /// Markers survive the wire with their SIDE intact — the field that lets a
    /// consumer tell "out of frame" from "never happened".
    #[test]
    fn markers_round_trip_with_which_side_they_fall_on() {
        use crate::story::{Marker, MarkerKind, Where};
        let s = story(vec![ev(
            9_000,
            Kind::Ambiguous {
                parties: 2,
                amount: 1,
            },
        )])
        .with_markers(vec![
            Marker {
                slot: 100,
                unix: 7,
                kind: MarkerKind::Launch,
                at: Where::Before,
            },
            Marker {
                slot: 9_000,
                unix: 9,
                kind: MarkerKind::Graduation,
                at: Where::Within,
            },
        ]);
        let w = encode(&s, "aa", 8_000, 10_000, true, Txs::Omit);
        let back = decode_markers(&from_bytes(&to_bytes(&w).unwrap()).unwrap());
        assert_eq!(back, s.markers);
        assert_eq!(back[0].at, Where::Before);
    }

    /// ⚠️ Markers are NOT rows. A fold over `rows` must never see an event at
    /// a slot the window does not cover.
    #[test]
    fn markers_do_not_leak_into_the_event_rows() {
        use crate::story::{Marker, MarkerKind, Where};
        let s = story(vec![ev(
            9_000,
            Kind::Ambiguous {
                parties: 2,
                amount: 1,
            },
        )])
        .with_markers(vec![Marker {
            slot: 100,
            unix: 7,
            kind: MarkerKind::Launch,
            at: Where::Before,
        }]);
        let w = encode(&s, "aa", 8_000, 10_000, true, Txs::Omit);
        assert_eq!(w.rows.len(), 1);
        assert_eq!(decode(&w).events.len(), 1);
        assert!(decode(&w).events.iter().all(|e| e.slot >= 8_000));
    }

    /// ⚠️ APPEND ONLY, same contract as the event tags.
    #[test]
    fn marker_tags_are_append_only() {
        for (i, t) in [
            MarkerTag::FirstMint,
            MarkerTag::Launch,
            MarkerTag::Graduation,
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(postcard::to_allocvec(t).unwrap()[0], i as u8);
        }
        for (i, t) in [WhereTag::Before, WhereTag::Within, WhereTag::After]
            .iter()
            .enumerate()
        {
            assert_eq!(postcard::to_allocvec(t).unwrap()[0], i as u8);
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
            decode(&w).events.iter().map(|e| e.slot).collect::<Vec<_>>(),
            vec![1_000, 1_000, 9_999]
        );
    }
}
