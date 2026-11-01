//! A token's history as compact binary files: what a replay reads.
//!
//! # Why
//!
//! The flow explorer's arena read JSON tapes built per request. On a dense
//! token they were the wrong shape. MEASURED on $NikePig in June 2024: one
//! page was 13 hours, 4,393 events and 4,200 holders, 1.26 MB of JSON (295 KB
//! compressed), 1.5 s to build, and about half a second of replay at a day a
//! second. Half its bytes were holders, three quarters of them never in its
//! events: they were there to settle opening balances.
//!
//! # The shape
//!
//! - **Parties** ([`PartyTable`]): every wallet the token's history touches,
//!   numbered in order of first appearance on chain from the token's first
//!   mint. That order is the chain's, so the numbering is deterministic: any
//!   build from the same history gives every party the same number, and a
//!   later build only adds numbers after the last. It holds IDENTITY only (a
//!   stake credential, or an address with no stake part). Names and
//!   services are applied where the history is drawn. The table is
//!   FIXED-WIDTH, so a reader holding a number fetches exactly that party's
//!   bytes with a range request, never a block of strangers.
//! - **Chunks** ([`Chunk`]): one UTC day of one [`Stream`], events in chain
//!   order, parties as numbers. Three streams, so a reader fetches only what
//!   it shows: the market (fills, pool and curve readings), transfers, and
//!   orders. A past day never changes.
//! - **Meta** ([`Meta`]): the venues and pools the chunks refer to by number.
//! - **Index** ([`Index`]): which chunks exist and what each holds, so a
//!   reader can fetch several ahead at once.
//!
//! Every file starts with four magic bytes and a format number. Integers are
//! unsigned LEB128 varints unless stated; a string or byte run is its length
//! as a varint, then the bytes.

mod codec;
mod identities;
mod pages;

pub use pages::{
    PAGE_BUDGET, PAGE_TOP_HOLDERS, PAGES_INDEX_PATH, Page, PageInfo, Person, grid, page_digest,
    page_path,
};

use codec::{Reader, Writer};

pub use codec::Error;
pub use identities::{
    ACTIVE_PATH, HOLDER_HEADER, HOLDER_RECORD, HOLDERS_OVERFLOW_PATH, HOLDERS_PATH, HOLDING_PATH,
    HolderTable, Holding, Identities, Identity, NAME_BYTES, OPENING_PATH, ROLES_PATH, Role, Roles,
    checkpoint_path,
};

/// The format of every file here. Bumped when any layout changes.
pub const FORMAT: u16 = 6;

const MAGIC_PARTIES: &[u8; 4] = b"TTPT";
const MAGIC_OVERFLOW: &[u8; 4] = b"TTPO";
const MAGIC_CHUNK: &[u8; 4] = b"TTCH";
const MAGIC_META: &[u8; 4] = b"TTMT";
const MAGIC_INDEX: &[u8; 4] = b"TTIX";

// ── parties ─────────────────────────────────────────────────────────────────

/// Who a party is: the identity a wallet's history is grouped by.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PartyKey {
    /// A stake KEY credential: an ordinary wallet, however many addresses.
    StakeKey([u8; 28]),
    /// A stake SCRIPT credential.
    StakeScript([u8; 28]),
    /// An address with no stake part (an enterprise or pointer address): the
    /// address bytes are the identity.
    Address(Vec<u8>),
    /// A Byron address, as the chain spells it.
    Byron(String),
}

/// The token's parties, numbered from its first mint.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartyTable {
    pub keys: Vec<PartyKey>,
}

/// Bytes before the first record of the party table: magic, format, the
/// record width, a reserved byte, the count (`u32`, little-endian), and four
/// reserved bytes.
pub const PARTY_HEADER: u64 = 16;

/// Bytes per party record: a tag, and 28 bytes.
pub const PARTY_RECORD: u64 = 29;

/// The party table's file name, relative to the token's directory.
pub const PARTIES_PATH: &str = "parties.bin";

/// Where the keys too long for a record go (addresses with no stake part,
/// Byron addresses), relative to the token's directory.
pub const OVERFLOW_PATH: &str = "parties-overflow.bin";

/// One record of the party table, as read on its own: a stake credential
/// whole, or where in the overflow file the rest of the key is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartyRecord {
    Key(PartyKey),
    /// An address with no stake part (`byron: false`) or a Byron address,
    /// at `offset .. offset + len` of the overflow file.
    Overflow {
        byron: bool,
        offset: u32,
        len: u32,
    },
}

impl PartyTable {
    /// The byte range of party `id`'s record in the table file: what a
    /// range request for one party asks for.
    pub const fn record_range(id: u32) -> (u64, u64) {
        (PARTY_HEADER + id as u64 * PARTY_RECORD, PARTY_RECORD)
    }

    /// The table file and its overflow file.
    ///
    /// A record is `tag:u8` and 28 bytes: tag 0 a stake key and 1 a stake
    /// script, the credential whole; tag 2 an address with no stake part and
    /// 3 a Byron address, the offset (`u32`, little-endian) and length (`u32`)
    /// of its bytes in the overflow file, then 20 zero bytes. The overflow
    /// file is `"TTPO" format:u16`, then the bytes, with offsets from its
    /// start.
    pub fn encode(&self) -> (Vec<u8>, Vec<u8>) {
        let mut table = Vec::with_capacity(PARTY_HEADER as usize + self.keys.len() * 29);
        table.extend_from_slice(MAGIC_PARTIES);
        table.extend_from_slice(&FORMAT.to_le_bytes());
        table.push(PARTY_RECORD as u8);
        table.push(0);
        table.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());
        table.extend_from_slice(&[0; 4]);
        let mut overflow = Vec::new();
        overflow.extend_from_slice(MAGIC_OVERFLOW);
        overflow.extend_from_slice(&FORMAT.to_le_bytes());
        for k in &self.keys {
            table.extend_from_slice(&key_record(k, &mut overflow));
        }
        (table, overflow)
    }

    /// One record, as a range request returns it.
    pub fn record(rec: &[u8]) -> Result<PartyRecord, Error> {
        let rec: &[u8; PARTY_RECORD as usize] = rec
            .try_into()
            .map_err(|_| Error::Invalid("a party record is 29 bytes"))?;
        let cred = || -> [u8; 28] { rec[1..].try_into().expect("28") };
        let word = |i: usize| u32::from_le_bytes(rec[i..i + 4].try_into().expect("4"));
        Ok(match rec[0] {
            0 => PartyRecord::Key(PartyKey::StakeKey(cred())),
            1 => PartyRecord::Key(PartyKey::StakeScript(cred())),
            t @ (2 | 3) => PartyRecord::Overflow {
                byron: t == 3,
                offset: word(1),
                len: word(5),
            },
            _ => return Err(Error::Invalid("a party tag this reader does not know")),
        })
    }

    /// The whole table back, from both files.
    pub fn decode(table: &[u8], overflow: &[u8]) -> Result<Self, Error> {
        let head = table.get(..PARTY_HEADER as usize).ok_or(Error::Short)?;
        if &head[..4] != MAGIC_PARTIES {
            return Err(Error::NotThisFile);
        }
        let format = u16::from_le_bytes([head[4], head[5]]);
        if format != FORMAT {
            return Err(Error::Format(format));
        }
        if u64::from(head[6]) != PARTY_RECORD {
            return Err(Error::Invalid(
                "a party record width this reader does not know",
            ));
        }
        if overflow.get(..4) != Some(MAGIC_OVERFLOW.as_slice()) {
            return Err(Error::NotThisFile);
        }
        let n = u32::from_le_bytes(head[8..12].try_into().expect("4")) as usize;
        let body = &table[PARTY_HEADER as usize..];
        if body.len() != n * PARTY_RECORD as usize {
            return Err(Error::Invalid(
                "the party count and the table's length disagree",
            ));
        }
        let mut keys = Vec::with_capacity(n);
        for rec in body.as_chunks::<{ PARTY_RECORD as usize }>().0 {
            keys.push(Self::record(rec)?.resolve(overflow)?);
        }
        Ok(Self { keys })
    }
}

/// A key as its 29-byte record, putting what does not fit at the end of
/// `overflow` (whose offsets count from its own start).
pub(crate) fn key_record(k: &PartyKey, overflow: &mut Vec<u8>) -> [u8; PARTY_RECORD as usize] {
    let mut rec = [0u8; PARTY_RECORD as usize];
    match k {
        PartyKey::StakeKey(c) => rec[1..].copy_from_slice(c),
        PartyKey::StakeScript(c) => {
            rec[0] = 1;
            rec[1..].copy_from_slice(c);
        }
        PartyKey::Address(_) | PartyKey::Byron(_) => {
            let (tag, bytes) = match k {
                PartyKey::Address(a) => (2, a.as_slice()),
                PartyKey::Byron(s) => (3, s.as_bytes()),
                _ => unreachable!("matched above"),
            };
            rec[0] = tag;
            rec[1..5].copy_from_slice(&(overflow.len() as u32).to_le_bytes());
            rec[5..9].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
            overflow.extend_from_slice(bytes);
        }
    }
    rec
}

impl PartyRecord {
    /// The whole key, reading what overflowed from `overflow`.
    pub fn resolve(self, overflow: &[u8]) -> Result<PartyKey, Error> {
        Ok(match self {
            PartyRecord::Key(k) => k,
            PartyRecord::Overflow { byron, offset, len } => {
                let bytes = overflow
                    .get(offset as usize..offset as usize + len as usize)
                    .ok_or(Error::Short)?;
                match byron {
                    false => PartyKey::Address(bytes.to_vec()),
                    true => PartyKey::Byron(
                        String::from_utf8(bytes.to_vec())
                            .map_err(|_| Error::Invalid("a Byron address not UTF-8"))?,
                    ),
                }
            }
        })
    }
}

// ── events ──────────────────────────────────────────────────────────────────

/// Which events a chunk holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Stream {
    /// Fills, batched fills, pool readings and curve readings: the price and
    /// the trades. What a market view needs and nothing else.
    Market,
    /// Tokens wallet to wallet, mints, burns, and arrivals from below the
    /// archive's floor.
    Transfers,
    /// Orders placed on and cancelled from order-book venues.
    Orders,
}

impl Stream {
    pub const ALL: [Stream; 3] = [Stream::Market, Stream::Transfers, Stream::Orders];

    pub fn name(self) -> &'static str {
        match self {
            Stream::Market => "market",
            Stream::Transfers => "transfers",
            Stream::Orders => "orders",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Stream::Market => 0,
            Stream::Transfers => 1,
            Stream::Orders => 2,
        }
    }

    const fn of_code(c: u8) -> Option<Stream> {
        match c {
            0 => Some(Stream::Market),
            1 => Some(Stream::Transfers),
            2 => Some(Stream::Orders),
            _ => None,
        }
    }
}

/// What happened. Amounts are raw units, unsigned: the kind says the
/// direction. A party is a [`PartyTable`] number; `None` where the chain does
/// not say (a fill on a shared order address the box could not trace).
#[derive(Debug, Clone, PartialEq)]
pub enum What {
    /// A swap. `sell`: the units went INTO the pool.
    Fill {
        party: Option<u32>,
        venue: u16,
        sell: bool,
        units: u64,
        /// A sell spent from an order contract: the units left the trader
        /// when the order was PLACED, so the fill moves nothing of theirs.
        escrowed: bool,
        /// The ADA the party paid or got, where the chain says it directly:
        /// an order book's fill, which no pool reading prices. `None` for a
        /// pool's fill, which the pool's own readings price.
        lovelace: Option<u64>,
    },
    /// Several orders into one pool at once. Never decomposed.
    Batch {
        venue: u16,
        orders: u32,
        units: u64,
    },
    /// A pool's reserves: the watched token's, and the quote side's where the
    /// venue publishes it. `pool` indexes [`Meta::pools`].
    Pool {
        pool: u16,
        base: u64,
        quote: Option<u64>,
    },
    /// A bonding curve's position. `progress_ppm` is millionths up its cap.
    Curve {
        pool: u16,
        lovelace: u64,
        units_left: u64,
        progress_ppm: Option<u32>,
    },
    Transfer {
        from: u32,
        to: u32,
        units: u64,
    },
    Mint {
        to: Option<u32>,
        units: u64,
    },
    Burn {
        from: Option<u32>,
        units: u64,
    },
    /// Units whose source is below the archive's floor.
    Arrived {
        to: u32,
        units: u64,
    },
    Placement {
        party: Option<u32>,
        venue: u16,
        units: u64,
    },
    Cancellation {
        party: Option<u32>,
        venue: u16,
        units: u64,
    },
}

impl What {
    /// The stream an event belongs to.
    pub fn stream(&self) -> Stream {
        match self {
            What::Fill { .. } | What::Batch { .. } | What::Pool { .. } | What::Curve { .. } => {
                Stream::Market
            }
            What::Transfer { .. }
            | What::Mint { .. }
            | What::Burn { .. }
            | What::Arrived { .. } => Stream::Transfers,
            What::Placement { .. } | What::Cancellation { .. } => Stream::Orders,
        }
    }

    const fn tag(&self) -> u8 {
        match self {
            What::Fill { sell: false, .. } => 0,
            What::Fill { sell: true, .. } => 1,
            What::Batch { .. } => 2,
            What::Pool { quote: None, .. } => 3,
            What::Pool { quote: Some(_), .. } => 4,
            What::Curve { .. } => 5,
            What::Transfer { .. } => 6,
            What::Mint { .. } => 7,
            What::Burn { .. } => 8,
            What::Arrived { .. } => 9,
            What::Placement { .. } => 10,
            What::Cancellation { .. } => 11,
        }
    }
}

/// One event, where it happened.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub slot: u64,
    /// Seconds since the epoch, as the block says.
    pub unix: u64,
    /// The event begins a new transaction (rather than being another leg of
    /// the one before it). Transaction hashes are not carried: a replay
    /// groups by transaction, it does not cite them.
    pub new_tx: bool,
    pub what: What,
}

/// One UTC day of one stream.
#[derive(Debug, Clone, PartialEq)]
pub struct Chunk {
    pub stream: Stream,
    /// Days since the epoch.
    pub day: u32,
    pub events: Vec<Event>,
}

impl What {
    /// The parties this event names.
    pub fn parties(&self) -> impl Iterator<Item = u32> {
        let (a, b) = match self {
            What::Fill { party, .. }
            | What::Placement { party, .. }
            | What::Cancellation { party, .. } => (*party, None),
            What::Transfer { from, to, .. } => (Some(*from), Some(*to)),
            What::Mint { to: p, .. } | What::Burn { from: p, .. } => (*p, None),
            What::Arrived { to, .. } => (Some(*to), None),
            What::Batch { .. } | What::Pool { .. } | What::Curve { .. } => (None, None),
        };
        a.into_iter().chain(b)
    }
}

/// An optional party as a varint: 0 is none, n+1 is party n.
fn opt(w: &mut Writer, p: Option<u32>) {
    w.uint(p.map_or(0, |p| u64::from(p) + 1));
}

fn read_opt(r: &mut Reader) -> Result<Option<u32>, Error> {
    Ok(match r.u32()? {
        0 => None,
        n => Some(n - 1),
    })
}

impl Chunk {
    /// `"TTCH" format:u16 stream:u8 day first_slot first_unix count`, then
    /// each event: a head byte (low 4 bits the kind, bit 4 "same slot as the
    /// last", bit 5 "a new transaction", bit 6 a fill's `escrowed`, bit 7 a
    /// fill's `lovelace` follows its units), the slot
    /// and time as deltas from the last event's unless the same slot, then
    /// the kind's fields.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new(MAGIC_CHUNK);
        w.byte(self.stream.code());
        w.uint(u64::from(self.day));
        let (first_slot, first_unix) = self.events.first().map_or((0, 0), |e| (e.slot, e.unix));
        w.uint(first_slot);
        w.uint(first_unix);
        w.uint(self.events.len() as u64);
        let (mut slot, mut unix) = (first_slot, first_unix);
        for e in &self.events {
            let same = e.slot == slot;
            let escrowed = matches!(e.what, What::Fill { escrowed: true, .. });
            let priced = matches!(
                e.what,
                What::Fill {
                    lovelace: Some(_),
                    ..
                }
            );
            let head = e.what.tag()
                | u8::from(same) << 4
                | u8::from(e.new_tx) << 5
                | u8::from(escrowed) << 6
                | u8::from(priced) << 7;
            w.byte(head);
            if !same {
                w.uint(e.slot - slot);
                w.uint(e.unix.saturating_sub(unix));
                (slot, unix) = (e.slot, e.unix);
            }
            match &e.what {
                What::Fill {
                    party,
                    venue,
                    units,
                    lovelace,
                    ..
                } => {
                    opt(&mut w, *party);
                    w.uint(u64::from(*venue));
                    w.uint(*units);
                    if let Some(l) = lovelace {
                        w.uint(*l);
                    }
                }
                What::Batch {
                    venue,
                    orders,
                    units,
                } => {
                    w.uint(u64::from(*venue));
                    w.uint(u64::from(*orders));
                    w.uint(*units);
                }
                What::Pool { pool, base, quote } => {
                    w.uint(u64::from(*pool));
                    w.uint(*base);
                    if let Some(q) = quote {
                        w.uint(*q);
                    }
                }
                What::Curve {
                    pool,
                    lovelace,
                    units_left,
                    progress_ppm,
                } => {
                    w.uint(u64::from(*pool));
                    w.uint(*lovelace);
                    w.uint(*units_left);
                    w.uint(progress_ppm.map_or(0, |p| u64::from(p) + 1));
                }
                What::Transfer { from, to, units } => {
                    w.uint(u64::from(*from));
                    w.uint(u64::from(*to));
                    w.uint(*units);
                }
                What::Mint { to: p, units } | What::Burn { from: p, units } => {
                    opt(&mut w, *p);
                    w.uint(*units);
                }
                What::Arrived { to, units } => {
                    w.uint(u64::from(*to));
                    w.uint(*units);
                }
                What::Placement {
                    party,
                    venue,
                    units,
                }
                | What::Cancellation {
                    party,
                    venue,
                    units,
                } => {
                    opt(&mut w, *party);
                    w.uint(u64::from(*venue));
                    w.uint(*units);
                }
            }
        }
        w.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_CHUNK)?;
        let stream = Stream::of_code(r.byte()?)
            .ok_or(Error::Invalid("a stream this reader does not know"))?;
        let day = r.u32()?;
        let (mut slot, mut unix) = (r.uint()?, r.uint()?);
        let n = r.len()?;
        let mut events = Vec::with_capacity(n.min(1 << 20));
        for _ in 0..n {
            let head = r.byte()?;
            if head & 0x10 == 0 {
                slot += r.uint()?;
                unix += r.uint()?;
            }
            let u16_ = |r: &mut Reader| -> Result<u16, Error> {
                u16::try_from(r.uint()?).map_err(|_| Error::Invalid("a number past 16 bits"))
            };
            let what = match head & 0x0f {
                t @ (0 | 1) => What::Fill {
                    party: read_opt(&mut r)?,
                    venue: u16_(&mut r)?,
                    units: r.uint()?,
                    sell: t == 1,
                    escrowed: head & 0x40 != 0,
                    lovelace: if head & 0x80 != 0 {
                        Some(r.uint()?)
                    } else {
                        None
                    },
                },
                2 => What::Batch {
                    venue: u16_(&mut r)?,
                    orders: r.u32()?,
                    units: r.uint()?,
                },
                t @ (3 | 4) => What::Pool {
                    pool: u16_(&mut r)?,
                    base: r.uint()?,
                    quote: if t == 4 { Some(r.uint()?) } else { None },
                },
                5 => What::Curve {
                    pool: u16_(&mut r)?,
                    lovelace: r.uint()?,
                    units_left: r.uint()?,
                    progress_ppm: match r.u32()? {
                        0 => None,
                        p => Some(p - 1),
                    },
                },
                6 => What::Transfer {
                    from: r.u32()?,
                    to: r.u32()?,
                    units: r.uint()?,
                },
                7 => What::Mint {
                    to: read_opt(&mut r)?,
                    units: r.uint()?,
                },
                8 => What::Burn {
                    from: read_opt(&mut r)?,
                    units: r.uint()?,
                },
                9 => What::Arrived {
                    to: r.u32()?,
                    units: r.uint()?,
                },
                t @ (10 | 11) => {
                    let (party, venue, units) = (read_opt(&mut r)?, u16_(&mut r)?, r.uint()?);
                    match t {
                        10 => What::Placement {
                            party,
                            venue,
                            units,
                        },
                        _ => What::Cancellation {
                            party,
                            venue,
                            units,
                        },
                    }
                }
                _ => return Err(Error::Invalid("an event kind this reader does not know")),
            };
            events.push(Event {
                slot,
                unix,
                new_tx: head & 0x20 != 0,
                what,
            });
        }
        r.end()?;
        Ok(Self {
            stream,
            day,
            events,
        })
    }
}

// ── meta and index ──────────────────────────────────────────────────────────

/// A pool or curve the chunks refer to by number.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pool {
    /// An index into [`Meta::venues`].
    pub venue: u16,
    /// The script address, bech32.
    pub address: String,
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
    /// The quote side's unit; empty for ADA.
    pub quote_policy: Vec<u8>,
    pub quote_name: Vec<u8>,
    /// A bonding curve rather than a constant-product pool: its rate is not
    /// quote over base.
    pub curve: bool,
}

/// What the chunks refer to by number.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Meta {
    /// 56 hex characters.
    pub policy: String,
    /// The asset name the history is of, raw.
    pub unit: Vec<u8>,
    pub venues: Vec<String>,
    pub pools: Vec<Pool>,
    /// What the token is called, where a registry says; empty where none
    /// does, and a reader falls back to the asset name.
    pub ticker: String,
    /// Display decimals, where a registry says. `None` is "render raw", not
    /// zero: decimals are not on chain.
    pub decimals: Option<u8>,
    /// Units minted less units burned over the history the files hold.
    pub supply: Option<u64>,
    /// The token's launch on a bonding curve, as `(slot, unix)`: where a
    /// replay marks it. `None` for a token that never had one.
    pub launch: Option<(u64, u64)>,
}

impl Meta {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new(MAGIC_META);
        w.bytes(self.policy.as_bytes());
        w.bytes(&self.unit);
        w.uint(self.venues.len() as u64);
        for v in &self.venues {
            w.bytes(v.as_bytes());
        }
        w.uint(self.pools.len() as u64);
        for p in &self.pools {
            w.uint(u64::from(p.venue));
            w.bytes(p.address.as_bytes());
            w.bytes(&p.key_policy);
            w.bytes(&p.key_name);
            w.bytes(&p.quote_policy);
            w.bytes(&p.quote_name);
            w.byte(u8::from(p.curve));
        }
        w.bytes(self.ticker.as_bytes());
        // Absent as 0, else the value plus one.
        w.uint(self.decimals.map_or(0, |d| u64::from(d) + 1));
        w.uint(self.supply.map_or(0, |s| s.saturating_add(1)));
        // Absent as a slot of 0, else the slot plus one, then the time.
        match self.launch {
            Some((slot, unix)) => {
                w.uint(slot.saturating_add(1));
                w.uint(unix);
            }
            None => w.uint(0),
        }
        w.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_META)?;
        let policy = r.string()?;
        let unit = r.bytes()?.to_vec();
        let venues = (0..r.len()?)
            .map(|_| r.string())
            .collect::<Result<Vec<_>, _>>()?;
        let mut pools = Vec::new();
        for _ in 0..r.len()? {
            pools.push(Pool {
                venue: u16::try_from(r.uint()?)
                    .map_err(|_| Error::Invalid("a venue past 16 bits"))?,
                address: r.string()?,
                key_policy: r.bytes()?.to_vec(),
                key_name: r.bytes()?.to_vec(),
                quote_policy: r.bytes()?.to_vec(),
                quote_name: r.bytes()?.to_vec(),
                curve: r.byte()? != 0,
            });
        }
        let ticker = r.string()?;
        let decimals = match r.uint()? {
            0 => None,
            d => Some(u8::try_from(d - 1).map_err(|_| Error::Invalid("decimals past 8 bits"))?),
        };
        let supply = r.uint()?.checked_sub(1);
        let launch = match r.uint()? {
            0 => None,
            slot => Some((slot - 1, r.uint()?)),
        };
        r.end()?;
        Ok(Self {
            policy,
            unit,
            venues,
            pools,
            ticker,
            decimals,
            supply,
            launch,
        })
    }
}

/// One chunk, as the index lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkInfo {
    pub stream: Stream,
    pub day: u32,
    pub first_slot: u64,
    pub last_slot: u64,
    pub events: u32,
    /// Encoded size.
    pub bytes: u32,
}

/// What exists: every chunk, and how far the party table runs.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Index {
    pub parties: u32,
    /// By stream, then by day.
    pub chunks: Vec<ChunkInfo>,
    /// The grid's pages, oldest first: what a replay fetches (see `pages`).
    pub pages: Vec<PageInfo>,
}

impl Index {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new(MAGIC_INDEX);
        w.uint(u64::from(self.parties));
        w.uint(self.chunks.len() as u64);
        for c in &self.chunks {
            w.byte(c.stream.code());
            w.uint(u64::from(c.day));
            w.uint(c.first_slot);
            w.uint(c.last_slot - c.first_slot);
            w.uint(u64::from(c.events));
            w.uint(u64::from(c.bytes));
        }
        w.uint(self.pages.len() as u64);
        for p in &self.pages {
            w.uint(u64::from(p.first_day));
            w.uint(u64::from(p.last_day - p.first_day));
            w.uint(u64::from(p.events));
            w.uint(u64::from(p.bytes));
            w.uint(p.digest);
        }
        w.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_INDEX)?;
        let parties = r.u32()?;
        let mut chunks = Vec::new();
        for _ in 0..r.len()? {
            let stream = Stream::of_code(r.byte()?)
                .ok_or(Error::Invalid("a stream this reader does not know"))?;
            let day = r.u32()?;
            let first_slot = r.uint()?;
            chunks.push(ChunkInfo {
                stream,
                day,
                first_slot,
                last_slot: first_slot + r.uint()?,
                events: r.u32()?,
                bytes: r.u32()?,
            });
        }
        let mut pages = Vec::new();
        for _ in 0..r.len()? {
            let first_day = r.u32()?;
            pages.push(PageInfo {
                first_day,
                last_day: first_day
                    .checked_add(r.u32()?)
                    .ok_or(Error::Invalid("a day overflows"))?,
                events: r.u32()?,
                bytes: r.u32()?,
                digest: r.uint()?,
            });
        }
        r.end()?;
        Ok(Self {
            parties,
            chunks,
            pages,
        })
    }
}

/// A chunk's file name, relative to the token's directory.
pub fn chunk_path(stream: Stream, day: u32) -> String {
    format!("{}/{day}.bin", stream.name())
}

#[cfg(test)]
mod tests;
