//! Who a token's parties are: keys and handles, by party number.
//!
//! Three files, for three questions a reader asks at very different rates:
//!
//! - **[`HolderTable`]** (`holders.bin`): every party, dense. Party `n` is at
//!   byte `HOLDER_HEADER + n * HOLDER_RECORD`, so a server answers "who are
//!   these parties" with range reads and no search. Never fetched whole by a
//!   client: on $NikePig it is 5.6 MB, almost all of it 81,000 airdropped
//!   Byron wallets the default view never shows.
//! - **[`Identities`]** (`active.bin`, `opening.bin`): a sparse, self-contained
//!   subset. `active.bin` is everyone who has ever traded or placed an order,
//!   plus the largest holders: MEASURED, 8,168 of $NikePig's 98,468 parties,
//!   enough to label every trade of its history. `opening.bin` is what the
//!   first screen shows, fetched with the first chunk.
//! - **[`Holding`]** (`holding.bin`): every current holder by balance, largest
//!   first, for resolving all holders when a reader asks.
//!
//! A key is the 29-byte record the party table uses; one too long for it
//! (an address with no stake part, a Byron address) is in an overflow
//! section. A handle is as the chain spells it (punycode for emoji), without
//! the `$`, at most [`NAME_BYTES`] bytes, zero-padded.

use crate::codec::{Reader, Writer};
use crate::{Error, FORMAT, PARTY_RECORD, PartyKey, PartyRecord, PartyTable, key_record};

/// The dense table, relative to the token's directory.
pub const HOLDERS_PATH: &str = "holders.bin";
/// Its overflow: the keys too long for a record.
pub const HOLDERS_OVERFLOW_PATH: &str = "holders-overflow.bin";
/// Everyone who has ever traded or placed an order, and the largest holders.
pub const ACTIVE_PATH: &str = "active.bin";
/// What the first screen shows: the largest holders and the latest traders.
pub const OPENING_PATH: &str = "opening.bin";
/// Every current holder by balance.
pub const HOLDING_PATH: &str = "holding.bin";

/// A handle's field: the longest held is 25 bytes (an emoji handle in
/// punycode) and the longest sub-handle 28, MEASURED over every handle held.
pub const NAME_BYTES: usize = 28;
/// Bytes before the dense table's first record: magic, format, record width,
/// a reserved byte, the count (`u32`, little-endian), four reserved bytes.
pub const HOLDER_HEADER: u64 = 16;
/// Bytes per dense record: the key's 29 and the handle's 28.
pub const HOLDER_RECORD: u64 = PARTY_RECORD + NAME_BYTES as u64;

const MAGIC_HOLDERS: &[u8; 4] = b"TTHT";
const MAGIC_HOLDERS_OVERFLOW: &[u8; 4] = b"TTHO";
const MAGIC_IDENTITIES: &[u8; 4] = b"TTID";
const MAGIC_HOLDING: &[u8; 4] = b"TTHD";

/// Who one party is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub key: PartyKey,
    /// The handle it shows as, without the `$`.
    pub name: Option<String>,
}

fn name_field(name: Option<&str>) -> Result<[u8; NAME_BYTES], Error> {
    let mut field = [0u8; NAME_BYTES];
    if let Some(n) = name {
        let b = n.as_bytes();
        if b.len() > NAME_BYTES || b.is_empty() || b.contains(&0) {
            return Err(Error::Invalid(
                "a handle empty, too long, or holding a zero byte",
            ));
        }
        field[..b.len()].copy_from_slice(b);
    }
    Ok(field)
}

fn read_name(field: &[u8]) -> Result<Option<String>, Error> {
    let end = field.iter().position(|b| *b == 0).unwrap_or(field.len());
    match end {
        0 => Ok(None),
        _ => String::from_utf8(field[..end].to_vec())
            .map(Some)
            .map_err(|_| Error::Invalid("a handle not UTF-8")),
    }
}

// ── the dense table ─────────────────────────────────────────────────────────

/// Every party, by number.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HolderTable {
    pub identities: Vec<Identity>,
}

impl HolderTable {
    /// The table from the party numbering and each party's handle.
    pub fn new(parties: &PartyTable, names: &[Option<String>]) -> Self {
        Self {
            identities: parties
                .keys
                .iter()
                .enumerate()
                .map(|(i, k)| Identity {
                    key: k.clone(),
                    name: names.get(i).cloned().flatten(),
                })
                .collect(),
        }
    }

    /// The byte range of party `id`'s record: what one range request asks.
    pub const fn record_range(id: u32) -> (u64, u64) {
        (HOLDER_HEADER + id as u64 * HOLDER_RECORD, HOLDER_RECORD)
    }

    /// The table and its overflow file. The overflow file is `"TTHO"
    /// format:u16` then the bytes; a record's offsets count from its start.
    pub fn encode(&self) -> Result<(Vec<u8>, Vec<u8>), Error> {
        let n = self.identities.len();
        let mut table = Vec::with_capacity(HOLDER_HEADER as usize + n * HOLDER_RECORD as usize);
        table.extend_from_slice(MAGIC_HOLDERS);
        table.extend_from_slice(&FORMAT.to_le_bytes());
        table.push(HOLDER_RECORD as u8);
        table.push(0);
        table.extend_from_slice(&(n as u32).to_le_bytes());
        table.extend_from_slice(&[0; 4]);
        let mut overflow = Vec::new();
        overflow.extend_from_slice(MAGIC_HOLDERS_OVERFLOW);
        overflow.extend_from_slice(&FORMAT.to_le_bytes());
        for id in &self.identities {
            table.extend_from_slice(&key_record(&id.key, &mut overflow));
            table.extend_from_slice(&name_field(id.name.as_deref())?);
        }
        Ok((table, overflow))
    }

    /// One record as a range request returns it: the key (or where in the
    /// overflow file the rest of it is) and the handle.
    pub fn record(rec: &[u8]) -> Result<(PartyRecord, Option<String>), Error> {
        if rec.len() != HOLDER_RECORD as usize {
            return Err(Error::Invalid("a holder record is 57 bytes"));
        }
        let (key, name) = rec.split_at(PARTY_RECORD as usize);
        Ok((PartyTable::record(key)?, read_name(name)?))
    }

    pub fn decode(table: &[u8], overflow: &[u8]) -> Result<Self, Error> {
        let head = table.get(..HOLDER_HEADER as usize).ok_or(Error::Short)?;
        if &head[..4] != MAGIC_HOLDERS || overflow.get(..4) != Some(MAGIC_HOLDERS_OVERFLOW) {
            return Err(Error::NotThisFile);
        }
        let format = u16::from_le_bytes([head[4], head[5]]);
        if format != FORMAT {
            return Err(Error::Format(format));
        }
        if u64::from(head[6]) != HOLDER_RECORD {
            return Err(Error::Invalid(
                "a holder record width this reader does not know",
            ));
        }
        let n = u32::from_le_bytes(head[8..12].try_into().expect("4")) as usize;
        let body = &table[HOLDER_HEADER as usize..];
        if body.len() != n * HOLDER_RECORD as usize {
            return Err(Error::Invalid(
                "the holder count and the table's length disagree",
            ));
        }
        let mut identities = Vec::with_capacity(n);
        for rec in body.as_chunks::<{ HOLDER_RECORD as usize }>().0 {
            let (key, name) = Self::record(rec)?;
            identities.push(Identity {
                key: key.resolve(overflow)?,
                name,
            });
        }
        Ok(Self { identities })
    }
}

// ── a sparse, self-contained subset ─────────────────────────────────────────

/// Some parties, with their numbers, readable on their own.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Identities {
    /// Ascending by party number, one each.
    pub entries: Vec<(u32, Identity)>,
}

impl Identities {
    /// `"TTID" format:u16 count`, then each record (`party:u32` little-endian,
    /// the 29-byte key, the 28-byte handle), then the overflow section as a
    /// length and its bytes, offsets counting from its start.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut w = Writer::new(MAGIC_IDENTITIES);
        w.uint(self.entries.len() as u64);
        let mut overflow = Vec::new();
        for (party, id) in &self.entries {
            w.raw(&party.to_le_bytes());
            w.raw(&key_record(&id.key, &mut overflow));
            w.raw(&name_field(id.name.as_deref())?);
        }
        w.bytes(&overflow);
        Ok(w.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_IDENTITIES)?;
        let n = r.len()?;
        let width = 4 + HOLDER_RECORD as usize;
        let records = r.take_raw(n.checked_mul(width).ok_or(Error::Short)?)?;
        let overflow = r.bytes()?;
        r.end()?;
        let mut entries = Vec::with_capacity(n);
        let mut last: Option<u32> = None;
        for rec in records.chunks_exact(width) {
            let party = u32::from_le_bytes(rec[..4].try_into().expect("4"));
            if last.is_some_and(|l| l >= party) {
                return Err(Error::Invalid("parties out of order or listed twice"));
            }
            last = Some(party);
            let (key, name) = HolderTable::record(&rec[4..])?;
            entries.push((
                party,
                Identity {
                    key: key.resolve(overflow)?,
                    name,
                },
            ));
        }
        Ok(Self { entries })
    }

    /// Who party `id` is, if this subset holds them.
    pub fn get(&self, id: u32) -> Option<&Identity> {
        self.entries
            .binary_search_by_key(&id, |(p, _)| *p)
            .ok()
            .map(|i| &self.entries[i].1)
    }
}

// ── current holders ─────────────────────────────────────────────────────────

/// Every party holding the token now, and how much, largest first.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Holding {
    pub balances: Vec<(u32, u64)>,
}

impl Holding {
    /// `"TTHD" format:u16 count`, then each party and balance as varints.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new(MAGIC_HOLDING);
        w.uint(self.balances.len() as u64);
        for (party, balance) in &self.balances {
            w.uint(u64::from(*party));
            w.uint(*balance);
        }
        w.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_HOLDING)?;
        let n = r.len()?;
        let mut balances = Vec::with_capacity(n);
        for _ in 0..n {
            balances.push((r.u32()?, r.uint()?));
        }
        r.end()?;
        Ok(Self { balances })
    }
}
