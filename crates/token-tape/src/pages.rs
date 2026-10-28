//! Pages: the files a replay actually fetches.
//!
//! # Why
//!
//! A chunk is one day of one stream, which is the right unit for building
//! and for sealing, and the wrong one for a reader: a quiet stretch of a
//! token's life is hundreds of days of a few events each, so the page a
//! reader opens on was hundreds of requests. And to name who those events
//! are about, the reader needed the whole party table, a megabyte on a dense
//! token, before it could draw anything.
//!
//! A [`Page`] is a run of whole days cut from the token's GRID, with every
//! chunk of those days and the identity, role and closing balance of every
//! party the page needs: everyone its events name, and the largest holders
//! when it closes. One request opens a page, and it settles on its own.
//!
//! # The grid
//!
//! Cut from the token's FIRST day forward: a page closes once it holds
//! [`PAGE_BUDGET`] events. So a page, once closed, never changes as days are
//! added, and is cached for good; only the last page is open, and grows until
//! it closes. Every reader cuts the same grid from the index's chunks
//! ([`grid`]), and [`Index::pages`] lists it as written.

use crate::codec::{Reader, Writer};
use crate::identities::Role;
use crate::{Chunk, Error, PartyKey};

/// Events a page holds before it closes: about the 2,000 movements a JSON
/// page held, once fills are counted with their pool readings.
pub const PAGE_BUDGET: u32 = 3_000;

/// The largest holders at a page's close that it names even if its events
/// do not: what a holders view shows first.
pub const PAGE_TOP_HOLDERS: usize = 500;

const MAGIC_PAGE: &[u8; 4] = b"TTPG";

/// The token's days cut into pages, OLDEST FIRST, as `(first, last)` day.
///
/// `days` is every day with events and how many, in any order; a page
/// closes on the day that takes it to `budget`. The last page may be short:
/// it is the open one.
pub fn grid(days: impl IntoIterator<Item = (u32, u32)>, budget: u32) -> Vec<(u32, u32)> {
    let mut per_day: std::collections::BTreeMap<u32, u32> = Default::default();
    for (day, events) in days {
        *per_day.entry(day).or_default() += events;
    }
    let mut out = Vec::new();
    // The open page: its first day, its last so far, and its events.
    let mut open: Option<(u32, u32, u32)> = None;
    for (day, events) in per_day {
        let (first, _, total) = open.unwrap_or((day, day, 0));
        let total = total.saturating_add(events);
        if total >= budget {
            out.push((first, day));
            open = None;
        } else {
            open = Some((first, day, total));
        }
    }
    if let Some((first, last, _)) = open {
        out.push((first, last));
    }
    out
}

/// One page, as the index lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageInfo {
    pub first_day: u32,
    pub last_day: u32,
    pub events: u32,
    /// Encoded size.
    pub bytes: u32,
    /// [`page_digest`] of the encoded page: part of its file name, so a file
    /// name never names different bytes. A republish uploads only the pages
    /// whose bytes changed (the open one, mostly), and every page can be
    /// cached for good.
    pub digest: u64,
}

impl PageInfo {
    /// This page's file, relative to the token's directory.
    pub fn path(&self) -> String {
        page_path(self.first_day, self.digest)
    }
}

/// The index a replay reads: the same format as `index.bin`, with the pages
/// and none of the chunks, which only a builder needs. About 600 bytes where
/// the whole index is 34 KB on $NikePig.
pub const PAGES_INDEX_PATH: &str = "pages.bin";

/// A page's file name, relative to the token's directory: its first day and
/// its bytes' digest.
pub fn page_path(first_day: u32, digest: u64) -> String {
    format!("pages/{first_day}-{digest:016x}.bin")
}

/// FNV-1a over a page's encoded bytes: what names its file. Not a security
/// property, only "these bytes".
pub fn page_digest(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Somebody a page names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub party: u32,
    pub key: PartyKey,
    /// The handle it shows as, without the `$`.
    pub name: Option<String>,
    /// `None` for a person.
    pub role: Option<Role>,
    /// What it held when the page closed.
    pub closing: u64,
}

/// A page of whole days: its chunks, and who it names.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub first_day: u32,
    pub last_day: u32,
    /// People (not pools, venues or sinks) holding anything when it closes:
    /// the holder count, which the people it names fall short of.
    pub holders_total: u32,
    /// Ascending by party.
    pub people: Vec<Person>,
    pub chunks: Vec<Chunk>,
}

fn write_key(w: &mut Writer, k: &PartyKey) {
    match k {
        PartyKey::StakeKey(c) => {
            w.byte(0);
            w.raw(c);
        }
        PartyKey::StakeScript(c) => {
            w.byte(1);
            w.raw(c);
        }
        PartyKey::Address(a) => {
            w.byte(2);
            w.bytes(a);
        }
        PartyKey::Byron(s) => {
            w.byte(3);
            w.bytes(s.as_bytes());
        }
    }
}

fn read_key(r: &mut Reader) -> Result<PartyKey, Error> {
    let cred = |r: &mut Reader| -> Result<[u8; 28], Error> {
        r.take_raw(28)?
            .try_into()
            .map_err(|_| Error::Invalid("a credential is 28 bytes"))
    };
    Ok(match r.byte()? {
        0 => PartyKey::StakeKey(cred(r)?),
        1 => PartyKey::StakeScript(cred(r)?),
        2 => PartyKey::Address(r.bytes()?.to_vec()),
        3 => PartyKey::Byron(r.string()?),
        _ => return Err(Error::Invalid("a key kind this reader does not know")),
    })
}

impl Page {
    /// `"TTPG" format:u16`, the days, the holder count, the people (each the
    /// gap from the party before, the key, the name or empty, the role or 0,
    /// the closing balance), then each chunk's own encoding, length first.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new(MAGIC_PAGE);
        w.uint(u64::from(self.first_day));
        w.uint(u64::from(self.last_day - self.first_day));
        w.uint(u64::from(self.holders_total));
        w.uint(self.people.len() as u64);
        let mut last = 0u32;
        for p in &self.people {
            w.uint(u64::from(p.party - last));
            last = p.party;
            write_key(&mut w, &p.key);
            w.bytes(p.name.as_deref().unwrap_or("").as_bytes());
            w.byte(p.role.map_or(0, |r| r as u8));
            w.uint(p.closing);
        }
        w.uint(self.chunks.len() as u64);
        for c in &self.chunks {
            w.bytes(&c.encode());
        }
        w.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut r = Reader::open(bytes, MAGIC_PAGE)?;
        let first_day = r.u32()?;
        let last_day = first_day
            .checked_add(r.u32()?)
            .ok_or(Error::Invalid("a day overflows"))?;
        let holders_total = r.u32()?;
        let n = r.len()?;
        let mut people = Vec::with_capacity(n);
        let mut last = 0u32;
        for _ in 0..n {
            let party = last
                .checked_add(r.u32()?)
                .ok_or(Error::Invalid("a party number overflows"))?;
            last = party;
            let key = read_key(&mut r)?;
            let name = r.string()?;
            let role = match r.byte()? {
                0 => None,
                b => Some(Role::from_byte(b)?),
            };
            people.push(Person {
                party,
                key,
                name: (!name.is_empty()).then_some(name),
                role,
                closing: r.uint()?,
            });
        }
        let mut chunks = Vec::new();
        for _ in 0..r.len()? {
            chunks.push(Chunk::decode(r.bytes()?)?);
        }
        r.end()?;
        Ok(Self {
            first_day,
            last_day,
            holders_total,
            people,
            chunks,
        })
    }
}
