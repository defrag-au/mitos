//! The read side: an immutable artifact, mapped, answering the questions the
//! module surface asks.
//!
//! # Every hit is confirmed
//!
//! An entry is 12 bytes — a 64-bit prefix and a row offset — so it is a
//! **candidate**, and the row it points at is what decides. The row holds the
//! outref, the address and the interned policy ids in full, so:
//!
//! | query | what the row has to corroborate |
//! |---|---|
//! | `utxo` | its own 36-byte outref equals the key asked for |
//! | `by_address` | its address bytes equal the query's, exactly |
//! | `by_payment_cred` | its derived credential equals the query's, and is a *payment* one |
//! | `by_policy` | its resolved policy equals the query's, exactly |
//!
//! This is `tx-index`'s discipline — "8 bytes of the hash, re-hash the located
//! body" — one level in: the narrow key narrows, the record decides. A prefix
//! collision costs one extra row read and never a wrong answer.
//!
//! Note that `by_policy`'s confirm is *exact* rather than the prefix comparison
//! the verifier uses: the row carries an interned id, so resolving that id
//! through `policy.tbl` gives 28 bytes to compare, and nothing has to be taken on
//! trust. `verify` compares prefixes because it is checking entries, not
//! answering queries.
//!
//! Confirmation happens **here** rather than being handed to the caller as a
//! stream of candidates, because the row is already in hand — a caller filtering
//! its own candidates would read every row a second time to reject the ones this
//! module just read.
//!
//! # Why a mapping
//!
//! The artifact is 2.2 GB and the box it has to fit on is 8 GB, so the live set
//! cannot be held anon — and it should not be: the page cache is evictable and a
//! heap is not (`FOOTPRINT` §4.1). A lookup touches one directory page plus one
//! or two entry pages and reads the row in place, so the resident set is the
//! working set and nothing more. Publishing swaps files by `rename`
//! (`format`), so a mapping held across a rebuild keeps addressing the inode it
//! opened and no reader re-opens anything.

use std::ops::Range;
use std::path::{Path, PathBuf};

#[cfg(feature = "mmap")]
use anyhow::Context;
use anyhow::{Result, bail};

use crate::address::{self, Kind};
use crate::format::{self, Directory, Meta, PolicyTable, split};
// The published file names are only needed by the `open(&Path)` constructors,
// which are the mmap path. A remote reader gets them from `publish::PARTED`.
#[cfg(feature = "mmap")]
use crate::format::{
    DIRECTORY_ADDR, DIRECTORY_OUTREF, DIRECTORY_POLICY, POLICY_FILE, ROWS_FILE, TABLE_ADDR,
    TABLE_OUTREF, TABLE_POLICY,
};
use crate::row::Row;
// `RowRead` is the error of `Base::row`, which is the zero-copy borrow only
// `MmapSource` can honour.
#[cfg(feature = "mmap")]
use crate::row::RowRead;
#[cfg(feature = "mmap")]
use crate::source::MmapSource;
use crate::source::Source;

/// An outref — a transaction hash and an output index. The live set's key, and
/// the only thing an enumeration of it needs to return: a caller that wants the
/// output reads it back by this, which is what the wasm surface does.
pub type OutRef = ([u8; 32], u32);

/// The most refs one page will return.
///
/// The wasm surface is documented as "hard-capped at 100K refs"; the cap lives
/// here as well because this is the layer that could return a million, and a
/// caller that forgets is the one that pays for it.
pub const MAX_PAGE: usize = 100_000;

/// Where an enumeration stopped, so the next call can continue.
///
/// Opaque on purpose: it is an entry index within one ordering, and a caller that
/// built one itself could resume in the wrong place and silently skip rows. The
/// prefix is carried so a cursor cannot be replayed against a different key —
/// the mistake that would otherwise look like a missing page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cursor {
    prefix: u64,
    at: u64,
}

/// One page of refs, and how to get the next.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Page {
    pub refs: Vec<OutRef>,
    /// `None` when the run is exhausted. `Some` even when `refs` did not reach
    /// the limit, if the page ended on candidates the rows rejected — a caller
    /// that stopped on a short or empty page rather than on `None` would miss
    /// rows.
    pub next: Option<Cursor>,
}

/// An immutable artifact, read through some [`Source`].
///
/// Defaults to [`DefaultSource`](crate::source::DefaultSource) — `MmapSource`
/// with the `mmap` feature on — so every existing caller, and the box's measured
/// 1223 ns lookup, is unaffected by the seam existing.
pub struct Base<S = crate::source::DefaultSource> {
    dir: PathBuf,
    meta: Meta,
    rows: S,
    outref: Table<S>,
    policy: Table<S>,
    addr: Table<S>,
    policies: PolicyTable<S>,
}

struct Table<S = crate::source::DefaultSource> {
    entries: S,
    dir: Directory<S>,
    count: u64,
}

#[cfg(feature = "mmap")]
impl Table<MmapSource> {
    fn open(dir: &Path, table: &str, directory: &str) -> Result<Table<MmapSource>> {
        let entries = MmapSource::open(&dir.join(table))?;
        let dir_map = Directory::open(dir, directory)?;
        Table::from_sources(entries, dir_map, table)
    }
}

impl<S: Source> Table<S> {
    /// Take the entry array and its directory, and check the directory really
    /// partitions the array (`B5`).
    ///
    /// ⚠️ `validate` reads all 2^21 buckets and probes the array per non-empty
    /// bucket — fine over a mapping at 73 ms, impossible remotely (§4.2). A
    /// remote reader must construct this some other way; the check has to move
    /// to publish time, which is what the attestation is for.
    ///
    /// Which is why this is `mmap`-gated rather than generic-and-unused: it is
    /// the *local* constructor by definition, since the one thing it does beyond
    /// reading a header is the check that only a mapping can afford.
    #[cfg(feature = "mmap")]
    fn from_sources(entries: S, dir: Directory<S>, what: &str) -> Result<Table<S>> {
        let count = format::check_array_header_src(&entries, what, 12)?;
        dir.validate(
            |i| format::entry_at_src(&entries, i as u64).map(|e| e.0),
            count,
        )?;
        Ok(Table {
            entries,
            dir,
            count,
        })
    }

    fn entry(&self, i: u64) -> Result<(u64, u32)> {
        format::entry_at_src(&self.entries, i)
    }

    /// The entries carrying `prefix`, as a contiguous range.
    ///
    /// Binary search inside the bucket rather than a scan of it: a bucket is ~5
    /// entries on average, but a busy wallet's credential can be tens of
    /// thousands all sharing one prefix, and those are a page to return rather
    /// than five entries to skip past.
    fn run(&self, prefix: u64) -> Result<Range<u64>> {
        let bucket = self.dir.range_for(prefix)?;
        let end = (bucket.end as u64).min(self.count);
        let start = (bucket.start as u64).min(end);
        let first_at_or_after = |key: u64| -> Result<u64> {
            let (mut a, mut b) = (start, end);
            while a < b {
                let m = a + (b - a) / 2;
                if self.entry(m)?.0 < key {
                    a = m + 1;
                } else {
                    b = m;
                }
            }
            Ok(a)
        };
        let lo = first_at_or_after(prefix)?;
        let hi = if prefix == u64::MAX {
            end
        } else {
            first_at_or_after(prefix + 1)?
        };
        Ok(lo..hi.max(lo))
    }
}

#[cfg(feature = "mmap")]
impl Base<MmapSource> {
    /// Map an artifact. Validates what it can cheaply: every file's version, the
    /// counts each ordering claims against `book.meta`, and that each directory
    /// partitions its array. A corrupt artifact is refused here rather than
    /// answering wrongly later.
    pub fn open(dir: &Path) -> Result<Base<MmapSource>> {
        let meta = Meta::open(dir)?;
        if meta.layout_version != format::LAYOUT_VERSION {
            bail!(
                "book.meta is layout_version {}; this build reads {}",
                meta.layout_version,
                format::LAYOUT_VERSION
            );
        }
        let rows_path = dir.join(ROWS_FILE);
        let rows = MmapSource::open(&rows_path)?;
        if rows.size() != meta.rows_bytes {
            bail!(
                "{} is {} B; book.meta claims {}",
                rows_path.display(),
                rows.size(),
                meta.rows_bytes
            );
        }
        let outref = Table::open(dir, TABLE_OUTREF, DIRECTORY_OUTREF)?;
        if outref.count != meta.by_outref_entries || outref.count != meta.rows {
            bail!(
                "{} holds {} entries; book.meta says {} entries and {} rows",
                TABLE_OUTREF,
                outref.count,
                meta.by_outref_entries,
                meta.rows
            );
        }
        // The two row-permuted orderings must agree with the row count. Checked
        // here as well as by `verify`, because a consumer that never runs the
        // verifier is the one that would serve a short index as if it were whole.
        let addr = Table::open(dir, TABLE_ADDR, DIRECTORY_ADDR)?;
        if addr.count != meta.rows {
            bail!(
                "{} holds {} entries against {} rows — not a permutation of them",
                TABLE_ADDR,
                addr.count,
                meta.rows
            );
        }
        let policy = Table::open(dir, TABLE_POLICY, DIRECTORY_POLICY)?;
        let policies = PolicyTable::open(dir).with_context(|| format!("reading {POLICY_FILE}"))?;
        if policies.len() as u64 != meta.policies {
            bail!(
                "{POLICY_FILE} holds {} policies; book.meta says {}",
                policies.len(),
                meta.policies
            );
        }
        Ok(Base {
            dir: dir.to_path_buf(),
            meta,
            rows,
            outref,
            policy,
            addr,
            policies,
        })
    }

    /// The row at a byte offset, **borrowed from the mapping**.
    ///
    /// Only sound here, and that is the whole reason the concrete impl exists:
    /// `MmapSource::Bytes<'a>` *is* `&'a [u8]`, so the borrow outlives the call.
    /// Over any other source the bytes live in a guard that dies with the call,
    /// which is why the generic surface is [`Base::with_row`].
    ///
    /// Keeping this is not a convenience. `LIVE_BOOK.md` §Decided rests on spend
    /// resolution confirming from memory — over ~138 M resolutions the difference
    /// between a borrow and a copy is **69 minutes versus 77 hours** — so the box
    /// must not pay for the seam existing.
    pub fn row(&self, offset: u32) -> Result<Row<'_>, RowRead> {
        let at = offset as usize;
        let bytes = self.rows.bytes();
        if at >= bytes.len() {
            return Err(RowRead::OutOfRange);
        }
        Row::parse(&bytes[at..])
    }

    /// Is this output live, and what does it say — zero-copy.
    ///
    /// The generic equivalent is [`Base::with_utxo`]; see [`Base::row`] for why
    /// this one can hand the row back.
    pub fn utxo(&self, tx: &[u8; 32], index: u32) -> Result<Option<Row<'_>>> {
        match self.find_entry(tx, index)? {
            Some((_, offset)) => Ok(self.row(offset).ok()),
            None => Ok(None),
        }
    }
}

impl<S: Source> Base<S> {
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Live rows. Also the entry count of `by_outref` and `by_addr`.
    pub fn rows(&self) -> u64 {
        self.meta.rows
    }

    pub fn policies(&self) -> &PolicyTable<S> {
        &self.policies
    }

    /// The row at a byte offset, handed to `f` while borrowed.
    ///
    /// `Ok(None)` means the bytes there are not a row — a prefix collision that
    /// pointed somewhere useless, which every confirm treats as "not a match".
    /// `Err` means the *read* failed, which is a different thing and must not be
    /// mistaken for it.
    ///
    /// ⚠️ **`MAX_ROW` is a speculative over-read, not a limit on the format.**
    /// A row's length is inside its header, so a remote source cannot know how
    /// much to fetch without two dependent round trips. §4.5 measures the median
    /// row at 166 B and the longest in the whole store at 7,689 B, so 8 KiB
    /// covers every row mainnet holds with room to spare — and `Row::parse`
    /// still refuses a row that runs past what it was given, so a fixture that
    /// exceeded it would fail loudly rather than truncate.
    pub fn with_row<T>(&self, offset: u32, f: impl FnOnce(Row<'_>) -> T) -> Result<Option<T>> {
        const MAX_ROW: usize = 8 * 1024;
        if u64::from(offset) >= self.rows.size() {
            return Ok(None);
        }
        let bytes = self.rows.read_upto(u64::from(offset), MAX_ROW)?;
        Ok(Row::parse(&bytes).ok().map(f))
    }

    /// Is this output live, and what does it say — handed to `f` while borrowed.
    ///
    /// One directory read, one entry read, one row read, and the row's own
    /// 36-byte outref is what decides, so a prefix collision cannot return
    /// somebody else's output.
    ///
    /// ⚠️ **Scoped rather than returning `Row<'_>`, and that is forced.** Over a
    /// mapping the row is a borrow of something that outlives the call; over any
    /// remote source it is a borrow of a guard that dies with it. A signature
    /// that returned the row could only ever have been honest for one
    /// implementation. `Base<MmapSource>::utxo` keeps the zero-copy shape for the
    /// box, where §Decided's 69-minutes-versus-77-hours argument lives.
    pub fn with_utxo<T>(
        &self,
        tx: &[u8; 32],
        index: u32,
        f: impl FnOnce(Row<'_>) -> T,
    ) -> Result<Option<T>> {
        match self.find_entry(tx, index)? {
            Some((_, offset)) => self.with_row(offset, f),
            None => Ok(None),
        }
    }

    /// The `by_outref` entry a live output occupies — a **dense id** for the row,
    /// in `0..rows()`.
    ///
    /// [`Base::row`] takes a byte offset into a variable-length arena, which is
    /// not an index: two rows at neighbouring offsets may be thousands of bytes
    /// apart. A caller that has to keep *one bit per base row* — a follower
    /// marking what a later chunk has spent — needs the dense one. It is also the
    /// more stable of the two, since it is derived from the row's own outref
    /// rather than from where the row happened to be written.
    pub fn outref_entry(&self, tx: &[u8; 32], index: u32) -> Result<Option<u64>> {
        Ok(self.find_entry(tx, index)?.map(|(entry, _)| entry))
    }

    /// The (entry index, row offset) of a live output, or `None`.
    ///
    /// Returns the *offset* rather than the row so the borrow ends here — see
    /// [`Base::with_utxo`]. A row that will not parse is skipped rather than
    /// reported, which is what makes a prefix collision cost a read instead of
    /// answering with somebody else's output; a read that *fails* is an error,
    /// because that is a different thing.
    fn find_entry(&self, tx: &[u8; 32], index: u32) -> Result<Option<(u64, u32)>> {
        let (prefix, _) = split(tx, index);
        for i in self.outref.run(prefix)? {
            let (p, offset) = self.outref.entry(i)?;
            if p != prefix {
                break;
            }
            if self
                .with_row(offset, |row| row.outref() == (*tx, index))?
                .unwrap_or(false)
            {
                return Ok(Some((i, offset)));
            }
        }
        Ok(None)
    }

    /// Every live output at exactly this address.
    ///
    /// Takes the raw address bytes, not a bech32 string: the artifact stores the
    /// bytes, and a bech32 decode per query would be paid to save each caller
    /// one. A caller holding a string decodes it once
    /// (`pallas_addresses::Address::from_bech32`) and passes `to_vec()`, which is
    /// also how the entry was keyed.
    pub fn by_address(&self, address: &[u8], after: Option<Cursor>, limit: usize) -> Result<Page> {
        let Some(cred) = address::credential(address) else {
            // pallas would not call these bytes an address, so no row can match
            // them. An empty page, not an error: the honest answer to "what is at
            // this address" for a non-address is "nothing".
            return Ok(Page::default());
        };
        self.enumerate(&self.addr, cred.prefix(), after, limit, |row| {
            Ok(row.address() == address)
        })
    }

    /// Every live output whose address shares this **payment** credential,
    /// whatever its staking part.
    ///
    /// The other two kinds are excluded on purpose. A reward address (types
    /// 14–15) carries a *stake* credential, and returning it for a payment query
    /// would be a wrong answer rather than a candidate — the caller asked for
    /// outputs a payment credential controls. A Byron row is excluded for the
    /// same reason: a bootstrap address's `root` is its identity, not a payment
    /// credential, and this primitive is documented as "28-byte payment-key or
    /// payment-script hash".
    pub fn by_payment_cred(
        &self,
        cred: &[u8; 28],
        after: Option<Cursor>,
        limit: usize,
    ) -> Result<Page> {
        let prefix = u64::from_le_bytes(cred[0..8].try_into().expect("8 bytes"));
        self.enumerate(&self.addr, prefix, after, limit, |row| {
            Ok(matches!(
                address::credential(row.address()),
                Some(c) if c.kind == Kind::Payment && c.bytes() == cred
            ))
        })
    }

    /// Every live output holding at least one asset of this policy.
    ///
    /// One entry per `(row, policy)`, so a row holding two assets of the policy is
    /// returned once. The confirm resolves each of the row's interned ids through
    /// the table and compares 28 bytes, which is why this needs no id lookup for
    /// the query: the entry that got us here was already keyed on the policy's
    /// prefix, and the row is what makes it exact.
    pub fn by_policy(
        &self,
        policy: &[u8; 28],
        after: Option<Cursor>,
        limit: usize,
    ) -> Result<Page> {
        let prefix = u64::from_le_bytes(policy[0..8].try_into().expect("8 bytes"));
        self.enumerate(&self.policy, prefix, after, limit, |row| {
            // Not `.any()`: resolving an interned id is a read, so it can fail,
            // and a failure must propagate rather than read as "no match".
            for a in row.assets() {
                if self.policies.get(a.policy)? == Some(*policy) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// The shared shape of the three enumerations: walk the run for `prefix` from
    /// `after`, confirm each candidate against its row, and stop once `limit`
    /// confirmed refs are collected or the run is exhausted.
    ///
    /// The cursor is an entry index, so a page that confirmed nothing still moves
    /// forward — which is what stops a caller paging over a long run of rejected
    /// candidates from looping on the same page forever.
    /// The shared shape of the three enumerations.
    ///
    /// `confirm` is **fallible** because one of the three needs it to be:
    /// `by_policy` resolves the row's interned id through `policy.tbl`, which is
    /// a read and can therefore fail. Collapsing that into `false` would report a
    /// failed read as "this row does not hold that policy" — a wrong answer
    /// wearing the shape of a right one.
    fn enumerate(
        &self,
        table: &Table<S>,
        prefix: u64,
        after: Option<Cursor>,
        limit: usize,
        confirm: impl Fn(Row<'_>) -> Result<bool>,
    ) -> Result<Page> {
        let run = table.run(prefix)?;
        let mut at = match after {
            Some(c) => {
                if c.prefix != prefix {
                    bail!("a cursor for prefix {} replayed against {prefix}", c.prefix);
                }
                c.at.max(run.start)
            }
            None => run.start,
        };
        let limit = limit.min(MAX_PAGE);
        let mut refs = Vec::new();
        while at < run.end && refs.len() < limit {
            let (p, offset) = table.entry(at)?;
            at += 1;
            if p != prefix {
                // The bucket held a neighbouring prefix: the run is over, and the
                // cursor must not be left mid-bucket to be resumed into it.
                at = run.end;
                break;
            }
            // A row that will not parse is skipped; a read or a confirm that
            // FAILS propagates. `with_row` keeps the borrow inside the call.
            let hit = self.with_row(offset, |row| {
                confirm(row).map(|ok| ok.then(|| row.outref()))
            })?;
            if let Some(outref) = hit.transpose()?.flatten() {
                refs.push(outref);
            }
        }
        let next = (at < run.end).then_some(Cursor { prefix, at });
        Ok(Page { refs, next })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{META_FILE, Perm, write_ordering, write_policy_tbl};
    use crate::row::{Asset, Datum, ROW_HEADER, write_asset_record, write_header};

    /// `base_slot` is the one optional field in `book.meta`, and the three states it
    /// has to keep apart are: **never written** (an artifact that does not know where
    /// it is), **written** (a stamped one), and **written and then repaired** — a
    /// `reindex` rewrites six derived lines and must carry the rest through, or a
    /// plane that could answer `tip` before a repair silently stops being able to.
    ///
    /// The other half is that absence is not an error while a *typo* still is: a key
    /// present but unparseable has to fail, or "unknown" and "corrupt" become the
    /// same state.
    #[test]
    fn base_slot_is_optional_survives_a_repair_and_rejects_a_typo() {
        let dir = std::env::temp_dir().join(format!("live-book-meta-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create");

        let all_zeros = Meta::default();
        all_zeros.write(&dir).expect("write");
        assert_eq!(
            Meta::open(&dir).expect("open").base_slot,
            None,
            "a build that never saw a block must not claim a position"
        );

        Meta::stamp_base_slot(&dir, 197_899_197).expect("stamp");
        let stamped = Meta::open(&dir).expect("open");
        assert_eq!(stamped.base_slot, Some(197_899_197));
        // Everything *else* is unchanged: a stamp is an insertion, not a re-render.
        let mut without_position = stamped.clone();
        without_position.base_slot = None;
        assert_eq!(without_position, all_zeros);

        // A second stamp replaces the line rather than appending one — two
        // `base_slot` lines would leave the reader taking the stale first match.
        Meta::stamp_base_slot(&dir, 42).expect("re-stamp");
        let again = Meta::open(&dir).expect("open");
        assert_eq!(again.base_slot, Some(42));

        // Every derived key, because `rewrite_derived` **replaces** the derived set:
        // a key it is not handed is a line that goes away, and `Meta::open` then fails
        // on the missing field rather than on a wrong one. `reindex` passes all six;
        // this mirrors it.
        Meta::rewrite_derived(
            &dir,
            &[
                ("by_policy_entries", 3),
                ("by_addr_entries", 9),
                ("addr_payment", 7),
                ("addr_stake", 0),
                ("addr_bootstrap", 2),
                ("addr_unkeyed", 0),
            ],
        )
        .expect("repair");
        let repaired = Meta::open(&dir).expect("open");
        assert_eq!(
            repaired.base_slot,
            Some(42),
            "a repair must not drop the position"
        );
        assert_eq!(
            repaired.addr_payment, 7,
            "and must apply what it was asked to"
        );

        // A typo *in the line itself*, not a second line: `Meta::parse` takes the
        // first match, so an appended duplicate would leave the valid one winning and
        // this would test nothing. The `assert_ne` is the guard for that — the same
        // silent-no-op the edit-tools rule exists for, one layer down.
        let text = std::fs::read_to_string(dir.join(META_FILE)).expect("read");
        let typo = text.replace("base_slot 42", "base_slot 12x");
        assert_ne!(typo, text, "the fixture must actually have been changed");
        std::fs::write(dir.join(META_FILE), typo).expect("write");
        assert!(
            Meta::open(&dir).is_err(),
            "a `base_slot` that will not parse is corruption, not an unknown position"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A base address (type 0): a payment credential and a stake credential.
    fn base_address(payment: u8, stake: u8) -> Vec<u8> {
        let mut a = vec![0x01];
        a.extend_from_slice(&[payment; 28]);
        a.extend_from_slice(&[stake; 28]);
        a
    }

    /// A reward address (type 14): a stake credential and nothing else.
    fn reward_address(stake: u8) -> Vec<u8> {
        let mut a = vec![0xE1];
        a.extend_from_slice(&[stake; 28]);
        a
    }

    fn encode_row(
        hash: [u8; 32],
        index: u32,
        lovelace: u64,
        address: &[u8],
        assets: &[(u32, &[u8], u64)],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        for (id, name, qty) in assets {
            write_asset_record(&mut body, *id, name, *qty);
        }
        let mut head = [0u8; ROW_HEADER];
        write_header(
            &mut head,
            (&hash, index),
            3,
            lovelace,
            address.len() as u16,
            assets.len() as u16,
            &Datum::None,
        );
        let mut out = head.to_vec();
        out.extend_from_slice(address);
        out.extend_from_slice(&body);
        out
    }

    /// A whole artifact, written by this test from the format's own public
    /// writers.
    ///
    /// Deliberately **not** built with the probe's builder: this checks the reader
    /// against bytes the test laid out, so a bug shared by the reader and the
    /// builder cannot hide in both. The probe's own test checks the other
    /// direction — that its builder produces something this reader accepts.
    struct Fixture {
        dir: std::path::PathBuf,
        rows: Vec<Vec<u8>>,
    }

    impl Fixture {
        fn new(name: &str) -> Fixture {
            let dir = std::env::temp_dir().join(format!("live-book-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create dir");
            Fixture {
                dir,
                rows: Vec::new(),
            }
        }

        /// Append a row and return the offset it was written at.
        fn push(
            &mut self,
            hash: u8,
            index: u32,
            lovelace: u64,
            address: &[u8],
            assets: &[(u32, &[u8], u64)],
        ) -> u32 {
            let offset: u32 = self.rows.iter().map(Vec::len).sum::<usize>() as u32;
            self.rows
                .push(encode_row([hash; 32], index, lovelace, address, assets));
            offset
        }

        /// Write the rows and every ordering, then open it.
        /// Write the rows and every ordering, then open it. `&self` rather than
        /// `self` so the test keeps its handle on the directory and can clean up.
        fn finish(&self, policies: &[[u8; 28]]) -> Base {
            let rows: Vec<u8> = self.rows.concat();
            std::fs::write(self.dir.join(ROWS_FILE), &rows).expect("write rows");

            let mut outref = Vec::new();
            let mut addr = Vec::new();
            let mut policy = Vec::new();
            let mut at = 0u64;
            for row_bytes in &self.rows {
                let row = Row::parse(row_bytes).expect("the test's own row parses");
                let (hash, index) = row.outref();
                let (prefix, tail) = split(&hash, index);
                outref.push(Perm {
                    prefix,
                    tail,
                    row: at as u32,
                });
                let cred = address::credential(row.address()).expect("the fixture's addresses key");
                addr.push(Perm {
                    prefix: cred.prefix(),
                    tail: 0,
                    row: at as u32,
                });
                let mut seen: Vec<u32> = Vec::new();
                for a in row.assets() {
                    if !seen.contains(&a.policy) {
                        seen.push(a.policy);
                        policy.push(Perm {
                            prefix: Asset::prefix(&policies[a.policy as usize]),
                            tail: 0,
                            row: at as u32,
                        });
                    }
                }
                at += row_bytes.len() as u64;
            }
            outref.sort_unstable();
            addr.sort_unstable();
            policy.sort_unstable();
            write_ordering(&self.dir, TABLE_OUTREF, DIRECTORY_OUTREF, &outref).expect("outref");
            write_ordering(&self.dir, TABLE_ADDR, DIRECTORY_ADDR, &addr).expect("addr");
            write_ordering(&self.dir, TABLE_POLICY, DIRECTORY_POLICY, &policy).expect("policy");
            write_policy_tbl(&self.dir, policies).expect("policy tbl");

            let meta = Meta {
                layout_version: crate::format::LAYOUT_VERSION,
                rows: self.rows.len() as u64,
                rows_bytes: rows.len() as u64,
                by_outref_entries: outref.len() as u64,
                by_addr_entries: addr.len() as u64,
                by_policy_entries: policy.len() as u64,
                policies: policies.len() as u64,
                ..Meta::default()
            };
            meta.write(&self.dir).expect("meta");
            Base::open(&self.dir).expect("open")
        }

        fn remove(&self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// A base's fingerprint answers "is this the base that delta was built
    /// against", so the two properties that matter are what it **ignores**: a
    /// repair, and how the builder happened to use its memory.
    #[test]
    fn fingerprint_ignores_repairs_and_the_build_path() {
        let base = Meta {
            layout_version: crate::format::LAYOUT_VERSION,
            rows: 10_911_674,
            rows_bytes: 1_808_876_205,
            by_outref_entries: 10_911_674,
            policies: 231_737,
            base_slot: Some(197_899_197),
            ..Meta::default()
        };
        let want = base.fingerprint();

        // `LIVE_BOOK.md` §Decided: the artifact is byte-identical across runs at
        // 0, 1 and 7 compactions. Two identical artifacts must not be called
        // different bases because the builder compacted more often.
        let mut noisy = base.clone();
        noisy.compactions = 67;
        noisy.rehashes = 202;
        noisy.spent_hits = 344_578_541;
        noisy.spent_misses = 14_098;
        assert_eq!(
            noisy.fingerprint(),
            want,
            "build-path fields must not change a base's identity"
        );

        // `reindex` rewrites the derived counts from the same rows. The rows did
        // not move, so a delta built before the repair is still valid after it.
        let mut repaired = base.clone();
        repaired.by_policy_entries = 10_946_451;
        repaired.by_addr_entries = 10_911_674;
        repaired.addr_payment = 10_527_049;
        repaired.addr_bootstrap = 384_625;
        assert_eq!(
            repaired.fingerprint(),
            want,
            "a repair must not change a base's identity"
        );
    }

    /// The other half: anything that makes it a *different* live set must be
    /// visible, and "does not know where it sits" is not slot 0.
    #[test]
    fn fingerprint_separates_different_bases() {
        let base = Meta {
            layout_version: crate::format::LAYOUT_VERSION,
            rows: 10_911_674,
            base_slot: Some(197_899_197),
            ..Meta::default()
        };

        let mut later = base.clone();
        later.base_slot = Some(197_899_198);
        assert_ne!(later.fingerprint(), base.fingerprint(), "a later height");

        let mut unstamped = base.clone();
        unstamped.base_slot = None;
        assert_ne!(
            unstamped.fingerprint(),
            base.fingerprint(),
            "unknown position must not collapse onto a known one"
        );

        let mut shorter = base.clone();
        shorter.rows -= 1;
        assert_ne!(shorter.fingerprint(), base.fingerprint(), "a different set");

        let mut relaid = base.clone();
        relaid.rows_bytes += 1;
        assert_ne!(
            relaid.fingerprint(),
            base.fingerprint(),
            "a different arena"
        );
    }

    /// A fingerprint nobody can explain is a mystery rather than a mismatch, so
    /// the bytes it is taken over stay inspectable.
    #[test]
    fn identity_text_is_diffable_and_excludes_what_it_claims_to() {
        let mut meta = Meta {
            base_slot: None,
            ..Meta::default()
        };
        meta.compactions = 67;
        meta.addr_payment = 10_527_049;

        let text = meta.identity_text();
        assert!(text.contains("base_slot unknown"), "{text}");
        for key in Meta::DERIVED_KEYS {
            assert!(
                !text.contains(&format!("{key} ")),
                "derived key {key} reached the identity text"
            );
        }
        for key in ["compactions", "rehashes", "spent_hits", "spent_misses"] {
            assert!(
                !text.contains(&format!("{key} ")),
                "build-path key {key} reached the identity text"
            );
        }
    }

    /// An artifact on disk fingerprints the same read back as it did written —
    /// the property every pairing check depends on.
    #[test]
    fn fingerprint_survives_a_round_trip_through_the_file() {
        let (f, base) = fixture("fingerprint-round-trip");
        let from_open = base.meta().fingerprint();
        let reread = Meta::open(f.dir.as_path()).expect("reopen").fingerprint();
        assert_eq!(from_open, reread);

        // And a stamp is a real change of identity, because it is a real change
        // of what the artifact claims to be.
        Meta::stamp_base_slot(f.dir.as_path(), 12_345).expect("stamp");
        let stamped = Meta::open(f.dir.as_path()).expect("reopen").fingerprint();
        assert_ne!(stamped, from_open);
        f.remove();
    }

    /// Two rows sharing a payment credential but differing in their stake part,
    /// one row at a reward address whose stake credential *is* that same 28 bytes,
    /// and one unrelated row. The overlap is the whole point: it is the case a
    /// credential-keyed index cannot tell apart on its own.
    fn fixture(name: &str) -> (Fixture, Base) {
        let mut f = Fixture::new(name);
        f.push(
            0xA1,
            0,
            1_000,
            &base_address(0x11, 0x22),
            &[(0, b"nft1", 7)],
        );
        f.push(
            0xA2,
            1,
            2_000,
            &base_address(0x11, 0x33),
            &[(0, b"nft2", 8)],
        );
        f.push(0xA3, 2, 3_000, &reward_address(0x11), &[]);
        f.push(0xA4, 3, 4_000, &base_address(0x44, 0x55), &[(1, b"x", 9)]);
        let policies = {
            let mut p0 = [0u8; 28];
            p0[0] = 0xC1;
            let mut p1 = [0u8; 28];
            p1[0] = 0xC2;
            [p0, p1]
        };
        let base = f.finish(&policies);
        (f, base)
    }

    #[test]
    fn a_point_lookup_confirms_the_outref() {
        let (f, base) = fixture("point");
        let row = base.utxo(&[0xA1; 32], 0).expect("lookup").expect("live");
        assert_eq!(row.lovelace(), 1_000);
        assert_eq!(row.address(), base_address(0x11, 0x22));
        // A live tx hash with an index nothing created is absent, not an error.
        assert!(base.utxo(&[0xA1; 32], 9).expect("lookup").is_none());
        assert!(base.utxo(&[0xFF; 32], 0).expect("lookup").is_none());
        f.remove();
    }

    #[test]
    fn an_address_returns_its_own_rows_and_not_its_credentials_others() {
        let (f, base) = fixture("address");
        // Both rows carry payment credential 0x11, but only one is at this
        // address — the confirm is the full address, so the other is rejected.
        let page = base
            .by_address(&base_address(0x11, 0x22), None, 100)
            .expect("by_address");
        assert_eq!(page.refs.len(), 1, "{:?}", page.refs);
        assert_eq!(page.refs[0].1, 0);
        assert!(page.next.is_none());

        // And the address that is not a payment address at all still works,
        // because it is keyed on its own credential like any other.
        let page = base
            .by_address(&reward_address(0x11), None, 100)
            .expect("by_address");
        assert_eq!(page.refs.len(), 1);
        assert_eq!(page.refs[0].1, 2);
        f.remove();
    }

    #[test]
    fn a_payment_credential_returns_both_stake_variants_and_not_the_reward_holder() {
        let (f, base) = fixture("cred");
        let page = base
            .by_payment_cred(&[0x11; 28], None, 100)
            .expect("by cred");
        assert_eq!(page.refs.len(), 2, "{:?}", page.refs);
        let mut indices: Vec<u32> = page.refs.iter().map(|r| r.1).collect();
        indices.sort_unstable();
        // Row 2 is at a reward address whose *stake* credential is the same 28
        // bytes. It is not a payment credential there, so it is not this answer.
        assert_eq!(indices, vec![0, 1]);
        f.remove();
    }

    #[test]
    fn a_policy_returns_each_holding_row_once() {
        let (f, base) = fixture("policy");
        let mut p0 = [0u8; 28];
        p0[0] = 0xC1;
        let page = base.by_policy(&p0, None, 100).expect("by policy");
        assert_eq!(page.refs.len(), 2, "rows 0 and 1 hold the policy");

        let mut p1 = [0u8; 28];
        p1[0] = 0xC2;
        assert_eq!(
            base.by_policy(&p1, None, 100)
                .expect("by policy")
                .refs
                .len(),
            1
        );

        // A policy no row holds is an empty page, not an error and not every row.
        let absent = [0xEE; 28];
        let page = base.by_policy(&absent, None, 100).expect("by policy");
        assert!(page.refs.is_empty());
        f.remove();
    }

    #[test]
    fn paging_walks_a_run_once_and_a_cursor_is_not_replayable() {
        let (f, base) = fixture("paging");
        let mut after = None;
        let mut seen = Vec::new();
        loop {
            let page = base.by_payment_cred(&[0x11; 28], after, 1).expect("page");
            seen.extend(page.refs.iter().map(|r| r.1));
            match page.next {
                Some(c) => after = Some(c),
                None => break,
            }
        }
        seen.sort_unstable();
        assert_eq!(
            seen,
            vec![0, 1],
            "a limit of 1 still enumerates the whole run"
        );

        // A cursor carries the key it came from, so replaying it against another
        // returns an error rather than a plausible-looking wrong page.
        let page = base.by_payment_cred(&[0x11; 28], None, 1).expect("page");
        let cursor = page.next.expect("a next page");
        let err = base.by_payment_cred(&[0x44; 28], Some(cursor), 1);
        assert!(
            err.is_err(),
            "a cursor must not be replayable against another key"
        );
        f.remove();
    }

    #[test]
    fn a_non_address_returns_nothing_rather_than_guessing() {
        let (f, base) = fixture("nonsense");
        // A type-9 header is not an address, and a short payload is not a
        // credential: neither may be keyed and neither may panic.
        let mut nonsense = vec![0x91];
        nonsense.extend_from_slice(&[0x11; 28]);
        assert!(
            base.by_address(&nonsense, None, 10)
                .expect("by address")
                .refs
                .is_empty()
        );
        assert!(
            base.by_address(&[], None, 10)
                .expect("by address")
                .refs
                .is_empty()
        );
        assert!(
            base.by_address(&base_address(0x11, 0x22)[..28], None, 10)
                .expect("by address")
                .refs
                .is_empty()
        );
        f.remove();
    }
}
