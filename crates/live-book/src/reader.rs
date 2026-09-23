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

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::address::{self, Kind};
use crate::format::{
    self, DIRECTORY_ADDR, DIRECTORY_OUTREF, DIRECTORY_POLICY, Directory, Meta, POLICY_FILE,
    PolicyTable, ROWS_FILE, TABLE_ADDR, TABLE_OUTREF, TABLE_POLICY, check_array_header, entry_at,
    split,
};
use crate::row::{Row, RowRead};

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

/// An immutable artifact, mapped read-only.
pub struct Base {
    dir: PathBuf,
    meta: Meta,
    rows: Mmap,
    outref: Table,
    policy: Table,
    addr: Table,
    policies: PolicyTable,
}

struct Table {
    entries: Mmap,
    dir: Directory,
    count: u64,
}

impl Table {
    fn open(dir: &Path, table: &str, directory: &str) -> Result<Table> {
        let entries = format::map(&dir.join(table))?;
        let count = check_array_header(&entries, table, 12)?;
        let dir_map = Directory::open(dir, directory)?;
        dir_map.validate(|i| entry_at(&entries, i).0, count)?;
        Ok(Table {
            entries,
            dir: dir_map,
            count,
        })
    }

    fn entry(&self, i: u64) -> (u64, u32) {
        entry_at(&self.entries, i as usize)
    }

    /// The entries carrying `prefix`, as a contiguous range.
    ///
    /// Binary search inside the bucket rather than a scan of it: a bucket is ~5
    /// entries on average, but a busy wallet's credential can be tens of
    /// thousands all sharing one prefix, and those are a page to return rather
    /// than five entries to skip past.
    fn run(&self, prefix: u64) -> Range<u64> {
        let bucket = self.dir.range_for(prefix);
        let end = (bucket.end as u64).min(self.count);
        let start = (bucket.start as u64).min(end);
        let first_at_or_after = |key: u64| {
            let (mut a, mut b) = (start, end);
            while a < b {
                let m = a + (b - a) / 2;
                if self.entry(m).0 < key {
                    a = m + 1;
                } else {
                    b = m;
                }
            }
            a
        };
        let lo = first_at_or_after(prefix);
        let hi = if prefix == u64::MAX {
            end
        } else {
            first_at_or_after(prefix + 1)
        };
        lo..hi.max(lo)
    }
}

impl Base {
    /// Map an artifact. Validates what it can cheaply: every file's version, the
    /// counts each ordering claims against `book.meta`, and that each directory
    /// partitions its array. A corrupt artifact is refused here rather than
    /// answering wrongly later.
    pub fn open(dir: &Path) -> Result<Base> {
        let meta = Meta::open(dir)?;
        if meta.layout_version != format::LAYOUT_VERSION {
            bail!(
                "book.meta is layout_version {}; this build reads {}",
                meta.layout_version,
                format::LAYOUT_VERSION
            );
        }
        let rows_path = dir.join(ROWS_FILE);
        let rows = format::map(&rows_path)?;
        if rows.len() as u64 != meta.rows_bytes {
            bail!(
                "{} is {} B; book.meta claims {}",
                rows_path.display(),
                rows.len(),
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

    pub fn policies(&self) -> &PolicyTable {
        &self.policies
    }

    /// The row at a byte offset in `rows.bin`.
    pub fn row(&self, offset: u32) -> Result<Row<'_>, RowRead> {
        let at = offset as usize;
        if at >= self.rows.len() {
            return Err(RowRead::OutOfRange);
        }
        Row::parse(&self.rows[at..])
    }

    /// Is this output live, and what does it say?
    ///
    /// One directory page, one entry page, one row read — and the row's own
    /// 36-byte outref is what decides, so a prefix collision cannot return
    /// somebody else's output.
    pub fn utxo(&self, tx: &[u8; 32], index: u32) -> Result<Option<Row<'_>>> {
        let (prefix, _) = split(tx, index);
        for i in self.outref.run(prefix) {
            let (p, offset) = self.outref.entry(i);
            if p != prefix {
                break;
            }
            let Ok(row) = self.row(offset) else {
                continue;
            };
            if row.outref() == (*tx, index) {
                return Ok(Some(row));
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
            row.address() == address
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
            matches!(
                address::credential(row.address()),
                Some(c) if c.kind == Kind::Payment && c.bytes() == cred
            )
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
            row.assets()
                .any(|a| self.policies.get(a.policy) == Some(policy))
        })
    }

    /// The shared shape of the three enumerations: walk the run for `prefix` from
    /// `after`, confirm each candidate against its row, and stop once `limit`
    /// confirmed refs are collected or the run is exhausted.
    ///
    /// The cursor is an entry index, so a page that confirmed nothing still moves
    /// forward — which is what stops a caller paging over a long run of rejected
    /// candidates from looping on the same page forever.
    fn enumerate(
        &self,
        table: &Table,
        prefix: u64,
        after: Option<Cursor>,
        limit: usize,
        confirm: impl Fn(Row<'_>) -> bool,
    ) -> Result<Page> {
        let run = table.run(prefix);
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
            let (p, offset) = table.entry(at);
            at += 1;
            if p != prefix {
                // The bucket held a neighbouring prefix: the run is over, and the
                // cursor must not be left mid-bucket to be resumed into it.
                at = run.end;
                break;
            }
            match self.row(offset) {
                Ok(row) => {
                    if confirm(row) {
                        refs.push(row.outref());
                    }
                }
                Err(RowRead::OutOfRange | RowRead::Short) => continue,
            }
        }
        let next = (at < run.end).then_some(Cursor { prefix, at });
        Ok(Page { refs, next })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Perm, write_ordering, write_policy_tbl};
    use crate::row::{Asset, Datum, ROW_HEADER, write_asset_record, write_header};

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
