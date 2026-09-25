//! The live book's **builder** and its audit — the probe's half of `LIVE_BOOK.md`.
//!
//! The artifact's layout, its writer and its reader live in `live-book`; this is
//! what walks a chunk log into one (`Book`) and what reads one back
//! (`verify`, `reindex`). Nothing here defines a byte offset.
//!
//! Writes `rows.bin` + `by_outref.idx` + `dir.idx` + `by_policy.idx` +
//! `policy.dir.idx` + `by_addr.idx` + `addr.dir.idx` + `policy.tbl` + `book.meta`
//! from a pass over the chunk log. Not yet: the delta chain, the datum table, the
//! base/delta split.
//!
//! # The shape, and why it is this shape
//!
//! The fold's live set today is a `HashMap<(Hash<32>, u32), Entry>` — 64 bytes a
//! slot with alignment, and 2.17 GiB of anonymous memory at mainnet size
//! (`DECODE_SHAPE.md` §2.7). This replaces it with two structures that do
//! different jobs:
//!
//! - **the rows**, appended to a file in the row format. Nothing reads them
//!   during the build except a compaction scan, so they never need to be
//!   resident; the page cache holds them and the kernel may take them back.
//! - **the index**, in RAM, 16 bytes a slot: `u64 prefix ‖ u32 tail ‖ u32 row`.
//!   The artifact's entry is 12 bytes — `u64 prefix ‖ u32 row` — and a prefix hit
//!   is confirmed against the row; a build cannot afford that confirm per spend
//!   (344 M of them), so the build index carries 96 bits of key and needs no
//!   confirm at all. It is discarded at the end, and the artifact's 12-byte index
//!   is derived from it.
//!
//!   Those 96 bits are 64 of the outref's hash and 32 more **mixed with the
//!   output index**, because an outref is `(hash, index)` and a transaction's
//!   outputs all share the hash — so a key taken from the hash alone merges
//!   output 0 with output 1 of every transaction. `finish` checks the key is
//!   unique across live rows; the mixed tail keeps that check honest rather than
//!   decorative.
//!
//! The bitmap `B4` describes is not needed here: a spend tombstones its slot, so
//! the surviving index entries *are* the liveness mask, and compaction decides
//! which rows to keep by probing each row's own outref (which the row format
//! guarantees is in the row).
//!
//! # Deviations from the format, stated
//!
//! - An inline datum's bytes live at the end of their own row, with the length in
//!   the header's `datum_len` — inside the transaction size limit (16 KB), so a
//!   `u16` is enough. `datum_ref` is therefore unused (0). A `datum.tbl` for dedup
//!   is phase 2.
//! - Hash-referenced datums (kind 2) store the 32-byte hash inline the same way
//!   rather than through the table.
//! - The scratch arena and `rows.bin` share the encoding exactly, so compaction
//!   is a verbatim copy of live rows — no remapping, one sequential pass.
//!
//! Divergence from the format is visible in `layout_version`, which every file
//! starts with.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use live_book::address::{self, Kind};
use live_book::format::{
    self, DIRECTORY_ADDR, DIRECTORY_OUTREF, DIRECTORY_POLICY, LAYOUT_VERSION, Meta, Perm,
    PolicyTable, TABLE_ADDR, TABLE_OUTREF, TABLE_POLICY, policy_perms, split, write_directory,
    write_ordering,
};
use live_book::row::{self, MIN_ASSET_RECORD, RowRead};
use pallas_primitives::Hash;

/// The row-offset cap. Phase 1 bails here rather than wrapping an offset; the
/// periodic compaction below keeps a whole-store build under it.
const ARENA_LIMIT: u64 = 3 << 30;
/// Sentinel `row` values in the build index. A real row offset never reaches
/// these: the arena is capped below 4 GiB, which is the `u32` addressing limit
/// the format names. `EMPTY` is deliberately the directory's own empty marker —
/// both mean "no entry here".
const EMPTY: u32 = u32::MAX;
const TOMB: u32 = u32::MAX - 1;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Slot {
    prefix: u64,
    tail: u32,
    row: u32,
}

impl Slot {
    const EMPTY_SLOT: Slot = Slot {
        prefix: 0,
        tail: 0,
        row: EMPTY,
    };

    fn is_live(self) -> bool {
        self.row != EMPTY && self.row != TOMB
    }
}

/// What the build cost, for the report and for the budget decision.
#[derive(Clone, Copy, Default, Debug)]
pub struct Cost {
    pub created: u64,
    /// Spends that resolved to a row the book holds.
    pub spent_hits: u64,
    /// Spends that resolved to nothing — expected at a window's floor, and the
    /// `misresolved` count over the whole chain.
    pub spent_misses: u64,
    pub rows_live: u64,
    pub scratch_bytes: u64,
    pub index_bytes: u64,
    pub rows_bytes: u64,
    pub rehashes: u64,
    pub compactions: u64,
    /// Distinct 64-bit prefixes among live rows. The artifact entry is 64 bits
    /// and a lookup confirms a candidate against its row, so a lookup costs one
    /// row read per candidate — and outputs of one transaction share their hash,
    /// and so their prefix, by construction. This is what a reader actually pays.
    pub distinct_prefixes: u64,
    /// Entries in `by_policy.idx` — one per (row, policy), not per asset name.
    pub by_policy_entries: u64,
    /// Entries in `by_addr.idx` — one per row that has a credential, and the
    /// number is a measurement: `rows − addr_unkeyed` is what the derivation found.
    pub by_addr_entries: u64,
    /// Live rows by what their address keyed on. Three numbers rather than one so
    /// the dispatch is measured rather than assumed: a policy of "slice bytes
    /// 1..29" is wrong for Byron alone, and only the count says whether mainnet
    /// exercises that arm.
    pub addr_payment: u64,
    pub addr_stake: u64,
    pub addr_bootstrap: u64,
    /// Live rows whose address pallas would not parse, so nothing could key them.
    /// Expected to be zero; `by_addr_entries + addr_unkeyed == rows` is the
    /// identity that says so, and it is checked rather than hoped for.
    pub addr_unkeyed: u64,
    /// Σ lovelace over live rows, for the one check that has an outside oracle.
    pub live_lovelace: u128,
    pub live_assets: u64,
    /// The longest address seen, and how many passed `B2`'s original `u8` width.
    /// Measured rather than assumed, because this is the figure that decides
    /// whether a `u16` is enough or long addresses need a side table.
    pub max_addr_len: u32,
    pub addrs_over_255: u64,
}

/// The live set under construction.
pub struct Book {
    dir: PathBuf,
    scratch_path: PathBuf,
    scratch: BufWriter<File>,
    scratch_bytes: u64,
    slots: Vec<Slot>,
    mask: usize,
    live: usize,
    tombs: usize,
    policies: HashMap<[u8; 28], u32>,
    policy_order: Vec<[u8; 28]>,
    /// Compact once the scratch holds this many *dead* bytes — rows the index no
    /// longer points at.
    ///
    /// Dead weight, not arena size. The arena cannot shrink below the live set, so
    /// a size trigger that the live set alone exceeds fires on every single insert:
    /// measured at `--book-compact-mb 1` over a 3-chunk window, 26,524 compactions.
    /// At the 1 GiB default that regime begins near the end of a mainnet build,
    /// where the live set is ~1.6 GB. Measured against dead bytes it is ~50
    /// compactions over mainnet's ~53 GB of churn.
    compact_at: u64,
    /// What the last compaction left behind: the live set's size at that moment,
    /// and so the floor the next trigger measures from.
    last_live_bytes: u64,
    /// The slot of the last block the walk applied — written to `book.meta` by
    /// [`Book::finish`].
    ///
    /// Recorded as the walk passes each block rather than handed to `finish`, because
    /// the walk is the only place that has the block in hand — and it is the one fact
    /// about a book that no transaction inside it carries.
    base_slot: Option<u64>,
    cost: Cost,
}

impl Book {
    pub fn create(dir: &Path, compact_at: u64) -> Result<Book> {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating book dir {}", dir.display()))?;
        if compact_at >= ARENA_LIMIT {
            bail!(
                "--book-compact-mb of {} reaches the u32 row-offset limit ({} MiB); \
                 the arena must compact before that",
                compact_at >> 20,
                ARENA_LIMIT >> 20
            );
        }
        let scratch_path = dir.join("rows.scratch");
        let scratch = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&scratch_path)
            .with_context(|| format!("opening {}", scratch_path.display()))?;
        let slots = vec![Slot::EMPTY_SLOT; 1 << 16];
        Ok(Book {
            dir: dir.to_path_buf(),
            scratch_path,
            scratch: BufWriter::with_capacity(4 << 20, scratch),
            scratch_bytes: 0,
            slots,
            mask: (1 << 16) - 1,
            live: 0,
            tombs: 0,
            policies: HashMap::new(),
            policy_order: Vec::new(),
            compact_at,
            last_live_bytes: 0,
            base_slot: None,
            cost: Cost::default(),
        })
    }

    /// One created output. `assets` yields `(policy, name, quantity)`.
    #[allow(clippy::too_many_arguments)]
    pub fn create_output<'a>(
        &mut self,
        outref: (Hash<32>, u32),
        era: u8,
        lovelace: u64,
        address: &[u8],
        datum: row::Datum<'_>,
        assets: impl Iterator<Item = (&'a [u8], &'a [u8], u64)>,
    ) -> Result<()> {
        if address.len() > u16::MAX as usize {
            bail!(
                "address of {} bytes exceeds the row's u16 addr_len",
                address.len()
            );
        }
        let addr_len = address.len() as u32;
        self.cost.max_addr_len = self.cost.max_addr_len.max(addr_len);
        if addr_len > u8::MAX as u32 {
            self.cost.addrs_over_255 += 1;
        }
        if datum.payload().len() > u16::MAX as usize {
            bail!(
                "datum of {} bytes exceeds the row's u16",
                datum.payload().len()
            );
        }

        // Assets are written after the address, with policies interned to u32.
        // The record's shape is the reader's, so it is written by the reader's
        // own encoder rather than by a second copy of it here.
        let mut body: Vec<u8> = Vec::with_capacity(64);
        let mut n_assets: u16 = 0;
        for (policy, name, qty) in assets {
            if policy.len() != 28 {
                bail!("policy id of {} bytes is not 28", policy.len());
            }
            if name.len() > u8::MAX as usize {
                bail!("asset name of {} bytes exceeds the row's u8", name.len());
            }
            let mut key = [0u8; 28];
            key.copy_from_slice(policy);
            let id = match self.policies.get(&key) {
                Some(id) => *id,
                None => {
                    let id = self.policy_order.len() as u32;
                    self.policies.insert(key, id);
                    self.policy_order.push(key);
                    id
                }
            };
            row::write_asset_record(&mut body, id, name, qty);
            n_assets = n_assets.saturating_add(1);
        }

        let row_offset = u32::try_from(self.scratch_bytes).map_err(|_| {
            anyhow::anyhow!("book arena passed 4 GiB at offset {}", self.scratch_bytes)
        })?;

        let mut hash = [0u8; 32];
        hash.copy_from_slice(outref.0.as_ref());
        let mut head = [0u8; row::ROW_HEADER];
        row::write_header(
            &mut head,
            (&hash, outref.1),
            era,
            lovelace,
            address.len() as u16,
            n_assets,
            &datum,
        );
        self.scratch.write_all(&head)?;
        self.scratch.write_all(address)?;
        self.scratch.write_all(&body)?;
        self.scratch.write_all(datum.payload())?;
        let row_len = (row::ROW_HEADER + address.len() + body.len() + datum.payload().len()) as u64;
        self.scratch_bytes += row_len;

        self.cost.created += 1;
        self.insert(&outref.0, outref.1, row_offset);
        self.live += 1;

        if self.scratch_bytes.saturating_sub(self.last_live_bytes) > self.compact_at {
            self.compact_scratch()?;
        }
        if self.scratch_bytes > ARENA_LIMIT {
            bail!(
                "book arena is {} bytes after compaction and past the u32 row-offset limit",
                self.scratch_bytes
            );
        }
        Ok(())
    }

    /// Where the walk has got to, as the walk sees it. Called once per block, so it
    /// is a field assignment rather than anything that looks at the slot.
    pub fn set_base_slot(&mut self, slot: u64) {
        self.base_slot = Some(slot);
    }

    /// One spend. `true` when the book held it.
    pub fn spend(&mut self, outref: (Hash<32>, u32)) -> bool {
        let (prefix, tail) = split(&outref.0, outref.1);
        match self.find(prefix, tail) {
            Some(i) => {
                self.slots[i].row = TOMB;
                self.live -= 1;
                self.tombs += 1;
                self.cost.spent_hits += 1;
                // Rehash before the table is mostly tombstones, with a floor on
                // the trigger so a young chain does not rehash every block.
                if self.tombs > (self.live / 4).max(1 << 20) {
                    self.rehash();
                }
                true
            }
            None => {
                self.cost.spent_misses += 1;
                false
            }
        }
    }

    // ---- the index ----

    fn find(&self, prefix: u64, tail: u32) -> Option<usize> {
        let mut i = self.probe_start(prefix, tail);
        for _ in 0..self.slots.len() {
            let s = self.slots[i];
            if s.row == EMPTY {
                return None;
            }
            if s.row != TOMB && s.prefix == prefix && s.tail == tail {
                return Some(i);
            }
            i = (i + 1) & self.mask;
        }
        None
    }

    fn insert(&mut self, hash: &Hash<32>, index: u32, row: u32) {
        if (self.live + self.tombs + 1) * 8 >= self.slots.len() * 7 {
            self.rehash();
        }
        let (prefix, tail) = split(hash, index);
        let mut i = self.probe_start(prefix, tail);
        loop {
            let s = self.slots[i];
            if s.row == EMPTY || s.row == TOMB {
                self.slots[i] = Slot { prefix, tail, row };
                return;
            }
            debug_assert!(
                !(s.prefix == prefix && s.tail == tail),
                "an outref was created twice"
            );
            i = (i + 1) & self.mask;
        }
    }

    fn probe_start(&self, prefix: u64, tail: u32) -> usize {
        (mix(prefix ^ u64::from(tail).wrapping_mul(0x9e37_79b9_7f4a_7c15)) as usize) & self.mask
    }

    /// Capacity for `live` rows plus the tombstones that may accumulate before
    /// the next rehash, at `hashbrown`'s 7/8 load factor. 10/7 rather than 2×
    /// because a table sized for twice the live set is 537 MB at mainnet
    /// against 268 MB, and the difference is the whole memory budget.
    fn desired_cap(live: usize) -> usize {
        let want = live.saturating_mul(10) / 7 + 1;
        let mut cap = 1usize << 16;
        while cap < want {
            cap <<= 1;
        }
        cap
    }

    /// Rebuild the table without tombstones, sized for the live set.
    fn rehash(&mut self) {
        let cap = Self::desired_cap(self.live);
        if cap == self.slots.len() && self.tombs == 0 {
            return;
        }
        let old = std::mem::replace(&mut self.slots, vec![Slot::EMPTY_SLOT; cap]);
        self.mask = cap - 1;
        self.tombs = 0;
        for s in old {
            if s.is_live() {
                let mut i = self.probe_start(s.prefix, s.tail);
                while self.slots[i].row != EMPTY {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = s;
            }
        }
        self.cost.rehashes += 1;
    }

    // ---- compaction ----

    /// Rewrite the scratch keeping only rows the index still holds live, and
    /// repoint every live slot at its new offset. One sequential pass in, one
    /// out; the rows carry their own outrefs, so liveness needs no bitmap.
    fn compact_into(&mut self, src: &Path, dst: &Path) -> Result<u64> {
        self.scratch.flush()?;
        let mut r = BufReader::with_capacity(4 << 20, File::open(src)?);
        let mut w = BufWriter::with_capacity(4 << 20, File::create(dst)?);
        let mut row = vec![0u8; 0];
        let mut out: u64 = 0;
        let mut kept = 0u64;
        loop {
            row.clear();
            let mut head = [0u8; row::ROW_HEADER];
            match r.read_exact(&mut head) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            row.extend_from_slice(&head);
            let addr_len = row::addr_len(&head) as usize;
            let n_assets = row::n_assets(&head) as usize;
            let datum_len = row::datum_len(&head) as usize;
            let mut addr_bytes = vec![0u8; addr_len];
            r.read_exact(&mut addr_bytes)?;
            row.extend_from_slice(&addr_bytes);
            for _ in 0..n_assets {
                let mut id = [0u8; 4];
                r.read_exact(&mut id)?;
                row.extend_from_slice(&id);
                let mut nl = [0u8; 1];
                r.read_exact(&mut nl)?;
                row.push(nl[0]);
                let name_len = nl[0] as usize;
                let mut name = vec![0u8; name_len + 8];
                r.read_exact(&mut name)?;
                row.extend_from_slice(&name);
            }
            let mut datum = vec![0u8; datum_len];
            r.read_exact(&mut datum)?;
            row.extend_from_slice(&datum);

            // Live? Ask the index with the row's own outref — the format
            // guarantees it is in the row, which is what makes a bitmap
            // unnecessary.
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&head[row::OFF_OUTREF..row::OFF_OUTREF + 32]);
            let index = u32::from_le_bytes(
                head[row::OFF_OUT_INDEX..row::OFF_OUT_INDEX + 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            let (prefix, key_tail) = split(&hash, index);
            if let Some(i) = self.find(prefix, key_tail) {
                let new = u32::try_from(out)
                    .map_err(|_| anyhow::anyhow!("compacted arena passed 4 GiB at offset {out}"))?;
                w.write_all(&row)?;
                self.slots[i].row = new;
                out += row.len() as u64;
                kept += 1;
            }
        }
        w.flush()?;
        Ok(kept)
    }

    fn compact_scratch(&mut self) -> Result<()> {
        let tmp = self.dir.join("rows.compact");
        self.compact_into(&self.scratch_path.clone(), &tmp)?;
        std::fs::rename(&tmp, &self.scratch_path)?;
        self.scratch = BufWriter::with_capacity(
            4 << 20,
            OpenOptions::new().append(true).open(&self.scratch_path)?,
        );
        let len = std::fs::metadata(&self.scratch_path)?.len();
        self.scratch_bytes = len;
        self.last_live_bytes = len;
        self.cost.compactions += 1;
        Ok(())
    }

    // ---- the artifact ----

    /// Compact, then write `rows.bin`, `by_outref.idx`, `dir.idx`, `policy.tbl`
    /// and `book.meta`. Returns the cost with the final numbers filled in.
    pub fn finish(mut self) -> Result<Cost> {
        let rows_path = self.dir.join("rows.bin");
        self.compact_into(&self.scratch_path.clone(), &rows_path)?;
        let rows_bytes = std::fs::metadata(&rows_path)?.len();
        if self.scratch_path.exists() {
            std::fs::remove_file(&self.scratch_path)?;
        }

        // The live totals come from the compacted file rather than from the
        // create path: `rows.bin` holds only live rows, so a sequential scan of
        // it *is* the live set. The scan also collects the raw material of both
        // derived orderings — `by_policy`'s and `by_addr`'s — because it already
        // reads every address and every asset record to step past them.
        //
        // One function rather than a scan per consumer: the scan is the expensive
        // part, and sharing it removes the possibility of the totals and the
        // ordering disagreeing about which rows exist. `reindex` calls the same one.
        let mut scan = scan_rows(&rows_path, self.policy_order.len())?;
        self.cost.live_lovelace = scan.live_lovelace;
        self.cost.live_assets = scan.live_assets;

        // The artifact index: 12-byte entries, canonical order. The build's key is
        // what decides whether the live set is correct, so two live rows on one
        // key is a hard failure rather than a print.
        let mut entries: Vec<Perm> = self
            .slots
            .iter()
            .filter(|s| s.is_live())
            .map(|s| Perm {
                prefix: s.prefix,
                tail: s.tail,
                row: s.row,
            })
            .collect();
        entries.sort_unstable();
        for w in entries.windows(2) {
            if w[0].prefix == w[1].prefix && w[0].tail == w[1].tail {
                bail!(
                    "two live rows share a 96-bit key ({} / {}): the build index \
                     cannot tell them apart, so the live set it produced is wrong",
                    w[0].prefix,
                    w[0].tail
                );
            }
        }
        // Only the leading 64 of those 96 bits reach the artifact, and that is
        // deliberate (`B3`): the entry is 64 bits and a lookup confirms a
        // candidate against its row. Outputs of one transaction share a hash, so
        // they share a prefix too — which makes the mean candidates per prefix,
        // not one, the cost of a lookup. Measured, not assumed.
        self.cost.distinct_prefixes = if entries.is_empty() {
            0
        } else {
            entries
                .windows(2)
                .filter(|w| w[0].prefix != w[1].prefix)
                .count() as u64
                + 1
        };

        // Settle the counts before anything is written, so a build whose two
        // implementations disagree leaves no artifact behind. `book.meta` is
        // written last, which makes its presence mean the rest passed.
        self.cost.rows_live = self.live as u64;
        self.cost.rows_bytes = rows_bytes;
        self.cost.scratch_bytes = self.scratch_bytes;
        self.cost.index_bytes = (self.slots.len() * std::mem::size_of::<Slot>()) as u64;
        if entries.len() as u64 != self.cost.rows_live {
            bail!(
                "index holds {} live entries but the build counted {} rows",
                entries.len(),
                self.cost.rows_live
            );
        }

        write_ordering(&self.dir, TABLE_OUTREF, DIRECTORY_OUTREF, &entries)?;

        // The interning table, before the orderings that need to resolve an id
        // through it: one reader of `policy.tbl` (`PolicyTable`, a mapping), used
        // by the build, by a repair and by a consumer, so an id resolves to the
        // same 28 bytes in all three.
        format::write_policy_tbl(&self.dir, &self.policy_order)?;
        let policies = PolicyTable::open(&self.dir)?;

        // The derived orderings, from the raw material the scan above collected.
        // One call, so `reindex` writes the same bytes — and so the two of them
        // cannot disagree about what a derived view contains.
        let derived = write_derived_orderings(&self.dir, &mut scan, &policies)?;
        self.cost.by_policy_entries = derived.policy_entries;
        self.cost.by_addr_entries = derived.addr_entries;
        self.cost.addr_payment = derived.addr_payment;
        self.cost.addr_stake = derived.addr_stake;
        self.cost.addr_bootstrap = derived.addr_bootstrap;
        self.cost.addr_unkeyed = derived.addr_unkeyed;

        // The artifact describing itself, and it is `Meta` rather than `writeln!`s
        // here: a reader that parses `book.meta` into fields and a writer that
        // prints them ad hoc is exactly how a field ends up read under a name
        // nobody writes. Last, so its presence means everything before it passed.
        Meta {
            layout_version: LAYOUT_VERSION,
            rows: self.cost.rows_live,
            created: self.cost.created,
            spent_hits: self.cost.spent_hits,
            spent_misses: self.cost.spent_misses,
            live_lovelace: self.cost.live_lovelace,
            live_assets: self.cost.live_assets,
            rows_bytes,
            by_outref_entries: entries.len() as u64,
            policies: self.policy_order.len() as u64,
            compactions: self.cost.compactions,
            rehashes: self.cost.rehashes,
            distinct_prefixes: self.cost.distinct_prefixes,
            by_policy_entries: self.cost.by_policy_entries,
            max_addr_len: self.cost.max_addr_len,
            addrs_over_255: self.cost.addrs_over_255,
            by_addr_entries: self.cost.by_addr_entries,
            addr_payment: self.cost.addr_payment,
            addr_stake: self.cost.addr_stake,
            addr_bootstrap: self.cost.addr_bootstrap,
            addr_unkeyed: self.cost.addr_unkeyed,
            // `None` when the walk never said. A book built by a caller that never
            // called `set_base_slot` does not know where it is, and a zero here would
            // be a claim rather than an absence.
            base_slot: self.base_slot,
        }
        .write(&self.dir)?;

        Ok(self.cost)
    }
}

// --------------------------------------------------------- reading it back

/// What a read-back of one ordering found.
///
/// Its own type rather than a set of fields on `VerifyReport`, because the
/// artifact has **three** orderings and they are checked identically: one
/// `Ordering` each, one `verify_ordering` call each, differing only in the confirm
/// that ties an entry to its row. Written when there were two, and it is what made
/// the third cost a call rather than a function — a check that cannot be pointed
/// at an ordering is a check that gets copied, and the copy is where a reader
/// stops reading.
#[derive(Default, Debug)]
pub struct Ordering {
    pub entries: u64,
    pub sampled: u64,
    pub unsorted: u64,
    pub dir_violations: u64,
    /// Entries whose prefix the row they point at does not corroborate.
    pub mismatches: u64,
    pub out_of_range: u64,
    pub ill_formed: u64,
    pub lookup_nanos: u128,
}

impl Ordering {
    pub fn failures(&self) -> u64 {
        self.unsorted + self.dir_violations + self.mismatches + self.out_of_range + self.ill_formed
    }
}

/// What a read-back of a published artifact found: one `Ordering` per ordering,
/// each held to the same generic properties.
///
/// Three of them rather than three sets of fields on this struct, because a check
/// that cannot be pointed at an ordering is a check that gets copied — and the
/// copy is where a reader stops reading. `by_outref` was written before a second
/// ordering existed and `by_policy` was written as its copy; `by_addr` is the
/// third, and it cost a call rather than a function.
#[derive(Default, Debug)]
pub struct VerifyReport {
    pub outref: Ordering,
    pub policy: Ordering,
    pub addr: Ordering,
}

impl VerifyReport {
    pub fn failures(&self) -> u64 {
        self.outref.failures() + self.policy.failures() + self.addr.failures()
    }
}

/// Resolve an outref the way a consumer does: `dir.idx` to narrow, then
/// `by_outref.idx` to find the row offset, then `rows.bin` for the row.
///
/// This is the first code in the tree that reads the format, and that is the
/// whole point of it. The three identities `finish` checks compare the builder
/// against the fold, and **both of those decode the same bytes** — so none of
/// them tests the artifact as an artifact. This one does: it opens three files,
/// believes nothing in `book.meta` that it cannot re-derive, and holds the entries
/// to the two properties `B5` claims for them (canonical order, and a directory
/// that partitions the array).
///
/// It also times the lookups, which is the number that decides whether this
/// replaces a `redb` point read rather than being a second store beside it.
pub fn verify(dir: &Path, sample: u64) -> Result<VerifyReport> {
    let meta = Meta::open(dir)?;
    if meta.layout_version != LAYOUT_VERSION {
        bail!(
            "book.meta is layout_version {}; this build reads {LAYOUT_VERSION}",
            meta.layout_version
        );
    }
    let rows_live = meta.rows;
    let rows_bytes = meta.rows_bytes;
    let declared = meta.by_outref_entries;
    // Cross-checked rather than required: the count that matters is the one in the
    // file, which `verify_ordering` reads itself. This is a second opinion.
    let declared_addr = meta.by_addr_entries;

    let rows_len = std::fs::metadata(dir.join("rows.bin"))
        .with_context(|| format!("reading {}/rows.bin", dir.display()))?
        .len();
    if rows_len != rows_bytes {
        bail!("rows.bin is {rows_len} B; book.meta claims {rows_bytes}");
    }

    let mut rep = VerifyReport {
        outref: verify_ordering(
            dir,
            TABLE_OUTREF,
            DIRECTORY_OUTREF,
            sample,
            |f, off, len| read_row_outref(f, off, len).map(|o| vec![split(&o.0, o.1).0]),
        )?,
        ..VerifyReport::default()
    };
    if rep.outref.entries != declared || rep.outref.entries != rows_live {
        bail!(
            "by_outref.idx holds {}: book.meta says {declared} entries and {rows_live} rows",
            rep.outref.entries
        );
    }

    // The `by_policy` confirm is the one that has to reach into a row's body: the
    // entry carries a *policy* prefix, so corroborating it means parsing the asset
    // records, resolving each interned id through `policy.tbl`, and finding that
    // prefix among them. A row that merely exists at the offset is not enough —
    // which is the difference between "the offset is in range" and "this row is
    // why the entry is here".
    let policies = PolicyTable::open(dir)?;
    rep.policy = verify_ordering(
        dir,
        TABLE_POLICY,
        DIRECTORY_POLICY,
        sample,
        |f, off, len| row_policy_prefixes(f, off, len, &policies),
    )?;

    // And `by_addr`'s is the strongest of the three, because it re-derives: the
    // entry claims a credential prefix and the only way to check it is to run the
    // same derivation the builder ran over the same bytes. A wrong `1..29` offset
    // in the derivation cannot pass its own check.
    rep.addr = verify_ordering(dir, TABLE_ADDR, DIRECTORY_ADDR, sample, row_cred_prefix)?;
    if rep.addr.entries != rows_live {
        bail!(
            "by_addr.idx holds {} entries against {rows_live} rows — the two orderings are \
             not permutations of the same row set, so the index is missing rows",
            rep.addr.entries
        );
    }
    if rep.addr.entries != declared_addr {
        bail!(
            "by_addr.idx holds {} entries; book.meta says {declared_addr}",
            rep.addr.entries
        );
    }

    Ok(rep)
}

/// Check one ordering the way a consumer reads it: canonical order, a directory
/// that partitions the entry array, and a sample of entries whose prefix the row
/// they point at has to corroborate.
///
/// `confirm` derives the prefixes the row at `offset` actually supports; the
/// entry is a failure unless its prefix is one of them. Everything else here is
/// ordering-agnostic, which is the point — the three orderings differ only in
/// what `confirm` reaches into, and the shared writer (`write_ordering`) is what
/// makes them share an entry width, a canonical order and a directory fill.
fn verify_ordering(
    dir: &Path,
    table: &str,
    directory: &str,
    sample: u64,
    confirm: impl Fn(&mut File, u64, u64) -> Result<Vec<u64>, RowRead>,
) -> Result<Ordering> {
    let rows_bytes = std::fs::metadata(dir.join("rows.bin"))
        .with_context(|| format!("reading {}/rows.bin", dir.display()))?
        .len();
    let raw = format::read_file(dir, table)?;
    let mut rep = Ordering::default();
    let count = format::check_array_header(&raw, table, 12)?;
    rep.entries = count;
    let entry = |i: usize| format::entry_at(&raw, i);

    // `B5`: canonical order is the 96-bit build key, whose leading 64 bits are what
    // the entry stores — so the array is ascending by prefix.
    for i in 1..count as usize {
        if entry(i).0 < entry(i - 1).0 {
            rep.unsorted += 1;
        }
    }

    let draw = format::read_file(dir, directory)?;
    if draw.len() < 8 {
        bail!("{directory} is too short to hold a header");
    }
    let dl = u32::from_le_bytes(draw[0..4].try_into().expect("4 bytes"));
    let dbits = u32::from_le_bytes(draw[4..8].try_into().expect("4 bytes"));
    if dl != LAYOUT_VERSION || dbits != format::DIR_BITS {
        bail!("{directory} has layout {dl} / DIR_BITS {dbits}");
    }
    let n_buckets = 1usize << dbits;
    if draw.len() != format::directory_bytes() {
        bail!("{directory} is {} B", draw.len());
    }
    let bucket = |b: usize| -> u32 { format::bucket_at(&draw, b) };

    // `B5`: the buckets partition the entry array — non-overlapping, contiguous,
    // covering — so they are monotone and never address past the end.
    let mut prev = 0u32;
    for b in 0..n_buckets {
        let v = bucket(b);
        if v < prev || u64::from(v) > count {
            rep.dir_violations += 1;
        }
        prev = v;
    }
    // And a bucket's entries really are in it: `[start, end)` is the range the
    // directory claims, and the first entry of a non-empty range must carry that
    // bucket's top bits. Emptiness is `start == end`, not `start == count` —
    // which is exactly what the backward fill buys.
    for b in 0..n_buckets {
        let start = bucket(b) as usize;
        let end = if b + 1 < n_buckets {
            bucket(b + 1) as usize
        } else {
            count as usize
        };
        if start >= end {
            continue;
        }
        if (entry(start).0 >> (64 - dbits)) as usize != b {
            rep.dir_violations += 1;
        }
    }

    let take = if sample == 0 {
        count
    } else {
        sample.min(count)
    };
    let step = (count / take.max(1)).max(1) as usize;
    let rows_path = dir.join("rows.bin");
    let mut f = File::open(&rows_path)
        .with_context(|| format!("opening {} ({table} points into it)", rows_path.display()))?;
    let t = Instant::now();
    let mut i = 0usize;
    while i < count as usize {
        let (prefix, row) = entry(i);
        match confirm(&mut f, u64::from(row), rows_bytes) {
            Ok(prefixes) => {
                if !prefixes.contains(&prefix) {
                    rep.mismatches += 1;
                }
            }
            Err(RowRead::OutOfRange) => rep.out_of_range += 1,
            Err(RowRead::Short) => rep.ill_formed += 1,
        }
        rep.sampled += 1;
        i += step;
    }
    rep.lookup_nanos = t.elapsed().as_nanos();
    Ok(rep)
}

/// The credential prefix the row at `offset` actually has — `by_addr`'s confirm.
///
/// It re-derives rather than reading anything the builder wrote, which is what
/// makes it a check and not a restatement: if `credential` read the wrong offset,
/// the builder and this reader would read the same wrong offset together and
/// agree. They still would — a confirm cannot falsify a shared derivation. What
/// it falsifies is everything *around* the derivation: an entry pointing at the
/// wrong row, a prefix written from a different field, a row whose bytes are not
/// the address the entry claims. The derivation itself is falsified against
/// pallas's own decode, which is a different implementation.
fn row_cred_prefix(f: &mut File, offset: u64, rows_bytes: u64) -> Result<Vec<u64>, RowRead> {
    let addr = read_row_address(f, offset, rows_bytes)?;
    Ok(match address::credential(&addr) {
        Some(c) => vec![c.prefix()],
        None => Vec::new(),
    })
}

/// The address bytes of the row at `offset`.
///
/// A seek and one `read_exact`, then the crate parses what was read: the offsets
/// and the length arithmetic are `row`'s, and this is only the file access. The
/// verifier deliberately does not go through `Row::parse`'s full bounds check for
/// the *outref* below — see `read_row_outref` — but where it wants a value it
/// wants the same value the reader hands a consumer.
fn read_row_address(f: &mut File, offset: u64, rows_bytes: u64) -> Result<Vec<u8>, RowRead> {
    let head = read_head(f, offset, rows_bytes)?;
    let addr_len = u64::from(row::addr_len(&head));
    let base = offset + row::ROW_HEADER as u64;
    if base + addr_len > rows_bytes {
        return Err(RowRead::OutOfRange);
    }
    let mut addr = vec![0u8; addr_len as usize];
    f.read_exact(&mut addr).map_err(|_| RowRead::Short)?;
    Ok(addr)
}

/// The 56-byte header at `offset`, or why there is not one.
fn read_head(f: &mut File, offset: u64, rows_bytes: u64) -> Result<[u8; row::ROW_HEADER], RowRead> {
    if offset + row::ROW_HEADER as u64 > rows_bytes {
        return Err(RowRead::OutOfRange);
    }
    let mut head = [0u8; row::ROW_HEADER];
    f.seek(SeekFrom::Start(offset))
        .map_err(|_| RowRead::Short)?;
    f.read_exact(&mut head).map_err(|_| RowRead::Short)?;
    Ok(head)
}

/// Every *distinct* policy prefix the row at `offset` holds, resolved through the
/// interning table. Consecutive dedup, for the same reason the builder uses it:
/// a value's assets are grouped by policy, so one policy cannot reappear later in
/// a row.
fn row_policy_prefixes(
    f: &mut File,
    offset: u64,
    rows_bytes: u64,
    policies: &PolicyTable,
) -> Result<Vec<u64>, RowRead> {
    let head = read_head(f, offset, rows_bytes)?;
    let addr_len = u64::from(row::addr_len(&head));
    let n_assets = row::n_assets(&head);
    f.seek(SeekFrom::Start(offset + row::ROW_HEADER as u64 + addr_len))
        .map_err(|_| RowRead::Short)?;

    let mut out: Vec<u64> = Vec::new();
    let mut skip = Vec::new();
    for _ in 0..n_assets {
        let mut id = [0u8; 4];
        f.read_exact(&mut id).map_err(|_| RowRead::Short)?;
        let mut nl = [0u8; 1];
        f.read_exact(&mut nl).map_err(|_| RowRead::Short)?;
        skip.clear();
        skip.resize(nl[0] as usize + 8, 0);
        f.read_exact(&mut skip).map_err(|_| RowRead::Short)?;
        let policy = policies
            .get(u32::from_le_bytes(id))
            .map_err(|_| RowRead::Short)?
            .ok_or(RowRead::Short)?;
        let prefix = live_book::Asset::prefix(&policy);
        if out.last() != Some(&prefix) {
            out.push(prefix);
        }
    }
    Ok(out)
}

/// The outref the row at `offset` claims, or why it is not a row.
///
/// A mis-pointed offset has to survive three things: the row has to start inside
/// the file, its variable-length parts have to fit inside the file, and — the
/// caller's check — its outref has to hash back to the prefix that sent us here.
///
/// The middle one is deliberately a *lower bound* rather than a parse: every
/// asset record is at least `MIN_ASSET_RECORD` bytes, so the arithmetic needs no
/// name lengths read. Reading each record to its end would make this a full row
/// parse, which is the *reader's* job — this is an audit of the index, and a
/// loose bound is the right instrument for it.
fn read_row_outref(f: &mut File, offset: u64, rows_bytes: u64) -> Result<([u8; 32], u32), RowRead> {
    let head = read_head(f, offset, rows_bytes)?;
    let addr_len = u64::from(row::addr_len(&head));
    let n_assets = u64::from(row::n_assets(&head));
    let datum_len = u64::from(row::datum_len(&head));
    let base = offset + row::ROW_HEADER as u64;
    if base + addr_len + (MIN_ASSET_RECORD as u64) * n_assets + datum_len > rows_bytes {
        return Err(RowRead::OutOfRange);
    }

    let mut h = [0u8; 32];
    h.copy_from_slice(&head[row::OFF_OUTREF..row::OFF_OUTREF + 32]);
    Ok((
        h,
        u32::from_le_bytes(
            head[row::OFF_OUT_INDEX..row::OFF_OUT_INDEX + 4]
                .try_into()
                .expect("4 bytes"),
        ),
    ))
}

// --------------------------------------------------------- rebuilding a view

/// What one sequential scan of `rows.bin` yields — the raw material of every
/// ordering that is a function of the rows alone.
struct RowScan {
    live_lovelace: u128,
    live_assets: u64,
    /// One entry per `(row, policy)`, not per asset name: a row holding two assets
    /// of one policy yields one entry, which is what makes an enumeration return a
    /// ref once rather than once per name it holds.
    row_policies: Vec<(u32, u32)>,
    /// One entry per row that has a credential, already in `Perm` shape with the
    /// credential's leading 8 bytes as its prefix. Sorted by the caller, in place.
    row_addr: Vec<Perm>,
    /// Live rows by what their address keyed on. See `Cost`.
    addr_payment: u64,
    addr_stake: u64,
    addr_bootstrap: u64,
    addr_unkeyed: u64,
}

/// One sequential scan of `rows.bin`, returning what three consumers want from it:
/// the live totals the identities compare, the raw material of `by_policy`, and
/// the raw material of `by_addr`.
///
/// One function rather than a scan per consumer. The scan is the expensive part,
/// and sharing it removes the possibility of the totals and the orderings
/// disagreeing about which rows exist. `finish` calls it during a build; `reindex`
/// calls it against an artifact that already exists.
///
/// It is also where the credential is derived, which is what makes `by_addr`
/// **reindexable**: the address is in the row, so a view keyed on it never needs
/// the walk that wrote the row — the same property that let `by_policy` be added
/// to a finished artifact for 5.5 s instead of 50 minutes.
fn scan_rows(rows_path: &Path, n_policies: usize) -> Result<RowScan> {
    let mut out = RowScan {
        live_lovelace: 0,
        live_assets: 0,
        row_policies: Vec::new(),
        row_addr: Vec::new(),
        addr_payment: 0,
        addr_stake: 0,
        addr_bootstrap: 0,
        addr_unkeyed: 0,
    };
    let mut r = BufReader::with_capacity(4 << 20, File::open(rows_path)?);
    let mut head = [0u8; row::ROW_HEADER];
    let mut row_offset: u32 = 0;
    loop {
        match r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let lovelace = u64::from_le_bytes(
            head[row::OFF_LOVELACE..row::OFF_LOVELACE + 8]
                .try_into()
                .expect("8 bytes"),
        );
        let addr_len = row::addr_len(&head) as usize;
        let n_assets = row::n_assets(&head) as usize;
        let datum_len = row::datum_len(&head) as usize;
        out.live_lovelace = out.live_lovelace.saturating_add(u128::from(lovelace));
        out.live_assets += n_assets as u64;
        // File order is address, assets, datum — the order `row::write_header`'s
        // caller emits and the order `Row::parse` walks.
        let mut addr = vec![0u8; addr_len];
        r.read_exact(&mut addr)?;
        match address::credential(&addr) {
            Some(c) => {
                out.row_addr.push(Perm {
                    prefix: c.prefix(),
                    tail: 0,
                    row: row_offset,
                });
                match c.kind {
                    Kind::Payment => out.addr_payment += 1,
                    Kind::Stake => out.addr_stake += 1,
                    Kind::Bootstrap => out.addr_bootstrap += 1,
                }
            }
            None => out.addr_unkeyed += 1,
        }
        let mut payload = 0usize;
        let mut last_id: Option<u32> = None;
        for _ in 0..n_assets {
            let mut id = [0u8; 4];
            r.read_exact(&mut id)?;
            let mut nl = [0u8; 1];
            r.read_exact(&mut nl)?;
            let mut name = vec![0u8; nl[0] as usize + 8];
            r.read_exact(&mut name)?;
            payload += MIN_ASSET_RECORD + nl[0] as usize;

            // Consecutive dedup is enough: a value's assets are grouped by policy
            // (it is a `BTreeMap` keyed by one), so a policy cannot reappear later
            // in the same row.
            let id = u32::from_le_bytes(id);
            if last_id != Some(id) {
                if id as usize >= n_policies {
                    bail!(
                        "row at {row_offset} names policy id {id}, but the table holds \
                         {n_policies}"
                    );
                }
                out.row_policies.push((id, row_offset));
                last_id = Some(id);
            }
        }
        let mut datum = vec![0u8; datum_len];
        r.read_exact(&mut datum)?;
        let row_len = row::ROW_HEADER + addr_len + payload + datum_len;
        row_offset = row_offset
            .checked_add(u32::try_from(row_len).expect("row fits in u32"))
            .context("row offsets passed the u32 the directory addresses with")?;
    }
    Ok(out)
}

/// The `by_policy` and `by_addr` orderings came to, for the cost and the report.
struct Derived {
    policy_entries: u64,
    addr_entries: u64,
    addr_payment: u64,
    addr_stake: u64,
    addr_bootstrap: u64,
    addr_unkeyed: u64,
}

/// Write the orderings that are a function of the rows, and report what they came
/// to — `by_policy.idx` + `policy.dir.idx`, `by_addr.idx` + `addr.dir.idx`.
///
/// One function because `finish` and `reindex` both need it, and because the last
/// time this was written twice in one function the second copy was a duplicate of
/// the first. A repair that produces a different artifact from the build is not a
/// repair, and the way to make that impossible is to not have two of them.
///
/// The `by_addr` identity is checked here, where both numbers are in hand:
/// **every row has a credential, or the derivation has a hole in it.** A row whose
/// address pallas refuses is not silently unkeyed — it is counted, and the count
/// is asserted to be zero against the rows the scan found. An artifact built from
/// a chain that genuinely carries such a row will fail to build rather than serve
/// an index that quietly omits it.
fn write_derived_orderings(
    dir: &Path,
    scan: &mut RowScan,
    policies: &PolicyTable,
) -> Result<Derived> {
    let policy_entries = policy_perms(&scan.row_policies, policies)?;
    write_ordering(dir, TABLE_POLICY, DIRECTORY_POLICY, &policy_entries)?;

    scan.row_addr.sort_unstable();
    let addr_entries = scan.row_addr.len() as u64;
    write_ordering(dir, TABLE_ADDR, DIRECTORY_ADDR, &scan.row_addr)?;

    let rows = addr_entries + scan.addr_unkeyed;
    if scan.addr_unkeyed > 0 {
        bail!(
            "{} of {rows} rows hold an address with no derivable credential, so \
             `by_addr` is not a permutation of the rows",
            scan.addr_unkeyed
        );
    }
    Ok(Derived {
        policy_entries: policy_entries.len() as u64,
        addr_entries,
        addr_payment: scan.addr_payment,
        addr_stake: scan.addr_stake,
        addr_bootstrap: scan.addr_bootstrap,
        addr_unkeyed: scan.addr_unkeyed,
    })
}

/// What a reindex wrote.
pub struct ReindexReport {
    pub policy_entries: u64,
    pub addr_entries: u64,
    pub addr_payment: u64,
    pub addr_stake: u64,
    pub addr_bootstrap: u64,
    pub outref_entries: u64,
}

/// Rebuild the orderings that are derivable from the row store, without touching
/// the chain.
///
/// This is the payoff of the row store being one array with narrow permutations
/// over it (`B1`): a view can be added, or repaired, from an artifact that already
/// exists. Without this, adding one means re-reading 215 GB to produce a file
/// derivable from 1.8 GB of local disk — which is what happened once, for
/// `by_policy`, and why this exists. `by_addr` cost the same: it was added to a
/// finished whole-store artifact in a minute, not by re-walking 9,181 chunks.
///
/// It does what is derivable and refuses to pretend otherwise:
///
/// - `by_policy` from `rows.bin`, because the interned policy id is written inside
///   each row;
/// - `by_addr` from `rows.bin`, because the address is written inside each row and
///   its credential is a function of those bytes;
/// - `dir.idx` from `by_outref.idx`, because a directory is a function of the
///   prefixes it indexes.
///
/// Anything whose key is in **neither** the rows nor the indexes — the mint
/// ledger, witness datums, aux data — cannot be reindexed. Those bytes exist only
/// in the block bodies, so the walk is the only place they can be captured.
///
/// `rows.bin`, `by_outref.idx` and `policy.tbl` are read and never written: a
/// repair cannot damage its own source.
pub fn reindex(dir: &Path) -> Result<ReindexReport> {
    let policies = PolicyTable::open(dir)?;
    let mut scan = scan_rows(&dir.join("rows.bin"), policies.len())?;
    let derived = write_derived_orderings(dir, &mut scan, &policies)?;

    let raw = format::read_file(dir, TABLE_OUTREF)?;
    let n = format::check_array_header(&raw, TABLE_OUTREF, 12)?;
    let prefixes: Vec<u64> = (0..n as usize)
        .map(|i| format::entry_at(&raw, i).0)
        .collect();
    write_directory(
        dir,
        DIRECTORY_OUTREF,
        prefixes.iter().copied(),
        prefixes.len(),
    )?;

    // Last, for the same reason `finish` writes it last: the file's content should
    // mean every write before it succeeded. Only the lines a repair computes are
    // rewritten — see `Meta::rewrite_derived`.
    Meta::rewrite_derived(
        dir,
        &[
            ("by_policy_entries", derived.policy_entries),
            ("by_addr_entries", derived.addr_entries),
            ("addr_payment", derived.addr_payment),
            ("addr_stake", derived.addr_stake),
            ("addr_bootstrap", derived.addr_bootstrap),
            ("addr_unkeyed", derived.addr_unkeyed),
        ],
    )?;

    Ok(ReindexReport {
        policy_entries: derived.policy_entries,
        addr_entries: derived.addr_entries,
        addr_payment: derived.addr_payment,
        addr_stake: derived.addr_stake,
        addr_bootstrap: derived.addr_bootstrap,
        outref_entries: n,
    })
}

/// Position of the key in the table. Not a security boundary: the key is a
/// chain hash and the table is builder-local. FxHash-shaped, as `DECODE_SHAPE`
/// §2.7 measured SipHash to be worth ~3 s over the whole chain.
fn mix(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    x ^ (x >> 33)
}
