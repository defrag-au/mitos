//! The live book — phase 1 of `LIVE_BOOK.md`'s builder.
//!
//! Writes `rows.bin` + `by_outref.idx` + `dir.idx` + `policy.tbl` from a pass
//! over the chunk log. Not yet: `by_addr`, the delta chain, the datum table, the
//! base/delta split.
//!
//! # The shape, and why it is this shape
//!
//! The fold's live set today is a `HashMap<(Hash<32>, u32), Entry>` — 64 bytes a
//! slot with alignment, and 2.17 GiB of anonymous memory at mainnet size
//! (`DECODE_SHAPE.md` §2.7). This replaces it with two structures that do
//! different jobs:
//!
//! - **the rows**, appended to a file in `B2`'s encoding. Nothing reads them
//!   during the build except a compaction scan, so they never need to be
//!   resident; the page cache holds them and the kernel may take them back.
//! - **the index**, in RAM, 16 bytes a slot: `u64 prefix ‖ u32 tail ‖ u32 row`.
//!   `B3`'s *artifact* entry is 12 bytes — `u64 prefix ‖ u32 row` — and a prefix
//!   hit is confirmed against the row; a build cannot afford that confirm per
//!   spend (344 M of them), so the build index carries 96 bits of key and needs
//!   no confirm at all. It is discarded at the end, and the artifact's 12-byte
//!   index is derived from it.
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
//! which rows to keep by probing each row's own outref (which `B2` guarantees is
//! in the row).
//!
//! # Deviations from `B2`, stated
//!
//! - `B2` does not say where an inline datum's bytes live. Phase 1 puts them at
//!   the end of their own row, with the length in the two bytes `B2` reserves —
//!   inside the transaction size limit (16 KB), so a `u16` is enough. `datum_ref`
//!   is therefore unused (0). A `datum.tbl` for dedup is phase 2.
//! - `B2` gives `addr_len` one byte. Real mainnet data falsifies that: chunk 81
//!   of 9,181 holds a 513-byte Byron address — `[#6.24(502-byte payload), crc32]`
//!   with the payload reading `[root(28), attributes(~470), addrtype]`. It is
//!   `u16` here, which costs nothing because the byte comes out of the header's
//!   spare; the build records the longest address it saw, so the width stays a
//!   measurement rather than a guess.
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
use pallas_primitives::Hash;

/// `B3`: 2^21 buckets, ~8 MB, ~5 entries per bucket at 10.9 M rows.
pub const DIR_BITS: u32 = 21;
/// The only layout this code writes or reads.
///
/// 2: the build key folds the output index into its tail (version 1 omitted it,
/// so a version-1 artifact has the outputs of each transaction merged).
/// 3: `addr_len` is `u16`, not `u8` (version 2 could not hold a Byron address:
/// real mainnet data falsifies the `u8` at chunk 81 of 9,181 with a 513-byte
/// address). The header is still 56 bytes — the byte came out of the spare.
pub const LAYOUT_VERSION: u32 = 3;
/// `B2`'s fixed header is 56 bytes, 8-aligned.
const ROW_HEADER: usize = 56;
/// Sentinel `row` values. A real row offset never reaches these: the arena is
/// capped below 4 GiB, which is the `u32` addressing limit `B2` names.
const EMPTY: u32 = u32::MAX;
const TOMB: u32 = u32::MAX - 1;
/// The row-offset cap. Phase 1 bails here rather than wrapping an offset; the
/// periodic compaction below keeps a whole-store build under it.
const ARENA_LIMIT: u64 = 3 << 30;

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

/// One entry of a permutation: the 64-bit prefix a lookup matches, and the row it
/// points at.
///
/// `tail` is the build's business — it is what makes the build index's key 96 bits
/// wide and therefore confirm-free — and is deliberately **not** written to the
/// artifact, where 64 bits plus a confirm against the row is the trade `B3` makes.
/// That asymmetry is why this is a type rather than two tuples: every ordering
/// hands the writer the same shape, so the entry width, the canonical order and
/// the directory fill are written in one place.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Perm {
    prefix: u64,
    tail: u32,
    row: u32,
}

/// Write one ordering: its `{u64 prefix, u32 row}` permutations in canonical
/// order, plus a `2^21`-bucket directory over them.
///
/// Every ordering goes through here so they cannot disagree about the format, the
/// canonical order, or the fill — and the fill is the part that was wrong. An
/// empty bucket must carry the index of the next one that is not; filling with the
/// entry count instead makes an empty bucket *precede* a non-empty one, so
/// `buckets[b] > buckets[b + 1]`, the array stops being the partition `B5` claims,
/// and a consumer reading the range as `end − start` underflows. `verify` catches
/// it, and `a_book_round_trips_and_a_corrupted_entry_is_caught` guards the fix.
fn write_ordering(dir: &Path, table: &str, directory: &str, entries: &[Perm]) -> Result<()> {
    let mut w = BufWriter::with_capacity(4 << 20, File::create(dir.join(table))?);
    w.write_all(&LAYOUT_VERSION.to_le_bytes())?;
    w.write_all(&(entries.len() as u64).to_le_bytes())?;
    for e in entries {
        w.write_all(&e.prefix.to_le_bytes())?;
        w.write_all(&e.row.to_le_bytes())?;
    }
    w.flush()?;

    let n_buckets = 1usize << DIR_BITS;
    let mut buckets = vec![EMPTY; n_buckets];
    for (i, e) in entries.iter().enumerate() {
        let b = (e.prefix >> (64 - DIR_BITS)) as usize;
        if buckets[b] == EMPTY {
            buckets[b] = i as u32;
        }
    }
    let mut next = entries.len() as u32;
    for b in (0..n_buckets).rev() {
        if buckets[b] == EMPTY {
            buckets[b] = next;
        } else {
            next = buckets[b];
        }
    }
    let mut d = BufWriter::with_capacity(1 << 20, File::create(dir.join(directory))?);
    d.write_all(&LAYOUT_VERSION.to_le_bytes())?;
    d.write_all(&DIR_BITS.to_le_bytes())?;
    for b in &buckets {
        d.write_all(&b.to_le_bytes())?;
    }
    d.flush()?;
    Ok(())
}

/// A datum as the walk sees it.
pub enum Datum<'a> {
    None,
    Inline(&'a [u8]),
    /// Hash-referenced: the bytes are the 32-byte hash, and phase 1 keeps them
    /// rather than pointing at a table.
    Hash(&'a [u8]),
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
        datum: Datum<'_>,
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
        let (datum_kind, datum_bytes): (u8, &[u8]) = match datum {
            Datum::None => (0, &[]),
            Datum::Inline(b) => (1, b),
            Datum::Hash(b) => (2, b),
        };
        if datum_bytes.len() > u16::MAX as usize {
            bail!("datum of {} bytes exceeds the row's u16", datum_bytes.len());
        }

        // Assets are written after the address, with policies interned to u32.
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
            body.extend_from_slice(&id.to_le_bytes());
            body.push(name.len() as u8);
            body.extend_from_slice(name);
            body.extend_from_slice(&qty.to_le_bytes());
            n_assets = n_assets.saturating_add(1);
        }

        let row_offset = u32::try_from(self.scratch_bytes).map_err(|_| {
            anyhow::anyhow!("book arena passed 4 GiB at offset {}", self.scratch_bytes)
        })?;

        let mut head = [0u8; ROW_HEADER];
        head[0..32].copy_from_slice(outref.0.as_ref());
        head[32..36].copy_from_slice(&outref.1.to_le_bytes());
        head[36..44].copy_from_slice(&lovelace.to_le_bytes());
        // 44..48 datum_ref: unused in phase 1 (see the module header).
        head[48] = datum_kind;
        head[49..51].copy_from_slice(&(address.len() as u16).to_le_bytes());
        head[51..53].copy_from_slice(&n_assets.to_le_bytes());
        head[53] = era;
        head[54..56].copy_from_slice(&(datum_bytes.len() as u16).to_le_bytes());
        self.scratch.write_all(&head)?;
        self.scratch.write_all(address)?;
        self.scratch.write_all(&body)?;
        self.scratch.write_all(datum_bytes)?;
        let row_len = (ROW_HEADER + address.len() + body.len() + datum_bytes.len()) as u64;
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
            let mut head = [0u8; ROW_HEADER];
            match r.read_exact(&mut head) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            row.extend_from_slice(&head);
            let addr_len = u16::from_le_bytes([head[49], head[50]]) as usize;
            let n_assets = u16::from_le_bytes([head[51], head[52]]) as usize;
            let datum_len = u16::from_le_bytes([head[54], head[55]]) as usize;
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

            // Live? Ask the index with the row's own outref — `B2` guarantees it
            // is in the row, which is what makes a bitmap unnecessary.
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&head[0..32]);
            let index = u32::from_le_bytes(head[32..36].try_into().expect("4 bytes"));
            let (prefix, key_tail) = split(&Hash::from(hash), index);
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
        // it *is* the live set — and it is the same scan a verifier would run.
        //
        // `by_policy` is collected here for the same reason: this scan already
        // reads every asset record to skip it, so the second ordering costs no
        // extra I/O and no per-output work in the hot path. Deriving an ordering
        // from the row store is also what `B1` says the orderings *are*.
        let mut by_policy: Vec<(u32, u32)> = Vec::new();
        {
            let mut r = BufReader::with_capacity(4 << 20, File::open(&rows_path)?);
            let mut head = [0u8; ROW_HEADER];
            let mut row_offset: u32 = 0;
            loop {
                match r.read_exact(&mut head) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }
                let lovelace = u64::from_le_bytes(head[36..44].try_into().expect("8 bytes"));
                let addr_len = u16::from_le_bytes([head[49], head[50]]) as usize;
                let n_assets = u16::from_le_bytes([head[51], head[52]]) as usize;
                let datum_len = u16::from_le_bytes([head[54], head[55]]) as usize;
                self.cost.live_lovelace =
                    self.cost.live_lovelace.saturating_add(u128::from(lovelace));
                self.cost.live_assets += n_assets as u64;
                // File order is address, assets, datum — the same order the
                // writer emits and the same order `compact_into` parses.
                let mut addr = vec![0u8; addr_len];
                r.read_exact(&mut addr)?;
                let mut payload = 0usize;
                let mut last_id: Option<u32> = None;
                for _ in 0..n_assets {
                    let mut id = [0u8; 4];
                    r.read_exact(&mut id)?;
                    let mut nl = [0u8; 1];
                    r.read_exact(&mut nl)?;
                    let mut name = vec![0u8; nl[0] as usize + 8];
                    r.read_exact(&mut name)?;
                    payload += 4 + 1 + name.len();

                    // Consecutive dedup is enough: a value's assets are grouped by
                    // policy (it is a `BTreeMap` keyed by one), so a policy cannot
                    // reappear later in the same row. One entry per (row, policy) is
                    // what makes an enumeration return a ref once, not once per
                    // asset name it holds.
                    let id = u32::from_le_bytes(id);
                    if last_id != Some(id) {
                        if id as usize >= self.policy_order.len() {
                            bail!(
                                "row at {row_offset} names policy id {id}, but the table \
                                 holds {}",
                                self.policy_order.len()
                            );
                        }
                        by_policy.push((id, row_offset));
                        last_id = Some(id);
                    }
                }
                let mut datum = vec![0u8; datum_len];
                r.read_exact(&mut datum)?;
                let row_len = ROW_HEADER + addr_len + payload + datum_len;
                row_offset = row_offset
                    .checked_add(u32::try_from(row_len).expect("row fits in u32"))
                    .context("row offsets passed the u32 the directory addresses with")?;
            }
        }

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

        write_ordering(&self.dir, "by_outref.idx", "dir.idx", &entries)?;

        // The second ordering. Its prefix is the policy's leading 8 bytes — the same
        // convention every ordering uses — and it needs no `tail`: a policy is
        // exactly 28 bytes, so two distinct policies sharing a 64-bit prefix is
        // 1.5e-9 at this cardinality, and the row confirm settles the rest.
        //
        // Note what the directory does and does not do here. Every row holding one
        // policy shares its prefix, so they all land in a single bucket and the
        // lookup is "find the run's start, then scan it" — which is exactly the
        // query. Narrowing *within* a policy is the page cursor's job, not the
        // directory's.
        let mut policy_entries: Vec<Perm> = by_policy
            .iter()
            .map(|(id, row)| {
                let policy = &self.policy_order[*id as usize];
                let mut p = [0u8; 8];
                p.copy_from_slice(&policy[0..8]);
                Perm {
                    prefix: u64::from_le_bytes(p),
                    tail: 0,
                    row: *row,
                }
            })
            .collect();
        policy_entries.sort_unstable();
        write_ordering(
            &self.dir,
            "by_policy.idx",
            "policy.dir.idx",
            &policy_entries,
        )?;
        self.cost.by_policy_entries = policy_entries.len() as u64;

        let mut p = BufWriter::with_capacity(1 << 20, File::create(self.dir.join("policy.tbl"))?);
        p.write_all(&LAYOUT_VERSION.to_le_bytes())?;
        p.write_all(&(self.policy_order.len() as u64).to_le_bytes())?;
        for pol in &self.policy_order {
            p.write_all(pol)?;
        }
        p.flush()?;

        let mut m = BufWriter::new(File::create(self.dir.join("book.meta"))?);
        writeln!(m, "layout_version {LAYOUT_VERSION}")?;
        writeln!(m, "rows {}", self.cost.rows_live)?;
        writeln!(m, "created {}", self.cost.created)?;
        writeln!(m, "spent_hits {}", self.cost.spent_hits)?;
        writeln!(m, "spent_misses {}", self.cost.spent_misses)?;
        writeln!(m, "live_lovelace {}", self.cost.live_lovelace)?;
        writeln!(m, "live_assets {}", self.cost.live_assets)?;
        writeln!(m, "rows_bytes {rows_bytes}")?;
        writeln!(m, "by_outref_entries {}", entries.len())?;
        writeln!(m, "policies {}", self.policy_order.len())?;
        writeln!(m, "compactions {}", self.cost.compactions)?;
        writeln!(m, "rehashes {}", self.cost.rehashes)?;
        writeln!(m, "distinct_prefixes {}", self.cost.distinct_prefixes)?;
        writeln!(m, "by_policy_entries {}", self.cost.by_policy_entries)?;
        writeln!(m, "max_addr_len {}", self.cost.max_addr_len)?;
        writeln!(m, "addrs_over_255 {}", self.cost.addrs_over_255)?;
        m.flush()?;

        Ok(self.cost)
    }
}

/// The book's 96-bit key for a live row: 8 bytes of the outref's hash, then 4
/// more mixed with the outref's output index.
///
/// The index has to be in the key. An outref is `(hash, index)`, a transaction's
/// outputs all share its hash, and most transactions have more than one output —
/// so a key taken from the hash alone collapses a whole transaction onto one
/// entry, and the second output silences the first.
fn split(h: &Hash<32>, index: u32) -> (u64, u32) {
    let b = h.as_ref();
    let mut p = [0u8; 8];
    p.copy_from_slice(&b[0..8]);
    let mut t = [0u8; 4];
    t.copy_from_slice(&b[8..12]);
    // Mixed rather than added or `xor`ed raw: consecutive indices would
    // otherwise probe adjacent slots — index 0 and index 1 differ by one in the
    // low bit — which is the clustering a linear probe is worst at.
    (u64::from_le_bytes(p), u32::from_le_bytes(t) ^ mix32(index))
}

/// A 32-bit finaliser for the output index. See `split`.
fn mix32(mut x: u32) -> u32 {
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^ (x >> 16)
}

// --------------------------------------------------------- reading it back

/// What a read-back of one ordering found.
///
/// Its own type rather than more fields on `VerifyReport`, because the artifact
/// now has two orderings and a third (`by_addr`) is designed — and a check that
/// cannot be pointed at an ordering is a check that gets copied.
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

/// What a read-back of a published artifact found.
#[derive(Default, Debug)]
pub struct VerifyReport {
    pub entries: u64,
    pub sampled: u64,
    pub unsorted: u64,
    pub dir_violations: u64,
    /// Rows whose own outref does not hash to the prefix the index sent us to.
    pub prefix_mismatches: u64,
    pub rows_out_of_range: u64,
    pub rows_ill_formed: u64,
    pub lookup_nanos: u128,
    /// `by_policy.idx`. Checked by `verify_policy` rather than folded in with the
    /// `by_outref` checks above, which were written before a second ordering
    /// existed; uniting them is a tidy-up, not a correctness gap — the two check
    /// the same generic properties and each has its own confirm.
    pub policy: Ordering,
}

impl VerifyReport {
    pub fn failures(&self) -> u64 {
        self.unsorted
            + self.dir_violations
            + self.prefix_mismatches
            + self.rows_out_of_range
            + self.rows_ill_formed
            + self.policy.failures()
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
/// It also times the lookup, which is the number that decides whether this
/// replaces a `redb` point read rather than being a second store beside it.
pub fn verify(dir: &Path, sample: u64) -> Result<VerifyReport> {
    let meta = std::fs::read_to_string(dir.join("book.meta"))
        .with_context(|| format!("reading {}/book.meta", dir.display()))?;
    let field = |k: &str| -> Result<u64> {
        meta.lines()
            .find_map(|l| l.strip_prefix(k).and_then(|r| r.trim().parse().ok()))
            .with_context(|| format!("book.meta has no `{k}`"))
    };

    let layout = field("layout_version")? as u32;
    if layout != LAYOUT_VERSION {
        bail!("book.meta is layout_version {layout}; this build reads {LAYOUT_VERSION}");
    }
    let rows_live = field("rows")?;
    let rows_bytes = field("rows_bytes")?;
    let declared = field("by_outref_entries")?;

    let mut rep = VerifyReport::default();

    let rows_len = std::fs::metadata(dir.join("rows.bin"))?.len();
    if rows_len != rows_bytes {
        bail!("rows.bin is {rows_len} B; book.meta claims {rows_bytes}");
    }

    let raw = std::fs::read(dir.join("by_outref.idx"))?;
    if raw.len() < 12 {
        bail!("by_outref.idx is too short to hold a header");
    }
    let ilayout = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes"));
    if ilayout != LAYOUT_VERSION {
        bail!("by_outref.idx is layout_version {ilayout}; this build reads {LAYOUT_VERSION}");
    }
    let count = u64::from_le_bytes(raw[4..12].try_into().expect("8 bytes"));
    if count != declared || count != rows_live {
        bail!(
            "by_outref.idx holds {count}: book.meta says {declared} entries and {rows_live} rows"
        );
    }
    let want = 12 + 12 * count as usize;
    if raw.len() != want {
        bail!(
            "by_outref.idx is {} B; {count} entries need {want}",
            raw.len()
        );
    }
    rep.entries = count;

    let entry = |i: usize| -> (u64, u32) {
        let b = &raw[12 + 12 * i..];
        (
            u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
        )
    };

    // `B5`: canonical order is the 96-bit build key, whose leading 64 bits are what
    // the entry stores — so the array is ascending by prefix.
    for i in 1..count as usize {
        if entry(i).0 < entry(i - 1).0 {
            rep.unsorted += 1;
        }
    }

    let draw = std::fs::read(dir.join("dir.idx"))?;
    if draw.len() < 8 {
        bail!("dir.idx is too short to hold a header");
    }
    let dl = u32::from_le_bytes(draw[0..4].try_into().expect("4 bytes"));
    if dl != LAYOUT_VERSION {
        bail!("dir.idx is layout_version {dl}; this build reads {LAYOUT_VERSION}");
    }
    let dbits = u32::from_le_bytes(draw[4..8].try_into().expect("4 bytes"));
    if dbits != DIR_BITS {
        bail!("dir.idx has DIR_BITS {dbits}; this build reads {DIR_BITS}");
    }
    let n_buckets = 1usize << dbits;
    let want_dir = 8 + n_buckets * 4;
    if draw.len() != want_dir {
        bail!(
            "dir.idx is {} B; {n_buckets} buckets need {want_dir}",
            draw.len()
        );
    }
    let bucket = |b: usize| -> u32 {
        u32::from_le_bytes(draw[8 + 4 * b..8 + 4 * b + 4].try_into().expect("4 bytes"))
    };

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
    let mut f = File::open(dir.join("rows.bin"))?;
    let t = Instant::now();
    let mut i = 0usize;
    while i < count as usize {
        let (prefix, row) = entry(i);
        match read_row_outref(&mut f, u64::from(row), rows_bytes) {
            Ok(oref) => {
                if split(&oref.0, oref.1).0 != prefix {
                    rep.prefix_mismatches += 1;
                }
            }
            Err(RowRead::OutOfRange) => rep.rows_out_of_range += 1,
            Err(RowRead::Short) => rep.rows_ill_formed += 1,
        }
        rep.sampled += 1;
        i += step;
    }
    rep.lookup_nanos = t.elapsed().as_nanos();
    rep.policy = verify_policy(dir, sample)?;
    Ok(rep)
}

/// Check `by_policy.idx` the same way: canonical order, a directory that
/// partitions, and a sample of entries whose prefix the row has to corroborate.
///
/// The confirm here is stronger than the outref one and worth stating: the entry
/// carries a *policy* prefix, so corroborating it means parsing the row's asset
/// records, resolving each interned id through `policy.tbl`, and checking that one
/// of the row's policies leads with that prefix. A row that merely exists at the
/// offset is not enough — which is the difference between "the offset is in range"
/// and "this row is why the entry is here".
fn verify_policy(dir: &Path, sample: u64) -> Result<Ordering> {
    let pt = std::fs::read(dir.join("policy.tbl"))?;
    if pt.len() < 12 {
        bail!("policy.tbl is too short to hold a header");
    }
    let pl = u32::from_le_bytes(pt[0..4].try_into().expect("4 bytes"));
    if pl != LAYOUT_VERSION {
        bail!("policy.tbl is layout_version {pl}; this build reads {LAYOUT_VERSION}");
    }
    let n_policies = u64::from_le_bytes(pt[4..12].try_into().expect("8 bytes"));
    if pt.len() != 12 + 28 * n_policies as usize {
        bail!(
            "policy.tbl is {} B; {n_policies} policies need {}",
            pt.len(),
            12 + 28 * n_policies as usize
        );
    }
    let policies: Vec<[u8; 28]> = (0..n_policies as usize)
        .map(|i| {
            let mut p = [0u8; 28];
            p.copy_from_slice(&pt[12 + 28 * i..12 + 28 * i + 28]);
            p
        })
        .collect();

    let rows_bytes = std::fs::metadata(dir.join("rows.bin"))?.len();
    let raw = std::fs::read(dir.join("by_policy.idx"))?;
    let mut rep = Ordering::default();
    if raw.len() < 12 {
        bail!("by_policy.idx is too short to hold a header");
    }
    let il = u32::from_le_bytes(raw[0..4].try_into().expect("4 bytes"));
    if il != LAYOUT_VERSION {
        bail!("by_policy.idx is layout_version {il}; this build reads {LAYOUT_VERSION}");
    }
    let count = u64::from_le_bytes(raw[4..12].try_into().expect("8 bytes"));
    let want = 12 + 12 * count as usize;
    if raw.len() != want {
        bail!(
            "by_policy.idx is {} B; {count} entries need {want}",
            raw.len()
        );
    }
    rep.entries = count;
    let entry = |i: usize| -> (u64, u32) {
        let b = &raw[12 + 12 * i..];
        (
            u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
        )
    };
    for i in 1..count as usize {
        if entry(i).0 < entry(i - 1).0 {
            rep.unsorted += 1;
        }
    }

    let draw = std::fs::read(dir.join("policy.dir.idx"))?;
    let dl = u32::from_le_bytes(draw[0..4].try_into().expect("4 bytes"));
    let dbits = u32::from_le_bytes(draw[4..8].try_into().expect("4 bytes"));
    if dl != LAYOUT_VERSION || dbits != DIR_BITS {
        bail!("policy.dir.idx has layout {dl} / DIR_BITS {dbits}");
    }
    let n_buckets = 1usize << dbits;
    if draw.len() != 8 + n_buckets * 4 {
        bail!("policy.dir.idx is {} B", draw.len());
    }
    let bucket = |b: usize| -> u32 {
        u32::from_le_bytes(draw[8 + 4 * b..8 + 4 * b + 4].try_into().expect("4 bytes"))
    };
    let mut prev = 0u32;
    for b in 0..n_buckets {
        let v = bucket(b);
        if v < prev || u64::from(v) > count {
            rep.dir_violations += 1;
        }
        prev = v;
    }
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
    let mut f = File::open(dir.join("rows.bin"))?;
    let t = Instant::now();
    let mut i = 0usize;
    while i < count as usize {
        let (prefix, row) = entry(i);
        match row_policy_prefixes(&mut f, u64::from(row), rows_bytes, &policies) {
            Ok(prefixes) => {
                if !prefixes.contains(&prefix) {
                    rep.mismatches += 1;
                }
            }
            Err(_) => rep.out_of_range += 1,
        }
        rep.sampled += 1;
        i += step;
    }
    rep.lookup_nanos = t.elapsed().as_nanos();
    Ok(rep)
}

/// Every *distinct* policy prefix the row at `offset` holds, resolved through the
/// interning table. Consecutive dedup, for the same reason the builder uses it:
/// a value's assets are grouped by policy, so one policy cannot reappear later in
/// a row.
fn row_policy_prefixes(
    f: &mut File,
    offset: u64,
    rows_bytes: u64,
    policies: &[[u8; 28]],
) -> Result<Vec<u64>, RowRead> {
    let base = offset + ROW_HEADER as u64;
    if base > rows_bytes {
        return Err(RowRead::OutOfRange);
    }
    let mut head = [0u8; ROW_HEADER];
    f.seek(SeekFrom::Start(offset))
        .map_err(|_| RowRead::Short)?;
    f.read_exact(&mut head).map_err(|_| RowRead::Short)?;
    let addr_len = u64::from(u16::from_le_bytes([head[49], head[50]]));
    let n_assets = u16::from_le_bytes([head[51], head[52]]);
    f.seek(SeekFrom::Start(base + addr_len))
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
        let id = u32::from_le_bytes(id) as usize;
        if id >= policies.len() {
            return Err(RowRead::Short);
        }
        let mut p = [0u8; 8];
        p.copy_from_slice(&policies[id][0..8]);
        let prefix = u64::from_le_bytes(p);
        if out.last() != Some(&prefix) {
            out.push(prefix);
        }
    }
    Ok(out)
}

enum RowRead {
    /// The row does not fit inside `rows.bin`.
    OutOfRange,
    Short,
}

/// The outref the row at `offset` claims, or why it is not a row.
///
/// A mis-pointed offset has to survive three things: the row has to start inside
/// the file, its variable-length parts have to fit inside the file, and — the
/// caller's check — its outref has to hash back to the prefix that sent us here.
///
/// The middle one is deliberately a *lower bound* rather than a parse: every
/// asset record is at least 4 (policy id) + 1 (name length) + 8 (quantity) bytes,
/// so the arithmetic needs no name lengths read. Reading each record to its end
/// would make this a full row parse, which is the caller's job, not a bound's.
fn read_row_outref(f: &mut File, offset: u64, rows_bytes: u64) -> Result<(Hash<32>, u32), RowRead> {
    let base = offset + ROW_HEADER as u64;
    if base > rows_bytes {
        return Err(RowRead::OutOfRange);
    }
    let mut head = [0u8; ROW_HEADER];
    f.seek(SeekFrom::Start(offset))
        .map_err(|_| RowRead::Short)?;
    f.read_exact(&mut head).map_err(|_| RowRead::Short)?;

    let addr_len = u64::from(u16::from_le_bytes([head[49], head[50]]));
    let n_assets = u64::from(u16::from_le_bytes([head[51], head[52]]));
    let datum_len = u64::from(u16::from_le_bytes([head[54], head[55]]));
    if base + addr_len + 13 * n_assets + datum_len > rows_bytes {
        return Err(RowRead::OutOfRange);
    }

    let mut h = [0u8; 32];
    h.copy_from_slice(&head[0..32]);
    Ok((
        Hash::from(h),
        u32::from_le_bytes(head[32..36].try_into().expect("4 bytes")),
    ))
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
