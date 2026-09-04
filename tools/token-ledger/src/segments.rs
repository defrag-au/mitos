//! Segments — a pass's rows spilled to disk as they are produced, and the
//! compaction that folds them into the pass's final files.
//!
//! # Why the pass holds nothing
//!
//! The first archive-writing pass kept every transaction it found in memory
//! until the end, because a later chunk could reveal the source of an input
//! and the pass reached back into the transaction to append the negative
//! delta. That cost 1.7 GB of RSS on a full ClayNation walk and made
//! concurrent policies a memory question before anything else.
//!
//! But the archive's read rule is already *sum by `(tx, unit, party)` over
//! immutable files*. A backfill does not need to mutate anything: it is one
//! more signed row, exactly as a cross-pass correction already is. So the
//! pass keeps a bounded buffer, flushes it as a **segment** every
//! [`SEGMENT_ROWS`] rows or [`SEGMENT_CHUNKS`] chunks, and holds nothing
//! else. Segments are the same format as the final file — same schema, same
//! stamp, same reader — so they are servable the moment they exist. That is
//! the lower LOD a reader scrubbing into an in-progress stretch sees, and it
//! is what a satellite would publish while it walks.
//!
//! Two streams, because the footer counts transactions per file:
//!
//! - `seg-NNNN.parquet` — a transaction's rows as first found (its outputs,
//!   plus a placeholder for a minted-or-burned unit that reached nobody).
//!   Every transaction appears in exactly one segment, so the footers' tx
//!   counts sum correctly.
//! - `corr-NNNN.parquet` — every resolution, whichever pass the spender came
//!   from, as a signed row at the spender's slot. Movements, never
//!   transactions.
//!
//! # Compaction
//!
//! When the pass lands its segments are folded into `movements.parquet` (+
//! `corrections.parquet` for rows belonging to earlier passes' transactions)
//! by a streaming k-way merge: one row group per open file in memory, files
//! opened only as the merge frontier reaches them. A `(tx, unit)` group is
//! summed per party, parties that net to nothing are dropped, and a
//! transaction that moved nothing disappears — which is where the
//! change-pass-through transactions the first walk measured (89 of 160 in a
//! window) go. The rule is the reader's own, applied once so readers need
//! not.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use policy_archive::{Archive, ArchiveWriter, GroupPolicy, Movement, SparseBytes, Stamp};

use crate::archive::{FileEntry, FileKind, RangeFile};

/// Flush a segment once either buffer holds this many rows…
pub const SEGMENT_ROWS: usize = 50_000;
/// …or this many chunks have passed since the last flush, so a quiet stretch
/// still publishes and a reader watching the pass sees the floor move.
pub const SEGMENT_CHUNKS: u64 = 200;

/// Which stream a file belongs to, by name. `FileEntry` carries no kind
/// because the name already says it and a manifest edit cannot disagree
/// with the file it names.
pub fn kind_of(file: &str) -> FileKind {
    match file.starts_with("corr-") || file == crate::archive::CORRECTIONS {
        true => FileKind::Corrections,
        false => FileKind::Movements,
    }
}

/// The writer's order: block time, then transaction, then unit, then party —
/// the order the merge in [`compact`] relies on.
pub fn sort_for_writer(rows: &mut [Movement]) {
    rows.sort_by(|a, b| {
        a.block_time
            .cmp(&b.block_time)
            .then_with(|| a.tx_hash.cmp(&b.tx_hash))
            .then_with(|| a.unit_name.cmp(&b.unit_name))
            .then_with(|| a.address.cmp(&b.address))
    });
}

/// Rows → one stamped file, via a temp path so a crash never leaves a
/// half-written artifact under its final name.
pub fn write_file(path: &Path, stamp: &Stamp, rows: Vec<Movement>) -> Result<FileEntry> {
    let tmp = path.with_extension("parquet.tmp");
    let sink = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let mut w = ArchiveWriter::new(sink, stamp, GroupPolicy::DAILY)?;
    for row in rows {
        w.push(row)?;
    }
    let written = w.finish()?;
    std::fs::rename(&tmp, path)?;
    Ok(FileEntry {
        file: path
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default(),
        rows: written.rows,
        min_slot: written.min_slot,
        max_slot: written.max_slot,
    })
}

/// The pass's bounded buffer and the segments it has flushed.
pub struct SegmentWriter {
    dir: PathBuf,
    stamp: Stamp,
    own: Vec<Movement>,
    corr: Vec<Movement>,
    seq: u32,
    chunks_since_flush: u64,
    /// Every segment written so far, in order.
    pub flushed: Vec<FileEntry>,
}

impl SegmentWriter {
    /// `stamp` is what every segment is stamped with; the covered range is
    /// overwritten per segment with its own rows' bounds.
    pub fn new(dir: &Path, stamp: Stamp) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            stamp,
            own: Vec::new(),
            corr: Vec::new(),
            seq: 0,
            chunks_since_flush: 0,
            flushed: Vec::new(),
        })
    }

    /// A transaction's rows as first found.
    pub fn push_own(&mut self, rows: Vec<Movement>) {
        self.own.extend(rows);
    }

    /// A resolution — a signed row for a transaction already found.
    pub fn push_corr(&mut self, row: Movement) {
        self.corr.push(row);
    }

    /// Rows buffered and not yet on disk — the live view's share.
    pub fn buffered(&self) -> usize {
        self.own.len() + self.corr.len()
    }

    /// Called after every chunk. Flushes when a threshold is met and returns
    /// what it wrote; empty when nothing was.
    pub fn end_chunk(&mut self) -> Result<Vec<FileEntry>> {
        self.chunks_since_flush += 1;
        let due = self.own.len() >= SEGMENT_ROWS
            || self.corr.len() >= SEGMENT_ROWS
            || (self.chunks_since_flush >= SEGMENT_CHUNKS && self.buffered() > 0);
        match due {
            true => self.flush(),
            false => Ok(Vec::new()),
        }
    }

    /// Whatever is left. Nothing may be pushed after this.
    pub fn finish(mut self) -> Result<Vec<FileEntry>> {
        // `flush` records what it writes; nothing to add here.
        self.flush()?;
        Ok(self.flushed)
    }

    fn flush(&mut self) -> Result<Vec<FileEntry>> {
        let mut out = Vec::new();
        let mut own = std::mem::take(&mut self.own);
        let mut corr = std::mem::take(&mut self.corr);
        // Both streams together, so a reader mirroring the buffer can clear
        // it on one signal.
        for (prefix, rows) in [("seg", &mut own), ("corr", &mut corr)] {
            if rows.is_empty() {
                continue;
            }
            sort_for_writer(rows);
            let mut stamp = self.stamp.clone();
            stamp.covered_from = rows.iter().map(|m| m.slot).min().unwrap_or(0);
            stamp.covered_to = rows.iter().map(|m| m.slot).max().unwrap_or(0);
            let path = self.dir.join(format!("{prefix}-{:04}.parquet", self.seq));
            out.push(write_file(&path, &stamp, std::mem::take(rows))?);
        }
        if !out.is_empty() {
            self.seq += 1;
        }
        self.chunks_since_flush = 0;
        self.flushed.extend(out.iter().cloned());
        Ok(out)
    }
}

/// What compaction produced.
pub struct Compacted {
    pub movements: FileEntry,
    pub corrections: Option<FileEntry>,
    /// Transactions with at least one row in `movements`.
    pub written: u64,
    /// Distinct units with a row.
    pub units: u64,
}

/// One open segment in the merge: the file, and a cursor over its rows in
/// writer order, one row group in memory at a time.
struct Cursor {
    source: RangeFile,
    bytes: SparseBytes,
    archive: Archive,
    next_group: usize,
    rows: std::vec::IntoIter<Movement>,
    /// A row read ahead for its key and not yet handed out. The first
    /// version rebuilt the group's iterator to put a peeked row back —
    /// quadratic in group size, and it showed as a 565 MB, seven-minute
    /// compaction on ClayNation.
    head: Option<Movement>,
}

impl Cursor {
    fn open(path: &Path) -> Result<Self> {
        let (source, bytes, archive) = crate::archive::open_footer(path)?;
        Ok(Self {
            source,
            bytes,
            archive,
            next_group: 0,
            rows: Vec::new().into_iter(),
            head: None,
        })
    }

    fn next(&mut self) -> Result<Option<Movement>> {
        if let Some(m) = self.head.take() {
            return Ok(Some(m));
        }
        loop {
            if let Some(m) = self.rows.next() {
                return Ok(Some(m));
            }
            if self.next_group >= self.archive.num_groups() {
                return Ok(None);
            }
            let g = self.next_group;
            self.next_group += 1;
            let (s, l) = self.archive.group_range(g);
            self.source.fetch(&mut self.bytes, s, l)?;
            self.rows = self.archive.read_group(&self.bytes, g)?.into_iter();
        }
    }
}

/// The merge key: writer order — block time, transaction, unit, party.
type Key = (u64, Vec<u8>, Vec<u8>, String);

fn key(m: &Movement) -> Key {
    (
        m.block_time,
        m.tx_hash.clone(),
        m.unit_name.clone(),
        m.address.clone(),
    )
}

/// Fold a pass's segments into its final files.
///
/// Rows at or above `ceiling` belong to transactions an EARLIER pass
/// archived and go to `corrections.parquet`; the rest are this pass's own
/// and go to `movements.parquet`. Segments are deleted on success and left
/// untouched on failure, so a crash mid-compaction loses nothing.
pub fn compact(
    dir: &Path,
    segments: &[FileEntry],
    ceiling: u64,
    stamp: &Stamp,
) -> Result<Compacted> {
    // Files by their lowest slot, so a file is opened only when the merge
    // frontier reaches it — the k-way merge never holds more open groups
    // than the files overlapping the current moment.
    let mut files: Vec<&FileEntry> = segments.iter().collect();
    files.sort_by_key(|f| f.min_slot.unwrap_or(0));
    let mut next_file = 0usize;
    let mut cursors: Vec<Cursor> = Vec::new();
    // Min-heap on the writer key.
    let mut heap: BinaryHeap<Reverse<(Key, usize)>> = BinaryHeap::new();

    let mut movements = ArchiveWriter::new(
        File::create(dir.join(format!("{}.tmp", crate::archive::MOVEMENTS)))?,
        &Stamp {
            covered_from: segments
                .iter()
                .filter_map(|f| f.min_slot)
                .min()
                .unwrap_or(0),
            covered_to: ceiling.saturating_sub(1),
            ..stamp.clone()
        },
        GroupPolicy::DAILY,
    )?;
    let mut corrections: Option<ArchiveWriter<File>> = None;
    let mut corr_bounds: Option<(u64, u64)> = None;

    let mut written = 0u64;
    let mut units: HashSet<Vec<u8>> = HashSet::new();
    let mut written_txs: HashSet<Vec<u8>> = HashSet::new();

    // The open (tx, unit) group.
    let mut group: Option<Group> = None;

    loop {
        // Open every file whose lowest slot is at or below the frontier.
        while let Some(f) = files.get(next_file) {
            let frontier = heap.peek().map(|Reverse(((bt, _, _, _), _))| *bt);
            let file_bt = mitos_chain_walk::slot_to_unix(f.min_slot.unwrap_or(0));
            if frontier.is_some_and(|bt| file_bt > bt) {
                break;
            }
            let mut c = Cursor::open(&dir.join(&f.file))?;
            if let Some(k) = c.peek_key()? {
                heap.push(Reverse((k, cursors.len())));
                cursors.push(c);
            }
            next_file += 1;
        }
        let Some(Reverse((_, idx))) = heap.pop() else {
            break;
        };
        let m = cursors[idx]
            .next()?
            .context("a cursor in the heap has no row")?;
        if let Some(n) = cursors[idx].peek_key()? {
            heap.push(Reverse((n, idx)));
        }

        // Group boundary?
        let same = group
            .as_ref()
            .is_some_and(|g| g.tx_hash == m.tx_hash && g.unit == m.unit_name);
        if !same {
            if let Some(done) = group.take() {
                emit(
                    done,
                    ceiling,
                    dir,
                    stamp,
                    &mut movements,
                    &mut corrections,
                    &mut corr_bounds,
                    &mut written_txs,
                    &mut units,
                )?;
            }
            group = Some(Group::start(&m));
        }
        group.as_mut().expect("open group").add(&m);
    }
    if let Some(done) = group.take() {
        emit(
            done,
            ceiling,
            dir,
            stamp,
            &mut movements,
            &mut corrections,
            &mut corr_bounds,
            &mut written_txs,
            &mut units,
        )?;
    }
    written += written_txs.len() as u64;

    let mv = movements.finish()?;
    let mv_path = dir.join(crate::archive::MOVEMENTS);
    std::fs::rename(
        dir.join(format!("{}.tmp", crate::archive::MOVEMENTS)),
        &mv_path,
    )?;
    let movements_entry = FileEntry {
        file: crate::archive::MOVEMENTS.to_string(),
        rows: mv.rows,
        min_slot: mv.min_slot,
        max_slot: mv.max_slot,
    };
    let corrections_entry = match corrections {
        Some(w) => {
            let c = w.finish()?;
            let path = dir.join(crate::archive::CORRECTIONS);
            std::fs::rename(
                dir.join(format!("{}.tmp", crate::archive::CORRECTIONS)),
                &path,
            )?;
            Some(FileEntry {
                file: crate::archive::CORRECTIONS.to_string(),
                rows: c.rows,
                min_slot: c.min_slot,
                max_slot: c.max_slot,
            })
        }
        None => None,
    };
    for f in segments {
        let _ = std::fs::remove_file(dir.join(&f.file));
    }
    Ok(Compacted {
        movements: movements_entry,
        corrections: corrections_entry,
        written,
        units: units.len() as u64,
    })
}

impl Cursor {
    /// The key of the next row without consuming it.
    fn peek_key(&mut self) -> Result<Option<Key>> {
        let Some(m) = self.next()? else {
            return Ok(None);
        };
        let k = key(&m);
        self.head = Some(m);
        Ok(Some(k))
    }
}

/// One `(transaction, unit)` being summed.
struct Group {
    tx_hash: Vec<u8>,
    unit: Vec<u8>,
    slot: u64,
    block_time: u64,
    net_mint: i64,
    parties: BTreeMap<String, i64>,
    placeholder: bool,
}

impl Group {
    fn start(m: &Movement) -> Self {
        Self {
            tx_hash: m.tx_hash.clone(),
            unit: m.unit_name.clone(),
            slot: m.slot,
            block_time: m.block_time,
            net_mint: 0,
            parties: BTreeMap::new(),
            placeholder: false,
        }
    }

    fn add(&mut self, m: &Movement) {
        if m.net_mint != 0 {
            self.net_mint = m.net_mint;
        }
        match m.is_placeholder() {
            true => self.placeholder = true,
            false => *self.parties.entry(m.address.clone()).or_insert(0) += m.amount,
        }
    }

    /// The rows this group becomes: parties that moved, or a placeholder
    /// when the unit was minted or burned and nobody attributable moved it,
    /// or nothing when the asset only passed through.
    fn rows(self) -> Vec<Movement> {
        let row = |address: String, amount: i64| Movement {
            slot: self.slot,
            block_time: self.block_time,
            tx_hash: self.tx_hash.clone(),
            unit_name: self.unit.clone(),
            address,
            amount,
            net_mint: self.net_mint,
        };
        let moved: Vec<Movement> = self
            .parties
            .iter()
            .filter(|(_, a)| **a != 0)
            .map(|(addr, a)| row(addr.clone(), *a))
            .collect();
        if !moved.is_empty() {
            return moved;
        }
        if self.net_mint != 0 || self.placeholder {
            return vec![row(String::new(), 0)];
        }
        Vec::new()
    }
}

#[allow(clippy::too_many_arguments)]
fn emit(
    group: Group,
    ceiling: u64,
    dir: &Path,
    stamp: &Stamp,
    movements: &mut ArchiveWriter<File>,
    corrections: &mut Option<ArchiveWriter<File>>,
    corr_bounds: &mut Option<(u64, u64)>,
    written_txs: &mut HashSet<Vec<u8>>,
    units: &mut HashSet<Vec<u8>>,
) -> Result<()> {
    let slot = group.slot;
    let hash = group.tx_hash.clone();
    let unit = group.unit.clone();
    let rows = group.rows();
    if rows.is_empty() {
        return Ok(());
    }
    units.insert(unit);
    if slot >= ceiling {
        // An earlier pass's transaction: only the correction rows, and never
        // a placeholder — the archived row already says what was minted.
        let w = match corrections.as_mut() {
            Some(w) => w,
            None => {
                let file = File::create(dir.join(format!("{}.tmp", crate::archive::CORRECTIONS)))?;
                let lo = slot;
                *corr_bounds = Some((lo, lo));
                *corrections = Some(ArchiveWriter::new(
                    file,
                    &Stamp {
                        covered_from: lo,
                        covered_to: u64::MAX,
                        ..stamp.clone()
                    },
                    GroupPolicy::DAILY,
                )?);
                corrections.as_mut().expect("just set")
            }
        };
        for r in rows.into_iter().filter(|r| !r.is_placeholder()) {
            w.push(r)?;
        }
    } else {
        written_txs.insert(hash);
        for r in rows {
            movements.push(r)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use policy_archive::Completeness;

    fn mv(slot: u64, tx: u8, unit: &str, addr: &str, amount: i64, net_mint: i64) -> Movement {
        Movement {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![tx; 32],
            unit_name: unit.as_bytes().to_vec(),
            address: addr.to_string(),
            amount,
            net_mint,
        }
    }

    fn stamp() -> Stamp {
        Stamp {
            policy_hex: "ab".repeat(28),
            completeness: Completeness::Partial,
            walk_from: Some(0),
            walk_to: Some(1),
            covered_from: 0,
            covered_to: 1,
            sealed_unix: 0,
        }
    }

    /// Segments spill on the row threshold, corrections ride in their own
    /// stream, and compaction folds them into what the one-file pass wrote:
    /// parties summed, pass-throughs gone, placeholders kept, earlier-pass
    /// corrections routed to their own file.
    #[test]
    fn segments_compact_to_the_single_file_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut w = SegmentWriter::new(dir, stamp()).unwrap();
        // tx 1 (slot 900): alice +1 A, later resolved alice −1 A → passes
        // through; and bob +1 B, resolved from carol → a transfer.
        w.push_own(vec![
            mv(900, 1, "A", "alice", 1, 0),
            mv(900, 1, "B", "bob", 1, 0),
        ]);
        // tx 2 (slot 800): a burn nobody attributable held → placeholder.
        w.push_own(vec![mv(800, 2, "C", "", 0, -1)]);
        assert!(w.end_chunk().unwrap().is_empty(), "under the threshold");
        // Force a flush by chunk count.
        for _ in 0..SEGMENT_CHUNKS {
            let _ = w.end_chunk().unwrap();
        }
        assert_eq!(w.flushed.len(), 1);
        w.push_corr(mv(900, 1, "A", "alice", -1, 0));
        w.push_corr(mv(900, 1, "B", "carol", -1, 0));
        // A correction for a transaction an EARLIER pass archived, above the
        // ceiling.
        w.push_corr(mv(5_000, 9, "A", "dave", -1, 0));
        let segments = w.finish().unwrap();
        assert_eq!(segments.len(), 2);
        assert!(
            segments
                .iter()
                .any(|f| kind_of(&f.file) == FileKind::Corrections)
        );

        let out = compact(dir, &segments, 1_000, &stamp()).unwrap();
        assert_eq!(out.written, 2, "tx 1 moved B, tx 2 burned C");
        assert_eq!(out.units, 3);
        assert!(!dir.join("seg-0000.parquet").exists(), "segments are gone");
        let corr = out.corrections.expect("dave's correction");
        assert_eq!(corr.rows, 1);

        let mut a = crate::archive::PolicyArchive::open_files(&[
            (dir.join(&out.movements.file), FileKind::Movements),
            (dir.join(&corr.file), FileKind::Corrections),
        ])
        .unwrap();
        let rows = a.movements_page(10, None).unwrap();
        let folded = crate::archive::fold_rows(rows);
        assert_eq!(folded.len(), 2);
        let one = folded.iter().find(|r| r.tx_hash == vec![1; 32]).unwrap();
        assert_eq!(one.units.len(), 1, "A passed through and is gone");
        assert_eq!(one.units[0].name, b"B");
        assert_eq!(one.units[0].parties.len(), 2);
        let two = folded.iter().find(|r| r.tx_hash == vec![2; 32]).unwrap();
        assert!(two.units[0].parties.is_empty());
        assert_eq!(two.units[0].net_mint, -1);
        let dave = a.movements_of(&[9; 32]).unwrap();
        assert_eq!(dave.len(), 1);
        assert_eq!(dave[0].amount, -1);
    }
}
