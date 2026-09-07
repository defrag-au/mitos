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

use anyhow::{Context, Result, bail};
use policy_archive::{Archive, ArchiveWriter, GroupPolicy, Movement, SparseBytes, Stamp};

use crate::archive::{FileEntry, RangeFile};

/// Flush a segment once either buffer holds this many rows…
pub const SEGMENT_ROWS: usize = 50_000;
/// …or this many chunks have passed since the last flush, so a quiet stretch
/// still publishes and a reader watching the pass sees the floor move.
pub const SEGMENT_CHUNKS: u64 = 200;

pub use crate::archive::kind_of;

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
        units: 0,
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
    let inputs: Vec<PathBuf> = segments.iter().map(|f| dir.join(&f.file)).collect();
    let out = merge_files(&inputs, dir, crate::archive::MOVEMENTS, ceiling, stamp)?;
    for p in &inputs {
        let _ = std::fs::remove_file(p);
    }
    Ok(out)
}

/// How many uncompacted passes an archive carries before it is rolled up on
/// landing. Every extra pass is a footer per read for every reader.
pub const ROLLUP_AFTER_PASSES: usize = 3;

/// Fold EVERYTHING the manifest names — rollup, passes, corrections — into
/// one file at the policy's root, and rewrite the manifest to point at it.
///
/// The merge is the one [`compact`] uses with the ceiling at infinity: every
/// row is "own", so a correction lands on the row it corrects and the
/// output has no corrections file. Sequence numbers keep counting, so a
/// reader mid-flight on the old manifest still finds old files until the
/// prune; the manifest is written BEFORE the old files are removed.
pub fn rollup(dir: &Path, manifest: &mut crate::archive::Manifest, sealed_unix: u64) -> Result<()> {
    // IMMUTABLE only: the volatile tail is replaced whole on its next
    // refresh, so folding it into a file nothing rewrites would freeze a
    // stretch that can still roll back.
    let old_files = manifest.immutable_files();
    if old_files.len() < 2 {
        return Ok(());
    }
    let inputs: Vec<PathBuf> = old_files.iter().map(|(rel, _)| dir.join(rel)).collect();
    // The ROLLUP's own sequence, not the pass sequence — see
    // `Manifest::next_rollup_seq` for the archive this distinction cost.
    let rollup_seq = manifest.next_rollup_seq();
    let name = format!("archive-{rollup_seq:04}.parquet");
    // The invariant that would have made that a loud failure instead of a
    // silent deletion: the merge must never write over one of its inputs,
    // because the prune below removes every input.
    let out_path = dir.join(&name);
    if inputs.contains(&out_path) {
        bail!(
            "rollup would write {} over one of its own inputs — refusing; \
             the rollup sequence is not unique",
            out_path.display()
        );
    }
    let stamp = Stamp {
        policy_hex: manifest.policy.clone(),
        completeness: manifest.completeness(),
        walk_from: manifest.walk_from(),
        walk_to: manifest.walk_to(),
        covered_from: manifest.walk_from().unwrap_or(0),
        covered_to: manifest.walk_to().unwrap_or(u64::MAX),
        sealed_unix,
    };
    let out = merge_files(&inputs, dir, &name, u64::MAX, &stamp)?;

    // The carried state moves to the root before the pass directory that
    // held it goes.
    if let Some(rel) = manifest.pending_file() {
        let from = dir.join(&rel);
        if from.exists() && rel != crate::archive::PENDING {
            std::fs::copy(&from, dir.join(crate::archive::PENDING))?;
        }
    }
    let previous_rollup = manifest.rollup.take();
    manifest.rollup = Some(FileEntry {
        units: out.units,
        ..out.movements
    });
    // EXACTLY the passes whose files went into the merge — every pass the
    // manifest named when `old_files` was taken. A pass with a lower `seq`
    // that lands later is a LOOSE pass, not a folded one: it was still in
    // flight, its rows are not in this file, and marking it by a threshold
    // over `seq` is how 72 days of ClayNation were deleted.
    for pass in manifest
        .passes
        .iter_mut()
        .filter(|p| p.kind == crate::archive::RangeKind::Immutable)
    {
        pass.rolled_up = true;
    }
    manifest.rolled_up_through = manifest.latest_pass().map(|p| p.seq);
    manifest.rollup_seq = rollup_seq;
    manifest.pending = Some(crate::archive::PENDING.to_string());
    manifest.updated_unix = sealed_unix;
    crate::archive::store_manifest(dir, manifest)?;
    crate::archive::store_bundle(dir, manifest)?;

    // Prune: the files the new manifest no longer names.
    for p in &inputs {
        let _ = std::fs::remove_file(p);
    }
    if let Some(prev) = previous_rollup {
        let _ = std::fs::remove_file(dir.join(prev.file));
    }
    for pass in manifest.passes.iter().filter(|p| p.rolled_up) {
        let _ = std::fs::remove_dir_all(dir.join(&pass.dir));
    }
    Ok(())
}

#[derive(clap::Args, Debug)]
pub struct RollupArgs {
    /// Archive root (`<root>/<policy_hex>/manifest.json`).
    #[arg(long, default_value = "archive")]
    pub archive_dir: PathBuf,
    /// 56-hex policy id.
    #[arg(long)]
    pub policy: String,
}

/// `token-ledger rollup` — fold a policy's passes into one file now.
pub fn run_rollup(args: RollupArgs) -> Result<()> {
    let dir = crate::archive::policy_dir(&args.archive_dir, &args.policy.to_lowercase());
    let Some(mut manifest) = crate::archive::load_manifest(&dir)? else {
        anyhow::bail!("no archive at {}", dir.display());
    };
    let before = manifest.files().len();
    let t = std::time::Instant::now();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    rollup(&dir, &mut manifest, now)?;
    let r = manifest.rollup.as_ref();
    println!(
        "rolled {before} files into {} ({} rows, {} units) in {:.1}s",
        r.map_or("nothing", |r| r.file.as_str()),
        r.map_or(0, |r| r.rows),
        r.map_or(0, |r| r.units),
        t.elapsed().as_secs_f64()
    );
    Ok(())
}

/// The streaming k-way merge behind [`compact`] and [`rollup`]: `inputs`
/// (any mix of movement and correction files, each in writer order) → one
/// movements file named `movements_name` in `out_dir`, plus
/// `corrections.parquet` beside it for rows at or above `ceiling`.
fn merge_files(
    inputs: &[PathBuf],
    out_dir: &Path,
    movements_name: &str,
    ceiling: u64,
    stamp: &Stamp,
) -> Result<Compacted> {
    // Footers first, to order the files by their lowest slot: a file is then
    // opened for rows only when the merge frontier reaches it, so the k-way
    // merge never holds more open groups than the files overlapping the
    // current moment.
    let mut files: Vec<(u64, &PathBuf)> = Vec::with_capacity(inputs.len());
    for p in inputs {
        let (_, _, archive) = crate::archive::open_footer(p)?;
        files.push((archive.stamp().covered_from, p));
    }
    files.sort_by_key(|(from, _)| *from);
    let lowest = files.first().map_or(0, |(from, _)| *from);
    let mut next_file = 0usize;
    let mut cursors: Vec<Cursor> = Vec::new();
    // Min-heap on the writer key.
    let mut heap: BinaryHeap<Reverse<(Key, usize)>> = BinaryHeap::new();

    let mv_tmp = out_dir.join(format!("{movements_name}.tmp"));
    let mut movements = ArchiveWriter::new(
        File::create(&mv_tmp)?,
        &Stamp {
            covered_from: lowest.min(stamp.covered_from),
            covered_to: stamp.covered_to.min(ceiling.saturating_sub(1)),
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
    let dir = out_dir;

    loop {
        // Open every file whose lowest slot is at or below the frontier.
        while let Some((from, path)) = files.get(next_file) {
            let frontier = heap.peek().map(|Reverse(((bt, _, _, _), _))| *bt);
            let file_bt = mitos_chain_walk::slot_to_unix(*from);
            if frontier.is_some_and(|bt| file_bt > bt) {
                break;
            }
            let mut c = Cursor::open(path)?;
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
    std::fs::rename(&mv_tmp, dir.join(movements_name))?;
    let movements_entry = FileEntry {
        file: movements_name.to_string(),
        rows: mv.rows,
        min_slot: mv.min_slot,
        max_slot: mv.max_slot,
        units: units.len() as u64,
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
                units: 0,
            })
        }
        None => None,
    };
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
    // COUNTED, not recorded. The rows are written either way — the archive is
    // a record of what happened on chain and a metadata update is something
    // that happened — but `units` is the number a reader is shown as the size
    // of the collection, and a policy's asset list is not its supply.
    //
    // A CIP-68 mint creates a reference twin per collectible, so counting the
    // raw set doubled Mekka S1: 5,000 NFTs reported as 9,690 units. CIP-27
    // adds one empty-named royalty token with a supply of zero, which is the
    // `+1` on every "6,001 units" for a 6,000-piece drop. The same rule is
    // applied to the holder field in the frontend, from the same crate, so
    // the header and the picture cannot state different sizes.
    if cardano_assets::AssetRole::of_bytes(&unit).standing() == cardano_assets::UnitStanding::Unit {
        units.insert(unit);
    }
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
    use crate::archive::FileKind;
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

    /// The same, for a unit name that is not valid UTF-8 — every CIP-68
    /// asset, whose `000643b0` label ends in a byte no `&str` can hold.
    fn mv_raw(slot: u64, tx: u8, unit: Vec<u8>, addr: &str, amount: i64) -> Movement {
        Movement {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![tx; 32],
            unit_name: unit,
            address: addr.to_string(),
            amount,
            net_mint: 0,
        }
    }

    /// A POLICY'S ASSET LIST IS NOT ITS SUPPLY.
    ///
    /// `units` is the number a reader is shown as the size of the collection,
    /// and three kinds of token live under a collection's policy. Counting
    /// them all reported Mekka S1 — 5,000 NFTs — as 9,690 units, and put a
    /// `+1` on every CIP-25 drop for its royalty token.
    ///
    /// The ROWS are unaffected and that is the point: the archive records
    /// what happened on chain, including the metadata update, and only the
    /// count is a judgement about what a collection is.
    #[test]
    fn plumbing_is_recorded_but_not_counted_as_a_unit() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut w = SegmentWriter::new(dir, stamp()).unwrap();
        let labelled = |label: [u8; 4]| {
            let mut name = label.to_vec();
            name.extend_from_slice(b"MD0001");
            name
        };
        w.push_own(vec![
            // The collectible, and its metadata twin in the same transaction.
            mv_raw(900, 1, labelled([0x00, 0x0d, 0xe1, 0x40]), "alice", 1),
            mv_raw(900, 1, labelled([0x00, 0x06, 0x43, 0xb0]), "vault", 1),
            // A plain CIP-25 asset, and the CIP-27 royalty token: no name.
            mv(900, 1, "Perp2214", "bob", 1, 0),
            mv_raw(900, 1, Vec::new(), "vault", 1),
        ]);
        for _ in 0..=SEGMENT_CHUNKS {
            let _ = w.end_chunk().unwrap();
        }
        let segments = w.finish().unwrap();
        let out = compact(dir, &segments, 1_000, &stamp()).unwrap();

        assert_eq!(
            out.units, 2,
            "one CIP-68 collectible and one CIP-25 asset — the reference twin \
             and the royalty token are not units of the collection"
        );

        // …and all four movements are still in the archive.
        let mut a = crate::archive::PolicyArchive::open_files(&[(
            dir.join(&out.movements.file),
            FileKind::Movements,
        )])
        .unwrap();
        let rows = a.movements_page(10, None).unwrap();
        let names: std::collections::HashSet<Vec<u8>> =
            rows.iter().map(|r| r.unit_name.clone()).collect();
        assert_eq!(
            names.len(),
            4,
            "the reference twin and the royalty token are RECORDED, just not \
             counted — the archive is a record, the count is a judgement"
        );
    }

    /// A rollup folds every file the manifest names into one at the root,
    /// takes over the pending set, and leaves the manifest pointing at only
    /// that — with the same rows the separate files gave.
    #[test]
    fn a_rollup_folds_the_passes_into_one_file() {
        use crate::archive::{Manifest, PassEntry, PolicyArchive, store_manifest, store_pending};
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let mut manifest = Manifest::new(&"ab".repeat(28));
        // Pass 0: two transactions, one waiting on a source.
        let p0 = dir.join(PassEntry::dir_name(0));
        std::fs::create_dir_all(&p0).unwrap();
        let mv0 = write_file(
            &p0.join(crate::archive::MOVEMENTS),
            &stamp(),
            vec![mv(900, 1, "A", "bob", 1, 0), mv(950, 2, "B", "carol", 1, 1)],
        )
        .unwrap();
        store_pending(
            &p0.join(crate::archive::PENDING),
            &crate::archive::PendingFile::default(),
        )
        .unwrap();
        // Pass 1: one older transaction and the correction for tx 1.
        let p1 = dir.join(PassEntry::dir_name(1));
        std::fs::create_dir_all(&p1).unwrap();
        let mv1 = write_file(
            &p1.join(crate::archive::MOVEMENTS),
            &stamp(),
            vec![mv(500, 3, "A", "alice", 1, 0)],
        )
        .unwrap();
        let corr1 = write_file(
            &p1.join(crate::archive::CORRECTIONS),
            &stamp(),
            vec![mv(900, 1, "A", "alice", -1, 0)],
        )
        .unwrap();
        store_pending(
            &p1.join(crate::archive::PENDING),
            &crate::archive::PendingFile::default(),
        )
        .unwrap();
        for (seq, mvs, corr) in [(0, mv0, None), (1, mv1, Some(corr1))] {
            manifest.passes.push(PassEntry {
                seq,
                dir: PassEntry::dir_name(seq),
                ceiling: 1_000,
                floor: 400,
                windows: Vec::new(),
                kind: crate::archive::RangeKind::Immutable,
                rolled_up: false,
                movements: Some(mvs),
                corrections: corr,
                segments: Vec::new(),
                pending: 0,
                found: 0,
                written: 0,
                backfilled: 0,
                units: 0,
                secs: 0.0,
                written_unix: 0,
            });
        }
        store_manifest(dir, &manifest).unwrap();

        let before = PolicyArchive::open(dir)
            .unwrap()
            .unwrap()
            .feed_rows(10, None)
            .unwrap();
        rollup(dir, &mut manifest, 7).unwrap();
        assert_eq!(manifest.files().len(), 1, "one file, at the root");
        assert_eq!(manifest.rolled_up_through, Some(1));
        assert!(dir.join(crate::archive::PENDING).exists());
        assert!(!p0.exists() && !p1.exists(), "pass directories are gone");
        let r = manifest.rollup.as_ref().unwrap();
        assert_eq!(r.units, 2);

        let mut a = PolicyArchive::open(dir).unwrap().unwrap();
        let after = a.feed_rows(10, None).unwrap();
        assert_eq!(after, before, "the rollup reads exactly as the passes did");
        let one = after.iter().find(|r| r.tx_hash == vec![1; 32]).unwrap();
        assert_eq!(one.units[0].parties.len(), 2, "the correction is folded in");
        assert_eq!(a.coverage().total_txs, 3);
    }
}
