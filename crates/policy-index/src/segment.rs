//! Per-chunk segment files — the append-only extraction layer.
//!
//! `policy/segments/NNNNN.pseg` mirrors `NNNNN.chunk` one to one, written once
//! via `.tmp` + rename and never rewritten. Same discipline as the tx-index's
//! segments and for the same reason: a chunk file is immutable once complete,
//! so its segment is a fact that never needs revisiting. A corrupt or missing
//! one is re-extracted from its chunk and nothing else is affected.
//!
//! ⚠️ The extension is `.pseg`, not `.seg`. The two indexes live under one
//! index root and their files are the same shape from a distance; a shared
//! extension plus a mistaken directory would let one be read as the other,
//! which the magic check would catch — but only after a directory listing had
//! already promised the wrong thing.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::format::{RECORD_BYTES, Record, SEGMENT_HEADER_BYTES, SegmentHeader, chunk_number};

pub const POLICY_DIR: &str = "policy";
pub const SEGMENTS_DIR: &str = "segments";
pub const SEGMENT_EXT: &str = ".pseg";

pub fn policy_dir(index_dir: &Path) -> PathBuf {
    index_dir.join(POLICY_DIR)
}

pub fn segments_dir(index_dir: &Path) -> PathBuf {
    policy_dir(index_dir).join(SEGMENTS_DIR)
}

pub fn segment_path(index_dir: &Path, chunk: u16) -> PathBuf {
    segments_dir(index_dir).join(format!("{chunk:05}{SEGMENT_EXT}"))
}

/// Sorted chunk numbers that have a policy segment on disk.
pub fn list_segments(index_dir: &Path) -> Result<Vec<u16>> {
    let dir = segments_dir(index_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut nums: Vec<u16> = std::fs::read_dir(&dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let n = chunk_number(&name, SEGMENT_EXT)?;
            u16::try_from(n).ok()
        })
        .collect();
    nums.sort_unstable();
    Ok(nums)
}

/// Write a segment atomically. `records` must already be in ASSET order.
pub fn write_segment(index_dir: &Path, chunk: u16, records: &[Record]) -> Result<PathBuf> {
    debug_assert!(
        records
            .windows(2)
            .all(|w| w[0].asset_key() <= w[1].asset_key()),
        "records must be sorted before writing — compaction merges on that order"
    );
    let count = u32::try_from(records.len())
        .with_context(|| format!("chunk {chunk}: {} records exceed u32", records.len()))?;

    let dir = segments_dir(index_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let final_path = segment_path(index_dir, chunk);
    let tmp = final_path.with_extension("pseg.tmp");

    let mut buf = vec![0u8; SEGMENT_HEADER_BYTES + records.len() * RECORD_BYTES];
    SegmentHeader { chunk, count }.write(&mut buf[..SEGMENT_HEADER_BYTES]);
    for (i, r) in records.iter().enumerate() {
        let at = SEGMENT_HEADER_BYTES + i * RECORD_BYTES;
        r.write(&mut buf[at..at + RECORD_BYTES]);
    }

    let mut f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    // Rename LAST: a reader either sees no segment or a whole one, never a
    // partially written file that parses.
    std::fs::rename(&tmp, &final_path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), final_path.display()))?;
    Ok(final_path)
}

/// A mapped segment.
pub struct Segment {
    mmap: Mmap,
    pub header: SegmentHeader,
}

impl Segment {
    pub fn open(path: &Path) -> Result<Segment> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: segments are written whole under a temp name and renamed in;
        // nothing writes to a published segment in place.
        let mmap =
            unsafe { Mmap::map(&f) }.with_context(|| format!("mapping {}", path.display()))?;
        let header = SegmentHeader::read(&mmap)?;
        let want = SEGMENT_HEADER_BYTES + header.count as usize * RECORD_BYTES;
        if mmap.len() != want {
            bail!(
                "{}: length {} != {want} implied by header count {}",
                path.display(),
                mmap.len(),
                header.count
            );
        }
        Ok(Segment { mmap, header })
    }

    pub fn len(&self) -> usize {
        self.header.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.header.count == 0
    }

    pub fn record(&self, i: usize) -> Option<Record> {
        if i >= self.len() {
            return None;
        }
        let at = SEGMENT_HEADER_BYTES + i * RECORD_BYTES;
        Some(Record::read(&self.mmap[at..at + RECORD_BYTES]))
    }

    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        (0..self.len()).map(|i| self.record(i).expect("in range"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(p: u64, n: u64, c: u16) -> Record {
        Record {
            policy_prefix: p,
            name_prefix: n,
            chunk: c,
            offset: 10,
            len: 20,
            aux_offset: 30,
            aux_len: 40,
            burned: false,
        }
    }

    #[test]
    fn a_segment_round_trips_through_the_reader() {
        let dir = tempfile::tempdir().unwrap();
        let records = vec![rec(1, 1, 5), rec(1, 2, 5), rec(9, 1, 5)];
        let path = write_segment(dir.path(), 5, &records).unwrap();

        let seg = Segment::open(&path).unwrap();
        assert_eq!(seg.header.chunk, 5);
        assert_eq!(seg.len(), 3);
        assert_eq!(seg.records().collect::<Vec<_>>(), records);
    }

    #[test]
    fn an_empty_segment_is_legal() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 12, &[]).unwrap();
        let seg = Segment::open(&path).unwrap();
        assert!(seg.is_empty());
        assert_eq!(seg.records().count(), 0);
    }

    #[test]
    fn segments_are_listed_in_chunk_order() {
        let dir = tempfile::tempdir().unwrap();
        for c in [9u16, 1, 5] {
            write_segment(dir.path(), c, &[rec(1, 1, c)]).unwrap();
        }
        assert_eq!(list_segments(dir.path()).unwrap(), vec![1, 5, 9]);
    }

    #[test]
    fn listing_an_index_with_no_policy_segments_is_empty_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        assert!(list_segments(dir.path()).unwrap().is_empty());
    }

    /// ⚠️ A truncated file must FAIL, not return the records that happen to
    /// be present — a short read here would silently drop mints.
    #[test]
    fn a_truncated_segment_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 3, &[rec(1, 1, 3), rec(2, 2, 3)]).unwrap();
        let full = std::fs::read(&path).unwrap();
        std::fs::write(&path, &full[..full.len() - RECORD_BYTES / 2]).unwrap();
        assert!(Segment::open(&path).is_err());
    }

    /// The `.tmp` is renamed only after `sync_all`, so a listing never names a
    /// file a reader cannot open.
    #[test]
    fn writing_leaves_no_temp_file_behind() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 4, &[rec(1, 1, 4)]).unwrap();
        let leftovers: Vec<String> = std::fs::read_dir(segments_dir(dir.path()))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "left {leftovers:?}");
    }

    /// The two indexes share an index root. A policy segment must not land
    /// where the tx-index lists its own.
    #[test]
    fn policy_segments_live_under_their_own_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_segment(dir.path(), 1, &[rec(1, 1, 1)]).unwrap();
        assert!(path.starts_with(policy_dir(dir.path())));
        assert!(path.to_string_lossy().ends_with(".pseg"));
    }
}
