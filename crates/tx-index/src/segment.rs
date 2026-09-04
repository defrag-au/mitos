//! Per-chunk segment files — the append-only extraction layer.
//!
//! `segments/NNNNN.seg` mirrors `NNNNN.chunk` one to one. Written once via
//! a `.tmp` + rename, never rewritten; a corrupt or missing segment is
//! re-extracted from its chunk, nothing else is affected.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::format::{
    ENTRY_BYTES, Entry, Location, SEGMENT_HEADER_BYTES, SegmentHeader, chunk_number,
};

pub const SEGMENTS_DIR: &str = "segments";

pub fn segments_dir(index_dir: &Path) -> PathBuf {
    index_dir.join(SEGMENTS_DIR)
}

pub fn segment_path(index_dir: &Path, chunk: u16) -> PathBuf {
    segments_dir(index_dir).join(format!("{chunk:05}.seg"))
}

/// Sorted chunk numbers that have a segment on disk.
pub fn list_segments(index_dir: &Path) -> Result<Vec<u16>> {
    let dir = segments_dir(index_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut nums: Vec<u16> = std::fs::read_dir(&dir)
        .with_context(|| format!("reading {}", dir.display()))?
        .filter_map(|e| {
            let name = e.ok()?.file_name().into_string().ok()?;
            let n = chunk_number(&name, ".seg")?;
            u16::try_from(n).ok()
        })
        .collect();
    nums.sort_unstable();
    Ok(nums)
}

/// Write a segment atomically. `entries` must already be sorted by prefix.
pub fn write_segment(index_dir: &Path, hdr: SegmentHeader, entries: &[Entry]) -> Result<PathBuf> {
    if entries.len() != hdr.count as usize {
        bail!(
            "segment {} header count {} != {} entries",
            hdr.chunk,
            hdr.count,
            entries.len()
        );
    }
    debug_assert!(entries.windows(2).all(|w| w[0].prefix <= w[1].prefix));

    let dir = segments_dir(index_dir);
    std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let final_path = segment_path(index_dir, hdr.chunk);
    let tmp = final_path.with_extension("seg.tmp");

    let mut buf = vec![0u8; SEGMENT_HEADER_BYTES + entries.len() * ENTRY_BYTES];
    hdr.write(&mut buf[..SEGMENT_HEADER_BYTES]);
    for (i, e) in entries.iter().enumerate() {
        let at = SEGMENT_HEADER_BYTES + i * ENTRY_BYTES;
        e.write(&mut buf[at..at + ENTRY_BYTES]);
    }

    let mut f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, &final_path)
        .with_context(|| format!("renaming {} → {}", tmp.display(), final_path.display()))?;
    Ok(final_path)
}

/// A memory-mapped segment.
pub struct SegmentFile {
    mmap: Mmap,
    pub header: SegmentHeader,
}

impl SegmentFile {
    pub fn open(path: &Path) -> Result<SegmentFile> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: the file is written once via rename and never modified in
        // place; a concurrent unlink keeps the mapping valid until dropped.
        let mmap =
            unsafe { Mmap::map(&f) }.with_context(|| format!("mapping {}", path.display()))?;
        let header = SegmentHeader::read(&mmap)?;
        let want = SEGMENT_HEADER_BYTES + header.count as usize * ENTRY_BYTES;
        if mmap.len() != want {
            bail!(
                "{}: length {} != {want} implied by header",
                path.display(),
                mmap.len()
            );
        }
        Ok(SegmentFile { mmap, header })
    }

    pub fn len(&self) -> usize {
        self.header.count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.header.count == 0
    }

    fn raw(&self, i: usize) -> &[u8] {
        let at = SEGMENT_HEADER_BYTES + i * ENTRY_BYTES;
        &self.mmap[at..at + ENTRY_BYTES]
    }

    pub fn entry(&self, i: usize) -> Entry {
        Entry::read(self.raw(i))
    }

    /// Prefix of entry `i` without decoding the location — the compactor's
    /// histogram pass touches every entry and wants nothing else.
    pub fn prefix(&self, i: usize) -> u64 {
        Entry::prefix_at(self.raw(i))
    }

    pub fn entries(&self) -> impl Iterator<Item = Entry> + '_ {
        (0..self.len()).map(|i| self.entry(i))
    }

    /// Every location whose prefix equals `prefix` (sorted → binary search
    /// to the run, then walk it).
    pub fn find(&self, prefix: u64) -> Vec<Location> {
        let n = self.len();
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if Entry::prefix_at(self.raw(mid)) < prefix {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let mut out = Vec::new();
        let mut i = lo;
        while i < n {
            let e = self.entry(i);
            if e.prefix != prefix {
                break;
            }
            out.push(e.loc);
            i += 1;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tx-index-seg-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn e(prefix: u64, offset: u32) -> Entry {
        Entry {
            prefix,
            loc: Location {
                chunk: 7,
                offset,
                len: 100,
            },
        }
    }

    #[test]
    fn write_then_find() {
        let dir = scratch("find");
        let entries = vec![e(1, 10), e(5, 20), e(5, 30), e(9, 40)];
        let hdr = SegmentHeader {
            era: 6,
            chunk: 7,
            count: 4,
        };
        write_segment(&dir, hdr, &entries).unwrap();
        assert_eq!(list_segments(&dir).unwrap(), vec![7]);

        let seg = SegmentFile::open(&segment_path(&dir, 7)).unwrap();
        assert_eq!(seg.header, hdr);
        assert_eq!(seg.find(0), vec![]);
        assert_eq!(seg.find(1).len(), 1);
        let five = seg.find(5);
        assert_eq!(five.len(), 2);
        assert_eq!(five[0].offset, 20);
        assert_eq!(five[1].offset, 30);
        assert_eq!(seg.find(9)[0].offset, 40);
        assert_eq!(seg.find(10), vec![]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn empty_segment_is_fine() {
        let dir = scratch("empty");
        let hdr = SegmentHeader {
            era: 1,
            chunk: 0,
            count: 0,
        };
        write_segment(&dir, hdr, &[]).unwrap();
        let seg = SegmentFile::open(&segment_path(&dir, 0)).unwrap();
        assert!(seg.is_empty());
        assert_eq!(seg.find(0), vec![]);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
