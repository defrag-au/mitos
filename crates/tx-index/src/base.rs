//! The compacted base — a prefix directory over one sorted entry array.
//!
//! Tx hashes are uniform, so bucketing on the top 24 bits gives ~7 entries
//! per bucket on mainnet and a lookup is one fence read plus a scan of one
//! or two cache lines. No tree, no probe chain, and the whole thing is a
//! read-only mapping the page cache keeps hot.

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::format::{BaseHeader, ENTRY_BYTES, Entry, Location, bucket_of};

pub const BASE_FILE: &str = "base.idx";

pub fn base_path(index_dir: &Path) -> PathBuf {
    index_dir.join(BASE_FILE)
}

pub struct BaseFile {
    mmap: Mmap,
    pub header: BaseHeader,
}

impl BaseFile {
    pub fn open(path: &Path) -> Result<BaseFile> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: the base is produced whole under a temp name and swapped in
        // by rename; nothing writes to a published base in place.
        let mmap =
            unsafe { Mmap::map(&f) }.with_context(|| format!("mapping {}", path.display()))?;
        let header = BaseHeader::read(&mmap)?;
        if mmap.len() as u64 != header.file_len() {
            bail!(
                "{}: length {} != {} implied by header",
                path.display(),
                mmap.len(),
                header.file_len()
            );
        }
        Ok(BaseFile { mmap, header })
    }

    pub fn len(&self) -> u64 {
        self.header.count
    }

    pub fn is_empty(&self) -> bool {
        self.header.count == 0
    }

    /// Era byte of a chunk the base covers.
    pub fn era_of(&self, chunk: u16) -> Option<u8> {
        if chunk < self.header.first_chunk || chunk > self.header.last_chunk {
            return None;
        }
        let at = self.header.era_off as usize + usize::from(chunk - self.header.first_chunk);
        Some(self.mmap[at])
    }

    fn fence(&self, bucket: usize) -> usize {
        let at = self.header.dir_off as usize + bucket * 4;
        u32::from_le_bytes(self.mmap[at..at + 4].try_into().expect("4 bytes")) as usize
    }

    fn raw(&self, i: usize) -> &[u8] {
        let at = self.header.entries_off as usize + i * ENTRY_BYTES;
        &self.mmap[at..at + ENTRY_BYTES]
    }

    pub fn entry(&self, i: usize) -> Entry {
        Entry::read(self.raw(i))
    }

    /// Every location whose prefix equals `prefix`. Entries within a bucket
    /// are sorted, so the scan stops at the first larger prefix.
    pub fn find(&self, prefix: u64) -> Vec<Location> {
        let b = bucket_of(prefix, self.header.dir_bits);
        let (lo, hi) = (self.fence(b), self.fence(b + 1));
        let mut out = Vec::new();
        for i in lo..hi {
            let p = Entry::prefix_at(self.raw(i));
            if p < prefix {
                continue;
            }
            if p > prefix {
                break;
            }
            out.push(Entry::read(self.raw(i)).loc);
        }
        out
    }
}
