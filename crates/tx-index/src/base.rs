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

use crate::format::{BaseHeader, ENTRY_BYTES, Entry};
use crate::locate::{Span, entries_span, era_span, fence_pair, fence_span, search};

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
        let span = era_span(&self.header, chunk)?;
        Some(self.mmap[span.offset as usize])
    }

    fn span_bytes(&self, span: Span) -> &[u8] {
        span.of(&self.mmap)
    }

    fn raw(&self, i: usize) -> &[u8] {
        let at = self.header.entries_off as usize + i * ENTRY_BYTES;
        &self.mmap[at..at + ENTRY_BYTES]
    }

    pub fn entry(&self, i: usize) -> Entry {
        Entry::read(self.raw(i))
    }

    /// Every entry whose prefix equals `prefix`. Entries within a bucket are
    /// sorted, so the scan stops at the first larger prefix. Returns whole
    /// entries so the caller can reach the aux span without a second read.
    ///
    /// The offsets come from [`crate::locate`], so a reader over the network
    /// and this one over the mapping are locating with the same arithmetic.
    pub fn find(&self, prefix: u64) -> Vec<Entry> {
        let (lo, hi) = fence_pair(self.span_bytes(fence_span(&self.header, prefix)))
            .expect("a mapped base's fence pair is 8 bytes");
        let run = self.span_bytes(entries_span(&self.header, lo, hi));
        // The mapping holds the whole run, so the truncation guard in `search`
        // — which exists for callers that fetched, and may have fetched short —
        // cannot fire here.
        search(prefix, lo, hi, run).expect("the mapping holds every entry of the run")
    }
}
