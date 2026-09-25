//! Where an artifact's bytes come from.
//!
//! The reader's parsing is already abstract over `&[u8]` — `entry_at`,
//! `bucket_at` and `Row::parse` all take a slice — so only *acquisition* is
//! mmap-specific. This is the seam that makes it swappable, so the same reader
//! serves a file on the box, an object over HTTP range requests, and a browser's
//! cache, per `MITOS_AT_THE_EDGE.md` §10.5.
//!
//! # Two reads, because the format needs both
//!
//! [`Source::read`] is the ordinary case: a known offset and a known length — an
//! index entry, a directory bucket, a policy id.
//!
//! [`Source::read_upto`] exists because **a row's length is not knowable until
//! its header is parsed.** `Row::parse` is handed an open-ended slice and works
//! out where the row ends from `addr_len` and `n_assets`. Locally that is free;
//! remotely it would be two dependent round trips per row, so the remote impl
//! over-reads speculatively (§4.5: median row 166 B, so 1 KiB covers
//! essentially every row). Putting it in the trait makes that a property of the
//! seam rather than a trick one implementation plays.
//!
//! # Zero-copy is not negotiable locally
//!
//! `LIVE_BOOK.md` §Decided rests on it: spend resolution confirms from memory,
//! and over ~138 M resolutions the difference between a borrow and a re-read is
//! **69 minutes versus 77 hours**. So [`Source::Bytes`] is a *borrow* rather than
//! a `Vec`, and `MmapSource` returns `&[u8]` with no copy at all. A remote
//! implementation pays a copy because it must; the local one must not pay for
//! its existence.
//!
//! # `mmap` off, and why the default type parameter survives it
//!
//! `memmap2` does not build for `wasm32-unknown-unknown`, so the file-backed
//! source is behind the `mmap` feature. But `Base`, `Table`, `Directory` and
//! `PolicyTable` all *default* their source parameter, and eight call sites
//! across mitos rely on that default. A `cfg` cannot be put on a default type
//! parameter, and duplicating four struct definitions to vary it would be worse
//! than the problem.
//!
//! So the default names [`DefaultSource`], which exists under both
//! configurations: `MmapSource` with the feature on, and an **uninhabited** enum
//! with it off. `Base<DefaultSource>` then still typechecks in a wasm build and
//! is simply unconstructible — which is the honest answer, because a wasm
//! consumer has to name its own source.

use std::ops::Deref;
#[cfg(feature = "mmap")]
use std::path::Path;

use anyhow::Result;
#[cfg(feature = "mmap")]
use anyhow::{Context, bail};

/// A place bytes can be read from, by offset.
pub trait Source {
    /// A borrow of the bytes read. `&[u8]` for a mapping; a guard into a page
    /// cache for anything remote.
    type Bytes<'a>: Deref<Target = [u8]>
    where
        Self: 'a;

    /// Exactly `len` bytes at `offset`. Short reads are an error: every caller
    /// of this knows the width it wants, so a truncated answer is corruption.
    fn read(&self, offset: u64, len: usize) -> Result<Self::Bytes<'_>>;

    /// **Up to** `max` bytes at `offset`, and fewer at the end of the source.
    ///
    /// For a variable-length record whose length is inside it — see the module
    /// header. A caller must tolerate getting less than it asked for, and must
    /// check that what it got was enough.
    fn read_upto(&self, offset: u64, max: usize) -> Result<Self::Bytes<'_>>;

    /// The total size, for the length checks `Base::open` makes against
    /// `book.meta`.
    fn size(&self) -> u64;
}

/// What every source parameter in this crate defaults to. See the module header
/// for why this is an alias rather than a `cfg` on the default itself.
#[cfg(feature = "mmap")]
pub type DefaultSource = MmapSource;

/// The default with `mmap` off: **uninhabited**, so a `Base<DefaultSource>` is
/// well-typed and cannot be built.
///
/// Deliberately not a unit struct that errors at runtime. A type with no values
/// makes "there is no file-backed source in this build" a compile-time fact, and
/// the `match *self {}` arms below are how the compiler is told the methods are
/// unreachable rather than unimplemented.
#[cfg(not(feature = "mmap"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultSource {}

#[cfg(not(feature = "mmap"))]
impl Source for DefaultSource {
    type Bytes<'a> = &'a [u8];

    fn read(&self, _offset: u64, _len: usize) -> Result<&[u8]> {
        match *self {}
    }

    fn read_upto(&self, _offset: u64, _max: usize) -> Result<&[u8]> {
        match *self {}
    }

    fn size(&self) -> u64 {
        match *self {}
    }
}

/// A read-only mapping — the local case, and the one that must stay zero-copy.
///
/// # Safety
///
/// `Mmap::map`'s: the file must not be mutated for the life of the mapping.
/// Everything this crate writes satisfies that by swapping rather than
/// truncating, and a published artifact is moved into place by `rename`, so a
/// mapping never observes a write (`format`'s header).
#[cfg(feature = "mmap")]
pub struct MmapSource {
    map: memmap2::Mmap,
}

#[cfg(feature = "mmap")]
impl MmapSource {
    pub fn open(path: &Path) -> Result<MmapSource> {
        let f = std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let map = unsafe { memmap2::Mmap::map(&f) }
            .with_context(|| format!("mapping {}", path.display()))?;
        Ok(MmapSource { map })
    }

    /// The whole mapping, for the paths that still scan it directly.
    pub fn bytes(&self) -> &[u8] {
        &self.map
    }
}

#[cfg(feature = "mmap")]
impl Source for MmapSource {
    type Bytes<'a> = &'a [u8];

    fn read(&self, offset: u64, len: usize) -> Result<&[u8]> {
        let at = usize::try_from(offset).context("offset does not fit this machine's usize")?;
        let end = at.checked_add(len).context("offset + len overflows")?;
        if end > self.map.len() {
            bail!(
                "read of {len} B at {offset} runs past the source's {} B",
                self.map.len()
            );
        }
        Ok(&self.map[at..end])
    }

    fn read_upto(&self, offset: u64, max: usize) -> Result<&[u8]> {
        let at = usize::try_from(offset).context("offset does not fit this machine's usize")?;
        if at > self.map.len() {
            bail!(
                "read at {offset} starts past the source's {} B",
                self.map.len()
            );
        }
        let end = at.saturating_add(max).min(self.map.len());
        Ok(&self.map[at..end])
    }

    fn size(&self) -> u64 {
        self.map.len() as u64
    }
}

#[cfg(all(test, feature = "mmap"))]
mod tests {
    use super::*;
    use std::io::Write;

    fn fixture(name: &str, bytes: &[u8]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join("live-book-source-tests");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(bytes).unwrap();
        path
    }

    #[test]
    fn read_is_exact_and_refuses_to_run_past_the_end() {
        let path = fixture("exact.bin", &[0, 1, 2, 3, 4, 5, 6, 7]);
        let s = MmapSource::open(&path).unwrap();

        assert_eq!(s.size(), 8);
        assert_eq!(s.read(2, 3).unwrap(), [2u8, 3, 4].as_slice());
        assert_eq!(s.read(5, 3).unwrap(), [5u8, 6, 7].as_slice());

        // A short read is corruption, not a partial answer: every caller of
        // `read` knows the width it wants.
        assert!(s.read(6, 3).is_err(), "must not silently truncate");
        assert!(s.read(9, 1).is_err(), "must not read past the end");
    }

    #[test]
    fn read_upto_clamps_rather_than_failing() {
        let path = fixture("upto.bin", &[10, 11, 12, 13]);
        let s = MmapSource::open(&path).unwrap();

        // The variable-length case: ask for more than is there and get what is.
        assert_eq!(s.read_upto(2, 100).unwrap(), [12u8, 13].as_slice());
        assert_eq!(s.read_upto(0, 2).unwrap(), [10u8, 11].as_slice());
        // Exactly at the end is an empty read, not an error — a row store's last
        // row ends there and a reader must be able to ask.
        assert!(s.read_upto(4, 10).unwrap().is_empty());
        assert!(
            s.read_upto(5, 10).is_err(),
            "starting past the end is an error"
        );
    }

    #[test]
    fn arithmetic_that_would_overflow_is_an_error_not_a_wrap() {
        let path = fixture("overflow.bin", &[0; 16]);
        let s = MmapSource::open(&path).unwrap();
        assert!(s.read(u64::MAX, 8).is_err());
        assert!(s.read(8, usize::MAX).is_err());
        // `read_upto` saturates by design — an enormous `max` is a speculative
        // over-read asking for "the rest", which is legitimate.
        assert_eq!(s.read_upto(8, usize::MAX).unwrap().len(), 8);
    }
}
