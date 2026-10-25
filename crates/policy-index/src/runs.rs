//! `runs.pidx` — the policy table ON ITS OWN: prefix → the chunk holding the
//! earliest mint.
//!
//! # Why it exists
//!
//! MEASURED on cardano-infra 2026-10-24, from `base.pidx`'s own header: the file
//! is 700,596,312 bytes for 19,072,823 records and 232,745 policies.
//!
//! | section | bytes | share |
//! |---|---|---|
//! | directory (2^21 + 1 × u32) | 8,388,672 | 1.2% |
//! | records (× 32) | 610,330,336 | 87.1% |
//! | time permutation (× 4) | 76,291,292 | 10.9% |
//! | **policy runs (× 24)** | **5,585,880** | **0.8%** |
//!
//! The floor probe — `token-ledger`'s admission, on every policy it has never
//! seen — reads **only the last row**. The other 99.2% is the record array and
//! its permutation, which only the catalogue builder, the profile reader and
//! `verify` touch.
//!
//! # Why a sidecar and not a smaller base
//!
//! The base is recompacted whole whenever a new chunk lands: 6.5 s, ~1.3 GB of
//! I/O, to add about a thousand records a day. That is tolerable for a daily
//! batch and wrong for anything more frequent — a tail pass that wanted to add
//! an hour's mints would rewrite 668 MiB to do it.
//!
//! A run here is **insert-only**, because a policy's first chunk never changes
//! once it has minted. So this file can be maintained by whoever learns a new
//! policy first, at 16 bytes per policy.
//!
//! ⚠️ **It carries no `start`/`len`.** Those two fields in [`PolicyRun`] index
//! the BASE's record array, and a sidecar entry written by a tail pass has no
//! position in a base that has not been rebuilt. Carrying them would invite a
//! reader to slice an array it does not belong to. The floor probe needs the
//! chunk, so the chunk is all this file states.
//!
//! # Precedence
//!
//! [`Floors`] prefers this file and falls back to `base.pidx`. A sidecar can be
//! AHEAD of the base (a tail pass appended to it since the last compaction) and
//! must never be BEHIND it, because `compact` writes both in the same pass.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::base::{Base, base_path};
use crate::format::PolicyRun;

pub const RUNS_FILE: &str = "runs.pidx";
pub const RUNS_MAGIC: [u8; 4] = *b"PXR1";
pub const RUNS_HEADER_BYTES: usize = 64;

/// Bumped when the stride or a field meaning changes. An older file is REFUSED
/// rather than reinterpreted — the same rule `base.pidx` keeps.
pub const RUNS_VERSION: u8 = 1;

/// 8 bytes of policy prefix, 2 of chunk, 6 reserved. The reserves are where a
/// `shard` id would land if the record array is ever sharded by chunk range.
pub const FLOOR_BYTES: usize = 16;

pub fn runs_path(index_dir: &Path) -> PathBuf {
    crate::segment::policy_dir(index_dir).join(RUNS_FILE)
}

/// One policy's floor: the chunk its earliest mint or burn landed in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FloorRun {
    pub policy_prefix: u64,
    pub first_chunk: u16,
}

impl FloorRun {
    fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), FLOOR_BYTES);
        out.fill(0);
        out[0..8].copy_from_slice(&self.policy_prefix.to_le_bytes());
        out[8..10].copy_from_slice(&self.first_chunk.to_le_bytes());
    }

    fn read(b: &[u8]) -> FloorRun {
        debug_assert_eq!(b.len(), FLOOR_BYTES);
        FloorRun {
            policy_prefix: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            first_chunk: u16::from_le_bytes(b[8..10].try_into().expect("2 bytes")),
        }
    }
}

/// The sidecar, memory-mapped. 3.7 MiB at 232,745 policies.
#[derive(Debug)]
pub struct Runs {
    mmap: Mmap,
    count: u64,
    first_chunk: u16,
    last_chunk: u16,
}

impl Runs {
    pub fn open(path: &Path) -> Result<Runs> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: the file is built under `runs.pidx.tmp` and renamed in; nothing
        // writes to a published sidecar in place.
        let mmap =
            unsafe { Mmap::map(&f) }.with_context(|| format!("mapping {}", path.display()))?;
        if mmap.len() < RUNS_HEADER_BYTES {
            bail!("{}: truncated header", path.display());
        }
        if mmap[0..4] != RUNS_MAGIC {
            bail!(
                "{}: not a policy-index runs file (bad magic)",
                path.display()
            );
        }
        if mmap[4] != RUNS_VERSION {
            bail!(
                "{}: runs format version {} (this build reads {RUNS_VERSION})",
                path.display(),
                mmap[4]
            );
        }
        let first_chunk = u16::from_le_bytes(mmap[6..8].try_into().expect("2 bytes"));
        let last_chunk = u16::from_le_bytes(mmap[8..10].try_into().expect("2 bytes"));
        let count = u64::from_le_bytes(mmap[16..24].try_into().expect("8 bytes"));
        let want = RUNS_HEADER_BYTES as u64 + count * FLOOR_BYTES as u64;
        if mmap.len() as u64 != want {
            bail!(
                "{}: length {} != {} implied by header",
                path.display(),
                mmap.len(),
                want
            );
        }
        Ok(Runs {
            mmap,
            count,
            first_chunk,
            last_chunk,
        })
    }

    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The chunks this file makes claims about.
    pub fn covers(&self) -> (u16, u16) {
        (self.first_chunk, self.last_chunk)
    }

    fn at(&self, i: usize) -> FloorRun {
        let at = RUNS_HEADER_BYTES + i * FLOOR_BYTES;
        FloorRun::read(&self.mmap[at..at + FLOOR_BYTES])
    }

    /// Every policy's floor, in prefix order.
    pub fn runs(&self) -> impl Iterator<Item = FloorRun> + '_ {
        (0..self.count as usize).map(|i| self.at(i))
    }

    /// Binary search over a table the page cache keeps resident — one probe, no
    /// chunk I/O, no record array.
    pub fn floor_of(&self, policy_prefix: u64) -> Option<u16> {
        let (mut lo, mut hi) = (0usize, self.count as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.at(mid).policy_prefix.cmp(&policy_prefix) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(self.at(mid).first_chunk),
            }
        }
        None
    }
}

/// Write `runs.pidx` from a table, sorted, under a temp name and renamed.
///
/// Returns the file's length. The caller is expected to have derived `runs`
/// from the same compaction that produced the base beside it.
pub fn write(
    path: &Path,
    first_chunk: u16,
    last_chunk: u16,
    runs: &mut Vec<FloorRun>,
) -> Result<u64> {
    // Sorted because the reader binary-searches; deduped because a policy's
    // floor is one row and two would make `len` disagree with the base.
    runs.sort_unstable_by_key(|r| r.policy_prefix);
    runs.dedup_by_key(|r| r.policy_prefix);

    let tmp = path.with_extension("pidx.tmp");
    let mut w =
        BufWriter::new(File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?);
    let mut header = [0u8; RUNS_HEADER_BYTES];
    header[0..4].copy_from_slice(&RUNS_MAGIC);
    header[4] = RUNS_VERSION;
    header[6..8].copy_from_slice(&first_chunk.to_le_bytes());
    header[8..10].copy_from_slice(&last_chunk.to_le_bytes());
    header[16..24].copy_from_slice(&(runs.len() as u64).to_le_bytes());
    w.write_all(&header)?;
    let mut buf = [0u8; FLOOR_BYTES];
    for r in runs.iter() {
        r.write(&mut buf);
        w.write_all(&buf)?;
    }
    w.flush()?;
    let file = w
        .into_inner()
        .map_err(|e| anyhow::anyhow!("flushing {}: {e}", tmp.display()))?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
    Ok(RUNS_HEADER_BYTES as u64 + runs.len() as u64 * FLOOR_BYTES as u64)
}

/// Whichever artifact answers the floor probe.
///
/// The sidecar wins when it is there: it is the newer of the two whenever
/// anything has appended to it, and it is the only one a frequent writer can
/// maintain cheaply. `base.pidx` is the fallback for an index built before the
/// sidecar existed — which is every index on the box until the next compaction
/// writes one.
pub enum Floors {
    Runs(Runs),
    Base(Base),
}

impl Floors {
    /// `Ok(None)` when the directory holds neither artifact.
    ///
    /// A sidecar that exists but cannot be read is an ERROR rather than a
    /// fallback: `compact` writes it in the same pass as the base, so an
    /// unreadable one means the build is broken and I want to hear about it.
    pub fn open(index_dir: &Path) -> Result<Option<Floors>> {
        let sidecar = runs_path(index_dir);
        if sidecar.exists() {
            return Ok(Some(Floors::Runs(Runs::open(&sidecar)?)));
        }
        let base = base_path(index_dir);
        if base.exists() {
            return Ok(Some(Floors::Base(Base::open(&base)?)));
        }
        Ok(None)
    }

    /// Which artifact answered — for the log line, so an operator can tell a
    /// fresh sidecar from a stale base.
    pub fn source(&self) -> &'static str {
        match self {
            Floors::Runs(_) => RUNS_FILE,
            Floors::Base(_) => "base.pidx",
        }
    }

    pub fn first_chunk_of(&self, policy_prefix: u64) -> Option<u16> {
        match self {
            Floors::Runs(r) => r.floor_of(policy_prefix),
            Floors::Base(b) => b.first_chunk_of(policy_prefix),
        }
    }

    pub fn covers(&self) -> (u16, u16) {
        match self {
            Floors::Runs(r) => r.covers(),
            Floors::Base(b) => b.covers(),
        }
    }

    /// How many policies this artifact holds a floor for.
    pub fn len(&self) -> u64 {
        match self {
            Floors::Runs(r) => r.len(),
            Floors::Base(b) => b.policies(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The base's policy table as sidecar rows, so `compact` writes both from one
/// walk of the sorted records.
pub fn from_policy_runs(runs: &[PolicyRun]) -> Vec<FloorRun> {
    let mut out: Vec<FloorRun> = runs
        .iter()
        .map(|r| FloorRun {
            policy_prefix: r.policy_prefix,
            first_chunk: r.first_chunk,
        })
        .collect();
    out.sort_unstable_by_key(|r| r.policy_prefix);
    out.dedup_by_key(|r| r.policy_prefix);
    out
}

/// The size of one sidecar row, in bytes — 8 of prefix and 2 of chunk, against
/// the base's 24-byte `PolicyRun`, which also indexes an array this file has no
/// access to.
#[cfg(test)]
mod tests {
    use super::*;

    fn rows() -> Vec<FloorRun> {
        vec![
            FloorRun {
                policy_prefix: 30,
                first_chunk: 12,
            },
            FloorRun {
                policy_prefix: 10,
                first_chunk: 0,
            },
            FloorRun {
                policy_prefix: 20,
                first_chunk: 7,
            },
        ]
    }

    #[test]
    fn a_sidecar_round_trips_and_sorts_on_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut r = rows();
        let len = write(&path, 0, 12, &mut r).unwrap();
        assert_eq!(len, RUNS_HEADER_BYTES as u64 + 3 * FLOOR_BYTES as u64);

        let runs = Runs::open(&path).unwrap();
        assert_eq!(runs.len(), 3);
        assert_eq!(runs.covers(), (0, 12));
        let got: Vec<u64> = runs.runs().map(|r| r.policy_prefix).collect();
        assert_eq!(
            got,
            vec![10, 20, 30],
            "sorted, whatever order it arrived in"
        );
    }

    #[test]
    fn a_floor_probe_finds_the_chunk_and_nothing_for_a_stranger() {
        let dir = tempfile::tempdir().unwrap();
        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut r = rows();
        write(&path, 0, 12, &mut r).unwrap();

        let runs = Runs::open(&path).unwrap();
        assert_eq!(runs.floor_of(10), Some(0));
        assert_eq!(runs.floor_of(20), Some(7));
        assert_eq!(runs.floor_of(30), Some(12));
        assert_eq!(runs.floor_of(15), None, "a prefix it never saw");
        assert_eq!(runs.floor_of(0), None);
        assert_eq!(runs.floor_of(u64::MAX), None);
    }

    #[test]
    fn a_duplicated_policy_keeps_one_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut r = vec![
            FloorRun {
                policy_prefix: 10,
                first_chunk: 4,
            },
            FloorRun {
                policy_prefix: 10,
                first_chunk: 9,
            },
        ];
        write(&path, 0, 9, &mut r).unwrap();
        let runs = Runs::open(&path).unwrap();
        assert_eq!(runs.len(), 1);
    }

    #[test]
    fn a_file_that_is_not_a_sidecar_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"not a runs file at all, honestly").unwrap();
        let err = Runs::open(&path).unwrap_err().to_string();
        assert!(
            err.contains("bad magic") || err.contains("truncated"),
            "{err}"
        );
    }

    /// A file whose header claims more rows than it holds must fail its length
    /// check, not read past the end.
    #[test]
    fn a_truncated_sidecar_fails_its_length_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut r = rows();
        write(&path, 0, 12, &mut r).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 4]).unwrap();
        let err = Runs::open(&path).unwrap_err().to_string();
        assert!(err.contains("length"), "{err}");
    }

    /// `Floors` prefers the sidecar, falls back to a base, and reports neither.
    #[test]
    fn floors_prefer_the_sidecar_and_fall_back_to_the_base() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Floors::open(dir.path()).unwrap().is_none());

        let path = runs_path(dir.path());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut r = rows();
        write(&path, 0, 12, &mut r).unwrap();

        let floors = Floors::open(dir.path()).unwrap().expect("a sidecar");
        assert_eq!(floors.source(), RUNS_FILE);
        assert_eq!(floors.first_chunk_of(20), Some(7));
        assert_eq!(floors.len(), 3);
        assert_eq!(floors.covers(), (0, 12));
    }
}
