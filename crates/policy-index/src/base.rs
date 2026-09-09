//! The compacted base — a prefix directory over one sorted record array, plus
//! the time permutation and the policy side-table.
//!
//! Read-only, memory-mapped, no allocation on the lookup path beyond the
//! `Vec` a run is collected into. The page cache keeps the directory (8 MiB)
//! and the policy table (~11 MiB) resident, which is what makes a floor probe
//! **one binary search and zero chunk I/O**.
//!
//! # ⚠️ A prefix is not an identity
//!
//! Records are keyed by the first 8 bytes of the policy id and of
//! `blake2b(asset_name)`. Over ~15M records that is not a collision-free
//! space, so **every answer this module gives is a CANDIDATE**. The naming
//! reflects it: [`Base::records_of`] takes a `policy_prefix`, not a policy.
//!
//! A caller holding the real policy id and asset name must confirm against the
//! transaction the record points at — the same contract `tx-index` has, where
//! a located body is re-hashed against the full requested hash. This module
//! cannot do that confirmation itself: it has no access to the chunks, by
//! design.
//!
//! What a collision actually costs, so the risk is sized rather than feared:
//!
//! | collision | effect |
//! |---|---|
//! | two policies share a prefix | their runs merge; `first_chunk` becomes the earlier — **a floor too LOW**, which costs reading and never truncates |
//! | two assets share a name prefix | an extra candidate, filtered by confirmation |
//!
//! Both degrade toward "more work", never toward "wrong and silent".

use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use memmap2::Mmap;

use crate::format::{
    BaseHeader, PERM_BYTES, POLICY_BYTES, PolicyRun, RECORD_BYTES, Record, bucket_of,
};
use crate::segment::policy_dir;

pub const BASE_FILE: &str = "base.pidx";

pub fn base_path(index_dir: &Path) -> PathBuf {
    policy_dir(index_dir).join(BASE_FILE)
}

pub struct Base {
    mmap: Mmap,
    pub header: BaseHeader,
}

impl Base {
    pub fn open(path: &Path) -> Result<Base> {
        let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
        // SAFETY: a base is built whole under a temp name and renamed in;
        // nothing writes to a published base in place.
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
        Ok(Base { mmap, header })
    }

    pub fn len(&self) -> u64 {
        self.header.count
    }

    pub fn is_empty(&self) -> bool {
        self.header.count == 0
    }

    pub fn policies(&self) -> u64 {
        self.header.policies
    }

    pub fn covers(&self) -> (u16, u16) {
        (self.header.first_chunk, self.header.last_chunk)
    }

    /// A record by its position in the array — for `verify`, which walks by
    /// stride rather than by policy.
    pub fn record_at_index(&self, i: usize) -> Record {
        self.record_at(i)
    }

    fn record_at(&self, i: usize) -> Record {
        let at = self.header.records_off as usize + i * RECORD_BYTES;
        Record::read(&self.mmap[at..at + RECORD_BYTES])
    }

    fn perm_at(&self, i: usize) -> u32 {
        let at = self.header.perm_off as usize + i * PERM_BYTES;
        u32::from_le_bytes(self.mmap[at..at + PERM_BYTES].try_into().expect("4 bytes"))
    }

    fn run_at(&self, i: usize) -> PolicyRun {
        let at = self.header.policies_off as usize + i * POLICY_BYTES;
        PolicyRun::read(&self.mmap[at..at + POLICY_BYTES])
    }

    /// The policy table entry for a prefix, by binary search over a table the
    /// page cache keeps resident.
    pub fn run_of(&self, policy_prefix: u64) -> Option<PolicyRun> {
        let n = self.header.policies as usize;
        let (mut lo, mut hi) = (0usize, n);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let p = self.run_at(mid).policy_prefix;
            match p.cmp(&policy_prefix) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(self.run_at(mid)),
            }
        }
        None
    }

    /// **The floor probe's answer**: the immutable chunk holding this policy's
    /// earliest mint, without reading a single chunk.
    ///
    /// Chunk rather than slot is not a limitation here — it is the precision
    /// the consumer already wants. `token-ledger`'s `--probe-first-mint`
    /// deliberately subtracts one chunk of margin, because a floor that is too
    /// HIGH truncates an archive while letting it call itself complete. A
    /// chunk-grained answer cannot err in that direction.
    pub fn first_chunk_of(&self, policy_prefix: u64) -> Option<u16> {
        self.run_of(policy_prefix).map(|r| r.first_chunk)
    }

    /// Every mint/burn record under a policy prefix, in ASSET order.
    pub fn records_of(&self, policy_prefix: u64) -> Vec<Record> {
        let Some(run) = self.run_of(policy_prefix) else {
            return Vec::new();
        };
        (run.start as usize..(run.start + run.len) as usize)
            .map(|i| self.record_at(i))
            .collect()
    }

    /// The same records in MINT-TIME order — the second ordering, via the
    /// permutation rather than a second copy.
    pub fn records_by_time(&self, policy_prefix: u64) -> Vec<Record> {
        let Some(run) = self.run_of(policy_prefix) else {
            return Vec::new();
        };
        (run.start as usize..(run.start + run.len) as usize)
            .map(|i| self.record_at(self.perm_at(i) as usize))
            .collect()
    }

    /// Candidate records for one asset. Binary search within the policy's run,
    /// then widen over equal name prefixes.
    ///
    /// ⚠️ CANDIDATES. Confirm against the transaction before believing one —
    /// see this module's header.
    pub fn candidates(&self, policy_prefix: u64, name_prefix: u64) -> Vec<Record> {
        let Some(run) = self.run_of(policy_prefix) else {
            return Vec::new();
        };
        let (lo0, hi0) = (run.start as usize, (run.start + run.len) as usize);
        let (mut lo, mut hi) = (lo0, hi0);
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.record_at(mid).name_prefix.cmp(&name_prefix) {
                std::cmp::Ordering::Less => lo = mid + 1,
                _ => hi = mid,
            }
        }
        let mut out = Vec::new();
        let mut i = lo;
        while i < hi0 {
            let r = self.record_at(i);
            if r.name_prefix != name_prefix {
                break;
            }
            out.push(r);
            i += 1;
        }
        out
    }

    /// The record that CREATED an asset: its earliest non-burn candidate.
    ///
    /// ⚠️ Earliest MINT, not earliest record. An asset can be minted, burned
    /// and minted again; the burn sits in the same run and is not an origin.
    pub fn origin(&self, policy_prefix: u64, name_prefix: u64) -> Option<Record> {
        self.candidates(policy_prefix, name_prefix)
            .into_iter()
            .find(|r| !r.burned)
    }

    /// Every record in the base, in stored order — for `verify`.
    pub fn records(&self) -> impl Iterator<Item = Record> + '_ {
        (0..self.header.count as usize).map(|i| self.record_at(i))
    }

    /// Bucket bounds from the fence array, for structural checks.
    pub fn bucket_bounds(&self, bucket: usize) -> (u32, u32) {
        let at = self.header.dir_off as usize + bucket * 4;
        let lo = u32::from_le_bytes(self.mmap[at..at + 4].try_into().expect("4 bytes"));
        let hi = u32::from_le_bytes(self.mmap[at + 4..at + 8].try_into().expect("4 bytes"));
        (lo, hi)
    }

    /// Structural self-check: the ordering, the fences and the permutation all
    /// have to agree with each other, and every one of them is derived rather
    /// than stated — so a disagreement is a real defect and not a stale field.
    ///
    /// This is the half of `verify` that needs no chunks. Re-deriving records
    /// from the immutable DB is the other half and belongs to whoever holds
    /// the chunks.
    pub fn verify_structure(&self) -> Result<()> {
        let n = self.header.count as usize;
        let mut prev: Option<(u64, u64, u16)> = None;
        for i in 0..n {
            let r = self.record_at(i);
            if let Some(p) = prev
                && p > r.asset_key()
            {
                bail!("records out of order at {i}");
            }
            prev = Some(r.asset_key());
            let b = bucket_of(r.policy_prefix, self.header.dir_bits);
            let (lo, hi) = self.bucket_bounds(b);
            if (i as u32) < lo || (i as u32) >= hi {
                bail!("record {i} sits outside the fence of its bucket {b}");
            }
        }
        // The permutation must be a permutation: every index exactly once,
        // and confined to its own policy's run.
        let mut seen = vec![false; n];
        for k in 0..self.header.policies as usize {
            let run = self.run_at(k);
            let (lo, hi) = (run.start as usize, (run.start + run.len) as usize);
            if hi > n {
                bail!("policy run {k} runs past the record array");
            }
            let mut min_chunk = u16::MAX;
            for i in lo..hi {
                let target = self.perm_at(i) as usize;
                if target < lo || target >= hi {
                    bail!("permutation at {i} escapes its policy run");
                }
                if std::mem::replace(&mut seen[target], true) {
                    bail!("permutation visits record {target} twice");
                }
                min_chunk = min_chunk.min(self.record_at(i).chunk);
            }
            if run.len > 0 && run.first_chunk != min_chunk {
                bail!(
                    "policy run {k} says first_chunk {} but its records start at {min_chunk}",
                    run.first_chunk
                );
            }
        }
        if seen.iter().any(|s| !s) {
            bail!("the permutation does not cover every record");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compact::compact;
    use crate::segment::write_segment;

    fn rec(p: u64, n: u64, c: u16, burned: bool) -> Record {
        Record {
            policy_prefix: p,
            name_prefix: n,
            chunk: c,
            offset: 1,
            len: 2,
            aux_offset: 3,
            aux_len: 4,
            burned,
        }
    }

    fn built(records: Vec<Record>) -> (tempfile::TempDir, Base) {
        let dir = tempfile::tempdir().unwrap();
        let mut rs = records;
        rs.sort_unstable_by_key(|r| r.asset_key());
        write_segment(dir.path(), 0, &rs).unwrap();
        compact(dir.path()).unwrap();
        let base = Base::open(&base_path(dir.path())).unwrap();
        (dir, base)
    }

    #[test]
    fn a_built_base_passes_its_own_structural_check() {
        let (_d, base) = built(vec![
            rec(5, 1, 0, false),
            rec(1, 9, 0, false),
            rec(5, 2, 0, false),
            rec(1, 3, 0, false),
        ]);
        base.verify_structure().unwrap();
    }

    #[test]
    fn an_asset_lookup_returns_only_its_own_candidates() {
        let (_d, base) = built(vec![
            rec(1, 100, 0, false),
            rec(1, 200, 0, false),
            rec(1, 300, 0, false),
        ]);
        let got = base.candidates(1, 200);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name_prefix, 200);
        assert!(base.candidates(1, 999).is_empty());
    }

    /// ⚠️ An asset minted, burned and re-minted: the ORIGIN is the first
    /// non-burn, not the first record.
    #[test]
    fn the_origin_skips_a_burn() {
        let (_d, base) = built(vec![
            rec(1, 100, 5, true),
            rec(1, 100, 9, false),
            rec(1, 100, 2, false),
        ]);
        let origin = base.origin(1, 100).unwrap();
        assert_eq!(origin.chunk, 2);
        assert!(!origin.burned);
    }

    /// A name prefix collision yields several candidates rather than a guess —
    /// the caller confirms against the transaction.
    #[test]
    fn colliding_name_prefixes_come_back_as_several_candidates() {
        let (_d, base) = built(vec![
            rec(1, 100, 3, false),
            rec(1, 100, 7, false),
            rec(1, 555, 1, false),
        ]);
        assert_eq!(base.candidates(1, 100).len(), 2);
    }

    #[test]
    fn an_unknown_policy_answers_empty_rather_than_erroring() {
        let (_d, base) = built(vec![rec(1, 1, 0, false)]);
        assert!(base.records_of(42).is_empty());
        assert!(base.records_by_time(42).is_empty());
        assert!(base.candidates(42, 1).is_empty());
        assert_eq!(base.first_chunk_of(42), None);
    }

    /// ⚠️ The check has to be able to FAIL, or it proves nothing. Corrupt a
    /// stored `first_chunk` and the structural check must notice.
    #[test]
    fn the_structural_check_catches_a_corrupted_policy_table() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 0, &[rec(1, 1, 4, false)]).unwrap();
        compact(dir.path()).unwrap();
        let path = base_path(dir.path());

        let mut bytes = std::fs::read(&path).unwrap();
        let hdr = BaseHeader::read(&bytes).unwrap();
        // Overwrite the run's first_chunk with a value its records contradict.
        let at = hdr.policies_off as usize + 16;
        bytes[at..at + 2].copy_from_slice(&9999u16.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let base = Base::open(&path).unwrap();
        let err = base.verify_structure().unwrap_err().to_string();
        assert!(err.contains("first_chunk"), "unhelpful error: {err}");
    }

    #[test]
    fn a_base_whose_length_contradicts_its_header_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 0, &[rec(1, 1, 0, false)]).unwrap();
        compact(dir.path()).unwrap();
        let path = base_path(dir.path());
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 8]).unwrap();
        assert!(Base::open(&path).is_err());
    }
}
