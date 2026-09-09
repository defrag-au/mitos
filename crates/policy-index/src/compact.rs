//! Segments → base: a counting sort into a memory-mapped file, then two
//! derived sections built by walking the sorted result.
//!
//! Mirrors `tx-index`'s compaction — histogram, place, sort in place — and
//! adds the two things a policy index needs that a hash index does not.
//!
//! ```text
//! 0. scan segments: bucket histogram, distinct policy prefixes, chunk range
//! 1. fences from the histogram
//! 2. place every record at its bucket cursor in the mapped output
//! 3. sort each bucket in place by (policy, asset, chunk)
//! 4. walk the sorted records once, emitting:
//!      - the POLICY table (one run per policy, carrying its first chunk)
//!      - the TIME permutation (u32 indices, (chunk, asset) within each run)
//! ```
//!
//! ⚠️ Step 0 collects distinct policy prefixes into a set — ~500k × 8 B ≈ 4 MB
//! — purely so the file can be laid out correctly the first time. The
//! alternative was allocating for the worst case (every record its own
//! policy, 360 MB of slack) and truncating afterwards. 4 MB of exactness beats
//! 360 MB of guesswork.
//!
//! The output is built under `base.pidx.tmp` and renamed, so a reader never
//! sees a partial base and an existing mapping stays valid until dropped.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use memmap2::MmapMut;

use crate::base::base_path;
use crate::format::{
    BASE_HEADER_BYTES, BaseHeader, DIR_BITS, PERM_BYTES, POLICY_BYTES, PolicyRun, RECORD_BYTES,
    Record, bucket_of,
};
use crate::segment::{Segment, list_segments, segment_path};

#[derive(Clone, Copy, Debug)]
pub struct CompactStats {
    pub segments: usize,
    pub first_chunk: u16,
    pub last_chunk: u16,
    pub records: u64,
    pub policies: u64,
    pub file_len: u64,
    pub wall_secs: f64,
}

/// Build `policy/base.pidx` from every policy segment on disk.
///
/// ⚠️ Segments must be CONTIGUOUS. A gap means a chunk was never extracted,
/// and a base built over it would silently miss that chunk's mints — which,
/// for a floor probe, means confidently reporting a first mint that is not the
/// first mint.
pub fn compact(index_dir: &Path) -> Result<CompactStats> {
    let started = Instant::now();
    let chunks = list_segments(index_dir)?;
    let Some((&first, &last)) = chunks.first().zip(chunks.last()) else {
        bail!(
            "no policy segments under {} to compact",
            index_dir.display()
        );
    };
    for w in chunks.windows(2) {
        if w[1] != w[0] + 1 {
            bail!(
                "policy segments are not contiguous: chunk {} is missing",
                w[0] + 1
            );
        }
    }

    let segs: Vec<Segment> = chunks
        .iter()
        .map(|c| Segment::open(&segment_path(index_dir, *c)))
        .collect::<Result<_>>()?;

    // ── 0. histogram, distinct policies, total ────────────────────────────
    let n_buckets = 1usize << DIR_BITS;
    let mut counts = vec![0u32; n_buckets];
    let mut seen: HashSet<u64> = HashSet::new();
    let mut total = 0u64;
    for seg in &segs {
        for r in seg.records() {
            counts[bucket_of(r.policy_prefix, DIR_BITS)] += 1;
            seen.insert(r.policy_prefix);
            total += 1;
        }
    }
    let policies = seen.len() as u64;
    drop(seen);

    let header = BaseHeader::layout(DIR_BITS, first, last, total, policies);
    let path = base_path(index_dir);
    let tmp = path.with_extension("pidx.tmp");
    std::fs::create_dir_all(crate::segment::policy_dir(index_dir))?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("creating {}", tmp.display()))?;
    file.set_len(header.file_len())?;
    // SAFETY: this file is private to this call until the rename below.
    let mut map = unsafe { MmapMut::map_mut(&file) }
        .with_context(|| format!("mapping {}", tmp.display()))?;

    // ── 1. fences (exclusive prefix sum, plus the terminator) ─────────────
    let mut cursors = vec![0u32; n_buckets + 1];
    let mut running = 0u32;
    for b in 0..n_buckets {
        cursors[b] = running;
        running = running
            .checked_add(counts[b])
            .context("bucket cursor overflowed u32 — more than 4G records")?;
    }
    cursors[n_buckets] = running;
    let dir_at = header.dir_off as usize;
    for (b, c) in cursors.iter().enumerate() {
        let at = dir_at + b * 4;
        map[at..at + 4].copy_from_slice(&c.to_le_bytes());
    }

    // ── 2. place ──────────────────────────────────────────────────────────
    let mut place = cursors.clone();
    let rec_at = header.records_off as usize;
    for seg in &segs {
        for r in seg.records() {
            let b = bucket_of(r.policy_prefix, DIR_BITS);
            let slot = place[b] as usize;
            place[b] += 1;
            let at = rec_at + slot * RECORD_BYTES;
            r.write(&mut map[at..at + RECORD_BYTES]);
        }
    }
    drop(segs);

    // ── 3. sort each bucket in place ──────────────────────────────────────
    // ~7 records per bucket, so most of these are a single compare.
    for b in 0..n_buckets {
        let (lo, hi) = (cursors[b] as usize, cursors[b + 1] as usize);
        if hi - lo < 2 {
            continue;
        }
        let mut bucket: Vec<Record> = (lo..hi)
            .map(|i| {
                let at = rec_at + i * RECORD_BYTES;
                Record::read(&map[at..at + RECORD_BYTES])
            })
            .collect();
        bucket.sort_unstable_by_key(|r| r.asset_key());
        for (k, r) in bucket.iter().enumerate() {
            let at = rec_at + (lo + k) * RECORD_BYTES;
            r.write(&mut map[at..at + RECORD_BYTES]);
        }
    }

    // ── 4. policy runs + time permutation ─────────────────────────────────
    //
    // ⚠️ A run is a stretch of equal POLICY PREFIX, not of equal policy id.
    // Two policies sharing 8 leading bytes would merge into one run. At ~500k
    // policies that is a ~7e-9 event, and it DEGRADES SAFELY: the run's
    // `first_chunk` becomes the earlier of the two (a floor that is too low,
    // which costs reading and never truncates), and the extra assets are
    // filtered by the reader's confirmation against the body. It is not
    // silent corruption, but it is worth knowing it is possible.
    let mut runs: Vec<PolicyRun> = Vec::with_capacity(policies as usize);
    let mut perm: Vec<u32> = Vec::with_capacity(total as usize);
    let read_rec = |map: &MmapMut, i: usize| -> Record {
        let at = rec_at + i * RECORD_BYTES;
        Record::read(&map[at..at + RECORD_BYTES])
    };

    let mut i = 0usize;
    while i < total as usize {
        let prefix = read_rec(&map, i).policy_prefix;
        let start = i;
        let mut first_chunk = u16::MAX;
        let mut in_run: Vec<(u16, u64, u32)> = Vec::new();
        while i < total as usize {
            let r = read_rec(&map, i);
            if r.policy_prefix != prefix {
                break;
            }
            first_chunk = first_chunk.min(r.chunk);
            in_run.push((r.chunk, r.name_prefix, i as u32));
            i += 1;
        }
        // The TIME ordering, within this policy only.
        in_run.sort_unstable_by_key(|(c, n, _)| (*c, *n));
        perm.extend(in_run.into_iter().map(|(_, _, idx)| idx));
        runs.push(PolicyRun {
            policy_prefix: prefix,
            start: start as u32,
            len: (i - start) as u32,
            first_chunk,
        });
    }
    if runs.len() as u64 != policies {
        bail!(
            "counted {policies} distinct policies but built {} runs",
            runs.len()
        );
    }

    let perm_at = header.perm_off as usize;
    for (k, idx) in perm.iter().enumerate() {
        let at = perm_at + k * PERM_BYTES;
        map[at..at + PERM_BYTES].copy_from_slice(&idx.to_le_bytes());
    }
    let pol_at = header.policies_off as usize;
    for (k, run) in runs.iter().enumerate() {
        let at = pol_at + k * POLICY_BYTES;
        run.write(&mut map[at..at + POLICY_BYTES]);
    }

    // Header LAST, so a torn write leaves a file that fails its magic check
    // rather than one that parses with wrong offsets.
    header.write(&mut map[..BASE_HEADER_BYTES]);
    map.flush()?;
    drop(map);
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;

    Ok(CompactStats {
        segments: chunks.len(),
        first_chunk: first,
        last_chunk: last,
        records: total,
        policies,
        file_len: header.file_len(),
        wall_secs: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::Base;
    use crate::segment::write_segment;

    fn rec(p: u64, n: u64, c: u16) -> Record {
        Record {
            policy_prefix: p,
            name_prefix: n,
            chunk: c,
            offset: 100,
            len: 20,
            aux_offset: 200,
            aux_len: 30,
            burned: false,
        }
    }

    fn build(chunks: &[(u16, Vec<Record>)]) -> (tempfile::TempDir, CompactStats) {
        let dir = tempfile::tempdir().unwrap();
        for (c, mut rs) in chunks.iter().cloned() {
            rs.sort_unstable_by_key(|r| r.asset_key());
            write_segment(dir.path(), c, &rs).unwrap();
        }
        let stats = compact(dir.path()).unwrap();
        (dir, stats)
    }

    #[test]
    fn a_base_round_trips_every_record() {
        let (dir, stats) = build(&[
            (0, vec![rec(7, 2, 0), rec(3, 9, 0)]),
            (1, vec![rec(7, 1, 1)]),
        ]);
        assert_eq!(stats.records, 3);
        assert_eq!(stats.policies, 2);

        let base = Base::open(&base_path(dir.path())).unwrap();
        assert_eq!(base.len(), 3);
        assert_eq!(base.policies(), 2);
    }

    /// The whole point of the side-table: a policy's floor without touching a
    /// chunk. Records arrive out of order across segments, so this also pins
    /// that the run's `first_chunk` is a MINIMUM, not the first one seen.
    #[test]
    fn a_policys_first_chunk_is_the_minimum_across_segments() {
        let (dir, _) = build(&[
            (0, vec![rec(7, 1, 0)]),
            (1, vec![rec(7, 2, 1)]),
            (2, vec![rec(7, 3, 2)]),
        ]);
        let base = Base::open(&base_path(dir.path())).unwrap();
        assert_eq!(base.first_chunk_of(7), Some(0));
    }

    #[test]
    fn a_policy_the_index_never_saw_has_no_floor() {
        let (dir, _) = build(&[(0, vec![rec(7, 1, 0)])]);
        let base = Base::open(&base_path(dir.path())).unwrap();
        assert_eq!(base.first_chunk_of(999), None);
    }

    /// Records group by policy and sort by asset within it — the ordering the
    /// asset lookup binary-searches.
    #[test]
    fn records_come_back_grouped_by_policy_and_sorted_by_asset() {
        let (dir, _) = build(&[(
            0,
            vec![rec(2, 5, 0), rec(1, 9, 0), rec(1, 3, 0), rec(2, 1, 0)],
        )]);
        let base = Base::open(&base_path(dir.path())).unwrap();
        let one: Vec<u64> = base.records_of(1).iter().map(|r| r.name_prefix).collect();
        let two: Vec<u64> = base.records_of(2).iter().map(|r| r.name_prefix).collect();
        assert_eq!(one, vec![3, 9]);
        assert_eq!(two, vec![1, 5]);
    }

    /// The second ordering, and the reason it is a permutation: the same
    /// records, in mint-time order, within one policy.
    #[test]
    fn the_time_view_orders_one_policys_records_by_chunk() {
        let (dir, _) = build(&[
            (0, vec![rec(1, 500, 0)]),
            (1, vec![rec(1, 100, 1)]),
            (2, vec![rec(1, 300, 2)]),
        ]);
        let base = Base::open(&base_path(dir.path())).unwrap();
        // Asset order is by name: 100, 300, 500.
        assert_eq!(
            base.records_of(1)
                .iter()
                .map(|r| r.name_prefix)
                .collect::<Vec<_>>(),
            vec![100, 300, 500]
        );
        // Time order is by chunk: 500 (chunk 0), 100 (chunk 1), 300 (chunk 2).
        assert_eq!(
            base.records_by_time(1)
                .iter()
                .map(|r| r.chunk)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            base.records_by_time(1)
                .iter()
                .map(|r| r.name_prefix)
                .collect::<Vec<_>>(),
            vec![500, 100, 300]
        );
    }

    /// ⚠️ A missing chunk means missing mints, and for a floor probe that is a
    /// confidently wrong answer rather than a missing one. It must refuse.
    #[test]
    fn a_gap_in_the_segments_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        write_segment(dir.path(), 0, &[rec(1, 1, 0)]).unwrap();
        write_segment(dir.path(), 2, &[rec(1, 2, 2)]).unwrap();
        let err = compact(dir.path()).unwrap_err().to_string();
        assert!(err.contains("contiguous"), "unhelpful error: {err}");
    }

    #[test]
    fn compacting_nothing_is_an_error_not_an_empty_base() {
        let dir = tempfile::tempdir().unwrap();
        assert!(compact(dir.path()).is_err());
    }

    #[test]
    fn compaction_leaves_no_temp_file_behind() {
        let (dir, _) = build(&[(0, vec![rec(1, 1, 0)])]);
        let leftovers: Vec<String> = std::fs::read_dir(crate::segment::policy_dir(dir.path()))
            .unwrap()
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "left {leftovers:?}");
    }

    /// Re-compacting over an existing base replaces it wholesale.
    #[test]
    fn compaction_is_idempotent() {
        let (dir, first) = build(&[(0, vec![rec(1, 1, 0), rec(2, 2, 0)])]);
        let again = compact(dir.path()).unwrap();
        assert_eq!(first.records, again.records);
        assert_eq!(first.policies, again.policies);
        assert_eq!(first.file_len, again.file_len);
    }
}
