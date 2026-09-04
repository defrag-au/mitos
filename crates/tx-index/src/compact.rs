//! Segments → base: a counting sort into a memory-mapped file.
//!
//! Three passes, none of which holds the entry set in memory:
//!
//! 1. histogram every segment's prefixes into 2^24 bucket counts → fences;
//! 2. place every entry at its bucket cursor in the mapped output;
//! 3. sort each bucket in place (~7 entries; most buckets are one compare).
//!
//! The output is built under `base.idx.tmp` and renamed over `base.idx`, so
//! a reader is never exposed to a partial base and a mapping of the old one
//! stays valid until that reader drops it.

use std::fs::OpenOptions;
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use memmap2::MmapMut;

use crate::base::base_path;
use crate::format::{BaseHeader, DIR_BITS, ENTRY_BYTES, Entry, bucket_of};
use crate::segment::{SegmentFile, list_segments, segment_path};

#[derive(Clone, Copy, Debug)]
pub struct CompactStats {
    pub segments: usize,
    pub first_chunk: u16,
    pub last_chunk: u16,
    pub entries: u64,
    pub file_len: u64,
    pub wall_secs: f64,
}

/// Build `base.idx` from every segment on disk. Segments must be contiguous;
/// a gap means a chunk was never extracted and the base would silently miss
/// its transactions.
pub fn compact(index_dir: &Path) -> Result<CompactStats> {
    let started = Instant::now();
    let chunks = list_segments(index_dir)?;
    let Some((&first, &last)) = chunks.first().zip(chunks.last()) else {
        bail!("no segments under {} to compact", index_dir.display());
    };
    for w in chunks.windows(2) {
        if w[1] != w[0] + 1 {
            bail!("segments are not contiguous: chunk {} is missing", w[0] + 1);
        }
    }

    let segs = chunks
        .iter()
        .map(|c| SegmentFile::open(&segment_path(index_dir, *c)))
        .collect::<Result<Vec<_>>>()?;

    // Pass 1 — histogram.
    let n_buckets = 1usize << DIR_BITS;
    let mut counts = vec![0u32; n_buckets];
    let mut total = 0u64;
    for s in &segs {
        for i in 0..s.len() {
            counts[bucket_of(s.prefix(i), DIR_BITS)] += 1;
        }
        total += s.len() as u64;
    }
    if total > u64::from(u32::MAX) {
        bail!("{total} entries exceed the u32 directory fence");
    }
    let mut fences = vec![0u32; n_buckets + 1];
    for b in 0..n_buckets {
        fences[b + 1] = fences[b] + counts[b];
    }
    drop(counts);

    // Layout + file.
    let header = BaseHeader::layout(DIR_BITS, first, last, total);
    let final_path = base_path(index_dir);
    let tmp = final_path.with_extension("idx.tmp");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("creating {}", tmp.display()))?;
    file.set_len(header.file_len())?;
    // SAFETY: private temp file, exclusively ours until the rename.
    let mut map = unsafe { MmapMut::map_mut(&file) }.context("mapping base.idx.tmp")?;

    header.write(&mut map[..crate::format::BASE_HEADER_BYTES]);
    let era_off = header.era_off as usize;
    for (i, s) in segs.iter().enumerate() {
        map[era_off + i] = s.header.era;
    }
    let dir_off = header.dir_off as usize;
    for (b, f) in fences.iter().enumerate() {
        map[dir_off + b * 4..dir_off + b * 4 + 4].copy_from_slice(&f.to_le_bytes());
    }

    // Pass 2 — place.
    let entries_off = header.entries_off as usize;
    let mut cursor: Vec<u32> = fences[..n_buckets].to_vec();
    for s in &segs {
        for e in s.entries() {
            let b = bucket_of(e.prefix, DIR_BITS);
            let at = entries_off + cursor[b] as usize * ENTRY_BYTES;
            e.write(&mut map[at..at + ENTRY_BYTES]);
            cursor[b] += 1;
        }
    }
    drop(cursor);

    // Pass 3 — sort within buckets.
    let mut scratch: Vec<Entry> = Vec::new();
    for b in 0..n_buckets {
        let (lo, hi) = (fences[b] as usize, fences[b + 1] as usize);
        if hi - lo < 2 {
            continue;
        }
        scratch.clear();
        for i in lo..hi {
            let at = entries_off + i * ENTRY_BYTES;
            scratch.push(Entry::read(&map[at..at + ENTRY_BYTES]));
        }
        scratch.sort_unstable_by_key(|e| e.prefix);
        for (k, e) in scratch.iter().enumerate() {
            let at = entries_off + (lo + k) * ENTRY_BYTES;
            e.write(&mut map[at..at + ENTRY_BYTES]);
        }
    }

    map.flush().context("flushing base.idx.tmp")?;
    drop(map);
    file.sync_all()?;
    drop(file);
    std::fs::rename(&tmp, &final_path)
        .with_context(|| format!("renaming {} → {}", tmp.display(), final_path.display()))?;

    Ok(CompactStats {
        segments: segs.len(),
        first_chunk: first,
        last_chunk: last,
        entries: total,
        file_len: header.file_len(),
        wall_secs: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::BaseFile;
    use crate::format::{Location, SegmentHeader};
    use crate::segment::write_segment;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("tx-index-compact-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Deterministic pseudo-random 64-bit values — a splitmix64 stream.
    fn mix(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    #[test]
    fn compacted_base_finds_every_entry() {
        let dir = scratch("find");
        let mut all = Vec::new();
        for chunk in 0u16..4 {
            let mut entries: Vec<Entry> = (0..500u32)
                .map(|i| Entry {
                    prefix: mix(u64::from(chunk) * 1_000 + u64::from(i)),
                    loc: Location {
                        chunk,
                        offset: i * 7,
                        len: 50,
                    },
                })
                .collect();
            // A deliberate cross-chunk prefix collision.
            if chunk == 3 {
                entries[0].prefix = mix(0);
            }
            entries.sort_unstable_by_key(|e| e.prefix);
            write_segment(
                &dir,
                SegmentHeader {
                    era: 6 + u8::from(chunk == 3),
                    chunk,
                    count: entries.len() as u32,
                },
                &entries,
            )
            .unwrap();
            all.extend(entries);
        }

        let stats = compact(&dir).unwrap();
        assert_eq!(stats.segments, 4);
        assert_eq!(stats.entries, 2_000);

        let base = BaseFile::open(&base_path(&dir)).unwrap();
        assert_eq!(base.header.first_chunk, 0);
        assert_eq!(base.header.last_chunk, 3);
        assert_eq!(base.era_of(0), Some(6));
        assert_eq!(base.era_of(3), Some(7));
        assert_eq!(base.era_of(4), None);

        for e in &all {
            let hits = base.find(e.prefix);
            assert!(hits.contains(&e.loc), "missing {e:?}");
        }
        // The collision returns both locations.
        assert_eq!(base.find(mix(0)).len(), 2);
        // A prefix nobody has.
        assert!(base.find(mix(u64::MAX)).is_empty());

        // Global order holds across the whole array.
        let mut last = 0u64;
        for i in 0..base.len() as usize {
            let p = base.entry(i).prefix;
            assert!(p >= last);
            last = p;
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn gap_is_refused() {
        let dir = scratch("gap");
        for chunk in [0u16, 2] {
            write_segment(
                &dir,
                SegmentHeader {
                    era: 1,
                    chunk,
                    count: 0,
                },
                &[],
            )
            .unwrap();
        }
        let err = compact(&dir).unwrap_err().to_string();
        assert!(err.contains("chunk 1 is missing"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
