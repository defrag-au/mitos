//! The locate arithmetic, with the reads taken out.
//!
//! `base.rs` answers *where in the mapped file*; this answers *which bytes of
//! the object must I fetch* — the same question one transport removed. A caller
//! holding an `mmap` and a caller holding a browser `fetch` compute identical
//! offsets, so the definition lives here once and both reach for it. Two
//! copies would be two chances to disagree, and the disagreement would be
//! silent: a wrong offset returns bytes, just not the ones asked for.
//!
//! Everything is `&[u8]` in and numbers out. No I/O, no `std::fs`, no runtime —
//! it builds for `wasm32-unknown-unknown` and carries no opinion about how the
//! bytes arrive.
//!
//! # Three reads
//!
//! | read | bytes | where |
//! |---|---|---|
//! | header | [`BASE_HEADER_BYTES`](crate::format::BASE_HEADER_BYTES) | offset 0 |
//! | fence pair | 8 | `dir_off + 4·bucket` |
//! | entry run | 24 × (hi − lo) | `entries_off + 24·lo` |
//!
//! Then one more per located body, at the entry's own `chunk` + `offset`.
//!
//! # …or two, against a shard
//!
//! The base addresses a lookup as `fence → run` in one object, which is right when
//! that object is a local file and wrong when it is a 2.83 GiB R2 object the CDN will
//! not cache. [`SHARD_BUCKETS`] splits the entry region into objects small enough to
//! *be* cached — see the shard section at the foot of this module for the arithmetic
//! and the measurements behind the size.
//!
//! # Nothing here is a guess
//!
//! The fence pair bounds the bucket **exactly**, so the run is fetched once at
//! its true length; and the entry carries its body's `len`, so the body read is
//! exact too. There is no speculative over-read and no "is this a short read or
//! a missing record" ambiguity to resolve, which is why there is no ladder,
//! budget or paging in this module. (The book's reader carries all three for
//! the opposite reason: an index entry there has no length, so a row read is a
//! window and a parse failure is two different things wearing one face.)

use anyhow::{Result, bail};

use crate::format::{BaseHeader, ENTRY_BYTES, Entry, bucket_of};

/// Bytes in one directory slot.
pub const FENCE_BYTES: u64 = 4;

/// The two slots a lookup reads: `bucket` and `bucket + 1`, which between them
/// bound that bucket's entries.
pub const FENCE_PAIR_BYTES: u64 = FENCE_BYTES * 2;

/// A byte range to fetch from the base object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub offset: u64,
    pub len: u64,
}

impl Span {
    /// The slice this span names in a buffer that holds the whole object.
    pub fn of<'a>(&self, whole: &'a [u8]) -> &'a [u8] {
        let start = self.offset as usize;
        &whole[start..start + self.len as usize]
    }
}

/// Where the fence pair bounding `prefix`'s bucket lives.
///
/// One read, not two: the pair is contiguous, and the pair *is* what bounds the
/// bucket — `fence(bucket)` and `fence(bucket + 1)` are two halves of it.
pub fn fence_span(header: &BaseHeader, prefix: u64) -> Span {
    let bucket = bucket_of(prefix, header.dir_bits) as u64;
    Span {
        offset: header.dir_off + bucket * FENCE_BYTES,
        len: FENCE_PAIR_BYTES,
    }
}

/// The entry index range from the 8 bytes [`fence_span`] asked for.
pub fn fence_pair(bytes: &[u8]) -> Result<(u32, u32)> {
    if bytes.len() < FENCE_PAIR_BYTES as usize {
        bail!(
            "fence pair truncated: {} bytes, wanted {FENCE_PAIR_BYTES}",
            bytes.len()
        );
    }
    let lo = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes"));
    let hi = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
    if hi < lo {
        bail!("fence pair is inverted: {lo}..{hi}");
    }
    Ok((lo, hi))
}

/// Where the entries for a bucket live. `len` is 0 for an empty bucket, which
/// is a legitimate answer and not an error — the bucket exists, nothing is in
/// it.
pub fn entries_span(header: &BaseHeader, lo: u32, hi: u32) -> Span {
    let n = u64::from(hi.saturating_sub(lo));
    Span {
        offset: header.entries_off + u64::from(lo) * ENTRY_BYTES as u64,
        len: n * ENTRY_BYTES as u64,
    }
}

/// Every entry in a fetched run whose prefix equals `prefix`.
///
/// `bytes` must be the run [`entries_span`] asked for, starting at entry `lo`
/// with `hi − lo` entries in it. A SHORT run is refused rather than scanned:
/// entries within a bucket are sorted, so the scan stops at the first larger
/// prefix, and a run that ends early would stop for the wrong reason. "Absent
/// from the index" and "absent from the bytes you fetched" are different
/// answers, and only one of them is safe to remember.
pub fn search(prefix: u64, lo: u32, hi: u32, bytes: &[u8]) -> Result<Vec<Entry>> {
    let want = entries_span_len(lo, hi);
    if (bytes.len() as u64) < want {
        bail!(
            "entry run truncated: {} bytes for {lo}..{hi}, wanted {want}",
            bytes.len()
        );
    }
    let (chunks, _) = bytes.as_chunks::<ENTRY_BYTES>();
    let mut out = Vec::new();
    for raw in chunks.iter().take(want as usize / ENTRY_BYTES) {
        let p = Entry::prefix_at(raw);
        if p < prefix {
            continue;
        }
        if p > prefix {
            break;
        }
        out.push(Entry::read(raw));
    }
    Ok(out)
}

fn entries_span_len(lo: u32, hi: u32) -> u64 {
    u64::from(hi.saturating_sub(lo)) * ENTRY_BYTES as u64
}

// ── entry shards: the same lookup, addressed into an object the CDN keeps ──────

/// Buckets per entry shard.
///
/// ⚠️ **512 is a hosting number, and what it fixes is the shard COUNT, not the shard
/// size.** A hosted lookup is one request per range, and a cold object costs the edge a
/// fixed latency that barely moves with size — measured 2026-09-27 against the published
/// mirror: a never-read 252-byte pointer **343 ms**, a never-read 4 MiB chunk **626–816
/// ms**. What IS proportional is bytes, and a cold reader's total pull is `shards_touched
/// × shard_size`: 2²⁴ / 512 = 32,768 shards of ~93 KiB, so a 300-transaction wallet pulls
/// **~27 MB** against a cold cache — and ~3 GB if the same bytes were a few dozen large
/// objects, because 300 scattered lookups touch ~300 distinct shards however the entries
/// are grouped.
///
/// ⚠️ **A shard carries its own fence table.** The pair that bounds a bucket is the same
/// pair the base's directory holds, so it could be read from there — but that object is
/// 64 MiB, and a cold range read of it makes the edge pull all 64 MiB before it can
/// answer. Self-contained shards mean a reader never touches, or warms, the directory.
pub const SHARD_BUCKETS: u64 = 512;

/// The fence table at the head of every shard: one u32 per boundary, so
/// [`SHARD_BUCKETS`] + 1 of them. The values are LOCAL to the shard — the shard's own
/// first fence value is subtracted out — so a reader addressing a bucket inside a shard
/// needs to know nothing about where that shard sits in the base.
pub const SHARD_FENCE_BYTES: u64 = (SHARD_BUCKETS + 1) * FENCE_BYTES;

/// Which shard holds `bucket`'s entries.
pub fn shard_of(bucket: u64) -> u64 {
    bucket / SHARD_BUCKETS
}

/// How many shards a bucket space of `n_buckets` divides into. The last one is short
/// when `n_buckets` is not a multiple of [`SHARD_BUCKETS`], which mainnet's 2²⁴ is and a
/// hypothetical narrower directory would not be.
pub fn shard_count(n_buckets: u64) -> u64 {
    n_buckets.div_ceil(SHARD_BUCKETS)
}

/// The first bucket a shard covers.
pub fn shard_first_bucket(shard: u64) -> u64 {
    shard * SHARD_BUCKETS
}

/// Where a bucket's fence pair lives **inside its own shard**.
///
/// The same pair [`fence_span`] names in the base's directory — one read, because the
/// pair is contiguous and the pair is what bounds the bucket — but at a shard-local
/// offset and, when it comes back, in shard-local values.
pub fn shard_fence_span(bucket: u64) -> Span {
    Span {
        offset: (bucket - shard_first_bucket(shard_of(bucket))) * FENCE_BYTES,
        len: FENCE_PAIR_BYTES,
    }
}

/// Where a shard's first entry index lives in the base's directory.
///
/// ⚠️ **Read from the directory, not from the shard's own fence table.** The table a shard
/// carries holds the same numbers rebased on zero, which is what lets a shard be
/// self-contained — and which means it cannot say where the shard starts in the base.
/// A reader that wants the pair from the directory (batched, several to a request) needs
/// this value to convert; a reader that wants it from the shard needs [`shard_fence_span`]
/// instead.
pub fn shard_base_span(header: &BaseHeader, shard: u64) -> Span {
    Span {
        offset: header.dir_off + shard * SHARD_BUCKETS * FENCE_BYTES,
        len: FENCE_BYTES,
    }
}

/// The entry index a shard starts at, from the 4 bytes [`shard_base_span`] asked for.
pub fn shard_base(bytes: &[u8]) -> Result<u32> {
    if bytes.len() < FENCE_BYTES as usize {
        bail!("a shard base is {FENCE_BYTES} bytes, got {}", bytes.len());
    }
    Ok(u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")))
}

/// Where a bucket's entries live **inside its own shard**, from the pair
/// [`shard_fence_span`] returned.
///
/// ⚠️ `lo` and `hi` must be the SHARD-LOCAL indices that pair carried, not the base's.
/// The layout is the same one [`entries_span`] addresses, shifted by the fence table that
/// now precedes it.
pub fn shard_entries_span(lo: u32, hi: u32) -> Span {
    Span {
        offset: SHARD_FENCE_BYTES + u64::from(lo) * ENTRY_BYTES as u64,
        len: entries_span_len(lo, hi),
    }
}

/// Where a bucket's entries live inside its shard, from the **global** pair a read of the
/// directory returns.
///
/// ⚠️ **The one subtraction in this module that a wrong input makes plausible rather than
/// broken.** `base` is the shard's own first entry index; get it wrong and the span still
/// lands inside the object, still returns bytes, and still looks like a run — while
/// carrying another bucket's entries. The 8-byte prefix check catches some of that and
/// not all of it. So an underflow is refused rather than wrapped, and a reader that
/// cannot say where its shard starts must not take this path.
pub fn shard_entries_span_from(lo: u32, hi: u32, base: u32) -> Result<Span> {
    if hi < lo {
        bail!("entry range {lo}..{hi} is inverted");
    }
    let local = lo
        .checked_sub(base)
        .ok_or_else(|| anyhow::anyhow!("entry {lo} precedes its shard's first entry {base}"))?;
    Ok(shard_entries_span(local, hi - base))
}

/// The length of a shard whose fence table ends at `local_count`: the table, then the
/// entries that value counts.
pub fn shard_bytes(local_count: u32) -> u64 {
    SHARD_FENCE_BYTES + u64::from(local_count) * ENTRY_BYTES as u64
}

/// Every shard's bytes together, for a directory of `n_buckets` over `count` entries.
///
/// A property of the header alone, which is the point of it: a publisher writes this many
/// bytes and a verifier compares the mirror against this number, without either holding
/// the shards.
pub fn shards_bytes(n_buckets: u64, count: u64) -> u64 {
    shard_count(n_buckets) * SHARD_FENCE_BYTES + count * ENTRY_BYTES as u64
}

/// One shard's fence table, rebased on its own first value.
///
/// `global` is `fence[shard·SHARD_BUCKETS ..= (shard+1)·SHARD_BUCKETS]` — the same
/// `SHARD_BUCKETS + 1` values a reader of the base would use, spanning the shard's
/// buckets and the boundary after them.
///
/// Here rather than in the publisher because it is the *inverse* of what a reader does:
/// the reader adds the table's own offsets to the shard's base, so a publisher that
/// rebased differently would produce shards that read as every bucket starting at zero.
/// That is a silent wrong answer, not a failure.
pub fn rebase_fences(global: &[u32]) -> Result<Vec<u32>> {
    let want = SHARD_BUCKETS as usize + 1;
    if global.len() != want {
        bail!(
            "a shard's fence table is {want} values, got {}",
            global.len()
        );
    }
    let first = global[0];
    global
        .iter()
        .map(|v| {
            v.checked_sub(first)
                .ok_or_else(|| anyhow::anyhow!("fence {v} precedes the shard's own {first}"))
        })
        .collect()
}

/// Where the era byte of `chunk` lives. `None` when the base does not cover
/// that chunk — which is not a failure, it is a reader asking about the tip or
/// about a chunk from another build.
pub fn era_span(header: &BaseHeader, chunk: u16) -> Option<Span> {
    if chunk < header.first_chunk || chunk > header.last_chunk {
        return None;
    }
    Some(Span {
        offset: header.era_off + u64::from(chunk - header.first_chunk),
        len: 1,
    })
}

/// The body's span within its own chunk object. The chunk itself is
/// [`Entry::loc`]'s `chunk` field — a body never straddles two of them.
pub fn body_span(entry: &Entry) -> Span {
    Span {
        offset: u64::from(entry.loc.offset),
        len: u64::from(entry.loc.len),
    }
}

/// The auxiliary data's span within the same chunk, when the tx has any.
pub fn aux_span(entry: &Entry) -> Option<Span> {
    entry.aux.map(|a| Span {
        offset: u64::from(a.offset),
        len: u64::from(a.len),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::BASE_HEADER_BYTES;

    /// The published base's header, read off the object itself
    /// (`index/tx/v1/09202-416476537b08/base.idx`, 3,043,602,984 bytes).
    fn published_header() -> BaseHeader {
        BaseHeader {
            dir_bits: 24,
            first_chunk: 0,
            last_chunk: 9202,
            count: 124_020_199,
            era_off: 64,
            dir_off: 9_280,
            entries_off: 67_118_208,
        }
    }

    /// The reference hash this crate's published base was checked against:
    /// `000040ca401e8fea7a4a0457d9839bb2e50584d912fbf629def3113a074fd8f7`.
    const REFERENCE: u64 = 0x0000_40ca_401e_8fea;

    /// Bucket 64's six entries, 522..528, fetched from the published object as
    /// `Range: bytes=67130736-67130879`. Real bytes, not invented ones — a
    /// fixture built from the format alone would agree with whatever this
    /// module believes the format is.
    const BUCKET_64: [&str; 6] = [
        "b2386cc507400000340e70e6f900e0000000000000000000",
        "82cb39544c400000360a17021f01fe043333200161000000",
        "ff11fd9b73400000440ecf24e100e4000000000000000000",
        "b75f6ed3b9400000cd03d8b62a0089000000000000000000",
        "ea8f1e40ca400000be23d64634009c000000000000000000",
        "891315fddf400000d71f3e5a21007a020000000000000000",
    ];

    const LO: u32 = 522;
    const HI: u32 = 528;

    fn bucket_64() -> Vec<u8> {
        let hex: String = BUCKET_64.concat();
        hex::decode(hex).expect("the fixture is hex")
    }

    #[test]
    fn the_published_header_implies_the_published_length() {
        // The one arithmetic check on the header: a base's file length is
        // derivable, so a reader can tell it holds the whole object before it
        // starts believing offsets.
        assert_eq!(published_header().file_len(), 3_043_602_984);
    }

    #[test]
    fn the_spans_are_the_ones_the_object_served() {
        let h = published_header();
        // Both of these were the actual requests made against the object, and
        // both returned the bytes asserted below.
        assert_eq!(
            fence_span(&h, REFERENCE),
            Span {
                offset: 9_536,
                len: 8
            }
        );
        assert_eq!(
            entries_span(&h, LO, HI),
            Span {
                offset: 67_130_736,
                len: 144
            }
        );
        // Both section offsets sit on a 64-byte boundary and after the section
        // before them — the two rules `BaseHeader::layout` exists to keep.
        assert_eq!(h.era_off, BASE_HEADER_BYTES as u64);
        assert_eq!(h.dir_off % 64, 0);
        assert_eq!(h.entries_off % 64, 0);
        assert!(h.dir_off >= h.era_off + h.n_chunks() as u64);
        assert!(h.entries_off >= h.dir_off + ((1u64 << h.dir_bits) + 1) * 4);
    }

    #[test]
    fn the_reference_hash_locates_the_reference_body() {
        let entries = search(REFERENCE, LO, HI, &bucket_64()).unwrap();
        assert_eq!(entries.len(), 1, "one entry carries this prefix");
        let e = entries[0];
        assert_eq!(e.loc.chunk, 9150);
        assert_eq!(e.loc.offset, 3_426_006);
        assert_eq!(e.loc.len, 156);
        assert_eq!(e.aux, None, "this tx carries no auxiliary data");
        assert_eq!(
            body_span(&e),
            Span {
                offset: 3_426_006,
                len: 156
            }
        );
    }

    /// Every entry in the bucket shares the prefix's top 24 bits, and they come
    /// back in ascending order — the property the scan's early `break` trusts.
    #[test]
    fn the_bucket_is_sorted_and_shares_the_directory_key() {
        let entries = search(0, LO, HI, &bucket_64()).unwrap();
        assert!(entries.is_empty(), "no entry has an all-zero prefix");
        let bucket = bucket_of(REFERENCE, 24);
        for h in BUCKET_64 {
            let raw = hex::decode(h).unwrap();
            assert_eq!(bucket_of(Entry::prefix_at(&raw), 24), bucket);
        }
    }

    #[test]
    fn a_later_prefix_stops_the_scan_early() {
        // Entry 4 is the reference; entry 5 sorts after it, so asking for 4
        // must not return 5 even though 5 is in the run.
        let entries = search(REFERENCE, LO, HI, &bucket_64()).unwrap();
        assert_eq!(entries.len(), 1);
        let next = hex::decode(BUCKET_64[5]).unwrap();
        assert!(Entry::prefix_at(&next) > REFERENCE);
    }

    #[test]
    fn a_truncated_run_is_refused_rather_than_scanned() {
        let short = &bucket_64()[..120];
        let err = search(REFERENCE, LO, HI, short).unwrap_err().to_string();
        assert!(err.contains("truncated"), "{err}");
    }

    #[test]
    fn a_prefix_absent_from_its_bucket_scans_to_nothing() {
        // The same bucket, a prefix in it that is not there.
        let absent = REFERENCE + 1;
        assert!(search(absent, LO, HI, &bucket_64()).unwrap().is_empty());
    }

    #[test]
    fn an_empty_bucket_is_a_zero_length_span_not_an_error() {
        let h = published_header();
        assert_eq!(
            entries_span(&h, 700, 700),
            Span {
                offset: h.entries_off + 700 * 24,
                len: 0
            }
        );
        assert!(search(REFERENCE, 700, 700, &[]).unwrap().is_empty());
    }

    #[test]
    fn an_inverted_fence_pair_is_refused() {
        let mut bytes = [0u8; 8];
        bytes[0..4].copy_from_slice(&9u32.to_le_bytes());
        bytes[4..8].copy_from_slice(&3u32.to_le_bytes());
        assert!(fence_pair(&bytes).is_err());
    }

    /// The shard arithmetic against the published base: 2²⁴ buckets at 512 to a shard.
    #[test]
    fn every_bucket_lands_in_exactly_one_shard() {
        let n_buckets = 1u64 << 24;
        assert_eq!(shard_count(n_buckets), 32_768);
        assert_eq!(shard_of(0), 0);
        assert_eq!(shard_of(511), 0);
        assert_eq!(shard_of(512), 1);
        assert_eq!(shard_of(n_buckets - 1), 32_767);
        // The boundary is a multiple, so nothing is short; a narrower directory would
        // leave a last shard holding only the buckets that exist.
        assert_eq!(shard_count(n_buckets * 4), n_buckets * 4 / 512);
        assert_eq!(shard_count(513), 2);
    }

    /// ⚠️ The one arithmetic that a publisher and a reader must agree on, because
    /// disagreeing produces a plausible wrong answer rather than a failure: the reader
    /// adds a shard's local offsets to the shard's own base, so a table that was not
    /// rebased reads as every bucket starting at zero.
    #[test]
    fn the_shard_round_trips_through_its_own_fence_table() {
        // Bucket 64 is the shard's 65th, and it holds entries 522..528 globally.
        let mut global = vec![522u32; SHARD_BUCKETS as usize + 1];
        global[65] = 528;
        let local = rebase_fences(&global).unwrap();
        assert_eq!(local[0], 0, "the shard's own first value is the origin");
        assert_eq!(local[64], 0);
        assert_eq!(local[65], 6);

        // A reader asks for bucket 64 — the 65th boundary pair — and gets (0, 6).
        let span = shard_fence_span(64);
        assert_eq!(
            span,
            Span {
                offset: 256,
                len: 8
            }
        );
        let mut pair = [0u8; 8];
        pair[0..4].copy_from_slice(&local[64].to_le_bytes());
        pair[4..8].copy_from_slice(&local[65].to_le_bytes());
        let (lo, hi) = fence_pair(&pair).unwrap();
        assert_eq!((lo, hi), (0, 6));

        // …and the run those bound is the real bucket 64, at the fence table's end.
        assert_eq!(
            shard_entries_span(lo, hi),
            Span {
                offset: SHARD_FENCE_BYTES,
                len: 144
            }
        );
        let found = search(REFERENCE, lo, hi, &bucket_64()).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].loc.chunk, 9150);
        assert_eq!(found[0].loc.offset, 3_426_006);
    }

    /// A bucket in a later shard is rebased on THAT shard's first value, not on zero —
    /// which is the whole reason the table is local.
    #[test]
    fn a_later_shard_is_rebased_on_its_own_first_value() {
        let bucket = shard_first_bucket(3) + 2;
        let mut global = vec![900_000u32; SHARD_BUCKETS as usize + 1];
        global[3] = 900_121;
        let local = rebase_fences(&global).unwrap();
        assert_eq!(local[2], 0);
        assert_eq!(local[3], 121);
        let span = shard_fence_span(bucket);
        assert_eq!(span, Span { offset: 8, len: 8 }, "shard-local, not global");
        assert_eq!(
            shard_entries_span(0, 121),
            Span {
                offset: SHARD_FENCE_BYTES,
                len: 2_904
            }
        );
    }

    #[test]
    fn a_fence_table_that_is_not_a_shard_is_refused() {
        assert!(rebase_fences(&[1, 2, 3]).is_err());
        let mut descending = vec![10u32; SHARD_BUCKETS as usize + 1];
        descending[0] = 20;
        let err = rebase_fences(&descending).unwrap_err().to_string();
        assert!(err.contains("precedes"), "{err}");
    }

    #[test]
    fn a_shard_is_its_fence_table_plus_the_entries_it_counts() {
        assert_eq!(SHARD_FENCE_BYTES, 2_052);
        assert_eq!(shard_bytes(0), 2_052);
        assert_eq!(shard_bytes(7), 2_052 + 7 * 24);
        // The published base's own average: 124,020,199 entries over 32,768 shards.
        assert_eq!(shard_bytes(3_785), 92_892);
    }

    /// The other way to reach a shard's entries: the pair read from the base's DIRECTORY.
    /// Those values are global, so the shard's own first entry has to be subtracted — and
    /// the byte span has to come out identical to the shard-local route.
    #[test]
    fn a_global_pair_addresses_the_same_run_as_a_local_one() {
        let h = published_header();
        // Bucket 64 is in shard 0, and shard 0 starts at entry 0 on this base.
        let base = shard_base(&0u32.to_le_bytes()).unwrap();
        assert_eq!(base, 0);
        let span = shard_entries_span_from(LO, HI, base).unwrap();
        assert_eq!(span, shard_entries_span(LO, HI));
        assert_eq!(span.len, 144);

        // A later shard, where the two routes differ: bucket 700 of shard 1 starts at
        // global entry 6,000 and the shard starts at 5,900, so the run begins 100 entries
        // in — 2,400 bytes past the fence table.
        let span = shard_entries_span_from(6_000, 6_100, 5_900).unwrap();
        assert_eq!(
            span,
            Span {
                offset: SHARD_FENCE_BYTES + 2_400,
                len: 2_400
            }
        );
        assert_eq!(
            span,
            shard_entries_span(100, 200),
            "the same run, addressed from either end"
        );
    }

    /// ⚠️ A shard base that does not bound the entry index must FAIL. Wrapped, it would
    /// return bytes from the far end of the shard under the name of this bucket — which is
    /// a silent wrong answer, not a short read.
    #[test]
    fn a_shard_base_that_does_not_bound_the_entry_is_refused() {
        let err = shard_entries_span_from(100, 200, 5_000)
            .unwrap_err()
            .to_string();
        assert!(err.contains("precedes"), "{err}");
        let err = shard_entries_span_from(200, 100, 0)
            .unwrap_err()
            .to_string();
        assert!(err.contains("inverted"), "{err}");
        assert!(shard_base(&[1, 2]).is_err());
    }

    #[test]
    fn a_shard_base_is_addressed_off_the_directory() {
        let h = published_header();
        // Shard 0's base is the directory's first slot; 512 buckets to a shard, 4 bytes each.
        assert_eq!(
            shard_base_span(&h, 0),
            Span {
                offset: h.dir_off,
                len: 4
            }
        );
        assert_eq!(
            shard_base_span(&h, 1),
            Span {
                offset: h.dir_off + 2_048,
                len: 4
            }
        );
        // The last shard's base is one shard-width before the sentinel — fence[512 · 32,767],
        // with the (2²⁴ + 1)th value bounding the final bucket.
        let last = shard_base_span(&h, 32_767);
        assert_eq!(
            last.offset,
            h.dir_off + (1 << 24) * 4 - SHARD_BUCKETS * FENCE_BYTES
        );
        assert!(last.offset + last.len <= h.entries_off);
    }

    #[test]
    fn era_bytes_are_addressed_by_chunk_and_bounded_by_the_coverage() {
        let h = published_header();
        assert_eq!(era_span(&h, 0), Some(Span { offset: 64, len: 1 }));
        assert_eq!(
            era_span(&h, 9202),
            Some(Span {
                offset: 64 + 9202,
                len: 1
            })
        );
        // The tip, and a chunk from a build that covered more: both are
        // "this base cannot answer", which is why this is an Option.
        assert_eq!(era_span(&h, 9203), None);
        assert_eq!(era_span(&h, 0).map(|s| s.offset), Some(h.era_off));
    }

    /// An aux span is read against the body's chunk, never its own — the
    /// format stores no chunk for it, because they cannot differ.
    #[test]
    fn an_aux_span_borrows_the_body_s_chunk() {
        let raw = hex::decode(BUCKET_64[1]).unwrap();
        let e = Entry::read(&raw);
        assert_eq!(e.loc.chunk, 2614);
        assert_eq!(
            aux_span(&e),
            Some(Span {
                offset: 18_887_475,
                len: 97
            })
        );
        assert_eq!(e.aux_location().unwrap().chunk, 2614);
        assert_eq!(
            aux_span(&Entry::read(&hex::decode(BUCKET_64[0]).unwrap())),
            None
        );
    }

    #[test]
    fn a_span_slices_the_buffer_it_names() {
        let whole: Vec<u8> = (0..=255u8).collect();
        let span = Span { offset: 10, len: 3 };
        assert_eq!(span.of(&whole), &[10, 11, 12]);
    }

    #[test]
    fn the_span_helpers_agree_with_the_header_layout() {
        // Built through the layout function rather than by hand, so a change to
        // the padding rules moves both sides together.
        let h = BaseHeader::layout(24, 100, 199, 1_000);
        let prefix = 0xAB_0000_0000_0000u64;
        let span = fence_span(&h, prefix);
        let bucket = bucket_of(prefix, h.dir_bits) as u64;
        assert_eq!(span.offset, h.dir_off + bucket * 4);
        assert_eq!(span.offset + span.len, h.dir_off + bucket * 4 + 8);
        assert!(
            span.offset + span.len <= h.entries_off,
            "fences precede entries"
        );
        assert_eq!(entries_span(&h, 0, h.count as u32).len, h.count * 24);
    }
}
