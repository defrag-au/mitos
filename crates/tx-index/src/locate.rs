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
