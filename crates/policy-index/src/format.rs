//! On-disk shapes: one 32-byte mint record, a 4-byte time permutation, and a
//! 24-byte policy side-table. Fixed strides throughout, so compaction places
//! records rather than re-encoding them — the same discipline `tx-index` uses.
//!
//! # ⚠️ The entry is 32 bytes, not 24
//!
//! `POLICY_INDEX.md` proposed reusing the tx-index's 24-byte entry verbatim.
//! **That does not survive contact with the problem.** A tx entry's key is ONE
//! hash; a mint record has to identify a policy AND an asset within it, and
//! carry a location and an aux span:
//!
//! ```text
//! policy_prefix u64  8   first 8 B of the policy id — already a hash, so uniform
//! name_prefix   u64  8   first 8 B of blake2b(asset_name); 0 for the empty name
//! chunk         u16  2   immutable file holding the minting tx
//! offset        u32  4   byte offset of the tx BODY in that chunk
//! len           u16  2   body length
//! aux_offset    u32  4   byte offset of that tx's auxiliary data (CIP-25)
//! aux_len       u16  2   0 = none
//! flags          u8  1   bit 0: this event BURNED rather than minted
//! reserved       u8  1   must be zero
//!                   ─── 32
//! ```
//!
//! Squeezing to 24 would mean dropping either the policy (making entries
//! unmergeable across chunks, because a segment holds many policies) or the
//! aux span (throwing away the metadata lookup that is half the point). Both
//! are worse than 8 bytes.
//!
//! # Why the time ordering is a permutation, not a second copy
//!
//! Two orderings were asked for and both are wanted — asset lookup and mint
//! timeline. Storing the records twice would cost a second ~480 MB. Instead
//! the second ordering is a `u32` INDEX ARRAY over the same records, grouped
//! by the same policies in the same order and sorted within each policy's run
//! by `(chunk, name_prefix)`.
//!
//! ⇒ 4 bytes per record instead of 32, and the two views cannot disagree
//! about what a record says, because there is only one record.
//!
//! # Sizing, against MEASURED counts
//!
//! 11,175,616 native assets on mainnet; INFERRED ~15M mint events once
//! re-mints and burns are counted.
//!
//! | section | width | ~15M events |
//! |---|---|---|
//! | records | 32 B | 458 MiB |
//! | time permutation | 4 B | 57 MiB |
//! | policy side-table | 24 B | 11 MiB at 500k policies |
//! | prefix directory | 4 B × 2^21 | 8 MiB |
//! | **total** | | **534 MiB** |
//!
//! Against the 3.03 GB `base.idx` the tx-index already keeps. (MiB
//! throughout: `POLICY_INDEX.md`'s first estimate said "~770 MB" by counting
//! the second ordering as a full copy of the records rather than a
//! permutation, and "~560" by mixing MB with MiB. `BaseHeader::layout` is the
//! authority and a test pins it.)

use anyhow::{Result, bail};

pub const RECORD_BYTES: usize = 32;
pub const POLICY_BYTES: usize = 24;
pub const PERM_BYTES: usize = 4;

pub const SEGMENT_MAGIC: [u8; 4] = *b"PXS1";
pub const BASE_MAGIC: [u8; 4] = *b"PXB1";
pub const SEGMENT_HEADER_BYTES: usize = 16;
pub const BASE_HEADER_BYTES: usize = 64;

/// Bumped when any stride or field meaning changes. An older file is REFUSED
/// rather than reinterpreted: reading 32-byte records at a different width
/// returns plausible garbage instead of failing, which is the failure mode
/// this whole crate exists to avoid.
pub const FORMAT_VERSION: u8 = 1;

/// Directory width. 2^21 buckets over ~15M records is ~7 per bucket — the same
/// target `tx-index` picked for its 2^24 over 123.6M — and an 8 MB fence that
/// stays resident.
///
/// ⚠️ NOT 24 like the tx-index. At 15M records 2^24 buckets would be 0.9
/// entries per bucket and the 64 MB directory would outweigh a sixth of the
/// data it indexes. The width follows the corpus, not the sibling.
pub const DIR_BITS: u8 = 21;

/// A mint or burn of one asset, and where the transaction that did it lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record {
    pub policy_prefix: u64,
    pub name_prefix: u64,
    pub chunk: u16,
    pub offset: u32,
    pub len: u16,
    pub aux_offset: u32,
    pub aux_len: u16,
    /// ⚠️ A BURN is recorded, not skipped. "The first mint" is the origin, and
    /// you cannot tell a first mint from a re-mint after a burn unless the
    /// burn is in the record too.
    pub burned: bool,
}

const FLAG_BURNED: u8 = 1 << 0;

impl Record {
    /// The sort key of the ASSET ordering: group by policy, then by asset,
    /// then oldest first so a run's first entry is its earliest event.
    pub fn asset_key(&self) -> (u64, u64, u16) {
        (self.policy_prefix, self.name_prefix, self.chunk)
    }

    /// The sort key of the TIME ordering, within one policy's run.
    pub fn time_key(&self) -> (u16, u64) {
        (self.chunk, self.name_prefix)
    }

    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), RECORD_BYTES);
        out[0..8].copy_from_slice(&self.policy_prefix.to_le_bytes());
        out[8..16].copy_from_slice(&self.name_prefix.to_le_bytes());
        out[16..18].copy_from_slice(&self.chunk.to_le_bytes());
        out[18..22].copy_from_slice(&self.offset.to_le_bytes());
        out[22..24].copy_from_slice(&self.len.to_le_bytes());
        out[24..28].copy_from_slice(&self.aux_offset.to_le_bytes());
        out[28..30].copy_from_slice(&self.aux_len.to_le_bytes());
        out[30] = if self.burned { FLAG_BURNED } else { 0 };
        out[31] = 0;
    }

    pub fn read(b: &[u8]) -> Record {
        debug_assert_eq!(b.len(), RECORD_BYTES);
        Record {
            policy_prefix: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            name_prefix: u64::from_le_bytes(b[8..16].try_into().expect("8 bytes")),
            chunk: u16::from_le_bytes(b[16..18].try_into().expect("2 bytes")),
            offset: u32::from_le_bytes(b[18..22].try_into().expect("4 bytes")),
            len: u16::from_le_bytes(b[22..24].try_into().expect("2 bytes")),
            aux_offset: u32::from_le_bytes(b[24..28].try_into().expect("4 bytes")),
            aux_len: u16::from_le_bytes(b[28..30].try_into().expect("2 bytes")),
            burned: b[30] & FLAG_BURNED != 0,
        }
    }

    /// Read the policy prefix alone — the hot path of a bucket scan.
    pub fn policy_prefix_at(b: &[u8]) -> u64 {
        u64::from_le_bytes(b[0..8].try_into().expect("8 bytes"))
    }

    /// `None` when the minting transaction carried no metadata.
    ///
    /// This span is the reason an asset lookup is worth having: it points at
    /// the CIP-25 metadata of the transaction that created the asset, in the
    /// same chunk as the body, so "what is this asset" is one `pread`.
    pub fn aux_span(&self) -> Option<(u32, u16)> {
        (self.aux_len != 0).then_some((self.aux_offset, self.aux_len))
    }
}

/// One policy's run in the record array.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PolicyRun {
    pub policy_prefix: u64,
    /// Index of the run's first record.
    pub start: u32,
    /// Records in the run — mint EVENTS, not distinct assets.
    pub len: u32,
    /// The chunk of the policy's earliest event. THE FLOOR PROBE'S ANSWER,
    /// readable without touching the record array at all.
    pub first_chunk: u16,
}

impl PolicyRun {
    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), POLICY_BYTES);
        out.fill(0);
        out[0..8].copy_from_slice(&self.policy_prefix.to_le_bytes());
        out[8..12].copy_from_slice(&self.start.to_le_bytes());
        out[12..16].copy_from_slice(&self.len.to_le_bytes());
        out[16..18].copy_from_slice(&self.first_chunk.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> PolicyRun {
        debug_assert_eq!(b.len(), POLICY_BYTES);
        PolicyRun {
            policy_prefix: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            start: u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
            len: u32::from_le_bytes(b[12..16].try_into().expect("4 bytes")),
            first_chunk: u16::from_le_bytes(b[16..18].try_into().expect("2 bytes")),
        }
    }

    pub fn prefix_at(b: &[u8]) -> u64 {
        u64::from_le_bytes(b[0..8].try_into().expect("8 bytes"))
    }
}

/// The 8-byte prefix of a 28-byte policy id.
///
/// A policy id is blake2b-224 of the minting script, so it is already uniform
/// and needs no further hashing — the same property that lets `tx-index`
/// bucket raw tx hashes.
pub fn policy_prefix(policy_id: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = policy_id.len().min(8);
    b[..n].copy_from_slice(&policy_id[..n]);
    u64::from_be_bytes(b)
}

/// The 8-byte prefix of an asset name.
///
/// ⚠️ HASHED, unlike the policy. An asset name is human-chosen — `#0001`,
/// `SolJourney1-001` — so raw bytes would pile a whole collection into a
/// handful of buckets and turn a bucket scan into a collection scan. blake2b
/// restores the uniformity the directory assumes.
pub fn name_prefix(asset_name: &[u8]) -> u64 {
    if asset_name.is_empty() {
        return 0;
    }
    let h = pallas_crypto::hash::Hasher::<256>::hash(asset_name);
    u64::from_be_bytes(h.as_ref()[0..8].try_into().expect("8 bytes"))
}

/// The directory key of a prefix: its top `dir_bits` bits.
pub fn bucket_of(prefix: u64, dir_bits: u8) -> usize {
    (prefix >> (64 - u32::from(dir_bits))) as usize
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub chunk: u16,
    pub count: u32,
}

impl SegmentHeader {
    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), SEGMENT_HEADER_BYTES);
        out.fill(0);
        out[0..4].copy_from_slice(&SEGMENT_MAGIC);
        out[4] = FORMAT_VERSION;
        out[6..8].copy_from_slice(&self.chunk.to_le_bytes());
        out[8..12].copy_from_slice(&self.count.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> Result<SegmentHeader> {
        if b.len() < SEGMENT_HEADER_BYTES {
            bail!("policy segment header truncated ({} bytes)", b.len());
        }
        if b[0..4] != SEGMENT_MAGIC {
            // Named rather than generic: the likeliest cause is a tx-index
            // segment reached by the wrong reader, and the two are the same
            // shape on disk from a distance.
            bail!("not a policy-index segment (bad magic) — a tx-index segment?");
        }
        if b[4] != FORMAT_VERSION {
            bail!(
                "policy segment format version {} (this build reads {FORMAT_VERSION})",
                b[4]
            );
        }
        Ok(SegmentHeader {
            chunk: u16::from_le_bytes(b[6..8].try_into().expect("2 bytes")),
            count: u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
        })
    }
}

/// Base header. Every section offset is explicit so a reader never re-derives
/// an alignment rule the writer applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseHeader {
    pub dir_bits: u8,
    pub first_chunk: u16,
    pub last_chunk: u16,
    /// Mint events.
    pub count: u64,
    /// Distinct policies.
    pub policies: u64,
    pub dir_off: u64,
    pub records_off: u64,
    pub perm_off: u64,
    pub policies_off: u64,
}

impl BaseHeader {
    pub fn layout(
        dir_bits: u8,
        first_chunk: u16,
        last_chunk: u16,
        count: u64,
        policies: u64,
    ) -> BaseHeader {
        let n_buckets = 1u64 << dir_bits;
        let dir_off = BASE_HEADER_BYTES as u64;
        let records_off = align64(dir_off + (n_buckets + 1) * 4);
        let perm_off = align64(records_off + count * RECORD_BYTES as u64);
        let policies_off = align64(perm_off + count * PERM_BYTES as u64);
        BaseHeader {
            dir_bits,
            first_chunk,
            last_chunk,
            count,
            policies,
            dir_off,
            records_off,
            perm_off,
            policies_off,
        }
    }

    pub fn file_len(&self) -> u64 {
        self.policies_off + self.policies * POLICY_BYTES as u64
    }

    pub fn n_buckets(&self) -> usize {
        1usize << self.dir_bits
    }

    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), BASE_HEADER_BYTES);
        out.fill(0);
        out[0..4].copy_from_slice(&BASE_MAGIC);
        out[4] = FORMAT_VERSION;
        out[5] = self.dir_bits;
        out[6..8].copy_from_slice(&self.first_chunk.to_le_bytes());
        out[8..10].copy_from_slice(&self.last_chunk.to_le_bytes());
        out[16..24].copy_from_slice(&self.count.to_le_bytes());
        out[24..32].copy_from_slice(&self.policies.to_le_bytes());
        out[32..40].copy_from_slice(&self.dir_off.to_le_bytes());
        out[40..48].copy_from_slice(&self.records_off.to_le_bytes());
        out[48..56].copy_from_slice(&self.perm_off.to_le_bytes());
        out[56..64].copy_from_slice(&self.policies_off.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> Result<BaseHeader> {
        if b.len() < BASE_HEADER_BYTES {
            bail!("policy base header truncated ({} bytes)", b.len());
        }
        if b[0..4] != BASE_MAGIC {
            bail!("not a policy-index base (bad magic)");
        }
        if b[4] != FORMAT_VERSION {
            bail!(
                "policy base format version {} (this build reads {FORMAT_VERSION})",
                b[4]
            );
        }
        let hdr = BaseHeader {
            dir_bits: b[5],
            first_chunk: u16::from_le_bytes(b[6..8].try_into().expect("2 bytes")),
            last_chunk: u16::from_le_bytes(b[8..10].try_into().expect("2 bytes")),
            count: u64::from_le_bytes(b[16..24].try_into().expect("8 bytes")),
            policies: u64::from_le_bytes(b[24..32].try_into().expect("8 bytes")),
            dir_off: u64::from_le_bytes(b[32..40].try_into().expect("8 bytes")),
            records_off: u64::from_le_bytes(b[40..48].try_into().expect("8 bytes")),
            perm_off: u64::from_le_bytes(b[48..56].try_into().expect("8 bytes")),
            policies_off: u64::from_le_bytes(b[56..64].try_into().expect("8 bytes")),
        };
        if hdr.dir_bits == 0 || hdr.dir_bits > 32 {
            bail!("policy base dir_bits {} out of range", hdr.dir_bits);
        }
        if hdr.last_chunk < hdr.first_chunk {
            bail!(
                "policy base chunk range {}..={} is inverted",
                hdr.first_chunk,
                hdr.last_chunk
            );
        }
        Ok(hdr)
    }
}

fn align64(x: u64) -> u64 {
    x.div_ceil(64) * 64
}

/// Parse `NNNNN` from a `NNNNN.pseg` file name.
pub fn chunk_number(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)?.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec() -> Record {
        Record {
            policy_prefix: 0x0102_0304_0506_0708,
            name_prefix: 0x1112_1314_1516_1718,
            chunk: 5851,
            offset: 123_456,
            len: 15_245,
            aux_offset: 654_321,
            aux_len: 15_872,
            burned: false,
        }
    }

    #[test]
    fn a_record_round_trips() {
        let mut b = [0u8; RECORD_BYTES];
        rec().write(&mut b);
        assert_eq!(Record::read(&b), rec());
    }

    /// The burn flag is the difference between "this asset was created here"
    /// and "this asset was destroyed here", and both live in the same array.
    #[test]
    fn the_burn_flag_round_trips_and_defaults_off() {
        let mut burn = rec();
        burn.burned = true;
        let mut b = [0u8; RECORD_BYTES];
        burn.write(&mut b);
        assert!(Record::read(&b).burned);

        let mut b = [0u8; RECORD_BYTES];
        rec().write(&mut b);
        assert!(!Record::read(&b).burned);
    }

    /// ⚠️ MEASURED on-chain art maxima (body 15,245 B, aux 15,872 B) sit well
    /// inside u16 — this pins that the fields really do carry them.
    #[test]
    fn the_measured_on_chain_art_maxima_fit() {
        let mut b = [0u8; RECORD_BYTES];
        rec().write(&mut b);
        let got = Record::read(&b);
        assert_eq!(got.len, 15_245);
        assert_eq!(got.aux_len, 15_872);
    }

    #[test]
    fn the_hot_path_prefix_read_matches_a_full_read() {
        let mut b = [0u8; RECORD_BYTES];
        rec().write(&mut b);
        assert_eq!(Record::policy_prefix_at(&b), rec().policy_prefix);
    }

    #[test]
    fn an_absent_aux_span_reads_as_none() {
        let mut r = rec();
        r.aux_len = 0;
        r.aux_offset = 0;
        let mut b = [0u8; RECORD_BYTES];
        r.write(&mut b);
        assert_eq!(Record::read(&b).aux_span(), None);
    }

    /// A policy id is ALREADY a hash, so its own bytes bucket evenly. This
    /// pins that we do not accidentally start hashing it — that would be
    /// harmless but would break every file written before the change.
    #[test]
    fn a_policy_prefix_is_its_own_leading_bytes() {
        let id = hex::decode("caff93803e51c7b97bf79146790bfa3feb0d0b856ef16113b391b997").unwrap();
        assert_eq!(policy_prefix(&id), 0xcaff_9380_3e51_c7b9);
    }

    /// ⚠️ THE POINT OF HASHING NAMES. Sequential collection names differ in
    /// their LAST bytes, so raw prefixes would be identical and every asset in
    /// a 10k collection would land in one bucket.
    #[test]
    fn sequential_asset_names_scatter_across_buckets() {
        let names: Vec<Vec<u8>> = (0..64)
            .map(|i| format!("SolJourney1-{i:04}").into_bytes())
            .collect();
        let buckets: std::collections::HashSet<usize> = names
            .iter()
            .map(|n| bucket_of(name_prefix(n), DIR_BITS))
            .collect();
        // Raw bytes would give ONE bucket; hashed, they scatter.
        assert!(
            buckets.len() > 50,
            "expected wide scatter, got {} buckets from 64 names",
            buckets.len()
        );
        let raw: std::collections::HashSet<u64> = names
            .iter()
            .map(|n| {
                let mut b = [0u8; 8];
                let k = n.len().min(8);
                b[..k].copy_from_slice(&n[..k]);
                u64::from_be_bytes(b)
            })
            .collect();
        assert_eq!(raw.len(), 1, "the unhashed prefix really is degenerate");
    }

    #[test]
    fn the_empty_asset_name_has_a_stable_prefix() {
        assert_eq!(name_prefix(b""), 0);
    }

    #[test]
    fn a_policy_run_round_trips() {
        let run = PolicyRun {
            policy_prefix: 0xdead_beef_0000_0001,
            start: 42,
            len: 513,
            first_chunk: 2707,
        };
        let mut b = [0u8; POLICY_BYTES];
        run.write(&mut b);
        assert_eq!(PolicyRun::read(&b), run);
        assert_eq!(PolicyRun::prefix_at(&b), run.policy_prefix);
    }

    #[test]
    fn a_segment_header_round_trips() {
        let hdr = SegmentHeader {
            chunk: 9129,
            count: 1234,
        };
        let mut b = [0u8; SEGMENT_HEADER_BYTES];
        hdr.write(&mut b);
        assert_eq!(SegmentHeader::read(&b).unwrap(), hdr);
    }

    /// ⚠️ The two crates write the same-shaped files side by side. Reading one
    /// as the other must FAIL rather than return plausible garbage.
    #[test]
    fn a_tx_index_segment_is_refused() {
        let mut b = [0u8; SEGMENT_HEADER_BYTES];
        b[0..4].copy_from_slice(b"TXS1");
        b[4] = 2;
        let err = SegmentHeader::read(&b).unwrap_err().to_string();
        assert!(err.contains("tx-index"), "unhelpful error: {err}");
    }

    #[test]
    fn a_future_format_version_is_refused_rather_than_reinterpreted() {
        let mut b = [0u8; SEGMENT_HEADER_BYTES];
        b[0..4].copy_from_slice(&SEGMENT_MAGIC);
        b[4] = FORMAT_VERSION + 1;
        assert!(SegmentHeader::read(&b).is_err());
    }

    #[test]
    fn base_sections_do_not_overlap_and_are_aligned() {
        let h = BaseHeader::layout(DIR_BITS, 0, 9129, 15_000_000, 500_000);
        assert!(h.dir_off < h.records_off);
        assert!(h.records_off < h.perm_off);
        assert!(h.perm_off < h.policies_off);
        assert!(h.records_off >= h.dir_off + (h.n_buckets() as u64 + 1) * 4);
        assert!(h.perm_off >= h.records_off + h.count * RECORD_BYTES as u64);
        assert!(h.policies_off >= h.perm_off + h.count * PERM_BYTES as u64);
        for off in [h.dir_off, h.records_off, h.perm_off, h.policies_off] {
            assert_eq!(off % 64, 0, "section at {off} is not 64-byte aligned");
        }
    }

    #[test]
    fn a_base_header_round_trips() {
        let h = BaseHeader::layout(DIR_BITS, 3, 9129, 15_000_000, 500_000);
        let mut b = [0u8; BASE_HEADER_BYTES];
        h.write(&mut b);
        assert_eq!(BaseHeader::read(&b).unwrap(), h);
    }

    #[test]
    fn an_inverted_chunk_range_is_refused() {
        let mut h = BaseHeader::layout(DIR_BITS, 0, 10, 1, 1);
        h.first_chunk = 20;
        let mut b = [0u8; BASE_HEADER_BYTES];
        h.write(&mut b);
        assert!(BaseHeader::read(&b).is_err());
    }

    /// The whole-corpus figure the design note sizes against, asserted so a
    /// stride change cannot quietly invalidate it.
    ///
    /// 534 MiB for the projected 15M mint events — against the 3.03 GB the
    /// tx-index's own base already occupies on the same box.
    #[test]
    fn fifteen_million_events_fit_the_predicted_footprint() {
        let h = BaseHeader::layout(DIR_BITS, 0, 9129, 15_000_000, 500_000);
        let mib = h.file_len() / 1_048_576;
        assert_eq!(mib, 534, "footprint moved: {mib} MiB for 15M events");
    }
}
