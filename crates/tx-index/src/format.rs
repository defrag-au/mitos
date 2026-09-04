//! On-disk formats — ONE entry shape shared by the per-chunk segments and
//! the compacted base, so compaction places fixed-size records and never
//! re-encodes.
//!
//! Entry (16 bytes, little-endian fields):
//!
//! | field  | type | meaning |
//! |--------|------|---------|
//! | prefix | u64  | first 8 bytes of the tx hash read big-endian — numeric order is hash order, top bits are the directory bucket |
//! | chunk  | u16  | immutable file number (`NNNNN.chunk`) |
//! | offset | u32  | byte offset of the tx BODY within that chunk file |
//! | len    | u16  | body length in bytes |
//!
//! The location points at the BODY, not the block: the body is the hashed
//! item, it is contiguous in every era (Shelley+ blocks split bodies and
//! witnesses into separate arrays, Byron wraps `[tx, witnesses]`), and it is
//! all an output lookup needs. A resolve reads a few hundred bytes, not a
//! block.
//!
//! Era is not in the entry: hard forks land on epoch boundaries and epochs
//! are whole chunks on every Cardano network, so era is a property of the
//! chunk. Segments carry it in their header; the base carries a one-byte
//! per-chunk table. Extraction verifies the invariant and refuses a chunk
//! that mixes eras rather than record a lie.

use anyhow::{Context, Result, bail};
use pallas_traverse::Era;

pub const ENTRY_BYTES: usize = 16;

pub const SEGMENT_MAGIC: [u8; 4] = *b"TXS1";
pub const SEGMENT_HEADER_BYTES: usize = 16;

pub const BASE_MAGIC: [u8; 4] = *b"TXB1";
pub const BASE_HEADER_BYTES: usize = 64;

/// Directory width. 2^24 buckets over ~110M mainnet entries is ~7 per
/// bucket; the fence array is 64 MB and stays hot.
pub const DIR_BITS: u8 = 24;

pub const FORMAT_VERSION: u8 = 1;

/// Where a tx body lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Location {
    pub chunk: u16,
    pub offset: u32,
    pub len: u16,
}

/// One index record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    pub prefix: u64,
    pub loc: Location,
}

impl Entry {
    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), ENTRY_BYTES);
        out[0..8].copy_from_slice(&self.prefix.to_le_bytes());
        out[8..10].copy_from_slice(&self.loc.chunk.to_le_bytes());
        out[10..14].copy_from_slice(&self.loc.offset.to_le_bytes());
        out[14..16].copy_from_slice(&self.loc.len.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> Entry {
        debug_assert_eq!(b.len(), ENTRY_BYTES);
        Entry {
            prefix: u64::from_le_bytes(b[0..8].try_into().expect("8 bytes")),
            loc: Location {
                chunk: u16::from_le_bytes(b[8..10].try_into().expect("2 bytes")),
                offset: u32::from_le_bytes(b[10..14].try_into().expect("4 bytes")),
                len: u16::from_le_bytes(b[14..16].try_into().expect("2 bytes")),
            },
        }
    }

    /// Read the prefix alone — the hot path of a bucket scan.
    pub fn prefix_at(b: &[u8]) -> u64 {
        u64::from_le_bytes(b[0..8].try_into().expect("8 bytes"))
    }
}

/// The directory key of a hash: its top `DIR_BITS` bits.
pub fn bucket_of(prefix: u64, dir_bits: u8) -> usize {
    (prefix >> (64 - u32::from(dir_bits))) as usize
}

/// The 8-byte prefix an index stores for a 32-byte tx hash.
pub fn prefix_of(hash: &[u8; 32]) -> u64 {
    u64::from_be_bytes(hash[0..8].try_into().expect("8 bytes"))
}

/// pallas's own era numbering (Byron = 1 … Conway = 7), narrowed to a byte.
pub fn era_to_u8(era: Era) -> u8 {
    u16::from(era) as u8
}

pub fn era_from_u8(b: u8) -> Result<Era> {
    Era::try_from(u16::from(b)).map_err(|e| anyhow::anyhow!("era byte {b}: {e:?}"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub era: u8,
    pub chunk: u16,
    pub count: u32,
}

impl SegmentHeader {
    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), SEGMENT_HEADER_BYTES);
        out[0..4].copy_from_slice(&SEGMENT_MAGIC);
        out[4] = FORMAT_VERSION;
        out[5] = self.era;
        out[6..8].copy_from_slice(&self.chunk.to_le_bytes());
        out[8..12].copy_from_slice(&self.count.to_le_bytes());
        out[12..16].copy_from_slice(&[0u8; 4]);
    }

    pub fn read(b: &[u8]) -> Result<SegmentHeader> {
        if b.len() < SEGMENT_HEADER_BYTES {
            bail!("segment header truncated ({} bytes)", b.len());
        }
        if b[0..4] != SEGMENT_MAGIC {
            bail!("not a tx-index segment (bad magic)");
        }
        if b[4] != FORMAT_VERSION {
            bail!(
                "segment format version {} (this build reads {FORMAT_VERSION})",
                b[4]
            );
        }
        Ok(SegmentHeader {
            era: b[5],
            chunk: u16::from_le_bytes(b[6..8].try_into().expect("2 bytes")),
            count: u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")),
        })
    }
}

/// Base header. All section offsets are explicit so the reader never
/// re-derives padding rules.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseHeader {
    pub dir_bits: u8,
    pub first_chunk: u16,
    pub last_chunk: u16,
    pub count: u64,
    pub era_off: u64,
    pub dir_off: u64,
    pub entries_off: u64,
}

impl BaseHeader {
    pub fn write(&self, out: &mut [u8]) {
        debug_assert_eq!(out.len(), BASE_HEADER_BYTES);
        out.fill(0);
        out[0..4].copy_from_slice(&BASE_MAGIC);
        out[4] = FORMAT_VERSION;
        out[5] = self.dir_bits;
        out[6..8].copy_from_slice(&self.first_chunk.to_le_bytes());
        out[8..10].copy_from_slice(&self.last_chunk.to_le_bytes());
        out[16..24].copy_from_slice(&self.count.to_le_bytes());
        out[24..32].copy_from_slice(&self.era_off.to_le_bytes());
        out[32..40].copy_from_slice(&self.dir_off.to_le_bytes());
        out[40..48].copy_from_slice(&self.entries_off.to_le_bytes());
    }

    pub fn read(b: &[u8]) -> Result<BaseHeader> {
        if b.len() < BASE_HEADER_BYTES {
            bail!("base header truncated ({} bytes)", b.len());
        }
        if b[0..4] != BASE_MAGIC {
            bail!("not a tx-index base (bad magic)");
        }
        if b[4] != FORMAT_VERSION {
            bail!(
                "base format version {} (this build reads {FORMAT_VERSION})",
                b[4]
            );
        }
        let hdr = BaseHeader {
            dir_bits: b[5],
            first_chunk: u16::from_le_bytes(b[6..8].try_into().expect("2 bytes")),
            last_chunk: u16::from_le_bytes(b[8..10].try_into().expect("2 bytes")),
            count: u64::from_le_bytes(b[16..24].try_into().expect("8 bytes")),
            era_off: u64::from_le_bytes(b[24..32].try_into().expect("8 bytes")),
            dir_off: u64::from_le_bytes(b[32..40].try_into().expect("8 bytes")),
            entries_off: u64::from_le_bytes(b[40..48].try_into().expect("8 bytes")),
        };
        if hdr.dir_bits == 0 || hdr.dir_bits > 32 {
            bail!("base dir_bits {} out of range", hdr.dir_bits);
        }
        if hdr.last_chunk < hdr.first_chunk {
            bail!(
                "base chunk range {}..={} is inverted",
                hdr.first_chunk,
                hdr.last_chunk
            );
        }
        Ok(hdr)
    }

    pub fn n_chunks(&self) -> usize {
        usize::from(self.last_chunk - self.first_chunk) + 1
    }

    pub fn n_buckets(&self) -> usize {
        1usize << self.dir_bits
    }

    /// Lay out a base for `n_chunks` era bytes and `n_buckets` fence slots,
    /// each section 64-byte aligned.
    pub fn layout(dir_bits: u8, first_chunk: u16, last_chunk: u16, count: u64) -> BaseHeader {
        let n_chunks = usize::from(last_chunk - first_chunk) + 1;
        let n_buckets = 1usize << dir_bits;
        let era_off = BASE_HEADER_BYTES as u64;
        let dir_off = align64(era_off + n_chunks as u64);
        let entries_off = align64(dir_off + ((n_buckets + 1) * 4) as u64);
        BaseHeader {
            dir_bits,
            first_chunk,
            last_chunk,
            count,
            era_off,
            dir_off,
            entries_off,
        }
    }

    pub fn file_len(&self) -> u64 {
        self.entries_off + self.count * ENTRY_BYTES as u64
    }
}

fn align64(x: u64) -> u64 {
    x.div_ceil(64) * 64
}

/// Parse `NNNNN` from a `NNNNN.chunk` / `NNNNN.seg` file name.
pub fn chunk_number(name: &str, ext: &str) -> Option<u64> {
    name.strip_suffix(ext)?.parse::<u64>().ok()
}

/// Chunk numbers must fit the entry's u16. A chunk is 21,600 slots — six
/// hours since Shelley — so mainnet sits at ~9,100 in 2026 and gains 28 a
/// week; the field overflows in the early 2060s.
pub fn chunk_u16(n: u64) -> Result<u16> {
    u16::try_from(n).with_context(|| format!("chunk {n} exceeds the u16 entry field"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_round_trips() {
        let e = Entry {
            prefix: 0xDEAD_BEEF_0123_4567,
            loc: Location {
                chunk: 3021,
                offset: 87_654_321,
                len: 16_383,
            },
        };
        let mut buf = [0u8; ENTRY_BYTES];
        e.write(&mut buf);
        assert_eq!(Entry::read(&buf), e);
        assert_eq!(Entry::prefix_at(&buf), e.prefix);
    }

    #[test]
    fn prefix_is_big_endian_hash_order() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        a[0] = 0x01;
        b[0] = 0x02;
        assert!(prefix_of(&a) < prefix_of(&b));
        assert_eq!(bucket_of(prefix_of(&b), 24), 0x02_00_00);
    }

    #[test]
    fn headers_round_trip() {
        let s = SegmentHeader {
            era: 7,
            chunk: 42,
            count: 9,
        };
        let mut buf = [0u8; SEGMENT_HEADER_BYTES];
        s.write(&mut buf);
        assert_eq!(SegmentHeader::read(&buf).unwrap(), s);

        let b = BaseHeader::layout(24, 0, 2999, 110_000_000);
        let mut buf = [0u8; BASE_HEADER_BYTES];
        b.write(&mut buf);
        assert_eq!(BaseHeader::read(&buf).unwrap(), b);
        assert_eq!(b.era_off % 64, 0);
        assert_eq!(b.dir_off % 64, 0);
        assert_eq!(b.entries_off % 64, 0);
        assert!(b.dir_off >= b.era_off + 3000);
        assert!(b.entries_off >= b.dir_off + ((1 << 24) + 1) * 4);
    }

    #[test]
    fn era_bytes_round_trip() {
        for era in [
            Era::Byron,
            Era::Shelley,
            Era::Allegra,
            Era::Mary,
            Era::Alonzo,
            Era::Babbage,
            Era::Conway,
        ] {
            assert_eq!(era_from_u8(era_to_u8(era)).unwrap(), era);
        }
    }
}
