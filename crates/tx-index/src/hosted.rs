//! The index as *hosted*: one immutable base per build, and a pointer naming the build a
//! reader should read.
//!
//! # Why the base is not at a fixed path
//!
//! The base is rebuilt whole whenever the chunk store grows — it covers exactly the chunks
//! that have sealed — so its contents change, and its *offsets* change with them. A reader
//! resolves a hash in three separate ranged reads (header, fence pair, entries), so a fixed
//! path would let a cached header from one build be paired with a fresh fence from the next.
//! The two builds are not interchangeable and the answer would be quietly wrong.
//!
//! So the bytes live at a build-addressed path and a tiny pointer names the current one. The
//! pointer is the only mutable object, replacing it is atomic, and a reader that has resolved
//! it is reading one build for the whole request. `MITOS_AT_THE_EDGE.md` §4.6 makes the same
//! argument for the book.
//!
//! # Why the build id carries a digest
//!
//! The chunk range alone is not enough. The format has already widened once (v1 → v2), and a
//! rebuild of the *same* coverage with fixed code would take the same path — where the
//! publisher's `--ignore-existing` would then skip the upload and leave the old bytes served
//! under a name that claims to be the new build. A content-derived id makes skipping safe,
//! which is the whole point of the flag.
//!
//! Nothing here reads a file: this module is `&[u8]` and plain data, so a Worker and a native
//! publisher share one shape. It is ungated by the `mmap` feature for the same reason.

use serde::{Deserialize, Serialize};

use crate::format::{BaseHeader, FORMAT_VERSION};

/// The file the base is written to. Must equal `crate::base::BASE_FILE` — a test asserts it,
/// because this module is compiled where that one is not.
pub const BASE_FILE: &str = "base.idx";

/// The prefix under the bucket that already holds the chunk mirror. The hosted layout's
/// version rides in it (`v1`); the *entry* format is [`FORMAT_VERSION`] and travels in the
/// base's own header and in [`Pointer::format`].
pub const DEFAULT_PREFIX: &str = "index/tx/v1";

/// The pointer's key, relative to the prefix.
pub const POINTER_FILE: &str = "current";

/// Where the base for `build` lives, relative to the bucket root.
pub fn base_key(prefix: &str, build: &str) -> String {
    format!("{prefix}/{build}/{BASE_FILE}")
}

/// Where a chunk file lives under the mirror — for a reader that has located a
/// body and needs to fetch it.
///
/// The mirror is the node's immutable directory verbatim, so the name is the
/// node's own: five decimal digits, then `.chunk`. **The padding is
/// load-bearing.** `09161.chunk` is the object; `9161.chunk` is not a shorter
/// way to the same bytes, it is a different key that is not there.
pub fn chunk_key(prefix: &str, chunk: u16) -> String {
    format!("{prefix}/{chunk:05}.chunk")
}

/// The build id: the coverage, and enough of the content's digest to tell a rebuild of that
/// same coverage apart from the build it is replacing.
pub fn build_id(last_chunk: u16, digest: &[u8; 32]) -> String {
    format!("{last_chunk:05}-{}", hex::encode(&digest[..6]))
}

/// The pointer: everything a reader needs before its first read of the base.
///
/// `base_key` is carried rather than derived, so the key shape lives in the data and a reader
/// needs no rule for assembling it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pointer {
    /// The entry format of the base this names. A reader that does not know it must refuse
    /// rather than interpret 24-byte records at whatever width it does know.
    pub format: u8,
    /// [`build_id`] for the base.
    pub build: String,
    /// The base's object key, relative to the bucket root.
    pub base_key: String,
    pub first_chunk: u16,
    pub last_chunk: u16,
    /// Entries in the base — the header's `count`, repeated here so a reader can size its
    /// expectations without a second read.
    pub entries: u64,
    /// The object's length in bytes.
    pub bytes: u64,
}

impl Pointer {
    /// The pointer for a base whose header and object size are known.
    pub fn new(prefix: &str, header: &BaseHeader, digest: &[u8; 32], bytes: u64) -> Pointer {
        let build = build_id(header.last_chunk, digest);
        Pointer {
            format: FORMAT_VERSION,
            base_key: base_key(prefix, &build),
            build,
            first_chunk: header.first_chunk,
            last_chunk: header.last_chunk,
            entries: header.count,
            bytes,
        }
    }

    /// Whether the header this pointer led to describes the same build. The check a reader
    /// wants after its first ranged read, and the one that catches a pointer and a base that
    /// drifted apart.
    pub fn describes(&self, header: &BaseHeader) -> bool {
        header.first_chunk == self.first_chunk
            && header.last_chunk == self.last_chunk
            && header.count == self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(first_chunk: u16, last_chunk: u16, count: u64) -> BaseHeader {
        BaseHeader::layout(24, first_chunk, last_chunk, count)
    }

    #[test]
    fn a_build_id_names_the_coverage_and_the_content() {
        let digest = [0xab; 32];
        assert_eq!(build_id(9201, &digest), "09201-abababababab");
        // The same coverage with different bytes is a different build — which is what makes
        // `--ignore-existing` on the upload safe.
        let mut other = [0xab; 32];
        other[0] = 0xcd;
        assert_ne!(build_id(9201, &digest), build_id(9201, &other));
    }

    #[test]
    fn the_base_key_puts_the_build_under_the_prefix() {
        assert_eq!(
            base_key("index/tx/v1", "09201-abababababab"),
            "index/tx/v1/09201-abababababab/base.idx"
        );
    }

    #[test]
    fn a_chunk_key_is_the_nodes_own_five_digit_name() {
        // `mainnet/09161.chunk` is a real object — fetched over the mirror to
        // confirm the padding, because the unpadded form is the plausible
        // mistake and it would 404 rather than mislead.
        assert_eq!(chunk_key("mainnet", 0), "mainnet/00000.chunk");
        assert_eq!(chunk_key("mainnet", 9161), "mainnet/09161.chunk");
        assert_eq!(chunk_key("mainnet", 9202), "mainnet/09202.chunk");
    }

    #[test]
    fn a_pointer_carries_what_a_reader_needs_before_its_first_read() {
        let digest = [0x01; 32];
        let p = Pointer::new(
            "index/tx/v1",
            &header(0, 9201, 124_012_537),
            &digest,
            3_043_419_096,
        );
        assert_eq!(p.format, FORMAT_VERSION);
        assert_eq!(p.build, "09201-010101010101");
        assert_eq!(p.base_key, "index/tx/v1/09201-010101010101/base.idx");
        assert_eq!(p.first_chunk, 0);
        assert_eq!(p.last_chunk, 9201);
        assert_eq!(p.entries, 124_012_537);
        assert_eq!(p.bytes, 3_043_419_096);
    }

    #[test]
    fn the_pointer_round_trips_through_the_json_a_worker_parses() {
        let p = Pointer::new("index/tx/v1", &header(0, 9201, 7), &[0x02; 32], 99);
        let json = serde_json::to_string(&p).unwrap();
        // The exact keys are the contract with the reader, so assert them rather than only
        // the round trip.
        for key in [
            "\"format\"",
            "\"build\"",
            "\"base_key\"",
            "\"first_chunk\"",
            "\"last_chunk\"",
            "\"entries\"",
            "\"bytes\"",
        ] {
            assert!(json.contains(key), "{key} missing from {json}");
        }
        assert_eq!(serde_json::from_str::<Pointer>(&json).unwrap(), p);
    }

    #[test]
    fn a_pointer_describes_the_header_it_came_from() {
        let h = header(0, 9201, 7);
        let p = Pointer::new("index/tx/v1", &h, &[0x03; 32], 99);
        assert!(p.describes(&h));
        assert!(
            !p.describes(&header(0, 9202, 7)),
            "a different coverage is not this build"
        );
        assert!(
            !p.describes(&header(0, 9201, 8)),
            "a different count is not this build"
        );
    }

    /// The one thing this module cannot see: the builder's own constant.
    #[cfg(feature = "mmap")]
    #[test]
    fn the_hosted_file_name_is_the_one_the_builder_writes() {
        assert_eq!(BASE_FILE, crate::base::BASE_FILE);
    }
}
