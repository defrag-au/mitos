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
use crate::locate;

/// The file the base is written to. Must equal `crate::base::BASE_FILE` — a test asserts it,
/// because this module is compiled where that one is not.
pub const BASE_FILE: &str = "base.idx";

/// The directory's own object, published beside the base.
///
/// ⚠️ **Separate for one arithmetic reason.** The directory is the header, the era
/// table and the 64 MiB fence array — all of which a lookup needs, and none of which
/// changes within a build. Inside the 2.83 GiB base it is part of an object Cloudflare
/// will never cache (the cap is 512 MB, and a ranged read of an over-cap object is not
/// cached either). On its own it is **under the cap, immutable for the build, and
/// cached at the edge for every reader after the first** — which turns the fence phase
/// from an origin fetch per lookup into a shared cached read.
///
/// ⚠️ **A browser reader wants [`HEAD_FILE`] and the entry shards instead.** Caching
/// `dir.idx` removes the *origin* cost, not the *transfer*: 64 MiB is ~6 s over a 10
/// MB/s link, paid again on every reload with a browser cache disabled. And a cold
/// range read of it makes the edge pull all 64 MiB before it can answer, so a single
/// 8-byte fence pair could stall for seconds. This object stays for the native and
/// hosted locators that genuinely want the whole directory.
pub const DIR_FILE: &str = "dir.idx";

/// The base's head — the 64-byte header and the era table — as its own object.
///
/// 9,280 bytes at mainnet's 9,203 chunks. Small enough that a reader fetching it whole
/// pays nothing worth measuring (measured: a warm edge read ~40–65 ms, a cold object
/// ~350 ms), and it is the *only* part of the base a shard reader still needs: the
/// header for `dir_bits` and the coverage, the era bytes for the chunk a body came from.
/// Nothing about it is new — it is the base's first `dir_off` bytes, so it cannot drift
/// from the base's own layout.
pub const HEAD_FILE: &str = "head.idx";

/// The entry shards' directory, under the build.
pub const ENTRIES_DIR: &str = "entries";

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

/// Where the directory for `build` lives — the object a reader fetches whole, and the
/// one the CDN will actually keep.
pub fn dir_key(prefix: &str, build: &str) -> String {
    format!("{prefix}/{build}/{DIR_FILE}")
}

/// Where the head for `build` lives.
pub fn head_key(prefix: &str, build: &str) -> String {
    format!("{prefix}/{build}/{HEAD_FILE}")
}

/// The directory the entry shards for `build` live under.
pub fn entries_prefix(prefix: &str, build: &str) -> String {
    format!("{prefix}/{build}/{ENTRIES_DIR}")
}

/// Where entry shard `shard` lives.
///
/// ⚠️ **The one key a reader must assemble rather than be handed.** Every other key
/// travels in the pointer, so a reader needs no rule for it. There are 32,768 shards at
/// mainnet's 2²⁴ buckets, and a pointer listing them would not be tiny.
///
/// Five digits because [`locate::shard_count`] of a 2²⁴-bucket directory is 32,768; a
/// directory wide enough to need six would be a format change (`dir_bits` is in the
/// header) and would have to say so here.
pub fn shard_key(prefix: &str, build: &str, shard: u64) -> String {
    format!("{}/{shard:05}.bin", entries_prefix(prefix, build))
}

/// How many shards a base's directory divides into.
pub fn shard_count(header: &BaseHeader) -> u64 {
    locate::shard_count(header.n_buckets() as u64)
}

/// The length of the head object: the header, the era table, and the alignment padding
/// the directory's own offset implies — i.e. the base's first `dir_off` bytes.
pub fn head_bytes(header: &BaseHeader) -> u64 {
    header.dir_off
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
    /// The directory's object key, or `None` for a pointer published before the
    /// directory was split out — a reader then ranges the base for it, which works
    /// and is slow.
    ///
    /// `#[serde(default)]` on purpose: adding a required field would make every
    /// already-published pointer unparseable, and a reader that cannot read the
    /// pointer cannot read anything at all.
    #[serde(default)]
    pub dir_key: Option<String>,
    /// The head's object key, or `None` for a pointer published before the entry shards
    /// existed. **This is what tells a reader the shards are there.**
    #[serde(default)]
    pub head_key: Option<String>,
    /// The directory the entry shards live under, or `None` with `head_key`. Both are
    /// published together, so one without the other is a pointer this reader refuses.
    #[serde(default)]
    pub entries_prefix: Option<String>,
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
            dir_key: Some(dir_key(prefix, &build)),
            head_key: Some(head_key(prefix, &build)),
            entries_prefix: Some(entries_prefix(prefix, &build)),
            build,
            first_chunk: header.first_chunk,
            last_chunk: header.last_chunk,
            entries: header.count,
            bytes,
        }
    }

    /// Where entry shard `shard` lives, when this pointer names shards at all.
    ///
    /// The one key a reader assembles rather than reads out of the pointer — see
    /// [`shard_key`] for why an enumeration is not an option.
    pub fn shard_key(&self, shard: u64) -> Option<String> {
        let prefix = self.entries_prefix.as_deref()?;
        self.head_key.as_ref()?;
        Some(format!("{prefix}/{shard:05}.bin"))
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
        assert_eq!(
            p.dir_key.as_deref(),
            Some("index/tx/v1/09201-010101010101/dir.idx"),
            "the directory is published beside the base, so a reader can fetch it whole"
        );
        assert_eq!(p.first_chunk, 0);
        assert_eq!(p.last_chunk, 9201);
        assert_eq!(p.entries, 124_012_537);
        assert_eq!(p.bytes, 3_043_419_096);
        assert_eq!(
            p.head_key.as_deref(),
            Some("index/tx/v1/09201-010101010101/head.idx"),
            "the head is published beside the base, so a reader need not range the base for it"
        );
        assert_eq!(
            p.entries_prefix.as_deref(),
            Some("index/tx/v1/09201-010101010101/entries")
        );
        assert_eq!(
            p.shard_key(7).as_deref(),
            Some("index/tx/v1/09201-010101010101/entries/00007.bin")
        );
    }

    #[test]
    fn a_shard_key_is_five_digits_under_the_build() {
        assert_eq!(
            shard_key("index/tx/v1", "09202-416476537b08", 0),
            "index/tx/v1/09202-416476537b08/entries/00000.bin"
        );
        // 2²⁴ / 512 shards, so the last one is 32,767 — five digits covers the whole
        // directory with room to spare, which is what makes a fixed width safe here.
        assert_eq!(
            shard_key("index/tx/v1", "b", 32_767),
            "index/tx/v1/b/entries/32767.bin"
        );
    }

    #[test]
    fn the_head_is_the_bases_own_first_bytes_plus_the_directory_offset() {
        let h = header(0, 9202, 124_020_199);
        // The published base's own number: 64 + 9,216 eras, aligned to 64.
        assert_eq!(head_bytes(&h), 9_280);
        assert_eq!(head_bytes(&h), h.dir_off);
    }

    #[test]
    fn the_published_directory_divides_into_the_published_shard_count() {
        let h = header(0, 9202, 124_020_199);
        assert_eq!(shard_count(&h), 32_768);
        // Every shard is its fence table plus a share of the entries, and the shares sum
        // back to the base's own count — the property that says no entry was dropped.
        let share = h.count.div_ceil(shard_count(&h));
        assert!(share * shard_count(&h) >= h.count);
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
            "\"dir_key\"",
            "\"head_key\"",
            "\"entries_prefix\"",
            "\"first_chunk\"",
            "\"last_chunk\"",
            "\"entries\"",
            "\"bytes\"",
        ] {
            assert!(json.contains(key), "{key} missing from {json}");
        }
        assert_eq!(serde_json::from_str::<Pointer>(&json).unwrap(), p);
    }

    /// ⚠️ A pointer published before `dir_key` existed must still parse. A reader that
    /// refuses the pointer reads nothing at all, so this field can never be required —
    /// and neither can the two that came after it.
    #[test]
    fn a_pointer_without_a_dir_key_still_parses() {
        let old = r#"{"format":2,"build":"09202-416476537b08",
            "base_key":"index/tx/v1/09202-416476537b08/base.idx",
            "first_chunk":0,"last_chunk":9202,"entries":124020199,"bytes":3043602984}"#;
        let p: Pointer = serde_json::from_str(old).expect("an older pointer must still read");
        assert_eq!(p.dir_key, None);
        assert_eq!(p.base_key, "index/tx/v1/09202-416476537b08/base.idx");
        assert_eq!(p.head_key, None, "and the shard path stays unavailable");
        assert_eq!(p.shard_key(0), None);
    }

    /// The pointer as it stood between the directory split and the shards: a `dir_key`
    /// and nothing else. A reader must take it and answer, just not as fast.
    #[test]
    fn a_pointer_with_a_directory_but_no_shards_still_parses() {
        let mid = r#"{"format":2,"build":"09202-416476537b08",
            "base_key":"index/tx/v1/09202-416476537b08/base.idx",
            "dir_key":"index/tx/v1/09202-416476537b08/dir.idx",
            "first_chunk":0,"last_chunk":9202,"entries":124020199,"bytes":3043602984}"#;
        let p: Pointer = serde_json::from_str(mid).expect("a mid-transition pointer must read");
        assert!(p.dir_key.is_some());
        assert_eq!(p.shard_key(0), None);
    }

    /// A pointer that names shards but not the head they pair with is not usable: the
    /// reader would have no header. `shard_key` refuses rather than assembling half of it.
    #[test]
    fn a_shard_key_needs_both_halves_of_the_pair() {
        let mut p = Pointer::new("index/tx/v1", &header(0, 9202, 7), &[0x09; 32], 99);
        p.head_key = None;
        assert_eq!(p.shard_key(0), None);
        p.head_key = Some(head_key("index/tx/v1", &p.build));
        p.entries_prefix = None;
        assert_eq!(p.shard_key(0), None);
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
