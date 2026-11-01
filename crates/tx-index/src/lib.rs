//! tx-index — tx hash → (chunk, body offset) over a Mithril immutable DB.
//!
//! # Why
//!
//! A Mithril "Cardano DB" snapshot is the node's immutable directory
//! verbatim: chunk files of concatenated block CBOR, with sidecar indices
//! keyed by SLOT. Nothing maps a tx hash to anything — a hash is computed
//! from the body bytes and never stored — so resolving a transaction input
//! (tx hash + output index → the producing output) means either carrying a
//! UTxO buffer through a forward walk or decoding and hashing every body in
//! a band of chunks until the wanted one turns up. Every walker in this
//! workspace does one or the other.
//!
//! This crate does the decode+hash exactly once per completed chunk and
//! keeps the answer.
//!
//! # Shape
//!
//! Two layers, deliberately separate:
//!
//! - **Extraction** (`segments/NNNNN.seg`) — one file per completed chunk,
//!   entries sorted by hash prefix. Chunk files are immutable once complete
//!   (the node only appends; Mithril signs per-file digests that never
//!   change), so a segment is written once and never rewritten. New chunks
//!   after a snapshot refresh mean new segments, nothing else.
//!
//! - **Lookup** (`base.idx`) — a derived, rebuildable cache over all
//!   segments: a 2^24-bucket prefix directory in front of one globally sorted
//!   entry array. Tx hashes are uniform, so a random lookup is one directory
//!   read and a scan of ~7 entries. Rebuilding is a counting sort of ~16-byte
//!   records, seconds to a minute; extraction is the part that costs.
//!
//! Segments newer than the base (a refresh landed, compaction hasn't) are
//! consulted first, newest first — inputs skew young, so that order is also
//! the cheap one.
//!
//! An entry stores only the first 8 bytes of the hash. The read side fetches
//! the located body, re-hashes it and compares to the FULL requested hash, so
//! a prefix collision costs one wasted read and never a wrong answer.
//!
//! # Native and wasm
//!
//! The `mmap` feature (**on by default**) carries everything that touches a
//! filesystem: the chunk extractor, the per-chunk segments, the compacted
//! [`base`], and the reader that maps them. `memmap2` cannot build for
//! `wasm32-unknown-unknown` and [`reader`] reaches for `std::os::unix`, so a
//! Cloudflare Worker takes this crate with `default-features = false` and gets
//! the half a locator needs — [`format`] (the 64-byte base header, the fence
//! directory, the 24-byte entry) and [`decode`], both `&[u8]` in and plain data
//! out. It drives the reads itself; `MITOS_AT_THE_EDGE.md` §7.6 is what it is
//! reading them *for*.

// The parsing half: `&[u8]` in, plain data out. Built for wasm.
pub mod decode;
pub mod format;
pub mod hosted;
pub mod locate;
pub mod wire;

// The storage half: chunk files, segments, a mapped base. Native only.
#[cfg(feature = "mmap")]
pub mod base;
#[cfg(feature = "mmap")]
pub mod compact;
#[cfg(feature = "mmap")]
pub mod extract;
#[cfg(feature = "mmap")]
pub mod reader;
#[cfg(feature = "mmap")]
pub mod segment;

pub use format::{AuxSpan, Entry, Location, prefix_of};
pub use locate::Span;
#[cfg(feature = "mmap")]
pub use reader::{AuxLookup, Coverage, Index, IndexHandle, Located, Resolution, TxBody};

/// The tx hash of a body: blake2b-256 over its bytes. The one computation
/// extraction, verification and the read side must all agree on.
pub fn tx_hash(body: &[u8]) -> [u8; 32] {
    let h = pallas_crypto::hash::Hasher::<256>::hash(body);
    let mut out = [0u8; 32];
    out.copy_from_slice(h.as_ref());
    out
}
