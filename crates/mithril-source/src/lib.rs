//! mithril-source — read a Mithril snapshot's immutable files over HTTP ranges.
//!
//! # Why
//!
//! A Mithril "Cardano DB" snapshot is `cardano-node`'s immutable directory
//! verbatim: `NNNNN.chunk` is bare concatenated block CBOR with no separators, and
//! `NNNNN.primary`/`NNNNN.secondary` are the node's own slot→byte-offset index over
//! it. Mithril ships all three, so a mirror of the snapshot is a mirror of the
//! index too.
//!
//! That is what makes a snapshot usable *remotely*. Every other project publishing
//! a snapshot to object storage does it to bootstrap a node: download the archive,
//! unpack, done — sequential, whole-file, once. Reading one as a live indexer needs
//! random access, repeatedly, for years, and the sidecars are what make a block
//! addressable without holding 128 MiB of chunk to find its end.
//!
//! # Shape
//!
//! - [`source`] — where bytes come from. [`RangeSource`] is one method pair, with
//!   [`LocalDir`] and [`HttpMirror`] behind it, and a batched [`read_ranges`] for
//!   many spans at once.
//! - [`sidecar`] — `.primary`/`.secondary` → the entry table: each block's slot,
//!   byte offset, header span, block hash and CRC32.
//! - [`multipart`] — the `multipart/byteranges` body Cloudflare answers 2–300
//!   ranges with.
//! - [`chunk`] — [`Chunk`]: a streaming block walk, a single-span read, a batched
//!   many-span read, and a windowed `Read + Seek` handle.
//!
//! Nothing here decodes a block. It addresses bytes; the consumer decides what they
//! mean — which is what lets the same reader serve `tx-index`, a book builder, and
//! a Worker without any of them being special-cased.
//!
//! # The readers, and what each costs
//!
//! | want | call | requests |
//! |---|---|---|
//! | every block in order | [`Chunk::blocks`] | one per 4 MiB window |
//! | the bytes at one location | [`Chunk::read_span`] | one |
//! | many scattered locations | [`Chunk::read_spans`] | one per ≤299 spans |
//! | a file-like handle | [`Chunk::file`] | one per window, one per seek |
//!
//! RSS is bounded by the window, not the chunk.
//!
//! [`read_ranges`]: RangeSource::read_ranges
//!
//! # Not built yet
//!
//! - **A Durable Object / Worker source.** [`RangeSource`] is object-safe and
//!   `HttpMirror` is the only network impl, so a Worker-side source is a new impl
//!   and nothing else.
//! - **Readahead for a batched caller.** [`Chunk::read_spans`] fetches exactly what
//!   it is asked for; a caller that would take a whole 1 MiB cache line anyway
//!   (which is what the edge fetches from R2) gets no benefit from asking for 2 KiB.

pub mod chunk;
pub mod multipart;
pub mod sidecar;
pub mod source;

pub use chunk::{Block, Blocks, Chunk, ChunkFile, Span};
pub use multipart::Part;
pub use sidecar::{Entry, Primary, SECONDARY_ENTRY_LEN, parse_primary, read_entries};
pub use source::{HttpMirror, LocalDir, MAX_BATCH_BYTES, MAX_BATCH_RANGES, MAX_RANGE, RangeSource};
