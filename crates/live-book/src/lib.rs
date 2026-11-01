//! The live book: the live UTxO set as an immutable artifact, and a reader over
//! it.
//!
//! `docs/design/LIVE_BOOK.md` is the spec. In one paragraph: the live set is
//! **10,911,674 outputs** on mainnet, `rows.bin` is a byte-addressed arena of
//! variable-length rows holding them, and each of three narrow orderings over
//! that one array is a permutation of 12-byte `{u64 prefix, u32 row}` entries
//! under a 2^21-bucket directory. Nothing is keyed by anything but a prefix, and
//! every lookup confirms against the row the entry points at, so a collision
//! costs a read rather than an answer.
//!
//! | ordering | answers | entries on mainnet |
//! |---|---|---|
//! | `by_outref` | is this output live, and what is it | 10,911,674 |
//! | `by_policy` | what holds this policy | 10,946,451 |
//! | `by_addr` | what does this credential hold | 10,911,674 |
//!
//! # What is here and what is not
//!
//! This crate is the **format and its reader**. The builder (a fold's live set,
//! which writes the artifact) and the verifier are still in
//! `tools/fold-probe`; they import [`format`] and [`row`] rather than holding a
//! second copy of the layout, and they move here when the second consumer of
//! *writing* appears — which is what happened to reading, and is why this crate
//! exists at all.
//!
//! # Dependencies
//!
//! `memmap2`, `anyhow`, and `pallas-addresses` for the one arm of the credential
//! derivation that is a CBOR parse ([`address`]). Deliberately no pallas types in
//! the format: a row's outref is a `[u8; 32]`, and a caller holding pallas's
//! `Hash<32>` converts once at its own boundary.
//!
//! # Native and wasm
//!
//! The `mmap` feature (**on by default**) carries the file-backed path:
//! `MmapSource`, every `open(&Path)` constructor, and `DatumTable`. `memmap2`
//! cannot build for `wasm32-unknown-unknown`, so a Cloudflare Worker serving a
//! published book takes this crate with `default-features = false` and gets the
//! parsing without the acquisition — `entry_at`, `bucket_at`, `split`,
//! `Row::parse` and [`address`] are all `&[u8]` in and plain data out. It names
//! its own [`Source`], or drives the reads itself; see [`publish`] for the part
//! arithmetic and `MITOS_AT_THE_EDGE.md` for why a remote reader batches where
//! the local one loops.
//!
//! # Using it
//!
//! ```no_run
//! # fn main() -> anyhow::Result<()> {
//! use std::path::Path;
//!
//! let book = live_book::Base::open(Path::new("/srv/data/fold-book/full3"))?;
//! println!("{} live rows", book.rows());
//!
//! // A point lookup: the row's own outref is what confirms it.
//! # let tx = [0u8; 32];
//! if let Some(row) = book.utxo(&tx, 0)? {
//!     println!("{} lovelace at {} bytes of address", row.lovelace(), row.address().len());
//! }
//!
//! // An enumeration, one page at a time. The cursor is opaque and keyed to the
//! // query, so a replay against another key is refused rather than silent.
//! # let cred = [0x11u8; 28];
//! let mut after = None;
//! loop {
//!     let page = book.by_payment_cred(&cred, after, 1_000)?;
//!     for (tx, index) in &page.refs {
//!         let _ = (tx, index);
//!     }
//!     after = page.next;
//!     if after.is_none() {
//!         break;
//!     }
//! }
//! # Ok(()) }
//! ```

pub mod address;
pub mod format;
pub mod plan;
pub mod publish;
pub mod reader;
pub mod row;
pub mod source;

pub use format::{BaseFingerprint, LAYOUT_VERSION, Meta, PolicyTable};
pub use reader::{Base, Cursor, MAX_PAGE, OutRef, Page};
pub use row::{Asset, Assets, Datum, Row, RowRead};
#[cfg(feature = "mmap")]
pub use source::MmapSource;
pub use source::{DefaultSource, Source};
