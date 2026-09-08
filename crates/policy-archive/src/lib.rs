//! The sealed Parquet archive of a policy's movements — writer and reader.
//!
//! One file is one **level-1 partition**: every attributed movement of one
//! policy inside a covered slot range, one row per `(transaction, unit,
//! party)`, in slot order. The design it implements is
//! `docs/design/POLICY_ARCHIVE_AND_SCALE.md` (cnft.dev-workers); the choices
//! that matter to a reader are these.
//!
//! # The footer is the density tier
//!
//! Row groups are **time-aligned** — nominally one calendar day, with quiet
//! days merged up to a row-count floor and busy days split at a cap, always on
//! a transaction boundary (see [`groups`]). Parquet already records per
//! row-group `num_rows` and per-column min/max statistics, so a reader that
//! fetches only the footer knows, for every group, its slot range and how many
//! movements it holds. What the statistics cannot say — transactions, mints
//! and burns per group — the writer appends to the footer's key-value metadata
//! as small side tables. Together that is a daily histogram of the policy's
//! whole life, from one range request and no decoded data pages
//! ([`density`]).
//!
//! # The stamp travels WITH the file
//!
//! Which policy, which slot range this file covers, how far the ledger that
//! produced it had walked, and whether that walk was complete: all in the
//! footer ([`schema::Stamp`]). A partition that cannot say whether it is a
//! window or an archive is a liability, and a manifest that says it for the
//! file lives in the database this design intends to delete.
//!
//! # Point lookups without a database
//!
//! `tx_hash` carries a split-block bloom filter per row group. A lookup by hash
//! reads the footer, then each group's filter (a few KB), then only the groups
//! that might hold it ([`reader::Archive::candidate_groups`]).
//!
//! # No arrow, no async, no I/O
//!
//! The crate is pure over byte ranges: a caller fetches what
//! [`reader`] asks for and hands the bytes back. That is what lets the same
//! code run on cardano-infra against a file, in a Worker against R2, and in a
//! browser bundle — and what keeps the wasm cost at the 0.15 MB the format
//! doc measured rather than the 2.5 MB the convenient API costs.

pub mod bundle;
pub mod density;
pub mod feed;
pub mod graph;
pub mod groups;
pub mod manifest;
pub mod multi;
/// What a script output HELD, decoded or not — the tier that lets a decoder
/// added later re-derive its history from the archive instead of from chunks.
pub mod observation;
/// Price, as far as ONE policy's archive can honestly take it — and a name for
/// everything it cannot.
pub mod price;
/// What a policy's units ARE — and therefore what is worth recording about it.
pub mod profile;
pub mod reader;
/// Movements → TRADES: folding a swap's two or three transactions back into
/// the one thing a person did.
pub mod trade;
pub mod schema;
pub mod writer;

pub use bundle::{BUNDLE, BUNDLE_FORMAT, Bundle, BundledFooter};
pub use density::DensityBucket;
pub use feed::{FeedRow, PartyMove, UnitMove, fold_rows};
pub use graph::{
    Edge, GRAPH, GRAPH_FORMAT, Movement as PartyMovement, MovementGraph, Moves, NOBODY, PartyKey,
};
pub use groups::{BucketSummary, GroupPolicy, GroupSummary};
pub use manifest::{FileEntry, FileKind, Manifest, PassEntry, RangeKind, SlotRange, merge_ranges};
pub use multi::{FetchedFooter, Got, MultiArchive, Want, fetch_footer};
pub use observation::{
    Decoded, OBSERVATIONS, OBSERVATION_FORMAT, Observation, ObservationWriter, read_all,
};
pub use price::{PairDepth, Spot, Unit, price_slots, spot_at};
pub use profile::{Class, Profile};
pub use reader::{Archive, SparseBytes};
pub use schema::{Completeness, Movement, Stamp};
pub use writer::{ArchiveWriter, Written};

/// Everything that can go wrong on either side of the file.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("parquet: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("rows must arrive in slot order: saw block_time {got} after {had}")]
    OutOfOrder { had: u64, got: u64 },
    #[error("the footer has no `{0}` entry — not a policy archive, or an older one")]
    MissingStamp(&'static str),
    #[error("footer entry `{key}` is malformed: {detail}")]
    BadStamp { key: &'static str, detail: String },
    #[error("footer side tables describe {listed} groups but the file has {actual}")]
    GroupCountMismatch { listed: usize, actual: usize },
    #[error("bytes {start}..{end} have not been fetched")]
    NotFetched { start: u64, end: u64 },
    #[error("the file is shorter than a parquet footer ({0} bytes)")]
    TooShort(u64),
    /// The caller's transport failed. Wrapped as a string so this crate
    /// stays free of any transport's error type.
    #[error("fetch: {0}")]
    Fetch(String),
    /// A bundle that does not decode, or carries a manifest that does not.
    #[error("bundle: {0}")]
    Bundle(String),
}

pub type Result<T> = std::result::Result<T, Error>;
