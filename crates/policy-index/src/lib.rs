//! policy-index — `(policy, asset)` → the mint that created it.
//!
//! # Why, when tx-index already exists
//!
//! `tx-index` answers "where is transaction H" in one directory read. It
//! cannot answer "which transaction first minted policy P", and no amount of
//! speed changes that: its 24-byte entry is keyed by the first 8 bytes of a
//! transaction hash and **carries no policy dimension at all**. The two are
//! different questions and only one of them is indexed.
//!
//! So `token-ledger`'s floor probe goes to Koios, `project-ledger` reconciles
//! its walk against a paginated Koios asset list, and "what is this asset"
//! leaves the box entirely — on a pipeline whose whole premise is that
//! certified history is already here.
//!
//! # The shared pass is the design
//!
//! MEASURED 2026-09-09: harvesting mints costs **+1.8%** on top of the
//! extraction pass `tx-index` already runs (6.6 GB, 2.74M transactions,
//! 687,914 mint events), and the cost does not grow with mint density — the
//! NFT-boom sample carries 15× the mints per byte for the same 1.3%. That is
//! the consequence of the body being **already decoded**: reading
//! `transaction_bodies[i].mint` is a field access, not a parse.
//!
//! ⚠️ **A second independent pass would cost +100%, not +1.8%.** So this crate
//! deliberately owns no I/O: it never opens a chunk, never splits CBOR and
//! never decides what a block is. It is pure over blocks the caller already
//! decoded, and the caller is `tx-index`'s extractor. Give this crate its own
//! walk and the entire cost argument evaporates.
//!
//! # Layout
//!
//! ```text
//! <index-dir>/
//!   segments/NNNNN.seg        ← tx-index's, untouched
//!   base.idx                  ← tx-index's, untouched
//!   policy/
//!     segments/NNNNN.pseg     ← one per chunk, append-only
//!     base.pidx               ← derived: directory + records + time perm + policies
//! ```
//!
//! # What it holds
//!
//! One 32-byte [`format::Record`] per mint or burn event, sorted by
//! `(policy, asset, chunk)`; a `u32` permutation giving the same records in
//! `(chunk, asset)` order within each policy; and a 24-byte
//! [`format::PolicyRun`] per policy carrying its run bounds and **first
//! chunk** — so a floor probe is one binary search over a resident table with
//! no chunk I/O at all.
//!
//! Each record carries the minting transaction's auxiliary-data span, which is
//! where CIP-25 metadata lives. Asset → mint transaction → its metadata is
//! then a single `pread`.
//!
//! # ⚠️ Mints only
//!
//! This indexes mints and burns. It does **not** index occurrences —
//! "every transaction that touched policy P" is bounded by every transfer ever
//! ($SNEK alone is 3,304,907 transactions), and it already exists as the
//! policy archive, which is incremental and carries interpretation. A mint
//! index makes that archive's job smaller by handing it a floor and an asset
//! list. An occurrence index would duplicate its reason to exist.
//!
//! Design: `cnft.dev-workers/docs/design/POLICY_INDEX.md`.

pub mod extract;
pub mod format;
pub mod segment;

pub use extract::{Mints, TxSpans};
pub use format::{
    BaseHeader, DIR_BITS, PolicyRun, RECORD_BYTES, Record, SegmentHeader, bucket_of, name_prefix,
    policy_prefix,
};
pub use segment::{Segment, list_segments, segment_path, segments_dir, write_segment};
