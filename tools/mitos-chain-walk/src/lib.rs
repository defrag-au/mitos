//! mitos-chain-walk — the plumbing every Mithril-snapshot walker shares.
//!
//! Extracted from `market-ledger` (2026-08-16) so `project-ledger` could reuse
//! it without becoming a mode of a tool whose contract is a static venue set.
//! Four pieces, all venue-agnostic:
//!
//! - [`mithril`] — shell out to `mithril-client` to download + verify a
//!   certified immutable DB (optionally a partial immutable-file range).
//! - [`decode`] — bare-pallas tx decode into the parts a walker needs
//!   (outputs with datums, inputs with canonical-order redeemers, witness
//!   datums, aux data).
//! - [`checkpoint`] — the crash-visible JSON mirror of a walker's last
//!   committed checkpoint, plus the wipe used by reset paths.
//! - [`chain`] — open the immutable DB as a block iterator (from genesis or
//!   seeked to a point), parse `<slot>:<hash>` points, and mainnet
//!   slot → unix time.
//!
//! What is deliberately NOT here: any store, any registry, any notion of what
//! a "watched" output is. Those are the walker's business.

pub mod chain;
pub mod checkpoint;
pub mod decode;
pub mod mithril;

pub use chain::{open_blocks, parse_point, slot_to_unix};
pub use decode::{Asset, DecodedInput, DecodedOutput, DecodedTx, OutRef, decode_tx};
