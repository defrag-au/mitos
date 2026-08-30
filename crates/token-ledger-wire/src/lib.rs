//! Wire format for `token-ledger export` — the artifacts a frontend scrubs.
//!
//! Two tiers, because the projections split cleanly by what they need and that
//! split, not the row count, is the right seam:
//!
//! - [`Spine`] — the reserve curve plus cohort checkpoints. Drives every
//!   always-on face (cap band, supply cascade, composition, holder count) and
//!   stays small at any token size, so the time-scrub is interactive the
//!   moment it loads.
//! - [`Detail`] — the movement columns and the party dictionary. Drives flows
//!   and per-wallet drill-down, and scales with the log. Streams in behind the
//!   spine.
//! - [`TxIds`] — transaction hashes, indexed by ordinal. Needed only to open a
//!   specific transaction, so it loads on click and never during a scrub.
//!
//! Splitting [`TxIds`] out was a measured decision, not a guess. On $Aliens the
//! hashes were **213,440 of the detail page's 371,615 bytes — 57%** — and gzip
//! barely touched them, because a hash is incompressible by construction. The
//! original reasoning had been that side-tabling hashes behind an ordinal would
//! pay for itself by removing repetition across movements sharing one
//! transaction; measurement showed that for this token movements and
//! transactions are near 1:1 (6,609 vs 6,670), so there was no repetition to
//! remove and 32 flat bytes per transaction simply dominated. The fix is to put
//! them where a scrub never has to load them.
//!
//! A fourth tier — one wallet's full history — remains a `serve` request, not a
//! file.
//!
//! # Why columnar
//!
//! `market-ledger-wire` encodes `Vec<EventRow>`, which is right for a paged
//! event feed and wrong here. This access pattern is columnar — "all slots",
//! "sum amounts by cohort over a range" — and a `Vec<Row>` forces decoding
//! every field of every row into allocated structs before any of that. Sorted
//! slot columns are also monotonic, so [`delta_encode`] turns them into small
//! varints; row-major interleaving destroys that. And a scrub only touches the
//! slot column to binary-search the playhead, leaving the rest cold.
//!
//! # Encoding contract
//!
//! Postcard is positional — there are no field names or tags on the wire, so
//! the struct definitions here ARE the format:
//!
//! - never reorder, remove, or insert fields; never change a field's type or
//!   `Option`-ness;
//! - no `#[serde(skip_serializing_if)]` / `default` — every field is always
//!   present;
//! - enum variants are append-only (discriminant = declaration order);
//! - the parallel columns of a table must stay the same length — a reordered
//!   or truncated column is a silent reinterpretation, not a decode failure,
//!   which is why [`Spine::validate`] and [`Detail::validate`] exist and why
//!   the decoders call them.
//!
//! Any change bumps [`WIRE_VERSION`] and adds V2 types alongside the V1 ones.
//! `version` is the first field of both pages and a plain `u8`, so it encodes
//! as byte 0 of every payload — clients peek it before decoding the rest and
//! fail loudly instead of decoding garbage.

use serde::{Deserialize, Serialize};

pub mod projections;

/// Version byte at offset 0 of every encoded page. Bump on ANY change to the
/// types in this crate (see the module docs for what counts).
pub const WIRE_VERSION: u8 = 1;

/// What went wrong decoding a page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// Byte 0 did not match [`WIRE_VERSION`].
    Version { expected: u8, found: u8 },
    /// Postcard could not decode the payload.
    Malformed,
    /// Decoded, but internally inconsistent — parallel columns of differing
    /// length, or an index pointing outside its dictionary. Reported rather
    /// than tolerated: a frontend that silently renders a truncated column
    /// shows a wrong number with no error.
    Inconsistent(&'static str),
}

impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WireError::Version { expected, found } => {
                write!(
                    f,
                    "wire version mismatch: expected {expected}, found {found}"
                )
            }
            WireError::Malformed => write!(f, "malformed payload"),
            WireError::Inconsistent(what) => write!(f, "inconsistent page: {what}"),
        }
    }
}

impl std::error::Error for WireError {}

/// The watched asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetId {
    pub policy: [u8; 28],
    pub asset_name: Vec<u8>,
    /// Display decimals. Identity is the hex name; this is presentation only,
    /// and the token registry — not the chain — is authoritative for it.
    pub decimals: u8,
}

/// A pool on the reserve curve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolMeta {
    pub dex: String,
    /// Pool-instance key (LP policy + name) where known.
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
    /// How firmly the instance is identified: `datum` / `value` / `ambiguous`
    /// / `unknown`. Carried so the UI can render a pool it cannot fully name
    /// differently from one it can, rather than flattening the two.
    pub key_basis: String,
    /// `None` when no decoder supplied it — the realisable figure is then
    /// optimistic by this pool's fee, and the UI should say so.
    pub fee_bps: Option<u32>,
}

/// A party in the detail dictionary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyMeta {
    pub address: String,
    pub stake: Option<String>,
    /// Index into [`Spine::cohorts`].
    pub cohort: u16,
    /// How firmly the cohort is known: `proven` / `decoded` / `registered` /
    /// `chain`. The whole point of separating this from the cohort is that a
    /// provably-unspendable script and a wallet somebody named are not the
    /// same kind of claim.
    pub basis: String,
}

/// Cohort totals at a point in the log, computed offline over the complete
/// history.
///
/// **Exact, not a summary.** A total read here is the true total at that
/// transaction — the checkpoint stride bounds *resolution*, never correctness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spine {
    pub version: u8,
    pub asset: AssetId,
    /// First and last slot covered.
    pub domain: (u64, u64),
    /// Block time of the last transaction — the ledger's own as-of date.
    /// Maturity and "now" are judged against this, never wall clock.
    pub last_block_time: u64,
    /// Total minted less protocol burns.
    pub nominal_supply: i64,
    /// Cohort names, interned. Indices into this are used by `cp_totals` and
    /// [`PartyMeta::cohort`].
    pub cohorts: Vec<String>,
    /// Transactions between checkpoints. Bounds both the frontend's replay
    /// cost *and* the resolution of a spine-only rendering, which is the
    /// trade-off to weigh when choosing it.
    pub checkpoint_stride: u32,
    /// Checkpoint slots, delta-encoded — see [`delta_encode`].
    pub cp_slots: Vec<u64>,
    /// Transaction ordinal each checkpoint sits at, so a frontend knows where
    /// to resume replaying detail from.
    pub cp_tx_ord: Vec<u32>,
    /// Holders with a non-zero balance at each checkpoint.
    pub cp_holders: Vec<u32>,
    /// `cohorts.len()` totals per checkpoint, row-major.
    pub cp_totals: Vec<i64>,
    /// Vesting supply past its unlock at each checkpoint — claimable, and so
    /// part of the sellable floor.
    ///
    /// Carried here rather than derived by the consumer because maturity needs
    /// decoded lock schedules *and* the lock's whole lifetime, neither of which
    /// is in the movement columns. Without it a consumer must treat all vesting
    /// as locked and understates what could hit the market.
    pub cp_vest_matured: Vec<i64>,
    /// Vesting supply still before its unlock at each checkpoint. Cannot move.
    ///
    /// A lock whose datum did not decode is counted **here**, never as matured:
    /// a lock we cannot read is not an absent lock.
    pub cp_vest_locked: Vec<i64>,
    pub pools: Vec<PoolMeta>,
    /// Reserve-curve slots, delta-encoded.
    pub rc_slots: Vec<u64>,
    /// Index into `pools` for each reserve point.
    pub rc_pool: Vec<u16>,
    /// Watched-asset reserve at each point.
    pub rc_base: Vec<i64>,
    /// Lovelace reserve at each point.
    pub rc_quote: Vec<i64>,
}

impl Spine {
    pub fn checkpoint_count(&self) -> usize {
        self.cp_slots.len()
    }

    pub fn reserve_point_count(&self) -> usize {
        self.rc_slots.len()
    }

    /// Cohort totals at checkpoint `i`.
    pub fn checkpoint_totals(&self, i: usize) -> Option<&[i64]> {
        let n = self.cohorts.len();
        self.cp_totals.get(i * n..(i + 1) * n)
    }

    pub fn validate(&self) -> Result<(), WireError> {
        let cps = self.cp_slots.len();
        if self.cp_tx_ord.len() != cps
            || self.cp_holders.len() != cps
            || self.cp_vest_matured.len() != cps
            || self.cp_vest_locked.len() != cps
        {
            return Err(WireError::Inconsistent(
                "checkpoint columns differ in length",
            ));
        }
        if self.cp_totals.len() != cps * self.cohorts.len() {
            return Err(WireError::Inconsistent(
                "cp_totals is not cohorts x checkpoints",
            ));
        }
        let rcs = self.rc_slots.len();
        if self.rc_pool.len() != rcs || self.rc_base.len() != rcs || self.rc_quote.len() != rcs {
            return Err(WireError::Inconsistent(
                "reserve-curve columns differ in length",
            ));
        }
        if self.rc_pool.iter().any(|p| *p as usize >= self.pools.len()) {
            return Err(WireError::Inconsistent(
                "reserve point names an unknown pool",
            ));
        }
        Ok(())
    }
}

/// Movement columns plus the dictionaries they index into.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Detail {
    pub version: u8,
    pub parties: Vec<PartyMeta>,
    /// Slot per transaction ordinal, delta-encoded.
    pub tx_slots: Vec<u64>,
    /// Net mint per transaction ordinal (0 / +mint / −burn).
    pub tx_net_mint: Vec<i64>,
    /// Movement columns. `mv_tx` indexes `tx_hashes`/`tx_slots`; `mv_party`
    /// indexes `parties`.
    pub mv_tx: Vec<u32>,
    pub mv_party: Vec<u32>,
    /// Signed balance change. **Not** a directed pair: a `(from, to)` edge
    /// cannot be derived from a multi-party transaction without a heuristic,
    /// and the obvious one is wrong exactly where the interesting activity is.
    pub mv_amount: Vec<i64>,
}

/// Transaction hashes, indexed by ordinal — the click-through tier.
///
/// Kept out of [`Detail`] because a scrub never needs them and they are the
/// single largest, least compressible thing the export produces. See the
/// module header for the measurement that moved them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxIds {
    pub version: u8,
    /// Parallel to [`Detail::tx_slots`] — ordinal `i` in one is ordinal `i` in
    /// the other. A consumer holding both must check the lengths agree; they
    /// are separate files and nothing else can enforce it.
    pub hashes: Vec<[u8; 32]>,
}

impl Detail {
    pub fn movement_count(&self) -> usize {
        self.mv_tx.len()
    }

    pub fn tx_count(&self) -> usize {
        self.tx_slots.len()
    }

    pub fn validate(&self) -> Result<(), WireError> {
        let txs = self.tx_slots.len();
        if self.tx_net_mint.len() != txs {
            return Err(WireError::Inconsistent(
                "transaction columns differ in length",
            ));
        }
        let mvs = self.mv_tx.len();
        if self.mv_party.len() != mvs || self.mv_amount.len() != mvs {
            return Err(WireError::Inconsistent("movement columns differ in length"));
        }
        if self.mv_tx.iter().any(|t| *t as usize >= txs) {
            return Err(WireError::Inconsistent(
                "movement names an unknown transaction",
            ));
        }
        if self
            .mv_party
            .iter()
            .any(|p| *p as usize >= self.parties.len())
        {
            return Err(WireError::Inconsistent("movement names an unknown party"));
        }
        Ok(())
    }
}

/// Delta-encode a monotonically non-decreasing column.
///
/// Element 0 is absolute; the rest are gaps from their predecessor. Postcard
/// varint-encodes `u64`, so a column of large slot numbers with small gaps
/// collapses from 8 bytes an entry to one or two.
///
/// Panics are avoided rather than checked: a non-monotonic input would
/// underflow, so this saturates instead, and [`delta_decode`] will then round-
/// trip to something wrong. Callers hold sorted columns by construction — the
/// ledger is written in chain order.
pub fn delta_encode(values: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(values.len());
    let mut prev = 0u64;
    for (i, v) in values.iter().enumerate() {
        out.push(if i == 0 { *v } else { v.saturating_sub(prev) });
        prev = *v;
    }
    out
}

/// Inverse of [`delta_encode`].
pub fn delta_decode(deltas: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(deltas.len());
    let mut acc = 0u64;
    for (i, d) in deltas.iter().enumerate() {
        acc = if i == 0 { *d } else { acc.saturating_add(*d) };
        out.push(acc);
    }
    out
}

fn check_version(bytes: &[u8]) -> Result<(), WireError> {
    match bytes.first() {
        Some(&v) if v == WIRE_VERSION => Ok(()),
        Some(&v) => Err(WireError::Version {
            expected: WIRE_VERSION,
            found: v,
        }),
        None => Err(WireError::Malformed),
    }
}

pub fn encode_spine(spine: &Spine) -> Result<Vec<u8>, WireError> {
    spine.validate()?;
    postcard::to_stdvec(spine).map_err(|_| WireError::Malformed)
}

pub fn decode_spine(bytes: &[u8]) -> Result<Spine, WireError> {
    check_version(bytes)?;
    let spine: Spine = postcard::from_bytes(bytes).map_err(|_| WireError::Malformed)?;
    spine.validate()?;
    Ok(spine)
}

pub fn encode_detail(detail: &Detail) -> Result<Vec<u8>, WireError> {
    detail.validate()?;
    postcard::to_stdvec(detail).map_err(|_| WireError::Malformed)
}

pub fn decode_detail(bytes: &[u8]) -> Result<Detail, WireError> {
    check_version(bytes)?;
    let detail: Detail = postcard::from_bytes(bytes).map_err(|_| WireError::Malformed)?;
    detail.validate()?;
    Ok(detail)
}

pub fn encode_tx_ids(ids: &TxIds) -> Result<Vec<u8>, WireError> {
    postcard::to_stdvec(ids).map_err(|_| WireError::Malformed)
}

pub fn decode_tx_ids(bytes: &[u8]) -> Result<TxIds, WireError> {
    check_version(bytes)?;
    postcard::from_bytes(bytes).map_err(|_| WireError::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spine() -> Spine {
        Spine {
            version: WIRE_VERSION,
            asset: AssetId {
                policy: [7u8; 28],
                asset_name: b"TOK".to_vec(),
                decimals: 0,
            },
            domain: (100, 900),
            last_block_time: 1_700_000_000,
            nominal_supply: 1_000,
            cohorts: vec!["burn".into(), "wallet".into()],
            checkpoint_stride: 64,
            cp_slots: delta_encode(&[100, 500, 900]),
            cp_tx_ord: vec![0, 64, 128],
            cp_holders: vec![1, 5, 9],
            cp_totals: vec![0, 1000, 10, 990, 20, 980],
            cp_vest_matured: vec![0, 0, 0],
            cp_vest_locked: vec![0, 0, 0],
            pools: vec![PoolMeta {
                dex: "cswap".into(),
                key_policy: vec![1; 28],
                key_name: b"LP".to_vec(),
                key_basis: "datum".into(),
                fee_bps: Some(85),
            }],
            rc_slots: delta_encode(&[200, 400]),
            rc_pool: vec![0, 0],
            rc_base: vec![10, 12],
            rc_quote: vec![100, 90],
        }
    }

    fn detail() -> Detail {
        Detail {
            version: WIRE_VERSION,
            parties: vec![PartyMeta {
                address: "addr1...".into(),
                stake: None,
                cohort: 1,
                basis: "chain".into(),
            }],
            tx_slots: delta_encode(&[100, 300]),
            tx_net_mint: vec![1000, 0],
            mv_tx: vec![0, 1],
            mv_party: vec![0, 0],
            mv_amount: vec![1000, -5],
        }
    }

    #[test]
    fn spine_round_trips() {
        let bytes = encode_spine(&spine()).unwrap();
        assert_eq!(bytes[0], WIRE_VERSION, "version must be byte 0");
        assert_eq!(decode_spine(&bytes).unwrap(), spine());
    }

    #[test]
    fn tx_ids_round_trip_separately() {
        // Hashes live in their own file so a scrub never loads them.
        let ids = TxIds {
            version: WIRE_VERSION,
            hashes: vec![[1u8; 32], [2u8; 32]],
        };
        let bytes = encode_tx_ids(&ids).unwrap();
        assert_eq!(bytes[0], WIRE_VERSION);
        assert_eq!(decode_tx_ids(&bytes).unwrap(), ids);
    }

    #[test]
    fn detail_round_trips() {
        let bytes = encode_detail(&detail()).unwrap();
        assert_eq!(bytes[0], WIRE_VERSION);
        assert_eq!(decode_detail(&bytes).unwrap(), detail());
    }

    #[test]
    fn a_wrong_version_fails_loudly_rather_than_decoding_garbage() {
        let mut bytes = encode_spine(&spine()).unwrap();
        bytes[0] = WIRE_VERSION + 1;
        assert_eq!(
            decode_spine(&bytes),
            Err(WireError::Version {
                expected: WIRE_VERSION,
                found: WIRE_VERSION + 1
            })
        );
    }

    #[test]
    fn delta_encoding_round_trips_and_shrinks_the_payload() {
        let slots: Vec<u64> = (0..500).map(|i| 179_562_649 + i * 20).collect();
        assert_eq!(delta_decode(&delta_encode(&slots)), slots);
        // The point of the exercise: large absolute values with small gaps
        // must encode far smaller than the raw column.
        let raw = postcard::to_stdvec(&slots).unwrap();
        let encoded = postcard::to_stdvec(&delta_encode(&slots)).unwrap();
        assert!(
            encoded.len() * 3 < raw.len(),
            "delta encoding should shrink a slot column by well over 3x: {} vs {}",
            encoded.len(),
            raw.len()
        );
    }

    #[test]
    fn mismatched_columns_are_rejected_not_rendered() {
        // A truncated column is a silent reinterpretation, not a decode
        // failure — so it has to be caught explicitly.
        let mut s = spine();
        s.cp_holders.pop();
        assert!(matches!(encode_spine(&s), Err(WireError::Inconsistent(_))));

        let mut d = detail();
        d.mv_amount.pop();
        assert!(matches!(encode_detail(&d), Err(WireError::Inconsistent(_))));
    }

    #[test]
    fn dangling_indices_are_rejected() {
        let mut d = detail();
        d.mv_party = vec![0, 9];
        assert!(matches!(encode_detail(&d), Err(WireError::Inconsistent(_))));

        let mut s = spine();
        s.rc_pool = vec![0, 5];
        assert!(matches!(encode_spine(&s), Err(WireError::Inconsistent(_))));
    }
}
