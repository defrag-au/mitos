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
/// v2 added [`Spine::rc_eps_bps`] — the reserve curve's declared error bound.
/// Postcard is positional, so an added field is a breaking change and a v1
/// reader must reject a v2 page rather than decode it shifted.
pub const WIRE_VERSION: u8 = 2;

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

/// One party, assembled from the two tiers it is stored across.
///
/// **Not a wire type.** [`Detail`] holds the hot columns and [`PartyIds`] the
/// addresses; this is what a consumer builds after loading both, for the one
/// wallet it is actually rendering. Materialising every party into this shape
/// is what the split exists to avoid — that is the 28.7 MB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Party<'a> {
    pub address: &'a str,
    pub stake: Option<&'a str>,
    /// Index into [`Spine::cohorts`].
    pub cohort: u16,
    /// How firmly the cohort is known: `proven` / `decoded` / `registered` /
    /// `chain`. The whole point of separating this from the cohort is that a
    /// provably-unspendable script and a wallet somebody named are not the
    /// same kind of claim.
    pub basis: &'a str,
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
    /// How far the reserve curve may deviate from the true reserves, in basis
    /// points. `0` means every observation is present and the curve is exact.
    ///
    /// **This is the one part of the spine that is not exact**, and it is
    /// declared rather than assumed so a consumer can render the bound instead
    /// of implying a precision it does not have. Cohort checkpoints remain
    /// exact at any stride; only the curve is reduced.
    ///
    /// The reduction drops a point when neither the pool's price nor either of
    /// its reserves has moved by more than this since the last point KEPT FOR
    /// THAT POOL — so the guarantee is per-pool and holds at every slot, not
    /// just at retained points. First and last points of each pool are always
    /// kept, so a pool's birth and its tip are exact regardless.
    pub rc_eps_bps: u32,
}

/// Which reserve points to keep, so the curve stays within `eps_bps` of truth.
///
/// Returns indices into the input columns, ascending. `eps_bps == 0` keeps
/// everything, which is the exact curve.
///
/// ## Why this exists
///
/// The reserve curve is the spine's size floor — checkpoints compress, the
/// curve does not. Measured on WRT (1,174,382 transactions): raising the
/// checkpoint stride from 64 to 16,384 moved the spine only 957 KB → 634 KB
/// gzipped, because 51,318 reserve points dominate everything else. Reducing
/// *by time* would misrepresent a quiet pool that then moves sharply; reducing
/// by **price-change magnitude** keeps exactly the points that carry
/// information.
///
/// ## The comparison is per pool, and against the last KEPT point
///
/// Both matter. Per pool, because [`projections::reserves_at`] reconstructs
/// each pool's latest state and sums them — a global rule would drop a point
/// that is the only record of some pool's state. Against the last kept point
/// rather than the previous input point, because otherwise a long run of
/// sub-threshold moves in the same direction accumulates without bound.
pub fn reduce_curve(pool: &[u16], base: &[i64], quote: &[i64], eps_bps: u32) -> Vec<usize> {
    let n = pool.len();
    if eps_bps == 0 {
        return (0..n).collect();
    }
    // Last point kept for each pool, so the error is measured against what a
    // consumer will actually reconstruct.
    let mut last_kept: std::collections::HashMap<u16, (i64, i64)> =
        std::collections::HashMap::new();
    // The final point of each pool is always kept — a pool's tip must be exact,
    // and `stats` reads it directly.
    let mut final_ix: std::collections::HashMap<u16, usize> = std::collections::HashMap::new();
    for (i, p) in pool.iter().enumerate() {
        final_ix.insert(*p, i);
    }

    let moved = |prev: i64, now: i64| -> bool {
        if prev == now {
            return false;
        }
        // A reserve arriving at or leaving zero is always material: it is a
        // pool being created or drained, and a relative test cannot see it.
        if prev == 0 || now == 0 {
            return true;
        }
        let delta = (now - prev).unsigned_abs() as u128;
        delta * 10_000 > prev.unsigned_abs() as u128 * eps_bps as u128
    };

    let mut keep = Vec::with_capacity(n / 4);
    for i in 0..n {
        let p = pool[i];
        let (b, q) = (base[i], quote[i]);
        let material = match last_kept.get(&p) {
            // First sighting of a pool is always kept.
            None => true,
            Some(&(pb, pq)) => {
                // Depth on either side, and price. Price is checked separately
                // because both reserves can drift together — a proportional
                // liquidity add moves depth without moving price at all, and a
                // swap moves price while depth barely changes.
                moved(pb, b) || moved(pq, q) || price_moved(pb, pq, b, q, eps_bps)
            }
        };
        if material || final_ix.get(&p) == Some(&i) {
            keep.push(i);
            last_kept.insert(p, (b, q));
        }
    }
    keep
}

/// Has `quote/base` moved by more than `eps_bps`? Compared as a cross-product
/// so there is no division and no float.
fn price_moved(pb: i64, pq: i64, b: i64, q: i64, eps_bps: u32) -> bool {
    if pb <= 0 || b <= 0 {
        return pb != b;
    }
    // prev = pq/pb, now = q/b. |now - prev| / prev  ==  |q*pb - pq*b| / (pq*b).
    let lhs = (q as i128 * pb as i128 - pq as i128 * b as i128).unsigned_abs();
    let rhs = (pq as i128).unsigned_abs() * b as u128;
    lhs * 10_000 > rhs * eps_bps as u128
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
///
/// ## Parties are split hot from cold
///
/// A party used to be a [`PartyMeta`] record carrying its address. Measured on
/// WRT (173,388 parties): that dictionary was **28,696,700 bytes — 61.8% of
/// the whole detail artifact**, against 17,768,400 bytes of movement columns.
/// The columns were never the problem; at 11.0 bytes per movement they are
/// already lean. The *identifiers* were, exactly as with [`TxIds`], where
/// hashes turned out to be 57% of a file everyone assumed was rows.
///
/// So what a scrub needs stays and what it does not leaves. Attributing a
/// movement to a cohort needs `party_cohort` and `party_basis` — a few bytes
/// each. Rendering an address needs 103 characters, and only when somebody
/// drills into one wallet. The addresses live in [`PartyIds`], loaded on click.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Detail {
    pub version: u8,
    /// Basis names, interned — `proven` / `decoded` / `registered` / `chain`.
    /// Four distinct strings across every party, so storing the string per
    /// party cost more than the cohort it qualifies.
    pub bases: Vec<String>,
    /// Cohort per party ordinal — index into [`Spine::cohorts`].
    pub party_cohort: Vec<u16>,
    /// Basis per party ordinal — index into `bases`. Kept beside the cohort
    /// rather than folded into it: a provably-unspendable script and a wallet
    /// somebody merely named are the same cohort and very different claims.
    pub party_basis: Vec<u16>,
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

/// Party addresses, indexed by ordinal — the other click-through tier.
///
/// Parallel to [`Detail::party_cohort`]: party `i` in one is party `i` in the
/// other. They are separate files and nothing but that convention binds them,
/// so a consumer holding both must check the lengths agree — see
/// [`PartyIds::agrees_with`].
///
/// Held apart from [`Detail`] for the same measured reason as [`TxIds`]: on
/// WRT this is 28.7 MB against 17.8 MB of movement columns, and a time-scrub
/// never renders an address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartyIds {
    pub version: u8,
    /// Bech32 payment address per party ordinal.
    pub addresses: Vec<String>,
    /// Bech32 stake address, where the payment address has a delegation part.
    pub stakes: Vec<Option<String>>,
}

impl PartyIds {
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    /// Do these addresses belong to that detail page?
    ///
    /// The check a consumer cannot skip: mismatched files index-shift every
    /// address onto the wrong party, which renders perfectly and is entirely
    /// wrong.
    pub fn agrees_with(&self, detail: &Detail) -> bool {
        self.addresses.len() == detail.party_count() && self.stakes.len() == detail.party_count()
    }

    /// Assemble one party across the two tiers. `None` if the ordinal is out of
    /// range in either, which is the case a length check would have caught.
    pub fn party<'a>(&'a self, detail: &'a Detail, ix: usize) -> Option<Party<'a>> {
        Some(Party {
            address: self.addresses.get(ix)?,
            stake: self.stakes.get(ix)?.as_deref(),
            cohort: *detail.party_cohort.get(ix)?,
            basis: detail.bases.get(*detail.party_basis.get(ix)? as usize)?,
        })
    }
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

    pub fn party_count(&self) -> usize {
        self.party_cohort.len()
    }

    /// How many distinct cohorts the parties actually span.
    pub fn cohort_span(&self) -> usize {
        let mut seen: Vec<u16> = self.party_cohort.clone();
        seen.sort_unstable();
        seen.dedup();
        seen.len()
    }

    /// What an inline per-movement cohort byte would cost.
    ///
    /// The alternative to a global `party -> cohort` table. Inline, a chunk
    /// answers "which cohort did this movement touch" without reading anything
    /// outside itself, which is the property chunking needs — a self-contained
    /// chunk has no cross-file dependency to get wrong.
    ///
    /// Cheap because a cohort is one of a handful of values, so the column is
    /// long runs of the same byte and compresses to near nothing.
    pub fn inline_cohort_bytes(&self) -> usize {
        let col: Vec<u8> = self
            .mv_party
            .iter()
            .map(|p| self.party_cohort[*p as usize] as u8)
            .collect();
        postcard::to_allocvec(&col).map(|v| v.len()).unwrap_or(0)
    }

    /// Encoded size of the party attribute columns (`bases`, `party_cohort`,
    /// `party_basis`) alone.
    ///
    /// These are global — every movement in the log indexes into them — so if
    /// the movement columns are ever chunked, this is the cost that would be
    /// duplicated into each chunk. That makes it the number that decides
    /// whether chunking needs a further tier split or can be done in place.
    pub fn party_attr_bytes(&self) -> usize {
        postcard::to_allocvec(&(&self.bases, &self.party_cohort, &self.party_basis))
            .map(|v| v.len())
            .unwrap_or(0)
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
        if self.party_basis.len() != self.party_cohort.len() {
            return Err(WireError::Inconsistent("party columns differ in length"));
        }
        if self
            .mv_party
            .iter()
            .any(|p| *p as usize >= self.party_count())
        {
            return Err(WireError::Inconsistent("movement names an unknown party"));
        }
        if self
            .party_basis
            .iter()
            .any(|b| *b as usize >= self.bases.len())
        {
            return Err(WireError::Inconsistent("party names an unknown basis"));
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

pub fn encode_party_ids(ids: &PartyIds) -> Result<Vec<u8>, WireError> {
    if ids.addresses.len() != ids.stakes.len() {
        return Err(WireError::Inconsistent("party id columns differ in length"));
    }
    postcard::to_stdvec(ids).map_err(|_| WireError::Malformed)
}

pub fn decode_party_ids(bytes: &[u8]) -> Result<PartyIds, WireError> {
    check_version(bytes)?;
    let ids: PartyIds = postcard::from_bytes(bytes).map_err(|_| WireError::Malformed)?;
    if ids.addresses.len() != ids.stakes.len() {
        return Err(WireError::Inconsistent("party id columns differ in length"));
    }
    Ok(ids)
}

#[cfg(test)]
mod party_tier_tests {
    use super::*;

    fn detail() -> Detail {
        Detail {
            version: WIRE_VERSION,
            bases: vec!["chain".into(), "proven".into()],
            party_cohort: vec![0, 1, 0],
            party_basis: vec![0, 1, 0],
            tx_slots: delta_encode(&[100]),
            tx_net_mint: vec![10],
            mv_tx: vec![0],
            mv_party: vec![2],
            mv_amount: vec![10],
        }
    }

    fn ids() -> PartyIds {
        PartyIds {
            version: WIRE_VERSION,
            addresses: vec!["addr_a".into(), "addr_b".into(), "addr_c".into()],
            stakes: vec![Some("stake_a".into()), None, None],
        }
    }

    #[test]
    fn the_two_tiers_reassemble_one_party() {
        let (d, i) = (detail(), ids());
        let p = i.party(&d, 1).expect("party 1");
        assert_eq!(p.address, "addr_b");
        assert_eq!(p.stake, None);
        assert_eq!(p.cohort, 1);
        assert_eq!(p.basis, "proven");
    }

    #[test]
    fn a_short_party_file_is_caught_rather_than_shifting_every_address() {
        // The failure this check exists for: drop one address and every party
        // after it silently wears its neighbour's identity. Nothing errors,
        // nothing looks wrong, and every attribution past that point is a lie.
        let d = detail();
        let mut i = ids();
        assert!(i.agrees_with(&d));
        i.addresses.pop();
        i.stakes.pop();
        assert!(!i.agrees_with(&d));
    }

    #[test]
    fn party_ids_round_trip_separately_from_detail() {
        let bytes = encode_party_ids(&ids()).expect("encode");
        assert_eq!(decode_party_ids(&bytes).expect("decode"), ids());
    }

    #[test]
    fn mismatched_party_id_columns_are_rejected() {
        let mut i = ids();
        i.stakes.pop();
        assert!(encode_party_ids(&i).is_err());
    }

    #[test]
    fn a_basis_index_outside_the_dictionary_is_rejected() {
        let mut d = detail();
        d.party_basis[0] = 9;
        assert!(d.validate().is_err());
    }

    #[test]
    fn the_hot_tier_carries_no_addresses() {
        // The whole point of the split, asserted so it cannot regress: on WRT
        // the addresses were 28.7 MB against 17.8 MB of movement columns, and
        // a time-scrub never renders one.
        let encoded = encode_detail(&detail()).expect("encode");
        let text = String::from_utf8_lossy(&encoded);
        assert!(
            !text.contains("addr_"),
            "detail must not carry addresses — they belong in PartyIds"
        );
    }
}

#[cfg(test)]
mod curve_reduction_tests {
    use super::*;

    /// Replay a reduced curve the way `reserves_at` does — each pool's latest
    /// kept state — and report the worst relative price error against truth at
    /// every input index.
    fn worst_price_error(pool: &[u16], base: &[i64], quote: &[i64], eps: u32) -> f64 {
        let keep = reduce_curve(pool, base, quote, eps);
        let kept: std::collections::HashSet<usize> = keep.iter().copied().collect();
        let mut latest: std::collections::HashMap<u16, (i64, i64)> =
            std::collections::HashMap::new();
        let mut worst = 0.0f64;
        for i in 0..pool.len() {
            if kept.contains(&i) {
                latest.insert(pool[i], (base[i], quote[i]));
            }
            let (rb, rq) = latest[&pool[i]];
            let seen = rq as f64 / rb as f64;
            let truth = quote[i] as f64 / base[i] as f64;
            worst = worst.max((seen - truth).abs() / truth);
        }
        worst
    }

    #[test]
    fn zero_epsilon_keeps_every_point() {
        let pool = vec![0u16; 5];
        let base = vec![100, 101, 102, 103, 104];
        let quote = vec![100, 100, 100, 100, 100];
        assert_eq!(reduce_curve(&pool, &base, &quote, 0), vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn a_flat_pool_collapses_to_its_endpoints() {
        // 200 identical observations carry one bit of information.
        let pool = vec![0u16; 200];
        let base = vec![1_000_000i64; 200];
        let quote = vec![2_000_000i64; 200];
        let keep = reduce_curve(&pool, &base, &quote, 10);
        assert_eq!(keep, vec![0, 199], "first and last only");
    }

    #[test]
    fn the_error_never_exceeds_the_declared_bound() {
        // The guarantee the artifact advertises. A drifting pool is the hard
        // case: each step is sub-threshold, so a naive "compare to previous
        // input" rule would let the error accumulate without limit.
        let n = 500;
        let pool = vec![0u16; n];
        let base: Vec<i64> = (0..n).map(|_| 1_000_000i64).collect();
        let quote: Vec<i64> = (0..n).map(|i| 1_000_000 + i as i64 * 400).collect();
        for eps in [10u32, 50, 200] {
            let worst = worst_price_error(&pool, &base, &quote, eps);
            let bound = eps as f64 / 10_000.0;
            assert!(
                worst <= bound * 1.0001,
                "eps {eps}: worst {worst} exceeded bound {bound}"
            );
        }
    }

    #[test]
    fn drift_accumulating_below_the_threshold_is_still_caught() {
        // 0.05% per step, 500 steps — 22% total. Comparing against the last
        // KEPT point catches it; comparing against the previous input would
        // keep only the endpoints and be 22% wrong in between.
        let n = 500;
        let pool = vec![0u16; n];
        let base = vec![1_000_000i64; n];
        let quote: Vec<i64> = (0..n).map(|i| 1_000_000 + i as i64 * 500).collect();
        let keep = reduce_curve(&pool, &base, &quote, 100);
        assert!(
            keep.len() > 10,
            "sub-threshold drift must still be sampled, kept {}",
            keep.len()
        );
        assert!(worst_price_error(&pool, &base, &quote, 100) <= 0.0101);
    }

    #[test]
    fn each_pool_is_reduced_against_its_own_history() {
        // Interleaved pools. A global rule would compare pool 1's reserves
        // against pool 0's and drop points that are the only record of a
        // pool's state.
        let pool = vec![0u16, 1, 0, 1, 0, 1];
        let base = vec![100, 5_000_000, 100, 5_000_000, 100, 5_000_000];
        let quote = vec![100, 9_000_000, 100, 9_000_000, 100, 9_000_000];
        let keep = reduce_curve(&pool, &base, &quote, 10);
        // Both pools are flat, so each keeps only its first and last.
        assert_eq!(keep, vec![0, 1, 4, 5]);
    }

    #[test]
    fn a_pool_draining_to_zero_is_always_kept() {
        // A relative test cannot see a move to zero, and a drained pool is
        // exactly the event a liquidity view exists to show.
        let pool = vec![0u16, 0, 0, 0];
        let base = vec![1_000_000, 1_000_000, 0, 0];
        let quote = vec![2_000_000, 2_000_000, 0, 0];
        let keep = reduce_curve(&pool, &base, &quote, 10);
        assert!(keep.contains(&2), "the drain must survive reduction");
    }

    #[test]
    fn a_proportional_liquidity_add_moves_depth_without_moving_price() {
        // Both reserves double: price is unchanged, but depth is not, and the
        // realisable band reads depth. Checking price alone would drop this.
        let pool = vec![0u16, 0, 0];
        let base = vec![1_000_000, 2_000_000, 2_000_000];
        let quote = vec![2_000_000, 4_000_000, 4_000_000];
        let keep = reduce_curve(&pool, &base, &quote, 10);
        assert!(keep.contains(&1), "a depth change must survive reduction");
    }

    #[test]
    fn a_pools_tip_is_exact_however_hard_the_curve_is_reduced() {
        // `stats` reads the last point per pool directly, so reduction must
        // never move it.
        let pool = vec![0u16, 0, 0, 0];
        let base = vec![1_000_000, 1_000_001, 1_000_002, 999_999];
        let quote = vec![2_000_000, 2_000_001, 2_000_002, 1_999_998];
        let keep = reduce_curve(&pool, &base, &quote, 10_000);
        assert_eq!(*keep.last().unwrap(), 3);
    }
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
            // Exact — a fixture should not carry an error bound it does not
            // need, or a test asserting reserves could pass on a reduced curve.
            rc_eps_bps: 0,
        }
    }

    fn detail() -> Detail {
        Detail {
            version: WIRE_VERSION,
            bases: vec!["chain".into()],
            party_cohort: vec![1],
            party_basis: vec![0],
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
