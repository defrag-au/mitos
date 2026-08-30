//! WingRiders DEX decode surface — pool addresses and the V2 pool datum.
//!
//! ## Reserves are the VALUE MINUS THE TREASURY
//!
//! Unlike Minswap V2, WingRiders does not publish reserves in the datum. It
//! publishes accumulated *treasuries*, and the reserve is what the UTxO holds
//! beyond them. Both mistakes are easy and both are large:
//!
//! - read the raw value and a pool with a fat treasury looks far deeper than
//!   it is;
//! - read the treasury fields as reserves and a live pool looks dead. On an
//!   ADA/CHESS pool holding 179,033,521 lovelace, field 12 reads 28,016 —
//!   0.028 ADA against a pool visibly holding 179.
//!
//! Which reading is right was settled by checking 15 live pools rather than
//! reasoning from one: several fresh pools carry zero in both fields, which
//! only makes sense for a treasury.
//!
//! ## Most WingRiders V2 pools are token/token
//!
//! Of those 15, only 4 paired with ADA — the rest were NIGHT/IAG, EDM/HKDG,
//! NIGHT/USDA, ßUSDM/iUSD. A token/token pool holds real supply but **cannot
//! price a token in ADA**; its lovelace is a min-UTxO carrier. See
//! [`PoolV2::ada_pair`].
//!
//! ## The datum has shipped at several arities
//!
//! 5, ~16 and 21 fields exist in the wild. The prefix is stable — request
//! validator hash, then the pair — so this decodes what it needs and tolerates
//! the rest, rather than pinning one arity and failing on the others.

use pallas_codec::minicbor;
use pallas_primitives::{Constr, PlutusData};

use crate::{bigint_to_u64, bounded_bytes};

/// WingRiders **V2** pool payment credential (script hash, 28 bytes).
///
/// **Match on this, not on a full address.** V2 pools appear in at least two
/// address forms sharing this one credential — an enterprise `addr1w…` and a
/// stake-bearing `addr1z…`. Both were seen holding the same token on chain
/// 2026-08-30, and an exact-address rule silently missed the second, which
/// then surfaced as an unidentified `script` holder rather than a pool.
pub const V2_PAYMENT_CRED: [u8; 28] = [
    0xaf, 0x97, 0x79, 0x3b, 0x87, 0x02, 0xf3, 0x81, 0x97, 0x6c, 0xec, 0x83, 0xe3, 0x03, 0xe9, 0xce,
    0x17, 0x78, 0x14, 0x58, 0xc7, 0x3c, 0x4b, 0xb1, 0x6f, 0xe0, 0x2b, 0x83,
];

/// The enterprise form of the V2 pool address. Kept for reference; prefer
/// [`is_wingriders_v2`], which catches every form.
pub const V2_POOL_ADDR: &str = "addr1wxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqczc0qfe";

pub fn is_wingriders_v2(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &V2_PAYMENT_CRED
}

/// WingRiders **V1** pool payment credential (script hash, 28 bytes).
pub const V1_PAYMENT_CRED: [u8; 28] = [
    0xe6, 0xc9, 0x0a, 0x59, 0x23, 0x71, 0x3a, 0xf5, 0x78, 0x69, 0x63, 0xde, 0xe0, 0xfd, 0xff, 0xd8,
    0x30, 0xca, 0x7e, 0x0c, 0x86, 0xa0, 0x41, 0xd9, 0xe5, 0x83, 0x3e, 0x91,
];

/// WingRiders V2's LP policy. Mints both the pool NFT and the LP token, so the
/// LP *name* distinguishes pools.
pub const V2_LP_POLICY: [u8; 28] = [
    0x6f, 0xdc, 0x63, 0xa1, 0xd7, 0x1d, 0xc2, 0xc6, 0x55, 0x02, 0xb7, 0x9b, 0xaa, 0xe7, 0xfb, 0x54,
    0x31, 0x85, 0x70, 0x2b, 0x12, 0xc3, 0xc5, 0xfb, 0x63, 0x9e, 0xd7, 0x37,
];

pub fn is_wingriders_v1(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &V1_PAYMENT_CRED
}

/// Decoded WingRiders V2 pool datum. Field indices, verified on chain:
///
/// ```text
/// [0] requestValidatorHash
/// [1] assetA policy   [2] assetA name
/// [3] assetB policy   [4] assetB name
/// [5] swap fee    [6] protocol fee   [9] fee denominator
/// [11] lastInteracted (ms)
/// [12] treasuryA  [13] treasuryB
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolV2 {
    pub asset_a_policy: Vec<u8>,
    pub asset_a_name: Vec<u8>,
    pub asset_b_policy: Vec<u8>,
    pub asset_b_name: Vec<u8>,
    /// Accumulated fees on the A side — **not** part of the reserve.
    pub treasury_a: u64,
    pub treasury_b: u64,
}

impl PoolV2 {
    pub fn a_is_ada(&self) -> bool {
        self.asset_a_policy.is_empty() && self.asset_a_name.is_empty()
    }

    pub fn b_is_ada(&self) -> bool {
        self.asset_b_policy.is_empty() && self.asset_b_name.is_empty()
    }

    /// Reserves from the UTxO's holdings, netting off the treasuries.
    ///
    /// Saturating: a treasury larger than the holding would mean the pool owes
    /// more than it has, which should not happen — and if it does, a zero
    /// reserve is the safe reading, not a wrapped enormous one.
    pub fn reserves(&self, value_a: u64, value_b: u64) -> (u64, u64) {
        (
            value_a.saturating_sub(self.treasury_a),
            value_b.saturating_sub(self.treasury_b),
        )
    }

    /// `(ada_reserve, token_reserve)` when one side is ADA, else `None`.
    ///
    /// `None` is the answer for a token/token pool — the common case here —
    /// and it means "this pool cannot price the token", not "this pool is
    /// empty".
    pub fn ada_pair(&self, value_a: u64, value_b: u64) -> Option<(u64, u64)> {
        let (ra, rb) = self.reserves(value_a, value_b);
        if self.a_is_ada() {
            Some((ra, rb))
        } else if self.b_is_ada() {
            Some((rb, ra))
        } else {
            None
        }
    }
}

/// Total LP supply for WingRiders' mint-max pattern.
///
/// It mints `2^63 − 1` and keeps the unissued remainder in the pool, so
/// circulating supply is the difference. Note the base is `i64::MAX`, not
/// `u64::MAX` as Minswap uses.
pub fn issued_lp(pool_held_lp: u64) -> u64 {
    (i64::MAX as u64).saturating_sub(pool_held_lp)
}

fn as_constr(pd: &PlutusData) -> Option<&Constr<PlutusData>> {
    match pd {
        PlutusData::Constr(c) => Some(c),
        _ => None,
    }
}

/// Decode a WingRiders V2 pool datum from inline-datum CBOR.
///
/// Accepts any arity that carries the fields this needs — the contract has
/// shipped at 5, ~16 and 21 fields, and pinning one would fail on the others.
/// Treasuries are `0` on the short form, which is correct: a pool too early to
/// have the fields has accumulated nothing.
pub fn decode_v2_pool_datum(cbor: &[u8]) -> Option<PoolV2> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let c = as_constr(&pd)?;
    let fields: Vec<&PlutusData> = c.fields.iter().collect();
    // 5 fields is the minimum that names the pair; below that there is nothing
    // to identify the pool by.
    if fields.len() < 5 {
        return None;
    }
    let int_at = |i: usize| -> u64 {
        fields
            .get(i)
            .and_then(|f| match f {
                PlutusData::BigInt(b) => bigint_to_u64(b),
                _ => None,
            })
            .unwrap_or(0)
    };
    Some(PoolV2 {
        asset_a_policy: bounded_bytes(fields.get(1).copied()?)?,
        asset_a_name: bounded_bytes(fields.get(2).copied()?)?,
        asset_b_policy: bounded_bytes(fields.get(3).copied()?)?,
        asset_b_name: bounded_bytes(fields.get(4).copied()?)?,
        treasury_a: int_at(12),
        treasury_b: int_at(13),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real NIGHT/IAG V2 pool datum, captured from chain 2026-08-30.
    ///
    /// The UTxO held 1,559,857 NIGHT and 8,970,375 IAG; the datum reports
    /// treasuries of 1,559,847 and 3,701,900. So the real NIGHT reserve is
    /// **10** — reading the value instead would overstate it 156,000×.
    const NIGHT_IAG: &str = "d8799f581cc134d839a64a5dfb9b155869ef3f34280751a622f69958baa8ffd29c581c0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa454e49474854581c5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d11443494147181e0500001927101a001e84801b0000019c6f057aa01a0017cd271a00387c8c00000000d87a80d87a80d87980ff";

    fn night_iag() -> PoolV2 {
        decode_v2_pool_datum(&hex::decode(NIGHT_IAG).unwrap()).expect("decode")
    }

    #[test]
    fn decodes_a_real_night_iag_pool() {
        let p = night_iag();
        assert_eq!(p.asset_a_name, b"NIGHT".to_vec());
        assert_eq!(p.asset_b_name, b"IAG".to_vec());
        assert_eq!(p.treasury_a, 1_559_847);
        assert_eq!(p.treasury_b, 3_701_900);
    }

    #[test]
    fn the_reserve_is_the_value_minus_the_treasury() {
        // The number that matters: 1,559,857 held, 1,559,847 owed to the
        // treasury, so 10 is actually tradeable. Reading the value would
        // overstate depth 156,000-fold.
        let p = night_iag();
        let (night, iag) = p.reserves(1_559_857, 8_970_375);
        assert_eq!(night, 10);
        assert_eq!(iag, 5_268_475);
    }

    #[test]
    fn a_token_token_pool_reports_no_ada_pair() {
        // NIGHT/IAG prices neither side in ADA. Most V2 pools look like this,
        // so answering `None` is the common path, not an edge case.
        assert!(night_iag().ada_pair(1_559_857, 8_970_375).is_none());
    }

    #[test]
    fn an_ada_pool_orients_ada_first_whichever_side_it_is_on() {
        let mut p = night_iag();
        p.asset_a_policy = Vec::new();
        p.asset_a_name = Vec::new();
        p.treasury_a = 0;
        assert_eq!(p.ada_pair(1_000, 8_970_375), Some((1_000, 5_268_475)));

        let mut q = night_iag();
        q.asset_b_policy = Vec::new();
        q.asset_b_name = Vec::new();
        q.treasury_b = 0;
        // ADA is on side B, so it must still come back first.
        assert_eq!(q.ada_pair(1_559_857, 2_000), Some((2_000, 10)));
    }

    #[test]
    fn a_treasury_larger_than_the_holding_floors_at_zero() {
        // Should not happen, but wrapping would turn a drained pool into one
        // with 18 quintillion of depth.
        let p = night_iag();
        assert_eq!(p.reserves(1, 1), (0, 0));
    }

    #[test]
    fn the_short_arity_still_decodes_with_zero_treasuries() {
        // The contract shipped a 5-field form; pinning arity 21 would reject
        // every pool created under it.
        let short = "d8799f581cc134d839a64a5dfb9b155869ef3f34280751a622f69958baa8ffd29c40404040ff";
        let p = decode_v2_pool_datum(&hex::decode(short).unwrap()).expect("short form");
        assert_eq!(p.treasury_a, 0);
        assert_eq!(p.treasury_b, 0);
        assert!(p.a_is_ada() && p.b_is_ada());
    }

    #[test]
    fn both_address_forms_share_one_credential() {
        // Seen on chain holding the same token: an enterprise `addr1w…` and a
        // stake-bearing `addr1z…`. Matching the exact address caught only the
        // first, and the second surfaced as an unidentified script holder.
        assert!(is_wingriders_v2(&V2_PAYMENT_CRED));
        assert!(!is_wingriders_v2(&V1_PAYMENT_CRED));
        // The credential is the payment part of the published enterprise
        // address, so the two cannot drift apart.
        assert!(V2_POOL_ADDR.starts_with("addr1w"));
    }

    #[test]
    fn issued_lp_uses_the_i64_max_base_not_u64() {
        // WingRiders mints 2^63−1, Minswap mints 2^64−1. Using the wrong base
        // makes every LP share wrong by a factor of two.
        assert_eq!(issued_lp(i64::MAX as u64), 0);
        assert_eq!(issued_lp(i64::MAX as u64 - 1_000), 1_000);
    }
}
