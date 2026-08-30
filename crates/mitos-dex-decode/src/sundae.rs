//! SundaeSwap DEX decode surface — pool credentials and the V3 pool datum.
//!
//! ## V3 is the version that can be read from an unspent output
//!
//! Sampling 1,000 live UTxOs at each pool credential on 2026-08-30 split the
//! versions cleanly:
//!
//! ```text
//! SundaeSwap V3   996 inline datums,   0 hash-only
//! SundaeSwap V1     0 inline datums, 937 hash-only
//! ```
//!
//! V1 is PlutusV1, so its pool state is committed as a *hash* and the preimage
//! only has to appear in the witness set of the transaction that **spends** the
//! output — i.e. one interaction later than the state it describes. Decoding V1
//! therefore needs a hash→datum cache in the walker, not just a decoder, and it
//! is deliberately not attempted here. [`V1_PAYMENT_CRED`] is recorded so a
//! consumer can still classify a V1 pool UTxO as a pool (which is enough to get
//! the cohort right) without pretending it can price one.
//!
//! ## Reserves are the VALUE MINUS `protocol_fees`, on the ADA side only
//!
//! V3 publishes neither reserve. It publishes `protocol_fees` — lovelace the
//! pool holds but does not trade — and the reserve is what is left. Two checks
//! settled this rather than one reading:
//!
//! - Across 120 live ADA pools, `sqrt(reserve_a × reserve_b)` matches the
//!   pool's own `circulating_lp` with a **median error of 0.45%** (p90 3.0%).
//!   That is the constant-product invariant agreeing with a number the pool
//!   publishes independently, so the subtraction is right.
//! - On all 7 live **token/token** pools, `protocol_fees` equals the UTxO's
//!   entire lovelace balance. That only makes sense if the field is lovelace
//!   overhead — so on a token/token pool it must come off *neither* token side.
//!   See [`PoolV3::reserves`].
//!
//! Reading the raw value instead overstates a shallow pool badly: the
//! RobertKennedy pool holds 6,175,027 lovelace of which 6,172,002 is fees, so
//! its real ADA depth is **3,025** — a 2,000× overstatement.
//!
//! ## The pool script mints its own NFT
//!
//! [`POOL_NFT_POLICY`] and [`POOL_PAYMENT_CRED`] are the same 28 bytes. So the
//! pool NFT is self-certifying: an output at the pool credential holding a
//! token minted by that same credential is a pool, no datum required. The NFT's
//! name is [`NFT_NAME_PREFIX`] (CIP-68 label 222) followed by the datum's
//! `ident` — verified on 996 of 996 sampled pools — which makes it a
//! pool-INSTANCE key readable from the value alone.

use pallas_codec::minicbor;
use pallas_primitives::{Constr, PlutusData};

use crate::{bigint_to_u64, bounded_bytes};

/// SundaeSwap **V3** pool payment credential (script hash, 28 bytes).
pub const POOL_PAYMENT_CRED: [u8; 28] = [
    0xe0, 0x30, 0x25, 0x60, 0xce, 0xd2, 0xfd, 0xcb, 0xfc, 0xb2, 0x60, 0x26, 0x97, 0xdf, 0x97, 0x0c,
    0xd0, 0xd6, 0xa3, 0x8f, 0x94, 0xb3, 0x27, 0x03, 0xf5, 0x1c, 0x31, 0x2b,
];

/// Policy of the V3 pool NFT — **the same hash as [`POOL_PAYMENT_CRED`]**, the
/// script minting its own identity token. Not a copy-paste slip; asserted by a
/// test so it cannot be "fixed" into two different constants.
pub const POOL_NFT_POLICY: [u8; 28] = POOL_PAYMENT_CRED;

/// CIP-68 label 222 — every V3 pool NFT name is this followed by the 28-byte
/// `ident`.
pub const NFT_NAME_PREFIX: [u8; 4] = [0x00, 0x0d, 0xe1, 0x40];

/// The single V3 pool address. Unlike Splash and WingRiders V2, every sampled
/// pool sat at exactly one address — but recognition still matches the
/// credential, because that assumption has now been falsified twice.
pub const POOL_ADDR: &str = "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e";

pub fn is_sundae_v3(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &POOL_PAYMENT_CRED
}

/// SundaeSwap **V1** pool payment credential (script hash, 28 bytes).
///
/// Recognition only. V1 datums are hash-committed (937 of 1,000 sampled live
/// UTxOs), so there is no decoder here — see the module docs.
pub const V1_PAYMENT_CRED: [u8; 28] = [
    0x40, 0x20, 0xe7, 0xfc, 0x2d, 0xe7, 0x5a, 0x07, 0x29, 0xc3, 0xcc, 0x3a, 0xf7, 0x15, 0xb3, 0x4d,
    0x98, 0x38, 0x1e, 0x0c, 0xdb, 0xcf, 0xa9, 0x9c, 0x95, 0x0b, 0xc3, 0xac,
];

pub fn is_sundae_v1(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &V1_PAYMENT_CRED
}

/// Decoded SundaeSwap V3 pool datum. Field indices, verified on chain:
///
/// ```text
/// [0] ident (28 bytes)
/// [1] assets — an ARRAY of two ARRAYS, each [policy, name]. Bare arrays,
///     not `Constr`, unlike every other DEX in this crate.
/// [2] circulating_lp
/// [3] bid_fees_per_10_thousand   [4] ask_fees_per_10_thousand
/// [5] fee_manager (Option)       [6] market_open (posix ms)
/// [7] protocol_fees (lovelace)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolV3 {
    /// Unique per pool. Also the tail of the pool NFT's name — see
    /// [`PoolV3::nft_name`].
    pub ident: Vec<u8>,
    pub asset_a_policy: Vec<u8>,
    pub asset_a_name: Vec<u8>,
    pub asset_b_policy: Vec<u8>,
    pub asset_b_name: Vec<u8>,
    /// LP tokens outstanding, as the pool itself reports them.
    pub circulating_lp: u64,
    pub bid_fees_per_10_thousand: u64,
    pub ask_fees_per_10_thousand: u64,
    /// Lovelace held but not tradeable. **Not** part of the reserve.
    pub protocol_fees: u64,
}

impl PoolV3 {
    pub fn a_is_ada(&self) -> bool {
        self.asset_a_policy.is_empty() && self.asset_a_name.is_empty()
    }

    pub fn b_is_ada(&self) -> bool {
        self.asset_b_policy.is_empty() && self.asset_b_name.is_empty()
    }

    /// The pool NFT's asset name: [`NFT_NAME_PREFIX`] ++ `ident`. Matched all
    /// 996 sampled pools, so this is the pool-instance key.
    pub fn nft_name(&self) -> Vec<u8> {
        let mut n = NFT_NAME_PREFIX.to_vec();
        n.extend_from_slice(&self.ident);
        n
    }

    /// Reserves from the UTxO's holdings, netting off `protocol_fees`.
    ///
    /// The fee is lovelace, so it comes off whichever side is ADA — and off
    /// **neither** side when the pool is token/token, where the lovelace is
    /// carrier ADA that was never a reserve to begin with. Subtracting it from
    /// a token side there would silently shave real depth off a pool.
    pub fn reserves(&self, value_a: u64, value_b: u64) -> (u64, u64) {
        if self.a_is_ada() {
            (value_a.saturating_sub(self.protocol_fees), value_b)
        } else if self.b_is_ada() {
            (value_a, value_b.saturating_sub(self.protocol_fees))
        } else {
            (value_a, value_b)
        }
    }

    /// `(ada_reserve, token_reserve)` when one side is ADA, else `None`.
    ///
    /// `None` means "this pool cannot price the token", not "this pool is
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

    /// The larger of the two directional fees, in ten-thousandths.
    ///
    /// Which of `bid`/`ask` applies to selling the token is not something this
    /// crate has verified, and the two differ on 7 of 996 live pools — rare,
    /// but real. Taking the maximum is the conservative choice for a realisable
    /// -value estimate: it can understate what a seller receives, never
    /// overstate it. A caller that knows the direction should read the two
    /// fields directly.
    pub fn max_fee_per_10_thousand(&self) -> u64 {
        self.bid_fees_per_10_thousand
            .max(self.ask_fees_per_10_thousand)
    }
}

fn as_constr(pd: &PlutusData) -> Option<&Constr<PlutusData>> {
    match pd {
        PlutusData::Constr(c) => Some(c),
        _ => None,
    }
}

fn as_array(pd: &PlutusData) -> Option<&[PlutusData]> {
    match pd {
        PlutusData::Array(a) => Some(a.as_slice()),
        _ => None,
    }
}

/// `[policy, name]` as a bare two-element array. ADA is `["", ""]`.
fn asset_class(pd: &PlutusData) -> Option<(Vec<u8>, Vec<u8>)> {
    let a = as_array(pd)?;
    Some((bounded_bytes(a.first()?)?, bounded_bytes(a.get(1)?)?))
}

fn int_at(fields: &[&PlutusData], i: usize) -> Option<u64> {
    match fields.get(i)? {
        PlutusData::BigInt(b) => bigint_to_u64(b),
        _ => None,
    }
}

/// Decode a SundaeSwap V3 pool datum from inline-datum CBOR.
///
/// Arity is pinned at 8 here, unlike Splash and WingRiders where multiple
/// arities ship concurrently: all 996 sampled V3 pools carried exactly 8
/// fields, and the trailing fields are the ones this needs, so a prefix read
/// would buy nothing.
pub fn decode_v3_pool_datum(cbor: &[u8]) -> Option<PoolV3> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let c = as_constr(&pd)?;
    let fields: Vec<&PlutusData> = c.fields.iter().collect();
    if fields.len() != 8 {
        return None;
    }
    let pair = as_array(fields[1])?;
    let (asset_a_policy, asset_a_name) = asset_class(pair.first()?)?;
    let (asset_b_policy, asset_b_name) = asset_class(pair.get(1)?)?;
    Some(PoolV3 {
        ident: bounded_bytes(fields[0])?,
        asset_a_policy,
        asset_a_name,
        asset_b_policy,
        asset_b_name,
        circulating_lp: int_at(&fields, 2)?,
        bid_fees_per_10_thousand: int_at(&fields, 3)?,
        ask_fees_per_10_thousand: int_at(&fields, 4)?,
        protocol_fees: int_at(&fields, 7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real ADA/SWANGONDEEZNUTS V3 pool, captured from chain 2026-08-30 at
    /// `72da5ce1…#0`. The UTxO held 82,427,829 lovelace and
    /// 95,623,317,765,719 tokens.
    const SWANGO: &str = "d8799f581c67d2bb41de1b0e97f6a9d2b720ee0bfe0a92d4651e5ea6ba5e250f3f9f9f4040ff9f581c9a8610a4476220781f89488dc3be5e729ca9ad45e623978bd32eebce4f5357414e474f4e4445455a4e555453ffff1b000000137ae1e2dd181e181ed87a80001a00844360ff";

    /// A real ADA/RobertKennedy V3 pool at `8b4803f2…#0` — one of the 7 live
    /// pools whose bid and ask fees differ.
    const KENNEDY: &str = "d8799f581cc330233b694b8729a4bf95c796aaad91603a4fa0498bb90e68547c0f9f9f4040ff9f581c86166251bee577f98171f8a067a0b7807a9a73c5a78d26076609ccc14d526f626572744b656e6e656479ffff191e3c1864181ed87a80001a005e2d62ff";

    fn swango() -> PoolV3 {
        decode_v3_pool_datum(&hex::decode(SWANGO).unwrap()).expect("decode")
    }

    #[test]
    fn decodes_a_real_ada_pool() {
        let p = swango();
        assert!(p.a_is_ada(), "side A is ADA");
        assert_eq!(p.asset_b_name, b"SWANGONDEEZNUTS".to_vec());
        assert_eq!(p.circulating_lp, 83_666_002_653);
        assert_eq!(p.protocol_fees, 8_668_000);
        assert_eq!(p.bid_fees_per_10_thousand, 30);
        assert_eq!(p.ask_fees_per_10_thousand, 30);
    }

    #[test]
    fn the_reserve_is_the_value_minus_the_protocol_fees() {
        // 82,427,829 held, 8,668,000 of it fees, so 73,759,829 trades.
        let p = swango();
        assert_eq!(
            p.ada_pair(82_427_829, 95_623_317_765_719),
            Some((73_759_829, 95_623_317_765_719))
        );
    }

    #[test]
    fn the_reserves_satisfy_the_constant_product_invariant() {
        // The independent check that the subtraction is right: the pool's own
        // circulating_lp should be sqrt(reserve_a * reserve_b). Subtracting
        // the fees lands within 0.4%; NOT subtracting them lands ~5% out, in
        // the direction that flatters the pool.
        let p = swango();
        let (ada, tok) = p.ada_pair(82_427_829, 95_623_317_765_719).unwrap();
        let root = (ada as u128 * tok as u128).isqrt() as u64;
        let err = root.abs_diff(p.circulating_lp) as f64 / p.circulating_lp as f64;
        assert!(err < 0.01, "netted reserves satisfy sqrt(a*b) ≈ lp: {err}");

        let raw = (82_427_829u128 * tok as u128).isqrt() as u64;
        let raw_err = raw.abs_diff(p.circulating_lp) as f64 / p.circulating_lp as f64;
        assert!(raw_err > err, "the raw value is a worse fit: {raw_err}");
    }

    #[test]
    fn a_token_token_pool_keeps_both_sides_whole() {
        // On every live token/token pool, protocol_fees equals the UTxO's
        // whole lovelace balance. Netting it off a TOKEN side would delete
        // real depth, so `reserves` must leave both alone.
        let mut p = swango();
        p.asset_a_policy = vec![0xaa; 28];
        p.asset_a_name = b"A".to_vec();
        assert_eq!(p.reserves(1_000, 2_000), (1_000, 2_000));
        assert!(p.ada_pair(1_000, 2_000).is_none());
    }

    #[test]
    fn ada_comes_back_first_whichever_side_it_is_on() {
        let mut p = swango();
        p.asset_a_policy = vec![0xaa; 28];
        p.asset_a_name = b"TOK".to_vec();
        p.asset_b_policy = Vec::new();
        p.asset_b_name = Vec::new();
        // ADA is on side B now, so it must still be returned first.
        assert_eq!(
            p.ada_pair(500, 10_000_000),
            Some((10_000_000 - 8_668_000, 500))
        );
    }

    #[test]
    fn asymmetric_fees_are_preserved_and_bounded_conservatively() {
        // 7 of 996 live pools have bid != ask. Collapsing them to one field
        // would quietly misprice those pools.
        let p = decode_v3_pool_datum(&hex::decode(KENNEDY).unwrap()).expect("decode");
        assert_eq!(p.bid_fees_per_10_thousand, 100);
        assert_eq!(p.ask_fees_per_10_thousand, 30);
        assert_eq!(p.max_fee_per_10_thousand(), 100);
    }

    #[test]
    fn a_shallow_pool_is_mostly_fees() {
        // 6,175,027 lovelace held, 6,172,002 of it fees. Reading the value
        // would overstate this pool's ADA depth 2,000-fold.
        let p = decode_v3_pool_datum(&hex::decode(KENNEDY).unwrap()).unwrap();
        let (ada, _) = p.ada_pair(6_175_027, 7_740).unwrap();
        assert_eq!(ada, 3_025);
    }

    #[test]
    fn the_pool_nft_name_is_the_cip68_prefix_plus_the_ident() {
        // Matched 996 of 996 sampled pools, so a consumer can key a pool
        // instance off the value without decoding anything.
        let p = swango();
        assert_eq!(
            hex::encode(p.nft_name()),
            "000de14067d2bb41de1b0e97f6a9d2b720ee0bfe0a92d4651e5ea6ba5e250f3f"
        );
    }

    #[test]
    fn the_pool_script_mints_its_own_nft() {
        // The NFT policy and the pool script hash really are the same 28
        // bytes. Pinned so nobody "corrects" it into two constants.
        assert_eq!(POOL_NFT_POLICY, POOL_PAYMENT_CRED);
        assert!(is_sundae_v3(&POOL_PAYMENT_CRED));
    }

    #[test]
    fn v1_is_recognised_but_not_confused_with_v3() {
        // V1 exists here for cohort classification only — its datums are
        // hash-committed and there is no decoder.
        assert!(is_sundae_v1(&V1_PAYMENT_CRED));
        assert!(!is_sundae_v3(&V1_PAYMENT_CRED));
        assert!(!is_sundae_v1(&POOL_PAYMENT_CRED));
    }

    #[test]
    fn a_wrong_arity_is_rejected_rather_than_half_read() {
        let short = "d8799f581c67d2bb41de1b0e97f6a9d2b720ee0bfe0a92d4651e5ea6ba5e250f3f00ff";
        assert!(decode_v3_pool_datum(&hex::decode(short).unwrap()).is_none());
    }
}
