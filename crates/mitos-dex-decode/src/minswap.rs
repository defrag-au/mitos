//! Minswap DEX decode surface — pool script credentials and the V2 pool datum.
//!
//! Pools are recognised by **payment credential**, not full address: the stake
//! part is contract-derived per pool, so a full-address set would need one
//! entry per pool and would miss every new one.
//!
//! ## Reserves come from the DATUM, not the UTxO value
//!
//! This is the correction that matters and it is not marginal. A Minswap V2
//! pool UTxO holds its reserves *plus* a fixed ADA deposit, accumulated fees,
//! its pool NFT and its unissued LP supply. Measured on chain 2026-08-30:
//!
//! | pool | value | `reserve_a` | error if you read value |
//! |---|---|---|---|
//! | ADA/DOG | 316,378,044 | 308,499,396 | +2.6% |
//! | ADA/Rookie | 5,477,110 | 1 | 5.5M× |
//! | ADA/ßGERK (token side) | 124,950,000,397,180 | 397,180 | 314,000,000× |
//!
//! A dead pool reads as a live one and a donated balance reads as depth. So
//! [`PoolV2::reserve_a`] / [`reserve_b`](PoolV2::reserve_b) are authoritative
//! and callers must not substitute the value.
//!
//! ## V1 and V2 are SWAPPED in `shared-crates/address-registry`
//!
//! Verified on chain 2026-06-24: the credential labelled "Minswap V2" there
//! (`e1317b15…`) is actually V1, and the one labelled "Minswap" (`ea07b733…`)
//! is V2. The constants below are correct; anything keying off that registry's
//! labels inherits the error.

use pallas_codec::minicbor;
use pallas_primitives::{Constr, PlutusData};

use crate::{bigint_to_u64, bounded_bytes};

/// Minswap **V1** pool payment credential (script hash, 28 bytes).
pub const V1_PAYMENT_CRED: [u8; 28] = [
    0xe1, 0x31, 0x7b, 0x15, 0x2f, 0xaa, 0xc1, 0x34, 0x26, 0xe6, 0xa8, 0x3e, 0x06, 0xff, 0x88, 0xa4,
    0xd6, 0x2c, 0xce, 0x3c, 0x16, 0x34, 0xab, 0x0a, 0x5e, 0xc1, 0x33, 0x09,
];

/// Minswap **V2** pool payment credential (script hash, 28 bytes).
pub const V2_PAYMENT_CRED: [u8; 28] = [
    0xea, 0x07, 0xb7, 0x33, 0xd9, 0x32, 0x12, 0x9c, 0x37, 0x8a, 0xf6, 0x27, 0x43, 0x6e, 0x7c, 0xbc,
    0x2e, 0xf0, 0xbf, 0x96, 0xe0, 0x03, 0x6b, 0xb5, 0x1b, 0x3b, 0xde, 0x6b,
];

/// Minswap V2's shared "authen" policy — mints every V2 pool's NFT and LP
/// token, so the LP *name* is what distinguishes one pool from another.
pub const V2_AUTHEN_POLICY: [u8; 28] = [
    0xf5, 0x80, 0x8c, 0x2c, 0x99, 0x0d, 0x86, 0xda, 0x54, 0xbf, 0xc9, 0x7d, 0x89, 0xce, 0xe6, 0xef,
    0xa2, 0x0c, 0xd8, 0x46, 0x16, 0x16, 0x35, 0x94, 0x78, 0xd9, 0x6b, 0x4c,
];

pub fn is_minswap_v1(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &V1_PAYMENT_CRED
}

pub fn is_minswap_v2(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &V2_PAYMENT_CRED
}

/// An asset as Minswap encodes it: `Constr 0 [policy, name]`. ADA is the empty
/// policy with the empty name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AssetClass {
    pub policy: Vec<u8>,
    pub name: Vec<u8>,
}

impl AssetClass {
    pub fn is_ada(&self) -> bool {
        self.policy.is_empty() && self.name.is_empty()
    }
}

/// Decoded Minswap V2 pool datum (`Constr 0`, 10 fields):
///
/// ```text
/// [0] batchingCred     [1] assetA        [2] assetB
/// [3] totalLiquidity   [4] reserveA      [5] reserveB
/// [6] feeA             [7] feeB          [8] feeShareOpt
/// [9] allowDynamicFee
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolV2 {
    pub asset_a: AssetClass,
    pub asset_b: AssetClass,
    pub total_liquidity: u64,
    pub reserve_a: u64,
    pub reserve_b: u64,
    /// Fee numerator against a 10,000 basis. Minswap V2 fees are per-direction;
    /// this is the A→B side.
    pub fee_a_bps: u64,
    pub fee_b_bps: u64,
}

impl PoolV2 {
    /// The pool's ADA side, if it has one, as `(ada_reserve, token_reserve)`.
    ///
    /// `None` for a token/token pool. Those hold real supply but **cannot
    /// price a token in ADA** — their lovelace is a min-UTxO carrier, and
    /// treating it as a quote reserve is wrong by orders of magnitude.
    pub fn ada_pair(&self) -> Option<(u64, u64)> {
        if self.asset_a.is_ada() {
            Some((self.reserve_a, self.reserve_b))
        } else if self.asset_b.is_ada() {
            Some((self.reserve_b, self.reserve_a))
        } else {
            None
        }
    }
}

fn as_constr(pd: &PlutusData) -> Option<&Constr<PlutusData>> {
    match pd {
        PlutusData::Constr(c) => Some(c),
        _ => None,
    }
}

/// `Constr 0 [policy, name]`.
fn asset_class(pd: &PlutusData) -> Option<AssetClass> {
    let c = as_constr(pd)?;
    let f: Vec<&PlutusData> = c.fields.iter().collect();
    Some(AssetClass {
        policy: bounded_bytes(f.first().copied()?)?,
        name: bounded_bytes(f.get(1).copied()?)?,
    })
}

fn int_at(fields: &[&PlutusData], i: usize) -> Option<u64> {
    match fields.get(i)? {
        PlutusData::BigInt(b) => bigint_to_u64(b),
        _ => None,
    }
}

/// Decode a Minswap V2 pool datum from inline-datum CBOR.
///
/// Returns `None` for anything that is not a well-formed 10-field
/// alternative-0 record — defensive against unrelated `PlutusData` landing at
/// the pool address, and against a future contract version silently changing
/// arity.
pub fn decode_v2_pool_datum(cbor: &[u8]) -> Option<PoolV2> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let c = as_constr(&pd)?;
    let fields: Vec<&PlutusData> = c.fields.iter().collect();
    if fields.len() != 10 {
        return None;
    }
    Some(PoolV2 {
        asset_a: asset_class(fields.get(1).copied()?)?,
        asset_b: asset_class(fields.get(2).copied()?)?,
        total_liquidity: int_at(&fields, 3)?,
        reserve_a: int_at(&fields, 4)?,
        reserve_b: int_at(&fields, 5)?,
        fee_a_bps: int_at(&fields, 6)?,
        fee_b_bps: int_at(&fields, 7)?,
    })
}

/// Total LP supply from the pool's own LP holding, for the mint-max pattern.
///
/// Minswap mints `u64::MAX` LP up front and keeps the unissued remainder in
/// the pool, so circulating supply is the difference. Prefer the datum's
/// `total_liquidity` where available; this exists for callers reading value
/// only.
pub fn issued_lp(pool_held_lp: u64) -> u64 {
    u64::MAX.saturating_sub(pool_held_lp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real ADA/DOG V2 pool datum, captured from chain 2026-08-30.
    ///
    /// Cross-checked against the UTxO it came from: the pool held 316,378,044
    /// lovelace and 993,870,205 DOG, while the datum reports reserves of
    /// 308,499,396 and 993,089,063. The gaps are the ADA deposit and
    /// accumulated fees — which is the whole reason this decoder exists.
    const ADA_DOG: &str = "d8799fd8799fd87a9f581c1eae96baf29e27682ea3f815aba361a0c6059d45e4bfbe95bbd2f44affffd8799f4040ffd8799f581c2229793477361daf2e47489ffca31ef4fbd5d142b609b87e18d7f19743444f47ff1a20a5953e1a126353c41a3b315627181e181ed8799f190682ffd87980ff";

    #[test]
    fn decodes_a_real_ada_dog_pool() {
        let p = decode_v2_pool_datum(&hex::decode(ADA_DOG).unwrap()).expect("decode");
        assert!(p.asset_a.is_ada(), "asset A is the ADA side");
        assert_eq!(p.asset_b.name, b"DOG".to_vec());
        assert_eq!(p.total_liquidity, 547_722_558);
        assert_eq!(p.reserve_a, 308_499_396);
        assert_eq!(p.reserve_b, 993_089_063);
        assert_eq!(p.fee_a_bps, 30);
        assert_eq!(p.fee_b_bps, 30);
    }

    #[test]
    fn the_datum_reserve_is_not_the_utxo_value() {
        // The pool this came from held 316,378,044 lovelace. Reading that as
        // the reserve overstates depth by the ADA deposit plus fees, and on a
        // dead pool overstates it by millions of times.
        let p = decode_v2_pool_datum(&hex::decode(ADA_DOG).unwrap()).unwrap();
        assert!(
            p.reserve_a < 316_378_044,
            "datum reserve must be below the UTxO value, never equal to it"
        );
    }

    #[test]
    fn ada_pair_orients_the_reserves() {
        let p = decode_v2_pool_datum(&hex::decode(ADA_DOG).unwrap()).unwrap();
        let (ada, token) = p.ada_pair().expect("ADA-paired");
        assert_eq!(ada, 308_499_396);
        assert_eq!(token, 993_089_063);
    }

    #[test]
    fn a_token_token_pool_has_no_ada_pair() {
        // Such a pool holds real supply but cannot price anything in ADA, and
        // saying so is the point — its lovelace is a carrier, not a reserve.
        let mut p = decode_v2_pool_datum(&hex::decode(ADA_DOG).unwrap()).unwrap();
        p.asset_a = AssetClass {
            policy: vec![1; 28],
            name: b"AAA".to_vec(),
        };
        assert!(p.ada_pair().is_none());
    }

    #[test]
    fn wrong_arity_is_rejected_rather_than_misread() {
        // A future contract version with a different field count must fail
        // closed; reading fields 4/5 positionally out of a different record
        // would produce confident nonsense.
        let short = "d8799fd8799f4040ffd8799f4040ff01ff";
        assert!(decode_v2_pool_datum(&hex::decode(short).unwrap()).is_none());
    }

    #[test]
    fn credentials_are_distinct_and_not_swapped() {
        assert!(is_minswap_v2(&V2_PAYMENT_CRED));
        assert!(is_minswap_v1(&V1_PAYMENT_CRED));
        assert!(!is_minswap_v2(&V1_PAYMENT_CRED));
        assert_ne!(V1_PAYMENT_CRED, V2_PAYMENT_CRED);
    }

    #[test]
    fn issued_lp_inverts_the_mint_max_pattern() {
        assert_eq!(
            issued_lp(u64::MAX),
            0,
            "nothing issued when the pool holds all"
        );
        assert_eq!(issued_lp(u64::MAX - 1_000), 1_000);
    }
}
