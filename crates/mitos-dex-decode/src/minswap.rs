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
//! ## V1 and V2 were SWAPPED in `shared-crates/address-registry` — FIXED
//!
//! Verified on chain 2026-06-24: the credential labelled "Minswap V2" there
//! (`e1317b15…`) is actually V1, and the one labelled "Minswap" (`ea07b733…`)
//! is V2. The constants below were correct and the registry inherited the
//! swap.
//!
//! Corrected in that crate on 2026-09-08, by re-deriving each credential from
//! its own address prefix rather than trusting either file. Note the labels
//! there never carried a version — only the comments did — so the error was
//! invisible to a lookup and misleading only to a reader.

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

/// Policy of the V1 pool NFT — one token per pool, so its NAME is the
/// pool-instance key. Present in every sampled V1 pool UTxO.
pub const V1_POOL_NFT_POLICY: [u8; 28] = [
    0x0b, 0xe5, 0x5d, 0x26, 0x2b, 0x29, 0xf5, 0x64, 0x99, 0x8f, 0xf8, 0x1e, 0xfe, 0x21, 0xbd, 0xc0,
    0x02, 0x26, 0x21, 0xc1, 0x2f, 0x15, 0xaf, 0x08, 0xd0, 0xf2, 0xdd, 0xb1,
];

/// Policy of the `MINSWAP` factory token every V1 pool also carries.
///
/// Both this and [`V1_POOL_NFT_POLICY`] sit in the pool UTxO's value and are
/// **not reserves**. A V1 pool holds exactly: its NFT, this token, and the two
/// sides — so netting these two out leaves the reserves.
pub const V1_FACTORY_POLICY: [u8; 28] = [
    0x13, 0xaa, 0x2a, 0xcc, 0xf2, 0xe1, 0x56, 0x17, 0x23, 0xaa, 0x26, 0x87, 0x1e, 0x07, 0x1f, 0xdf,
    0x32, 0xc8, 0x67, 0xcf, 0xf7, 0xe7, 0xd5, 0x0a, 0xd4, 0x70, 0xd6, 0x2f,
];

/// Decoded Minswap **V1** pool datum (`Constr 0`, 5 fields):
///
/// ```text
/// [0] assetA   [1] assetB   [2] totalLiquidity   [3] rootKLast
/// [4] profitSharing (Option)
/// ```
///
/// ## V1 publishes no reserves
///
/// Unlike V2, which carries `reserveA`/`reserveB` in the datum, V1 carries only
/// the LP total. Reserves come from the UTxO's **value**, less the pool NFT and
/// the factory token — see [`V1_POOL_NFT_POLICY`].
///
/// ## Whether a fixed ADA deposit should also come off is UNRESOLVED
///
/// It is widely said that V1 pools hold a fixed deposit. Testing it across the
/// sampled ADA pools was **inconclusive**: the constant-product residual is
/// flat from 0 to 4.5 ADA (median ~23% either way), because these pools are
/// years old and LP value has drifted far more than a few ADA. So no deposit is
/// subtracted here. That understates nothing on a real pool — a few ADA against
/// hundreds — and the alternative is inventing a constant the data does not
/// support. It matters only on a dead pool, where the depth is noise anyway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolV1 {
    pub asset_a: AssetClass,
    pub asset_b: AssetClass,
    pub total_liquidity: u64,
}

impl PoolV1 {
    /// `(ada_reserve, token_reserve)` given the two sides' holdings, or `None`
    /// for a token/token pool — which holds real supply but cannot price it.
    pub fn ada_pair(&self, value_a: u64, value_b: u64) -> Option<(u64, u64)> {
        if self.asset_a.is_ada() {
            Some((value_a, value_b))
        } else if self.asset_b.is_ada() {
            Some((value_b, value_a))
        } else {
            None
        }
    }

    /// Is this policy one of the two the pool carries for its own bookkeeping
    /// rather than as a reserve?
    pub fn is_overhead_policy(policy: &[u8]) -> bool {
        policy == V1_POOL_NFT_POLICY || policy == V1_FACTORY_POLICY
    }
}

/// Decode a Minswap V1 pool datum.
///
/// V1 is PlutusV1, so this CBOR comes from the creating transaction's witness
/// set rather than from an inline datum — see the crate docs on datum hashes.
pub fn decode_v1_pool_datum(cbor: &[u8]) -> Option<PoolV1> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let c = as_constr(&pd)?;
    let fields: Vec<&PlutusData> = c.fields.iter().collect();
    if fields.len() != 5 {
        return None;
    }
    Some(PoolV1 {
        asset_a: asset_class(fields.first().copied()?)?,
        asset_b: asset_class(fields.get(1).copied()?)?,
        total_liquidity: int_at(&fields, 2)?,
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

    /// A real ADA/CHIMPY **V1** pool at `57e81fa7…#0`, captured 2026-08-30.
    ///
    /// The UTxO held 718,437,917 lovelace and 1,356,132,463,270 CHIMPY,
    /// alongside its pool NFT and the `MINSWAP` factory token. The datum's
    /// `totalLiquidity` of 30,955,673,379 sits within 0.8% of
    /// `sqrt(718,437,917 × 1,356,132,463,270)`, which is the check that the
    /// value — not some subset of it — is the reserve.
    const V1_CHIMPY: &str = "d8799fd8799f4040ffd8799f581cff791cdf3857627970df7f7930bfb7c8eee3ce45df43860d68b9ef60464348494d5059ff1b00000007351a17231b0000000744785751d8799fd8799fd8799fd8799f581caafb1196434cb837fd6f21323ca37b302dff6387e8a84b3fa28faf56ffd8799fd8799fd8799f581c52563c5410bff6a0d43ccebb7c37e1f69f5eb260552521adff33b9c2ffffffffd87a80ffffff";

    fn v1_chimpy() -> PoolV1 {
        decode_v1_pool_datum(&hex::decode(V1_CHIMPY).unwrap()).expect("decode")
    }

    #[test]
    fn decodes_a_real_v1_pool() {
        let p = v1_chimpy();
        assert!(p.asset_a.is_ada());
        assert_eq!(p.asset_b.name, b"CHIMPY".to_vec());
        assert_eq!(p.total_liquidity, 30_955_673_379);
    }

    #[test]
    fn v1_reserves_come_from_the_value_and_satisfy_constant_product() {
        // V1 publishes no reserves, so this is the evidence that the value is
        // the right source: the pool's own totalLiquidity should be
        // sqrt(a*b), and it is, to 0.8%.
        let p = v1_chimpy();
        let (ada, tok) = p.ada_pair(718_437_917, 1_356_132_463_270).unwrap();
        let root = (ada as u128 * tok as u128).isqrt() as u64;
        let err = root.abs_diff(p.total_liquidity) as f64 / p.total_liquidity as f64;
        assert!(err < 0.02, "value-sourced reserves fit sqrt(a*b): {err}");
    }

    #[test]
    fn the_v1_nft_and_factory_tokens_are_not_reserves() {
        // A V1 pool holds exactly four things; two of them are bookkeeping.
        // Counting either as a reserve would put a quantity of 1 on a side.
        assert!(PoolV1::is_overhead_policy(&V1_POOL_NFT_POLICY));
        assert!(PoolV1::is_overhead_policy(&V1_FACTORY_POLICY));
        assert!(!PoolV1::is_overhead_policy(&V2_AUTHEN_POLICY));
    }

    #[test]
    fn a_v1_token_token_pool_reports_no_ada_pair() {
        let mut p = v1_chimpy();
        p.asset_a.policy = vec![0xaa; 28];
        p.asset_a.name = b"A".to_vec();
        assert!(p.ada_pair(1, 2).is_none());
    }

    #[test]
    fn the_v1_and_v2_datums_do_not_decode_as_each_other() {
        // Both are `Constr 0`; only the arity separates them, so a decoder
        // that tolerated arity would silently read V1 fields at V2 offsets.
        let v1 = hex::decode(V1_CHIMPY).unwrap();
        assert!(decode_v2_pool_datum(&v1).is_none());
        let v2 = hex::decode(ADA_DOG).unwrap();
        assert!(decode_v1_pool_datum(&v2).is_none());
    }

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
