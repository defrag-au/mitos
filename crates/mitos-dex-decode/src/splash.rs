//! Splash DEX decode surface — pool credentials and the pool datum.
//!
//! ## There is more than one Splash pool contract
//!
//! This file used to say *"Single canonical bech32 — every Splash pool lives
//! here."* That is false, and it was falsified by a token rather than by
//! reading: $Dong's Splash pool sits at a **different payment credential**
//! carrying the same Splash stake credential, so an exact-address rule found
//! nothing and 98,352,422 tokens — 9.8% of supply, and the venue holding ~84%
//! of the token's routable liquidity — surfaced as an unidentified script
//! holder.
//!
//! Both credentials host many pools, and their datums are the same shape at
//! different arities (15 and 11 fields observed). So recognition matches a
//! **set of payment credentials**, and the decoder reads only the stable
//! prefix rather than pinning a length.
//!
//! Versions are deliberately not numbered here. The contracts are
//! distinguishable and that is all a consumer needs; inventing "V3"/"V4"
//! labels we cannot verify would be a guess dressed as provenance — and the
//! neighbouring `address-registry` already has Minswap's V1/V2 labels the
//! wrong way round for exactly that reason.

use pallas_codec::minicbor;
use pallas_primitives::{Constr, PlutusData};

use crate::bounded_bytes;

/// The pool address this crate has always known — `addr1x…`, script payment
/// plus script stake.
pub const POOL_SCRIPT_ADDR: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";

/// Payment credential of [`POOL_SCRIPT_ADDR`].
pub const POOL_CRED_A: [u8; 28] = [
    0xcb, 0x68, 0x4a, 0x69, 0xe7, 0x89, 0x07, 0xa9, 0x79, 0x6b, 0x21, 0xfc, 0x15, 0x0a, 0x75, 0x8a,
    0xf5, 0xf2, 0x80, 0x5e, 0x5e, 0xd5, 0xd5, 0xa8, 0xce, 0x9f, 0x76, 0xf1,
];

/// A second pool contract, found holding $Dong 2026-08-30.
///
/// Confirmed Splash structurally, not by association: its datum carries the
/// same four leading fields as [`POOL_CRED_A`]'s — pool NFT, ADA, the token,
/// LP token — with Splash's `<TOKEN>_ADA_NFT` / `<TOKEN>_ADA_LQ` naming. The
/// shared stake credential alone would NOT have been enough, since the
/// snek.fun launchpad shares it too and is not a pool.
pub const POOL_CRED_B: [u8; 28] = [
    0x9d, 0xee, 0x06, 0x59, 0x68, 0x6c, 0x3a, 0xb8, 0x07, 0x89, 0x5c, 0x92, 0x9e, 0x32, 0x84, 0xc1,
    0x12, 0x22, 0xaf, 0xfd, 0x71, 0x0b, 0x09, 0xbe, 0x69, 0x0f, 0x92, 0x4d,
];

/// Every known Splash pool payment credential.
pub const POOL_CREDS: [[u8; 28]; 2] = [POOL_CRED_A, POOL_CRED_B];

pub fn is_splash_pool(payment_cred: &[u8; 28]) -> bool {
    POOL_CREDS.contains(payment_cred)
}

/// SpotOrder script address prefix (51 chars = `addr1z` + header byte +
/// 28-byte payment hash worth of bech32). Each user's order glues its own
/// stake credential to the same payment script, so consumers prefix-match.
pub const ORDER_SCRIPT_ADDR_PREFIX: &str = "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q";

/// Payment credential of [`ORDER_SCRIPT_ADDR_PREFIX`].
///
/// A consumer holding a raw credential cannot prefix-match a bech32 string —
/// `policy-archive`'s movement rows are the case in point. Measured on $PERP:
/// **571 distinct order addresses**, one per trader, all this one credential.
///
/// The stake part is the CUSTOMER's, and on $PERP **571 of 571** of those
/// stakes also appear as an ordinary wallet holder, so a fill spent from here
/// names its own trader.
pub const ORDER_CRED: [u8; 28] = [
    0x46, 0x4e, 0xee, 0xe8, 0x9f, 0x05, 0xaf, 0xf7, 0x87, 0xd4, 0x00, 0x45, 0xaf, 0x2a, 0x40, 0xa8,
    0x3f, 0xd9, 0x6c, 0x51, 0x31, 0x97, 0xd3, 0x2f, 0xbc, 0x54, 0xff, 0x02,
];

pub fn is_splash_order(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &ORDER_CRED
}

/// An asset as Splash encodes it: `Constr 121 [policy, name]`. ADA is the
/// empty policy with the empty name.
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

/// A Splash pool's published identity.
///
/// Only the stable prefix is decoded. The trailing fields are fee and
/// treasury parameters that differ between the two contracts, and reading them
/// positionally across arities is how a decoder starts returning confident
/// nonsense.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplashPool {
    /// The pool NFT — one per pool, so this is the pool-INSTANCE key.
    pub pool_nft: AssetClass,
    pub asset_a: AssetClass,
    pub asset_b: AssetClass,
    /// The LP token, whose holders own the reserves.
    pub lp_token: AssetClass,
}

impl SplashPool {
    /// `true` when one side is ADA, so the pool can price its token.
    pub fn is_ada_paired(&self) -> bool {
        self.asset_a.is_ada() || self.asset_b.is_ada()
    }

    /// The non-ADA side, if this is an ADA pool.
    pub fn token_side(&self) -> Option<&AssetClass> {
        if self.asset_a.is_ada() {
            Some(&self.asset_b)
        } else if self.asset_b.is_ada() {
            Some(&self.asset_a)
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

fn asset_class(pd: &PlutusData) -> Option<AssetClass> {
    let c = as_constr(pd)?;
    let f: Vec<&PlutusData> = c.fields.iter().collect();
    Some(AssetClass {
        policy: bounded_bytes(f.first().copied()?)?,
        name: bounded_bytes(f.get(1).copied()?)?,
    })
}

/// Decode a Splash pool datum.
///
/// Accepts any arity carrying the four leading asset fields — 15 and 11 have
/// both been seen on chain, and pinning either would reject the other's pools
/// entirely.
pub fn decode_pool_datum(cbor: &[u8]) -> Option<SplashPool> {
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let c = as_constr(&pd)?;
    let fields: Vec<&PlutusData> = c.fields.iter().collect();
    if fields.len() < 4 {
        return None;
    }
    Some(SplashPool {
        pool_nft: asset_class(fields.first().copied()?)?,
        asset_a: asset_class(fields.get(1).copied()?)?,
        asset_b: asset_class(fields.get(2).copied()?)?,
        lp_token: asset_class(fields.get(3).copied()?)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real pool at [`POOL_CRED_A`] — CAT/ADA, 15 fields.
    const CRED_A_CAT: &str = "d8798fd87982581c0903babb113f583e4fde49b6bfcd9ca5971b624f71a67326de9eac064b4341545f4144415f4e4654d879824040d87982581c3698fedda7a6ef2db3d71594107b4200a524857bb262d76ac0d160c843434154d87982581cea7501e84ed53b1c08c52fc65f066570460c02198aaa4cbb8484b77d4a4341545f4144415f4c511a000173181901f419138819c3501913881a0007a12019c3509fd8799fd87a9f581c66e711a4bf9ddf46ff239143870b6893055a4fd4dea9f99fed6665cdffffff581c75c4570eb625ae881b32a34c52b159f6f3f3f2c7aaabf5bac468813358200911bfd7e207b18324ee51cae9517834a619f1ece02467df818a6cbc0f371f5c00";

    #[test]
    fn decodes_the_stable_prefix_whatever_the_arity() {
        let p = decode_pool_datum(&hex::decode(CRED_A_CAT).unwrap()).expect("decode");
        assert_eq!(p.pool_nft.name, b"CAT_ADA_NFT".to_vec());
        assert!(p.asset_a.is_ada(), "field 1 is the ADA side");
        assert_eq!(p.asset_b.name, b"CAT".to_vec());
        assert_eq!(p.lp_token.name, b"CAT_ADA_LQ".to_vec());
    }

    #[test]
    fn an_ada_pool_reports_its_token_side() {
        let p = decode_pool_datum(&hex::decode(CRED_A_CAT).unwrap()).unwrap();
        assert!(p.is_ada_paired());
        assert_eq!(p.token_side().unwrap().name, b"CAT".to_vec());
    }

    #[test]
    fn both_pool_contracts_are_recognised() {
        // The whole point: one credential was known, the other was found
        // holding 9.8% of a token's supply and read as an unnamed script.
        assert!(is_splash_pool(&POOL_CRED_A));
        assert!(is_splash_pool(&POOL_CRED_B));
        assert_ne!(POOL_CRED_A, POOL_CRED_B);
    }

    #[test]
    fn a_short_record_is_rejected_rather_than_half_read() {
        let short = "d87983d879824040d879824040d879824040";
        assert!(decode_pool_datum(&hex::decode(short).unwrap()).is_none());
    }
}
