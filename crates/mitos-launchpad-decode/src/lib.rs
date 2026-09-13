//! Launchpad recognition and bonding-curve datum decode.
//!
//! # Why this is its own crate
//!
//! A launch decides a token's entire holder distribution in one transaction,
//! and every surface we run rendered it as a transfer to an unnamed script.
//! The consequences are not cosmetic: on $PERP the creator spent 9,440 ADA to
//! buy **70% of supply** from their own bonding curve inside the mint
//! transaction, and a holder chart shows that as "a wallet holds 70%".
//!
//! A bonding curve is not a DEX — its price is not constant-product, its
//! inventory has never been owned by anyone, and it has a published finish
//! line — so it lives beside `mitos-dex-decode` rather than inside it.
//!
//! Full chain evidence, including the graduation-rate census and the two
//! corrections this file's constants came out of, is in
//! `mitos/docs/design/SNEK_FUN_LAUNCH_LIFECYCLE.md`.
//!
//! # snek.fun, in three policy ids
//!
//! Recognise by the **policy ids**, not the addresses. The addresses moved
//! once already — $Dong (2024-09) graduated to Splash's `POOL_CRED_B` and
//! later tokens to `POOL_CRED_A` — while these three policies have been stable
//! across two years and every token checked.
//!
//! | marker | meaning |
//! |---|---|
//! | mint of [`BONDING_POOL_NFT_POLICY`] | a launch — in the token's OWN mint tx |
//! | burn of it | graduation |
//! | [`GRADUATED_POOL_NFT_POLICY`] present | the resulting AMM pool |
//!
//! ⚠️ **Only 1.82% of launches ever graduate** (425 burned of 23,335 minted,
//! full census 2026-09-08). The launchpad's own API answers only for those
//! 425, so anything that must see the other 22,910 has to come from chain.

use pallas_codec::minicbor;
use pallas_primitives::{BigInt, PlutusData};

/// Payment credential of snek.fun's bonding-curve contract.
///
/// ⚠️ It shares a STAKE credential with Splash's pool contract
/// (`b2f6abf6…`), which is how `shared-crates/address-registry` came to label
/// this address "DexHunter". Match on the PAYMENT credential; a stake-derived
/// name is wrong here and the error is not small — this contract receives up
/// to 96% of a token's supply at mint.
pub const BONDING_CURVE_CRED: [u8; 28] = [
    0x90, 0x5a, 0xb8, 0x69, 0x96, 0x1b, 0x09, 0x4f, 0x1b, 0x81, 0x97, 0x27, 0x8c, 0xfe, 0x15, 0xb4,
    0x5c, 0xbe, 0x49, 0xfa, 0x8f, 0x32, 0xc6, 0xb0, 0x14, 0xf8, 0x5a, 0x2d,
];

/// One NFT per launch, minted in the token's own mint transaction and burned
/// at graduation. Supply 1 means still bonding; 0 means graduated or closed.
pub const BONDING_POOL_NFT_POLICY: [u8; 28] = [
    0x63, 0xf9, 0x47, 0xb8, 0xd9, 0x53, 0x5b, 0xc4, 0xe4, 0xce, 0x69, 0x19, 0xe3, 0xdc, 0x05, 0x65,
    0x47, 0xe8, 0xd3, 0x0a, 0xda, 0x12, 0xf2, 0x9a, 0xa5, 0xf8, 0x26, 0xb8,
];

/// The pool NFT minted at graduation, on whichever Splash contract is current.
pub const GRADUATED_POOL_NFT_POLICY: [u8; 28] = [
    0xd8, 0xeb, 0x52, 0xca, 0xf3, 0x28, 0x9a, 0x28, 0x80, 0x28, 0x8b, 0x23, 0x14, 0x1c, 0xe3, 0xd2,
    0xa7, 0x02, 0x5d, 0xcf, 0x76, 0xf2, 0x6f, 0xd5, 0x65, 0x9a, 0xdd, 0x06,
];

/// The LP token minted at graduation.
///
/// MEASURED on $PERP: the LP asset's ENTIRE supply sits in the pool and
/// `asset_addresses` returns exactly one holder — the pool itself. No wallet
/// holds any, so the graduated liquidity cannot be withdrawn by anyone.
///
/// ⚠️ Do NOT derive that from the issued-LP base arithmetic. By the
/// `base − in_pool` convention the issued amount computes to 1,678,087,768 —
/// non-zero — because the contract mints `base − initial_lp` and simply never
/// mints the initial LP. The base arithmetic says 1.68e9 issued; the chain says
/// nobody has any. The reliable test is whether any address OTHER than the pool
/// holds this token.
pub const GRADUATED_LP_POLICY: [u8; 28] = [
    0x6e, 0x91, 0x7b, 0x8b, 0x96, 0x50, 0x78, 0xa3, 0x98, 0x04, 0xa6, 0x31, 0x3e, 0x5b, 0xe7, 0x35,
    0x35, 0x61, 0x24, 0x21, 0xac, 0xd7, 0x0a, 0xa8, 0x3f, 0x0e, 0xc2, 0x00,
];

/// The lovelace a freshly created curve holds before anybody buys.
///
/// The datum's `ada_cap_threshold` INCLUDES it, while snek.fun's API reports
/// the cap without it — measured as a difference of exactly 3,000,000 on the
/// 10,991,175,000 family. So bonding progress is
/// `(lovelace − SEED) / (cap − SEED)`.
pub const CURVE_SEED_LOVELACE: i64 = 3_000_000;

/// The curve's address, for consumers that match by address rather than by
/// credential. Its payment part is [`BONDING_CURVE_CRED`].
pub const BONDING_CURVE_ADDR: &str = "addr1xxg94wrfjcdsjncmsxtj0r87zk69e0jfl28n934sznu95tdj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrs2993lw";

pub fn is_snek_fun_curve(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &BONDING_CURVE_CRED
}

/// Payment credential of snek.fun's ORDER contract — where a buy or sell
/// waits before a batcher spends it into the curve.
///
/// # Why this is here and not in `mitos-dex-decode`
///
/// The curve is a launchpad, not a DEX, and its order contract belongs beside
/// it. `venue::SITES` deliberately covers DEX contracts only; the launchpad's
/// two credentials are registered together by the consumer.
///
/// # ⚠️ It is snek.fun's OWN contract, not an aggregator's
///
/// Established rather than assumed, because naming a router after one of the
/// venues it routes to would be exactly the mislabelling this workspace keeps
/// hitting. MEASURED: of the first **20 transactions that spend this
/// contract, 20 consume [`BONDING_CURVE_CRED`] and nothing else** — it routes
/// to one pool, so it is that pool's order contract.
///
/// # ⚠️ The STAKE part names the trader
///
/// Like Sundae and unlike CSwap, the trader's own stake credential is composed
/// onto this script: **144 outputs across 66 distinct addresses** on $Aliens.
/// So a snek.fun order leg can say who placed it.
///
/// Found from `1f489de2…` (a placement) and `7727a852…` (its fill).
pub const ORDER_CRED: [u8; 28] = [
    0xd9, 0x14, 0x3a, 0xc6, 0x34, 0x73, 0xb1, 0x7a, 0x21, 0x5d, 0x1b, 0x74, 0x84, 0xdf, 0xb6, 0xac,
    0x6b, 0x4a, 0x00, 0x05, 0xbe, 0xb0, 0xe2, 0x6a, 0x6c, 0xa0, 0x2c, 0x96,
];

pub fn is_snek_fun_order(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &ORDER_CRED
}

/// An asset as the bonding datum spells it. ADA is the empty policy with the
/// empty name — the same convention every DEX datum here uses.
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

/// A bonding pool, as its own datum publishes it.
///
/// `Constr 0` with exactly nine fields. Fields 0–2 and 6 are MEASURED — the
/// pool was looked up by its NFT, field 2's unit is the token actually in the
/// UTxO's value, and field 6 matches the cap the API reports plus the seed.
/// Fields 3 and 4 are inferred from magnitude and from matching the API's
/// `aNum`/`bNum` on the same pools.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BondingPool {
    /// The pool-instance key. One per launch.
    pub pool_nft: AssetClass,
    /// Datum field 1 — ADA on every pool observed.
    pub asset_x: AssetClass,
    /// Datum field 2 — the launched token.
    pub asset_y: AssetClass,
    /// Curve parameters. ⚠️ The curve they describe is UNRESOLVED — see
    /// [`BondingPool::graduation_market_cap`]. Carried, never evaluated.
    pub a_num: i64,
    pub b_num: i64,
    /// Lovelace at which the pool graduates, INCLUDING the seed.
    pub ada_cap_threshold: i64,
    /// Three 28-byte credentials whose roles are UNRESOLVED.
    ///
    /// Across 10 live pools field 5 had one distinct value, field 7 one, and
    /// field 8 two — so at least two are platform roles (executor, fee sink,
    /// admin) rather than the per-pool creator, which the API reports as
    /// varying per token. Named by position rather than guessed at: the
    /// creator's identity is recoverable from the launch transaction's output
    /// split, and does not need these.
    pub credential_5: Vec<u8>,
    pub credential_7: Vec<u8>,
    pub credential_8: Vec<u8>,
}

impl BondingPool {
    /// How far up the curve this pool is, as a fraction of its own cap.
    ///
    /// Both terms drop the seed, because the cap includes it and the pool
    /// starts holding it.
    pub fn progress(&self, lovelace: i64) -> Option<f64> {
        let span = self.ada_cap_threshold - CURVE_SEED_LOVELACE;
        (span > 0).then(|| (lovelace - CURVE_SEED_LOVELACE) as f64 / span as f64)
    }

    /// The market cap this pool graduates at, in lovelace, given the supply of
    /// the token and the amount still on the curve at the cap.
    ///
    /// The threshold is a MARKET CAP target and the ADA cap is back-solved from
    /// it — the numbers say so out loud. MEASURED: tier A graduates at
    /// **42,069.01 ADA** (420 and 69, exact to 0.00002% across every tier-A
    /// token) and tier B at **69,016.55 ADA**, reproduced on 12 of 12 further
    /// graduated pools sampled at random.
    ///
    /// ⚠️ This is the cap priced off what the curve COLLECTED. The Splash pool
    /// that results opens ~1.88% lower — 41,276.50 ADA on tier A — because a
    /// flat ~209.58 ADA executor fee leaves in the graduation transaction.
    /// Rendering one beside the other without naming both reads as an
    /// arithmetic bug rather than a fee.
    /// ⚠️ The cap is taken NET OF THE SEED. Using the datum's figure whole puts
    /// tier A at 42,080.50 ADA instead of 42,069.01 — close enough to look
    /// right and wrong enough to miss that the number is deliberate. The seed
    /// is lovelace the pool was created holding, not money the curve took.
    pub fn graduation_market_cap(&self, supply: i64, tokens_left_at_cap: i64) -> Option<i64> {
        let collected = self.ada_cap_threshold - CURVE_SEED_LOVELACE;
        (tokens_left_at_cap > 0 && collected > 0)
            .then(|| ((collected as i128 * supply as i128) / tokens_left_at_cap as i128) as i64)
    }
}

/// Decode a bonding-pool datum.
///
/// Returns `None` on anything that is not the nine-field shape, rather than
/// reading a shorter constructor positionally — the way a decoder starts
/// returning confident nonsense.
pub fn decode_bonding_datum(datum_cbor: &[u8]) -> Option<BondingPool> {
    let pd: PlutusData = minicbor::decode(datum_cbor).ok()?;
    let fields = match &pd {
        PlutusData::Constr(c) if c.fields.len() == 9 => &c.fields,
        _ => return None,
    };
    Some(BondingPool {
        pool_nft: asset_class(&fields[0])?,
        asset_x: asset_class(&fields[1])?,
        asset_y: asset_class(&fields[2])?,
        a_num: int(&fields[3])?,
        b_num: int(&fields[4])?,
        ada_cap_threshold: int(&fields[6])?,
        credential_5: bytes(&fields[5])?,
        credential_7: bytes(&fields[7])?,
        credential_8: bytes(&fields[8])?,
    })
}

/// `Constr _ [policy, name]`.
fn asset_class(pd: &PlutusData) -> Option<AssetClass> {
    match pd {
        PlutusData::Constr(c) if c.fields.len() == 2 => Some(AssetClass {
            policy: bytes(&c.fields[0])?,
            name: bytes(&c.fields[1])?,
        }),
        _ => None,
    }
}

fn bytes(pd: &PlutusData) -> Option<Vec<u8>> {
    match pd {
        PlutusData::BoundedBytes(b) => Some((**b).to_vec()),
        _ => None,
    }
}

fn int(pd: &PlutusData) -> Option<i64> {
    match pd {
        PlutusData::BigInt(BigInt::Int(i)) => i64::try_from(i128::from(*i)).ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A LIVE bonding pool's inline datum, copied from the chain UTxO holding
    /// pool NFT `69915116…` (token "RIDDLE", still bonding). Copied, never
    /// retyped — a hand-transcribed byte once shifted a WingRiders treasury by
    /// 512 and only a golden test caught it.
    const RIDDLE_DATUM: &str = "d87989d87982581c63f947b8d9535bc4e4ce6919e3dc056547e8d30ada12f29aa5f826b8582069915116eacdce5df6a00416cf2e165fc2b0f276aa82af52bb8cc120853aaf9fd879824040d87982581cdf7584ce88bd504c9b345aa7e39f2c8981547ac9e5ceb875a865de6f46524944444c451b000000116fa7491e1a001312d0581c5cb2c968e5d1c7197a6ce7615967310a375545d9bc65063a964335b21b000000028fd474c0581cd112cf99ea4239c905c3828c233ff94d7cd893e3386128eafef05478581c13f16ea9438a85cbf59a44db0439ac7727c2753e2010f614b00f4902";

    fn riddle() -> BondingPool {
        decode_bonding_datum(&hex::decode(RIDDLE_DATUM).unwrap())
            .expect("a live bonding datum must decode")
    }

    #[test]
    fn the_datum_names_the_pool_and_both_sides() {
        let p = riddle();
        assert_eq!(p.pool_nft.policy, BONDING_POOL_NFT_POLICY.to_vec());
        assert_eq!(
            hex::encode(&p.pool_nft.name),
            "69915116eacdce5df6a00416cf2e165fc2b0f276aa82af52bb8cc120853aaf9f"
        );
        // ADA is the empty policy with the empty name — the pool's own
        // spelling, and the convention the observations tier reuses.
        assert!(p.asset_x.is_ada());
        assert_eq!(
            hex::encode(&p.asset_y.policy),
            "df7584ce88bd504c9b345aa7e39f2c8981547ac9e5ceb875a865de6f"
        );
        assert_eq!(p.asset_y.name, b"RIDDLE".to_vec());
        assert!(!p.asset_y.is_ada());
    }

    #[test]
    fn the_curve_parameters_and_cap_come_off_the_datum() {
        let p = riddle();
        assert_eq!(p.a_num, 74_887_678_238);
        assert_eq!(p.b_num, 1_250_000);
        assert_eq!(p.ada_cap_threshold, 11_003_000_000);
        // Three credentials whose roles are unresolved; carried verbatim so a
        // later reader can settle them without a re-walk.
        assert_eq!(
            hex::encode(&p.credential_5),
            "5cb2c968e5d1c7197a6ce7615967310a375545d9bc65063a964335b2"
        );
        assert_eq!(
            hex::encode(&p.credential_7),
            "d112cf99ea4239c905c3828c233ff94d7cd893e3386128eafef05478"
        );
        assert_eq!(
            hex::encode(&p.credential_8),
            "13f16ea9438a85cbf59a44db0439ac7727c2753e2010f614b00f4902"
        );
    }

    /// RIDDLE's live UTxO holds 18,000,001 lovelace — 15 ADA of buying above a
    /// 3 ADA seed, against an 11,000 ADA span.
    #[test]
    fn progress_discounts_the_seed_at_both_ends() {
        let p = riddle();
        let progress = p.progress(18_000_001).unwrap();
        assert!(
            (progress - 0.001_363_6).abs() < 1e-6,
            "expected ~0.136% of the way up, got {progress}"
        );
        // A pool nobody has bought from is at zero, not at the seed's share.
        assert_eq!(p.progress(CURVE_SEED_LOVELACE), Some(0.0));
    }

    /// The threshold is a MARKET CAP target and the ADA cap is back-solved from
    /// it. $PERP graduated with 261,194,031 of 1,000,000,000 left on the curve
    /// against a datum cap of 10,991,175,000 — which is 42,069 ADA, 420 and 69,
    /// and not a number a curve lands on by accident.
    #[test]
    fn tier_a_graduates_at_the_meme_market_cap() {
        let perp = BondingPool {
            ada_cap_threshold: 10_991_175_000,
            ..riddle()
        };
        let mcap = perp
            .graduation_market_cap(1_000_000_000, 261_194_031)
            .unwrap();
        assert_eq!(mcap, 42_069_012_672, "42,069.01 ADA");

        // Netting the seed is what makes it land. Taken whole the same pool
        // reads 42,080.50 — close enough to look right.
        let naive = (perp.ada_cap_threshold as i128 * 1_000_000_000i128) / 261_194_031i128;
        assert_eq!(naive as i64, 42_080_498_386);
    }

    #[test]
    fn the_bonding_curve_credential_is_matched_on_payment_not_stake() {
        assert!(is_snek_fun_curve(&BONDING_CURVE_CRED));
        // Splash's pool credential shares this contract's STAKE credential and
        // is a different contract; naming by stake is what produced the
        // "DexHunter" mislabel in `shared-crates/address-registry`.
        let splash_pool_cred: [u8; 28] = [
            0xcb, 0x68, 0x4a, 0x69, 0xe7, 0x89, 0x07, 0xa9, 0x79, 0x6b, 0x21, 0xfc, 0x15, 0x0a,
            0x75, 0x8a, 0xf5, 0xf2, 0x80, 0x5e, 0x5e, 0xd5, 0xd5, 0xa8, 0xce, 0x9f, 0x76, 0xf1,
        ];
        assert!(!is_snek_fun_curve(&splash_pool_cred));
    }

    /// A shorter constructor is refused rather than read positionally.
    #[test]
    fn a_datum_of_the_wrong_shape_is_declined() {
        assert!(decode_bonding_datum(&[]).is_none());
        // `Constr 0 [Int 1]` — valid PlutusData, not a bonding pool.
        assert!(decode_bonding_datum(&hex::decode("d8799f01ff").unwrap()).is_none());
    }

    /// Both snek.fun credentials, pinned as HEX so they can be checked against
    /// a block explorer — the only way anyone will ever verify them.
    ///
    /// ⚠️ The CURVE and the ORDER are different contracts. Registering one as
    /// the other turns every placement into a swap, or every swap into an
    /// intention, from the same movement.
    #[test]
    fn the_curve_and_the_order_are_distinct_contracts() {
        assert_eq!(
            hex::encode(BONDING_CURVE_CRED),
            "905ab869961b094f1b8197278cfe15b45cbe49fa8f32c6b014f85a2d",
        );
        assert_eq!(
            hex::encode(ORDER_CRED),
            "d9143ac63473b17a215d1b7484dfb6ac6b4a0005beb0e26a6ca02c96",
        );
        assert_ne!(BONDING_CURVE_CRED, ORDER_CRED);
        assert!(is_snek_fun_curve(&BONDING_CURVE_CRED) && !is_snek_fun_order(&BONDING_CURVE_CRED));
        assert!(is_snek_fun_order(&ORDER_CRED) && !is_snek_fun_curve(&ORDER_CRED));
    }
}
