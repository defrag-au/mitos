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

// ============================================================================
// LumpFun — the v5 "ada-order" cohort
// ============================================================================
//
// ⚠️ **NOT LumpPad.** Both sit under the lumpfun.com domain family
// (`lumppad.lumpfun.com` is LumpPad), and they are different protocols: LumpPad
// quotes in LUMP and publishes a `Constr 2` pool datum with eleven fields,
// while LumpFun quotes in ADA and publishes a `Constr 3` curve datum with six.
// Nothing here decodes a LumpPad pool and nothing in `LUMPPAD_INTEGRATION.md`
// applies to it. See `cnft.dev-workers/docs/design/LUMPFUN_V5.md`.
//
// The curve validator is parameterised PER LAUNCH (policy, name, creator, fee
// bps, treasury, fee routing, graduation threshold), so — unlike snek.fun — it
// has no shared credential to match on. What identifies a curve is its STATE
// NFT, `<policy>.000643b0504f4f4c` under the launched token's own policy.

/// The supply every LumpFun launch mints, exactly once, with 0 decimals.
///
/// Load-bearing rather than decorative: [`LumpFunCurve::virtual_ada`] recovers
/// the curve's compiled-in ADA offset from one state by measuring how far the
/// token side has come from this figure. A cohort that minted a different
/// supply would derive a different offset — silently, because the arithmetic
/// stays self-consistent. It is pinned by the one-shot mint policy, which is
/// verifiable from any launch transaction.
pub const LUMPFUN_SUPPLY: i64 = 1_000_000_000;

/// The lovelace a LumpFun pool always holds that is NOT on the curve: the pool
/// UTxO's own min-ADA. Observed on the launch, on buys and on a sell:
/// `lovelace == LUMPFUN_POOL_MIN_LOVELACE + ada_reserve + platform + creator`.
pub const LUMPFUN_POOL_MIN_LOVELACE: i64 = 3_000_000;

/// The state NFT's asset name: CIP-67 label 100 (`000643b0`) + `POOL`.
/// One per pool, under the token's policy.
pub const LUMPFUN_STATE_NFT_NAME: [u8; 8] = [0x00, 0x06, 0x43, 0xb0, b'P', b'O', b'O', b'L'];

/// Payment credential of LumpFun's shared order contract — where a buy or sell
/// waits for the settler that spends it into the curve.
///
/// ⚠️ **One address for every trader.** The order datum names the owner, but
/// the address's payment part is this script and it has no stake part, so a
/// movement through it cannot name who placed the order. Register it as a
/// shared order contract, never as a per-customer one.
///
/// MEASURED: every v5 trade found on the Dust Bunny pool (`a1b085f4…`) spends
/// this contract alongside the curve. `POST /api/tx/build` with `action:buy`
/// answers `"order": true` for a v5 token, and `order-buy` is rejected as
/// unsupported — so a v5 buy is an order placement, not a direct spend.
pub const LUMPFUN_ORDER_CRED: [u8; 28] = [
    0x23, 0xc8, 0x70, 0xa9, 0x6e, 0xf1, 0x34, 0x3a, 0x38, 0xc6, 0x1a, 0x0d, 0x56, 0x6c, 0x9d, 0x39,
    0xd1, 0x30, 0xd5, 0x3e, 0xaa, 0x15, 0x17, 0xca, 0x92, 0xf6, 0x0e, 0xb4,
];

pub fn is_lumpfun_order(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &LUMPFUN_ORDER_CRED
}

/// A LumpFun curve, as its pool datum publishes it.
///
/// `Constr 3` with exactly six fields. All six are MEASURED on chain — the
/// launch, two buys and a sell — rather than inferred from a bundle:
///
/// ```text
/// Constr 3 [ policy, name, ada_reserve, token_reserve,
///            platform_accrued, creator_accrued ]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LumpFunCurve {
    pub policy: Vec<u8>,
    pub name: Vec<u8>,
    /// Lovelace on the curve, net of [`LUMPFUN_POOL_MIN_LOVELACE`] and of both
    /// fee buckets.
    pub ada_reserve: i64,
    /// The curve's TOKEN side. ⚠️ **Virtual**: it includes tokens the curve
    /// prices against but does not hold, so it is larger than the pool UTxO's
    /// token quantity by a constant that is fixed per launch. The difference is
    /// what [`Self::virtual_tokens`] returns.
    pub token_reserve: i64,
    /// Platform fees accrued inside the pool UTxO and not yet claimed.
    pub platform_accrued: i64,
    /// Creator fees accrued inside the pool UTxO and not yet claimed.
    pub creator_accrued: i64,
}

impl LumpFunCurve {
    /// Tokens the curve prices against but does not hold.
    ///
    /// Exact, from one output: the datum's token side minus the quantity the
    /// pool UTxO actually holds. MEASURED on Dust Bunny: 125,337,602 at every
    /// state inspected.
    pub fn virtual_tokens(&self, held: i64) -> Option<i64> {
        ((0..=LUMPFUN_SUPPLY).contains(&held) && self.token_reserve >= held)
            .then(|| self.token_reserve - held)
    }

    /// The curve's virtual ADA offset, recovered from ONE state.
    ///
    /// The curve is a constant product over `(ada_reserve + V, token_reserve)`
    /// and it begins at `(0, LUMPFUN_SUPPLY + virtual_tokens)`. For any state,
    /// then:
    ///
    /// ```text
    /// V * (supply + virtual_tokens) = (ada_reserve + V) * token_reserve
    /// V * (supply - held)           = ada_reserve * token_reserve
    /// ```
    ///
    /// because `token_reserve = held + virtual_tokens` and `supply - held` is
    /// everything the curve has sold. That is the whole derivation: no cohort
    /// constant, no history, and no dependence on a second observation.
    ///
    /// ⚠️ **Recovered, not exact.** The validator floors every trade, so the
    /// state sits a little off the exact curve and the recovered value is a
    /// little above the compiled one. MEASURED on Dust Bunny: 5,000,000,130
    /// from the fill state and 5,000,000,482 from the launch state, against the
    /// 5,000,000,000 the cohort compiled in. The price is accurate to ~1e-8
    /// relative — far below a hundredth of a token at these sizes — but code
    /// that BUILDS A TRANSACTION must use the compiled value, because the
    /// validator recomputes the quote with it.
    pub fn virtual_ada(&self, held: i64) -> Option<i64> {
        self.virtual_tokens(held)?;
        let sold = LUMPFUN_SUPPLY - held;
        if self.ada_reserve <= 0 || sold <= 0 {
            return None;
        }
        let v = (self.ada_reserve as i128) * (self.token_reserve as i128) / (sold as i128);
        let v = i64::try_from(v).ok()?;
        (v > 0).then_some(v)
    }

    /// The curve's EFFECTIVE sides for `x·y = k`: `(tokens, lovelace)`.
    ///
    /// These are the numbers a constant-product price and a price-impact
    /// figure are computed from. They are not the pool UTxO's balances — see
    /// [`Self::virtual_tokens`] and [`Self::virtual_ada`] — which is why a
    /// consumer that reports them must say so.
    ///
    /// `None` before the curve's first buy: with `ada_reserve == 0` there is no
    /// offset to recover and therefore no price. That is undefined, not zero.
    pub fn effective(&self, held: i64) -> Option<(i64, i64)> {
        let virtual_ada = self.virtual_ada(held)?;
        Some((
            self.token_reserve,
            self.ada_reserve.checked_add(virtual_ada)?,
        ))
    }
}

/// Decode a LumpFun curve datum.
///
/// Returns `None` on anything that is not `Constr 3` with six fields, rather
/// than reading a different constructor positionally — the way a decoder starts
/// returning confident nonsense. A LumpPad pool datum (`Constr 2`, eleven
/// fields) is declined here by design.
pub fn decode_lumpfun_datum(datum_cbor: &[u8]) -> Option<LumpFunCurve> {
    let pd: PlutusData = minicbor::decode(datum_cbor).ok()?;
    let c = match &pd {
        PlutusData::Constr(c)
            if c.fields.len() == 6 && (c.any_constructor == Some(3) || c.tag == 124) =>
        {
            c
        }
        _ => return None,
    };
    Some(LumpFunCurve {
        policy: bytes(c.fields.first()?)?,
        name: bytes(c.fields.get(1)?)?,
        ada_reserve: int(c.fields.get(2)?)?,
        token_reserve: int(c.fields.get(3)?)?,
        platform_accrued: int(c.fields.get(4)?)?,
        creator_accrued: int(c.fields.get(5)?)?,
    })
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

    // ------------------------------------------------------------------------
    // LumpFun — golden states from the Dust Bunny pool `a1b085f4…`
    // ------------------------------------------------------------------------

    /// The pool's inline datum in the sample buy `1f7e8e57…`, copied from the
    /// chain (Koios `datum_info`, hash `aaaff6e9…`). Never retyped: a
    /// hand-transcribed byte once shifted a WingRiders treasury by 512 and only
    /// a golden test caught it.
    const BUNNY_AFTER: &str = "d87c9f581ca1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba2934542554e4e591a5caf6ac01a3329db221a001e84801a001e8480ff";

    /// The pool's inline datum in the launch transaction `b8f6252d…`, with the
    /// creator's 25 ADA first buy already applied (hash `27024edb…`).
    const BUNNY_LAUNCH: &str = "d87c9f581ca1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba2934542554e4e591a017d78401a42bddc1c1a0003d0901a0003d090ff";

    /// How much of the token the pool UTxO held at that state — read from the
    /// output, not from the datum.
    const BUNNY_HELD: i64 = 733_043_488;
    const BUNNY_HELD_AT_LAUNCH: i64 = 994_401_306;

    fn bunny() -> LumpFunCurve {
        decode_lumpfun_datum(&hex::decode(BUNNY_AFTER).unwrap()).expect("a live curve datum")
    }

    #[test]
    fn the_curve_datum_names_the_token_and_all_six_fields() {
        let c = bunny();
        assert_eq!(
            hex::encode(&c.policy),
            "a1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba293"
        );
        assert_eq!(c.name, b"BUNNY");
        assert_eq!(c.ada_reserve, 1_555_000_000);
        assert_eq!(c.token_reserve, 858_381_090);
        assert_eq!(c.platform_accrued, 2_000_000);
        assert_eq!(c.creator_accrued, 2_000_000);
        // The invariant every LumpFun pool state satisfies.
        assert_eq!(
            LUMPFUN_POOL_MIN_LOVELACE + c.ada_reserve + c.platform_accrued + c.creator_accrued,
            1_562_000_000,
            "the pool UTxO's lovelace"
        );
    }

    /// The token side is virtual by a fixed amount — the same at the launch and
    /// three trades later — and that constant is recoverable from one output.
    #[test]
    fn the_virtual_token_offset_is_constant_across_states() {
        assert_eq!(bunny().virtual_tokens(BUNNY_HELD), Some(125_337_602));
        let launch = decode_lumpfun_datum(&hex::decode(BUNNY_LAUNCH).unwrap()).unwrap();
        assert_eq!(
            launch.virtual_tokens(BUNNY_HELD_AT_LAUNCH),
            Some(125_337_602)
        );
    }

    /// The ADA offset the cohort compiled in, recovered from each state alone.
    ///
    /// ⚠️ It is NOT exact to the lovelace, and this test says so rather than
    /// pretending otherwise: the validator floors every trade, so each state
    /// sits a little off the exact curve. The tolerance here is 1,000 lovelace
    /// against an offset of 5 billion — one part in five million.
    #[test]
    fn the_virtual_ada_offset_is_recovered_from_one_state() {
        let launch = decode_lumpfun_datum(&hex::decode(BUNNY_LAUNCH).unwrap()).unwrap();
        for (curve, held, want) in [
            (bunny(), BUNNY_HELD, 5_000_000_130),
            (launch, BUNNY_HELD_AT_LAUNCH, 5_000_000_482),
        ] {
            let got = curve.virtual_ada(held).expect("a state with a first buy");
            assert_eq!(got, want, "held={held}");
            assert!(
                (got - 5_000_000_000).abs() < 1_000,
                "{got} is more than 1,000 lovelace from the compiled 5,000,000,000"
            );
        }
    }

    /// The price the curve shows, against the one LumpFun's own API reported
    /// for the same state: `priceLovelace` 7,636,468 (scaled by 1e6).
    #[test]
    fn the_effective_reserves_price_the_token_the_way_the_venue_does() {
        let c = bunny();
        let (base, quote) = c.effective(BUNNY_HELD).expect("priceable after a buy");
        assert_eq!(base, 858_381_090);
        assert_eq!(quote, 1_555_000_000 + c.virtual_ada(BUNNY_HELD).unwrap());
        let scaled = (quote as i128) * 1_000_000 / (base as i128);
        assert_eq!(scaled as i64, 7_636_468);
    }

    /// And the quote it produces, against the one `/api/tx/build` handed back
    /// for a 5 ADA buy of the same state: 654,253 tokens.
    #[test]
    fn a_five_ada_buy_quotes_the_venues_own_number() {
        let c = bunny();
        let (base, _) = c.effective(BUNNY_HELD).unwrap();
        let effective = c.ada_reserve as i128 + c.virtual_ada(BUNNY_HELD).unwrap() as i128;
        let amount_in: i128 = 5_000_000;
        let out = (base as i128) * amount_in / (effective + amount_in);
        assert_eq!(out as i64, 654_253);
    }

    /// A launch with no first buy has `ada_reserve == 0`: the price is
    /// undefined, not zero, and the decoder says so by declining to price it.
    #[test]
    fn a_curve_with_no_first_buy_is_not_priced() {
        let c = LumpFunCurve {
            policy: vec![0xaa; 28],
            name: b"NEW".to_vec(),
            ada_reserve: 0,
            token_reserve: LUMPFUN_SUPPLY + 125_000_000,
            platform_accrued: 0,
            creator_accrued: 0,
        };
        assert_eq!(c.virtual_tokens(LUMPFUN_SUPPLY), Some(125_000_000));
        assert_eq!(c.virtual_ada(LUMPFUN_SUPPLY), None);
        assert_eq!(c.effective(LUMPFUN_SUPPLY), None);
    }

    /// A state whose numbers cannot describe THIS supply is refused rather than
    /// made to fit: held above supply, or a token side below what the pool
    /// holds, is a decoder-reading-the-wrong-thing bug.
    #[test]
    fn an_inconsistent_state_is_not_made_to_fit() {
        let c = bunny();
        assert_eq!(c.virtual_tokens(LUMPFUN_SUPPLY + 1), None);
        assert_eq!(c.virtual_tokens(-1), None);
        assert_eq!(c.virtual_tokens(c.token_reserve + 1), None);
    }

    /// Only the six-field `Constr 3` shape decodes. A LumpPad pool datum, a
    /// snek.fun bonding datum and the order datum all decline — the whole point
    /// of two protocols sharing a brand is that they must not be confused.
    #[test]
    fn another_protocols_datum_is_declined() {
        assert!(decode_lumpfun_datum(&[]).is_none());
        // The order datum from `2dcc4d22…#0`: Constr 0, eight fields.
        let order = "d8799f581c4a00e5040c2d7e201a9c20744ace64bf28d1cda55999a4931e406922d8799f581ccba51a2e5b5b802a0402a87b762d2ecc6b1a9b6d0dd708daf07b82adff581ca1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba2934542554e4e59d879801a0bebc2001a018798941a0006ddd0ff";
        assert!(decode_lumpfun_datum(&hex::decode(order).unwrap()).is_none());
        assert!(decode_lumpfun_datum(&hex::decode(RIDDLE_DATUM).unwrap()).is_none());
    }

    /// Both LumpFun credentials pinned as hex, for the same reason the snek.fun
    /// ones are: an explorer is the only way anyone will ever verify them.
    #[test]
    fn the_lumpfun_order_credential_is_pinned() {
        assert_eq!(
            hex::encode(LUMPFUN_ORDER_CRED),
            "23c870a96ef1343a38c61a0d566c9d39d130d53eaa1517ca92f60eb4",
        );
        assert!(is_lumpfun_order(&LUMPFUN_ORDER_CRED));
        assert!(!is_lumpfun_order(&ORDER_CRED));
        assert!(!is_snek_fun_order(&LUMPFUN_ORDER_CRED));
        assert_ne!(LUMPFUN_ORDER_CRED, ORDER_CRED);
    }

    /// The state NFT's name is CIP-67 label 100 + "POOL", which is how the
    /// observer finds a curve whose validator is parameterised per launch.
    #[test]
    fn the_state_nft_name_is_label_100_pool() {
        assert_eq!(&LUMPFUN_STATE_NFT_NAME[..4], &[0x00, 0x06, 0x43, 0xb0]);
        assert_eq!(&LUMPFUN_STATE_NFT_NAME[4..], b"POOL");
    }
}
