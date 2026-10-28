//! The observer seam — what the reverse walk records about a script output,
//! and who gets to interpret it.
//!
//! # The walk does not know what a DEX is, and must not learn
//!
//! `reverse` finds outputs; it does not decode them. An [`OutputObserver`] is
//! handed each script-held output and may annotate it. That keeps
//! `policy-archive` free of every decode crate and keeps this binary free of
//! venue logic — it wires an observer it was handed.
//!
//! # Recording the candidate is the point, not a fallback
//!
//! An observation is written whether or not an observer claims the output.
//! Cohort classification is a pure function of the address, so `classify`
//! re-derives all history in a second; a decoded observation has no such
//! property, because it was produced by the decoders that existed when the pass
//! ran. Keeping the raw output — address, value, datum — is what lets a decoder
//! added later re-derive its history from the archive instead of from certified
//! chunks.
//!
//! See `cnft.dev-workers/docs/design/POLICY_ARCHIVE_OBSERVATIONS.md`.

use mitos_chain_walk::decode::DecodedOutput;
use policy_archive::observation::Decoded;

/// Annotates a script output the walk has already decided is interesting.
///
/// `qty` is the watched unit's quantity, already extracted by the walk;
/// `datum` is inline or resolved from the creating transaction's witness set.
pub trait OutputObserver: Send + Sync {
    fn observe(
        &self,
        out: &DecodedOutput,
        qty: i64,
        watched_policy: &[u8],
        watched_name: &[u8],
        datum: Option<&[u8]>,
    ) -> Option<Decoded>;
}

/// The DEX observer: a thin adapter over `mitos-pool-observe`, which is where
/// every venue's reserve rule actually lives.
pub struct DexObserver;

impl OutputObserver for DexObserver {
    fn observe(
        &self,
        out: &DecodedOutput,
        qty: i64,
        watched_policy: &[u8],
        watched_name: &[u8],
        datum: Option<&[u8]>,
    ) -> Option<Decoded> {
        let obs = mitos_pool_observe::recognise(out, qty, watched_policy, watched_name, datum)?;
        let quote = obs.quote.as_ref();
        Some(Decoded {
            venue: obs.dex.to_string(),
            key_policy: obs.key_policy,
            key_name: obs.key_name,
            key_basis: obs.key_basis.as_str().to_string(),
            // Always measured: `base` is the watched asset and the walk
            // extracted its quantity.
            base_reserve: obs.base.reserve.unwrap_or(0),
            quote_policy: quote.map(|q| q.policy.clone()),
            quote_name: quote.map(|q| q.name.clone()),
            quote_reserve: quote.and_then(|q| q.reserve),
            fee_bps: obs.fee_bps,
            total_lp: obs.total_lp,
            reserve_source: obs.reserve_source.as_str().to_string(),
            pricing: policy_archive::observation::pricing::CONSTANT_PRODUCT.to_string(),
        })
    }
}

/// The launchpad observer: a bonding curve is not a DEX, so it gets its own.
///
/// It reports the curve's two sides the same way a pool does, but deliberately
/// publishes **no fee and no LP** — a bonding curve has neither — and records
/// the ADA cap in `total_lp`'s place would be a lie, so that is left absent
/// too. The cap and the curve parameters live in the datum, which the raw
/// observation already carries.
pub struct LaunchpadObserver;

impl OutputObserver for LaunchpadObserver {
    fn observe(
        &self,
        out: &DecodedOutput,
        qty: i64,
        watched_policy: &[u8],
        watched_name: &[u8],
        datum: Option<&[u8]>,
    ) -> Option<Decoded> {
        if let Some(decoded) = lumpfun(out, qty, watched_policy, watched_name, datum) {
            return Some(decoded);
        }

        let cred = mitos_cohort::payment_cred(&out.address)?;
        if !mitos_launchpad_decode::is_snek_fun_curve(&cred) {
            return None;
        }
        let pool = datum.and_then(mitos_launchpad_decode::decode_bonding_datum);
        Some(Decoded {
            // From the constant, not a literal: the trade fold names the same
            // curve from `venue::SNEK_FUN`, and two spellings would stop a
            // curve state and a curve fill joining.
            venue: mitos_dex_decode::venue::SNEK_FUN.to_string(),
            key_policy: pool
                .as_ref()
                .map(|p| p.pool_nft.policy.clone())
                .unwrap_or_default(),
            key_name: pool
                .as_ref()
                .map(|p| p.pool_nft.name.clone())
                .unwrap_or_default(),
            // A curve recognised by credential but unreadable is still a
            // curve; its identity is what is missing, not its supply.
            key_basis: match pool.is_some() {
                true => "datum",
                false => "unknown",
            }
            .to_string(),
            base_reserve: qty,
            // The curve's other side is ADA, and its reserve is the output's
            // lovelace INCLUDING the 3 ADA seed — netting is the reader's job,
            // because the cap the seed must be netted against lives in the
            // datum and a reader that has one has the other.
            quote_policy: Some(Vec::new()),
            quote_name: Some(Vec::new()),
            quote_reserve: Some(out.lovelace as i64),
            // ⚠️ Deliberately absent. A bonding curve charges no swap fee and
            // issues no LP, and the price is NOT constant-product — reporting
            // a fee of zero would invite exactly the pricing this crate's
            // notes say is wrong by ~3× at the top of the curve.
            fee_bps: None,
            total_lp: None,
            reserve_source: "value".to_string(),
            // ⚠️ NOT constant-product, and saying so is what stops the curve's
            // reserves being summed into a `Σquote/Σbase` aggregate. On the
            // first end-to-end run they were, and $PERP priced at half its
            // real value — 261,194,031 curve tokens joined a 189M base.
            pricing: policy_archive::observation::pricing::BONDING_CURVE.to_string(),
        })
    }
}

/// LumpFun's curve, recognised by its STATE NFT rather than by a credential.
///
/// ⚠️ **A per-launch validator means there is no shared credential.** snek.fun
/// ships one curve script for every token, so `is_snek_fun_curve` is a
/// credential test; LumpFun parameterises the validator with the policy, the
/// token name, the creator, the fee bps and the graduation threshold, so every
/// pool has its own script hash. What is shared is the STATE NFT's name under
/// the launched token's own policy — one per pool, minted at launch — and that
/// is what this matches.
///
/// The curve is a constant product over **effective** reserves (a virtual
/// offset on both sides). The offsets are recovered from the pool's own state:
/// the token side's is `datum.token_reserve - held`, and the ADA side's follows
/// from the curve having begun at `(0, supply + virtual_tokens)` — see
/// `LumpFunCurve::virtual_ada`. Both are exact enough to price with, and
/// neither is a configured constant.
///
/// A curve with no first buy has no recoverable ADA offset. It is still a
/// curve, so it is recorded as one — under `bonding-curve`, which is the honest
/// "real reserves, no formula" answer this crate already has — and it becomes
/// priceable the moment the first buy lands.
fn lumpfun(
    out: &DecodedOutput,
    qty: i64,
    watched_policy: &[u8],
    watched_name: &[u8],
    datum: Option<&[u8]>,
) -> Option<Decoded> {
    let state_nft = out.assets.iter().any(|a| {
        a.policy == watched_policy && a.name == mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME
    });
    if !state_nft {
        return None;
    }
    let curve = mitos_launchpad_decode::decode_lumpfun_datum(datum?)?;
    // The pool holds the token whose curve it is. A datum naming another unit
    // means the walk handed this observer a different sighting — a bug, not a
    // pool — and claiming it would make one token's curve price another's.
    if curve.policy != watched_policy || curve.name != watched_name {
        return None;
    }

    let (base_reserve, quote_reserve, source, pricing) = match curve.effective(qty) {
        Some((base, quote)) => (
            base,
            Some(quote),
            "datum-plus-virtual",
            policy_archive::observation::pricing::CONSTANT_PRODUCT_VIRTUAL,
        ),
        // No first buy: `ada_reserve` is zero, the offsets cannot be recovered
        // and the price is undefined. Report the raw balances, so this row and
        // the story's `CurveState` for it agree to the lovelace.
        None => (
            qty,
            Some(out.lovelace as i64),
            "value",
            policy_archive::observation::pricing::BONDING_CURVE,
        ),
    };

    Some(Decoded {
        venue: mitos_dex_decode::venue::LUMPFUN_V5.to_string(),
        key_policy: watched_policy.to_vec(),
        key_name: mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME.to_vec(),
        key_basis: "datum".to_string(),
        base_reserve,
        quote_policy: Some(Vec::new()),
        quote_name: Some(Vec::new()),
        quote_reserve,
        // ⚠️ Deliberately absent. LumpFun's 2% is charged on the ADA LEG (1%
        // platform + 1% creator) and paid on top; the curve itself nets
        // nothing. Reporting 200 bps would make a consumer net it a second
        // time, which is the classic way a quote comes out just under the
        // chain's.
        fee_bps: None,
        total_lp: None,
        reserve_source: source.to_string(),
        pricing: pricing.to_string(),
    })
}

/// Every observer, tried in order; the first to claim the output wins.
///
/// Order matters only where two could claim one output, which cannot happen
/// today — a credential is a DEX pool or a bonding curve, never both — but the
/// first-wins rule is stated rather than left to chance.
pub fn default_observers() -> Vec<Box<dyn OutputObserver>> {
    vec![Box::new(DexObserver), Box::new(LaunchpadObserver)]
}

/// Decode again every observation nothing claimed when it was walked, with
/// the observers THIS build has; returns how many it now claims.
///
/// What keeping the candidates was for (see the module's docs): a decoder
/// added after a walk reads that walk's history without a re-walk. MEASURED on
/// $NikePig: Splash's third pool contract and VyFi were registered after the
/// box walked the token, and 9,128 Splash and 7,002 VyFi pool states sat
/// undecoded, so every trade on them had no ADA figure.
///
/// The output is rebuilt from what the observation kept: its address, its
/// lovelace, the watched unit's quantity and its datum. That is everything an
/// ADA-paired pool's reserves are read from; a pool's other assets (its NFT,
/// a token/token pair's far side) were not kept, so such a pool decodes with
/// its identity or its pair unknown, as a datum-less one does.
pub fn redecode(observations: &mut [policy_archive::Observation], policy: &[u8]) -> usize {
    use mitos_chain_walk::decode::Asset;
    let observers = default_observers();
    let mut claimed = 0;
    for o in observations.iter_mut().filter(|o| o.decoded.is_none()) {
        let Ok(qty) = u64::try_from(o.unit_amount) else {
            continue;
        };
        let out = DecodedOutput {
            address: o.address.clone(),
            lovelace: u64::try_from(o.lovelace).unwrap_or(0),
            assets: vec![Asset::new(policy.to_vec(), o.unit_name.clone(), qty)],
            index: 0,
            datum_hash: None,
            inline_datum: None,
            min_utxo: 0,
        };
        o.decoded = observe_all(
            &observers,
            &out,
            o.unit_amount,
            policy,
            &o.unit_name,
            o.datum.as_deref(),
        );
        claimed += usize::from(o.decoded.is_some());
    }
    claimed
}

/// Run the chain of observers over one output.
pub fn observe_all(
    observers: &[Box<dyn OutputObserver>],
    out: &DecodedOutput,
    qty: i64,
    watched_policy: &[u8],
    watched_name: &[u8],
    datum: Option<&[u8]>,
) -> Option<Decoded> {
    observers
        .iter()
        .find_map(|o| o.observe(out, qty, watched_policy, watched_name, datum))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitos_chain_walk::decode::Asset;
    use policy_archive::observation::pricing;

    /// Dust Bunny (`a1b085f4…`), the first LumpFun v5 launch.
    const POLICY: &str = "a1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba293";
    /// Its curve — a per-launch address, which is why the state NFT is the key.
    const POOL: &str = "addr1wyj2cxrm7pkfz0sx6j6lgueg8035yrs4ry9xupcvcxlrg6cywmqny";
    /// Tokens the pool UTxO actually held, read from the output.
    const HELD: i64 = 733_043_488;

    /// The pool's inline datum in the sample buy `1f7e8e57…`, copied from the
    /// chain (Koios `datum_info`, hash `aaaff6e9…`).
    const DATUM: &str = "d87c9f581ca1b085f414fb7799d378e90e39d24c9e54c841a311d72b2f57cba2934542554e4e591a5caf6ac01a3329db221a001e84801a001e8480ff";

    fn policy() -> Vec<u8> {
        hex::decode(POLICY).unwrap()
    }

    fn output(lovelace: u64, assets: Vec<Asset>) -> DecodedOutput {
        DecodedOutput {
            address: POOL.to_string(),
            lovelace,
            assets,
            index: 0,
            datum_hash: None,
            inline_datum: None,
            min_utxo: 1_500_000,
        }
    }

    fn pool_output() -> DecodedOutput {
        output(
            1_562_000_000,
            vec![
                Asset::nft(
                    policy(),
                    mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME.to_vec(),
                ),
                Asset::new(policy(), b"BUNNY".to_vec(), HELD as u64),
            ],
        )
    }

    fn observe(out: &DecodedOutput, name: &[u8]) -> Option<Decoded> {
        let datum = hex::decode(DATUM).unwrap();
        observe_all(
            &default_observers(),
            out,
            HELD,
            &policy(),
            name,
            Some(&datum),
        )
    }

    /// The whole point: a per-launch curve has no shared credential, so it is
    /// found by its state NFT — and priced over the EFFECTIVE reserves the
    /// datum and the held quantity together recover.
    #[test]
    fn a_lumpfun_curve_is_recognised_by_its_state_nft_and_priced() {
        let d = observe(&pool_output(), b"BUNNY").expect("a curve the observer must claim");
        assert_eq!(d.venue, mitos_dex_decode::venue::LUMPFUN_V5);
        assert_eq!(d.pricing, pricing::CONSTANT_PRODUCT_VIRTUAL);
        assert_eq!(d.reserve_source, "datum-plus-virtual");
        assert_eq!(d.key_basis, "datum");
        assert_eq!(
            d.key_name,
            mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME.to_vec()
        );
        assert_eq!(
            d.base_reserve, 858_381_090,
            "the curve's virtual token side"
        );
        let effective_quote = d.quote_reserve.expect("a state with a first buy");
        assert_eq!(effective_quote, 1_555_000_000 + 5_000_000_130);
        assert_eq!(
            d.fee_bps, None,
            "the 2% is charged on the ADA leg, not netted by the curve"
        );
        // 7.636468 lovelace per whole unit — LumpFun's own priceLovelace is
        // 7,636,468 when scaled by 1e6.
        let price = effective_quote as f64 / d.base_reserve as f64;
        assert!((price - 7.636_468).abs() < 1e-5, "{price}");
    }

    /// The state NFT is a unit of the same policy and sits in the same output.
    /// The walk hands the observer every unit in turn, so a claim under the
    /// NFT's own name would record a curve sighting whose `unit_amount` is 1.
    #[test]
    fn the_state_nft_unit_is_not_a_second_curve_sighting() {
        assert!(
            observe(
                &pool_output(),
                &mitos_launchpad_decode::LUMPFUN_STATE_NFT_NAME
            )
            .is_none()
        );
    }

    /// A datum naming a different token is not this pool's — claiming it would
    /// let one launch's curve price another.
    #[test]
    fn a_curve_that_names_another_token_is_not_claimed() {
        assert!(observe(&pool_output(), b"OTHER").is_none());
    }

    /// The same address without the state NFT is somebody paying tokens into
    /// the script, not a pool state. An address alone is not a pool.
    #[test]
    fn an_output_without_the_state_nft_is_not_a_pool() {
        let out = output(
            1_562_000_000,
            vec![Asset::new(policy(), b"BUNNY".to_vec(), HELD as u64)],
        );
        assert!(observe(&out, b"BUNNY").is_none());
    }
}
