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
        _watched_policy: &[u8],
        _watched_name: &[u8],
        datum: Option<&[u8]>,
    ) -> Option<Decoded> {
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

/// Every observer, tried in order; the first to claim the output wins.
///
/// Order matters only where two could claim one output, which cannot happen
/// today — a credential is a DEX pool or a bonding curve, never both — but the
/// first-wins rule is stated rather than left to chance.
pub fn default_observers() -> Vec<Box<dyn OutputObserver>> {
    vec![Box::new(DexObserver), Box::new(LaunchpadObserver)]
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
