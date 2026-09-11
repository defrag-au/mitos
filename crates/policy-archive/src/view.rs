//! The cumulative view: **fold once, project many**.
//!
//! # Why this exists
//!
//! Every consumer of an archive had grown its own pass over the rows — the
//! price tier, the token band, the archive tool's pool table, the browser's
//! chart — and each one re-derived "the latest state of each pool" in its own
//! slightly different way. They disagreed. `spot_at` keyed a pool by address
//! AND instance key; the token band keyed it by venue alone, and reported
//! *"splash: liquidity not published"* over 29,121 ADA the archive was holding.
//! Same file, same question, two answers.
//!
//! A view separates the two halves of that work so only ONE of them can drift:
//!
//! | | what it is | how often |
//! |---|---|---|
//! | [`PolicyView::apply`] | the FOLD — accumulate state | once per row |
//! | [`PolicyView::spot`] | the PROJECTION — answer a question | once per reader |
//!
//! The fold is the expensive, order-sensitive, easy-to-get-wrong half, and it
//! now exists once. A projection is a pure function of the folded state, so a
//! second consumer adds a projection and inherits the fold rather than writing
//! a fourth version of "latest per pool".
//!
//! # The bucket decision is NAMED
//!
//! `spot_at` decided where a pool's reserves land inside a nested `match`
//! buried in its loop. That decision is the whole of the price's honesty —
//! summing a bonding curve halved $PERP, and summing a dead pool put $NIKEPIG
//! 50% high — and it could not be tested without building a `Spot`. It is now
//! [`Bucket`], returned by [`Pool::bucket`], testable on its own.
//!
//! # What this does NOT do yet
//!
//! ⚠️ It folds [`Observation`]s, not [`crate::story::Kind`] events. Those are
//! the same facts, and the intent is one fold serving box and browser alike —
//! but the story's `PoolState` carries only the venue and the instance key,
//! **not** the address or the key policy, so folding it cannot tell two pools
//! apart at the resolution [`PoolKey`] demands. On a venue that publishes no
//! instance key every pool collapses into one bucket and their reserves merge.
//! Closing that means giving the story stream a pool table and an ordinal, the
//! way a spine already does. Until then the box folds observations here and
//! the browser gets a projection, never the fold.

use std::collections::HashMap;

use crate::observation::{Observation, pricing};
use crate::price::{DEFAULT_STALE_AFTER_SLOTS, PairDepth, Spot, Unit};
use crate::story::{Kind, PoolIdent, Story, StoryEvent};

/// WHICH pool. Address AND instance key, because neither alone identifies one.
///
/// ⚠️ Both halves are load-bearing and each covers a case the other cannot:
///
/// - **Address alone** merges every pool on a venue that hosts them all at one
///   script address and tells them apart by a pool NFT — CSwap, Splash. Their
///   reserves would sum into one nonsense pool.
/// - **Instance key alone** merges every pool on a venue that publishes no key
///   at all, where `key_name` is empty for all of them, and merges pools across
///   venues that happen to reuse a key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolKey {
    /// The script address, bech32, as the rows spell it.
    pub address: String,
    pub key_policy: Vec<u8>,
    pub key_name: Vec<u8>,
}

/// One pool's LAST known state, and when it was last seen.
///
/// ⚠️ "Last seen" is not "still there". An observation is only written when a
/// pool's UTxO is touched **while holding the asset**, so a pool that is
/// drained stops being observed at the moment *before* it emptied and its final
/// full reserves sit here for ever. That is what [`Bucket::Stale`] is for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pool {
    pub venue: String,
    /// Slot of the last sighting.
    pub slot: u64,
    /// Tie-break only, so the fold cannot depend on the order rows arrived in.
    pub tx_hash: Vec<u8>,
    /// The watched policy's reserve, raw.
    pub base: i64,
    /// The far side's reserve. `None` is **"this venue does not publish it"**,
    /// which is not zero.
    pub quote: Option<i64>,
    /// What the asset is paired WITH. `None` when the pair is unknown.
    pub quote_unit: Option<Unit>,
    /// One of [`pricing`]. Empty on a row written before the column existed.
    pub pricing: String,
}

/// Where a pool's reserves land in a projection, and **why**.
///
/// Four outcomes, four different problems, and collapsing any two of them has
/// cost a wrong number on the page. See each variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Bucket {
    /// Summed into the price: constant-product, seen recently, with a measured
    /// pair and reserves on both sides.
    Priced,
    /// Real reserves under a model this crate does not evaluate — a launchpad
    /// bonding curve, whose rate is **not** `quote / base`. Needs a FORMULA.
    ///
    /// A row with an empty `pricing` lands here too: not known to be
    /// constant-product is not the same as known to be otherwise.
    OffModel,
    /// Last seen longer ago than the horizon. Needs NOTHING — the reserves were
    /// real when written and the archive cannot tell whether they still exist.
    Stale,
    /// Nothing summable: the pair is unknown, or unmeasured, or a side is zero.
    /// Guessing a zero for an unmeasured far side drags the aggregate toward
    /// zero exactly where the data is thinnest.
    Unusable,
}

impl Bucket {
    /// Every variant, so a caller tabulating them cannot quietly omit one when
    /// a fifth is added.
    pub const ALL: [Bucket; 4] = [
        Bucket::Priced,
        Bucket::OffModel,
        Bucket::Stale,
        Bucket::Unusable,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Bucket::Priced => "priced",
            Bucket::OffModel => "off-model",
            Bucket::Stale => "stale",
            Bucket::Unusable => "unusable",
        }
    }
}

/// The knobs a projection turns. Not a fold input — changing one re-projects,
/// it never re-folds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Projection {
    /// How far back a sighting may be and still describe the price at the slot
    /// being asked about. See [`DEFAULT_STALE_AFTER_SLOTS`].
    pub stale_after: u64,
}

impl Default for Projection {
    fn default() -> Self {
        Projection {
            stale_after: DEFAULT_STALE_AFTER_SLOTS,
        }
    }
}

impl Projection {
    /// With the staleness horizon given explicitly. `u64::MAX` counts every
    /// pool's last sighting however old — reachable so a caller auditing
    /// history can ask for it deliberately, never by default.
    pub fn within(stale_after: u64) -> Self {
        Projection { stale_after }
    }
}

impl Pool {
    /// Where this pool's reserves land, as of slot `at`.
    ///
    /// ⚠️ **Order matters.** `Unusable` is decided BEFORE `Stale`: a pool with
    /// no measured pair contributes nothing whether it is fresh or not, and
    /// reporting it under "stale liquidity" would name reserves that were never
    /// countable as reserves that merely aged out.
    pub fn bucket(&self, at: u64, p: &Projection) -> Bucket {
        let (Some(_), Some(quote)) = (&self.quote_unit, self.quote) else {
            return Bucket::Unusable;
        };
        if self.base <= 0 || quote <= 0 {
            return Bucket::Unusable;
        }
        // ⚠️ STALE BEFORE THE MODEL SPLIT. A pool nobody has touched in a month
        // cannot set a price whatever its pricing model says, and letting it
        // through here is what put $NIKEPIG 50% high.
        if at.saturating_sub(self.slot) > p.stale_after {
            return Bucket::Stale;
        }
        match self.pricing.as_str() {
            pricing::CONSTANT_PRODUCT => Bucket::Priced,
            _ => Bucket::OffModel,
        }
    }
}

/// One policy's state, accumulated from its rows.
///
/// Fold forward only. To answer for an EARLIER position, re-fold from a
/// checkpoint — do not ask this one backwards, and [`PolicyView::spot`] will
/// say so in a debug build if you do.
#[derive(Debug, Clone, Default)]
pub struct PolicyView {
    pools: HashMap<PoolKey, Pool>,
    ceiling: Option<u64>,
    claimed: usize,
    /// Rows no decoder recognised. Kept because "we see nothing here" and "we
    /// see something we cannot read" are different answers, and a pool table
    /// that shows only the second looks complete when it is not.
    candidates: usize,
    /// Story events naming a pool ordinal the stream's table does not hold.
    ///
    /// ⚠️ Always zero for a stream this workspace produced. Counted because
    /// the alternative to counting is folding such an event into a placeholder
    /// pool, which merges unrelated reserves silently — and a client that sees
    /// a non-zero here knows its price is incomplete rather than wrong.
    unresolved_pool_refs: usize,
}

impl PolicyView {
    /// Fold every row. Rows need not be sorted.
    pub fn from_observations(observations: &[Observation]) -> Self {
        let mut view = PolicyView::default();
        for o in observations {
            view.apply(o);
        }
        view
    }

    /// Fold every row at or before `slot`.
    pub fn upto(observations: &[Observation], slot: u64) -> Self {
        let mut view = PolicyView::default();
        for o in observations.iter().filter(|o| o.slot <= slot) {
            view.apply(o);
        }
        view
    }

    /// Fold a whole story — the SAME fold a client folding the stream runs.
    ///
    /// ⚠️ This is the point of the exercise. The box folds
    /// [`Observation`]s because that is what it reads off disk; a browser
    /// folds [`StoryEvent`]s because that is what comes down the socket. They
    /// must agree, and `observations_and_story_fold_identically` asserts it
    /// rather than leaving it to a hand-check — which is how the browser and
    /// the archive tier silently disagreed three times in five days.
    pub fn from_story(story: &Story) -> Self {
        let mut view = PolicyView::default();
        for e in &story.events {
            view.apply_event(e, &story.pools);
        }
        view
    }

    /// Fold one story event.
    ///
    /// `pools` is the stream's pool table — [`Story::pools`]. An ordinal
    /// pointing outside it is a bug in whatever produced the stream and the
    /// event is SKIPPED rather than folded into a placeholder pool, because a
    /// placeholder would merge unrelated reserves, which is the failure this
    /// whole table exists to prevent.
    pub fn apply_event(&mut self, e: &StoryEvent, pools: &[PoolIdent]) {
        let (ident, next) = match &e.kind {
            Kind::PoolState(p) => {
                let Some(ident) = pools.get(p.pool as usize) else {
                    self.unresolved_pool_refs += 1;
                    return;
                };
                let quote_unit = match (&p.quote_policy, &p.quote_name) {
                    (Some(policy), Some(name)) => Some(Unit {
                        policy: policy.clone(),
                        name: name.clone(),
                    }),
                    // ⚠️ The stream spells ADA as a pair of `None`s, where an
                    // observation row spells it as a pair of EMPTY vectors.
                    // Reading `None` here as "pair unknown" would put every
                    // ADA pool in `Bucket::Unusable` and the price would be
                    // undefined for every token.
                    (None, None) => Some(Unit::ada()),
                    _ => None,
                };
                (
                    ident,
                    Pool {
                        venue: ident.venue.clone(),
                        slot: e.slot,
                        tx_hash: e.tx.clone(),
                        base: p.base,
                        quote: p.quote,
                        quote_unit,
                        pricing: p.pricing.clone(),
                    },
                )
            }
            // ⚠️ A CURVE IS A POOL HERE. `story::build` splits bonding curves
            // into their own kind because they are the launch timeline — but
            // their reserves are real, and the observation fold counts them
            // (as `Bucket::OffModel`, never priced). Skipping them here would
            // make the two folds disagree on exactly the liquidity
            // `Spot::unpriceable` and `Spot::stale` exist to report.
            Kind::CurveState {
                pool,
                lovelace,
                tokens_left,
                ..
            } => {
                let Some(ident) = pools.get(*pool as usize) else {
                    self.unresolved_pool_refs += 1;
                    return;
                };
                (
                    ident,
                    Pool {
                        venue: ident.venue.clone(),
                        slot: e.slot,
                        tx_hash: e.tx.clone(),
                        base: *tokens_left,
                        quote: Some(*lovelace),
                        quote_unit: Some(Unit::ada()),
                        pricing: pricing::BONDING_CURVE.to_string(),
                    },
                )
            }
            // ⚠️ An unclaimed script is a CANDIDATE, the same thing
            // `apply` counts when an observation has no `Decoded`. Counting it
            // keeps the two folds' `candidates` figures comparable.
            Kind::UnclaimedScript { .. } => {
                self.ceiling = Some(self.ceiling.map_or(e.slot, |c| c.max(e.slot)));
                self.candidates += 1;
                return;
            }
            // Supply and venue activity move no pool reserves. They belong to
            // projections this view does not serve yet, and inventing a fold
            // for them here would be guessing at their shape.
            _ => return,
        };
        let key = PoolKey {
            address: ident.address.clone(),
            key_policy: ident.key_policy.clone(),
            key_name: ident.key_name.clone(),
        };
        self.absorb(key, next);
    }

    /// Fold one row.
    pub fn apply(&mut self, o: &Observation) {
        let Some(d) = &o.decoded else {
            // ⚠️ The ceiling moves for a candidate too: the archive HAS seen
            // this slot, it just could not read what it saw.
            self.ceiling = Some(self.ceiling.map_or(o.slot, |c| c.max(o.slot)));
            self.candidates += 1;
            return;
        };
        let key = PoolKey {
            address: o.address.clone(),
            key_policy: d.key_policy.clone(),
            key_name: d.key_name.clone(),
        };
        let quote_unit = match (&d.quote_policy, &d.quote_name) {
            (Some(policy), Some(name)) => Some(Unit {
                policy: policy.clone(),
                name: name.clone(),
            }),
            _ => None,
        };
        let next = Pool {
            venue: d.venue.clone(),
            slot: o.slot,
            tx_hash: o.tx_hash.clone(),
            base: d.base_reserve,
            quote: d.quote_reserve,
            quote_unit,
            pricing: d.pricing.clone(),
        };
        self.absorb(key, next);
    }

    /// The fold itself: keep the NEWEST sighting of a pool.
    ///
    /// ⚠️ **One body, two adapters.** [`apply`](Self::apply) and
    /// [`apply_event`](Self::apply_event) differ only in how they read a
    /// sighting out of their own input; everything that decides what the view
    /// BECOMES happens here. Two copies of this — even correct ones — would be
    /// two things to keep in step, which is the whole failure being unwound.
    fn absorb(&mut self, key: PoolKey, next: Pool) {
        self.ceiling = Some(self.ceiling.map_or(next.slot, |c| c.max(next.slot)));
        self.claimed += 1;
        match self.pools.get_mut(&key) {
            // Ties broken by tx_hash so the answer cannot depend on the order
            // rows happened to arrive in.
            Some(cur) if (next.slot, &next.tx_hash) > (cur.slot, &cur.tx_hash) => *cur = next,
            Some(_) => {}
            None => {
                self.pools.insert(key, next);
            }
        }
    }

    /// Every pool's last known state.
    pub fn pools(&self) -> impl Iterator<Item = (&PoolKey, &Pool)> {
        self.pools.iter()
    }

    pub fn pool_count(&self) -> usize {
        self.pools.len()
    }

    /// The highest slot folded, `None` on an empty view.
    pub fn ceiling(&self) -> Option<u64> {
        self.ceiling
    }

    /// Rows a decoder claimed.
    pub fn claimed(&self) -> usize {
        self.claimed
    }

    /// Rows nothing recognised — see [`PolicyView::candidates`]'s field note.
    pub fn candidates(&self) -> usize {
        self.candidates
    }

    /// Story events whose pool ordinal fell outside the stream's table. Non-zero
    /// means the stream is malformed and this view is missing reserves.
    pub fn unresolved_pool_refs(&self) -> usize {
        self.unresolved_pool_refs
    }

    /// How many pools fall in each bucket, as of `at`. The pool table's
    /// summary, and the cheapest way to see WHY a price is thin.
    pub fn buckets(&self, at: u64, p: &Projection) -> HashMap<Bucket, usize> {
        let mut counts: HashMap<Bucket, usize> = Bucket::ALL.iter().map(|b| (*b, 0)).collect();
        for pool in self.pools.values() {
            *counts.entry(pool.bucket(at, p)).or_default() += 1;
        }
        counts
    }

    /// The price at `at`, aggregated per pair.
    ///
    /// The four rules `crate::price` documents all live here: piecewise
    /// constant, undefined rather than zero, `Σquote / Σbase` over a MERGED
    /// pool rather than an average of prices, and a depth floor that gates
    /// quoting one pool without ever filtering the sum.
    ///
    /// ⚠️ `at` is the slot the answer is FOR, and must be at or after the
    /// fold's [`ceiling`](PolicyView::ceiling). Asking backwards would answer
    /// with sightings from after the slot requested; a debug build asserts, and
    /// a release build skips those pools so the leak is bounded rather than
    /// silent. Neither is a substitute for re-folding to the position you want.
    pub fn spot(&self, at: u64, p: &Projection) -> Spot {
        debug_assert!(
            self.ceiling.is_none_or(|c| at >= c),
            "spot() asked for slot {at} from a view folded to {:?} — re-fold to \
             the earlier position rather than asking this one backwards",
            self.ceiling,
        );

        let mut by_pair: HashMap<Unit, PairDepth> = HashMap::new();
        let mut off_model: HashMap<Unit, PairDepth> = HashMap::new();
        let mut stale: HashMap<Unit, PairDepth> = HashMap::new();

        for pool in self.pools.values() {
            if pool.slot > at {
                continue;
            }
            let bucket = pool.bucket(at, p);
            let target = match bucket {
                Bucket::Unusable => continue,
                Bucket::Priced => &mut by_pair,
                Bucket::OffModel => &mut off_model,
                Bucket::Stale => &mut stale,
            };
            // `bucket` returning anything but `Unusable` is what proves both of
            // these are present; the guards keep that local rather than making
            // the reader hold it in their head.
            let (Some(unit), Some(quote)) = (pool.quote_unit.clone(), pool.quote) else {
                continue;
            };
            let e = target.entry(unit.clone()).or_insert(PairDepth {
                quote_unit: unit,
                base: 0,
                quote: 0,
                pools: 0,
                thinnest: i64::MAX,
                deepest: 0,
            });
            e.base += pool.base as i128;
            e.quote += quote as i128;
            e.pools += 1;
            e.thinnest = e.thinnest.min(quote);
            e.deepest = e.deepest.max(quote);
        }

        let mut ada = None;
        let mut unresolved: Vec<PairDepth> = Vec::new();
        for (unit, depth) in by_pair {
            match unit.is_ada() {
                true => ada = Some(depth),
                false => unresolved.push(depth),
            }
        }
        Spot {
            slot: at,
            ada,
            unresolved: ranked(unresolved),
            unpriceable: ranked(off_model.into_values().collect()),
            stale: ranked(stale.into_values().collect()),
        }
    }
}

/// Deepest first, then by unit — a deterministic order for a caller that
/// renders or hashes it.
fn ranked(mut v: Vec<PairDepth>) -> Vec<PairDepth> {
    v.sort_by(|a, b| b.quote.cmp(&a.quote).then(a.quote_unit.cmp(&b.quote_unit)));
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::Decoded;

    fn decoded(venue: &str, key: &[u8], base: i64, quote: Option<i64>, ada: bool) -> Decoded {
        Decoded {
            venue: venue.to_string(),
            key_policy: Vec::new(),
            key_name: key.to_vec(),
            key_basis: "test".into(),
            base_reserve: base,
            quote_policy: Some(match ada {
                true => Vec::new(),
                false => vec![9u8; 28],
            }),
            quote_name: Some(match ada {
                true => Vec::new(),
                false => b"OTHER".to_vec(),
            }),
            quote_reserve: quote,
            fee_bps: None,
            total_lp: None,
            reserve_source: "test".into(),
            pricing: pricing::CONSTANT_PRODUCT.into(),
        }
    }

    fn obs(slot: u64, addr: &str, key: &[u8], base: i64, quote: Option<i64>) -> Observation {
        Observation {
            slot,
            block_time: slot,
            tx_hash: vec![slot as u8],
            address: addr.into(),
            lovelace: quote.unwrap_or(0),
            unit_name: b"TOK".to_vec(),
            unit_amount: base,
            datum: None,
            decoded: Some(decoded("v", key, base, quote, true)),
        }
    }

    /// The case the address half of [`PoolKey`] exists for: one venue address,
    /// two pools told apart only by their NFT.
    #[test]
    fn one_address_two_instance_keys_stay_two_pools() {
        let view = PolicyView::from_observations(&[
            obs(10, "addr_shared", b"A", 100, Some(1_000)),
            obs(10, "addr_shared", b"B", 300, Some(9_000)),
        ]);
        assert_eq!(view.pool_count(), 2);
        let spot = view.spot(10, &Projection::default());
        let ada = spot.ada.unwrap();
        assert_eq!(ada.pools, 2);
        assert_eq!(ada.base, 400);
        assert_eq!(ada.quote, 10_000);
    }

    /// The case the instance-key half exists for: two addresses, no key at all.
    #[test]
    fn two_addresses_with_no_instance_key_stay_two_pools() {
        let view = PolicyView::from_observations(&[
            obs(10, "addr_one", b"", 100, Some(1_000)),
            obs(10, "addr_two", b"", 300, Some(9_000)),
        ]);
        assert_eq!(view.pool_count(), 2);
        assert_eq!(view.spot(10, &Projection::default()).ada.unwrap().pools, 2);
    }

    /// The fold keeps the LATEST sighting, not the last one handed to it.
    #[test]
    fn fold_is_order_independent() {
        let rows = [
            obs(10, "a", b"K", 100, Some(1_000)),
            obs(50, "a", b"K", 200, Some(8_000)),
            obs(30, "a", b"K", 150, Some(4_000)),
        ];
        let forward = PolicyView::from_observations(&rows);
        let mut reverse = PolicyView::default();
        for o in rows.iter().rev() {
            reverse.apply(o);
        }
        let p = Projection::default();
        assert_eq!(forward.spot(50, &p), reverse.spot(50, &p));
        assert_eq!(forward.spot(50, &p).ada.unwrap().base, 200);
    }

    /// Folding to a position is not the same as filtering at projection time,
    /// and the view says which one it is doing.
    #[test]
    fn upto_answers_the_earlier_position() {
        let rows = [
            obs(10, "a", b"K", 100, Some(1_000)),
            obs(50, "a", b"K", 200, Some(8_000)),
        ];
        let p = Projection::default();
        assert_eq!(
            PolicyView::upto(&rows, 20).spot(20, &p).ada.unwrap().base,
            100
        );
        assert_eq!(
            PolicyView::upto(&rows, 60).spot(60, &p).ada.unwrap().base,
            200
        );
    }

    /// An unmeasured far side is not a zero far side.
    #[test]
    fn unmeasured_quote_is_unusable_not_stale() {
        let pool = Pool {
            venue: "v".into(),
            slot: 0,
            tx_hash: vec![],
            base: 100,
            quote: None,
            quote_unit: Some(Unit::ada()),
            pricing: pricing::CONSTANT_PRODUCT.into(),
        };
        // Ancient AND unmeasured — `Unusable` wins, because it never counted.
        assert_eq!(
            pool.bucket(u64::MAX / 2, &Projection::default()),
            Bucket::Unusable
        );
    }

    #[test]
    fn staleness_is_measured_from_the_slot_asked_about() {
        let pool = Pool {
            venue: "v".into(),
            slot: 1_000,
            tx_hash: vec![],
            base: 100,
            quote: Some(1_000),
            quote_unit: Some(Unit::ada()),
            pricing: pricing::CONSTANT_PRODUCT.into(),
        };
        let p = Projection::within(100);
        assert_eq!(pool.bucket(1_050, &p), Bucket::Priced);
        assert_eq!(pool.bucket(1_100, &p), Bucket::Priced);
        assert_eq!(pool.bucket(1_101, &p), Bucket::Stale);
    }

    #[test]
    fn a_bonding_curve_is_off_model_not_priced() {
        let mut pool = Pool {
            venue: "launchpad".into(),
            slot: 10,
            tx_hash: vec![],
            base: 100,
            quote: Some(1_000),
            quote_unit: Some(Unit::ada()),
            pricing: pricing::BONDING_CURVE.into(),
        };
        let p = Projection::default();
        assert_eq!(pool.bucket(10, &p), Bucket::OffModel);
        // A row written before the column existed is NOT assumed to be the
        // common case.
        pool.pricing = String::new();
        assert_eq!(pool.bucket(10, &p), Bucket::OffModel);
    }

    #[test]
    fn buckets_names_every_variant_even_at_zero() {
        let view = PolicyView::from_observations(&[obs(10, "a", b"K", 100, Some(1_000))]);
        let counts = view.buckets(10, &Projection::default());
        for b in Bucket::ALL {
            assert!(counts.contains_key(&b), "{b:?} missing from the tally");
        }
        assert_eq!(counts[&Bucket::Priced], 1);
    }

    /// 🔑 **THE TEST THIS WHOLE LAYER EXISTS FOR.**
    ///
    /// The box folds observations; a client folds the story stream built from
    /// those same observations. If the two disagree, one of the numbers on
    /// somebody's screen is wrong — and before this test the only thing
    /// checking it was me comparing figures by hand, which caught three
    /// disagreements in five days and would not have caught a fourth.
    ///
    /// The rows below are the shapes that actually broke it:
    ///
    /// - two pools at ONE address, told apart only by their key policy — the
    ///   `8fe8039d…` merge, worth 200,000,000 base;
    /// - two pools with NO instance key at all, at different addresses — most
    ///   of Minswap V2, which the old wire collapsed into one bucket;
    /// - a bonding curve, which `story::build` splits into its own kind;
    /// - a token-paired pool, which must stay `unresolved` on both sides;
    /// - an unmeasured far side, which must stay `Unusable` on both sides.
    #[test]
    fn observations_and_story_fold_identically() {
        let ada = |slot: u64, addr: &str, kp: u8, key: &[u8], base: i64, quote: Option<i64>| {
            let mut o = obs(slot, addr, key, base, quote);
            if let Some(d) = o.decoded.as_mut() {
                d.key_policy = vec![kp; 28];
            }
            o
        };
        let mut token_paired = obs(40, "addr_tok", b"T", 500, Some(9_000));
        if let Some(d) = token_paired.decoded.as_mut() {
            d.quote_policy = Some(vec![7u8; 28]);
            d.quote_name = Some(b"OTHER".to_vec());
        }
        let mut unmeasured = obs(45, "addr_unm", b"U", 500, None);
        if let Some(d) = unmeasured.decoded.as_mut() {
            d.quote_reserve = None;
        }
        let mut curve = obs(50, "addr_curve", b"", 700, Some(12_000));
        if let Some(d) = curve.decoded.as_mut() {
            d.pricing = pricing::BONDING_CURVE.into();
        }
        let mut unclaimed = obs(55, "addr_mystery", b"", 10, Some(10));
        unclaimed.decoded = None;

        let rows = vec![
            // Same address, different key POLICY — must stay two pools.
            ada(10, "addr_shared", 1, b"K", 100, Some(1_000)),
            ada(20, "addr_shared", 2, b"K", 300, Some(9_000)),
            // No instance key at all, two addresses — must stay two pools.
            ada(25, "addr_one", 0, b"", 400, Some(4_000)),
            ada(30, "addr_two", 0, b"", 600, Some(6_000)),
            // A second sighting, so "newest wins" is exercised on both paths.
            ada(35, "addr_one", 0, b"", 450, Some(5_000)),
            token_paired,
            unmeasured,
            curve,
            unclaimed,
        ];

        let story = crate::story::build(&[], &rows, &crate::trade::Roles::default());
        let from_obs = PolicyView::from_observations(&rows);
        let from_story = PolicyView::from_story(&story);

        assert_eq!(from_story.unresolved_pool_refs(), 0, "stream is malformed");
        assert_eq!(
            from_obs.pool_count(),
            from_story.pool_count(),
            "the two folds disagree about HOW MANY pools exist"
        );
        assert_eq!(from_obs.candidates(), from_story.candidates());

        // ⚠️ The whole projection, not just the headline. A spot that matches
        // while `stale` or `unpriceable` differs is still two different
        // stories about the same archive.
        let at = from_obs.ceiling().unwrap();
        let p = Projection::default();
        assert_eq!(from_obs.spot(at, &p), from_story.spot(at, &p));

        // And the fold really did separate everything it was meant to.
        let spot = from_obs.spot(at, &p);
        assert_eq!(spot.ada.as_ref().unwrap().pools, 4, "4 ADA pools");
        assert_eq!(spot.unresolved.len(), 1, "the token-paired one");
        assert_eq!(spot.unpriceable.len(), 1, "the curve");
    }

    #[test]
    fn undecoded_rows_are_counted_not_dropped_silently() {
        let mut row = obs(10, "a", b"K", 100, Some(1_000));
        row.decoded = None;
        let view = PolicyView::from_observations(&[row, obs(20, "a", b"K", 100, Some(1_000))]);
        assert_eq!(view.candidates(), 1);
        assert_eq!(view.claimed(), 1);
        // ⚠️ The ceiling moves for a candidate too: the archive HAS seen slot
        // 10, it just could not read what it saw.
        assert_eq!(view.ceiling(), Some(20));
    }
}
