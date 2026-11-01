//! A policy's whole life, folded ONCE, small enough to read from the edge.
//!
//! # Why this exists
//!
//! The flow explorer's token views drew every figure from the archive's rows
//! at request time. That holds for a token with a few thousand movements and
//! fails for one with hundreds of thousands. MEASURED on 2026-10-26 against
//! $NIKEPIG (331k transactions, 723k movements, 861 days):
//!
//! - the overview route read every observation and folded it inside the
//!   Worker, and died with Cloudflare's 1101 after 26 s;
//! - a rewind reads 5,000-movement pages one after another and stops after
//!   30 of them, which reaches about a fifth of the way back;
//! - the holder count the arena showed was the length of the page it asked
//!   for, not the number of holders.
//!
//! The box already folds the whole history when a walk completes (the series
//! and the balance table, [`crate::series`], [`crate::holders`]). This module
//! is a projection of those same rows into what a chart, a holder map and a
//! far rewind need, written beside them and published with the archive.
//!
//! # What is in it
//!
//! | | what | size on $NIKEPIG |
//! |---|---|---|
//! | [`Summary::daily`] | one [`Candle`] a day, the whole life, dense | 861 |
//! | [`Summary::hourly`] | one an hour, the last [`HOURLY_DAYS`] days | 720 |
//! | [`Summary::holders`] | a [`HolderDay`] a day: count, new, exits, top-10 share | 861 |
//! | [`Summary::distribution`] | balances by share band and by role, at the tip | one |
//! | [`Summary::top`] | the [`TOP_HOLDERS`] largest balances, classed | 500 |
//! | [`Summary::checkpoints`] | where each [`Checkpoint`] file sits | ~29 |
//!
//! # Checkpoints are what make a far rewind one read
//!
//! A page of history cannot know its own closing balances: they are the
//! opening balances of the page after it, so an older page settles only once
//! every newer page is loaded. A [`Checkpoint`] is every positive balance as
//! it stood BEFORE a given slot. A page that ends just below that slot closes
//! on the checkpoint, and settles on its own.
//!
//! Spacing is a fixed grid in movements, [`checkpoint_every`], so the files at
//! a given slot do not move when the archive grows and a publish uploads only
//! the new ones. Only a COMPLETE archive gets them: a balance needs every
//! movement since the mint, and a walk that has not reached it has none.
//!
//! # What it does NOT know
//!
//! Roles come from the caller ([`Classify`]): the address registry lives with
//! the box, not with this crate. A party is classed by the address it is shown
//! by, once. Liquidity is not here: [`crate::series::Series`] carries the
//! aggregate price and the valued trades, and nothing else.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::schema::Movement;
use crate::series::{DAY, HOUR, Series};

/// On-disk and R2 name of the summary, beside `graph.bin`.
pub const SUMMARY: &str = "summary.bin";
/// Bumped whenever any type here changes shape. Checked on byte zero before
/// anything is deserialised, as [`crate::SERIES_FORMAT`] is.
///
/// ⚠️ **2 adds [`Summary::decimals`] and [`Summary::segmentation`].** Postcard
/// encodes a struct as its fields in order, so a version-1 file has nothing for
/// the new fields to read and a version-1 read by this build fails deep inside
/// with "Hit the end of buffer" — which reads as a corrupt artifact when the
/// artifact is fine and the CACHE is old. Version 1 is refused on byte zero
/// instead, and the next fold rewrites it.
///
/// ⚠️ **3 adds [`Summary::unit`].** Same reason, same refusal: a version-2 file
/// has no unit to read, and every reader of this crate checks the byte before
/// it decodes anything.
pub const SUMMARY_FORMAT: u8 = 3;
/// The directory, beside the manifest, holding one file per checkpoint.
pub const CHECKPOINTS: &str = "checkpoints";
/// Bumped whenever [`Checkpoint`] changes shape.
pub const CHECKPOINT_FORMAT: u8 = 1;
/// Days of hourly candles kept: what a 1H, 24H or 7D chart needs, and a 30D
/// one at its finest.
pub const HOURLY_DAYS: u64 = 30;
/// Holders listed by name. The arena draws at most 400 creatures.
pub const TOP_HOLDERS: usize = 500;
/// Share bands, as parts per million of circulating supply: the arena's tier
/// floors (shrimp, crab, fish, dolphin, shark, whale, kraken), so its tiles can
/// count every holder and not only the ones it draws.
pub const DEFAULT_BANDS_PPM: [u32; 7] = [0, 100, 500, 2_500, 5_000, 10_000, 50_000];
/// The finest checkpoint spacing, in movements: five older pages.
pub const CHECKPOINT_BASE: u64 = 25_000;
/// The spacing doubles until there are at most this many.
pub const MAX_CHECKPOINTS: u64 = 128;

const PPM: u128 = 1_000_000;

/// What kind of holder a party is. Only a [`Role::Person`] is ranked, banded
/// or counted as a holder, as the arena counts them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Role {
    Person,
    /// Tokens here are gone for good.
    Burn,
    /// A vesting lock.
    Lock,
    Staking,
    /// A venue: a DEX pool, an order book, a marketplace or exchange wallet.
    Venue,
    /// A script nobody has named.
    Contract,
}

/// The caller's answer for one address: its role, and a name if it has one.
pub type Classify<'a> = &'a dyn Fn(&str) -> (Role, Option<String>);

/// One stretch of time. Prices are lovelace per RAW unit, as
/// [`crate::series::Point::spot`] is; volume is lovelace.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Candle {
    /// The stretch's start, unix seconds.
    pub t: u64,
    /// The price prevailing at `t`, the highest, lowest and last within it.
    /// `None` before the first ADA pool was seen, never 0.
    pub open: Option<f64>,
    pub high: Option<f64>,
    pub low: Option<f64>,
    pub close: Option<f64>,
    /// Whether the price moved inside the stretch, or was carried into it.
    pub seen: bool,
    pub buy_lovelace: u64,
    pub sell_lovelace: u64,
    pub buys: u32,
    pub sells: u32,
    /// Trades that could not be valued: no ADA pool had been seen yet.
    pub unvalued: u32,
    /// Distinct transactions and movement rows in the stretch.
    pub txs: u32,
    pub movements: u32,
}

/// The holders on one day, as they stood at its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderDay {
    pub t: u64,
    /// Every party with a positive balance.
    pub parties: u32,
    /// Of those, the [`Role::Person`]s.
    pub people: u32,
    /// People who went from nothing to something during the day. A wallet
    /// that sold out and bought back counts again.
    pub new: u32,
    /// People who went from something to nothing during the day.
    pub exits: u32,
    /// The ten largest people's share of circulating supply, in ppm.
    pub top10_ppm: u32,
}

/// One share band at the tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Band {
    /// The band starts at this share of circulating supply, in ppm.
    pub floor_ppm: u32,
    pub people: u32,
    pub amount: u64,
}

/// Every party of one role at the tip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleTotal {
    pub role: Role,
    pub parties: u32,
    pub amount: u64,
}

/// How supply sits at the tip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Distribution {
    /// Σ of every positive balance.
    pub held: u64,
    /// Held by [`Role::Burn`] parties.
    pub burned: u64,
    /// `held − burned`: what bands and shares are measured against.
    pub circulating: u64,
    pub parties: u32,
    pub people: u32,
    pub bands: Vec<Band>,
    pub roles: Vec<RoleTotal>,
    /// The largest one, ten and hundred people's share, in ppm.
    pub top1_ppm: u32,
    pub top10_ppm: u32,
    pub top100_ppm: u32,
    /// Parties whose balance came out negative: inbound movements the archive
    /// is missing, as [`crate::holders::Fold::negative`] counts them.
    pub negative: u32,
}

/// One named holder.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopHolder {
    /// Stake credential hex, or the address where there is no stake part:
    /// [`crate::holders::key_of`].
    pub key: String,
    /// The address the party is shown by.
    pub address: String,
    pub amount: u64,
    pub role: Role,
    pub label: Option<String>,
    pub movements: u32,
    /// LP-decomposition attribution from a pool. ⚠️ **ADDITIVE** to
    /// [`TopHolder::amount`], unlike [`HolderEntry`]'s `lp_amount`: the archive's
    /// holder fold has never moved the pool's reserve into its providers, so a
    /// provider's raw balance does not include this. Never add the pool's full
    /// `amount` and the providers' `lp_amount` together; use
    /// [`Segmentation::lp`]'s `residual` for the pool's side.
    #[serde(default)]
    pub lp_amount: u64,
    /// Locked units beneficially owned by this party. ⚠️ **A SUBSET** of
    /// [`TopHolder::amount`]: a CrowdLock address delegates to its owner's
    /// stake credential, so the holder fold already counts the locked units in
    /// the row. `free = amount - vested`.
    ///
    /// Joined from the owner's stake credential where the lock delegates, and
    /// from [`VestLock::lock_address`] where it does not — a script that holds
    /// its own units is keyed by its address, not by the owner's credential.
    #[serde(default)]
    pub vested: u64,
}

/// A lock platform, for the vesting detail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VestStyle {
    Shield,
    CrowdLock,
    SnekFun,
    Other,
}

/// One lock position attributed to its owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VestLock {
    pub amount: u64,
    pub unlock_ts_ms: u64,
    pub lock_address: String,
    pub style: VestStyle,
}

/// Every lock a party beneficially owns at the tip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VestOwner {
    /// The owner's holder key.
    pub key: String,
    pub amount: u64,
    pub locks: Vec<VestLock>,
}

/// One LP provider's share of a pool's reserve.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LpProvider {
    /// The provider's holder key.
    pub key: String,
    /// LP tokens held (or staked) by the provider.
    pub lp_tokens: u64,
    /// The policy's units their LP share accounts for.
    pub amount: u64,
}

/// How much of a pool's reserve the decomposition could account for.
///
/// Externally tagged rather than `#[serde(tag = "status")]`: this travels
/// inside the postcard-encoded summary, and postcard cannot decode an
/// internally tagged enum — it buffers the variant through `deserialize_any`,
/// which postcard answers with `WontImplement`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LpStatus {
    /// Every provider was resolved from the LP policy's archive.
    Complete,
    /// Some LP remained in the residual (for example, staked in a farm the
    /// scan could not name).
    Partial { because: String },
    /// No claim is made: the LP policy's archive was absent or incomplete.
    Unavailable { because: String },
}

/// A CSwap pool's reserve split between its LP providers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LpDecomposition {
    pub pool_address: String,
    pub pool_key: String,
    /// The pool's raw balance of the policy, from the holder fold.
    pub reserve: u64,
    /// The LP supply the pool datum names.
    pub total_lp: u64,
    pub providers: Vec<LpProvider>,
    /// `reserve - Σ providers.amount`: the pool's own side. Use this instead of
    /// the pool holder's `amount` whenever the providers are displayed.
    pub residual: u64,
    pub status: LpStatus,
}

/// Tip-only decomposition of the raw holder table.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Segmentation {
    /// The slot the tip facts were read at.
    pub as_of: u64,
    #[serde(default)]
    pub vests: Vec<VestOwner>,
    #[serde(default)]
    pub lp: Option<LpDecomposition>,
}

/// Where one checkpoint file is, and what it holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRef {
    /// Balances are as they stood BEFORE this slot: every movement below it is
    /// folded in, nothing at or above it.
    pub slot: u64,
    /// The block time of the first movement at `slot`.
    pub unix: u64,
    /// Movement rows below `slot`.
    pub movements_before: u64,
    /// Positive balances in the file.
    pub parties: u32,
    /// Its path relative to the policy's directory.
    pub rel: String,
}

/// Every positive balance as it stood before [`Checkpoint::slot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub format: u8,
    pub policy: String,
    pub slot: u64,
    pub unix: u64,
    /// `(address the party is shown by, balance)`, largest first, then by
    /// address. One row per party, as [`crate::holders`] keys them.
    pub balances: Vec<(String, u64)>,
}

/// The whole summary.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub format: u8,
    pub policy: String,
    /// The policy's user unit — its asset name, HEX (`c373…`), not bytes.
    ///
    /// It is identity, not a figure: the unit is what a picture, a registry
    /// entry and a market route are all asked for by, and the summary is the
    /// one read every token view already makes. `decimals` above belongs to
    /// THIS unit, so a reader that carried the scale without the subject could
    /// not say what the scale applied to.
    ///
    /// `None` on a fold that saw no user unit (a policy whose rows are all
    /// plumbing — a reference twin, a royalty declaration).
    #[serde(default)]
    pub unit: Option<String>,
    /// The archive's immutable ceiling when this was folded: what makes it
    /// stale, as for [`crate::SeriesFile::immutable_to`].
    pub immutable_to: u64,
    /// The highest slot folded.
    pub settled_to: u64,
    /// Whether the archive reached the mint. Without it there are no
    /// checkpoints and the balances are a floor, not a fact.
    pub complete: bool,
    pub first_unix: u64,
    pub last_unix: u64,
    pub daily: Vec<Candle>,
    pub hourly: Vec<Candle>,
    pub holders: Vec<HolderDay>,
    pub distribution: Distribution,
    /// The largest parties by BALANCE, plus any LP provider that holds no units
    /// of the token itself — appended by [`Summary::apply_segmentation`], with
    /// `amount == 0` and the pool side in `lp_amount`. A reader that wants rows
    /// with a position selects on `amount > 0 || lp_amount > 0`.
    pub top: Vec<TopHolder>,
    pub checkpoints: Vec<CheckpointRef>,
    /// Display decimals. `None` when nobody has stated them — decimals are not
    /// on chain, so there is no rule to apply, only knowledge (the token
    /// registry's, or the curated shared table's). A reader that gets `None`
    /// renders RAW units and says so.
    ///
    /// `Some(0)` and `None` are different answers, which is why this is an
    /// `Option` rather than a defaulted number.
    #[serde(default)]
    pub decimals: Option<u8>,
    /// Trades over the whole life that could not be valued.
    pub unvalued: u64,
    /// LP and vesting attribution at the tip. Empty on a summary folded before
    /// the decomposition existed; see [`Summary::apply_segmentation`].
    #[serde(default)]
    pub segmentation: Segmentation,
}

impl Summary {
    /// Attach the tip decomposition and copy its per-holder totals onto the
    /// matching [`TopHolder`]s, so a reader that only wants `free / lp /
    /// vested` does not have to join two tables.
    pub fn apply_segmentation(&mut self, seg: Segmentation) {
        use std::collections::{HashMap, HashSet};

        let lp: HashMap<&str, u64> = seg
            .lp
            .as_ref()
            .map(|lp| {
                lp.providers
                    .iter()
                    .map(|p| (p.key.as_str(), p.amount))
                    .collect()
            })
            .unwrap_or_default();
        let vests: HashMap<&str, u64> = seg
            .vests
            .iter()
            .map(|v| (v.key.as_str(), v.amount))
            .collect();
        // A lock script that does NOT delegate holds its units itself, so the
        // holder fold keys that row by the script ADDRESS while the vest scan
        // attributes the lock to the OWNER's stake credential. The lock address
        // is the join left over; sum per address, because one script may serve
        // several owners. A row that matched by key never consults this, which
        // is what keeps a delegating CrowdLock from being counted twice.
        let vests_by_lock: HashMap<&str, u64> =
            seg.vests
                .iter()
                .flat_map(|v| v.locks.iter())
                .fold(HashMap::new(), |mut out, lock| {
                    *out.entry(lock.lock_address.as_str()).or_default() += lock.amount;
                    out
                });
        for h in &mut self.top {
            h.lp_amount = lp.get(h.key.as_str()).copied().unwrap_or(0);
            let vested = vests
                .get(h.key.as_str())
                .copied()
                .or_else(|| vests_by_lock.get(h.address.as_str()).copied())
                .unwrap_or(0);
            // Keeping `vested` within `amount` is what makes
            // `free = amount - vested` true, and it stops a lock that outlived
            // its units from wrapping.
            h.vested = vested.min(h.amount);
        }

        // A provider can hold LP and no units of the token itself, and then the
        // holder fold has no row for it: `distribution` ranks parties by
        // BALANCE and skips a non-positive one. Appended here rather than kept
        // there, because such a party is not a holder of this token and must
        // not move `parties`, the share bands or the role totals — but it does
        // have a position, and a table that omits it says the pool's reserve
        // came from nowhere.
        //
        // ⚠️ These rows are why a reader selects on `amount > 0 || lp_amount > 0`
        // and never on `amount > 0` alone.
        let known: HashSet<&str> = self.top.iter().map(|h| h.key.as_str()).collect();
        let mut missing: Vec<(&str, u64)> = lp
            .iter()
            .filter(|(key, amount)| **amount > 0 && !known.contains(*key))
            .map(|(key, amount)| (*key, *amount))
            .collect();
        // Biggest pool position first, then the key. `lp` is a HashMap, so
        // emitting these in arrival order would not be byte-stable.
        missing.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
        self.top.extend(missing.into_iter().map(|(key, lp_amount)| TopHolder {
            key: key.to_string(),
            // No address: this party never moved the token, so the archive has
            // none to give. A name is asked for by the key instead.
            address: String::new(),
            amount: 0,
            role: Role::Person,
            label: None,
            movements: 0,
            lp_amount,
            vested: 0,
        }));

        self.segmentation = seg;
    }
}

/// What [`build`] folds.
pub struct Inputs<'a> {
    pub policy: &'a str,
    /// The policy's user unit, asset name HEX, where the caller knows it.
    /// Computed there rather than here: which unit is the user's is
    /// `cardano-assets`' rule, and this crate folds whatever it is handed.
    pub unit: Option<&'a str>,
    /// The whole life's series, as [`crate::series::fold`] made it.
    pub series: &'a Series,
    /// Every movement in the archive, in any order.
    pub movements: &'a [Movement],
    pub immutable_to: u64,
    pub complete: bool,
    /// The token's display decimals, where the caller knows them; `None` renders
    /// raw. Passed in rather than looked up here: the archive crate owns no
    /// registry.
    pub decimals: Option<u8>,
    pub classify: Classify<'a>,
}

/// The checkpoint spacing for an archive of `total` movements:
/// [`CHECKPOINT_BASE`], doubled until there are at most [`MAX_CHECKPOINTS`].
///
/// A ladder rather than `total / MAX_CHECKPOINTS` so the grid does not move
/// with every walk: the files at a given slot stay the same files, and a
/// publish uploads only the new ones until the archive doubles.
pub fn checkpoint_every(total: u64) -> u64 {
    let mut every = CHECKPOINT_BASE;
    while total / every > MAX_CHECKPOINTS {
        every *= 2;
    }
    every
}

/// The relative path of a checkpoint: its slot, and a hash of its bytes.
///
/// ⚠️ The hash is load-bearing. The balances below a slot are NOT fixed for
/// ever: a later pass can resolve the source of an old movement (a correction)
/// and move a balance that a checkpoint already recorded. Named by slot alone,
/// the new file would carry the old name, a publish that skips what R2 already
/// holds would keep the stale one, and its year-long immutable cache would
/// serve it regardless. Named by content, a changed file is a new object.
pub fn checkpoint_rel(slot: u64, bytes: &[u8]) -> String {
    format!("{CHECKPOINTS}/cp-{slot}-{:016x}.bin", fnv1a(bytes))
}

/// FNV-1a, 64-bit: a name for some bytes, not a defence against anyone.
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ *b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

struct Party {
    address: String,
    amount: i64,
    movements: u32,
    role: Role,
    label: Option<String>,
}

/// Running counts the day series is read off.
#[derive(Default)]
struct Counts {
    parties: u32,
    people: u32,
    new: u32,
    exits: u32,
}

/// Fold the summary, and the checkpoint files it names.
pub fn build(inp: &Inputs) -> (Summary, Vec<Checkpoint>) {
    use std::collections::HashMap;

    let mut rows: Vec<&Movement> = inp.movements.iter().collect();
    // The chain's order: a block at a time, and its transactions in place.
    rows.sort_by(|a, b| {
        (a.slot, a.tx_index, &a.tx_hash, &a.address)
            .cmp(&(b.slot, b.tx_index, &b.tx_hash, &b.address))
    });

    let first_unix = rows
        .iter()
        .map(|m| m.block_time)
        .chain(inp.series.points.iter().map(|p| p.unix))
        .min()
        .unwrap_or(0);
    let last_unix = rows
        .iter()
        .map(|m| m.block_time)
        .chain(inp.series.points.iter().map(|p| p.unix))
        .chain(inp.series.trades.iter().map(|t| t.unix))
        .max()
        .unwrap_or(0);
    let hourly_from =
        (last_unix.saturating_sub(HOURLY_DAYS * DAY) / HOUR * HOUR).max(first_unix / HOUR * HOUR);

    let every = checkpoint_every(rows.len() as u64);
    let mut next_checkpoint = every;

    let mut parties: HashMap<String, Party> = HashMap::new();
    // Address → key, filled as the fold meets each address. See the entry below.
    let mut keys: HashMap<&str, String> = HashMap::new();
    let mut counts = Counts::default();
    let mut days: Vec<HolderDay> = Vec::new();
    let mut day: Option<u64> = None;
    let mut day_changed = false;
    let mut last_top10 = 0u32;
    let mut daily_activity: HashMap<u64, (u32, u32)> = HashMap::new();
    let mut hourly_activity: HashMap<u64, (u32, u32)> = HashMap::new();
    let mut checkpoints: Vec<Checkpoint> = Vec::new();
    let mut refs: Vec<CheckpointRef> = Vec::new();
    let mut prev: Option<(&[u8], u64)> = None;
    let mut settled_to = 0u64;

    for (i, m) in rows.iter().enumerate() {
        settled_to = settled_to.max(m.slot);
        // A checkpoint at the first block boundary past each grid line, taken
        // BEFORE this block is folded: everything below its slot is in.
        if inp.complete
            && i as u64 >= next_checkpoint
            && prev.is_some_and(|(_, slot)| slot < m.slot)
        {
            let cp = checkpoint(inp.policy, m.slot, m.block_time, &parties);
            refs.push(CheckpointRef {
                slot: cp.slot,
                unix: cp.unix,
                movements_before: i as u64,
                parties: cp.balances.len() as u32,
                // Postcard into a Vec cannot fail for these types; an empty
                // body would only name the file by its slot's hash of nothing.
                rel: checkpoint_rel(cp.slot, &cp.encode().unwrap_or_default()),
            });
            checkpoints.push(cp);
            next_checkpoint = (i as u64 / every + 1) * every;
        }

        // Close every day before this movement's.
        let today = m.block_time / DAY * DAY;
        match day {
            None => day = Some(today),
            Some(d) if d < today => {
                close_days(
                    &mut days,
                    d,
                    today,
                    &mut counts,
                    &parties,
                    day_changed,
                    &mut last_top10,
                );
                day = Some(today);
                day_changed = false;
            }
            Some(_) => {}
        }

        // Activity, per transaction and per row.
        let new_tx = prev.is_none_or(|(tx, slot)| tx != m.tx_hash.as_slice() || slot != m.slot);
        prev = Some((m.tx_hash.as_slice(), m.slot));
        let tick = |map: &mut HashMap<u64, (u32, u32)>, t: u64| {
            let e = map.entry(t).or_default();
            e.0 += new_tx as u32;
            e.1 += 1;
        };
        tick(&mut daily_activity, today);
        let hour = m.block_time / HOUR * HOUR;
        if hour >= hourly_from {
            tick(&mut hourly_activity, hour);
        }

        // Balances, keyed as the holder table keys them.
        if m.is_placeholder() || m.address.is_empty() {
            continue;
        }
        // Address → key, once per address rather than once per movement: the
        // key is a bech32 decode, and these rows repeat a few thousand
        // addresses millions of times.
        let key = keys
            .entry(m.address.as_str())
            .or_insert_with(|| crate::holders::key_of(&m.address));
        let party = parties.entry(key.clone()).or_insert_with(|| {
            let (role, label) = (inp.classify)(&m.address);
            Party {
                address: m.address.clone(),
                amount: 0,
                movements: 0,
                role,
                label,
            }
        });
        // The representative address is the smallest this party used, not the
        // first one this fold happened to see: the holder table folds the same
        // history in file order, and the two must name (and therefore label)
        // one party the same way.
        if m.address < party.address {
            party.address = m.address.clone();
            let (role, label) = (inp.classify)(&party.address);
            party.role = role;
            party.label = label;
        }
        let before = party.amount;
        party.amount += m.amount;
        party.movements += 1;
        let person = party.role == Role::Person;
        match (before > 0, party.amount > 0) {
            (false, true) => {
                counts.parties += 1;
                if person {
                    counts.people += 1;
                    counts.new += 1;
                }
            }
            (true, false) => {
                counts.parties -= 1;
                if person {
                    counts.people -= 1;
                    counts.exits += 1;
                }
            }
            _ => {}
        }
        day_changed = true;
    }
    if let Some(d) = day {
        close_days(
            &mut days,
            d,
            d + DAY,
            &mut counts,
            &parties,
            day_changed,
            &mut last_top10,
        );
    }

    let daily = candles(
        inp.series,
        DAY,
        first_unix / DAY * DAY,
        last_unix,
        &daily_activity,
    );
    let hourly = candles(inp.series, HOUR, hourly_from, last_unix, &hourly_activity);
    let (distribution, top) = distribution(&parties);

    (
        Summary {
            format: SUMMARY_FORMAT,
            policy: inp.policy.to_string(),
            unit: inp.unit.map(str::to_string),
            immutable_to: inp.immutable_to,
            settled_to,
            complete: inp.complete,
            decimals: inp.decimals,
            first_unix,
            last_unix,
            daily,
            hourly,
            holders: days,
            distribution,
            top,
            checkpoints: refs,
            unvalued: inp.series.unvalued,
            segmentation: Segmentation::default(),
        },
        checkpoints,
    )
}

/// Write a [`HolderDay`] for `from` and every quiet day up to `to`.
fn close_days(
    days: &mut Vec<HolderDay>,
    from: u64,
    to: u64,
    counts: &mut Counts,
    parties: &HashMap<String, Party>,
    changed: bool,
    last_top10: &mut u32,
) {
    // A top-10 share is a pass over every balance, so it is taken only on a
    // day something moved; a quiet day carries it.
    if changed {
        *last_top10 = top_share(parties, 10);
    }
    let mut t = from;
    while t < to {
        days.push(HolderDay {
            t,
            parties: counts.parties,
            people: counts.people,
            new: counts.new,
            exits: counts.exits,
            top10_ppm: *last_top10,
        });
        counts.new = 0;
        counts.exits = 0;
        t += DAY;
    }
}

fn circulating(parties: &HashMap<String, Party>) -> u64 {
    parties
        .values()
        .filter(|p| p.amount > 0 && p.role != Role::Burn)
        .map(|p| p.amount as u64)
        .sum()
}

fn ppm(part: u64, whole: u64) -> u32 {
    match whole {
        0 => 0,
        w => (part as u128 * PPM / w as u128) as u32,
    }
}

/// The `n` largest people's share of circulating supply, in ppm.
fn top_share(parties: &HashMap<String, Party>, n: usize) -> u32 {
    let mut people: Vec<u64> = parties
        .values()
        .filter(|p| p.amount > 0 && p.role == Role::Person)
        .map(|p| p.amount as u64)
        .collect();
    let top: u64 = match people.len() > n {
        true => {
            people.select_nth_unstable_by(n - 1, |a, b| b.cmp(a));
            people[..n].iter().sum()
        }
        false => people.iter().sum(),
    };
    ppm(top, circulating(parties))
}

fn checkpoint(policy: &str, slot: u64, unix: u64, parties: &HashMap<String, Party>) -> Checkpoint {
    let mut balances: Vec<(String, u64)> = parties
        .values()
        .filter(|p| p.amount > 0)
        .map(|p| (p.address.clone(), p.amount as u64))
        .collect();
    balances.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Checkpoint {
        format: CHECKPOINT_FORMAT,
        policy: policy.to_string(),
        slot,
        unix,
        balances,
    }
}

fn distribution(parties: &HashMap<String, Party>) -> (Distribution, Vec<TopHolder>) {
    let circulating = circulating(parties);
    let mut held = 0u64;
    let mut burned = 0u64;
    let mut negative = 0u32;
    let mut roles: HashMap<Role, (u32, u64)> = HashMap::new();
    let mut bands: Vec<Band> = DEFAULT_BANDS_PPM
        .iter()
        .map(|&floor_ppm| Band {
            floor_ppm,
            people: 0,
            amount: 0,
        })
        .collect();
    let mut positive: Vec<(&String, &Party)> = Vec::new();
    for (key, p) in parties {
        if p.amount < 0 {
            negative += 1;
        }
        if p.amount <= 0 {
            continue;
        }
        let amount = p.amount as u64;
        held += amount;
        if p.role == Role::Burn {
            burned += amount;
        }
        let r = roles.entry(p.role).or_default();
        r.0 += 1;
        r.1 += amount;
        if p.role == Role::Person {
            let share = ppm(amount, circulating);
            if let Some(b) = bands.iter_mut().rev().find(|b| share >= b.floor_ppm) {
                b.people += 1;
                b.amount += amount;
            }
        }
        positive.push((key, p));
    }
    positive.sort_by(|a, b| b.1.amount.cmp(&a.1.amount).then_with(|| a.0.cmp(b.0)));

    let people: Vec<u64> = positive
        .iter()
        .filter(|(_, p)| p.role == Role::Person)
        .map(|(_, p)| p.amount as u64)
        .collect();
    let top_n = |n: usize| ppm(people.iter().take(n).sum(), circulating);

    let mut roles: Vec<RoleTotal> = roles
        .into_iter()
        .map(|(role, (parties, amount))| RoleTotal {
            role,
            parties,
            amount,
        })
        .collect();
    roles.sort_by_key(|r| r.role);

    let top = positive
        .iter()
        .take(TOP_HOLDERS)
        .map(|(key, p)| TopHolder {
            key: (*key).clone(),
            address: p.address.clone(),
            amount: p.amount as u64,
            role: p.role,
            label: p.label.clone(),
            movements: p.movements,
            lp_amount: 0,
            vested: 0,
        })
        .collect();

    (
        Distribution {
            held,
            burned,
            circulating,
            parties: positive.len() as u32,
            people: people.len() as u32,
            bands,
            roles,
            top1_ppm: top_n(1),
            top10_ppm: top_n(10),
            top100_ppm: top_n(100),
            negative,
        },
        top,
    )
}

/// Dense candles of `width` seconds from `from` (aligned) through `to`.
fn candles(
    series: &Series,
    width: u64,
    from: u64,
    to: u64,
    activity: &HashMap<u64, (u32, u32)>,
) -> Vec<Candle> {
    if to < from || width == 0 {
        return Vec::new();
    }
    let n = ((to - from) / width + 1) as usize;
    let mut out: Vec<Candle> = (0..n)
        .map(|i| {
            let t = from + i as u64 * width;
            let (txs, movements) = activity.get(&t).copied().unwrap_or_default();
            Candle {
                t,
                open: None,
                high: None,
                low: None,
                close: None,
                seen: false,
                buy_lovelace: 0,
                sell_lovelace: 0,
                buys: 0,
                sells: 0,
                unvalued: 0,
                txs,
                movements,
            }
        })
        .collect();
    let index = |unix: u64| {
        (unix >= from)
            .then(|| ((unix - from) / width) as usize)
            .filter(|&i| i < n)
    };

    // The price carried into the first stretch: the last change before it.
    let mut carried: Option<f64> = series
        .points
        .iter()
        .take_while(|p| p.unix < from)
        .last()
        .map(|p| p.spot);
    let mut points = series
        .points
        .iter()
        .skip_while(|p| p.unix < from)
        .peekable();
    for c in out.iter_mut() {
        c.open = carried;
        c.high = carried;
        c.low = carried;
        c.close = carried;
        while let Some(p) = points.next_if(|p| p.unix < c.t + width) {
            c.seen = true;
            c.open = c.open.or(Some(p.spot));
            c.high = Some(c.high.map_or(p.spot, |h| h.max(p.spot)));
            c.low = Some(c.low.map_or(p.spot, |l| l.min(p.spot)));
            c.close = Some(p.spot);
        }
        carried = c.close;
    }

    for t in &series.trades {
        let Some(i) = index(t.unix) else { continue };
        let c = &mut out[i];
        let lovelace = t.lovelace.max(0) as u64;
        if lovelace == 0 {
            c.unvalued += 1;
        }
        match t.sold {
            true => {
                c.sells += 1;
                c.sell_lovelace += lovelace;
            }
            false => {
                c.buys += 1;
                c.buy_lovelace += lovelace;
            }
        }
    }
    out
}

#[derive(Debug, thiserror::Error)]
pub enum SummaryError {
    #[error(
        "{what} is format {found}, this build reads {expected} — a stale copy, not a broken one"
    )]
    Version {
        what: &'static str,
        found: u8,
        expected: u8,
    },
    #[error("{0} is empty")]
    Empty(&'static str),
    #[error("decoding the {what}: {source}")]
    Decode {
        what: &'static str,
        source: postcard::Error,
    },
    #[error("encoding: {0}")]
    Encode(#[from] postcard::Error),
}

fn checked<'a, T: Deserialize<'a>>(
    bytes: &'a [u8],
    what: &'static str,
    expected: u8,
) -> Result<T, SummaryError> {
    match bytes.first() {
        Some(&f) if f == expected => {}
        Some(&found) => {
            return Err(SummaryError::Version {
                what,
                found,
                expected,
            });
        }
        None => return Err(SummaryError::Empty(what)),
    }
    postcard::from_bytes(bytes).map_err(|source| SummaryError::Decode { what, source })
}

impl Summary {
    pub fn encode(&self) -> Result<Vec<u8>, SummaryError> {
        Ok(postcard::to_stdvec(self)?)
    }

    /// Version-checked decode, on byte zero before anything else is trusted.
    pub fn decode(bytes: &[u8]) -> Result<Self, SummaryError> {
        checked(bytes, "summary", SUMMARY_FORMAT)
    }

    /// The checkpoint at or after `slot` nearest to it: the one a page that
    /// should reach back to `slot` closes on.
    pub fn checkpoint_after(&self, slot: u64) -> Option<&CheckpointRef> {
        self.checkpoints.iter().find(|c| c.slot >= slot)
    }

    /// The checkpoint before exactly `slot`, if there is one.
    pub fn checkpoint_at(&self, slot: u64) -> Option<&CheckpointRef> {
        self.checkpoints.iter().find(|c| c.slot == slot)
    }

    /// Every checkpoint file this summary names, relative to the policy.
    pub fn files(&self) -> HashSet<String> {
        self.checkpoints.iter().map(|c| c.rel.clone()).collect()
    }
}

impl Checkpoint {
    pub fn encode(&self) -> Result<Vec<u8>, SummaryError> {
        Ok(postcard::to_stdvec(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SummaryError> {
        checked(bytes, "checkpoint", CHECKPOINT_FORMAT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::series::{Point, Trade};

    const A: &str = "addr_a";
    const B: &str = "addr_b";
    const BURN: &str = "addr_burn";

    fn mv(slot: u64, unix: u64, tx: u8, addr: &str, amount: i64) -> Movement {
        Movement {
            slot,
            block_time: unix,
            tx_hash: vec![tx],
            unit_name: b"U".to_vec(),
            address: addr.into(),
            amount,
            net_mint: 0,
            tx_index: 0,
        }
    }

    fn classify(address: &str) -> (Role, Option<String>) {
        match address {
            BURN => (Role::Burn, Some("burn address".into())),
            _ => (Role::Person, None),
        }
    }

    /// Same stake key, two payment addresses — the live Aliens pool. The two
    /// folds used to pick different representatives and label it differently.
    const POOL_A: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";
    const POOL_B: &str = "addr1xxg94wrfjcdsjncmsxtj0r87zk69e0jfl28n934sznu95tdj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrs2993lw";

    fn labelled(address: &str) -> (Role, Option<String>) {
        match address {
            POOL_A => (Role::Venue, Some("Splash exchange".into())),
            POOL_B => (Role::Venue, Some("snek.fun bonding curve".into())),
            _ => (Role::Person, None),
        }
    }

    #[test]
    fn a_partys_representative_is_the_smallest_address_and_its_label_follows() {
        let movements = vec![mv(1, 10, 1, POOL_B, 100), mv(1, 10, 2, POOL_A, 300)];
        let (s, _) = build(&Inputs {
            policy: "p",
            unit: None,
            series: &Series::default(),
            movements: &movements,
            immutable_to: 0,
            complete: true,
            decimals: None,
            classify: &labelled,
        });
        let canonical = POOL_A.min(POOL_B);
        let holder = s
            .top
            .iter()
            .find(|h| h.key == crate::holders::key_of(POOL_A))
            .expect("one party");
        assert_eq!(holder.address, canonical);
        assert_eq!(
            holder.label.as_deref(),
            Some(if canonical == POOL_A {
                "Splash exchange"
            } else {
                "snek.fun bonding curve"
            })
        );
    }

    fn summary(
        movements: &[Movement],
        series: &Series,
        complete: bool,
    ) -> (Summary, Vec<Checkpoint>) {
        build(&Inputs {
            policy: "p",
            unit: None,
            series,
            movements,
            immutable_to: 0,
            complete,
            decimals: None,
            classify: &classify,
        })
    }

    #[test]
    fn segmentation_copies_lp_and_vested_totals_onto_the_holders() {
        let movements = vec![mv(1, 10, 1, A, 100), mv(1, 10, 2, B, 50)];
        let (mut s, _) = summary(&movements, &Series::default(), true);
        let a = crate::holders::key_of(A);
        let b = crate::holders::key_of(B);
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: vec![VestOwner {
                key: a.clone(),
                amount: 40,
                locks: vec![VestLock {
                    amount: 40,
                    unlock_ts_ms: 1_800_000_000_000,
                    lock_address: "addr_lock".into(),
                    style: VestStyle::CrowdLock,
                }],
            }],
            lp: Some(LpDecomposition {
                pool_address: "addr_pool".into(),
                pool_key: "pool".into(),
                reserve: 100,
                total_lp: 1_000,
                providers: vec![LpProvider {
                    key: a.clone(),
                    lp_tokens: 100,
                    amount: 30,
                }],
                residual: 70,
                status: LpStatus::Complete,
            }),
        });
        let ha = s.top.iter().find(|h| h.key == a).expect("A");
        let hb = s.top.iter().find(|h| h.key == b).expect("B");
        assert_eq!(ha.vested, 40, "vested is a subset of the amount");
        assert_eq!(ha.lp_amount, 30, "lp is additive attribution");
        assert_eq!(hb.vested, 0);
        assert_eq!(hb.lp_amount, 0);
        assert_eq!(s.segmentation.as_of, 10);
        assert_eq!(s.segmentation.lp.as_ref().unwrap().residual, 70);
    }

    /// A provider can hold LP and no units of the token, and then the holder
    /// fold has no row to copy onto — the reserve would be attributed to nobody
    /// and the pool would read as having come from nowhere. It gets its own row
    /// instead, which must not move the holder counts.
    #[test]
    fn a_provider_with_no_balance_gets_a_row_without_becoming_a_holder() {
        let movements = vec![mv(1, 10, 1, A, 100)];
        let (mut s, _) = summary(&movements, &Series::default(), true);
        let parties = s.distribution.parties;
        let held = s.distribution.held;
        let provider = |key: &str, lp_tokens: u64, amount: u64| LpProvider {
            key: key.into(),
            lp_tokens,
            amount,
        };
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: Vec::new(),
            lp: Some(LpDecomposition {
                pool_address: "addr_pool".into(),
                pool_key: "pool".into(),
                reserve: 100,
                total_lp: 1_000,
                providers: vec![provider("bb", 10, 5), provider("cc", 20, 20)],
                residual: 75,
                status: LpStatus::Complete,
            }),
        });
        let extra: Vec<&TopHolder> = s.top.iter().filter(|h| h.amount == 0).collect();
        assert_eq!(extra.len(), 2, "both providers get a row");
        assert_eq!(
            extra
                .iter()
                .map(|h| (h.key.as_str(), h.lp_amount))
                .collect::<Vec<_>>(),
            vec![("cc", 20), ("bb", 5)],
            "the bigger pool position sorts first, so a re-fold is stable"
        );
        assert!(
            extra
                .iter()
                .all(|h| h.address.is_empty() && h.role == Role::Person),
            "no address to give, and no role to claim"
        );
        assert_eq!(
            s.distribution.parties, parties,
            "a provider is not a holder of this token"
        );
        assert_eq!(s.distribution.held, held, "and moves no supply");
    }

    /// `vested` must never exceed the raw balance: a caller subtracting it to
    /// get `free` would otherwise wrap.
    #[test]
    fn vested_is_clamped_to_the_holder_amount() {
        let movements = vec![mv(1, 10, 1, A, 10)];
        let (mut s, _) = summary(&movements, &Series::default(), true);
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: vec![VestOwner {
                key: crate::holders::key_of(A),
                amount: 999,
                locks: Vec::new(),
            }],
            lp: None,
        });
        assert_eq!(s.top[0].vested, 10);
    }

    /// A lock script that does not delegate holds its units at its own
    /// address, so the holder row is keyed by that address while the vest scan
    /// names the owner's stake credential. The lock address is the join that
    /// remains; without it the row reads as entirely free. 4,063,508 units is
    /// the snek.fun vesting script measured on $Aliens, 2026-10-31.
    #[test]
    fn a_non_delegating_lock_script_gets_its_vested_by_address() {
        const SCRIPT: &str = "addr_vesting_script";
        let movements = vec![mv(1, 10, 1, SCRIPT, 4_063_508)];
        let (mut s, _) = summary(&movements, &Series::default(), true);
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: vec![VestOwner {
                key: crate::holders::key_of(A),
                amount: 4_063_508,
                locks: vec![VestLock {
                    amount: 4_063_508,
                    unlock_ts_ms: 0,
                    lock_address: SCRIPT.into(),
                    style: VestStyle::SnekFun,
                }],
            }],
            lp: None,
        });
        let row = s
            .top
            .iter()
            .find(|h| h.address == SCRIPT)
            .expect("the script's row");
        assert_eq!(row.vested, 4_063_508);
        assert_eq!(row.amount - row.vested, 0, "no free part");
    }

    /// The fallback must not ADD to a key match: a delegating CrowdLock is
    /// named by its owner's credential and its units sit at the owner's row,
    /// so consulting the lock address as well would double them.
    #[test]
    fn a_key_match_is_not_summed_with_a_lock_address_match() {
        let movements = vec![mv(1, 10, 1, A, 40)];
        let (mut s, _) = summary(&movements, &Series::default(), true);
        let key = s.top[0].key.clone();
        let address = s.top[0].address.clone();
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: vec![VestOwner {
                key,
                amount: 40,
                locks: vec![VestLock {
                    amount: 40,
                    unlock_ts_ms: 0,
                    lock_address: address,
                    style: VestStyle::CrowdLock,
                }],
            }],
            lp: None,
        });
        assert_eq!(s.top[0].vested, 40);
    }

    #[test]
    fn candles_carry_the_price_through_quiet_stretches() {
        let series = Series {
            points: vec![
                Point {
                    slot: 1,
                    unix: 10,
                    spot: 2.0,
                },
                Point {
                    slot: 2,
                    unix: 20,
                    spot: 4.0,
                },
                Point {
                    slot: 3,
                    unix: 2 * DAY + 5,
                    spot: 1.0,
                },
            ],
            trades: vec![
                Trade {
                    slot: 2,
                    unix: 20,
                    units: 5,
                    lovelace: 20,
                    sold: false,
                },
                Trade {
                    slot: 3,
                    unix: 2 * DAY + 5,
                    units: 5,
                    lovelace: 5,
                    sold: true,
                },
            ],
            unvalued: 0,
        };
        let rows = [mv(1, 10, 1, A, 100), mv(3, 2 * DAY + 5, 2, A, -5)];
        let (s, _) = summary(&rows, &series, true);
        assert_eq!(s.daily.len(), 3, "dense: the quiet day is there");
        let d0 = s.daily[0];
        assert_eq!(
            (d0.open, d0.high, d0.low, d0.close),
            (Some(2.0), Some(4.0), Some(2.0), Some(4.0))
        );
        assert_eq!((d0.buys, d0.buy_lovelace, d0.txs), (1, 20, 1));
        let quiet = s.daily[1];
        assert!(!quiet.seen);
        assert_eq!(quiet.close, Some(4.0), "a quiet day carries the close");
        let d2 = s.daily[2];
        assert_eq!(
            (d2.open, d2.low, d2.close),
            (Some(4.0), Some(1.0), Some(1.0))
        );
        assert_eq!((d2.sells, d2.sell_lovelace), (1, 5));
    }

    #[test]
    fn a_price_before_any_pool_is_none_not_zero() {
        let series = Series::default();
        let (s, _) = summary(&[mv(1, 10, 1, A, 100)], &series, true);
        assert_eq!(s.daily[0].close, None);
        assert_eq!(s.daily[0].txs, 1);
    }

    #[test]
    fn holder_days_count_arrivals_exits_and_carry_quiet_days() {
        let rows = [
            mv(1, 10, 1, A, 100),
            mv(1, 10, 1, B, 50),
            mv(2, 2 * DAY + 1, 2, A, -100),
            mv(2, 2 * DAY + 1, 2, BURN, 100),
        ];
        let (s, _) = summary(&rows, &Series::default(), true);
        assert_eq!(s.holders.len(), 3);
        let d0 = s.holders[0];
        assert_eq!((d0.parties, d0.people, d0.new, d0.exits), (2, 2, 2, 0));
        let quiet = s.holders[1];
        assert_eq!((quiet.people, quiet.new, quiet.exits), (2, 0, 0));
        let d2 = s.holders[2];
        assert_eq!(
            (d2.parties, d2.people, d2.new, d2.exits),
            (2, 1, 0, 1),
            "A sold into the burn: one person left, the sink a party but no person"
        );
        assert_eq!(d2.top10_ppm, 1_000_000, "B holds all that circulates");
    }

    #[test]
    fn distribution_bands_people_and_sets_burns_apart() {
        let rows = [
            mv(1, 10, 1, A, 900),
            mv(1, 10, 1, B, 100),
            mv(1, 10, 1, BURN, 1_000),
            mv(1, 10, 1, "addr_c", -5),
        ];
        let (s, _) = summary(&rows, &Series::default(), true);
        let d = &s.distribution;
        assert_eq!((d.held, d.burned, d.circulating), (2_000, 1_000, 1_000));
        assert_eq!((d.parties, d.people, d.negative), (3, 2, 1));
        assert_eq!(d.top1_ppm, 900_000);
        assert_eq!(d.top10_ppm, 1_000_000);
        let kraken = d.bands.last().expect("a band");
        assert_eq!(
            (kraken.floor_ppm, kraken.people),
            (50_000, 2),
            "both hold over 5%"
        );
        assert_eq!(s.top[0].address, BURN);
        assert_eq!(s.top[0].role, Role::Burn);
        assert_eq!(s.top[0].label.as_deref(), Some("burn address"));
    }

    /// ⚠️ THE PROPERTY A CHECKPOINT EXISTS FOR: it is exactly the balances of
    /// every movement below its slot, and it falls on a block boundary.
    #[test]
    fn checkpoints_hold_every_balance_below_their_slot() {
        let every = CHECKPOINT_BASE;
        let mut rows = Vec::new();
        for i in 0..(every * 2 + 10) {
            let slot = i / 3 + 1; // three rows a block
            rows.push(mv(
                slot,
                slot,
                (i % 200) as u8,
                if i % 2 == 0 { A } else { B },
                1,
            ));
        }
        let (s, cps) = summary(&rows, &Series::default(), true);
        assert_eq!(cps.len(), 2);
        for (cp, r) in cps.iter().zip(&s.checkpoints) {
            assert_eq!(cp.slot, r.slot);
            assert_eq!(r.rel, checkpoint_rel(cp.slot, &cp.encode().unwrap()));
            assert!(r.rel.starts_with(&format!("{CHECKPOINTS}/cp-{}-", cp.slot)));
            let below: u64 = rows.iter().filter(|m| m.slot < cp.slot).count() as u64;
            assert_eq!(
                r.movements_before, below,
                "a checkpoint sits on a block boundary"
            );
            let held: u64 = cp.balances.iter().map(|(_, a)| a).sum();
            assert_eq!(held, below, "every row below the slot, none at it");
            assert_eq!(Checkpoint::decode(&cp.encode().unwrap()).unwrap(), *cp);
        }
        assert_eq!(
            s.checkpoint_after(cps[0].slot - 1).map(|c| c.slot),
            Some(cps[0].slot)
        );
        assert_eq!(
            s.checkpoint_at(cps[1].slot).map(|c| c.slot),
            Some(cps[1].slot)
        );
    }

    #[test]
    fn an_incomplete_archive_has_no_checkpoints() {
        let rows: Vec<Movement> = (0..CHECKPOINT_BASE * 2)
            .map(|i| mv(i + 1, i + 1, (i % 200) as u8, A, 1))
            .collect();
        let (s, cps) = summary(&rows, &Series::default(), false);
        assert!(cps.is_empty() && s.checkpoints.is_empty());
    }

    #[test]
    fn the_checkpoint_grid_stays_put_as_an_archive_grows() {
        assert_eq!(checkpoint_every(723_000), CHECKPOINT_BASE);
        assert_eq!(
            checkpoint_every(CHECKPOINT_BASE * MAX_CHECKPOINTS),
            CHECKPOINT_BASE
        );
        assert_eq!(
            checkpoint_every(CHECKPOINT_BASE * (MAX_CHECKPOINTS + 1)),
            CHECKPOINT_BASE * 2
        );
        assert!(10_000_000 / checkpoint_every(10_000_000) <= MAX_CHECKPOINTS);
    }

    #[test]
    fn the_summary_round_trips_and_refuses_another_build() {
        let (s, _) = summary(&[mv(1, 10, 1, A, 1)], &Series::default(), true);
        let bytes = s.encode().unwrap();
        assert_eq!(Summary::decode(&bytes).unwrap(), s);
        let mut future = bytes.clone();
        future[0] = SUMMARY_FORMAT + 1;
        assert!(matches!(
            Summary::decode(&future),
            Err(SummaryError::Version { .. })
        ));
        assert!(matches!(Summary::decode(&[]), Err(SummaryError::Empty(_))));
    }

    /// The unit is identity and rides beside the scale: a reader holding
    /// `decimals` alone could not say what the scale applies to. `Some("c373")`
    /// must survive the wire, and an absent unit must stay absent rather than
    /// becoming an empty string.
    #[test]
    fn the_unit_rides_with_the_summary_and_an_absent_one_stays_absent() {
        let classify = |_: &str| (Role::Person, None);
        let movements = vec![mv(1, 10, 1, A, 5)];
        let named = |unit: Option<&str>| {
            build(&Inputs {
                policy: "p",
                unit,
                series: &Series::default(),
                movements: &movements,
                immutable_to: 0,
                complete: true,
                decimals: None,
                classify: &classify,
            })
            .0
        };
        let with = named(Some("c373"));
        assert_eq!(with.unit.as_deref(), Some("c373"));
        assert_eq!(Summary::decode(&with.encode().unwrap()).unwrap(), with);
        assert_eq!(named(None).unit, None);
    }

    /// A summary from the format before `unit` must say it is STALE, not
    /// corrupt: the same payload minus one field is a prefix of this one, and
    /// decoding it as this one fails on the missing bytes.
    #[test]
    fn a_summary_from_the_previous_format_reads_as_stale() {
        let (s, _) = summary(&[mv(1, 10, 1, A, 1)], &Series::default(), true);
        let mut older = s.encode().unwrap();
        older[0] = SUMMARY_FORMAT - 1;
        match Summary::decode(&older) {
            Err(SummaryError::Version {
                found, expected, ..
            }) => {
                assert_eq!((found, expected), (SUMMARY_FORMAT - 1, SUMMARY_FORMAT));
            }
            other => panic!("wanted a version error, got {other:?}"),
        }
    }

    /// The segmentation rides inside the summary's postcard payload, so every
    /// shape in it must be postcard-representable. An internally tagged enum is
    /// not, and the failure only appears on the archive a walk wrote.
    #[test]
    fn a_segmented_summary_round_trips() {
        let (mut s, _) = summary(&[mv(1, 10, 1, A, 4)], &Series::default(), true);
        // The scale rides in the same payload, and `Some(6)` is not `None`.
        s.decimals = Some(6);
        s.apply_segmentation(Segmentation {
            as_of: 10,
            vests: vec![VestOwner {
                key: A.into(),
                amount: 1,
                locks: vec![VestLock {
                    amount: 1,
                    unlock_ts_ms: 1_700_000_000_000,
                    lock_address: "addr_lock".into(),
                    style: VestStyle::CrowdLock,
                }],
            }],
            lp: Some(LpDecomposition {
                pool_address: "addr_pool".into(),
                pool_key: "pool".into(),
                reserve: 1_000,
                total_lp: 500,
                providers: vec![LpProvider {
                    key: A.into(),
                    lp_tokens: 100,
                    amount: 200,
                }],
                residual: 800,
                status: LpStatus::Partial {
                    because: "some LP was staked in an unnamed farm".into(),
                },
            }),
        });
        let bytes = s.encode().unwrap();
        assert_eq!(Summary::decode(&bytes).unwrap(), s);
    }
}
