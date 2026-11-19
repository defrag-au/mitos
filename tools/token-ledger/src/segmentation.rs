//! Tip decomposition of a policy's raw holder table.
//!
//! The archive already carries the raw lock and pool datums in its observation
//! tier, so both decompositions are re-derived from the archive itself — no
//! chain scan and no second source. The two are deliberately different shapes:
//!
//! - **Vesting is a SUBSET of a holder's `amount`.** A CrowdLock address
//!   delegates to its owner's stake credential, so the holder fold already
//!   counts the locked units in that row. `free = amount - vested`.
//! - **LP is ADDITIVE to a holder's `amount`.** The pool is its own holder; the
//!   provider's raw balance has never included the pool's reserve. The pool's
//!   `residual` is the side no provider's share accounts for, and a reader must
//!   use that instead of the pool holder's full amount whenever providers are
//!   shown.
//!
//! A lock observation is a reading at the slot the lock output appeared, not a
//! guarantee it is still there. The scan scales the observed locks at an
//! address down to that address's CURRENT movement balance when the two
//! disagree; what it cannot name stays at the lock address rather than being
//! invented onto a wallet.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use mitos_dex_decode::cswap;
use policy_archive::summary::{
    LpDecomposition, LpProvider, LpStatus, Segmentation, VestLock, VestOwner, VestStyle,
};
use policy_archive::{Observation, holders};

use crate::archive::{
    History, LockFor, holders_from, load_manifest, policy_dir, read_observations,
};

/// Build the tip decomposition for one policy.
///
/// `movements` must already be filtered to user units, and `as_of` is the
/// summary's `settled_to`.
///
/// `lock`, when given, is taken for the LP policy while its archive is read:
/// that archive has a tail of its own, rewritten under its own landing lock.
pub fn build(
    dir: &Path,
    as_of: u64,
    observations: &[Observation],
    movements: &[policy_archive::Movement],
    lock: Option<LockFor>,
    registry: &mitos_registry::Registry,
) -> Segmentation {
    let by_payment = payment_map(movements);
    let by_address = address_balances(movements);
    let by_key = representative_addresses(movements);
    let vests = vesting(observations, &by_payment, &by_key, &by_address, registry);
    let lp = lp_decomposition(dir, observations, &by_address, lock);
    Segmentation { as_of, vests, lp }
}

/// An LP policy this policy's decomposition needs and does not have.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LpWanted {
    /// The LP token's policy, hex.
    pub policy: String,
    /// A slot at or below the LP token's first mint, for when the policy index
    /// does not know it: a day below the pool's first sighting here. A pool's LP
    /// is minted in the transaction that creates the pool, so the first datum
    /// naming that LP policy is at or after the mint, and a day under-shoots
    /// it. A floor too high would produce a short archive that calls itself
    /// complete — see the README's traps.
    pub floor: u64,
}

/// One day of slots: how far [`LpWanted::floor`] under-shoots the first pool
/// sighting.
const LP_FLOOR_MARGIN_SLOTS: u64 = 86_400;

/// The LP policy this archive's pool names, when that policy has no complete
/// archive beside this one.
///
/// Without it the decomposition reports the pool `Unavailable` and every
/// provider reads zero — MEASURED 2026-11-11 on $PERP: 61.2M units in the CSwap
/// pool attributed to nobody, because nothing had ever walked its LP token.
/// Walking it is small (50 transactions there), so the fold asks for it rather
/// than leaving it to someone to notice.
pub fn lp_wanted(dir: &Path, observations: &[Observation]) -> Option<LpWanted> {
    let (_, datum) = latest_cswap_pool(observations)?;
    if datum.lp_policy.is_empty() {
        return None;
    }
    let policy = hex::encode(&datum.lp_policy);
    let walked = dir
        .parent()
        .and_then(|root| load_manifest(&policy_dir(root, &policy)).ok().flatten())
        .is_some_and(|m| m.completeness() == policy_archive::Completeness::Complete);
    if walked {
        return None;
    }
    let first_sighting = observations
        .iter()
        .filter(|o| o.address == cswap::POOL_SCRIPT_ADDR)
        .filter(|o| {
            o.datum
                .as_deref()
                .and_then(cswap::decode_pool_datum)
                .is_some_and(|d| d.lp_policy == datum.lp_policy)
        })
        .map(|o| o.slot)
        .min()?;
    Some(LpWanted {
        policy,
        floor: first_sighting.saturating_sub(LP_FLOOR_MARGIN_SLOTS),
    })
}

/// A lock observation, before its address's current balance is applied.
struct PendingVest {
    owner: String,
    amount: u64,
    unlock_ts_ms: u64,
    style: VestStyle,
    /// Unix seconds the position's output appeared: when it was opened.
    opened_unix: u64,
}

/// One live lock or staking position, its amount scaled to what its address still holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub owner: String,
    pub lock_address: String,
    pub amount: u64,
    pub unlock_ts_ms: u64,
    pub style: VestStyle,
    /// Unix seconds the position was opened. A maturity rule reads this: a position opened
    /// inside its window is new, whatever its owner held before.
    pub opened_unix: u64,
}

/// Every live position the archive observed, attributed to its owner — what [`build`]'s vests
/// group by owner, flat and with each position's opening time.
pub fn positions(
    observations: &[Observation],
    movements: &[policy_archive::Movement],
    registry: &mitos_registry::Registry,
) -> Vec<Position> {
    let by_payment = payment_map(movements);
    let by_address = address_balances(movements);
    scaled_positions(observations, &by_payment, &by_address, registry)
}

fn payment_map(movements: &[policy_archive::Movement]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    // Address → its payment part, worked out once. `address_parts` is a bech32
    // decode plus two allocations, and these rows repeat a few thousand
    // addresses millions of times.
    let mut parts: HashMap<&str, Option<String>> = HashMap::new();
    for m in movements {
        let payment = parts.entry(m.address.as_str()).or_insert_with(|| {
            policy_archive::trade::address_parts(&m.address).map(|(payment, _)| payment)
        });
        let Some(payment) = payment.as_ref() else {
            continue;
        };
        if out.contains_key(payment) {
            continue;
        }
        out.insert(payment.clone(), holders::key_of(&m.address));
    }
    out
}

fn address_balances(movements: &[policy_archive::Movement]) -> HashMap<&str, i64> {
    let mut out: HashMap<&str, i64> = HashMap::new();
    for m in movements {
        // Borrowed rather than cloned: this is one allocation per ROW today, and
        // the map only ever holds one entry per distinct address.
        *out.entry(m.address.as_str()).or_default() += m.amount;
    }
    out
}

/// One address per holder key: the smallest it was seen by, which is the same
/// rule the summary's own [`policy_archive::summary::TopHolder::address`]
/// follows, so an owner unrolled out of a lock names the party the same way the
/// holder table would.
///
/// A lock's own address cannot serve: a CrowdLock script delegates to the
/// PLATFORM's stake key, so every unit it holds folds under `$crowdlock` rather
/// than under the wallet that locked it. The wallet's payment credential does
/// appear in the movement rows — as the sender of the deposit — which is what
/// this is for. `account_key` is a bech32 decode, so it is worked out once per
/// distinct address rather than once per row.
fn representative_addresses(movements: &[policy_archive::Movement]) -> HashMap<String, String> {
    let mut keys: HashMap<&str, String> = HashMap::new();
    let mut out: HashMap<String, String> = HashMap::new();
    for m in movements {
        let key = keys
            .entry(m.address.as_str())
            .or_insert_with(|| holders::key_of(&m.address));
        match out.get_mut(key) {
            Some(current) if current.as_str() <= m.address.as_str() => {}
            Some(current) => current.clone_from(&m.address),
            None => {
                out.insert(key.clone(), m.address.clone());
            }
        }
    }
    out
}

/// Every lock position the archive observed, attributed to its owner.
fn vesting(
    observations: &[Observation],
    by_payment: &HashMap<String, String>,
    by_key: &HashMap<String, String>,
    by_address: &HashMap<&str, i64>,
    registry: &mitos_registry::Registry,
) -> Vec<VestOwner> {
    let mut owners: BTreeMap<String, VestOwner> = BTreeMap::new();
    for position in scaled_positions(observations, by_payment, by_address, registry) {
        let owner = owners
            .entry(position.owner.clone())
            .or_insert_with(|| VestOwner {
                key: position.owner,
                address: String::new(),
                amount: 0,
                locks: Vec::new(),
            });
        owner.amount = owner.amount.saturating_add(position.amount);
        owner.locks.push(VestLock {
            amount: position.amount,
            unlock_ts_ms: position.unlock_ts_ms,
            lock_address: position.lock_address,
            style: position.style,
        });
    }
    // The address each owner is named by, where the policy's movements saw one.
    for owner in owners.values_mut() {
        if let Some(address) = by_key.get(&owner.key) {
            owner.address.clone_from(address);
        }
    }
    owners.into_values().collect()
}

/// Every lock observation attributed to its owner, each address's set scaled to what the address
/// still holds — a claimed lock's share leaves with it.
fn scaled_positions(
    observations: &[Observation],
    by_payment: &HashMap<String, String>,
    by_address: &HashMap<&str, i64>,
    registry: &mitos_registry::Registry,
) -> Vec<Position> {
    let mut pending: BTreeMap<String, Vec<PendingVest>> = BTreeMap::new();
    for o in observations {
        let Some((style, shape)) = vest_style(&o.address, registry) else {
            continue;
        };
        let Some(datum) = o.datum.as_deref() else {
            continue;
        };
        let Some((owner_pkh_hex, unlock_ts_ms)) = position_datum(shape, datum) else {
            continue;
        };
        let amount = u64::try_from(o.unit_amount).unwrap_or(0);
        if amount == 0 {
            continue;
        }
        pending
            .entry(o.address.clone())
            .or_default()
            .push(PendingVest {
                owner: owner_key(&o.address, &owner_pkh_hex, by_payment),
                amount,
                unlock_ts_ms,
                style,
                opened_unix: o.block_time,
            });
    }

    let mut out: Vec<Position> = Vec::new();
    for (address, items) in pending {
        let current = by_address
            .get(address.as_str())
            .copied()
            .unwrap_or(0)
            .max(0) as u64;
        if current == 0 {
            // Every lock at this address has been claimed.
            continue;
        }
        let observed: u64 = items.iter().map(|p| p.amount).sum();
        let amounts = scale_to(current, observed, items.iter().map(|p| p.amount).collect());
        for (item, amount) in items.into_iter().zip(amounts) {
            if amount == 0 {
                continue;
            }
            out.push(Position {
                owner: item.owner,
                lock_address: address.clone(),
                amount,
                unlock_ts_ms: item.unlock_ts_ms,
                style: item.style,
                opened_unix: item.opened_unix,
            });
        }
    }
    out
}

/// Scale observed amounts down to `current`, preserving their sum exactly.
///
/// `current` is the address's movement balance; `observed` is what the lock
/// observations say was locked, and `amounts` are those observations. A
/// partially claimed set is common, and a row that reports more locked than the
/// address holds would make `free = amount - vested` wrap. When `current >=
/// observed` nothing is scaled: the difference is simply locks nobody's datum
/// named.
///
/// `current` is distributed across the entries by weight, so the split stays
/// proportional even when the weights are not already a partition of `current`,
/// and a set of equal locks stays equal instead of the rounding loss landing on
/// whichever lock happens to be last.
fn scale_to(current: u64, observed: u64, amounts: Vec<u64>) -> Vec<u64> {
    if observed == 0 || current >= observed {
        return amounts;
    }
    let total: u128 = amounts.iter().map(|a| u128::from(*a)).sum();
    if total == 0 {
        return vec![0; amounts.len()];
    }
    let mut out = Vec::with_capacity(amounts.len());
    let mut assigned: u64 = 0;
    let mut cumulative: u128 = 0;
    for amount in &amounts {
        cumulative += u128::from(*amount);
        let target = (cumulative * u128::from(current) / total) as u64;
        out.push(target.saturating_sub(assigned));
        assigned = target;
    }
    out
}

fn vest_style(
    address: &str,
    registry: &mitos_registry::Registry,
) -> Option<(VestStyle, DatumShape)> {
    let cred = mitos_cohort::payment_cred(address)?;
    if mitos_vesting_decode::crowd_lock::is_crowd_lock(&cred) {
        Some((VestStyle::CrowdLock, DatumShape::Timed))
    } else if mitos_vesting_decode::snek_fun::is_snek_fun(&cred) {
        Some((VestStyle::SnekFun, DatumShape::Timed))
    } else if mitos_vesting_decode::perps_staking::is_perps_staking(&cred) {
        // Staked, not vesting: its datum's date is when it was staked. `Other` keeps the
        // summary's wire format, and the owner is what a reader of the vests needs.
        Some((VestStyle::Other, DatumShape::Timed))
    } else if mitos_vesting_decode::tsuru_staking::is_tsuru_staking(&cred) {
        // Staked, and the datum carries NO date at all — the owner-only shape.
        Some((VestStyle::Other, DatumShape::OwnerOnly))
    } else if registry.uses(&hex::encode(cred)).iter().any(|use_of| {
        matches!(
            use_of.category,
            mitos_registry::Category::Vesting
                | mitos_registry::Category::Lock
                // A registered staking site is the same claim as a registered
                // lock: the units at it belong to whoever the datum names.
                | mitos_registry::Category::Staking
        )
    }) {
        // Registered, but no decoder here reads it: still a position, with no
        // schedule — the same weaker evidence the cohort records.
        Some((VestStyle::Other, DatumShape::Timed))
    } else {
        None
    }
}

/// Which decoder reads a position's datum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DatumShape {
    /// The lock family: a timestamp and an owner. Its date is a deadline for a
    /// lock and, at a staking site, when the position was staked — never a
    /// maturity the decoder invents.
    Timed,
    /// An owner and nothing else: a staking site that records no date at all.
    OwnerOnly,
}

/// The owner a position's datum names, and the timestamp it carries when it
/// carries one.
///
/// An owner-only position reports `unlock_ts_ms = 0`, which is what a
/// `VestStyle::Other` row without a schedule already carries. The contract says
/// nothing about unlocking; the style is what tells a reader not to take a date
/// off the row rather than the decoder supplying a wrong one.
fn position_datum(shape: DatumShape, cbor: &[u8]) -> Option<(String, u64)> {
    match shape {
        DatumShape::Timed => {
            let decoded = mitos_vesting_decode::decode_vesting_datum(cbor)?;
            Some((decoded.owner_pkh_hex, decoded.unlock_ts_ms))
        }
        DatumShape::OwnerOnly => {
            let decoded = mitos_vesting_decode::decode_owner_only_datum(cbor)?;
            Some((decoded.owner_pkh_hex, 0))
        }
    }
}

/// The owner behind a lock address.
///
/// ⚠️ **The datum's owner outranks the address's own stake part.** A CrowdLock
/// script delegates to the PLATFORM's stake key, so `key_of` on a lock address
/// answers `$crowdlock` for every lock the script holds — which attributes other
/// people's units to the platform and gives every owner the same key. The
/// payment key hash in the datum is the only thing that distinguishes one locker
/// from another, so it is resolved against a wallet address seen in the movement
/// rows FIRST; the address's own key is the fallback for a lock that genuinely
/// delegates to its owner (or one this fold cannot place at all).
fn owner_key(address: &str, owner_pkh_hex: &str, by_payment: &HashMap<String, String>) -> String {
    // The datum's owner is tried FIRST, ahead of the address's own stake part.
    // A CrowdLock script delegates to the PLATFORM's stake key, so `key_of`
    // there answers `$crowdlock` for every lock it holds — attributing other
    // people's units to the platform. The payment credential the datum names is
    // the only thing that distinguishes one locker from another, and the wallet
    // appears in the movement rows as the sender of its deposit.
    by_payment
        .get(owner_pkh_hex)
        .cloned()
        .unwrap_or_else(|| holders::key_of(address))
}

/// The latest CSwap pool datum in the archive, with the address it sat at.
fn latest_cswap_pool(observations: &[Observation]) -> Option<(String, cswap::CswapPoolDatum)> {
    let mut best: Option<(u64, String, cswap::CswapPoolDatum)> = None;
    for o in observations {
        if o.address != cswap::POOL_SCRIPT_ADDR {
            continue;
        }
        let Some(datum) = o.datum.as_deref().and_then(cswap::decode_pool_datum) else {
            continue;
        };
        if best.as_ref().is_none_or(|(slot, ..)| o.slot > *slot) {
            best = Some((o.slot, o.address.clone(), datum));
        }
    }
    best.map(|(_, address, datum)| (address, datum))
}

fn lp_decomposition(
    dir: &Path,
    observations: &[Observation],
    by_address: &HashMap<&str, i64>,
    lock: Option<LockFor>,
) -> Option<LpDecomposition> {
    let (pool_address, datum) = latest_cswap_pool(observations)?;
    let reserve = by_address
        .get(pool_address.as_str())
        .copied()
        .unwrap_or(0)
        .max(0) as u64;
    let unavailable = |because: String| LpDecomposition {
        pool_key: holders::key_of(&pool_address),
        pool_address: pool_address.clone(),
        reserve,
        total_lp: datum.total_lp_tokens,
        providers: Vec::new(),
        residual: reserve,
        status: LpStatus::Unavailable { because },
    };
    if datum.total_lp_tokens == 0 {
        return Some(unavailable("the pool datum names no LP supply".into()));
    }
    let lp_policy = hex::encode(&datum.lp_policy);
    if lp_policy.is_empty() {
        return Some(unavailable("the pool datum names no LP policy".into()));
    }
    let Some(root) = dir.parent() else {
        return Some(unavailable("the archive has no root directory".into()));
    };
    let lp_dir = policy_dir(root, &lp_policy);
    // Held until the LP archive's reads below are done: the manifest, the
    // holder fold and the farm's observations all name its tip pass. Never for
    // this archive itself: the caller holds that lock and it is not re-entrant.
    let landing = lock
        .filter(|_| lp_dir != dir)
        .map(|lock_for| lock_for(&lp_policy));
    let _held = landing
        .as_ref()
        .map(|l| l.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
    let Some(manifest) = load_manifest(&lp_dir).ok().flatten() else {
        return Some(unavailable(format!(
            "the LP policy {lp_policy} has not been walked"
        )));
    };
    if manifest.completeness() != policy_archive::Completeness::Complete {
        return Some(unavailable(format!(
            "the LP policy {lp_policy}'s archive is incomplete"
        )));
    }
    let Some(mut history) = History::open(&lp_dir).ok().flatten() else {
        return Some(unavailable(format!(
            "the LP policy {lp_policy}'s archive cannot be opened"
        )));
    };
    let Ok(fold) = holders_from(&mut history) else {
        return Some(unavailable(format!(
            "the LP policy {lp_policy}'s holder table cannot be folded"
        )));
    };

    let mut providers: BTreeMap<String, Provider> = fold
        .holders
        .iter()
        .map(|h| {
            (
                h.key.clone(),
                Provider {
                    lp_tokens: h.amount.max(0) as u64,
                    address: h.address.clone(),
                },
            )
        })
        .collect();

    let farm_key = holders::key_of(cswap::FARM_SCRIPT_ADDR);
    let farm_lp = providers.remove(&farm_key).map_or(0, |p| p.lp_tokens);
    let mut partial = None;
    if farm_lp > 0 {
        let observations = read_observations(&lp_dir, &manifest).unwrap_or_default();
        let staked = staked_shares(&observations, &datum.lp_name);
        if staked.is_empty() {
            // The farm holds LP nobody's datum named. It stays a provider so
            // the reserve is still fully accounted for, but the report says so.
            //
            // No address: the farm is a script, so a name would be a name for
            // the farm rather than for whoever owns the LP. `key` is already
            // the farm's own address, which is what a reader showing the row
            // can shorten.
            providers.insert(
                farm_key,
                Provider {
                    lp_tokens: farm_lp,
                    address: String::new(),
                },
            );
            partial = Some("the farm's LP positions could not be named".to_string());
        } else {
            let observed: u64 = staked.values().sum();
            let keys: Vec<String> = staked.keys().cloned().collect();
            let amounts = scale_to(farm_lp, observed, staked.values().copied().collect());
            let mut allocated = 0u64;
            for (key, amount) in keys.into_iter().zip(amounts) {
                allocated = allocated.saturating_add(amount);
                // A staking datum names a credential and nothing else, so these
                // rows have no address to give and are named by their key.
                providers
                    .entry(key)
                    .or_insert_with(Provider::unnamed)
                    .lp_tokens += amount;
            }
            let farm_residual = farm_lp.saturating_sub(allocated);
            if farm_residual > 0 {
                providers.insert(
                    farm_key,
                    Provider {
                        lp_tokens: farm_residual,
                        address: String::new(),
                    },
                );
                partial = Some("part of the farm's LP remained un-named".to_string());
            }
        }
    }

    let (providers, residual) = lp_amounts(reserve, datum.total_lp_tokens, providers);
    let status = match partial {
        Some(because) => LpStatus::Partial { because },
        None => LpStatus::Complete,
    };
    Some(LpDecomposition {
        pool_key: holders::key_of(&pool_address),
        pool_address,
        reserve,
        total_lp: datum.total_lp_tokens,
        providers,
        residual,
        status,
    })
}

/// The staker's share of the farm, from its staking datums.
fn staked_shares(observations: &[Observation], lp_name: &[u8]) -> BTreeMap<String, u64> {
    let mut out: BTreeMap<String, u64> = BTreeMap::new();
    for o in observations {
        if o.address != cswap::FARM_SCRIPT_ADDR || o.unit_name != lp_name {
            continue;
        }
        let Some(datum) = o.datum.as_deref() else {
            continue;
        };
        let Some(cred) = cswap::decode_staking_datum(datum) else {
            continue;
        };
        let amount = u64::try_from(o.unit_amount).unwrap_or(0);
        if amount > 0 {
            *out.entry(hex::encode(cred)).or_default() += amount;
        }
    }
    out
}

/// One LP provider as the LP policy's fold names it: what it holds, and the
/// address it is shown by.
///
/// The address is the whole reason this is a struct rather than a bare count: a
/// provider that holds no units of the token being folded has no row in THIS
/// archive, so the LP policy's fold is the only place an address for it exists —
/// and an address is what `/api/handles` is asked about. See
/// [`policy_archive::summary::LpProvider`].
#[derive(Clone, Debug, Default)]
struct Provider {
    lp_tokens: u64,
    address: String,
}

impl Provider {
    /// A provider known only by a credential, as a farm's staking datum names
    /// one.
    fn unnamed() -> Self {
        Self::default()
    }
}

/// Turn LP token holdings into policy units, leaving the pool residual.
fn lp_amounts(
    reserve: u64,
    total_lp: u64,
    providers: BTreeMap<String, Provider>,
) -> (Vec<LpProvider>, u64) {
    let mut out = Vec::new();
    let mut attributed = 0u64;
    for (key, provider) in providers {
        let amount = mitos_dex_decode::lp_share(provider.lp_tokens, reserve, total_lp);
        attributed = attributed.saturating_add(amount);
        if amount > 0 {
            out.push(LpProvider {
                key,
                address: provider.address,
                lp_tokens: provider.lp_tokens,
                amount,
            });
        }
    }
    (out, reserve.saturating_sub(attributed))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The $PERP staking site, and two real positions on it (2026-11-10):
    /// 1,035,179 owned by `cce969f5…` and 328,938 by `8974b7a4…`.
    const SITE: &str = "addr1zyzayvten00vtt9tz0n8pdqs0kq62ujcznpdu7gq6s30md5uwrkupakg244k2dedgyzg0r6k0wgyplm2qdq0trqny98qv0xlua";
    const POSITION_A: &str =
        "d879821b0000019c247836ec581ccce969f5c0a16d5735feb093b883e4656f0dd4eb377dbc5d787391bf";
    const POSITION_B: &str =
        "d879821b0000019c247b4650581c8974b7a444f372d07f15abcc0ecd9729e79f9e196b72a0bb3fc5410c";

    fn position(slot: u64, tx: u8, datum: &str, amount: i64) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![tx; 32],
            address: SITE.into(),
            lovelace: 1_400_750,
            unit_name: b"PERP COIN".to_vec(),
            unit_amount: amount,
            datum: Some(hex::decode(datum).unwrap()),
            decoded: None,
            output_index: Some(0),
            spent: false,
        }
    }

    /// The $TOOL staking site, and a real position on it captured from chain
    /// 2026-11-19 (42,568 $TOOL owned by `6ec2284b…`). Its datum is the
    /// owner-only shape — one hash, no date.
    const TOOL_SITE: &str = "addr1wxcwhhtgh5q08f3nc5yhlcpy845lkfat4ntld9vma33uf3q4fagjy";
    const TOOL_POSITION: &str =
        "d87981581c6ec2284b5cd330d2c140b8472cd8ff71ccb2c66d50e563f51d8c710f";
    const TOOL_OWNER_PKH: &str = "6ec2284b5cd330d2c140b8472cd8ff71ccb2c66d50e563f51d8c710f";

    /// The site holds 44.2% of supply as one row unless its positions are read.
    /// Reading them needs the owner-only decoder; the lock family's decoder
    /// must not be what answers here, because it would demand a date the datum
    /// does not carry.
    #[test]
    fn an_owner_only_staking_position_is_attributed_to_its_owner() {
        let mut o = position(100, 3, TOOL_POSITION, 42_568);
        o.address = TOOL_SITE.into();
        let balances = HashMap::from([(TOOL_SITE, 42_568)]);
        let by_payment = HashMap::from([(TOOL_OWNER_PKH.to_string(), "owner".to_string())]);
        let owners = vesting(
            &[o],
            &by_payment,
            &HashMap::new(),
            &balances,
            &mitos_registry::Registry::pinned(),
        );

        assert_eq!(owners.len(), 1);
        let only = &owners[0];
        assert_eq!(only.key, "owner", "the datum's owner is the party");
        assert_eq!(only.amount, 42_568);
        assert_eq!(only.locks[0].style, VestStyle::Other);
        assert_eq!(
            only.locks[0].unlock_ts_ms, 0,
            "the datum carries no date, and none is invented"
        );
    }

    /// A site reads the shape it declares and nothing else: the two decoders
    /// must not stand in for each other.
    #[test]
    fn a_site_reads_only_the_datum_shape_it_declares() {
        let timed = hex::decode(POSITION_A).unwrap();
        let owner_only = hex::decode(TOOL_POSITION).unwrap();
        assert!(position_datum(DatumShape::Timed, &owner_only).is_none());
        assert!(position_datum(DatumShape::OwnerOnly, &timed).is_none());
        assert_eq!(
            position_datum(DatumShape::OwnerOnly, &owner_only),
            Some((TOOL_OWNER_PKH.to_string(), 0))
        );
    }

    fn spend_of(slot: u64, position: &Observation) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: position.tx_hash.clone(),
            address: String::new(),
            lovelace: 0,
            unit_name: Vec::new(),
            unit_amount: 0,
            datum: None,
            decoded: None,
            output_index: position.output_index,
            spent: true,
        }
    }

    /// Before spends were recorded, a claimed lock stayed in the archive and
    /// the site's live balance was scaled across every position ever seen —
    /// an active staker credited less than they hold, and a former one
    /// credited something. With the spend recorded and joined, the claimed
    /// position is gone and the survivor keeps its whole amount.
    #[test]
    fn a_claimed_lock_drops_out_and_the_survivor_is_not_scaled() {
        let a = position(100, 1, POSITION_A, 1_035_179);
        let b = position(110, 2, POSITION_B, 328_938);
        // B was claimed: the site now holds only A's amount.
        let rows = policy_archive::live(vec![spend_of(200, &b), a.clone(), b]);
        let balances = HashMap::from([(SITE, 1_035_179)]);
        let owners = vesting(
            &rows,
            &HashMap::new(),
            &HashMap::new(),
            &balances,
            &mitos_registry::Registry::pinned(),
        );

        assert_eq!(owners.len(), 1, "the claimed position names no owner");
        let only = &owners[0];
        assert_eq!(only.amount, 1_035_179, "the whole position, unscaled");
        assert_eq!(only.locks.len(), 1);
        assert_eq!(only.locks[0].style, VestStyle::Other);
    }

    /// The same two positions with no spend recorded: the old reading, kept so
    /// the scale-down is known to still cover an archive written before the
    /// column existed. Both owners resolve, and the site's balance is split
    /// across both positions — the claimed one credited something it no longer
    /// holds, the survivor credited less than it does.
    #[test]
    fn without_a_recorded_spend_the_balance_is_still_scaled_across_both() {
        let a = position(100, 1, POSITION_A, 1_035_179);
        let b = position(110, 2, POSITION_B, 328_938);
        let balances = HashMap::from([(SITE, 1_035_179)]);
        // The owners' payment keys, as the movement rows would have resolved them.
        let by_payment = HashMap::from([
            (
                "cce969f5c0a16d5735feb093b883e4656f0dd4eb377dbc5d787391bf".to_string(),
                "owner-a".to_string(),
            ),
            (
                "8974b7a444f372d07f15abcc0ecd9729e79f9e196b72a0bb3fc5410c".to_string(),
                "owner-b".to_string(),
            ),
        ]);
        let owners = vesting(
            &[a, b],
            &by_payment,
            &HashMap::new(),
            &balances,
            &mitos_registry::Registry::pinned(),
        );
        assert_eq!(owners.len(), 2);
        let total: u64 = owners.iter().map(|o| o.amount).sum();
        assert_eq!(total, 1_035_179, "scaled to the balance, sum preserved");
        assert!(owners.iter().all(|o| o.amount < 1_035_179));
    }

    #[test]
    fn observed_locks_scale_down_to_the_current_balance_and_keep_their_sum() {
        assert_eq!(scale_to(50, 100, vec![60, 40]), vec![30, 20]);
        assert_eq!(scale_to(30, 100, vec![1, 1, 1]), vec![10, 10, 10]);
        assert_eq!(scale_to(150, 100, vec![60, 40]), vec![60, 40]);
        assert_eq!(scale_to(0, 100, vec![60, 40]), vec![0, 0]);
    }

    #[test]
    fn lp_shares_are_proportional_and_the_remainder_is_the_residual() {
        let provider = |lp_tokens: u64| Provider {
            lp_tokens,
            address: String::new(),
        };
        let providers = BTreeMap::from([
            ("a".to_string(), provider(250)),
            ("b".to_string(), provider(250)),
        ]);
        let (out, residual) = lp_amounts(1_000, 1_000, providers);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].amount, 250);
        assert_eq!(out[1].amount, 250);
        assert_eq!(residual, 500);
    }

    #[test]
    fn an_empty_provider_set_leaves_the_whole_pool_as_residual() {
        let (out, residual) = lp_amounts(1_000, 1_000, BTreeMap::new());
        assert!(out.is_empty());
        assert_eq!(residual, 1_000);
    }

    /// $PERP's CSwap pool and its LP token, as the pool datum names them.
    const PERP_POLICY: &str = "d9d59e71eb99e653a493956a6826a406060706bd8c9a2427b3564f64";
    const LP_POLICY: &str = "f1dfb5314d99902d370e059a4851095646bfe8db57fb0d72a2d106fc";

    /// A CSwap pool datum (`Constr 0`, eight fields) for the ADA × $PERP pool.
    fn perp_pool_datum(total_lp: u32) -> Vec<u8> {
        let perp_name = hex::encode("PERP COIN");
        let lp_name = hex::encode("C-LP: ADA x PERP COIN");
        // Constr 0, indefinite list: total LP (u32), fee 85 bps, quote = ADA
        // (two empty byte strings), base policy and name, LP policy and name.
        let fields = [
            format!("1a{total_lp:08x}"),
            "1855".to_string(),
            "40".to_string(),
            "40".to_string(),
            format!("581c{PERP_POLICY}"),
            format!("49{perp_name}"),
            format!("581c{LP_POLICY}"),
            format!("55{lp_name}"),
        ];
        hex::decode(format!("d8799f{}ff", fields.concat())).unwrap()
    }

    fn pool_sighting(slot: u64, total_lp: u32) -> Observation {
        Observation {
            slot,
            block_time: 1_700_000_000 + slot,
            tx_hash: vec![slot as u8; 32],
            address: cswap::POOL_SCRIPT_ADDR.into(),
            lovelace: 1_000_000,
            unit_name: b"PERP COIN".to_vec(),
            unit_amount: 61_177_936,
            datum: Some(perp_pool_datum(total_lp)),
            decoded: None,
            output_index: Some(0),
            spent: false,
        }
    }

    /// MEASURED 2026-11-11: $PERP's pool held 61.2M units attributed to nobody
    /// because its LP token had never been walked. The fold now names it, with
    /// a floor a day below the pool's FIRST sighting — never its latest, which
    /// would cut off every LP movement before it.
    #[test]
    fn an_unwalked_lp_policy_is_asked_for_with_a_floor_below_the_first_sighting() {
        let root = tempfile::tempdir().unwrap();
        let dir = policy_dir(root.path(), PERP_POLICY);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(cswap::decode_pool_datum(&perp_pool_datum(1)).is_some());
        let rows = vec![
            pool_sighting(172_600_000, 838_040_878),
            pool_sighting(172_500_000, 1_000),
        ];
        assert_eq!(
            lp_wanted(&dir, &rows),
            Some(LpWanted {
                policy: LP_POLICY.into(),
                floor: 172_500_000 - LP_FLOOR_MARGIN_SLOTS,
            })
        );
        assert_eq!(lp_wanted(&dir, &[]), None, "no pool, nothing to walk");
    }
}
