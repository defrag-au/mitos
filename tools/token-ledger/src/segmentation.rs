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

use crate::archive::{History, holders_from, load_manifest, policy_dir, read_observations};

/// Build the tip decomposition for one policy.
///
/// `movements` must already be filtered to user units, and `as_of` is the
/// summary's `settled_to`.
pub fn build(
    dir: &Path,
    as_of: u64,
    observations: &[Observation],
    movements: &[policy_archive::Movement],
) -> Segmentation {
    let by_payment = payment_map(movements);
    let by_address = address_balances(movements);
    let vests = vesting(observations, &by_payment, &by_address);
    let lp = lp_decomposition(dir, observations, &by_address);
    Segmentation { as_of, vests, lp }
}

/// A lock observation, before its address's current balance is applied.
struct PendingVest {
    owner: String,
    amount: u64,
    unlock_ts_ms: u64,
    style: VestStyle,
}

fn payment_map(movements: &[policy_archive::Movement]) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for m in movements {
        let Some((payment, _)) = policy_archive::trade::address_parts(&m.address) else {
            continue;
        };
        out.entry(payment)
            .or_insert_with(|| holders::key_of(&m.address));
    }
    out
}

fn address_balances(movements: &[policy_archive::Movement]) -> HashMap<String, i64> {
    let mut out: HashMap<String, i64> = HashMap::new();
    for m in movements {
        *out.entry(m.address.clone()).or_default() += m.amount;
    }
    out
}

/// Every lock position the archive observed, attributed to its owner.
fn vesting(
    observations: &[Observation],
    by_payment: &HashMap<String, String>,
    by_address: &HashMap<String, i64>,
) -> Vec<VestOwner> {
    let mut pending: BTreeMap<String, Vec<PendingVest>> = BTreeMap::new();
    for o in observations {
        let Some(style) = vest_style(&o.address) else {
            continue;
        };
        let Some(datum) = o.datum.as_deref() else {
            continue;
        };
        let Some(decoded) = mitos_vesting_decode::decode_vesting_datum(datum) else {
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
                owner: owner_key(&o.address, &decoded.owner_pkh_hex, by_payment),
                amount,
                unlock_ts_ms: decoded.unlock_ts_ms,
                style,
            });
    }

    let mut owners: BTreeMap<String, VestOwner> = BTreeMap::new();
    for (address, items) in pending {
        let current = by_address.get(&address).copied().unwrap_or(0).max(0) as u64;
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
            let owner = owners
                .entry(item.owner.clone())
                .or_insert_with(|| VestOwner {
                    key: item.owner,
                    amount: 0,
                    locks: Vec::new(),
                });
            owner.amount = owner.amount.saturating_add(amount);
            owner.locks.push(VestLock {
                amount,
                unlock_ts_ms: item.unlock_ts_ms,
                lock_address: address.clone(),
                style: item.style,
            });
        }
    }
    owners.into_values().collect()
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

fn vest_style(address: &str) -> Option<VestStyle> {
    let cred = mitos_cohort::payment_cred(address)?;
    if mitos_vesting_decode::crowd_lock::is_crowd_lock(&cred) {
        Some(VestStyle::CrowdLock)
    } else if mitos_vesting_decode::snek_fun::is_snek_fun(&cred) {
        Some(VestStyle::SnekFun)
    } else if mitos_cohort::classify(address, &[], &[]).cohort == mitos_cohort::Cohort::Vesting {
        Some(VestStyle::Other)
    } else {
        None
    }
}

/// The owner behind a lock address.
///
/// A CrowdLock address delegates to the owner's stake credential, so its holder
/// key IS the owner. A lock address with no stake part (snek.fun, some Shield
/// positions) names its owner only in the datum; resolve that payment key hash
/// against a wallet address seen in the movement rows. If neither resolves, the
/// lock stays on its own key rather than being guessed onto a wallet.
fn owner_key(address: &str, owner_pkh_hex: &str, by_payment: &HashMap<String, String>) -> String {
    if matches!(
        policy_archive::trade::address_parts(address),
        Some((_, Some(_)))
    ) {
        return holders::key_of(address);
    }
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
    by_address: &HashMap<String, i64>,
) -> Option<LpDecomposition> {
    let (pool_address, datum) = latest_cswap_pool(observations)?;
    let reserve = by_address.get(&pool_address).copied().unwrap_or(0).max(0) as u64;
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

    let mut providers: BTreeMap<String, u64> = fold
        .holders
        .iter()
        .map(|h| (h.key.clone(), h.amount.max(0) as u64))
        .collect();

    let farm_key = holders::key_of(cswap::FARM_SCRIPT_ADDR);
    let farm_lp = providers.remove(&farm_key).unwrap_or(0);
    let mut partial = None;
    if farm_lp > 0 {
        let observations = read_observations(&lp_dir, &manifest).unwrap_or_default();
        let staked = staked_shares(&observations, &datum.lp_name);
        if staked.is_empty() {
            // The farm holds LP nobody's datum named. It stays a provider so
            // the reserve is still fully accounted for, but the report says so.
            providers.insert(farm_key, farm_lp);
            partial = Some("the farm's LP positions could not be named".to_string());
        } else {
            let observed: u64 = staked.values().sum();
            let keys: Vec<String> = staked.keys().cloned().collect();
            let amounts = scale_to(farm_lp, observed, staked.values().copied().collect());
            let mut allocated = 0u64;
            for (key, amount) in keys.into_iter().zip(amounts) {
                allocated = allocated.saturating_add(amount);
                *providers.entry(key).or_default() += amount;
            }
            let farm_residual = farm_lp.saturating_sub(allocated);
            if farm_residual > 0 {
                providers.insert(farm_key, farm_residual);
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

/// Turn LP token holdings into policy units, leaving the pool residual.
fn lp_amounts(
    reserve: u64,
    total_lp: u64,
    providers: BTreeMap<String, u64>,
) -> (Vec<LpProvider>, u64) {
    let mut out = Vec::new();
    let mut attributed = 0u64;
    for (key, lp_tokens) in providers {
        let amount = mitos_dex_decode::lp_share(lp_tokens, reserve, total_lp);
        attributed = attributed.saturating_add(amount);
        if amount > 0 {
            out.push(LpProvider {
                key,
                lp_tokens,
                amount,
            });
        }
    }
    (out, reserve.saturating_sub(attributed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_locks_scale_down_to_the_current_balance_and_keep_their_sum() {
        assert_eq!(scale_to(50, 100, vec![60, 40]), vec![30, 20]);
        assert_eq!(scale_to(30, 100, vec![1, 1, 1]), vec![10, 10, 10]);
        assert_eq!(scale_to(150, 100, vec![60, 40]), vec![60, 40]);
        assert_eq!(scale_to(0, 100, vec![60, 40]), vec![0, 0]);
    }

    #[test]
    fn lp_shares_are_proportional_and_the_remainder_is_the_residual() {
        let providers = BTreeMap::from([("a".to_string(), 250u64), ("b".to_string(), 250u64)]);
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
}
