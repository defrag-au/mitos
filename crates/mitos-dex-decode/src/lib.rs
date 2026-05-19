//! `mitos-dex-decode` — shared DEX datum + address decode
//! library for mitos community modules.
//!
//! The CSwap/Splash recognition modules and `holder-distribution`'s
//! LP-pool decomposition both need to read the same on-chain
//! shapes — pool datums, staking datums, the contracts' script
//! addresses. A wasm community module is a single `.rs` file;
//! the only way two modules share code is a crate. This is that
//! crate: pure decode logic, no WIT bindings, no host-fn
//! coupling, so a module pulls in nothing transitive beyond
//! pallas by depending on it.
//!
//! See `docs/design/HOLDER_DISTRIBUTION_LP_DECOMPOSITION.md` —
//! "the decode library is the genuine reuse unit; composition
//! over coordination."

pub mod cswap;

/// Redistribute one LP provider's share of a pool's reserve of
/// some asset: `lp_held / total_lp_supply` of `pool_reserve`.
///
/// `u128` intermediate so the multiply can't overflow before the
/// divide. Returns 0 for a zero (or unknown) total supply rather
/// than dividing by zero.
pub fn lp_share(lp_held: u64, pool_reserve: u64, total_lp_supply: u64) -> u64 {
    if total_lp_supply == 0 {
        return 0;
    }
    let share = (lp_held as u128 * pool_reserve as u128) / total_lp_supply as u128;
    // A real share is bounded by the pool reserve, so this can't
    // truncate for well-formed inputs; saturate defensively.
    u64::try_from(share).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lp_share_proportional() {
        // Holding 25% of supply earns 25% of the reserve.
        assert_eq!(lp_share(250, 4_000, 1_000), 1_000);
        // The whole supply earns the whole reserve.
        assert_eq!(lp_share(1_000, 4_000, 1_000), 4_000);
    }

    #[test]
    fn lp_share_zero_supply_is_zero() {
        assert_eq!(lp_share(100, 4_000, 0), 0);
    }

    #[test]
    fn lp_share_no_overflow_on_large_inputs() {
        // Large reserve × large holding would overflow u64 before
        // the divide; the u128 intermediate keeps it exact.
        assert_eq!(
            lp_share(u64::MAX, u64::MAX, u64::MAX),
            u64::MAX,
        );
    }
}
