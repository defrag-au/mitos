//! Shared address classification + originator discovery for
//! DEX swap events.
//!
//! `find_originator` is the fallback used by `cswap-dex` and
//! `splash-dex` when their primary `find_swapper(produced)`
//! returns nothing — typically because the swap's
//! immediate token recipient is a script-payment address (an
//! aggregator / order-routing wrapper). The shape is generic
//! over an iterator of `(address, lovelace)` so callers can pass
//! a slice projection of `ConsumedEvent` (or any other
//! input-list shape) without dragging `mitos-data-plane` types
//! into this crate.

/// Mainnet bech32 prefixes for key-payment Shelley addresses:
/// - `addr1q…` — key payment + key stake (most common user wallet)
/// - `addr1u…` — key payment + pointer stake
/// - `addr1v…` — key payment + no stake (enterprise)
///
/// Excluded by design: `addr1w/x/y/z…` (script payment). These
/// are smart-contract destinations (pools, aggregator escrows,
/// vesting contracts) that don't represent a user identity for
/// trade-feed attribution.
const MAINNET_USER_PREFIXES: &[&str] = &["addr1q", "addr1u", "addr1v"];
const TESTNET_USER_PREFIXES: &[&str] = &["addr_test1q", "addr_test1u", "addr_test1v"];

/// `true` when `addr` is a key-payment Shelley address on either
/// mainnet or testnet. Script-payment addresses and Byron
/// addresses return `false`.
pub fn is_user_wallet(addr: &str) -> bool {
    MAINNET_USER_PREFIXES.iter().any(|p| addr.starts_with(p))
        || TESTNET_USER_PREFIXES.iter().any(|p| addr.starts_with(p))
}

/// Pick the dominant key-wallet input from a transaction's
/// consumed-output list, ranked by lovelace. Used to surface
/// "who funded this swap" when the immediate output recipient
/// is a script — aggregator wrappers spend the user's wallet
/// UTxOs into a wrapped order, so the user's address appears
/// on inputs even when it's absent from the swap outputs.
///
/// Returns `None` when no key-wallet input is present (e.g.
/// contract-to-contract flows). Ties broken in iteration order
/// (`max_by_key` keeps the *last* maximum), matching natural
/// "later overrides earlier" intuition.
pub fn find_originator<'a, I>(inputs: I) -> Option<&'a str>
where
    I: IntoIterator<Item = (&'a str, u64)>,
{
    inputs
        .into_iter()
        .filter(|(addr, _)| is_user_wallet(addr))
        .max_by_key(|(_, lovelace)| *lovelace)
        .map(|(addr, _)| addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Sample addresses used across the tests. `q` = key+key,
    // `u` = key+pointer, `v` = key+enterprise, `x/z` = script.
    const USER_Q1: &str = "addr1q9xp27xnlm837dz4r6fk63qyrlqt573mqeqm6v2n928565p9n897pq0lovelace";
    const USER_Q2: &str =
        "addr1qxvtxteraurl5n3d9eae5yyzgpx7zutzqsmx9tyhsv76wsvasazx8r5xwqtnfjsfrnat3h6yryc";
    const USER_U: &str = "addr1u8njek8z5jt2cpaupkcpyl0pl85v3k76ejgwtjcx7szlzwc87e3jh";
    const USER_V: &str = "addr1vy9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q";
    const SCRIPT_X: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrx";
    const SCRIPT_Z: &str = "addr1z8d9k3aw6w24eyfjacy809h68dv2rwnpw0arrfau98jk6nhv88awp8s";
    const TESTNET_Q: &str = "addr_test1qsomewhere";
    const BYRON: &str = "Ae2tdPwUPEZ4YjgvykNpoFeYUxoyhNj2kg8KfKWN2FizsSpLUPv68MpTVDo";

    #[test]
    fn user_wallet_recognises_key_payment_prefixes() {
        assert!(is_user_wallet(USER_Q1));
        assert!(is_user_wallet(USER_Q2));
        assert!(is_user_wallet(USER_U));
        assert!(is_user_wallet(USER_V));
        assert!(is_user_wallet(TESTNET_Q));
    }

    #[test]
    fn user_wallet_rejects_script_and_byron() {
        assert!(!is_user_wallet(SCRIPT_X));
        assert!(!is_user_wallet(SCRIPT_Z));
        assert!(!is_user_wallet(BYRON));
        assert!(!is_user_wallet(""));
    }

    #[test]
    fn find_originator_empty_inputs_returns_none() {
        let inputs: Vec<(&str, u64)> = vec![];
        assert_eq!(find_originator(inputs), None);
    }

    #[test]
    fn find_originator_all_script_inputs_returns_none() {
        // The exact scenario for our slice-12 motivating TX
        // (`78f10cfc…`) before considering the user's wallet —
        // a pool input + an order escrow input, both scripts.
        // Without a key-wallet input, we have no originator to
        // surface; consumers fall back to whatever
        // `swapper_address` carries.
        let inputs = vec![(SCRIPT_X, 15_130_035_312), (SCRIPT_Z, 2_690_000)];
        assert_eq!(find_originator(inputs), None);
    }

    #[test]
    fn find_originator_single_user_input_returns_it() {
        let inputs = vec![(USER_Q1, 138_008_395)];
        assert_eq!(find_originator(inputs), Some(USER_Q1));
    }

    #[test]
    fn find_originator_picks_dominant_user_input_by_lovelace() {
        // The classic 78f10cfc shape — same user wallet appears
        // in two inputs, plus a pool script input. Originator
        // should be the user wallet (largest of the two, or
        // either since they're identical).
        let inputs = vec![
            (USER_Q1, 138_008_395),
            (SCRIPT_X, 15_130_035_312),
            (USER_Q1, 142_451_763),
        ];
        // The second USER_Q1 entry has more lovelace.
        assert_eq!(find_originator(inputs), Some(USER_Q1));
    }

    #[test]
    fn find_originator_prefers_user_even_if_script_richer() {
        // Pool inputs are ~thousands of ADA; the user's input
        // is small in comparison. Make sure the script's huge
        // lovelace doesn't promote it.
        let inputs = vec![(SCRIPT_X, 15_000_000_000_000), (USER_Q1, 100_000_000)];
        assert_eq!(find_originator(inputs), Some(USER_Q1));
    }

    #[test]
    fn find_originator_picks_largest_among_multiple_distinct_users() {
        // A multi-funder swap (rare but possible). Should pick
        // the user wallet that contributed the most ADA, since
        // that's the strongest signal of "who initiated this".
        let inputs = vec![
            (USER_Q1, 50_000_000),
            (USER_Q2, 250_000_000),
            (USER_U, 100_000_000),
        ];
        assert_eq!(find_originator(inputs), Some(USER_Q2));
    }

    #[test]
    fn find_originator_handles_testnet_addresses() {
        let inputs = vec![(SCRIPT_X, 10_000_000_000), (TESTNET_Q, 5_000_000)];
        assert_eq!(find_originator(inputs), Some(TESTNET_Q));
    }

    #[test]
    fn find_originator_ignores_byron_inputs() {
        // Byron wallets exist on chain and are valid TX inputs,
        // but we don't classify them as "user wallets" for
        // attribution since CSwap/Splash don't accept them as
        // swap origins anyway. Treat them like scripts: ignore.
        let inputs = vec![(BYRON, 500_000_000), (USER_V, 10_000_000)];
        assert_eq!(find_originator(inputs), Some(USER_V));
    }
}
