//! Cross-cutting helpers used by the dispatcher's residual pass.
//!
//! Currently just the `same_wallet` rule (see
//! `docs/design/DOMAIN_REFACTOR.md` "Same-wallet suppression"
//! section). Lives in `mitos-core` rather than `mitos-protocol`
//! because the rule is a framework-internal concern: it's applied
//! once at residual-pass emission time, never on the consumer side.

use pallas_addresses::{Address, ShelleyDelegationPart};

/// Compare two bech32 addresses for "same wallet" semantics, used
/// by the residual pass to suppress `AssetMovement` events that
/// represent UTxO change going back to the same wallet (HD-wallet
/// derivation typically lands change at a different payment
/// address but with the same stake credential as the source).
///
/// Rule (per `docs/design/DOMAIN_REFACTOR.md`):
///
/// - **Both Shelley addresses with key-delegation stake creds** →
///   compare stake key hashes.
/// - **Both Shelley with script-delegation stake creds** →
///   compare stake script hashes.
/// - **Mixed key/script, or any non-Shelley** (Byron, enterprise,
///   script-only, parse failure) → fall back to bech32 string
///   equality.
///
/// Conservative for edge cases: when in doubt, treat as different
/// wallets. Under-suppression (emitting a movement for what's
/// actually self-transfer) is cosmetically noisy but correct;
/// over-suppression (silently dropping a real transfer) would
/// silently lose data.
pub fn same_wallet(addr_a: &str, addr_b: &str) -> bool {
    let parsed_a = Address::from_bech32(addr_a);
    let parsed_b = Address::from_bech32(addr_b);

    match (parsed_a, parsed_b) {
        (Ok(Address::Shelley(s_a)), Ok(Address::Shelley(s_b))) => {
            match (s_a.delegation(), s_b.delegation()) {
                (ShelleyDelegationPart::Key(key_a), ShelleyDelegationPart::Key(key_b)) => {
                    key_a.as_ref() == key_b.as_ref()
                }
                (ShelleyDelegationPart::Script(scr_a), ShelleyDelegationPart::Script(scr_b)) => {
                    scr_a.as_ref() == scr_b.as_ref()
                }
                // Mixed key/script delegation, or `Null` (enterprise
                // half) on either side — fall back to bech32
                // equality. Enterprise + base addresses for the
                // same payment cred are *not* treated as same-
                // wallet here because there's no stake-cred to
                // compare; the rule errs on the conservative side.
                _ => addr_a == addr_b,
            }
        }
        // Either side is non-Shelley (Byron, parse failure, etc.)
        // — exact bech32 equality is the only safe answer.
        _ => addr_a == addr_b,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pallas_addresses::{Network, ShelleyAddress};
    use pallas_addresses::{ShelleyDelegationPart as Deleg, ShelleyPaymentPart as Pay};

    /// Build a mainnet base address (bech32) from explicit
    /// payment + stake credentials. Used to construct test
    /// fixtures with known stake-cred relationships.
    fn make_addr(pay: [u8; 28], stake: [u8; 28]) -> String {
        let payment = Pay::key_hash(pay.into());
        let delegation = Deleg::key_hash(stake.into());
        Address::Shelley(ShelleyAddress::new(Network::Mainnet, payment, delegation))
            .to_bech32()
            .unwrap()
    }

    #[test]
    fn same_stake_cred_counts_as_same_wallet() {
        // Two addresses, same stake cred, different payment creds
        // — the canonical "HD-wallet change" pattern.
        let stake = [0x42; 28];
        let a = make_addr([0x01; 28], stake);
        let b = make_addr([0x02; 28], stake);
        assert!(same_wallet(&a, &b));
    }

    #[test]
    fn different_stake_cred_counts_as_different_wallet() {
        let a = make_addr([0x01; 28], [0x42; 28]);
        let b = make_addr([0x02; 28], [0x99; 28]);
        assert!(!same_wallet(&a, &b));
    }

    #[test]
    fn identical_address_is_same_wallet() {
        let a = make_addr([0x01; 28], [0x42; 28]);
        assert!(same_wallet(&a, &a));
    }

    #[test]
    fn unparseable_addresses_fall_back_to_string_equality() {
        // Garbage strings can't be parsed as bech32 — fallback
        // path uses raw equality.
        assert!(same_wallet("not_an_address", "not_an_address"));
        assert!(!same_wallet("not_an_address", "different_garbage"));
    }
}
