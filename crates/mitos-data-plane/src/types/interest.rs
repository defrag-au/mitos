//! `InterestPredicate` + `InterestSet` — declarative event filter.
//!
//! The platform's v2 dispatch path filters events against a
//! module's declared interest before any wasm call. Modules see
//! only events from TXs with at least one matching event,
//! eliminating the per-module `watched_address(addr)` checks
//! every v1 module re-implemented.
//!
//! The Rust types here mirror the v2 WIT's `interest-predicate`
//! variant. WIT bindings convert to/from these via simple
//! match arms (host-side codegen).
//!
//! Predicates are pure: matching against a `TypedOutput` is
//! address parsing + asset-multiset lookup, no I/O. The
//! platform applies the set once per output / mint / tick to
//! decide whether to emit the corresponding event.

use cardano_assets::PolicyId;
use serde::{Deserialize, Serialize};

use crate::types::TypedOutput;
use crate::types::output::AssetEntry;

/// Stake credential — the payment-credential-independent
/// identity of an address's "owner". Used by `at-stake-cred`.
///
/// Matches the WIT `stake-cred` variant exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StakeCred {
    /// 28-byte stake key hash (`stake1u...` / `stake1y...`
    /// addresses).
    KeyHash([u8; 28]),
    /// 28-byte stake script hash (delegation contracts).
    ScriptHash([u8; 28]),
}

/// One predicate the module wants to watch. Matches the WIT
/// `interest-predicate` variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InterestPredicate {
    /// Bech32 payment address; matches outputs whose address
    /// is exactly this string.
    AtAddress(String),
    /// 28-byte payment credential (payment-key or payment-script
    /// hash); matches outputs whose address shares this payment
    /// credential, regardless of staking part.
    ///
    /// Use case: contract sweeps where the staking part varies
    /// per UTxO (e.g. CrowdLock vests — same script payment
    /// cred, different derived staking creds per lock).
    AtPaymentCred([u8; 28]),
    /// 28-byte stake credential; matches outputs whose address
    /// shares this stake credential, regardless of payment
    /// credential.
    AtStakeCred(StakeCred),
    /// 28-byte policy id; matches outputs whose asset multiset
    /// contains any asset under this policy. Also matches
    /// `MintedEvent`s under this policy.
    HoldsPolicy(PolicyId),
    /// Specific asset; matches outputs holding this exact
    /// `(policy, asset_name)` and `MintedEvent`s for it.
    HoldsAsset {
        policy: PolicyId,
        asset_name: Vec<u8>,
    },
    /// Periodic wake-up. Module receives a `TickEvent` every
    /// `seconds` (wall-clock).
    TickEvery(u32),
}

/// The set of predicates a module is currently watching.
/// Constructed from `update-interest` calls; persisted via
/// `state-kv` so a host restart without an attached companion
/// keeps filtering correctly.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InterestSet {
    pub predicates: Vec<InterestPredicate>,
}

impl InterestSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_predicate(mut self, p: InterestPredicate) -> Self {
        self.predicates.push(p);
        self
    }

    pub fn add(&mut self, p: InterestPredicate) {
        if !self.predicates.contains(&p) {
            self.predicates.push(p);
        }
    }

    pub fn remove(&mut self, p: &InterestPredicate) {
        self.predicates.retain(|q| q != p);
    }

    pub fn replace(&mut self, predicates: Vec<InterestPredicate>) {
        self.predicates = predicates;
    }

    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &InterestPredicate> {
        self.predicates.iter()
    }

    /// Does any predicate in the set match this output?
    /// Used for `produced` / `consumed` / `referenced` events.
    ///
    /// Unresolved outputs (dispatcher placeholders for inputs
    /// whose prior output the data plane couldn't fetch) never
    /// match. Address-keyed predicates would otherwise spuriously
    /// match an empty `AtAddress("")` and policy/asset-keyed
    /// predicates already see an empty asset multiset — the
    /// short-circuit keeps both paths from accidentally
    /// dispatching phantom events. Modules that want to react
    /// to redeemer-driven Cancel-style consumes regardless of
    /// prior-output resolution will get an explicit opt-in
    /// predicate variant (drop site #1 follow-up in
    /// `docs/design/EVENT_DELIVERY_RESILIENCE.md`).
    pub fn matches_output(&self, output: &TypedOutput) -> bool {
        if output.is_unresolved() {
            return false;
        }
        self.predicates
            .iter()
            .any(|p| predicate_matches_output(p, output))
    }

    /// Does any predicate in the set match this `(policy,
    /// asset_name)` mint? Used for `MintedEvent` filtering.
    /// Only `HoldsPolicy` / `HoldsAsset` predicates apply —
    /// mints have no address.
    pub fn matches_mint(&self, policy: &PolicyId, asset_name: &[u8]) -> bool {
        self.predicates.iter().any(|p| match p {
            InterestPredicate::HoldsPolicy(pp) => pp == policy,
            InterestPredicate::HoldsAsset {
                policy: pp,
                asset_name: an,
            } => pp == policy && an.as_slice() == asset_name,
            _ => false,
        })
    }

    /// Iterate all `TickEvery(seconds)` registrations. The
    /// platform's tick scheduler reads these to know when to
    /// fire ticks.
    pub fn tick_intervals(&self) -> impl Iterator<Item = u32> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            InterestPredicate::TickEvery(s) => Some(*s),
            _ => None,
        })
    }

    /// Iterate all `AtAddress(bech32)` predicates. The
    /// platform's bootstrap orchestrator reads these to know
    /// which addresses to scan for current-state hydration.
    pub fn watched_addresses(&self) -> impl Iterator<Item = &str> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            InterestPredicate::AtAddress(a) => Some(a.as_str()),
            _ => None,
        })
    }

    /// Iterate all `AtPaymentCred(cred)` predicates. The
    /// bootstrap orchestrator reads these to scan the current
    /// UTxO set under each payment credential via the
    /// by-payment-cred index — payment-cred-side analogue of
    /// `watched_addresses`.
    pub fn watched_payment_creds(&self) -> impl Iterator<Item = &[u8; 28]> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            InterestPredicate::AtPaymentCred(c) => Some(c),
            _ => None,
        })
    }

    /// Iterate all `HoldsPolicy(policy)` predicates. The
    /// bootstrap orchestrator reads these to scan the current
    /// UTxO set under each policy via `search_utxos` and
    /// synthesise events for every existing holder — the
    /// policy-side analogue of `watched_addresses`.
    pub fn watched_policies(&self) -> impl Iterator<Item = &PolicyId> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            InterestPredicate::HoldsPolicy(p) => Some(p),
            _ => None,
        })
    }
}

fn predicate_matches_output(p: &InterestPredicate, output: &TypedOutput) -> bool {
    match p {
        InterestPredicate::AtAddress(addr) => output.address == *addr,
        InterestPredicate::AtPaymentCred(cred) => payment_cred_matches(cred, &output.address),
        InterestPredicate::AtStakeCred(cred) => stake_cred_matches(cred, &output.address),
        InterestPredicate::HoldsPolicy(policy) => output
            .assets
            .iter()
            .any(|e: &AssetEntry| e.policy_id == *policy),
        InterestPredicate::HoldsAsset { policy, asset_name } => {
            let needle_hex = hex::encode(asset_name);
            output
                .assets
                .iter()
                .any(|e: &AssetEntry| e.policy_id == *policy && e.asset_name_hex == needle_hex)
        }
        // Tick predicates don't apply to outputs.
        InterestPredicate::TickEvery(_) => false,
    }
}

/// Decode the bech32 address and check whether its payment
/// credential (28-byte payment-key or payment-script hash)
/// matches `cred`. Falls back to `false` on parse errors —
/// better to under-match than to over-match a malformed
/// address.
fn payment_cred_matches(cred: &[u8; 28], address_bech32: &str) -> bool {
    let Ok(addr) = pallas_addresses::Address::from_bech32(address_bech32) else {
        return false;
    };
    let pallas_addresses::Address::Shelley(s) = addr else {
        return false;
    };
    use pallas_addresses::ShelleyPaymentPart;
    match s.payment() {
        ShelleyPaymentPart::Key(h) => h.as_ref() == cred,
        ShelleyPaymentPart::Script(h) => h.as_ref() == cred,
    }
}

/// Decode the bech32 address and check whether its stake
/// credential matches `cred`. Falls back to `false` on parse
/// errors — better to under-match than to over-match a
/// malformed address.
fn stake_cred_matches(cred: &StakeCred, address_bech32: &str) -> bool {
    let Ok(addr) = pallas_addresses::Address::from_bech32(address_bech32) else {
        return false;
    };
    let pallas_addresses::Address::Shelley(s) = addr else {
        return false;
    };
    let stake = s.delegation();
    use pallas_addresses::ShelleyDelegationPart;
    match (cred, stake) {
        (StakeCred::KeyHash(want), ShelleyDelegationPart::Key(got)) => got.as_ref() == want,
        (StakeCred::ScriptHash(want), ShelleyDelegationPart::Script(got)) => got.as_ref() == want,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DecodeLevel;

    fn output(address: &str, assets: Vec<AssetEntry>) -> TypedOutput {
        TypedOutput {
            address: address.to_owned(),
            lovelace: 1_000_000,
            assets,
            datum: None,
            script_ref: None,
            original_cbor: None,
            decoded_at: DecodeLevel::Lean,
            resolution: crate::types::Resolution::Resolved,
        }
    }

    fn policy(b: u8) -> PolicyId {
        // PolicyId is hex-typed; construct from a 56-char hex
        // string of the same byte. `new_unchecked` skips length
        // validation in tests where we know the input is good.
        let hex = vec![format!("{b:02x}"); 28].concat();
        PolicyId::new_unchecked(hex)
    }

    fn asset(p: u8, name: &str, qty: u64) -> AssetEntry {
        AssetEntry {
            policy_id: policy(p),
            asset_name_hex: hex::encode(name.as_bytes()),
            quantity: qty,
        }
    }

    #[test]
    fn at_address_matches_exact() {
        let mut set = InterestSet::new();
        set.add(InterestPredicate::AtAddress("addr1xyz".to_owned()));

        assert!(set.matches_output(&output("addr1xyz", vec![])));
        assert!(!set.matches_output(&output("addr1abc", vec![])));
    }

    #[test]
    fn holds_policy_matches_any_asset_under_policy() {
        let mut set = InterestSet::new();
        set.add(InterestPredicate::HoldsPolicy(policy(0xaa)));

        let with_match = output(
            "addr1",
            vec![asset(0xaa, "TokenA", 1), asset(0xbb, "Other", 1)],
        );
        let without_match = output("addr1", vec![asset(0xbb, "Other", 1)]);

        assert!(set.matches_output(&with_match));
        assert!(!set.matches_output(&without_match));
    }

    #[test]
    fn holds_asset_matches_exact_pair() {
        let mut set = InterestSet::new();
        set.add(InterestPredicate::HoldsAsset {
            policy: policy(0xaa),
            asset_name: b"TokenA".to_vec(),
        });

        let exact = output("addr1", vec![asset(0xaa, "TokenA", 1)]);
        let wrong_name = output("addr1", vec![asset(0xaa, "TokenB", 1)]);
        let wrong_policy = output("addr1", vec![asset(0xbb, "TokenA", 1)]);

        assert!(set.matches_output(&exact));
        assert!(!set.matches_output(&wrong_name));
        assert!(!set.matches_output(&wrong_policy));
    }

    #[test]
    fn matches_mint_uses_policy_predicates() {
        let mut set = InterestSet::new();
        set.add(InterestPredicate::HoldsPolicy(policy(0xaa)));

        assert!(set.matches_mint(&policy(0xaa), b"AnyName"));
        assert!(!set.matches_mint(&policy(0xbb), b"AnyName"));
    }

    #[test]
    fn matches_mint_uses_holds_asset_predicates() {
        let mut set = InterestSet::new();
        set.add(InterestPredicate::HoldsAsset {
            policy: policy(0xaa),
            asset_name: b"Specific".to_vec(),
        });

        assert!(set.matches_mint(&policy(0xaa), b"Specific"));
        assert!(!set.matches_mint(&policy(0xaa), b"Other"));
    }

    #[test]
    fn watched_addresses_iterates_only_at_address() {
        let set = InterestSet::default()
            .with_predicate(InterestPredicate::AtAddress("addr1xyz".to_owned()))
            .with_predicate(InterestPredicate::HoldsPolicy(policy(0xaa)))
            .with_predicate(InterestPredicate::AtAddress("addr1abc".to_owned()));

        let addrs: Vec<&str> = set.watched_addresses().collect();
        assert_eq!(addrs, vec!["addr1xyz", "addr1abc"]);
    }

    #[test]
    fn unresolved_output_never_matches() {
        // A dispatcher placeholder must not match any predicate
        // shape, even pathological ones like `AtAddress("")`
        // which would otherwise spuriously match the empty
        // address of the placeholder.
        let unresolved = TypedOutput::unresolved();
        let pathological = InterestSet::default()
            .with_predicate(InterestPredicate::AtAddress(String::new()))
            .with_predicate(InterestPredicate::AtPaymentCred([0u8; 28]))
            .with_predicate(InterestPredicate::HoldsPolicy(policy(0xaa)));
        assert!(
            !pathological.matches_output(&unresolved),
            "unresolved output must short-circuit to false"
        );
    }

    #[test]
    fn add_is_idempotent() {
        let mut set = InterestSet::new();
        let p = InterestPredicate::AtAddress("addr1".to_owned());
        set.add(p.clone());
        set.add(p.clone());
        assert_eq!(set.predicates.len(), 1);
    }

    #[test]
    fn tick_intervals_iterates_only_tick_every() {
        let set = InterestSet::default()
            .with_predicate(InterestPredicate::TickEvery(60))
            .with_predicate(InterestPredicate::AtAddress("addr1".to_owned()))
            .with_predicate(InterestPredicate::TickEvery(3600));

        let intervals: Vec<u32> = set.tick_intervals().collect();
        assert_eq!(intervals, vec![60, 3600]);
    }
}
