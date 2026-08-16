//! Cardano address → [`Party`] + the credentials the walker matches on.
//!
//! The party is the stake address when there is one, else the payment address
//! itself — and the absence of a stake key is *signal* (off-ramp shape; terminal
//! by rule), carried on `Party::has_stake_credential`. Pointer-delegated
//! addresses are treated as stakeless: the pointer is not a reusable identity.
//! Byron addresses are their own party key with no stake credential.
//!
//! [`Resolved::stake_cred`] is what the activity counter keys on (28 raw bytes,
//! script or key), and [`Resolved::payment_cred`] is what the policy-signer seed
//! matches (any address whose payment key hash equals the `sig` credential).

use chain_ledger::Party;
use pallas_addresses::{Address, ShelleyDelegationPart, StakeAddress};

/// One output address, resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub party: Party,
    /// Raw 28-byte staking credential (key or script) when present.
    pub stake_cred: Option<[u8; 28]>,
    /// Raw 28-byte payment credential (key or script) for Shelley addresses.
    pub payment_cred: Option<[u8; 28]>,
    /// The payment part is a script (marketplace contracts look like this).
    pub payment_is_script: bool,
}

/// Resolve a decoded pallas address. Never fails: anything unrecognisable
/// becomes a stakeless party keyed by `fallback` (the bech32/base58 string the
/// walker already has), so a weird address is recorded, not dropped.
pub fn resolve(addr: &Address, fallback: &str) -> Resolved {
    match addr {
        Address::Shelley(sh) => {
            let payment_cred = Some(hash28(sh.payment().as_hash()));
            let payment_is_script = sh.payment().is_script();
            match sh.delegation() {
                ShelleyDelegationPart::Key(_) | ShelleyDelegationPart::Script(_) => {
                    let stake_cred = sh.delegation().as_hash().map(hash28);
                    let key = StakeAddress::try_from(sh.clone())
                        .ok()
                        .and_then(|s| s.to_bech32().ok())
                        .unwrap_or_else(|| fallback.to_owned());
                    Resolved {
                        party: Party::cardano_stake(key),
                        stake_cred,
                        payment_cred,
                        payment_is_script,
                    }
                }
                ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => Resolved {
                    party: Party::cardano_enterprise(
                        sh.to_bech32().unwrap_or_else(|_| fallback.to_owned()),
                    ),
                    stake_cred: None,
                    payment_cred,
                    payment_is_script,
                },
            }
        }
        Address::Stake(st) => {
            // A stake address as an output address can't happen on-chain; keep
            // it total anyway.
            let stake_cred = Some(hash28_slice(st.payload().as_ref()));
            Resolved {
                party: Party::cardano_stake(st.to_bech32().unwrap_or_else(|_| fallback.to_owned())),
                stake_cred,
                payment_cred: None,
                payment_is_script: false,
            }
        }
        Address::Byron(b) => Resolved {
            party: Party::cardano_enterprise(b.to_base58()),
            stake_cred: None,
            payment_cred: None,
            payment_is_script: false,
        },
    }
}

/// Resolve from the bech32/base58 string form (what `DecodedOutput.address`
/// carries). Falls back to a stakeless party keyed by the string.
pub fn resolve_str(s: &str) -> Resolved {
    match Address::from_bech32(s) {
        Ok(a) => resolve(&a, s),
        Err(_) => Resolved {
            party: Party::cardano_enterprise(s),
            stake_cred: None,
            payment_cred: None,
            payment_is_script: false,
        },
    }
}

/// The party for a registry `stake1…` string.
pub fn stake_party(stake_bech32: &str) -> Party {
    Party::cardano_stake(stake_bech32)
}

fn hash28(h: &pallas_crypto::hash::Hash<28>) -> [u8; 28] {
    let mut out = [0u8; 28];
    out.copy_from_slice(h.as_ref());
    out
}

fn hash28_slice(b: &[u8]) -> [u8; 28] {
    let mut out = [0u8; 28];
    out.copy_from_slice(&b[..28]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mainnet base address (payment key + stake key).
    const BASE: &str = "addr1qx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3n0d3vllmyqwsx5wktcd8cc3sq835lu7drv2xwl2wywfgse35a3x";
    #[test]
    fn base_address_resolves_to_stake_party() {
        let r = resolve_str(BASE);
        assert!(r.party.has_stake_credential);
        assert!(r.party.key.starts_with("stake1"));
        assert!(r.stake_cred.is_some());
        assert!(r.payment_cred.is_some());
        assert!(!r.payment_is_script);
    }

    #[test]
    fn enterprise_address_is_stakeless_party() {
        // Build the enterprise form of BASE (same payment part, no delegation).
        let Address::Shelley(base) = Address::from_bech32(BASE).unwrap() else {
            panic!("base is shelley");
        };
        let ent = pallas_addresses::ShelleyAddress::new(
            base.network(),
            base.payment().clone(),
            ShelleyDelegationPart::Null,
        );
        let ent_str = ent.to_bech32().unwrap();
        assert!(ent_str.starts_with("addr1v"));
        let r = resolve_str(&ent_str);
        assert!(!r.party.has_stake_credential);
        assert_eq!(r.party.key, ent_str);
        assert!(r.stake_cred.is_none());
        assert!(r.payment_cred.is_some());
        // Same payment key as the base address.
        assert_eq!(r.payment_cred, resolve_str(BASE).payment_cred);
    }

    #[test]
    fn garbage_is_recorded_not_dropped() {
        let r = resolve_str("not-an-address");
        assert!(!r.party.has_stake_credential);
        assert_eq!(r.party.key, "not-an-address");
    }
}
