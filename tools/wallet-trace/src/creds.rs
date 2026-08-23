//! Address → the two 28-byte credentials the index keys on.
//!
//! `blake2b224(vkey)` from a witness and the payment credential inside an
//! address are THE SAME 28 bytes (see [`crate::witness`]), so a co-signing group
//! and an address land in one key space with no bridging step. What addresses
//! add is the **stake credential** sitting beside the payment one — and that is
//! the pairing that turns a cluster of anonymous key hashes into a list of
//! `stake1…` wallets a human can actually open.
//!
//! Mirrors `project-ledger/src/party.rs`, but deliberately narrower: that module
//! resolves a *party* for value attribution and must be total over every weird
//! address on the chain. This one only answers "which two credentials, if any",
//! and returns `None` wherever the answer would be a guess.

use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart};

use crate::witness::KeyHash;

/// The credential pair an output reveals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredPair {
    pub payment: KeyHash,
    pub stake: KeyHash,
    /// The stake part is a script (a contract's own delegation), not a person.
    pub stake_is_script: bool,
}

/// The `(payment, stake)` pair for an address, when it has both.
///
/// `None` for enterprise/pointer/Byron addresses — no stake credential means
/// nothing to pair. That is not a loss: those are exactly the shapes
/// `project-ledger` already treats as terminal off-ramps.
///
/// **Script payment credentials are rejected.** An `addr1z…` carries a
/// contract's payment script beside the *user's* stake key, so pairing them
/// would assert that a person owns a marketplace validator. This is the PREFIX
/// TRAP that once made 168 separate people look like a single DEX.
pub fn cred_pair(addr: &Address) -> Option<CredPair> {
    let Address::Shelley(sh) = addr else {
        return None;
    };
    let payment = match sh.payment() {
        ShelleyPaymentPart::Key(h) => hash28(h.as_ref()),
        ShelleyPaymentPart::Script(_) => return None,
    };
    let (stake, stake_is_script) = match sh.delegation() {
        ShelleyDelegationPart::Key(h) => (hash28(h.as_ref()), false),
        ShelleyDelegationPart::Script(h) => (hash28(h.as_ref()), true),
        ShelleyDelegationPart::Pointer(_) | ShelleyDelegationPart::Null => return None,
    };
    Some(CredPair {
        payment,
        stake,
        stake_is_script,
    })
}

fn hash28(b: &[u8]) -> KeyHash {
    let mut out = [0u8; 28];
    out.copy_from_slice(&b[..28]);
    out
}

/// Render a stake credential as a mainnet `stake1…` address.
pub fn stake_bech32(cred: &KeyHash, is_script: bool) -> String {
    use pallas_addresses::{Network, StakeAddress, StakePayload};
    let payload = if is_script {
        StakePayload::Script((*cred).into())
    } else {
        StakePayload::Stake((*cred).into())
    };
    StakeAddress::new(Network::Mainnet, payload)
        .to_bech32()
        .unwrap_or_else(|_| hex::encode(cred))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `$uss.enterprise` holding address, and the credentials the ADA
    /// Handle API independently reports for it. If this ever disagrees, the
    /// extraction is wrong in a way that would silently mis-seed every trace.
    const USS: &str = "addr1q90m498g7ysyqmc98mt43uft8d6v8qjx2vwl6nau6kvcc3vm8z68n9678hkjkte543vn7kavy94pjd9kzexxd89z2eaq8xa4jh";
    const USS_PAYMENT: &str = "5fba94e8f120406f053ed758f12b3b74c38246531dfd4fbcd5998c45";
    const USS_STAKE: &str = "stake1uxdn3dreja0rmmft9u62ckfltwkzz6sexjmpvnrxnj39v7s7nt2xu";

    #[test]
    fn base_address_yields_both_credentials() {
        let a = Address::from_bech32(USS).unwrap();
        let p = cred_pair(&a).expect("base address has both credentials");
        assert_eq!(hex::encode(p.payment), USS_PAYMENT);
        assert!(!p.stake_is_script);
        assert_eq!(stake_bech32(&p.stake, p.stake_is_script), USS_STAKE);
    }

    /// An `addr1z…` under the SAME stake key — one of the three Koios reports
    /// for this wallet. Those are contracts the wallet transacted with, and
    /// pairing them would seat a validator as a person.
    #[test]
    fn script_payment_is_rejected() {
        let script = "addr1z9ryamhgnuz6lau86sqytte2gz5rlktv2yce05e0h3207q5m8z68n9678hkjkte543vn7kavy94pjd9kzexxd89z2eaqcuuwp3";
        let a = Address::from_bech32(script).unwrap();
        assert_eq!(cred_pair(&a), None);
    }

    #[test]
    fn stakeless_addresses_yield_nothing() {
        // Enterprise address (payment key, no delegation part).
        let ent = "addr1v9rrf49gcqpxlylsyhkqvpj9tvhpmnhpqzqyfrfyaqmzrzq2zdlnk";
        if let Ok(a) = Address::from_bech32(ent) {
            assert_eq!(cred_pair(&a), None);
        }
    }
}
