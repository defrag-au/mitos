//! Stake credentials named EXPLICITLY by a transaction — certificates and
//! reward withdrawals.
//!
//! ## Why this is worth a table of its own
//!
//! Stake credentials already reach the co-signing graph on their own: a reward
//! withdrawal cannot be built without the stake key's signature, so the stake
//! credential lands in `cosign` beside whichever payment keys funded the
//! transaction. The first full-chain trace saw exactly that —
//! `$uss.enterprise`'s stake key appeared as a co-signer 362 times.
//!
//! What the witness set does NOT carry is *which* of those 28-byte hashes is a
//! stake credential. Every key in a cluster looks alike, so `trace` reported 49
//! of 51 co-signers as "no address ever seen beside them" — opaque, when some
//! of them are stake credentials that name a wallet directly.
//!
//! This table supplies that labelling, and two things follow:
//!
//! 1. a cluster member that is a known stake credential renders as a `stake1…`
//!    wallet **without** needing a `cred_pair` sighting;
//! 2. a transaction naming **two or more distinct stake credentials** is
//!    same-owner evidence in its own right — nobody withdraws another person's
//!    rewards or registers their stake key.
//!
//! Script credentials are recorded with a flag rather than dropped: a script
//! stake credential is a contract's own delegation and must never be read as a
//! person, which is the same PREFIX TRAP `creds.rs` guards against.

use pallas_primitives::StakeCredential;
use pallas_traverse::MultiEraTx;

use crate::witness::KeyHash;

/// What a transaction did to a stake credential.
pub const KIND_REG: &str = "reg";
pub const KIND_DEREG: &str = "dereg";
pub const KIND_DELEG: &str = "deleg";
pub const KIND_WITHDRAW: &str = "withdraw";
pub const KIND_VOTE: &str = "vote";

/// One stake credential named by a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StakeEvent {
    pub cred: KeyHash,
    pub is_script: bool,
    pub kind: &'static str,
}

/// Every stake credential this transaction names, via certificate or withdrawal.
pub fn stake_events(tx: &MultiEraTx<'_>) -> Vec<StakeEvent> {
    let mut out = Vec::new();

    // Withdrawals are the strongest of the lot: the stake key MUST witness the
    // transaction for one to be valid, so this row and the witness set are two
    // views of the same fact.
    for (account, _amount) in tx.withdrawals_sorted_set() {
        if let Some(ev) = from_reward_account(account) {
            out.push(ev);
        }
    }

    for cert in tx.certs() {
        // Conway first — it is the live era and carries the governance variants
        // Alonzo has no representation for.
        if let Some(c) = cert.as_conway() {
            use pallas_primitives::conway::Certificate as C;
            let (cred, kind) = match c {
                C::StakeRegistration(c) => (c, KIND_REG),
                C::Reg(c, _) => (c, KIND_REG),
                C::StakeDeregistration(c) => (c, KIND_DEREG),
                C::UnReg(c, _) => (c, KIND_DEREG),
                C::StakeDelegation(c, _) => (c, KIND_DELEG),
                C::StakeRegDeleg(c, _, _) => (c, KIND_DELEG),
                C::StakeVoteDeleg(c, _, _) => (c, KIND_DELEG),
                C::StakeVoteRegDeleg(c, _, _, _) => (c, KIND_DELEG),
                C::VoteDeleg(c, _) => (c, KIND_VOTE),
                C::VoteRegDeleg(c, _, _) => (c, KIND_VOTE),
                // Pool and committee certificates name operators and hot/cold
                // keys, not a delegator's stake credential. Out of scope.
                _ => continue,
            };
            out.push(from_credential(cred, kind));
        } else if let Some(c) = cert.as_alonzo() {
            use pallas_primitives::alonzo::Certificate as C;
            let (cred, kind) = match c {
                C::StakeRegistration(c) => (c, KIND_REG),
                C::StakeDeregistration(c) => (c, KIND_DEREG),
                C::StakeDelegation(c, _) => (c, KIND_DELEG),
                _ => continue,
            };
            out.push(from_credential(cred, kind));
        }
    }

    out.sort_by(|a, b| a.cred.cmp(&b.cred).then(a.kind.cmp(b.kind)));
    out.dedup();
    out
}

fn from_credential(c: &StakeCredential, kind: &'static str) -> StakeEvent {
    let (bytes, is_script) = match c {
        StakeCredential::AddrKeyhash(h) => (h.as_ref(), false),
        StakeCredential::ScriptHash(h) => (h.as_ref(), true),
    };
    StakeEvent {
        cred: hash28(bytes),
        is_script,
        kind,
    }
}

/// A reward account is a 29-byte stake address: one header byte then the
/// 28-byte credential. The header's high nibble is `0b1111` for a script
/// credential and `0b1110` for a key.
fn from_reward_account(account: &[u8]) -> Option<StakeEvent> {
    if account.len() != 29 {
        return None;
    }
    Some(StakeEvent {
        cred: hash28(&account[1..]),
        is_script: (account[0] >> 4) & 0x0f == 0x0f,
        kind: KIND_WITHDRAW,
    })
}

fn hash28(b: &[u8]) -> KeyHash {
    let mut out = [0u8; 28];
    out.copy_from_slice(&b[..28]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reward_account_splits_header_from_credential() {
        let mut acct = vec![0xe1]; // 0b1110_0001 — key credential, mainnet
        acct.extend_from_slice(&[7u8; 28]);
        let ev = from_reward_account(&acct).expect("29 bytes is a reward account");
        assert_eq!(ev.cred, [7u8; 28]);
        assert!(!ev.is_script);
        assert_eq!(ev.kind, KIND_WITHDRAW);

        let mut script = vec![0xf1]; // 0b1111_0001 — SCRIPT credential
        script.extend_from_slice(&[9u8; 28]);
        assert!(from_reward_account(&script).unwrap().is_script);
    }

    /// A wrong-length account must be dropped, not silently truncated into a
    /// plausible-looking credential that would seat a wallet that never existed.
    #[test]
    fn malformed_reward_accounts_are_refused() {
        assert_eq!(from_reward_account(&[0xe1; 28]), None);
        assert_eq!(from_reward_account(&[]), None);
        assert_eq!(from_reward_account(&[0xe1; 30]), None);
    }

    #[test]
    fn script_credentials_are_flagged_not_dropped() {
        let c = StakeCredential::ScriptHash([3u8; 28].into());
        let ev = from_credential(&c, KIND_DELEG);
        assert!(ev.is_script);
        assert_eq!(ev.cred, [3u8; 28]);
    }
}
