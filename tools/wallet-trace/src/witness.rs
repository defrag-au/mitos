//! Signer keys — the clustering primitive.
//!
//! Cardano requires every key-locked input to be authorised by a signature in
//! the transaction's **own witness set**. So the co-signing group is readable
//! from the block bytes alone: no input resolution, no outref ladder, no Koios.
//! That single fact is why this walker is stateless and forward-only where
//! `project-ledger` needed 2,786,844 ref lookups to say anything at all.
//!
//! `blake2b224(vkey)` **is** the payment key hash — the same 28-byte credential
//! space `project-ledger/src/party.rs` extracts from addresses. Witnesses and
//! addresses therefore land in ONE union-find with no bridging step.
//!
//! ## What the witness set is a superset of
//!
//! It covers the key-locked inputs (the classic multi-input heuristic) and also
//! `required_signers`, certificate signers, reward-withdrawal signers, and
//! native-script multisig participants. More signal — and more false-merge
//! surface, which is [`crate::probe`]'s whole reason for existing.
//!
//! ## What is deliberately excluded
//!
//! **Byron bootstrap witnesses.** Their payload carries a chain code alongside
//! the public key, so the hash of the key alone is not the credential that
//! address commits to — treating them as key hashes would silently invent
//! identities. Pre-Icarus wallets are already known-orphaned elsewhere in this
//! workspace. They are COUNTED (see [`SignerSet::bootstrap`]) rather than
//! silently dropped, because a walker that skips something without saying so is
//! how a 6.4% resolution rate went unnoticed for a week.

use std::collections::BTreeSet;

use pallas_crypto::hash::Hasher;
use pallas_traverse::MultiEraTx;

/// A 28-byte credential: `blake2b224` of an ed25519 public key. Identical in
/// shape and meaning to the payment/stake credential inside an address.
pub type KeyHash = [u8; 28];

/// Hash a raw ed25519 public key to its credential.
pub fn key_hash(vkey: &[u8]) -> KeyHash {
    let h = Hasher::<224>::hash(vkey);
    let mut out = [0u8; 28];
    out.copy_from_slice(h.as_ref());
    out
}

/// One transaction's signers.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SignerSet {
    /// Distinct credentials that signed. Sorted, so a group has a canonical
    /// form regardless of witness order.
    pub keys: BTreeSet<KeyHash>,
    /// Byron bootstrap witnesses present, not hashed. See the module doc.
    pub bootstrap: usize,
}

impl SignerSet {
    /// Distinct signing credentials. A group of `< 2` joins nothing.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// No signing credentials at all. Combined with `bootstrap > 0` this is a
    /// transaction wholly invisible to witness clustering — a Byron-era spend.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Whether this transaction can contribute a co-signing edge at all.
    pub fn is_group(&self) -> bool {
        self.keys.len() >= 2
    }
}

/// The signers of `tx`, deduplicated by credential.
///
/// Deduplication matters: a witness set may legitimately repeat a key, and a
/// naive count would inflate the group size and trip the size cap that keeps
/// batchers out of the clustering.
pub fn signer_keys(tx: &MultiEraTx<'_>) -> SignerSet {
    SignerSet {
        keys: tx
            .vkey_witnesses()
            .iter()
            .map(|w| key_hash(w.vkey.as_ref()))
            .collect(),
        bootstrap: tx.bootstrap_witnesses().len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cross-checked against pallas-crypto's own documented example, which
    /// exists precisely to pin "the 28-byte digest used for public key
    /// addresses". If this fails, we are hashing with the wrong algorithm or
    /// the wrong width — a silent version of that would make every cluster
    /// wrong while looking perfectly plausible.
    #[test]
    fn key_hash_is_blake2b_224() {
        assert_eq!(
            hex::encode(key_hash(b"My Public Key")),
            "c123c9bc0e9e31a20a4aa23518836ec5fb54bdc85735c56b38eb79a5"
        );
        assert_eq!(key_hash(b"anything").len(), 28);
    }

    #[test]
    fn groups_need_two_distinct_keys() {
        let mut s = SignerSet::default();
        assert!(!s.is_group());
        s.keys.insert(key_hash(b"a"));
        assert!(!s.is_group());
        s.keys.insert(key_hash(b"b"));
        assert!(s.is_group());
        // A repeated key is one credential, not two.
        s.keys.insert(key_hash(b"a"));
        assert_eq!(s.len(), 2);
    }
}
