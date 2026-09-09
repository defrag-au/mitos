//! snek.fun vesting platform — shared payment script.
//!
//! snek.fun (Splash Protocol) locks sit at a Shelley address
//! with this 28-byte payment credential. As with CrowdLock, the
//! stake credential varies, so consumers recognise a lock by
//! **payment-credential match**, not by full address.
//!
//! Its lock datum is the same family as Shield / CrowdLock —
//! `Constr 0 [ Int(deadline_ms), Bytes(owner_pkh) ]` — differing
//! only in that the owner hash is bare rather than wrapped in a
//! one-element list. [`super::decode_vesting_datum`] handles
//! both.

/// snek.fun's lock payment credential (script hash, 28 bytes).
///
/// Established 2026-08-29 from $Aliens
/// (`16657df3….416c69656e73`): this credential's live UTxOs held
/// 4,063,508 tokens, an exact match for the two locks reported by
/// snek.fun's public pools-feed API
/// (`analytics.snek.fun/v1/pools-feed/initial/state`), and their
/// datums decode to precisely the deadlines that API returns.
pub const PAYMENT_CRED: [u8; 28] = [
    0xdd, 0xbe, 0xbc, 0x62, 0x63, 0x72, 0x6b, 0xcc, 0x03, 0x59, 0x34, 0xb3, 0xb7, 0x1c, 0x1f, 0xb1,
    0x44, 0xa7, 0x8a, 0x1c, 0xbb, 0xc5, 0x74, 0x99, 0xc4, 0x5b, 0x1e, 0x89,
];

/// True when `payment_cred` is snek.fun's lock script hash.
pub fn is_snek_fun(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &PAYMENT_CRED
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode_vesting_datum;

    /// Two real $Aliens lock datums, captured from chain 2026-08-29.
    ///
    /// Both are cross-checked against snek.fun's own API, which
    /// reported these exact deadlines for these exact amounts —
    /// so this is a golden test against an independent source,
    /// not against our own decoder's output.
    const LOCK_A: &str =
        "d879821b0000019eefafa1c3581cbbc1005d1057303cfa5e1e47bd8666cc78584262ce119360e9f5f97f";
    const LOCK_B: &str =
        "d879821b000001a024ad2f95581cbbc1005d1057303cfa5e1e47bd8666cc78584262ce119360e9f5f97f";
    const OWNER: &str = "bbc1005d1057303cfa5e1e47bd8666cc78584262ce119360e9f5f97f";

    #[test]
    fn decodes_real_snekfun_locks() {
        let a = decode_vesting_datum(&hex::decode(LOCK_A).unwrap()).expect("lock A");
        assert_eq!(a.unlock_ts_ms, 1_782_137_725_379);
        assert_eq!(a.owner_pkh_hex, OWNER);

        let b = decode_vesting_datum(&hex::decode(LOCK_B).unwrap()).expect("lock B");
        assert_eq!(b.unlock_ts_ms, 1_787_321_724_821);
        assert_eq!(b.owner_pkh_hex, OWNER);
    }

    #[test]
    fn recognises_its_own_credential() {
        assert!(is_snek_fun(&PAYMENT_CRED));
        assert!(!is_snek_fun(&crate::crowd_lock::PAYMENT_CRED));
        // The two platforms must stay distinguishable — they share a
        // datum family but not a script.
        assert_ne!(PAYMENT_CRED, crate::crowd_lock::PAYMENT_CRED);
    }
}
