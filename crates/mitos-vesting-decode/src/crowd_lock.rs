//! CrowdLock vesting platform — shared payment script.
//!
//! Every CrowdLock lock UTxO sits at a Shelley address with this
//! 28-byte payment credential, regardless of project token. The
//! stake credential is varying (contract-derived per locker), so
//! `holder-distribution` and `vesting-tracker` recognise
//! CrowdLock locks by **payment-credential match**, not by full
//! address.

/// The CrowdLock platform's payment credential (script hash,
/// 28 bytes).
pub const PAYMENT_CRED: [u8; 28] = [
    0x38, 0x1c, 0xd9, 0xb6, 0x20, 0xce, 0x9d, 0x7c, 0x73, 0xdd, 0x45, 0xcf, 0xe2, 0x07, 0x97, 0x1c,
    0xc7, 0x9d, 0xf7, 0x92, 0x51, 0x00, 0x31, 0x16, 0x09, 0x4f, 0x6b, 0x36,
];

/// True when `payment_cred` is the CrowdLock platform's script
/// hash.
pub fn is_crowd_lock(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &PAYMENT_CRED
}
