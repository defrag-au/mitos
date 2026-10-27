//! VyFi DEX: pool and order credentials.
//!
//! Identified on $NikePig (2026-10-27), where the pool held 7,002 outputs and
//! 7.3 billion units read as an unclaimed script:
//!
//! - the pool's one NFT (policy `8dc8980c…`, empty name) was minted with the
//!   CIP-20 message `"VyFi: LP Init"`;
//! - the order contract was created in the SAME transaction as the pool
//!   script (`14d2d247…`), and every one of its outputs seen feeds the pool.
//!
//! Recognition only. The pool datum is three integers (MEASURED
//! `[12044037, 3836, 6903610]`), most likely the two treasuries and the issued
//! LP shares, but that reading is not verified, so reserves are not decoded
//! here and a VyFi pool publishes no price yet.

/// The pool contract's payment credential. Pool addresses carry VyFi's own
/// stake credential (`b6811a70…`), not a customer's.
pub const POOL_CRED: [u8; 28] = [
    0xcd, 0x2d, 0x50, 0xa2, 0xf8, 0x36, 0xab, 0x27, 0xa7, 0x5f, 0x13, 0xd6, 0xb9, 0x8a, 0x54, 0xc0,
    0x08, 0x84, 0x8f, 0x91, 0xea, 0x3b, 0xc0, 0x25, 0xc2, 0x7e, 0x76, 0xda,
];

/// The order contract's payment credential.
///
/// ⚠️ ONE enterprise address for every trader,
/// `addr1wy2cqa9mlhsz7wg9476z8cgywj2y7qwqqcsng6wm6at2y7q7cjkqh`. The owner is in
/// the datum (MEASURED: a 56-byte payment-plus-stake key pair, then the order
/// kind with its minimum to receive), not the address, so a spend from it
/// cannot name who placed the order.
pub const ORDER_CRED: [u8; 28] = [
    0x15, 0x80, 0x74, 0xbb, 0xfd, 0xe0, 0x2f, 0x39, 0x05, 0xaf, 0xb4, 0x23, 0xe1, 0x04, 0x74, 0x94,
    0x4f, 0x01, 0xc0, 0x06, 0x21, 0x34, 0x69, 0xdb, 0xd7, 0x56, 0xa2, 0x78,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The constants are the hashes the chain reports, byte for byte.
    #[test]
    fn the_credentials_match_their_hex() {
        assert_eq!(
            hex::encode(POOL_CRED),
            "cd2d50a2f836ab27a75f13d6b98a54c008848f91ea3bc025c27e76da"
        );
        assert_eq!(
            hex::encode(ORDER_CRED),
            "158074bbfde02f3905afb423e10474944f01c006213469dbd756a278"
        );
    }
}
