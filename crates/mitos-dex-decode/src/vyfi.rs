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
//! # The pool datum: two treasuries and the issued shares
//!
//! Three integers, `[treasury_a, treasury_b, issued_shares]`, and the
//! reserves are the UTxO's value LESS the treasuries. MEASURED over five
//! consecutive states of $NikePig's pool (2026-10-27): `treasury_a` grew by
//! exactly 0.1% of the lovelace paid in on a buy (28,684 on 28,684,316) and
//! `treasury_b` by 0.1% of the tokens paid in on each sell (14 on 14,141, 13
//! on 13,850, 34 on 34,488); `issued_shares` never moved across swaps. The
//! treasury is held in the pool's output and owed to the protocol, not
//! liquidity, so it is netted the way WingRiders' is.

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

pub fn is_vyfi_pool(payment_cred: &[u8; 28]) -> bool {
    payment_cred == &POOL_CRED
}

/// A VyFi pool's datum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolDatum {
    /// Owed to the protocol in asset A (ADA, on an ADA pool), held in the
    /// output: not liquidity.
    pub treasury_a: u64,
    /// Owed in asset B, likewise.
    pub treasury_b: u64,
    /// LP shares issued.
    pub issued_shares: u64,
}

/// Decode a VyFi pool datum: `Constr 0 [Int, Int, Int]`.
pub fn decode_pool_datum(cbor: &[u8]) -> Option<PoolDatum> {
    use pallas_codec::minicbor;
    use pallas_primitives::PlutusData;
    let pd: PlutusData = minicbor::decode(cbor).ok()?;
    let PlutusData::Constr(c) = pd else {
        return None;
    };
    let ints: Vec<u64> = c
        .fields
        .iter()
        .map(|f| match f {
            PlutusData::BigInt(b) => crate::bigint_to_u64(b),
            _ => None,
        })
        .collect::<Option<_>>()?;
    let [treasury_a, treasury_b, issued_shares] = ints.as_slice() else {
        return None;
    };
    Some(PoolDatum {
        treasury_a: *treasury_a,
        treasury_b: *treasury_b,
        issued_shares: *issued_shares,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// $NikePig's pool after a buy, `857a9936…`: 384.121807 ADA and 207,184
    /// units in the output, of which the treasuries are owed.
    #[test]
    fn a_pool_datum_is_two_treasuries_and_the_shares() {
        let d = decode_pool_datum(&hex::decode("d8799f1a00b7c705190efc1a0069573aff").unwrap())
            .expect("a pool datum");
        assert_eq!(
            d,
            PoolDatum {
                treasury_a: 12_044_037,
                treasury_b: 3_836,
                issued_shares: 6_903_610,
            }
        );
        assert!(decode_pool_datum(&hex::decode("d8799f1a00b7c705ff").unwrap()).is_none());
    }

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
