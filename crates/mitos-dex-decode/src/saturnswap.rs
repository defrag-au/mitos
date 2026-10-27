//! SaturnSwap: an order book, recognised by credential and by stake.
//!
//! MEASURED on $NikePig (2026-10-27), from decoded transactions and datums:
//!
//! - **Orders** sit at ONE exchange contract ([`EXCHANGE_CRED`]), under the
//!   OWNER's stake credential: the wallet that later withdrew an order's
//!   proceeds carried the same stake as the order. An order under SaturnSwap's
//!   own stake ([`STAKE_CRED`]) names no owner.
//! - **A taker buys** by spending sell orders: the token goes straight to the
//!   taker's wallet, the ADA to an escrow. **A taker sells** by paying the
//!   token into escrows, one per buy order filled.
//! - **Escrows** are every OTHER script under [`STAKE_CRED`], a dozen on
//!   $NikePig alone. An escrow output's datum is only the filled order's output
//!   reference; it pays out later to the order's owner, or relists into the
//!   owner's order at the exchange.
//!
//! The order datum (MEASURED): payout address (an escrow under
//! [`STAKE_CRED`]), offered policy and name, offered amount, wanted policy and
//! name, wanted amount, a flag, and the order's own output reference as its id.
//! Not decoded here: the trade fold reads the movements, which say enough.

/// The exchange contract, where open orders sit.
pub const EXCHANGE_CRED: [u8; 28] = [
    0x1a, 0xf8, 0x4a, 0x9e, 0x69, 0x7e, 0x1e, 0x7b, 0x04, 0x2a, 0x0a, 0x06, 0xf0, 0x61, 0xe8, 0x81,
    0x82, 0xfe, 0xb9, 0xe9, 0xad, 0xa9, 0x50, 0xb3, 0x6a, 0x91, 0x6b, 0xd5,
];

/// SaturnSwap's own stake credential: on every escrow, and on the exchange
/// for the orders that name no owner.
pub const STAKE_CRED: [u8; 28] = [
    0x5e, 0xa4, 0x81, 0x52, 0x30, 0x30, 0xb2, 0x3a, 0x49, 0x52, 0x86, 0xca, 0x1a, 0x18, 0xbd, 0x14,
    0x1a, 0x49, 0x3e, 0x9b, 0x5a, 0x19, 0xd8, 0x89, 0x95, 0x3f, 0x6c, 0xdb,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_credentials_match_their_hex() {
        assert_eq!(
            hex::encode(EXCHANGE_CRED),
            "1af84a9e697e1e7b042a0a06f061e88182feb9e9ada950b36a916bd5"
        );
        assert_eq!(
            hex::encode(STAKE_CRED),
            "5ea481523030b23a495286ca1a18bd141a493e9b5a19d889953f6cdb"
        );
    }
}
