//! DexHunter: an aggregator's own order contracts.
//!
//! Identified on $NikePig (2026-10-27) by the CIP-20 messages their
//! transactions carry (`"Dexhunter Trade"`, `"cancel"`, `"STOPLOSS
//! Creation"`, `"DCA Complete"`) and read from decoded transactions:
//!
//! - [`LIMIT_ORDER_CREDS`]: orders under the CUSTOMER's stake, filled into
//!   pools (one was measured filling into Splash) and cancelled back to the
//!   owner. They read like any customer-keyed order contract.
//! - [`DCA_CRED`] and [`STOP_LOSS_CRED`]: ONE enterprise address each,
//!   holding the owner's tokens and routing them into other venues' orders
//!   (Sundae, Minswap, VyFi measured) or back to a wallet. A payout to a
//!   wallet is a delivery or a refund, and the address names no owner, so
//!   neither is read as a trade or a cancellation.

/// The two customer-keyed limit-order contracts, by transactions on
/// $NikePig: 4,771 and 2,320.
pub const LIMIT_ORDER_CREDS: [[u8; 28]; 2] = [
    crate::cred("dbe7a3d8a1d82990992a38eea1a2efaa68e931e252fc92ca1383809b"),
    crate::cred("2025463437ee5d64e89814a66ce7f98cb184a66ae85a2fbbfd750106"),
];

/// The DCA contract: `addr1wxtlzuefvjg9k0r36ysk895gzjk97wl8027ffqc2snfje7gkzkf3h`.
pub const DCA_CRED: [u8; 28] =
    crate::cred("97f1732964905b3c71d12163968814ac5f3be77abc94830a84d32cf9");

/// The stop-loss contract: `addr1w9hvftxrlw74wzk6vf0jfyp8wl8vt4arf8aq70rm4paselc46ptfq`.
pub const STOP_LOSS_CRED: [u8; 28] =
    crate::cred("6ec4acc3fbbd570ada625f24902777cec5d7a349fa0f3c7ba87b0cff");
