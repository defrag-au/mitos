//! Marketplace-agnostic wire vocabulary shared across the
//! `<brand>-store-{listing,sale,offer}` module families.
//!
//! Types here must stay venue-neutral: anything specific to one
//! marketplace's contracts (versions, redeemer quirks) belongs in
//! that marketplace's own module file.

use serde::{Deserialize, Serialize};

/// One recipient of a listing's proceeds, decoded from the
/// on-chain payout list (`Constr 0 [ Address, Lovelace ]` — the
/// shape jpg.store and Wayup share).
///
/// A listing's total price is the sum of its payout lovelace.
/// Consumers derive fee / royalty / seller-take by matching the
/// credentials against their own registries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingPayout {
    /// Recipient payment-credential hash, lowercase hex (28 bytes).
    pub payment_pkh: String,
    /// Optional stake credential, lowercase hex (28 bytes). Some
    /// listings encode enterprise-only recipients.
    pub stake_pkh: Option<String>,
    /// Lovelace this recipient receives when the listing is bought.
    pub lovelace: u64,
}
