//! Resolve a marketplace contract script address to a typed
//! `MarketplaceBrand`.
//!
//! The existing classifier already records which marketplace
//! contract a given `TxType::Sale` / `Listing*` / `Offer*` variant
//! came from — it surfaces the contract address as a `String` field
//! (`marketplace`, optional on `Sale`). We resolve that string to a
//! brand via `address_registry::Marketplace::from_address`, then
//! map the registry's `Marketplace` enum into our typed
//! `mitos_protocol::MarketplaceBrand`.
//!
//! Doing the lookup mitos-side (rather than threading brand
//! through the classifier) keeps `tx-classifier` free of
//! mitos-protocol coupling. The address-registry is already the
//! source of truth for which script belongs to which brand; we
//! re-use it here verbatim.

use address_registry::Marketplace as RegistryMarketplace;
use mitos_protocol::MarketplaceBrand;

/// Resolve a marketplace contract script address to a brand.
///
/// `Unknown` is returned when:
/// - the address doesn't appear in the registry at all
/// - the registry recognises it but as a brand mitos doesn't
///   currently catalogue (registry has `Unknown`)
///
/// In both cases the consumer worker can decide whether to drop
/// the event or attempt a generic handler. The original script
/// string is preserved upstream (in `OfferCancelPayload::Unknown`'s
/// `brand_script` field, etc.) so manual triage is possible.
pub fn marketplace_brand_from_address(addr: &str) -> MarketplaceBrand {
    match RegistryMarketplace::from_address(addr) {
        Some(RegistryMarketplace::JpgStore) => MarketplaceBrand::JpgStore,
        Some(RegistryMarketplace::Wayup) => MarketplaceBrand::Wayup,
        Some(RegistryMarketplace::Unknown) | None => MarketplaceBrand::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jpg_store_v1_address_resolves_to_jpgstore() {
        // Sale contract address from address-registry.
        let addr = "addr1zxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tvpw288a4x0xf8pxgcntelxmyclq83s0ykeehchz2wtspks905plm";
        assert_eq!(
            marketplace_brand_from_address(addr),
            MarketplaceBrand::JpgStore
        );
    }

    #[test]
    fn jpg_store_v2_address_resolves_to_jpgstore() {
        let addr = "addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx";
        assert_eq!(
            marketplace_brand_from_address(addr),
            MarketplaceBrand::JpgStore
        );
    }

    #[test]
    fn jpg_store_v4_address_resolves_to_jpgstore() {
        let addr = "addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp";
        assert_eq!(
            marketplace_brand_from_address(addr),
            MarketplaceBrand::JpgStore
        );
    }

    #[test]
    fn wayup_address_resolves_to_wayup() {
        let addr = "addr1zxnk7racqx3f7kg7npc4weggmpdskheu8pm57egr9av0mtvasazx8r5xwqtnfjsfrnat3h6yrycd2hfm9qpg7d0hf50s7x4y79";
        assert_eq!(
            marketplace_brand_from_address(addr),
            MarketplaceBrand::Wayup
        );
    }

    #[test]
    fn unknown_address_resolves_to_unknown() {
        // Random non-marketplace address — even if the format is
        // valid, the registry has no entry, so we get Unknown.
        let addr = "addr1qx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj0vs2qd4a6v9hyqsdqsqfppmt";
        assert_eq!(
            marketplace_brand_from_address(addr),
            MarketplaceBrand::Unknown
        );
    }
}
