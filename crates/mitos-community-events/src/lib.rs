//! Shared event types for mitos community modules.
//!
//! Each community module (`mitos/community-modules/<name>/`) owns
//! a submodule here: `pub mod <name>;`. Consumers depend on this
//! single crate and `use mitos_community_events::<name>::*;` rather
//! than each dApp shipping its own `<module>-events` crate.
//!
//! See `docs/strategy/COMMUNITY_MODULES.md` for the design.

pub mod asset_metadata_update;
pub mod asset_transfer;
pub mod burn_address;
pub mod cip25_mint;
pub mod cip68_mint;
pub mod dex;
pub mod jpg_store_listing;
pub mod jpg_store_offer;
pub mod jpg_store_sale;
pub mod standard_burn;

/// Decode + pretty-print an emit-channel payload for a known
/// community module. Returns `None` when the `module_id` isn't
/// registered, the channel doesn't carry a known event shape,
/// or the payload fails to deserialise.
///
/// Used by `mitos-run` to surface typed emissions during local
/// module testing — the registry pattern lets us keep wire-shape
/// authority in one crate while host-side tools render whatever
/// modules emit without hard-coding decode paths per tool.
///
/// Module-id strings match the artifact's `manifest.module.id`
/// (== the module's source-file stem with hyphens preserved).
#[cfg(feature = "decode")]
pub fn decode_emit(module_id: &str, channel: u32, payload: &[u8]) -> Option<String> {
    match module_id {
        "asset-metadata-update" => asset_metadata_update::decode_emit(channel, payload),
        "asset-transfer" => asset_transfer::decode_emit(channel, payload),
        "burn-address" => burn_address::decode_emit(channel, payload),
        "cip-25-mint" => cip25_mint::decode_emit(channel, payload),
        "cip-68-mint" => cip68_mint::decode_emit(channel, payload),
        "cswap-dex" => dex::decode_emit(channel, payload),
        "jpg-store-listing" => jpg_store_listing::decode_emit(channel, payload),
        "jpg-store-offer" => jpg_store_offer::decode_emit(channel, payload),
        "jpg-store-sale" => jpg_store_sale::decode_emit(channel, payload),
        "standard-burn" => standard_burn::decode_emit(channel, payload),
        _ => None,
    }
}

#[cfg(all(test, feature = "decode"))]
mod decode_tests {
    use super::*;

    #[test]
    fn round_trip_standard_burn() {
        let burn = standard_burn::Burn {
            policy: "a".repeat(56),
            asset_name_hex: "b".repeat(8),
            tx_hash: "c".repeat(64),
            quantity_burned: 42,
        };
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&burn, &mut buf).unwrap();

        let json = decode_emit("standard-burn", 0, &buf).expect("decoder registered");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["quantity_burned"], 42);
        assert_eq!(parsed["policy"], "a".repeat(56));
    }

    #[test]
    fn unknown_module_id_returns_none() {
        assert!(decode_emit("does-not-exist", 0, &[]).is_none());
    }

    #[test]
    fn non_zero_channel_returns_none() {
        let burn = standard_burn::Burn {
            policy: String::new(),
            asset_name_hex: String::new(),
            tx_hash: String::new(),
            quantity_burned: 0,
        };
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&burn, &mut buf).unwrap();
        assert!(decode_emit("standard-burn", 1, &buf).is_none());
    }

    #[test]
    fn malformed_payload_returns_none() {
        assert!(decode_emit("standard-burn", 0, &[0xff, 0xff, 0xff]).is_none());
    }
}
