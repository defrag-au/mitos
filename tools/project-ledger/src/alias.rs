//! Wallet aliases observed during the walk — no indexer required.
//!
//! People search for a wallet by whatever they have to hand: a `$handle`, a
//! payment address, a stake key. The ledger keys parties by stake key, so the
//! other two need recording — and both are visible on the same decode pass
//! the walk already does:
//!
//! - every output is already resolved to a party, so its **payment address**
//!   is free;
//! - an **ADA Handle** is an asset under one well-known policy, so an output
//!   carrying one names its receiver. Classic handles are the bare UTF-8 name;
//!   CIP-68 handles carry the `000de140` user-token label (the `000643b0`
//!   reference token is the metadata carrier and is NOT a handle anyone holds).
//!
//! Recorded only for wallets the ledger tracks (holders + watched parties) —
//! the set anyone would search for — which keeps it bounded by the project,
//! not the chain.

use mitos_chain_walk::decode::Asset;

use crate::asset_class::AssetClass;

/// ADA Handle mainnet policy id.
pub const ADA_HANDLE_POLICY: [u8; 28] = [
    0xf0, 0xff, 0x48, 0xbb, 0xb7, 0xbb, 0xe9, 0xd5, 0x9a, 0x40, 0xf1, 0xce, 0x90, 0xe9, 0xe9, 0xd0,
    0xff, 0x50, 0x02, 0xec, 0x48, 0xf2, 0x32, 0xb4, 0x9c, 0xa0, 0xfb, 0x9a,
];

/// The handle names (without `$`) among `assets`, if any.
///
/// Sub-handles (`sub@root`) and virtual handles are returned verbatim — they
/// are still what the holder is called.
pub fn handles_in(assets: &[Asset]) -> Vec<String> {
    assets
        .iter()
        .filter(|a| a.policy.as_slice() == ADA_HANDLE_POLICY)
        .filter_map(|a| handle_name(&a.name))
        .collect()
}

/// A handle's display name from its asset-name bytes: strip a CIP-67 user
/// label if present, reject reference tokens and non-UTF-8, lowercase (handles
/// are case-insensitive by convention).
pub fn handle_name(asset_name: &[u8]) -> Option<String> {
    let body = match AssetClass::of(asset_name) {
        AssetClass::UserNft => &asset_name[4..],
        AssetClass::Reference | AssetClass::Ft | AssetClass::Rft => return None,
        AssetClass::Plain => asset_name,
    };
    let s = std::str::from_utf8(body).ok()?.trim();
    if s.is_empty() || s.len() > 64 {
        return None;
    }
    Some(s.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(policy: [u8; 28], name: &[u8]) -> Asset {
        Asset {
            policy: policy.to_vec(),
            name: name.to_vec(),
        }
    }

    #[test]
    fn classic_and_cip68_handles_resolve_reference_does_not() {
        let classic = asset(ADA_HANDLE_POLICY, b"Mekka");
        let mut cip68 = vec![0x00, 0x0d, 0xe1, 0x40];
        cip68.extend_from_slice(b"djo");
        let cip68 = asset(ADA_HANDLE_POLICY, &cip68);
        let mut reference = vec![0x00, 0x06, 0x43, 0xb0];
        reference.extend_from_slice(b"djo");
        let reference = asset(ADA_HANDLE_POLICY, &reference);
        let other_policy = asset([0x29; 28], b"Mekka");

        let hs = handles_in(&[classic, cip68, reference, other_policy]);
        assert_eq!(hs, vec!["mekka", "djo"]);
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(handle_name(&[0xff, 0xfe]), None);
        assert_eq!(handle_name(b""), None);
        assert_eq!(handle_name(b"   "), None);
        assert_eq!(handle_name(b"sub@root"), Some("sub@root".into()));
    }
}
