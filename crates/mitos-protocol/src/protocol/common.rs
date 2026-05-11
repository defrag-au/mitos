//! Cross-domain primitives. Kept here so each domain module
//! references the same `Address`/`Lovelace`/`OutputRef`/`PlutusBytes`
//! shapes and stays free of domain-crossing imports.
//!
//! Deliberately minimal — string addresses match the classifier's
//! representation; raw plutus data is left as opaque bytes until a
//! consumer surfaces a genuine need to decode at the framework
//! boundary. Per the community-modules-first preference, deep
//! decode work belongs in brand-specific community modules rather
//! than as extensions here.

use cardano_assets::{AssetId, PolicyId};
use enumset::EnumSetType;
use serde::{Deserialize, Serialize};

use super::Domain;

/// NFT-centric classification of an asset by its `asset_name_hex`
/// shape. Drives server-side filtering at the indexer: a consumer
/// that only wants user-facing NFTs subscribes with
/// `roles: enum_set!(NativeUser | Cip68User)` and the indexer
/// drops reference tokens, sentinel mints, etc. before delivery.
///
/// This is orthogonal to `cardano_assets::TokenType` (which is
/// fungibility-based — Nft / Ft / Rft). `AssetRole` captures
/// *purpose by name shape*: did this asset come out of a CIP-68
/// reference label, a CIP-68 user label, or no label at all? Plus
/// the special "empty asset name" sentinel that minting scripts
/// commonly mint at policy creation as a thread/control token.
///
/// `EnumSetType` lets `EnumSet<AssetRole>` serve as a compact
/// filter-axis on `Interest`.
#[derive(Debug, EnumSetType, Hash, Serialize, Deserialize, Default)]
pub enum AssetRole {
    /// Plain native asset, no CIP-68 prefix. Holdable by users.
    /// The default outcome for any non-prefixed asset name.
    #[default]
    NativeUser,
    /// CIP-68 user token (label 222 prefix `000de140`).
    Cip68User,
    /// CIP-68 reference NFT (label 100 prefix `000643b0`). Holds
    /// collection-level metadata at a script address; not
    /// user-owned.
    Cip68Reference,
    /// CIP-68 reference fungible (label 444 prefix `000ad79c`).
    /// Reference token for a CIP-68 fungible; not user-owned.
    Cip68RefFungible,
    /// Empty asset name. Typically a minting-script sentinel /
    /// thread token minted once at policy creation.
    Empty,
}

impl AssetRole {
    /// Derive the role from a hex-encoded asset name. Cheap
    /// prefix-check, no allocation. Used by indexers at emission
    /// time and by `Interest::matches_asset_role` for client
    /// filtering.
    pub fn from_asset_name_hex(name: &str) -> AssetRole {
        if name.is_empty() {
            return AssetRole::Empty;
        }
        if name.len() >= 8 {
            match &name[..8] {
                "000de140" => AssetRole::Cip68User,
                "000643b0" => AssetRole::Cip68Reference,
                "000ad79c" => AssetRole::Cip68RefFungible,
                _ => AssetRole::NativeUser,
            }
        } else {
            // Asset name shorter than a CIP-68 label can't carry
            // one. Treat as plain native.
            AssetRole::NativeUser
        }
    }
}

/// Bech32 Cardano address. Kept stringly here to match the existing
/// classifier output and avoid forcing a typed-address dep into
/// `mitos-core`.
pub type Address = String;

/// Lovelace amount. Newtype-free for ergonomic arithmetic; the field
/// names in payload structs are explicit (`price_lovelace`,
/// `royalty_lovelace`) so misuse at call sites is rare.
pub type Lovelace = u64;

/// Hex-encoded raw Plutus datum / redeemer. Kept opaque at this
/// layer; consumers that need the decoded form re-decode locally
/// using their preferred Plutus library. Storing as bytes (rather
/// than a typed `PlutusData`) avoids a pallas dep at the type-defs
/// crate boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlutusBytes(#[serde(with = "serde_bytes")] pub Vec<u8>);

impl PlutusBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Lightweight transaction-output reference — same shape as
/// `dolos_core::TxoRef` but local so the protocol module doesn't
/// transitively pull dolos types into a consumer that just wants the
/// event vocabulary. Hex-stringly to keep wire format human-readable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OutputRef {
    pub tx_hash: String,
    pub index: u32,
}

impl OutputRef {
    pub fn new(tx_hash: impl Into<String>, index: u32) -> Self {
        Self {
            tx_hash: tx_hash.into(),
            index,
        }
    }
}

/// What mitos emits over the replication channel for protocol
/// indexers (marketplace, dex, lending). One record per
/// `(policy, domain_event)` pair — a tx that touches N policies
/// across N protocols emits N records, regardless of how many
/// assets within each policy the tx referenced.
///
/// `asset_name_hex` is `Some` for events targeting a specific
/// asset within the policy (single-asset sales, single-asset
/// offers) and `None` for events that target the policy as a
/// whole (collection-wide offers) or span multiple assets in the
/// same policy (bundle listings — the asset list lives in the
/// payload, but only one event is emitted per policy).
///
/// `policy_id` and `asset_name_hex` are what the asset-axis of
/// `Interest` matches against. Brand and event-kind are projected
/// out of `domain` via methods on the `Domain` arm's payload (e.g.
/// `marketplace.brand()` / `marketplace.kind()`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolEvent {
    pub policy_id: PolicyId,
    pub asset_name_hex: Option<String>,
    pub tx_hash: String,
    pub slot: u64,
    pub domain: Domain,
}

/// Movement claim returned from `Indexer::handle_event` to the
/// dispatcher's residual pass.
///
/// Indexers that classify an asset transfer (e.g.
/// `Marketplace::Sale`, `Marketplace::ListingCreate`) emit their
/// domain-specific event AND return a `MovementClaim` for the
/// same `(asset, from, to)` triplet. The dispatcher's residual
/// tail-step (`none_match-indexer`) reads the accumulated claim
/// set and skips emitting a redundant `Domain::AssetMovement`
/// for any movement already covered.
///
/// Claims are dispatcher-internal — they don't appear on the
/// consumer-facing wire. They live in `mitos-protocol` (rather
/// than `mitos-core`) because they're stable typed primitives
/// that benefit from serde derives + wire-shape stability, even
/// though current consumers don't read them.
///
/// See `docs/design/DOMAIN_REFACTOR.md` "Dispatch mechanism" for
/// how claims feed the residual pass.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MovementClaim {
    /// The asset that moved (policy_id + asset_name_hex).
    pub asset: AssetId,
    /// Bech32 source address.
    pub from: Address,
    /// Bech32 destination address.
    pub to: Address,
}
