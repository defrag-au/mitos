//! Wire-format event types for the `asset-metadata-update`
//! community module — metadata refreshes that aren't initial
//! mints.
//!
//! Two refresh paths exist on Cardano:
//!
//! - **CIP-25 refresh**: a TX with `quantity_delta <= 0` for an
//!   asset (a burn, or a burn-and-remint pair netting non-
//!   positive) that nonetheless carries label-721 metadata for
//!   the asset. Chain indexers historically used this hack to
//!   rotate NFT metadata without requiring the on-chain
//!   ownership change of a true mint.
//! - **CIP-68 refresh**: a TX that consumes the `_100`-prefixed
//!   reference output and re-produces it (under the same policy
//!   and asset name) with a different datum. No `tx.mint` entry
//!   is needed — the reference asset moves between UTxOs and
//!   the datum is the metadata source.
//!
//! See `mitos/docs/design/MINT_BURN_MODULES.md` for the family
//! design.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AssetMetadataUpdate {
    /// CIP-25 metadata refresh observed in a non-positive-mint TX.
    Cip25 {
        /// 56-char lowercase hex policy id.
        policy: String,
        /// Hex of the asset name (raw on-chain bytes).
        asset_name_hex: String,
        /// 64-char lowercase hex tx hash.
        tx_hash: String,
        /// JSON-stringified label-721 entry for the asset.
        /// `None` if the label-721 block exists but the asset's
        /// entry within it failed to decode.
        new_metadata_json: Option<String>,
    },
    /// CIP-68 reference-output respend with a different datum.
    Cip68 {
        /// 56-char lowercase hex policy id.
        policy: String,
        /// Hex of the `_100`-prefix-tagged asset name (full
        /// on-chain bytes, CIP-67 4-byte header included).
        reference_asset_name_hex: String,
        /// 64-char lowercase hex tx hash.
        tx_hash: String,
        /// Raw CBOR of the consumed reference output's datum.
        #[serde(with = "serde_bytes")]
        previous_datum_cbor: Vec<u8>,
        /// Raw CBOR of the produced reference output's new datum.
        #[serde(with = "serde_bytes")]
        new_datum_cbor: Vec<u8>,
        /// JSON-stringified CIP-68 metadata map (Constructor-0
        /// field 0) of the previous datum. `None` if decode
        /// failed or the structure didn't match the CIP-68 shape.
        previous_metadata_json: Option<String>,
        /// JSON-stringified CIP-68 metadata map of the new datum.
        /// `None` under the same conditions as
        /// `previous_metadata_json`.
        new_metadata_json: Option<String>,
    },
}
