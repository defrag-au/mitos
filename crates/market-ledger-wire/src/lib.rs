//! Wire format for the market-ledger read surface (`market-ledger serve`).
//!
//! Postcard-encoded dictionary envelope: an [`EventsPage`] carries interned
//! side tables (policies / stakes / marketplaces / venues) and rows that
//! reference them by index, with hashes as fixed byte arrays rather than hex
//! text. Server encodes with [`encode_events_page`]; consumers (egui WASM
//! frontends, workers) decode with [`decode_events_page`].
//!
//! # Encoding contract
//!
//! Postcard is positional — there are no field names or tags on the wire, so
//! the struct definitions here ARE the format:
//!
//! - never reorder, remove, or insert fields; never change a field's type or
//!   `Option`-ness;
//! - no `#[serde(skip_serializing_if)]` / `default` — every field is always
//!   present;
//! - enum variants are append-only (discriminant = declaration order);
//! - rows live inside `Vec<EventRow>`, so even *appending* a field to
//!   [`EventRow`] is a breaking change.
//!
//! Any change therefore bumps [`WIRE_VERSION`] and adds V2 types alongside
//! the V1 ones. `version` is the first field of [`EventsPage`] and a plain
//! `u8`, so it encodes as byte 0 of every payload — clients peek it before
//! decoding the rest and fail loudly on a mismatch instead of decoding
//! garbage.

use serde::{Deserialize, Serialize};

/// Version byte at offset 0 of every encoded [`EventsPage`]. Bump on ANY
/// change to the types in this crate (see the module docs for what counts).
pub const WIRE_VERSION: u8 = 1;

/// Market event kind. Discriminants are postcard varint tags in declaration
/// order — append-only, never reorder (pinned by a test below).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventKind {
    Sold,                    // 0
    Listed,                  // 1
    PriceChange,             // 2
    Delisted,                // 3
    OfferAccepted,           // 4
    CollectionOfferAccepted, // 5
    OfferCreated,            // 6
    OfferUpdated,            // 7
    OfferCancelled,          // 8
}

impl EventKind {
    /// All kinds, in wire-discriminant order.
    pub const ALL: [EventKind; 9] = [
        EventKind::Sold,
        EventKind::Listed,
        EventKind::PriceChange,
        EventKind::Delisted,
        EventKind::OfferAccepted,
        EventKind::CollectionOfferAccepted,
        EventKind::OfferCreated,
        EventKind::OfferUpdated,
        EventKind::OfferCancelled,
    ];

    /// The ledger's `market_events.kind` string for this kind.
    pub fn as_db_str(&self) -> &'static str {
        match self {
            EventKind::Sold => "sold",
            EventKind::Listed => "listed",
            EventKind::PriceChange => "price_change",
            EventKind::Delisted => "delisted",
            EventKind::OfferAccepted => "offer_accepted",
            EventKind::CollectionOfferAccepted => "collection_offer_accepted",
            EventKind::OfferCreated => "offer_created",
            EventKind::OfferUpdated => "offer_updated",
            EventKind::OfferCancelled => "offer_cancelled",
        }
    }

    /// Inverse of [`Self::as_db_str`].
    pub fn from_db_str(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_db_str() == s)
    }
}

/// One market event. String-ish columns are interned: `policy`,
/// `seller_stake`, `buyer_stake`, `marketplace` and `venue` index into the
/// side tables on the enclosing [`EventsPage`].
///
/// Deliberately omitted vs the ledger row: `fingerprint` (CIP-14, derivable
/// from `policy` + `asset_name`; still filterable server-side) and `source`
/// (operational provenance).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRow {
    pub tx_hash: [u8; 32],
    /// Index into [`EventsPage::policies`].
    pub policy: u32,
    /// Raw asset-name bytes (may be empty).
    pub asset_name: Vec<u8>,
    pub kind: EventKind,
    pub price_lovelace: Option<u64>,
    pub buyer_price_lovelace: Option<u64>,
    /// Index into [`EventsPage::stakes`].
    pub seller_stake: Option<u32>,
    /// Index into [`EventsPage::stakes`].
    pub buyer_stake: Option<u32>,
    /// Index into [`EventsPage::marketplaces`].
    pub marketplace: u32,
    /// Index into [`EventsPage::venues`].
    pub venue: u32,
    pub bundle_size: Option<u32>,
    pub output_index: Option<u32>,
    pub fee_waived: bool,
    pub slot: u64,
    pub block_height: Option<u64>,
    /// Unix seconds.
    pub block_time: i64,
}

/// One page of query results: interned side tables + rows + continuation
/// cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventsPage {
    /// MUST stay the first field — encodes as byte 0 (see module docs).
    pub version: u8,
    /// Policy ids (28 raw bytes each), referenced by [`EventRow::policy`].
    pub policies: Vec<[u8; 28]>,
    /// Stake addresses as stored in the ledger (bech32 text), interned.
    pub stakes: Vec<String>,
    pub marketplaces: Vec<String>,
    pub venues: Vec<String>,
    /// Ordered (slot ASC, rowid ASC) — the pagination order.
    pub events: Vec<EventRow>,
    /// Opaque cursor for the next page; `None` = end of results.
    pub next_cursor: Option<String>,
}

impl EventsPage {
    /// An empty page at the current wire version.
    pub fn empty() -> Self {
        EventsPage {
            version: WIRE_VERSION,
            policies: Vec::new(),
            stakes: Vec::new(),
            marketplaces: Vec::new(),
            venues: Vec::new(),
            events: Vec::new(),
            next_cursor: None,
        }
    }
}

/// One current live listing. Interned columns (`policy`, `seller`, `venue`)
/// index the [`ListingsPage`] side tables. `price_lovelace` is the listing
/// **ask** (the datum payout sum); consumers fold the venue fee for the
/// buyer-price (wayup `+ask/49`). `None` only for the rare jpg listing whose
/// datum isn't resolvable on-chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingRow {
    /// Index into [`ListingsPage::policies`].
    pub policy: u32,
    /// Raw asset-name bytes.
    pub asset_name: Vec<u8>,
    pub price_lovelace: Option<u64>,
    /// Index into [`ListingsPage::sellers`].
    pub seller: Option<u32>,
    /// Index into [`ListingsPage::venues`].
    pub venue: u32,
    /// The current listing UTxO.
    pub tx_hash: [u8; 32],
    pub output_index: u32,
    pub listed_slot: u64,
    /// Unix seconds.
    pub listed_time: i64,
}

/// One page of current listings for a policy — interned side tables + rows +
/// floor/count header + continuation cursor. Same versioning + append-only
/// contract as [`EventsPage`] (byte 0 = [`WIRE_VERSION`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListingsPage {
    /// MUST stay the first field — encodes as byte 0 (see module docs).
    pub version: u8,
    pub policies: Vec<[u8; 28]>,
    /// Seller stake identifiers as stored, interned.
    pub sellers: Vec<String>,
    pub venues: Vec<String>,
    /// Ordered price ASC (cheapest first; unpriced last) — floor is `[0]`.
    pub listings: Vec<ListingRow>,
    /// Collection-wide floor = min served price over all priced listings.
    pub floor_lovelace: Option<u64>,
    /// Total current listings for the policy (not just this page).
    pub count: u32,
    /// Opaque cursor for the next page; `None` = end.
    pub next_cursor: Option<String>,
}

impl ListingsPage {
    /// An empty page at the current wire version.
    pub fn empty() -> Self {
        ListingsPage {
            version: WIRE_VERSION,
            policies: Vec::new(),
            sellers: Vec::new(),
            venues: Vec::new(),
            listings: Vec::new(),
            floor_lovelace: None,
            count: 0,
            next_cursor: None,
        }
    }
}

/// Decode-side errors.
#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    /// Payload is empty — not even a version byte.
    Empty,
    /// Byte 0 didn't match [`WIRE_VERSION`]: `{ got }` is what the server
    /// sent. A version-locked consumer should surface this as "update me".
    VersionMismatch { got: u8 },
    /// Postcard decode failure after the version check.
    Decode(postcard::Error),
}

impl core::fmt::Display for WireError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WireError::Empty => write!(f, "empty payload"),
            WireError::VersionMismatch { got } => {
                write!(
                    f,
                    "wire version mismatch: got {got}, expected {WIRE_VERSION}"
                )
            }
            WireError::Decode(e) => write!(f, "postcard decode failed: {e}"),
        }
    }
}

impl std::error::Error for WireError {}

/// Encode a page to the wire bytes.
pub fn encode_events_page(page: &EventsPage) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(page)
}

/// Decode wire bytes, checking the version byte first.
pub fn decode_events_page(bytes: &[u8]) -> Result<EventsPage, WireError> {
    match bytes.first() {
        None => Err(WireError::Empty),
        Some(&v) if v != WIRE_VERSION => Err(WireError::VersionMismatch { got: v }),
        Some(_) => postcard::from_bytes(bytes).map_err(WireError::Decode),
    }
}

/// Encode a listings page to the wire bytes.
pub fn encode_listings_page(page: &ListingsPage) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(page)
}

/// Decode listings wire bytes, checking the version byte first.
pub fn decode_listings_page(bytes: &[u8]) -> Result<ListingsPage, WireError> {
    match bytes.first() {
        None => Err(WireError::Empty),
        Some(&v) if v != WIRE_VERSION => Err(WireError::VersionMismatch { got: v }),
        Some(_) => postcard::from_bytes(bytes).map_err(WireError::Decode),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_row() -> EventRow {
        EventRow {
            tx_hash: [0xab; 32],
            policy: 0,
            asset_name: b"SpaceBud9668".to_vec(),
            kind: EventKind::Sold,
            price_lovelace: Some(980_000_000),
            buyer_price_lovelace: Some(1_000_000_000),
            seller_stake: Some(0),
            buyer_stake: Some(1),
            marketplace: 0,
            venue: 0,
            bundle_size: Some(3),
            output_index: Some(1),
            fee_waived: false,
            slot: 142_837_465,
            block_height: Some(11_234_567),
            block_time: 1_753_142_400,
        }
    }

    fn sparse_row() -> EventRow {
        EventRow {
            tx_hash: [0x01; 32],
            policy: 0,
            asset_name: Vec::new(),
            kind: EventKind::OfferCancelled,
            price_lovelace: None,
            buyer_price_lovelace: None,
            seller_stake: None,
            buyer_stake: None,
            marketplace: 0,
            venue: 0,
            bundle_size: None,
            output_index: None,
            fee_waived: true,
            slot: 1,
            block_height: None,
            block_time: 1_596_059_091,
        }
    }

    #[test]
    fn round_trip() {
        let page = EventsPage {
            version: WIRE_VERSION,
            policies: vec![[0x1f; 28]],
            stakes: vec!["stake1uyyrtmamw".into(), "stake1u888ludr".into()],
            marketplaces: vec!["wayup".into()],
            venues: vec!["wayup".into()],
            events: vec![full_row(), sparse_row()],
            next_cursor: Some("142837465:9912034".into()),
        };
        let bytes = encode_events_page(&page).unwrap();
        assert_eq!(bytes[0], WIRE_VERSION);
        let decoded = decode_events_page(&bytes).unwrap();
        assert_eq!(decoded, page);
    }

    #[test]
    fn empty_page_round_trips() {
        let page = EventsPage::empty();
        let bytes = encode_events_page(&page).unwrap();
        assert_eq!(decode_events_page(&bytes).unwrap(), page);
    }

    #[test]
    fn version_mismatch_is_loud() {
        let mut bytes = encode_events_page(&EventsPage::empty()).unwrap();
        bytes[0] = 2;
        assert_eq!(
            decode_events_page(&bytes),
            Err(WireError::VersionMismatch { got: 2 })
        );
        assert_eq!(decode_events_page(&[]), Err(WireError::Empty));
    }

    /// Freezes the enum order contract: each kind's postcard tag is its
    /// declaration position. If this test fails, the wire format broke.
    #[test]
    fn event_kind_discriminants_pinned() {
        for (expected, kind) in EventKind::ALL.iter().enumerate() {
            let bytes = postcard::to_allocvec(kind).unwrap();
            assert_eq!(
                bytes,
                vec![expected as u8],
                "discriminant drift for {kind:?}"
            );
        }
    }

    #[test]
    fn kind_db_str_round_trips() {
        for kind in EventKind::ALL {
            assert_eq!(EventKind::from_db_str(kind.as_db_str()), Some(kind));
        }
        assert_eq!(EventKind::from_db_str("bogus"), None);
    }

    #[test]
    fn listings_page_round_trip() {
        let page = ListingsPage {
            version: WIRE_VERSION,
            policies: vec![[0x1f; 28]],
            sellers: vec!["stake1uyyrtmamw".into()],
            venues: vec!["wayup".into()],
            listings: vec![
                ListingRow {
                    policy: 0,
                    asset_name: b"Bud1".to_vec(),
                    price_lovelace: Some(30_000_000),
                    seller: Some(0),
                    venue: 0,
                    tx_hash: [0xab; 32],
                    output_index: 0,
                    listed_slot: 142_000_000,
                    listed_time: 1_750_000_000,
                },
                ListingRow {
                    policy: 0,
                    asset_name: b"Bud2".to_vec(),
                    price_lovelace: None, // unpriced jpg
                    seller: None,
                    venue: 0,
                    tx_hash: [0x01; 32],
                    output_index: 1,
                    listed_slot: 142_000_100,
                    listed_time: 1_750_000_100,
                },
            ],
            floor_lovelace: Some(30_000_000),
            count: 2,
            next_cursor: Some("30000000:5".into()),
        };
        let bytes = encode_listings_page(&page).unwrap();
        assert_eq!(bytes[0], WIRE_VERSION);
        assert_eq!(decode_listings_page(&bytes).unwrap(), page);
        assert_eq!(decode_listings_page(&[]), Err(WireError::Empty));
    }
}
