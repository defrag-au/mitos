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

    /// How this kind reads with a count in front of it — `13 offers created`.
    ///
    /// # Why the enum owns this
    ///
    /// English puts the plural on the NOUN, and in these labels the noun is
    /// not the last word: `offer_created` pluralises to `offers created`, not
    /// `offer createds`. Nothing derivable from the slug gets that right, and
    /// a caller de-slugging with a naive `+s` produced `13 offer created` on a
    /// live card.
    ///
    /// Several kinds are invariant. `sold`, `listed` and `delisted` are past
    /// participles standing in for "13 [items] sold" — they take no plural at
    /// all, and adding one would be worse than the bug this fixes.
    ///
    /// Both forms are static, so counting costs no allocation.
    pub fn counted_label(&self, count: u64) -> &'static str {
        let one = count == 1;
        match (self, one) {
            // Participles: "13 sold" is already correct.
            (EventKind::Sold, _) => "sold",
            (EventKind::Listed, _) => "listed",
            (EventKind::Delisted, _) => "delisted",

            (EventKind::PriceChange, true) => "price change",
            (EventKind::PriceChange, false) => "price changes",
            (EventKind::OfferAccepted, true) => "offer accepted",
            (EventKind::OfferAccepted, false) => "offers accepted",
            (EventKind::CollectionOfferAccepted, true) => "collection offer accepted",
            (EventKind::CollectionOfferAccepted, false) => "collection offers accepted",
            (EventKind::OfferCreated, true) => "offer created",
            (EventKind::OfferCreated, false) => "offers created",
            (EventKind::OfferUpdated, true) => "offer updated",
            (EventKind::OfferUpdated, false) => "offers updated",
            (EventKind::OfferCancelled, true) => "offer cancelled",
            (EventKind::OfferCancelled, false) => "offers cancelled",
        }
    }

    /// Did an asset actually change hands, and money with it?
    ///
    /// # Why this distinction has a name
    ///
    /// Only three of these nine kinds are a trade. The other six are
    /// INTENTIONS — an asking price posted, changed, or withdrawn; an offer
    /// made or pulled — and their price is what somebody *wanted*, not what
    /// anybody paid.
    ///
    /// Conflating them misreads badly in exactly one direction: a listing at
    /// 813 ₳ presented like a settlement says a wallet received 813 ₳ when it
    /// received nothing at all. Anything that headlines a price, colours a
    /// figure as income, or totals a wallet's activity has to ask this first.
    ///
    /// A `PriceChange` is not a settlement even though it carries two prices;
    /// a `Delisted` is not one even though it ends a listing that had one.
    pub fn is_settlement(&self) -> bool {
        match self {
            EventKind::Sold | EventKind::OfferAccepted | EventKind::CollectionOfferAccepted => true,
            EventKind::Listed
            | EventKind::PriceChange
            | EventKind::Delisted
            | EventKind::OfferCreated
            | EventKind::OfferUpdated
            | EventKind::OfferCancelled => false,
        }
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

    /// `13 offer created` shipped on a live card. The plural goes on the
    /// NOUN, which in these labels is not the last word.
    #[test]
    fn a_count_pluralises_the_noun_not_the_participle() {
        use EventKind::*;
        assert_eq!(OfferCreated.counted_label(13), "offers created");
        assert_eq!(OfferCreated.counted_label(1), "offer created");
        assert_eq!(OfferAccepted.counted_label(2), "offers accepted");
        assert_eq!(
            CollectionOfferAccepted.counted_label(3),
            "collection offers accepted"
        );
        assert_eq!(PriceChange.counted_label(4), "price changes");
    }

    /// `13 sold` is already correct — these are participles standing in for
    /// "13 [items] sold", and pluralising them would be worse than the bug.
    #[test]
    fn participle_kinds_take_no_plural() {
        use EventKind::*;
        for kind in [Sold, Listed, Delisted] {
            assert_eq!(
                kind.counted_label(1),
                kind.counted_label(13),
                "{kind:?} should read the same at any count"
            );
        }
    }

    /// Every kind has words at every count, and none is left as a raw slug —
    /// an underscore reaching a card means a kind was added without being
    /// given any.
    #[test]
    fn every_kind_reads_as_english_at_any_count() {
        for kind in EventKind::ALL {
            for n in [0u64, 1, 2, 13] {
                let label = kind.counted_label(n);
                assert!(!label.contains('_'), "{kind:?} at {n}: {label}");
                assert!(!label.is_empty(), "{kind:?} at {n}");
            }
            // Zero reads like many; only ONE is special.
            assert_eq!(kind.counted_label(0), kind.counted_label(2), "{kind:?}");
        }
    }

    /// EXACTLY THREE KINDS ARE A TRADE. Written out in full rather than as a
    /// count, so a kind added later has to be classified deliberately here
    /// instead of falling into whichever arm the author reached for — and the
    /// direction that matters is a non-settlement wrongly reading as one,
    /// which is how an asking price becomes reported income.
    #[test]
    fn only_a_trade_is_a_settlement() {
        use EventKind::*;
        for kind in [Sold, OfferAccepted, CollectionOfferAccepted] {
            assert!(kind.is_settlement(), "{kind:?} moves an asset for money");
        }
        for kind in [
            Listed,
            PriceChange,
            Delisted,
            OfferCreated,
            OfferUpdated,
            OfferCancelled,
        ] {
            assert!(
                !kind.is_settlement(),
                "{kind:?} is an intention, not a trade — its price is what \
                 somebody wanted, not what anybody paid"
            );
        }
        // And the two sets together are the whole enum, so nothing added later
        // can go unclassified.
        assert_eq!(
            EventKind::ALL.iter().filter(|k| k.is_settlement()).count()
                + EventKind::ALL.iter().filter(|k| !k.is_settlement()).count(),
            EventKind::ALL.len()
        );
        assert_eq!(
            EventKind::ALL.iter().filter(|k| k.is_settlement()).count(),
            3
        );
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
