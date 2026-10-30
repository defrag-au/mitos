//! A market-ledger page, translated into `chain-events`.
//!
//! The marketplace half of the Cardano input. `tools/market-ledger` already decodes
//! jpg.store and Wayup listings, sales and offers through `mitos-marketplace-decode`;
//! this module does the last step and nothing else — one [`ChainEvent`] per row, in the
//! `market` family.
//!
//! # What it refuses to decide
//!
//! - **A re-price is [`MarketEvent::Updated`], not a second `Placed`.** The wire keeps
//!   `price_change` apart from `listed`; the transition vocabulary keeps them apart for
//!   the same reason, so a consumer counting listings never has to notice that the same
//!   order id arrived twice.
//! - **An order reference only where the row names one.** A listing create or re-price
//!   carries its own output in its own transaction, so `tx:index` is a real UTxO. A sale,
//!   a delist or an offer acceptance *spends* an order and reports at most the spent
//!   output's index with the spending transaction's hash — two halves of different
//!   references. Joining them would fabricate an oref that never existed, so those rows
//!   carry `None` and a consumer joins on asset and time instead.
//! - **No price at all rather than a wrong one.** `price_lovelace` is absent for an
//!   in-kind settlement: the ledger keeps the real consideration as JSON in its own
//!   `price_detail` column, which this adapter does not parse. `None` is a stated
//!   absence; a zero would read as a free sale.
//! - **The transaction has no position.** The wire carries a hash and no index within the
//!   block, so [`TxRef`](chain_events::TxRef) is built with `0` — the same convention the
//!   OpenSea adapter uses. The position is a coordinate and is not hashed into
//!   [`EventId`](chain_events::EventId), so a row that does carry one can fill it in
//!   without rewriting an id.
//!
//! # Parties are stake addresses
//!
//! A market row's seller and buyer are **stake** addresses (`stake1…`), because that is
//! what the marketplace decode can read off a datum. The holder adapter's parties are
//! **payment** addresses (`addr1…`), because a movement names the address that held the
//! asset. The common key is the stake credential — `policy_archive::holders::key_of`
//! maps one to the other — so anything joining the two families must normalise first.
//! This is the one place the two halves of the same collection disagree about identity,
//! and it is a property of the chain rather than of either adapter.

use chain_events::{
    AccountRef, ChainCoord, ChainEvent, ChainRef, EventBody, EventSource, MarketEvent, MarketParty,
    OfferScope, Order, OrderRef, PartyRole, Price, Side, TxRef,
};
use market_ledger_wire::{EventKind, EventRow, EventsPage};

use crate::Collection;

/// ADA's scale. Every price this adapter can read is lovelace.
const ADA_DECIMALS: u8 = 6;

/// What one page produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketTranslated {
    /// One event per representable row.
    pub events: Vec<ChainEvent>,
    /// Rows whose asset name has no CAIP-19 identity — today only the empty name.
    pub unrepresentable: Vec<Vec<u8>>,
    /// Rows for a collection other than this one. A page serves whatever the caller
    /// asked for, and a caller that asks for two policies gets both.
    pub foreign: usize,
    /// Rows the page cannot resolve, or that state a fact the vocabulary cannot express.
    ///
    /// A page is written by one writer from one set of side tables, so an index that does
    /// not resolve is corruption rather than an ordinary miss; it is counted so it cannot
    /// disappear. A placed order with no price is the other case: the wire says it exists
    /// and does not say what it asks.
    pub malformed: usize,
}

impl Collection {
    /// Translates every row of `page` that belongs to this collection.
    ///
    /// `recorded_at` is the caller's clock; this crate does not read one.
    pub fn from_market_page(&self, page: &EventsPage, recorded_at: u64) -> MarketTranslated {
        let policy = self.policy().to_owned();
        let mut events = Vec::with_capacity(page.events.len());
        let mut unrepresentable = Vec::new();
        let mut foreign = 0usize;
        let mut malformed = 0usize;

        for row in &page.events {
            let ours = page
                .policies
                .get(row.policy as usize)
                .map(|bytes| hex::encode(bytes) == policy);
            match ours {
                Some(true) => {}
                Some(false) => {
                    foreign += 1;
                    continue;
                }
                // An index into the page's own side table that does not resolve.
                None => {
                    malformed += 1;
                    continue;
                }
            }

            let Ok(seller) = party(page, self.chain(), row.seller_stake) else {
                malformed += 1;
                continue;
            };
            let Ok(buyer) = party(page, self.chain(), row.buyer_stake) else {
                malformed += 1;
                continue;
            };

            let Some(asset) = self.asset(&row.asset_name) else {
                unrepresentable.push(row.asset_name.clone());
                continue;
            };

            let Some(body) = body(row, seller, buyer) else {
                malformed += 1;
                continue;
            };
            let discriminator = body.discriminator();
            let occurred_at = u64::try_from(row.block_time).unwrap_or_default();
            let tx = TxRef::new(hex::encode(row.tx_hash), 0);

            events.push(
                ChainEvent::draft(
                    self.chain(),
                    self.asset_type().clone(),
                    EventBody::Market(body),
                    occurred_at,
                    EventSource::MarketplaceDecode,
                )
                .asset(asset)
                .at(ChainCoord::new(row.slot))
                .tx(tx)
                .coverage(self.coverage())
                .build(&discriminator, recorded_at),
            );
        }

        MarketTranslated {
            events,
            unrepresentable,
            foreign,
            malformed,
        }
    }
}

/// The transition one row states, or `None` when it states one this vocabulary cannot
/// express.
fn body(
    row: &EventRow,
    seller: Option<AccountRef>,
    buyer: Option<AccountRef>,
) -> Option<MarketEvent> {
    let price = row
        .price_lovelace
        .map(|amount| Price::native(u128::from(amount), ADA_DECIMALS));
    let order = order_ref(row);

    Some(match row.kind {
        EventKind::Sold => MarketEvent::Filled {
            order: Order::Unknown,
            side: Side::Ask,
            price,
            parties: parties(seller, buyer),
        },
        EventKind::Listed => MarketEvent::Placed {
            order: order?,
            side: Side::Ask,
            price: price?,
            parties: parties(seller, None),
        },
        EventKind::PriceChange => MarketEvent::Updated {
            order: order?,
            price: price?,
        },
        EventKind::Delisted => MarketEvent::Cancelled {
            order: Order::Unknown,
            side: Some(Side::Ask),
            price,
            parties: parties(seller, None),
        },
        // Cardano's venues offer against one asset; there is no trait-scoped or
        // collection-wide *creation* to distinguish from an item offer.
        EventKind::OfferCreated => MarketEvent::Placed {
            order: order?,
            side: Side::Bid(OfferScope::Item),
            price: price?,
            parties: parties(None, buyer),
        },
        EventKind::OfferUpdated => MarketEvent::Updated {
            order: order?,
            price: price?,
        },
        EventKind::OfferAccepted => MarketEvent::Filled {
            order: Order::Unknown,
            side: Side::Bid(OfferScope::Item),
            price,
            parties: parties(seller, buyer),
        },
        // The scope is the only thing that tells the two acceptances apart, and the
        // ledger records it as a flag on one kind, so it is stated here rather than
        // inferred later.
        EventKind::CollectionOfferAccepted => MarketEvent::Filled {
            order: Order::Unknown,
            side: Side::Bid(OfferScope::Collection),
            price,
            parties: parties(seller, buyer),
        },
        EventKind::OfferCancelled => MarketEvent::Cancelled {
            order: Order::Unknown,
            side: Some(Side::Bid(OfferScope::Item)),
            price,
            parties: parties(None, buyer),
        },
    })
}

/// The parties a row names, in the order the role enum states them.
fn parties(seller: Option<AccountRef>, buyer: Option<AccountRef>) -> Vec<MarketParty> {
    let mut parties = Vec::with_capacity(2);
    if let Some(account) = seller {
        parties.push(MarketParty {
            role: PartyRole::Seller,
            account,
        });
    }
    if let Some(account) = buyer {
        parties.push(MarketParty {
            role: PartyRole::Buyer,
            account,
        });
    }
    parties
}

/// The order a row names, where it names one of its own.
///
/// Only a create or a re-price satisfies that: the row's transaction produced the order
/// UTxO it reports the index of. A spend reports the *prior* output index beside the
/// *spending* transaction's hash, which is not one reference — see the module docs.
fn order_ref(row: &EventRow) -> Option<OrderRef> {
    let names_its_own = matches!(
        row.kind,
        EventKind::Listed
            | EventKind::PriceChange
            | EventKind::OfferCreated
            | EventKind::OfferUpdated
    );
    if !names_its_own {
        return None;
    }
    let index = row.output_index?;
    Some(OrderRef::new(format!(
        "{}:{index}",
        hex::encode(row.tx_hash)
    )))
}

/// A stake address the page interns, or an error when the index does not resolve.
fn party(page: &EventsPage, chain: ChainRef, index: Option<u32>) -> Result<Option<AccountRef>, ()> {
    match index {
        None => Ok(None),
        Some(index) => page
            .stakes
            .get(index as usize)
            .map(|address| Some(AccountRef::new(chain, address.clone())))
            .ok_or(()),
    }
}

#[cfg(test)]
mod tests {
    //! Synthetic rows. These tests are about the mapping — which kind becomes which, and
    //! where a reference is real — not about any fact on chain.

    use super::*;
    use chain_events::Coverage;
    use std::str::FromStr;

    /// The Perps policy, so the tests name the collection this work is for.
    const POLICY: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";
    const ASSET: &[u8] = b"Perp";

    fn collection() -> Collection {
        Collection::new(
            ChainRef::from_str("cardano:mainnet").unwrap(),
            POLICY,
            Coverage::Complete,
        )
        .unwrap()
    }

    fn row(kind: EventKind, asset: &[u8]) -> EventRow {
        EventRow {
            tx_hash: [0xab; 32],
            policy: 0,
            asset_name: asset.to_vec(),
            kind,
            price_lovelace: Some(147_000_000),
            buyer_price_lovelace: Some(150_000_000),
            seller_stake: Some(0),
            buyer_stake: Some(1),
            marketplace: 0,
            venue: 0,
            bundle_size: None,
            output_index: None,
            fee_waived: false,
            slot: 170_445_182,
            block_height: Some(10_000_000),
            block_time: 1_700_000_000,
        }
    }

    fn page(rows: Vec<EventRow>) -> EventsPage {
        let mut page = EventsPage::empty();
        page.policies = vec![hex::decode(POLICY).unwrap().try_into().unwrap()];
        page.stakes = vec!["stake1seller".to_owned(), "stake1buyer".to_owned()];
        page.marketplaces = vec!["jpg".to_owned()];
        page.venues = vec!["jpg".to_owned()];
        page.events = rows;
        page
    }

    /// A listing that names its own output, and a sale that does not.
    fn listed(index: u32) -> EventRow {
        EventRow {
            output_index: Some(index),
            ..row(EventKind::Listed, ASSET)
        }
    }

    fn market(event: &ChainEvent) -> &MarketEvent {
        let EventBody::Market(fact) = &event.body else {
            panic!("expected a market fact");
        };
        fact
    }

    #[test]
    fn a_sale_is_a_fill_with_both_sides_and_its_price() {
        let translated = collection().from_market_page(&page(vec![row(EventKind::Sold, ASSET)]), 7);

        assert!(translated.unrepresentable.is_empty());
        assert_eq!(translated.events.len(), 1);

        let event = &translated.events[0];
        assert_eq!(event.collection.reference(), POLICY);
        assert_eq!(
            event.asset.as_ref().and_then(|asset| asset.token_id()),
            Some("50657270")
        );
        assert_eq!(event.coord, Some(ChainCoord::new(170_445_182)));
        assert_eq!(event.occurred_at, 1_700_000_000);
        assert_eq!(event.source, EventSource::MarketplaceDecode);
        assert_eq!(event.coverage, Coverage::Complete);
        assert_eq!(event.recorded_at, 7);

        let MarketEvent::Filled {
            side,
            price,
            parties,
            ..
        } = market(event)
        else {
            panic!("expected a fill");
        };
        assert_eq!(*side, Side::Ask);
        assert_eq!(*price, Some(Price::native(147_000_000, ADA_DECIMALS)));
        assert_eq!(
            parties
                .iter()
                .map(|party| (party.role, party.account.address.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (PartyRole::Seller, "stake1seller"),
                (PartyRole::Buyer, "stake1buyer"),
            ]
        );
    }

    #[test]
    fn a_listing_names_its_order_and_a_sale_does_not() {
        let translated =
            collection().from_market_page(&page(vec![listed(3), row(EventKind::Sold, ASSET)]), 1);

        let MarketEvent::Placed { order, side, .. } = market(&translated.events[0]) else {
            panic!("expected a placement");
        };
        assert_eq!(*side, Side::Ask);
        assert_eq!(order.id, format!("{}:3", "ab".repeat(32)));

        // The sale reports the LISTING's output index beside the SPENDING transaction's
        // hash. Joining them would name a UTxO that never existed. `Unknown` here means
        // the source cannot name the order — never that a sale happened without one.
        let MarketEvent::Filled { order, .. } = market(&translated.events[1]) else {
            panic!("expected a fill");
        };
        assert!(matches!(order, Order::Unknown));
    }

    #[test]
    fn a_re_price_is_an_update_and_not_a_second_listing() {
        let translated = collection().from_market_page(
            &page(vec![
                listed(3),
                EventRow {
                    tx_hash: [0xcd; 32],
                    output_index: Some(4),
                    ..row(EventKind::PriceChange, ASSET)
                },
            ]),
            1,
        );

        let MarketEvent::Updated { order, price } = market(&translated.events[1]) else {
            panic!("expected a re-price");
        };
        assert_eq!(price.amount, 147_000_000);
        assert_eq!(
            order.id,
            format!("{}:4", "cd".repeat(32)),
            "a re-price names the order it changed"
        );
        // The distinction the old vocabulary could not make: counting placements does
        // not count this.
        assert_eq!(market(&translated.events[0]).kind(), "placed");
        assert_eq!(market(&translated.events[1]).kind(), "updated");
    }

    #[test]
    fn a_collection_offer_acceptance_states_its_scope() {
        let translated = collection().from_market_page(
            &page(vec![
                row(EventKind::OfferAccepted, ASSET),
                row(EventKind::CollectionOfferAccepted, ASSET),
            ]),
            1,
        );

        let sides: Vec<_> = translated
            .events
            .iter()
            .map(|event| {
                let MarketEvent::Filled { side, .. } = market(event) else {
                    panic!("expected a fill");
                };
                *side
            })
            .collect();
        assert_eq!(
            sides,
            vec![
                Side::Bid(OfferScope::Item),
                Side::Bid(OfferScope::Collection)
            ]
        );
    }

    #[test]
    fn an_offer_acceptance_does_not_fabricate_an_order_reference() {
        let accepted = EventRow {
            output_index: Some(9),
            ..row(EventKind::OfferAccepted, ASSET)
        };
        let translated = collection().from_market_page(&page(vec![accepted]), 1);

        let MarketEvent::Filled { order, .. } = market(&translated.events[0]) else {
            panic!("expected a fill");
        };
        assert!(matches!(order, Order::Unknown));
    }

    #[test]
    fn an_in_kind_settlement_carries_no_price_rather_than_zero() {
        let swap = EventRow {
            price_lovelace: None,
            buyer_price_lovelace: None,
            ..row(EventKind::Sold, ASSET)
        };
        let translated = collection().from_market_page(&page(vec![swap]), 1);

        let MarketEvent::Filled { price, .. } = market(&translated.events[0]) else {
            panic!("expected a fill");
        };
        assert_eq!(*price, None);
    }

    #[test]
    fn a_placed_order_with_no_price_is_reported_rather_than_given_one() {
        // The vocabulary cannot state a listing that asks nothing, so the row is counted
        // rather than translated with an invented zero.
        let unpriced = EventRow {
            price_lovelace: None,
            ..listed(1)
        };
        let translated = collection().from_market_page(&page(vec![unpriced]), 1);

        assert!(translated.events.is_empty());
        assert_eq!(translated.malformed, 1);
    }

    #[test]
    fn a_placement_that_cannot_name_its_order_is_reported_rather_than_stored() {
        // A placement IS an order, so one the row cannot name cannot be applied to a
        // book. The wire always carries the output index; a row without one is corrupt.
        let mut unnameable = listed(1);
        unnameable.output_index = None;
        let translated = collection().from_market_page(&page(vec![unnameable]), 1);

        assert!(translated.events.is_empty());
        assert_eq!(translated.malformed, 1);
    }

    #[test]
    fn a_withdrawal_states_its_side_and_never_a_settlement() {
        let translated = collection().from_market_page(
            &page(vec![
                row(EventKind::Delisted, ASSET),
                row(EventKind::OfferCancelled, ASSET),
            ]),
            1,
        );

        let sides: Vec<_> = translated
            .events
            .iter()
            .map(|event| {
                let MarketEvent::Cancelled { side, .. } = market(event) else {
                    panic!("expected a cancellation");
                };
                *side
            })
            .collect();
        assert_eq!(
            sides,
            vec![Some(Side::Ask), Some(Side::Bid(OfferScope::Item))]
        );
    }

    #[test]
    fn the_root_token_has_no_representable_identity_and_is_reported() {
        let translated = collection().from_market_page(&page(vec![row(EventKind::Sold, b"")]), 1);

        assert_eq!(translated.unrepresentable, vec![Vec::<u8>::new()]);
        assert!(translated.events.is_empty());
    }

    #[test]
    fn a_row_for_another_policy_is_counted_rather_than_translated() {
        let mut page = page(vec![row(EventKind::Sold, ASSET)]);
        page.policies[0] = [0xef; 28];

        let translated = collection().from_market_page(&page, 1);

        assert!(translated.events.is_empty());
        assert_eq!(translated.foreign, 1);
    }

    #[test]
    fn an_index_that_does_not_resolve_is_counted_rather_than_dropped() {
        let mut page = page(vec![row(EventKind::Sold, ASSET)]);
        page.stakes.clear();

        let translated = collection().from_market_page(&page, 1);

        assert!(translated.events.is_empty());
        assert_eq!(translated.malformed, 1);
    }

    #[test]
    fn the_same_page_translates_to_the_same_ids_twice() {
        let page = page(vec![row(EventKind::Sold, ASSET), listed(3)]);

        let first = collection().from_market_page(&page, 1_000);
        let second = collection().from_market_page(&page, 2_000);

        assert_eq!(first.events.len(), 2);
        // The id is the fact's, not the recording's: a second replay with a different
        // clock upserts the same row rather than adding another.
        assert_eq!(first.events[0].id, second.events[0].id);
        assert_eq!(first.events[1].id, second.events[1].id);
        assert_ne!(first.events[0].recorded_at, second.events[0].recorded_at);
    }

    #[test]
    fn two_sales_of_one_asset_in_one_transaction_are_two_events() {
        // A batch: same chain, same transaction, same asset, same kind, same price. Only
        // the parties keep them apart, and without them the store would upsert one sale
        // over the other.
        let mut page = page(vec![row(EventKind::Sold, ASSET)]);
        page.stakes.push("stake1second".to_owned());
        let mut second = page.events[0].clone();
        second.buyer_stake = Some(2);
        page.events.push(second);

        let translated = collection().from_market_page(&page, 1);

        assert_eq!(translated.events.len(), 2);
        assert_ne!(translated.events[0].id, translated.events[1].id);
    }
}
