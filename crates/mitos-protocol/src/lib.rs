//! Wire-format types + selector machinery for the mitos
//! subscription model.
//!
//! This crate is deliberately **framework-free** — no dolos,
//! gasket, axum, tokio-tungstenite. CF Workers and other consumers
//! pull this in solely to deserialise protocol events and express
//! subscription `Interest`s. The indexer-runtime side
//! (`mitos-core`) re-exports everything here for the in-process
//! matching engine.
//!
//! See `../../docs/design/SUBSCRIPTION_MECHANICS.md` for the full
//! design rationale.

mod interest;
pub mod protocol;
pub mod subscribe;
pub mod wire;

#[cfg(test)]
mod interest_tests;

pub use interest::{
    AssetMovementSelector, AssetSelector, BurnSelector, DexSelector, DomainSelector, Interest,
    LendingSelector, MarketplaceSelector, MintSelector, ValueFilter, any_interest_matches_asset,
    any_interest_matches_asset_role, any_interest_matches_event, watched_policies,
};
pub use protocol::{
    Address, AssetMovementPayload, AssetRole, BorrowPayload, BurnPayload, Dex, DexBrand,
    DexEventKind, Domain, Lending, LendingBrand, LendingEventKind, ListingPayload, Lovelace,
    Marketplace, MarketplaceBrand, MarketplaceEventKind, MintPayload, MovementClaim,
    OfferAcceptPayload, OfferCancelPayload, OfferCreatePayload, OfferUpdatePayload, OutputRef,
    PlutusBytes, ProtocolEvent, SalePayload, SwapPayload, UnlistingPayload,
};
pub use subscribe::{
    DialBackOverride, SUBSCRIBE_MIME, SubscribeRequest, SubscribeResponse, SubscribeWireError,
};
pub use wire::{
    ChainPoint, ClientMessage, InterestOp, ServerMessage, SubscribeReply, decode_client,
    decode_server, encode_client, encode_server,
};
