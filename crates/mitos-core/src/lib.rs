//! Core trait + dispatcher + domain wiring + CF replication for the
//! mitos framework.
//!
//! See `../../docs/design/INDEXER_TRAIT.md` for the contract.
//! See `../../docs/design/ARCHITECTURE.md` for why this exists.
//! See `../../docs/design/CF_REPLICATION.md` for the subscription
//! model behind the trait's associated `Scope` and `Change` types.

mod auth;
mod bundle;
mod coordinator;
mod dispatcher;
mod domain;
mod emitter;
mod handle;
pub mod helpers;
mod indexer;
mod replicate;
mod replicator;
mod transport;

#[cfg(test)]
mod tests;

pub use auth::AuthToken;
pub use bundle::{Bundle, print_config_summary};
pub use coordinator::TxClaimCoordinator;
pub use dispatcher::{run_dispatcher, run_synchronized_dispatcher};
pub use domain::{load_config, setup_domain, spawn_sync_pipeline};
pub use emitter::{EmittedRecord, Emitter};
pub use handle::{IndexerAdapter, IndexerHandle};
pub use indexer::{Indexer, SubscribeReply};
pub use replicate::{
    ClientMessage, ServerMessage, chain_point_from_wire, chain_point_to_wire, decode_client,
    decode_server, encode_client, encode_server, replicate_router, subscribe_reply_to_wire,
};
pub use replicator::{Replicator, Subscription, SubscriptionId};
pub use transport::{AxumWs, TungsteniteWs, WsTransport};

// Re-export the wire-format / selector types so framework-side
// consumers (indexers, the matching engine) get them via
// `mitos_core::*` without needing a direct `mitos-protocol` dep.
// Deserialise-only consumers (CF Workers) pull `mitos-protocol`
// directly.
pub use mitos_protocol::{
    AssetSelector, DexSelector, DomainSelector, Interest, LendingSelector, MarketplaceSelector,
    ValueFilter, protocol,
};

// Re-export the dolos types indexers need at the trait surface, so
// downstream crates only need to depend on `mitos-core`.
pub use dolos::adapters::DomainAdapter;
pub use dolos_core::{BlockHash, ChainPoint, Domain, TipEvent, TipSubscription};
