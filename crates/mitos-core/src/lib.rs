//! Core trait + dispatcher + domain wiring + CF replication for the
//! mitos framework.
//!
//! See `../../docs/design/INDEXER_TRAIT.md` for the contract.
//! See `../../docs/design/ARCHITECTURE.md` for why this exists.
//! See `../../docs/design/CF_REPLICATION.md` for the subscription
//! model behind the trait's associated `Scope` and `Change` types.

mod bundle;
mod dispatcher;
mod domain;
mod emitter;
mod handle;
mod indexer;
mod replicate;

pub use bundle::Bundle;
pub use dispatcher::run_dispatcher;
pub use domain::{load_config, setup_domain, spawn_sync_pipeline};
pub use emitter::{EmittedRecord, Emitter};
pub use handle::{IndexerAdapter, IndexerHandle};
pub use indexer::{Indexer, SubscribeReply};
pub use replicate::{
    ClientMessage, ServerMessage, decode_client, encode_server, replicate_router, send_server,
};

// Re-export the dolos types indexers need at the trait surface, so
// downstream crates only need to depend on `mitos-core`.
pub use dolos::adapters::DomainAdapter;
pub use dolos_core::{ChainPoint, Domain, TipEvent, TipSubscription};
