//! Core trait + dispatcher for the mitos framework.
//!
//! See `../../docs/design/INDEXER_TRAIT.md` for the contract.
//! See `../../docs/design/ARCHITECTURE.md` for why this exists.

mod dispatcher;
mod indexer;

pub use dispatcher::run_dispatcher;
pub use indexer::Indexer;

// Re-export the dolos types indexers need at the trait surface, so
// downstream crates only need to depend on `mitos-core`.
pub use dolos_core::{ChainPoint, Domain, TipEvent};
