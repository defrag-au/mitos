//! Typed query / response surface for the data plane.
//!
//! Single-file-per-concern layout: each conceptual type cluster
//! lives in its own module and re-exports here. Strong-typed
//! discriminated unions (not `bytes` or `String` + runtime
//! validation) are the defining property — the data plane API
//! exists in part to give the rest of the framework typed
//! contracts that the compiler enforces.

mod asset_state;
mod chain_point;
mod decode;
mod error;
mod event;
mod interest;
mod output;
mod output_ref;
mod page;
mod params;
mod predicate;
mod tip;
mod tx_record;

pub use asset_state::{AssetMintState, Cip25Resolution};
pub use chain_point::ChainPoint;
pub use decode::DecodeLevel;
pub use error::{DataPlaneError, DataPlaneResult};
pub use event::{
    ConsumedEvent, DispatchEvent, MintedEvent, ProducedEvent, ReferencedEvent, RollbackEvent,
    TickEvent, TxContextEvent, TxEventBatch, UtxoEvent, ValidityInterval,
};
pub use interest::{InterestPredicate, InterestSet, StakeCred};
pub use output::{AssetEntry, Resolution, ScriptLanguage, TypedDatum, TypedOutput, TypedScript};
pub use output_ref::OutputRef;
pub use page::{Page, PageRequest};
pub use params::ProtocolParameters;
pub use predicate::{AddressPattern, AssetPattern, OutputRefPattern, UtxoPattern, UtxoPredicate};
pub use tip::ChainTip;
pub use tx_record::{ConsumedInput, MintEntry, ReferencedInput, TxRecord};
