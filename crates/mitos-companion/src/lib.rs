//! Mitos companion runtime SDK.
//!
//! See `mitos/docs/strategy/MITOS_COMPANION_RUNTIME_V1.md` for the full
//! design rationale. This crate ships:
//!
//! - [`MitosCompanion`] — top-level trait the dApp implements once per
//!   companion type. Owns the channel set, config, schema, RPC routes.
//! - [`MitosChannel`] — per-channel sub-trait. One impl per channel a
//!   companion subscribes to. Owns the typed `Event` and `apply_event`.
//! - [`MitosChannelDyn`] — object-safe erased view; the runtime uses
//!   this to dispatch by channel name. Blanket-impl'd for any
//!   [`MitosChannel`].
//! - [`MitosCompanionRuntime`] (later modules) — plain generic struct
//!   the dApp embeds inside its own `#[durable_object]` wrapper and
//!   forwards DO methods into.
//!
//! ## Quick start
//!
//! ```ignore
//! use mitos_companion::{MitosCompanion, MitosChannel, MitosChannelDyn, Ctx};
//!
//! struct OwnershipChannel;
//!
//! #[async_trait::async_trait(?Send)]
//! impl MitosChannel for OwnershipChannel {
//!     const NAME: &'static str = "ownership";
//!     type Event = OwnershipChange;
//!     async fn apply_event(&self, ctx: &Ctx, ev: OwnershipChange) -> Result<()> {
//!         // synchronous SQL exec block — no .await between statements
//!         Ok(())
//!     }
//! }
//!
//! struct OwnershipImpl;
//!
//! impl MitosCompanion for OwnershipImpl {
//!     const NAME: &'static str = "ownership-companion";
//!     type Config = OwnershipConfig;
//!     fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
//!         vec![Box::new(OwnershipChannel)]
//!     }
//! }
//! ```

pub mod ctx;
pub mod error;
pub mod interest;
pub mod meta;
pub mod runtime;
pub mod subscribe;
pub mod traits;
pub mod wire;

#[cfg(test)]
mod tests;

pub use ctx::{Ctx, SqlStorageValue};
pub use error::{CompanionError, Result};
pub use interest::{InterestRow, NO_CHANNEL, rows_to_interests, rows_to_interests_for_channel};
pub use meta::{RUNTIME_SCHEMA_VERSION, RegistrationCache};
pub use runtime::MitosCompanionRuntime;
pub use subscribe::{
    DialBackOverride, MITOS_AUTH_TOKEN_ENV, MITOS_HOST_URL_ENV, SubscribeRequest,
    SubscribeResponse, decode_subscribe, decode_subscribe_response, encode_subscribe,
};
pub use traits::{MitosChannel, MitosChannelDyn, MitosCompanion};
pub use wire::{
    ChainPoint, ClientMessage, Interest, InterestOp, ServerMessage, SubscribeReply, decode_client,
    decode_server, encode_client, encode_server,
};
