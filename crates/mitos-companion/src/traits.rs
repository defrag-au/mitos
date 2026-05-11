//! Trait surface — `MitosCompanion` (top-level) + `MitosChannel`
//! (per-channel) + `MitosChannelDyn` (object-safe erased).
//!
//! See the design doc's "Trait shape" section. The split exists
//! because:
//! - Each channel has its own typed `Event` (struct, enum, tuple,
//!   anything `DeserializeOwned`); strong typing per channel
//!   catches drift at compile time.
//! - `MitosCompanion::channels()` returns
//!   `Vec<Box<dyn MitosChannelDyn>>`; the dyn-trait flavour erases
//!   the per-channel `Event` so a single Vec can hold heterogeneous
//!   channels.
//! - `MitosChannelDyn` is blanket-impl'd for any `MitosChannel`,
//!   so dApps never write it themselves.

use crate::ctx::Ctx;
use crate::error::Result;
use crate::wire::ChainPoint;
use mitos_protocol::SubscribeTarget;

/// Top-level companion trait. Implemented once per dApp companion.
/// Owns the channel set, config, schema, and dApp RPC routes.
///
/// The runtime fans inbound mitos events out to the right channel
/// by tag.
pub trait MitosCompanion: Send + Sync + 'static {
    /// Stable name (matches the indexer's `name()` on the mitos
    /// side). Used for routing, logging, schema isolation.
    const NAME: &'static str;

    /// Per-companion config — typically initial interest set, auth
    /// tokens, etc. Loaded from DO storage on first request.
    type Config: serde::de::DeserializeOwned + Default + Send;

    /// Channels this companion subscribes to. Most companions have
    /// one; multi-channel companions return several. Each channel
    /// decodes events into its own typed `Event`.
    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>>;

    /// SQLite schema migration. Default: no-op (the runtime always
    /// creates `mitos_companion_meta`, `mitos_companion_interest`,
    /// and the registration cache row itself).
    fn migrate(&self) -> Result<()> {
        Ok(())
    }

    /// What this companion subscribes to. Default: one
    /// `SubscribeTarget::Module` with name = `Self::NAME` — i.e.
    /// classic single-wasm-module companions Just Work without
    /// overriding.
    ///
    /// Override to declare:
    ///
    /// - **Indexer target** instead of module (subscribe to an
    ///   in-tree indexer via the unified-subscribe bridge — see
    ///   `docs/design/UNIFIED_SUBSCRIBE.md`):
    ///   ```ignore
    ///   fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
    ///       vec![SubscribeTarget::Indexer { name: "marketplace".into() }]
    ///   }
    ///   ```
    /// - **Multi-target** (a single companion subscribing to
    ///   several sources — e.g. one wasm module + one in-tree
    ///   indexer):
    ///   ```ignore
    ///   fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
    ///       vec![
    ///           SubscribeTarget::Module { name: "jpg-co".into() },
    ///           SubscribeTarget::Indexer { name: "marketplace".into() },
    ///       ]
    ///   }
    ///   ```
    ///   The host opens one dial-back WS per target (per
    ///   UNIFIED_SUBSCRIBE.md's v1 multi-target shape), each
    ///   landing at `/_internal/replicate-<target_name>` on the
    ///   companion. Channel routing matches by target name.
    fn subscribe_targets(&self) -> Vec<SubscribeTarget> {
        vec![SubscribeTarget::Module {
            name: Self::NAME.to_string(),
        }]
    }
}

/// Per-channel handler. Implemented once per channel a companion
/// subscribes to. Each channel owns its own typed `Event`.
///
/// `?Send` is used on the trait method because worker-rs's DO
/// futures are single-threaded (wasm); requiring `Send` on user
/// code would be a footgun.
#[async_trait::async_trait(?Send)]
pub trait MitosChannel: 'static {
    /// Stable channel name. Matches the host-side indexer channel
    /// + the WS Hibernation tag the runtime sets.
    const NAME: &'static str;

    /// Wire shape for events on this channel. Sourced from
    /// `mitos-protocol` (or another shared crate) so there's no
    /// mirror-types drift.
    type Event: serde::de::DeserializeOwned;

    /// Per-event hook. Called inside the DO's output gate window;
    /// the dApp does any `.await` IO first, then performs all SQL
    /// writes synchronously via `ctx.exec(...)`. The runtime
    /// appends a synchronous cursor advance after this returns.
    ///
    /// Returning `Err` causes the runtime to:
    /// 1. Advance the cursor anyway (so streaming continues).
    /// 2. Send `ClientMessage::Nack { emission_id, error }` upstream.
    ///
    /// The dApp must therefore write `apply_event` so that retrying
    /// from the un-advanced cursor (or via host-driven replay)
    /// either re-converges to the same state (idempotent) or
    /// repairs the partial write.
    async fn apply_event(&self, ctx: &Ctx, event: Self::Event) -> Result<()>;

    /// Optional: undo hook for chain reorgs. Default: log warn.
    async fn undo(&self, _ctx: &Ctx, point: ChainPoint) -> Result<()> {
        tracing::warn!(?point, channel = Self::NAME, "undo no-op");
        Ok(())
    }
}

/// Object-safe erased view. The runtime uses this internally to
/// dispatch by channel name — `MitosCompanion::channels()` returns
/// a `Vec<Box<dyn MitosChannelDyn>>`, and the runtime walks it to
/// look up the right channel for each Apply frame.
///
/// Blanket-impl'd for any `MitosChannel`; dApps never write this
/// themselves.
#[async_trait::async_trait(?Send)]
pub trait MitosChannelDyn: 'static {
    /// Stable channel name (matches `MitosChannel::NAME`).
    fn name(&self) -> &'static str;

    /// CBOR-decode the payload bytes into the channel's `Event`
    /// type, then dispatch to `apply_event`. Surfaces decode
    /// failures as [`crate::error::CompanionError::Decode`] —
    /// caller (the runtime) translates that to a `Nack` frame.
    async fn apply_bytes(&self, ctx: &Ctx, bytes: &[u8]) -> Result<()>;

    /// Forward to `MitosChannel::undo`.
    async fn undo(&self, ctx: &Ctx, point: ChainPoint) -> Result<()>;
}

#[async_trait::async_trait(?Send)]
impl<C: MitosChannel> MitosChannelDyn for C {
    fn name(&self) -> &'static str {
        C::NAME
    }

    async fn apply_bytes(&self, ctx: &Ctx, bytes: &[u8]) -> Result<()> {
        let event: C::Event =
            ciborium::de::from_reader(bytes).map_err(|e| crate::error::CompanionError::Decode {
                channel: C::NAME,
                message: e.to_string(),
            })?;
        self.apply_event(ctx, event).await
    }

    async fn undo(&self, ctx: &Ctx, point: ChainPoint) -> Result<()> {
        MitosChannel::undo(self, ctx, point).await
    }
}
