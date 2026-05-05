use thiserror::Error;

/// Errors surfaced by the companion runtime.
///
/// `apply_event` errors from the dApp's [`crate::MitosChannel`] impl
/// are wrapped in [`CompanionError::Apply`] and surfaced upstream as
/// `ClientMessage::Nack` frames; the host records them in
/// `module_emissions` with status `nacked`. See the design doc's
/// "Emissions log on the host" section.
#[derive(Debug, Error)]
pub enum CompanionError {
    /// dApp's `apply_event` returned `Err`. The runtime catches and
    /// surfaces these as Nack frames; cursor still advances.
    #[error("apply_event failed for channel {channel}: {source}")]
    Apply {
        channel: &'static str,
        #[source]
        source: anyhow::Error,
    },

    /// CBOR decode of an event payload failed. Indicates a wire-shape
    /// mismatch between the host's emit type and the channel's
    /// `Event` type. Surface as Nack so the host can flag operator.
    #[error("CBOR decode failed for channel {channel}: {message}")]
    Decode {
        channel: &'static str,
        message: String,
    },

    /// A frame referenced a channel name not registered in this
    /// companion's `channels()` vec. Indicates host/companion
    /// version drift.
    #[error("unknown channel: {0}")]
    UnknownChannel(String),

    /// SQL exec failed inside the runtime's cursor or interest
    /// helpers. Distinct from `Apply` because the dApp's code wasn't
    /// the source.
    #[error("storage error: {0}")]
    Storage(String),

    /// Wire-protocol decode error for an inbound `ServerMessage`
    /// frame. Surfaces a host-side bug or version skew.
    #[error("wire protocol decode error: {0}")]
    Wire(String),
}

pub type Result<T, E = CompanionError> = std::result::Result<T, E>;

/// Bridge to worker-rs's Error type so DO method bodies can use
/// `?` to propagate companion errors back through the worker
/// runtime. The conversion stringifies; structure is preserved
/// in `CompanionError` for callers that catch before this bridge.
impl From<CompanionError> for worker::Error {
    fn from(err: CompanionError) -> Self {
        worker::Error::from(err.to_string())
    }
}
