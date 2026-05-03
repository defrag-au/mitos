//! `logging` interface — funnels module logs into the host's
//! `tracing` subscriber.
//!
//! Module-side `log(level, target, message)` lands as a
//! `tracing::event!` host-side, with `module_id` and `target`
//! attached as fields so log queries can filter to a single
//! module.

use crate::bindings::{LogLevel, LoggingHost};
use crate::host_fns::HostState;

impl LoggingHost for HostState {
    async fn log(
        &mut self,
        level: LogLevel,
        target: String,
        message: String,
    ) -> wasmtime::Result<()> {
        let module = &self.module_id;
        match level {
            LogLevel::Trace => {
                tracing::trace!(target: "mitos_module", module, target = %target, "{message}")
            }
            LogLevel::Debug => {
                tracing::debug!(target: "mitos_module", module, target = %target, "{message}")
            }
            LogLevel::Info => {
                tracing::info!(target: "mitos_module", module, target = %target, "{message}")
            }
            LogLevel::Warn => {
                tracing::warn!(target: "mitos_module", module, target = %target, "{message}")
            }
            LogLevel::Error => {
                tracing::error!(target: "mitos_module", module, target = %target, "{message}")
            }
        }
        Ok(())
    }
}
