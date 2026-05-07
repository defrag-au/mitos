//! v2 `logging` interface — same shape as v1, plumbed against
//! v2 bindings.

use crate::bindings_v2::{LogLevel, LoggingHost};
use crate::host_fns_v2::HostStateV2;

impl LoggingHost for HostStateV2 {
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
