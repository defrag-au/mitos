//! Trap supervision — catch traps, consult the module's
//! declared `trap-policy`, decide what to do.
//!
//! See `MITOS_PLATFORM_V1.md` §"Resolved design questions" #4
//! for the rationale: module authors know better than the host
//! whether a given indexer can safely replay, must skip, or has
//! to halt for human review. The host enforces a small enum of
//! strategies.

// `RetryPolicy` and `TrapStrategy` come from the world-level
// `use types.{...}` and live at the bindgen module's top-level.
use crate::bindings_v2::{RetryPolicy, TrapStrategy};

/// Outcome of one dispatch attempt.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SupervisorOutcome {
    /// Dispatch succeeded; advance the cursor.
    Ok,
    /// Dispatch trapped, retry budget remains. Caller should
    /// wait `backoff_ms` then retry the same block.
    Retry { backoff_ms: u32 },
    /// Strategy `Replay`: retry budget exhausted, restart the
    /// instance, replay the block.
    RestartAndReplay,
    /// Strategy `SkipAndMark`: log, advance cursor past the
    /// failing block, continue.
    SkipAndContinue,
    /// Strategy `Quarantine`: stop the module, do not advance
    /// the cursor, surface to the operator.
    Quarantine,
}

/// Per-instance supervisor state. Tracks consecutive-failure
/// counter, applies the module's declared policy.
pub struct Supervisor {
    strategy: TrapStrategy,
    retry: RetryPolicy,
    consecutive_failures: u32,
}

impl Supervisor {
    pub fn new(strategy: TrapStrategy, retry: RetryPolicy) -> Self {
        Self {
            strategy,
            retry,
            consecutive_failures: 0,
        }
    }

    /// Record a successful dispatch; reset the failure counter.
    pub fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    /// Record a trap; decide the outcome.
    pub fn record_trap(&mut self) -> SupervisorOutcome {
        self.consecutive_failures += 1;

        if self.consecutive_failures < self.retry.max_retries {
            let backoff = self.compute_backoff_ms();
            return SupervisorOutcome::Retry {
                backoff_ms: backoff,
            };
        }

        match self.strategy {
            TrapStrategy::Replay => SupervisorOutcome::RestartAndReplay,
            TrapStrategy::SkipAndMark => SupervisorOutcome::SkipAndContinue,
            TrapStrategy::Quarantine => SupervisorOutcome::Quarantine,
        }
    }

    fn compute_backoff_ms(&self) -> u32 {
        // Exponential backoff capped at retry.backoff_cap_ms.
        let base = 50u32;
        let exp = base.saturating_mul(1u32 << self.consecutive_failures.min(10));
        exp.min(self.retry.backoff_cap_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retry(max: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries: max,
            backoff_cap_ms: 1_000,
        }
    }

    #[test]
    fn first_traps_within_budget_yield_retry() {
        let mut sup = Supervisor::new(TrapStrategy::Replay, retry(3));
        assert!(matches!(sup.record_trap(), SupervisorOutcome::Retry { .. }));
        assert!(matches!(sup.record_trap(), SupervisorOutcome::Retry { .. }));
    }

    #[test]
    fn replay_strategy_falls_through_after_budget() {
        let mut sup = Supervisor::new(TrapStrategy::Replay, retry(2));
        let _ = sup.record_trap(); // 1
        assert_eq!(sup.record_trap(), SupervisorOutcome::RestartAndReplay); // 2 → fallthrough
    }

    #[test]
    fn skip_strategy_advances_after_budget() {
        let mut sup = Supervisor::new(TrapStrategy::SkipAndMark, retry(1));
        assert_eq!(sup.record_trap(), SupervisorOutcome::SkipAndContinue);
    }

    #[test]
    fn quarantine_strategy_halts_after_budget() {
        let mut sup = Supervisor::new(TrapStrategy::Quarantine, retry(1));
        assert_eq!(sup.record_trap(), SupervisorOutcome::Quarantine);
    }

    #[test]
    fn success_resets_failure_counter() {
        let mut sup = Supervisor::new(TrapStrategy::Replay, retry(3));
        let _ = sup.record_trap();
        let _ = sup.record_trap();
        sup.record_success();
        // First post-success trap should still be a Retry, not a
        // fall-through — counter reset confirmed.
        assert!(matches!(sup.record_trap(), SupervisorOutcome::Retry { .. }));
    }

    #[test]
    fn backoff_caps() {
        let mut sup = Supervisor::new(TrapStrategy::Replay, retry(20));
        for _ in 0..10 {
            if let SupervisorOutcome::Retry { backoff_ms } = sup.record_trap() {
                assert!(backoff_ms <= 1_000);
            }
        }
    }
}
