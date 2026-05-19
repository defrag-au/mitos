//! Per-instance resource-budget observability.
//!
//! Phase 1 of `docs/design/WASM_BUDGET_CHUNKING.md`: a wasmtime
//! `ResourceLimiter` that records peak linear-memory use and
//! flags a denied/failed `memory.grow`, plus `TrapClass` — the
//! classification a heavy-op driver (recapture's `rebootstrap`
//! loop, module `init`) surfaces instead of an opaque "trapped."
//!
//! Why this exists: **fuel exhaustion is already a distinct,
//! meaningful trap (`OutOfFuel`); an OOM is not.** A failed
//! `memory.grow` flows through `cabi_realloc` → the Rust
//! allocator → an `unreachable` trap, *indistinguishable* from a
//! genuine `panic!` or logic bug on the trap code alone. The
//! limiter is the missing tell: it sees the failed grow, sets an
//! OOM flag, and `TrapClass::classify` reads that flag to
//! disambiguate.
//!
//! Host-only. No WIT or guest-module changes — Phase 1 is purely
//! observational; the limiter does not tighten any budget unless
//! a `max_memory_bytes` ceiling is explicitly configured (the
//! default is `None`, leaving the module's own declared maximum
//! as the only ceiling).

use wasmtime::{ResourceLimiter, Trap};

/// Linear-memory telemetry for one wasm `Store`. Lives on
/// `HostStateV2`; wasmtime calls it back on every `memory.grow`.
/// The host reads it after a guest call (via `Store::data`) to
/// classify a trap and feed adaptive page sizing (Phase 2).
#[derive(Debug, Default)]
pub struct BudgetLimiter {
    /// Optional host-imposed linear-memory ceiling, in bytes. A
    /// grow whose `desired` exceeds it is denied (`memory.grow`
    /// returns `-1`), producing a clean, classified OOM at a
    /// known point rather than an unbounded allocator abort.
    /// `None` (the default) is observational-only — Phase 1 does
    /// not tighten the budget.
    max_memory_bytes: Option<usize>,

    /// High-water mark of linear memory across the instance's
    /// lifetime, in bytes. Survives `reset_call` — it is a
    /// lifetime statistic, not a per-call one.
    peak_memory_bytes: usize,

    /// Set when a `memory.grow` was denied by `max_memory_bytes`
    /// or failed in the wasmtime allocator. A trap observed while
    /// this is set is a definite OOM, not a module logic bug.
    /// Per-call — cleared by `reset_call` before each invocation
    /// the host intends to classify.
    oom: bool,
}

impl BudgetLimiter {
    /// Build a limiter. `max_memory_bytes = None` leaves the
    /// module's declared maximum as the only ceiling (Phase 1
    /// default).
    pub fn new(max_memory_bytes: Option<usize>) -> Self {
        Self {
            max_memory_bytes,
            peak_memory_bytes: 0,
            oom: false,
        }
    }

    /// Lifetime peak linear-memory use, in bytes.
    pub fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes
    }

    /// Whether a `memory.grow` was denied or failed since the
    /// last `reset_call`. The OOM tell `TrapClass::classify`
    /// consults.
    pub fn hit_oom(&self) -> bool {
        self.oom
    }

    /// Clear the per-call OOM flag before a fresh guest call the
    /// host intends to classify. `peak_memory_bytes` is
    /// intentionally *not* reset — it tracks the instance's
    /// lifetime high-water mark.
    pub fn reset_call(&mut self) {
        self.oom = false;
    }
}

impl ResourceLimiter for BudgetLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if let Some(cap) = self.max_memory_bytes
            && desired > cap
        {
            // Deliberate, classified OOM at a known point.
            self.oom = true;
            return Ok(false);
        }
        if desired > self.peak_memory_bytes {
            self.peak_memory_bytes = desired;
        }
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        // The grow was allowed by `memory_growing` but failed in
        // the wasmtime allocator (host OOM / module-declared max).
        // Flag it so the `unreachable` trap that follows is
        // classified as OOM rather than a module fault.
        self.oom = true;
        Err(error)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // Table growth is bounded by the module's element-count
        // max and is never a chunking concern — always allow.
        Ok(true)
    }
}

/// Classified outcome of a guest call that returned `Err`. Lets a
/// heavy-op driver surface a meaningful outcome ("ran out of
/// memory") instead of an opaque "trapped."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrapClass {
    /// Per-call fuel budget exhausted (`Trap::OutOfFuel`).
    /// Retryable: the host refuels and loops.
    OutOfFuel,
    /// Linear-memory exhaustion — a denied or failed `memory.grow`
    /// was observed on the limiter. Retryable only with a smaller
    /// page (Phase 2 chunking); on its own a refuel won't help.
    OutOfMemory,
    /// Epoch deadline hit (`Trap::Interrupt`) — a wall-clock-ish
    /// hard interrupt.
    Timeout,
    /// Anything else — a genuine module logic fault: a `panic!`,
    /// an `unreachable` *not* preceded by an OOM, or a host-fn
    /// error propagated as a trap.
    Fault,
}

impl TrapClass {
    /// Classify a guest-call `Err`. `oom` is the limiter's
    /// `hit_oom()` flag, read off the `Store` data after the
    /// failing call.
    ///
    /// The OOM flag is load-bearing: an allocator abort traps as
    /// `UnreachableCodeReached`, the same code a buggy module
    /// produces. Only the limiter-observed failed grow tells the
    /// two apart.
    pub fn classify(err: &wasmtime::Error, oom: bool) -> TrapClass {
        match err.downcast_ref::<Trap>() {
            Some(Trap::OutOfFuel) => TrapClass::OutOfFuel,
            Some(Trap::Interrupt) => TrapClass::Timeout,
            // `unreachable` is what Rust's allocator abort traps
            // as — disambiguated only by the limiter's OOM flag.
            _ if oom => TrapClass::OutOfMemory,
            _ => TrapClass::Fault,
        }
    }

    /// Stable lowercase label for logs / telemetry.
    pub fn as_str(&self) -> &'static str {
        match self {
            TrapClass::OutOfFuel => "out-of-fuel",
            TrapClass::OutOfMemory => "out-of-memory",
            TrapClass::Timeout => "timeout",
            TrapClass::Fault => "fault",
        }
    }
}

impl std::fmt::Display for TrapClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Smallest page the sizer ever clamps to. Below this, host-call
/// overhead dominates; if even this traps, the module is
/// pathological and the host gives up the round.
const MIN_PAGE: u32 = 64;

/// Page a fresh scan starts at. Conservative — a prod recapture
/// (2026-05-19) trapped `out-of-fuel` on a 2048-UTxO page of
/// `holder-distribution` (`read_utxos` + bech32 parse + ledger
/// fold per UTxO). The sizer only ever probes *down* from here.
const INITIAL_PAGE: u32 = 256;

/// Host-owned page sizer for the bulk `utxos-by-*` host-fns.
/// Phase 2 of `WASM_BUDGET_CHUNKING.md`; reworked for Approach A
/// after the 2026-05-19 prod incident.
///
/// **Shrink-only.** It starts at `INITIAL_PAGE` and halves on a
/// trap or heavy fuel use, probing down to the per-module safe
/// page; it never grows back up. An upward AIMD step that
/// overshot the per-call fuel budget is what caused the
/// incident — the per-UTxO fold cost is data-dependent (a UTxO
/// with many asset names is far heavier), so no fixed page is
/// universally safe. The recovery path is "shrink + the host
/// re-instantiates and retries the predicate" (Approach A), not
/// "grow back and hope."
#[derive(Debug, Clone)]
pub struct AdaptiveSizer {
    /// Current page-size clamp, in refs.
    current: u32,
}

impl Default for AdaptiveSizer {
    fn default() -> Self {
        Self {
            current: INITIAL_PAGE,
        }
    }
}

impl AdaptiveSizer {
    /// Page size to return now, reconciling the module's hint
    /// with the clamp: `min(hint, current)`. A `0` hint (module
    /// deferring entirely to the host) yields `current`.
    pub fn page_limit(&self, hint: u32) -> usize {
        let hinted = if hint == 0 {
            self.current
        } else {
            hint.min(self.current)
        };
        hinted.max(MIN_PAGE) as usize
    }

    /// The current clamp, in refs — telemetry / tests.
    pub fn current(&self) -> u32 {
        self.current
    }

    /// Feed one guest call's budget telemetry. The sizer halves
    /// `current` on an OOM or on heavy (`> 80%`) fuel use; it
    /// never grows. The host carries the shrunk value into a
    /// re-instantiated module via `seed_current`.
    pub fn observe(&mut self, fuel_used: u64, fuel_limit: u64, hit_oom: bool) {
        if hit_oom {
            self.decrease();
            return;
        }
        if fuel_limit > 0 && (fuel_used as f64 / fuel_limit as f64) > 0.8 {
            self.decrease();
        }
    }

    /// Halve the page, floored at `MIN_PAGE`. Used by `observe`
    /// and directly by the host loop on a trap.
    pub fn decrease(&mut self) {
        self.current = (self.current / 2).max(MIN_PAGE);
    }

    /// Seed the page when the host re-instantiates a module
    /// after a retryable trap (Approach A): the shrunk page from
    /// the trapped instance is carried into the fresh one so the
    /// retried predicate resumes at the smaller page rather than
    /// the default. Clamped to `[MIN_PAGE, INITIAL_PAGE]`.
    pub fn seed_current(&mut self, page: u32) {
        self.current = page.clamp(MIN_PAGE, INITIAL_PAGE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_out_of_fuel() {
        let err: wasmtime::Error = Trap::OutOfFuel.into();
        assert_eq!(TrapClass::classify(&err, false), TrapClass::OutOfFuel);
        // Fuel wins even if an OOM was also flagged — the trap
        // code is the definitive signal here.
        assert_eq!(TrapClass::classify(&err, true), TrapClass::OutOfFuel);
    }

    #[test]
    fn classify_timeout() {
        let err: wasmtime::Error = Trap::Interrupt.into();
        assert_eq!(TrapClass::classify(&err, false), TrapClass::Timeout);
    }

    #[test]
    fn classify_unreachable_is_oom_when_flagged() {
        let err: wasmtime::Error = Trap::UnreachableCodeReached.into();
        // Without the limiter flag an `unreachable` is opaque —
        // treated as a module fault.
        assert_eq!(TrapClass::classify(&err, false), TrapClass::Fault);
        // With the flag it is a definite OOM.
        assert_eq!(TrapClass::classify(&err, true), TrapClass::OutOfMemory);
    }

    #[test]
    fn limiter_tracks_peak_and_reset() {
        let mut lim = BudgetLimiter::new(None);
        assert!(lim.memory_growing(0, 65536, None).unwrap());
        assert!(lim.memory_growing(65536, 131072, None).unwrap());
        assert!(lim.memory_growing(131072, 98304, None).unwrap());
        assert_eq!(lim.peak_memory_bytes(), 131072);
        assert!(!lim.hit_oom());

        // A failed grow flags OOM; `reset_call` clears the flag
        // but keeps the peak.
        lim.memory_grow_failed(wasmtime::Error::msg("grow failed"))
            .unwrap_err();
        assert!(lim.hit_oom());
        lim.reset_call();
        assert!(!lim.hit_oom());
        assert_eq!(lim.peak_memory_bytes(), 131072);
    }

    #[test]
    fn limiter_denies_grow_past_ceiling() {
        let mut lim = BudgetLimiter::new(Some(100_000));
        assert!(lim.memory_growing(0, 65536, None).unwrap());
        assert!(!lim.hit_oom());
        // Past the ceiling — denied, OOM flagged.
        assert!(!lim.memory_growing(65536, 200_000, None).unwrap());
        assert!(lim.hit_oom());
        // The denied grow does not move the peak.
        assert_eq!(lim.peak_memory_bytes(), 65536);
    }

    #[test]
    fn sizer_page_limit_honours_hint_and_clamp() {
        let s = AdaptiveSizer::default();
        assert_eq!(s.current(), 256);
        // Module hint below the clamp wins.
        assert_eq!(s.page_limit(100), 100);
        // Hint above the clamp is clamped to `current`.
        assert_eq!(s.page_limit(1_000_000), 256);
        // A zero hint defers entirely to the host.
        assert_eq!(s.page_limit(0), 256);
    }

    #[test]
    fn sizer_shrinks_on_pressure_and_never_grows() {
        let mut s = AdaptiveSizer::default();
        // Spare fuel — the sizer does not grow.
        s.observe(10, 100, false);
        assert_eq!(s.current(), 256);
        // Heavy (>80%) fuel use → halve.
        s.observe(90, 100, false);
        assert_eq!(s.current(), 128);
        // Still no upward growth on a later spare-fuel call.
        s.observe(10, 100, false);
        assert_eq!(s.current(), 128);
        // An OOM halves regardless of fuel.
        s.observe(0, 100, true);
        assert_eq!(s.current(), 64);
    }

    #[test]
    fn sizer_decrease_floors_at_min() {
        let mut s = AdaptiveSizer::default();
        for _ in 0..100 {
            s.decrease();
        }
        assert_eq!(s.current(), 64);
        // The floor still yields a usable page.
        assert_eq!(s.page_limit(1_000_000), 64);
    }

    #[test]
    fn sizer_seed_carries_a_shrunk_page_clamped() {
        let mut s = AdaptiveSizer::default();
        s.seed_current(128);
        assert_eq!(s.current(), 128);
        // Clamped into [MIN_PAGE, INITIAL_PAGE].
        s.seed_current(5);
        assert_eq!(s.current(), 64);
        s.seed_current(99_999);
        assert_eq!(s.current(), 256);
    }
}
