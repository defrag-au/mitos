//! Spike guest module. Validates:
//!  1. `borrow<resolved-block>` parameter — guest cannot stash
//!     the handle past `handle-event` (compile-time enforced
//!     via the lifetime on `&ResolvedBlock`).
//!  2. Async host fn (`state-kv.get-value`) — call site compiles
//!     and returns a typed `Option<Vec<u8>>` from the guest's
//!     point of view.
//!  3. Tuple-returning export `trap-policy: func() -> tuple<...>`.

wit_bindgen::generate!({
    path: "../wit",
    world: "mitos-module",
});

struct Spike;

impl Guest for Spike {
    fn module_version() -> (u32, u32) {
        (1, 0)
    }

    fn trap_policy() -> (TrapStrategy, RetryPolicy) {
        (
            TrapStrategy::Replay,
            RetryPolicy {
                max_retries: 3,
                backoff_cap_ms: 1_000,
            },
        )
    }

    fn init(_config: Vec<u8>) {}

    fn handle_event(_channel: u32, block: &ResolvedBlock) {
        let _slot = block.slot();
        let _tx_count = block.tx_count();
        let _maybe_input = block.get_consumed_input(0, 0);

        let _v = mitos::spike::state_kv::get_value("spike-key");
        mitos::spike::state_kv::set_value("spike-key", &[1, 2, 3]);

        // CLAIM 1 (negative test). Uncommenting MUST fail to
        // compile because `&ResolvedBlock` is borrowed for the
        // lifetime of the call.
        //
        //   static mut STASH: Option<&ResolvedBlock> = None;
        //   unsafe { STASH = Some(block); }
    }
}

export!(Spike);
