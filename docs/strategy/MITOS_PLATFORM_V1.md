# Mitos platform v1 — concrete shape

**Status: committed.** This doc captures the implementation
shape we're building, not options we're weighing. Sister to
`MITOS_COMPANION_PATTERN.md` (the paired-deployable thesis) and
`../design/MITOS_ISOLATION_ROADMAP.md` (the historical context
for *why* this shape).

The trigger condition has fired: **no more mitos deploys until
platform v1 lands**. The current monolithic-bundle model is
blocking iteration; rebuilding mitos to ship indexer changes is
no longer acceptable.

Cross-references:
- `MITOS_COMPANION_PATTERN.md` — the paired-deployable contract
  this platform serves
- `CARDANO_DAPP_FRAMEWORK_THESIS.md` — the broader framework
  framing; platform v1 is the runtime piece
- `../design/MITOS_ISOLATION_ROADMAP.md` — context for why we're
  here and what we ruled out
- `../design/MITOS_DATA_PLANE_API.md` — the chain query primitive
  v1 modules consume via host functions
- `../design/INDEXER_TRAIT.md` — the trait shape platform v1
  module exports correspond to
- `../design/CF_REPLICATION.md` — the wire protocol the host
  runtime continues to expose unchanged

## V1 in one sentence

**Platform v1 = the existing mitos host, minus the statically-
composed indexer bundle, plus a wasmtime runtime that loads one
locally-distributed `.wasm` indexer module from the filesystem.**

That's it. Multi-tenancy, hot-reload over HTTP, per-team auth,
OCI registries, fair scheduling — all explicitly out of scope
for v1. The point of v1 is to **prove the shape end-to-end**
with one real indexer (`OwnershipIndexer`) running in the
sandbox, producing the same observable behaviour mitos has
today, with the indexer code shipping independently of the
mitos host binary.

## V1 scope (strict)

In:
- One wasm module: `ownership-indexer.wasm`
- One host: stripped-down mitos that loads it from a configured
  filesystem path on startup
- WIT-defined ABI between them
- Wasmtime runtime with fuel, epoch interruption, and
  `ResourceLimiter` configured from day one
- Per-worker `Store`, shared `Engine` + `Linker` (the standard
  wasmtime multi-tenancy pattern, used here for one tenant)
- Host functions for: chain reads (data plane), KV state,
  event emission, structured logging
- ABI version handshake (host refuses to load mismatched modules)
- Lazy consumed-input resolution behind the `resolved-block`
  resource (host resolves on first access, memoises for block
  lifetime; module never sees raw CBOR)
- Per-module redb cursor with bounded lag
- Author-declared trap strategy (`replay` / `skip-and-mark` /
  `quarantine`) with bounded retry policy; host supervisor
  consults it on every trap
- Same observable WS replication output as today

Out (defer to v2+):
- Multi-tenant module hosting (one module loaded, one slot)
- HTTP control plane (`/_admin/modules/*`)
- Hot-reload of running modules (restart-on-config-change is fine)
- Multiple module-language support (Rust → wasm32-wasip2 only)
- Module distribution / OCI / object storage
- Per-team auth scoping
- Marketplace indexer wasm port (lands on platform after v1)
- Companion-side `MitosCompanion` runtime SDK (separate work)
- `cargo cardano init` / `deploy` scaffolding (separate work)

This list is short on purpose. The risk is feature-creep
during v1; the discipline is: **anything that isn't on the In
list waits until v1 ships and we've learned from running it**.

## The reversal: WIT, not hand-rolled

A previous round of this conversation concluded with "hand-roll
the ABI in v1; switch to WIT later". That call is **reversed**.

**Why hand-rolled looked right last week:** WIT tooling has
historically been rough — `cargo-component` ceremony, churn
between `wasm32-unknown-unknown` and `wasm32-wasip2`, frequent
breaking changes in `wit-bindgen`. Hand-rolling a small ABI
against `wasm32-unknown-unknown` looked like the lower-risk path
for a v1.

**Why WIT is right now:** Spin v4.0.0 (mid-2026) is the existence
proof that the toolchain has stabilised. The Spin team
explicitly removed `cargo-component` from their docs because the
plain `cargo build --target wasm32-wasip2 --release` flow with
`wit-bindgen 0.54` works without ceremony. Wasmtime 44's
`component::bindgen!` macro generates idiomatic host code from
the same WIT files. The pieces fit; we'd be solving a problem
that no longer exists.

**What this changes:**
- We define our ABI in `.wit` files from day one
- Module side: `wit-bindgen 0.54` generates guest stubs from WIT
- Host side: `wasmtime::component::bindgen!` generates host
  trait + dispatcher from the same WIT
- Build target is `wasm32-wasip2` (not the old `unknown`)
- No `cargo-component`; plain `cargo build`
- No `spin-componentize`; module already lands as a component

The cost of this reversal is small — the WIT files are the
*specification*; the implementation pattern is the same either
way. The benefit is that the ABI is a typed artifact, not a
bag of `extern "C"` functions, and that we get host bindings
generated rather than written.

## WIT shape (sketch)

The ABI is a starting point — names and types will iterate
through implementation. The shape:

```wit
package mitos:platform@0.1.0;

interface types {
    record output-ref { tx-hash: list<u8>, index: u32 }
    record asset-id { policy: list<u8>, name: list<u8> }
    record typed-output { /* typed output from data plane */ }
    variant decode-level { lean, with-datum, full }
    variant utxo-predicate { /* tree algebra from data plane */ }
    record page-request { max-items: u32, start-token: option<string> }
    record page { items: list<typed-output>, next-token: option<string> }
    /* ... */
}

interface chain-data {
    use types.{output-ref, typed-output, decode-level,
               utxo-predicate, page-request, page};
    read-utxos: func(refs: list<output-ref>,
                     decode: decode-level) -> list<typed-output>;
    search-utxos: func(predicate: utxo-predicate,
                       decode: decode-level,
                       page: page-request) -> page;
}

interface block-context {
    /// Resource handle: host owns the decoded block; module
    /// pulls fields lazily without paying CBOR marshalling cost
    /// on every dispatch.
    resource resolved-block {
        slot: func() -> u64;
        tx-count: func() -> u32;
        get-tx: func(idx: u32) -> tx;
        /// Lazy consumed-input lookup. First call for a given
        /// (tx-idx, input-idx) triggers a single read against
        /// snapshot state via the data plane; result is memoised
        /// for the block's lifetime. Modules that don't ask
        /// don't pay. Marketplace-style indexers should prefer
        /// the batch variant below.
        get-consumed-input: func(tx-idx: u32, input-idx: u32)
                            -> option<typed-output>;
        /// Bulk consumed-input lookup for a single tx. Resolves
        /// all inputs in one data-plane call (one redb read
        /// transaction, one batched B-tree walk). Use this when
        /// the module knows it will need most inputs of a tx —
        /// classifier-style workloads.
        get-consumed-inputs: func(tx-idx: u32)
                             -> list<option<typed-output>>;
    }
    record tx { /* lazy view; module calls back through the resource */ }
}

interface state-kv {
    get-value: func(key: string) -> option<list<u8>>;
    set-value: func(key: string, value: list<u8>);
    delete-value: func(key: string);
}

interface emit {
    /// Module emits a pre-CBOR'd typed event. Host fans out to
    /// the existing CF replication WS without re-encoding.
    emit-event: func(channel: u32, event: list<u8>);
}

interface logging {
    record log-record { level: log-level, target: string,
                        message: string, fields: list<tuple<string,string>> }
    variant log-level { trace, debug, info, warn, error }
    log: func(record: log-record);
}

world mitos-module {
    import chain-data;
    import block-context;
    import state-kv;
    import emit;
    import logging;

    /// Module ABI version — host enforces compatibility before
    /// any other call. (1, 0) for v1; bump on any breaking
    /// change. Avoids Balius's silent-breakage pattern from
    /// their PR #98.
    export module-version: func() -> tuple<u32, u32>;

    /// Called once at module load. Module registers channels +
    /// declares its watch-set via host calls during init. Host
    /// uses the registered channels to dispatch via the universal
    /// handle-event below.
    export init: func(config: list<u8>);

    /// Single dispatch entry. Channel discriminates which logical
    /// stream the host is delivering on; events carry the typed
    /// payload (e.g. block context handle, individual tx, etc.)
    export handle-event: func(channel: u32,
                              event: chain-event)
                         -> result<_, handle-error>;
}
```

The `init`-registers-channels handshake mirrors Balius's
approach; the universal `handle-event(channel, event)` keeps the
ABI surface tiny while letting modules express many logical
event streams.

WIT syntax note from the spike: when the world's exports take a
`borrow<resolved-block>`, the resource must be `use`d into the
world's scope first. The dotted form `borrow<block-context.resolved-block>`
fails to parse; the correct shape is:

```wit
world mitos-module {
    use block-context.{resolved-block};
    import block-context;
    export handle-event: func(channel: u32, block: borrow<resolved-block>);
}
```

## Build pipeline

Module side:
```toml
# indexer/Cargo.toml
[package]
name = "ownership-indexer-module"
edition = "2024"

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.54"
# ... shared types crate, no pallas, no native deps
```

```bash
cargo build --target wasm32-wasip2 --release -p ownership-indexer-module
# produces target/wasm32-wasip2/release/ownership_indexer_module.wasm
```

Host side:
```toml
# mitos-platform/Cargo.toml
[dependencies]
wasmtime = { version = "44", features = ["async", "component-model"] }
```

```rust
wasmtime::component::bindgen!({
    path: "wit",
    world: "mitos-module",
    imports: { default: async | trappable },
    exports: { default: async },
    with: {
        // Path format: <package>:<name>/<interface>.<resource>
        // — dot before the resource, NOT slash.
        "mitos:platform/block-context.resolved-block": ResolvedBlock,
    },
});
```

`add_to_linker` takes a `HasData` parameter; for the common
"Store data implements every host trait" case the canonical
idiom is:

```rust
MitosModule::add_to_linker::<_, HasSelf<HostState>>(
    &mut linker, |s| s,
)?;
```

Empty interface-level `Host` marker traits must be implemented
on the store data even when the interface contains only records
or free functions — bindgen requires `impl mitos::platform::types::Host for HostState {}`.

Note: we do **not** enable `wasm_component_model_async` on the
`Config`. That flag turns on the Component Model concurrency ABI
(`stream`, `future`, `error-context`) which our WIT does not
use; wasmtime 44 documents it as "_very_ incomplete". The
`imports: { default: async }` bindgen knob is a separate
mechanism — stable since wasmtime 12, used by Spin in production
— that simply makes generated host trait fns return `Future`s.
That's all we need.

The deprecated `Config::async_support(true)` is also a no-op as
of wasmtime 42 ([RELEASES.md](https://github.com/bytecodealliance/wasmtime/blob/release-42.0.0/RELEASES.md))
— don't call it. Async support is implicit once the wasmtime
crate's `async` feature is enabled.

`Config::consume_fuel(true)` and `Config::epoch_interruption(true)`
must both be set before any per-store fuel/epoch call. Epoch
interruption additionally requires `store.set_epoch_deadline(N)`
before the first guest call — otherwise the first instruction
traps as `wasm trap: interrupt`. A background thread bumps the
epoch periodically; the deadline is the number of ticks the
guest may execute before being interrupted.

That's the entire build pipeline. No `cargo-component`, no
`spin-componentize`, no `wac` composition tool. Plain cargo +
two crate dependencies.

## What we vendor from Balius

Balius (TxPipe's wasm indexer framework, Apache-2.0 licensed,
~12 months inactive but architecturally solid) solved several
problems we'd otherwise resolve from scratch. We vendor four
files with explicit attribution rather than depend on the crate
— Balius's runtime is wider than what we need, and depending on
an inactive project for our load-bearing platform is the wrong
risk profile.

### Files vendored

Each file is copied into `mitos-platform/src/vendored/balius/`
with the upstream Apache-2.0 license header preserved verbatim
and a `// Vendored from txpipe/balius @ <commit>: ...` annotation
on top.

**1. `router.rs`** (~140 lines, from `balius-runtime/src/router.rs`)
- Provides `MatchKey` — "given this tx/utxo, which workers care?"
- The matching engine for our equivalent of the Watch grammar
- Keep their algorithmic approach; replace their match-key
  variants with our typed predicates from `mitos-data-plane`

**2. ~~`store.rs`~~** — REMOVED from the vendoring plan after
inspection. Balius's `store.rs` is a host-side WAL for
replay-safe worker restart; mitos has a different replay model
(dolos's archive is our WAL). We built a focused `CursorStore`
in `mitos_platform::storage` instead — ~80 lines, single redb
table, single-row cursor. KV side stays vendored from #3 below.
See `MITOS_PLATFORM_DEPLOYMENT.md` §"Resolved design questions"
#7 for the rationale.

**3. `kv/redb.rs`** (from `balius-runtime/src/kv/redb.rs`)
- Worker-prefixed KV implementation
- Backs the `state-kv` host interface
- Used as-is; only the trait it implements is renamed to fit
  our naming

**4. `metrics.rs`** (~291 lines, from `balius-runtime/src/metrics.rs`)
- OpenTelemetry per-worker metrics surface
- Names of metrics adapted to our existing `mitos_*` naming
  convention; structure preserved

### Attribution

Each vendored file carries:
```rust
// Vendored from github.com/txpipe/balius @ <commit-sha>
// Original path: balius-runtime/src/<path>
// Apache-2.0 — see LICENSE-APACHE-2.0 (vendored alongside)
//
// Local modifications:
// - <list of substantive deltas>
```

We add `LICENSE-APACHE-2.0` to `mitos-platform/src/vendored/balius/`
and a top-level `NOTICE` referencing the upstream project. The
`Cargo.toml` for `mitos-platform` does **not** claim authorship
of these files; the `authors` field plus the per-file headers
make the lineage explicit.

This is the same pattern dolos uses for its handful of vendored
pallas helpers — proven, low-friction, audit-traceable.

### Patterns we copy (without vendoring)

Three Balius design patterns we adopt by reimplementation
because the code is small enough that copying loses portability:

1. **`init`-registers-channels handshake.** Module's `init`
   exported function calls back into the host to register the
   logical channels it will receive events on; host then
   dispatches via universal `handle-event(channel, event)`.
   Tiny ABI surface, expressive routing.

2. **Per-worker `Store`, shared `Engine` + `Linker`.** Standard
   wasmtime multi-tenancy idiom: one `Engine` (compilation +
   runtime config) shared, one `Linker` (host fn definitions)
   shared, but each module instance gets its own `Store` (state +
   resource accounting). V1 has one tenant but the shape is
   right for v2's multi-tenant case.

3. **`FnHandler<F, C, E, R>` typed-function adapter.** Pattern
   from Balius's `qol.rs` for converting a typed Rust closure
   into a host function the wasmtime runtime can call. Saves
   per-host-fn boilerplate; we reimplement against our types.

## Things we explicitly do *not* copy from Balius

Reading their codebase taught us what to avoid. These are
deliberate departures, not oversights:

**1. ABI versioning from day one.** Balius shipped without a
version handshake; PR #98 silently broke modules built against
older runtimes. Our `module-version` export is mandatory and
checked before any other call.

**2. Per-worker cursor with bounded lag, not min-cursor coupling.**
Balius's runtime advances based on `min(all worker cursors)`,
which means a slow worker stalls all workers. We track per-worker
cursors with a bounded lag tolerance; a slow module gets restarted
or quarantined, it doesn't block the host.

**3. Resolve consumed inputs host-side, lazily.** Balius hands
raw CBOR to modules; our `block-context` resource resolves
consumed inputs from snapshot state via the data plane, but
only on demand — first access triggers the lookup, the result
is memoised for the block's lifetime. Eager pre-resolution was
the original design but the cost analysis (every input lookup
is a redb B-tree point read; ownership-style indexers consume
zero inputs; busy mainnet blocks have hundreds of inputs) made
lazy the obvious answer. Modules still can't get raw CBOR — the
resource owns the decode path either way. Marketplace-style
indexers that want bulk resolution use the
`get-consumed-inputs(tx_idx)` batch variant, which amortises the
redb read-transaction open cost.

**4. Author-declared trap supervision, not trap propagation.**
Balius's runtime propagates traps up; we catch them, log them,
and consult the module's declared `trap-strategy` (`replay` /
`skip-and-mark` / `quarantine`) with bounded retry. A trapping
module shouldn't break the chain follower, *and* the module
author — not the host — gets to decide whether replaying is
safe, skipping is acceptable, or human review is required.
Keeps semantics in the module source where they belong.

**5. Pallas not in modules.** Balius modules can pull pallas in
as a wasm dependency; we forbid this. The host owns block decode;
modules consume typed values. Performance reasons (CBOR
marshalling at the boundary is expensive when modules need raw
shapes) and correctness reasons (decode logic shouldn't fork per
module).

## Resource limits from day one

Wasmtime has three independent runaway-prevention mechanisms;
v1 turns all three on:

**Fuel.** Each `handle-event` call gets a fuel budget (initially
generous, e.g. 100M units; tunable per-channel). Exhaustion
returns a trap; supervisor handles the restart.

**Epoch interruption.** Background thread bumps an epoch counter
periodically; `Store` is configured to check the epoch and trap
on mismatch. Catches infinite loops that don't burn fuel
(unlikely in pure compute, real for futures and loops over
small primitives). Wall-clock-driven, deterministic enough.

**`ResourceLimiter`.** Per-instance memory cap (initial 64 MiB,
tunable); table-element cap; instance-count cap. Wasmtime calls
the limiter before allocating; we deny over-budget allocations
and the module traps with a clear error.

These costs are small (single-digit % overhead in wasmtime
benchmarks) and the benefit is decisive: a buggy module cannot
take the host down. This is *the* reason v1 is wasm and not
native plugins.

## Platform layer breakdown

Estimated ~800-1500 lines for the platform crate (excluding
vendored Balius files, which add another ~700):

- **Module registry** (~150 lines) — load, version-check,
  instantiate, supervise restart, quarantine on N failures
- **Per-subscription instance management** (~250 lines) — each
  active CF subscription gets a module instance; lifecycle
  tied to subscription; cursor tracked separately per
  instance
- **Host fn bindings** (~400 lines) — implement the WIT-imported
  interfaces (`chain-data`, `block-context`, `state-kv`, `emit`,
  `logging`); thin wrappers over `mitos-data-plane`, redb, the
  CF replication channel, and `tracing`
- **Subscription lifecycle** (~200 lines) — translate CF WS
  subscribe → module instance + channel registration → event
  dispatch loop → cursor advance
- **`resolved-block` resource** (~200 lines) — per-block, build
  the resource backed by the decoded block + a lazy resolution
  cache; on `get-consumed-input` / `get-consumed-inputs` calls,
  fetch from the data plane and memoise. Resource lives in a
  long-lived `ResourceTable` on the `Store`; pushed before
  `handle-event`, deleted after. Pass as `borrow` so the guest
  can't stash it past the dispatch boundary (compile-time
  enforcement). Modelled on `wasmtime-wasi-http`'s pattern for
  request-body resources.

Tests + integration layer adds another ~500 lines on top. The
crate is small on purpose; that's the discipline of v1.

## V1 done definition

V1 ships when:

1. `ownership-indexer.wasm` builds from a clean checkout via
   `cargo build --target wasm32-wasip2 --release`
2. Mitos platform host loads it on startup from a configured
   path (e.g. `/etc/mitos/modules/ownership-indexer.wasm`)
3. The host's existing CF replication WS produces **bit-for-bit
   identical** events to the current monolithic-bundle host for
   a recorded test corpus of mainnet blocks
4. Backfill works (ownership state at tip after 24h soak matches
   the current host's state on the same input)
5. A deliberately-trapping test module gets caught, logged,
   restarted up to N times, and quarantined — without taking the
   host down
6. Resource limits trigger (test module that allocates 200 MiB
   gets denied at 64 MiB; test module with infinite loop gets
   epoch-interrupted within 1s)
7. ABI version mismatch test refuses to load and logs a clear
   diagnostic
8. CI builds both halves (host + module) on every PR; integration
   test runs the platform with the real ownership module against
   a recorded block stream

The success bar is **observable equivalence** with the current
host, plus **provable isolation** of the module from the host.
No new features.

## After v1

Once v1 is running and stable, the natural next steps:

- Port `MarketplaceIndexer` to the same shape (the real test of
  the `block-context` resource — input resolution + classifier)
- Add HTTP control plane (`/_admin/modules/*`) for
  upload/reload/list
- Add multi-module support (one host, N tenants' modules)
- Companion-side `MitosCompanion` runtime SDK (the CF half of
  the paired-deployable contract)
- `cargo cardano init` / `deploy` scaffolding

These are explicitly out of v1 scope. We learn from running v1
before committing the shapes for v2.

## Resolved design questions (post-spike)

The spikes below were run before committing v1 implementation.
Recording the answers here so the rationale is recoverable.

### 1. Async over the WIT boundary — RESOLVED: commit to async

Use `imports: { default: async | trappable }, exports: { default:
async }` on the bindgen macro. Spin v4.0.0 (2026-04) ships this
pattern in production; the `imports: { default: async }` knob has
been stable in wasmtime since v12. No fallback to sync host fns
+ dedicated thread pool is warranted.

Concrete gotchas baked into v1:
- **Send bound is mandatory** on host fn futures. Don't hold
  `!Send` types (`Rc`, `RefCell`) across `.await` in host
  fns. `Arc<redb::Database>` and our dolos-cardano clients are
  fine.
- **Don't `block_on` inside async host fns** — deadlocks Tokio
  under load. Use `tokio::task::spawn_blocking` for any
  unavoidable blocking call.
- **Pair with `epoch_interruption(true)`** for guest preemption.
  Wasmtime async only yields to Tokio at `.await` boundaries in
  host fns; pure-compute guest loops need epoch ticks.
- **Don't enable `wasm_component_model_async`** on `Config` —
  that's the Component Model concurrency ABI for `stream`,
  `future`, `error-context`, which our WIT does not use and
  which wasmtime 44 documents as "_very_ incomplete".
- **Drop deprecated `Config::async_support(true)`** — no-op as
  of wasmtime 42.

### 2. `resolved-block` resource lifetime — RESOLVED: persistent ResourceTable, push-and-delete per block

Pattern (modelled on `wasmtime-wasi-http`'s body-resource
handling):

```rust
// In StoreData
struct HostState {
    table: ResourceTable,  // long-lived; lives in Store
    // ... other state
}

// Per-block dispatch
let resource = state.table.push(resolved_block)?;
instance.call_handle_event(&mut store, channel, event, &resource).await?;
state.table.delete::<ResolvedBlock>(resource_id)?;
```

Key calls:
- **One `ResourceTable` per `Store`, lives the lifetime of the
  instance.** Don't recreate per block.
- **Pass the resource as `borrow`** (not `own`). The guest can
  read from it during `handle-event` but cannot stash it past
  the call boundary — compile-time enforcement. Avoids the
  parent-child tracking complexity of `own`-typed resources.
- **`Resource<T>` is a non-forgeable `u32` index.** If a buggy
  guest somehow held a stale handle (only possible with `own`
  + late drop), the next access traps via `ResourceTableError`
  — memory-safe.
- **Don't model `tx` and `typed-output` as resources.** Return
  them as plain WIT records by value. Resources are for
  identity/lazy semantics; these don't need either, and modelling
  them as records sidesteps parent-child cleanup ordering.

### 3. Pre-resolution cost — RESOLVED: lazy-on-demand inside the resource

Originally specced as eager pre-resolution before dispatch; the
analysis flipped this to lazy:

- Every consumed-input lookup is a redb B-tree point read against
  the dolos UTxO state (`get_sparse` opens a read transaction,
  loops refs, does N independent lookups; no in-memory cache
  layer).
- Today's `OwnershipIndexer` resolves zero consumed inputs (it
  only walks `tx.produces()`). Eager pre-resolution would charge
  it ~hundreds of redb reads per block for data it never
  touches — a regression vs. today.
- Cardano blocks are getting fatter (Input Endorsers); eager
  scales linearly with block fatness whether modules care or not.

Lazy + memoise inside `resolved-block` keeps the
"module-author-can't-get-it-wrong" property (modules still can't
see raw CBOR; the resource owns the decode path). Marketplace-
style indexers that want bulk resolution use
`get-consumed-inputs(tx_idx)` to amortise the redb read-tx open
cost across all inputs of one tx. Pure-output indexers pay zero.

## Resolved design questions (continued)

### 4. Cursor coordination on mid-block trap — RESOLVED: author-declared strategy

Module authors know best whether their indexer can safely replay
a block, must skip, or has to halt for human review. The host
supports a small enum of strategies declared by the module up
front; the supervisor consults it on every trap.

Add to the WIT:

```wit
interface lifecycle {
    /// How the host should handle a trap during handle-event.
    variant trap-strategy {
        /// Replay the block on restart. Module promises
        /// idempotent dispatch (e.g. ownership-style: typed
        /// upserts, no side effects per call).
        replay,
        /// Skip the failing block, advance the cursor, log a
        /// structured warning. Module accepts gaps for the sake
        /// of liveness (e.g. best-effort metadata indexers).
        skip-and-mark,
        /// Stop the module, do not advance the cursor, surface
        /// loudly. Operator must intervene before processing
        /// resumes. For modules whose state would be corrupted
        /// by skipping or by replaying a non-idempotent step.
        quarantine,
    }

    record retry-policy {
        /// Bounded retry count before falling through to the
        /// strategy. Lets transient errors (e.g. snapshot read
        /// blip) recover without invoking the strategy at all.
        max-retries: u32,
        /// Exponential backoff cap, ms.
        backoff-cap-ms: u32,
    }
}

world mitos-module {
    // ...existing exports...

    /// Called once at module load, before init. Module declares
    /// its supervision policy; host uses it for the lifetime
    /// of this instance. Returning different values across
    /// restarts of the same module version is undefined.
    export trap-policy: func() -> tuple<trap-strategy, retry-policy>;
}
```

V1 default for `OwnershipIndexer`: `replay`, max-retries 3,
backoff cap 1s. The module *also* gets to record its choice
explicitly — no implicit "host picks for you" — so the strategy
shows up in module source, not in operator config.

The strategy is per-module, not per-event. Per-event would let
modules signal "this specific block is fine to skip but the
next isn't"; that's expressive but adds plumbing v1 doesn't
need. If we discover a real use case post-v1, the natural shape
is `handle-event -> result<_, handle-error>` already returns a
typed error; we'd extend `handle-error` with strategy hints
later.

Validation: integration tests for each strategy variant —
inject a trap, verify (a) replay produces matching state in a
control run, (b) skip advances cursor and logs the gap, (c)
quarantine stops the instance and surfaces a clear operator
diagnostic.

### 5. Module config payload format — RESOLVED: CBOR + per-module TOML alongside wrangler.toml

The TOML file lives in the dApp's repo next to `wrangler.toml`
— same pattern operators already know, same colocation the
companion-pattern thesis assumes. The host transcodes to CBOR'd
typed config and hands to `init`.

```
my-app/
├── wrangler.toml
├── mitos.toml          # ← module config; loaded by mitos host
├── shared/             # types crate; defines the typed config struct
├── indexer/            # the wasm module
└── companion/          # the CF DO
```

Concretely:
- The module's `shared/` crate defines a `Config` struct
  (`#[derive(Serialize, Deserialize)]`).
- `mitos.toml` is human-edited; deserialises into `Config` via
  `toml`.
- The mitos host reads the TOML file, deserialises into the
  module's typed `Config` shape (known via the host's view of
  the shared crate, or via a registered config schema — TBD
  during impl), re-encodes as CBOR, passes to
  `init(config: list<u8>)`.
- Module side: `wit_bindgen`-generated `init` impl deserialises
  the CBOR back into the typed `Config` from the same shared
  crate. Wire format is CBOR (compact, deterministic, fast); the
  *human-facing* format is TOML.

Open detail for v1: how the host knows the config schema
(parse-with-validation) — options are (a) host loads the schema
from the wasm module's `__mitos_config_schema` custom section,
(b) host treats the CBOR as opaque and only the module
validates. Default to (b) for v1; iterate to (a) if operator
typo diagnostics become a problem.

For the `OwnershipIndexer` initial port, `Config` is small —
the watched policy set + a backfill-style flag. Hand-validating
that in the module is fine.

## Lessons banked from existing implementations

- **Balius's silent ABI breakage (PR #98)** — taught us to
  version the ABI from day one
- **Balius's min-cursor coupling** — taught us to track per-worker
  cursors with bounded lag rather than a global min
- **Spin's mid-2026 toolchain stability** — gave us confidence
  WIT was the right call now even though it wasn't last week
- **Lunatic's actor-per-worker pattern** — informed our
  per-instance supervision shape (catch and restart, not trap
  propagation)
- **Mitos's monolithic-bundle pain over the past 18 months** —
  the trigger condition is real; this isn't speculative work
- **The `mitos-data-plane` Phase A spike** — confirmed the typed
  query API is the right shape for module consumption; v1
  exposes it via WIT without further redesign
- **The WIT spike at `spikes/wit-spike/`** — end-to-end validated
  the three load-bearing claims before committing platform
  structure: (a) `borrow<resolved-block>` resource passed to
  `handle-event` enforces non-stash via Rust borrow lifetime,
  (b) `imports: { default: async | trappable }` flows through
  Tokio cleanly with `tokio::time::sleep` inside an async host
  fn, (c) tuple-returning export `trap-policy: func() -> tuple<trap-strategy, retry-policy>`
  works with bindgen first-try. Spike artifact compiles + runs
  on wasmtime 44 / wit-bindgen 0.54 / wasm32-wasip2.
