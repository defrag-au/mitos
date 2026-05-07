# Mitos platform v2 — eUTXO event dispatch

**Status: committed.** Sister to `MITOS_PLATFORM_V1.md` (which
landed the wasm-isolated module runtime) and
`MITOS_COMPANION_PATTERN.md` (the paired-deployable thesis).
This doc captures the v2 dispatch model we're building, not
options we're weighing.

The trigger condition fired during the jpg-co bootstrap trap of
2026-05: the v1 block-centric `handle_event(channel, block)`
shape forces every module to re-implement the same filtering,
chunking, and bootstrap-vs-tip-vs-backfill phase logic. v1 was
right for chain-followers and wrong for indexers. v2 fixes that.

Cross-references:
- `MITOS_PLATFORM_V1.md` — the runtime substrate v2 builds on
  (wasmtime engine, registry, lifecycle, host-fns, storage all
  carry over unchanged)
- `MITOS_COMPANION_PATTERN.md` — the paired-deployable contract
  this dispatch model serves
- `../design/MITOS_DATA_PLANE_API.md` — chain-data primitives;
  remain available as host-fns
- `../HOWTO_DEBUG_TRAPS.md` — the case study that motivated v2

## V2 in one sentence

**Platform v2 = same wasmtime runtime, same companion-pattern
deployment, but the dispatch unit is the eUTXO event filtered
by declared interest, not the raw Cardano block.**

The module receives a stream of `produced` / `consumed` events
matching its `Interest` set, batched per Cardano TX, in chain
order, with explicit `rollback` markers between batches when
the chain forks. Bootstrap, backfill, and tip dispatch all
flow through the same path; the module sees no distinction.

## Why v1 was wrong shape

v1 dispatched whole blocks. The module iterated TXs, walked
outputs and consumed inputs, applied its own
`watched_version(addr)` style filter, and decided what to emit.
This pattern repeated in every module — `collection-ownership`,
`marketplace`, `jpg-co`. Each module also had to:

- Implement bootstrap (current-state hydration on first deploy)
  separately from tip dispatch — different host-fn surface,
  different code path, different fuel pressure
- Hand-roll progress persistence for chunked work
- Re-derive interest from manifest config + per-call address
  matching

The jpg-co bootstrap trap of 2026-05 surfaced the cost: 4500+
unspent CO outputs to process at first deploy, single
`init()` call, fuel exhaustion mid-iteration. The fix path
considered (chunked-bootstrap-via-handle_event-state-machine)
would have made every future module re-implement the same
machinery. The platform was the wrong place to stop.

The v1 block-centric shape leaks the chain-follower's
perspective into indexer code. A jpg.store CO indexer doesn't
care about blocks; it cares about CO outputs being produced or
consumed.

## Type ownership

V2 owns its own types end-to-end — none of the WIT-surfaced
records, variants, or chain-points are direct re-exports of
dolos types. Mitos types live in `mitos-data-plane` and the
WIT bindings; conversions cross via `From<T>` impls in
`mitos-data-plane::adapters::dolos` (or equivalent
adapter modules per upstream).

Reasons:
- **Dolos version drift is constant.** v1 has already had to
  pin a specific tag (`v1.0.3`) and absorb breaking changes on
  every bump. Owning our types means dolos changes land as
  adapter-impl changes, not API breaks reaching all the way to
  the wasm module.
- **WIT positional encoding is unforgiving.** Adding a field
  to a record is a major-bump-breaking change in component
  ABI. Our types stabilise on what *modules* need; dolos's
  types evolve on dolos's schedule.
- **The companion side stays clean.** Frontend/companion-DO
  code consumes our shapes via `mirror-types`-style schemars
  generation; the DO never imports dolos directly.

This applies to `chain-point`, `output-ref`, `typed-output`,
`asset-id`, `asset-entry`, `typed-datum`, all event variants.
Same module that defined them today (`mitos-data-plane`)
keeps that responsibility — the WIT just becomes one
projection of those types.

## V2 contract — the WIT

```wit
interface types {
    record output-ref { /* unchanged from v1 */ }
    record asset-id { /* unchanged */ }
    record asset-entry { /* unchanged */ }
    record typed-output { /* unchanged */ }
    record typed-datum { /* unchanged — hash always present, payload may be empty */ }

    /// One event the platform delivers to a module. Filtered
    /// against the module's Interest set before dispatch —
    /// modules only see events from TXs that match their
    /// interest. Within a matching TX they receive every
    /// variant the platform produces (see ordering below) so
    /// they have full TX-level context, not just the matched
    /// output(s).
    variant utxo-event {
        produced(produced-event),
        consumed(consumed-event),
        referenced(referenced-event),
        minted(minted-event),
        tx-context(tx-context-event),
    }

    record produced-event {
        /// Chain point at which this output entered the UTxO
        /// set. Monotonic across an event stream within a
        /// dispatch, and across dispatches for non-rollback
        /// runs.
        cursor: chain-point,
        /// 32-byte tx hash that produced this output.
        tx-hash: list<u8>,
        /// 0-based index of the producing tx within its block.
        tx-idx: u32,
        oref: output-ref,
        output: typed-output,
        /// `Some` when the output carries a datum. Hash always
        /// populated; payload populated when the host could
        /// resolve (caller-blind inline-or-witness-set
        /// resolution per the data-plane contract). Modules
        /// that need metadata-encoded datums fall back via
        /// `chain-data::tx-metadata` using `tx-hash`.
        datum: option<typed-datum>,
    }

    record consumed-event {
        cursor: chain-point,
        /// Tx that consumed the output (the spending tx).
        consuming-tx-hash: list<u8>,
        consuming-tx-idx: u32,
        /// The spent ref (refers to the output produced
        /// earlier, possibly in a different block).
        oref: output-ref,
        prior-output: typed-output,
        prior-datum: option<typed-datum>,
        /// Plutus redeemer used to spend this input. `None`
        /// when the prior output was key-locked (no script
        /// involved). Critical for distinguishing operations
        /// on the same script address (Cancel vs Accept on a
        /// CO; Repay vs Default on a loan; etc.).
        redeemer: option<list<u8>>,
    }

    /// Reference input — a UTxO read by the consuming TX
    /// without being spent. Plutus contracts use these for
    /// read-only oracle access (price feeds, registry
    /// lookups). Surfaces only when the referenced output
    /// matches an Interest predicate.
    record referenced-event {
        cursor: chain-point,
        /// The TX that referenced this output.
        referencing-tx-hash: list<u8>,
        referencing-tx-idx: u32,
        oref: output-ref,
        prior-output: typed-output,
        prior-datum: option<typed-datum>,
    }

    /// Mint or burn of a single asset. One `minted-event` per
    /// `(policy, asset-name)` pair in the TX's mint field.
    /// Filtered by `holds-policy` / `holds-asset` predicates;
    /// fired regardless of whether the resulting output(s)
    /// are also matched by an Interest.
    record minted-event {
        cursor: chain-point,
        tx-hash: list<u8>,
        tx-idx: u32,
        policy: list<u8>,
        asset-name: list<u8>,
        /// Signed: positive = mint, negative = burn.
        quantity-delta: s64,
    }

    /// TX-level facts that don't fit per-output. Fired once
    /// per TX that matched any Interest. Modules that don't
    /// care about TX context just ignore the variant; modules
    /// that do (loans/escrow modules wanting validity
    /// intervals, multi-sig modules wanting required-signers,
    /// etc.) consume it.
    ///
    /// `tx-context` is always the FIRST event in a TX's
    /// dispatch batch (see "Per-TX event ordering" below),
    /// so subsequent `consumed`/`produced` events can be
    /// processed with TX-level state already in scope.
    record tx-context-event {
        cursor: chain-point,
        tx-hash: list<u8>,
        tx-idx: u32,
        validity-interval: validity-interval,
        /// Required additional signers (28-byte key hashes
        /// declared in the TX's `required_signers` field, not
        /// the inputs' implicit signers).
        required-signers: list<list<u8>>,
    }

    record validity-interval {
        valid-from: option<u64>,    // slot, inclusive
        valid-to: option<u64>,      // slot, exclusive
    }

    /// Periodic wake-up. Fires when any of the module's
    /// `tick-every(seconds)` interest predicates is due. Used
    /// for garbage collection, periodic snapshots, heartbeats
    /// — anything that needs to run on wall-clock cadence
    /// rather than block cadence. Cursor reflects the chain
    /// state at the time the tick fires.
    record tick-event {
        cursor: chain-point,
        /// Wall-clock timestamp (seconds since UNIX epoch)
        /// when the tick fired.
        timestamp: u64,
        /// Which `tick-every` registration triggered this
        /// tick. Lets a module register multiple intervals
        /// (e.g. 60s for snapshot, 3600s for compaction) and
        /// dispatch by interval id.
        interval-seconds: u32,
    }

    /// Rollback marker. Inserted between event batches when
    /// the chain forks past `to-cursor`. Modules that maintain
    /// derived state in their companion DO use this to
    /// rewind: every event with `cursor > to-cursor` they've
    /// emitted is now invalid.
    record rollback-event {
        to-cursor: chain-point,
    }

    /// What the platform actually dispatches via
    /// `handle-events`.
    variant dispatch-event {
        utxo(utxo-event),
        tick(tick-event),
        rollback(rollback-event),
    }

    /// Where on chain we are. Variant so we can represent
    /// genesis pre-state (`origin`) and slot-only points
    /// (`slot-only`, used by some indexers' captured cursors)
    /// alongside fully-specified `specific` points. Mitos
    /// owns this type; dolos conversions live in
    /// `mitos-data-plane::adapters::dolos`.
    variant chain-point {
        origin,
        slot-only(u64),
        specific(specific-point),
    }

    record specific-point {
        slot: u64,
        block-hash: list<u8>,
    }
}

/// Module-private KV state. Unchanged from v1.
interface state-kv { /* unchanged */ }

/// Structured logging. Unchanged from v1.
interface logging { /* unchanged */ }

/// Event emission. Unchanged from v1.
interface emit { /* unchanged */ }

/// Chain-data lookups. Unchanged from v1 except for one
/// added rollup. v2 takes a permissive stance on utility
/// host-fns: anything that helps a module do its job
/// cleanly belongs in the WIT. Wasm-side reimplementations
/// of indexer lookups are pure deadweight — the host has
/// dolos in process and the lookup is one redb call.
///
/// When in doubt, surface it.
interface chain-data {
    /* v1 methods unchanged: read-utxos, utxos-by-address,
       read-output-datums, read-output-hashes, tx-metadata */

    /// Full TX rollup — single call returns every component
    /// a module might need: inputs (with prior outputs and
    /// redeemers), reference inputs, outputs, mint field,
    /// required signers, validity interval, aux-data.
    ///
    /// Use cases this covers:
    /// - P2P atomic swap: module's interest matched one
    ///   party's events; `read-tx` fetches the
    ///   counterparty's inputs/outputs to assemble the swap.
    /// - Marketplace-fee inspection: module matched the
    ///   sale's CO consumption; `read-tx` finds the
    ///   marketplace-fee output among the siblings.
    /// - Loans-protocol settlement: module matched the
    ///   escrow spend; `read-tx` surfaces the repayment
    ///   output and any LP-token mint/burn from the same TX.
    ///
    /// `None` when the tx isn't known to the archive.
    read-tx: func(tx-hash: list<u8>) -> option<tx-record>;

    record tx-record {
        tx-hash: list<u8>,
        tx-idx: u32,
        cursor: chain-point,
        inputs: list<consumed-input>,
        reference-inputs: list<referenced-input>,
        outputs: list<typed-output>,
        mint: list<mint-entry>,
        required-signers: list<list<u8>>,
        validity-interval: validity-interval,
        /// Aux-data CBOR (TX metadata + native scripts +
        /// plutus scripts wrapper). Same shape as
        /// `tx-metadata` returns. `None` when no aux-data.
        aux-data: option<list<u8>>,
    }

    record consumed-input {
        oref: output-ref,
        prior-output: typed-output,
        prior-datum: option<typed-datum>,
        redeemer: option<list<u8>>,
    }

    record referenced-input {
        oref: output-ref,
        prior-output: typed-output,
        prior-datum: option<typed-datum>,
    }

    record mint-entry {
        policy: list<u8>,
        asset-name: list<u8>,
        quantity-delta: s64,
    }
}

/// Dynamic interest mechanics — runtime-mutable. Unchanged
/// from v1: companion pushes `InterestOp::{Add, Remove,
/// Replace}` over the WS, host calls `update-interest` on
/// the running module, module persists via `state-kv` so
/// restart-without-companion still filters correctly. v2
/// extends the predicate vocabulary (see "Interest model"
/// below).
interface interest { /* extended; see below */ }

world mitos-module {
    use types.{trap-strategy, retry-policy, dispatch-event};
    use interest.{interest-op};

    import chain-data;
    import state-kv;
    import emit;
    import logging;

    /// ABI version handshake. v2 modules return (2, 0).
    /// Mismatch is enforced at module load — v1 modules
    /// can't run on v2 hosts and vice-versa.
    export module-version: func() -> tuple<u32, u32>;

    /// Trap supervision policy. Unchanged from v1.
    export trap-policy: func() -> tuple<trap-strategy, retry-policy>;

    /// One-shot init at module load. Same shape as v1:
    /// CBOR-encoded typed config from the module's `<name>.toml`.
    /// Init is *light* in v2 — no bootstrap work happens here.
    /// Modules that need current-state hydration declare
    /// addresses in their interest set; the platform dispatches
    /// the bootstrap stream before live tip dispatch.
    export init: func(config: list<u8>);

    /// Single dispatch entry. The platform calls this with a
    /// list of events that share a single Cardano TX (or, in
    /// the bootstrap case, a chunk of synthesised
    /// per-output events). Events are in chain TX order.
    ///
    /// Rollback events arrive between TX batches; modules
    /// receive them as a separate `dispatch-event::rollback`
    /// variant in an otherwise normal `handle-events` call.
    export handle-events: func(events: list<dispatch-event>);

    /// Dynamic interest mutation. Unchanged from v1.
    export update-interest: func(op: interest-op, items-cbor: list<u8>)
                            -> result<_, string>;
}
```

Notable removals from v1:
- `block-context::resolved-block` resource and all its
  methods — modules never see blocks
- The `(channel: u32, block: borrow<resolved-block>)`
  signature on `handle-event`
- Per-output / per-tx walking helpers that lived on the
  block resource (modules iterate the dispatched event
  list directly)

## Dispatch unit semantics

One `handle-events` call delivers events from one Cardano TX,
all sharing the same TX hash. Within that TX the platform
emits events in this fixed order:

1. **`tx-context`** (exactly one, first) — TX-level facts
   (validity interval, required signers) so subsequent
   per-output events can be processed with TX-level state
   already in scope.
2. **`referenced` events** — reference inputs (read-only),
   one per matching reference input.
3. **`consumed` events** — inputs being spent, one per
   matching input. Carry the redeemer if script-locked.
4. **`produced` events** — outputs being created, one per
   matching output.
5. **`minted` events** — TX's mint field, one per
   `(policy, asset_name)` pair.

This ordering is the most deterministic shape we can give
modules without imposing a sort cost. It mirrors the
Plutus-script-context semantics (a script sees its own
context's referenced inputs, then inputs, then outputs, then
mint) and gives the module a predictable structure to match
against.

Within each category, events appear in their natural Cardano
TX order (input index for inputs, output index for outputs).

Properties:

- A module that cares about atomic relationships within a TX
  (marketplace fill: spent CO + new owner output produced;
  loan repayment: spent escrow + repayment output + LP-token
  burn) sees them all in one call.
- A module that doesn't care just iterates the list.
- TXs that produce no events matching the module's interest
  trigger no `handle-events` call — most blocks → zero wasm
  invocations for any specific module.
- The `tx-context` event is always first within the call,
  even when the module's interest only matched (say) a
  `produced` event. This means TX-level facts are reliably
  available before per-output processing.

Bootstrap is dispatched as a series of `handle-events` calls
where each call's events all share one historical
producing-TX hash. The cursor on each event is the producing
TX's chain point — *not* a synthesised value. This means a
module's emitted-event ordering is consistent with chain order
even during bootstrap. Bootstrap dispatch synthesises only
the events the historical state warrants: `produced` events
for the captured outputs, plus `tx-context` for the producing
TX. No `referenced`/`consumed`/`minted` events on bootstrap
(those are TX-time facts, not state-time facts).

`tick` events are dispatched between TX batches when due.
They never share a `handle-events` call with `utxo` events —
keeps tick-driven side-effects isolated from chain-driven
work.

Per-call fuel budget = `fuel_per_call` (100M today). The
platform refuels between calls. Modules that exceed it on a
real-world TX have a genuine performance issue, not an
artefact of bootstrap volume.

## Interest model

Runtime-mutable, declared via the existing
`update-interest` export. Predicates v2 admits:

```wit
variant interest-predicate {
    /// Bech32 payment address; matches outputs whose address
    /// is exactly this string.
    at-address(string),

    /// 28-byte stake credential; matches outputs whose
    /// address shares this stake credential, regardless of
    /// payment credential. Lets a wallet-history tracker
    /// follow all of a user's outputs across multiple
    /// payment keys delegating to the same stake key.
    at-stake-cred(stake-cred),

    /// 28-byte policy id; matches outputs whose asset
    /// multiset contains any asset under this policy. Also
    /// matches `minted-event`s under this policy.
    holds-policy(list<u8>),

    /// Specific asset; matches outputs holding this exact
    /// (policy, asset_name) and `minted-event`s for it.
    holds-asset(asset-id),

    /// Periodic wake-up. Module receives a `tick-event`
    /// every `seconds` (wall-clock) while running. Multiple
    /// `tick-every` registrations with different intervals
    /// dispatch independently.
    tick-every(u32),
}

variant stake-cred {
    key-hash(list<u8>),     // 28-byte stake key hash
    script-hash(list<u8>),  // 28-byte stake script hash
}
```

Filtering applies symmetrically across event variants:

- `produced` event matches if its `output` matches the predicate
- `consumed` event matches if its `prior-output` matches
- `referenced` event matches if its `prior-output` matches
- `minted` event matches `holds-policy` / `holds-asset`
  predicates only (address predicates don't apply — mints
  have no address)
- `tx-context` event is delivered once per TX where any
  other event in that TX matched at least one predicate
- `tick-event` matches `tick-every(seconds)` registrations

So an Interest of `at-address(addr1...CO)` on the jpg.store
script address surfaces:
- new CO outputs produced (Created)
- existing CO outputs spent (Cancelled or Accepted) — with
  the redeemer that distinguishes which
- the producing/consuming TX's `tx-context` (validity, etc.)

…without any module-side filtering. The module just decides
what to emit per event.

A loans-platform module declares
`at-address(addr1...escrow)` plus `holds-policy(loan_token_policy)`
and gets, for every relevant TX:
- `tx-context` (validity interval, signers)
- `referenced` events for any oracle UTxOs referenced
- `consumed` event for the spent escrow with redeemer
- `produced` event for the new escrow / repayment output
- `minted` event for LP-token burn on close

…all in one `handle-events` call.

Modules that need richer filtering (regex, predicate
combinators) post-filter inside `handle-events`. v2 explicitly
does not ship a custom-predicate WIT — once you're inside
the wasm sandbox the cost of post-filtering is one match arm.

## Bootstrap-as-events

Bootstrap is the dispatch of "events that brought the current
UTxO set's matching subset into existence". The platform tracks
bootstrap completion **per address** (not per module): a key
like `__bootstrap_complete:<addr>` in the module's state-kv
records that the address has been hydrated.

When `update-interest` adds an `at-address(addr)` predicate
(or when the manifest config introduces one at first deploy),
the platform checks the per-address bootstrap flag:

- **Not yet bootstrapped**: enumerate `utxos_by_address(addr)`;
  for each unspent ref, synthesise a `produced-event` keyed
  by the producing TX hash; group events sharing a producing
  TX into one `handle-events` call; dispatch with refuel
  between calls. Set `__bootstrap_complete:<addr>` once
  drained. Live tip dispatch continues in parallel from the
  chain cursor captured at bootstrap-start.

- **Already bootstrapped**: skip; live tip dispatch only for
  this address.

This is fully automatic. Operators don't need to remember a
`DELETE /bootstrap-cursor` step when adding a watched address
— the platform notices the missing flag and runs bootstrap
for the new address only. Existing addresses stay marked
complete; their bootstrap doesn't replay.

**Idempotence is the contract.** The companion DO must accept
duplicate `produced` / `consumed` events for the same
`(tx_hash, output_index)` and converge to the same state. Our
existing companion DOs already meet this: `Created` is
UPSERT-by-PK, `Spent` is DELETE-where-exists. Modules
authoring v2 companion DOs MUST follow the same pattern.

This is what makes bootstrap re-runs safe even when they
shouldn't strictly be needed: an operator who *does* want to
force re-hydration (after a companion-DO schema change, say)
deletes the `__bootstrap_complete:<addr>` key directly via
the existing `state-kv` admin path; the platform re-runs
bootstrap on next start, the DO absorbs the duplicate events
without divergence.

The synthesised events use the *real* producing TX hash and
chain point. Modules that want to dedupe against their
companion DO's existing rows (UPSERT semantics) get
deterministic keys. No synthetic slot numbers; no fake tx
hashes.

The aux-data path: `handle-events` does not eagerly populate
aux-data for bootstrap events. Modules that need it (like
jpg-co's metadata-encoded-datum fallback) call
`chain-data::tx-metadata(tx-hash)` from within
`handle-events` — same code as live dispatch, lazy resolution
via the dolos archive.

## Rollbacks

When the chain follower receives `TipEvent::Undo`, the platform
emits a `dispatch-event::rollback(to-cursor)` in the next
`handle-events` call (or as the sole event in a dedicated
call if the rollback arrives between batches). The module's
companion DO MUST treat all events with `cursor > to-cursor`
as invalid and roll its derived state back.

What the module / companion does on rollback is application
policy, not platform policy. Typical patterns:

- DELETE all rows where `cursor > to-cursor`, replay events
  as they arrive again from the re-applied blocks
- Maintain a savepoint; rollback to it on `RollbackEvent`,
  apply forward from there

The platform itself rolls its own driver cursor back to
`to-cursor` and resumes feeding events from there. Modules
emitting events with cursors after a rollback will see those
events again on replay; the companion DO's UPSERT/DELETE
contract handles re-delivery cleanly.

What the v2 contract guarantees:
- Rollback events are delivered in chain order with respect
  to the surrounding event stream.
- After a rollback, no events with `cursor > to-cursor` will
  arrive until the chain re-advances past it.
- The same TX never produces conflicting events without an
  intervening rollback (atomicity invariant).

## Migration from v1

Hard cutover. Three modules to migrate:
- `collection-ownership` — host-internal, owned in mitos repo
- `marketplace` — host-internal, owned in mitos repo
- `jpg-co` — external (cnft.dev-workers), uses companion pattern

Order of operations:
1. Land WIT v2 in `mitos-platform/wit/world.wit` as a parallel
   world (`mitos-module-v2`). Old world stays for v1 modules
   during the transition.
2. Implement v2 dispatch path in `mitos-platform`:
   - Block decoder → Interest filter → per-TX event batches
   - Bootstrap orchestration: `utxos_by_address` walk →
     synthesise events per producing-TX → dispatch
   - Rollback emission on `TipEvent::Undo`
   - `bootstrap_cursor` persistence in `ModuleStorage`
3. Wire ABI version handshake to route v2 modules through
   v2 dispatch and v1 modules through the existing path.
4. Migrate `jpg-co` first — smallest, has captured trap
   fixtures we can use as regression tests via `mitos-run`.
   Validate the bootstrap-as-events path against
   `last-trap.toml` from the v1 trap.
5. Migrate `collection-ownership` and `marketplace`.
6. Delete v1 dispatch path; remove `block-context` resource
   from the WIT.

`mitos-run` updates in lockstep with each step:
- Step 2: synthesise events from the existing fixture
  format (utxo + tx_metadata) and dispatch through v2
- Step 4: jpg-co fixture replays validate the migration

## Edge cases / non-goals

**Cross-TX atomicity (e.g. detecting a swap that spans two
TXs)** — out of scope. Modules detect via cursor proximity if
needed.

**Aggregations (counts, sums, time-series)** — orthogonal to
the dispatch model; companion-DO concern.

**Custom predicates (regex address matching, payment-cred
matching, datum-shape matching)** — not in v2 WIT. Stake-cred
matching IS in v2 (`at-stake-cred`); other predicates we
post-filter inside `handle-events`. If a real workload
demonstrates a hot path that justifies host-side custom
filtering, add it in v3.

**Cross-participant lookups in a TX where the module's
interest only matched one party (atomic swaps, multi-party
scripts, marketplace-fee inspection)** — covered by
`chain-data::read-tx(tx-hash)`. Lazy, opt-in. One host call
returns the full TX rollup so the module never has to
reconstruct from partial events.

**Lossless reconstruction of "the original block" from event
stream** — explicitly not a goal. Modules that need block-level
introspection are the wrong abstraction for an indexer; they
should be implemented as host-side dolos consumers, not v2
modules.

**Module quarantine / supervisor policy** — unchanged from v1.
A trap during `handle-events` triggers the supervisor's
configured strategy (replay, skip-and-mark, quarantine).
Trap-context fixture capture (the path we just built) covers
v2 dispatch; the captured fixture format extends to events
trivially.

## Open questions

None blocking. The following are implementation details we'll
resolve as code is written:

- Per-call event-batch sizing for bootstrap when one
  producing TX has many outputs at the watched address (e.g.
  a CO TX with 4 outputs all at the same script address —
  one batch, fine; a script address with thousands of
  outputs from the same producing TX — pathological,
  doesn't happen in practice).
- Whether the per-address bootstrap state-kv key should be
  reserved namespace (e.g. `__platform/bootstrap/<addr>`) so
  modules can't accidentally collide. Probably yes — keep
  platform-internal keys out of the module's flat state-kv
  namespace. Implementation detail.
- Whether to additionally expose `datum-by-hash(hash)` and
  `script-by-hash(hash)` as standalone host-fns. The data
  plane already supports them; cheap to lift into the WIT
  if a module needs them. Not blocking v2.0; add when the
  first module asks.
