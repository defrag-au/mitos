# Isolation roadmap

How mitos's indexer-deployment model should evolve once the
current monolithic-bundle approach hits its limits. **This is a
forward-looking design — nothing here is built yet.** Captured so
the architectural direction is recoverable later when we hit the
trigger conditions.

Cross-references:
- `ARCHITECTURE.md` — current host design (single-process bundle
  with statically-composed indexers)
- `SUBSCRIPTION_MECHANICS.md` — `Interest` selectors, the
  *consumer-side* declarative-filter precedent that informs the
  *indexer-side* `Watch` grammar proposed here
- `INDEXER_TRAIT.md` — the trait that needs to stabilise before
  any of this can land
- `ROADMAP.md` step 8+ — `mitos-protocol` extraction (already
  partially done) is on the path to here

## The frustration

As of 2026-05 the host (mitos) and every indexer (`OwnershipIndexer`,
`MarketplaceIndexer`, future DEX / lending / etc.) compile into a
single monolithic bundle binary. Adding or modifying an indexer
means:

- Edit code in the mitos repo
- `cargo build --release` the entire bundle (~75s on the netcup
  box, longer locally)
- `systemctl restart mitos-mainnet` — drops every CF replication
  WS, re-bootstraps Dolos cursor checks, re-establishes peer
  connections

That's painful for two reasons that compound each other:

1. **Coupling between host and indexer lifecycles.** A change
   that's purely consumer-specific (e.g. a new collection-stats
   indexer for one team) requires the mitos repo to be touched and
   redeployed. Cross-team velocity suffers; the mitos repo
   becomes a governance bottleneck.
2. **Lack of isolation.** A panic or runaway loop in one indexer
   takes the whole host down. There's no fault boundary between
   indexers, between an indexer and the dispatcher, or between an
   indexer and Dolos's chain follower.

The indexer abstractions and runtime behaviour we have are sound.
The deployment / loading model isn't.

## What we explicitly don't want

- **Pure wasm-everywhere.** Pallas decode in pure wasm is
  workable but pays measurable boundary costs and loses native
  crypto / SIMD where it counts. Workloads that do many host-
  state lookups per block (marketplace input resolution being a
  notable example) compound the boundary cost. Going pure-wasm
  buys isolation at a non-trivial throughput tax.
- **Pure subprocess plugins.** Real OS-level isolation but every
  chain event becomes IPC. Dozens of subscribers × hundreds of
  events per block × IPC roundtrip is a budget we don't want to
  spend.
- **Hot-reload as a goal in itself.** What we want is *decoupled
  deployment lifecycles* between the host and the indexers — not
  zero downtime. Restarts are fine, *cross-repo coupled restarts*
  aren't.

## The shape we're heading toward

A **two-tier hybrid**: native indexing primitives in the host,
wasm modules for per-consumer transform / projection logic. The
wasm boundary only crosses for what's cheap; the hot loop stays
native.

```
┌──────────────────────────────────────────────────────────────┐
│ mitos host (native, single process)                          │
│                                                              │
│   Dolos data plane ──── chain events                         │
│            │                                                 │
│            ▼                                                 │
│   ┌────────────────────────────────────────────┐             │
│   │  Native indexing primitives                │             │
│   │   - block traverse / tx walk                │            │
│   │   - input resolution (state lookups)        │            │
│   │   - asset-multiset extraction               │            │
│   │   - datum decode (inline + witness-set)     │            │
│   │   - address-pattern + policy filtering      │            │
│   │   - RawTxData construction (classifier)     │            │
│   │  Pallas full-speed; results memoised across │             │
│   │  modules subscribing to overlapping work.   │             │
│   └────────────────┬───────────────────────────┘             │
│                    │ typed inputs                            │
│                    ▼                                         │
│   ┌────────────────────────────────────────────┐             │
│   │  wasm module instances (wasmtime sandbox)  │             │
│   │   - declare Watch (intent) at register     │             │
│   │   - declare DecodeRequest (what host       │             │
│   │     pre-decodes for me)                    │             │
│   │   - per matching event: transform typed     │            │
│   │     inputs → typed ProtocolEvent / Change   │            │
│   │   - emit via host function                 │             │
│   │   - own state in linear memory; host       │             │
│   │     persists via get/set_state             │             │
│   └────────────────────────────────────────────┘             │
└──────────────────────────────────────────────────────────────┘
```

The wasm module's job is **"given typed inputs, produce typed
outputs"** — it does not parse blocks, does not run pallas, does
not do crypto. It does declarative *intent declaration* up front
(what slice of chain matters) and *typed transformation* per
event.

## Goals

- **Colocation.** Indexer code lives next to the consumer that
  produces it (e.g.
  `cnft.dev-workers/indexers/collection-stats/`).
  Worker authors own their indexers; mitos repo stays small (host
  + primitives + ABI).
- **Decoupled deployment lifecycles.** A new indexer or an
  iteration on an existing one ships independently of the host.
  Worker `wrangler deploy` (or equivalent) pushes the `.wasm`
  artifact to mitos's control plane; mitos hot-loads it. No
  mitos rebuild, no host restart.
- **Real fault isolation.** Wasmtime sandbox per module: a panic
  / OOM / runaway loop kills the offending module instance, not
  the host. Other indexers and the chain follower keep running.
  This is the actual reason wasm earns its keep here — `catch_unwind`
  on a native plugin doesn't cover SIGSEGV / infinite loops / OOM.
- **Native-grade chain decode.** All the heavy work — block
  parse, UTxO resolution, datum decode, fingerprint, classifier
  pre-pass — runs in the host at full pallas speed. Wasm modules
  pay only the transform-step boundary cost, which is bounded
  per emitted event (not per chain event).

## Non-goals

- **Hot-swap individual indexer logic without ever restarting.**
  Nice-to-have; not the point. Module load/reload must be a clean
  transactional operation, but if it occasionally requires a
  graceful module re-init, that's fine.
- **Multi-language modules.** Modules are Rust → wasm32. We're
  not building a polyglot indexer SDK. Other languages can
  follow the published ABI if they want; we won't design for
  them.
- **A scripting language for indexers.** No CEL, no Lua, no
  custom DSL. Modules are typed Rust; the *intent* part is a
  typed Rust enum (`Watch`), not a string DSL.

## Proposed API surfaces

These are sketches to anchor the design discussion. Names and
shapes will be iterated during the actual design phase.

### 1. The `Watch` grammar (declarative intent)

Each module declares up-front what slice of chain activity it
wants to be invoked for. The host plans block scans against the
*union* of all registered Watches; modules whose Watch doesn't
match a given block are never invoked for it.

```rust
pub struct IndexerIntent {
    pub name: String,
    pub watch: Watch,
    pub decode: DecodeRequest,
    pub state_schema: Option<StateSchema>,
}

pub enum Watch {
    /// Host calls module once per block (after pre-decode).
    Block,
    /// Host calls module per tx whose shape matches the filter.
    /// Module never sees a tx that doesn't pass.
    Tx(TxFilter),
    /// Host calls module per output produced at one of the
    /// named addresses or under one of the named policies.
    Output(OutputFilter),
}

pub struct TxFilter {
    pub spends_at: Vec<AddressPattern>,
    pub produces_at: Vec<AddressPattern>,
    pub mints: Vec<PolicyId>,
    pub references_script: Vec<ScriptHash>,
    pub combine: Combine, // any | all
}
```

`Watch` is to the *indexer* what `Interest` is to the *consumer*:
both are typed declarative filters. They compose at runtime —
`Watch` filters the chain into what each indexer sees; `Interest`
filters indexer output into what each consumer receives.

**Highest-risk design call.** The vocabulary here defines the
framework. Get it wrong and modules fall back to "give me raw
blocks", defeating the model. Get it right and 90% of indexers
never touch raw bytes. Worth taking time on; first cut should be
informed by reviewing every indexer pattern we've encountered
(ownership, marketplace, listings, jpg.store CO, dex, lending).

### 2. `DecodeRequest` (pre-decoded inputs)

What the host hands to the module per invocation. Heavy decode
runs once host-side; results are shared across all modules whose
Watch matched the same event.

```rust
pub struct DecodeRequest {
    /// Resolve consumed inputs from chain state and supply as
    /// already-decoded `MultiEraOutput`. Solves the
    /// state-applied-before-dispatch issue without exposing it
    /// to module authors.
    pub resolved_inputs: bool,
    /// Inline + witness-set datums, supplied as typed PlutusData.
    pub decoded_datums: bool,
    /// Asset-movement multiset per output, pre-computed.
    pub asset_movements: bool,
    /// Build the classifier-ready `RawTxData` shape so modules
    /// using the marketplace classifier don't pay the assembly
    /// cost wasm-side.
    pub raw_tx_data: bool,
}
```

The host memoises results across modules in a single block: if
five modules ask for `decoded_datums`, decode happens once.

### 3. Host functions (called from inside the module)

Modules occasionally need to read host state (snapshot lookups,
historical UTxO resolution, indexed by-policy queries, etc.).
These cross the wasm boundary on demand.

```rust
// Conceptual signatures — wire format would be guest stubs over
// shared linear-memory + a small RPC envelope.

fn state_get_utxos(refs: &[OutputRef]) -> HashMap<OutputRef, EraCbor>;
fn indexes_utxos_by_policy(policy: &PolicyId) -> Vec<OutputRef>;
fn indexes_utxos_by_address(addr: &Address) -> Vec<OutputRef>;
fn fingerprint(asset: &AssetId) -> Fingerprint;
fn read_cursor() -> ChainPoint;

// Module persistence — replaces redb-per-indexer.
fn get_state(key: &str) -> Option<Vec<u8>>;
fn set_state(key: &str, value: &[u8]);

// Output emission — fan-out target for typed events.
fn emit(change: &[u8]); // CBOR'd per indexer's Change type
```

API design pressure: bulk fetches should be the natural pattern
(`state_get_utxos(refs)` over a slice, not one `get_utxo(ref)` per
call). Boundary-crossing cost compounds; bulk APIs amortise.

### 4. Module persistence

Indexers that maintain state (ownership's `WatchState`, hypothetical
materialised views) persist via the host. Two options:

- **Module-owned linear memory + snapshot/restore.** Module owns
  its state in its own memory; host calls `snapshot() -> bytes` /
  `restore(bytes)` periodically. Simple guest API; opaque blob to
  the host.
- **Host-provided typed KV.** Module calls `set_state(key, bytes)`
  / `get_state(key)`. Host owns the redb / equivalent; module
  pays per-call cost.

Lean toward (a) for simplicity at first. (b) if multiple modules
need to share state (unlikely in practice).

### 5. Control-plane HTTP API (module upload + lifecycle)

Mitos exposes endpoints for module management:

```
POST /_admin/modules                # upload .wasm + IndexerIntent
GET  /_admin/modules                # list registered modules
DELETE /_admin/modules/{name}       # unregister + reclaim state
POST /_admin/modules/{name}/reload  # bump to new wasm artifact
```

Auth: same bearer token surface as today (`MITOS_AUTH_TOKEN`).
Worker `wrangler deploy` integrates by bundling a step that
uploads the module.

## How existing indexers would map

**`OwnershipIndexer`** — naturally fits this model. Watch:
`Output(OutputFilter { policies: <watched set>, .. })`. Decode:
`asset_movements: true`. Per-event transform: emit
`OwnershipChange::Transfer` for each asset under a watched policy.
State: `WatchState` (Empty / Bounded / Unbounded) lives in
module linear memory; host snapshots it.

**`MarketplaceIndexer`** — Watch:
`Tx(TxFilter { references_script: <marketplace contracts>, .. })`.
Decode: `raw_tx_data: true`, `resolved_inputs: true`. Per-event
transform: run classifier rules wasm-side (the classifier is
logic, not parsing — wasm cost is small for this), emit one
`ProtocolEvent` per `(policy, marketplace_event)` pair.

**Future `DexIndexer`** — Watch:
`Tx(TxFilter { references_script: <dex pool addresses>, .. })`.
Decode: `raw_tx_data: true`. Same shape as marketplace.

**Future `JpgListingsIndexer`** — Watch: `Output(OutputFilter {
addresses: <jpg.store CO addresses>, .. })` plus `decoded_datums:
true`. Per-event transform: decode datum, emit listing record.

The pattern repeats: a small `Watch` declaration plus a small
`DecodeRequest` plus a thin transform function. The transform
is where consumer-specific intent lives; everything else is
host-shared work.

## Migration path

Don't try to land this in one PR. Three phases, each useful on
its own:

### Phase A — Stabilise the `Indexer` trait + extract host
*(Trigger: indexer count grows past 3-4, current trait surface
churns less than monthly)*

- Move `mitos-core::Indexer` trait + dispatcher + replicator + bundle
  composition into a `mitos-host` crate published from the mitos
  repo. (Most of this is already true post-Phase-1; just
  formalise the API.)
- Stabilise the trait: no more breaking changes without a
  versioned crate bump.
- Document the trait's contract (already done as
  `INDEXER_TRAIT.md` — keep current).

This phase enables phase B. By itself it does nothing for
deployment ergonomics.

### Phase B — Native-plugin model (no wasm yet)
*(Trigger: cross-repo PR coordination becomes the dominant
friction; we're tired of touching the mitos repo for every
consumer-team indexer)*

- Move indexer crates out of `mitos/crates/` into
  `cnft.dev-workers/indexers/<name>/` (or similar — colocated
  with consumers).
- `bundles/default/main.rs` becomes the manifest: imports each
  external indexer crate via git rev pin, calls `add_indexer`.
- Wrap dispatcher's `handle_event` calls in `catch_unwind` for
  *light* fault isolation (Rust panics caught; not real
  isolation).
- CI builds release binaries (no on-box `cargo build`); deploy
  via SSH copy + symlink swap.
- Adding a new indexer: create the crate in the worker repo,
  bump the rev pin in mitos, add one line to `main.rs`,
  redeploy. Mitos repo touch is trivial; deploy is fast.

This phase delivers colocation + sane deployment. Doesn't deliver
real isolation. Most of the perceived frustration probably
evaporates here without ever needing wasm.

### Phase C — Wasm modules + intent API
*(Trigger: an indexer takes the host down in production; OR we
hit 10+ indexers with diverging deploy needs; OR external
contributors want to ship indexers without write access to our
repos)*

- Design + iterate on the `Watch` grammar (highest-risk design
  call — review every indexer pattern we've encountered).
- Implement native primitives (block traverse, input resolution,
  decode kit) as host-side helpers; expose via `Watch` /
  `DecodeRequest`.
- Spike: reimplement `OwnershipIndexer` against the wasm API.
  Validate the abstractions on the simplest case first.
- Spike: reimplement `MarketplaceIndexer` against the wasm API.
  This is the real test — input resolution + classifier. If it
  feels forced, the API needs work.
- Roll out per indexer; native plugins from phase B can coexist
  with wasm modules indefinitely.
- Add the control-plane HTTP API (`/_admin/modules/*`).
- Worker deploy pipelines: integrate `.wasm` upload into
  `wrangler deploy` flows.

Phase C is the big-ticket work. Avoid until phase B's
limitations bite for real reasons.

## Open questions

- **Watch grammar coverage.** Will `TxFilter { spends_at,
  produces_at, mints, references_script }` be enough for 90%+ of
  indexers? Or do we need `produces_at_with_datum_shape`,
  `transitions_state_at`, etc.? Start narrow; extend on demand.
- **Module discovery / registry.** Where do `.wasm` artefacts
  live? Local filesystem on the box (uploaded via API)? OCI
  registry? GitHub Releases? Each has different operational
  shapes. Start with filesystem-on-box, evolve once we have
  multiple deploy targets.
- **Versioning.** Each module has an ABI version (host
  primitives stable across versions). Mitos host enforces
  compatibility; mismatched modules refuse to load. Need to
  design the version negotiation up front.
- **State migration.** When a module's `Change` type evolves,
  what happens to its persisted state? Same problem as the
  redb-table-name bump in Phase 4. Module-owned state means
  module-owned migration; host can offer a snapshot/clear
  primitive but won't enforce schemas.
- **Module-level resource limits.** Wasmtime supports memory
  caps, fuel-based instruction limits, time budgets. Need
  reasonable defaults so a runaway module is killed not just
  *eventually* but *promptly*. Defaults should be tunable per
  module (high-volume marketplace decode probably needs more
  than a metadata-derivation indexer).
- **Backfill in the new model.** Today indexers populate
  `Vec<Change>` synchronously in `subscribe()`. With wasm,
  backfill enumeration is host-side (`indexes_utxos_by_policy`
  is a host function); the module just transforms each
  enumerated UTxO. Different control flow; needs design.
- **Cross-indexer dependencies.** What if indexer B wants to
  consume indexer A's output stream rather than chain events?
  Current trait doesn't model this; new model could expose it
  cheaply (host fans out internally between modules). Useful for
  e.g. "alert evaluator wants marketplace events filtered to a
  policy set".

## Trigger conditions for picking this work back up

Don't start any of this until at least one of these is true. The
current model is fine until it isn't.

1. **Indexer count > 5.** Current model scales fine to a handful;
   coupling pressure compounds with each.
2. **Cross-repo PR coordination becomes the dominant friction.**
   When the natural answer to "where does this indexer go?" is
   "in someone else's repo", phase B becomes attractive.
3. **An indexer takes the host down in production.** No `catch_unwind`
   workaround is going to be sufficient at that point; phase C
   becomes a real safety requirement, not just an architectural
   nicety.
4. **External contributors want to ship indexers.** Inviting an
   external team to write code that runs in our process raises
   the isolation bar dramatically; wasm is the only sane answer.
5. **Indexer iteration speed becomes the bottleneck on
   product-side velocity.** When teams measure "ship a new
   collection-stats query" in days because of mitos coupling,
   not because the logic is hard, the architecture is wrong for
   the workload.

If none of these are true, the existing single-process
statically-composed bundle is a perfectly good answer — the
architecture is *correct enough* until the org's scale changes
the calculus.

## Lessons banked from current implementation

These inform the design above; worth not relearning.

- **Backfill must respect `change_matches_scope`.** Discovered
  during Phase 7 role-axis rollout — backfill records were
  bypassing the live-tail filter. Fixed in `mitos-core`. Future
  intent API must guarantee filter consistency between backfill
  and live tail by construction.
- **Wire-format changes need explicit versioning.** Phase 4's
  redb table-name bump (`subscriptions` → `subscriptions_v2`)
  was the right call, but ad-hoc. A future model with hot-loaded
  modules needs ABI versioning on the module-host interface as a
  first-class concern.
- **`EnumSet<T>` serialises as a packed integer, not an array.**
  Surprising on the wire. Future intent APIs that take typed
  filter sets will hit the same surface; document or
  custom-serialise to a more readable form.
- **DNS resolver hygiene matters operationally.** Cloudflare
  worker delete/redeploy cycles can leave stale negative-cache
  entries; a future control-plane that pushes modules to mitos
  via HTTP will need to be robust to similar transient resolver
  weirdness, or we're recreating the same class of bug at a
  different layer.
