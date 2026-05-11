# Module composition — upstream dependency declaration

**Status:** roadmap item. Not implemented; no concrete design
pinned yet. Captured here so the shape is on file for when a
real use case forces the work.

## The vision

A wasm module declares **upstream dependencies** on other
modules whose events it wants to consume. The host's dispatcher
honours the declaration by:

1. Building a DAG of module dependencies at registration time
2. Running upstream modules first per tx (topological order)
3. Feeding upstream modules' emissions to downstream modules as
   typed events the downstream's `handle-events` consumes

Declaration is either via a **macro** on the module's source
(captured in the module manifest at build time) or via
**configuration** (a `[depends_on]` table in `<name>.toml` that
`mitos-build` parses + writes into the manifest).

## Why this matters

Two scenarios where module composition earns its keep:

### Scenario 1 — single-worker module ordering

A dApp's worker hosts multiple modules with ordering
requirements. Example:

- `co-watcher.rs` — emits `CoCreated/Spent` events for jpg.store
  + wayup CO contracts.
- `co-enricher.rs` — consumes `CoCreated`, fetches floor price
  from another data source, emits `EnrichedCoCreated` with the
  floor-price field added.
- `co-alerter.rs` — consumes `EnrichedCoCreated`, emits
  `AlertFired` when bid ≥ floor × 1.2.

Today the dApp would have to either:
- Collapse all three into one fat module (poor separation of
  concerns)
- Run them as three independent modules and reassemble in the
  companion DO (ordering not guaranteed; companion sees raw
  events from all three)

With dependency declaration, the dispatch ordering is
**explicit** and the wire between modules is **typed**. Each
module stays focused; the composition lives in declarations.

### Scenario 2 — cross-dApp community module reuse

(Composes with the tier model in `LAYERED_RESPONSIBILITIES.md`.)

A community-published `floor-price` module (tier 1) is consumed
by multiple dApps. A new dApp wanting alerts based on floor
price declares dependency on the community module — its own
module receives `FloorPriceUpdated` events ready to consume,
without re-implementing floor-price tracking.

This is the natural endpoint of the wasm-module promotion
pathway: tier-1 community modules become **building blocks**,
not just leaves.

## Declaration shape (sketch)

Macro-based:

```rust
#[mitos::module]
#[depends_on(module = "co-watcher", events = "CoCreated")]
pub fn handle_events(events: Vec<CoCreated>) {
    // ...
}
```

Configuration-based (`<name>.toml`):

```toml
[depends_on]
co-watcher = { events = ["CoCreated"] }
floor-price = { events = ["FloorPriceUpdated"] }
```

The macro is more discoverable in source. The TOML is easier
for operator-time wiring (the deploying operator decides which
upstream module satisfies the dependency). Both are
forward-compat — pick later.

## Architectural pieces this requires

Not blocking until a real use case appears, but worth
enumerating:

1. **Module DAG construction.** At module-load time, resolve
   declared dependencies into a topological order. Cycles =
   load-time error.
2. **Cross-module event delivery.** The host gains a "feed
   module X's emissions into module Y's `handle-events`"
   primitive. WIT contract extension; module Y's
   `Self::Event` is the upstream's emission type.
3. **Typed event contract.** Upstream module's emission type
   has to be name-resolvable from the downstream module's
   build. Likely via a shared Rust crate the upstream
   publishes alongside its module (similar to today's
   `types/jpg-co-events`).
4. **Versioning.** Upstream module updates can break
   downstream consumers. Module manifest carries a version
   range the downstream expects; mismatch at load = clear
   error.
5. **Cycle / fan-in semantics.** A module consumed by N
   downstream modules runs once per tx; its emissions fan
   out. Cycles aren't allowed.

## What this is NOT

- **Not a substitute for in-tree indexers.** Chain primitives
  still belong in-tree (per `LAYERED_RESPONSIBILITIES.md`).
  Module composition is for higher-level
  transformations + aggregations.
- **Not a generic message-passing layer.** The flow is
  per-tx, ordered, typed. Not pub/sub between arbitrary
  components.
- **Not an alternative to consumer-side projection.** The
  companion DO still does state projection + business
  workflow. Module composition is for chain-near
  transformations that benefit from declared ordering.

## Trigger to implement

Likely the second real use case that needs ordering across
modules within one worker, OR the first community module
that earns a downstream consumer. Until then, the existing
patterns (single fat module, or per-module subscription with
companion-side reassembly) are sufficient.

## Cross-references

- `LAYERED_RESPONSIBILITIES.md` — the three-layer split this
  composes on top of; tier-1 community modules are the
  cross-dApp case of this composition idea
- `MITOS_PLATFORM_V2.md` — the wasm-module hosting
  architecture this would extend
- `docs/design/SUBSCRIPTION_MECHANICS.md` — `Interest` is the
  consumer-side filter model; module composition is the
  producer-side ordering model. Complementary, not
  overlapping.
