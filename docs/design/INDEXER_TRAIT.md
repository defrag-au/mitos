# Indexer trait contract

> **Status: stale trait sketch.** The trait signature documented
> in this file is from an earlier iteration. The actual
> `Indexer<D: Domain>` trait lives in
> `crates/mitos-core/src/indexer.rs` and is generic over
> `Domain`, with associated `Scope` / `Change` types,
> `subscribe` / `unsubscribe` / `change_matches_scope` /
> `is_internal` methods, and `handle_event → Vec<MovementClaim>`
> (returns claims for the residual-pass coordinator —
> `docs/design/DOMAIN_REFACTOR.md`). The 3-method trait sketched
> below doesn't match what any in-tree indexer implements.
>
> **Authoritative source:** `crates/mitos-core/src/indexer.rs`.
>
> **For new indexers**, follow the community-modules pattern —
> see `../strategy/COMMUNITY_MODULES.md` and
> `../HOWTO_CONSUMING_A_COMMUNITY_MODULE.md`. The in-tree trait
> documented (poorly) here is grandfathered: the three legacy
> indexers (`collection-ownership-indexer`,
> `marketplace-indexer`, `mint-burn-indexer`) retired in 2026-05
> in favour of community wasm modules. Only `none-match-indexer`
> still implements this trait — it stays as the dispatcher's
> residual-pass coordinator. See
> `../strategy/LAYERED_RESPONSIBILITIES.md` for the layering
> rationale.

> **Original framing (preserved):** this doc covers the
> in-process Rust trait used by indexers compiled directly into a
> mitos bundle binary. The wasm-module shape — now canonical for
> new work — uses the WIT contract at
> `crates/mitos-platform/wit-v2/world.wit` (v2 ABI) consumed via
> `wit_bindgen::generate!` and built with `mitos-build`. The two
> shapes share concepts (idempotent dispatch, bootstrap,
> Apply/Undo/Mark, scope-as-Interest)
> but differ in surface — wasm modules export `init` /
> `handle-event` / `update-interest` over WIT instead of
> implementing this Rust trait, and they emit events to the host
> via `emit::emit-event(channel, cbor)` rather than mounting
> their own HTTP routes (the companion DO owns the RPC surface).
>
> The worked examples below (`JpgCoIndexer`, mounted-routes,
> `Indexer::new(config)` constructors) are illustrative of the
> static-crate shape; reach for the wasm-module / companion shape
> first.

The `Indexer` trait is the entire framework-side surface an indexer module
must implement. This doc is the contract: what's expected, what's
guaranteed, what's optional.

## The trait

```rust
use async_trait::async_trait;
use dolos_core::{ChainPoint, Domain, TipEvent};

#[async_trait]
pub trait Indexer: Send + Sync {
    /// Stable identifier. Used for log scoping, storage path naming,
    /// and route prefixes by convention. Must be valid as a filesystem
    /// directory name.
    fn name(&self) -> &'static str;

    /// One-time pull of current chain state into the indexer's
    /// materialized view at startup. Called before any chain events
    /// are dispatched. Returns the chain point we caught up to —
    /// the dispatcher will start streaming events from this point.
    async fn bootstrap(&mut self, domain: &dyn Domain) -> anyhow::Result<ChainPoint>;

    /// Single chain event. The dispatcher calls this for every
    /// subscribed event in order. Implementations MUST be idempotent
    /// against re-delivery (see "Idempotency" below).
    async fn handle_event(
        &mut self,
        domain: &dyn Domain,
        event: &TipEvent,
    ) -> anyhow::Result<()>;

    /// HTTP routes this indexer exposes. The bundle merges all
    /// indexers' routes under a shared axum::Router.
    fn routes(&self) -> axum::Router;
}
```

## Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│  1. Indexer::new(...)                                        │
│     - construct from config                                  │
│     - open materialized-view storage                         │
│     - prepare for bootstrap (NOT chain queries here)         │
├─────────────────────────────────────────────────────────────┤
│  2. bootstrap(domain) -> ChainPoint                          │
│     - query domain.indexes().utxos_by_X(...) for current     │
│       state at watched addresses/policies                    │
│     - decode datums (inline OR via domain.query()            │
│       .plutus_data(hash))                                    │
│     - upsert into materialized view (atomic per-UTxO)        │
│     - return current chain tip                               │
├─────────────────────────────────────────────────────────────┤
│  3. Dispatcher subscribes via domain.watch_tip(returned point)│
│     - per-indexer tokio task starts                          │
├─────────────────────────────────────────────────────────────┤
│  4. handle_event(domain, ev) loop, forever:                  │
│     - TipEvent::Apply(point, block): forward, apply deltas   │
│     - TipEvent::Undo(point, block):  reverse same block      │
│     - TipEvent::Mark(point):         optional cursor save    │
├─────────────────────────────────────────────────────────────┤
│  5. routes() merged into bundle's HTTP server                │
│     - usually a sub-Router under "/<name>/..."               │
└─────────────────────────────────────────────────────────────┘
```

## Idempotency requirement

`handle_event` may be called more than once for the same event during
recovery (process restart with stale cursor, dispatcher retry on
transient error). Implementations MUST tolerate re-delivery without
corrupting state.

Concrete discipline:

- **Inserts**: `INSERT ... ON CONFLICT (chain_determined_pk) DO UPDATE`
  (UPSERT), keyed on something derivable from the block (e.g. TxoRef =
  `(tx_hash, output_index)`).
- **Deletes**: `DELETE WHERE pk = ?` — natural no-op on missing row.
- **Cursor updates**: monotonic compare-and-swap; never go backward.
- **Aggregates**: derive on read via `SELECT SUM(...)`, do not maintain
  incrementally-updated counters.
- **Atomicity**: all writes for one event in a single storage transaction.

This invariant is what makes the framework's recovery, bootstrap, and
parallel-run-migration semantics work. See
`CARDANO-SHIKU.md` for the broader rationale.

## What `bootstrap` actually does

Bootstrap pulls current chain state into the indexer's view. The pattern:

```rust
async fn bootstrap(&mut self, domain: &dyn Domain) -> Result<ChainPoint> {
    // 1. enumerate UTxOs of interest from Dolos's secondary indexes
    let txo_refs = domain.indexes().utxos_by_address(&self.contract_addr).await?;

    // 2. hydrate the actual UTxOs from state
    let utxos = domain.state().get_utxos(txo_refs.into_iter().collect()).await?;

    // 3. decode + materialize each one
    for (txo_ref, output) in utxos {
        let datum = match output.datum() {
            Some(InlineDatum(plutus)) => plutus,
            Some(DatumHash(h)) => domain.query().plutus_data(&h).await?
                .ok_or(/* missing datum */)?,
            None => continue,
        };
        let decoded = self.decode_datum(&datum)?;
        self.store.upsert(&txo_ref, &decoded)?;
    }

    // 4. return where we are
    let point = domain.archive().get_tip().await?;
    Ok(point)
}
```

In-process; no gRPC; no subrequest limits; no MiniBF round-trips.

## What `handle_event` does for each variant

### `TipEvent::Apply(ChainPoint, RawBlock)`

Forward block. The indexer should:

1. Parse the block CBOR via `pallas_traverse::MultiEraBlock::decode(&block)`
2. For each transaction:
   - For each output: if at a watched address, extract datum (inline or
     resolved against the TX's witness set — hash-referenced datums
     attached as witnesses are in the SAME block, no Dolos lookup needed)
   - For each consumed input: if it was something we'd previously stored,
     remove it
3. Atomically apply both produces and consumes to the materialized view
4. Optionally: save cursor (or wait for next Mark)

### `TipEvent::Undo(ChainPoint, RawBlock)`

Reverse the previously-applied block. The block CBOR is the SAME bytes
that were previously delivered as Apply, so reversing is symmetric:

1. Parse the same block
2. For each output we previously stored: remove from view
3. For each input we previously removed: re-add (the consumed UTxO's
   datum is still recoverable via `domain.query().plutus_data(&hash)`
   if needed, since Dolos retains it in the archive)
4. Atomically commit the reversal

### `TipEvent::Mark(ChainPoint)`

Checkpoint signal. The indexer can persist its cursor here; the
dispatcher won't deliver events with chain points before the last Mark
the indexer acknowledged on a future restart. Indexers that persist
cursor on every Apply can ignore Mark.

## Storage conventions

The framework recommends each indexer:

- Open its storage under `<bundle-data-dir>/indexers/<name>/`
- Use `redb` (workspace dep) for embedded KV unless there's a reason
  not to
- Maintain a separate `cursor` table with the last successfully-applied
  ChainPoint, advanced atomically with each event's deltas

Other choices are fine if the indexer makes them deliberately. The
framework doesn't enforce a storage backend; the trait only requires
the lifecycle behaviour.

## What the trait deliberately doesn't include

- **No `shutdown` hook** — graceful shutdown is the bundle's concern;
  storage durability is the indexer's concern via its own write
  semantics.
- **No `health` method** — bundle aggregates cursor lag directly;
  indexers expose their cursor via routes().
- **No reorg-depth limits** — Dolos handles rollback semantics; indexers
  just process Apply/Undo events.
- **No event filtering** — every indexer sees every event. Filtering is
  the indexer's job (typically by inspecting addresses in transaction
  outputs/inputs).

## Routes convention

By convention, an indexer's routes are mounted under `/{name}/...`:

```rust
fn routes(&self) -> axum::Router {
    Router::new()
        .route("/by-creator/{pkh}", get(self.handle_by_creator))
        .route("/by-policy/{policy}", get(self.handle_by_policy))
        // ...
        .with_state(self.store.clone())
}
```

The bundle adds the `/{name}/` prefix when merging:

```rust
let app = indexers.iter().fold(Router::new(), |r, ix| {
    r.nest(&format!("/{}", ix.name()), ix.routes())
});
```

So a `JpgCoIndexer` with name `"jpg-co"` exposes `/jpg-co/by-creator/{pkh}`.

## Constructor convention

The trait doesn't dictate a constructor. By convention, indexers offer:

```rust
impl JpgCoIndexer {
    pub fn new(config: &Config) -> anyhow::Result<Self> { ... }
}
```

The bundle's `main.rs` constructs each indexer it includes from the
shared bundle config. Indexer-specific config can be a sub-section of
the bundle config or a separate file — the bundle decides.
