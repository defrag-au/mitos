# Architecture

This is a focused implementation companion to
`~/code/defrag/cnft.dev-workers/docs/design/CARDANO-SHIKU.md` — the broader
architectural rationale lives there. This doc covers the implementation
choices specific to this framework.

## One-paragraph summary

A bundle is a single OS process that links Dolos's chain-follower + state
store + secondary indexes as Rust library code, and dispatches every chain
tip event (Apply, Undo, Mark) to one or more `Indexer` trait implementations
linked into the same binary. Each indexer owns its own decoder, materialized
view, and HTTP routes; all indexers share the chain data plane via direct
function calls into Dolos's `Domain` trait. The result is one process per
deployment unit, with N modules contributing functionality.

## Why embed Dolos as a library

Three alternatives were considered:

| Option | Verdict |
|---|---|
| Two processes (Dolos + sidecar service via gRPC) | Pays gRPC cost on every query; constrained by Dolos's API gaps (`Query.ReadData` is `todo!()`, no SearchUtxos pagination); doubles process count |
| Dolos + N2C tail to rebuild own state | Discards Dolos's already-derived current state; reinvents the chain-follower |
| Embed Dolos as library + add our own routes | One process, direct trait access to all of Dolos's internals, no API translation |

Embedding wins because it gives us **everything Dolos has already done** at
function-call speed, with no API boundary to fight.

The cost: we depend on `dolos-core` and `dolos-cardano` crate APIs which
don't have semver guarantees. Each Dolos release is a deliberate upgrade
decision, not a passive dep bump. Tag-pinned in workspace Cargo.toml.

## Why one process per box, not multiple

Naive plugin model: deploy each indexer as its own service, each running
its own embedded Dolos. Operationally absurd — every box runs N copies of
the chain follower for the same chain.

Right answer: **one chain data plane per box, multiple indexer modules
sharing it via the trait dispatcher.** The data plane is the expensive
shared resource; the indexer logic is the cheap per-domain code.

This is the same observation that motivated Balius (`txpipe/balius`):
applications are made of multiple business-logic units that all need
chain data. Balius solves it with WASM modules sandboxed inside a daemon.
We solve it with native Rust modules linked at compile time. The choice
points at trust: Balius supports untrusted third-party modules; this
framework is for trusted first-party code where sandboxing isn't the
constraint.

## The Indexer trait surface

Three methods. Specifics in `INDEXER_TRAIT.md`.

```rust
trait Indexer: Send + Sync {
    fn name(&self) -> &'static str;

    /// One-time pull of current state at startup.
    /// Returns the chain point we caught up to.
    async fn bootstrap(&mut self, domain: &Domain) -> Result<ChainPoint>;

    /// Apply or roll back a single block, or update cursor on Mark.
    async fn handle_event(&mut self, domain: &Domain, event: &TipEvent) -> Result<()>;

    /// HTTP routes the bundle should mount for this indexer.
    fn routes(&self) -> axum::Router;
}
```

Backed by:

- **Bootstrap**: `domain.indexes().utxos_by_policy(&p)`,
  `domain.state().get_utxos(refs)`, `domain.query().plutus_data(&hash)` —
  all in-process, no gRPC.
- **Apply**: `TipEvent::Apply(ChainPoint, RawBlock)` carries the full
  block CBOR. Indexers parse via `pallas-traverse`, find outputs at
  watched addresses, decode datums (inline OR from witness set —
  hash-referenced datums are in the same TX). No round-trip to Dolos.
- **Undo**: `TipEvent::Undo(ChainPoint, RawBlock)` carries the rolled-back
  block — we have the inverse operation directly. No op-log needed at
  the framework level.
- **Mark**: cursor checkpoint. Each indexer persists its position on its
  own cadence.

## How indexers compose in a bundle

A bundle's `main.rs` looks roughly like:

```rust
let domain = setup_domain(&config)?;          // Dolos init

let mut indexers: Vec<Box<dyn Indexer>> = vec![
    Box::new(JpgCoIndexer::new(&config)?),
    Box::new(JpgListingsIndexer::new(&config)?),
    // ... other indexers this bundle includes
];

// bootstrap each
for ix in &mut indexers {
    let from = ix.bootstrap(&domain).await?;
    // spawn its event loop
    spawn_dispatcher(ix.box_clone(), domain.watch_tip(Some(from))?);
}

// merge HTTP surfaces
let app = indexers.iter().fold(Router::new(), |r, ix| r.merge(ix.routes()));
axum::serve(listener, app).await
```

The dispatcher is a tokio task per indexer that loops on
`TipSubscription::next_tip().await`, calls `ix.handle_event(&domain, &ev)`,
logs errors, retries forever.

Per-indexer subscriptions are cheap because Dolos's `DomainAdapter` uses
a `tokio::sync::broadcast::Sender<TipEvent>` internally — every
subscriber gets every event independently, with the broadcast queue depth
being the only shared resource.

## Storage discipline per indexer

Each indexer's materialized view is its own. Default storage is `redb`
in a per-indexer subdirectory (`<bundle-data-dir>/indexers/<name>/`).
The storage layout, schema, and migration policy are the indexer's concern,
not the framework's.

The `CARDANO-SHIKU.md` invariant about idempotent writes still applies —
each indexer must use `INSERT ... ON CONFLICT DO UPDATE` (UPSERT), monotonic
cursor advance, derived-not-counted aggregates. Dolos's `TipEvent` flow
includes Mark/Undo events that re-deliver state during recovery, so
non-idempotent writes will silently corrupt.

## Reorg correctness

Dolos's `TipEvent::Undo(point, block)` carries the rolled-back block, so
indexers have the exact CBOR that was previously applied. Reversing it is
a deterministic walk over the same blocks they applied earlier.

This means the "operation log" pattern from `CARDANO-SHIKU.md` is not
strictly necessary at the framework level — Undo's full-block payload
plays that role. Individual indexers may still want their own op-logs if
their materialized view derives information that isn't recoverable from
the block alone (e.g. cumulative counters), but the simple case is
clean: same block goes through the indexer's apply/undo logic in reverse.

## Schema migrations across bundles

Per `CARDANO-SHIKU.md`, breaking schema changes to an indexer's view are
handled by **parallel-run re-snapshot** at the bundle level: deploy
bundle v2 alongside v1, both following chain independently, then swap
traffic when v2's indexers are caught up. The framework supports this
cleanly because:

- Each bundle owns its own data directory
- Each indexer's materialized view is keyed on chain-determined IDs
  (deterministic across bundles)
- Bootstrap is idempotent — a fresh bundle catches up from chain
- The HTTP API surface is identical between bundle versions (modulo
  the migrated schema)

The indexer trait is stable across bundle versions; only the indexer's
storage schema changes. The chain data plane (Dolos) is shared in the
sense of "same upstream chain" but each bundle has its own Dolos data
directory; no cross-bundle storage sharing.

## Where mitos lives in the stack

Mitos is **not** the runtime substrate for our dApps. Our dApps live on
Cloudflare Workers — that's the always-on, multi-region, billed-per-
request layer that user traffic hits. Mitos runs on budget VPSes
co-located with chain infrastructure; it can be down for maintenance,
restarted for a Dolos version bump, or migrated between boxes without
the dApp going dark.

What mitos contributes is a **projection of the chain** — the subset of
on-chain state an app actually needs, decoded and materialized into a
shape the app can query cheaply. The goal is for that projection to
flow into Cloudflare (Durable Objects, D1, KV — whichever fits the
access pattern) so that the dApp's hot path never has to reach back to
the VPS.

The natural analogue is a CouchDB-style document store with a
replication protocol: each indexer's materialized view is a "database",
and a CF Durable Object subscribes to changes and applies them
locally. The crucial difference is the **change cursor**: in CouchDB
it's a per-database monotonic sequence number; for mitos the natural
cursor is the Cardano `(slot, block_hash)` pair, since that's the unit
the chain itself rolls forward and back on. A replicating consumer
that knows its last-applied `(slot, hash)` can:

- ask mitos for everything since that point, and
- handle reorgs by recognizing when mitos's history diverges from the
  consumer's last-known hash, and rolling back to the fork point.

The shape of that protocol — long-poll HTTP, SSE, WebSocket, signed
snapshot bundles, something else — is parked until Phase 2 produces
a real materialized view we'd want to replicate. The architectural
commitment now is just that **mitos's HTTP surface is designed to be
replicated, not just queried**: every indexer's view should be
expressible as a stream of `(slot, hash, change)` records, with
`Apply` and `Undo` as the two change kinds, mirroring the `TipEvent`
contract one level up.

This framing also explains why mitos doesn't try to be highly available
on its own — it's the upstream of a replication tree, not the serving
layer. HA at the VPS layer would be solving the wrong problem; HA at
the CF layer is what the platform already gives us for free.

## The Dolos coupling

Embedding Dolos as a library means mitos inherits two real operational
constraints. Worth naming explicitly so they don't surprise anyone
later.

**Version coupling at the WAL schema level.** Dolos versions its WAL
schema and refuses to recover a data dir written by a different schema
version. Concretely: a mitos build pinned to `dolos = { tag = "v1.0.3" }`
will not start against a data dir written by `dolos v1.1.0`, and vice
versa. Each workspace bump is a deliberate decision involving (a)
recompiling mitos, and (b) ensuring the data dir mitos points at was
written by a compatible Dolos. There is no online upgrade. This is
acceptable because the chain follower is the deployed binary; we
control the rebuild cadence.

**Data dir is an atomic unit.** WAL + state + index must be a
consistent snapshot. Filesystem-level snapshotting only works while
Dolos is fully stopped — concurrent writes during a copy produce a
state mitos refuses to bootstrap on, with a `state` cursor ahead of the
`archive`/WAL cursor. The deploy story (Phase 3) inherits this: bundle
parallel-run for schema migrations means each bundle owns its own
Dolos data dir and bootstraps from Mithril independently, not by
copying from a sibling.

`dolos doctor reset-wal` exists as a recovery tool for small WAL/state
divergences, but treat it as a backstop, not a routine workaround. The
ROADMAP records the empirical incident this came out of.

## Where Shiku fits

`Shiku` (the deploy tool from `~/code/defrag/augminted-bots/shiku/`)
deploys this bundle as a single managed app. The unit of deploy is the
bundle binary; Shiku's atomic activation handles the parallel-run +
swap pattern. See ROADMAP for the deploy story.

The cursor-aware health check from `CARDANO-SHIKU.md` is genuinely
needed at the bundle level: "healthy" means "all indexers are caught up
to chain tip." A bundle's `/health` endpoint should aggregate each
indexer's cursor lag.
