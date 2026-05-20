# Architecture

> **Status (2026-05): partial rewrite.** The "Indexer trait surface"
> and "How indexers compose in a bundle" sections below describe the
> v1 static-bundle model — the three legacy in-tree indexers
> (`collection-ownership`, `marketplace`, `mint-burn`) and the
> `Indexer` trait sketch retired once consumers cut over to
> platform-v2 wasm community modules. The current dispatch unit is
> the eUTXO event filtered by declared interest, delivered to wasm
> modules via `handle-events`, with CF Worker companions consuming
> module emissions over HTTP. Current authorities:
>
> - `docs/strategy/MITOS_PLATFORM_V2.md` — runtime model + WIT ABI
> - `docs/strategy/MITOS_COMPANION_PATTERN.md` — the host/companion split
> - `docs/strategy/COMMUNITY_MODULES.md` — community-modules-first preference
> - `crates/mitos-platform/wit-v2/world.wit` — exact ABI
> - `bundles/default/src/main.rs` — actual bundle composition (residual
>   `none-match-indexer` + wasm-module hosting)
> - `crates/mitos-core/src/indexer.rs` — the live `Indexer` trait
>
> The "Why embed Dolos as a library," "Why one process per box,"
> "Storage discipline," "Reorg correctness," "Where mitos lives in
> the stack," and "The Dolos coupling" sections remain accurate
> architectural rationale and are kept verbatim.

This is a focused implementation companion to
`~/code/defrag/cnft.dev-workers/docs/design/CARDANO-SHIKU.md` — the broader
architectural rationale lives there. This doc covers the implementation
choices specific to this framework.

## One-paragraph summary

A bundle is a single OS process that links Dolos's chain-follower + state
store + secondary indexes as Rust library code, dispatches every chain
tip event through the platform-v2 eUTXO event composer, and runs N wasm
community modules in-process via wasmtime. Each module declares an
interest set; the platform filters TXs against it and dispatches typed
events (`produced`, `consumed`, `referenced`, `minted`, `tx-context`,
plus `tick` and `rollback` markers) to the module's `handle-events`
export. Module emissions are POSTed to subscribed CF Worker companions
via HTTP. One process per deployment unit, with N wasm modules
contributing functionality and one residual `none-match-indexer` for
asset-movement coverage no specific-domain module claims.

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

## The Indexer trait surface (historical)

The original `Indexer` trait sketch — three methods (`name`,
`bootstrap`, `handle_event`, `routes`) on a `Box<dyn Indexer>` —
described the v1 static-bundle model that has been retired in
favour of wasm community modules. The live `Indexer` trait at
`crates/mitos-core/src/indexer.rs` is the residual surface for the
`none-match-indexer` coordinator and the unified-subscribe bridge;
new chain-recognition code does **not** implement it. See
`INDEXER_TRAIT.md` for the current shape and
`docs/design/DOMAIN_REFACTOR.md` for the model shift.

## How modules compose in a bundle

A bundle's `main.rs` (see `bundles/default/src/main.rs`) is small
and almost entirely composition:

```rust
let domain = mitos_core::setup_domain(&config)?;
let mut bundle = Bundle::new(domain, config, listen, data_dir);

// Residual pass: emits AssetMovement events for asset transfers
// that no specific-domain module claimed. Switches the dispatcher
// to synchronised mode.
let claim_coordinator = bundle.enable_residual_pass();
bundle.add_indexer(NoneMatchIndexer::new(claim_coordinator));

// Wasm-module hosting + community-module auto-load.
bundle.enable_modules(modules_dir);
bundle.enable_community_modules(community_modules_dir);

bundle.run(exit).await?;
```

The chain-sync pipeline feeds the platform's TX-claim coordinator,
which composes each TX into a `DispatchEvent` stream. Each wasm
module's interest predicates are evaluated host-side; matching
events are dispatched to the module's `handle-events` export.
Module emissions accumulate in a per-module `EmissionsStore`,
and a per-module dialer pool POSTs them to subscribed companions
over HTTP. See `docs/design/DIALER_CONCURRENCY.md` for the
parallel-keyed delivery model.

The trait dispatch shape is preserved internally for the residual
`none-match-indexer` and the unified-subscribe bridge — but adding
recognition for a new contract or token shape is now a matter of
adding a wasm module under `community-modules/<name>/` (or in a
dApp's own repo) rather than implementing the `Indexer` trait.

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
