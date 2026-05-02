# mitos

> μίτος — *thread*. Theseus's thread through the Labyrinth.

A composable framework for building Cardano indexers as Rust modules that share
a single in-process chain data plane.

Each deployment is a **bundle**: a Rust binary that embeds Dolos as a library
plus one or more indexer modules. The bundle runs as a single OS process,
sharing chain state, ledger state, and secondary indexes across modules. Each
module owns its own decoder logic, materialized view, and HTTP endpoints.

The name comes from Greek *μίτος* — the thread Ariadne gave Theseus to find his
way out of the Labyrinth. Each indexer is one thread of meaning pulled from the
chain; a bundle weaves them together into something an application can actually
use. Sits in the txpipe-adjacent Greek-mythology naming neighbourhood
(Dolos / Pallas / Balius / Mithril).

## What problem this solves

Existing Cardano data nodes (Dolos, Dingo, cardano-db-sync) expose a generic
chain-data API. Real applications then layer their own decoder + materialized
view code on top of that API, paying network and serialization costs on every
read. mitos collapses those layers: the chain data plane and the
domain-specific indexers run in the same process, with native function calls
between them and direct access to Dolos's lookup primitives
(`utxos_by_policy`, `plutus_data_by_hash`, etc.) without going through gRPC
or REST.

The architectural rationale is in `docs/design/ARCHITECTURE.md`. The
contract every indexer must implement is in `docs/design/INDEXER_TRAIT.md`.

## Status

**Phase 1 validated end-to-end against a real Dolos data directory.** The
default bundle starts, recovers state + index keyspaces from a snapshot of
the production mainnet data dir, runs the Cardano logic, and dispatches
`TipEvent::Apply` blocks to the `JpgCoIndexer` stub as the chain advances.

In place:

- Workspace structure
- Public `Indexer<D: Domain>` trait + tip-event dispatcher
- `mitos-core::{load_config, setup_domain, spawn_sync_pipeline}` —
  replicates Dolos's `bin/dolos/common.rs` initialization
- A stub `JpgCoIndexer` that logs Apply/Undo/Mark events
- A `default` bundle main that composes Dolos + the stub, runs an axum
  HTTP server, and handles graceful shutdown

Things explicitly NOT done yet (see `docs/design/ROADMAP.md`):

- Real `JpgCoIndexer::bootstrap` (currently returns `ChainPoint::Origin`)
- Apply/Undo logic that actually decodes blocks and writes a materialized view
- HTTP route implementations
- Storage layer for indexer materialized views (redb scaffolded as workspace dep)
- ARM64 cross-compile + deploy via Shiku

## Layout

```
mitos/
├── crates/
│   ├── mitos-core/          # the Indexer trait, dispatcher, common types
│   └── jpg-co-indexer/      # first concrete indexer (jpg.store collection offers)
├── bundles/
│   └── default/             # composite binary: Dolos + chosen indexers
├── docs/
│   └── design/
│       ├── ARCHITECTURE.md  # why this exists, how it composes
│       ├── INDEXER_TRAIT.md # the contract for indexer authors
│       └── ROADMAP.md       # what's done, what's next, in what order
└── README.md (you are here)
```

A bundle's `Cargo.toml` declares which indexer crates it includes. To add an
indexer to a deployment, add a workspace dep + register it in `main.rs`.
Different deployments can be different bundles.

## Building

A `flake.nix` provides the dev shell — same `defrag-nix` `rust-worker-stack`
the wider org uses, so the toolchain is in lock-step with cnft.dev-workers
and similar repos:

```
nix develop -c cargo build                       # build everything
nix develop -c cargo build -p mitos --release    # release binary for deployment
```

If you have cargo on PATH already (e.g. via system rustup), plain
`cargo build` works the same — the flake is convenience, not a hard
requirement.

Dolos crate dependencies are git deps pinned to a specific tag in
`Cargo.toml` (currently `v1.0.3`). First build will resolve and compile
them — this can take a while. Subsequent rebuilds are incremental.

**The pinned tag must match the version of Dolos that wrote the data
directory you're pointing the bundle at.** Dolos's WAL schema is versioned
and a mismatch fails fast with `WAL schema not compatible: found=N
expected=M`. See `docs/design/ROADMAP.md` Phase 1 notes for the full
incident and recovery commands.

## Running

The default bundle expects a Dolos-managed data directory (initialized by
`dolos bootstrap mithril ...` against the same `dolos.toml` config schema). For
local dev:

```
DOLOS_CONFIG=/path/to/dolos.toml cargo run -p mitos
```

The bundle starts the chain-sync pipeline, brings each indexer through
`bootstrap()`, and dispatches `TipEvent`s as the WAL advances. The
`JpgCoIndexer` is currently a stub that just logs events — Phase 2 fills
in real bootstrap, decode, and HTTP routes (see ROADMAP).

The Dolos data directory is an **atomic unit**: WAL, state, and index
must be a consistent snapshot. To clone a running Dolos instance for
mitos experiments, **stop Dolos cleanly** first, then `cp -a` the whole
data dir. Filesystem-level snapshots taken while Dolos is writing will
produce a state that fails to recover.

## Testing

End-to-end recipes for exercising the CF replication path —
protocol-only loop with `mitos-tail`, full mitos↔CF DO round-trip,
and the parallel-run convergence diff against the existing
`collection-ownership` worker — are in
[`docs/TESTING.md`](docs/TESTING.md). Start there once the bundle
builds.

## Related

- `~/code/defrag/cnft.dev-workers/docs/design/CARDANO-SHIKU.md` —
  the broader architecture mitos implements
- `~/code/defrag/cnft.dev-workers/workers/dolos-spike/` —
  validated the chain-data primitives mitos depends on
- `~/code/github/dolos/` — the embedded data plane
