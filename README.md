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

Skeleton. The pieces in place:

- Workspace structure
- Public `Indexer<D: Domain>` trait
- Tip-event dispatcher signature
- A stub `JpgCoIndexer` that just logs events
- A `default` bundle main that wires Dolos + the stub together (Dolos init
  is currently a TODO — see `bundles/default/src/main.rs`)

Things explicitly NOT done yet (see `docs/design/ROADMAP.md`):

- Actual `setup_domain` wiring (need to replicate `dolos/src/bin/dolos/common.rs`'s logic)
- Storage layer for indexer materialized views (redb scaffolded as workspace dep)
- Bootstrap implementations (use `domain.indexes()` + `domain.state()`)
- HTTP route implementations
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

Standard cargo. Pinned to Rust toolchain via `rust-toolchain.toml` (TODO).

```
cargo build                       # build everything
cargo build -p mitos --release    # release binary for deployment
```

Dolos crate dependencies are git deps from `txpipe/dolos@v1.1.0`. First build
will resolve and compile them — this can take a while. Subsequent rebuilds are
incremental.

## Running

The default bundle expects a Dolos-managed data directory (initialized by
`dolos bootstrap mithril ...` against the same `dolos.toml` config schema). For
local dev:

```
DOLOS_CONFIG=/path/to/dolos.toml cargo run -p mitos
```

This is currently a stub — actual chain-event flow is gated on the
`setup_domain` TODO in `bundles/default/src/main.rs`. See ROADMAP for the
sequence.

## Related

- `~/code/defrag/cnft.dev-workers/docs/design/CARDANO-SHIKU.md` —
  the broader architecture mitos implements
- `~/code/defrag/cnft.dev-workers/workers/dolos-spike/` —
  validated the chain-data primitives mitos depends on
- `~/code/github/dolos/` — the embedded data plane
