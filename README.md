# mitos

> μίτος — *thread*. Theseus's thread through the Labyrinth.

A composable framework for building Cardano indexers as Rust modules that share
a single in-process chain data plane.

> ## ⚠️ Here be dragons
>
> This repository is **early-stage and under heavy development**. APIs change
> without notice, on-disk formats are not yet stable, the wire protocol is
> evolving frame-by-frame, and there is no commitment to backwards
> compatibility yet. Expect rough edges, half-implemented subsystems, and
> documentation that drifts ahead of the code.
>
> It is open-sourced primarily so that consumers (including the author's CF
> Worker companions) can pin to specific commits and so that the design
> conversation can happen in public. **It is not ready for production use,
> external contributions are not solicited yet, and there are no support
> promises.** If you find this interesting, the design documents in
> [`docs/strategy/`](docs/strategy/) are probably more useful than the code.
>
> ## ⚠️ AI-co-authored
>
> Substantial portions of this repository — code, design documents, commit
> messages, PR descriptions — have been **heavily co-authored with
> [Claude](https://www.anthropic.com/claude)** (Anthropic's LLM, primarily
> via Claude Code) under the human author's direction and review. Designs
> and implementations were iterated through dialogue; Claude both proposed
> and refined the architecture you'll find documented in `docs/`. Decisions
> are still owned and reviewed by the human author, but readers evaluating
> the code or design rationale should know the provenance.

Each deployment is a **bundle**: a Rust binary that embeds Dolos as a library,
hosts one or more indexer modules, and (since Platform v1) can dynamically
load + sandbox additional indexers as wasm components. The bundle runs as a
single OS process, sharing chain state, ledger state, and secondary indexes
across modules. Each module owns its own decoder logic, materialized view, and
HTTP endpoints.

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

The architectural rationale lives in [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md).
The contract every indexer must implement is in
[`docs/design/INDEXER_TRAIT.md`](docs/design/INDEXER_TRAIT.md). The active
workstreams (platform isolation, companion runtime, dApp framework thesis)
live in [`docs/strategy/`](docs/strategy/).

## Status

Active workstreams (most recent first):

- **Companion runtime v1** — CF Worker Durable Object SDK
  (`mitos-companion`) that absorbs ~70% of the boilerplate every
  CF Worker companion currently hand-rolls. Wire types live in
  `mitos-protocol` (no mirror drift); subscribe endpoint in
  `mitos-platform`; runtime SDK in `mitos-companion`. PR 1 of a
  planned 7-PR delivery has landed; PR 2 (dynamic interest) is
  next. Design:
  [`MITOS_COMPANION_RUNTIME_V1.md`](docs/strategy/MITOS_COMPANION_RUNTIME_V1.md).
- **Platform v1** — wasm-isolated module runtime with hot-loadable
  indexers, author-declared trap policies, and resource limits.
  Validated end-to-end against mainnet; ownership-indexer module emits live.
  Design: [`MITOS_PLATFORM_V1.md`](docs/strategy/MITOS_PLATFORM_V1.md),
  deployment story: [`MITOS_PLATFORM_DEPLOYMENT.md`](docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md).
- **CF replication** — Apply/Undo/Mark protocol over WebSocket between mitos
  and Cloudflare Durable Objects. Live in production.
  Design: [`docs/design/CF_REPLICATION.md`](docs/design/CF_REPLICATION.md).

Concrete indexers in the tree:

- `jpg-co-indexer` — collection offers from jpg.store
- `collection-ownership-indexer` — per-policy ownership-change feed (the
  canonical reference indexer)
- `marketplace-indexer` — multi-marketplace event taxonomy

For the longer arc see [`docs/design/ROADMAP.md`](docs/design/ROADMAP.md) and
[`docs/design/MITOS_ISOLATION_ROADMAP.md`](docs/design/MITOS_ISOLATION_ROADMAP.md).

## Layout

```
mitos/
├── crates/
│   ├── mitos-core/                    # Indexer trait, dispatcher, CF replication
│   ├── mitos-protocol/                # framework-free wire types (wire ↔ companions)
│   ├── mitos-data-plane/              # typed chain-data lookups over Dolos
│   ├── mitos-platform/                # wasm module runtime (hot-load, sandbox, supervise)
│   ├── mitos-companion/               # CF Worker DO runtime SDK (companion-side)
│   ├── jpg-co-indexer/                # jpg.store collection offers indexer
│   ├── collection-ownership-indexer/  # per-policy ownership-change feed
│   └── marketplace-indexer/           # multi-marketplace event taxonomy
├── bundles/
│   └── default/                       # composite binary: Dolos + chosen indexers
├── modules/
│   └── ownership-indexer/             # wasm module testing scaffolding
├── tools/
│   ├── mitos-admin/                   # admin HTTP client (deploy modules etc.)
│   ├── mitos-build/                   # builds wasm module artifacts + manifests
│   ├── mitos-tail/                    # observability CLI for the CF replication path
│   ├── capture-block/                 # capture chain blocks for tests
│   └── diff-collection-ownership/     # parallel-run convergence diff harness
└── docs/
    ├── design/                        # contract docs (ARCHITECTURE, ROADMAP, etc.)
    └── strategy/                      # active-workstream design docs
```

A bundle's `Cargo.toml` declares which indexer crates it includes. Different
deployments can be different bundles. As of Platform v1, indexers can also be
loaded as wasm modules at runtime via `mitos-admin` rather than baked into the
bundle binary.

## Building

A `flake.nix` provides the dev shell. Same toolchain as the rest of the org:

```sh
nix develop -c cargo build                       # build everything
nix develop -c cargo build -p mitos --release    # release binary for deployment
```

If you have cargo on PATH already (e.g. via system rustup), plain
`cargo build` works the same — the flake is convenience, not a hard
requirement.

Dolos crate dependencies are git-pinned to a specific tag in `Cargo.toml`
(currently `v1.0.3`). First build will resolve and compile them — this
takes a while. Subsequent rebuilds are incremental.

**The pinned Dolos tag must match the version that wrote the data
directory you're pointing the bundle at.** Dolos's WAL schema is versioned
and a mismatch fails fast with `WAL schema not compatible: found=N
expected=M`. See [`docs/design/ROADMAP.md`](docs/design/ROADMAP.md) Phase 1
notes for the full incident and recovery commands.

## Running

The default bundle expects a Dolos-managed data directory (initialized by
`dolos bootstrap mithril ...` against the same `dolos.toml` config schema):

```sh
DOLOS_CONFIG=/path/to/dolos.toml cargo run -p mitos
```

The bundle starts the chain-sync pipeline, brings each indexer through
`bootstrap()`, and dispatches `TipEvent`s as the WAL advances.

The Dolos data directory is an **atomic unit**: WAL, state, and index
must be a consistent snapshot. To clone a running Dolos instance for
mitos experiments, **stop Dolos cleanly** first, then `cp -a` the whole
data dir. Filesystem-level snapshots taken while Dolos is writing will
produce a state that fails to recover.

## Testing

End-to-end recipes for exercising the CF replication path —
protocol-only loop with `mitos-tail`, full mitos↔CF DO round-trip,
and the parallel-run convergence diff against an existing CF
Worker indexer — are in [`docs/TESTING.md`](docs/TESTING.md).

## License

Apache-2.0 — same license Dolos ships under, picked deliberately to keep
things aligned with the embedded data plane. See [`LICENSE`](LICENSE).

## Design documents

If you want to understand mitos rather than run it, read in this order:

1. [`docs/strategy/CARDANO_DAPP_FRAMEWORK_THESIS.md`](docs/strategy/CARDANO_DAPP_FRAMEWORK_THESIS.md) — the why.
2. [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md) — the how, at the bundle level.
3. [`docs/strategy/MITOS_PLATFORM_V1.md`](docs/strategy/MITOS_PLATFORM_V1.md) — the wasm-module runtime.
4. [`docs/strategy/MITOS_COMPANION_PATTERN.md`](docs/strategy/MITOS_COMPANION_PATTERN.md) — the paired-deployable thesis.
5. [`docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`](docs/strategy/MITOS_COMPANION_RUNTIME_V1.md) — the CF-side SDK.
6. [`docs/design/INDEXER_TRAIT.md`](docs/design/INDEXER_TRAIT.md) — the contract for indexer authors.
7. [`docs/design/CF_REPLICATION.md`](docs/design/CF_REPLICATION.md) — the WS protocol.
