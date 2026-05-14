# mitos

> μίτος — *thread*. Theseus's thread through the Labyrinth.

A composable framework for building Cardano indexers as wasm modules that share
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
hosts the Platform v2 wasm-module runtime, and dynamically loads community
modules at startup. The bundle runs as a single OS process, sharing chain
state, ledger state, and secondary indexes across modules. Each module owns
its own decoder logic, materialized view, and HTTP endpoints, with state
isolated behind the WIT-defined `mitos:platform-v2` ABI.

The name comes from Greek *μίτος* — the thread Ariadne gave Theseus to find his
way out of the Labyrinth. Each module is one thread of meaning pulled from the
chain; a bundle weaves them together into something an application can actually
use. Sits in the txpipe-adjacent Greek-mythology naming neighbourhood
(Dolos / Pallas / Balius / Mithril).

## What problem this solves

Existing Cardano data nodes (Dolos, Dingo, cardano-db-sync) expose a generic
chain-data API. Real applications then layer their own decoder + materialized
view code on top of that API, paying network and serialization costs on every
read. mitos collapses those layers: the chain data plane and the
domain-specific modules run in the same process, with native function calls
between them and direct access to Dolos's lookup primitives
(`utxos_by_policy`, `plutus_data_by_hash`, etc.) without going through gRPC
or REST.

The architectural rationale lives in [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md).
The contract every wasm module implements is defined by the WIT world in
`crates/mitos-platform/wit-v2/world.wit` and documented in
[`docs/strategy/MITOS_PLATFORM_V2.md`](docs/strategy/MITOS_PLATFORM_V2.md).
The active workstreams (community modules, paired-deployable companions,
dApp framework thesis) live in [`docs/strategy/`](docs/strategy/).

## Status

Shipped + in production:

- **Platform v2 — eUTXO event dispatch.** Wasm-isolated module runtime where
  the dispatch unit is the filtered eUTXO event, not the raw block.
  Bootstrap, backfill, and tip dispatch all flow through one path; modules
  see no distinction. The v1 block-CBOR path retired May 2026 once both
  production modules had migrated.
  Design: [`MITOS_PLATFORM_V2.md`](docs/strategy/MITOS_PLATFORM_V2.md);
  deployment story: [`MITOS_PLATFORM_DEPLOYMENT.md`](docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md).
- **Community modules.** Thirteen wasm modules ship in `community-modules/`
  — jpg.store (listing / offer / sale), CIP-25 / CIP-68 mints, CSWAP +
  Splash DEXes, holder distribution, asset movement, vesting, burn
  taxonomies. Loadable by any bundle, addressable from any companion by
  name. Design:
  [`COMMUNITY_MODULES.md`](docs/strategy/COMMUNITY_MODULES.md).
- **Companion runtime v1.** CF Worker Durable Object SDK
  (`mitos-companion`) absorbing the per-companion subscribe / WS
  Hibernation / emission-id / recapture-hook boilerplate. Production
  consumers: `jpg-store-mirror`, `collections-mitos`. Design:
  [`MITOS_COMPANION_RUNTIME_V1.md`](docs/strategy/MITOS_COMPANION_RUNTIME_V1.md).
- **Recapture v1.** Coordinated state rebuild — host signals each
  subscribed companion to drop projected state, then re-runs the module's
  bootstrap into a clean target. Replaces the manual multi-step reset.
  Design: [`RECAPTURE.md`](docs/design/RECAPTURE.md).
- **Tiered aux-data cache + Maestro fallback.** TX aux-data CBOR cached
  permanently in `<storage_root>/aux_data.redb` — populated proactively
  from live blocks, written through on archive hits, and resolved lazily
  via Maestro when bootstrap walks TXs older than the Dolos archive
  horizon. The Maestro tier is rate-limit-aware (process-wide semaphore,
  `Retry-After`-respecting backoff). Lets bootstrap resolve years-old
  TXs the local archive has pruned.
- **CF replication.** Apply/Undo/Mark protocol over WebSocket between
  mitos and Cloudflare Durable Objects. Live in production. Design:
  [`CF_REPLICATION.md`](docs/design/CF_REPLICATION.md).

For the longer arc see [`docs/design/ROADMAP.md`](docs/design/ROADMAP.md) and
[`docs/strategy/MODULE_COMPOSITION.md`](docs/strategy/MODULE_COMPOSITION.md)
(upstream-module dependencies — roadmap, not built).

## Layout

```
mitos/
├── crates/
│   ├── mitos-core/                # dispatcher, CF replication, in-tree indexer trait
│   ├── mitos-protocol/            # framework-free wire types (wire ↔ companions)
│   ├── mitos-data-plane/          # typed chain-data lookups over Dolos
│   ├── mitos-platform/            # wasm module runtime (v2 dispatch, aux-data cache, Maestro fallback)
│   ├── mitos-companion/           # CF Worker DO runtime SDK (companion-side)
│   ├── mitos-community-events/    # shared event types for community modules
│   └── none-match-indexer/        # residual-pass coordinator for the synchronised dispatcher
├── community-modules/             # wasm modules loaded at bundle startup
│   ├── asset-metadata-update/
│   ├── asset-transfer/
│   ├── burn-address/
│   ├── cip-25-mint/
│   ├── cip-68-mint/
│   ├── cswap-dex/
│   ├── holder-distribution/
│   ├── jpg-store-listing/
│   ├── jpg-store-offer/
│   ├── jpg-store-sale/
│   ├── splash-dex/
│   ├── standard-burn/
│   └── vesting-tracker/
├── bundles/
│   └── default/                   # composite binary: Dolos + Platform v2 runtime
├── tools/
│   ├── mitos-admin/               # admin HTTP client (`health`; legacy subscribe routes retired)
│   ├── mitos-build/               # builds wasm module artifacts + manifests
│   ├── mitos-run/                 # local fixture-driven module test runner
│   ├── mitos-tail/                # observability CLI for the CF replication path
│   ├── capture-block/             # capture chain blocks for tests
│   └── diff-collection-ownership/ # parallel-run convergence diff harness
└── docs/
    ├── design/                    # contract docs (ARCHITECTURE, RECAPTURE, ROADMAP, …)
    └── strategy/                  # active-workstream design docs
```

The default bundle links the framework crates and runs the Platform v2 host;
modules are *not* baked into the binary. Each community module is built into a
wasm artifact + manifest by `mitos-build` and loaded via the platform registry
at host startup.

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

The bundle starts the chain-sync pipeline, loads each community module
through Platform v2's registry, runs `init()` on every module to dispatch
the synthetic-event bootstrap scan, and then dispatches eUTXO events as
the WAL advances.

The Dolos data directory is an **atomic unit**: WAL, state, and index
must be a consistent snapshot. To clone a running Dolos instance for
mitos experiments, **stop Dolos cleanly** first, then `cp -a` the whole
data dir. Filesystem-level snapshots taken while Dolos is writing will
produce a state that fails to recover.

The aux-data cache at `<storage_root>/aux_data.redb` is independent of
the Dolos directory and persists across bundle restarts. Setting
`MAESTRO_API_KEY` enables the third resolution tier for TXs older than
the Dolos archive horizon; `MAESTRO_MAX_INFLIGHT` (default 4) caps
process-wide concurrent Maestro requests.

## Testing

End-to-end recipes for exercising the CF replication path —
protocol-only loop with `mitos-tail`, full mitos↔CF DO round-trip,
and the parallel-run convergence diff against an existing CF
Worker indexer — are in [`docs/TESTING.md`](docs/TESTING.md).

Local module-level testing without the production host or a Dolos
snapshot uses `mitos-run` against fixture-driven inputs — see
[`docs/HOWTO_TESTING_COMMUNITY_MODULES.md`](docs/HOWTO_TESTING_COMMUNITY_MODULES.md).

## License

Apache-2.0 — same license Dolos ships under, picked deliberately to keep
things aligned with the embedded data plane. See [`LICENSE`](LICENSE).

## Design documents

If you want to **build a community module** on this stack today,
start here:

- [`docs/HOWTO_FIRST_MODULE.md`](docs/HOWTO_FIRST_MODULE.md) — end-to-end
  walkthrough using current tooling (`mitos-build`, `mitos-admin`,
  `mitos-companion`).
- [`docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md`](docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md)
  — companion-side trait surface, `on_recapture` hook, multi-target
  subscribe, WS Hibernation + emission-id semantics.
- [`docs/HOWTO_TESTING_COMMUNITY_MODULES.md`](docs/HOWTO_TESTING_COMMUNITY_MODULES.md)
  — fixture-driven local runs via `mitos-run`.
- [`docs/HOWTO_DEBUG_TRAPS.md`](docs/HOWTO_DEBUG_TRAPS.md) /
  [`docs/HOWTO_DEBUGGING_DEPLOYED_MODULES.md`](docs/HOWTO_DEBUGGING_DEPLOYED_MODULES.md)
  — what to do when a module traps locally or in production.
- [`docs/design/MITOS_BUILD.md`](docs/design/MITOS_BUILD.md) — TOML schema,
  materialisation rules, manifest format for the single-file-module build tool.

If you want to **understand mitos** rather than run it, read in this order:

1. [`docs/strategy/CARDANO_DAPP_FRAMEWORK_THESIS.md`](docs/strategy/CARDANO_DAPP_FRAMEWORK_THESIS.md) — the why.
2. [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md) — the how, at the bundle level.
3. [`docs/strategy/MITOS_PLATFORM_V2.md`](docs/strategy/MITOS_PLATFORM_V2.md) — the wasm runtime + eUTXO event dispatch model.
4. [`docs/strategy/COMMUNITY_MODULES.md`](docs/strategy/COMMUNITY_MODULES.md) — where chain-recognition logic should live.
5. [`docs/strategy/LAYERED_RESPONSIBILITIES.md`](docs/strategy/LAYERED_RESPONSIBILITIES.md) — worker vs community module vs in-tree crate.
6. [`docs/strategy/MITOS_COMPANION_PATTERN.md`](docs/strategy/MITOS_COMPANION_PATTERN.md) — the paired-deployable thesis.
7. [`docs/strategy/MITOS_COMPANION_RUNTIME_V1.md`](docs/strategy/MITOS_COMPANION_RUNTIME_V1.md) — the CF-side SDK.
8. [`docs/design/CF_REPLICATION.md`](docs/design/CF_REPLICATION.md) — the WS protocol.
9. [`docs/design/RECAPTURE.md`](docs/design/RECAPTURE.md) — coordinated state rebuild.
10. [`docs/design/DOMAIN_REFACTOR.md`](docs/design/DOMAIN_REFACTOR.md) — the Mint / Burn / AssetMovement domain taxonomy + synchronised-dispatcher rationale.
11. [`docs/strategy/MODULE_COMPOSITION.md`](docs/strategy/MODULE_COMPOSITION.md) — upstream-module-dependency roadmap item.
