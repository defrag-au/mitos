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
- **Community modules.** Nineteen wasm modules ship in `community-modules/`
  — jpg.store **and Wayup** (listing / offer / sale each), CIP-25 / CIP-68
  mints, CSWAP + Splash DEXes, holder distribution, collection holders +
  metadata, asset movement, vesting, credit and burn taxonomies. Loadable by
  any bundle, addressable from any companion by name. Design:
  [`COMMUNITY_MODULES.md`](docs/strategy/COMMUNITY_MODULES.md).
- **Companion runtime v1.** CF Worker Durable Object SDK
  (`mitos-companion`) absorbing the per-companion subscribe / HTTP
  apply-and-recapture delivery / emission-id / multi-client identity
  boilerplate. Production consumers: `jpg-store-mirror`,
  `collections-mitos`. Design:
  [`MITOS_COMPANION_RUNTIME_V1.md`](docs/strategy/MITOS_COMPANION_RUNTIME_V1.md).
- **HTTP companion delivery.** Per-emission HTTP POST to
  `/_internal/apply-<channel>` / `/_internal/recapture-<channel>`
  on the companion, with 200 / 422 / 5xx mapping to Ack / Nack /
  transport-retry. Replaced the original WebSocket / Hibernation
  surface. Per-companion partition-keyed lane pool keeps ordering
  within a key while parallelising across keys
  ([`DIALER_CONCURRENCY.md`](docs/design/DIALER_CONCURRENCY.md)).
- **Multi-client companion identity.** Companion records keyed by
  `(module_id, client_id, companion_key)` so two consumers sharing
  the same `companion_key` (e.g. dev + prod) get independent
  emission streams. Design:
  [`MULTI_CLIENT_COMPANIONS.md`](docs/design/MULTI_CLIENT_COMPANIONS.md).
- **Recapture v1.** Coordinated state rebuild — host signals each
  subscribed companion to drop projected state, then re-runs the module's
  bootstrap (re-entrant via the `rebootstrap` export) into a clean
  target. Replaces the manual multi-step reset. Design:
  [`RECAPTURE.md`](docs/design/RECAPTURE.md) +
  [`WASM_BUDGET_CHUNKING.md`](docs/design/WASM_BUDGET_CHUNKING.md).
- **Tiered indexer-data cache + Maestro fallback.** Hash-addressable
  chain facts cached permanently in `<storage_root>/indexer_data.redb`
  (two namespaces: TX aux-data CBOR keyed by tx_hash, Plutus datum CBOR
  keyed by datum_hash). aux-data is populated proactively from live
  blocks and written through on archive hits; both namespaces resolve
  lazily via Maestro on a local miss — aux-data for TXs older than the
  Dolos archive horizon, datums for hash-only CIP-68 reference datums
  whose preimage never landed in Dolos's `DATUM_NS` (the snapshot-gap).
  The Maestro tier is rate-limit-aware (process-wide semaphore,
  `Retry-After`-respecting backoff). Lets bootstrap resolve years-old
  TXs and datums the local node can't.
- **Minibf bridge.** Blockfrost-compatible HTTP surface from Dolos's
  `dolos_minibf` router mounted at `/minibf` on the bundle, gated by
  the bundle's shared auth middleware. Lets consumers query the
  underlying chain data over Blockfrost-shaped endpoints without
  taking a Maestro dependency. Design:
  [`MINIBF_BRIDGE.md`](docs/design/MINIBF_BRIDGE.md).

For the longer arc see [`docs/design/ROADMAP.md`](docs/design/ROADMAP.md) and
[`docs/strategy/MODULE_COMPOSITION.md`](docs/strategy/MODULE_COMPOSITION.md)
(upstream-module dependencies — roadmap, not built).

## ⚠️ Two families live here

The repository holds **two kinds of program that share almost nothing but a
workspace and a pallas pin.** Knowing which one you are looking at is the first
thing to establish, because the conventions differ completely.

| | **A — the bundle** | **B — snapshot walkers** |
|---|---|---|
| what it is | one long-running process | one-shot / on-demand binaries |
| chain source | **Dolos**, live, following the tip | **Mithril immutable chunk files**, certified, offline |
| unit of work | a wasm module reacting to an eUTXO event | a walk over a slot range |
| output | events pushed to a CF companion | Parquet / sqlite / a memory-mapped index |
| lives in | `bundles/`, `community-modules/`, `crates/mitos-*` | `tools/{token,market,project}-ledger`, `wallet-*`, `*-index` |
| entry point | `docs/design/ARCHITECTURE.md` | each tool's own `README.md` |

Family B does **not** load the wasm runtime, subscribe to anything, or talk to
Dolos. It reads chunk files. If you are editing a walker and find yourself
reaching for `mitos-platform`, you are in the wrong half.

They meet in exactly two places: the shared **decode crates**
(`mitos-dex-decode`, `mitos-marketplace-decode`, `mitos-vesting-decode`,
`mitos-launchpad-decode`) — a datum is a datum whoever is reading it — and the
`pallas` version pin.

## Layout

```
mitos/
├── bundles/default/               # A: composite binary — Dolos + Platform v2 runtime
├── community-modules/             # A: 13 wasm modules loaded at bundle startup
└── docs/{design,strategy}/        # contract docs and active-workstream docs
```

### `crates/` — libraries

**A — the framework**

| crate | what |
|---|---|
| `mitos-platform` | wasm module runtime (v2 dispatch, aux-data cache, Maestro fallback, dialer). The largest thing in the repo. |
| `mitos-core` | dispatcher, `Bundle`, replicate-router test surface, in-tree `Indexer` trait |
| `mitos-data-plane` | typed chain-state queries over Dolos |
| `mitos-protocol` | framework-free wire types (subscribe, interest, frames) |
| `mitos-companion` | CF Worker Durable Object SDK — the *companion* side |
| `mitos-community-events` | typed event payloads modules emit |
| `mitos-module-kit` | module-author helpers (budget limiter, re-entrant chunking) |
| `none-match-indexer` | residual pass for movements no specific domain claimed |

**B — walkers, indexes and archives**

| crate | what |
|---|---|
| `chain-sieve` | SIMD substring search over raw chunk bytes; CBOR decode only on hit. The scanning machine every "needle in 225 GB" tool shares. |
| `tx-index` | tx hash → `(chunk, body offset)`. Input resolution in one `pread`. |
| `policy-index` | `(policy, asset)` → its mint tx **and its metadata span**. Rides tx-index's extraction pass. |
| `policy-archive` | the sealed Parquet archive of one policy's movements — writer, reader, and the tiers over it |
| `token-ledger-wire` / `market-ledger-wire` | wire formats those tools' consumers decode |

**Shared by both — decode**

| crate | what |
|---|---|
| `mitos-dex-decode` | CSwap / Splash / Minswap pool + order datums and credentials |
| `mitos-marketplace-decode` | jpg.store / Wayup redeemers and listing datums |
| `mitos-vesting-decode` | Shield / CrowdLock vesting datums |
| `mitos-launchpad-decode` | snek.fun bonding curves — a launch is not a constant-product pool |
| `mitos-cohort` | what KIND of holder an address is (burn / pool / vesting / script / wallet) and how firmly that is known |
| `mitos-pool-observe` | read a DEX pool's reserves the way that venue actually publishes them |
| `mitos-koios` | the Koios calls this workspace makes, in one place, plus the first-mint FLOOR rule |

### `tools/` — binaries

**B — snapshot walkers and their indexes**

| tool | what | README |
|---|---|---|
| `token-ledger` | one policy's movements → Parquet archive, on demand; serves a live correcting feed | [→](tools/token-ledger/README.md) |
| `market-ledger` | listings / offers / sales across marketplaces into one slot-keyed ledger | [→](tools/market-ledger/README.md) |
| `project-ledger` | one project's mint window: capital in, holders forming, capital out | [→](tools/project-ledger/README.md) |
| `wallet-sieve` | on-demand single-wallet flow excavation | [→](tools/wallet-sieve/README.md) |
| `wallet-trace` | wallet clustering from vkey witness sets | [→](tools/wallet-trace/README.md) |
| `tx-index` | build + serve the tx-hash index | [→](tools/tx-index/README.md) |
| `mitos-chain-walk` | *library*, not a binary: the shared walker plumbing (bootstrap, chunk iteration, bare-pallas decode, checkpoint mirror) | [→](tools/mitos-chain-walk/README.md) |

**A — framework tooling**

| tool | what | README |
|---|---|---|
| `mitos-admin` | admin HTTP client (modules, recapture, emissions, companions) | [→](tools/mitos-admin/README.md) |
| `mitos-build` | builds wasm module artifacts + manifests | [→](tools/mitos-build/README.md) |
| `mitos-run` | fixture-driven local module runner — no Dolos, no host | [→](tools/mitos-run/README.md) |
| `mitos-tail` | WS CBOR client for the `/replicate/{indexer}` test surface | |
| `capture-block` | capture chain blocks as test fixtures | |
| `diff-collection-ownership` | parallel-run convergence diff harness | |

The default bundle links the family-A crates and runs the Platform v2 host;
modules are *not* baked into the binary. Each community module is built into a
wasm artifact + manifest by `mitos-build` and loaded via the platform registry
at host startup.

> **Where the documentation actually is.** Crates without a `README.md` are
> documented by their `//!` module header, which is kept current because it
> sits next to the code — start there, not with a search. The design rationale
> for anything substantial lives in `docs/design/` or `docs/strategy/`, and the
> analytical walkers' designs live in the sibling `cnft.dev-workers/docs/design/`
> repo (`TX_INDEX.md`, `POLICY_INDEX.md`, `POLICY_ARCHIVE_*.md`,
> `MARKET_LEDGER.md`, …).

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
(currently **`v1.2.0`** — `Cargo.toml` is the authority, and its comment
carries the reasoning for the pin). First build will resolve and compile them;
this takes a while, and subsequent rebuilds are incremental.

**The pinned Dolos tag must match the version that wrote the data
directory you're pointing the bundle at.** Dolos's WAL schema is versioned
and a mismatch fails fast with `WAL schema not compatible: found=N
expected=M`. See [`docs/design/ROADMAP.md`](docs/design/ROADMAP.md) Phase 1
notes for the full incident and recovery commands.

⚠️ **v1.2.0 versions the WAL on-disk schema and force-resets on
incompatibility**, so expect a Dolos **resync** on first run after the bump.
Two other pins are load-bearing and documented in `Cargo.toml`: one `pallas`
across mitos and the embedded Dolos (they share types at the follower
boundary), and a `utxorpc-spec` held at 0.19.0 — 0.19.2 made
`asset_name`/`policy_id` optional within the 0.19 line and will not compile
Dolos, so a broad `cargo update` can break the build.

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

The indexer-data cache at `<storage_root>/indexer_data.redb` (aux-data
+ datums) is independent of the Dolos directory and persists across
bundle restarts. An older `aux_data.redb` is migrated in place on first
open. Setting `MAESTRO_API_KEY` enables the Maestro resolution tier (TXs
older than the Dolos archive horizon, plus snapshot-gapped datums);
`MAESTRO_MAX_INFLIGHT` (default 4) caps process-wide concurrent Maestro
requests.

## Testing

End-to-end recipes for exercising a running host + community
modules + a companion — bring-up of a bundle with wasm-module
hosting, end-to-end mitos → CF companion delivery, forced
recapture — are in [`docs/TESTING.md`](docs/TESTING.md).

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
  — companion-side trait surface, `client_id` + `on_recapture`
  hooks, multi-target subscribe, HTTP apply / recapture delivery.
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
8. [`docs/design/RECAPTURE.md`](docs/design/RECAPTURE.md) — coordinated state rebuild.
9. [`docs/design/WASM_BUDGET_CHUNKING.md`](docs/design/WASM_BUDGET_CHUNKING.md) — re-entrant `rebootstrap` + chunked snapshot emission for large policies.
10. [`docs/design/DIALER_CONCURRENCY.md`](docs/design/DIALER_CONCURRENCY.md) — partition-keyed parallel HTTP delivery.
11. [`docs/design/DOMAIN_REFACTOR.md`](docs/design/DOMAIN_REFACTOR.md) — the Mint / Burn / AssetMovement domain taxonomy + synchronised-dispatcher rationale.
12. [`docs/design/CF_REPLICATION.md`](docs/design/CF_REPLICATION.md) — original WS replication wire shapes (historical; superseded by HTTP delivery, retained as protocol reference).
13. [`docs/strategy/MODULE_COMPOSITION.md`](docs/strategy/MODULE_COMPOSITION.md) — upstream-module-dependency roadmap item.
