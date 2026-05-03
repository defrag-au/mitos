# Cardano dApp framework — thesis

A unified, opinionated, **Rust-everywhere** stack for building
Cardano dApps on Cloudflare-native infrastructure. **This is a
thesis document, not an implementation roadmap** — it captures
the strategic framing so the direction is recoverable when the
work is actually picked up. Specifically captures *why* the gap
matters, *what* would close it, and *when* it's worth committing
to.

The defining commitment: **Rust through the whole off-chain
pipeline. Aiken on-chain. Frontend is bring-your-own.** No
TypeScript / JavaScript anywhere in the framework's own surface;
consumers are free to ship any frontend that consumes the
Rust-backed RPC surface (Rust → wasm via Leptos / Dioxus / Yew is
the natural fit, but TS / Swift / Kotlin / anything that speaks
the RPC works).

This is a deliberate audience choice. Most existing Cardano
"dApp framework" attempts target JS/TS devs; the framework
imagined here serves Rust-fluent devs explicitly. The dev pool is
smaller but the value prop is sharper, the stack is more
cohesive, and the existing Cardano Rust ecosystem (pallas, dolos,
mitos, balius's runtime, the entire shared-crates substrate) is
genuinely production-ready by mid-2026.

Mitos and the indexer-replication work are one component of the
framework imagined here, not the whole thing. This doc is
deliberately scoped wider than the architectural docs and isn't
trying to design any individual piece — those decisions belong in
their own design docs as the work progresses.

Cross-references:
- `../design/MITOS_DATA_PLANE_API.md` — data plane API design
  (one component of this framework)
- `../design/MITOS_ISOLATION_ROADMAP.md` — wasm host fn surface
  for indexer modules (relevant if framework includes
  consumer-colocated indexer logic)
- `../design/SUBSCRIPTION_MECHANICS.md` — Interest selectors
  (consumer-side filter abstraction this framework would expose
  to dApp builders)

## Empirical grounding

This thesis isn't speculative. It's grounded in 18 months of
shipping Cardano dApps on a Cloudflare-native stack — cnft.dev,
PFP City, the various per-collection workers, the rewards/auth
flows. What's worked, what's hurt:

**What's worked well:**

- **Cloudflare Workers + Durable Objects + R2** as a backend,
  with **Workers running Rust → wasm32 via `worker-rs`**.
  Eliminates VM ops, gives stateful per-tenant coordinators
  that traditional Cardano stacks fake with Postgres + crons.
  Cost-efficient at low volume, scales transparently. The
  cnft.dev stack is **already Rust-end-to-end on the
  worker side** — this thesis makes that explicit.
- **The shared-crates Rust ecosystem** (cardano-assets,
  cardano-tx, datum-parsing, transactions, tx-classifier,
  pallas-addresses) running cleanly on wasm32 inside CF
  Workers. Pallas as a stack runs on wasm; the off-chain Rust
  story is genuinely viable in 2026 in a way it wasn't in
  2023.
- **Aiken** for on-chain logic. Survey shows >75% of Cardano
  devs use it; lived experience confirms it's the right
  default. CIP-57 blueprint emission is built in.
- **Custom indexers** (cnft.dev-workers/indexers/* + the
  classifier worker pattern) when paired with a chain-data
  source. The shape works.
- **CIP-30 wallet integration** is sound; the standard works,
  the implementations interoperate. *(Note: CIP-30 is the one
  unavoidable JS bridge — wallets are browser extensions.
  The framework's surface stops at "produce signed-tx CBOR";
  the wallet handoff itself uses whatever the consumer's
  frontend chooses, JS or wasm-bindgen interop.)*

**What's hurt — the gaps that motivate this thesis:**

- **No opinionated chain-data plane.** Started with Oura +
  classifier-fed events; the path was workable but every
  team building anything similar would hit the same setup
  problems. Mitos exists because the alternative was every
  team running their own follower.
- **No unified pattern for "subscribe to chain → store →
  serve."** Each indexer/worker pair is hand-wired —
  schemas, replication paths, DO routing all bespoke. The
  collections-mitos worker is an instance of a pattern; the
  pattern is recoverable but not packaged.
- **Tx building is split between Rust workers and JS-shaped
  off-chain libs (Lucid / Mesh).** This is exactly the
  friction the Rust-everywhere pivot eliminates. cnft.dev
  already builds txs in Rust via the shared-crates
  cardano-tx pipeline; the friction comes from anywhere we've
  had to bridge to a JS/TS toolchain (frontend tx flows that
  hand off to Lucid). Owning the entire tx-building pipeline
  in Rust collapses this gap entirely.
- **Auth is bespoke every time.** CIP-8 sign-in to a
  worker-side session is a solved problem in principle but
  rebuilt per app in practice. No "auth ships with the
  template" experience.
- **Contract type-safety is hand-rolled.** Aiken produces
  CIP-57 blueprints; no Rust toolchain generates typed Rust
  bindings from them in an opinionated way. Each app retypes
  its contract surface — datum / redeemer shapes,
  parameters, asset constraints — in Rust by hand. This is
  exactly the Anchor-equivalent we're missing.
- **Operational orchestration patterns** (multi-step tx flows,
  retries, idempotency, drift-detection between chain and
  worker state) are reinvented per project. There's no
  Cardano-specific equivalent of "background job framework".

The pattern: every individual layer has at least one good
implementation. The integration glue does not exist.

## The thesis

> A Rust-fluent Cardano dApp builder shouldn't have to integrate
> six Cardano-specific tools to ship something. The framework
> should provide a unified, opinionated, Rust-everywhere stack
> that covers the common pieces and lets teams focus on their
> actual product logic.

Phrased as a positioning statement:

> If `cargo cardano init` produced a working Cargo workspace
> with Rust-on-Workers backend + chain-data subscription + DO
> state + Rust-side CIP-30 helpers + CIP-8 auth + typed Aiken
> bindings (Rust types, not TS), in one command, **the median
> Rust-fluent dev could ship a Cardano dApp end-to-end** without
> ever touching a JS/TS toolchain on the framework's surface.

The benchmark to clear: **Anchor on Solana**. New Solana dev
sits down, runs `anchor init`, has a working program + client
+ test in an hour — all Rust. We're proposing the same shape
for Cardano: Aiken on-chain (the typed-language analog of
Solana's Move/Anchor program), Rust everywhere off-chain. Same
audience profile, same integration ergonomics, same opinionation
discipline. Cardano forces a six-tool tasting menu before line
one; this framework would force one menu choice (the framework
itself) and zero further integration work for the common cases.

The audience is deliberately narrow: **Rust-fluent devs who
want to ship Cardano dApps**. JS/TS-fluent devs are well-served
by Mesh and Lucid Evolution; we're not chasing them. The Rust
audience is currently underserved (Balius narrowed scope; no
opinionated stack exists) and the Cardano Rust ecosystem
(pallas / dolos / mitos / shared-crates) is mature enough by
mid-2026 to actually build on.

## Why the gap exists

Not for lack of effort. Multiple projects have tried to fill it
and either narrowed scope or stalled. Concrete examples worth
learning from:

**Balius (txpipe, 2023–present).** Originally pitched as a
"headless dApp framework" — see the open 2024 issues on the repo
covering domain events, dashboard UI, blueprint integration, tx
signing interface. None of those issues got resolved. What
shipped is a competent WASM workers runtime that one or two
Catalyst-funded enterprise teams use for tx orchestration. The
framework half got quietly retired in favour of a narrower
runtime.

**use-cardano starter** (2022–early 2024). Next.js + Lucid
template. Useful as a starting point; failed because a template
without an opinionated runtime underneath it isn't a framework —
the dev still had to wire everything up.

**DAB / ledger-sync.** Data-plane plays without dApp context.
Cardano Foundation explicitly stopped pursuing both. Lesson:
data plane without an app shape doesn't grab adoption.

**Hollow** (Balius's predecessor name). Same shape, same outcome.

The pattern: **Cardano keeps shipping layers. Nobody ships the
integration.** Each project that tried picked one of:

1. Span the whole stack from first principles → couldn't ship
   because the surface was too big (Balius, Hollow)
2. Ship a slice and hope teams compose → composition didn't
   happen because the slice was infrastructure not product (DAB,
   ledger-sync)
3. Provide a template → broken because the underlying runtime
   wasn't opinionated (use-cardano)

The right answer is (4): **extract the integration patterns from
a working vertical app**, not design them in vacuum. cnft.dev's
existing stack — across the various workers we've shipped — is
the substrate.

## Why now

Three signals say this is the right moment to ship a unified
framework, not earlier and not much later:

1. **Foundation is funding `cardano-init`** — IO Developer
   Experience Initiative, late 2025, governance-backed. They've
   named the gap publicly and are funding work to address it.
   Cardano leadership sees this as a top-tier problem.
2. **The Foundation's own 2025 Developer Survey** (109 devs)
   identified "fragmented tooling, scattered documentation,
   lack of coordination" as the #1 reason devs abandon the
   ecosystem. The thesis isn't fringe — it's mainstream.
3. **The on-chain layer has converged.** Aiken won, CIP-57
   blueprints work, governance is settling, Hydra v2 has alpha
   shipping. The off-chain / app layer is the active frontier
   for the next 12–24 months.
4. **The Cardano Rust ecosystem has matured enough to build on.**
   Pallas (v1.0+), Dolos (v1.1+), Balius's runtime, the
   shared-crates substrate, mitos itself — all production-ready
   Rust by mid-2026 and all wasm32-compatible where it matters.
   This wasn't true 18 months ago. The Rust-everywhere thesis
   is feasible *now* in a way it explicitly wasn't earlier.

If the Foundation/IO team ships `cardano-init` first as a
TS-default toolkit (almost certain — the Foundation's audience
skews TS), this framework's Rust-everywhere positioning becomes
*more* differentiated, not less. The two don't compete on
audience. Six months still matter — but the differentiation is
the Rust commitment, not the CF deploy target alone.

## Positioning

What the framework is:

- **Rust-everywhere off-chain.** Worker code in Rust → wasm32.
  Indexer modules in Rust. Tx-building in Rust (cardano-tx /
  pallas-tx evolutions). Aiken-blueprint codegen produces
  typed *Rust* bindings. Shared types compose across the
  whole pipeline because they're the same language. No JS/TS
  in the framework's own surface.
- **Aiken on-chain.** Typed-language analog of Solana's
  Anchor-Move pairing.
- **Frontend is bring-your-own.** The framework's surface
  stops at the RPC boundary it exposes from Workers. Frontends
  consume that — Rust → wasm via Leptos / Dioxus / Yew is the
  natural fit (same language, shared types, native wasm-bindgen
  CIP-30 interop), but TS / Swift / Kotlin / mobile clients
  work fine if they speak the RPC. The framework doesn't ship
  a frontend template by default. **Wizards / scaffolding can
  optionally add a Rust-wasm frontend** as an opt-in flag, but
  there's no TS template anywhere in the framework.
- **Opinionated.** One choice per layer where there's a
  meaningful choice. Aiken on-chain. Rust on Workers via
  worker-rs. UTxO-RPC over gRPC for chain data (mitos-served
  locally, Demeter-served managed). cardano-tx (or its
  evolution) for tx building. The opinionation is the feature.
- **Cardano-first, CF-as-deploy-target.** The framework's
  audience is "Rust-fluent dev who wants to ship a Cardano
  dApp", not "CF dev who wants to do Cardano". CF is how it
  deploys; Cardano is what it does.
- **Vertical-slice MVP, expand from there.** First version
  ships exactly enough to build one real-world dApp shape end
  to end. Generalises later, *after* the slice has external
  users.
- **Extract, don't design.** The patterns come from cnft.dev's
  existing stack, where they've been validated by ~18 months
  of production use. New abstractions are last resort.

What the framework is not:

- **Not a node operator.** Doesn't run the Cardano node;
  doesn't replace Demeter or Maestro. Mitos consumes from a
  Cardano data source; the framework consumes from mitos.
- **Not a competitor to Aiken.** Builds on top of it.
- **Not a JS/TS toolkit.** Mesh and Lucid Evolution are good
  for what they do; this framework explicitly serves the
  audience they don't. Zero TypeScript in the framework's
  own code or templates.
- **Not an L1 protocol experiment.** No new on-chain
  primitives. The framework is integration glue + scaffolding
  + opinions; the on-chain layer is whatever Aiken produces.
- **Not multi-platform out of the gate.** Cloudflare-native
  first. If the framework succeeds, "deploy to Linux VPS"
  comes later — but optimising for that case from day one
  guarantees no opinionation.

The opinionation is the feature. Anchor's killer trait isn't
tech — it's that it's *the default*. New Solana devs don't have
to choose. The Rust-everywhere commitment is a sharper version
of that — the framework absorbs every "but what about X?"
question by saying *"X is great, this framework just isn't for
that workflow"*. That clarity is what makes opinionation work.

## What composes vs. needs building

**Compose, ready to use as-is:**

| Layer | Choice | Notes |
|---|---|---|
| On-chain language | Aiken | Survey winner; uncontested |
| Chain-data plane | mitos (self-host) or Demeter (managed) | Both serve UTxO-RPC; same Rust client code works against either |
| Tx building (off-chain) | shared-crates `cardano-tx` (Rust) | Already production-tested in cnft.dev; pallas-tx underneath. wasm32-clean. |
| Asset / address typing | shared-crates `cardano-assets`, `pallas-addresses` | Typed PolicyId, AssetId, Fingerprint, Address newtypes; CIP-14 baked in |
| Datum decoding | shared-crates `datum-parsing` + pallas | Typed PlutusData, server-side resolution |
| Backend runtime | Cloudflare Workers + DOs (via `worker-rs`) | Rust → wasm32; production-stable, ergonomic |
| Storage | DO SQLite + R2 | Already proven on cnft.dev |
| Wallet bridge | CIP-30 (browser only — frontend's responsibility) | Framework emits signed-tx-ready CBOR; frontend handles wallet handoff via wasm-bindgen interop or whatever |
| On-chain ↔ blueprint | Aiken's built-in CIP-57 emitter | Existing pipeline |

**Needs new building (the framework's actual contribution):**

| Surface | What's missing |
|---|---|
| `cargo cardano init` (or similar) | Opinionated scaffolding generator producing a Cargo workspace: contracts/ + worker/ + indexer/ + shared-types/, optionally + frontend/ |
| Typed Aiken client codegen | CIP-57 blueprint → typed Rust bindings. The Anchor analog. Produces shared types worker / frontend / tests all consume. |
| Tx-template macro / DSL | Rust macro / typed builder for tx templates: `tx_template!(claim, { params }) → CBOR + descriptor`. Worker-side, callable from frontend via RPC. |
| CIP-8 sign-in pattern | CF Worker primitive for wallet-bound session: nonce challenge → CIP-8 signature verification → JWT/cookie → DO-bound session. Same primitive every dApp would otherwise rebuild. |
| Worker subscription DSL | Declarative "subscribe to chain X, store as Y, serve as Z" — extraction of the cnft.dev collections-mitos pattern as a packaged abstraction |
| DO state primitives | Per-tenant chain-state coordinator (cnft.dev's collections-mitos pattern, packaged as a reusable crate) |
| Notification primitives | Discord/email/push as a first-class verb. cnft.dev's DISPATCH worker pattern, generalised. |
| RPC surface | Worker-exposed RPC the frontend talks to. Typed Rust client (for Rust→wasm consumers) sharing types with the worker via the `shared/` crate, plus a JSON-RPC adapter for non-Rust frontends. |
| Dev tooling | Local mitos + dev wallet + worker dev server in one command. `cargo cardano dev` |

The new-build pieces are mostly **integration glue, Rust-side
codegen, and opinionated wiring**. None of them require novel
on-chain primitives. Most are ~weeks of focused work each, made
substantially easier by the Rust-everywhere commitment — shared
types between worker / indexer / contract / (Rust) frontend
collapse the integration surface dramatically.

## The MVP vertical slice

Smallest possible end-to-end demonstration that the framework
holds together. From `cargo cardano init my-collection-page`
to "live dApp" in <30 minutes:

1. **Scaffolding generator** produces an opinionated Cargo
   workspace:
   - `contracts/` — Aiken project pre-seeded with a minimal
     parameterised validator (e.g. claim-with-signature),
     CIP-57 blueprint emission wired up
   - `shared/` — Rust crate for shared types between worker /
     indexer / (optional) Rust frontend. Includes
     blueprint-codegen output (typed Rust bindings for
     contract datums / redeemers / parameters).
   - `worker/` — Rust → wasm32 CF Worker via worker-rs, with
     chain subscription stub, CIP-8 sign-in primitive, RPC
     surface stub. Builds with `wrangler deploy`.
   - `indexer/` — Rust indexer module colocated with the
     worker (or, in the longer-term wasm-isolation roadmap,
     a separate compilable wasm module). Subscribes to mitos
     for the relevant policy.
   - (Optional, behind `--frontend rust`) `frontend/` — Leptos
     starter with CIP-30 wasm-bindgen interop scaffolding +
     pre-wired RPC client to the worker.
   - Workspace `Cargo.toml` + `aiken.toml` + `wrangler.toml`
     correctly cross-wired so `cargo cardano dev` runs the
     full local stack and `cargo cardano deploy` ships to CF.
2. **Dev command** (`cargo cardano dev`) runs locally:
   - Mitos against testnet (or shared dev mitos), serving the
     worker
   - Worker in `wrangler dev` (Rust → wasm32 build path)
   - Optional frontend in Trunk hot-reload (if `--frontend rust`)
     or detached (if bring-your-own)
   - All connected; tx-building against the dev wallet
3. **One demonstration flow** wired end-to-end:
   - Frontend (or curl): connect wallet (CIP-30 wasm-bindgen
     interop), call `claim` RPC on worker
   - Worker: typed param check from Aiken blueprint (Rust
     types — same crate the frontend uses), build tx via
     `cardano-tx`, return CBOR
   - Frontend: pass CBOR to wallet for signing, submit signed
     CBOR back to worker
   - Worker: submit to chain via mitos's tx-submission proxy
     (or Demeter), store optimistically in DO
   - Mitos indexer: detect confirmation, replicate to worker
     DO; client polls or RPCs for confirmed state
   - All steps observable in dev tooling
4. **Deploy command** (`cargo cardano deploy`) ships to CF +
   sets up a managed mitos subscription (or the team's own
   self-hosted mitos).

If `cargo cardano init` produces this in one command and the dev
walks through the demonstration successfully — **all in Rust on
the off-chain side, Aiken on-chain, frontend optional** — the
framework exists. Everything else is iteration on top.

## The Cloudflare + Rust bet — opportunity and risk

**Opportunity:**

- **Rust → wasm32 on Workers** via worker-rs is genuinely
  ergonomic by mid-2026, well-supported, and the model the
  cnft.dev stack already uses end-to-end. No translation
  layer, no JS shim, no CSL build pain.
- UTxO-RPC over gRPC fits Workers natively (binary,
  stateless, edge-friendly) far better than Blockfrost REST
  polling fits a traditional VPS stack. Rust gRPC clients
  are first-class.
- Durable Objects give per-customer/per-app stateful
  coordinators with no operations overhead — the abstraction
  Cardano dApps repeatedly fake with Postgres + cron
- R2 is the right place for per-asset off-chain artifacts
  (images, metadata, snapshots) at near-zero cost
- Cost profile is exceptional at low volume — many Cardano
  apps live in the long tail where CF's free/low tier is
  generous and a VPS is overkill
- **Pallas + shared-crates is wasm32-clean.** The off-chain
  Rust ecosystem we'd build on already runs on Workers.
- The 18 months of cnft.dev experience is on this stack;
  every pattern is already validated against real apps
- No public framework exists in the Cardano ↔ CF ↔ Rust
  intersection. Triple-greenfield positioning.

**Risk:**

- **The dev pool is small.** Rust + Cardano + CF Workers is
  a three-way intersection. Each axis individually narrows
  the audience. Adoption depends on the value prop being
  sharp enough that Rust devs reach across to learn Cardano
  — same dynamic that worked for Anchor on Solana.
- **No frontend ecosystem to plug into out of the box.**
  Bring-your-own-frontend is the right call but means we
  don't ship the "click here, see app" experience that
  Mesh/Lucid templates do. Mitigation: ship excellent worker
  RPC examples and one optional Leptos starter.
- CF's model isn't suitable for every dApp shape (long-running
  background jobs, large compute, certain operational
  patterns). Trying to force-fit limits the framework's reach.
- If IO's `cardano-init` ships first with a TS default
  (overwhelmingly likely), it will become "the" Cardano
  scaffolding for the median dev. This framework is
  differentiated by the Rust commitment, not competing for
  the same audience — but visibility / SEO / community
  recognition all favour the first thing IO blesses.

**Mitigation strategy:** Lean *into* the differentiation.
"Rust-everywhere Cardano dApp framework" reaches a specific
audience that's currently underserved (Balius narrowed; nothing
else exists). Don't try to be "the default for all Cardano
devs" — be "the obvious choice for the Rust subset". Network
effects within that subset are sufficient if the framework is
genuinely good for them.

## What this means for existing mitos work

- **Data plane API (`MITOS_DATA_PLANE_API.md`) — unchanged
  direction.** It's the chain-query primitive any framework
  needs. Existing roadmap stands.
- **Isolation roadmap (`MITOS_ISOLATION_ROADMAP.md`) — wasm
  path looks more attractive in this framing.** If the
  framework has consumer-colocated indexer logic (workers
  ship their own indexer modules deployed alongside their
  worker code), wasm sandboxing is the natural answer for
  fault isolation. Phase C of the isolation roadmap becomes
  "first vertical that demonstrates module + worker
  colocation," not "infrastructure for its own sake."
- **Mitos becomes one component, not the product.** Today
  mitos is the visible thing being built. In the framework
  framing, mitos is a piece — the chain-data plane —
  alongside scaffolding, codegen, tx hooks, auth, worker
  patterns. The roadmap's framing should evolve to reflect
  that, but the *work* doesn't change shape: mitos still
  needs the data plane API + isolation, just with the
  understanding that they serve a wider audience than mitos
  alone.

## Trigger conditions

Don't start framework work until at least one of:

1. **cnft.dev's pattern is stable enough to extract.** If we're
   still iterating on the worker / DO / mitos pattern monthly,
   it's too early — extracting an unstable pattern produces a
   framework that needs constant breaking changes. We'll know
   when the patterns settle (no fundamental shape changes for
   ~3 months).
2. **A second team wants to use cnft.dev's stack.** External
   demand validates the patterns are general enough to package.
   Without external pull, we're packaging for ourselves and
   the cost-benefit favours staying with the bespoke shape.
3. **`cardano-init` is released and missing the CF angle.**
   If IO ships the Foundation-funded init tool and it's
   Linux-VPS-default, that's both validation that init tools
   are wanted and an opening for a CF-native alternative.
4. **A contract from a partner team specifically asks for
   "build us a Cardano dApp on CF Workers".** The clearest
   external signal — someone willing to be the design partner
   for the MVP vertical slice.

If none of these are true, the existing piecemeal approach
works. The framework is an investment whose payoff is "many
future apps cheaper to ship"; pursuing it before there's a
second app to ship is investment without amortisation.

## Open questions

1. **Tx-building Rust crate.** shared-crates' `cardano-tx` is
   the obvious starting point — already production-tested in
   cnft.dev. Whether to commit to it as-is, evolve it as the
   framework's tx-building primitive, or fork+slim is a real
   call. Decision criteria: API ergonomics for tx-template
   macros, wasm32 cleanliness, dependency surface.
2. **Codegen path for Aiken blueprints.** Roll our own
   blueprint-to-Rust generator? Adopt or evolve any of the
   lighter codegen experiments in the wild? Hand-maintain a
   small generator targeting our specific tx-template shape?
   Smaller is faster, and the output should be plain Rust
   structs that compose with `cardano-tx`.
3. **CIP-30 wasm-bindgen ergonomics.** The wallet boundary is
   browser-only and JS-shaped. For the optional Rust-wasm
   frontend story, what's the cleanest CIP-30 shim? Is there
   one to adopt or do we ship our own? Affects only the
   optional frontend track but worth resolving before that
   ships.
4. **RPC surface format.** Worker exposes RPC the frontend
   talks to. Rust → wasm frontend wants typed in-language
   calls; non-Rust frontends want JSON. Probably ship the
   typed Rust client + a JSON-RPC adapter; the typed types
   come from the same `shared/` crate the worker uses.
5. **Distribution model.** Cargo crate(s) + a separate CLI
   crate (`cargo cardano`) is the obvious layout. Is the CLI
   `cargo`-subcommand-shaped (`cargo cardano init`) or
   standalone (`cardano-cli init`)? `cargo` subcommand fits
   the Rust audience.
6. **Open source from day one or proprietary first?** Open
   source has network effects but distracts via PR
   maintenance overhead. Proprietary lets us iterate freely
   but loses the framework-of-record positioning. The Rust
   audience has a strong open-source bias; proprietary
   probably doesn't fit.
7. **Naming.** Calling the framework something. Nothing yet
   reserved. Probably matters less than getting it shipped,
   but worth picking before public surface starts existing.
8. **Relationship to mitos.** Mitos has its own naming and
   identity. In this framework framing, mitos is the data
   plane. Is "mitos" a sub-brand of the framework or remains
   independent? Open. Likely the framework gets its own
   top-level name and mitos retains its identity as a
   component.
9. **Teaching surface.** A framework with great docs +
   tutorials + worked examples is more valuable than a great
   framework with bad docs. Especially true for the Rust
   audience — devs evaluate frameworks heavily on the depth
   of `examples/` and reference docs. Allocating time for the
   teaching surface as a first-class deliverable (not an
   afterthought) is part of the commitment.

## Lessons banked from observed failures

For posterity — these patterns recur in the projects that
tried and didn't quite land:

- **Don't span the full stack from first principles.** Balius
  tried; the 2024 design issues are still open and unresolved.
  Extract from a working app instead.
- **A template is not a framework.** use-cardano shipped a
  template; teams still had to wire everything. The runtime
  underneath has to be opinionated.
- **Data plane alone doesn't pull adoption.** DAB and
  ledger-sync proved this. Pair data with app shape from the
  start, even if the app shape is minimal.
- **Be the default or be nothing.** Anchor wins because new
  Solana devs don't choose. Cardano frameworks that try to
  support every off-chain library / every wallet adapter /
  every backend pattern end up supporting none of them well.
- **Maintenance dwarfs initial build.** Frameworks that ship
  v0.1 and stop are the norm. The commitment is to v1.0 + 12
  months of iteration, not v0.1 ship-and-pray.
