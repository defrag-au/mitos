# Mitos companion pattern — paired deployables

A dApp built on this framework consists of **two paired
deployables** that ship together: a **mitos-side indexer
module** (lives in mitos's execution environment, reads chain
state, emits typed events) and a **CF-side mitos companion**
(lives as a Cloudflare Durable Object, owns per-customer state
and the user-facing RPC surface). They're designed in tandem,
deployed in tandem, and version together.

**This is a thesis document, not an implementation roadmap** —
captures the architectural shape so the design is recoverable
when the work is picked up. Sister doc to
`CARDANO_DAPP_FRAMEWORK_THESIS.md` (the broader Rust-everywhere
framework framing) and `MITOS_ISOLATION_ROADMAP.md` (the
mitos-side WASM module mechanics). This doc is specifically
about the *paired deployable shape* — how the two halves know
about each other, what the contract between them looks like,
and what makes this different from just "indexer + worker".

Cross-references:
- `CARDANO_DAPP_FRAMEWORK_THESIS.md` — strategic framing of the
  Rust-everywhere framework this is one piece of
- `MITOS_ISOLATION_ROADMAP.md` — host-side mechanics of WASM
  indexer modules (how mitos loads / sandboxes / dispatches them)
- `../design/MITOS_DATA_PLANE_API.md` — the chain query API
  indexer modules use for state lookups
- `../design/SUBSCRIPTION_MECHANICS.md` — the typed `Interest`
  subscription model that connects indexer events to consumers
- `../design/CF_REPLICATION.md` — the wire protocol the two
  halves currently use

## The shape

```
┌─────────────────────────────────┐    ┌─────────────────────────────────┐
│  mitos-side indexer module      │    │  CF-side mitos companion        │
│  (Rust → wasm32, Balius-ish)    │    │  (Rust → wasm32 in CF DO)       │
│                                 │    │                                 │
│  - reads chain state via plane  │◄──►│  - per-app state in DO SQLite   │
│  - emits typed events           │ WS │  - subscribes to module events  │
│  - declares Watch (intent)      │    │  - exposes RPC to frontend      │
│  - persists watch-set state     │    │  - handles wallet auth (CIP-8)  │
│                                 │    │  - tx-template building         │
│  Lives IN mitos host process    │    │  - lives in customer's CF env   │
└─────────────────────────────────┘    └─────────────────────────────────┘
                ▲                                       ▲
                │                                       │
        ┌───────┴───────┐                       ┌───────┴───────┐
        │ mitos host    │                       │ frontend (any)│
        │ (chain plane, │                       │ Rust-wasm /   │
        │  scheduler,   │                       │ TS / mobile / │
        │  dispatcher)  │                       │ CLI / agent   │
        └───────────────┘                       └───────────────┘
```

The framework's contribution is the **paired-deployable
contract**: same shared types crate consumed by both halves,
same Aiken-blueprint-derived bindings, same auth/session
patterns, same RPC shape. A dApp builder writes both halves in
one workspace, one PR, one deploy.

## What's already built (today, ad-hoc)

The cnft.dev stack is the existence proof that this pattern
*works* — every piece of it is operational in production right
now, just hand-wired:

- **Mitos host** runs on netcup, exposes WS replication channel
- **`OwnershipIndexer`** in mitos reads the chain, emits typed
  `OwnershipChange::Transfer` events
- **`collections-mitos` worker + DO** in CF subscribes to the
  ownership feed, persists state, exposes `/api/check`,
  `/api/owner`, `/api/bundle`, `/api/stats` to the frontend
- **CIP-30 wallet → frontend → worker → mitos** flow works
  end-to-end for the use cases we ship

What's missing:
- **The pairing is bespoke each time.** New collection? Hand-
  edit mitos's indexer registry. New per-customer DO logic?
  Handcraft the worker. No scaffolding, no framework SDK, no
  deploy command that ships both halves together.
- **The two halves drift.** OwnershipChange's wire format
  lives in two crates (mitos's + the worker's mirror). When
  the schema evolves we update both manually. We've shipped
  bugs from this pattern (the `Hash<32>` serialisation issue
  documented in ROADMAP step 10).
- **The indexer module ships inside mitos.** Touching an
  indexer means rebuilding mitos's bundle binary. Ownership
  team waits on framework team. (This is the
  `MITOS_ISOLATION_ROADMAP.md` problem.)
- **No contract enforcement.** The worker assumes events come
  in a specific shape; if the indexer changes that shape the
  failure is at runtime, not compile time. Shared-types crate
  is the obvious fix; we just haven't formalised the pattern.

The framework version of this is **the same architecture, packaged**:

## What "packaged" means concretely

> **Note on this section.** Two layouts coexist in this doc:
> the **steady-state vision** below the next subheading
> (a future `cargo cardano init`-generated workspace), and
> the **current convention** that ships in production today
> (single-file modules under a CF Worker's `modules/`
> directory, materialised by `mitos-build`). New modules
> should follow the current convention; the steady-state
> vision is forward-looking.
>
> For the today-shape: see `docs/HOWTO_FIRST_MODULE.md`
> (walkthrough), `docs/design/MITOS_BUILD.md` (build tool
> contract), and `cnft.dev-workers/workers/collections-mitos/`
> (reference implementation).

### Current convention (today's tooling)

A dApp builder adds two things to an existing CF Worker
monorepo: a `modules/<feature>.rs` next to their worker, and a
shared types crate under `types/<feature>-events/`. There is
**no module Cargo.toml** — `mitos-build` synthesises one from
the bundled host WIT and a paired `<feature>.toml` whose
`[deps]` table lists the user-declared dependencies.

```
your-worker-monorepo/
├── workers/
│   └── <my-worker>/
│       ├── Cargo.toml            # CF Worker DO — depends on
│       │                         #   mitos-companion, mitos-protocol,
│       │                         #   <feature>-events
│       ├── wrangler.toml
│       ├── src/                  # DurableObject impl, RPC handlers
│       └── modules/
│           ├── <feature>.rs      # the wasm indexer module — single file
│           └── <feature>.toml    # runtime config + build-time [deps]
└── types/
    └── <feature>-events/         # shared event-shape crate
        └── src/lib.rs            # types both halves deserialise
```

Build + deploy is a two-step manual flow today:

```bash
# Build the module artifact (wasm + manifest + config.cbor):
mitos-build --module workers/<my-worker>/modules/<feature>.rs

# Upload to a running mitos host:
mitos-admin upload-module \
    --artifact workers/<my-worker>/modules/target/mitos/<feature>

# Deploy the CF DO half (standard wrangler):
cd workers/<my-worker> && wrangler deploy

# Tell mitos to dial the companion's replication endpoint:
mitos-admin add --indexer <feature> \
    --target wss://<my-worker>.workers.dev/<feature>/replicate
```

Module updates replace the running instance on the mitos host;
the chain-point cursor and module-private `state-kv` persist
across replacement. Companion updates roll out via standard
`wrangler deploy`.

The companion-runtime SDK (`mitos-companion` crate) absorbs the
WS lifecycle, cursor persistence, `/api/_interest/*` endpoints,
and Apply/Undo/Mark dispatch. The dApp builder implements
`MitosCompanion` (top-level companion declaration) and one or
more `MitosChannel` traits (per-channel `apply_event` handlers).

### Steady-state vision (future `cargo cardano` shape)

Once `cargo cardano init` ships, the dApp builder runs that
once and gets a self-contained workspace:

```
my-app/
├── Cargo.toml                    # workspace
├── shared/                       # types both halves consume
│   ├── src/lib.rs                # OwnershipChange-equivalent + RPC types
│   └── src/blueprint_codegen.rs  # generated from Aiken CIP-57
├── contracts/                    # Aiken on-chain
│   └── validators/...
├── indexer/                      # the mitos-side module
│   ├── Cargo.toml                # depends on `shared/`, mitos-data-plane,
│   │                             # mitos-protocol (typed Interest etc.)
│   ├── src/lib.rs                # impl Indexer<...> — Watch declaration,
│   │                             # event emission, state via host fns
│   └── target/wasm32/.../indexer.wasm  # built artifact
├── companion/                    # the CF DO half
│   ├── Cargo.toml                # depends on `shared/`, worker-rs
│   ├── src/lib.rs                # CF Worker entrypoint
│   ├── src/do.rs                 # DurableObject impl
│   ├── src/rpc.rs                # typed RPC surface (uses `shared/` types)
│   └── wrangler.toml
├── tools/                        # optional: CLI for migrations etc.
└── README.md
```

`cargo cardano deploy` ships **both halves**:
1. Builds `indexer.wasm`, uploads to mitos's control plane
2. Builds `companion` worker, `wrangler deploy`s it
3. Wires the two together: registers a subscription on mitos
   pointing at `companion`'s `/replicate` endpoint, scoped via
   `Interest` to whatever the indexer emits

`cargo cardano dev` runs locally:
1. Local mitos against testnet (or shared dev mitos), with
   the indexer module loaded
2. `wrangler dev` for the companion
3. WS connection between them
4. Optional Rust-wasm frontend hot-reloading on top

Differences from the current convention:
- Self-contained workspace (vs. modules co-resident with an
  existing worker monorepo).
- A separate `indexer/` crate with its own Cargo.toml (vs.
  single-file modules with `mitos-build` synthesising the crate).
- A single `cargo cardano deploy` orchestrating both halves +
  subscription registration (vs. the manual `mitos-build` →
  `mitos-admin upload-module` → `wrangler deploy` →
  `mitos-admin add` sequence).

Critically — and true under both shapes: **changing the indexer
doesn't redeploy mitos.** The wasm module gets uploaded to the
running mitos host, the running instance is replaced (cursor +
state-kv persist), and dispatch resumes. Both halves deploy
independently of the mitos *host*, but together as a unit
relative to each other.

## What's distinct about the companion pattern

This isn't "indexer + worker" generically. The companion-pattern
contributions are:

### 1. Typed contracts across the deployment boundary

The `shared/` crate is the source of truth. The indexer's
`Change` type, the companion's RPC request/response types, the
state schema's row types, the Aiken blueprint's
datum/redeemer/parameter types — all defined once, consumed by
both halves.

No mirror types. No serde drift. If the indexer changes its
event shape, the companion fails to compile; if the companion's
RPC shape changes, the frontend (if Rust) fails to compile. The
cross-deployment-boundary type system is what makes the pattern
scale to many dApps without each one accumulating its own
serialisation foot-guns.

### 2. Indexer module ships with the consumer

The indexer is **the consumer's code**, not the framework's. A
team writing a new dApp writes their own indexer alongside their
companion DO logic. Both live in the dApp's repo; both deploy
together; both versioned together.

This is the architectural inversion vs. today: today's
`OwnershipIndexer` lives in the mitos repo and serves the
ownership use case for everyone. Tomorrow's indexer modules
live in their owning team's repo, are uploaded to mitos, and
serve their team's use cases. Mitos becomes a *host* for
team-owned indexer modules, not a registry of org-wide indexer
implementations.

### 3. The companion is opinionated

The CF DO half isn't "any worker, you write it". The framework
ships a `MitosCompanion` trait that the companion implements,
plus a runtime that:

- Manages the WS subscription to mitos (handle reconnects,
  cursor persistence, backfill detection)
- Decodes incoming events using the `shared/` types
- Provides typed CIP-30 / CIP-8 helpers
- Exposes a typed RPC surface via Hono (or whatever)
- Handles per-tenant state in DO SQLite with migration
  support
- Wires CF bindings (R2, KV, queues) to standardised slots

The dApp builder writes their `apply_event(event) -> ()`
handler, their `rpc::handler_for_method(...)` callbacks, and
their state schema. Everything else — WS lifecycle, auth,
serialisation, deploy plumbing — is framework-provided.

### 4. Shared-types crate is the integration point

The `shared/` crate is small but defining. It typically contains:

- The indexer's `Change` enum (same shape both sides see)
- The companion's RPC request/response enums
- DO row types (so persistence and RPC can share definitions)
- Re-exports from blueprint codegen (typed validator interfaces)
- App-domain types (`UserProfile`, `OrderState`, whatever)

It's the *contract* between indexer, companion, and (Rust)
frontend. Compiles for native (worker target), wasm32 (CF
target), and wasm32 (mitos indexer target) with the same
source.

This is precisely how Anchor's `#[derive(Accounts)]` macro
generates types both the on-chain program and the off-chain
client consume. Same contract-as-code pattern, applied to
Cardano dApps' indexer + companion + frontend triangle.

## Where mitos fits in the bigger picture

Mitos was originally framed as an "indexer framework" — and
that's still accurate at the host level. But in the companion-
pattern view, mitos is **the runtime that hosts indexer modules
on behalf of dApp teams**. It's analogous to:

- Cloudflare Workers (the runtime that hosts JS / Rust-wasm
  worker code)
- Solana (the runtime that hosts BPF-compiled programs)
- Aptos / Sui (the runtimes that host Move modules)

Difference: those platforms own the deployment surface (you
push your code to *their* infrastructure). Mitos in this
framework is **self-hosted by default** — a team runs their
own mitos instance, often on a single VPS, and deploys their
indexer modules to it. The framework provides the *primitive*
(mitos host) and the *deployment story* (`cargo cardano
deploy`); whether the host is shared between teams or
team-owned is an operational choice.

A managed mitos hosting service is plausible later — `Demeter`
already manages dolos for users; a mitos-hosting analog could
fit the same shape — but isn't a blocker for the framework.

## What this changes about ongoing mitos work

**The data plane API** (`MITOS_DATA_PLANE_API.md`) is the chain
query primitive both indexer modules and companion DOs use.
Same trait, two transports:
- `LocalDataPlane` for indexer modules running in-process in
  mitos (today's Phase A)
- A future `IpcDataPlane` or `GrpcDataPlane` for companion DOs
  that want direct chain queries (utxorpc-compat for
  cross-machine, in-CF-region for managed mitos)

**The isolation roadmap** (`MITOS_ISOLATION_ROADMAP.md`) Phase
C — wasm host fn API for indexer modules — is the actual
deployment mechanism for this pattern. The "module + companion"
shape doesn't work without per-team module deployment;
isolation Phase C is what enables that.

**The replication wire protocol** (`CF_REPLICATION.md`) stays
mostly as-is — it's already the right shape (typed Interest
subscription, WS-based event delivery). Companion DOs subscribe
the way `collections-mitos` subscribes today; the framework
just generalises the pattern.

**Subscription mechanics** (`SUBSCRIPTION_MECHANICS.md`) — the
typed `Interest` vocabulary the framework's companion runtime
uses to express what events it cares about, generated from the
indexer module's declarations. The pattern's contribution here
is making the connection explicit: indexer Watch declarations
become consumer Interest subscriptions automatically.

## The contract between halves

A dApp's two halves communicate through a few well-defined
surfaces:

### 1. Subscription handshake

Companion DO subscribes via WS to mitos, scoped via `Interest`.
The Interest is **derived from the indexer module's typed
event surface** — if the indexer emits `Change::Sale` and
`Change::Listing`, the companion can subscribe to either or
both with no string-typing.

### 2. Event delivery

Mitos pushes events over the WS. Events are CBOR-encoded; both
halves use the same `shared/` types crate to encode/decode.
Wire format change in either half == compile error in the
other.

### 3. State coordination (one-way)

The indexer is read-only; it doesn't mutate companion state.
The companion owns its state entirely. State changes flow:
chain → mitos → indexer (typed event) → companion DO
(persists). This is the same data flow we have today; the
framework's contribution is making it boilerplate-free.

### 4. RPC surface (frontend ↔ companion)

Companion exposes typed RPC; frontend (Rust-wasm or any) calls
it. The framework provides the RPC surface scaffold, the
companion fills in handlers. Wallet auth (CIP-8 sign-in) is a
first-class verb in the surface.

### 5. Admin surface (mitos ↔ companion lifecycle)

`cargo cardano deploy` orchestrates: upload indexer wasm to
mitos's control plane, deploy companion to CF, register WS
subscription on mitos pointing at companion. `cargo cardano
status` queries both halves and reports drift.

## Open questions

1. **WASM ABI for indexer modules.** Borrow Balius's WIT
   shape directly (currently the closest existing reference,
   even though Balius itself is in maintenance mode)? Define
   our own from `mitos-data-plane`'s Rust trait + the
   `Indexer` trait? Probably the latter — Balius's WIT is
   informed by goals overlapping but not identical to ours,
   and we'd want our ABI to be ours.

2. **Module distribution + storage.** Where do uploaded indexer
   wasm artifacts live? Local FS on the mitos host? Object
   storage with a manifest? OCI registry? Probably FS-on-host
   for v1; iterate when there's more than one mitos instance.

3. **Module versioning + ABI compatibility.** Each module
   declares its required ABI version on upload; mitos host
   refuses incompatible versions. Need to design version
   negotiation up front so older modules don't break when the
   host updates.

4. **Module state migration.** When an indexer module's state
   schema evolves between deployments, what happens? Module-
   owned state means module-owned migration; framework can
   provide a snapshot/clear primitive but can't enforce
   schemas. Same problem as the redb-table-name bump in
   Phase 4 of the protocol work.

5. **Companion hot-reload.** `wrangler deploy` already does
   the right thing on the CF side. Module hot-reload on mitos
   side is the harder case; either accept brief downtime per
   reload or build graceful handoff.

6. **Cross-tenant isolation.** When mitos hosts multiple
   teams' modules, how does mitos prevent module A from
   reading module B's state, blocking module B with a runaway
   loop, or clogging the WS pipeline? Wasmtime sandbox covers
   most; resource accounting + fair scheduling needs design.

7. **Multi-companion subscriptions.** A complex dApp might want
   multiple companion DOs (one per "context" — e.g. per
   collection, per channel, per game). How does the
   subscription registry scale? Today's mitos handles a few;
   a popular framework might mean hundreds per host.

8. **Auth bridging.** mitos's auth (`MITOS_AUTH_TOKEN`) is one
   token per host. When multiple teams' modules + companions
   run on shared mitos, how do they identify themselves to
   each other? Probably per-module API keys with scoped
   subscription rights. Real design needed.

## Trigger conditions

This pattern is interesting now but not buildable until:

1. **Isolation roadmap Phase C lands** (or at least starts —
   wasm module loading mechanics in mitos). Without dynamic
   module loading the "ship the indexer with the companion"
   part doesn't work.
2. **The data plane API stabilises.** The indexer module's
   primary chain access is via the data plane; the API
   contract needs to be settled before we ask team-owned
   modules to depend on it.
3. **A second team wants to ship a Cardano dApp on this
   stack.** The pattern only earns its keep if there's a
   second use case; the first use case (cnft.dev) can keep
   being hand-wired and we extract the pattern from
   experience.
4. **`cardano-init` ships from IO and demonstrates the
   alternative shape.** Their TS-default scaffold gives us
   visibility into the design space; we can position
   ourselves as the Rust+CF specialisation rather than
   guessing the right framing.

## Lessons banked from cnft.dev

The companion pattern isn't theoretical — cnft.dev is doing
this hand-wired today. Worth banking what's worked vs. what's
been painful:

- **Per-policy DO routing works very well.** `idFromName(policy_id)`
  gives a clean per-tenant coordinator; SQLite-on-DO is
  surprisingly capable; cost is sane.
- **WS replication is more reliable than HTTP polling.** The
  Hibernation API + reconnect logic survives CF Worker cold
  starts gracefully.
- **Cursor persistence per consumer matters.** Both halves
  need cursor tracking; companion knows its last-applied,
  mitos knows what was sent. Coordination via the typed
  `SubscribeReply` works.
- **Mirror types are a recurring footgun.** The
  `OwnershipChange` mirror in the worker has caused at least
  two bugs (Hash<32> serialisation, role field default
  semantics). The `mitos-protocol` crate fixed the
  framework-internal version of this; the framework needs to
  productise this for dApp teams.
- **Auth is per-host today.** `MITOS_AUTH_TOKEN` works for
  one team's mitos. For framework-managed multi-team
  hosting, this gets non-trivial fast.
- **Reset + re-backfill is the cleanest schema-migration
  mechanism.** When the DO's storage shape changes, drop
  tables + re-subscribe to trigger fresh backfill. Works for
  small-state collections (~10k rows); not for very large
  ones. The framework should provide a more graceful
  alternative.
- **The deployment timing matters.** When mitos and the
  companion get out of sync (one deployed, other not), the
  WS rejects subscribe payloads it doesn't understand. The
  framework's deploy command should orchestrate this.
