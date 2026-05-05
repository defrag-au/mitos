# Mitos companion runtime v1

The CF Worker / Durable Object half of the paired-deployable
shape laid out in `MITOS_COMPANION_PATTERN.md`. This doc
captures the **concrete v1 implementation shape** for the
companion-side runtime SDK: what's in it, what stays dApp-side,
crate location, trait surface, and order of operations.

Sister to:
- `MITOS_COMPANION_PATTERN.md` — the paired-deployable thesis
- `MITOS_PLATFORM_V1.md` — the mitos-side wasm runtime that
  publishes events the companion consumes
- `MITOS_PLATFORM_DEPLOYMENT.md` — the deployment story for
  the mitos-side half (this doc is the matching deploy story
  for the CF half)
- `../design/CF_REPLICATION.md` — the wire protocol the runtime
  speaks
- `../design/SUBSCRIPTION_MECHANICS.md` — the typed `Interest`
  vocabulary

## Terminology

Canonical vocabulary for this doc and follow-on work. We've
been sloppy about which side of the wire we're talking about;
these terms fix that.

| Term | Meaning | Lives in |
|---|---|---|
| **Mitos host** | The service running on the dedicated box (netcup mitos-mainnet today). Owns the chain follower, wasm module lifecycle, WAL replay, emissions log. | `mitos/` repo |
| **Module** | A wasm artifact running inside the mitos host. Filters chain events per its interest set; emits matched events as `ServerMessage::Apply`. **Modules live with the dApp code that depends on them**, deployed *to* the mitos host. The `modules/<name>/` directories in the `mitos/` repo are testing scaffolding only — production modules live in dApp repos (e.g. `cnft.dev-workers/modules/<name>/`). The module's `mitos.toml` declares the **dial-back URL template** for its companions (parameterised on companion key) — the dApp's Worker URL is known at module-build time and bakes in cleanly. | dApp repo (production); `mitos/modules/` (test only) |
| **Companion key** | Stable string the dApp uses to address one specific Companion DO instance. Passed to `id_from_name()` for DO routing; substituted into the module's dial-back URL template; carried in subscribe call as `companion_key`; scopes rows in the host's `module_emissions` log. **Load-bearing in four places** — must agree end-to-end. | dApp's choice; runtime ships no API for it. |
| **Tenant** | dApp-level concept of an isolation unit (customer, account, team, policy, …). Translates to a Companion key in the dApp Worker's route handlers. The runtime never sees "tenant"; it sees the Companion key the dApp Worker passes in. | dApp-level only. |
| **Channel** | A logical event stream within a module (`ownership`, `marketplace`, …). One module can host multiple channels. | Defined in module; consumed by companions. |
| **Interest set** | The dApp's expression of "what I want to be told about". Source of truth on companion side; replicated to the module as filter. | Companion DO SQLite (canonical), Module redb (replica). |
| **Emission** | One matched event the module sends to a companion as `ServerMessage::Apply`. | Recorded in host's `module_emissions` log (see "Emissions log on the host"). |
| **Companion** | The CF-side counterpart of a module. A `#[durable_object]` DO using the runtime. Receives emissions, calls dApp's `apply_event`, owns the dApp's state slice. | `cnft.dev-workers/workers/<name>-companion/` (or wherever the dApp keeps its CF code) |
| **Runtime** | The `mitos-companion` crate. Absorbs companion boilerplate. Provides `MitosCompanionRuntime<C>`, `MitosCompanion` trait, `MitosChannel` trait. | `mitos/crates/mitos-companion/` |
| **dApp Worker** | The dApp's CF Worker — HTTP entrypoint, business logic. Mitos-unaware (treats companions as service bindings). | dApp's repo, e.g. `cnft.dev-workers/workers/<dapp>/` |
| **dApp** | The product. One or more dApp Workers + N companions + frontend + DBs. Owns its own modules in its own repo. | Multi-crate scope. |
| **ACK / NACK** | Companion's response to an emission after `apply_event`. ACK = success; NACK = the dApp handler errored. | Wire protocol additions (see "Emissions log"). |

Throughout the rest of the doc, "the runtime" specifically
means the `mitos-companion` crate (item 4 in the table); "the
host" specifically means the mitos service (item 1).

## What v1 is

A **runtime crate** (`mitos-companion`) that absorbs the
boilerplate every CF Worker companion currently hand-rolls,
plus a **starter template / migration target** for at least
one existing companion (collections-mitos) to validate the API
under real load.

The dApp builder writes:

- A typed `Config` struct for their watched policies + RPC
  shape
- An `apply_event(event) -> ()` handler — the *intent* of their
  indexer's per-event behaviour, expressed as typed event-to-
  state-mutation
- A `state_schema` definition (or a typed migration trait)
- A typed RPC surface — typically just route+handler pairs
  matching the existing worker-rs Router shape

The runtime owns:

- Initial registration via HTTPS POST to
  `/api/companions/subscribe` on first DO wake (carries
  module + companion key + cursor + interests + optional
  dial-back override)
- Inbound WebSocket lifecycle (mitos dials, DO accepts via
  Hibernation API, persists cursor across reconnects)
- Per-message decode through `mitos-protocol`'s typed
  `ServerMessage` (`Apply`/`Undo`/`Mark`/`Connected`)
- Cursor coordination — last-applied checkpoint in DO SQLite,
  resume on reconnect, replay-detection on indexer restart
- Schema migration helpers (idempotent CREATE IF NOT EXISTS,
  versioned migrations table, on-mismatch reset path)
- Auth check on inbound mitos sockets (`MITOS_AUTH_TOKEN` via
  CF Secrets Store binding)
- Multi-channel multiplexing — a single companion DO consumes
  multiple subscriptions (ownership + marketplace + future
  indexers) via WS Hibernation tags
- Ack/Nack delivery acknowledgement back to the host
- `/_internal/wake` endpoint for dApp-Worker-driven
  bootstrapping

## Concrete v1 scope (strict)

In:
- `mitos-companion` crate in `mitos/crates/mitos-companion/` (alongside `mitos-protocol`, in the public mitos repo)
- `MitosCompanion` trait — the dApp builder's entry point
- `MitosCompanionRuntime<C: MitosCompanion>` — plain struct (no
  `#[durable_object]`, no `DurableObject` impl) that the dApp
  embeds inside their own DO wrapper
- WS lifecycle: **mitos dials companion**, DO accepts via
  Hibernation API, decode, dispatch, hibernate
- Cursor persistence in DO SQLite (`mitos_companion_meta` table)
- Schema migration helpers
- Auth check via Bearer token
- **HTTPS subscribe call** — companion POSTs
  `/api/companions/subscribe` to mitos on first wake; carries
  module + companion key + cursor + interests + optional
  dial-back override
- **`/_internal/wake` endpoint** on every Companion DO class —
  triggered by dApp Worker during onboarding to materialize the
  DO and run the subscribe call
- **`mitos.toml [companion]` block** — module declares
  `replicate_url` template (with `{key}` substitution) and
  `auth_header` defaults
- **Dynamic interest mechanics** — companion is the source of
  truth for what it watches; subscribe/unsubscribe at runtime
  over the held WS, no module redeploy
- Typed `Interest`-driven subscription scope construction
- Multi-channel support (one Companion DO, N inbound mitos sockets)
- **Ack/Nack wire protocol** — companion confirms successful
  apply or surfaces apply errors back to the host
- **`ServerMessage::Connected` readiness frame** — first frame
  mitos sends after dial; gives companion a sync point
- **Host-side emissions log** — `module_emissions` redb table
  per module; failure visibility, audit, replay all flow from
  here (operator surface via `mitos-admin emissions ...`);
  doubles as delivery queue for offline companions (`queued`
  status)
- Migration of `collections-mitos` to use the runtime (the
  validation arc)

Out (defer to v2):
- CIP-30 / CIP-8 wallet auth helpers — separate workstream
- Tx-template builder — separate, lives in `cardano-tx`
- R2 / KV / queues standardised slots — only when a real dApp
  asks for them
- `cargo cardano init` scaffolding
- Frontend RPC type generation

## What's hand-rolled today (the pain to absorb)

Survey of the existing `workers/collections-mitos/`:

| Area | Lines (approx) | Type |
|---|---|---|
| WS upgrade routing per `policy_id` | ~30 | Boilerplate |
| Auth check (Bearer header) | ~25 | Boilerplate |
| WS Hibernation accept + tag | ~40 | Boilerplate |
| ClientMessage::Subscribe send | ~50 | Boilerplate (typed but mechanical) |
| Per-message decode | ~30 | Boilerplate |
| Apply/Undo/Mark dispatch | ~80 | Mostly boilerplate |
| Cursor read/write helpers | ~50 | Boilerplate |
| Schema migration | ~120 | Boilerplate |
| Ownership SQLite mutation | ~150 | dApp-specific (stays) |
| Marketplace event multiplex | ~80 | Boilerplate |
| Read API endpoints | ~200 | dApp-specific (stays) |
| Reset / admin endpoints | ~80 | Boilerplate |
| Top-level `lib.rs` routing | ~196 | Boilerplate |
| **Total** | **~1411** | **~70% boilerplate** |

The runtime should absorb that 70%. A new dApp's companion
crate should land at ~400 lines of dApp-specific intent,
leaving the framework to own everything else.

## Composition model: one worker, many companions

The runtime is designed so a single dApp Worker drives **N
companion DOs**, each a focused mitos consumer for one
concern. This is the steady-state shape; "one companion = one
worker" is a degenerate case.

### Two distinct roles

- **dApp Worker** — the dApp's CF Worker. HTTP entrypoint,
  business logic, owns the user-facing read API, owns
  user-facing state (sessions, frontend coordination,
  tx-templates). Does not speak the mitos WS protocol
  directly. Per Terminology section.
- **Companion** — a focused mitos consumer (see Terminology).
  One DO class per concern (ownership, marketplace, mint
  events, governance, …), each subscribed to its own
  mitos channel(s) and owning its own SQLite slice. Speaks
  the mitos WS protocol via the runtime; does not face the
  public internet.

### Worker → companion fan-out

The worker holds N `DurableObjectNamespace` bindings, one per
companion type:

```toml
[[durable_objects.bindings]]
name = "OWNERSHIP_DO"
class_name = "OwnershipCompanion"

[[durable_objects.bindings]]
name = "MARKETPLACE_DO"
class_name = "MarketplaceCompanion"

[[durable_objects.bindings]]
name = "GOVERNANCE_DO"
class_name = "GovernanceCompanion"
```

Each `*Companion` is a thin wrapper that owns a
`MitosCompanionRuntime<C>` and forwards DO method calls into
it (see "Runtime DO shape" below for the canonical pattern).

The worker routes RPC calls into the right companion via
`id_from_name(companion_key)` (see Terminology) — keeps
per-tenant isolation across companions. A single worker
request can fan out to multiple companions in parallel
(e.g. `/api/policy/:id/overview` queries ownership +
marketplace + governance DOs concurrently and composes the
result).

### Companion ↔ companion: don't

Companions never call each other directly. Cross-cutting reads
happen at the worker layer, which composes their results.
This keeps each companion's failure domain isolated and its
interest set independent — the ownership companion subscribes
to policies the dApp wants ownership history for; the
marketplace companion subscribes to policies the dApp wants
sale events for. Same policy can appear in both with no
coupling.

### Multi-companion vs. multi-channel (not collision)

- **Multi-companion** (this section) = one worker driving
  several *separate* companion DOs because the concerns are
  independent and benefit from isolated state, scaling, and
  lifecycle. Default shape.
- **Multi-channel** (later section) = one companion DO
  consuming *multiple* mitos channels because they're tightly
  related (e.g. ownership + asset-metadata feed for the same
  tenant slice, where the data is always read together).

Default guidance: **start with multi-companion (one DO per
concern)**. Reach for multi-channel only when two channels
are so coupled their data is always read together — typically
because a single SQL query needs columns sourced from both.

### Runtime impact: zero

Multi-companion fan-out is purely a "the worker imports the
runtime crate twice with different generic params" affair.
The runtime crate itself is unchanged — `MitosCompanionRuntime<C>`
doesn't know or care that another `MitosCompanionRuntime<D>`
exists in the same worker. No new PR work.

## Trait shape (sketch, will iterate)

The trait split is **`MitosCompanion` (the dApp's top-level
runtime config) + `MitosChannel` (per-channel event handlers,
one impl per channel)**. The runtime erases per-channel types
via a `MitosChannelDyn` trait-object so a single `Vec` can
hold heterogeneous channels.

```rust
/// Implemented once per dApp companion. Owns the channel set,
/// config, schema, and dApp RPC routes. The runtime fans
/// inbound mitos events out to the right channel by tag.
pub trait MitosCompanion: Send + Sync + 'static {
    /// Stable name (matches the indexer's `name()` on the
    /// mitos side). Used for routing, logging, schema isolation.
    const NAME: &'static str;

    /// Per-companion config — typically initial interest set,
    /// auth tokens, etc. Loaded from DO storage on first request.
    type Config: serde::de::DeserializeOwned + Default + Send;

    /// Channels this companion subscribes to. Most companions
    /// have one; multi-channel companions return several.
    /// Each channel decodes events into its own typed `Event`.
    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>>;

    /// SQLite schema migration. Default: no-op (the runtime
    /// always creates `mitos_companion_meta`,
    /// `mitos_companion_interest`, and the registration cache
    /// row itself).
    fn migrate(&self, state: &State) -> Result<()> {
        Ok(())
    }

    /// dApp's RPC routes. Mounted under `/api/*` by the runtime.
    /// Runtime reserves `/api/_*` (health, meta, interest).
    fn rpc_routes(&self) -> worker::Router {
        worker::Router::new()
    }
}

/// Implemented once per channel a companion subscribes to.
/// Each channel has its own typed `Event` — could be a struct,
/// an enum, or any `DeserializeOwned` shape. The most common
/// pattern is an enum (chain events naturally have variants:
/// `OwnershipChange::Transfer | Burn | Mint`).
#[async_trait]
pub trait MitosChannel: Send + Sync + 'static {
    /// Stable channel name. Matches the host-side indexer
    /// channel + the WS Hibernation tag the runtime sets.
    const NAME: &'static str;

    /// Wire shape for events on this channel. Sourced from
    /// `mitos-protocol` (or another shared crate) so there's
    /// no mirror-types drift.
    type Event: serde::de::DeserializeOwned + Send;

    /// Per-event hook. Called inside a DO storage transaction;
    /// returning `Err` rolls back the transaction and the event
    /// re-delivers on next attempt.
    async fn apply_event(&self, ctx: &Ctx, event: Self::Event) -> Result<()>;

    /// Optional: undo hook for chain reorgs. Default: log warn.
    async fn undo(&self, ctx: &Ctx, point: ChainPoint) -> Result<()> {
        tracing::warn!(?point, channel = Self::NAME, "undo no-op");
        Ok(())
    }
}

/// Object-safe erased view; the runtime uses this internally
/// to dispatch by channel name. The blanket impl lifts any
/// `MitosChannel` into a `MitosChannelDyn` automatically.
#[async_trait]
pub trait MitosChannelDyn: Send + Sync + 'static {
    fn name(&self) -> &'static str;
    async fn apply_bytes(&self, ctx: &Ctx, bytes: &[u8]) -> Result<()>;
    async fn undo(&self, ctx: &Ctx, point: ChainPoint) -> Result<()>;
}

#[async_trait]
impl<C: MitosChannel> MitosChannelDyn for C {
    fn name(&self) -> &'static str { C::NAME }
    async fn apply_bytes(&self, ctx: &Ctx, bytes: &[u8]) -> Result<()> {
        let event: C::Event = ciborium::de::from_reader(bytes)?;
        self.apply_event(ctx, event).await
    }
    async fn undo(&self, ctx: &Ctx, point: ChainPoint) -> Result<()> {
        MitosChannel::undo(self, ctx, point).await
    }
}
```

### Concrete dApp shape

Single-channel companion:

```rust
struct OwnershipChannel { /* deps */ }

#[async_trait]
impl MitosChannel for OwnershipChannel {
    const NAME: &'static str = "ownership";
    type Event = OwnershipChange;  // enum from mitos-protocol
    async fn apply_event(&self, ctx: &Ctx, ev: OwnershipChange) -> Result<()> {
        match ev {
            OwnershipChange::Transfer { .. } => { /* ownership SQL */ }
            OwnershipChange::Burn { .. }     => { /* ... */ }
            OwnershipChange::Mint { .. }     => { /* ... */ }
        }
        Ok(())
    }
}

struct OwnershipImpl;

impl MitosCompanion for OwnershipImpl {
    const NAME: &'static str = "ownership-companion";
    type Config = OwnershipConfig;
    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![Box::new(OwnershipChannel { /* ... */ })]
    }
}
```

Multi-channel companion (rare — only when channels are
tightly coupled): push more `Box::new(...)` into `channels()`.

### Why split the trait

- **Strong typing per channel** — each `apply_event` takes a
  channel-specific `Event`. Compiler catches drift.
- **Per-channel testability** — unit-test
  `OwnershipChannel::apply_event(synthetic_event)` without
  spinning up a runtime or DO.
- **Independent evolution** — channels in different stages of
  schema evolution can coexist in one companion without
  cross-coupling.
- **`Event` is unconstrained** — struct, enum, tuple, anything
  `DeserializeOwned`. Most channels will pick enum (chain events
  naturally have variants); the trait doesn't force the shape.
- **Multi-channel falls out for free** — same companion impl,
  longer `channels()` Vec.

Note on `Router`: the trait uses worker-rs's existing
`worker::Router` (not a fresh `axum_like` wrapper). Existing
companions already use it, less type churn, fewer deps.
Documented in `RPC surface` section below.

## Runtime DO shape

The runtime ships a **plain generic struct**, not a generic
DO. The dApp writes a non-generic `#[durable_object]` wrapper
per companion type and forwards each DO method into the
runtime. This costs ~30 lines of forwarder boilerplate per
DO class but isolates blast radius if `worker-rs` changes its
macro behaviour in future versions, and keeps the runtime
crate independent of macro internals.

```rust
// In `mitos-companion` — no `#[durable_object]`, no DO impl.
// Just a regular generic struct with async methods.
pub struct MitosCompanionRuntime<C: MitosCompanion> {
    state: State,
    env: Env,
    inner: C,
}

impl<C: MitosCompanion> MitosCompanionRuntime<C> {
    pub fn new(state: State, env: Env, inner: C) -> Self { ... }

    pub async fn fetch(&self, req: Request) -> Result<Response> {
        // Standard routing:
        //   /_internal/replicate              ← mitos dials, runtime accepts WS upgrade
        //   /_internal/replicate-<channel>    ← multi-channel WS upgrade
        //   /_internal/wake                   ← dApp Worker pings; triggers HTTPS subscribe
        //   /api/_interest/*                  ← interest mutate / list
        //   /api/_health, /api/_meta          ← runtime-owned
        //   /_admin/reset                     ← drop tables, recreate via migrate()
        //   /_admin/cursor                    ← inspect cursor
        //   /api/*                            ← delegated to inner.rpc_routes()
    }

    pub async fn websocket_message(
        &self,
        ws: WebSocket,
        msg: WebSocketIncomingMessage,
    ) -> Result<()> {
        // 1. Decode envelope via mitos-protocol::decode_server
        // 2. Match on Apply / Undo / Mark
        // 3. For Apply: cbor-decode payload to C::Event, call inner.apply_event()
        // 4. Atomic cursor advance via mitos_companion_meta table
        // 5. Hibernate
    }

    pub async fn websocket_close(...) -> Result<()> { ... }
    pub async fn websocket_error(...) -> Result<()> { ... }
}
```

```rust
// In the dApp's worker — once per companion type. ~30 lines
// of forwarder boilerplate; the runtime owns the actual logic.
#[durable_object]
pub struct OwnershipCompanion {
    runtime: MitosCompanionRuntime<OwnershipImpl>,
}

#[durable_object]
impl DurableObject for OwnershipCompanion {
    fn new(state: State, env: Env) -> Self {
        Self {
            runtime: MitosCompanionRuntime::new(state, env, OwnershipImpl::default()),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        self.runtime.fetch(req).await
    }

    async fn websocket_message(
        &self,
        ws: WebSocket,
        msg: WebSocketIncomingMessage,
    ) -> Result<()> {
        self.runtime.websocket_message(ws, msg).await
    }

    async fn websocket_close(
        &self,
        ws: WebSocket,
        code: usize,
        reason: String,
        was_clean: bool,
    ) -> Result<()> {
        self.runtime.websocket_close(ws, code, reason, was_clean).await
    }

    async fn websocket_error(&self, ws: WebSocket, err: Error) -> Result<()> {
        self.runtime.websocket_error(ws, err).await
    }
}
```

### Why composition, not a generic `#[durable_object]`

The macro alternative — `#[durable_object] pub struct
MitosCompanionDO<C>(...)` — would let the dApp write a
one-liner per companion. But:

- worker-rs's macro generates `extern` functions with
  per-class symbol names; with generics those symbols must
  be parameterised in ways the wasm-bindgen layer may not
  handle cleanly. Even if it compiles, debugging at the JS
  boundary becomes painful.
- We don't want the runtime crate to be hostage to worker-rs
  macro behaviour changing across versions. Composition
  contains that blast radius — a macro change affects the
  thin wrapper the dApp owns, not the runtime SDK.
- ~30 lines of forwarder per DO class × ~3-5 classes per
  worker = ~150 lines total. Cheap.
- Composition gives the dApp an escape hatch — they can
  intercept any DO method (custom `alarm()` handler, etc.)
  without fighting a generic surface.
- The runtime crate stays portable: any DO host (worker-rs,
  future alternatives) can wrap `MitosCompanionRuntime<C>`.

## Cursor coordination (the load-bearing detail)

Each companion DO persists its **last-applied chain point** so
mitos's `Replicator` can resume the WS subscription cleanly
across DO restarts, CF cold starts, and mitos redeploys.

```sql
CREATE TABLE IF NOT EXISTS mitos_companion_meta (
    key   TEXT PRIMARY KEY,
    value BLOB NOT NULL
);
-- Keys:
--   cursor_chain_point — CBOR-encoded ChainPoint (Specific|Origin|Tip)
--   schema_version     — TEXT, for schema migrations
--   last_subscribe_at  — TEXT (RFC3339), for diagnostics
```

### Cursor format: CBOR-encoded `ChainPoint`

The cursor is stored as a single row keyed
`cursor_chain_point` with a CBOR-encoded `ChainPoint` BLOB.
Resolved 2026-05-05.

Rationale:
- `ChainPoint` is a sum type (`Specific(slot, hash) | Origin |
  Tip`). Splitting into `cursor_slot` + `cursor_hash` rows
  can't represent `Origin` / `Tip` cleanly without sentinel
  values; CBOR carries the variant tag natively.
- Host-side platform already CBOR-encodes `ChainPoint` for
  its own cursor persistence in redb. Same wire shape on both
  sides means shared serialization helpers from
  `mitos-protocol` and no translation layer.
- Single-row read = one fewer SQL exec on resume.
- Forward-compat: if `ChainPoint` gains fields (e.g. `era`,
  `block_number`), CBOR carries them; split-column shape
  would need a schema migration.

```rust
// Read
let bytes: Vec<u8> = ctx.query_row_blob(
    "SELECT value FROM mitos_companion_meta WHERE key = 'cursor_chain_point'",
)?;
let cursor: ChainPoint = ciborium::de::from_reader(&bytes[..])?;

// Write (synchronous, no .await — runs inside the output gate
// alongside the dApp's apply writes)
let mut buf = Vec::new();
ciborium::ser::into_writer(&new_point, &mut buf)?;
ctx.exec(
    "INSERT OR REPLACE INTO mitos_companion_meta (key, value)
     VALUES ('cursor_chain_point', ?)",
    vec![SqlStorageValue::Blob(buf)],
)?;
```

Trade-off accepted: CBOR BLOB is less inspectable than two
TEXT rows in raw SQLite dumps. The `/_admin/cursor` endpoint
surfaces the decoded `ChainPoint` as JSON, so this isn't real
operator pain.

### One-shot migration from collections-mitos's split-row format

Collections-mitos today uses two TEXT rows (`cursor_slot` +
`cursor_hash`). On first runtime startup against an existing
DO, the runtime's `migrate()` step:

1. Checks for existing `cursor_slot` + `cursor_hash` rows.
2. If present and `cursor_chain_point` is missing:
   - Reconstruct `ChainPoint::Specific(slot, hash)`.
   - CBOR-encode and INSERT under the new key.
   - DELETE the old rows.
3. From then on, only `cursor_chain_point` is written.

One-shot per DO, idempotent, no backfill mechanics. Lands in
PR 5 (the collections-mitos migration PR).

### Atomicity: output-gate, not explicit transactions

Cloudflare's SQLite-backed DOs **do not expose explicit SQL
transactions** to the runtime SDK we use. Verified during PR 1
design (2026-05-05):

- worker-rs's `Storage::transaction(...)` only wraps the
  legacy KV API — its closure `Transaction` argument has no
  `sql()` method, so SQL ops aren't covered.
- The JS-side `ctx.storage.transactionSync(callback)` *does*
  scope SQL, but its callback **forbids `await`** — and it
  isn't exposed in worker-rs as of v0.8.1.
- Atomicity therefore comes from the **output gate**: a
  contiguous run of `sql.exec(...)` calls with no `.await`
  between them is auto-coalesced into one atomic implicit
  transaction, gated by the output gate (responses are held
  until durable).
- Source: [Zero-latency SQLite in Durable Objects](https://blog.cloudflare.com/sqlite-in-durable-objects/),
  [Rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/).

This shapes the runtime in three concrete ways:

**1. The dApp's `apply_event` must not `.await` external IO
between mutations and cursor advance.** The runtime contract:
do all external IO *first* (HTTP fetch, KV lookup, anything
involving an `.await`), build the full row set in memory,
then perform all SQL writes synchronously at the end. The
runtime's wrapper around `apply_event` enforces this by
calling cursor-advance immediately after the dApp returns —
synchronous, no awaits between.

**2. The cursor advance must be idempotent.** Because there's
no transactional rollback covering SQL + external IO, and
because **WS Hibernation does not redeliver messages on
handler error**, the runtime can't rely on "frame re-delivers
on rollback". Instead: cursor advance is `INSERT OR REPLACE`,
keyed on the chain point (slot+hash); replaying the same
event lands the same cursor row. dApp mutations should also
be idempotent at the chain-point level — typically achieved
because Cardano events are themselves idempotent (a
`Transfer { tx_hash, output_index }` applied twice yields the
same final state).

**3. Cursor-after-success ordering, with Ack/Nack upstream.**
The runtime's WS handler sequences as:

```rust
async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) -> Result<()> {
    let frame = decode_server(&msg)?;
    match frame {
        ServerMessage::Apply { emission_id, point, channel, payload } => {
            let ch = self.lookup_channel(&channel)?;
            // 1. dApp does external IO + builds row set in apply_event.
            //    .await is allowed BEFORE SQL writes; not interleaved with them.
            let result = ch.apply_bytes(&self.ctx(), &payload).await;

            match result {
                Ok(()) => {
                    // 2a. Synchronous cursor advance — output gate wraps both writes.
                    self.write_cursor_sync(&point)?;
                    // 2b. Send Ack upstream (after gate flush; fire-and-forget).
                    send_ack(&ws, emission_id).await?;
                }
                Err(e) => {
                    // 2a'. Advance cursor anyway so streaming continues.
                    self.write_cursor_sync(&point)?;
                    // 2b'. Send Nack upstream — host records into module_emissions.
                    send_nack(&ws, emission_id, &e).await?;
                }
            }
        }
        ServerMessage::Undo { point } => { /* idempotent reverse */ }
        ServerMessage::Mark { point } => {
            self.write_cursor_sync(&point)?;
        }
    }
    Ok(())
}
```

The Ack/Nack frame is fire-and-forget from the companion's
side — it does not block the cursor commit. Host-side
emissions log records status on receipt; if the frame is
lost, the row stays `pending` and ages out to `timeout` per
the host's policy. The companion's local cursor is the
source of truth for resume on reconnect; the host's log is
authoritative for cross-companion visibility and operator
replay.

If `apply_bytes` errors, the *partial dApp writes* before the
error may have already been gated to durable. The dApp must
therefore design `apply_event` implementations so that
retrying from the un-advanced cursor (or via a host-driven
replay from `module_emissions`) either re-converges to the
same state (idempotent) or repairs the partial write. For
Cardano chain events this is the natural shape; we document
it as a hard requirement.

### Failure modes (under the output-gate model)

| Failure | Outcome |
|---|---|
| `apply_event` errors after some SQL writes | Cursor advances; partial writes persist; **Nack sent**; row marked `nacked` in host's `module_emissions`; replay must be idempotent |
| `apply_event` errors before any SQL writes | Cursor advances; no dApp state change; **Nack sent**; row marked `nacked`; replay re-applies cleanly |
| DO crash mid-handler | Output gate hasn't flushed; all uncommitted writes lost; row stays `pending` host-side until ack-timeout; reconnect re-applies from last durable cursor |
| WS Hibernation handler error | No frame redelivery; cursor state is the source of truth on reconnect; host's row stays `pending` until ack-timeout |
| Ack/Nack frame lost in transit | Companion has committed; host row stays `pending` until ack-timeout, then `timeout`; operator can re-emit if state divergence suspected |
| Companion offline when match arrives | Row written `queued`; never sent over WS; on reconnect, host drains `queued` rows in order before resuming live stream |
| Companion offline longer than mitos WAL retention | `queued` rows in `module_emissions` outlive the WAL; recovery still works from emissions log |
| Mitos restart mid-stream | Mitos's resumption uses the cursor point; replays from there; idempotent apply yields same state |

### Recommended dApp `apply_event` shape

```rust
async fn apply_event(&self, ctx: &Ctx, event: OwnershipChange) -> Result<()> {
    // 1. (optional) external IO with .await — fetch metadata, validate, etc.
    let enriched = self.enrich(&event).await?;

    // 2. Build all SQL writes in memory.
    let writes = self.build_writes(enriched);

    // 3. Synchronous SQL exec block — no .await between statements.
    for w in writes {
        ctx.exec(&w.sql, w.bindings)?;  // synchronous
    }
    Ok(())
}
```

The runtime then synchronously appends a cursor-advance exec
in the same gate window. If the dApp follows this pattern,
apply + cursor land atomically.

### Future: `transactionSync` binding

Once worker-rs ships a Rust binding for
`ctx.storage.transactionSync(...)` (or we hand-roll one via
`worker-sys`), the runtime can wrap apply + cursor in an
explicit synchronous transaction. This adds rollback-on-
error semantics for the no-await case (which is what we
already need), but doesn't change the "no await inside
transaction" rule. Track as v1.5; not blocking for v1.

## Addressing & wake-up: mitos dials companions

Cloudflare Durable Objects do not have public HTTPS receiving
addresses. A DO is reachable only via its parent Worker's
fetch handler, which routes inbound requests to specific DO
instances via `id_from_name(key)`. Nothing outside CF can
"dial a DO directly".

This forces a load-bearing direction-of-connection choice.
Resolved 2026-05-05: **mitos dials companions** — never
the reverse. Rationale:

- **WebSocket Hibernation API requires server-side-accepted
  WS** (`state.acceptWebSocket(ws, tags)`). Outbound (DO-
  initiated) WSs are regular client WSs — DO has to stay
  alive to hold them, no hibernation, billing accumulates.
  Hibernation is non-negotiable for our cost model.
- This is also the existing `collections-mitos` shape, so
  the runtime preserves a working pattern.

### Topology

1. **dApp Worker** exposes a stable HTTPS route that handles
   inbound WS upgrades, e.g.
   `wss://collections-mitos.cnft.dev/_internal/replicate?key={key}`.
2. **Worker fetch handler** upgrades the WS and routes to the
   right Companion DO via
   `env.OWNERSHIP_DO.id_from_name(key).get_stub().fetch(req)`.
3. **Companion DO** accepts the WS via
   `state.acceptWebSocket(ws, tags)` — gets full Hibernation.
4. **Mitos host** dials the Worker URL with the right `key`.
   The act of dialing wakes/creates the DO via the Worker →
   DO routing chain.
5. Once accepted, the WS is server-held. The DO can hibernate.
   CF wakes it on each `webSocketMessage`. Cost-shape: brief
   CPU spikes per delivery, no always-on charge.

### Module config: declare the dial-back URL template

The dApp Worker URL is known at module-build time (the dApp
team writes both halves), so it bakes cleanly into the
module's `mitos.toml`:

```toml
[companion]
replicate_url = "wss://collections-mitos.cnft.dev/_internal/replicate?key={key}"
auth_header   = "Authorization"
# auth_value is supplied by the companion at subscribe time, not in deploy config —
# the module's redb stores per-companion auth so it stays out of source control.
```

`{key}` is the templating placeholder; mitos substitutes the
companion's `companion_key` at dial time. v1 supports query-
param substitution as shown; path-segment substitution
(`/replicate/{key}`) is identical syntactically and works the
same way.

For the rare per-companion override case (multi-tenant SaaS
with per-customer subdomains, etc.), the subscribe call
accepts an optional `dial_back` block that overrides the
module defaults — see "Subscribe call" below.

### Subscribe call: companion → mitos via HTTPS

A single HTTPS POST does "register-for-delivery": tells mitos
who I am, which interests I want to track, where to dial me,
and where to resume from. Mitos persists the lot, dials back,
and starts streaming.

```
POST https://mitos.host/api/companions/subscribe
Authorization: Bearer <module_auth_token>
Content-Type: application/cbor

{
  module_name:    "ownership-indexer",
  companion_key:  "customer_42",
  resume_from:    Option<ChainPoint>,        // last cursor from companion DO storage
  interests:      Vec<Interest>,             // initial interest set
  dial_back:      Option<DialBackOverride>,  // None → use module config defaults
}

pub struct DialBackOverride {
  pub url:         Option<String>,
  pub auth_header: Option<String>,
  pub auth_value:  Option<String>,
}
```

Response:

```
200 OK
{
  status: "subscribed",
  next_emission_id: u64,
}
```

### Lifecycle flow

```
[dApp Worker first request to companion DO]
                ↓
   [Companion DO wakes; runtime POSTs /api/companions/subscribe]
                ↓
   [Mitos persists registration in module redb;
    dials companion's WS URL using module config + companion_key]
                ↓
   [Worker upgrades WS; routes to DO via id_from_name(key);
    DO state.acceptWebSocket() → hibernation]
                ↓
   [Mitos sends ServerMessage::Connected { last_emission_id } as readiness signal]
                ↓
   [Mitos drains queued rows from module_emissions in chain-point order]
                ↓
   [DO hibernates between deliveries; mitos wakes it via WS frames]
                ↓
   [Subsequent interest mutations: ClientMessage::Interest over held WS]
                ↓
   [WS drop → mitos owns reconnect; re-dials Worker URL]
                ↓
   [Same DO accepts new WS; resume from cursor; drain queued]
```

### Wake-up: mitos drives all dial-ups

Companions don't need DO Alarms for liveness. Mitos wakes
them by dialing whenever there's emission to deliver, and
the Hibernation API keeps the WS server-held across
hibernation cycles. Edge cases:

- **CF eviction (companion fully gone from memory)**: mitos
  dials Worker URL → `id_from_name(key)` materializes a fresh
  DO instance backed by durable storage (cursor + dApp tables
  + interest set survive). DO accepts WS, sees its own
  cursor, can immediately resume.
- **WS drop (network blip, mitos restart)**: mitos detects
  on its side; re-dials. No companion-side action needed.

### Re-registration semantics

Calling `/api/companions/subscribe` again is **idempotent**:
mitos overwrites the stored config + interest set with the
new payload. Useful for:
- URL changes (dApp moves domains).
- Auth rotation.
- Full state reset (companion lost DO storage and is starting
  over).

Companion runtime caches its last successful registration in
DO SQLite (`mitos_companion_registration` row) and skips re-
subscribing on subsequent wake-ups unless config has changed.

### Bootstrapping: companion needs a request to wake first

Edge case for v1: customer signs up, dApp Worker writes its
own DB, but never pings the Companion DO. Mitos has no
registration → can't dial → matched events for that
customer's policies have nowhere to go.

v1 acceptable answer: **dApp Worker pings the Companion DO
during onboarding** to trigger the first subscribe call.
Pattern:

```rust
// In dApp Worker — customer-creation flow
let stub = env.OWNERSHIP_DO.id_from_name(&customer_id)?.get_stub()?;
stub.fetch_with_str("/_internal/wake").await?;  // triggers subscribe
```

The runtime exposes `/_internal/wake` on every Companion DO
class for this purpose. Documented as a required step in the
"new customer onboarding" runbook.

v2 may explore **mitos-initiated wake** — host pings the
Worker URL when matched events arrive for an unregistered
key, prompting the Worker to materialize the DO and trigger
subscribe. Out of v1 scope; the manual ping is fine for the
collections-mitos migration.

### Companion key choice (Q8 substance)

The Companion key is the string passed to `id_from_name()`
that addresses a specific Companion DO instance. The runtime
ships **no API** for it — pure dApp-level concern, derived in
dApp Worker route handlers from the dApp's tenant model.

Recommended shapes:

| Shape | Key | When to use |
|---|---|---|
| **Per-customer (recommended)** | `companion_key = customer_id` | One Companion DO per customer; their Companion holds N policies via dynamic interest. Cross-policy queries local. Scales with customer count, not policy count. **Use this unless you have a specific reason not to.** |
| **Per-resource (acceptable)** | `companion_key = policy_id` | One Companion DO per resource (policy, asset, etc.). Cross-resource queries fan out. Use when each resource truly is a distinct concern with its own schema. Today's collections-mitos shape (pre-dynamic-interest). |
| **Singleton (not recommended)** | `companion_key = "global"` | Single bottleneck; defeats CF per-DO routing; avoid except for narrow coordinator patterns. |

**Switching shapes is a real migration**: Companion key is
load-bearing in four places (DO addressing, host emission
scoping, Worker URL `{key}` substitution, subscribe-call
`companion_key` field). Once production emissions are queued
under one shape, switching requires draining old queues +
re-registering under new keys + accepting transient gaps.
Pick deliberately.

**PR 5 collections-mitos migration** keeps the existing
per-policy Companion key shape during the runtime-validation
phase. Consolidating to per-customer is a separate follow-up
PR — out of v1 scope.

## Emissions log on the host

The host platform maintains a **write-forward emissions log**
per module — a single source of truth for "what did this
module send downstream and what's its status". Failures fall
out as a filter view; replay is "re-emit rows in some
range"; cross-companion aggregation is free.

This replaces the originally-proposed companion-side
`mitos_companion_dlq` table. The companion stays minimal;
operability lives where the operator already runs
`mitos-admin`.

### Schema (host-side redb table per module)

```
module_emissions (per module, in host redb):
    id              u64 (monotonic, autoincrement)
    matched_at      timestamp           (when host found this event for this companion)
    sent_at         timestamp (nullable; set when actually emitted over WS)
    chain_point     CBOR(ChainPoint)
    channel         text
    payload         bytes (CBOR-encoded event)
    companion_id    text (DO instance id; or stable companion key)
    status          text: queued | pending | acked | nacked | timeout
    status_at       timestamp
    error           text (nullable; populated on nacked)
```

One row per match per receiving companion. If 5 companions
have overlapping interest sets, a single matched event
becomes 5 rows. Storage cost scales with active companions ×
matched events; for typical dApp shapes this is ~thousands
of rows per day per module — trivial for redb on local SSD.

### Status lifecycle — emissions log as delivery queue

The emissions log doubles as the host's **delivery queue**.
When a module finds an event matching a companion's interest,
a row is always written; whether it gets emitted over WS
*now* depends on whether that companion is currently
connected.

| Status | Meaning |
|---|---|
| `queued` | Match found; companion is offline (no active WS). Row buffered for future delivery. |
| `pending` | Frame sent over WS; awaiting Ack/Nack. |
| `acked` | Companion confirmed successful `apply_event`. |
| `nacked` | Companion confirmed `apply_event` errored. |
| `timeout` | Frame was sent but no Ack/Nack within 24h (typically WS drop or DO crash mid-handler). |

Transitions:

```
                                     +------- (deliver now)-> pending -+- ack ->  acked
match arrives -+-(WS open)---->------+                                  +- nack -> nacked
               |                                                        +- 24h --> timeout
               +-(WS closed)-> queued -(WS reconnects, drain)-> pending -+
```

### Reconnect: drain `queued` rows in order

When the companion re-connects (`Subscribe { from: <local_cursor>, ... }`),
the host:

1. Looks up `queued` rows for this companion with
   `chain_point > local_cursor`, ordered by `id`.
2. Streams them as `ServerMessage::Apply` frames in order.
   Each row's status moves `queued → pending` as it goes
   on the wire.
3. Continues with new live emissions (which append fresh
   `queued`/`pending` rows depending on whether they're
   delivered while the WS is hot).

The companion processes these as fresh events; idempotent
`apply_event` (Q3 contract) handles any double-apply if the
companion's local cursor was actually ahead of the host's
view. Each frame gets Acked, status `pending → acked`.

This means the emissions log naturally **outlives the WAL
retention window**. If the companion was offline longer than
mitos's WAL keeps blocks, recovery from queued emissions
still works — events are buffered in `module_emissions`,
not just in the WAL. The WAL is for new subscribers
catching up to chain history; the emissions log is for
existing companions catching up to *their* delivery stream.

### Wire protocol additions

`ServerMessage::Apply` gains an `emission_id` field:

```rust
ServerMessage::Apply {
    emission_id: u64,         // NEW — opaque to companion, echoes back in Ack/Nack
    point: ChainPoint,
    channel: String,
    payload: Vec<u8>,
}
```

Two new `ClientMessage` variants:

```rust
pub enum ClientMessage {
    Subscribe { from: Option<ChainPoint>, interests: Vec<Interest> },
    Interest  { op: InterestOp, items: Vec<Interest> },
    Ack       { emission_id: u64 },                    // NEW
    Nack      { emission_id: u64, error: String },     // NEW
}
```

Both ack/nack frames are fire-and-forget from the companion's
perspective; host updates row status on receipt.

### Cursor as derived view

The host can now reconstruct a per-companion effective cursor
from the emissions log:

```
SELECT MAX(chain_point) FROM module_emissions
WHERE companion_id = X AND status = 'acked'
```

The companion still tracks its own `cursor_chain_point` in DO
SQLite (Q4) for its local resume logic. The two are
complementary: companion's local cursor is authoritative for
its own resume; host's emissions log is authoritative for
operator visibility, replay decisions, and cross-companion
aggregation. They reconcile naturally — on WS re-subscribe,
companion advertises its local cursor, host can cross-check
and re-emit any pending/nacked rows the companion missed.

### Operator surface (mitos-admin)

```
mitos-admin emissions list <module> [--status nacked]
                                    [--channel X]
                                    [--since-slot N]
                                    [--companion C]
mitos-admin emissions replay <module> --ids 100-150
mitos-admin emissions replay <module> --since-slot 100
mitos-admin emissions purge  <module> --status acked --older-than 24h
```

Replay is just "re-emit rows" — host walks the rows and
re-sends as fresh `ServerMessage::Apply` frames (with new
`emission_id`s tagged to the replay batch). Companion treats
them as fresh emissions; idempotent `apply_event` (Q3
contract) handles double-apply safely.

### Compaction policy (v1 default)

- **Acked** rows: drop after 7 days (configurable per module).
- **Nacked** rows: retain until explicitly cleared via
  `emissions purge`.
- **Pending** rows: age out to `timeout` after 24h, then
  follow nacked retention.
- **Queued** rows: **never auto-expire**. They're the
  buffered delivery queue for an offline companion; expiring
  them silently would lose events. Operator handles cleanup
  for genuinely-abandoned companions via
  `mitos-admin emissions purge --status queued --companion C`.
  v2 may introduce per-module `queued_max_age_days` config
  if real workloads show abandoned-companion rows piling up.
- **Timeout** rows: retain until cleared (operator decides
  whether to re-emit or accept loss).

### Why this replaces the original DLQ proposal

The originally-proposed `mitos_companion_dlq` on the
companion side conflated two concerns: **failure visibility**
(what went wrong?) and **replay control** (how do we
re-process events?). The emissions log gives us both, plus
audit history of *successful* deliveries, with a single
write-forward log on the host:

- Failure visibility = `WHERE status = 'nacked'` over the log.
- Replay = re-emit rows, by id range or chain-point range.
- Audit = the whole table is your audit trail.
- Cross-companion aggregation = free, since the log is host-side.
- Companion stays minimal — no DLQ table, no `/_admin/dlq`
  endpoint.

## RPC surface

The dApp's `rpc_routes()` returns a `worker::Router` mounted
under `/api/*`. Standard handler signature is the same as
existing worker-rs handlers — no new framework concept. The
runtime contributes:

- `/api/_health` — cursor lag, slot count since boot, uptime
- `/api/_meta` — the dApp's name, version, schema version,
  watched policies (sanitised)

Future v2: typed RPC trait that auto-generates frontend
TypeScript bindings. Out of scope for v1.

## Dynamic interest mechanics

The v1 platform spike used static `policies = [...]` in
`mitos.toml` for convenience while the wasm runtime was being
proven out. That is **not the steady state**. Real companions
(collections-ownership today) need to add and drop watched
policies at runtime — new collection onboards, customer
unsubscribes, agentic alert rule fires, etc. — without
redeploying the host module.

The companion runtime owns this end-to-end:

### Source of truth

The companion DO's storage is the **canonical interest set**
for that companion's scope (whatever Companion key shape the
dApp picked — see Q8). The mitos-side module's filter set is
a **replica** of what the companion most recently asserted.
On any disagreement, the companion wins; the host re-syncs.

```sql
CREATE TABLE IF NOT EXISTS mitos_companion_interest (
    kind         TEXT NOT NULL,        -- 'policy' | 'address' | 'asset' | ...
    value        TEXT NOT NULL,        -- hex policy_id, bech32 address, etc.
    channel      TEXT NOT NULL,        -- which channel this interest feeds
    added_at     TEXT NOT NULL,
    PRIMARY KEY (kind, value, channel)
);
```

### RPC surface (companion → companion)

The dApp's worker drives interest via RPC handlers the runtime
provides for free:

```rust
// Mounted under /api/_interest/* by the runtime
POST   /api/_interest/subscribe    { kind, value, channel }
POST   /api/_interest/unsubscribe  { kind, value, channel }
GET    /api/_interest              -> [{ kind, value, channel, added_at }]
```

The dApp typically wraps these with its own routes (e.g.
collections-mitos's existing `/api/collections/:policy/watch`)
that translate to the runtime's interest mutations.

### Wire protocol additions (companion ↔ host)

Consolidated wire surface across all Q5/Q6/Q7/Q8 resolutions:

```rust
pub enum ClientMessage {
    // WS-bound state assertion (rare; e.g. drift detection / full re-sync).
    // Initial registration happens via the HTTPS subscribe call, not this frame.
    Subscribe { from: Option<ChainPoint>, interests: Vec<Interest> },

    // Dynamic interest mutations over the held WS.
    Interest  { op: InterestOp, items: Vec<Interest> },

    // Emission delivery acknowledgement (Q5).
    Ack       { emission_id: u64 },
    Nack      { emission_id: u64, error: String },
}

pub enum ServerMessage {
    Apply     { emission_id: u64, point: ChainPoint, channel: String, payload: Vec<u8> },
    Undo      { point: ChainPoint },
    Mark      { point: ChainPoint },
    Connected { last_emission_id: u64 },  // first frame mitos sends after dial; readiness signal
}

pub enum InterestOp {
    Add,
    Remove,
    Replace,  // for full re-sync over WS (matches HTTPS subscribe call's interest set)
}
```

Initial registration happens **out-of-band over HTTPS** (see
"Addressing & wake-up" above) — this carries the companion's
full state including dial-back config, cursor, and interest
set in one round-trip. The WS `Subscribe` frame is for
in-session re-assertion, not first contact.

### Module-side ABI

New WIT export on the `mitos:platform/mitos-module` world:

```wit
update-interest: func(op: interest-op, items: list<interest>) -> result<_, string>;
```

The host platform invokes this when WS frames arrive. The
module persists its interest set in its own redb table so
that a host restart without companion still filters correctly
until the companion reconnects and resyncs.

`mitos.toml`'s `policies = [...]` becomes a **bootstrap
default** only — useful for static-config test deploys, but
production companions empty the field and drive interest
exclusively over WS.

### Crash / reconnect semantics

| Event | Runtime behaviour |
|---|---|
| Companion DO evicted | Interest set + dial-back config persisted in DO SQLite; restored on next request; re-registers via HTTPS subscribe call if config changed |
| WS drops | Mitos re-dials Worker URL using stored config; companion DO accepts new WS; resume from cursor |
| Host module restart | Module re-reads its own redb interest + registration tables; mitos re-dials known companions to re-establish WSes |
| Host process restart (no module state) | Module starts empty; companions re-POST subscribe on next dApp Worker request to them, rehydrating registration + interest |
| Cold start (companion + host both fresh) | Module starts empty; first request to companion triggers HTTPS subscribe call → mitos dials back → WS established |

The invariant: **companion's DO storage is the source; the
module's redb table is a write-through cache; the in-memory
filter set is a hot-path replica of redb**.

### Migration path for collections-mitos

The existing collection-ownership worker already exposes
add/remove RPCs and tracks watched policies in its own SQLite.
The migration in PR 5 is mostly a column rename + drop the
hand-rolled WS-frame send code; the dApp's external API is
preserved.

### Delta granularity: immediate, with `bulk_subscribe` reserved for v2

For v1, every `/api/_interest/subscribe` /
`/api/_interest/unsubscribe` RPC call emits its own
`ClientMessage::Interest` frame immediately. No debounce, no
silent batching. RPC returns when the frame has been sent (or
queued for send). The wire format's `items: Vec<Interest>`
field is *reserved* for explicit batching, not used by the v1
default RPC handlers.

Resolved 2026-05-05.

Rationale:
- Steady-state mutation rate is low (single user actions:
  "watch this collection"). Onboarding bursts are rare and
  short-lived.
- WS frame send is cheap; a burst of 100 frames over a hot
  connection is microseconds and host-module's filter set
  mutation is in-memory.
- Immediate-send keeps the consistency window tight — once
  RPC returns, the host has the mutation. Batching introduces
  a window where the RPC has acked locally but the host
  hasn't received the update, which is a subtle correctness
  gap for "after subscribe returns, I will receive matching
  events".
- Each frame is a discrete event in logs — better
  observability for debugging filter mismatches.

### Future improvement: `bulk_subscribe` RPC variant

The wire format `Interest { op, items: Vec<Interest> }`
already supports multi-item payloads. A future
`POST /api/_interest/bulk_subscribe` RPC variant on the
companion runtime would accept `{ items: [...] }` and emit
*one* WS frame carrying all of them. Triggers when a real
workload demands it (operator CSV import, customer migration,
etc.) — not blocking for v1.

Companion-side debounce config (e.g. coalesce all calls
within 50ms) is a separate v2 path, opt-in per dApp, and
never enabled by default. The hard rule for v1: **no silent
batching**. The dApp can predict frame cardinality from RPC
cardinality.

## Multi-channel support

A single companion DO can subscribe to multiple mitos
channels — useful when channels are tightly coupled (typical
case: data is always read together so co-locating their
SQLite slices avoids cross-DO joins). The trait shape (see
above) handles this directly: `MitosCompanion::channels()`
returns one or more `Box<dyn MitosChannelDyn>`, one per
channel.

```rust
impl MitosCompanion for OwnershipPlusMarketplace {
    const NAME: &'static str = "ownership-plus-marketplace";
    type Config = MyConfig;
    fn channels(&self) -> Vec<Box<dyn MitosChannelDyn>> {
        vec![
            Box::new(OwnershipChannel { /* ... */ }),
            Box::new(MarketplaceChannel { /* ... */ }),
        ]
    }
}
```

Runtime opens one WS connection per channel (each with its
own Hibernation tag = channel name), dispatches incoming
events to the right channel impl by tag, and per-channel
typed decode happens via `MitosChannelDyn::apply_bytes` →
`MitosChannel::apply_event`.

Reminder (see "Composition model"): default to
multi-companion (one DO per concern). Reach for multi-channel
only when concerns are tightly coupled at the SQL layer.

## Crate location

`mitos/crates/mitos-companion/`
- Lives in the public mitos repo alongside `mitos-protocol`,
  so both halves of the wire protocol have a single source of
  truth and evolve in lockstep
- Anyone consuming mitos can clone, build, and use the
  companion runtime — no private-repo gates
- Discoverable next to the design docs in `docs/strategy/`
  and `docs/design/` that describe the protocol it implements

The original PR 1 plan placed this crate in
`cnft.dev-workers/types/mitos-companion/` on the rationale
that it depended on `worker-rs` (CF-specific). That rationale
turned out to be weak: `worker-rs` builds fine on host
targets for library work, mitos-companion has no `[[bin]]`
targets, and all 13 tests are pure-Rust round-trip / dispatch
contract tests. The bigger concern — that hosting the runtime
in a private workspace defeats the point of mitos being
public — won out, and the crate was moved here in the move-PR
that landed before PR 2 of the runtime delivery.

## Order of operations

Seven PRs, each shippable. Dynamic-interest mechanics +
emissions log land *before* the collections-mitos migration
because the migration loses functionality vs. the existing
worker without them.

### PR 1 — `mitos-companion` crate skeleton + addressing + WS lifecycle (~700 lines)
- New crate at `mitos/crates/mitos-companion/`
- `MitosCompanion` trait + `MitosChannel` sub-trait +
  `MitosChannelDyn` blanket impl
- `MitosCompanionRuntime<C>` plain struct (no DO macro, no
  DurableObject impl — see "Runtime DO shape")
- **Addressing**: `mitos.toml [companion] replicate_url` +
  `auth_header` config on the host side; runtime POSTs
  `/api/companions/subscribe` on first DO wake
- Host: `POST /api/companions/subscribe` endpoint —
  persists registration in module redb, dials the companion's
  Worker URL using the template + key
- Mitos dials → Worker upgrades → DO `acceptWebSocket` →
  Hibernation API
- `ServerMessage::Connected { last_emission_id }` as
  readiness signal
- `/_internal/wake` endpoint on Companion DO (triggered by
  dApp Worker during onboarding)
- Per-message decode via mitos-protocol
- Cursor read/write helpers (CBOR-encoded `cursor_chain_point`)
- `mitos_companion_registration` row tracking last successful
  subscribe; skip re-subscribe on subsequent wakes unless
  config changed
- Schema migration helpers (the `mitos_companion_meta` table,
  including one-shot migration from split-row format)
- Tests against a synthetic mitos sender (mock WS)

### PR 2 — Dynamic interest wire protocol (~500 lines split host + companion)
- `ClientMessage::Interest { op, items }` in mitos-protocol
  *(already landed in PR 1's wire-types consolidation)*
- Companion runtime: `mitos_companion_interest` SQLite table,
  `subscribe`/`unsubscribe`/`list` RPC handlers under
  `/api/_interest/*`
- HTTPS subscribe call carries initial interest set;
  in-session mutations emit `ClientMessage::Interest` frames
  over held WS (immediate, no batching)
- Host platform: WIT export `update-interest`, module-side
  state-kv-persisted interest set, module's `init` rehydrates
  from state-kv before consulting `policies = [...]` bootstrap
- Host: `mitos.toml` `policies` becomes bootstrap-only default
- Tests: companion-side wire round-trips + interest-row → wire
  Interest translation

**Scope note**: the host-side WS-receive-loop branch that
parses inbound `ClientMessage::Interest` frames and calls
`update-interest` on a running module via a control channel
is **deferred to PR 3**. PR 3 substantially refactors the
WS receive path anyway (to add the `Ack`/`Nack` parsing +
the dial-back path's read loop) so adding the Interest
branch + follower control-channel plumbing there avoids
double-touching the same code. PR 2 ships the wire surface,
RPC handlers, module-side handler, and the wake-time
HTTPS subscribe call (which already includes the full
interest set in `SubscribeRequest.interests` so the host
has the canonical set persisted from first connection,
even before in-session mutation flows are wired).

### PR 3 — Emissions log + Ack/Nack wire protocol (split into 3a foundation + 3b delivery)

**PR 3a — Foundation (landed)**:
- `ServerMessage::Apply` gains `emission_id: u64`
  *(already landed in PR 1's wire-types consolidation)*
- New `ClientMessage::Ack` and `ClientMessage::Nack` frames
  *(already landed in PR 1's wire-types consolidation)*
- Companion runtime: sends Ack after successful apply + cursor
  advance; sends Nack on apply error (cursor still advances)
  *(already landed in PR 1's runtime DO shape)*
- Host: `EmissionsStore` per-module redb log
  (`<storage>/<id>/emissions.redb`) with the full status
  lifecycle (`Queued` → `Pending` → `Acked`/`Nacked`/`Timeout`),
  monotonic ID assignment, queued-for-companion lookup,
  filter-and-purge ops. 7 unit tests pass.
- Host: `companions/subscribe` endpoint returns the real
  `next_emission_id` from the module's emissions log
  (`peek_next_id`), so companions get a sync point on first
  connect.
- Storage helpers: `module_dir_for_companions(id)`,
  `emissions_path(id)`.

**PR 3b — Delivery engine (deferred)**:
- Host: emit-interception path — when a module emits, append
  to `EmissionsStore` (status `Pending` if WS connected,
  `Queued` if not) and dispatch over the held WS.
- Host: dial-back implementation — mitos opens WS to
  companion's Worker URL using the registered `replicate_url`
  template, sends `ServerMessage::Connected { last_emission_id }`
  as the first frame, drains `queued` rows in chain-point
  order before live stream resumes.
- Host: WS-receive-loop refactor — parse inbound
  `ClientMessage::{Interest, Ack, Nack, Unsubscribe}` frames;
  add follower control-channel + `update-interest` host call
  on Interest frames *(work also deferred from PR 2 — see
  PR 2 scope note)*.
- Host: `mitos-admin emissions list/replay/purge` subcommands.
- Compaction: Acked rows drop after 7d, Nacked retained,
  Pending → timeout after 24h, Queued never auto-expires.
- Integration tests: full round-trip apply → ack; apply
  error → nack; offline companion → queued → drain on
  reconnect; operator-driven replay; ack-timeout aging.

**Why split**: PR 3b naturally lands alongside PR 5
(collections-mitos migration), where a real consumer
exercises the dial-back + queued-drain flow end-to-end. PR 3a
ships the data-layer foundation (emissions log, sync-point
wiring) so the rest of the work has somewhere to land. Until
PR 3b lands, the emissions log writes nothing — the value is
infrastructural, not behavioural.

**Operational note (transitional)**: PR 2 added
`update-interest` to the WIT. Module wasm artifacts built
before PR 2 will fail instantiation with `no export
`update-interest` found`. Rebuild module wasm via
`mitos-build` before re-running host tests after pulling
this branch.

### PR 4 — Multi-channel support
- `Channel` type + per-channel dispatch
- WS Hibernation tag → channel name routing
- Per-channel interest scoping (interest rows have `channel` column)
- Tests with two channels (ownership + marketplace)

### PR 5 — Migrate `collections-mitos` to the runtime
- Concrete `OwnershipImpl: MitosCompanion` + `OwnershipChannel: MitosChannel`
- DO wrapper (`#[durable_object]`) forwards to runtime
- Existing `/api/collections/:policy/watch` routes translate
  to runtime's interest mutations
- One-shot cursor migration from split-row → CBOR-encoded format
- Diff outputs against existing impl during validation
- Drop the ~700 lines of boilerplate that the runtime now owns
- Validates the multi-companion model: ownership and
  marketplace event handling moved to *separate* `*Companion`
  DO classes in the same worker (today they're co-located in
  one DO via multi-channel — splitting them is a small refactor
  that surfaces the multi-companion ergonomics under real load)

### PR 6 — RPC surface scaffold + admin endpoints
- `/api/_health` + `/api/_meta` baseline
- dApp's `rpc_routes()` mounted under `/api/*`
- Documentation for typical handler patterns

### PR 7 — Second consumer port (proof of API generality)
- Pick another worker that's hand-rolling its own consumer
- Port to runtime
- Surface any gaps the API still has after PR 5

After PR 7: the runtime API is settled enough to commit to.
v2 work (CIP-30 helpers, tx-template builder, init scaffold)
becomes additive.

## Design decisions log

All eight originally-open questions are resolved. Captured
here as a permanent record of *why* the design landed where
it did, so they don't get rediscovered or relitigated.

1. ~~**`#[durable_object]` macro vs. composition.**~~ **Resolved
   2026-05-05**: Composition. Runtime ships
   `MitosCompanionRuntime<C>` (plain generic struct); dApp
   writes a non-generic `#[durable_object]` wrapper per
   companion type and forwards DO methods. Isolates blast
   radius from worker-rs macro changes; ~30 lines of forwarder
   per DO class is acceptable cost. See "Runtime DO shape"
   above. **Re-validated post-emissions-log**: still holds —
   emissions log is host-side, doesn't touch the runtime DO
   shape.

2. ~~**Event type CBOR shape.**~~ **Resolved 2026-05-05**:
   Per-channel typed `Event` via the `MitosChannel` sub-trait.
   Each channel impl owns its own `type Event:
   DeserializeOwned` — struct, enum, tuple, whatever fits.
   Runtime erases per-channel types via a `MitosChannelDyn`
   blanket impl that handles CBOR decode + dispatch. See
   "Trait shape" above. **Re-validated post-emissions-log**:
   still holds — emission_id is metadata in `ServerMessage::Apply`,
   not part of the channel's `Event`.

3. ~~**DO storage transaction semantics under WS Hibernation.**~~
   **Resolved 2026-05-05**: Output-gate model, not explicit
   transactions. SQLite-backed DOs auto-coalesce contiguous
   `sql.exec` runs (no `.await` between) into atomic implicit
   transactions; WS Hibernation handlers don't redeliver on
   error. Runtime contract: dApp does all `.await` IO first,
   then synchronous SQL writes; runtime appends synchronous
   cursor-advance in the same gate window. dApp mutations +
   cursor must be idempotent. See "Atomicity: output-gate"
   above. **Re-validated post-emissions-log**: idempotency
   contract is now load-bearing for emissions replay too —
   replayed emissions re-run `apply_event`, must converge.
   Ack/Nack send happens after the gate window (regular WS
   send, no atomicity requirement).

4. ~~**Cursor format.**~~ **Resolved 2026-05-05**: Single
   CBOR-encoded `cursor_chain_point` BLOB row. Matches host-
   side `mitos-platform` shape, supports all `ChainPoint`
   variants natively, forward-compat for new fields. Migrate
   from collections-mitos's split-row format one-shot in PR 5.
   See "Cursor format: CBOR-encoded `ChainPoint`" above.
   **Re-validated post-emissions-log**: companion's local
   cursor remains authoritative for resume; host's emissions
   log gives a *derived* cursor view per-companion (via
   `MAX(chain_point) WHERE status = 'acked'`). Two
   complementary sources of truth, not in conflict.

5. ~~**Error handling on `apply_event` failure.**~~ **Resolved
   2026-05-05**: Companion sends `ClientMessage::Nack {
   emission_id, error }` upstream and advances cursor to keep
   streaming. Host writes the error into the row's `error`
   column in `module_emissions` and marks status `nacked`.
   No DLQ table on the companion side. Operator triages via
   `mitos-admin emissions list <module> --status nacked`,
   replays via `mitos-admin emissions replay <module> ...`.
   Idempotency contract from Q3 makes replay safe (re-applied
   events converge). See "Emissions log on the host" above.

6. ~~**Interest delta granularity.**~~ **Resolved 2026-05-05**:
   Immediate, no debounce, no silent batching. Every
   subscribe/unsubscribe RPC emits its own `ClientMessage::Interest`
   frame. Wire format's `items: Vec<Interest>` field is
   reserved for a future explicit `bulk_subscribe` RPC variant
   (v2) — not used by v1 default RPC handlers. See "Delta
   granularity" + "Future improvement: `bulk_subscribe`" under
   Dynamic interest mechanics above.

7. ~~**Interest persistence on the host side.**~~ **Resolved
   2026-05-05**: Module runs forever; interest set persists
   in module redb across process restarts. Matched events for
   offline companions are written to `module_emissions` with
   status `queued` (never silently dropped). On companion
   reconnect, host drains `queued` rows in chain-point order
   before resuming live stream — emissions log doubles as a
   delivery queue that outlives the WAL retention window.
   `queued` rows never auto-expire in v1; operator cleans up
   genuinely-abandoned companions via
   `mitos-admin emissions purge --status queued --companion C`.
   v2 may add `idle_timeout_hours` for auto-stop and
   `queued_max_age_days` for auto-expiry if real workloads
   warrant. See "Status lifecycle — emissions log as delivery
   queue" + "Reconnect: drain `queued` rows in order" above.

8. ~~**Tenant-key derivation for multi-companion fan-out.**~~
   **Resolved 2026-05-05** (via "Addressing & wake-up"
   section above): renamed to **Companion key** to align with
   terminology table. Runtime ships no API for it; dApp's
   choice. Recommended per-customer (`companion_key = customer_id`);
   per-resource (`companion_key = policy_id`) acceptable for
   distinct-schema cases; singleton not recommended. Load-
   bearing in four places (DO addressing, host emission
   scoping, Worker URL `{key}` substitution, subscribe-call
   payload) — must agree end-to-end. Switching shapes is a
   real migration. PR 5 collections-mitos migration keeps
   existing per-policy shape; consolidating to per-customer
   is a separate follow-up PR (out of v1 scope).

## Deferred (don't pollute v1 scope)

- **CIP-30 / CIP-8 wallet auth helpers** — separate
  workstream. Touches frontend coordination + session
  management. Belongs in a `mitos-wallet` sister crate.
- **Tx-template builder** — already partially exists in
  `cardano-tx`; the companion runtime would tap into it but
  not own it.
- **Frontend RPC type generation** — schemars + jsdoc-bridge
  pattern from `mirror-types` works; not framework-internal.
- **`cargo cardano init` scaffold** — depends on the runtime
  API being settled. Pick up after PR 7.
- **`bulk_subscribe` RPC + companion-side debounce** — the
  wire format already supports batched `items: Vec<Interest>`
  payloads, but v1 RPC handlers emit one frame per mutation.
  Add explicit `POST /api/_interest/bulk_subscribe` and/or
  per-dApp opt-in debounce config when a real workload (bulk
  CSV import, customer migration, frame storm) demands it.
- **Idle module auto-stop** — v1 modules run forever. Add
  per-module `idle_timeout_hours` config in `mitos.toml` if
  real workloads show meaningful CPU cost from orphaned
  modules with long-disconnected companions.
- **Queued emission auto-expiry** — v1 keeps `queued` rows
  forever. Add per-module `queued_max_age_days` config if
  abandoned-companion rows accumulate enough to matter.
  Operator-driven `mitos-admin emissions purge` covers the
  v1 case.
- **Auth federation / per-team tokens** — mirrors the
  platform-side multi-user auth roadmap. Same trigger
  conditions (second team onboards).

## Trigger conditions (when to start)

This work is interesting now (mitos platform v1 just shipped;
companions consume what it publishes). It's only *blocking*
when:

1. A new dApp wants to ship its first companion and we don't
   want them to repeat the 1411-line boilerplate
2. The pain of mirror-types drift in `collections-mitos`
   surfaces a real bug in production
3. A second team wants to ship a Cardano dApp on the stack
   (companion-pattern thesis trigger #3)

Of those, #1 is most likely soon (other dApps in the
cnft.dev-workers tree could move to mitos consumption). #2
is steady-state pain that this work eliminates. #3 is
strategic but not imminent.

## Lessons banked from `collections-mitos`

What's worked well that the runtime should preserve:

- **Per-Companion-key DO routing via `id_from_name(key)`** —
  clean per-companion coordinator, scales to thousands of
  instances, CF routing handles distribution. (Today's
  `collections-mitos` uses per-policy keys; per-Q8 the
  recommended shape for new dApps is per-customer, but the
  routing mechanic is identical.)
- **Hibernation API + reconnect** — survives CF cold starts,
  doesn't burn DO compute on idle subscriptions. The
  mitos-dials-companion direction is what unlocks this; see
  "Addressing & wake-up" above.
- **CBOR over WS** — compact, deterministic, fast; the
  mitos-protocol crate handles versioning
- **Reset + re-backfill** — when schema needs change, drop
  tables + re-subscribe from `Origin`. Works for small-state
  collections; runtime should preserve this escape hatch.

What's been painful and the runtime should fix:

- **Mirror types drift** — `OwnershipChange` shape lives in two
  places (mitos's + the worker's local mirror). The
  `mitos-protocol` crate fixed the framework-internal version;
  the runtime should productise this for dApp teams. Per-
  channel `Self::Event` type with single source-of-truth.
- **Cursor coordination edge cases** — what happens when
  mitos restarts mid-block? When the DO crashes mid-apply?
  Runtime owns the atomicity invariant once, dApps don't
  rediscover it.
- **Auth boilerplate** — every consumer hand-rolls Bearer
  check + Secrets Store binding. Move to runtime.
- **WS Hibernation tags for multi-channel** — easy to get
  wrong; runtime owns it.
