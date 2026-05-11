# Unified subscribe — bridging in-tree indexers into companion-runtime

In-tree indexers (`marketplace-indexer`, `mint-burn-indexer`,
`none-match-indexer`) and wasm modules currently reach consumers
through two parallel mechanisms with different registration
models, different lifecycle, and different operator surfaces.
This doc proposes collapsing them into one: companion DOs
register via the existing companion-runtime handshake, but can
target either kind of source on the other end.

After this lands, the legacy `Replicator` (host-side outbound
WS dialer driven by `mitos-admin add`) retires, and operators
have a single subscription model to reason about.

## Motivation

The `DOMAIN_REFACTOR.md` work identified three indexers worth
keeping in-tree as composable host-side primitives — they decode
chain data into typed events that every dApp would otherwise
reimplement:

- `marketplace-indexer` — script-recognition + redeemer decoding
  for jpg.store / wayup / dropspot / SpaceBudz
- `mint-burn-indexer` — walks `tx.mint` directly
- `none-match-indexer` — residual `AssetMovement` emission

These shouldn't move to wasm modules; they're foundational. But
their *consumer-facing path* is the legacy `Replicator` —
operator-driven `mitos-admin add` registrations, mitos dials
out to consumer URLs. Meanwhile, wasm-module events flow to
companion DOs through the modern path: companion-runtime SDK
posts a subscribe handshake, mitos dials back, events flow.

Two paths, same wire format, different registration model.
The `Replicator` is already on the retirement path
(`crates/mitos-core/src/replicator.rs` headers note this), but
it can't actually retire until in-tree indexers have somewhere
else to send events.

This doc closes that gap.

## Today: two parallel paths

```
                       ┌──────────────────────┐
in-tree indexer  ─────→│ broadcast::Sender    │──→ Replicator     ──→ consumer URL
                       │  (per-indexer)       │    (legacy)
                       └──────────────────────┘    (mitos-admin
                                                    add --target-url)

wasm module      ─────→ host_v2::ModuleHostV2 ──→ companion-runtime ──→ companion DO
                                                  (POST /api/companions/subscribe;
                                                   mitos dials back)
```

Both paths terminate in a WS streaming `mitos_protocol::ServerMessage`
records (`Apply`, `Undo`, `Mark`, `SubscribeReply`). The wire
protocol is identical. Only the *which-end-is-which* and the
*registration-shape* differ.

## Proposed: unified path

```
in-tree indexer  ─────┐
                      ├──→  unified subscribe registry  ──→  companion DO
wasm module      ─────┘     (POST /api/companions/subscribe;       (mitos-companion
                             mitos dials back)                      runtime, unchanged)
```

Companion DOs continue using `mitos-companion::post_subscribe`
exactly as today; the SDK gains a typed `SubscribeTarget` enum
so the handshake can declare *what* is being subscribed to
(wasm module vs in-tree indexer) without stringly typed
prefixes. Mitos's `/api/companions/subscribe` handler routes
the dial-back to the right source.

## Type-level change: `SubscribeRequest`

Current shape (in `mitos-protocol::subscribe`):

```rust
pub struct SubscribeRequest {
    pub module_name: String,
    pub companion_key: String,
    pub resume_from: Option<ChainPoint>,
    pub interests: Vec<Interest>,
    pub dial_back: Option<DialBackOverride>,
}
```

Proposed shape:

```rust
pub struct SubscribeRequest {
    pub targets: Vec<SubscribeTarget>,
    pub companion_key: String,
    pub resume_from: Option<ChainPoint>,
    pub interests: Vec<Interest>,
    pub dial_back: Option<DialBackOverride>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubscribeTarget {
    /// Wasm module identity. Today's behaviour. Mitos routes the
    /// dial-back to the module's emit stream via host_v2.
    Module { name: String },
    /// In-tree indexer name (e.g. `"mint-burn"`, `"marketplace"`).
    /// Mitos routes the dial-back to the indexer's broadcast
    /// channel via the bridge described below.
    ///
    /// Indexers marked internal (`Indexer::is_internal()` →
    /// `true`, e.g. `none-match`) are NOT subscribable through
    /// this path — the host rejects such requests with a 400.
    /// See "Indexer visibility" below.
    Indexer { name: String },
}
```

This is a **breaking wire change** for `SubscribeRequest`, but
all existing producers are under our control (the
`mitos-companion` SDK is the only caller). Migration is a
one-line update at each call site: `module_name: "X"` becomes
`targets: vec![SubscribeTarget::Module { name: "X".into() }]`.
The new arms and `Vec` shape are opt-in.

The `module_name` → `targets` rename clarifies that "module"
is no longer the only addressable thing, and the vector shape
gives companions the freedom to declare multiple sources on
one subscribe call (see "Multi-target subscriptions" below).

## Host-side routing

Today's `/api/companions/subscribe` handler lives in
`mitos-platform::companions` and is wasm-module-specific. After
this refactor:

```rust
async fn handle_subscribe(req: SubscribeRequest, ...) -> SubscribeResponse {
    match req.target {
        SubscribeTarget::Module { name } => {
            // Existing path — same as today.
            module_subscribe_registry.add(name, req.companion_key, ...)
        }
        SubscribeTarget::Indexer { name } => {
            // New path — looks up the indexer in the bundle's
            // `Vec<Arc<dyn IndexerHandle>>`, registers a dial-back
            // mapping that bridges its broadcast channel to the
            // companion's WS.
            indexer_subscribe_registry.add(name, req.companion_key, ...)
        }
    }
}
```

The `indexer_subscribe_registry` is the new piece. On dial-back
time, it:

1. Looks up the indexer's broadcast channel by name (from a map
   the bundle constructs at startup over its registered
   indexers)
2. Subscribes a `broadcast::Receiver`
3. Pumps received `EmittedRecord` values into a WS connection
   to the companion's dial-back URL, encoded as
   `ServerMessage::Apply` / `Undo` / `Mark`
4. Filters by the companion's `Vec<Interest>` using the
   indexer's `change_matches_scope` per record (existing
   `IndexerHandle` machinery)

This is structurally what `Replicator` does today, just driven
by the modern subscribe handshake instead of operator-driven
`mitos-admin add`.

### Indexer visibility

Not every in-tree indexer should be subscribable from
companions. `none-match-indexer` is the obvious case — it's a
dispatcher-internal residual emitter, and exposing it to
companions would conflate "this is a framework-internal piece"
with "this is a dApp-facing protocol channel."

`Indexer<D>` gains an `is_internal()` method, defaulting to
`false`:

```rust
pub trait Indexer<D: Domain>: Send + Sync {
    // ... existing methods ...

    /// Marks an indexer as framework-internal — not
    /// subscribable from companion modules. Defaults to
    /// `false` (most indexers are public protocol surfaces).
    /// `none-match-indexer` overrides to `true`.
    fn is_internal(&self) -> bool {
        false
    }
}
```

The unified subscribe handler rejects
`SubscribeTarget::Indexer { name }` requests where the resolved
indexer's `is_internal()` returns `true`, with a 400 and a
clear error message naming the indexer. `mitos-admin` and any
operator-facing listing of subscribable indexers should also
filter on this flag.

### Multi-target subscriptions

A single subscribe handshake declares `Vec<SubscribeTarget>`.
The host's bridge opens one dial-back WS per target — i.e. N
WS connections per companion DO for an N-target subscription.
Each WS is tagged with its target identifier (via the existing
WS Hibernation tag pattern) so the companion runtime routes
incoming frames to the right channel.

The companion-side `MitosCompanion` trait already supports
multiple `MitosChannel` impls per DO. The unified subscribe
maps cleanly: one target → one channel → one WS.

**v1 implementation: one WS per target.** Multiplexing
multiple targets onto a single WS (with a target-id field on
each frame) is a future optimisation if WS-per-target proves
expensive at scale; the wire format change to support it is
additive (one extra field on `ServerMessage`), so deferring it
is safe.

### Cursor handling

Both targets use the same `ChainPoint` cursor model. For
wasm-module targets, host_v2 already handles resume from
cursor + replay-from-state-kv-backfill. For indexer targets,
the bridge invokes the indexer's `subscribe()` method
(returning `SubscribeReply::{Resume, SnapshotRedirect, Fork}`
plus an optional backfill `Vec<Self::Change>`) — the existing
trait method that's currently called by the legacy
`replicate_router`'s server-accepted-WS path.

Same trait, same cursor semantics, different driver.

**Backfill ↔ live-tail equivalence.** A guarantee carried over
from the existing trait contract: backfill records are
delivered to the companion *in the same shape* as live-tail
records — i.e. CBOR-encoded `ServerMessage::Apply` frames on
the dial-back WS, indistinguishable from frames that arrive
later from the broadcast channel. The companion's
`apply_event` is invoked identically; it doesn't know (and
doesn't need to know) which were historical. The unified path
preserves this — both target kinds deliver backfill the same
way.

### Interest handling

Wasm modules accept dynamic `update-interest` calls from the
companion runtime. In-tree indexers receive `Interest`-flavoured
scope at subscribe time; today their scope can mutate via
re-subscribe (the `subscribe()` method overwrites). For v1 of
the unified path, in-tree indexer subscriptions accept the
initial `interests` field on subscribe and don't yet expose
dynamic mutation. Adding dynamic mutation later is additive
(a new `ClientMessage::UpdateInterest` frame on the WS).

### Auth

Same `MITOS_AUTH_TOKEN` as today (host's secret store binding).
Companion auth is bearer-token on the `POST /api/companions/
subscribe` call; mitos's outbound dial-back includes a bearer
header the companion verifies on its `/_internal/replicate`
endpoint.

## Companion-side change

`mitos-companion`'s `MitosCompanion` trait already declares a
`NAME` constant the runtime uses for subscribe. Today it's
treated as the wasm-module identity. After this refactor, the
runtime needs a way to know whether `NAME` refers to a module
or an indexer. Two sub-options:

**Option A — explicit target on `MitosCompanion`:**

```rust
pub trait MitosCompanion {
    const NAME: &'static str;
    const TARGET_KIND: TargetKind = TargetKind::Module;  // default
    // ...
}
```

dApps consuming an in-tree indexer override:
```rust
const TARGET_KIND: TargetKind = TargetKind::Indexer;
```

Discoverable, type-level, but spreads two concepts (name +
kind) across a static surface.

**Option B — separate constructors / builder:**

```rust
let runtime = MitosCompanionRuntime::module(state, env, MyImpl);
// or
let runtime = MitosCompanionRuntime::indexer(state, env, MyImpl);
```

The runtime then constructs the appropriate `SubscribeTarget`
on first wake. Slightly more imperative, but keeps
`MitosCompanion::NAME` as a single string and the kind as
constructor-level intent.

**Decision: Option B.** dApps wire the runtime in their
`DurableObject::new`; that's a natural place to declare "this
companion consumes from indexer X" vs "from module Y." Static-
trait-constants are appropriate when the kind is a property of
the type; here it's a property of the runtime configuration.

## Replicator retirement

Once unified subscribe lands and one consumer migrates, the
`Replicator` struct (`crates/mitos-core/src/replicator.rs`) and
its admin surface (`mitos-admin add`, `/_admin/subscriptions`)
become dead weight. Retirement is per-PR:

1. **Land unified subscribe** — both paths coexist; existing
   `mitos-admin add` registrations keep working.
2. **Migrate the one in-flight consumer** that uses
   `mitos-admin add` today (none in production at time of
   writing — `marketplace-indexer` has no live subscribers,
   `collection-ownership-indexer` only feeds the legacy
   `cnft.dev-workers/workers/collection-ownership/` worker
   via service binding, not WS replication).
3. **Delete `Replicator`** + its redb-backed
   `subscriptions.redb` registry + the `mitos-admin add`
   command. `mitos-admin` keeps its other commands
   (upload-module, list-modules, etc.).

Estimated cleanup: ~600 lines (replicator.rs is ~700; some
shared helpers stay).

## What this doesn't change

- **Wasm modules keep emitting via `emit_event`.** The host's
  side of how those events are gathered is unchanged.
- **Companion DOs and `mitos-companion` SDK keep their current
  shape.** Channels, cursor handling, dynamic interest, the
  WS Hibernation wiring — all unchanged.
- **Wire protocol.** `ServerMessage` / `ClientMessage` /
  `EmittedRecord` are unchanged. Only `SubscribeRequest` gains
  the `target` enum.
- **In-tree indexer trait surface (`Indexer<D>`).** Unchanged
  — the bridge consumes existing trait methods (`bootstrap`,
  `handle_event`, `subscribe`, `change_matches_scope`).

## Migration

1. **Land the wire-format change.** New `SubscribeTarget` enum
   in `mitos-protocol`; `SubscribeRequest::module_name` →
   `target`. Both `mitos-protocol` and `mitos-companion`
   bumped together. Existing consumers update one line.
2. **Build the indexer-side registry on the host.**
   `/api/companions/subscribe` learns to handle
   `SubscribeTarget::Indexer { name }` by looking up the
   indexer in the bundle's existing
   `Vec<Arc<dyn IndexerHandle>>`. Dial-back path bridges the
   broadcast channel to the companion's WS.
3. **First consumer: mint-watcher PoC.** Subscribes via
   `MitosCompanionRuntime::indexer(...)` against the
   `mint-burn` indexer. Validates the unified path
   end-to-end.
4. **Second consumer: collection-ownership-on-mitos.** Per
   the integration plan in
   `cnft.dev-workers/docs/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`,
   subscribes against multiple in-tree indexer targets
   (`mint-burn`, `marketplace`, `none-match`) to fold their
   events into ownership projection.
5. **Retire `Replicator`** as detailed above.

Each step is independently mergeable. Step 1 can land before
the host-side bridge is built — old consumers simply keep using
`SubscribeTarget::Module` and the existing path.

## Resolved decisions

1. ~~**Multi-target subscriptions on one WS.**~~ **Resolved:**
   subscribe takes `Vec<SubscribeTarget>`; v1 implementation
   opens one dial-back WS per target. Multi-target on a
   single multiplexed WS is a future optimisation (additive,
   not blocking). `none-match-indexer` and other internal
   indexers (those returning `Indexer::is_internal() == true`)
   are not subscribable through this path — see "Indexer
   visibility" above. Documented in the type-level change and
   "Multi-target subscriptions" sections.

2. ~~**Per-target backfill semantics.**~~ **Resolved:** the
   existing backfill-feels-the-same-as-live-tail guarantee is
   preserved. Backfill records are delivered as
   `ServerMessage::Apply` frames identical to live-tail
   frames; the companion's `apply_event` doesn't know which
   were historical. Both target kinds preserve this. See
   "Cursor handling" section.

3. ~~**What does `mitos-admin` look like after retirement?**~~
   **Resolved:** the CLI keeps its module-related commands
   (`upload-module`, `list-modules`, `restart-module`,
   `delete-module`, `emissions`). Replicator-related commands
   (`add`, `list-subscriptions`, `remove`) are removed
   entirely. Operators who want to inspect subscribed
   companions read the unified registry via a new
   `mitos-admin list-companions` (lands alongside Replicator
   retirement).

4. ~~**Naming collision: indexer name vs module name.**~~
   **Resolved:** enforced at **wasm-module upload time**. The
   host maintains a set of reserved names (the in-tree
   indexers registered with the bundle), and
   `mitos-admin upload-module` returns 400 with a clear error
   if the module's name shadows an in-tree indexer. Startup
   also asserts no existing on-disk modules collide (loud
   failure rather than silent shadowing).

## Cross-references

- `DOMAIN_REFACTOR.md` — the in-tree indexers being bridged
  (mint-burn, none-match) and the marketplace claim adaptation
- `SUBSCRIPTION_MECHANICS.md` — `Interest` types flowing
  through the subscribe handshake
- `INDEXER_TRAIT.md` — `Indexer<D>` trait the bridge consumes
  from
- `cnft.dev-workers/docs/COLLECTION_OWNERSHIP_MITOS_INTEGRATION.md`
  — second consumer of the unified path; integration plan that
  motivates this work
- `crates/mitos-core/src/replicator.rs` — the retiring code
