# Cloudflare replication protocol

How an indexer's materialized view in mitos becomes queryable state in
Cloudflare. The architectural framing is in `ARCHITECTURE.md` under
"Where mitos lives in the stack" — this doc covers the wire-level
protocol and the consumer patterns we expect to support.

The single sentence: **mitos pushes change records over a long-lived
HTTP/2 channel; full-state replay happens via a sealed R2 snapshot
keyed by `(slot, hash)` plus a resume cursor.**

## Why push, not pull

Earlier framing leaned on a CouchDB-style pull-based changes feed. We
moved to push because the asymmetry favours it: CF is the always-on
side, mitos is the maintenance-window side. A pull model means CF has
to retry against a possibly-down mitos; a push model means mitos
buffers locally during its own outages and flushes when CF is reached
again. The reliability burden lands where it's cheaper to handle.

The trade-off accepted: mitos has to maintain an outbound connection
and a per-consumer retransmit buffer. Both are bounded.

## Wire model

Every record on the channel has the same envelope, regardless of
whether it's part of live tail or part of a backlog flush:

```
{
  cursor: { slot: u64, hash: [u8; 32] },
  kind:   Apply | Undo,
  change: <indexer-defined payload>
}
```

`Apply` and `Undo` are the only two kinds. There is **no separate
"snapshot" kind** — the snapshot escape hatch is out-of-band (see
below), so consumer code only ever needs to handle the two events.

Mitos additionally emits periodic `Mark` heartbeats that carry just a
cursor, used by the consumer for "I am up to date" health checks and
liveness detection. Marks are not stored, only observed.

### Encoding

**CBOR is the wire encoding**, with zstd compression at the channel
level (not per-record). Three reasons specific to this stack:

- Upstream is CBOR end to end. Dolos speaks CBOR natively, pallas
  decodes CBOR, change payloads are mostly already CBOR-shaped
  (UTxOs, datums, addresses). Re-encoding to JSON would mean
  hex-stringing every binary field — ~2x size before compression and
  a pointless decode/encode round-trip on the producer side.
- Snapshot files use CBOR+zstd (see "Snapshot mechanics"). Reusing
  the wire encoding for snapshots means one decoder in the consumer,
  and lets us preserve the "synthetic applies during replay are
  byte-identical to live applies" property without a translation
  layer.
- CF Workers handle CBOR fine via `ciborium` on wasm32; no platform
  constraint pushes toward JSON.

**JSON is available as a dev/debug mode**, selected at subscribe time
via `?format=json` query param or `Accept: application/json` header.
Same envelope, same field names, lossy on binary fields (hex-encoded).
Defaulted off in production; defaulted on for `curl`-based inspection.

Considered and rejected: **Protobuf/gRPC** (buys schema enforcement
at the cost of codegen and a build-step burden; for one-repo contracts
inline schema versioning in the CBOR header is sufficient),
**MessagePack** (strictly worse ecosystem fit than CBOR for our
Cardano/Rust stack, no size or speed win), **custom binary**
(premature; CBOR overhead vs handcrafted is in the noise vs network
round-trip).

Per-record schema versioning: each indexer's change payload starts
with a `schema_version: u8` byte, allowing the producer to bump the
shape independently of the framework. Consumers reject unknown
versions on subscribe and the channel falls back to snapshot-redirect
to a snapshot the consumer's version can decode (or fails the
subscribe if no compatible snapshot exists — a deploy ordering issue
the operator has to resolve).

### Cursor semantics

The cursor is `(slot, block_hash)`, matching the Cardano chain's
notion of identity. Slot alone is insufficient because two competing
blocks at the same slot have different hashes during a reorg.

Consumer state must persist its last-applied cursor. On reconnect, the
consumer sends `subscribe(last_cursor)` and mitos replies with one of
three things:

1. **Resume from cursor** — backlog is small enough to stream; mitos
   sends Apply/Undo records starting strictly after `last_cursor`.
2. **Snapshot redirect** — backlog exceeds mitos's retention window,
   or `last_cursor` references a block mitos no longer has. Reply is
   `{ snapshot_url, snapshot_cursor }`. Consumer fetches the snapshot
   from R2, applies it, then re-subscribes with `snapshot_cursor` and
   gets resume-from-cursor.
3. **Fork recognition** — `last_cursor` references a block mitos has
   *replaced* (the consumer was on a fork mitos has since rolled
   back). Reply lists `Undo` records back to the common ancestor,
   followed by the new chain's `Apply` records. Consumer applies them
   in order. This is the same shape as live reorg handling, just with
   a deeper rollback.

### Why one record shape covers both replay and live

Replay-as-applies is the same trick Debezium's incremental snapshot
uses: the snapshot phase emits synthetic Apply records identical in
shape to live ones. Consumer code has one loop, one event handler,
one persistence path. The complexity of "are we replaying or live?"
stays inside mitos's transmit logic, not the consumer's apply logic.

The snapshot redirect is a different shape (sealed R2 object instead
of a stream of records) only because the size warrants it. For
indexers with small views — vesting events, mint events — we can
elide the redirect and just stream the synthetic applies. The
threshold is a per-indexer config setting.

## Snapshot mechanics

Each indexer is responsible for periodically freezing its
materialized view to R2 at a known cursor. The framework provides a
`Snapshotter` helper but the indexer owns the schema.

Snapshot file format:

```
snapshot-<indexer>-<slot>-<hash[..8]>.cbor.zst
  ├── header: { indexer, cursor: (slot, hash), schema_version, count }
  ├── records: [<change-payload>, …]    # same payload shape as Apply
  └── footer: { sha256, byte_count }
```

CBOR for compactness, zstd for transport. The header's
`schema_version` lets consumers reject snapshots they can't decode
without applying garbage.

R2 layout: `mitos-snapshots/<indexer>/snapshot-<slot>-<hash>.cbor.zst`
plus a `latest.json` pointer per indexer. Mitos prunes old snapshots
beyond a retention count.

**Consistency**: a snapshot is taken at a fixed cursor and contains
the full view at that cursor. The producing indexer briefly pauses
writes (or uses a copy-on-write read transaction) while the snapshot
is written. The cursor in the header is exclusive — the consumer
resumes with `cursor + 1` semantics: "give me everything strictly
after this slot/hash".

## Subscriptions

Most indexers can't reasonably watch the whole chain — `OwnershipIndexer`
in particular only cares about the policies CF has registered interest
in. Subscriptions are part of the protocol so the watch set is dynamic
at runtime, not a deploy-time config.

### Lifecycle for a single scope (collection-ownership example)

1. **CF-side trigger.** A user registers `policy_id` for tracking via
   the existing admin flow. The collection-ownership DO for that
   policy is created (or its first request arrives) and finds no
   local cursor — this is a cold subscription.
2. **DO subscribes to mitos.** Over the WebSocket, the DO sends a
   subscribe envelope containing the indexer name, an indexer-defined
   scope payload (here: `{ policy_id }`), and `cursor: Origin`.
3. **Mitos extends its watch set.** The `OwnershipIndexer` adds the
   policy to its in-memory set, persisted to redb so it survives
   restart. From this slot forward, every block touching the policy
   produces `Apply` records on this subscription.
4. **Mitos backfills historical state.** Because the cursor is
   `Origin`, mitos delivers the current ownership state for the
   policy as of tip. Two mechanisms, indexer's choice based on view
   size:
   - *Synthetic-applies stream*: enumerate UTxOs at addresses holding
     the policy via Dolos's by-policy index, resolve owners via
     `domain.state()`, emit one `Apply` per asset over the channel.
     Typical PFP-sized collection (~10k assets) is a few hundred KB
     of CBOR — comfortable inline.
   - *Snapshot redirect*: very large collections (>50k assets, say)
     get a one-shot R2 snapshot + resume cursor reply, so the
     channel doesn't sit busy on a single subscribe.
5. **Live tail begins.** From the cursor mitos returned, every
   relevant Apply/Undo flows naturally.

### Subscribe-at-block-boundary

A subscribe arriving while mitos is processing block `B` is deferred
until after `B` completes. The cursor returned is `(B, B_hash)`; the
consumer's backfill therefore reflects the post-`B` view. The
alternative (replay partial-block deltas) is more invasive than the
one-block subscription delay justifies.

### Unsubscribe

DO-driven, not admin-driven. When a DO observes its own staleness
(no inbound queries for a configurable window, or operator command),
it sends `unsubscribe { scope }`. Mitos drops the scope from the
watch set. Existing snapshots stay in R2 for a retention window so
re-subscription is cheap, then prune.

The DO is the source of truth for "should this still be watched."
Mitos honors the current set of live subscriptions; it doesn't
second-guess them.

### Indexer trait extension

Subscriptions need typed scope payloads, not stringly-typed JSON
blobs. Each indexer declares its own scope type as an associated
type on the trait:

```rust
trait Indexer<D: Domain>: Send + Sync {
    type Scope: serde::de::DeserializeOwned + serde::Serialize + Send + Sync;

    fn name(&self) -> &'static str;
    async fn bootstrap(&mut self, domain: &D) -> Result<ChainPoint>;
    async fn handle_event(&mut self, domain: &D, event: &TipEvent) -> Result<()>;
    fn routes(&self) -> Router;

    /// Default impl: ignore scope, treat all consumers as
    /// "watch everything this indexer produces".
    async fn subscribe(
        &mut self,
        domain: &D,
        scope: Self::Scope,
        cursor: ChainPoint,
    ) -> Result<SubscribeReply> { /* default: full-feed */ }

    async fn unsubscribe(&mut self, scope: Self::Scope) -> Result<()> { Ok(()) }
}

// Indexer-side:
#[derive(Serialize, Deserialize)]
pub struct OwnershipScope { pub policy_id: PolicyId }

impl<D: Domain> Indexer<D> for OwnershipIndexer {
    type Scope = OwnershipScope;
    // override subscribe / unsubscribe with the watch-set logic above
}

impl<D: Domain> Indexer<D> for JpgCoIndexer {
    type Scope = ();   // fixed contract addresses, no per-scope concept
    // accepts default no-op subscribe
}
```

Wire-level decoding (CBOR bytes → `Self::Scope`) happens once per
subscribe message, in indexer-specific glue generated by the bundle
registration helper. After that point every reference to scope is
the typed value — no string-keyed lookups, no `serde_json::Value`,
no smuggled "we don't know what this is" into the framework.

**Object safety trade.** The associated type means `Indexer` is no
longer object-safe (no `dyn Indexer`). The bundle's collection of
indexers therefore can't be a `Vec<Arc<Mutex<dyn Indexer<D>>>>` like
the Phase 1 scaffolding. Instead, `Bundle::add_indexer<I: Indexer<D>>`
is generic and stores per-indexer `Box<dyn IndexerHandle>` adapters
internally — the `Scope` type is erased *only inside the framework's
adapter*, never in user-facing indexer code. Same pattern axum uses
to type-erase `Handler<T>` into `BoxedHandler` inside the router. The
bundle author writes:

```rust
let mut bundle = Bundle::new(domain);
bundle.add_indexer(OwnershipIndexer::new()?);
bundle.add_indexer(JpgCoIndexer::new()?);
bundle.run().await?;
```

…and never sees the type erasure.

### Server placement

The replication WebSocket server lives **in the same axum app the
bundle already runs for indexer HTTP routes**, not a separate
listener or process. A dedicated upgrade endpoint
(`/replicate/{indexer}`) handles the WebSocket handoff and routes
into the framework's connection-handling code; each indexer's
existing `routes()` Router continues to nest under
`/<indexer-name>/...`. One listener, one auth surface, one place to
operate.

## Retention windows

Three numbers per indexer, set in the bundle config:

- **Live retention** — how many slots of recent change records mitos
  keeps available for resume-without-snapshot. Default: ~24 hours of
  slots. Sized to cover normal CF outage windows.
- **Snapshot cadence** — how often a fresh snapshot is written.
  Default: every 6 hours. Trade-off: more frequent snapshots = faster
  recovery from large gaps but more R2 writes.
- **Snapshot retention** — how many old snapshots to keep. Default:
  4 (covers a 24-hour reset window).

Sized so a CF consumer offline for under a day stays in the
resume-from-cursor path, between 1–4 days takes the snapshot path
with a recent snapshot, and over 4 days requires manual intervention
(by then there are bigger problems anyway).

## Push channel mechanics

**WebSocket with the Cloudflare Durable Object Hibernation API.** This
isn't a free choice — it's mandated by CF billing. A naive long-lived
HTTP/2 fetch into a DO bills *active duration for the full lifetime of
the connection* even when idle, because the DO request handler is open
the entire time. At chain pacing that's 24 hours of billed duration
per consumer per day for what's actually a few minutes of work.

The Hibernation API decouples socket lifetime from DO billing: CF
holds the WebSocket, the DO hibernates between messages, and active
duration only accrues while a message is being processed. For our
workload (records arriving as the chain advances, ~20s block cadence,
ms-scale handler work), this collapses billed duration by ~99%
compared to the non-hibernating path.

Mitos initiates the WebSocket as the client; the consumer DO accepts
it via `state.acceptWebSocket(ws)` rather than holding a fetch handler
open. Authentication via a long-lived token in the upgrade request.
Connection stays open indefinitely; reconnect on transport failure.

**Batch records per message.** DO requests cost ~$0.15/M; at naive
one-record-per-message and chain throughput, a single indexer-consumer
pair runs ~7.5M requests/month. WebSocket messages aren't billed
per-record, so packing multiple records into a single message (bounded
by message size limits, ~1 MB on CF) is free and meaningful at scale.
Mitos coalesces records produced within the same block into one
message; the consumer iterates and applies them in order.

Considered and rejected for the CF acceptor:

- **HTTP/2 server-push fetch into a DO** — no hibernation equivalent;
  bills continuous active duration. Cost-prohibitive at >1 consumer.
- **SSE into a Worker** — same problem, no hibernation, plus DO state
  is what we actually want to mutate.
- **Plain HTTP POST every N seconds** — viable as a fallback (no
  long-lived connection at all, billing is per-request), at the cost
  of N seconds of replication lag. Keep in pocket if we hit a
  hibernation constraint, but not the default.

### Cost line items

For one indexer-consumer pair at chain pacing (rough order-of-magnitude):

- **DO active duration**: ~3 min/day (~hibernating most of the time)
  → fractions of a cent per month
- **DO requests**: WebSocket messages, batched per block → ~130k/month
  per consumer → ~$0.02/month
- **R2 PUT (snapshot writes)**: 4 per indexer per day at 6-hour cadence
  → trivial
- **R2 GET (snapshot fetches)**: only on consumer cold-start or large
  gap → typically zero
- **R2 storage**: snapshot size × retention (~4 generations) per
  indexer → cents/month
- **R2 → Worker egress**: free (in-network)

Real cost at scale is dominated by DO storage for the materialized
view itself (which is what `collection-ownership` already pays today
and isn't introduced by this protocol). The replication channel adds
~pennies/month per consumer.

**Backpressure**: mitos buffers up to N records per consumer. If the
consumer is slow and the buffer fills, mitos drops the connection
(not the records — they remain in the indexer's view). On reconnect
the consumer takes the resume path and catches up. This is preferable
to unbounded buffering because it bounds mitos's memory regardless of
consumer count or behaviour.

**Acknowledgement**: consumer sends periodic cursor acks back over
the channel. Mitos uses these to (a) trim its retransmit buffer and
(b) report consumer lag in its `/health` endpoint.

**Multiplexing**: one consumer can subscribe to multiple indexers
over the same connection. Each subscription is independent (own
cursor, own snapshot fallback, own backpressure budget).

## Consumer patterns

Two patterns supported, validated by examining current cnft.dev-workers
flows.

### Pattern A: Durable Object as replica

The natural CF substrate for stateful per-key data. The DO holds a
small SQLite table of "current state at cursor X" plus a cursor
column. Apply mutates rows, Undo reverts them, the DO's own queries
read straight from SQLite.

Validated against `workers/collection-ownership/` — the existing DO
schema (asset → owner, change log, trait bitmap) maps directly. The
existing read APIs (`/api/check`, `/api/owner`, `/api/bundle`) keep
working unchanged because they're already SQL queries against the
DO's storage. Only the ingest path changes: instead of receiving
`TxClassification` from the classifier, the DO receives
`Apply(cursor, change)` from mitos.

Reorg handling is the genuinely new capability the protocol adds —
the existing flow has none, relying on confirmation depth. `Undo`
makes the rollback explicit.

### Pattern B: WebSocket fan-out for browser clients

The `services/holder-map` library is a Rust+WASM frontend that
already speaks WebSocket and consumes `ServerMessage` updates from
some server. Under mitos this becomes: a thin DO subscribes to mitos,
maintains its replica, and re-publishes the `Apply`/`Undo` stream as
WebSocket messages to connected browsers. Browser reconnect triggers
a re-snapshot from the DO's current state — same shape as DO
reconnect to mitos, one level deeper.

This pattern reuses the protocol verbatim; the DO is just a relay.

### What stays in CF, what moves to mitos

The hard rule: **mitos owns chain-derived state. CF owns
side-effect-tracking state.**

Concrete examples:

- "Who owns asset X?" — chain-derived → mitos.
- "Has user Y dismissed this notification?" — user-supplied →
  CF (D1).
- "Did we already send this Discord message?" — side-effect ledger →
  CF (D1, small dedup table). **Crucially, mitos must not know.** If
  mitos resnapshots and replays, the dedup table prevents duplicate
  delivery; the dedup table is small enough that it isn't a migration
  burden.
- "What alert rules does user Z have?" — user config → CF (D1).
- "Visual profile / image / description for asset X" — enrichment
  output → CF (D1, populated by a separate pipeline). Notification
  dispatch waits for these to exist; mitos doesn't.

This split keeps the "mitos can be rebuilt from scratch" property
intact: nothing CF holds is recoverable from chain alone, and
nothing mitos holds is unrecoverable. The two never need to be
re-derived from each other.

## Discord delivery: out of scope for the protocol

Discord webhook reliability from CF has been a recurring pain point.
Rather than absorb that into mitos, the recommended split:

- Mitos pushes `mint-event` change records to a CF DO.
- DO holds a per-channel dedup table (`(asset_id, channel_id) →
  sent_at`) so resnapshots don't cause duplicate posts.
- Discord delivery happens via a small always-on relay process (could
  live alongside mitos on the same VPS, but is logically a separate
  service with a different rebuilable property — its retry/backoff
  state is *not* rebuildable, so it gets durable storage of its own
  outbound queue).

Three reasons not to put delivery in mitos:

1. Mitos must be rebuildable from scratch; "we already sent this"
   is not.
2. Discord rate-limits and webhook flakiness are an HTTP-client
   problem, not an indexing problem; conflating them muddies both.
3. The relay can serve other side-effect destinations (Telegram,
   email, etc.) without mitos growing them.

## Open numbers and decisions

These get resolved during the Phase 2 prototype against
collection-ownership, not now:

- Authentication token format and rotation policy.
- Live retention default per indexer — 24h is a guess; tune from
  observed CF outage durations.
- Whether the snapshot redirect should ever be skipped for very
  small views (vesting, mint events) and the threshold to apply.
- Per-indexer config schema for retention/cadence/encoding choices.

## Validation flows

Three current cnft.dev-workers flows pressure-tested the protocol:

| Flow | Verdict | Notes |
|---|---|---|
| `collection-ownership` | Excellent fit | Pure chain-derived state, idempotent writes already in place. The protocol's `Undo` actually fixes an existing reorg gap. **First migration target.** |
| `holder-map` | N/A as migration target, but validates Pattern B | Frontend library, no CF state. Confirms the WebSocket-relay-via-DO consumer shape works. |
| Mint notifications | Medium fit, blocked on dedup clarification | Chain-derived part is clean. Side-effect part needs CF to own the "already sent" ledger; current code's dedup mechanism is unclear and must be confirmed before migration. **Second target after Discord-relay decision.** |
