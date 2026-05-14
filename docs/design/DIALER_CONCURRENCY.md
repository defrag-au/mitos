# Dialer concurrency — partition-keyed parallel delivery

**Status: design draft** (2026-05-14). No code changes yet. The
problem surfaced during a routine `jpg-store-offer` recapture:
the post-WS HTTP dialer ([`dialer.rs:684-770`]) is strictly
serial per `(companion, target)` and delivers at ~6 acks/sec on
the production link to `jpgsm.cnft.dev`. The refill emitted
~7,300 events; the DO's `collection_offers` count climbed at the
same ~6/s rate (348 → 518 across 30s of observation), making
recapture a >15-minute operation where the WS-transport era took
under 10 seconds for an equivalent set.

This is structural, not a one-off load spike. As Cardano's
throughput roadmap lands (Ouroboros Peras, Leios, Input
Endorsers) the live emission rate climbs too, and any module
that wants to keep up with tip will hit the same wall —
recapture just exposed it first because it concentrates the
work.

The hard constraint making this non-trivial: ordering. We
chose serial delivery in v1 specifically because some module
invariants depend on slot-order arrival of correlated events
(jpg-store-offer's `policy_leaders` map needs `OfferAdded` and
`OfferRemoved` for the same policy applied in chain order, even
when the events come from different UTxOs). Naive parallelism
breaks those invariants silently — the DO would converge to a
wrong leader and stay there.

This doc proposes **partition-keyed concurrency**: per-module
declared keys carve the emission stream into independent lanes,
the dialer maintains a bounded pool of per-lane workers, ordering
is preserved *within* a lane but lanes run in parallel. Modules
without cross-event invariants (most of them) get full
parallelism at no cost; modules with them declare the right key
once and keep correctness for free.

Cross-references:
- `crates/mitos-platform/src/dialer.rs` — the serial drain loop
  this redesigns.
- `crates/mitos-platform/src/emissions.rs` — the row schema that
  gains a partition-key column.
- `crates/mitos-platform/wit-v2/world.wit` — the `emit` interface
  that gains a key parameter.
- `docs/design/EVENT_DELIVERY_RESILIENCE.md` — companion piece;
  this doc is about *throughput*, that one is about *not silently
  losing events*.
- `docs/design/RECAPTURE.md` — recapture is the operation this
  speeds up most dramatically.
- `docs/design/UNIFIED_SUBSCRIBE.md` — the per-target dial-loop
  layout this builds inside.
- `cnft.dev-workers` memory: `reference_jpg_mirror_drift_recovery.md`
  — operational triage that surfaced the recapture latency.

## Why the existing parallelism doesn't help

The dialer is already parallel across `(companion, target)` —
[`spawn_per_target_loop` at `dialer.rs:580`] launches one tokio
task per companion's subscribed target, each with its own
emissions table and its own HTTP client. But:

1. **Most modules have one production companion.** `jpg-store-offer`
   has exactly one: `jpgsm.cnft.dev`. So the per-companion
   parallelism degenerates to one lane in practice.
2. **Most modules have one channel.** `jpg-store-offer` has one
   channel (`OfferEvent`). The per-target parallelism doesn't
   split it either.
3. **Within a `(companion, target)` task, drain is serial.**
   [`drain_apply` at `dialer.rs:684`] is a `for row in queued`
   loop — each row is `Queued → Pending → POST → ack/nack`
   before the next row starts. Even if we added a second
   companion or channel, this serial inner loop is the bottleneck
   for any single high-volume stream.

Recapture concentrates everything into one stream, so it hits
this worst case maximally. But the same ceiling applies to live
tip dispatch whenever the chain produces a block with many
events for one module.

## The ordering constraint we have to preserve

Modules can have cross-event invariants where the *result* of
applying events in the wrong order differs from applying them in
chain order. For jpg-store-offer specifically:

- Per-UTxO: `OfferAdded((tx_A, 0))` then `OfferRemoved((tx_A, 0))`.
  Reordering inverts the row's presence. The DO uses
  `INSERT OR REPLACE` for adds and `DELETE` for removes, so the
  final state depends on apply order.
- Per-policy: two `OfferAdded` events for the same `policy_id`
  from different TXs in adjacent blocks. The DO maintains a
  `policy_leaders` aggregate (highest lovelace CO per policy)
  derived from the row set. If the lower-lovelace add applies
  *after* the higher-lovelace add, the leader is momentarily
  wrong; subsequent reads see the stale value until another
  event happens to touch that policy.

The first case (per-UTxO) is universal — any projection of
events into a row keyed by `oref` needs same-oref ordering.

The second case (per-policy aggregate) is module-specific —
modules without aggregates (a flat append-only log of TX
hashes, for example) wouldn't care.

The partition key must cover both: it's the *widest* scope of
state any single event can touch in the module's DO.

## Within-lane slot ordering is the guarantee

Within any one lane, events apply in strict slot order. This is
non-negotiable and follows from the existing pipeline without
extra mechanism:

1. The host's [`drain_one`] in `host_v2.rs` consumes chain
   messages in slot order — that's how dolos hands them to the
   platform.
2. Every match the host produces gets the next monotonic id from
   the emissions log ([`EmissionsStore::reserve_next_id`]). Id
   order is therefore a total order over emissions that respects
   slot order, with intra-slot sub-ordering by `(tx_idx, event
   index within TX)`.
3. The dialer's lane workers drain by id ascending. Within a
   lane, that's slot ascending. Apply order = chain order, full
   stop.

The partition key is a *scope identifier*, not an ordering
signal. The dialer never compares slots, never decodes payloads,
never sorts by `chain_point`. Slot order is a property of the
id-ordered queue, not something the lane logic computes.

This is also why tx_hash-based partitioning (when it's the right
scope, which is rare) doesn't break ordering despite tx_hashes
being unsortable random bits: the *partition assignment*
scatters across lanes, but the *id order within each lane* still
gives slot order. The hash randomness only affects which lane an
event lands in — never the order it arrives there.

The rule for module authors: **events that share an invariant
must hash to the same lane**, and the dialer handles the rest.

## Across-lane coordination: rollbacks and cursors

Within a lane is easy. Across lanes, two things need explicit
cross-lane coordination that didn't matter under serial drain.

### Rollbacks are cross-lane barriers

A `RollbackEvent` ([`wit-v2/world.wit:184`]) is a global
statement: every event with `cursor > to_cursor` is invalid and
the module's derived state must un-apply them. Under serial
drain this was trivial — by the time the rollback was the next
row to apply, all preceding events had already finished applying,
so the module's `handle-events(rollback)` call saw a quiesced
state.

Under parallel lanes, that's no longer true. Lane X might be
applying slot 1005 while lane Y is mid-flight on slot 1003 while
a rollback to slot 1000 sits in the queue. If the dialer
dispatched the rollback eagerly, it would race with Y's in-flight
POSTs and the module's projection would be inconsistent.

Rollback emissions are therefore **barriers**. When the dialer
sees a rollback in the queue:

1. **Stop dispatching new work to any lane.** New emissions stay
   `Queued`; lanes complete only their currently in-flight
   POSTs.
2. **Wait for every lane to ack its in-flight row.** The
   coordinated-backoff worker pool already has the machinery
   for parking lanes; the barrier reuses it.
3. **Dispatch the rollback as a single sequential POST**,
   independent of lane assignment. The module's
   `handle-events(rollback)` runs once, sees a fully-applied
   pre-rollback state, and un-applies cleanly.
4. **Release the pool back to parallel.** The queue now contains
   only post-rollback emissions (possibly including replayed
   events with new ids), which fan out across lanes again.

Cost: every rollback serialises the dialer briefly. Acceptable
— rollbacks are rare (chain reorgs of more than a few blocks
are exceptional), and the barrier window is bounded by the slowest
in-flight POST. Per-lane backoff would have made this strictly
harder; pool-level backoff (already a decision) gives us the
barrier primitive for free.

### Cursor advancement is the floor, not the ceiling

Today the DO's chain cursor advances after each `apply_event`
returns 2xx — it equals the slot of the last successfully-applied
event, which under serial drain is the latest slot the module
has *fully processed*.

Under parallel lanes the "latest slot any lane has touched" is
no longer a meaningful cursor. Lane X applied slot 1005 but lane
Y is still on slot 1000 — claiming "we've processed up to slot
1005" is a lie. The correct cursor is **min(per-lane cursors)**:
the slot below which every lane has definitely applied everything.

Implementation:

- Each lane worker tracks `last_applied_slot` for its current
  drain pass.
- A supervisor task computes `min(all lanes)` after every ack
  and writes the result through `ApplyBody.cursor` to the
  companion.
- Idle lanes (no work for that key right now) report the highest
  slot they've seen, not 0 — otherwise an idle lane would
  artificially hold the floor down. "Highest slot seen" comes
  from the supervisor's queue scan: when the supervisor knows
  every queued event with id ≤ N has been dispatched, every
  lane's effective floor for unassigned keys is ≥ slot(N).

The min-cursor rule has a useful side effect: after a host
restart, resume cursor = min(lanes) means we replay any events
whose slot was past the floor but not yet acked by *all* lanes.
That's exactly the right behaviour — we don't lose acks for
slots > floor (they're idempotent re-applies anyway, since
`apply_event` is required to be idempotent under the existing
contract), and we don't skip anything we hadn't actually
finished.

Rollback barriers naturally re-synchronise the floor: after a
rollback at slot N, every lane's cursor is exactly N, so the
min cursor jumps to N. No special-case handling needed.

## Where the partition key comes from

The platform can't infer the key. The dialer sees CBOR-opaque
payloads ([`EmissionRecord.payload`] is `Vec<u8>`), and host-side
decoding would couple the platform to every module's event
schema. Modules must declare the key per emission.

Today's emit signature (WIT):

```wit
interface emit {
    emit-event: func(channel: u32, event: list<u8>);
}
```

Proposed addition:

```wit
interface emit {
    /// Emit an event onto a channel without a partition key.
    /// All keyless events serialise against each other in the
    /// emission's "global" lane. Use when the module has no
    /// cross-event invariants — equivalent to v1 behaviour.
    emit-event: func(channel: u32, event: list<u8>);

    /// Emit an event with a partition key. Events sharing a key
    /// drain serially in id-order on the dialer; events with
    /// different keys drain in parallel.
    ///
    /// Key choice rule: the key must cover the widest scope of
    /// module-side state this event could touch. If two events
    /// can ever touch the same row / aggregate / index entry,
    /// they must share a key.
    ///
    /// Empty `partition-key` is equivalent to `emit-event` (the
    /// global lane). Keys are opaque to the platform — modules
    /// pick whatever encoding makes the invariant cover correct.
    emit-event-keyed: func(channel: u32, partition-key: list<u8>,
                          event: list<u8>);
}
```

Backwards-compat is automatic: existing modules call
`emit-event` and stay on the global lane (serial, identical to
today). New modules call `emit-event-keyed` and opt into the
parallel path.

Worked example for jpg-store-offer: `policy_id` (28 bytes) is
the right key. Same-policy events serialise (covers both per-oref
and per-policy invariants since per-oref events are always
same-policy); different-policy events parallelise. With ~5k
distinct policies active in production at any time, recapture
parallelises across as many lanes as the dialer worker pool
exposes.

### Key choice by state shape

The key isn't an ordering signal — it's a *scope identifier*
naming the unit of state the event can mutate. Match the key to
the invariant scope:

- **Per-policy projections** (NFT collection trackers, marketplace
  listings keyed by collection) → `policy_id`.
- **Per-address projections** (wallet activity, holder ledgers
  keyed by owner) → `address` or `payment_credential`.
- **Per-asset projections** (per-NFT history, asset-level
  metadata caches) → `policy_id || asset_name`.
- **Per-stake-cred projections** (delegation tracking, rewards
  aggregates) → `stake_credential`.

`oref` (`tx_hash || output_index`) is *only* correct for modules
whose entire state is keyed strictly by UTxO and never aggregates
across UTxOs. This is rarer than it sounds — even a simple
"holder map" module aggregates per-address. When in doubt,
widen the key. The cost of a too-wide key is reduced parallelism;
the cost of a too-narrow key is silent state corruption under
load.

The module shared crate declares the key extractor next to the
event definition so the choice is visible at review time:

```rust
impl PartitionKey for OfferEvent {
    fn partition_key(&self) -> Vec<u8> {
        match self {
            OfferEvent::Added { policy_id, .. } => policy_id.clone(),
            OfferEvent::Removed { policy_id, .. } => policy_id.clone(),
        }
    }
}
```

## Emissions log change

[`EmissionRecord`] gains one field:

```rust
pub struct EmissionRecord {
    pub id: u64,
    pub matched_at: String,
    pub sent_at: Option<String>,
    pub chain_point: ChainPoint,
    pub channel: String,
    pub payload: Vec<u8>,
    pub companion_id: String,
    pub status: EmissionStatus,
    pub status_at: String,
    pub error: Option<String>,

    /// Partition key. Empty = global lane. Populated by
    /// `emit-event-keyed` host-fn; defaults to empty for the
    /// legacy `emit-event` path. CBOR encodes empty as a
    /// zero-length list<u8>, so this is forward-compatible with
    /// existing on-disk rows that don't have the field —
    /// `#[serde(default)]` reads them as empty.
    #[serde(default)]
    pub partition_key: Vec<u8>,
}
```

No schema migration needed — redb stores CBOR-encoded values
and serde defaults handle the missing field. Old rows drain
through the global lane (correct: they came from modules that
didn't declare a key).

A second `list_queued_for_companion_by_lane` query is added that
returns rows grouped by `partition_key`. Implementation: a single
scan that bucket-sorts in memory; the queued set is bounded by
the worker pool's pending capacity so the bucket-sort is cheap.

## Dialer change

The per-target task gains a worker pool. Replace the existing
single `drain_apply` body with:

```text
spawn_per_target_loop(target):
    let workers = LaneWorkerPool::new(N_LANES);
    loop {
        select! {
            outbound = recv_control() => handle_recapture(...)
            _ = tick.tick() => {
                let lanes = store.list_queued_by_partition_key();
                for (key, rows) in lanes {
                    workers.dispatch(key, rows);
                }
            }
            _ = workers.completion() => { /* tick if idle */ }
        }
    }
```

The worker pool exposes two operations:
- `dispatch(key, rows)` — assigns the row batch to the worker
  currently owning `key`, or to an idle worker if `key` is
  unassigned. Once assigned, that worker drains those rows
  serially in id-order.
- `completion()` — fires when a worker finishes its batch and
  releases its key. The supervisor uses this to schedule the
  next `list_queued_by_partition_key` pass.

**Lane assignment policy.** Hash the partition key to a worker
slot mod N. Keeps assignment deterministic across passes (the
same policy lands on the same worker through the recapture, so
its draining stays serial). Hash collisions across distinct keys
just mean those keys share a worker — correct, slightly less
parallel.

**Worker pool size.** Default `N_LANES = 16`. The bottleneck
isn't local CPU (each worker's hot path is `await
client.post(...)`); it's the receiving worker's request budget
and the network. 16 saturates a CF Workers endpoint reasonably
without queuing requests against ourselves. Configurable via
`mitos.toml`:

```toml
[dial.concurrency]
lanes = 16
```

**Coordinated backoff per target endpoint.** All N lanes for a
given `(companion, target)` POST to the *same* Worker URL, so a
5xx is almost always a property of the endpoint, not the
individual lane. Per-lane backoff would have every lane discover
the same outage independently and converge on the same backoff
seconds out of phase — wasted requests during the discovery
window, and ragged recovery as lanes wake at different times.

Backoff state is therefore held by the worker pool, not the
individual workers. When any worker's POST returns a 5xx or
transport error, the pool enters a backoff window: all lanes
park until the timer elapses, then one canary lane attempts a
single POST. 2xx releases the pool back to full parallelism;
another 5xx doubles the backoff and re-parks everyone. The
emission rows themselves stay `Queued` (not `Pending`) during
backoff so a host crash mid-outage doesn't strand rows in the
in-flight status.

422 (semantic Nack) is *not* an endpoint outage and doesn't
trigger pool backoff — only that row is marked `Nacked`, and
the lane continues with the next row in id-order.

**Rollback barrier handling.** When the supervisor's queue scan
encounters a `RollbackEvent` emission, it suppresses dispatch of
that row and any rows after it, parks the pool until every lane
acks its in-flight POST, then dispatches the rollback as a
single non-lane POST. After the rollback's 2xx, the pool resumes
and the cursor floor jumps to the rollback's `to_cursor`. The
machinery is the same as the 5xx backoff path — park lanes,
quiesce, do the thing, resume — just triggered by a queue
condition rather than a transport error.

**Cursor floor reporting.** The supervisor tracks per-lane
`last_applied_slot` and computes `min(lanes)` after every ack.
The min is what's stamped onto outgoing `ApplyBody.cursor` so the
companion's DO advances its persisted cursor to a value that
honestly represents "fully processed below this slot." This is a
small change from today's "slot of the last applied event" —
correct under serial drain, lossy under parallel.

**redb write contention.** Status updates (`Queued → Pending →
Acked/Nacked`) currently happen synchronously around each POST.
With N parallel workers, those writes interleave on the
single-writer redb file. Two options:

1. **Serialise status writes through a dedicated task.** Workers
   send `(id, status)` messages over an mpsc channel; a single
   writer task batches them into one redb txn per tick. This is
   the simpler design and matches the access pattern (writes are
   small, frequent, and tolerate batching latency).
2. **Per-worker write batching with retry.** Each worker holds
   N status updates in memory, opens a redb txn, writes, retries
   on `WriteError::Lock`. More complex; only worth it if the
   batched-writer task becomes the new bottleneck (we don't
   expect it to — the write volume is one row per POST, not per
   event).

We'll start with option 1.

## Recapture in this world

Recapture today:
1. Admin POST → `recapture_module` pushes `Recapture` frame onto
   each companion's outbound channel.
2. Per-target task forwards it as a single POST to the
   companion; awaits 2xx, fires oneshot.
3. Mitos host begins refill walk; emissions for refilled UTxOs
   land in the emissions log as `Queued`.
4. Dialer's tick loop sees the queued rows and drains them
   serially.

Step 4 is what serialised. Under the new design:

1-3 unchanged.
4. Refill emissions land with their declared `partition_key`
   (set by the module's `emit-event-keyed` calls). For
   jpg-store-offer with policy-id keying, ~5k policies → up to
   16 lanes active simultaneously. Each lane drains serially
   (correct), all 16 in parallel (fast).

Expected speedup: bounded by `min(N_LANES, distinct_keys)`. For
recapture workloads that span many keys, ~16x. For live tip
dispatch in normal blocks, usually fewer distinct keys → smaller
speedup, but live volume isn't the bottleneck currently.

## Decisions

- **Per-lane stats** — yes, ship in Phase 4. Expose lane queue
  depth and drain rate via `/_admin/modules/<id>/emissions` so
  operators can spot one-hot-policy starvation.
- **Backoff coordinated per target endpoint, not per lane** —
  all lanes for one `(companion, target)` POST to the same Worker
  URL, so failures are endpoint-scoped. Pool-level backoff (see
  the dialer section).
- **jpg-store-offer is the first migration** — it surfaced the
  bottleneck, has an obvious key (`policy_id`), and gives a
  measurable before/after on `jpgsm.cnft.dev`.
- **No cross-companion ordering** — each companion's emissions
  table and dial task is independent. Partition keying is
  per-companion.
- **Ship dialer first, modules opt in over time** — adding
  `emit-event-keyed` is a non-breaking ABI extension. Existing
  modules call `emit-event` and stay on the global lane,
  identical to today's behaviour. Module rebuilds land
  asynchronously per module.

## Open questions

**Q1: Is the partition key the right shape?**
The current proposal is "opaque `Vec<u8>` chosen by the module
author, scoped to the widest invariant the module's events touch."
That gives total flexibility but no platform-side help — picking
a too-narrow key is a silent correctness bug. Alternatives worth
considering:

- **Typed partition keys** declared in WIT as a sum type
  (`PartitionScope::Policy(bytes) | PartitionScope::Address(bytes)
  | PartitionScope::Asset(...) | ...`) so the platform can
  reason about scope and operators see human-readable lane
  identities in stats. Loses flexibility for unusual modules.
- **Multiple keys per event** — an event tagged with both a
  per-oref key and a per-policy key, the dialer treats them
  as an intersection (both lanes must be free before
  dispatching). Solves "this event touches two scopes" cleanly
  but adds significant dialer complexity.
- **Slot-window keys** — bucket events by `(slot / window)` so
  ordering within a time window is preserved across all
  invariants in that window. Different reasoning model: parallel
  *between* windows, serial *within*. Worth thinking through
  for modules whose invariant scope is hard to enumerate
  (TX-context-driven state).

Currently leaning toward opaque `Vec<u8>` + clear
state-shape-to-key guidance in the module shared crate, but
worth a closer look before Phase 1 lands.

## Phases

**Phase 1 — schema + host fn (1-2 days).**
- Add `partition_key: Vec<u8>` to `EmissionRecord` with serde
  default.
- Add `emit-event-keyed` to `wit-v2/world.wit`.
- Plumb the new host-fn through `host_fns_v2/emit.rs` into the
  emission record.
- Existing modules still call `emit-event` and stay on the
  global lane. Wire and behaviour unchanged on the dialer side.

**Phase 2 — lane-aware dialer (5-7 days).**
- Add `LaneWorkerPool` in `dialer.rs`.
- Refactor `spawn_per_target_loop` to dispatch by partition key.
- Pool-level coordinated backoff (replaces per-task backoff).
- Rollback barrier handling: park-quiesce-dispatch-resume on
  encountering a `RollbackEvent` emission.
- Cursor-floor tracking: per-lane `last_applied_slot` +
  supervisor-computed `min(lanes)` stamped onto outgoing
  `ApplyBody.cursor`.
- Add the serialised status-writer task.
- Configure `lanes` in `mitos.toml`.
- Smoke-test: existing modules (no keys) drain through the
  global lane, end-to-end behaviour identical to today —
  including a forced rollback test to confirm barrier
  semantics on the single-lane path.

**Phase 3 — jpg-store-offer migration (1 day).**
- Update the module's emits to use `emit-event-keyed` with
  policy_id.
- Trigger a recapture against `jpgsm.cnft.dev`, measure refill
  latency, compare against the pre-change baseline (~6 acks/s
  serial vs expected ~80-100 acks/s for 16 lanes).

**Phase 4 — operator tooling (2 days).**
- Per-lane stats in `/_admin/modules/<id>/emissions`.
- `mitos-admin` CLI surfacing of lane depth + drain rate.

**Phase 5 — opportunistic migration.**
- Other community modules adopt keying as they're touched.
- No deadline; benefits accrue per-module.

## What this does not address

- Bulk delivery (one POST carrying many events). Different
  axis: bulk reduces per-event overhead, lane concurrency
  reduces total wall-time. They compose. Bulk is a separate
  proposal; the lane-concurrency work doesn't preclude it.
- Indexer-target lanes. Indexer emissions follow the same dial
  path. The design extends transparently — indexers can declare
  partition keys the same way modules do. Out of scope for v1;
  add when an indexer-side bottleneck materialises.
- Cross-companion replay ordering. Each companion's emissions
  log is independent. The design doesn't change that.
