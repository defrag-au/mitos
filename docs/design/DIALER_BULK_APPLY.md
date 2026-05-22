# Dialer bulk apply — batched POSTs with per-emission status

**Status: design draft** (2026-05-16). No code changes yet. Sits
on top of the partition-keyed dialer pool
([`DIALER_CONCURRENCY.md`](DIALER_CONCURRENCY.md)) which shipped
2026-05-14 at `lanes=8`. The pool moved sustained throughput from
~6 events/sec to ~50 events/sec on the production link to
`jpgsm.cnft.dev` — much closer to the WS-transport era's
~700 events/sec but still ~10× short of it.

> **Update 2026-05-23.** Open question 1 (companion idempotency) is
> **resolved** — the v1 runtime audit (see "Idempotency requirement"
> below) shows applies are chain-point-idempotent at the application
> layer, so Phase 1 needs no runtime dedup work. The doc's original
> emission-id dedup model was inaccurate and has been corrected.
> A second motivation has also surfaced from the collection-ownership
> dev recovery: bulk apply fixes recapture **coordination timeouts**,
> not just raw speed (see "Why this also fixes recapture timeouts").
> Still no code; Phase 1/2 are ready to build.

The remaining gap is structural: with the pool deployed, the per-
lane drain is `for emission in queued { POST, await ack, next }`.
Throughput is `lanes × (1 / round-trip-latency)`. At 8 lanes and
150ms median round-trip, that's ~53 events/sec — the observed
ceiling. Adding more lanes hits per-lane HTTP/2 head-of-line
limits and Maestro-style rate-limit pressure from the companion's
side; the marginal lane returns less than the previous one.

This doc proposes **bulk apply**: each POST carries up to M
emissions for one partition lane, the response carries per-
emission status, the host demuxes status back into the emissions
store per row. Round-trip cost is paid once per M events instead
of once per event. With M=50 and the same 150ms round-trip, a
single lane delivers ~330 events/sec; the eight-lane pool
collectively pushes ~2,600 events/sec — past the WS-era number.

The cost is partial-success orchestration: the response model has
to encode "some applied, some rejected, some not attempted" and
the host has to be precise about which emissions stay queued,
which get acked, and which get nacked. This doc is mostly about
getting that right.

Cross-references:
- [`DIALER_CONCURRENCY.md`](DIALER_CONCURRENCY.md) — the
  partition-keyed pool this builds inside. Bulk apply preserves
  the same partition-key contract: within one POST, emissions
  belong to the same lane.
- [`EVENT_DELIVERY_RESILIENCE.md`](EVENT_DELIVERY_RESILIENCE.md) —
  companion piece on not silently losing events. Bulk apply must
  not introduce a new silent-drop mode under partial response.
- [`RECAPTURE.md`](RECAPTURE.md) — the operation that benefits
  most from bulk. A recapture that emits 4,300 UTxOs takes
  ~75 seconds at `lanes=8` today; at M=50 it'd be ~3-5 seconds.
- [`SUBSCRIPTION_MECHANICS.md`](SUBSCRIPTION_MECHANICS.md) — the
  subscribe handshake where companions might advertise bulk
  support (open question; see below).
- `crates/mitos-platform/src/dialer/pool.rs` — the per-lane drain
  this rewrites.
- `crates/mitos-platform/src/emissions.rs` — the row status
  schema this still uses unchanged.
- `crates/mitos-companion/` — companion runtime SDK; the new
  `apply-bulk` handler lives here.

## What the pool already does

Just to set the baseline. Per
[`DIALER_CONCURRENCY.md`](DIALER_CONCURRENCY.md):

1. Each emission row has a partition key. Within a lane, emissions
   drain in slot order; lanes drain in parallel.
2. Per-emission drain is `POST /apply -> ack/nack/timeout`,
   status written back to the emissions table.
3. Failures map to retry on the next tick (`Queued`), permanent
   nack (`Nacked`, 422-class), or pending (`Pending`, mid-flight).

Bulk apply changes step 2 only. Steps 1 and 3 are unchanged —
partition keys still define ordering and parallelism, the
emissions store still tracks per-row status. The change is
purely in the wire shape of the POST, plus the companion-side
handler that interprets a batch.

## Why this also fixes recapture timeouts (not just speed)

Observed during the collection-ownership dev recovery (2026-05-23,
the SB6 cross-contamination fix). A `companion=*` recapture of
`collection-holders` over two policies **timed out** — `1 ready,
1 timed out` — and the host correctly aborted the bootstrap-refill
rather than seed ghost rows.

The cause was throughput, not a recapture bug. One companion's DO
(a Cloudflare Durable Object) was saturated draining an in-flight
cold-start backlog: ~1,500 tiny per-emission Apply POSTs, applied
one-at-a-time because CF DOs **serialise all inbound requests**.
The recapture's `on_recapture` POST queued behind that backlog and
couldn't ACK inside the per-companion timeout, so coordination
failed.

Bulk apply removes the saturation: the same backlog drains in a
handful of batched POSTs instead of ~1,500 single-row ones, the DO
request queue stays short, and `on_recapture` is serviced
promptly. So bulk apply is not only "recapture finishes faster"
(75s → ~3–5s per the recapture cross-ref) but "recapture
**coordination stops timing out** under any concurrent
cold-start/backfill load." For multi-policy hosts this is the
difference between recapture being reliable and being a coin-flip
whenever a DO is busy.

(The dev recovery's slowest leg was actually the dynamic-interest
`Add` cold-start, which dispatches many tiny per-batch Delta
emissions rather than a few large `SnapshotChunk`s — the
worst-case shape for the per-row drain, and the best-case win for
bulk.)

## Response model — per-emission results vs applied-through

Two models were considered. The conclusion is **per-emission
results**.

### Option A — per-emission results (chosen)

```json
POST /_internal/apply-jpg-store-offer-bulk?key=jpg-co
Body:
{
  "channel": "OfferEvent",
  "emissions": [
    { "id": 1234, "payload": {...}, "matched_at": "..." },
    { "id": 1235, "payload": {...}, "matched_at": "..." },
    { "id": 1236, "payload": {...}, "matched_at": "..." }
  ]
}

Response 200:
{
  "results": [
    { "id": 1234, "status": "applied" },
    { "id": 1235, "status": "applied" },
    { "id": 1236, "status": "rejected", "error": "datum hash mismatch" }
  ]
}
```

Host iterates `results`, marks each emission Acked or Nacked in
its store. Mirrors the current per-row 422 semantics exactly —
one bad emission marks itself Nacked, the rest in the batch
proceed normally.

### Option B — applied-through cursor (rejected)

```json
Response 200:
{ "applied_through_id": 1235, "rejected_at_id": 1236, "rejected_error": "..." }
```

Companion guarantees strict-in-order application, stops at the
first rejection, reports the watermark. Simpler response shape.
Host marks everything ≤ `applied_through_id` Acked, the rejection
Nacked, everything after stays Queued.

**Why A wins.** Option B has a trap: a single 422 stalls the
entire remainder of the batch — and on retry, the dialer would
replay starting with the rejected emission, hit the same 422,
stall again. The "skip past the bad one" logic has to live
somewhere; in B it has to be an admin action (manually marking
the row Nacked so the next retry skips it), in A it's just the
protocol. Option A preserves the operational ergonomics modules
already rely on: 422 is per-row terminal, the lane keeps moving.

### What about 5xx?

A 5xx response on the whole POST means **no rows count as
applied**. Companion is expected to be all-or-nothing under
internal errors: it either commits the whole batch and responds
200, or commits nothing and responds 5xx. The host marks all
emissions in the batch as still Queued (back from Pending) and
retries on the next tick.

The "applied but response lost" case (network drop after
companion committed) is covered by idempotency below.

## Idempotency requirement

**Resolved by audit (2026-05-20, re-verified 2026-05-21).** The
load-bearing property is **chain-point idempotency at the
application layer**, *not* emission-id dedup at the protocol layer.
An earlier draft of this section required companions to track
`emission.id` and dedup on it; that was wrong about how the v1
runtime works, and the dedup it described is neither present nor
needed. Corrected below.

What the audit found in the mitos-companion v1 runtime:
- `emission_id` is opaque to the companion. There is **no**
  persistent emission-id tracking table (the schema has only
  `mitos_companion_meta`, `_interest`, `_registration`).
- `apply_bytes` (`mitos-companion/src/runtime.rs:235`) is invoked
  **unconditionally** on every Apply — there is no `seen(id)`
  short-circuit.
- Idempotency is the dApp `apply_event` handler's responsibility,
  keyed on **chain points** — `tx_hash`+`output_index` for typical
  Cardano events, slot+hash for the cursor (`INSERT OR REPLACE`).
  This is the documented Q3 contract in
  [`MITOS_COMPANION_RUNTIME_V1.md`](../strategy/MITOS_COMPANION_RUNTIME_V1.md)
  (§Q3, ~lines 640–650).

Why that's sufficient for bulk + retry: Cardano events are
naturally idempotent. Re-applying `Transfer { tx_hash,
output_index }` (or a `SnapshotChunk` of holdings keyed by asset
name) yields the same state; the cursor advance is
`INSERT OR REPLACE` on the chain point. A double-apply on the
"applied but response lost" retry **converges** — it doesn't
corrupt. So the bulk handler can be the plain loop:

```
on apply_bulk(batch):              # batch is one partition (= one
    for emission in batch:         # policy, post-SB6 keying), in order
        try:
            apply(emission.payload)     # chain-point-idempotent
            results.append({ id, status: "applied" })
        except ApplicationError as e:
            results.append({ id, status: "rejected", error: e.message })
    return { results }
```

No `seen(emission.id)` table; the `id` is used only by the host to
demux per-row status back into the emissions store, never by the
companion as a dedup key.

**Implication:** Phase 1 proceeds with **zero runtime-side dedup
work**. The only hard requirement — dApp handlers being
chain-point-idempotent — is already a v1 runtime contract every
companion satisfies today.

## Ordering within a batch

Two guarantees the protocol commits to:

1. **Same partition → same batch, in order.** The host's lane
   accumulator only batches emissions that share a partition
   key. Within the batch, emissions are in slot order. Companions
   apply them sequentially.
2. **Cross-partition: separate batches, separate lanes.**
   Emissions with different partition keys go through different
   lanes; bulk apply doesn't change that. Lanes drain in parallel
   as before.

The companion's `apply_bulk` handler MUST process the batch
sequentially. Internal parallelism within the handler (e.g.
spawning per-emission tasks) breaks the per-partition ordering
guarantee. Documented in the handler trait contract; trivially
satisfied by an idiomatic `for emission in batch` loop.

## Batch sizing and flush triggers

Bounded by two things:

- **Max batch size M.** Cap on emissions per POST. Larger M
  amortizes round-trip cost more aggressively but increases
  per-request memory + transit time. Starting point: `M=50`.
  Configurable via `MITOS_BULK_BATCH_MAX` env var. Companion can
  reject batches larger than its own limit with 413 → host
  halves M and retries.
- **Flush window W.** Time-based flush so a slow trickle doesn't
  pool indefinitely waiting for M to fill. The lane already has
  a tick loop (`POLL_INTERVAL` in `pool.rs`); the flush window
  reuses it. Starting point: `W=100ms`. Configurable via
  `MITOS_BULK_FLUSH_WINDOW_MS`.

The accumulator drains when:
- the next-tick fires *and* there are queued emissions for the
  lane (drain whatever is available, up to M), OR
- the queued count for the lane reaches M (immediate drain).

At tip dispatch rates (a few events/sec), the time-based flush
dominates — bulk gives almost no benefit but no harm either.
During recapture or backfill the count-based flush dominates and
the benefit is maximised.

## Host-side drain loop

Per-lane drain transitions from the current shape:

```rust
// today
for emission in queued_for_lane {
    status_writer.set_pending(emission.id);
    let res = client.post(apply_url).json(&emission).send().await;
    match res.status() {
        200 => status_writer.set_acked(emission.id),
        422 => status_writer.set_nacked(emission.id, err),
        5xx => status_writer.set_queued(emission.id),  // retry next tick
        ...
    }
}
```

to:

```rust
// bulk
let chunk: Vec<Emission> = queued_for_lane.into_iter().take(M).collect();
for e in &chunk { status_writer.set_pending(e.id); }

let body = ApplyBulkRequest { channel, emissions: chunk.clone() };
let res = client.post(apply_bulk_url).json(&body).send().await;
match res.status() {
    200 => {
        let parsed: ApplyBulkResponse = res.json().await?;
        // Build {id -> status} for O(1) demux
        let seen: HashMap<u64, &Result> = parsed.results.iter()
            .map(|r| (r.id, r)).collect();
        for e in &chunk {
            match seen.get(&e.id).map(|r| r.status) {
                Some("applied")  => status_writer.set_acked(e.id),
                Some("rejected") => status_writer.set_nacked(e.id, err),
                None             => status_writer.set_queued(e.id),  // companion truncated
            }
        }
    }
    404 | 415 => fall_back_to_single_row(chunk),  // see Migration
    5xx       => for e in &chunk { status_writer.set_queued(e.id); },
    _         => for e in &chunk { status_writer.set_queued(e.id); },
}
```

Three things worth flagging:

- **Missing `id` in `results`.** If the companion responds with
  status 200 but omits an emission from `results`, the host
  treats it as Queued (back from Pending) and retries next tick.
  This is the defensive case for a companion that truncated its
  response or hit an internal limit mid-batch.
- **Extra `id` in `results`.** If the companion responds with a
  result for an emission we didn't send, ignore it (don't touch
  the store).
- **Status writer batching.** The current
  `spawn_status_writer` already drains its mpsc in a single redb
  transaction (`pool.rs:148`). Bulk fits naturally: M acks land
  in the same write tx. Latency parity with the per-row path.

## Migration — endpoint coexistence

Bulk lives at a new URL: `/_internal/apply-<module>-bulk?key=…`.
The single-row `/_internal/apply-<module>?key=…` stays.

Per-companion capability is discovered lazily:

1. Host tries bulk on first drain for a `(companion, target)`.
2. On 200 or 5xx, the companion supports bulk; cache the
   capability.
3. On 404 (route not found) or 415 (Unsupported Media Type),
   cache "no bulk" and fall back to the per-row path. Don't
   re-probe for the lifetime of the dial-loop task.
4. Companion shutdowns / restarts naturally re-probe on the
   next attempt.

Avoids a capability negotiation in the subscribe handshake.
Simpler operationally: production companion gets upgraded, host
discovers bulk on the next batch, throughput jumps with no host-
side config change.

Alternative considered: capabilities flag in
`ClientMessage::Subscribe` ([`SUBSCRIPTION_MECHANICS.md`](SUBSCRIPTION_MECHANICS.md)).
Rejected because it ties dialer capability to subscribe lifecycle
— a companion that upgrades mid-life-of-subscription needs to
disconnect-reconnect for the host to notice. Lazy probe lets the
host pick up the new endpoint within one tick.

## Companion-side handler contract

The mitos-companion trait gains a default-impl method:

```rust
trait MitosCompanion {
    /// Existing — apply one emission.
    async fn apply(&self, emission: Emission) -> ApplyResult;

    /// New — apply a batch. Default impl iterates `apply` per
    /// row in order; companions can override for a single
    /// transaction across the batch (faster when state is
    /// transactional).
    async fn apply_bulk(&self, batch: Vec<Emission>) -> Vec<ApplyResult> {
        let mut out = Vec::with_capacity(batch.len());
        for e in batch {
            out.push(self.apply(e).await);
        }
        out
    }
}
```

A companion that wants single-tx semantics across the batch (DO
storage supports `transaction()` blocks) overrides `apply_bulk`
and runs the whole batch inside one transaction. If anything
throws, the transaction rolls back and the response is 5xx — all
or nothing.

A companion that doesn't override gets per-row semantics for
free, with the throughput benefit of one HTTP round-trip per
batch but the application-side cost of M sequential async calls.
Still much faster than M HTTP round-trips.

## Failure modes — full enumeration

| Scenario | Companion observed | Response | Host action |
|---|---|---|---|
| All apply cleanly | M applied | 200 with M `applied` results | M Acked |
| One per-row 422 | M-1 applied, 1 rejected | 200 with `applied`+`rejected` mix | M-1 Acked, 1 Nacked |
| Companion overrides `apply_bulk` with tx, throws mid-batch | 0 applied (rollback) | 5xx | M back to Queued |
| Companion crashes mid-batch (no tx) | K applied (committed), N-K not | (no response or 5xx after timeout) | M back to Queued; retry replays full batch; chain-point idempotency makes re-applying K converge |
| Network drop after companion commits | M applied | (no response) | M back to Queued; retry replays; re-applying M converges (returns 200 with M `applied`) |
| Companion responds 200 but omits some IDs | depends | 200 with K<M results | K mapped to results, M-K back to Queued for retry |
| Companion responds 200 with IDs we didn't send | n/a | 200 | Ignore unknown IDs |
| Companion doesn't know the bulk endpoint | nothing | 404 | Cache "no bulk"; fall back to single-row drain |
| Companion rejects batch size | nothing | 413 | Halve M for this companion; retry |

The two "still queued after retry" cases (truncated response,
crash before response) both rely on **chain-point idempotency**
(re-applying the same chain event converges) to make the retry's
double-apply harmless. That's the load-bearing property — already
a v1 runtime contract, not new work; everything else is mechanical
demuxing.

## Status writer load

The current per-row drain hits redb once per emission (Pending
write, then Acked/Nacked write — two transactions per row, but
the status writer task batches contiguous updates into one tx).

Bulk: one Pending burst of M writes (batched in one tx), then
one Acked/Nacked burst of M writes (batched in one tx). Net:
roughly 2 redb transactions per batch of M emissions, regardless
of M. redb's write throughput is the floor.

This is a strict improvement: M emissions used to cost M× the
status-writer churn even with the existing batching, since
Pending and Acked writes for adjacent emissions could interleave
(serial drain + tokio scheduling = arrivals on the mpsc are
spread out). Bulk concentrates them.

## Open questions

1. ~~**Companion idempotency audit.**~~ **RESOLVED (2026-05-20,
   re-verified 05-21).** The v1 runtime does *not* dedup on
   emission id — `apply_bytes` runs unconditionally, no tracking
   table. Safety comes from chain-point idempotency in the dApp
   handler (a documented v1 contract). No handler-runtime change
   needed. See "Idempotency requirement". The `seen(id)` dedup the
   earlier draft assumed has been removed from this doc.
2. **413 handling.** If a companion's batch-size limit is
   different from ours, do we want config-level negotiation
   (companion advertises max in subscribe) or pure runtime
   discovery (start at M=50, halve on 413, cache)? Lean
   runtime; capabilities flag is the escape hatch if a
   companion really wants a non-default M.
3. **Channel-mixing.** A module may emit on multiple channels
   (e.g. `OfferEvent`, `OfferIndexEvent`). The bulk request
   has a single `channel` field. Two options: (a) one POST per
   channel per batch, (b) channel becomes per-emission. (a) is
   simpler and matches the current per-emission target URL
   structure. (b) is more efficient when channels are imbalanced.
   Lean (a) for now; revisit if multi-channel modules become
   common.
4. **Response size cap.** A batch of M=50 with verbose
   per-emission error strings could push response size into
   the tens of KB. Worth bounding per-emission error length on
   the companion side (truncate at 256 chars?).
5. **Pool dialer reuse.** The lane accumulator lives where today's
   per-row `for row in queued` loop lives. Easiest spike: replace
   the inner body, keep `lanes=8` and partition keys unchanged.

## Implementation phases

Splitting into small landable units, each independently mergeable
and revertable:

**Phase 1 — companion-side handler.**
1. Add `apply_bulk` to the `MitosCompanion` trait with default
   impl.
2. Wire `/_internal/apply-<module>-bulk` route in the companion
   runtime to call `apply_bulk` and emit the per-emission result
   array.
3. ~~Companion idempotency audit + fix.~~ **Done** — audit
   resolved (open question 1); no fix needed, applies are
   chain-point-idempotent.
4. Roll out to `jpgsm.cnft.dev` and a test companion. Verify
   the route responds correctly with hand-crafted POSTs.

**Phase 2 — host-side dial with fallback.**
5. Add `ApplyBulkRequest` / `ApplyBulkResponse` types in
   `mitos-protocol`.
6. Lane accumulator in `pool.rs`: drain up to M, flush on time
   window OR full.
7. Single POST per drain; demux response into status writer.
8. Capability cache (lazy probe; 404 → fall back to per-row).
9. Roll out to production with `M=1` initially (still per-row
   semantically but using the new path) to validate the round-
   trip without changing throughput characteristics. Then ramp
   M.

**Phase 3 — tuning.**
10. Measure recapture latency at M=10, 25, 50, 100 against
    `jpgsm.cnft.dev`. Lock in default.
11. Status writer batching audit — confirm the burst patterns
    don't surprise redb's write path.
12. 413 backoff path (per open question 2).

**Phase 4 (optional) — companion transactional bulk.**
13. Update `jpgsm` (and similar) to override `apply_bulk` with
    a single DO transaction across the batch. Application
    failure within the tx → 5xx → all back to Queued. Removes
    the partial-success case for these companions; the response
    shape stays the same (just always "all applied" or 5xx).

Phases 1 and 2 are the load-bearing ones for throughput. Phase 3
is needed to pick the right M. Phase 4 is a per-companion
optimisation that doesn't affect the protocol.

## Non-goals

- **Pipelining.** Sending POST N+1 before POST N's response
  arrives. The throughput math doesn't need it (M=50 is enough)
  and the partial-success cases get harder with pipelining (a
  POST K's response could overtake POST K-1's).
- **Server-Sent Events / streaming response.** Same — M=50 is
  enough and SSE complicates demux.
- **Bringing back WS dialback.** The shape of this proposal is
  specifically the one that keeps HTTP semantics (idempotent
  retries, statelessness, easy DO hibernation) while recovering
  most of the WS-era throughput. WS revival remains an option if
  bulk doesn't get us there, but isn't this doc's scope.
- **Per-emission ack streaming during a long batch.** The host
  doesn't see partial progress mid-batch — only the final
  response. An M=50 batch that takes 5s means 5s of Pending
  before the demux. Acceptable; the existing per-row path also
  doesn't surface intra-emission progress.
