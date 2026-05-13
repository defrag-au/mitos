# Event delivery resilience — closing the silent-drop paths

**Status: design draft** (2026-05-12). No code changes yet. The
problems are surfaced from a production triage: jpg.store offer
cancels landed on chain, mitos's emissions log had no
corresponding rows, and the consumer's projection drifted into
zombies. Recapture cleared it; this doc is how we stop needing
to recapture.

The mitos event pipeline has three silent-drop paths between
"chain block decoded" and "consumer's `apply_event` runs." Each
of them currently produces a consumer with **less data than the
chain shows**, with no log line, no metric, and no automatic
recovery. This doc enumerates them, proposes fixes, and
sequences the work.

Cross-references:
- `HOWTO_DEBUGGING_DEPLOYED_MODULES.md` — operator-facing
  triage checklist that surfaces these drops.
- `design/RECAPTURE.md` — today's only recovery mechanism.
- `crates/mitos-data-plane/src/dispatch.rs` — drop site #1.
- `crates/mitos-platform/src/dialer.rs` — drop site #2.
- `crates/mitos-platform/src/host_v2.rs` (`drain_one`) —
  drop site #3.
- Memory: `project_jpg_store_offer_silent_drop.md` (consumer
  side) — the triage that surfaced this work.

## Drop site #1 — dispatcher discards Consumed events with unresolvable prior outputs

`build_tx_batch` (`dispatch.rs:126-144`) builds Consumed events
via `filter_map`:

```rust
let consumed: Vec<UtxoEvent> = tx.inputs.iter().filter_map(|input| {
    let (mut prior_output, mut prior_datum) = resolved
        .get(&(input.oref.tx_hash, input.oref.index))
        .cloned()?;                       // ← silent drop
    backfill_prior_datum(&mut prior_output, &mut prior_datum, &tx.witness_datums);
    Some(UtxoEvent::Consumed(ConsumedEvent { /* ... */ }))
}).collect();
```

The doc comment immediately above (`build_event_batches`, lines
74-78) describes the intended behaviour as "spent / unknown
refs simply absent — the dispatcher emits the corresponding
events with empty-but-typed placeholder shapes." The code does
the opposite: when `resolved.get(...)` returns `None`, the
input is removed from the batch and the module never sees it.

**When this fires in production.** `read_utxos` falls through
to `read_utxo_from_archive` when current state misses (which is
normal for inputs being consumed in the current block).
`read_utxo_from_archive` (`impls/local.rs:153`) needs
`get_block_by_slot` to succeed — and prod dolos prunes block
bodies past a horizon (~September 2025 at time of writing).
Result: any TX consuming an offer/listing/etc UTxO whose create
TX is in a pruned block has its Consumed event silently
dropped.

The symptom is exactly "TX visible on chain, no emission in the
log." This drove the May 2026 jpg.store offer drift.

**Fix.** Honour the doc-comment contract. When `resolved.get`
misses, still emit a `Consumed` event with placeholder
`prior_output`:

```rust
let consumed: Vec<UtxoEvent> = tx.inputs.iter().map(|input| {
    let (prior_output, prior_datum) = match resolved.get(&(input.oref.tx_hash, input.oref.index)) {
        Some((o, d)) => {
            let mut o = o.clone();
            let mut d = d.clone();
            backfill_prior_datum(&mut o, &mut d, &tx.witness_datums);
            (o, d)
        }
        None => (TypedOutput::unresolved(), None),
    };
    UtxoEvent::Consumed(ConsumedEvent {
        cursor: cursor.clone(),
        consuming_tx_hash: tx.tx_hash,
        consuming_tx_idx: tx.tx_idx,
        oref: input.oref,
        prior_output,
        prior_datum,
        redeemer: input.redeemer.clone(),
    })
}).collect();
```

`TypedOutput::unresolved()` returns an output flagged as
`address: ""` (or an explicit `resolution: Resolution::Unknown`
field; the wire shape needs a small decision). Modules can
distinguish on that flag:

- Address-keyed modules (e.g., jpg-store-offer watches V2/V3
  script addresses) skip unresolved Consumed events — they
  can't know the prior output was theirs, but they also lose
  no signal they had before this fix.
- Redeemer-driven modules (jpg-store-offer's Cancel detection
  is one — the redeemer alone tells you the consume's
  semantic) can opt in and emit a partial Cancel even when
  the prior output is gone. The OREF + redeemer are enough.

`event_matches`'s interest filter (`dispatch.rs:240-248`) needs
a small adjustment so an unresolved Consumed never matches an
address predicate — otherwise we'd dispatch a phantom event to
every interest set that happened to be address-keyed.
Module-side opt-in is via a follow-up interest-predicate kind
(`MatchUnresolvedConsumed`) that explicitly subscribes.

**Migration story.** Existing modules don't break: their
interest predicates are all address- or policy-keyed, and the
filter never matches an unresolved Consumed. Modules that *want*
to handle the unresolved case must opt in.

**Why not just widen the dolos archive horizon?** That helps
but doesn't close the path. Even with a longer horizon, any TX
consuming a UTxO older than the new horizon is still silently
dropped. The dispatcher contract is the durable fix; the
horizon is a separate optimisation.

## Drop site #2 — dialer leaves emissions stuck `Pending` after WS death

`dial_and_pump` (`dialer.rs:684`) connects, drains queued
emissions, then runs the pump loop:

```rust
async fn drain_queued(/* ... */) -> anyhow::Result<()> {
    let queued = store.list_queued_for_companion(companion_key)?;
    for row in queued {
        send_msg(sink, &ServerMessage::Apply { /* ... */ }).await?;
        store.update_status(row.id, EmissionStatus::Pending, &now, None)?;
    }
    Ok(())
}
```

The status transition is `Queued → Pending` after a successful
send; `Pending → Acked` on the consumer's `ClientMessage::Ack`.
There is no path that moves `Pending` back to `Queued` on its
own.

If the WS dies between Send and Ack (a 1006 dirty close is the
common shape on CF — Durable Object hibernation tears the
socket without a Close frame), the row is stuck `Pending`
forever. On reconnect, `drain_queued` only sees `Queued` rows;
the orphaned `Pending` row is invisible to the redelivery path.

Recovery today requires operator action:

```bash
mitos-admin emissions-replay <emission-id>
```

which flips the row back to `Queued`. That's fine for a
known-failed delivery; it's terrible as a baseline because
nothing tells the operator the row needs replaying.

**Fix — primary.** On dial-and-pump entry, before
`drain_queued`, reset all `Pending` rows for this companion
back to `Queued`:

```rust
let reset = store.requeue_pending_for_companion(&req.companion_key)?;
if reset > 0 {
    info!(companion = %req.companion_key, count = reset,
          "requeued pending emissions on reconnect");
}
drain_queued(&mut sink, store, &req.companion_key).await?;
```

This is safe by induction on the wire protocol:
- The consumer's `apply_event` is required to be idempotent
  (the existing recapture contract already mandates this —
  bootstrap re-emits Created events on a clean projection).
- An emission that *was* applied but whose Ack was lost in
  flight will be redelivered, the consumer's `apply_event`
  will run idempotently, the new Ack succeeds. No drift.
- An emission that *wasn't* applied (consumer crashed
  mid-apply) will be redelivered and applied for the first
  time.

The cost is double-application for in-flight emissions across
a disconnect. That's acceptable given idempotency is already a
hard requirement.

**Fix — secondary.** Age-out `Pending` rows that have sat
longer than `PENDING_TIMEOUT` (say 60s) into `Timeout` status,
then operator-or-automatic replay. This catches the case where
the WS appears healthy (no close event) but the consumer has
silently died — a TCP keepalive variant. Lower-priority than
the primary fix because the WS-died-cleanly case is far more
common in CF's hibernation model.

**Observability.** Add a counter
`mitos_emissions_requeued_on_reconnect_total{companion}` and
log the count at info on each reconnect. Sustained non-zero
values point at a flaky consumer.

## Drop site #3 — `drain_one` discards emissions when no companion is registered

`drain_one` (`host_v2.rs:907`) iterates the companions directory
for the module and appends one row per registered companion:

```rust
let companions_dir = storage.module_dir_for_companions(module_id);
if !companions_dir.exists() {
    return;                              // ← silent drop
}
let read = match std::fs::read_dir(&companions_dir) { /* ... */ };
for entry in read.flatten() {
    // append one row per .cbor companion file
}
```

If `companions_dir` is empty (or doesn't exist), the wasm
module's emit is silently discarded. No row, no log, no
metric.

This is correct in spirit — there's nowhere to deliver to —
but it's a footgun in two scenarios:

1. **Window between module activation and first subscribe.**
   The host comes up, the follower starts pumping blocks
   through the module before any consumer has subscribed.
   Emissions fire into the void. Recapture papers over this
   on each consumer's first subscribe, but only if the
   consumer is the one driving recapture (it isn't, today —
   recapture is operator-triggered).
2. **A consumer's CBOR file is removed externally.** Today
   only `delete-module` / `evict-module` do this. But the
   coupling is implicit and the consequence is silent.

**Fix.** Always append the row, even if no companion is
registered. Use a special companion-id sentinel like
`"unsubscribed"` (or a separate `unsubscribed_emissions`
table, depending on schema preference):

```rust
let mut wrote = false;
if companions_dir.exists() {
    for entry in fs::read_dir(&companions_dir)?.flatten() {
        // ... existing per-companion append
        wrote = true;
    }
}
if !wrote {
    store.append("unsubscribed", &channel, /* ... */, EmissionStatus::Queued, &now)?;
}
```

When a consumer subscribes, the subscribe handler
(`companions.rs:subscribe_handler`) rewrites every
`companion_id == "unsubscribed"` row for this module to the
new subscriber's `companion_key`. The dialer's existing
`drain_queued` picks them up.

**Cost.** A small amount of disk for unsubscribed-emission
rows that may never be consumed. Bounded by emissions volume
during the no-subscriber window, which in steady-state is
zero.

**When this fires.** Less common than drops #1 and #2 because
the typical lifecycle is "module uploaded → companion
subscribes from a worker that's already deployed." But it
shows up the first time a new community module is rolled out
ahead of its consumer, or after a `delete-module` + redeploy
on the consumer side.

## Sequencing the work

1. **Dispatcher fix (drop #1) first.** This is the one the
   2026-05-12 triage hit. Architecturally clean (honours an
   existing doc comment). Migration story is "modules opt in"
   so existing consumers stay unaffected. Highest leverage.
2. **Dialer pending-requeue (drop #2 primary) second.** Code
   change is one new `EmissionsStore` method plus three
   lines in `dial_and_pump`. Catches the latent
   correctness bug before it bites a consumer that *isn't*
   recoverable via recapture (e.g., a customer-isolated DO
   where recapture would clobber unrelated state).
3. **Pending timeout (drop #2 secondary) third.** Lower
   priority; do it once we have a flaky consumer that
   demonstrates the WS-appears-alive-but-stuck case.
4. **Unsubscribed-emission persistence (drop #3) last.** The
   smallest current blast radius; primarily future-proofing
   for the multi-consumer / staggered-rollout shape.

Each fix is independently shippable — they're orthogonal in
the codebase. Bundling them is convenient for one set of
release notes but not required for correctness.

## What this doesn't address

- **Block-level dispatch gaps.** If mitos's follower itself
  skips a block (decode failure, follower crash mid-block),
  no events fire and none of the drops above are relevant.
  That class is covered by the existing trap workflow plus
  the recapture nuke.
- **Consumer-side idempotency violations.** The fixes assume
  the consumer's `apply_event` is idempotent. If a module
  author writes a non-idempotent handler, double-application
  on requeue will diverge. The mitigation is documentation
  (existing in `MITOS_COMPANION_PATTERN.md`) plus the
  existing recapture protocol that explicitly hard-resets
  the projection.
- **Archive horizon.** Widening dolos's archive horizon is a
  separate optimisation. Drop #1's fix removes the
  *correctness* dependency on the horizon, but a longer
  horizon still reduces the rate of unresolved-Consumed
  events — which is good both for downstream throughput and
  for modules that can't usefully handle the unresolved
  case.

## Risks + open questions

- **Wire compatibility for `TypedOutput::unresolved()`.** Does
  the WIT shape carry this as an explicit `resolution` enum
  or as a sentinel address string? The former is cleaner but
  requires a v2.1 WIT bump. The latter ships in v2.0 but
  pushes the parsing burden into every module. Default
  recommendation: enum, v2.1 bump, since this is the kind of
  thing modules will get wrong silently if it's a string
  sentinel.
- **`event_matches` semantics for unresolved Consumed.** Does
  the absence of an address mean "no interest matches" by
  default, or do we want a separate
  `InterestPredicate::ConsumedUnresolved` opt-in? The latter
  is more explicit but adds a wire kind. Default
  recommendation: opt-in predicate.
- **Idempotency contract surfacing.** We rely on it for the
  dialer fix; today it's a doc note. Worth thinking about
  whether the companion runtime should track applied
  emission_ids and skip re-application server-side rather
  than relying on the dApp's SQL to absorb the double-write.
  Trade-off is per-companion state vs. trusting the dApp;
  punt for now, revisit if a non-idempotent consumer
  surfaces.
- **Recapture interaction.** None of the three fixes change
  recapture's role — it remains the right tool for
  rebuilding a projection that's drifted for any reason. But
  with drops #1 and #2 closed, the *causes* of drift narrow
  to "schema mismatch" and "consumer bug" rather than
  "infrastructure silently lost events." That's the goal.
