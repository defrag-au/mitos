# HOWTO: debug a deployed module that isn't behaving

`HOWTO_DEBUG_TRAPS.md` covers the case where a module *crashes* —
the host returns a wasm error and you reproduce locally with
`mitos-run`. This doc covers the case where a module is **alive
and apparently healthy** but a consumer reports missing events:
"this TX landed on chain, the worker DB should have updated, but
it didn't."

Those are different failure shapes. A trap is loud — the host's
`/_admin/modules/<id>` shows it, the consumer's WS reconnects in
a tight loop, alerts fire. A silent drop is quiet — the worker
acks every emission it gets, the host reports no errors, and the
gap only surfaces when a human notices a stale row.

This is the checklist for that second case. Follow it
top-to-bottom on a fresh context; the order is "cheap signal
first, expensive replay last."

Cross-references:
- `HOWTO_DEBUG_TRAPS.md` — sibling guide for the trapping case.
- `design/EVENT_DELIVERY_RESILIENCE.md` — the silent-drop paths
  this guide helps you triage, and the planned fixes for the
  ones that shouldn't be silent.
- `design/RECAPTURE.md` — the operational nuke when triage
  confirms a projection has drifted and you want to rebuild
  cleanly.
- `tools/mitos-admin/` — every command in this doc.

## Prerequisites

- `MITOS_URL` pointing at the host (e.g.
  `https://mitos.defrag.cc`). `mitos-admin` defaults to local;
  set this explicitly.
- `MITOS_AUTH_TOKEN` for `/_admin/*` endpoints.
- A specific symptom to investigate: the consumer-side TX hash
  (or wallet pkh, or chain slot range) where the event should
  have shown up but didn't. Triaging without a coordinate is
  guessing.

## Step 1 — Confirm the host is healthy

```bash
mitos-admin health
```

`uptime` should be growing across calls; if it resets between
invocations the host is restarting and your other diagnostics
are racing the restart loop. Address that first (`journalctl -u
mitos-mainnet -n 200` on the box).

`indexers: none-match` is the normal status — historical
indexer-mode subscriptions don't have to be present for
community modules to work. If `indexers` shows something
unexpected and your symptom is module-driven, that line is a
distraction.

## Step 2 — Confirm the module is loaded + not trapped

```bash
mitos-admin list-modules
mitos-admin get-module <id>
curl -sS -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    $MITOS_URL/_admin/modules/<id>/last-trap
```

You want:
- `list-modules` includes `<id>` with the sha256 you expect
  (cross-check against `mitos-build`'s output for the version
  you intended to deploy — silent skew here is the
  highest-yield bug to catch early).
- `last-trap` returns `no trap fixture captured for this
  module`. If it returns a fixture, you're in
  `HOWTO_DEBUG_TRAPS.md` territory, not this one.

## Step 3 — Inspect the emission log around the symptom

This is the single highest-signal step. Every event the wasm
module emits gets a row here, regardless of whether the
companion is online to receive it. Whether the row exists at all
tells you which side of the rail to look at next.

```bash
# Active backlog only (Queued + Pending — the actionable subset).
mitos-admin emissions --module <id>

# Full history, larger window.
mitos-admin emissions --module <id> --status all --limit 200
```

For each row you care about, the columns to read are:

- **`status`** — `acked` is the happy path. `queued` means
  waiting for the companion to dial in (normal if the consumer
  worker is hibernated). `pending` means sent over WS but no
  ack yet (normal briefly; suspicious if it sits >30s).
  `nacked`/`timeout` means the companion saw it and refused/
  timed out — `--status all` shows these.
- **`matched_at`** — host wall-clock when the wasm module
  called `emit_event`. Translate via `date -u -d
  @<unix-seconds>` to compare with consumer-side timestamps.
- **`chain_point`** — slot + block hash. Use this to map an
  emission back to the on-chain TX that triggered it.

The diagnostic forks here.

## Step 4a — There is an emission row but it's not `acked`

Then mitos has done its job — the consumer is the suspect.

- `queued`: companion isn't connected. Check whether the
  consumer worker is up. For CF DOs, a `wait_until` auto-wake
  in the worker's `fetch` handler is the usual mechanism to
  keep the WS alive; if your fetch handler doesn't fire, the
  DO is cold and emissions queue indefinitely.
- `pending`: companion is connected but the ack hasn't come
  back. Either the consumer is processing slowly (look at the
  worker's CPU/log spew), or the WS died after `Apply` was
  sent but before `Ack`. Today this is a known orphan path
  (see `design/EVENT_DELIVERY_RESILIENCE.md`); the workaround
  is:

  ```bash
  mitos-admin emissions-replay <emission-id>
  ```

  which flips the row back to `Queued`. The dialer picks it
  up on the next poll tick.
- `nacked`: the companion explicitly refused. The `error`
  column has the consumer's message. Most often this is a
  schema mismatch (consumer can't decode the payload —
  module + event-crate version skew).

## Step 4b — There is no emission row at all for the window

Then the wasm module never called `emit_event` for the TXs you
expected. Either the dispatcher didn't route the events to the
module, or the module saw them and silently skipped.

The cheap differentiator first: pull the trapping fixture path
to confirm there's no quiet crash you missed:

```bash
mitos-admin get-module <id>     # last-trap line
```

If it's clean, you've narrowed to two possibilities:

1. **Dispatcher dropped the event.** Today, the data-plane
   dispatcher silently drops `Consumed` events whose prior
   output can't be resolved (`crates/mitos-data-plane/src/
   dispatch.rs::build_tx_batch`, `filter_map` with `cloned()?`).
   The biggest real-world trigger is the dolos archive horizon
   — outputs from blocks past the prune window can't be
   resolved by `read_utxo_from_archive`, so any TX that
   consumes one of those outputs has its consume event
   dropped before the module ever sees it. See
   `design/EVENT_DELIVERY_RESILIENCE.md` for the planned fix.
2. **Module decoded but silently skipped.** The wasm module's
   own logic dropped the event — e.g., datum CBOR didn't
   match the expected shape, address didn't match the watched
   set. Run the offending TX's block through `mitos-run` with
   the artifact you uploaded (see `HOWTO_DEBUG_TRAPS.md` step
   3 for the invocation) to see exactly what the module
   decides on those bytes.

To collect the data for case (2), pull the block CBOR from the
host:

```bash
curl -sS -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
    "$MITOS_URL/_admin/blocks/by-tx/<tx-hash>" \
    -o /tmp/<tx-hash>.block.cbor
```

Replay locally with `--block`. Module-side `logging::log` calls
will tell you which branch of the decoder dismissed the event.

## Step 5 — Reconcile the projection if needed

Once you've confirmed (and fixed, where applicable) the
upstream cause, the consumer's projected state may still hold
zombie rows from the silent drops. Don't try to surgically
delete them — use `recapture`:

```bash
mitos-admin recapture --module <id>
```

`design/RECAPTURE.md` covers the protocol. Briefly: the host
signals every subscribed companion to drop its projected
state, waits for confirmation, then re-runs the module's
bootstrap pass against current-state UTxOs. Zombies disappear,
live state re-materialises.

Recapture is heavyweight enough that you don't want it as a
periodic cron — but it's the right tool when a silent-drop bug
has been live long enough to accumulate drift.

## Worked example: jpg.store offer cancels not finalising (2026-05-12)

Symptom: consumer reported "cancel TXs landed in jpg.store at
21:28 GMT+10, worker's `collection_offers` rows still showing
unspent."

```bash
# 1. Host healthy
$ mitos-admin health
status:        ok
uptime:        7h14m17s

# 2. Module loaded, not trapped
$ mitos-admin get-module jpg-store-offer
id:            jpg-store-offer
sha256:        e43952251812929ea060a67ccb94d5c08ec0e54eab637623c01cb0b7d391315d
$ curl -sS -H "Authorization: Bearer $MITOS_AUTH_TOKEN" \
       $MITOS_URL/_admin/modules/jpg-store-offer/last-trap
no trap fixture captured for this module

# 3. Inspect emissions around the symptom window
$ mitos-admin emissions --module jpg-store-offer --status all --limit 200
# ... 41 rows, all `acked`, queued=0, pending=0.
# Last emission before the symptom: slot 187018998 at 21:28:09.
# Next emission after the symptom: slot 187020934 at 22:00:25.
# 32-minute gap covering the user's TXs.
```

The gap means there are no rows to retry (4a does not apply).
The wasm module wasn't called for those consumes (4b: dispatcher
or module silent skip).

Cross-checking: the consumed offers had been created weeks
earlier, near the dolos archive prune horizon. `read_utxos`
couldn't resolve the prior outputs → dispatcher's
`filter_map(.cloned()?)` silently dropped the Consumed events →
module never emitted → no rows in the emissions log →
consumer's `collection_offers` rows stay zombie.

Resolution: `mitos-admin recapture --module jpg-store-offer`
cleared the projection. The bootstrap pass rebuilt from
current-state UTxOs (only live offers, no zombies). The
underlying dispatcher silent-drop is tracked in
`design/EVENT_DELIVERY_RESILIENCE.md`.

Total time from "items not finalising" to "we know exactly
where the drop happened and how to recover": about ten
minutes. If a fresh context spends much longer than that
following this checklist, file an issue against this doc with
the missing step.
