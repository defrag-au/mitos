# event-merge

Merges `chain-events` NDJSON streams from several producers into one, in chain order.

The producers are independent and each writes the same envelope: `token-ledger walk
--events` reads the Mithril chunks, `policy-events` reads a sealed archive, and the
marketplace tap (when it lands) will read `market-ledger`. They interleave block by block
and transaction by transaction, so a consumer that wants a policy's history wants them
merged — not concatenated.

```sh
# Two producers, one collection.
event-merge --policy <policy-hex> holder.ndjson market.ndjson > perps.ndjson

# As many as there are — a third is another argument, not another code path.
event-merge --policy <hex> chunks.ndjson archive.ndjson market.ndjson > perps.ndjson

# From the walks directly, without a file in between.
event-merge --policy <hex> - - < market-and-holder.ndjson
```

The merged stream goes to stdout; the report goes to stderr, so a stream is only ever
events.

## What the order is

`(slot, tx index, event index)` — the chain's own ordering. `family` and `id` tie-break
facts that share a position, so a buyer and a seller of one asset in one transaction still
have a fixed order that does not depend on how the command was typed.

A source that cannot state a position says zero, and a source that cannot state a block
sorts **last**: a fact with no block cannot be claimed to have happened before one. The
report says how many were unplaced rather than pretending they fit.

## Duplicates and corrections

Rows are deduplicated by `EventId`, which is derived from the fact — so the walk and the
archive agree on the same movement, and re-reading a window does not add a row.

- Same id, higher `revision` → replaces the earlier row (`superseded`).
- Same id, same revision, **different position** → a *conflict*: one producer is wrong.
  The better-placed copy is kept, so the result is a function of the facts rather than of
  argument order, and the count appears in the report.

Dedupe runs before the sort on purpose: two copies of one fact can carry different
positions, and sorting first would put them far apart.

## Memory

The merge is streaming. Each feed is read one event at a time, and a slot is released —
written, then dropped — only once every feed's head has moved past it. The working set is
one block's worth of events when the feeds keep pace, and the gap between them when one
lags. The report prints `peak held`, which is that number.

Measured on the local Perps data: **1,445 events merged with itself, peak held 56**; the
**18,382-event archive stream, peak held 214**.

Two cases cost their own size:

- A **newest-first** source — `policy-events` pages that way — is read whole and sorted
  before the merge starts, because its smallest event is its *last* line. The report names
  it `descending (buffered)`.
- A fact with **no block** (a marketplace message) cannot be bounded by a slot at all, so
  it waits for the end. It sorts last, and the count is reported.

## The report

```
# event-merge · 2 sources · 19827 read · 18382 kept · 0 filtered · 1445 duplicates · 0 superseded · 1276 conflicts · 0 unplaced · peak held 214
#   .tmp/walk.ndjson  1445 events
#   .tmp/archive.ndjson  18382 events · descending (buffered)
# span 170958295–201583758
```

## Limits

- **It will not invent order.** If a producer emits `tx.index: 0` for every row, the merge
  is exact to the slot and arbitrary within it. `--policy` does not change that.
- **A source must ascend by slot.** A producer's own order *inside* a block is fine — the
  merge sorts within a block, because the whole block is held until the watermark passes
  it — but a feed that emits slot 200 and then slot 100 is refused rather than quietly
  misordered.
- **Malformed lines are counted, not fatal** — one bad line in a long concatenation should
  not cost the rest. The first error is quoted in the report.
