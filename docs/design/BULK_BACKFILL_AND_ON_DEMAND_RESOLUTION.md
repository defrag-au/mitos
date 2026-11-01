# Bulk backfill is the wrong shape — notes for the mitos refactor

Written 2026-09-19, from measurements taken while backfilling the jpg.store ask
book (~170k unspent listing outputs) into the cnft.dev-workers mirror.

The exercise was a consumer-side one, but three of its findings are mitos's, and
one of them is a correctness bug rather than a performance one.

---

## 1. `GET /_admin/utxos` caps silently, and that blocks reconciliation

Measured: the jpg V1 sale address returned **exactly 100,000** refs. V2/V3
returned 70,059 with `truncated: false`.

The cap is reported (`truncated`), and the consumer does the right thing with
it — it refuses to prune:

```
NOT pruned: mitos CAPPED at least one address, so this walk saw a prefix
of the unspent set, not the set. Pruning against it would delete every
live listing past the cap.
```

But the consequence is that **any address with >100k unspent outputs can never
be reconciled**. Not "is slow to reconcile" — *cannot*, ever, by any consumer,
because no consumer can see the set. A book fed this way accumulates rows for
listings that were spent long ago and has no safe way to remove them.

**Ask: cursor pagination on `/_admin/utxos`.** Something like
`?address=…&after=<tx_hash#index>&limit=N`, with a `next` in the response.
That:

- removes the ceiling, so reconciliation becomes possible at all
- bounds the response — it currently serialises 70k refs into one JSON body
- lets a consumer checkpoint, which matters for the reason in §2

This is the one item here I would call a bug. The other two are shape.

---

## 2. The cost is transport, not mitos

Same `POST /resolve` request, one item:

| | latency |
|---|---|
| on the box | **0.8 – 5.5 ms** |
| from a laptop over an SSH tunnel | **1.6 – 6.6 s** |

Roughly **1000×**. (The tunnel figure includes contention with the backfill
that was running at the time, which is itself part of the point — ~350
sequential requests through one forward self-congest.)

So: the services are not slow. A full walk is ~350 requests, which is under two
seconds of actual work and twelve-plus minutes over a tunnel. Every failure
observed during the exercise was the tunnel resetting, never a service:

```
tx-index /resolve failed after 4 attempts: io: Connection refused
mitos /_admin/status failed (attempt 1/4): io: Connection reset by peer
```

**Ask: nothing, for speed.** Optimising sub-millisecond handlers would be
optimising the wrong thing. The lesson is operational — **bulk walks belong on
the box**, as a scheduled job next to the data, not in a tool run from a
laptop. Worth stating in the ops doc rather than solving in code.

It does argue for §1's checkpointing, though: a job that cannot resume must
redo everything after any interruption. Ours died at ~61,500 of ~174k and
restarted from zero.

---

## 3. The real fix: make it on-demand, not bulk

The backfill exists because the ask book is fed by a companion subscription
that registers at the chain tip and therefore never sees listings already
sitting unspent. That framing led to a batch walk. The batch is the wrong
answer to a right observation.

**A listing's datum is only needed when somebody tries to buy it.** That is a
perfect trigger, and it is already on the hot path — the mirror's
`resolve_listings` knows precisely when a row's `datum_cbor` is `NULL`.

So the shape should be: **treat the stored preimage as a CACHE, and make the
existing recovery path its filler.** On a miss, recover once and write it back.
Then:

- cost follows usage — hot listings populate first, the long tail never costs
  anything
- the ~13% whose preimages are unrecoverable (22,796 of 170,061 here) are
  attempted once, recorded, and never retried
- there is no 174k-write batch, ever
- the second attempt at the same listing is free

Today the mirror's buy path already recovers the preimage from the creating
transaction's metadata — and then **throws the result away**. Persisting it is
most of the work.

The batch then becomes an optional warm-up rather than a prerequisite, which is
a much better place for it to sit.

### What this needs from mitos

Nothing, strictly — the recovery already works. But two things would help:

- **A public-ish path to auxiliary data.** `tx-index` is loopback on the box, so
  a Cloudflare DO cannot reach it; recovery has to happen in the worker, which
  reaches Koios instead. If mitos exposed aux data on its authenticated admin
  surface the consumer could use one source for both legs.
- **Per-listing resolution as a first-class call.** Both legs of the current
  recovery are keyed differently — the output by UTxO ref (`/resolve`), the
  preimage by *creating-transaction* hash (`/aux`) — because jpg publishes
  listing datums in metadata and never witnesses them. A consumer resolving one
  listing has to make two calls and join them. One call taking a UTxO ref and
  answering "output + preimage" would remove the join from every consumer that
  needs it.

---

## Summary for the refactor

1. **Paginate `/_admin/utxos`** — correctness, not speed. Without it, large
   addresses can never be reconciled by anyone.
2. **Bulk walks run on the box.** Ops, not code. ~1000× difference.
3. **Prefer on-demand resolution with write-back** over batch seeding. Consider
   a combined per-UTxO "output + datum preimage" call to support it.

Related: `docs/design/FALLBACK_PROVIDER_AND_BATCH_PREFETCH.md` (the same
tension between bulk prefetch and on-demand resolution, from the host side),
`docs/design/EVENT_DELIVERY_RESILIENCE.md`.
