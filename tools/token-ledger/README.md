# token-ledger

One policy's complete movement history, walked from a certified Mithril
snapshot into a **sealed Parquet archive** — and served as a live, correcting
feed while the walk is still happening.

Family B (see the root README): this reads immutable chunk files. It does not
load the wasm runtime, subscribe to anything, or talk to Dolos.

Designs live in the sibling repo `cnft.dev-workers/docs/design/`:
`POLICY_ARCHIVE_AND_SCALE.md` (the artifact), `POLICY_WALK_SCHEDULER.md` (the
job model — **read its "Order of work" first**), `POLICY_ARCHIVE_OBSERVATIONS.md`
(the decoded tier), `POLICY_PROFILE.md` (what a policy IS, and the supply
invariant).

## The one thing to understand first

There are **two walks**, and they produce the same rows from the same chunks:

- **`walk`** — forward, mint → tip, into sqlite. Complete-or-nothing: it starts
  at the policy's first mint so its buffer is exact, and it produces the newest
  row **last**.
- **`reverse`** — backward, tip → mint, into Parquet. Progressive: it produces
  the newest row **first** and deepens on demand, which is what a feed needs
  and what makes a policy nobody has indexed viewable in seconds.

`reverse` is the one in production. `walk` remains as the independent
derivation the archive is reconciled against — 14,971 transactions and 30,803
`(tx, party)` deltas, zero differences. That agreement is why a guard added to
one walk must be added to the other.

## Commands

```bash
# THE ONE IN PRODUCTION. Walk backward from the archive's floor into Parquet.
token-ledger reverse --archive-dir <dir> --token <policy> --data-dir <db> \
    [--days N | --to-slot S] [--first-mint S] [--probe-first-mint] \
    [--policy-index-dir <dir>] [--tx-index-dir <dir>] [--no-observe]

# Read an archive back the way a Worker would — footers first, then only the
# row groups a page needs — and report what it cost in requests and bytes.
# Also prints the supply reconciliation, observations, trades and density.
token-ledger archive --archive-dir <dir> --policy <policy> [--density]

# Party-to-party movement graph. --by-stake is usually what you want.
token-ledger graph --archive-dir <dir> --policy <policy> --by-stake --write

token-ledger rollup    # fold a policy's passes into one file
token-ledger bundle    # manifest + every footer, one blob for KV
token-ledger publish   # R2 + KV, by hand, with a per-step record
token-ledger export    # the artifacts a frontend loads
token-ledger stats     # derived balances at tip — the reconciliation surface
token-ledger classify  # cohort a set of addresses
token-ledger probe     # what a policy looks like before committing to a walk

# The hosted surface: any token on demand, behind a poll endpoint.
token-ledger serve --data-dir <db> --archive-dir <dir> [--publish-archive] \
    [--tx-index-dir <dir>] [--policy-index-dir <dir>] [--tail-db <spool>] \
    [--walk-workers 4] [--seek-workers 20]
```

## Serve surface

| route | what |
|---|---|
| `GET /policy/{p}` | job state + coverage |
| `POST /policy/{p}/refresh?to_slot=S` | admit a policy; walk it to its mint |
| `POST /policy/{p}/seek` | read one window a reader asked for, ahead of the descent |
| `GET /policy/{p}/events` | the correcting feed |
| `GET /policy/{p}/density` | daily histogram, from footers alone |
| `GET /policy/{p}/tx/{hash}` | one transaction's rows |

`?to_slot=` on refresh is the caller asserting the policy's first mint. Without
it the daemon asks the **policy-index** (microseconds, local) and falls back to
Koios — see `--policy-index-dir`.

## On disk

```
<archive-dir>/<policy_hex>/
  manifest.json               passes, coverage, completeness, profile — written LAST, read FIRST
  archive-NNNN.parquet        a ROLLUP: every pass so far, folded
  pass-NNNN/movements.parquet one pass not yet rolled up
  pass-NNNN/corrections.parquet   later resolutions of earlier passes' sources
  pass-NNNN/observations.parquet  what script outputs HELD, decoded or not
  pass-NNNN/pending.bin       inputs this pass is still waiting on
  bundle.bin  graph.bin  published.json
```

A reader merges **every** file the manifest names by summing rows per
`(transaction, unit, party)`. Rollup and compaction are that same sum applied
ahead of time so readers open fewer files.

⚠️ `graph.bin` and `published.json` are **not** produced by a walk. A re-walk
into a fresh tree will not have them, and swapping that tree in without
carrying them forward silently drops every movement graph.

## Traps worth knowing before you touch it

- **A floor that is too HIGH does not fail.** It produces a short archive that
  calls itself COMPLETE, because completeness is measured against that number.
  Every first-mint source deliberately under-shoots.
- **The supply invariant is free and exact**: `Σ party amounts == Σ per-tx
  net_mint`, per unit, both sides out of the same file. It runs on every
  landing and on `archive`. The *sign* of the gap is the discriminator — a
  positive gap is normal on a partial archive and damning on a complete one.
- **Phase-2 failed transactions decode perfectly and their outputs never
  existed.** Both walks skip them and count what they skipped.
- **`Manifest` deserialises with serde ignoring unknown fields**, so an old
  binary reads a new manifest happily and then silently drops the new field on
  its next write. Stop the daemon, swap, start it — in that order.
