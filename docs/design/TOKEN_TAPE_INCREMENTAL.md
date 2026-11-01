# The token tape, kept current — sealed days and one live tail

Status: **design, nothing built** (2026-10-27). How `token-ledger pack`'s files
(`crates/token-tape`) stay current without rebuilding a token's history every
few minutes. Figures marked *measured* are from `pack` on $NikePig
(`c881c20e…`) on a development Mac, not the ARM box; the box is slower and
shares its CPU with the indexers.

## 1. The question

`pack` writes a token's history as binary files the flow explorer's arena
reads: day chunks of market, transfer and order events, the party table with
handles (`holders.bin`), the active and opening identity subsets, and current
balances (`holding.bin`). How expensive is it to keep them current, and how
current do they need to be for the arena to feel live?

## 2. What a rebuild costs

*Measured*, $NikePig, 722,667 movements, 98,468 parties:

| Step | Time |
|---|---|
| Read the archive and fold the story | 3.0 s |
| Fold the ADA Handle archive (5.75 M movements) for names | ~4 s |
| Build and write ~2,400 chunk files and the identity files | ~2 s |
| **Total** | **8.7 s** |

Linear in movements: a token ten times busier is minutes. All 33 archives on
the box every five minutes would occupy a core for much of each cycle,
re-deriving history that did not change. A full rebuild is a maintenance
operation, not a schedule.

## 3. What actually changes

- **A finished day never changes.** Its chunks are fixed once the rows under
  it are immutable.
- **Party numbers only grow** (the chain's order; see `pack`'s module docs):
  a new party appends; no record moves.
- **Today changes, and balances do.** `holding.bin` moves with every trade;
  the active set grows when a wallet trades for the first time.
- **Handles change hands slowly.** A party's name moves only when its wallet
  gains or loses a handle.

## 4. The rule

`ARTIFACT_SHARDING.md`'s, applied here:

> **A sealed file is named by what it was derived from. Re-deriving it yields
> the same bytes. Growth adds files; it never edits one.**

And the one exception it allows: **everything not yet sealed is ONE file,
replaced whole** on every refresh. A reader never has to merge two versions of
the same thing.

## 5. Two tiers, on the box's existing two cadences

The box already works this way for the series and balances
(`archive::write_derived`, `tail_series`): a settled carry written when a walk
completes, including the daily top-up, keyed by the archive's immutable
boundary; a volatile tail refreshed every 30 s (*the tip thread's log*), folded
onto the carry when read. The tape follows the same split.

### 5.1 Sealed — when a walk completes (about daily)

The boundary is `settled_to`, the highest immutable slot. **A day is sealed
when it ends at or below `settled_to`**; the day holding the boundary is not.

| File | Sealed when | Changes after |
|---|---|---|
| `market/<day>.bin`, `transfers/<day>.bin`, `orders/<day>.bin` | the day is sealed | never |
| `holders/<n>.bin`: parties `n × 65,536 …`, 57 bytes each | the segment is full | never |
| `holders/<last>.bin`: the open segment | each seal | rewritten until full |
| `holders-overflow/<n>.bin` | with its segment | as its segment |
| `active.bin`, `opening.bin`, `holding.bin` | each seal | rewritten each seal |
| `meta.bin`, `index.bin` | each seal | rewritten each seal |

`holders.bin` becomes segments so that a seal rewrites only the open one
(*measured*: the whole table is 5.6 MB for $NikePig; a full segment is 3.7 MB
raw). Overflow is segmented with its records, so its offsets stay local.

### 5.2 Tail — each tip refresh (30 s)

One file, `tail.bin`, holds everything above `settled_to`:

- **events**, all three streams, chain order: the open day and whatever the
  volatile tail has since;
- **parties numbered in the tail**: identities for numbers `≥` the sealed
  count, in the same chain order;
- **new traders**: parties first seen in the market or order streams since
  the seal, so the active set is `active.bin ∪` these;
- **balance changes** since the seal, for the parties the tail touched, so
  current balances are `holding.bin` plus these.

A tail number is **provisional**: the volatile tail can roll back, and a
rollback that drops a party's first appearance moves every number after it.
Normally the seal assigns the same numbers (same chain, same order). A sealed
number never changes. A reader treats `tail.bin` as a unit and replaces
everything it learned from the previous one.

## 6. The carry

`tape-carry.bin` in the archive directory: what a seal continues from. Not
published.

- `settled_to`, and the archive's immutable pass list it was built over;
- the party count, and every party's balance at `settled_to`;
- the active set;
- the venue and pool tables (their numbers are append-only too);
- the unit;
- the builder's version.

The key-to-number map is rebuilt from the sealed segments when the carry is
loaded (98,468 entries: milliseconds).

## 7. The three operations

| Operation | When | Reads | Cost |
|---|---|---|---|
| **Full build** | no carry; builder version changed; the immutable passes below `settled_to` changed (a backfill, an observation repair) | the whole archive | *measured* 8.7 s on $NikePig |
| **Seal** | a walk completes | movements and observations in the newly sealed days | proportional to a day's rows |
| **Tail** | each tip refresh | everything above `settled_to` (about a day) | proportional to a day's rows, every 30 s |

The tail is the only thing on a fast cadence, and it is bounded by about a
day's activity. If that proves too much for the busiest tokens, the tail can
also carry a small sub-carry of its own; it is not needed for $NikePig (its
busiest day is 6,220 market events).

## 8. Names

Handles come from the ADA Handle policy's archive (`names::fold_handles`),
which is itself a token archive on the box. Folding 5.75 M movements for every
token's seal is the wrong place for that work. Instead:

- **the Handle archive's own seal** writes a derived artifact beside it: every
  stake credential holding a handle, and the name it shows as (66,746 wallets
  *measured*, 56 bytes each, ~3.7 MB);
- **every token's seal reads that** to name its parties: a lookup, not a fold;
- **a token's tail** names its new parties from the same artifact.

Names are therefore up to a day stale, which is fine for something that
changes by hand. A wallet newer than the last Handle seal is unnamed in the
files; the arena's live edge already asks the handle service for those.

Changing the choice rule (OG first, sub-handles) rewrites every token's
segments and subsets, which is a full build of the identity files only, not
of the chunks.

## 9. Publishing and caching

`sync` publishes an archive whose manifest has moved since its last upload; it
would publish the tape directory the same way.

- **Sealed files**: immutable, `Cache-Control: immutable`, cached at the edge
  indefinitely.
- **`tail.bin`**: `max-age=30`. The arena polls it no faster than it changes.
- **Cost**: about six object writes per token per seal and one per tail
  refresh. At 30 s for 33 tokens that is ~95,000 writes a day, about $0.40 a
  day on R2; throttling the tail's publish to 60 s halves it.

## 10. What the arena reads, in order

1. `index.bin`, `meta.bin`, `opening.bin`: the first screen, named.
2. The chunks for the window, newest first, and `tail.bin` for today.
3. `active.bin` in the background: every trade of the history labelled.
4. On request, `holding.bin` and then parties in batches through the
   resolution endpoint, which range-reads `holders/<n>.bin`.

## 11. Open questions

- **Rollback depth.** How far back the tip thread's tail can roll, which bounds
  how often a provisional number is withdrawn. Measure before relying on tail
  numbers outliving one refresh.
- **The open segment's size.** 65,536 parties keeps a segment at 3.7 MB raw;
  a busier token fills one in weeks. Smaller segments mean more files and
  fewer bytes rewritten per seal.
- **Multi-unit policies.** `pack` takes the unit most events are about. A
  collection needs per-unit chunks, which is a separate design.
- **The 158,309 ambiguous events** on $NikePig (several parties on one side,
  likely airdrops) are in no stream. Neither the old tape nor this one shows
  them.
