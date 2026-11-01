# Capturing the datum set and pushing it to R2

How to produce the whole-store datum capture — `hash → preimage` for hash-attached
outputs — and get it into the bucket. Design: `docs/design/DATUM_RESOLVER_FORMAT.md`;
the reader that consumes it is not built yet, and this is deliberately ahead of it.

Run all of it **on the box that holds the chunks** (the arm box): the capture is a
sink on the fold's deep walk, not a second pass over anything, and the push needs the
bucket's credentials.

## 1. What one window is

The capture is published in **windows**: one `datum.tbl` + `datum.bin` pair per window
(every `--datum-chunks` chunks — 100 by default), named by the slot range the walk
covered and each file's own blake2b-256, plus one catalogue (`datum/v1/current`) that
grows to name every window.

⚠️ **A window is also the memory unit.** `DatumCapture` holds the window's distinct
hashes plus their bytes in memory until it writes, so the window decides the peak.
Measured payloads per 100 chunks (`DECODE_SHAPE.md` §2.10):

| window | distinct hashes | payload | entry array |
|---|---|---|---|
| Conway tip | 18,096 | 2.7 MB | 0.7 MB |
| Babbage | 373,559 | 80 MB | 14 MB |
| Alonzo | 625,471 | 69 MB | 24 MB |

100 chunks is a good unit: ~100 MB of pair at worst, a couple of minutes of decode,
and a push that finishes while the next window is being walked. A bigger window costs
memory linearly and delays the first object landing; a smaller one multiplies runs.

⚠️ **A single table cannot hold the whole chain.** `write_datum_table` refuses a
payload over `u32::MAX` (the offsets are 32-bit), and the store-wide payload is
single-digit GB. The windows are what keeps each table addressable, and they merge
because the key is the datum's own hash.

## 2. Capture — one command for the whole store

```sh
fold-probe fold \
  --immutable /path/to/mithril/db/immutable \
  --chunks 0 --datum-chunks 100 \
  --datum-tbl /var/lib/mitos/datum
```

- `--chunks 0` walks every complete chunk.
- `--datum-chunks 100` (the default) closes, writes and lays out a **window** every
  100 chunks, then starts a fresh capture — so the run holds one window in memory,
  not the chain, and the first window is on disk and uploadable within minutes.
- `--datum-tbl` is the capture dir: it holds `datum.tbl` + `datum.bin` (the last
  window, re-readable by `DatumTable`) and `publish/` — the uploadable set, whose
  paths are the bucket keys.
- The flag implies `--chain-decode`; the witness bytes exist only in that projection.

The run prints one block per window as it closes, and a closing line with what the
*set* now holds:

```
datum window 3 — chunks 9200..=9201 (the witness datums this walk revealed)
  coverage   slots 198720000..=198763160 · a miss means "not revealed in this range"
  entries    515 distinct hashes · 78 KiB payload · offered 1042 times in 148 KiB → 47% dedup
  attached   611 hash-attached outputs created · 603 answerable at creation (98.69%) —
  publish    /var/lib/mitos/datum/publish  (paths under it are the object keys)
    table      datum/v1/w-198720000-198763160-967ffaad….tbl  20 KiB  blake2b-256 967ffaad…
    blob       datum/v1/w-198720000-198763160-b983ee68….bin  78 KiB  blake2b-256 b983ee68…
    catalogue  datum/v1/current  · 3 window(s) in the set
  run        3 window(s) written this run · the set holds 3 window(s) · 689 KiB · /var/lib/mitos/datum/publish
```

The **attached** figure is the one to watch: the share of hash-attached outputs
whose preimage is already on chain. At the tip it should be **~99%**, and a window
far below that means the capture is missing something, not that the chain was quiet.

The sizing — why 100 chunks and why not one window for the chain — is §1.

## 3. Push it

```sh
fold-probe datum-push \
  --layout /var/lib/mitos/datum/publish \
  --dest r2:<bucket> \
  --run
```

Run it whenever you like — after the capture, or partway through a long one (the
layout holds every window that has closed, so a push mid-capture lands what exists).
It is idempotent, and it checks itself before it starts: every object the catalogue
names must be on disk at the length the catalogue claims, or it refuses rather than
pushing a set it cannot vouch for.

Points to keep:

- **Windows are pushed before the catalogue**, always. A reader starts at
  `datum/v1/current`; a catalogue naming objects that are not there yet is a window of
  broken reads.
- The window objects are content-addressed, so they go up with
  `--ignore-existing` and a year-long immutable cache posture. A re-capture that finds
  the same datums produces byte-identical files, so a re-run or a re-push of a covered
  window **uploads nothing**.
- The catalogue is the one mutable object: no `--ignore-existing`, and
  `Cache-Control: public, max-age=60`.
- `--dest` is `remote:bucket` (or `remote:bucket/prefix`). No prefix is needed: the
  keys already carry their namespace, so the set lands at the bucket's `datum/v1/`.
- The whole set is meant for the **same bucket the book is published to**; nothing in
  the layout collides with the book's keys.

## 4. Resume, and what a re-run costs

The catalogue is the state. `datum/v1/current` lists every window captured, one line
each, sorted by chunk — so the first chunk no window covers is where to restart:

```sh
tail -1 /var/lib/mitos/datum/publish/datum/v1/current   # window <fc> <lc> …
```

```sh
fold-probe fold --immutable /path/to/immutable --start <lc + 1> --chunks 0 \
  --datum-chunks 100 --datum-tbl /var/lib/mitos/datum
```

Nothing is lost by re-running a covered window: the capture rewrites its own
catalogue line and its own objects, and because the objects are content-addressed a
re-capture that finds the same datums produces byte-identical files — so the push
skips them.

- Expect hours, not days: `DECODE_SHAPE.md` §2.9 puts the whole-store deep walk at
  **90.6 minutes** of wall on the arm box, and the capture rides that walk (+60–81% of
  the decode). Measured for calibration on the dev box, 2026-09-28: **1,000 chunks at
  the tip side (chunks 8202–9201) in 58 s → 10 windows, 77 MiB of pair**, with 19k–62k
  distinct hashes per window. The 2021–2023 windows are heavier (see §1's table), so do
  not extrapolate the tip's density to the whole store.
- Disk: the layout holds every window's pair (~2× the store-wide payload, single-digit
  GB) plus the capture dir's own `datum.tbl`/`datum.bin` for the last window. Nothing
  is deleted by a push; the layout is also the local mirror of the set.

## 5. What good looks like when it lands

```sh
rclone cat r2:<bucket>/datum/v1/current
```

- Every line is a window: chunks, slots, entry count, layout version, both lengths and
  both digests.
- The set should cover the store contiguously, from the first chunk to the last
  complete one. A gap is a window that has not been walked yet — the coverage claim is
  per window, so a gap is visible rather than silent.
- Spot-check one object against its key on the box:

```sh
rclone cat r2:<bucket>/datum/v1/w-<first>-<last>-<digest>.tbl | b2sum -l 256
```

  a reader does this check on every lookup (`DATUM_RESOLVER_FORMAT.md` §8), so a
  mismatch here is the same failure it would refuse.

## 6. What is *not* done yet

The reader. `locate_datums` still knows only the body and the metadata, and
`direct.rs` has no `datum_by_hash`. The data can land on R2 before either exists —
that is the point of this runbook — but nothing will read the set until the frontend
steps in `DATUM_RESOLVER_FORMAT.md` §10 are built.

## 7. The first full set (2026-09-28)

Captured on the ARM box against its own store (9,206 chunks), pushed to the Mithril
bucket, and verified by fetching objects back:

| | |
|---|---|
| wall | **2.02 h** for 9,206 chunks (the same store's plain fold is 1.26 h — the capture's share is the rest) |
| windows | **93**, chunks `0..=9205`, contiguous |
| distinct datums | **20,370,399** — from 44,490,815 witness instances, 54% dedup |
| pair bytes | **4.55 GiB** across 186 objects, plus the catalogue |
| hash-attached outputs created | 31,035,023 |
| …answerable at creation | 24,491,749 = **78.92%** |

Landed at `r2:mithril-snapshots/datum/v1/` via
`mithril-publish.sh --exec … datum-push … --dest r2:mithril-snapshots --run` — the
same bucket and credential wrapper the snapshot publish uses, so a tool that drives
rclone needs no config of its own. Verified by fetching the **largest** pair back
(a 29.8 MB table and its 157 MB blob) and re-hashing both to the digests in their
own keys, plus one smaller window earlier in the same run.

Run log: `/opt/scratchpad/datum-full.log`; the layout is `/srv/data/datum-set/`.
The build there used `CARGO_TARGET_DIR=/srv/data/cargo-target`, because the box's
root volume has under 3 GB free and the store's volume has 408 GB.
