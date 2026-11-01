# mithril-fetch

A policy id in, the Mithril immutable chunks its history needs out. Family B
(see the root README): this reads and writes chunk files, and never loads the
wasm runtime or talks to Dolos.

## The one thing to understand first

**A Mithril chunk is not a token's slice of the chain.** An immutable file is
21,600 slots (six hours, four a day) holding every transaction in that window,
and the aggregator only serves a contiguous `ImmutableFileRange`. A policy
therefore resolves to a **range**: the chunk before its first mint, through the
tip or a chosen end.

At the measured ~25 MB per immutable file:

| window | chunks | roughly |
|---|---|---|
| 1 day | 4 | 100 MB |
| 1 month | 122 | 3 GB |
| 6 months | 730 | 18 GB |
| 1 year | 1,460 | 37 GB |

That is why the plan screen exists, and why it shows the estimate before
anything is fetched.

The first mint comes from Koios: `policy_asset_info` returns every asset the
policy ever minted, and the floor is the **earliest `creation_time`** across
them — never `rows[0]`. The rule lives in `crates/mitos-koios` with the incident
that produced it. A floor that is too *high* truncates the walk and lets it
report itself complete, so this tool starts one chunk earlier than the slot's
own chunk.

## Commands

```bash
# The TUI.
mithril-fetch

# Pre-fill the policy and resolve immediately.
mithril-fetch --policy <56 hex chars>

# No TUI: resolve, print the plan, fetch, print the next command.
mithril-fetch --run --policy <policy> --dest /path/to/chunk-store

# A probe: only the first N immutable files from the floor.
mithril-fetch --run --policy <policy> --dest <store> --chunks 2

# Stop at a slot instead of the tip (its immutable file is included).
mithril-fetch --run --policy <policy> --dest <store> --to-slot 134006400

# The whole published store, gently, with no policy in mind.
mithril-fetch --backfill --dest <store>

# A bounded or more/less gentle backfill.
mithril-fetch --backfill --dest <store> --backfill-from 6201 --backfill-to 7000 \
    --backfill-parallel 4 --backfill-batch 20 --backfill-pause 10
```

The TUI:

| screen | keys |
|---|---|
| policy | type hex · `Enter` resolve · `Esc` clear · `v` coverage · `Ctrl-C` quit |
| resolving | `Esc` back · `q` quit |
| plan | `Enter` start · digits edit the end chunk · `t` tip · `v` coverage · `Esc` back |
| fetching | `Esc` cancel · `q` quit |
| done | `Esc` new policy · `v` coverage · `q` quit |
| failed | `r` retry · `Esc` back · `v` coverage |
| coverage | `b` backfill · `r` rescan · `Esc` stop a running backfill (then back) · `q` quit |

## Store layout

The store is **shared**: sealed immutable files are identical for every policy,
so a per-policy copy would duplicate tens of gigabytes.

```
<MITHRIL_DATA_DIR>/
  immutable/06201.chunk 06201.primary 06201.secondary  ...
  policies/<policy>.json      # floor, ranges, certificate and snapshot hashes
  .mithril-fetch-staging/     # only while a batch is in flight
  .mithril-certificate-chain  # validated chain links, so a rerun does not re-walk it
  archives/                   # `token-ledger reverse --archive-dir` writes here
```

`MITHRIL_DATA_DIR` is read from the environment or `mitos/.env` (which is
gitignored; `.env.example` is the committed template). `--dest` wins over both.
There is no default, deliberately: a silent multi-gigabyte download into `./data`
is not a useful surprise.

## Coverage and backfill

`v` opens the coverage screen. It scans the store locally first, so the local
numbers are there even when the aggregator is not, and then joins them to
Mithril's published digest map:

```
Local       4 files · 148.8 MiB · 06201–06204
Published   00001–09338 · 9,338 files · beacon 09338 (epoch 664)
Covered     4 of 9,338 · <1%
Missing     9,334 files · ≈ 220.7 GiB
Gaps        2 run(s) · 00001–06200, 06205–09338
```

`00001` is the first number, not `00000`: Mithril's `ImmutableFileRange` starts
at 1 and a fetch of 0 is refused. Coverage is therefore counted against the
published set from 1 to the newest signed beacon.

`b` starts a backfill of the whole published window (`--backfill-from` and
`--backfill-to` narrow it). The defaults are the slow profile — 4 concurrent
downloads, 20 files per verified batch, 10 seconds idle between batches — so the
transfer leaves the link and the disk alone between bursts rather than saturating
them. `Esc` stops after the batch in flight; `b` resumes from the first file that
is still missing.

A full mainnet backfill is ~9,300 files and ~220 GiB. At the defaults it is a
background job measured in days, which is the point.

## Verification and resume

Every batch goes through the same two checks `mithril-snapshot sync` makes:

1. `mithril-client` proves the range against the snapshot's certificate chain
   and computes a Merkle proof over exactly those files.
2. The staged parts are hashed and compared with the aggregator's published
   digest map — a different claim, about the bytes on **this disk**.

Only then is a part renamed into `immutable/`. A cancelled or failed run costs
the staging directory; the store keeps everything that reached it. Re-running
scans the store and fetches only the missing runs, so resume is free. A batch is
at most 200 files (~5 GB), which is the most a cancel can cost.

The certificate chain is the slow part of a cold start: verifying it walks back
to the genesis certificate, one HTTPS request and one multi-signature check per
Mithril epoch. Measured 2026-10-30 on a fresh store in a **debug** build: the
first one-file batch took 3m56s end to end, nearly all of it the 127-certificate
chain walk, while a second batch against the same store verified 3 certificates
and finished in under 20 seconds. Every verified link is therefore written to
`.mithril-certificate-chain`, and the client's own verifier cache reads it: the
first batch of a store pays the walk, later batches (and later runs against the
same store) verify only the newest epoch and follow cached links for the rest.
The screen shows how many certificates have been checked, so a long walk is
visibly working rather than hung.

The store is refused, not repaired, if it holds an incomplete triple inside the
requested range: `lift` never overwrites, and guessing which half is canonical
is not something this tool will do.

## Testing

```bash
nix develop -c cargo test -p mithril-fetch -p mithril-snapshot
nix develop -c cargo clippy -p mithril-fetch -p mithril-snapshot --all-targets -- -D warnings
```

The pure half (`plan.rs`, `coverage.rs`) is tested without a network: the
earliest-asset rule, the one-chunk margin, chunk arithmetic, end-chunk bounds,
the estimate, and coverage runs. `app.rs` tests key handling, the coverage and
backfill state, and that a worker event from an older generation cannot repaint
a new screen. `crates/mithril-snapshot` tests the chain cache's round trip and
its refusal to accept a malformed line.

`--fixture` draws a captured screen through a `TestBackend`; `--fixture …
--capture <stem>` writes `<stem>.txt` and `<stem>.json`. `tui-shot` can drive
the real binary with `--fixture` and a PTY scenario:

```text
run artifacts=.tmp/tui settle=60ms timeout=10s
tui app bin=mithril-fetch size=120x40 args="--fixture .tmp/frames/plan.json"
wait app contains="First mint"
capture app out=01-plan
resize app 80x24
assert app contains="00184" message="the floor chunk survives 80 columns"
key app q
wait app exit=true
```

A real run on 2026-10-29, `--chunks 2`, policy
`141eea77…4f64` ($DONG): Koios found one asset, the mint at slot 133,983,726
put the floor at immutable file 06201, and two files (57.8 MiB) were downloaded,
certificate-proved and digest-checked against beacon 09334. Re-running the same
command fetched nothing and reported the two files already present.

## Traps

- **The aggregator's newest snapshot moves while a run is in progress.** Sealed
  chunks never change, and each batch is proved against its own snapshot, so a
  store filled across two runs is still consistent. The manifest records which
  snapshot each range came from.
- **`chain-sieve` and `pallas-hardano` drop the highest local immutable file**
  because a running node may still be appending to it. A restored snapshot is
  `0..=beacon` and the beacon is signed, so this tool asks for the beacon and
  does not apply that rule. Readers that scan the store locally do apply it; see
  `crates/mithril-snapshot`'s header for why both rules exist.
- **A policy with a very large asset set makes `policy_asset_info` paginate**
  (1,000 rows a page). The resolving screen shows the count Koios returned, so a
  slow resolve is visible rather than indistinguishable from a hang.
- **The `token-ledger` command on the done screen is printed, not run.** The
  chunks are the expensive part; the walk is a separate decision.
- **One writer per store.** Two runs share `.mithril-fetch-staging`, and the
  second would clear the first's transfer. The store is a cache of sealed files,
  not a queue; run one fetch against it at a time. The chain cache is
  append-only and safe for readers, but a backfill and a policy fetch are still
  two writers to the staging directory.
- **The first chain walk of a store is minutes, not seconds.** It is one
  request per Mithril epoch since genesis, and the screen says how many
  certificates it has checked. Later batches use the cache and are quick. A
  fresh *store* pays it once; deleting `.mithril-certificate-chain` pays it
  again.
