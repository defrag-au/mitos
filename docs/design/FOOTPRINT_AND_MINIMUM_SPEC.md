# Footprint and minimum spec — profiling mitos down to a cheap VPS

**Status: PROPOSED** (2026-09-21). Nothing here is built. This doc sets the
measurement plan, the pass criteria and the order of work; it deliberately
does NOT design the unified Mithril walker (§8 only states the constraints
that work must honour).

Cross-references:
- `docs/strategy/MITOS_PLATFORM_V3.md` — §10 "Box hardening", W1e (the
  restart-peak investigation), Appendix A (capacity), Appendix B (ARM
  benchmark + filesystem). This doc **absorbs W1e** and gives it pass criteria.
- `docs/design/TX_INDEX_DATASOURCE.md`, `crates/tx-index/src/lib.rs` — the
  on-disk index format referred to in §3.
- `docs/design/MITHRIL_PHONEBOOK.md` — why most of the dolos data dir is
  not the phonebook we serve (sketch; not this phase).
- `cnft.dev-workers/docs/operations/mitos-operations.md` — how the box is
  operated (no `nix develop` on the box; rsync, not push/pull).

## 1. Goal

mitos should be something a third party can run on a cheap VPS. The
deliverable is a **published minimum spec per install profile** (§7) that we
have actually run at, plus the defaults and code changes needed to get there.
"Lower is better" is the whole brief: every GB of RAM or 100 GB of disk we
remove widens who can run it.

The test bed is the netcup **VPS 3000 ARM G11** (12 vCore aarch64 / 24 GB /
768 GB NVMe) — already benchmarked in v3 Appendix B: 69/69 goldens pass
unmodified, ~1.6–1.8x x86 wall time, 0.07% steal. It runs mainnet only, in
isolation from the production box. 24 GB is generous on purpose: we constrain
it artificially with cgroups (§5) rather than by buying smaller boxes.

## 2. What is measured today

From v3 Appendix A (x86 box, 2026-09-19) and the operations doc:

| | value |
|---|---|
| `mitos-mainnet` RSS, steady | 8.9 G |
| `mitos-mainnet` RSS, restart peak | **17.5 G — unexplained** (`auto_resume` is already sequential) |
| ledger / index services, combined | ~8.5 G |
| mainnet CPU | 3.1 cores average |
| dolos data | 145 G — archive 93 / index 39 / wal 11 / state 4 |
| module state | ~7 G (`collection-metadata` kv 1.3 G + emissions 2.1 G dominate) |
| Mithril immutable chunks | 217 G, ~9,150 chunks, +~35–40 G/yr |
| tx-index | 5.7 G (base.idx 1.95 G + segments), 123.6M entries |
| policy-index | ~0.5 G |

None of these numbers is *attributed*. We know the totals, not what they are
made of, and that is the gap this doc closes first.

## 3. What is already lean — do not spend effort here

`tx-index` and `policy-index` are mmap'd, sorted, prefix-directory formats. A
fully cold tx lookup is one directory page + one entry page + one `pread` of
the body from the chunk file — three random NVMe reads. Tx hashes are uniform,
so the entry array has no locality for page cache to exploit; the only part
worth keeping hot is the 64 MB directory (2^24 × u32). Expected resident need:
on the order of 100 MB, all of it evictable file-backed pages.

**Verify, don't assume:** `fincore` on `base.idx` + segments on the current box,
and cold-vs-warm lookup latency on the ARM box with caches dropped
(`echo 3 > /proc/sys/vm/drop_caches`). If cold p99 stays in low single-digit
ms, the index layer is done and gets a line in §7, nothing more.

## 4. Leads already visible in the code

Hypotheses with a file:line, not findings. Each needs the attribution in §5
before anyone changes it.

1. **Module kv stores open redb with a 10 GB cache cap.**
   `crates/mitos-platform/src/vendored/balius/kv.rs:55` —
   `set_cache_size(1024 * 1024 * cache_size.unwrap_or(10_000))`, and every
   production caller found passes `None` (`lifecycle.rs:2149`, `:2157`, `:3631`,
   `moves.rs:2592`, `:2610`). That is a *cap per module*, inherited from
   upstream balius. redb's cache is user-space: it is **anonymous memory the
   kernel cannot evict**, so under a cgroup limit it OOMs instead of shrinking.
2. **Four more redb open sites take the library default** (believed 1 GiB each
   — confirm against the pinned redb): `emissions.rs:160`,
   `indexer_data_cache.rs:77`, `watched_ref_index.rs:91`, `storage.rs:1929`.
   Emissions and kv are per instance, so the theoretical ceiling scales with
   module count (~19 on mainnet) — and v3 forks add instances.
3. **No global allocator is set** anywhere in the workspace, so the host runs
   on glibc malloc. A many-threaded process that bursts (a restart replaying
   every module) and then idles is the textbook case for arena fragmentation:
   RSS that never comes back. Cheap to test: `MALLOC_ARENA_MAX=2` as an env
   var, no rebuild.
4. **Embedded dolos (v1.2.0)** brings its own redb + fjall caches and a 93 G
   archive. Its cache and history-retention knobs have not been located yet —
   that is a task in §5, not an assumption.
5. **Wasm linear memory.** Only the `dev` profile sets `max_memory_bytes`
   (512 MB, `mitos-lifecycle/src/policy.rs:162`); shared modules are uncapped
   (`:144`). Probably small next to 1–4, but it is per instance and unmeasured.

If (1)+(2) explain the steady state and (3) explains the restart peak, the
floor drops a long way for very little code. If they don't, §5 tells us where
it actually is.

## 5. Phase 1 — attribution

Goal: a table that splits mainnet RSS, steady and at restart peak, into
named owners. No optimisation lands before this table exists.

**Validity first — an isolated box does no work (found 2026-09-21).**
Companions are Cloudflare Durable Objects that subscribe to the PRODUCTION
host. Nothing subscribes to the ARM box, so every dynamic-interest module
there scans for nothing and emits almost nothing: RSS, CPU and emissions
growth would all read low, and the published minimum would be the minimum for
a host that is idle. The box therefore runs with production's persisted
`companions/` registrations (which is where a module's scan-interest is
restored from) and `MITOS_DELIVERY=shadow` — emissions are written, planned,
marked delivered and compacted, and no connection to a companion is ever
opened. That last part is a SAFETY property before it is a profiling one: a
host holding production's registrations has four ways to reach production's
DOs, one of which is a recapture WIPE. See WO-F2, and WO-F1 §4 for the second
lock. `mitos_shadow_suppressed_total` climbing is the proof the modules are
working; if it is flat, stop measuring.

*Decided 2026-09-21: the ARM box is a REAL mainnet server instead.* Workers
register with it and it dials them back, which closes the same gap with real
load rather than replayed registrations — and makes its numbers more honest,
not less, since delivery is part of what a self-hoster's box does. Shadow
delivery stays in WO-F2 as a tool (a second box, a dry run of a new build),
but this box does not use it. The safety rule that remains: it holds its OWN
auth token and NO Cloudflare credentials for the ledger tools. Whatever is
pointed at it is pointed deliberately, by name.

**Measure on the box, per process:**
- `/proc/<pid>/smaps_rollup` sampled every 5 s through a full restart →
  `Rss`, `Anonymous`, `File`, `Swap`. The anon/file split is the first
  discriminator: file-backed is evictable and mostly harmless; anon is the
  real floor.
- cgroup v2 per unit: `memory.current`, `memory.peak`, `memory.stat`
  (`anon`, `file`, `file_mapped`), and `memory.pressure` / `io.pressure` (PSI).
- Per-mapping `smaps` at steady state and at peak, bucketed by path
  (`*.redb`, fjall dirs, chunk files, `[heap]`, anonymous).

**Attribute the anon portion** (one experiment at a time, restart between):
1. `MALLOC_ARENA_MAX=2` — does the restart peak move? Does steady?
2. redb caches clamped via a temporary env override (kv → 64 MB, others →
   32 MB). Record RSS *and* block-processing latency + tip lag, because this
   trades memory for reads.
3. Module count: start with 1 module, then 5, then all. Slope = per-instance
   cost; intercept = host + dolos.
4. dolos alone (host up, zero modules) — the irreducible core.
5. If a large anon remainder is still unexplained: build once with jemalloc +
   heap profiling (`prof:true`) behind a cargo feature and take a dump at peak.
   This is the expensive step; only reach for it if 1–4 leave a gap.

**Output:** the attribution table, committed into this doc as §5a, replacing
the hypotheses in §4 with findings.

## 6. Phase 2 — the squeeze test (ARM box)

Run `mitos-mainnet` and the index services in systemd slices and walk
`MemoryMax` down: **16 → 12 → 8 → 6 → 4 G** (with `MemoryHigh` at ~90% of
each, `MemorySwapMax=0` so swap can't hide the answer). At each rung, run for
≥ 24 h including at least one cold restart and one Mithril snapshot refresh.

A rung **passes** only if all of:
- no OOM kill, and `memory.events` `oom` = 0;
- the follower holds tip: lag p99 ≤ 2 blocks, no sustained growth;
- a cold restart returns to tip in ≤ 2x the unconstrained time;
- PSI `memory` some-avg300 < 5%, `io` some-avg300 < 15%;
- fallback resolution p99 (tx-index lookup + body read) ≤ 10 ms;
- the 69 goldens still pass on the box.

The lowest passing rung, plus headroom, is the published RAM minimum. Repeat
the ladder after each optimisation in §9 lands — the ladder is the regression
test for this whole effort, and should become a script, not a ritual.

CPU gets the same treatment with `CPUQuota` (400% → 200%) once memory is
settled; v3 Appendix A says mainnet averages 3.1 cores on Zen 5, so expect
~5 on Neoverse-N1 and find out whether 4 vCores can hold tip.

## 7. Install profiles — the thing we publish

Disk, not RAM, is what prices most people out: 217 G of chunks + 145 G of
dolos is ~360 G before any module state. So the spec is tiered by what is
held locally. Resolution is already local-first, and `tx-index` already
serves over HTTP, so a remote tier is wiring rather than invention.

| profile | holds locally | resolves via | target |
|---|---|---|---|
| **lean** | dolos (pruned) + module state | remote resolution endpoint | the cheapest VPS that passes §6 |
| **standard** | + recent chunk window + index over it | local for young inputs, remote beyond | mid VPS |
| **full** | + all chunks, tx-index, policy-index, walker outputs | local only; can serve others | today's box / VPS 3000-class |

Targets are filled in from measurement, not guessed here. Open per-profile
questions this phase must answer:
- How far can dolos be pruned? The archive (93 G) and index (39 G) are the
  bulk; which modules actually read history older than the 7-day aux window?
  `docs/design/MITHRIL_PHONEBOOK.md` is the sketch that the live UTxO (~4 G
  state) is the part we keep and the rest is a node-shaped extra.
- What chunk window covers, say, 99% of input resolutions? Inputs skew young —
  measure the age distribution from resolution logs before picking a number.
- What does `lean` trust? A remote resolver returns a body that the reader
  re-hashes against the full requested tx hash (`tx-index` already does this),
  so a lying resolver can withhold but not forge. State that in the docs.

### The stated minimum: VPS 1000 ARM G11 (2026-09-22)

6 vCore aarch64 · **8 GB RAM** · 256 GB NVMe. This is the box the architecture
has to fit, and it is not the box anything has been measured on (12 cores/24 G /
768 GB). What it costs, from the measured figures:

| | budget | note |
|---|---|---|
| **Disk** | 230.7 GB of chunks against ~238 GiB usable | the store alone leaves ~23 GiB for the OS, the artifacts and the next chunk. It fits only if nothing else lives there — and the other thing that would (dolos's 147 G data dir) does not |
| **RAM** | 8 GB total, shared with the serving side | today's fold peaks at **3.10 GiB RSS / 2.17 GiB anon** for the live set alone (`fold-full.log`) |
| **CPU** | 6 vCore, shared with the serving side | see below |

**CPU is not the binding constraint, which reorders the work.** Consuming the
store is 230.7 GB: at 100–125 MB/s (a 1 Gbit/s link) that is 31–38 minutes, and
the folding fits inside it — the primitive walk decodes at 164–259 MB/s/core
(`DECODE_SHAPE.md` §2.6), the apply is 459 s over the whole store (7.7 min at
0.25 of a core), and the frame walk is 3.1 min. Call it **2–4 of 6 cores for
the duration**, leaving 2+ for serving. So optimising decode speed buys
*politeness* on this box, while two other things buy *feasibility*:

1. **The log cannot be held.** 230.7 GB of chunks + artifacts + OS is over the
disk, so the input is a **rolling window**: `mithril.rs` already drives
   `mithril-client cardano-db download --start/--end`, so the box can hold a few
   hundred chunks, fold them, and delete them. A 400-chunk window is ~10 GB.
   That makes the log **not re-readable**, which promotes one-walk-N-artifacts
   from an optimisation to a **requirement** (`MITHRIL_PHONEBOOK.md` §1).
2. **The live set cannot be a `HashMap`.** 2.17 GiB anon on an 8 GB box, next to
a serving side that needs its own headroom and a page cache that the kernel can
   evict where it cannot — FOOTPRINT §4.1's measured point. The target is
   **anon RSS ≤ 512 MB for the folder**, which the compact shape reaches: 12
   bytes per row is 130 MB at 10.9 M rows (`LIVE_BOOK.md`), and the rows belong
   in the artifact file rather than the heap.

The rest of the goal set, so nothing is optimised at another's expense:

- **first install** — bounded RSS, resumable, no 230 GB of scratch;
- **refresh** — every ~6 h, incremental chunks, tiny;
- **serve** — mmap'd artifacts, low anon RSS, must not be starved while folding;
- **re-derive** — a schema change or a projection bug means re-reading history,
  which without the log means re-downloading it. Expensive but *possible*, and
  the reason the corpus harness matters more on this box than on the big one;
- **co-tenancy** — all of the above on one 6-core/8 GB box, so the folder runs
  `Nice`/`IOSchedulingClass=idle` and bounded, per §8.

## 8. Constraints on the unified walker (not a design)

The index services currently cost ~8.5 G across separate processes. The
proposed replacement — one generic Mithril walker with extractors writing
binary files and dbs — is out of scope here, but if it is built it must:
- keep extractor output **mmap-friendly and file-backed** (the tx-index
  pattern), so its footprint is evictable page cache rather than anon heap;
- run with a **bounded anon RSS independent of chain length** — streaming
  extraction, no whole-chain in-memory maps. **Target: ≤ 512 MB anon for the
  folder**, against 2.17 GiB measured for today's live set (§7's stated
  minimum); reachable because the index is 12 B/row and the rows are file-backed;
- take `--threads` and run `Nice`/`IOSchedulingClass=idle` by default: it is a
  co-tenant of a tip-following process and must never starve it;
- be resumable per chunk (chunks are immutable once complete), so a small box
  can build over hours without needing the peak of a one-shot run;
- be optional: `lean` runs without it.

### What a bigger box buys, in order (2026-09-22)

The constraints above are the *floor*. They must not become a ceiling: a folder on
a 768 GB box should be faster than the same folder on the 256 GB one, using the
same artifact format. One knob (`max_memory_bytes`, already required for every
profile by §9) and `--threads`; everything below is derived from those.

| resource | buys | measured basis |
|---|---|---|
| **cores** | the whole wall time. `max(decode/P, apply)` | decode scales 7.5× at 8 threads (14.2 s → 1.9 s, 150 chunks); the applier is one core and is 7.7 min a pass |
| **memory, a few tens of MB** | the decode pipeline itself | one plan per in-flight chunk: 1.7 MB on Conway, 6 MB on Babbage — a 16-deep queue is under 100 MB. **The fast path is nearly free** |
| **memory, ~150 MB** | **mandatory**: the build index at 12 B/row | below this the folder cannot hold mainnet's live set. This is the one item that must fit |
| **memory, GB** | page cache: the artifacts stay resident, and the read stage disappears | read is 2.8 min a pass and already page-cache-bound |
| **memory, GB** | the choice to hold **the log** instead of windowing it — which is the only way re-derivation avoids a re-download | §7's disk budget |
| **memory, GB** | more sinks built in one pass, each with its own resident state | one walk, N artifacts is a requirement, not an option |
| **hugepages** | the one genuine memory-for-speed trade on the apply | 146 ns/op at 70 k entries → 787 at 10.9 M: DRAM + TLB, which `MADV_HUGEPAGE` on the index attacks where a bigger cache does not |

**What memory does NOT buy**, so nobody buys more of it expecting this:

- **bytes are bytes.** 230.7 GB must be read, hashed and decoded once. Memory
  cannot reduce it, and hashing alone is a ~3-minute floor a pass.
- **the applier is ordered.** Block N+1 needs N. More memory buys nothing here;
  only the pipeline's depth helps, and its floor is the applier's own rate.
- **no cache to size.** There is deliberately no working set larger than the
  index: the log is the source of truth and everything else is an mmap'd
  projection. A dolos-style `cache = 500 MB` has no analogue here, which is a
  property of the design rather than an omission.

**The rule that makes this safe: the artifact format never varies with the
budget.** Only the folder's strategy does — queue depth, sink concurrency,
window-vs-log, anon-vs-file-backed rows. A book built on the 768 GB box must be
byte-identical to one built on the 256 GB box, or the small box cannot verify
what the big one produced.

## 9. Optimisation levers, in expected value order

Each is gated on §5 confirming the owner. Every one re-runs the §6 ladder.

1. **Bound every redb cache explicitly**, sized from config with small
   defaults; delete the 10 GB fallback. One knob per store class
   (kv / emissions / aux cache / watched-ref), plus a host-wide budget that
   divides across live instances so forks can't multiply it.
2. **Allocator**: `MALLOC_ARENA_MAX` in the unit file if it is enough;
   otherwise jemalloc/mimalloc as the global allocator with decay tuned, behind
   a feature until proven on both x86 and aarch64.
3. **Restart shape**: if the peak is replay concurrency rather than
   fragmentation, stagger instance start and cap concurrent catch-ups.
4. **dolos retention + cache config** for the `lean` profile.
5. **`max_memory_bytes` for every profile**, not just `dev` — already required
   by v3 before instance-per-digest ships; this doc supplies the numbers.
6. **Ship hardened units as the default install**: `MemoryHigh`/`MemoryMax`
   from the measured profile, `OOMScoreAdjust=-500` on the follower, `noatime`,
   small swap with `swappiness=10`.

## 10. Packaging prerequisites surfaced by this work

**Now its own design: `docs/design/INSTALLABLE_MITOS.md`** (PROPOSED
2026-09-21) — release binaries, one `mitos.toml`, `mitos init` / `doctor` /
`bootstrap`, a module catalogue, the install as a systemd state machine, and a
published data snapshot for time-to-tip. One finding there bears directly on
THIS doc: mitos is compiled on the box today, so the real minimum spec may be
`rustc`'s, not mitos's. Measure the build (`/usr/bin/time -v cargo build
--release -p mitos` on the ARM box) alongside §5 — if its peak RSS is above
the ladder's lowest passing rung, §6's numbers are not reachable by anyone
until release binaries exist.

Not in scope to solve, but this box is where they get found:
- ~~aarch64 availability of `mithril-client`~~ — **answered 2026-09-21.** The
  CRATE (`mithril-client 0.12.2`) is already in mitos's `Cargo.lock` via
  dolos's default features, so it has been building on aarch64 since the
  Appendix B benchmark.
- **First-run path is one binary (`mitos bootstrap`).** It fetches (if the
  store is empty) into a scratch dir, verifies certificate chain AND files
  (unlike upstream dolos, which inverts the skip-validation condition),
  renames into the chunk store, and imports. It never deletes the store.
  **Still not in the binary:** the weekly incremental refresh
  (`snapshot-refresh.sh` + mithril-client CLI 0.13.9 `--start N`). The crate
  at 0.12.2 used here is the v1 `cardano_database()` API; ranged (`From(n)`)
  download is a v2 (`cardano_database_v2` / `ImmutableFileRange`) surface
  that 0.12.2 does not expose the way the 0.13.9 CLI does. Do not bump.
- Module WASM builds are not reproducible across machines — self-hosters must
  pull shared modules by digest, never build them.
- A first-run path that does not need our operational knowledge: snapshot
  fetch, index build, time-to-tip stated honestly per profile. The first FULL
  fetch + import is `mitos bootstrap`. Incremental refresh remains the
  `mithril-client` CLI. Index build and time-to-tip are still WO-F3.

## 11. Order of work

As work orders (index: `docs/work-orders/README.md`, "The footprint track"):
**WO-F1** (`infra`) is step 1; **WO-F2** is the code steps 3–5 need before
they can be believed (cache knobs with UNCHANGED defaults + shadow delivery);
**WO-F3** is steps 2, 3 (on ARM) and 5; **WO-F4** is step 4's lever 1 with
measured defaults, and is half of platform v3's mainnet gate. Step 3's
zero-rebuild half runs on the x86 box with `infra`'s
`services/monitoring/smaps-sample.sh`, at the next planned restart.

The box's previous tenants were retired 2026-09-21; it is rebuilt from
nothing, by script, and is expected to be rebuilt again.

1. Provision the ARM box per v3 Appendix B (small ext4 root, XFS data volume
   with `reflink=1`, `noatime`); mainnet only, no preprod.
2. §3 verification (an hour; closes the index question).
3. §5 attribution on the **current x86 box first** for the zero-rebuild
   experiments (smaps sampling, `MALLOC_ARENA_MAX`) — that is where the
   17.5 G peak reproduces today — then on ARM.
4. Land lever 1 (+2 if indicated). Re-measure.
5. §6 ladder on ARM → first published numbers for `full`.
6. §7 questions → `lean` and `standard` numbers.
7. Feed the walker constraints (§8) into that design when it is scoped.

## 12. Non-goals

- Designing the unified walker or the remote resolution protocol.
- Replacing dolos or redb.
- Optimising anything §5 has not shown to matter.
- Multi-tenant hosting economics — this is about one operator, one box.
