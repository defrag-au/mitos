# fold-probe

What a UTxO fold over the Mithril chunk log actually costs. A measurement tool,
not a service — it answers a question rather than serving one.

Written for `docs/design/MITHRIL_PHONEBOOK.md` §3 ("two ceilings") and
`docs/design/FOOTPRINT_AND_MINIMUM_SPEC.md` §7 ("how far can dolos be pruned",
"the live UTxO is the part we keep"). Both rest on a claim that a purpose-built
folder is cheaper than `import_blocks`, and neither has a number under it.

## The four numbers, and why one probe reports all of them

| # | number | what it decides |
|---|---|---|
| 1 | decode vs apply wall time | only apply is serial — this split is the ceiling on every parallel answer below |
| 2 | same-chunk create→spend rate | whether speculative parallel apply is viable (high = a redo loop that eats the win) |
| 3 | live-set entries and shape | the sizing input for a packed or mmap'd layout, from real outputs |
| 4 | peak anon RSS | `FOOTPRINT` §4.1: anonymous memory is what a cgroup limit OOMs on |

Reporting any one of them alone is how a fold gets an architecture decision
made on a number that does not mean what it looks like.

## Two modes, because a window cannot answer (3)

```sh
# A contiguous window with a real live set. Seconds per chunk.
fold-probe fold --immutable /var/lib/mitos/data/immutable --chunks 10

# A named region, when the question is era-specific (Byron vs Conway differ).
fold-probe fold --immutable <dir> --start 9130 --chunks 10

# Every complete chunk, NO set at all — counters only, embarrassingly parallel.
fold-probe count --immutable <dir> --threads 8
```

`fold` walks a contiguous window with a real `HashMap<OutRef, Entry>`.
⚠️ **It starts from an EMPTY set**, so the entries it ends with are the residue
of the window and *not* mainnet's live set. Extrapolating that number to "the
size of the book" is not a rounding error, it is a different quantity.

`count` walks every complete chunk with no set: every output a valid tx creates,
minus every output a valid tx spends. No ordering requirement anywhere, so it is
a thread pool over chunks. `created − spent` **is** the live-set entry count, and
that is the only honest way to size the book without folding it.

## Running it on the ARM box

The store is the Mithril immutable directory, the one that already holds
`NNNNN.chunk` — on the current layout, `.../data/immutable`. Both modes read
only. Neither writes anything, anywhere.

```sh
# 1. Smoke: does it decode real chunks at all, and how fast. Minutes.
fold-probe fold --immutable <dir> --chunks 1

# 2. The four numbers over a representative window (~2.5 days). Minutes.
fold-probe fold --immutable <dir> --chunks 10

# 3. The exact live-set size. Hours, and it is a full decode pass of the store —
#    run it with the fold NOT running, or accept the contention and note it.
fold-probe count --immutable <dir> --threads $(nproc)

# 4. Optional, and the definitive version of (1)+(2): the real serial fold.
#    Hours. `--chunks 0` means every complete chunk.
fold-probe fold --immutable <dir> --chunks 0
```

`count` prints progress with a rate and an ETA every 50 chunks on stderr.

## Sanity checks — run these before believing anything

1. **`count` txs ≈ 123.6 M.** `crates/tx-index/README.md` measured 123,602,296
   entries over ~9,130 chunks, and `list_chunks` excludes the same newest chunk
   there as here, so the two passes cover the same bytes. A figure far off that
   is a decode bug, not a finding.
2. **`count` chunks == the segment count** from `tx-index stats`.
3. **`live` must be positive** in `count`. It prints a warning and refuses to
   call itself a live-set count when it is not (a `--max-chunks` run cannot be
   genesis-anchored, and will be negative).
4. **`same%` ≤ `hit%`**, always. With a cold window they are equal, which is the
   arithmetic proving itself.
5. **`Σ lovelace created` per chunk.** Expect on the order of 10⁸–10⁹ ₳ per 6 h
   chunk. That band is an expectation, not a measurement from this repo — the
   first run calibrates it, and a figure orders of magnitude outside it is the
   fastest signal of a bad decode.
6. **The entries column should plateau.** A set that grows linearly with chunks
   walked means spends are not being matched.

## What this deliberately does not do

Read this before quoting a decode figure as "the cost of a folder":

- **No addresses.** A folder matching module interest pays `address()` per
  output (`docs/design/WATCHED_UTXO_INDEX.md`); this does not. The decode figure
  is a floor — add the interest match separately.
- **No datum hashing.** Inline datum length is recorded; the blake2b over it is
  not. A real set can hold the datum on the containing output, so this is close
  to free, but it is not measured.
- **No collateral.** A phase-2 failure consumes its collateral and creates its
  collateral return; both are skipped with the invalid transaction. `count`
  reports the invalid count so the size of the omission is visible rather than
  assumed small.
- **No entity graph.** UTxO only. `AssetState` and the rest
  (`docs/design/COLLECTION_MODULES.md` §"what survives pruning") are separate
  projections with their own cost.
- **No I/O tuning.** One `fs::read` per chunk on the folding thread; `read` time
  is reported separately so a prefetch stage can be justified rather than
  assumed. No `posix_fadvise`, no mmap.
- **Default `SipHash`.** The map uses std's hasher, which a tuned folder would
  beat — so the apply figure is pessimistic in the direction that matters least
  (it makes parallelism look *less* attractive, not more).

## Cross-compiling for the ARM box

The target works. The default **linker** does not, and it fails the same way
through both routes — `cargo zigbuild`:

```
error: unsupported linker arg: --fix-cortex-a53-843419
```

and plain `cargo build --target aarch64-unknown-linux-musl`, which resolves `cc`
to Apple's clang:

```
ld: unknown option: --fix-cortex-a53-843419
```

The flag comes from rustc's own `aarch64-unknown-linux-musl` target spec, and it
is an **LLD** flag — Apple's `ld` never accepted it and zig 0.16.0's driver does
not forward it. So the fix is the linker rustc ships, which does accept it. No
zig, no PATH edit, no store path:

```sh
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
    cargo build --release --target aarch64-unknown-linux-musl -p fold-probe
```

VERIFIED 2026-09-22 from `aarch64-apple-darwin` with this workspace's pins
(rustc 1.98.0, zig 0.16.0): links, and `file` reports
`ELF 64-bit LSB executable, ARM aarch64, statically linked, stripped` at 1.6 MB.
`rust-lld` resolves by name because rustc looks it up in its own sysroot — hence
no `PATH` work.

The flag is a property of the target spec, not of this crate, so the same
one-liner builds every binary in the workspace. A `.cargo/config.toml` entry
would make it the default:

```toml
[target.aarch64-unknown-linux-musl]
linker = "rust-lld"
```

Not added here: it changes how the repo builds for everyone, which is a
maintainer decision rather than a probe one.

⚠️ **Do not take numbers from the musl cross-build.** Measured on the ARM box
2026-09-22, 150 chunks, identical output every time:

| build | 1 thread | 8 threads | 12 threads |
|---|---|---|---|
| `aarch64-unknown-linux-musl` (zigbuild) | 31.7 s · 97 MB/s | **70.9 s · 43 MB/s** | — |
| native `aarch64-unknown-linux-gnu` (built on the box) | 14.2 s · 217 MB/s | 1.9 s · 1,642 MB/s | 1.6 s · 1,962 MB/s |

musl was **2.2× slower on one thread and got *slower* with more** — 8 threads
beat 1 by 0.45×, with `vmstat` showing 370k context switches/s and the disk
98% idle, i.e. allocator contention and not I/O. `FOOTPRINT` §4.1's finding is
about glibc for the same reason `INSTALLABLE_MITOS` §3.1 picks glibc targets:
the allocator is part of the measurement, not a detail under it.

The cross-build is a fast first look and a smoke test that the code runs on
Linux at all. **Every number goes in a document comes from a native build on the
box** — `rsync` the tree (15 MB without `target/`) and
`cargo build --release -p fold-probe`, which took 1m38s cold on 12 cores.
