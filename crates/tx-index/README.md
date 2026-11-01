# tx-index (library)

tx hash → `(chunk, body offset)` over a Mithril immutable DB.

The binary that builds and serves this is `tools/tx-index`; **this crate is
what walkers on the box link against.** Design and measurements:
`cnft.dev-workers/docs/design/TX_INDEX.md`.

## Why

A Mithril snapshot is the node's immutable directory verbatim: chunk files of
concatenated block CBOR, with sidecars keyed by SLOT. **Nothing maps a tx hash
to anything** — a hash is computed from body bytes and never stored. So
resolving a transaction input meant either carrying a UTxO buffer through a
forward walk or decoding and hashing every body in a band of chunks until the
wanted one turned up. Every walker in this workspace did one or the other.

This pays the decode+hash exactly once per completed chunk and keeps the answer.

## Two layers, deliberately separate

- **`segments/NNNNN.seg`** — one per completed chunk, entries sorted by hash
  prefix. Chunk files are immutable once complete (the node only appends;
  Mithril signs per-file digests that never change), so a segment is written
  once and never rewritten.
- **`base.idx`** — a derived, rebuildable cache: a 2²⁴-bucket prefix directory
  in front of one globally sorted array. Tx hashes are uniform, so a random
  lookup is one directory read and a scan of ~7 entries. Rebuilding is a
  counting sort; **extraction is the part that costs.**

Segments newer than the base are consulted first, newest first — inputs skew
young, so that order is also the cheap one.

MEASURED 2026-09-27: 124,020,199 entries, 3,043,602,984-byte base, 9,203 chunks.

## What `publish` puts on the mirror

Four shapes of the same bytes, because a *local* reader and a *hosted* one want opposite
things. A local reader maps the base and pays nothing per lookup; a hosted reader pays a
request per range, so what it needs is for each of those requests to hit a **cacheable**
object — Cloudflare caps that at 512 MB and the base is 2.83 GiB, so every read of it is
`cf-cache-status: BYPASS` and a full origin fetch.

| object | size | who reads it |
|---|---|---|
| `<build>/base.idx` | 2.83 GiB | native readers that map it; a hosted reader's fallback |
| `<build>/dir.idx` | 64 MiB | a reader that wants the whole fence array at once. **Cacheable**, unlike the base |
| `<build>/head.idx` | 9,280 B | every hosted reader: the header and the era table |
| `<build>/entries/NNNNN.bin` | ~93 KiB × 32,768 | every hosted reader: the entry shards |

An **entry shard** is `[fence table][entries]` for 512 buckets, with the fence values
rebased on the shard's own first. So the pair that bounds a bucket and the run it bounds
come out of one object, two ranged reads, ~200 bytes — and the reader never has to warm
the 64 MiB directory to find them. A cold range read of an object makes the edge pull the
whole object first, which is why the shards are small: a 300-transaction wallet touches
~300 of them, so ~28 MB cold rather than gigabytes.

MEASURED 2026-09-27, from Brisbane against the published mirror:

| read | ttfb | cache |
|---|---|---|
| 8-byte fence pair, warm shard | **49–105 ms** | HIT |
| 8-byte fence pair, never-read shard | 404 ms | MISS |
| 144-byte entry run out of `base.idx` | **308–401 ms, every time** | BYPASS |
| whole 64 MiB `dir.idx` | 6.29 s (37 ms ttfb) | HIT |
| never-read 252-byte object | 343 ms | MISS |
| never-read 4 MiB chunk | 626–816 ms | MISS |

The last two are the shape of a cold cost: **a flat ~350 ms per object, not per byte.**
That is why the shard count matters more than the shard size, and why `dir.idx` is not on
a browser's path.

## Native and wasm

`mmap` (**on by default**) carries the chunk extractor, the segments, the base
and the reader over it. It is off for wasm — `memmap2` cannot build for
`wasm32-unknown-unknown` — so a Worker takes this crate with
`default-features = false` and gets `format` and `decode`: the 64-byte base
header, the fence directory and the 24-byte entry, `&[u8]` in and plain data
out. That is what a locator reads a *hosted* base with
(`MITOS_AT_THE_EDGE.md` §7.6).

## 🔑 An entry stores only 8 bytes of the hash

The read side fetches the located body, **re-hashes it, and compares against
the full requested hash** — so a prefix collision costs one wasted read and
never a wrong answer. That confirmation is not optional; it is what makes an
entry carrying only 8 bytes of the hash sound.

## 🔑 The extraction pass is a shared resource

`extract_chunk_observed` / `extract_to_segment_observed` take a callback over
primitives, so a **second** index can be derived from the same block decode.
`crates/policy-index` rides it for +1.8%, where its own pass would be +100%.

Deliberately a callback rather than a trait over a foreign type: neither crate
depends on the other, and the caller wires them. A shared trait would have made
this crate depend on every index that ever wants to ride along.

## Traps

- ⚠️ **`list_chunks` excludes the NEWEST chunk.** The newest immutable file is
  still being appended to until the next one seals, and Mithril re-ships it
  whole on every refresh.
- ⚠️ **The extractor splits CBOR items itself** rather than trusting the
  `.secondary` sidecar, so every offset recorded is a fact about bytes this
  code actually saw. `span_of` range-checks each slice against the buffer it
  must have come from — an out-of-buffer offset would be a plausible-looking
  number pointing at unrelated bytes, and the hash check only covers bodies.
  Aux data is not hash-addressed, so a bad span there would be served silently
  as someone else's metadata.
- ⚠️ **A segment or base of the wrong format version is REFUSED**, not
  reinterpreted: the entry stride changed, and reading old bytes at the new
  width returns garbage locations instead of failing.
- Era is not in the entry — hard forks land on epoch boundaries and epochs are
  whole chunks, so era is a property of the chunk. Extraction refuses a chunk
  that mixes eras rather than record a lie.
