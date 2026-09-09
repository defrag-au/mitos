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

MEASURED: 123,602,296 entries, 3.03 GB base, ~9,130 chunks on mainnet.

## 🔑 An entry stores only 8 bytes of the hash

The read side fetches the located body, **re-hashes it, and compares against
the full requested hash** — so a prefix collision costs one wasted read and
never a wrong answer. That confirmation is not optional; it is what makes the
16-byte-class entry sound.

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
