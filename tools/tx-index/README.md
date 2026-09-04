# tx-index

tx hash → (chunk, body offset) over a Mithril immutable DB, so resolving a
transaction input is one directory read and one ~500-byte `pread`, not a
UTxO buffer carried through a walk or a decode-and-hash sweep over a band
of chunks.

Library: `crates/tx-index` (what walkers on the box link). This binary is
the refresh step and the loopback read surface. Design and measurements:
`cnft.dev-workers/docs/design/TX_INDEX.md`.

## Commands

```bash
# Extract a segment for every completed chunk that lacks one, then compact.
# First run on an empty --index-dir = the full-chain bootstrap.
tx-index build --immutable <db/immutable> --index-dir <dir> [--threads 8] \
    [--compact auto|always|never] [--tail-limit 8] [--max-chunks N]

# Rebuild base.idx from the segments on disk.
tx-index compact --immutable <db/immutable> --index-dir <dir>

# Re-read every Nth body of each segment, re-hash, check prefix + decode.
tx-index verify --immutable <db/immutable> --index-dir <dir> [--stride 97] [--chunks 9000,9050]

# One hash → body + outputs (or --index N → one output). JSON.
tx-index lookup --immutable <db/immutable> --index-dir <dir> <hash> [--index N]

tx-index stats --immutable <db/immutable> --index-dir <dir>

# axum on 127.0.0.1:8186; TX_INDEX_TOKEN gates /tx/* and /resolve.
tx-index serve --immutable <db/immutable> --index-dir <dir> [--reload-secs 30]
```

## On disk

```
<index-dir>/
  segments/NNNNN.seg   one per completed chunk, entries sorted by hash prefix;
                       written once (tmp + rename), never rewritten
  base.idx             2^24-bucket prefix directory + every entry sorted;
                       rebuilt whole (tmp + rename) after a refresh
```

Entry = 16 bytes: 8-byte hash prefix, chunk `u16`, body offset `u32`, body
length `u16`. Era is per chunk (segment header / base era table). A hit is
always verified by re-hashing the body against the full requested hash, so
the 8-byte prefix can never yield a wrong answer, only a wasted read.

The newest chunk file is never indexed — Mithril keeps appending to it
until the next one seals — so a hash newer than the last sealed chunk is
`unknown_tx` here and still belongs to Koios / a tail spool.

## Library use

```rust
let idx = tx_index::Index::open(index_dir, immutable)?;
match idx.resolve(&hash, 0)? {
    tx_index::Resolution::Found { output, .. } => { /* address, lovelace, assets… */ }
    tx_index::Resolution::NoSuchOutput { outputs, .. } => { /* index past the end */ }
    tx_index::Resolution::UnknownTx => { /* tip or never landed → fallback */ }
}
```

`IndexHandle` wraps the same for a long-running process and re-maps when
the directory changes underneath it.
