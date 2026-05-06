# WIT datum extension

Adds module-side access to resolved datum bytes via two new
methods on the `block-context::resolved-block` resource. Required
for any indexer module that needs to decode datums (marketplace,
DEX, oracle, lending — basically anything beyond pure UTxO
ownership tracking).

This doc captures the platform-side rationale before the data-plane
wiring lands. Cross-references:

- `crates/mitos-platform/wit/world.wit` — the WIT change itself
  (in-tree as of the same commit that adds this doc)
- `MITOS_DATA_PLANE_API.md` — the in-process trait this WIT
  surface projects from
- `MITOS_BUILD.md` — the build-tool's "Datum access" note that
  references this doc
- `cnft.dev-workers/docs/design/JPG_CO_MODULE.md` — the first
  consumer; concrete shape the extension is sized against

## What's added

Two new methods on the existing `resolved-block` resource and one
new record:

```wit
record typed-datum {
    hash: list<u8>,        // 32 bytes
    payload: list<u8>,     // resolved on-chain CBOR bytes
}

resource resolved-block {
    // ... existing methods unchanged ...

    get-output-datum: func(tx-idx: u32, output-idx: u32)
                      -> option<typed-datum>;

    get-consumed-input-datum: func(tx-idx: u32, input-idx: u32)
                              -> option<typed-datum>;
}
```

`typed-output` is **not** modified. Records in WIT are
positionally-encoded in the Canonical ABI, so adding a field is
an ABI-breaking change requiring a major version bump. Resource
methods are additive — older modules that never call the new
methods are unaffected.

## Why this shape and not a `datum: option<typed-datum>` field on `typed-output`

Considered. Rejected because:

1. **ABI break vs additive.** Adding the field forces
   `HOST_ABI_MAJOR` from 1 to 2 and rebuilds every module. Adding
   methods is additive and stays at major 1.
2. **Pay-for-what-you-use.** `get-output` is the hot path for
   ownership-style indexers walking every output of every block.
   Carrying optional datum bytes on every `typed-output` adds
   marshalling cost the ownership module pays for nothing.
   Separate methods let the resolution + marshalling fire only
   when called.
3. **No semantic gain.** The datum is logically attached to the
   output, but the module's typical access pattern is "filter
   outputs by address, *then* fetch datum for matches" — a
   two-step flow that maps cleanly onto two method calls.

## Why no `plutus-data-by-hash` host fn

Considered. Rejected because:

It would re-leak the inline-vs-hash distinction the data plane is
designed to seal. A module that calls `plutus_data_by_hash(h)`
has already had to decide "is this a hash datum or an inline
datum" — at that point the abstraction is broken. The
caller-blind principle requires the module to ask "give me the
datum on this output" without knowing how it's stored on-chain.

The `get-output-datum(tx_idx, output_idx)` shape preserves the
seal: input is an output coordinate, output is bytes-or-none.
The host's resolution path (inline → same-block witness set →
archive) is private.

## Resolution path (host-side)

When `get-output-datum(tx_idx, output_idx)` is called, the host
checks in order:

1. **Inline datum on the output** — already on the decoded block.
   Free.
2. **Hash-attached, datum present in same block's witness set** —
   already on the decoded block (the host parses witnesses
   eagerly at block-decode time). Free, one map lookup.
3. **Hash-attached, datum cross-block** — Dolos archive lookup
   via `domain.query().plutus_data(&hash)`. Sub-ms warm, ~ms
   cold redb read.

Result is **memoised per-block** — second call for the same
output returns the cached resolution.

For consumed-input datum resolution, the host has already
resolved the prior output via `get-consumed-input` (lazy
data-plane read). The datum lookup uses the same memoised path
as produced outputs.

## Cost analysis

### Per-block CPU + I/O

Worst case for a multi-marketplace indexer watching dozens of
addresses (jpg-co + DEX + lending):

- ~50-200 datums of interest per block at peak; archive lookups
  for the cross-block fraction (typically <20% of the total)
- Per-lookup cost: <1ms warm, <10ms cold
- Worst-case per-block: ~200 × 10ms = 2s in pure-cold-cache
  scenarios; ~200 × 0.5ms = 100ms warm
- Realistic steady-state: <50ms per block, single-digit-percent
  CPU at mainnet's 20s block rate

For ownership-style indexers that don't call the new methods:
**zero cost** — they stay at `decode-level::lean` and the
resolution path is never triggered.

### Per-block boundary marshalling

Each `get-output-datum` call returns at most one
`typed-datum` (hash + payload bytes). Average payload size
~500B. Calls happen only when the module needs them (filtered
by address-of-interest first). For a marketplace indexer
emitting ~10 events per block, that's ~5KB extra boundary copy
per block. Negligible.

### Archive read amplification

`domain.query().plutus_data(&hash)` is one redb read per
unique hash. Dolos already maintains this index for its own
gRPC API, so we're not adding a new index — just exercising
an existing one more aggressively.

If profiling shows archive reads becoming a bottleneck (unlikely
at the volumes above), an in-memory LRU in front of the
archive read is the obvious fix. Park until evidence demands.

## What needs implementing host-side

Three pieces in `crates/mitos-platform/`:

1. **`block_context::ResolvedBlock` impl** — add the two new
   methods. They operate on the host's already-decoded block;
   inline + same-block-witness paths are zero-I/O.
2. **`chain_data` archive bridge** — for cross-block hash
   resolution, route via `domain.query().plutus_data(&hash)`.
3. **`mitos-data-plane` interface** — add the resolution
   primitive if it's not already there, mirroring the in-process
   `TypedDatum` shape but trimmed to `(hash, payload)` for the
   WIT projection (per `MITOS_DATA_PLANE_API.md`'s note about
   the WIT collapse).

No changes needed in `mitos-build` — the bundled WIT is read at
mitos-build's compile time, so a fresh `cargo install` of
`mitos-build` after the WIT change picks up the new shape
automatically.

## Backwards compatibility

The change is additive at the WIT level:

- **Existing modules** (just `ownership-indexer` today): unaffected.
  Don't call the new methods → no behaviour change. No rebuild
  required.
- **Existing `typed-output` consumers**: unchanged. Record layout
  preserved.
- **`HOST_ABI_MAJOR` stays at 1.** The minor version (currently
  unenforced metadata in manifests) can stay at 0 or bump to 1
  for clarity; not load-bearing.

## Open questions

1. **Should `decode-level::with-datum` pre-resolve** the datum on
   every output as part of `get-output`, or stay lazy via the new
   methods? Lazy is simpler and aligns with the consumed-input
   precedent. Eager pre-resolve costs nothing extra for indexers
   that always call `get-output-datum` immediately, but bills
   indexers that filter-then-fetch. Default to lazy unless
   profiling shows a clear win for eager.
2. **Memoisation lifetime** — per-block (current proposal) vs
   per-dispatch. Per-block is simpler and the natural unit for
   resource lifetime. Per-dispatch (multiple blocks in one
   handle-event call) doesn't apply because dispatch is one
   block per call by construction.
3. **Bulk datum lookup** for marketplace indexers walking many
   outputs at once — `get-output-datums(tx-idx) -> list<option<typed-datum>>`
   parallel to `get-consumed-inputs`? Park until profiling shows
   the per-output call overhead matters; v1 stays minimal.
