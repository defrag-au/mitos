# cip68-hash-only-datum

Cold-start golden for `collection-metadata` over a CIP-68
reference token whose datum is attached **by hash** (no inline
bytes on the output). It is the regression guard for **CO3** — the
hash-only resolution path that the inline-datum fixtures (mekka,
nikeverse) don't cover.

## What it proves

During cold-start, `decode_page` reads the ref-token UTxO and calls
`read_output_datums`. For a hash-only datum the host returns a
`TypedDatum` with the hash set but `original_cbor == None`. The
module's `resolve_datum_bytes` sees the empty payload and falls
back to `chain_data::datum_by_hash`, which resolves the preimage.

The bug this guards against: the `read_output_datums` facade used
to collapse the unresolved entry to `None`, dropping the hash, so
`decode_page` skipped the asset and emitted no `Initial`. Revert
that fix (`crates/mitos-platform/src/host_fns/mod.rs`, the blanket
`read_output_datums` impl) and this golden fails — the single
`initial` event disappears, leaving only `snapshot_begin` /
`snapshot_end`.

## How the datum was generated

Synthetic. The datum is a CIP-68 V1 `Constr 0 [metadata_map,
version=1]` where `metadata_map` is:

```
name       = "Hashfox #0001"
image      = "ipfs://bafkreihashfox0001imagecidplaceholder0001"
background = "Cobalt"
body       = "Mecha"
eyes       = "Laser"
```

The CBOR (`fixture.toml` → `[[datum]].cbor_hex`) and its
blake2b-256 hash (`datum_hash` on the UTxO + `[[datum]].hash`)
were produced with a throwaway test that built the `PlutusData`
via `pallas` and printed `hex(cbor)` + `hex(Hasher::<256>(cbor))`.
The hash is the real datum hash of the CBOR, so the
output→preimage link is authentic even though the values are
invented.

`background` / `body` / `eyes` are the trait-bearing scalar keys
the downstream worker turns into trait bitmaps; `name` / `image`
are handled specially. Map keys serialise alphabetically in the
emitted `metadata_json`.

## Regenerating expected.json

```
UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh
```

(Requires `target/release/mitos-run` and the collection-metadata
wasm artifact; review the diff before committing.)
