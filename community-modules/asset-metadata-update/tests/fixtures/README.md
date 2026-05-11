# asset-metadata-update fixtures

Real mainnet block fixtures for the asset-metadata-update
module's CIP-68 variant, pulled from the production mitos host
via `GET /_admin/blocks/by-tx/{tx_hash}`.

## Fixtures

### `156189660.block.cbor` + `172076571.block.cbor` + `nikeverse1501.toml`

A two-block fixture exercising the CIP-68 metadata-refresh path
end-to-end. Both blocks are required: the first contains the
original CIP-68 mint (provides the prior UTxO + witness datum
the dispatcher needs to resolve at update time), the second
contains the actual update spend.

- **Mint block** (slot 156189660, 2025-05-20)
  - TX `87529915...d0fbf8`: mints 5 paired CIP-68 entries
    (Nikeverse 0980/1009/1501/1852/2847)
- **Update block** (slot 172076571, 2025-11-20)
  - TX `14402c8c...10a252a`: consumes the `_100 Nikeverse1501`
    UTxO and produces a new one with a different datum.
    Zero mints — pure ref-output respend.
- **Asset**: `000643b04e696b65766572736531353031` ("Nikeverse1501")
- **Policy**: `de79250af8caffc7a64645d86939159f665d4107c3f198562007bf32`

Run order matters — pass the mint block first so its outputs
land in the fixture's UTxO set before the dispatcher resolves
the update block's consumed input:

```bash
mitos-run \
  --artifact target/mitos/asset-metadata-update \
  --fixture community-modules/asset-metadata-update/tests/fixtures/nikeverse1501.toml \
  --block community-modules/asset-metadata-update/tests/fixtures/156189660.block.cbor \
  --block community-modules/asset-metadata-update/tests/fixtures/172076571.block.cbor
```

Expected: one `AssetMetadataUpdate::Cip68` event for
`000643b04e696b65766572736531353031`. The previous metadata
points at an `ipfs://Qm...` (CIDv0) image; the new metadata
swaps it for the equivalent `ipfs://bafybei...` (CIDv1) hash.
Everything else in the metadata is unchanged — a real-world
IPFS-format migration captured live.

## What this surfaced (and was fixed alongside)

- `mitos-run::FixtureDataPlane` previously didn't harvest UTxOs
  from blocks, only aux-data + witness datums. The dispatcher's
  `read_utxos` for prior-output resolution returned nothing,
  silently dropping every Consumed event. Now harvests UTxOs
  too (datum hash, address, assets) via the newly-`pub`
  `mitos_data_plane::project_typed_output` +
  `block_events::extract_datum_info` helpers.
