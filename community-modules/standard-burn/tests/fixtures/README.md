# standard-burn fixtures

Real mainnet block fixtures for the standard-burn module's
golden tests, pulled from the production mitos host via
`GET /_admin/blocks/by-tx/{tx_hash}`.

## Fixtures

### `173579456.block.cbor` + `derp-policy.toml` — single-asset clean burn

- **Slot**: 173579456 (epoch 612, era Conway, 2025-12-07)
- **Burn TX**: `7ac8414f94f5c8a3bbcb48ea7722254faaacc4b3faaf8ac6edf5a64a14b47485`
- **Policy**: `e74862a09d17a9cb03174a6bd5fa305b8684475c4c36021591c606e0` (Derp Birds)
- **Asset (hex)**: `44503034323739` (UTF-8 `DP04279`)
- **Mint shape**: `quantity_delta = -1`, **no label-721 aux-data**

Exercises the standard-burn happy path: pre-filtered
`MintedEvent` arrives with negative delta → module negates,
emits a single `Burn` event with `quantity_burned = 1`. No
metadata, no extra fields.

## Note on Derp Birds burn vs update history

Derp Birds (policy `e74862a0...c606e0`) is interesting for
both this module and `asset-metadata-update`'s Cip25 variant.
Older Derp Birds history (~Jan 2025) used the
**re-mint-then-burn** CIP-25 metadata update hack:

1. Original mint with label-721 metadata
2. Re-mint of same asset (positive `quantity_delta`) with new
   label-721 metadata
3. Clean burn (negative `quantity_delta`, no metadata) to
   bring supply back to 1

But that history is outside the production mitos archive
horizon (see `~/.claude/.../reference_mitos_archive_horizon.md`).
The recent Derp Birds activity (Nov 2025 — present) is
clean single-asset burns, no re-mint pattern. That's what
this fixture captures.
