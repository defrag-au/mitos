# cip-68-mint fixtures

Real mainnet block fixtures for the cip-68-mint module's
golden tests, pulled from the production mitos host via
`GET /_admin/blocks/by-tx/{tx_hash}`.

## Fixtures

### `165639806.block.cbor` + `policy.toml` — 12-pair CIP-68 mint

- **Slot**: 165639806 (epoch 581, era Conway, 2025-09-07)
- **Mint TX**: `3c60bfff18b8a7f385a64c744cb44837ceff64f9966f07b5c81cb9a0edad54f9`
- **Policy**: `29728939434a25e57ef6a9b94ba3215508264fee665bbb35b16a2d56`
  (Mekka Dynasty)
- **24 mints in one TX**: 12 `_100` reference tokens + 12
  `_222` user NFTs, paired by human-name suffix.
- **Datum shape**: hash-only on outputs; actual PlutusData
  CBOR lives in the TX's witness set (typical CIP-68 pattern).
  Resolves via `chain_data::datum_by_hash`.

Exercises the full cip-68-mint path:
- CIP-67 label parsing (100 → reference, 222 → user NFT)
- in-batch buffering of `Minted` + `Produced` events
- pairing user-mint with reference-mint by human-name suffix
- datum resolution via `chain_data::datum_by_hash` (the
  reference UTxO carries a hash-only datum)
- PlutusData Constructor-0 walk → metadata map → JSON

This block surfaced two real bugs during initial testing:
1. `parse_cip67` was reading the CIP-67 label as plain
   bytes 1-2 instead of bit-unpacking the nibble-aligned
   12-bit label inside the 4-byte prefix.
2. The platform's `chain_data::datum_by_hash` host fn was
   hardcoded to return `None` (TODO comment, never wired).
   CIP-68 modules calling it would always fail to resolve.

Both fixed in the same PR as this fixture.
