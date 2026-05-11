# cip-25-mint fixtures

Real mainnet block fixtures for the cip-25-mint module's
golden tests, pulled from the production mitos host via the
`GET /_admin/blocks/by-tx/{tx_hash}` admin endpoint.

## Fixtures

### `186913993.block.cbor` + `perp-policy.toml` — single-asset baseline

- **Slot**: 186913993 (epoch 630, era Conway, 2026-05-10)
- **Block hash**: `e41bedef8f65b26ecddd4439f6789b8dc18da9783e1a79575dbf03d54564b150`
- **Mint TX**: `3f99d88cf07027d69a1f36cd0d9dec1b90fb1366b675a36e7625cb9f05ab9644`
- **Policy**: `e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0`
- **Asset (hex)**: `5065727035313538` (UTF-8 `Perp5158`)
- **Mint shape**: CIP-25, single asset, quantity = 1.

Validates that `cip-25-mint` emits one well-formed `Cip25Mint`
event with `metadata_json = Some(...)` against the real
Perp5158 mint. Exercises the happy path end-to-end.

### `186629202.block.cbor` + `ug-policy.toml` — 4-asset batch mint

- **Slot**: 186629202 (epoch 629, era Conway, 2026-05-07)
- **Mint TX**: `1420f1dd8b532c9314a06760211a41769e41a7439f298ec69098e74dc967f0ca`
- **Policy**: `8972aab912aed2cf44b65916e206324c6bdcb6fbd3dc4eb634fdbd28`
- **Assets**: `UG3334`/`UG3335`/`UG3336`/`UG3337` (collection
  "UGs by Squashua 2024"), 4 NFTs in one TX.

Multi-asset variant — proves:
- the `thread_local!` aux-data cache (4 minted events → 1
  `tx_metadata` host-fn call, not 4);
- per-asset-name lookup inside one policy's metadata sub-map;
- N-emission dispatch from a single `handle-events` batch.

## Re-pulling

Block fixtures come from the live archive on the production
mitos host. To refresh or add a new one:

```bash
TOKEN=$(ssh root@159.195.57.187 'grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2')

ssh root@159.195.57.187 "curl -sS -D /tmp/h.txt \
  -H 'Authorization: Bearer $TOKEN' \
  http://127.0.0.1:8181/_admin/blocks/by-tx/<tx_hash> \
  -o /tmp/block.cbor && grep -i x-mitos-block-slot /tmp/h.txt"

scp root@159.195.57.187:/tmp/block.cbor \
    community-modules/cip-25-mint/tests/fixtures/<slot>.block.cbor
```

The slot reported in `x-mitos-block-slot` becomes the filename.
