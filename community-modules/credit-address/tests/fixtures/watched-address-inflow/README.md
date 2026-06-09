# watched-address-inflow

Golden scenario for `credit-address`: an output landing at a watched address emits
one `AddressCredit` carrying the output's lovelace + asset list.

- **Block:** `158943010.block.cbor` — a real mainnet block (reused from
  `burn-address`'s `elite-cats-sink` fixture; captured via `tools/capture-block`).
- **TX:** `d695425b745a3416d999ba2a8072786c917a138913ceb33e2f9e62067ff0ad1e`.
- **Watched address:** `addr1w8qmxkacjdffxah0l3qg8hq2pmvs58q8lcy42zy9kda2ylc6dy5r4`
  (`fixture.toml`, `kind = "at_address"`).
- **Payer inputs:** the TX's two inputs (both from `addr1qxvtxter…`) are seeded as
  `[[utxo]]` entries (resolved via Koios mainnet `tx_info`) so the dispatcher can
  resolve the consumed prior-outputs — that's how the module attributes
  `from_address` (the largest-lovelace input). Without them the module skips the
  credit (no resolvable payer → backstop).
- **Expected:** one `AddressCredit` for output index 0 — `lovelace = 1198180` plus
  the `8e40ce04…` / `54686520456c6974652043617473` ("The Elite Cats") asset, qty
  169000, the watched-address echo, `slot = 158943010`, and the resolved
  `from_address = addr1qxvtxter…`. No `metadata` (this TX carries none; the field
  is omitted when absent). This is the credit-address contrast with burn-address:
  it emits even on the pure-ADA value and carries the `lovelace`.

Regenerate the golden after a payload change:

```
cargo run --release -p mitos-build -- --module community-modules/credit-address/credit_address.rs --fast
cargo build --release --bin mitos-run
UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh   # review the diff before committing
```
