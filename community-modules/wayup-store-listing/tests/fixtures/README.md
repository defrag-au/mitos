# wayup-store-listing golden fixtures

Four scenarios, all replaying real mainnet blocks (captured
2026-07-17 via the prod admin endpoint — see the
`wayup-store-offer` fixtures README for the capture workflow).
Consumed listings' prior outputs + datums are supplied via
`[[utxo]]`/`[[datum]]` (sourced from Koios `tx_info`/`datum_info`)
since their create TXs aren't in the spend blocks.

| scenario | source | asserts |
|---|---|---|
| `listing-create` | TX `1ec380cd…` (slot 192663597) | five `create` (tappy collection, 122.5 ADA, 2 payouts each) — Wayup creates resolve their hash-only datum in-block, so prices are real (unlike jpg creates) |
| `listing-delist` | TX `d1e39e43…` (slot 192664494) | one `unlisting` (DP02916) — cancel redeemer `d87a80`, seller stake `5d3429…` matches the datum owner |
| `listing-update-batch` | TX `383d83e2…` (slot 192665968) | eleven `update` (DPs, 43 → 38 ADA) — Wayup price edits are cancel+recreate batches in one TX |
| `listing-ignores-sales` | TX `4edc97fd…` (slot 192438478) | **zero events** — buy spends (constructor 0) and same-TX offer creates are not listing lifecycle |
