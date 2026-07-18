# wayup-store-sale golden fixtures

Four scenarios, all replaying real mainnet blocks (captured
2026-07-17 via the prod admin endpoint — see the
`wayup-store-offer` fixtures README for the capture workflow).
Consumed listings supplied via `[[utxo]]`/`[[datum]]` from Koios.

| scenario | source | asserts |
|---|---|---|
| `sale-single` | TX `3b7862ee…` (slot 192679346) | one `sale` — BabyCroc #1067, 783 ADA, seller stake `b246ed…`, buy redeemer `Constr 0 [0]` |
| `sale-sweep` | TX `82dd69d5…` (slot 192651107) | seven `sale` — OUTPOST pieces from three distinct sellers, one buyer |
| `sale-mixed-offers` | TX `4edc97fd…` (slot 192438478) | four `sale` — cross-policy sweep (3 Perps + a CIP-68 Nikeverse name), buy redeemer fields are input indices (0/3/6/9 — why matching is on the `d879` prefix), and thirteen same-TX offer creates must not confuse buyer matching |
| `sale-ignores-delists` | TX `d1e39e43…` (slot 192664494) | **zero events** — a cancel spend (`d87a80`) is not a sale |
