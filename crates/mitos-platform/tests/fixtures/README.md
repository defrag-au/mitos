# Block CBOR fixtures

Recorded mainnet block CBORs for the observable-equivalence
harness. Each fixture is a `*.block.cbor` file containing the
raw bytes of one block as `MultiEraBlock::decode` accepts.

## Adding a fixture

The simplest way to capture one is from the running mitos host's
data plane. Once the framework SDK lands a CLI subcommand will
do this in one step; for v1 the manual flow is:

```bash
# 1. Pick a slot known to carry an asset of interest (a Black
#    Flag mint, say). Mainnet explorers like cardanoscan show
#    block contents.
# 2. Use dolos or maestro to fetch the block CBOR by point.
# 3. Save bytes to <slot>.block.cbor in this directory.
```

Once a fixture is present, the equivalence test
`mainnet_fixture_emission_equivalence` picks it up automatically
and asserts the wasm module + reference emitter produce the
same `OwnershipChange::Transfer` events.

## V1 scope

The synthesised-`TxView` equivalence test runs without
fixtures; it covers shape parity. The fixture-driven test
covers the decoder + projection + emit chain end-to-end —
the full v1 done-definition validation.

Until a fixture lands the fixture test auto-skips with a
diagnostic.
