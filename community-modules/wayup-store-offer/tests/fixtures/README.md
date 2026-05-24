# wayup-store-offer golden fixtures

## Runnable now

- **`offer-create-bootstrap/`** — cold-start create decode via the
  payment-credential bootstrap path. Real mainnet offers (one
  collection-wide, one asset-specific `PRED03385`); hash-only
  datums resolved through `chain_data::datum_by_hash`
  (`[[datum]]`), no block CBOR required. Covers every
  wayup-specific decode path: payment-cred watch, non-positional
  target payout, collection-wide vs asset-specific.

## Pending block capture (accept / cancel / update)

These need real block CBOR, since Consumed events only come from
`--block` dispatch (the `mitos-run` fixture format can't
synthesise a consume). `capture-block` reads a local dolos
archive (exclusive redb lock — stop the writer first), which
isn't reachable from CI; capture against the dolos node and drop
the `*.block.cbor` files in, then `UPDATE_GOLDEN=1` to generate
`expected.json`.

Each scenario needs **two** blocks: the offer's create block (so
the harness harvests the witness-set datum + the prior output)
and the spend block.

| scenario | spend TX | spend slot | create TX (datum source) | expect |
|---|---|---|---|---|
| `offer-accept` | `361985963006a6ed1e3ab4a338d8ee19a464712f7d62d8d03772e2b0553651f7` | _capture_ | the offer's create TX | one `accept` (asset under `ffa56051…`, price = offer lovelace, bidder not in `required_signers`) |
| `offer-cancel` | `ec1019f4c0bd2a1e52ca6c8bd96bec571206d6d51f2d3716e580330ad04e19b1` | _capture_ | the offers' create TX(s) | `cancel`(s) — bidder owner key `cba51a…` IS in `required_signers`, no asset delivered |
| `offer-accept-batched` | `3fc138a4b9cb28c7be0f12737139520ffd7124312dc78832bcda6fab02d367ea` | _capture_ | the offer's create TX | one `accept` (HouseOfTitans `53d6297f…`) — the accept is batched with a listing to a different script; only the offer-script spend should emit |

Capture (example):

```sh
cargo build --release --bin capture-block
./target/release/capture-block --config dolos.toml --slot <SLOT> \
  --out community-modules/wayup-store-offer/tests/fixtures/offer-cancel/<SLOT>.block.cbor
```

Then:

```sh
cargo build --release --bin mitos-build --bin mitos-run
./target/release/mitos-build --module community-modules/wayup-store-offer/wayup_store_offer.rs --fast
UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh   # review the diff before committing
```

### Discrimination these assert

Accept and cancel both spend with redeemer `d87a80`, so the
goldens are what pin the redeemer-agnostic logic: **accept** iff a
target-policy asset is delivered to a non-offer output AND the
bidder's owner key is NOT in the TX's `required_signers`;
otherwise **cancel**. The batched-accept scenario additionally
asserts that batching the accept with unrelated operations (a
listing to the sale script) still yields exactly one `accept`.
