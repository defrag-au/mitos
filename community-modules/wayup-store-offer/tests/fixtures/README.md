# wayup-store-offer golden fixtures

## Runnable now

- **`offer-create-bootstrap/`** — cold-start create decode via the
  payment-credential bootstrap path. Real mainnet offers (one
  collection-wide, one asset-specific `PRED03385`); hash-only
  datums resolved through `chain_data::datum_by_hash`
  (`[[datum]]`), no block CBOR required. Covers every
  wayup-specific decode path: payment-cred watch, non-positional
  target payout, collection-wide vs asset-specific.

## Pending block capture (accept / cancel / batched accept)

These need real block CBOR, since Consumed events only come from
`--block` dispatch (the `mitos-run` fixture format can't
synthesise a consume). The three fixtures are **pre-authored in
`.staging/`** — interest + the consumed offer's `[[utxo]]` +
`[[datum]]` (datum CBOR verified to its hash) are filled in; only
the spend `*.block.cbor` is missing. They live under the
dot-prefixed `.staging/` so `run-golden-tests.sh` (which globs
`*/`) skips them and the suite stays green until they're promoted.

Each needs only the **spend block** — the consumed offer's prior
output + datum come from the fixture entries (the offer's create
TX need not be captured).

| staged scenario | spend slot | spend TX | expect |
|---|---|---|---|
| `offer-accept` | `188007187` | `361985963006a6ed…` | one `accept`, Mekanism2212 under `ffa56051…`, price 55 ADA |
| `offer-cancel` | `188007620` | `ec1019f4…` | two `cancel` (bidder `cba51a…` in `required_signers`, no delivery) |
| `offer-accept-batched` | `188010024` | `3fc138a4…` | one `accept`, HouseOfTitans**6219** under `53d6297f…` — NOT the 5984 listed to the sale script in the same TX |

`capture-block` reads a local dolos archive (exclusive redb lock
— stop the writer first), so run it on the dolos node. To
activate each (example for cancel):

```sh
cargo build --release --bin capture-block --bin mitos-build --bin mitos-run
S=community-modules/wayup-store-offer/tests/fixtures
./target/release/capture-block --config dolos.toml --slot 188007620 \
  --out $S/.staging/offer-cancel/188007620.block.cbor
mv $S/.staging/offer-cancel $S/offer-cancel          # promote out of .staging
./target/release/mitos-build --module community-modules/wayup-store-offer/wayup_store_offer.rs --fast
UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh        # generates expected.json
```

**Review the generated `expected.json` before committing** —
`UPDATE_GOLDEN` records whatever the module emits, so it captures
regressions thereafter but does NOT verify first-time
correctness. Check it against the "expect" column above (e.g. the
batched accept must report `486f7573654f66546974616e7336323139`
= HouseOfTitans6219, the asset paid to the bidder, not the listed
5984).

### Discrimination these assert

Accept and cancel both spend with redeemer `d87a80`, so the
goldens are what pin the redeemer-agnostic logic: **accept** iff a
target-policy asset is delivered to a non-offer output AND the
bidder's owner key is NOT in the TX's `required_signers`;
otherwise **cancel**. The batched-accept scenario additionally
asserts that batching the accept with unrelated operations (a
listing to the sale script) still yields exactly one `accept`.
