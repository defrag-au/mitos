# wayup-store-offer golden fixtures

Four scenarios, all replaying real mainnet data and passing under
`scripts/run-golden-tests.sh`.

| scenario | source | asserts |
|---|---|---|
| `offer-create-bootstrap` | offers `3938ca11…` (out 1) + `42f7a522…` (out 1), via payment-cred bootstrap | two `create` — collection-wide (`a316bcf7…`) + asset-specific (`PRED03385` / `73056bff…`); hash-only datum via `datum_by_hash`; non-positional target payout |
| `offer-accept` | spend `361985963006a6ed…` (slot 188007187) | one `accept`, Mekanism2212 under `ffa56051…`, 55 ADA — the asset paid to the bidder, not the seller's change Mekanisms |
| `offer-cancel` | spend `ec1019f4…` (slot 188007620) | two `cancel` — bidder `cba51a…` in `required_signers`, no delivery |
| `offer-accept-batched` | spend `3fc138a4…` (slot 188010024) | one `accept`, HouseOfTitans**6219** under `53d6297f…`, 150 ADA — the recipient-match picks the asset paid to the bidder, NOT the 5984 listed to the sale script in the same TX |

The create scenario is bootstrap-style (`bootstrap = true`,
`[[utxo]]` + `[[datum]]`, no block). The accept/cancel scenarios
replay the spend block; the consumed offer's prior output + datum
are supplied via `[[utxo]]`/`[[datum]]` (hash-verified) since the
offer's create TX isn't in the spend block.

## Regenerating / adding scenarios

Blocks were pulled from production mitos's read-only admin
endpoint (no service downtime — `capture-block` is the
stop-the-writer alternative):

```sh
TOKEN=$(ssh root@159.195.57.187 'grep ^MITOS_AUTH_TOKEN= /etc/default/mitos-mainnet | cut -d= -f2')
ssh root@159.195.57.187 "curl -sS -D /tmp/h.txt -H 'Authorization: Bearer $TOKEN' \
  http://127.0.0.1:8181/_admin/blocks/by-tx/<tx_hash> -o /tmp/b.cbor; grep -i x-mitos-block-slot /tmp/h.txt"
scp root@159.195.57.187:/tmp/b.cbor <scenario>/<slot>.block.cbor
```

Then `mitos-build --module …/wayup_store_offer.rs --fast` and
`UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh`. **Review the
generated `expected.json`** — `UPDATE_GOLDEN` records whatever the
module emits, so it pins regressions but not first-time
correctness; check it against the on-chain truth (asset names,
counts) before committing.
