# burn-address fixtures

Real mainnet block fixtures for the burn-address module's
golden tests.

## Fixtures

### `158943010.block.cbor` + `elite-cats-sink.toml` — script-address burn sink

- **Slot**: 158943010 (epoch 595, 2025-06-21)
- **TX**: `d695425b745a3416d999ba2a8072786c917a138913ceb33e2f9e62067ff0ad1e`
- **Sink address**: `addr1w8qmxkacjdffxah0l3qg8hq2pmvs58q8lcy42zy9kda2ylc6dy5r4`
  (script credential, no staking key — the canonical
  burn-address shape)
- **Asset**: `8e40ce04191c1d584614d537d3adab4c082889bb8319c9aba8ffceb4` /
  `54686520456c6974652043617473` ("The Elite Cats")
- **Quantity at sink**: 169,000

The TX has four outputs:
- output 0 sends 169,000 of the asset to the sink (the burn)
- output 1: 844.6 ADA back to the sender (no assets)
- output 2: 5 ADA back to the sender (no assets)
- output 3: change UTxO with the **remaining** 5,494,317 of
  the same asset back to the sender's wallet

Expected emission: **one** `AddressBurn` for output 0 only.
Outputs 1/2 are pure-ADA and skipped (no assets to burn);
output 3 is at the sender's wallet, not a watched address.

## What this surfaced (and was fixed alongside)

- **Production bug** in burn-address: the module previously
  assumed the platform filtered Produced events per-output for
  `at_address` interest. The platform actually filters per-TX
  (`dispatch.rs:183-186` — any matching event qualifies → ALL
  events dispatched). Without per-output filtering in the
  module, every TX that touched a watched address would emit
  AddressBurn for the sender's own change outputs too.
- **mitos-run gap**: never invoked the module's
  `update_interest` export, so any module that needs runtime
  knowledge of its own interest set (burn-address being the
  textbook case) couldn't be tested via fixture alone. Now
  encodes the fixture's predicate list as CBOR and calls
  `update_interest(Replace, items_cbor)` after `init()` —
  same shape the production follower uses.

The module decodes the predicate CBOR with `serde::Deserialize`
on a local mirror of the on-wire `InterestPredicate` enum
(only the `AtAddress` variant carries semantic meaning here;
the others deserialise into `IgnoredAny` for shape-completeness).
