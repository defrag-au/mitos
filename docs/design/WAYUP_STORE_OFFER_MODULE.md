# wayup-store-offer community module

Design for a mitos community module that tracks **Wayup**
collection + asset offers, paralleling the existing
`jpg-store-offer` module. Realises Phase 4 of the consumer's
relayering plan
(`cnft.dev-workers/docs/JPG_STORE_MIRROR_RELAYERING.md`, which
sketched it under the placeholder name `wayup-co`).

Status: **Phases 1–2 complete; all four goldens passing
(55/55).** Phase 1 (static payment-cred manifest interest) and
Phase 2 (the `wayup-store-offer` module + `wayup_store_offer`
event types) are in the working tree, with real-mainnet goldens
for create (bootstrap), accept, cancel, and batched-accept
(blocks pulled live from production mitos's
`/_admin/blocks/by-tx` endpoint — no downtime). Consumer-side
wiring (Phase 3) is the remaining work, in `cnft.dev-workers`.

The headline finding: Wayup's offers decode to **the same
shape** as jpg.store's (`Constr0[bidder, [payouts]]`, hash-only
datum, collection-wide vs asset-specific via an empty/populated
asset-name map). So the module is a close sibling of
`jpg-store-offer`, the wire events mirror its field set, and the
`jpg-store-mirror` companion absorbs it as a second channel into
the same `collection_offers` table. Four mechanical differences
drive the whole design — everything else is a rename.

## The four differences from jpg.store

| | jpg.store (`jpg-store-offer`) | Wayup (`wayup-store-offer`) |
|---|---|---|
| **Watch target** | one fixed bech32 address (shared marketplace staking cred) | **payment credential only** — staking cred is the *bidder's own stake key*, so each bidder's offers sit at a different full address sharing payment script `27d46ecb…128ea` |
| **Accept vs Cancel** | inverted redeemer: `d87980`=Cancel, `d87a80`=Accept | **both use `d87a80`** — no redeemer signal; discriminate by asset delivery + `required_signers` |
| **Datum recovery** | hash-only, bytes published in metadata labels 50–63 (self-recovering) | hash-only, **no metadata chunks** (label 674 only) — relies on host `DATUM_NS` (witness-set) + Maestro fallback |
| **Target payout** | last payout in the list | **whichever payout carries a non-ADA policy** (it's the middle one: fee / buyer / royalty) |

## On-chain format (decoded)

### Addresses & credentials

- **Offer payment script:** `27d46ecbec94b052d8f875cf3beafd0e8ca40e8ad069f677e0a128ea`
- **Offer UTxO address** = this payment script + **the bidder's
  own stake credential**. Different bidders ⇒ different full
  bech32 addresses. Verified: two sample offers at
  `addr1zynagmkta…ksy9ssnc` (bidder `cba51a…`) and
  `addr1zynagmkta…3ryn3m` (bidder `cc927c8d…`) share the payment
  script, differ in the staking part.
- **Marketplace-fee payout target:** payment cred `5f08a64f…`
  (`addr1q90s3fj0tq89s9e4qu8pk8fvu…`). Receives the per-cart
  "supporter" fee (~1 ADA) and the offer's marketplace-fee
  payout (e.g. 3 ADA). Not protocol-required, just baked into
  the TX — irrelevant to tracking.
- **Sale/listing script:** `addr1zxnk7racq…` (a *different*
  contract). Out of scope here — a future `wayup-store-listing`
  module, mirroring the existing `jpg-store-listing`.

### Datum (hash-only on chain)

```
Constr0 [
  bidder_owner_key : Bytes(28),     // bidder's stake cred; also the cancel signer
  payouts          : [ Constr0 [ Address, Value ] ... ]   // fee, buyer(NFT), royalty
]
```

- `Address` = full Plutus address
  `Constr0[paymentCredWrapped, stakingCredWrapped]`.
- `Value` = `Map<PolicyId, Constr0[flag:Int, Map<AssetName,Int>]>`:
  - **`flag=1`, empty inner map** → "any one asset under this
    policy" = **collection-wide** offer.
  - **`flag=0`, inner map populated** → exact value. ADA payouts
    are `{"" : Constr0[0, {"":lovelace}]}`; an **asset-specific**
    NFT payout is `{policy : Constr0[0, {nameHex:1}]}`.
- **Target collection** = the payout whose value map has a
  **non-empty policy key**. Scan all payouts — it is *not*
  positional. Empty asset-name map ⇒ collection-wide;
  populated ⇒ asset-specific (e.g. `PRED03385`, `PRED07677`).

This maps onto the same `decode_offer_datum` extraction
`jpg-store-offer` uses (`bidder_pkh`, `target_policy`,
`target_asset_names`) — the only change is iterating payouts to
find the non-ADA one instead of taking `.last()`.

### Lifecycle discrimination

- **Create** — produced output at the offer payment cred with a
  decodable datum. (Same as jpg.store.)
- **Accept** — offer UTxO spent with redeemer `d87a80`; a
  produced **non-script** output delivers an asset under the
  offer's `target_policy` to the bidder's wallet (= the datum's
  NFT-payout address). The NFT seller signs as a normal input
  owner.
- **Cancel** — offer UTxO spent with redeemer `d87a80`; **no**
  asset delivered; locked lovelace returns to the bidder; the
  bidder's owner key (datum field 0) appears in
  `additional_signers`.

Because accept and cancel share a redeemer, the `is_cancel`
redeemer branch from `jpg-store-offer` is dropped. The
implemented rule (`flush_buffer`): a consume is an **Accept** iff
the bidder's owner key is **NOT** in the TX's `required_signers`
**AND** the target asset is delivered to the **bidder's payout
address**; otherwise it's a **Cancel**. The `required_signers`
guard (the bidder signs only to reclaim) prevents the rare
false-accept where a cancel's change coincidentally carries a
target-policy asset; the delivery requirement prevents a
false-accept when a cancel omits `required_signers`.

The accept-finder matches the delivered asset by the **recipient's
payment credential** (the datum's NFT-payout address), not just
the policy. This is load-bearing: an accept TX routinely produces
*other* outputs under the same policy — the seller's change
(holding more of the collection) and, when the accept is batched
with a listing, a same-collection asset sent to the sale script.
Matching only on policy would report the wrong asset name; the
recipient match pins the actual delivery (verified by the
`offer-accept-batched` golden: HouseOfTitans6219 to the bidder,
not the 5984 listed to the sale script in the same TX).
`required_signers` arrives on the `TxContextEvent` the host emits
first for every matching TX.

### Accept payment mechanics (the non-atomic gotcha)

Verified against `3fc138a4…` (the chain-parent of `fab68bf1…`):
on accept, the consumed offer's lovelace is split per the datum
payouts — marketplace fee + royalty as dedicated outputs, NFT to
the bidder — and **the seller's net proceeds are folded into the
seller's change output, not a dedicated output**. Wayup also
**batches the accept with unrelated operations** (e.g. listing
other assets to the sale script) and **chains transactions**
(folds proceeds into change that the next TX spends).

Tracking consequences (all already handled by the
`jpg-store-offer` architecture — call them out so they don't
regress):

- **`price_lovelace` = the consumed offer UTxO's lovelace**, not
  inferred from outputs.
- **`seller_address` is commingled/unreliable** — there's no
  clean "seller received X" output. Capture best-effort (the
  input-owner who supplied the delivered NFT) or leave empty.
  The bidder-centric "my offers" view doesn't need it.
- **The offer-script spend is the sole source of truth** — emit
  exactly one Accept per spend regardless of what else the TX
  batches. Don't assume one-tx-one-offer (carts spend many;
  keep the flat-list `TxBuffer`).
- **Update pairing** keys on the bidder owner-key / stake-cred,
  not the address. A single cart can accept bidder X's offer
  while creating bidder Y's — they must not pair. Reuse
  `jpg-store-offer`'s "exactly 1 consume + 1 produce per bidder"
  guard. (Whether Wayup exposes an in-place "edit" at all is
  unconfirmed; the generic pairing is harmless if it doesn't.)

## Watching by payment credential

The make-or-break question — can the module subscribe to a
payment cred (varying staking part) and **backfill the existing
open offer book** — is **yes**:

- `ChainDataPlane::utxos_by_payment_cred()` is a first-class data
  method backed by dolos's `utxos_by_payment()` index (same tier
  as address/policy; 100K refs/call cap).
- `bootstrap_v2::run_bootstrap` already walks `AtPaymentCred`
  predicates via `scan_one_payment_cred`, and **recapture**
  clears the per-cred bootstrap flag and re-walks them — so the
  recapture workaround relied on for jpg-co works here too.
- `vesting-tracker` is the proven precedent (CrowdLock: one
  payment script, per-user varying staking cred).

### The one required mitos host change

`AtPaymentCred` is fully supported at runtime and in bootstrap,
but it can only enter the interest set two ways today:

1. **Dynamically** via `update_interest` (the `vesting-tracker`
   path — consumer-driven over the replication WS).
2. **Statically** via the manifest — **but `InterestSection`
   only models `addresses` + `policies`**
   (`../../crates/mitos-platform/src/manifest.rs`).

Wayup's payment cred is fixed and known at build time, so a
static declaration is the right ergonomics (declarative,
auto-bootstrapped on deploy, no consumer choreography). This
**shipped as Phase 1** — a small, contained change so a module's
source `<name>.toml` can declare:

```toml
[interest]
payment_credentials = ["27d46ecbec94b052d8f875cf3beafd0e8ca40e8ad069f677e0a128ea"]
```

Implemented in:

1. `crates/mitos-platform/src/manifest.rs` — `payment_credentials:
   Vec<String>` on `InterestSection` (`#[serde(default,
   skip_serializing_if)]`); folded into `is_empty()`.
2. `crates/mitos-platform/src/bootstrap_v2.rs` —
   `interest_from_manifest` takes the creds and pushes
   `InterestPredicate::AtPaymentCred([u8;28])` per cred via
   `decode_payment_cred` (hex + 28-byte length check, warn-and-skip
   on bad input, same shape as the invalid-policy path).
3. `crates/mitos-platform/src/host_v2.rs` — the
   `interest_from_manifest` caller passes
   `manifest.interest.payment_credentials`.
4. `tools/mitos-build/src/main.rs` — `read_interest_section`
   parses `payment_credentials` from the source TOML into the
   generated manifest.

`run_bootstrap` needed no change — it already walks `AtPaymentCred`
predicates. Tests: `bootstrap_v2::tests` (cred → predicate;
malformed-cred skip; length check) +
`manifest::tests::interest_payment_credentials_round_trip` (guards
the TOML serializer against dropping the nested table when
addresses/policies are empty — the `is_chunked_cold_start_module`
serializer caveat). `cargo clippy` clean.

(The dynamic `update_interest` path remains available as a
fallback — register `AtPaymentCred(27d46ecb…)` and let
`bootstrap_one_predicate` backfill — but the static manifest field
is the durable shape and what the module will use.)

### Datum resolution caveat

Wayup datums are hash-only **with no metadata fallback**, so the
module's `resolve_datum_bytes` is just "use the host-supplied
payload" — drop the labels-50+ parser entirely. The payload must
be populated from the witness-set datum (`DATUM_NS`), with the
Maestro `datum_by_hash` fallback that the collection-ownership
CIP-68 datum-resolution work added covering snapshot gaps.
**Confirm during build** that the host resolves witness-set
datums for both produced (create) and consumed (accept/cancel)
offers, and — critically — for the **synthetic bootstrap**
events (existing open offers resolved via `utxos_by_payment_cred`
must still get their datum bytes). This is the main
implementation risk; everything else is mechanical.

## Wire events

New `mitos_community_events::wayup_store_offer` submodule
mirroring `jpg_store_offer`'s field set, brand-namespaced:

```rust
pub enum WayupStoreOffer { Create(OfferCreate), Cancel(OfferCancel),
                           Accept(OfferAccept), Update(OfferUpdate) }
pub enum WayupStoreOfferVersion { V1 }   // single contract today; keep for fwd-compat
```

Fields identical to the jpg.store equivalents (`bidder_pkh`,
`tx_hash`, `output_index`, `lovelace`, `datum_cbor`,
`target_policy`, `target_asset_names`, `price_lovelace`,
`seller_address`, prior/new refs). `co_version` →
`WayupStoreOfferVersion`. Partition key = `target_policy`, same
as jpg.store, so per-policy serialisation + leader tracking carry
over unchanged.

> **Naming note:** the original relayering doc called this
> `wayup-co`. The later cutover doc established the
> `<brand>-store-offer` convention; use **`wayup-store-offer`**
> for the module id and `wayup_store_offer` for the events
> submodule.

## Companion changes (`cnft.dev-workers/workers/jpg-store-mirror`)

The companion already stores jpg.store offers in
`collection_offers` and was built anticipating a second source.
Phase 4 (consumer-repo work):

1. **`source_module` column** on `collection_offers` (the
   `do_state.rs` note already flags this). Backfill existing rows
   to `'jpg-store-offer'`.
2. **`WayupStoreOfferChannel`** — a second `MitosChannel`
   subscribing to `Module("wayup-store-offer")`, mapping
   `WayupStoreOffer::{Create→insert, Cancel/Accept→delete,
   Update→delete+insert}` to the same SQL, stamping
   `source_module = 'wayup-store-offer'`.
3. **Scope recapture** — `on_recapture`'s unconditional
   `DELETE FROM collection_offers` becomes
   `WHERE source_module = ?` so recapturing one brand doesn't wipe
   the other.
4. **Leader tracking** is per-policy and source-agnostic; decide
   whether the policy leader spans both marketplaces or is
   per-source. (Likely span both — the "best collection-wide
   offer" is a cross-marketplace fact. If so, recompute over the
   union; no schema change beyond the column.)
5. **UI** — brand-aware filtering / badging (jpg.store vs Wayup),
   per the relayering acceptance gate.

## Phases

1. **mitos host (DONE):** static `payment_credentials` interest
   (4 spots above) + unit tests (cred→predicate, malformed skip,
   round-trip). The existing
   `bootstrap_by_policy_v2.rs` integration test already proves the
   `AtPaymentCred` bootstrap dispatch path
   (`scan_one_payment_cred`); a manifest-driven golden lands with
   the module fixtures in Phase 2.
2. **mitos module (DONE):** `community-modules/wayup-store-offer/`
   + `mitos_community_events::wayup_store_offer`. Datum decode
   (non-positional target payout), `required_signers`+asset
   accept/cancel discrimination, `datum_by_hash` resolution (no
   metadata fallback). **All four goldens passing** in
   `run-golden-tests.sh` (55/55): `offer-create-bootstrap`
   (collection-wide + asset-specific via the payment-cred
   bootstrap path), `offer-accept` (Mekanism2212, 55 ADA),
   `offer-cancel` (two cancels, bidder in `required_signers`),
   `offer-accept-batched` (HouseOfTitans6219 — recipient-match
   excludes the same-collection 5984 listed to the sale script in
   the same TX). See `tests/fixtures/README.md`.
3. **worker companion:** `source_module` column +
   `WayupStoreOfferChannel` + scoped recapture (consumer repo).
   *Gate:* Wayup COs appear in `co-stats` alongside jpg.store;
   recapture of one brand leaves the other intact.
4. **UI:** brand filter. *Gate:* relayering Phase 4 gate.

Phase 1 is independent and reusable (any future payment-cred
module benefits). Phases 2–4 follow the `jpg-store-offer`
template. All additive — Wayup lands without touching the
jpg.store path.

## Open questions

- **Witness-set datum resolution on bootstrap** — the main risk
  (see caveat above). Confirm before Phase 2.
- **In-place offer edit** — does Wayup expose one (consume +
  re-produce same bidder)? If not, the Update path is dead code
  but harmless. Decode a wider TX sample to confirm.
- **`seller_address` policy** — best-effort from inputs, or drop
  the field for Wayup? Bidder-centric tracking doesn't need it.
- **Leader scope** — per-source vs cross-marketplace (see
  companion §4).

## Cross-references

- `community-modules/jpg-store-offer/` — the module to clone
- `community-modules/jpg-store-listing/` — pattern for the future
  `wayup-store-listing` sibling
- `community-modules/vesting-tracker/` — the `AtPaymentCred`
  precedent (one payment script, varying staking cred)
- `crates/mitos-platform/src/bootstrap_v2.rs` — `run_bootstrap` /
  `scan_one_payment_cred` / `interest_from_manifest`
- `crates/mitos-data-plane/src/lib.rs` — `utxos_by_payment_cred`
- `cnft.dev-workers/docs/JPG_STORE_OFFER_CUTOVER.md` — the
  4-variant event surface this mirrors
- `cnft.dev-workers/docs/JPG_STORE_MIRROR_RELAYERING.md` —
  Phase 4, which this realises
