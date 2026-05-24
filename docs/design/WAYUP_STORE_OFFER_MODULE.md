# wayup-store-offer community module

Design for a mitos community module that tracks **Wayup**
collection + asset offers, paralleling the existing
`jpg-store-offer` module. Realises Phase 4 of the consumer's
relayering plan
(`cnft.dev-workers/docs/JPG_STORE_MIRROR_RELAYERING.md`, which
sketched it under the placeholder name `wayup-co`).

Status: **Phases 1–3 complete.** Phase 1 (static payment-cred
manifest interest) + Phase 2 (the `wayup-store-offer` module +
`wayup_store_offer` event types, five real-mainnet goldens
passing 56/56, blocks pulled live from prod mitos's
`/_admin/blocks/by-tx` — no downtime) are in the mitos working
tree. Phases 3–4 (consumer wiring + UI) are in the `cnft.dev-workers`
working tree: `jpg-store-mirror` subscribes to both modules,
projects both into `collection_offers`, and the egui "My Offers"
tab badges each offer with its marketplace (see "Consumer
wiring" below). Remaining: deploy/commit sequencing.

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
**Confirmed (in-block creates):** Wayup reveals the offer datum
in the **create TX's witness set** — the `offer-create-sale`
golden resolves all six creates from the block with no
hand-authored `[[datum]]`, so dolos `DATUM_NS` (which indexes
witness datums) resolves creates live. Consumes likewise carry
the datum in the spending TX's witness set (Plutus requires the
spender to supply it). The remaining unverified case is the
**bootstrap** path for *long-open* offers: if an offer's create
block has been pruned past the archive horizon, its witness datum
may be absent from `DATUM_NS`, falling to the Maestro
`datum_by_hash` fallback (the CIP-68 hash-only datum-resolution
mechanism in collection-ownership). Low risk, but worth a
recapture smoke on a long-standing offer.

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

## Consumer wiring (`cnft.dev-workers/workers/jpg-store-mirror`) — Phase 3 DONE

Implemented in `src/do_state.rs` (builds + clippy clean on
`wasm32-unknown-unknown`):

1. **`source_module` column** on `collection_offers` — added to
   `CREATE TABLE` and via an idempotent `ALTER … ADD COLUMN …
   DEFAULT 'jpg-store-offer'` so pre-existing rows (all jpg.store)
   backfill correctly.
2. **`WayupStoreOfferChannel`** (`NAME = "wayup-store-offer"`,
   `Event = WayupStoreOffer`) registered alongside
   `JpgStoreOfferChannel` in `channels()`. Both events are mapped
   onto a source-neutral `OfferEvent`/`OfferRowInput` and share
   one SQL projection (`apply_create`/`apply_spent`/`apply_update`
   refactored to be source-agnostic); each channel stamps its own
   `source_module` + `co_version` string at the conversion
   boundary (`wayup_co_version_str` → `"V1"`).
3. **`subscribe_targets()` override** returns both
   `SubscribeTarget::Module` (jpg + wayup). The runtime opens a
   dial-back per target at `/_internal/apply-<name>` (worker's
   `:target` wildcard route) and routes by name
   (`trim_start_matches("/_internal/apply-")` → `lookup_channel`).
   Added `mitos-protocol` as a worker dep for `SubscribeTarget`.
4. **Recapture scoped by module** — `on_recapture` now does
   `DELETE FROM collection_offers WHERE source_module = ?` (the
   `module` arg == the channel name == the stamped `source_module`),
   so recapturing one brand leaves the other intact.
5. **Leader tracking spans both marketplaces** — `query_policy_leader`
   is source-agnostic (`WHERE target_policy = ? AND
   target_asset_names IS NULL`), so the policy leader is the best
   collection-wide CO across jpg.store + Wayup. No change needed;
   revisit only if a per-marketplace leader is wanted.

6. **UI marketplace badging (DONE)** — `mirror-types::UnspentCo`
   gains a `source_module` field (+ `marketplace_label()`),
   threaded through `/api/my-cos` + the ui-flow snapshot/delta into
   the frontend's `DiscoveredOffer`. The egui "My Offers" tab now
   attributes each offer: collection-wide tiles sub-group by
   `(marketplace, price)` and show the marketplace name in place of
   the generic "CO" glyph; asset-specific tiles carry it in the
   corner badge; both surface it in the hover tooltip. Builds +
   clippy clean on `wasm32-unknown-unknown` (types + worker +
   frontend).

7. **Dual-credential "My Offers" matching (DONE)** — the two
   marketplaces key `offerer_pkh` on *different* credentials:
   jpg.store on the bidder's **payment** cred (datum field 0),
   Wayup on the **stake** cred (datum field 0 = the offer
   frankenaddress's staking part / cancel signer). The frontend's
   wallet-derived `extract_payment_pkh` alone therefore found jpg
   offers but missed every Wayup one. Fix (consumer-side only, no
   module change — the rows are already keyed correctly): the
   frontend also derives the stake cred (`extract_stake_pkh`) and
   passes `?pkh=<payment>&stake=<stake>` to the snapshot + flow WS;
   `handle_my_cos` matches `offerer_pkh IN (payment, stake)` (the
   two are distinct 28-byte hashes, so each marketplace's rows
   match the right cred); `handle_flow_upgrade` tags the socket
   `flow:<payment>` **and** `flow:<stake>` so live Wayup deltas
   (stake-cred `bidder_pkh`) land on the same socket (delta routing
   unchanged). Wayup identity deliberately stays the stake cred —
   robust to a delivery address ≠ the connected wallet, and to
   connecting a different payment address under the same stake key.

**Still to do:** `co-stats` still groups by `co_version` only (V1
now appears alongside V2/V3 — operator-only, left as-is); the
`mirror-types` JSDoc/schemars output should be regenerated
(`cargo run -p mirror-types --bin mirror-types-jsdoc …`) so the
collection-explorer picks up the new `source_module` field.

### Deploy / commit sequencing

The worker now references `mitos_community_events::wayup_store_offer`,
which only exists in the **local mitos working tree** (Phase 2,
uncommitted). So, in order:

1. Commit mitos Phase 1+2 and publish a rev that includes
   `wayup_store_offer` + the deployed `wayup-store-offer` module.
2. Bump the `[workspace.dependencies]` mitos rev in
   `cnft.dev-workers` and **re-comment the `[patch."…/mitos"]`
   block** (local-dev patch must not ship — CI uses the git rev).
3. Deploy mitos (so the host hosts `wayup-store-offer`) **before**
   the worker re-subscribes — verify the multi-target subscribe
   degrades gracefully if the host doesn't yet know the module
   (one bad target must not drop the jpg subscription).

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
   metadata fallback). **All five goldens passing** in
   `run-golden-tests.sh` (56/56): `offer-create-bootstrap`
   (collection-wide + asset-specific via the payment-cred
   bootstrap path), `offer-create-sale` (six asset-specific
   creates via the in-block path, batched with a sale that must
   not emit), `offer-accept` (Mekanism2212, 55 ADA),
   `offer-cancel` (two cancels, bidder in `required_signers`),
   `offer-accept-batched` (HouseOfTitans6219 — recipient-match
   excludes the same-collection 5984 listed to the sale script in
   the same TX). See `tests/fixtures/README.md`.
3. **worker companion (DONE):** `source_module` column +
   `WayupStoreOfferChannel` + multi-target `subscribe_targets()` +
   scoped recapture, in `cnft.dev-workers` (builds + clippy clean,
   wasm target). See "Consumer wiring" above. *Gate (post-deploy):*
   Wayup COs appear alongside jpg.store; recapture of one brand
   leaves the other intact.
4. **UI (DONE):** per-offer marketplace badge in the "My Offers"
   tab (`source_module` on the wire → tile label / corner badge /
   tooltip). Builds + clippy clean (wasm). *Gate (post-deploy):*
   jpg.store + Wayup offers both visible, each clearly attributed.

Phase 1 is independent and reusable (any future payment-cred
module benefits). Phases 2–4 follow the `jpg-store-offer`
template. All additive — Wayup lands without touching the
jpg.store path.

## Open questions

- **Witness-set datum resolution** — creates + consumes confirmed
  in-block (see caveat). Residual: bootstrap of a *long-open*
  offer whose create block is pruned past the archive horizon
  (Maestro fallback covers it; smoke on recapture).
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
