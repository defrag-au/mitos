# CIP-86 Metadata Oracles — mutable CIP-25 in `collection-metadata`

> **Status: DESIGN, not built (2026-09-07).** Scopes support for
> [CIP-86 "NFT Metadata Update Oracles"](https://github.com/cardano-foundation/CIPs/tree/master/CIP-0086)
> — post-mint CIP-25 metadata updates authorised by an oracle key, requiring no
> mint and no ref-token custody. Extends
> [`COLLECTION_MODULES.md`](./COLLECTION_MODULES.md) (the `collection-metadata`
> module and its CIP-25 facade). Shares its state substrate with Control C of
> [`METADATA_OBLITERATION_DEFENSE.md`](./METADATA_OBLITERATION_DEFENSE.md) —
> build them together. Consumer impact lands on `cnft.dev-workers`
> `collection-ownership`.

## TL;DR

CIP-86 lets a designated **oracle key** rewrite a CIP-25 asset's metadata
fields in an ordinary transaction — no mint, no burn, no ref token, no
relationship to the policy's UTxOs at all. Three consequences:

1. **`collection-metadata`'s CIP-25 half must become stateful.** It currently
   resolves once and caches forever (`collection_metadata.rs:1004-1006`). CIP-86
   turns metadata into a fold over chain history.
2. **The platform can't currently see the transactions.** Every interest
   predicate is UTxO-shaped; a CIP-86 tx matches none of them. Needs a new
   `has-metadata-label` predicate.
3. **It is a new metadata-obliteration vector, and a worse one** — see §7. A
   time-locked policy no longer implies immutable metadata.

Scope note that shrinks the problem considerably: **CIP-86 applies to CIP-25
assets only.** The spec requires that the asset "has had CIP-25 metadata
previously defined". The CIP-68 half of `collection-metadata` is untouched.

---

## 1. What CIP-86 actually specifies

The metadata label is **`86`**, not 867 — and the CIP flags it as provisional
("we're using `86` as a stand-in for the CIP number that will eventually be
assigned"). **Carry it as module config, never a `const`.**

Four actions under the top-level `86` key:

| Action | Effect | Authorised by |
|---|---|---|
| `assign_metadata_oracle` | sets `main_address` / `update_address` for a policy | initial: a mint/burn tx by the issuer. subsequent: the **main** address key |
| `simple_metadata_update` | per-token field set/clear (`null` removes) | the **update** address key |
| `tabular_metadata_update` | `field_paths` × `token_names` × `values` rectangle | the **update** address key |
| `regex_metadata_update` | fields applied to every token name matching a PCRE | the **update** address key |

Two encodings: **v1** = policy ids and token names as text; **v2** = raw bytes,
selected by `version: 2` under the `86` key. Regex is prohibited in v2.

**Ordering** is normative. Across transactions: blockchain order (slot, then
tx index). Within one transaction: CIP-25 first, then CIP-86 as
regex → tabular → simple → oracle assignment.

**Implicit assignment:** a native-script policy with no explicit assignment is
implicitly assigned an oracle at the enterprise address derived from "the
verification key from the native script", for *both* addresses. See §7 — this
is the dangerous clause.

---

## 2. Why this belongs in `collection-metadata`

`asset-metadata-update` is the tempting home — it's the existing "metadata
changed outside a mint" channel, it's stateless, and its `TxBuffer` /
`find_in_map_by_key_str_or_bytes` machinery transfers directly. But folding
there (or in the consumer) is **incorrect**, not merely inelegant:

`collection-metadata` owns the authoritative snapshot. Its `SnapshotBegin`
causes the consumer to wipe its metadata table outright —
`DELETE FROM asset_metadata` at
`cnft.dev-workers/workers/collection-ownership/src/ownership_do/mitos.rs:678`.
Recapture, re-onboard and orphan recovery all trigger it. Any CIP-86 fold held
anywhere else is silently reverted to mint-time CIP-25 on the next snapshot,
with no error.

⇒ **Wherever else CIP-86 is observed, `collection-metadata` must hold the
folded document.** That makes it the primary home, and makes a second
implementation elsewhere redundant rather than complementary.

`asset-metadata-update` still gains a `Cip86` variant, but purely as a *live
delta notification* — it is what drives `cid_index.rs:119-121`'s IIIF image
refresh staging, and that path works today. It does not fold.

Both parse via a shared crate, exactly as `cip-25-mint` and
`collection-metadata` already share `cardano_assets::cip25::cip25_metadata_json`
so their `metadata_json` is byte-identical (`cip_25_mint.rs:14-17`).

---

## 3. Architecture

```
   ┌─ platform ────────────────────────────────────────────────┐
   │ InterestPredicate::HasMetadataLabel(u64)      ← NEW        │
   │   matched in build_tx_batch() from TxDraft.aux_data_cbor   │
   │ TxContextEvent.witness_vkey_hashes            ← NEW        │
   └───────────────────────────┬────────────────────────────────┘
                               │ label-86 TXs now dispatch
   ┌───────────────────────────▼────────────────────────────────┐
   │  collection-metadata                                       │
   │                                                            │
   │  Interest: holds_policy(X)  +  has_metadata_label(86)      │
   │                                                            │
   │  State (kv):                                               │
   │    mentry:<policy>:<suffix>   → StoredEntry (folded doc)    │
   │    moracle:<policy>           → OracleAssignment    ← NEW   │
   │    mrev:<policy>:<slot16>:<suffix> → InverseDelta   ← NEW   │
   │                                                            │
   │  Emits: Snapshot{Begin,Chunk,End} | Initial | Updated      │
   └───────────────────────────┬────────────────────────────────┘
                               │
   ┌───────────────────────────▼────────────────────────────────┐
   │  collection-ownership worker — unchanged apply path         │
   │  stage_metadata_entry → asset_metadata → finalise_metadata  │
   └────────────────────────────────────────────────────────────┘

   shared: cardano-assets::cip86  (parse + apply, feature-gated)
           also consumed by asset-metadata-update for its Cip86 delta
```

---

## 4. Platform changes

### 4.1 `has-metadata-label` interest predicate

Every predicate today is UTxO-shaped (`at-address`, `at-payment-cred`,
`at-stake-cred`, `holds-policy`, `holds-asset`, `tick-every` —
`crates/mitos-data-plane/src/types/interest.rs:40`) and the only matchers are
`matches_output` / `matches_mint` (`:184`, `:210`). A CIP-86 tx matches nothing.

Watching the oracle's update address does **not** work as a substitute: the CIP
is explicit that inputs and outputs "are ignored", so the oracle address need
never appear in the tx.

**This is cheaper than it looks.** `TxDraft.aux_data_cbor` already exists and is
already populated, documented as *"currently unused — modules fetch on demand"*
(`crates/mitos-data-plane/src/block_events.rs:63-68`). The change is:

- add `InterestPredicate::HasMetadataLabel(u64)` + a `matches_metadata_labels`
  matcher that peeks only the top-level integer keys of the aux-data map;
- extend the `any_match` disjunction in `build_tx_batch`
  (`crates/mitos-data-plane/src/dispatch.rs:182-189`);
- `bootstrap_v2.rs` treats it like `TickEvery` — no current-state to hydrate
  (`:413-416`).

**Asymmetry to accept:** label matching narrows to "this tx carries label 86",
not "…for a policy we track". Policy filtering happens module-side. Today
label-86 traffic is near zero so the firehose is negligible; revisit if
adoption grows.

### 4.2 `witness-vkey-hashes` on `tx-context`

CIP-86 authorisation is "signed by the signing key of the update address".
`required-signers` (`wit-v2/world.wit:224-226`) is the tx body's optional
`required_signers` field — essentially only populated in Plutus flows. It is
not the same thing and cannot substitute.

Add `witness-vkey-hashes: list<list<u8>>` (blake2b-224 per vkey in the witness
set) to `tx-context-event` and `tx-record`. Extraction sits next to the existing
`witness_datums` extraction at `block_events.rs:241-245` — `MultiEraTx` already
exposes the witness set there.

**Modules must not verify signatures.** The ledger's UTXOW rule rejects any
transaction carrying an invalid vkey witness, so the presence of a witness hash
on-chain *is* proof of a valid signature over the tx body. Comparing the hash
against the oracle address's payment credential is the whole check.

> **Coordination cost:** adding a field to `tx-context-event` is a breaking WIT
> change, and `module-version` mismatch is enforced at module load
> (`world.wit:590-593`). Every module rebuilds and redeploys together.

This change is worth landing on its own merits — "which keys authorised this
tx" is unavailable to every module today.

### 4.3 (Deferred) label index for historical cold start

`rebootstrap` reconstructs from current UTxO state (`utxos_by_policy` →
`cip25_metadata_batch`). There is no way to enumerate historical label-86
transactions, so a policy onboarded *after* its CIP-86 updates cannot replay
them. `tx-index` doesn't help — it is keyed by tx hash.

Correct fix is a narrow `(label, slot, tx_idx)` index built during the walk,
exposed as `chain-data::txs-by-metadata-label`. Deferred to Phase 4; see §6 for
what Phase 2 does instead.

---

## 5. Module design

### 5.1 Shared parse + apply crate

New `cardano-assets/src/cip86.rs` behind a `cip86` feature, alongside the
existing `cip25.rs` / `cip68.rs`:

- `parse_label_86(aux_cbor) -> Option<Cip86Block>` — handles v1/v2 key encoding.
  The str-or-bytes key walk in
  `community-modules/asset-metadata-update/asset_metadata_update.rs:261` is the
  reference implementation; lift it rather than rewrite it.
- `apply_actions(doc: &mut Value, actions: &[Cip86Action]) -> InverseDelta` —
  applies in spec order and **returns the inverse** for rollback.

Returning the inverse from the same function that applies is deliberate: it
makes it structurally impossible for the undo path to drift from the do path.

### 5.2 Oracle registry

`moracle:<policy_hex>` → `OracleAssignment { main_cred, update_cred, source: Explicit | Implicit, assigned_slot, assigned_tx }`.

Explicit assignment requires a mint/burn of the policy for the *initial*
assignment (so it arrives on the existing `holds_policy` path) and a
main-key signature for subsequent ones (label-86 path). Implicit assignment is
**off by default** — see §7.

### 5.3 Folded document + change detection

`StoredEntry` gains `doc_hash: [u8; 32]` (blake2b-256 over the canonical
JSON serialisation of the folded document).

Today CIP-25 entries are stored with `datum_hash: Vec::new()`
(`collection_metadata.rs:1029`) — there is no change-detection key at all, which
is why the write-once `continue` at `:1004-1006` is safe. That `continue`
becomes: re-fold, compare `doc_hash`, emit `Updated` only on change. `datum_hash`
stays as-is for CIP-68 — per `METADATA_OBLITERATION_DEFENSE.md` §4 it is the
burn-proof content anchor and must not be repurposed.

### 5.4 Ordering

`flush_buffer` gains a third phase, after the existing two:

1. CIP-68 ref-token pairing (unchanged)
2. CIP-25 plain-token first-sighting (unchanged, except the write-once check)
3. **CIP-86 actions** — regex → tabular → simple → oracle assignment

Spec-mandated "CIP-25 before CIP-86 within a tx" falls out of phase 2 preceding
phase 3.

### 5.5 Rollback

`handle_rollback` is currently a deliberate no-op relying on re-apply
convergence (`:880-889`). That is sound while metadata is a pure function of
current UTxO state — true for CIP-68 (datum is on-chain now) and trivially true
for immutable CIP-25. **CIP-86 invalidates it.** A fold over history cannot be
re-derived from the current UTxO set.

Store inverse deltas, not document snapshots:

```
mrev:<policy_hex>:<slot_be16>:<suffix_hex> → InverseDelta
```

On `rollback(to_cursor)`: `kv_scan` the `mrev:<policy>:` prefix, re-apply
inverses for `slot > to_cursor` in **descending** slot order, delete as you go.
Prune below `tip - 2160 blocks` on a `tick`.

> **Trap:** `list-values` / `kv-scan` sort lexicographically
> (`world.wit:472-487`). The slot component **must** be fixed-width
> zero-padded hex (16 digits) or ordering is wrong — decimal slot strings sort
> `"1000" < "999"`.

### 5.6 Snapshot fidelity

Until §4.3 lands, a cold-started policy cannot replay pre-subscription CIP-86
history. Do not paper over this. `SnapshotBegin` carries:

```rust
enum OracleReplay { NotApplicable, Complete, IncompleteFrom(u64) }
```

`NotApplicable` when the policy has no oracle assignment; `IncompleteFrom(slot)`
when the module's history starts after the oracle was assigned. Consumers can
then decide whether to trust the snapshot or flag the collection.

---

## 6. Wire-format changes

`MetadataEntry.immutable: bool` is documented as *"`true` for CIP-25
(mint-time-only, no update path)"*
(`crates/mitos-community-events/src/collection_metadata.rs:68-71`). CIP-86
falsifies the premise, and the field is now genuinely three-valued. Replace it:

```rust
pub enum Mutability {
    /// CIP-25, no oracle assigned. Metadata is fixed at mint.
    Immutable,
    /// CIP-68 — rotatable by whoever holds the `000643b0` ref token.
    RefTokenDatum,
    /// CIP-25 + CIP-86 — rewritable by the policy's oracle update key.
    MetadataOracle,
}
```

`version` keeps its meaning as a monotonic counter: mint-time CIP-25 is `1`,
each applied CIP-86 update increments. This makes `MetadataUpdated.prior_version`
meaningful for CIP-25 for the first time.

---

## 7. Security — CIP-86 as an obliteration vector

`METADATA_OBLITERATION_DEFENSE.md` §1 enumerates two ways a maintainer can
destroy metadata. CIP-86 adds a third, and it is **strictly the weakest
precondition of the three**:

| Vector | Requires |
|---|---|
| CIP-25 republish hack | minting key **and** an unexpired policy |
| CIP-68 ref-token rewrite | custody of the ref tokens |
| **CIP-86 oracle update** | the update key. Nothing else. |

The load-bearing loss: **a time-locked policy no longer implies immutable
metadata.** Today, "the policy is locked" is the guarantee collectors rely on
for CIP-25 permanence. CIP-86 decouples metadata authority from minting
authority entirely — an expired policy's metadata stays rewritable forever by
whoever holds the update key.

**Implicit assignment makes this retroactive and silent.** Under the CIP, every
native-script policy with no explicit assignment is *automatically* assigned an
oracle at the address derived from its script key. A project that locked its
policy years ago and considers its collection finished would, under a
spec-faithful implementation, have a live metadata oracle it never asked for and
does not know exists.

**Decision: implicit assignment is OFF by default.** We honour explicit
`assign_metadata_oracle` only, with implicit assignment available as a
per-policy opt-in. This is a deliberate spec deviation and should be documented
as such in the module docstring.

Secondary justification: implicit derivation is under-specified anyway. "The
verification key from the native script" is only well-defined for a bare `sig`
script — `all` / `any` / `atLeast` policies have no single key — and the script
itself lives in the minting tx's witness set, which the module does not
currently see.

Two further hooks into the existing defense design:

- The §5.5 inverse-delta history **is** Control C ("per-asset metadata history
  in the module"). Build once, serve both.
- Control A's degradation circuit-breaker
  (`traits.rs::reconcile_traits`) needs no change to catch CIP-86 blanking —
  it triggers on set-bit collapse regardless of cause. Confirm it fires on an
  oracle-driven wipe as part of Phase 2 acceptance.

---

## 8. Scope cuts

**Regex updates: not implemented.** ECMA-262 regex, sourced from an untrusted
on-chain payload, evaluated against every token name under a policy, inside a
fuel-budgeted wasm module. Catastrophic backtracking is a trivial denial-of-
service against the module. CIP-86 v2 prohibits regex anyway. A `regex_metadata_update`
is parsed, logged, and ignored.

If it is ever required: evaluate host-side with the `regex` crate (linear time
by construction; it refuses to compile backreferences, which is a safe deny)
and cap the candidate name set.

**Tabular field-path escaping is undefined by the spec.** `field_paths` are
dot-separated with no stated escape for field names containing dots. We treat
every `.` as a separator, document it, and reject paths that would create a
conflicting scalar/object shape.

---

## 9. Consumer impact — `collection-ownership`

Deliberately small, because the fold happens upstream. The apply path
(`stage_metadata_entry` → `mark_metadata_dirty`, `mitos.rs:649-652`) already
handles `MetadataEvent::Updated` and needs no change.

What does need attention:

1. **Finalise debouncing.** `finalise_metadata` rebuilds trait schema, bitmaps
   and rarity for the *whole* collection. Under CIP-25 that runs at onboarding
   and rarely again. A CIP-86 oracle bot can emit continuously — the Equine
   worked example in the CIP updates horse stats. `mark_metadata_dirty` on every
   update means a full-collection rebuild per update inside a DO. Needs an alarm
   coalescing window, and ideally a rule that only trait-bearing field changes
   mark dirty.
2. **`rarity_rank` becomes time-varying.** Every consumer treating it as stable
   — pricing, collector badges, the frontend — inherits that. Audit before
   Phase 2 ships.
3. **`Mutability` enum** replaces the `immutable` bool at the deserialisation
   site.
4. **IIIF image refresh** continues to flow through
   `asset-metadata-update` → `image_refresh::stage_from_update`. No change.

---

## 10. Phases

| Phase | Scope | Stop-early value |
|---|---|---|
| **0** | `witness-vkey-hashes` on `tx-context` / `tx-record` | Useful independently — no module can answer "who authorised this" today |
| **1** | `has-metadata-label` predicate; `cardano-assets::cip86` parse; oracle registry + `OracleAssigned` emission. **No metadata mutation.** | Proves dispatch end-to-end; tells us who is actually using CIP-86 on mainnet |
| **2** | `simple_metadata_update` fold, live-only, inverse-delta history, `Mutability` enum, worker debounce | The 90% case working |
| **3** | `tabular_metadata_update` | Cheap once Phase 2's path-setter exists |
| **4** | `txs-by-metadata-label` index → retroactive cold start; `OracleReplay::Complete` | Only if adoption warrants |

Phases 0 and 1 are worth doing regardless. Phase 2 onward should be gated on
observing real mainnet label-86 traffic — the CIP is still a draft with a
provisional label.

---

## 11. Testing

Golden fixtures under `community-modules/collection-metadata/tests/fixtures/`,
following the existing `cip25-reveal-perp` / `cip68-datum-update-noop` pattern:

- `cip86-oracle-assign-explicit` — assignment in a minting tx
- `cip86-oracle-assign-rotate` — main-key-signed reassignment, no mint
- `cip86-simple-update-authorised` — update key present in witness set → applied
- `cip86-simple-update-unauthorised` — wrong key → **no emission** (the critical
  negative test)
- `cip86-field-removal-null` — `null` clears a field
- `cip86-tabular-update` — rectangle applied across 5 assets
- `cip86-rollback-restores` — apply two updates, roll back one, assert the
  document matches the intermediate state exactly
- `cip86-regex-ignored` — parsed, logged, no state change

The unauthorised-update fixture is the one that matters most: a signature check
that silently passes everything is indistinguishable from a working
implementation until someone rewrites a collection you don't own.

---

## 12. Open questions

1. **Label 86 is provisional.** If the CIP is assigned a different number, does
   the module track both, or cut over? Config-driven label list handles either;
   decide the migration story before Phase 2.
2. **Firehose ceiling.** At what label-86 volume does module-side policy
   filtering stop being acceptable, and do we need a compound
   `has_metadata_label(86) AND holds_policy(X)` predicate? Measure in Phase 1.
3. **RFT semantics.** CIP-86 addresses "token names"; for an RFT with many
   copies the metadata is per-identity, which matches our keying. Confirm no
   quantity interaction before Phase 2.
4. **Interaction with `lock_metadata_at_slot`.** A locked collection should
   presumably ignore CIP-86 updates after the lock slot — that falls out of
   "resolve ≤ N" for free, but confirm when Control B is built.
