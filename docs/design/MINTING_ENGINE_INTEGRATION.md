# Minting-engine ← mitos integration

> ## UPDATE 2026-06-07 — re-scoped after reading the interest/companion code
>
> Supersedes the recommendation below (and a 2026-06-06 draft that proposed a new
> generic `tx-confirmation` module — **withdrawn**, see below). The original gap
> analysis is still useful background. Engine-side design:
> `cnft.dev-workers/docs/design/MINT_CONFIRMATION.md`.
>
> **Model shifts since May 27:** (1) fulfilment state moved to `mint_allocations`
> (slots) + `mint_log` (per-tx); the projection target is now `mint_log`
> submitted→confirmed + slots minting→minted + order→delivered via the engine's
> `transition_orders` primitive. (2) **Inline-mint distribution** — the engine
> mints AND delivers in ONE tx, so confirm = deliver, **one signal**.
>
> ### Decision: consume the EXISTING mint modules. Do NOT build a new module.
>
> Reading the code settled the "is a tx-hash interest too dynamic?" question — and
> it's moot, because **CF consumers can't express a tx-hash interest at all**:
>
> - **Two interest models, don't conflate them.** A CF consumer subscribes with
>   `mitos_protocol::Interest` (`crates/mitos-protocol/src/interest.rs`) — an
>   **asset-centric** selector (`Policy` / `Asset` / `Fingerprint` + domain). No
>   tx-hash axis, no address axis. The richer
>   `mitos_data_plane::InterestPredicate` (`AtPaymentCred` / `AtAddress` /
>   `HoldsPolicy` / `TickEvery`, dynamic via `update-interest`) is the **platform's
>   module-dispatch filter**, NOT what a CF consumer sends. (This distinction is a
>   doc gap — now noted in `SUBSCRIPTION_MECHANICS.md`.)
> - So the only consumer-expressible grain is **`AssetSelector::Policy`** — and
>   it's **stable**: registered once per collection at create-time, exactly as
>   `collection-ownership` does (`workers/collection-ownership/src/ownership_do/
>   handlers.rs` `onboard_to_mitos`). No per-tx churn. The user's "too dynamic"
>   instinct was right; a tx-hash interest set is both unexpressible here and the
>   wrong grain.
> - **The engine's mint txs ARE mints under its policy.** Subscribe to the
>   existing `cip-25-mint` + `cip-68-mint` modules (policy-filtered), match the
>   event's `tx_hash` to `mint_log` consumer-side → confirm. `collection-ownership`
>   **already subscribes to both** (`ownership_do/mitos.rs` `subscribe_targets`) —
>   the engine companion is a copy of that proven pattern. A brand-new module would
>   need a tx-hash interest axis that doesn't exist + a wasm build + dual-network
>   deploy, for no benefit over reusing what's there.
> - **Two channels (CIP-25 + CIP-68) is the accepted cost** — you mint exactly
>   those two standards, and the Maestro straggler-sweep backstops any gap (incl. a
>   future standard). This is the opposite of the 2026-06-06 draft: per-standard
>   channels are *fine* here precisely because the consumer grain is policy, not tx.
>
> ### Gap 1 (slot on mint events) — BUILT 2026-06-07
>
> `Cip25Mint` / `Cip68Mint` now carry `slot: u64`
> (`crates/mitos-community-events/src/{cip25_mint,cip68_mint}.rs`), matching
> `asset-transfer`. For the hard fork, rollback matters because inline "confirmed =
> delivered". Mitos surfaces reorgs via `ServerMessage::Undo { cursor: ChainPoint }`
> (`crates/mitos-protocol/src/wire.rs`), but the companion `undo()` hook defaults to
> a no-op (`crates/mitos-companion/src/traits.rs`) and `collection-ownership`
> doesn't override it (it relies on idempotent re-apply — fine for ownership, NOT
> for a "delivered" flag). The engine companion must implement `undo()` to revert,
> and a **precise** revert needs the confirmed tx's slot — now available.
> Implementation: `slot` sourced from the event `cursor` via a `cursor_slot` helper
> (copied from `asset-transfer`); `#[serde(default)]` keeps the field additive (old
> payloads decode `slot = 0`); `module_version` left at `(2,0)` (its major is the
> host ABI version, enforced `==2` at instantiation — the minor is unused for content
> versioning, so a bump would only diverge from the family). Golden fixtures
> regenerated (`UPDATE_GOLDEN=1`) + assert green (58/58).
>
> ### Build order (engine-side #1 + #2 already BUILT in cnft.dev-workers)
> 1. **#1 projection** ✓ — `db::confirm_mint_tx` / `revert_mint_tx` + confirm/
>    rollback routes.
> 2. **#2 Maestro straggler-sweep** ✓ — bounded, depth-buffered, CIP-agnostic; the
>    **fork-safe path** (push-feed confirms at ~0 depth, so the sweep — not the
>    companion — is what's relied on through the fork).
> 3. **Gap 1** (mitos) ✓ — `slot` on the two mint events (BUILT 2026-06-07).
> 4. **#3 engine companion** — subscribe `cip-25-mint` + `cip-68-mint`, policy
>    interest (registered at collection create), match `tx_hash` → `confirm_mint_tx`;
>    `undo()` → `revert_mint_tx` by slot. Sweep demotes to backstop. Lands after the
>    fork (the sweep covers it).

Design for what `cnft.dev-workers/workers/minting-engine` needs from mitos
to close its mint-lifecycle loop, and a frank inventory of what's
already there vs. what's missing. **Execution is deferred** — this
doc is the pickup point.

Status: **scoped, not started.** Authored 2026-05-27 after surveying
the existing community modules + their event shapes. Picked up after
the minting-engine has shipped its build→sign→submit path and can put
mints into the mempool, but before automated confirmation tracking is
wired.

## Why this doc

The minting-engine submits multi-recipient batch mint TXs to chain.
It currently knows nothing about what happens to those TXs after
submission — `collection_assets.status` stays at `'minting'`
indefinitely. Operationally that means:

- the portal dashboard can't show "minted N/total Y" live;
- the per-wallet `wallet_mints.minted_count` counters can't advance
  (gating logic for per-wallet phase limits stays unenforceable);
- failed mints (TXs that fall out of mempool / get rolled back)
  never get their reservations released.

Mitos is the natural authority for this loop — already running, already
watching every block, already filtering by policy, already pushing
events to companions. We picked it over the alternatives explicitly:
captain-hook is being deprecated, polling Maestro per `tx_hash` is
expensive + stale, and a bespoke chain-sync inside the minting-engine
worker would duplicate framework work.

What's *not* on the critical path right now: actually building any of
this. The engine is fine submitting + leaving rows at `'minting'` while
preprod testing happens (operator can verify on chain manually).
Confirmation wiring lands when the live dashboard wants to show
green-on-confirm.

## The lifecycle, with mitos signals overlaid

```
operator/order   engine            mitos                portal
─────────────    ──────            ─────                ──────
   POST order ─► reserve assets ──►(no event)
                pack + sign + submit ──►(no event)
                stamp `minting`,
                tx_hash on assets
                                    Cip25Mint event ◄─── chain confirms tx
                                    (policy, name,
                                     tx_hash, qty,
                                     metadata)
                  ◄─ companion POST ──
                advance `minted`,
                stamp `minted_at`
                                    Transfer event  ◄─── tx output settles
                                    (policy, name,        on recipient addr
                                     from, to,            (often same block)
                                     tx_hash, slot,
                                     qty)
                  ◄─ companion POST ──
                advance `delivered`
                bump wallet_mints
                                                     ─── dashboard goes green
```

Two distinct signals: **mint confirmed** (the TX landed; the asset
exists on chain) and **delivery confirmed** (the recipient received
the asset). In a single-TX batch mint they fire in the same block;
in pay-then-fulfil flows they can be hours apart.

Both are policy-filterable today via the standard companion
subscription pattern.

## What's already there

| Module | Event | What it carries | What it doesn't |
|---|---|---|---|
| `cip-25-mint` | `Cip25Mint` | policy, asset_name_hex, tx_hash, quantity, metadata_json, **slot** (Gap 1, 2026-06-07) | recipient address |
| `cip-68-mint` | `Cip68Mint` | policy, tx_hash, reference (datum_cbor + json), user (cip67_label, asset_name_hex, qty), **slot** (Gap 1, 2026-06-07) | recipient address |
| `standard-burn` | `Burn` | policy, asset_name_hex, tx_hash, quantity, previous_owner | **slot** |
| `asset-transfer` | `Transfer` | policy, asset_name_hex, from_address, to_address, tx_hash, slot, quantity | (covers the gap on its own for delivery) |

All four are policy-filtered server-side via the standard companion
interest set (`kind="policy"`, value = 56-char hex). All four roll
into `Domain::Mint(...)` / `Domain::AssetMovement(...)` on the
protocol envelope.

`collection-ownership` in cnft.dev-workers is the reference consumer
— it subscribes to multiple of these modules at once, fans events
into a per-policy DO, and persists holder state.

## What the minting-engine integration would look like, in shape

A new companion living inside `workers/minting-engine` (so the engine
owns its whole state machine — no cross-worker coordination for the
`collection_assets` row transitions). Companion declares two channels:

- **`cip-25-mint`** — on each event, match `(policy, asset_name_hex)`
  to a row in `collection_assets WHERE status='minting'`, advance to
  `'minted'`, stamp `minted_at`. Bump `collections.minted_count`.
  Optionally service-binding-fan-out to `client-management` so the
  portal's `PortalSessionDO` broadcasts a delta and the operator sees
  the card flip live.
- **`asset-transfer`** — on each event, if `to_address` matches the
  recipient recorded at order-reserve time, advance the row from
  `'minted'` to `'delivered'`, bump `wallet_mints.minted_count` for
  per-wallet phase-limit enforcement on the *next* order.

Interest registration: extend `client_management::collections::create`
to call `mitos_companion::interest::add_interest("policy", &policy_id)`
after the `collections` INSERT lands. Once registered, mitos starts
pushing events for that policy the next block it sees one. Bootstrap
explicitly does *not* replay historical mints; this is fine because
we control when the policy comes into existence (the same
`create_collection` call that derives the policy_id is what registers
it).

Cost: roughly a half day for cip-25 channel + interest registration,
another half day for asset-transfer channel + delivery state
transition. Total ~1 day of focused work to land both channels.

That work is unblocked *today* against the existing modules — no
mitos changes needed for the basic confirmation + delivery loop.
Below are the gaps we'd close if/when we want richer signal.

## Gap 1 — slot on community-module mint/burn events

**Status: DONE 2026-06-07** — `Cip25Mint`, `Cip68Mint`, **and** `Burn`
(`standard-burn`) now all carry `slot`. The mint/burn family is now symmetric
with `Transfer`. (Done together for hygiene even though the engine confirmation
loop only consumes the two mint modules — no current `Burn` consumer needs slot.)

**Symptom (resolved):** `Cip25Mint`, `Cip68Mint`, `Burn` didn't carry `slot`.
`Transfer` does. Asymmetric.

**Why it matters for us:**
- chronological correlation across the lifecycle ("this mint confirmed
  at slot S₁, recipient received at slot S₂") needs both ends to carry
  slot; today only the transfer end does;
- reporting / dashboard "minted N seconds ago" wants slot-from-now;
- when we eventually surface "all mints under this policy" in the
  dashboard, ordering by slot is the right primary key (`tx_hash`
  alphabetises arbitrarily).

**Why the protocol envelope already has it:** `ProtocolEvent` carries
`slot: u64` at the top level (see
`crates/mitos-protocol/src/protocol/mod.rs`). The community-module
event types simply don't re-export it into the payload they emit.

**Fix (applied):** Added `slot: u64` to `Cip25Mint`, `Cip68Mint`, and `Burn`.
Source is the event `cursor` (`MintedEvent`/`ProducedEvent` both carry
`cursor: chain-point`), read via a `cursor_slot(&ChainPoint)` helper copied
verbatim from `asset-transfer` — the same precedent `Transfer` set. `cip-68-mint`
captures the slot into its per-TX `TxBuffer` (first event wins, exactly like
`tx_hash`); `cip-25-mint` and `standard-burn` are stateless per-`Minted`-event so
they stamp it inline.

**Dev-workflow gotcha:** `mitos-run` statically links
`mitos_community_events::decode_emit` to turn emitted CBOR back into JSON for the
golden diff (`tools/mitos-run/src/main.rs`). So after changing an **event
struct**, you must rebuild `mitos-run` (`cargo build --release -p mitos-run`)
*before* regenerating goldens — otherwise its stale decoder silently drops the new
field (serde ignores unknown fields on deserialise) and the regenerated
`expected.json` won't contain it. Rebuilding the module wasm alone is not enough.

**Risk handling:** Wire-format change for live consumers, mitigated two ways:
(1) ciborium encodes structs as field-keyed CBOR maps, so existing consumers
ignore the new key (forward-compat); (2) `#[serde(default)]` on `slot` means a
*new* consumer decoding an *old* (pre-slot) payload gets `slot = 0` instead of a
decode error (backward-compat). `module_version` was **not** bumped: its major
is the host ABI version (enforced `== HOST_ABI_MAJOR_V2` at instantiation in
`registry_v2.rs`), and the minor is never consulted for content versioning — all
v2 modules declare `(2,0)`, so a bump would only diverge from the family without
effect.

**Verification:** `mitos-build` for both modules (wasm compiles), then
`UPDATE_GOLDEN=1 ./scripts/run-golden-tests.sh` regenerated the 3 fixtures
(perp 1 event @ slot 186913993, ug 4 @ 186629202, mekka 12 @ 165639806) — diff
is *only* the slot additions — and the plain assert pass is green (58/58).

**Effort:** ~30 min per module (as estimated).

## Gap 2 — recipient address on mint events

**Symptom:** A `Cip25Mint` or `Cip68Mint` event tells you the
`(policy, asset_name)` came into existence in `tx_hash`, but not which
output of that TX the new asset was minted to. For the
minting-engine's *common case* (single-TX batch mint, recipient
receives in the same block), we currently have to layer
`asset-transfer` just to know who got what.

**Why it matters for us:**
- two channels per policy when one would do, for the very common
  "minted + delivered in the same TX" case;
- the asset-transfer event fires once *per* output movement, which
  for an FT mint distributed across many recipients is N events vs.
  N+1 (the mint + N transfers) — modest fan-out but real;
- the mint event already has the TX in scope; computing the
  output→recipient map is a one-pass walk over `tx.outputs` against
  the just-minted assets. Cheap, deterministic.

**Why this might NOT belong on the mint event:**
- Cardano semantics: minting and movement are *distinct* operations
  in the ledger; bundling them on the mint event blurs that line. The
  `asset-transfer` module's job is precisely the movement domain;
  doing it twice (once on mint, once on transfer for the same atoms)
  is duplicative.
- For "pay-then-fulfil" mints where the mint TX puts the asset back
  at the issuer's own wallet (held until payment confirms), then a
  separate TX moves it to the buyer, the in-block recipient is the
  *issuer*, not the buyer. The mint event reporting recipient =
  issuer would be misleading.

**Recommendation:** **Don't add recipients to the mint event.**
Subscribe to `asset-transfer` for delivery; that's what it's for. The
fan-out is real but small (one extra POST per asset in the typical
case), and the layering stays clean.

We *could* add a derived `recipients: Vec<(asset_name_hex, address,
quantity)>` field if profiling later shows asset-transfer round-trips
dominate cost — but lift the asset-transfer module surface, don't
distort the mint module's semantics.

## Gap 3 — generic tx-confirmation primitive

**Symptom:** Our engine knows the exact `tx_hash` it submitted. The
only signal it strictly needs to advance `minting → minted` is "this
tx_hash landed at slot S". The mint event is an over-rich substitute
(carries metadata, per-asset breakdown) for a question that's really
"did the TX confirm?".

**Why this might be a future module:** A `tx-confirmation` module
keyed on a consumer-supplied set of `tx_hash`es (or
`(payment_credential, tx_hash)` pairs) that emits
`TxConfirmed { tx_hash, slot, block_hash }` events. Useful for:

- minting-engine confirmation (this doc's use case);
- wallet-operations send/withdraw confirmation;
- any service that submits a TX and needs to know it landed without
  caring what's *in* the TX.

**Why not now:** The existing mint modules answer our specific
question well enough; we just need slot (gap 1). A generic
tx-confirmation module is a wider concern — different consumers
(loan-book repayments, wallet-operations submissions, DEX swap
confirmations) would all benefit, but the design space is large
(interest model: hashes? credentials? both? bootstrap replay?
retention?). Worth a separate doc when more than one consumer is
queued.

For minting-engine today: gap 1 alone closes the confirmation hole.

## Gap 4 — bootstrap replay of historical mints

**Symptom:** Community modules emit forward-only events. Subscribing
late means historical mints under that policy are invisible.

**Why it's not on our critical path:** The minting-engine controls
when a policy comes into existence — `create_collection` derives the
policy_id *and* (when integrated) registers interest with mitos in
the same call. There's no time when a policy exists on chain before
we've registered interest in it. Backfill is a non-issue for our use
case.

For other consumers (someone bringing an existing collection into
the platform), this matters — they'd need an out-of-band backfill
path. That's out of scope for minting-engine integration; flagging
for the wider design.

## Recommendation

When confirmation wiring becomes critical-path (likely when the mint
dashboard #19 ships and operators want green-on-confirm in real
time):

1. **Land gap 1 (slot on mint/burn events) in mitos.** ~30 min × 3
   modules + abi_version bump + changelog. Backwards-compatible.
2. **Build the minting-engine companion against the (now-enriched)
   modules.** ~1 day for cip-25 + asset-transfer channels +
   interest registration in `create_collection`.
3. **Skip gap 2 — use asset-transfer for delivery.** Keep
   mint-module semantics narrow.
4. **Defer gap 3 indefinitely** unless a second consumer surfaces the
   same tx-confirmation need. Then design as a standalone module.
5. **Gap 4 is not a minting-engine problem** — out-of-scope here.

Net: ~1.5 days of work across both repos when picked up. Everything
needed to start that work is documented in this file.

## Anti-patterns (carried over from project memory)

- **Don't write a new in-tree mint indexer.** The community-module +
  companion pattern is the locked-in shape; in-tree indexers for
  chain-recognition work are not preferred.
- **Don't make policy-filtering string-DSL-shaped.** `Interest` enum
  selectors only; per-asset-name filtering is post-filtered consumer
  side.
- **Don't bake bech32 addresses into module wire formats.** Hex
  policy / asset_name / tx_hash on the wire; addresses are the only
  bech32 in flight, rendered at the consumer boundary.
- **Don't poll Maestro for confirmation per tx_hash.** Push from
  mitos is the way; polling burns Maestro quota for stale data.

## References

In mitos:
- `community-modules/cip-25-mint/cip_25_mint.rs` — the mint event
  source we'd consume first
- `community-modules/asset-transfer/asset_transfer.rs` — delivery
  side; already carries `slot`
- `crates/mitos-companion/src/{traits.rs,interest.rs,subscribe.rs}`
  — companion SDK
- `crates/mitos-protocol/src/protocol/mint.rs` — the envelope-level
  `MintPayload`
- `docs/design/SUBSCRIPTION_MECHANICS.md` — Interest model
- `docs/design/DOMAIN_REFACTOR.md` — the Mint/Burn/AssetMovement
  taxonomy
- `docs/HOWTO_CONSUMING_A_COMMUNITY_MODULE.md` — companion how-to

In cnft.dev-workers:
- `workers/minting-engine/src/` — the engine the companion would live
  alongside
- `data/schema/client-db/0001-initial.sql` — `collection_assets`
  state machine (`available → reserved → minting → minted → ...`)
- `workers/collection-ownership/src/lib.rs` — reference companion
  implementation, multi-channel pattern proven in prod
- `docs/design/MINTING_ENGINE.md` — engine's own design doc; layer 6
  (confirmation + delivery) is where this integration plugs in
