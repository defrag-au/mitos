# SaturnSwap Analysis

## TL;DR

SaturnSwap is a Cardano order-book DEX that markets itself as
"no-batcher" but in practice **fills are gated on a protocol-held
required-signer key**. Without SaturnSwap's signing service online,
no fills can be constructed by anyone — orders just sit. Makers can
always cancel and recover their funds, so it's soft-custodial in the
specific sense of "execution depends on a centralized service" but
not "funds are trapped."

**Current verdict (2026-05): not worth integrating right now.**
The required-signer gate means we can't build fills ourselves (no
arbitrage / no programmatic execution), and the protocol-availability
risk makes ingest events noisy by nature. Revisit if SaturnSwap
removes the required signer, opens-sources their validators, or
becomes large enough that decode-only ingest is independently valuable.

## How we got here

Reverse-engineered from three Train-token fill TXs (no public
Aiken/Plutus source exists; the Orion-Crypto GitHub org only has
two C# wallet forks):

- `83f61e4634341de4a9e96d514ef19e68e38336e329a6b4c82af0e67947d1af14` — batched 8-order fill
- `7ce836b39bd8a8933d42ee3f237740c92b3a8a82103b7c2203704d5c004bea2a` — single partial fill
- `19e0a9fd2430901fee87a3b599f97fd43ab82fb061f3c04249c20f7e0c8c3506` — single partial fill

The last two are earlier links of the same partial-fill chain that
ends as input 5 of the batched TX. The chain reconciles exactly
(`6,883,242 Train sold for 327,573,454 lovelace`, constant
`47.59 lov/Train`).

## Architecture

### Scripts

Both scripts share the SaturnSwap protocol stake credential
`5ea481523030b23a495286ca1a18bd141a493e9b5a19d889953f6cdb` (purely
delegation — the staking script does not run on fills).

| Role | Script hash | Address |
|---|---|---|
| Order (Plutus V2, audited, closed-source) | `1af84a9e697e1e7b042a0a06f061e88182feb9e9ada950b36a916bd5` | `addr1zyd0sj57d9lpu7cy9g9qdurpazqc9l4eaxk6j59nd2gkh4275jq4yvpskgayj55xegdp30g5rfynax66r8vgn9fldndsqzf5tn` |
| Payout | `bec4575e6b77dfd0f60ccf510b0aa3dfc8ef69faa9774928130a849c` | `addr1zxlvg467ddmal58kpn84zzc2500u3mmfl25hwjfgzv9gf8z75jq4yvpskgayj55xegdp30g5rfynax66r8vgn9fldndshkm7wv` |

The reference script for the order validator lives at
`b3e3b7acef46f70ef511e7ea91c231a6e090a8a9790837a2b83396cf499203b3#0`
(22.59 ADA UTxO). Reused across every fill we observed.

DefiLlama's TVL adapter confirms the order address is the single TVL-
bearing script for SaturnSwap, so all open orders live there.

### Order datum

```text
OrderDatum = Constr 0 [
  owner          : Address,            -- always Address(ScriptCred(payout_script), Some(protocol_stake))
  sell_policy    : Bytes,              -- "" for ADA
  sell_name      : Bytes,
  sell_amount    : Int,
  buy_policy     : Bytes,              -- "" for ADA
  buy_name       : Bytes,
  buy_amount     : Int,
  flag           : Maybe<...>,         -- Nothing (Constr 1 []) in every observed order
  parent_ref     : TxOutRef            -- previous UTxO in the partial-fill chain; (#"00", 0) when root
]
```

Notable: the maker's wallet is **not** present in the order datum.
The "owner" address is always the payout script with the protocol
stake credential. Maker identity recovery must happen elsewhere
(probably during a payout-claim TX, which we haven't seen).

Partial fills chain via `parent_ref`. To reconstruct order lineage,
walk `parent_ref` backwards until you hit `(#"00", 0)`.

### Payout datum (very simple)

```text
PayoutDatum = Constr 0 [ TxOutRef ]   -- the order UTxO that produced this payout
```

Every maker payout output carries a datum that is just the
`TxOutRef` of the order input it came from. This is how
`order_input ↔ payout_output` pairing is reconcilable from
datums alone.

### Fill redeemer

```text
SpendRedeemer = Constr 0 [
  payout_lovelace : Int,   -- ADA going to the maker (== buy_amount for full fill, pro-rata for partial)
  self_input_idx  : Int,   -- this order's index in tx.inputs
  payout_out_idx  : Int    -- the matching payout output's index in tx.outputs
]
```

This is a self-locator pattern: the validator can `tx.outputs[payout_out_idx]`
directly instead of scanning. Cheap.

### Validator checks (extracted from script trace strings)

**Fill path:**

- `script_utxo_has_enough_asset`
- `is_asset_valid`
- `is_owner_valid`
- `is_order_active`
- `is_correct_swap_split` (partial-fill leftover correctness)
- `min_utxo_goes_back_to_script`
- `owner_value_has_correct_amount` / `owner_value_has_lovelace_amount`
- `is_correct_owner_ada_amount`
- `is_token_amount_correct`
- `is_acceptable_amount_sell`
- `is_correct_policy_id_{sell,buy}`
- `is_correct_asset_name_{sell,buy}`
- `is_correct_amount_{sell,buy}`
- `is_correct_valid_before_time`
- `inline_payment_datum`
- `is_fee_correct` + `is_fee_paid_to_address`

**Cancel path:**

- `is_cancel_valid`
- `tx_signed_by_owner`

So the cancel path only requires the maker's signature.

## Fee model

Two fee outputs per fill TX:

| Output | Formula | Recipient |
|---|---|---|
| Fee #2 (plain, no datum) | `max(1.0 ADA, 1% × executed_buy_amount)` | `addr1q8ana34nh6p2mcgzn2ysf3ufw2k834zzgfnujgym5tjdpp8w5qqdm6x976zqwjx5la6umsv8fc5u8q3u5ujeqmnq6zxqt8l4pj` |
| Fee #1 (inline datum tagging one order) | `max(~1.46 ADA datum-min-UTxO, fee#2 + 0.25 ADA)` | `addr1qyafdvdyhe04e4teqrwglkl93nsw7srwsj2g8r2ha9rj8z3duthu9cexm0vwn7ne74pk03vysnjkfwd93mr9t4jacwrs9pywvl` |

Validated against all three TXs (within ≤4 lovelace of model). So
**total protocol fee ≈ 2% of executed volume + 0.25 ADA flat, floored at ~2.46 ADA**.
Significantly higher than CSWAP/Splash on small fills (3.6% effective
on a 69 ADA fill). In batch fills, fee #1's datum only tags one of
the orders, so attribution from on-chain alone is lossy.

`executed_buy_amount` is the sum of pro-rata partial-fill amounts
in the TX, not full ask sizes.

## The centralization gate

Every fill TX we observed has two required signers:

- `98b32f23ef07fa4e2d2e7b9a1082404de17162043662ac97833da741` — the taker
- `7c2328db12987149ce8fdbbaa932c11542e24d6bd2d4876abffa58b8` — **same across all observed fills**

`7c2328db…` can't be a per-order maker key (different orders, same
key). It's a SaturnSwap-held protocol signer enforced by the order
validator via the required-signers list. Caveat: we've only seen
one taker across our three sample TXs — we should confirm the same
required signer appears on a fill by a different taker before
treating this as locked-in. Strongly suggestive but not yet
proven beyond doubt.

**Consequences:**

- Fills require SaturnSwap's signing service to be online and willing
  to sign. Functionally a router/scooper key, just lighter than
  Minswap/CSWAP's batcher infra.
- We cannot build an arbitrage bot or programmatic fill flow without
  their cooperation.
- If SaturnSwap goes offline (outage, deprecation, shutdown), open
  orders freeze. Makers can still cancel (`tx_signed_by_owner`
  cancel path is self-driveable), but no fills happen.
- "Self-recoverable but illiquid" is the right mental model for
  open orders during an outage.

## What's still open

Two TX samples would close the schema:

1. **A cancel TX** — spend from ORDER_SCRIPT without any new
   ORDER_SCRIPT or PAYOUT_SCRIPT output. Confirms the cancel
   redeemer constructor and the maker-only signing path.
2. **A payout-claim TX** — spend from PAYOUT_SCRIPT
   (`bec4575e…`). Reveals how makers prove identity when claiming
   (since maker identity is absent from the order datum), and
   whether the payout script has its own reference script.

Other smaller items:

- Confirm `7c2328db…` appears on a fill TX by a different taker.
- Confirm the fee #1 datum's purpose — is it just an audit anchor,
  or a maker rebate routed back somehow?
- The `flag` field at datum index 7 has only been observed as
  `Nothing`. Some order types might use it (expiry deadline? fill-or-kill flag?).

## Integration sketch (when/if we revisit)

A SaturnSwap mitos community module would emit:

- **OrderPlaced** — output to ORDER_SCRIPT with `parent_ref = (#"00", 0)`,
  no spend of any ORDER_SCRIPT input by the same maker.
- **OrderFilledPartial** — spend ORDER_SCRIPT, create new
  ORDER_SCRIPT output with `parent_ref = spent_input_outref`,
  payout to PAYOUT_SCRIPT.
- **OrderFilledFull** — spend ORDER_SCRIPT, payout to PAYOUT_SCRIPT,
  no new ORDER_SCRIPT output continuing the lineage.
- **OrderCancelled** — spend ORDER_SCRIPT with no payout
  (need a sample to confirm shape).
- **MakerPayoutClaimed** — spend PAYOUT_SCRIPT (need a sample).

Decode-only ingest is straightforward and would slot into the
DEX community modules pattern (`docs/design/DEX_COMMUNITY_MODULES.md`).
Recognition is by script hash, not address-registry lookup, since
all SaturnSwap orders share one script address.

## External resources

- Site: <https://saturnswap.io>
- Docs: <https://docs.saturnswap.io>
- GraphQL: `https://api.saturnswap.io/v1/graphql` — order book / volume / token data
- DefiLlama profile: <https://defillama.com/protocol/saturn-swap>
- DefiLlama TVL adapter: <https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/saturnswap/index.js>
- DefiLlama volume adapter: <https://github.com/DefiLlama/dimension-adapters/blob/master/dexs/saturnswap/index.ts>
- Twitter: <https://x.com/SaturnSwap>
- Org GitHub (no contract source): <https://github.com/Orion-Crypto>
