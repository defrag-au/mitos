# Angel Finance (Levvy) - On-Chain Protocol Reference

## Overview

Angel Finance (branded as Levvy) is a peer-to-peer lending protocol on Cardano. Lenders offer ADA loans, borrowers put up Cardano native tokens (fungible or NFT) as collateral. If the borrower repays principal + interest within the loan duration, the lender claims their ADA back. If the loan expires without repayment, the lender can liquidate and claim the collateral.

## Script Addresses

Three validator address patterns are in use. Each uses PlutusV3 validators deployed as reference scripts.

### V1 Validator (script hash: `e9f1d49defa5d1f3ba0981c3199cccc69b831dd6409857cafce50caf`)

| Type | Address / Prefix | Notes |
|------|-----------------|-------|
| Enterprise | `addr1w85lr4yaa7jarua6pxquxxvuenrfhqca6eqfs472lnjsetcmkrsmp` | No staking credential. Holds reference scripts + loan UTxOs |
| Base (addr1z) | `addr1z85lr4yaa7jarua6pxquxxvuenrfhqca6eqfs472lnjset` + staking | Per-lender staking credential variant |

### V2 Validator (script hash: `4cf4baed35ae008e1a38bc8b1c76c5a6dbb78640d366bb4ca53cf39a`)

| Type | Address / Prefix | Notes |
|------|-----------------|-------|
| Base (addr1z) | `addr1z9x0fwhdxkhqprs68z7gk8rkckndhduxgrfkdw6v55708x` + staking | Per-lender staking credential. Active loans observed here |

### Bech32 Script Credentials

Used with Maestro's `/addresses/cred/{credential}/transactions` endpoint:

| Validator | Bech32 Credential |
|-----------|-------------------|
| V1 | `script1a8caf8005hgl8wsfs8p3n8xvc6dcx8wkgzv90jhuu5x27vu2f8j` |
| V2 | `script1fn6t4mf44cqgux3chj93cak95mdm0pjq6dntkn998nee58s2w8h` |

> **Note**: Maestro's credential endpoint only finds TXs at the V1 credential currently. V2 validator TXs (e.g. `addr1z9x0fwhdxkhqq...`) are discoverable via the full address or by scanning TX inputs/outputs. Loans exist across **both** validators simultaneously.

### Reference Scripts

| TX Hash | Index | Script Hash | Validator |
|---------|-------|-------------|-----------|
| `35ffaab12ab4f60218e1778c4ce0db75923516d1efe2a9b1046a3c11a488b66c` | 0 | See note | V1 spending validator (used in claim TX ref inputs) |
| `3f44dc653460393dad1525e68955767a57137cce1c9e8d40c828c3a955ac728e` | 0 | See note | Auth/staking validator (used in claim TX ref inputs) |
| `192b946fc628f9244625f5df67a858c7cb47a401bc978bccd9a1e9be14201358` | 0 | `ad0e806c...` | Staking validator (used in cancel TX withdraw-zero) |
| `ec792a789e49cf88eac237d70bbcdd28f560763ebd013bb4ee09977e12d7f97f` | 0 | See note | Additional ref script (observed in claim TX) |

### Staking Scripts (Withdraw-Zero Pattern)

Different operations use different staking script hashes for the withdraw-zero authorization:

| Operation | Staking Script Hash | Notes |
|-----------|-------------------|-------|
| Cancel (offer reclaim) | `ad0e806c530324acfe15d0e284a8ca247b1b425f36429e3a03c8b716` | Used in `angel.rs` |
| Claim (repaid loan) | `a32353f22f76b1535ef78e6df793548769e1011ce462669fa2b7a97c` | Observed in Eternl claim TX |

## Loan Lifecycle

```
Lender creates offer     Borrower accepts        Borrower repays         Lender claims ADA
    (redeemer 0)            (redeemer 2)            (redeemer 3?)           (redeemer 3)
  ┌───────────┐          ┌───────────┐          ┌───────────────┐        ┌───────────────┐
  │ Deposit   │          │ Collateral│          │ Borrower sends │        │ Lender spends │
  │ ADA at    │───────►  │ locked,   │───────►  │ principal +    │──────► │ script UTxO,  │
  │ script    │          │ ADA sent  │          │ interest back  │        │ receives ADA  │
  │ (c=0)     │          │ to borrower│         │ to script UTxO │        │               │
  └───────────┘          │ (c=1 or 2)│          │ (ADA replaces  │        └───────────────┘
                         └───────────┘          │  collateral)   │               │
                                                └───────────────┘        Loan expires
                                                                         without repay
                                                                               │
                                                                         ┌─────▼─────────┐
                                                                         │ Lender claims  │
                                                                         │ collateral     │
                                                                         │ tokens         │
                                                                         └───────────────┘
```

### Key Insight: Repayment Changes the UTxO Value

When a borrower repays, they send principal + interest **back to the script UTxO**, replacing the collateral tokens with ADA. The datum constructor stays the same (c=2), but the UTxO value changes dramatically:

| State | UTxO Value | Datum |
|-------|-----------|-------|
| Active (pre-repay) | ~2 ADA + collateral tokens | c=2, fields show collateral details |
| Repaid (pending claim) | principal + interest ADA | c=2, same datum fields |
| After claim | UTxO consumed | — |

**This means**: To determine if a loan has been repaid, compare the UTxO's ADA value against the principal in the datum. If `utxo_lovelace ≈ principal + interest`, the borrower has repaid and the lender just needs to claim.

## Datum Constructors

Inline datums on loan UTxOs. The outer constructor indicates loan type/state, wrapping an inner Constructor 0 with the field payload.

### Constructor 0 — Loan Offers (12 fields)

Lender has deposited ADA, waiting for a borrower.

```
Constr 0 [ Constr 0 [
  [0]  lender_credential    — Constr 0 [ Constr 0 [ Bytes(28) ] ]
  [1]  loan_principal       — Constr 0 [ policy: Bytes, name: Bytes, amount: Int ] (ADA)
  [2]  desired_collateral   — Constr 0 [ policy: Bytes(28), name: Bytes, qty: Int ]
  [3]  interest_amount      — Constr 0 [ policy: Bytes, name: Bytes, amount: Int ] (ADA)
  [4]  duration_ms          — Int  (604800000 = 7d, 1209600000 = 14d)
  [5]  flag_0               — Constr 0/1 []
  [6]  min_utxo_deposit     — varies (sometimes Constr with asset triple, sometimes Constr 1 [])
  [7-11] boolean flags      — Constr 0/1 []
]]
```

### Constructor 1 — Active Fungible-Collateral Loans (12 fields)

Borrower accepted, collateral tokens locked. Field layout shifts — borrower credential inserted at [1].

```
Constr 1 [ Constr 0 [
  [0]  lender_credential
  [1]  borrower_credential  — Constr 0 [ Constr 0 [ Bytes(28) ] ]
  [2]  loan_principal       — asset triple (ADA amount)
  [3]  collateral_locked    — asset triple (token + quantity)
  [4]  interest_amount      — asset triple (ADA amount)
  [5]  duration_ms          — Int
  [6]  acceptance_timestamp  — Int (POSIX ms, e.g. 1774298399000)
  [7]  tx_hash_ref          — Bytes(32)  (reference to acceptance TX?)
  [8-11] boolean flags
]]
```

### Constructor 2 — NFT/Token Collateral Loans (11 fields)

Used for both NFT-backed and fungible token loans. Includes borrower credential (active) or not (offer).

**As an active loan (with borrower):**
```
Constr 2 [ Constr 0 [
  [0]  lender_credential
  [1]  borrower_credential
  [2]  collateral_token     — asset triple (policy + name + qty)
  [3]  loan_principal       — asset triple (ADA amount)
  [4]  interest_amount      — asset triple (ADA amount)
  [5]  tx_hash_ref          — Bytes(32)
  [6-7] boolean flags
  [8]  protocol_credential  — Constr 0 [ Constr 0 [ Bytes(28) ] ]  (e.g. 10b94c19...)
  [9-10] boolean flags
]]
```

**As an offer (no borrower):**
```
Constr 2 [ Constr 0 [
  [0]  lender_credential
  [1]  loan_principal
  [2]  desired_collateral
  [3]  interest_amount
  [4]  duration_ms
  [5-10] flags/metadata
]]
```

### Constructor 4 — Extended Loan State (13 fields)

Additional metadata variant. Includes list fields not seen in other constructors. Observed with NIGHT, STRIKE, CSWAP, USDM collateral.

### Distinguishing Offers from Active Loans

Within constructors 2 and 4, offers and active loans share the same constructor but differ in:
1. **Token presence**: Active loans have collateral tokens locked in the UTxO; offers have only ADA
2. **ADA amount**: Offers hold the principal ADA; active loans hold min-UTxO (~2 ADA) + collateral tokens
3. **Repaid loans**: Hold principal + interest ADA (no collateral tokens) — same constructor as active

**Reliable heuristic for loan state:**
- `has_non_ada_tokens` → Active loan (collateral locked, borrower hasn't repaid)
- `!has_non_ada_tokens && lovelace > principal * 0.9` → Repaid, pending lender claim
- `!has_non_ada_tokens && lovelace ≈ principal` → Open offer (no borrower yet)

## Redeemer Actions

### Spend Redeemers

| Outer Constructor | Action | Description |
|-------------------|--------|-------------|
| `Constr(0, [...])` | **Create / Modify** | Lender creates or modifies a loan offer |
| `Constr(2, [...])` | **Accept** | Borrower accepts offer — deposits collateral, receives ADA |
| `Constr(3, [...])` | **Claim** | Lender claims from a repaid or expired loan |
| `Constr(4, [...])` | **Claim (alt)** | Alternative claim variant (seen in older TXs) |

### Withdrawal Redeemers (Withdraw-Zero)

| Data | Action |
|------|--------|
| `Constr(0, [...])` | Cancel authorization (used with `ad0e806c...` staking script) |
| `2` (integer) | Claim authorization (used with `a32353f2...` staking script) |

## Claim TX Structure (Repaid Loan)

Decoded from Eternl-built claim transaction:

```
INPUTS:
  [0] addr1z9x0fwh...  (V2 script UTxO — the repaid loan, 640 ADA)
  [1] addr1q...         (lender wallet — fee source)

OUTPUTS:
  [0] 1.49 ADA → borrower  (min-UTxO deposit return)
  [1] 639.96 ADA → lender  (principal + interest)
  [2] 407.64 ADA → lender  (change from fee UTxO)

REDEEMERS:
  spend #0: Constr(3, [Constr(0, [0, 1, Constr(0,[0]), Constr(1,[])×7, Constr(0,[]), Constr(1,[]), Constr(0,[])])])
  reward #0: data=2, exunits=(537311 mem, 185586307 steps)

WITHDRAWAL: withdraw-zero at script hash a32353f2...
REQUIRED SIGNER: lender PKH
REFERENCE INPUTS: 3 reference script UTxOs (spending + auth + other)
COLLATERAL: 1 wallet UTxO with collateral return
```

### Key Differences: Cancel vs Claim

| Aspect | Cancel (offer reclaim) | Claim (repaid loan) |
|--------|----------------------|---------------------|
| Spend redeemer | `Constr(0, [...])` | `Constr(3, [...])` |
| Reward redeemer | `Constr(0, [...])` data | `2` (integer) |
| Staking script | `ad0e806c...` | `a32353f2...` |
| Output to borrower | No | Yes (min-UTxO return) |
| Lender receives | Original ADA deposit | Principal + interest |
| Script UTxO held | Lender's ADA (offer) | Principal + interest ADA (repaid) |

## Example Transactions

| Action | TX Hash | Notes |
|--------|---------|-------|
| Offer creation | `b6e3069da670...` | 2000 ADA CSWAP offer at enterprise addr |
| Loan acceptance | `deda5096063a...` | Borrower locks CSWAP, receives ADA. Datum c=0→c=1 |
| Borrower repayment | (TX that produced `3ea26d0d...#2`) | ADA returned to script UTxO, collateral released |
| Lender claim | Eternl CBOR (see above) | Spend c=2 UTxO with redeemer 3, lender gets ADA |
| Offer cancel | `2fcd8487...` | Multi-offer cancel across V1 + V2 addresses |
| Batch claim | `b1f767f6...` | 3 repaid loans claimed in single TX |

## Live Loan Inventory (March 2026)

Enterprise address (`addr1w85lr4...`) holds ~39 UTxOs:
- ~13 reference script UTxOs
- ~26 loan UTxOs with inline datums

### Active NIGHT Loans

| UTxO | Constructor | NIGHT Locked | Principal | Interest | Lender PKH |
|------|------------|-------------|-----------|----------|------------|
| `dbc01b3b...#0` | 2 | 39,408,812 | 5 ADA | 0 ADA | `7e6823d4...` |
| `4df7fe81...#0` | 2 | 41,051,844 | 5 ADA | 0 ADA | `7e6823d4...` |

### NIGHT Offers (Open)

| UTxO | ADA | NIGHT Requested | Lender PKH |
|------|-----|----------------|------------|
| `9deb58e1...#0` | 501 | 5,000,010,888 | `2b85bd7b...` |
| `223563cb...#0` | 500 | 3,826,692,123 | `2b85bd7b...` |
| `360af799...#2` | 1.6 | 5,649,630 | `4e230f37...` |

### Collateral Tokens in Active Use

| Token | Policy ID | Type |
|-------|-----------|------|
| NIGHT | `0691b2fecca1ac4f53cb6dfb00b7013e561d1f34403b957cbb5af1fa` | Fungible (Midnight) |
| CSWAP | `c863ceaa796d5429b526c336ab45016abd636859f331758e67204e5c` | Fungible |
| ANGELS | `8fe8039d057c71fdfb1095e260f153f18a5834d85d9c868ddf7307bc` | CIP-68 |
| USDM | Various | Stablecoin |
| STRIKE | Various | Fungible |
| BANK | `2b28c81dbba6d67e4b5a997c6be1212cba9d60d33f82444ab8b1f218` | Fungible |

## DO Loan State Classification

For the loan-book DO to properly catalog loans, each UTxO should be classified into one of these states:

| State | Detection Logic | Frontend Label |
|-------|----------------|----------------|
| **Offer** | c=0, or (c=2/4 with no tokens and lovelace ≈ principal) | "Open Offer" |
| **Active** | c=1/2/4 with collateral tokens locked, lovelace ≈ min-UTxO | "Active Loan" |
| **Repaid (Pending Claim)** | c=1/2/4, no collateral tokens, lovelace ≈ principal + interest | "Repaid — Claim ADA" |
| **Defaulted (Pending Claim)** | c=1/2/4 with collateral tokens, loan duration expired | "Defaulted — Claim Collateral" |
| **Pending Cancel** | Offer with cancel TX submitted but not yet confirmed | "Cancelling..." |

### Detecting Expiry (Defaulted)

For Constructor 1 loans, the datum includes `accepted_at_ms` (field [6]) and `duration_ms` (field [5]). A loan is expired when:
```
current_time_ms > accepted_at_ms + duration_ms
```

For Constructor 2 loans, there's no explicit timestamp in the datum, but the acceptance TX time can be inferred from the UTxO's creation slot.
