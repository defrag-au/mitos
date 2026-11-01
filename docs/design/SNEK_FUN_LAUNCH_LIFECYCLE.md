# snek.fun — the launch lifecycle on chain, and how to report it

Established 2026-09-07 by walking four launched tokens ($PERP, $Aliens, $TOONK,
$Dong) plus a sample of live bonding pools, against Koios and snek.fun's own
analytics API. Written because a launchpad launch is a **once-per-token event
that decides the token's entire holder distribution**, and every surface we run
currently renders it as an ordinary transfer to an unnamed script.

Companion to `cnft.dev-workers/docs/design/POLICY_ARCHIVE_OBSERVATIONS.md`,
which defines the observations tier this decodes into, and sibling to
`HOLDER_DISTRIBUTION_MULTI_DEX.md`, which holds the same kind of datum
reference for the DEXes.

Everything below is marked **MEASURED**, **INFERRED** or **UNRESOLVED**. Two
things in here were previously recorded wrongly on the strength of inference,
and both corrections are kept.

---

## Identity — all MEASURED

| thing | value |
|---|---|
| **Bonding curve contract** | `addr1xxg94wrfjcdsjncmsxtj0r87zk69e0jfl28n934sznu95tdj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrs2993lw` |
| payment credential | `905ab869961b094f1b8197278cfe15b45cbe49fa8f32c6b014f85a2d` |
| stake credential | `b2f6abf60ccde92eae1a2f4fdf65f2eaf6208d872c6f0e597cc10b07` |
| **Bonding pool NFT policy** | `63f947b8d9535bc4e4ce6919e3dc056547e8d30ada12f29aa5f826b8` |
| **Graduated pool NFT policy** | `d8eb52caf3289a2880288b23141ce3d2a7025dcf76f26fd5659add06` |
| **Graduated LP token policy** | `6e917b8b965078a39804a6313e5be73535612421acd70aa83f0ec200` |
| Graduation target (newer) | Splash `POOL_CRED_A` = `cb684a69e78907a9796b21fc150a758af5f2805e5ed5d5a8ce9f76f1` |
| Graduation target (older) | Splash `POOL_CRED_B` = `9dee0659686c3ab807895c929e3284c11222affd710b09be690f924d` |

Proof for the contract, which is the entry that was previously wrong: two
independently chosen **live** bonding pool NFTs under `63f947b8…`
(`69915116eacd…` and `2a2bfa018e6d…`) both resolve via Koios `asset_addresses`
to exactly that address. It is not an inference from association.

### ⚠️ CORRECTION — this contract is NOT DexHunter

`shared-crates/address-registry` labels this exact address
`AC::Script(SC::Exchange { label: "DexHunter" })`. **That is wrong.** It is
snek.fun's bonding curve.

The mislabel has an identifiable cause and it is one we already documented:
**the bonding contract and Splash's pool contract share the stake credential
`b2f6abf6…` exactly** (verified by decoding both addresses). Naming a script by
its stake credential is precisely the failure `reference_stake_registry_shared_credentials`
warns about — *"Splash/DexHunter: 14 scripts, one credential"* — and here that
warning is not hypothetical: it produced a wrong entry in our own registry, on
a contract that receives up to 96% of a token's supply at mint.

### ⚠️ `5aa89e79…` IS A DIFFERENT CONTRACT — not a mis-recorded credential

An earlier $Aliens session recorded a credential `5aa89e79…` alongside this
contract. A first pass at this document called that a mis-transcription of
`905ab869…`. **That was wrong too**, and the shape of the error is worth
keeping: two contracts were collapsed into one because they were discussed in
the same breath, and then "corrected" into one by a decoder check that only
ever confirmed the address it was given.

MEASURED on the frozen $Aliens ledger, 2026-09-08 — they are distinct, and both
are real:

| credential | address | holds | what it is |
|---|---|---|---|
| `905ab869…` | `addr1xxg94wrf…` | ~0 at tip | the **bonding curve**; took 96.53% at mint, drained at graduation |
| `5aa89e79…` | `addr1x9d238ne…` | **22,594,540 (2.26%)** | UNIDENTIFIED, and still in the `script — KIND UNKNOWN` band |

The bonding curve holding nothing after graduation is exactly right: its
inventory left in the graduation transaction. `5aa89e79…` acquired its position
some other way and remains a live question — a good first customer for the
policy profile's unnamed-script work.

⇒ **Two contracts named in one sentence is how one of them disappears.** The
decoder round-trip that "confirmed" the merge could not have caught it: it was
asked whether an address decoded to a credential, not whether two credentials
were the same contract.

---

## The launch is ONE transaction

MEASURED on all four tokens. A snek.fun launch mints, seeds the curve and lets
the creator buy, all in the same transaction:

- mints **1,000,000,000** of the token (all four; `mint_cnt` 1, `burn_cnt` 0)
- mints **one bonding pool NFT** under `63f947b8…`, in the *same* transaction
  (`minting_tx_hash` of PERP's pool NFT == the token's own mint tx
  `aa55d489…`)
- creates the curve UTxO at the bonding contract holding the pool NFT, the
  token remainder and the ADA the creator paid
- pays the creator's purchased tokens out to the creator's own wallet

### The creator's share is BOUGHT, not granted

This is the finding that matters most for reporting, and it is the one a
holder chart cannot show. MEASURED:

| token | to curve | to creator | ADA the creator paid into the curve | curve filled by the mint tx |
|---|---|---|---|---|
| $Aliens | 96.53% | 3.47% | 93 ADA | 0.5% of cap |
| $TOONK | 96.16% | 3.84% | 103 ADA | 0.6% |
| $Dong | 79.87% | 20.13% | 463 ADA | 4.2% |
| **$PERP** | **30.00%** | **70.00%** | **9,440 ADA** | **85.9%** |

The creator is identified exactly, not by heuristic: $PERP's recipient address
decodes to payment credential `dd18ca6c…` and stake credential `0c48139f…`,
which are byte-for-byte the `poolAuthor.pkh` and `poolAuthor.skh` snek.fun's
own API reports for that pool.

All four are `launchType: 0`, `version: v1` — **the same product**. The
creator's share is not a platform tier, it is how much the creator chose to buy
from their own curve in the launch transaction, at the cheapest point on it.

$PERP's launch is therefore reportable as a specific thing: **the creator spent
9,440 ADA to take 70% of supply and fill 86% of the bonding cap in the mint
transaction itself**, after which the token graduated 122 seconds later. On any
holder chart that is "a wallet holds 70%".

---

## The bonding phase

The curve UTxO is a two-sided pool in one output — ADA on one side, the token
on the other — so **it fits the observations tier's `(unit, reserve)` pair with
no special-casing**, and its ADA side uses the empty-policy/empty-name
convention the datum itself spells (`Constr 0 [h'', h'']`).

MEASURED, from live pools:

- a fresh curve holds **3,000,000 lovelace and 1,000,000,000 tokens**
- as buys land, lovelace rises and the token side falls
- **progress = (lovelace − 3,000,000) / (cap − 3,000,000)**, both in lovelace

Sampled live pools, showing the convexity plainly:

| pool | lovelace | % of cap | tokens left |
|---|---|---|---|
| Dutch Gold | 119,092,572 | 1.08% | 918,097,552 |
| Chad | 42,081,488 | 0.38% | 969,877,637 |
| RIDDLE | 18,000,001 | 0.16% | 988,034,214 |
| Water Melon | 3,597,792 | 0.03% | 999,521,785 |

Dutch Gold has sold **8.2% of supply for 1.08% of the cap**. That asymmetry is
the whole reason the creator's pre-buy is cheap, and it is why "% of supply
sold" and "% of the raise complete" must never be rendered as the same bar.

### Phase length varies by four orders of magnitude

MEASURED (transactions touching the bonding pool NFT, and the span between the
first and last):

| token | bonding txs | span |
|---|---|---|
| $PERP | 5 | **122 seconds** |
| $Dong | 12 | 2.1 hours |
| $Aliens | 447 | 6.8 days |
| $TOONK | ≥1,000 (Koios page cap) | 173 days |

This is the measurement that settles an earlier wrong conclusion, already
recorded in `reference_snekfun_bonding_is_offchain`: bonding **is** on-chain,
and $Dong looked otherwise only because its phase was degenerately short.
$PERP is shorter still.

---

## Graduation is ONE transaction, and it is unmistakable

MEASURED on all four. The graduation tx spends the curve UTxO and:

1. **burns the bonding pool NFT** (`63f947b8…` in the input, in no output)
2. pays a **flat ~209.58 ADA fee** to the pool's `permittedExecutor`
3. **mints** the graduated pool NFT (`d8eb52ca…`) and LP tokens
   (`6e917b8b…`)
4. creates a **Splash** pool holding the remaining ADA and the remaining tokens

$PERP's, in full:

```
IN   bonding contract   10,991.17 ADA + poolNFT 63f947b8…577933e5 + 261,194,031 PERP
OUT  addr1v8km7vl…         209.58 ADA                                    (executor fee)
OUT  Splash POOL_CRED_A  10,781.17 ADA + poolNFT d8eb52ca…87e3b412
                                       + LP 6e917b8b…41a55e47 (9,223,372,035,176,688,039)
                                       + 261,194,031 PERP
```

Two details worth keeping:

- **The fee is flat, not proportional.** 209.58 ADA on $Aliens (18,188 ADA
  cap), 209.58 on $PERP and $TOONK, 209.62 on $Dong (10,988 ADA cap). Reading
  it as a percentage would put it at 1.9% on one token and 1.2% on another.
- **The fee goes to the pool's own `permittedExecutor`, which differs per
  pool.** $PERP/$TOONK/$Dong → `edbf33f5…`; $Aliens → `e8659419…`. Both were
  confirmed to match the executor the API reports for that pool exactly. It is
  not one platform treasury address, so don't register one.

### 🔑 The threshold is a MARKET CAP target, not an ADA raise

`adaCapThreshold` is the *derived* parameter. What the platform actually targets
is a **market cap**, and the numbers say so out loud. MEASURED —
`adaCapThreshold ÷ tokens left on the curve × total supply`:

| tier | adaCapThreshold | tokens left at graduation | **graduation market cap** |
|---|---|---|---|
| A — $Dong, $PERP (2024-09 → 2025-01) | 10,988,175,000 | 261,194,03x | **42,069.01 ADA** |
| B — $TOONK, $Aliens (2025-09 → 2026-02) | 18,188,400,000 | 263,536,7xx | **69,016.55 ADA** |

**42,069** is 420 and 69. It is exact to 0.00002%, across every token in tier A,
and it is not a number a curve lands on by accident. Tier B is ~69,000. The
threshold is set for the culture and the ADA cap is back-solved from it.

Confirmed beyond the four worked tokens: **12 of 12 further graduated pools
sampled at random from the burned bonding NFTs land on the identical figure**
(one outlier, "Shnek Coin", at 41,441.87). So this is the platform's rule, not a
property of the tokens we happened to pick.

⚠️ **Two different market caps, and a UI must not confuse them.** The 42,069 is
priced off the cap the curve *collected*. The Splash pool that actually results
is priced off the cap **minus the flat executor fee**, so it opens at
**41,276.50 ADA** (tier A) / **68,231.09 ADA** (tier B) — 1.88% lower, which is
the 209.58 ADA fee. Quoting "graduated at 42,069 ADA" beside a pool that prices
the token at 41,276.50 is the kind of discrepancy that reads as a bug in our
numbers rather than as a fee.

### The graduation target has moved

$Dong (2024-09) graduated to Splash **`POOL_CRED_B`** (`9dee0659…`); the three
later tokens graduated to **`POOL_CRED_A`** (`cb684a69…`). This retroactively
explains `POOL_CRED_B`, which an earlier session found "holding $Dong" and
confirmed as Splash structurally without knowing why it existed: **it is the
older snek.fun graduation target.**

The stable marker across both is the **pool NFT policy `d8eb52ca…`**, which is
identical on all four. Match on that, not on the pool address.

---

## The bonding pool datum

MEASURED from a live pool's inline datum (`Constr 0`, exactly 9 fields):

```
Constr 0 [
  0  Constr 0 [ bytes(28) poolNftPolicy, bytes(32) poolNftName ]   -- pool INSTANCE key
  1  Constr 0 [ bytes h'', bytes h'' ]                             -- asset X = ADA
  2  Constr 0 [ bytes(28) tokenPolicy, bytes tokenName ]           -- asset Y
  3  int   aNum
  4  int   bNum
  5  bytes(28)                                                     -- see below
  6  int   adaCapThreshold        (INCLUDES the 3,000,000 seed)
  7  bytes(28)                                                     -- see below
  8  bytes(28)                                                     -- see below
]
```

- Fields 0, 1 and 2 are **MEASURED**: the pool was looked up *by* its NFT, and
  field 2's unit is the token actually present in the UTxO's value.
- Fields 3, 4, 6 are **INFERRED** from magnitude and from matching the API's
  `aNum` / `bNum` / `adaCapThreshold` for the same pools.
- Field 6 **includes the seed**: datum `10,991,175,000` against the API's
  `10,988,175,000` — a difference of exactly 3,000,000, the seed lovelace.
  MEASURED on one pool family; worth re-checking on the 18,188,400,000 family.
  It is also **derived, not chosen** — see the market-cap section: the platform
  picks a target market cap and back-solves the cap. Report the market cap.
  A third cap family exists in the wild (datum `11,003,000,000`, so an API cap
  of `11,000,000,000`); no graduated token from it was sampled, so its target
  market cap is UNMEASURED.
- **Fields 5, 7 and 8 are UNRESOLVED.** Across 10 live pools, field 5 had **one**
  distinct value, field 7 **one**, field 8 **two**. None is the per-pool
  `poolAuthor`, which the API reports as varying per token — so at least two of
  the three are platform roles (executor, fee sink, admin), not the creator.
  The creator's identity is *not* recoverable from this datum on current
  evidence; it is recoverable from the launch transaction's output split.

---

## The curve formula is UNRESOLVED — and must not be guessed

A constant-product curve with a virtual ADA reserve fits the *early* pools
strikingly well. Implied virtual reserve `a0 = Δa·(B0−Δb)/Δb`, over seven live
pools:

| pool | Δa (lovelace) | Δb (tokens) | implied a0 |
|---|---|---|---|
| Water Melon | 597,792 | 478,215 | **1,249,450,826** |
| Your family | 9,032,388 | 7,218,392 | 1,242,269,564 |
| RIDDLE | 15,000,001 | 11,965,786 | 1,238,574,231 |
| Dutch Gold | 116,092,572 | 81,902,448 | 1,301,356,782 |

The smallest trade lands within **0.04%** of `bNum × 1000 = 1,250,000,000`, and
the residual grows with pool activity in the direction a per-trade fee would
push it. That is a good hypothesis.

**It is also wrong at scale, and that is the point.** Running the same fit at
graduation:

- $PERP: 738,805,969 tokens sold for 10,988 ADA ⇒ implied a0 ≈ **3,884 ADA**
- $Aliens: 736,463,246 sold for 18,188 ADA ⇒ implied a0 ≈ **6,508 ADA**

against ~1,250 ADA at the start. **The curve steepens; it is not constant
product.** A linear-price curve was also tested against two constraints and
then falsified on a third pool (predicted 210 ADA against a measured 116).

So: `aNum` and `bNum` parameterise something else, and pricing a bonding pool
with `constant_product_out` would be wrong by ~3× at the top of the curve while
looking plausible at the bottom. Same class of error as reading a Minswap
pool's value instead of its datum — silent, and in the pool's favour.

### The endpoint, at least, is exact

The market-cap finding above turns the graduation end of the curve from a
fitted number into a **hard constraint**, and it is the same for every token in
a tier: at `x = adaCapThreshold` the curve has sold exactly
`supply − (adaCapThreshold × supply ÷ target_mcap)` tokens — 738,805,96x for
tier A, 736,463,2xx for tier B, reproduced to within a handful of base units
across 16 tokens.

Whoever solves the curve therefore has both ends pinned: `a0 ≈ bNum × 1000`
lovelace at `x = 0` (fitted to 0.04% on the smallest observed trade), and an
exact token count at `x = cap`. That is a much stronger starting position than
a two-point fit, and it is worth attempting before reading the validator.

Until it is solved, a bonding pool should contribute **reserves and progress**,
and **withhold a price** — the posture `is_quotable` already takes.

---

## ~98% of launches never graduate

MEASURED 2026-09-08 as a **full census** — every asset under `63f947b8…` paged
out of Koios `policy_asset_info`, live = `total_supply` 1, graduated = burned:

| | |
|---|---|
| bonding-pool NFTs ever minted | **23,335** |
| still bonding | **22,910** |
| burned — graduated or closed | **425** |
| **graduation rate** | **1.82%** |

An earlier three-page sample of 3,000 gave 1.87%, so the sampling held. The
census also gives the platform's shape over time, which the sample could not:

| month | launches | graduated |
|---|---|---|
| 2024-09 | 4,859 | 1.7% |
| 2024-12 | 3,542 | 2.7% |
| 2025-05 | 1,362 | 1.7% |
| 2026-05 | 581 | 0.9% |
| 2026-08 | 297 | 0.3% |

A 4,859-launch opening wave tapering to a few hundred a month, with the
graduation rate peaking near 2.7% in late 2024 and now running under 1%.

⚠️ **snek.fun's own analytics API only answers for tokens that graduated.**
Eight live bonding pools sampled — RIDDLE, SONIC, Chad, Dutch Gold and others —
all return `{"what": "asset=..."}` rather than a pool record. So the off-chain
API carries **survivorship bias by construction**: it is a registry of winners,
not of launches. The chain has all 3,000+. Any "launchpad success rate" taken
from the API would be ~100%.

That is the strongest argument in this document for chain-derived detection
over the API, and it generalises to every launchpad.

---

## What a decoder needs, and where it goes

Per `POLICY_ARCHIVE_OBSERVATIONS.md`, none of this belongs in token-ledger. The
shape is a decode crate plus an observer:

- **A `launchpad` module** — `mitos-dex-decode` is the closest existing home
  (it already carries pool creds, datum decoders and `originator`), though a
  sibling `mitos-launchpad-decode` is defensible since a bonding curve is not
  a DEX. Contents: the credentials above, `is_snek_fun_curve(&cred)`, and
  `decode_bonding_datum` returning the pool instance key, both asset sides and
  the cap.
- **Three lifecycle events, all chain-derivable with no API:**
  - **Launch** — a mint of the token in the same tx as a mint under
    `63f947b8…`. The output split gives the creator's share and the ADA they
    paid, exactly.
  - **Bonding progress** — an observation per curve UTxO: two `(unit, reserve)`
    sides plus `cap`. Price withheld until the curve is solved.
  - **Graduation** — a burn under `63f947b8…` with a mint under `d8eb52ca…` in
    the same tx. Unambiguous, and it carries the executor fee and the pool it
    graduated into.
- **`address-registry`**: correct the `DexHunter` label on
  `addr1xxg94wrf…`, and add `905ab869…` to the *credential* table so the
  contract is findable the way the archive actually stores parties.

The three policy ids are the durable handles. Addresses moved once already
(`POOL_CRED_B` → `POOL_CRED_A`) and executors vary per pool; the mint/burn
policies have not changed across two years and four tokens.

---

## How this lands in token-ledger

### What it exposes, and it is not only an archive problem

1. **~98% of launchpad tokens are permanently invisible to us.** They never
   graduate, so `pools::recognise` never sees a pool it knows, so `spot` stays
   `None` forever and ~96% of supply sits in the cascade's `script — KIND
   UNKNOWN` band. For those tokens token-ledger currently has nothing to say.
2. **For the graduates, the pre-graduation life is blank** — $Aliens 6.8 days,
   $TOONK 173 days of real on-chain trading rendered as "no price".
3. **The cascade has no band for supply nobody has bought yet.**

### 🔑 Realised price needs no curve

The curve formula being unresolved restricts *depth*, not price. Every buy moves
the curve UTxO by `Δada` and `Δtokens` **in one transaction**, so `Δada/Δtokens`
is an observation, not a model.

⇒ spot and last-traded are available through the bonding phase **with the
constants in this document and nothing else**. Only the realisable band needs
the curve solved. This is the same split `PRICE_ORACLE.md` already draws
between last-traded and depth-weighted prices.

### Two modelling decisions, taken with the user 2026-09-07

**1. Unsold bonding inventory gets its OWN cascade band, above the float.**
A new cohort sitting where `burn` sits — deducted from nominal supply to reach
reachable float, not counted within it:

```
nominal supply
− provably unspendable (burn)
− UNSOLD INVENTORY (on the bonding curve)      ← new
= reachable float
    of which pooled / vesting / script / wallets
```

Rejected: counting it as `pool` and therefore float. It is a pool and it is
tradeable against, so LP-in-float superficially applies — but it would make a
token nobody has bought read as ~100% float, and the realisable band would
price selling inventory that was never held. A token that has sold 1% of supply
should read as ~1% float. **The float number has to keep meaning the same thing
it means for a non-launchpad token.**

At graduation the band empties: inventory becomes pool, in one transaction, on a
known slot.

**2. A pool with no issued LP is a distinct, surfaced sub-state.**
MEASURED on $PERP's graduated pool: the LP asset's entire `total_supply`
(9,223,372,035,176,688,039) sits in the pool, and `asset_addresses` returns
exactly one holder — the pool itself. **No wallet holds any LP; the liquidity
cannot be withdrawn by anyone, ever.**

That is a real, verifiable positive signal of a kind usually claimed and never
proved, and it is what the LP decomposition should say when it has nothing to
decompose to. `LP holders: 0` alone reads as missing data.

⚠️ **Do not derive this from the issued-LP base arithmetic.** By the
`base − in_pool` convention the issued amount here computes to 1,678,087,768 —
non-zero — because the contract mints `base − initial_lp` and puts all of it in
the pool, so the initial LP is never minted rather than being minted and held.
The base arithmetic says "1.68e9 issued"; the chain says nobody has any. **The
reliable test is whether any address other than the pool holds the LP token.**

### Order of work

Numbered against `POLICY_ARCHIVE_OBSERVATIONS.md`'s own list; 3 and 4 are where
the launchpad reporting lands, and both sit in the FORWARD pipeline, so results
are visible before any archive work.

0. **Reconcile the transaction count** (archive 14,360 vs forward 35,334 vs
   Koios 35,955). Gate — the fold-semantics explanation is inferred, not tested.
1. **Extract `cohort.rs` + the pure parts of `pools.rs` into a crate.**
2. **Reshape `PoolObservation`** to two `(unit, reserve)` sides, `ada_paired`
   derived, quotability explicit. Provable against existing `stats` output.
3. **The launchpad decoder.** Constants are pinned above, so this is writing,
   not research: `is_snek_fun_curve`, `decode_bonding_datum`, the three
   lifecycle events, the `Inventory` cohort, the locked-pool sub-state, and the
   realised-price series.
4. **Registry fixes** — the DexHunter label, `905ab869…` and the DEX creds into
   `address-registry`'s credential table, the burn sink out of `tokens.toml`.
5. **Observations tier** — `OutputObserver` on `Hooks`, observation file kind,
   three observers (DEX, launchpad, marketplace).
6. **Currency graph and denomination**, path and weakest hop on every quote.
7. **Trade folding and batch fills.**
8. **Solve the curve** — both ends now pinned — then extend the realisable band
   back through bonding.

## Potential later build: walk the launchpad AS a policy

NOT SCHEDULED. Recorded because it is materially cheaper than the shape the
deferred work was scoped against, and the next person to pick this up should
know it exists before designing anything per-token.

Chain detection of unbonded launches was deferred on 2026-09-08 in favour of
API-sourced, graduated-only reporting (see `POLICY_PROFILE.md`). That deferral
was scoped as if provenance had to be established **per token**, at each
token's floor. It does not.

**`63f947b8…` is a policy like any other, so the existing policy-archive
pipeline can ingest it with no new code.** One reverse walk over it yields:

- every launch, as a mint, at a slot;
- every graduation, as a burn, at a slot;
- the complete set — including the **22,910 tokens that never graduated** and
  that the launchpad's own API structurally cannot describe;
- launchpad-health reporting (launches and graduations per month, the table
  above) as a by-product rather than a separate build.

The bonding NFT is a full handle onto the token it launched: its asset name is
the pool instance key, and the pool datum carries the token's unit, the ADA cap
and the curve parameters. So the launchpad's archive is also an index *into*
every token's archive.

This is the same trick the design already leans on elsewhere — the marker is a
mint/burn under a stable policy, and mint/burn is the one thing the archive
records natively in `net_mint`.

⚠️ **The one thing to measure first: unit CARDINALITY, not volume.** Movement
volume is trivial — two movements per NFT, so ~47,000 for the whole platform's
history. But **23,335 distinct `unit_name` values under one policy** is a shape
no walk has ever been run against; every policy walked so far has had one unit
or a handful. `PolicyCoverage.units` is already flagged in
`POLICY_ARCHIVE_AND_SCALE.md`'s backlog as a lower bound from the archive, and
high cardinality is a different stress from high row count — it hits the
manifest's per-pass `units` accounting, the graph's party/unit dictionaries and
the wire's interning, none of which have seen it. Measure before assuming it is
free.

Applies beyond snek.fun: any launchpad that marks its bonding pools with a mint
and its graduations with a burn under one policy is trackable the same way, and
the pattern is likely rather than particular.

## Open

- **The curve formula.** Constraints measured at both ends above; whoever
  solves it has them.
- **Datum fields 5, 7, 8.** Needs a live pool whose token the API also knows —
  which, given the survivorship bias, means catching a pool between graduation
  and the bonding UTxO being spent, or reading the validator.
- **Whether `d8eb52ca…` is snek.fun-specific or Splash-wide.** If specific, it
  is a permanent provenance marker: any Splash pool carrying that NFT policy is
  a graduated launchpad token, forever. Not yet tested against a Splash pool
  known to have no launchpad history.
- **Other launchpads.** The shape here — one mint tx, an NFT marking the
  bonding pool, a burn marking graduation — is likely a pattern rather than a
  snek.fun quirk, but that is an expectation, not a measurement.
