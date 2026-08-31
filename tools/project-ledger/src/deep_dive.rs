//! The deliverable — a project deep dive as ONE self-describing JSON fragment.
//!
//! The tool is an investigation surface; this is the publication surface. A
//! notebook reads this file and charts it (the pattern
//! `chain-forensics/notebook/src/data/*.json.js` already uses: the Rust tool
//! writes an artifact, a Framework loader re-emits it, pages consume it via
//! `FileAttachment`).
//!
//! ## Self-describing, because the reader will not have been in the room
//!
//! Three properties, each of which exists because its absence caused a real
//! error while this case was being worked:
//!
//! 1. **The base travels with the shares.** `external_raise` is in the
//!    fragment and every share is precomputed against it. A consumer that only
//!    received `gross_proceeds` would divide by it and understate every figure
//!    — which is exactly what happened by hand, reading contractor pay as "on
//!    target" when it was over.
//! 2. **Units never merge.** Legs are per unit, and there is no total-value
//!    field for a charting layer to reach for. Converting assets to ADA needs
//!    a price assumption the chain never made.
//! 3. **Caveats are DATA, not prose.** They are generated from the ledger's
//!    actual state and carried in the fragment, so a chart cannot be rendered
//!    without them being available to render too. A figure that travels
//!    without its caveat becomes a claim nobody can defend — and this is
//!    material intended for people who were not part of the investigation.
//!
//! Every figure that can be opened carries its transaction hashes and an
//! explorer URL. A number a reader cannot check is not evidence.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::store::{DistributionBase, DistributionLegRow, Ledger};

/// Bump when a consumer would have to change. The notebook checks it rather
/// than silently charting a shape it does not understand.
///
/// v2 — added `uses`: every ADA that left the project's wallets, by
/// destination, plus what is still held.
/// v3 — added `units_seen` (every unit, so a non-ADA flow cannot be silently
/// absent) and `self_mint` (the team allocation that appears in no table).
/// v4 — added `mint_timeline`: mints per day split public vs project-funded.
/// v5 — added `held_now` per leg: acquired and still-held are different
/// figures and quoting either alone misleads.
/// v6 — added `supply_onward`: where team-minted units went next, and whether
/// that was a sale, compensation in kind, or a gift.
/// v7 — `self_mint` funding is now WINDOWED to match `provenance` (lifetime
/// sums badly misread a busy wallet), and carries `core_share_weighted`.
/// v8 — commitments carry an optional off-chain `counterpart_*`: something
/// claimed to discharge a line that the walk cannot reach. A mining project's
/// largest promise buys machines, and machines are not a UTxO — so the line
/// most worth checking is the one the chain is structurally blind to. Kept
/// beside `measured_share`, never folded into it.
/// v9 — commitments carry `contingent_on`: a promise over money the project
/// does not have yet. Unmeasured then means NOT YET DUE, and reporting it as
/// a coverage gap would accuse a project of missing a promise that has not
/// come due — the mirror of the error v8 guards against.
/// v10 — added `rewards`: what went BACK to holders, measured from the walk
/// via declared `rewards_funding` → `rewards_distribution` parties. Until this
/// existed the fragment measured only what left, and a page wanting the return
/// figure had to cite another document. The same change stops a reward-funding
/// wallet being counted as founder pay or ops spend.
pub const SCHEMA_VERSION: u32 = 12;

fn tx_url(tx: &str) -> String {
    format!("https://cardanoscan.io/transaction/{tx}")
}

/// Which contractor functions are spending against the MARKETING commitment.
///
/// `sponsorship` belongs here, not in ops·tools·team. Paying an athlete to
/// carry the brand is promotion, not work on the product — filing it under ops
/// would overstate one published commitment and understate the other at the
/// same time, which is the worst of both.
///
/// One definition, used by every consumer. Written out per call site, the two
/// halves drift and a function ends up counted twice or not at all.
pub(crate) fn is_marketing(function: Option<&str>) -> bool {
    matches!(function, Some("marketing") | Some("sponsorship"))
}

/// Wallets in the REWARD pipeline: the one that funds distributions and the
/// one that pays them out.
///
/// Money reaching these is on its way BACK to holders, so it is neither a cost
/// nor extraction, and counting it as either inverts the finding. On Mekka S1
/// the funding wallet sat undeclared and fell through to `founder` — putting
/// **15,217 ₳, 68% of measured "founder pay"**, on the founder. The wallet that
/// paid holders every distribution was being reported as the founder taking
/// money out.
pub(crate) fn is_rewards(function: Option<&str>) -> bool {
    matches!(
        function,
        Some("rewards_funding") | Some("rewards_distribution")
    )
}

fn stake_url(key: &str) -> Option<String> {
    key.starts_with("stake1")
        .then(|| format!("https://cardanoscan.io/stakekey/{key}"))
}

#[derive(Debug, Serialize)]
pub struct DeepDive {
    pub schema_version: u32,
    pub generated_unix: u64,
    pub project: ProjectIdentity,
    pub window: Window,
    pub supply: Supply,
    pub base: Base,
    pub commitments: Vec<Commitment>,
    pub uses: Uses,
    /// The team allocation that appears in no allocation table. See
    /// [`SelfMint`]. `None` when the project never minted to itself.
    pub self_mint: Option<SelfMint>,
    /// Mints per day, split public vs project-funded. See [`MintDay`].
    pub mint_timeline: Vec<MintDay>,
    /// Where team-minted supply went after the team took it, and whether it
    /// was paid for. See [`OnwardLeg`].
    pub supply_onward: Vec<OnwardLeg>,
    /// Every unit seen moving through the project's wallets. See [`UnitSeen`].
    pub units_seen: Vec<UnitSeen>,
    pub distributions: Vec<Leg>,
    /// What actually reached holders. `None` when no reward pipeline has been
    /// declared — which means UNMEASURED, never zero. See [`Rewards`].
    pub rewards: Option<Rewards>,
    pub provenance: Option<Provenance>,
    /// What must not be separated from the numbers above. Ordered most severe
    /// first so a renderer that shows only the top few still shows the ones
    /// that matter.
    pub caveats: Vec<Caveat>,
}

/// Money that went BACK to holders, measured from the walk.
///
/// Derived from two declared functions rather than from a wallet list:
/// `rewards_funding` pays `rewards_distribution`, and the sum of those flows is
/// what was distributed. Declaring the ROLE rather than the address means the
/// measure survives the project changing provider, which is the same reason the
/// original analysis found these by CIP-20 tag instead of by wallet.
///
/// This is the counterweight to every extraction figure in the fragment. A
/// report that measures only what left and never what came back is not an
/// accounting, and quoting a per-unit return from another document rather than
/// from the walk invites exactly the "where did that come from" that the rest
/// of this artifact exists to answer.
#[derive(Debug, Serialize)]
pub struct Rewards {
    pub lovelace: i64,
    /// One per distribution — these are batch payments, so this is the number
    /// of DROPS, not the number of holders paid.
    pub distributions: u64,
    pub first_day_unix: Option<i64>,
    pub last_day_unix: Option<i64>,
    /// Divided across every holder-facing unit ever minted. A blunt average:
    /// the project weighted its actual payouts by rarity, so no individual
    /// holder received exactly this. It is the right figure for "what did the
    /// collection return per NFT" and the wrong one for any single asset.
    pub per_unit_lovelace: Option<i64>,
    /// Funder → provider, so a reader can check the pipeline themselves.
    pub funders: Vec<String>,
    pub providers: Vec<String>,
    /// Total seen arriving at named recipients in the payout transactions.
    /// LOWER than `lovelace` — the walk only sees recipients inside its
    /// frontier, so this is a floor on the fan-out, not a second total. The
    /// gap between the two is the coverage.
    pub observed_to_recipients: i64,
    /// Rewards that landed on wallets the project declared. See
    /// [`RewardRecipient`] — this is where a founder holding free-minted
    /// supply shows up as being paid by it.
    pub to_declared: Vec<RewardRecipient>,
}

/// A declared party that received holder rewards.
///
/// Rewards accrue PER ASSET, and the chain does not care what the asset cost.
/// A unit the project minted to itself for nothing earns exactly as much as
/// one a member of the public paid full price for — so a team wallet holding
/// free-minted supply draws an income stream from the distribution it funds.
/// That is a payment to the team by any reasonable reading, and it appears in
/// no allocation table.
#[derive(Debug, Serialize)]
pub struct RewardRecipient {
    pub party: String,
    pub party_url: Option<String>,
    pub label: Option<String>,
    pub role: String,
    /// What they did. Carried beside the role because the role alone loses the
    /// distinction that decides which commitment the spend is measured
    /// against — `sponsorship` and `moderation` are both `contractor`, but the
    /// first is marketing and the second is ops.
    pub function: Option<String>,
    pub lovelace: i64,
    /// How many of the distributions this wallet appeared in. Appearing in all
    /// of them is the difference between holding through the run and having
    /// bought in late.
    pub drops: u64,
    /// Units this wallet holds TODAY that were minted by the project's own
    /// funded wallets — the free supply the income above accrues to.
    pub free_minted_held: i64,
}

#[derive(Debug, Serialize)]
pub struct ProjectIdentity {
    pub name: String,
    pub policy_id: String,
    pub policy_label: String,
    pub policy_url: String,
}

#[derive(Debug, Serialize)]
pub struct Window {
    pub floor_slot: u64,
    pub tip_slot: u64,
    /// `observed` once the walk reconciled every asset the policy minted;
    /// `asserted` otherwise. An asserted floor means the window's start is a
    /// claim, so everything measured inside it inherits that.
    pub floor_basis: String,
    pub note: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Supply {
    /// Holder-facing units — what a person can own.
    pub minted: i64,
    /// CIP-68 reference tokens. Non-zero means the raw asset count runs about
    /// double the NFT count, which is the classic double-count here.
    pub reference_tokens: i64,
    /// What the indexer says the policy minted, live. Higher than `minted`
    /// usually means the collection was still minting past the snapshot.
    pub expected_raw: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct Base {
    pub gross_proceeds: i64,
    pub circular: i64,
    pub external_raise: i64,
    pub circular_txs: u64,
    pub circular_assets: u64,
    pub rule: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Commitment {
    pub category: String,
    /// `mint_funds` or `rewards` — which published breakdown this belongs to.
    /// A consumer MUST filter by it before summing: two breakdowns each
    /// summing to 100% look like one summing to 200%, which breaks the
    /// exhaustiveness argument.
    pub group: String,
    /// `null` means NOTHING WAS PUBLISHED — not a target of zero. A renderer
    /// must draw no marker; drawing one at zero asserts a promise nobody made.
    pub advertised_share: Option<f64>,
    pub source: String,
    /// Measured share of the external raise, when a leg maps to this category.
    /// `null` means nothing in this ledger measures it — which is itself worth
    /// showing, and is why the field exists rather than being omitted.
    pub measured_share: Option<f64>,
    /// An off-chain claim against this line, in lovelace. NEVER merge this
    /// into `measured_share`: that field is reproducible from the walk and
    /// this one rests on someone's word. A renderer must show which is which.
    pub counterpart_lovelace: Option<i64>,
    /// Same value as a share of the external raise, so it sits on the same
    /// axis as `advertised_share` and can be read against it directly.
    pub counterpart_share: Option<f64>,
    /// `asserted` | `document` | `observed`. Present whenever a counterpart
    /// is, and the reason a reader can discount it appropriately.
    pub counterpart_basis: Option<String>,
    /// Includes any currency conversion, because a USD figure compared against
    /// an ADA pledge is only as good as the rate and the date behind it.
    pub counterpart_source: Option<String>,
    /// The event that must occur before this line can be spent against. When
    /// set, "unmeasured" means NOT YET DUE rather than not found, and a
    /// renderer must not show it as a shortfall.
    pub contingent_on: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct Leg {
    pub party: String,
    pub party_url: Option<String>,
    pub label: Option<String>,
    pub role: String,
    pub function: Option<String>,
    /// `lovelace` or `asset`. NEVER add across these.
    pub unit: String,
    pub quantity: i64,
    pub transactions: u64,
    /// Assets acquired for no consideration above the min-UTxO carrier floor.
    /// Zero on lovelace legs, where it is meaningless.
    pub unpaid_units: u64,
    /// Units STILL HELD, against `quantity` which is what was ACQUIRED.
    ///
    /// Both are published because each alone misleads in a different
    /// direction. `$jprigs33` acquired 105 and holds 79: quoting 105 as a
    /// holding overstates the current position by a third, and quoting 79 as
    /// the take hides that 26 were received free and then passed on.
    pub held_now: Option<i64>,
    /// Precomputed so no consumer picks its own denominator. `null` on asset
    /// legs — there is no honest share of a money raise for a thing that is
    /// not money.
    pub share_of_external_raise: Option<f64>,
    pub basis: String,
    pub evidence: Vec<Evidence>,
}

#[derive(Debug, Serialize)]
pub struct Evidence {
    pub tx_hash: String,
    pub tx_url: String,
    pub quantity: i64,
    /// What the receiving party paid in the same transaction, above the
    /// carrier floor. `0` on an asset leg is the finding, not a gap.
    pub consideration: i64,
    pub slot: u64,
}

#[derive(Debug, Serialize)]
pub struct Provenance {
    pub total_minted: i64,
    pub effective_team: i64,
    pub effective_team_share: f64,
    pub holders_examined: i64,
    pub flagged: i64,
    pub threshold: f64,
    pub window_days: i64,
    /// `asserted` (a human named the core roots) or `derived` (the tool
    /// inferred one arithmetically). This grades every figure above it.
    pub roots_basis: String,
}

/// Where the money went, as slices that SUM TO EVERYTHING.
///
/// Internal transfers between the project's own wallets are excluded — moving
/// money from the treasury to an ops wallet is not a use of it, and counting it
/// would let a project inflate its own spending by shuffling.
///
/// The `unattributed` slice is the honest one and is usually the largest. It is
/// not a finding of misuse: it is money whose destination carries no declared
/// identity, so the tool cannot say what it was for. Publishing a chart without
/// it would imply the attributed slices are the whole picture.
#[derive(Debug, Serialize)]
pub struct Uses {
    /// External outflow + what is still held. The pie's denominator.
    pub total: i64,
    pub still_held: i64,
    pub slices: Vec<UseSlice>,
}

#[derive(Debug, Serialize)]
pub struct UseSlice {
    pub category: String,
    pub lovelace: i64,
    pub share: f64,
    /// False for `unattributed` and `still held` — a renderer should mark them
    /// differently, because neither is a statement about purpose.
    pub attributed: bool,
}

/// The self-mint, stated as the thing it actually is: a team allocation that
/// appears in no allocation table.
///
/// This exists because the mechanism is genuinely hard to see. Each individual
/// fact is unremarkable — a project can fund a wallet, a wallet can mint, mint
/// proceeds can arrive — and none of them looks like an allocation. The effect
/// only appears when they are put in a row:
///
/// 1. The project sends money to a wallet.
/// 2. That wallet mints, and the money returns as "mint proceeds".
/// 3. The units stay with the wallet.
///
/// Net: the units moved to the team, the money did not move at all, and the
/// mint books record a sale. The comparison fields exist so a reader can see
/// the mint AS IT APPEARS beside the mint AS MEASURED, which is the only
/// framing where the distortion is obvious.
#[derive(Debug, Serialize)]
pub struct SelfMint {
    pub units: i64,
    pub share_of_supply: f64,
    /// Units that went to buyers outside the project.
    ///
    /// **Do NOT try to refine this by asking whether the MINT TRANSACTION paid
    /// the project.** A batching provider settles to the treasury on its own
    /// schedule, separately from the transactions that deliver the tokens, so
    /// a paid buyer's units land in both a settling and a non-settling tx. That
    /// test reported 472 of Mekka S2's 1,142 units as "given away" and moved
    /// the price from 53.8 ₳ to 102.4 ₳ — both wrong. **1,139 of 1,142 units
    /// were minted by wallets that paid the provider**; the other 3 went to the
    /// provider's own settlement wallet.
    ///
    /// The same batching also inflates a naive per-tx price: a settling tx
    /// carries ADA covering units minted in OTHER transactions, so
    /// `treasury ÷ units in that tx` reads ~103 ₳ against a true ~60 ₳.
    pub public_units: i64,
    /// What the mint transactions total — the figure a reader would otherwise
    /// take as "raised".
    pub apparent_raise: i64,
    pub actual_raise: i64,
    /// Money the project sent to the wallets that minted to themselves,
    /// WITHIN the funding window before each wallet's first mint.
    ///
    /// Windowed, not lifetime. A first version summed all inbound ever and
    /// produced a badly wrong answer on a busy wallet: Mekka S1's largest
    /// self-minter also takes marketplace proceeds and sale income, so lifetime
    /// inbound counted 71,614 ₳ of unrelated money as "their own funds" and put
    /// the mint at 66% project-funded. `provenance`, measuring over the window,
    /// put the same wallets at 94%. Two numbers on one page disagreeing about
    /// the same thing is worse than either being slightly off.
    pub project_funding: i64,
    /// What those wallets brought from anywhere else in the same window. When
    /// this is ~0, the team acquired supply without putting up money of its own.
    pub outside_funding: i64,
    /// `provenance`'s asset-weighted core-funded share across the flagged
    /// holders — the authoritative figure, since it propagates coreness through
    /// intermediaries rather than looking only one hop back.
    ///
    /// Published alongside the raw ADA so a reader can see they agree. If they
    /// ever diverge sharply, the windowing is wrong, not the trace.
    pub core_share_weighted: Option<f64>,
}

/// Where team-minted supply went AFTER the team took it.
///
/// The direct distribution legs stop at the first hop: supply leaving a
/// project wallet. But a founder holding units acquired for nothing can spend
/// them, and when the recipient is a declared contractor that is compensation —
/// paid in units instead of ADA, and invisible to every ADA figure on the page.
/// Measured on Mekka S2: `$ariknfts`, a declared dev, received 5 units from the
/// founder and paid nothing for them.
///
/// Bounded to senders who are project-side, project-funded, or carry a declared
/// identity. Once supply reaches an unnamed third party, what they do with it is
/// theirs, and following further would attribute a stranger's trade to the
/// project.
#[derive(Debug, Serialize)]
pub struct OnwardLeg {
    pub recipient: String,
    pub label: Option<String>,
    /// The recipient's declared identity, when they have one. This is what
    /// separates compensation from a giveaway.
    pub declared_role: Option<String>,
    pub units: i64,
    /// Lovelace the recipient paid in the same transactions.
    pub consideration: i64,
    /// What these specific units cost to mint, summed per asset.
    ///
    /// Per ASSET, not a median: S2's mint ranged 2.7–158.6 ₳ per unit, so an
    /// average would misvalue any individual transfer badly. Each unit is
    /// costed at its own mint transaction's payment divided by the units that
    /// transaction minted.
    ///
    /// This is an OBSERVED price for that unit — what was actually paid to
    /// bring it into existence — not a market valuation. It says what the
    /// project gave up, not what the recipient could sell it for.
    pub value_at_mint: i64,
    /// `sale` — they paid. `compensation` — no payment, and they hold a
    /// declared role. `gift` — no payment, no declared role.
    ///
    /// `gift` deliberately does NOT distinguish a community prize from an
    /// undisclosed payment: both look identical on chain, and guessing which
    /// would be inventing a motive.
    pub kind: &'static str,
}

/// Mints per day, split by who received them.
///
/// A mint is the one event in a collection's life that is unambiguously
/// public: a counter goes up and everyone can see it. Splitting that counter by
/// WHO minted is the difference between "the collection is selling" and "the
/// collection's own wallets are minting", and no aggregator makes that
/// distinction.
///
/// Days with no mints at all are omitted rather than zero-filled; a renderer
/// should treat the axis as time, not as an index, or a two-week gap will
/// render as a single step.
#[derive(Debug, Serialize)]
pub struct MintDay {
    /// Midnight UTC of the day, unix seconds.
    pub day_unix: i64,
    pub public: i64,
    /// Minted to a wallet the project owns or funded.
    pub team: i64,
}

/// EVERY unit that moved through the project's own wallets, ADA included.
///
/// Exists so a non-ADA flow can never be silently absent. The money sections
/// are ADA-denominated; without this, a project that paid its team in USDM
/// would render as having paid nobody, and the page would look complete while
/// being wrong. Listing the units makes the omission visible even where the
/// tool cannot yet fold them into a share.
#[derive(Debug, Serialize)]
pub struct UnitSeen {
    pub unit: String,
    pub ticker: String,
    /// True when this is money by `chain_ledger::tokens::is_settlement_unit` —
    /// a sourced list, not a guess at what looks fungible.
    pub settlement: bool,
    pub legs: i64,
    /// Raw on-chain quantity. NOT decimal-adjusted: the decimals belong to the
    /// token registry, and applying a guessed exponent to a stablecoin is how a
    /// figure lands six orders of magnitude out.
    pub gross_raw: i64,
}

#[derive(Debug, Serialize)]
pub struct Caveat {
    pub id: &'static str,
    /// `blocking` — do not publish a figure this touches without stating it.
    /// `material` — changes how a number should be read.
    /// `context` — worth knowing.
    pub severity: &'static str,
    pub text: String,
}

/// Build the fragment from a ledger that has had `distributions` run.
pub fn build(
    ledger: &Ledger,
    base: &DistributionBase,
    legs: &[DistributionLegRow],
    carrier_floor: i64,
) -> Result<DeepDive> {
    let conn = ledger.conn();
    let meta = |k: &str| -> Option<String> {
        conn.query_row("SELECT v FROM walk_meta WHERE k = ?", [k], |r| r.get(0))
            .ok()
    };

    let policy_id = meta("policy_id").unwrap_or_default();
    let raise = base.external_raise;
    let share = |q: i64| (raise > 0).then(|| q as f64 / raise as f64);

    // ── labels + evidence, looked up once ──────────────────────────────────
    let mut labels: BTreeMap<String, String> = BTreeMap::new();
    let mut stmt = conn.prepare("SELECT key, label FROM party WHERE label IS NOT NULL")?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (k, l) = row?;
        labels.insert(k, l);
    }

    let mut ev_stmt = conn.prepare(
        "SELECT tx_hash, quantity, consideration, slot FROM distribution_evidence
         WHERE party = ?1 AND unit = ?2 ORDER BY slot",
    )?;

    let mut distributions = Vec::new();
    for l in legs {
        let evidence = ev_stmt
            .query_map(rusqlite::params![l.party, l.unit], |r| {
                let tx: String = r.get(0)?;
                Ok(Evidence {
                    tx_url: tx_url(&tx),
                    tx_hash: tx,
                    quantity: r.get(1)?,
                    consideration: r.get(2)?,
                    slot: r.get::<_, i64>(3)?.max(0) as u64,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        distributions.push(Leg {
            party_url: stake_url(&l.party),
            label: labels.get(&l.party).cloned(),
            party: l.party.clone(),
            role: l.role.clone(),
            function: l.function.clone(),
            share_of_external_raise: (l.unit == "lovelace").then(|| share(l.quantity)).flatten(),
            unit: l.unit.clone(),
            quantity: l.quantity,
            transactions: l.legs,
            unpaid_units: l.unpaid_units,
            held_now: l.held_now,
            basis: l.basis.clone(),
            evidence,
        });
    }

    // ── commitments, each with what actually measures it ───────────────────
    //
    // The mapping is explicit rather than derived from role names, because a
    // published category and an internal role are different vocabularies and
    // pretending otherwise is how marketing pay ends up measured against the
    // ops budget.
    let measured_for = |category: &str| -> Option<i64> {
        let sum: i64 = legs
            .iter()
            .filter(|l| l.unit == "lovelace")
            .filter(|l| match category {
                // `ops` counts here too. A project OPERATING wallet that sits
                // outside the value boundary — `$pervsn`, which the project
                // published as its Development wallet — produces distribution
                // legs, and money sent to it is ops spend. Project-side wallets
                // never reach this point: internal transfers generate no leg.
                // `is_rewards` is excluded from BOTH spending categories. A
                // wallet that funds holder distributions is not ops spend and
                // not marketing — the money is going back to the people who
                // paid it in, which no mint-funds line describes.
                "ops_team" => {
                    !is_rewards(l.function.as_deref())
                        && ((l.role == "contractor" && !is_marketing(l.function.as_deref()))
                            || l.role == "ops")
                }
                "marketing" => l.role == "contractor" && is_marketing(l.function.as_deref()),
                "founder_pay" => l.role == "founder" && !is_rewards(l.function.as_deref()),
                _ => false,
            })
            .map(|l| l.quantity)
            .sum();
        (sum > 0).then_some(sum)
    };
    let mut commitments = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT category, share, source, grp,
                counterpart_lovelace, counterpart_basis, counterpart_source, contingent_on
           FROM commitment ORDER BY grp, category",
    )?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<f64>>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<i64>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, Option<String>>(6)?,
            r.get::<_, Option<String>>(7)?,
        ))
    })? {
        let (
            category,
            advertised_share,
            source,
            group,
            counterpart_lovelace,
            counterpart_basis,
            counterpart_source,
            contingent_on,
        ) = row?;
        commitments.push(Commitment {
            contingent_on,
            counterpart_share: counterpart_lovelace.and_then(share),
            counterpart_lovelace,
            counterpart_basis,
            counterpart_source,
            // Only mint-fund lines have a spending counterpart in this ledger.
            // A rewards line is measured against a distribution stream the walk
            // does not model, so it stays null rather than reading as zero.
            measured_share: (group == "mint_funds")
                .then(|| measured_for(&category).and_then(share))
                .flatten(),
            category,
            group,
            advertised_share,
            source,
        });
    }

    // ── provenance ─────────────────────────────────────────────────────────
    let minted_holder_facing: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM asset_event WHERE kind='mint' AND asset_class IN ('nft','ft','rft')",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    // ── rewards: what went BACK to holders ─────────────────────────────────
    //
    // Measured funder → provider. Both ends must be declared: without the
    // provider this would sum every outflow the funding wallet ever made,
    // including its own off-ramps, and report them as money paid to holders.
    let declared_keys = |function: &str| -> Result<Vec<String>> {
        let mut s = conn.prepare("SELECT key FROM party WHERE declared_function = ?1")?;
        let v = s
            .query_map([function], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(v)
    };
    let funders = declared_keys("rewards_funding")?;
    let providers = declared_keys("rewards_distribution")?;
    let rewards = if funders.is_empty() || providers.is_empty() {
        None
    } else {
        let list = |v: &[String]| {
            v.iter()
                .map(|k| format!("'{}'", k.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        };
        let sql = format!(
            "SELECT COALESCE(SUM(quantity), 0), COUNT(DISTINCT tx_hash),
                    MIN(block_time), MAX(block_time)
               FROM unit_flow
              WHERE unit = 'lovelace'
                AND counterparty IN ({})
                AND party IN ({})",
            list(&funders),
            list(&providers)
        );
        let (lovelace, distributions, first, last) = conn.query_row(&sql, [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<i64>>(3)?,
            ))
        })?;
        // The PAYOUT transactions, identified by fan-out rather than by a hash
        // list. A distribution pays every eligible holder at once; the same
        // provider's other traffic — mint deliveries — pays exactly one party
        // at a time. Twenty separates the two by an order of magnitude and
        // needs no hard-coded hashes, so it survives the next distribution.
        let drop_txs = format!(
            "SELECT tx_hash FROM unit_flow
              WHERE unit = 'lovelace' AND quantity > 0 AND counterparty IN ({})
              GROUP BY tx_hash HAVING COUNT(DISTINCT party) >= 20",
            list(&providers)
        );
        let observed_to_recipients: i64 = conn
            .query_row(
                &format!(
                    "SELECT COALESCE(SUM(quantity), 0) FROM unit_flow
                      WHERE unit = 'lovelace' AND quantity > 0 AND tx_hash IN ({drop_txs})"
                ),
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let mut to_declared = Vec::new();
        let recipients_sql = format!(
            "SELECT u.party, p.declared_role, p.label, p.declared_function,
                    SUM(u.quantity), COUNT(DISTINCT u.tx_hash),
                    (SELECT COUNT(*) FROM asset_event e
                       JOIN asset_holder h ON h.asset_name = e.asset_name
                      WHERE e.kind = 'mint' AND e.asset_class = 'nft'
                        AND h.party = u.party
                        AND (e.to_party IN (SELECT key FROM party WHERE project_side = 1)
                          OR e.to_party IN (SELECT holder FROM provenance_verdict WHERE flagged = 1)))
               FROM unit_flow u JOIN party p ON p.key = u.party
              WHERE u.unit = 'lovelace' AND u.quantity > 0
                AND u.tx_hash IN ({drop_txs})
                AND p.declared_role IS NOT NULL
              GROUP BY u.party ORDER BY SUM(u.quantity) DESC"
        );
        let mut s = conn.prepare(&recipients_sql)?;
        for row in s.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })? {
            let (party, role, label, function, lovelace, drops, free_minted_held) = row?;
            to_declared.push(RewardRecipient {
                party_url: stake_url(&party),
                party,
                label,
                role,
                function,
                lovelace,
                drops: drops.max(0) as u64,
                free_minted_held,
            });
        }

        (lovelace > 0).then_some(Rewards {
            lovelace,
            distributions: distributions.max(0) as u64,
            first_day_unix: first,
            last_day_unix: last,
            per_unit_lovelace: (minted_holder_facing > 0).then(|| lovelace / minted_holder_facing),
            funders: funders.clone(),
            providers: providers.clone(),
            observed_to_recipients,
            to_declared,
        })
    };
    let reference_tokens: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM asset_event WHERE kind='mint' AND asset_class='reference'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let provenance = conn
        .query_row(
            "SELECT SUM(assets), SUM(flagged), COUNT(*), MIN(threshold), MIN(window_days),
                    MIN(roots_basis)
             FROM provenance_verdict",
            [],
            |r| {
                Ok((
                    r.get::<_, Option<i64>>(0)?,
                    r.get::<_, Option<i64>>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, Option<f64>>(3)?,
                    r.get::<_, Option<i64>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                ))
            },
        )
        .ok()
        .filter(|(_, _, n, _, _, _)| *n > 0)
        .map(|(_, flagged, n, threshold, window_days, roots_basis)| {
            let team = base.circular_assets as i64;
            Provenance {
                total_minted: minted_holder_facing,
                effective_team: team,
                effective_team_share: match minted_holder_facing > 0 {
                    true => team as f64 / minted_holder_facing as f64,
                    false => 0.0,
                },
                holders_examined: n,
                flagged: flagged.unwrap_or(0),
                threshold: threshold.unwrap_or(0.0),
                window_days: window_days.unwrap_or(0),
                roots_basis: roots_basis.unwrap_or_else(|| "unknown".into()),
            }
        });

    let uses = uses(conn)?;
    let units_seen = units_seen(conn)?;
    let self_mint = self_mint(conn, base, minted_holder_facing);
    let mint_timeline = mint_timeline(conn)?;
    let supply_onward = supply_onward(conn, carrier_floor)?;
    let caveats = caveats(
        conn,
        base,
        legs,
        &commitments,
        &units_seen,
        provenance.as_ref(),
        &meta,
    );

    Ok(DeepDive {
        schema_version: SCHEMA_VERSION,
        generated_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        project: ProjectIdentity {
            name: meta("project").unwrap_or_default(),
            policy_label: meta("policy_label").unwrap_or_default(),
            policy_url: format!("https://cardanoscan.io/tokenPolicy/{policy_id}"),
            policy_id,
        },
        window: Window {
            floor_slot: base.floor_slot,
            tip_slot: base.tip_slot,
            floor_basis: meta("floor_basis").unwrap_or_else(|| "unknown".into()),
            note: "Opens at the policy's first mint and closes at the snapshot tip. \
                   Activity outside it belongs to another window and is not measured here.",
        },
        supply: Supply {
            minted: minted_holder_facing,
            reference_tokens,
            expected_raw: meta("expected_assets").and_then(|v| v.parse().ok()),
        },
        base: Base {
            gross_proceeds: base.gross_proceeds,
            circular: base.circular,
            external_raise: base.external_raise,
            circular_txs: base.circular_txs,
            circular_assets: base.circular_assets,
            rule: "Every share is computed on external_raise. gross_proceeds includes the \
                   portion the project paid itself by funding its own wallets to mint, and \
                   a share computed on it understates every figure.",
        },
        commitments,
        uses,
        self_mint,
        mint_timeline,
        supply_onward,
        units_seen,
        distributions,
        rewards,
        provenance,
        caveats,
    })
}

/// Team-minted supply that moved on, and whether it was paid for.
fn supply_onward(conn: &rusqlite::Connection, carrier_floor: i64) -> Result<Vec<OnwardLeg>> {
    let hf = crate::distributions::holder_facing_sql();
    let mut stmt = conn.prepare(&format!(
        // EXCEPT declared contractors — a paid person minting with their fee is
        // a customer, not a front. See `distributions::run` for why.
        "WITH own AS (
             SELECT k FROM (
                 SELECT key AS k FROM party WHERE project_side = 1
                 UNION SELECT holder FROM provenance_verdict WHERE flagged = 1)
             WHERE k NOT IN (SELECT key FROM party WHERE declared_role = 'contractor')),
         team_minted AS (
             SELECT asset_name FROM asset_event
             WHERE kind = 'mint' AND asset_class IN ({hf}) AND to_party IN (SELECT k FROM own)),
         -- Per-ASSET mint cost: what that transaction paid, divided by the
         -- units it minted. A bulk mint of 24 for 2,500 ADA costs ~104 each;
         -- using a collection-wide average would price a cheap unit as a dear
         -- one and vice versa, and the S2 range is 2.7–158.6.
         mint_cost AS (
             SELECT m.asset_name,
                    COALESCE((SELECT SUM(p.lovelace) FROM mint_payment p
                              WHERE p.tx_hash = m.tx_hash), 0) * 1.0
                    / NULLIF((SELECT COUNT(*) FROM asset_event x
                              WHERE x.tx_hash = m.tx_hash AND x.kind = 'mint'
                                AND x.asset_class IN ({hf})), 0) AS cost
             FROM asset_event m
             WHERE m.kind = 'mint' AND m.asset_class IN ({hf})),
         -- Senders still inside the project's orbit: its wallets, the fronts it
         -- funded, and anyone it has named. Beyond that the supply is a third
         -- party's to trade.
         inside AS (
             SELECT k FROM own
             UNION SELECT key FROM party WHERE declared_role IS NOT NULL)
         SELECT e.to_party,
                (SELECT value FROM party_alias a
                  WHERE a.party = e.to_party AND a.kind = 'handle' LIMIT 1),
                (SELECT declared_role FROM party p WHERE p.key = e.to_party),
                COUNT(*),
                COALESCE((SELECT SUM(-d.delta) FROM tx_delta d
                          WHERE d.party = e.to_party
                            AND d.tx_hash IN (SELECT x.tx_hash FROM asset_event x
                                              WHERE x.to_party = e.to_party
                                                AND x.asset_name IN (SELECT asset_name FROM team_minted))), 0),
                CAST(COALESCE(SUM((SELECT c.cost FROM mint_cost c
                                   WHERE c.asset_name = e.asset_name)), 0) AS INTEGER)
         FROM asset_event e
         WHERE e.kind = 'transfer'
           AND e.asset_name IN (SELECT asset_name FROM team_minted)
           AND e.from_party IN (SELECT k FROM inside)
           AND e.to_party NOT IN (SELECT k FROM own)
           AND e.to_party IS NOT NULL
           -- SCRIPTS ARE NOT RECIPIENTS. `stake17…` is a script stake
           -- credential and `addr1w…` a script payment address, so a transfer
           -- there is a LISTING or a contract interaction, not a gift to a
           -- person. Mekka S1 sent 80 NFTs to jpg.store's script; counting
           -- those as give-aways would turn ordinary listings into the largest
           -- unexplained distribution on the page.
           --
           -- The asset usually comes straight back or is sold, and either way
           -- the marketplace never owned it in any sense a reader means.
           AND e.to_party NOT LIKE 'stake17%'
           AND e.to_party NOT LIKE 'addr1w%'
         GROUP BY e.to_party
         ORDER BY COUNT(*) DESC"
    ))?;
    let rows = stmt
        .query_map([], |r| {
            let declared_role: Option<String> = r.get(2)?;
            let consideration: i64 = r.get(4)?;
            Ok(OnwardLeg {
                recipient: r.get(0)?,
                label: r.get(1)?,
                units: r.get(3)?,
                // A payment above the carrier floor makes it a sale whoever the
                // recipient is; only unpaid transfers are distributions.
                kind: match (consideration > carrier_floor, declared_role.is_some()) {
                    (true, _) => "sale",
                    (false, true) => "compensation",
                    (false, false) => "gift",
                },
                consideration: consideration.max(0),
                value_at_mint: r.get(5)?,
                declared_role,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Mints per day, split public vs project-funded.
fn mint_timeline(conn: &rusqlite::Connection) -> Result<Vec<MintDay>> {
    let hf = crate::distributions::holder_facing_sql();
    let mut stmt = conn.prepare(&format!(
        "WITH own AS (
             SELECT k FROM (
                 SELECT key AS k FROM party WHERE project_side = 1
                 UNION SELECT holder FROM provenance_verdict WHERE flagged = 1)
             WHERE k NOT IN (SELECT key FROM party WHERE declared_role = 'contractor'))
         SELECT CAST(strftime('%s', date(block_time, 'unixepoch')) AS INTEGER) AS day,
                SUM(CASE WHEN to_party IN (SELECT k FROM own) THEN 0 ELSE 1 END),
                SUM(CASE WHEN to_party IN (SELECT k FROM own) THEN 1 ELSE 0 END)
         FROM asset_event
         WHERE kind = 'mint' AND asset_class IN ({hf}) AND to_party IS NOT NULL
         GROUP BY day ORDER BY day"
    ))?;
    let rows = stmt
        .query_map([], |r| {
            Ok(MintDay {
                day_unix: r.get(0)?,
                public: r.get(1)?,
                team: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// How the self-mint was funded, and what the mint looks like with it removed.
fn self_mint(
    conn: &rusqlite::Connection,
    base: &DistributionBase,
    minted: i64,
) -> Option<SelfMint> {
    if base.circular_assets == 0 {
        return None;
    }
    // The FRONTS: wallets `provenance` traced to the project but which the
    // project does not own. Its own wallets are excluded — a treasury minting
    // directly needs no funding leg, and counting the transfer that got the
    // money there would book an internal move as external funding.
    let hf = crate::distributions::holder_facing_sql();
    // Cardano slot ~= 1s, so a day is 86,400 slots — the same conversion
    // `provenance` uses to turn `window_days` into a slot span.
    let funding = |from_project: bool| -> i64 {
        let op = match from_project {
            true => "IN",
            false => "NOT IN",
        };
        conn.query_row(
            &format!(
                "WITH flagged AS (
                     SELECT holder, window_days FROM provenance_verdict WHERE flagged = 1),
                 first_mint AS (
                     SELECT to_party AS holder, MIN(slot) AS slot
                     FROM asset_event
                     WHERE kind = 'mint' AND asset_class IN ({hf}) AND to_party IS NOT NULL
                     GROUP BY to_party)
                 SELECT COALESCE(SUM(v.delta), 0)
                 FROM value_event v
                 JOIN flagged f ON f.holder = v.party
                 JOIN first_mint m ON m.holder = v.party
                 WHERE v.delta > 0
                   AND v.party NOT IN (SELECT key FROM party WHERE project_side = 1)
                   AND v.counterparty {op} (SELECT key FROM party WHERE project_side = 1)
                   AND v.slot BETWEEN m.slot - (f.window_days * 86400) AND m.slot"
            ),
            [],
            |r| r.get(0),
        )
        .unwrap_or(0)
    };
    let core_share_weighted: Option<f64> = conn
        .query_row(
            "SELECT SUM(assets * core_share) / NULLIF(SUM(assets), 0)
             FROM provenance_verdict WHERE flagged = 1",
            [],
            |r| r.get(0),
        )
        .ok()
        .flatten();
    let units = base.circular_assets as i64;
    Some(SelfMint {
        units,
        share_of_supply: match minted > 0 {
            true => units as f64 / minted as f64,
            false => 0.0,
        },
        public_units: (minted - units).max(0),
        apparent_raise: base.gross_proceeds,
        actual_raise: base.external_raise,
        project_funding: funding(true),
        outside_funding: funding(false),
        core_share_weighted,
    })
}

/// Every unit that moved through the project's own wallets.
///
/// Bounded to project-side deliberately: the frontier as a whole touches
/// thousands of units (12,091 on the Mekka S2 walk), and listing those would
/// describe the Cardano token universe rather than this project.
fn units_seen(conn: &rusqlite::Connection) -> Result<Vec<UnitSeen>> {
    let mut stmt = conn.prepare(
        "SELECT unit, COUNT(*), COALESCE(SUM(ABS(quantity)), 0)
         FROM unit_flow
         WHERE party IN (SELECT key FROM party WHERE project_side = 1)
         GROUP BY unit",
    )?;
    let mut out: Vec<UnitSeen> = stmt
        .query_map([], |r| {
            let unit: String = r.get(0)?;
            Ok(UnitSeen {
                ticker: ticker(&unit),
                settlement: chain_ledger::tokens::is_settlement_unit(&unit),
                unit,
                legs: r.get(1)?,
                gross_raw: r.get(2)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // Money first, then by activity. A reader scanning for "was anything other
    // than ADA used to pay people" should meet the answer immediately.
    out.sort_by(|a, b| b.settlement.cmp(&a.settlement).then(b.legs.cmp(&a.legs)));
    Ok(out)
}

/// A readable name for a unit: `ADA`, the ASCII asset name when it decodes
/// (CIP-67 label stripped), else an elided unit string.
fn ticker(unit: &str) -> String {
    if unit == "lovelace" {
        return "ADA".into();
    }
    let name = unit.split_once('.').map(|(_, n)| n).unwrap_or(unit);
    // A CIP-67 label is 8 hex chars of prefix; strip it when present, since
    // `0014df10USDM` is USDM with a label, not a token called `\0\x14ß\x10USDM`.
    let body = match name.len() > 8 && name.starts_with("00") {
        true => &name[8..],
        false => name,
    };
    hex::decode(body)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_graphic()))
        .unwrap_or_else(|| format!("{}…", &unit[..unit.len().min(10)]))
}

/// Every ADA that left the project's wallets, by destination, plus what is
/// still there. Slices sum to `total`.
fn uses(conn: &rusqlite::Connection) -> Result<Uses> {
    // Outflow that crossed the perimeter, grouped by the destination's declared
    // identity. `counterparty NOT IN (project-side)` is what makes an internal
    // transfer invisible here — the treasury topping up an ops wallet has not
    // spent anything, and letting it count would make shuffling look like
    // deployment.
    // "Undeclared" is NOT "unknown", and conflating them was a real error: on
    // Mekka S2 the unattributed slice was 72.6% of outflow, of which 93% went
    // somewhere the investigation had already identified — the self-mint
    // wallets, the relays that carried money to a swap desk, and the minting
    // platform. Charting all of that as unattributed reads as a black hole and
    // overstates what is actually unaccounted for.
    //
    // So before falling back, ask two more questions the ledger can answer.
    let own_money: BTreeSet<String> = {
        let mut s = BTreeSet::new();
        let mut q = conn.prepare(
            "SELECT k FROM (
                 SELECT key AS k FROM party WHERE project_side = 1
                 UNION SELECT holder FROM provenance_verdict WHERE flagged = 1)
             WHERE k NOT IN (SELECT key FROM party WHERE declared_role = 'contractor')",
        )?;
        for k in q.query_map([], |r| r.get::<_, String>(0))? {
            s.insert(k?);
        }
        s
    };
    // Single-use addresses that swept onward, and where they landed. The relay
    // hop is load-bearing: filtering on the immediate counterparty alone makes
    // a fresh bare address look like a final destination, when it is a pipe.
    let relay_target: BTreeMap<String, String> = {
        let mut m = BTreeMap::new();
        let mut q = conn.prepare("SELECT relay_addr, to_addr FROM relay_hop")?;
        for row in q.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (relay, to) = row?;
            m.insert(relay, to);
        }
        m
    };

    // GROUP BY the RAW expressions, never the aliases. Grouping on aliases here
    // silently failed to collapse: one destination class came back as three
    // partial rows, and the founder's outflow was bucketed as unattributed
    // instead of to the founder — a mislabel that moved money out of the
    // category the whole page is about.
    let mut buckets: BTreeMap<String, i64> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT v.counterparty,
                COALESCE(p.declared_role, ''),
                COALESCE(p.declared_function, ''),
                -SUM(v.delta)
         FROM value_event v
         LEFT JOIN party p ON p.key = v.counterparty
         WHERE v.delta < 0
           AND v.party IN (SELECT key FROM party WHERE project_side = 1)
           AND v.counterparty NOT IN (SELECT key FROM party WHERE project_side = 1)
         GROUP BY v.counterparty, COALESCE(p.declared_role, ''),
                  COALESCE(p.declared_function, '')",
    )?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, i64>(3)?,
        ))
    })? {
        let (counterparty, role, function, amount) = row?;
        if amount <= 0 {
            continue;
        }
        // Published categories, not internal role names: a reader compares
        // these against the project's own breakdown, and "contractor/dev" is
        // not a thing the project ever promised a share of.
        let label = match (role.as_str(), function.as_str()) {
            // FIRST, ahead of every role arm, because the funding wallet also
            // looks like ops and whichever arm caught it would file money on
            // its way back to holders as a cost — the one use of funds that is
            // the opposite of a cost, and the only slice here that argues in
            // the project's favour.
            //
            // FUNDING ONLY, never the distributor. On Mekka the airdrop
            // provider is ALSO the minting provider, so money the project
            // sends it is service fees; counting those as rewards inflated
            // this slice by 7,813 ₳ — a third of it — by relabelling a cost as
            // a payout. Rewards are the funder→provider leg, measured
            // separately; this slice is only what the project put IN.
            (_, "rewards_funding") => "returned to holders".to_string(),
            ("contractor", f) if is_marketing(Some(f)) => "marketing".to_string(),
            ("contractor", _) | ("ops", _) => "ops · tools · team".to_string(),
            // Money to a wallet that minted with it. Not unknown at all — it is
            // the self-mint, reported in full in its own section.
            //
            // Matched on own-money membership REGARDLESS of declared role. An
            // earlier version required the role to be blank, so the moment a
            // front was named in the registry its funding stopped reading as
            // self-mint funding and fell through to the role's own name —
            // identifying a wallet made the mechanism it served less visible,
            // which is precisely backwards.
            _ if own_money.contains(&counterparty) => "self-mint funding".to_string(),
            ("", _) if relay_target.contains_key(&counterparty) => {
                // A pipe, not a destination. Named as such so the reader can
                // follow it rather than reading a bare address as an endpoint.
                "swept onward via single-use address".to_string()
            }
            ("", _) => "undeclared destination".to_string(),
            (r, _) => r.to_string(),
        };
        *buckets.entry(label).or_default() += amount;
    }

    // Net across the perimeter: an internal transfer contributes +X and −X and
    // cancels, so this is what the project still holds.
    let still_held: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(delta), 0) FROM tx_delta
             WHERE party IN (SELECT key FROM party WHERE project_side = 1)",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0)
        .max(0);

    let total: i64 = buckets.values().sum::<i64>() + still_held;
    let share = |v: i64| match total > 0 {
        true => v as f64 / total as f64,
        false => 0.0,
    };
    let mut slices: Vec<UseSlice> = buckets
        .into_iter()
        .map(|(category, lovelace)| UseSlice {
            // `attributed` means "we can say what this money was for". A relay
            // hop tells us where money WENT, not what it was for, so it is not
            // attributed — but it is not a mystery either, and the category
            // name says which.
            attributed: !matches!(
                category.as_str(),
                "undeclared destination" | "swept onward via single-use address"
            ),
            share: share(lovelace),
            category,
            lovelace,
        })
        .collect();
    // Attributed first and largest first, so the slice a reader must not skip
    // — the unattributed remainder — sits at the end of the legend where it
    // reads as a remainder rather than as a category.
    slices.sort_by(|a, b| {
        b.attributed
            .cmp(&a.attributed)
            .then(b.lovelace.cmp(&a.lovelace))
    });
    slices.push(UseSlice {
        category: "still held".into(),
        lovelace: still_held,
        share: share(still_held),
        attributed: false,
    });
    Ok(Uses {
        total,
        still_held,
        slices,
    })
}

/// Generate the caveats from what the ledger ACTUALLY says.
///
/// Derived, never a fixed list: a hardcoded disclaimer block is ignored after
/// the second read, and worse, it stays identical when the underlying state
/// changes. These appear only when true, so their presence is information.
#[allow(clippy::too_many_arguments)]
/// What to say about a published commitment the walk cannot measure.
///
/// `None` when the line needs no caveat — either nothing was published, or
/// something in the ledger measures it.
///
/// An off-chain counterpart changes what the silence MEANS, and only that. The
/// line is still unmeasured; it is no longer *unanswered*, so calling it
/// blocking would overstate the gap. It drops to `material` and names what the
/// claim rests on — never presenting the claim as a measurement, which is the
/// error the whole tool exists to avoid.
fn commitment_caveat(c: &Commitment) -> Option<Caveat> {
    let advertised = c.advertised_share?;
    if c.measured_share.is_some() {
        return None;
    }
    let pct = advertised * 100.0;
    // A line whose triggering event has not happened is not a coverage gap —
    // there is nothing yet to cover. Saying otherwise would report a project
    // as having failed a promise that is not due, which is a real unfairness
    // and not a conservative one.
    if let Some(condition) = &c.contingent_on {
        return Some(Caveat {
            id: "commitment_not_yet_due",
            severity: "context",
            text: format!(
                "'{}' was advertised at {pct:.0}% of a pool that DOES NOT EXIST YET — it is \
                 contingent on {condition}. Nothing measures it because nothing has been spent \
                 against it. Recorded so the promise is on file when the money arrives.",
                c.category
            ),
        });
    }
    let (severity, text) = match (&c.counterpart_basis, c.counterpart_share) {
        (Some(basis), Some(share)) => (
            "material",
            format!(
                "'{}' was advertised at {pct:.0}% and NOTHING in this ledger measures it — the \
                 spend leaves the chain before it becomes whatever was promised. A counterpart \
                 worth {:.0}% of the raise is claimed against it — basis '{basis}' — which is not \
                 a measurement and must not be read as one.",
                c.category,
                share * 100.0
            ),
        ),
        _ => (
            "blocking",
            format!(
                "'{}' was advertised at {pct:.0}% but NOTHING in this ledger measures it — no \
                 declared wallet maps to that category. Its absence from the charts is a gap in \
                 coverage, not a finding of zero.",
                c.category
            ),
        ),
    };
    Some(Caveat {
        id: "commitment_unmeasured",
        severity,
        text,
    })
}

fn caveats(
    conn: &rusqlite::Connection,
    base: &DistributionBase,
    legs: &[DistributionLegRow],
    commitments: &[Commitment],
    units_seen: &[UnitSeen],
    provenance: Option<&Provenance>,
    meta: &dyn Fn(&str) -> Option<String>,
) -> Vec<Caveat> {
    let mut out = Vec::new();

    if meta("floor_basis").as_deref() != Some("observed") {
        out.push(Caveat {
            id: "floor_asserted",
            severity: "material",
            text: "The walk did not reconcile every asset the policy minted, so the window's \
                   start is asserted rather than proven. Usually this means the collection was \
                   still minting past the snapshot; it can also mean the floor is wrong."
                .into(),
        });
    }

    if let Some(p) = provenance {
        if p.roots_basis == "derived" {
            out.push(Caveat {
                id: "derived_core_roots",
                severity: "blocking",
                text: "Coreness was anchored on a DERIVED root — the dominant mint-proceeds \
                       destination, treated as the project's by arithmetic rather than named by \
                       anyone. Every team-funded figure inherits that basis."
                    .into(),
            });
        }
    } else {
        out.push(Caveat {
            id: "no_provenance",
            severity: "blocking",
            text: "No provenance pass has run, so self-mint figures count only wallets the \
                   project owns outright and miss any front it funded but does not own."
                .into(),
        });
    }

    // Identity is asserted by an operator; the chain cannot say who anyone is.
    let asserted: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM party WHERE declared_role IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if asserted > 0 {
        out.push(Caveat {
            id: "identities_asserted",
            severity: "material",
            text: format!(
                "{asserted} wallet identities (founder, contractor, treasury) are ASSERTED by \
                 an operator with a recorded source. The chain proves the movements, never who \
                 anyone is."
            ),
        });
    }

    // A published commitment with nothing measuring it is the most dangerous
    // silence in the whole fragment: the chart simply has no bar, and absence
    // reads as zero rather than as unmeasured.
    out.extend(commitments.iter().filter_map(commitment_caveat));

    if legs.iter().any(|l| l.unit == "asset" && l.unpaid_units > 0) {
        out.push(Caveat {
            id: "in_kind_not_valued",
            severity: "material",
            text: "Assets transferred for no consideration are reported as COUNTS and are \
                   deliberately not valued in ADA. Any ADA figure for them would rest on a \
                   price assumption the chain never made, and adding the two units together \
                   would double-count a self-mint whose money returned to the project."
                .into(),
        });
    }

    // The money sections are ADA-denominated. If the project moved another
    // settlement unit, say so — a reader has no way to tell "ADA was all of it"
    // from "ADA was all we charted", and the two are very different claims.
    let other_money: Vec<&UnitSeen> = units_seen
        .iter()
        .filter(|u| u.settlement && u.unit != "lovelace")
        .collect();
    if !other_money.is_empty() {
        out.push(Caveat {
            id: "non_ada_settlement",
            severity: "material",
            text: format!(
                "ADA was not the only money here: the project's wallets also moved {}. \
                 Raw on-chain quantities are in the units table. The ADA shares above do NOT \
                 include {} — the chain never quoted a rate between them, so folding both into \
                 one percentage would invent a price.",
                other_money
                    .iter()
                    .map(|u| format!("{} ({} legs)", u.ticker, u.legs))
                    .collect::<Vec<_>>()
                    .join(", "),
                match other_money.len() {
                    1 => "it",
                    _ => "them",
                }
            ),
        });
    }

    if base.circular > 0 {
        out.push(Caveat {
            id: "circular_excluded",
            severity: "context",
            text: format!(
                "{:.0} ADA of the mint total never came from a buyer: across {} transactions \
                 the project sent money to wallets it controls or funded, those wallets \
                 minted, and the money returned as mint proceeds. It is not counted as income \
                 — but it is not missing either. The {} units it bought are counted as supply \
                 the team took, because a founder minting with the project's money is the team \
                 allocation being spent in units rather than in ADA.",
                base.circular as f64 / 1e6,
                base.circular_txs,
                base.circular_assets
            ),
        });
    }

    let unresolved: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM value_event WHERE unresolved_inputs > 0",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if unresolved > 0 {
        out.push(Caveat {
            id: "unresolved_inputs",
            severity: "context",
            text: format!(
                "{unresolved} value rows were booked with at least one unresolved input, so \
                 their attribution is a floor rather than a settled figure."
            ),
        });
    }

    let rank = |s: &str| match s {
        "blocking" => 0,
        "material" => 1,
        _ => 2,
    };
    out.sort_by_key(|c| rank(c.severity));
    out
}

/// Write the fragment, and read it back to prove what actually landed.
pub fn write(dive: &DeepDive, path: &Path) -> Result<()> {
    let text = serde_json::to_string_pretty(dive).context("serialising deep dive")?;
    std::fs::write(path, &text).with_context(|| format!("writing {}", path.display()))?;
    // An artifact nobody checked is the failure mode this whole tool refuses
    // elsewhere; a truncated write is silent otherwise.
    let back: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(path).with_context(|| format!("re-reading {}", path.display()))?,
    )
    .context("re-parsing the fragment just written")?;
    anyhow::ensure!(
        back.get("schema_version").and_then(|v| v.as_u64()) == Some(u64::from(SCHEMA_VERSION)),
        "the fragment written to {} did not read back with schema_version {SCHEMA_VERSION}",
        path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stake_key_links_to_the_explorer_and_a_bare_address_does_not() {
        assert_eq!(
            stake_url("stake1abc").as_deref(),
            Some("https://cardanoscan.io/stakekey/stake1abc")
        );
        // Bare/enterprise addresses are not stake keys; a stakekey URL for one
        // 404s, and a dead link in published evidence is worse than none.
        assert_eq!(stake_url("addr1v9xyz"), None);
    }

    /// Severity ordering is load-bearing: a renderer showing "the top two"
    /// must get the blocking ones.
    #[test]
    fn caveats_sort_blocking_first() {
        let mut v = [
            Caveat {
                id: "c",
                severity: "context",
                text: String::new(),
            },
            Caveat {
                id: "b",
                severity: "blocking",
                text: String::new(),
            },
            Caveat {
                id: "m",
                severity: "material",
                text: String::new(),
            },
        ];
        let rank = |s: &str| match s {
            "blocking" => 0,
            "material" => 1,
            _ => 2,
        };
        v.sort_by_key(|c| rank(c.severity));
        assert_eq!(v.iter().map(|c| c.id).collect::<Vec<_>>(), ["b", "m", "c"]);
    }

    fn commitment(category: &str) -> Commitment {
        Commitment {
            category: category.into(),
            group: "mint_funds".into(),
            advertised_share: Some(0.8),
            source: "infographic".into(),
            measured_share: None,
            counterpart_lovelace: None,
            counterpart_share: None,
            counterpart_basis: None,
            counterpart_source: None,
            contingent_on: None,
        }
    }

    /// The reward pipeline must not be read as a cost. Money reaching the
    /// wallet that funds holder distributions is on its way back OUT to
    /// holders; counting it as founder pay or ops spend inverts the sign of
    /// the finding, and on Mekka S1 it put 68% of "founder pay" on the founder.
    #[test]
    fn reward_pipeline_wallets_are_neither_ops_spend_nor_founder_pay() {
        assert!(is_rewards(Some("rewards_funding")));
        assert!(is_rewards(Some("rewards_distribution")));

        // Everything else must be unaffected — an over-broad rule here would
        // silently erase real spending from both published categories.
        for f in [
            Some("dev"),
            Some("art"),
            Some("moderation"),
            Some("marketing"),
            Some("sponsorship"),
            None,
        ] {
            assert!(!is_rewards(f), "{f:?} must not read as reward plumbing");
        }
        // The two classifiers are disjoint: a function cannot be both promotion
        // and a payout, and overlapping them would double-count.
        assert!(!is_marketing(Some("rewards_funding")));
    }

    /// A project may publish how it will split money it does not yet have.
    /// Reporting that as an unmet commitment accuses it of failing a promise
    /// that is not due — the opposite error to the one this tool guards, and
    /// just as bad.
    #[test]
    fn a_promise_over_money_that_does_not_exist_yet_is_not_a_coverage_gap() {
        let c = commitment_caveat(&Commitment {
            category: "machine_sale_hashpower".into(),
            group: "machine_sale".into(),
            advertised_share: Some(0.75),
            contingent_on: Some("the sale of the 27 miners, not begun".into()),
            ..commitment("machine_sale_hashpower")
        })
        .unwrap();

        assert_eq!(
            c.id, "commitment_not_yet_due",
            "a contingent line is a different finding from an unmeasured one"
        );
        assert_eq!(
            c.severity, "context",
            "blocking would report a failure to deliver on a promise that is not due"
        );
        assert!(
            c.text.contains("the sale of the 27 miners"),
            "the condition is the whole point — it must be shown: {}",
            c.text
        );
        // The contingency must win over the unmeasured path even when the line
        // otherwise looks exactly like a gap.
        assert!(!c.text.contains("gap in coverage"), "{}", c.text);
    }

    /// An off-chain counterpart ANSWERS a commitment without MEASURING it. The
    /// distinction is the point of the field, so the caveat must keep saying
    /// "unmeasured" even while reporting the claim — otherwise a reader takes
    /// someone's recollection for a walk of the chain.
    #[test]
    fn a_counterpart_softens_the_caveat_without_ever_claiming_a_measurement() {
        let bare = commitment_caveat(&commitment("mining_hardware")).unwrap();
        assert_eq!(bare.severity, "blocking");

        let claimed = commitment_caveat(&Commitment {
            counterpart_lovelace: Some(201_568_000_000),
            counterpart_share: Some(0.5515),
            counterpart_basis: Some("asserted".into()),
            counterpart_source: Some("operator recollection".into()),
            ..commitment("mining_hardware")
        })
        .unwrap();

        assert_eq!(
            claimed.severity, "material",
            "a claim against the line is not nothing, so blocking overstates the gap"
        );
        assert!(
            claimed.text.contains("NOTHING in this ledger measures it"),
            "the line is STILL unmeasured — softening severity must not soften that: {}",
            claimed.text
        );
        assert!(
            claimed.text.contains("must not be read as one"),
            "the caveat has to say the counterpart is not a measurement: {}",
            claimed.text
        );
        // 55% claimed against an 80% pledge. Reporting the pledge as answered
        // would hide a quarter of the raise; the caveat must carry the figure
        // so the shortfall is visible at the point the claim is made.
        assert!(claimed.text.contains("55%"), "{}", claimed.text);
    }

    /// The counterpart must never suppress the caveat entirely, and a measured
    /// line must never raise one. Both directions, because getting either
    /// backwards silently changes what a published chart claims.
    #[test]
    fn measured_lines_are_silent_and_claimed_lines_are_not() {
        assert!(
            commitment_caveat(&Commitment {
                measured_share: Some(0.42),
                ..commitment("ops_team")
            })
            .is_none(),
            "a measured commitment needs no caveat"
        );
        assert!(
            commitment_caveat(&Commitment {
                advertised_share: None,
                ..commitment("supply")
            })
            .is_none(),
            "nothing was published, so there is no promise to caveat"
        );
        // A basis with no figure is a half-filled row, not a claim. It must
        // stay blocking rather than reading as answered.
        assert_eq!(
            commitment_caveat(&Commitment {
                counterpart_basis: Some("asserted".into()),
                ..commitment("mining_hardware")
            })
            .unwrap()
            .severity,
            "blocking"
        );
    }
}
