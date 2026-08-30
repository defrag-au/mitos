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
pub const SCHEMA_VERSION: u32 = 7;

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
    pub provenance: Option<Provenance>,
    /// What must not be separated from the numbers above. Ordered most severe
    /// first so a renderer that shows only the top few still shows the ones
    /// that matter.
    pub caveats: Vec<Caveat>,
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
    /// `null` means NOTHING WAS PUBLISHED — not a target of zero. A renderer
    /// must draw no marker; drawing one at zero asserts a promise nobody made.
    pub advertised_share: Option<f64>,
    pub source: String,
    /// Measured share of the external raise, when a leg maps to this category.
    /// `null` means nothing in this ledger measures it — which is itself worth
    /// showing, and is why the field exists rather than being omitted.
    pub measured_share: Option<f64>,
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
                "ops_team" => {
                    (l.role == "contractor" && !is_marketing(l.function.as_deref()))
                        || l.role == "ops"
                }
                "marketing" => l.role == "contractor" && is_marketing(l.function.as_deref()),
                "founder_pay" => l.role == "founder",
                _ => false,
            })
            .map(|l| l.quantity)
            .sum();
        (sum > 0).then_some(sum)
    };
    let mut commitments = Vec::new();
    let mut stmt =
        conn.prepare("SELECT category, share, source FROM commitment ORDER BY category")?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<f64>>(1)?,
            r.get::<_, String>(2)?,
        ))
    })? {
        let (category, advertised_share, source) = row?;
        commitments.push(Commitment {
            measured_share: measured_for(&category).and_then(share),
            category,
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
    for c in commitments {
        if c.advertised_share.is_some() && c.measured_share.is_none() {
            out.push(Caveat {
                id: "commitment_unmeasured",
                severity: "blocking",
                text: format!(
                    "'{}' was advertised at {:.0}% but NOTHING in this ledger measures it — no \
                     declared wallet maps to that category. Its absence from the charts is a \
                     gap in coverage, not a finding of zero.",
                    c.category,
                    c.advertised_share.unwrap_or(0.0) * 100.0
                ),
            });
        }
    }

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
}
