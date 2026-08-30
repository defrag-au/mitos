//! `distributions` — self-mints, contractor pay, founder pay.
//!
//! Three sections, one command, one denominator. Design:
//! `cnft.dev-workers/docs/design/DISTRIBUTIONS_REPORT.md`.
//!
//! ## Why one command
//!
//! The sections share a base, a window and a role set. Computing the base once
//! and citing it from all three is not tidiness — it is the only structural
//! defence against the error that produced it. Measured on Mekka S2
//! (2026-08-30), contractor pay read as **21.7%, apparently on target**
//! against gross mint proceeds and **27.9% — about 40% over** the 20% pledged
//! against the honest base. Three independent subcommands would each derive
//! their own base and the disagreement would be invisible.
//!
//! ## The denominator rule
//!
//! **Mint proceeds are not the raise.** A project that funds its own wallets
//! to mint its own supply books its own money as revenue. Of S2's 68,821 ₳ of
//! treasury proceeds, 15,350 came from mints delivered to wallets the project
//! had funded — the treasury paying itself. `external_raise` removes it, and
//! every share is computed on that.
//!
//! ## What this does NOT do
//!
//! It does not value assets in ADA. Legs are stored per unit and reported per
//! unit, because converting needs a price assumption the chain never made —
//! and an ADA-only total under-reports precisely whoever was paid in kind.
//! `$jprigs33` is the measured case: 12 ₳ of carrier against 105 assets, so a
//! lovelace-only view puts the founder's take at 0.0% of the raise.
//!
//! ## An asset receipt is not a distribution
//!
//! Only assets arriving FROM the project count — project-side wallets, or the
//! fronts `provenance` traced back to them. A contractor who wins an NFT in a
//! raffle has received nothing from the project, and counting it would inflate
//! their pay with other people's gifts.
//!
//! This is not hypothetical: `$aesch` (moderation) holds 4 assets that a
//! sender-blind count read as payment-in-kind. They came from a raffle service
//! (`genovault` / `rafflekwic`) alongside 1.2–1.7 ₳ of ADA *inbound*. The
//! strict test reports the moderator as receiving nothing from S2 funds, which
//! is what the chain actually shows.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::store::{DistributionBase, DistributionEvidenceRow, DistributionLegRow, Ledger};

#[derive(clap::Args, Debug)]
pub struct DistributionsArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// Lovelace at or below which an outflow in an asset-receipt transaction
    /// reads as min-UTxO CARRIER rather than consideration.
    ///
    /// An asset transfer must drag ADA with it — the protocol will not let a
    /// token sit on chain without it — so "did any ADA move?" is not a payment
    /// test. `$jprigs33` shows 11.7 ₳ of outbound across 8 legs that is
    /// entirely carrier, and a naive test reads those as purchases.
    ///
    /// The default sits above a typical NFT-bearing output's floor
    /// (~1.2–1.5 ₳) and below any plausible sale. A genuine sub-2 ₳ purchase
    /// would be misread as a gift; that trade is deliberate, because the
    /// opposite error turns a give-away into a sale and erases the finding.
    #[arg(long, default_value_t = 2_000_000)]
    pub carrier_floor: u64,

    /// How many legs to print per section.
    #[arg(long, default_value_t = 20)]
    pub report: usize,

    /// Also write the project deep dive as a JSON fragment for a notebook.
    ///
    /// The publication surface. Self-describing — it carries the base, the
    /// commitments, per-transaction evidence with explorer links, and the
    /// caveats generated from this ledger's actual state — so a chart cannot
    /// be built from it without the qualifications being available too.
    #[arg(long)]
    pub export: Option<PathBuf>,
}

/// Holder-facing CIP-67 classes — what a person can own. A CIP-68 collection
/// mints two tokens per NFT and counting the reference token double-counts
/// supply.
const HOLDER_FACING: [&str; 3] = ["nft", "ft", "rft"];

fn in_list(items: &BTreeSet<String>) -> String {
    items
        .iter()
        .map(|k| format!("'{}'", k.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",")
}

/// `HOLDER_FACING` as a SQL list, so the classes are declared once. Written
/// out twice, the reference token eventually creeps into one of them and
/// supply silently doubles.
pub(crate) fn holder_facing_sql() -> String {
    HOLDER_FACING
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn run(args: &DistributionsArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;

    let project_side = ledger.project_side_parties()?;
    if project_side.is_empty() {
        anyhow::bail!(
            "distributions: no project-side wallets — nothing can be inside the boundary, \
             so every figure would be zero. Declare the treasury in the registry and re-seed."
        );
    }
    // Wallets whose MINTS were bought with the project's money: those the
    // project owns outright, plus those `provenance` traced back to it.
    //
    // The second set is why `provenance` must have run: a front is
    // core-FUNDED without being core-OWNED, and on S2 two of the three
    // self-minting wallets were exactly that. Using project-side alone would
    // have found 33 of the 148 self-minted assets.
    let core_funded = ledger.core_funded_holders()?;
    if core_funded.is_empty() {
        tracing::warn!(
            "distributions: no provenance verdicts — self-mints will count only wallets the \
             project OWNS, missing any front it merely funded. Run `provenance` first."
        );
    }
    let own_money: BTreeSet<String> = project_side.union(&core_funded).cloned().collect();

    let (base, circular_txs) = compute_base(&ledger, &project_side, &own_money)?;
    let (legs, evidence) = compute_legs(&ledger, &project_side, &own_money, args)?;

    ledger.replace_distributions(&base, &legs, &evidence)?;
    report(&base, &legs, &circular_txs, args);

    if let Some(path) = &args.export {
        // Built AFTER the write, so the fragment is generated from what is
        // actually in the ledger rather than from what we were about to put
        // there. The two should agree; only one of them is what a reader can
        // later re-derive.
        let dive = crate::deep_dive::build(&ledger, &base, &legs, args.carrier_floor as i64)?;
        crate::deep_dive::write(&dive, path)?;
        let blocking = dive
            .caveats
            .iter()
            .filter(|c| c.severity == "blocking")
            .count();
        tracing::info!(
            path = %path.display(),
            schema = crate::deep_dive::SCHEMA_VERSION,
            legs = dive.distributions.len(),
            caveats = dive.caveats.len(),
            blocking,
            "distributions: deep dive written"
        );
        for c in dive.caveats.iter().filter(|c| c.severity == "blocking") {
            tracing::warn!(id = c.id, "deep dive: BLOCKING caveat — {}", c.text);
        }
    }
    Ok(())
}

/// The denominator: gross mint proceeds, how much of it was circular, and the
/// external raise that remains.
fn compute_base(
    ledger: &Ledger,
    project_side: &BTreeSet<String>,
    own_money: &BTreeSet<String>,
) -> Result<(DistributionBase, usize)> {
    let conn = ledger.conn();
    let ps = in_list(project_side);
    let om = in_list(own_money);
    let hf = holder_facing_sql();

    let gross: i64 = conn
        .query_row(
            &format!(
                "SELECT COALESCE(SUM(lovelace), 0) FROM mint_payment WHERE destination IN ({ps})"
            ),
            [],
            |r| r.get(0),
        )
        .context("summing mint proceeds")?;

    // Per mint transaction: what the project was paid, how many holder-facing
    // assets it minted, and how many went to own-money wallets.
    //
    // APPORTIONED, not all-or-nothing. A mint tx that delivered to a front AND
    // to real buyers is partly circular, and booking the whole payment would
    // over-state exactly the way the relay table did before it was fixed. On
    // S2 the split happens to be clean (148 own-money, 0 others) — which is a
    // measurement, not a licence to assume it.
    let mut stmt = conn.prepare(&format!(
        "SELECT e.tx_hash,
                SUM(CASE WHEN e.to_party IN ({om}) THEN 1 ELSE 0 END),
                COUNT(*),
                COALESCE((SELECT SUM(p.lovelace) FROM mint_payment p
                          WHERE p.tx_hash = e.tx_hash AND p.destination IN ({ps})), 0)
         FROM asset_event e
         WHERE e.kind = 'mint'
           AND e.asset_class IN ({hf})
           AND e.to_party IS NOT NULL
         GROUP BY e.tx_hash"
    ))?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
            r.get::<_, i64>(3)?,
        ))
    })?;

    let mut circular: i128 = 0;
    let mut circular_assets: i64 = 0;
    let mut circular_txs = 0usize;
    for row in rows {
        let (_tx, own, total, paid) = row?;
        if own == 0 || total == 0 {
            continue;
        }
        circular_txs += 1;
        circular_assets += own;
        circular += i128::from(paid) * i128::from(own) / i128::from(total);
    }

    let floor_slot: i64 = conn
        .query_row(
            "SELECT COALESCE(CAST(v AS INTEGER), 0) FROM walk_meta WHERE k = 'floor_slot'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    let tip_slot: i64 = conn
        .query_row(
            "SELECT COALESCE(slot, 0) FROM walk_cursor WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let circular = circular.min(i128::from(gross)).max(0) as i64;
    Ok((
        DistributionBase {
            computed_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            floor_slot: floor_slot.max(0) as u64,
            tip_slot: tip_slot.max(0) as u64,
            gross_proceeds: gross,
            circular,
            external_raise: gross - circular,
            circular_txs: circular_txs as u64,
            circular_assets: circular_assets.max(0) as u64,
        },
        circular_txs,
    ))
}

/// One leg per (party, role, unit), plus the transactions behind each.
fn compute_legs(
    ledger: &Ledger,
    project_side: &BTreeSet<String>,
    own_money: &BTreeSet<String>,
    args: &DistributionsArgs,
) -> Result<(Vec<DistributionLegRow>, Vec<DistributionEvidenceRow>)> {
    let conn = ledger.conn();
    let ps = in_list(project_side);
    let om = in_list(own_money);
    let hf = holder_facing_sql();

    // Identities the operator has declared, and which side each sits on. Only
    // parties OUTSIDE the boundary can receive a distribution — a transfer
    // between two project wallets is internal movement, not a payment out.
    let mut ident = conn.prepare(
        "SELECT key, declared_role, declared_function, project_side
         FROM party WHERE declared_role IS NOT NULL",
    )?;
    let identities: Vec<(String, String, Option<String>, bool)> = ident
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)? == 1,
            ))
        })?
        .collect::<std::result::Result<_, _>>()?;

    let mut legs = Vec::new();
    let mut evidence = Vec::new();

    for (key, role, function, inside) in identities {
        if inside {
            continue;
        }

        // ── ADA received from inside the boundary ──────────────────────────
        let mut stmt = conn.prepare(&format!(
            "SELECT tx_hash, SUM(delta), slot FROM value_event
             WHERE party = ?1 AND delta > 0 AND counterparty IN ({ps})
             GROUP BY tx_hash"
        ))?;
        let rows: Vec<(String, i64, i64)> = stmt
            .query_map([&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<std::result::Result<_, _>>()?;
        if !rows.is_empty() {
            let total: i128 = rows.iter().map(|(_, q, _)| i128::from(*q)).sum();
            legs.push(DistributionLegRow {
                party: key.clone(),
                role: role.clone(),
                function: function.clone(),
                unit: "lovelace".into(),
                quantity: total.min(i128::from(i64::MAX)) as i64,
                legs: rows.len() as u64,
                unpaid_units: 0,
                // Money, not units — there is nothing to still be holding.
                held_now: None,
                first_slot: rows
                    .iter()
                    .map(|(_, _, s)| *s)
                    .min()
                    .map(|s| s.max(0) as u64),
                last_slot: rows
                    .iter()
                    .map(|(_, _, s)| *s)
                    .max()
                    .map(|s| s.max(0) as u64),
                basis: "observed".into(),
            });
            for (tx, q, slot) in rows {
                evidence.push(DistributionEvidenceRow {
                    party: key.clone(),
                    unit: "lovelace".into(),
                    tx_hash: tx,
                    quantity: q,
                    consideration: 0,
                    slot: slot.max(0) as u64,
                });
            }
        }

        // ── Settlement TOKENS received from inside the boundary ────────────
        //
        // ADA is not the only money. A project pays in USDM, off-ramps through
        // a stable, and settles in DJED — Mekka's own frontier moves 1.8M USDM
        // across 25 parties — and a lovelace-only view reports every one of
        // those payments as nothing at all.
        //
        // `unit_flow`, not `value_event`: the latter IS lovelace, by column
        // type. What counts as money is delegated to
        // `chain_ledger::tokens::is_settlement_unit`, the same sourced list
        // `provenance` uses, so a token counts because we can say why rather
        // than because it looked fungible.
        //
        // One leg PER UNIT, and no share is computed: converting USDM to ADA
        // needs a rate the chain never quoted.
        let mut stmt = conn.prepare(&format!(
            "SELECT unit, tx_hash, SUM(quantity), MIN(slot)
             FROM unit_flow
             WHERE counterparty = ?1 AND quantity > 0 AND unit != 'lovelace'
               AND party IN ({ps})
             GROUP BY unit, tx_hash"
        ))?;
        let token_rows: Vec<(String, String, i64, i64)> = stmt
            .query_map([&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
            .collect::<std::result::Result<_, _>>()?;
        let mut by_unit: BTreeMap<String, Vec<(String, i64, i64)>> = BTreeMap::new();
        for (unit, tx, qty, slot) in token_rows {
            if !chain_ledger::tokens::is_settlement_unit(&unit) {
                continue;
            }
            by_unit.entry(unit).or_default().push((tx, qty, slot));
        }
        for (unit, rows) in by_unit {
            let total: i128 = rows.iter().map(|(_, q, _)| i128::from(*q)).sum();
            legs.push(DistributionLegRow {
                party: key.clone(),
                role: role.clone(),
                function: function.clone(),
                unit: unit.clone(),
                quantity: total.min(i128::from(i64::MAX)) as i64,
                legs: rows.len() as u64,
                unpaid_units: 0,
                // Money, not units — there is nothing to still be holding.
                held_now: None,
                first_slot: rows
                    .iter()
                    .map(|(_, _, s)| *s)
                    .min()
                    .map(|s| s.max(0) as u64),
                last_slot: rows
                    .iter()
                    .map(|(_, _, s)| *s)
                    .max()
                    .map(|s| s.max(0) as u64),
                basis: "observed".into(),
            });
            for (tx, qty, slot) in rows {
                evidence.push(DistributionEvidenceRow {
                    party: key.clone(),
                    unit: unit.clone(),
                    tx_hash: tx,
                    quantity: qty,
                    consideration: 0,
                    slot: slot.max(0) as u64,
                });
            }
        }

        // ── Assets received from the project's own money ───────────────────
        //
        // `from_party IN own_money` catches a transfer out of a project or
        // front wallet; `kind = 'mint'` catches the party minting directly
        // with money that traced back. Both are supply leaving the project.
        let mut stmt = conn.prepare(&format!(
            "SELECT e.tx_hash, COUNT(*), MIN(e.slot),
                    COALESCE((SELECT -d.delta FROM tx_delta d
                              WHERE d.tx_hash = e.tx_hash AND d.party = ?1), 0)
             FROM asset_event e
             WHERE e.to_party = ?1
               AND e.asset_class IN ({hf})
               AND (e.from_party IN ({om}) OR (e.kind = 'mint' AND ?1 IN ({om})))
             GROUP BY e.tx_hash"
        ))?;
        let rows: Vec<(String, i64, i64, i64)> = stmt
            .query_map([&key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
            .collect::<std::result::Result<_, _>>()?;
        if !rows.is_empty() {
            let total: i64 = rows.iter().map(|(_, n, _, _)| *n).sum();
            // Consideration is the party's NET outflow in the same
            // transaction, above the carrier floor. Net, because gross would
            // count their own change back as payment.
            let unpaid: i64 = rows
                .iter()
                .filter(|(_, _, _, paid)| *paid <= args.carrier_floor as i64)
                .map(|(_, n, _, _)| *n)
                .sum();
            // What they still have, which is NOT what they took. `$jprigs33`
            // received 105 and holds 79, having passed 26 on. Carrying only
            // the acquired figure overstates a current position; carrying only
            // holdings hides a give-away that was later sold. Both, always.
            let held_now: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM asset_holder WHERE party = ?1",
                    [&key],
                    |r| r.get(0),
                )
                .unwrap_or(0);
            legs.push(DistributionLegRow {
                party: key.clone(),
                role: role.clone(),
                function: function.clone(),
                unit: "asset".into(),
                quantity: total,
                legs: rows.len() as u64,
                unpaid_units: unpaid.max(0) as u64,
                held_now: Some(held_now),
                first_slot: rows
                    .iter()
                    .map(|(_, _, s, _)| *s)
                    .min()
                    .map(|s| s.max(0) as u64),
                last_slot: rows
                    .iter()
                    .map(|(_, _, s, _)| *s)
                    .max()
                    .map(|s| s.max(0) as u64),
                basis: "observed".into(),
            });
            for (tx, n, slot, paid) in rows {
                evidence.push(DistributionEvidenceRow {
                    party: key.clone(),
                    unit: "asset".into(),
                    tx_hash: tx,
                    quantity: n,
                    consideration: paid.max(0),
                    slot: slot.max(0) as u64,
                });
            }
        }
    }
    Ok((legs, evidence))
}

fn ada(l: i64) -> String {
    format!("{:.0}", l as f64 / 1e6)
}

fn report(
    base: &DistributionBase,
    legs: &[DistributionLegRow],
    circular_txs: &usize,
    args: &DistributionsArgs,
) {
    tracing::info!(
        gross_ada = ada(base.gross_proceeds),
        circular_ada = ada(base.circular),
        external_raise_ada = ada(base.external_raise),
        circular_txs = circular_txs,
        circular_assets = base.circular_assets,
        "distributions: THE BASE. Shares are computed on the EXTERNAL RAISE, never on \
         gross — the circular portion is the project's own money returning as its own \
         revenue, and counting it understates every share."
    );
    if base.external_raise <= 0 {
        tracing::warn!("distributions: external raise is zero — no shares can be computed");
        return;
    }

    // Per role, per unit. Units are reported separately and never summed:
    // there is no honest exchange rate between an NFT and an ADA here.
    let mut by_role: BTreeMap<(String, String), (i64, u64)> = BTreeMap::new();
    for l in legs {
        let e = by_role.entry((l.role.clone(), l.unit.clone())).or_default();
        e.0 += l.quantity;
        e.1 += l.unpaid_units;
    }
    for ((role, unit), (qty, unpaid)) in &by_role {
        if unit == "lovelace" {
            tracing::info!(
                role = %role,
                ada = ada(*qty),
                share_of_external_raise =
                    format!("{:.1}%", *qty as f64 / base.external_raise as f64 * 100.0),
                "distributions: paid in ADA"
            );
        } else {
            tracing::info!(
                role = %role,
                assets = qty,
                unpaid = unpaid,
                "distributions: received in ASSETS — no ADA equivalent is asserted, and \
                 `unpaid` is the count acquired for no consideration"
            );
        }
    }

    for l in legs.iter().take(args.report) {
        tracing::info!(
            party = %l.party,
            role = %l.role,
            function = l.function.as_deref().unwrap_or("—"),
            unit = %l.unit,
            quantity = if l.unit == "lovelace" { ada(l.quantity) } else { l.quantity.to_string() },
            legs = l.legs,
            unpaid = l.unpaid_units,
            "distributions: leg"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Quoting is the whole risk in a hand-built `IN (…)`: a party key comes
    /// from a registry an operator wrote, and an apostrophe would otherwise
    /// end the literal early.
    #[test]
    fn party_lists_are_quoted_and_escaped() {
        let s: BTreeSet<String> = ["stake1a".to_string(), "it's".to_string()]
            .into_iter()
            .collect();
        assert_eq!(in_list(&s), "'it''s','stake1a'");
    }

    #[test]
    fn holder_facing_classes_exclude_the_cip68_reference_token() {
        assert!(!HOLDER_FACING.contains(&"reference"));
        assert!(HOLDER_FACING.contains(&"nft"));
    }
}
