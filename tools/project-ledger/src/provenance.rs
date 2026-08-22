//! `provenance` — how much of the mint was bought with the project's own money.
//!
//! ## The question this answers
//!
//! A project advertises an allocation breakdown: so much to the team, so much
//! public. "Treasury-based minting through dark wallets" is the way that
//! commitment gets quietly exceeded — core money moves to an undisclosed
//! wallet, the wallet mints like any customer, and the supply lands under
//! team control while the mint books read as public demand. This pass measures
//! the EFFECTIVE team-funded supply so it can be compared with the advertised
//! one. The comparison itself stays with the operator: the chain knows who
//! funded a mint, not what was promised on Discord.
//!
//! ## Method
//!
//! 1. Every holder-facing mint acquisition (`asset_event`, CIP-68-aware) by a
//!    non-core wallet, with the holder's spend as context.
//! 2. The holder's INBOUND funding in a window around their minting, by
//!    source.
//! 3. Each source's **coreness**, propagated: an asserted core/founder wallet
//!    or a non-terminal project seed is 1.0 outright; everything else takes
//!    the funding-weighted coreness of ITS funders, for two rounds — so a
//!    dark intermediary (core-funded, asserting nothing) passes what it is
//!    through to whoever it funds. A declared terminal (a CEX funder) is 0.0
//!    by construction: money from outside the chain is the EXONERATING
//!    direction here.
//! 4. Verdict per holder: the funding-share that is core, alongside the share
//!    that is UNATTRIBUTED — the fraction is a floor, and hiding the unknown
//!    share would dress partial coverage as a complete answer.
//!
//! ## What this is, epistemically
//!
//! Derived, not asserted — and unlike an interest score it IS meant to become
//! a figure, so every flagged holder prints its funding legs with transaction
//! hashes. A claim of the form "the founders minted N through fronts" must
//! survive hostile checking or it must not be made.
//!
//! Requires a ledger walked with `--watch-holders` (holders seated from the
//! floor): without their booked inbound legs there is nothing to attribute,
//! and the pass says so rather than reporting zeros.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::Result;
use rusqlite::OptionalExtension;

use crate::store::Ledger;

#[derive(clap::Args, Debug)]
pub struct ProvenanceArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// The app's annotations sidecar (human classifications). Defaults to
    /// `<db>.annotations.db`. Core/founder assertions are the roots of
    /// coreness; without any, only the registry seeds anchor it.
    #[arg(long)]
    pub annotations: Option<PathBuf>,

    /// Days of funding history before a holder's FIRST mint that still count
    /// toward their mint funding. Money from years earlier is savings, not a
    /// front being loaded.
    #[arg(long, default_value_t = 45)]
    pub window_days: u64,

    /// Core-funded fraction at or above which a holder is flagged.
    #[arg(long, default_value_t = 0.5)]
    pub threshold: f64,

    /// Also root coreness on the DERIVED dominant mint-proceeds destination
    /// (`classify` records it in meta). For a cold collection with no human
    /// assertions yet — clearly reported as derived, never silently mixed
    /// with asserted roots.
    #[arg(long)]
    pub derived_roots: bool,

    /// How many flagged holders to print in full (all are counted).
    #[arg(long, default_value_t = 20)]
    pub report: usize,
}

/// One wallet's mint acquisitions and their funding decomposition.
struct HolderVerdict {
    key: String,
    assets: u64,
    /// Net spend across their mint txs (atomic mints) — context, not verdict.
    mint_spend: i128,
    /// Funding-share from the core cluster, 0..1 over ATTRIBUTED inbound —
    /// the MAXIMUM across payment units (see [`headline`]).
    core_share: f64,
    /// Share of windowed inbound with no resolved payer, in the headline
    /// unit — the fraction above is a FLOOR and this is how far it could move.
    unknown_share: f64,
    /// Per-unit decomposition ("ada 12% · USDM 98%") when more than one
    /// payment unit funded the wallet.
    via: Option<String>,
    /// Top funding legs: (display label, example tx).
    legs: Vec<(String, String)>,
}

/// Whether a `unit_flow` unit participates in FUNDING analysis: lovelace, or
/// a CIP-67 labelled fungible (333/444 — USDM is `0014df10` + "USDM").
///
/// Mint payments arrive in ADA or a stablecoin; NFTs riding through a wallet
/// are custody, not funding, and counting them would let one airdropped
/// token rewrite a wallet's money story.
fn is_payment_unit(unit: &str) -> bool {
    if unit == "lovelace" {
        return true;
    }
    match unit.split_once('.') {
        Some((_, name)) => name.starts_with("0014df10") || name.starts_with("001bc280"),
        None => false,
    }
}

/// A human ticker for a payment unit: "ada", or the label-stripped asset name
/// when it decodes as ASCII ("USDM"), else the elided unit.
fn unit_ticker(unit: &str) -> String {
    if unit == "lovelace" {
        return "ada".into();
    }
    let name = unit.split_once('.').map(|(_, n)| n).unwrap_or(unit);
    let body = name.get(8..).unwrap_or("");
    match hex::decode(body)
        .ok()
        .and_then(|b| String::from_utf8(b).ok())
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_graphic()))
    {
        Some(t) => t,
        None => format!("{}…", &unit[..unit.len().min(12)]),
    }
}

/// The verdict across payment units: the MAXIMUM per-unit core share, with
/// its unknown share, and a "via" breakdown when several units funded the
/// wallet.
///
/// Max, not an average: units are incomparable without an invented exchange
/// rate, and the anti-evasion reading is the honest one — a wallet loaded
/// with core USDM is core-funded regardless of how clean its ADA history
/// looks. The per-unit breakdown is printed so the max never hides its basis.
fn headline(per_unit: &[(String, f64, f64)]) -> (f64, f64, Option<String>) {
    let best = per_unit
        .iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .cloned()
        .unwrap_or(("ada".into(), 0.0, 0.0));
    let via = (per_unit.len() > 1).then(|| {
        per_unit
            .iter()
            .map(|(u, c, _)| format!("{u} {:.0}%", c * 100.0))
            .collect::<Vec<_>>()
            .join(" · ")
    });
    (best.1, best.2, via)
}

const SLOTS_PER_DAY: u64 = 86_400;

/// Funding-weighted coreness of a source map: `(core_share, unknown_share)`.
///
/// The empty source key is the walk's "payer unresolved" condition — counted
/// as its own share rather than as zero-coreness, because "40% of this
/// wallet's funding is unattributed" and "40% is attributed to strangers" are
/// different findings and only one of them is closable by `resolve-local`.
fn weighted(sources: &BTreeMap<String, i128>, c: &BTreeMap<String, f64>) -> (f64, f64) {
    let total: i128 = sources.values().sum();
    if total <= 0 {
        return (0.0, 0.0);
    }
    let mut core = 0.0;
    let mut unknown = 0.0;
    for (src, v) in sources {
        let share = *v as f64 / total as f64;
        if src.is_empty() {
            unknown += share;
        } else {
            core += share * c.get(src).copied().unwrap_or(0.0);
        }
    }
    (core, unknown)
}

/// Two pinned Jacobi rounds of coreness over the inbound map — enough to see
/// treasury → intermediary → buyer, and deliberately no further: a third hop
/// is where "funded by the project" starts meaning "participates in the same
/// economy", and the figure must not creep.
fn propagate(
    coreness: &mut BTreeMap<String, f64>,
    inbound: &BTreeMap<String, BTreeMap<String, i128>>,
    pinned_zero: &BTreeSet<String>,
) {
    for _ in 0..2 {
        let prev = coreness.clone();
        for (party, sources) in inbound {
            if prev.get(party).copied().unwrap_or(0.0) >= 1.0 || pinned_zero.contains(party) {
                continue;
            }
            let (core, _) = weighted(sources, &prev);
            if core > 0.0 {
                coreness.insert(party.clone(), core);
            }
        }
    }
}

pub fn run(args: &ProvenanceArgs) -> Result<()> {
    let mut ledger = Ledger::open(&args.db)?;
    let conn = ledger.connection();

    // ── who is CORE ────────────────────────────────────────────────────────
    // Roots: human core/founder assertions + non-terminal project seeds.
    // Declared terminals are exactly 0.0 — external money exonerates.
    let human = crate::score::load_assertions(args.annotations.as_deref(), &args.db)?;
    let mut coreness: BTreeMap<String, f64> = BTreeMap::new();
    for (key, (class, _)) in &human {
        if matches!(class.as_str(), "core" | "founder") {
            coreness.insert(key.clone(), 1.0);
        }
    }
    let mut stmt = conn.prepare(
        "SELECT key FROM party
         WHERE role IN ('declared', 'signer', 'royalty') AND terminal_reason IS NULL",
    )?;
    for k in stmt.query_map([], |r| r.get::<_, String>(0))? {
        coreness.insert(k?, 1.0);
    }
    // The derived root, only when asked: the dominant mint-proceeds
    // destination classify computed. A cold collection's first pass has
    // nothing else to stand on — but a DERIVED root changes the figure's
    // epistemic grade, so it is opt-in and loudly reported.
    let dominant_meta: Option<String> = conn
        .query_row(
            "SELECT v FROM walk_meta WHERE k = 'mint_proceeds_dominant'",
            [],
            |r| r.get(0),
        )
        .optional()?;
    if args.derived_roots
        && let Some(meta) = dominant_meta
        && let Some((key, rest)) = meta.split_once(' ')
        && !key.is_empty()
    {
        if coreness.contains_key(key) {
            tracing::info!("provenance: derived root {key} already asserted — nothing added");
        } else {
            coreness.insert(key.to_string(), 1.0);
            tracing::warn!(
                key,
                detail = rest,
                "provenance: DERIVED root in use — the dominant mint-proceeds destination is \
                 treated as core by arithmetic, not assertion. Figures below inherit that \
                 basis; assert the wallet in the app to upgrade them."
            );
        }
    }
    let roots = coreness.len();
    if roots == 0 {
        tracing::warn!(
            "provenance: NO core roots — no core/founder assertions in the sidecar and no \
             non-terminal seeds. Coreness cannot propagate from nothing; classify wallets in \
             the app (or declare them in the registry) first — or run with --derived-roots \
             to stand on the dominant mint-proceeds destination."
        );
    }

    // ── holders and their mints ────────────────────────────────────────────
    let mut stmt = conn.prepare(
        "SELECT to_party, tx_hash, slot FROM asset_event
         WHERE kind = 'mint' AND asset_class IN ('nft', 'plain') AND to_party IS NOT NULL",
    )?;
    struct Mints {
        assets: u64,
        txs: BTreeSet<String>,
        first_slot: u64,
        last_slot: u64,
    }
    let mut mints: BTreeMap<String, Mints> = BTreeMap::new();
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)? as u64,
        ))
    })?;
    for row in rows {
        let (to, tx, slot) = row?;
        let e = mints.entry(to).or_insert(Mints {
            assets: 0,
            txs: BTreeSet::new(),
            first_slot: u64::MAX,
            last_slot: 0,
        });
        e.assets += 1;
        e.txs.insert(tx);
        e.first_slot = e.first_slot.min(slot);
        e.last_slot = e.last_slot.max(slot);
    }
    let total_minted: u64 = mints.values().map(|m| m.assets).sum();

    // Mints straight into core wallets need no tracing — they ARE the direct
    // team allocation, reported as their own line.
    let direct_core: u64 = mints
        .iter()
        .filter(|(k, _)| coreness.get(*k).copied().unwrap_or(0.0) >= 1.0)
        .map(|(_, m)| m.assets)
        .sum();

    // A ledger without seated holders has no inbound legs to attribute.
    let holder_parties: i64 = conn.query_row(
        "SELECT COUNT(*) FROM party WHERE role = 'holder'",
        [],
        |r| r.get(0),
    )?;
    if holder_parties == 0 {
        tracing::warn!(
            "provenance: no `holder` parties in this ledger — it was walked before \
             --watch-holders (or with it off), so buyers' funding legs are NOT booked and \
             attribution below covers only wallets the money frontier happened to seat. \
             Re-walk before trusting these numbers."
        );
    }

    // ── whole-ledger inbound decomposition, for INTERMEDIARIES ────────────
    // One pass over value_event; per party, inbound by source. Coreness then
    // propagates over this map for two rounds, so treasury → ops → buyer is
    // visible even when "ops" asserts nothing.
    let mut inbound: BTreeMap<String, BTreeMap<String, i128>> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT party, counterparty, SUM(delta) FROM value_event
         WHERE delta > 0 GROUP BY party, counterparty",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)? as i128,
        ))
    })?;
    for row in rows {
        let (p, cp, v) = row?;
        *inbound.entry(p).or_default().entry(cp).or_insert(0) += v;
    }
    // Roots stay pinned at 1.0 (an assertion outranks the arithmetic about
    // it); declared terminals pinned at 0.0.
    let mut pinned_zero: BTreeSet<String> = BTreeSet::new();
    let mut stmt = conn.prepare("SELECT key FROM party WHERE terminal_reason = 'declared'")?;
    for k in stmt.query_map([], |r| r.get::<_, String>(0))? {
        pinned_zero.insert(k?);
    }
    propagate(&mut coreness, &inbound, &pinned_zero);

    // ── per-holder verdicts, over the WINDOWED inbound ────────────────────
    let window = args.window_days * SLOTS_PER_DAY;
    let mut spend_stmt = conn.prepare(
        "SELECT COALESCE(SUM(delta), 0) FROM tx_delta WHERE party = ?1 AND tx_hash = ?2",
    )?;
    let mut fund_stmt = conn.prepare(
        "SELECT counterparty, SUM(delta), MIN(tx_hash) FROM value_event
         WHERE party = ?1 AND delta > 0 AND slot >= ?2 AND slot <= ?3
         GROUP BY counterparty ORDER BY SUM(delta) DESC",
    )?;
    // Token funding — mint payments arrive in USDM as well as ADA (the S2
    // model), and a wallet loaded with core stablecoin is core-funded however
    // clean its ADA history looks. Carrier rows are excluded by unit shape:
    // only labelled fungibles participate (`is_payment_unit`).
    let mut token_stmt = conn.prepare(
        "SELECT unit, counterparty, SUM(quantity), MIN(tx_hash) FROM unit_flow
         WHERE party = ?1 AND quantity > 0 AND unit <> 'lovelace'
           AND slot >= ?2 AND slot <= ?3
         GROUP BY unit, counterparty",
    )?;
    let mut verdicts: Vec<HolderVerdict> = Vec::new();
    for (key, m) in &mints {
        if coreness.get(key).copied().unwrap_or(0.0) >= 1.0 {
            continue; // direct core allocation, already counted
        }
        let mut mint_spend: i128 = 0;
        for tx in &m.txs {
            let d: i64 = spend_stmt.query_row(rusqlite::params![key, tx], |r| r.get(0))?;
            if d < 0 {
                mint_spend += i128::from(-d);
            }
        }
        let from_slot = m.first_slot.saturating_sub(window) as i64;

        // Funding sources per payment unit: "ada" from the netted value
        // events, each labelled fungible from its unit flows.
        let mut by_unit: BTreeMap<String, BTreeMap<String, i128>> = BTreeMap::new();
        // `(ticker, source, raw quantity for sorting, display string, tx)`.
        let mut raw_legs: Vec<(String, String, i128, String, String)> = Vec::new();
        let rows =
            fund_stmt.query_map(rusqlite::params![key, from_slot, m.last_slot as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? as i128,
                    r.get::<_, String>(2)?,
                ))
            })?;
        for row in rows {
            let (src, v, tx) = row?;
            by_unit
                .entry("ada".into())
                .or_default()
                .insert(src.clone(), v);
            raw_legs.push((
                "ada".into(),
                src,
                v,
                chain_ledger::tokens::format_quantity("lovelace", v),
                tx,
            ));
        }
        let rows =
            token_stmt.query_map(rusqlite::params![key, from_slot, m.last_slot as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)? as i128,
                    r.get::<_, String>(3)?,
                ))
            })?;
        for row in rows {
            let (unit, src, v, tx) = row?;
            if !is_payment_unit(&unit) {
                continue;
            }
            let ticker = unit_ticker(&unit);
            *by_unit
                .entry(ticker.clone())
                .or_default()
                .entry(src.clone())
                .or_insert(0) += v;
            // Rendered with the unit's own decimals where known — an
            // unscaled stablecoin leg reads a million times too big, and
            // this line is a FIGURE, not a hint.
            let shown = chain_ledger::tokens::format_quantity(&unit, v);
            raw_legs.push((ticker, src, v, shown, tx));
        }

        let per_unit: Vec<(String, f64, f64)> = by_unit
            .iter()
            .map(|(u, sources)| {
                let (c, unk) = weighted(sources, &coreness);
                (u.clone(), c, unk)
            })
            .collect();
        let (core_share, unknown_share, via) = headline(&per_unit);

        // Keep only legs that carry actual coreness — the evidence trail.
        raw_legs.retain(|(_, src, _, _, _)| coreness.get(src).copied().unwrap_or(0.0) > 0.05);
        raw_legs.sort_by_key(|l| std::cmp::Reverse(l.2));
        raw_legs.truncate(4);
        let legs: Vec<(String, String)> = raw_legs
            .into_iter()
            .map(|(unit, src, _, shown, tx)| {
                (
                    format!(
                        "{shown} {unit} from {src} (coreness {:.2})",
                        coreness.get(&src).copied().unwrap_or(0.0)
                    ),
                    tx,
                )
            })
            .collect();
        verdicts.push(HolderVerdict {
            key: key.clone(),
            assets: m.assets,
            mint_spend,
            core_share,
            unknown_share,
            via,
            legs,
        });
    }

    // ── report ─────────────────────────────────────────────────────────────
    let flagged: Vec<&HolderVerdict> = {
        let mut v: Vec<&HolderVerdict> = verdicts
            .iter()
            .filter(|h| h.core_share >= args.threshold)
            .collect();
        v.sort_by(|a, b| {
            (b.core_share * b.assets as f64).total_cmp(&(a.core_share * a.assets as f64))
        });
        v
    };
    let flagged_assets: u64 = flagged.iter().map(|h| h.assets).sum();
    let unattributed_heavy = verdicts.iter().filter(|h| h.unknown_share > 0.5).count();

    tracing::info!(
        total_minted,
        direct_core,
        core_funded = flagged_assets,
        effective_team = direct_core + flagged_assets,
        holders = verdicts.len(),
        flagged = flagged.len(),
        core_roots = roots,
        threshold = args.threshold,
        window_days = args.window_days,
        "provenance: EFFECTIVE team-funded supply = direct core mints + mints by \
         core-funded wallets. Compare against the ADVERTISED allocation — the chain \
         cannot know what was promised."
    );
    if unattributed_heavy > 0 {
        tracing::warn!(
            holders = unattributed_heavy,
            "provenance: holders whose windowed funding is >50% UNRESOLVED — their \
             core share is a floor, not a verdict. resolve-local closes this."
        );
    }
    for h in flagged.iter().take(args.report) {
        tracing::info!(
            holder = %h.key,
            assets = h.assets,
            core_share = format!("{:.0}%", h.core_share * 100.0),
            unknown = format!("{:.0}%", h.unknown_share * 100.0),
            via = h.via.as_deref().unwrap_or("ada"),
            spend = format!("{:.0} ada", h.mint_spend as f64 / 1e6),
            "provenance: core-funded holder"
        );
        for (label, tx) in &h.legs {
            tracing::info!("    ← {label} e.g. tx {tx}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(entries: &[(&str, i128)]) -> BTreeMap<String, i128> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), *v))
            .collect()
    }

    /// The dark-wallet chain the pass exists for: treasury (asserted core) →
    /// ops wallet (asserts nothing) → buyer. After propagation the buyer's
    /// funding reads as core THROUGH the intermediary.
    #[test]
    fn coreness_flows_through_an_undisclosed_intermediary() {
        let mut coreness: BTreeMap<String, f64> = BTreeMap::new();
        coreness.insert("treasury".into(), 1.0);
        let mut inbound: BTreeMap<String, BTreeMap<String, i128>> = BTreeMap::new();
        // ops is 100% treasury-funded; buyer is 80% ops-funded, 20% stranger.
        inbound.insert("ops".into(), m(&[("treasury", 1_000)]));
        inbound.insert("buyer".into(), m(&[("ops", 800), ("stranger", 200)]));
        propagate(&mut coreness, &inbound, &BTreeSet::new());

        assert!((coreness["ops"] - 1.0).abs() < 1e-9, "ops IS core money");
        let (share, _) = weighted(&m(&[("ops", 800), ("stranger", 200)]), &coreness);
        assert!(
            (share - 0.8).abs() < 1e-9,
            "the buyer's mint money is 80% core, via a wallet that asserted nothing"
        );
    }

    /// A declared terminal — a CEX funder — is pinned at ZERO: money from
    /// outside the chain is the exonerating direction, and it must stay that
    /// way even when the exchange wallet itself was once paid by core.
    #[test]
    fn external_money_stays_external() {
        let mut coreness: BTreeMap<String, f64> = BTreeMap::new();
        coreness.insert("treasury".into(), 1.0);
        let pinned: BTreeSet<String> = ["cex-funder".to_string()].into();
        let mut inbound: BTreeMap<String, BTreeMap<String, i128>> = BTreeMap::new();
        inbound.insert("cex-funder".into(), m(&[("treasury", 5_000)]));
        inbound.insert("buyer".into(), m(&[("cex-funder", 1_000)]));
        propagate(&mut coreness, &inbound, &pinned);
        assert_eq!(coreness.get("cex-funder"), None, "pinned at zero");
        let (share, _) = weighted(&m(&[("cex-funder", 1_000)]), &coreness);
        assert_eq!(share, 0.0, "exchange withdrawals do not inherit coreness");
    }

    /// The unresolved payer is its own share, never silently zero — "40%
    /// unattributed" and "40% from strangers" are different findings.
    #[test]
    fn unresolved_funding_is_reported_not_buried() {
        let coreness: BTreeMap<String, f64> = [("treasury".to_string(), 1.0)].into();
        let (core, unknown) = weighted(&m(&[("treasury", 600), ("", 400)]), &coreness);
        assert!((core - 0.6).abs() < 1e-9);
        assert!((unknown - 0.4).abs() < 1e-9);
    }

    /// Payment units: lovelace and labelled fungibles (USDM is 333). An NFT
    /// riding through a wallet is custody, not funding.
    #[test]
    fn only_money_shaped_units_fund_a_mint() {
        assert!(is_payment_unit("lovelace"));
        // USDM: label 0014df10 + "USDM"
        assert!(is_payment_unit(
            "c48cbb3d5087d47e0193cb26b6cabbc655e3b06806f2543f9e56e10f.0014df105553444d"
        ));
        // an RFT (444)
        assert!(is_payment_unit("aa.001bc28054657374"));
        // a user NFT (222) and a plain-named NFT do NOT fund anyone
        assert!(!is_payment_unit("aa.000de1404d4430303031"));
        assert!(!is_payment_unit("aa.4d656b6b613031"));
        assert_eq!(
            unit_ticker(
                "c48cbb3d5087d47e0193cb26b6cabbc655e3b06806f2543f9e56e10f.0014df105553444d"
            ),
            "USDM"
        );
        assert_eq!(unit_ticker("lovelace"), "ada");
    }

    /// The cross-unit verdict is the MAXIMUM per-unit core share — units are
    /// incomparable without inventing a rate, and a wallet loaded with core
    /// USDM is core-funded however clean its ADA history looks. The breakdown
    /// rides along so the max never hides its basis.
    #[test]
    fn the_verdict_takes_the_most_incriminating_unit() {
        let per_unit = vec![
            ("ada".to_string(), 0.12, 0.05),
            ("USDM".to_string(), 0.98, 0.0),
        ];
        let (core, unknown, via) = headline(&per_unit);
        assert!((core - 0.98).abs() < 1e-9);
        assert!(
            unknown.abs() < 1e-9,
            "unknown share follows the chosen unit"
        );
        assert_eq!(via.as_deref(), Some("ada 12% · USDM 98%"));
        // One unit: no breakdown to print.
        let (c, _, via) = headline(&[("ada".to_string(), 0.4, 0.1)]);
        assert!((c - 0.4).abs() < 1e-9);
        assert_eq!(via, None);
    }
}
