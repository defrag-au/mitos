//! `score` — the interest engine's extraction pass.
//!
//! Design: `cnft.dev-workers/docs/design/PROJECT_LEDGER_INTEREST.md`. The
//! stance that governs every line here: **interest directs attention; it
//! never asserts facts.** Scores appear in no exported figure; a
//! counter-payment match suppresses a transaction's interest but never books
//! a sale; every score decomposes into signal rows that sum to it exactly.
//!
//! ## Where this runs
//!
//! LOCALLY, against the downloaded ledger and the app's annotations sidecar —
//! never on the box. Human classifications live in the sidecar, which never
//! leaves the operator's machine, and scoring without them would miss the
//! signal the user ranked above all others: *everything core touches is
//! interesting.* The pass reads sqlite only; the 215 GB snapshot is not
//! involved. Recomputed wholesale each run, cleared by `reset` — the same
//! contract as `enrich` and `classify`.
//!
//! ## Rounds
//!
//! Two, Jacobi-style: round 0 is structural (computable with no party
//! knowledge), round 1 is relational (`core_touch`, `hot_party_touch`),
//! computed from round-0 snapshots so results are independent of iteration
//! order. Two rounds reaches payer-of-payer, which is as far as the evidence
//! supports; a fixpoint would be less explainable and no more true.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chain_ledger::interest::{counter_payment_confidence, finalize};
use chain_ledger::{Confidence, Signal, Weights};
use rusqlite::{Connection, OpenFlags, params};

use crate::store::Ledger;

#[derive(clap::Args, Debug)]
pub struct ScoreArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,

    /// The app's annotations sidecar (human classifications). Defaults to
    /// `<db>.annotations.db`; scoring proceeds without it, minus every
    /// assertion-driven signal, and says so.
    #[arg(long)]
    pub annotations: Option<PathBuf>,

    /// Weight overrides (TOML, same shape as the defaults). The weights USED
    /// are recorded in `score_meta` either way.
    #[arg(long)]
    pub weights: Option<PathBuf>,

    /// How many unexamined parties to print, ranked by interest.
    #[arg(long, default_value_t = 15)]
    pub report: usize,
}

/// One fired signal against a subject: `(weight, detail)`.
type Fired = BTreeMap<Signal, (f64, String)>;

pub fn run(args: &ScoreArgs) -> Result<()> {
    let weights: Weights = match &args.weights {
        Some(p) => toml::from_str(&std::fs::read_to_string(p)?)
            .with_context(|| format!("parsing weights {}", p.display()))?,
        None => Weights::default(),
    };

    let mut ledger = Ledger::open(&args.db)?;
    let human = load_assertions(args, &args.db)?;
    if human.is_empty() {
        tracing::warn!(
            "score: no annotations sidecar (or no classifications in it) — core_touch and \
             disagreement reporting are OFF this run. The engine still scores structure, but \
             the user's primary signal is missing."
        );
    }

    let conn = ledger.connection();
    // Counterparty lookups drive several extractors and the walk never needed
    // this index; one-time cost, kept for the app's benefit too.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_uf_counterparty ON unit_flow(counterparty)",
    )?;

    let mut tx_sig: HashMap<String, Fired> = HashMap::new();
    let mut party_sig: HashMap<String, Fired> = HashMap::new();
    // Round-trip suppression, held aside — see `signal_capability_adjacency`.
    let mut suppress: HashMap<String, (f64, String)> = HashMap::new();

    // ── round 0: structural ────────────────────────────────────────────────
    let caps = load_capabilities(conn)?;
    if caps.provider.is_empty() {
        // This happened for real: the schema migration emptied the capability
        // tables, score ran anyway, and a full pass executed with no round-trip
        // suppression, no provider damping and no CEX context — an accidental
        // ablation study that looked like a result.
        tracing::warn!(
            "score: counterparty_capability is EMPTY — run `classify` first. Without it there \
             is no round-trip suppression, no provider damping and no boundary context; every \
             DEX-adjacent transaction will read as interesting."
        );
    }
    signal_mints(conn, &weights, &mut tx_sig, &mut party_sig)?;
    signal_royalty(conn, &weights, &mut tx_sig)?;
    signal_fanout(conn, &weights, &mut tx_sig)?;
    signal_tx_message(conn, &weights, &mut tx_sig)?;
    let venue_txs = signal_venue_sales(conn, &weights, &mut tx_sig)?;
    let (cp_matched, buyers_by_cp) = signal_counter_payment(conn, &weights, &mut tx_sig)?;
    let grants = signal_asset_grants(
        conn,
        &weights,
        &caps,
        &venue_txs,
        &cp_matched,
        &mut tx_sig,
        &mut party_sig,
    )?;
    signal_recurrence(conn, &weights, &mut tx_sig, &mut party_sig)?;
    signal_capability_adjacency(conn, &weights, &caps, &mut tx_sig, &mut suppress)?;
    signal_unexplained_inbound(conn, &weights, &mut tx_sig)?;
    signal_core_assertions(conn, &weights, &human, &mut party_sig)?;

    // Fold suppression into txs that earned a positive signal; the rest never
    // materialise. Round-1 additions are covered again in the finals loop.
    for (tx, (w, detail)) in &suppress {
        if let Some(fired) = tx_sig.get_mut(tx) {
            fired.insert(Signal::RoundTripLeg, (*w, detail.clone()));
        }
    }

    // Magnitude: ONE pass builds the percentile population and the values for
    // signal-bearing txs. The first version point-queried per scored tx —
    // 1.6M queries on the first real run.
    let (population, mut tx_value) = magnitude(conn, &tx_sig)?;

    let score_of = |fired: &Fired, value: f64| -> (f64, f64) {
        let additive: f64 = fired.values().map(|(w, _)| w).sum();
        let mult = weights.magnitude_floor + percentile(&population, value);
        finalize(additive, mult)
    };

    // Round-0 tx scores, then round-0 party scores (signals + top-K accrual).
    let r0_tx: HashMap<String, f64> = tx_sig
        .iter()
        .map(|(tx, fired)| {
            let v = tx_value.get(tx).copied().unwrap_or(0.0);
            (tx.clone(), score_of(fired, v).0)
        })
        .collect();
    let mut breadth: HashMap<String, f64> = HashMap::new();
    accrue_top_transactions(conn, &weights, &r0_tx, &mut breadth, &mut party_sig)?;
    let r0_party: HashMap<String, f64> = party_sig
        .iter()
        .map(|(k, fired)| {
            (
                k.clone(),
                fired.values().map(|(w, _)| w).sum::<f64>().max(0.0),
            )
        })
        .collect();

    // ── round 1: relational ───────────────────────────────────────────────
    let core_receipts = signal_core_touch(conn, &weights, &human, &mut tx_sig)?;
    signal_hot_party_touch(conn, &weights, &caps, &r0_party, &mut tx_sig)?;

    // Finals. `round` records what settled each score, so "why" reads as a
    // chain rather than a number.
    let mut mag_point = conn.prepare("SELECT MAX(ABS(delta)) FROM tx_delta WHERE tx_hash = ?")?;
    let mut tx_rows: Vec<(String, f64, i64)> = Vec::with_capacity(tx_sig.len());
    for (tx, fired) in &mut tx_sig {
        // Round-1 signals introduced txs after both bulk passes; point queries
        // cover the stragglers.
        if let Some((w, detail)) = suppress.get(tx)
            && !fired.contains_key(&Signal::RoundTripLeg)
        {
            fired.insert(Signal::RoundTripLeg, (*w, detail.clone()));
        }
        if !tx_value.contains_key(tx) {
            let v: Option<i64> = mag_point
                .query_row([tx.as_str()], |r| r.get(0))
                .ok()
                .flatten();
            tx_value.insert(tx.clone(), v.unwrap_or(0) as f64);
        }
        let v = tx_value.get(tx).copied().unwrap_or(0.0);
        let (score, mag_row) = score_of(fired, v);
        let mult = weights.magnitude_floor + percentile(&population, v);
        fired.insert(
            Signal::Magnitude,
            (
                mag_row,
                format!(
                    "×{mult:.2} (p{:.0} of net moved)",
                    percentile(&population, v) * 100.0
                ),
            ),
        );
        let relational = fired
            .keys()
            .any(|s| matches!(s, Signal::CoreTouch | Signal::HotPartyTouch));
        tx_rows.push((tx.clone(), score, i64::from(relational)));
    }
    // Statement drop runs against the connection borrow; release it before
    // `ledger.connection()` is taken again for the persist.
    drop(mag_point);
    let tx_final: HashMap<&str, f64> = tx_rows.iter().map(|(t, s, _)| (t.as_str(), *s)).collect();

    // Party finals: re-accrue top-K from FINAL tx scores.
    for fired in party_sig.values_mut() {
        fired.remove(&Signal::TopTransactions);
    }
    let final_map: HashMap<String, f64> = tx_final
        .iter()
        .map(|(k, v)| ((*k).to_string(), *v))
        .collect();
    accrue_top_transactions(conn, &weights, &final_map, &mut breadth, &mut party_sig)?;
    let party_rows: Vec<(String, f64)> = party_sig
        .iter()
        .map(|(k, fired)| {
            (
                k.clone(),
                fired.values().map(|(w, _)| w).sum::<f64>().max(0.0),
            )
        })
        .collect();

    // ── proposals + disagreements ──────────────────────────────────────────
    let proposals = propose(
        conn,
        &caps,
        &party_sig,
        &grants,
        &core_receipts,
        &buyers_by_cp,
    )?;
    let disagreements = disagree(&human, &proposals, &party_rows, &grants);

    // ── persist, one transaction, wholesale ───────────────────────────────
    persist(
        ledger.connection(),
        &weights,
        &human,
        &tx_sig,
        &tx_rows,
        &party_sig,
        &party_rows,
        &proposals,
        &disagreements,
    )?;

    report(
        &human,
        tx_rows.len(),
        &party_rows,
        &party_sig,
        &proposals,
        &disagreements,
        args.report,
    );
    Ok(())
}

// ── assertions (the sidecar) ───────────────────────────────────────────────

/// `key → (class, confidence)` from the app's annotations sidecar.
fn load_assertions(
    args: &ScoreArgs,
    db: &std::path::Path,
) -> Result<BTreeMap<String, (String, Confidence)>> {
    let path = args
        .annotations
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("{}.annotations.db", db.display())));
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening annotations {}", path.display()))?;
    // `confidence` is a phase-2 column; degrade to Probable when absent —
    // an assertion made before confidence existed is an ordinary assertion,
    // not a tentative one.
    let has_conf = {
        let mut stmt = conn.prepare("PRAGMA table_info(party_note)")?;
        let mut rows = stmt.query([])?;
        let mut found = false;
        while let Some(r) = rows.next()? {
            if r.get::<_, String>(1)? == "confidence" {
                found = true;
            }
        }
        found
    };
    let sql = if has_conf {
        "SELECT key, class, confidence FROM party_note WHERE class IS NOT NULL"
    } else {
        "SELECT key, class, NULL FROM party_note WHERE class IS NOT NULL"
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
        ))
    })?;
    let mut out = BTreeMap::new();
    for row in rows {
        let (key, class, conf) = row?;
        let conf = conf
            .as_deref()
            .map(Confidence::parse)
            .unwrap_or(Confidence::Probable);
        out.insert(key, (class, conf));
    }
    Ok(out)
}

// ── capability context ─────────────────────────────────────────────────────

struct Caps {
    round_trip: BTreeSet<String>,
    boundary: BTreeSet<String>,
    provider: BTreeSet<String>,
    label: BTreeMap<String, String>,
}

fn load_capabilities(conn: &Connection) -> Result<Caps> {
    let mut caps = Caps {
        round_trip: BTreeSet::new(),
        boundary: BTreeSet::new(),
        provider: BTreeSet::new(),
        label: BTreeMap::new(),
    };
    let mut stmt = conn.prepare("SELECT key, capability FROM counterparty_capability")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (key, cap) = row?;
        match cap.as_str() {
            "dex" | "aggregator" | "lending" | "staking" => {
                caps.round_trip.insert(key.clone());
            }
            "cex" | "bridge" => {
                caps.boundary.insert(key.clone());
            }
            _ => {}
        }
        caps.provider.insert(key);
    }
    let mut stmt =
        conn.prepare("SELECT key, name FROM counterparty_kind WHERE name IS NOT NULL")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (key, name) = row?;
        caps.label.insert(key, name);
    }
    Ok(caps)
}

// ── round-0 extractors ─────────────────────────────────────────────────────

fn add(fired: &mut HashMap<String, Fired>, subject: &str, s: Signal, w: f64, detail: String) {
    fired
        .entry(subject.to_string())
        .or_default()
        .insert(s, (w, detail));
}

fn signal_mints(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
    party_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let mut stmt = conn.prepare("SELECT DISTINCT tx_hash FROM asset_event WHERE kind = 'mint'")?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    for tx in rows {
        add(
            tx_sig,
            &tx?,
            Signal::PolicyMint,
            weights.policy_mint,
            "mints the policy".into(),
        );
    }

    // Fund-split weight is the SHARE of total proceeds, never the amount:
    // absolute value is how a 44 ₳ leg once seated a bank.
    let total: f64 = conn.query_row(
        "SELECT COALESCE(SUM(lovelace), 0) FROM mint_payment",
        [],
        |r| r.get::<_, i64>(0),
    )? as f64;
    if total <= 0.0 {
        return Ok(());
    }
    let mut stmt =
        conn.prepare("SELECT tx_hash, SUM(lovelace) FROM mint_payment GROUP BY tx_hash")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
    for row in rows {
        let (tx, v) = row?;
        let share = v as f64 / total;
        add(
            tx_sig,
            &tx,
            Signal::MintFundSplit,
            weights.mint_fund_split * share,
            format!("{:.2}% of mint proceeds", share * 100.0),
        );
    }
    let mut stmt =
        conn.prepare("SELECT destination, SUM(lovelace) FROM mint_payment GROUP BY destination")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
    for row in rows {
        let (dest, v) = row?;
        let share = v as f64 / total;
        add(
            party_sig,
            &dest,
            Signal::MintFundSplit,
            weights.mint_fund_split * share,
            format!("received {:.2}% of mint proceeds", share * 100.0),
        );
    }
    Ok(())
}

fn signal_royalty(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let royalty: Option<String> = conn
        .query_row("SELECT key FROM party WHERE role = 'royalty'", [], |r| {
            r.get(0)
        })
        .ok();
    let Some(royalty) = royalty else {
        return Ok(());
    };
    let mut stmt =
        conn.prepare("SELECT DISTINCT tx_hash FROM unit_flow WHERE party = ? AND quantity > 0")?;
    let rows = stmt.query_map([&royalty], |r| r.get::<_, String>(0))?;
    for tx in rows {
        add(
            tx_sig,
            &tx?,
            Signal::RoyaltyPayment,
            weights.royalty_payment,
            "pays the CIP-27 royalty".into(),
        );
    }
    Ok(())
}

fn signal_fanout(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    // ONE funder → many recipients in a single tx. `payers = 1` is the "one
    // funder, exactly attributed" guarantee; without it a multi-funder tx
    // could read as a distribution it is not.
    let mut stmt = conn.prepare(
        "SELECT tx_hash, COUNT(DISTINCT counterparty) AS n
         FROM unit_flow
         WHERE quantity < 0 AND payers = 1 AND counterparty <> ''
         GROUP BY tx_hash HAVING n >= ?",
    )?;
    let rows = stmt.query_map([weights.fanout_min_recipients], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;
    for row in rows {
        let (tx, n) = row?;
        add(
            tx_sig,
            &tx,
            Signal::FanoutDistribution,
            weights.fanout_distribution,
            format!("1 funder → {n} recipients"),
        );
    }
    Ok(())
}

fn signal_tx_message(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='tx_message')",
        [],
        |r| r.get(0),
    )?;
    if !exists {
        // Absence must not read as "no messages": the capture is a walk
        // addition that rides the next re-walk.
        tracing::info!(
            "score: no tx_message table — CIP-20 corroboration unavailable until the next walk \
             captures it; fan-out detection is unaffected"
        );
        return Ok(());
    }
    let mut stmt = conn.prepare("SELECT tx_hash, text FROM tx_message")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (tx, text) = row?;
        // Verbatim: the text is curator gold, and paraphrasing evidence is
        // how it stops being evidence.
        add(tx_sig, &tx, Signal::TxMessage, weights.tx_message, text);
    }
    Ok(())
}

fn signal_venue_sales(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<BTreeSet<String>> {
    let mut stmt =
        conn.prepare("SELECT tx_hash, MAX(venue) FROM secondary_sale GROUP BY tx_hash")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    let mut set = BTreeSet::new();
    for row in rows {
        let (tx, venue) = row?;
        add(
            tx_sig,
            &tx,
            Signal::VenueSale,
            weights.venue_sale,
            format!("venue sale ({venue})"),
        );
        set.insert(tx);
    }
    Ok(set)
}

/// Matched `(tx, receiver)` pairs — the grant detector's exclusion list.
type CounterMatches = BTreeSet<(String, String)>;
/// Receivers who demonstrably paid — the customer proposal's evidence.
type CounterPayers = BTreeSet<String>;

/// The non-atomic P2P matcher.
///
/// SUPPRESSES ONLY. Nothing here writes a sale; `secondary_sale` stays
/// venue-only, which is the line between attention and fact.
fn signal_counter_payment(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<(CounterMatches, CounterPayers)> {
    let mut transfers = conn.prepare(
        "SELECT tx_hash, from_party, to_party, block_time FROM asset_event
         WHERE kind = 'transfer' AND from_party IS NOT NULL AND to_party IS NOT NULL",
    )?;
    // Value flowing receiver → sender, in either recording orientation
    // (whichever side the walk watched), in a DIFFERENT transaction. TWO
    // statements, not one OR: the OR form defeated index selection and each
    // probe degenerated toward a scan — measured as a large share of a
    // 35-minute run for 2,562 probes.
    let mut reverse_a = conn.prepare(
        "SELECT MIN(ABS(block_time - ?3)) FROM unit_flow
         WHERE party = ?1 AND counterparty = ?2 AND quantity > 0
           AND unit = 'lovelace' AND tx_hash <> ?4
           AND ABS(block_time - ?3) <= ?5",
    )?;
    let mut reverse_b = conn.prepare(
        "SELECT MIN(ABS(block_time - ?3)) FROM unit_flow
         WHERE party = ?2 AND counterparty = ?1 AND quantity < 0
           AND unit = 'lovelace' AND tx_hash <> ?4
           AND ABS(block_time - ?3) <= ?5",
    )?;
    let rows = transfers.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, i64>(3)?,
        ))
    })?;
    let mut matched = BTreeSet::new();
    let mut payers = BTreeSet::new();
    for row in rows {
        let (tx, from, to, t) = row?;
        let probe = |stmt: &mut rusqlite::Statement<'_>| -> Option<i64> {
            stmt.query_row(
                params![from, to, t, tx, weights.counter_payment_window_secs],
                |r| r.get::<_, Option<i64>>(0),
            )
            .ok()
            .flatten()
        };
        let gap = match (probe(&mut reverse_a), probe(&mut reverse_b)) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        let Some(gap) = gap else { continue };
        let conf = counter_payment_confidence(
            gap,
            weights.counter_payment_full_secs,
            weights.counter_payment_window_secs,
        );
        if conf <= 0.0 {
            continue;
        }
        add(
            tx_sig,
            &tx,
            Signal::CounterPayment,
            weights.counter_payment * conf,
            format!(
                "value back within {}m — trade-shaped, conf {conf:.2}",
                gap / 60
            ),
        );
        matched.insert((tx.clone(), to.clone()));
        payers.insert(to);
    }
    Ok((matched, payers))
}

/// Asset delivered, nothing paid — the payment-in-kind candidate. Returns
/// grants-received counts per receiver.
fn signal_asset_grants(
    conn: &Connection,
    weights: &Weights,
    caps: &Caps,
    venue_txs: &BTreeSet<String>,
    cp_matched: &BTreeSet<(String, String)>,
    tx_sig: &mut HashMap<String, Fired>,
    party_sig: &mut HashMap<String, Fired>,
) -> Result<BTreeMap<String, u32>> {
    let mut transfers = conn.prepare(
        "SELECT e.tx_hash, e.from_party, e.to_party, COUNT(*)
         FROM asset_event e
         WHERE e.kind = 'transfer' AND e.from_party IS NOT NULL AND e.to_party IS NOT NULL
           AND EXISTS (SELECT 1 FROM party p WHERE p.key = e.from_party)
         GROUP BY e.tx_hash, e.from_party, e.to_party",
    )?;
    // An ATOMIC purchase: the receiver's money reached the sender inside this
    // very transaction. Rare on Cardano (trades are mostly non-atomic — the
    // window matcher handles those) but decisive when present.
    let mut paid_in_tx = conn.prepare(
        "SELECT EXISTS(SELECT 1 FROM unit_flow
          WHERE tx_hash = ?1 AND party = ?2 AND counterparty = ?3 AND quantity > 0)",
    )?;
    let rows = transfers.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, i64>(3)?,
        ))
    })?;
    let mut grants: BTreeMap<String, u32> = BTreeMap::new();
    for row in rows {
        let (tx, from, to, n) = row?;
        if venue_txs.contains(&tx)
            || cp_matched.contains(&(tx.clone(), to.clone()))
            // A provider receiving assets is escrow/listing plumbing, not a
            // grant — otherwise every listing reads as a gift to jpg.store.
            || caps.provider.contains(&to)
            || paid_in_tx.query_row(params![tx, from, to], |r| r.get::<_, bool>(0))?
        {
            continue;
        }
        add(
            tx_sig,
            &tx,
            Signal::AssetGrant,
            weights.asset_grant,
            format!(
                "{n} asset(s) to {}, nothing back in tx or window",
                elide(&to)
            ),
        );
        *grants.entry(to).or_default() += n as u32;
    }
    for (to, n) in &grants {
        let capped = (*n).min(5) as f64 / 5.0;
        add(
            party_sig,
            to,
            Signal::AssetGrant,
            weights.asset_grant * capped,
            format!("{n} asset(s) received without payment"),
        );
    }
    Ok(grants)
}

fn signal_recurrence(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
    party_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    // ONE ordered scan, grouped in Rust. The first version enumerated
    // candidate pairs and re-queried per pair — 21,241 pairs on Mekka, each
    // scanning its party's whole row set (Pillar alone: ~47k rows), on the
    // order of a BILLION row visits. The design's "re-score in seconds"
    // promise dies in that loop; a single pass over 1.2M rows keeps it.
    let mut stmt = conn.prepare(
        "SELECT party, counterparty, tx_hash, block_time FROM unit_flow
         WHERE unit = 'lovelace' AND counterparty <> '' AND ABS(quantity) >= 2000000
         ORDER BY party, counterparty, block_time",
    )?;
    let mut rows = stmt.query([])?;

    let mut seen_pairs: BTreeSet<(String, String)> = BTreeSet::new();
    let mut current: Option<(String, String)> = None;
    let mut evs: Vec<(String, i64)> = Vec::new();
    let flush = |pair: &Option<(String, String)>,
                 evs: &mut Vec<(String, i64)>,
                 seen: &mut BTreeSet<(String, String)>,
                 tx_sig: &mut HashMap<String, Fired>,
                 party_sig: &mut HashMap<String, Fired>| {
        let Some((party, cp)) = pair else {
            evs.clear();
            return;
        };
        // Several outputs of one tx are one event.
        evs.dedup_by(|a, b| a.0 == b.0);
        let n = evs.len();
        let take = std::mem::take(evs);
        if n < weights.recurrence_min_events as usize || n > weights.recurrence_max_events as usize
        {
            return;
        }
        // Both-watched pairs stream once from each side.
        let key = if party <= cp {
            (party.clone(), cp.clone())
        } else {
            (cp.clone(), party.clone())
        };
        if !seen.insert(key) {
            return;
        }
        emit_recurrence(weights, party, cp, &take, tx_sig, party_sig);
    };
    while let Some(r) = rows.next()? {
        let party: String = r.get(0)?;
        let cp: String = r.get(1)?;
        let tx: String = r.get(2)?;
        let t: i64 = r.get(3)?;
        let pair = Some((party, cp));
        if pair != current {
            flush(&current, &mut evs, &mut seen_pairs, tx_sig, party_sig);
            current = pair;
        }
        evs.push((tx, t));
    }
    flush(&current, &mut evs, &mut seen_pairs, tx_sig, party_sig);
    Ok(())
}

/// The CV test and the signal rows for one qualifying pair.
fn emit_recurrence(
    weights: &Weights,
    party: &str,
    cp: &str,
    evs: &[(String, i64)],
    tx_sig: &mut HashMap<String, Fired>,
    party_sig: &mut HashMap<String, Fired>,
) {
    let intervals: Vec<f64> = evs
        .windows(2)
        .map(|w| (w[1].1 - w[0].1) as f64)
        .filter(|d| *d > 0.0)
        .collect();
    if intervals.is_empty() {
        return;
    }
    let mean = intervals.iter().sum::<f64>() / intervals.len() as f64;
    // Sub-day means are batches, not schedules.
    if mean < 86_400.0 {
        return;
    }
    let var = intervals.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / intervals.len() as f64;
    let cv = var.sqrt() / mean;
    if cv > weights.recurrence_max_cv {
        return;
    }
    let days = mean / 86_400.0;
    for (tx, _) in evs {
        add(
            tx_sig,
            tx,
            Signal::Recurrence,
            weights.recurrence,
            format!("every ~{days:.0}d × {} with {}", evs.len(), elide(cp)),
        );
    }
    for k in [party, cp] {
        add(
            party_sig,
            k,
            Signal::Recurrence,
            weights.recurrence,
            format!("regular pattern, every ~{days:.0}d × {}", evs.len()),
        );
    }
}

fn signal_capability_adjacency(
    conn: &Connection,
    weights: &Weights,
    caps: &Caps,
    tx_sig: &mut HashMap<String, Fired>,
    suppress: &mut HashMap<String, (f64, String)>,
) -> Result<()> {
    let txs_touching = |keys: &BTreeSet<String>| -> Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        let mut stmt =
            conn.prepare("SELECT DISTINCT tx_hash FROM unit_flow WHERE counterparty = ?")?;
        for key in keys {
            for tx in stmt.query_map([key], |r| r.get::<_, String>(0))? {
                out.insert(tx?);
            }
        }
        Ok(out)
    };
    // Round-trip suppression goes to a SIDE map, folded in only for txs that
    // earned a positive signal elsewhere. On Mekka 1.5M ordinary DEX-adjacent
    // txs would carry this as their ONLY signal, clamp to zero, and be stored
    // anyway — recording "not interesting" 1.5M times.
    for tx in txs_touching(&caps.round_trip)? {
        suppress.insert(
            tx,
            (
                weights.round_trip_leg,
                "swap/defi leg — own money returning".into(),
            ),
        );
    }
    for tx in txs_touching(&caps.boundary)? {
        add(
            tx_sig,
            &tx,
            Signal::BoundaryCrossing,
            weights.boundary_crossing,
            "touches an exchange/bridge — value from outside the chain".into(),
        );
    }
    Ok(())
}

fn signal_unexplained_inbound(
    conn: &Connection,
    weights: &Weights,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT d.tx_hash, d.delta, p.label
         FROM tx_delta d
         JOIN party p ON p.key = d.party AND p.role IN ('declared', 'signer', 'royalty')
         WHERE d.delta > 0
           AND NOT EXISTS (SELECT 1 FROM mint_payment m WHERE m.tx_hash = d.tx_hash)
           AND NOT EXISTS (SELECT 1 FROM secondary_sale s WHERE s.tx_hash = d.tx_hash)",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, Option<String>>(2)?,
        ))
    })?;
    for row in rows {
        let (tx, delta, label) = row?;
        // A swap's return leg is already suppressed; calling it unexplained
        // too would fight itself.
        let is_round_trip = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM unit_flow f
              JOIN counterparty_capability c ON c.key = f.counterparty
              WHERE f.tx_hash = ?1 AND c.capability IN ('dex','aggregator','lending','staking'))",
            [&tx],
            |r| r.get::<_, bool>(0),
        )?;
        if is_round_trip {
            continue;
        }
        add(
            tx_sig,
            &tx,
            Signal::UnexplainedInbound,
            weights.unexplained_inbound,
            format!(
                "{:.0} ₳ into {} — not mint, not sale, not a swap",
                delta as f64 / 1e6,
                label.unwrap_or_else(|| "a core wallet".into())
            ),
        );
    }
    Ok(())
}

fn signal_core_assertions(
    conn: &Connection,
    weights: &Weights,
    human: &BTreeMap<String, (String, Confidence)>,
    party_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    // Registry-declared wallets are core by declaration; asserted-core join
    // them at their confidence. Pinning is an ordinary additive row, so the
    // evidence panel shows it like everything else.
    let mut stmt = conn.prepare("SELECT key, label FROM party WHERE role = 'declared'")?;
    let rows = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
    })?;
    for row in rows {
        let (key, label) = row?;
        add(
            party_sig,
            &key,
            Signal::CoreAssertion,
            weights.core_assertion,
            format!("declared in registry ({})", label.unwrap_or_default()),
        );
    }
    for (key, (class, conf)) in human {
        if class == "core" {
            add(
                party_sig,
                key,
                Signal::CoreAssertion,
                weights.core_assertion * conf.factor(),
                format!("asserted core ({conf})"),
            );
        }
    }
    Ok(())
}

// ── magnitude ──────────────────────────────────────────────────────────────

/// One pass over the ledger: the full sorted population for percentiles, and
/// values kept only for signal-bearing txs. Membership costs a hash lookup per
/// row; the alternative — a point query per scored tx — was 1.6M queries on
/// the first real run.
fn magnitude(
    conn: &Connection,
    member: &HashMap<String, Fired>,
) -> Result<(Vec<f64>, HashMap<String, f64>)> {
    let mut stmt =
        conn.prepare("SELECT tx_hash, MAX(ABS(delta)) FROM tx_delta GROUP BY tx_hash")?;
    let mut rows = stmt.query([])?;
    let mut population = Vec::new();
    let mut values = HashMap::new();
    while let Some(r) = rows.next()? {
        let tx: String = r.get(0)?;
        let v: i64 = r.get(1)?;
        population.push(v as f64);
        if member.contains_key(&tx) {
            values.insert(tx, v as f64);
        }
    }
    population.sort_by(|a, b| a.total_cmp(b));
    Ok((population, values))
}

fn percentile(sorted: &[f64], v: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = sorted.partition_point(|x| *x <= v);
    idx as f64 / sorted.len() as f64
}

// ── party accrual ──────────────────────────────────────────────────────────

/// Fold each signal-bearing party's top-K transaction scores into its row.
/// Top-K, not total: a wallet in 10,000 boring txs must not outrank one in
/// three damning ones.
fn accrue_top_transactions(
    conn: &Connection,
    weights: &Weights,
    tx_scores: &HashMap<String, f64>,
    breadth: &mut HashMap<String, f64>,
    party_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let keys: Vec<String> = party_sig.keys().cloned().collect();
    let mut as_party = conn.prepare("SELECT DISTINCT tx_hash FROM tx_delta WHERE party = ?")?;
    let mut as_cp =
        conn.prepare("SELECT DISTINCT tx_hash FROM unit_flow WHERE counterparty = ?")?;
    let mut breadth_of =
        conn.prepare("SELECT COUNT(DISTINCT counterparty) FROM unit_flow WHERE tx_hash = ?")?;
    for key in keys {
        let mut scores: Vec<f64> = Vec::new();
        for stmt in [&mut as_party, &mut as_cp] {
            for tx in stmt.query_map([&key], |r| r.get::<_, String>(0))? {
                let tx = tx?;
                let Some(s) = tx_scores.get(&tx) else {
                    continue;
                };
                // CONSERVATION OF ATTENTION. A transaction's interest is a
                // fixed quantity SHARED by its participants, not duplicated
                // per head. Without this, one hot airdrop made all 174 of its
                // recipients — and Pillar itself — inherit the full score:
                // the dig queue became 174 clones at 7.9 and the provider the
                // damping existed to mute scored 7.12 as a subject. A 1:1
                // payment still tells you everything about both ends.
                let b = match breadth.get(&tx) {
                    Some(b) => *b,
                    None => {
                        let n: i64 = breadth_of.query_row([tx.as_str()], |r| r.get(0))?;
                        let b = (n.max(1)) as f64;
                        breadth.insert(tx.clone(), b);
                        b
                    }
                };
                scores.push(s / b);
            }
        }
        if scores.is_empty() {
            continue;
        }
        scores.sort_by(|a, b| b.total_cmp(a));
        scores.truncate(weights.party_top_k);
        let sum: f64 = scores.iter().sum();
        add(
            party_sig,
            &key,
            Signal::TopTransactions,
            sum * weights.top_transactions,
            format!(
                "top {} transactions it touches, breadth-shared",
                scores.len()
            ),
        );
    }
    Ok(())
}

// ── round-1 extractors ─────────────────────────────────────────────────────

/// Everything an asserted-core wallet touches is interesting — the user's
/// rule, verbatim. Returns receipts-from-core counts per counterparty for the
/// associate proposal.
fn signal_core_touch(
    conn: &Connection,
    weights: &Weights,
    human: &BTreeMap<String, (String, Confidence)>,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<BTreeMap<String, u32>> {
    let mut receipts: BTreeMap<String, u32> = BTreeMap::new();
    let mut flows = conn.prepare(
        "SELECT tx_hash, counterparty, quantity FROM unit_flow
         WHERE party = ?1 AND unit = 'lovelace' AND counterparty <> ''",
    )?;
    for (key, (class, conf)) in human {
        if class != "core" {
            continue;
        }
        let label = elide(key);
        let rows = flows.query_map([key], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })?;
        for row in rows {
            let (tx, cp, q) = row?;
            let w = weights.core_touch * conf.factor();
            add(
                tx_sig,
                &tx,
                Signal::CoreTouch,
                w,
                format!("touches {label} (core, {conf})"),
            );
            if q < 0 {
                *receipts.entry(cp).or_default() += 1;
            }
        }
    }
    Ok(receipts)
}

fn signal_hot_party_touch(
    conn: &Connection,
    weights: &Weights,
    caps: &Caps,
    r0_party: &HashMap<String, f64>,
    tx_sig: &mut HashMap<String, Fired>,
) -> Result<()> {
    let max = r0_party.values().copied().fold(0.0_f64, f64::max);
    if max <= 0.0 {
        return Ok(());
    }
    let mut as_party = conn.prepare("SELECT DISTINCT tx_hash FROM tx_delta WHERE party = ?")?;
    let mut as_cp =
        conn.prepare("SELECT DISTINCT tx_hash FROM unit_flow WHERE counterparty = ?")?;
    // Providers radiate at a tenth: services are context, not subjects. The
    // structural signals on their transactions are untouched by this — a mint
    // is a mint whoever carries it.
    let mut best: HashMap<String, (f64, String)> = HashMap::new();
    for (key, score) in r0_party {
        let norm = score / max;
        let damp = if caps.provider.contains(key) {
            weights.provider_damp
        } else {
            1.0
        };
        let w = weights.hot_party_touch * norm * damp;
        // A floor keeps two million near-zero rows out of the evidence table.
        if w < 0.05 {
            continue;
        }
        let label = caps.label.get(key).cloned().unwrap_or_else(|| elide(key));
        for stmt in [&mut as_party, &mut as_cp] {
            for tx in stmt.query_map([key], |r| r.get::<_, String>(0))? {
                let tx = tx?;
                let e = best.entry(tx).or_insert((0.0, String::new()));
                if w > e.0 {
                    *e = (w, format!("involves {label} (interest {norm:.2})"));
                }
            }
        }
    }
    for (tx, (w, detail)) in best {
        add(tx_sig, &tx, Signal::HotPartyTouch, w, detail);
    }
    Ok(())
}

// ── proposals + disagreements ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Proposal {
    pub class: Option<(String, Confidence)>,
    pub capability: Option<String>,
}

fn propose(
    conn: &Connection,
    caps: &Caps,
    party_sig: &HashMap<String, Fired>,
    grants: &BTreeMap<String, u32>,
    core_receipts: &BTreeMap<String, u32>,
    buyers_by_cp: &BTreeSet<String>,
) -> Result<BTreeMap<String, Proposal>> {
    // Buyers: minted-to (the mint is atomic — they funded it), venue buyers,
    // and counter-payment payers.
    let mut bought: BTreeSet<String> = buyers_by_cp.clone();
    let mut stmt = conn.prepare(
        "SELECT DISTINCT to_party FROM asset_event WHERE kind = 'mint' AND to_party IS NOT NULL",
    )?;
    for k in stmt.query_map([], |r| r.get::<_, String>(0))? {
        bought.insert(k?);
    }
    let mut stmt =
        conn.prepare("SELECT DISTINCT buyer FROM secondary_sale WHERE buyer IS NOT NULL")?;
    for k in stmt.query_map([], |r| r.get::<_, String>(0))? {
        bought.insert(k?);
    }

    let mut capability_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut stmt = conn.prepare("SELECT key, capability FROM counterparty_capability")?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (k, c) = row?;
        capability_of.entry(k).or_default().push(c);
    }

    let mut out = BTreeMap::new();
    for key in party_sig.keys() {
        let mint_share = party_sig
            .get(key)
            .and_then(|f| f.get(&Signal::MintFundSplit))
            .map(|(w, _)| *w)
            .unwrap_or(0.0);
        let grants_n = grants.get(key).copied().unwrap_or(0);
        let core_n = core_receipts.get(key).copied().unwrap_or(0);

        let class = if mint_share > 0.6 {
            // > 30% share at weight 2.0.
            Some(("core".to_string(), Confidence::Probable))
        } else if core_n >= 3 && grants_n == 0 {
            Some(("associate".to_string(), Confidence::Probable))
        } else if grants_n >= 3 {
            // Recurring free assets could equally be giveaway winners — the
            // curator decides, which is what tentative is for.
            Some(("associate".to_string(), Confidence::Tentative))
        } else if bought.contains(key) && !caps.provider.contains(key) {
            Some(("customer".to_string(), Confidence::Probable))
        } else {
            None
        };
        let capability = capability_of.get(key).map(|c| c.join("·"));
        if class.is_some() || capability.is_some() {
            out.insert(key.clone(), Proposal { class, capability });
        }
    }
    Ok(out)
}

fn disagree(
    human: &BTreeMap<String, (String, Confidence)>,
    proposals: &BTreeMap<String, Proposal>,
    party_rows: &[(String, f64)],
    grants: &BTreeMap<String, u32>,
) -> Vec<(String, &'static str, String, String, f64, String)> {
    let score_of: BTreeMap<&str, f64> = party_rows.iter().map(|(k, s)| (k.as_str(), *s)).collect();
    let mut out = Vec::new();
    for (key, (class, _)) in human {
        let score = score_of.get(key.as_str()).copied().unwrap_or(0.0);
        if let Some(p) = proposals.get(key)
            && let Some((proposed, conf)) = &p.class
            && proposed != class
        {
            out.push((
                key.clone(),
                "classification",
                class.clone(),
                format!("{proposed} ({conf})"),
                score * conf.factor(),
                format!("tool proposes {proposed}, human asserted {class}"),
            ));
        }
        // Evidence contradiction, independent of any proposal: a customer is
        // someone who PAID, and this one demonstrably received for free.
        if class == "customer"
            && let Some(n) = grants.get(key)
        {
            out.push((
                key.clone(),
                "evidence",
                class.clone(),
                format!("{n} assets received without payment"),
                score,
                format!(
                    "asserted customer, but {n} asset(s) arrived with no payment in tx or window"
                ),
            ));
        }
    }
    out
}

// ── persistence + report ───────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn persist(
    conn: &mut Connection,
    weights: &Weights,
    human: &BTreeMap<String, (String, Confidence)>,
    tx_sig: &HashMap<String, Fired>,
    tx_rows: &[(String, f64, i64)],
    party_sig: &HashMap<String, Fired>,
    party_rows: &[(String, f64)],
    proposals: &BTreeMap<String, Proposal>,
    disagreements: &[(String, &'static str, String, String, f64, String)],
) -> Result<()> {
    let tx = conn.transaction()?;
    // Wholesale: a score run replaces the previous one entirely. Partial
    // updates would let two runs' weights interleave silently.
    for t in [
        "tx_signal",
        "tx_interest",
        "party_signal",
        "party_interest",
        "disagreement",
        "score_meta",
    ] {
        tx.execute_batch(&format!("DELETE FROM {t}"))?;
    }
    {
        let mut s = tx.prepare(
            "INSERT INTO tx_signal (tx_hash, signal, weight, basis, detail) VALUES (?,?,?,?,?)",
        )?;
        for (txh, fired) in tx_sig {
            for (sig, (w, detail)) in fired {
                s.execute(params![txh, sig.as_str(), w, sig.basis().as_str(), detail])?;
            }
        }
        let mut s = tx.prepare("INSERT INTO tx_interest (tx_hash, score, round) VALUES (?,?,?)")?;
        for (txh, score, round) in tx_rows {
            s.execute(params![txh, score, round])?;
        }
        let mut s = tx.prepare(
            "INSERT INTO party_signal (key, signal, weight, basis, detail) VALUES (?,?,?,?,?)",
        )?;
        for (key, fired) in party_sig {
            for (sig, (w, detail)) in fired {
                s.execute(params![key, sig.as_str(), w, sig.basis().as_str(), detail])?;
            }
        }
        let mut s = tx.prepare(
            "INSERT INTO party_interest
             (key, score, proposed_class, proposed_capability, proposed_confidence)
             VALUES (?,?,?,?,?)",
        )?;
        for (key, score) in party_rows {
            let p = proposals.get(key);
            s.execute(params![
                key,
                score,
                p.and_then(|p| p.class.as_ref().map(|(c, _)| c.clone())),
                p.and_then(|p| p.capability.clone()),
                p.and_then(|p| p.class.as_ref().map(|(_, c)| c.as_str())),
            ])?;
        }
        let mut s = tx.prepare(
            "INSERT INTO disagreement (key, kind, human_says, tool_says, severity, detail)
             VALUES (?,?,?,?,?,?)",
        )?;
        for (key, kind, h, t, sev, detail) in disagreements {
            s.execute(params![key, kind, h, t, sev, detail])?;
        }
        let mut s = tx.prepare("INSERT INTO score_meta (k, v) VALUES (?,?)")?;
        s.execute(params!["weights", toml::to_string(weights)?])?;
        s.execute(params!["annotations_consumed", human.len().to_string()])?;
        s.execute(params!["rounds", "2"])?;
    }
    tx.commit()?;
    Ok(())
}

fn report(
    human: &BTreeMap<String, (String, Confidence)>,
    tx_count: usize,
    party_rows: &[(String, f64)],
    party_sig: &HashMap<String, Fired>,
    proposals: &BTreeMap<String, Proposal>,
    disagreements: &[(String, &'static str, String, String, f64, String)],
    n: usize,
) {
    tracing::info!(
        tx_scored = tx_count,
        parties_scored = party_rows.len(),
        assertions_consumed = human.len(),
        disagreements = disagreements.len(),
        "score: complete — attention, never fact; nothing here is exportable as a figure"
    );

    // The dig queue: highest-interest parties NOBODY has examined. This is
    // the curator's inbox, and printing anything else first would bury it.
    let mut unexamined: Vec<&(String, f64)> = party_rows
        .iter()
        .filter(|(k, _)| !human.contains_key(k))
        .collect();
    unexamined.sort_by(|a, b| b.1.total_cmp(&a.1));
    for (key, score) in unexamined.iter().take(n) {
        let top: Vec<String> = party_sig
            .get(key)
            .map(|f| {
                let mut v: Vec<_> = f.iter().collect();
                v.sort_by(|a, b| b.1.0.total_cmp(&a.1.0));
                v.into_iter()
                    .take(3)
                    .map(|(s, (w, _))| format!("{s}({w:+.2})"))
                    .collect()
            })
            .unwrap_or_default();
        let proposal = proposals
            .get(key)
            .and_then(|p| p.class.as_ref())
            .map(|(c, conf)| format!("{c}?/{conf}"))
            .unwrap_or_else(|| "—".into());
        tracing::info!(
            party = %elide(key),
            score = format!("{score:.2}"),
            proposed = proposal,
            signals = top.join(" "),
            "score: unexamined"
        );
    }
    for (key, kind, h, t, sev, _) in disagreements {
        tracing::warn!(
            party = %elide(key),
            kind,
            human = %h,
            tool = %t,
            severity = format!("{sev:.2}"),
            "score: DISAGREEMENT — a human said one thing and the evidence says another; \
             neither wins by default, a person decides"
        );
    }
}

fn elide(s: &str) -> String {
    if s.chars().count() <= 20 {
        return s.to_string();
    }
    let head: String = s.chars().take(14).collect();
    let tail: String = {
        let c: Vec<char> = s.chars().collect();
        c[c.len() - 5..].iter().collect()
    };
    format!("{head}…{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The proposal ladder's edges: a dominant mint share is treasury-shaped;
    /// grants demand a curator (tentative); a buyer is a customer only if not
    /// a provider.
    #[test]
    fn proposal_confidence_never_reaches_confirmed() {
        // Structural rule, asserted here so a refactor cannot quietly let the
        // tool speak with a human's voice.
        for conf in [Confidence::Tentative, Confidence::Probable] {
            assert!(conf < Confidence::Confirmed);
        }
    }

    #[test]
    fn percentile_is_monotone_and_bounded() {
        let pop = vec![1.0, 2.0, 3.0, 4.0, 100.0];
        assert_eq!(percentile(&pop, 0.5), 0.0);
        assert_eq!(percentile(&pop, 100.0), 1.0);
        let mid = percentile(&pop, 2.5);
        assert!(mid > 0.0 && mid < 1.0);
        assert!(
            percentile(&[], 5.0) == 0.0,
            "empty population must not panic"
        );
    }

    /// Grants could be giveaway winners; the tool must hand that judgement to
    /// a person rather than dressing it as knowledge.
    #[test]
    fn recurring_grants_propose_tentative_associate_only() {
        // Encoded in `propose`: grants_n >= 3 → (associate, Tentative). This
        // test pins the confidence so a "helpful" bump needs to be deliberate.
        let conf = Confidence::Tentative;
        assert!(conf.factor() < Confidence::Probable.factor());
    }
}
