//! Per-asset provenance — which units the project minted to itself, and where
//! each one sits now.
//!
//! ## Why this is an asset-level artifact and not a number
//!
//! Every other figure in this tool is an aggregate: *the team minted 14.7% of
//! supply*. That is the right shape for judging a project and the wrong shape
//! for the question an individual holder actually asks, which is **"is the one
//! I own part of that?"** Nobody can answer it from a percentage, and the
//! answer matters to them in a way the aggregate does not.
//!
//! It also changes who can check the work. A share is something a reader takes
//! on trust; a list of asset names with a current holder against each is
//! something they can verify against their own wallet in a few seconds. That
//! is the strongest form this evidence takes.
//!
//! ## "Tainted" is a claim about ORIGIN, never about the holder
//!
//! An asset is tainted here if the wallet that minted it was funded by the
//! project — nothing more. The overwhelming majority have since been **bought,
//! in good faith, at market price, by people with no connection to the
//! project**: on Mekka S1, 608 of 749 sit with the public.
//!
//! So the flag must never be rendered as a mark against the holder. It says
//! the project manufactured this unit rather than selling it, and that the
//! buyer paid the project's own supply-inflation. The holder is the injured
//! party, not the culprit — a UI that shades a wallet red gets the moral
//! direction exactly backwards, which is why `holder_is_project` is carried
//! separately and why the wording here is deliberate.

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::store::Ledger;

/// Bump when a consumer would have to change.
pub const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, clap::Args)]
pub struct TaintedArgs {
    #[arg(long, default_value = "project-ledger.db")]
    pub db: PathBuf,
    /// Write the asset-level provenance artifact here.
    #[arg(long)]
    pub export: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct TaintedExport {
    pub schema_version: u32,
    pub policy_id: String,
    pub generated_unix: u64,
    pub summary: Summary,
    /// What `tainted` does and does not mean, travelling WITH the data.
    /// A consumer that renders the flag without this is misrepresenting it.
    pub definition: Definition,
    pub assets: Vec<TaintedAsset>,
    /// Pre-aggregated per current holder, so a lookup UI needs no scan.
    pub holders: Vec<HolderRollup>,
}

#[derive(Debug, Serialize)]
pub struct Definition {
    pub tainted: &'static str,
    pub not_an_accusation: &'static str,
    pub basis: &'static str,
}

#[derive(Debug, Serialize)]
pub struct Summary {
    pub total_assets: i64,
    pub tainted_assets: i64,
    pub tainted_share: f64,
    /// Tainted units that have moved on to wallets with no declared project
    /// role — the people this artifact exists for.
    pub tainted_with_public: i64,
    pub tainted_with_project: i64,
    pub public_wallets_holding_tainted: i64,
}

#[derive(Debug, Serialize)]
pub struct TaintedAsset {
    /// Hex asset name — the identity. A display name can change; this cannot.
    pub asset_name: String,
    pub minter: String,
    pub minter_label: Option<String>,
    /// The transaction that minted it. A label is an assertion; this is the
    /// receipt — it lets a holder check the claim against an explorer rather
    /// than taking the flag on trust, which is the whole point of publishing
    /// per-asset rather than a percentage.
    pub mint_tx: String,
    pub mint_tx_url: String,
    /// `project_wallet` (the project's own) or `funded_front` (a wallet whose
    /// funding traced back to the project through up to two intermediaries).
    /// Kept apart because the second is an inference and the first is not.
    pub taint_basis: Option<&'static str>,
    pub tainted: bool,
    pub holder: Option<String>,
    pub holder_label: Option<String>,
    /// Set when the CURRENT holder is the project. The distinction the whole
    /// module turns on: still-held is the team's allocation, sold-on is a
    /// member of the public holding a unit the team manufactured.
    pub holder_is_project: bool,
}

#[derive(Debug, Serialize)]
pub struct HolderRollup {
    pub holder: String,
    pub label: Option<String>,
    pub declared_role: Option<String>,
    pub assets_held: i64,
    pub tainted_held: i64,
}

/// Is the holder the PROJECT, as opposed to merely someone the project named?
///
/// `declared_role IS NOT NULL` is the wrong test and was the first thing tried:
/// it counted `$mekkacrow`, declared explicitly as a **genuine customer**, as
/// the project holding its own supply. Being identified in this ledger is not
/// the same as being on the project's side of it — most declared parties are
/// counterparties, and a customer is exactly the person this artifact exists to
/// protect from the label.
///
/// `contractor` is excluded too, and that is a judgement rather than an
/// oversight: someone paid in units is a supplier who took payment in kind,
/// not the project warehousing supply. The rollup keeps `declared_role`, so a
/// consumer that disagrees can split them differently without re-running.
fn is_project_role(role: Option<&str>) -> bool {
    matches!(
        role,
        Some("founder") | Some("ops") | Some("treasury") | Some("mint")
    )
}

pub fn run(args: &TaintedArgs) -> Result<()> {
    let ledger = Ledger::open(&args.db)?;
    let conn = ledger.conn();
    let policy_id: String = conn
        .query_row(
            "SELECT value FROM walk_meta WHERE key='policy_id'",
            [],
            |r| r.get(0),
        )
        .unwrap_or_default();

    // The tainted set, with WHICH of the two bases applies. A project wallet
    // minting is observed; a funded front is an inference from `provenance`,
    // and collapsing them would let the weaker basis inherit the stronger's
    // authority — the same separation `counterpart_basis` exists for.
    let mut assets = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT e.asset_name, e.to_party, mp.label, h.party, hp.label, hp.declared_role, e.tx_hash,
                CASE WHEN e.to_party IN (SELECT key FROM party WHERE project_side = 1)
                     THEN 'project_wallet'
                     WHEN e.to_party IN (SELECT holder FROM provenance_verdict WHERE flagged = 1)
                     THEN 'funded_front' END
           FROM asset_event e
           LEFT JOIN party mp ON mp.key = e.to_party
           LEFT JOIN asset_holder h ON h.asset_name = e.asset_name
           LEFT JOIN party hp ON hp.key = h.party
          WHERE e.kind = 'mint' AND e.asset_class = 'nft'
          ORDER BY e.asset_name",
    )?;
    for row in stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, Option<String>>(5)?,
            r.get::<_, String>(6)?,
            r.get::<_, Option<String>>(7)?,
        ))
    })? {
        let (asset_name, minter, minter_label, holder, holder_label, holder_role, mint_tx, basis) =
            row?;
        let taint_basis = match basis.as_deref() {
            Some("project_wallet") => Some("project_wallet"),
            Some("funded_front") => Some("funded_front"),
            _ => None,
        };
        assets.push(TaintedAsset {
            asset_name,
            minter,
            minter_label,
            mint_tx_url: format!("https://cardanoscan.io/transaction/{mint_tx}"),
            mint_tx,
            tainted: taint_basis.is_some(),
            taint_basis,
            holder_is_project: is_project_role(holder_role.as_deref()),
            holder,
            holder_label,
        });
    }

    let mut holders = Vec::new();
    let mut hstmt = conn.prepare(
        "SELECT h.party, p.label, p.declared_role, COUNT(*),
                SUM(CASE WHEN e.to_party IN (SELECT key FROM party WHERE project_side = 1)
                          OR e.to_party IN (SELECT holder FROM provenance_verdict WHERE flagged = 1)
                         THEN 1 ELSE 0 END)
           FROM asset_holder h
           JOIN asset_event e ON e.asset_name = h.asset_name
                             AND e.kind = 'mint' AND e.asset_class = 'nft'
           LEFT JOIN party p ON p.key = h.party
          GROUP BY h.party ORDER BY 5 DESC, 4 DESC",
    )?;
    for row in hstmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, i64>(3)?,
            r.get::<_, i64>(4)?,
        ))
    })? {
        let (holder, label, declared_role, assets_held, tainted_held) = row?;
        holders.push(HolderRollup {
            holder,
            label,
            declared_role,
            assets_held,
            tainted_held,
        });
    }

    let total_assets = assets.len() as i64;
    let tainted_assets = assets.iter().filter(|a| a.tainted).count() as i64;
    let tainted_with_project = assets
        .iter()
        .filter(|a| a.tainted && a.holder_is_project)
        .count() as i64;
    let public_wallets_holding_tainted = holders
        .iter()
        .filter(|h| h.tainted_held > 0 && !is_project_role(h.declared_role.as_deref()))
        .count() as i64;

    let export = TaintedExport {
        schema_version: SCHEMA_VERSION,
        policy_id,
        generated_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default(),
        summary: Summary {
            total_assets,
            tainted_assets,
            tainted_share: if total_assets > 0 {
                tainted_assets as f64 / total_assets as f64
            } else {
                0.0
            },
            tainted_with_public: tainted_assets - tainted_with_project,
            tainted_with_project,
            public_wallets_holding_tainted,
        },
        definition: Definition {
            tainted: "The wallet that MINTED this unit was funded by the project — either a \
                      wallet the project holds outright, or one whose funding traces back to \
                      the project. It is a statement about the unit's origin only.",
            not_an_accusation: "It is NOT a mark against whoever holds the unit now. Most \
                                tainted units were bought at market price by people with no \
                                connection to the project, who are the injured party rather \
                                than the cause: they paid for supply the project manufactured \
                                rather than sold. Do not shade a holder's wallet as suspect.",
            basis: "`project_wallet` is OBSERVED — the minting wallet is the project's. \
                    `funded_front` is INFERRED from provenance, following funding through up \
                    to two intermediaries, and is the weaker of the two. Never merge them.",
        },
        assets,
        holders,
    };

    // Compact, not pretty. One row per asset means pretty-printing nearly
    // doubles the file for whitespace a frontend never reads, and this ships
    // over the wire to anyone checking their own wallet.
    let json = serde_json::to_string(&export)?;
    std::fs::write(&args.export, &json)
        .with_context(|| format!("writing {}", args.export.display()))?;
    tracing::info!(
        path = %args.export.display(),
        schema = SCHEMA_VERSION,
        total = export.summary.total_assets,
        tainted = export.summary.tainted_assets,
        with_public = export.summary.tainted_with_public,
        public_wallets = export.summary.public_wallets_holding_tainted,
        "tainted: asset provenance written"
    );
    Ok(())
}
