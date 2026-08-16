//! Minimal blocking Koios client — the two calls this tool makes to an indexer,
//! and no more:
//!
//! - `/policy_asset_info` at seed time (the mint floor + the asset list the
//!   walk is reconciled against at the end);
//! - `/utxo_info` as the LAST rung of the input-resolution ladder (each ref
//!   fetched once, ever — write-through to `outref_cache`).
//!
//! Typed request bodies (project rule: no `serde_json::json!`), typed rows.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const DEFAULT_BASE: &str = "https://api.koios.rest/api/v1";

pub struct Koios {
    base: String,
    http: reqwest::blocking::Client,
    token: Option<String>,
}

#[derive(Serialize)]
struct UtxoRefsReq<'a> {
    #[serde(rename = "_utxo_refs")]
    utxo_refs: &'a [String],
    #[serde(rename = "_extended")]
    extended: bool,
}

/// `/utxo_info` row (the fields we keep).
#[derive(Debug, Clone, Deserialize)]
pub struct UtxoInfo {
    pub tx_hash: String,
    pub tx_index: u32,
    pub address: String,
    /// Lovelace — Koios sends it as a string.
    pub value: String,
    #[serde(default)]
    pub asset_list: Option<Vec<UtxoAsset>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct UtxoAsset {
    pub policy_id: String,
    pub asset_name: String,
    pub quantity: String,
}

/// `/policy_asset_info` row (the fields we keep).
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // the reconciliation step will read mint_cnt/burn_cnt per asset
pub struct PolicyAsset {
    pub asset_name: Option<String>,
    pub fingerprint: Option<String>,
    pub minting_tx_hash: Option<String>,
    pub total_supply: Option<String>,
    pub mint_cnt: Option<i64>,
    pub burn_cnt: Option<i64>,
    /// Unix seconds.
    pub creation_time: Option<i64>,
}

impl Koios {
    pub fn new(base: Option<String>, token: Option<String>) -> Result<Self> {
        Ok(Self {
            base: base.unwrap_or_else(|| DEFAULT_BASE.to_owned()),
            http: reqwest::blocking::Client::builder()
                .user_agent("project-ledger/0.0.1")
                .timeout(std::time::Duration::from_secs(60))
                .build()
                .context("building http client")?,
            token,
        })
    }

    fn auth(&self, r: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
        match &self.token {
            Some(t) => r.bearer_auth(t),
            None => r,
        }
    }

    /// Resolve up to 100 refs (`tx_hash#idx`) in one POST.
    pub fn utxo_info(&self, refs: &[String]) -> Result<Vec<UtxoInfo>> {
        if refs.is_empty() {
            return Ok(Vec::new());
        }
        if refs.len() > 100 {
            bail!("utxo_info: batch of {} exceeds 100", refs.len());
        }
        let url = format!("{}/utxo_info", self.base);
        let resp = self
            .auth(self.http.post(&url))
            .json(&UtxoRefsReq {
                utxo_refs: refs,
                extended: true,
            })
            .send()
            .context("koios utxo_info")?;
        let status = resp.status();
        if !status.is_success() {
            bail!("koios utxo_info: HTTP {status}");
        }
        resp.json().context("koios utxo_info body")
    }

    /// Every asset ever minted under a policy (paginated, 1000/page).
    pub fn policy_asset_info(&self, policy_hex: &str) -> Result<Vec<PolicyAsset>> {
        let mut out = Vec::new();
        let mut offset = 0usize;
        loop {
            let url = format!(
                "{}/policy_asset_info?_asset_policy={policy_hex}&offset={offset}&limit=1000",
                self.base
            );
            let resp = self
                .auth(self.http.get(&url))
                .send()
                .context("koios policy_asset_info")?;
            let status = resp.status();
            if !status.is_success() {
                bail!("koios policy_asset_info: HTTP {status}");
            }
            let page: Vec<PolicyAsset> = resp.json().context("koios policy_asset_info body")?;
            let n = page.len();
            out.extend(page);
            if n < 1000 {
                break;
            }
            offset += n;
        }
        Ok(out)
    }
}
