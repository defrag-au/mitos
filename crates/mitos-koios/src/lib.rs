//! The Koios calls this workspace actually makes — and the FLOOR rule.
//!
//! Lifted out of `project-ledger` 2026-09-09, unchanged in behaviour, when
//! `token-ledger` needed the same endpoint. One client, so a schema drift is
//! fixed once; Koios's row shapes have moved under us before.
//!
//! Typed request bodies and typed rows throughout (project rule: no
//! `serde_json::json!`).
//!
//! # Why a policy's floor lives here rather than in a caller
//!
//! ⚠️ **A policy's first mint is the MINIMUM `creation_time` across its
//! assets — never the first row of the response.** That is the whole of
//! [`floor_unix`], it is three lines, and getting it wrong has already cost
//! real time:
//!
//! > RIDDLE's floor was taken from a `policy_asset_info` page rather than
//! > from the earliest asset under the policy, and came out **273,059 slots
//! > too high**. The walk then stopped short of the real mint, and
//! > `coverage_is_complete` reported COMPLETE anyway, because a floor is
//! > exactly what completeness is measured against. Only the supply invariant
//! > caught it.
//!
//! A wrong floor is the worst shape of error this pipeline has: it does not
//! fail, it produces a short archive that CALLS ITSELF COMPLETE. So the rule
//! is stated once, next to the call that returns the rows it applies to, with
//! the failure it prevents written down beside it.

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
pub struct UtxoAsset {
    pub policy_id: String,
    pub asset_name: String,
    pub quantity: String,
}

/// `/policy_asset_info` row (the fields we keep).
#[derive(Debug, Clone, Deserialize)]
pub struct PolicyAsset {
    pub asset_name: Option<String>,
    pub fingerprint: Option<String>,
    pub minting_tx_hash: Option<String>,
    pub total_supply: Option<String>,
    pub mint_cnt: Option<i64>,
    pub burn_cnt: Option<i64>,
    /// Unix seconds. ⚠️ PER ASSET — see [`floor_unix`].
    pub creation_time: Option<i64>,
}

/// The policy's first mint, in unix seconds: the EARLIEST creation across
/// every asset the policy ever minted.
///
/// ⚠️ Not `rows[0].creation_time`. Koios does not promise an order, a policy
/// mints new assets throughout its life, and a floor that is too HIGH makes a
/// short walk claim completeness — see this module's header for what that
/// cost. Assets with no `creation_time` are skipped rather than treated as
/// zero, which would drag the floor to genesis and merely make the walk slow.
///
/// `None` when the policy has no assets, or none with a creation time — the
/// caller then has no floor, which is honest and means "walk from genesis",
/// not "walk from now".
pub fn floor_unix(assets: &[PolicyAsset]) -> Option<i64> {
    assets.iter().filter_map(|a| a.creation_time).min()
}

impl Koios {
    pub fn new(base: Option<String>, token: Option<String>) -> Result<Self> {
        Ok(Self {
            base: base.unwrap_or_else(|| DEFAULT_BASE.to_owned()),
            http: reqwest::blocking::Client::builder()
                .user_agent(concat!("mitos-koios/", env!("CARGO_PKG_VERSION")))
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
    ///
    /// ⚠️ Pass the result through [`floor_unix`] rather than reading a row.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(name: &str, created: Option<i64>) -> PolicyAsset {
        PolicyAsset {
            asset_name: Some(name.to_string()),
            fingerprint: None,
            minting_tx_hash: None,
            total_supply: None,
            mint_cnt: None,
            burn_cnt: None,
            creation_time: created,
        }
    }

    /// ⚠️ THE BUG THIS FUNCTION EXISTS FOR. Koios promises no order, and a
    /// policy mints new assets throughout its life, so the first row is
    /// routinely LATER than the policy's first mint. Taking it once cost a
    /// short archive that called itself complete.
    #[test]
    fn the_floor_is_the_earliest_asset_not_the_first_row() {
        let assets = [
            asset("late", Some(1_700_000_000)),
            asset("first", Some(1_600_000_000)),
            asset("middle", Some(1_650_000_000)),
        ];
        assert_eq!(floor_unix(&assets), Some(1_600_000_000));
        // Stated explicitly: the naive read would have been this.
        assert_ne!(floor_unix(&assets), assets[0].creation_time);
    }

    /// An asset with no creation time contributes nothing. Treating it as 0
    /// would drag the floor to genesis — merely slow, but it would also mask
    /// a real floor behind a policy that has one.
    #[test]
    fn assets_without_a_creation_time_are_skipped_not_zeroed() {
        let assets = [asset("unknown", None), asset("known", Some(1_650_000_000))];
        assert_eq!(floor_unix(&assets), Some(1_650_000_000));
    }

    /// No floor is a real answer — "walk from genesis", never "walk from now".
    #[test]
    fn a_policy_with_nothing_datable_has_no_floor() {
        assert_eq!(floor_unix(&[]), None);
        assert_eq!(floor_unix(&[asset("a", None)]), None);
    }

    #[test]
    fn a_single_asset_policy_is_its_own_floor() {
        assert_eq!(
            floor_unix(&[asset("VIPER", Some(1_684_786_738))]),
            Some(1_684_786_738)
        );
    }
}
