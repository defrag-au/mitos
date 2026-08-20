//! The registry — declares only what the chain cannot state.
//!
//! One policy (the entry point), the wallets an operator asserts are "the
//! project's money" (each with a `source`, because that is an assertion), the
//! custodial-scale thresholds, and any parties asserted to be custodial. The
//! royalty address and the policy signer are NOT declarable: both are observed
//! during the walk (CIP-27 `777.addr`; the mint script's `sig` credential).

use std::path::Path;

use anyhow::{Context, Result, bail};
use chain_ledger::{Party, Thresholds};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Registry {
    pub project: String,
    #[serde(rename = "policy")]
    pub policies: Vec<PolicyDecl>,
    #[serde(default, rename = "wallet")]
    pub wallets: Vec<WalletDecl>,
    #[serde(default)]
    pub terminal: TerminalDecl,
}

#[derive(Debug, Deserialize)]
pub struct PolicyDecl {
    /// Hex policy id (28 bytes).
    pub id: String,
    pub label: String,
    /// Optional walk-floor override (absolute slot). Recorded as
    /// `floor_source = declared` — an assertion, not an observation.
    pub floor: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // `role` is surfaced by export
pub struct WalletDecl {
    /// `stake1…` (the party key). Enterprise addresses are not declarable as
    /// project wallets — a stakeless address is terminal by shape.
    pub stake: String,
    pub label: String,
    pub role: String,
    /// Required: who says so.
    pub source: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct TerminalDecl {
    pub receipts: Option<u32>,
    pub counterparties: Option<u32>,
    /// New distinct wallets paid per window, above which the window is hot.
    /// Omit to take the measured default — see `Thresholds`.
    pub payees_per_window: Option<u32>,
    /// Window length in slots (Cardano slot ≈ 1s, so 86,400 ≈ a day).
    pub payee_window_slots: Option<u64>,
    /// Hot windows required before freezing. Above 1 so a one-off airdrop
    /// burst is not mistaken for a payout service.
    pub payee_hot_windows: Option<u32>,
    #[serde(default, rename = "party")]
    pub parties: Vec<TerminalParty>,
}

#[derive(Debug, Deserialize)]
pub struct TerminalParty {
    pub stake: String,
    pub label: String,
    pub source: String,
}

impl Registry {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading registry {}", path.display()))?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let r: Registry = toml::from_str(text).context("parsing registry TOML")?;
        r.validate()?;
        Ok(r)
    }

    fn validate(&self) -> Result<()> {
        if self.policies.len() != 1 {
            bail!(
                "registry must declare exactly one [[policy]] (got {}) — one ledger per project",
                self.policies.len()
            );
        }
        let p = &self.policies[0];
        let bytes = hex::decode(&p.id).context("policy id is not hex")?;
        if bytes.len() != 28 {
            bail!("policy id must be 28 bytes, got {}", bytes.len());
        }
        for w in &self.wallets {
            if !w.stake.starts_with("stake1") {
                bail!("[[wallet]] {} must be a stake1… address", w.label);
            }
            if w.source.trim().is_empty() {
                bail!("[[wallet]] {} needs a source — it is an assertion", w.label);
            }
        }
        for t in &self.terminal.parties {
            if t.source.trim().is_empty() {
                bail!("[[terminal.party]] {} needs a source", t.label);
            }
            if self.wallets.iter().any(|w| w.stake == t.stake) {
                bail!("{} is both a [[wallet]] and a [[terminal.party]]", t.stake);
            }
        }
        Ok(())
    }

    pub fn policy(&self) -> &PolicyDecl {
        &self.policies[0]
    }

    #[cfg(test)]
    pub fn policy_bytes(&self) -> [u8; 28] {
        let v = hex::decode(&self.policy().id).expect("validated");
        let mut out = [0u8; 28];
        out.copy_from_slice(&v);
        out
    }

    pub fn thresholds(&self) -> Thresholds {
        let d = Thresholds::default();
        Thresholds {
            receipts: self.terminal.receipts.unwrap_or(d.receipts),
            counterparties: self.terminal.counterparties.unwrap_or(d.counterparties),
            // Outbound fan-out RATE — the only measure that sees an exchange hot
            // wallet, which receives from almost nobody while paying thousands.
            payees_per_window: self
                .terminal
                .payees_per_window
                .unwrap_or(d.payees_per_window),
            payee_window_slots: self
                .terminal
                .payee_window_slots
                .unwrap_or(d.payee_window_slots),
            payee_hot_windows: self
                .terminal
                .payee_hot_windows
                .unwrap_or(d.payee_hot_windows),
        }
    }

    pub fn declared_terminal(&self) -> impl Iterator<Item = Party> + '_ {
        self.terminal
            .parties
            .iter()
            .map(|t| Party::cardano_stake(t.stake.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = include_str!("../registry.toml");

    #[test]
    fn example_registry_parses() {
        let r = Registry::parse(EXAMPLE).unwrap();
        assert_eq!(r.project, "Mekka");
        assert_eq!(r.policy_bytes().len(), 28);
        assert_eq!(r.wallets.len(), 1);
        assert_eq!(r.thresholds().receipts, 1000);
        assert_eq!(r.thresholds().counterparties, 300);
        assert_eq!(r.declared_terminal().count(), 0);
    }

    #[test]
    fn rejects_unsourced_wallet_and_terminal_overlap() {
        let unsourced = r#"
project = "x"
[[policy]]
id = "29728939434a25e57ef6a9b94ba3215508264fee665bbb35b16a2d56"
label = "p"
[[wallet]]
stake = "stake1abc"
label = "t"
role = "treasury"
source = "  "
"#;
        assert!(Registry::parse(unsourced).is_err());

        let overlap = r#"
project = "x"
[[policy]]
id = "29728939434a25e57ef6a9b94ba3215508264fee665bbb35b16a2d56"
label = "p"
[[wallet]]
stake = "stake1abc"
label = "t"
role = "treasury"
source = "me"
[[terminal.party]]
stake = "stake1abc"
label = "cex"
source = "me"
"#;
        assert!(Registry::parse(overlap).is_err());
    }

    #[test]
    fn rejects_bad_policy() {
        let short = r#"
project = "x"
[[policy]]
id = "abcd"
label = "p"
"#;
        assert!(Registry::parse(short).is_err());
    }
}
