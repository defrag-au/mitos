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
pub struct WalletDecl {
    /// `stake1…` (the party key). Enterprise addresses are not declarable as
    /// project wallets — a stakeless address is terminal by shape.
    pub stake: String,
    pub label: String,
    pub role: String,
    /// Required: who says so.
    pub source: String,
}

/// Roles that mean **the project owns this wallet**, so value arriving here
/// has come back and value leaving here has crossed the boundary.
///
/// A closed set, deliberately. The alternative — treat anything that is not
/// obviously external as the project's — gets the default wrong in the
/// dangerous direction, because a mistaken `project_side` launders an
/// extraction into a deployment.
const PROJECT_SIDE_ROLES: [&str; 6] = ["treasury", "mint", "holding", "vault", "ops", "project"];

impl WalletDecl {
    /// Does this declaration place the wallet inside the project boundary?
    ///
    /// Case- and whitespace-insensitive, and an UNRECOGNISED role is reported
    /// by [`WalletDecl::unknown_role`] rather than silently answering "no".
    /// A registry typo that quietly means "not the project" is precisely the
    /// silent failure this codebase keeps having to relearn.
    pub fn is_project_side(&self) -> bool {
        let r = self.role.trim().to_ascii_lowercase();
        PROJECT_SIDE_ROLES.contains(&r.as_str())
    }

    /// The role string when it matches nothing known — for a startup warning.
    /// `external` and `customer` are legitimate non-project roles and are not
    /// reported.
    pub fn unknown_role(&self) -> Option<&str> {
        let r = self.role.trim().to_ascii_lowercase();
        (!PROJECT_SIDE_ROLES.contains(&r.as_str())
            && !matches!(r.as_str(), "external" | "customer" | "partner"))
        .then_some(self.role.as_str())
    }
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

    /// Stake keys the OPERATOR declared terminal in the TOML. Exactly that —
    /// see [`Self::terminal_parties`] for the set the frontier is built with.
    pub fn declared_terminal(&self) -> impl Iterator<Item = Party> + '_ {
        self.terminal
            .parties
            .iter()
            .map(|t| Party::cardano_stake(t.stake.clone()))
    }

    /// Every party that must be recorded but never expanded: the operator's
    /// declarations PLUS every shared service the address registry knows.
    ///
    /// The registry half is what stops this being per-project busywork. A
    /// minting provider takes its fee inside the mint transaction of every
    /// project it serves, so the mint decode seats it as a payee on every
    /// ledger. Expanded, it drags in the provider's OTHER clients: Anvil's fee
    /// wallet alone holds over a thousand unspent fee UTxOs from unrelated
    /// collections. Knowing it once, centrally, means each new collection gets
    /// the guard for free instead of waiting for someone to notice.
    pub fn terminal_parties(&self) -> impl Iterator<Item = Party> + '_ {
        self.declared_terminal().chain(
            address_registry::STAKE_REGISTRY
                .keys()
                .map(|s| Party::cardano_stake((*s).to_string())),
        )
    }

    /// Known shared services seated automatically, with their registry labels.
    pub fn registry_services() -> impl Iterator<Item = (String, &'static str)> {
        address_registry::STAKE_REGISTRY
            .entries()
            .map(|(k, v)| ((*k).to_string(), v.label))
    }
}

#[cfg(test)]
mod tests {

    /// The role string was DEAD CODE for weeks (`#[allow(dead_code)]`, "surfaced
    /// by export"). Now it decides the project boundary, so a typo has to be
    /// loud: an unrecognised role means NOT the project, which is the safe
    /// answer, but silence about it is how a treasury goes unrecorded.
    #[test]
    fn a_wallet_role_decides_the_project_boundary_and_a_typo_is_reported() {
        let w = |role: &str| WalletDecl {
            stake: "stake1x".into(),
            label: "l".into(),
            role: role.into(),
            source: "s".into(),
        };
        for r in [
            "treasury",
            "TREASURY",
            "  Treasury ",
            "mint",
            "holding",
            "vault",
        ] {
            assert!(w(r).is_project_side(), "{r} is the project's own wallet");
            assert_eq!(w(r).unknown_role(), None);
        }
        // Legitimately outside, and not worth a warning.
        for r in ["external", "customer", "partner"] {
            assert!(!w(r).is_project_side());
            assert_eq!(w(r).unknown_role(), None);
        }
        // A typo: outside the boundary (safe) AND reported (loud).
        assert!(!w("tresury").is_project_side());
        assert_eq!(w("tresury").unknown_role(), Some("tresury"));
    }

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

    /// A registry that declares nothing still guards against the shared
    /// minting providers, or every new collection re-learns that its mint
    /// provider is not part of the project.
    #[test]
    fn known_services_are_terminal_even_when_the_toml_declares_none() {
        let r = Registry::parse(EXAMPLE).unwrap();
        assert_eq!(r.declared_terminal().count(), 0, "the TOML declares none");
        assert!(
            r.terminal_parties().count() > 0,
            "but the address registry's shared services are still seated terminal"
        );
        let anvil = "stake1uy50zl7a9k9c74v66c0gn833at5sh83qnjldk8hg4rrv05g3mmskr";
        assert!(
            r.terminal_parties().any(|p| p.key == anvil),
            "Anvil (the mint provider) must never be expandable"
        );
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
