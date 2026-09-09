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
    /// WHO this party is. Identity only — it does not decide the boundary on
    /// its own; see [`WalletDecl::is_project_side`].
    pub role: String,
    /// What a `contractor` was engaged to do: `dev` | `art` | `marketing` |
    /// `moderation`. Free-form and purely for grouping in reports — the 15%
    /// ops·tools·team bucket covers all of them, so this never affects
    /// attribution, only presentation.
    pub function: Option<String>,
    /// Override the boundary that `role` implies. `"project"` | `"external"`.
    ///
    /// Exists because identity and side are genuinely independent: a founder
    /// who also operates the treasury wallet is `role = "founder"` on a wallet
    /// that really is project-side. Rare, and it must be STATED rather than
    /// inferred, which is the whole point of making it explicit.
    pub side: Option<String>,
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

/// Roles that name a party WITHOUT placing it inside the boundary.
///
/// `founder` and `contractor` are the load-bearing additions, and both are
/// external BY DEFAULT. That is not a judgement about anyone's honesty — it
/// follows from what the boundary means. Value reaching a principal or a paid
/// contractor personally has *left* the project, whatever the project calls
/// that person.
///
/// Getting this backwards erases the finding rather than weakening it. On
/// Mekka S2, `$jprigs33` received 105 assets for no consideration; had
/// `founder` been project-side those transfers would have netted out inside
/// the perimeter and shown as internal movement. The extraction would not have
/// been understated — it would have been invisible.
const EXTERNAL_ROLES: [&str; 5] = ["founder", "contractor", "external", "customer", "partner"];

impl WalletDecl {
    fn norm(s: &str) -> String {
        s.trim().to_ascii_lowercase()
    }

    /// The identity, normalised.
    pub fn role_key(&self) -> String {
        Self::norm(&self.role)
    }

    /// What a contractor was engaged to do, normalised. `None` for every other
    /// role — a `function` on a treasury wallet means nothing and is dropped
    /// rather than stored as a misleading grouping key.
    pub fn function_key(&self) -> Option<String> {
        (self.role_key() == "contractor")
            .then(|| self.function.as_deref().map(Self::norm))
            .flatten()
    }

    /// Does this declaration place the wallet inside the project boundary?
    ///
    /// **Identity and side are separate questions**, and this is where they
    /// meet. `role` says who a party is; the side says whether value reaching
    /// them has left the project. Usually the role implies the side, so the
    /// side is derived — but an explicit `side` always wins, because the
    /// exceptions are real (a founder who also runs the treasury wallet) and
    /// must be stated rather than guessed at.
    ///
    /// Case- and whitespace-insensitive, and an UNRECOGNISED role is reported
    /// by [`WalletDecl::unknown_role`] rather than silently answering "no".
    /// A registry typo that quietly means "not the project" is precisely the
    /// silent failure this codebase keeps having to relearn.
    pub fn is_project_side(&self) -> bool {
        match self.side.as_deref().map(Self::norm).as_deref() {
            Some("project") => true,
            Some("external") => false,
            // An unrecognised `side` is NOT silently ignored — falling through
            // to the role would answer a question the operator was trying to
            // override. `unknown_side` reports it and the safe answer stands.
            _ => PROJECT_SIDE_ROLES.contains(&self.role_key().as_str()),
        }
    }

    /// True when the side was STATED rather than inferred from the role.
    /// Worth logging: an explicit override on a `founder` moves a principal
    /// inside the perimeter, which is exactly the change that can make an
    /// extraction disappear.
    pub fn side_is_explicit(&self) -> bool {
        matches!(
            self.side.as_deref().map(Self::norm).as_deref(),
            Some("project") | Some("external")
        )
    }

    /// The role string when it matches nothing known — for a startup warning.
    /// Roles in either the project-side or the external set are legitimate and
    /// are not reported.
    pub fn unknown_role(&self) -> Option<&str> {
        let r = self.role_key();
        (!PROJECT_SIDE_ROLES.contains(&r.as_str()) && !EXTERNAL_ROLES.contains(&r.as_str()))
            .then_some(self.role.as_str())
    }

    /// The `side` string when it was given but is not one we understand.
    /// Reported separately from `unknown_role` because the failure is worse:
    /// a typo here means the operator TRIED to state the boundary and the tool
    /// quietly used the role's default instead.
    pub fn unknown_side(&self) -> Option<&str> {
        let s = self.side.as_deref()?;
        (!matches!(Self::norm(s).as_str(), "project" | "external")).then_some(s)
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
    /// WHO this party is — optional, and orthogonal to being terminal.
    ///
    /// Terminal answers "may this party recruit others into the frontier?";
    /// the role answers "who are they?". `$jprigs33` is `founder` AND terminal:
    /// a principal whose own funding history should be booked in full, but who
    /// must never pull their personal social graph into the watch set.
    ///
    /// Without this a founder can only be named by declaring them a
    /// `[[wallet]]`, which makes them EXPAND — and on Mekka S1 seeding two such
    /// wallets took the frontier from 180 parties to 6,424. Naming and
    /// expanding must be separable, or the safe option is also the anonymous
    /// one.
    pub role: Option<String>,
    /// What a `contractor` was engaged to do. Present here as well as on
    /// `[[wallet]]` because a contractor usually wants naming WITHOUT
    /// expansion: `$predlings` has ~1,883 transactions of unrelated personal
    /// activity, and seating that as an expanding member would recruit all of
    /// it to learn one retainer figure.
    pub function: Option<String>,
    /// Required: who says so.
    pub source: String,
}

impl TerminalParty {
    /// The declared identity, normalised. A terminal party is never
    /// project-side — it is recorded on contact precisely because it sits
    /// outside — so there is no `side` here to get wrong.
    pub fn role_key(&self) -> Option<String> {
        self.role.as_deref().map(WalletDecl::norm)
    }

    /// Function, normalised, and only for a `contractor` — same rule as
    /// [`WalletDecl::function_key`].
    pub fn function_key(&self) -> Option<String> {
        (self.role_key().as_deref() == Some("contractor"))
            .then(|| self.function.as_deref().map(WalletDecl::norm))
            .flatten()
    }

    /// A role string that matches nothing known, for a startup warning.
    pub fn unknown_role(&self) -> Option<&str> {
        let r = self.role_key()?;
        (!PROJECT_SIDE_ROLES.contains(&r.as_str()) && !EXTERNAL_ROLES.contains(&r.as_str()))
            .then_some(self.role.as_deref().unwrap_or_default())
    }
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
        let w = decl;
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

    /// Naming a party and bounding it are DIFFERENT questions.
    ///
    /// `founder` and `contractor` identify who someone is; neither puts them
    /// inside the perimeter. Value reaching a principal or a paid contractor
    /// personally has left the project, whatever the project calls them.
    ///
    /// The consequence if this regresses is not a smaller number, it is a
    /// missing one: on Mekka S2 `$jprigs33` took 105 assets for no
    /// consideration, and a project-side `founder` would net those transfers
    /// out inside the perimeter and render the extraction invisible.
    #[test]
    fn founder_and_contractor_name_a_party_without_bounding_it() {
        for r in ["founder", "FOUNDER", " Contractor ", "contractor"] {
            assert!(
                !decl(r).is_project_side(),
                "{r} is an identity, not a place inside the boundary"
            );
            assert_eq!(decl(r).unknown_role(), None, "{r} is a recognised role");
        }
    }

    /// The exception has to be statable, and stated — a founder who also runs
    /// the treasury wallet is real. An explicit `side` overrides the role's
    /// default in both directions, and reports itself as explicit so the
    /// override can be logged rather than applied silently.
    #[test]
    fn an_explicit_side_overrides_the_role_default_in_both_directions() {
        let sided = |role: &str, side: &str| WalletDecl {
            side: Some(side.into()),
            ..decl(role)
        };

        assert!(sided("founder", "project").is_project_side());
        assert!(sided("founder", "project").side_is_explicit());
        assert!(!sided("treasury", "external").is_project_side());
        assert!(!decl("founder").side_is_explicit(), "derived, not stated");
    }

    /// A typo in `side` is worse than a typo in `role`: the operator was
    /// TRYING to state the boundary. Falling through to the role's default
    /// would answer the very question they were overriding, so the bad value
    /// is reported and the safe answer stands.
    #[test]
    fn an_unrecognised_side_is_reported_and_does_not_silently_apply() {
        let typo = WalletDecl {
            side: Some("projekt".into()),
            ..decl("founder")
        };
        assert_eq!(typo.unknown_side(), Some("projekt"));
        assert!(!typo.side_is_explicit());
        assert!(
            !typo.is_project_side(),
            "an unusable override must not quietly move a founder inside"
        );
        assert_eq!(decl("founder").unknown_side(), None, "absent is not a typo");
    }

    /// `function` groups contractor pay for reporting and means nothing
    /// anywhere else. Dropping it off non-contractors keeps it from becoming a
    /// grouping key that quietly splits the treasury into fictional buckets.
    #[test]
    fn function_is_kept_for_contractors_and_dropped_elsewhere() {
        let f = |role: &str, function: &str| WalletDecl {
            function: Some(function.into()),
            ..decl(role)
        };
        assert_eq!(
            f("contractor", "Moderation").function_key().as_deref(),
            Some("moderation")
        );
        assert_eq!(f("treasury", "dev").function_key(), None);
        assert_eq!(
            decl("contractor").function_key(),
            None,
            "absent stays absent"
        );
    }

    use super::*;

    /// A minimal declaration; tests vary one field via struct update.
    fn decl(role: &str) -> WalletDecl {
        WalletDecl {
            stake: "stake1x".into(),
            label: "l".into(),
            role: role.into(),
            function: None,
            side: None,
            source: "s".into(),
        }
    }

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
