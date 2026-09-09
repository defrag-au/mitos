//! Watched-token registry — the pluggability seam.
//!
//! A token is declarative config, not a code path. It names the
//! `(policy, asset_name)` to follow and an optional `floor_slot`, and that is
//! the whole contract.
//!
//! **A ledger watches one asset, or one whole policy.** The delta primitive in
//! `store` is `(tx, party, unit) -> signed amount`. It carried no unit until
//! 2026-09, when watching a whole policy required one: summing a policy's
//! assets into a single per-party scalar makes a tx that moves one unit out and
//! another in net to zero, hiding both moves.
//!
//! The unit on the key is what PRESERVES the per-tx conservation check that
//! makes this walk self-verifying — it becomes per unit rather than per tx.
//! Without it the check could not have been kept at all, which is why the
//! earlier restriction to a single asset was the honest position at the time.
//!
//! [`TokenEntry::asset_name`] is therefore `Option`: `None` watches the whole
//! policy, `Some("")` watches the genuinely empty-named asset. Those are
//! different requests and the type says so — the same distinction the field's
//! own docs already drew between an omitted name and an empty one.

use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Mainnet immutable-DB chunk size in slots — mirrors
/// `mitos_chain_walk::mithril::CHUNK_SLOTS`, restated so a floor can be
/// reported as a file number without pulling the bootstrap module in.
pub const CHUNK_SLOTS: u64 = 21_600;

#[derive(Debug, Deserialize)]
struct RegistryFile {
    token: Vec<TokenEntry>,
    // `sink` was here until 2026-09-08. Burn sinks moved to
    // `shared-crates/address-registry` as `ScriptCategory::Burn { evidence }`,
    // keyed by payment credential — a sink is a property of the SCRIPT, and
    // holding it per-token meant a sink was only known to the tokens somebody
    // had already registered it against. `serde` still ignores an unknown key,
    // so an old file with a `[[sink]]` table loads without complaint.
    #[serde(default)]
    lock_platform: Vec<LockPlatform>,
}

/// A lock/vesting platform recognised by payment credential.
///
/// Separate from the built-in CrowdLock recognition because the two carry
/// different evidence. CrowdLock's datum decodes, so its positions come with a
/// schedule and an owner — `basis: decoded`. A platform registered here is
/// known to be a lock (`basis: registered`) but its datum shape is not one we
/// can read, so its supply counts as **locked** with no maturity date. That is
/// deliberately the conservative reading; the alternative hands supply to the
/// float on the strength of our own decode gap.
#[derive(Debug, Clone, Deserialize)]
pub struct LockPlatform {
    pub name: String,
    /// 56-char hex of the 28-byte payment script hash. Matched on the payment
    /// part only — lock platforms glue a per-locker stake credential onto one
    /// shared script, so a full-address set would need an entry per locker.
    pub payment_cred: String,
    pub evidence: String,
}

impl LockPlatform {
    pub fn cred_bytes(&self) -> Result<[u8; 28]> {
        let b = hex::decode(&self.payment_cred)
            .with_context(|| format!("lock platform `{}`: payment_cred is not hex", self.name))?;
        <[u8; 28]>::try_from(b.as_slice()).map_err(|_| {
            anyhow::anyhow!(
                "lock platform `{}`: payment_cred must be 28 bytes",
                self.name
            )
        })
    }
}

/// An address tokens can reach but never leave.
///
// `SinkEntry` lived here until 2026-09-08. Its rule — that `evidence` is
// mandatory, because "provably unspendable" and "believed unspendable" are
// different claims about an address that removes supply from the float —
// survives intact as `address_registry::ScriptCategory::Burn { evidence }`.
// It stopped being a per-token concern the moment two walked tokens were found
// sending supply to the same script.

#[derive(Debug, Clone, Deserialize)]
pub struct TokenEntry {
    /// Short label used for the ledger filename and log lines (`aliens`).
    pub name: String,
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes, or `None` to watch the
    /// WHOLE POLICY.
    ///
    /// An omitted name and an empty name are different requests, which is why
    /// this is `Option<String>` and not a `String` that happens to be empty:
    /// `Some("")` is the asset whose on-chain name is zero bytes — a real and
    /// common shape — while `None` is every asset under the policy.
    ///
    /// Policy mode is what an NFT collection needs: thousands of units, no one
    /// of which is the token. It also serves a fungible policy that minted
    /// under more than one name.
    #[serde(default)]
    pub asset_name: Option<String>,
    /// Display decimals. Identity is the hex asset name; this is presentation
    /// only, and the **token registry — not the chain — is authoritative** for
    /// it. Read it from Koios `asset_info.token_registry_metadata.decimals`
    /// rather than inferring it from supply magnitude.
    ///
    /// `None` when the entry says nothing, which is **not** the same as `0` —
    /// hence `Option` rather than `#[serde(default)]`. An explicit `0` is a
    /// sourced claim that the token has no fractional part; an absent one is an
    /// invitation to ask [`chain_ledger::tokens`]. Collapsing the two would let
    /// an unstated value silently override a curated one.
    ///
    /// Absent is right for $Aliens, $Dong, $PERP and $NIKEPIG — all genuinely
    /// 0 dp, which is why the scaling went unexercised until $CSWAP (6 dp)
    /// arrived and `stats` printed its spot as `0.00000000`. A default that is
    /// right for every case you have is not the same as a default that is
    /// right.
    ///
    /// Resolve through [`TokenEntry::resolved_decimals`], never by reading this
    /// field directly.
    pub decimals: Option<u8>,
    /// Slot of the policy's FIRST mint. The walk's floor.
    ///
    /// This is not an optimisation with a correctness cost, which is the usual
    /// shape of a walk floor. Before its first mint the asset did not exist, so
    /// there is nothing earlier to miss and the outref buffer is complete by
    /// construction from here. Omit it and the walk starts at genesis, which is
    /// correct but ~16× slower for a recently-minted token.
    pub floor_slot: Option<u64>,
}

impl TokenEntry {
    pub fn policy_bytes(&self) -> Result<Vec<u8>> {
        let b = hex::decode(&self.policy)
            .with_context(|| format!("token `{}`: policy is not hex", self.name))?;
        if b.len() != 28 {
            bail!(
                "token `{}`: policy must be 28 bytes, got {}",
                self.name,
                b.len()
            );
        }
        Ok(b)
    }

    /// The watched asset-name bytes, or `None` for a whole-policy watch.
    ///
    /// Returns `Ok(None)` rather than an error for policy mode: "no single
    /// name" is a legitimate answer here, and callers that genuinely require
    /// one should say so with [`Self::require_asset_name`].
    pub fn asset_name_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.asset_name
            .as_ref()
            .map(|n| {
                hex::decode(n)
                    .with_context(|| format!("token `{}`: asset_name is not hex", self.name))
            })
            .transpose()
    }

    /// The asset name for a caller that cannot express a policy-wide watch.
    ///
    /// The export artifacts and the `stats` supply reconciliation are both
    /// single-unit shapes today, so they refuse a policy entry loudly here
    /// rather than silently picking one of its assets to describe.
    pub fn require_asset_name(&self) -> Result<Vec<u8>> {
        self.asset_name_bytes()?.with_context(|| {
            format!(
                "token `{}` watches a whole policy; this operation needs a single asset",
                self.name
            )
        })
    }

    /// The immutable file containing the floor — what `bootstrap --start` wants.
    pub fn floor_file(&self) -> Option<u64> {
        self.floor_slot.map(|s| s / CHUNK_SLOTS)
    }

    /// `policy_hex.asset_name_hex` — the key [`chain_ledger::tokens`] uses —
    /// or the bare `policy_hex` for a whole-policy watch.
    ///
    /// Keyed by the full unit rather than by ticker on purpose: anyone can mint
    /// a token called `USDM`, and matching on the name would let them borrow a
    /// real stablecoin's scale.
    ///
    /// The two forms are distinguishable by the dot, which is what lets a
    /// ledger file, an artifact prefix and a serve route all key off this one
    /// string without a second flag travelling beside it.
    pub fn unit(&self) -> String {
        match &self.asset_name {
            Some(name) => format!("{}.{name}", self.policy),
            None => self.policy.clone(),
        }
    }

    /// Display decimals: this entry's own value, else the curated shared table,
    /// else unknown.
    ///
    /// The order matters. A local entry is a sourced decision about a token we
    /// actually watch, so it wins; the shared table is the fallback that stops
    /// every new token starting life silently mis-scaled. `None` means *render
    /// raw*, which is what `chain-ledger` does for an unknown unit and the only
    /// honest answer — decimals are not on chain, so there is no rule to apply,
    /// only knowledge.
    pub fn resolved_decimals(&self) -> Option<u8> {
        self.decimals.or_else(|| {
            chain_ledger::tokens::decimals(&self.unit()).and_then(|d| u8::try_from(d).ok())
        })
    }
}

fn read(path: &Path) -> Result<RegistryFile> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading token registry {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parsing token registry {}", path.display()))
}

/// Load the registry and return the one requested token.
pub fn load(path: &Path, name: &str) -> Result<TokenEntry> {
    let entry = read(path)?
        .token
        .into_iter()
        .find(|t| t.name == name)
        .with_context(|| format!("no token named `{name}` in {}", path.display()))?;

    // Validate eagerly so a malformed policy fails at startup rather than
    // silently matching nothing for the length of a walk.
    entry.policy_bytes()?;
    entry.asset_name_bytes()?;
    Ok(entry)
}

/// Load by registry name, or accept a raw on-chain identity for something
/// nobody has registered — the serve path's "any token on demand".
///
/// Two accepted raw shapes:
/// - `<policy_hex>.<asset_name_hex>` — one unit
/// - `<policy_hex>` (bare 56 hex) — the WHOLE POLICY
///
/// A synthetic entry carries no floor (the walk starts at genesis, which the
/// sieve gate makes tolerable) and no curated decimals (the chain-ledger
/// fallback still applies). Its `name` IS the unit, so ledger and artifact
/// filenames are keyed by on-chain identity.
pub fn load_or_unit(path: &Path, name_or_unit: &str) -> Result<TokenEntry> {
    if let Ok(entry) = load(path, name_or_unit) {
        return Ok(entry);
    }
    let unit = name_or_unit.to_lowercase();
    // A bare policy is the whole-policy watch. Checked before the split so a
    // 56-hex string is never read as a policy with an absent name — they are
    // the same characters and only the dot tells them apart.
    let (policy, asset_name) = match unit.split_once('.') {
        Some((p, n)) => (p.to_string(), Some(n.to_string())),
        None if unit.len() == 56 && unit.chars().all(|c| c.is_ascii_hexdigit()) => {
            (unit.clone(), None)
        }
        None => bail!(
            "`{name_or_unit}` is not a registered token, a `<policy_hex>.<name_hex>` unit, \
             or a bare 56-hex policy id"
        ),
    };
    let entry = TokenEntry {
        name: unit.clone(),
        policy,
        asset_name,
        decimals: None,
        floor_slot: None,
    };
    entry.policy_bytes()?;
    entry.asset_name_bytes()?;
    Ok(entry)
}

/// The registered token holding this on-chain unit, if any — so a request
/// arriving by unit reuses the curated entry (floor, decimals, nickname-keyed
/// ledger db) instead of a synthetic one.
pub fn find_by_unit(path: &Path, unit: &str) -> Result<Option<TokenEntry>> {
    let unit = unit.to_lowercase();
    Ok(read(path)?.token.into_iter().find(|t| t.unit() == unit))
}

/// Every registered lock platform, validated.
pub fn load_lock_platforms(path: &Path) -> Result<Vec<LockPlatform>> {
    let platforms = read(path)?.lock_platform;
    for p in &platforms {
        p.cred_bytes()?;
    }
    Ok(platforms)
}
