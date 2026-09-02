//! Watched-token registry — the pluggability seam.
//!
//! A token is declarative config, not a code path. It names the
//! `(policy, asset_name)` to follow and an optional `floor_slot`, and that is
//! the whole contract.
//!
//! **One watched asset per ledger.** Not one policy — one asset. The delta
//! primitive in `store` is `(tx, party) -> signed amount`, which is only
//! unambiguous for a single fungible unit; a policy carrying several assets
//! would need the asset on the key and would break the per-tx conservation
//! check that makes this walk self-verifying. Every token this tool currently
//! targets is a single-asset policy. Widening it is a schema change, so the
//! restriction is enforced here rather than discovered later.

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
    #[serde(default)]
    sink: Vec<SinkEntry>,
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
/// **`evidence` is mandatory and is the point of the type.** "Provably
/// unspendable" and "believed unspendable" are different claims, and an address
/// that removes supply from the float — and, under `BURN_LEDGER.md`, buys paid
/// access — is exactly where an unexamined assumption becomes expensive. So a
/// sink cannot be registered without saying how it was established, mirroring
/// the address-registry convention of storing a `source` string.
#[derive(Debug, Clone, Deserialize)]
pub struct SinkEntry {
    pub address: String,
    pub evidence: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenEntry {
    /// Short label used for the ledger filename and log lines (`aliens`).
    pub name: String,
    /// 56-char lowercase hex policy id.
    pub policy: String,
    /// Lowercase hex of the on-chain asset-name bytes. May be empty for
    /// empty-name assets, so it is required rather than optional — an omitted
    /// name and an empty name are different assets.
    pub asset_name: String,
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

    pub fn asset_name_bytes(&self) -> Result<Vec<u8>> {
        hex::decode(&self.asset_name)
            .with_context(|| format!("token `{}`: asset_name is not hex", self.name))
    }

    /// The immutable file containing the floor — what `bootstrap --start` wants.
    pub fn floor_file(&self) -> Option<u64> {
        self.floor_slot.map(|s| s / CHUNK_SLOTS)
    }

    /// `policy_hex.asset_name_hex` — the key [`chain_ledger::tokens`] uses.
    ///
    /// Keyed by the full unit rather than by ticker on purpose: anyone can mint
    /// a token called `USDM`, and matching on the name would let them borrow a
    /// real stablecoin's scale.
    pub fn unit(&self) -> String {
        format!("{}.{}", self.policy, self.asset_name)
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

/// Load by registry name, or accept a raw `<policy_hex>.<asset_name_hex>`
/// unit for a token nobody has registered — the serve path's "any token on
/// demand". A synthetic entry carries no floor (the walk starts at genesis,
/// which the sieve gate makes tolerable) and no curated decimals (the
/// chain-ledger fallback still applies). Its `name` IS the unit, so ledger
/// and artifact filenames are keyed by on-chain identity.
pub fn load_or_unit(path: &Path, name_or_unit: &str) -> Result<TokenEntry> {
    if let Ok(entry) = load(path, name_or_unit) {
        return Ok(entry);
    }
    let unit = name_or_unit.to_lowercase();
    let Some((policy, asset_name)) = unit.split_once('.') else {
        bail!(
            "`{name_or_unit}` is neither a registered token nor a `<policy_hex>.<name_hex>` unit"
        );
    };
    let entry = TokenEntry {
        name: unit.clone(),
        policy: policy.to_string(),
        asset_name: asset_name.to_string(),
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

/// Every registered unspendable sink.
pub fn load_sinks(path: &Path) -> Result<Vec<SinkEntry>> {
    Ok(read(path)?.sink)
}

/// Every registered lock platform, validated.
pub fn load_lock_platforms(path: &Path) -> Result<Vec<LockPlatform>> {
    let platforms = read(path)?.lock_platform;
    for p in &platforms {
        p.cred_bytes()?;
    }
    Ok(platforms)
}
