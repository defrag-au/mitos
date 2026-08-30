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
