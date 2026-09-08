//! Validates the `[interest]` declarations of every community module in the
//! repo.
//!
//! Module configs hard-code bech32 addresses rather than depending on the
//! shared `address-registry` crate — the modules compile to wasm and the list
//! is small, so updating means editing a string literal. That trade is fine,
//! but it means nothing checks the strings, and a bad one fails *silently*: the
//! host simply never matches it, so the module watches one fewer address than
//! its config claims and no error is ever raised.
//!
//! That is not hypothetical. `jpg-store-listing` shipped a "V3" address with an
//! `addr1w` header — which by definition carries no staking part — yet with a
//! staking part appended. It matched nothing for as long as it was there.
//!
//! Round-tripping is the check that catches it. A corrupt bech32 still decodes
//! into *something*; only re-encoding and comparing proves the header agrees
//! with the payload.

use std::path::{Path, PathBuf};

use pallas_addresses::Address;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ModuleConfig {
    #[serde(default)]
    interest: InterestSection,
}

#[derive(Debug, Default, Deserialize)]
struct InterestSection {
    #[serde(default)]
    addresses: Vec<String>,
    #[serde(default)]
    policies: Vec<String>,
    #[serde(default)]
    payment_credentials: Vec<String>,
}

fn community_modules_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../community-modules")
}

/// `community-modules/<name>/<name_with_underscores>.toml`. Deliberately not a
/// recursive glob: `tests/fixtures/*/fixture.toml` are test inputs and may hold
/// deliberately odd values.
fn module_configs() -> Vec<(String, ModuleConfig)> {
    let dir = community_modules_dir();
    let entries =
        std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));

    let mut out = Vec::new();
    for entry in entries.flatten() {
        if !entry.path().is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let config_path = entry
            .path()
            .join(format!("{}.toml", name.replace('-', "_")));
        if !config_path.exists() {
            continue;
        }
        let raw = std::fs::read_to_string(&config_path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", config_path.display()));
        let config: ModuleConfig = toml::from_str(&raw)
            .unwrap_or_else(|e| panic!("cannot parse {}: {e}", config_path.display()));
        out.push((name, config));
    }
    out
}

#[test]
fn every_declared_address_is_well_formed() {
    let mut bad = Vec::new();
    let mut checked = 0;

    for (module, config) in module_configs() {
        for address in &config.interest.addresses {
            checked += 1;
            match Address::from_bech32(address) {
                Ok(decoded) => {
                    let reencoded = decoded.to_bech32().unwrap_or_default();
                    if reencoded != *address {
                        bad.push(format!(
                            "{module}: {address} does not round-trip \
                             (re-encodes to {reencoded}) — header and payload disagree, \
                             so this address matches nothing"
                        ));
                    }
                }
                Err(e) => bad.push(format!("{module}: {address} does not decode: {e}")),
            }
        }
    }

    assert!(
        bad.is_empty(),
        "malformed addresses in community-module configs:\n  {}",
        bad.join("\n  ")
    );
    assert!(
        checked > 0,
        "no module addresses were checked — has the layout or naming changed?"
    );
}

/// Policy ids and payment credentials are 28-byte hashes; anything else is a
/// paste error that would also silently match nothing.
#[test]
fn every_declared_hash_is_28_bytes_of_lowercase_hex() {
    let mut bad = Vec::new();

    for (module, config) in module_configs() {
        let labelled = config
            .interest
            .policies
            .iter()
            .map(|p| ("policy", p))
            .chain(
                config
                    .interest
                    .payment_credentials
                    .iter()
                    .map(|c| ("payment_credential", c)),
            );

        for (kind, value) in labelled {
            if value.len() != 56 {
                bad.push(format!(
                    "{module}: {kind} {value} is {} chars, expected 56",
                    value.len()
                ));
            } else if hex::decode(value).is_err() {
                bad.push(format!("{module}: {kind} {value} is not valid hex"));
            } else if value.chars().any(|c| c.is_ascii_uppercase()) {
                bad.push(format!(
                    "{module}: {kind} {value} must be lowercase — matching is byte-exact"
                ));
            }
        }
    }

    assert!(
        bad.is_empty(),
        "malformed hashes in community-module configs:\n  {}",
        bad.join("\n  ")
    );
}
