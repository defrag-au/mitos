//! Target parsing — a wallet's on-chain identity is one or two 28-byte
//! credentials, and the sieve matches those bytes RAW inside chunk files.
//!
//! Accepted shapes:
//! - `stake1…` — one stake credential; finds every base address delegating
//!   to it, whatever the payment key.
//! - `addr1…` — payment credential plus (when present) the stake credential.
//! - 56 hex chars — one bare credential, matched against both address parts.
//!
//! What a stake-only target misses, by design: enterprise addresses of the
//! same wallet (no stake part on the address, nothing to match). Those are
//! the single-use bare addresses the relay-hop rule chases; the spike counts
//! what it sees and leaves that chase to the product pass.

use anyhow::{Context, Result, bail};
use pallas_addresses::{Address, ShelleyDelegationPart, ShelleyPaymentPart, StakePayload};

/// One credential to hunt for: a display label + the 28 raw bytes.
pub struct Cred {
    pub label: String,
    pub bytes: [u8; 28],
}

/// Stable cache key for a credential set — the same wallet asked for via the
/// same shape always lands on the same key, whatever bech32 string spelled it.
pub fn canonical(creds: &[Cred]) -> String {
    let mut parts: Vec<String> = creds
        .iter()
        .map(|c| format!("{}:{}", c.label, hex::encode(c.bytes)))
        .collect();
    parts.sort();
    parts.join("+")
}

pub fn parse(target: &str) -> Result<Vec<Cred>> {
    if let Ok(raw) = hex::decode(target) {
        let bytes: [u8; 28] = raw
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("hex target must be 28 bytes, got {}", raw.len()))?;
        return Ok(vec![Cred {
            label: "cred".into(),
            bytes,
        }]);
    }

    let addr = Address::from_bech32(target)
        .context("target is neither 28-byte hex nor a bech32 address")?;
    let mut creds = Vec::new();
    match addr {
        Address::Stake(sa) => {
            let h = match sa.payload() {
                StakePayload::Stake(h) => h,
                StakePayload::Script(h) => h,
            };
            creds.push(Cred {
                label: "stake".into(),
                bytes: h.as_slice().try_into().expect("stake cred is 28 bytes"),
            });
        }
        Address::Shelley(sh) => {
            let p = match sh.payment() {
                ShelleyPaymentPart::Key(h) => h,
                ShelleyPaymentPart::Script(h) => h,
            };
            creds.push(Cred {
                label: "payment".into(),
                bytes: p.as_slice().try_into().expect("payment cred is 28 bytes"),
            });
            match sh.delegation() {
                ShelleyDelegationPart::Key(h) | ShelleyDelegationPart::Script(h) => {
                    creds.push(Cred {
                        label: "stake".into(),
                        bytes: h.as_slice().try_into().expect("stake cred is 28 bytes"),
                    });
                }
                _ => {}
            }
        }
        Address::Byron(_) => bail!("Byron addresses carry no Shelley credentials to sieve for"),
    }
    if creds.is_empty() {
        bail!("no credentials extracted from target");
    }
    Ok(creds)
}
