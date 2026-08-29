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

/// A parsed target: what to hunt for, and whether it is a wallet at all.
pub struct Target {
    pub creds: Vec<Cred>,
    /// A SCRIPT credential — a contract address, not somebody's wallet.
    ///
    /// Worth knowing before any scan because contracts are a different order
    /// of magnitude: the jpg.store marketplace put 614,149 rows and 157 MB
    /// into the cache from ONE year plus one backfill segment, and its real
    /// history is every trade on the platform. The row cap catches it, but
    /// only after paying for a whole segment — and no reader wants a
    /// marketplace's flow feed anyway.
    pub is_script: bool,
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

pub fn parse(target: &str) -> Result<Target> {
    if let Ok(raw) = hex::decode(target) {
        let bytes: [u8; 28] = raw
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("hex target must be 28 bytes, got {}", raw.len()))?;
        return Ok(Target {
            creds: vec![Cred {
                label: "cred".into(),
                bytes,
            }],
            // A bare hash carries no header, so script-ness is UNKNOWABLE
            // here. Reported as false: the operator CLI takes this shape and
            // is trusted, and the hosted surface never sees it.
            is_script: false,
        });
    }

    let addr = Address::from_bech32(target)
        .context("target is neither 28-byte hex nor a bech32 address")?;
    let mut creds = Vec::new();
    let mut is_script = false;
    match addr {
        Address::Stake(sa) => {
            let h = match sa.payload() {
                StakePayload::Stake(h) => h,
                StakePayload::Script(h) => {
                    // A script STAKE credential counts too: it is the
                    // delegation part of a contract's own address, and asking
                    // for it sweeps the same traffic by another name.
                    is_script = true;
                    h
                }
            };
            creds.push(Cred {
                label: "stake".into(),
                bytes: h.as_slice().try_into().expect("stake cred is 28 bytes"),
            });
        }
        Address::Shelley(sh) => {
            let p = match sh.payment() {
                ShelleyPaymentPart::Key(h) => h,
                ShelleyPaymentPart::Script(h) => {
                    is_script = true;
                    h
                }
            };
            creds.push(Cred {
                label: "payment".into(),
                bytes: p.as_slice().try_into().expect("payment cred is 28 bytes"),
            });
            match sh.delegation() {
                ShelleyDelegationPart::Key(h) => creds.push(Cred {
                    label: "stake".into(),
                    bytes: h.as_slice().try_into().expect("stake cred is 28 bytes"),
                }),
                ShelleyDelegationPart::Script(h) => {
                    is_script = true;
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
    Ok(Target { creds, is_script })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The jpg.store v1 marketplace contract — the address that prompted this
    /// guard. `addr1x…` is script payment AND script stake.
    const JPG_STORE_V1: &str = "addr1xxgx3far7qygq0k6epa0zcvcvrevmn0ypsnfsue94nsn3tfvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8eks2utwdd";

    #[test]
    fn a_marketplace_contract_reads_as_a_script() {
        let t = parse(JPG_STORE_V1).expect("parses");
        assert!(t.is_script, "jpg.store v1 must be recognised as a contract");
        assert_eq!(t.creds.len(), 2, "script payment + script stake");
    }

    /// The other side of the guard: an ordinary wallet must NOT trip it, or
    /// the refusal locks every reader out of the product.
    #[test]
    fn ordinary_wallets_are_not_scripts() {
        for addr in [
            // base address, key payment + key stake
            "addr1q9e43z5wu47c5xxgluj9kamh2jf5pkg0mn5tzles2uj9qwgj0ltlkk9eh88vunyat9ykz7gmjrnc4pkr9clxvxjl4czqhqduat",
            // enterprise, key payment, no stake
            "addr1v9srzmt3s3h98jdy6e08jm2605s5yjfhjt0dg0na4e77yssehvg4s",
            // stake key on its own
            "stake1u9af4pdvysr7ysqm3n0l2j2slkzlrvkz8ks8gn8mwy77rrsawvqey",
        ] {
            let t = parse(addr).unwrap_or_else(|e| panic!("{addr} should parse: {e}"));
            assert!(!t.is_script, "{addr} is a wallet, not a contract");
        }
    }

    /// A bare credential hash has no header to read, so it cannot be judged.
    /// It reports "not a script" and is reachable only from the trusted CLI.
    #[test]
    fn a_bare_hash_cannot_be_judged() {
        let t = parse(&"ab".repeat(28)).expect("28 bytes of hex");
        assert!(!t.is_script);
        assert_eq!(t.creds.len(), 1);
    }
}
