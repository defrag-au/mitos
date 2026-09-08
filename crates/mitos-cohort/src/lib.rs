//! Cohort classification — chain-derivable only.
//!
//! Every party gets a cohort and, with it, **how firmly that cohort is known**.
//! The whole point of separating the two is that a provably-unspendable script
//! and a wallet somebody told us belongs to the team are not the same kind of
//! claim, and rendering them identically launders a guess into a fact.
//!
//! # Why this is its own crate, separate from `mitos-pool-observe`
//!
//! Both were modules of the `token-ledger` binary, which made them unreachable
//! from the archive path and from a Worker reading R2 — the interpretive layer
//! existed and was locked inside an executable. Splitting them apart rather
//! than into one crate is a **dependency** decision, not a taste one: this
//! crate needs only `pallas-addresses` and a vesting decoder, so it links into
//! wasm cheaply, while `mitos-pool-observe` pulls `mitos-chain-walk` for the
//! walker's output shape. Fused, every consumer that only wanted to ask "what
//! kind of holder is this address" would link the whole chain-walk stack.
//!
//! This crate deliberately stops at what the chain proves:
//!
//! | Cohort | Basis | What it rests on |
//! |---|---|---|
//! | `burn`    | `proven`  | a registered sink whose script provably cannot spend |
//! | `pool`    | `decoded` | recognised at a DEX pool address, reserves decoded |
//! | `vesting` | `decoded` | a lock platform whose datum we read — schedule and owner in hand |
//! | `vesting` | `registered` | a lock platform registered with evidence, datum shape unreadable |
//! | `script`  | `chain`   | payment credential is a script — header byte, nothing more |
//! | `wallet`  | `chain`   | payment credential is a key |
//!
//! `script` is the honest residual: the address is a contract of *some* kind —
//! an order book, something uncatalogued — and we do not yet know which. It is
//! a first-class, visible cohort rather than a silent addition to ordinary
//! float, because the difference between "we know this is liquid" and "we have
//! not looked" is the difference this surface exists to show.
//!
//! **`vesting` is still chain-derivable**, which is why it belongs here: it is
//! a payment-credential match against a platform contract whose shape
//! `mitos-vesting-decode` establishes, not somebody's say-so. What remains
//! *declared* — and deliberately absent — is "this wallet is the team" or "this
//! script is Project X's treasury". Those need a source and an `as_of`, and the
//! design requires them to stay mutable so history recomputes when they change.
//! Baking them in here would put them in the one place they must not be.
//!
//! Because every cohort below is a pure function of the stored address (plus
//! the pool and sink sets), reclassifying is a re-derivation, never a re-walk.
//! That property is load-bearing — see `POLICY_ARCHIVE_OBSERVATIONS.md`, which
//! rejected inlining cohorts per movement precisely to protect it.

use pallas_addresses::Address;

/// A party's cohort. Ordered as the supply cascade renders them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cohort {
    /// Provably unspendable. Removed from supply, permanently.
    Burn,
    /// Sitting on a launchpad bonding curve, unsold. Nobody has ever owned it.
    ///
    /// Deducted from supply ALONGSIDE `Burn` rather than counted within the
    /// float, and the reason is arithmetic rather than taste. A bonding curve
    /// is a pool and is tradeable against, so the LP-in-float rule superficially
    /// applies — but applying it makes a token nobody has bought read as ~100%
    /// float, and the realisable band would price selling stock that was never
    /// held. A token that has sold 1% of supply should read as ~1% float.
    ///
    /// Empties into `Pool` at graduation, in one transaction, at a known slot —
    /// so the transition is an event on the spine rather than a reclassification.
    Inventory,
    /// A decoded DEX pool. In the float — see the LP-in-float decision — but
    /// its own band, because a pool is not a holder.
    Pool,
    /// A known lock platform (CrowdLock / Shield). Recognised by payment
    /// credential; the lock datum additionally yields an unlock timestamp and
    /// the real owner behind the contract, which is what lets the cascade
    /// separate *still locked* from *matured but unclaimed*.
    Vesting,
    /// Script-controlled, kind unknown. Might be locked, might be an open
    /// order. The uncertainty is the finding.
    Script,
    /// Key-controlled. An ordinary holder.
    Wallet,
}

impl Cohort {
    pub fn as_str(&self) -> &'static str {
        match self {
            Cohort::Burn => "burn",
            Cohort::Inventory => "inventory",
            Cohort::Pool => "pool",
            Cohort::Vesting => "vesting",
            Cohort::Script => "script",
            Cohort::Wallet => "wallet",
        }
    }

    /// How firmly this cohort is known.
    pub fn basis(&self) -> &'static str {
        match self {
            Cohort::Burn => "proven",
            Cohort::Inventory | Cohort::Pool | Cohort::Vesting => "decoded",
            Cohort::Script | Cohort::Wallet => "chain",
        }
    }
}

/// Lower-case hex. The registry's lookups are exact and its own guard test
/// enforces lower case, so this must not use an upper-case formatter.
fn hex_lower(bytes: &[u8; 28]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The 28-byte payment credential of a Shelley address, if it has one.
pub fn payment_cred(address: &str) -> Option<[u8; 28]> {
    match Address::from_bech32(address).ok()? {
        Address::Shelley(sh) => match sh.payment() {
            pallas_addresses::ShelleyPaymentPart::Key(h)
            | pallas_addresses::ShelleyPaymentPart::Script(h) => Some(**h),
        },
        _ => None,
    }
}

/// Whether the address's PAYMENT part is a script.
///
/// Read through pallas rather than by bech32 prefix: `addr1z` covers two
/// distinct address types and a prefix test silently mis-sorts one of them.
/// Byron and stake addresses are not scripts.
pub fn is_script_address(address: &str) -> bool {
    matches!(
        Address::from_bech32(address),
        Ok(Address::Shelley(sh)) if sh.payment().is_script()
    )
}

/// A cohort together with how firmly it is known.
///
/// Basis is per-classification rather than per-cohort because `vesting` can be
/// reached two ways with genuinely different strength: a platform whose datum
/// we decode (schedule and owner in hand) versus one we have merely registered
/// as a lock. Collapsing them would present a guess and a reading identically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classification {
    pub cohort: Cohort,
    pub basis: &'static str,
}

/// Classify one address.
///
/// `sinks` and `pools` are exact address sets; `lock_creds` are 28-byte
/// payment credentials. Everything else is decided by the payment credential's
/// kind, read through pallas rather than by matching a bech32 prefix —
/// `addr1z` covers two distinct address types and a prefix test would silently
/// mis-sort one of them.
pub fn classify(address: &str, pools: &[String], lock_creds: &[[u8; 28]]) -> Classification {
    let c = |cohort: Cohort| Classification {
        cohort,
        basis: cohort.basis(),
    };
    if pools.iter().any(|p| p == address) {
        return c(Cohort::Pool);
    }
    match Address::from_bech32(address) {
        Ok(Address::Shelley(sh)) => {
            let cred = match sh.payment() {
                pallas_addresses::ShelleyPaymentPart::Key(h)
                | pallas_addresses::ShelleyPaymentPart::Script(h) => **h,
            };
            // Burn sinks come from `address-registry`, by CREDENTIAL. They used
            // to be an exact-address list loaded from per-token config, which
            // made a property of the script into a property of each token that
            // happened to reach it — and meant a sink was only known to the
            // tokens somebody had already registered it against.
            if matches!(
                address_registry::lookup_payment_credential(&hex_lower(&cred))
                    .map(|e| &e.category),
                Some(address_registry::AddressCategory::Script(
                    address_registry::ScriptCategory::Burn { .. }
                ))
            ) {
                return c(Cohort::Burn);
            }
            // Lock platforms glue a per-locker stake credential onto one shared
            // payment script, so this must match the payment part only — a
            // full-address set would need one entry per locker and would miss
            // every new one.
            // Checked before the lock platforms because a launchpad curve is a
            // stronger claim than either: the contract is named, and what it
            // holds has never been owned by anybody.
            if mitos_launchpad_decode::is_snek_fun_curve(&cred) {
                c(Cohort::Inventory)
            } else if mitos_vesting_decode::crowd_lock::is_crowd_lock(&cred)
                || mitos_vesting_decode::snek_fun::is_snek_fun(&cred)
            {
                c(Cohort::Vesting)
            } else if lock_creds.contains(&cred) {
                // Known to be a lock, but its datum is not one we read — so no
                // schedule, and its supply stays locked rather than maturing.
                Classification {
                    cohort: Cohort::Vesting,
                    basis: "registered",
                }
            } else if sh.payment().is_script() {
                c(Cohort::Script)
            } else {
                c(Cohort::Wallet)
            }
        }
        // Byron addresses have no script form; a stake address never holds an
        // asset. Both are key-controlled for our purposes.
        _ => c(Cohort::Wallet),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The verified always-fails sink from BURN_LEDGER.md.
    const BURN: &str = "addr1w8qmxkacjdffxah0l3qg8hq2pmvs58q8lcy42zy9kda2ylc6dy5r4";
    // CSwap pool (script payment + key stake, `addr1z`).
    const CSWAP: &str = "addr1z8ke0c9p89rjfwmuh98jpt8ky74uy5mffjft3zlcld9h7ml3lmln3mwk0y3zsh3gs3dzqlwa9rjzrxawkwm4udw9axhs6fuu6e";
    // An ordinary base address.
    const WALLET: &str = "addr1qylnwp3lp2re0jtw9kf0dfvxf4mkvwt3jqzqhqzc5jvxjqrcfxvqcnf2v7xqcnqzsxsdxaewwqnyzcnrqhqhqhqhqhqcnfsz3";

    /// snek.fun's bonding curve — script payment AND script stake, `addr1x`.
    const CURVE: &str = "addr1xxg94wrfjcdsjncmsxtj0r87zk69e0jfl28n934sznu95tdj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrs2993lw";
    /// Splash's pool contract, which shares the curve's STAKE credential.
    const SPLASH_POOL: &str = "addr1x89ksjnfu7ys02tedvslc9g2wk90tu5qte0dt4dge60hdudj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrsg0g63z";

    /// Unsold launchpad supply is its own cohort, not the `script — KIND
    /// UNKNOWN` residual it used to land in. On a token mid-bonding this is up
    /// to 96% of supply.
    #[test]
    fn a_bonding_curve_is_inventory() {
        let c = classify(CURVE, &[], &[]);
        assert_eq!(c.cohort, Cohort::Inventory);
        assert_eq!(c.basis, "decoded");
    }

    /// The curve and Splash's pool share a stake credential exactly, which is
    /// how a registry elsewhere came to call the curve "DexHunter". Matching on
    /// the PAYMENT credential is what keeps them apart — if this ever fails,
    /// a token's unsold inventory is being counted as pooled liquidity.
    #[test]
    fn a_shared_stake_credential_does_not_make_two_contracts_one() {
        assert_eq!(classify(CURVE, &[], &[]).cohort, Cohort::Inventory);
        // Not registered as a pool here, so it falls to the honest residual —
        // the point is only that it is NOT read as the curve.
        assert_eq!(
            classify(SPLASH_POOL, &[], &[]).cohort,
            Cohort::Script,
            "the pool must not inherit the curve's cohort from a shared stake part"
        );
        // And with the pool registered, it is a pool rather than inventory.
        let pools = vec![SPLASH_POOL.to_string()];
        assert_eq!(classify(SPLASH_POOL, &pools, &[]).cohort, Cohort::Pool);
    }

    /// The sink is no longer injectable — it comes from `address-registry`, by
    /// credential, with the evidence for its unspendability attached. So this
    /// asserts the REAL one resolves rather than that an arbitrary address can
    /// be declared a burn, which is a stronger test: a caller can no longer
    /// nominate a sink by passing a list.
    #[test]
    fn the_registered_sink_is_proven_burn() {
        assert_eq!(classify(BURN, &[], &[]).cohort, Cohort::Burn);
        assert_eq!(classify(BURN, &[], &[]).basis, "proven");
    }

    /// And an address nobody registered is NOT a burn, however much it looks
    /// like one. `addr1w…` is script-payment-with-no-stake — the same shape as
    /// the real sink — and shape is not evidence.
    #[test]
    fn an_unregistered_script_is_not_a_sink() {
        const LOOKALIKE: &str = "addr1w8n8kq3j96v03a3znqqy9f54prt8uf6s4lyj7nuf2cvg2ucnwhs68";
        assert_eq!(classify(LOOKALIKE, &[], &[]).cohort, Cohort::Script);
    }

    #[test]
    fn pool_beats_the_generic_script_reading() {
        // Without the pool set this is just "a script"; with it, it is a pool.
        // Getting that precedence backwards would hide every pool inside the
        // unclassified band.
        assert_eq!(classify(CSWAP, &[], &[]).cohort, Cohort::Script);
        let pools = vec![CSWAP.to_string()];
        assert_eq!(classify(CSWAP, &pools, &[]).cohort, Cohort::Pool);
    }

    #[test]
    fn script_payment_is_not_a_wallet() {
        assert_eq!(classify(CSWAP, &[], &[]).cohort, Cohort::Script);
        assert_eq!(classify(CSWAP, &[], &[]).cohort.basis(), "chain");
    }

    #[test]
    fn unparseable_addresses_do_not_become_scripts() {
        // Fall back to wallet rather than inflating the script band with
        // decode failures — an unclassified band that grows because of our own
        // bugs would be worse than useless.
        assert_eq!(
            classify("not-an-address", &[], &[]).cohort,
            Cohort::Wallet
        );
    }

    #[test]
    fn key_payment_is_a_wallet() {
        assert_eq!(classify(WALLET, &[], &[]).cohort, Cohort::Wallet);
    }

    // Two real $Aliens holders: same CrowdLock payment script, different
    // per-locker stake credentials. Matching on the full address would need an
    // entry per locker and would miss every new one, so this pins the
    // payment-credential rule rather than the addresses.
    const LOCK_A: &str = "addr1zyupekdkyr8f6lrnm4zulcs8juwv080hjfgsqvgkp98kkdkrxp0e2m4utglc7hmzkuta3e2td72cdjq9m9xlfn6rz8vq86l65l";
    const LOCK_B: &str = "addr1zyupekdkyr8f6lrnm4zulcs8juwv080hjfgsqvgkp98kkdhym9auk0rgpz3lurkryvhl55046ak6ex4tlyj6mxxj735syr937w";

    #[test]
    fn crowdlock_is_vesting_across_differing_stake_parts() {
        assert_eq!(classify(LOCK_A, &[], &[]).cohort, Cohort::Vesting);
        assert_eq!(classify(LOCK_B, &[], &[]).cohort, Cohort::Vesting);
        assert_eq!(classify(LOCK_A, &[], &[]).cohort.basis(), "decoded");
        assert_ne!(
            payment_cred(LOCK_A),
            None,
            "payment credential must be extractable"
        );
        assert_eq!(payment_cred(LOCK_A), payment_cred(LOCK_B));
    }

    // A real $Aliens script holder that is NOT any known lock platform — the
    // launchpad's bonding-curve contract. Stands in for "some contract we have
    // registered as a lock but cannot decode".
    //
    // It used to be the snek.fun lock address, until snek.fun's credential and
    // datum moved into `mitos-vesting-decode` and it stopped being an example
    // of an unregistered platform. The assertion is unchanged; only the fixture
    // had to be a contract that is still genuinely unknown.
    const UNKNOWN_SCRIPT: &str = "addr1x9d238ne9evvyu8vrqpqdfz6ltpk4d9wc9mnlg9q4ursl889t0d6nktep03tacdtww0278hdyqp40pla5kf6h4pfwzzqa2av33";

    #[test]
    fn a_registered_platform_is_vesting_but_weaker_evidence() {
        // Unregistered it is just an unnamed script; registered it is vesting,
        // and the basis says we could not read its schedule.
        assert_eq!(
            classify(UNKNOWN_SCRIPT, &[], &[]).cohort,
            Cohort::Script
        );
        let creds = [payment_cred(UNKNOWN_SCRIPT).expect("payment cred")];
        let got = classify(UNKNOWN_SCRIPT, &[], &creds);
        assert_eq!(got.cohort, Cohort::Vesting);
        assert_eq!(
            got.basis, "registered",
            "a registered lock must not claim the same evidence as a decoded one"
        );
        // Built-in platforms keep the stronger basis even when registered
        // ones exist alongside them.
        assert_eq!(classify(LOCK_A, &[], &creds).basis, "decoded");
        assert_eq!(classify(SNEKFUN_LOCK, &[], &[]).basis, "decoded");
    }

    /// snek.fun is recognised by the crate now, not by local config — so it
    /// must classify as decoded vesting with no registry entry at all.
    const SNEKFUN_LOCK: &str = "addr1w8wma0rzvdexhnqrty6t8dcur7c5ffu2rjau2ayec3d3azg5qp35x";

    #[test]
    fn snekfun_is_recognised_without_any_local_registration() {
        let got = classify(SNEKFUN_LOCK, &[], &[]);
        assert_eq!(got.cohort, Cohort::Vesting);
        assert_eq!(got.basis, "decoded");
    }

    /// Precedence: provably-gone outranks locked-for-now, and outranks a
    /// launchpad curve. Asserted by ORDER in `classify` rather than by
    /// injecting a sink, now that sinks come from the registry — if a
    /// credential were ever registered as both, burn must win.
    #[test]
    fn burn_is_checked_before_every_other_script_cohort() {
        let src = include_str!("lib.rs");
        let burn_at = src.find("return c(Cohort::Burn);").expect("burn arm");
        let launchpad_at = src.find("is_snek_fun_curve").expect("launchpad arm");
        let vesting_at = src.find("is_crowd_lock").expect("vesting arm");
        assert!(
            burn_at < launchpad_at && burn_at < vesting_at,
            "burn must be tested first — provably unspendable outranks every \
             claim about what a script is FOR"
        );
    }
}
