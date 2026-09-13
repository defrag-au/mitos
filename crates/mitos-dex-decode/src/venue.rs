//! The canonical venue vocabulary — ONE spelling per venue, as symbols.
//!
//! # ⚠️ Why this file exists
//!
//! These names were inline string literals scattered through
//! `mitos-pool-observe` (`dex: "splash"`, `dex: "minswap-v2"`, …). Nothing
//! stopped a second consumer inventing its own set, and one did: a token band
//! built its venue names by hand and rendered
//!
//! ```text
//! splash        29,121 ADA   fills    0
//! da5b47aed39…  not published   fills  334
//! ```
//!
//! — named pools with no trading, beside credentials doing all of it. The two
//! halves of the same venue never joined because they were spelled by two
//! different authors.
//!
//! 🔑 **A venue's name is an identifier, and identifiers belong in one place.**
//! With these as constants, a mismatch is a compile error rather than a silent
//! failure to join.
//!
//! # Spelling rules
//!
//! - **lower case, hyphenated** — these are slugs, not display strings. A
//!   surface wanting `CSWAP` title-cases at the edge.
//! - **the VERSION is part of the identity** where a venue runs more than one
//!   incompatible contract. `minswap-v1` and `minswap-v2` publish reserves
//!   differently and are not interchangeable, so merging them under `minswap`
//!   would ask a reader to trust one number computed two ways.
//!
//! ⚠️ `address-registry` labels the same contracts `"Splash"`, `"Minswap"`,
//! `"CSWAP"` — a DISPLAY vocabulary, version-free, and deliberately not this
//! one. Do not join the two by string equality; the registry is authoritative
//! for *what a script is*, this is authoritative for *which pool
//! implementation published these reserves*.

/// Splash (formerly Spectrum).
pub const SPLASH: &str = "splash";
/// CSwap. One pool address, one order address, both shared across every pair.
pub const CSWAP: &str = "cswap";
/// Minswap V1 — reserves are the output's own value.
pub const MINSWAP_V1: &str = "minswap-v1";
/// Minswap V2 — reserves live in the datum, net of treasury.
pub const MINSWAP_V2: &str = "minswap-v2";
pub const SUNDAE_V1: &str = "sundae-v1";
pub const SUNDAE_V3: &str = "sundae-v3";
pub const WINGRIDERS_V1: &str = "wingriders-v1";
pub const WINGRIDERS_V2: &str = "wingriders-v2";
/// snek.fun's bonding curve. ⚠️ Not a constant-product pool — its reserves are
/// real and its rate is NOT `quote / base`.
pub const SNEK_FUN: &str = "snek.fun";

/// Every venue this workspace can name.
pub const ALL: [&str; 9] = [
    SPLASH,
    CSWAP,
    MINSWAP_V1,
    MINSWAP_V2,
    SUNDAE_V1,
    SUNDAE_V3,
    WINGRIDERS_V1,
    WINGRIDERS_V2,
    SNEK_FUN,
];

/// What a contract DOES, as far as a trade fold is concerned.
///
/// ⚠️ Not a bool and not a string. The two roles produce different events from
/// the same movement — a wallet paying an ORDER is a placement, a wallet paying
/// a POOL is a swap — and getting them the wrong way round silently converts
/// intentions into trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteRole {
    /// Holds the reserves. Its counterparty is the trader.
    Pool,
    /// Holds an order awaiting a batcher. May or may not name who placed it.
    Order,
}

impl SiteRole {
    pub const ALL: [SiteRole; 2] = [SiteRole::Pool, SiteRole::Order];
}

/// How a contract is matched on chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteKey {
    /// The PAYMENT credential. Minswap, WingRiders and Sundae derive their
    /// stake part per pool, so a full-address set would need an entry each and
    /// would miss every pool created after it was written.
    Cred([u8; 28]),
    /// A genuine single address — CSwap and Splash really are one each.
    Address(&'static str),
}

/// One contract this workspace can name: where it is, whose it is, what it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Site {
    pub key: SiteKey,
    pub venue: &'static str,
    pub role: SiteRole,
    /// ⚠️ `true` when ONE address serves every trader, so a movement spent
    /// from it cannot name who traded and never will. CSwap's order contract
    /// is the measured case.
    pub shared: bool,
}

/// ⚠️ **THE REGISTRY. Every venue contract, declared ONCE.**
///
/// # Why this exists
///
/// The observation side (`mitos-pool-observe::recognise`) and the trade-fold
/// side (`policy-archive::trade::Roles`) each need to know which credentials
/// belong to which venue, and each used to carry its own hand-written list.
/// They drifted, exactly as two lists of the same thing do.
///
/// MEASURED on **$DONUT** (`a8d877eb…`): the observer decoded **765
/// SundaeSwap V3 pool states** holding 6,028 ₳, and the trade fold — which had
/// never been told Sundae's credential — classified every one of its swaps as
/// a plain `Transfer`. The analysis tab draws on venue activity, so the token
/// rendered as if it had no venue at all: **0 fills against 765 pool
/// sightings**. Nothing errored.
///
/// A venue added here reaches both halves. A venue added to only one of them
/// is the bug above.
pub const SITES: [Site; 13] = [
    Site {
        key: SiteKey::Cred(crate::splash::POOL_CRED_A),
        venue: SPLASH,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::splash::POOL_CRED_B),
        venue: SPLASH,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::splash::ORDER_CRED),
        venue: SPLASH,
        role: SiteRole::Order,
        shared: false,
    },
    Site {
        key: SiteKey::Address(crate::cswap::POOL_SCRIPT_ADDR),
        venue: CSWAP,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::cswap::ORDER_CRED),
        venue: CSWAP,
        role: SiteRole::Order,
        shared: crate::cswap::ORDER_IS_SHARED_ADDRESS,
    },
    Site {
        key: SiteKey::Cred(crate::minswap::V1_PAYMENT_CRED),
        venue: MINSWAP_V1,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::minswap::V2_PAYMENT_CRED),
        venue: MINSWAP_V2,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::minswap::V2_ORDER_CRED),
        venue: MINSWAP_V2,
        role: SiteRole::Order,
        shared: false,
    },
    // ⚠️ THE FOUR THAT WERE MISSING. `recognise` has decoded all of them since
    // the observation tier shipped; the trade fold had never heard of them.
    Site {
        key: SiteKey::Cred(crate::sundae::POOL_PAYMENT_CRED),
        venue: SUNDAE_V3,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::sundae::V1_PAYMENT_CRED),
        venue: SUNDAE_V1,
        role: SiteRole::Pool,
        shared: false,
    },
    // ⚠️ NOT shared — Sundae composes the TRADER's stake credential onto this
    // payment script, so an order leg names who placed it. 253 distinct
    // addresses on one credential, measured on $DONUT.
    Site {
        key: SiteKey::Cred(crate::sundae::ORDER_PAYMENT_CRED),
        venue: SUNDAE_V3,
        role: SiteRole::Order,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::wingriders::V2_PAYMENT_CRED),
        venue: WINGRIDERS_V2,
        role: SiteRole::Pool,
        shared: false,
    },
    Site {
        key: SiteKey::Cred(crate::wingriders::V1_PAYMENT_CRED),
        venue: WINGRIDERS_V1,
        role: SiteRole::Pool,
        shared: false,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// ⚠️ Two venues sharing a name would silently merge their pools, their
    /// liquidity and their fills into one row.
    #[test]
    fn every_venue_name_is_distinct() {
        let mut seen: Vec<&str> = ALL.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), ALL.len());
    }

    /// Slugs, not display strings — a surface that wants `CSWAP` upper-cases
    /// at the edge. Pinned because the join to observation names is by exact
    /// equality.
    #[test]
    fn names_are_lower_case_slugs() {
        for v in ALL {
            assert_eq!(v, v.to_lowercase(), "{v} is not a slug");
            assert!(!v.contains(' '), "{v} has a space — slugs are hyphenated");
        }
    }

    /// The versioned venues keep their version. Merging `minswap-v1` and
    /// `minswap-v2` would present reserves read by two different rules as one
    /// figure.
    #[test]
    fn venues_with_incompatible_contracts_stay_versioned() {
        assert_ne!(MINSWAP_V1, MINSWAP_V2);
        assert!(MINSWAP_V1.ends_with("-v1") && MINSWAP_V2.ends_with("-v2"));
    }

    /// ⚠️ EVERY POOL CONTRACT THIS CRATE CAN RECOGNISE MUST BE REGISTERED.
    ///
    /// This is the $DONUT test. `mitos-pool-observe::recognise` dispatches on
    /// these seven predicates; the trade fold reads [`SITES`]. A credential
    /// with a decoder and no site decodes its reserves perfectly and has every
    /// one of its swaps classified as a plain transfer — 765 pool sightings
    /// against 0 fills, and nothing anywhere errors.
    ///
    /// Written against the PREDICATES rather than the constants, so a venue
    /// whose credential changes is still checked against the thing that
    /// actually recognises it.
    #[test]
    fn every_recognised_pool_contract_has_a_site() {
        type Pred = fn(&[u8; 28]) -> bool;
        let recognised: [(&str, Pred); 7] = [
            ("splash", crate::splash::is_splash_pool),
            ("minswap-v1", crate::minswap::is_minswap_v1),
            ("minswap-v2", crate::minswap::is_minswap_v2),
            ("sundae-v3", crate::sundae::is_sundae_v3),
            ("sundae-v1", crate::sundae::is_sundae_v1),
            ("wingriders-v2", crate::wingriders::is_wingriders_v2),
            ("wingriders-v1", crate::wingriders::is_wingriders_v1),
        ];
        for (name, is_pool) in recognised {
            let found = SITES.iter().any(|s| match s.key {
                SiteKey::Cred(c) => s.role == SiteRole::Pool && is_pool(&c),
                // CSwap is the address-keyed one and has no cred predicate.
                SiteKey::Address(_) => false,
            });
            assert!(
                found,
                "{name} has a pool decoder and NO site — its reserves will \
                 decode and its swaps will read as transfers",
            );
        }
        // …and CSwap, which is matched on the whole address.
        assert!(SITES.iter().any(|s| matches!(
            s.key,
            SiteKey::Address(a) if a == crate::cswap::POOL_SCRIPT_ADDR
        )),);
    }

    /// A site names a venue [`ALL`] knows. A slug invented here would join
    /// nothing, which is the failure `venue.rs` was created to end.
    #[test]
    fn every_site_names_a_known_venue() {
        for s in SITES {
            assert!(ALL.contains(&s.venue), "{} is not in ALL", s.venue);
        }
    }

    /// ⚠️ One credential, one role. The same contract registered as both a
    /// pool and an order would make a wallet's payment to it read as a swap
    /// or a placement depending on iteration order.
    #[test]
    fn no_credential_carries_two_roles() {
        for (i, a) in SITES.iter().enumerate() {
            for b in SITES.iter().skip(i + 1) {
                if a.key == b.key {
                    panic!("{:?} is registered twice: {a:?} and {b:?}", a.key);
                }
            }
        }
    }

    /// Only CSwap's order contract is shared, and only an ORDER can be — a
    /// pool's counterparty is always the trader.
    #[test]
    fn only_an_order_contract_is_ever_shared() {
        for s in SITES.iter().filter(|s| s.shared) {
            assert_eq!(s.role, SiteRole::Order, "{} pool marked shared", s.venue);
        }
    }
}
