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
}
