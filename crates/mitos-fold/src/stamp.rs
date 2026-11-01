//! Where an aggregate was built from: a base point and the identity of its source.
//!
//! A [`Stamp`] is the coordinate every artifact carries. "As of slot S" must mean the same
//! thing to every aggregate, and a reader must be able to refuse an artifact whose base is
//! not the one it expected. The [`Fingerprint`] is the opaque identity of the walk or
//! upstream artifact the aggregate was built from — never a content hash of the artifact
//! itself.

use std::fmt;
use std::str::FromStr;

use crate::{Error, Result};

/// The identity of the source an aggregate was built from.
///
/// Opaque on purpose: a fingerprint answers "was this built against the same source", not
/// "are these bytes intact". The row store's `BaseFingerprint` is one producer; its display
/// form is the same 32 lowercase hex digits, so the two convert without loss through text.
///
/// There is deliberately no `Default` and no zero constant. A source that is not known is a
/// value the caller has to name, not a value that appears when one was forgotten.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fingerprint(u128);

impl Fingerprint {
    /// Build from the 128-bit value a producer already holds.
    pub const fn from_u128(value: u128) -> Self {
        Self(value)
    }

    /// The 128-bit value, for a caller that stores fingerprints as integers.
    pub const fn to_u128(self) -> u128 {
        self.0
    }

    /// Build from 16 big-endian bytes — the order [`Display`](fmt::Display) prints.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }

    /// The 16 big-endian bytes of the value.
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }
}

impl fmt::Debug for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fingerprint({self})")
    }
}

impl fmt::Display for Fingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl FromStr for Fingerprint {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 32 {
            return Err(Error::InvalidFingerprint {
                value: value.to_owned(),
                reason: "expected exactly 32 hex digits",
            });
        }
        if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::InvalidFingerprint {
                value: value.to_owned(),
                reason: "expected only hex digits",
            });
        }
        let parsed = u128::from_str_radix(value, 16).map_err(|_| Error::InvalidFingerprint {
            value: value.to_owned(),
            reason: "the value does not parse as 128 bits",
        })?;
        Ok(Self(parsed))
    }
}

/// The base point and source identity an aggregate's artifact is stamped with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stamp {
    /// The slot of the last block the walk applied. Genesis is `0`.
    pub base_slot: u64,
    /// The block hash at `base_slot`, when the source recorded one. `None` means the source
    /// did not carry it — not a zero hash.
    pub base_hash: Option<[u8; 32]>,
    /// The identity of the walk or upstream artifact this was built from.
    pub source: Fingerprint,
}

impl Stamp {
    /// Construct a stamp. Nothing is derived here: a caller that does not know its source
    /// must not proceed by inventing one.
    pub const fn new(base_slot: u64, base_hash: Option<[u8; 32]>, source: Fingerprint) -> Self {
        Self {
            base_slot,
            base_hash,
            source,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fingerprint_prints_and_parses_as_32_hex_digits() {
        let fingerprint = Fingerprint::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        assert_eq!(fingerprint.to_string(), "0123456789abcdef0123456789abcdef");
        assert_eq!(
            fingerprint.to_string().parse::<Fingerprint>().unwrap(),
            fingerprint
        );
    }

    #[test]
    fn uppercase_hex_parses_and_prints_lowercase() {
        let fingerprint = "0123456789ABCDEF0123456789ABCDEF"
            .parse::<Fingerprint>()
            .unwrap();
        assert_eq!(fingerprint.to_string(), "0123456789abcdef0123456789abcdef");
    }

    #[test]
    fn a_fingerprint_is_strict_about_width_and_alphabet() {
        for value in [
            "",
            "0123",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789abcdef0123456789abcdeg",
            "0x23456789abcdef0123456789abcdef",
        ] {
            assert!(
                matches!(
                    value.parse::<Fingerprint>(),
                    Err(Error::InvalidFingerprint { .. })
                ),
                "{value:?} should be refused"
            );
        }
    }

    #[test]
    fn bytes_round_trip_big_endian() {
        let mut bytes = [0u8; 16];
        bytes[15] = 1;
        let fingerprint = Fingerprint::from_bytes(bytes);
        assert_eq!(fingerprint.to_u128(), 1);
        assert_eq!(fingerprint.to_bytes(), bytes);
    }

    #[test]
    fn a_stamp_keeps_an_absent_hash_absent() {
        let stamp = Stamp::new(42, None, Fingerprint::from_u128(7));
        assert_eq!(stamp.base_slot, 42);
        assert_eq!(stamp.base_hash, None);
        assert_eq!(stamp.source, Fingerprint::from_u128(7));
    }
}
