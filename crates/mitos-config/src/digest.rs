//! A sha256 digest as 32 bytes, shown as lowercase hex.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Sha256Hex([u8; 32]);

impl Sha256Hex {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn parse(value: &str) -> Result<Self, Error> {
        let trimmed = value.trim();
        if trimmed.len() != 64 || !trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::InvalidDigest {
                value: value.to_owned(),
            });
        }
        let mut out = [0u8; 32];
        hex::decode_to_slice(trimmed, &mut out).map_err(|_| Error::InvalidDigest {
            value: value.to_owned(),
        })?;
        Ok(Self(out))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for Sha256Hex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl Serialize for Sha256Hex {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256Hex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Sha256Hex;
    use crate::error::Error;

    #[test]
    fn parse_round_trips_lowercase() {
        let hex = "ab".repeat(32);
        let d = Sha256Hex::parse(&hex).unwrap();
        assert_eq!(d.to_hex(), hex);
    }

    #[test]
    fn parse_accepts_uppercase_and_normalises() {
        let d = Sha256Hex::parse(&"CD".repeat(32)).unwrap();
        assert_eq!(d.to_hex(), "cd".repeat(32));
    }

    #[test]
    fn parse_refuses_wrong_length() {
        match Sha256Hex::parse("deadbeef") {
            Err(Error::InvalidDigest { value }) => assert_eq!(value, "deadbeef"),
            other => panic!("expected InvalidDigest, got {other:?}"),
        }
    }
}
