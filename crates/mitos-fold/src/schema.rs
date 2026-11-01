//! The stable name of an aggregate's output shape.
//!
//! A schema is `kind` + `version`, and both are part of the artifact's identity. The kind
//! is a compile-time identifier, not a runtime string: an aggregate's schema is a property
//! of its code. The version is the only thing that changes when the fields do, and a reader
//! that does not know it must refuse the artifact rather than guess.

use std::fmt;

use crate::{Error, Result};

/// The name and version of an artifact's row shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaId {
    kind: &'static str,
    version: u32,
}

impl SchemaId {
    /// Construct a schema id. Validation happens in [`SchemaId::validate`], which the
    /// registry calls at registration and the artifact writer calls before publishing.
    pub const fn new(kind: &'static str, version: u32) -> Self {
        Self { kind, version }
    }

    /// The identifier — lowercase ASCII letters, digits and hyphens.
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    /// The shape version. At least 1.
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Refuse a schema id that cannot safely name an artifact.
    ///
    /// The kind becomes a file name and a registry key, so it is restricted to characters
    /// that are unambiguous in both. The version starts at 1: zero would be indistinguishable
    /// from a field that was never written.
    pub fn validate(&self) -> Result<()> {
        if self.version == 0 {
            return Err(Error::InvalidSchema {
                schema: *self,
                reason: "the version must be at least 1",
            });
        }
        if self.kind.is_empty() {
            return Err(Error::InvalidSchema {
                schema: *self,
                reason: "the kind is empty",
            });
        }
        if !self
            .kind
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(Error::InvalidSchema {
                schema: *self,
                reason: "the kind must be lowercase ASCII letters, digits or hyphens",
            });
        }
        if self.kind.starts_with('-') || self.kind.ends_with('-') {
            return Err(Error::InvalidSchema {
                schema: *self,
                reason: "the kind must not start or end with a hyphen",
            });
        }
        Ok(())
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/v{}", self.kind, self.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_valid_schema_exposes_its_parts() {
        let schema = SchemaId::new("holder-table", 3);
        assert_eq!(schema.kind(), "holder-table");
        assert_eq!(schema.version(), 3);
        assert_eq!(schema.to_string(), "holder-table/v3");
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn version_zero_is_refused() {
        let error = SchemaId::new("holder-table", 0).validate().unwrap_err();
        assert!(matches!(error, Error::InvalidSchema { .. }));
    }

    #[test]
    fn an_empty_kind_is_refused() {
        let error = SchemaId::new("", 1).validate().unwrap_err();
        assert!(matches!(error, Error::InvalidSchema { .. }));
    }

    #[test]
    fn uppercase_and_punctuation_are_refused() {
        for kind in [
            "Holder-Table",
            "holder_table",
            "holder.table",
            "holder table",
        ] {
            let error = SchemaId::new(kind, 1).validate().unwrap_err();
            assert!(
                matches!(error, Error::InvalidSchema { .. }),
                "{kind} should be refused"
            );
        }
    }

    #[test]
    fn a_leading_or_trailing_hyphen_is_refused() {
        for kind in ["-holder", "holder-"] {
            let error = SchemaId::new(kind, 1).validate().unwrap_err();
            assert!(
                matches!(error, Error::InvalidSchema { .. }),
                "{kind} should be refused"
            );
        }
    }
}
