//! Typed refusals. Every message names the value that was wrong.

use std::path::PathBuf;

use crate::file::Profile;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("mitos.toml: {0}")]
    Parse(String),

    #[error("catalogue.toml: {0}")]
    CatalogueParse(String),

    #[error("unknown field `{key}` in {file}")]
    UnknownField { file: &'static str, key: String },

    #[error("listen address `{value}` is not a socket address (want host:port)")]
    InvalidListen { value: String },

    #[error("profile `{profile}` is not implemented yet (only `full` has behaviour)")]
    ProfileNotImplemented { profile: Profile },

    #[error("sha256 digest `{value}` is not 64 hex characters")]
    InvalidDigest { value: String },

    #[error(
        "catalogue path `{path}` is not a relative path inside the tarball (no `..`, not absolute)"
    )]
    CataloguePathEscape { path: String },

    #[error("tx-index is half-configured: {which} is set and the other is not (both or neither)")]
    TxIndexHalfConfigured { which: &'static str },

    #[error("chunk store path `{0}` is empty")]
    EmptyChunkStore(PathBuf),
}

impl Error {
    pub fn from_toml(file: &'static str, err: toml::de::Error) -> Self {
        let msg = err.message().to_owned();
        // serde's deny_unknown_fields: "unknown field `foo`, expected ..."
        if let Some(key) = unknown_field_name(&msg) {
            return Self::UnknownField {
                file,
                key: key.to_owned(),
            };
        }
        match file {
            "catalogue.toml" => Self::CatalogueParse(err.to_string()),
            _ => Self::Parse(err.to_string()),
        }
    }
}

fn unknown_field_name(msg: &str) -> Option<&str> {
    let rest = msg.strip_prefix("unknown field `")?;
    let key = rest.split('`').next()?;
    if key.is_empty() { None } else { Some(key) }
}

#[cfg(test)]
mod tests {
    use super::unknown_field_name;

    #[test]
    fn unknown_field_name_extracts_the_typo() {
        assert_eq!(
            unknown_field_name("unknown field `netwrok`, expected one of `network`, `profile`"),
            Some("netwrok")
        );
    }
}
