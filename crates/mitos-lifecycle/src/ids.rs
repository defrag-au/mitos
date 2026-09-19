//! The identifiers of platform v3 — §3 "Model" of the design.
//!
//! Every one of these ends up in a filesystem path on the host, so each is
//! validated on the way IN (including through serde) and there is no way to
//! hold an unvalidated one. The rules are the ones the v2 host already applies
//! (`manifest::validate_module_id`, `companions::validate_client_id`), moved
//! here so they exist once.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("{kind} `{value}` is invalid: {why}")]
    Invalid {
        kind: &'static str,
        value: String,
        why: &'static str,
    },
}

/// A validated string newtype. `try_from`/`into` String for serde, so a bad
/// value in a persisted file or on the wire is a deserialisation error rather
/// than something a later `join()` turns into a path traversal.
macro_rules! validated_id {
    ($(#[$meta:meta])* $name:ident, $kind:literal, $validate:path) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: &str) -> Result<Self, IdError> {
                match $validate(value) {
                    Ok(()) => Ok(Self(value.to_owned())),
                    Err(why) => Err(IdError::Invalid {
                        kind: $kind,
                        value: value.to_owned(),
                        why,
                    }),
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(&value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

fn slug(value: &str, max: usize) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > max {
        return Err("length out of range");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("only [a-z0-9-] is allowed");
    }
    Ok(())
}

fn module_id(value: &str) -> Result<(), &'static str> {
    slug(value, 64)
}

fn namespace(value: &str) -> Result<(), &'static str> {
    slug(value, 32)
}

/// Permissive of `.` because client ids are worker hostnames today
/// (`ownership.cnft.dev`, `hooks.epochify.space`). A leading dot is refused so
/// the host's dot-prefixed reserved directories stay distinguishable.
fn client_id(value: &str) -> Result<(), &'static str> {
    if value.is_empty() || value.len() > 128 {
        return Err("length out of range");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return Err("only [A-Za-z0-9._-] is allowed");
    }
    if value.starts_with('.') {
        return Err("must not start with `.`");
    }
    Ok(())
}

fn sha256_hex(value: &str) -> Result<(), &'static str> {
    if value.len() != 64 {
        return Err("must be 64 hex characters");
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
    {
        return Err("must be lowercase hex");
    }
    Ok(())
}

validated_id!(
    /// A module's name: `wayup-store-offer`. Names a LINEAGE of artifacts, not
    /// one of them — see [`InstanceKey`] for what actually runs.
    ModuleId,
    "module id",
    module_id
);

validated_id!(
    /// A config-defined isolation domain (`prod`, `dev`). Owns a token, a set
    /// of allowed client ids and a [`crate::policy::NamespaceProfile`].
    Namespace,
    "namespace",
    namespace
);

validated_id!(
    /// A worker deployment — atomic across all of its companion DOs.
    ClientId,
    "client id",
    client_id
);

validated_id!(
    /// Lowercase hex SHA-256. Lowercase only, so two spellings of one digest
    /// can never be two identities.
    Sha256Hex,
    "sha256",
    sha256_hex
);

/// A client, scoped to its namespace. The same hostname under `dev` and `prod`
/// tokens is two clients.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ClientKey {
    pub namespace: Namespace,
    pub client: ClientId,
}

impl fmt::Display for ClientKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.client)
    }
}

/// What actually runs: one follower, one cursor, one kv, one emissions store.
///
/// Identity is the artifact AND its config, because two clients can announce
/// the same wasm with a different `config.cbor` (`jpg-store-offer` carries a
/// script address) and those must not share state. It is NOT tied to a
/// namespace: `dev` and `prod` announcing the same pair share one instance,
/// which is the steady state and the reason forking is cheap.
///
/// A struct rather than a derived hash on purpose — identity by value needs no
/// hasher in a pure crate, and the fields stay legible in a journal entry.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct InstanceKey {
    pub module: ModuleId,
    pub wasm: Sha256Hex,
    /// `None` = the module is started with no config (`init(&[])`), which the
    /// host already distinguishes from an empty config.
    pub config: Option<Sha256Hex>,
}

impl InstanceKey {
    /// Directory name under `<modules>/<module>/`. Truncated digests: 16 hex
    /// characters each is 64 bits per half, and a collision would additionally
    /// need to be within ONE module's handful of live instances.
    pub fn dir_name(&self) -> String {
        let wasm = &self.wasm.as_str()[..16];
        match &self.config {
            Some(config) => format!("{wasm}-{}", &config.as_str()[..16]),
            None => format!("{wasm}-noconfig"),
        }
    }
}

impl fmt::Display for InstanceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.module, self.dir_name())
    }
}

/// A module lineage's monotonic build counter. Drives the upgrade-action
/// ledger ([`crate::upgrade`]) and NOTHING else — it is not identity. Two
/// digests may share a revision (a dev rebuild without a bump); that is a move
/// with no actions, not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Revision(pub u32);

/// A slot number. Only ever compared, never converted to time here — that
/// needs an era offset and fails silently by weeks when guessed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Slot(pub u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_refuse_anything_that_could_move_a_path() {
        for bad in ["", "../x", "a/b", "A", "a b", "a\0"] {
            assert!(ModuleId::parse(bad).is_err(), "module id accepted {bad:?}");
            assert!(Namespace::parse(bad).is_err(), "namespace accepted {bad:?}");
        }
        for bad in ["", ".", "..", ".hidden", "a/b", "a\\b"] {
            assert!(ClientId::parse(bad).is_err(), "client id accepted {bad:?}");
        }
        assert!(ClientId::parse("hooks.epochify.space").is_ok());
    }

    #[test]
    fn a_bad_id_is_a_deserialisation_error_not_a_value() {
        let err = toml::from_str::<ClientKey>("namespace = \"prod\"\nclient = \"../etc\"");
        assert!(err.is_err());
    }

    #[test]
    fn uppercase_hex_is_a_different_spelling_and_is_refused() {
        let lower = "ab".repeat(32);
        assert!(Sha256Hex::parse(&lower).is_ok());
        assert!(Sha256Hex::parse(&lower.to_uppercase()).is_err());
    }

    #[test]
    fn config_is_part_of_instance_identity() {
        let module = ModuleId::parse("jpg-store-offer").unwrap();
        let wasm = Sha256Hex::parse(&"11".repeat(32)).unwrap();
        let a = InstanceKey {
            module: module.clone(),
            wasm: wasm.clone(),
            config: Some(Sha256Hex::parse(&"22".repeat(32)).unwrap()),
        };
        let b = InstanceKey {
            module,
            wasm,
            config: None,
        };
        assert_ne!(a, b);
        assert_ne!(a.dir_name(), b.dir_name());
    }
}
