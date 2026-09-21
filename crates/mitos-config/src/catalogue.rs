//! `catalogue.toml` from the modules release tarball.
//!
//! Defined here (Phase 1 writes it; Phase 4 reads it) so the type exists
//! before the release job. Digests are of FILE BYTES, matching
//! `BuiltModule.{wasm,manifest}`.

use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::digest::Sha256Hex;
use crate::error::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalogue {
    pub version: u32,
    #[serde(default)]
    pub modules: Vec<CatalogueEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogueEntry {
    pub id: String,
    pub wasm_sha256: Sha256Hex,
    pub manifest_sha256: Sha256Hex,
    pub revision: u32,
    /// Relative directory of this module inside the tarball
    /// (`<name>/` holding `<name>.wasm` + `manifest.toml` + `built-module.json`).
    pub path: String,
}

impl Catalogue {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let cat: Self = toml::from_str(text).map_err(|e| Error::from_toml("catalogue.toml", e))?;
        cat.validate()?;
        Ok(cat)
    }

    pub fn validate(&self) -> Result<(), Error> {
        for entry in &self.modules {
            refuse_escape(&entry.path)?;
        }
        Ok(())
    }

    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

fn refuse_escape(path: &str) -> Result<(), Error> {
    let p = Path::new(path);
    if path.is_empty() || p.is_absolute() || path.starts_with('/') {
        return Err(Error::CataloguePathEscape {
            path: path.to_owned(),
        });
    }
    if p.components()
        .any(|c| matches!(c, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(Error::CataloguePathEscape {
            path: path.to_owned(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Catalogue;
    use crate::error::Error;

    const SAMPLE: &str = r#"
version = 1

[[modules]]
id = "collection-holders"
wasm_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
manifest_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
revision = 3
path = "collection-holders/"
"#;

    #[test]
    fn round_trip() {
        let cat = Catalogue::parse(SAMPLE).unwrap();
        assert_eq!(cat.modules.len(), 1);
        assert_eq!(cat.modules[0].id, "collection-holders");
        assert_eq!(cat.modules[0].revision, 3);
        let again = Catalogue::parse(&cat.to_toml().unwrap()).unwrap();
        assert_eq!(cat, again);
    }

    #[test]
    fn unknown_field_names_the_key() {
        let err = Catalogue::parse("version = 1\nextra = true\n").unwrap_err();
        match err {
            Error::UnknownField { file, key } => {
                assert_eq!(file, "catalogue.toml");
                assert_eq!(key, "extra");
            }
            other => panic!("expected UnknownField, got {other}"),
        }
    }

    #[test]
    fn parent_dir_path_is_refused() {
        let text = r#"
version = 1
[[modules]]
id = "x"
wasm_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
manifest_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
revision = 1
path = "../etc"
"#;
        match Catalogue::parse(text) {
            Err(Error::CataloguePathEscape { path }) => assert_eq!(path, "../etc"),
            other => panic!("expected CataloguePathEscape, got {other:?}"),
        }
    }

    #[test]
    fn absolute_path_is_refused() {
        let text = r#"
version = 1
[[modules]]
id = "x"
wasm_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
manifest_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
revision = 1
path = "/etc/passwd"
"#;
        match Catalogue::parse(text) {
            Err(Error::CataloguePathEscape { .. }) => {}
            other => panic!("expected CataloguePathEscape, got {other:?}"),
        }
    }
}
