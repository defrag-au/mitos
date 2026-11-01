//! The artifact envelope: what an aggregate produced, without the file bytes.
//!
//! The envelope is the contract a publisher, a reader and the registry's inventory share.
//! It names the shape, the schema, the base it was built at, the files that carry the rows,
//! and the counts a reader can check before trusting them.
//!
//! The bytes themselves are not here. An aggregate writes them through the walker's sink;
//! this crate only describes them.

use std::collections::HashSet;
use std::fmt;
use std::path::{Component, Path};

use crate::schema::SchemaId;
use crate::stamp::Stamp;
use crate::{Error, Result};

/// The physical shape of an artifact.
///
/// The envelope names the shape so a reader can dispatch without knowing the schema. A
/// fourth or fifth shape is allowed; it is an addition to this enum, not a new envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArtifactKind {
    /// A key → row table.
    Point,
    /// A sorted run with a directory and an opaque cursor.
    Ordered,
    /// One small typed record: counts, top-N, histogram, digest.
    Summary,
    /// A sequence of rows in chain order.
    Log,
}

impl ArtifactKind {
    /// The stable lowercase name used in an inventory or a path.
    pub const fn as_str(self) -> &'static str {
        match self {
            ArtifactKind::Point => "point",
            ArtifactKind::Ordered => "ordered",
            ArtifactKind::Summary => "summary",
            ArtifactKind::Log => "log",
        }
    }
}

impl fmt::Display for ArtifactKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One file in the artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactFile {
    /// A relative name inside the artifact directory, using `/` as the separator.
    pub name: String,
    /// The file's length. The envelope's `counts.bytes` is the sum of these.
    pub bytes: u64,
}

impl ArtifactFile {
    /// Construct a file entry, refusing a name that could escape the artifact root.
    pub fn new(name: impl Into<String>, bytes: u64) -> Result<Self> {
        let name = name.into();
        validate_file_name(&name)?;
        Ok(Self { name, bytes })
    }
}

/// What the artifact holds. `bytes` must equal the sum of [`ArtifactFile::bytes`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Rows in the primary table.
    pub rows: u64,
    /// Entries across every ordering.
    pub entries: u64,
    /// Bytes across every file.
    pub bytes: u64,
}

impl Counts {
    /// Construct a count set. Consistency with the file list is checked by
    /// [`Artifact::validate`].
    pub const fn new(rows: u64, entries: u64, bytes: u64) -> Self {
        Self {
            rows,
            entries,
            bytes,
        }
    }
}

/// An additive field a reader may ignore.
///
/// Tag 0 is reserved, so a zeroed field can never be mistaken for a real extension. A tag
/// is assigned once and never reused; a new shape is a new tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Extension {
    /// The assigned tag. Never 0.
    pub tag: u32,
    /// The tag's own encoding. Opaque to every reader that does not know the tag.
    pub body: Vec<u8>,
}

impl Extension {
    /// Construct an extension, refusing the reserved tag.
    pub fn new(tag: u32, body: impl Into<Vec<u8>>) -> Result<Self> {
        if tag == 0 {
            return Err(Error::ReservedExtensionTag);
        }
        Ok(Self {
            tag,
            body: body.into(),
        })
    }
}

/// The description of one aggregate's output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    /// The physical shape.
    pub kind: ArtifactKind,
    /// The row schema and its version.
    pub schema: SchemaId,
    /// The base point and source identity.
    pub stamp: Stamp,
    /// Every file the artifact consists of. At least one.
    pub files: Vec<ArtifactFile>,
    /// What the artifact holds.
    pub counts: Counts,
    /// Additive fields a reader may ignore.
    pub extensions: Vec<Extension>,
}

impl Artifact {
    /// Construct an artifact and validate it immediately.
    pub fn new(
        kind: ArtifactKind,
        schema: SchemaId,
        stamp: Stamp,
        files: Vec<ArtifactFile>,
        counts: Counts,
        extensions: Vec<Extension>,
    ) -> Result<Self> {
        let artifact = Self {
            kind,
            schema,
            stamp,
            files,
            counts,
            extensions,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    /// Check everything the envelope can check without reading the bytes.
    pub fn validate(&self) -> Result<()> {
        self.schema.validate()?;

        if self.files.is_empty() {
            return Err(Error::EmptyArtifact);
        }

        let mut names = HashSet::with_capacity(self.files.len());
        let mut listed = 0u64;
        for file in &self.files {
            validate_file_name(&file.name)?;
            if !names.insert(file.name.as_str()) {
                return Err(Error::DuplicateFileName {
                    name: file.name.clone(),
                });
            }
            listed = listed
                .checked_add(file.bytes)
                .ok_or(Error::ByteCountOverflow)?;
        }
        if listed != self.counts.bytes {
            return Err(Error::ByteCountMismatch {
                counted: self.counts.bytes,
                listed,
            });
        }

        let mut tags = HashSet::with_capacity(self.extensions.len());
        for extension in &self.extensions {
            if extension.tag == 0 {
                return Err(Error::ReservedExtensionTag);
            }
            if !tags.insert(extension.tag) {
                return Err(Error::DuplicateExtension { tag: extension.tag });
            }
        }

        Ok(())
    }
}

/// A file name is a relative path of normal components, `/`-separated.
///
/// This is the same rule a reader needs to keep a fetched name inside the artifact's own
/// directory. Absolute paths, `..`, `.` and Windows separators are refused rather than
/// normalised: an artifact name that means two different things on two hosts is a bug.
fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name is empty",
        });
    }
    if name.chars().any(char::is_control) {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name contains a control character",
        });
    }
    if name.contains('\\') {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the separator is `/`, not `\\`",
        });
    }
    if name.contains("//") {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name contains an empty path component",
        });
    }
    if name.ends_with('/') {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name ends with a separator",
        });
    }

    let path = Path::new(name);
    if path.is_absolute() {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name is absolute",
        });
    }
    if path
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(Error::UnsafeFileName {
            name: name.to_owned(),
            reason: "the name must be a relative path of normal components",
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stamp::Fingerprint;

    fn stamp() -> Stamp {
        Stamp::new(42, None, Fingerprint::from_u128(7))
    }

    fn file(name: &str, bytes: u64) -> ArtifactFile {
        ArtifactFile::new(name, bytes).unwrap()
    }

    fn extension(tag: u32) -> Extension {
        Extension::new(tag, vec![tag as u8]).unwrap()
    }

    fn sample() -> Artifact {
        Artifact::new(
            ArtifactKind::Summary,
            SchemaId::new("holder-summary", 1),
            stamp(),
            vec![file("summary.bin", 128)],
            Counts::new(1, 0, 128),
            vec![extension(1)],
        )
        .unwrap()
    }

    #[test]
    fn a_valid_artifact_validates() {
        let artifact = sample();
        assert_eq!(artifact.kind, ArtifactKind::Summary);
        assert_eq!(artifact.schema, SchemaId::new("holder-summary", 1));
        assert!(artifact.validate().is_ok());
    }

    #[test]
    fn an_empty_file_list_is_refused() {
        let artifact = Artifact {
            files: Vec::new(),
            counts: Counts::new(0, 0, 0),
            ..sample()
        };
        assert!(matches!(artifact.validate(), Err(Error::EmptyArtifact)));
    }

    #[test]
    fn a_name_that_escapes_the_artifact_root_is_refused() {
        for name in [
            "",
            "/absolute",
            "../escape",
            "a/../../b",
            "./relative",
            "a\\b",
            "dir/",
            "a//b",
        ] {
            let error = ArtifactFile::new(name, 1).unwrap_err();
            assert!(
                matches!(error, Error::UnsafeFileName { .. }),
                "{name:?} should be refused"
            );
        }
    }

    #[test]
    fn a_nested_relative_name_is_allowed() {
        let file = ArtifactFile::new("parts/0001.bin", 10).unwrap();
        assert_eq!(file.name, "parts/0001.bin");
        assert_eq!(file.bytes, 10);
    }

    #[test]
    fn a_zero_byte_file_is_allowed() {
        let artifact = Artifact::new(
            ArtifactKind::Ordered,
            SchemaId::new("empty-run", 1),
            stamp(),
            vec![file("run.idx", 0)],
            Counts::new(0, 0, 0),
            Vec::new(),
        )
        .unwrap();
        assert!(artifact.validate().is_ok());
    }

    #[test]
    fn a_duplicate_file_name_is_refused() {
        let artifact = Artifact {
            files: vec![file("rows.bin", 1), file("rows.bin", 1)],
            counts: Counts::new(0, 0, 2),
            ..sample()
        };
        assert!(matches!(
            artifact.validate(),
            Err(Error::DuplicateFileName { .. })
        ));
    }

    #[test]
    fn a_byte_count_that_does_not_match_the_files_is_refused() {
        let artifact = Artifact {
            counts: Counts::new(0, 0, 127),
            ..sample()
        };
        assert!(matches!(
            artifact.validate(),
            Err(Error::ByteCountMismatch {
                counted: 127,
                listed: 128
            })
        ));
    }

    #[test]
    fn a_byte_count_overflow_is_refused() {
        let artifact = Artifact {
            files: vec![file("a.bin", u64::MAX), file("b.bin", 1)],
            counts: Counts::new(0, 0, 0),
            ..sample()
        };
        assert!(matches!(artifact.validate(), Err(Error::ByteCountOverflow)));
    }

    #[test]
    fn extension_tag_zero_is_reserved() {
        let error = Extension::new(0, Vec::new()).unwrap_err();
        assert!(matches!(error, Error::ReservedExtensionTag));

        let artifact = Artifact {
            extensions: vec![Extension {
                tag: 0,
                body: Vec::new(),
            }],
            ..sample()
        };
        assert!(matches!(
            artifact.validate(),
            Err(Error::ReservedExtensionTag)
        ));
    }

    #[test]
    fn duplicate_extension_tags_are_refused() {
        let artifact = Artifact {
            extensions: vec![extension(2), extension(2)],
            ..sample()
        };
        assert!(matches!(
            artifact.validate(),
            Err(Error::DuplicateExtension { tag: 2 })
        ));
    }

    #[test]
    fn the_kind_has_a_stable_name() {
        assert_eq!(ArtifactKind::Point.as_str(), "point");
        assert_eq!(ArtifactKind::Ordered.as_str(), "ordered");
        assert_eq!(ArtifactKind::Summary.as_str(), "summary");
        assert_eq!(ArtifactKind::Log.as_str(), "log");
        assert_eq!(ArtifactKind::Summary.to_string(), "summary");
    }
}
