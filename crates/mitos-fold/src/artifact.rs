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
use std::fmt::Write as _;
use std::path::{Component, Path};

use crate::schema::SchemaId;
use crate::stamp::{Fingerprint, Stamp};
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
    /// The manifest's file name inside an artifact directory.
    ///
    /// One name, in the contract rather than in a writer, because a reader has to find it
    /// without being told which tool wrote the artifact.
    pub const FILE: &'static str = "artifact.meta";

    /// The envelope's own format version.
    ///
    /// Separate from a schema's version on purpose: this numbers *the manifest*, while a
    /// schema numbers the rows. A new field in the manifest bumps this; a new column in a
    /// row bumps the schema.
    pub const VERSION: u32 = 1;

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

    /// Render the envelope as its manifest.
    ///
    /// The `key value` text form, the same one `live-book`'s `book.meta` uses, rather than a
    /// fourth convention: it is inspectable, diffable, and needs no codec. Scalars come
    /// first in a fixed order, then one `file` line per file and one `extension` line per
    /// extension — both repeatable, and both read in the order they are written.
    pub fn encode(&self) -> String {
        let mut out = String::with_capacity(256);
        let mut line = |key: &str, value: &dyn fmt::Display| {
            writeln!(out, "{key} {value}").expect("writing to a String cannot fail");
        };

        line("version", &Self::VERSION);
        line("kind", &self.kind);
        line("schema", &self.schema);
        line("base_slot", &self.stamp.base_slot);
        if let Some(hash) = self.stamp.base_hash {
            line("base_hash", &hex(&hash));
        }
        line("source", &self.stamp.source);
        line("rows", &self.counts.rows);
        line("entries", &self.counts.entries);
        line("bytes", &self.counts.bytes);
        for file in &self.files {
            writeln!(out, "file {} {}", file.bytes, file.name)
                .expect("writing to a String cannot fail");
        }
        for extension in &self.extensions {
            writeln!(out, "extension {} {}", extension.tag, hex(&extension.body))
                .expect("writing to a String cannot fail");
        }
        out
    }

    /// Parse a manifest, refusing anything this build does not write.
    ///
    /// `expected` is the schema the caller requires, not a hint. A schema is named by a
    /// `&'static str` in this crate because it is a property of the reader's code, so a
    /// manifest cannot introduce a schema the reader has never heard of — a version it does
    /// not know is refused by name rather than parsed and returned.
    pub fn decode(text: &str, expected: SchemaId) -> Result<Artifact> {
        let mut version = None;
        let mut kind = None;
        let mut schema = None;
        let mut base_slot = None;
        let mut base_hash = None;
        let mut source = None;
        let mut rows = None;
        let mut entries = None;
        let mut byte_count = None;
        let mut files = Vec::new();
        let mut extensions = Vec::new();

        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let (key, value) = raw.split_once(' ').ok_or_else(|| Error::InvalidEnvelope {
                reason: format!("line {line}: a field is `key value`, found {raw:?}"),
            })?;

            match key {
                "version" => once(&mut version, line, key, number(line, key, value)?)?,
                "kind" => once(&mut kind, line, key, kind_of(line, value)?)?,
                "schema" => {
                    if value != expected.to_string() {
                        return Err(Error::SchemaMismatch {
                            found: value.to_owned(),
                            expected,
                        });
                    }
                    once(&mut schema, line, key, expected)?;
                }
                "base_slot" => once(&mut base_slot, line, key, number(line, key, value)?)?,
                "base_hash" => {
                    let parsed = bytes(line, key, value)?;
                    let hash: [u8; 32] = parsed.try_into().map_err(|_| Error::InvalidEnvelope {
                        reason: format!("line {line}: `base_hash` must be 32 bytes"),
                    })?;
                    once(&mut base_hash, line, key, hash)?;
                }
                "source" => {
                    let parsed: Fingerprint =
                        value
                            .parse()
                            .map_err(|error: Error| Error::InvalidEnvelope {
                                reason: format!("line {line}: `source`: {error}"),
                            })?;
                    once(&mut source, line, key, parsed)?;
                }
                "rows" => once(&mut rows, line, key, number(line, key, value)?)?,
                "entries" => once(&mut entries, line, key, number(line, key, value)?)?,
                "bytes" => once(&mut byte_count, line, key, number(line, key, value)?)?,
                "file" => {
                    let (size, name) =
                        value
                            .split_once(' ')
                            .ok_or_else(|| Error::InvalidEnvelope {
                                reason: format!("line {line}: a file is `file <bytes> <name>`"),
                            })?;
                    let size = size.parse().map_err(|_| Error::InvalidEnvelope {
                        reason: format!("line {line}: `file` size is not a number: {size:?}"),
                    })?;
                    files.push(ArtifactFile::new(name, size)?);
                }
                "extension" => {
                    let (tag, body) =
                        value
                            .split_once(' ')
                            .ok_or_else(|| Error::InvalidEnvelope {
                                reason: format!(
                                    "line {line}: an extension is `extension <tag> <hex>`"
                                ),
                            })?;
                    let tag: u32 = tag.parse().map_err(|_| Error::InvalidEnvelope {
                        reason: format!("line {line}: `extension` tag is not a number: {tag:?}"),
                    })?;
                    extensions.push(Extension::new(tag, bytes(line, key, body)?)?);
                }
                other => {
                    return Err(Error::InvalidEnvelope {
                        reason: format!("line {line}: unknown field `{other}`"),
                    });
                }
            }
        }

        let version = required(version, "version")?;
        if version != Self::VERSION {
            return Err(Error::UnsupportedEnvelope { version });
        }
        // `schema` is written only when it matched `expected`, so its absence is the same
        // refusal as a mismatch — reported as the missing field it is.
        required(schema, "schema")?;

        Artifact::new(
            required(kind, "kind")?,
            expected,
            Stamp::new(
                required(base_slot, "base_slot")?,
                base_hash,
                required(source, "source")?,
            ),
            files,
            Counts::new(
                required(rows, "rows")?,
                required(entries, "entries")?,
                required(byte_count, "bytes")?,
            ),
            extensions,
        )
    }

    /// The files a manifest names, without the rest of it.
    ///
    /// For a reader that serves or stores an artifact **without interpreting it** — a host
    /// handing bytes to a module, a mirror, a copy. The schema and the stamp are the
    /// business of a reader that will parse rows, and this one has not claimed to be that:
    /// naming a schema it does not read would be the very guess [`Artifact::decode`]
    /// refuses.
    ///
    /// Only `file` lines are read. Every name is validated the same way [`Artifact`]
    /// validates it, so a name that could escape the artifact root is still refused.
    pub fn file_names(text: &str) -> Result<Vec<ArtifactFile>> {
        let mut files = Vec::new();
        for (index, raw) in text.lines().enumerate() {
            let line = index + 1;
            let Some(value) = raw.strip_prefix("file ") else {
                continue;
            };
            let (size, name) = value
                .split_once(' ')
                .ok_or_else(|| Error::InvalidEnvelope {
                    reason: format!("line {line}: a file is `file <bytes> <name>`"),
                })?;
            let size = size.parse().map_err(|_| Error::InvalidEnvelope {
                reason: format!("line {line}: `file` size is not a number: {size:?}"),
            })?;
            files.push(ArtifactFile::new(name, size)?);
        }
        if files.is_empty() {
            return Err(Error::EmptyArtifact);
        }
        Ok(files)
    }

    /// Refuse an artifact whose base is not the one the caller requires.
    ///
    /// Exact, all three fields. A caller that cares about fewer compares them itself; a
    /// check that silently ignored the source is the pairing hole this exists to close.
    /// The refusal names the slot and the source, which is what a reader pins a delta to.
    pub fn require_stamp(&self, expected: Stamp) -> Result<()> {
        if self.stamp != expected {
            return Err(Error::StampMismatch {
                found: describe(self.stamp),
                expected: describe(expected),
            });
        }
        Ok(())
    }
}

/// A stamp as a reader names it: the slot it is, and the source it came from.
fn describe(stamp: Stamp) -> String {
    format!("slot {} from {}", stamp.base_slot, stamp.source)
}

/// Store a scalar field, refusing a second one: a manifest with two `rows` lines has no
/// single answer, and picking either would be a guess.
fn once<T>(slot: &mut Option<T>, line: usize, key: &str, value: T) -> Result<()> {
    if slot.is_some() {
        return Err(Error::InvalidEnvelope {
            reason: format!("line {line}: `{key}` appears twice"),
        });
    }
    *slot = Some(value);
    Ok(())
}

fn required<T>(slot: Option<T>, key: &str) -> Result<T> {
    slot.ok_or_else(|| Error::InvalidEnvelope {
        reason: format!("the manifest has no `{key}`"),
    })
}

fn number<T>(line: usize, key: &str, value: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| Error::InvalidEnvelope {
        reason: format!("line {line}: `{key}` is not a number: {value:?}"),
    })
}

fn kind_of(line: usize, value: &str) -> Result<ArtifactKind> {
    match value {
        "point" => Ok(ArtifactKind::Point),
        "ordered" => Ok(ArtifactKind::Ordered),
        "summary" => Ok(ArtifactKind::Summary),
        "log" => Ok(ArtifactKind::Log),
        other => Err(Error::InvalidEnvelope {
            reason: format!("line {line}: unknown kind `{other}`"),
        }),
    }
}

const DIGITS: &[u8; 16] = b"0123456789abcdef";

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

fn bytes(line: usize, key: &str, value: &str) -> Result<Vec<u8>> {
    let nibble = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    };

    let digits = value.as_bytes();
    if !digits.len().is_multiple_of(2) {
        return Err(Error::InvalidEnvelope {
            reason: format!("line {line}: `{key}` has an odd number of hex digits"),
        });
    }
    let mut out = Vec::with_capacity(digits.len() / 2);
    for pair in digits.as_chunks::<2>().0 {
        let (Some(high), Some(low)) = (nibble(pair[0]), nibble(pair[1])) else {
            return Err(Error::InvalidEnvelope {
                reason: format!("line {line}: `{key}` is not hex: {value:?}"),
            });
        };
        out.push(high << 4 | low);
    }
    Ok(out)
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

    // ---- the manifest ----

    /// Everything the envelope can carry: two files, a base hash and an extension.
    fn rich() -> Artifact {
        Artifact::new(
            ArtifactKind::Ordered,
            SchemaId::new("holder-table", 1),
            Stamp::new(
                43_199_989,
                Some([0x51, 0x95, 0xdf, 0xa0].repeat(8).try_into().expect("32")),
                Fingerprint::from_u128(0x7cf),
            ),
            vec![file("holders.tsv", 64_929), file("parts/0001.bin", 10)],
            Counts::new(294, 1, 64_939),
            vec![Extension::new(9, vec![0x00, 0xff, 0x10]).expect("tag 9")],
        )
        .expect("a valid artifact")
    }

    #[test]
    fn an_artifact_round_trips_through_its_manifest() {
        let artifact = rich();
        let text = artifact.encode();
        let parsed = Artifact::decode(&text, artifact.schema).expect("decode");
        assert_eq!(parsed, artifact);
    }

    #[test]
    fn an_artifact_without_a_base_hash_round_trips() {
        let artifact = sample();
        assert_eq!(artifact.stamp.base_hash, None);
        let parsed = Artifact::decode(&artifact.encode(), artifact.schema).expect("decode");
        assert_eq!(parsed, artifact);
        assert_eq!(parsed.stamp.base_hash, None);
    }

    #[test]
    fn the_manifest_is_stable_across_encodes() {
        // Two runs over the same artifact produce the same bytes, which is what the
        // rebuild check compares.
        assert_eq!(rich().encode(), rich().encode());
    }

    #[test]
    fn a_schema_the_reader_does_not_know_is_refused_by_name() {
        let text = rich().encode();
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 2)).unwrap_err();
        assert!(matches!(error, Error::SchemaMismatch { .. }));
        let message = error.to_string();
        assert!(
            message.contains("holder-table/v1") && message.contains("holder-table/v2"),
            "the refusal names both versions: {message}"
        );
    }

    #[test]
    fn a_manifest_without_a_schema_is_refused() {
        let text = rich()
            .encode()
            .lines()
            .filter(|line| !line.starts_with("schema "))
            .collect::<Vec<_>>()
            .join("\n");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::InvalidEnvelope { .. }));
        assert!(error.to_string().contains("schema"), "{error}");
    }

    #[test]
    fn an_unknown_field_is_refused() {
        let mut text = rich().encode();
        text.push_str("nonsense 1\n");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::InvalidEnvelope { .. }));
        assert!(error.to_string().contains("nonsense"), "{error}");
    }

    #[test]
    fn a_field_that_appears_twice_is_refused() {
        let mut text = rich().encode();
        text.push_str("rows 7\n");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::InvalidEnvelope { .. }));
        assert!(error.to_string().contains("rows"), "{error}");
    }

    #[test]
    fn an_unsupported_envelope_version_is_refused() {
        let text = rich().encode().replace("version 1\n", "version 2\n");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::UnsupportedEnvelope { version: 2 }));
    }

    #[test]
    fn a_manifest_whose_byte_count_does_not_match_its_files_is_refused() {
        let text = rich().encode().replace("bytes 64939", "bytes 64938");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::ByteCountMismatch { .. }));
    }

    #[test]
    fn an_odd_number_of_hex_digits_is_refused() {
        let text = rich()
            .encode()
            .replace("extension 9 00ff10", "extension 9 00ff1");
        let error = Artifact::decode(&text, SchemaId::new("holder-table", 1)).unwrap_err();
        assert!(matches!(error, Error::InvalidEnvelope { .. }));
        assert!(error.to_string().contains("hex"), "{error}");
    }

    #[test]
    fn the_required_stamp_is_accepted_and_any_other_is_refused() {
        let artifact = rich();
        artifact
            .require_stamp(artifact.stamp)
            .expect("its own stamp");

        let other = Stamp::new(
            artifact.stamp.base_slot,
            None,
            Fingerprint::from_u128(0xabc),
        );
        let error = artifact.require_stamp(other).unwrap_err();
        assert!(matches!(error, Error::StampMismatch { .. }));
        let message = error.to_string();
        assert!(
            message.contains("43199989") && message.contains("00000000000000000000000000000abc"),
            "the refusal names both bases: {message}"
        );
    }
}
