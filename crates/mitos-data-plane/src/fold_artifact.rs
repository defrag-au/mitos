//! A fold artifact, read the way a host will serve it.
//!
//! A fold artifact is a directory: the manifest ([`Artifact::FILE`]) and the files it names.
//! `mitos-fold` owns the description and the text form; this reads one from disk and answers
//! the three questions a consumer has — *what does it say*, *what is in it*, *give me these
//! bytes* — without handing out a path.
//!
//! # Two readers, deliberately
//!
//! [`FoldArtifact::open`] checks the files and nothing else: it is for a reader that will
//! **serve or store** the artifact without interpreting it, which is what a host does. Such a
//! reader does not name a schema, because naming one it does not read would be a guess.
//!
//! [`FoldArtifact::parse`] adds the schema check: it is for a reader that will **parse rows**
//! and therefore knows what it requires. That is the contract's rule — the reader names the
//! schema, and a manifest naming another is refused by name.
//!
//! # Why the bytes are paged and not parsed
//!
//! The row encoding is the one part of an artifact expected to change: the holder table is
//! TSV today and a binary sorted array later (`docs/design/FOLD_PLANE.md` §7). A reader here
//! that parsed rows would have to change with it, in the one place every consumer shares. So
//! this hands back bytes by offset and the *consumer* owns the row format — the same division
//! the envelope itself makes.
//!
//! # Why a file is only readable if the manifest names it
//!
//! `read` refuses a name the manifest does not list. That is not validation for its own sake:
//! a module is untrusted code, and a reader that took a path would let it read whatever the
//! host can reach.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use mitos_fold::{Artifact, ArtifactFile, SchemaId, Stamp};

/// The largest page a caller may ask for in one call.
///
/// A cap rather than a convention: the bytes cross into a wasm module, and an unbounded
/// `len` is a request to materialise an arbitrarily large file in a linear memory the host
/// does not control.
pub const MAX_PAGE: u32 = 256 * 1024;

/// A fold artifact on disk, opened.
#[derive(Debug)]
pub struct FoldArtifact {
    dir: PathBuf,
    manifest: String,
    files: Vec<ArtifactFile>,
}

/// Why an artifact could not be opened or read.
#[derive(Debug, thiserror::Error)]
pub enum FoldError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("the manifest at {path} is not one this reader accepts: {reason}")]
    Manifest { path: PathBuf, reason: String },
    #[error("the manifest names no file `{name}`")]
    UnknownFile { name: String },
    #[error("`{name}` is {found} bytes; the manifest says {expected}")]
    LengthMismatch {
        name: String,
        found: u64,
        expected: u64,
    },
    #[error("`{name}` has no bytes at offset {offset}")]
    OutOfRange { name: String, offset: u64 },
    #[error("a page of {len} bytes exceeds the {MAX_PAGE}-byte limit")]
    PageTooLarge { len: u32 },
}

impl FoldArtifact {
    /// Whether `dir` is an artifact directory — one with a manifest to name.
    ///
    /// A question about the layout, asked where the layout is known. A host that serves
    /// several artifacts has to tell one artifact from a directory *of* them without
    /// opening either, otherwise it would have to reconstruct the manifest's own file
    /// name — which is the contract's business, not the caller's.
    pub fn is_artifact(dir: &Path) -> bool {
        dir.join(Artifact::FILE).is_file()
    }

    /// Open `dir` and check that every file the manifest names is the length it claims.
    ///
    /// A file whose length is not the manifest's is an artifact a writer did not finish, and
    /// serving it would hand a consumer rows it cannot trust. That check needs no schema, so
    /// neither does this.
    pub fn open(dir: &Path) -> Result<FoldArtifact, FoldError> {
        let manifest_path = dir.join(Artifact::FILE);
        let manifest = std::fs::read_to_string(&manifest_path).map_err(|source| FoldError::Io {
            path: manifest_path.clone(),
            source,
        })?;
        let files = Artifact::file_names(&manifest).map_err(|error| FoldError::Manifest {
            path: manifest_path,
            reason: error.to_string(),
        })?;

        for file in &files {
            let path = dir.join(&file.name);
            let found = std::fs::metadata(&path)
                .map_err(|source| FoldError::Io {
                    path: path.clone(),
                    source,
                })?
                .len();
            if found != file.bytes {
                return Err(FoldError::LengthMismatch {
                    name: file.name.clone(),
                    found,
                    expected: file.bytes,
                });
            }
        }

        Ok(FoldArtifact {
            dir: dir.to_path_buf(),
            manifest,
            files,
        })
    }

    /// The manifest, verbatim.
    ///
    /// Handed over whole and uninterpreted: a consumer that will parse rows is the one that
    /// checks the schema, the stamp and the counts.
    pub fn manifest(&self) -> &str {
        &self.manifest
    }

    /// Parse the manifest as `schema`, refusing any other.
    ///
    /// This is the check a row-reading consumer makes. It re-validates the manifest in full,
    /// so the counters it returns are the ones the writer wrote rather than the ones
    /// [`FoldArtifact::open`] happened to need.
    pub fn parse(&self, schema: SchemaId) -> Result<Artifact, FoldError> {
        Artifact::decode(&self.manifest, schema).map_err(|error| FoldError::Manifest {
            path: self.dir.join(Artifact::FILE),
            reason: error.to_string(),
        })
    }

    /// The base every consumer coordinates on, when the manifest is one this reader parses.
    pub fn stamp(&self, schema: SchemaId) -> Result<Stamp, FoldError> {
        Ok(self.parse(schema)?.stamp)
    }

    /// The names the manifest lists, in the order it lists them.
    pub fn files(&self) -> impl Iterator<Item = &str> {
        self.files.iter().map(|file| file.name.as_str())
    }

    /// The length the manifest claims for `name`.
    pub fn file_bytes(&self, name: &str) -> Option<u64> {
        self.files
            .iter()
            .find(|file| file.name == name)
            .map(|file| file.bytes)
    }

    /// Up to `len` bytes of `name` at `offset`.
    ///
    /// Refuses a name the manifest does not list, and a `len` above [`MAX_PAGE`]. A short
    /// read at the end of a file is the tail, not an error — the caller asks again and gets
    /// nothing; asking *at* or past the end is [`FoldError::OutOfRange`], because that is a
    /// caller that has lost its place rather than one that has finished.
    pub fn read(&self, name: &str, offset: u64, len: u32) -> Result<Vec<u8>, FoldError> {
        if len > MAX_PAGE {
            return Err(FoldError::PageTooLarge { len });
        }
        let size = self
            .file_bytes(name)
            .ok_or_else(|| FoldError::UnknownFile {
                name: name.to_owned(),
            })?;
        if offset >= size {
            return Err(FoldError::OutOfRange {
                name: name.to_owned(),
                offset,
            });
        }

        let path = self.dir.join(name);
        let mut handle = std::fs::File::open(&path).map_err(|source| FoldError::Io {
            path: path.clone(),
            source,
        })?;
        handle
            .seek(SeekFrom::Start(offset))
            .map_err(|source| FoldError::Io {
                path: path.clone(),
                source,
            })?;

        let mut page =
            Vec::with_capacity(usize::try_from(u64::from(len)).unwrap_or(MAX_PAGE as usize));
        handle
            .take(len.into())
            .read_to_end(&mut page)
            .map_err(|source| FoldError::Io { path, source })?;
        Ok(page)
    }

    /// The whole of a named file.
    ///
    /// For a consumer that knows the file is small — a summary, a manifest-sized table. A
    /// consumer reading rows should page instead.
    pub fn read_all(&self, name: &str) -> Result<Vec<u8>, FoldError> {
        if self.file_bytes(name).is_none() {
            return Err(FoldError::UnknownFile {
                name: name.to_owned(),
            });
        }
        let path = self.dir.join(name);
        std::fs::read(&path).map_err(|source| FoldError::Io { path, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pilot_dir() -> Option<PathBuf> {
        let dir = if let Some(path) = std::env::var_os("MITOS_PILOT_DIR") {
            PathBuf::from(path)
        } else {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/pilot/full")
        };
        dir.join(Artifact::FILE).exists().then_some(dir)
    }

    fn artifact() -> Option<FoldArtifact> {
        let dir = pilot_dir()?;
        Some(FoldArtifact::open(&dir).expect("the pilot artifact opens"))
    }

    #[test]
    fn the_pilot_artifact_opens_and_names_its_files() {
        let Some(artifact) = artifact() else {
            eprintln!("skipping: no pilot artifact (run `fold-probe pilot`)");
            return;
        };

        assert!(artifact.files().any(|name| name == "holders.tsv"));
        let rows = artifact.file_bytes("holders.tsv").expect("a rows file");
        assert_eq!(
            rows as usize,
            artifact.read_all("holders.tsv").unwrap().len()
        );
    }

    #[test]
    fn the_manifest_parses_as_its_own_schema_and_no_other() {
        let Some(artifact) = artifact() else {
            return;
        };
        let parsed = artifact
            .parse(SchemaId::new("holder-table", 1))
            .expect("the pilot's schema");
        assert_eq!(parsed.stamp.base_slot, 43_199_989);
        assert_eq!(
            parsed.counts.bytes,
            artifact.file_bytes("holders.tsv").unwrap()
        );

        let error = artifact
            .parse(SchemaId::new("holder-table", 99))
            .unwrap_err();
        assert!(matches!(error, FoldError::Manifest { .. }));
    }

    #[test]
    fn a_page_is_the_first_bytes_of_the_file() {
        let Some(artifact) = artifact() else {
            return;
        };
        let whole = artifact.read_all("holders.tsv").expect("read all");
        let page = artifact.read("holders.tsv", 0, 64).expect("read a page");
        assert_eq!(page, whole[..64]);
        assert_eq!(
            &page[..7],
            b"holder\t",
            "the first page starts at the header"
        );
    }

    #[test]
    fn paging_without_gaps_rebuilds_the_file() {
        let Some(artifact) = artifact() else {
            return;
        };
        let whole = artifact.read_all("holders.tsv").expect("read all");

        let mut rebuilt = Vec::new();
        let mut offset = 0u64;
        while offset < whole.len() as u64 {
            let page = artifact
                .read("holders.tsv", offset, 4096)
                .expect("read a page");
            assert!(!page.is_empty(), "a page inside the file is not empty");
            rebuilt.extend_from_slice(&page);
            offset += page.len() as u64;
        }
        assert_eq!(rebuilt, whole);
    }

    #[test]
    fn a_file_the_manifest_does_not_name_is_refused() {
        let Some(artifact) = artifact() else {
            return;
        };
        let error = artifact.read("../../etc/passwd", 0, 16).unwrap_err();
        assert!(matches!(error, FoldError::UnknownFile { .. }));
    }

    #[test]
    fn reading_past_the_end_is_refused_and_a_page_is_capped() {
        let Some(artifact) = artifact() else {
            return;
        };
        let bytes = artifact.file_bytes("holders.tsv").unwrap();
        let error = artifact.read("holders.tsv", bytes, 16).unwrap_err();
        assert!(matches!(error, FoldError::OutOfRange { .. }));

        let error = artifact.read("holders.tsv", 0, MAX_PAGE + 1).unwrap_err();
        assert!(matches!(error, FoldError::PageTooLarge { .. }));
    }
}
