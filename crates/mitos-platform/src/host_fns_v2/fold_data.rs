//! `fold-data` host functions: the fold artifact this host serves.
//!
//! One artifact per host, named by `MITOS_FOLD_ARTIFACT` — a directory holding a manifest and
//! the files it names. Read-only by construction: there is no write path here, and a module
//! can only name a file the manifest lists (`FoldArtifact::read`).
//!
//! # Why the artifact is process-wide
//!
//! It is host configuration, in the same class as `MITOS_BOOK_DIR`: every module on this host
//! reads the same artifacts, and there is no per-module choice to make. Like the book it is
//! read once at first use rather than threaded through the host and registry constructors for
//! a value that never varies. The type is [immutable after
//! initialisation](std::sync::OnceLock), so this is shared configuration, not shared mutable
//! state.
//!
//! # A host that cannot open its artifact serves nothing
//!
//! `meta()` answers `none` and the reason goes to the log, once. The alternative — trapping
//! inside a guest call — makes a misconfiguration look like a module bug. A module that
//! requires the artifact must handle `none` anyway, because a host is allowed to serve none.

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use mitos_data_plane::FoldArtifact;

use crate::bindings_v2::FoldDataHost;
use crate::host_fns_v2::HostStateV2;

/// The artifact this host serves, opened once on first use.
static SERVED: OnceLock<Option<Arc<FoldArtifact>>> = OnceLock::new();

/// The configured artifact, or `None` when this host serves none.
fn served() -> Option<&'static Arc<FoldArtifact>> {
    SERVED
        .get_or_init(|| {
            // An empty value is unset, not a directory: a compose file can only
            // name a variable that resolves, so the local stack always exports
            // this one and leaves it empty when it serves no artifact.
            let dir = std::env::var_os("MITOS_FOLD_ARTIFACT").filter(|dir| !dir.is_empty())?;
            let dir = PathBuf::from(dir);
            match FoldArtifact::open(&dir) {
                Ok(artifact) => {
                    tracing::info!(
                        artifact = %dir.display(),
                        files = artifact.files().count(),
                        "fold artifact served",
                    );
                    Some(Arc::new(artifact))
                }
                Err(error) => {
                    tracing::error!(
                        artifact = %dir.display(),
                        %error,
                        "MITOS_FOLD_ARTIFACT names an artifact this host cannot serve; \
                         fold-data answers none",
                    );
                    None
                }
            }
        })
        .as_ref()
}

impl FoldDataHost for HostStateV2 {
    async fn meta(&mut self) -> wasmtime::Result<Option<String>> {
        Ok(served().map(|artifact| artifact.manifest().to_owned()))
    }

    async fn files(&mut self) -> wasmtime::Result<Vec<String>> {
        Ok(served()
            .map(|artifact| artifact.files().map(str::to_owned).collect())
            .unwrap_or_default())
    }

    async fn read(
        &mut self,
        file: String,
        offset: u64,
        len: u32,
    ) -> wasmtime::Result<Option<Vec<u8>>> {
        let Some(artifact) = served() else {
            return Ok(None);
        };
        match artifact.read(&file, offset, len) {
            Ok(page) => Ok(Some(page)),
            // `none` is the protocol's word for "no such file" and "past the end"; a
            // misconfigured page size is the module's own bug and says so in the log rather
            // than inside a guest call.
            Err(error) => {
                tracing::debug!(file, offset, len, %error, "fold-data read refused");
                Ok(None)
            }
        }
    }
}
