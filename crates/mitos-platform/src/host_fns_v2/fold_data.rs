//! `fold-data` host functions: the fold artifact **this module instance** serves.
//!
//! # One artifact per module, resolved when the instance is built
//!
//! `MITOS_FOLD_ARTIFACT` names one of two shapes:
//!
//! - **An artifact directory** — one with its own `artifact.meta`. Every module on the
//!   host serves it. That is the single-policy host, and what `deploy/local` mounts.
//! - **A directory of artifacts named by module id** — `<root>/<module-id>/`. Each module
//!   serves its own. That is how one wasm serves several policies: a variant is a module
//!   id, every variant names the same content-addressed wasm, and the artifact is what
//!   differs.
//!
//! The resolution happens here rather than in the module because the module cannot see the
//! filesystem, and it happens *per instance* rather than once per process because two
//! modules on one host may serve different tables. The previous shape was a process-wide
//! `OnceLock`, which was right for exactly as long as the host served exactly one artifact.
//!
//! A host that cannot open its artifact serves nothing: `meta()` answers `none` and the
//! reason goes to the log, naming the module and the path. The alternative — trapping
//! inside a guest call — makes a misconfiguration look like a module bug, and a module that
//! requires the artifact has to handle `none` anyway, because a host is allowed to serve
//! nothing.
//!
//! # Why a file is only readable if the manifest names it
//!
//! `read` refuses a name the manifest does not list. That is not validation for its own
//! sake: a module is untrusted code, and a reader that took a path would let it read
//! whatever the host can reach.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mitos_data_plane::FoldArtifact;

use crate::bindings_v2::FoldDataHost;
use crate::host_fns_v2::HostStateV2;

/// The artifact `module_id` serves, opened for its instance, or `None`.
///
/// `None` is a configuration state, not a failure: a host may serve no artifact, and a
/// module that requires one is expected to say so and stay inert.
pub fn artifact_for(module_id: &str) -> Option<Arc<FoldArtifact>> {
    // An empty value is unset, not a directory: a compose file can only name a
    // variable that resolves, so the local stack always exports this one and leaves it
    // empty when it serves no artifact.
    let configured = std::env::var_os("MITOS_FOLD_ARTIFACT").filter(|dir| !dir.is_empty())?;
    artifact_in(&PathBuf::from(configured), module_id)
}

/// The resolution itself, over an explicit root — so the two shapes are testable without
/// a process-wide environment variable.
fn artifact_in(root: &Path, module_id: &str) -> Option<Arc<FoldArtifact>> {
    // A directory that is itself an artifact is served to every module: the
    // single-policy host, and the shape every existing deployment has.
    let dir = if FoldArtifact::is_artifact(root) {
        root.to_path_buf()
    } else {
        let named = root.join(module_id);
        if !FoldArtifact::is_artifact(&named) {
            tracing::debug!(
                module = %module_id,
                root = %root.display(),
                "fold-data: no artifact for this module; it serves none",
            );
            return None;
        }
        named
    };

    match FoldArtifact::open(&dir) {
        Ok(artifact) => {
            tracing::info!(
                module = %module_id,
                artifact = %dir.display(),
                files = artifact.files().count(),
                "fold artifact served",
            );
            Some(Arc::new(artifact))
        }
        Err(error) => {
            tracing::error!(
                module = %module_id,
                artifact = %dir.display(),
                %error,
                "MITOS_FOLD_ARTIFACT names an artifact this host cannot serve; \
                 fold-data answers none for this module",
            );
            None
        }
    }
}

impl FoldDataHost for HostStateV2 {
    async fn meta(&mut self) -> wasmtime::Result<Option<String>> {
        Ok(self
            .fold
            .as_ref()
            .map(|artifact| artifact.manifest().to_owned()))
    }

    async fn files(&mut self) -> wasmtime::Result<Vec<String>> {
        Ok(self
            .fold
            .as_ref()
            .map(|artifact| artifact.files().map(str::to_owned).collect())
            .unwrap_or_default())
    }

    async fn read(
        &mut self,
        file: String,
        offset: u64,
        len: u32,
    ) -> wasmtime::Result<Option<Vec<u8>>> {
        let Some(artifact) = self.fold.as_ref() else {
            return Ok(None);
        };
        match artifact.read(&file, offset, len) {
            Ok(page) => Ok(Some(page)),
            // `none` is the protocol's word for "no such file" and "past the end"; a
            // misconfigured page size is the module's own bug and says so in the log
            // rather than inside a guest call.
            Err(error) => {
                tracing::debug!(file, offset, len, %error, "fold-data read refused");
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pilot artifact, or `None` when it has not been run.
    fn pilot() -> Option<PathBuf> {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/pilot/full");
        FoldArtifact::is_artifact(&dir).then_some(dir)
    }

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mitos-fold-data-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos()),
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// A copy of `from` at `to` — the artifact is two files, and copying is what a
    /// second variant of one build looks like on disk.
    fn copy_artifact(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).expect("artifact dir");
        for entry in std::fs::read_dir(from).expect("read the artifact") {
            let entry = entry.expect("entry");
            std::fs::copy(entry.path(), to.join(entry.file_name())).expect("copy");
        }
    }

    /// The shape every existing deployment has: one directory, every module on the host.
    #[test]
    fn a_directory_that_is_an_artifact_is_served_to_every_module() {
        let Some(pilot) = pilot() else {
            eprintln!("skipping: no pilot artifact");
            return;
        };

        let first = artifact_in(&pilot, "holders-from-fold").expect("one shape");
        let second = artifact_in(&pilot, "some-other-module").expect("the same shape");
        assert_eq!(first.manifest(), second.manifest());
    }

    /// The shape that makes one wasm serve several policies: a module id names the
    /// artifact, so two modules on one host serve different tables.
    #[test]
    fn a_root_of_artifacts_serves_each_module_its_own() {
        let Some(pilot) = pilot() else {
            eprintln!("skipping: no pilot artifact");
            return;
        };
        let root = scratch("root");
        copy_artifact(&pilot, &root.join("variant-one"));
        // A directory that exists but is not an artifact is still not one.
        std::fs::create_dir_all(root.join("variant-two")).expect("an empty variant dir");

        assert!(
            artifact_in(&root, "variant-one").is_some(),
            "variant-one has its own artifact",
        );
        assert!(
            artifact_in(&root, "variant-two").is_none(),
            "a directory with no manifest is not an artifact",
        );
        assert!(
            artifact_in(&root, "variant-three").is_none(),
            "a module with no directory serves none",
        );

        std::fs::remove_dir_all(&root).ok();
    }
}
