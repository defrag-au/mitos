//! Write-then-rename. "Destination exists" means the destination is complete.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WritePlan {
    /// Bytes already on disk; nothing written.
    AlreadyComplete,
    /// New file appeared via rename from a temp name.
    Wrote,
    /// Destination existed with different bytes. The new content is beside it
    /// as `*.new`; the original is untouched.
    LeftNew { new_path: PathBuf },
}

pub fn tmp_beside(path: &Path) -> PathBuf {
    let mut raw = path.as_os_str().to_owned();
    raw.push(".tmp");
    PathBuf::from(raw)
}

pub fn new_beside(path: &Path) -> PathBuf {
    let mut raw = path.as_os_str().to_owned();
    raw.push(".new");
    PathBuf::from(raw)
}

/// Write `bytes` to `path` via a sibling `.tmp` then rename. Creates parent
/// directories. Optional unix mode is applied to the temp file so the
/// destination is born with it.
pub fn atomic_write(path: &Path, bytes: &[u8], mode: Option<u32>) -> io::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let tmp = tmp_beside(path);
    fs::write(&tmp, bytes)?;
    if let Some(mode) = mode {
        set_mode(&tmp, mode)?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

pub fn write_unless_edited(path: &Path, bytes: &[u8], mode: Option<u32>) -> io::Result<WritePlan> {
    match fs::read(path) {
        Ok(existing) if existing == bytes => Ok(WritePlan::AlreadyComplete),
        Ok(_) => {
            let new_path = new_beside(path);
            atomic_write(&new_path, bytes, mode)?;
            Ok(WritePlan::LeftNew { new_path })
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            atomic_write(path, bytes, mode)?;
            Ok(WritePlan::Wrote)
        }
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{WritePlan, write_unless_edited};
    use std::fs;

    #[test]
    fn write_then_rename_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        assert_eq!(
            write_unless_edited(&path, b"one", None).unwrap(),
            WritePlan::Wrote
        );
        assert_eq!(fs::read(&path).unwrap(), b"one");
        assert!(!dir.path().join("file.tmp").exists());
    }

    #[test]
    fn identical_rewrite_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        write_unless_edited(&path, b"one", None).unwrap();
        assert_eq!(
            write_unless_edited(&path, b"one", None).unwrap(),
            WritePlan::AlreadyComplete
        );
    }

    #[test]
    fn different_bytes_leave_dot_new() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        write_unless_edited(&path, b"one", None).unwrap();
        match write_unless_edited(&path, b"two", None).unwrap() {
            WritePlan::LeftNew { new_path } => {
                assert_eq!(fs::read(&path).unwrap(), b"one");
                assert_eq!(fs::read(&new_path).unwrap(), b"two");
            }
            other => panic!("expected LeftNew, got {other:?}"),
        }
    }
}
