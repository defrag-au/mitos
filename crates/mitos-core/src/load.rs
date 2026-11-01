//! Choose today's dolos.toml path, or a mitos.toml, without mixing them.

use std::path::{Path, PathBuf};

use dolos_core::config::RootConfig;
use mitos_config::env::DEFAULT_MITOS_TOML;
use mitos_config::{EnvLookup, MitosFile, Resolved, derive_dolos, resolve};

use crate::adapt::{materialise_genesis, to_root_config};
use crate::domain::load_config;

/// How this process was described. Legacy is today's unit files; Mitos is
/// PLAN-I. The two do not mix: a mitos.toml never reads a dolos.toml beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigMode {
    Legacy { dolos_toml: PathBuf },
    Mitos { path: PathBuf },
}

/// `config_file` is `--config-file` / `MITOS_CONFIG`. When set, we are in
/// mitos.toml mode even if the path is missing (the read then fails by name).
/// Otherwise `/etc/mitos/mitos.toml` existing is what flips the switch.
/// `exists` is injected so tests do not touch the real `/etc`.
pub fn select_mode(
    config_file: Option<&Path>,
    dolos_toml: &Path,
    default_mitos_toml: &Path,
    exists: impl Fn(&Path) -> bool,
) -> ConfigMode {
    if let Some(path) = config_file {
        return ConfigMode::Mitos {
            path: path.to_owned(),
        };
    }
    if exists(default_mitos_toml) {
        return ConfigMode::Mitos {
            path: default_mitos_toml.to_owned(),
        };
    }
    ConfigMode::Legacy {
        dolos_toml: dolos_toml.to_owned(),
    }
}

pub fn default_mitos_toml_path() -> &'static Path {
    Path::new(DEFAULT_MITOS_TOML)
}

pub struct LoadedMitos {
    pub resolved: Resolved,
    pub root: RootConfig,
}

pub fn load_mitos_toml(text: &str, lookup: EnvLookup<'_>) -> anyhow::Result<LoadedMitos> {
    let file = MitosFile::parse(text).map_err(|e| anyhow::anyhow!("{e}"))?;
    let resolved = resolve(&file, lookup).map_err(|e| anyhow::anyhow!("{e}"))?;
    let derived = derive_dolos(&resolved).map_err(|e| anyhow::anyhow!("{e}"))?;
    let genesis = materialise_genesis(&resolved.data_dir, &derived.genesis)?;
    let root = to_root_config(&derived, genesis);
    Ok(LoadedMitos { resolved, root })
}

pub fn load_legacy_dolos_toml(path: &Path) -> anyhow::Result<RootConfig> {
    load_config(path)
}

#[cfg(test)]
mod tests {
    use super::{ConfigMode, select_mode};
    use std::path::Path;

    #[test]
    fn no_mitos_toml_is_legacy() {
        let mode = select_mode(
            None,
            Path::new("dolos.toml"),
            Path::new("/etc/mitos/mitos.toml"),
            |_| false,
        );
        assert_eq!(
            mode,
            ConfigMode::Legacy {
                dolos_toml: Path::new("dolos.toml").to_owned()
            }
        );
    }

    #[test]
    fn existing_default_file_selects_mitos_mode() {
        let mode = select_mode(
            None,
            Path::new("dolos.toml"),
            Path::new("/etc/mitos/mitos.toml"),
            |p| p == Path::new("/etc/mitos/mitos.toml"),
        );
        match mode {
            ConfigMode::Mitos { path } => {
                assert_eq!(path, Path::new("/etc/mitos/mitos.toml"));
            }
            other => panic!("expected Mitos, got {other:?}"),
        }
    }

    #[test]
    fn explicit_config_file_wins_even_if_missing() {
        let mode = select_mode(
            Some(Path::new("/tmp/custom.toml")),
            Path::new("dolos.toml"),
            Path::new("/etc/mitos/mitos.toml"),
            |_| false,
        );
        match mode {
            ConfigMode::Mitos { path } => assert_eq!(path, Path::new("/tmp/custom.toml")),
            other => panic!("expected Mitos, got {other:?}"),
        }
    }
}
