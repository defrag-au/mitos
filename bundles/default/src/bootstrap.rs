//! `mitos bootstrap` CLI glue.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use mitos_core::{
    BootstrapPaths, DolosImporter, Fetch, MithrilFetcher, UnixDisk, mithril_from_root,
    run_bootstrap,
};

use crate::cli::{Args, BootstrapArgs};

pub fn run(parent: &Args, args: BootstrapArgs) -> Result<()> {
    let fetch = match args.fetch.as_str() {
        "never" => Fetch::Never,
        _ => Fetch::IfEmpty,
    };
    let chunk_store = args
        .chunk_store
        .clone()
        .or_else(|| chunk_store_from_mitos_toml(parent))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "--chunk-store is required (the directory that contains immutable/; \
                 set it, MITOS_CHUNK_STORE, or chunk_store in mitos.toml)"
            )
        })?;

    let (root_config, node_root) = load_root(parent)?;
    let mithril = mithril_from_root(&root_config).map_err(|e| anyhow::anyhow!("{e}"))?;
    let (store, state_file, marker) = mitos_core::paths_for(&node_root, &chunk_store);
    let importer = DolosImporter {
        config: root_config,
        chunk_size: args.chunk_size,
        chunk_store: store.clone(),
    };
    run_bootstrap(
        BootstrapPaths {
            chunk_store: &store,
            state_file: &state_file,
            marker: &marker,
        },
        fetch,
        &UnixDisk,
        &MithrilFetcher { config: mithril },
        &importer,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

fn chunk_store_from_mitos_toml(parent: &Args) -> Option<PathBuf> {
    let path = parent
        .config_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(mitos_config::env::DEFAULT_MITOS_TOML));
    let text = std::fs::read_to_string(path).ok()?;
    mitos_config::MitosFile::parse(&text).ok()?.chunk_store
}

fn load_root(parent: &Args) -> Result<(dolos_core::config::RootConfig, PathBuf)> {
    let mode = mitos_core::select_mode(
        parent.config_file.as_deref(),
        Path::new(&parent.config),
        mitos_core::default_mitos_toml_path(),
        |p| p.exists(),
    );
    match mode {
        mitos_core::ConfigMode::Legacy { dolos_toml } => {
            let cfg = mitos_core::load_legacy_dolos_toml(&dolos_toml)?;
            let node_root = PathBuf::from(&parent.config)
                .parent()
                .map(Path::to_path_buf)
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| PathBuf::from("."));
            Ok((cfg, node_root))
        }
        mitos_core::ConfigMode::Mitos { path } => {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let lookup = |k: &str| std::env::var(k).ok();
            let loaded = mitos_core::load_mitos_toml(&text, &lookup)?;
            Ok((loaded.root, loaded.resolved.data_dir))
        }
    }
}
