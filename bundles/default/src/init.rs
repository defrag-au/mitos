//! `mitos init`. Idempotent. `--root` prefixes every path so tests need
//! no privileges.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use mitos_config::env::DEFAULT_MITOS_TOML;
use mitos_config::{DEFAULT_AUTH_TOKEN_FILE, MitosFile};
use mitos_core::{WritePlan, write_unless_edited};

use crate::cli::InitArgs;

const UNIT_MITOS: &str = include_str!("units/mitos.service");
const UNIT_BOOTSTRAP: &str = include_str!("units/mitos-bootstrap.service");
const UNIT_SLICE: &str = include_str!("units/mitos.slice");

/// Release layout. A box we still rsync-and-build onto can drop a symlink
/// at this path (Phase 1); a stranger unpacks the tarball here.
const DEFAULT_EXEC: &str = "/usr/local/bin/mitos";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReport {
    pub steps: Vec<InitStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitStep {
    pub name: &'static str,
    pub detail: String,
}

pub fn run(args: InitArgs) -> Result<()> {
    let report = apply(&args)?;
    for step in &report.steps {
        println!("{}: {}", step.name, step.detail);
    }
    if report.steps.iter().any(|s| s.detail.contains(".new")) {
        anyhow::bail!("left .new beside one or more operator-edited files; not clobbering");
    }
    Ok(())
}

pub fn apply(args: &InitArgs) -> Result<InitReport> {
    let mut steps = Vec::new();
    let root = args.root.as_deref();
    let mut file = MitosFile::default_for(args.network, args.profile);
    if let Some(dir) = &args.data_dir {
        file.data_dir = Some(dir.clone());
    }
    let data_dir = file
        .data_dir
        .clone()
        .unwrap_or_else(mitos_config::file::default_data_dir);
    let config_path = PathBuf::from(DEFAULT_MITOS_TOML);
    let token_path = PathBuf::from(DEFAULT_AUTH_TOKEN_FILE);
    let exec = DEFAULT_EXEC;

    ensure_user(root, &mut steps)?;

    for dir in [
        data_dir.join("data"),
        data_dir.join("mitos-data"),
        data_dir.join("modules"),
        PathBuf::from("/etc/mitos"),
        PathBuf::from("/etc/systemd/system"),
    ] {
        let real = rooted(root, &dir);
        std::fs::create_dir_all(&real).with_context(|| format!("creating {}", real.display()))?;
        steps.push(InitStep {
            name: "dir",
            detail: format!("{}", real.display()),
        });
    }

    let token_real = rooted(root, &token_path);
    match write_token(&token_real)? {
        TokenWrite::Wrote => steps.push(InitStep {
            name: "auth_token",
            detail: format!("wrote {} mode 0600", token_real.display()),
        }),
        TokenWrite::Kept => steps.push(InitStep {
            name: "auth_token",
            detail: format!("unchanged {}", token_real.display()),
        }),
    }

    let toml_text = file.to_toml().context("serialise default mitos.toml")?;
    let config_real = rooted(root, &config_path);
    push_write(
        &mut steps,
        "mitos.toml",
        &config_real,
        write_unless_edited(&config_real, toml_text.as_bytes(), Some(0o644))?,
    );

    let vars = [
        (
            "__MITOS_DATA_DIR__",
            data_dir.to_str().context("data_dir utf8")?,
        ),
        ("__MITOS_CONFIG__", DEFAULT_MITOS_TOML),
        ("__MITOS_EXEC__", exec),
    ];
    write_unit(
        root,
        &mut steps,
        "mitos.service",
        &render(UNIT_MITOS, &vars),
    )?;
    write_unit(
        root,
        &mut steps,
        "mitos-bootstrap.service",
        &render(UNIT_BOOTSTRAP, &vars),
    )?;
    write_unit(root, &mut steps, "mitos.slice", UNIT_SLICE)?;

    if root.is_none() {
        let status = Command::new("systemctl")
            .arg("daemon-reload")
            .status()
            .context("systemctl daemon-reload")?;
        if !status.success() {
            anyhow::bail!("systemctl daemon-reload failed: {status}");
        }
        steps.push(InitStep {
            name: "daemon-reload",
            detail: "ok".into(),
        });
    } else {
        steps.push(InitStep {
            name: "daemon-reload",
            detail: "skipped (--root)".into(),
        });
    }

    Ok(InitReport { steps })
}

fn write_unit(
    root: Option<&Path>,
    steps: &mut Vec<InitStep>,
    name: &str,
    body: &str,
) -> Result<()> {
    let path = rooted(root, &PathBuf::from("/etc/systemd/system").join(name));
    let plan = write_unless_edited(&path, body.as_bytes(), Some(0o644))?;
    push_write(steps, "unit", &path, plan);
    Ok(())
}

fn push_write(steps: &mut Vec<InitStep>, name: &'static str, path: &Path, plan: WritePlan) {
    let detail = match plan {
        WritePlan::Wrote => format!("wrote {}", path.display()),
        WritePlan::AlreadyComplete => format!("unchanged {}", path.display()),
        WritePlan::LeftNew { new_path } => {
            format!("left {} beside {}", new_path.display(), path.display())
        }
    };
    steps.push(InitStep { name, detail });
}

fn render(template: &str, vars: &[(&str, &str)]) -> String {
    let mut out = template.to_owned();
    for (k, v) in vars {
        out = out.replace(k, v);
    }
    out
}

pub fn rooted(root: Option<&Path>, absolute: &Path) -> PathBuf {
    match root {
        None => absolute.to_owned(),
        Some(r) => {
            let stripped = absolute.strip_prefix("/").unwrap_or(absolute);
            r.join(stripped)
        }
    }
}

enum TokenWrite {
    Wrote,
    Kept,
}

fn write_token(path: &Path) -> Result<TokenWrite> {
    if path.exists() {
        return Ok(TokenWrite::Kept);
    }
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .context("open /dev/urandom")?
        .read_exact(&mut bytes)
        .context("read /dev/urandom")?;
    let hex = hex::encode(bytes);
    mitos_core::atomic_write(path, hex.as_bytes(), Some(0o600))?;
    Ok(TokenWrite::Wrote)
}

fn ensure_user(root: Option<&Path>, steps: &mut Vec<InitStep>) -> Result<()> {
    if root.is_some() {
        steps.push(InitStep {
            name: "user",
            detail: "skipped (--root)".into(),
        });
        return Ok(());
    }
    let exists = Command::new("getent")
        .args(["passwd", "mitos"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if exists {
        steps.push(InitStep {
            name: "user",
            detail: "unchanged mitos".into(),
        });
        return Ok(());
    }
    let status = Command::new("useradd")
        .args([
            "--system",
            "--home-dir",
            "/var/lib/mitos",
            "--shell",
            "/usr/sbin/nologin",
            "--user-group",
            "mitos",
        ])
        .status()
        .context("useradd mitos")?;
    if !status.success() {
        anyhow::bail!("useradd mitos failed: {status}");
    }
    steps.push(InitStep {
        name: "user",
        detail: "created mitos".into(),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{apply, rooted};
    use crate::cli::InitArgs;
    use mitos_config::{NetworkId, Profile};
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn args(root: &std::path::Path) -> InitArgs {
        InitArgs {
            network: NetworkId::Mainnet,
            profile: Profile::Full,
            data_dir: None,
            root: Some(root.to_owned()),
        }
    }

    #[test]
    fn init_is_idempotent_under_root() {
        let dir = tempfile::tempdir().unwrap();
        let report1 = apply(&args(dir.path())).unwrap();
        let report2 = apply(&args(dir.path())).unwrap();
        assert!(
            report1
                .steps
                .iter()
                .any(|s| s.name == "mitos.toml" && s.detail.starts_with("wrote"))
        );
        assert!(
            report2
                .steps
                .iter()
                .any(|s| s.name == "mitos.toml" && s.detail.starts_with("unchanged"))
        );
        assert!(
            report2
                .steps
                .iter()
                .any(|s| s.detail.contains("skipped (--root)"))
        );
    }

    #[test]
    fn token_is_0600_and_not_clobbered() {
        let dir = tempfile::tempdir().unwrap();
        apply(&args(dir.path())).unwrap();
        let token = rooted(
            Some(dir.path()),
            std::path::Path::new("/etc/mitos/auth.token"),
        );
        let mode = fs::metadata(&token).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let first = fs::read(&token).unwrap();
        apply(&args(dir.path())).unwrap();
        assert_eq!(fs::read(&token).unwrap(), first);
    }

    #[test]
    fn operator_edited_toml_is_left_with_dot_new() {
        let dir = tempfile::tempdir().unwrap();
        apply(&args(dir.path())).unwrap();
        let cfg = rooted(
            Some(dir.path()),
            std::path::Path::new("/etc/mitos/mitos.toml"),
        );
        fs::write(&cfg, "network = \"preprod\"\n").unwrap();
        let report = apply(&args(dir.path())).unwrap();
        assert!(report.steps.iter().any(|s| s.detail.contains(".new")));
        assert_eq!(fs::read_to_string(&cfg).unwrap(), "network = \"preprod\"\n");
    }

    #[test]
    fn units_embed_slice_oom_and_nofile() {
        let dir = tempfile::tempdir().unwrap();
        apply(&args(dir.path())).unwrap();
        let unit = fs::read_to_string(rooted(
            Some(dir.path()),
            std::path::Path::new("/etc/systemd/system/mitos.service"),
        ))
        .unwrap();
        assert!(unit.contains("Slice=mitos.slice"));
        assert!(unit.contains("OOMScoreAdjust=-500"));
        assert!(unit.contains("LimitNOFILE=65536"));
        assert!(unit.contains("TimeoutStopSec=90"));
        assert!(unit.contains("Requires=mitos-bootstrap.service"));
        let boot = fs::read_to_string(rooted(
            Some(dir.path()),
            std::path::Path::new("/etc/systemd/system/mitos-bootstrap.service"),
        ))
        .unwrap();
        assert!(boot.contains("ConditionPathExists=!/var/lib/mitos/.bootstrapped"));
        assert!(boot.contains("RemainAfterExit=yes"));
        assert!(boot.contains("Type=oneshot"));
    }
}
