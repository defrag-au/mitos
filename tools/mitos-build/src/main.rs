//! `mitos-build`: build a wasm-module crate into a deployable
//! artifact directory.
//!
//! Mirrors `worker-build`'s shape: one focused tool, produces an
//! artifact ready for `mitos-cli upload-module` to consume.
//!
//! See `MITOS_PLATFORM_DEPLOYMENT.md` §2 for the manifest
//! schema and §"Phase 2 — `mitos-build` tool" for the
//! delivery sequence.
//!
//! Usage:
//!   cd <my-dapp>/
//!   mitos-build --crate indexer
//!   # or, from a workspace member with [package.metadata.mitos]:
//!   mitos-build
//!
//! Outputs (under `target/mitos/<id>/`):
//!   - `<id>.wasm` — the binary
//!   - `manifest.toml` — auto-generated; never hand-edit

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, anyhow};
use clap::Parser;
use mitos_platform::inspect::{InspectResult, dry_inspect};
use mitos_platform::manifest::{
    AbiSection, BuildSection, Manifest, ModuleSection, TrapPolicySection, sha256_hex,
};

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Build a mitos wasm-module crate into a deployable artifact"
)]
struct Args {
    /// Workspace member to build. Should be the crate that
    /// contains `wit_bindgen::generate!` + `impl Guest`. Used
    /// as the `cargo build -p` argument.
    #[arg(long)]
    crate_name: String,

    /// Module ID written to the manifest. Defaults to the
    /// crate name with underscores replaced by hyphens. Must
    /// match `[a-z0-9-]+` (max 64 chars).
    #[arg(long)]
    module_id: Option<String>,

    /// Workspace root. Defaults to current directory.
    #[arg(long, default_value = ".")]
    workspace: PathBuf,

    /// Cargo profile. v1 supports release only — debug-built
    /// modules trigger fuel exhaustion under realistic block
    /// sizes.
    #[arg(long, default_value = "release")]
    profile: String,

    /// Where to emit the artifact. Defaults to
    /// `<workspace>/target/mitos/<module-id>/`.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Skip running cargo, use the wasm at `--wasm-path` directly.
    /// Useful for CI pipelines that already build separately.
    #[arg(long)]
    wasm_path: Option<PathBuf>,

    /// Dry-run: validate + summarise without writing the
    /// artifact directory. Returns non-zero if validation fails.
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let module_id = args
        .module_id
        .clone()
        .unwrap_or_else(|| args.crate_name.replace('_', "-"));

    // 1. Build (or accept a pre-built path).
    let wasm_path = match args.wasm_path.as_ref() {
        Some(p) => p.clone(),
        None => cargo_build(&args)?,
    };
    let wasm_bytes =
        std::fs::read(&wasm_path).with_context(|| format!("reading {}", wasm_path.display()))?;
    tracing::info!(
        path = %wasm_path.display(),
        bytes = wasm_bytes.len(),
        "wasm built/loaded",
    );

    // 2. Wasmtime dry-load — the strongest signal of correctness.
    let inspect = dry_inspect(&wasm_path)
        .await
        .map_err(|e| anyhow!("dry_inspect: {e}"))?;
    log_inspect(&inspect);

    // 3. Build the manifest from inspected values + build metadata.
    let manifest = build_manifest(&module_id, &wasm_bytes, &inspect, &args)?;

    if args.dry_run {
        println!("--dry-run: not writing artifact");
        println!("{}", manifest.to_toml().context("serialise manifest")?);
        return Ok(());
    }

    // 4. Emit artifact directory.
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| args.workspace.join("target").join("mitos").join(&module_id));
    std::fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;
    let wasm_out = out.join(format!("{module_id}.wasm"));
    std::fs::copy(&wasm_path, &wasm_out)
        .with_context(|| format!("copying wasm to {}", wasm_out.display()))?;
    let manifest_out = out.join("manifest.toml");
    std::fs::write(&manifest_out, manifest.to_toml().context("manifest toml")?)
        .with_context(|| format!("writing {}", manifest_out.display()))?;

    // If a `mitos.toml` is colocated with the module workspace,
    // CBOR-encode its parsed value tree as `config.cbor` next to
    // the wasm. The module's `init` deserialises this back into
    // its `Config` shape via ciborium. Wrangler-style: human
    // edits TOML, machine ships CBOR.
    let mitos_toml = args.workspace.join("mitos.toml");
    if mitos_toml.exists() {
        let toml_str = std::fs::read_to_string(&mitos_toml)
            .with_context(|| format!("reading {}", mitos_toml.display()))?;
        let value: toml::Value = toml::from_str(&toml_str)
            .with_context(|| format!("parsing {}", mitos_toml.display()))?;
        let mut buf = Vec::new();
        ciborium::ser::into_writer(&value, &mut buf).context("CBOR-encode mitos.toml")?;
        let config_out = out.join("config.cbor");
        std::fs::write(&config_out, &buf)
            .with_context(|| format!("writing {}", config_out.display()))?;
        tracing::info!(
            path = %mitos_toml.display(),
            cbor_bytes = buf.len(),
            "config.cbor written"
        );
    } else {
        tracing::info!(
            "no mitos.toml at {} — module will get empty config",
            mitos_toml.display()
        );
    }

    println!("Module:        {module_id}");
    println!("SHA-256:       {}", manifest.module.sha256);
    println!("Size:          {} bytes", manifest.module.size_bytes);
    println!(
        "ABI version:   {}.{}",
        manifest.abi.version_major, manifest.abi.version_minor
    );
    println!(
        "Trap policy:   {} (max-retries={}, backoff-cap-ms={})",
        manifest.trap_policy.strategy,
        manifest.trap_policy.max_retries,
        manifest.trap_policy.backoff_cap_ms
    );
    println!("WIT world:     mitos:platform/mitos-module");
    println!("Artifact:      {}", out.display());

    Ok(())
}

fn cargo_build(args: &Args) -> anyhow::Result<PathBuf> {
    tracing::info!(crate_name = %args.crate_name, profile = %args.profile, "cargo build");
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--target")
        .arg("wasm32-wasip2")
        .arg("--profile")
        .arg(&args.profile)
        .arg("-p")
        .arg(&args.crate_name)
        .current_dir(&args.workspace);
    let status = cmd
        .status()
        .with_context(|| "running cargo (is it on PATH?)")?;
    if !status.success() {
        return Err(anyhow!("cargo build failed (exit {:?})", status.code()));
    }

    // Resolve the produced .wasm. Cargo writes to
    // `<workspace>/target/wasm32-wasip2/<profile>/<crate>.wasm`
    // with hyphens in the crate name converted to underscores.
    let crate_underscore = args.crate_name.replace('-', "_");
    let profile_dir = if args.profile == "dev" {
        "debug"
    } else {
        &args.profile
    };
    let path = args
        .workspace
        .join("target")
        .join("wasm32-wasip2")
        .join(profile_dir)
        .join(format!("{crate_underscore}.wasm"));
    if !path.exists() {
        return Err(anyhow!(
            "expected wasm at {} but it doesn't exist; \
             check `cargo build` output above",
            path.display()
        ));
    }
    Ok(path)
}

fn build_manifest(
    module_id: &str,
    wasm_bytes: &[u8],
    inspect: &InspectResult,
    args: &Args,
) -> anyhow::Result<Manifest> {
    let crate_version = read_crate_version(&args.workspace, &args.crate_name).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "could not read crate version; using 0.0.0");
        "0.0.0".to_owned()
    });
    let rust_version = rustc_version().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "could not get rustc version");
        "unknown".to_owned()
    });
    let git_sha = git_sha(&args.workspace).ok();
    let build_id = chrono::Utc::now().to_rfc3339();

    Ok(Manifest {
        module: ModuleSection {
            id: module_id.to_owned(),
            sha256: sha256_hex(wasm_bytes),
            size_bytes: wasm_bytes.len() as u64,
        },
        abi: AbiSection {
            version_major: inspect.abi_version_major,
            version_minor: inspect.abi_version_minor,
            wit_package: "mitos:platform".to_owned(),
            wit_world: "mitos-module".to_owned(),
        },
        trap_policy: TrapPolicySection {
            strategy: inspect.trap_strategy.clone(),
            max_retries: inspect.trap_max_retries,
            backoff_cap_ms: inspect.trap_backoff_cap_ms,
        },
        build: BuildSection {
            rust_version,
            target: "wasm32-wasip2".to_owned(),
            profile: args.profile.clone(),
            build_id,
            git_sha,
            crate_version,
        },
    })
}

fn log_inspect(inspect: &InspectResult) {
    tracing::info!(
        abi = format!(
            "{}.{}",
            inspect.abi_version_major, inspect.abi_version_minor
        ),
        trap_strategy = %inspect.trap_strategy,
        trap_max_retries = inspect.trap_max_retries,
        trap_backoff_cap_ms = inspect.trap_backoff_cap_ms,
        "inspected",
    );
}

fn read_crate_version(workspace: &Path, crate_name: &str) -> anyhow::Result<String> {
    // Best-effort scan: iterate workspace members for a Cargo.toml
    // whose [package].name matches. Avoids depending on cargo
    // metadata (heavier dep). For the v1 layout (`modules/<id>/<crate>/`)
    // the crate's Cargo.toml is one level below the workspace.
    //
    // Two version shapes to handle:
    //   - literal string: `version = "0.1.0"`
    //   - workspace inheritance: `version.workspace = true` —
    //     resolve by reading the workspace's root Cargo.toml's
    //     `[workspace.package].version`
    let crate_underscore = crate_name.replace('-', "_");
    let candidates = [
        workspace.join(crate_name).join("Cargo.toml"),
        workspace.join(&crate_underscore).join("Cargo.toml"),
        workspace.join("module").join("Cargo.toml"),
    ];
    for path in &candidates {
        if !path.exists() {
            continue;
        }
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => continue,
        };
        let parsed = match text.parse::<toml::Value>() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name = parsed
            .get("package")
            .and_then(|p| p.get("name"))
            .and_then(|n| n.as_str());
        if name != Some(crate_name) && name != Some(crate_underscore.as_str()) {
            continue;
        }
        let version = parsed.get("package").and_then(|p| p.get("version"));
        if let Some(v) = version.and_then(|v| v.as_str()) {
            return Ok(v.to_owned());
        }
        // version.workspace = true → resolve from workspace root.
        if version
            .and_then(|v| v.get("workspace"))
            .and_then(|w| w.as_bool())
            == Some(true)
        {
            return read_workspace_version(workspace);
        }
    }
    Err(anyhow!("no Cargo.toml found for crate `{crate_name}`"))
}

fn read_workspace_version(workspace: &Path) -> anyhow::Result<String> {
    let root = workspace.join("Cargo.toml");
    let text =
        std::fs::read_to_string(&root).with_context(|| format!("reading {}", root.display()))?;
    let parsed = text
        .parse::<toml::Value>()
        .context("parsing workspace toml")?;
    parsed
        .get("workspace")
        .and_then(|w| w.get("package"))
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_owned())
        .ok_or_else(|| anyhow!("no [workspace.package].version in {}", root.display()))
}

fn rustc_version() -> anyhow::Result<String> {
    let output = Command::new("rustc")
        .arg("--version")
        .output()
        .context("running rustc --version")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Format: `rustc 1.95.0 (sha 2026-mm-dd)`
    let version = stdout
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow!("unexpected rustc output: {stdout}"))?;
    Ok(version.to_owned())
}

fn git_sha(workspace: &Path) -> anyhow::Result<String> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--short=12")
        .arg("HEAD")
        .current_dir(workspace)
        .output()
        .context("running git rev-parse")?;
    if !output.status.success() {
        return Err(anyhow!("git rev-parse failed"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
