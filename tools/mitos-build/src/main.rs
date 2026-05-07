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
use clap::{Parser, Subcommand};
use mitos_platform::inspect::{InspectResult, dry_inspect};
use mitos_platform::manifest::{
    AbiSection, BuildSection, Manifest, ModuleSection, TrapPolicySection, sha256_hex,
};

/// Host WIT contracts bundled at compile time. Each `mitos-build`
/// release pins specific WIT versions; upgrading the tool
/// upgrades the WIT every single-file module is built against.
///
/// v1: legacy `mitos-module` world — block-centric `handle-event`
/// dispatch. Existing modules (`collection-ownership-indexer`,
/// `marketplace-indexer`, jpg-co's pre-migration shape) build
/// against this.
///
/// v2: `mitos-module-v2` world — eUTXO event-stream dispatch
/// per `MITOS_PLATFORM_V2.md`. Modules opt in by setting
/// `abi_version = 2` at the top of their `<name>.toml`.
const HOST_WIT_V1: &str = include_str!("../../../crates/mitos-platform/wit/world.wit");
const HOST_WIT_V2: &str = include_str!("../../../crates/mitos-platform/wit-v2/world.wit");

/// Absolute path to the `mitos-protocol` crate baked at compile
/// time. Single-file modules need a path-dep into mitos-protocol
/// (it carries `Interest`, `ChainPoint`, etc.); resolving from
/// `CARGO_MANIFEST_DIR` lets the materialised crate find it
/// without the user threading paths around. Tied to the build
/// tree mitos-build was compiled in — re-run cargo install after
/// moving the source.
const MITOS_PROTOCOL_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../crates/mitos-protocol",
);

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Build a mitos wasm-module crate into a deployable artifact",
    // Subcommands (e.g. `prepare`) carry their own args and don't
    // need the top-level `--crate-name` / `--module` requirement.
    subcommand_negates_reqs = true,
)]
struct Args {
    /// Single-file module shape: build from a `<name>.rs` +
    /// optional `<name>.toml`. mitos-build materialises a Cargo
    /// crate around the user's source under `target/mitos-build/<id>/`,
    /// injects the wit_bindgen header + bundled host WIT, and
    /// runs cargo. Mutually exclusive with `--crate-name` /
    /// `--workspace`.
    ///
    /// Argument can be either the `.rs` file directly or the
    /// stem (we'll find `<stem>.rs` next to it).
    #[arg(long, conflicts_with_all = ["crate_name", "workspace"])]
    module: Option<PathBuf>,

    /// Workspace member to build. Used as the `cargo build -p`
    /// argument. Required for the legacy multi-crate shape;
    /// omit when using `--module`.
    #[arg(long, required_unless_present = "module")]
    crate_name: Option<String>,

    /// Module ID written to the manifest. Defaults to the file
    /// stem (single-file mode) or the crate name with
    /// underscores replaced by hyphens (legacy mode). Must
    /// match `[a-z0-9-]+` (max 64 chars).
    #[arg(long)]
    module_id: Option<String>,

    /// Workspace root for the legacy multi-crate shape. Defaults
    /// to the current directory. Ignored when `--module` is set.
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

    #[command(subcommand)]
    cmd: Option<SubCmd>,
}

#[derive(Subcommand, Debug)]
enum SubCmd {
    /// Materialise the single-file module's Cargo crate without
    /// building. Useful for IDE / rust-analyzer pointing — the
    /// printed path can be opened in your editor for completion
    /// against the bindgen-generated traits.
    Prepare {
        /// `<name>.rs` (or stem) to materialise.
        #[arg(long)]
        module: PathBuf,
    },
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

    // Subcommand dispatch — `prepare` exits early without
    // running cargo.
    if let Some(SubCmd::Prepare { module }) = args.cmd.as_ref() {
        let single = SingleFileSpec::resolve(module)?;
        let materialised = materialise_module(&single)?;
        println!("{}", materialised.crate_dir.display());
        println!(
            "Open the path above in your IDE for rust-analyzer \
             completion against the bindgen-generated traits."
        );
        return Ok(());
    }

    // Single-file mode: materialise a Cargo crate around the
    // user's `.rs` + bundled WIT, then fall through into the
    // legacy build path with synthetic crate-name + workspace.
    let (build_workspace, build_crate_name, build_module_id, build_config_path) =
        if let Some(module) = args.module.as_ref() {
            let single = SingleFileSpec::resolve(module)?;
            let materialised = materialise_module(&single)?;
            let module_id = args.module_id.clone().unwrap_or(single.module_id.clone());
            (
                materialised.workspace_root,
                materialised.crate_name,
                module_id,
                single.config_path.clone(),
            )
        } else {
            let crate_name = args
                .crate_name
                .clone()
                .ok_or_else(|| anyhow!("--crate-name is required when --module is not given"))?;
            let module_id = args
                .module_id
                .clone()
                .unwrap_or_else(|| crate_name.replace('_', "-"));
            (args.workspace.clone(), crate_name, module_id, None)
        };
    let module_id = build_module_id;

    // 1. Build (or accept a pre-built path).
    let wasm_path = match args.wasm_path.as_ref() {
        Some(p) => p.clone(),
        None => cargo_build_at(&build_workspace, &build_crate_name, &args.profile)?,
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
    let manifest = build_manifest(
        &module_id,
        &wasm_bytes,
        &inspect,
        &build_workspace,
        &build_crate_name,
        &args.profile,
        build_config_path.as_deref(),
    )?;

    if args.dry_run {
        println!("--dry-run: not writing artifact");
        println!("{}", manifest.to_toml().context("serialise manifest")?);
        return Ok(());
    }

    // 4. Emit artifact directory.
    //
    // For single-file mode: the artifact lands next to the
    // user's `.rs` (under `<source-dir>/target/mitos/<id>/`)
    // rather than buried inside the materialised work dir.
    // Operators expect to find it next to the source.
    let out = args.out.clone().unwrap_or_else(|| {
        let source_dir = args
            .module
            .as_ref()
            .and_then(|m| {
                
                if m.is_file() { m.parent().map(|p| p.to_path_buf()) } else { Some(m.clone()) }
            })
            .unwrap_or_else(|| build_workspace.clone());
        source_dir.join("target").join("mitos").join(&module_id)
    });
    std::fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;
    let wasm_out = out.join(format!("{module_id}.wasm"));
    std::fs::copy(&wasm_path, &wasm_out)
        .with_context(|| format!("copying wasm to {}", wasm_out.display()))?;
    let manifest_out = out.join("manifest.toml");
    std::fs::write(&manifest_out, manifest.to_toml().context("manifest toml")?)
        .with_context(|| format!("writing {}", manifest_out.display()))?;

    // CBOR-encode the colocated mitos config as `config.cbor`
    // next to the wasm. The module's `init` deserialises this
    // back into its `Config` shape via ciborium. Wrangler-style:
    // human edits TOML, machine ships CBOR.
    //
    // Single-file mode: `<stem>.toml` next to the `.rs`.
    // Legacy mode: `<workspace>/mitos.toml`.
    let mitos_toml = if let Some(module) = args.module.as_ref() {
        let single = SingleFileSpec::resolve(module)?;
        single.config_path.unwrap_or_else(|| build_workspace.join("__nonexistent__.toml"))
    } else {
        args.workspace.join("mitos.toml")
    };
    if mitos_toml.exists() {
        let toml_str = std::fs::read_to_string(&mitos_toml)
            .with_context(|| format!("reading {}", mitos_toml.display()))?;
        let mut value: toml::Value = toml::from_str(&toml_str)
            .with_context(|| format!("parsing {}", mitos_toml.display()))?;
        // Strip the build-time-only `[deps]` table — those keys
        // are consumed by mitos-build (see `materialise_module`)
        // and have no business being shipped as runtime config
        // to the module's `init`. Without this, the module's
        // `Config` deserializer would see a bogus `deps` field.
        if let toml::Value::Table(ref mut t) = value {
            t.remove("deps");
        }
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

fn cargo_build_at(workspace: &Path, crate_name: &str, profile: &str) -> anyhow::Result<PathBuf> {
    tracing::info!(crate_name = %crate_name, profile = %profile, "cargo build");
    let mut cmd = Command::new("cargo");
    cmd.arg("build")
        .arg("--target")
        .arg("wasm32-wasip2")
        .arg("--profile")
        .arg(profile)
        .arg("-p")
        .arg(crate_name)
        .current_dir(workspace);
    let status = cmd
        .status()
        .with_context(|| "running cargo (is it on PATH?)")?;
    if !status.success() {
        return Err(anyhow!("cargo build failed (exit {:?})", status.code()));
    }

    // Resolve the produced .wasm. Cargo writes to
    // `<workspace>/target/wasm32-wasip2/<profile>/<crate>.wasm`
    // with hyphens in the crate name converted to underscores.
    let crate_underscore = crate_name.replace('-', "_");
    let profile_dir = if profile == "dev" { "debug" } else { profile };
    let path = workspace
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

// ============================================================================
// Single-file module support
// ============================================================================

/// Which platform ABI a module targets. Determines which WIT
/// gets bundled into the materialised crate and which world
/// the wit_bindgen macro generates against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbiVersion {
    /// `mitos-module` world — block-centric `handle-event`
    /// dispatch. Default when `<name>.toml` doesn't declare
    /// `abi_version`.
    V1,
    /// `mitos-module-v2` world — eUTXO event-stream dispatch
    /// per `MITOS_PLATFORM_V2.md`. Opt in by setting
    /// `abi_version = 2` at the top of the module's
    /// `<name>.toml`.
    V2,
}

/// Resolved single-file module: paths to `<name>.rs` and the
/// optional `<name>.toml`, plus the inferred module-id (file
/// stem, hyphenated).
struct SingleFileSpec {
    rs_path: PathBuf,
    config_path: Option<PathBuf>,
    module_id: String,
    abi_version: AbiVersion,
}

impl SingleFileSpec {
    fn resolve(arg: &Path) -> anyhow::Result<Self> {
        let rs_path = if arg.extension().and_then(|s| s.to_str()) == Some("rs") {
            arg.to_path_buf()
        } else {
            // Allow the user to pass `modules/ownership` and we'll
            // append `.rs`. Mirrors how `clippy --fix path/to/file`
            // accepts either form.
            let with_rs = arg.with_extension("rs");
            if !with_rs.exists() {
                return Err(anyhow!(
                    "single-file module: neither {} nor {} exists",
                    arg.display(),
                    with_rs.display()
                ));
            }
            with_rs
        };
        if !rs_path.exists() {
            return Err(anyhow!(
                "single-file module: {} does not exist",
                rs_path.display()
            ));
        }
        let stem = rs_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("invalid file name: {}", rs_path.display()))?;
        let module_id = stem.replace('_', "-");
        let config_candidate = rs_path.with_extension("toml");
        let config_path = config_candidate.exists().then_some(config_candidate);

        // Read `abi_version` from the top of `<name>.toml` if
        // present. Default v1 — existing modules keep working
        // without changes; v2 modules opt in by adding
        // `abi_version = 2`.
        let abi_version = match &config_path {
            None => AbiVersion::V1,
            Some(path) => {
                let toml_str = std::fs::read_to_string(path)
                    .with_context(|| format!("reading {}", path.display()))?;
                let value: toml::Value = toml::from_str(&toml_str)
                    .with_context(|| format!("parsing {}", path.display()))?;
                match value.get("abi_version").and_then(|v| v.as_integer()) {
                    Some(1) | None => AbiVersion::V1,
                    Some(2) => AbiVersion::V2,
                    Some(other) => {
                        return Err(anyhow!(
                            "{}: unsupported abi_version = {other} (only 1 or 2)",
                            path.display(),
                        ));
                    }
                }
            }
        };

        Ok(Self {
            rs_path: rs_path.canonicalize().unwrap_or(rs_path),
            config_path,
            module_id,
            abi_version,
        })
    }
}

/// Outcome of `materialise_module`: paths the build flow uses.
struct Materialised {
    /// Workspace root (cargo's `current_dir`). Contains the
    /// `[workspace] members = ["module"]` Cargo.toml.
    workspace_root: PathBuf,
    /// `module/` directory (the cdylib crate). Same as
    /// `workspace_root.join("module")`.
    crate_dir: PathBuf,
    /// The crate's `[package].name`. Used as `cargo build -p`.
    crate_name: String,
}

/// Materialise a Cargo crate around a single-file module under
/// `<rs_dir>/target/mitos-build/<id>/`. Idempotent — overwrites
/// generated files on every invocation; cargo's incremental
/// compilation cache stays warm because file mtimes only update
/// when content changes.
fn materialise_module(spec: &SingleFileSpec) -> anyhow::Result<Materialised> {
    let crate_name = format!("{}-module", spec.module_id);
    let rs_dir = spec
        .rs_path
        .parent()
        .ok_or_else(|| anyhow!("source has no parent: {}", spec.rs_path.display()))?
        .to_path_buf();
    let workspace_root = rs_dir
        .join("target")
        .join("mitos-build")
        .join(&spec.module_id);
    let crate_dir = workspace_root.join("module");
    let wit_dir = workspace_root.join("wit");
    let src_dir = crate_dir.join("src");
    std::fs::create_dir_all(&src_dir)
        .with_context(|| format!("creating {}", src_dir.display()))?;
    std::fs::create_dir_all(&wit_dir)
        .with_context(|| format!("creating {}", wit_dir.display()))?;

    // Workspace Cargo.toml. Includes a `[patch]` block redirecting
    // the public mitos-protocol git URL to the local path, so that
    // any transitive deps (e.g. user-declared crates that pull
    // mitos-protocol via workspace inheritance) all resolve to
    // the same physical crate. Without the patch, cargo errors
    // with "two different versions of crate `mitos_protocol`."
    let workspace_toml = format!(
        r#"# AUTO-GENERATED by mitos-build — DO NOT EDIT.
# Regenerated on every `mitos-build --module ...` run.
[workspace]
members = ["module"]
resolver = "2"

[workspace.package]
version = "0.0.0"
edition = "2024"
publish = false

[profile.release]
opt-level = "z"
lto = true
codegen-units = 1
# Keep symbols + line tables in release wasm so trap backtraces
# resolve to source locations. Platform v1 prioritises debuggability
# over wire size — modules run server-side on the operator's mitos
# host, so the ~30-50% size bump has no cost. Revisit if module
# count grows or cold-start matters.
strip = false
debug = "line-tables-only"

[patch."https://github.com/defrag-au/mitos"]
mitos-protocol = {{ path = "{mitos_protocol}" }}
"#,
        mitos_protocol = MITOS_PROTOCOL_PATH,
    );
    write_if_changed(&workspace_root.join("Cargo.toml"), &workspace_toml)?;

    // Crate Cargo.toml. Fixed dep set — `wit-bindgen`, `serde`,
    // `ciborium`, `hex`, `mitos-protocol`. mitos-protocol comes
    // from the absolute path baked at compile time so the user
    // doesn't have to thread workspace patches around. Optional
    // user-declared deps come from the `[deps]` table in
    // `<name>.toml` (paths resolved relative to that file).
    let user_deps_block = render_user_deps(spec)?;
    let crate_toml = format!(
        r#"# AUTO-GENERATED by mitos-build — DO NOT EDIT.
[package]
name = "{crate_name}"
version.workspace = true
edition.workspace = true
publish.workspace = true

[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.54"
serde = {{ version = "1", features = ["derive"] }}
ciborium = "0.2"
hex = "0.4"
mitos-protocol = {{ path = "{mitos_protocol}" }}
{user_deps_block}"#,
        crate_name = crate_name,
        mitos_protocol = MITOS_PROTOCOL_PATH,
        user_deps_block = user_deps_block,
    );
    write_if_changed(&crate_dir.join("Cargo.toml"), &crate_toml)?;

    // WIT contract bundled at compile time. v1 vs v2 is picked
    // off the spec — `<name>.toml`'s `abi_version` field, with
    // v1 as the default for existing modules.
    let (host_wit, world_name) = match spec.abi_version {
        AbiVersion::V1 => (HOST_WIT_V1, "mitos-module"),
        AbiVersion::V2 => (HOST_WIT_V2, "mitos-module-v2"),
    };
    write_if_changed(&wit_dir.join("world.wit"), host_wit)?;

    // Generate the crate's `src/lib.rs`. Layout:
    //
    //   <leading //! module docs from user source>
    //   <wit_bindgen::generate! macro — header injected here>
    //   <rest of user source>
    //
    // Inner doc comments (`//!`) must precede all items, so we
    // hoist them above the macro. The macro's bindgen-generated
    // items become available to the user code that follows.
    let user_src = std::fs::read_to_string(&spec.rs_path)
        .with_context(|| format!("reading {}", spec.rs_path.display()))?;
    let (user_inner_docs, user_rest) = split_inner_docs(&user_src);
    let lib_rs = format!(
        r#"// AUTO-GENERATED by mitos-build — DO NOT EDIT.
// Regenerated on every `mitos-build --module {rs_path}` run.
{user_inner_docs}
// Header injected by mitos-build: wit_bindgen macro pointing at
// the bundled host WIT under `../wit`. User source below.
wit_bindgen::generate!({{
    path: "../wit",
    world: "{world_name}",
}});

{user_rest}
"#,
        rs_path = spec.rs_path.display(),
        user_inner_docs = user_inner_docs,
        user_rest = user_rest,
    );
    write_if_changed(&src_dir.join("lib.rs"), &lib_rs)?;

    Ok(Materialised {
        workspace_root,
        crate_dir,
        crate_name,
    })
}

/// Render any `[deps]` entries from the user's `<name>.toml`
/// into Cargo's `[dependencies]` syntax. Path entries are
/// canonicalised against the toml file's directory so the
/// resolved path is correct regardless of where the
/// materialised crate lives. Empty string if no `<name>.toml`
/// or no `[deps]` table.
fn render_user_deps(spec: &SingleFileSpec) -> anyhow::Result<String> {
    let Some(config_path) = &spec.config_path else {
        return Ok(String::new());
    };
    let toml_str = std::fs::read_to_string(config_path)
        .with_context(|| format!("reading {}", config_path.display()))?;
    let value: toml::Value = toml::from_str(&toml_str)
        .with_context(|| format!("parsing {}", config_path.display()))?;
    let Some(deps) = value.get("deps").and_then(|v| v.as_table()) else {
        return Ok(String::new());
    };
    let toml_dir = config_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let mut out = String::new();
    for (name, dep) in deps {
        let rendered = match dep {
            toml::Value::String(version) => format!("{name} = \"{version}\""),
            toml::Value::Table(t) => {
                let mut t = t.clone();
                // Resolve path entries against the toml's dir +
                // canonicalise so the materialised Cargo.toml
                // doesn't depend on where it lives. Skip if the
                // path doesn't exist (cargo will surface the
                // error with a clearer message at build time).
                if let Some(toml::Value::String(path_str)) = t.get("path").cloned() {
                    let resolved = toml_dir.join(&path_str);
                    let canonical = resolved.canonicalize().unwrap_or(resolved);
                    t.insert(
                        "path".into(),
                        toml::Value::String(canonical.display().to_string()),
                    );
                }
                let inline = toml::to_string(&t)
                    .map_err(|e| anyhow!("serialise dep {name}: {e}"))?
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name} = {{ {inline} }}")
            }
            other => {
                return Err(anyhow!(
                    "deps.{name} must be a string or table, got {other:?}"
                ));
            }
        };
        out.push_str(&rendered);
        out.push('\n');
    }
    Ok(out)
}

/// Split user source into a leading inner-doc block (`//!`
/// lines and blank lines that come before any non-doc content)
/// and the rest. Hoisting the doc block above the injected
/// wit_bindgen macro keeps `//!` comments valid (inner
/// attributes must precede all items in a module).
fn split_inner_docs(src: &str) -> (String, String) {
    let mut docs = String::new();
    let mut rest_start = 0usize;
    for line in src.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("//!") || trimmed.is_empty() {
            docs.push_str(line);
            docs.push('\n');
            rest_start += line.len() + 1;
        } else {
            break;
        }
    }
    let rest = src.get(rest_start..).unwrap_or("").to_string();
    (docs, rest)
}

/// Write `content` to `path` only if it differs. Avoids
/// touching mtime on no-op runs so cargo's incremental cache
/// stays warm.
fn write_if_changed(path: &Path, content: &str) -> anyhow::Result<()> {
    if let Ok(existing) = std::fs::read_to_string(path)
        && existing == content {
            return Ok(());
        }
    std::fs::write(path, content).with_context(|| format!("writing {}", path.display()))
}

fn build_manifest(
    module_id: &str,
    wasm_bytes: &[u8],
    inspect: &InspectResult,
    workspace: &Path,
    crate_name: &str,
    profile: &str,
    // Path to the module's `<name>.toml` (single-file mode);
    // `None` for the legacy multi-crate mode where `[interest]`
    // has nowhere to live anyway.
    config_path: Option<&Path>,
) -> anyhow::Result<Manifest> {
    let crate_version = read_crate_version(workspace, crate_name).unwrap_or_else(|e| {
        tracing::warn!(error = %e, "could not read crate version; using 0.0.0");
        "0.0.0".to_owned()
    });
    let rust_version = rustc_version().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "could not get rustc version");
        "unknown".to_owned()
    });
    let git_sha = git_sha(workspace).ok();
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
            profile: profile.to_owned(),
            build_id,
            git_sha,
            crate_version,
        },
        // Interest section: read from `<name>.toml`'s
        // `[interest]` table when present. v2 modules declare
        // watched addresses here so the platform's bootstrap
        // orchestrator can hydrate state on first deploy. v1
        // modules omit this and the section round-trips empty.
        interest: read_interest_section(config_path)?,
    })
}

/// Read `[interest]` from the module's `<name>.toml` if
/// present. Today supports only `addresses = [...]`; richer
/// predicate vocabulary lands when the manifest schema does
/// (likely v2.1).
fn read_interest_section(
    config_path: Option<&Path>,
) -> anyhow::Result<mitos_platform::manifest::InterestSection> {
    let Some(path) = config_path else {
        return Ok(Default::default());
    };
    let toml_str = std::fs::read_to_string(path)
        .with_context(|| format!("reading {}", path.display()))?;
    let value: toml::Value = toml::from_str(&toml_str)
        .with_context(|| format!("parsing {}", path.display()))?;
    let Some(interest) = value.get("interest").and_then(|v| v.as_table()) else {
        return Ok(Default::default());
    };
    let addresses: Vec<String> = match interest.get("addresses") {
        Some(toml::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_owned()))
            .collect(),
        _ => Vec::new(),
    };
    Ok(mitos_platform::manifest::InterestSection { addresses })
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
