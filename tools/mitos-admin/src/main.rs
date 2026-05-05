//! `mitos-admin`: thin CLI wrapper around mitos's HTTP admin
//! surface.
//!
//! Subscriptions:
//!
//! - `health` — pretty-print `/health` (uptime, indexer list,
//!   per-status subscription counts).
//! - `list` — `GET /_admin/subscriptions` formatted as a table or
//!   `--json` for piping.
//! - `add` — `POST /_admin/subscriptions` with friendly args.
//! - `remove <id>` — `DELETE /_admin/subscriptions/{id}`.
//!
//! Modules (the platform v1 deployment surface — see
//! `docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md`):
//!
//! - `list-modules` — `GET /_admin/modules`.
//! - `get-module <id>` — `GET /_admin/modules/{id}`.
//! - `upload-module --artifact <dir>` — multipart-upload a
//!   `mitos-build` artifact to `POST /_admin/modules/{id}`.
//! - `deploy --crate-name <name>` — chains `mitos-build` then
//!   `upload-module`. The wrangler-deploy ergonomic shape.
//!
//! Reads the bearer token from `MITOS_AUTH_TOKEN`. Mitos base URL
//! defaults to `http://127.0.0.1:8080` (the bundle's default
//! listen) — override with `--mitos`.

use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Parser, Debug)]
#[command(version, about = "manage mitos subscriptions and inspect health")]
struct Args {
    /// Mitos base URL.
    #[arg(long, env = "MITOS_URL", default_value = "http://127.0.0.1:8080")]
    mitos: String,

    /// Bearer token for `/_admin/*` endpoints. Required unless
    /// mitos is in open mode.
    #[arg(long, env = "MITOS_AUTH_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Print mitos's /health response (uptime, indexer list,
    /// subscription summary by status).
    Health,

    /// List all registered subscriptions.
    List {
        /// Emit raw JSON instead of the human-readable table.
        #[arg(long)]
        json: bool,
    },

    /// Register a new outbound subscription.
    Add {
        /// Indexer name (e.g. `collection-ownership`).
        #[arg(long)]
        indexer: String,

        /// Target WebSocket URL (e.g.
        /// `wss://collections-mitos.<account>.workers.dev/_internal/replicate?policy_id=abc`).
        #[arg(long)]
        target: String,

        /// Scope payload as JSON, e.g.
        /// `--scope-json '{"policy_id":"abc..."}'`. Shape must
        /// match the indexer's `Scope` type. Use `null` for
        /// indexers with `Scope = ()`.
        #[arg(long)]
        scope_json: String,

        /// Resume cursor: `origin`, `<slot>`, or `<slot>:<hash_hex>`.
        #[arg(long, default_value = "origin")]
        cursor: String,
    },

    /// Drop an existing subscription by id.
    Remove {
        /// Subscription id (from `list` or the response of `add`).
        id: u64,
    },

    /// List registered modules.
    ListModules {
        /// Emit raw JSON instead of the human-readable table.
        #[arg(long)]
        json: bool,
    },

    /// Show a single module's status + manifest summary.
    GetModule {
        /// Module id (e.g. `ownership`).
        id: String,
    },

    /// Upload a `mitos-build` artifact to mitos's admin endpoint.
    UploadModule {
        /// Path to the artifact directory `mitos-build` produced
        /// (contains `<id>.wasm` + `manifest.toml`).
        #[arg(long)]
        artifact: std::path::PathBuf,
    },

    /// Re-instantiate a running module without re-uploading.
    /// Useful for clearing a quarantined-flag or for forcing a
    /// follower to re-pickup a config change once that wires up.
    RestartModule {
        /// Module id (e.g. `ownership`).
        id: String,
    },

    /// Stop a running module's follower + drop the slot. Artifact
    /// stays on disk for rollback per
    /// `MITOS_PLATFORM_DEPLOYMENT.md` §"Resolved design questions"
    /// #1; remove with `rm -rf <storage>/<id>` if you really
    /// want it gone.
    DeleteModule {
        /// Module id.
        id: String,
    },

    /// Build then upload — wrangler-deploy ergonomics. Shells out
    /// to `mitos-build`, then POSTs the artifact via
    /// `upload-module`. Equivalent to running both steps by hand.
    Deploy {
        /// Crate name to build (the wasm-module crate). Same
        /// shape as `mitos-build --crate-name`.
        #[arg(long)]
        crate_name: String,

        /// Module id (defaults to `crate-name` with underscores
        /// → hyphens).
        #[arg(long)]
        module_id: Option<String>,

        /// Workspace root containing the wasm-module crate.
        #[arg(long, default_value = ".")]
        workspace: std::path::PathBuf,

        /// Override the path to the `mitos-build` binary.
        /// Defaults to expecting `mitos-build` on PATH.
        #[arg(long, default_value = "mitos-build")]
        mitos_build: std::path::PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .compact()
        .init();

    let Args { mitos, token, cmd } = Args::parse();
    let cli = Cli { mitos, token };
    // 60s timeout — uploads trigger wasmtime component validation
    // + dry-instantiate on the host, which can take 20s+ in
    // production. Read-only commands (list/get) finish in
    // milliseconds; the timeout only matters for upload + restart.
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;

    match cmd {
        Cmd::Health => cmd_health(&client, &cli).await,
        Cmd::List { json } => cmd_list(&client, &cli, json).await,
        Cmd::Add {
            indexer,
            target,
            scope_json,
            cursor,
        } => cmd_add(&client, &cli, indexer, target, scope_json, cursor).await,
        Cmd::Remove { id } => cmd_remove(&client, &cli, id).await,
        Cmd::ListModules { json } => cmd_list_modules(&client, &cli, json).await,
        Cmd::GetModule { id } => cmd_get_module(&client, &cli, id).await,
        Cmd::UploadModule { artifact } => cmd_upload_module(&client, &cli, artifact).await,
        Cmd::RestartModule { id } => cmd_restart_module(&client, &cli, id).await,
        Cmd::DeleteModule { id } => cmd_delete_module(&client, &cli, id).await,
        Cmd::Deploy {
            crate_name,
            module_id,
            workspace,
            mitos_build,
        } => cmd_deploy(&client, &cli, crate_name, module_id, workspace, mitos_build).await,
    }
}

struct Cli {
    mitos: String,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HealthResp {
    status: String,
    uptime_secs: u64,
    indexers: Vec<String>,
    replicator: ReplicatorSummary,
}

#[derive(Debug, Deserialize)]
struct ReplicatorSummary {
    total: usize,
    connecting: usize,
    connected: usize,
    disconnected: usize,
    backing_off: usize,
}

async fn cmd_health(client: &Client, cli: &Cli) -> anyhow::Result<()> {
    let url = format!("{}/health", cli.mitos);
    let resp: HealthResp = client
        .get(&url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    println!("status:        {}", resp.status);
    println!("uptime:        {}", format_duration(resp.uptime_secs));
    println!("indexers:      {}", resp.indexers.join(", "));
    println!(
        "subscriptions: total={} connected={} connecting={} backing_off={} disconnected={}",
        resp.replicator.total,
        resp.replicator.connected,
        resp.replicator.connecting,
        resp.replicator.backing_off,
        resp.replicator.disconnected,
    );
    Ok(())
}

#[derive(Debug, Deserialize, Serialize)]
struct SubEntry {
    id: u64,
    sub: SubBody,
    state: ConnStateView,
}

#[derive(Debug, Deserialize, Serialize)]
struct SubBody {
    indexer: String,
    target_url: String,
    cursor: Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct ConnStateView {
    status: String,
    last_connected_at: Option<u64>,
    last_error: Option<String>,
    backoff_secs: u64,
}

async fn cmd_list(client: &Client, cli: &Cli, json_out: bool) -> anyhow::Result<()> {
    let url = format!("{}/_admin/subscriptions", cli.mitos);
    let resp = auth(client.get(&url), cli.token.as_deref())
        .send()
        .await?
        .error_for_status()?;

    if json_out {
        let v: Value = resp.json().await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }

    let entries: Vec<SubEntry> = resp.json().await?;
    if entries.is_empty() {
        println!("(no subscriptions)");
        return Ok(());
    }

    println!(
        "{:<4}  {:<24}  {:<12}  {:<8}  TARGET",
        "ID", "INDEXER", "STATUS", "BACKOFF"
    );
    for e in entries {
        let backoff = if e.state.backoff_secs > 0 {
            format!("{}s", e.state.backoff_secs)
        } else {
            "-".into()
        };
        println!(
            "{:<4}  {:<24}  {:<12}  {:<8}  {}",
            e.id, e.sub.indexer, e.state.status, backoff, e.sub.target_url
        );
        if let Some(err) = e.state.last_error {
            println!("      └─ last_error: {err}");
        }
    }
    Ok(())
}

async fn cmd_add(
    client: &Client,
    cli: &Cli,
    indexer: String,
    target: String,
    scope_json: String,
    cursor: String,
) -> anyhow::Result<()> {
    let scope: Value = serde_json::from_str(&scope_json)
        .map_err(|e| anyhow::anyhow!("--scope-json is not valid JSON: {e}"))?;

    let body = json!({
        "indexer": indexer,
        "target_url": target,
        "scope": scope,
        "cursor": cursor,
    });

    let url = format!("{}/_admin/subscriptions", cli.mitos);
    let resp = auth(client.post(&url), cli.token.as_deref())
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("add failed: {status}: {text}");
    }

    #[derive(Deserialize)]
    struct AddResp {
        id: u64,
    }
    let added: AddResp = resp.json().await?;
    println!("added subscription id={}", added.id);
    Ok(())
}

async fn cmd_remove(client: &Client, cli: &Cli, id: u64) -> anyhow::Result<()> {
    let url = format!("{}/_admin/subscriptions/{id}", cli.mitos);
    let resp = auth(client.delete(&url), cli.token.as_deref())
        .send()
        .await?;
    let status = resp.status();
    if status.as_u16() == 204 {
        println!("removed subscription id={id}");
    } else if status.as_u16() == 404 {
        anyhow::bail!("no subscription with id={id}");
    } else {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("remove failed: {status}: {text}");
    }
    Ok(())
}

fn auth(req: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(t) => req.bearer_auth(t),
        None => req,
    }
}

// ---------------------------------------------------------------------
// Module subcommands (platform v1 deployment surface)
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ModuleSummary {
    id: String,
    sha256: String,
    size_bytes: u64,
    abi_version: String,
    trap_strategy: String,
    crate_version: String,
}

async fn cmd_list_modules(client: &Client, cli: &Cli, json_out: bool) -> anyhow::Result<()> {
    let url = format!("{}/_admin/modules", cli.mitos);
    let resp = auth(client.get(&url), cli.token.as_deref())
        .send()
        .await?
        .error_for_status()?;
    if json_out {
        let v: Value = resp.json().await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    let entries: Vec<ModuleSummary> = resp.json().await?;
    if entries.is_empty() {
        println!("(no modules registered)");
        return Ok(());
    }
    println!(
        "{:<24}  {:<12}  {:<8}  {:<12}  {:<8}  SHA256",
        "ID", "VERSION", "ABI", "TRAP", "SIZE"
    );
    for m in entries {
        println!(
            "{:<24}  {:<12}  {:<8}  {:<12}  {:<8}  {}",
            m.id,
            m.crate_version,
            m.abi_version,
            m.trap_strategy,
            format_bytes(m.size_bytes),
            short_sha(&m.sha256),
        );
    }
    Ok(())
}

async fn cmd_get_module(client: &Client, cli: &Cli, id: String) -> anyhow::Result<()> {
    let url = format!("{}/_admin/modules/{id}", cli.mitos);
    let resp = auth(client.get(&url), cli.token.as_deref()).send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        anyhow::bail!("module `{id}` not registered");
    }
    let resp = resp.error_for_status()?;
    let m: ModuleSummary = resp.json().await?;
    println!("id:            {}", m.id);
    println!("sha256:        {}", m.sha256);
    println!("size:          {} bytes", m.size_bytes);
    println!("abi_version:   {}", m.abi_version);
    println!("trap_strategy: {}", m.trap_strategy);
    println!("crate_version: {}", m.crate_version);
    Ok(())
}

async fn cmd_upload_module(
    client: &Client,
    cli: &Cli,
    artifact: std::path::PathBuf,
) -> anyhow::Result<()> {
    upload_artifact(client, cli, &artifact).await
}

async fn upload_artifact(
    client: &Client,
    cli: &Cli,
    artifact: &std::path::Path,
) -> anyhow::Result<()> {
    // Locate manifest + wasm in the artifact dir. The build tool
    // writes `manifest.toml` + `<id>.wasm` (id read from the
    // manifest itself).
    let manifest_path = artifact.join("manifest.toml");
    if !manifest_path.exists() {
        anyhow::bail!(
            "no manifest.toml in {} — is this a `mitos-build` artifact?",
            artifact.display()
        );
    }
    let manifest_str = std::fs::read_to_string(&manifest_path)?;
    let module_id = parse_module_id(&manifest_str)?;
    let wasm_path = artifact.join(format!("{module_id}.wasm"));
    if !wasm_path.exists() {
        anyhow::bail!(
            "no {} in {}; manifest says id=`{}`",
            wasm_path.file_name().unwrap().to_string_lossy(),
            artifact.display(),
            module_id
        );
    }
    let wasm_bytes = std::fs::read(&wasm_path)?;
    let config_path = artifact.join("config.cbor");
    let config_bytes = if config_path.exists() {
        Some(std::fs::read(&config_path)?)
    } else {
        None
    };
    tracing::info!(
        module = %module_id,
        manifest_bytes = manifest_str.len(),
        wasm_bytes = wasm_bytes.len(),
        config_bytes = config_bytes.as_ref().map(|c| c.len()),
        "uploading"
    );

    let mut form = reqwest::multipart::Form::new()
        .part(
            "manifest",
            reqwest::multipart::Part::text(manifest_str)
                .file_name("manifest.toml")
                .mime_str("text/toml")?,
        )
        .part(
            "wasm",
            reqwest::multipart::Part::bytes(wasm_bytes)
                .file_name(format!("{module_id}.wasm"))
                .mime_str("application/wasm")?,
        );
    if let Some(cfg) = config_bytes {
        form = form.part(
            "config",
            reqwest::multipart::Part::bytes(cfg)
                .file_name("config.cbor")
                .mime_str("application/cbor")?,
        );
    }

    let url = format!("{}/_admin/modules/{module_id}", cli.mitos);
    let resp = auth(client.post(&url), cli.token.as_deref())
        .multipart(form)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        // Host returns structured `{ "error": ..., "code": ... }`
        // for known failure modes; fall back to raw body for
        // anything else.
        let text = resp.text().await.unwrap_or_default();
        match serde_json::from_str::<Value>(&text) {
            Ok(v) if v.get("code").is_some() => {
                anyhow::bail!(
                    "upload failed: {status}: {} ({})",
                    v.get("error")
                        .and_then(|e| e.as_str())
                        .unwrap_or("(no error message)"),
                    v.get("code").and_then(|c| c.as_str()).unwrap_or("?")
                )
            }
            _ => anyhow::bail!("upload failed: {status}: {text}"),
        }
    }
    #[derive(Deserialize)]
    struct UploadResp {
        ok: bool,
        module: ModuleSummary,
    }
    let body: UploadResp = resp.json().await?;
    println!(
        "uploaded module={} sha={} ok={}",
        body.module.id,
        short_sha(&body.module.sha256),
        body.ok
    );
    Ok(())
}

async fn cmd_restart_module(client: &Client, cli: &Cli, id: String) -> anyhow::Result<()> {
    let url = format!("{}/_admin/modules/{id}/restart", cli.mitos);
    let resp = auth(client.post(&url), cli.token.as_deref()).send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        anyhow::bail!("module `{id}` not registered");
    }
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("restart failed: {status}: {text}");
    }
    println!("restarted module={id}");
    Ok(())
}

async fn cmd_delete_module(client: &Client, cli: &Cli, id: String) -> anyhow::Result<()> {
    let url = format!("{}/_admin/modules/{id}", cli.mitos);
    let resp = auth(client.delete(&url), cli.token.as_deref())
        .send()
        .await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        anyhow::bail!("module `{id}` not registered");
    }
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("delete failed: {status}: {text}");
    }
    println!("deleted module={id} (artifact preserved on disk)");
    Ok(())
}

async fn cmd_deploy(
    client: &Client,
    cli: &Cli,
    crate_name: String,
    module_id: Option<String>,
    workspace: std::path::PathBuf,
    mitos_build: std::path::PathBuf,
) -> anyhow::Result<()> {
    let resolved_id = module_id
        .clone()
        .unwrap_or_else(|| crate_name.replace('_', "-"));

    // Step 1: invoke mitos-build. Defaults emit artifact under
    // `<workspace>/target/mitos/<id>/`; we let mitos-build's
    // default win so the artifact lives somewhere predictable
    // for both the build and upload halves.
    println!(">>> mitos-build --crate-name {crate_name} --module-id {resolved_id}");
    let mut cmd = std::process::Command::new(&mitos_build);
    cmd.arg("--crate-name")
        .arg(&crate_name)
        .arg("--module-id")
        .arg(&resolved_id)
        .arg("--workspace")
        .arg(&workspace);
    let status = cmd
        .status()
        .map_err(|e| anyhow::anyhow!("running {}: {e}", mitos_build.display()))?;
    if !status.success() {
        anyhow::bail!("mitos-build failed (exit {:?})", status.code());
    }

    // Step 2: upload the produced artifact.
    let artifact = workspace.join("target").join("mitos").join(&resolved_id);
    println!(">>> upload-module --artifact {}", artifact.display());
    upload_artifact(client, cli, &artifact).await
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn parse_module_id(manifest_str: &str) -> anyhow::Result<String> {
    let parsed: Value = serde_json::from_str("{}").unwrap();
    let _ = parsed;
    // Use serde_json roundtrip via toml→json since toml-rs would
    // be another dep — hand-parse the `id = "..."` line under
    // [module] for now (the manifest is generated, format stable).
    let mut in_module = false;
    for line in manifest_str.lines() {
        let trimmed = line.trim();
        if trimmed == "[module]" {
            in_module = true;
            continue;
        }
        if trimmed.starts_with('[') {
            in_module = false;
            continue;
        }
        if in_module
            && let Some(rest) = trimmed.strip_prefix("id")
            && let Some(rest) = rest.trim_start().strip_prefix('=')
        {
            return Ok(rest.trim().trim_matches('"').to_owned());
        }
    }
    anyhow::bail!("manifest.toml has no [module].id field")
}

fn short_sha(sha: &str) -> String {
    if sha.len() > 12 {
        format!("{}…", &sha[..12])
    } else {
        sha.to_owned()
    }
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1}M", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1}K", bytes as f64 / 1024.0)
    } else {
        format!("{bytes}B")
    }
}

fn format_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}
