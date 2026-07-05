//! `mitos-admin`: thin CLI wrapper around mitos's HTTP admin
//! surface.
//!
//! - `health` — pretty-print `/health` (uptime, indexer list).
//!
//! The legacy `list` / `add` / `remove` subcommands (which drove
//! the outbound `Replicator` subscription model + its
//! `/_admin/subscriptions` admin routes) retired alongside the
//! three legacy in-tree indexers (collection-ownership,
//! marketplace, mint-burn). Platform-v2 wasm modules use the
//! companion runtime's HTTPS subscribe path
//! (`/api/companions/subscribe`); manage them via the
//! `list-modules` / `upload-module` / `evict-module` /
//! `recapture` subcommands below.
//!
//! Modules (the platform-v2 deployment surface — see
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
use serde_json::Value;

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

    /// Print mitos's authed /_admin/status: version + build SHA,
    /// uptime, chain tip, archive horizon, and a per-module
    /// companion + last-trap summary. The first call to reach for
    /// when diagnosing host health (replaces SSH `systemctl` +
    /// `journalctl` for liveness).
    Status {
        /// Emit raw JSON instead of the human-readable summary.
        #[arg(long)]
        json: bool,
    },

    /// Tail the host's recent operational events (recaptures,
    /// traps) from `/_admin/events` — the structured replacement for
    /// `journalctl | grep`. `--follow` polls for new events.
    Tail {
        /// Only events for this module.
        #[arg(long)]
        module: Option<String>,
        /// Only this event kind (e.g. `trap`, `recapture_completed`).
        #[arg(long)]
        kind: Option<String>,
        /// Poll continuously (every 2s) for new events.
        #[arg(long)]
        follow: bool,
        /// Emit raw JSON, one object per event.
        #[arg(long)]
        json: bool,
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

    /// Coordinate a state-rebuild for every subscribed companion
    /// of a module. The protocol is in
    /// `docs/design/RECAPTURE.md`: mitos sends `Recapture` over
    /// each companion's dial-back WS, awaits `RecaptureReady`,
    /// wipes the module's bootstrap-done flags, restarts the
    /// follower (re-walks unspent UTxOs at the watched
    /// addresses), and sends `RecaptureDone`. Use after a
    /// schema migration, or when the dApp side has drifted.
    ///
    /// v1 always targets all subscribers (companion=*). Per-
    /// companion targeting is a planned follow-up.
    Recapture {
        /// Module id (e.g. `jpg-co`).
        id: String,
        /// Free-form operator label surfaced in the companion's
        /// `on_recapture` log line and the admin response. Useful
        /// for tying ops events back to the trigger.
        #[arg(long)]
        reason: Option<String>,
    },

    /// Stop a running module's follower + drop the slot. Artifact
    /// stays on disk for rollback per
    /// `MITOS_PLATFORM_DEPLOYMENT.md` §"Resolved design questions"
    /// #1; remove with `rm -rf <storage>/<id>` if you really
    /// want it gone — or use `evict-module` for a full retirement
    /// in one operation.
    DeleteModule {
        /// Module id.
        id: String,
    },

    /// Fully retire a module: stop the slot, drop the dialer's
    /// in-memory state for any subscribed companions, and remove
    /// the artifact directory. Refuses if companion records still
    /// exist on disk unless `--force` is passed (which logs a
    /// loud warning on the host).
    ///
    /// Use after retiring all consumers of a module. For the
    /// rollback-friendly "stop slot only" variant, use
    /// `delete-module` instead.
    EvictModule {
        /// Module id.
        id: String,
        /// Skip the companions-still-registered safety check.
        /// Reaps any in-memory dial loops + removes the artifact
        /// dir regardless of on-disk companion records. Logged
        /// loudly on the host. Use when the consumer side has
        /// already retired but cleanup didn't reach the host.
        #[arg(long)]
        force: bool,
    },

    /// Surgically remove a single companion record from a module's
    /// store. Cancels the in-memory dial task for that exact
    /// `(client_id, companion_key)` pair and deletes the on-disk
    /// `.cbor` file. Other consumers of the same module — same
    /// `companion_key` with a different `client_id`, or a
    /// different key entirely — stay registered. See
    /// `docs/design/MULTI_CLIENT_COMPANIONS.md`.
    DeleteCompanion {
        /// Module id.
        #[arg(long)]
        module: String,
        /// Client instance identifier (e.g. the dial-back URL
        /// host portion: `hooks.epochify.space`).
        #[arg(long = "client-id")]
        client_id: String,
        /// Companion key (the dApp-chosen identity passed in
        /// `SubscribeRequest.companion_key`).
        #[arg(long)]
        key: String,
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

    /// List rows from a module's emissions log. Default filter
    /// is `queued,pending` — the actionable backlog. Pass
    /// `--status all` to include `acked,nacked,timeout`.
    Emissions {
        /// Module id (e.g. `ownership`).
        #[arg(long)]
        module: String,

        /// Comma-separated status filter:
        /// `queued,pending,acked,nacked,timeout`. `all` skips
        /// the filter.
        #[arg(long, default_value = "queued,pending")]
        status: String,

        /// Filter to a specific companion (DO name).
        #[arg(long)]
        companion: Option<String>,

        /// Cap response size. Default 50.
        #[arg(long, default_value_t = 50)]
        limit: usize,

        /// Cursor pagination — skip rows with id ≤ this.
        #[arg(long)]
        after_id: Option<u64>,

        /// Emit raw JSON instead of the table.
        #[arg(long)]
        json: bool,
    },

    /// Replay a single emission row by id. Flips its status
    /// back to `Queued` so the dialer redelivers on the next
    /// poll cycle. Useful for retrying a `Nacked` or
    /// `Timeout` row after fixing the cause.
    EmissionsReplay {
        #[arg(long)]
        module: String,
        /// Row id (from `emissions list`).
        emission_id: u64,
    },

    /// Purge emissions matching a status filter. Refuses to
    /// run without an explicit `--status` (no blast-radius
    /// purges). Common usage: `--status acked` for compaction.
    EmissionsPurge {
        #[arg(long)]
        module: String,

        /// REQUIRED. Comma-separated status filter. `all` is
        /// rejected — use specific statuses.
        #[arg(long)]
        status: String,

        /// Optional companion filter.
        #[arg(long)]
        companion: Option<String>,
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
        Cmd::Status { json } => cmd_status(&client, &cli, json).await,
        Cmd::Tail {
            module,
            kind,
            follow,
            json,
        } => cmd_tail(&client, &cli, module, kind, follow, json).await,
        Cmd::ListModules { json } => cmd_list_modules(&client, &cli, json).await,
        Cmd::GetModule { id } => cmd_get_module(&client, &cli, id).await,
        Cmd::UploadModule { artifact } => cmd_upload_module(&client, &cli, artifact).await,
        Cmd::RestartModule { id } => cmd_restart_module(&client, &cli, id).await,
        Cmd::Recapture { id, reason } => cmd_recapture(&client, &cli, id, reason).await,
        Cmd::DeleteModule { id } => cmd_delete_module(&client, &cli, id).await,
        Cmd::EvictModule { id, force } => cmd_evict_module(&client, &cli, id, force).await,
        Cmd::DeleteCompanion {
            module,
            client_id,
            key,
        } => cmd_delete_companion(&client, &cli, module, client_id, key).await,
        Cmd::Deploy {
            crate_name,
            module_id,
            workspace,
            mitos_build,
        } => cmd_deploy(&client, &cli, crate_name, module_id, workspace, mitos_build).await,
        Cmd::Emissions {
            module,
            status,
            companion,
            limit,
            after_id,
            json,
        } => {
            cmd_emissions_list(
                &client, &cli, module, status, companion, limit, after_id, json,
            )
            .await
        }
        Cmd::EmissionsReplay {
            module,
            emission_id,
        } => cmd_emissions_replay(&client, &cli, module, emission_id).await,
        Cmd::EmissionsPurge {
            module,
            status,
            companion,
        } => cmd_emissions_purge(&client, &cli, module, status, companion).await,
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
    Ok(())
}

fn auth(req: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(t) => req.bearer_auth(t),
        None => req,
    }
}

#[derive(Debug, Deserialize)]
struct StatusResp {
    version: String,
    build_sha: String,
    uptime_secs: u64,
    tip: Option<StatusTip>,
    archive_horizon_slot: Option<u64>,
    modules: Vec<StatusModule>,
}

#[derive(Debug, Deserialize)]
struct StatusTip {
    slot: u64,
    hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatusModule {
    id: String,
    companions: usize,
    queued: usize,
    pending: usize,
    /// Defaults to `true` when talking to a pre-`running`-field host
    /// so an older server doesn't render every module as DOWN.
    #[serde(default = "default_true")]
    running: bool,
    #[serde(default)]
    recapture_in_progress: bool,
    #[serde(default)]
    bootstrap_in_progress: bool,
    #[serde(default)]
    last_result: Option<StatusLastResult>,
    last_trap_secs_ago: Option<u64>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct StatusLastResult {
    kind: String,
    utxos_ingested: u64,
    duration_ms: u64,
    outcome: String,
    secs_ago: u64,
}

async fn cmd_status(client: &Client, cli: &Cli, json_out: bool) -> anyhow::Result<()> {
    let url = format!("{}/_admin/status", cli.mitos);
    let resp = auth(client.get(&url), cli.token.as_deref())
        .send()
        .await?
        .error_for_status()?;
    if json_out {
        let v: Value = resp.json().await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    let s: StatusResp = resp.json().await?;
    println!("version:        {}", s.version);
    println!("build:          {}", s.build_sha);
    println!("uptime:         {}", format_duration(s.uptime_secs));
    match &s.tip {
        Some(t) => println!(
            "tip:            slot {} {}",
            t.slot,
            t.hash.as_deref().unwrap_or("(no hash)")
        ),
        None => println!("tip:            (unavailable)"),
    }
    match s.archive_horizon_slot {
        Some(slot) => println!("archive horizon: slot {slot}"),
        None => println!("archive horizon: (not reported)"),
    }
    println!("modules:        {}", s.modules.len());
    for m in &s.modules {
        let mut notes = Vec::new();
        // Derived display phase — precedence: an in-flight
        // recapture/bootstrap explains a not-running follower, so
        // only flag DOWN when nothing is working on reviving it.
        // (The API keeps the three independent flags; this word is
        // presentation only.)
        if !m.running && !m.recapture_in_progress && !m.bootstrap_in_progress && m.companions > 0 {
            notes.push("DOWN".to_string());
        }
        if m.queued + m.pending > 0 {
            notes.push(format!("BACKLOG {}q/{}p", m.queued, m.pending));
        }
        if m.recapture_in_progress {
            notes.push("RECAPTURING".to_string());
        }
        if m.bootstrap_in_progress {
            notes.push("BOOTSTRAPPING".to_string());
        }
        if let Some(secs) = m.last_trap_secs_ago {
            notes.push(format!("last trap {} ago", format_duration(secs)));
        }
        let suffix = if notes.is_empty() {
            String::new()
        } else {
            format!("  [{}]", notes.join(", "))
        };
        println!("  {:<28}  {} companion(s){suffix}", m.id, m.companions);
        if let Some(lr) = &m.last_result {
            println!(
                "      last {}: {} ({} utxos, {}ms) {} ago",
                lr.kind,
                lr.outcome,
                lr.utxos_ingested,
                lr.duration_ms,
                format_duration(lr.secs_ago),
            );
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct EventsResp {
    events: Vec<Value>,
    latest_seq: u64,
}

async fn cmd_tail(
    client: &Client,
    cli: &Cli,
    module: Option<String>,
    kind: Option<String>,
    follow: bool,
    json_out: bool,
) -> anyhow::Result<()> {
    let mut after: u64 = 0;
    let mut first = true;
    loop {
        let mut url = format!("{}/_admin/events?after={after}&limit=1000", cli.mitos);
        if let Some(m) = module.as_deref().filter(|s| !s.is_empty()) {
            url.push_str(&format!("&module={m}"));
        }
        if let Some(k) = kind.as_deref().filter(|s| !s.is_empty()) {
            url.push_str(&format!("&kind={k}"));
        }
        let resp: EventsResp = auth(client.get(&url), cli.token.as_deref())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if first && resp.events.is_empty() && !follow {
            println!("(no events recorded)");
        }
        for ev in &resp.events {
            if json_out {
                println!("{}", serde_json::to_string(ev)?);
            } else {
                print_event(ev);
            }
        }
        after = resp.latest_seq;
        first = false;
        if !follow {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    Ok(())
}

/// Human-readable one-liner for an event JSON object:
/// `[seq]  <age> ago  <kind>  <module>  <variant fields>`.
fn print_event(ev: &Value) {
    let seq = ev.get("seq").and_then(Value::as_u64).unwrap_or(0);
    let module = ev.get("module").and_then(Value::as_str).unwrap_or("?");
    let kind = ev.get("kind").and_then(Value::as_str).unwrap_or("?");
    let ts = ev.get("ts_unix").and_then(Value::as_u64).unwrap_or(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let age = format_duration(now.saturating_sub(ts));
    let extras: Vec<String> = ev
        .as_object()
        .map(|m| {
            m.iter()
                .filter(|(k, _)| !matches!(k.as_str(), "seq" | "ts_unix" | "module" | "kind"))
                .map(|(k, v)| format!("{k}={}", compact_value(v)))
                .collect()
        })
        .unwrap_or_default();
    println!(
        "[{seq:>4}] {age:>9} ago  {kind:<22} {module}  {}",
        extras.join(" ")
    );
}

/// Render a JSON value compactly for event output — bare strings
/// (no quotes), `null`, else the JSON form.
fn compact_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
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

#[derive(Debug, Deserialize)]
struct CompanionsResp {
    #[serde(default)]
    recapture_in_progress: bool,
    companions: Vec<CompanionDetailResp>,
}

#[derive(Debug, Deserialize)]
struct CompanionDetailResp {
    client_id: String,
    companion_key: String,
    #[serde(default)]
    watched_policies: Vec<String>,
    #[serde(default)]
    unbounded_interest: bool,
    resume_slot: Option<u64>,
    queued: usize,
    pending: usize,
    acked: usize,
    nacked: usize,
    timeout: usize,
    last_drain_secs_ago: Option<u64>,
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

    // Companion detail — interest, resume cursor, per-status emission
    // counts, last-drain age. The per-companion stall view.
    let curl = format!("{}/_admin/modules/{id}/companions", cli.mitos);
    let c: CompanionsResp = auth(client.get(&curl), cli.token.as_deref())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let recap = if c.recapture_in_progress {
        "  [RECAPTURE IN PROGRESS]"
    } else {
        ""
    };
    println!("companions:    {}{recap}", c.companions.len());
    for comp in &c.companions {
        let interest = if comp.unbounded_interest {
            "all policies".to_string()
        } else {
            format!("{} policies", comp.watched_policies.len())
        };
        let cursor = comp
            .resume_slot
            .map(|s| format!("slot {s}"))
            .unwrap_or_else(|| "fresh".to_string());
        let drain = match comp.last_drain_secs_ago {
            Some(secs) => format!("drained {} ago", format_duration(secs)),
            None => "never drained".to_string(),
        };
        println!(
            "  {}/{}  [{interest}]  cursor {cursor}  q={} p={} a={} n={} t={}  {drain}",
            comp.companion_key,
            comp.client_id,
            comp.queued,
            comp.pending,
            comp.acked,
            comp.nacked,
            comp.timeout,
        );
    }
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

/// Request body for `POST /_admin/modules/{id}/recapture`.
/// Mirrors the `RecaptureRequest` shape in
/// `mitos-platform::admin`; we declare it locally rather than
/// pulling the platform crate into the CLI dep graph just to
/// reuse two field names.
#[derive(Debug, Serialize)]
struct RecaptureBody {
    /// v1 always sends `"*"`. The server 400s anything else.
    companion: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

/// Response shape from a successful recapture. Surfaced in
/// the typed CLI output for pretty-printing.
#[derive(Debug, Deserialize)]
struct RecaptureResp {
    module: String,
    companions_targeted: usize,
    events_emitted: u64,
    duration_ms: u64,
}

/// Error body the admin endpoint returns on non-2xx. Shape
/// matches `AdminError` on the server side.
#[derive(Debug, Deserialize)]
struct AdminError {
    error: String,
    code: String,
}

async fn cmd_recapture(
    client: &Client,
    cli: &Cli,
    id: String,
    reason: Option<String>,
) -> anyhow::Result<()> {
    let url = format!("{}/_admin/modules/{id}/recapture", cli.mitos);
    let body = RecaptureBody {
        companion: "*",
        reason: reason.clone(),
    };
    let resp = auth(client.post(&url).json(&body), cli.token.as_deref())
        .send()
        .await?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();

    if status.is_success() {
        let parsed: RecaptureResp = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("decode response: {e}; body={text}"))?;
        println!("recapture complete");
        println!("  module:              {}", parsed.module);
        println!("  companions targeted: {}", parsed.companions_targeted);
        println!("  events emitted:      {}", parsed.events_emitted);
        println!("  duration (ms):       {}", parsed.duration_ms);
        if let Some(r) = reason {
            println!("  reason:              {r}");
        }
        return Ok(());
    }

    // Try to decode the structured AdminError body so the
    // operator gets the platform's code + message. Fall back to
    // raw status + text if decoding fails.
    let detail = serde_json::from_str::<AdminError>(&text)
        .map(|e| format!("{}: {}", e.code, e.error))
        .unwrap_or_else(|_| text.clone());
    anyhow::bail!("recapture failed ({status}): {detail}");
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

/// JSON-deserialisation shape for `POST .../evict` success.
/// Mirror of `mitos_platform::admin::EvictResponse`.
#[derive(serde::Deserialize)]
struct EvictResponse {
    module: String,
    cancelled_companions: Vec<String>,
    artifact_removed: bool,
}

/// Conflict body when companions still registered + no `--force`.
/// Mirror of `mitos_platform::admin::EvictConflictBody`.
#[derive(serde::Deserialize)]
struct EvictConflictBody {
    error: String,
    companion_keys: Vec<String>,
    hint: String,
}

async fn cmd_evict_module(
    client: &Client,
    cli: &Cli,
    id: String,
    force: bool,
) -> anyhow::Result<()> {
    let suffix = if force { "?force=true" } else { "" };
    let url = format!("{}/_admin/modules/{id}/evict{suffix}", cli.mitos);
    let resp = auth(client.post(&url), cli.token.as_deref()).send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        anyhow::bail!("module `{id}` not registered");
    }
    if status.as_u16() == 409 {
        let body: EvictConflictBody = resp.json().await?;
        anyhow::bail!(
            "evict refused for `{id}`: {} ({})\n  registered companion_keys: {}\n  hint: {}",
            body.error,
            "pass --force to override",
            body.companion_keys.join(", "),
            body.hint,
        );
    }
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("evict failed: {status}: {text}");
    }
    let body: EvictResponse = resp.json().await?;
    println!(
        "evicted module={} artifact_removed={} cancelled_companions={}",
        body.module,
        body.artifact_removed,
        body.cancelled_companions.len(),
    );
    if !body.cancelled_companions.is_empty() {
        for k in &body.cancelled_companions {
            println!("  - {k}");
        }
    }
    Ok(())
}

async fn cmd_delete_companion(
    client: &Client,
    cli: &Cli,
    module: String,
    client_id: String,
    key: String,
) -> anyhow::Result<()> {
    // URL-encode the path segments lazily — `client_id` charset is
    // restricted to `[a-zA-Z0-9._-]` server-side so this is mostly
    // defensive; `companion_key` is restricted to `[a-zA-Z0-9_-]`.
    let url = format!(
        "{}/_admin/modules/{module}/companions/{client_id}/{key}",
        cli.mitos
    );
    let resp = auth(client.delete(&url), cli.token.as_deref())
        .send()
        .await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        anyhow::bail!(
            "no such companion record (module={module}, client_id={client_id}, key={key})"
        );
    }
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("delete-companion failed: {status}: {text}");
    }
    println!("deleted companion module={module} client_id={client_id} companion_key={key}");
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
// Emissions subcommands
// ---------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct EmissionsListResp {
    rows: Vec<EmissionView>,
    total: usize,
    counts: std::collections::BTreeMap<String, usize>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // chain_point only surfaces in --json output
struct EmissionView {
    id: u64,
    matched_at: String,
    sent_at: Option<String>,
    chain_point: Value,
    channel: String,
    companion_id: String,
    status: String,
    status_at: String,
    error: Option<String>,
    payload_bytes: usize,
}

#[allow(clippy::too_many_arguments)] // CLI dispatch — fine for one shot
async fn cmd_emissions_list(
    client: &Client,
    cli: &Cli,
    module: String,
    status: String,
    companion: Option<String>,
    limit: usize,
    after_id: Option<u64>,
    json_out: bool,
) -> anyhow::Result<()> {
    let mut url = reqwest::Url::parse(&format!("{}/_admin/modules/{module}/emissions", cli.mitos))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("status", &status);
        q.append_pair("limit", &limit.to_string());
        if let Some(c) = &companion {
            q.append_pair("companion_key", c);
        }
        if let Some(a) = after_id {
            q.append_pair("after_id", &a.to_string());
        }
    }
    let resp = auth(client.get(url), cli.token.as_deref())
        .send()
        .await?
        .error_for_status()?;
    if json_out {
        let v: Value = resp.json().await?;
        println!("{}", serde_json::to_string_pretty(&v)?);
        return Ok(());
    }
    let body: EmissionsListResp = resp.json().await?;
    if body.rows.is_empty() {
        println!("(no emissions match filter; total={})", body.total);
    } else {
        println!(
            "{:<8}  {:<10}  {:<32}  {:<12}  {:<8}  MATCHED_AT",
            "ID", "STATUS", "COMPANION", "CHANNEL", "BYTES"
        );
        for r in &body.rows {
            let companion = if r.companion_id.len() > 32 {
                format!("{}…", &r.companion_id[..30])
            } else {
                r.companion_id.clone()
            };
            println!(
                "{:<8}  {:<10}  {:<32}  {:<12}  {:<8}  {}",
                r.id, r.status, companion, r.channel, r.payload_bytes, r.matched_at
            );
            if let Some(err) = &r.error {
                println!("           error: {err}");
            }
            if let Some(sent) = &r.sent_at {
                println!("           sent_at: {sent} → status_at: {}", r.status_at);
            }
        }
        println!();
        println!(
            "total: {} (showing {} via limit) — counts: {}",
            body.total,
            body.rows.len(),
            body.counts
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

async fn cmd_emissions_replay(
    client: &Client,
    cli: &Cli,
    module: String,
    emission_id: u64,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/_admin/modules/{module}/emissions/{emission_id}/replay",
        cli.mitos
    );
    let resp = auth(client.post(&url), cli.token.as_deref()).send().await?;
    let status = resp.status();
    if status.as_u16() == 404 {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("not found: {text}");
    }
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("replay failed: {status}: {text}");
    }
    let v: Value = resp.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
}

async fn cmd_emissions_purge(
    client: &Client,
    cli: &Cli,
    module: String,
    status: String,
    companion: Option<String>,
) -> anyhow::Result<()> {
    let mut url = reqwest::Url::parse(&format!("{}/_admin/modules/{module}/emissions", cli.mitos))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("status", &status);
        if let Some(c) = &companion {
            q.append_pair("companion_key", c);
        }
    }
    let resp = auth(client.delete(url), cli.token.as_deref())
        .send()
        .await?;
    let s = resp.status();
    if !s.is_success() {
        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!("purge failed: {s}: {text}");
    }
    let v: Value = resp.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);
    Ok(())
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
