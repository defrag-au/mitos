//! `mitos-admin`: thin CLI wrapper around mitos's HTTP admin
//! surface.
//!
//! Subcommands:
//!
//! - `health` — pretty-print `/health` (uptime, indexer list,
//!   per-status subscription counts).
//! - `list` — `GET /_admin/subscriptions` formatted as a table or
//!   `--json` for piping.
//! - `add` — `POST /_admin/subscriptions` with friendly args.
//! - `remove <id>` — `DELETE /_admin/subscriptions/{id}`.
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
        /// `wss://collection-ownership-mitos.<account>.workers.dev/_internal/replicate?policy_id=abc`).
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
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
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
    let resp: HealthResp = client.get(&url).send().await?.error_for_status()?.json().await?;
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
    let resp = auth(client.delete(&url), cli.token.as_deref()).send().await?;
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
