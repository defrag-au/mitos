//! CLI parse. Bare `mitos` means "run", with today's flags and env names.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::parser::ValueSource;
use clap::{Args as ClapArgs, Parser, Subcommand};
use mitos_config::{NetworkId, Profile};

#[derive(Debug, Parser)]
#[command(version, about = "mitos default bundle")]
pub struct Args {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Path to the Dolos config file (same schema as stock dolos.toml).
    /// When `/etc/mitos/mitos.toml` is absent this is the legacy path.
    /// `mitos bootstrap` imports into the data dir this file names.
    #[arg(long, env = "DOLOS_CONFIG", default_value = "dolos.toml")]
    pub config: String,

    /// HTTP listen address for indexer routes.
    #[arg(long, env = "BUNDLE_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: SocketAddr,

    /// Where mitos stores its own state. Independent of Dolos's
    /// data dir. Currently used by platform-v2 hosting paths (the
    /// emissions log + cursor checkpoint per wasm module live
    /// under `--modules-dir`, which is a separate path).
    #[arg(long, env = "BUNDLE_DATA_DIR", default_value = "./mitos-data")]
    pub data_dir: PathBuf,

    /// Wasm-module artifact storage. Setting this enables the
    /// `/_admin/modules/*` admin surface + auto-resume of any
    /// modules already registered under the path. Unset =
    /// classic statically-composed bundle (today's behaviour).
    /// See `docs/strategy/MITOS_PLATFORM_DEPLOYMENT.md`.
    #[arg(long, env = "BUNDLE_MODULES_DIR")]
    pub modules_dir: Option<PathBuf>,

    /// Community-modules source tree. When set + `--modules-dir`
    /// is enabled, the bundle activates any pre-built community
    /// module under `<dir>/<name>/build/` whose sha differs from
    /// the currently-active artifact in `--modules-dir`. Typical
    /// value: `<mitos-checkout>/community-modules`. See
    /// `docs/strategy/COMMUNITY_MODULES.md`.
    #[arg(long, env = "BUNDLE_COMMUNITY_MODULES_DIR")]
    pub community_modules_dir: Option<PathBuf>,

    /// Print the loaded configuration to stdout, then exit 0
    /// without starting the chain follower or HTTP server.
    /// Useful for verifying env vars and paths before committing
    /// to a long-running process.
    #[arg(long)]
    pub print_config_only: bool,

    /// `mitos.toml`. When set, the embedded-dolos config is derived
    /// from this file and a `dolos.toml` is never read. When unset,
    /// `/etc/mitos/mitos.toml` existing is what flips that switch;
    /// otherwise today's `DOLOS_CONFIG` path is used, byte for byte.
    #[arg(long, env = "MITOS_CONFIG")]
    pub config_file: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Create user, directories, token, default mitos.toml, systemd units.
    Init(InitArgs),
    /// Preflight + postflight. Every refusal names the number it wanted
    /// and the number it found.
    Doctor(DoctorArgs),
    /// Fetch (if needed) and import a Mithril snapshot into the embedded
    /// dolos data dir. Never deletes the chunk store.
    Bootstrap(BootstrapArgs),
}

#[derive(Debug, ClapArgs)]
pub struct InitArgs {
    #[arg(long, default_value = "mainnet", value_parser = parse_network)]
    pub network: NetworkId,
    #[arg(long, default_value = "full", value_parser = parse_profile)]
    pub profile: Profile,
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Prefix every filesystem path (tests). Nothing in the suite needs root.
    #[arg(long)]
    pub root: Option<PathBuf>,
}

#[derive(Debug, ClapArgs)]
pub struct BootstrapArgs {
    /// Directory that CONTAINS `immutable/`. Env `MITOS_CHUNK_STORE`.
    /// No default: guessing where 217 G goes is not a feature.
    #[arg(long, env = "MITOS_CHUNK_STORE")]
    pub chunk_store: Option<PathBuf>,
    /// `if-empty` (default) fetches only when the store has no chunks.
    /// `never` refuses if the store is empty.
    #[arg(long, default_value = "if-empty", value_parser = parse_fetch)]
    pub fetch: String,
    #[arg(long, default_value_t = 500)]
    pub chunk_size: usize,
}

fn parse_fetch(s: &str) -> Result<String, String> {
    match s {
        "if-empty" | "never" => Ok(s.to_owned()),
        other => Err(format!(
            "unknown --fetch `{other}` (want if-empty or never)"
        )),
    }
}

#[derive(Debug, ClapArgs)]
pub struct DoctorArgs {
    /// Poll until at tip, printing progress.
    #[arg(long)]
    pub wait: bool,
    /// Typed JSON on stdout instead of text.
    #[arg(long)]
    pub json: bool,
    #[arg(long, env = "MITOS_CONFIG")]
    pub config_file: Option<PathBuf>,
    #[arg(long)]
    pub root: Option<PathBuf>,
}

fn parse_network(s: &str) -> Result<NetworkId, String> {
    s.parse::<NetworkId>().map_err(|e| e.to_string())
}

fn parse_profile(s: &str) -> Result<Profile, String> {
    s.parse::<Profile>().map_err(|e| e.to_string())
}

/// CLI-set values only (not clap defaults, not env). Fed into mitos-config's
/// lookup ahead of `std::env` so `--listen` still wins in mitos.toml mode
/// without the clap default of `:8080` overriding the file.
pub fn command_line_overrides(matches: &clap::ArgMatches) -> Vec<(String, String)> {
    let mut out = Vec::new();
    push_if_cli(matches, "listen", &mut out);
    push_if_cli(matches, "data_dir", &mut out);
    push_if_cli(matches, "modules_dir", &mut out);
    push_if_cli(matches, "community_modules_dir", &mut out);
    out
}

fn push_if_cli(matches: &clap::ArgMatches, id: &str, out: &mut Vec<(String, String)>) {
    if matches.value_source(id) != Some(ValueSource::CommandLine) {
        return;
    }
    let env_name = match id {
        "listen" => mitos_config::env::BUNDLE_LISTEN,
        "data_dir" => mitos_config::env::BUNDLE_DATA_DIR,
        "modules_dir" => mitos_config::env::BUNDLE_MODULES_DIR,
        "community_modules_dir" => mitos_config::env::BUNDLE_COMMUNITY_MODULES_DIR,
        _ => return,
    };
    if let Some(v) = matches.get_one::<String>(id) {
        out.push((env_name.to_owned(), v.clone()));
        return;
    }
    if let Some(v) = matches.get_one::<SocketAddr>(id) {
        out.push((env_name.to_owned(), v.to_string()));
        return;
    }
    if let Some(v) = matches.get_one::<PathBuf>(id) {
        out.push((env_name.to_owned(), v.display().to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::{CommandFactory, Parser};

    fn arg<'a>(cmd: &'a clap::Command, id: &str) -> &'a clap::Arg {
        cmd.get_arguments()
            .find(|a| a.get_id() == id)
            .unwrap_or_else(|| panic!("missing arg {id}"))
    }

    fn defaults(cmd: &clap::Command, id: &str) -> Vec<String> {
        arg(cmd, id)
            .get_default_values()
            .iter()
            .map(|v| v.to_string_lossy().into_owned())
            .collect()
    }

    fn env_name(cmd: &clap::Command, id: &str) -> Option<String> {
        arg(cmd, id)
            .get_env()
            .map(|e| e.to_string_lossy().into_owned())
    }

    #[test]
    fn the_no_subcommand_invocation_parses_exactly_as_before() {
        let args = Args::try_parse_from(["mitos"]).expect("bare mitos must parse");
        assert!(
            args.command.is_none(),
            "bare mitos must mean run, not a subcommand"
        );
        assert!(!args.print_config_only);
        assert!(args.modules_dir.is_none());
        assert!(args.community_modules_dir.is_none());
        assert!(args.config_file.is_none());

        let cmd = Args::command();
        assert_eq!(defaults(&cmd, "config"), ["dolos.toml"]);
        assert_eq!(defaults(&cmd, "listen"), ["127.0.0.1:8080"]);
        assert_eq!(defaults(&cmd, "data_dir"), ["./mitos-data"]);
        assert_eq!(env_name(&cmd, "config").as_deref(), Some("DOLOS_CONFIG"));
        assert_eq!(env_name(&cmd, "listen").as_deref(), Some("BUNDLE_LISTEN"));
        assert_eq!(
            env_name(&cmd, "data_dir").as_deref(),
            Some("BUNDLE_DATA_DIR")
        );
        assert_eq!(
            env_name(&cmd, "modules_dir").as_deref(),
            Some("BUNDLE_MODULES_DIR")
        );
        assert_eq!(
            env_name(&cmd, "community_modules_dir").as_deref(),
            Some("BUNDLE_COMMUNITY_MODULES_DIR")
        );
    }

    #[test]
    fn bootstrap_is_an_optional_subcommand() {
        let args = Args::try_parse_from([
            "mitos",
            "bootstrap",
            "--chunk-store",
            "/opt/market-ledger/snapshot-full/db",
            "--fetch",
            "never",
        ])
        .unwrap();
        match args.command {
            Some(super::Command::Bootstrap(b)) => {
                assert_eq!(b.fetch, "never");
                assert_eq!(b.chunk_size, 500);
            }
            other => panic!("expected Bootstrap, got {other:?}"),
        }
    }

    #[test]
    fn init_is_an_optional_subcommand() {
        let args = Args::try_parse_from(["mitos", "init", "--network", "preprod"]).unwrap();
        match args.command {
            Some(super::Command::Init(init)) => {
                assert_eq!(init.network.to_string(), "preprod");
                assert_eq!(init.profile.to_string(), "full");
            }
            other => panic!("expected Init, got {other:?}"),
        }
    }
}
