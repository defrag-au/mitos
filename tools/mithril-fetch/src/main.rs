//! `mithril-fetch` — a policy id in, the Mithril chunks its history needs out.
//!
//! The chain source is a Mithril immutable-file range, not a token's slice: the
//! first mint comes from Koios, and the range runs from the chunk before it to
//! the newest signed chunk (or a chosen end). The store is shared, because
//! sealed chunks are identical for every policy.
//!
//! The TUI is the default. `--run` is the same core without a terminal, and
//! `--fixture` draws a captured screen through a `TestBackend`, which is how
//! `tui-shot` reviews a frame that would otherwise need the network.

mod app;
mod fetch;
mod fmt;
mod harness;
mod manifest;
mod plan;
mod render;

use std::io::{IsTerminal, Write, stdout};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use crossterm::event::{self, Event as TerminalEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::{mpsc, oneshot};

use app::{Action, App, Event, FetchState, PlanBound, Screen};
use fetch::Observer;
use manifest::PolicyManifest;
use plan::{PolicyId, RangePlan};

#[derive(Parser)]
#[command(
    name = "mithril-fetch",
    about = "Resolve a policy's first mint and fetch the Mithril immutable chunks its history needs",
    version
)]
struct Cli {
    /// Policy id (56 hex characters). Omit to start on the input screen.
    #[arg(long)]
    policy: Option<String>,

    /// Chunk store root. Defaults to $MITHRIL_DATA_DIR, loaded from .env.
    #[arg(long)]
    dest: Option<PathBuf>,

    /// Last slot to fetch; its immutable file is included.
    ///
    /// Defaults to the newest signed chunk, so a full mint → tip history.
    #[arg(long, conflicts_with = "chunks")]
    to_slot: Option<u64>,

    /// Fetch only the first N immutable files from the policy's floor.
    ///
    /// A probe: enough to check the store, the verification and the walk without
    /// committing to the whole range.
    #[arg(long)]
    chunks: Option<u64>,

    /// Resolve and fetch without the TUI, printing to stderr and stdout.
    #[arg(long)]
    run: bool,

    /// A screen fixture for `tui-shot` and the `TestBackend` harness.
    #[arg(long)]
    fixture: Option<PathBuf>,

    /// With --fixture: write <stem>.txt and <stem>.json instead of opening the TUI.
    #[arg(long)]
    capture: Option<PathBuf>,

    /// Frame size for --fixture, columns x rows.
    #[arg(long, default_value = "120x40")]
    size: String,
}

fn main() -> Result<()> {
    load_dotenv();
    let cli = Cli::parse();

    if let Some(path) = &cli.fixture {
        if cli.run {
            bail!("--fixture and --run are different modes");
        }
        let size = cli
            .size
            .parse::<harness::Size>()
            .map_err(anyhow::Error::msg)?;
        let screen = harness::load(path)?;

        if let Some(stem) = &cli.capture {
            harness::capture(&screen, size, stem)?;
            println!("wrote {}.txt and {}.json", stem.display(), stem.display());
            return Ok(());
        }

        require_terminal("--fixture without --capture")?;
        return run_tui(App::fixture(screen), None);
    }

    if cli.capture.is_some() {
        bail!("--capture needs --fixture");
    }
    if cli.chunks == Some(0) {
        bail!("--chunks 0 asks for nothing");
    }

    let destination = cli
        .dest
        .clone()
        .or_else(|| std::env::var_os("MITHRIL_DATA_DIR").map(PathBuf::from));

    if cli.run {
        let text = cli
            .policy
            .as_deref()
            .context("--run needs --policy (or omit --run for the TUI)")?;
        let policy = PolicyId::parse(text).map_err(anyhow::Error::msg)?;
        let destination = destination.context(
            "no chunk store — pass --dest or set MITHRIL_DATA_DIR (mitos/.env is read \
             automatically)",
        )?;
        return run_once(policy, destination, cli.to_slot, cli.chunks);
    }

    require_terminal("the TUI")?;

    let (screen, initial) = match cli.policy.as_deref() {
        Some(text) => match PolicyId::parse(text) {
            Ok(policy) if destination.is_some() => (
                Screen::Resolving {
                    policy,
                    status: "starting…".to_string(),
                },
                Some(policy),
            ),
            Ok(_) => (
                Screen::Policy {
                    input: text.to_string(),
                    error: Some(
                        "no chunk store — set MITHRIL_DATA_DIR in mitos/.env or pass --dest"
                            .to_string(),
                    ),
                },
                None,
            ),
            Err(message) => (
                Screen::Policy {
                    input: text.to_string(),
                    error: Some(message),
                },
                None,
            ),
        },
        None => (
            Screen::Policy {
                input: String::new(),
                error: None,
            },
            None,
        ),
    };

    let mut app = App::new(destination, screen);
    app.bound = PlanBound {
        to_slot: cli.to_slot,
        chunks: cli.chunks,
    };
    run_tui(app, initial)
}

fn load_dotenv() {
    match dotenvy::dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => eprintln!("mithril-fetch: could not read .env: {error}"),
    }
}

fn require_terminal(what: &str) -> Result<()> {
    if stdout().is_terminal() {
        Ok(())
    } else {
        bail!(
            "{what} needs a terminal; use `--run --policy <hex>` for a scripted fetch, or \
             `--fixture <file> --capture <stem>` to draw a frame"
        )
    }
}

fn run_tui(mut app: App, initial: Option<PolicyId>) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let observer: Observer = Arc::new(move |event| {
        if tx.send(event).is_err() {
            // The UI has exited. A worker's last event has nowhere to go, and
            // that is not a failure worth reporting.
        }
    });

    if let Some(policy) = initial {
        start_resolve(&mut app, &observer, policy);
    }

    let mut terminal = TerminalGuard::new()?;
    while !app.quit {
        while let Ok(event) = rx.try_recv() {
            app.apply(event);
        }

        terminal.draw(&app)?;

        let timeout = app
            .repaint_interval()
            .unwrap_or_else(|| Duration::from_millis(250));
        if event::poll(timeout)? {
            match event::read()? {
                TerminalEvent::Key(key) if key.kind == KeyEventKind::Press => {
                    let action = app.on_key(key.code, key.modifiers);
                    execute(action, &mut app, &observer);
                }
                TerminalEvent::Resize(_, _) => {}
                _ => {}
            }
        }

        app.tick = app.tick.wrapping_add(1);
    }

    // Leave no worker running behind the alternate screen.
    cancel(&mut app);
    Ok(())
}

fn start_resolve(app: &mut App, observer: &Observer, policy: PolicyId) {
    if app.destination.is_none() {
        app.screen = Screen::Failed {
            policy,
            message: "no chunk store — set MITHRIL_DATA_DIR in mitos/.env or pass --dest"
                .to_string(),
            retry: false,
        };
        return;
    }

    let generation = app.next_generation();
    app.screen = Screen::Resolving {
        policy,
        status: "starting…".to_string(),
    };

    let observer = observer.clone();
    std::thread::spawn(
        move || match fetch::resolve_blocking(&policy, generation, &observer) {
            Ok((floor, tip)) => (observer)(Event::Resolved {
                generation,
                floor,
                tip,
            }),
            Err(error) => (observer)(Event::ResolveFailed {
                generation,
                message: format!("{error:#}"),
            }),
        },
    );
}

fn start_fetch(app: &mut App, observer: &Observer, plan: RangePlan) {
    let generation = app.next_generation();
    let (cancel_tx, cancel_rx) = oneshot::channel();
    app.cancel = Some(cancel_tx);
    app.screen = Screen::Fetching {
        plan: plan.clone(),
        phase: "starting…".to_string(),
        fetch: FetchState::started(&plan),
    };

    let observer = observer.clone();
    std::thread::spawn(move || {
        match fetch::fetch_blocking(&plan, generation, observer.clone(), cancel_rx) {
            Ok(outcome) => (observer)(Event::Done {
                generation,
                outcome,
            }),
            Err(error) => (observer)(Event::FetchFailed {
                generation,
                message: format!("{error:#}"),
            }),
        }
    });
}

fn execute(action: Action, app: &mut App, observer: &Observer) {
    match action {
        Action::None => {}
        Action::Quit => {
            cancel(app);
            app.quit = true;
        }
        Action::Resolve(policy) => start_resolve(app, observer, policy),
        Action::Start(plan) => start_fetch(app, observer, plan),
        Action::Cancel => {
            let (policy, done) = match &app.screen {
                Screen::Fetching { plan, fetch, .. } => (plan.policy, fetch.files_done),
                _ => return,
            };
            cancel(app);
            app.generation += 1;
            app.screen = Screen::Failed {
                policy,
                message: format!(
                    "cancelled after {} verified file(s). Press r to resume; the store keeps \
                     everything that reached it.",
                    fmt::count(done)
                ),
                retry: true,
            };
        }
        Action::Back => {
            cancel(app);
            app.back();
        }
        Action::Retry(policy) => start_resolve(app, observer, policy),
    }
}

fn cancel(app: &mut App) {
    if let Some(cancel) = app.take_cancel()
        && cancel.send(()).is_err()
    {
        // The worker finished before the cancellation arrived. Its event is
        // stale because the generation has moved on.
    }
}

fn run_once(
    policy: PolicyId,
    destination: PathBuf,
    to_slot: Option<u64>,
    chunks: Option<u64>,
) -> Result<()> {
    let observer: Observer = Arc::new(|event| match event {
        Event::Status { message, .. } => eprintln!("{message}"),
        Event::RangeDone { range, .. } => eprintln!(
            "verified    {:05}–{:05} · {} files · {} · snapshot {}",
            range.from,
            range.to,
            fmt::count(range.files),
            fmt::bytes(range.bytes),
            short(&range.snapshot_hash)
        ),
        _ => {}
    });

    let (floor, tip) = fetch::resolve_blocking(&policy, 0, &observer)?;
    let plan = RangePlan::new(policy, floor, destination, tip, None)?.bounded(to_slot, chunks)?;
    print_plan(&plan);

    let (_cancel, cancel_rx) = oneshot::channel();
    let outcome = fetch::fetch_blocking(&plan, 0, observer, cancel_rx)?;

    println!();
    println!(
        "fetched     {} files · {}",
        fmt::count(outcome.files),
        fmt::bytes(outcome.bytes)
    );
    if outcome.already_present > 0 {
        println!(
            "present     {} files were already in the store",
            fmt::count(outcome.already_present)
        );
    }
    println!(
        "manifest    {}",
        PolicyManifest::path(&plan.destination, &plan.policy).display()
    );
    println!();
    println!("next: {}", plan::next_command(&plan));

    Ok(())
}

fn print_plan(plan: &RangePlan) {
    let floor = &plan.floor;
    println!("policy      {}", plan.policy);
    println!(
        "first mint  {} · {} · slot {}",
        floor.earliest_asset,
        fmt::utc(floor.earliest_unix),
        fmt::count(floor.slot)
    );
    if let Some(tx) = &floor.minting_tx {
        println!("minting tx  {tx}");
    }
    println!(
        "chunk       fetch from {:05} (slot falls in {:05}; one chunk of margin)",
        plan.from, floor.exact_chunk
    );
    println!(
        "newest      {:05} · epoch {} · {}",
        plan.latest_beacon, plan.latest_epoch, plan.snapshot_created
    );
    println!(
        "range       {:05}–{:05} · {} immutable files",
        plan.from,
        plan.to,
        fmt::count(plan.files())
    );
    println!(
        "estimate    ≈ {} ({} B/file, the snapshot's average)",
        fmt::bytes(plan.estimated_bytes()),
        fmt::count(plan.per_file_bytes)
    );
    println!("store       {}", plan.destination.display());
    println!();
    let _ = stdout().flush();
}

fn short(hash: &str) -> String {
    hash.chars().take(12).collect()
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode().context("entering raw mode")?;
        let mut out = stdout();
        execute!(out, EnterAlternateScreen).context("entering the alternate screen")?;
        let terminal =
            Terminal::new(CrosstermBackend::new(out)).context("creating the terminal")?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, app: &App) -> Result<()> {
        self.terminal
            .draw(|frame| render::draw(frame, &app.screen, app.tick))
            .context("drawing the frame")?;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Err(error) = disable_raw_mode() {
            eprintln!("mithril-fetch: could not leave raw mode: {error}");
        }
        if let Err(error) = execute!(self.terminal.backend_mut(), LeaveAlternateScreen) {
            eprintln!("mithril-fetch: could not leave the alternate screen: {error}");
        }
        if let Err(error) = self.terminal.show_cursor() {
            eprintln!("mithril-fetch: could not restore the cursor: {error}");
        }
    }
}
