//! A terminal firehose over the `chain-events` NDJSON stream.
//!
//! ```text
//! event-firehose --profile profiles/perps.toml            # run the walk, draw it
//!
//! event-firehose .tmp/perps.ndjson                        # a recorded stream
//! event-firehose .tmp/perps.ndjson --follow               # a file a producer is writing
//! event-firehose .tmp/perps.ndjson --snapshot .tmp/frame.txt
//! ```
//!
//! # `--profile` is the one-command wiring
//!
//! A profile names the snapshot and the collections (`tools/token-ledger`'s
//! `src/profile.rs`), and this tool runs `token-ledger walk --profile <file> --events`
//! with the child's stdout on a pipe. The child never touches the terminal, so the keys
//! stay here — which a bare `… | event-firehose` cannot promise: crossterm reads keys from
//! stdin when stdin is a tty and from `/dev/tty` otherwise, and an editor's agent panel,
//! a task runner or a `run-shell` has no controlling terminal. One process owns the
//! screen and the walk is its child, so leaving the viewer stops the walk.
//!
//! # A stream is still the interface
//!
//! The producer writes one [`ChainEvent`] per line and keeps its own report on stderr, so
//! the viewer needs no coupling to `token-ledger`. Under `--profile` the walk's report
//! goes to `.tmp/<profile>.walk.log` and its exit status is drawn in the footer.
//!
//! # What the screen is for
//!
//! The stream's shape, not a dashboard: what arrived, in what order, how a block's
//! activity is distributed, and which rows are worth a second look (a split, a batch, a
//! movement with no party). An event that cannot be parsed is counted and named in the
//! footer; it never stops the stream.

mod app;
mod render;

use std::io::{BufRead, BufReader, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chain_events::ChainEvent;
use clap::Parser;
use crossterm::event::{self, Event as TerminalEvent, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::{CrosstermBackend, TestBackend};
use ratatui::buffer::Buffer;

use app::App;

#[derive(Parser, Debug)]
#[command(
    name = "event-firehose",
    version,
    about = "Reads a chain-events NDJSON stream and draws it as a terminal firehose"
)]
struct Cli {
    /// NDJSON stream to read. Omit or pass `-` for stdin.
    #[arg(value_name = "STREAM")]
    stream: Option<String>,

    /// Run `token-ledger walk --profile <FILE>` and draw what it finds.
    ///
    /// One command, no pipe: the walk's stdout is the stream and this process keeps the
    /// terminal, so keys work where a pipeline would need `/dev/tty`. The profile names
    /// the snapshot, so the events come straight from the Mithril chunks.
    #[arg(long, value_name = "FILE", conflicts_with_all = ["stream", "follow", "snapshot"])]
    profile: Option<PathBuf>,

    /// The token-ledger to run for `--profile`. Defaults to the one beside this binary.
    #[arg(long, value_name = "PATH", requires = "profile")]
    token_ledger: Option<PathBuf>,

    /// For a file, keep reading as it grows (like `tail -f`).
    ///
    /// The fallback when something else owns the producer: write the stream to a file and
    /// tail it, so stdin stays the terminal and the run stays replayable.
    #[arg(long)]
    follow: bool,

    /// Render one frame to this file as plain text and exit. No terminal needed.
    #[arg(long, value_name = "FILE")]
    snapshot: Option<PathBuf>,

    /// Frame size for --snapshot, as COLSxROWS.
    #[arg(long, default_value = "120x36", value_name = "COLSxROWS")]
    size: String,

    /// Feed rows kept on screen.
    #[arg(long, default_value_t = 200)]
    feed: usize,
}

impl Cli {
    fn label(&self) -> String {
        if let Some(profile) = &self.profile {
            let name = profile
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_else(|| profile.display().to_string());
            return format!("profile {name}");
        }
        match self.stream.as_deref() {
            None | Some("-") => "stdin".to_owned(),
            Some(path) => path.to_owned(),
        }
    }

    /// Whether the stream arrives on stdin, which decides where keys have to come from.
    fn reads_stdin(&self) -> bool {
        self.profile.is_none() && self.stream.as_deref().is_none_or(|stream| stream == "-")
    }
}

/// One message from the reader thread.
enum Msg {
    Event(Box<ChainEvent>),
    Malformed(String),
    Closed,
}

/// How often a live screen is repainted even when nothing changed. The producer's logs go
/// to the same terminal, so a log line written while the viewer is on the alternate screen
/// sits over the frame until the next draw — a slow repaint heals it. A closed or paused
/// stream does not repaint, so `tui-shot` still sees a quiet screen.
const HEARTBEAT: Duration = Duration::from_secs(1);

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.feed == 0 {
        bail!("--feed 0 asks for nothing");
    }
    if cli.follow && cli.stream.as_deref().is_none_or(|stream| stream == "-") {
        bail!("--follow needs a file; stdin is already a stream");
    }
    if cli.follow && cli.snapshot.is_some() {
        bail!("--follow and --snapshot are different modes");
    }

    if let Some(out) = &cli.snapshot {
        return snapshot(&cli, out);
    }
    if !std::io::stdout().is_terminal() {
        bail!(
            "event-firehose needs a terminal; use `--snapshot <file>` to draw a frame from a \
             recorded stream"
        );
    }

    run(&cli)
}

fn run(cli: &Cli) -> Result<()> {
    require_input(cli.reads_stdin())?;

    let (tx, rx) = mpsc::channel();
    let mut walk = match &cli.profile {
        Some(profile) => Some(start_walk(cli, profile, tx.clone())?),
        None => {
            spawn_reader(cli, tx);
            None
        }
    };

    let mut app = App::new(cli.label(), cli.feed);
    let mut terminal = TerminalGuard::new()?;
    // A static screen should be quiet: `tui-shot` waits for a settled frame, and a loop
    // that redraws on a timer can never settle. The rate tick still marks it dirty while
    // the stream is live, so the screen moves when the numbers do.
    let mut dirty = true;
    let mut drew_at = Instant::now();

    while !app.quit {
        // A walk this viewer started says how it ended. The exit code is the difference
        // between a finished stream and a failed one, and the log holds the reason.
        if app.exit.is_none() && take_exit(walk.as_mut(), &mut app.exit) {
            dirty = true;
        }

        if drain(&rx, &mut app) {
            dirty = true;
        }
        let previous_rate = app.rate;
        app.tick(Instant::now());
        if app.rate != previous_rate {
            dirty = true;
        }

        if dirty || (!app.closed && drew_at.elapsed() >= HEARTBEAT) {
            terminal.draw(&app)?;
            dirty = false;
            drew_at = Instant::now();
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                TerminalEvent::Key(key) if key.kind == KeyEventKind::Press => {
                    dirty = true;
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => app.quit = true,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            app.quit = true;
                        }
                        KeyCode::Char('p') | KeyCode::Char(' ') => app.paused = !app.paused,
                        _ => {}
                    }
                }
                TerminalEvent::Resize(_, _) => dirty = true,
                _ => {}
            }
        }
    }

    // The walk is ours, so leaving stops it. Otherwise it spends the rest of its run
    // writing to a pipe nobody reads.
    if let Some(mut walk) = walk {
        stop(&mut walk.child)?;
    }

    Ok(())
}

/// Stops a walk that is still running. A walk that has already finished is left alone —
/// `kill` on a reaped child is an error, and it is not one.
fn stop(child: &mut Child) -> Result<()> {
    if matches!(child.try_wait(), Ok(None)) {
        child.kill().context("stopping the walk")?;
        let _ = child.wait();
    }
    Ok(())
}

/// Takes the exit status of a walk that has finished, and says whether it did.
fn take_exit(walk: Option<&mut Walk>, exit: &mut Option<String>) -> bool {
    let Some(walk) = walk else {
        return false;
    };
    let Ok(Some(status)) = walk.child.try_wait() else {
        return false;
    };
    *exit = Some(format!("walk {status} · {}", walk.log.display()));
    true
}

/// Replays a recorded stream into one frame, with no terminal involved.
fn snapshot(cli: &Cli, out: &Path) -> Result<()> {
    let (tx, rx) = mpsc::channel();
    spawn_reader(cli, tx);

    let mut app = App::new(cli.label(), cli.feed);
    loop {
        match rx.recv().context("reading the stream")? {
            Msg::Event(event) => app.push(*event),
            Msg::Malformed(line) => app.malformed(line),
            Msg::Closed => break,
        }
    }
    app.close();

    let (width, height) = parse_size(&cli.size)?;
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).context("creating the snapshot terminal")?;
    terminal
        .draw(|frame| render::draw(frame, &app))
        .context("drawing the snapshot")?;

    let text = plain_text(terminal.backend().buffer());
    std::fs::write(out, &text)
        .with_context(|| format!("writing the snapshot to {}", out.display()))?;

    let events = app.lines;
    println!(
        "wrote {} · {events} events · {}",
        out.display(),
        render::summary(&app.counts)
    );
    Ok(())
}

/// Proves keys can be read before the screen is switched.
///
/// Crossterm reads from stdin **if stdin is a tty**, and otherwise opens `/dev/tty`.
/// A viewer fed by a pipe has stdin occupied by the stream, so it needs a controlling
/// terminal — and an editor's agent panel, a task runner or a captured shell does not
/// provide one. Without this check the failure lands after the alternate screen is
/// entered and reads as a bare `Failed to initialize input reader` backtrace.
fn require_input(from_stdin: bool) -> Result<()> {
    let error = match event::poll(Duration::ZERO) {
        Ok(_) => return Ok(()),
        Err(error) => error,
    };
    let reason = if from_stdin {
        "stdin carries the event stream, so keys must come from /dev/tty, which this \
         environment does not provide"
    } else {
        "this environment has no readable /dev/tty for keys"
    };
    bail!(
        "event-firehose cannot read keys ({error}): {reason}. Run it in a real terminal \
         (`tty` prints a device), or use `--snapshot <file>` to draw a frame"
    )
}

fn drain(rx: &Receiver<Msg>, app: &mut App) -> bool {
    let mut changed = false;
    loop {
        match rx.try_recv() {
            Ok(Msg::Event(event)) => {
                app.push(*event);
                changed = true;
            }
            Ok(Msg::Malformed(line)) => {
                app.malformed(line);
                changed = true;
            }
            Ok(Msg::Closed) => {
                if !app.closed {
                    app.close();
                    changed = true;
                }
                break;
            }
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => {
                // The channel stays disconnected, so this arm is reached on every poll.
                // Reporting it as a change would redraw a still screen forever.
                if !app.closed {
                    app.close();
                    changed = true;
                }
                break;
            }
        }
    }
    changed
}

/// Reads the stream on its own thread, so the UI keeps drawing while a walk is slow and
/// keeps draining after the producer has gone.
fn spawn_reader(cli: &Cli, tx: Sender<Msg>) {
    let stream = cli.stream.clone().unwrap_or_else(|| "-".to_owned());
    let follow = cli.follow;

    std::thread::spawn(move || {
        let result = if stream == "-" {
            let stdin = std::io::stdin();
            read_lines(stdin.lock(), follow, &tx)
        } else {
            match std::fs::File::open(&stream) {
                Ok(file) => read_lines(BufReader::new(file), follow, &tx),
                Err(error) => Err(error),
            }
        };
        if let Err(error) = result {
            let _ = tx.send(Msg::Malformed(format!("{stream}: {error}")));
        }
        let _ = tx.send(Msg::Closed);
    });
}

/// The walk this viewer started, and where its report went.
struct Walk {
    child: Child,
    log: PathBuf,
}

impl Drop for Walk {
    /// A walk is this process's child, so it must not outlive it — including on the paths
    /// that return early, where `run` never reaches its explicit stop.
    fn drop(&mut self) {
        let _ = stop(&mut self.child);
    }
}

/// Runs the profile's walk with its events on a pipe and its report in a file.
///
/// The child's stdin is null: it must not race this process for the terminal, and this
/// process is the one that decides when the screen closes. Its stderr goes to a file for
/// the same reason — a log line drawn over the alternate screen sits there until the next
/// heartbeat, and a walk's report is worth keeping anyway.
fn start_walk(cli: &Cli, profile: &Path, tx: Sender<Msg>) -> Result<Walk> {
    let ledger = ledger_binary(cli)?;
    let log = log_path(profile);
    if let Some(dir) = log.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let sink =
        std::fs::File::create(&log).with_context(|| format!("creating {}", log.display()))?;

    let mut child = Command::new(&ledger)
        .arg("walk")
        .arg("--profile")
        .arg(profile)
        .arg("--events")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(sink))
        .spawn()
        .with_context(|| format!("running {}", ledger.display()))?;

    let stdout = child.stdout.take().context("the walk's stdout")?;
    spawn_pipe(
        BufReader::new(stdout),
        format!("{} walk", ledger.display()),
        tx,
    );

    Ok(Walk { child, log })
}

/// `token-ledger` beside this binary, or the one named on the command line.
///
/// Beside, not on `PATH`: both are built by the same `cargo build`, so the viewer and the
/// walker are the same revision by construction. A different one is a `--token-ledger`
/// away, which is also what a nix-installed pair would need.
fn ledger_binary(cli: &Cli) -> Result<PathBuf> {
    if let Some(path) = &cli.token_ledger {
        return Ok(path.clone());
    }
    let me = std::env::current_exe().context("finding this executable")?;
    let dir = me.parent().context("this executable has no directory")?;
    let name = if cfg!(windows) {
        "token-ledger.exe"
    } else {
        "token-ledger"
    };
    let beside = dir.join(name);
    if beside.is_file() {
        return Ok(beside);
    }
    bail!(
        "no {name} beside {} — build it with `cargo build -p token-ledger`, or name one \
         with --token-ledger",
        me.display()
    )
}

/// Where the walk's own report goes. Next to the other scratch output, named for the
/// profile so two of them cannot overwrite each other.
fn log_path(profile: &Path) -> PathBuf {
    let stem = profile
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_else(|| "walk".to_owned());
    PathBuf::from(".tmp").join(format!("{stem}.walk.log"))
}

/// Reads a child's stdout on its own thread, exactly as a file is read.
fn spawn_pipe(reader: impl BufRead + Send + 'static, what: String, tx: Sender<Msg>) {
    std::thread::spawn(move || {
        if let Err(error) = read_lines(reader, false, &tx) {
            let _ = tx.send(Msg::Malformed(format!("{what}: {error}")));
        }
        let _ = tx.send(Msg::Closed);
    });
}

/// How long a follower waits before asking a file for more.
const FOLLOW_POLL: Duration = Duration::from_millis(100);

fn read_lines(mut reader: impl BufRead, follow: bool, tx: &Sender<Msg>) -> std::io::Result<()> {
    let mut line = String::new();
    loop {
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            // EOF. A follower waits for more; a partial line stays in the buffer, so the
            // next successful read completes it rather than parsing half an event.
            if !follow {
                return Ok(());
            }
            std::thread::sleep(FOLLOW_POLL);
            continue;
        }
        if follow && !line.ends_with('\n') {
            // EOF landed in the middle of a line the writer has not finished.
            std::thread::sleep(FOLLOW_POLL);
            continue;
        }

        let line = std::mem::take(&mut line);
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<ChainEvent>(line) {
            Ok(event) => {
                if tx.send(Msg::Event(Box::new(event))).is_err() {
                    // The UI has exited; there is nobody left to stream for.
                    return Ok(());
                }
            }
            Err(error) => {
                if tx.send(Msg::Malformed(format!("{error}: {line}"))).is_err() {
                    return Ok(());
                }
            }
        }
    }
}

fn parse_size(text: &str) -> Result<(u16, u16)> {
    let (columns, rows) = text
        .split_once('x')
        .with_context(|| format!("{text:?} is not COLSxROWS"))?;
    Ok((
        columns
            .parse()
            .with_context(|| format!("{columns:?} is not a width"))?,
        rows.parse()
            .with_context(|| format!("{rows:?} is not a height"))?,
    ))
}

/// The frame as characters, one terminal row per line. A snapshot is for reading, not for
/// replaying colour escapes.
fn plain_text(buffer: &Buffer) -> String {
    let area = buffer.area;
    let mut out = String::new();
    for y in area.y..area.y + area.height {
        for x in area.x..area.x + area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        while out.ends_with(' ') {
            out.pop();
        }
        out.push('\n');
    }
    out
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode().context("entering raw mode")?;
        let mut out = std::io::stdout();
        execute!(out, EnterAlternateScreen).context("entering the alternate screen")?;
        let terminal =
            Terminal::new(CrosstermBackend::new(out)).context("creating the terminal")?;
        Ok(Self { terminal })
    }

    fn draw(&mut self, app: &App) -> Result<()> {
        self.terminal
            .draw(|frame| render::draw(frame, app))
            .context("drawing the frame")?;
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Err(error) = disable_raw_mode() {
            eprintln!("event-firehose: could not leave raw mode: {error}");
        }
        if let Err(error) = execute!(self.terminal.backend_mut(), LeaveAlternateScreen) {
            eprintln!("event-firehose: could not leave the alternate screen: {error}");
        }
        if let Err(error) = self.terminal.show_cursor() {
            eprintln!("event-firehose: could not restore the cursor: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::fixture::holder;
    use chain_events::HolderKind;
    use std::io::Write;

    #[test]
    fn a_size_is_two_numbers_around_an_x() {
        assert_eq!(parse_size("120x36").unwrap(), (120, 36));
        assert!(parse_size("120").is_err());
        assert!(parse_size("widex36").is_err());
    }

    #[test]
    fn follow_reads_a_line_appended_after_eof() {
        let path = std::env::temp_dir().join(format!(
            "event-firehose-follow-{}.ndjson",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let event = holder(HolderKind::Mint, &[("addr1va", 1)], 100);
        let line = serde_json::to_string(&event).unwrap();
        std::fs::write(&path, format!("{line}\n")).unwrap();

        let (tx, rx) = mpsc::channel();
        let reader = std::fs::File::open(&path).unwrap();
        let handle = std::thread::spawn(move || read_lines(BufReader::new(reader), true, &tx));

        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)),
            Ok(Msg::Event(_))
        ));

        // The reader is now at EOF. A line appended after that must still arrive.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        writeln!(file, "{line}").unwrap();
        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(2)),
            Ok(Msg::Event(_))
        ));

        // Dropping the receiver stops the follower: its next send fails.
        drop(rx);
        writeln!(file, "{{not json").unwrap();
        let outcome = handle.join().expect("the reader stops when the UI is gone");
        assert!(outcome.is_ok(), "the reader failed: {outcome:?}");

        std::fs::remove_file(&path).unwrap();
    }
}
