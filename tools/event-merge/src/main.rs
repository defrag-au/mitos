//! Merges several `chain-events` NDJSON streams into one, in chain order.
//!
//! The producers are independent: `token-ledger walk --events` reads the chunks,
//! `policy-events` reads a sealed archive, and the marketplace tap will read
//! `market-ledger`. Each writes the same envelope, so a consumer that wants "this
//! policy's history" wants them merged — and in chain order, not concatenated, because
//! they interleave block by block and transaction by transaction.
//!
//! ```text
//! event-merge --policy <hex> holder.ndjson market.ndjson > perps.ndjson
//! ```
//!
//! # Streaming, bounded by the watermark
//!
//! Every feed is read one event at a time. The merge holds a slot until every feed's head
//! has moved past it, then writes and drops everything below — so memory is one block's
//! worth of events when the feeds keep pace, and the gap between them when one lags. The
//! report prints `peak held`, which is that number.
//!
//! The one exception is a source that arrives newest-first (the archive producer pages
//! that way): its smallest event is the *last* line, so it has to be read whole and
//! sorted before the merge can start. The report names it as `descending (buffered)`.
//!
//! # N sources, not two
//!
//! A third producer is another argument, not another code path.
//!
//! # A closed pipe is not an error
//!
//! `| head` is a consumer that has finished. The merge stops rather than reading the rest
//! of the file for nobody.

mod merge;

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chain_events::ChainEvent;
use clap::Parser;

use merge::{Feed, Key, merge, order_of};

#[derive(Parser, Debug)]
#[command(
    name = "event-merge",
    version,
    about = "Merges chain-events NDJSON streams into one, in chain order"
)]
struct Cli {
    /// Keep only this collection (a bare 56-hex policy id). Repeatable; omit to keep all.
    #[arg(long = "policy", value_name = "HEX")]
    policy: Vec<String>,

    /// NDJSON streams to merge. `-` reads stdin.
    #[arg(value_name = "STREAM", required = true)]
    input: Vec<PathBuf>,

    /// Write here instead of stdout.
    #[arg(long, value_name = "FILE")]
    out: Option<PathBuf>,
}

/// Returned by the writer when the consumer has gone, so the merge stops instead of
/// reading the rest of a file for nobody.
#[derive(Debug)]
struct Closed;

impl std::fmt::Display for Closed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the consumer closed the pipe")
    }
}

impl std::error::Error for Closed {}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let policies = cli
        .policy
        .iter()
        .map(|policy| check_policy(policy))
        .collect::<Result<Vec<_>>>()?;

    let mut feeds: Vec<Box<dyn Feed>> = Vec::with_capacity(cli.input.len());
    for path in &cli.input {
        feeds.push(Box::new(FileFeed::open(path)?));
    }

    let mut out: Box<dyn Write> = match &cli.out {
        Some(path) => Box::new(BufWriter::new(
            std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?,
        )),
        None => Box::new(BufWriter::new(std::io::stdout())),
    };

    let mut closed = false;
    let written = merge(&mut feeds, &policies, &mut |event| {
        let mut line = serde_json::to_vec(&event).context("serialising an event")?;
        line.push(b'\n');
        match out.write_all(&line) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::BrokenPipe => {
                closed = true;
                Err(Closed.into())
            }
            Err(error) => Err(error).context("writing the merged stream"),
        }
    });

    let stats = match written {
        Ok(stats) => stats,
        // The consumer stopped reading. That is a finished command, not a failed one.
        Err(error) if error.is::<Closed>() => return Ok(()),
        Err(error) => return Err(error),
    };

    if !closed
        && let Err(error) = out.flush()
        && error.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(error).context("flushing the merged stream");
    }

    report(&feeds, &stats);
    Ok(())
}

fn check_policy(text: &str) -> Result<String> {
    let policy = text.to_lowercase();
    if policy.len() != 56 || !policy.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("--policy {text:?} is not a 56-hex policy id");
    }
    Ok(policy)
}

/// A reader that yields events and counts what it could not parse.
struct Lines {
    path: PathBuf,
    reader: Box<dyn BufRead>,
    malformed: u64,
    first_error: Option<String>,
}

impl Lines {
    fn open(path: &Path) -> Result<Self> {
        let reader: Box<dyn BufRead> = if path.as_os_str() == "-" {
            Box::new(BufReader::new(std::io::stdin()))
        } else {
            let file =
                std::fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
            Box::new(BufReader::new(file))
        };
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            malformed: 0,
            first_error: None,
        })
    }

    /// The next event, skipping — and counting — lines that are not one.
    fn next_event(&mut self) -> Result<Option<ChainEvent>> {
        loop {
            let mut line = String::new();
            let read = self
                .reader
                .read_line(&mut line)
                .with_context(|| format!("reading {}", self.path.display()))?;
            if read == 0 {
                return Ok(None);
            }

            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<ChainEvent>(line) {
                Ok(event) => return Ok(Some(event)),
                Err(error) => {
                    self.malformed += 1;
                    self.first_error
                        .get_or_insert_with(|| format!("{error}: {}", shorten(line)));
                }
            }
        }
    }
}

/// One file or stdin, served in ascending chain order.
struct FileFeed {
    lines: Lines,
    /// Events read while the direction was being decided — at most two, and empty after.
    pending: VecDeque<ChainEvent>,
    /// A newest-first source, read whole and sorted.
    buffered: Option<std::vec::IntoIter<ChainEvent>>,
    was_buffered: bool,
    read: u64,
    last: Option<(bool, u64)>,
}

impl FileFeed {
    fn open(path: &Path) -> Result<Self> {
        let mut lines = Lines::open(path)?;
        let mut pending = VecDeque::new();
        let mut first = None;

        if let Some(event) = lines.next_event()? {
            first = Some(order_of(&event));
            pending.push_back(event);
        }
        if let Some(first_order) = first
            && let Some(second) = lines.next_event()?
        {
            if order_of(&second) < first_order {
                // Newest-first: the smallest event is the LAST line, so the whole source
                // has to be read before the merge can start. The one case that costs its
                // own size, and the report says so.
                let mut all: Vec<ChainEvent> = pending.drain(..).collect();
                all.push(second);
                while let Some(event) = lines.next_event()? {
                    all.push(event);
                }
                all.sort_by_key(Key::of);
                return Ok(Self {
                    lines,
                    pending: VecDeque::new(),
                    buffered: Some(all.into_iter()),
                    was_buffered: true,
                    read: 0,
                    last: None,
                });
            }
            pending.push_back(second);
        }

        Ok(Self {
            lines,
            pending,
            buffered: None,
            was_buffered: false,
            read: 0,
            last: None,
        })
    }
}

impl Feed for FileFeed {
    fn next_event(&mut self) -> Result<Option<ChainEvent>> {
        if let Some(buffered) = &mut self.buffered {
            let event = buffered.next();
            self.read += u64::from(event.is_some());
            return Ok(event);
        }

        let event = match self.pending.pop_front() {
            Some(event) => Some(event),
            None => self.lines.next_event()?,
        };
        if let Some(event) = &event {
            let order = order_of(event);
            if let Some(last) = self.last
                && order < last
            {
                // The merge releases a slot on the promise that no feed can produce
                // anything below it. A source that goes backwards breaks that promise,
                // so it is refused rather than quietly misordered. Only the BLOCK has to
                // ascend — a producer's own order inside one block is not the merge's.
                bail!(
                    "{} is not in slot order: a later event sorts before an earlier one. \
                     Merge that source on its own, or fix the producer",
                    self.lines.path.display()
                );
            }
            self.last = Some(order);
            self.read += 1;
        }
        Ok(event)
    }

    fn label(&self) -> String {
        self.lines.path.display().to_string()
    }

    fn malformed(&self) -> u64 {
        self.lines.malformed
    }

    fn buffered(&self) -> bool {
        self.was_buffered
    }

    fn read(&self) -> u64 {
        self.read
    }
}

fn report(feeds: &[Box<dyn Feed>], stats: &merge::Stats) {
    let mut out = std::io::stderr().lock();
    let _ = writeln!(
        out,
        "# event-merge · {} sources · {} read · {} kept · {} filtered · {} duplicates · \
         {} superseded · {} conflicts · {} unplaced · peak held {}",
        feeds.len(),
        stats.read,
        stats.kept,
        stats.filtered,
        stats.duplicates,
        stats.superseded,
        stats.conflicts,
        stats.unplaced,
        stats.peak_held,
    );

    for feed in feeds {
        let mut note = String::new();
        if feed.buffered() {
            note.push_str(" · descending (buffered)");
        }
        if feed.malformed() > 0 {
            note.push_str(&format!(" · {} malformed", feed.malformed()));
        }
        let _ = writeln!(out, "#   {}  {} events{note}", feed.label(), feed.read());
    }

    let _ = writeln!(out, "# span {}", span(stats));
}

fn span(stats: &merge::Stats) -> String {
    match (stats.first_slot, stats.last_slot) {
        (Some(first), Some(last)) => format!("{first}–{last}"),
        _ => "no placed facts".to_owned(),
    }
}

fn shorten(line: &str) -> String {
    const MAX: usize = 120;
    match line.char_indices().nth(MAX) {
        Some((at, _)) => format!("{}…", &line[..at]),
        None => line.to_owned(),
    }
}
