# event-firehose

A terminal view over the `chain-events` NDJSON stream. The producer writes one event per
line and keeps its own report on stderr; the viewer reads that line stream and draws it.

## One command: `--profile`

```sh
event-firehose --profile profiles/perps.toml
```

The viewer runs `token-ledger walk --profile <file> --events` itself, with the child's
stdout on a pipe and its stdin null. The terminal stays with the viewer, so keys work
everywhere — including an editor's agent panel and a task runner, which have no controlling
terminal for crossterm to fall back to. The profile names the Mithril snapshot, so the
events come straight from the chunks: no archive, no intermediate file.

The walk's own report goes to `.tmp/<profile>.walk.log` and its exit status is drawn in the
footer, so a failed walk is visible on screen and explained in the log. Leaving the viewer
stops the walk — it is the viewer's child. `--token-ledger <path>` names a walker somewhere
other than beside this binary.

## The other wirings

**Two commands, no pipe.** The producer writes a file, the viewer follows it. stdin stays on
the terminal, so keys work everywhere — including an editor's agent panel and a task runner,
which have no controlling terminal for crossterm to fall back to.

```sh
token-ledger walk --data-dir ~/data/mithril --policy <policy-hex> --events \
    > .tmp/perps.ndjson 2> .tmp/walk.log
event-firehose .tmp/perps.ndjson --follow
```

Run the first with `&` or in another terminal. `--follow` waits at EOF and parses each line
as it lands, holding a partial line until its newline arrives. The file survives the run, so
the same stream replays later without `--follow`.

**A pipe, where a real terminal exists.** `tty` prints a device. Not in a `run-shell`, an
`if-shell` or a captured command — those have no controlling terminal, and the viewer says
so before it switches screens.

```sh
token-ledger walk --data-dir ~/data/mithril --policy <policy-hex> --events \
    2> .tmp/walk.log | event-firehose
```

Keep the `2>`: `|&` or `2>&1` puts the producer's report into the stream the viewer parses,
where it shows up as malformed lines.

**A finished stream, and a frame with no terminal at all:**

```sh
event-firehose .tmp/archive-events.ndjson
event-firehose .tmp/archive-events.ndjson --snapshot .tmp/frame.txt --size 120x36
```

`--follow` and `--snapshot` are different modes and cannot be combined; `--follow` refuses
`-` (stdin is already a stream).

## What it shows

- **Header** — source, `live` / `paused` / `closed`, events consumed, ingest rate.
- **Counters** — mint · transfer · burn · market · asset · admin, then accounts, units moved,
  the slot range with the stream's direction, and the coverage the events claim.
- **`events / 1k slots`** — a histogram of chain activity. A quiet stretch is drawn as empty
  buckets, so a mint burst after a lull reads as a burst. A live walk is drawn in chain order;
  the archive, which pages newest-first, is drawn newest first and says so.
- **Feed** — the newest chain rows: slot, chain time, kind, quantity, asset name, and the
  movement. A transfer is `from -> to`, a mint `-> to`, and a side with several parties is
  `N parties` rather than a guessed sender. Splits, batches and unattributed rows are marked.
- **Footer** — the biggest net mover and the last anomaly.

`q` quits. `p` or space pauses the feed; the counters keep counting. A stream that ends leaves
the screen up and freezes the rate at the run's average.

## The screen is a pure function

`app.rs` folds events into a bounded model and takes the clock as an argument; `render.rs` draws
that model. The tests build events with the real types and assert both the model and a
`TestBackend` frame, so layout changes are checked without a terminal. `--snapshot` renders one
plain-text frame, which is what a review of a recorded stream should read.

## Capturing a frame

`tui-shot` owns stdin through its PTY, so a capture replays a recorded stream rather than a
live one:

```text
run artifacts=.tmp/tui/firehose settle=40ms

tui app bin=event-firehose size=120x36 args=".tmp/perps4.ndjson"

wait app contains="closed"
capture app out=01-arrival

key app p
wait app contains="paused"
capture app out=02-paused

resize app 80x24
wait app idle=120ms
capture app out=03-narrow

key app q
wait app exit=true
```

## Limits

- Every family is drawn, but only the holder family carries a quantity today; a market row
  shows its order reference instead of a price.
- 120 buckets of 1,000 slots is about a day and a half of chain. That is the scale a mint burst
  or a quiet week needs; it is not a zoomable chart.
- The feed is capped at `--feed` rows (default 200) and the account table is unbounded, so a
  policy with millions of distinct addresses is the case to watch.
