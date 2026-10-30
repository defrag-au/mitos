//! A pure function of [`Screen`] and a clock tick. No I/O, no decisions.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Gauge, Paragraph, Wrap};

use crate::app::{BackfillSettings, CoverageView, FetchState, Outcome, Screen};
use crate::coverage::{ChunkRange, LocalCoverage, PolicyCoverage, PublishedCoverage};
use crate::fmt;
use crate::plan::RangePlan;

pub fn draw(frame: &mut Frame<'_>, screen: &Screen, tick: u64) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(0),
            Constraint::Length(2),
        ])
        .split(frame.area());

    draw_header(frame, chunks[0], screen);
    draw_body(frame, chunks[1], screen, tick);
    draw_footer(frame, chunks[2], screen);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, screen: &Screen) {
    let subject = match screen {
        Screen::Policy { .. } => "a policy → its chunks".to_string(),
        Screen::Resolving { policy, .. } => policy.to_hex(),
        Screen::Plan { plan, .. } | Screen::Fetching { plan, .. } | Screen::Done { plan, .. } => {
            plan.policy.to_hex()
        }
        Screen::Failed { policy, .. } => policy.to_hex(),
        Screen::Coverage { .. } => "the local chunk store".to_string(),
    };

    let lines = vec![
        Line::from(Span::styled(
            format!("mithril-fetch · {}", screen.title()),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(subject, Style::default().fg(Color::DarkGray))),
    ];

    frame.render_widget(
        Paragraph::new(Text::from(lines)).block(Block::bordered()),
        area,
    );
}

fn draw_body(frame: &mut Frame<'_>, area: Rect, screen: &Screen, tick: u64) {
    let block = Block::bordered().title(screen.title());
    let inner = block.inner(area);
    frame.render_widget(block, area);

    match screen {
        Screen::Policy { input, error } => draw_policy(frame, inner, input, error.as_deref()),
        Screen::Resolving { policy, status } => {
            draw_resolving(frame, inner, policy.to_hex(), status, tick)
        }
        Screen::Plan {
            plan,
            end_input,
            error,
        } => draw_plan(frame, inner, plan, end_input, error.as_deref()),
        Screen::Fetching { plan, phase, fetch } => draw_fetching(frame, inner, plan, phase, fetch),
        Screen::Done { plan, outcome } => draw_done(frame, inner, plan, outcome),
        Screen::Failed {
            policy,
            message,
            retry,
        } => draw_failed(frame, inner, policy.to_hex(), message, *retry),
        Screen::Coverage { view } => draw_coverage(frame, inner, view),
    }
}

fn draw_policy(frame: &mut Frame<'_>, area: Rect, input: &str, error: Option<&str>) {
    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Policy id (56 hex characters)",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(vec![
            Span::styled("> ", Style::default().fg(Color::Cyan)),
            Span::raw(input.to_string()),
        ]),
        Line::from(""),
    ];

    match error {
        Some(error) => lines.push(Line::from(Span::styled(
            error.to_string(),
            Style::default().fg(Color::Red),
        ))),
        None => lines.push(Line::from(Span::styled(
            "The first mint comes from Koios; the chunks come from Mithril.",
            Style::default().fg(Color::DarkGray),
        ))),
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        area,
    );

    // The input is the fourth line. Clamp so a cramped terminal leaves the
    // cursor inside the frame rather than hiding it off the edge.
    let x = area
        .x
        .saturating_add(2)
        .saturating_add(input.chars().count() as u16)
        .min(area.right().saturating_sub(1));
    let y = area
        .y
        .saturating_add(3)
        .min(area.bottom().saturating_sub(1));
    frame.set_cursor_position((x, y));
}

fn draw_resolving(frame: &mut Frame<'_>, area: Rect, policy: String, status: &str, tick: u64) {
    const SPINNER: [&str; 4] = ["|", "/", "-", "\\"];
    let spinner = SPINNER[(tick / 2) as usize % SPINNER.len()];

    let lines = vec![
        Line::from(""),
        Line::from(vec![
            Span::styled(format!("{spinner} "), Style::default().fg(Color::Cyan)),
            Span::raw(status.to_string()),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            format!("policy  {policy}"),
            Style::default().fg(Color::DarkGray),
        )),
    ];

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_plan(
    frame: &mut Frame<'_>,
    area: Rect,
    plan: &RangePlan,
    end_input: &str,
    error: Option<&str>,
) {
    let mut lines = plan_lines(plan);
    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        label("End chunk"),
        Span::styled(
            format!("{end_input:>6}"),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(
                "   editable · {}–{} · t = tip",
                plan.from, plan.latest_beacon
            ),
            Style::default().fg(Color::DarkGray),
        ),
    ]));

    if let Some(error) = error {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            error.to_string(),
            Style::default().fg(Color::Red),
        )));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        area,
    );
}

fn plan_lines(plan: &RangePlan) -> Vec<Line<'static>> {
    let floor = &plan.floor;
    let lines = vec![
        Line::from(""),
        kv("Policy", plan.policy.to_hex()),
        kv(
            "First mint",
            format!(
                "{} · {} · slot {}",
                floor.earliest_asset,
                fmt::utc(floor.earliest_unix),
                fmt::count(floor.slot)
            ),
        ),
        kv(
            "Minting tx",
            floor
                .minting_tx
                .clone()
                .unwrap_or_else(|| "not returned by Koios".to_string()),
        ),
        kv(
            "Chunk",
            format!(
                "fetch from {:05} (slot falls in {:05}; one chunk of margin)",
                plan.from, floor.exact_chunk
            ),
        ),
        kv(
            "Newest signed",
            format!(
                "{:05} · epoch {} · {}",
                plan.latest_beacon, plan.latest_epoch, plan.snapshot_created
            ),
        ),
        kv(
            "Range",
            format!(
                "{:05}–{:05} · {} immutable files",
                plan.from,
                plan.to,
                fmt::count(plan.files())
            ),
        ),
        kv(
            "Estimated",
            format!(
                "≈ {} ({} B/file, the snapshot's average)",
                fmt::bytes(plan.estimated_bytes()),
                fmt::count(plan.per_file_bytes)
            ),
        ),
        kv("Destination", plan.destination.display().to_string()),
    ];
    lines
}

fn draw_fetching(
    frame: &mut Frame<'_>,
    area: Rect,
    plan: &RangePlan,
    phase: &str,
    fetch: &FetchState,
) {
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    let done = fetch.files_done + fetch.batch_completed;
    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(f64::from(fetch.percent()) / 100.0)
        // The label lives on its own line: a centred label over the filled bar
        // runs into the block glyphs at narrow widths, and the body's own block
        // is the only frame this screen needs.
        .label("");
    frame.render_widget(gauge, parts[0]);

    let rate = fetch.rate();
    let eta = fetch
        .eta_secs()
        .map(fmt::duration)
        .unwrap_or_else(|| "—".to_string());
    let mut lines = vec![
        Line::from(""),
        kv(
            "Files",
            format!(
                "{} of {} · {}%",
                fmt::count(done),
                fmt::count(fetch.files_total),
                fetch.percent()
            ),
        ),
        kv(
            "Received",
            format!(
                "{} of ≈ {}",
                fmt::bytes(fetch.received()),
                fmt::bytes(fetch.projected_bytes())
            ),
        ),
        kv("Rate", format!("{}/s · ETA {eta}", fmt::bytes(rate as u64))),
        kv("Phase", phase.to_string()),
        kv(
            "In flight",
            if fetch.current.is_empty() {
                "—".to_string()
            } else {
                fetch
                    .current
                    .iter()
                    .map(|number| format!("{number:05}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            },
        ),
        kv("Elapsed", fmt::duration(fetch.elapsed_secs())),
        kv(
            "Store",
            format!(
                "{} · {} file(s) done",
                plan.destination.display(),
                fmt::count(fetch.files_done)
            ),
        ),
    ];
    if fetch.ranges.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "A cancelled run resumes at the first missing immutable file.",
            Style::default().fg(Color::DarkGray),
        )));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        parts[1],
    );
}

fn draw_done(frame: &mut Frame<'_>, area: Rect, plan: &RangePlan, outcome: &Outcome) {
    let heading = if outcome.files == 0 {
        "The store already holds this range."
    } else {
        "Downloaded and verified."
    };
    let ranges = if outcome.ranges.is_empty() {
        "none — the range was already present".to_string()
    } else {
        outcome
            .ranges
            .iter()
            .map(|range| format!("{:05}–{:05}", range.from, range.to))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            heading,
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        kv(
            "Fetched",
            format!(
                "{} files · {}",
                fmt::count(outcome.files),
                fmt::bytes(outcome.bytes)
            ),
        ),
        kv(
            "Already present",
            format!("{} files", fmt::count(outcome.already_present)),
        ),
        kv("Elapsed", fmt::duration(outcome.elapsed_secs)),
        kv("Ranges", ranges),
        kv("Store", plan.destination.display().to_string()),
        kv("Manifest", format!("policies/{}.json", plan.policy)),
        Line::from(""),
        Line::from(Span::styled(
            "Next",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];
    for line in crate::plan::next_command_lines(plan) {
        lines.push(Line::from(format!("  {line}")));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_failed(frame: &mut Frame<'_>, area: Rect, policy: String, message: &str, retry: bool) {
    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Could not continue.",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(message.to_string()),
        Line::from(""),
        Line::from(Span::styled(
            format!("policy  {policy}"),
            Style::default().fg(Color::DarkGray),
        )),
    ];
    if retry {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Press r to try again; a cancelled download resumes where it stopped.",
            Style::default().fg(Color::DarkGray),
        )));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_coverage(frame: &mut Frame<'_>, area: Rect, view: &CoverageView) {
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(0)])
        .split(area);

    let percent = view
        .published
        .as_ref()
        .map(PublishedCoverage::percent)
        .unwrap_or(0);
    let gauge = Gauge::default()
        .gauge_style(Style::default().fg(Color::Cyan))
        .ratio(f64::from(percent) / 100.0)
        .label("");
    frame.render_widget(gauge, parts[0]);

    let mut lines = vec![Line::from("")];
    lines.extend(local_lines(view.local.as_ref()));
    lines.extend(published_lines(view.published.as_ref()));
    lines.extend(policy_lines(view));
    lines.extend(backfill_lines(view));

    if let Some(error) = &view.error {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            error.clone(),
            Style::default().fg(Color::Red),
        )));
    }
    if !view.status.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            view.status.clone(),
            Style::default().fg(Color::DarkGray),
        )));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }),
        parts[1],
    );
}

fn local_lines(local: Option<&LocalCoverage>) -> Vec<Line<'static>> {
    match local {
        Some(local) => {
            let mut lines = vec![
                kv(
                    "Local",
                    format!(
                        "{} files · {} · {}",
                        fmt::count(local.files),
                        fmt::bytes(local.bytes),
                        span(local.first, local.last)
                    ),
                ),
                kv(
                    "Incomplete",
                    if local.incomplete.is_empty() {
                        "none".to_string()
                    } else {
                        format!(
                            "{} · {}",
                            fmt::count(local.incomplete.len() as u64),
                            numbers(&local.incomplete, 8)
                        )
                    },
                ),
            ];
            if !local.policy_errors.is_empty() {
                lines.push(kv(
                    "Policy errors",
                    format!("{} manifest(s) unreadable", local.policy_errors.len()),
                ));
            }
            lines
        }
        None => vec![kv("Local", "scanning…".to_string())],
    }
}

fn published_lines(published: Option<&PublishedCoverage>) -> Vec<Line<'static>> {
    match published {
        Some(published) => {
            let mut lines = vec![
                kv(
                    "Published",
                    format!(
                        "00001–{:05} · {} files · beacon {:05} (epoch {})",
                        published.beacon,
                        fmt::count(published.published_files),
                        published.beacon,
                        published.epoch
                    ),
                ),
                kv(
                    "Covered",
                    format!(
                        "{} of {} · {}",
                        fmt::count(published.covered_files),
                        fmt::count(published.published_files),
                        published.percent_label()
                    ),
                ),
                kv(
                    "Missing",
                    format!(
                        "{} files · ≈ {}",
                        fmt::count(published.missing_files),
                        fmt::bytes(published.missing_bytes)
                    ),
                ),
                kv(
                    "Gaps",
                    if published.gaps.is_empty() {
                        "none".to_string()
                    } else {
                        format!(
                            "{} run(s) · {}",
                            published.gap_count,
                            gap_list(&published.gaps, 6)
                        )
                    },
                ),
            ];
            if published.unpublished_files > 0 {
                lines.push(kv(
                    "Unpublished",
                    format!(
                        "{} local file(s) Mithril does not publish",
                        fmt::count(published.unpublished_files)
                    ),
                ));
            }
            lines.push(kv(
                "Per file",
                format!(
                    "≈ {} (the snapshot's average)",
                    fmt::bytes(published.per_file_bytes)
                ),
            ));
            lines
        }
        None => vec![kv("Published", "reading the aggregator…".to_string())],
    }
}

fn policy_lines(view: &CoverageView) -> Vec<Line<'static>> {
    let Some(local) = &view.local else {
        return Vec::new();
    };
    if local.policies.is_empty() {
        return Vec::new();
    }

    let mut lines = vec![
        Line::from(""),
        Line::from(Span::styled(
            "Policies",
            Style::default().add_modifier(Modifier::BOLD),
        )),
    ];
    for policy in local.policies.iter().take(8) {
        lines.push(policy_line(policy));
    }
    if local.policies.len() > 8 {
        lines.push(Line::from(format!("  +{} more", local.policies.len() - 8)));
    }
    lines
}

fn policy_line(policy: &PolicyCoverage) -> Line<'static> {
    let end = policy
        .requested_to
        .map(|to| format!("{to:05}"))
        .unwrap_or_else(|| "-----".to_string());
    Line::from(format!(
        "  {:<12} {:05}–{end}  {:>5}/{:<5} {:>3}%  {}",
        short(&policy.policy, 12),
        policy.floor_chunk,
        fmt::count(policy.covered),
        fmt::count(policy.total),
        policy.percent(),
        short(&policy.earliest_asset, 12)
    ))
}

fn backfill_lines(view: &CoverageView) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from("")];
    match &view.fill {
        Some(fill) => {
            let fetch = &fill.fetch;
            lines.push(kv(
                "Backfill",
                format!(
                    "running · {} of {} files · {} of ≈ {} · {}/s · ETA {}",
                    fmt::count(fetch.files_done + fetch.batch_completed),
                    fmt::count(fetch.files_total),
                    fmt::bytes(fetch.received()),
                    fmt::bytes(fetch.projected_bytes()),
                    fmt::bytes(fetch.rate() as u64),
                    fetch
                        .eta_secs()
                        .map(fmt::duration)
                        .unwrap_or_else(|| "—".to_string())
                ),
            ));
            lines.push(kv("Phase", fill.phase.clone()));
            lines.push(kv("Elapsed", fmt::duration(fetch.elapsed_secs())));
            lines.push(kv("Settings", settings_line(fill.settings)));
        }
        None => {
            let Some(published) = &view.published else {
                // A rescan clears the join while the new one is being read; a
                // range built from a missing beacon would read as 00001–00000.
                lines.push(kv("Backfill", "waiting for the published set…".to_string()));
                return lines;
            };
            let missing = published.missing_files;
            let target = published.beacon;
            if missing == 0 {
                lines.push(kv(
                    "Backfill",
                    "complete — nothing published is missing".to_string(),
                ));
            } else {
                lines.push(kv(
                    "Backfill",
                    format!(
                        "idle · b fills 00001–{:05} · {}",
                        target,
                        settings_line(view.settings)
                    ),
                ));
            }
        }
    }
    lines
}

fn settings_line(settings: BackfillSettings) -> String {
    format!(
        "{} parallel · {}-file batches · {}s pause",
        settings.parallel, settings.batch_files, settings.pause_secs
    )
}

fn span(first: Option<u64>, last: Option<u64>) -> String {
    match (first, last) {
        (Some(first), Some(last)) => format!("{first:05}–{last:05}"),
        _ => "empty".to_string(),
    }
}

fn gap_list(gaps: &[ChunkRange], max: usize) -> String {
    let mut out = gaps
        .iter()
        .take(max)
        .map(ChunkRange::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    if gaps.len() > max {
        out.push_str(&format!(", +{} more", gaps.len() - max));
    }
    out
}

fn numbers(numbers: &[u64], max: usize) -> String {
    let mut out = numbers
        .iter()
        .take(max)
        .map(|number| format!("{number:05}"))
        .collect::<Vec<_>>()
        .join(" ");
    if numbers.len() > max {
        out.push_str(&format!(" +{}", numbers.len() - max));
    }
    out
}

fn short(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect, screen: &Screen) {
    let hints = match screen {
        Screen::Policy { .. } => "Enter resolve · v coverage · Esc clear · Ctrl-C quit",
        Screen::Resolving { .. } => "Esc back · q quit",
        Screen::Plan { .. } => {
            "Enter start · digits edit end · t tip · v coverage · Esc back · q quit"
        }
        Screen::Fetching { .. } => "Esc cancel · q quit",
        Screen::Done { .. } => "v coverage · Esc new policy · q quit",
        Screen::Failed { retry: true, .. } => "r retry · v coverage · Esc back · q quit",
        Screen::Failed { retry: false, .. } => "v coverage · Esc back · q quit",
        Screen::Coverage { view } if view.fill.is_some() => "Esc stop · q quit",
        Screen::Coverage { .. } => "b backfill · r rescan · Esc back · q quit",
    };

    frame.render_widget(
        Paragraph::new(Line::from(Span::styled(
            hints,
            Style::default().fg(Color::DarkGray),
        ))),
        area,
    );
}

fn label(text: &'static str) -> Span<'static> {
    Span::styled(format!("{text:<17}"), Style::default().fg(Color::DarkGray))
}

fn kv(text: &'static str, value: String) -> Line<'static> {
    Line::from(vec![label(text), Span::raw(value)])
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::app::{FetchState, Outcome, Screen};
    use crate::plan::{PolicyFloor, PolicyId, RangePlan, Tip};

    fn plan() -> RangePlan {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = PolicyFloor {
            assets: 1,
            earliest_unix: 1_600_000_000,
            earliest_asset: "DONG".to_string(),
            minting_tx: Some("ab".repeat(32)),
            slot: 4_000_000,
            exact_chunk: 185,
            floor_chunk: 184,
        };
        RangePlan::new(
            policy,
            floor,
            PathBuf::from("/store"),
            Tip {
                beacon: 9_400,
                epoch: 500,
                created_at: "2026-10-29T00:00:00Z".to_string(),
                total_bytes: 232_439_984_455,
            },
            None,
        )
        .unwrap()
    }

    fn frame_text(screen: &Screen, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, screen, 4)).unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
                    .trim_end()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn the_plan_names_the_floor_the_range_and_the_estimate() {
        let text = frame_text(
            &Screen::Plan {
                plan: plan(),
                end_input: "9400".to_string(),
                error: None,
            },
            120,
            40,
        );
        assert!(text.contains("First mint"));
        assert!(text.contains("fetch from 00184"));
        assert!(text.contains("00184–09400"));
        assert!(text.contains("Estimated"));
        assert!(text.contains("End chunk"));
    }

    #[test]
    fn a_narrow_plan_frame_still_shows_the_floor_and_the_end() {
        let text = frame_text(
            &Screen::Plan {
                plan: plan(),
                end_input: "0200".to_string(),
                error: None,
            },
            80,
            24,
        );
        assert!(text.contains("00184"));
        assert!(text.contains("0200"));
        assert!(text.contains("Enter start"));
    }

    #[test]
    fn the_fetching_frame_reports_files_bytes_and_rate() {
        let plan = plan();
        let mut fetch = FetchState::started(&plan);
        fetch.apply(crate::app::FetchProgress::Started { total: 2 });
        fetch.apply(crate::app::FetchProgress::FileStarted {
            number: 184,
            size: 25_000_000,
        });
        fetch.apply(crate::app::FetchProgress::FileProgress {
            number: 184,
            downloaded: 12_500_000,
        });
        let text = frame_text(
            &Screen::Fetching {
                plan,
                phase: "fetching 00184–00185".to_string(),
                fetch,
            },
            120,
            40,
        );
        assert!(text.contains("Received"));
        assert!(text.contains("12.4 MB") || text.contains("11.9 MiB"));
        assert!(text.contains("fetching 00184–00185"));
    }

    #[test]
    fn the_done_frame_prints_the_command_to_explore_the_policy() {
        let plan = plan();
        let outcome = Outcome {
            ranges: vec![],
            files: 9_217,
            bytes: 232_439_984_455,
            already_present: 0,
            elapsed_secs: 3_725,
        };
        let text = frame_text(&Screen::Done { plan, outcome }, 120, 40);
        assert!(text.contains("Downloaded and verified"));
        assert!(text.contains("token-ledger reverse"));
        assert!(text.contains("--first-mint 4000000"));
    }

    #[test]
    fn a_done_frame_with_nothing_fetched_says_the_store_had_it() {
        let plan = plan();
        let outcome = Outcome {
            ranges: vec![],
            files: 0,
            bytes: 0,
            already_present: 9_217,
            elapsed_secs: 0,
        };
        let text = frame_text(&Screen::Done { plan, outcome }, 100, 30);
        assert!(text.contains("The store already holds this range"));
        assert!(!text.contains("Downloaded and verified"));
    }

    #[test]
    fn the_coverage_frame_shows_local_published_and_the_backfill_settings() {
        use std::collections::BTreeSet;

        let view = CoverageView {
            local: Some(LocalCoverage {
                files: 4,
                bytes: 151_000_000,
                first: Some(6201),
                last: Some(6204),
                incomplete: Vec::new(),
                policies: Vec::new(),
                policy_errors: Vec::new(),
                complete: BTreeSet::from([6201, 6202, 6203, 6204]),
            }),
            published: Some(PublishedCoverage {
                beacon: 9334,
                epoch: 664,
                created_at: "2026-10-29T00:00:00Z".to_string(),
                per_file_bytes: 25_394_967,
                published_files: 9335,
                covered_files: 4,
                missing_files: 9331,
                missing_bytes: 236_960_000_000,
                unpublished_files: 0,
                gaps: vec![ChunkRange { from: 1, to: 6200 }],
                gap_count: 1,
            }),
            settings: BackfillSettings {
                parallel: 4,
                batch_files: 20,
                pause_secs: 10,
            },
            ..CoverageView::default()
        };
        let text = frame_text(&Screen::Coverage { view }, 120, 40);
        assert!(text.contains("Covered"));
        assert!(text.contains("9,331 files"));
        assert!(text.contains("00001–06200"));
        assert!(text.contains("4 parallel · 20-file batches · 10s pause"));
        assert!(text.contains("b backfill"));
        assert!(
            !text.contains("│┌"),
            "the gauge must not draw a second border inside the body"
        );
    }

    #[test]
    fn a_coverage_frame_without_the_published_half_does_not_invent_a_range() {
        use std::collections::BTreeSet;

        let view = CoverageView {
            local: Some(LocalCoverage {
                files: 4,
                bytes: 151_000_000,
                first: Some(6201),
                last: Some(6204),
                incomplete: Vec::new(),
                policies: Vec::new(),
                policy_errors: Vec::new(),
                complete: BTreeSet::from([6201, 6202, 6203, 6204]),
            }),
            status: "reading the aggregator's signed snapshots…".to_string(),
            ..CoverageView::default()
        };
        let text = frame_text(&Screen::Coverage { view }, 100, 30);
        assert!(text.contains("waiting for the published set"));
        assert!(
            !text.contains("00001–00000"),
            "a missing beacon must not become chunk zero"
        );
    }

    #[test]
    fn the_policy_frame_says_what_it_wants() {
        let text = frame_text(
            &Screen::Policy {
                input: "16657df".to_string(),
                error: Some("a policy id is 56 hex characters, this one is 7".to_string()),
            },
            100,
            30,
        );
        assert!(text.contains("56 hex characters"));
        assert!(text.contains("16657df"));
    }
}
