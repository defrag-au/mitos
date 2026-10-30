//! The screen, as a pure function of [`App`].
//!
//! Nothing here reads the clock, the stream or the terminal size beyond the frame it is
//! handed, so a capture is reproducible and a test draws the same picture every run.

use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Cell, Paragraph, Row as TableRow, Sparkline, Table, Wrap};

use crate::app::{App, Counts, Order, Row, short};

/// Draws the whole screen.
pub fn draw(frame: &mut Frame, app: &App) {
    let areas = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(2),
        Constraint::Length(4),
        Constraint::Min(4),
        Constraint::Length(1),
    ])
    .split(frame.area());

    frame.render_widget(Paragraph::new(header(app)), areas[0]);
    frame.render_widget(Paragraph::new(counters(app)), areas[1]);
    sparkline(frame, app, areas[2]);
    feed(frame, app, areas[3]);
    frame.render_widget(Paragraph::new(footer(app)), areas[4]);
}

fn header(app: &App) -> Line<'static> {
    let status = if app.paused {
        "paused"
    } else if app.closed {
        "closed"
    } else {
        "live"
    };
    let status_style = match status {
        "live" => Style::default().fg(Color::Green),
        "paused" => Style::default().fg(Color::Yellow),
        _ => Style::default().fg(Color::DarkGray),
    };

    Line::from(vec![
        Span::styled(
            "event-firehose",
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" · "),
        Span::raw(app.source.clone()),
        Span::raw(" · "),
        Span::styled(status, status_style),
        Span::raw(format!(" · {} events · {:.1}/s", app.lines, app.rate)),
    ])
}

fn counters(app: &App) -> Vec<Line<'static>> {
    let counts = &app.counts;
    let family = Line::from(vec![
        count("mint", counts.mint, Color::LightGreen),
        separator(),
        count("transfer", counts.transfer, Color::Reset),
        separator(),
        count("burn", counts.burn, Color::LightRed),
        separator(),
        count("market", counts.market, Color::LightCyan),
        separator(),
        count("asset", counts.asset, Color::LightMagenta),
        separator(),
        count("admin", counts.admin, Color::LightYellow),
    ]);

    let mut second = format!(
        "accounts {} · moved {} · slots {}{} · coverage {}",
        app.accounts.len(),
        app.moved,
        app.slot_range(),
        direction_arrow(app),
        app.coverage_label()
    );
    if app.notable > 0 {
        second.push_str(&format!(" · notable {}", app.notable));
    }
    if app.malformed > 0 {
        second.push_str(&format!(" · malformed {}", app.malformed));
    }
    if app.blocks.out_of_order > 0 {
        second.push_str(&format!(" · out of order {}", app.blocks.out_of_order));
    }

    vec![family, Line::raw(second)]
}

fn count(label: &'static str, value: u64, colour: Color) -> Span<'static> {
    Span::styled(
        format!("{label} {value}"),
        Style::default().fg(colour).add_modifier(Modifier::BOLD),
    )
}

fn separator() -> Span<'static> {
    Span::styled(
        " · ",
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::DIM),
    )
}

fn direction_arrow(app: &App) -> &'static str {
    match app.blocks.order() {
        Some(Order::Ascending) => " ↑",
        Some(Order::Descending) => " ↓",
        None => "",
    }
}

fn sparkline(frame: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    let data = app.block_counts();
    // `Sparkline` draws the FIRST `width` items. The window holds more buckets than a
    // narrow terminal can show and the newest are at the end, so the oldest are dropped
    // here — otherwise the bars that matter are the ones cut off.
    let width = usize::from(area.width.saturating_sub(2));
    let visible = if width > 0 && data.len() > width {
        &data[data.len() - width..]
    } else {
        &data[..]
    };
    let title = match (app.blocks.order(), visible.is_empty()) {
        (_, true) => "events / 1k slots".to_owned(),
        (Some(Order::Ascending), false) => "events / 1k slots · chain order".to_owned(),
        (Some(Order::Descending), false) => "events / 1k slots · newest first".to_owned(),
        (None, false) => "events / 1k slots".to_owned(),
    };
    frame.render_widget(
        Sparkline::default()
            .data(visible)
            .style(Style::default().fg(Color::Cyan))
            .block(Block::bordered().title(title)),
        area,
    );
}

fn feed(frame: &mut Frame, app: &App, area: ratatui::layout::Rect) {
    if app.feed.is_empty() {
        // An empty screen is usually a walk that resumed past its cap and emitted nothing,
        // not a broken viewer. Say so rather than leaving a blank panel.
        let (message, style) = if app.closed {
            (
                "the stream closed before any event. A walk that resumed past --max-blocks \
                 emits nothing: use a fresh --db (or --fresh). Otherwise check --from-slot \
                 and the policy id."
                    .to_owned(),
                Style::default().fg(Color::Yellow),
            )
        } else {
            (
                "waiting for events…".to_owned(),
                Style::default().fg(Color::DarkGray),
            )
        };
        frame.render_widget(
            Paragraph::new(message)
                .style(style)
                .wrap(Wrap { trim: true })
                .block(Block::bordered().title("feed")),
            area,
        );
        return;
    }

    // The deque is newest-arrival first. For an ascending stream that is newest chain
    // first; a descending stream arrives newest chain first, so its deque is reversed to
    // put the newest row at the top too.
    let rows: Vec<TableRow<'static>> = match app.blocks.order() {
        Some(Order::Descending) => app.feed.iter().rev().map(feed_row).collect(),
        _ => app.feed.iter().map(feed_row).collect(),
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(9),
            Constraint::Length(4),
            Constraint::Length(16),
            Constraint::Min(10),
        ],
    )
    .header(
        TableRow::new(["slot", "time", "kind", "qty", "asset", "movement"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::bordered().title(format!(
        "feed · newest chain first · {}/{}",
        app.feed.len(),
        app.capacity
    )));
    frame.render_widget(table, area);
}

fn feed_row(row: &Row) -> TableRow<'static> {
    let mut movement = row.movement.clone();
    if let Some(reason) = row.notable {
        movement.push_str(" · ");
        movement.push_str(reason);
    }

    TableRow::new(vec![
        Cell::from(
            row.slot
                .map(|slot| slot.to_string())
                .unwrap_or_else(|| "—".to_owned()),
        ),
        Cell::from(utc_hms(row.time)),
        Cell::from(row.kind.clone()),
        Cell::from(
            row.quantity
                .map(|quantity| quantity.to_string())
                .unwrap_or_default(),
        ),
        Cell::from(row.asset.clone()),
        Cell::from(movement),
    ])
    .style(row_style(row))
}

fn row_style(row: &Row) -> Style {
    match row.notable {
        Some("unattributed") => Style::default().fg(Color::LightRed),
        Some(_) => Style::default().fg(Color::Yellow),
        None => Style::default().fg(kind_colour(&row.kind)),
    }
}

fn kind_colour(kind: &str) -> Color {
    match kind {
        "mint" => Color::LightGreen,
        "burn" => Color::LightRed,
        "transfer" => Color::Reset,
        "placed" | "updated" | "reopened" | "filled" | "cancelled" | "invalidated" => {
            Color::LightCyan
        }
        _ => Color::Reset,
    }
}

fn footer(app: &App) -> Line<'static> {
    let mut spans = vec![
        Span::styled(
            "q",
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        ),
        Span::raw(" quit · "),
        Span::styled(
            "p",
            Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
        ),
        Span::raw(" pause"),
    ];

    if let Some((account, delta)) = app.top_movers(1).first() {
        spans.push(Span::raw(format!(" · top {} {delta:+}", short(account))));
    }
    if let Some(check) = &app.notable_last {
        spans.push(Span::styled(
            format!(" · last notable: {check}"),
            Style::default().fg(Color::Yellow),
        ));
    }
    if let Some(line) = &app.malformed_last {
        spans.push(Span::styled(
            format!(" · malformed: {line}"),
            Style::default().fg(Color::LightRed),
        ));
    }
    if let Some(exit) = &app.exit {
        spans.push(Span::styled(
            format!(" · {exit}"),
            Style::default().fg(Color::DarkGray),
        ));
    }

    Line::from(spans)
}

/// A compact count line, used by the snapshot writer's summary.
pub fn summary(counts: &Counts) -> String {
    format!(
        "mint {} · transfer {} · burn {} · market {} · asset {} · admin {}",
        counts.mint, counts.transfer, counts.burn, counts.market, counts.asset, counts.admin
    )
}

/// A chain time as a UTC clock, which is what the chain itself reports.
fn utc_hms(unix: u64) -> String {
    let seconds = unix % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3_600,
        (seconds % 3_600) / 60,
        seconds % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_events::Coverage;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn row(slot: u64, kind: &'static str, quantity: u64, movement: &str) -> Row {
        Row {
            slot: Some(slot),
            time: 1_762_532_187,
            kind: kind.to_owned(),
            quantity: Some(quantity),
            asset: "Perp1969".to_owned(),
            movement: movement.to_owned(),
            notable: None,
        }
    }

    fn app() -> App {
        let mut app = App::new("perps4.ndjson", 50);
        app.lines = 3;
        app.rate = 12.5;
        app.counts = Counts {
            mint: 2,
            transfer: 1,
            ..Counts::default()
        };
        app.coverage = Some(Coverage::Complete);
        app.min_slot = Some(170_445_182);
        app.max_slot = Some(170_445_190);
        app.feed
            .push_back(row(170_445_182, "mint", 1, "-> addr1q..q0e4"));
        app.feed.push_front(row(
            170_445_190,
            "transfer",
            1,
            "addr1q..aaaa -> addr1q..bbbb",
        ));
        app
    }

    fn buffer_text(terminal: &Terminal<TestBackend>) -> String {
        let buffer = terminal.backend().buffer();
        let area = buffer.area;
        let mut text = String::new();
        for y in area.y..area.y + area.height {
            for x in area.x..area.x + area.width {
                text.push_str(buffer[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    fn draw_at(app: &App, width: u16, height: u16) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| super::draw(frame, app)).unwrap();
        buffer_text(&terminal)
    }

    #[test]
    fn the_feed_shows_both_directions_and_the_counters() {
        let text = draw_at(&app(), 120, 36);

        assert!(text.contains("event-firehose"));
        assert!(text.contains("perps4.ndjson"));
        assert!(text.contains("events / 1k slots"));
        assert!(text.contains("mint 2"));
        assert!(text.contains("transfer 1"));
        assert!(text.contains("170445182"));
        assert!(text.contains("16:16:27"));
        assert!(text.contains("addr1q..aaaa -> addr1q..bbbb"));
        assert!(text.contains("Perp1969"));
        assert!(text.contains("q quit"));
    }

    #[test]
    fn the_sparkline_drops_the_oldest_buckets_not_the_newest() {
        use crate::app::fixture::holder;
        use chain_events::HolderKind;

        let mut app = App::new("test", 10);
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 100));
        // A gap wider than the window leaves quiet buckets and one active block, newest
        // last. The widget takes the FIRST `width` items, so this is the case where the
        // only bar would be cut off if the viewer sliced the wrong end.
        app.push(holder(HolderKind::Mint, &[("addr1va", 1)], 400_000));

        assert_eq!(app.block_counts().last(), Some(&1));
        let text = draw_at(&app, 80, 24);
        assert!(
            text.contains('█'),
            "the active bucket must be on screen:\n{text}"
        );
    }

    #[test]
    fn an_empty_closed_stream_explains_itself() {
        let mut app = App::new("test", 10);
        app.close();

        let text = draw_at(&app, 120, 36);
        assert!(text.contains("closed before any event"));
        assert!(text.contains("--max-blocks"));
    }

    #[test]
    fn a_closed_stream_says_so() {
        let mut app = app();
        app.closed = true;
        assert!(draw_at(&app, 120, 36).contains("closed"));
    }

    #[test]
    fn a_paused_feed_says_so() {
        let mut app = app();
        app.paused = true;
        assert!(draw_at(&app, 120, 36).contains("paused"));
    }

    #[test]
    fn the_screen_survives_a_narrow_terminal() {
        // The feed columns cannot all fit at 80; the table clips rather than wrapping.
        let text = draw_at(&app(), 80, 24);
        assert!(text.contains("feed · newest chain first"));
        assert!(text.contains("q quit"));
    }
}
