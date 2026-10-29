//! The in-process frame renderer: a fixture screen, a `TestBackend`, no terminal.
//!
//! `tui-shot` owns a PTY and is the review for keys and resizes. This is the
//! cheap half: it draws a captured screen at a chosen size for microseconds, so
//! a layout can be read before a network call exists. `--fixture <file>` also
//! enters the real TUI read-only, which is how `tui-shot` captures a screen
//! that would otherwise need Koios and the aggregator.

use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result, bail};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use serde::{Deserialize, Serialize};

use crate::app::Screen;

/// A frame size in columns x rows, the spelling `tui-shot` uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Size {
    pub cols: u16,
    pub rows: u16,
}

impl Size {
    pub fn new(cols: u16, rows: u16) -> Result<Self, String> {
        if cols == 0 || rows == 0 {
            return Err(format!(
                "{cols}x{rows} has no {}",
                if cols == 0 { "columns" } else { "rows" }
            ));
        }
        if cols > 500 || rows > 200 {
            return Err(format!("{cols}x{rows} is larger than 500x200"));
        }
        Ok(Self { cols, rows })
    }
}

impl fmt::Display for Size {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}x{}", self.cols, self.rows)
    }
}

impl FromStr for Size {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let (cols, rows) = text
            .split_once('x')
            .ok_or_else(|| format!("`{text}` is not a size (columns x rows, such as 120x40)"))?;
        let cols: u16 = cols
            .parse()
            .map_err(|_| format!("`{text}` is not a size (columns x rows, such as 120x40)"))?;
        let rows: u16 = rows
            .parse()
            .map_err(|_| format!("`{text}` is not a size (columns x rows, such as 120x40)"))?;
        Self::new(cols, rows).map_err(|error| format!("`{text}`: {error}"))
    }
}

#[derive(Debug, Serialize)]
pub struct Report<'a> {
    pub size: Size,
    pub screen: &'a Screen,
    pub text: String,
}

pub fn load(path: &Path) -> Result<Screen> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))
}

/// Draw `screen` at `size` and return the frame as plain text, trailing spaces
/// trimmed per line.
pub fn render(screen: &Screen, size: Size) -> Result<String> {
    let backend = TestBackend::new(size.cols, size.rows);
    let mut terminal = Terminal::new(backend).context("creating the test terminal")?;
    terminal
        .draw(|frame| crate::render::draw(frame, screen, 0))
        .context("drawing the frame")?;

    let buffer = terminal.backend().buffer();
    Ok((0..size.rows)
        .map(|y| {
            (0..size.cols)
                .map(|x| buffer[(x, y)].symbol())
                .collect::<String>()
                .trim_end()
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Write `<stem>.txt` (the frame) and `<stem>.json` (the frame, size and screen).
pub fn capture(screen: &Screen, size: Size, stem: &Path) -> Result<()> {
    if stem.as_os_str().is_empty() {
        bail!("--capture needs a stem");
    }
    if let Some(parent) = stem
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }

    let text = render(screen, size)?;
    let text_path = stem.with_extension("txt");
    fs::write(&text_path, &text).with_context(|| format!("writing {}", text_path.display()))?;

    let report = Report { size, screen, text };
    let json_path = stem.with_extension("json");
    let json = serde_json::to_vec_pretty(&report).context("serialising the frame report")?;
    fs::write(&json_path, json).with_context(|| format!("writing {}", json_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_are_columns_by_rows_and_are_bounded() {
        assert_eq!(
            "120x40".parse::<Size>().unwrap(),
            Size {
                cols: 120,
                rows: 40
            }
        );
        assert!("120".parse::<Size>().is_err());
        assert!("0x40".parse::<Size>().is_err());
        assert!("501x40".parse::<Size>().is_err());
    }

    #[test]
    fn a_policy_fixture_renders_to_text() {
        let screen = Screen::Policy {
            input: "16657df".to_string(),
            error: None,
        };
        let text = render(
            &screen,
            Size {
                cols: 100,
                rows: 30,
            },
        )
        .unwrap();
        assert!(text.contains("mithril-fetch · policy"));
        assert!(text.contains("16657df"));
    }

    #[test]
    fn a_screen_round_trips_through_the_fixture_format() {
        let screen = Screen::Failed {
            policy: crate::plan::PolicyId::parse(
                "16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7",
            )
            .unwrap(),
            message: "offline".to_string(),
            retry: true,
        };
        let json = serde_json::to_vec(&screen).unwrap();
        let loaded: Screen = serde_json::from_slice(&json).unwrap();
        assert!(matches!(loaded, Screen::Failed { retry: true, .. }));
    }
}
