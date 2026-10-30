//! The state machine the renderer draws and the workers feed.
//!
//! It is pure: keys go in and [`Action`]s come out, worker events mutate the
//! screen, and nothing here opens a socket, reads a chunk or writes a file. The
//! harness can therefore replay a fixture without a network or a terminal.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use crossterm::event::{KeyCode, KeyModifiers};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::coverage::{LocalCoverage, PublishedCoverage};
use crate::fmt;
use crate::manifest::FetchedRange;
use crate::plan::{PolicyFloor, PolicyId, RangePlan, Tip};

/// One step of the download the library reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchProgress {
    Started { total: u64 },
    FileStarted { number: u64, size: u64 },
    FileProgress { number: u64, downloaded: u64 },
    FileCompleted { number: u64 },
    Completed,
}

/// Accumulated download state. One batch at a time, with the finished batches
/// folded into `files_done` / `bytes_done`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FetchState {
    pub started_unix: i64,
    pub files_total: u64,
    pub bytes_expected: u64,
    pub files_done: u64,
    pub bytes_done: u64,
    pub batch_total: u64,
    pub batch_completed: u64,
    #[serde(with = "number_keyed")]
    pub batch_sizes: BTreeMap<u64, u64>,
    #[serde(with = "number_keyed")]
    pub batch_inflight: BTreeMap<u64, u64>,
    pub current: Vec<u64>,
    pub ranges: Vec<FetchedRange>,
}

impl FetchState {
    pub fn expected(files_total: u64, bytes_expected: u64) -> Self {
        Self {
            started_unix: fmt::now_unix(),
            files_total,
            bytes_expected,
            ..Self::default()
        }
    }

    pub fn started(plan: &RangePlan) -> Self {
        Self::expected(plan.files(), plan.estimated_bytes())
    }

    pub fn apply(&mut self, progress: FetchProgress) {
        match progress {
            FetchProgress::Started { total } => {
                self.batch_total = total;
                self.batch_completed = 0;
                self.batch_sizes.clear();
                self.batch_inflight.clear();
                self.current.clear();
            }
            FetchProgress::FileStarted { number, size } => {
                self.batch_sizes.insert(number, size);
                self.batch_inflight.insert(number, 0);
                if !self.current.contains(&number) {
                    self.current.push(number);
                    self.current.sort_unstable();
                }
            }
            FetchProgress::FileProgress { number, downloaded } => {
                self.batch_inflight.insert(number, downloaded);
            }
            FetchProgress::FileCompleted { number } => {
                self.batch_inflight.remove(&number);
                self.batch_completed += 1;
                self.current.retain(|candidate| *candidate != number);
            }
            FetchProgress::Completed => {}
        }
    }

    pub fn finish_batch(&mut self, files: u64, bytes: u64) {
        self.files_done += files;
        self.bytes_done += bytes;
        self.batch_total = 0;
        self.batch_completed = 0;
        self.batch_sizes.clear();
        self.batch_inflight.clear();
        self.current.clear();
    }

    /// Bytes received in the batch in flight: a finished archive counts at its
    /// full size, an unfinished one at what the library last reported.
    pub fn batch_received(&self) -> u64 {
        self.batch_sizes
            .iter()
            .map(|(number, size)| match self.batch_inflight.get(number) {
                Some(downloaded) => *downloaded,
                None => *size,
            })
            .sum()
    }

    pub fn received(&self) -> u64 {
        self.bytes_done + self.batch_received()
    }

    pub fn percent(&self) -> u16 {
        if self.files_total == 0 {
            return 0;
        }
        let done = self.files_done + self.batch_completed;
        (done.saturating_mul(100) / self.files_total).min(100) as u16
    }

    pub fn elapsed_secs(&self) -> u64 {
        fmt::now_unix().saturating_sub(self.started_unix).max(0) as u64
    }

    pub fn rate(&self) -> f64 {
        let elapsed = self.elapsed_secs();
        if elapsed == 0 {
            0.0
        } else {
            self.received() as f64 / elapsed as f64
        }
    }

    /// The bytes estimate, corrected by the sizes of the batch in flight.
    ///
    /// A plan estimates `files × the snapshot's average`, and a small range can
    /// beat that: 06203 is 53 MiB where the average is 24 MiB, so an uncorrected
    /// screen read "53.4 MiB of ≈ 48.4 MiB" and reported an ETA of zero. The
    /// library names each file's real size when its download starts, so once a
    /// batch is in flight the total can be rebuilt from what is known plus the
    /// average for what is not.
    pub fn projected_bytes(&self) -> u64 {
        if self.files_total == 0 {
            return self.bytes_expected;
        }
        let average = self.bytes_expected / self.files_total;
        let known: u64 = self.batch_sizes.values().sum();
        let counted = self.files_done + self.batch_sizes.len() as u64;
        let unknown = self.files_total.saturating_sub(counted);
        (self.bytes_done + known + unknown.saturating_mul(average)).max(self.bytes_expected)
    }

    pub fn eta_secs(&self) -> Option<u64> {
        let rate = self.rate();
        if rate <= 0.0 {
            return None;
        }
        let remaining = self.projected_bytes().saturating_sub(self.received());
        Some((remaining as f64 / rate) as u64)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Outcome {
    pub ranges: Vec<FetchedRange>,
    pub files: u64,
    pub bytes: u64,
    pub already_present: u64,
    pub elapsed_secs: u64,
}

/// How gently a backfill runs. Defaults are the slow profile: leave the link
/// and the disk idle between small bursts rather than take the whole machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillSettings {
    pub parallel: usize,
    pub batch_files: u64,
    pub pause_secs: u64,
}

impl Default for BackfillSettings {
    fn default() -> Self {
        Self {
            parallel: 4,
            batch_files: 20,
            pause_secs: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillRun {
    pub fetch: FetchState,
    pub phase: String,
    pub settings: BackfillSettings,
}

impl BackfillRun {
    pub fn new(settings: BackfillSettings, files: u64, bytes: u64) -> Self {
        Self {
            fetch: FetchState::expected(files, bytes),
            phase: "starting…".to_string(),
            settings,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoverageView {
    /// The directory scan, available before (and without) the aggregator.
    pub local: Option<LocalCoverage>,
    /// The local scan joined to Mithril's published set.
    pub published: Option<PublishedCoverage>,
    pub status: String,
    pub error: Option<String>,
    pub fill: Option<BackfillRun>,
    pub settings: BackfillSettings,
}

/// A backfill the UI asked for.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillSpec {
    pub destination: PathBuf,
    pub from: u64,
    pub to: u64,
    pub settings: BackfillSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "screen", rename_all = "snake_case")]
pub enum Screen {
    Policy {
        input: String,
        error: Option<String>,
    },
    Resolving {
        policy: PolicyId,
        status: String,
    },
    Plan {
        plan: RangePlan,
        end_input: String,
        error: Option<String>,
    },
    Fetching {
        plan: RangePlan,
        phase: String,
        fetch: FetchState,
    },
    Done {
        plan: RangePlan,
        outcome: Outcome,
    },
    Failed {
        policy: PolicyId,
        message: String,
        retry: bool,
    },
    Coverage {
        view: CoverageView,
    },
}

impl Screen {
    pub fn title(&self) -> &'static str {
        match self {
            Screen::Policy { .. } => "policy",
            Screen::Resolving { .. } => "resolving",
            Screen::Plan { .. } => "plan",
            Screen::Fetching { .. } => "fetching",
            Screen::Done { .. } => "done",
            Screen::Failed { .. } => "failed",
            Screen::Coverage { .. } => "coverage",
        }
    }
}

/// Immutable file numbers as JSON object keys.
///
/// `serde_json` writes an integer key as a string but will not read one back as
/// an integer, so a fixture captured from a running screen would fail to load
/// without this. Only the fixture and report paths use it.
mod number_keyed {
    use std::collections::BTreeMap;

    use serde::de::Error as _;
    use serde::ser::SerializeMap;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<u64, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut out = serializer.serialize_map(Some(map.len()))?;
        for (number, value) in map {
            out.serialize_entry(&number.to_string(), value)?;
        }
        out.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<u64, u64>, D::Error> {
        let raw = BTreeMap::<String, u64>::deserialize(deserializer)?;
        raw.into_iter()
            .map(|(number, value)| {
                number
                    .parse::<u64>()
                    .map(|number| (number, value))
                    .map_err(D::Error::custom)
            })
            .collect()
    }
}

/// What a worker tells the screen.
#[derive(Debug, Clone)]
pub enum Event {
    Status {
        generation: u64,
        message: String,
    },
    Resolved {
        generation: u64,
        floor: PolicyFloor,
        tip: Tip,
    },
    ResolveFailed {
        generation: u64,
        message: String,
    },
    Progress {
        generation: u64,
        progress: FetchProgress,
    },
    RangeDone {
        generation: u64,
        range: FetchedRange,
        files: u64,
        bytes: u64,
    },
    FetchFailed {
        generation: u64,
        message: String,
    },
    Done {
        generation: u64,
        outcome: Outcome,
    },
    CoverageLocal {
        generation: u64,
        local: LocalCoverage,
    },
    CoveragePublished {
        generation: u64,
        published: PublishedCoverage,
    },
    CoverageFailed {
        generation: u64,
        message: String,
    },
}

impl Event {
    pub fn generation(&self) -> u64 {
        match self {
            Event::Status { generation, .. }
            | Event::Resolved { generation, .. }
            | Event::ResolveFailed { generation, .. }
            | Event::Progress { generation, .. }
            | Event::RangeDone { generation, .. }
            | Event::FetchFailed { generation, .. }
            | Event::Done { generation, .. }
            | Event::CoverageLocal { generation, .. }
            | Event::CoveragePublished { generation, .. }
            | Event::CoverageFailed { generation, .. } => *generation,
        }
    }
}

/// What the main loop should do after a key.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    None,
    Quit,
    Resolve(PolicyId),
    Start(RangePlan),
    Cancel,
    Back,
    Retry(PolicyId),
    Coverage,
    Backfill(BackfillSpec),
}

/// The CLI's bounds, applied once a resolved plan exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct PlanBound {
    pub to_slot: Option<u64>,
    pub chunks: Option<u64>,
}

pub struct App {
    pub screen: Screen,
    pub destination: Option<PathBuf>,
    pub bound: PlanBound,
    pub backfill: BackfillSettings,
    /// The CLI's bounds on the TUI's backfill, for a probe or a staged fill.
    pub backfill_from: Option<u64>,
    pub backfill_to: Option<u64>,
    /// Bumped whenever the screen stops caring about a worker's result. An
    /// event from an older generation is ignored, so a cancelled download
    /// cannot repaint a new plan.
    pub generation: u64,
    pub tick: u64,
    pub quit: bool,
    /// A fixture is a still frame: keys may leave it, never start work.
    pub fixture: bool,
    pub cancel: Option<oneshot::Sender<()>>,
}

impl App {
    pub fn new(destination: Option<PathBuf>, screen: Screen) -> Self {
        Self {
            screen,
            destination,
            bound: PlanBound::default(),
            backfill: BackfillSettings::default(),
            backfill_from: None,
            backfill_to: None,
            generation: 0,
            tick: 0,
            quit: false,
            fixture: false,
            cancel: None,
        }
    }

    pub fn fixture(screen: Screen) -> Self {
        let mut app = Self::new(None, screen);
        app.fixture = true;
        app
    }

    pub fn next_generation(&mut self) -> u64 {
        self.generation += 1;
        self.cancel = None;
        self.generation
    }

    pub fn take_cancel(&mut self) -> Option<oneshot::Sender<()>> {
        self.cancel.take()
    }

    pub fn back(&mut self) {
        let policy = match &self.screen {
            Screen::Policy { .. } => None,
            Screen::Resolving { policy, .. } => Some(*policy),
            Screen::Plan { plan, .. } => Some(plan.policy),
            Screen::Fetching { plan, .. } => Some(plan.policy),
            Screen::Done { plan, .. } => Some(plan.policy),
            Screen::Failed { policy, .. } => Some(*policy),
            Screen::Coverage { .. } => None,
        };
        self.generation += 1;
        self.cancel = None;
        self.screen = Screen::Policy {
            input: policy.map(PolicyId::to_hex).unwrap_or_default(),
            error: None,
        };
    }

    pub fn on_key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> Action {
        if modifiers.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('c') {
            return Action::Quit;
        }

        if self.fixture {
            return match code {
                KeyCode::Char('q') | KeyCode::Esc => Action::Quit,
                _ => Action::None,
            };
        }

        // Read before the screen is borrowed: the coverage arm needs them.
        let destination = self.destination.clone();
        let backfill_from = self.backfill_from;
        let backfill_to = self.backfill_to;

        match &mut self.screen {
            Screen::Policy { input, error } => match code {
                KeyCode::Char(character)
                    if (character.is_ascii_hexdigit() || character == 'x') && input.len() < 58 =>
                {
                    input.push(character);
                    *error = None;
                    Action::None
                }
                KeyCode::Backspace => {
                    input.pop();
                    *error = None;
                    Action::None
                }
                KeyCode::Esc => {
                    input.clear();
                    *error = None;
                    Action::None
                }
                KeyCode::Enter => match PolicyId::parse(input) {
                    Ok(policy) => Action::Resolve(policy),
                    Err(message) => {
                        *error = Some(message);
                        Action::None
                    }
                },
                KeyCode::Char('v') => Action::Coverage,
                _ => Action::None,
            },
            Screen::Resolving { .. } => match code {
                KeyCode::Esc => Action::Back,
                KeyCode::Char('q') => Action::Quit,
                _ => Action::None,
            },
            Screen::Plan {
                plan,
                end_input,
                error,
            } => match code {
                KeyCode::Char(character) if character.is_ascii_digit() && end_input.len() < 7 => {
                    end_input.push(character);
                    *error = None;
                    Action::None
                }
                KeyCode::Backspace => {
                    end_input.pop();
                    *error = None;
                    Action::None
                }
                KeyCode::Char('t') => {
                    *end_input = plan.latest_beacon.to_string();
                    *error = None;
                    Action::None
                }
                KeyCode::Enter => match end_input.parse::<u64>() {
                    Ok(end) => match plan.with_end(end) {
                        Ok(started) => Action::Start(started),
                        Err(problem) => {
                            *error = Some(problem.to_string());
                            Action::None
                        }
                    },
                    Err(_) => {
                        *error = Some(format!("`{end_input}` is not an immutable file number"));
                        Action::None
                    }
                },
                KeyCode::Esc => Action::Back,
                KeyCode::Char('q') => Action::Quit,
                KeyCode::Char('v') => Action::Coverage,
                _ => Action::None,
            },
            Screen::Fetching { .. } => match code {
                KeyCode::Esc => Action::Cancel,
                KeyCode::Char('q') => Action::Quit,
                _ => Action::None,
            },
            Screen::Done { .. } => match code {
                KeyCode::Char('q') => Action::Quit,
                KeyCode::Esc => Action::Back,
                KeyCode::Char('v') => Action::Coverage,
                _ => Action::None,
            },
            Screen::Failed { policy, retry, .. } => match code {
                KeyCode::Char('r') if *retry => Action::Retry(*policy),
                KeyCode::Esc => Action::Back,
                KeyCode::Char('q') => Action::Quit,
                KeyCode::Char('v') => Action::Coverage,
                _ => Action::None,
            },
            Screen::Coverage { view } => match code {
                KeyCode::Char('b') if view.fill.is_none() && view.published.is_some() => {
                    let Some(published) = &view.published else {
                        return Action::None;
                    };
                    let Some(destination) = destination else {
                        return Action::None;
                    };
                    Action::Backfill(BackfillSpec {
                        destination,
                        from: backfill_from.unwrap_or(1),
                        to: backfill_to
                            .unwrap_or(published.beacon)
                            .min(published.beacon),
                        settings: view.settings,
                    })
                }
                KeyCode::Esc if view.fill.is_some() => Action::Cancel,
                KeyCode::Esc => Action::Back,
                KeyCode::Char('r') => Action::Coverage,
                KeyCode::Char('q') => Action::Quit,
                _ => Action::None,
            },
        }
    }

    pub fn apply(&mut self, event: Event) {
        if event.generation() != self.generation {
            return;
        }

        match event {
            Event::Status { message, .. } => match &mut self.screen {
                Screen::Resolving { status, .. } => *status = message,
                Screen::Fetching { phase, .. } => *phase = message,
                Screen::Coverage { view } => {
                    if let Some(fill) = &mut view.fill {
                        fill.phase = message.clone();
                    }
                    view.status = message;
                }
                _ => {}
            },
            Event::Resolved { floor, tip, .. } => {
                let policy = match &self.screen {
                    Screen::Resolving { policy, .. } => *policy,
                    _ => return,
                };
                let Some(destination) = self.destination.clone() else {
                    self.screen = Screen::Failed {
                        policy,
                        message: "no destination — set MITHRIL_DATA_DIR or pass --dest".to_string(),
                        retry: false,
                    };
                    return;
                };
                match RangePlan::new(policy, floor, destination, tip, None)
                    .and_then(|plan| plan.bounded(self.bound.to_slot, self.bound.chunks))
                {
                    Ok(plan) => {
                        let end_input = plan.to.to_string();
                        self.screen = Screen::Plan {
                            plan,
                            end_input,
                            error: None,
                        };
                    }
                    Err(error) => {
                        self.screen = Screen::Failed {
                            policy,
                            message: error.to_string(),
                            retry: true,
                        };
                    }
                }
            }
            Event::ResolveFailed { message, .. } => {
                let policy = match &self.screen {
                    Screen::Resolving { policy, .. } => *policy,
                    _ => return,
                };
                self.screen = Screen::Failed {
                    policy,
                    message,
                    retry: true,
                };
            }
            Event::Progress { progress, .. } => match &mut self.screen {
                Screen::Fetching { fetch, .. } => fetch.apply(progress),
                Screen::Coverage { view } => {
                    if let Some(fill) = &mut view.fill {
                        fill.fetch.apply(progress);
                    }
                }
                _ => {}
            },
            Event::RangeDone {
                range,
                files,
                bytes,
                ..
            } => match &mut self.screen {
                Screen::Fetching { fetch, .. } => {
                    fetch.finish_batch(files, bytes);
                    fetch.ranges.push(range);
                }
                Screen::Coverage { view } => {
                    if let Some(fill) = &mut view.fill {
                        fill.fetch.finish_batch(files, bytes);
                        fill.fetch.ranges.push(range);
                    }
                }
                _ => {}
            },
            Event::FetchFailed { message, .. } => {
                self.cancel = None;
                let policy = match &self.screen {
                    Screen::Fetching { plan, .. } => Some(plan.policy),
                    _ => None,
                };
                if let Some(policy) = policy {
                    self.screen = Screen::Failed {
                        policy,
                        message,
                        retry: true,
                    };
                } else if let Screen::Coverage { view } = &mut self.screen {
                    view.fill = None;
                    view.error = Some(message);
                    view.status = "backfill stopped — r rescans, b resumes".to_string();
                }
            }
            Event::Done { outcome, .. } => {
                self.cancel = None;
                let plan = match &self.screen {
                    Screen::Fetching { plan, .. } => Some(plan.clone()),
                    _ => None,
                };
                if let Some(plan) = plan {
                    self.screen = Screen::Done { plan, outcome };
                } else if let Screen::Coverage { view } = &mut self.screen {
                    view.fill = None;
                    view.status = "backfill complete — reading the store again…".to_string();
                }
            }
            Event::CoverageLocal { local, .. } => {
                if let Screen::Coverage { view } = &mut self.screen {
                    view.local = Some(local);
                    // Every scan ends with a `CoveragePublished` that joins this
                    // local set. Keeping the previous join up in the meantime
                    // shows a Covered count that disagrees with the Local line
                    // beside it — a fresh scan with a stale verdict on top.
                    view.published = None;
                }
            }
            Event::CoveragePublished { published, .. } => {
                if let Screen::Coverage { view } = &mut self.screen {
                    view.published = Some(published);
                    view.status.clear();
                }
            }
            Event::CoverageFailed { message, .. } => {
                if let Screen::Coverage { view } = &mut self.screen {
                    view.error = Some(message);
                    view.status =
                        "the local scan is shown; the published set could not be read".to_string();
                }
            }
        }
    }

    /// How long until the heartbeat should repaint, if anything is moving.
    pub fn repaint_interval(&self) -> Option<Duration> {
        match &self.screen {
            Screen::Resolving { .. } | Screen::Fetching { .. } => Some(Duration::from_millis(100)),
            Screen::Coverage { view } if view.fill.is_some() => Some(Duration::from_millis(100)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coverage::ChunkRange;

    const POLICY: &str = "16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7";

    fn local_coverage() -> LocalCoverage {
        LocalCoverage {
            files: 2,
            bytes: 100,
            first: Some(6201),
            last: Some(6202),
            incomplete: Vec::new(),
            policies: Vec::new(),
            policy_errors: Vec::new(),
            complete: [6201, 6202].into_iter().collect(),
        }
    }

    fn published_coverage() -> PublishedCoverage {
        PublishedCoverage {
            beacon: 6204,
            epoch: 500,
            created_at: "now".to_string(),
            per_file_bytes: 50,
            published_files: 4,
            covered_files: 2,
            missing_files: 2,
            missing_bytes: 100,
            unpublished_files: 0,
            gaps: vec![ChunkRange {
                from: 6203,
                to: 6204,
            }],
            gap_count: 1,
        }
    }

    fn plan() -> RangePlan {
        let policy = PolicyId::parse(POLICY).unwrap();
        let floor = PolicyFloor {
            assets: 2,
            earliest_unix: 1_600_000_000,
            earliest_asset: "x".to_string(),
            minting_tx: None,
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
                total_bytes: 1_000,
            },
            None,
        )
        .unwrap()
    }

    #[test]
    fn typing_a_policy_and_enter_resolves_it() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Policy {
                input: String::new(),
                error: None,
            },
        );
        for character in POLICY.chars() {
            assert_eq!(
                app.on_key(KeyCode::Char(character), KeyModifiers::NONE),
                Action::None
            );
        }
        assert_eq!(
            app.on_key(KeyCode::Enter, KeyModifiers::NONE),
            Action::Resolve(PolicyId::parse(POLICY).unwrap())
        );
    }

    #[test]
    fn a_bad_policy_stays_on_the_screen_with_a_reason() {
        let mut app = App::new(
            None,
            Screen::Policy {
                input: "nothex".to_string(),
                error: None,
            },
        );
        assert_eq!(app.on_key(KeyCode::Enter, KeyModifiers::NONE), Action::None);
        match &app.screen {
            Screen::Policy { error, .. } => assert!(error.as_deref().unwrap().contains("hex")),
            other => panic!("stayed on {other:?}"),
        }
    }

    #[test]
    fn the_plan_screen_edits_the_end_chunk_and_starts() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Plan {
                plan: plan(),
                end_input: "9400".to_string(),
                error: None,
            },
        );
        for _ in 0..4 {
            app.on_key(KeyCode::Backspace, KeyModifiers::NONE);
        }
        for character in "0200".chars() {
            app.on_key(KeyCode::Char(character), KeyModifiers::NONE);
        }
        match app.on_key(KeyCode::Enter, KeyModifiers::NONE) {
            Action::Start(started) => assert_eq!(started.to, 200),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn an_end_before_the_floor_is_refused() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Plan {
                plan: plan(),
                end_input: "10".to_string(),
                error: None,
            },
        );
        assert_eq!(app.on_key(KeyCode::Enter, KeyModifiers::NONE), Action::None);
        match &app.screen {
            Screen::Plan { error, .. } => {
                assert!(error.as_deref().unwrap().contains("before the first chunk"))
            }
            other => panic!("stayed on {other:?}"),
        }
    }

    #[test]
    fn a_stale_worker_event_cannot_repaint_a_new_screen() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Resolving {
                policy: PolicyId::parse(POLICY).unwrap(),
                status: "asking".to_string(),
            },
        );
        app.back();
        app.apply(Event::ResolveFailed {
            generation: 0,
            message: "stale".to_string(),
        });
        assert!(matches!(app.screen, Screen::Policy { .. }));
    }

    #[test]
    fn fetch_state_round_trips_with_numeric_keys() {
        let plan = plan();
        let mut state = FetchState::started(&plan);
        state.apply(FetchProgress::FileStarted {
            number: 184,
            size: 1_000,
        });
        let json = serde_json::to_vec(&state).unwrap();
        let loaded: FetchState = serde_json::from_slice(&json).unwrap();
        assert_eq!(loaded.batch_sizes.get(&184), Some(&1_000));
    }

    #[test]
    fn progress_accumulates_files_and_bytes_across_batches() {
        let plan = plan();
        let mut state = FetchState::started(&plan);
        state.apply(FetchProgress::Started { total: 2 });
        state.apply(FetchProgress::FileStarted {
            number: 184,
            size: 1_000,
        });
        state.apply(FetchProgress::FileProgress {
            number: 184,
            downloaded: 400,
        });
        assert_eq!(state.batch_received(), 400);
        state.apply(FetchProgress::FileCompleted { number: 184 });
        state.apply(FetchProgress::FileStarted {
            number: 185,
            size: 1_000,
        });
        // 184 counts at its full size now that it is complete; 185 has not
        // reported a byte yet.
        assert_eq!(state.received(), 1_000);
        state.finish_batch(2, 2_000);
        assert_eq!(state.files_done, 2);
        assert_eq!(state.bytes_done, 2_000);
        assert_eq!(state.percent(), 0);
    }

    #[test]
    fn a_batch_that_beats_the_average_corrects_the_estimate() {
        let mut state = FetchState::expected(2, 48);
        state.apply(FetchProgress::Started { total: 1 });
        state.apply(FetchProgress::FileStarted {
            number: 6203,
            size: 53,
        });
        // 53 for the file in flight, 24 (the snapshot average) for the next.
        assert_eq!(state.projected_bytes(), 77);

        state.apply(FetchProgress::FileCompleted { number: 6203 });
        state.finish_batch(1, 53);
        state.apply(FetchProgress::Started { total: 1 });
        state.apply(FetchProgress::FileStarted {
            number: 6204,
            size: 37,
        });
        assert_eq!(state.projected_bytes(), 90);
        // 6204 has been announced but has not reported a byte yet, which is
        // exactly when the projection matters: the estimate already includes it.
        assert_eq!(state.received(), 53);
    }

    #[test]
    fn v_opens_coverage_from_the_policy_screen() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Policy {
                input: String::new(),
                error: None,
            },
        );
        assert_eq!(
            app.on_key(KeyCode::Char('v'), KeyModifiers::NONE),
            Action::Coverage
        );
    }

    #[test]
    fn coverage_starts_a_backfill_over_the_published_horizon() {
        let view = CoverageView {
            local: Some(local_coverage()),
            published: Some(published_coverage()),
            settings: BackfillSettings {
                parallel: 2,
                batch_files: 5,
                pause_secs: 1,
            },
            ..CoverageView::default()
        };
        let mut app = App::new(Some(PathBuf::from("/store")), Screen::Coverage { view });

        match app.on_key(KeyCode::Char('b'), KeyModifiers::NONE) {
            Action::Backfill(spec) => {
                assert_eq!(spec.from, 1);
                assert_eq!(spec.to, 6204);
                assert_eq!(spec.settings.batch_files, 5);
                assert_eq!(spec.destination, PathBuf::from("/store"));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn coverage_esc_stops_a_running_backfill_before_leaving() {
        let mut view = CoverageView {
            published: Some(published_coverage()),
            ..CoverageView::default()
        };
        view.fill = Some(BackfillRun::new(BackfillSettings::default(), 2, 100));
        let mut app = App::new(Some(PathBuf::from("/store")), Screen::Coverage { view });
        assert_eq!(app.on_key(KeyCode::Esc, KeyModifiers::NONE), Action::Cancel);

        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Coverage {
                view: CoverageView {
                    published: Some(published_coverage()),
                    ..CoverageView::default()
                },
            },
        );
        assert_eq!(app.on_key(KeyCode::Esc, KeyModifiers::NONE), Action::Back);
    }

    #[test]
    fn a_coverage_scan_updates_both_halves() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Coverage {
                view: CoverageView::default(),
            },
        );
        app.apply(Event::CoverageLocal {
            generation: 0,
            local: local_coverage(),
        });
        app.apply(Event::CoveragePublished {
            generation: 0,
            published: published_coverage(),
        });

        match &app.screen {
            Screen::Coverage { view } => {
                assert_eq!(view.local.as_ref().unwrap().files, 2);
                assert_eq!(view.published.as_ref().unwrap().beacon, 6204);
                assert!(view.status.is_empty());
            }
            other => panic!("stayed on {other:?}"),
        }
    }

    #[test]
    fn a_coverage_rescan_drops_the_stale_published_join() {
        let mut app = App::new(
            Some(PathBuf::from("/store")),
            Screen::Coverage {
                view: CoverageView {
                    local: Some(local_coverage()),
                    published: Some(published_coverage()),
                    ..CoverageView::default()
                },
            },
        );

        app.apply(Event::CoverageLocal {
            generation: 0,
            local: local_coverage(),
        });

        match &app.screen {
            Screen::Coverage { view } => {
                assert_eq!(view.local.as_ref().unwrap().files, 2);
                assert!(
                    view.published.is_none(),
                    "a new local scan must not sit beside the previous join"
                );
            }
            other => panic!("left coverage for {other:?}"),
        }
    }

    #[test]
    fn a_finished_backfill_keeps_the_coverage_screen() {
        let mut view = CoverageView {
            published: Some(published_coverage()),
            ..CoverageView::default()
        };
        view.fill = Some(BackfillRun::new(BackfillSettings::default(), 2, 100));
        let mut app = App::new(Some(PathBuf::from("/store")), Screen::Coverage { view });

        app.apply(Event::Done {
            generation: 0,
            outcome: Outcome::default(),
        });

        match &app.screen {
            Screen::Coverage { view } => {
                assert!(view.fill.is_none());
                assert!(view.status.contains("backfill complete"));
            }
            other => panic!("left coverage for {other:?}"),
        }
    }

    #[test]
    fn a_failed_backfill_stays_on_coverage_with_the_error() {
        let mut view = CoverageView {
            published: Some(published_coverage()),
            ..CoverageView::default()
        };
        view.fill = Some(BackfillRun::new(BackfillSettings::default(), 2, 100));
        let mut app = App::new(Some(PathBuf::from("/store")), Screen::Coverage { view });

        app.apply(Event::FetchFailed {
            generation: 0,
            message: "offline".to_string(),
        });

        match &app.screen {
            Screen::Coverage { view } => {
                assert!(view.fill.is_none());
                assert_eq!(view.error.as_deref(), Some("offline"));
            }
            other => panic!("left coverage for {other:?}"),
        }
    }
}
