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
    pub fn started(plan: &RangePlan) -> Self {
        Self {
            started_unix: fmt::now_unix(),
            files_total: plan.files(),
            bytes_expected: plan.estimated_bytes(),
            ..Self::default()
        }
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

    pub fn eta_secs(&self) -> Option<u64> {
        let rate = self.rate();
        if rate <= 0.0 {
            return None;
        }
        let remaining = self.bytes_expected.saturating_sub(self.received());
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
            | Event::Done { generation, .. } => *generation,
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
                _ => Action::None,
            },
            Screen::Failed { policy, retry, .. } => match code {
                KeyCode::Char('r') if *retry => Action::Retry(*policy),
                KeyCode::Esc => Action::Back,
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
            Event::Progress { progress, .. } => {
                if let Screen::Fetching { fetch, .. } = &mut self.screen {
                    fetch.apply(progress);
                }
            }
            Event::RangeDone {
                range,
                files,
                bytes,
                ..
            } => {
                if let Screen::Fetching { fetch, .. } = &mut self.screen {
                    fetch.finish_batch(files, bytes);
                    fetch.ranges.push(range);
                }
            }
            Event::FetchFailed { message, .. } => {
                let policy = match &self.screen {
                    Screen::Fetching { plan, .. } => plan.policy,
                    _ => return,
                };
                self.cancel = None;
                self.screen = Screen::Failed {
                    policy,
                    message,
                    retry: true,
                };
            }
            Event::Done { outcome, .. } => {
                let plan = match &self.screen {
                    Screen::Fetching { plan, .. } => plan.clone(),
                    _ => return,
                };
                self.cancel = None;
                self.screen = Screen::Done { plan, outcome };
            }
        }
    }

    /// How long until the heartbeat should repaint, if anything is moving.
    pub fn repaint_interval(&self) -> Option<Duration> {
        match self.screen {
            Screen::Resolving { .. } | Screen::Fetching { .. } => Some(Duration::from_millis(100)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = "16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7";

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
}
