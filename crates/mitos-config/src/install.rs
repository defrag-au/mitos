//! Persisted install progress. `bootstrap` writes it; `doctor` reads it.
//!
//! `.bootstrapped` is a SEPARATE marker, written last. This file can say
//! `Bootstrapped` a moment before the marker appears; recover treats that
//! as "write the marker, do not re-import".

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum InstallPhase {
    Fetching,
    Verifying,
    Importing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum InstallState {
    NotStarted,
    Fetching,
    Verifying,
    Importing { cursor: u64 },
    Bootstrapped { tip_slot: u64 },
    Failed { phase: InstallPhase, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverAction {
    Start,
    ResumeFetch,
    ResumeVerify,
    ResumeImport { cursor: u64 },
    WriteMarker { tip_slot: u64 },
    Retry { phase: InstallPhase },
}

impl InstallState {
    /// What a process that died in this persisted state should do next.
    /// The observed disk (plan()) still wins: recover never says "fetch"
    /// if the store already has chunks — that is plan()'s job.
    pub fn recover(&self) -> RecoverAction {
        match self {
            Self::NotStarted => RecoverAction::Start,
            Self::Fetching => RecoverAction::ResumeFetch,
            Self::Verifying => RecoverAction::ResumeVerify,
            Self::Importing { cursor } => RecoverAction::ResumeImport { cursor: *cursor },
            Self::Bootstrapped { tip_slot } => RecoverAction::WriteMarker {
                tip_slot: *tip_slot,
            },
            Self::Failed { phase, .. } => RecoverAction::Retry { phase: *phase },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{InstallPhase, InstallState, RecoverAction};

    fn round_trip(state: &InstallState) -> InstallState {
        let text = serde_json::to_string(state).unwrap();
        serde_json::from_str(&text).unwrap()
    }

    #[test]
    fn recover_from_every_persisted_stage() {
        let cases: &[(InstallState, RecoverAction)] = &[
            (InstallState::NotStarted, RecoverAction::Start),
            (InstallState::Fetching, RecoverAction::ResumeFetch),
            (InstallState::Verifying, RecoverAction::ResumeVerify),
            (
                InstallState::Importing { cursor: 42 },
                RecoverAction::ResumeImport { cursor: 42 },
            ),
            (
                InstallState::Bootstrapped { tip_slot: 99 },
                RecoverAction::WriteMarker { tip_slot: 99 },
            ),
            (
                InstallState::Failed {
                    phase: InstallPhase::Fetching,
                    reason: "disk".into(),
                },
                RecoverAction::Retry {
                    phase: InstallPhase::Fetching,
                },
            ),
        ];
        for (state, want) in cases {
            let loaded = round_trip(state);
            assert_eq!(&loaded, state, "serialised form must round-trip");
            assert_eq!(loaded.recover(), *want);
        }
    }
}
