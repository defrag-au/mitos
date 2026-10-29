//! Policy → immutable-file window: the arithmetic, with no I/O and no renderer.
//!
//! The one thing to understand here is that a Mithril chunk is not a token's
//! slice of the chain. An immutable file is 21,600 slots (six hours, four a day)
//! holding every transaction in that window, and the aggregator will only serve
//! a contiguous `ImmutableFileRange`. A policy therefore resolves to a range:
//! the chunk before its first mint, through the tip or a chosen end.
//!
//! The floor deliberately under-shoots by one chunk. That is the rule
//! `token-ledger`'s probe already follows, and the asymmetry is why: a floor
//! too low costs one chunk of reading, while a floor too high truncates the
//! archive and lets it report itself complete, because completeness is measured
//! against that same number.

use std::fmt;
use std::path::PathBuf;

use mitos_chain_walk::mithril::immutable_file_for_slot;
use mitos_koios::PolicyAsset;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A policy id: exactly 28 bytes, written as 56 hex characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyId([u8; 28]);

impl PolicyId {
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let text = text.strip_prefix("0x").unwrap_or(text);
        if text.len() != 56 {
            return Err(format!(
                "a policy id is 56 hex characters, this one is {}",
                text.len()
            ));
        }
        let mut bytes = [0u8; 28];
        hex::decode_to_slice(text, &mut bytes)
            .map_err(|_| "a policy id is hex only (0-9, a-f)".to_string())?;
        Ok(Self(bytes))
    }

    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for PolicyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

impl Serialize for PolicyId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for PolicyId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        PolicyId::parse(&text).map_err(serde::de::Error::custom)
    }
}

/// What Koios knows about the policy's first mint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyFloor {
    /// How many assets Koios returned. A policy can mint assets for years, so
    /// this is the whole set, not the assets that existed at the floor.
    pub assets: usize,
    /// The earliest `creation_time` across those assets, in unix seconds.
    pub earliest_unix: i64,
    pub earliest_asset: String,
    pub minting_tx: Option<String>,
    /// The slot `earliest_unix` converts to. Mainnet Shelley slots are one
    /// second, so this is the mint block's slot.
    pub slot: u64,
    /// The chunk that slot falls in, without the safety margin. Shown so the
    /// margin is visible rather than mysterious.
    pub exact_chunk: u64,
    /// The first chunk to fetch: one before `exact_chunk`.
    pub floor_chunk: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum FloorError {
    #[error("Koios knows no assets for this policy — is it a policy id, and has it minted?")]
    NoAssets,
    #[error(
        "Koios knows this policy's assets but none has a creation time — the first mint cannot \
         be placed, and guessing the tip would be worse than stopping"
    )]
    NoDatableAsset,
}

/// The floor rule, in one place.
///
/// ⚠️ The earliest asset, never `assets[0]`. Koios promises no order, and a
/// policy mints new assets throughout its life; taking the first row once put
/// RIDDLE's floor 273,059 slots too high.
pub fn floor_from_assets(assets: &[PolicyAsset]) -> Result<PolicyFloor, FloorError> {
    if assets.is_empty() {
        return Err(FloorError::NoAssets);
    }
    let earliest_unix = mitos_koios::floor_unix(assets).ok_or(FloorError::NoDatableAsset)?;
    let earliest = assets
        .iter()
        .find(|asset| asset.creation_time == Some(earliest_unix))
        .expect("floor_unix returned a minimum from this slice");

    let slot = token_ledger_wire::sample::unix_to_slot(earliest_unix);
    let exact_chunk = immutable_file_for_slot(slot);

    Ok(PolicyFloor {
        assets: assets.len(),
        earliest_unix,
        earliest_asset: earliest
            .asset_name
            .clone()
            .unwrap_or_else(|| "(unnamed)".to_string()),
        minting_tx: earliest.minting_tx_hash.clone(),
        slot,
        exact_chunk,
        floor_chunk: exact_chunk.saturating_sub(1),
    })
}

/// The newest signed snapshot, as the aggregator describes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tip {
    pub beacon: u64,
    pub epoch: u64,
    pub created_at: String,
    /// The snapshot's uncompressed database size. It includes ancillary files
    /// and omits `00000`, so the per-file figure below is an estimate — stated
    /// as one in the UI rather than passed off as a measurement.
    pub total_bytes: u64,
}

impl Tip {
    pub fn per_file_bytes(&self) -> u64 {
        self.total_bytes.checked_div(self.beacon).unwrap_or(0)
    }
}

/// The chunks a policy needs, and what fetching them is expected to cost.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePlan {
    pub policy: PolicyId,
    pub floor: PolicyFloor,
    pub destination: PathBuf,
    pub latest_beacon: u64,
    pub latest_epoch: u64,
    pub snapshot_created: String,
    pub per_file_bytes: u64,
    pub from: u64,
    pub to: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error(
        "the policy's first chunk is {from:05}, after the newest signed chunk {to:05} — the \
         aggregator has no chunk for it"
    )]
    Empty { from: u64, to: u64 },
    #[error("the end chunk {end:05} is before the first chunk to fetch ({from:05})")]
    EndBeforeStart { end: u64, from: u64 },
    #[error("the end chunk {end:05} is after the newest signed chunk ({tip:05})")]
    EndAfterTip { end: u64, tip: u64 },
}

impl RangePlan {
    pub fn new(
        policy: PolicyId,
        floor: PolicyFloor,
        destination: PathBuf,
        tip: Tip,
        requested_end: Option<u64>,
    ) -> Result<Self, PlanError> {
        // Immutable file numbering starts at 1 on mainnet.
        let from = floor.floor_chunk.max(1);
        if let Some(end) = requested_end {
            if end > tip.beacon {
                return Err(PlanError::EndAfterTip {
                    end,
                    tip: tip.beacon,
                });
            }
            if end < from {
                return Err(PlanError::EndBeforeStart { end, from });
            }
        }
        let to = requested_end.unwrap_or(tip.beacon);
        if from > to {
            return Err(PlanError::Empty { from, to });
        }

        let per_file_bytes = tip.per_file_bytes();
        Ok(Self {
            policy,
            floor,
            destination,
            latest_beacon: tip.beacon,
            latest_epoch: tip.epoch,
            snapshot_created: tip.created_at,
            per_file_bytes,
            from,
            to,
        })
    }

    /// The same plan with a different last chunk.
    pub fn with_end(&self, end: u64) -> Result<Self, PlanError> {
        if end < self.from {
            return Err(PlanError::EndBeforeStart {
                end,
                from: self.from,
            });
        }
        if end > self.latest_beacon {
            return Err(PlanError::EndAfterTip {
                end,
                tip: self.latest_beacon,
            });
        }

        let mut plan = self.clone();
        plan.to = end;
        Ok(plan)
    }

    /// Apply the CLI's optional bounds: an end slot, or the first N chunks.
    ///
    /// Both are expressed in the terms the caller has before a plan exists — a
    /// slot, or a file count — and become an end chunk here, so `--run` and the
    /// TUI agree on what they mean.
    pub fn bounded(mut self, to_slot: Option<u64>, chunks: Option<u64>) -> Result<Self, PlanError> {
        if let Some(slot) = to_slot {
            self = self.with_end(end_chunk_for_slot(slot, self.latest_beacon))?;
        }
        if let Some(first) = chunks {
            let end = self
                .from
                .saturating_add(first.saturating_sub(1))
                .min(self.latest_beacon);
            self = self.with_end(end)?;
        }
        Ok(self)
    }

    pub fn files(&self) -> u64 {
        self.to - self.from + 1
    }

    pub fn estimated_bytes(&self) -> u64 {
        self.files().saturating_mul(self.per_file_bytes)
    }
}

/// The chunk a slot falls in, clamped to what the aggregator can serve.
pub fn end_chunk_for_slot(slot: u64, latest_beacon: u64) -> u64 {
    immutable_file_for_slot(slot).min(latest_beacon)
}

/// The command a policy archive walk starts with once the chunks are here.
pub fn next_command(plan: &RangePlan) -> String {
    format!(
        "token-ledger reverse --archive-dir {} --token {} --data-dir {} --first-mint {}",
        plan.destination.join("archives").display(),
        plan.policy,
        plan.destination.display(),
        plan.floor.slot
    )
}

/// The same command split at its arguments, so a narrow frame keeps the
/// continuation visibly attached to it.
pub fn next_command_lines(plan: &RangePlan) -> Vec<String> {
    vec![
        format!(
            "token-ledger reverse --archive-dir {} \\",
            plan.destination.join("archives").display()
        ),
        format!("  --token {} \\", plan.policy),
        format!(
            "  --data-dir {} --first-mint {}",
            plan.destination.display(),
            plan.floor.slot
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitos_chain_walk::mithril::CHUNK_SLOTS;

    fn asset(name: &str, created: Option<i64>, tx: Option<&str>) -> PolicyAsset {
        PolicyAsset {
            asset_name: Some(name.to_string()),
            fingerprint: None,
            minting_tx_hash: tx.map(str::to_string),
            total_supply: None,
            mint_cnt: None,
            burn_cnt: None,
            creation_time: created,
        }
    }

    fn tip(beacon: u64) -> Tip {
        Tip {
            beacon,
            epoch: 500,
            created_at: "2026-10-29T00:00:00Z".to_string(),
            total_bytes: 232_439_984_455,
        }
    }

    #[test]
    fn a_policy_id_is_28_bytes_of_hex() {
        let text = "16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7";
        let policy = PolicyId::parse(text).unwrap();
        assert_eq!(policy.to_hex(), text);
        assert_eq!(PolicyId::parse(&format!("0x{text}")).unwrap(), policy);
        assert_eq!(
            PolicyId::parse(text.to_uppercase().as_str()).unwrap(),
            policy
        );
    }

    #[test]
    fn a_short_or_non_hex_id_is_refused_with_its_own_reason() {
        assert!(
            PolicyId::parse("16657df3")
                .unwrap_err()
                .contains("56 hex characters")
        );
        let wrong = "z".repeat(56);
        assert!(PolicyId::parse(&wrong).unwrap_err().contains("hex only"));
    }

    /// The rule the module exists for: the earliest asset, never the first row.
    #[test]
    fn the_floor_is_the_earliest_asset_and_one_chunk_below_it() {
        let assets = [
            asset("late", Some(1_700_000_000), Some("aa")),
            asset("first", Some(1_600_000_000), Some("bb")),
        ];
        let floor = floor_from_assets(&assets).unwrap();
        assert_eq!(floor.earliest_unix, 1_600_000_000);
        assert_eq!(floor.earliest_asset, "first");
        assert_eq!(floor.minting_tx.as_deref(), Some("bb"));

        let expected_slot = token_ledger_wire::sample::unix_to_slot(1_600_000_000);
        assert_eq!(floor.slot, expected_slot);
        assert_eq!(floor.exact_chunk, expected_slot / CHUNK_SLOTS);
        assert_eq!(floor.floor_chunk, floor.exact_chunk - 1);
    }

    #[test]
    fn a_policy_with_nothing_datable_has_no_floor() {
        assert!(matches!(floor_from_assets(&[]), Err(FloorError::NoAssets)));
        assert!(matches!(
            floor_from_assets(&[asset("a", None, None)]),
            Err(FloorError::NoDatableAsset)
        ));
    }

    #[test]
    fn the_floor_never_underflows_at_genesis() {
        let floor = floor_from_assets(&[asset("first", Some(1_506_203_091), None)]).unwrap();
        assert_eq!(floor.exact_chunk, 0);
        assert_eq!(floor.floor_chunk, 0);
    }

    #[test]
    fn a_plan_defaults_to_the_tip_and_clamps_an_earlier_end() {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = floor_from_assets(&[asset("x", Some(1_600_000_000), None)]).unwrap();
        let plan = RangePlan::new(
            policy,
            floor.clone(),
            PathBuf::from("/store"),
            tip(9_400),
            None,
        )
        .unwrap();
        assert_eq!(plan.to, 9_400);
        assert_eq!(plan.files(), 9_400 - plan.from + 1);

        let shorter = plan.with_end(plan.from + 9).unwrap();
        assert_eq!(shorter.files(), 10);
        assert!(matches!(
            plan.with_end(plan.from - 1),
            Err(PlanError::EndBeforeStart { .. })
        ));
        assert!(matches!(
            plan.with_end(9_401),
            Err(PlanError::EndAfterTip { .. })
        ));
    }

    #[test]
    fn a_floor_above_the_tip_is_an_error_not_an_empty_download() {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = PolicyFloor {
            assets: 1,
            earliest_unix: 1_700_000_000,
            earliest_asset: "x".to_string(),
            minting_tx: None,
            slot: 9_500 * CHUNK_SLOTS,
            exact_chunk: 9_500,
            floor_chunk: 9_499,
        };
        let error =
            RangePlan::new(policy, floor, PathBuf::from("/store"), tip(9_400), None).unwrap_err();
        assert!(matches!(error, PlanError::Empty { .. }));
    }

    #[test]
    fn a_slot_maps_to_its_chunk_and_never_past_the_beacon() {
        assert_eq!(end_chunk_for_slot(0, 9_400), 0);
        assert_eq!(end_chunk_for_slot(21_599, 9_400), 0);
        assert_eq!(end_chunk_for_slot(21_600, 9_400), 1);
        assert_eq!(end_chunk_for_slot(u64::MAX, 9_400), 9_400);
    }

    #[test]
    fn the_cli_bounds_become_an_end_chunk() {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = PolicyFloor {
            assets: 1,
            earliest_unix: 1_600_000_000,
            earliest_asset: "x".to_string(),
            minting_tx: None,
            slot: 4_000_000,
            exact_chunk: 185,
            floor_chunk: 184,
        };
        let plan =
            RangePlan::new(policy, floor, PathBuf::from("/store"), tip(9_400), None).unwrap();

        assert_eq!(plan.clone().bounded(None, Some(3)).unwrap().to, 186);
        assert_eq!(plan.clone().bounded(None, Some(100_000)).unwrap().to, 9_400);
        assert_eq!(
            plan.clone()
                .bounded(Some(200 * CHUNK_SLOTS), None)
                .unwrap()
                .to,
            200
        );
        assert_eq!(plan.clone().bounded(None, None).unwrap().to, 9_400);
    }

    #[test]
    fn a_plan_estimate_is_files_times_the_snapshot_average() {
        let policy =
            PolicyId::parse("16657df32ad8eaa8f8c628586ac6b8ba3771226c12bd69b582738fb7").unwrap();
        let floor = PolicyFloor {
            assets: 1,
            earliest_unix: 1_600_000_000,
            earliest_asset: "x".to_string(),
            minting_tx: None,
            slot: 1_500_000_000,
            exact_chunk: 100,
            floor_chunk: 99,
        };
        let plan = RangePlan::new(
            policy,
            floor,
            PathBuf::from("/store"),
            tip(9_202),
            Some(199),
        )
        .unwrap();
        assert_eq!(plan.files(), 101);
        // 232,439,984,455 / 9,202 = 25,259,724 bytes per file.
        assert_eq!(plan.per_file_bytes, 25_259_724);
        assert_eq!(plan.estimated_bytes(), 101 * 25_259_724);
    }
}
