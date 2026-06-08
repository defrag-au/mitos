//! Dynamic interest helpers — companion side.
//!
//! Per the design doc (`MITOS_COMPANION_RUNTIME_V1.md`, "Dynamic
//! interest mechanics"), the **canonical interest set** for a
//! companion lives in its DO SQLite. The host-side module's
//! filter set is a replica of what the companion most recently
//! asserted; the companion wins on any divergence.
//!
//! The on-disk model is a simplified `(kind, value, channel)`
//! tuple shape — one row per atomic interest. The wire model is
//! `Vec<mitos_protocol::Interest>` (richer typed enum). This
//! module owns both the storage layer and the
//! row-row-to-wire-Interest translation.
//!
//! For v1 we support `kind = "policy"` only. v2 will broaden to
//! `address`, `asset`, etc. as the dApp surface demands.

use cardano_assets::PolicyId;
use mitos_protocol::{AssetSelector, Interest};
use serde::{Deserialize, Serialize};
use worker::SqlStorage;

use crate::ctx::SqlStorageValue;
use crate::error::{CompanionError, Result};

/// Supported interest kinds. Strings on the wire so v2 additions
/// don't require a schema migration; companions silently skip
/// kinds they don't understand on read.
pub mod kinds {
    pub const POLICY: &str = "policy";
    /// Raw output-address watch (bech32) → `Interest::at_address`,
    /// bridged host-side to the data-plane `AtAddress` predicate.
    /// For modules that watch addresses (e.g. `credit-address`).
    pub const ADDRESS: &str = "address";
}

/// Empty-string channel marker — used when the dApp is single-
/// channel or the interest applies across all channels.
pub const NO_CHANNEL: &str = "";

/// One row in `mitos_companion_interest`. Used by the RPC
/// handlers and as the building block for translating to the
/// wire-format `mitos_protocol::Interest` enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterestRow {
    pub kind: String,
    pub value: String,
    /// Channel this interest is scoped to. Empty string means
    /// "all channels"; non-empty matches the channel name on the
    /// host's WS Hibernation tag (used by multi-channel companions
    /// to keep per-channel interest sets isolated).
    #[serde(default)]
    pub channel: String,
    /// RFC3339 timestamp the row was first added. Diagnostic only.
    pub added_at: String,
}

/// Insert a single interest row. Idempotent — re-adding the same
/// `(kind, value, channel)` tuple is a no-op (matches the
/// PRIMARY KEY constraint).
pub fn add_interest(
    sql: &SqlStorage,
    kind: &str,
    value: &str,
    channel: &str,
    added_at_rfc3339: &str,
) -> Result<()> {
    sql.exec(
        "INSERT OR IGNORE INTO mitos_companion_interest \
         (kind, value, channel, added_at) VALUES (?, ?, ?, ?)",
        Some(vec![
            SqlStorageValue::from(kind),
            SqlStorageValue::from(value),
            SqlStorageValue::from(channel),
            SqlStorageValue::from(added_at_rfc3339),
        ]),
    )
    .map_err(|e| CompanionError::Storage(format!("add_interest: {e}")))?;
    Ok(())
}

/// Remove a single interest row. No-op if the row doesn't exist.
pub fn remove_interest(sql: &SqlStorage, kind: &str, value: &str, channel: &str) -> Result<()> {
    sql.exec(
        "DELETE FROM mitos_companion_interest \
         WHERE kind = ? AND value = ? AND channel = ?",
        Some(vec![
            SqlStorageValue::from(kind),
            SqlStorageValue::from(value),
            SqlStorageValue::from(channel),
        ]),
    )
    .map_err(|e| CompanionError::Storage(format!("remove_interest: {e}")))?;
    Ok(())
}

/// Read every row in `mitos_companion_interest`. Used on WS
/// reconnect (full Replace) and by the `GET /api/_interest`
/// handler.
pub fn list_interests(sql: &SqlStorage) -> Result<Vec<InterestRow>> {
    let cursor = sql
        .exec(
            "SELECT kind, value, channel, added_at FROM mitos_companion_interest \
             ORDER BY kind, value, channel",
            None,
        )
        .map_err(|e| CompanionError::Storage(format!("list_interests: {e}")))?;

    let mut out = Vec::new();
    for row in cursor.next::<InterestRow>() {
        let row = row.map_err(|e| CompanionError::Storage(format!("decode interest row: {e}")))?;
        out.push(row);
    }
    Ok(out)
}

/// Translate a slice of stored rows into wire-format
/// `mitos_protocol::Interest` values. v1 supports `kind = "policy"`
/// only — other kinds are skipped with a warn log.
///
/// The host's `update-interest` WIT call accepts a CBOR-encoded
/// `Vec<Interest>`; this helper produces that Vec across all
/// channels. For multi-channel companions, prefer
/// [`rows_to_interests_for_channel`] which filters rows whose
/// `channel` matches the WS connection's Hibernation tag.
pub fn rows_to_interests(rows: &[InterestRow]) -> Vec<Interest> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        match row.kind.as_str() {
            kinds::POLICY => match PolicyId::new(row.value.clone()) {
                Ok(p) => {
                    let mut interest = Interest::any();
                    interest.asset = AssetSelector::Policy(p);
                    out.push(interest);
                }
                Err(e) => {
                    tracing::warn!(
                        kind = %row.kind,
                        value = %row.value,
                        error = %e,
                        "skipping invalid policy interest row",
                    );
                }
            },
            kinds::ADDRESS => {
                // A raw output-address watch — no parsing (the address is an opaque
                // bech32 string the data-plane matches verbatim). Empty values are
                // dropped so a malformed row can't widen the watch.
                if row.value.is_empty() {
                    tracing::warn!("skipping empty address interest row");
                } else {
                    out.push(Interest::at_address(row.value.clone()));
                }
            }
            other => {
                tracing::warn!(
                    kind = %other,
                    value = %row.value,
                    "unsupported interest kind; supported: `policy`, `address`",
                );
            }
        }
    }
    out
}

/// Channel-scoped variant of [`rows_to_interests`]. Filters rows
/// whose `channel` field is either:
///
/// - **equal to `target_channel`** — exact match for the WS this
///   frame is being sent over, or
/// - **empty (`NO_CHANNEL`)** — the row applies to every channel
///   the companion holds (single-channel default + cross-channel
///   interests).
///
/// Rows with a non-matching, non-empty channel are skipped.
/// Multi-channel companions use this when broadcasting
/// `ClientMessage::Interest` frames so the ownership channel's
/// interests don't bleed into the marketplace channel's filter
/// set, etc.
pub fn rows_to_interests_for_channel(rows: &[InterestRow], target_channel: &str) -> Vec<Interest> {
    let filtered: Vec<&InterestRow> = rows
        .iter()
        .filter(|r| r.channel == target_channel || r.channel == NO_CHANNEL)
        .collect();
    let owned: Vec<InterestRow> = filtered.into_iter().cloned().collect();
    rows_to_interests(&owned)
}

// ============================================================================
// RPC request/response shapes
// ============================================================================

/// Body for `POST /api/_interest/subscribe` and
/// `POST /api/_interest/unsubscribe`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestMutateRequest {
    pub kind: String,
    pub value: String,
    /// Channel scope. `None` / missing → empty string (all channels).
    #[serde(default)]
    pub channel: Option<String>,
}

/// Body for `GET /api/_interest`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestListResponse {
    pub interests: Vec<InterestRow>,
}

/// Reply shape for the mutate endpoints. `op_result = "added"` /
/// `"removed"` / `"noop"` distinguishes the cases for clients
/// that care.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InterestMutateResponse {
    pub op_result: String,
    pub kind: String,
    pub value: String,
    pub channel: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_to_interests_translates_policy_rows() {
        let rows = vec![
            InterestRow {
                kind: kinds::POLICY.to_string(),
                value: "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6".to_string(),
                channel: NO_CHANNEL.to_string(),
                added_at: "2026-05-05T00:00:00Z".to_string(),
            },
            InterestRow {
                kind: kinds::POLICY.to_string(),
                value: "793aca910dc6a400ced6c94698c6f01d6479d701227fc9a7287ae2a5".to_string(),
                channel: NO_CHANNEL.to_string(),
                added_at: "2026-05-05T00:00:00Z".to_string(),
            },
        ];
        let interests = rows_to_interests(&rows);
        assert_eq!(interests.len(), 2);
        for i in &interests {
            assert!(matches!(i.asset, AssetSelector::Policy(_)));
        }
    }

    #[test]
    fn rows_to_interests_skips_unknown_kinds() {
        let rows = vec![InterestRow {
            kind: "stake-key".into(), // genuinely unsupported kind
            value: "stake1...".into(),
            channel: "".into(),
            added_at: "2026-05-05T00:00:00Z".into(),
        }];
        let interests = rows_to_interests(&rows);
        assert!(interests.is_empty());
    }

    #[test]
    fn rows_to_interests_translates_address() {
        let rows = vec![InterestRow {
            kind: kinds::ADDRESS.into(),
            value: "addr1watched".into(),
            channel: "".into(),
            added_at: "2026-05-05T00:00:00Z".into(),
        }];
        let interests = rows_to_interests(&rows);
        assert_eq!(interests.len(), 1);
        assert_eq!(
            interests[0].domain,
            mitos_protocol::DomainSelector::AtAddress("addr1watched".into())
        );
    }

    #[test]
    fn rows_to_interests_skips_invalid_policy_hex() {
        let rows = vec![InterestRow {
            kind: kinds::POLICY.into(),
            value: "not-hex".into(),
            channel: "".into(),
            added_at: "2026-05-05T00:00:00Z".into(),
        }];
        let interests = rows_to_interests(&rows);
        assert!(interests.is_empty());
    }

    // ====================================================================
    // Multi-channel scoping
    // ====================================================================

    fn make_policy_row(value: &str, channel: &str) -> InterestRow {
        InterestRow {
            kind: kinds::POLICY.into(),
            value: value.into(),
            channel: channel.into(),
            added_at: "2026-05-05T00:00:00Z".into(),
        }
    }

    #[test]
    fn for_channel_includes_exact_match_and_global() {
        // Three rows: one ownership-scoped, one marketplace-
        // scoped, one global (NO_CHANNEL). When sending the
        // ownership channel's WS frame, we want ownership +
        // global rows; the marketplace row stays out.
        let rows = vec![
            make_policy_row(
                "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6",
                "ownership",
            ),
            make_policy_row(
                "793aca910dc6a400ced6c94698c6f01d6479d701227fc9a7287ae2a5",
                "marketplace",
            ),
            make_policy_row(
                "aaaa11111111111111111111111111111111111111111111111111aa",
                NO_CHANNEL,
            ),
        ];
        let owned_for_ownership = rows_to_interests_for_channel(&rows, "ownership");
        // 1 ownership-scoped + 1 global = 2.
        assert_eq!(owned_for_ownership.len(), 2);

        let owned_for_marketplace = rows_to_interests_for_channel(&rows, "marketplace");
        // 1 marketplace-scoped + 1 global = 2.
        assert_eq!(owned_for_marketplace.len(), 2);
    }

    #[test]
    fn for_channel_excludes_other_channels() {
        let rows = vec![
            make_policy_row(
                "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6",
                "ownership",
            ),
            make_policy_row(
                "793aca910dc6a400ced6c94698c6f01d6479d701227fc9a7287ae2a5",
                "marketplace",
            ),
        ];
        // No global rows — each channel sees only its own.
        let owned_for_ownership = rows_to_interests_for_channel(&rows, "ownership");
        assert_eq!(owned_for_ownership.len(), 1);
        let owned_for_marketplace = rows_to_interests_for_channel(&rows, "marketplace");
        assert_eq!(owned_for_marketplace.len(), 1);
    }

    #[test]
    fn for_channel_with_empty_target_matches_only_global_rows() {
        // Calling for_channel("") with mixed rows: only NO_CHANNEL
        // rows return. Channel-scoped rows are skipped because
        // their channel doesn't match the empty target.
        let rows = vec![
            make_policy_row(
                "b3dab69f7e6100849434fb1781e34bd12a916557f6231b8d2629b6f6",
                "ownership",
            ),
            make_policy_row(
                "aaaa11111111111111111111111111111111111111111111111111aa",
                NO_CHANNEL,
            ),
        ];
        let global_only = rows_to_interests_for_channel(&rows, "");
        assert_eq!(global_only.len(), 1);
    }
}
