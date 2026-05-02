//! Collection ownership indexer: per-policy NFT ownership tracking.
//!
//! This is the first migration target for the CF replication
//! prototype, mirroring `cnft.dev-workers/workers/collection-ownership/`.
//! See `docs/design/CF_REPLICATION.md` Phase 4.5 for the build order.
//!
//! Current state (post Phase 4 trait surgery):
//! - `Scope = Vec<Interest>` — consumers express interest using the
//!   shared `mitos_protocol::Interest` vocabulary; the indexer
//!   projects the asset axis (Domain/Value axes are inert here —
//!   ownership produces state changes, not protocol events).
//! - The indexer maintains an in-memory `watch_set: HashSet<PolicyId>`
//!   derived from the union of `AssetSelector::Policy/Asset/Trait`
//!   policies across all live subscriptions. An interest with
//!   `AssetSelector::Any` or `AssetSelector::Fingerprint` (which
//!   doesn't constrain by policy) collapses the watch set to "scan
//!   everything" — `watch_set` becomes `None`.
//! - Backfill: each new subscribe drives `utxos_by_policy` lookups
//!   for the distinct policies referenced by the subscriber's
//!   interests, synthesising `OwnershipChange::Transfer` records.
//!   Subscribers using `Any`/`Fingerprint` selectors get no
//!   backfill (would require enumerating every policy on chain) —
//!   they live-tail only.

use std::collections::HashSet;

use async_trait::async_trait;
use axum::Router;
use cardano_assets::PolicyId;
use dolos_cardano::indexes::CardanoIndexExt;
use dolos_core::{ChainPoint, Domain, StateStore, TipEvent};
use mitos_core::{Emitter, Indexer, SubscribeReply};
use mitos_protocol::{Interest, any_interest_matches_asset, watched_policies};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Single ownership change record.
///
/// Phase 3 only emits `Transfer` (an output containing an asset of a
/// watched policy was produced). Phase 5+ may add `Burn` once the
/// indexer tracks consumed-with-no-replacement; for now, a transfer
/// to a script-locked address handles the common cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OwnershipChange {
    Transfer {
        policy_id: String,
        asset_name: String,
        /// CIP-14 fingerprint (`asset1...`). Computed once on the
        /// indexer side via `cardano_assets::AssetId::fingerprint_typed`
        /// and emitted as a string on the wire (CBOR doesn't gain
        /// from typed wrappers in transit). The CF-side mirror
        /// can leave this as `String` for now or upgrade to typed
        /// in the planned `mitos-protocol` extraction (ROADMAP
        /// step 10).
        ///
        /// Adding this field is wire-compatible with older CF
        /// workers that don't know about it — serde+CBOR ignores
        /// unknown map entries on read.
        asset_fingerprint: String,
        new_owner: String,
        tx_hash: String,
        output_index: u32,
    },
}

/// Aggregate watch state across all live subscriptions.
///
/// `Bounded(set)` — every active subscription's interests project to
/// a finite set of policies; we can skip blocks that touch none of
/// them.
///
/// `Unbounded` — at least one subscription has `AssetSelector::Any`
/// or `AssetSelector::Fingerprint` (which doesn't constrain by
/// policy). Indexer must scan every output and post-filter.
///
/// `Empty` — no live subscriptions. Skip all blocks.
///
/// On unsubscribe we don't shrink the set (only correctness
/// implication is brief over-scanning until the next subscribe
/// recomputes). Phase 5+ refinement.
enum WatchState {
    Empty,
    Bounded(HashSet<PolicyId>),
    Unbounded,
}

pub struct OwnershipIndexer {
    watched: WatchState,
}

impl OwnershipIndexer {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            watched: WatchState::Empty,
        })
    }
}

impl Default for OwnershipIndexer {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

#[async_trait]
impl<D: Domain> Indexer<D> for OwnershipIndexer {
    type Scope = Vec<Interest>;
    type Change = OwnershipChange;

    fn name(&self) -> &'static str {
        "collection-ownership"
    }

    async fn bootstrap(&mut self, _domain: &D) -> anyhow::Result<ChainPoint> {
        info!(
            indexer = "collection-ownership",
            "bootstrap: watch state empty until first subscribe"
        );
        Ok(ChainPoint::Origin)
    }

    async fn handle_event(
        &mut self,
        _domain: &D,
        event: &TipEvent,
        emitter: &Emitter<Self::Change>,
    ) -> anyhow::Result<()> {
        match event {
            TipEvent::Apply(_, block) => {
                let watched = match &self.watched {
                    WatchState::Empty => return Ok(()),
                    WatchState::Bounded(set) if set.is_empty() => return Ok(()),
                    state => state,
                };
                let parsed = match MultiEraBlock::decode(block.as_ref()) {
                    Ok(b) => b,
                    Err(e) => {
                        warn!(error = %e, "block decode failed; skipping");
                        return Ok(());
                    }
                };
                for tx in parsed.txs() {
                    let tx_hash = hex::encode(tx.hash());
                    for (idx, output) in tx.produces() {
                        let address = match output.address() {
                            Ok(a) => a.to_string(),
                            Err(e) => {
                                debug!(error = %e, tx = %tx_hash, "output address parse failed");
                                continue;
                            }
                        };
                        for policy_assets in output.value().assets() {
                            let policy_hex = hex::encode(policy_assets.policy());
                            if !watch_state_contains(watched, &policy_hex) {
                                continue;
                            }
                            for asset in policy_assets.assets() {
                                let asset_name_hex = hex::encode(asset.name());
                                let fingerprint = compute_fingerprint(&policy_hex, &asset_name_hex);
                                emitter.apply(OwnershipChange::Transfer {
                                    policy_id: policy_hex.clone(),
                                    asset_name: asset_name_hex,
                                    asset_fingerprint: fingerprint,
                                    new_owner: address.clone(),
                                    tx_hash: tx_hash.clone(),
                                    output_index: idx as u32,
                                });
                            }
                        }
                    }
                }
            }
            // Undo + Mark are auto-emitted by the framework. The
            // indexer's own state (the watch set) is unaffected by
            // chain reorgs — only consumer-side materialized views
            // need to roll back, and the framework's Undo signal
            // handles that.
            TipEvent::Undo(_, _) | TipEvent::Mark(_) => {}
        }
        Ok(())
    }

    fn routes(&self) -> Router {
        // Phase 5+: expose a watch-set introspection endpoint here
        // (e.g. `GET /watched` returning current policies). For now,
        // empty Router — the bundle still nests it under
        // `/collection-ownership/`.
        Router::new()
    }

    async fn subscribe(
        &mut self,
        domain: &D,
        scope: Self::Scope,
        consumer_cursor: ChainPoint,
        backfill: &mut Vec<Self::Change>,
    ) -> anyhow::Result<SubscribeReply> {
        // Project this consumer's interests down to a policy set
        // (None = subscriber wants Any/Fingerprint, can't backfill
        // without enumerating all policies on chain).
        let policies_for_backfill = watched_policies(&scope);

        // Update the indexer-wide watch state to include this
        // consumer's policies (or escalate to Unbounded if needed).
        merge_into_watch_state(&mut self.watched, policies_for_backfill.as_ref());

        // Cold subscribes (`Origin` cursor) with a bounded policy
        // set get a full backfill synthesised from current state.
        // Warm subscribes skip backfill (live tail from cursor is
        // enough). Subscribers using `Any`/`Fingerprint` selectors
        // also skip backfill — Phase 5+ may add chain-wide
        // backfill via snapshot redirect for those.
        let do_backfill =
            matches!(consumer_cursor, ChainPoint::Origin) && policies_for_backfill.is_some();

        let resume_cursor = if do_backfill {
            // Safe: do_backfill implies Some.
            let policies = policies_for_backfill.as_ref().unwrap();
            let mut last_cursor = consumer_cursor;
            for policy in policies {
                last_cursor = backfill_for_policy(domain, policy.as_str(), backfill)?;
            }
            last_cursor
        } else {
            consumer_cursor
        };

        info!(
            indexer = "collection-ownership",
            interests = scope.len(),
            policies_added = policies_for_backfill.as_ref().map(|p| p.len()),
            watched = ?self.watched,
            backfilled = backfill.len(),
            ?resume_cursor,
            "subscribe"
        );

        Ok(SubscribeReply::Resume {
            cursor: resume_cursor,
        })
    }

    async fn unsubscribe(&mut self, _scope: Self::Scope) -> anyhow::Result<()> {
        // Best-effort: don't shrink the watch set on unsubscribe.
        // The only correctness implication is wasted CPU on blocks
        // matching now-stale policies, which the next subscribe or
        // restart resolves. Tracked under Phase 5+ refinement.
        info!(
            indexer = "collection-ownership",
            "unsubscribe (watch state preserved)"
        );
        Ok(())
    }

    fn change_matches_scope(scope: &Self::Scope, change: &Self::Change) -> bool {
        match change {
            OwnershipChange::Transfer {
                policy_id,
                asset_name,
                ..
            } => match PolicyId::new(policy_id.as_str()) {
                Ok(p) => any_interest_matches_asset(scope, &p, Some(asset_name)),
                Err(_) => false,
            },
        }
    }
}

fn watch_state_contains(state: &WatchState, policy_hex: &str) -> bool {
    match state {
        WatchState::Empty => false,
        WatchState::Unbounded => true,
        WatchState::Bounded(set) => match PolicyId::new(policy_hex) {
            Ok(p) => set.contains(&p),
            Err(_) => false,
        },
    }
}

fn merge_into_watch_state(state: &mut WatchState, additions: Option<&HashSet<PolicyId>>) {
    match additions {
        None => *state = WatchState::Unbounded,
        Some(new_policies) => match state {
            WatchState::Unbounded => {}
            WatchState::Empty => *state = WatchState::Bounded(new_policies.clone()),
            WatchState::Bounded(existing) => existing.extend(new_policies.iter().cloned()),
        },
    }
}

impl std::fmt::Debug for WatchState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatchState::Empty => write!(f, "Empty"),
            WatchState::Unbounded => write!(f, "Unbounded"),
            WatchState::Bounded(s) => write!(f, "Bounded({} policies)", s.len()),
        }
    }
}

/// Synthesise backfill records for a policy from current chain
/// state. Returns the cursor the consumer should resume from
/// (mitos's view of current tip after enumeration).
///
/// Procedure:
/// 1. Read the current cursor from `domain.state()`.
/// 2. Enumerate UTxOs at this policy via the by-policy index.
/// 3. Hydrate each UTxO via `domain.state().get_utxos`.
/// 4. Decode each output, extract address + assets, emit one
///    Transfer record per asset under the watched policy.
fn backfill_for_policy<D: Domain>(
    domain: &D,
    policy_hex: &str,
    out: &mut Vec<OwnershipChange>,
) -> anyhow::Result<ChainPoint> {
    let resume_cursor = domain
        .state()
        .read_cursor()
        .map_err(|e| anyhow::anyhow!("read_cursor: {e:?}"))?
        .unwrap_or(ChainPoint::Origin);

    let policy_bytes =
        hex::decode(policy_hex).map_err(|e| anyhow::anyhow!("invalid policy_id hex: {e}"))?;

    let utxo_set = domain
        .indexes()
        .utxos_by_policy(&policy_bytes)
        .map_err(|e| anyhow::anyhow!("utxos_by_policy: {e:?}"))?;

    let txo_refs: Vec<_> = utxo_set.into_iter().collect();
    if txo_refs.is_empty() {
        return Ok(resume_cursor);
    }

    let utxo_map = domain
        .state()
        .get_utxos(txo_refs)
        .map_err(|e| anyhow::anyhow!("get_utxos: {e:?}"))?;

    for (txo_ref, era_cbor) in utxo_map {
        let era: pallas::ledger::traverse::Era = match era_cbor.0.try_into() {
            Ok(e) => e,
            Err(_) => {
                debug!(?txo_ref, "skipping output with un-convertible era");
                continue;
            }
        };
        let output = match MultiEraOutput::decode(era, &era_cbor.1) {
            Ok(o) => o,
            Err(e) => {
                warn!(?txo_ref, error = %e, "decode utxo failed; skipping");
                continue;
            }
        };

        let address = match output.address() {
            Ok(a) => a.to_string(),
            Err(e) => {
                debug!(?txo_ref, error = %e, "output address parse failed; skipping");
                continue;
            }
        };

        for policy_assets in output.value().assets() {
            if hex::encode(policy_assets.policy()) != policy_hex {
                continue;
            }
            for asset in policy_assets.assets() {
                let asset_name_hex = hex::encode(asset.name());
                let fingerprint = compute_fingerprint(policy_hex, &asset_name_hex);
                out.push(OwnershipChange::Transfer {
                    policy_id: policy_hex.to_string(),
                    asset_name: asset_name_hex,
                    asset_fingerprint: fingerprint,
                    new_owner: address.clone(),
                    tx_hash: hex::encode(txo_ref.0),
                    output_index: txo_ref.1,
                });
            }
        }
    }

    Ok(resume_cursor)
}

/// Compute the CIP-14 asset fingerprint for `(policy_hex,
/// asset_name_hex)`.
///
/// Returns the bech32 string (`asset1...`). Falls back to an empty
/// string with a warn log if the inputs don't validate as a
/// well-formed `AssetId` — exceedingly rare since both come
/// straight from on-chain data, but the framework's emit loop
/// shouldn't crash on a single malformed input.
fn compute_fingerprint(policy_hex: &str, asset_name_hex: &str) -> String {
    use cardano_assets::AssetId;

    // CIP-14 is defined for any asset, including empty asset
    // name. AssetId::new rejects empty names, so use
    // new_unchecked when we hit that case (the indexer already
    // accepts these via `output.value().assets()` even though
    // the consumer DO will filter them out as minting-script
    // artifacts).
    let asset_id = if asset_name_hex.is_empty() {
        AssetId::new_unchecked(policy_hex.to_string(), String::new())
    } else {
        match AssetId::new(policy_hex.to_string(), asset_name_hex.to_string()) {
            Ok(a) => a,
            Err(e) => {
                warn!(
                    policy = %policy_hex,
                    asset_name = %asset_name_hex,
                    error = %e,
                    "invalid AssetId for fingerprint computation; emitting empty"
                );
                return String::new();
            }
        }
    };

    match asset_id.fingerprint_typed() {
        Ok(f) => f.into_string(),
        Err(e) => {
            warn!(error = %e, "fingerprint computation failed; emitting empty");
            String::new()
        }
    }
}
