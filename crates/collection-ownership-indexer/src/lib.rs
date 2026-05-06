//! Collection ownership indexer: per-policy NFT ownership tracking.
//!
//! ## Status: legacy. Slated for retirement.
//!
//! Behaviour-equivalent with the wasm-module port at
//! `cnft.dev-workers/workers/collections-mitos/modules/ownership.rs`.
//! Both produce `OwnershipChange::Transfer` events for the same
//! blocks; the wasm module ships through `mitos-platform`'s
//! companion-runtime path (single-file build via `mitos-build`,
//! upload via `mitos-admin upload-module`); this static crate
//! ships compiled into the host binary and feeds the legacy
//! `mitos-core::Replicator` outbound subscription model.
//!
//! Production cutover plan: once the legacy
//! `cnft.dev-workers/workers/collection-ownership/` consumer
//! migrates onto `collections-mitos`'s companion-runtime path,
//! the two `replicator.connected` legacy subscriptions go away
//! and this crate (along with `mitos-core::Replicator`,
//! `IndexerHandle`, `run_subscriber`) becomes deletable.
//! Until then, leaving in place — production ownership data
//! flows through here.
//!
//! ## Original docs:
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
use dolos_core::{ChainPoint, Domain, TipEvent};
use mitos_core::{Emitter, Indexer, SubscribeReply};
use mitos_protocol::{AssetRole, Interest, any_interest_matches_asset_role, watched_policies};
use pallas::ledger::traverse::MultiEraBlock;
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
        /// NFT-centric classification of this asset by its name
        /// shape. Computed once at emit time via
        /// `mitos_protocol::AssetRole::from_asset_name_hex` so
        /// consumers don't have to re-derive it; also lets the
        /// indexer's `change_matches_scope` drop reference NFTs
        /// and minting-script sentinels server-side when an
        /// `Interest` narrows the `roles` axis.
        ///
        /// `#[serde(default)]` because old payloads on the wire
        /// (pre-AssetRole) lack the field; serde substitutes
        /// `AssetRole::NativeUser` (the most permissive default
        /// — every receiver currently filters explicitly).
        #[serde(default = "default_native_user")]
        role: AssetRole,
        new_owner: String,
        tx_hash: String,
        output_index: u32,
    },
}

fn default_native_user() -> AssetRole {
    AssetRole::NativeUser
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
                                let role = AssetRole::from_asset_name_hex(&asset_name_hex);
                                emitter.apply(OwnershipChange::Transfer {
                                    policy_id: policy_hex.clone(),
                                    asset_name: asset_name_hex,
                                    asset_fingerprint: fingerprint,
                                    role,
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
                last_cursor =
                    backfill_for_policy_via_data_plane(domain, policy.as_str(), backfill).await?;
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
                role,
                ..
            } => match PolicyId::new(policy_id.as_str()) {
                Ok(p) => any_interest_matches_asset_role(scope, &p, Some(asset_name), *role),
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
/// state via the `mitos_data_plane::LocalDataPlane` trait surface.
/// Returns the cursor the consumer should resume from.
///
/// Compared to a hand-rolled `dolos_core::Domain` walk:
/// - No manual era / output decode (plane handles it).
/// - No manual asset-multiset walk to re-filter to the policy
///   (plane's predicate filters server-side via the
///   `utxos_by_policy` index — we still walk the multiset because
///   a single UTxO can carry assets from multiple policies even
///   though the index found it via one specific policy).
/// - `resume_cursor` comes from the page's `tip` rather than a
///   pre-walk `read_cursor`; same effect, slightly more honest
///   (the cursor reflects what the data is consistent with).
async fn backfill_for_policy_via_data_plane<D: dolos_core::Domain>(
    domain: &D,
    policy_hex: &str,
    out: &mut Vec<OwnershipChange>,
) -> anyhow::Result<ChainPoint> {
    use cardano_assets::PolicyId;
    use mitos_data_plane::{
        ChainDataPlane, DecodeLevel, LocalDataPlane, PageRequest, UtxoPattern, UtxoPredicate,
    };

    let plane = LocalDataPlane::new(domain);
    let policy =
        PolicyId::new(policy_hex).map_err(|e| anyhow::anyhow!("invalid policy_id: {e:?}"))?;
    let predicate = UtxoPredicate::Match(UtxoPattern::under_policy(policy.clone()));

    // Phase A LocalDataPlane returns a single page; pagination
    // not yet implemented. Loop is structured for the future
    // pagination story, but ends after one iteration today.
    let mut req = PageRequest::first(1000);
    loop {
        let page = plane
            .search_utxos(&predicate, DecodeLevel::Lean, req.clone())
            .await
            .map_err(|e| anyhow::anyhow!("search_utxos: {e}"))?;

        for (oref, output) in &page.items {
            for asset in &output.assets {
                if asset.policy_id != policy {
                    continue;
                }
                let fingerprint = compute_fingerprint(policy_hex, &asset.asset_name_hex);
                let role = AssetRole::from_asset_name_hex(&asset.asset_name_hex);
                out.push(OwnershipChange::Transfer {
                    policy_id: policy_hex.to_string(),
                    asset_name: asset.asset_name_hex.clone(),
                    asset_fingerprint: fingerprint,
                    role,
                    new_owner: output.address.clone(),
                    tx_hash: hex::encode(oref.tx_hash),
                    output_index: oref.index,
                });
            }
        }

        match page.next_token {
            Some(t) => req = PageRequest::next(t, 1000),
            // Last page — its tip is the resume cursor.
            None => return Ok(page.tip.point),
        }
    }
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
