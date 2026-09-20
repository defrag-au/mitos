//! The hand-off watermark — how a client changes instance without a block
//! being delivered twice, or not at all.
//!
//! D4's first statement of the switch was "pause BOTH followers at the same
//! block boundary, then commit". Two followers have two independent
//! subscriptions; nothing in the host can pause one, let alone align two on a
//! block. This is what replaced it, and it needs no pause primitive at all —
//! only the quiesce the host already trusts (`stop_instance`, which finishes
//! the block in flight and flushes the cursor).
//!
//! ```text
//!   quiesce staging  →  cursor S_B
//!   quiesce live     →  cursor S_A        require  S_B ≤ S_A   (else resume, retry)
//!   staging.handoffs[client] = S_A        (this module)
//!   row.current = staging                 (THE commit — `client_move`)
//!   resume both
//! ```
//!
//! From the commit on, [`crate::routing::fan_out`] says the STAGING instance
//! serves the client. It resumes from `S_B` and replays `(S_B, S_A]` — blocks
//! the live instance already delivered — and [`Handoffs::admits`] withholds
//! them. Everything after `S_A` is delivered. The old instance resumes too,
//! and `fan_out` has it `Skip` the client. Nothing twice, nothing missing, and
//! no instant at which both could speak.
//!
//! `S_B ≤ S_A` is what makes that true with no HOLD. Were staging AHEAD, it
//! would already have processed — and, serving nobody, discarded — blocks in
//! `(S_A, S_B]` that the live instance never reached; they would be lost. The
//! host does not wait for the order to come right (that would be waiting on
//! the world with two followers stopped): it resumes both and tries again on a
//! later tick. Two followers at tip are on the same block almost always.
//!
//! # Rollbacks
//!
//! A reorg to `R` below the watermark voids what the old instance delivered
//! on the abandoned fork. The serving instance sends the client the undo (an
//! undo is never withheld), and the blocks that REPLACE `(R, S_A]` are new
//! blocks nobody has delivered — so the watermark drops to `R`. That is
//! [`Handoffs::rolled_back`], and it is why this state lives WITH THE INSTANCE
//! rather than on the client row: its one writer while the follower runs is
//! that follower's own emission drain, and a fan-out must never write a row.
//! The lifecycle manager writes it only while the follower is stopped — at the
//! commit. Never both at once.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ids::{ClientId, Slot};

/// How far past a watermark the chain must be before it is forgotten.
///
/// Cardano's stability window is `3k/f` = 129,600 slots (36 hours): no
/// rollback can reach further back. A watermark older than that can never be
/// lowered and never withholds anything again, so it is only a line in a file.
pub const STABILITY_WINDOW_SLOTS: u64 = 129_600;

/// Whether this instance's state file needs rewriting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Changed {
    Yes,
    No,
}

/// What the serving instance does with a forward emission for a client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
    Deliver,
    /// At or before the watermark: the client's PREVIOUS instance delivered
    /// this block. Writing a row would deliver it twice.
    Withheld,
}

/// One instance's hand-off watermarks: for each client it took over, the slot
/// its predecessor had delivered up to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Handoffs {
    #[serde(default)]
    after: BTreeMap<ClientId, Slot>,
}

impl Handoffs {
    pub fn is_empty(&self) -> bool {
        self.after.is_empty()
    }

    pub fn watermark(&self, client: &ClientId) -> Option<Slot> {
        self.after.get(client).copied()
    }

    /// The commit: this instance serves `client` for every block AFTER
    /// `after`. Replaces any earlier watermark — a client that left and came
    /// back is handed over afresh.
    pub fn begin(&mut self, client: ClientId, after: Slot) {
        self.after.insert(client, after);
    }

    /// A forward emission at `at`. An UNDO is never asked about: a rollback
    /// is delivered unconditionally.
    pub fn admits(&self, client: &ClientId, at: Slot) -> Admit {
        match self.after.get(client) {
            Some(after) if at <= *after => Admit::Withheld,
            _ => Admit::Deliver,
        }
    }

    /// The chain rolled back to `to`. Every watermark above it drops to it:
    /// the blocks that will replace `(to, watermark]` are new, and nobody has
    /// delivered them.
    pub fn rolled_back(&mut self, to: Slot) -> Changed {
        let mut changed = Changed::No;
        for after in self.after.values_mut() {
            if *after > to {
                *after = to;
                changed = Changed::Yes;
            }
        }
        changed
    }

    /// Forget watermarks the chain has moved a whole stability window past.
    pub fn settle(&mut self, tip: Slot) -> Changed {
        let before = self.after.len();
        self.after
            .retain(|_, after| after.0.saturating_add(STABILITY_WINDOW_SLOTS) >= tip.0);
        if self.after.len() == before {
            Changed::No
        } else {
            Changed::Yes
        }
    }
}

/// Whether the two quiesced cursors allow a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    /// Staging is at or behind live: commit with this watermark.
    Commit { after: Slot },
    /// Staging got ahead. Resume both and try again later — see the module
    /// doc for what committing here would lose.
    StagingAhead,
}

pub fn align(live: Slot, staging: Slot) -> Alignment {
    if staging <= live {
        Alignment::Commit { after: live }
    } else {
        Alignment::StagingAhead
    }
}

/// How close staging must be before the host bothers to quiesce anything.
///
/// Quiescing stops the LIVE follower too, so it is not tried on a staging
/// instance still hours behind. Three blocks' worth of slots: close enough
/// that the replay after the commit is seconds, loose enough that ordinary
/// jitter between two followers at tip does not keep a move waiting.
pub const CAUGHT_UP_WITHIN_SLOTS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUp {
    CaughtUp,
    StillBehind { by: u64 },
}

pub fn catch_up(live: Slot, staging: Slot) -> CatchUp {
    let by = live.0.saturating_sub(staging.0);
    if by <= CAUGHT_UP_WITHIN_SLOTS {
        CatchUp::CaughtUp
    } else {
        CatchUp::StillBehind { by }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(host: &str) -> ClientId {
        ClientId::parse(host).unwrap()
    }

    /// THE property, simulated end to end: a chain of blocks, a live
    /// instance that delivered up to `S_A`, a staging instance resuming from
    /// any `S_B ≤ S_A`. Every block reaches the client exactly once.
    #[test]
    fn every_block_is_delivered_exactly_once_across_a_hand_off() {
        let chain: Vec<u64> = (1..=40).map(|n| n * 20).collect();
        let c = client("ownership.cnft.dev");
        for live_at in [200, 400, 800] {
            for staging_at in chain.iter().copied().filter(|s| *s <= live_at) {
                let Alignment::Commit { after } = align(Slot(live_at), Slot(staging_at)) else {
                    panic!("staging at or behind live must align");
                };
                let mut handoffs = Handoffs::default();
                handoffs.begin(c.clone(), after);

                let mut delivered: Vec<u64> =
                    chain.iter().copied().filter(|s| *s <= live_at).collect();
                for slot in chain.iter().copied().filter(|s| *s > staging_at) {
                    if handoffs.admits(&c, Slot(slot)) == Admit::Deliver {
                        delivered.push(slot);
                    }
                }
                assert_eq!(delivered, chain, "live {live_at}, staging {staging_at}");
            }
        }
    }

    /// Why `StagingAhead` refuses: those blocks were processed by an instance
    /// that served nobody, and never reached by the one that did.
    #[test]
    fn staging_ahead_of_live_would_lose_blocks_so_it_does_not_commit() {
        assert_eq!(align(Slot(400), Slot(420)), Alignment::StagingAhead);
        assert_eq!(
            align(Slot(400), Slot(400)),
            Alignment::Commit { after: Slot(400) }
        );
    }

    /// A reorg below the watermark: the old instance's deliveries on the dead
    /// fork are void, and the blocks that replace them are nobody's yet.
    #[test]
    fn a_rollback_below_the_watermark_lowers_it() {
        let c = client("ownership.cnft.dev");
        let mut handoffs = Handoffs::default();
        handoffs.begin(c.clone(), Slot(400));
        assert_eq!(handoffs.admits(&c, Slot(380)), Admit::Withheld);

        assert_eq!(handoffs.rolled_back(Slot(360)), Changed::Yes);
        assert_eq!(
            handoffs.admits(&c, Slot(380)),
            Admit::Deliver,
            "the block now at 380 is a NEW block"
        );
        assert_eq!(handoffs.admits(&c, Slot(360)), Admit::Withheld);

        // A rollback that stays above the watermark touches nothing.
        assert_eq!(handoffs.rolled_back(Slot(500)), Changed::No);
        assert_eq!(handoffs.watermark(&c), Some(Slot(360)));
    }

    #[test]
    fn a_client_with_no_watermark_is_always_served() {
        let mut handoffs = Handoffs::default();
        handoffs.begin(client("ownership.cnft.dev"), Slot(400));
        assert_eq!(
            handoffs.admits(&client("jpgsm.cnft.dev"), Slot(1)),
            Admit::Deliver
        );
    }

    #[test]
    fn a_watermark_is_forgotten_once_no_rollback_can_reach_it() {
        let c = client("ownership.cnft.dev");
        let mut handoffs = Handoffs::default();
        handoffs.begin(c.clone(), Slot(1_000));
        assert_eq!(
            handoffs.settle(Slot(1_000 + STABILITY_WINDOW_SLOTS)),
            Changed::No
        );
        assert_eq!(
            handoffs.settle(Slot(1_001 + STABILITY_WINDOW_SLOTS)),
            Changed::Yes
        );
        assert!(handoffs.is_empty());
    }

    /// It is a file an instance's own drain rewrites; a typo in it must not
    /// become "no watermark", which would deliver a block twice.
    #[test]
    fn the_file_round_trips_and_refuses_what_it_does_not_know() {
        let mut handoffs = Handoffs::default();
        handoffs.begin(client("ownership.cnft.dev"), Slot(400));
        let text = toml::to_string(&handoffs).unwrap();
        assert_eq!(toml::from_str::<Handoffs>(&text).unwrap(), handoffs);
        assert_eq!(toml::from_str::<Handoffs>("").unwrap(), Handoffs::default());
        assert!(toml::from_str::<Handoffs>("aftr = {}").is_err());
    }

    #[test]
    fn catching_up_is_measured_in_slots_behind_live() {
        assert_eq!(catch_up(Slot(1_000), Slot(1_000)), CatchUp::CaughtUp);
        assert_eq!(catch_up(Slot(1_000), Slot(940)), CatchUp::CaughtUp);
        assert_eq!(
            catch_up(Slot(1_000), Slot(900)),
            CatchUp::StillBehind { by: 100 }
        );
        // Ahead is "caught up"; whether it can COMMIT is `align`'s question.
        assert_eq!(catch_up(Slot(1_000), Slot(1_020)), CatchUp::CaughtUp);
    }
}
