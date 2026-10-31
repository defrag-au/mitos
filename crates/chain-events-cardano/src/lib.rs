//! `policy-archive`'s movement rows and `market-ledger`'s marketplace rows, translated
//! into `chain-events`.
//!
//! The archive has already done the hard part: it folded every `(transaction, unit,
//! party)` row into one [`FeedRow`] per transaction, reconciled the sums, and graded its
//! own completeness. This crate does the last step — one [`ChainEvent`] per unit move —
//! and nothing else. It reads, it does not walk.
//!
//! # What it refuses to decide
//!
//! - **The kind comes from `net_mint`**, via [`HolderEvent::from_net_mint`], never from
//!   the shape of the party list. A fee split is a transfer; a mint paying two addresses
//!   is a mint.
//! - **The coverage is the caller's**, because completeness is a property of the walk
//!   that produced the archive, not of a row. Passing it in is what stops every event
//!   claiming `Complete`.
//! - **An asset name CAIP-19 cannot express is reported, not dropped.** Cardano allows
//!   an empty asset name — the policy's root token — and CAIP-19 has no token id for it.
//!   [`Translated::unrepresentable`] carries those names so a supply that does not
//!   reconcile has a stated reason.
//! - **A party is the account, not the address that happened to hold it.** Every party is
//!   keyed by [`policy_archive::trade::account_key`] — the stake credential, lower-case
//!   hex — so a wallet that rotates payment addresses is one party, and so a movement and a
//!   marketplace sale naming the same wallet are the same party. Both halves of a
//!   collection go through that one function; see the identity note in [`market`].
//!
//! The marketplace half of the Cardano input is [`market`], and it decodes nothing: a
//! listing is a UTxO observation that `tools/market-ledger` has already read with
//! `mitos-marketplace-decode`. This crate is the translation, for both halves.

use chain_events::{
    AccountRef, AssetRefError, AssetType, ChainCoord, ChainEvent, ChainRef, Coverage, EventBody,
    EventSource, HolderEvent, PartyDelta, TokenRef, TxRef,
};
use policy_archive::feed::FeedRow;
use policy_archive::trade::account_key;
use std::collections::BTreeMap;

mod market;

pub use market::MarketTranslated;

/// One policy on one chain, ready to translate rows.
///
/// Constructed once and reused across a whole archive: the collection identity is
/// validated here, so a bad policy id fails at startup rather than on the ten-thousandth
/// row.
pub struct Collection {
    chain: ChainRef,
    asset_type: AssetType,
    coverage: Coverage,
}

impl Collection {
    /// A Cardano collection, identified by its policy id.
    ///
    /// `coverage` is how much of this policy's history the archive accounts for — the
    /// archive's own `Completeness`, decided by the caller from the manifest.
    pub fn new(chain: ChainRef, policy: &str, coverage: Coverage) -> Result<Self, AssetRefError> {
        let asset_type = AssetType::new(chain, "cnt", policy)?;
        Ok(Self {
            chain,
            asset_type,
            coverage,
        })
    }

    /// The policy id this collection was built from.
    pub fn policy(&self) -> &str {
        self.asset_type.reference()
    }

    /// The chain this collection is on.
    pub fn chain(&self) -> ChainRef {
        self.chain
    }

    /// The collection's CAIP-19 reference without a token half.
    pub fn asset_type(&self) -> &AssetType {
        &self.asset_type
    }

    /// How much of this collection's history the source accounts for.
    pub fn coverage(&self) -> Coverage {
        self.coverage
    }

    /// Translates one transaction's worth of movements.
    ///
    /// `recorded_at` is the caller's clock; this crate does not read one.
    pub fn from_row(&self, row: &FeedRow, recorded_at: u64) -> Translated {
        let mut events = Vec::with_capacity(row.units.len());
        let mut unrepresentable = Vec::new();

        for unit in &row.units {
            let Some(asset) = self.asset(&unit.name) else {
                unrepresentable.push(unit.name.clone());
                continue;
            };

            // Key by account, then net. Two payment addresses of one wallet are one
            // party, and a movement between them changed nothing: 22.6% of Perps transfers
            // are that rotation, and left un-netted they read as a sale and an instant
            // re-buy — which revokes "never sold" and ends a "held since mint" span for a
            // holder who did neither. A `BTreeMap` also makes the party order depend on the
            // accounts rather than on how the archive enumerated the row.
            let mut net: BTreeMap<String, i64> = BTreeMap::new();
            for party in &unit.parties {
                *net.entry(account_key(&party.address)).or_insert(0) += party.amount;
            }
            let parties: Vec<PartyDelta> = net
                .into_iter()
                .filter(|(_, delta)| *delta != 0)
                .map(|(address, delta)| PartyDelta {
                    account: AccountRef::new(self.chain, address),
                    delta,
                })
                .collect();

            // Every party netted to zero and no supply was created: ownership did not
            // change, so there is no holder fact to state.
            if parties.is_empty() && unit.net_mint == 0 {
                continue;
            }

            let body = HolderEvent::from_net_mint(unit.net_mint, parties);
            let discriminator = body.discriminator();

            events.push(
                ChainEvent::draft(
                    self.chain,
                    self.asset_type.clone(),
                    EventBody::Holder(body),
                    row.block_time,
                    EventSource::PolicyArchive,
                )
                .asset(asset)
                .at(ChainCoord::new(row.slot))
                .tx(TxRef::new(hex::encode(&row.tx_hash), row.tx_index))
                .coverage(self.coverage)
                .build(&discriminator, recorded_at),
            );
        }

        Translated {
            events,
            unrepresentable,
        }
    }

    /// The asset a unit names, or `None` when CAIP-19 cannot express it.
    ///
    /// The empty name is the case that matters: it is a real token (Cardano allows it,
    /// and a policy's root token uses it) with no token id to write.
    pub(crate) fn asset(&self, name: &[u8]) -> Option<TokenRef> {
        let name_hex = hex::encode(name);
        if name_hex.is_empty() {
            return None;
        }
        TokenRef::new(self.asset_type.clone(), Some(&name_hex)).ok()
    }
}

/// What one row produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translated {
    /// One event per representable unit move.
    pub events: Vec<ChainEvent>,
    /// Units, by raw asset name, that have no representable identity. Today that is
    /// only the empty name — the policy's root token — which CAIP-19 cannot write.
    pub unrepresentable: Vec<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain_events::HolderKind;
    use policy_archive::feed::{PartyMove, UnitMove};
    use std::str::FromStr;

    /// The Perps policy, so the tests name the collection this work is for.
    const POLICY: &str = "e6ba9c0ff27be029442c32533c6efd956a60d15ecb976acbb64c4de0";

    fn chain() -> ChainRef {
        ChainRef::from_str("cardano:mainnet").unwrap()
    }

    fn collection() -> Collection {
        Collection::new(chain(), POLICY, Coverage::Complete).unwrap()
    }

    fn row(tx_index: u32, units: Vec<UnitMove>) -> FeedRow {
        FeedRow {
            tx_hash: vec![0xab; 32],
            tx_index,
            slot: 170_445_182,
            block_time: 1_700_000_000,
            units,
        }
    }

    fn unit(name: &[u8], net_mint: i64, parties: &[(&str, i64)]) -> UnitMove {
        UnitMove {
            name: name.to_vec(),
            net_mint,
            parties: parties
                .iter()
                .map(|(address, amount)| PartyMove {
                    address: (*address).to_owned(),
                    amount: *amount,
                })
                .collect(),
        }
    }

    /// A base address over `pay` and `stake`, network 1 — the shape a wallet uses.
    fn wallet(pay: u8, stake: u8) -> String {
        const CHARSET: &[u8] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";
        let mut bytes = vec![0x01];
        bytes.extend([pay; 28]);
        bytes.extend([stake; 28]);
        let (mut acc, mut bits, mut out) = (0u32, 0u32, String::from("addr1"));
        for b in bytes {
            acc = (acc << 8) | u32::from(b);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(CHARSET[((acc >> bits) & 31) as usize] as char);
            }
        }
        if bits > 0 {
            out.push(CHARSET[((acc << (5 - bits)) & 31) as usize] as char);
        }
        out + "qqqqqq"
    }

    #[test]
    fn a_wallet_rotating_payment_addresses_states_no_holder_fact() {
        // The common case, not an edge: 22.6% of Perps transfers are one wallet moving an
        // asset to another of its own addresses. Nothing changed hands, and a holder fact
        // here would revoke "never sold" for a holder who never sold.
        let from = wallet(1, 9);
        let to = wallet(2, 9);
        let translated =
            collection().from_row(&row(3, vec![unit(b"Perp", 0, &[(&from, -1), (&to, 1)])]), 1);
        assert!(translated.events.is_empty());
        assert!(translated.unrepresentable.is_empty());
    }

    #[test]
    fn a_real_transfer_names_the_account_on_both_sides() {
        // Different stake credentials: one event, two parties, each keyed by its credential.
        let from = wallet(1, 9);
        let to = wallet(2, 10);
        let event = &collection()
            .from_row(&row(3, vec![unit(b"Perp", 0, &[(&from, -1), (&to, 1)])]), 1)
            .events[0];
        let EventBody::Holder(holder) = &event.body else {
            panic!("a holder row is a holder event")
        };
        assert_eq!(holder.parties.len(), 2);
        // Sorted by account key, so the row's own enumeration order cannot reach the id.
        assert_eq!(holder.parties[0].account.address, "09".repeat(28));
        assert_eq!(holder.parties[1].account.address, "0a".repeat(28));
        assert_eq!(holder.parties[0].delta, -1);
        assert_eq!(holder.parties[1].delta, 1);
    }

    #[test]
    fn a_mint_that_reached_nobody_is_still_a_fact() {
        // Netting must not swallow a supply change: a mint with no watched output is the
        // reason a supply can be reconciled at all.
        let translated = collection().from_row(&row(3, vec![unit(b"Perp", 1, &[])]), 1);
        assert_eq!(translated.events.len(), 1);
    }

    #[test]
    fn an_address_with_no_stake_credential_is_its_own_party() {
        // An enterprise address has no credential to fold under. Emptying its identity
        // would collapse every stake-less address into one party.
        let event = &collection()
            .from_row(&row(3, vec![unit(b"Perp", 1, &[("addr1vminter", 1)])]), 1)
            .events[0];
        let EventBody::Holder(holder) = &event.body else {
            panic!("a holder row is a holder event")
        };
        assert_eq!(holder.parties[0].account.address, "addr1vminter");
    }

    #[test]
    fn a_mint_row_carries_the_archives_coordinates() {
        let translated = collection().from_row(
            &row(7, vec![unit(b"Perp", 1, &[("addr1vminter", 1)])]),
            1_700_000_001,
        );

        assert!(translated.unrepresentable.is_empty());
        assert_eq!(translated.events.len(), 1);

        let event = &translated.events[0];
        assert_eq!(event.chain, chain());
        assert_eq!(event.collection.reference(), POLICY);
        // "Perp" as hex — the token id CAIP-19 carries for a Cardano asset name.
        assert_eq!(
            event.asset.as_ref().and_then(|asset| asset.token_id()),
            Some("50657270")
        );
        assert_eq!(event.coord, Some(ChainCoord::new(170_445_182)));
        assert_eq!(
            event.tx,
            Some(TxRef {
                hash: "ab".repeat(32),
                index: 7,
                event_index: 0,
            })
        );
        assert_eq!(event.occurred_at, 1_700_000_000);
        assert_eq!(event.source, EventSource::PolicyArchive);
        assert_eq!(event.coverage, Coverage::Complete);

        let EventBody::Holder(movement) = &event.body else {
            panic!("expected a movement");
        };
        assert_eq!(movement.kind, HolderKind::Mint);
    }

    #[test]
    fn a_transaction_touching_two_units_is_two_events_with_one_coordinate() {
        let translated = collection().from_row(
            &row(
                3,
                vec![
                    unit(b"One", 0, &[("addr1va", -1), ("addr1vb", 1)]),
                    unit(b"Two", 0, &[("addr1va", -1), ("addr1vb", 1)]),
                ],
            ),
            1,
        );

        assert_eq!(translated.events.len(), 2);
        assert_eq!(translated.events[0].coord, translated.events[1].coord);
        assert_eq!(translated.events[0].tx, translated.events[1].tx);
        assert_ne!(translated.events[0].asset, translated.events[1].asset);
        assert_ne!(translated.events[0].id, translated.events[1].id);
    }

    #[test]
    fn the_transactions_position_in_its_block_does_not_change_the_id() {
        // The archive records the position the reverse walk saw; the forward walk and a
        // marketplace feed have none. A fact reported by both must upsert, not duplicate.
        let archived = collection().from_row(
            &row(
                28,
                vec![unit(b"Perp", 0, &[("addr1va", -1), ("addr1vb", 1)])],
            ),
            1_000,
        );
        let walked = collection().from_row(
            &row(
                0,
                vec![unit(b"Perp", 0, &[("addr1va", -1), ("addr1vb", 1)])],
            ),
            1_000,
        );

        assert_eq!(archived.events[0].id, walked.events[0].id);
        // The coordinate is still carried, so ordering is not lost with the identity.
        assert_eq!(archived.events[0].tx.as_ref().map(|tx| tx.index), Some(28));
    }

    #[test]
    fn the_root_token_has_no_representable_identity_and_is_reported() {
        // Cardano allows an empty asset name and a policy's root token uses it; CAIP-19
        // has no token id to write for one. Reported rather than dropped, so a supply
        // that does not reconcile has a stated reason.
        let translated = collection().from_row(
            &row(
                0,
                vec![
                    unit(b"", 1, &[("addr1vminter", 1)]),
                    unit(b"Named", 1, &[("addr1vminter", 1)]),
                ],
            ),
            1,
        );

        assert_eq!(translated.unrepresentable, vec![Vec::<u8>::new()]);
        assert_eq!(translated.events.len(), 1);
        assert_eq!(
            translated.events[0]
                .asset
                .as_ref()
                .and_then(|asset| asset.token_id()),
            Some("4e616d6564")
        );
    }

    #[test]
    fn a_fee_split_is_still_a_transfer() {
        // One seller, two recipients, no supply change. The kind is the supply delta's
        // answer, not the party list's.
        let translated = collection().from_row(
            &row(
                1,
                vec![unit(
                    b"Split",
                    0,
                    &[
                        ("addr1vseller", -10),
                        ("addr1vroyalty", 7),
                        ("addr1vmarket", 3),
                    ],
                )],
            ),
            1,
        );

        let EventBody::Holder(movement) = &translated.events[0].body else {
            panic!("expected a movement");
        };
        assert_eq!(movement.kind, HolderKind::Transfer);
        assert_eq!(movement.parties.len(), 3);
        assert_eq!(
            chain_events::direction(movement),
            chain_events::Direction::Ambiguous
        );
    }

    #[test]
    fn the_same_row_translates_to_the_same_ids_twice() {
        let row = row(
            2,
            vec![unit(b"Perp", 0, &[("addr1va", -1), ("addr1vb", 1)])],
        );

        let first = collection().from_row(&row, 1_000);
        let second = collection().from_row(&row, 2_000);

        assert_eq!(first.events.len(), 1);
        // The id is the fact's, not the recording's: a second replay with a different
        // clock upserts the same row rather than adding another.
        assert_eq!(first.events[0].id, second.events[0].id);
        assert_ne!(first.events[0].recorded_at, second.events[0].recorded_at);
    }
}
