//! The walk's event tap — the chain, as `chain-events`, on stdout.
//!
//! Writes NDJSON, one [`ChainEvent`] per line, as the walk passes each transaction. It
//! derives nothing the archive would derive differently: the same `Movement` rows the
//! writer stores are handed to `policy_archive::feed::fold_rows` and then to
//! `chain_events_cardano::Collection`, so a replay of this stream and a fold of the
//! finished archive produce the **same event ids**. A later ingest is an upsert, not a
//! second copy — which is the whole point of an experiment stream that is allowed to be
//! thrown away.
//!
//! # Why it taps the walk instead of reading chunks itself
//!
//! A movement needs the value and owner of the **spent** input, and a block does not carry
//! them — inputs are outrefs. The walk resolves them through its outref buffer, built from
//! the collection's first mint, which is why it can attribute a transfer at all. A second
//! chunk reader would have to rebuild that buffer, and two implementations of "who owned
//! the input" is how the two would quietly disagree.
//!
//! # A closed pipe stops the walk
//!
//! Writing to a consumer that has exited is not an error to swallow. The run stops and
//! says so; continuing would walk the chain for nobody.

use std::io::{ErrorKind, Write};

use anyhow::{Context, Result};
use chain_events::{ChainEvent, ChainRef, Coverage};
use chain_events_cardano::Collection;
use policy_archive::Movement;
use policy_archive::feed::fold_rows;

use crate::store::TxRow;

/// What the stream carried.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    /// Transactions the tap translated.
    pub transactions: u64,
    /// Events written.
    pub events: u64,
    /// Unit moves with no representable identity — today only the root token, whose
    /// empty asset name CAIP-19 cannot write.
    pub unrepresentable: u64,
}

/// Writes one collection's movement events to stdout as they are walked.
pub struct Tap {
    collection: Collection,
    out: std::io::BufWriter<std::io::Stdout>,
    /// One clock for the whole stream: a stream is one recording session.
    recorded_at: u64,
    /// Set when the consumer closed the pipe. Every later call is a no-op, so the walk
    /// can finish its bookkeeping rather than unwinding through it.
    closed: bool,
    counts: Counts,
}

impl Tap {
    /// Opens the stream, announcing what it is on stderr.
    ///
    /// The header goes to stderr, never stdout: stdout is NDJSON and nothing else, so
    /// `| jaq` and `| grep` see only events.
    pub fn open(chain: ChainRef, policy: &str, coverage: Coverage) -> Result<Self> {
        let collection = Collection::new(chain, policy, coverage)?;
        eprintln!(
            "# chain-events · {chain} · {policy}\n\
             # family: holder · coverage: {}\n\
             # source: mithril chunks, replayed · ids are derived from the fact, so a \
             later archive upserts over this stream",
            match coverage {
                Coverage::Complete => "complete",
                Coverage::Partial => "partial",
            }
        );

        Ok(Self {
            collection,
            out: std::io::BufWriter::new(std::io::stdout()),
            recorded_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or_default(),
            closed: false,
            counts: Counts::default(),
        })
    }

    /// What the stream has carried so far.
    pub fn counts(&self) -> Counts {
        self.counts
    }

    /// Whether the consumer closed the pipe.
    pub fn closed(&self) -> bool {
        self.closed
    }

    /// Translates one walked transaction and writes its events.
    pub fn emit(&mut self, tx: &TxRow) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let movements = movements_of(tx);
        if movements.is_empty() {
            return Ok(());
        }

        self.counts.transactions += 1;
        for row in fold_rows(movements) {
            let translated = self.collection.from_row(&row, self.recorded_at);
            self.counts.unrepresentable += translated.unrepresentable.len() as u64;
            for event in translated.events {
                self.write(&event)?;
                if self.closed {
                    return Ok(());
                }
            }
        }
        self.flush()
    }

    /// Flushes what has been written. A block's events reach the consumer as a batch.
    pub fn flush(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        match self.out.flush() {
            Ok(()) => Ok(()),
            Err(error) if closed_pipe(&error) => {
                self.closed = true;
                Ok(())
            }
            Err(error) => Err(error).context("flushing the event stream"),
        }
    }

    fn write(&mut self, event: &ChainEvent) -> Result<()> {
        let mut line = serde_json::to_vec(event).context("serialising an event")?;
        line.push(b'\n');
        match self.out.write_all(&line) {
            Ok(()) => {
                self.counts.events += 1;
                Ok(())
            }
            Err(error) if closed_pipe(&error) => {
                self.closed = true;
                Ok(())
            }
            Err(error) => Err(error).context("writing the event stream"),
        }
    }
}

/// The `Movement` rows a walked transaction stands for.
///
/// One per party delta, plus a placeholder for a unit whose net mint reached nobody — the
/// same row the archive's writer stores, so [`fold_rows`] keeps a party-less mint rather
/// than dropping it.
fn movements_of(tx: &TxRow) -> Vec<Movement> {
    let mint_of = |name: &[u8]| {
        tx.net_mint
            .iter()
            .find(|(unit, _)| unit.as_slice() == name)
            .map_or(0, |(_, mint)| *mint)
    };

    let mut movements: Vec<Movement> = Vec::with_capacity(tx.deltas.len() + tx.net_mint.len());
    for delta in &tx.deltas {
        movements.push(Movement {
            slot: tx.slot,
            block_time: tx.block_time,
            tx_hash: tx.tx_hash.as_ref().to_vec(),
            unit_name: delta.name.clone(),
            address: delta.address.clone(),
            amount: delta.amount,
            net_mint: mint_of(&delta.name),
            // The transaction's place in its block, so a merged stream can order two
            // facts from one block. Not identity — `EventId` does not hash it.
            tx_index: tx.tx_index,
        });
    }

    // A mint that landed in no watched output still happened. `fold_rows` keeps a unit
    // with a net mint and no parties, which is exactly the fact.
    for (name, mint) in &tx.net_mint {
        if *mint == 0 || movements.iter().any(|m| &m.unit_name == name) {
            continue;
        }
        movements.push(Movement {
            slot: tx.slot,
            block_time: tx.block_time,
            tx_hash: tx.tx_hash.as_ref().to_vec(),
            unit_name: name.clone(),
            address: String::new(),
            amount: 0,
            net_mint: *mint,
            tx_index: tx.tx_index,
        });
    }

    movements
}

fn closed_pipe(error: &std::io::Error) -> bool {
    error.kind() == ErrorKind::BrokenPipe
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::DeltaRow;

    fn tx(deltas: Vec<DeltaRow>, net_mint: Vec<(Vec<u8>, i64)>) -> TxRow {
        TxRow {
            tx_hash: pallas_primitives::Hash::from([0xab; 32]),
            tx_index: 3,
            slot: 170_445_182,
            block_time: 1_700_000_000,
            net_mint,
            deltas,
            pools: Vec::new(),
            locks_created: Vec::new(),
            locks_spent: Vec::new(),
        }
    }

    fn delta(address: &str, name: &[u8], amount: i64) -> DeltaRow {
        DeltaRow {
            address: address.to_owned(),
            stake: None,
            name: name.to_vec(),
            amount,
        }
    }

    #[test]
    fn a_mint_becomes_one_row_per_party_with_the_transactions_net_mint() {
        let rows = movements_of(&tx(
            vec![delta("addr1vmint", b"Perp", 2)],
            vec![(b"Perp".to_vec(), 2)],
        ));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].address, "addr1vmint");
        assert_eq!(rows[0].amount, 2);
        // Read from the transaction, not inferred from the party amounts.
        assert_eq!(rows[0].net_mint, 2);
        assert_eq!(rows[0].slot, 170_445_182);
        assert_eq!(rows[0].tx_hash, vec![0xab; 32]);
    }

    #[test]
    fn a_transfer_carries_no_mint() {
        let rows = movements_of(&tx(
            vec![
                delta("addr1vfrom", b"Perp", -1),
                delta("addr1vto", b"Perp", 1),
            ],
            Vec::new(),
        ));

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.net_mint == 0));
    }

    #[test]
    fn the_transactions_position_is_carried_into_its_rows() {
        // Chain order is (slot, tx_index). Flattening the position to zero is what
        // makes two facts in one block unorderable once a market stream is merged in.
        let rows = movements_of(&tx(
            vec![delta("addr1vmint", b"Perp", 2)],
            vec![(b"Perp".to_vec(), 2)],
        ));
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tx_index, 3);

        // The party-less mint placeholder carries it too — it is a fact from the same
        // transaction, not a row about nothing.
        let rows = movements_of(&tx(Vec::new(), vec![(b"Perp".to_vec(), 1)]));
        assert_eq!(rows[0].tx_index, 3);
    }

    #[test]
    fn a_unit_that_reached_nobody_is_still_a_mint() {
        // The archive's writer stores a placeholder for this; the tap must too, or the
        // unit's supply silently depends on which tool read the chain.
        let rows = movements_of(&tx(Vec::new(), vec![(b"Perp".to_vec(), 1)]));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].address, "");
        assert_eq!(rows[0].amount, 0);
        assert_eq!(rows[0].net_mint, 1);

        // And `fold_rows` keeps it, which is why the placeholder is worth writing.
        let feed = fold_rows(rows);
        assert_eq!(feed.len(), 1);
        assert_eq!(feed[0].units.len(), 1);
        assert_eq!(feed[0].units[0].net_mint, 1);
        assert!(feed[0].units[0].parties.is_empty());
    }

    #[test]
    fn the_net_mint_is_read_per_unit_rather_than_summed() {
        // Two units in one transaction: a sum would be right only by accident.
        let rows = movements_of(&tx(
            vec![
                delta("addr1vmint", b"One", 1),
                delta("addr1vmint", b"Two", 1),
            ],
            vec![(b"One".to_vec(), 1), (b"Two".to_vec(), 3)],
        ));

        assert_eq!(rows[0].net_mint, 1);
        assert_eq!(rows[1].net_mint, 3);
    }
}
