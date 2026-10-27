use super::*;

fn ev(slot: u64, new_tx: bool, what: What) -> Event {
    Event {
        slot,
        unix: 1_700_000_000 + slot,
        new_tx,
        what,
    }
}

/// Every kind, both ways round, with the cases a varint or a flag could get
/// wrong: party 0 against no party, a quote of 0 against none, the same slot.
#[test]
fn a_chunk_of_every_kind_round_trips() {
    let events = vec![
        ev(
            100,
            true,
            What::Fill {
                party: Some(0),
                venue: 1,
                sell: true,
                units: 5,
            },
        ),
        ev(
            100,
            false,
            What::Pool {
                pool: 2,
                base: 1_000_000,
                quote: Some(0),
            },
        ),
        ev(
            101,
            true,
            What::Fill {
                party: None,
                venue: 0,
                sell: false,
                units: u64::MAX,
            },
        ),
        ev(
            101,
            false,
            What::Pool {
                pool: 0,
                base: 7,
                quote: None,
            },
        ),
        ev(
            150,
            true,
            What::Batch {
                venue: 3,
                orders: 4,
                units: 900,
            },
        ),
        ev(
            151,
            true,
            What::Curve {
                pool: 1,
                lovelace: 9,
                units_left: 8,
                progress_ppm: Some(0),
            },
        ),
        ev(
            151,
            false,
            What::Curve {
                pool: 1,
                lovelace: 9,
                units_left: 8,
                progress_ppm: None,
            },
        ),
        ev(
            152,
            true,
            What::Transfer {
                from: 70_000,
                to: 3,
                units: 1,
            },
        ),
        ev(
            152,
            true,
            What::Mint {
                to: None,
                units: 1_000,
            },
        ),
        ev(
            152,
            true,
            What::Burn {
                from: Some(9),
                units: 2,
            },
        ),
        ev(153, true, What::Arrived { to: 4, units: 3 }),
        ev(
            154,
            true,
            What::Placement {
                party: Some(5),
                venue: 2,
                units: 6,
            },
        ),
        ev(
            155,
            true,
            What::Cancellation {
                party: None,
                venue: 2,
                units: 6,
            },
        ),
    ];
    let mut ids: Vec<u32> = events.iter().flat_map(|e| e.what.parties()).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids, vec![0, 3, 4, 5, 9, 70_000], "every named party, once");
    let chunk = Chunk {
        stream: Stream::Market,
        day: 19_700,
        events,
    };
    let bytes = chunk.encode();
    assert_eq!(Chunk::decode(&bytes), Ok(chunk));
}

/// The table round-trips, and one record read on its own, as a range
/// request would fetch it, says who the party is or where the rest is.
#[test]
fn the_party_table_round_trips_and_reads_one_record_at_a_time() {
    let mut keys: Vec<PartyKey> = (0..5_000u32)
        .map(|i| PartyKey::StakeKey([(i % 251) as u8; 28]))
        .collect();
    keys[1] = PartyKey::StakeScript([7; 28]);
    keys[2] = PartyKey::Address(vec![0x61; 29]);
    keys[3] = PartyKey::Byron("DdzFFzCqrht".to_string());
    let table = PartyTable { keys };
    let (file, overflow) = table.encode();
    assert_eq!(file.len() as u64, PARTY_HEADER + 5_000 * PARTY_RECORD);
    assert_eq!(PartyTable::decode(&file, &overflow), Ok(table.clone()));

    let one = |id: u32| {
        let (start, len) = PartyTable::record_range(id);
        PartyTable::record(&file[start as usize..(start + len) as usize]).expect("a record")
    };
    assert_eq!(one(4_999), PartyRecord::Key(table.keys[4_999].clone()));
    assert_eq!(one(1), PartyRecord::Key(PartyKey::StakeScript([7; 28])));
    let PartyRecord::Overflow {
        byron: true,
        offset,
        len,
    } = one(3)
    else {
        panic!("a Byron address overflows");
    };
    assert_eq!(
        &overflow[offset as usize..(offset + len) as usize],
        b"DdzFFzCqrht"
    );
    assert!(PartyTable::decode(&file[..file.len() - 1], &overflow).is_err());
}

#[test]
fn meta_and_index_round_trip() {
    let meta = Meta {
        policy: "c881".repeat(14),
        unit: b"NikePig".to_vec(),
        venues: vec!["minswap-v1".into(), "sundae-v3".into()],
        pools: vec![Pool {
            venue: 1,
            address: "addr1z8…".into(),
            key_policy: vec![1, 2],
            key_name: vec![],
            quote_policy: vec![],
            quote_name: vec![],
            curve: false,
        }],
    };
    assert_eq!(Meta::decode(&meta.encode()), Ok(meta));
    let index = Index {
        parties: 70_000,
        chunks: vec![ChunkInfo {
            stream: Stream::Transfers,
            day: 19_900,
            first_slot: 127_000_000,
            last_slot: 127_086_000,
            events: 4_000,
            bytes: 30_000,
        }],
    };
    assert_eq!(Index::decode(&index.encode()), Ok(index));
}

/// What a reader must refuse rather than misread.
#[test]
fn a_damaged_file_is_refused() {
    let chunk = Chunk {
        stream: Stream::Orders,
        day: 1,
        events: vec![ev(
            5,
            true,
            What::Placement {
                party: Some(1),
                venue: 0,
                units: 2,
            },
        )],
    };
    let bytes = chunk.encode();
    assert_eq!(Chunk::decode(&bytes[..bytes.len() - 1]), Err(Error::Short));
    let mut longer = bytes.clone();
    longer.push(0);
    assert!(matches!(Chunk::decode(&longer), Err(Error::Invalid(_))));
    assert_eq!(Meta::decode(&bytes), Err(Error::NotThisFile));
    let mut newer = bytes.clone();
    newer[4] = 99;
    assert_eq!(Chunk::decode(&newer), Err(Error::Format(99)));
}

/// A mix of every kind of key, named and not.
fn identities() -> Vec<Identity> {
    let named = |key, name: &str| Identity {
        key,
        name: Some(name.to_string()),
    };
    let unnamed = |key| Identity { key, name: None };
    vec![
        named(PartyKey::StakeKey([1; 28]), "olecram"),
        unnamed(PartyKey::StakeScript([2; 28])),
        unnamed(PartyKey::Address(vec![0x61; 29])),
        unnamed(PartyKey::Byron("DdzFFzCqrht".to_string())),
        named(PartyKey::StakeKey([5; 28]), "xn--pn8hr1g"),
        // The longest a handle can be: a sub-handle.
        named(PartyKey::StakeKey([6; 28]), "abcdefghijklm@nopqrstuvwxyz0"),
    ]
}

/// The dense table round-trips, and one record read on its own, as a range
/// request fetches it, gives the key and the handle.
#[test]
fn the_holder_table_round_trips_and_reads_one_party_at_a_time() {
    let table = HolderTable {
        identities: identities(),
    };
    let (file, overflow) = table.encode().expect("encodes");
    assert_eq!(file.len() as u64, HOLDER_HEADER + 6 * HOLDER_RECORD);
    assert_eq!(HolderTable::decode(&file, &overflow), Ok(table.clone()));

    let one = |id: u32| {
        let (start, len) = HolderTable::record_range(id);
        HolderTable::record(&file[start as usize..(start + len) as usize]).expect("a record")
    };
    let (key, name) = one(0);
    assert_eq!(key, PartyRecord::Key(PartyKey::StakeKey([1; 28])));
    assert_eq!(name.as_deref(), Some("olecram"));
    let (key, name) = one(3);
    assert_eq!(name, None);
    assert_eq!(
        key.resolve(&overflow),
        Ok(PartyKey::Byron("DdzFFzCqrht".to_string()))
    );
}

/// A handle longer than the field, or empty, is refused rather than cut.
#[test]
fn a_handle_that_does_not_fit_is_refused() {
    let too_long = HolderTable {
        identities: vec![Identity {
            key: PartyKey::StakeKey([1; 28]),
            name: Some("x".repeat(NAME_BYTES + 1)),
        }],
    };
    assert!(too_long.encode().is_err());
}

/// The sparse subset reads on its own, overflowing keys and all.
#[test]
fn a_sparse_subset_round_trips_and_answers_by_number() {
    let entries: Vec<(u32, Identity)> = [7u32, 40, 41, 900, 98_000, 98_467]
        .into_iter()
        .zip(identities())
        .collect();
    let subset = Identities { entries };
    let back = Identities::decode(&subset.encode().expect("encodes")).expect("decodes");
    assert_eq!(back, subset);
    assert_eq!(
        back.get(900).map(|i| &i.key),
        Some(&PartyKey::Byron("DdzFFzCqrht".to_string()))
    );
    assert_eq!(back.get(8), None);

    let mut shuffled = subset.clone();
    shuffled.entries.swap(0, 1);
    assert!(Identities::decode(&shuffled.encode().expect("encodes")).is_err());
}

#[test]
fn current_holders_round_trip() {
    let holding = Holding {
        balances: vec![(4, 500_000_000_000), (0, 12), (98_000, 1)],
    };
    assert_eq!(Holding::decode(&holding.encode()), Ok(holding));
}
