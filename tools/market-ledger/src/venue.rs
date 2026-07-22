//! Venue registry — the pluggability seam.
//!
//! Venues are declarative config, not code paths scattered through the walker.
//! A venue names its watched sale/offer addresses (jpg — exact bech32) or
//! payment credentials (Wayup — the per-seller staking part varies, so we match
//! the payment part), an `earliest_slot` fast-skip floor, and a `decoder`
//! dispatch key into `mitos-marketplace-decode`. Adding a marketplace later is a
//! config edit plus (if genuinely new) its decoder in the crate — the walker
//! needs no change.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use pallas_addresses::{Address, ShelleyPaymentPart};
use serde::Deserialize;

/// What a watched address/credential is used for at a venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Sale,
    Offer,
}

/// The decoder dispatch key — which venue's decode functions in
/// `mitos-marketplace-decode` handle this venue's txs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoder {
    Jpg,
    Wayup,
}

impl Decoder {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "jpg" => Ok(Self::Jpg),
            "wayup" => Ok(Self::Wayup),
            other => bail!("unknown venue decoder `{other}` (expected jpg|wayup)"),
        }
    }
}

/// A watched address/credential resolved to its venue, channel, and decoder.
#[derive(Debug, Clone)]
pub struct Watch {
    pub venue: String,
    // `channel` + `decoder` are read by the outref-buffer slice (decoder
    // dispatch into `mitos-marketplace-decode`, sale-vs-offer routing) and by
    // the unit tests; the Slice-2 walker only reads `venue`.
    #[allow(dead_code)]
    pub channel: Channel,
    #[allow(dead_code)]
    pub decoder: Decoder,
}

struct VenueMeta {
    // Consumed by the outref-buffer slice (per-venue decoder dispatch).
    #[allow(dead_code)]
    decoder: Decoder,
    earliest_slot: u64,
}

/// The enabled venue set with a fast watched-set lookup.
pub struct VenueRegistry {
    venues: BTreeMap<String, VenueMeta>,
    /// Exact bech32 → watch (jpg).
    exact_addrs: HashMap<String, Watch>,
    /// Payment credential → watch (Wayup).
    creds: HashMap<[u8; 28], Watch>,
}

impl VenueRegistry {
    /// Load the registry, keeping only `selected` venues (empty = all). Errors on
    /// a selected name that isn't in the file.
    pub fn load(path: &Path, selected: &[String]) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading venue registry {}", path.display()))?;
        Self::parse(&text, selected)
            .with_context(|| format!("in venue registry {}", path.display()))
    }

    fn parse(text: &str, selected: &[String]) -> Result<Self> {
        let file: RegistryFile = toml::from_str(text).context("parsing venue registry TOML")?;

        for name in selected {
            if !file.venue.contains_key(name) {
                let have = file.venue.keys().cloned().collect::<Vec<_>>().join(", ");
                bail!("venue `{name}` not in registry (have: {have})");
            }
        }

        let mut venues = BTreeMap::new();
        let mut exact_addrs: HashMap<String, Watch> = HashMap::new();
        let mut creds: HashMap<[u8; 28], Watch> = HashMap::new();

        for (name, cfg) in file.venue {
            if !selected.is_empty() && !selected.contains(&name) {
                continue;
            }
            let decoder =
                Decoder::parse(&cfg.decoder).with_context(|| format!("venue `{name}`"))?;
            venues.insert(
                name.clone(),
                VenueMeta {
                    decoder,
                    earliest_slot: cfg.earliest_slot,
                },
            );

            for (addrs, channel) in [
                (&cfg.sale_addrs, Channel::Sale),
                (&cfg.offer_addrs, Channel::Offer),
            ] {
                for addr in addrs {
                    exact_addrs.insert(
                        addr.clone(),
                        Watch {
                            venue: name.clone(),
                            channel,
                            decoder,
                        },
                    );
                }
            }
            for (cred_hexes, channel) in [
                (&cfg.sale_creds, Channel::Sale),
                (&cfg.offer_creds, Channel::Offer),
            ] {
                for c in cred_hexes {
                    let cred = parse_cred(c)
                        .with_context(|| format!("venue `{name}` credential `{c}`"))?;
                    creds.insert(
                        cred,
                        Watch {
                            venue: name.clone(),
                            channel,
                            decoder,
                        },
                    );
                }
            }
        }

        if venues.is_empty() {
            bail!("no venues enabled");
        }
        Ok(Self {
            venues,
            exact_addrs,
            creds,
        })
    }

    /// The lowest `earliest_slot` across enabled venues — the walk's fast-skip
    /// floor when no explicit start slot is given.
    pub fn min_earliest_slot(&self) -> u64 {
        self.venues
            .values()
            .map(|v| v.earliest_slot)
            .min()
            .unwrap_or(0)
    }

    /// Classify a Shelley bech32 address: exact match first (jpg), then payment
    /// credential (Wayup). `None` when the address is not watched.
    pub fn watch_for(&self, bech32: &str) -> Option<&Watch> {
        if let Some(w) = self.exact_addrs.get(bech32) {
            return Some(w);
        }
        let cred = payment_cred(bech32)?;
        self.creds.get(&cred)
    }

    /// Enabled venue names, sorted.
    pub fn venue_names(&self) -> impl Iterator<Item = &str> {
        self.venues.keys().map(String::as_str)
    }
}

/// Extract a Shelley address's 28-byte payment credential (key or script hash).
fn payment_cred(bech32: &str) -> Option<[u8; 28]> {
    let Ok(Address::Shelley(s)) = Address::from_bech32(bech32) else {
        return None;
    };
    Some(match s.payment() {
        ShelleyPaymentPart::Key(h) => **h,
        ShelleyPaymentPart::Script(h) => **h,
    })
}

fn parse_cred(hex_str: &str) -> Result<[u8; 28]> {
    let bytes = hex::decode(hex_str).context("credential is not hex")?;
    bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("credential must be 28 bytes, got {}", bytes.len()))
}

// --- TOML shape ---

#[derive(Debug, Deserialize)]
struct RegistryFile {
    venue: BTreeMap<String, VenueCfg>,
}

#[derive(Debug, Deserialize)]
struct VenueCfg {
    decoder: String,
    #[serde(default)]
    sale_addrs: Vec<String>,
    #[serde(default)]
    offer_addrs: Vec<String>,
    #[serde(default)]
    sale_creds: Vec<String>,
    #[serde(default)]
    offer_creds: Vec<String>,
    earliest_slot: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
        [venue.jpg]
        decoder = "jpg"
        sale_addrs = ["addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx"]
        earliest_slot = 33000000

        [venue.wayup]
        decoder = "wayup"
        sale_creds = ["a76f0fb801a29f591e9871576508d85b0b5f3c38774f65032f58fdad"]
        earliest_slot = 90000000
    "#;

    #[test]
    fn parses_and_matches_exact_and_cred() {
        let reg = VenueRegistry::parse(SAMPLE, &[]).unwrap();
        assert_eq!(reg.min_earliest_slot(), 33000000);
        // exact jpg sale address
        let jpg = reg
            .watch_for("addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx")
            .expect("jpg watched");
        assert_eq!(jpg.venue, "jpg");
        assert_eq!(jpg.channel, Channel::Sale);
        assert!(matches!(jpg.decoder, Decoder::Jpg));
        // an unrelated address is not watched
        assert!(
            reg.watch_for("addr1w999n67e47he8y0v36hjtzluargwu25zw94f6lqnm82aqqsg4xkcp")
                .is_none()
        );
    }

    #[test]
    fn selection_filters_and_validates() {
        let reg = VenueRegistry::parse(SAMPLE, &["jpg".to_string()]).unwrap();
        assert_eq!(reg.venue_names().collect::<Vec<_>>(), vec!["jpg"]);
        assert_eq!(reg.min_earliest_slot(), 33000000);
        assert!(VenueRegistry::parse(SAMPLE, &["nope".to_string()]).is_err());
    }
}
