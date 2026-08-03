//! Venue registry — the pluggability seam.
//!
//! Venues are declarative config, not code paths scattered through the walker.
//! A venue names its watched sale/offer addresses (jpg — exact bech32) or
//! payment credentials (Wayup — the per-seller staking part varies, so we match
//! the payment part), an `earliest_slot` fast-skip floor, and a `decoder`
//! (with the credentials that decoder's config needs). Adding a marketplace
//! later is a config edit plus (if genuinely new) its decoder in the crate.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use mitos_marketplace_decode::{WayupOfferConfig, WayupSaleConfig};
use pallas_addresses::{Address, ShelleyPaymentPart};
use serde::Deserialize;

/// What a watched address/credential is used for at a venue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    Sale,
    Offer,
}

/// A venue's decoder plus the config it needs. jpg classifies by hard-coded
/// address in the crate; Wayup needs its sale/fee/offer payment credentials.
pub enum VenueDecoder {
    Jpg,
    Wayup {
        sale: WayupSaleConfig,
        offer: WayupOfferConfig,
    },
}

/// A watched address/credential resolved to its venue + channel.
#[derive(Debug, Clone)]
pub struct Watch {
    pub venue: String,
    pub channel: Channel,
}

struct VenueMeta {
    decoder: VenueDecoder,
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
            let decoder = build_decoder(&name, &cfg)?;
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

    /// The decoder + config for a venue by name.
    pub fn decoder(&self, venue: &str) -> Option<&VenueDecoder> {
        self.venues.get(venue).map(|m| &m.decoder)
    }

    /// Enabled venue names, sorted.
    pub fn venue_names(&self) -> impl Iterator<Item = &str> {
        self.venues.keys().map(String::as_str)
    }
}

fn build_decoder(name: &str, cfg: &VenueCfg) -> Result<VenueDecoder> {
    match cfg.decoder.as_str() {
        "jpg" => Ok(VenueDecoder::Jpg),
        "wayup" => Ok(VenueDecoder::Wayup {
            sale: WayupSaleConfig::from_hex(
                cfg.sale_creds.first().map(String::as_str).unwrap_or(""),
                cfg.fee_cred.as_str(),
            ),
            offer: WayupOfferConfig::from_hex(
                cfg.offer_creds.first().map(String::as_str).unwrap_or(""),
            ),
        }),
        other => bail!("venue `{name}`: unknown decoder `{other}` (expected jpg|wayup)"),
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
    /// Fee-address payment credential (Wayup) — enables `fee_waived` detection.
    #[serde(default)]
    fee_cred: String,
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
        offer_creds = ["27d46ecbec94b052d8f875cf3beafd0e8ca40e8ad069f677e0a128ea"]
        earliest_slot = 90000000
    "#;

    #[test]
    fn parses_and_matches_exact_and_cred() {
        let reg = VenueRegistry::parse(SAMPLE, &[]).unwrap();
        assert_eq!(reg.min_earliest_slot(), 33000000);
        let jpg = reg
            .watch_for("addr1x8rjw3pawl0kelu4mj3c8x20fsczf5pl744s9mxz9v8n7efvjel5h55fgjcxgchp830r7h2l5msrlpt8262r3nvr8ekstg4qrx")
            .expect("jpg watched");
        assert_eq!(jpg.venue, "jpg");
        assert_eq!(jpg.channel, Channel::Sale);
        assert!(matches!(reg.decoder("jpg"), Some(VenueDecoder::Jpg)));
        assert!(matches!(
            reg.decoder("wayup"),
            Some(VenueDecoder::Wayup { .. })
        ));
    }

    #[test]
    fn selection_filters_and_validates() {
        let reg = VenueRegistry::parse(SAMPLE, &["jpg".to_string()]).unwrap();
        assert_eq!(reg.venue_names().collect::<Vec<_>>(), vec!["jpg"]);
        assert!(VenueRegistry::parse(SAMPLE, &["nope".to_string()]).is_err());
    }
}
