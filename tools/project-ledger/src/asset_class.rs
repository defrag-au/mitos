//! CIP-67 asset-name labels — what kind of token this actually is.
//!
//! Without this the ledger double-counts. A CIP-68 collection mints **two**
//! tokens per NFT under one policy: the user token (label 222) that a holder
//! receives, and the reference token (label 100) that carries the metadata and
//! lives at a script address forever. Counting both as "assets out" doubles the
//! supply AND creates one enormous phantom holder — measured on Mekka S1:
//! 10,001 minted assets is really **5,000 NFTs**, with all 5,000 reference
//! tokens sitting on a single address while the real holder base is 157.
//!
//! Separately, some mints hand out a fungible token alongside the NFT. Labelled
//! fungibles (333/444) are classified here so they can be shown as their own
//! thing rather than inflating a holder count.
//!
//! Matching is on the 4-byte label prefix, the same way
//! `cardano_assets::NftPurpose` does it — deliberately NOT a general CIP-67
//! decode. Only these four labels are standardised, and an exact prefix match
//! cannot mis-fire on an unlabelled name that merely starts with a zero byte.

/// What a token is, for the purpose of "who holds the collection".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AssetClass {
    /// CIP-68 label 100 — metadata carrier. Lives at a script; NOT a holding.
    Reference,
    /// CIP-68 label 222 — the NFT a holder actually owns.
    UserNft,
    /// CIP-68 label 333 — fungible token.
    Ft,
    /// CIP-68 label 444 — rich fungible token.
    Rft,
    /// No CIP-67 label. A plain CIP-25 NFT, or an unlabelled token.
    Plain,
}

impl AssetClass {
    /// Classify from the raw on-chain asset-name bytes.
    pub fn of(name: &[u8]) -> Self {
        if name.len() < 4 {
            return Self::Plain;
        }
        match &name[..4] {
            [0x00, 0x06, 0x43, 0xb0] => Self::Reference,
            [0x00, 0x0d, 0xe1, 0x40] => Self::UserNft,
            [0x00, 0x14, 0xdf, 0x10] => Self::Ft,
            [0x00, 0x1b, 0xc2, 0x80] => Self::Rft,
            _ => Self::Plain,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::UserNft => "nft",
            Self::Ft => "ft",
            Self::Rft => "rft",
            Self::Plain => "plain",
        }
    }

    /// Whether owning this means being a holder of the collection.
    ///
    /// Reference tokens are excluded — they are metadata infrastructure, not
    /// ownership. Fungibles are excluded because a balance is not a holding of
    /// *the collection*; an airdropped token would otherwise inflate the
    /// holder base.
    pub fn is_holding(self) -> bool {
        matches!(self, Self::UserNft | Self::Plain)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hexname(s: &str) -> Vec<u8> {
        hex::decode(s).unwrap()
    }

    #[test]
    fn classifies_the_four_standard_labels() {
        // Real Mekka S1 names, straight from the ledger.
        assert_eq!(
            AssetClass::of(&hexname("000643b04d4430303031")),
            AssetClass::Reference
        );
        assert_eq!(
            AssetClass::of(&hexname("000de1404d4430303031")),
            AssetClass::UserNft
        );
        assert_eq!(AssetClass::of(&hexname("0014df1054657374")), AssetClass::Ft);
        assert_eq!(
            AssetClass::of(&hexname("001bc28054657374")),
            AssetClass::Rft
        );
    }

    #[test]
    fn only_the_user_token_counts_as_a_holding() {
        // This is the double-count fix: one CIP-68 NFT = one holding, not two.
        assert!(AssetClass::UserNft.is_holding());
        assert!(!AssetClass::Reference.is_holding());
        // An airdropped fungible is not a holding of the collection.
        assert!(!AssetClass::Ft.is_holding());
        assert!(!AssetClass::Rft.is_holding());
        // A plain CIP-25 NFT is.
        assert!(AssetClass::Plain.is_holding());
    }

    #[test]
    fn unlabelled_names_are_plain() {
        assert_eq!(AssetClass::of(b"MekkaDynasty001"), AssetClass::Plain);
        assert_eq!(AssetClass::of(b""), AssetClass::Plain);
        assert_eq!(AssetClass::of(b"abc"), AssetClass::Plain);
        // Starts with a zero byte but is not a standard label.
        assert_eq!(
            AssetClass::of(&hexname("00000000deadbeef")),
            AssetClass::Plain
        );
    }
}
