//! Axo: an order book whose orders trade against each other in place.
//!
//! MEASURED on $NikePig (2026-10-27), from decoded transactions:
//!
//! - an order (a "strategy") is a UTxO at an Axo script, under its OWNER's
//!   stake credential, holding the owner's ADA and token;
//! - a trade (`"Axo: Trade"`) spends two orders and swaps ADA and the token
//!   between them: both continue at their own addresses, one owner having
//!   bought and the other sold;
//! - closing an order (`"Axo: Closed by Owner"`, `"Closed by Protocol /
//!   Trade Completed"`) pays its contents to the wallet carrying the same
//!   stake: a withdrawal or the delivery of what it bought, never a trade.
//!
//! # ⚠️ The list is what $NikePig's history named, not all of Axo
//!
//! [`ORDER_CREDS`] holds every script whose transactions carried an `"Axo:"`
//! CIP-20 message on $NikePig. The first is Axo's general contract (it held
//! ten different tokens when sampled); the rest each hold one, mostly under a
//! single stake, and look like individual traders' parameterised strategies.
//! Another token's Axo traders can use scripts not listed here, and those
//! read as unclaimed until added.
pub const ORDER_CREDS: [[u8; 28]; 20] = [
    crate::cred("55ff0e63efa0694e8065122c552e80c7b51768b7f20917af25752a7c"),
    crate::cred("c2c756eb9b1c8ac5d376b6ae7f474931bdecf40279b73ad4de57d0e7"),
    crate::cred("66a6bfe25e7e73c6c27ebbc1d13dd18b74da6eab003762e809a65c8f"),
    crate::cred("2e36ac845a6f847d76acf553ed52c28b1fd1e8cee39ca5ba8c2e91d9"),
    crate::cred("db39df384ce0bb895d1601b1fbb876afbc3ead6bf60a1e94ded9a069"),
    crate::cred("6b35255f5a768be1dd664d85529743e8632153b1a24621a7c20c9bea"),
    crate::cred("d31d1f8e727ce3d94d791ea6fb1502ec9283f5d86e54a4da4fde8b25"),
    crate::cred("dfbb7ee1edcea9d3cf87e2e42ed2ea1071eb8a670ce654f229c8d96c"),
    crate::cred("2fbcfa4fa5a53553c644fb2fc4162206b526be60b86b881ffe57241f"),
    crate::cred("fcc896deb55dd1befccc6246e886c1f08def3fe673d0009008222684"),
    crate::cred("f731111234a2508a3fa55c1897b3e07e5e5892d4115c2336ad277aef"),
    crate::cred("c8b1252870525ab48a7d90ef29132f252b8391db63565d08cceddb66"),
    crate::cred("6de585ce16c7a4a101f55c06327d1501d65d54cba96afb4be66573bd"),
    crate::cred("486fd50580610334b3f59a851aead250627b8c72b2018a24e35918d2"),
    crate::cred("765fb5aeefdcd6a598ee998091c05d37241d4e6b761bd2a5032c2934"),
    crate::cred("0c98fd7b3590ad664e4589ee484c6ec14eada08ee3f557706a62eab1"),
    crate::cred("8f2fee1e5be61e3d58368a7d10db6eba60543bbe63885d7754a47155"),
    crate::cred("0116b17c1f359bf0190cff4abf0ffc1adac8cc05288e0691f206a448"),
    crate::cred("9fd41da27127452544bb441905af003d6bc1ea1220eaa336d622121c"),
    crate::cred("394ff8e31ad31a21876a22b68b99d187178b6341a0f70686b3fa0649"),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// No script is listed twice.
    #[test]
    fn every_order_credential_is_distinct() {
        let mut seen = ORDER_CREDS.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), ORDER_CREDS.len());
    }

    #[test]
    fn the_general_contract_decodes_from_its_hex() {
        assert_eq!(
            hex::encode(ORDER_CREDS[0]),
            "55ff0e63efa0694e8065122c552e80c7b51768b7f20917af25752a7c"
        );
    }
}
