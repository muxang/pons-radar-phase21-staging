use crate::Receipt;
use alloy_primitives::U256;
use pons_domain::{LogIndex, LogTopic, TokenAddress, TxHash, WalletAddress};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransferEvidence {
    pub token: TokenAddress,
    pub from: WalletAddress,
    pub to: WalletAddress,
    pub amount_raw: String,
    pub log_index: LogIndex,
    pub tx_hash: TxHash,
}
#[derive(Debug, Error)]
pub enum TransferEvidenceError {
    #[error("malformed ERC20 Transfer log at index {0}")]
    Malformed(u64),
    #[error("invalid Transfer identifier: {0}")]
    Identifier(String),
}
/// Returns the canonical ERC20 Transfer signature topic.
///
/// # Panics
/// The compile-time constant is valid fixed-width hexadecimal and cannot fail to parse.
#[must_use]
pub fn transfer_topic() -> LogTopic {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .parse()
        .expect("standard ERC20 Transfer topic")
}
/// Extracts strict, protocol-independent ERC20 Transfer logs from a receipt.
///
/// # Errors
/// Returns an error when a log claims to be Transfer but has malformed indexed fields or data.
pub fn extract_erc20_transfers(
    receipt: &Receipt,
) -> Result<Vec<TransferEvidence>, TransferEvidenceError> {
    receipt
        .logs
        .iter()
        .filter(|v| v.topics.first() == Some(&transfer_topic()))
        .map(|v| {
            if v.topics.len() != 3 || v.data.len() != 32 {
                return Err(TransferEvidenceError::Malformed(v.log_index.get()));
            }
            let from = WalletAddress::from_slice(&v.topics[1].as_bytes()[12..])
                .map_err(|e| TransferEvidenceError::Identifier(e.to_string()))?;
            let to = WalletAddress::from_slice(&v.topics[2].as_bytes()[12..])
                .map_err(|e| TransferEvidenceError::Identifier(e.to_string()))?;
            let token = TokenAddress::from_slice(v.address.as_bytes())
                .map_err(|e| TransferEvidenceError::Identifier(e.to_string()))?;
            Ok(TransferEvidence {
                token,
                from,
                to,
                amount_raw: U256::from_be_slice(&v.data).to_string(),
                log_index: v.log_index,
                tx_hash: v.tx_hash,
            })
        })
        .collect()
}
