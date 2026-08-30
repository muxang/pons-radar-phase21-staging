//! Strong, lossless domain primitives shared by storage and future chain modules.

mod identifiers;
mod numbers;

pub use identifiers::{
    BlockHash, ContractAddress, CurveAddress, IdentifierParseError, LogTopic, TokenAddress, TxHash,
    WalletAddress,
};
pub use numbers::{BlockNumber, ChainId, LogIndex, NormalizedAmount, RawAmount};
