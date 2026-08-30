//! Protocol-independent Robinhood Chain transport and gap-recovery infrastructure.

mod coordinator;
mod health;
mod http;
mod startup;
mod traits;
mod transfers;
mod types;
mod ws;

pub use coordinator::{BackfillCoordinator, BackfillSettings, ReconnectPolicy, RunError};
pub use health::{ChainHealth, ChainHealthSnapshot, RpcStatus};
pub use http::HttpRpcProvider;
pub use startup::{WsChainProbe, WsStartupState, probe_ws_startup, reconnect_ws_until_ready};
pub use traits::{BatchHandler, ChainRpc, ChainSubscription, SubscriptionFactory};
pub use transfers::{
    TransferEvidence, TransferEvidenceError, extract_erc20_transfers, transfer_topic,
};
pub use types::{
    BlockHeader, ChainBatch, ChainLog, IngestionSource, LogFilter, Receipt, SubscriptionEvent,
};
pub use ws::WsRpcProvider;

pub const ROBINHOOD_CHAIN_ID: u64 = 4663;

/// Validates a chain endpoint against the configured network identity.
///
/// # Errors
///
/// Returns a transport error or fails closed when the chain ID differs.
pub async fn verify_chain_id(
    provider: &dyn ChainRpc,
    expected: pons_domain::ChainId,
) -> Result<(), RunError> {
    let actual = provider.chain_id().await?;
    if actual == expected {
        Ok(())
    } else {
        Err(RunError::WrongChain { expected, actual })
    }
}
