use async_trait::async_trait;
use pons_domain::{BlockNumber, ChainId, ContractAddress, TxHash};

use crate::{BlockHeader, ChainBatch, ChainLog, LogFilter, Receipt, RunError, SubscriptionEvent};

#[async_trait]
pub trait ChainRpc: Send + Sync {
    async fn chain_id(&self) -> Result<ChainId, RunError>;
    async fn block_number(&self) -> Result<BlockNumber, RunError>;
    async fn code(&self, address: ContractAddress) -> Result<Vec<u8>, RunError>;
    async fn call(
        &self,
        address: ContractAddress,
        data: Vec<u8>,
        block: BlockNumber,
    ) -> Result<Vec<u8>, RunError> {
        let _ = (address, data, block);
        Err(RunError::Rpc(
            "eth_call is not supported by this provider".into(),
        ))
    }
    async fn block(&self, number: BlockNumber) -> Result<Option<BlockHeader>, RunError>;
    async fn receipt(&self, hash: TxHash) -> Result<Option<Receipt>, RunError>;
    async fn logs(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError>;
}

#[async_trait]
pub trait ChainSubscription: Send {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>>;
}

#[async_trait]
pub trait SubscriptionFactory: Send + Sync {
    async fn connect(
        &self,
        expected_chain_id: ChainId,
        filter: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError>;
}

#[async_trait]
pub trait BatchHandler: Send + Sync {
    async fn handle(&self, batch: ChainBatch) -> Result<(), RunError>;
}
