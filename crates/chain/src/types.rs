use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress, LogIndex, LogTopic, TxHash};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub timestamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainLog {
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    /// Transaction position in the block. Some RPC implementations omit this field.
    pub transaction_index: Option<u64>,
    pub log_index: LogIndex,
    pub address: ContractAddress,
    pub topics: Vec<LogTopic>,
    pub data: Vec<u8>,
    pub removed: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogFilter {
    pub addresses: Vec<ContractAddress>,
    pub topics: Vec<Option<Vec<LogTopic>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    pub tx_hash: TxHash,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub succeeded: bool,
    pub logs: Vec<ChainLog>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    NewHead(BlockNumber),
    Log(ChainLog),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngestionSource {
    Live,
    ChainBackfill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainBatch {
    pub source: IngestionSource,
    pub chain_id: ChainId,
    pub from_block: BlockNumber,
    pub to_block: BlockNumber,
    pub terminal_hash: BlockHash,
    pub logs: Vec<ChainLog>,
}
