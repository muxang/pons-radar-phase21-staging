use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress, LogIndex, TxHash};
use sqlx::PgPool;
use uuid::Uuid;

pub struct InsertRawLog<'a> {
    pub chain_id: ChainId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub log_index: LogIndex,
    pub address: ContractAddress,
    pub topics: &'a serde_json::Value,
    pub data: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct RawLogRepository {
    pool: PgPool,
}

impl RawLogRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a raw log once and returns its stable identifier.
    ///
    /// # Errors
    ///
    /// Returns a database error if the insert or existing-row lookup fails.
    pub async fn insert_idempotent(&self, log: &InsertRawLog<'_>) -> Result<Uuid, sqlx::Error> {
        sqlx::query_scalar(
            r"
            INSERT INTO raw_chain_logs
                (chain_id, block_number, block_hash, tx_hash, log_index, address, topics, data)
            VALUES ($1::numeric, $2::numeric, $3, $4, $5::numeric, $6, $7, $8)
            ON CONFLICT (chain_id, tx_hash, log_index)
            DO UPDATE SET tx_hash = EXCLUDED.tx_hash
            WHERE raw_chain_logs.block_number = EXCLUDED.block_number
              AND raw_chain_logs.block_hash = EXCLUDED.block_hash
              AND raw_chain_logs.address = EXCLUDED.address
              AND raw_chain_logs.topics = EXCLUDED.topics
              AND raw_chain_logs.data = EXCLUDED.data
            RETURNING id
            ",
        )
        .bind(log.chain_id.get().to_string())
        .bind(log.block_number.get().to_string())
        .bind(log.block_hash.as_bytes().as_slice())
        .bind(log.tx_hash.as_bytes().as_slice())
        .bind(log.log_index.get().to_string())
        .bind(log.address.as_bytes().as_slice())
        .bind(log.topics)
        .bind(log.data)
        .fetch_one(&self.pool)
        .await
    }
}
