use chrono::{DateTime, Utc};
use pons_domain::{BlockHash, BlockNumber, ChainId};
use sqlx::{FromRow, PgPool};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainCursor {
    pub stream: String,
    pub chain_id: ChainId,
    pub last_processed_block: BlockNumber,
    pub last_processed_hash: BlockHash,
    pub updated_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct CursorRow {
    stream: String,
    chain_id: String,
    last_processed_block: String,
    last_processed_hash: Vec<u8>,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct ChainCursorRepository {
    pool: PgPool,
}

impl ChainCursorRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Loads the durable cursor for one logical stream.
    ///
    /// # Errors
    ///
    /// Returns a database or stored-domain decoding error.
    pub async fn get(&self, stream: &str) -> Result<Option<ChainCursor>, sqlx::Error> {
        let row: Option<CursorRow> = sqlx::query_as(
            r"
            SELECT stream, chain_id::text AS chain_id,
                   last_processed_block::text AS last_processed_block,
                   last_processed_hash, updated_at
            FROM chain_cursors WHERE stream = $1
            ",
        )
        .bind(stream)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(ChainCursor {
                stream: row.stream,
                chain_id: ChainId::new(row.chain_id.parse().map_err(decode_error)?),
                last_processed_block: BlockNumber::new(
                    row.last_processed_block.parse().map_err(decode_error)?,
                ),
                last_processed_hash: BlockHash::from_slice(&row.last_processed_hash)
                    .map_err(decode_error)?,
                updated_at: row.updated_at,
            })
        })
        .transpose()
    }

    /// Advances or creates a stream cursor without permitting its chain to change.
    ///
    /// # Errors
    ///
    /// Returns an error if persistence fails or the stream already belongs to another chain.
    pub async fn upsert(
        &self,
        stream: &str,
        chain_id: ChainId,
        block: BlockNumber,
        hash: BlockHash,
    ) -> Result<(), sqlx::Error> {
        let result = sqlx::query(
            r"
            INSERT INTO chain_cursors
                (stream, chain_id, last_processed_block, last_processed_hash)
            VALUES ($1, $2::numeric, $3::numeric, $4)
            ON CONFLICT (stream) DO UPDATE SET
                last_processed_block = EXCLUDED.last_processed_block,
                last_processed_hash = EXCLUDED.last_processed_hash,
                updated_at = now()
            WHERE chain_cursors.chain_id = EXCLUDED.chain_id
            ",
        )
        .bind(stream)
        .bind(chain_id.get().to_string())
        .bind(block.get().to_string())
        .bind(hash.as_bytes().as_slice())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            Ok(())
        } else {
            Err(sqlx::Error::RowNotFound)
        }
    }
}

fn decode_error(error: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}
