use chrono::{DateTime, Utc};
use pons_domain::{
    BlockHash, BlockNumber, ChainId, ContractAddress, CurveAddress, LogIndex, TokenAddress, TxHash,
    WalletAddress,
};
use serde_json::Value;
use sqlx::PgPool;
use std::fmt::Write as _;
use thiserror::Error;
use uuid::Uuid;

use super::{EventOutboxRepository, NewOutboxEvent};

pub struct PersistTokenLaunch<'a> {
    pub deployment_id: Uuid,
    pub chain_id: ChainId,
    pub factory: ContractAddress,
    pub token: TokenAddress,
    pub curve: CurveAddress,
    pub deployer: WalletAddress,
    pub pair_token: TokenAddress,
    pub launch_config_id: &'a str,
    pub graduation_threshold: &'a str,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub transaction_index: Option<u64>,
    pub tx_hash: TxHash,
    pub log_index: LogIndex,
    pub topics: &'a Value,
    pub data: &'a [u8],
    pub launch_time: DateTime<Utc>,
    pub parser_version: i32,
    pub schema_version: i32,
    pub normalized_payload: &'a Value,
    pub outbox_payload: &'a Value,
}
pub struct RecordIngestionError<'a> {
    pub deployment_id: Uuid,
    pub chain_id: ChainId,
    pub block_number: BlockNumber,
    pub block_hash: BlockHash,
    pub tx_hash: TxHash,
    pub log_index: LogIndex,
    pub emitter: ContractAddress,
    pub topics: &'a Value,
    pub data: &'a [u8],
    pub parser_version: i32,
    pub schema_version: i32,
    pub error: &'a str,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistedLaunch {
    pub token_id: Uuid,
    pub raw_log_id: Uuid,
    pub event_id: Vec<u8>,
    pub outbox_seq: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredCurve {
    pub curve: CurveAddress,
    pub token: TokenAddress,
    pub token_id: Uuid,
    pub deployment_id: Uuid,
    pub launch_block: BlockNumber,
}
#[derive(Debug, Error)]
pub enum TokenLaunchPersistenceError {
    #[error("token launch conflicts with immutable token evidence")]
    TokenConflict,
    #[error("curve already maps to another token or token already maps to another curve")]
    CurveConflict,
    #[error("normalized event payload conflicts with this parser/schema version")]
    NormalizedConflict,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

#[derive(Clone, Debug)]
pub struct TokenLaunchRepository {
    pool: PgPool,
}
#[allow(clippy::missing_errors_doc)]
impl TokenLaunchRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn persist(
        &self,
        value: &PersistTokenLaunch<'_>,
    ) -> Result<PersistedLaunch, TokenLaunchPersistenceError> {
        let mut tx = self.pool.begin().await?;
        let raw_id:Uuid=sqlx::query_scalar(r"INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data,status) VALUES($1::numeric,$2::numeric,$3,$4,$5::numeric,$6,$7,$8,'PENDING') ON CONFLICT(chain_id,tx_hash,log_index) DO UPDATE SET tx_hash=EXCLUDED.tx_hash WHERE raw_chain_logs.block_number=EXCLUDED.block_number AND raw_chain_logs.block_hash=EXCLUDED.block_hash AND raw_chain_logs.address=EXCLUDED.address AND raw_chain_logs.topics=EXCLUDED.topics AND raw_chain_logs.data=EXCLUDED.data RETURNING id")
   .bind(value.chain_id.get().to_string()).bind(value.block_number.get().to_string()).bind(value.block_hash.as_bytes().as_slice()).bind(value.tx_hash.as_bytes().as_slice()).bind(value.log_index.get().to_string()).bind(value.factory.as_bytes().as_slice()).bind(value.topics).bind(value.data).fetch_one(&mut *tx).await?;
        let event_id:Vec<u8>=sqlx::query_scalar(r"INSERT INTO normalized_events(raw_log_id,chain_id,tx_hash,log_index,event_type,parser_version,schema_version,payload) VALUES($1,$2::numeric,$3,$4::numeric,'PONS_V2_TOKEN_LAUNCHED',$5,$6,$7) ON CONFLICT(raw_log_id,event_type,parser_version,schema_version) DO UPDATE SET payload=normalized_events.payload WHERE normalized_events.payload=EXCLUDED.payload RETURNING event_id")
   .bind(raw_id).bind(value.chain_id.get().to_string()).bind(value.tx_hash.as_bytes().as_slice()).bind(value.log_index.get().to_string()).bind(value.parser_version).bind(value.schema_version).bind(value.normalized_payload).fetch_optional(&mut *tx).await?.ok_or(TokenLaunchPersistenceError::NormalizedConflict)?;
        let token_id:Option<Uuid>=sqlx::query_scalar(r"INSERT INTO tokens(chain_id,address,curve_address,factory_address,deployer,pair_token,launch_tx,launch_block,launch_log_index,launch_time,lifecycle,deployment_id,launch_config_id,graduation_threshold_raw,launch_transaction_index,launch_raw_log_id,launch_normalized_event_id) VALUES($1::numeric,$2,$3,$4,$5,$6,$7,$8::numeric,$9::numeric,$10,'ACTIVE_CURVE',$11,$12,$13,$14::numeric,$15,$16) ON CONFLICT(chain_id,address) DO UPDATE SET address=EXCLUDED.address WHERE tokens.curve_address=EXCLUDED.curve_address AND tokens.factory_address=EXCLUDED.factory_address AND tokens.deployer=EXCLUDED.deployer AND tokens.pair_token=EXCLUDED.pair_token AND tokens.launch_tx=EXCLUDED.launch_tx AND tokens.launch_block=EXCLUDED.launch_block AND tokens.launch_log_index=EXCLUDED.launch_log_index AND tokens.deployment_id=EXCLUDED.deployment_id AND tokens.launch_config_id=EXCLUDED.launch_config_id AND tokens.graduation_threshold_raw=EXCLUDED.graduation_threshold_raw RETURNING id")
   .bind(value.chain_id.get().to_string()).bind(value.token.as_bytes().as_slice()).bind(value.curve.as_bytes().as_slice()).bind(value.factory.as_bytes().as_slice()).bind(value.deployer.as_bytes().as_slice()).bind(value.pair_token.as_bytes().as_slice()).bind(value.tx_hash.as_bytes().as_slice()).bind(value.block_number.get().to_string()).bind(value.log_index.get().to_string()).bind(value.launch_time).bind(value.deployment_id).bind(value.launch_config_id).bind(value.graduation_threshold).bind(value.transaction_index.map(|v|v.to_string())).bind(raw_id).bind(&event_id).fetch_optional(&mut *tx).await?;
        let token_id = token_id.ok_or(TokenLaunchPersistenceError::TokenConflict)?;
        let curve_ok:Option<i32>=sqlx::query_scalar(r"INSERT INTO pons_curves(chain_id,curve_address,token_id,token_address,deployment_id,launch_raw_log_id) VALUES($1::numeric,$2,$3,$4,$5,$6) ON CONFLICT(chain_id,curve_address) DO UPDATE SET curve_address=EXCLUDED.curve_address WHERE pons_curves.token_id=EXCLUDED.token_id AND pons_curves.token_address=EXCLUDED.token_address AND pons_curves.deployment_id=EXCLUDED.deployment_id RETURNING 1")
   .bind(value.chain_id.get().to_string()).bind(value.curve.as_bytes().as_slice()).bind(token_id).bind(value.token.as_bytes().as_slice()).bind(value.deployment_id).bind(raw_id).fetch_optional(&mut *tx).await?;
        if curve_ok.is_none() {
            return Err(TokenLaunchPersistenceError::CurveConflict);
        }
        sqlx::query(
            "INSERT INTO token_metadata_jobs(token_id,requested_block) VALUES($1,$2::numeric) ON CONFLICT(token_id) DO NOTHING",
        )
        .bind(token_id)
        .bind(value.block_number.get().to_string())
        .execute(&mut *tx)
        .await?;
        let dedupe = format!("token.launched:{}", hex(&event_id));
        let outbox = EventOutboxRepository::append_in_transaction(
            &mut tx,
            &NewOutboxEvent {
                event_type: "token.launched",
                schema_version: value.schema_version,
                aggregate_type: Some("token"),
                aggregate_id: Some(token_id),
                dedupe_key: &dedupe,
                payload: value.outbox_payload,
            },
        )
        .await?;
        tx.commit().await?;
        Ok(PersistedLaunch {
            token_id,
            raw_log_id: raw_id,
            event_id,
            outbox_seq: outbox.seq,
        })
    }
    pub async fn load_curves(
        &self,
        chain: ChainId,
    ) -> Result<Vec<(CurveAddress, TokenAddress)>, TokenLaunchPersistenceError> {
        let rows:Vec<(Vec<u8>,Vec<u8>)>=sqlx::query_as("SELECT curve_address,token_address FROM pons_curves WHERE chain_id=$1::numeric ORDER BY curve_address").bind(chain.get().to_string()).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|(curve, token)| {
                Ok((
                    CurveAddress::from_slice(&curve).map_err(decode)?,
                    TokenAddress::from_slice(&token).map_err(decode)?,
                ))
            })
            .collect()
    }
    pub async fn load_curve_records(
        &self,
        chain: ChainId,
    ) -> Result<Vec<StoredCurve>, TokenLaunchPersistenceError> {
        type CurveRow = (Vec<u8>, Vec<u8>, Uuid, Uuid, String);
        let rows: Vec<CurveRow> = sqlx::query_as("SELECT c.curve_address,c.token_address,c.token_id,c.deployment_id,t.launch_block::text FROM pons_curves c JOIN tokens t ON t.id=c.token_id WHERE c.chain_id=$1::numeric ORDER BY c.curve_address").bind(chain.get().to_string()).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|(curve, token, token_id, deployment_id, launch)| {
                Ok(StoredCurve {
                    curve: CurveAddress::from_slice(&curve).map_err(decode)?,
                    token: TokenAddress::from_slice(&token).map_err(decode)?,
                    token_id,
                    deployment_id,
                    launch_block: BlockNumber::new(launch.parse::<u64>().map_err(decode)?),
                })
            })
            .collect()
    }
    pub async fn record_error(
        &self,
        value: &RecordIngestionError<'_>,
    ) -> Result<(), TokenLaunchPersistenceError> {
        sqlx::query(r"INSERT INTO chain_ingestion_errors(deployment_id,chain_id,block_number,block_hash,tx_hash,log_index,emitter,topics,data,parser_version,schema_version,error) VALUES($1,$2::numeric,$3::numeric,$4,$5,$6::numeric,$7,$8,$9,$10,$11,$12) ON CONFLICT(deployment_id,chain_id,tx_hash,log_index,parser_version,schema_version) DO UPDATE SET error=EXCLUDED.error,topics=EXCLUDED.topics,data=EXCLUDED.data,observed_at=now()")
      .bind(value.deployment_id).bind(value.chain_id.get().to_string()).bind(value.block_number.get().to_string()).bind(value.block_hash.as_bytes().as_slice()).bind(value.tx_hash.as_bytes().as_slice()).bind(value.log_index.get().to_string()).bind(value.emitter.as_bytes().as_slice()).bind(value.topics).bind(value.data).bind(value.parser_version).bind(value.schema_version).bind(value.error).execute(&self.pool).await?;
        Ok(())
    }
}
fn hex(value: &[u8]) -> String {
    value.iter().fold(
        String::with_capacity(value.len() * 2),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        },
    )
}
fn decode(error: impl std::error::Error + Send + Sync + 'static) -> TokenLaunchPersistenceError {
    TokenLaunchPersistenceError::Database(sqlx::Error::Decode(Box::new(error)))
}
