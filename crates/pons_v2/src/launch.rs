use std::{collections::HashMap, sync::Arc};

use alloy_primitives::{Address, B256, Bytes, Log};
use alloy_sol_types::{SolEvent, sol};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pons_chain::{
    BatchHandler, ChainBatch, ChainLog, ChainRpc, LogFilter, ROBINHOOD_CHAIN_ID, RunError,
};
use pons_domain::{BlockNumber, ChainId, CurveAddress, LogTopic, TokenAddress, WalletAddress};
use pons_storage::repositories::{
    DeploymentRepository, PersistTokenLaunch, ProtocolDeployment, RecordIngestionError,
    StoredCurve, TokenLaunchRepository,
};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::RwLock;

pub const TOKEN_LAUNCHED_PARSER_VERSION: i32 = 1;
pub const TOKEN_LAUNCHED_SCHEMA_VERSION: i32 = 1;

sol! { event TokenLaunched(address indexed token,address indexed curve,address indexed deployer,address pairToken,uint256 launchConfigId,uint256 graduationThreshold); }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedTokenLaunch {
    pub token: TokenAddress,
    pub curve: CurveAddress,
    pub deployer: WalletAddress,
    pub pair_token: TokenAddress,
    pub launch_config_id: String,
    pub graduation_threshold: String,
}
#[derive(Debug, Error)]
pub enum LaunchError {
    #[error("deployment is not an active trusted Pons V2 factory at block {0}")]
    InactiveDeployment(u64),
    #[error("log emitter does not match deployment factory")]
    WrongEmitter,
    #[error("removed log cannot create a token")]
    Removed,
    #[error("invalid TokenLaunched ABI: {0}")]
    Decode(String),
    #[error("block {0} unavailable for launch timestamp")]
    MissingBlock(u64),
    #[error("invalid block timestamp {0}")]
    InvalidTimestamp(u64),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("curve registry conflict")]
    CurveConflict,
}

#[must_use]
pub fn token_launched_topic() -> LogTopic {
    LogTopic::new(TokenLaunched::SIGNATURE_HASH)
}
#[must_use]
pub fn factory_log_filter(deployment: &ProtocolDeployment) -> LogFilter {
    LogFilter {
        addresses: vec![deployment.address],
        topics: vec![Some(vec![token_launched_topic()])],
    }
}

/// Strictly decodes a launch only after emitter and deployment-scope validation.
///
/// # Errors
///
/// Returns an explanatory error for inactive deployments, removed logs, emitter
/// mismatch, or any non-canonical ABI encoding.
pub fn decode_token_launched(
    log: &ChainLog,
    deployment: &ProtocolDeployment,
) -> Result<DecodedTokenLaunch, LaunchError> {
    if log.removed {
        return Err(LaunchError::Removed);
    }
    if log.address != deployment.address {
        return Err(LaunchError::WrongEmitter);
    }
    if !deployment_active_at(deployment, log.block_number) {
        return Err(LaunchError::InactiveDeployment(log.block_number.get()));
    }
    let address = Address::from_slice(log.address.as_bytes());
    let topics: Vec<B256> = log
        .topics
        .iter()
        .map(|v| B256::from_slice(v.as_bytes()))
        .collect();
    let alloy_log = Log::new(address, topics, Bytes::copy_from_slice(&log.data))
        .ok_or_else(|| LaunchError::Decode("more than four topics".into()))?;
    let decoded = TokenLaunched::decode_log_validate(&alloy_log)
        .map_err(|error| LaunchError::Decode(error.to_string()))?
        .data;
    Ok(DecodedTokenLaunch {
        token: TokenAddress::from_slice(decoded.token.as_slice())
            .map_err(|e| LaunchError::Decode(e.to_string()))?,
        curve: CurveAddress::from_slice(decoded.curve.as_slice())
            .map_err(|e| LaunchError::Decode(e.to_string()))?,
        deployer: WalletAddress::from_slice(decoded.deployer.as_slice())
            .map_err(|e| LaunchError::Decode(e.to_string()))?,
        pair_token: TokenAddress::from_slice(decoded.pairToken.as_slice())
            .map_err(|e| LaunchError::Decode(e.to_string()))?,
        launch_config_id: decoded.launchConfigId.to_string(),
        graduation_threshold: decoded.graduationThreshold.to_string(),
    })
}

fn deployment_active_at(value: &ProtocolDeployment, block: BlockNumber) -> bool {
    value.protocol == "PONS"
        && value.generation == "V2"
        && value.chain_id == ChainId::new(ROBINHOOD_CHAIN_ID)
        && value.enabled
        && value.health == "VERIFIED"
        && (value.trust_basis == "PINNED_CODE_HASH"
            || (value.trust_basis == "OPERATOR_APPROVED" && value.approved_by.is_some()))
        && value.start_block <= block
        && value.end_block.is_none_or(|end| block <= end)
}

#[derive(Clone, Default)]
pub struct CurveRegistry(Arc<RwLock<HashMap<CurveAddress, StoredCurve>>>);
impl CurveRegistry {
    /// Rebuilds the in-memory curve lookup from its durable `PostgreSQL` mapping.
    ///
    /// # Errors
    ///
    /// Returns an error for persistence decoding or conflicting durable mappings.
    pub async fn rebuild(repository: &TokenLaunchRepository) -> Result<Self, LaunchError> {
        let values = repository
            .load_curve_records(ChainId::new(ROBINHOOD_CHAIN_ID))
            .await
            .map_err(|e| LaunchError::Storage(e.to_string()))?;
        let registry = Self::default();
        {
            let mut state = registry.0.write().await;
            for record in values {
                if state.insert(record.curve, record).is_some() {
                    return Err(LaunchError::CurveConflict);
                }
            }
        }
        Ok(registry)
    }
    pub async fn token(&self, curve: CurveAddress) -> Option<TokenAddress> {
        self.0.read().await.get(&curve).map(|value| value.token)
    }
    pub async fn curve(&self, curve: CurveAddress) -> Option<StoredCurve> {
        self.0.read().await.get(&curve).cloned()
    }
    pub async fn records(&self) -> Vec<StoredCurve> {
        self.0.read().await.values().cloned().collect()
    }
    async fn register(&self, record: StoredCurve) -> Result<(), LaunchError> {
        let mut state = self.0.write().await;
        if state
            .get(&record.curve)
            .is_some_and(|existing| existing != &record)
        {
            return Err(LaunchError::CurveConflict);
        }
        state.insert(record.curve, record);
        Ok(())
    }
}

pub struct TokenLaunchHandler {
    deployment_id: uuid::Uuid,
    deployments: DeploymentRepository,
    launches: TokenLaunchRepository,
    rpc: Arc<dyn ChainRpc>,
    curves: CurveRegistry,
}
impl TokenLaunchHandler {
    #[must_use]
    pub fn new(
        deployment_id: uuid::Uuid,
        deployments: DeploymentRepository,
        launches: TokenLaunchRepository,
        rpc: Arc<dyn ChainRpc>,
        curves: CurveRegistry,
    ) -> Self {
        Self {
            deployment_id,
            deployments,
            launches,
            rpc,
            curves,
        }
    }
}

#[async_trait]
impl BatchHandler for TokenLaunchHandler {
    async fn handle(&self, batch: ChainBatch) -> Result<(), RunError> {
        let deployment = self
            .deployments
            .get(self.deployment_id)
            .await
            .map_err(handler)?
            .ok_or_else(|| RunError::Handler("deployment disappeared".into()))?;
        for log in batch.logs {
            let topics = Value::Array(
                log.topics
                    .iter()
                    .map(|topic| Value::String(topic.to_string()))
                    .collect(),
            );
            let decoded = match decode_token_launched(&log, &deployment) {
                Ok(decoded) => decoded,
                Err(error) => {
                    self.launches
                        .record_error(&RecordIngestionError {
                            deployment_id: deployment.id,
                            chain_id: batch.chain_id,
                            block_number: log.block_number,
                            block_hash: log.block_hash,
                            tx_hash: log.tx_hash,
                            log_index: log.log_index,
                            emitter: log.address,
                            topics: &topics,
                            data: &log.data,
                            parser_version: TOKEN_LAUNCHED_PARSER_VERSION,
                            schema_version: TOKEN_LAUNCHED_SCHEMA_VERSION,
                            error: &error.to_string(),
                        })
                        .await
                        .map_err(handler)?;
                    return Err(handler(error));
                }
            };
            let block = self
                .rpc
                .block(log.block_number)
                .await?
                .ok_or(LaunchError::MissingBlock(log.block_number.get()))
                .map_err(handler)?;
            if block.hash != log.block_hash {
                return Err(RunError::Handler("launch log block hash mismatch".into()));
            }
            let launch_time = DateTime::<Utc>::from_timestamp(
                i64::try_from(block.timestamp)
                    .map_err(|_| handler(LaunchError::InvalidTimestamp(block.timestamp)))?,
                0,
            )
            .ok_or(LaunchError::InvalidTimestamp(block.timestamp))
            .map_err(handler)?;
            let normalized = launch_json(&decoded, &deployment, &log, launch_time);
            let persisted = self
                .launches
                .persist(&PersistTokenLaunch {
                    deployment_id: deployment.id,
                    chain_id: batch.chain_id,
                    factory: deployment.address,
                    token: decoded.token,
                    curve: decoded.curve,
                    deployer: decoded.deployer,
                    pair_token: decoded.pair_token,
                    launch_config_id: &decoded.launch_config_id,
                    graduation_threshold: &decoded.graduation_threshold,
                    block_number: log.block_number,
                    block_hash: log.block_hash,
                    transaction_index: log.transaction_index,
                    tx_hash: log.tx_hash,
                    log_index: log.log_index,
                    topics: &topics,
                    data: &log.data,
                    launch_time,
                    parser_version: TOKEN_LAUNCHED_PARSER_VERSION,
                    schema_version: TOKEN_LAUNCHED_SCHEMA_VERSION,
                    normalized_payload: &normalized,
                    outbox_payload: &normalized,
                })
                .await
                .map_err(handler)?;
            self.curves
                .register(StoredCurve {
                    curve: decoded.curve,
                    token: decoded.token,
                    token_id: persisted.token_id,
                    deployment_id: deployment.id,
                    launch_block: log.block_number,
                })
                .await
                .map_err(handler)?;
        }
        Ok(())
    }
}

fn launch_json(
    value: &DecodedTokenLaunch,
    deployment: &ProtocolDeployment,
    log: &ChainLog,
    time: DateTime<Utc>,
) -> Value {
    json!({"deployment_id":deployment.id,"factory":deployment.address.to_string(),"token":value.token.to_string(),"curve":value.curve.to_string(),"deployer":value.deployer.to_string(),"pair_token":value.pair_token.to_string(),"launch_config_id":value.launch_config_id,"graduation_threshold":value.graduation_threshold,"launch_block":log.block_number.get(),"launch_time":time,"tx_hash":log.tx_hash.to_string(),"transaction_index":log.transaction_index,"log_index":log.log_index.get()})
}
fn handler(error: impl std::fmt::Display) -> RunError {
    RunError::Handler(error.to_string())
}
