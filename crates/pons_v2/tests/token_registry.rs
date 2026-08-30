use async_trait::async_trait;
use pons_chain::{
    BackfillCoordinator, BackfillSettings, BatchHandler, BlockHeader, ChainBatch, ChainHealth,
    ChainLog, ChainRpc, ChainSubscription, IngestionSource, LogFilter, Receipt, ReconnectPolicy,
    RunError, SubscriptionEvent, SubscriptionFactory,
};
use pons_domain::{
    BlockHash, BlockNumber, ChainId, ContractAddress, CurveAddress, LogIndex, LogTopic, TxHash,
};
use pons_storage::repositories::{
    ChainCursorRepository, DeploymentChanges, DeploymentRepository, NewProtocolDeployment,
    TokenLaunchRepository,
};
use pons_v2::{
    CurveRegistry, TOKEN_LAUNCHED_PARSER_VERSION, TOKEN_LAUNCHED_SCHEMA_VERSION,
    TokenLaunchHandler, factory_log_filter,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    address: String,
    block_number: String,
    block_hash: String,
    block_timestamp: String,
    transaction_hash: String,
    transaction_index: String,
    log_index: String,
    removed: bool,
    topics: Vec<String>,
    data: String,
}
fn q(value: &str) -> u64 {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).unwrap()
}
fn fixture() -> (ChainLog, u64) {
    let value: Fixture = serde_json::from_str(include_str!(
        "../../../fixtures/pons_v2/token_launched_0xeaae3543.json"
    ))
    .unwrap();
    (
        ChainLog {
            block_number: BlockNumber::new(q(&value.block_number)),
            block_hash: value.block_hash.parse().unwrap(),
            tx_hash: value.transaction_hash.parse().unwrap(),
            transaction_index: Some(q(&value.transaction_index)),
            log_index: LogIndex::new(q(&value.log_index)),
            address: value.address.parse().unwrap(),
            topics: value
                .topics
                .into_iter()
                .map(|topic| topic.parse().unwrap())
                .collect(),
            data: alloy_primitives::hex::decode(value.data).unwrap(),
            removed: value.removed,
        },
        q(&value.block_timestamp),
    )
}
struct Rpc {
    head: u64,
    logs: Mutex<Vec<ChainLog>>,
    timestamp: u64,
}
#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(self.head))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
    }
    async fn block(&self, number: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        let hash = self
            .logs
            .lock()
            .unwrap()
            .iter()
            .find(|log| log.block_number == number)
            .map_or_else(
                || BlockHash::from_slice(&[0; 32]).unwrap(),
                |log| log.block_hash,
            );
        Ok(Some(BlockHeader {
            number,
            hash,
            parent_hash: BlockHash::from_slice(&[0; 32]).unwrap(),
            timestamp: self.timestamp,
        }))
    }
    async fn receipt(&self, _: TxHash) -> Result<Option<Receipt>, RunError> {
        Ok(None)
    }
    async fn logs(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        assert_eq!(filter.addresses.len(), 1);
        assert_eq!(filter.topics.len(), 1);
        Ok(self
            .logs
            .lock()
            .unwrap()
            .iter()
            .filter(|log| log.block_number >= from && log.block_number <= to)
            .cloned()
            .collect())
    }
}
struct Ws;
struct Sub;
#[async_trait]
impl ChainSubscription for Sub {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        std::future::pending().await
    }
}
#[async_trait]
impl SubscriptionFactory for Ws {
    async fn connect(
        &self,
        _: ChainId,
        _: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError> {
        Ok(Box::new(Sub))
    }
}
async fn deployment(pool: &PgPool, start: u64) -> pons_storage::repositories::ProtocolDeployment {
    let repository = DeploymentRepository::new(pool.clone());
    let topics = json!([]);
    let value = repository
        .create(&NewProtocolDeployment {
            chain_id: ChainId::new(4663),
            address: "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
                .parse()
                .unwrap(),
            start_block: BlockNumber::new(start),
            end_block: None,
            enabled: false,
            expected_event_topics: &topics,
            expected_code_hash: Some(BlockHash::from_slice(&[1; 32]).unwrap()),
            source: "fixture",
            interface_fingerprint: "pons-v2-factory:v1",
        })
        .await
        .unwrap();
    repository
        .save_verification(
            value.id,
            "VERIFIED",
            &json!({"code_hash_matches":true}),
            None,
        )
        .await
        .unwrap();
    repository
        .update(
            value.id,
            &DeploymentChanges {
                start_block: None,
                end_block: None,
                enabled: Some(true),
                expected_event_topics: None,
                expected_code_hash: None,
                source: None,
                interface_fingerprint: None,
            },
        )
        .await
        .unwrap()
        .unwrap()
}
async fn handler(
    pool: &PgPool,
    id: uuid::Uuid,
    rpc: Arc<Rpc>,
) -> (Arc<TokenLaunchHandler>, CurveRegistry) {
    let launches = TokenLaunchRepository::new(pool.clone());
    let curves = CurveRegistry::rebuild(&launches).await.unwrap();
    (
        Arc::new(TokenLaunchHandler::new(
            id,
            DeploymentRepository::new(pool.clone()),
            launches,
            rpc,
            curves.clone(),
        )),
        curves,
    )
}
fn batch(log: ChainLog) -> ChainBatch {
    ChainBatch {
        source: IngestionSource::Live,
        chain_id: ChainId::new(4663),
        from_block: log.block_number,
        to_block: log.block_number,
        terminal_hash: log.block_hash,
        logs: vec![log],
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn historical_backfill_persists_all_state_and_restart_rebuilds_curves(pool: PgPool) {
    let (log, timestamp) = fixture();
    let deployment = deployment(&pool, log.block_number.get()).await;
    let rpc = Arc::new(Rpc {
        head: log.block_number.get(),
        logs: Mutex::new(vec![log.clone()]),
        timestamp,
    });
    let (handler, curves) = handler(&pool, deployment.id, rpc.clone()).await;
    let worker = BackfillCoordinator::new(
        ChainId::new(4663),
        format!("pons-v2:{}:launches", deployment.id),
        rpc,
        Arc::new(Ws),
        ChainCursorRepository::new(pool.clone()),
        handler,
        factory_log_filter(&deployment),
        BackfillSettings {
            start_block: deployment.start_block,
            chunk_blocks: 1000,
        },
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        ChainHealth::default(),
    );
    worker.sync_once().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT lifecycle FROM tokens")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "ACTIVE_CURVE"
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM normalized_events WHERE event_type='PONS_V2_TOKEN_LAUNCHED' AND parser_version=$1 AND schema_version=$2").bind(TOKEN_LAUNCHED_PARSER_VERSION).bind(TOKEN_LAUNCHED_SCHEMA_VERSION).fetch_one(&pool).await.unwrap(),1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type='token.launched'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT requested_block::text FROM token_metadata_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        deployment.start_block.get().to_string()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM token_metadata_jobs WHERE status='PENDING'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let curve = "0x6393305a54b3b15819a3e8f693addadd2eebd021"
        .parse::<CurveAddress>()
        .unwrap();
    assert!(curves.token(curve).await.is_some());
    let rebuilt = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool))
        .await
        .unwrap();
    assert!(rebuilt.token(curve).await.is_some());
}
#[sqlx::test(migrations = "../../migrations")]
async fn backfill_and_live_duplicate_are_idempotent(pool: PgPool) {
    let (log, timestamp) = fixture();
    let deployment = deployment(&pool, log.block_number.get()).await;
    let rpc = Arc::new(Rpc {
        head: log.block_number.get(),
        logs: Mutex::new(vec![log.clone()]),
        timestamp,
    });
    let (handler, _) = handler(&pool, deployment.id, rpc).await;
    handler.handle(batch(log.clone())).await.unwrap();
    handler.handle(batch(log)).await.unwrap();
    for table in [
        "raw_chain_logs",
        "normalized_events",
        "tokens",
        "pons_curves",
        "event_outbox",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "{table}");
    }
}
fn address_topic(address: ContractAddress) -> LogTopic {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_bytes());
    LogTopic::from_slice(&bytes).unwrap()
}
#[sqlx::test(migrations = "../../migrations")]
async fn token_and_curve_conflicts_fail_closed(pool: PgPool) {
    let (log, timestamp) = fixture();
    let deployment = deployment(&pool, log.block_number.get()).await;
    let rpc = Arc::new(Rpc {
        head: log.block_number.get(),
        logs: Mutex::new(vec![log.clone()]),
        timestamp,
    });
    let (handler, _) = handler(&pool, deployment.id, rpc).await;
    handler.handle(batch(log.clone())).await.unwrap();
    let mut token_conflict = log.clone();
    token_conflict.topics[2] = address_topic(ContractAddress::from_slice(&[8; 20]).unwrap());
    token_conflict.tx_hash = TxHash::from_slice(&[8; 32]).unwrap();
    token_conflict.log_index = LogIndex::new(8);
    assert!(handler.handle(batch(token_conflict)).await.is_err());
    let mut curve_conflict = log;
    curve_conflict.topics[1] = address_topic(ContractAddress::from_slice(&[7; 20]).unwrap());
    curve_conflict.tx_hash = TxHash::from_slice(&[7; 32]).unwrap();
    curve_conflict.log_index = LogIndex::new(7);
    assert!(handler.handle(batch(curve_conflict)).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM tokens")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw_chain_logs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
#[sqlx::test(migrations = "../../migrations")]
async fn handler_failure_does_not_advance_cursor(pool: PgPool) {
    let (mut log, timestamp) = fixture();
    log.data.truncate(32);
    let deployment = deployment(&pool, log.block_number.get()).await;
    let rpc = Arc::new(Rpc {
        head: log.block_number.get(),
        logs: Mutex::new(vec![log]),
        timestamp,
    });
    let (handler, _) = handler(&pool, deployment.id, rpc.clone()).await;
    let stream = format!("pons-v2:{}:launches", deployment.id);
    let worker = BackfillCoordinator::new(
        ChainId::new(4663),
        &stream,
        rpc,
        Arc::new(Ws),
        ChainCursorRepository::new(pool.clone()),
        handler,
        factory_log_filter(&deployment),
        BackfillSettings {
            start_block: deployment.start_block,
            chunk_blocks: 1000,
        },
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        ChainHealth::default(),
    );
    assert!(worker.sync_once().await.is_err());
    assert!(
        ChainCursorRepository::new(pool.clone())
            .get(&stream)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM chain_ingestion_errors")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}
