use alloy_primitives::U256;
use async_trait::async_trait;
use chrono::{TimeDelta, Utc};
use pons_chain::{
    BatchHandler, BlockHeader, ChainBatch, ChainLog, ChainRpc, IngestionSource, LogFilter, Receipt,
    RunError, extract_erc20_transfers, transfer_topic,
};
use pons_domain::{
    BlockHash, BlockNumber, ChainId, ContractAddress, CurveAddress, LogIndex, LogTopic,
    TokenAddress, TxHash, WalletAddress,
};
use pons_storage::repositories::{
    ConfirmationRepository, StoredCurve, TokenLaunchRepository, TradeCandidateIdentity,
    TradeRepository,
};
use pons_v2::{
    ConfirmationWorkerSettings, CurveRegistry, CurveTradeHandler, TradeCandidateMatcher,
    TradeConfirmationWorker, curve_buy_topic, curve_sell_topic,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

const TOKEN: &str = "0x175b585de47f960530b9e8b54829027283e966ae";
const CURVE: &str = "0xda8eea02ce81b9f274f428f3409ebd61244c7825";
const TRACKED: &str = "0x07b9aad1e7b7697a629bb511cd959d814a92e4b9";
const OTHER: &str = "0xe33e9e479df8802cb0866d5d05258bec4cf62948";
const HASH: &str = "0xc0830d47f19c628f18b33e9bf39a1e9330f82621ab36528b59b660c7c1e3b875";
const TX: &str = "0x50c4e554b198ee9fa764f8cc64cab72ada9d3f1cc4efe521af4570d8e888371c";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    transaction_hash: String,
    transaction_from: String,
    block_number: String,
    block_hash: String,
    status: String,
    token: String,
    curve: String,
    buyer: String,
    recipient: String,
    tokens_out_raw: String,
    logs: Vec<FixtureLog>,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureLog {
    address: String,
    topics: Vec<String>,
    data: String,
    log_index: String,
}
fn qty(v: &str) -> u64 {
    u64::from_str_radix(v.trim_start_matches("0x"), 16).unwrap()
}
fn bytes(v: &str) -> Vec<u8> {
    let v = v.trim_start_matches("0x");
    (0..v.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&v[i..i + 2], 16).unwrap())
        .collect()
}
fn topic_address(v: &str) -> LogTopic {
    let v: WalletAddress = v.parse().unwrap();
    let mut b = [0; 32];
    b[12..].copy_from_slice(v.as_bytes());
    LogTopic::from_slice(&b).unwrap()
}
fn data(values: &[U256]) -> Vec<u8> {
    values.iter().flat_map(U256::to_be_bytes::<32>).collect()
}
fn curve_log(side: &str, actor: &str, recipient: &str, amount: U256) -> ChainLog {
    let values = if side == "BUY" {
        [U256::from(10), amount, U256::ONE, U256::ZERO]
    } else {
        [amount, U256::from(10), U256::ONE, U256::ZERO]
    };
    ChainLog {
        block_number: BlockNumber::new(100),
        block_hash: HASH.parse().unwrap(),
        tx_hash: TX.parse().unwrap(),
        transaction_index: Some(1),
        log_index: LogIndex::new(7),
        address: CURVE.parse().unwrap(),
        topics: vec![
            if side == "BUY" {
                curve_buy_topic()
            } else {
                curve_sell_topic()
            },
            topic_address(actor),
            topic_address(recipient),
        ],
        data: data(&values),
        removed: false,
    }
}
fn transfer(from: &str, to: &str, amount: U256, index: u64) -> ChainLog {
    ChainLog {
        block_number: BlockNumber::new(100),
        block_hash: HASH.parse().unwrap(),
        tx_hash: TX.parse().unwrap(),
        transaction_index: Some(1),
        log_index: LogIndex::new(index),
        address: TOKEN.parse().unwrap(),
        topics: vec![transfer_topic(), topic_address(from), topic_address(to)],
        data: amount.to_be_bytes::<32>().to_vec(),
        removed: false,
    }
}

#[test]
fn real_receipt_fixture_extracts_exact_curve_to_recipient_transfer() {
    let f: Fixture = serde_json::from_str(include_str!(
        "../../../fixtures/pons_v2/curve_buy_receipt_0x50c4.json"
    ))
    .unwrap();
    let receipt = Receipt {
        tx_hash: f.transaction_hash.parse().unwrap(),
        block_number: BlockNumber::new(qty(&f.block_number)),
        block_hash: f.block_hash.parse().unwrap(),
        succeeded: qty(&f.status) == 1,
        logs: f
            .logs
            .into_iter()
            .map(|v| ChainLog {
                block_number: BlockNumber::new(qty(&f.block_number)),
                block_hash: f.block_hash.parse().unwrap(),
                tx_hash: f.transaction_hash.parse().unwrap(),
                transaction_index: Some(1),
                log_index: LogIndex::new(qty(&v.log_index)),
                address: v.address.parse().unwrap(),
                topics: v.topics.into_iter().map(|v| v.parse().unwrap()).collect(),
                data: bytes(&v.data),
                removed: false,
            })
            .collect(),
    };
    let values = extract_erc20_transfers(&receipt).unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].token.to_string(), f.token);
    assert_eq!(values[0].from.to_string(), f.curve);
    assert_eq!(values[0].to.to_string(), f.recipient);
    assert_eq!(values[0].amount_raw, f.tokens_out_raw);
    assert_ne!(f.buyer, f.recipient);
    assert_ne!(f.transaction_from, f.buyer);
}

#[derive(Clone)]
struct Matcher {
    identity: Option<TradeCandidateIdentity>,
}
#[async_trait]
impl TradeCandidateMatcher for Matcher {
    async fn matched_identity(
        &self,
        address: WalletAddress,
        _: chrono::DateTime<Utc>,
    ) -> Result<Option<TradeCandidateIdentity>, RunError> {
        Ok(self.identity.clone().filter(|v| v.wallet == address))
    }
}
#[derive(Clone)]
struct Rpc {
    receipt: Arc<Mutex<Receipt>>,
    failures: Arc<AtomicUsize>,
}
#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(100))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![])
    }
    async fn block(&self, n: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        Ok(Some(BlockHeader {
            number: n,
            hash: HASH.parse().unwrap(),
            parent_hash: BlockHash::from_slice(&[0; 32]).unwrap(),
            timestamp: 1_700_000_000,
        }))
    }
    async fn receipt(&self, _: TxHash) -> Result<Option<Receipt>, RunError> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                (v > 0).then(|| v - 1)
            })
            .is_ok()
        {
            Err(RunError::Rpc("temporary".into()))
        } else {
            Ok(Some(self.receipt.lock().unwrap().clone()))
        }
    }
    async fn logs(
        &self,
        _: BlockNumber,
        _: BlockNumber,
        _: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        Ok(vec![])
    }
}
async fn setup(pool: &PgPool) -> (StoredCurve, TradeCandidateIdentity) {
    let deployment:uuid::Uuid=sqlx::query_scalar("INSERT INTO protocol_deployments(protocol,generation,chain_id,address,start_block,enabled,source,health,interface_fingerprint) VALUES('PONS','V2',4663,$1,1,true,'test','VERIFIED','pons-v2-factory:v1') RETURNING id").bind([0x77_u8;20].as_slice()).fetch_one(pool).await.unwrap();
    let token_id:uuid::Uuid=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle,deployment_id) VALUES(4663,$1,$2,$3,90,to_timestamp(1699999990),'ACTIVE_CURVE',$4) RETURNING id").bind(TOKEN.parse::<TokenAddress>().unwrap().as_bytes().as_slice()).bind(CURVE.parse::<CurveAddress>().unwrap().as_bytes().as_slice()).bind(OTHER.parse::<WalletAddress>().unwrap().as_bytes().as_slice()).bind(deployment).fetch_one(pool).await.unwrap();
    let raw:uuid::Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data) VALUES(4663,90,$1,$2,99,$3,'[]','') RETURNING id").bind(HASH.parse::<BlockHash>().unwrap().as_bytes().as_slice()).bind([0x55_u8;32].as_slice()).bind([0x77_u8;20].as_slice()).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO pons_curves(chain_id,curve_address,token_id,token_address,deployment_id,launch_raw_log_id) VALUES(4663,$1,$2,$3,$4,$5)").bind(CURVE.parse::<CurveAddress>().unwrap().as_bytes().as_slice()).bind(token_id).bind(TOKEN.parse::<TokenAddress>().unwrap().as_bytes().as_slice()).bind(deployment).bind(raw).execute(pool).await.unwrap();
    let trader: uuid::Uuid =
        sqlx::query_scalar("INSERT INTO traders(handle) VALUES('alice') RETURNING id")
            .fetch_one(pool)
            .await
            .unwrap();
    let wallet:uuid::Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified) VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',0.95,true) RETURNING id").bind(trader).bind(TRACKED.parse::<WalletAddress>().unwrap().as_bytes().as_slice()).fetch_one(pool).await.unwrap();
    (
        StoredCurve {
            curve: CURVE.parse().unwrap(),
            token: TOKEN.parse().unwrap(),
            token_id,
            deployment_id: deployment,
            launch_block: BlockNumber::new(90),
        },
        TradeCandidateIdentity {
            trader_id: trader,
            trader_wallet_id: wallet,
            wallet: TRACKED.parse().unwrap(),
            confidence: "0.9500".into(),
            verified: true,
            role: "ROBINHOOD_EXECUTION_ADDRESS".into(),
            source: "OPERATOR_VERIFIED".into(),
            snapshot: json!({"handle":"alice","confidence":"0.9500"}),
        },
    )
}
fn batch(log: ChainLog) -> ChainBatch {
    batch_from(log, IngestionSource::Live)
}
fn batch_from(log: ChainLog, source: IngestionSource) -> ChainBatch {
    ChainBatch {
        source,
        chain_id: ChainId::new(4663),
        from_block: BlockNumber::new(100),
        to_block: BlockNumber::new(100),
        terminal_hash: HASH.parse().unwrap(),
        logs: vec![log],
    }
}

#[derive(Clone)]
struct IntervalMatcher {
    identity: TradeCandidateIdentity,
    from: chrono::DateTime<Utc>,
    to: chrono::DateTime<Utc>,
}
#[async_trait]
impl TradeCandidateMatcher for IntervalMatcher {
    async fn matched_identity(
        &self,
        address: WalletAddress,
        at: chrono::DateTime<Utc>,
    ) -> Result<Option<TradeCandidateIdentity>, RunError> {
        Ok(
            (address == self.identity.wallet && self.from <= at && at < self.to)
                .then(|| self.identity.clone()),
        )
    }
}
async fn handler(
    pool: &PgPool,
    rpc: Arc<Rpc>,
    identity: Option<TradeCandidateIdentity>,
) -> CurveTradeHandler {
    let curves = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
        .await
        .unwrap();
    CurveTradeHandler::new(curves, TradeRepository::new(pool.clone()), rpc)
        .with_candidate_matcher(Arc::new(Matcher { identity }))
}

async fn interval_handler(
    pool: &PgPool,
    rpc: Arc<Rpc>,
    matcher: IntervalMatcher,
) -> CurveTradeHandler {
    let curves = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
        .await
        .unwrap();
    CurveTradeHandler::new(curves, TradeRepository::new(pool.clone()), rpc)
        .with_candidate_matcher(Arc::new(matcher))
}
fn worker(pool: &PgPool, rpc: Arc<Rpc>) -> TradeConfirmationWorker {
    TradeConfirmationWorker::new(
        ConfirmationRepository::new(pool.clone()),
        rpc,
        ConfirmationWorkerSettings {
            concurrency: 1,
            rpc_timeout: Duration::from_secs(1),
            poll_interval: Duration::from_millis(1),
            retry_minimum: Duration::from_millis(1),
            retry_maximum: Duration::from_millis(2),
        },
    )
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn buy_candidate_uses_recipient_not_buyer_and_confirms_exact_transfer(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let amount = U256::from(42);
    let receipt = Receipt {
        tx_hash: TX.parse().unwrap(),
        block_number: BlockNumber::new(100),
        block_hash: HASH.parse().unwrap(),
        succeeded: true,
        logs: vec![
            transfer(CURVE, TRACKED, U256::from(20), 5),
            transfer(CURVE, TRACKED, U256::from(22), 6),
        ],
    };
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(receipt)),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    let h = handler(&pool, rpc.clone(), Some(identity)).await;
    h.handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    h.handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    let repo = ConfirmationRepository::new(pool.clone());
    let job = repo.claim_due().await.unwrap().unwrap();
    worker(&pool, rpc).process(&job).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT confirmation_level FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "BUY_CONFIRMED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let timing:(i64,String,String)=sqlx::query_as("SELECT launch_age_ms,launch_age_blocks::text,confirmation_confidence::text FROM smart_trades").fetch_one(&pool).await.unwrap();
    assert_eq!(timing.0, 10_000);
    assert_eq!(timing.1, "10");
    assert_eq!(timing.2, "1.0000");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type='smart_trade.buy_confirmed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let snapshot: serde_json::Value =
        sqlx::query_scalar("SELECT identity_snapshot FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(snapshot["handle"], "alice");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM position_rebuild_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "confirmation transaction durably marks the position dirty"
    );
    let semantics: (String, bool) =
        sqlx::query_as("SELECT classification_source,realtime_alert_eligible FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(semantics, ("LIVE".into(), true));
}

#[sqlx::test(migrations = "../../migrations")]
async fn late_backfilled_trade_uses_event_time_identity_and_is_never_realtime(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let wallet_id = identity.trader_wallet_id;
    let occurred = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let matcher = IntervalMatcher {
        identity,
        from: occurred - TimeDelta::days(9),
        to: occurred + TimeDelta::days(10),
    };
    let amount = U256::from(42);
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, amount, 6)],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    interval_handler(&pool, rpc.clone(), matcher)
        .await
        .handle(batch_from(
            curve_log("BUY", OTHER, TRACKED, amount),
            IngestionSource::ChainBackfill,
        ))
        .await
        .unwrap();
    sqlx::query("UPDATE trader_wallets SET enabled=false WHERE id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await
        .unwrap();
    let job = ConfirmationRepository::new(pool.clone())
        .claim_due()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.classification_source, "CHAIN_BACKFILL");
    assert!(!job.realtime_alert_eligible);
    worker(&pool, rpc).process(&job).await.unwrap();
    let row: (String, bool, String) = sqlx::query_as(
        "SELECT classification_source,realtime_alert_eligible,confirmation_level FROM smart_trades",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        row,
        ("CHAIN_BACKFILL".into(), false, "BUY_CONFIRMED".into())
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM event_outbox WHERE event_type LIKE 'smart_trade.%'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "smart_trade.buy_backfilled"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn late_trade_before_identity_effective_time_creates_no_candidate(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let occurred = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
    let matcher = IntervalMatcher {
        identity,
        from: occurred + TimeDelta::days(1),
        to: occurred + TimeDelta::days(10),
    };
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    interval_handler(&pool, rpc, matcher)
        .await
        .handle(batch_from(
            curve_log("BUY", OTHER, TRACKED, U256::ONE),
            IngestionSource::ChainBackfill,
        ))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn identity_backfill_confirmation_uses_non_realtime_outbox_semantics(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let amount = U256::from(42);
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, amount, 6)],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    handler(&pool, rpc.clone(), Some(identity))
        .await
        .handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    sqlx::query("UPDATE smart_trades SET classification_source='IDENTITY_BACKFILL',realtime_alert_eligible=false")
        .execute(&pool).await.unwrap();
    let job = ConfirmationRepository::new(pool.clone())
        .claim_due()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(job.classification_source, "IDENTITY_BACKFILL");
    assert!(!job.realtime_alert_eligible);
    worker(&pool, rpc).process(&job).await.unwrap();
    let event: String = sqlx::query_scalar(
        "SELECT event_type FROM event_outbox WHERE event_type LIKE 'smart_trade.%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(event, "smart_trade.buy_backfilled");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type='smart_trade.buy_confirmed'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn buyer_only_match_untracked_recipient_and_plain_transfer_create_no_candidate(pool: PgPool) {
    let (_, mut identity) = setup(&pool).await;
    identity.wallet = OTHER.parse().unwrap();
    let amount = U256::from(42);
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, amount, 6)],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    handler(&pool, rpc, Some(identity))
        .await
        .handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM trade_confirmation_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn sell_uses_seller_not_quote_recipient_and_mismatch_rejects(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let amount = U256::from(12);
    let receipt = Receipt {
        tx_hash: TX.parse().unwrap(),
        block_number: BlockNumber::new(100),
        block_hash: HASH.parse().unwrap(),
        succeeded: true,
        logs: vec![transfer(TRACKED, CURVE, amount, 6)],
    };
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(receipt)),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    handler(&pool, rpc.clone(), Some(identity))
        .await
        .handle(batch(curve_log("SELL", TRACKED, OTHER, amount)))
        .await
        .unwrap();
    let repo = ConfirmationRepository::new(pool.clone());
    let job = repo.claim_due().await.unwrap().unwrap();
    worker(&pool, rpc).process(&job).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT confirmation_level FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "SELL_CONFIRMED"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn temporary_rpc_failure_retries_after_restart_and_identity_snapshot_survives_disable(
    pool: PgPool,
) {
    let (_, identity) = setup(&pool).await;
    let wallet_id = identity.trader_wallet_id;
    let amount = U256::from(9);
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, amount, 6)],
        })),
        failures: Arc::new(AtomicUsize::new(1)),
    });
    handler(&pool, rpc.clone(), Some(identity))
        .await
        .handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    assert_eq!(
        rpc.failures.load(Ordering::SeqCst),
        1,
        "curve ingestion must not call receipt RPC"
    );
    sqlx::query("UPDATE trader_wallets SET enabled=false WHERE id=$1")
        .bind(wallet_id)
        .execute(&pool)
        .await
        .unwrap();
    let repo = ConfirmationRepository::new(pool.clone());
    let first = repo.claim_due().await.unwrap().unwrap();
    assert!(worker(&pool, rpc.clone()).process(&first).await.is_err());
    repo.retry(
        first.smart_trade_id,
        "temporary",
        Utc::now() - TimeDelta::seconds(1),
    )
    .await
    .unwrap();
    let resumed = ConfirmationRepository::new(pool.clone())
        .claim_due()
        .await
        .unwrap()
        .unwrap();
    worker(&pool, rpc).process(&resumed).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT confirmation_level FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "BUY_CONFIRMED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT identity_snapshot->>'handle' FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "alice"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn amount_mismatch_is_integrity_conflict_and_stale_claim_recovers(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, U256::from(3), 6)],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    handler(&pool, rpc.clone(), Some(identity))
        .await
        .handle(batch(curve_log("BUY", OTHER, TRACKED, U256::from(4))))
        .await
        .unwrap();
    let repo = ConfirmationRepository::new(pool.clone());
    let first = repo.claim_due().await.unwrap().unwrap();
    sqlx::query("UPDATE trade_confirmation_jobs SET locked_at=now()-interval '10 minutes' WHERE smart_trade_id=$1").bind(first.smart_trade_id).execute(&pool).await.unwrap();
    let stale = repo.claim_due().await.unwrap().unwrap();
    worker(&pool, rpc).process(&stale).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT confirmation_level FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "INTEGRITY_CONFLICT"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type LIKE 'smart_trade.%'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn orphaned_protocol_trade_is_rejected_without_receipt_confirmation(pool: PgPool) {
    let (_, identity) = setup(&pool).await;
    let amount = U256::ONE;
    let rpc = Arc::new(Rpc {
        receipt: Arc::new(Mutex::new(Receipt {
            tx_hash: TX.parse().unwrap(),
            block_number: BlockNumber::new(100),
            block_hash: HASH.parse().unwrap(),
            succeeded: true,
            logs: vec![transfer(CURVE, TRACKED, amount, 6)],
        })),
        failures: Arc::new(AtomicUsize::new(0)),
    });
    handler(&pool, rpc.clone(), Some(identity))
        .await
        .handle(batch(curve_log("BUY", OTHER, TRACKED, amount)))
        .await
        .unwrap();
    sqlx::query("UPDATE token_trades SET status='ORPHANED'")
        .execute(&pool)
        .await
        .unwrap();
    let job = ConfirmationRepository::new(pool.clone())
        .claim_due()
        .await
        .unwrap()
        .unwrap();
    worker(&pool, rpc).process(&job).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT confirmation_level FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "REJECTED"
    );
}
