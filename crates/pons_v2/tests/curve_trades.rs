use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_primitives::U256;
use async_trait::async_trait;
use pons_chain::{
    BackfillCoordinator, BackfillSettings, BatchHandler, BlockHeader, ChainBatch, ChainHealth,
    ChainLog, ChainRpc, ChainSubscription, IngestionSource, LogFilter, Receipt, ReconnectPolicy,
    RunError, SubscriptionEvent, SubscriptionFactory,
};
use pons_domain::{
    BlockHash, BlockNumber, ChainId, ContractAddress, CurveAddress, LogIndex, LogTopic,
    TokenAddress, TxHash, WalletAddress,
};
use pons_storage::repositories::{
    ChainCursorRepository, StoredCurve, TokenLaunchRepository, TradeRepository,
};
use pons_v2::{
    CURVE_TRADE_PARSER_VERSION, CURVE_TRADE_SCHEMA_VERSION, CurveRegistry, CurveTradeHandler,
    DecodedCurveEvent, batched_curve_filters, bounded_curve_shards, curve_buy_refunded_topic,
    curve_buy_topic, curve_log_filter, curve_sell_topic, decode_curve_event, stable_curve_shards,
};
use sqlx::PgPool;

const CURVE: &str = "0x6393305a54b3b15819a3e8f693addadd2eebd021";
const TOKEN: &str = "0xf9b84b5f789499632bc222a91d79f433c263c827";
const BUYER: &str = "0x1111111111111111111111111111111111111111";
const RECIPIENT: &str = "0x2222222222222222222222222222222222222222";
const HASH: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
type TradeRow = (String, String, Vec<u8>, Vec<u8>, String, String);

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradeFixture {
    token: String,
    curve: String,
    launch_block: String,
    logs: Vec<FixtureLog>,
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FixtureLog {
    kind: String,
    address: String,
    topics: Vec<String>,
    data: String,
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    transaction_index: String,
    log_index: String,
}
fn hex(value: &str) -> Vec<u8> {
    let v = value.strip_prefix("0x").unwrap();
    (0..v.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&v[i..i + 2], 16).unwrap())
        .collect()
}
fn quantity(value: &str) -> u64 {
    u64::from_str_radix(value.strip_prefix("0x").unwrap(), 16).unwrap()
}

#[test]
fn real_curve_buy_and_sell_fixture_decodes_offline() {
    let fixture: TradeFixture = serde_json::from_str(include_str!(
        "../../../fixtures/pons_v2/curve_trades_da8eea02.json"
    ))
    .unwrap();
    let known = StoredCurve {
        curve: fixture.curve.parse().unwrap(),
        token: fixture.token.parse().unwrap(),
        token_id: uuid::Uuid::nil(),
        deployment_id: uuid::Uuid::nil(),
        launch_block: BlockNumber::new(quantity(&fixture.launch_block)),
    };
    let decoded: Vec<_> = fixture
        .logs
        .into_iter()
        .map(|v| {
            let log = ChainLog {
                block_number: BlockNumber::new(quantity(&v.block_number)),
                block_hash: v.block_hash.parse().unwrap(),
                tx_hash: v.transaction_hash.parse().unwrap(),
                transaction_index: Some(quantity(&v.transaction_index)),
                log_index: LogIndex::new(quantity(&v.log_index)),
                address: v.address.parse().unwrap(),
                topics: v.topics.into_iter().map(|t| t.parse().unwrap()).collect(),
                data: hex(&v.data),
                removed: false,
            };
            (v.kind, decode_curve_event(&log, &known).unwrap())
        })
        .collect();
    assert!(
        matches!(&decoded[0],(kind,DecodedCurveEvent::Buy{actor,recipient,..}) if kind=="BUY" && actor!=recipient)
    );
    assert!(
        matches!(&decoded[1],(kind,DecodedCurveEvent::Sell{actor,recipient,..}) if kind=="SELL" && actor==recipient)
    );
}

fn topic_address(value: &str) -> LogTopic {
    let address = value.parse::<WalletAddress>().unwrap();
    let mut bytes = [0_u8; 32];
    bytes[12..].copy_from_slice(address.as_bytes());
    LogTopic::from_slice(&bytes).unwrap()
}
fn data(values: &[U256]) -> Vec<u8> {
    values.iter().flat_map(U256::to_be_bytes::<32>).collect()
}
fn log(topic: LogTopic, index: u64, values: &[U256]) -> ChainLog {
    ChainLog {
        block_number: BlockNumber::new(100),
        block_hash: HASH.parse().unwrap(),
        tx_hash: format!("0x{:064x}", index + 1).parse().unwrap(),
        transaction_index: Some(3),
        log_index: LogIndex::new(index),
        address: CURVE.parse().unwrap(),
        topics: vec![topic, topic_address(BUYER), topic_address(RECIPIENT)],
        data: data(values),
        removed: false,
    }
}
fn known() -> StoredCurve {
    StoredCurve {
        curve: CURVE.parse().unwrap(),
        token: TOKEN.parse().unwrap(),
        token_id: uuid::Uuid::nil(),
        deployment_id: uuid::Uuid::nil(),
        launch_block: BlockNumber::new(100),
    }
}

#[test]
fn representative_buy_sell_decode_indexed_parties_and_u256_fields() {
    let max = U256::MAX;
    let buy = decode_curve_event(
        &log(
            curve_buy_topic(),
            0,
            &[max, U256::from(2), U256::from(3), U256::from(4)],
        ),
        &known(),
    )
    .unwrap();
    assert_eq!(buy.execution_actor(), BUYER.parse().unwrap());
    assert_eq!(buy.market_participant(), Some(RECIPIENT.parse().unwrap()));
    assert_eq!(
        buy,
        DecodedCurveEvent::Buy {
            actor: BUYER.parse().unwrap(),
            recipient: RECIPIENT.parse().unwrap(),
            quote_in: max.to_string(),
            tokens_out: "2".into(),
            fee: "3".into(),
            tax: "4".into()
        }
    );
    let sell = decode_curve_event(
        &log(
            curve_sell_topic(),
            1,
            &[U256::from(5), U256::from(6), U256::from(7), U256::from(8)],
        ),
        &known(),
    )
    .unwrap();
    assert_eq!(sell.execution_actor(), BUYER.parse().unwrap());
    assert_eq!(sell.market_participant(), Some(BUYER.parse().unwrap()));
    assert_eq!(sell.proceeds_recipient(), Some(RECIPIENT.parse().unwrap()));
    assert_eq!(
        sell,
        DecodedCurveEvent::Sell {
            actor: BUYER.parse().unwrap(),
            recipient: RECIPIENT.parse().unwrap(),
            tokens_in: "5".into(),
            quote_out: "6".into(),
            fee: "7".into(),
            tax: "8".into()
        }
    );
}

#[test]
fn unknown_wrong_topic_and_malformed_events_fail_closed() {
    let value = log(curve_buy_topic(), 0, &[U256::from(1); 4]);
    let mut other = known();
    other.curve = "0x9999999999999999999999999999999999999999"
        .parse()
        .unwrap();
    assert!(decode_curve_event(&value, &other).is_err());
    let mut wrong = value.clone();
    wrong.topics[0] = HASH.parse().unwrap();
    assert!(decode_curve_event(&wrong, &known()).is_err());
    let mut malformed = value;
    malformed.data.pop();
    assert!(decode_curve_event(&malformed, &known()).is_err());
}

#[test]
fn refund_is_distinct_accounting_event_not_buy() {
    let mut value = log(curve_buy_refunded_topic(), 2, &[U256::from(99)]);
    value.topics.pop();
    assert_eq!(
        decode_curve_event(&value, &known()).unwrap(),
        DecodedCurveEvent::BuyRefunded {
            buyer: BUYER.parse().unwrap(),
            refund: "99".into()
        }
    );
}

#[test]
fn large_registry_filters_are_batched() {
    let curves: Vec<StoredCurve> = (0..10_000_u64)
        .map(|n| {
            let mut bytes = [0_u8; 20];
            bytes[12..].copy_from_slice(&n.to_be_bytes());
            StoredCurve {
                curve: CurveAddress::from_slice(&bytes).unwrap(),
                token: TokenAddress::from_slice(&bytes).unwrap(),
                token_id: uuid::Uuid::new_v4(),
                deployment_id: uuid::Uuid::new_v4(),
                launch_block: BlockNumber::new(1),
            }
        })
        .collect();
    assert_eq!(batched_curve_filters(&curves[..100], 500).len(), 1);
    assert_eq!(batched_curve_filters(&curves[..1_000], 500).len(), 2);
    let filters = batched_curve_filters(&curves, 500);
    assert_eq!(filters.len(), 20);
    assert!(filters.iter().all(|v| v.addresses.len() <= 500));
    assert!(
        filters
            .iter()
            .all(|v| v.topics[0].as_ref().unwrap().len() == 3)
    );
    let shards = stable_curve_shards(&curves, 32);
    assert!(shards.len() <= 32);
    assert_eq!(
        shards.iter().map(|(_, values)| values.len()).sum::<usize>(),
        10_000
    );
    assert!(shards.iter().all(|(_, values)| values.len() <= 500));

    let small = bounded_curve_shards(&curves[..100], 500);
    assert_eq!(small.len(), 1, "100 curves need only one RPC stream");
    for count in [1_000, 10_000] {
        let adaptive = bounded_curve_shards(&curves[..count], 500);
        assert_eq!(
            adaptive
                .iter()
                .map(|(_, values)| values.len())
                .sum::<usize>(),
            count
        );
        assert!(adaptive.iter().all(|(_, values)| values.len() <= 500));
        assert!(adaptive.len() < 32 || count == 10_000);
    }
}

#[derive(Clone)]
struct Rpc;
#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(100))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
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
        Ok(None)
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

#[derive(Clone)]
struct BackfillRpc {
    logs: Arc<Mutex<Vec<ChainLog>>>,
}
#[async_trait]
impl ChainRpc for BackfillRpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(101))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
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
        Ok(None)
    }
    async fn logs(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        Ok(self
            .logs
            .lock()
            .unwrap()
            .iter()
            .filter(|v| {
                v.block_number >= from
                    && v.block_number <= to
                    && filter.addresses.contains(&v.address)
            })
            .cloned()
            .collect())
    }
}

async fn setup(pool: &PgPool) -> StoredCurve {
    let deployment:uuid::Uuid=sqlx::query_scalar("INSERT INTO protocol_deployments(protocol,generation,chain_id,address,start_block,enabled,source,health,interface_fingerprint) VALUES('PONS','V2',4663,$1,100,true,'test','VERIFIED','pons-v2-factory:v1') RETURNING id").bind([0x77_u8;20].as_slice()).fetch_one(pool).await.unwrap();
    let token_id:uuid::Uuid=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,lifecycle,deployment_id) VALUES(4663,$1,$2,$3,100,'ACTIVE_CURVE',$4) RETURNING id").bind(TOKEN.parse::<TokenAddress>().unwrap().as_bytes().as_slice()).bind(CURVE.parse::<CurveAddress>().unwrap().as_bytes().as_slice()).bind(BUYER.parse::<WalletAddress>().unwrap().as_bytes().as_slice()).bind(deployment).fetch_one(pool).await.unwrap();
    let raw:uuid::Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data) VALUES(4663,100,$1,$2,99,$3,'[]','') RETURNING id").bind(HASH.parse::<BlockHash>().unwrap().as_bytes().as_slice()).bind([0x55_u8;32].as_slice()).bind([0x77_u8;20].as_slice()).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO pons_curves(chain_id,curve_address,token_id,token_address,deployment_id,launch_raw_log_id) VALUES(4663,$1,$2,$3,$4,$5)").bind(CURVE.parse::<CurveAddress>().unwrap().as_bytes().as_slice()).bind(token_id).bind(TOKEN.parse::<TokenAddress>().unwrap().as_bytes().as_slice()).bind(deployment).bind(raw).execute(pool).await.unwrap();
    StoredCurve {
        curve: CURVE.parse().unwrap(),
        token: TOKEN.parse().unwrap(),
        token_id,
        deployment_id: deployment,
        launch_block: BlockNumber::new(100),
    }
}
fn batch(value: ChainLog) -> ChainBatch {
    ChainBatch {
        source: IngestionSource::Live,
        chain_id: ChainId::new(4663),
        from_block: BlockNumber::new(100),
        to_block: BlockNumber::new(100),
        terminal_hash: HASH.parse().unwrap(),
        logs: vec![value],
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn buy_sell_persistence_is_atomic_idempotent_versioned_and_outboxed(pool: PgPool) {
    setup(&pool).await;
    let curves = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
        .await
        .unwrap();
    let handler = CurveTradeHandler::new(curves, TradeRepository::new(pool.clone()), Arc::new(Rpc));
    let buy = log(
        curve_buy_topic(),
        10,
        &[U256::from(11), U256::from(12), U256::from(2), U256::from(1)],
    );
    handler.handle(batch(buy.clone())).await.unwrap();
    handler.handle(batch(buy)).await.unwrap();
    let sell = log(
        curve_sell_topic(),
        11,
        &[U256::from(9), U256::from(8), U256::from(2), U256::from(1)],
    );
    handler.handle(batch(sell.clone())).await.unwrap();
    handler.handle(batch(sell)).await.unwrap();
    let rows: Vec<TradeRow> = sqlx::query_as(
        "SELECT side,event_type,actor,recipient,fee_raw,tax_raw FROM token_trades ORDER BY side",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].2, rows[0].3);
    assert_eq!((&rows[0].4, &rows[0].5), (&"2".into(), &"1".into()));
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM normalized_events WHERE event_type IN ('PONS_V2_CURVE_BUY','PONS_V2_CURVE_SELL') AND parser_version=$1 AND schema_version=$2").bind(CURVE_TRADE_PARSER_VERSION).bind(CURVE_TRADE_SCHEMA_VERSION).fetch_one(&pool).await.unwrap(),2);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type IN ('trade.buy','trade.sell')"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn conflicting_trade_fails_closed_and_refund_never_creates_buy(pool: PgPool) {
    setup(&pool).await;
    let curves = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
        .await
        .unwrap();
    let handler = CurveTradeHandler::new(curves, TradeRepository::new(pool.clone()), Arc::new(Rpc));
    let original = log(curve_buy_topic(), 20, &[U256::from(1); 4]);
    handler.handle(batch(original.clone())).await.unwrap();
    let mut conflict = original;
    conflict.data = data(&[U256::from(2); 4]);
    assert!(handler.handle(batch(conflict)).await.is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM token_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let mut refund = log(curve_buy_refunded_topic(), 21, &[U256::from(3)]);
    refund.topics.pop();
    handler.handle(batch(refund)).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM token_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM curve_accounting_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn launch_block_backfill_catches_delayed_first_buy_and_restart_is_idempotent(pool: PgPool) {
    let curve = setup(&pool).await;
    let first_buy = log(
        curve_buy_topic(),
        30,
        &[U256::from(7), U256::from(8), U256::from(1), U256::ZERO],
    );
    let rpc = Arc::new(BackfillRpc {
        logs: Arc::new(Mutex::new(vec![first_buy.clone()])),
    });
    let stream = "phase6-backfill";
    for _restart in 0..2 {
        let registry = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
            .await
            .unwrap();
        assert_eq!(registry.token(curve.curve).await, Some(curve.token));
        let worker = BackfillCoordinator::new(
            ChainId::new(4663),
            stream,
            rpc.clone(),
            Arc::new(Ws),
            ChainCursorRepository::new(pool.clone()),
            Arc::new(CurveTradeHandler::new(
                registry,
                TradeRepository::new(pool.clone()),
                rpc.clone(),
            )),
            curve_log_filter(std::slice::from_ref(&curve)),
            BackfillSettings {
                start_block: curve.launch_block,
                chunk_blocks: 100,
            },
            ReconnectPolicy {
                minimum: Duration::from_millis(1),
                maximum: Duration::from_millis(2),
            },
            ChainHealth::default(),
        );
        worker.sync_once().await.unwrap();
    }
    // A live overlap is harmless because the database identity is authoritative.
    let registry = CurveRegistry::rebuild(&TokenLaunchRepository::new(pool.clone()))
        .await
        .unwrap();
    CurveTradeHandler::new(registry, TradeRepository::new(pool.clone()), rpc)
        .handle(batch(first_buy))
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM token_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        ChainCursorRepository::new(pool)
            .get(stream)
            .await
            .unwrap()
            .unwrap()
            .last_processed_block,
        BlockNumber::new(101)
    );
}
