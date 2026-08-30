use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use pons_chain::{
    BackfillCoordinator, BackfillSettings, BatchHandler, BlockHeader, ChainBatch, ChainHealth,
    ChainLog, ChainRpc, ChainSubscription, IngestionSource, LogFilter, Receipt, ReconnectPolicy,
    RunError, SubscriptionEvent, SubscriptionFactory, verify_chain_id,
};
use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress, LogIndex, TxHash};
use pons_storage::repositories::ChainCursorRepository;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

struct MockRpc {
    chain_id: ChainId,
    head: AtomicU64,
    ranges: Mutex<Vec<(u64, u64)>>,
    logs: Mutex<Vec<ChainLog>>,
    head_calls: AtomicUsize,
}

impl MockRpc {
    fn new(chain_id: u64, head: u64) -> Self {
        Self {
            chain_id: ChainId::new(chain_id),
            head: AtomicU64::new(head),
            ranges: Mutex::new(Vec::new()),
            logs: Mutex::new(Vec::new()),
            head_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl ChainRpc for MockRpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(self.chain_id)
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        self.head_calls.fetch_add(1, Ordering::SeqCst);
        Ok(BlockNumber::new(self.head.load(Ordering::SeqCst)))
    }
    async fn code(&self, _address: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
    }
    async fn block(&self, number: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        Ok(Some(BlockHeader {
            number,
            hash: BlockHash::from_slice(&[u8::try_from(number.get()).unwrap_or(0); 32]).unwrap(),
            parent_hash: BlockHash::from_slice(&[0; 32]).unwrap(),
            timestamp: number.get(),
        }))
    }
    async fn receipt(&self, _hash: TxHash) -> Result<Option<Receipt>, RunError> {
        Ok(None)
    }
    async fn logs(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        _filter: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        self.ranges.lock().unwrap().push((from.get(), to.get()));
        Ok(self.logs.lock().unwrap().clone())
    }
}

#[derive(Default)]
struct RecordingHandler {
    batches: Mutex<Vec<ChainBatch>>,
    fail: AtomicBool,
}

#[async_trait]
impl BatchHandler for RecordingHandler {
    async fn handle(&self, batch: ChainBatch) -> Result<(), RunError> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(RunError::Handler("fixture failure".to_owned()));
        }
        self.batches.lock().unwrap().push(batch);
        Ok(())
    }
}

struct EmptySubscription;

#[async_trait]
impl ChainSubscription for EmptySubscription {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        std::future::pending().await
    }
}

#[derive(Default)]
struct MockWs {
    connects: AtomicUsize,
}

#[async_trait]
impl SubscriptionFactory for MockWs {
    async fn connect(
        &self,
        _expected: ChainId,
        _filter: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError> {
        let attempt = self.connects.fetch_add(1, Ordering::SeqCst);
        if attempt == 0 {
            Ok(Box::new(DisconnectSubscription))
        } else {
            Ok(Box::new(EmptySubscription))
        }
    }
}

struct DisconnectSubscription;

#[async_trait]
impl ChainSubscription for DisconnectSubscription {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        Some(Err(RunError::Disconnected))
    }
}

struct ReconnectSemanticsWs {
    connects: AtomicUsize,
    rpc: Arc<MockRpc>,
}
#[async_trait]
impl SubscriptionFactory for ReconnectSemanticsWs {
    async fn connect(
        &self,
        _: ChainId,
        _: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError> {
        let attempt = self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(if attempt == 0 {
            Box::new(AdvanceThenDisconnect {
                rpc: self.rpc.clone(),
            })
        } else {
            Box::new(OneLiveHead {
                rpc: self.rpc.clone(),
                sent: false,
            })
        })
    }
}
struct AdvanceThenDisconnect {
    rpc: Arc<MockRpc>,
}
#[async_trait]
impl ChainSubscription for AdvanceThenDisconnect {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        self.rpc.head.store(10, Ordering::SeqCst);
        Some(Err(RunError::Disconnected))
    }
}
struct OneLiveHead {
    rpc: Arc<MockRpc>,
    sent: bool,
}
#[async_trait]
impl ChainSubscription for OneLiveHead {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        if self.sent {
            std::future::pending().await
        } else {
            self.sent = true;
            self.rpc.head.store(11, Ordering::SeqCst);
            Some(Ok(SubscriptionEvent::NewHead(BlockNumber::new(11))))
        }
    }
}

struct BurstWs;

#[async_trait]
impl SubscriptionFactory for BurstWs {
    async fn connect(
        &self,
        _expected: ChainId,
        _filter: &LogFilter,
    ) -> Result<Box<dyn ChainSubscription>, RunError> {
        Ok(Box::new(BurstSubscription { next: 1 }))
    }
}

struct BurstSubscription {
    next: u64,
}

#[async_trait]
impl ChainSubscription for BurstSubscription {
    async fn next(&mut self) -> Option<Result<SubscriptionEvent, RunError>> {
        if self.next <= 100 {
            let value = self.next;
            self.next += 1;
            Some(Ok(SubscriptionEvent::NewHead(BlockNumber::new(value))))
        } else {
            std::future::pending().await
        }
    }
}

fn chain_log(block: u64, transaction_index: Option<u64>, log_index: u64, marker: u8) -> ChainLog {
    ChainLog {
        block_number: BlockNumber::new(block),
        block_hash: BlockHash::from_slice(&[u8::try_from(block).unwrap(); 32]).unwrap(),
        tx_hash: TxHash::from_slice(&[marker; 32]).unwrap(),
        transaction_index,
        log_index: LogIndex::new(log_index),
        address: ContractAddress::from_slice(&[marker; 20]).unwrap(),
        topics: Vec::new(),
        data: Vec::new(),
        removed: false,
    }
}

fn coordinator(
    pool: PgPool,
    rpc: Arc<MockRpc>,
    ws: Arc<MockWs>,
    handler: Arc<RecordingHandler>,
    health: ChainHealth,
) -> BackfillCoordinator {
    BackfillCoordinator::new(
        ChainId::new(4663),
        "fixture",
        rpc,
        ws,
        ChainCursorRepository::new(pool),
        handler,
        LogFilter::default(),
        BackfillSettings {
            start_block: BlockNumber::new(1),
            chunk_blocks: 2,
        },
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        health,
    )
}

#[tokio::test]
async fn wrong_chain_fails_closed() {
    let rpc = MockRpc::new(1, 0);
    assert!(matches!(
        verify_chain_id(&rpc, ChainId::new(4663)).await,
        Err(RunError::WrongChain { .. })
    ));
}

#[sqlx::test(migrations = "../../migrations")]
async fn backfill_is_chunked_and_resumes_from_durable_cursor(pool: PgPool) {
    let rpc = Arc::new(MockRpc::new(4663, 5));
    let handler = Arc::new(RecordingHandler::default());
    let worker = coordinator(
        pool.clone(),
        rpc.clone(),
        Arc::new(MockWs::default()),
        handler.clone(),
        ChainHealth::default(),
    );
    worker.verify_http_chain().await.unwrap();
    worker.sync_once().await.unwrap();
    assert_eq!(*rpc.ranges.lock().unwrap(), [(1, 2), (3, 4), (5, 5)]);
    assert_eq!(
        ChainCursorRepository::new(pool.clone())
            .get("fixture")
            .await
            .unwrap()
            .unwrap()
            .last_processed_block,
        BlockNumber::new(5)
    );

    rpc.head.store(7, Ordering::SeqCst);
    worker.sync_once().await.unwrap();
    assert_eq!(
        *rpc.ranges.lock().unwrap(),
        [(1, 2), (3, 4), (5, 5), (6, 7)]
    );
    assert_eq!(handler.batches.lock().unwrap().len(), 4);
    assert!(
        handler
            .batches
            .lock()
            .unwrap()
            .iter()
            .all(|v| v.source == IngestionSource::ChainBackfill)
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn handler_failure_does_not_advance_cursor(pool: PgPool) {
    let rpc = Arc::new(MockRpc::new(4663, 2));
    let handler = Arc::new(RecordingHandler::default());
    handler.fail.store(true, Ordering::SeqCst);
    let worker = coordinator(
        pool.clone(),
        rpc,
        Arc::new(MockWs::default()),
        handler,
        ChainHealth::default(),
    );
    assert!(worker.sync_once().await.is_err());
    assert!(
        ChainCursorRepository::new(pool)
            .get("fixture")
            .await
            .unwrap()
            .is_none()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn cursor_hash_mismatch_fails_closed(pool: PgPool) {
    ChainCursorRepository::new(pool.clone())
        .upsert(
            "fixture",
            ChainId::new(4663),
            BlockNumber::new(1),
            BlockHash::from_slice(&[0xff; 32]).unwrap(),
        )
        .await
        .unwrap();
    let worker = coordinator(
        pool,
        Arc::new(MockRpc::new(4663, 2)),
        Arc::new(MockWs::default()),
        Arc::new(RecordingHandler::default()),
        ChainHealth::default(),
    );
    assert!(matches!(
        worker.sync_once().await,
        Err(RunError::CursorHashMismatch { block: 1 })
    ));
}

#[sqlx::test(migrations = "../../migrations")]
async fn websocket_disconnect_reconnects_and_health_records_it(pool: PgPool) {
    let ws = Arc::new(MockWs::default());
    let health = ChainHealth::default();
    let worker = Arc::new(coordinator(
        pool,
        Arc::new(MockRpc::new(4663, 0)),
        ws.clone(),
        Arc::new(RecordingHandler::default()),
        health.clone(),
    ));
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let worker = worker.clone();
        let cancellation = cancellation.clone();
        async move { worker.run_until(cancellation).await }
    });
    for _ in 0..100 {
        if ws.connects.load(Ordering::SeqCst) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    cancellation.cancel();
    task.await.unwrap().unwrap();
    assert!(ws.connects.load(Ordering::SeqCst) >= 2);
    assert!(health.snapshot().await.ws_reconnects >= 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn reconnect_gap_is_backfill_and_only_next_head_is_live(pool: PgPool) {
    let rpc = Arc::new(MockRpc::new(4663, 0));
    let handler = Arc::new(RecordingHandler::default());
    let ws = Arc::new(ReconnectSemanticsWs {
        connects: AtomicUsize::new(0),
        rpc: rpc.clone(),
    });
    let worker = BackfillCoordinator::new(
        ChainId::new(4663),
        "reconnect-semantics",
        rpc,
        ws.clone(),
        ChainCursorRepository::new(pool),
        handler.clone(),
        LogFilter::default(),
        BackfillSettings {
            start_block: BlockNumber::new(1),
            chunk_blocks: 20,
        },
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        ChainHealth::default(),
    );
    let cancellation = CancellationToken::new();
    let token = cancellation.clone();
    let task = tokio::spawn(async move { worker.run_until(token).await });
    for _ in 0..200 {
        if handler
            .batches
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.to_block == BlockNumber::new(11))
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    cancellation.cancel();
    task.await.unwrap().unwrap();
    let batches = handler.batches.lock().unwrap();
    assert!(ws.connects.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        batches
            .iter()
            .find(|v| v.to_block == BlockNumber::new(10))
            .unwrap()
            .source,
        IngestionSource::ChainBackfill
    );
    assert_eq!(
        batches
            .iter()
            .find(|v| v.to_block == BlockNumber::new(11))
            .unwrap()
            .source,
        IngestionSource::Live
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn notification_burst_is_coalesced_into_one_http_catch_up(pool: PgPool) {
    let rpc = Arc::new(MockRpc::new(4663, 0));
    let worker = Arc::new(BackfillCoordinator::new(
        ChainId::new(4663),
        "fixture",
        rpc.clone(),
        Arc::new(BurstWs),
        ChainCursorRepository::new(pool),
        Arc::new(RecordingHandler::default()),
        LogFilter::default(),
        BackfillSettings {
            start_block: BlockNumber::new(1),
            chunk_blocks: 100,
        },
        ReconnectPolicy {
            minimum: Duration::from_millis(1),
            maximum: Duration::from_millis(2),
        },
        ChainHealth::default(),
    ));
    let cancellation = CancellationToken::new();
    let task = tokio::spawn({
        let cancellation = cancellation.clone();
        async move { worker.run_until(cancellation).await }
    });
    for _ in 0..100 {
        if !rpc.ranges.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    cancellation.cancel();
    task.await.unwrap().unwrap();
    assert_eq!(*rpc.ranges.lock().unwrap(), [(1, 100)]);
    assert_eq!(rpc.head_calls.load(Ordering::SeqCst), 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn logs_are_sorted_before_batch_handler_with_stable_missing_tx_fallback(pool: PgPool) {
    let rpc = Arc::new(MockRpc::new(4663, 2));
    *rpc.logs.lock().unwrap() = vec![
        chain_log(2, Some(0), 0, 4),
        chain_log(1, None, 7, 3),
        chain_log(1, Some(1), 5, 2),
        chain_log(1, Some(0), 2, 1),
    ];
    let handler = Arc::new(RecordingHandler::default());
    coordinator(
        pool,
        rpc,
        Arc::new(MockWs::default()),
        handler.clone(),
        ChainHealth::default(),
    )
    .sync_once()
    .await
    .unwrap();
    let batches = handler.batches.lock().unwrap();
    let order: Vec<_> = batches[0]
        .logs
        .iter()
        .map(|log| {
            (
                log.block_number.get(),
                log.transaction_index,
                log.log_index.get(),
            )
        })
        .collect();
    assert_eq!(
        order,
        [
            (1, Some(0), 2),
            (1, Some(1), 5),
            (1, None, 7),
            (2, Some(0), 0)
        ]
    );
}
