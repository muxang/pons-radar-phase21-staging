use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::{Address, U256};
use alloy_sol_types::{SolCall, sol};
use async_trait::async_trait;
use pons_chain::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};
use pons_domain::{BlockNumber, ChainId, ContractAddress, TxHash};
use pons_storage::repositories::TokenMetadataRepository;
use pons_v2::{DESCRIPTION_LIMIT, MetadataWorkerSettings, TokenMetadataWorker};
use sqlx::PgPool;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

#[derive(serde::Deserialize)]
struct CapturedFixture {
    calls: HashMap<String, CapturedCall>,
}
#[derive(serde::Deserialize)]
struct CapturedCall {
    selector: String,
    result: String,
}

sol! {
    struct TokenSocials { string twitter; string telegram; string discord; string website; string farcaster; }
    function name() external view returns (string value);
    function symbol() external view returns (string value);
    function decimals() external view returns (uint8 value);
    function totalSupply() external view returns (uint256 value);
    function getTokenInfo() external view returns (address tokenDeployer, string tokenLogo, string tokenDescription, TokenSocials tokenSocials);
}

#[derive(Clone)]
enum Reply {
    Value(Vec<u8>),
    Error(String),
    Delay(Duration, Vec<u8>),
    Flaky(Arc<AtomicUsize>, Vec<u8>),
}

#[derive(Clone)]
struct Rpc {
    replies: Arc<RwLock<HashMap<[u8; 4], Reply>>>,
    broken: Arc<RwLock<HashSet<ContractAddress>>>,
    unavailable_blocks: Arc<RwLock<HashSet<BlockNumber>>>,
    called_blocks: Arc<RwLock<Vec<BlockNumber>>>,
}

impl Rpc {
    fn profile(deployer: Address, description: &str) -> Self {
        let mut replies = HashMap::new();
        replies.insert(
            nameCall::SELECTOR,
            Reply::Value(nameCall::abi_encode_returns(&"Ponso".into())),
        );
        replies.insert(
            symbolCall::SELECTOR,
            Reply::Value(symbolCall::abi_encode_returns(&"PONSO".into())),
        );
        replies.insert(
            decimalsCall::SELECTOR,
            Reply::Value(decimalsCall::abi_encode_returns(&18)),
        );
        replies.insert(
            totalSupplyCall::SELECTOR,
            Reply::Value(totalSupplyCall::abi_encode_returns(&U256::from(
                1_000_000_u64,
            ))),
        );
        replies.insert(
            getTokenInfoCall::SELECTOR,
            Reply::Value(getTokenInfoCall::abi_encode_returns(&getTokenInfoReturn {
                tokenDeployer: deployer,
                tokenLogo: "ipfs://pons-logo".into(),
                tokenDescription: description.into(),
                tokenSocials: TokenSocials {
                    twitter: "https://x.com/pons".into(),
                    telegram: "https://t.me/pons".into(),
                    discord: "javascript:alert(1)".into(),
                    website: "https://pons.family/path".into(),
                    farcaster: "@pons".into(),
                },
            })),
        );
        Self {
            replies: Arc::new(RwLock::new(replies)),
            broken: Arc::new(RwLock::new(HashSet::new())),
            unavailable_blocks: Arc::new(RwLock::new(HashSet::new())),
            called_blocks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    async fn set_info(&self, deployer: Address, description: String) {
        self.replies.write().await.insert(
            getTokenInfoCall::SELECTOR,
            Reply::Value(getTokenInfoCall::abi_encode_returns(&getTokenInfoReturn {
                tokenDeployer: deployer,
                tokenLogo: "ipfs://pons-logo".into(),
                tokenDescription: description,
                tokenSocials: TokenSocials {
                    twitter: "https://x.com/pons".into(),
                    telegram: "https://t.me/pons".into(),
                    discord: "javascript:alert(1)".into(),
                    website: "https://pons.family/path".into(),
                    farcaster: "@pons".into(),
                },
            })),
        );
    }

    fn captured() -> Self {
        let fixture: CapturedFixture = serde_json::from_str(include_str!(
            "../../../fixtures/pons_v2/token_metadata_0xf9b84b5f.json"
        ))
        .unwrap();
        let replies = fixture
            .calls
            .into_values()
            .map(|call| {
                let selector: [u8; 4] = decode_hex(&call.selector).try_into().unwrap();
                (selector, Reply::Value(decode_hex(&call.result)))
            })
            .collect();
        Self {
            replies: Arc::new(RwLock::new(replies)),
            broken: Arc::new(RwLock::new(HashSet::new())),
            unavailable_blocks: Arc::new(RwLock::new(HashSet::new())),
            called_blocks: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

fn decode_hex(value: &str) -> Vec<u8> {
    let digits = value.strip_prefix("0x").unwrap();
    (0..digits.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&digits[index..index + 2], 16).unwrap())
        .collect()
}

#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(4663))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(35_064_968))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(vec![1])
    }
    async fn call(
        &self,
        address: ContractAddress,
        data: Vec<u8>,
        block: BlockNumber,
    ) -> Result<Vec<u8>, RunError> {
        self.called_blocks.write().await.push(block);
        if self.unavailable_blocks.read().await.contains(&block) {
            return Err(RunError::Rpc("historical state unavailable".into()));
        }
        if self.broken.read().await.contains(&address) {
            return Err(RunError::Rpc("malicious token reverted".into()));
        }
        let selector: [u8; 4] = data
            .get(..4)
            .ok_or_else(|| RunError::Rpc("missing selector".into()))?
            .try_into()
            .unwrap();
        match self
            .replies
            .read()
            .await
            .get(&selector)
            .cloned()
            .ok_or_else(|| RunError::Rpc("unknown selector".into()))?
        {
            Reply::Value(value) => Ok(value),
            Reply::Error(error) => Err(RunError::Rpc(error)),
            Reply::Delay(delay, value) => {
                tokio::time::sleep(delay).await;
                Ok(value)
            }
            Reply::Flaky(remaining, value) => {
                if remaining.swap(0, Ordering::SeqCst) > 0 {
                    Err(RunError::Rpc("temporary failure".into()))
                } else {
                    Ok(value)
                }
            }
        }
    }
    async fn block(&self, _: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        Ok(None)
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

fn settings(timeout: Duration) -> MetadataWorkerSettings {
    MetadataWorkerSettings {
        concurrency: 2,
        rpc_timeout: timeout,
        poll_interval: Duration::from_millis(5),
        retry_minimum: Duration::from_millis(10),
        retry_maximum: Duration::from_millis(20),
        refresh_interval: Duration::from_secs(3600),
        historical_attempts_before_fallback: 2,
    }
}

async fn insert_token(pool: &PgPool, suffix: u8, deployer: Address) -> uuid::Uuid {
    let mut address = [0_u8; 20];
    address[19] = suffix;
    let id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO tokens(chain_id,address,deployer,launch_block,lifecycle) VALUES(4663,$1,$2,100,'ACTIVE_CURVE') RETURNING id",
    ).bind(address.as_slice()).bind(deployer.as_slice()).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO token_metadata_jobs(token_id,requested_block) VALUES($1,100)")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    id
}

async fn process_due(pool: &PgPool, rpc: Rpc, timeout: Duration) {
    let repository = TokenMetadataRepository::new(pool.clone());
    let job = repository.claim_due().await.unwrap().unwrap();
    TokenMetadataWorker::new(repository, Arc::new(rpc), settings(timeout))
        .unwrap()
        .process(&job)
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn captured_phase_four_token_metadata_fixture_decodes_offline(pool: PgPool) {
    let deployer = Address::from_slice(&decode_hex("0x5be0405dc84593fddbeccb80abb9d8cb0df75519"));
    let token_id = insert_token(&pool, 7, deployer).await;
    process_due(&pool, Rpc::captured(), Duration::from_secs(1)).await;
    let row: (String, String, String, String, bool) = sqlx::query_as(
        "SELECT name,symbol,token_description,twitter,deployer_matches_launch FROM token_metadata_current WHERE token_id=$1",
    )
    .bind(token_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "Yuyhu Seree");
    assert_eq!(row.1, "YUYHU");
    assert_eq!(row.2, "launchblitz.ai");
    assert!(row.3.starts_with("https://x.com/"));
    assert!(row.4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn standard_and_v2_metadata_original_current_history_and_deployer_match(pool: PgPool) {
    let deployer = Address::repeat_byte(0x11);
    let token_id = insert_token(&pool, 1, deployer).await;
    let rpc = Rpc::profile(deployer, "original description");
    process_due(&pool, rpc.clone(), Duration::from_secs(1)).await;
    assert_eq!(
        rpc.called_blocks.read().await.as_slice(),
        &[BlockNumber::new(100); 5]
    );

    let row: (String,String,i16,String,bool,serde_json::Value) = sqlx::query_as(
        "SELECT name,symbol,decimals,total_supply_raw,deployer_matches_launch,normalized_socials FROM token_metadata_original WHERE token_id=$1"
    ).bind(token_id).fetch_one(&pool).await.unwrap();
    assert_eq!(
        (&row.0, &row.1, row.2, &row.3),
        (&"Ponso".into(), &"PONSO".into(), 18, &"1000000".into())
    );
    assert!(row.4);
    assert!(row.5["website"].as_str().unwrap().starts_with("https://"));
    assert!(row.5["discord"].is_null());
    let evidence: (String, bool, String, String) = sqlx::query_as(
        "SELECT capture_mode,exact_launch_snapshot,requested_block::text,observed_block::text FROM token_metadata_original WHERE token_id=$1",
    )
    .bind(token_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        evidence,
        ("LAUNCH_BLOCK".into(), true, "100".into(), "100".into())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM token_metadata_snapshots WHERE token_id=$1"
        )
        .bind(token_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    sqlx::query(
        "UPDATE token_metadata_jobs SET status='PENDING',next_attempt_at=now() WHERE token_id=$1",
    )
    .bind(token_id)
    .execute(&pool)
    .await
    .unwrap();
    process_due(&pool, rpc.clone(), Duration::from_secs(1)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM token_metadata_snapshots WHERE token_id=$1"
        )
        .bind(token_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );

    rpc.set_info(deployer, "changed description".into()).await;
    sqlx::query(
        "UPDATE token_metadata_jobs SET status='PENDING',next_attempt_at=now() WHERE token_id=$1",
    )
    .bind(token_id)
    .execute(&pool)
    .await
    .unwrap();
    process_due(&pool, rpc, Duration::from_secs(1)).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM token_metadata_snapshots WHERE token_id=$1"
        )
        .bind(token_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT token_description FROM token_metadata_original WHERE token_id=$1"
        )
        .bind(token_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "original description"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT token_description FROM token_metadata_current WHERE token_id=$1"
        )
        .bind(token_id)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "changed description"
    );
    assert!(
        sqlx::query("UPDATE token_metadata_original SET name='overwrite' WHERE token_id=$1")
            .bind(token_id)
            .execute(&pool)
            .await
            .is_err()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn mismatch_is_warning_and_oversized_untrusted_content_is_bounded(pool: PgPool) {
    let launch_deployer = Address::repeat_byte(0x11);
    let token_id = insert_token(&pool, 2, launch_deployer).await;
    let rpc = Rpc::profile(Address::repeat_byte(0x22), "initial");
    rpc.set_info(
        Address::repeat_byte(0x22),
        "🦀".repeat(DESCRIPTION_LIMIT + 20),
    )
    .await;
    rpc.replies.write().await.insert(
        nameCall::SELECTOR,
        Reply::Value(nameCall::abi_encode_returns(&"N".repeat(300))),
    );
    process_due(&pool, rpc, Duration::from_secs(1)).await;
    let row:(bool,Option<String>,String,String,serde_json::Value)=sqlx::query_as("SELECT deployer_matches_launch,integrity_warning,name,token_description,raw_metadata FROM token_metadata_current WHERE token_id=$1").bind(token_id).fetch_one(&pool).await.unwrap();
    assert!(!row.0);
    assert!(row.1.unwrap().contains("differs"));
    assert_eq!(row.2.chars().count(), 256);
    assert_eq!(row.3.chars().count(), DESCRIPTION_LIMIT);
    assert!(row.4["truncated_fields"].as_array().unwrap().len() >= 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn revert_and_timeout_are_durable_retry_state(pool: PgPool) {
    let deployer = Address::repeat_byte(0x11);
    let revert_id = insert_token(&pool, 3, deployer).await;
    let rpc = Rpc::profile(deployer, "ok");
    rpc.replies.write().await.insert(
        nameCall::SELECTOR,
        Reply::Error("execution reverted".into()),
    );
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(pool.clone()),
        Arc::new(rpc),
        settings(Duration::from_millis(30)),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(worker.run_until(cancel.clone()));
    tokio::time::sleep(Duration::from_millis(45)).await;
    cancel.cancel();
    task.await.unwrap().unwrap();
    let row: (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status,attempts,last_error FROM token_metadata_jobs WHERE token_id=$1",
    )
    .bind(revert_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, "RETRY");
    assert!(row.1 >= 1);
    assert!(row.2.unwrap().contains("reverted"));

    sqlx::query(
        "UPDATE token_metadata_jobs SET next_attempt_at=now()+interval '1 hour' WHERE token_id=$1",
    )
    .bind(revert_id)
    .execute(&pool)
    .await
    .unwrap();
    let timeout_id = insert_token(&pool, 4, deployer).await;
    let slow = Rpc::profile(deployer, "ok");
    let encoded = nameCall::abi_encode_returns(&"Ponso".into());
    slow.replies.write().await.insert(
        nameCall::SELECTOR,
        Reply::Delay(Duration::from_millis(100), encoded),
    );
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(pool.clone()),
        Arc::new(slow),
        settings(Duration::from_millis(10)),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(worker.run_until(cancel.clone()));
    tokio::time::sleep(Duration::from_millis(35)).await;
    cancel.cancel();
    task.await.unwrap().unwrap();
    let error: Option<String> =
        sqlx::query_scalar("SELECT last_error FROM token_metadata_jobs WHERE token_id=$1")
            .bind(timeout_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(error.unwrap().contains("timed out"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn transient_failure_retries_then_succeeds(pool: PgPool) {
    let deployer = Address::repeat_byte(0x11);
    let token_id = insert_token(&pool, 8, deployer).await;
    let rpc = Rpc::profile(deployer, "eventual success");
    rpc.replies.write().await.insert(
        nameCall::SELECTOR,
        Reply::Flaky(
            Arc::new(AtomicUsize::new(1)),
            nameCall::abi_encode_returns(&"Ponso".into()),
        ),
    );
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(pool.clone()),
        Arc::new(rpc),
        settings(Duration::from_secs(1)),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(worker.run_until(cancel.clone()));
    for _ in 0..100 {
        let state: (String, i32) =
            sqlx::query_as("SELECT status,attempts FROM token_metadata_jobs WHERE token_id=$1")
                .bind(token_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if state.0 == "SUCCEEDED" {
            assert!(state.1 >= 2);
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
    let state: String =
        sqlx::query_scalar("SELECT status FROM token_metadata_jobs WHERE token_id=$1")
            .bind(token_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "SUCCEEDED");
    let evidence: (String, bool, String) = sqlx::query_as(
        "SELECT capture_mode,exact_launch_snapshot,observed_block::text FROM token_metadata_original WHERE token_id=$1",
    )
    .bind(token_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(evidence, ("LAUNCH_BLOCK".into(), true, "100".into()));
}

#[sqlx::test(migrations = "../../migrations")]
async fn unavailable_historical_state_falls_back_with_explicit_non_exact_evidence(pool: PgPool) {
    let deployer = Address::repeat_byte(0x11);
    let token_id = insert_token(&pool, 9, deployer).await;
    let cursor_hash = [0x44_u8; 32];
    sqlx::query("INSERT INTO chain_cursors(stream,chain_id,last_processed_block,last_processed_hash) VALUES('phase-5.1-proof',4663,777,$1)")
        .bind(cursor_hash.as_slice()).execute(&pool).await.unwrap();
    let rpc = Rpc::profile(deployer, "fallback");
    rpc.unavailable_blocks
        .write()
        .await
        .insert(BlockNumber::new(100));
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(pool.clone()),
        Arc::new(rpc.clone()),
        settings(Duration::from_secs(1)),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(worker.run_until(cancel.clone()));
    for _ in 0..150 {
        let state: String =
            sqlx::query_scalar("SELECT status FROM token_metadata_jobs WHERE token_id=$1")
                .bind(token_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if state == "SUCCEEDED" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
    let evidence: (String, bool, String, String, i32) = sqlx::query_as(
        "SELECT o.capture_mode,o.exact_launch_snapshot,o.requested_block::text,o.observed_block::text,j.historical_attempts FROM token_metadata_original o JOIN token_metadata_jobs j USING(token_id) WHERE o.token_id=$1",
    ).bind(token_id).fetch_one(&pool).await.unwrap();
    assert_eq!(
        evidence,
        (
            "FIRST_AVAILABLE".into(),
            false,
            "100".into(),
            "35064968".into(),
            2
        )
    );
    assert!(
        rpc.called_blocks
            .read()
            .await
            .contains(&BlockNumber::new(100))
    );
    assert!(
        rpc.called_blocks
            .read()
            .await
            .contains(&BlockNumber::new(35_064_968))
    );
    let cursor: String = sqlx::query_scalar(
        "SELECT last_processed_block::text FROM chain_cursors WHERE stream='phase-5.1-proof'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(cursor, "777");
}

#[sqlx::test(migrations = "../../migrations")]
async fn restart_resumes_pending_and_broken_token_does_not_block_another(pool: PgPool) {
    let deployer = Address::repeat_byte(0x11);
    let broken = insert_token(&pool, 5, deployer).await;
    let good = insert_token(&pool, 6, deployer).await;
    let rpc = Rpc::profile(deployer, "ok");
    let mut broken_bytes = [0_u8; 20];
    broken_bytes[19] = 5;
    rpc.broken
        .write()
        .await
        .insert(ContractAddress::from_slice(&broken_bytes).unwrap());
    // The first process simulates a prior process dying after claim; make it stale for restart recovery.
    let repository = TokenMetadataRepository::new(pool.clone());
    let claimed = repository.claim_due().await.unwrap().unwrap();
    sqlx::query(
        "UPDATE token_metadata_jobs SET locked_at=now()-interval '10 minutes' WHERE token_id=$1",
    )
    .bind(claimed.token_id)
    .execute(&pool)
    .await
    .unwrap();
    rpc.replies.write().await.insert(
        symbolCall::SELECTOR,
        Reply::Value(symbolCall::abi_encode_returns(&"PONSO".into())),
    );
    let worker = TokenMetadataWorker::new(
        TokenMetadataRepository::new(pool.clone()),
        Arc::new(rpc),
        settings(Duration::from_secs(1)),
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(worker.run_until(cancel.clone()));
    for _ in 0..100 {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM token_metadata_jobs WHERE token_id=$1 AND status='SUCCEEDED'",
        )
        .bind(good)
        .fetch_one(&pool)
        .await
        .unwrap();
        if count == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cancel.cancel();
    task.await.unwrap().unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM token_metadata_current WHERE token_id IN ($1,$2)")
            .bind(broken)
            .bind(good)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let broken_state: String =
        sqlx::query_scalar("SELECT status FROM token_metadata_jobs WHERE token_id=$1")
            .bind(broken)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(broken_state, "RETRY");
}
