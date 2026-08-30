use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress, LogIndex, RawAmount, TxHash};
use pons_storage::{
    MIGRATOR,
    repositories::{
        ChainCursorRepository, EventOutboxRepository, InsertRawLog, NewOutboxEvent,
        RawLogRepository,
    },
};
use serde_json::json;
use sqlx::PgPool;

fn raw_log(topics: &serde_json::Value) -> InsertRawLog<'_> {
    InsertRawLog {
        chain_id: ChainId::new(4663),
        block_number: BlockNumber::new(42),
        block_hash: BlockHash::from_slice(&[0x11; 32]).unwrap(),
        tx_hash: TxHash::from_slice(&[0x22; 32]).unwrap(),
        log_index: LogIndex::new(3),
        address: ContractAddress::from_slice(&[0x33; 20]).unwrap(),
        topics,
        data: &[0xab, 0xcd],
    }
}

async fn insert_normalized(
    pool: &PgPool,
    raw_log_id: uuid::Uuid,
    parser_version: i32,
    schema_version: i32,
) -> Result<Vec<u8>, sqlx::Error> {
    sqlx::query_scalar(
        r"INSERT INTO normalized_events
           (raw_log_id, chain_id, tx_hash, log_index, event_type,
            parser_version, schema_version, payload)
           VALUES ($1, 4663, $2, 3, 'fixture.event', $3, $4, '{}')
           RETURNING event_id",
    )
    .bind(raw_log_id)
    .bind([0x22_u8; 32].as_slice())
    .bind(parser_version)
    .bind(schema_version)
    .fetch_one(pool)
    .await
}

#[sqlx::test(migrations = "../../migrations")]
async fn empty_database_migrates_and_migration_rerun_is_safe(pool: PgPool) {
    MIGRATOR.run(&pool).await.unwrap();
    MIGRATOR.run(&pool).await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        r"
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        ",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for expected in [
        "alert_events",
        "app_settings",
        "audit_logs",
        "chain_blocks",
        "chain_cursors",
        "event_outbox",
        "normalized_events",
        "protocol_deployments",
        "raw_chain_logs",
        "tokens",
        "trader_wallets",
        "traders",
        "users",
    ] {
        assert!(
            tables.iter().any(|table| table == expected),
            "missing {expected}"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn raw_log_repository_is_idempotent_and_constraints_reject_duplicates(pool: PgPool) {
    let repository = RawLogRepository::new(pool.clone());
    let topics = json!(["0x01"]);
    let first = repository
        .insert_idempotent(&raw_log(&topics))
        .await
        .unwrap();
    let second = repository
        .insert_idempotent(&raw_log(&topics))
        .await
        .unwrap();
    assert_eq!(first, second);

    let conflicting_topics = json!(["0x02"]);
    assert!(
        repository
            .insert_idempotent(&raw_log(&conflicting_topics))
            .await
            .is_err()
    );

    let raw_count: i64 = sqlx::query_scalar("SELECT count(*) FROM raw_chain_logs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(raw_count, 1);

    let first_event = sqlx::query(
        r"INSERT INTO normalized_events
           (raw_log_id, chain_id, tx_hash, log_index, event_type, schema_version, payload)
           VALUES ($1, 4663, $2, 3, 'fixture.event', 1, '{}')",
    )
    .bind(first)
    .bind([0x22_u8; 32].as_slice())
    .execute(&pool)
    .await;
    assert!(first_event.is_ok());

    let duplicate = sqlx::query(
        r"INSERT INTO normalized_events
           (raw_log_id, chain_id, tx_hash, log_index, event_type, schema_version, payload)
           VALUES ($1, 4663, $2, 3, 'fixture.event', 1, '{}')",
    )
    .bind(first)
    .bind([0x22_u8; 32].as_slice())
    .execute(&pool)
    .await;
    assert!(duplicate.is_err());
}

#[sqlx::test(migrations = "../../migrations")]
async fn normalized_event_identity_is_versioned_and_deterministic(pool: PgPool) {
    let repository = RawLogRepository::new(pool.clone());
    let topics = json!(["0x01"]);
    let raw_log_id = repository
        .insert_idempotent(&raw_log(&topics))
        .await
        .unwrap();

    let v1_id = insert_normalized(&pool, raw_log_id, 1, 1).await.unwrap();
    assert_eq!(v1_id.len(), 32);
    assert!(insert_normalized(&pool, raw_log_id, 1, 1).await.is_err());

    let parser_v2_id = insert_normalized(&pool, raw_log_id, 2, 1).await.unwrap();
    let schema_v2_id = insert_normalized(&pool, raw_log_id, 1, 2).await.unwrap();
    assert_ne!(v1_id, parser_v2_id);
    assert_ne!(v1_id, schema_v2_id);
    assert_ne!(parser_v2_id, schema_v2_id);

    sqlx::query(
        "DELETE FROM normalized_events WHERE raw_log_id = $1 AND parser_version = 1 AND schema_version = 1",
    )
    .bind(raw_log_id)
    .execute(&pool)
    .await
    .unwrap();
    let rebuilt_v1_id = insert_normalized(&pool, raw_log_id, 1, 1).await.unwrap();
    assert_eq!(rebuilt_v1_id, v1_id);

    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM normalized_events WHERE raw_log_id = $1")
            .bind(raw_log_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 3);

    sqlx::query("DELETE FROM normalized_events WHERE raw_log_id = $1")
        .bind(raw_log_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM raw_chain_logs WHERE id = $1")
        .bind(raw_log_id)
        .execute(&pool)
        .await
        .unwrap();
    let rebuilt_raw_log_id = repository
        .insert_idempotent(&raw_log(&topics))
        .await
        .unwrap();
    assert_ne!(rebuilt_raw_log_id, raw_log_id);
    let full_rebuild_id = insert_normalized(&pool, rebuilt_raw_log_id, 1, 1)
        .await
        .unwrap();
    assert_eq!(full_rebuild_id, v1_id);
}

#[sqlx::test(migrations = "../../migrations")]
async fn database_checks_enforce_lossless_canonical_values(pool: PgPool) {
    let too_short_address = sqlx::query(
        "INSERT INTO tokens (chain_id, address, total_supply_raw) VALUES (4663, $1, '0')",
    )
    .bind([0_u8; 19].as_slice())
    .execute(&pool)
    .await;
    assert!(too_short_address.is_err());

    let non_canonical_amount = sqlx::query(
        "INSERT INTO tokens (chain_id, address, total_supply_raw) VALUES (4663, $1, '01')",
    )
    .bind([1_u8; 20].as_slice())
    .execute(&pool)
    .await;
    assert!(non_canonical_amount.is_err());

    let overflow_amount = sqlx::query(
        "INSERT INTO tokens (chain_id, address, total_supply_raw) VALUES (4663, $1, $2)",
    )
    .bind([3_u8; 20].as_slice())
    .bind("115792089237316195423570985008687907853269984665640564039457584007913129639936")
    .execute(&pool)
    .await;
    assert!(overflow_amount.is_err());

    let maximum = RawAmount::new(alloy_primitives::U256::MAX).to_storage_string();
    sqlx::query("INSERT INTO tokens (chain_id, address, total_supply_raw) VALUES (4663, $1, $2)")
        .bind([2_u8; 20].as_slice())
        .bind(&maximum)
        .execute(&pool)
        .await
        .unwrap();
    let stored: String =
        sqlx::query_scalar("SELECT total_supply_raw FROM tokens WHERE address = $1")
            .bind([2_u8; 20].as_slice())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored, maximum);
}

#[sqlx::test(migrations = "../../migrations")]
async fn outbox_sequence_is_monotonic_idempotent_and_replayable(pool: PgPool) {
    let repository = EventOutboxRepository::new(pool);
    let payload = json!({"fixture": true});
    let first_event = NewOutboxEvent {
        event_type: "fixture.first",
        schema_version: 1,
        aggregate_type: None,
        aggregate_id: None,
        dedupe_key: "fixture:first",
        payload: &payload,
    };
    let first = repository.append(&first_event).await.unwrap();
    let duplicate = repository.append(&first_event).await.unwrap();
    assert_eq!(first.seq, duplicate.seq);
    assert_eq!(first.id, duplicate.id);

    let second_event = NewOutboxEvent {
        event_type: "fixture.second",
        schema_version: 1,
        aggregate_type: None,
        aggregate_id: None,
        dedupe_key: "fixture:second",
        payload: &payload,
    };
    let second = repository.append(&second_event).await.unwrap();
    assert!(second.seq > first.seq);

    let replay = repository.after(first.seq, 100).await.unwrap();
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].id, second.id);
}

#[sqlx::test(migrations = "../../migrations")]
async fn chain_cursor_persists_for_restart_and_rejects_chain_changes(pool: PgPool) {
    let repository = ChainCursorRepository::new(pool);
    let hash = BlockHash::from_slice(&[0x44; 32]).unwrap();
    repository
        .upsert(
            "fixture-stream",
            ChainId::new(4663),
            BlockNumber::new(100),
            hash,
        )
        .await
        .unwrap();
    let restored = repository.get("fixture-stream").await.unwrap().unwrap();
    assert_eq!(restored.chain_id, ChainId::new(4663));
    assert_eq!(restored.last_processed_block, BlockNumber::new(100));
    assert_eq!(restored.last_processed_hash, hash);

    let next_hash = BlockHash::from_slice(&[0x45; 32]).unwrap();
    repository
        .upsert(
            "fixture-stream",
            ChainId::new(4663),
            BlockNumber::new(101),
            next_hash,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get("fixture-stream")
            .await
            .unwrap()
            .unwrap()
            .last_processed_block,
        BlockNumber::new(101)
    );
    assert!(
        repository
            .upsert(
                "fixture-stream",
                ChainId::new(1),
                BlockNumber::new(102),
                next_hash,
            )
            .await
            .is_err()
    );
}
