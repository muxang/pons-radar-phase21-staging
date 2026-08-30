use pons_storage::repositories::SignalRepository;
use pons_v2::{SignalEngineConfig, evaluate_signals};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

fn config() -> SignalEngineConfig {
    SignalEngineConfig {
        windows: vec![30, 60, 180, 300, 900],
        minimum_independent: 1,
        minimum_qualified: 1,
        minimum_identity: Decimal::new(90, 2),
        watch: Decimal::from(10),
        strong: Decimal::from(60),
        high: Decimal::from(80),
        cooling: Decimal::from(70),
        cooling_exit: Decimal::new(40, 2),
        distribution_exit: Decimal::new(60, 2),
        minimum_confidence: Decimal::ZERO,
        high_max_age_seconds: 900,
        weights: [45, 25, 15, 10, 5],
        timing: [Decimal::ONE; 5],
        tiers: [Decimal::ONE; 5],
        rule_version: 17,
        weight_version: 18,
        calculation_version: 19,
    }
}

async fn seed_live_buy(pool: &PgPool) -> (Uuid, Uuid) {
    let token: Uuid = sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle)VALUES(4663,$1,$2,$3,1,to_timestamp(1700000000),'ACTIVE_CURVE')RETURNING id")
        .bind(vec![1_u8; 20]).bind(vec![2_u8; 20]).bind(vec![3_u8; 20])
        .fetch_one(pool).await.unwrap();
    let trader: Uuid = sqlx::query_scalar(
        "INSERT INTO traders(handle,manual_tier)VALUES('signal-test','S')RETURNING id",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let address = vec![4_u8; 20];
    let wallet: Uuid = sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified)VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true)RETURNING id")
        .bind(trader).bind(&address).fetch_one(pool).await.unwrap();
    let raw: Uuid = sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,10,$1,$2,1,$3,'[]','')RETURNING id")
        .bind(vec![5_u8; 32]).bind(vec![6_u8; 32]).bind(vec![2_u8; 20])
        .fetch_one(pool).await.unwrap();
    let token_trade: Uuid = sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status)SELECT 4663,$1,address,curve_address,'PONS_V2_CURVE_BUY','BUY',$2,$3,'100','50','2','1',10,$4,$5,0,1,to_timestamp(1700000010),$6,$7,'CONFIRMED'FROM tokens WHERE id=$1 RETURNING id")
        .bind(token).bind(vec![9_u8; 20]).bind(&address).bind(vec![5_u8; 32])
        .bind(vec![6_u8; 32]).bind(raw).bind(vec![7_u8; 32])
        .fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,launch_age_ms,buyer_rank,smart_buyer_rank,identity_snapshot,evidence,confirmed_at,classification_source,realtime_alert_eligible)VALUES($1,$2,$3,$4,$5,'BUY','BUY_CONFIRMED',1,1,'50','100','2','1',10,$6,1,to_timestamp(1700000010),10000,1,1,$7,'{}',now(),'LIVE',true)")
        .bind(token).bind(token_trade).bind(trader).bind(wallet).bind(address)
        .bind(vec![6_u8; 32]).bind(serde_json::json!({"identity_confidence":"1.0"}))
        .execute(pool).await.unwrap();
    (token, token_trade)
}

#[sqlx::test(migrations = "../../migrations")]
async fn durable_rebuild_versions_rules_outbox_and_orphan_removal(pool: PgPool) {
    let (token, token_trade) = seed_live_buy(&pool).await;
    let repo = SignalRepository::new(pool.clone());
    let cfg = config();
    repo.activate_rule_set(
        cfg.rule_version,
        cfg.weight_version,
        cfg.calculation_version,
        &serde_json::json!({"test":true}),
    )
    .await
    .unwrap();

    let job = repo.claim_due().await.unwrap().unwrap();
    let input = repo.load(job.token_id).await.unwrap();
    assert_eq!(input.trades.len(), 1);
    let result = evaluate_signals(&input, &job, &cfg).unwrap();
    assert_eq!(result.consensus.len(), 5);
    assert!(result.signals.iter().all(|s| {
        s.component_scores["research_narrative"]["status"].as_str() == Some("UNAVAILABLE")
    }));
    repo.persist(&job, &result).await.unwrap();

    let versions: (i32, i32, i32) = sqlx::query_as("SELECT rule_version,weight_version,calculation_version FROM signal_snapshots WHERE token_id=$1 AND current_generation ORDER BY effective_at DESC LIMIT 1")
        .bind(token).fetch_one(&pool).await.unwrap();
    assert_eq!(versions, (17, 18, 19));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM signal_rules")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM current_signal_states WHERE token_id=$1"
        )
        .bind(token)
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
    let live_events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM event_outbox WHERE event_type LIKE 'signal.%'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(live_events >= 1);

    sqlx::query("UPDATE token_trades SET status='ORPHANED' WHERE id=$1")
        .bind(token_trade)
        .execute(&pool)
        .await
        .unwrap();
    let orphan_job = repo.claim_due().await.unwrap().unwrap();
    let orphan_input = repo.load(token).await.unwrap();
    assert!(orphan_input.trades.is_empty());
    let orphan_result = evaluate_signals(&orphan_input, &orphan_job, &cfg).unwrap();
    assert!(orphan_result.signals.is_empty());
    repo.persist(&orphan_job, &orphan_result).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM current_signal_states WHERE token_id=$1"
        )
        .bind(token)
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type LIKE 'signal.%'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        live_events
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn stale_job_is_restart_safe_and_historical_origin_is_not_realtime(pool: PgPool) {
    let (token, _) = seed_live_buy(&pool).await;
    sqlx::query("UPDATE smart_trades SET classification_source='IDENTITY_BACKFILL',realtime_alert_eligible=false WHERE token_id=$1")
        .bind(token).execute(&pool).await.unwrap();
    sqlx::query("UPDATE signal_rebuild_jobs SET status='PROCESSING',locked_at=now()-interval '10 minutes',trigger_origin='IDENTITY_BACKFILL',trigger_realtime_eligible=false WHERE token_id=$1")
        .bind(token).execute(&pool).await.unwrap();
    let repo = SignalRepository::new(pool.clone());
    let job = repo.claim_due().await.unwrap().unwrap();
    assert_eq!(job.trigger_origin, "IDENTITY_BACKFILL");
    let result = evaluate_signals(&repo.load(token).await.unwrap(), &job, &config()).unwrap();
    assert!(result.signals.iter().all(|s| !s.realtime));
    assert!(result.transitions.iter().all(|s| !s.realtime));
    repo.persist(&job, &result).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type LIKE 'signal.%'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
}
