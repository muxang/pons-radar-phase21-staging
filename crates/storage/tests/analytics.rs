use chrono::{TimeZone, Utc};
use pons_storage::repositories::{PositionRepository, TraderAnalyticsRepository};
use rust_decimal::Decimal;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

struct Base {
    token: Uuid,
    trader: Uuid,
    wallet: Uuid,
    address: Vec<u8>,
}
async fn base(pool: &PgPool, m: u8) -> Base {
    let token=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle)VALUES(4663,$1,$2,$3,1,to_timestamp(1700000000),'ACTIVE_CURVE')RETURNING id").bind(vec![m;20]).bind(vec![m+1;20]).bind(vec![m+2;20]).fetch_one(pool).await.unwrap();
    let trader =
        sqlx::query_scalar("INSERT INTO traders(handle,manual_tier)VALUES($1,'S')RETURNING id")
            .bind(format!("analytics-{m}"))
            .fetch_one(pool)
            .await
            .unwrap();
    let address = vec![m + 3; 20];
    let wallet=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified)VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',0.95,true)RETURNING id").bind(trader).bind(&address).fetch_one(pool).await.unwrap();
    Base {
        token,
        trader,
        wallet,
        address,
    }
}
#[allow(clippy::too_many_arguments)]
async fn smart(
    pool: &PgPool,
    b: &Base,
    m: u8,
    block: i64,
    side: &str,
    token: &str,
    quote: &str,
) -> Uuid {
    let tx = vec![m; 32];
    let bh = vec![m + 1; 32];
    let curve = vec![m + 2; 20];
    let actor = vec![m + 4; 20];
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,$1,$2,$3,0,$4,'[]','')RETURNING id").bind(block).bind(&bh).bind(&tx).bind(&curve).fetch_one(pool).await.unwrap();
    let tt:Uuid=sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status)SELECT 4663,$1,address,$2,$3,$4,$5,$6,$7,$8,'0','0',$9,$10,$11,0,0,to_timestamp($9),$12,$13,'CONFIRMED'FROM tokens WHERE id=$1 RETURNING id").bind(b.token).bind(curve).bind(if side=="BUY"{"PONS_V2_CURVE_BUY"}else{"PONS_V2_CURVE_SELL"}).bind(side).bind(actor).bind(&b.address).bind(quote).bind(token).bind(block).bind(bh).bind(&tx).bind(raw).bind(vec![m+5;32]).fetch_one(pool).await.unwrap();
    sqlx::query_scalar("INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,launch_age_ms,buyer_rank,smart_buyer_rank,entry_price_quote,execution_price_scope,identity_snapshot,evidence,confirmed_at,classification_source,realtime_alert_eligible)VALUES($1,$2,$3,$4,$5,$6,$7,1,1,$8,$9,'0','0',$10,$11,0,to_timestamp($10),($10-1700000000)*1000,1,1,$9::numeric/$8::numeric,'EVENT_POSITION_EXACT','{}','{}',to_timestamp($10),'CHAIN_BACKFILL',false)RETURNING id").bind(b.token).bind(tt).bind(b.trader).bind(b.wallet).bind(&b.address).bind(side).bind(if side=="BUY"{"BUY_CONFIRMED"}else{"SELL_CONFIRMED"}).bind(token).bind(quote).bind(block).bind(tx).fetch_one(pool).await.unwrap()
}
async fn positions(pool: &PgPool, id: Uuid) {
    let repo = PositionRepository::new(pool.clone());
    repo.mark_dirty_for_smart_trade(id).await.unwrap();
    let j = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&j).await.unwrap();
}
async fn analytics(pool: &PgPool, trader: Uuid, now: i64) {
    let repo = TraderAnalyticsRepository::new(pool.clone());
    let j = repo.claim_due().await.unwrap().unwrap();
    assert_eq!(j.trader_id, trader);
    repo.rebuild(&j, Utc.timestamp_opt(now, 0).unwrap())
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn episodes_outcomes_and_as_of_are_rebuildable_without_lookahead(pool: PgPool) {
    let b = base(&pool, 70).await;
    let open = smart(&pool, &b, 71, 1_700_000_010, "BUY", "10", "100").await;
    smart(&pool, &b, 72, 1_700_000_020, "BUY", "5", "60").await;
    smart(&pool, &b, 73, 1_700_000_030, "SELL", "4", "50").await;
    let close = smart(&pool, &b, 74, 1_700_000_040, "SELL", "11", "140").await;
    positions(&pool, close).await;
    sqlx::query("INSERT INTO token_market_snapshots(token_id,snapshot_kind,snapshot_at,snapshot_block,age_since_launch_ms,buy_count,sell_count,unique_buyers,unique_sellers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw,smart_unique_buyers,smart_unique_sellers,smart_buy_quote_raw,smart_sell_quote_raw,smart_net_flow_raw,holder_count,price_basis,calculation_version,state_exact,state_scope,evidence,spot_price_quote,created_at)VALUES($1,'TEST_5M',to_timestamp(1700000310),310,310000,1,0,1,0,'100','0',100,'100','0',100,1,0,'100','0',100,1,'CURVE_SPOT',2,true,'BLOCK_STATE_EXACT','{}',15,to_timestamp(1700000310))").bind(b.token).execute(&pool).await.unwrap();
    analytics(&pool, b.trader, 1_700_000_610).await;
    let episodes: i64 = sqlx::query_scalar("SELECT count(*)FROM trader_position_episodes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(episodes, 1);
    let e: (i32, i32, Option<chrono::DateTime<Utc>>) =
        sqlx::query_as("SELECT add_count,reduce_count,closed_at FROM trader_position_episodes")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!((e.0, e.1, e.2.is_some()), (1, 1, true));
    let outcomes: Vec<(i32, String)> = sqlx::query_as(
        "SELECT horizon_seconds,status FROM trader_episode_outcomes ORDER BY horizon_seconds",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(outcomes[0], (300, "AVAILABLE".into()));
    assert_eq!(outcomes[1], (900, "PENDING".into()));
    let repo = TraderAnalyticsRepository::new(pool.clone());
    let before = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_000_100, 0).unwrap(),
            "KNOWLEDGE_TIME",
        )
        .await
        .unwrap()
        .unwrap();
    let after = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_000_400, 0).unwrap(),
            "KNOWLEDGE_TIME",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before["inputs"]["matured_horizons"], 0);
    assert_eq!(after["inputs"]["matured_horizons"], 1);
    assert_eq!(after["inputs"]["content_in_score"], false);
    assert_eq!(after["sample_size"], 1);
    assert!(Decimal::from_str(after["score"].as_str().unwrap()).unwrap() < Decimal::from(90));
    assert!(Decimal::from_str(after["confidence"].as_str().unwrap()).unwrap() < Decimal::from(60));
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT manual_tier FROM traders WHERE id=$1")
            .bind(b.trader)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "S"
    );
    let reopened = smart(&pool, &b, 75, 1_700_000_500, "BUY", "2", "30").await;
    positions(&pool, reopened).await;
    analytics(&pool, b.trader, 1_700_000_610).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM trader_position_episodes")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    let historical = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_000_100, 0).unwrap(),
            "KNOWLEDGE_TIME",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(historical["sample_size"], 1);
    let replay_trade: Uuid =
        sqlx::query_scalar("SELECT token_trade_id FROM smart_trades WHERE id=$1")
            .bind(reopened)
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE token_trades SET status='ORPHANED'WHERE id=$1")
        .bind(replay_trade)
        .execute(&pool)
        .await
        .unwrap();
    positions(&pool, reopened).await;
    analytics(&pool, b.trader, 1_700_000_610).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM trader_position_episodes")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    let view = repo.analytics(b.trader, 100).await.unwrap();
    assert_eq!(view["current"]["sample_size"], 1);
    assert_eq!(view["use_dynamic_trader_score"], false);
    let _: Uuid = open;
}

#[sqlx::test(migrations = "../../migrations")]
async fn post_graduation_missing_outcome_is_censored_not_failure(pool: PgPool) {
    let b = base(&pool, 80).await;
    let open = smart(&pool, &b, 81, 1_700_000_010, "BUY", "10", "100").await;
    positions(&pool, open).await;
    sqlx::query(
        "UPDATE tokens SET lifecycle='GRADUATED',updated_at=to_timestamp(1700000200)WHERE id=$1",
    )
    .bind(b.token)
    .execute(&pool)
    .await
    .unwrap();
    analytics(&pool, b.trader, 1_700_004_000).await;
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM trader_episode_outcomes ORDER BY horizon_seconds")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses[0], "CENSORED_POST_GRADUATION");
    assert_eq!(statuses[1], "CENSORED_POST_GRADUATION");
    assert!(statuses.contains(&"PENDING".into()));
}

#[sqlx::test(migrations = "../../migrations")]
async fn late_identity_chain_and_market_evidence_respect_knowledge_time(pool: PgPool) {
    for (marker, source) in [(90_u8, "IDENTITY_BACKFILL"), (100_u8, "CHAIN_BACKFILL")] {
        let b = base(&pool, marker).await;
        let trade = smart(&pool, &b, marker + 1, 1_700_000_010, "BUY", "10", "100").await;
        sqlx::query("UPDATE smart_trades SET classification_source=$2,confirmed_at=to_timestamp(1700777610)WHERE id=$1").bind(trade).bind(source).execute(&pool).await.unwrap();
        positions(&pool, trade).await;
        analytics(&pool, b.trader, 1_700_864_100).await;
        let repo = TraderAnalyticsRepository::new(pool.clone());
        assert!(
            repo.score_as_of(
                b.trader,
                Utc.timestamp_opt(1_700_345_600, 0).unwrap(),
                "KNOWLEDGE_TIME"
            )
            .await
            .unwrap()
            .is_none()
        );
        let reconstructed = repo
            .score_as_of(
                b.trader,
                Utc.timestamp_opt(1_700_345_600, 0).unwrap(),
                "EVENT_TIME_RECONSTRUCTED",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reconstructed["sample_size"], 1);
        assert_eq!(reconstructed["mode"], "EVENT_TIME_RECONSTRUCTED");
        let known = repo
            .score_as_of(
                b.trader,
                Utc.timestamp_opt(1_700_864_000, 0).unwrap(),
                "KNOWLEDGE_TIME",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(known["sample_size"], 1);
        assert_eq!(known["mode"], "KNOWLEDGE_TIME");
    }
    let b = base(&pool, 110).await;
    let trade = smart(&pool, &b, 111, 1_700_000_010, "BUY", "10", "100").await;
    positions(&pool, trade).await;
    sqlx::query("INSERT INTO token_market_snapshots(token_id,snapshot_kind,snapshot_at,snapshot_block,age_since_launch_ms,buy_count,sell_count,unique_buyers,unique_sellers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw,smart_unique_buyers,smart_unique_sellers,smart_buy_quote_raw,smart_sell_quote_raw,smart_net_flow_raw,holder_count,price_basis,calculation_version,state_exact,state_scope,evidence,spot_price_quote,created_at)VALUES($1,'LATE_5M',to_timestamp(1700000310),310,310000,1,0,1,0,'100','0',100,'100','0',100,1,0,'100','0',100,1,'CURVE_SPOT',2,true,'BLOCK_STATE_EXACT','{}',15,to_timestamp(1700001210))").bind(b.token).execute(&pool).await.unwrap();
    analytics(&pool, b.trader, 1_700_002_000).await;
    let repo = TraderAnalyticsRepository::new(pool);
    let before = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_000_400, 0).unwrap(),
            "KNOWLEDGE_TIME",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before["inputs"]["matured_horizons"], 0);
    let event = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_000_400, 0).unwrap(),
            "EVENT_TIME_RECONSTRUCTED",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event["inputs"]["matured_horizons"], 1);
    let after = repo
        .score_as_of(
            b.trader,
            Utc.timestamp_opt(1_700_001_300, 0).unwrap(),
            "KNOWLEDGE_TIME",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after["inputs"]["matured_horizons"], 1);
}
