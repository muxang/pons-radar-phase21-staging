use chrono::{TimeDelta, Utc};
use pons_storage::repositories::IdentityClassificationRepository;
use sqlx::PgPool;
use uuid::Uuid;

async fn base(pool: &PgPool) -> (Uuid, Uuid, Vec<u8>) {
    let token:Uuid=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle) VALUES(4663,$1,$2,$3,1,now()-interval '2 days','ACTIVE_CURVE') RETURNING id").bind([1_u8;20].as_slice()).bind([2_u8;20].as_slice()).bind([3_u8;20].as_slice()).fetch_one(pool).await.unwrap();
    let trader: Uuid = sqlx::query_scalar("INSERT INTO traders(handle) VALUES($1) RETURNING id")
        .bind(format!("alice-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
    let address = vec![4_u8; 20];
    let wallet:Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified,enabled,valid_from) VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',0.95,true,true,now()-interval '1 day') RETURNING id").bind(trader).bind(&address).fetch_one(pool).await.unwrap();
    (token, wallet, address)
}
async fn trade(
    pool: &PgPool,
    token: Uuid,
    address: &[u8],
    side: &str,
    at: chrono::DateTime<Utc>,
    n: u8,
) -> Uuid {
    let tx = vec![n; 32];
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data) VALUES(4663,$1,$2,$3,$4,$5,'[]','') RETURNING id").bind(i32::from(n)).bind(vec![8_u8;32]).bind(&tx).bind(i32::from(n)).bind(vec![2_u8;20]).fetch_one(pool).await.unwrap();
    sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,log_index,block_time,raw_log_id,normalized_event_id) VALUES(4663,$1,$2,$3,$4,$5,$6,$7,'10','20','1','2',$8,$9,$10,$8,$11,$12,$13) RETURNING id")
 .bind(token).bind(vec![1_u8;20]).bind(vec![2_u8;20]).bind(if side=="BUY"{"PONS_V2_CURVE_BUY"}else{"PONS_V2_CURVE_SELL"}).bind(side)
 .bind(if side=="SELL"{address}else{&[9_u8;20]}).bind(if side=="BUY"{address}else{&[9_u8;20]}).bind(i32::from(n)).bind(vec![8_u8;32]).bind(tx).bind(at).bind(raw).bind(vec![n;32]).fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn historical_buy_sell_are_paginated_idempotent_and_non_realtime(pool: PgPool) {
    let (token, wallet, address) = base(&pool).await;
    let now = Utc::now();
    trade(&pool, token, &address, "BUY", now - TimeDelta::hours(2), 10).await;
    trade(
        &pool,
        token,
        &address,
        "SELL",
        now - TimeDelta::hours(1),
        11,
    )
    .await;
    let repo = IdentityClassificationRepository::new(pool.clone());
    assert!(
        repo.enqueue_eligible(wallet, "0.90")
            .await
            .unwrap()
            .is_some()
    );
    let first = repo.claim_due().await.unwrap().unwrap();
    let p = repo.process_page(&first, 1).await.unwrap();
    assert_eq!((p.scanned, p.created, p.complete), (1, 1, false));
    let second = repo.claim_due().await.unwrap().unwrap();
    let p = repo.process_page(&second, 1).await.unwrap();
    assert_eq!((p.scanned, p.created, p.complete), (1, 1, false));
    let third = repo.claim_due().await.unwrap().unwrap();
    assert!(repo.process_page(&third, 1).await.unwrap().complete);
    let rows:Vec<(String,bool,String,String)>=sqlx::query_as("SELECT classification_source,realtime_alert_eligible,confirmation_level,t.status FROM smart_trades s JOIN token_trades t ON t.id=s.token_trade_id ORDER BY s.side").fetch_all(&pool).await.unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.0 == "IDENTITY_BACKFILL"
        && !r.1
        && r.2.ends_with("STRONG")
        && r.3 == "PENDING"));
    assert!(
        repo.enqueue_eligible(wallet, "0.90")
            .await
            .unwrap()
            .is_some()
    );
    while let Some(j) = repo.claim_due().await.unwrap() {
        if repo.process_page(&j, 10).await.unwrap().complete {
            break;
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    sqlx::query("UPDATE trader_wallets SET enabled=false WHERE id=$1")
        .bind(wallet)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn eligibility_and_event_time_ranges_are_fail_closed(pool: PgPool) {
    let (token, wallet, address) = base(&pool).await;
    let now = Utc::now();
    trade(&pool, token, &address, "BUY", now - TimeDelta::days(2), 20).await;
    trade(&pool, token, &address, "BUY", now - TimeDelta::hours(1), 21).await;
    let repo = IdentityClassificationRepository::new(pool.clone());
    sqlx::query("UPDATE trader_wallets SET identity_confidence=0.50 WHERE id=$1")
        .bind(wallet)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        repo.enqueue_eligible(wallet, "0.90")
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query(
        "UPDATE trader_wallets SET identity_confidence=0.95,valid_from=$2,valid_to=$3 WHERE id=$1",
    )
    .bind(wallet)
    .bind(now - TimeDelta::hours(3))
    .bind(now)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        repo.enqueue_eligible(wallet, "0.90")
            .await
            .unwrap()
            .is_some()
    );
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.process_page(&job, 50).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM smart_trades")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn stale_processing_job_is_recovered(pool: PgPool) {
    let (_, wallet, _) = base(&pool).await;
    let repo = IdentityClassificationRepository::new(pool.clone());
    repo.enqueue_eligible(wallet, "0.90").await.unwrap();
    let claimed = repo.claim_due().await.unwrap().unwrap();
    sqlx::query(
        "UPDATE identity_classification_jobs SET locked_at=now()-interval '6 minutes' WHERE id=$1",
    )
    .bind(claimed.id)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(repo.claim_due().await.unwrap().unwrap().id, claimed.id);
}
