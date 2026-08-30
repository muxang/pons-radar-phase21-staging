use pons_storage::repositories::{
    POSITION_BASIS, POSITION_CALCULATION_VERSION, PositionRepository,
};
use sqlx::PgPool;
use uuid::Uuid;

struct Base {
    token: Uuid,
    trader: Uuid,
    wallet: Uuid,
    address: Vec<u8>,
}
async fn base(pool: &PgPool, marker: u8) -> Base {
    let token:Uuid=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle) VALUES(4663,$1,$2,$3,1,now()-interval '2 days','ACTIVE_CURVE') RETURNING id").bind(vec![marker;20]).bind(vec![marker+1;20]).bind(vec![marker+2;20]).fetch_one(pool).await.unwrap();
    let trader: Uuid = sqlx::query_scalar("INSERT INTO traders(handle) VALUES($1) RETURNING id")
        .bind(format!("trader-{marker}"))
        .fetch_one(pool)
        .await
        .unwrap();
    let address = vec![marker + 3; 20];
    let wallet:Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified) VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true) RETURNING id").bind(trader).bind(&address).fetch_one(pool).await.unwrap();
    Base {
        token,
        trader,
        wallet,
        address,
    }
}
async fn identity_for_token(pool: &PgPool, token: Uuid, marker: u8) -> Base {
    let trader: Uuid = sqlx::query_scalar("INSERT INTO traders(handle) VALUES($1) RETURNING id")
        .bind(format!("identity-{marker}"))
        .fetch_one(pool)
        .await
        .unwrap();
    let address = vec![marker; 20];
    let wallet:Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified) VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true) RETURNING id").bind(trader).bind(&address).fetch_one(pool).await.unwrap();
    Base {
        token,
        trader,
        wallet,
        address,
    }
}
#[allow(clippy::too_many_arguments)]
async fn trade(
    pool: &PgPool,
    b: &Base,
    marker: u8,
    block: u64,
    txi: Option<u64>,
    log: u64,
    side: &str,
    token: &str,
    quote: &str,
    source: &str,
    status: &str,
) -> Uuid {
    let tx = vec![marker; 32];
    let hash = vec![marker.wrapping_add(40); 32];
    let curve = vec![marker.wrapping_add(20); 20];
    let other_actor = vec![marker + 5; 20];
    let other_recipient = vec![marker + 6; 20];
    let actor = if side == "SELL" {
        &b.address
    } else {
        &other_actor
    };
    let recipient = if side == "BUY" {
        &b.address
    } else {
        &other_recipient
    };
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data) VALUES(4663,$1::numeric,$2,$3,$4::numeric,$5,'[]','') RETURNING id").bind(block.to_string()).bind(&hash).bind(&tx).bind(log.to_string()).bind(&curve).fetch_one(pool).await.unwrap();
    let tt:Uuid=sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status) SELECT 4663,$1,t.address,$2,$3,$4,$5,$6,$7,$8,'0','0',$9::numeric,$10,$11,$12::numeric,$13::numeric,to_timestamp(1700000000+$9::numeric),$14,$15,$16 FROM tokens t WHERE t.id=$1 RETURNING id").bind(b.token).bind(&curve).bind(if side=="BUY"{"PONS_V2_CURVE_BUY"}else{"PONS_V2_CURVE_SELL"}).bind(side).bind(actor).bind(recipient).bind(quote).bind(token).bind(block.to_string()).bind(&hash).bind(&tx).bind(txi.map(|v|v.to_string())).bind(log.to_string()).bind(raw).bind(vec![marker;32]).bind(status).fetch_one(pool).await.unwrap();
    sqlx::query_scalar("INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,identity_snapshot,evidence,confirmed_at,classification_source,realtime_alert_eligible) VALUES($1,$2,$3,$4,$5,$6,$7,1,1,$8,$9,'0','0',$10::numeric,$11,$12::numeric,to_timestamp(1700000000+$10::numeric),'{}','{}',now(),$13,$14) RETURNING id").bind(b.token).bind(tt).bind(b.trader).bind(b.wallet).bind(&b.address).bind(side).bind(if side=="BUY"{"BUY_CONFIRMED"}else{"SELL_CONFIRMED"}).bind(token).bind(quote).bind(block.to_string()).bind(tx).bind(log.to_string()).bind(source).bind(source=="LIVE").fetch_one(pool).await.unwrap()
}
async fn rebuild(pool: &PgPool, smart: Uuid) -> pons_storage::repositories::PositionRebuildResult {
    let repo = PositionRepository::new(pool.clone());
    repo.mark_dirty_for_smart_trade(smart).await.unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&job).await.unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn chain_order_rebuilds_open_add_reduce_close_and_mixed_late_sources(pool: PgPool) {
    let b = base(&pool, 10).await;
    let later = trade(
        &pool,
        &b,
        11,
        103,
        Some(1),
        2,
        "BUY",
        "30",
        "60",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    trade(
        &pool,
        &b,
        12,
        108,
        Some(0),
        1,
        "SELL",
        "10",
        "25",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let r = rebuild(&pool, later).await;
    assert_eq!(r.balance_raw, "20");
    let late = trade(
        &pool,
        &b,
        13,
        100,
        Some(2),
        9,
        "BUY",
        "20",
        "40",
        "CHAIN_BACKFILL",
        "CONFIRMED",
    )
    .await;
    let r = rebuild(&pool, late).await;
    assert_eq!((r.events, r.balance_raw.as_str()), (3, "40"));
    let events:Vec<(String,String,String)>=sqlx::query_as("SELECT event_type,balance_before_raw,balance_after_raw FROM position_events ORDER BY block_number,COALESCE(transaction_index,log_index),log_index").fetch_all(&pool).await.unwrap();
    assert_eq!(
        events,
        vec![
            ("OPEN_POSITION".into(), "0".into(), "20".into()),
            ("ADD_POSITION".into(), "20".into(), "50".into()),
            ("REDUCE_POSITION".into(), "50".into(), "40".into())
        ]
    );
    let close = trade(
        &pool,
        &b,
        14,
        110,
        None,
        4,
        "SELL",
        "40",
        "90",
        "IDENTITY_BACKFILL",
        "CONFIRMED",
    )
    .await;
    let r = rebuild(&pool, close).await;
    assert_eq!(r.balance_raw, "0");
    let p: (bool, String, i32, String) = sqlx::query_as(
        "SELECT open,position_basis,calculation_version,balance_raw FROM wallet_token_positions",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        p,
        (
            false,
            POSITION_BASIS.into(),
            POSITION_CALCULATION_VERSION,
            "0".into()
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM position_events ORDER BY block_number DESC LIMIT 1"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "CLOSE_POSITION"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn overbalance_warns_without_clamp_and_u256_accumulation_is_lossless(pool: PgPool) {
    let b = base(&pool, 30).await;
    let max_minus_one =
        "115792089237316195423570985008687907853269984665640564039457584007913129639934";
    let first = trade(
        &pool,
        &b,
        31,
        1,
        Some(0),
        1,
        "BUY",
        max_minus_one,
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    trade(
        &pool,
        &b,
        32,
        2,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    trade(
        &pool,
        &b,
        33,
        3,
        Some(0),
        1,
        "SELL",
        "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let r = rebuild(&pool, first).await;
    assert_eq!(r.balance_raw, "0");
    let over = trade(
        &pool,
        &b,
        34,
        4,
        Some(0),
        1,
        "SELL",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let r = rebuild(&pool, over).await;
    assert_eq!((r.balance_raw.as_str(), r.warnings), ("0", 1));
    let warning:(String,String,String)=sqlx::query_as("SELECT event_type,balance_before_raw,balance_after_raw FROM position_events ORDER BY block_number DESC LIMIT 1").fetch_one(&pool).await.unwrap();
    assert_eq!(
        warning,
        ("POSITION_INTEGRITY_WARNING".into(), "0".into(), "0".into())
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn ranks_recompute_and_orphaned_trade_is_removed_on_restart_safe_rebuild(pool: PgPool) {
    let b = base(&pool, 50).await;
    let first = trade(
        &pool,
        &b,
        51,
        20,
        Some(0),
        1,
        "BUY",
        "10",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let trader2: Uuid =
        sqlx::query_scalar("INSERT INTO traders(handle) VALUES('rank-two') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
    let address2 = vec![88_u8; 20];
    let wallet2:Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified) VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true) RETURNING id").bind(trader2).bind(&address2).fetch_one(&pool).await.unwrap();
    let b2 = Base {
        token: b.token,
        trader: trader2,
        wallet: wallet2,
        address: address2,
    };
    let second = trade(
        &pool,
        &b2,
        58,
        30,
        Some(0),
        1,
        "BUY",
        "7",
        "1",
        "IDENTITY_BACKFILL",
        "CONFIRMED",
    )
    .await;
    let other_actor = vec![99_u8; 20];
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data) VALUES(4663,10,$1,$2,1,$3,'[]','') RETURNING id").bind(vec![90_u8;32]).bind(vec![91_u8;32]).bind(vec![92_u8;20]).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status) SELECT 4663,$1,address,$2,'PONS_V2_CURVE_BUY','BUY',$3,$3,'1','1','0','0',10,$4,$5,0,1,now(),$6,$7,'CONFIRMED' FROM tokens WHERE id=$1").bind(b.token).bind(vec![92_u8;20]).bind(other_actor).bind(vec![90_u8;32]).bind(vec![91_u8;32]).bind(raw).bind(vec![93_u8;32]).execute(&pool).await.unwrap();
    rebuild(&pool, first).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT buyer_rank FROM smart_trades WHERE id=$1")
            .bind(first)
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    rebuild(&pool, second).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT smart_buyer_rank FROM smart_trades WHERE id=$1")
            .bind(second)
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
    let late = trade(
        &pool,
        &b,
        52,
        5,
        Some(0),
        1,
        "BUY",
        "5",
        "1",
        "CHAIN_BACKFILL",
        "CONFIRMED",
    )
    .await;
    rebuild(&pool, late).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT smart_buyer_rank FROM smart_trades WHERE id=$1")
            .bind(first)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert!(sqlx::query_scalar::<_,bool>("SELECT bool_and(buyer_rank=1) FROM smart_trades WHERE trader_wallet_id=$1 AND side='BUY'").bind(b.wallet).fetch_one(&pool).await.unwrap());
    sqlx::query("UPDATE token_trades SET status='ORPHANED' WHERE id=(SELECT token_trade_id FROM smart_trades WHERE id=$1)").bind(late).execute(&pool).await.unwrap();
    let repo = PositionRepository::new(pool.clone());
    let claimed = repo.claim_due().await.unwrap().unwrap();
    sqlx::query("UPDATE position_rebuild_jobs SET locked_at=now()-interval '6 minutes' WHERE token_id=$1 AND trader_wallet_id=$2").bind(claimed.token_id).bind(claimed.trader_wallet_id).execute(&pool).await.unwrap();
    let recovered = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&recovered).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT balance_raw FROM wallet_token_positions WHERE trader_wallet_id=$1"
        )
        .bind(b.wallet)
        .fetch_one(&pool)
        .await
        .unwrap(),
        "10"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM position_events e JOIN wallet_token_positions p ON p.id=e.position_id WHERE p.trader_wallet_id=$1")
            .bind(b.wallet)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn router_buys_rank_unique_recipients_and_historical_recipient_reorders(pool: PgPool) {
    let alice = base(&pool, 110).await;
    let bob = identity_for_token(&pool, alice.token, 120).await;
    let tom = identity_for_token(&pool, alice.token, 130).await;
    let router = vec![200_u8; 20];
    let a1 = trade(
        &pool,
        &alice,
        111,
        10,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let b1 = trade(
        &pool,
        &bob,
        121,
        20,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let t1 = trade(
        &pool,
        &tom,
        131,
        30,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    let a2 = trade(
        &pool,
        &alice,
        112,
        40,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "LIVE",
        "CONFIRMED",
    )
    .await;
    sqlx::query("UPDATE token_trades SET actor=$1 WHERE id IN (SELECT token_trade_id FROM smart_trades WHERE id=ANY($2))").bind(&router).bind(vec![a1,b1,t1]).execute(&pool).await.unwrap();
    sqlx::query("UPDATE token_trades SET actor=recipient WHERE id=(SELECT token_trade_id FROM smart_trades WHERE id=$1)").bind(a2).execute(&pool).await.unwrap();
    rebuild(&pool, a1).await;
    for (id, buyer, smart) in [(a1, 1_i64, 1_i64), (a2, 1, 1), (b1, 2, 2), (t1, 3, 3)] {
        let rank: (i64, i64) =
            sqlx::query_as("SELECT buyer_rank,smart_buyer_rank FROM smart_trades WHERE id=$1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rank, (buyer, smart));
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(DISTINCT recipient) FROM token_trades WHERE token_id=$1 AND side='BUY'"
        )
        .bind(alice.token)
        .fetch_one(&pool)
        .await
        .unwrap(),
        3
    );
    assert_eq!(sqlx::query_scalar::<_,Vec<u8>>("SELECT actor FROM token_trades WHERE id=(SELECT token_trade_id FROM smart_trades WHERE id=$1)").bind(a1).fetch_one(&pool).await.unwrap(),router,"raw protocol actor remains Router");
    let zoe = identity_for_token(&pool, alice.token, 140).await;
    let z1 = trade(
        &pool,
        &zoe,
        141,
        5,
        Some(0),
        1,
        "BUY",
        "1",
        "1",
        "CHAIN_BACKFILL",
        "CONFIRMED",
    )
    .await;
    sqlx::query("UPDATE token_trades SET actor=$1 WHERE id=(SELECT token_trade_id FROM smart_trades WHERE id=$2)").bind(&router).bind(z1).execute(&pool).await.unwrap();
    rebuild(&pool, z1).await;
    for (id, rank) in [(z1, 1_i64), (a1, 2), (a2, 2), (b1, 3), (t1, 4)] {
        let value: (i64, i64) =
            sqlx::query_as("SELECT buyer_rank,smart_buyer_rank FROM smart_trades WHERE id=$1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(value, (rank, rank));
    }
}
