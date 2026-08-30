use chrono::{TimeZone, Utc};
use pons_domain::{BlockHash, BlockNumber, ChainId, LogIndex, TokenAddress, TxHash, WalletAddress};
use pons_storage::repositories::{CurveObservation, MarketRepository, PersistTransfer};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

struct Token {
    id: Uuid,
    address: TokenAddress,
    curve: Vec<u8>,
}
async fn token(pool: &PgPool) -> Token {
    let address = TokenAddress::from_slice(&[1; 20]).unwrap();
    let curve = vec![2_u8; 20];
    let id=sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle)VALUES(4663,$1,$2,$3,1,to_timestamp(1700000000),'ACTIVE_CURVE')RETURNING id").bind(address.as_bytes().as_slice()).bind(&curve).bind(vec![3_u8;20]).fetch_one(pool).await.unwrap();
    Token { id, address, curve }
}

async fn smart_buy(
    pool: &PgPool,
    t: &Token,
    marker: u8,
    quote: &str,
    tokens: &str,
    fee: &str,
    tax: &str,
) {
    let trader: Uuid = sqlx::query_scalar("INSERT INTO traders(handle)VALUES($1)RETURNING id")
        .bind(format!("market-{marker}"))
        .fetch_one(pool)
        .await
        .unwrap();
    let wallet_address = vec![marker; 20];
    let wallet:Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified)VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true)RETURNING id").bind(trader).bind(&wallet_address).fetch_one(pool).await.unwrap();
    let tx = vec![marker; 32];
    let hash = vec![marker + 40; 32];
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,500,$1,$2,$3::numeric,$4,'[]','')RETURNING id").bind(&hash).bind(&tx).bind(marker.to_string()).bind(&t.curve).fetch_one(pool).await.unwrap();
    let trade:Uuid=sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status)VALUES(4663,$1,$2,$3,'PONS_V2_CURVE_BUY','BUY',$4,$5,$6,$7,$8,$9,500,$10,$11,$12::numeric,$13::numeric,to_timestamp(1700000500),$14,$15,'CONFIRMED')RETURNING id").bind(t.id).bind(t.address.as_bytes().as_slice()).bind(&t.curve).bind(vec![99_u8;20]).bind(&wallet_address).bind(quote).bind(tokens).bind(fee).bind(tax).bind(&hash).bind(&tx).bind(marker.to_string()).bind(marker.to_string()).bind(raw).bind(vec![marker;32]).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,identity_snapshot,evidence,confirmed_at,classification_source,realtime_alert_eligible,entry_price_quote,entry_curve_progress,entry_implied_fdv_quote,entry_market_state_exact)VALUES($1,$2,$3,$4,$5,'BUY','BUY_CONFIRMED',1,1,$6,$7,$8,$9,500,$10,$11::numeric,to_timestamp(1700000500),'{}','{}',now(),'LIVE',true,999,0.99,999,true)").bind(t.id).bind(trade).bind(trader).bind(wallet).bind(wallet_address).bind(tokens).bind(quote).bind(fee).bind(tax).bind(tx).bind(marker.to_string()).execute(pool).await.unwrap();
}
#[allow(clippy::too_many_arguments)]
async fn trade(
    pool: &PgPool,
    t: &Token,
    n: u8,
    block: u64,
    side: &str,
    actor: u8,
    recipient: u8,
    quote: &str,
    amount: &str,
    fee: &str,
    tax: &str,
) {
    let tx = vec![n; 32];
    let hash = vec![n + 20; 32];
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,$1::numeric,$2,$3,$1::numeric,$4,'[]','')RETURNING id").bind(block.to_string()).bind(&hash).bind(&tx).bind(&t.curve).fetch_one(pool).await.unwrap();
    sqlx::query("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status)VALUES(4663,$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::numeric,$13,$14,0,$12::numeric,to_timestamp(1700000000+$12::numeric),$15,$16,'CONFIRMED')").bind(t.id).bind(t.address.as_bytes().as_slice()).bind(&t.curve).bind(if side=="BUY"{"PONS_V2_CURVE_BUY"}else{"PONS_V2_CURVE_SELL"}).bind(side).bind(vec![actor;20]).bind(vec![recipient;20]).bind(quote).bind(amount).bind(fee).bind(tax).bind(block.to_string()).bind(hash).bind(tx).bind(raw).bind(vec![n;32]).execute(pool).await.unwrap();
}
async fn transfer(
    repo: &MarketRepository,
    t: &Token,
    n: u8,
    from: u8,
    to: u8,
    amount: &str,
    block: u64,
) {
    repo.persist_transfer(&PersistTransfer {
        chain_id: ChainId::new(4663),
        token_id: t.id,
        token: t.address,
        from: WalletAddress::from_slice(&[from; 20]).unwrap(),
        to: WalletAddress::from_slice(&[to; 20]).unwrap(),
        amount_raw: amount,
        block_number: BlockNumber::new(block),
        block_hash: BlockHash::from_slice(&[n + 50; 32]).unwrap(),
        tx_hash: TxHash::from_slice(&[n; 32]).unwrap(),
        transaction_index: Some(0),
        log_index: LogIndex::new(n.into()),
        block_time: Utc
            .timestamp_opt(1_700_000_000 + i64::try_from(block).unwrap(), 0)
            .unwrap(),
        topics: &json!([]),
        data: &[0; 32],
    })
    .await
    .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn volume_refund_identity_holder_and_snapshots_rebuild_exactly(pool: PgPool) {
    let t = token(&pool).await;
    let repo = MarketRepository::new(pool.clone());
    trade(&pool, &t, 10, 30, "BUY", 9, 4, "100", "50", "10", "5").await;
    trade(&pool, &t, 11, 60, "BUY", 9, 5, "20", "10", "2", "1").await;
    trade(&pool, &t, 12, 90, "BUY", 9, 6, "30", "15", "3", "2").await;
    trade(&pool, &t, 13, 120, "SELL", 4, 8, "40", "20", "4", "2").await;
    let raw:Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,100,$1,$2,99,$3,'[]','')RETURNING id").bind(vec![80_u8;32]).bind(vec![81_u8;32]).bind(&t.curve).fetch_one(&pool).await.unwrap();
    sqlx::query("INSERT INTO curve_accounting_events(chain_id,token_id,curve_address,event_type,actor,amount_raw,block_number,block_hash,tx_hash,log_index,block_time,raw_log_id,normalized_event_id)VALUES(4663,$1,$2,'PONS_V2_CURVE_BUY_REFUNDED',$3,'50',100,$4,$5,99,now(),$6,$7)").bind(t.id).bind(&t.curve).bind(vec![9_u8;20]).bind(vec![80_u8;32]).bind(vec![81_u8;32]).bind(raw).bind(vec![82_u8;32]).execute(&pool).await.unwrap();
    transfer(&repo, &t, 20, 0, 4, "100", 30).await;
    transfer(&repo, &t, 21, 4, 5, "40", 60).await;
    transfer(&repo, &t, 22, 5, 2, "40", 90).await;
    transfer(&repo, &t, 22, 5, 2, "40", 90).await;
    assert_eq!(repo.observation_blocks(t.id).await.unwrap().len(), 4);
    repo.save_observation(&CurveObservation {
        token_id: t.id,
        block_number: BlockNumber::new(30),
        quote_reserve_raw: "1000",
        token_reserve_raw: "2000",
        sellable_tokens_raw: "900",
        reserved_tokens_raw: "100",
        real_quote_reserve_raw: "100",
        graduation_threshold_raw: "1000",
        ready_to_graduate: false,
        token_decimals: 18,
        quote_decimals: 18,
        curve_progress: "0.100000000000000000",
        quote_progress: "0.100000000000000000",
        spot_price_quote: "0.500000000000000000",
        curve_implied_fdv_quote: "500.000000000000000000",
        integrity_warning: None,
        evidence: &json!({"source":"ETH_CALL_AT_BLOCK","block":30}),
    })
    .await
    .unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&job).await.unwrap();
    let m:(i64,i64,i64,String,String,String,String,String,String,i64,String,i64)=sqlx::query_as("SELECT buy_count,sell_count,unique_buyers,user_buy_volume_raw,user_sell_volume_raw,user_net_flow_raw::text,curve_effective_in_raw,curve_effective_out_raw,curve_effective_net_flow_raw::text,refund_count,refund_quote_total_raw,raw_holder_count FROM token_market_state").fetch_one(&pool).await.unwrap();
    assert_eq!(
        m,
        (
            3,
            1,
            3,
            "150".into(),
            "40".into(),
            "110".into(),
            "127".into(),
            "46".into(),
            "81".into(),
            1,
            "50".into(),
            1
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM token_transfers")
            .fetch_one(&pool)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM token_market_snapshots")
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM token_market_snapshots WHERE state_exact AND snapshot_block=30 AND curve_progress=0.1 AND spot_price_quote=0.5").fetch_one(&pool).await.unwrap(),1);
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM token_market_snapshots WHERE NOT state_exact AND evidence->>'curve_state'='UNAVAILABLE_AT_BLOCK'").fetch_one(&pool).await.unwrap(),6);
}

#[sqlx::test(migrations = "../../migrations")]
async fn block_end_context_never_impersonates_event_position_and_execution_price_is_exact(
    pool: PgPool,
) {
    let t = token(&pool).await;
    sqlx::query("UPDATE tokens SET decimals=6,total_supply_raw='1000000000000' WHERE id=$1")
        .bind(t.id)
        .execute(&pool)
        .await
        .unwrap();
    smart_buy(
        &pool,
        &t,
        31,
        "2000000000000000000",
        "1000000",
        "100000000000000000",
        "100000000000000000",
    )
    .await;
    smart_buy(&pool, &t, 32, "3000000000000000000", "1000000", "0", "0").await;
    smart_buy(&pool, &t, 33, "4000000000000000000", "1000000", "0", "0").await;
    let repo = MarketRepository::new(pool.clone());
    repo.save_observation(&CurveObservation {
        token_id: t.id,
        block_number: BlockNumber::new(500),
        quote_reserve_raw: "900",
        token_reserve_raw: "1000",
        sellable_tokens_raw: "100",
        reserved_tokens_raw: "100",
        real_quote_reserve_raw: "900",
        graduation_threshold_raw: "1000",
        ready_to_graduate: false,
        token_decimals: 6,
        quote_decimals: 18,
        curve_progress: "0.900000000000000000",
        quote_progress: "0.900000000000000000",
        spot_price_quote: "9.000000000000000000",
        curve_implied_fdv_quote: "9000.000000000000000000",
        integrity_warning: None,
        evidence: &json!({"source":"ETH_CALL_AT_BLOCK"}),
    })
    .await
    .unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&job).await.unwrap();
    // token_trades owns transaction_index; query through it for deterministic same-block order.
    let rows:Vec<(String,String,String,String,bool)>=sqlx::query_as("SELECT st.entry_price_quote::text,st.entry_net_execution_price_quote::text,st.execution_price_scope,st.entry_context_scope,st.entry_market_state_exact FROM smart_trades st JOIN token_trades tr ON tr.id=st.token_trade_id ORDER BY tr.transaction_index,tr.log_index").fetch_all(&pool).await.unwrap();
    assert_eq!(
        rows[0],
        (
            "2.0000000000000000".into(),
            "1.80000000000000000000".into(),
            "EVENT_POSITION_EXACT".into(),
            "BLOCK_STATE_EXACT".into(),
            false
        )
    );
    assert_eq!(rows[1].0, "3.0000000000000000");
    assert_eq!(rows[2].0, "4.0000000000000000");
    assert!(rows.iter().all(|v| v.3 == "BLOCK_STATE_EXACT" && !v.4));
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT state_scope FROM token_market_snapshots WHERE snapshot_kind='SMART_BUY_1'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        "BLOCK_STATE_EXACT"
    );
    assert_eq!(sqlx::query_scalar::<_,String>("SELECT evidence->>'state_position' FROM curve_state_observations WHERE token_id=$1 AND block_number=500").bind(t.id).fetch_one(&pool).await.unwrap(),"END_OF_BLOCK");
}
