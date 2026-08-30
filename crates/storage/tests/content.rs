use chrono::{TimeZone, Utc};
use pons_storage::repositories::{ContentRepository, NewContentReference};
use serde_json::json;
use sqlx::PgPool;

async fn identities(pool: &PgPool, marker: u8) -> (uuid::Uuid, uuid::Uuid) {
    let token = sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,launch_time,lifecycle)VALUES(4663,$1,$2,$3,1,now()-interval '1 day','ACTIVE_CURVE')RETURNING id")
        .bind(vec![marker;20]).bind(vec![marker+1;20]).bind(vec![marker+2;20]).fetch_one(pool).await.unwrap();
    let trader = sqlx::query_scalar("INSERT INTO traders(handle)VALUES($1)RETURNING id")
        .bind(format!("content-{marker}"))
        .fetch_one(pool)
        .await
        .unwrap();
    (trader, token)
}

async fn smart_trade(
    pool: &PgPool,
    trader: uuid::Uuid,
    token: uuid::Uuid,
    marker: u8,
    side: &str,
    epoch: i64,
) -> uuid::Uuid {
    let wallet_address = vec![marker; 20];
    let wallet:uuid::Uuid=sqlx::query_scalar("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified)VALUES($1,4663,$2,'ROBINHOOD_EXECUTION_ADDRESS','OPERATOR_VERIFIED',1,true)RETURNING id").bind(trader).bind(&wallet_address).fetch_one(pool).await.unwrap();
    let tx = vec![marker; 32];
    let block_hash = vec![marker + 1; 32];
    let curve = vec![marker + 2; 20];
    let actor = vec![marker + 3; 20];
    let raw:uuid::Uuid=sqlx::query_scalar("INSERT INTO raw_chain_logs(chain_id,block_number,block_hash,tx_hash,log_index,address,topics,data)VALUES(4663,$1,$2,$3,0,$4,'[]','')RETURNING id").bind(epoch).bind(&block_hash).bind(&tx).bind(&curve).fetch_one(pool).await.unwrap();
    let trade:uuid::Uuid=sqlx::query_scalar("INSERT INTO token_trades(chain_id,token_id,token_address,curve_address,event_type,side,actor,recipient,quote_amount_raw,token_amount_raw,fee_raw,tax_raw,block_number,block_hash,tx_hash,transaction_index,log_index,block_time,raw_log_id,normalized_event_id,status)SELECT 4663,$1,address,$2,$3,$4,$5,$6,'10','5','1','1',$7,$8,$9,0,0,to_timestamp($7),$10,$11,'CONFIRMED'FROM tokens WHERE id=$1 RETURNING id").bind(token).bind(&curve).bind(if side=="BUY"{"PONS_V2_CURVE_BUY"}else{"PONS_V2_CURVE_SELL"}).bind(side).bind(&actor).bind(&wallet_address).bind(epoch).bind(&block_hash).bind(&tx).bind(raw).bind(vec![marker+4;32]).fetch_one(pool).await.unwrap();
    sqlx::query_scalar("INSERT INTO smart_trades(token_id,token_trade_id,trader_id,trader_wallet_id,wallet_address,side,confirmation_level,confirmation_confidence,confirmation_version,token_amount_raw,quote_amount_raw,fee_raw,tax_raw,block_number,tx_hash,log_index,block_time,identity_snapshot,evidence,confirmed_at,classification_source,realtime_alert_eligible)VALUES($1,$2,$3,$4,$5,$6,$7,1,1,'5','10','1','1',$8,$9,0,to_timestamp($8),'{}','{}',now(),'CHAIN_BACKFILL',false)RETURNING id").bind(token).bind(trade).bind(trader).bind(wallet).bind(wallet_address).bind(side).bind(if side=="BUY"{"BUY_CONFIRMED"}else{"SELL_CONFIRMED"}).bind(epoch).bind(tx).fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn manual_reference_is_bounded_idempotent_historical_and_never_raw(pool: PgPool) {
    let (trader, token) = identities(&pool, 21).await;
    let repo = ContentRepository::new(pool.clone());
    let hash = [9_u8; 32];
    let published = Utc.with_ymd_and_hms(2026, 8, 1, 12, 0, 0).unwrap();
    let input = NewContentReference {
        trader_id: trader,
        token_id: Some(token),
        content_type: "TRADE_THESIS",
        platform: "FOMO",
        external_reference: Some("https://example.test/reference/1"),
        published_at: published,
        title: Some("Thesis reference"),
        summary: Some("Operator-authored short summary"),
        stance: Some("BULLISH"),
        narratives: &json!(["launch"]),
        content_hash: &hash,
        provenance: &json!({"source":"operator"}),
    };
    let (id, created) = repo.create_manual(&input).await.unwrap();
    assert!(created);
    assert_eq!(repo.create_manual(&input).await.unwrap(), (id, false));
    let row:(bool,bool,bool,String)=sqlx::query_as("SELECT raw_content_available,raw_content_authorized,realtime_alert_eligible,authorization_basis FROM trader_content_items WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!(row, (false, false, false, "MANUAL_REFERENCE".into()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM authorized_raw_content")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert!(sqlx::query("UPDATE content_providers SET automatic_fetch_enabled=true WHERE provider_key='manual-reference'").execute(&pool).await.is_err());
    assert!(
        sqlx::query(
            "INSERT INTO authorized_raw_content(content_id,content)VALUES($1,'forbidden full text')"
        )
        .bind(id)
        .execute(&pool)
        .await
        .is_err()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn published_time_drives_relation_and_unknown_stance_is_not_guessed(pool: PgPool) {
    let (trader, token) = identities(&pool, 31).await;
    let repo = ContentRepository::new(pool.clone());
    let hash = [7_u8; 32];
    let published = Utc.with_ymd_and_hms(2023, 11, 14, 22, 13, 21).unwrap();
    repo.create_manual(&NewContentReference {
        trader_id: trader,
        token_id: Some(token),
        content_type: "TRADE_THESIS",
        platform: "FOMO",
        external_reference: None,
        published_at: published,
        title: None,
        summary: Some("Observed after the chain action, but published before it"),
        stance: Some("UNKNOWN"),
        narratives: &json!([]),
        content_hash: &hash,
        provenance: &json!({}),
    })
    .await
    .unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&job).await.unwrap();
    let relations: Vec<String> = sqlx::query_scalar(
        "SELECT relation_type FROM content_trade_relations ORDER BY relation_type",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(relations, vec!["CONTENT_WITHOUT_POSITION"]);
    let outbox:(bool,String)=sqlx::query_as("SELECT (payload->>'realtime_alert_eligible')::boolean,payload->>'classification_source' FROM event_outbox WHERE event_type='content.created'").fetch_one(&pool).await.unwrap();
    assert_eq!(outbox, (false, "MANUAL_REFERENCE".into()));
}

#[sqlx::test(migrations = "../../migrations")]
async fn symbol_only_content_is_not_automatically_bound(pool: PgPool) {
    let (trader, _) = identities(&pool, 41).await;
    let repo = ContentRepository::new(pool.clone());
    let hash = [4_u8; 32];
    repo.create_manual(&NewContentReference {
        trader_id: trader,
        token_id: None,
        content_type: "POST",
        platform: "FOMO",
        external_reference: None,
        published_at: Utc::now(),
        title: Some("ABC"),
        summary: Some("Mentions an ambiguous symbol only"),
        stance: None,
        narratives: &json!([]),
        content_hash: &hash,
        provenance: &json!({}),
    })
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM token_content_links")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*)FROM content_relation_jobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn late_content_rebuild_uses_published_at_and_structured_alignment(pool: PgPool) {
    let (trader, token) = identities(&pool, 51).await;
    smart_trade(&pool, trader, token, 52, "BUY", 1_700_000_100).await;
    smart_trade(&pool, trader, token, 53, "SELL", 1_700_000_200).await;
    let repo = ContentRepository::new(pool.clone());
    let hash = [5_u8; 32];
    repo.create_manual(&NewContentReference {
        trader_id: trader,
        token_id: Some(token),
        content_type: "TRADE_THESIS",
        platform: "FOMO",
        external_reference: None,
        published_at: Utc.timestamp_opt(1_700_000_150, 0).unwrap(),
        title: None,
        summary: Some("Published between buy and sell"),
        stance: Some("BULLISH"),
        narratives: &json!([]),
        content_hash: &hash,
        provenance: &json!({}),
    })
    .await
    .unwrap();
    let job = repo.claim_due().await.unwrap().unwrap();
    repo.rebuild(&job).await.unwrap();
    let kinds: Vec<String> = sqlx::query_scalar(
        "SELECT relation_type FROM content_trade_relations ORDER BY relation_type",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(kinds.contains(&"THESIS_AFTER_BUY".into()));
    assert!(kinds.contains(&"THESIS_BEFORE_SELL".into()));
    assert!(kinds.contains(&"CONTENT_POSITION_ALIGNED".into()));
    assert!(kinds.contains(&"CONTENT_POSITION_DIVERGENT".into()));
    let delta:Vec<i64>=sqlx::query_scalar("SELECT delta_ms FROM content_trade_relations WHERE relation_type LIKE 'THESIS_%'ORDER BY delta_ms").fetch_all(&pool).await.unwrap();
    assert_eq!(delta, vec![-50_000, 50_000]);
}
