use pons_storage::repositories::{
    AlertPreferenceChanges, AlertRepository, EventOutboxRepository, NewOutboxEvent,
};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

async fn source(pool: &PgPool, kind: &str, key: &str, aggregate: Uuid, payload: serde_json::Value) {
    EventOutboxRepository::new(pool.clone())
        .append(&NewOutboxEvent {
            event_type: kind,
            schema_version: 1,
            aggregate_type: Some(if kind.starts_with("signal.") {
                "token"
            } else {
                "smart_trade"
            }),
            aggregate_id: Some(aggregate),
            dedupe_key: key,
            payload: &payload,
        })
        .await
        .unwrap();
}
async fn drain(repo: &AlertRepository) {
    for _ in 0..100 {
        if !repo.process_next().await.unwrap() {
            break;
        }
    }
}
async fn token(pool: &PgPool, marker: u8) -> Uuid {
    sqlx::query_scalar("INSERT INTO tokens(chain_id,address,curve_address,deployer,launch_block,lifecycle)VALUES(4663,$1,$2,$3,1,'ACTIVE_CURVE')RETURNING id").bind(vec![marker;20]).bind(vec![marker+1;20]).bind(vec![marker+2;20]).fetch_one(pool).await.unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn updater_success_and_rollback_failure_create_durable_alerts(pool: PgPool) {
    let repo = AlertRepository::new(pool.clone());
    source(
        &pool,
        "system.update_applied",
        "update-ok",
        Uuid::new_v4(),
        json!({"realtime_alert_eligible":true,"old_version":"1.0.0","new_version":"1.1.0"}),
    )
    .await;
    source(
        &pool,
        "system.update_rollback_failed",
        "update-critical",
        Uuid::new_v4(),
        json!({"realtime_alert_eligible":true}),
    )
    .await;
    drain(&repo).await;
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT alert_type,severity FROM alert_events ORDER BY created_at,id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![
            ("SYSTEM_UPDATE".into(), "INFO".into()),
            ("SYSTEM_WARNING".into(), "CRITICAL".into())
        ]
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn realtime_historical_types_dedupe_and_finality_updates(pool: PgPool) {
    let repo = AlertRepository::new(pool.clone());
    let smart = Uuid::new_v4();
    let signal_token = token(&pool, 30).await;
    source(&pool,"smart_trade.buy_confirmed","buy-live",smart,json!({"realtime_alert_eligible":true,"classification_source":"LIVE","chain_finality":"PENDING","event_effective_at":"2026-08-01T00:00:00Z"})).await;
    source(
        &pool,
        "smart_trade.buy_backfilled",
        "buy-history",
        signal_token,
        json!({"realtime_alert_eligible":false,"classification_source":"CHAIN_BACKFILL"}),
    )
    .await;
    source(
        &pool,
        "signal.strong_watch",
        "strong",
        signal_token,
        json!({"realtime_alert_eligible":true,"score":"70"}),
    )
    .await;
    source(
        &pool,
        "signal.high_priority",
        "high",
        signal_token,
        json!({"realtime_alert_eligible":true,"score":"90"}),
    )
    .await;
    source(
        &pool,
        "position.close",
        "close",
        Uuid::new_v4(),
        json!({"realtime_alert_eligible":true}),
    )
    .await;
    source(
        &pool,
        "signal.distribution",
        "distribution",
        signal_token,
        json!({"realtime_alert_eligible":true}),
    )
    .await;
    drain(&repo).await;
    let live: (bool, bool, String) = sqlx::query_as(
        "SELECT realtime_alert_eligible,provisional,status FROM alert_events WHERE semantic_key=$1",
    )
    .bind(format!("SMART_BUY:{smart}"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(live, (true, true, "ACTIVE".into()));
    let historical:(bool,String)=sqlx::query_as("SELECT realtime_alert_eligible,classification_source FROM alert_events WHERE dedupe_key='alert:buy-history'").fetch_one(&pool).await.unwrap();
    assert_eq!(historical, (false, "CHAIN_BACKFILL".into()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM alert_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        6
    );
    source(&pool,"smart_trade.buy_confirmed","buy-confirmed",smart,json!({"realtime_alert_eligible":true,"classification_source":"LIVE","chain_finality":"CONFIRMED"})).await;
    drain(&repo).await;
    let state: (bool, String) =
        sqlx::query_as("SELECT provisional,status FROM alert_events WHERE semantic_key=$1")
            .bind(format!("SMART_BUY:{smart}"))
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, (false, "ACTIVE".into()));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM alert_events")
            .fetch_one(&pool)
            .await
            .unwrap(),
        6
    );
    source(&pool,"smart_trade.buy_confirmed","buy-orphan",smart,json!({"realtime_alert_eligible":true,"classification_source":"LIVE","chain_finality":"ORPHANED"})).await;
    drain(&repo).await;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM alert_events WHERE semantic_key=$1")
            .bind(format!("SMART_BUY:{smart}"))
            .fetch_one(&pool)
            .await
            .unwrap(),
        "RETRACTED"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM event_outbox WHERE event_type='alert.retracted'"
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn preferences_and_alert_center_pagination_are_durable(pool: PgPool) {
    let repo = AlertRepository::new(pool.clone());
    let signal_token = token(&pool, 40).await;
    let user:Uuid=sqlx::query_scalar("INSERT INTO users(username,password_hash,role)VALUES('alerts','$argon2id$fixture','ADMIN')RETURNING id").fetch_one(&pool).await.unwrap();
    let defaults = repo.preferences(user).await.unwrap();
    assert!(defaults.sound_enabled);
    let saved = repo
        .save_preferences(
            user,
            &AlertPreferenceChanges {
                sound_enabled: false,
                voice_enabled: false,
                desktop_notifications_enabled: false,
                speak_strong: true,
                speak_high_priority: true,
                speak_wallet_close: true,
                speak_distribution: true,
                speak_system_update: true,
                smart_buy_alerts: false,
                provisional_alerts: false,
                minimum_signal_score: "50".into(),
                minimum_smart_trade_amount: Some("1000000000000000000".into()),
            },
        )
        .await
        .unwrap();
    assert!(!saved.sound_enabled);
    source(
        &pool,
        "signal.watch",
        "page-1",
        signal_token,
        json!({"event_effective_at":"2026-08-01T00:00:01Z"}),
    )
    .await;
    source(
        &pool,
        "signal.watch",
        "page-2",
        signal_token,
        json!({"event_effective_at":"2026-08-01T00:00:02Z"}),
    )
    .await;
    drain(&repo).await;
    let first = repo.list(None, 1).await.unwrap();
    assert_eq!(first.len(), 1);
    let second = repo
        .list(Some(first[0].event_effective_at), 1)
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_ne!(first[0].id, second[0].id);
    assert!(repo.mark(first[0].id, true, true).await.unwrap());
}
