use super::{EventOutboxRepository, NewOutboxEvent, OutboxEvent};
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct AlertRepository {
    pool: PgPool,
}
#[derive(Clone, Debug, FromRow)]
pub struct AlertRecord {
    pub id: Uuid,
    pub seq: i64,
    pub alert_type: String,
    pub severity: String,
    pub token_id: Option<Uuid>,
    pub trader_id: Option<Uuid>,
    pub title: String,
    pub message: String,
    pub speech_text: Option<String>,
    pub payload: Value,
    pub realtime_alert_eligible: bool,
    pub provisional: bool,
    pub chain_finality: Option<String>,
    pub event_effective_at: DateTime<Utc>,
    pub classification_source: Option<String>,
    pub status: String,
    pub read_at: Option<DateTime<Utc>>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub target_reference: Option<String>,
    pub created_at: DateTime<Utc>,
    pub dedupe_key: String,
}
#[derive(Clone, Debug, FromRow, serde::Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct AlertPreferences {
    pub sound_enabled: bool,
    pub voice_enabled: bool,
    pub desktop_notifications_enabled: bool,
    pub speak_strong: bool,
    pub speak_high_priority: bool,
    pub speak_wallet_close: bool,
    pub speak_distribution: bool,
    pub speak_system_update: bool,
    pub smart_buy_alerts: bool,
    pub provisional_alerts: bool,
    pub minimum_signal_score: String,
    pub minimum_smart_trade_amount: Option<String>,
}
#[derive(Clone, Debug, serde::Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct AlertPreferenceChanges {
    pub sound_enabled: bool,
    pub voice_enabled: bool,
    pub desktop_notifications_enabled: bool,
    pub speak_strong: bool,
    pub speak_high_priority: bool,
    pub speak_wallet_close: bool,
    pub speak_distribution: bool,
    pub speak_system_update: bool,
    pub smart_buy_alerts: bool,
    pub provisional_alerts: bool,
    pub minimum_signal_score: String,
    pub minimum_smart_trade_amount: Option<String>,
}
struct Draft {
    kind: &'static str,
    severity: &'static str,
    title: &'static str,
    speech: Option<&'static str>,
}

#[allow(clippy::missing_errors_doc)]
impl AlertRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    #[allow(clippy::too_many_lines)]
    pub async fn process_next(&self) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let cursor: i64 = sqlx::query_scalar(
            "SELECT last_processed_seq FROM alert_engine_cursor WHERE singleton FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let Some(source):Option<OutboxEvent>=sqlx::query_as("SELECT seq,id,event_type,schema_version,aggregate_type,aggregate_id,dedupe_key,payload,created_at FROM event_outbox WHERE seq>$1 ORDER BY seq LIMIT 1").bind(cursor).fetch_optional(&mut*tx).await? else{tx.commit().await?;return Ok(false)};
        if let Some(d) = classify(&source) {
            let classification = string(
                &source.payload,
                &["classification_source", "classification_origin"],
            );
            let realtime = source
                .payload
                .get("realtime_alert_eligible")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                && !classification
                    .as_deref()
                    .is_some_and(|v| v.contains("BACKFILL"));
            let chain = string(&source.payload, &["chain_finality", "chain_status"]);
            let provisional = chain.as_deref() == Some("PENDING");
            let retracted = chain.as_deref() == Some("ORPHANED");
            let effective = source
                .payload
                .get("event_effective_at")
                .or_else(|| source.payload.get("block_time"))
                .and_then(Value::as_str)
                .and_then(|v| v.parse().ok())
                .unwrap_or(source.created_at);
            let mut token = uuid(&source.payload, "token_id").or_else(|| {
                (source.aggregate_type.as_deref() == Some("token"))
                    .then_some(source.aggregate_id)
                    .flatten()
            });
            let mut trader = uuid(&source.payload, "trader_id");
            if source.event_type.starts_with("smart_trade.") {
                if let Some(smart_trade_id) = source.aggregate_id {
                    if let Some((token_id, trader_id)) = sqlx::query_as::<_, (Uuid, Uuid)>(
                        "SELECT token_id,trader_id FROM smart_trades WHERE id=$1",
                    )
                    .bind(smart_trade_id)
                    .fetch_optional(&mut *tx)
                    .await?
                    {
                        token = Some(token_id);
                        trader = Some(trader_id);
                    }
                }
            }
            let semantic = if source.event_type.starts_with("smart_trade.") {
                format!(
                    "{}:{}",
                    d.kind,
                    source
                        .aggregate_id
                        .map_or_else(|| source.dedupe_key.clone(), |v| v.to_string())
                )
            } else {
                format!("{}:{}", d.kind, source.dedupe_key)
            };
            let id:Uuid=sqlx::query_scalar("INSERT INTO alert_events(seq,token_id,alert_type,severity,title,message,speech_text,payload,dedupe_key,source_outbox_seq,source_event_type,semantic_key,trader_id,realtime_alert_eligible,provisional,chain_finality,event_effective_at,classification_source,status,target_reference)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)ON CONFLICT(semantic_key)DO UPDATE SET payload=EXCLUDED.payload,provisional=EXCLUDED.provisional,chain_finality=EXCLUDED.chain_finality,status=EXCLUDED.status,realtime_alert_eligible=EXCLUDED.realtime_alert_eligible RETURNING id").bind(source.seq).bind(token).bind(d.kind).bind(d.severity).bind(d.title).bind(d.title).bind(d.speech).bind(&source.payload).bind(format!("alert:{}",source.dedupe_key)).bind(source.seq).bind(&source.event_type).bind(&semantic).bind(trader).bind(realtime).bind(provisional).bind(&chain).bind(effective).bind(&classification).bind(if retracted{"RETRACTED"}else{"ACTIVE"}).bind(token.map(|v|format!("/tokens/{v}"))).fetch_one(&mut*tx).await?;
            let p = json!({"alert_id":id,"alert_type":d.kind,"severity":d.severity,"title":d.title,"message":d.title,"speech_text":d.speech,"token_id":token,"trader_id":trader,"realtime_alert_eligible":realtime,"provisional":provisional,"chain_finality":chain,"classification_source":classification,"event_effective_at":effective,"status":if retracted{"RETRACTED"}else{"ACTIVE"},"target_reference":token.map(|v|format!("/tokens/{v}"))});
            EventOutboxRepository::append_in_transaction(
                &mut tx,
                &NewOutboxEvent {
                    event_type: if retracted {
                        "alert.retracted"
                    } else {
                        "alert.created"
                    },
                    schema_version: 1,
                    aggregate_type: Some("alert"),
                    aggregate_id: Some(id),
                    dedupe_key: &format!("alert.delivery:{}", source.seq),
                    payload: &p,
                },
            )
            .await?;
        }
        sqlx::query(
            "UPDATE alert_engine_cursor SET last_processed_seq=$1,updated_at=now() WHERE singleton",
        )
        .bind(source.seq)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }
    pub async fn list(
        &self,
        before: Option<DateTime<Utc>>,
        limit: i64,
    ) -> Result<Vec<AlertRecord>, sqlx::Error> {
        sqlx::query_as("SELECT id,seq,alert_type,severity,token_id,trader_id,title,message,speech_text,payload,realtime_alert_eligible,provisional,chain_finality,event_effective_at,classification_source,status,read_at,acknowledged_at,target_reference,created_at,dedupe_key FROM alert_events WHERE $1::timestamptz IS NULL OR event_effective_at<$1 ORDER BY event_effective_at DESC,id DESC LIMIT $2").bind(before).bind(limit).fetch_all(&self.pool).await
    }
    pub async fn mark(&self, id: Uuid, read: bool, ack: bool) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query("UPDATE alert_events SET read_at=CASE WHEN $2 THEN COALESCE(read_at,now())ELSE read_at END,acknowledged_at=CASE WHEN $3 THEN COALESCE(acknowledged_at,now())ELSE acknowledged_at END WHERE id=$1").bind(id).bind(read).bind(ack).execute(&self.pool).await?.rows_affected()==1)
    }
    pub async fn preferences(&self, user: Uuid) -> Result<AlertPreferences, sqlx::Error> {
        sqlx::query_as("INSERT INTO alert_preferences(user_id)VALUES($1)ON CONFLICT(user_id)DO UPDATE SET user_id=EXCLUDED.user_id RETURNING sound_enabled,voice_enabled,desktop_notifications_enabled,speak_strong,speak_high_priority,speak_wallet_close,speak_distribution,speak_system_update,smart_buy_alerts,provisional_alerts,minimum_signal_score::text,minimum_smart_trade_amount").bind(user).fetch_one(&self.pool).await
    }
    pub async fn save_preferences(
        &self,
        user: Uuid,
        v: &AlertPreferenceChanges,
    ) -> Result<AlertPreferences, sqlx::Error> {
        sqlx::query_as("INSERT INTO alert_preferences(user_id,sound_enabled,voice_enabled,desktop_notifications_enabled,speak_strong,speak_high_priority,speak_wallet_close,speak_distribution,speak_system_update,smart_buy_alerts,provisional_alerts,minimum_signal_score,minimum_smart_trade_amount)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::numeric,$13)ON CONFLICT(user_id)DO UPDATE SET sound_enabled=EXCLUDED.sound_enabled,voice_enabled=EXCLUDED.voice_enabled,desktop_notifications_enabled=EXCLUDED.desktop_notifications_enabled,speak_strong=EXCLUDED.speak_strong,speak_high_priority=EXCLUDED.speak_high_priority,speak_wallet_close=EXCLUDED.speak_wallet_close,speak_distribution=EXCLUDED.speak_distribution,speak_system_update=EXCLUDED.speak_system_update,smart_buy_alerts=EXCLUDED.smart_buy_alerts,provisional_alerts=EXCLUDED.provisional_alerts,minimum_signal_score=EXCLUDED.minimum_signal_score,minimum_smart_trade_amount=EXCLUDED.minimum_smart_trade_amount,updated_at=now()RETURNING sound_enabled,voice_enabled,desktop_notifications_enabled,speak_strong,speak_high_priority,speak_wallet_close,speak_distribution,speak_system_update,smart_buy_alerts,provisional_alerts,minimum_signal_score::text,minimum_smart_trade_amount").bind(user).bind(v.sound_enabled).bind(v.voice_enabled).bind(v.desktop_notifications_enabled).bind(v.speak_strong).bind(v.speak_high_priority).bind(v.speak_wallet_close).bind(v.speak_distribution).bind(v.speak_system_update).bind(v.smart_buy_alerts).bind(v.provisional_alerts).bind(&v.minimum_signal_score).bind(&v.minimum_smart_trade_amount).fetch_one(&self.pool).await
    }
}
fn classify(v: &OutboxEvent) -> Option<Draft> {
    Some(match v.event_type.as_str() {
        "smart_trade.buy_confirmed" | "smart_trade.buy_backfilled" => Draft {
            kind: "SMART_BUY",
            severity: "INFO",
            title: "Smart BUY",
            speech: Some("重点钱包买入代币。"),
        },
        "smart_trade.sell_confirmed" | "smart_trade.sell_backfilled" => Draft {
            kind: "SMART_SELL",
            severity: "STRONG",
            title: "Smart SELL",
            speech: Some("重点钱包卖出代币。"),
        },
        "position.open" => Draft {
            kind: "SMART_POSITION_OPEN",
            severity: "INFO",
            title: "重点钱包建仓",
            speech: None,
        },
        "position.add" => Draft {
            kind: "SMART_POSITION_ADD",
            severity: "WATCH",
            title: "重点钱包加仓",
            speech: None,
        },
        "position.reduce" => Draft {
            kind: "SMART_POSITION_REDUCE",
            severity: "WATCH",
            title: "重点钱包减仓",
            speech: None,
        },
        "position.close" => Draft {
            kind: "SMART_POSITION_CLOSE",
            severity: "HIGH",
            title: "重点钱包清仓",
            speech: Some("风险提醒，重点钱包已清仓。"),
        },
        "signal.watch" => Draft {
            kind: "SIGNAL_WATCH",
            severity: "WATCH",
            title: "关注信号",
            speech: None,
        },
        "signal.strong_watch" => Draft {
            kind: "SIGNAL_STRONG",
            severity: "STRONG",
            title: "强关注信号",
            speech: Some("代币出现强关注信号。"),
        },
        "signal.high_priority" => Draft {
            kind: "SIGNAL_HIGH_PRIORITY",
            severity: "HIGH",
            title: "高优先级信号",
            speech: Some("高优先级提醒，多个重点钱包共同买入。"),
        },
        "signal.cooling" => Draft {
            kind: "SIGNAL_COOLING",
            severity: "WATCH",
            title: "信号降温",
            speech: None,
        },
        "signal.distribution" => Draft {
            kind: "SIGNAL_DISTRIBUTION",
            severity: "HIGH",
            title: "重点钱包集中退出",
            speech: Some("风险提醒，代币出现重点钱包集中退出。"),
        },
        "system.warning" => Draft {
            kind: "SYSTEM_WARNING",
            severity: "CRITICAL_SYSTEM",
            title: "系统警告",
            speech: None,
        },
        "system.update" | "system.update_applied" => Draft {
            kind: "SYSTEM_UPDATE",
            severity: "INFO",
            title: "系统升级完成",
            speech: Some("系统升级已经完成，请刷新页面。"),
        },
        "system.update_failed" | "system.update_rolled_back" => Draft {
            kind: "SYSTEM_WARNING",
            severity: "HIGH",
            title: "System update failed and was rolled back",
            speech: None,
        },
        "system.update_rollback_failed" => Draft {
            kind: "SYSTEM_WARNING",
            severity: "CRITICAL",
            title: "CRITICAL: system update rollback failed",
            speech: None,
        },
        _ => return None,
    })
}
fn string(v: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|k| v.get(*k)?.as_str().map(str::to_owned))
}
fn uuid(v: &Value, key: &str) -> Option<Uuid> {
    v.get(key)?.as_str()?.parse().ok()
}
