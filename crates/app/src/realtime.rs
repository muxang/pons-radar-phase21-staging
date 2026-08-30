use std::time::Duration;

use chrono::{DateTime, Utc};
use pons_storage::repositories::{EventOutboxRepository, OutboxEvent};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::version::{API_SCHEMA_VERSION, APP_VERSION, FRONTEND_BUILD_ID};

#[derive(Clone, Debug)]
pub struct RealtimeSettings {
    pub poll_interval: Duration,
    pub heartbeat_interval: Duration,
    pub client_queue_capacity: usize,
    pub replay_limit_max: i64,
}

impl Default for RealtimeSettings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_secs(15),
            client_queue_capacity: 256,
            replay_limit_max: 500,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct EventEnvelope {
    pub seq: i64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub schema_version: i32,
    pub server_version: &'static str,
    pub frontend_build_id: &'static str,
    pub timestamp: DateTime<Utc>,
    pub realtime_alert_eligible: bool,
    pub classification_source: Option<String>,
    pub trade_evidence: Option<String>,
    pub chain_finality: Option<String>,
    pub signal_finality: Option<String>,
    pub provisional: bool,
    pub data: Value,
}

impl From<OutboxEvent> for EventEnvelope {
    fn from(value: OutboxEvent) -> Self {
        let string = |keys: &[&str]| {
            keys.iter()
                .find_map(|key| value.payload.get(*key)?.as_str().map(str::to_owned))
        };
        let chain_finality = string(&["chain_finality", "chain_status"]);
        Self {
            seq: value.seq,
            event_type: value.event_type,
            schema_version: value.schema_version,
            server_version: APP_VERSION,
            frontend_build_id: FRONTEND_BUILD_ID,
            timestamp: value.created_at,
            realtime_alert_eligible: value
                .payload
                .get("realtime_alert_eligible")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            classification_source: string(&["classification_source", "classification_origin"]),
            trade_evidence: string(&["trade_evidence", "confirmation_level"]),
            provisional: chain_finality.as_deref() == Some("PENDING"),
            chain_finality,
            signal_finality: string(&["signal_finality"]),
            data: value.payload,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct HelloEnvelope {
    pub seq: i64,
    #[serde(rename = "type")]
    pub event_type: &'static str,
    pub schema_version: u32,
    pub server_version: &'static str,
    pub frontend_build_id: &'static str,
    pub api_schema_version: u32,
    pub current_outbox_seq: i64,
    pub server_time: DateTime<Utc>,
}

impl HelloEnvelope {
    #[must_use]
    pub fn new(high_watermark: i64) -> Self {
        Self {
            seq: high_watermark,
            event_type: "system.hello",
            schema_version: 1,
            server_version: APP_VERSION,
            frontend_build_id: FRONTEND_BUILD_ID,
            api_schema_version: API_SCHEMA_VERSION,
            current_outbox_seq: high_watermark,
            server_time: Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct EventHub {
    repository: EventOutboxRepository,
    sender: broadcast::Sender<EventEnvelope>,
    settings: RealtimeSettings,
}

impl EventHub {
    #[must_use]
    pub fn new(repository: EventOutboxRepository, settings: RealtimeSettings) -> Self {
        let (sender, _) = broadcast::channel(settings.client_queue_capacity.max(1));
        Self {
            repository,
            sender,
            settings,
        }
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.sender.subscribe()
    }

    #[must_use]
    pub const fn repository(&self) -> &EventOutboxRepository {
        &self.repository
    }

    #[must_use]
    pub const fn settings(&self) -> &RealtimeSettings {
        &self.settings
    }

    pub async fn run(self, shutdown: CancellationToken) {
        let mut cursor = self.repository.publisher_cursor().await.unwrap_or(0);
        let mut interval = tokio::time::interval(self.settings.poll_interval);
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let Ok(events) = self.repository.after(cursor, 256).await else { continue };
                    for event in events {
                        cursor = event.seq;
                        let _ = self.sender.send(event.clone().into());
                        let _ = self.repository.mark_published(event.seq).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn event(payload: Value) -> OutboxEvent {
        OutboxEvent {
            seq: 1,
            id: Uuid::nil(),
            event_type: "smart_trade.buy_confirmed".into(),
            schema_version: 1,
            aggregate_type: None,
            aggregate_id: None,
            dedupe_key: "test".into(),
            payload,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn confirmation_and_chain_finality_are_independent() {
        let pending: EventEnvelope =
            event(json!({"confirmation_level":"CONFIRMED","chain_finality":"PENDING"})).into();
        assert_eq!(pending.trade_evidence.as_deref(), Some("CONFIRMED"));
        assert!(pending.provisional);
        let confirmed: EventEnvelope =
            event(json!({"confirmation_level":"CONFIRMED","chain_finality":"CONFIRMED"})).into();
        assert!(!confirmed.provisional);
        let orphaned: EventEnvelope =
            event(json!({"confirmation_level":"CONFIRMED","chain_finality":"ORPHANED"})).into();
        assert_eq!(orphaned.chain_finality.as_deref(), Some("ORPHANED"));
        assert!(!orphaned.provisional);
    }

    #[test]
    fn historical_origins_retain_non_realtime_semantics() {
        for source in ["CHAIN_BACKFILL", "IDENTITY_BACKFILL"] {
            let envelope: EventEnvelope =
                event(json!({"classification_source":source,"realtime_alert_eligible":false}))
                    .into();
            assert_eq!(envelope.classification_source.as_deref(), Some(source));
            assert!(!envelope.realtime_alert_eligible);
        }
    }

    #[tokio::test]
    async fn bounded_client_queue_detects_a_slow_receiver() {
        let (sender, mut receiver) = broadcast::channel(2);
        sender.send(1).unwrap();
        sender.send(2).unwrap();
        sender.send(3).unwrap();
        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
        assert_eq!(receiver.recv().await.unwrap(), 2);
        assert_eq!(receiver.recv().await.unwrap(), 3);
    }
}
