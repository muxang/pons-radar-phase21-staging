use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction};
use uuid::Uuid;

pub struct NewOutboxEvent<'a> {
    pub event_type: &'a str,
    pub schema_version: i32,
    pub aggregate_type: Option<&'a str>,
    pub aggregate_id: Option<Uuid>,
    pub dedupe_key: &'a str,
    pub payload: &'a Value,
}

#[derive(Clone, Debug, FromRow, PartialEq)]
pub struct OutboxEvent {
    pub seq: i64,
    pub id: Uuid,
    pub event_type: String,
    pub schema_version: i32,
    pub aggregate_type: Option<String>,
    pub aggregate_id: Option<Uuid>,
    pub dedupe_key: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct EventOutboxRepository {
    pool: PgPool,
}

impl EventOutboxRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Appends an event, or returns the existing row for the same semantic key.
    ///
    /// Sequence values are allocated by `PostgreSQL` and are suitable as replay cursors.
    ///
    /// # Errors
    ///
    /// Returns a database error when the event cannot be persisted or read back.
    pub async fn append(&self, event: &NewOutboxEvent<'_>) -> Result<OutboxEvent, sqlx::Error> {
        let mut transaction = self.pool.begin().await?;
        let persisted = Self::append_in_transaction(&mut transaction, event).await?;
        transaction.commit().await?;
        Ok(persisted)
    }

    /// Appends an event inside a caller-owned transaction.
    ///
    /// This is the integration point for future domain writes that must atomically
    /// persist source state and their outbox event before any delivery occurs.
    ///
    /// # Errors
    ///
    /// Returns a database error when the event cannot be persisted or read back.
    pub async fn append_in_transaction(
        transaction: &mut Transaction<'_, Postgres>,
        event: &NewOutboxEvent<'_>,
    ) -> Result<OutboxEvent, sqlx::Error> {
        // A sequence does not itself guarantee commit ordering. This lock makes it
        // impossible for a lower sequence to commit after a delivered higher cursor.
        sqlx::query("LOCK TABLE event_outbox IN EXCLUSIVE MODE")
            .execute(&mut **transaction)
            .await?;
        sqlx::query_as(
            r"
            INSERT INTO event_outbox
                (event_type, schema_version, aggregate_type, aggregate_id, dedupe_key, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key
            RETURNING seq, id, event_type, schema_version, aggregate_type,
                      aggregate_id, dedupe_key, payload, created_at
            ",
        )
        .bind(event.event_type)
        .bind(event.schema_version)
        .bind(event.aggregate_type)
        .bind(event.aggregate_id)
        .bind(event.dedupe_key)
        .bind(event.payload)
        .fetch_one(&mut **transaction)
        .await
    }

    /// Returns durable events strictly after a replay cursor in ascending order.
    ///
    /// # Errors
    ///
    /// Returns a database error if replay rows cannot be loaded.
    pub async fn after(&self, seq: i64, limit: i64) -> Result<Vec<OutboxEvent>, sqlx::Error> {
        self.range(seq, i64::MAX, limit).await
    }

    /// Returns durable events after `seq`, capped at an inclusive high watermark.
    ///
    /// # Errors
    ///
    /// Returns a database error if the replay page cannot be loaded.
    pub async fn range(
        &self,
        seq: i64,
        through_seq: i64,
        limit: i64,
    ) -> Result<Vec<OutboxEvent>, sqlx::Error> {
        sqlx::query_as(
            r"
            SELECT seq, id, event_type, schema_version, aggregate_type,
                   aggregate_id, dedupe_key, payload, created_at
            FROM event_outbox
            WHERE seq > $1 AND seq <= $2
            ORDER BY seq ASC
            LIMIT $3
            ",
        )
        .bind(seq)
        .bind(through_seq)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
    }

    /// Returns the largest committed global sequence, or zero for an empty outbox.
    ///
    /// # Errors
    ///
    /// Returns a database error if the watermark cannot be read.
    pub async fn high_watermark(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COALESCE(max(seq), 0) FROM event_outbox")
            .fetch_one(&self.pool)
            .await
    }

    /// Returns the sequence immediately before the first event not observed by the publisher.
    ///
    /// # Errors
    ///
    /// Returns a database error if publisher progress cannot be read.
    pub async fn publisher_cursor(&self) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar("SELECT COALESCE((SELECT min(seq)-1 FROM event_outbox WHERE published_at IS NULL),(SELECT COALESCE(max(seq),0) FROM event_outbox))")
            .fetch_one(&self.pool)
            .await
    }

    /// Records operational broadcast progress. It is not a client acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns a database error if operational metadata cannot be updated.
    pub async fn mark_published(&self, seq: i64) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE event_outbox SET published_at=COALESCE(published_at,now()) WHERE seq=$1",
        )
        .bind(seq)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
