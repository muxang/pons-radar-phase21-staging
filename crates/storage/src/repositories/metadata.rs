use chrono::{DateTime, Utc};
use pons_domain::{BlockNumber, TokenAddress, WalletAddress};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataJob {
    pub token_id: Uuid,
    pub token: TokenAddress,
    pub launch_deployer: WalletAddress,
    pub attempts: i32,
    pub requested_block: Option<BlockNumber>,
    pub historical_attempts: i32,
    pub original_exists: bool,
}

pub struct MetadataObservation<'a> {
    pub token_id: Uuid,
    pub content_hash: &'a [u8; 32],
    pub name: &'a str,
    pub symbol: &'a str,
    pub decimals: u8,
    pub total_supply_raw: &'a str,
    pub token_deployer: WalletAddress,
    pub token_logo: &'a str,
    pub token_description: &'a str,
    pub twitter: &'a str,
    pub telegram: &'a str,
    pub discord: &'a str,
    pub website: &'a str,
    pub farcaster: &'a str,
    pub normalized_socials: &'a Value,
    pub raw_metadata: &'a Value,
    pub deployer_matches_launch: bool,
    pub integrity_warning: Option<&'a str>,
    pub observed_block: BlockNumber,
    pub observed_at: DateTime<Utc>,
    pub capture_mode: &'a str,
    pub exact_launch_snapshot: bool,
    pub requested_block: Option<BlockNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetadataPersistResult {
    pub original_created: bool,
    pub snapshot_created: bool,
}

#[derive(Clone, Debug)]
pub struct TokenMetadataRepository {
    pool: PgPool,
}

type ClaimedRow = (Uuid, Vec<u8>, Vec<u8>, i32, Option<String>, i32, bool);

#[allow(clippy::missing_errors_doc)]
impl TokenMetadataRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn claim_due(&self) -> Result<Option<MetadataJob>, sqlx::Error> {
        let row: Option<ClaimedRow> = sqlx::query_as(
            r"WITH candidate AS (
                SELECT j.token_id FROM token_metadata_jobs j
                WHERE (j.status IN ('PENDING','RETRY','SUCCEEDED') AND j.next_attempt_at<=now())
                   OR (j.status='IN_PROGRESS' AND j.locked_at<now()-interval '5 minutes')
                ORDER BY j.next_attempt_at,j.token_id FOR UPDATE SKIP LOCKED LIMIT 1)
              UPDATE token_metadata_jobs j
              SET status='IN_PROGRESS',attempts=j.attempts+1,locked_at=now(),updated_at=now()
              FROM candidate c,tokens t WHERE j.token_id=c.token_id AND t.id=j.token_id
              RETURNING j.token_id,t.address,t.deployer,j.attempts,j.requested_block::text,
                j.historical_attempts,EXISTS(SELECT 1 FROM token_metadata_original o WHERE o.token_id=j.token_id)",
        ).fetch_optional(&self.pool).await?;
        row.map(
            |(
                token_id,
                token,
                deployer,
                attempts,
                requested,
                historical_attempts,
                original_exists,
            )| {
                Ok(MetadataJob {
                    token_id,
                    token: TokenAddress::from_slice(&token).map_err(decode)?,
                    launch_deployer: WalletAddress::from_slice(&deployer).map_err(decode)?,
                    attempts,
                    requested_block: requested
                        .map(|v| v.parse::<u64>().map(BlockNumber::new))
                        .transpose()
                        .map_err(decode)?,
                    historical_attempts,
                    original_exists,
                })
            },
        )
        .transpose()
    }

    pub async fn persist(
        &self,
        value: &MetadataObservation<'_>,
        next_refresh_at: DateTime<Utc>,
    ) -> Result<MetadataPersistResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let original_created = insert_original(&mut tx, value).await?;
        let snapshot_created = sqlx::query_scalar::<_, bool>(
            r"INSERT INTO token_metadata_snapshots(token_id,content_hash,metadata,deployer_matches_launch,integrity_warning,observed_block,observed_at,capture_mode,exact_launch_snapshot,requested_block)
              VALUES($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$10::numeric)
              ON CONFLICT(token_id,content_hash) DO NOTHING RETURNING true",
        ).bind(value.token_id).bind(value.content_hash.as_slice()).bind(value.raw_metadata)
         .bind(value.deployer_matches_launch).bind(value.integrity_warning)
         .bind(value.observed_block.get().to_string()).bind(value.observed_at)
         .bind(value.capture_mode).bind(value.exact_launch_snapshot)
         .bind(value.requested_block.map(|v| v.get().to_string()))
         .fetch_optional(&mut *tx).await?.unwrap_or(false);
        upsert_current(&mut tx, value).await?;
        sqlx::query("UPDATE token_metadata_jobs SET status='SUCCEEDED',last_error=NULL,next_attempt_at=$2,locked_at=NULL,updated_at=now() WHERE token_id=$1")
            .bind(value.token_id).bind(next_refresh_at).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(MetadataPersistResult {
            original_created,
            snapshot_created,
        })
    }

    pub async fn retry(
        &self,
        token_id: Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
        historical_failure: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE token_metadata_jobs SET status='RETRY',last_error=$2,next_attempt_at=$3,locked_at=NULL,historical_attempts=historical_attempts+CASE WHEN $4 THEN 1 ELSE 0 END,updated_at=now() WHERE token_id=$1")
            .bind(token_id).bind(error).bind(next_attempt_at).bind(historical_failure)
            .execute(&self.pool).await?;
        Ok(())
    }
}

async fn insert_original(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    v: &MetadataObservation<'_>,
) -> Result<bool, sqlx::Error> {
    Ok(sqlx::query_scalar::<_, bool>(
        r"INSERT INTO token_metadata_original(token_id,content_hash,name,symbol,decimals,total_supply_raw,token_deployer,token_logo,token_description,twitter,telegram,discord,website,farcaster,normalized_socials,raw_metadata,deployer_matches_launch,integrity_warning,observed_block,observed_at,capture_mode,exact_launch_snapshot,requested_block)
          SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::numeric,$20,$21,$22,$23::numeric
          WHERE NOT EXISTS(SELECT 1 FROM token_metadata_original WHERE token_id=$1)
          ON CONFLICT(token_id) DO NOTHING RETURNING true",
    ).bind(v.token_id).bind(v.content_hash.as_slice()).bind(v.name).bind(v.symbol)
     .bind(i16::from(v.decimals)).bind(v.total_supply_raw).bind(v.token_deployer.as_bytes().as_slice())
     .bind(v.token_logo).bind(v.token_description).bind(v.twitter).bind(v.telegram).bind(v.discord)
     .bind(v.website).bind(v.farcaster).bind(v.normalized_socials).bind(v.raw_metadata)
     .bind(v.deployer_matches_launch).bind(v.integrity_warning).bind(v.observed_block.get().to_string())
     .bind(v.observed_at).bind(v.capture_mode).bind(v.exact_launch_snapshot)
     .bind(v.requested_block.map(|value| value.get().to_string()))
     .fetch_optional(&mut **tx).await?.unwrap_or(false))
}

async fn upsert_current(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    v: &MetadataObservation<'_>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r"INSERT INTO token_metadata_current(token_id,content_hash,name,symbol,decimals,total_supply_raw,token_deployer,token_logo,token_description,twitter,telegram,discord,website,farcaster,normalized_socials,raw_metadata,deployer_matches_launch,integrity_warning,observed_block,observed_at,capture_mode,exact_launch_snapshot,requested_block)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::numeric,$20,$21,$22,$23::numeric)
          ON CONFLICT(token_id) DO UPDATE SET content_hash=EXCLUDED.content_hash,name=EXCLUDED.name,symbol=EXCLUDED.symbol,decimals=EXCLUDED.decimals,total_supply_raw=EXCLUDED.total_supply_raw,token_deployer=EXCLUDED.token_deployer,token_logo=EXCLUDED.token_logo,token_description=EXCLUDED.token_description,twitter=EXCLUDED.twitter,telegram=EXCLUDED.telegram,discord=EXCLUDED.discord,website=EXCLUDED.website,farcaster=EXCLUDED.farcaster,normalized_socials=EXCLUDED.normalized_socials,raw_metadata=EXCLUDED.raw_metadata,deployer_matches_launch=EXCLUDED.deployer_matches_launch,integrity_warning=EXCLUDED.integrity_warning,observed_block=EXCLUDED.observed_block,observed_at=EXCLUDED.observed_at,capture_mode=EXCLUDED.capture_mode,exact_launch_snapshot=EXCLUDED.exact_launch_snapshot,requested_block=EXCLUDED.requested_block,updated_at=now()",
    ).bind(v.token_id).bind(v.content_hash.as_slice()).bind(v.name).bind(v.symbol)
     .bind(i16::from(v.decimals)).bind(v.total_supply_raw).bind(v.token_deployer.as_bytes().as_slice())
     .bind(v.token_logo).bind(v.token_description).bind(v.twitter).bind(v.telegram).bind(v.discord)
     .bind(v.website).bind(v.farcaster).bind(v.normalized_socials).bind(v.raw_metadata)
     .bind(v.deployer_matches_launch).bind(v.integrity_warning).bind(v.observed_block.get().to_string())
     .bind(v.observed_at).bind(v.capture_mode).bind(v.exact_launch_snapshot)
     .bind(v.requested_block.map(|value| value.get().to_string()))
     .execute(&mut **tx).await?;
    Ok(())
}

fn decode(error: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}
