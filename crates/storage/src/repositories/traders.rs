use chrono::{DateTime, Utc};
use pons_domain::{ChainId, WalletAddress};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug, FromRow)]
pub struct Trader {
    pub id: Uuid,
    pub handle: String,
    pub display_name: Option<String>,
    pub manual_tier: Option<String>,
    pub status: String,
    pub notes: Option<String>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
#[derive(Clone, Debug)]
pub struct TraderWallet {
    pub id: Uuid,
    pub trader_id: Uuid,
    pub trader_handle: String,
    pub chain_id: ChainId,
    pub address: WalletAddress,
    pub role: String,
    pub source: String,
    pub confidence: String,
    pub verified: bool,
    pub enabled: bool,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub notes: Option<String>,
    pub evidence: Value,
}
#[derive(FromRow)]
struct WalletRow {
    id: Uuid,
    trader_id: Uuid,
    trader_handle: String,
    chain_id: String,
    address: Vec<u8>,
    role: String,
    source: String,
    confidence: String,
    verified: bool,
    enabled: bool,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    notes: Option<String>,
    evidence: Value,
}

#[derive(Clone, Debug)]
pub struct NewTrader<'a> {
    pub handle: &'a str,
    pub display_name: Option<&'a str>,
    pub manual_tier: Option<&'a str>,
    pub notes: Option<&'a str>,
}
#[derive(Clone, Debug)]
pub struct TraderChanges<'a> {
    pub display_name: Option<Option<&'a str>>,
    pub manual_tier: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub notes: Option<Option<&'a str>>,
}
#[derive(Clone, Debug)]
pub struct NewTraderWallet<'a> {
    pub trader_id: Uuid,
    pub chain_id: ChainId,
    pub address: WalletAddress,
    pub role: &'a str,
    pub source: &'a str,
    pub confidence: &'a str,
    pub verified: bool,
    pub enabled: bool,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub notes: Option<&'a str>,
    pub evidence: &'a Value,
}
#[derive(Clone, Debug)]
pub struct WalletChanges<'a> {
    pub enabled: Option<bool>,
    pub verified: Option<bool>,
    pub confidence: Option<&'a str>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<Option<DateTime<Utc>>>,
    pub notes: Option<Option<&'a str>>,
    pub evidence: Option<&'a Value>,
}

#[derive(Clone, Debug)]
pub struct TraderRepository {
    pool: PgPool,
}
#[allow(clippy::missing_errors_doc)]
impl TraderRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    #[must_use]
    pub fn pool(&self) -> PgPool {
        self.pool.clone()
    }
    pub async fn create(&self, v: &NewTrader<'_>) -> Result<Trader, sqlx::Error> {
        sqlx::query_as("INSERT INTO traders(handle,display_name,manual_tier,notes) VALUES(lower($1),$2,$3,$4) RETURNING id,handle,display_name,manual_tier,status,notes,enabled,created_at,updated_at").bind(v.handle).bind(v.display_name).bind(v.manual_tier).bind(v.notes).fetch_one(&self.pool).await
    }
    pub async fn update(
        &self,
        id: Uuid,
        v: &TraderChanges<'_>,
    ) -> Result<Option<Trader>, sqlx::Error> {
        sqlx::query_as("UPDATE traders SET display_name=CASE WHEN $2 THEN $3 ELSE display_name END,manual_tier=CASE WHEN $4 THEN $5 ELSE manual_tier END,status=COALESCE($6,status),enabled=COALESCE($6,status)='ACTIVE',notes=CASE WHEN $7 THEN $8 ELSE notes END,updated_at=now() WHERE id=$1 RETURNING id,handle,display_name,manual_tier,status,notes,enabled,created_at,updated_at").bind(id).bind(v.display_name.is_some()).bind(v.display_name.flatten()).bind(v.manual_tier.is_some()).bind(v.manual_tier.flatten()).bind(v.status).bind(v.notes.is_some()).bind(v.notes.flatten()).fetch_optional(&self.pool).await
    }
    pub async fn get(&self, id: Uuid) -> Result<Option<Trader>, sqlx::Error> {
        sqlx::query_as("SELECT id,handle,display_name,manual_tier,status,notes,enabled,created_at,updated_at FROM traders WHERE id=$1").bind(id).fetch_optional(&self.pool).await
    }
    pub async fn by_handle(&self, h: &str) -> Result<Option<Trader>, sqlx::Error> {
        sqlx::query_as("SELECT id,handle,display_name,manual_tier,status,notes,enabled,created_at,updated_at FROM traders WHERE handle=lower($1)").bind(h).fetch_optional(&self.pool).await
    }
    pub async fn list(&self) -> Result<Vec<Trader>, sqlx::Error> {
        sqlx::query_as("SELECT id,handle,display_name,manual_tier,status,notes,enabled,created_at,updated_at FROM traders ORDER BY handle").fetch_all(&self.pool).await
    }
    pub async fn add_wallet(&self, v: &NewTraderWallet<'_>) -> Result<TraderWallet, sqlx::Error> {
        let row:WalletRow=sqlx::query_as("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified,enabled,valid_from,valid_to,notes,evidence) VALUES($1,$2::numeric,$3,$4,$5,$6::numeric,$7,$8,$9,$10,$11,$12) RETURNING id,trader_id,(SELECT handle FROM traders WHERE id=trader_id) trader_handle,chain_id::text chain_id,address,role,source,identity_confidence::text confidence,verified,enabled,valid_from,valid_to,notes,evidence").bind(v.trader_id).bind(v.chain_id.get().to_string()).bind(v.address.as_bytes().as_slice()).bind(v.role).bind(v.source).bind(v.confidence).bind(v.verified).bind(v.enabled).bind(v.valid_from).bind(v.valid_to).bind(v.notes).bind(v.evidence).fetch_one(&self.pool).await?;
        decode_wallet(row)
    }
    pub async fn import_wallet(
        &self,
        handle: &str,
        tier: Option<&str>,
        v: &NewTraderWallet<'_>,
    ) -> Result<TraderWallet, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let trader_id: Uuid =
            match sqlx::query_scalar("SELECT id FROM traders WHERE handle=lower($1)")
                .bind(handle)
                .fetch_optional(&mut *tx)
                .await?
            {
                Some(id) => id,
                None => {
                    sqlx::query_scalar(
                        "INSERT INTO traders(handle,manual_tier) VALUES(lower($1),$2) RETURNING id",
                    )
                    .bind(handle)
                    .bind(tier)
                    .fetch_one(&mut *tx)
                    .await?
                }
            };
        let row:WalletRow=sqlx::query_as("INSERT INTO trader_wallets(trader_id,chain_id,address,role,source,identity_confidence,verified,enabled,valid_from,valid_to,notes,evidence) VALUES($1,$2::numeric,$3,$4,$5,$6::numeric,$7,$8,$9,$10,$11,$12) RETURNING id,trader_id,(SELECT handle FROM traders WHERE id=trader_id) trader_handle,chain_id::text chain_id,address,role,source,identity_confidence::text confidence,verified,enabled,valid_from,valid_to,notes,evidence").bind(trader_id).bind(v.chain_id.get().to_string()).bind(v.address.as_bytes().as_slice()).bind(v.role).bind(v.source).bind(v.confidence).bind(v.verified).bind(v.enabled).bind(v.valid_from).bind(v.valid_to).bind(v.notes).bind(v.evidence).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        decode_wallet(row)
    }
    pub async fn update_wallet(
        &self,
        id: Uuid,
        v: &WalletChanges<'_>,
    ) -> Result<Option<TraderWallet>, sqlx::Error> {
        let row:Option<WalletRow>=sqlx::query_as("UPDATE trader_wallets SET enabled=COALESCE($2,enabled),verified=COALESCE($3,verified),identity_confidence=COALESCE($4::numeric,identity_confidence),valid_from=COALESCE($5,valid_from),valid_to=CASE WHEN $6 THEN $7 ELSE valid_to END,notes=CASE WHEN $8 THEN $9 ELSE notes END,evidence=COALESCE($10,evidence),updated_at=now() WHERE id=$1 RETURNING id,trader_id,(SELECT handle FROM traders WHERE id=trader_id) trader_handle,chain_id::text chain_id,address,role,source,identity_confidence::text confidence,verified,enabled,valid_from,valid_to,notes,evidence").bind(id).bind(v.enabled).bind(v.verified).bind(v.confidence).bind(v.valid_from).bind(v.valid_to.is_some()).bind(v.valid_to.flatten()).bind(v.notes.is_some()).bind(v.notes.flatten()).bind(v.evidence).fetch_optional(&self.pool).await?;
        row.map(decode_wallet).transpose()
    }
    pub async fn wallets(&self, trader: Option<Uuid>) -> Result<Vec<TraderWallet>, sqlx::Error> {
        let rows:Vec<WalletRow>=sqlx::query_as("SELECT w.id,w.trader_id,t.handle trader_handle,w.chain_id::text chain_id,w.address,w.role,w.source,w.identity_confidence::text confidence,w.verified,w.enabled,w.valid_from,w.valid_to,w.notes,w.evidence FROM trader_wallets w JOIN traders t ON t.id=w.trader_id WHERE $1::uuid IS NULL OR w.trader_id=$1 ORDER BY t.handle,w.created_at").bind(trader).fetch_all(&self.pool).await?;
        rows.into_iter().map(decode_wallet).collect()
    }
    pub async fn active(
        &self,
        confidence: &str,
        at: DateTime<Utc>,
    ) -> Result<Vec<TraderWallet>, sqlx::Error> {
        let rows:Vec<WalletRow>=sqlx::query_as("SELECT w.id,w.trader_id,t.handle trader_handle,w.chain_id::text chain_id,w.address,w.role,w.source,w.identity_confidence::text confidence,w.verified,w.enabled,w.valid_from,w.valid_to,w.notes,w.evidence FROM trader_wallets w JOIN traders t ON t.id=w.trader_id WHERE w.chain_id=4663 AND w.role='ROBINHOOD_EXECUTION_ADDRESS' AND w.enabled AND w.verified AND t.enabled AND t.status='ACTIVE' AND w.identity_confidence >= $1::numeric AND w.valid_from <= $2 AND (w.valid_to IS NULL OR w.valid_to > $2)").bind(confidence).bind(at).fetch_all(&self.pool).await?;
        rows.into_iter().map(decode_wallet).collect()
    }
    pub async fn eligible_history(
        &self,
        confidence: &str,
    ) -> Result<Vec<TraderWallet>, sqlx::Error> {
        let rows:Vec<WalletRow>=sqlx::query_as("SELECT w.id,w.trader_id,t.handle trader_handle,w.chain_id::text chain_id,w.address,w.role,w.source,w.identity_confidence::text confidence,w.verified,w.enabled,w.valid_from,w.valid_to,w.notes,w.evidence FROM trader_wallets w JOIN traders t ON t.id=w.trader_id WHERE w.chain_id=4663 AND w.role='ROBINHOOD_EXECUTION_ADDRESS' AND w.enabled AND w.verified AND t.enabled AND t.status='ACTIVE' AND w.identity_confidence >= $1::numeric ORDER BY w.address,w.valid_from").bind(confidence).fetch_all(&self.pool).await?;
        rows.into_iter().map(decode_wallet).collect()
    }
}
fn decode_wallet(r: WalletRow) -> Result<TraderWallet, sqlx::Error> {
    Ok(TraderWallet {
        id: r.id,
        trader_id: r.trader_id,
        trader_handle: r.trader_handle,
        chain_id: ChainId::new(r.chain_id.parse().map_err(decode)?),
        address: WalletAddress::from_slice(&r.address).map_err(decode)?,
        role: r.role,
        source: r.source,
        confidence: r.confidence,
        verified: r.verified,
        enabled: r.enabled,
        valid_from: r.valid_from,
        valid_to: r.valid_to,
        notes: r.notes,
        evidence: r.evidence,
    })
}
fn decode(e: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(e))
}
