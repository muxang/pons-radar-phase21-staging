use chrono::{DateTime, Utc};
use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct ProtocolDeployment {
    pub id: Uuid,
    pub protocol: String,
    pub generation: String,
    pub chain_id: ChainId,
    pub address: ContractAddress,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub enabled: bool,
    pub expected_event_topics: Value,
    pub expected_code_hash: Option<BlockHash>,
    pub source: String,
    pub interface_fingerprint: String,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub health: String,
    pub verification_evidence: Value,
    pub verification_error: Option<String>,
    pub trust_basis: String,
    pub approved_by: Option<Uuid>,
    pub approved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub struct NewProtocolDeployment<'a> {
    pub chain_id: ChainId,
    pub address: ContractAddress,
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
    pub enabled: bool,
    pub expected_event_topics: &'a Value,
    pub expected_code_hash: Option<BlockHash>,
    pub source: &'a str,
    pub interface_fingerprint: &'a str,
}

pub struct DeploymentChanges<'a> {
    pub start_block: Option<BlockNumber>,
    pub end_block: Option<Option<BlockNumber>>,
    pub enabled: Option<bool>,
    pub expected_event_topics: Option<&'a Value>,
    pub expected_code_hash: Option<Option<BlockHash>>,
    pub source: Option<&'a str>,
    pub interface_fingerprint: Option<&'a str>,
}

#[derive(FromRow)]
struct Row {
    id: Uuid,
    protocol: String,
    generation: String,
    chain_id: String,
    address: Vec<u8>,
    start_block: String,
    end_block: Option<String>,
    enabled: bool,
    expected_event_topics: Value,
    expected_code_hash: Option<Vec<u8>>,
    source: String,
    interface_fingerprint: String,
    last_verified_at: Option<DateTime<Utc>>,
    health: String,
    verification_evidence: Value,
    verification_error: Option<String>,
    trust_basis: String,
    approved_by: Option<Uuid>,
    approved_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl TryFrom<Row> for ProtocolDeployment {
    type Error = sqlx::Error;
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            protocol: row.protocol,
            generation: row.generation,
            chain_id: ChainId::new(row.chain_id.parse().map_err(decode)?),
            address: ContractAddress::from_slice(&row.address).map_err(decode)?,
            start_block: BlockNumber::new(row.start_block.parse().map_err(decode)?),
            end_block: row
                .end_block
                .map(|v| v.parse().map(BlockNumber::new).map_err(decode))
                .transpose()?,
            enabled: row.enabled,
            expected_event_topics: row.expected_event_topics,
            expected_code_hash: row
                .expected_code_hash
                .map(|v| BlockHash::from_slice(&v).map_err(decode))
                .transpose()?,
            source: row.source,
            interface_fingerprint: row.interface_fingerprint,
            last_verified_at: row.last_verified_at,
            health: row.health,
            verification_evidence: row.verification_evidence,
            verification_error: row.verification_error,
            trust_basis: row.trust_basis,
            approved_by: row.approved_by,
            approved_at: row.approved_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DeploymentRepository {
    pool: PgPool,
}

#[allow(clippy::missing_errors_doc)]
impl DeploymentRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> Result<Vec<ProtocolDeployment>, sqlx::Error> {
        rows(sqlx::query_as(&select_sql()).fetch_all(&self.pool).await?)
    }
    pub async fn get(&self, id: Uuid) -> Result<Option<ProtocolDeployment>, sqlx::Error> {
        sqlx::query_as::<_, Row>(&format!("{} WHERE id=$1", select_sql()))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }
    pub async fn create(
        &self,
        value: &NewProtocolDeployment<'_>,
    ) -> Result<ProtocolDeployment, sqlx::Error> {
        let id: Uuid = sqlx::query_scalar(
            r"INSERT INTO protocol_deployments
          (protocol,generation,chain_id,address,start_block,end_block,enabled,expected_event_topics,
           expected_code_hash,source,interface_fingerprint)
          VALUES ('PONS','V2',$1::numeric,$2,$3::numeric,$4::numeric,$5,$6,$7,$8,$9) RETURNING id",
        )
        .bind(value.chain_id.get().to_string())
        .bind(value.address.as_bytes().as_slice())
        .bind(value.start_block.get().to_string())
        .bind(value.end_block.map(|v| v.get().to_string()))
        .bind(value.enabled)
        .bind(value.expected_event_topics)
        .bind(value.expected_code_hash.map(|v| v.as_bytes().to_vec()))
        .bind(value.source)
        .bind(value.interface_fingerprint)
        .fetch_one(&self.pool)
        .await?;
        self.get(id).await?.ok_or(sqlx::Error::RowNotFound)
    }
    pub async fn update(
        &self,
        id: Uuid,
        value: &DeploymentChanges<'_>,
    ) -> Result<Option<ProtocolDeployment>, sqlx::Error> {
        sqlx::query(r"UPDATE protocol_deployments SET
          start_block=COALESCE($2::numeric,start_block), end_block=CASE WHEN $3 THEN $4::numeric ELSE end_block END,
          enabled=COALESCE($5,enabled), expected_event_topics=COALESCE($6,expected_event_topics),
          expected_code_hash=CASE WHEN $7 THEN $8 ELSE expected_code_hash END, source=COALESCE($9,source),
          interface_fingerprint=COALESCE($10,interface_fingerprint),
          health=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN 'UNVERIFIED' ELSE health END,
          last_verified_at=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN NULL ELSE last_verified_at END,
          verification_evidence=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN '{}' ELSE verification_evidence END,
          verification_error=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN NULL ELSE verification_error END,
          trust_basis=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN 'UNTRUSTED' ELSE trust_basis END,
          approved_by=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN NULL ELSE approved_by END,
          approved_at=CASE WHEN $2 IS NOT NULL OR $3 OR $6 IS NOT NULL OR $7 OR $9 IS NOT NULL OR $10 IS NOT NULL THEN NULL ELSE approved_at END,
          updated_at=now() WHERE id=$1")
          .bind(id).bind(value.start_block.map(|v|v.get().to_string())).bind(value.end_block.is_some())
          .bind(value.end_block.flatten().map(|v|v.get().to_string())).bind(value.enabled)
          .bind(value.expected_event_topics).bind(value.expected_code_hash.is_some())
          .bind(value.expected_code_hash.flatten().map(|v|v.as_bytes().to_vec())).bind(value.source)
          .bind(value.interface_fingerprint).execute(&self.pool).await?;
        self.get(id).await
    }
    pub async fn save_verification(
        &self,
        id: Uuid,
        health: &str,
        evidence: &Value,
        error: Option<&str>,
    ) -> Result<ProtocolDeployment, sqlx::Error> {
        sqlx::query("UPDATE protocol_deployments SET health=$2,last_verified_at=now(),verification_evidence=$3,verification_error=$4,trust_basis=CASE WHEN $2='VERIFIED' AND ($3->>'code_hash_matches')::boolean IS TRUE THEN 'PINNED_CODE_HASH' WHEN $2='VERIFIED' THEN 'OPERATOR_APPROVED' ELSE 'UNTRUSTED' END,approved_at=CASE WHEN $2='VERIFIED' THEN now() ELSE NULL END,approved_by=CASE WHEN $2='VERIFIED' THEN approved_by ELSE NULL END,updated_at=now() WHERE id=$1")
          .bind(id).bind(health).bind(evidence).bind(error).execute(&self.pool).await?;
        self.get(id).await?.ok_or(sqlx::Error::RowNotFound)
    }
    pub async fn set_approver(&self, id: Uuid, user_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE protocol_deployments SET approved_by=$2,approved_at=now() WHERE id=$1 AND health='VERIFIED'").bind(id).bind(user_id).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn active_verified(
        &self,
        chain: ChainId,
        at: BlockNumber,
    ) -> Result<Vec<ProtocolDeployment>, sqlx::Error> {
        rows(sqlx::query_as::<_,Row>(&format!("{} WHERE chain_id=$1::numeric AND enabled AND health='VERIFIED' AND (trust_basis='PINNED_CODE_HASH' OR (trust_basis='OPERATOR_APPROVED' AND approved_by IS NOT NULL)) AND start_block <= $2::numeric AND (end_block IS NULL OR end_block >= $2::numeric)",select_sql()))
          .bind(chain.get().to_string()).bind(at.get().to_string()).fetch_all(&self.pool).await?)
    }
}

fn select_sql() -> String {
    "SELECT id,protocol,generation,chain_id::text chain_id,address,start_block::text start_block,end_block::text end_block,enabled,expected_event_topics,expected_code_hash,source,interface_fingerprint,last_verified_at,health,verification_evidence,verification_error,trust_basis,approved_by,approved_at,created_at,updated_at FROM protocol_deployments".into()
}
fn rows(values: Vec<Row>) -> Result<Vec<ProtocolDeployment>, sqlx::Error> {
    values.into_iter().map(TryInto::try_into).collect()
}
fn decode(error: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(error))
}
