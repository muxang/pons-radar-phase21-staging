use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(Clone, Debug, FromRow)]
pub struct AdminUser {
    pub id: Uuid,
    pub username: String,
    pub password_hash: String,
    pub enabled: bool,
    pub role: String,
}
#[derive(Clone, Debug, FromRow)]
pub struct AuthenticatedSession {
    pub session_id: Uuid,
    pub user_id: Uuid,
    pub username: String,
    pub role: String,
    pub csrf_hash: Vec<u8>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct AuthRepository {
    pool: PgPool,
}
#[allow(clippy::missing_errors_doc)]
impl AuthRepository {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
    pub async fn has_users(&self) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
            .fetch_one(&self.pool)
            .await?
            > 0)
    }
    pub async fn create_first_admin(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<Option<AdminUser>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("LOCK TABLE users IN EXCLUSIVE MODE")
            .execute(&mut *tx)
            .await?;
        if sqlx::query_scalar::<_, i64>("SELECT count(*) FROM users")
            .fetch_one(&mut *tx)
            .await?
            > 0
        {
            return Ok(None);
        }
        let user=sqlx::query_as("INSERT INTO users(username,password_hash,role) VALUES($1,$2,'ADMIN') RETURNING id,username,password_hash,enabled,role").bind(username).bind(password_hash).fetch_one(&mut *tx).await?;
        tx.commit().await?;
        Ok(Some(user))
    }
    pub async fn by_username(&self, username: &str) -> Result<Option<AdminUser>, sqlx::Error> {
        sqlx::query_as("SELECT id,username,password_hash,enabled,role FROM users WHERE username=$1")
            .bind(username)
            .fetch_optional(&self.pool)
            .await
    }
    pub async fn create_session(
        &self,
        user_id: Uuid,
        token_hash: &[u8],
        csrf_hash: &[u8],
        hours: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO admin_sessions(user_id,token_hash,csrf_hash,expires_at) VALUES($1,$2,$3,now()+$4 * interval '1 hour')").bind(user_id).bind(token_hash).bind(csrf_hash).bind(hours).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn session(
        &self,
        token_hash: &[u8],
    ) -> Result<Option<AuthenticatedSession>, sqlx::Error> {
        sqlx::query_as(r"SELECT s.id session_id,u.id user_id,u.username,u.role,s.csrf_hash,s.expires_at FROM admin_sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1 AND s.revoked_at IS NULL AND s.expires_at>now() AND u.enabled").bind(token_hash).fetch_optional(&self.pool).await
    }
    pub async fn revoke(&self, token_hash: &[u8]) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE admin_sessions SET revoked_at=now() WHERE token_hash=$1 AND revoked_at IS NULL",
        )
        .bind(token_hash)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    pub async fn audit(
        &self,
        user_id: Option<Uuid>,
        action: &str,
        target_type: &str,
        target_id: Option<&str>,
        details: &Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO audit_logs(user_id,action,target_type,target_id,details) VALUES($1,$2,$3,$4,$5)").bind(user_id).bind(action).bind(target_type).bind(target_id).bind(details).execute(&self.pool).await?;
        Ok(())
    }
    pub async fn purge_expired(&self) -> Result<u64, sqlx::Error> {
        Ok(sqlx::query("DELETE FROM admin_sessions WHERE expires_at < now() OR revoked_at < now() - interval '24 hours'").execute(&self.pool).await?.rows_affected())
    }
}
