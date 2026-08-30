use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

#[derive(Clone, Copy, Debug)]
pub struct DatabaseConfig {
    pub max_connections: u32,
    pub acquire_timeout: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    #[must_use]
    pub const fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connects to `PostgreSQL` and applies all forward-only migrations.
    ///
    /// # Errors
    ///
    /// Returns an error if the pool cannot connect or a migration fails.
    pub async fn connect(url: &str, config: DatabaseConfig) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout)
            .connect(url)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self { pool })
    }

    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn ready(&self) -> bool {
        matches!(
            sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(&self.pool)
                .await,
            Ok(1)
        )
    }
}
