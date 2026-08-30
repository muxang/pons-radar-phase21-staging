//! `PostgreSQL` infrastructure and repositories. SQL is kept inside this crate.

mod database;
pub mod repositories;

pub use database::{Database, DatabaseConfig, MIGRATOR};
pub use sqlx::PgPool;
