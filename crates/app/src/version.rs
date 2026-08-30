use chrono::{DateTime, Utc};
use serde::Serialize;

pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const FRONTEND_BUILD_ID: &str = match option_env!("FRONTEND_BUILD_ID") {
    Some(value) => value,
    None => "dev",
};
pub const API_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize)]
pub struct VersionInfo {
    pub app_version: &'static str,
    pub frontend_build_id: &'static str,
    pub api_schema_version: u32,
    pub started_at: DateTime<Utc>,
}

impl VersionInfo {
    #[must_use]
    pub const fn new(started_at: DateTime<Utc>) -> Self {
        Self {
            app_version: APP_VERSION,
            frontend_build_id: FRONTEND_BUILD_ID,
            api_schema_version: API_SCHEMA_VERSION,
            started_at,
        }
    }
}
