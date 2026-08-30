use std::time::Duration;

use async_trait::async_trait;
use pons_storage::repositories::{ContentRelationJob, ContentRepository};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderType {
    AuthorizedFomo,
    UserAuthorizedImport,
    ManualReference,
    OtherAuthorizedProvider,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuthorizationBasis {
    OfficialApi,
    WrittenPermission,
    UserProvided,
    ManualReference,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProviderCapabilities {
    pub automatic_fetch: bool,
    pub import: bool,
    pub raw_storage_allowed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProviderIdentity {
    pub key: String,
    pub provider_type: ProviderType,
    pub authorization_basis: AuthorizationBasis,
    pub provenance: Value,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderHealth {
    Unavailable,
    Disabled,
    Healthy,
    Degraded(String),
}
#[derive(Clone, Debug)]
pub struct ProviderContentReference {
    pub external_id: Option<String>,
    pub external_reference: Option<String>,
    pub published_at: chrono::DateTime<chrono::Utc>,
    pub summary: Option<String>,
    pub structured_analysis: Value,
}

#[async_trait]
pub trait TraderContentProvider: Send + Sync {
    fn identity(&self) -> ProviderIdentity;
    fn capabilities(&self) -> ProviderCapabilities;
    async fn health(&self) -> ProviderHealth;
    async fn fetch(&self, _cursor: Option<&str>) -> Result<Vec<ProviderContentReference>, String>;
}

#[must_use]
pub fn automatic_fetch_authorized(
    identity: &ProviderIdentity,
    capabilities: &ProviderCapabilities,
) -> bool {
    capabilities.automatic_fetch
        && matches!(
            identity.authorization_basis,
            AuthorizationBasis::OfficialApi
                | AuthorizationBasis::WrittenPermission
                | AuthorizationBasis::UserProvided
        )
}

/// The only shipped provider. It is an operator-authored reference/import boundary,
/// never an automated Fomo content source.
pub struct ManualReferenceProvider;
#[async_trait]
impl TraderContentProvider for ManualReferenceProvider {
    fn identity(&self) -> ProviderIdentity {
        ProviderIdentity {
            key: "manual-reference".into(),
            provider_type: ProviderType::ManualReference,
            authorization_basis: AuthorizationBasis::ManualReference,
            provenance: serde_json::json!({"operator_authored":true}),
        }
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            automatic_fetch: false,
            import: true,
            raw_storage_allowed: false,
        }
    }
    async fn health(&self) -> ProviderHealth {
        ProviderHealth::Disabled
    }
    async fn fetch(&self, _: Option<&str>) -> Result<Vec<ProviderContentReference>, String> {
        Err("manual reference provider has no automatic fetch capability".into())
    }
}

pub async fn run_content_relation_worker(
    repository: ContentRepository,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            ()=cancellation.cancelled()=>return,
            result=repository.claim_due()=>match result {
                Ok(Some(job))=>process(&repository,&job).await,
                Ok(None)=>tokio::time::sleep(Duration::from_millis(500)).await,
                Err(error)=>{tracing::error!(%error,"content relation claim failed");tokio::time::sleep(Duration::from_secs(2)).await;}
            }
        }
    }
}
async fn process(repository: &ContentRepository, job: &ContentRelationJob) {
    if let Err(error) = repository.rebuild(job).await {
        let exponent = u32::try_from(job.attempts.clamp(0, 8)).unwrap_or(8);
        let delay = Duration::from_secs(2_u64.saturating_pow(exponent).min(300));
        if let Err(retry) = repository
            .retry(
                job,
                &error.to_string(),
                chrono::Utc::now()
                    + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::minutes(5)),
            )
            .await
        {
            tracing::error!(%retry,"content relation retry persistence failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authorization_is_fail_closed() {
        let provider = ManualReferenceProvider;
        assert!(!automatic_fetch_authorized(
            &provider.identity(),
            &provider.capabilities()
        ));
        let mut identity = provider.identity();
        identity.provider_type = ProviderType::AuthorizedFomo;
        identity.authorization_basis = AuthorizationBasis::ManualReference;
        let caps = ProviderCapabilities {
            automatic_fetch: true,
            import: true,
            raw_storage_allowed: false,
        };
        assert!(!automatic_fetch_authorized(&identity, &caps));
    }
}
