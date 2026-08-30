use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pons_chain::RunError;
use pons_domain::WalletAddress;
use pons_storage::repositories::TradeCandidateIdentity;
use pons_storage::repositories::{
    IdentityClassificationRepository, TraderRepository, TraderWallet,
};
use pons_v2::TradeCandidateMatcher;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Clone)]
pub struct ExecutionWalletRegistry {
    repository: TraderRepository,
    minimum_confidence: Arc<str>,
    values: Arc<RwLock<HashMap<WalletAddress, TraderWallet>>>,
    history: Arc<RwLock<HashMap<WalletAddress, Vec<TraderWallet>>>>,
}

#[async_trait]
impl TradeCandidateMatcher for ExecutionWalletRegistry {
    async fn matched_identity(
        &self,
        address: WalletAddress,
        at: DateTime<Utc>,
    ) -> Result<Option<TradeCandidateIdentity>, RunError> {
        Ok(self.match_at(address,at).await?.map(|v|TradeCandidateIdentity{trader_id:v.trader_id,trader_wallet_id:v.id,wallet:v.address,confidence:v.confidence.clone(),verified:v.verified,role:v.role.clone(),source:v.source.clone(),snapshot:json!({"trader_id":v.trader_id,"trader_handle":v.trader_handle,"trader_wallet_id":v.id,"wallet":v.address.to_string(),"chain_id":v.chain_id.get(),"role":v.role,"source":v.source,"identity_confidence":v.confidence,"verified":v.verified,"valid_from":v.valid_from,"valid_to":v.valid_to,"evidence":v.evidence})}))
    }
}
#[allow(clippy::missing_errors_doc)]
impl ExecutionWalletRegistry {
    pub async fn rebuild(
        repository: TraderRepository,
        minimum_confidence: impl Into<Arc<str>>,
    ) -> Result<Self, sqlx::Error> {
        let value = Self {
            repository,
            minimum_confidence: minimum_confidence.into(),
            values: Arc::default(),
            history: Arc::default(),
        };
        value.refresh_at(Utc::now()).await?;
        Ok(value)
    }
    #[must_use]
    pub const fn repository(&self) -> &TraderRepository {
        &self.repository
    }
    pub async fn enqueue_historical_classification(
        &self,
        wallet_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        IdentityClassificationRepository::new(self.repository.pool())
            .enqueue_eligible(wallet_id, &self.minimum_confidence)
            .await
    }
    pub async fn refresh(&self) -> Result<(), sqlx::Error> {
        self.refresh_at(Utc::now()).await
    }
    pub async fn refresh_at(&self, at: DateTime<Utc>) -> Result<(), sqlx::Error> {
        let rows = self.repository.active(&self.minimum_confidence, at).await?;
        let mut next = HashMap::with_capacity(rows.len());
        for wallet in rows {
            if next.insert(wallet.address, wallet).is_some() {
                return Err(sqlx::Error::Protocol(
                    "ambiguous active execution wallet".into(),
                ));
            }
        }
        *self.values.write().await = next;
        let mut history: HashMap<WalletAddress, Vec<TraderWallet>> = HashMap::new();
        for wallet in self
            .repository
            .eligible_history(&self.minimum_confidence)
            .await?
        {
            history.entry(wallet.address).or_default().push(wallet);
        }
        *self.history.write().await = history;
        Ok(())
    }
    pub async fn match_address(&self, address: WalletAddress) -> Option<TraderWallet> {
        self.values.read().await.get(&address).cloned()
    }
    pub async fn len(&self) -> usize {
        self.values.read().await.len()
    }
    pub async fn is_empty(&self) -> bool {
        self.values.read().await.is_empty()
    }
    pub async fn match_at(
        &self,
        address: WalletAddress,
        at: DateTime<Utc>,
    ) -> Result<Option<TraderWallet>, RunError> {
        let values = self.history.read().await;
        let mut found = values
            .get(&address)
            .into_iter()
            .flatten()
            .filter(|v| v.valid_from <= at && v.valid_to.is_none_or(|to| at < to));
        let first = found.next().cloned();
        if found.next().is_some() {
            return Err(RunError::Handler(
                "ambiguous execution identity at event time".into(),
            ));
        }
        Ok(first)
    }
}
