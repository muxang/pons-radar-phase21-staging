use std::sync::Arc;

use async_trait::async_trait;
use pons_chain::{BlockHeader, ChainLog, ChainRpc, LogFilter, Receipt, RunError};
use pons_domain::{BlockHash, BlockNumber, ChainId, ContractAddress, TxHash};
use pons_storage::repositories::{DeploymentChanges, DeploymentRepository, NewProtocolDeployment};
use pons_v2::{DeploymentRegistry, PONS_V2_FACTORY_FINGERPRINT, RegistryError};
use serde_json::json;
use sha3::{Digest, Keccak256};
use sqlx::PgPool;

struct Rpc {
    chain: u64,
    code: Vec<u8>,
}
#[async_trait]
impl ChainRpc for Rpc {
    async fn chain_id(&self) -> Result<ChainId, RunError> {
        Ok(ChainId::new(self.chain))
    }
    async fn block_number(&self) -> Result<BlockNumber, RunError> {
        Ok(BlockNumber::new(0))
    }
    async fn code(&self, _: ContractAddress) -> Result<Vec<u8>, RunError> {
        Ok(self.code.clone())
    }
    async fn block(&self, _: BlockNumber) -> Result<Option<BlockHeader>, RunError> {
        Ok(None)
    }
    async fn receipt(&self, _: TxHash) -> Result<Option<Receipt>, RunError> {
        Ok(None)
    }
    async fn logs(
        &self,
        _: BlockNumber,
        _: BlockNumber,
        _: &LogFilter,
    ) -> Result<Vec<ChainLog>, RunError> {
        Ok(vec![])
    }
}

async fn create(
    repo: &DeploymentRepository,
    expected: Option<BlockHash>,
    enabled: bool,
    marker: u8,
) -> pons_storage::repositories::ProtocolDeployment {
    let topics = json!(["0x1111111111111111111111111111111111111111111111111111111111111111"]);
    repo.create(&NewProtocolDeployment {
        chain_id: ChainId::new(4663),
        address: ContractAddress::from_slice(&[marker; 20]).unwrap(),
        start_block: BlockNumber::new(10),
        end_block: Some(BlockNumber::new(20)),
        enabled,
        expected_event_topics: &topics,
        expected_code_hash: expected,
        source: "fixture",
        interface_fingerprint: PONS_V2_FACTORY_FINGERPRINT,
    })
    .await
    .unwrap()
}

#[sqlx::test(migrations = "../../migrations")]
async fn valid_deployment_persists_evidence_and_survives_registry_reload(pool: PgPool) {
    let repo = DeploymentRepository::new(pool.clone());
    let code = vec![0x60, 0x01];
    let hash = BlockHash::from_slice(&Keccak256::digest(&code)).unwrap();
    let deployment = create(&repo, Some(hash), false, 1).await;
    let registry = DeploymentRegistry::new(
        repo.clone(),
        Arc::new(Rpc {
            chain: 4663,
            code: code.clone(),
        }),
    );
    let verified = registry.verify(deployment.id).await.unwrap();
    assert_eq!(verified.health, "VERIFIED");
    assert_eq!(verified.verification_evidence["bytecode_present"], true);
    assert!(verified.last_verified_at.is_some());
    repo.update(
        deployment.id,
        &DeploymentChanges {
            start_block: None,
            end_block: None,
            enabled: Some(true),
            expected_event_topics: None,
            expected_code_hash: None,
            source: None,
            interface_fingerprint: None,
        },
    )
    .await
    .unwrap();
    let reloaded = DeploymentRegistry::new(
        DeploymentRepository::new(pool),
        Arc::new(Rpc { chain: 4663, code }),
    );
    assert_eq!(
        reloaded
            .active_at(BlockNumber::new(15))
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        reloaded
            .active_at(BlockNumber::new(21))
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn empty_bytecode_and_wrong_chain_are_degraded_and_never_active(pool: PgPool) {
    for (marker, chain, code, reason) in [
        (2, 4663, vec![], "empty_bytecode"),
        (3, 1, vec![1], "chain_id_mismatch"),
    ] {
        let repo = DeploymentRepository::new(pool.clone());
        let deployment = create(&repo, None, true, marker).await;
        let registry = DeploymentRegistry::new(repo.clone(), Arc::new(Rpc { chain, code }));
        assert!(matches!(
            registry.verify(deployment.id).await,
            Err(RegistryError::Invalid(_))
        ));
        let saved = repo.get(deployment.id).await.unwrap().unwrap();
        assert_eq!(saved.health, "DEGRADED");
        assert!(
            saved.verification_evidence["checks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == reason)
        );
        assert!(
            registry
                .active_at(BlockNumber::new(15))
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn expected_code_hash_match_verifies_and_mismatch_is_excluded(pool: PgPool) {
    let code = vec![0x60, 0x02];
    let hash = BlockHash::from_slice(&Keccak256::digest(&code)).unwrap();
    let repo = DeploymentRepository::new(pool.clone());
    let good = create(&repo, Some(hash), false, 4).await;
    let registry = DeploymentRegistry::new(
        repo.clone(),
        Arc::new(Rpc {
            chain: 4663,
            code: code.clone(),
        }),
    );
    assert_eq!(
        registry
            .verify(good.id)
            .await
            .unwrap()
            .verification_evidence["code_hash_matches"],
        true
    );
    let bad = create(
        &repo,
        Some(BlockHash::from_slice(&[9; 32]).unwrap()),
        true,
        5,
    )
    .await;
    assert!(registry.verify(bad.id).await.is_err());
    assert!(
        registry
            .active_at(BlockNumber::new(15))
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn disabled_verified_deployment_is_not_an_ingestion_source(pool: PgPool) {
    let repo = DeploymentRepository::new(pool);
    let deployment = create(&repo, None, false, 6).await;
    let registry = DeploymentRegistry::new(
        repo,
        Arc::new(Rpc {
            chain: 4663,
            code: vec![1],
        }),
    );
    registry.verify(deployment.id).await.unwrap();
    assert!(
        registry
            .active_at(BlockNumber::new(15))
            .await
            .unwrap()
            .is_empty()
    );
}
