use chrono::Utc;
use pons_chain::ChainLog;
use pons_domain::{BlockNumber, ChainId, ContractAddress, LogIndex, LogTopic};
use pons_storage::repositories::ProtocolDeployment;
use pons_v2::{LaunchError, decode_token_launched, factory_log_filter, token_launched_topic};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Fixture {
    address: String,
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    transaction_index: String,
    log_index: String,
    removed: bool,
    topics: Vec<String>,
    data: String,
}
fn quantity(value: &str) -> u64 {
    u64::from_str_radix(value.trim_start_matches("0x"), 16).unwrap()
}
fn fixture() -> ChainLog {
    let value: Fixture = serde_json::from_str(include_str!(
        "../../../fixtures/pons_v2/token_launched_0xeaae3543.json"
    ))
    .unwrap();
    ChainLog {
        block_number: BlockNumber::new(quantity(&value.block_number)),
        block_hash: value.block_hash.parse().unwrap(),
        tx_hash: value.transaction_hash.parse().unwrap(),
        transaction_index: Some(quantity(&value.transaction_index)),
        log_index: LogIndex::new(quantity(&value.log_index)),
        address: value.address.parse().unwrap(),
        topics: value
            .topics
            .into_iter()
            .map(|v| v.parse().unwrap())
            .collect(),
        data: alloy_primitives::hex::decode(value.data).unwrap(),
        removed: value.removed,
    }
}
fn deployment() -> ProtocolDeployment {
    ProtocolDeployment {
        id: Uuid::new_v4(),
        protocol: "PONS".into(),
        generation: "V2".into(),
        chain_id: ChainId::new(4663),
        address: "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"
            .parse()
            .unwrap(),
        start_block: BlockNumber::new(26_841_846),
        end_block: None,
        enabled: true,
        expected_event_topics: serde_json::json!([]),
        expected_code_hash: None,
        source: "official-docs".into(),
        interface_fingerprint: "pons-v2-factory:v1".into(),
        last_verified_at: Some(Utc::now()),
        health: "VERIFIED".into(),
        verification_evidence: serde_json::json!({}),
        verification_error: None,
        trust_basis: "OPERATOR_APPROVED".into(),
        approved_by: Some(Uuid::new_v4()),
        approved_at: Some(Utc::now()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn real_v2_fixture_decodes_indexed_and_non_indexed_fields() {
    let decoded = decode_token_launched(&fixture(), &deployment()).unwrap();
    assert_eq!(
        decoded.token.to_string(),
        "0xf9b84b5f789499632bc222a91d79f433c263c827"
    );
    assert_eq!(
        decoded.curve.to_string(),
        "0x6393305a54b3b15819a3e8f693addadd2eebd021"
    );
    assert_eq!(
        decoded.deployer.to_string(),
        "0x5be0405dc84593fddbeccb80abb9d8cb0df75519"
    );
    assert_eq!(
        decoded.pair_token.to_string(),
        "0x6330d8c3178a418788df01a47479c0ce7ccf450b"
    );
    assert_eq!(decoded.launch_config_id, "0");
    assert_eq!(decoded.graduation_threshold, "52470673453406424989");
}
#[test]
fn filter_requires_factory_and_token_launched_topic() {
    let deployment = deployment();
    let filter = factory_log_filter(&deployment);
    assert_eq!(filter.addresses, [deployment.address]);
    assert_eq!(filter.topics, [Some(vec![token_launched_topic()])]);
    assert_eq!(
        token_launched_topic().to_string(),
        "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
    );
}
#[test]
fn wrong_emitter_wrong_topic_and_malformed_data_fail_closed() {
    let deployment = deployment();
    let mut log = fixture();
    log.address = ContractAddress::from_slice(&[9; 20]).unwrap();
    assert!(matches!(
        decode_token_launched(&log, &deployment),
        Err(LaunchError::WrongEmitter)
    ));
    let mut log = fixture();
    log.topics[0] = LogTopic::from_slice(&[9; 32]).unwrap();
    assert!(matches!(
        decode_token_launched(&log, &deployment),
        Err(LaunchError::Decode(_))
    ));
    let mut log = fixture();
    log.data.truncate(32);
    assert!(matches!(
        decode_token_launched(&log, &deployment),
        Err(LaunchError::Decode(_))
    ));
}
#[test]
fn deployment_state_and_block_range_are_enforced() {
    let log = fixture();
    let mut value = deployment();
    value.enabled = false;
    assert!(matches!(
        decode_token_launched(&log, &value),
        Err(LaunchError::InactiveDeployment(_))
    ));
    value.enabled = true;
    value.health = "UNVERIFIED".into();
    assert!(decode_token_launched(&log, &value).is_err());
    value.health = "VERIFIED".into();
    value.end_block = Some(BlockNumber::new(log.block_number.get() - 1));
    assert!(decode_token_launched(&log, &value).is_err());
}
