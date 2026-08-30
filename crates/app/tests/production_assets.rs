use std::{fs, path::PathBuf};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn systemd_unit_has_explicit_runtime_and_shutdown_boundaries() {
    let unit = fs::read_to_string(root().join("deploy/pons-radar.service")).unwrap();
    for required in [
        "User=pons-radar",
        "WorkingDirectory=/var/lib/pons-radar",
        "EnvironmentFile=/etc/pons-radar/environment",
        "KillSignal=SIGTERM",
        "KillMode=process",
        "TimeoutStopSec=45",
        "NoNewPrivileges=true",
        "ProtectSystem=strict",
        "UMask=0027",
    ] {
        assert!(
            unit.contains(required),
            "missing systemd boundary: {required}"
        );
    }
    assert!(!unit.contains("Environment=DATABASE_URL="));
}

#[test]
fn deployment_examples_contain_no_live_secrets_or_signal_feature_enablement() {
    let environment = fs::read_to_string(root().join("deploy/environment.example")).unwrap();
    assert!(environment.contains("REPLACE_ME"));
    assert!(!environment.contains("PRIVATE_KEY"));
    assert!(!environment.contains("SEED_PHRASE"));

    let config = fs::read_to_string(root().join("config.example.toml")).unwrap();
    assert_eq!(config.matches("[frontend_update]").count(), 1);
    assert!(config.contains("auto_install = false"));
    assert!(config.contains("request_timeout_seconds = 120"));
    assert!(config.contains("use_ai_research_in_signal = false"));
}

#[test]
fn live_validation_scripts_are_explicitly_operator_only() {
    let smoke = fs::read_to_string(root().join("scripts/phase21-rpc-smoke.sh")).unwrap();
    assert!(smoke.contains("Operator-only staging validation"));
    assert!(smoke.contains("expected Robinhood chain 4663/0x1237"));
    assert!(!smoke.contains("curl --insecure"));
    assert!(!smoke.contains("set +e"));
}
