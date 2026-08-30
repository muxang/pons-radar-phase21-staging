use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::Utc;
use ed25519_dalek::{Signer, SigningKey};
use flate2::{Compression, write::GzEncoder};
use pons_updater::*;
use semver::Version;
use sha2::{Digest, Sha256};

fn manifest() -> ReleaseManifest {
    ReleaseManifest {
        manifest_version: 1,
        app_version: Version::new(1, 2, 0),
        channel: "stable".into(),
        published_at: Utc::now(),
        git_commit: "abc".into(),
        build_timestamp: Utc::now(),
        frontend_build_id: "web-1".into(),
        api_schema_version: 1,
        min_db_schema: 20,
        max_db_schema: 23,
        target_db_schema: 23,
        rollback_safe: true,
        old_binary_compatible_with_target_schema: true,
        assets: vec![ReleaseAsset {
            platform: "linux".into(),
            architecture: "x86_64".into(),
            filename: "pons-radar-linux-x86_64.tar.gz".into(),
            size: 10,
            sha256: "00".repeat(32),
        }],
        release_notes: None,
        signing_key_id: "release-2026".into(),
    }
}

fn signed(value: &ReleaseManifest) -> (Vec<u8>, String, TrustedKeys) {
    let key = SigningKey::from_bytes(&[7; 32]);
    let raw = serde_json::to_vec(value).unwrap();
    let sig = STANDARD.encode(key.sign(&raw).to_bytes());
    let keys = TrustedKeys::from_hex([(
        "release-2026".into(),
        hex::encode(key.verifying_key().to_bytes()),
    )])
    .unwrap();
    (raw, sig, keys)
}

#[test]
fn signature_modified_unknown_key_and_version_fail_closed() {
    let (raw, sig, keys) = signed(&manifest());
    assert_eq!(
        verify_manifest(&raw, Some(&sig), &keys)
            .unwrap()
            .manifest
            .app_version,
        Version::new(1, 2, 0)
    );
    let mut changed = raw.clone();
    changed[5] ^= 1;
    assert!(verify_manifest(&changed, Some(&sig), &keys).is_err());
    assert!(matches!(
        verify_manifest(&raw, None, &keys),
        Err(UpdateError::MissingSignature)
    ));
    let empty = TrustedKeys::from_hex([]).unwrap();
    assert!(matches!(
        verify_manifest(&raw, Some(&sig), &empty),
        Err(UpdateError::UnknownKey(_))
    ));
    let mut unsupported = manifest();
    unsupported.manifest_version = 2;
    let (raw, sig, keys) = signed(&unsupported);
    assert!(matches!(
        verify_manifest(&raw, Some(&sig), &keys),
        Err(UpdateError::UnsupportedManifest(2))
    ));
}

#[test]
fn built_in_root_verifies_without_remote_or_deployment_bootstrap() {
    let raw = br#"{"manifest_version":1,"app_version":"1.2.0","channel":"stable","published_at":"2026-08-30T00:00:00Z","git_commit":"abc","build_timestamp":"2026-08-30T00:00:00Z","frontend_build_id":"web-1","api_schema_version":1,"min_db_schema":20,"max_db_schema":23,"target_db_schema":23,"rollback_safe":true,"old_binary_compatible_with_target_schema":true,"assets":[{"platform":"linux","architecture":"x86_64","filename":"pons-radar-linux-x86_64.tar.gz","size":10,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"}],"release_notes":null,"signing_key_id":"pons-release-root-2026-a"}"#;
    let signature =
        "SPgZJyQhjZ+FDPybVdYWoa22e+wWaspAI7zjT3qLCE8IaV5sPbLjp0HIsICdOIzVOaX+qmf9RgW3zsuaOVjSDw==";
    let roots = TrustedKeys::builtin_with_deployment([]).unwrap();
    assert!(roots.contains("pons-release-root-2026-a"));
    assert!(verify_manifest(raw, Some(signature), &roots).is_ok());
}

#[test]
fn signed_manifest_cannot_bootstrap_a_remote_key() {
    let value = manifest();
    let key = SigningKey::from_bytes(&[7; 32]);
    let mut json = serde_json::to_value(&value).unwrap();
    json["public_key"] = serde_json::Value::String(hex::encode(key.verifying_key().to_bytes()));
    json["remote_key_url"] = serde_json::Value::String("https://example.invalid/key".into());
    let raw = serde_json::to_vec(&json).unwrap();
    let sig = STANDARD.encode(key.sign(&raw).to_bytes());
    let roots = TrustedKeys::from_hex([(
        value.signing_key_id,
        hex::encode(key.verifying_key().to_bytes()),
    )])
    .unwrap();
    assert!(matches!(
        verify_manifest(&raw, Some(&sig), &roots),
        Err(UpdateError::InvalidManifest(_))
    ));
}

#[test]
fn deployment_pin_and_overlapping_rotation_are_explicit_and_fail_closed() {
    let key_a = SigningKey::from_bytes(&[11; 32]);
    let key_b = SigningKey::from_bytes(&[12; 32]);
    let key_c = SigningKey::from_bytes(&[13; 32]);
    let root =
        |id: &str, key: &SigningKey| (id.to_owned(), hex::encode(key.verifying_key().to_bytes()));
    let generation_n =
        TrustedKeys::from_hex([root("key-a", &key_a), root("key-b", &key_b)]).unwrap();
    let generation_n1 =
        TrustedKeys::from_hex([root("key-b", &key_b), root("key-c", &key_c)]).unwrap();
    assert!(generation_n.contains("key-b") && generation_n1.contains("key-b"));
    assert!(!generation_n1.contains("key-a"));

    let mut value = manifest();
    value.signing_key_id = "key-a".into();
    let raw = serde_json::to_vec(&value).unwrap();
    let sig = STANDARD.encode(key_a.sign(&raw).to_bytes());
    assert!(verify_manifest(&raw, Some(&sig), &generation_n).is_ok());
    assert!(matches!(
        verify_manifest(&raw, Some(&sig), &generation_n1),
        Err(UpdateError::UnknownKey(_))
    ));

    let deployment =
        TrustedKeys::builtin_with_deployment([root("deployment-key", &key_c)]).unwrap();
    assert!(deployment.contains("pons-release-root-2026-a"));
    assert!(deployment.contains("deployment-key"));
    assert!(matches!(
        TrustedKeys::builtin_with_deployment([root("pons-release-root-2026-a", &key_c)]),
        Err(UpdateError::ConflictingTrustedKey(_))
    ));
}

#[test]
fn architecture_schema_downgrade_and_rollback_policy_are_enforced() {
    let m = manifest();
    assert!(select_asset(&m, "linux", "x86_64").is_ok());
    assert!(matches!(
        select_asset(&m, "linux", "aarch64"),
        Err(UpdateError::WrongArchitecture(_, _))
    ));
    assert!(check_compatibility(&m, &Version::new(1, 1, 0), 22).is_ok());
    assert!(matches!(
        check_compatibility(&m, &Version::new(1, 2, 0), 22),
        Err(UpdateError::NotNewer)
    ));
    assert!(matches!(
        check_compatibility(&m, &Version::new(1, 1, 0), 19),
        Err(UpdateError::SchemaIncompatible { .. })
    ));
    let mut unsafe_release = m;
    unsafe_release.rollback_safe = false;
    assert!(matches!(
        check_compatibility(&unsafe_release, &Version::new(1, 1, 0), 22),
        Err(UpdateError::RollbackUnsafe)
    ));
}

#[test]
fn hashes_are_exact_and_secrets_are_redacted() {
    let bytes = b"release";
    assert_eq!(
        hash_reader(&bytes[..]).unwrap(),
        hex::encode(Sha256::digest(bytes))
    );
    assert_eq!(
        redact_secret("Bearer top-secret", Some("top-secret")),
        "Bearer [REDACTED]"
    );
    assert!(
        trusted_asset_url("owner", "repo", 1, 2)
            .unwrap()
            .as_str()
            .starts_with("https://api.github.com/")
    );
}

#[test]
fn file_size_and_sha256_are_fail_closed() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("asset");
    std::fs::write(&path, b"release").unwrap();
    let hash = hex::encode(Sha256::digest(b"release"));
    assert!(verify_file(&path, 7, &hash).is_ok());
    assert!(matches!(
        verify_file(&path, 7, &"00".repeat(32)),
        Err(UpdateError::HashMismatch)
    ));
    assert!(matches!(
        verify_file(&path, 6, &hash),
        Err(UpdateError::OversizedAsset)
    ));
}

#[tokio::test]
async fn atomic_replacement_copies_from_source_and_preserves_destination_mode() {
    let source_root = tempfile::tempdir().unwrap();
    let destination_root = tempfile::tempdir().unwrap();
    let source = source_root.path().join("backup");
    let destination = destination_root.path().join("pons-radar");
    tokio::fs::write(&source, b"old-binary").await.unwrap();
    tokio::fs::write(&destination, b"broken-binary")
        .await
        .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&destination, std::fs::Permissions::from_mode(0o750))
            .await
            .unwrap();
    }

    pons_updater::atomic_replace_binary(&source, &destination)
        .await
        .unwrap();
    assert_eq!(tokio::fs::read(&destination).await.unwrap(), b"old-binary");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            tokio::fs::metadata(&destination)
                .await
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
    }
    assert!(
        source.exists(),
        "rollback backup remains available for audit"
    );
}

fn archive(path: &std::path::Path, names: &[&str]) {
    let file = std::fs::File::create(path).unwrap();
    let gz = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(gz);
    for name in names {
        let data = b"x";
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o755);
        header.set_cksum();
        tar.append_data(&mut header, name, &data[..]).unwrap();
    }
    tar.finish().unwrap();
}

#[test]
fn safe_archive_requires_exact_files_and_rejects_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let good = temp.path().join("good.tar.gz");
    archive(&good, &EXPECTED_ARCHIVE_FILES);
    extract_safe(&good, &temp.path().join("out")).unwrap();
    let bad = temp.path().join("bad.tar.gz");
    let file = std::fs::File::create(&bad).unwrap();
    let gz = GzEncoder::new(file, Compression::default());
    let mut tar = tar::Builder::new(gz);
    let mut header = tar::Header::new_gnu();
    header.set_size(1);
    header.set_mode(0o755);
    header.set_cksum();
    // Construct the malicious path in the raw header because tar's safe API rejects it first.
    header.as_mut_bytes()[..13].copy_from_slice(b"../../etc/pwd");
    header.set_cksum();
    tar.append(&header, &b"x"[..]).unwrap();
    tar.finish().unwrap();
    assert!(extract_safe(&bad, &temp.path().join("bad-out")).is_err());
}

#[tokio::test]
async fn post_install_health_requires_ready_and_exact_build_identity() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = vec![0; 2048];
            let n = socket.read(&mut request).await.unwrap();
            let path = String::from_utf8_lossy(&request[..n]);
            let body = if path.starts_with("GET /api/v1/system/version ") {
                r#"{"app_version":"1.2.0","frontend_build_id":"web-1","api_schema_version":1}"#
            } else {
                r#"{"status":"ok"}"#
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    let plan = HandoffPlan {
        job_id: "00000000-0000-0000-0000-000000000001".into(),
        parent_pid: 1,
        current_binary: "old".into(),
        staged_binary: "new".into(),
        backup_binary: "backup".into(),
        service_name: "pons-radar.service".into(),
        health_base_url: format!("http://{address}"),
        target_version: "1.2.0".into(),
        previous_version: "1.1.0".into(),
        frontend_build_id: "web-1".into(),
        api_schema_version: 1,
        timeout_seconds: 1,
    };
    wait_for_target_health(&reqwest::Client::new(), &plan)
        .await
        .unwrap();
    server.abort();
}
