#![allow(clippy::missing_errors_doc)]

use std::{
    collections::HashMap,
    io::Read,
    path::{Component, Path, PathBuf},
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures_util::StreamExt;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

pub const SUPPORTED_MANIFEST_VERSION: u32 = 1;
pub const EXPECTED_ARCHIVE_FILES: [&str; 3] = ["pons-radar", "pons-radar-updater", "VERSION"];
const RELEASE_BUILD_KEY_ID: &str = match option_env!("PONS_RELEASE_TRUST_ROOT_ID") {
    None => "pons-release-root-2026-a",
    Some(value) => value,
};
const RELEASE_BUILD_KEY_HEX: &str = match option_env!("PONS_RELEASE_TRUST_ROOT_HEX") {
    None => "c4b11da5e5b7333b38c60943d853682332b07e388978a3dec53336c47116d0c5",
    Some(value) => value,
};
/// Offline release trust anchors compiled into both updater binaries. Release
/// builds inject the active public root through non-secret build inputs.
pub const BUILTIN_TRUSTED_KEYS: [(&str, &str); 2] = [
    (
        "pons-release-root-2026-a",
        "c4b11da5e5b7333b38c60943d853682332b07e388978a3dec53336c47116d0c5",
    ),
    (RELEASE_BUILD_KEY_ID, RELEASE_BUILD_KEY_HEX),
];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseManifest {
    pub manifest_version: u32,
    pub app_version: Version,
    pub channel: String,
    pub published_at: DateTime<Utc>,
    pub git_commit: String,
    pub build_timestamp: DateTime<Utc>,
    pub frontend_build_id: String,
    pub api_schema_version: u32,
    pub min_db_schema: i64,
    pub max_db_schema: i64,
    pub target_db_schema: i64,
    pub rollback_safe: bool,
    pub old_binary_compatible_with_target_schema: bool,
    pub assets: Vec<ReleaseAsset>,
    pub release_notes: Option<String>,
    pub signing_key_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseAsset {
    pub platform: String,
    pub architecture: String,
    pub filename: String,
    pub size: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedManifest {
    pub manifest: ReleaseManifest,
    pub sha256: String,
}

#[derive(Clone, Debug)]
pub struct TrustedKeys(HashMap<String, VerifyingKey>);

impl TrustedKeys {
    pub fn from_hex(
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, UpdateError> {
        let mut keys = HashMap::new();
        for (id, encoded) in values {
            let raw =
                hex::decode(encoded).map_err(|_| UpdateError::InvalidTrustedKey(id.clone()))?;
            let bytes: [u8; 32] = raw
                .try_into()
                .map_err(|_| UpdateError::InvalidTrustedKey(id.clone()))?;
            let key = VerifyingKey::from_bytes(&bytes)
                .map_err(|_| UpdateError::InvalidTrustedKey(id.clone()))?;
            if id.trim().is_empty() {
                return Err(UpdateError::InvalidTrustedKey(id));
            }
            if let Some(existing) = keys.insert(id.clone(), key) {
                if existing != key {
                    return Err(UpdateError::ConflictingTrustedKey(id));
                }
            }
        }
        Ok(Self(keys))
    }

    /// Constructs the immutable built-in trust set and adds explicit
    /// deployment-pinned roots. A deployment key can never replace a built-in
    /// key with the same id.
    pub fn builtin_with_deployment(
        deployment: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, UpdateError> {
        let mut trusted = Self::from_hex(
            BUILTIN_TRUSTED_KEYS
                .iter()
                .map(|(id, key)| ((*id).to_owned(), (*key).to_owned())),
        )?;
        let deployment = Self::from_hex(deployment)?;
        for (id, key) in deployment.0 {
            if let Some(existing) = trusted.0.get(&id) {
                if existing != &key {
                    return Err(UpdateError::ConflictingTrustedKey(id));
                }
            } else {
                trusted.0.insert(id, key);
            }
        }
        Ok(trusted)
    }

    #[must_use]
    pub fn contains(&self, key_id: &str) -> bool {
        self.0.contains_key(key_id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error("release manifest signature is missing")]
    MissingSignature,
    #[error("release signing key is unknown: {0}")]
    UnknownKey(String),
    #[error("trusted public key is invalid: {0}")]
    InvalidTrustedKey(String),
    #[error("deployment trusted key conflicts with built-in key id: {0}")]
    ConflictingTrustedKey(String),
    #[error("release manifest signature is invalid")]
    InvalidSignature,
    #[error("release manifest is invalid: {0}")]
    InvalidManifest(String),
    #[error("unsupported manifest version {0}")]
    UnsupportedManifest(u32),
    #[error("no release asset for {0}-{1}")]
    WrongArchitecture(String, String),
    #[error("asset SHA256 mismatch")]
    HashMismatch,
    #[error("asset exceeds signed size")]
    OversizedAsset,
    #[error("unsafe or unexpected archive entry: {0}")]
    UnsafeArchive(String),
    #[error("database schema {current} is outside supported range {min}..={max}")]
    SchemaIncompatible { current: i64, min: i64, max: i64 },
    #[error("release cannot safely roll back after its target database migration")]
    RollbackUnsafe,
    #[error("target version is not newer than current version")]
    NotNewer,
    #[error("untrusted release asset URL")]
    UntrustedAssetUrl,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

pub fn verify_manifest(
    raw: &[u8],
    signature_b64: Option<&str>,
    keys: &TrustedKeys,
) -> Result<VerifiedManifest, UpdateError> {
    let signature_b64 = signature_b64.ok_or(UpdateError::MissingSignature)?;
    // Read only the key id before trust verification. No install decision uses unverified fields.
    let value: serde_json::Value =
        serde_json::from_slice(raw).map_err(|e| UpdateError::InvalidManifest(e.to_string()))?;
    let key_id = value
        .get("signing_key_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| UpdateError::InvalidManifest("missing signing_key_id".into()))?;
    let key = keys
        .0
        .get(key_id)
        .ok_or_else(|| UpdateError::UnknownKey(key_id.into()))?;
    let bytes = STANDARD
        .decode(signature_b64.trim())
        .map_err(|_| UpdateError::InvalidSignature)?;
    let signature = Signature::from_slice(&bytes).map_err(|_| UpdateError::InvalidSignature)?;
    key.verify(raw, &signature)
        .map_err(|_| UpdateError::InvalidSignature)?;
    let manifest: ReleaseManifest =
        serde_json::from_slice(raw).map_err(|e| UpdateError::InvalidManifest(e.to_string()))?;
    validate_manifest(&manifest)?;
    Ok(VerifiedManifest {
        manifest,
        sha256: hex::encode(Sha256::digest(raw)),
    })
}

fn validate_manifest(value: &ReleaseManifest) -> Result<(), UpdateError> {
    if value.manifest_version != SUPPORTED_MANIFEST_VERSION {
        return Err(UpdateError::UnsupportedManifest(value.manifest_version));
    }
    if value.channel != "stable" {
        return Err(UpdateError::InvalidManifest(
            "unsupported release channel".into(),
        ));
    }
    if value.min_db_schema > value.max_db_schema
        || value.target_db_schema < value.min_db_schema
        || value.target_db_schema > value.max_db_schema
    {
        return Err(UpdateError::InvalidManifest(
            "inconsistent database schema bounds".into(),
        ));
    }
    if value.git_commit.is_empty() || value.frontend_build_id.is_empty() || value.assets.is_empty()
    {
        return Err(UpdateError::InvalidManifest(
            "required build metadata is empty".into(),
        ));
    }
    for asset in &value.assets {
        if asset.size == 0
            || asset.filename.contains('/')
            || asset.filename.contains('\\')
            || asset.sha256.len() != 64
            || hex::decode(&asset.sha256).is_err()
        {
            return Err(UpdateError::InvalidManifest(
                "invalid asset metadata".into(),
            ));
        }
    }
    Ok(())
}

pub fn select_asset<'a>(
    manifest: &'a ReleaseManifest,
    os: &str,
    arch: &str,
) -> Result<&'a ReleaseAsset, UpdateError> {
    manifest
        .assets
        .iter()
        .find(|a| a.platform == os && a.architecture == arch)
        .ok_or_else(|| UpdateError::WrongArchitecture(os.into(), arch.into()))
}

pub fn check_compatibility(
    manifest: &ReleaseManifest,
    current: &Version,
    db_schema: i64,
) -> Result<(), UpdateError> {
    if manifest.app_version <= *current {
        return Err(UpdateError::NotNewer);
    }
    if !(manifest.min_db_schema..=manifest.max_db_schema).contains(&db_schema) {
        return Err(UpdateError::SchemaIncompatible {
            current: db_schema,
            min: manifest.min_db_schema,
            max: manifest.max_db_schema,
        });
    }
    if !manifest.rollback_safe || !manifest.old_binary_compatible_with_target_schema {
        return Err(UpdateError::RollbackUnsafe);
    }
    Ok(())
}

pub fn trusted_asset_url(
    owner: &str,
    repo: &str,
    release_id: u64,
    asset_id: u64,
) -> Result<url::Url, UpdateError> {
    let url = format!(
        "https://api.github.com/repos/{owner}/{repo}/releases/assets/{asset_id}?release_id={release_id}"
    );
    let parsed = url::Url::parse(&url).map_err(|_| UpdateError::UntrustedAssetUrl)?;
    if parsed.scheme() != "https" || parsed.host_str() != Some("api.github.com") {
        return Err(UpdateError::UntrustedAssetUrl);
    }
    Ok(parsed)
}

pub async fn download_verified(
    client: &reqwest::Client,
    url: url::Url,
    destination: &Path,
    expected_size: u64,
    expected_sha256: &str,
    token: Option<&str>,
) -> Result<(), UpdateError> {
    if url.scheme() != "https" || url.host_str() != Some("api.github.com") {
        return Err(UpdateError::UntrustedAssetUrl);
    }
    let mut request = client.get(url).header("Accept", "application/octet-stream");
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request.send().await?.error_for_status()?;
    let mut file = tokio::fs::File::create(destination).await?;
    let mut stream = response.bytes_stream();
    let mut size = 0_u64;
    let mut hash = Sha256::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        size = size
            .checked_add(chunk.len() as u64)
            .ok_or(UpdateError::OversizedAsset)?;
        if size > expected_size {
            let _ = tokio::fs::remove_file(destination).await;
            return Err(UpdateError::OversizedAsset);
        }
        hash.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    if size != expected_size || hex::encode(hash.finalize()) != expected_sha256.to_ascii_lowercase()
    {
        drop(file);
        let _ = tokio::fs::remove_file(destination).await;
        return Err(UpdateError::HashMismatch);
    }
    Ok(())
}

/// Copies a verified binary into a sibling staging file and atomically renames it over
/// the destination. The source may reside on a different filesystem; the final rename
/// is always confined to the destination filesystem.
pub async fn atomic_replace_binary(source: &Path, destination: &Path) -> std::io::Result<()> {
    let permissions = tokio::fs::metadata(destination).await?.permissions();
    let replacement = destination.with_extension("pons-radar.new");
    tokio::fs::copy(source, &replacement).await?;
    tokio::fs::set_permissions(&replacement, permissions).await?;
    tokio::fs::OpenOptions::new()
        .read(true)
        .open(&replacement)
        .await?
        .sync_all()
        .await?;
    tokio::fs::rename(&replacement, destination).await?;
    if let Some(parent) = destination.parent() {
        tokio::fs::File::open(parent).await?.sync_all().await?;
    }
    Ok(())
}

pub fn extract_safe(archive: &Path, staging: &Path) -> Result<(), UpdateError> {
    std::fs::create_dir_all(staging)?;
    let decoder = flate2::read::GzDecoder::new(std::fs::File::open(archive)?);
    let mut tar = tar::Archive::new(decoder);
    let mut seen = Vec::new();
    for entry in tar.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let safe = path.components().all(|c| matches!(c, Component::Normal(_)))
            && path.components().count() == 1;
        let name = path.to_string_lossy().to_string();
        if !safe
            || !EXPECTED_ARCHIVE_FILES.contains(&name.as_str())
            || entry.header().entry_type().is_symlink()
            || entry.header().entry_type().is_hard_link()
        {
            return Err(UpdateError::UnsafeArchive(name));
        }
        entry.unpack(staging.join(&path))?;
        seen.push(name);
    }
    seen.sort();
    seen.dedup();
    let mut expected = EXPECTED_ARCHIVE_FILES.map(str::to_owned).to_vec();
    expected.sort();
    if seen != expected {
        return Err(UpdateError::UnsafeArchive(
            "archive file set is incomplete".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HandoffPlan {
    pub job_id: String,
    pub parent_pid: u32,
    pub current_binary: PathBuf,
    pub staged_binary: PathBuf,
    pub backup_binary: PathBuf,
    pub service_name: String,
    pub health_base_url: String,
    pub target_version: String,
    pub previous_version: String,
    pub frontend_build_id: String,
    pub api_schema_version: u32,
    pub timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HealthVersion {
    pub app_version: String,
    pub frontend_build_id: String,
    pub api_schema_version: u32,
}

pub async fn wait_for_target_health(
    client: &reqwest::Client,
    plan: &HandoffPlan,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(plan.timeout_seconds);
    loop {
        let health = client
            .get(format!("{}/healthz", plan.health_base_url))
            .send()
            .await;
        let ready = client
            .get(format!("{}/readyz", plan.health_base_url))
            .send()
            .await;
        let version = client
            .get(format!("{}/api/v1/system/version", plan.health_base_url))
            .send()
            .await;
        if health.as_ref().is_ok_and(|r| r.status().is_success())
            && ready.as_ref().is_ok_and(|r| r.status().is_success())
        {
            if let Ok(response) = version {
                if response.status().is_success() {
                    let got: HealthVersion = response.json().await?;
                    if got.app_version == plan.target_version
                        && got.frontend_build_id == plan.frontend_build_id
                        && got.api_schema_version == plan.api_schema_version
                    {
                        return Ok(());
                    }
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("post-install health/version verification timed out");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[must_use]
pub fn redact_secret(message: &str, secret: Option<&str>) -> String {
    secret
        .filter(|s| !s.is_empty())
        .map_or_else(|| message.to_owned(), |s| message.replace(s, "[REDACTED]"))
}

pub fn hash_reader(mut input: impl Read) -> Result<String, std::io::Error> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}

pub fn verify_file(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), UpdateError> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() > expected_size {
        return Err(UpdateError::OversizedAsset);
    }
    if metadata.len() != expected_size
        || hash_reader(std::fs::File::open(path)?)? != expected_sha256.to_ascii_lowercase()
    {
        return Err(UpdateError::HashMismatch);
    }
    Ok(())
}
