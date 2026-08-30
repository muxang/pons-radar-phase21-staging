#![allow(clippy::missing_errors_doc)]

use std::{path::PathBuf, time::Duration};

use anyhow::Context;
use pons_storage::repositories::{NewUpdateJob, UpdateRepository};
use pons_updater::{
    HandoffPlan, TrustedKeys, VerifiedManifest, check_compatibility, download_verified,
    extract_safe, select_asset, trusted_asset_url, verify_manifest,
};
use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{config::UpdaterSettings, version::APP_VERSION};

const USER_AGENT: &str = "pons-radar-secure-updater";

#[derive(Clone)]
pub struct UpdateService {
    config: UpdaterSettings,
    repository: UpdateRepository,
    client: reqwest::Client,
    keys: TrustedKeys,
    token: Option<String>,
    data_dir: PathBuf,
    shutdown: tokio_util::sync::CancellationToken,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GithubAsset {
    pub id: u64,
    pub name: String,
    pub size: u64,
    pub browser_download_url: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GithubRelease {
    pub id: u64,
    pub tag_name: String,
    pub draft: bool,
    pub prerelease: bool,
    pub published_at: Option<String>,
    pub html_url: String,
    pub assets: Vec<GithubAsset>,
}

#[derive(Clone, Debug)]
pub struct CheckedRelease {
    pub release: GithubRelease,
    pub verified: VerifiedManifest,
    pub asset_id: u64,
}

impl UpdateService {
    pub fn new(
        config: UpdaterSettings,
        repository: UpdateRepository,
        data_dir: PathBuf,
        token: Option<String>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> anyhow::Result<Self> {
        if config.auto_install {
            anyhow::bail!("automatic updater installation is forbidden in Phase 17");
        }
        let keys = TrustedKeys::builtin_with_deployment(
            config
                .deployment_pinned_trusted_keys
                .iter()
                .map(|k| (k.key_id.clone(), k.public_key_hex.clone())),
        )?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.request_timeout_seconds))
            .user_agent(USER_AGENT)
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()?;
        Ok(Self {
            config,
            repository,
            client,
            keys,
            token,
            data_dir,
            shutdown,
        })
    }
    #[must_use]
    pub fn repository(&self) -> &UpdateRepository {
        &self.repository
    }

    pub async fn check(&self, db_schema: i64) -> anyhow::Result<Option<Value>> {
        match self.check_inner(db_schema).await {
            Ok(None) => {
                self.repository.record_check(None, None).await?;
                Ok(None)
            }
            Ok(Some(value)) => {
                let json = checked_json(&value);
                self.repository.record_check(Some(&json), None).await?;
                Ok(Some(json))
            }
            Err(error) => {
                let safe = pons_updater::redact_secret(&error.to_string(), self.token.as_deref());
                self.repository.record_check(None, Some(&safe)).await?;
                Err(anyhow::anyhow!(safe))
            }
        }
    }
    pub async fn check_for_install(
        &self,
        db_schema: i64,
    ) -> anyhow::Result<Option<CheckedRelease>> {
        self.check_inner(db_schema).await
    }

    async fn check_inner(&self, db_schema: i64) -> anyhow::Result<Option<CheckedRelease>> {
        if !self.config.enabled {
            anyhow::bail!("updater is disabled");
        }
        let url = format!(
            "https://api.github.com/repos/{}/{}/releases?per_page=30",
            self.config.github_owner, self.config.github_repo
        );
        let mut request = self
            .client
            .get(url)
            .header("Accept", "application/vnd.github+json");
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let releases: Vec<GithubRelease> = request.send().await?.error_for_status()?.json().await?;
        let Some(release) = latest_stable(releases, &Version::parse(APP_VERSION)?)? else {
            return Ok(None);
        };
        let manifest_asset = release
            .assets
            .iter()
            .find(|a| a.name == "release-manifest.json")
            .context("release-manifest.json missing")?;
        let signature_asset = release
            .assets
            .iter()
            .find(|a| a.name == "release-manifest.sig")
            .context("release-manifest.sig missing")?;
        let raw = self.small_asset(manifest_asset).await?;
        let signature = String::from_utf8(self.small_asset(signature_asset).await?)
            .context("signature is not UTF-8 base64")?;
        let verified = verify_manifest(&raw, Some(&signature), &self.keys)?;
        if verified.manifest.app_version
            != Version::parse(release.tag_name.trim_start_matches('v'))?
        {
            anyhow::bail!("release tag and signed manifest version differ");
        }
        check_compatibility(&verified.manifest, &Version::parse(APP_VERSION)?, db_schema)?;
        let asset = select_asset(
            &verified.manifest,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )?;
        if asset.size > self.config.max_asset_bytes {
            anyhow::bail!("signed asset exceeds configured maximum size");
        }
        let github_asset = release
            .assets
            .iter()
            .find(|a| a.name == asset.filename)
            .context("signed platform asset missing from release")?;
        if github_asset.size != asset.size {
            anyhow::bail!("GitHub asset size differs from signed manifest");
        }
        Ok(Some(CheckedRelease {
            asset_id: github_asset.id,
            release,
            verified,
        }))
    }

    async fn small_asset(&self, asset: &GithubAsset) -> anyhow::Result<Vec<u8>> {
        if usize::try_from(asset.size).map_or(true, |size| size > self.config.max_manifest_bytes) {
            anyhow::bail!("manifest/signature exceeds size limit");
        }
        let expected_prefix = format!(
            "https://github.com/{}/{}/",
            self.config.github_owner, self.config.github_repo
        );
        if !asset.browser_download_url.starts_with(&expected_prefix) {
            anyhow::bail!("release metadata contains an untrusted asset URL");
        }
        let mut request = self.client.get(&asset.browser_download_url);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await?.error_for_status()?;
        let bytes = response.bytes().await?;
        if bytes.len() > self.config.max_manifest_bytes {
            anyhow::bail!("download exceeded size limit");
        }
        Ok(bytes.to_vec())
    }

    pub async fn create_install_job(
        &self,
        user: Uuid,
        checked: &CheckedRelease,
    ) -> anyhow::Result<Uuid> {
        let asset = select_asset(
            &checked.verified.manifest,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )?;
        let manifest = serde_json::to_value(&checked.verified.manifest)?;
        let job = self
            .repository
            .create_install(&NewUpdateJob {
                current_version: APP_VERSION,
                target_version: &checked.verified.manifest.app_version.to_string(),
                release_id: i64::try_from(checked.release.id)?,
                release_tag: &checked.release.tag_name,
                channel: &checked.verified.manifest.channel,
                manifest: &manifest,
                manifest_sha256: &checked.verified.sha256,
                asset_filename: &asset.filename,
                asset_sha256: &asset.sha256,
                signature_key_id: &checked.verified.manifest.signing_key_id,
                admin_user_id: user,
            })
            .await?;
        Ok(job.id)
    }

    pub async fn stage_and_handoff(
        &self,
        user: Uuid,
        checked: &CheckedRelease,
    ) -> anyhow::Result<Uuid> {
        let id = self.create_install_job(user, checked).await?;
        let result = self.stage_and_handoff_inner(id, checked).await;
        if let Err(error) = &result {
            let safe = pons_updater::redact_secret(&error.to_string(), self.token.as_deref());
            let _ = self.repository.fail(id, &safe).await;
        }
        result.map(|()| id)
    }

    async fn stage_and_handoff_inner(
        &self,
        id: Uuid,
        checked: &CheckedRelease,
    ) -> anyhow::Result<()> {
        let asset = select_asset(
            &checked.verified.manifest,
            std::env::consts::OS,
            std::env::consts::ARCH,
        )?;
        let root = self
            .staging_root()
            .join(checked.verified.manifest.app_version.to_string());
        let extracted = root.join("extracted");
        tokio::fs::create_dir_all(&root).await?;
        let archive = root.join(&asset.filename);
        let url = trusted_asset_url(
            &self.config.github_owner,
            &self.config.github_repo,
            checked.release.id,
            checked.asset_id,
        )?;
        download_verified(
            &self.client,
            url,
            &archive,
            asset.size,
            &asset.sha256,
            self.token.as_deref(),
        )
        .await?;
        self.repository
            .transition(id, &["DOWNLOADING"], "VERIFYING", None)
            .await?;
        extract_safe(&archive, &extracted)?;
        let packaged_version = tokio::fs::read_to_string(extracted.join("VERSION")).await?;
        if packaged_version.trim() != checked.verified.manifest.app_version.to_string() {
            anyhow::bail!("package VERSION differs from signed manifest")
        }
        let output = tokio::process::Command::new(extracted.join("pons-radar"))
            .arg("--version-json")
            .output()
            .await?;
        if !output.status.success() {
            anyhow::bail!("staged binary sanity check failed")
        }
        let sanity: Value = serde_json::from_slice(&output.stdout)?;
        if sanity["app_version"] != checked.verified.manifest.app_version.to_string()
            || sanity["frontend_build_id"] != checked.verified.manifest.frontend_build_id
            || sanity["api_schema_version"] != checked.verified.manifest.api_schema_version
        {
            anyhow::bail!("staged binary identity differs from signed manifest")
        }
        let backup = self
            .data_dir
            .join("update")
            .join("backups")
            .join(APP_VERSION);
        tokio::fs::create_dir_all(&backup).await?;
        self.repository
            .set_paths(id, &root.to_string_lossy(), &backup.to_string_lossy())
            .await?;
        let plan = HandoffPlan {
            job_id: id.to_string(),
            parent_pid: std::process::id(),
            current_binary: std::env::current_exe()?,
            staged_binary: extracted.join("pons-radar"),
            backup_binary: backup.join("pons-radar"),
            service_name: self.config.service_name.clone(),
            health_base_url: self.config.health_base_url.clone(),
            target_version: checked.verified.manifest.app_version.to_string(),
            previous_version: APP_VERSION.into(),
            frontend_build_id: checked.verified.manifest.frontend_build_id.clone(),
            api_schema_version: checked.verified.manifest.api_schema_version,
            timeout_seconds: 60,
        };
        let plan_path = root.join("handoff.json");
        tokio::fs::write(&plan_path, serde_json::to_vec_pretty(&plan)?).await?;
        if !self
            .repository
            .mark_installing(
                id,
                APP_VERSION,
                &checked.verified.manifest.app_version.to_string(),
                &checked.verified.manifest.frontend_build_id,
            )
            .await?
        {
            anyhow::bail!("update job was not staged")
        }
        let mut command = tokio::process::Command::new(extracted.join("pons-radar-updater"));
        command.arg(&plan_path);
        if let Some(token) = &self.token {
            command.env("GITHUB_TOKEN", token);
        }
        command.spawn().context("failed to launch updater helper")?;
        self.shutdown.cancel();
        Ok(())
    }

    #[must_use]
    pub fn staging_root(&self) -> PathBuf {
        self.data_dir.join("update").join("staging")
    }
    pub async fn recover_after_start(&self) -> anyhow::Result<()> {
        tokio::time::sleep(Duration::from_secs(2)).await;
        for job in self.repository.recoverable().await? {
            if job.target_version != APP_VERSION {
                continue;
            }
            let manifest = &job.manifest;
            if manifest["frontend_build_id"] != crate::version::FRONTEND_BUILD_ID
                || manifest["api_schema_version"] != crate::version::API_SCHEMA_VERSION
            {
                continue;
            }
            let health = self
                .client
                .get(format!("{}/healthz", self.config.health_base_url))
                .send()
                .await;
            let ready = self
                .client
                .get(format!("{}/readyz", self.config.health_base_url))
                .send()
                .await;
            if health.is_ok_and(|r| r.status().is_success())
                && ready.is_ok_and(|r| r.status().is_success())
            {
                self.repository
                    .complete_recovered(
                        &job,
                        crate::version::FRONTEND_BUILD_ID,
                        i32::try_from(crate::version::API_SCHEMA_VERSION)?,
                    )
                    .await?;
            }
        }
        Ok(())
    }
}

pub fn latest_stable(
    releases: Vec<GithubRelease>,
    current: &Version,
) -> anyhow::Result<Option<GithubRelease>> {
    let mut candidates = Vec::new();
    for release in releases.into_iter().filter(|r| !r.draft && !r.prerelease) {
        let version = Version::parse(release.tag_name.trim_start_matches('v'))
            .with_context(|| format!("invalid stable release semver {}", release.tag_name))?;
        if version > *current {
            candidates.push((version, release));
        }
    }
    candidates.sort_by(|a, b| b.0.cmp(&a.0));
    Ok(candidates.into_iter().next().map(|v| v.1))
}

fn checked_json(value: &CheckedRelease) -> Value {
    let asset = select_asset(
        &value.verified.manifest,
        std::env::consts::OS,
        std::env::consts::ARCH,
    )
    .ok();
    json!({"release_id":value.release.id,"tag":value.release.tag_name,"published_at":value.release.published_at,"release_reference":value.release.html_url,"manifest":value.verified.manifest,"manifest_sha256":value.verified.sha256,"signature":"VALID","asset_id":value.asset_id,"asset_sha256":asset.map(|a|&a.sha256),"schema_compatible":true,"rollback_compatible":true,"install_allowed":true})
}

#[cfg(test)]
mod tests {
    use super::*;
    fn release(id: u64, tag: &str, draft: bool, prerelease: bool) -> GithubRelease {
        GithubRelease {
            id,
            tag_name: tag.into(),
            draft,
            prerelease,
            published_at: None,
            html_url: "https://github.com/o/r/releases".into(),
            assets: vec![],
        }
    }
    #[test]
    fn stable_discovery_is_semver_ordered_and_ignores_drafts_and_prereleases() {
        let found = latest_stable(
            vec![
                release(1, "v1.9.0", false, false),
                release(2, "v1.10.0", false, false),
                release(3, "v9.0.0", true, false),
                release(4, "v8.0.0", false, true),
            ],
            &Version::new(1, 0, 0),
        )
        .unwrap()
        .unwrap();
        assert_eq!(found.id, 2);
        assert!(
            latest_stable(
                vec![release(1, "v1.0.0", false, false)],
                &Version::new(1, 0, 0)
            )
            .unwrap()
            .is_none()
        );
    }
    #[test]
    fn invalid_stable_semver_fails_closed() {
        assert!(
            latest_stable(
                vec![release(1, "latest", false, false)],
                &Version::new(1, 0, 0)
            )
            .is_err()
        );
    }
}
