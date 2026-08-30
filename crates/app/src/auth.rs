use std::sync::Arc;

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use axum::http::{HeaderMap, header};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use pons_storage::repositories::{AuthRepository, AuthenticatedSession};
use rand::RngCore;
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

pub const SESSION_COOKIE: &str = "pons_session";
pub const CSRF_COOKIE: &str = "pons_csrf";

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub secure_cookie: bool,
    pub session_hours: i64,
    pub allowed_origin: String,
    pub setup_token: Option<String>,
}
#[derive(Clone)]
pub struct AuthService {
    repository: AuthRepository,
    config: Arc<AuthConfig>,
}
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("setup unavailable")]
    SetupUnavailable,
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("storage error: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("password hashing failed")]
    Password,
}

#[allow(clippy::missing_errors_doc)]
impl AuthService {
    #[must_use]
    pub fn new(repository: AuthRepository, config: AuthConfig) -> Self {
        Self {
            repository,
            config: Arc::new(config),
        }
    }
    #[must_use]
    pub const fn repository(&self) -> &AuthRepository {
        &self.repository
    }
    pub async fn setup_required(&self) -> Result<bool, AuthError> {
        Ok(!self.repository.has_users().await?)
    }
    pub async fn setup(
        &self,
        headers: &HeaderMap,
        username: String,
        password: String,
    ) -> Result<(), AuthError> {
        self.require_origin(headers)?;
        let supplied = headers
            .get("x-setup-token")
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthError::SetupUnavailable)?;
        let expected = self
            .config
            .setup_token
            .as_deref()
            .ok_or(AuthError::SetupUnavailable)?;
        if supplied.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() != 1 {
            return Err(AuthError::SetupUnavailable);
        }
        validate_credentials(&username, &password)?;
        let hash = hash_password(password).await?;
        let user = self
            .repository
            .create_first_admin(&username, &hash)
            .await?
            .ok_or(AuthError::SetupUnavailable)?;
        self.repository
            .audit(
                Some(user.id),
                "auth.setup",
                "user",
                Some(&user.id.to_string()),
                &json!({"username":user.username}),
            )
            .await?;
        Ok(())
    }
    pub async fn login(
        &self,
        headers: &HeaderMap,
        username: String,
        password: String,
    ) -> Result<(AuthenticatedSession, String, String), AuthError> {
        self.require_origin(headers)?;
        let user = self
            .repository
            .by_username(&username)
            .await?
            .ok_or(AuthError::Unauthorized)?;
        if !user.enabled || !verify_password(user.password_hash.clone(), password).await? {
            return Err(AuthError::Unauthorized);
        }
        let token = random_token();
        let csrf = random_token();
        self.repository
            .create_session(
                user.id,
                &digest(&token),
                &digest(&csrf),
                self.config.session_hours,
            )
            .await?;
        self.repository
            .audit(Some(user.id), "auth.login", "session", None, &json!({}))
            .await?;
        let session = self
            .repository
            .session(&digest(&token))
            .await?
            .ok_or(AuthError::Unauthorized)?;
        Ok((session, token, csrf))
    }
    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedSession, AuthError> {
        let token = cookie(headers, SESSION_COOKIE).ok_or(AuthError::Unauthorized)?;
        self.repository
            .session(&digest(&token))
            .await?
            .ok_or(AuthError::Unauthorized)
    }
    pub async fn authenticate_websocket(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedSession, AuthError> {
        self.require_origin(headers)?;
        self.authenticate(headers).await
    }
    pub async fn require_mutation(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedSession, AuthError> {
        self.require_origin(headers)?;
        let session = self.authenticate(headers).await?;
        let csrf_header = headers
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthError::Forbidden)?;
        let csrf_cookie = cookie(headers, CSRF_COOKIE).ok_or(AuthError::Forbidden)?;
        if csrf_header
            .as_bytes()
            .ct_eq(csrf_cookie.as_bytes())
            .unwrap_u8()
            != 1
            || digest(csrf_header)
                .as_slice()
                .ct_eq(&session.csrf_hash)
                .unwrap_u8()
                != 1
        {
            return Err(AuthError::Forbidden);
        }
        Ok(session)
    }
    pub async fn logout(&self, headers: &HeaderMap) -> Result<AuthenticatedSession, AuthError> {
        let session = self.require_mutation(headers).await?;
        let token = cookie(headers, SESSION_COOKIE).ok_or(AuthError::Unauthorized)?;
        self.repository.revoke(&digest(&token)).await?;
        self.repository
            .audit(
                Some(session.user_id),
                "auth.logout",
                "session",
                Some(&session.session_id.to_string()),
                &json!({}),
            )
            .await?;
        Ok(session)
    }
    #[must_use]
    pub fn session_cookie(&self, token: &str) -> String {
        format!(
            "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age={};{}",
            self.config.session_hours * 3600,
            if self.config.secure_cookie {
                " Secure"
            } else {
                ""
            }
        )
    }
    #[must_use]
    pub fn csrf_cookie(&self, token: &str) -> String {
        format!(
            "{CSRF_COOKIE}={token}; Path=/; SameSite=Strict; Max-Age={};{}",
            self.config.session_hours * 3600,
            if self.config.secure_cookie {
                " Secure"
            } else {
                ""
            }
        )
    }
    #[must_use]
    pub fn clear_cookies(&self) -> [String; 2] {
        [
            format!(
                "{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0;{}",
                if self.config.secure_cookie {
                    " Secure"
                } else {
                    ""
                }
            ),
            format!(
                "{CSRF_COOKIE}=; Path=/; SameSite=Strict; Max-Age=0;{}",
                if self.config.secure_cookie {
                    " Secure"
                } else {
                    ""
                }
            ),
        ]
    }
    fn require_origin(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let origin = headers
            .get(header::ORIGIN)
            .and_then(|v| v.to_str().ok())
            .ok_or(AuthError::Forbidden)?;
        if origin != self.config.allowed_origin {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }
}
fn validate_credentials(username: &str, password: &str) -> Result<(), AuthError> {
    if !(3..=64).contains(&username.len())
        || !username
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(AuthError::Invalid(
            "username must be 3-64 ASCII letters, digits, _ or -".into(),
        ));
    }
    if !(12..=1024).contains(&password.len()) {
        return Err(AuthError::Invalid("password must be 12-1024 bytes".into()));
    }
    Ok(())
}
async fn hash_password(password: String) -> Result<String, AuthError> {
    tokio::task::spawn_blocking(move || {
        Argon2::default()
            .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
            .map(|v| v.to_string())
            .map_err(|_| AuthError::Password)
    })
    .await
    .map_err(|_| AuthError::Password)?
}
async fn verify_password(hash: String, password: String) -> Result<bool, AuthError> {
    tokio::task::spawn_blocking(move || {
        Ok(Argon2::default()
            .verify_password(
                password.as_bytes(),
                &PasswordHash::new(&hash).map_err(|_| AuthError::Password)?,
            )
            .is_ok())
    })
    .await
    .map_err(|_| AuthError::Password)?
}
fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}
fn digest(value: &str) -> Vec<u8> {
    Sha256::digest(value.as_bytes()).to_vec()
}
fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|part| {
            let (key, value) = part.trim().split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
}
