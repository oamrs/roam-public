//! Identity and organisation-sync provider hook — OSS trait contracts.
//!
//! Defines the abstract interfaces for federated authentication and team/org
//! synchronisation.  Concrete implementations (OIDC/OAuth2 via GitHub/GitLab,
//! LDAP) live in the enterprise backend and are never published in the `oam`
//! crate.

use async_trait::async_trait;

// ── Shared types ──────────────────────────────────────────────────────────────

/// A user identity resolved from a federated identity provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserIdentity {
    /// Unique identifier from the IdP (e.g. GitHub user ID as a string).
    pub id: String,
    /// Primary email address.
    pub email: String,
    /// Display name.
    pub name: String,
    /// Raw token scopes / groups as reported by the IdP.
    pub raw_roles: Vec<String>,
    /// Organisation slugs the user belongs to (resolved by the provider).
    pub organizations: Vec<String>,
}

/// Error returned by identity provider operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// No credential was supplied.
    MissingToken,
    /// The supplied token was rejected by the IdP.
    InvalidToken,
    /// The IdP is unreachable or returned an unexpected response.
    ProviderError(String),
    /// The user authenticated successfully but is not authorised for this resource.
    Unauthorized,
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentityError::MissingToken => write!(f, "Missing authentication token"),
            IdentityError::InvalidToken => write!(f, "Invalid authentication token"),
            IdentityError::ProviderError(msg) => write!(f, "Identity provider error: {}", msg),
            IdentityError::Unauthorized => write!(f, "Unauthorized"),
        }
    }
}

// ── IdentityProvider trait ────────────────────────────────────────────────────

/// Hook for federated authentication.
///
/// Accepts a raw bearer token (OAuth2 access token or OIDC ID token) and
/// resolves it to a [`UserIdentity`].  Enterprise implementations support
/// GitHub OAuth2, GitLab OIDC, and LDAP bind-based authentication.
#[async_trait]
pub trait IdentityProvider: Send + Sync {
    /// Authenticate a raw bearer token and return the resolved user identity.
    async fn authenticate(&self, token: &str) -> Result<UserIdentity, IdentityError>;
}

// ── OrgSync types ─────────────────────────────────────────────────────────────

/// Configuration for an organisation sync operation.
#[derive(Debug, Clone)]
pub struct OrgSyncConfig {
    /// Bearer token or bind credential for the upstream system.
    pub credential: String,
    /// Optional filter — only sync orgs matching this slug prefix.
    pub filter_prefix: Option<String>,
}

/// Summary of a completed organisation sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgSyncReport {
    /// Number of organisations created or updated.
    pub orgs_synced: usize,
    /// Number of user-org memberships written.
    pub memberships_synced: usize,
    /// Non-fatal warnings accumulated during the run.
    pub warnings: Vec<String>,
}

/// Error returned by org sync operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrgSyncError {
    /// Authentication with the upstream system failed.
    AuthFailed(String),
    /// The upstream system returned an error.
    ProviderError(String),
    /// A database write failed.
    StorageError(String),
}

impl std::fmt::Display for OrgSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrgSyncError::AuthFailed(msg) => write!(f, "Auth failed: {}", msg),
            OrgSyncError::ProviderError(msg) => write!(f, "Provider error: {}", msg),
            OrgSyncError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

// ── OrgSyncProvider trait ─────────────────────────────────────────────────────

/// Hook for synchronising external organisation and team structures into ROAM.
///
/// Enterprise implementations cover GitHub Teams, GitLab Groups, and LDAP
/// organisational units.  All three live exclusively in `services/backend`.
#[async_trait]
pub trait OrgSyncProvider: Send + Sync {
    /// Perform a full sync from the upstream system using `config`.
    async fn sync(&self, config: &OrgSyncConfig) -> Result<OrgSyncReport, OrgSyncError>;
}
