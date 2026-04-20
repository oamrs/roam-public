//! Per-organisation rate-limit quota — OSS trait contract.
//!
//! The global [`RateLimiter`] enforces flat connection and request-rate caps
//! for every caller.  This module adds an *organisation-aware* layer on top:
//! each `(org_id, role)` pair can be granted a larger or smaller budget than
//! the server default, enabling tiered plans (Free / Growth / Enterprise) where
//! higher-paying orgs get headroom that is denied on lower tiers.
//!
//! The OSS default ([`NoOpOrgRateLimitProvider`]) returns the same permissive
//! budget for every org; enterprise deployments swap in a database-backed
//! provider that reads per-org configuration.

use async_trait::async_trait;

// ── OrgRateLimit ──────────────────────────────────────────────────────────────

/// Per-organisation request budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgRateLimit {
    /// Maximum queries allowed per second from this org.
    pub requests_per_second: u32,
    /// Maximum number of concurrent gRPC connections from this org.
    pub max_concurrent_connections: u32,
    /// Whether the org is currently suspended (all requests denied).
    pub suspended: bool,
}

impl OrgRateLimit {
    /// Permissive defaults used by the OSS runtime.
    pub fn permissive() -> Self {
        Self {
            requests_per_second: 10_000,
            max_concurrent_connections: 100,
            suspended: false,
        }
    }
}

impl Default for OrgRateLimit {
    fn default() -> Self {
        Self::permissive()
    }
}

// ── OrgRateLimitProvider trait ────────────────────────────────────────────────

/// Hook for resolving the request-rate budget for a given organisation and role.
///
/// Called once per incoming connection before the global [`RateLimiter`] runs.
/// If the returned limit is lower than the global cap, the tighter limit wins.
///
/// # Tiering
///
/// Enterprise implementations back this with a persistent configuration store
/// so operators can adjust quotas per org without redeploying.  The OSS runtime
/// uses [`NoOpOrgRateLimitProvider`], which always returns a permissive limit.
#[async_trait]
pub trait OrgRateLimitProvider: Send + Sync {
    /// Return the quota for `org_id` acting under `role`.
    async fn get_limit(&self, org_id: &str, role: &str) -> OrgRateLimit;
}

// ── NoOpOrgRateLimitProvider — OSS default ────────────────────────────────────

/// Always-permissive provider used in the OSS runtime.
///
/// Every org receives the maximum budget regardless of plan tier.
/// Enterprise deployments replace this with a database-backed implementation.
pub struct NoOpOrgRateLimitProvider;

#[async_trait]
impl OrgRateLimitProvider for NoOpOrgRateLimitProvider {
    async fn get_limit(&self, _org_id: &str, _role: &str) -> OrgRateLimit {
        OrgRateLimit::permissive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_provider_returns_permissive_limit() {
        let provider = NoOpOrgRateLimitProvider;
        let limit = provider.get_limit("acme", "viewer").await;
        assert!(!limit.suspended);
        assert!(limit.requests_per_second >= 1_000);
    }
}
