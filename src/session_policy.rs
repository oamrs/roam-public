//! Session retention policy — OSS trait contract.
//!
//! Agent sessions are created when a client registers with the server.  Without
//! a retention policy, sessions accumulate indefinitely in the `agent_sessions`
//! table and the `memory_store`.  Multi-tenant enterprise operators need
//! per-organisation TTLs so that stale sessions are automatically reaped and
//! storage does not grow without bound.
//!
//! The OSS default ([`NoOpSessionPolicy`]) returns an unbounded retention config
//! and performs no cleanup.  Enterprise deployments replace it with a
//! `DoltSessionRetentionPolicy` backed by a `session_retention_policies` table.

use async_trait::async_trait;

// ── SessionRetentionConfig ────────────────────────────────────────────────────

/// Retention settings for sessions belonging to a single organisation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRetentionConfig {
    /// Maximum session lifetime in seconds, measured from `last_seen_at`.
    ///
    /// `None` means sessions are kept forever (OSS default).
    pub ttl_seconds: Option<u64>,
}

impl SessionRetentionConfig {
    /// Sessions live forever — the OSS / permissive default.
    pub fn unbounded() -> Self {
        Self { ttl_seconds: None }
    }
}

// ── AgentSessionPolicy trait ──────────────────────────────────────────────────

/// Hook for managing the lifecycle of agent sessions.
///
/// Implementations are expected to be cheap to call; providers should cache
/// retention configs rather than hitting the database on every request.
#[async_trait]
pub trait AgentSessionPolicy: Send + Sync {
    /// Return the retention configuration for the given organisation.
    async fn get_retention(&self, org_id: &str) -> SessionRetentionConfig;

    /// Delete all sessions that have exceeded their configured TTL.
    ///
    /// Returns the number of sessions removed, or an error description.
    /// Implementations should query each org's retention config and prune the
    /// `agent_sessions` table accordingly.
    async fn cleanup_expired(&self) -> Result<u64, String>;
}

// ── NoOpSessionPolicy — OSS default ──────────────────────────────────────────

/// Permissive policy that never expires sessions and performs no cleanup.
///
/// Used in the OSS runtime; enterprise deployments replace this with a
/// database-backed implementation.
pub struct NoOpSessionPolicy;

#[async_trait]
impl AgentSessionPolicy for NoOpSessionPolicy {
    async fn get_retention(&self, _org_id: &str) -> SessionRetentionConfig {
        SessionRetentionConfig::unbounded()
    }

    async fn cleanup_expired(&self) -> Result<u64, String> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_policy_returns_unbounded() {
        let policy = NoOpSessionPolicy;
        let config = policy.get_retention("acme").await;
        assert_eq!(config, SessionRetentionConfig::unbounded());
        assert!(config.ttl_seconds.is_none());
    }

    #[tokio::test]
    async fn noop_cleanup_is_no_op() {
        let policy = NoOpSessionPolicy;
        let deleted = policy
            .cleanup_expired()
            .await
            .expect("cleanup should succeed");
        assert_eq!(deleted, 0);
    }
}
