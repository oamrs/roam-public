//! Schema-mode access policy — OSS trait contract.
//!
//! [`SchemaMode`] controls how broadly an agent can explore and mutate data:
//! `DATA_FIRST` (read-only introspection), `CODE_FIRST` (read-write registered
//! models), and `HYBRID` (registered models + read-only fallback).
//!
//! Without a policy layer, any caller can request any mode.  For multi-tenant
//! enterprise deployments operators need to restrict which modes are available
//! per organisation or per role — e.g. free-tier orgs get `DATA_FIRST` only,
//! while paying customers unlock `CODE_FIRST` and `HYBRID`.
//!
//! The OSS default ([`NoOpSchemaModePolicy`]) permits every mode for every org.
//! Enterprise deployments replace it with a `DoltSchemaModePolicy` backed by a
//! `schema_mode_policies` table.

use async_trait::async_trait;

// ── SchemaModePolicyDecision ──────────────────────────────────────────────────

/// Decision returned by [`SchemaModePolicy::is_allowed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaModePolicyDecision {
    /// The requested mode is permitted for this org / role.
    Allowed,
    /// The requested mode is denied; `reason` explains what is permitted.
    Denied { reason: String },
}

// ── SchemaModePolicy trait ────────────────────────────────────────────────────

/// Hook for enforcing which schema modes an organisation may request.
///
/// Called during agent registration, before the agent session is created.
/// If the implementation returns [`SchemaModePolicyDecision::Denied`] the
/// registration request is rejected with a `403` and the agent never connects.
///
/// # Mode names
///
/// Modes are passed as the string values used in the proto enum:
/// `"DATA_FIRST"`, `"CODE_FIRST"`, `"HYBRID"`.
#[async_trait]
pub trait SchemaModePolicy: Send + Sync {
    /// Return whether `org_id` / `role` may register with `requested_mode`.
    async fn is_allowed(
        &self,
        org_id: &str,
        role: &str,
        requested_mode: &str,
    ) -> SchemaModePolicyDecision;
}

// ── NoOpSchemaModePolicy — OSS default ───────────────────────────────────────

/// Always-allow policy used in the OSS runtime.
///
/// All three schema modes are available to every org regardless of role.
/// Enterprise deployments replace this with a database-backed implementation.
pub struct NoOpSchemaModePolicy;

#[async_trait]
impl SchemaModePolicy for NoOpSchemaModePolicy {
    async fn is_allowed(
        &self,
        _org_id: &str,
        _role: &str,
        _requested_mode: &str,
    ) -> SchemaModePolicyDecision {
        SchemaModePolicyDecision::Allowed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_policy_allows_all_modes() {
        let policy = NoOpSchemaModePolicy;
        for mode in &["DATA_FIRST", "CODE_FIRST", "HYBRID"] {
            assert_eq!(
                policy.is_allowed("acme", "viewer", mode).await,
                SchemaModePolicyDecision::Allowed,
            );
        }
    }
}
