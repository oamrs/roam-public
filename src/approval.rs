//! Human-in-the-loop approval gate — OSS trait contract.
//!
//! Defines the abstract interface for pausing high-risk agent actions and
//! requesting human approval before execution commits.  The OSS default
//! ([`NoOpApprovalGate`]) always approves; enterprise implementations wire up
//! a persistent queue (e.g. Dolt-backed approval table, Slack notification,
//! or a webhook) and block until a reviewer responds.

use async_trait::async_trait;

// ── PendingAction ─────────────────────────────────────────────────────────────

/// A high-risk agent action awaiting human review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAction {
    /// Session that originated the action.
    pub session_id: String,
    /// Human-readable description of the action (e.g. the SQL that would execute).
    pub description: String,
    /// The risk tier that triggered escalation (e.g. `"mass-write"`, `"ddl"`).
    pub risk_tier: String,
    /// Submitting user identifier.
    pub requested_by: String,
}

// ── ApprovalDecision ──────────────────────────────────────────────────────────

/// Outcome of a human-in-the-loop review.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalDecision {
    /// The action is permitted to proceed.
    Approved,
    /// A reviewer explicitly rejected the action.
    Rejected {
        /// Free-text reason supplied by the reviewer.
        reason: String,
    },
    /// The gate timed out waiting for a reviewer.
    TimedOut,
}

// ── ApprovalGate trait ────────────────────────────────────────────────────────

/// Hook for human-in-the-loop approval of high-risk agent actions.
///
/// The executor calls `request_approval` before committing any action that
/// exceeds the configured risk threshold.  Implementations may be:
///
/// - **Synchronous-looking async** (wait until a reviewer responds via a
///   database row update or an HTTP webhook).
/// - **Time-bounded** (return [`ApprovalDecision::TimedOut`] after a deadline).
/// - **Permissive** (always approve — the default [`NoOpApprovalGate`]).
#[async_trait]
pub trait ApprovalGate: Send + Sync {
    /// Request approval for `action`.  The implementation may block until a
    /// reviewer responds or a timeout elapses.
    async fn request_approval(&self, action: &PendingAction) -> ApprovalDecision;
}

// ── NoOpApprovalGate — OSS default ───────────────────────────────────────────

/// Always-approve gate used in the OSS runtime.
///
/// This preserves backward compatibility: deployments that do not configure an
/// enterprise gate behave exactly as before, with no approval latency.
pub struct NoOpApprovalGate;

#[async_trait]
impl ApprovalGate for NoOpApprovalGate {
    async fn request_approval(&self, _action: &PendingAction) -> ApprovalDecision {
        ApprovalDecision::Approved
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_gate_always_approves() {
        let gate = NoOpApprovalGate;
        let action = PendingAction {
            session_id: "sess-1".into(),
            description: "DELETE FROM users WHERE verified = 0".into(),
            risk_tier: "mass-write".into(),
            requested_by: "agent@example.com".into(),
        };
        assert_eq!(
            gate.request_approval(&action).await,
            ApprovalDecision::Approved
        );
    }
}
