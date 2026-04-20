//! Agent memory provider hook — OSS trait contract.
//!
//! Defines the abstract interface for persistent session memory.  The `roam-public`
//! crate ships only the trait and a no-op implementation; durable storage backed
//! by Dolt branches lives in the enterprise backend.

use async_trait::async_trait;

// ── Shared data types ─────────────────────────────────────────────────────────

/// A single recalled memory entry for a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryEntry {
    /// Discriminator for the entry kind (e.g. `"tool_call"`, `"annotation"`).
    pub entry_type: String,
    /// Serialised content — arbitrary JSON string in practice.
    pub content: String,
    /// RFC-3339 creation timestamp.
    pub created_at: String,
}

// ── AgentMemoryProvider trait ─────────────────────────────────────────────────

/// Hook for persistent agent session memory.
///
/// Implementations are responsible for storing and retrieving per-session
/// context so that multi-turn conversations can maintain continuity.
///
/// The default OSS implementation ([`NoOpMemoryProvider`]) is intentionally
/// stateless; all durable storage is an enterprise-only concern.
#[async_trait]
pub trait AgentMemoryProvider: Send + Sync {
    /// Called when a new agent session is first registered.  Implementations may
    /// use this to allocate session-scoped resources (e.g. a Dolt branch).
    async fn on_session_registered(&self, session_id: &str);

    /// Record a tool call event for the given session.
    async fn on_tool_call(&self, session_id: &str, tool: &str, args: &str, result: &str);

    /// Retrieve all memory entries for a session to include as agent context.
    async fn get_context(&self, session_id: &str) -> Vec<MemoryEntry>;
}

// ── NoOpMemoryProvider — OSS default ─────────────────────────────────────────

/// No-op implementation of [`AgentMemoryProvider`].
///
/// This is the default used when no enterprise memory store is configured:
/// sessions are ephemeral and context is never persisted across restarts.
pub struct NoOpMemoryProvider;

#[async_trait]
impl AgentMemoryProvider for NoOpMemoryProvider {
    async fn on_session_registered(&self, _session_id: &str) {}

    async fn on_tool_call(&self, _session_id: &str, _tool: &str, _args: &str, _result: &str) {}

    async fn get_context(&self, _session_id: &str) -> Vec<MemoryEntry> {
        vec![]
    }
}
