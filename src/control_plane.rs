//! Control plane — OSS trait contract.
//!
//! Defines the types and trait for managing multi-step LLM tool workflows.
//! An LLM agent submits a [`PlanDefinition`] (a DAG of named [`StepDefinition`]
//! entries), then calls [`WorkflowOrchestrator::execute_step`] for each step in
//! dependency order.  Each execution returns a [`LlmContextUpdate`] that the
//! calling SDK feeds back into the next LLM API request, closing the feedback
//! loop: the LLM always sees the latest schema and prior step output before
//! choosing its next action.
//!
//! `roam-public` ships only the trait and a [`NoOpWorkflowOrchestrator`] that
//! always errors; durable, multi-tenant storage lives in the enterprise backend.

use crate::policy_engine::ToolIntent;
use crate::runtime_context::QueryRuntimeContext;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ── Status enums ─────────────────────────────────────────────────────────────

/// Overall lifecycle state of a plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl PlanStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            PlanStatus::Pending => "pending",
            PlanStatus::Running => "running",
            PlanStatus::Completed => "completed",
            PlanStatus::Failed => "failed",
            PlanStatus::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for PlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle state of a single step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Running,
    Completed,
    Failed,
    /// The step was not attempted because an earlier dependency failed or the
    /// plan was cancelled.
    Skipped,
}

impl StepStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            StepStatus::Pending => "pending",
            StepStatus::Running => "running",
            StepStatus::Completed => "completed",
            StepStatus::Failed => "failed",
            StepStatus::Skipped => "skipped",
        }
    }
}

impl std::fmt::Display for StepStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a table-level schema change was additive, structural, or destructive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DeltaOperation {
    New,
    Modified,
    Removed,
}

// ── Plan definition ───────────────────────────────────────────────────────────

/// A single step in a multi-step plan.
///
/// `query_template` may contain `{{step.<dep_id>.output}}` placeholders; the
/// orchestrator substitutes these with the JSON-serialised result of the named
/// prior step before passing the query to `QueryService`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepDefinition {
    /// Stable identifier for this step within the plan (used in `depends_on`).
    pub id: String,
    /// Human-readable label shown in status responses and events.
    pub name: String,
    /// The logical tool name (maps to a `ToolContract` on the server).
    pub tool_name: String,
    /// Declares what the step is allowed to do; drives policy enforcement.
    pub tool_intent: ToolIntent,
    /// SQL template, optionally containing `{{step.<id>.output}}` references.
    pub query_template: String,
    /// `id` values of steps that must reach `Completed` before this step runs.
    pub depends_on: Vec<String>,
    /// Tables whose schema should be snapshotted before/after this step to
    /// produce [`SchemaTableDelta`] entries in [`LlmContextUpdate`].
    pub schema_table_hints: Vec<String>,
}

/// A complete multi-step plan submitted by an LLM agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDefinition {
    pub name: String,
    pub description: String,
    pub steps: Vec<StepDefinition>,
}

// ── Execution results ─────────────────────────────────────────────────────────

/// The result of executing a single step.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub step_id: String,
    pub status: StepStatus,
    pub row_count: i64,
    /// JSON-serialised query result rows.
    pub output_json: String,
    /// JSON-serialised `Vec<SchemaTableDelta>` produced by this step.
    pub schema_delta_json: String,
    /// RFC-3339 timestamp of when the step completed (or failed).
    pub executed_at: String,
}

/// A persistent record of a plan together with all accumulated step results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRecord {
    pub plan_id: String,
    pub session_id: String,
    pub definition: PlanDefinition,
    pub status: PlanStatus,
    pub steps: Vec<StepResult>,
    /// RFC-3339 creation timestamp.
    pub created_at: String,
}

// ── LLM feedback types ────────────────────────────────────────────────────────

/// A table-level schema change detected during a step execution.
///
/// The calling SDK should update the tool definitions it passes to the LLM
/// to reflect these changes before the next LLM API call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaTableDelta {
    pub table_name: String,
    pub operation: DeltaOperation,
    /// JSON-serialised `TableDef` compatible with `SchemaService.GetTable`.
    pub table_def_json: String,
}

/// The feedback object returned with every step execution.
///
/// The calling SDK should:
/// 1. Replace or augment the LLM's tool definitions with the entries in
///    `schema_additions`.
/// 2. Append `augmentation_hints` as additional lines in the system prompt.
/// 3. Make `tool_output_json` available as the step's result context so the
///    LLM can reference it when choosing the next step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmContextUpdate {
    pub plan_id: String,
    pub step_id: String,
    /// JSON-serialised query result rows from the step that just executed.
    pub tool_output_json: String,
    /// Schema changes detected during this step (new or modified tables).
    pub schema_additions: Vec<SchemaTableDelta>,
    /// Plain-text hints to inject into the LLM system prompt.
    pub augmentation_hints: Vec<String>,
}

// ── WorkflowOrchestrator trait ────────────────────────────────────────────────

/// Hook for orchestrating multi-step LLM tool workflows.
///
/// Each method corresponds to one stage of the plan lifecycle.  Implementations
/// are responsible for persisting state between calls so that the LLM client
/// can call `execute_step` one step at a time across separate gRPC requests.
///
/// The OSS default ([`NoOpWorkflowOrchestrator`]) always errors; durable
/// implementations live in the enterprise backend.
#[async_trait]
pub trait WorkflowOrchestrator: Send + Sync {
    /// Validate and persist a new plan for the given session.
    ///
    /// Implementations must reject plans whose `depends_on` graph contains
    /// cycles and must initialise all steps with [`StepStatus::Pending`].
    async fn create_plan(
        &self,
        session_id: &str,
        definition: PlanDefinition,
    ) -> Result<PlanRecord, String>;

    /// Return the current plan record, or `Ok(None)` if no plan with that id
    /// exists in this deployment.
    async fn get_plan(&self, plan_id: &str) -> Result<Option<PlanRecord>, String>;

    /// Execute the named step within the given plan.
    ///
    /// The orchestrator must:
    /// 1. Verify all `depends_on` steps are `Completed`.
    /// 2. Substitute `{{step.<dep_id>.output}}` placeholders in
    ///    `query_template` using the JSON output of prior steps.
    /// 3. Snapshot the schema for `schema_table_hints` tables **before** and
    ///    **after** query execution and diff them to produce
    ///    `LlmContextUpdate.schema_additions`.
    /// 4. Call `QueryService.execute_query` with the resolved SQL.
    /// 5. Persist the `StepResult` and advance the plan status.
    /// 6. Return `(StepResult, LlmContextUpdate)` so the caller can feed the
    ///    updates back to the LLM.
    async fn execute_step(
        &self,
        plan_id: &str,
        step_id: &str,
        ctx: &QueryRuntimeContext,
    ) -> Result<(StepResult, LlmContextUpdate), String>;

    /// Cancel a pending or running plan.
    ///
    /// All `Pending` and `Running` steps must be set to [`StepStatus::Skipped`].
    async fn cancel_plan(&self, plan_id: &str) -> Result<(), String>;
}

// ── NoOpWorkflowOrchestrator — OSS default ────────────────────────────────────

/// No-op implementation of [`WorkflowOrchestrator`].
///
/// Returns an error for every stateful operation.  Used in OSS builds where no
/// enterprise storage backend is configured.
pub struct NoOpWorkflowOrchestrator;

#[async_trait]
impl WorkflowOrchestrator for NoOpWorkflowOrchestrator {
    async fn create_plan(
        &self,
        _session_id: &str,
        _definition: PlanDefinition,
    ) -> Result<PlanRecord, String> {
        Err("WorkflowOrchestrator not configured: no durable storage backend".to_string())
    }

    async fn get_plan(&self, _plan_id: &str) -> Result<Option<PlanRecord>, String> {
        Ok(None)
    }

    async fn execute_step(
        &self,
        _plan_id: &str,
        _step_id: &str,
        _ctx: &QueryRuntimeContext,
    ) -> Result<(StepResult, LlmContextUpdate), String> {
        Err("WorkflowOrchestrator not configured: no durable storage backend".to_string())
    }

    async fn cancel_plan(&self, _plan_id: &str) -> Result<(), String> {
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_create_plan_errors() {
        let orch = NoOpWorkflowOrchestrator;
        let def = PlanDefinition {
            name: "test".to_string(),
            description: "".to_string(),
            steps: vec![],
        };
        let result = orch.create_plan("session-1", def).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not configured"));
    }

    #[tokio::test]
    async fn noop_get_plan_returns_none() {
        let orch = NoOpWorkflowOrchestrator;
        let result = orch.get_plan("plan-xyz").await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn noop_execute_step_errors() {
        let orch = NoOpWorkflowOrchestrator;
        let ctx = QueryRuntimeContext::default();
        let result = orch.execute_step("plan-1", "step-1", &ctx).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn noop_cancel_plan_succeeds() {
        let orch = NoOpWorkflowOrchestrator;
        assert!(orch.cancel_plan("plan-1").await.is_ok());
    }

    #[test]
    fn plan_status_display() {
        assert_eq!(PlanStatus::Running.to_string(), "running");
        assert_eq!(PlanStatus::Completed.to_string(), "completed");
    }

    #[test]
    fn step_status_display() {
        assert_eq!(StepStatus::Skipped.to_string(), "skipped");
        assert_eq!(StepStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn step_definition_roundtrip() {
        let step = StepDefinition {
            id: "s1".to_string(),
            name: "Fetch users".to_string(),
            tool_name: "sql.read_select".to_string(),
            tool_intent: ToolIntent::ReadSelect,
            query_template: "SELECT id FROM users LIMIT 10".to_string(),
            depends_on: vec![],
            schema_table_hints: vec!["users".to_string()],
        };
        let json = serde_json::to_string(&step).unwrap();
        let back: StepDefinition = serde_json::from_str(&json).unwrap();
        assert_eq!(step, back);
    }

    #[test]
    fn llm_context_update_serialises() {
        let update = LlmContextUpdate {
            plan_id: "p1".to_string(),
            step_id: "s1".to_string(),
            tool_output_json: r#"[{"id":1}]"#.to_string(),
            schema_additions: vec![SchemaTableDelta {
                table_name: "orders".to_string(),
                operation: DeltaOperation::New,
                table_def_json: "{}".to_string(),
            }],
            augmentation_hints: vec!["The orders table is now available.".to_string()],
        };
        let json = serde_json::to_string(&update).unwrap();
        assert!(json.contains("plan_id"));
        assert!(json.contains("schema_additions"));
        assert!(json.contains("augmentation_hints"));
    }
}
