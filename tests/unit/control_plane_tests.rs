use oam::control_plane::{
    DeltaOperation, LlmContextUpdate, NoOpWorkflowOrchestrator, PlanDefinition, PlanStatus,
    SchemaTableDelta, StepDefinition, StepStatus, WorkflowOrchestrator,
};
use oam::interceptor::Event;
use oam::policy_engine::ToolIntent;
use oam::QueryRuntimeContext;

// ── NoOpWorkflowOrchestrator ──────────────────────────────────────────────────

fn minimal_plan() -> PlanDefinition {
    PlanDefinition {
        name: "test-plan".to_string(),
        description: "".to_string(),
        steps: vec![],
    }
}

#[tokio::test]
async fn noop_orchestrator_create_plan_returns_error() {
    let orch = NoOpWorkflowOrchestrator;
    let result = orch.create_plan("session-1", minimal_plan()).await;
    assert!(result.is_err(), "NoOp must reject create_plan");
    assert!(
        result.unwrap_err().contains("not configured"),
        "error should mention 'not configured'"
    );
}

#[tokio::test]
async fn noop_orchestrator_get_plan_returns_ok_none() {
    let orch = NoOpWorkflowOrchestrator;
    let result = orch.get_plan("plan-does-not-exist").await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn noop_orchestrator_execute_step_returns_error() {
    let orch = NoOpWorkflowOrchestrator;
    let ctx = QueryRuntimeContext::default();
    let result = orch.execute_step("plan-1", "step-1", &ctx).await;
    assert!(result.is_err(), "NoOp must reject execute_step");
    assert!(result.unwrap_err().contains("not configured"));
}

#[tokio::test]
async fn noop_orchestrator_cancel_plan_returns_ok() {
    let orch = NoOpWorkflowOrchestrator;
    let result = orch.cancel_plan("plan-1").await;
    assert!(result.is_ok(), "NoOp cancel_plan must succeed silently");
}

// ── PlanStatus ────────────────────────────────────────────────────────────────

#[test]
fn plan_status_all_variants_have_distinct_str() {
    let variants = [
        PlanStatus::Pending,
        PlanStatus::Running,
        PlanStatus::Completed,
        PlanStatus::Failed,
        PlanStatus::Cancelled,
    ];
    let strs: Vec<&str> = variants.iter().map(|s| s.as_str()).collect();
    let unique: std::collections::HashSet<&&str> = strs.iter().collect();
    assert_eq!(
        strs.len(),
        unique.len(),
        "all PlanStatus strings must be unique"
    );
}

#[test]
fn plan_status_display_matches_as_str() {
    let cases = [
        (PlanStatus::Pending, "pending"),
        (PlanStatus::Running, "running"),
        (PlanStatus::Completed, "completed"),
        (PlanStatus::Failed, "failed"),
        (PlanStatus::Cancelled, "cancelled"),
    ];
    for (status, expected) in cases {
        assert_eq!(status.as_str(), expected);
        assert_eq!(status.to_string(), expected);
    }
}

// ── StepStatus ────────────────────────────────────────────────────────────────

#[test]
fn step_status_all_variants_have_distinct_str() {
    let variants = [
        StepStatus::Pending,
        StepStatus::Running,
        StepStatus::Completed,
        StepStatus::Failed,
        StepStatus::Skipped,
    ];
    let strs: Vec<&str> = variants.iter().map(|s| s.as_str()).collect();
    let unique: std::collections::HashSet<&&str> = strs.iter().collect();
    assert_eq!(
        strs.len(),
        unique.len(),
        "all StepStatus strings must be unique"
    );
}

#[test]
fn step_status_display_matches_as_str() {
    let cases = [
        (StepStatus::Pending, "pending"),
        (StepStatus::Running, "running"),
        (StepStatus::Completed, "completed"),
        (StepStatus::Failed, "failed"),
        (StepStatus::Skipped, "skipped"),
    ];
    for (status, expected) in cases {
        assert_eq!(status.as_str(), expected);
        assert_eq!(status.to_string(), expected);
    }
}

// ── DeltaOperation ────────────────────────────────────────────────────────────

#[test]
fn delta_operation_serialises_to_screaming_snake_case() {
    assert_eq!(
        serde_json::to_string(&DeltaOperation::New).unwrap(),
        "\"NEW\""
    );
    assert_eq!(
        serde_json::to_string(&DeltaOperation::Modified).unwrap(),
        "\"MODIFIED\""
    );
    assert_eq!(
        serde_json::to_string(&DeltaOperation::Removed).unwrap(),
        "\"REMOVED\""
    );
}

#[test]
fn delta_operation_deserialises_from_screaming_snake_case() {
    let op: DeltaOperation = serde_json::from_str("\"NEW\"").unwrap();
    assert_eq!(op, DeltaOperation::New);
    let op: DeltaOperation = serde_json::from_str("\"MODIFIED\"").unwrap();
    assert_eq!(op, DeltaOperation::Modified);
    let op: DeltaOperation = serde_json::from_str("\"REMOVED\"").unwrap();
    assert_eq!(op, DeltaOperation::Removed);
}

// ── LlmContextUpdate ─────────────────────────────────────────────────────────

#[test]
fn llm_context_update_roundtrips_via_serde() {
    let update = LlmContextUpdate {
        plan_id: "plan-abc".to_string(),
        step_id: "step-1".to_string(),
        tool_output_json: "{\"id\": 42}".to_string(),
        schema_additions: vec![SchemaTableDelta {
            table_name: "orders".to_string(),
            operation: DeltaOperation::New,
            table_def_json: "{}".to_string(),
        }],
        augmentation_hints: vec!["A new table 'orders' is now available.".to_string()],
    };

    let json = serde_json::to_string(&update).expect("should serialise");
    let decoded: LlmContextUpdate = serde_json::from_str(&json).expect("should deserialise");
    assert_eq!(decoded.plan_id, "plan-abc");
    assert_eq!(decoded.step_id, "step-1");
    assert_eq!(decoded.schema_additions.len(), 1);
    assert_eq!(decoded.schema_additions[0].operation, DeltaOperation::New);
    assert_eq!(
        decoded.augmentation_hints[0],
        "A new table 'orders' is now available."
    );
}

#[test]
fn llm_context_update_empty_additions_roundtrips() {
    let update = LlmContextUpdate {
        plan_id: "plan-x".to_string(),
        step_id: "step-y".to_string(),
        tool_output_json: "null".to_string(),
        schema_additions: vec![],
        augmentation_hints: vec![],
    };
    let json = serde_json::to_string(&update).expect("should serialise");
    let decoded: LlmContextUpdate = serde_json::from_str(&json).expect("should deserialise");
    assert!(decoded.schema_additions.is_empty());
    assert!(decoded.augmentation_hints.is_empty());
}

// ── StepDefinition / PlanDefinition ──────────────────────────────────────────

#[test]
fn step_definition_fields_are_publicly_accessible() {
    let step = StepDefinition {
        id: "s1".to_string(),
        name: "Lookup customer".to_string(),
        tool_name: "query_customers".to_string(),
        tool_intent: ToolIntent::ReadSelect,
        query_template: "SELECT id FROM customers WHERE email = 'a@b.com'".to_string(),
        depends_on: vec![],
        schema_table_hints: vec!["customers".to_string()],
    };
    assert_eq!(step.id, "s1");
    assert_eq!(step.tool_intent, ToolIntent::ReadSelect);
    assert!(step.depends_on.is_empty());
}

#[test]
fn plan_definition_with_dependent_steps_is_constructable() {
    let step_a = StepDefinition {
        id: "a".to_string(),
        name: "First".to_string(),
        tool_name: "t".to_string(),
        tool_intent: ToolIntent::ReadSelect,
        query_template: "SELECT 1".to_string(),
        depends_on: vec![],
        schema_table_hints: vec![],
    };
    let step_b = StepDefinition {
        id: "b".to_string(),
        name: "Second".to_string(),
        tool_name: "t".to_string(),
        tool_intent: ToolIntent::ReadSelect,
        query_template: "SELECT {{step.a.output}}".to_string(),
        depends_on: vec!["a".to_string()],
        schema_table_hints: vec![],
    };
    let plan = PlanDefinition {
        name: "Sequential plan".to_string(),
        description: "Two steps".to_string(),
        steps: vec![step_a, step_b],
    };
    assert_eq!(plan.steps.len(), 2);
    assert_eq!(plan.steps[1].depends_on, vec!["a"]);
}

// ── Plan events ───────────────────────────────────────────────────────────────

#[test]
fn plan_created_event_roundtrips_via_serde() {
    let event = Event::PlanCreated {
        plan_id: "plan-1".to_string(),
        session_id: "sess-1".to_string(),
        step_count: 3,
        timestamp: "2026-04-20T00:00:00Z".to_string(),
    };
    let json = serde_json::to_string(&event).expect("should serialise");
    assert!(json.contains("PlanCreated"));
    assert!(json.contains("plan-1"));
    let decoded: Event = serde_json::from_str(&json).expect("should deserialise");
    match decoded {
        Event::PlanCreated {
            plan_id,
            step_count,
            ..
        } => {
            assert_eq!(plan_id, "plan-1");
            assert_eq!(step_count, 3);
        }
        other => panic!("unexpected variant: {:?}", other),
    }
}

#[test]
fn plan_step_executed_event_roundtrips_via_serde() {
    let event = Event::PlanStepExecuted {
        plan_id: "plan-2".to_string(),
        step_id: "step-a".to_string(),
        step_index: 0,
        status: "completed".to_string(),
        row_count: 5,
        duration_ms: 42.0,
        timestamp: "2026-04-20T00:00:01Z".to_string(),
    };
    let json = serde_json::to_string(&event).expect("should serialise");
    let decoded: Event = serde_json::from_str(&json).expect("should deserialise");
    match decoded {
        Event::PlanStepExecuted {
            plan_id,
            step_id,
            step_index,
            row_count,
            ..
        } => {
            assert_eq!(plan_id, "plan-2");
            assert_eq!(step_id, "step-a");
            assert_eq!(step_index, 0);
            assert_eq!(row_count, 5);
        }
        other => panic!("unexpected variant: {:?}", other),
    }
}

#[test]
fn plan_completed_event_roundtrips_via_serde() {
    let event = Event::PlanCompleted {
        plan_id: "plan-3".to_string(),
        session_id: "sess-2".to_string(),
        total_steps: 2,
        duration_ms: 100.0,
        timestamp: "2026-04-20T00:00:02Z".to_string(),
    };
    let json = serde_json::to_string(&event).expect("should serialise");
    let decoded: Event = serde_json::from_str(&json).expect("should deserialise");
    match decoded {
        Event::PlanCompleted {
            plan_id,
            total_steps,
            ..
        } => {
            assert_eq!(plan_id, "plan-3");
            assert_eq!(total_steps, 2);
        }
        other => panic!("unexpected variant: {:?}", other),
    }
}

#[test]
fn plan_failed_event_roundtrips_via_serde() {
    let event = Event::PlanFailed {
        plan_id: "plan-4".to_string(),
        session_id: "sess-3".to_string(),
        failed_step_id: "step-b".to_string(),
        reason: "execution error".to_string(),
        timestamp: "2026-04-20T00:00:03Z".to_string(),
    };
    let json = serde_json::to_string(&event).expect("should serialise");
    let decoded: Event = serde_json::from_str(&json).expect("should deserialise");
    match decoded {
        Event::PlanFailed {
            plan_id,
            failed_step_id,
            reason,
            ..
        } => {
            assert_eq!(plan_id, "plan-4");
            assert_eq!(failed_step_id, "step-b");
            assert_eq!(reason, "execution error");
        }
        other => panic!("unexpected variant: {:?}", other),
    }
}

#[test]
fn plan_events_have_correct_event_type_metadata() {
    let created = Event::PlanCreated {
        plan_id: "p".to_string(),
        session_id: "s".to_string(),
        step_count: 1,
        timestamp: "t".to_string(),
    };
    let meta = created.metadata();
    assert_eq!(
        meta.get("event_type").map(String::as_str),
        Some("PlanCreated")
    );
    assert_eq!(meta.get("plan_id").map(String::as_str), Some("p"));

    let executed = Event::PlanStepExecuted {
        plan_id: "p2".to_string(),
        step_id: "s1".to_string(),
        step_index: 0,
        status: "completed".to_string(),
        row_count: 1,
        duration_ms: 1.0,
        timestamp: "t".to_string(),
    };
    let meta2 = executed.metadata();
    assert_eq!(meta2.get("plan_id").map(String::as_str), Some("p2"));
    assert_eq!(meta2.get("step_index").map(String::as_str), Some("0"));
}

// ── QueryRuntimeContext plan headers ──────────────────────────────────────────

#[test]
fn runtime_context_extracts_plan_id_header() {
    let mut req = tonic::Request::new(());
    req.metadata_mut()
        .insert("x-roam-plan-id", "plan-abc123".parse().unwrap());

    let ctx = QueryRuntimeContext::from_metadata(req.metadata());
    assert_eq!(ctx.plan_id.as_deref(), Some("plan-abc123"));
    assert!(ctx.step_index.is_none());
}

#[test]
fn runtime_context_extracts_step_index_header() {
    let mut req = tonic::Request::new(());
    req.metadata_mut()
        .insert("x-roam-step-index", "3".parse().unwrap());

    let ctx = QueryRuntimeContext::from_metadata(req.metadata());
    assert_eq!(ctx.step_index, Some(3u32));
    assert!(ctx.plan_id.is_none());
}

#[test]
fn runtime_context_extracts_both_plan_headers_together() {
    let mut req = tonic::Request::new(());
    req.metadata_mut()
        .insert("x-roam-plan-id", "plan-xyz".parse().unwrap());
    req.metadata_mut()
        .insert("x-roam-step-index", "1".parse().unwrap());

    let ctx = QueryRuntimeContext::from_metadata(req.metadata());
    assert_eq!(ctx.plan_id.as_deref(), Some("plan-xyz"));
    assert_eq!(ctx.step_index, Some(1u32));
}

#[test]
fn runtime_context_plan_headers_count_in_has_values() {
    let ctx_with_plan = QueryRuntimeContext {
        plan_id: Some("plan-1".to_string()),
        ..Default::default()
    };
    assert!(ctx_with_plan.has_values());

    let ctx_with_step = QueryRuntimeContext {
        step_index: Some(0),
        ..Default::default()
    };
    assert!(ctx_with_step.has_values());

    let empty = QueryRuntimeContext::default();
    assert!(!empty.has_values());
}

#[test]
fn runtime_context_plan_id_absent_when_header_not_sent() {
    let req = tonic::Request::new(());
    let ctx = QueryRuntimeContext::from_metadata(req.metadata());
    assert!(ctx.plan_id.is_none());
    assert!(ctx.step_index.is_none());
}
