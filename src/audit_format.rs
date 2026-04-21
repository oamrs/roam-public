//! OCSF v1.1 event mapping for ROAM audit envelopes.
//!
//! [`event_to_ocsf`] converts an [`AuditEventEnvelope`] into an OCSF v1.1
//! JSON object suitable for ingestion by any OCSF-compatible SIEM or AI-SPM
//! tool.
//!
//! Class-UID mapping:
//!
//! | ROAM event                           | OCSF class                 | UID  |
//! |--------------------------------------|----------------------------|------|
//! | QueryExecuted                        | Database Activity          | 6003 |
//! | AccessDenied, PromptInjectionSignal… | Security Finding           | 2004 |
//! | SessionRegistered                    | Authentication             | 2001 |
//! | PlanCreated/Completed/Failed,        | API Activity               | 6005 |
//! | PlanStepExecuted,                    |                            |      |
//! | LlmToolCallAuditRecorded,            |                            |      |
//! | RuntimeAugmentationAuditRecorded     |                            |      |
//! | All other events                     | API Activity (unmapped)    | 6005 |

use crate::interceptor::{AuditEventEnvelope, Event};
use serde_json::{json, Value};
use std::collections::HashMap;

const OCSF_VERSION: &str = "1.1.0";
const PRODUCT_NAME: &str = "ROAM";
const PRODUCT_VENDOR: &str = "OAM";
const PRODUCT_VERSION: &str = env!("CARGO_PKG_VERSION");

const CLASS_UID_AUTH: i64 = 2001;
const CLASS_UID_SECURITY_FINDING: i64 = 2004;
const CLASS_UID_DATABASE_ACTIVITY: i64 = 6003;
const CLASS_UID_API_ACTIVITY: i64 = 6005;

/// Convert a ROAM [`AuditEventEnvelope`] into an OCSF v1.1 JSON object.
pub fn event_to_ocsf(envelope: &AuditEventEnvelope) -> Value {
    let base = base_record(envelope);
    let class_specific = class_specific_fields(&envelope.event);
    merge(base, class_specific)
}

// ── Base record (common fields for every OCSF event) ─────────────────────────

fn base_record(envelope: &AuditEventEnvelope) -> Value {
    let (class_uid, class_name, activity_id, activity_name) = classify(&envelope.event);

    // OCSF time is epoch milliseconds
    let time_ms = chrono::DateTime::parse_from_rfc3339(&envelope.emitted_at)
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|_| chrono::Utc::now().timestamp_millis());

    let mut record = json!({
        "metadata": {
            "version": OCSF_VERSION,
            "product": {
                "name": PRODUCT_NAME,
                "vendor_name": PRODUCT_VENDOR,
                "version": PRODUCT_VERSION,
            },
            "uid": format!("{}", envelope.sequence),
            "log_name": "roam-audit",
        },
        "class_uid": class_uid,
        "class_name": class_name,
        "activity_id": activity_id,
        "activity_name": activity_name,
        "category_uid": class_uid / 1000,
        "time": time_ms,
        "severity_id": default_severity_id(&envelope.event),
        "severity": default_severity_label(&envelope.event),
        "status": default_status(&envelope.event),
        "status_id": default_status_id(&envelope.event),
        // Chain-of-custody fields (ROAM-specific, placed in unmapped)
        "unmapped": {
            "roam_sequence": envelope.sequence,
            "roam_prev_hash": envelope.prev_hash,
            "roam_hash": envelope.hash,
        }
    });

    if let Some(trace_id) = &envelope.trace_id {
        record["metadata"]["trace_uid"] = json!(trace_id);
    }
    if let Some(span_id) = &envelope.span_id {
        record["metadata"]["span_uid"] = json!(span_id);
    }

    record
}

// ── Per-class field enrichment ────────────────────────────────────────────────

fn class_specific_fields(event: &Event) -> Value {
    match event {
        // ── Database Activity (6003) ─────────────────────────────────────────
        Event::QueryExecuted {
            db_identifier,
            query,
            status,
            row_count,
            execution_ms,
            context,
            ..
        } => {
            json!({
                "database": { "name": db_identifier },
                "query": { "querystring": query },
                "duration": execution_ms,
                "num_responses": row_count,
                "status": status,
                "actor": actor_from_context(context),
            })
        }
        Event::QueryValidationFailed {
            db_identifier,
            query,
            error_reason,
            context,
            ..
        } => {
            json!({
                "database": { "name": db_identifier },
                "query": { "querystring": query },
                "message": error_reason,
                "status": "Failure",
                "status_id": 2,
                "actor": actor_from_context(context),
            })
        }
        Event::QueryExecutionError {
            db_identifier,
            query,
            error_message,
            context,
            ..
        } => {
            json!({
                "database": { "name": db_identifier },
                "query": { "querystring": query },
                "message": error_message,
                "status": "Failure",
                "status_id": 2,
                "actor": actor_from_context(context),
            })
        }
        Event::RowsFiltered {
            db_identifier,
            table_name,
            user_id,
            context,
            ..
        } => {
            json!({
                "database": { "name": db_identifier },
                "table": { "name": table_name },
                "actor": { "user": { "uid": user_id } },
                "unmapped": context_to_map(context),
            })
        }
        Event::ColumnsRedacted {
            db_identifier,
            table_name,
            user_id,
            redacted_columns,
            context,
            ..
        } => {
            json!({
                "database": { "name": db_identifier },
                "table": { "name": table_name },
                "actor": { "user": { "uid": user_id } },
                "unmapped": {
                    "redacted_columns": redacted_columns.join(","),
                    "context": context_to_map(context),
                },
            })
        }

        // ── Authentication (2001) ────────────────────────────────────────────
        Event::SessionRegistered { session_id, .. } => {
            json!({
                "actor": { "session": { "uid": session_id } },
                "status": "Success",
                "status_id": 1,
            })
        }

        // ── Security Finding (2004) ──────────────────────────────────────────
        Event::AccessDenied {
            db_identifier,
            query,
            user_id,
            reason,
            context,
            ..
        } => {
            json!({
                "finding": {
                    "title": "Query Access Denied",
                    "desc": reason,
                    "uid": format!("roam-access-denied-{}", db_identifier),
                },
                "actor": { "user": { "uid": user_id } },
                "query": { "querystring": query },
                "unmapped": context_to_map(context),
            })
        }
        Event::PromptInjectionSignalRaised {
            input_excerpt,
            input_hash,
            matched_patterns,
            severity,
            action_taken,
            context,
            ..
        } => {
            json!({
                "finding": {
                    "title": "Prompt Injection Signal",
                    "desc": format!("Matched patterns: {}", matched_patterns.join(", ")),
                    "uid": format!("roam-injection-{}", input_hash),
                },
                "disposition": action_taken,
                "unmapped": {
                    "input_excerpt": input_excerpt,
                    "input_hash": input_hash,
                    "roam_severity": severity,
                    "context": context_to_map(context),
                },
            })
        }

        // ── API Activity (6005) ──────────────────────────────────────────────
        Event::LlmToolCallAuditRecorded {
            tool_name,
            function_arguments,
            result_summary,
            duration_ms,
            context,
            ..
        } => {
            json!({
                "api": {
                    "operation": tool_name,
                    "request": { "body": function_arguments },
                    "response": { "body": result_summary },
                },
                "duration": duration_ms,
                "actor": actor_from_context(context),
            })
        }
        Event::RuntimeAugmentationAuditRecorded { record, context } => {
            json!({
                "api": {
                    "operation": record.runtime_augmentation_name,
                    "request": { "body": record.query },
                    "response": { "body": record.rendered_output },
                },
                "unmapped": {
                    "selection_reason": record.selection_reason,
                    "runtime_augmentation_id": record.runtime_augmentation_id,
                    "context": context_to_map(context),
                },
            })
        }
        Event::PlanCreated {
            plan_id,
            session_id,
            step_count,
            ..
        } => {
            json!({
                "api": { "operation": "PlanCreated" },
                "unmapped": {
                    "plan_id": plan_id,
                    "session_id": session_id,
                    "step_count": step_count,
                },
            })
        }
        Event::PlanStepExecuted {
            plan_id,
            step_id,
            step_index,
            status,
            row_count,
            duration_ms,
            ..
        } => {
            json!({
                "api": { "operation": "PlanStepExecuted" },
                "status": status,
                "duration": duration_ms,
                "num_responses": row_count,
                "unmapped": {
                    "plan_id": plan_id,
                    "step_id": step_id,
                    "step_index": step_index,
                },
            })
        }
        Event::PlanCompleted {
            plan_id,
            session_id,
            total_steps,
            duration_ms,
            ..
        } => {
            json!({
                "api": { "operation": "PlanCompleted" },
                "status": "Success",
                "duration": duration_ms,
                "unmapped": {
                    "plan_id": plan_id,
                    "session_id": session_id,
                    "total_steps": total_steps,
                },
            })
        }
        Event::PlanFailed {
            plan_id,
            session_id,
            failed_step_id,
            reason,
            ..
        } => {
            json!({
                "api": { "operation": "PlanFailed" },
                "status": "Failure",
                "status_id": 2,
                "message": reason,
                "unmapped": {
                    "plan_id": plan_id,
                    "session_id": session_id,
                    "failed_step_id": failed_step_id,
                },
            })
        }

        // ── Remaining events → API Activity with unmapped payload ────────────
        _ => {
            let metadata = event.metadata();
            json!({ "unmapped": metadata_to_value(metadata) })
        }
    }
}

// ── Classification helpers ────────────────────────────────────────────────────

fn classify(event: &Event) -> (i64, &'static str, i32, &'static str) {
    match event {
        Event::SessionRegistered { .. } => (CLASS_UID_AUTH, "Authentication", 1, "Logon"),
        Event::AccessDenied { .. } | Event::PromptInjectionSignalRaised { .. } => {
            (CLASS_UID_SECURITY_FINDING, "Security Finding", 1, "Create")
        }
        Event::QueryExecuted { .. }
        | Event::QueryValidationFailed { .. }
        | Event::QueryExecutionError { .. }
        | Event::RowsFiltered { .. }
        | Event::ColumnsRedacted { .. } => {
            (CLASS_UID_DATABASE_ACTIVITY, "Database Activity", 1, "Query")
        }
        _ => (CLASS_UID_API_ACTIVITY, "API Activity", 1, "Invoke"),
    }
}

fn default_severity_id(event: &Event) -> i32 {
    match event {
        Event::AccessDenied { .. } => 3, // Medium
        Event::PromptInjectionSignalRaised { severity, .. } => match severity.as_str() {
            "high" => 4,   // High
            "medium" => 3, // Medium
            _ => 2,        // Low
        },
        Event::QueryExecutionError { .. } | Event::PlanFailed { .. } => 3,
        _ => 1, // Informational
    }
}

fn default_severity_label(event: &Event) -> &'static str {
    match default_severity_id(event) {
        4 => "High",
        3 => "Medium",
        2 => "Low",
        _ => "Informational",
    }
}

fn default_status(event: &Event) -> &'static str {
    match event {
        Event::QueryExecutionError { .. }
        | Event::QueryValidationFailed { .. }
        | Event::AccessDenied { .. }
        | Event::PlanFailed { .. } => "Failure",
        _ => "Success",
    }
}

fn default_status_id(event: &Event) -> i32 {
    match default_status(event) {
        "Failure" => 2,
        _ => 1,
    }
}

// ── Field construction helpers ────────────────────────────────────────────────

fn actor_from_context(context: &HashMap<String, String>) -> Value {
    let mut actor = json!({});
    if let Some(user_id) = context.get("user_id") {
        actor["user"] = json!({ "uid": user_id });
    }
    if let Some(session_id) = context.get("session_id") {
        actor["session"] = json!({ "uid": session_id });
    }
    actor
}

fn context_to_map(context: &HashMap<String, String>) -> Value {
    let map: serde_json::Map<String, Value> =
        context.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
    Value::Object(map)
}

fn metadata_to_value(metadata: HashMap<String, String>) -> Value {
    let map: serde_json::Map<String, Value> =
        metadata.into_iter().map(|(k, v)| (k, json!(v))).collect();
    Value::Object(map)
}

fn merge(mut base: Value, extra: Value) -> Value {
    if let (Value::Object(ref mut base_map), Value::Object(extra_map)) = (&mut base, extra) {
        for (key, value) in extra_map {
            match base_map.get_mut(&key) {
                Some(Value::Object(ref mut existing)) => {
                    if let Value::Object(new_map) = value {
                        for (k, v) in new_map {
                            existing.insert(k, v);
                        }
                    } else {
                        base_map.insert(key, value);
                    }
                }
                _ => {
                    base_map.insert(key, value);
                }
            }
        }
    }
    base
}
