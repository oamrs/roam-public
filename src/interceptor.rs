use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CriticalStatusEvent {
    pub entity_type: String,
    pub entity_id: String,
    pub status: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RuntimeAugmentationAuditRecord {
    pub db_identifier: String,
    pub query: String,
    pub runtime_augmentation_id: String,
    pub runtime_augmentation_name: String,
    pub selection_reason: String,
    pub rendered_output: String,
    pub timestamp: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "event_type")]
pub enum Event {
    #[serde(rename = "StatusChange")]
    StatusChange {
        entity_type: String,
        entity_id: String,
        status: String,
        timestamp: String,
    },
    #[serde(rename = "ColumnChange")]
    ColumnChange {
        entity_type: String,
        entity_id: String,
        column_name: String,
        old_value: String,
        new_value: String,
        timestamp: String,
    },
    #[serde(rename = "ConstraintViolation")]
    ConstraintViolation {
        entity_type: String,
        entity_id: String,
        constraint_name: String,
        reason: String,
        timestamp: String,
    },
    #[serde(rename = "QueryExecuted")]
    QueryExecuted {
        db_identifier: String,
        query: String,
        status: String,
        row_count: i32,
        execution_ms: i32,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    #[serde(rename = "QueryValidationFailed")]
    QueryValidationFailed {
        db_identifier: String,
        query: String,
        error_reason: String,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    #[serde(rename = "QueryExecutionError")]
    QueryExecutionError {
        db_identifier: String,
        query: String,
        error_message: String,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    #[serde(rename = "RuntimeAugmentationAuditRecorded")]
    RuntimeAugmentationAuditRecorded {
        record: RuntimeAugmentationAuditRecord,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    #[serde(rename = "ModelChanged")]
    ModelChanged {
        entity_type: String,
        entity_id: String,
        action: String,
        timestamp: String,
    },
    #[serde(rename = "SessionRegistered")]
    SessionRegistered {
        session_id: String,
        timestamp: String,
    },
    #[serde(rename = "TriggerFired")]
    TriggerFired {
        table_name: String,
        trigger_name: String,
        operation: String,
        row_id: Option<String>,
        timestamp: String,
    },
    /// Emitted when row-level security rewrote a query to filter rows.
    #[serde(rename = "RowsFiltered")]
    RowsFiltered {
        db_identifier: String,
        table_name: String,
        user_id: String,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    /// Emitted when column-level security rewrote a `SELECT *` to exclude
    /// restricted columns.
    #[serde(rename = "ColumnsRedacted")]
    ColumnsRedacted {
        db_identifier: String,
        table_name: String,
        user_id: String,
        /// Names of the columns that were removed from the query.
        redacted_columns: Vec<String>,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    /// Emitted when a query was blocked entirely by an access policy.
    #[serde(rename = "AccessDenied")]
    AccessDenied {
        db_identifier: String,
        query: String,
        user_id: String,
        reason: String,
        timestamp: String,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        context: HashMap<String, String>,
    },
    // ── Control plane events ─────────────────────────────────────────────────
    /// Emitted when a new multi-step plan is created for a session.
    #[serde(rename = "PlanCreated")]
    PlanCreated {
        plan_id: String,
        session_id: String,
        step_count: usize,
        timestamp: String,
    },
    /// Emitted when a single step within a plan finishes execution (or fails).
    #[serde(rename = "PlanStepExecuted")]
    PlanStepExecuted {
        plan_id: String,
        step_id: String,
        step_index: usize,
        /// "completed" | "failed"
        status: String,
        row_count: i64,
        duration_ms: f64,
        timestamp: String,
    },
    /// Emitted when all steps in a plan have reached `Completed`.
    #[serde(rename = "PlanCompleted")]
    PlanCompleted {
        plan_id: String,
        session_id: String,
        total_steps: usize,
        duration_ms: f64,
        timestamp: String,
    },
    /// Emitted when a plan is cancelled or a step fails and halts the plan.
    #[serde(rename = "PlanFailed")]
    PlanFailed {
        plan_id: String,
        session_id: String,
        failed_step_id: String,
        reason: String,
        timestamp: String,
    },
}

impl Event {
    fn insert_context_metadata(
        map: &mut HashMap<String, String>,
        context: &HashMap<String, String>,
    ) {
        for (key, value) in context {
            map.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }

    pub fn status_change(
        entity_type: String,
        entity_id: String,
        status: String,
        timestamp: String,
    ) -> Self {
        Event::StatusChange {
            entity_type,
            entity_id,
            status,
            timestamp,
        }
    }

    pub fn column_change(
        entity_type: String,
        entity_id: String,
        column_name: String,
        old_value: String,
        new_value: String,
        timestamp: String,
    ) -> Self {
        Event::ColumnChange {
            entity_type,
            entity_id,
            column_name,
            old_value,
            new_value,
            timestamp,
        }
    }

    pub fn constraint_violation(
        entity_type: String,
        entity_id: String,
        constraint_name: String,
        reason: String,
        timestamp: String,
    ) -> Self {
        Event::ConstraintViolation {
            entity_type,
            entity_id,
            constraint_name,
            reason,
            timestamp,
        }
    }

    pub fn query_executed(
        db_identifier: String,
        query: String,
        status: String,
        row_count: i32,
        execution_ms: i32,
        timestamp: String,
        context: HashMap<String, String>,
    ) -> Self {
        Event::QueryExecuted {
            db_identifier,
            query,
            status,
            row_count,
            execution_ms,
            timestamp,
            context,
        }
    }

    pub fn query_validation_failed(
        db_identifier: String,
        query: String,
        error_reason: String,
        timestamp: String,
        context: HashMap<String, String>,
    ) -> Self {
        Event::QueryValidationFailed {
            db_identifier,
            query,
            error_reason,
            timestamp,
            context,
        }
    }

    pub fn query_execution_error(
        db_identifier: String,
        query: String,
        error_message: String,
        timestamp: String,
        context: HashMap<String, String>,
    ) -> Self {
        Event::QueryExecutionError {
            db_identifier,
            query,
            error_message,
            timestamp,
            context,
        }
    }

    pub fn runtime_augmentation_audit_recorded(
        record: RuntimeAugmentationAuditRecord,
        context: HashMap<String, String>,
    ) -> Self {
        Event::RuntimeAugmentationAuditRecorded { record, context }
    }

    pub fn model_changed(entity_type: String, entity_id: String, action: String) -> Self {
        Event::ModelChanged {
            entity_type,
            entity_id,
            action,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn session_registered(session_id: String) -> Self {
        Event::SessionRegistered {
            session_id,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn trigger_fired(
        table_name: String,
        trigger_name: String,
        operation: String,
        row_id: Option<String>,
    ) -> Self {
        Event::TriggerFired {
            table_name,
            trigger_name,
            operation,
            row_id,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn rows_filtered(
        db_identifier: String,
        table_name: String,
        user_id: String,
        context: HashMap<String, String>,
    ) -> Self {
        Event::RowsFiltered {
            db_identifier,
            table_name,
            user_id,
            timestamp: chrono::Utc::now().to_rfc3339(),
            context,
        }
    }

    pub fn columns_redacted(
        db_identifier: String,
        table_name: String,
        user_id: String,
        redacted_columns: Vec<String>,
        context: HashMap<String, String>,
    ) -> Self {
        Event::ColumnsRedacted {
            db_identifier,
            table_name,
            user_id,
            redacted_columns,
            timestamp: chrono::Utc::now().to_rfc3339(),
            context,
        }
    }

    pub fn access_denied(
        db_identifier: String,
        query: String,
        user_id: String,
        reason: String,
        context: HashMap<String, String>,
    ) -> Self {
        Event::AccessDenied {
            db_identifier,
            query,
            user_id,
            reason,
            timestamp: chrono::Utc::now().to_rfc3339(),
            context,
        }
    }

    pub fn event_type(&self) -> &str {
        match self {
            Event::StatusChange { .. } => "StatusChange",
            Event::ColumnChange { .. } => "ColumnChange",
            Event::ConstraintViolation { .. } => "ConstraintViolation",
            Event::QueryExecuted { .. } => "QueryExecuted",
            Event::QueryValidationFailed { .. } => "QueryValidationFailed",
            Event::QueryExecutionError { .. } => "QueryExecutionError",
            Event::RuntimeAugmentationAuditRecorded { .. } => "RuntimeAugmentationAuditRecorded",
            Event::ModelChanged { .. } => "ModelChanged",
            Event::SessionRegistered { .. } => "SessionRegistered",
            Event::TriggerFired { .. } => "TriggerFired",
            Event::RowsFiltered { .. } => "RowsFiltered",
            Event::ColumnsRedacted { .. } => "ColumnsRedacted",
            Event::AccessDenied { .. } => "AccessDenied",
            Event::PlanCreated { .. } => "PlanCreated",
            Event::PlanStepExecuted { .. } => "PlanStepExecuted",
            Event::PlanCompleted { .. } => "PlanCompleted",
            Event::PlanFailed { .. } => "PlanFailed",
        }
    }

    pub fn metadata(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("event_type".to_string(), self.event_type().to_string());

        match self {
            Event::StatusChange {
                entity_type,
                entity_id,
                status,
                timestamp,
            } => {
                map.insert("entity_type".to_string(), entity_type.clone());
                map.insert("entity_id".to_string(), entity_id.clone());
                map.insert("status".to_string(), status.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::ColumnChange {
                entity_type,
                entity_id,
                column_name,
                old_value,
                new_value,
                timestamp,
            } => {
                map.insert("entity_type".to_string(), entity_type.clone());
                map.insert("entity_id".to_string(), entity_id.clone());
                map.insert("column_name".to_string(), column_name.clone());
                map.insert("old_value".to_string(), old_value.clone());
                map.insert("new_value".to_string(), new_value.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::ConstraintViolation {
                entity_type,
                entity_id,
                constraint_name,
                reason,
                timestamp,
            } => {
                map.insert("entity_type".to_string(), entity_type.clone());
                map.insert("entity_id".to_string(), entity_id.clone());
                map.insert("constraint_name".to_string(), constraint_name.clone());
                map.insert("reason".to_string(), reason.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::QueryExecuted {
                db_identifier,
                query,
                status,
                row_count,
                execution_ms,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("query".to_string(), query.clone());
                map.insert("status".to_string(), status.clone());
                map.insert("row_count".to_string(), row_count.to_string());
                map.insert("execution_ms".to_string(), execution_ms.to_string());
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::QueryValidationFailed {
                db_identifier,
                query,
                error_reason,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("query".to_string(), query.clone());
                map.insert("error_reason".to_string(), error_reason.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::QueryExecutionError {
                db_identifier,
                query,
                error_message,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("query".to_string(), query.clone());
                map.insert("error_message".to_string(), error_message.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::RuntimeAugmentationAuditRecorded { record, context } => {
                map.insert("db_identifier".to_string(), record.db_identifier.clone());
                map.insert("query".to_string(), record.query.clone());
                map.insert(
                    "runtime_augmentation_id".to_string(),
                    record.runtime_augmentation_id.clone(),
                );
                map.insert(
                    "runtime_augmentation_name".to_string(),
                    record.runtime_augmentation_name.clone(),
                );
                map.insert(
                    "selection_reason".to_string(),
                    record.selection_reason.clone(),
                );
                map.insert(
                    "rendered_output".to_string(),
                    record.rendered_output.clone(),
                );
                map.insert("timestamp".to_string(), record.timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::ModelChanged {
                entity_type,
                entity_id,
                action,
                timestamp,
            } => {
                map.insert("entity_type".to_string(), entity_type.clone());
                map.insert("entity_id".to_string(), entity_id.clone());
                map.insert("action".to_string(), action.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::SessionRegistered {
                session_id,
                timestamp,
            } => {
                map.insert("session_id".to_string(), session_id.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::TriggerFired {
                table_name,
                trigger_name,
                operation,
                row_id,
                timestamp,
            } => {
                map.insert("table_name".to_string(), table_name.clone());
                map.insert("trigger_name".to_string(), trigger_name.clone());
                map.insert("operation".to_string(), operation.clone());
                if let Some(id) = row_id {
                    map.insert("row_id".to_string(), id.clone());
                }
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::RowsFiltered {
                db_identifier,
                table_name,
                user_id,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("table_name".to_string(), table_name.clone());
                map.insert("user_id".to_string(), user_id.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::ColumnsRedacted {
                db_identifier,
                table_name,
                user_id,
                redacted_columns,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("table_name".to_string(), table_name.clone());
                map.insert("user_id".to_string(), user_id.clone());
                map.insert("redacted_columns".to_string(), redacted_columns.join(","));
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::AccessDenied {
                db_identifier,
                query,
                user_id,
                reason,
                timestamp,
                context,
            } => {
                map.insert("db_identifier".to_string(), db_identifier.clone());
                map.insert("query".to_string(), query.clone());
                map.insert("user_id".to_string(), user_id.clone());
                map.insert("reason".to_string(), reason.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
                Self::insert_context_metadata(&mut map, context);
            }
            Event::PlanCreated {
                plan_id,
                session_id,
                step_count,
                timestamp,
            } => {
                map.insert("plan_id".to_string(), plan_id.clone());
                map.insert("session_id".to_string(), session_id.clone());
                map.insert("step_count".to_string(), step_count.to_string());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::PlanStepExecuted {
                plan_id,
                step_id,
                step_index,
                status,
                row_count,
                duration_ms,
                timestamp,
            } => {
                map.insert("plan_id".to_string(), plan_id.clone());
                map.insert("step_id".to_string(), step_id.clone());
                map.insert("step_index".to_string(), step_index.to_string());
                map.insert("status".to_string(), status.clone());
                map.insert("row_count".to_string(), row_count.to_string());
                map.insert("duration_ms".to_string(), duration_ms.to_string());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::PlanCompleted {
                plan_id,
                session_id,
                total_steps,
                duration_ms,
                timestamp,
            } => {
                map.insert("plan_id".to_string(), plan_id.clone());
                map.insert("session_id".to_string(), session_id.clone());
                map.insert("total_steps".to_string(), total_steps.to_string());
                map.insert("duration_ms".to_string(), duration_ms.to_string());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
            Event::PlanFailed {
                plan_id,
                session_id,
                failed_step_id,
                reason,
                timestamp,
            } => {
                map.insert("plan_id".to_string(), plan_id.clone());
                map.insert("session_id".to_string(), session_id.clone());
                map.insert("failed_step_id".to_string(), failed_step_id.clone());
                map.insert("reason".to_string(), reason.clone());
                map.insert("timestamp".to_string(), timestamp.clone());
            }
        }

        map
    }

    pub fn plan_created(plan_id: String, session_id: String, step_count: usize) -> Self {
        Event::PlanCreated {
            plan_id,
            session_id,
            step_count,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn plan_step_executed(
        plan_id: String,
        step_id: String,
        step_index: usize,
        status: String,
        row_count: i64,
        duration_ms: f64,
    ) -> Self {
        Event::PlanStepExecuted {
            plan_id,
            step_id,
            step_index,
            status,
            row_count,
            duration_ms,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn plan_completed(
        plan_id: String,
        session_id: String,
        total_steps: usize,
        duration_ms: f64,
    ) -> Self {
        Event::PlanCompleted {
            plan_id,
            session_id,
            total_steps,
            duration_ms,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn plan_failed(
        plan_id: String,
        session_id: String,
        failed_step_id: String,
        reason: String,
    ) -> Self {
        Event::PlanFailed {
            plan_id,
            session_id,
            failed_step_id,
            reason,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

type SubscriberFn = Box<dyn Fn(&Event) + Send + 'static>;

type SubscriberMap = HashMap<usize, SubscriberFn>;

type TypeSubscriberMap = HashMap<String, Vec<SubscriberFn>>;

// ── Chain of Responsibility ────────────────────────────────────────────────────

/// Controls whether the remaining handlers in the chain are invoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleOutcome {
    /// Allow the next handler in the chain to run.
    Continue,
    /// Short-circuit the chain; no further handlers are invoked.
    Stop,
}

/// A segment in the `EventBus` chain-of-responsibility pipeline.
///
/// Implementors receive a reference to the dispatched `Event`, perform their
/// work (e.g., audit logging, gRPC stream forwarding, access control checks),
/// and return a [`HandleOutcome`] to signal whether the remaining chain should
/// continue.
pub trait EventHandler: Send + Sync {
    fn handle(&self, event: &Event) -> HandleOutcome;
}

// ─────────────────────────────────────────────────────────────────────────────

pub struct EventBus {
    events: Mutex<Vec<CriticalStatusEvent>>,
    generic_events: Mutex<Vec<Event>>,
    persisted_log: Mutex<Vec<Event>>,
    subscribers: Mutex<SubscriberMap>,
    type_subscribers: Mutex<TypeSubscriberMap>,
    next_subscriber_id: AtomicUsize,
    handler_chain: Mutex<Vec<Arc<dyn EventHandler>>>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub fn new() -> Self {
        EventBus {
            events: Mutex::new(Vec::new()),
            generic_events: Mutex::new(Vec::new()),
            persisted_log: Mutex::new(Vec::new()),
            subscribers: Mutex::new(HashMap::new()),
            type_subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: AtomicUsize::new(1),
            handler_chain: Mutex::new(Vec::new()),
        }
    }

    pub fn dispatch(&self, event: CriticalStatusEvent) -> Result<(), String> {
        self.events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .push(event);
        Ok(())
    }

    pub fn events(&self) -> Result<Vec<CriticalStatusEvent>, String> {
        self.events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    pub fn clear(&self) -> Result<(), String> {
        self.events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|mut guard| guard.clear())?;
        self.generic_events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|mut guard| guard.clear())?;
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|mut guard| guard.clear())?;
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|mut guard| guard.clear())
    }

    pub fn dispatch_generic(&self, event: &Event) -> Result<(), String> {
        self.generic_events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .push(event.clone());

        // Persist the event
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .push(event.clone());

        // Notify all subscribers
        let subscribers = self
            .subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;
        for (_id, callback) in subscribers.iter() {
            callback(event);
        }
        drop(subscribers);

        // Notify type-specific subscribers
        let type_subs = self
            .type_subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?;
        if let Some(callbacks) = type_subs.get(event.event_type()) {
            for callback in callbacks {
                callback(event);
            }
        }
        drop(type_subs);

        // Snapshot Arc pointers under the lock, then drop the lock before
        // invoking handlers so re-entrant dispatch or registration cannot deadlock.
        let snapshot: Vec<Arc<dyn EventHandler>> = self
            .handler_chain
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .clone();
        for handler in &snapshot {
            if handler.handle(event) == HandleOutcome::Stop {
                break;
            }
        }

        Ok(())
    }

    /// Append a [`EventHandler`] to the end of the chain-of-responsibility
    /// pipeline.  Handlers are invoked in registration order during every call
    /// to [`dispatch_generic`].  A handler may return [`HandleOutcome::Stop`]
    /// to prevent subsequent handlers from running.
    pub fn register_handler(&self, handler: Box<dyn EventHandler>) -> Result<(), String> {
        self.handler_chain
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .push(Arc::from(handler));
        Ok(())
    }

    pub fn all_events(&self) -> Result<Vec<Event>, String> {
        self.generic_events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    pub fn events_by_type(&self, event_type: &str) -> Result<Vec<Event>, String> {
        self.generic_events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| {
                guard
                    .iter()
                    .filter(|e| e.event_type() == event_type)
                    .cloned()
                    .collect()
            })
    }

    pub fn persisted_events(&self) -> Result<Vec<Event>, String> {
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    pub fn load_from_log(&self) -> Result<Vec<Event>, String> {
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    pub fn register_subscriber(
        &self,
        callback: Box<dyn Fn(&Event) + Send + 'static>,
    ) -> Result<usize, String> {
        let subscriber_id = self.next_subscriber_id.fetch_add(1, Ordering::SeqCst);
        self.subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .insert(subscriber_id, callback);
        Ok(subscriber_id)
    }

    pub fn unregister_subscriber(&self, subscriber_id: usize) -> Result<(), String> {
        self.subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .remove(&subscriber_id);
        Ok(())
    }

    pub fn register_subscriber_for_type(
        &self,
        event_type: &str,
        callback: Box<dyn Fn(&Event) + Send + 'static>,
    ) -> Result<(), String> {
        self.type_subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .entry(event_type.to_string())
            .or_insert_with(Vec::new)
            .push(callback);
        Ok(())
    }
}

static GLOBAL_EVENT_BUS: Lazy<EventBus> = Lazy::new(EventBus::new);

pub fn get_event_bus() -> &'static EventBus {
    &GLOBAL_EVENT_BUS
}

pub trait HasCriticalStatus {
    fn get_status(&self) -> String;
    fn get_entity_type(&self) -> String;
    fn get_entity_id(&self) -> String;
    fn get_timestamp(&self) -> String;
}

pub struct CriticalModelBehavior;

impl CriticalModelBehavior {
    pub async fn after_save<M>(
        model: M,
        _db: &impl std::any::Any,
        _insert: bool,
    ) -> Result<M, String>
    where
        M: HasCriticalStatus,
    {
        let status = model.get_status();
        if status == "CRITICAL" {
            let event = CriticalStatusEvent {
                entity_type: model.get_entity_type(),
                entity_id: model.get_entity_id(),
                status: status.clone(),
                timestamp: model.get_timestamp(),
            };
            get_event_bus()
                .dispatch(event)
                .map_err(|e| format!("Failed to dispatch event: {}", e))?;
        }
        Ok(model)
    }
}
