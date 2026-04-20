//! Concrete [`EventHandler`] implementations for the gRPC chain-of-responsibility pipeline.
//!
//! Each handler is a self-contained, stateless (or atomically-stateful) unit that can be
//! appended to the global [`EventBus`] handler chain via
//! [`EventBus::register_handler`].  Handlers return [`HandleOutcome::Continue`] by
//! default so the chain is not interrupted; a handler should only return
//! [`HandleOutcome::Stop`] when it has an authoritative reason to suppress further
//! processing (e.g., a rate-limit guard that has already emitted the error response).

use crate::interceptor::{Event, EventHandler, HandleOutcome};
use async_trait::async_trait;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ── AuditLogHandler ───────────────────────────────────────────────────────────

/// Writes a structured one-line JSON log entry to `stderr` for every dispatched
/// [`Event`].  This is the first handler registered in the default gRPC pipeline
/// so that every domain event is recorded before any downstream processing.
pub struct AuditLogHandler;

impl EventHandler for AuditLogHandler {
    fn handle(&self, event: &Event) -> HandleOutcome {
        if cfg!(not(test)) {
            let metadata = event.metadata();
            if let Ok(line) = serde_json::to_string(&metadata) {
                eprintln!("[audit] {line}");
            }
        }
        HandleOutcome::Continue
    }
}

// ── AuditExporter ─────────────────────────────────────────────────────────────

/// Hook for shipping audit records to external systems (SIEM, cloud logging,
/// centralised audit stores, etc.).
///
/// The local [`AuditLogHandler`] writes to `stderr` — suitable for OSS
/// single-node deployments.  Enterprise implementations implement this trait
/// to persist events durably and/or forward them to centralised audit backends.
///
/// Failures inside `export` **must not** propagate errors upstream; implementations
/// should log the failure and continue.
#[async_trait]
pub trait AuditExporter: Send + Sync {
    /// Export a batch of events to the external audit sink.
    async fn export(&self, events: &[Event]);
}

/// An [`EventHandler`] that delegates every event to an [`AuditExporter`] via a
/// fire-and-forget `tokio::spawn`.  Register this handler with the [`EventBus`]
/// after [`AuditLogHandler`] to add enterprise audit export to the pipeline.
pub struct AuditExportHandler {
    exporter: Arc<dyn AuditExporter>,
}

impl AuditExportHandler {
    pub fn new(exporter: Arc<dyn AuditExporter>) -> Self {
        Self { exporter }
    }
}

impl EventHandler for AuditExportHandler {
    fn handle(&self, event: &Event) -> HandleOutcome {
        let exporter = Arc::clone(&self.exporter);
        let event = event.clone();
        tokio::spawn(async move {
            exporter.export(&[event]).await;
        });
        HandleOutcome::Continue
    }
}

// ── QueryMetricsHandler ───────────────────────────────────────────────────────

/// Tracks the cumulative count of `QueryExecuted`, `QueryValidationFailed`, and
/// `QueryExecutionError` events through atomic counters.
///
/// The counters are cheap to read from any thread at any time — no lock required.
/// Obtain a shared reference via [`Arc<QueryMetricsHandler>`] and expose the
/// counts via a metrics or health endpoint.
pub struct QueryMetricsHandler {
    pub queries_executed: AtomicUsize,
    pub validation_failures: AtomicUsize,
    pub execution_errors: AtomicUsize,
}

impl QueryMetricsHandler {
    pub fn new() -> Self {
        Self {
            queries_executed: AtomicUsize::new(0),
            validation_failures: AtomicUsize::new(0),
            execution_errors: AtomicUsize::new(0),
        }
    }

    pub fn snapshot(&self) -> QueryMetricsSnapshot {
        QueryMetricsSnapshot {
            queries_executed: self.queries_executed.load(Ordering::Relaxed),
            validation_failures: self.validation_failures.load(Ordering::Relaxed),
            execution_errors: self.execution_errors.load(Ordering::Relaxed),
        }
    }
}

impl Default for QueryMetricsHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time copy of the atomic counters in [`QueryMetricsHandler`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryMetricsSnapshot {
    pub queries_executed: usize,
    pub validation_failures: usize,
    pub execution_errors: usize,
}

impl EventHandler for QueryMetricsHandler {
    fn handle(&self, event: &Event) -> HandleOutcome {
        match event {
            Event::QueryExecuted { .. } => {
                self.queries_executed.fetch_add(1, Ordering::Relaxed);
            }
            Event::QueryValidationFailed { .. } => {
                self.validation_failures.fetch_add(1, Ordering::Relaxed);
            }
            Event::QueryExecutionError { .. } => {
                self.execution_errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        HandleOutcome::Continue
    }
}

// ── SessionActivityHandler ────────────────────────────────────────────────────

/// Counts the number of `SessionRegistered` events seen so the gRPC layer can
/// expose a live session counter without a database round-trip.
pub struct SessionActivityHandler {
    pub sessions_registered: AtomicUsize,
}

impl SessionActivityHandler {
    pub fn new() -> Self {
        Self {
            sessions_registered: AtomicUsize::new(0),
        }
    }

    pub fn session_count(&self) -> usize {
        self.sessions_registered.load(Ordering::Relaxed)
    }
}

impl Default for SessionActivityHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl EventHandler for SessionActivityHandler {
    fn handle(&self, event: &Event) -> HandleOutcome {
        if matches!(event, Event::SessionRegistered { .. }) {
            self.sessions_registered.fetch_add(1, Ordering::Relaxed);
        }
        HandleOutcome::Continue
    }
}

// ── DataAccessEnforcedHandler ─────────────────────────────────────────────────

/// Tracks counts of data-access enforcement events emitted by RLS/CLS.
///
/// Counters are exposed atomically; obtain a snapshot via
/// [`DataAccessEnforcedHandler::snapshot`].
pub struct DataAccessEnforcedHandler {
    pub rows_filtered: AtomicUsize,
    pub columns_redacted: AtomicUsize,
    pub access_denied: AtomicUsize,
}

impl DataAccessEnforcedHandler {
    pub fn new() -> Self {
        Self {
            rows_filtered: AtomicUsize::new(0),
            columns_redacted: AtomicUsize::new(0),
            access_denied: AtomicUsize::new(0),
        }
    }

    pub fn snapshot(&self) -> DataAccessSnapshot {
        DataAccessSnapshot {
            rows_filtered: self.rows_filtered.load(Ordering::Relaxed),
            columns_redacted: self.columns_redacted.load(Ordering::Relaxed),
            access_denied: self.access_denied.load(Ordering::Relaxed),
        }
    }
}

impl Default for DataAccessEnforcedHandler {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time copy of the atomic counters in [`DataAccessEnforcedHandler`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataAccessSnapshot {
    pub rows_filtered: usize,
    pub columns_redacted: usize,
    pub access_denied: usize,
}

impl EventHandler for DataAccessEnforcedHandler {
    fn handle(&self, event: &Event) -> HandleOutcome {
        match event {
            Event::RowsFiltered { .. } => {
                self.rows_filtered.fetch_add(1, Ordering::Relaxed);
            }
            Event::ColumnsRedacted { .. } => {
                self.columns_redacted.fetch_add(1, Ordering::Relaxed);
            }
            Event::AccessDenied { .. } => {
                self.access_denied.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        HandleOutcome::Continue
    }
}

// ── DefaultHandlerChain ───────────────────────────────────────────────────────

/// Shared, arc-wrapped metrics and activity handles bundled together so the
/// gRPC server can register them with a single call and then retain references
/// for health checks.
#[derive(Clone)]
pub struct DefaultHandlerChain {
    pub query_metrics: Arc<QueryMetricsHandler>,
    pub session_activity: Arc<SessionActivityHandler>,
    pub data_access: Arc<DataAccessEnforcedHandler>,
}

impl DefaultHandlerChain {
    pub fn new() -> Self {
        Self {
            query_metrics: Arc::new(QueryMetricsHandler::new()),
            session_activity: Arc::new(SessionActivityHandler::new()),
            data_access: Arc::new(DataAccessEnforcedHandler::new()),
        }
    }
}

impl Default for DefaultHandlerChain {
    fn default() -> Self {
        Self::new()
    }
}

// ── Shared-reference EventHandler wrapper ─────────────────────────────────────

/// Newtype that lets an `Arc<T>` where `T: EventHandler` be registered with the
/// bus without giving up ownership of the underlying metrics struct.
pub struct SharedHandler<T: EventHandler>(pub Arc<T>);

impl<T: EventHandler + Send + Sync + 'static> EventHandler for SharedHandler<T> {
    fn handle(&self, event: &Event) -> HandleOutcome {
        self.0.handle(event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interceptor::{Event, EventBus};
    use std::collections::HashMap;

    fn query_executed_event() -> Event {
        Event::query_executed(
            "db".into(),
            "SELECT 1".into(),
            "ok".into(),
            1,
            5,
            "T".into(),
            HashMap::new(),
        )
    }

    fn session_registered_event() -> Event {
        Event::session_registered("sess-demo".into())
    }

    /// `QueryMetricsHandler` must increment the `queries_executed` counter on
    /// each `QueryExecuted` event and leave the other counters untouched.
    #[test]
    fn query_metrics_handler_counts_executed_events() {
        let handler = QueryMetricsHandler::new();
        handler.handle(&query_executed_event());
        handler.handle(&query_executed_event());
        let snap = handler.snapshot();
        assert_eq!(snap.queries_executed, 2);
        assert_eq!(snap.validation_failures, 0);
        assert_eq!(snap.execution_errors, 0);
    }

    /// `SessionActivityHandler` must count only `SessionRegistered` events.
    #[test]
    fn session_activity_handler_counts_session_registrations() {
        let handler = SessionActivityHandler::new();
        handler.handle(&session_registered_event());
        handler.handle(&session_registered_event());
        handler.handle(&query_executed_event()); // unrelated — must not increment
        assert_eq!(handler.session_count(), 2);
    }

    /// `DefaultHandlerChain` must work through the `EventBus` chain so metrics
    /// are updated when events are dispatched to the global bus.
    #[test]
    fn default_handler_chain_wires_into_event_bus() {
        let chain = DefaultHandlerChain::new();
        let bus = EventBus::new();

        bus.register_handler(Box::new(AuditLogHandler)).unwrap();
        bus.register_handler(Box::new(SharedHandler(Arc::clone(&chain.query_metrics))))
            .unwrap();
        bus.register_handler(Box::new(SharedHandler(Arc::clone(&chain.session_activity))))
            .unwrap();

        bus.dispatch_generic(&query_executed_event()).unwrap();
        bus.dispatch_generic(&session_registered_event()).unwrap();

        assert_eq!(chain.query_metrics.snapshot().queries_executed, 1);
        assert_eq!(chain.session_activity.session_count(), 1);
    }
}
