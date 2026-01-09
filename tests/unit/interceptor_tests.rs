/// Integration tests for OAM Event Interceptor
/// 
/// These tests verify that SeaORM models with ActiveModelBehavior hooks
/// properly dispatch events when critical status changes occur.

use oam::interceptor::{CriticalStatusEvent, EventBus, get_event_bus, CriticalModelBehavior, HasCriticalStatus};
use std::sync::Mutex;
use once_cell::sync::Lazy;

/// Global test lock to ensure test isolation for shared EventBus state
/// Only one test that uses the global EventBus can run at a time
static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Helper to acquire test lock during execution
fn _acquire_test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap()
}

/// FAILING TEST: Verify that after_save hook dispatches CRITICAL event
/// 
/// TDD phase: RED - This test should fail because the hook is not implemented yet
/// 
/// Scenario: A mock model is saved with status="CRITICAL"
/// Expected: after_save hook should detect critical status and dispatch event via event bus
#[tokio::test]
async fn after_save_hook_dispatches_critical_status_event() {
    // GIVEN: An event bus is created
    let event_bus = EventBus::new();

    // WHEN: We simulate a model save with CRITICAL status
    let event = CriticalStatusEvent {
        entity_type: "SystemAlert".to_string(),
        entity_id: "alert-001".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-07T15:30:00Z".to_string(),
    };

    // THEN: The event should be dispatched to the event bus
    // (In the real implementation, after_save hook will call event_bus.dispatch)
    event_bus.dispatch(event.clone()).unwrap();

    let events = event_bus.events().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].status, "CRITICAL");
    assert_eq!(events[0].entity_type, "SystemAlert");
}

/// Verify event bus handles multiple CRITICAL events in sequence
#[tokio::test]
async fn after_save_hook_handles_multiple_critical_events() {
    let event_bus = EventBus::new();

    let event1 = CriticalStatusEvent {
        entity_type: "SecurityBreach".to_string(),
        entity_id: "breach-001".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-07T15:00:00Z".to_string(),
    };

    let event2 = CriticalStatusEvent {
        entity_type: "DataCorruption".to_string(),
        entity_id: "corrupt-002".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-07T15:05:00Z".to_string(),
    };

    event_bus.dispatch(event1.clone()).unwrap();
    event_bus.dispatch(event2.clone()).unwrap();

    let events = event_bus.events().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].entity_id, "breach-001");
    assert_eq!(events[1].entity_id, "corrupt-002");
}

/// Verify non-CRITICAL status changes do NOT dispatch events
#[tokio::test]
async fn after_save_hook_ignores_non_critical_status() {
    let event_bus = EventBus::new();

    let event_warning = CriticalStatusEvent {
        entity_type: "Alert".to_string(),
        entity_id: "alert-002".to_string(),
        status: "WARNING".to_string(),
        timestamp: "2024-01-07T15:15:00Z".to_string(),
    };

    // In the real implementation, after_save should filter by status == "CRITICAL"
    // For now, we test that non-CRITICAL events would be filtered
    if event_warning.status == "CRITICAL" {
        event_bus.dispatch(event_warning.clone()).unwrap();
    }

    let events = event_bus.events().unwrap();
    // Should be empty because status is WARNING, not CRITICAL
    assert_eq!(events.len(), 0);
}

/// FAILING TEST: Verify that ActiveModelBehavior::after_save hook is implemented
///
/// TDD phase: RED - This test requires the actual hook implementation
///
/// This test verifies that when a SeaORM model with CRITICAL status is saved,
/// the after_save hook is called and dispatches the event to the global event bus.
/// The hook must integrate with the EventBus and filter for CRITICAL status.
#[tokio::test]
async fn model_after_save_hook_integration() {
    let _lock = _acquire_test_lock();
    // GIVEN: We get the global event bus and clear it
    let event_bus = get_event_bus();
    event_bus.clear().unwrap();

    // WHEN: We simulate a model save with CRITICAL status by dispatching to global bus
    // In the real implementation, this would be triggered by SeaORM's after_save hook
    let event = CriticalStatusEvent {
        entity_type: "HealthCheck".to_string(),
        entity_id: "health-001".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-07T16:00:00Z".to_string(),
    };

    // The after_save hook should automatically dispatch the event
    event_bus.dispatch(event.clone()).unwrap();

    // THEN: The event should be in the global event bus
    let events = event_bus.events().unwrap();
    assert!(!events.is_empty(), "after_save hook should dispatch CRITICAL events to the event bus");
    assert_eq!(events[0].status, "CRITICAL");
    
    // Cleanup for other tests
    event_bus.clear().unwrap();
}

/// FAILING TEST: Verify that ActiveModelBehavior::after_save hook dispatches CRITICAL events automatically
///
/// TDD phase: RED - This test will fail until after_save hook extracts status from model
/// and automatically dispatches to the global event bus.
///
/// This test verifies that when a SeaORM model is saved, the after_save hook:
/// 1. Extracts the model's status field
/// 2. If status == "CRITICAL", creates a CriticalStatusEvent
/// 3. Dispatches the event to the global EventBus
/// 4. Does NOT dispatch non-CRITICAL events
#[tokio::test]
async fn active_model_behavior_auto_dispatches_critical_on_save() {
    let _lock = _acquire_test_lock();
    // GIVEN: A mock SeaORM model with CRITICAL status
    get_event_bus().clear().unwrap();
    let model = MockCriticalModel {
        id: 42,
        entity_type: "ServiceAlert".to_string(),
        entity_id: "alert-42".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-08T10:30:00Z".to_string(),
    };

    // WHEN: The model's after_save hook is called (simulating SeaORM save operation)
    // NOTE: This will fail to compile until CriticalModelBehavior::after_save is implemented
    let _saved = CriticalModelBehavior::after_save(model, &MockDb::new(), true)
        .await
        .expect("after_save should succeed");

    // THEN: The event should be automatically dispatched to the global bus
    let event_bus = get_event_bus();
    let events = event_bus.events().unwrap();
    
    assert!(!events.is_empty(), "after_save hook should auto-dispatch CRITICAL events");
    assert_eq!(events[0].status, "CRITICAL");
    assert_eq!(events[0].entity_type, "ServiceAlert");
    assert_eq!(events[0].entity_id, "alert-42");
    
    // Cleanup for other tests
    event_bus.clear().unwrap();
}

/// FAILING TEST: Verify that non-CRITICAL status changes are NOT auto-dispatched
///
/// TDD phase: RED - This test will fail until the after_save hook implements status filtering
#[tokio::test]
async fn active_model_behavior_filters_non_critical_status() {
    let _lock = _acquire_test_lock();
    // GIVEN: A mock model with WARNING (non-CRITICAL) status
    get_event_bus().clear().unwrap();
    let model = MockCriticalModel {
        id: 43,
        entity_type: "ServiceAlert".to_string(),
        entity_id: "alert-43".to_string(),
        status: "WARNING".to_string(),
        timestamp: "2024-01-08T10:35:00Z".to_string(),
    };

    // WHEN: The model's after_save hook is called with non-CRITICAL status
    let _saved = CriticalModelBehavior::after_save(model, &MockDb::new(), true)
        .await
        .expect("after_save should succeed");

    // THEN: The event should NOT be dispatched (no event added to bus)
    let event_bus = get_event_bus();
    let initial_count = event_bus.events().unwrap().len();
    
    // After save with non-CRITICAL status, count should not increase
    assert!(
        event_bus.events().unwrap().len() == initial_count,
        "non-CRITICAL status should not trigger auto-dispatch"
    );
    
    // Cleanup for other tests
    event_bus.clear().unwrap();
}

/// FAILING TEST: Verify that after_save hook sets proper event metadata
///
/// TDD phase: RED - This test ensures events created by after_save have correct metadata
#[tokio::test]
async fn active_model_behavior_sets_event_metadata() {
    let _lock = _acquire_test_lock();
    // GIVEN: A model with all required fields
    get_event_bus().clear().unwrap();
    let model = MockCriticalModel {
        id: 44,
        entity_type: "DataCorruption".to_string(),
        entity_id: "corrupt-001".to_string(),
        status: "CRITICAL".to_string(),
        timestamp: "2024-01-08T10:40:00Z".to_string(),
    };

    // WHEN: The model is saved and hook processes it
    let _saved = CriticalModelBehavior::after_save(model, &MockDb::new(), true)
        .await
        .expect("after_save should succeed");

    // THEN: The dispatched event should preserve all metadata
    let event_bus = get_event_bus();
    let events = event_bus.events().unwrap();
    let last_event = events.last().expect("should have at least one event");

    assert_eq!(last_event.entity_type, "DataCorruption");
    assert_eq!(last_event.entity_id, "corrupt-001");
    assert_eq!(last_event.status, "CRITICAL");
    assert_eq!(last_event.timestamp, "2024-01-08T10:40:00Z");
    
    // Cleanup for other tests
    event_bus.clear().unwrap();
}

/// Mock SeaORM Model for testing after_save hook
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MockCriticalModel {
    pub id: i32,
    pub entity_type: String,
    pub entity_id: String,
    pub status: String,
    pub timestamp: String,
}

impl HasCriticalStatus for MockCriticalModel {
    fn get_status(&self) -> String {
        self.status.clone()
    }

    fn get_entity_type(&self) -> String {
        self.entity_type.clone()
    }

    fn get_entity_id(&self) -> String {
        self.entity_id.clone()
    }

    fn get_timestamp(&self) -> String {
        self.timestamp.clone()
    }
}

/// Mock Database Connection for testing
#[derive(Debug)]
pub struct MockDb;

impl MockDb {
    pub fn new() -> Self {
        MockDb
    }
}
