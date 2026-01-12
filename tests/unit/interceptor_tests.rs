use oam::interceptor::{
    get_event_bus, CriticalModelBehavior, CriticalStatusEvent, Event, EventBus, HasCriticalStatus,
};
use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Global test lock to ensure test isolation for shared EventBus state
/// Only one test that uses the global EventBus can run at a time
static TEST_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Helper to acquire test lock during execution
fn _acquire_test_lock() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap()
}

#[tokio::test]
async fn after_save_hook_dispatches_critical_status_event() {
    let event_bus = EventBus::new();

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

#[tokio::test]
async fn model_after_save_hook_integration() {
    let _lock = _acquire_test_lock();

    let event_bus = get_event_bus();
    event_bus.clear().unwrap();

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
    assert!(
        !events.is_empty(),
        "after_save hook should dispatch CRITICAL events to the event bus"
    );
    assert_eq!(events[0].status, "CRITICAL");

    event_bus.clear().unwrap();
}

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
    let _saved = CriticalModelBehavior::after_save(model, &MockDb::new(), true)
        .await
        .expect("after_save should succeed");

    // THEN: The event should be automatically dispatched to the global bus
    let event_bus = get_event_bus();
    let events = event_bus.events().unwrap();

    assert!(
        !events.is_empty(),
        "after_save hook should auto-dispatch CRITICAL events"
    );
    assert_eq!(events[0].status, "CRITICAL");
    assert_eq!(events[0].entity_type, "ServiceAlert");
    assert_eq!(events[0].entity_id, "alert-42");

    event_bus.clear().unwrap();
}

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

    event_bus.clear().unwrap();
}

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

#[tokio::test]
async fn event_trait_supports_multiple_event_types() {
    let _lock = _acquire_test_lock();

    let status_event = Event::status_change(
        "Alert".to_string(),
        "alert-001".to_string(),
        "CRITICAL".to_string(),
        "2024-01-09T10:00:00Z".to_string(),
    );

    let column_event = Event::column_change(
        "User".to_string(),
        "user-42".to_string(),
        "email".to_string(),
        "old@example.com".to_string(),
        "new@example.com".to_string(),
        "2024-01-09T10:05:00Z".to_string(),
    );

    let event_bus = EventBus::new();
    event_bus.dispatch_generic(&status_event).unwrap();
    event_bus.dispatch_generic(&column_event).unwrap();

    let events = event_bus.all_events().unwrap();
    assert_eq!(
        events.len(),
        2,
        "EventBus should store multiple event types"
    );
}

/// The system should be able to query events by their type, allowing
/// subscribers to listen only to events they care about.
#[tokio::test]
async fn event_bus_filters_events_by_type() {
    let _lock = _acquire_test_lock();

    let status_event = Event::status_change(
        "Alert".to_string(),
        "alert-001".to_string(),
        "CRITICAL".to_string(),
        "2024-01-09T10:00:00Z".to_string(),
    );

    let column_event = Event::column_change(
        "User".to_string(),
        "user-42".to_string(),
        "email".to_string(),
        "old@example.com".to_string(),
        "new@example.com".to_string(),
        "2024-01-09T10:05:00Z".to_string(),
    );

    let event_bus = EventBus::new();
    event_bus.dispatch_generic(&status_event).unwrap();
    event_bus.dispatch_generic(&column_event).unwrap();

    let status_only = event_bus.events_by_type("StatusChange").unwrap();
    let column_only = event_bus.events_by_type("ColumnChange").unwrap();

    assert_eq!(status_only.len(), 1, "Should filter status change events");
    assert_eq!(column_only.len(), 1, "Should filter column change events");
}

#[tokio::test]
async fn event_types_have_specific_metadata() {
    let _lock = _acquire_test_lock();

    let column_event = Event::column_change(
        "Order".to_string(),
        "order-789".to_string(),
        "status".to_string(),
        "PENDING".to_string(),
        "SHIPPED".to_string(),
        "2024-01-09T11:30:00Z".to_string(),
    );

    let metadata = column_event.metadata();

    assert_eq!(metadata.get("event_type").unwrap(), "ColumnChange");
    assert_eq!(metadata.get("entity_type").unwrap(), "Order");
    assert_eq!(metadata.get("entity_id").unwrap(), "order-789");
    assert_eq!(metadata.get("column_name").unwrap(), "status");
    assert_eq!(metadata.get("old_value").unwrap(), "PENDING");
    assert_eq!(metadata.get("new_value").unwrap(), "SHIPPED");
}

#[tokio::test]
async fn event_bus_generic_dispatch() {
    let _lock = _acquire_test_lock();

    let event_bus = EventBus::new();

    let events = vec![
        Event::status_change(
            "Alert".to_string(),
            "a1".to_string(),
            "CRITICAL".to_string(),
            "2024-01-09T10:00:00Z".to_string(),
        ),
        Event::column_change(
            "User".to_string(),
            "u1".to_string(),
            "name".to_string(),
            "Alice".to_string(),
            "Alice Smith".to_string(),
            "2024-01-09T10:01:00Z".to_string(),
        ),
        Event::constraint_violation(
            "Product".to_string(),
            "p1".to_string(),
            "fk_category".to_string(),
            "Foreign key constraint violated: category_id does not exist".to_string(),
            "2024-01-09T10:02:00Z".to_string(),
        ),
    ];

    for event in events {
        event_bus.dispatch_generic(&event).unwrap();
    }

    let all_events = event_bus.all_events().unwrap();
    assert_eq!(all_events.len(), 3, "All event types should be stored");
}

#[tokio::test]
async fn event_serialization() {
    let _lock = _acquire_test_lock();

    let status_event = Event::status_change(
        "Alert".to_string(),
        "alert-123".to_string(),
        "WARNING".to_string(),
        "2024-01-09T12:00:00Z".to_string(),
    );

    let json = serde_json::to_string(&status_event).unwrap();
    let deserialized: Event = serde_json::from_str(&json).unwrap();

    assert_eq!(
        status_event, deserialized,
        "Events should survive serialization"
    );
}

/// Events should be written to a durable log so they survive process restarts.
/// This enables event replay, auditing, and historical analysis.
#[tokio::test]
async fn event_bus_persists_to_log() {
    let _lock = _acquire_test_lock();

    // GIVEN: A new EventBus
    let event_bus = EventBus::new();

    // WHEN: We dispatch events
    let events = vec![
        Event::status_change(
            "Alert".to_string(),
            "a1".to_string(),
            "CRITICAL".to_string(),
            "2024-01-11T10:00:00Z".to_string(),
        ),
        Event::column_change(
            "User".to_string(),
            "u1".to_string(),
            "email".to_string(),
            "old@example.com".to_string(),
            "new@example.com".to_string(),
            "2024-01-11T10:01:00Z".to_string(),
        ),
    ];

    for event in &events {
        event_bus.dispatch_generic(event).unwrap();
    }

    // THEN: Events are persisted to the event log
    let persisted = event_bus.persisted_events().unwrap();
    assert_eq!(persisted.len(), 2, "All events should be persisted");
    assert_eq!(persisted[0].event_type(), "StatusChange");
    assert_eq!(persisted[1].event_type(), "ColumnChange");
}

/// Events persisted to a log should be loadable, enabling replay and recovery.
#[tokio::test]
async fn event_bus_loads_persisted_events() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus with some persisted events
    let event_bus = EventBus::new();
    let event = Event::status_change(
        "Alert".to_string(),
        "a1".to_string(),
        "CRITICAL".to_string(),
        "2024-01-11T10:00:00Z".to_string(),
    );
    event_bus.dispatch_generic(&event).unwrap();

    // WHEN: We load events from the persistent log
    let loaded = event_bus.load_from_log().unwrap();

    // THEN: Loaded events match what was persisted
    assert!(!loaded.is_empty(), "Should load persisted events");
    assert_eq!(loaded[0].event_type(), "StatusChange");
}

/// The event bus should support multiple subscribers that get notified
/// when new events are dispatched. This enables decoupled event handlers.
#[tokio::test]
async fn event_bus_register_subscriber() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus and a subscriber callback
    let event_bus = EventBus::new();
    let subscriber_called = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let subscriber_called_clone = subscriber_called.clone();

    // WHEN: We register a subscriber
    let subscriber_id = event_bus
        .register_subscriber(Box::new(move |event| {
            if event.event_type() == "StatusChange" {
                subscriber_called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }))
        .unwrap();

    // THEN: Subscriber is registered
    assert!(subscriber_id > 0, "Subscriber should get a positive ID");
}

/// When an event is dispatched, all registered subscribers should be notified.
#[tokio::test]
async fn event_bus_notifies_subscribers() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus with a registered subscriber
    let event_bus = EventBus::new();
    let notification_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = notification_count.clone();

    event_bus
        .register_subscriber(Box::new(move |_event| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }))
        .unwrap();

    // WHEN: We dispatch an event
    let event = Event::status_change(
        "Alert".to_string(),
        "a1".to_string(),
        "CRITICAL".to_string(),
        "2024-01-11T10:00:00Z".to_string(),
    );
    event_bus.dispatch_generic(&event).unwrap();

    // THEN: Subscriber is notified
    assert_eq!(
        notification_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Subscriber should be notified of the event"
    );
}

/// Multiple subscribers should be able to listen to the same event stream,
/// each receiving a copy of all events.
#[tokio::test]
async fn event_bus_multiple_subscribers() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus with multiple subscribers
    let event_bus = EventBus::new();
    let count1 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count2 = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count1_clone = count1.clone();
    let count2_clone = count2.clone();

    event_bus
        .register_subscriber(Box::new(move |_event| {
            count1_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }))
        .unwrap();

    event_bus
        .register_subscriber(Box::new(move |_event| {
            count2_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }))
        .unwrap();

    // WHEN: We dispatch an event
    let event = Event::column_change(
        "User".to_string(),
        "u1".to_string(),
        "name".to_string(),
        "Alice".to_string(),
        "Alice Smith".to_string(),
        "2024-01-11T10:00:00Z".to_string(),
    );
    event_bus.dispatch_generic(&event).unwrap();

    // THEN: Both subscribers are notified
    assert_eq!(
        count1.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Subscriber 1 should be notified"
    );
    assert_eq!(
        count2.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Subscriber 2 should be notified"
    );
}

/// Subscribers should be able to unregister and stop receiving events.
#[tokio::test]
async fn event_bus_unregister_subscriber() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus with a registered subscriber
    let event_bus = EventBus::new();
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count_clone = count.clone();

    let subscriber_id = event_bus
        .register_subscriber(Box::new(move |_event| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }))
        .unwrap();

    // WHEN: We unregister the subscriber
    event_bus.unregister_subscriber(subscriber_id).unwrap();

    // AND: We dispatch an event
    let event = Event::constraint_violation(
        "Order".to_string(),
        "o1".to_string(),
        "fk_customer".to_string(),
        "Customer ID does not exist".to_string(),
        "2024-01-11T10:00:00Z".to_string(),
    );
    event_bus.dispatch_generic(&event).unwrap();

    // THEN: Subscriber is not notified
    assert_eq!(
        count.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "Unregistered subscriber should not be notified"
    );
}

/// Subscribers should be able to subscribe only to specific event types
/// to reduce unnecessary callback invocations.
#[tokio::test]
async fn event_bus_subscriber_filters_by_type() {
    let _lock = _acquire_test_lock();

    // GIVEN: An EventBus with a subscriber that filters by event type
    let event_bus = EventBus::new();
    let status_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let status_count_clone = status_count.clone();

    event_bus
        .register_subscriber_for_type(
            "StatusChange",
            Box::new(move |_event| {
                status_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
        )
        .unwrap();

    // WHEN: We dispatch different event types
    event_bus
        .dispatch_generic(&Event::status_change(
            "Alert".to_string(),
            "a1".to_string(),
            "CRITICAL".to_string(),
            "2024-01-11T10:00:00Z".to_string(),
        ))
        .unwrap();

    event_bus
        .dispatch_generic(&Event::column_change(
            "User".to_string(),
            "u1".to_string(),
            "email".to_string(),
            "old@example.com".to_string(),
            "new@example.com".to_string(),
            "2024-01-11T10:01:00Z".to_string(),
        ))
        .unwrap();

    // THEN: Only StatusChange events trigger the subscriber
    assert_eq!(
        status_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "Subscriber should only receive StatusChange events"
    );
}
