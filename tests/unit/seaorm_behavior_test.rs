//! Tests for SeaORM behavior pattern and ModelChanged events
//!
//! These tests verify the after_save hook pattern that will be implemented
//! in backend models to dispatch events on save.

use oam::interceptor::Event;

#[test]
fn model_changed_event_creation() {
    let event = Event::model_changed(
        "organization".to_string(),
        "org-123".to_string(),
        "created".to_string(),
    );

    match event {
        Event::ModelChanged {
            entity_type,
            entity_id,
            action,
            ..
        } => {
            assert_eq!(entity_type, "organization");
            assert_eq!(entity_id, "org-123");
            assert_eq!(action, "created");
        }
        _ => panic!("Expected ModelChanged event"),
    }
}

#[test]
fn model_changed_event_type() {
    let event = Event::model_changed(
        "user".to_string(),
        "user-456".to_string(),
        "updated".to_string(),
    );

    assert_eq!(event.event_type(), "ModelChanged");
}

#[test]
fn model_changed_event_metadata() {
    let event = Event::model_changed(
        "user_organization".to_string(),
        "user-1:org-2".to_string(),
        "created".to_string(),
    );

    let metadata = event.metadata();
    assert_eq!(
        metadata.get("entity_type"),
        Some(&"user_organization".to_string())
    );
    assert_eq!(metadata.get("entity_id"), Some(&"user-1:org-2".to_string()));
    assert_eq!(metadata.get("action"), Some(&"created".to_string()));
    assert_eq!(
        metadata.get("event_type"),
        Some(&"ModelChanged".to_string())
    );
}

#[test]
fn model_changed_event_has_timestamp() {
    let event = Event::model_changed(
        "organization".to_string(),
        "org-789".to_string(),
        "updated".to_string(),
    );

    match event {
        Event::ModelChanged { timestamp, .. } => {
            assert!(!timestamp.is_empty(), "Timestamp should not be empty");
            // Verify it looks like an RFC3339 timestamp
            assert!(timestamp.contains('T'), "Should be RFC3339 format");
        }
        _ => panic!("Expected ModelChanged event"),
    }
}

#[test]
fn model_changed_distinguishes_actions() {
    let created = Event::model_changed(
        "org".to_string(),
        "org-1".to_string(),
        "created".to_string(),
    );

    let updated = Event::model_changed(
        "org".to_string(),
        "org-1".to_string(),
        "updated".to_string(),
    );

    match (&created, &updated) {
        (Event::ModelChanged { action: a1, .. }, Event::ModelChanged { action: a2, .. }) => {
            assert_eq!(a1, "created");
            assert_eq!(a2, "updated");
            assert_ne!(a1, a2);
        }
        _ => panic!("Expected ModelChanged events"),
    }
}
