use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Event payload for CRITICAL status changes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CriticalStatusEvent {
    pub entity_type: String,
    pub entity_id: String,
    pub status: String,
    pub timestamp: String,
}

/// Generalized Event enum supporting multiple event types
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
}

impl Event {
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

    /// Get the event type as a string
    pub fn event_type(&self) -> &str {
        match self {
            Event::StatusChange { .. } => "StatusChange",
            Event::ColumnChange { .. } => "ColumnChange",
            Event::ConstraintViolation { .. } => "ConstraintViolation",
        }
    }

    /// Get event metadata as a HashMap
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
        }

        map
    }
}

/// The callback signature for event subscribers
type SubscriberFn = Box<dyn Fn(&Event) + Send + 'static>;

/// A map of unique IDs to individual subscriber callbacks
type SubscriberMap = HashMap<usize, SubscriberFn>;

/// A map of event types (as Strings) to a list of interested callbacks
type TypeSubscriberMap = HashMap<String, Vec<SubscriberFn>>;

/// Global event bus for testing (will be replaced with actual event system)
pub struct EventBus {
    events: Mutex<Vec<CriticalStatusEvent>>,
    generic_events: Mutex<Vec<Event>>,
    persisted_log: Mutex<Vec<Event>>,
    subscribers: Mutex<SubscriberMap>,
    type_subscribers: Mutex<TypeSubscriberMap>,
    next_subscriber_id: AtomicUsize,
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
            .map(|mut guard| guard.clear())
    }

    /// Dispatch a generic event to the bus
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
        {
            let subscribers = self
                .subscribers
                .lock()
                .map_err(|e| format!("Failed to acquire lock: {}", e))?;
            for (_id, callback) in subscribers.iter() {
                callback(event);
            }
        }

        // Notify type-specific subscribers
        {
            let type_subs = self
                .type_subscribers
                .lock()
                .map_err(|e| format!("Failed to acquire lock: {}", e))?;
            if let Some(callbacks) = type_subs.get(event.event_type()) {
                for callback in callbacks {
                    callback(event);
                }
            }
        }

        Ok(())
    }

    /// Get all generic events from the bus
    pub fn all_events(&self) -> Result<Vec<Event>, String> {
        self.generic_events
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    /// Filter events by type
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

    /// Get all persisted events from the log
    pub fn persisted_events(&self) -> Result<Vec<Event>, String> {
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    /// Load events from the persistent log
    pub fn load_from_log(&self) -> Result<Vec<Event>, String> {
        self.persisted_log
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))
            .map(|guard| guard.clone())
    }

    /// Register a subscriber callback for all events
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

    /// Unregister a subscriber by ID
    pub fn unregister_subscriber(&self, subscriber_id: usize) -> Result<(), String> {
        self.subscribers
            .lock()
            .map_err(|e| format!("Failed to acquire lock: {}", e))?
            .remove(&subscriber_id);
        Ok(())
    }

    /// Register a subscriber callback for a specific event type
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

/// Global event bus instance for dispatching critical status events
static GLOBAL_EVENT_BUS: Lazy<EventBus> = Lazy::new(EventBus::new);

/// Get reference to the global event bus
pub fn get_event_bus() -> &'static EventBus {
    &GLOBAL_EVENT_BUS
}

/// Trait for models that support CRITICAL status event interception
pub trait HasCriticalStatus {
    fn get_status(&self) -> String;
    fn get_entity_type(&self) -> String;
    fn get_entity_id(&self) -> String;
    fn get_timestamp(&self) -> String;
}

/// ActiveModelBehavior hook for automatic CRITICAL event dispatch
pub struct CriticalModelBehavior;

impl CriticalModelBehavior {
    /// Hook called after a model is saved to the database
    /// Only dispatches events for models with CRITICAL status.
    /// Non-CRITICAL statuses are silently ignored.
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
