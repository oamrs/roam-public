use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use once_cell::sync::Lazy;

/// Event payload for CRITICAL status changes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CriticalStatusEvent {
    pub entity_type: String,
    pub entity_id: String,
    pub status: String,
    pub timestamp: String,
}

/// Global event bus for testing (will be replaced with actual event system)
pub struct EventBus {
    events: Mutex<Vec<CriticalStatusEvent>>,
}

impl EventBus {
    pub fn new() -> Self {
        EventBus {
            events: Mutex::new(Vec::new()),
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
            .map(|mut guard| guard.clear())
    }
}

/// Global event bus instance for dispatching critical status events
static GLOBAL_EVENT_BUS: Lazy<EventBus> = Lazy::new(|| EventBus::new());

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
    /// 
    /// Only dispatches events for models with CRITICAL status.
    /// Non-CRITICAL statuses are silently ignored.
    pub async fn after_save<M>(model: M, _db: &impl std::any::Any, _insert: bool) -> Result<M, String>
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
