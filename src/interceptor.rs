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
}

/// Global event bus instance for dispatching critical status events
static GLOBAL_EVENT_BUS: Lazy<EventBus> = Lazy::new(|| EventBus::new());

/// Get reference to the global event bus
pub fn get_event_bus() -> &'static EventBus {
    &GLOBAL_EVENT_BUS
}
