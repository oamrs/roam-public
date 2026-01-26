pub mod executor;
pub mod ffi;
pub mod generated;
pub mod interceptor;
pub mod mirror;
pub mod tcp;

// Re-export commonly used types
pub use executor::{QueryService, QueryServiceImpl, QueryStatus, SchemaService, SchemaServiceImpl};
pub use interceptor::{
    get_event_bus, CriticalModelBehavior, CriticalStatusEvent, Event, EventBus, HasCriticalStatus,
};
pub use mirror::{
    introspect_sqlite_path, Column, CompositeForeignKey, ForeignKey, SchemaModel, Table,
};
pub use tcp::{
    ApiKeyAuth, AuthProvider, AuthenticatedClient, JsonRpcClient, JsonRpcServer,
    JsonRpcServerConfig, RateLimitConfig, RateLimiter, RateLimiterStats, TokenAuth,
};
