pub mod execution_engine;
pub mod executor;
pub mod ffi;
pub mod generated;
pub mod grpc_executor;
pub mod interceptor;
pub mod mapper;
pub mod mirror;
pub mod tcp;

// Re-export commonly used types
pub use execution_engine::{
    ConnectionPool, ExecutionEngine, ExecutionMetrics, PoolStats, QueryPriority, QueryRequest,
};
pub use executor::{QueryService, QueryServiceImpl, QueryStatus, SchemaService, SchemaServiceImpl};
pub use grpc_executor::GrpcExecutor;
pub use interceptor::{
    get_event_bus, CriticalModelBehavior, CriticalStatusEvent, Event, EventBus, HasCriticalStatus,
};
pub use mapper::{GrpcMapper, LocalMapper, Mapper, TcpMapper};
pub use mirror::{
    introspect_sqlite_path, Column, CompositeForeignKey, ForeignKey, SchemaModel, Table,
};
pub use tcp::{
    ApiKeyAuth, AuthProvider, AuthenticatedClient, JsonRpcClient, JsonRpcServer,
    JsonRpcServerConfig, RateLimitConfig, RateLimiter, RateLimiterStats, TokenAuth,
};
