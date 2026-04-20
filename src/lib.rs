pub mod access_policy;
pub mod execution_engine;
pub mod executor;
pub mod ffi;
pub mod generated;
pub mod grpc_executor;
pub mod handlers;
pub mod interceptor;
pub mod mapper;
pub mod mirror;
pub mod policy_engine;
pub mod runtime_context;
pub mod tcp;

// Re-export commonly used types
pub use access_policy::{
    AccessEnforcer, AccessPolicy, AccessPolicyProvider, ColumnPolicy, EnforcementOutcome, RowPolicy,
};
pub use execution_engine::{
    CancellationToken, ConnectionPool, ExecutionEngine, ExecutionMetrics, PoolStats, QueryPriority,
    QueryRequest, QueryResult, ResultStatus,
};
pub use executor::{
    MirrorProvider, QueryRuntimeAugmentation, QueryRuntimeAugmentor, QueryService,
    QueryServiceImpl, QueryStatus, SchemaService, SchemaServiceImpl, SqliteMirrorProvider,
};
pub use grpc_executor::GrpcExecutor;
pub use handlers::{
    AuditLogHandler, DataAccessEnforcedHandler, DataAccessSnapshot, DefaultHandlerChain,
    QueryMetricsHandler, QueryMetricsSnapshot, SessionActivityHandler, SharedHandler,
};
pub use interceptor::{
    get_event_bus, CriticalModelBehavior, CriticalStatusEvent, Event, EventBus, EventHandler,
    HandleOutcome, HasCriticalStatus, RuntimeAugmentationAuditRecord,
};
pub use mapper::{GrpcMapper, LocalMapper, Mapper, TcpMapper};
pub use mirror::{
    introspect_sqlite_path, Column, CompositeForeignKey, FieldMapping, ForeignKey, Index,
    SchemaModel, Table, Trigger, UniqueIndex, UserDefinedType,
};
pub use policy_engine::{
    AuthorizationContext, AuthorizedSubqueryShape, PolicyContext, PolicyDecision, PolicyEngine,
    SubqueryPolicy, ToolContract, ToolIntent,
};
pub use runtime_context::QueryRuntimeContext;
pub use tcp::{
    ApiKeyAuth, AuthProvider, AuthenticatedClient, JsonRpcClient, JsonRpcServer,
    JsonRpcServerConfig, RateLimitConfig, RateLimiter, RateLimiterStats, TokenAuth,
};
