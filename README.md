# roam

The **roam** crate provides OAM (Object Agent Mapper) core functionality for distributed query execution, schema introspection, and event-driven architecture.

## Core Modules

### roam::execution_engine

High-throughput asynchronous execution engine for managing concurrent queries:

- **ExecutionEngine** - Orchestrates concurrent query execution with integrated connection pooling and JoinSet task management
- **QueryRequest** - Encapsulates requests with priority, unique ID, and timing metadata
- **QueryPriority** - Four-level priority system (Low, Normal, High, Critical)
- **ExecutionMetrics** - Advanced metrics tracking with latency histograms and per-database statistics
- **ConnectionPool** - Async connection pool management for SQLite with configurable limits
- **PoolStats** - Pool statistics including available/checked-out connection counts
- **DatabaseStats** - Per-database execution statistics and latency tracking
- **QueryResult** - Result container with status, output, error, and expiration information
- **ResultStatus** - Enum tracking pending/completed/failed/cancelled states
- **CancellationToken** - Task cancellation mechanism with state tracking

Features:
- Configurable concurrency limits: `ExecutionEngine::new(db_path, max_concurrent_queries)?`
- Unique request IDs for result tracking via UUID
- Priority-based request ordering (Critical > High > Normal > Low)
- Lock-free metrics: total queries, successful/failed counts, queue depth, active tasks
- Advanced metrics: success rates, latency percentiles (p95, p99), per-database stats
- Async connection pooling with semaphore-based concurrency control
- Connection acquisition with automatic release on drop
- Concurrent task execution via tokio::JoinSet with automatic metrics updates
- Spawn queries asynchronously with `engine.spawn_query(request)` for high-throughput execution
- Real-time latency tracking and per-database statistics collection
- Automatic database-specific performance monitoring
- Result retrieval by request ID: `engine.get_result(request_id).await`
- Blocking result retrieval with timeout: `engine.wait_for_result(request_id, 5000).await`
- Result status queries: `engine.result_status(request_id).await`
- Task cancellation: `engine.cancel_task(request_id).await` prevents completed tasks
- Cancellation token creation: `engine.create_cancellation_token(request_id).await`
- Cancellation status checks: `engine.is_task_cancelled(request_id).await`
- Automatic result cleanup for cancelled tasks
- Result expiration tracking: Automatic TTL (30ms default) on all results
- Expiration checks: `engine.is_result_expired(request_id).await`
- Garbage collection: `engine.garbage_collect_expired_results().await` returns count
- Result storage metrics: `engine.result_count().await` and `engine.garbage_collected_count().await`

### roam::executor

High-level query execution and schema services with multiple backend support:

- **QueryService** - Execute and validate SQL queries with parameter binding
- **SchemaService** - Introspect database schema and retrieve table metadata
- **Mapper Pattern** - Pluggable backends: LocalMapper (direct), TcpMapper (JSON-RPC), GrpcMapper (gRPC)

### roam::grpc_executor

gRPC server implementation using Tonic for distributed query access:

- **GrpcExecutor** - Standalone server wrapping QueryService and SchemaService
- **gRPC Services** - Proto-defined API for polyglot clients (Python, Go, .NET, etc.)
- **Async Architecture** - Tokio-based async/await throughout

Features:
- Start server on any address: `executor.start_server("127.0.0.1:50051").await?`
- Full QueryService RPC: `execute_query()`, `validate_query()`
- Full SchemaService RPC: `get_schema()`, `get_table()`
- Graceful shutdown support

### roam::mapper

Composable backend abstraction for distributed queries:

**Trait-based design:**
```rust
#[async_trait]
pub trait Mapper: Send + Sync {
    async fn validate_query(&self, request: ValidateQueryRequest) -> Result<ValidationResponse, String>;
    async fn execute_query(&self, request: ExecuteQueryRequest) -> Result<ExecuteQueryResponse, String>;
}
```

**Implementations:**

1. **LocalMapper** - Direct database access via embedded SQLite
   ```rust
   let mapper = LocalMapper::new("database.db")?;
   let result = mapper.execute_query(request).await?;
   ```

2. **TcpMapper** - Remote queries via JSON-RPC over TCP
   ```rust
   let mapper = TcpMapper::new(client).await?;
   let result = mapper.execute_query(request).await?;
   ```

3. **GrpcMapper** - Remote queries via gRPC with Tonic
   ```rust
   let mapper = GrpcMapper::new("http://localhost:50051").await?;
   let result = mapper.execute_query(request).await?;
   ```

**Composition Pattern:** Mix and match mappers in your application
```rust
let mapper: Box<dyn Mapper> = match config.backend {
    Backend::Local => Box::new(LocalMapper::new(db_path)?),
    Backend::Remote => Box::new(GrpcMapper::new(remote_addr).await?),
};
```

### roam::mirror

SeaORM Entity-to-Tool reflection for database schema introspection:

- **Table & Column Metadata**: Extracts all tables, columns with types, nullability, and constraints
- **Foreign Key Support**: Detects single-column and composite multi-column foreign keys with cascade actions
- **Enum Detection**: Extracts enum constraints from CHECK constraints
- **JSON Schema Generation**: Converts schema to JSON Schema for LLM consumption
- **Type Mapping**: SQL types ↔ JSON Schema instance types

### roam::interceptor

Event-driven architecture with global event bus:

- **EventBus** - Global event dispatch and subscription system
- **Event Types** - Extensible enum for different event categories
- **Subscribers** - Type-based filtering for efficient event handling
- **ModelChanged Events** - Track entity create/update actions

### roam::tcp

JSON-RPC over TCP implementation for legacy client support:

- **JsonRpcServer** - Standalone server with database path and auth provider configuration
- **JsonRpcClient** - Client for making JSON-RPC calls
- **Schema & Query Services** - Same interface as gRPC but over TCP

## Proto Definitions (roam-proto)

gRPC API contracts defined in Protocol Buffers:

### QueryService (v1.query.service.proto)

```protobuf
service QueryService {
  rpc ExecuteQuery (ExecuteQueryRequest) returns (ExecuteQueryResponse);
  rpc ValidateQuery (ValidateQueryRequest) returns (ValidationResponse);
}
```

Messages include parameters for database identifier, query string, limit, and timeout.

### SchemaService (v1.schema.service.proto)

```protobuf
service SchemaService {
  rpc GetSchema (GetSchemaRequest) returns (GetSchemaResponse);
  rpc GetTable (GetTableRequest) returns (GetTableResponse);
}
```

Enables schema introspection and table metadata retrieval over gRPC.

## Backend Model Integration

All backend domain models can emit events on creation/update:

```rust
#[derive(Clone, Debug, DeriveEntityModel, Eq, LlmSchema, JsonSchema)]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: String,
    // ... fields
}

impl ActiveModel {
    pub fn after_save_internal(model: &Model, insert: bool) -> Result<(), String> {
        let event = Event::model_changed(
            "entity_type".to_string(),
            model.id.clone(),
            if insert { "created" } else { "updated" }.to_string(),
        );
        get_event_bus().dispatch_generic(&event)
    }
}
```

Benefits:
- **LlmSchema derive** - Compile-time JSON schema generation
- **JsonSchema derive** - OpenAPI-compatible schema
- **After-save hooks** - Automatic event emission for audit trails
- **Type-safe events** - Structured event dispatch via EventBus

## Running Tests

### All Tests
```bash
make test
```

### Unit Tests (roam-public)
```bash
cd libraries/roam-public
cargo test --test unit
```

### Integration Tests (roam-public)
```bash
cd libraries/roam-public
cargo test --test integration
```

### Specific Test Suite
```bash
cargo test --test integration grpc_executor_tests
cargo test --test integration tcp_tests
cargo test --test unit executor_tests
```

## Test Coverage

**roam-public:**
- 137 unit tests (executor, mapper, mirror, interceptor, auth, rate limiting, execution_engine)
  - 45 execution_engine tests (ExecutionEngine, QueryRequest, QueryPriority, Metrics, ConnectionPool, JoinSet spawning, Enhanced metrics, Task result collection, Task cancellation, Result expiration & garbage collection)
- 53 integration tests (gRPC, TCP, executor)
- Total: 190 tests

**roam-schema:**
- 4 unit tests (LlmSchema macro derive)

**Backend (services/backend):**
- 40 unit tests (model creation, validation, schema generation, after-save hooks)

**Total: 234 tests across all crates**

## Architecture Highlights

### Asynchronous Execution Engine (Phase 1-7 Complete)
- ExecutionEngine for high-throughput concurrent query management
- QueryPriority system for request ordering (Critical > High > Normal > Low)
- Lock-free metrics via Arc<Atomic*> for performance
- Integrated async ConnectionPool with semaphore-based concurrency control
- tokio::JoinSet task management for concurrent query execution with automatic result collection
- Advanced metrics: success rates, latency percentiles, per-database statistics
- Real-time performance monitoring and database-specific insights
- Result status tracking: Pending → Completed/Failed/Cancelled state machine
- Synchronous and asynchronous result retrieval with timeout support
- UUID-based result lookup for distributed task coordination
- Task cancellation with CancellationToken: prevents cancellation of completed tasks
- Automatic result cleanup and status marking for cancelled tasks
- Result expiration with automatic TTL (30ms default) and garbage collection
- Memory-efficient result storage with automatic cleanup of expired results

### Distributed Query Execution
- Single service interface works with local, TCP, or gRPC backends
- Transparent client location abstraction
- Full async/await support throughout

### Event-Driven Models
- ModelChanged events for all entity operations
- Type-based event subscriptions
- Global EventBus for cross-cutting concerns (audit, webhooks, sync)

### Type-Safe gRPC
- Proto definitions ensure schema compatibility
- Compile-time code generation via Tonic
- Polyglot client support (Python, Go, .NET, JavaScript)

### Composable Mappers
- Trait-based abstraction for easy extension
- Mix multiple backends in single application
- Consistent error handling and timeouts

## Future Enhancements

- PostgreSQL support in mirror module
- Distributed transaction support across mappers
- Caching layer for schema queries
- Additional authentication providers
- Performance metrics and tracing
- Async connection pooling

Run tests:

```bash
cd roam-public
cargo test
```
