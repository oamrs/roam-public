# oam

The **OAM (Object Agent Mapping)** framework for Rust — distributed query execution, schema introspection, event-driven architecture, and LLM agent integration.

## Overview

`oam` is the core runtime library for building systems where AI agents interact with relational data. It provides:

- Multiple query backends (local SQLite, TCP JSON-RPC, gRPC) behind a single `Mapper` trait
- High-throughput async execution engine with priority queues and result tracking
- Database schema introspection with JSON Schema output for LLM tool calling
- Event bus for model-change propagation across the application
- Built-in gRPC server (via Tonic) for polyglot client support

## Quick Start

```toml
[dependencies]
oam = "0.1"
tokio = { version = "1", features = ["full"] }
```

### Local SQLite query

```rust
use oam::mapper::{LocalMapper, Mapper, ExecuteQueryRequest};

let mapper = LocalMapper::new("my_db.sqlite")?;
let response = mapper.execute_query(ExecuteQueryRequest {
    db_identifier: "my_db".into(),
    query: "SELECT id, name FROM organizations LIMIT 10".into(),
    limit: 10,
    timeout_seconds: 5,
}).await?;
```

### Remote gRPC query

```rust
use oam::mapper::{GrpcMapper, Mapper, ExecuteQueryRequest};

let mapper = GrpcMapper::new("http://localhost:50051").await?;
let response = mapper.execute_query(ExecuteQueryRequest { .. }).await?;
```

### Start a gRPC server

```rust
use oam::grpc_executor::GrpcExecutor;

let executor = GrpcExecutor::new("my_db.sqlite")?;
executor.start_server("127.0.0.1:50051").await?;
```

## Modules

| Module | Description |
|--------|-------------|
| `oam::execution_engine` | Async execution engine with priority queues, metrics, and result tracking |
| `oam::executor` | `QueryService` and `SchemaService` for executing and validating SQL |
| `oam::grpc_executor` | Standalone gRPC server wrapping executor services |
| `oam::mapper` | `Mapper` trait + `LocalMapper`, `TcpMapper`, `GrpcMapper` implementations |
| `oam::mirror` | SeaORM entity introspection → JSON Schema for LLM tool definitions |
| `oam::interceptor` | Global `EventBus` for model-change events |
| `oam::tcp` | JSON-RPC over TCP server and client |
| `oam::policy_engine` | RBAC policy evaluation |

## The `Mapper` Trait

All backends share the same interface:

```rust
#[async_trait]
pub trait Mapper: Send + Sync {
    async fn validate_query(&self, req: ValidateQueryRequest) -> Result<ValidationResponse, String>;
    async fn execute_query(&self, req: ExecuteQueryRequest)   -> Result<ExecuteQueryResponse, String>;
}
```

Choose the backend at runtime without changing application logic:

```rust
let mapper: Box<dyn Mapper> = match config.backend {
    Backend::Local   => Box::new(LocalMapper::new(db_path)?),
    Backend::Tcp     => Box::new(TcpMapper::new(client).await?),
    Backend::Remote  => Box::new(GrpcMapper::new(remote_addr).await?),
};
```

## LLM Schema Generation

Use `oam-schema`'s `LlmSchema` derive together with `oam::mirror` to expose your data models to LLM agents:

```rust
use oam_schema::LlmSchema;
use schemars::JsonSchema;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, JsonSchema, LlmSchema)]
pub struct Organization {
    pub id: String,
    pub name: String,
    pub parent_id: Option<String>,
}

// Pass to an OpenAI / Anthropic tool definition
let schema_json = serde_json::to_value(Organization::llm_schema()).unwrap();
```

## Running Tests

```bash
# All tests
make test FILTER=roam-public

# Unit tests only
cd libraries/roam-public && cargo test --test unit

# Integration tests
cd libraries/roam-public && cargo test --test integration
```

## Related Crates

| Crate | Description |
|-------|-------------|
| [`oam-proto`](https://crates.io/crates/oam-proto) | Shared gRPC/protobuf definitions |
| [`oam-schema`](https://crates.io/crates/oam-schema) | `LlmSchema` derive macro |

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT License](LICENSE-MIT) at your option.
