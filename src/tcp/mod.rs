//! JSON-RPC Server and Client Implementation

pub mod auth;
pub mod client;
pub mod rate_limit;
pub mod server;

pub use auth::{ApiKeyAuth, AuthError, AuthProvider, AuthenticatedClient, TokenAuth};
pub use client::{JsonRpcClient, QueryResponse, SchemaResponse};
pub use rate_limit::{RateLimitConfig, RateLimiter, RateLimiterStats};
pub use server::{JsonRpcServer, JsonRpcServerConfig, ServerHandle};
