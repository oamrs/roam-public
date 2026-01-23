//! gRPC Server and Client Implementation for Phase 1F
//!
//! This module provides the Tonic/gRPC server and client for network-based
//! communication with the OAM executor services.

pub mod auth;
pub mod client;
pub mod rate_limit;
pub mod server;

pub use auth::{ApiKeyAuth, AuthError, AuthProvider, AuthenticatedClient, TokenAuth};
pub use client::{GrpcClient, QueryResponse, SchemaResponse};
pub use rate_limit::{RateLimitConfig, RateLimiter, RateLimiterStats};
pub use server::{GrpcServer, GrpcServerConfig, ServerHandle};
