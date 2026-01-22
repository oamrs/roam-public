//! gRPC Server and Client Implementation for Phase 1F
//!
//! This module provides the Tonic/gRPC server and client for network-based
//! communication with the OAM executor services.

pub mod client;
pub mod server;

pub use client::{GrpcClient, QueryResponse, SchemaResponse};
pub use server::{GrpcServer, GrpcServerConfig, ServerHandle};
