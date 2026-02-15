//! Standalone runner for the ROAM Public Core Logic.
//! 
//! This binary spins up the gRPC services (Query, Schema, Agent) defined in `roam-public`
//! without the overhead of the full `roam-backend` (Rocket, OIDC, etc.).
//!
//! It is primarily used for:
//! 1. Integration testing of SDKs (Python, .NET, Node, etc.)
//! 2. Developing/Debugging the core OAM logic in isolation.
//!
//! Usage:
//! ```bash
//! cargo run --bin roam-grpc-server
//! ```
//! Environment Variables:
//! - `ROAM_GRPC_ADDR`: Address to bind to (default: "127.0.0.1:50051")
//! - `ROAM_DB_PATH`: Path to SQLite DB (default: temporary file)
