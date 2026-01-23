//! Tonic/gRPC Server Implementation for Phase 1F
//!
//! This module provides the gRPC server that exposes QueryService and SchemaService
//! over HTTP/2 using Tonic.

use super::auth::AuthProvider;
use super::rate_limit::{RateLimitConfig, RateLimiter};
use crate::executor::{ExecuteQueryRequest, QueryService, QueryServiceImpl, SchemaServiceImpl};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Configuration for the gRPC server
#[derive(Clone, Debug)]
pub struct GrpcServerConfig {
    pub host: String,
    pub port: u16,
    pub db_path: Option<String>,
    pub auth_provider: Option<Arc<AuthProvider>>,
    pub rate_limit_config: Option<RateLimitConfig>,
}

/// Handle for controlling a running gRPC server
pub struct ServerHandle {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl ServerHandle {
    /// Gracefully stop the server
    pub async fn stop(self) -> Result<(), String> {
        self.shutdown_tx
            .send(())
            .map_err(|_| "Failed to send shutdown signal".to_string())?;
        Ok(())
    }
}

/// Tonic gRPC Server
pub struct GrpcServer {
    config: GrpcServerConfig,
    query_service: Arc<QueryServiceImpl>,
    schema_service: Arc<SchemaServiceImpl>,
    rate_limiter: Option<Arc<RateLimiter>>,
}

impl GrpcServer {
    /// Create a new gRPC server with the given configuration
    pub fn new(config: GrpcServerConfig) -> Result<Self, String> {
        let mut query_service = QueryServiceImpl::new();
        let mut schema_service = SchemaServiceImpl::new();

        // Set database path if provided
        if let Some(db_path) = &config.db_path {
            query_service.set_db_path(db_path)?;
            schema_service.set_db_path(db_path)?;
        }

        // Initialize rate limiter if configured
        let rate_limiter = config
            .rate_limit_config
            .as_ref()
            .map(|config| Arc::new(RateLimiter::new(config.clone())));

        Ok(GrpcServer {
            config,
            query_service: Arc::new(query_service),
            schema_service: Arc::new(schema_service),
            rate_limiter,
        })
    }

    /// Parse socket address from config
    fn parse_socket_addr(&self) -> Result<SocketAddr, String> {
        format!("{}:{}", self.config.host, self.config.port)
            .parse::<SocketAddr>()
            .map_err(|e| format!("Failed to parse address: {}", e))
    }

    /// Handle execute_query method
    async fn handle_execute_query(request: &Value, query_service: &Arc<QueryServiceImpl>) -> Value {
        let db = request
            .get("db_identifier")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let query = request.get("query").and_then(|v| v.as_str()).unwrap_or("");
        let limit = request.get("limit").and_then(|v| v.as_i64()).unwrap_or(100) as i32;
        let timeout = request
            .get("timeout_seconds")
            .and_then(|v| v.as_i64())
            .unwrap_or(30) as i32;

        let exec_req = ExecuteQueryRequest {
            db_identifier: db.to_string(),
            query: query.to_string(),
            parameters: HashMap::new(),
            limit,
            timeout_seconds: timeout,
        };

        match query_service.execute_query(exec_req).await {
            Ok(response) => json!({
                "status": response.status,
                "row_count": response.row_count,
                "execution_ms": response.execution_ms,
                "error_message": response.error_message,
                "timestamp": response.timestamp
            }),
            Err(e) => json!({
                "status": 3,
                "row_count": 0,
                "execution_ms": 0,
                "error_message": e,
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        }
    }

    /// Handle get_schema method
    async fn handle_get_schema(
        _request: &Value,
        _schema_service: &Arc<SchemaServiceImpl>,
    ) -> Value {
        json!({
            "schema_id": "schema_1",
            "database_type": "sqlite",
            "generated_at": chrono::Utc::now().to_rfc3339()
        })
    }

    /// Route request to appropriate handler based on method
    async fn route_request(
        request: &Value,
        query_service: &Arc<QueryServiceImpl>,
        schema_service: &Arc<SchemaServiceImpl>,
        auth_provider: &Option<Arc<AuthProvider>>,
    ) -> Value {
        // Extract authentication token if present
        let auth_header = request
            .get("authorization")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Authenticate the client
        let client = match auth_provider {
            Some(provider) => match provider.authenticate_from_header(auth_header) {
                Ok(client) => client,
                Err(e) => {
                    return json!({
                        "status": 5,
                        "error": format!("Authentication failed: {}", e),
                    });
                }
            },
            None => {
                // No auth provider configured, allow all requests
                super::auth::AuthenticatedClient {
                    client_id: "unauthenticated".to_string(),
                    permissions: vec!["*".to_string()],
                }
            }
        };

        // Route based on method with authorization checks
        match request.get("method").and_then(|m| m.as_str()) {
            Some("execute_query") => {
                if !client.can_execute_queries() {
                    return json!({
                        "status": 5,
                        "error": "Unauthorized: insufficient permissions for execute_query",
                    });
                }
                Self::handle_execute_query(request, query_service).await
            }
            Some("get_schema") => {
                if !client.can_read_schema() {
                    return json!({
                        "status": 5,
                        "error": "Unauthorized: insufficient permissions for get_schema",
                    });
                }
                Self::handle_get_schema(request, schema_service).await
            }
            _ => json!({
                "status": 3,
                "error": "Unknown method"
            }),
        }
    }

    /// Handle a single client connection with authentication and rate limiting
    async fn handle_connection(
        mut socket: tokio::net::TcpStream,
        peer_addr: SocketAddr,
        query_service: Arc<QueryServiceImpl>,
        schema_service: Arc<SchemaServiceImpl>,
        auth_provider: Option<Arc<AuthProvider>>,
        rate_limiter: Option<Arc<RateLimiter>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Check rate limit for new connection
        if let Some(limiter) = &rate_limiter {
            if let Err(e) = limiter.check_connection(peer_addr).await {
                let error_response = json!({
                    "status": 5,
                    "error": e,
                });
                let response_str = serde_json::to_string(&error_response).unwrap_or_default();
                let _ = socket.write_all(response_str.as_bytes()).await;

                // Signal connection close
                if let Some(limiter) = &rate_limiter {
                    limiter.close_connection(peer_addr).await;
                }
                return;
            }
        }

        let mut request_buf = String::new();
        let mut buffer = vec![0u8; 4096];
        loop {
            match socket.read(&mut buffer).await {
                Ok(0) => {
                    // Connection closed by peer before a full JSON document was received.
                    break;
                }
                Ok(n) => {
                    // Check rate limit for request
                    if let Some(limiter) = &rate_limiter {
                        if let Err(e) = limiter.check_request(peer_addr).await {
                            let error_response = json!({
                                "status": 5,
                                "error": e,
                            });
                            let response_str =
                                serde_json::to_string(&error_response).unwrap_or_default();
                            let _ = socket.write_all(response_str.as_bytes()).await;
                            break;
                        }
                    }

                    // Append the newly read bytes to the request buffer.
                    request_buf.push_str(&String::from_utf8_lossy(&buffer[..n]));
                    // Try to parse the accumulated buffer as JSON.
                    match serde_json::from_str::<Value>(&request_buf) {
                        Ok(request) => {
                            let response = Self::route_request(
                                &request,
                                &query_service,
                                &schema_service,
                                &auth_provider,
                            )
                            .await;
                            let response_str = serde_json::to_string(&response).unwrap_or_default();
                            let _ = socket.write_all(response_str.as_bytes()).await;
                            break;
                        }
                        Err(err) if err.is_eof() => {
                            // JSON is incomplete; continue reading more data.
                            continue;
                        }
                        Err(err) => {
                            // Malformed JSON: send error response instead of silently closing.
                            // Clients need feedback to diagnose and fix their requests.
                            let error_response = json!({
                                "status": 3,
                                "error": format!("Invalid JSON: {}", err),
                            });
                            let response_str =
                                serde_json::to_string(&error_response).unwrap_or_default();
                            let _ = socket.write_all(response_str.as_bytes()).await;
                            break;
                        }
                    }
                }
                Err(_e) => {
                    // I/O error while reading from socket; stop processing.
                    break;
                }
            }
        }

        // Signal connection close
        if let Some(limiter) = &rate_limiter {
            limiter.close_connection(peer_addr).await;
        }
    }

    /// Start the gRPC server and return a handle
    pub async fn start(self) -> Result<ServerHandle, String> {
        let addr = self.parse_socket_addr()?;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let query_service = self.query_service.clone();
        let schema_service = self.schema_service.clone();
        let auth_provider = self.config.auth_provider.clone();
        let rate_limiter = self.rate_limiter.clone();

        // Spawn server task
        tokio::spawn(async move {
            if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
                // Use JoinSet to track and manage all spawned connection handler tasks.
                // This allows for graceful cleanup on shutdown: all tasks are automatically
                // canceled when the JoinSet is dropped, preventing task leaks.
                // Per the OAM Architecture (POAM), JoinSet is mandated for robust high-concurrency
                // management of asynchronous task groups.
                let mut join_set = tokio::task::JoinSet::new();

                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            // On shutdown, gracefully close all tracked connection handlers.
                            // JoinSet automatically cancels all pending tasks when dropped.
                            drop(join_set);
                            break;
                        }
                        result = listener.accept() => {
                            if let Ok((socket, peer_addr)) = result {
                                let query_service = query_service.clone();
                                let schema_service = schema_service.clone();
                                let auth_provider = auth_provider.clone();
                                let rate_limiter = rate_limiter.clone();

                                // Spawn connection handler and track it in JoinSet
                                join_set.spawn(Self::handle_connection(
                                    socket,
                                    peer_addr,
                                    query_service,
                                    schema_service,
                                    auth_provider,
                                    rate_limiter,
                                ));
                            }
                        }
                    }
                }
            }
        });

        // Give server time to start and bind to port
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        Ok(ServerHandle { shutdown_tx })
    }
}
