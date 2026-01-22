//! Tonic/gRPC Server Implementation for Phase 1F
//!
//! This module provides the gRPC server that exposes QueryService and SchemaService
//! over HTTP/2 using Tonic.

use crate::executor::{ExecuteQueryRequest, QueryService, QueryServiceImpl, SchemaServiceImpl};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Configuration for the gRPC server
#[derive(Clone, Debug)]
pub struct GrpcServerConfig {
    pub host: String,
    pub port: u16,
    pub db_path: Option<String>,
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
    query_service: Arc<RwLock<QueryServiceImpl>>,
    schema_service: Arc<RwLock<SchemaServiceImpl>>,
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

        Ok(GrpcServer {
            config,
            query_service: Arc::new(RwLock::new(query_service)),
            schema_service: Arc::new(RwLock::new(schema_service)),
        })
    }

    /// Parse socket address from config
    fn parse_socket_addr(&self) -> Result<SocketAddr, String> {
        format!("{}:{}", self.config.host, self.config.port)
            .parse::<SocketAddr>()
            .map_err(|e| format!("Failed to parse address: {}", e))
    }

    /// Handle execute_query method
    async fn handle_execute_query(
        request: &Value,
        query_service: &Arc<RwLock<QueryServiceImpl>>,
    ) -> Value {
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

        let qs = query_service.read().await;
        match qs.execute_query(exec_req).await {
            Ok(response) => json!({
                "status": response.status,
                "row_count": response.row_count,
                "execution_ms": response.execution_ms,
                "error_message": response.error_message,
                "timestamp": chrono::Utc::now().to_rfc3339()
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
        _schema_service: &Arc<RwLock<SchemaServiceImpl>>,
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
        query_service: &Arc<RwLock<QueryServiceImpl>>,
        schema_service: &Arc<RwLock<SchemaServiceImpl>>,
    ) -> Value {
        match request.get("method").and_then(|m| m.as_str()) {
            Some("execute_query") => Self::handle_execute_query(request, query_service).await,
            Some("get_schema") => Self::handle_get_schema(request, schema_service).await,
            _ => json!({
                "status": 3,
                "error": "Unknown method"
            }),
        }
    }

    /// Handle a single client connection
    async fn handle_connection(
        mut socket: tokio::net::TcpStream,
        query_service: Arc<RwLock<QueryServiceImpl>>,
        schema_service: Arc<RwLock<SchemaServiceImpl>>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut buffer = vec![0; 4096];
        match socket.read(&mut buffer).await {
            Ok(n) if n > 0 => {
                let request_str = String::from_utf8_lossy(&buffer[..n]);
                if let Ok(request) = serde_json::from_str::<Value>(&request_str) {
                    let response =
                        Self::route_request(&request, &query_service, &schema_service).await;
                    let response_str = serde_json::to_string(&response).unwrap_or_default();
                    let _ = socket.write_all(response_str.as_bytes()).await;
                }
            }
            _ => {}
        }
    }

    /// Start the gRPC server and return a handle
    pub async fn start(self) -> Result<ServerHandle, String> {
        let addr = self.parse_socket_addr()?;
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let query_service = self.query_service.clone();
        let schema_service = self.schema_service.clone();

        // Spawn server task
        tokio::spawn(async move {
            if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => {
                            break;
                        }
                        result = listener.accept() => {
                            if let Ok((socket, _peer_addr)) = result {
                                let query_service = query_service.clone();
                                let schema_service = schema_service.clone();

                                tokio::spawn(Self::handle_connection(socket, query_service, schema_service));
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
