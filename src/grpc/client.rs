//! gRPC Client for Phase 1F
//!
//! This module provides a client to connect to the gRPC server and execute
//! schema and query operations over the network.

use serde_json::{json, Value};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::net::TcpStream;

/// Response from executing a query over gRPC
#[derive(Debug, Clone)]
pub struct QueryResponse {
    pub status: i32,
    pub row_count: i32,
    pub execution_ms: i32,
    pub error_message: String,
    pub timestamp: String,
}

/// Response from getting schema over gRPC
#[derive(Debug, Clone)]
pub struct SchemaResponse {
    pub schema_id: String,
    pub database_type: String,
    pub generated_at: String,
}

/// gRPC Client for connecting to the executor server
pub struct GrpcClient {
    address: String,
    connected: Arc<std::sync::atomic::AtomicBool>,
}

impl GrpcClient {
    /// Normalize address by stripping protocol prefix if present
    fn normalize_address(address: &str) -> &str {
        if let Some(stripped) = address.strip_prefix("http://") {
            return stripped;
        }
        if let Some(stripped) = address.strip_prefix("https://") {
            return stripped;
        }
        address
    }

    /// Connect to a gRPC server at the given address
    pub async fn connect(address: &str) -> Result<Self, String> {
        let addr_with_port = Self::normalize_address(address);

        // Try to resolve and connect
        let resolved_addr = Self::resolve_address(addr_with_port)?;
        Self::verify_connection(resolved_addr).await?;

        Ok(GrpcClient {
            address: addr_with_port.to_string(),
            connected: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    /// Resolve address to socket address
    fn resolve_address(addr: &str) -> Result<std::net::SocketAddr, String> {
        addr.to_socket_addrs()
            .map_err(|_| "Invalid address format".to_string())?
            .next()
            .ok_or_else(|| "Could not resolve address".to_string())
    }

    /// Verify connection to server
    async fn verify_connection(addr: std::net::SocketAddr) -> Result<(), String> {
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            TcpStream::connect(addr),
        )
        .await
        .map_err(|_| "Failed to connect to server".to_string())?
        .map_err(|_| "Failed to connect to server".to_string())?;
        Ok(())
    }

    /// Check if client is connected
    pub fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Send request to server and receive JSON response
    async fn send_request(&self, request: Value) -> Result<Value, String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = Self::resolve_address(self.address.as_str())?;
        let mut stream = TcpStream::connect(addr)
            .await
            .map_err(|_| "Failed to connect to server".to_string())?;

        let request_str = request.to_string();
        stream
            .write_all(request_str.as_bytes())
            .await
            .map_err(|_| "Failed to send request".to_string())?;

        let mut buffer = vec![0; 4096];
        let n = stream
            .read(&mut buffer)
            .await
            .map_err(|_| "Failed to read response".to_string())?;

        if n == 0 {
            return Err("Failed to read response".to_string());
        }

        let response_str = String::from_utf8_lossy(&buffer[..n]);
        serde_json::from_str::<Value>(&response_str)
            .map_err(|_| "Failed to parse response".to_string())
    }

    /// Get schema from server
    pub async fn get_schema(&self, db_identifier: &str) -> Result<SchemaResponse, String> {
        let request = json!({
            "method": "get_schema",
            "db_identifier": db_identifier
        });

        let response = self.send_request(request).await?;

        Ok(SchemaResponse {
            schema_id: response
                .get("schema_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            database_type: response
                .get("database_type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            generated_at: response
                .get("generated_at")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }

    /// Execute a query on the server
    pub async fn execute_query(
        &self,
        db_identifier: &str,
        query: &str,
        limit: i32,
        timeout_seconds: i32,
    ) -> Result<QueryResponse, String> {
        let request = json!({
            "method": "execute_query",
            "db_identifier": db_identifier,
            "query": query,
            "limit": limit,
            "timeout_seconds": timeout_seconds
        });

        let response = self.send_request(request).await?;

        Ok(QueryResponse {
            status: response.get("status").and_then(|v| v.as_i64()).unwrap_or(3) as i32,
            row_count: response
                .get("row_count")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32,
            execution_ms: response
                .get("execution_ms")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32,
            error_message: response
                .get("error_message")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            timestamp: response
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        })
    }
}
