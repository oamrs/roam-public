//! Composable mapper trait for multiple data source backends
//!
//! This module defines the Mapper trait that enables pluggable implementations:
//! - TCP (JSON-RPC)
//! - gRPC (Tonic)
//! - Future implementations (direct DB access, etc.)
//!
//! The composition pattern allows multiple mappers to coexist and be used
//! interchangeably by the OAM executor.

use crate::executor::{
    ExecuteQueryRequest, ExecuteQueryResponse, QueryService, ValidateQueryRequest,
    ValidationResponse,
};
use async_trait::async_trait;

/// Trait for pluggable data source mappers
///
/// Implementations should handle routing queries to different backends
/// (TCP/JSON-RPC, gRPC, direct database, etc.) while maintaining a consistent interface.
///
/// # Example
/// ```ignore
/// // Any mapper can be used interchangeably
/// let mapper: Box<dyn Mapper> = Box::new(LocalMapper::new("test.db")?);
/// let result = mapper.execute_query(request).await?;
/// ```
#[async_trait]
pub trait Mapper: Send + Sync {
    /// Validate a query without executing it
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String>;

    /// Execute a validated query and return results
    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String>;
}

/// TCP (JSON-RPC) mapper - delegates to existing JsonRpcClient
///
/// Routes requests to a remote OAM server via JSON-RPC over TCP
pub struct TcpMapper {
    client: crate::tcp::JsonRpcClient,
}

impl TcpMapper {
    /// Create a new TCP mapper with the given client
    pub fn new(client: crate::tcp::JsonRpcClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Mapper for TcpMapper {
    async fn validate_query(
        &self,
        _request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        // For now, this is a placeholder
        // Full implementation would use the TCP client to validate queries
        Ok(ValidationResponse {
            valid: true,
            error_message: String::new(),
        })
    }

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        let timeout_seconds = if request.timeout_seconds > 0 {
            request.timeout_seconds
        } else {
            30
        };

        let result = self
            .client
            .execute_query(
                &request.db_identifier,
                &request.query,
                request.limit,
                timeout_seconds,
            )
            .await?;

        // Convert TCP QueryResponse to ExecuteQueryResponse
        Ok(ExecuteQueryResponse {
            status: result.status,
            row_count: result.row_count,
            execution_ms: result.execution_ms,
            error_message: result.error_message,
            timestamp: result.timestamp,
        })
    }
}

/// Local mapper - direct use of QueryServiceImpl
///
/// Provides direct access to the OAM executor logic without network overhead.
/// Suitable for in-process usage where performance is critical.
pub struct LocalMapper {
    query_service: crate::executor::QueryServiceImpl,
}

impl LocalMapper {
    /// Create a new local mapper with the given database path
    pub fn new(db_path: &str) -> Result<Self, String> {
        let mut query_service = crate::executor::QueryServiceImpl::new();
        query_service.set_db_path(db_path)?;

        Ok(Self { query_service })
    }
}

#[async_trait]
impl Mapper for LocalMapper {
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        self.query_service.validate_query(request).await
    }

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        self.query_service.execute_query(request).await
    }
}

/// gRPC mapper - delegates to remote Tonic server
///
/// Routes requests to a remote OAM server via gRPC/Tonic.
/// Maintains persistent client connections for efficient RPC calls.
#[derive(Clone, Debug)]
pub struct GrpcMapper {
    query_client:
        roam_proto::v1::query::query_service_client::QueryServiceClient<tonic::transport::Channel>,
}

impl GrpcMapper {
    /// Create a new gRPC mapper for the given server address
    ///
    /// # Arguments
    /// * `addr` - Server address (e.g., "http://localhost:50051")
    ///
    /// # Returns
    /// Result containing the mapper or connection error
    ///
    /// # Example
    /// ```ignore
    /// let mapper = GrpcMapper::new("http://localhost:50051").await?;
    /// let result = mapper.execute_query(request).await?;
    /// ```
    pub async fn new(addr: &str) -> Result<Self, String> {
        let channel = tonic::transport::Channel::from_shared(addr.to_string())
            .map_err(|e| format!("Invalid server address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("Failed to connect to gRPC server: {}", e))?;

        let query_client =
            roam_proto::v1::query::query_service_client::QueryServiceClient::new(channel);

        Ok(Self { query_client })
    }
}

#[async_trait]
impl Mapper for GrpcMapper {
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        let mut client = self.query_client.clone();

        let proto_request = roam_proto::v1::query::ValidateQueryRequest {
            db_identifier: request.db_identifier,
            query: request.query,
        };

        let response = client
            .validate_query(tonic::Request::new(proto_request))
            .await
            .map_err(|e| format!("gRPC call failed: {}", e))?;

        let proto_response = response.into_inner();
        Ok(ValidationResponse {
            valid: proto_response.valid,
            error_message: proto_response.error_message,
        })
    }

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        let mut client = self.query_client.clone();

        let proto_request = roam_proto::v1::query::ExecuteQueryRequest {
            db_identifier: request.db_identifier,
            query: request.query,
            limit: request.limit,
            timeout_seconds: request.timeout_seconds,
        };

        let response = client
            .execute_query(tonic::Request::new(proto_request))
            .await
            .map_err(|e| format!("gRPC call failed: {}", e))?;

        let proto_response = response.into_inner();
        Ok(ExecuteQueryResponse {
            status: proto_response.status,
            row_count: proto_response.row_count,
            execution_ms: proto_response.execution_ms,
            error_message: proto_response.error_message,
            timestamp: proto_response.timestamp,
        })
    }
}
