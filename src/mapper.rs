use crate::executor::{
    ExecuteQueryRequest, ExecuteQueryResponse, QueryService, ValidateQueryRequest,
    ValidationResponse,
};
use async_trait::async_trait;

#[async_trait]
pub trait Mapper: Send + Sync {
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String>;

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String>;
}

pub struct TcpMapper {
    client: crate::tcp::JsonRpcClient,
}

impl TcpMapper {
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
        Ok(ValidationResponse {
            valid: true,
            error_message: String::new(),
            event_metadata: Default::default(),
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

        Ok(ExecuteQueryResponse {
            status: result.status,
            row_count: result.row_count,
            execution_ms: result.execution_ms,
            error_message: result.error_message,
            timestamp: result.timestamp,
        })
    }
}

pub struct LocalMapper {
    query_service: crate::executor::QueryServiceImpl,
}

impl LocalMapper {
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

#[derive(Clone, Debug)]
pub struct GrpcMapper {
    query_client:
        roam_proto::v1::query::query_service_client::QueryServiceClient<tonic::transport::Channel>,
}

impl GrpcMapper {
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
            event_metadata: Default::default(),
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
