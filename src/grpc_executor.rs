use crate::executor::{
    ExecuteQueryRequest, GetSchemaRequest as ExecGetSchemaRequest,
    GetTableRequest as ExecGetTableRequest, QueryService, QueryServiceImpl, SchemaService,
    SchemaServiceImpl, ValidateQueryRequest,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{transport::Server, Request, Response, Status};

use roam_proto::v1::query::{
    query_service_server::{QueryService as ProtoQueryService, QueryServiceServer},
    ExecuteQueryRequest as ProtoExecuteQueryRequest,
    ExecuteQueryResponse as ProtoExecuteQueryResponse,
    ValidateQueryRequest as ProtoValidateQueryRequest, ValidationResponse,
};
use roam_proto::v1::schema::{
    schema_service_server::{SchemaService as ProtoSchemaService, SchemaServiceServer},
    GetSchemaRequest, GetSchemaResponse, GetTableRequest, GetTableResponse,
};

pub struct GrpcExecutor {
    query_service: Arc<Mutex<QueryServiceImpl>>,
    schema_service: Arc<Mutex<SchemaServiceImpl>>,
}

impl GrpcExecutor {
    pub fn new(db_path: &str) -> Result<Self, String> {
        let mut query_service = QueryServiceImpl::new();
        let mut schema_service = SchemaServiceImpl::new();

        query_service.set_db_path(db_path)?;
        schema_service.set_db_path(db_path)?;

        Ok(Self {
            query_service: Arc::new(Mutex::new(query_service)),
            schema_service: Arc::new(Mutex::new(schema_service)),
        })
    }

    pub async fn start_server(
        self,
        addr: &str,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn std::error::Error>> {
        let addr_parsed = addr.parse()?;

        let query_svc = GrpcQueryServiceImpl {
            inner: self.query_service.clone(),
        };
        let schema_svc = GrpcSchemaServiceImpl {
            inner: self.schema_service.clone(),
        };

        let handle = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(QueryServiceServer::new(query_svc))
                .add_service(SchemaServiceServer::new(schema_svc))
                .serve(addr_parsed)
                .await;
        });

        Ok(handle)
    }
}

struct GrpcQueryServiceImpl {
    inner: Arc<Mutex<QueryServiceImpl>>,
}

#[tonic::async_trait]
impl ProtoQueryService for GrpcQueryServiceImpl {
    async fn execute_query(
        &self,
        request: Request<ProtoExecuteQueryRequest>,
    ) -> Result<Response<ProtoExecuteQueryResponse>, Status> {
        let req = request.into_inner();

        let exec_req = ExecuteQueryRequest {
            db_identifier: req.db_identifier,
            query: req.query,
            parameters: Default::default(),
            limit: req.limit,
            timeout_seconds: req.timeout_seconds,
        };

        let service = self.inner.lock().await;
        match service.execute_query(exec_req).await {
            Ok(resp) => Ok(Response::new(ProtoExecuteQueryResponse {
                status: resp.status,
                row_count: resp.row_count,
                execution_ms: resp.execution_ms,
                error_message: resp.error_message,
                timestamp: resp.timestamp,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn validate_query(
        &self,
        request: Request<ProtoValidateQueryRequest>,
    ) -> Result<Response<ValidationResponse>, Status> {
        let req = request.into_inner();

        let exec_req = ValidateQueryRequest {
            db_identifier: req.db_identifier,
            query: req.query,
            parameters: Default::default(),
        };

        let service = self.inner.lock().await;
        match service.validate_query(exec_req).await {
            Ok(resp) => Ok(Response::new(ValidationResponse {
                valid: resp.valid,
                error_message: resp.error_message,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }
}

struct GrpcSchemaServiceImpl {
    inner: Arc<Mutex<SchemaServiceImpl>>,
}

#[tonic::async_trait]
impl ProtoSchemaService for GrpcSchemaServiceImpl {
    async fn get_schema(
        &self,
        request: Request<GetSchemaRequest>,
    ) -> Result<Response<GetSchemaResponse>, Status> {
        let req = request.into_inner();

        let exec_req = ExecGetSchemaRequest {
            db_identifier: req.db_identifier,
        };

        let service = self.inner.lock().await;
        match service.get_schema(exec_req).await {
            Ok(schema_resp) => Ok(Response::new(GetSchemaResponse {
                schema_id: schema_resp.schema_id,
                database_type: schema_resp.database_type,
                generated_at: schema_resp.generated_at,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();

        let exec_req = ExecGetTableRequest {
            db_identifier: req.db_identifier,
            table_name: req.table_name,
        };

        let service = self.inner.lock().await;
        match service.get_table(exec_req).await {
            Ok(table_resp) => Ok(Response::new(GetTableResponse {
                generated_at: table_resp.generated_at,
            })),
            Err(e) => Err(Status::internal(e)),
        }
    }
}
