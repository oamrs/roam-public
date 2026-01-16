//! OAM Executor: gRPC service definitions for Phase 1A
//!
//! This module provides trait definitions and message types for the OAM gRPC services.
//! Phase 1A focuses on infrastructure and service structure.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query parameter for parameterized queries
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryParameter {
    pub value: String,
    pub type_hint: String,
}

/// Query status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(i32)]
pub enum QueryStatus {
    Unspecified = 0,
    Success = 1,
    ValidationError = 2,
    ExecutionError = 3,
    Timeout = 4,
    Unauthorized = 5,
}

/// Schema service trait for database metadata operations
#[async_trait::async_trait]
pub trait SchemaService: Send + Sync {
    /// Get complete database schema
    async fn get_schema(&self, request: GetSchemaRequest) -> Result<GetSchemaResponse, String>;

    /// Get metadata for a specific table
    async fn get_table(&self, request: GetTableRequest) -> Result<GetTableResponse, String>;
}

/// Query service trait for query validation and execution
#[async_trait::async_trait]
pub trait QueryService: Send + Sync {
    /// Validate a SELECT query without executing it
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String>;

    /// Execute a validated SELECT query
    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String>;
}

// ============================================================================
// REQUEST/RESPONSE MESSAGE TYPES
// ============================================================================

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetSchemaRequest {
    pub db_identifier: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetSchemaResponse {
    pub schema_id: String,
    pub database_type: String,
    pub generated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetTableRequest {
    pub db_identifier: String,
    pub table_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetTableResponse {
    pub generated_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidateQueryRequest {
    pub db_identifier: String,
    pub query: String,
    pub parameters: HashMap<String, QueryParameter>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidationResponse {
    pub valid: bool,
    pub error_message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecuteQueryRequest {
    pub db_identifier: String,
    pub query: String,
    pub parameters: HashMap<String, QueryParameter>,
    pub limit: i32,
    pub timeout_seconds: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecuteQueryResponse {
    pub status: i32,
    pub row_count: i32,
    pub execution_ms: i32,
    pub error_message: String,
}

// ============================================================================
// SERVICE IMPLEMENTATIONS
// ============================================================================

/// Default SchemaService implementation for Phase 1A
pub struct SchemaServiceImpl;

impl SchemaServiceImpl {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SchemaServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SchemaService for SchemaServiceImpl {
    async fn get_schema(&self, _request: GetSchemaRequest) -> Result<GetSchemaResponse, String> {
        // Phase 1A: Return placeholder response
        Ok(GetSchemaResponse {
            schema_id: format!("schema_{}", uuid::Uuid::new_v4()),
            database_type: "SQLite".to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn get_table(&self, _request: GetTableRequest) -> Result<GetTableResponse, String> {
        // Phase 1A: Return placeholder response
        Ok(GetTableResponse {
            generated_at: chrono::Utc::now().to_rfc3339(),
        })
    }
}

/// Default QueryService implementation for Phase 1A
pub struct QueryServiceImpl;

impl QueryServiceImpl {
    pub fn new() -> Self {
        Self
    }
}

impl Default for QueryServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueryService for QueryServiceImpl {
    async fn validate_query(
        &self,
        _request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        // Phase 1A: Return placeholder - validation not yet implemented
        Ok(ValidationResponse {
            valid: false,
            error_message: "Phase 1A: Query validation not yet implemented".to_string(),
        })
    }

    async fn execute_query(
        &self,
        _request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        // Phase 1A: Return placeholder - execution not yet implemented
        Ok(ExecuteQueryResponse {
            status: QueryStatus::ExecutionError as i32,
            row_count: 0,
            execution_ms: 0,
            error_message: "Phase 1A: Query execution not yet implemented".to_string(),
        })
    }
}
