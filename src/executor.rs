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

/// Default SchemaService implementation
///
/// Phase 1A: Basic structure with placeholder responses
/// Phase 1B: Optionally supports database path for introspection
pub struct SchemaServiceImpl {
    db_path: Option<String>,
}

impl SchemaServiceImpl {
    pub fn new() -> Self {
        Self { db_path: None }
    }

    /// Set database path for Phase 1B introspection features
    pub fn set_db_path(&mut self, db_path: &str) -> Result<(), String> {
        self.db_path = Some(db_path.to_string());
        Ok(())
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
        if self.db_path.is_none() {
            return Ok(GetSchemaResponse {
                schema_id: format!("schema_{}", uuid::Uuid::new_v4()),
                database_type: "SQLite".to_string(),
                generated_at: chrono::Utc::now().to_rfc3339(),
            });
        }

        // Phase 1B: Database-backed introspection
        use crate::mirror::introspect_sqlite_path;

        let db_path = self.db_path.as_ref().unwrap();
        let _schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        Ok(GetSchemaResponse {
            schema_id: format!("schema_{}", uuid::Uuid::new_v4()),
            database_type: "SQLite".to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    async fn get_table(&self, request: GetTableRequest) -> Result<GetTableResponse, String> {
        // Phase 1A: Return placeholder if no db_path
        if self.db_path.is_none() {
            return Ok(GetTableResponse {
                generated_at: chrono::Utc::now().to_rfc3339(),
            });
        }

        // Phase 1B: Database-backed table retrieval
        use crate::mirror::introspect_sqlite_path;

        let db_path = self.db_path.as_ref().unwrap();
        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        // Validate table exists
        let _table = schema
            .tables
            .iter()
            .find(|t| t.name == request.table_name)
            .ok_or_else(|| format!("Table '{}' not found", request.table_name))?;

        Ok(GetTableResponse {
            generated_at: chrono::Utc::now().to_rfc3339(),
        })
    }
}

/// Default QueryService implementation
///
/// Phase 1A: Basic structure with placeholder responses
/// Phase 1B: Optionally supports database path for validation
pub struct QueryServiceImpl {
    db_path: Option<String>,
}

impl QueryServiceImpl {
    pub fn new() -> Self {
        Self { db_path: None }
    }

    /// Set database path for Phase 1B validation features
    pub fn set_db_path(&mut self, db_path: &str) -> Result<(), String> {
        self.db_path = Some(db_path.to_string());
        Ok(())
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
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        // Phase 1A: If no db_path set, return placeholder
        let db_path = match &self.db_path {
            Some(path) => path,
            None => {
                return Ok(ValidationResponse {
                    valid: false,
                    error_message: "Phase 1A: Query validation not yet implemented".to_string(),
                })
            }
        };

        // Phase 1B: Database-backed validation
        use crate::mirror::introspect_sqlite_path;

        let query_upper = request.query.to_uppercase();

        // Phase 1C: P2SQL SECURITY CHECKS (Prompt Injection Defense)

        // 1C.2: Reject line comments (--) - check FIRST before other separators
        if request.query.contains("--") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "SQL line comment syntax (--) is not allowed".to_string(),
            });
        }

        // 1C.3: Reject block comments (/* */)
        if request.query.contains("/*") || request.query.contains("*/") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "SQL block comment syntax (/* */) is not allowed".to_string(),
            });
        }

        // 1C.1: Reject command chaining (semicolons)
        if request.query.contains(';') {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "Command chaining detected: semicolon is not allowed".to_string(),
            });
        }

        // 1C.4: Reject PRAGMA statements
        if query_upper.contains("PRAGMA") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "PRAGMA statements are not allowed".to_string(),
            });
        }

        // 1C.5: Reject EXPLAIN statements
        if query_upper.starts_with("EXPLAIN") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "EXPLAIN statements are not allowed".to_string(),
            });
        }

        // 1C.6: Reject boolean-based injection patterns (OR '1'='1 or OR "1"="1)
        if (query_upper.contains(" OR '") || query_upper.contains(" OR \""))
            && (query_upper.contains("'1'='1") || query_upper.contains("\"1\"=\"1"))
        {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "Suspected boolean-based SQL injection detected".to_string(),
            });
        }

        // 1C.7: Reject UNION-based injection
        if query_upper.contains("UNION") && query_upper.contains("SELECT") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "UNION queries are not allowed - potential injection vector"
                    .to_string(),
            });
        }

        // 1C.8: Reject time-based blind injection (SLEEP, WAITFOR)
        if query_upper.contains("SLEEP(") || query_upper.contains("WAITFOR") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "SLEEP injection detected - time-based injection not allowed"
                    .to_string(),
            });
        }

        // 1C.9: Reject subquery injection in WHERE/FROM/SELECT
        if request.query.contains("(SELECT") || request.query.contains("(select") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "Subquery injection detected - embedded SELECT not allowed"
                    .to_string(),
            });
        }

        // 1C.10: Reject ATTACH/DETACH database manipulation
        if query_upper.contains("ATTACH") || query_upper.contains("DETACH") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "ATTACH/DETACH DATABASE statements are not allowed".to_string(),
            });
        }

        // Phase 1B: Basic DDL/DML/Schema Validation (after P2SQL checks)

        // Check for DDL/DML keywords (read-only enforcement)
        if query_upper.contains("INSERT")
            || query_upper.contains("UPDATE")
            || query_upper.contains("DELETE")
        {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "DML statements (INSERT, UPDATE, DELETE) are not allowed"
                    .to_string(),
            });
        }

        if query_upper.contains("CREATE")
            || query_upper.contains("DROP")
            || query_upper.contains("ALTER")
        {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "DDL statements (CREATE, DROP, ALTER) are not allowed".to_string(),
            });
        }

        // Check for FROM clause
        if !query_upper.contains("FROM") {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "Query must contain FROM clause".to_string(),
            });
        }

        // Introspect schema
        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        // Extract and validate table name
        let from_pos = query_upper.find("FROM").unwrap();
        let after_from = &request.query[from_pos + 4..].trim_start();
        let table_name = after_from
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();

        if !schema.tables.iter().any(|t| t.name == table_name) {
            return Ok(ValidationResponse {
                valid: false,
                error_message: format!("Table '{}' does not exist in schema", table_name),
            });
        }

        // If all checks pass
        Ok(ValidationResponse {
            valid: true,
            error_message: String::new(),
        })
    }

    async fn execute_query(
        &self,
        _request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        // Phase 1A/1B/1C: Placeholder - actual execution in Phase 1D
        Ok(ExecuteQueryResponse {
            status: QueryStatus::ExecutionError as i32,
            row_count: 0,
            execution_ms: 0,
            error_message: "Query execution not yet implemented".to_string(),
        })
    }
}
