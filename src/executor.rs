//! OAM Executor: Query and schema service implementations
//!
//! This module provides trait definitions and message types for query execution and schema introspection.

use once_cell::sync::Lazy;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

/// Connection cache to avoid repeated open/close overhead.
/// Each database path maintains a single cached connection for efficiency.
static DB_CONNECTION_CACHE: Lazy<Mutex<HashMap<String, Connection>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Get or create a cached database connection for the given path.
/// Connections are reused across queries to avoid expensive open/close cycles.
fn get_cached_connection(db_path: &str) -> Result<Connection, String> {
    let mut cache = DB_CONNECTION_CACHE
        .lock()
        .map_err(|e| format!("Failed to acquire connection cache lock: {}", e))?;

    // Return existing connection if available
    if let Some(conn) = cache.remove(db_path) {
        if conn.execute_batch("SELECT 1").is_ok() {
            return Ok(conn);
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("Failed to open database: {}", e))?;

    Ok(conn)
}

/// Return a connection to the cache for reuse.
/// Connections should be returned after use to enable reuse for subsequent queries.
fn cache_connection(db_path: &str, conn: Connection) -> Result<(), String> {
    let mut cache = DB_CONNECTION_CACHE
        .lock()
        .map_err(|e| format!("Failed to acquire connection cache lock: {}", e))?;

    cache.insert(db_path.to_string(), conn);

    Ok(())
}

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

/// Security check pattern for P2SQL validation
enum SecurityPattern {
    LineComment,
    BlockComment,
    CommandChain,
    Pragma,
    Explain,
    BooleanInjection,
    UnionInjection,
    TimeBlindInjection,
    SubqueryInjection,
    DatabaseManipulation,
}

impl SecurityPattern {
    /// Check if pattern exists in query and return error message
    fn check(&self, query: &str, query_upper: &str) -> Option<String> {
        match self {
            SecurityPattern::LineComment if query.contains("--") => {
                Some("SQL line comment syntax (--) is not allowed".to_string())
            }
            SecurityPattern::BlockComment if query.contains("/*") || query.contains("*/") => {
                Some("SQL block comment syntax (/* */) is not allowed".to_string())
            }
            SecurityPattern::CommandChain if query.contains(';') => {
                Some("Command chaining detected: semicolon is not allowed".to_string())
            }
            SecurityPattern::Pragma if query_upper.contains("PRAGMA") => {
                Some("PRAGMA statements are not allowed".to_string())
            }
            SecurityPattern::Explain if query_upper.trim_start().starts_with("EXPLAIN") => {
                Some("EXPLAIN statements are not allowed".to_string())
            }
            SecurityPattern::BooleanInjection
                if (query_upper.contains(" OR '") || query_upper.contains(" OR \""))
                    && (query_upper.contains("'1'='1") || query_upper.contains("\"1\"=\"1")) =>
            {
                Some("Suspected boolean-based SQL injection detected".to_string())
            }
            SecurityPattern::UnionInjection
                if query_upper.contains("UNION") && query_upper.contains("SELECT") =>
            {
                Some("UNION queries are not allowed - potential injection vector".to_string())
            }
            SecurityPattern::TimeBlindInjection
                if query_upper.contains("SLEEP(") || query_upper.contains("WAITFOR") =>
            {
                Some("SLEEP injection detected - time-based injection not allowed".to_string())
            }
            SecurityPattern::SubqueryInjection if query_upper.contains("(SELECT") => {
                Some("Subquery injection detected - embedded SELECT not allowed".to_string())
            }
            SecurityPattern::DatabaseManipulation
                if query_upper.contains("ATTACH") || query_upper.contains("DETACH") =>
            {
                Some("ATTACH/DETACH DATABASE statements are not allowed".to_string())
            }
            _ => None,
        }
    }
}

/// DML/DDL keyword patterns for read-only enforcement
#[derive(PartialEq)]
enum MutationPattern {
    Dml,
    Ddl,
}

impl MutationPattern {
    /// Check if mutation keyword exists in query and return error message
    fn check(&self, query_upper: &str) -> Option<String> {
        match self {
            MutationPattern::Dml
                if query_upper.contains("INSERT")
                    || query_upper.contains("UPDATE")
                    || query_upper.contains("DELETE") =>
            {
                Some("DML statements (INSERT, UPDATE, DELETE) are not allowed".to_string())
            }
            MutationPattern::Ddl
                if query_upper.contains("CREATE")
                    || query_upper.contains("DROP")
                    || query_upper.contains("ALTER") =>
            {
                Some("DDL statements (CREATE, DROP, ALTER) are not allowed".to_string())
            }
            _ => None,
        }
    }
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
    pub timestamp: String,
}

/// SchemaService implementation
pub struct SchemaServiceImpl {
    db_path: Option<String>,
}

impl SchemaServiceImpl {
    pub fn new() -> Self {
        Self { db_path: None }
    }

    /// Set database path for introspection
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
        if self.db_path.is_none() {
            return Ok(GetSchemaResponse {
                schema_id: format!("schema_{}", uuid::Uuid::new_v4()),
                database_type: "SQLite".to_string(),
                generated_at: chrono::Utc::now().to_rfc3339(),
            });
        }

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
        if self.db_path.is_none() {
            return Ok(GetTableResponse {
                generated_at: chrono::Utc::now().to_rfc3339(),
            });
        }

        use crate::mirror::introspect_sqlite_path;

        let db_path = self.db_path.as_ref().unwrap();
        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

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

/// QueryService implementation
pub struct QueryServiceImpl {
    db_path: Option<String>,
}

impl QueryServiceImpl {
    pub fn new() -> Self {
        Self { db_path: None }
    }

    /// Set database path for query validation
    pub fn set_db_path(&mut self, db_path: &str) -> Result<(), String> {
        self.db_path = Some(db_path.to_string());
        Ok(())
    }

    /// Run security pattern checks on query
    fn check_security_patterns(query: &str) -> Option<String> {
        let query_upper = query.to_uppercase();
        [
            SecurityPattern::LineComment,
            SecurityPattern::BlockComment,
            SecurityPattern::CommandChain,
            SecurityPattern::Pragma,
            SecurityPattern::Explain,
            SecurityPattern::BooleanInjection,
            SecurityPattern::UnionInjection,
            SecurityPattern::TimeBlindInjection,
            SecurityPattern::SubqueryInjection,
            SecurityPattern::DatabaseManipulation,
        ]
        .iter()
        .find_map(|pattern| pattern.check(query, &query_upper))
    }

    fn check_mutation_keywords(query_upper: &str) -> Option<String> {
        [MutationPattern::Dml, MutationPattern::Ddl]
            .iter()
            .find_map(|pattern| pattern.check(query_upper))
    }

    /// Build validation error response with event dispatch
    async fn build_validation_error_response(
        &self,
        request: &ExecuteQueryRequest,
        error_message: String,
        start_time: std::time::Instant,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        let event = Event::query_validation_failed(
            request.db_identifier.clone(),
            request.query.clone(),
            error_message.clone(),
            timestamp.clone(),
        );
        if let Err(e) = get_event_bus().dispatch_generic(&event) {
            eprintln!("Event dispatch failed for query_validation_failed: {}", e);
        }

        Ok(ExecuteQueryResponse {
            status: QueryStatus::ValidationError as i32,
            row_count: 0,
            execution_ms,
            error_message,
            timestamp,
        })
    }

    /// Build execution error response with event dispatch
    async fn build_execution_error_response(
        &self,
        request: &ExecuteQueryRequest,
        error_message: String,
        start_time: std::time::Instant,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        let event = Event::query_execution_error(
            request.db_identifier.clone(),
            request.query.clone(),
            error_message.clone(),
            timestamp.clone(),
        );
        if let Err(e) = get_event_bus().dispatch_generic(&event) {
            eprintln!("Event dispatch failed for query_execution_error: {}", e);
        }

        Ok(ExecuteQueryResponse {
            status: QueryStatus::ExecutionError as i32,
            row_count: 0,
            execution_ms,
            error_message,
            timestamp,
        })
    }

    /// Build execution response handling success and error cases
    async fn build_execution_response(
        &self,
        request: &ExecuteQueryRequest,
        execution_result: Result<i64, String>,
        start_time: std::time::Instant,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        match execution_result {
            Ok(row_count) => {
                let event = Event::query_executed(
                    request.db_identifier.clone(),
                    request.query.clone(),
                    "Success".to_string(),
                    row_count as i32,
                    execution_ms,
                    timestamp.clone(),
                );
                if let Err(e) = get_event_bus().dispatch_generic(&event) {
                    eprintln!("Event dispatch failed for query_executed: {}", e);
                }

                Ok(ExecuteQueryResponse {
                    status: QueryStatus::Success as i32,
                    row_count: row_count as i32,
                    execution_ms,
                    error_message: String::new(),
                    timestamp,
                })
            }
            Err(e) => {
                self.handle_execution_error(request.clone(), e, execution_ms, timestamp)
                    .await
            }
        }
    }

    /// Handle execution errors, distinguishing between timeout and other errors
    async fn handle_execution_error(
        &self,
        request: ExecuteQueryRequest,
        error: String,
        execution_ms: i32,
        timestamp: String,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};

        if error.to_lowercase().contains("timeout") {
            return Ok(ExecuteQueryResponse {
                status: QueryStatus::Timeout as i32,
                row_count: 0,
                execution_ms,
                error_message: error,
                timestamp,
            });
        }

        // Phase 1E: Dispatch QueryExecutionError event
        // Phase 1E: Dispatch QueryExecutionError event
        let event = Event::query_execution_error(
            request.db_identifier.clone(),
            request.query.clone(),
            error.clone(),
            timestamp.clone(),
        );
        if let Err(e) = get_event_bus().dispatch_generic(&event) {
            eprintln!(
                "Event dispatch failed for query_execution_error (in handle_execution_error): {}",
                e
            );
        }

        Ok(ExecuteQueryResponse {
            status: QueryStatus::ExecutionError as i32,
            row_count: 0,
            execution_ms,
            error_message: error,
            timestamp,
        })
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
        let db_path = match &self.db_path {
            Some(path) => path,
            None => {
                return Ok(ValidationResponse {
                    valid: false,
                    error_message: "Phase 1A: Query validation not yet implemented".to_string(),
                })
            }
        };

        use crate::mirror::introspect_sqlite_path;

        let query_upper = request.query.to_uppercase();

        if let Some(error_message) = Self::check_security_patterns(&request.query) {
            return Ok(ValidationResponse {
                valid: false,
                error_message,
            });
        }

        // Check for mutation keywords (read-only enforcement)
        if let Some(error_message) = Self::check_mutation_keywords(&query_upper) {
            return Ok(ValidationResponse {
                valid: false,
                error_message,
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

        // Extract and validate table name (robust to case, schema qualifiers, and quotes)
        let from_pos = query_upper
            .find("FROM")
            .expect("FROM clause existence was checked above");
        let after_from_upper = query_upper[from_pos + 4..].trim_start();
        let raw_table_token_upper = after_from_upper.split_whitespace().next().unwrap_or("");

        // Get the same token from the original query to preserve casing for error messages
        let after_from_original = request.query[from_pos + 4..].trim_start();
        let raw_table_token_original = after_from_original.split_whitespace().next().unwrap_or("");

        // Normalize table token for schema matching:
        // - strip trailing semicolons,
        // - drop schema qualifier (take last segment after '.'),
        // - remove surrounding quote-like characters.
        let raw_token = raw_table_token_upper.trim_end_matches(';');
        let last_segment = raw_token.rsplit('.').next().unwrap_or(raw_token);
        let normalized_table_name = last_segment
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_string();

        // Also preserve original casing for error messages
        let original_token = raw_table_token_original.trim_end_matches(';');
        let original_last_segment = original_token.rsplit('.').next().unwrap_or(original_token);
        let display_table_name = original_last_segment
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_string();

        if !schema
            .tables
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(&normalized_table_name))
        {
            return Ok(ValidationResponse {
                valid: false,
                error_message: format!("Table '{}' does not exist in schema", display_table_name),
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
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        let start_time = std::time::Instant::now();

        if let Some(error_msg) = Self::check_security_patterns(&request.query) {
            return self
                .build_validation_error_response(&request, error_msg, start_time)
                .await;
        }

        // Check mutation keywords
        let query_upper = request.query.to_uppercase();
        if let Some(error_msg) = Self::check_mutation_keywords(&query_upper) {
            return self
                .build_validation_error_response(&request, error_msg, start_time)
                .await;
        }

        // Get database path or return error
        let db_path = match &self.db_path {
            Some(path) => path,
            None => {
                let error_msg = "Database path not configured".to_string();
                return self
                    .build_execution_error_response(&request, error_msg, start_time)
                    .await;
            }
        };

        // Full validation including schema checks
        let validation_result = self
            .validate_query(ValidateQueryRequest {
                db_identifier: request.db_identifier.clone(),
                query: request.query.clone(),
                parameters: request.parameters.clone(),
            })
            .await?;

        if !validation_result.valid {
            return self
                .build_validation_error_response(
                    &request,
                    validation_result.error_message,
                    start_time,
                )
                .await;
        }

        // Execute query with timeout
        let execution_result = execute_query_on_database_async(
            db_path,
            &request.query,
            request.limit,
            request.timeout_seconds,
        )
        .await;

        self.build_execution_response(&request, execution_result, start_time)
            .await
    }
}

/// Async wrapper for query execution with timeout support
async fn execute_query_on_database_async(
    db_path: &str,
    query: &str,
    limit: i32,
    timeout_seconds: i32,
) -> Result<i64, String> {
    let db_path = db_path.to_string();
    let query = query.to_string();

    let task = tokio::task::spawn_blocking(move || {
        execute_query_blocking(&db_path, &query, limit, timeout_seconds)
    });

    if timeout_seconds > 0 {
        let timeout_duration = std::time::Duration::from_secs(timeout_seconds as u64);
        match tokio::time::timeout(timeout_duration, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(format!("Task execution error: {}", e)),
            Err(_) => Err(format!(
                "Query execution timeout: exceeded {} seconds",
                timeout_seconds
            )),
        }
    } else {
        match task.await {
            Ok(result) => result,
            Err(e) => Err(format!("Task execution error: {}", e)),
        }
    }
}

fn execute_query_blocking(
    db_path: &str,
    query: &str,
    limit: i32,
    timeout_seconds: i32,
) -> Result<i64, String> {
    let conn = get_cached_connection(db_path)?;

    // Set busy_timeout for database lock contention
    // This complements the tokio timeout for execution timeout
    if timeout_seconds > 0 {
        // Set busy_timeout to a fraction of the total timeout to allow query execution to proceed
        let busy_timeout = std::time::Duration::from_millis(
            (timeout_seconds as u64 * 500) / 1000, // 50% of timeout for lock waits
        );
        conn.busy_timeout(busy_timeout)
            .map_err(|e| format!("Failed to set busy timeout: {}", e))?;
    }

    // Execute query and count rows within a scope to ensure all borrows are dropped
    let result = {
        let mut stmt = conn
            .prepare(query)
            .map_err(|e| format!("Failed to prepare query: {}", e))?;

        let mut row_count = 0i64;
        let limit_i64 = limit as i64;

        let rows = stmt
            .query_map([], |_row| Ok(()))
            .map_err(|e| format!("Query execution failed: {}", e))?;

        for result in rows {
            match result {
                Ok(_) => {
                    row_count += 1;
                    if limit > 0 && row_count >= limit_i64 {
                        break;
                    }
                }
                Err(e) => {
                    return Err(format!("Query execution failed: {}", e));
                }
            }
        }
        Ok(row_count)
    };

    // Return connection to cache for reuse
    cache_connection(db_path, conn)?;

    result
}

