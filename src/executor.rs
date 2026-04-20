use crate::access_policy::{DataAccessEnforcer, EnforcementOutcome};
use crate::mirror::SchemaModel;
use crate::policy_engine::{PolicyContext, PolicyEngine, ToolIntent};
use crate::runtime_context::QueryRuntimeContext;
use once_cell::sync::Lazy;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

#[async_trait::async_trait]
pub trait MirrorProvider: Send + Sync {
    async fn introspect_schema(&self) -> Result<SchemaModel, String>;
}

pub struct SqliteMirrorProvider {
    db_path: String,
}

impl SqliteMirrorProvider {
    pub fn new(db_path: impl Into<String>) -> Self {
        Self {
            db_path: db_path.into(),
        }
    }
}

#[async_trait::async_trait]
impl MirrorProvider for SqliteMirrorProvider {
    async fn introspect_schema(&self) -> Result<SchemaModel, String> {
        crate::mirror::introspect_sqlite_path(&self.db_path)
            .map_err(|e| format!("SQLite introspection failed: {e}"))
    }
}

static DB_CONNECTION_CACHE: Lazy<Mutex<HashMap<String, Connection>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn get_cached_connection(db_path: &str) -> Result<Connection, String> {
    let mut cache = DB_CONNECTION_CACHE
        .lock()
        .map_err(|e| format!("Failed to acquire connection cache lock: {}", e))?;

    if let Some(conn) = cache.remove(db_path) {
        if conn.execute_batch("SELECT 1").is_ok() {
            return Ok(conn);
        }
    }

    let conn = Connection::open(db_path).map_err(|e| format!("Failed to open database: {}", e))?;

    Ok(conn)
}

fn cache_connection(db_path: &str, conn: Connection) -> Result<(), String> {
    let mut cache = DB_CONNECTION_CACHE
        .lock()
        .map_err(|e| format!("Failed to acquire connection cache lock: {}", e))?;

    cache.insert(db_path.to_string(), conn);

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryParameter {
    pub value: String,
    pub type_hint: String,
}

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

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryRuntimeAugmentation {
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub event_metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audit_events: Vec<crate::interceptor::Event>,
}

#[async_trait::async_trait]
pub trait QueryRuntimeAugmentor: Send + Sync {
    async fn augment(
        &self,
        db_identifier: &str,
        query: &str,
        runtime_context: &QueryRuntimeContext,
    ) -> Result<QueryRuntimeAugmentation, String>;
}

#[async_trait::async_trait]
pub trait SchemaService: Send + Sync {
    async fn get_schema(&self, request: GetSchemaRequest) -> Result<GetSchemaResponse, String>;

    async fn get_table(&self, request: GetTableRequest) -> Result<GetTableResponse, String>;
}

#[async_trait::async_trait]
pub trait QueryService: Send + Sync {
    async fn validate_query(
        &self,
        request: ValidateQueryRequest,
    ) -> Result<ValidationResponse, String>;

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetSchemaRequest {
    pub db_identifier: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetSchemaResponse {
    pub schema_id: String,
    pub database_type: String,
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<crate::mirror::Table>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_defined_types: Vec<crate::mirror::UserDefinedType>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetTableRequest {
    pub db_identifier: String,
    pub table_name: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GetTableResponse {
    pub generated_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<crate::mirror::Table>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ValidateQueryRequest {
    pub db_identifier: String,
    pub query: String,
    pub parameters: HashMap<String, QueryParameter>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ValidationResponse {
    pub valid: bool,
    pub error_message: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub event_metadata: HashMap<String, String>,
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

pub struct SchemaServiceImpl {
    db_path: Option<String>,
}

impl SchemaServiceImpl {
    pub fn new() -> Self {
        Self { db_path: None }
    }

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
                tables: vec![],
                user_defined_types: vec![],
            });
        }

        use crate::mirror::introspect_sqlite_path;

        let db_path = self.db_path.as_ref().unwrap();
        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        Ok(GetSchemaResponse {
            schema_id: format!("schema_{}", uuid::Uuid::new_v4()),
            database_type: "SQLite".to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            tables: schema.tables,
            user_defined_types: schema.user_defined_types,
        })
    }

    async fn get_table(&self, request: GetTableRequest) -> Result<GetTableResponse, String> {
        if self.db_path.is_none() {
            return Ok(GetTableResponse {
                generated_at: chrono::Utc::now().to_rfc3339(),
                table: None,
            });
        }

        use crate::mirror::introspect_sqlite_path;

        let db_path = self.db_path.as_ref().unwrap();
        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        let table = schema
            .tables
            .into_iter()
            .find(|t| t.name == request.table_name)
            .ok_or_else(|| format!("Table '{}' not found", request.table_name))?;

        Ok(GetTableResponse {
            generated_at: chrono::Utc::now().to_rfc3339(),
            table: Some(table),
        })
    }
}

pub struct QueryServiceImpl {
    db_path: Option<String>,
    runtime_augmentor: Option<Arc<dyn QueryRuntimeAugmentor>>,
    access_enforcer: Option<Arc<dyn DataAccessEnforcer>>,
}

impl QueryServiceImpl {
    pub fn new() -> Self {
        Self {
            db_path: None,
            runtime_augmentor: None,
            access_enforcer: None,
        }
    }

    pub fn set_db_path(&mut self, db_path: &str) -> Result<(), String> {
        self.db_path = Some(db_path.to_string());
        Ok(())
    }

    pub fn set_runtime_augmentor(&mut self, augmentor: Arc<dyn QueryRuntimeAugmentor>) {
        self.runtime_augmentor = Some(augmentor);
    }

    pub fn set_access_enforcer(&mut self, enforcer: Arc<dyn DataAccessEnforcer>) {
        self.access_enforcer = Some(enforcer);
    }

    fn evaluate_policy(query: &str, policy_context: Option<&PolicyContext>) -> Option<String> {
        let decision = match policy_context {
            Some(context) => PolicyEngine::evaluate_with_context(query, context),
            None => PolicyEngine::evaluate(query, ToolIntent::ReadSelect),
        };

        if decision.allowed {
            None
        } else {
            Some(
                decision
                    .reason
                    .unwrap_or_else(|| "Query rejected during policy validation".to_string()),
            )
        }
    }

    pub async fn validate_query_with_policy(
        &self,
        request: ValidateQueryRequest,
        policy_context: PolicyContext,
    ) -> Result<ValidationResponse, String> {
        self.validate_query_internal(request, Some(&policy_context), None)
            .await
    }

    pub async fn validate_query_with_runtime_context(
        &self,
        request: ValidateQueryRequest,
        runtime_context: QueryRuntimeContext,
    ) -> Result<ValidationResponse, String> {
        let policy_context = runtime_context.policy_context();
        self.validate_query_internal(request, policy_context.as_ref(), Some(&runtime_context))
            .await
    }

    pub async fn execute_query_with_policy(
        &self,
        request: ExecuteQueryRequest,
        policy_context: PolicyContext,
    ) -> Result<ExecuteQueryResponse, String> {
        self.execute_query_internal(request, Some(&policy_context), None)
            .await
    }

    pub async fn execute_query_with_runtime_context(
        &self,
        request: ExecuteQueryRequest,
        runtime_context: QueryRuntimeContext,
    ) -> Result<ExecuteQueryResponse, String> {
        let policy_context = runtime_context.policy_context();
        self.execute_query_internal(request, policy_context.as_ref(), Some(&runtime_context))
            .await
    }

    async fn validate_query_internal(
        &self,
        request: ValidateQueryRequest,
        policy_context: Option<&PolicyContext>,
        runtime_context: Option<&QueryRuntimeContext>,
    ) -> Result<ValidationResponse, String> {
        let runtime_augmentation = match self
            .augment_runtime_context(&request.db_identifier, &request.query, runtime_context)
            .await
        {
            Ok(aug) => aug,
            Err(error) => {
                return Ok(ValidationResponse {
                    valid: false,
                    error_message: format!("Runtime context augmentation failed: {error}"),
                    event_metadata: Default::default(),
                });
            }
        };

        // Dispatch any augmentation audit events so the audit trail is complete even on
        // validation failures.
        Self::dispatch_audit_events(runtime_augmentation.as_ref());

        let augmentation_metadata =
            Self::merged_event_metadata(None, runtime_augmentation.as_ref());

        // Enforce CODE_FIRST schema mode: queries are restricted to the registered table set.
        // Only applied when schema_mode is CODE_FIRST and table_names are explicitly registered.
        if let Some(ctx) = runtime_context {
            if ctx.schema_mode.as_deref() == Some("CODE_FIRST") && !ctx.table_names.is_empty() {
                if let Some(error_message) =
                    Self::validate_code_first_table_access(&request.query, &ctx.table_names)
                {
                    return Ok(ValidationResponse {
                        valid: false,
                        error_message,
                        event_metadata: augmentation_metadata,
                    });
                }
            }
        }

        let db_path = match &self.db_path {
            Some(path) => path,
            None => {
                return Ok(ValidationResponse {
                    valid: false,
                    error_message: "Database path is not configured".to_string(),
                    event_metadata: augmentation_metadata,
                });
            }
        };

        let mut response = Self::validate_query_against_db(db_path, &request, policy_context)?;
        // Merge augmentation metadata into the response so callers can include it in events.
        for (k, v) in augmentation_metadata {
            response.event_metadata.entry(k).or_insert(v);
        }
        Ok(response)
    }

    /// Extracts the primary table name from a `FROM` clause.
    ///
    /// Returns `Some((normalized, display))` where `normalized` is uppercased and unquoted
    /// (for case-insensitive comparison) and `display` preserves original case and quoting
    /// style (for error messages). Returns `None` when no top-level `FROM` clause is found.
    fn extract_table_name_from_query(query: &str) -> Option<(String, String)> {
        let from_pos = find_top_level_keyword_position(query, "FROM")?;

        let query_upper = query.to_uppercase();
        let raw_upper = query_upper[from_pos + 4..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';');
        let normalized = raw_upper
            .rsplit('.')
            .next()
            .unwrap_or(raw_upper)
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_string();

        let raw_original = query[from_pos + 4..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';');
        let display = raw_original
            .rsplit('.')
            .next()
            .unwrap_or(raw_original)
            .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']')
            .to_string();

        Some((normalized, display))
    }

    /// Checks that the table referenced in the query is within the set of registered tables
    /// for CODE_FIRST schema mode. Returns an error message if access is denied, or `None`
    /// if the access is allowed.
    fn validate_code_first_table_access(query: &str, allowed_tables: &[String]) -> Option<String> {
        let (normalized, display) = Self::extract_table_name_from_query(query)?;

        if allowed_tables
            .iter()
            .any(|t| t.eq_ignore_ascii_case(&normalized))
        {
            None
        } else {
            Some(format!(
                "Table '{}' is not registered in the schema for CODE_FIRST mode",
                display,
            ))
        }
    }

    /// Validate a query against both the configured policy and the live schema.
    ///
    /// This is the **Template Method** composition: policy gate first, then schema gate.
    fn validate_query_against_db(
        db_path: &str,
        request: &ValidateQueryRequest,
        policy_context: Option<&PolicyContext>,
    ) -> Result<ValidationResponse, String> {
        if let Some(error_message) = Self::validate_query_policy(&request.query, policy_context) {
            return Ok(ValidationResponse {
                valid: false,
                error_message,
                event_metadata: Default::default(),
            });
        }
        Self::validate_query_schema(db_path, request)
    }

    fn validate_query_policy(
        query: &str,
        policy_context: Option<&PolicyContext>,
    ) -> Option<String> {
        Self::evaluate_policy(query, policy_context)
    }

    fn validate_query_schema(
        db_path: &str,
        request: &ValidateQueryRequest,
    ) -> Result<ValidationResponse, String> {
        use crate::mirror::introspect_sqlite_path;

        let Some((normalized, display)) = Self::extract_table_name_from_query(&request.query)
        else {
            return Ok(ValidationResponse {
                valid: false,
                error_message: "SELECT statements must include a FROM clause".to_string(),
                event_metadata: Default::default(),
            });
        };

        let schema = introspect_sqlite_path(db_path)
            .map_err(|e| format!("Failed to introspect schema: {}", e))?;

        if schema
            .tables
            .iter()
            .any(|t| t.name.eq_ignore_ascii_case(&normalized))
        {
            Ok(ValidationResponse {
                valid: true,
                error_message: String::new(),
                event_metadata: Default::default(),
            })
        } else {
            Ok(ValidationResponse {
                valid: false,
                error_message: format!("Table '{}' does not exist in schema", display),
                event_metadata: Default::default(),
            })
        }
    }

    async fn execute_query_internal(
        &self,
        request: ExecuteQueryRequest,
        policy_context: Option<&PolicyContext>,
        runtime_context: Option<&QueryRuntimeContext>,
    ) -> Result<ExecuteQueryResponse, String> {
        let start_time = std::time::Instant::now();

        let runtime_augmentation = match self
            .augment_runtime_context(&request.db_identifier, &request.query, runtime_context)
            .await
        {
            Ok(augmentation) => augmentation,
            Err(error) => {
                return self
                    .build_validation_error_response(
                        &request,
                        format!("Runtime context augmentation failed: {error}"),
                        start_time,
                        runtime_context,
                        None,
                    )
                    .await;
            }
        };

        if let Some(error_message) = Self::validate_query_policy(&request.query, policy_context) {
            return self
                .build_validation_error_response(
                    &request,
                    error_message,
                    start_time,
                    runtime_context,
                    runtime_augmentation.as_ref(),
                )
                .await;
        }

        let db_path = match &self.db_path {
            Some(path) => path,
            None => {
                let error_msg = "Database path not configured".to_string();
                return self
                    .build_execution_error_response(
                        &request,
                        error_msg,
                        start_time,
                        runtime_context,
                        runtime_augmentation.as_ref(),
                    )
                    .await;
            }
        };

        let validation_request = ValidateQueryRequest {
            db_identifier: request.db_identifier.clone(),
            query: request.query.clone(),
            parameters: request.parameters.clone(),
        };

        let validation_result = Self::validate_query_schema(db_path, &validation_request)?;

        if !validation_result.valid {
            return self
                .build_validation_error_response(
                    &request,
                    validation_result.error_message,
                    start_time,
                    runtime_context,
                    runtime_augmentation.as_ref(),
                )
                .await;
        }

        let effective_query = match self
            .apply_enforcement(
                &request,
                db_path,
                runtime_context,
                runtime_augmentation.as_ref(),
            )
            .await
        {
            Ok(q) => q,
            Err(reason) => {
                return self
                    .build_validation_error_response(
                        &request,
                        format!("Access denied: {reason}"),
                        start_time,
                        runtime_context,
                        runtime_augmentation.as_ref(),
                    )
                    .await;
            }
        };

        let execution_result = execute_query_on_database_async(
            db_path,
            &effective_query,
            request.limit,
            request.timeout_seconds,
        )
        .await;

        self.build_execution_response(
            &request,
            execution_result,
            start_time,
            runtime_context,
            runtime_augmentation.as_ref(),
        )
        .await
    }

    async fn augment_runtime_context(
        &self,
        db_identifier: &str,
        query: &str,
        runtime_context: Option<&QueryRuntimeContext>,
    ) -> Result<Option<QueryRuntimeAugmentation>, String> {
        let Some(runtime_context) = runtime_context else {
            return Ok(None);
        };
        let Some(augmentor) = &self.runtime_augmentor else {
            return Ok(None);
        };

        augmentor
            .augment(db_identifier, query, runtime_context)
            .await
            .map(Some)
    }

    async fn build_validation_error_response(
        &self,
        request: &ExecuteQueryRequest,
        error_message: String,
        start_time: std::time::Instant,
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        Self::dispatch_audit_events(runtime_augmentation);

        let event = Event::query_validation_failed(
            request.db_identifier.clone(),
            request.query.clone(),
            error_message.clone(),
            timestamp.clone(),
            Self::merged_event_metadata(runtime_context, runtime_augmentation),
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

    async fn build_execution_error_response(
        &self,
        request: &ExecuteQueryRequest,
        error_message: String,
        start_time: std::time::Instant,
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        Self::dispatch_audit_events(runtime_augmentation);

        let event = Event::query_execution_error(
            request.db_identifier.clone(),
            request.query.clone(),
            error_message.clone(),
            timestamp.clone(),
            Self::merged_event_metadata(runtime_context, runtime_augmentation),
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

    async fn build_execution_response(
        &self,
        request: &ExecuteQueryRequest,
        execution_result: Result<i64, String>,
        start_time: std::time::Instant,
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> Result<ExecuteQueryResponse, String> {
        use crate::interceptor::{get_event_bus, Event};
        use chrono::Utc;

        let execution_ms = start_time.elapsed().as_millis() as i32;
        let timestamp = Utc::now().to_rfc3339();

        match execution_result {
            Ok(row_count) => {
                Self::dispatch_audit_events(runtime_augmentation);

                let event = Event::query_executed(
                    request.db_identifier.clone(),
                    request.query.clone(),
                    "Success".to_string(),
                    row_count as i32,
                    execution_ms,
                    timestamp.clone(),
                    Self::merged_event_metadata(runtime_context, runtime_augmentation),
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
                self.handle_execution_error(
                    request.clone(),
                    e,
                    execution_ms,
                    timestamp,
                    runtime_context,
                    runtime_augmentation,
                )
                .await
            }
        }
    }

    fn dispatch_audit_events(runtime_augmentation: Option<&QueryRuntimeAugmentation>) {
        use crate::interceptor::get_event_bus;

        let Some(runtime_augmentation) = runtime_augmentation else {
            return;
        };

        for event in &runtime_augmentation.audit_events {
            if let Err(error) = get_event_bus().dispatch_generic(event) {
                eprintln!("Event dispatch failed for runtime audit event: {}", error);
            }
        }
    }

    fn merged_event_metadata(
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> HashMap<String, String> {
        let mut metadata = runtime_context
            .map(QueryRuntimeContext::event_metadata)
            .unwrap_or_default();

        if let Some(runtime_augmentation) = runtime_augmentation {
            for (key, value) in &runtime_augmentation.event_metadata {
                metadata.entry(key.clone()).or_insert_with(|| value.clone());
            }
        }

        metadata
    }

    async fn handle_execution_error(
        &self,
        request: ExecuteQueryRequest,
        error: String,
        execution_ms: i32,
        timestamp: String,
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
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

        let event = Event::query_execution_error(
            request.db_identifier.clone(),
            request.query.clone(),
            error.clone(),
            timestamp.clone(),
            Self::merged_event_metadata(runtime_context, runtime_augmentation),
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

    async fn apply_enforcement(
        &self,
        request: &ExecuteQueryRequest,
        db_path: &str,
        runtime_context: Option<&QueryRuntimeContext>,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> Result<String, String> {
        match (&self.access_enforcer, runtime_context) {
            (Some(enforcer), Some(rtx)) => {
                Self::run_enforcement(enforcer, request, db_path, rtx, runtime_augmentation).await
            }
            _ => Ok(request.query.clone()),
        }
    }

    async fn run_enforcement(
        enforcer: &Arc<dyn DataAccessEnforcer>,
        request: &ExecuteQueryRequest,
        db_path: &str,
        rtx: &QueryRuntimeContext,
        runtime_augmentation: Option<&QueryRuntimeAugmentation>,
    ) -> Result<String, String> {
        use crate::interceptor::{get_event_bus, Event};
        use crate::mirror::introspect_sqlite_path;

        let schema: Option<SchemaModel> = introspect_sqlite_path(db_path).ok();
        let outcome = enforcer.enforce(&request.query, rtx, schema.as_ref()).await;

        match outcome {
            EnforcementOutcome::Allow => Ok(request.query.clone()),
            EnforcementOutcome::Rewrite {
                sql,
                ref redacted_columns,
            } => {
                let table =
                    crate::access_policy::extract_query_table(&request.query).unwrap_or_default();
                let user_id = rtx.user_id.clone().unwrap_or_default();
                let ctx = Self::merged_event_metadata(Some(rtx), runtime_augmentation);

                if !redacted_columns.is_empty() {
                    let ev = Event::columns_redacted(
                        request.db_identifier.clone(),
                        table.clone(),
                        user_id.clone(),
                        redacted_columns.clone(),
                        ctx.clone(),
                    );
                    let _ = get_event_bus().dispatch_generic(&ev);
                }

                if sql.contains("_roam_rls") {
                    let ev =
                        Event::rows_filtered(request.db_identifier.clone(), table, user_id, ctx);
                    let _ = get_event_bus().dispatch_generic(&ev);
                }

                Ok(sql)
            }
            EnforcementOutcome::Deny { reason } => {
                let user_id = rtx.user_id.clone().unwrap_or_default();
                let ctx = Self::merged_event_metadata(Some(rtx), runtime_augmentation);
                let ev = Event::access_denied(
                    request.db_identifier.clone(),
                    request.query.clone(),
                    user_id,
                    reason.clone(),
                    ctx,
                );
                let _ = get_event_bus().dispatch_generic(&ev);
                Err(reason)
            }
        }
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
        self.validate_query_internal(request, None, None).await
    }

    async fn execute_query(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponse, String> {
        self.execute_query_internal(request, None, None).await
    }
}

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

pub fn find_top_level_keyword_position(query: &str, keyword: &str) -> Option<usize> {
    let chars: Vec<(usize, char)> = query.char_indices().collect();
    let mut index = 0;
    let mut depth = 0usize;

    while index < chars.len() {
        let (_, ch) = chars[index];
        let next = chars.get(index + 1).map(|(_, next_char)| *next_char);

        if ch == '-' && next == Some('-') {
            index = skip_line_comment_chars(&chars, index + 2);
            continue;
        }

        if ch == '/' && next == Some('*') {
            index = skip_block_comment_chars(&chars, index + 2);
            continue;
        }

        if ch == '\'' {
            index = skip_single_quoted_literal_chars(&chars, index + 1);
            continue;
        }

        if ch == '"' || ch == '`' {
            index = skip_quoted_identifier_chars(&chars, index + 1, ch);
            continue;
        }

        if ch == '[' {
            index = skip_bracket_identifier_chars(&chars, index + 1);
            continue;
        }

        match ch {
            '(' => {
                depth += 1;
                index += 1;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            _ if depth == 0 && is_executor_word_start(ch) => {
                let (word, next_index) = read_executor_word(&chars, index);
                if word.eq_ignore_ascii_case(keyword) {
                    return Some(chars[index].0);
                }
                index = next_index;
            }
            _ => {
                index += 1;
            }
        }
    }

    None
}

fn skip_line_comment_chars(chars: &[(usize, char)], mut index: usize) -> usize {
    while index < chars.len() && chars[index].1 != '\n' {
        index += 1;
    }

    index
}

fn skip_block_comment_chars(chars: &[(usize, char)], mut index: usize) -> usize {
    while index + 1 < chars.len() {
        if chars[index].1 == '*' && chars[index + 1].1 == '/' {
            return index + 2;
        }
        index += 1;
    }

    chars.len()
}

fn skip_single_quoted_literal_chars(chars: &[(usize, char)], mut index: usize) -> usize {
    while index < chars.len() {
        if chars[index].1 == '\'' {
            if chars.get(index + 1).map(|(_, ch)| *ch) == Some('\'') {
                index += 2;
                continue;
            }

            return index + 1;
        }
        index += 1;
    }

    chars.len()
}

fn skip_quoted_identifier_chars(chars: &[(usize, char)], mut index: usize, quote: char) -> usize {
    while index < chars.len() {
        if chars[index].1 == quote {
            if chars.get(index + 1).map(|(_, ch)| *ch) == Some(quote) {
                index += 2;
                continue;
            }

            return index + 1;
        }
        index += 1;
    }

    chars.len()
}

fn skip_bracket_identifier_chars(chars: &[(usize, char)], mut index: usize) -> usize {
    while index < chars.len() {
        if chars[index].1 == ']' {
            if chars.get(index + 1).map(|(_, ch)| *ch) == Some(']') {
                index += 2;
                continue;
            }

            return index + 1;
        }
        index += 1;
    }

    chars.len()
}

fn is_executor_word_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn read_executor_word(chars: &[(usize, char)], start: usize) -> (String, usize) {
    let mut index = start + 1;

    while index < chars.len() && (chars[index].1.is_ascii_alphanumeric() || chars[index].1 == '_') {
        index += 1;
    }

    let word = chars[start..index]
        .iter()
        .map(|(_, ch)| *ch)
        .collect::<String>();
    (word, index)
}

fn execute_query_blocking(
    db_path: &str,
    query: &str,
    limit: i32,
    timeout_seconds: i32,
) -> Result<i64, String> {
    let conn = get_cached_connection(db_path)?;

    if timeout_seconds > 0 {
        let busy_timeout = std::time::Duration::from_millis(
            (timeout_seconds as u64 * 500) / 1000, // 50% of timeout for lock waits
        );
        conn.busy_timeout(busy_timeout)
            .map_err(|e| format!("Failed to set busy timeout: {}", e))?;
    }

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

    cache_connection(db_path, conn)?;

    result
}
