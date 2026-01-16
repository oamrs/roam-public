/// Executor module tests - Phase 1A: gRPC Infrastructure
///
/// These tests follow strict TDD discipline:
/// 1. Write failing tests for desired functionality
/// 2. Implement minimal production code to pass tests
/// 3. Refactor for quality
///
/// Phase 1A focuses on gRPC service definitions and basic infrastructure.
/// Phase 1B will add schema introspection, validation, and P2SQL security.
use oam::executor::{QueryService, QueryServiceImpl, SchemaService, SchemaServiceImpl};
use oam::generated::{
    ExecuteQueryRequest, GetSchemaRequest, GetTableRequest, QueryStatus, ValidateQueryRequest,
};

/// Test 1A.1: SchemaService can be instantiated
#[tokio::test]
async fn schema_service_can_be_created() {
    let _service = SchemaServiceImpl::new();
    // If this compiles and runs, SchemaServiceImpl exists
}

/// Test 1A.2: QueryService can be instantiated
#[tokio::test]
async fn query_service_can_be_created() {
    let _service = QueryServiceImpl::new();
    // If this compiles and runs, QueryServiceImpl exists
}

/// Test 1A.3: SchemaService::get_schema returns SchemaResponse with correct structure
#[tokio::test]
async fn schema_service_get_schema_returns_response() {
    let service = SchemaServiceImpl::new();
    let request = GetSchemaRequest {
        db_identifier: "primary".to_string(),
    };

    let result = service.get_schema(request).await;
    assert!(result.is_ok(), "get_schema should return Ok");

    let response = result.unwrap();
    assert!(
        !response.schema_id.is_empty(),
        "schema_id should not be empty"
    );
    assert!(
        !response.database_type.is_empty(),
        "database_type should not be empty"
    );
    assert!(
        !response.generated_at.is_empty(),
        "generated_at should not be empty"
    );
}

/// Test 1A.4: SchemaService::get_table returns TableResponse with correct structure
#[tokio::test]
async fn schema_service_get_table_returns_response() {
    let service = SchemaServiceImpl::new();
    let request = GetTableRequest {
        db_identifier: "primary".to_string(),
        table_name: "users".to_string(),
    };

    let result = service.get_table(request).await;
    assert!(result.is_ok(), "get_table should return Ok");

    let response = result.unwrap();
    assert!(
        !response.generated_at.is_empty(),
        "generated_at should not be empty"
    );
}

/// Test 1A.5: QueryService::validate_query returns ValidationResponse
#[tokio::test]
async fn query_service_validate_query_returns_response() {
    let service = QueryServiceImpl::new();
    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = service.validate_query(request).await;
    assert!(result.is_ok(), "validate_query should return Ok");

    let response = result.unwrap();
    // Response should be well-formed
    assert!(
        !response.error_message.is_empty() || response.valid,
        "Response should have either a valid flag or error message"
    );
}

/// Test 1A.6: QueryService::execute_query returns QueryResponse with status
#[tokio::test]
async fn query_service_execute_query_returns_response() {
    let service = QueryServiceImpl::new();
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok(), "execute_query should return Ok");

    let response = result.unwrap();
    // Status should be valid QueryStatus
    assert!(
        response.status >= 0,
        "status should be a valid QueryStatus code"
    );
}

/// Test 1A.7: Multiple schema requests with different db_identifiers work independently
#[tokio::test]
async fn schema_service_handles_multiple_databases() {
    let service = SchemaServiceImpl::new();

    let req1 = GetSchemaRequest {
        db_identifier: "db1".to_string(),
    };
    let req2 = GetSchemaRequest {
        db_identifier: "db2".to_string(),
    };

    let resp1 = service.get_schema(req1).await.unwrap();
    let resp2 = service.get_schema(req2).await.unwrap();

    // Both should return valid responses
    assert!(!resp1.schema_id.is_empty());
    assert!(!resp2.schema_id.is_empty());
    // Schema IDs should be different (unique per request in Phase 1A)
    assert_ne!(
        resp1.schema_id, resp2.schema_id,
        "Different requests should have different schema IDs"
    );
}

/// Test 1A.8: Query validation can accept queries with parameters
#[tokio::test]
async fn query_service_validate_query_with_parameters() {
    let service = QueryServiceImpl::new();

    let mut params = std::collections::HashMap::new();
    params.insert(
        "user_id".to_string(),
        oam::generated::QueryParameter {
            value: "123".to_string(),
            type_hint: "INTEGER".to_string(),
        },
    );

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users WHERE id = ?".to_string(),
        parameters: params,
    };

    let result = service.validate_query(request).await;
    assert!(
        result.is_ok(),
        "validate_query with parameters should return Ok"
    );
}

/// Test 1A.9: Query execution returns correct QueryStatus enum values
#[tokio::test]
async fn query_service_returns_valid_status_codes() {
    let service = QueryServiceImpl::new();
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 10,
        timeout_seconds: 5,
    };

    let response = service.execute_query(request).await.unwrap();

    // Status code should be one of the valid QueryStatus values
    let valid_status_codes = vec![
        QueryStatus::Unspecified as i32,
        QueryStatus::Success as i32,
        QueryStatus::ValidationError as i32,
        QueryStatus::ExecutionError as i32,
        QueryStatus::Timeout as i32,
        QueryStatus::Unauthorized as i32,
    ];

    assert!(
        valid_status_codes.contains(&response.status),
        "Status should be a valid QueryStatus code, got: {}",
        response.status
    );
}

/// Test 1A.10: Services implement Send + Sync for concurrent use
#[test]
fn services_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<SchemaServiceImpl>();
    assert_send_sync::<QueryServiceImpl>();
}
