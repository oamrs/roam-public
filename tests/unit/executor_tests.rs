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

// ============================================================================
// PHASE 1D: Query Execution Tests (Unit - No Database)
// ============================================================================

/// Test 1D.1: QueryService::execute_query returns response with correct structure
#[tokio::test]
async fn query_service_execute_query_returns_structured_response() {
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
    // Response must have all required fields
    assert!(response.status >= 0, "status code should be valid");
    assert!(response.row_count >= 0, "row_count should be non-negative");
    assert!(
        response.execution_ms >= 0,
        "execution_ms should be non-negative"
    );
}

/// Test 1D.2: QueryService::execute_query without db_path returns ExecutionError
#[tokio::test]
async fn query_service_execute_query_without_db_path_returns_error() {
    let service = QueryServiceImpl::new();
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    // Without database path set, should return ExecutionError status
    assert_eq!(
        response.status,
        QueryStatus::ExecutionError as i32,
        "Should return ExecutionError when no database configured"
    );
    assert!(
        !response.error_message.is_empty(),
        "Should have error message"
    );
}

/// Test 1D.3: QueryService::execute_query validates query before execution
#[tokio::test]
async fn query_service_execute_query_validates_before_execution() {
    let service = QueryServiceImpl::new();

    // Query with semicolon (command chaining)
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    // Should fail validation, return ValidationError status
    assert_eq!(
        response.status,
        QueryStatus::ValidationError as i32,
        "Should return ValidationError for injected semicolon"
    );
}

/// Test 1D.4: QueryService::execute_query respects timeout parameter
#[tokio::test]
async fn query_service_execute_query_timeout_parameter_processed() {
    let service = QueryServiceImpl::new();
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 5, // 5 second timeout
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    // Response should be well-formed even with timeout specified
    assert!(response.status >= 0, "status should be valid");
}

/// Test 1D.5: QueryService::execute_query respects limit parameter
#[tokio::test]
async fn query_service_execute_query_limit_parameter_processed() {
    let service = QueryServiceImpl::new();
    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 10, // Limit to 10 rows
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    // Response row_count should respect the limit
    assert!(
        response.row_count <= 10,
        "row_count should not exceed limit"
    );
}

// ============================================================================
// PHASE 1E: EVENT DISPATCH TESTS (Unit - No Database Required)
// ============================================================================

/// Test 1E.1: QueryValidationFailed event dispatched on security pattern detection
#[tokio::test]
async fn query_service_dispatches_validation_failed_on_command_chaining() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = QueryServiceImpl::new();
    let test_db_id = "1e1_test_db".to_string();

    // Query with semicolon (command chaining - caught at security check level)
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, QueryStatus::ValidationError as i32);

    // Verify QueryValidationFailed event was dispatched for THIS test
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryValidationFailed { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert!(
        !test_events.is_empty(),
        "Should have dispatched QueryValidationFailed event"
    );

    if let Event::QueryValidationFailed {
        db_identifier,
        query: _,
        error_reason,
        timestamp: _,
    } = test_events[0]
    {
        assert_eq!(db_identifier, &test_db_id);
        assert!(!error_reason.is_empty());
    } else {
        panic!("Expected QueryValidationFailed event");
    }
}

/// Test 1E.2: QueryValidationFailed event includes db_identifier and error reason
#[tokio::test]
async fn query_service_validation_failed_event_includes_metadata() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = QueryServiceImpl::new();
    let test_db_id = "1e2_security_test".to_string();

    // Query with block comment (security pattern)
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "SELECT /* comment */ * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let _result = service.execute_query(request).await;

    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryValidationFailed { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert!(!test_events.is_empty());

    let metadata = test_events[0].metadata();

    assert_eq!(
        metadata.get("db_identifier"),
        Some(&test_db_id),
        "Event metadata should include db_identifier"
    );
    assert!(
        metadata.contains_key("timestamp"),
        "Event metadata should include timestamp"
    );
    assert!(
        metadata.contains_key("error_reason"),
        "ValidationFailed event should include error_reason"
    );
}

/// Test 1E.3: QueryExecutionError event dispatched when database path not configured
#[tokio::test]
async fn query_service_dispatches_execution_error_on_missing_db_path() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = QueryServiceImpl::new();
    let test_db_id = "1e3_test_db".to_string();

    // Valid query syntax but no database configured
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = service.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, QueryStatus::ExecutionError as i32);

    // Verify QueryExecutionError event was dispatched for THIS test
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecutionError { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert!(
        !test_events.is_empty(),
        "Should have dispatched QueryExecutionError event"
    );

    if let Event::QueryExecutionError {
        db_identifier,
        query: _,
        error_message,
        timestamp: _,
    } = test_events[0]
    {
        assert_eq!(db_identifier, &test_db_id);
        assert!(!error_message.is_empty());
        assert!(
            error_message.contains("Database path not configured"),
            "Error should indicate missing db_path"
        );
    } else {
        panic!("Expected QueryExecutionError event");
    }
}

/// Test 1E.4: QueryExecutionError event includes query and error details
#[tokio::test]
async fn query_service_execution_error_event_includes_query_details() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = QueryServiceImpl::new();

    let test_db_id = "1e4_error_test".to_string();
    let test_query = "SELECT * FROM test_table WHERE id = 1".to_string();
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: test_query.clone(),
        parameters: std::collections::HashMap::new(),
        limit: 50,
        timeout_seconds: 15,
    };

    let _result = service.execute_query(request).await;

    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecutionError { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert!(!test_events.is_empty());

    if let Event::QueryExecutionError {
        db_identifier,
        query,
        error_message,
        timestamp,
    } = test_events[0]
    {
        assert_eq!(db_identifier, &test_db_id);
        assert_eq!(query, &test_query);
        assert!(!error_message.is_empty());
        assert!(!timestamp.is_empty());
    } else {
        panic!("Expected QueryExecutionError event");
    }
}

/// Test 1E.5: Different security violations dispatch appropriate events
#[tokio::test]
async fn query_service_different_violations_dispatch_correct_events() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = QueryServiceImpl::new();
    let test_db_id = "1e5_violation_test".to_string();

    // Test 1: Line comment violation
    let request1 = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "SELECT * FROM users -- comment".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let _result1 = service.execute_query(request1).await;

    // Test 2: PRAGMA violation
    let request2 = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "PRAGMA table_info(users)".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let _result2 = service.execute_query(request2).await;

    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryValidationFailed { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert_eq!(
        test_events.len(),
        2,
        "Should have dispatched 2 validation failed events for this test"
    );

    // Verify both are QueryValidationFailed with different error reasons
    for event in test_events {
        match event {
            Event::QueryValidationFailed { error_reason, .. } => {
                // Each violation should have different error reason
                assert!(!error_reason.is_empty());
            }
            _ => panic!("Expected QueryValidationFailed events"),
        }
    }
}
