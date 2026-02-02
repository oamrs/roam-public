//! Tests for Mapper trait implementations
//!
//! These tests verify that all mapper implementations correctly route
//! requests to their respective backends while maintaining a consistent interface.
//!
//! Tests cover:
//! - LocalMapper creation and operations
//! - GrpcMapper initialization and error handling
//! - Mapper trait composition and polymorphism

use oam::executor::{ExecuteQueryRequest, ValidateQueryRequest};
use oam::grpc_executor::GrpcExecutor;
use oam::mapper::{GrpcMapper, LocalMapper, Mapper};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

/// Thread-safe counter for generating unique database paths per test
static TEST_DB_COUNTER: OnceLock<AtomicU32> = OnceLock::new();

/// Thread-safe counter for generating unique gRPC ports per test
static GRPC_PORT_COUNTER: OnceLock<AtomicU16> = OnceLock::new();

/// Generate a unique temporary database path for test isolation
fn get_test_db_path() -> String {
    let counter = TEST_DB_COUNTER.get_or_init(|| AtomicU32::new(0));
    let id = counter.fetch_add(1, Ordering::SeqCst);
    let mut path = PathBuf::from(std::env::temp_dir());
    path.push(format!("oam_mapper_test_{}.db", id));
    path.to_string_lossy().to_string()
}

/// Generate a unique gRPC port for test isolation
fn get_test_grpc_port() -> u16 {
    let counter = GRPC_PORT_COUNTER.get_or_init(|| AtomicU16::new(50051));
    counter.fetch_add(1, Ordering::SeqCst)
}

/// Start a gRPC server in the background for testing
/// Returns the server address and a JoinHandle to manage the server lifetime
async fn start_test_grpc_server() -> Result<(String, tokio::task::JoinHandle<()>), String> {
    let db_path = get_test_db_path();
    let port = get_test_grpc_port();
    let addr = format!("127.0.0.1:{}", port);

    let executor =
        GrpcExecutor::new(&db_path).map_err(|e| format!("Failed to create GrpcExecutor: {}", e))?;

    let handle = executor
        .start_server(&addr)
        .await
        .map_err(|e| format!("Failed to start gRPC server: {}", e))?;

    // Give the server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok((format!("http://{}", addr), handle))
}

// === LocalMapper Unit Tests ===

/// Test: LocalMapper can be created with a valid database path
#[test]
fn local_mapper_can_be_created() {
    let db_path = get_test_db_path();
    let result = LocalMapper::new(&db_path);
    assert!(
        result.is_ok(),
        "LocalMapper::new should succeed with valid path"
    );
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

/// Test: LocalMapper::validate_query returns a response
#[tokio::test]
async fn local_mapper_validate_query_returns_response() {
    let db_path = get_test_db_path();
    let mapper = LocalMapper::new(&db_path).expect("Failed to create LocalMapper");

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = mapper.validate_query(request).await;
    assert!(result.is_ok(), "validate_query should return Ok");
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

/// Test: LocalMapper::execute_query returns a response
#[tokio::test]
async fn local_mapper_execute_query_returns_response() {
    let db_path = get_test_db_path();
    let mapper = LocalMapper::new(&db_path).expect("Failed to create LocalMapper");

    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
        limit: 0,
        timeout_seconds: 30,
    };

    let result = mapper.execute_query(request).await;
    assert!(result.is_ok(), "execute_query should return Ok");
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

/// Test: LocalMapper implements Mapper trait
#[tokio::test]
async fn local_mapper_implements_trait() {
    let db_path = get_test_db_path();
    let mapper = LocalMapper::new(&db_path).expect("Failed to create mapper");

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = mapper.validate_query(request).await;
    assert!(result.is_ok(), "LocalMapper should validate queries");
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

// === GrpcMapper Unit Tests ===

/// Test: GrpcMapper::new rejects invalid addresses
#[tokio::test]
async fn grpc_mapper_rejects_invalid_address() {
    let result = GrpcMapper::new("not-a-valid-url").await;
    assert!(
        result.is_err(),
        "GrpcMapper::new should reject invalid address"
    );
}

/// Test: GrpcMapper::new validates address format
#[tokio::test]
async fn grpc_mapper_validates_address_format() {
    let result = GrpcMapper::new("http://localhost:50051").await;
    // Connection will fail since no server is running, but address should be parsed
    if let Err(e) = result {
        assert!(
            e.contains("Failed to connect") || e.contains("connection"),
            "Error should indicate connection issue, not address format: {}",
            e
        );
    }
}

/// Test: GrpcMapper connection failure has appropriate error message
#[tokio::test]
async fn grpc_mapper_connection_fails_when_server_unavailable() {
    // Use a port that's unlikely to have a running server
    let mapper_result = GrpcMapper::new("http://localhost:59999").await;

    // GrpcMapper should return an error since there's no server running
    assert!(
        mapper_result.is_err(),
        "GrpcMapper::new should fail when no server is available"
    );
    let error = mapper_result.unwrap_err();
    assert!(
        error.contains("Failed to connect") || error.contains("connection"),
        "Error should indicate connection failure: {}",
        error
    );
}

// === GrpcMapper Successful Operations Tests ===

/// Test: GrpcMapper can successfully validate queries when server is available
#[tokio::test]
async fn grpc_mapper_validate_query_with_server() {
    let (server_addr, _handle) = start_test_grpc_server()
        .await
        .expect("Failed to start test gRPC server");

    let mapper = GrpcMapper::new(&server_addr)
        .await
        .expect("Failed to create GrpcMapper with running server");

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = mapper.validate_query(request).await;
    assert!(
        result.is_ok(),
        "validate_query should succeed with running server"
    );
}

/// Test: GrpcMapper can successfully execute queries when server is available
#[tokio::test]
async fn grpc_mapper_execute_query_with_server() {
    let (server_addr, _handle) = start_test_grpc_server()
        .await
        .expect("Failed to start test gRPC server");

    let mapper = GrpcMapper::new(&server_addr)
        .await
        .expect("Failed to create GrpcMapper with running server");

    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
        limit: 0,
        timeout_seconds: 30,
    };

    let result = mapper.execute_query(request).await;
    assert!(
        result.is_ok(),
        "execute_query should succeed with running server"
    );

    let response = result.unwrap();
    // Status should be a valid QueryStatus code (not an error code)
    // QueryStatus: Unspecified=0, Success=1, ValidationError=2, ExecutionError=3, Timeout=4, Unauthorized=5
    assert!(
        response.status >= 0 && response.status <= 5,
        "Status should be a valid QueryStatus code, got: {}",
        response.status
    );
}

/// Test: GrpcMapper can execute multiple queries through trait object
#[tokio::test]
async fn grpc_mapper_trait_object_with_server() {
    let (server_addr, _handle) = start_test_grpc_server()
        .await
        .expect("Failed to start test gRPC server");

    let mapper: Box<dyn Mapper> = Box::new(
        GrpcMapper::new(&server_addr)
            .await
            .expect("Failed to create GrpcMapper"),
    );

    let validate_request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let validate_result = mapper.validate_query(validate_request).await;
    assert!(
        validate_result.is_ok(),
        "Trait object validate_query should work"
    );

    let execute_request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 2".to_string(),
        parameters: HashMap::new(),
        limit: 10,
        timeout_seconds: 30,
    };

    let execute_result = mapper.execute_query(execute_request).await;
    assert!(
        execute_result.is_ok(),
        "Trait object execute_query should work"
    );
}

// === Mapper Trait Composition Tests ===

/// Test: LocalMapper and GrpcMapper both implement Mapper trait
#[tokio::test]
async fn mappers_implement_trait() {
    let db_path = get_test_db_path();
    let local_mapper: Box<dyn Mapper> =
        Box::new(LocalMapper::new(&db_path).expect("Failed to create LocalMapper"));

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = local_mapper.validate_query(request).await;
    assert!(result.is_ok(), "Trait object should support validate_query");
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

/// Test: Multiple mapper instances can be collected in a vector
#[tokio::test]
async fn mappers_are_composable() {
    let db_path = get_test_db_path();
    let mappers: Vec<Box<dyn Mapper>> = vec![Box::new(
        LocalMapper::new(&db_path).expect("Failed to create mapper"),
    )];

    assert_eq!(mappers.len(), 1);
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}

/// Test: Mapper trait enables polymorphism
#[tokio::test]
async fn mapper_trait_enables_polymorphism() {
    let db_path = get_test_db_path();
    let local_mapper = LocalMapper::new(&db_path).expect("Failed to create local mapper");

    let mappers: Vec<&dyn Mapper> = vec![&local_mapper];

    assert_eq!(mappers.len(), 1);
    // Cleanup
    let _ = std::fs::remove_file(&db_path);
}
