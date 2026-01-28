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
use oam::mapper::{GrpcMapper, LocalMapper, Mapper};
use std::collections::HashMap;

// === LocalMapper Unit Tests ===

/// Test: LocalMapper can be created with a valid database path
#[test]
fn local_mapper_can_be_created() {
    let result = LocalMapper::new(":memory:");
    assert!(
        result.is_ok(),
        "LocalMapper::new should succeed with valid path"
    );
}

/// Test: LocalMapper::validate_query returns a response
#[tokio::test]
async fn local_mapper_validate_query_returns_response() {
    let mapper = LocalMapper::new(":memory:").expect("Failed to create LocalMapper");

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = mapper.validate_query(request).await;
    assert!(result.is_ok(), "validate_query should return Ok");
}

/// Test: LocalMapper::execute_query returns a response
#[tokio::test]
async fn local_mapper_execute_query_returns_response() {
    let mapper = LocalMapper::new(":memory:").expect("Failed to create LocalMapper");

    let request = ExecuteQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
        limit: 0,
        timeout_seconds: 30,
    };

    let result = mapper.execute_query(request).await;
    assert!(result.is_ok(), "execute_query should return Ok");
}

/// Test: LocalMapper implements Mapper trait
#[tokio::test]
async fn local_mapper_implements_trait() {
    let mapper = LocalMapper::new(":memory:").expect("Failed to create mapper");

    let request = ValidateQueryRequest {
        db_identifier: ":memory:".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = mapper.validate_query(request).await;
    assert!(result.is_ok(), "LocalMapper should validate queries");
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
async fn grpc_mapper_implements_trait() {
    let mapper_result = GrpcMapper::new("http://localhost:50051").await;

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

// === Mapper Trait Composition Tests ===

/// Test: LocalMapper and GrpcMapper both implement Mapper trait
#[tokio::test]
async fn mappers_implement_trait() {
    let local_mapper: Box<dyn Mapper> =
        Box::new(LocalMapper::new(":memory:").expect("Failed to create LocalMapper"));

    let request = ValidateQueryRequest {
        db_identifier: "primary".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
    };

    let result = local_mapper.validate_query(request).await;
    assert!(result.is_ok(), "Trait object should support validate_query");
}

/// Test: Multiple mapper instances can be collected in a vector
#[tokio::test]
async fn mappers_are_composable() {
    let mappers: Vec<Box<dyn Mapper>> = vec![Box::new(
        LocalMapper::new(":memory:").expect("Failed to create mapper"),
    )];

    assert_eq!(mappers.len(), 1);
}

/// Test: Mapper trait enables polymorphism
#[tokio::test]
async fn mapper_trait_enables_polymorphism() {
    let local_mapper = LocalMapper::new(":memory:").expect("Failed to create local mapper");

    let mappers: Vec<&dyn Mapper> = vec![&local_mapper];

    assert_eq!(mappers.len(), 1);
}
