// ============================================================================
// PHASE 1F: Tonic/gRPC Server Integration Tests
// ============================================================================
//
// These tests verify the gRPC server implementation and network-based
// communication between client and server. Following TDD discipline:
// 1. Write failing tests for desired functionality
// 2. Implement minimal production code to pass tests
// 3. Refactor for quality

use oam::grpc::client::GrpcClient;
use oam::grpc::server::{GrpcServer, GrpcServerConfig};
use std::time::Duration;
use tempfile::NamedTempFile;

/// Test 1F.1: GrpcServer can be created and configured
#[tokio::test]
async fn grpc_server_can_be_created() {
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50051,
        db_path: None,
    };

    let server = GrpcServer::new(config);
    assert!(server.is_ok(), "GrpcServer should be creatable");
}

/// Test 1F.2: GrpcServer can be started on configured port
#[tokio::test]
async fn grpc_server_starts_on_configured_port() {
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50052,
        db_path: None,
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await;
    assert!(handle.is_ok(), "Server should start successfully");

    // Give server time to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify server is running by attempting connection
    let client = GrpcClient::connect("http://127.0.0.1:50052").await;

    assert!(
        client.is_ok(),
        "Client should be able to connect to running server"
    );

    // Cleanup: stop server
    let _result = handle.unwrap().stop().await;
}

/// Test 1F.3: GrpcClient can connect to running GrpcServer
#[tokio::test]
async fn grpc_client_connects_to_server() {
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50053,
        db_path: None,
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = GrpcClient::connect("http://127.0.0.1:50053")
        .await
        .expect("create client");

    assert!(client.is_connected(), "Client should be connected");

    let _result = handle.stop().await;
}

/// Test 1F.4: GrpcServer with database path can serve schema requests
#[tokio::test]
async fn grpc_server_serves_schema_over_network() {
    // Setup test database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    // Start server with database
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50054,
        db_path: Some(db_path.clone()),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect client and request schema
    let client = GrpcClient::connect("http://127.0.0.1:50054")
        .await
        .expect("create client");

    let schema_response = client.get_schema("test_db").await;
    assert!(
        schema_response.is_ok(),
        "Server should return schema over gRPC"
    );

    let schema = schema_response.unwrap();
    assert!(!schema.schema_id.is_empty(), "Schema should have an ID");

    let _result = handle.stop().await;
}

/// Test 1F.5: GrpcServer can execute queries over network
#[tokio::test]
async fn grpc_server_executes_query_over_network() {
    // Setup test database with data
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
            [],
        )
        .expect("create table");
    _conn
        .execute(
            "INSERT INTO products (name, price) VALUES ('Widget', 9.99)",
            [],
        )
        .expect("insert row");
    _conn
        .execute(
            "INSERT INTO products (name, price) VALUES ('Gadget', 19.99)",
            [],
        )
        .expect("insert row");
    drop(_conn);

    // Start server
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50055,
        db_path: Some(db_path),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect and execute query
    let client = GrpcClient::connect("http://127.0.0.1:50055")
        .await
        .expect("create client");

    let response = client
        .execute_query("test_db", "SELECT * FROM products", 100, 30)
        .await;

    assert!(response.is_ok(), "Query should execute over gRPC");
    let result = response.unwrap();
    assert_eq!(result.row_count, 2, "Should return 2 rows");
    assert_eq!(result.status, 1, "Status should be Success (1)");

    let _result = handle.stop().await;
}

/// Test 1F.6: GrpcServer validates queries before execution
#[tokio::test]
async fn grpc_server_validates_query_over_network() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
        .expect("create table");
    drop(_conn);

    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50056,
        db_path: Some(db_path),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = GrpcClient::connect("http://127.0.0.1:50056")
        .await
        .expect("create client");

    // Try to execute a mutation (INSERT)
    let response = client
        .execute_query(
            "test_db",
            "INSERT INTO data (value) VALUES ('test')",
            100,
            30,
        )
        .await;

    assert!(response.is_ok(), "Should return error response");
    let result = response.unwrap();
    assert_eq!(result.status, 2, "Status should be ValidationError (2)");
    assert!(
        result.error_message.contains("not allowed"),
        "Error message should mention mutation restriction"
    );

    let _result = handle.stop().await;
}

/// Test 1F.7: GrpcServer rejects malicious queries over network
#[tokio::test]
async fn grpc_server_blocks_injection_over_network() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50057,
        db_path: Some(db_path),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = GrpcClient::connect("http://127.0.0.1:50057")
        .await
        .expect("create client");

    // Try command chaining injection
    let response = client
        .execute_query("test_db", "SELECT * FROM users; DROP TABLE users;", 100, 30)
        .await;

    assert!(response.is_ok(), "Should return error response");
    let result = response.unwrap();
    assert_eq!(result.status, 2, "Status should be ValidationError (2)");
    assert!(
        result.error_message.contains("semicolon") || result.error_message.contains("chaining"),
        "Should block command chaining"
    );

    let _result = handle.stop().await;
}

/// Test 1F.8: GrpcServer handles multiple concurrent clients
#[tokio::test]
async fn grpc_server_handles_concurrent_clients() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    for i in 0..5 {
        _conn
            .execute(
                "INSERT INTO items (name) VALUES (?1)",
                [format!("item_{}", i)],
            )
            .expect("insert row");
    }
    drop(_conn);

    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50058,
        db_path: Some(db_path),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create multiple concurrent clients
    let mut handles = vec![];

    for i in 0..3 {
        let handle = tokio::spawn(async move {
            let client = GrpcClient::connect("http://127.0.0.1:50058")
                .await
                .expect("create client");

            let response = client
                .execute_query(&format!("db_{}", i), "SELECT * FROM items", 100, 30)
                .await
                .expect("query succeeds");

            assert_eq!(response.row_count, 5, "Each client should see 5 rows");
            response
        });
        handles.push(handle);
    }

    // Wait for all clients to complete
    for handle in handles {
        let _result = handle.await;
    }

    let _result = handle.stop().await;
}

/// Test 1F.9: GrpcServer propagates events over gRPC
#[tokio::test]
async fn grpc_server_propagates_execution_events() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE logs (id INTEGER PRIMARY KEY, message TEXT)",
            [],
        )
        .expect("create table");
    _conn
        .execute("INSERT INTO logs (message) VALUES ('test')", [])
        .expect("insert row");
    drop(_conn);

    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50059,
        db_path: Some(db_path),
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = GrpcClient::connect("http://127.0.0.1:50059")
        .await
        .expect("create client");

    let response = client
        .execute_query("test_db", "SELECT * FROM logs", 100, 30)
        .await
        .expect("query succeeds");

    // Response should include event metadata
    assert!(!response.timestamp.is_empty(), "Should include timestamp");
    assert_eq!(response.row_count, 1, "Should return 1 row");

    let _result = handle.stop().await;
}

/// Test 1F.10: GrpcServer can be gracefully shut down
#[tokio::test]
async fn grpc_server_graceful_shutdown() {
    let config = GrpcServerConfig {
        host: "127.0.0.1".to_string(),
        port: 50060,
        db_path: None,
    };

    let server = GrpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify client can connect
    let client = GrpcClient::connect("http://127.0.0.1:50060")
        .await
        .expect("create client");
    assert!(client.is_connected(), "Client should be connected");

    // Gracefully shutdown
    let result = handle.stop().await;
    assert!(result.is_ok(), "Shutdown should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify client cannot connect after shutdown
    let result = GrpcClient::connect("http://127.0.0.1:50060").await;
    assert!(
        result.is_err(),
        "Client should not be able to connect after shutdown"
    );
}
