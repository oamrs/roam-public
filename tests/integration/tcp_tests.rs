use oam::tcp::client::JsonRpcClient;
use oam::tcp::server::{JsonRpcServer, JsonRpcServerConfig};
use std::time::Duration;
use tempfile::NamedTempFile;

fn get_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}

#[tokio::test]
async fn tcp_server_can_be_created() {
    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: None,
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config);
    assert!(server.is_ok(), "JsonRpcServer should be creatable");
}

#[tokio::test]
async fn tcp_server_starts_on_configured_port() {
    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: None,
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await;
    assert!(handle.is_ok(), "Server should start successfully");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port)).await;

    assert!(
        client.is_ok(),
        "Client should be able to connect to running server"
    );

    // Cleanup: stop server
    let _result = handle.unwrap().stop().await;
}

#[tokio::test]
async fn tcp_client_connects_to_server() {
    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: None,
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
        .await
        .expect("create client");

    assert!(client.is_connected(), "Client should be connected");

    let _result = handle.stop().await;
}

#[tokio::test]
async fn tcp_server_serves_schema_over_network() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path.clone()),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut client = None;
    for attempt in 0..5 {
        match JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port)).await {
            Ok(c) => {
                client = Some(c);
                break;
            }
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                panic!("create client: {}", e);
            }
        }
    }
    let client = client.expect("client should be created");

    let schema_response = client.get_schema("test_db").await;
    assert!(
        schema_response.is_ok(),
        "Server should return schema over gRPC"
    );

    let schema = schema_response.unwrap();
    assert!(!schema.schema_id.is_empty(), "Schema should have an ID");

    let _result = handle.stop().await;
}

#[tokio::test]
async fn tcp_server_executes_query_over_network() {
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

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
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

#[tokio::test]
async fn tcp_server_validates_query_over_network() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)", [])
        .expect("create table");
    drop(_conn);

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
        .await
        .expect("create client");

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

#[tokio::test]
async fn tcp_server_blocks_injection_over_network() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap().to_string();

    let _conn = rusqlite::Connection::open(&db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
        .await
        .expect("create client");

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

#[tokio::test]
async fn tcp_server_handles_concurrent_clients() {
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

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create multiple concurrent clients
    let mut handles = vec![];
    let port_str = format!("http://127.0.0.1:{}", port);

    for i in 0..3 {
        let addr = port_str.clone();
        let handle = tokio::spawn(async move {
            let client = JsonRpcClient::connect(&addr).await.expect("create client");

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

#[tokio::test]
async fn tcp_server_propagates_execution_events() {
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

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: Some(db_path),
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
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

#[tokio::test]
async fn tcp_server_graceful_shutdown() {
    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: None,
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let client = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port))
        .await
        .expect("create client");
    assert!(client.is_connected(), "Client should be connected");

    let result = handle.stop().await;
    assert!(result.is_ok(), "Shutdown should succeed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let result = JsonRpcClient::connect(&format!("http://127.0.0.1:{}", port)).await;
    assert!(
        result.is_err(),
        "Client should not be able to connect after shutdown"
    );
}

#[tokio::test]
async fn tcp_server_sends_error_on_malformed_json() {
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let port = get_available_port().expect("Failed to get available port");
    let config = JsonRpcServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        db_path: None,
        auth_provider: None,
        rate_limit_config: None,
    };

    let server = JsonRpcServer::new(config).expect("create server");
    let handle = server.start().await.expect("start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut socket = tokio::net::TcpStream::connect(&format!("127.0.0.1:{}", port))
        .await
        .expect("connect to server");

    let malformed_json = "{invalid json";
    socket
        .write_all(malformed_json.as_bytes())
        .await
        .expect("write malformed json");

    let mut buffer = [0u8; 256];
    let n = socket.read(&mut buffer).await.expect("read error response");

    let response_str = String::from_utf8_lossy(&buffer[..n]);
    let response: serde_json::Value =
        serde_json::from_str(&response_str).expect("parse error response as JSON");

    assert_eq!(
        response.get("status"),
        Some(&json!(3)),
        "Should return status 3 for malformed request"
    );
    assert!(
        response
            .get("error")
            .and_then(|e| e.as_str())
            .map(|s| s.contains("Invalid JSON"))
            .unwrap_or(false),
        "Should return error message about invalid JSON"
    );

    // Cleanup
    let _ = handle.stop().await;
}
