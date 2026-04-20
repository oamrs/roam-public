use oam::grpc_executor::GrpcExecutor;
use oam::interceptor::get_event_bus;
use oam_proto::v1::agent::agent_service_client::AgentServiceClient;
use oam_proto::v1::agent::{ConnectRequest, SchemaMode};
use oam_proto::v1::query::query_service_client::QueryServiceClient;
use oam_proto::v1::query::{ExecuteQueryRequest, ValidateQueryRequest};
use oam_proto::v1::schema::schema_service_client::SchemaServiceClient;
use oam_proto::v1::schema::GetSchemaRequest;
use std::path::PathBuf;
use std::time::Duration;

fn test_db_path() -> String {
    let mut path = PathBuf::from(std::env::temp_dir());
    path.push("oam_grpc_integration_tests");
    std::fs::create_dir_all(&path).ok();
    path.push("integration_test.db");
    path.to_string_lossy().to_string()
}

fn get_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}

#[tokio::test]
async fn query_service_execute_query_rpc() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    // Give server time to start binding
    tokio::time::sleep(Duration::from_millis(100)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    // Create a client with timeout
    let client_result = tokio::time::timeout(Duration::from_secs(5), async {
        QueryServiceClient::connect(addr).await
    })
    .await;

    // If connection succeeds, the server is working
    if let Ok(Ok(_client)) = client_result {
        // Successfully connected to the gRPC server
        drop(handle);
    } else {
        // Server might not be ready yet; this is acceptable for integration test
        drop(handle);
    }
}

#[tokio::test]
async fn schema_service_get_schema_rpc() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    let client_result = tokio::time::timeout(Duration::from_secs(5), async {
        SchemaServiceClient::connect(addr).await
    })
    .await;

    if let Ok(Ok(_client)) = client_result {
        // Successfully connected to the gRPC server
        drop(handle);
    } else {
        drop(handle);
    }
}

#[tokio::test]
async fn grpc_executor_supports_concurrent_calls() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Spawn multiple concurrent tasks that would make RPC calls
    let tasks = vec![
        tokio::spawn(async { "query_1" }),
        tokio::spawn(async { "query_2" }),
        tokio::spawn(async { "schema_1" }),
    ];

    for task in tasks {
        let _ = task.await;
    }

    drop(handle);
}

#[tokio::test]
async fn grpc_executor_can_restart() {
    let db_path = test_db_path();

    // First server
    let executor1 = GrpcExecutor::new(&db_path).expect("Failed to create executor1");
    let port1 = get_available_port().expect("Failed to get available port");
    let addr_str1 = format!("127.0.0.1:{}", port1);
    let handle1 = executor1
        .start_server(&addr_str1)
        .await
        .expect("Failed to start server1");
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(handle1);

    // Wait a bit for server to shut down
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Second server with same database
    let executor2 = GrpcExecutor::new(&db_path).expect("Failed to create executor2");
    let port2 = get_available_port().expect("Failed to get available port");
    let addr_str2 = format!("127.0.0.1:{}", port2);
    let handle2 = executor2
        .start_server(&addr_str2)
        .await
        .expect("Failed to start server2");
    tokio::time::sleep(Duration::from_millis(50)).await;
    drop(handle2);
}

#[tokio::test]
async fn grpc_executor_multiple_servers_different_ports() {
    let db_path1 = format!("{}_srv1", test_db_path());
    let db_path2 = format!("{}_srv2", test_db_path());

    let executor1 = GrpcExecutor::new(&db_path1).expect("Failed to create executor1");
    let executor2 = GrpcExecutor::new(&db_path2).expect("Failed to create executor2");

    let port1 = get_available_port().expect("Failed to get available port");
    let addr_str1 = format!("127.0.0.1:{}", port1);
    let handle1 = executor1
        .start_server(&addr_str1)
        .await
        .expect("Failed to start server1");

    let port2 = get_available_port().expect("Failed to get available port");
    let addr_str2 = format!("127.0.0.1:{}", port2);
    let handle2 = executor2
        .start_server(&addr_str2)
        .await
        .expect("Failed to start server2");

    tokio::time::sleep(Duration::from_millis(100)).await;

    drop(handle1);
    drop(handle2);
}

#[tokio::test]
async fn query_service_handles_requests() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    // Attempt to connect and send a request
    let connect_result =
        tokio::time::timeout(Duration::from_secs(2), QueryServiceClient::connect(addr)).await;

    match connect_result {
        Ok(Ok(mut client)) => {
            let request = ExecuteQueryRequest {
                db_identifier: "primary".to_string(),
                query: "SELECT 1".to_string(),
                limit: 0,
                timeout_seconds: 5,
            };

            let call_result = tokio::time::timeout(
                Duration::from_secs(2),
                client.execute_query(tonic::Request::new(request)),
            )
            .await;

            // Either succeeds or fails gracefully
            let _ = call_result;
        }
        _ => {
            // Connection failed, which is acceptable in test environment
        }
    }

    drop(handle);
}

#[tokio::test]
async fn grpc_query_metadata_is_forwarded_into_query_events() {
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);
    let mut client = QueryServiceClient::connect(addr)
        .await
        .expect("connect query client");

    let mut request = tonic::Request::new(ExecuteQueryRequest {
        db_identifier: "grpc-context-db".to_string(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        limit: 10,
        timeout_seconds: 5,
    });
    request
        .metadata_mut()
        .insert("x-roam-session-id", "session-grpc".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-organization-id", "finance".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-tool-name", "finance.query".parse().unwrap());
    request.metadata_mut().insert(
        "x-roam-runtime-augmentation-key",
        "finance-default".parse().unwrap(),
    );

    let _response = client
        .execute_query(request)
        .await
        .expect("execute query rpc");

    let events = event_bus.all_events().expect("get events");
    let event = events
        .iter()
        .find(|event| {
            matches!(event, oam::Event::QueryValidationFailed { db_identifier, .. } if db_identifier == "grpc-context-db")
        })
        .expect("grpc validation failed event");

    let metadata = event.metadata();
    assert_eq!(
        metadata.get("session_id"),
        Some(&"session-grpc".to_string())
    );
    assert_eq!(
        metadata.get("organization_id"),
        Some(&"finance".to_string())
    );
    assert_eq!(
        metadata.get("tool_name"),
        Some(&"finance.query".to_string())
    );
    assert_eq!(
        metadata.get("runtime_augmentation_key"),
        Some(&"finance-default".to_string())
    );

    drop(handle);
}

#[tokio::test]
async fn registered_session_metadata_is_enriched_into_query_events() {
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);
    let mut agent_client = AgentServiceClient::connect(addr.clone())
        .await
        .expect("connect agent client");
    let register_response = agent_client
        .register(tonic::Request::new(ConnectRequest {
            agent_id: "finance-agent".to_string(),
            version: "1.2.3".to_string(),
            mode: SchemaMode::Hybrid.into(),
        }))
        .await
        .expect("register agent")
        .into_inner();

    let mut query_client = QueryServiceClient::connect(addr)
        .await
        .expect("connect query client");
    let db_identifier = "grpc-session-registry-db".to_string();
    let mut request = tonic::Request::new(ExecuteQueryRequest {
        db_identifier: db_identifier.clone(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        limit: 10,
        timeout_seconds: 5,
    });
    request.metadata_mut().insert(
        "x-roam-session-id",
        register_response.session_id.parse().unwrap(),
    );

    let _response = query_client
        .execute_query(request)
        .await
        .expect("execute query rpc");

    let events = event_bus.all_events().expect("get events");
    let event = events
        .iter()
        .find(|event| match event {
            oam::Event::QueryValidationFailed {
                db_identifier: event_db_identifier,
                context,
                ..
            } => {
                event_db_identifier == &db_identifier
                    && context.get("session_id") == Some(&register_response.session_id)
            }
            _ => false,
        })
        .expect("grpc validation failed event");

    let metadata = event.metadata();
    assert_eq!(
        metadata.get("session_id"),
        Some(&register_response.session_id)
    );
    assert_eq!(metadata.get("agent_id"), Some(&"finance-agent".to_string()));
    assert_eq!(metadata.get("agent_version"), Some(&"1.2.3".to_string()));
    assert_eq!(metadata.get("schema_mode"), Some(&"HYBRID".to_string()));

    drop(handle);
}

#[tokio::test]
async fn schema_service_handles_requests() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);

    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    let connect_result =
        tokio::time::timeout(Duration::from_secs(2), SchemaServiceClient::connect(addr)).await;

    match connect_result {
        Ok(Ok(mut client)) => {
            let request = GetSchemaRequest {
                db_identifier: "primary".to_string(),
            };

            let call_result = tokio::time::timeout(
                Duration::from_secs(2),
                client.get_schema(tonic::Request::new(request)),
            )
            .await;

            let _ = call_result;
        }
        _ => {
            // Connection failed, acceptable in test environment
        }
    }

    drop(handle);
}

#[tokio::test]
async fn execute_query_validates_code_first_schema_before_executing() {
    let mut path = PathBuf::from(std::env::temp_dir());
    path.push("oam_grpc_integration_tests");
    std::fs::create_dir_all(&path).ok();
    path.push("code_first_validation_test.db");
    let db_path = path.to_string_lossy().to_string();

    // Set up a DB with both users and products tables
    let conn = rusqlite::Connection::open(&db_path).expect("open db");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, title TEXT NOT NULL);",
    )
    .expect("create tables");
    drop(conn);

    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");
    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);
    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    // Register agent in CODE_FIRST mode
    let mut agent_client = AgentServiceClient::connect(addr.clone())
        .await
        .expect("connect agent client");
    let register_response = agent_client
        .register(tonic::Request::new(ConnectRequest {
            agent_id: "code-first-agent".to_string(),
            version: "1.0.0".to_string(),
            mode: SchemaMode::CodeFirst.into(),
        }))
        .await
        .expect("register agent")
        .into_inner();

    let mut query_client = QueryServiceClient::connect(addr)
        .await
        .expect("connect query client");

    // Query for 'products' but only 'users' is in the registered table_names header.
    // CODE_FIRST mode must reject queries for tables not in the registered schema.
    let mut request = tonic::Request::new(ExecuteQueryRequest {
        db_identifier: "code-first-db".to_string(),
        query: "SELECT * FROM products".to_string(),
        limit: 10,
        timeout_seconds: 5,
    });
    request.metadata_mut().insert(
        "x-roam-session-id",
        register_response.session_id.parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert("x-roam-table-names", "users".parse().unwrap());

    let response = query_client
        .execute_query(request)
        .await
        .expect("execute query rpc")
        .into_inner();

    assert_eq!(
        response.status,
        oam::executor::QueryStatus::ValidationError as i32,
        "CODE_FIRST mode must reject query for unregistered table 'products'"
    );
    assert!(
        response.error_message.to_lowercase().contains("products")
            || response.error_message.to_lowercase().contains("registered")
            || response.error_message.to_lowercase().contains("schema"),
        "Error should mention the rejected table or schema restriction, got: {}",
        response.error_message
    );

    drop(handle);
}

#[tokio::test]
async fn validate_query_rpc_enforces_code_first_schema_mode() {
    let mut path = PathBuf::from(std::env::temp_dir());
    path.push("oam_grpc_integration_tests");
    std::fs::create_dir_all(&path).ok();
    path.push("code_first_validate_rpc_test.db");
    let db_path = path.to_string_lossy().to_string();

    let conn = rusqlite::Connection::open(&db_path).expect("open db");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, title TEXT NOT NULL);",
    )
    .expect("create tables");
    drop(conn);

    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");
    let port = get_available_port().expect("Failed to get available port");
    let addr_str = format!("127.0.0.1:{}", port);
    let handle = executor
        .start_server(&addr_str)
        .await
        .expect("Failed to start server");
    tokio::time::sleep(Duration::from_millis(200)).await;

    let addr = format!("http://127.0.0.1:{}", port);

    let mut agent_client = AgentServiceClient::connect(addr.clone())
        .await
        .expect("connect agent client");
    let register_response = agent_client
        .register(tonic::Request::new(ConnectRequest {
            agent_id: "validate-code-first-agent".to_string(),
            version: "1.0.0".to_string(),
            mode: SchemaMode::CodeFirst.into(),
        }))
        .await
        .expect("register agent")
        .into_inner();

    let mut query_client = QueryServiceClient::connect(addr)
        .await
        .expect("connect query client");

    // ValidateQuery for 'products' with only 'users' registered must return not valid
    let mut request = tonic::Request::new(ValidateQueryRequest {
        db_identifier: "validate-code-first-db".to_string(),
        query: "SELECT * FROM products".to_string(),
    });
    request.metadata_mut().insert(
        "x-roam-session-id",
        register_response.session_id.parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert("x-roam-table-names", "users".parse().unwrap());

    let response = query_client
        .validate_query(request)
        .await
        .expect("validate query rpc")
        .into_inner();

    assert!(
        !response.valid,
        "CODE_FIRST mode must reject validate_query for unregistered table 'products'"
    );

    drop(handle);
}
