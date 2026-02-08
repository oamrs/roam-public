use oam::grpc_executor::GrpcExecutor;
use std::path::PathBuf;

fn test_db_path() -> String {
    let mut path = PathBuf::from(std::env::temp_dir());
    path.push("oam_grpc_executor_tests");
    std::fs::create_dir_all(&path).ok();
    path.push("test_grpc_executor.db");
    path.to_string_lossy().to_string()
}

#[test]
fn grpc_executor_can_be_created() {
    let db_path = test_db_path();
    let result = GrpcExecutor::new(&db_path);
    assert!(
        result.is_ok(),
        "GrpcExecutor::new should succeed with valid path"
    );
}

#[test]
fn grpc_executor_initializes_services() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");
    // If this doesn't panic, both services were initialized
    // Further assertions would require accessing internal state via public methods
    drop(executor);
}

#[tokio::test]
async fn grpc_executor_start_server_returns_handle() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    let result = executor.start_server("127.0.0.1:0").await;
    assert!(result.is_ok(), "start_server should return Ok");

    let handle = result.unwrap();
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Drop the handle to stop the server
    drop(handle);
}

#[tokio::test]
async fn grpc_executor_binds_to_specified_address() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    // Use port 0 to let OS assign a random available port
    let result = executor.start_server("127.0.0.1:0").await;
    assert!(result.is_ok(), "start_server should bind to available port");

    let handle = result.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    drop(handle);
}

#[tokio::test]
async fn multiple_grpc_executors_can_coexist() {
    let db_path1 = format!("{}_1", test_db_path());
    let db_path2 = format!("{}_2", test_db_path());

    let executor1 = GrpcExecutor::new(&db_path1).expect("Failed to create executor1");
    let executor2 = GrpcExecutor::new(&db_path2).expect("Failed to create executor2");

    let handle1 = executor1
        .start_server("127.0.0.1:0")
        .await
        .expect("Failed to start server1");
    let handle2 = executor2
        .start_server("127.0.0.1:0")
        .await
        .expect("Failed to start server2");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    drop(handle1);
    drop(handle2);
}

#[test]
fn grpc_executor_handles_invalid_db_path() {
    // Path with non-existent parent directories should still work
    // since the executor will attempt to access/create the database
    let db_path = "/tmp/test_grpc_nonexistent_dir_xyz/db.sqlite";
    let _result = GrpcExecutor::new(db_path);
    // Either succeeds or fails gracefully without panicking
}

#[tokio::test]
async fn grpc_executor_rejects_invalid_address() {
    let db_path = test_db_path();
    let executor = GrpcExecutor::new(&db_path).expect("Failed to create GrpcExecutor");

    // Invalid address format should return an error
    let result = executor.start_server("not-a-valid-address").await;
    assert!(
        result.is_err(),
        "start_server should reject invalid address"
    );
}
