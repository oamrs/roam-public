use oam::grpc_executor::{table_to_table_def, GrpcExecutor};
use oam::mirror::{Column, Index, Table, Trigger, UniqueIndex};
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

#[test]
fn table_to_table_def_maps_name_and_columns() {
    let table = Table {
        name: "users".to_string(),
        columns: vec![Column {
            name: "id".to_string(),
            sql_type: "INTEGER".to_string(),
            nullable: false,
            primary_key: true,
            default_value: None,
            enum_values: None,
        }],
        foreign_keys: vec![],
        composite_foreign_keys: vec![],
        unique_indexes: vec![],
        indexes: vec![],
        triggers: vec![],
        field_mappings: vec![],
    };

    let def = table_to_table_def(&table);

    assert_eq!(def.name, "users");
    assert_eq!(def.columns.len(), 1);
    assert_eq!(def.columns[0].name, "id");
    assert!(def.columns[0].is_primary_key);
    assert!(!def.columns[0].is_nullable);
}

#[test]
fn table_to_table_def_maps_unique_indexes() {
    let table = Table {
        name: "events".to_string(),
        columns: vec![],
        foreign_keys: vec![],
        composite_foreign_keys: vec![],
        unique_indexes: vec![UniqueIndex {
            name: "idx_events_key".to_string(),
            columns: vec!["key".to_string()],
        }],
        indexes: vec![Index {
            name: "idx_events_created_at".to_string(),
            columns: vec!["created_at".to_string()],
        }],
        triggers: vec![],
        field_mappings: vec![],
    };

    let def = table_to_table_def(&table);

    // Both unique and regular indexes should appear in def.indexes (unique ones with is_unique=true)
    let unique_idx = def.indexes.iter().find(|i| i.name == "idx_events_key");
    assert!(unique_idx.is_some(), "unique index should be present");
    assert!(
        unique_idx.unwrap().is_unique,
        "unique index should have is_unique=true"
    );

    let regular_idx = def
        .indexes
        .iter()
        .find(|i| i.name == "idx_events_created_at");
    assert!(regular_idx.is_some(), "regular index should be present");
    assert!(
        !regular_idx.unwrap().is_unique,
        "regular index should have is_unique=false"
    );
}

#[test]
fn table_to_table_def_maps_triggers() {
    let table = Table {
        name: "audit".to_string(),
        columns: vec![],
        foreign_keys: vec![],
        composite_foreign_keys: vec![],
        unique_indexes: vec![],
        indexes: vec![],
        triggers: vec![Trigger {
            name: "trg_audit_after_insert".to_string(),
            event: "INSERT".to_string(),
            timing: "AFTER".to_string(),
            table_name: "audit".to_string(),
            body: "BEGIN SELECT 1; END".to_string(),
        }],
        field_mappings: vec![],
    };

    let def = table_to_table_def(&table);

    assert_eq!(def.triggers.len(), 1);
    assert_eq!(def.triggers[0].name, "trg_audit_after_insert");
    assert_eq!(def.triggers[0].timing, "AFTER");
    assert_eq!(def.triggers[0].event, "INSERT");
}
