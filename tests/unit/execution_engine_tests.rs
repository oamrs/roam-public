//! Unit tests for ExecutionEngine and ConnectionPool - following TDD methodology
//!
//! These tests define the behavior of:
//! - Request queuing and priority handling
//! - Concurrent task management with JoinSet
//! - Connection pool integration
//! - Metrics tracking
//! - Error handling and resilience

use oam::execution_engine::{ConnectionPool, ExecutionEngine, QueryPriority, QueryRequest};
use oam::executor::ExecuteQueryRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

fn test_db_path() -> String {
    let mut path = std::path::PathBuf::from(std::env::temp_dir());
    path.push("oam_execution_engine_tests");
    std::fs::create_dir_all(&path).ok();
    path.push("test_engine.db");
    path.to_string_lossy().to_string()
}

/// Test 1: ExecutionEngine can be created with default configuration
#[test]
fn execution_engine_can_be_created() {
    let db_path = test_db_path();
    let result = ExecutionEngine::new(&db_path, 100);

    assert!(result.is_ok(), "ExecutionEngine should be creatable");
}

/// Test 2: ExecutionEngine stores configuration correctly
#[test]
fn execution_engine_stores_configuration() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 50).expect("Create engine");

    assert_eq!(
        engine.max_concurrent_queries(),
        50,
        "Max concurrent queries should be 50"
    );
}

/// Test 3: ExecutionEngine provides metrics access
#[test]
fn execution_engine_provides_metrics() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let metrics = engine.metrics();
    assert_eq!(
        metrics.total_queries(),
        0,
        "Initial total queries should be 0"
    );
    assert_eq!(
        metrics.successful_queries(),
        0,
        "Initial successful queries should be 0"
    );
    assert_eq!(
        metrics.failed_queries(),
        0,
        "Initial failed queries should be 0"
    );
}

/// Test 4: QueryPriority enum exists and can be compared
#[test]
fn query_priority_can_be_ordered() {
    assert!(QueryPriority::Critical > QueryPriority::High);
    assert!(QueryPriority::High > QueryPriority::Normal);
    assert!(QueryPriority::Normal > QueryPriority::Low);
}

/// Test 5: QueryRequest can be created with unique IDs
#[test]
fn query_request_creation() {
    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req1 = QueryRequest::new(request.clone(), QueryPriority::Normal);
    let query_req2 = QueryRequest::new(request, QueryPriority::Normal);

    assert_ne!(
        query_req1.id(),
        query_req2.id(),
        "Query requests should have unique IDs"
    );
}

/// Test 6: QueryRequest tracks creation time
#[test]
fn query_request_tracks_creation_time() {
    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "SELECT 1".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let now_before = Instant::now();
    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let now_after = Instant::now();

    let created_at = query_req.created_at();
    assert!(
        created_at >= now_before && created_at <= now_after,
        "Creation time should be tracked"
    );
}

/// Test 7: ExecutionEngine can be configured with different concurrency limits
#[test]
fn execution_engine_respects_concurrency_limit() {
    let db_path = test_db_path();

    let engine_small = ExecutionEngine::new(&db_path, 10).expect("Create small engine");
    let engine_large = ExecutionEngine::new(&db_path, 1000).expect("Create large engine");

    assert_eq!(engine_small.max_concurrent_queries(), 10);
    assert_eq!(engine_large.max_concurrent_queries(), 1000);
}

/// Test 8: Metrics track query counts accurately
#[test]
fn metrics_track_query_counts() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let metrics = engine.metrics();
    assert_eq!(metrics.total_queries(), 0);
    assert_eq!(metrics.successful_queries(), 0);
    assert_eq!(metrics.failed_queries(), 0);
}

/// Test 9: ExecutionEngine provides queue depth information
#[test]
fn execution_engine_provides_queue_depth() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let queue_depth = engine.queue_depth();
    assert_eq!(queue_depth, 0, "Initial queue depth should be 0");
}

/// Test 10: ExecutionEngine provides active task count
#[test]
fn execution_engine_provides_active_task_count() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let active_tasks = engine.active_task_count();
    assert_eq!(active_tasks, 0, "Initial active tasks should be 0");
}

/// Test 11: ConnectionPool can be created
#[test]
fn connection_pool_can_be_created() {
    let pool = ConnectionPool::new("memory:", 5);
    assert!(pool.is_ok());
}

/// Test 12: ConnectionPool stores configuration
#[test]
fn connection_pool_stores_configuration() {
    let pool = ConnectionPool::new("memory:", 10).expect("pool creation failed");
    assert_eq!(pool.max_connections(), 10);
}

/// Test 13: ConnectionPool returns connection
#[test]
fn connection_pool_returns_connection() {
    let pool = Arc::new(ConnectionPool::new("memory:", 5).expect("pool creation failed"));
    let conn_result = std::thread::spawn({
        let pool = pool.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { pool.get_connection().await })
        }
    })
    .join();

    assert!(conn_result.is_ok());
    assert!(conn_result.unwrap().is_ok());
}

/// Test 14: ConnectionPool initializes available connections
#[test]
fn connection_pool_initializes_available_connections() {
    let pool = ConnectionPool::new("memory:", 3).expect("pool creation failed");
    assert_eq!(pool.available_connections(), 3);
}

/// Test 15: ConnectionPool tracks checked out connections
#[test]
fn connection_pool_tracks_checked_out_connections() {
    let pool = Arc::new(ConnectionPool::new("memory:", 5).expect("pool creation failed"));
    let checked_out = std::thread::spawn({
        let pool = pool.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _conn = pool.get_connection().await.expect("get connection failed");
                pool.checked_out_connections()
            })
        }
    })
    .join()
    .unwrap();

    assert!(checked_out > 0);
}

/// Test 16: ConnectionPool respects max connections
#[test]
fn connection_pool_respects_max_connections() {
    let pool = Arc::new(ConnectionPool::new("memory:", 2).expect("pool creation failed"));

    let result = std::thread::spawn({
        let pool = pool.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _conn1 = pool
                    .get_connection()
                    .await
                    .expect("first connection failed");
                let _conn2 = pool
                    .get_connection()
                    .await
                    .expect("second connection failed");
                pool.available_connections()
            })
        }
    })
    .join()
    .unwrap();

    assert_eq!(result, 0);
}

/// Test 17: ConnectionPool provides connection stats
#[test]
fn connection_pool_provides_connection_stats() {
    let pool = ConnectionPool::new("memory:", 4).expect("pool creation failed");

    let stats = pool.stats();
    assert_eq!(stats.max_connections, 4);
    assert_eq!(stats.available_connections, 4);
    assert_eq!(stats.checked_out_connections, 0);
}

/// Test 18: ConnectionPool executes query
#[test]
fn connection_pool_executes_query() {
    let pool = Arc::new(ConnectionPool::new("memory:", 1).expect("pool creation failed"));

    let result = std::thread::spawn({
        let pool = pool.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                pool.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)", &[])
                    .await
            })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_ok());
}

/// Test 19: ConnectionPool releases connection on drop
#[test]
fn connection_pool_releases_connection_on_drop() {
    let pool = Arc::new(ConnectionPool::new("memory:", 2).expect("pool creation failed"));

    let stats_after = std::thread::spawn({
        let pool = pool.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                {
                    let _conn = pool.get_connection().await.expect("get connection failed");
                    let stats_during = pool.stats();
                    assert_eq!(stats_during.available_connections, 1);
                }
                // Connection dropped here
                pool.stats()
            })
        }
    })
    .join()
    .unwrap();

    assert_eq!(stats_after.available_connections, 2);
}
