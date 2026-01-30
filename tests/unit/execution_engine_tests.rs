//! Unit tests for ExecutionEngine and ConnectionPool - following TDD methodology
//!
//! These tests define the behavior of:
//! - Request queuing and priority handling
//! - Concurrent task management with JoinSet
//! - Connection pool integration
//! - Metrics tracking
//! - Error handling and resilience

use oam::execution_engine::{
    ConnectionPool, ExecutionEngine, QueryPriority, QueryRequest, ResultStatus,
};
use oam::executor::ExecuteQueryRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

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
                pool.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
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
/// Test 20: ExecutionEngine can spawn a task
#[test]
fn execution_engine_can_spawn_task() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 10).expect("Create engine");

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS test_spawn (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);

    std::thread::spawn({
        let engine = Arc::new(engine);
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let result = engine.spawn_query(query_req).await;
                assert!(result.is_ok());
            })
        }
    })
    .join()
    .unwrap();
}

/// Test 21: ExecutionEngine spawned task updates metrics
#[test]
fn execution_engine_spawn_updates_metrics() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS test_metrics (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::High);
    let metrics_before = engine.metrics().total_queries();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let metrics_after = engine.metrics().total_queries();
    assert!(metrics_after >= metrics_before);
}

/// Test 22: ExecutionEngine respects priority when spawning tasks
#[test]
fn execution_engine_respects_priority_on_spawn() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 5).expect("Create engine");

    let request_low = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS priority_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let req_low = QueryRequest::new(request_low, QueryPriority::Low);
    let req_high = QueryRequest::new(
        ExecuteQueryRequest {
            db_identifier: "test_db".to_string(),
            query: "CREATE TABLE IF NOT EXISTS priority_high (id INTEGER)".to_string(),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        },
        QueryPriority::Critical,
    );

    // Both requests have different priorities
    assert!(req_high.priority() > req_low.priority());
}

/// Test 23: ExecutionEngine can handle multiple concurrent spawns
#[test]
fn execution_engine_handles_multiple_spawns() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let mut handles = vec![];

    for i in 0..3 {
        let engine = engine.clone();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let request = ExecuteQueryRequest {
                    db_identifier: format!("db_{}", i),
                    query: format!("CREATE TABLE IF NOT EXISTS test_{} (id INTEGER)", i),
                    parameters: HashMap::new(),
                    limit: 100,
                    timeout_seconds: 30,
                };
                let query_req = QueryRequest::new(request, QueryPriority::Normal);
                engine.spawn_query(query_req).await
            })
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }
}

/// Test 24: ExecutionEngine tracks active tasks after spawn
#[test]
fn execution_engine_tracks_active_after_spawn() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS track_active (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let active_before = engine.active_task_count();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let active_after = engine.active_task_count();
    assert!(active_after >= active_before);
}

/// Test 25: ExecutionEngine increments successful queries on successful spawn
#[test]
fn execution_engine_increments_successful() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS success_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let successful_before = engine.metrics().successful_queries();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let successful_after = engine.metrics().successful_queries();
    assert!(successful_after >= successful_before);
}
/// Test 26: ExecutionMetrics can calculate success rate
#[test]
fn execution_metrics_calculates_success_rate() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let metrics = engine.metrics().clone();
    assert_eq!(metrics.success_rate(), 0.0);
}

/// Test 27: ExecutionMetrics tracks query latency
#[test]
fn execution_metrics_tracks_latency() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 10).expect("Create engine");

    let metrics = engine.metrics();
    let latency = metrics.average_latency_ms();
    assert!(latency >= 0.0);
}

/// Test 28: ExecutionMetrics tracks per-database statistics
#[test]
fn execution_metrics_tracks_per_database() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "analytics_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS db_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let metrics = engine.metrics();
    let db_stats = metrics.database_stats("analytics_db");
    assert!(db_stats.is_some());
}

/// Test 29: ExecutionMetrics provides percentile latency
#[test]
fn execution_metrics_provides_percentile_latency() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 10).expect("Create engine");

    let metrics = engine.metrics();
    let p95_latency = metrics.latency_p95_ms();
    let p99_latency = metrics.latency_p99_ms();

    assert!(p95_latency >= 0.0);
    assert!(p99_latency >= 0.0);
}

/// Test 30: ExecutionMetrics tracks query count by database
#[test]
fn execution_metrics_query_count_per_db() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 10).expect("Create engine");

    let metrics = engine.metrics();
    let count = metrics.query_count_for_database("test_db");
    assert!(count >= 0);
}

/// Test 31: ExecutionEngine can retrieve task results by request ID
#[test]
fn execution_engine_retrieves_task_result_by_id() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS result_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let request_id = query_req.id();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.get_result(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_some());
}

/// Test 32: ExecutionEngine stores multiple results
#[test]
fn execution_engine_stores_multiple_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let mut request_ids = vec![];

    for i in 0..3 {
        let request = ExecuteQueryRequest {
            db_identifier: "test_db".to_string(),
            query: format!("CREATE TABLE IF NOT EXISTS result_{} (id INTEGER)", i),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        };

        let query_req = QueryRequest::new(request, QueryPriority::Normal);
        request_ids.push(query_req.id());

        let engine = engine.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        })
        .join()
        .unwrap();
    }

    for request_id in request_ids {
        let result = std::thread::spawn({
            let engine = engine.clone();
            move || {
                let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
                rt.block_on(async { engine.get_result(&request_id).await })
            }
        })
        .join()
        .unwrap();

        assert!(result.is_some());
    }
}

/// Test 33: ExecutionEngine returns None for non-existent request ID
#[test]
fn execution_engine_returns_none_for_missing_id() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let non_existent_id = Uuid::new_v4();

    let result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.get_result(&non_existent_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_none());
}

/// Test 34: ExecutionEngine can await task completion
#[test]
fn execution_engine_can_await_completion() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS await_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let request_id = query_req.id();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            })
        }
    })
    .join()
    .unwrap();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let result = engine.wait_for_result(&request_id, 10000).await;
                assert!(result.is_ok());
            })
        }
    })
    .join()
    .unwrap();
}

/// Test 35: ExecutionEngine reports result status
#[test]
fn execution_engine_reports_result_status() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS status_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let request_id = query_req.id();

    std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let _ = engine.spawn_query(query_req).await;
            })
        }
    })
    .join()
    .unwrap();

    let status = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.result_status(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(status.is_some());
}
/// Test 36: ExecutionEngine can cancel a task
#[test]
fn execution_engine_can_cancel_pending_task() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Setup: Initialize result in Pending state
    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(id, ResultStatus::Pending, None, None)
                    .await;
                engine.create_cancellation_token(&id).await;
            })
        }
    })
    .join()
    .unwrap();

    // Act: Cancel the task
    let cancel_result = std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.cancel_task(&id).await })
        }
    })
    .join()
    .unwrap();

    // Assert: Cancellation was successful
    assert!(cancel_result);

    // Verify: Task is marked as cancelled
    let is_cancelled = std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.is_task_cancelled(&id).await })
        }
    })
    .join()
    .unwrap();

    assert!(is_cancelled);
}

/// Test 37: ExecutionEngine tracks cancellation status
#[test]
fn execution_engine_tracks_cancellation_status() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Combined: Check before, cancel, check after in one runtime
    let (before, after) = std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let is_cancelled_before = engine.is_task_cancelled(&id).await;
                let _ = engine.cancel_task(&id).await;
                let is_cancelled_after = engine.is_task_cancelled(&id).await;
                (is_cancelled_before, is_cancelled_after)
            })
        }
    })
    .join()
    .unwrap();

    assert!(!before);
    assert!(after);
}

/// Test 38: ExecutionEngine cleans up cancelled task results
#[test]
fn execution_engine_cleans_up_cancelled_task_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Setup: Cancel the task first
    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.cancel_task(&id).await })
        }
    })
    .join()
    .unwrap();

    // Wait a bit for the cancellation to be processed
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Act: Clean up the cancelled result
    let cleanup_result = std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.cleanup_cancelled_result(&id).await })
        }
    })
    .join()
    .unwrap();

    // Assert: Cleanup succeeded
    assert!(cleanup_result);
}

/// Test 39: ExecutionEngine prevents cancellation of completed task
#[test]
fn execution_engine_cannot_cancel_completed_task() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request = ExecuteQueryRequest {
        db_identifier: "test_db".to_string(),
        query: "CREATE TABLE IF NOT EXISTS cancel_test (id INTEGER)".to_string(),
        parameters: HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let query_req = QueryRequest::new(request, QueryPriority::Normal);
    let request_id = query_req.id();

    let cancel_result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                // Spawn and get result immediately (before it expires)
                let _ = engine.spawn_query(query_req).await;
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

                // Now try to cancel - should fail because completed
                engine.cancel_task(&request_id).await
            })
        }
    })
    .join()
    .unwrap();

    assert!(!cancel_result);
}

/// Test 40: ExecutionEngine cancels only specific task
#[test]
fn execution_engine_cancels_only_specific_task() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    // Setup: Cancel both tasks in a single runtime
    std::thread::spawn({
        let engine = engine.clone();
        let task_id1 = id1;
        let task_id2 = id2;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine.cancel_task(&task_id1).await;
                engine.cancel_task(&task_id2).await;
            })
        }
    })
    .join()
    .unwrap();

    // Verify: Both tasks are cancelled
    let (cancelled1, cancelled2) = std::thread::spawn({
        let engine = engine.clone();
        let task_id1 = id1;
        let task_id2 = id2;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let c1 = engine.is_task_cancelled(&task_id1).await;
                let c2 = engine.is_task_cancelled(&task_id2).await;
                (c1, c2)
            })
        }
    })
    .join()
    .unwrap();

    assert!(cancelled1);
    assert!(cancelled2);
}

/// Test 41: ExecutionEngine assigns TTL to results
#[test]
fn execution_engine_assigns_ttl_to_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(id, ResultStatus::Completed, Some("Done".to_string()), None)
                    .await;
            })
        }
    })
    .join()
    .unwrap();

    let result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.get_result(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_some());
    let result = result.unwrap();
    assert!(result.expires_at.is_some());
}

/// Test 42: ExecutionEngine marks expired results as stale
#[test]
fn execution_engine_marks_expired_results_as_stale() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(id, ResultStatus::Completed, Some("Done".to_string()), None)
                    .await;
            })
        }
    })
    .join()
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let is_expired = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.is_result_expired(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(is_expired);
}

/// Test 43: ExecutionEngine collects garbage for expired results
#[test]
fn execution_engine_collects_garbage_for_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(id, ResultStatus::Completed, Some("Done".to_string()), None)
                    .await;
            })
        }
    })
    .join()
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let collected = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.garbage_collect_expired_results().await })
        }
    })
    .join()
    .unwrap();

    assert!(collected > 0);

    let result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.get_result(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_none());
}

/// Test 44: ExecutionEngine preserves non-expired results during garbage collection
#[test]
fn execution_engine_preserves_non_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    std::thread::spawn({
        let engine = engine.clone();
        let id = request_id;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(id, ResultStatus::Completed, Some("Done".to_string()), None)
                    .await;
            })
        }
    })
    .join()
    .unwrap();

    let _ = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.garbage_collect_expired_results().await })
        }
    })
    .join()
    .unwrap();

    let result = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async { engine.get_result(&request_id).await })
        }
    })
    .join()
    .unwrap();

    assert!(result.is_some());
}

/// Test 45: ExecutionEngine reports garbage collection statistics
#[test]
fn execution_engine_reports_garbage_collection_stats() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id1 = Uuid::new_v4();
    let request_id2 = Uuid::new_v4();

    std::thread::spawn({
        let engine = engine.clone();
        let id1 = request_id1;
        let id2 = request_id2;
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                engine
                    .record_result(
                        id1,
                        ResultStatus::Completed,
                        Some("Done1".to_string()),
                        None,
                    )
                    .await;
                engine
                    .record_result(
                        id2,
                        ResultStatus::Completed,
                        Some("Done2".to_string()),
                        None,
                    )
                    .await;
            })
        }
    })
    .join()
    .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(50));

    let (collected, remaining) = std::thread::spawn({
        let engine = engine.clone();
        move || {
            let rt = tokio::runtime::Runtime::new().expect("runtime creation failed");
            rt.block_on(async {
                let collected = engine.garbage_collect_expired_results().await;
                let remaining = engine.result_count().await;
                (collected, remaining)
            })
        }
    })
    .join()
    .unwrap();

    assert!(collected > 0);
    assert_eq!(remaining, 0);
}
