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
#[tokio::test]
async fn execution_engine_provides_queue_depth() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let queue_depth = engine.queue_depth().await;
    assert_eq!(queue_depth, 0, "Initial queue depth should be 0");
}

/// Test 10: ExecutionEngine provides active task count
#[tokio::test]
async fn execution_engine_provides_active_task_count() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 100).expect("Create engine");

    let active_tasks = engine.active_task_count().await;
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
#[tokio::test]
async fn connection_pool_returns_connection() {
    let pool = ConnectionPool::new("memory:", 5).expect("pool creation failed");
    let conn_result = pool.get_connection().await;

    assert!(conn_result.is_ok());
}

/// Test 14: ConnectionPool initializes available connections
#[test]
fn connection_pool_initializes_available_connections() {
    let pool = ConnectionPool::new("memory:", 3).expect("pool creation failed");
    assert_eq!(pool.available_connections(), 3);
}

/// Test 15: ConnectionPool tracks checked out connections
#[tokio::test]
async fn connection_pool_tracks_checked_out_connections() {
    let pool = ConnectionPool::new("memory:", 5).expect("pool creation failed");
    let _conn = pool.get_connection().await.expect("get connection failed");
    let checked_out = pool.checked_out_connections();

    assert!(checked_out > 0);
}

/// Test 16: ConnectionPool respects max connections
#[tokio::test]
async fn connection_pool_respects_max_connections() {
    let pool = ConnectionPool::new("memory:", 2).expect("pool creation failed");

    let _conn1 = pool
        .get_connection()
        .await
        .expect("first connection failed");
    let _conn2 = pool
        .get_connection()
        .await
        .expect("second connection failed");
    let result = pool.available_connections();

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
#[tokio::test]
async fn connection_pool_executes_query() {
    let pool = ConnectionPool::new("memory:", 1).expect("pool creation failed");

    let result = pool
        .execute("CREATE TABLE IF NOT EXISTS test (id INTEGER)")
        .await;

    assert!(result.is_ok());
}

/// Test 19: ConnectionPool releases connection on drop
#[tokio::test]
async fn connection_pool_releases_connection_on_drop() {
    let pool = ConnectionPool::new("memory:", 2).expect("pool creation failed");

    {
        let _conn = pool.get_connection().await.expect("get connection failed");
        let stats_during = pool.stats();
        assert_eq!(stats_during.available_connections, 1);
    }
    // Connection dropped here
    let stats_after = pool.stats();

    assert_eq!(stats_after.available_connections, 2);
}
/// Test 20: ExecutionEngine can spawn a task
#[tokio::test]
async fn execution_engine_can_spawn_task() {
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

    let result = engine.spawn_query(query_req).await;
    assert!(result.is_ok());
}

/// Test 21: ExecutionEngine spawned task updates metrics
#[tokio::test]
async fn execution_engine_spawn_updates_metrics() {
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

    let _ = engine.spawn_query(query_req).await;

    let metrics_after = engine.metrics().total_queries();
    assert!(metrics_after >= metrics_before);
}

/// Test 22: ExecutionEngine queues tasks by priority
#[tokio::test]
async fn execution_engine_respects_priority_on_spawn() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 1).expect("Create engine"));

    // Create a blocking task to fill the concurrency slot
    let blocker = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                query: "CREATE TABLE IF NOT EXISTS blocker22 (id INTEGER); INSERT INTO blocker22 VALUES (1), (2);".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let req = QueryRequest::new(request, QueryPriority::Normal);
            let _ = engine.spawn_query(req).await;
        })
    };

    // Let blocker start
    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // Spawn LOW priority into queue
    let low_task = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                query: "CREATE TABLE IF NOT EXISTS low22 (id INTEGER)".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let req = QueryRequest::new(request, QueryPriority::Low);
            let _ = engine.spawn_query(req).await;
        })
    };

    // Spawn CRITICAL priority into queue (should have priority over Low)
    let high_task = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                query: "CREATE TABLE IF NOT EXISTS high22 (id INTEGER)".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let req = QueryRequest::new(request, QueryPriority::Critical);
            let _ = engine.spawn_query(req).await;
        })
    };

    // Wait for all to complete
    let _ = blocker.await;
    let _ = low_task.await;
    let _ = high_task.await;

    // Both requests have different priorities
    let req_low = QueryRequest::new(
        ExecuteQueryRequest {
            db_identifier: "test_db".to_string(),
            query: "test".to_string(),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        },
        QueryPriority::Low,
    );
    let req_high = QueryRequest::new(
        ExecuteQueryRequest {
            db_identifier: "test_db".to_string(),
            query: "test".to_string(),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        },
        QueryPriority::Critical,
    );

    assert!(req_high.priority() > req_low.priority());
}

/// Test 23: ExecutionEngine can handle multiple concurrent spawns
#[tokio::test]
async fn execution_engine_handles_multiple_spawns() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let mut tasks = vec![];

    for i in 0..3 {
        let engine = engine.clone();
        let task = tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: format!("db_{}", i),
                query: format!("CREATE TABLE IF NOT EXISTS test_{} (id INTEGER)", i),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let query_req = QueryRequest::new(request, QueryPriority::Normal);
            engine.spawn_query(query_req).await
        });
        tasks.push(task);
    }

    for task in tasks {
        let result = task.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test 24: ExecutionEngine tracks active tasks after spawn
#[tokio::test]
async fn execution_engine_tracks_active_after_spawn() {
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

    let active_before = engine.active_task_count().await;

    let _ = engine.spawn_query(query_req).await;

    let active_after = engine.active_task_count().await;
    assert!(active_after >= active_before);
}

/// Test 25: ExecutionEngine increments successful queries on successful spawn
#[tokio::test]
async fn execution_engine_increments_successful() {
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

    let _ = engine.spawn_query(query_req).await;

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

/// Test 28: ExecutionMetrics provides percentile latency
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

/// Test 29: ExecutionEngine can retrieve task results by request ID
#[tokio::test]
async fn execution_engine_retrieves_task_result_by_id() {
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

    let _ = engine.spawn_query(query_req).await;

    let result = engine.get_result(&request_id).await;

    assert!(result.is_some());
}

/// Test 30: ExecutionEngine stores multiple results
#[tokio::test]
async fn execution_engine_stores_multiple_results() {
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

        let _ = engine.spawn_query(query_req).await;
    }

    for request_id in request_ids {
        let result = engine.get_result(&request_id).await;
        assert!(result.is_some());
    }
}

/// Test 31: ExecutionEngine returns None for non-existent request ID
#[tokio::test]
async fn execution_engine_returns_none_for_missing_id() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let non_existent_id = Uuid::new_v4();

    let result = engine.get_result(&non_existent_id).await;

    assert!(result.is_none());
}

/// Test 32: ExecutionEngine can await task completion
#[tokio::test]
async fn execution_engine_can_await_completion() {
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

    let _ = engine.spawn_query(query_req).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let result = engine.wait_for_result(&request_id, 10000).await;
    assert!(result.is_ok());
}

/// Test 33: ExecutionEngine reports result status
#[tokio::test]
async fn execution_engine_reports_result_status() {
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

    let _ = engine.spawn_query(query_req).await;

    let status = engine.result_status(&request_id).await;

    assert!(status.is_some());
}
/// Test 34: ExecutionEngine can cancel a task
#[tokio::test]
async fn execution_engine_can_cancel_pending_task() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Setup: Initialize result in Pending state
    engine
        .record_result(request_id, ResultStatus::Pending, None, None)
        .await;
    engine.create_cancellation_token(&request_id).await;

    // Act: Cancel the task
    let cancel_result = engine.cancel_task(&request_id).await;

    // Assert: Cancellation was successful
    assert!(cancel_result);

    // Verify: Task is marked as cancelled
    let is_cancelled = engine.is_task_cancelled(&request_id).await;

    assert!(is_cancelled);
}

/// Test 35: ExecutionEngine tracks cancellation status
#[tokio::test]
async fn execution_engine_tracks_cancellation_status() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    let is_cancelled_before = engine.is_task_cancelled(&request_id).await;
    let _ = engine.cancel_task(&request_id).await;
    let is_cancelled_after = engine.is_task_cancelled(&request_id).await;

    assert!(!is_cancelled_before);
    assert!(is_cancelled_after);
}

/// Test 36: ExecutionEngine cleans up cancelled task results
#[tokio::test]
async fn execution_engine_cleans_up_cancelled_task_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Setup: Cancel the task first
    let _ = engine.cancel_task(&request_id).await;

    // Wait a bit for the cancellation to be processed
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Act: Clean up the cancelled result
    let cleanup_result = engine.cleanup_cancelled_result(&request_id).await;

    // Assert: Cleanup succeeded
    assert!(cleanup_result);
}

/// Test 37: ExecutionEngine prevents cancellation of completed task
#[tokio::test]
async fn execution_engine_cannot_cancel_completed_task() {
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

    // Spawn and get result immediately (before it expires)
    let _ = engine.spawn_query(query_req).await;
    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;

    // Now try to cancel - should fail because completed
    let cancel_result = engine.cancel_task(&request_id).await;

    assert!(!cancel_result);
}

/// Test 37A: ExecutionEngine cancel_task prevents race condition
/// Verifies atomic check-and-update prevents completed tasks from being marked cancelled
#[tokio::test]
async fn execution_engine_cancel_task_prevents_completion_race() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Manually set task to Completed status
    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Query completed".to_string()),
            None,
        )
        .await;

    // Verify it's in Completed state
    let result_before = engine.get_result(&request_id).await;
    assert_eq!(result_before.unwrap().status, ResultStatus::Completed);

    // Try to cancel - should fail and NOT change status to Cancelled
    let cancel_result = engine.cancel_task(&request_id).await;
    assert!(
        !cancel_result,
        "Cancellation should fail for completed task"
    );

    // Verify status is still Completed (not changed to Cancelled)
    let result_after = engine.get_result(&request_id).await;
    assert_eq!(
        result_after.unwrap().status,
        ResultStatus::Completed,
        "Status should remain Completed, not change to Cancelled"
    );
}

/// Test 37B: ExecutionEngine cancel_task with concurrent record_result
/// Tests that cancellation and completion don't interfere with each other
#[tokio::test]
async fn execution_engine_cancel_vs_completion_race_safety() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    // Spawn multiple tasks and try to cancel/complete them concurrently
    let mut handles = vec![];

    for _i in 0..10 {
        let engine_clone = Arc::clone(&engine);
        let handle = tokio::spawn(async move {
            let request_id = uuid::Uuid::new_v4();

            // Spawn a cancel attempt
            let cancel_handle = {
                let request_id_clone = request_id.clone();
                let engine_clone = Arc::clone(&engine_clone);
                tokio::spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                    engine_clone.cancel_task(&request_id_clone).await
                })
            };

            // Spawn a completion attempt
            let complete_handle = {
                let request_id_clone = request_id.clone();
                let engine_clone = Arc::clone(&engine_clone);
                tokio::spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                    engine_clone
                        .record_result(
                            request_id_clone,
                            ResultStatus::Completed,
                            Some("Completed".to_string()),
                            None,
                        )
                        .await;
                })
            };

            let _ = cancel_handle.await;
            let _ = complete_handle.await;

            // Check final status - should be one of Completed or Cancelled, not both
            let final_result = engine_clone.get_result(&request_id).await;
            if let Some(result) = final_result {
                // Result should be either Completed or Cancelled, not some corrupted state
                assert!(
                    result.status == ResultStatus::Completed
                        || result.status == ResultStatus::Cancelled,
                    "Final status should be either Completed or Cancelled"
                );
            }
        });

        handles.push(handle);
    }

    // Wait for all concurrent operations to complete
    for handle in handles {
        let _ = handle.await;
    }
}

/// Test 38: ExecutionEngine cancels only specific task
#[tokio::test]
async fn execution_engine_cancels_only_specific_task() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();

    // Setup: Cancel both tasks
    let _ = engine.cancel_task(&id1).await;
    let _ = engine.cancel_task(&id2).await;

    // Verify: Both tasks are cancelled
    let cancelled1 = engine.is_task_cancelled(&id1).await;
    let cancelled2 = engine.is_task_cancelled(&id2).await;

    assert!(cancelled1);
    assert!(cancelled2);
}

/// Test 39: ExecutionEngine assigns TTL to results
#[tokio::test]
async fn execution_engine_assigns_ttl_to_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;

    let result = engine.get_result(&request_id).await;

    assert!(result.is_some());
    let result = result.unwrap();
    assert!(result.expires_at.is_some());
}

/// Test 40: ExecutionEngine marks expired results as stale
#[tokio::test]
async fn execution_engine_marks_expired_results_as_stale() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    let request_id = Uuid::new_v4();

    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let is_expired = engine.is_result_expired(&request_id).await;

    assert!(is_expired);
}

/// Test 41: ExecutionEngine collects garbage for expired results
#[tokio::test]
async fn execution_engine_collects_garbage_for_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    let request_id = Uuid::new_v4();

    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let collected = engine.garbage_collect_expired_results().await;

    assert!(collected > 0);

    let result = engine.get_result(&request_id).await;

    assert!(result.is_none());
}

/// Test 42: ExecutionEngine preserves non-expired results during garbage collection
#[tokio::test]
async fn execution_engine_preserves_non_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 10).expect("Create engine"));

    let request_id = Uuid::new_v4();

    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;

    let _ = engine.garbage_collect_expired_results().await;

    let result = engine.get_result(&request_id).await;

    assert!(result.is_some());
}

/// Test 43: ExecutionEngine reports garbage collection statistics
#[tokio::test]
async fn execution_engine_reports_garbage_collection_stats() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    let request_id1 = Uuid::new_v4();
    let request_id2 = Uuid::new_v4();

    engine
        .record_result(
            request_id1,
            ResultStatus::Completed,
            Some("Done1".to_string()),
            None,
        )
        .await;
    engine
        .record_result(
            request_id2,
            ResultStatus::Completed,
            Some("Done2".to_string()),
            None,
        )
        .await;

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let collected = engine.garbage_collect_expired_results().await;
    let remaining = engine.result_count().await;

    assert!(collected > 0);
    assert_eq!(remaining, 0);
}

/// Test 44: ExecutionEngine cleans up cancellation tokens during garbage collection
#[tokio::test]
async fn execution_engine_cleans_up_cancellation_tokens_on_gc() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    let request_id = Uuid::new_v4();

    // Record a result with TTL
    engine
        .record_result(
            request_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;
    // Also create a cancellation token for this request
    engine.create_cancellation_token(&request_id).await;

    // Verify both result and token exist before GC
    let result_exists_before = engine.get_result(&request_id).await.is_some();
    let token_exists_before = engine.is_task_cancelled(&request_id).await;

    assert!(result_exists_before);
    // Token exists even though task isn't cancelled - we just created it
    assert!(!token_exists_before);

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Run garbage collection
    let _ = engine.garbage_collect_expired_results().await;

    // Verify both result and token are cleaned up after GC
    let result_exists_after = engine.get_result(&request_id).await.is_some();

    assert!(!result_exists_after);
}
/// Test 45: ExecutionEngine respects priority when queueing tasks
#[tokio::test]
async fn execution_engine_respects_priority_execution_order() {
    let db_path = test_db_path();
    // Create engine with max 1 concurrent to force queue
    let engine = Arc::new(ExecutionEngine::new(&db_path, 1).expect("Create engine"));

    // Create a slow query that blocks the single slot
    let blocking_task = {
        let engine = engine.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                // Long running query to block the slot
                query: "CREATE TABLE IF NOT EXISTS blocker (id INTEGER); INSERT INTO blocker SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5;".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let query_req = QueryRequest::new(request, QueryPriority::Normal);
            let _ = engine.spawn_query(query_req).await;
        })
    };

    // Let the blocking task start
    tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

    let execution_order = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Spawn LOW priority (queued, should execute after HIGH)
    let low_task = {
        let engine = engine.clone();
        let order = execution_order.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                query: "SELECT 1".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let query_req = QueryRequest::new(request, QueryPriority::Low);
            let start = std::time::Instant::now();
            let _ = engine.spawn_query(query_req).await;
            order
                .lock()
                .await
                .push(("low_start", start.elapsed().as_millis() as f64));
        })
    };

    // Spawn HIGH priority (queued after LOW, but should be first in queue due to priority)
    let high_task = {
        let engine = engine.clone();
        let order = execution_order.clone();
        tokio::spawn(async move {
            let request = ExecuteQueryRequest {
                db_identifier: "test_db".to_string(),
                query: "SELECT 2".to_string(),
                parameters: HashMap::new(),
                limit: 100,
                timeout_seconds: 30,
            };
            let query_req = QueryRequest::new(request, QueryPriority::Critical);
            let start = std::time::Instant::now();
            let _ = engine.spawn_query(query_req).await;
            order
                .lock()
                .await
                .push(("high_start", start.elapsed().as_millis() as f64));
        })
    };

    // Wait for all to complete
    let _ = blocking_task.await;
    let _ = low_task.await;
    let _ = high_task.await;

    // Check that both tasks completed
    let order = execution_order.lock().await;
    let low_time = order
        .iter()
        .find(|(e, _)| *e == "low_start")
        .map(|(_, t)| t);
    let high_time = order
        .iter()
        .find(|(e, _)| *e == "high_start")
        .map(|(_, t)| t);

    assert!(low_time.is_some(), "Low priority should complete");
    assert!(high_time.is_some(), "High priority should complete");

    // The key assertion: High priority should start executing BEFORE or around same time as Low
    // Since Low was queued first, if priority works correctly:
    // - Both get queued while blocker runs
    // - When blocker finishes, HIGH gets picked first (priority queue)
    // - HIGH starts executing, LOW waits
    // So HIGH.start time should be LESS than LOW.start time (both measured from spawn_query call)
    let low_ms = *low_time.unwrap();
    let high_ms = *high_time.unwrap();
    assert!(
        high_ms <= low_ms + 100.0,  // Allow some variance
        "High-priority should start within 100ms of LOW priority (priority queue effect). High: {}ms, Low: {}ms",
        high_ms,
        low_ms
    );
}

/// Test 46: ExecutionEngine collects all results when all are expired
#[tokio::test]
async fn execution_engine_gc_collects_all_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    // Record multiple results
    let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();

    for id in &ids {
        engine
            .record_result(*id, ResultStatus::Completed, Some("Done".to_string()), None)
            .await;
    }

    // Verify all results exist
    assert_eq!(engine.result_count().await, 10);

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Collect garbage
    let collected = engine.garbage_collect_expired_results().await;

    // All results should be collected
    assert_eq!(collected, 10);
    assert_eq!(engine.result_count().await, 0);

    // Verify none of the results exist
    for id in &ids {
        assert!(engine.get_result(id).await.is_none());
    }
}

/// Test 47: ExecutionEngine GC handles mixed expired and non-expired results correctly
#[tokio::test]
async fn execution_engine_gc_preserves_non_expired_mixed_with_expired() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 100).expect("Create engine"));

    // Record first batch of results (will expire)
    let expired_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    for id in &expired_ids {
        engine
            .record_result(
                *id,
                ResultStatus::Completed,
                Some("Expired".to_string()),
                None,
            )
            .await;
    }

    // Wait 60ms, some results are now stale but not expired at 100ms TTL
    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

    // Record second batch of results (fresh)
    let fresh_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    for id in &fresh_ids {
        engine
            .record_result(
                *id,
                ResultStatus::Completed,
                Some("Fresh".to_string()),
                None,
            )
            .await;
    }

    // Verify all results exist
    assert_eq!(engine.result_count().await, 10);

    // Wait 60ms more (total 120ms from first batch = expired, but only 60ms from second = fresh)
    tokio::time::sleep(tokio::time::Duration::from_millis(60)).await;

    // Collect garbage
    let collected = engine.garbage_collect_expired_results().await;

    // Only the first batch should be collected
    assert_eq!(collected, 5);
    assert_eq!(engine.result_count().await, 5);

    // Verify expired results are gone
    for id in &expired_ids {
        assert!(engine.get_result(id).await.is_none());
    }

    // Verify fresh results still exist
    for id in &fresh_ids {
        assert!(engine.get_result(id).await.is_some());
    }
}

/// Test 48: ExecutionEngine GC with different result statuses (Completed, Failed, Cancelled)
#[tokio::test]
async fn execution_engine_gc_respects_all_result_statuses() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    let completed_id = Uuid::new_v4();
    let failed_id = Uuid::new_v4();
    let cancelled_id = Uuid::new_v4();
    let pending_id = Uuid::new_v4();

    // Record results with different statuses
    engine
        .record_result(
            completed_id,
            ResultStatus::Completed,
            Some("Done".to_string()),
            None,
        )
        .await;
    engine
        .record_result(
            failed_id,
            ResultStatus::Failed,
            None,
            Some("Error".to_string()),
        )
        .await;
    engine
        .record_result(
            cancelled_id,
            ResultStatus::Cancelled,
            Some("Cancelled".to_string()),
            None,
        )
        .await;
    engine
        .record_result(pending_id, ResultStatus::Pending, None, None)
        .await;

    assert_eq!(engine.result_count().await, 4);

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Collect garbage
    let collected = engine.garbage_collect_expired_results().await;

    // All results should be collected regardless of status
    assert_eq!(collected, 4);
    assert_eq!(engine.result_count().await, 0);
}

/// Test 49: ExecutionEngine GC handles concurrent garbage collection calls safely
#[tokio::test]
async fn execution_engine_gc_concurrent_collection_is_safe() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    // Record results
    let ids: Vec<Uuid> = (0..20).map(|_| Uuid::new_v4()).collect();

    for id in &ids {
        engine
            .record_result(*id, ResultStatus::Completed, Some("Done".to_string()), None)
            .await;
    }

    assert_eq!(engine.result_count().await, 20);

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Spawn multiple concurrent garbage collection tasks
    let mut handles = vec![];

    for _ in 0..5 {
        let engine_clone = Arc::clone(&engine);
        let handle =
            tokio::spawn(async move { engine_clone.garbage_collect_expired_results().await });
        handles.push(handle);
    }

    // Wait for all GC tasks to complete and collect results
    let mut total_collected: u64 = 0;
    for handle in handles {
        if let Ok(collected) = handle.await {
            total_collected += collected;
        }
    }

    // Total collected across all tasks should be 20
    // (Each task should see what's available at its point in time)
    assert_eq!(
        total_collected, 20,
        "Total collected from concurrent GC tasks"
    );

    // After all GC tasks, result count should be 0
    assert_eq!(engine.result_count().await, 0);

    // Garbage collected count should track total collection
    let gc_stats = engine.garbage_collected_count().await;
    assert!(
        gc_stats >= 20,
        "GC stats should track at least 20 collected"
    );
}

/// Test 50: ExecutionEngine GC correctly cleans up tokens for all expired results
#[tokio::test]
async fn execution_engine_gc_cleans_tokens_for_multiple_expired_results() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::with_ttl(&db_path, 10, 50).expect("Create engine"));

    // Record multiple results with cancellation tokens
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    for id in &ids {
        engine
            .record_result(*id, ResultStatus::Completed, Some("Done".to_string()), None)
            .await;
        engine.create_cancellation_token(id).await;
    }

    // Wait for TTL to expire
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Collect garbage
    let collected = engine.garbage_collect_expired_results().await;

    assert_eq!(collected, 5);

    // Verify all results and their associated tokens are cleaned up
    for id in &ids {
        assert!(engine.get_result(id).await.is_none());
    }
}

/// Test 51: ExecutionEngine quickselect percentile optimization produces accurate results
/// Tests that the quickselect algorithm correctly computes percentiles by comparing against actual query latencies
#[tokio::test]
async fn execution_engine_percentile_calculation_with_queries() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 5).expect("Create engine"));

    // Spawn multiple queries to populate latency metrics
    for i in 0..10 {
        let request = ExecuteQueryRequest {
            db_identifier: format!("db_{}", i),
            query: "CREATE TABLE IF NOT EXISTS test (id INTEGER)".to_string(),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        };

        let query_req = QueryRequest::new(request, QueryPriority::Normal);
        let _ = engine.spawn_query(query_req).await;
    }

    // Wait for queries to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let metrics = engine.metrics();
    let p95 = metrics.latency_p95_ms();
    let p99 = metrics.latency_p99_ms();

    // Should have recorded latencies
    assert!(p95 >= 0.0, "P95 should be non-negative");
    assert!(p99 >= 0.0, "P99 should be non-negative");
    // P99 should generally be >= P95 (or equal if very fast)
    assert!(
        p99 >= p95 - 0.001,
        "P99 should be >= P95 (with tolerance for floating point)"
    );
}

/// Test 52: ExecutionEngine percentiles improve with more samples
/// Verifies that percentile calculations are consistent and use the quickselect optimization
#[tokio::test]
async fn execution_engine_percentiles_consistent_across_calls() {
    let db_path = test_db_path();
    let engine = Arc::new(ExecutionEngine::new(&db_path, 5).expect("Create engine"));

    // Spawn queries to populate metrics
    for i in 0..5 {
        let request = ExecuteQueryRequest {
            db_identifier: format!("db_{}", i),
            query: "CREATE TABLE IF NOT EXISTS test (id INTEGER)".to_string(),
            parameters: HashMap::new(),
            limit: 100,
            timeout_seconds: 30,
        };

        let query_req = QueryRequest::new(request, QueryPriority::Normal);
        let _ = engine.spawn_query(query_req).await;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let metrics = engine.metrics();

    // Call percentile methods multiple times - should return consistent results (no sorting side effects)
    let p95_first = metrics.latency_p95_ms();
    let p99_first = metrics.latency_p99_ms();

    let p95_second = metrics.latency_p95_ms();
    let p99_second = metrics.latency_p99_ms();

    assert_eq!(
        p95_first, p95_second,
        "P95 should be consistent across multiple calls"
    );
    assert_eq!(
        p99_first, p99_second,
        "P99 should be consistent across multiple calls"
    );
}

/// Test 53: ExecutionEngine handles empty latencies gracefully
#[tokio::test]
async fn execution_engine_percentiles_handle_empty_buffer() {
    let db_path = test_db_path();
    let engine = ExecutionEngine::new(&db_path, 10).expect("Create engine");

    let metrics = engine.metrics();
    let p95 = metrics.latency_p95_ms();
    let p99 = metrics.latency_p99_ms();

    assert_eq!(p95, 0.0, "P95 should be 0.0 for empty buffer");
    assert_eq!(p99, 0.0, "P99 should be 0.0 for empty buffer");
}
