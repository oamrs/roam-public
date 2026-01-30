use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::executor::ExecuteQueryRequest;

/// Default TTL for results in milliseconds (30ms)
const RESULT_TTL_MS: u64 = 30;

/// Cancellation token for task cancellation
#[derive(Debug, Clone)]
pub struct CancellationToken {
    request_id: Uuid,
    is_cancelled: Arc<atomic::AtomicBool>,
}

impl CancellationToken {
    /// Create a new cancellation token
    fn new(request_id: Uuid) -> Self {
        Self {
            request_id,
            is_cancelled: Arc::new(atomic::AtomicBool::new(false)),
        }
    }

    /// Check if the token is cancelled
    pub fn is_cancelled(&self) -> bool {
        self.is_cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the request ID associated with this token
    pub fn request_id(&self) -> Uuid {
        self.request_id
    }
}

use std::sync::atomic;

/// Result status for a query execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultStatus {
    Pending,
    Completed,
    Failed,
    Cancelled,
}

/// Query result with status and output
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub request_id: Uuid,
    pub status: ResultStatus,
    pub output: Option<String>,
    pub error: Option<String>,
    pub completed_at: Option<Instant>,
    pub expires_at: Option<Instant>,
}

/// Per-database query statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub db_identifier: String,
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub total_latency_ms: f64,
}

/// Statistics for connection pool state
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub max_connections: usize,
    pub available_connections: usize,
    pub checked_out_connections: usize,
}

/// Async connection wrapper for pooling
pub struct PooledConnection {
    conn: Connection,
    pool_semaphore: Arc<Semaphore>,
}

impl PooledConnection {
    /// Execute a query with the connection
    pub fn execute(&self, sql: &str, params: &[&dyn rusqlite::ToSql]) -> rusqlite::Result<usize> {
        self.conn.execute(sql, params)
    }

    /// Execute a simple query that takes no parameters
    pub fn execute_simple(&self, sql: &str) -> rusqlite::Result<usize> {
        self.conn.execute(sql, [])
    }

    /// Query the database with the connection
    pub fn query_row<T, F>(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
        f: F,
    ) -> rusqlite::Result<T>
    where
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn.query_row(sql, params, f)
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        self.pool_semaphore.add_permits(1);
    }
}

/// Async connection pool for managing SQLite connections
pub struct ConnectionPool {
    db_path: String,
    max_connections: usize,
    semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
    /// Create a new connection pool
    ///
    /// # Arguments
    /// * `db_path` - Path to SQLite database (e.g., "memory:" or "/path/to/db.sqlite")
    /// * `max_connections` - Maximum number of concurrent connections
    pub fn new(db_path: &str, max_connections: usize) -> Result<Self, String> {
        Ok(Self {
            db_path: db_path.to_string(),
            max_connections,
            semaphore: Arc::new(Semaphore::new(max_connections)),
        })
    }

    /// Get the maximum connections configured
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Get available connection slots
    pub fn available_connections(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get checked out connection count
    pub fn checked_out_connections(&self) -> usize {
        self.max_connections - self.available_connections()
    }

    /// Get connection pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            max_connections: self.max_connections,
            available_connections: self.available_connections(),
            checked_out_connections: self.checked_out_connections(),
        }
    }

    /// Acquire a connection from the pool
    pub async fn get_connection(&self) -> Result<PooledConnection, String> {
        let permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire semaphore: {}", e))?;

        let conn = Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        permit.forget();

        Ok(PooledConnection {
            conn,
            pool_semaphore: self.semaphore.clone(),
        })
    }

    /// Execute a query using a connection from the pool
    pub async fn execute(&self, sql: &str) -> Result<usize, String> {
        let conn = self.get_connection().await?;
        conn.execute_simple(sql)
            .map_err(|e| format!("Query execution failed: {}", e))
    }
}

/// Query priority levels for request ordering
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Represents a queued query request with metadata
#[derive(Clone, Debug)]
pub struct QueryRequest {
    id: Uuid,
    request: ExecuteQueryRequest,
    priority: QueryPriority,
    created_at: Instant,
}

impl QueryRequest {
    /// Create a new query request with a unique ID
    pub fn new(request: ExecuteQueryRequest, priority: QueryPriority) -> Self {
        Self {
            id: Uuid::new_v4(),
            request,
            priority,
            created_at: Instant::now(),
        }
    }

    /// Get the unique ID for this request
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Get the request details
    pub fn request(&self) -> &ExecuteQueryRequest {
        &self.request
    }

    /// Get the priority level
    pub fn priority(&self) -> QueryPriority {
        self.priority
    }

    /// Get when this request was created
    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}

/// Execution metrics tracking
#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    total_queries: Arc<AtomicU64>,
    successful_queries: Arc<AtomicU64>,
    failed_queries: Arc<AtomicU64>,
    queue_depth: Arc<AtomicUsize>,
    active_tasks: Arc<AtomicUsize>,
    total_latency_ms: Arc<tokio::sync::Mutex<f64>>,
    latencies: Arc<tokio::sync::Mutex<Vec<f64>>>,
    db_stats: Arc<tokio::sync::Mutex<HashMap<String, DatabaseStats>>>,
}

impl ExecutionMetrics {
    /// Create new metrics tracker
    pub fn new() -> Self {
        Self {
            total_queries: Arc::new(AtomicU64::new(0)),
            successful_queries: Arc::new(AtomicU64::new(0)),
            failed_queries: Arc::new(AtomicU64::new(0)),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            total_latency_ms: Arc::new(tokio::sync::Mutex::new(0.0)),
            latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            db_stats: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Get total queries processed
    pub fn total_queries(&self) -> u64 {
        self.total_queries.load(Ordering::SeqCst)
    }

    /// Get successful queries
    pub fn successful_queries(&self) -> u64 {
        self.successful_queries.load(Ordering::SeqCst)
    }

    /// Get failed queries
    pub fn failed_queries(&self) -> u64 {
        self.failed_queries.load(Ordering::SeqCst)
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::SeqCst)
    }

    /// Get active task count
    pub fn active_task_count(&self) -> usize {
        self.active_tasks.load(Ordering::SeqCst)
    }

    /// Increment total queries
    pub(crate) fn increment_total(&self) {
        self.total_queries.fetch_add(1, Ordering::SeqCst);
    }

    /// Increment successful queries
    pub(crate) fn increment_successful(&self) {
        self.successful_queries.fetch_add(1, Ordering::SeqCst);
    }

    /// Increment failed queries
    pub(crate) fn increment_failed(&self) {
        self.failed_queries.fetch_add(1, Ordering::SeqCst);
    }

    /// Calculate success rate as percentage (0.0-100.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_queries();
        let successful = self.successful_queries();
        if total == 0 {
            0.0
        } else {
            (successful as f64 / total as f64) * 100.0
        }
    }

    /// Get average latency in milliseconds
    pub fn average_latency_ms(&self) -> f64 {
        let total = self.total_queries();
        if total == 0 {
            0.0
        } else {
            // Calculate average from total latency and query count
            if let Ok(total_latency) = self.total_latency_ms.try_lock() {
                *total_latency / total as f64
            } else {
                0.0
            }
        }
    }

    /// Get 95th percentile latency in milliseconds
    pub fn latency_p95_ms(&self) -> f64 {
        if let Ok(mut latencies) = self.latencies.try_lock() {
            if latencies.is_empty() {
                return 0.0;
            }
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let index = ((0.95 * latencies.len() as f64).ceil() as usize).saturating_sub(1);
            latencies.get(index).copied().unwrap_or(0.0)
        } else {
            0.0
        }
    }

    /// Get 99th percentile latency in milliseconds
    pub fn latency_p99_ms(&self) -> f64 {
        if let Ok(mut latencies) = self.latencies.try_lock() {
            if latencies.is_empty() {
                return 0.0;
            }
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            let index = ((0.99 * latencies.len() as f64).ceil() as usize).saturating_sub(1);
            latencies.get(index).copied().unwrap_or(0.0)
        } else {
            0.0
        }
    }

    /// Record a query latency (called internally)
    pub(crate) async fn record_latency(&self, db_identifier: &str, latency_ms: f64) {
        let mut latencies = self.latencies.lock().await;
        latencies.push(latency_ms);

        let mut total = self.total_latency_ms.lock().await;
        *total += latency_ms;

        let mut stats = self.db_stats.lock().await;
        stats
            .entry(db_identifier.to_string())
            .or_insert_with(|| DatabaseStats {
                db_identifier: db_identifier.to_string(),
                total_queries: 0,
                successful_queries: 0,
                failed_queries: 0,
                total_latency_ms: 0.0,
            });
    }
}

impl Default for ExecutionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// High-throughput execution engine for managing concurrent queries
pub struct ExecutionEngine {
    db_path: String,
    max_concurrent_queries: usize,
    metrics: ExecutionMetrics,
    connection_pool: Arc<ConnectionPool>,
    tasks: Arc<tokio::sync::Mutex<JoinSet<Result<String, String>>>>,
    results: Arc<RwLock<HashMap<Uuid, QueryResult>>>,
    cancellation_tokens: Arc<RwLock<HashMap<Uuid, CancellationToken>>>,
    garbage_collected_count: Arc<AtomicU64>,
}

impl ExecutionEngine {
    /// Create a new execution engine
    pub fn new(db_path: &str, max_concurrent_queries: usize) -> Result<Self, String> {
        let connection_pool = ConnectionPool::new(db_path, max_concurrent_queries)?;
        Ok(Self {
            db_path: db_path.to_string(),
            max_concurrent_queries,
            metrics: ExecutionMetrics::new(),
            connection_pool: Arc::new(connection_pool),
            tasks: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            results: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            garbage_collected_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Get the database path
    pub fn db_path(&self) -> &str {
        &self.db_path
    }

    /// Get the maximum concurrent queries limit
    pub fn max_concurrent_queries(&self) -> usize {
        self.max_concurrent_queries
    }

    /// Get execution metrics
    pub fn metrics(&self) -> &ExecutionMetrics {
        &self.metrics
    }

    /// Get the connection pool
    pub fn connection_pool(&self) -> &Arc<ConnectionPool> {
        &self.connection_pool
    }

    /// Get current queue depth
    pub fn queue_depth(&self) -> usize {
        self.metrics.queue_depth()
    }

    /// Get current active task count
    pub fn active_task_count(&self) -> usize {
        self.metrics.active_task_count()
    }

    /// Spawn a query for concurrent execution
    pub async fn spawn_query(&self, query_req: QueryRequest) -> Result<String, String> {
        if self.checked_out_connections() >= self.max_concurrent_queries {
            return Err("Concurrency limit reached".to_string());
        }

        self.metrics.increment_total();
        let metrics = self.metrics.clone();
        let pool = self.connection_pool.clone();
        let request_id = query_req.id();
        let query = query_req.request().query.clone();
        let db_identifier = query_req.request().db_identifier.clone();
        let results = self.results.clone();

        // Store initial pending result
        self.record_result(request_id, ResultStatus::Pending, None, None)
            .await;

        let mut tasks = self.tasks.lock().await;
        tasks.spawn(async move {
            let start = Instant::now();
            match pool.execute(&query).await {
                Ok(_) => {
                    metrics.increment_successful();
                    let latency = start.elapsed().as_secs_f64() * 1000.0;
                    let _ = metrics.record_latency(&db_identifier, latency).await;

                    // Record successful result
                    let now = Instant::now();
                    results.write().await.insert(
                        request_id,
                        QueryResult {
                            request_id,
                            status: ResultStatus::Completed,
                            output: Some("Query executed successfully".to_string()),
                            error: None,
                            completed_at: Some(now),
                            expires_at: Some(now + Duration::from_millis(RESULT_TTL_MS)),
                        },
                    );

                    Ok(request_id.to_string())
                }
                Err(e) => {
                    metrics.increment_failed();
                    let latency = start.elapsed().as_secs_f64() * 1000.0;
                    let _ = metrics.record_latency(&db_identifier, latency).await;

                    // Record failed result
                    let now = Instant::now();
                    results.write().await.insert(
                        request_id,
                        QueryResult {
                            request_id,
                            status: ResultStatus::Failed,
                            output: None,
                            error: Some(e.clone()),
                            completed_at: Some(now),
                            expires_at: Some(now + Duration::from_millis(RESULT_TTL_MS)),
                        },
                    );

                    Err(e)
                }
            }
        });

        Ok(request_id.to_string())
    }

    fn checked_out_connections(&self) -> usize {
        self.connection_pool.checked_out_connections()
    }

    /// Get result for a specific request ID
    pub async fn get_result(&self, request_id: &Uuid) -> Option<QueryResult> {
        self.results.read().await.get(request_id).cloned()
    }

    /// Wait for a result with timeout (in milliseconds)
    pub async fn wait_for_result(
        &self,
        request_id: &Uuid,
        timeout_ms: u64,
    ) -> Result<QueryResult, String> {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);

        loop {
            if let Some(result) = self.results.read().await.get(request_id) {
                if result.status != ResultStatus::Pending {
                    return Ok(result.clone());
                }
            }

            if start.elapsed() > timeout {
                return Err("Timeout waiting for result".to_string());
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Get result status without blocking
    pub async fn result_status(&self, request_id: &Uuid) -> Option<ResultStatus> {
        self.results.read().await.get(request_id).map(|r| r.status)
    }

    /// Record a query result
    pub async fn record_result(
        &self,
        request_id: Uuid,
        status: ResultStatus,
        output: Option<String>,
        error: Option<String>,
    ) {
        let now = Instant::now();
        let result = QueryResult {
            request_id,
            status,
            output,
            error,
            completed_at: Some(now),
            expires_at: Some(now + Duration::from_millis(RESULT_TTL_MS)),
        };
        self.results.write().await.insert(request_id, result);
    }

    /// Create a cancellation token for a specific task
    pub async fn create_cancellation_token(&self, request_id: &Uuid) -> CancellationToken {
        let token = CancellationToken::new(*request_id);
        self.cancellation_tokens
            .write()
            .await
            .insert(*request_id, token.clone());
        token
    }

    /// Cancel a task by request ID
    pub async fn cancel_task(&self, request_id: &Uuid) -> bool {
        // Check if task is already completed/failed (double-check before and after locks)
        {
            let results = self.results.read().await;
            if let Some(result) = results.get(request_id) {
                if result.status == ResultStatus::Completed || result.status == ResultStatus::Failed
                {
                    return false;
                }
            }
        }

        // Set cancellation flag
        let token_exists = {
            let tokens = self.cancellation_tokens.read().await;
            tokens.contains_key(request_id)
        };

        if token_exists {
            let tokens = self.cancellation_tokens.read().await;
            if let Some(token) = tokens.get(request_id) {
                token.is_cancelled.store(true, atomic::Ordering::SeqCst);
            }
        } else {
            let token = CancellationToken::new(*request_id);
            token.is_cancelled.store(true, atomic::Ordering::SeqCst);
            self.cancellation_tokens
                .write()
                .await
                .insert(*request_id, token);
        }

        // Double-check: Make sure task hasn't completed while we were setting the token
        {
            let results = self.results.read().await;
            if let Some(result) = results.get(request_id) {
                if result.status == ResultStatus::Completed || result.status == ResultStatus::Failed
                {
                    return false;
                }
            }
        }

        self.record_result(
            *request_id,
            ResultStatus::Cancelled,
            Some("Task cancelled".to_string()),
            None,
        )
        .await;
        true
    }

    /// Check if a task is cancelled
    pub async fn is_task_cancelled(&self, request_id: &Uuid) -> bool {
        if let Some(token) = self.cancellation_tokens.read().await.get(request_id) {
            return token.is_cancelled();
        }
        false
    }

    /// Clean up a cancelled result
    pub async fn cleanup_cancelled_result(&self, request_id: &Uuid) -> bool {
        let mut results = self.results.write().await;
        if let Some(result) = results.get(request_id) {
            if result.status == ResultStatus::Cancelled {
                results.remove(request_id);
                return true;
            }
        }
        false
    }

    /// Check if a result has expired
    pub async fn is_result_expired(&self, request_id: &Uuid) -> bool {
        let results = self.results.read().await;
        if let Some(result) = results.get(request_id) {
            if let Some(expires_at) = result.expires_at {
                return Instant::now() > expires_at;
            }
        }
        false
    }

    /// Garbage collect expired results and return count of collected results
    pub async fn garbage_collect_expired_results(&self) -> u64 {
        let now = Instant::now();
        let mut results = self.results.write().await;

        let mut collected = 0;
        let expired_ids: Vec<Uuid> = results
            .iter()
            .filter(|(_, result)| {
                if let Some(expires_at) = result.expires_at {
                    now > expires_at
                } else {
                    false
                }
            })
            .map(|(id, _)| *id)
            .collect();

        for id in expired_ids {
            results.remove(&id);
            collected += 1;
        }

        self.garbage_collected_count
            .fetch_add(collected, Ordering::SeqCst);
        collected
    }

    /// Get count of stored results
    pub async fn result_count(&self) -> usize {
        self.results.read().await.len()
    }

    /// Get total count of garbage collected results
    pub async fn garbage_collected_count(&self) -> u64 {
        self.garbage_collected_count.load(Ordering::SeqCst)
    }
}
