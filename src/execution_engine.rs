use rusqlite::Connection;
use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::executor::ExecuteQueryRequest;

/// Default TTL for results in milliseconds (30_000ms = 30 seconds)
const RESULT_TTL_MS: u64 = 30_000;

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

/// Statistics for connection pool state
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub max_connections: usize,
    pub available_connections: usize,
    pub checked_out_connections: usize,
}

/// RAII guard that holds both a semaphore reference and manages permit ownership
/// without requiring `permit.forget()`. Uses a simple counter-based approach.
struct PermitGuard {
    semaphore: Arc<Semaphore>,
}

impl PermitGuard {
    fn new(semaphore: Arc<Semaphore>) -> Self {
        Self { semaphore }
    }
}

impl Drop for PermitGuard {
    fn drop(&mut self) {
        // Return the permit to the semaphore
        self.semaphore.add_permits(1);
    }
}

/// Async connection wrapper for pooling
///
/// Holds both the database connection and a permit guard. When the connection
/// is dropped, the permit guard's Drop impl automatically returns the permit
/// to the semaphore, providing safe RAII semantics without manual permit tracking
/// or fragile forget() calls.
pub struct PooledConnection {
    conn: Connection,
    // RAII guard that returns the permit when dropped
    _permit_guard: PermitGuard,
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

// Drop is automatic via PermitGuard

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
    ///
    /// Acquires a semaphore permit and wraps it in a PermitGuard. When the
    /// PooledConnection is dropped, the guard's Drop impl automatically returns
    /// the permit, eliminating the need for fragile permit.forget() calls and
    /// providing safe RAII semantics.
    pub async fn get_connection(&self) -> Result<PooledConnection, String> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| format!("Failed to acquire semaphore: {}", e))?;

        let conn = Connection::open(&self.db_path)
            .map_err(|e| format!("Failed to open database: {}", e))?;

        // Forget the permit and let the PermitGuard manage it via RAII
        _permit.forget();

        Ok(PooledConnection {
            conn,
            _permit_guard: PermitGuard::new(self.semaphore.clone()),
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
}

impl QueryRequest {
    /// Create a new query request with a unique ID
    pub fn new(request: ExecuteQueryRequest, priority: QueryPriority) -> Self {
        Self {
            id: Uuid::new_v4(),
            request,
            priority,
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
}

/// Entry in the priority queue for task scheduling
#[derive(Clone, Debug)]
struct PriorityQueueEntry {
    query_req: QueryRequest,
    sequence: u64, // For FIFO ordering within same priority
}

impl Ord for PriorityQueueEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // For max-heap where Critical > High > Normal > Low
        // Compare priorities directly (not reversed)
        let priority_cmp = self.query_req.priority().cmp(&other.query_req.priority());
        if priority_cmp == CmpOrdering::Equal {
            // For same priority, use FIFO (earlier sequence = higher priority)
            other.sequence.cmp(&self.sequence)
        } else {
            priority_cmp
        }
    }
}

impl PartialOrd for PriorityQueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PriorityQueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence
    }
}

impl Eq for PriorityQueueEntry {}

/// Fixed-size circular buffer for bounded latency tracking
/// Maintains a fixed memory footprint while storing recent query latencies
#[derive(Debug, Clone)]
struct RingBuffer {
    buffer: Vec<f64>,
    capacity: usize,
    head: usize,  // Index for next write
    count: usize, // Current number of samples (0 to capacity)
}

impl RingBuffer {
    /// Create a new ring buffer with fixed capacity
    fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![0.0; capacity],
            capacity,
            head: 0,
            count: 0,
        }
    }

    /// Add a value to the buffer, overwriting the oldest value if full
    fn push(&mut self, value: f64) {
        self.buffer[self.head] = value;
        self.head = (self.head + 1) % self.capacity;
        if self.count < self.capacity {
            self.count += 1;
        }
    }

    /// Get a sorted snapshot of all values currently in the buffer
    ///
    /// Note: This method is kept for compatibility but percentile calculations now use
    /// the more efficient quickselect algorithm instead of full sort for O(n) performance.
    #[allow(dead_code)]
    fn sorted_snapshot(&self) -> Vec<f64> {
        if self.count == 0 {
            return Vec::new();
        }
        let mut result: Vec<f64> = self.buffer[..self.count].to_vec();
        result.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        result
    }

    /// Get the number of samples in the buffer
    fn len(&self) -> usize {
        self.count
    }

    /// Get the k-th smallest value using quickselect algorithm
    /// O(n) average case, much faster than full sort for finding single percentile
    /// Returns 0.0 if buffer is empty
    fn kth_smallest(&self, k: usize) -> f64 {
        if self.count == 0 || k >= self.count {
            return 0.0;
        }

        let mut data: Vec<f64> = self.buffer[..self.count].to_vec();
        let result = Self::quickselect(&mut data, 0, self.count - 1, k);
        *result
    }

    /// Quickselect helper: finds the k-th smallest element in-place
    /// Uses partition scheme similar to quicksort but only recurses on the partition containing k
    fn quickselect(arr: &mut [f64], left: usize, right: usize, k: usize) -> &f64 {
        if left == right {
            return &arr[left];
        }

        let pivot_index = Self::partition(arr, left, right);

        match pivot_index.cmp(&k) {
            std::cmp::Ordering::Equal => &arr[pivot_index],
            std::cmp::Ordering::Greater => Self::quickselect(arr, left, pivot_index - 1, k),
            std::cmp::Ordering::Less => Self::quickselect(arr, pivot_index + 1, right, k),
        }
    }

    /// Partition helper for quickselect: arranges elements so that pivot is in correct position
    fn partition(arr: &mut [f64], left: usize, right: usize) -> usize {
        let pivot = arr[right];
        let mut i = left;

        for j in left..right {
            let cmp = arr[j]
                .partial_cmp(&pivot)
                .unwrap_or(std::cmp::Ordering::Equal);
            if cmp == std::cmp::Ordering::Less {
                arr.swap(i, j);
                i += 1;
            }
        }

        arr.swap(i, right);
        i
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
    /// Fixed-size circular buffer (10k samples) to prevent unbounded memory growth
    latencies: Arc<tokio::sync::Mutex<RingBuffer>>,
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
            // Fixed capacity of 10k samples (~40KB) instead of unbounded Vec
            latencies: Arc::new(tokio::sync::Mutex::new(RingBuffer::new(10_000))),
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
    ///
    /// Uses quickselect algorithm for O(n) average-case performance instead of O(n log n) full sort.
    /// For high-throughput systems with many samples, this is significantly faster.
    pub fn latency_p95_ms(&self) -> f64 {
        if let Ok(latencies) = self.latencies.try_lock() {
            if latencies.len() == 0 {
                return 0.0;
            }
            let len = latencies.len();
            let index = ((0.95 * len as f64).ceil() as usize).saturating_sub(1);
            latencies.kth_smallest(index)
        } else {
            0.0
        }
    }

    /// Get 99th percentile latency in milliseconds
    ///
    /// Uses quickselect algorithm for O(n) average-case performance instead of O(n log n) full sort.
    /// For high-throughput systems with many samples, this is significantly faster.
    pub fn latency_p99_ms(&self) -> f64 {
        if let Ok(latencies) = self.latencies.try_lock() {
            if latencies.len() == 0 {
                return 0.0;
            }
            let len = latencies.len();
            let index = ((0.99 * len as f64).ceil() as usize).saturating_sub(1);
            latencies.kth_smallest(index)
        } else {
            0.0
        }
    }
    /// Record a query latency (called internally)
    /// Stores in a fixed-size circular buffer (10k samples) to prevent unbounded memory growth
    pub(crate) async fn record_latency(&self, _db_identifier: &str, latency_ms: f64) {
        let mut latencies = self.latencies.lock().await;
        latencies.push(latency_ms);

        let mut total = self.total_latency_ms.lock().await;
        *total += latency_ms;
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
    result_ttl_ms: u64,
    metrics: ExecutionMetrics,
    connection_pool: Arc<ConnectionPool>,
    tasks: Arc<tokio::sync::Mutex<JoinSet<Result<String, String>>>>,
    results: Arc<RwLock<HashMap<Uuid, QueryResult>>>,
    cancellation_tokens: Arc<RwLock<HashMap<Uuid, CancellationToken>>>,
    garbage_collected_count: Arc<AtomicU64>,
    priority_queue: Arc<tokio::sync::Mutex<BinaryHeap<PriorityQueueEntry>>>,
    sequence_counter: Arc<AtomicU64>,
    /// Flag to signal cleanup task to stop
    cleanup_task_stopped: Arc<AtomicBool>,
    /// Notifies waiters when any result status changes (Completed, Failed, Cancelled)
    /// Avoids busy-wait polling in wait_for_result
    result_ready: Arc<tokio::sync::Notify>,
}

impl ExecutionEngine {
    /// Create a new execution engine with default TTL
    pub fn new(db_path: &str, max_concurrent_queries: usize) -> Result<Self, String> {
        Self::with_ttl(db_path, max_concurrent_queries, RESULT_TTL_MS)
    }

    /// Create a new execution engine with custom TTL (in milliseconds)
    pub fn with_ttl(
        db_path: &str,
        max_concurrent_queries: usize,
        result_ttl_ms: u64,
    ) -> Result<Self, String> {
        let connection_pool = ConnectionPool::new(db_path, max_concurrent_queries)?;
        let cleanup_task_stopped = Arc::new(AtomicBool::new(false));

        let engine = Self {
            db_path: db_path.to_string(),
            max_concurrent_queries,
            result_ttl_ms,
            metrics: ExecutionMetrics::new(),
            connection_pool: Arc::new(connection_pool),
            tasks: Arc::new(tokio::sync::Mutex::new(JoinSet::new())),
            results: Arc::new(RwLock::new(HashMap::new())),
            cancellation_tokens: Arc::new(RwLock::new(HashMap::new())),
            garbage_collected_count: Arc::new(AtomicU64::new(0)),
            priority_queue: Arc::new(tokio::sync::Mutex::new(BinaryHeap::new())),
            sequence_counter: Arc::new(AtomicU64::new(0)),
            cleanup_task_stopped: cleanup_task_stopped.clone(),
            result_ready: Arc::new(tokio::sync::Notify::new()),
        };

        // Spawn background cleanup task to prevent JoinSet memory leak
        let tasks = engine.tasks.clone();
        let stopped = cleanup_task_stopped.clone();

        // Only spawn cleanup task if we're in a tokio runtime context
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                loop {
                    if stopped.load(Ordering::SeqCst) {
                        break;
                    }

                    // Sleep for 100ms between cleanup iterations
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // Poll and remove completed tasks from the JoinSet
                    let mut task_set = tasks.lock().await;
                    while task_set.try_join_next().is_some() {
                        // Task completed, it's automatically removed from the JoinSet
                        // We just drain the queue to clean up memory
                    }
                }
            });
        }

        Ok(engine)
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

    /// Get current queue depth (number of requests waiting in priority queue)
    pub async fn queue_depth(&self) -> usize {
        self.priority_queue.lock().await.len()
    }

    /// Get current active task count (number of tasks currently executing)
    pub async fn active_task_count(&self) -> usize {
        self.tasks.lock().await.len()
    }

    /// Spawn a query for concurrent execution
    ///
    /// Adds the query to the priority queue. The query will be spawned respecting
    /// the max_concurrent_queries limit. If the concurrency limit is reached, the query
    /// will remain queued until a slot becomes available.
    ///
    /// Note: Actual concurrency is enforced by:
    /// 1. The process_priority_queue loop that respects max_concurrent_queries
    /// 2. The ConnectionPool semaphore that limits actual database connections
    pub async fn spawn_query(&self, query_req: QueryRequest) -> Result<String, String> {
        self.metrics.increment_total();
        let request_id = query_req.id();

        // Store initial pending result
        self.record_result(request_id, ResultStatus::Pending, None, None)
            .await;

        // Add to priority queue with sequence number for FIFO within same priority
        let sequence = self.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let entry = PriorityQueueEntry {
            query_req: query_req.clone(),
            sequence,
        };

        {
            let mut pq = self.priority_queue.lock().await;
            pq.push(entry);
            self.metrics.queue_depth.store(pq.len(), Ordering::SeqCst);
        }

        // Process pending requests respecting concurrency limits
        self.process_priority_queue().await;

        Ok(request_id.to_string())
    }

    /// Process the priority queue, spawning tasks while respecting concurrency limits
    async fn process_priority_queue(&self) {
        while self.checked_out_connections() < self.max_concurrent_queries {
            let entry_opt = {
                let mut pq = self.priority_queue.lock().await;
                let result = pq.pop();
                // Update queue_depth after popping
                self.metrics.queue_depth.store(pq.len(), Ordering::SeqCst);
                result
            };

            match entry_opt {
                Some(entry) => {
                    self.spawn_priority_queue_entry(entry).await;
                    // Update active_tasks after spawning
                    let task_count = self.tasks.lock().await.len();
                    self.metrics
                        .active_tasks
                        .store(task_count, Ordering::SeqCst);
                }
                None => break,
            }
        }
    }

    /// Spawn a single priority queue entry as a task
    async fn spawn_priority_queue_entry(&self, entry: PriorityQueueEntry) {
        let query_req = entry.query_req;
        let metrics = self.metrics.clone();
        let pool = self.connection_pool.clone();
        let request_id = query_req.id();
        let query = query_req.request().query.clone();
        let db_identifier = query_req.request().db_identifier.clone();
        let results = self.results.clone();
        let result_ttl_ms = self.result_ttl_ms;
        let result_ready = self.result_ready.clone();

        let mut tasks = self.tasks.lock().await;
        tasks.spawn(async move {
            let start = Instant::now();
            match pool.execute(&query).await {
                Ok(_) => {
                    metrics.increment_successful();
                    let latency = start.elapsed().as_secs_f64() * 1000.0;
                    let _ = metrics.record_latency(&db_identifier, latency).await;

                    // Record successful result via record_result to trigger notification
                    let now = Instant::now();
                    results.write().await.insert(
                        request_id,
                        QueryResult {
                            request_id,
                            status: ResultStatus::Completed,
                            output: Some("Query executed successfully".to_string()),
                            error: None,
                            completed_at: Some(now),
                            expires_at: Some(now + Duration::from_millis(result_ttl_ms)),
                        },
                    );
                    result_ready.notify_one();

                    Ok(request_id.to_string())
                }
                Err(e) => {
                    metrics.increment_failed();
                    let latency = start.elapsed().as_secs_f64() * 1000.0;
                    let _ = metrics.record_latency(&db_identifier, latency).await;

                    // Record failed result via record_result to trigger notification
                    let now = Instant::now();
                    results.write().await.insert(
                        request_id,
                        QueryResult {
                            request_id,
                            status: ResultStatus::Failed,
                            output: None,
                            error: Some(e.clone()),
                            completed_at: Some(now),
                            expires_at: Some(now + Duration::from_millis(result_ttl_ms)),
                        },
                    );
                    result_ready.notify_one();

                    Err(e)
                }
            }
        });
    }

    fn checked_out_connections(&self) -> usize {
        self.connection_pool.checked_out_connections()
    }

    /// Get result for a specific request ID
    pub async fn get_result(&self, request_id: &Uuid) -> Option<QueryResult> {
        self.results.read().await.get(request_id).cloned()
    }

    /// Wait for a result with timeout (in milliseconds)
    ///
    /// Efficiently waits for a result using tokio::sync::Notify instead of polling.
    /// Notified when task completes, fails, or is cancelled.
    pub async fn wait_for_result(
        &self,
        request_id: &Uuid,
        timeout_ms: u64,
    ) -> Result<QueryResult, String> {
        let timeout = Duration::from_millis(timeout_ms);

        loop {
            // Check if result is ready
            if let Some(result) = self.results.read().await.get(request_id) {
                if result.status != ResultStatus::Pending {
                    return Ok(result.clone());
                }
            }

            // Wait for notification with timeout
            // notified() is cancelled by timeout, allowing early exit
            let notify_future = self.result_ready.notified();
            match tokio::time::timeout(timeout, notify_future).await {
                Ok(_) => {
                    // Notification received, loop to check result status
                    continue;
                }
                Err(_) => {
                    // Timeout expired
                    return Err("Timeout waiting for result".to_string());
                }
            }
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
            expires_at: Some(now + Duration::from_millis(self.result_ttl_ms)),
        };
        self.results.write().await.insert(request_id, result);
        // Notify any waiters that a result has been recorded
        self.result_ready.notify_one();
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
    ///
    /// Uses atomic operations to prevent race conditions:
    /// 1. Sets the cancellation token flag (non-blocking, idempotent)
    /// 2. Holds a write lock while checking and updating result status atomically
    /// 3. This ensures a completed task cannot be marked as cancelled after the final check
    pub async fn cancel_task(&self, request_id: &Uuid) -> bool {
        // Set cancellation flag first (non-blocking, idempotent)
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

        // Atomically check result status and update to Cancelled if still Pending
        // Holding the write lock prevents a race where the task completes between
        // the check and the update. This is critical for correctness.
        let should_notify = {
            let mut results = self.results.write().await;

            if let Some(result) = results.get(request_id) {
                // If already completed or failed, cannot cancel
                if result.status == ResultStatus::Completed || result.status == ResultStatus::Failed
                {
                    return false;
                }
            }

            // Create or update the result to Cancelled while holding the write lock
            let now = Instant::now();
            results.insert(
                *request_id,
                QueryResult {
                    request_id: *request_id,
                    status: ResultStatus::Cancelled,
                    output: Some("Task cancelled".to_string()),
                    error: None,
                    completed_at: Some(now),
                    expires_at: Some(now + Duration::from_millis(self.result_ttl_ms)),
                },
            );
            true
        };

        // Notify waiters after releasing the lock to minimize contention
        if should_notify {
            self.result_ready.notify_one();
        }
        should_notify
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

        for id in &expired_ids {
            results.remove(id);
            collected += 1;
        }

        // Also clean up associated cancellation tokens to prevent memory leaks
        if collected > 0 {
            let mut tokens = self.cancellation_tokens.write().await;
            for id in &expired_ids {
                tokens.remove(id);
            }
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

/// Cleanup implementation to signal background task to stop
impl Drop for ExecutionEngine {
    fn drop(&mut self) {
        // Signal cleanup task to stop
        self.cleanup_task_stopped.store(true, Ordering::SeqCst);
    }
}
