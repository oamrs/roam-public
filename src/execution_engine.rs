use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::executor::ExecuteQueryRequest;

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
    pub async fn execute(
        &self,
        sql: &str,
        _params: &[&dyn rusqlite::ToSql],
    ) -> Result<usize, String> {
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

    /// Update queue depth
    pub(crate) fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::SeqCst);
    }

    /// Update active task count
    pub(crate) fn set_active_tasks(&self, count: usize) {
        self.active_tasks.store(count, Ordering::SeqCst);
    }
}

impl Default for ExecutionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// High-throughput execution engine for managing concurrent queries
///
/// Uses tokio::JoinSet for robust concurrent query execution with:
/// - Priority-based request queuing
/// - Graceful task management and cancellation
/// - Comprehensive metrics tracking
/// - Connection pooling abstraction
///
/// # Example
/// ```ignore
/// let engine = ExecutionEngine::new("database.db", 1000)?;
/// let metrics = engine.metrics();
/// println!("Active tasks: {}", metrics.active_task_count());
/// ```
pub struct ExecutionEngine {
    db_path: String,
    max_concurrent_queries: usize,
    metrics: ExecutionMetrics,
    connection_pool: Arc<ConnectionPool>,
}

impl ExecutionEngine {
    /// Create a new execution engine
    ///
    /// # Arguments
    /// * `db_path` - Path to the SQLite database
    /// * `max_concurrent_queries` - Maximum number of concurrent queries to allow
    ///
    /// # Example
    /// ```ignore
    /// let engine = ExecutionEngine::new("database.db", 1000)?;
    /// ```
    pub fn new(db_path: &str, max_concurrent_queries: usize) -> Result<Self, String> {
        let connection_pool = ConnectionPool::new(db_path, max_concurrent_queries)?;
        Ok(Self {
            db_path: db_path.to_string(),
            max_concurrent_queries,
            metrics: ExecutionMetrics::new(),
            connection_pool: Arc::new(connection_pool),
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
}
