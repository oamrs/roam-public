//! Rate Limiting and Connection Throttling for gRPC Server

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct RateLimitConfig {
    pub requests_per_second: u32,
    pub max_concurrent_connections: u32,
    pub max_total_connections: u32,
    pub window_seconds: u32,
}

impl RateLimitConfig {
    pub fn new() -> Self {
        Self {
            requests_per_second: 100,
            max_concurrent_connections: 10,
            max_total_connections: 1000,
            window_seconds: 1,
        }
    }

    // Permissive config for development/testing
    pub fn permissive() -> Self {
        Self {
            requests_per_second: 10000,
            max_concurrent_connections: 100,
            max_total_connections: 10000,
            window_seconds: 1,
        }
    }

    // Strict config for production security
    pub fn strict() -> Self {
        Self {
            requests_per_second: 50,
            max_concurrent_connections: 5,
            max_total_connections: 500,
            window_seconds: 1,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ClientRateLimit {
    request_times: Vec<Instant>,
    concurrent_connections: u32,
}

impl ClientRateLimit {
    fn new() -> Self {
        Self {
            request_times: Vec::new(),
            concurrent_connections: 0,
        }
    }

    fn check_request(&mut self, config: &RateLimitConfig, now: Instant) -> bool {
        // Clean old requests outside the window
        let window = Duration::from_secs(config.window_seconds as u64);
        self.request_times
            .retain(|t| now.duration_since(*t) < window);

        // Check rate limit
        if self.request_times.len() >= config.requests_per_second as usize {
            return false;
        }

        self.request_times.push(now);
        true
    }

    fn can_connect(&self, config: &RateLimitConfig) -> bool {
        self.concurrent_connections < config.max_concurrent_connections
    }

    fn increment_connections(&mut self) {
        self.concurrent_connections += 1;
    }

    fn decrement_connections(&mut self) {
        if self.concurrent_connections > 0 {
            self.concurrent_connections -= 1;
        }
    }
}

pub struct RateLimiter {
    config: RateLimitConfig,
    client_limits: Mutex<HashMap<String, ClientRateLimit>>,
    total_connections: Mutex<u32>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            client_limits: Mutex::new(HashMap::new()),
            total_connections: Mutex::new(0),
        }
    }

    pub async fn check_connection(&self, peer_addr: SocketAddr) -> Result<(), String> {
        let client_id = peer_addr.to_string();

        // Check global connection limit
        let mut total = self.total_connections.lock().await;
        if *total >= self.config.max_total_connections {
            return Err(format!(
                "Server at maximum capacity: {} connections",
                self.config.max_total_connections
            ));
        }

        // Check per-client connection limit
        let mut limits = self.client_limits.lock().await;
        let client_limit = limits.entry(client_id).or_insert_with(ClientRateLimit::new);

        if !client_limit.can_connect(&self.config) {
            return Err(format!(
                "Too many concurrent connections from {}. Max: {}",
                peer_addr, self.config.max_concurrent_connections
            ));
        }

        client_limit.increment_connections();
        *total += 1;

        Ok(())
    }

    /// Signal that a connection has closed
    pub async fn close_connection(&self, peer_addr: SocketAddr) {
        let client_id = peer_addr.to_string();

        let mut limits = self.client_limits.lock().await;
        if let Some(client_limit) = limits.get_mut(&client_id) {
            client_limit.decrement_connections();
        }

        let mut total = self.total_connections.lock().await;
        if *total > 0 {
            *total -= 1;
        }
    }

    pub async fn check_request(&self, peer_addr: SocketAddr) -> Result<(), String> {
        let client_id = peer_addr.to_string();
        let now = Instant::now();

        let mut limits = self.client_limits.lock().await;
        let client_limit = limits.entry(client_id).or_insert_with(ClientRateLimit::new);

        if client_limit.check_request(&self.config, now) {
            Ok(())
        } else {
            Err(format!(
                "Rate limit exceeded: {} requests/sec",
                self.config.requests_per_second
            ))
        }
    }

    pub async fn get_stats(&self) -> RateLimiterStats {
        let limits = self.client_limits.lock().await;
        let total_connections = *self.total_connections.lock().await;

        let active_clients = limits
            .iter()
            .filter(|(_, limit)| limit.concurrent_connections > 0)
            .count();

        RateLimiterStats {
            total_connections,
            active_clients,
            config: self.config.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    pub total_connections: u32,
    pub active_clients: usize,
    pub config: RateLimitConfig,
}
