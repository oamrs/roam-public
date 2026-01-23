use oam::{RateLimitConfig, RateLimiter};

#[tokio::test]
async fn test_rate_limiter_allows_requests_under_limit() {
    let config = RateLimitConfig {
        requests_per_second: 10,
        max_concurrent_connections: 5,
        max_total_connections: 100,
        window_seconds: 1,
    };

    let limiter = RateLimiter::new(config);
    let addr = "127.0.0.1:50000".parse().unwrap();

    // Connection check
    assert!(limiter.check_connection(addr).await.is_ok());

    // Request checks
    for _ in 0..10 {
        assert!(limiter.check_request(addr).await.is_ok());
    }

    limiter.close_connection(addr).await;
}

#[tokio::test]
async fn test_rate_limiter_blocks_excess_requests() {
    let config = RateLimitConfig {
        requests_per_second: 5,
        max_concurrent_connections: 5,
        max_total_connections: 100,
        window_seconds: 1,
    };

    let limiter = RateLimiter::new(config);
    let addr = "127.0.0.1:50001".parse().unwrap();

    limiter.check_connection(addr).await.unwrap();

    // Exhaust the rate limit
    for _ in 0..5 {
        assert!(limiter.check_request(addr).await.is_ok());
    }

    // Next request should be blocked
    assert!(limiter.check_request(addr).await.is_err());

    limiter.close_connection(addr).await;
}

#[tokio::test]
async fn test_rate_limiter_concurrent_connections() {
    let config = RateLimitConfig {
        requests_per_second: 100,
        max_concurrent_connections: 2,
        max_total_connections: 100,
        window_seconds: 1,
    };

    let limiter = RateLimiter::new(config);
    let addr1 = "127.0.0.1:50002".parse().unwrap();
    let addr2 = "127.0.0.1:50003".parse().unwrap();

    // Both should connect
    assert!(limiter.check_connection(addr1).await.is_ok());
    assert!(limiter.check_connection(addr1).await.is_ok());

    // Third connection from same client should fail
    assert!(limiter.check_connection(addr1).await.is_err());

    // Different client should connect
    assert!(limiter.check_connection(addr2).await.is_ok());

    limiter.close_connection(addr1).await;
    limiter.close_connection(addr1).await;
    limiter.close_connection(addr2).await;
}

#[tokio::test]
async fn test_rate_limiter_global_connection_limit() {
    let config = RateLimitConfig {
        requests_per_second: 100,
        max_concurrent_connections: 100,
        max_total_connections: 2,
        window_seconds: 1,
    };

    let limiter = RateLimiter::new(config);
    let addr1 = "127.0.0.1:50004".parse().unwrap();
    let addr2 = "127.0.0.1:50005".parse().unwrap();
    let addr3 = "127.0.0.1:50006".parse().unwrap();

    // First two should succeed
    assert!(limiter.check_connection(addr1).await.is_ok());
    assert!(limiter.check_connection(addr2).await.is_ok());

    // Third should fail due to global limit
    assert!(limiter.check_connection(addr3).await.is_err());

    limiter.close_connection(addr1).await;
    limiter.close_connection(addr2).await;
}

#[tokio::test]
async fn test_rate_limiter_stats() {
    let config = RateLimitConfig {
        requests_per_second: 100,
        max_concurrent_connections: 10,
        max_total_connections: 100,
        window_seconds: 1,
    };

    let limiter = RateLimiter::new(config);
    let addr1 = "127.0.0.1:50007".parse().unwrap();
    let addr2 = "127.0.0.1:50008".parse().unwrap();

    limiter.check_connection(addr1).await.unwrap();
    limiter.check_connection(addr2).await.unwrap();

    let stats = limiter.get_stats().await;
    assert_eq!(stats.total_connections, 2);
    assert_eq!(stats.active_clients, 2);

    limiter.close_connection(addr1).await;
    limiter.close_connection(addr2).await;

    let stats = limiter.get_stats().await;
    assert_eq!(stats.total_connections, 0);
}
