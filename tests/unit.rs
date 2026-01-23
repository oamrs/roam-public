// Test harness to expose tests placed under `tests/unit/` subdirectory.
// Cargo only discovers test files directly under `tests/`, so we include
// the file from the `unit/` folder here to satisfy the requested layout.

#[path = "unit/mirror_tests.rs"]
mod mirror_tests;

#[path = "unit/interceptor_tests.rs"]
mod interceptor_tests;

#[path = "unit/executor_tests.rs"]
mod executor_tests;

#[path = "unit/auth_tests.rs"]
mod auth_tests;

#[path = "unit/rate_limit_tests.rs"]
mod rate_limit_tests;
