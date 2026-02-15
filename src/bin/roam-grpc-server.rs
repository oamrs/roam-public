use oam::grpc_executor::GrpcExecutor;
use std::env;
use std::path::PathBuf;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure Address
    let addr = env::var("ROAM_GRPC_ADDR").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    
    // 2. Configure Database (Temporary for tests, or persistent if specified)
    let db_path = env::var("ROAM_DB_PATH").unwrap_or_else(|_| {
        let mut path = PathBuf::from(env::temp_dir());
        path.push("roam_test.db");
        path.to_string_lossy().to_string()
    });

    println!("Starting ROAM Public gRPC Server...");
    println!(">> Address: {}", addr);
    println!(">> Database: {}", db_path);

    // 3. Initialize Executor
    let executor = GrpcExecutor::new(&db_path).map_err(|e| format!("Failed to init executor: {}", e))?;

    // 4. Start Server
    let handle = executor.start_server(&addr).await?;
    
    println!("Server running. Press Ctrl+C to stop.");

    // 5. Wait for shutdown signal
    signal::ctrl_c().await?;
    println!("Shutting down...");
    
    // The handle is dropped here, stopping the server
    Ok(())
}
