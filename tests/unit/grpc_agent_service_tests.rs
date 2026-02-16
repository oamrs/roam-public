use oam::grpc_executor::GrpcAgentServiceImpl;
use roam_proto::v1::agent::{
    agent_service_server::AgentService, ConnectRequest, EventStreamRequest, SchemaMode,
};
use tokio_stream::StreamExt;
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn grpc_agent_registration_returns_valid_session_id() {
    let service = GrpcAgentServiceImpl::default();

    let request = Request::new(ConnectRequest {
        agent_id: "test-agent".to_string(),
        version: "0.1.0".to_string(),
        mode: SchemaMode::DataFirst.into(),
    });

    let result = service.register(request).await;
    assert!(result.is_ok(), "Register call should succeed");

    let response = result.unwrap().into_inner();
    assert!(response.success, "Response should be successful");
    assert!(
        !response.session_id.is_empty(),
        "Session ID should not be empty"
    );

    // Validate UUID format
    assert!(
        Uuid::parse_str(&response.session_id).is_ok(),
        "Session ID must be a valid UUID"
    );
}

#[tokio::test]
async fn grpc_agent_stream_events_returns_stream() {
    let service = GrpcAgentServiceImpl::default();

    let request = Request::new(EventStreamRequest {
        session_id: "test-session-id".to_string(),
    });

    let response = service.stream_events(request).await;
    assert!(response.is_ok(), "Stream events call should succeed");

    let mut stream = response.unwrap().into_inner();
    let next_event = stream.next().await;
    assert!(next_event.is_none(), "Stream should be empty for now");
}
