use oam::grpc_executor::GrpcAgentServiceImpl;
use oam::interceptor::{get_event_bus, Event as DomainEvent};
use roam_proto::v1::agent::{
    agent_service_server::AgentService, ConnectRequest, EventStreamRequest, SchemaMode,
};
use serial_test::serial;
use tokio::time::{timeout, Duration};
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

    let session = service
        .registered_session(&response.session_id)
        .await
        .expect("stored session");
    assert_eq!(session.agent_id, "test-agent");
    assert_eq!(session.version, "0.1.0");
    assert_eq!(session.schema_mode, SchemaMode::DataFirst);
}

#[serial_test::serial(event_bus)]
#[tokio::test]
async fn grpc_agent_stream_events_returns_session_filtered_events() {
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = GrpcAgentServiceImpl::default();

    let register_response = service
        .register(Request::new(ConnectRequest {
            agent_id: "stream-agent".to_string(),
            version: "0.2.0".to_string(),
            mode: SchemaMode::CodeFirst.into(),
        }))
        .await
        .expect("register session")
        .into_inner();

    let request = Request::new(EventStreamRequest {
        session_id: register_response.session_id.clone(),
    });

    let response = service.stream_events(request).await;
    assert!(response.is_ok(), "Stream events call should succeed");

    let mut stream = response.unwrap().into_inner();

    let _ = event_bus.dispatch_generic(&DomainEvent::query_validation_failed(
        "db".to_string(),
        "SELECT 1".to_string(),
        "bad query".to_string(),
        chrono::Utc::now().to_rfc3339(),
        [(
            "session_id".to_string(),
            register_response.session_id.clone(),
        )]
        .into_iter()
        .collect(),
    ));

    let next_event = timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("event received before timeout")
        .expect("stream item")
        .expect("grpc event payload");

    assert_eq!(next_event.r#type, "QueryValidationFailed");
    assert!(!next_event.payload.is_empty());
}

#[serial(event_bus)]
#[tokio::test]
async fn test_register_dispatches_session_registered_event() {
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let service = GrpcAgentServiceImpl::default();

    let response = service
        .register(Request::new(ConnectRequest {
            agent_id: "memory-agent".to_string(),
            version: "1.0.0".to_string(),
            mode: SchemaMode::DataFirst.into(),
        }))
        .await
        .expect("register")
        .into_inner();

    let events = event_bus
        .events_by_type("SessionRegistered")
        .expect("get events");

    assert_eq!(events.len(), 1, "exactly one SessionRegistered event");

    let metadata = events[0].metadata();
    assert_eq!(
        metadata.get("session_id"),
        Some(&response.session_id),
        "session_id in event metadata matches response"
    );
}
