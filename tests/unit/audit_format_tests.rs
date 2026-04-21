use oam::audit_format::event_to_ocsf;
use oam::interceptor::{AuditEventEnvelope, Event};

fn make_envelope(event: Event) -> AuditEventEnvelope {
    AuditEventEnvelope {
        sequence: 1,
        trace_id: None,
        span_id: None,
        prev_hash: String::new(),
        hash: "deadbeef".to_string(),
        event,
        emitted_at: "2025-01-01T00:00:00Z".to_string(),
    }
}

fn make_envelope_with_trace(event: Event) -> AuditEventEnvelope {
    AuditEventEnvelope {
        sequence: 42,
        trace_id: Some("trace-abc-123".to_string()),
        span_id: Some("span-xyz-456".to_string()),
        prev_hash: "prevhash".to_string(),
        hash: "cafebabe".to_string(),
        event,
        emitted_at: "2025-06-01T12:00:00Z".to_string(),
    }
}

#[test]
fn query_executed_maps_to_database_activity() {
    let event = Event::query_executed(
        "db1".into(),
        "SELECT 1".into(),
        "ok".into(),
        1,
        5,
        "T".into(),
        Default::default(),
    );
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["class_uid"], 6003);
    assert_eq!(ocsf["class_name"], "Database Activity");
}

#[test]
fn session_registered_maps_to_authentication() {
    let event = Event::session_registered("sess-1".into());
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["class_uid"], 2001);
    assert_eq!(ocsf["class_name"], "Authentication");
    assert_eq!(ocsf["activity_id"], 1); // Logon
}

#[test]
fn access_denied_maps_to_security_finding() {
    let event = Event::access_denied(
        "db1".into(),
        "DROP TABLE".into(),
        "alice".into(),
        "no permission".into(),
        Default::default(),
    );
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["class_uid"], 2004);
    assert_eq!(ocsf["class_name"], "Security Finding");
}

#[test]
fn injection_signal_maps_to_security_finding() {
    let event = Event::prompt_injection_signal_raised(
        "ignore all prev".into(),
        "abc123".into(),
        vec!["instruction_override".into()],
        "high".into(),
        "blocked".into(),
        Default::default(),
    );
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["class_uid"], 2004);
    assert_eq!(ocsf["severity_id"], 4); // High == 4 in OCSF
}

#[test]
fn ocsf_record_contains_chain_fields() {
    let event = Event::session_registered("s".into());
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["unmapped"]["roam_hash"], "deadbeef");
    assert_eq!(ocsf["unmapped"]["roam_sequence"], 1);
}

#[test]
fn ocsf_record_includes_trace_correlation_when_present() {
    let event = Event::session_registered("traced-session".into());
    let ocsf = event_to_ocsf(&make_envelope_with_trace(event));
    assert_eq!(ocsf["metadata"]["trace_uid"], "trace-abc-123");
    assert_eq!(ocsf["metadata"]["span_uid"], "span-xyz-456");
    assert_eq!(ocsf["unmapped"]["roam_sequence"], 42);
    assert_eq!(ocsf["unmapped"]["roam_prev_hash"], "prevhash");
}

#[test]
fn ocsf_version_is_set() {
    let event = Event::session_registered("ver-check".into());
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["metadata"]["version"], "1.1.0");
}

#[test]
fn llm_tool_call_maps_to_api_activity() {
    let event = Event::llm_tool_call_audit_recorded(
        "GET /api/orgs".into(),
        r#"{"path":"/api/orgs"}"#.into(),
        "http:200".into(),
        42,
        Default::default(),
    );
    let ocsf = event_to_ocsf(&make_envelope(event));
    assert_eq!(ocsf["class_uid"], 6005);
    assert_eq!(ocsf["class_name"], "API Activity");
}
