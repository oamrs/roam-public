use oam::policy_engine::ToolIntent;
use oam::QueryRuntimeContext;

#[test]
fn runtime_context_parses_metadata_into_structured_fields() {
    let mut request = tonic::Request::new(());
    request
        .metadata_mut()
        .insert("x-roam-session-id", "session-123".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-user-id", "user-9".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-organization-id", "finance".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-tool-name", "finance.query".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-tool-intent", "read_select".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-grants", "read:ledger,read:org".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roam-runtime-augmentation-id", "hook-1".parse().unwrap());
    request.metadata_mut().insert(
        "x-roam-runtime-augmentation-key",
        "finance-default".parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert("x-roam-domain-tags", "finance,accounting".parse().unwrap());
    request.metadata_mut().insert(
        "x-roam-table-names",
        "ledger_entries,organizations".parse().unwrap(),
    );

    let context = QueryRuntimeContext::from_metadata(request.metadata());

    assert_eq!(context.session_id.as_deref(), Some("session-123"));
    assert_eq!(context.user_id.as_deref(), Some("user-9"));
    assert_eq!(context.organization_id.as_deref(), Some("finance"));
    assert_eq!(context.tool_name.as_deref(), Some("finance.query"));
    assert_eq!(context.tool_intent, Some(ToolIntent::ReadSelect));
    assert_eq!(context.grants, vec!["read:ledger", "read:org"]);
    assert_eq!(context.runtime_augmentation_id.as_deref(), Some("hook-1"));
    assert_eq!(
        context.runtime_augmentation_key.as_deref(),
        Some("finance-default")
    );
    assert_eq!(context.domain_tags, vec!["finance", "accounting"]);
    assert_eq!(context.table_names, vec!["ledger_entries", "organizations"]);
}

#[test]
fn runtime_context_builds_policy_and_event_metadata() {
    let context = QueryRuntimeContext {
        tool_name: Some("finance.query".to_string()),
        tool_intent: Some(ToolIntent::ReadSelect),
        grants: vec!["read:ledger".to_string()],
        runtime_augmentation_key: Some("finance-default".to_string()),
        session_id: Some("session-123".to_string()),
        ..Default::default()
    }
    .with_registered_agent("agent-9", "2.4.1", "HYBRID");

    let policy_context = context.policy_context().expect("policy context");
    assert_eq!(policy_context.tool.name, "finance.query");
    assert_eq!(policy_context.tool.intent, ToolIntent::ReadSelect);
    assert_eq!(policy_context.authorization.grants, vec!["read:ledger"]);

    let metadata = context.event_metadata();
    assert_eq!(metadata.get("session_id"), Some(&"session-123".to_string()));
    assert_eq!(metadata.get("agent_id"), Some(&"agent-9".to_string()));
    assert_eq!(metadata.get("agent_version"), Some(&"2.4.1".to_string()));
    assert_eq!(metadata.get("schema_mode"), Some(&"HYBRID".to_string()));
    assert_eq!(
        metadata.get("runtime_augmentation_key"),
        Some(&"finance-default".to_string())
    );
    assert_eq!(
        metadata.get("tool_name"),
        Some(&"finance.query".to_string())
    );
}
