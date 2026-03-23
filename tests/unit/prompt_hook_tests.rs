use oam::prompt_hooks::{
    preview_prompt_hook, render_template, resolve_prompt_hook, PromptHookDefinition,
    PromptHookPreviewRequest, PromptHookRequestContext, PromptHookResolveRequest,
    PromptHookSchemaContext,
};

#[derive(Clone)]
struct TestHook {
    id: String,
    name: String,
    enabled: bool,
    priority: i32,
    selector_key: Option<String>,
    markdown_template: String,
    matching_rules_yaml: Option<String>,
}

impl PromptHookDefinition for TestHook {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn priority(&self) -> i32 {
        self.priority
    }

    fn selector_key(&self) -> Option<&str> {
        self.selector_key.as_deref()
    }

    fn markdown_template(&self) -> &str {
        &self.markdown_template
    }

    fn matching_rules_yaml(&self) -> Option<&str> {
        self.matching_rules_yaml.as_deref()
    }
}

fn sample_hook(
    id: &str,
    name: &str,
    priority: i32,
    selector_key: Option<&str>,
    matching_rules_yaml: Option<&str>,
    markdown_template: &str,
) -> TestHook {
    TestHook {
        id: id.to_string(),
        name: name.to_string(),
        enabled: true,
        priority,
        selector_key: selector_key.map(ToString::to_string),
        markdown_template: markdown_template.to_string(),
        matching_rules_yaml: matching_rules_yaml.map(ToString::to_string),
    }
}

#[test]
fn preview_reports_match_and_renders_markdown() {
    let preview = preview_prompt_hook(&PromptHookPreviewRequest {
        markdown_template: "# Prompt\nHello {{organization_id}} using {{schema_table_names_csv}}"
            .to_string(),
        matching_rules_yaml: Some(
            "request:\n  organization_ids: [finance]\nschema:\n  table_names: [ledger_entries]"
                .to_string(),
        ),
        resolve_request: PromptHookResolveRequest {
            request_context: PromptHookRequestContext {
                organization_id: Some("finance".to_string()),
                ..Default::default()
            },
            schema_context: PromptHookSchemaContext {
                table_names: vec!["ledger_entries".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    })
    .expect("preview should succeed");

    assert!(preview.matches_context);
    assert!(preview.rendered_prompt.contains("finance"));
    assert!(preview.rendered_prompt.contains("ledger_entries"));
}

#[test]
fn explicit_hook_id_overrides_priority_matching() {
    let hooks = vec![
        sample_hook(
            "finance-default",
            "Finance Default",
            100,
            Some("finance-default"),
            Some("request:\n  organization_ids: [finance]"),
            "Finance {{organization_id}}",
        ),
        sample_hook(
            "manual-override",
            "Manual Override",
            1,
            Some("manual-override"),
            None,
            "Manual {{organization_id}}",
        ),
    ];

    let resolution = resolve_prompt_hook(
        &hooks,
        &PromptHookResolveRequest {
            explicit_hook_id: Some("manual-override".to_string()),
            request_context: PromptHookRequestContext {
                organization_id: Some("finance".to_string()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("resolution should succeed");

    assert_eq!(resolution.selected_hook_id, "manual-override");
    assert_eq!(resolution.selection_reason, "explicit_hook_id");
}

#[test]
fn highest_priority_matching_hook_is_selected() {
    let hooks = vec![
        sample_hook(
            "generic",
            "Generic",
            10,
            None,
            Some("request:\n  tools: [query]"),
            "Generic {{tool_name}}",
        ),
        sample_hook(
            "schema-specific",
            "Schema Specific",
            50,
            None,
            Some("request:\n  tools: [query]\nschema:\n  table_names: [ledger_entries]"),
            "Schema {{schema_table_names_csv}}",
        ),
    ];

    let resolution = resolve_prompt_hook(
        &hooks,
        &PromptHookResolveRequest {
            request_context: PromptHookRequestContext {
                tool_name: Some("query".to_string()),
                ..Default::default()
            },
            schema_context: PromptHookSchemaContext {
                table_names: vec!["ledger_entries".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect("resolution should succeed");

    assert_eq!(resolution.selected_hook_id, "schema-specific");
    assert_eq!(resolution.selection_reason, "priority_match");
}

#[test]
fn ambiguous_priority_match_fails_closed() {
    let hooks = vec![
        sample_hook(
            "a",
            "Hook A",
            50,
            None,
            Some("request:\n  organization_ids: [finance]"),
            "A {{organization_id}}",
        ),
        sample_hook(
            "b",
            "Hook B",
            50,
            None,
            Some("request:\n  organization_ids: [finance]"),
            "B {{organization_id}}",
        ),
    ];

    let error = resolve_prompt_hook(
        &hooks,
        &PromptHookResolveRequest {
            request_context: PromptHookRequestContext {
                organization_id: Some("finance".to_string()),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .expect_err("resolution should fail when priority ties are ambiguous");

    assert!(error.contains("ambiguous"));
}

#[test]
fn template_rendering_rejects_unknown_variables() {
    let error = render_template("Hello {{unknown_value}}", &Default::default())
        .expect_err("unknown variables should fail validation");

    assert!(error.contains("unknown_value"));
}
