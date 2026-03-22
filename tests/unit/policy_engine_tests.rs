use oam::policy_engine::{
    AuthorizationContext, AuthorizedSubqueryShape, PolicyContext, PolicyDecision, PolicyEngine,
    SubqueryPolicy, ToolContract, ToolIntent,
};

fn assert_denied(decision: &PolicyDecision, expected_fragment: &str) {
    assert!(!decision.allowed, "expected policy decision to deny query");
    assert!(
        decision
            .reason
            .as_deref()
            .unwrap_or_default()
            .to_uppercase()
            .contains(&expected_fragment.to_uppercase()),
        "expected deny reason to contain '{expected_fragment}', got {:?}",
        decision.reason
    );
}

#[test]
fn read_intent_allows_basic_select_join() {
    let decision = PolicyEngine::evaluate(
        "SELECT u.id, o.name FROM users u JOIN organizations o ON o.id = u.organization_id",
        ToolIntent::ReadSelect,
    );

    assert!(decision.allowed, "basic read query should be allowed");
    assert_eq!(decision.classification, "read-select");
}

#[test]
fn read_intent_rejects_transaction_control() {
    let decision = PolicyEngine::evaluate("BEGIN TRANSACTION", ToolIntent::ReadSelect);

    assert_denied(&decision, "transaction");
}

#[test]
fn read_intent_rejects_ddl() {
    let decision = PolicyEngine::evaluate("DROP TABLE users", ToolIntent::ReadSelect);

    assert_denied(&decision, "ddl");
}

#[test]
fn read_intent_rejects_write_hidden_in_cte() {
    let decision = PolicyEngine::evaluate(
        "WITH touched AS (DELETE FROM users RETURNING id) SELECT id FROM touched",
        ToolIntent::ReadSelect,
    );

    assert_denied(&decision, "delete");
}

#[test]
fn read_intent_rejects_nested_subquery() {
    let decision = PolicyEngine::evaluate(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM organizations)",
        ToolIntent::ReadSelect,
    );

    assert_denied(&decision, "subquery");
}

#[test]
fn policy_context_allows_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM organizations)",
        &context,
    );

    assert!(
        decision.allowed,
        "allow-listed subquery should be permitted"
    );
    assert_eq!(decision.classification, "read-select");
}

#[test]
fn policy_context_allows_schema_qualified_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM main.organizations)",
        &context,
    );

    assert!(
        decision.allowed,
        "schema-qualified allow-listed subquery should be permitted"
    );
    assert_eq!(decision.classification, "read-select");
}

#[test]
fn policy_context_allows_quoted_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM \"organizations\")",
        &context,
    );

    assert!(
        decision.allowed,
        "quoted allow-listed subquery should be permitted"
    );
    assert_eq!(decision.classification, "read-select");
}

#[test]
fn policy_context_rejects_non_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM departments)",
        &context,
    );

    assert_denied(&decision, "departments");
}

#[test]
fn policy_context_rejects_quoted_non_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM [departments])",
        &context,
    );

    assert_denied(&decision, "departments");
}

#[test]
fn policy_context_rejects_schema_qualified_non_allowlisted_subquery_shape() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM main.departments)",
        &context,
    );

    assert_denied(&decision, "departments");
}

#[test]
fn policy_context_rejects_set_operation_inside_allowlisted_subquery() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users-by-organization".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::AllowListed(vec![AuthorizedSubqueryShape {
                table: "organizations".to_string(),
            }]),
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::ReadSelect],
            grants: vec!["tool:users.read".to_string()],
        },
    };

    let decision = PolicyEngine::evaluate_with_context(
        "SELECT id FROM users WHERE organization_id IN (SELECT id FROM organizations UNION SELECT id FROM organizations)",
        &context,
    );

    assert_denied(&decision, "union");
}

#[test]
fn authorization_context_rejects_unauthorized_intent() {
    let context = PolicyContext {
        tool: ToolContract {
            name: "list-users".to_string(),
            intent: ToolIntent::ReadSelect,
            subquery_policy: SubqueryPolicy::DenyAll,
        },
        authorization: AuthorizationContext {
            allowed_intents: vec![ToolIntent::WriteUpdate],
            grants: vec![],
        },
    };

    let decision = PolicyEngine::evaluate_with_context("SELECT id FROM users", &context);

    assert_denied(&decision, "authorized");
}
