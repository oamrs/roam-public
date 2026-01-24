use oam::tcp::auth::{ApiKeyAuth, AuthProvider, AuthenticatedClient, TokenAuth};

#[test]
fn test_api_key_auth() {
    let mut auth = ApiKeyAuth::new();
    auth.register_key(
        "test-key-123".to_string(),
        "test-client".to_string(),
        vec!["execute_query".to_string(), "read_schema".to_string()],
    );

    let client = auth.authenticate("test-key-123").unwrap();
    assert_eq!(client.client_id, "test-client");
    assert!(client.can_execute_queries());
    assert!(client.can_read_schema());
}

#[test]
fn test_api_key_auth_invalid() {
    let auth = ApiKeyAuth::new();
    assert!(auth.authenticate("invalid-key").is_err());
}

#[test]
fn test_token_auth() {
    let mut auth = TokenAuth::new();
    auth.register_token(
        "test-token-abc".to_string(),
        "token-client".to_string(),
        vec!["execute_query".to_string()],
    );

    let client = auth.authenticate("test-token-abc").unwrap();
    assert_eq!(client.client_id, "token-client");
    assert!(client.can_execute_queries());
    assert!(!client.can_read_schema());
}

#[test]
fn test_auth_provider_with_api_key() {
    let mut api_key_auth = ApiKeyAuth::new();
    api_key_auth.register_key(
        "key123".to_string(),
        "client1".to_string(),
        vec!["execute_query".to_string()],
    );

    let provider = AuthProvider::new().enable_api_keys(api_key_auth);
    let client = provider.authenticate_from_header("ApiKey key123").unwrap();
    assert_eq!(client.client_id, "client1");
}

#[test]
fn test_auth_provider_with_bearer_token() {
    let mut token_auth = TokenAuth::new();
    token_auth.register_token(
        "token-xyz".to_string(),
        "client2".to_string(),
        vec!["*".to_string()],
    );

    let provider = AuthProvider::new().enable_tokens(token_auth);
    let client = provider
        .authenticate_from_header("Bearer token-xyz")
        .unwrap();
    assert_eq!(client.client_id, "client2");
    assert!(client.has_permission("any_permission"));
}

#[test]
fn test_auth_disabled() {
    let provider = AuthProvider::new().disable_auth();
    let client = provider.authenticate_from_header("anything-goes").unwrap();
    assert_eq!(client.client_id, "anonymous");
    assert!(client.has_permission("execute_query"));
}

#[test]
fn test_wildcard_permissions() {
    let client = AuthenticatedClient {
        client_id: "admin".to_string(),
        permissions: vec!["*".to_string()],
    };

    assert!(client.has_permission("any_action"));
    assert!(client.can_execute_queries());
    assert!(client.can_read_schema());
}
