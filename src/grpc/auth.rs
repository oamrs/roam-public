//! Authentication and Authorization Module for gRPC Server
//!
//! Provides API key and token-based authentication mechanisms to control
//! access to the gRPC server and enforce authorization policies.

use std::collections::HashMap;

/// Authentication error types
#[derive(Debug, Clone)]
pub enum AuthError {
    MissingToken,
    InvalidToken,
    InvalidFormat,
    Unauthorized,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::MissingToken => write!(f, "Missing authentication token"),
            AuthError::InvalidToken => write!(f, "Invalid authentication token"),
            AuthError::InvalidFormat => write!(f, "Invalid authentication header format"),
            AuthError::Unauthorized => write!(f, "Unauthorized: insufficient permissions"),
        }
    }
}

/// Represents a client's authenticated session
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedClient {
    pub client_id: String,
    pub permissions: Vec<String>,
}

impl AuthenticatedClient {
    /// Check if client has a specific permission
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(&permission.to_string())
            || self.permissions.contains(&"*".to_string()) // Wildcard permission
    }

    /// Check if client can execute queries
    pub fn can_execute_queries(&self) -> bool {
        self.has_permission("execute_query")
    }

    /// Check if client can retrieve schema
    pub fn can_read_schema(&self) -> bool {
        self.has_permission("read_schema")
    }
}

/// API Key authentication provider
#[derive(Clone, Debug)]
pub struct ApiKeyAuth {
    keys: HashMap<String, AuthenticatedClient>,
}

impl ApiKeyAuth {
    /// Create a new API key authenticator
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
        }
    }

    /// Register an API key with associated client info and permissions
    pub fn register_key(&mut self, api_key: String, client_id: String, permissions: Vec<String>) {
        self.keys.insert(
            api_key,
            AuthenticatedClient {
                client_id,
                permissions,
            },
        );
    }

    /// Authenticate using an API key
    pub fn authenticate(&self, api_key: &str) -> Result<AuthenticatedClient, AuthError> {
        self.keys
            .get(api_key)
            .cloned()
            .ok_or(AuthError::InvalidToken)
    }

    /// Load keys from environment variable (comma-separated key=client_id format)
    /// Format: "key1=client1:execute_query;read_schema,key2=client2:*"
    pub fn load_from_env(&mut self, env_var: &str) -> Result<(), String> {
        let config = std::env::var(env_var).map_err(|_| {
            format!(
                "Environment variable {} not set for API key configuration",
                env_var
            )
        })?;

        for entry in config.split(',') {
            let parts: Vec<&str> = entry.trim().split('=').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid API key config entry: {}", entry));
            }

            let api_key = parts[0].to_string();
            let client_config = parts[1];

            let client_parts: Vec<&str> = client_config.split(':').collect();
            if client_parts.is_empty() {
                return Err(format!("Invalid client config: {}", client_config));
            }

            let client_id = client_parts[0].to_string();
            let permissions = if client_parts.len() > 1 {
                client_parts[1].split(';').map(|p| p.to_string()).collect()
            } else {
                vec!["execute_query".to_string(), "read_schema".to_string()]
            };

            self.register_key(api_key, client_id, permissions);
        }

        Ok(())
    }

    /// Get number of registered keys (useful for testing)
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }
}

impl Default for ApiKeyAuth {
    fn default() -> Self {
        Self::new()
    }
}

/// Token-based authentication provider
#[derive(Clone, Debug)]
pub struct TokenAuth {
    tokens: HashMap<String, AuthenticatedClient>,
}

impl TokenAuth {
    /// Create a new token authenticator
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }

    /// Register a token with associated client info and permissions
    pub fn register_token(&mut self, token: String, client_id: String, permissions: Vec<String>) {
        self.tokens.insert(
            token,
            AuthenticatedClient {
                client_id,
                permissions,
            },
        );
    }

    /// Authenticate using a token (typically from Bearer token in Authorization header)
    pub fn authenticate(&self, token: &str) -> Result<AuthenticatedClient, AuthError> {
        self.tokens
            .get(token)
            .cloned()
            .ok_or(AuthError::InvalidToken)
    }

    /// Get number of registered tokens (useful for testing)
    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }
}

impl Default for TokenAuth {
    fn default() -> Self {
        Self::new()
    }
}

/// Generic authentication provider that supports multiple mechanisms
#[derive(Clone, Debug)]
pub struct AuthProvider {
    api_key_auth: Option<ApiKeyAuth>,
    token_auth: Option<TokenAuth>,
    require_auth: bool,
}

impl AuthProvider {
    /// Create a new authentication provider
    pub fn new() -> Self {
        Self {
            api_key_auth: None,
            token_auth: None,
            require_auth: false,
        }
    }

    /// Enable API key authentication
    pub fn enable_api_keys(mut self, auth: ApiKeyAuth) -> Self {
        self.api_key_auth = Some(auth);
        self.require_auth = true;
        self
    }

    /// Enable token authentication
    pub fn enable_tokens(mut self, auth: TokenAuth) -> Self {
        self.token_auth = Some(auth);
        self.require_auth = true;
        self
    }

    /// Disable all authentication (for development/testing only)
    pub fn disable_auth(mut self) -> Self {
        self.require_auth = false;
        self
    }

    /// Authenticate using Authorization header
    /// Supports: "Bearer <token>" or "ApiKey <key>"
    pub fn authenticate_from_header(
        &self,
        auth_header: &str,
    ) -> Result<AuthenticatedClient, AuthError> {
        if !self.require_auth {
            // No authentication required - create a default unrestricted client
            return Ok(AuthenticatedClient {
                client_id: "anonymous".to_string(),
                permissions: vec!["*".to_string()],
            });
        }

        let parts: Vec<&str> = auth_header.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(AuthError::InvalidFormat);
        }

        match parts[0].to_lowercase().as_str() {
            "bearer" => self
                .token_auth
                .as_ref()
                .ok_or(AuthError::Unauthorized)?
                .authenticate(parts[1]),
            "apikey" => self
                .api_key_auth
                .as_ref()
                .ok_or(AuthError::Unauthorized)?
                .authenticate(parts[1]),
            _ => Err(AuthError::InvalidFormat),
        }
    }

    /// Check if authentication is required
    pub fn is_auth_required(&self) -> bool {
        self.require_auth
    }
}

impl Default for AuthProvider {
    fn default() -> Self {
        Self::new()
    }
}
