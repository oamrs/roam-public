use oam::executor::{QueryService, QueryServiceImpl, SchemaService, SchemaServiceImpl};
use oam::generated::{GetSchemaRequest, GetTableRequest, ValidateQueryRequest};

// ============================================================================
// PHASE 1B: Database Integration Tests
// ============================================================================

use tempfile::NamedTempFile;

/// Test 1B.1: QueryService rejects queries against non-existent tables
#[tokio::test]
async fn query_service_rejects_nonexistent_table() {
    // GIVEN: A temp SQLite database with only a 'users' table
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");
    drop(_conn);

    // Create a QueryServiceImpl and set database path
    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    // WHEN: We validate a query against a non-existent table
    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM nonexistent_table".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;

    // THEN: Validation should fail with table not found error
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "Query should be invalid");
    assert!(
        response.error_message.contains("nonexistent_table"),
        "Error should mention table name"
    );
}

/// Test 1B.2: QueryService accepts valid SELECT queries against existing tables
#[tokio::test]
async fn query_service_accepts_valid_select_query() {
    // GIVEN: A temp SQLite database with a users table
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");
    drop(_conn);

    // Create a QueryServiceImpl and set database path
    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    // WHEN: We validate a valid SELECT query against existing table
    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;

    // THEN: Validation should succeed
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.valid, "Valid query should pass validation");
    assert!(
        response.error_message.is_empty(),
        "No error for valid query"
    );
}

/// Test 1B.3: QueryService rejects DDL statements (CREATE)
#[tokio::test]
async fn query_service_rejects_ddl_create() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();
    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "CREATE TABLE hack (id INT)".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "DDL CREATE should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("DDL"),
        "Error should mention DDL"
    );
}

/// Test 1B.4: QueryService rejects DML statements (INSERT)
#[tokio::test]
async fn query_service_rejects_dml_insert() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();
    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "INSERT INTO users (name) VALUES ('hacker')".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "DML INSERT should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("DML"),
        "Error should mention DML"
    );
}

/// Test 1B.5: SchemaService returns schema for introspected database
#[tokio::test]
async fn schema_service_introspects_sqlite_tables() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create users table");
    _conn
        .execute(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER)",
            [],
        )
        .expect("create posts table");
    drop(_conn);

    let mut schema_service = SchemaServiceImpl::new();
    schema_service.set_db_path(db_path).expect("set db path");

    let request = GetSchemaRequest {
        db_identifier: "test".to_string(),
    };

    let result = schema_service.get_schema(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.schema_id.is_empty());
    assert_eq!(response.database_type, "SQLite");
}

/// Test 1B.6: SchemaService validates table existence
#[tokio::test]
async fn schema_service_retrieves_table_columns() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
            [],
        )
        .expect("create table");
    drop(_conn);

    let mut schema_service = SchemaServiceImpl::new();
    schema_service.set_db_path(db_path).expect("set db path");

    let request = GetTableRequest {
        db_identifier: "test".to_string(),
        table_name: "users".to_string(),
    };

    let result = schema_service.get_table(request).await;
    assert!(result.is_ok(), "Should retrieve existing table");
}

/// Test 1B.7: SchemaService rejects non-existent tables
#[tokio::test]
async fn schema_service_rejects_nonexistent_table() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut schema_service = SchemaServiceImpl::new();
    schema_service.set_db_path(db_path).expect("set db path");

    let request = GetTableRequest {
        db_identifier: "test".to_string(),
        table_name: "nonexistent".to_string(),
    };

    let result = schema_service.get_table(request).await;
    assert!(result.is_err(), "Should reject non-existent table");
}

/// Test 1B.8: QueryService supports parameterized queries
#[tokio::test]
async fn query_service_validates_parameterized_query() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let mut params = std::collections::HashMap::new();
    params.insert(
        "name".to_string(),
        oam::generated::QueryParameter {
            value: "Alice".to_string(),
            type_hint: "TEXT".to_string(),
        },
    );

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users WHERE name = ?".to_string(),
        parameters: params,
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.valid, "Parameterized query should validate");
}

/// Test 1B.9: QueryService rejects queries without FROM clause
#[tokio::test]
async fn query_service_rejects_query_without_from() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();
    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT 1".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "Query without FROM should be rejected");
    assert!(response.error_message.contains("FROM"));
}

/// Test 1B.10: QueryService validates against schema with foreign keys
#[tokio::test]
async fn query_service_validates_foreign_key_tables() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create users");
    _conn.execute(
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id))",
        [],
    )
    .expect("create posts");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM posts".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(response.valid, "Valid FK query should pass");
}

// ============================================================================
// PHASE 1C: P2SQL Security - Prompt Injection Defense
// ============================================================================

/// Test 1C.1: QueryService rejects queries with semicolons (command chaining)
#[tokio::test]
async fn query_service_rejects_command_chaining_semicolon() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "Query with semicolon should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("SEMICOLON")
            || response.error_message.contains(";"),
        "Error should mention semicolon or command chaining"
    );
}

/// Test 1C.2: QueryService rejects queries with SQL line comments (--)
#[tokio::test]
async fn query_service_rejects_line_comments() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users WHERE id = 1 -- ; DROP TABLE users".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(
        !response.valid,
        "Query with line comment should be rejected"
    );
    assert!(
        response.error_message.to_uppercase().contains("COMMENT")
            || response.error_message.contains("--"),
        "Error should mention comment syntax"
    );
}

/// Test 1C.3: QueryService rejects queries with block comments (/* */)
#[tokio::test]
async fn query_service_rejects_block_comments() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users /* injection */ WHERE id = 1".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(
        !response.valid,
        "Query with block comment should be rejected"
    );
    assert!(
        response.error_message.to_uppercase().contains("COMMENT")
            || response.error_message.contains("/*"),
        "Error should mention block comment syntax"
    );
}

/// Test 1C.4: QueryService rejects PRAGMA statements
#[tokio::test]
async fn query_service_rejects_pragma_statements() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "PRAGMA database_list".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "PRAGMA statement should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("PRAGMA"),
        "Error should mention PRAGMA"
    );
}

/// Test 1C.5: QueryService rejects EXPLAIN statements
#[tokio::test]
async fn query_service_rejects_explain_statements() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "EXPLAIN SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "EXPLAIN statement should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("EXPLAIN"),
        "Error should mention EXPLAIN"
    );
}

/// Test 1C.6: QueryService rejects boolean-based SQL injection (OR '1'='1)
#[tokio::test]
async fn query_service_rejects_boolean_injection() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users WHERE id = 1 OR '1'='1".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "Boolean injection should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("INJECTION")
            || response.error_message.to_uppercase().contains("SUSPICIOUS"),
        "Error should indicate injection detection"
    );
}

/// Test 1C.7: QueryService rejects UNION-based injection (UNION SELECT)
#[tokio::test]
async fn query_service_rejects_union_injection() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users UNION SELECT * FROM passwords".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "UNION injection should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("UNION")
            || response.error_message.to_uppercase().contains("INJECTION"),
        "Error should mention UNION or injection"
    );
}

/// Test 1C.8: QueryService rejects time-based blind injection (SLEEP)
#[tokio::test]
async fn query_service_rejects_sleep_injection() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users WHERE id = 1 AND SLEEP(5)".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "SLEEP injection should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("SLEEP")
            || response.error_message.to_uppercase().contains("INJECTION"),
        "Error should mention SLEEP or injection"
    );
}

/// Test 1C.9: QueryService rejects subquery injection in WHERE clause
#[tokio::test]
async fn query_service_rejects_subquery_injection() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users WHERE id = (SELECT password FROM admins)".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "Subquery injection should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("SUBQUERY")
            || response.error_message.to_uppercase().contains("INJECTION"),
        "Error should mention subquery or injection"
    );
}

/// Test 1C.10: QueryService rejects ATTACH/DETACH database attacks
#[tokio::test]
async fn query_service_rejects_attach_database() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", [])
        .expect("create table");
    drop(_conn);

    let mut validator = QueryServiceImpl::new();
    validator.set_db_path(db_path).expect("set db path");

    let request = ValidateQueryRequest {
        db_identifier: "test".to_string(),
        query: "ATTACH DATABASE '/tmp/evil.db' AS evil".to_string(),
        parameters: std::collections::HashMap::new(),
    };

    let result = validator.validate_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert!(!response.valid, "ATTACH DATABASE should be rejected");
    assert!(
        response.error_message.to_uppercase().contains("ATTACH")
            || response.error_message.to_uppercase().contains("DATABASE"),
        "Error should mention ATTACH or database manipulation"
    );
}
