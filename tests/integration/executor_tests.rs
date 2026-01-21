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

// ============================================================================
// PHASE 1D: Query Execution Tests (Integration - With Database)
// ============================================================================

use oam::generated::ExecuteQueryRequest;

/// Test 1D.6: QueryService executes valid SELECT query and returns rows
#[tokio::test]
async fn query_service_executes_select_and_returns_row_count() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");
    _conn
        .execute("INSERT INTO users (name) VALUES ('Alice')", [])
        .expect("insert row 1");
    _conn
        .execute("INSERT INTO users (name) VALUES ('Bob')", [])
        .expect("insert row 2");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let request = ExecuteQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response.status,
        oam::generated::QueryStatus::Success as i32,
        "Query should succeed"
    );
    assert_eq!(response.row_count, 2, "Should return 2 rows");
    assert!(
        response.execution_ms >= 0,
        "execution_ms should be recorded"
    );
}

/// Test 1D.7: QueryService respects row limit in result set
#[tokio::test]
async fn query_service_respects_limit_parameter() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");

    // Insert 5 rows
    for i in 0..5 {
        _conn
            .execute(
                "INSERT INTO users (name) VALUES (?1)",
                [&format!("User{}", i)],
            )
            .expect("insert row");
    }
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let request = ExecuteQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 3, // Limit to 3 rows
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(response.status, oam::generated::QueryStatus::Success as i32);
    assert_eq!(
        response.row_count, 3,
        "Should return exactly 3 rows due to limit"
    );
}

/// Test 1D.8: QueryService rejects invalid queries with ValidationError
#[tokio::test]
async fn query_service_rejects_invalid_query_with_validation_error() {
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

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    // Query with command chaining
    let request = ExecuteQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users; DROP TABLE users;".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(
        response.status,
        oam::generated::QueryStatus::ValidationError as i32,
        "Should fail validation"
    );
}

/// Test 1D.9: QueryService returns ExecutionError for non-existent table
#[tokio::test]
async fn query_service_returns_execution_error_for_nonexistent_table() {
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

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    // Query against non-existent table
    let request = ExecuteQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM nonexistent".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    // Table validation happens before execution, so should be ValidationError
    assert_eq!(
        response.status,
        oam::generated::QueryStatus::ValidationError as i32,
        "Should return ValidationError because table doesn't exist"
    );
}

/// Test 1D.10: QueryService executes parameterized query with substitution
#[tokio::test]
async fn query_service_executes_parameterized_query() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )
        .expect("create table");
    _conn
        .execute("INSERT INTO users (name) VALUES ('Alice')", [])
        .expect("insert row");
    _conn
        .execute("INSERT INTO users (name) VALUES ('Bob')", [])
        .expect("insert row");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let mut params = std::collections::HashMap::new();
    params.insert(
        "name".to_string(),
        oam::generated::QueryParameter {
            value: "Alice".to_string(),
            type_hint: "TEXT".to_string(),
        },
    );

    // Phase 1D: Parameters are accepted but not yet bound (that's Phase 1E)
    // This test verifies the query executes; parameter substitution will be tested in Phase 1E
    let request = ExecuteQueryRequest {
        db_identifier: "test".to_string(),
        query: "SELECT * FROM users".to_string(), // Simple query without WHERE for Phase 1D
        parameters: params,
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();

    assert_eq!(response.status, oam::generated::QueryStatus::Success as i32);
    assert_eq!(
        response.row_count, 2,
        "Should return all users (Phase 1D doesn't bind parameters yet)"
    );
}

// ============================================================================
// PHASE 1E: EVENT INTEGRATION TESTS (With Database)
// ============================================================================

/// Test 1E.1: QueryService dispatches QueryExecuted event when query succeeds with database
#[tokio::test]
async fn query_service_dispatches_query_executed_event_on_db_success() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    // Create a test database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .expect("create table");
    _conn
        .execute("INSERT INTO test_table (value) VALUES ('row1')", [])
        .expect("insert row");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let test_db_id = "ie1e1_test_db".to_string();
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.clone(),
        query: "SELECT * FROM test_table".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(response.status, oam::generated::QueryStatus::Success as i32);

    // Check that QueryExecuted event was dispatched for THIS test
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecuted { db_identifier, .. } = e {
                db_identifier == &test_db_id
            } else {
                false
            }
        })
        .collect();

    assert!(
        !test_events.is_empty(),
        "Should have dispatched QueryExecuted event on success"
    );

    if let Event::QueryExecuted {
        db_identifier,
        query: _,
        status,
        row_count,
        execution_ms: _,
        timestamp: _,
    } = test_events[0]
    {
        assert_eq!(db_identifier, &test_db_id);
        assert_eq!(status, "Success");
        assert_eq!(row_count, &1i32);
    } else {
        panic!("Expected QueryExecuted event");
    }
}

/// Test 1E.2: QueryService dispatches QueryValidationFailed event when validation fails with database
#[tokio::test]
async fn query_service_dispatches_query_validation_failed_event_on_db_validation_failure() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    // Clear previous events
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let test_db_id = "ie1e2_validation_test";

    // Create a test database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .expect("create table");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    // Query against non-existent table
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.to_string(),
        query: "SELECT * FROM nonexistent_table".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(
        response.status,
        oam::generated::QueryStatus::ValidationError as i32
    );

    // Check that QueryValidationFailed event was dispatched
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryValidationFailed { db_identifier, .. } = e {
                db_identifier == test_db_id
            } else {
                false
            }
        })
        .collect();
    assert!(
        !test_events.is_empty(),
        "Should have dispatched QueryValidationFailed event on validation error"
    );

    let event = test_events[0];
    match event {
        Event::QueryValidationFailed {
            db_identifier,
            query: _,
            error_reason,
            timestamp: _,
        } => {
            assert_eq!(db_identifier, test_db_id);
            assert!(!error_reason.is_empty());
        }
        _ => panic!("Expected QueryValidationFailed event, got {:?}", event),
    }
}

/// Test 1E.3: QueryService dispatches QueryExecutionError event when execution fails
#[tokio::test]
async fn query_service_dispatches_query_execution_error_event_on_db_execution_failure() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    // Clear previous events
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let test_db_id = "ie1e3_execution_error_test";

    // Create a test database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .expect("create table");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    // Query with malformed SQL (after validation passes)
    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.to_string(),
        query: "SELECT * FROM test_table WHERE INVALID_COLUMN = 'test'".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let result = executor.execute_query(request).await;
    assert!(result.is_ok());
    let response = result.unwrap();
    assert_eq!(
        response.status,
        oam::generated::QueryStatus::ExecutionError as i32
    );

    // Check that QueryExecutionError event was dispatched
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecutionError { db_identifier, .. } = e {
                db_identifier == test_db_id
            } else {
                false
            }
        })
        .collect();
    assert!(
        !test_events.is_empty(),
        "Should have dispatched QueryExecutionError event on execution error"
    );

    let event = test_events[0];
    match event {
        Event::QueryExecutionError {
            db_identifier,
            query: _,
            error_message,
            timestamp: _,
        } => {
            assert_eq!(db_identifier, test_db_id);
            assert!(!error_message.is_empty());
        }
        _ => panic!("Expected QueryExecutionError event, got {:?}", event),
    }
}

/// Test 1E.4: Dispatched events include complete execution metadata
#[tokio::test]
async fn query_service_events_include_complete_execution_metadata() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    // Clear previous events
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let test_db_id = "ie1e4_metadata_test";

    // Create a test database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .expect("create table");
    _conn
        .execute("INSERT INTO test_table (value) VALUES ('test1')", [])
        .expect("insert row");
    _conn
        .execute("INSERT INTO test_table (value) VALUES ('test2')", [])
        .expect("insert row");
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.to_string(),
        query: "SELECT * FROM test_table".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let _result = executor.execute_query(request).await;

    // Get dispatched events
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecuted { db_identifier, .. } = e {
                db_identifier == test_db_id
            } else {
                false
            }
        })
        .collect();
    assert!(!test_events.is_empty());

    let event = test_events[0];
    match event {
        Event::QueryExecuted {
            db_identifier,
            query,
            status,
            row_count,
            execution_ms,
            timestamp,
        } => {
            assert_eq!(db_identifier, test_db_id);
            assert_eq!(query, "SELECT * FROM test_table");
            assert_eq!(status, "Success");
            assert_eq!(row_count, &2i32);
            assert!(execution_ms > &0, "execution_ms should be positive");
            assert!(!timestamp.is_empty(), "timestamp should not be empty");
        }
        _ => panic!("Expected QueryExecuted event with full metadata"),
    }
}

/// Test 1E.5: Events track row count from query execution
#[tokio::test]
async fn query_service_events_track_row_count() {
    use oam::interceptor::get_event_bus;
    use oam::Event;

    // Clear previous events
    let event_bus = get_event_bus();
    let _ = event_bus.clear();

    let test_db_id = "ie1e5_row_count_test";

    // Create a test database with multiple rows
    let tmp = NamedTempFile::new().expect("create tmp file");
    let db_path = tmp.path().to_str().unwrap();

    let _conn = rusqlite::Connection::open(db_path).expect("open db");
    _conn
        .execute(
            "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .expect("create table");

    for i in 0..5 {
        _conn
            .execute(
                "INSERT INTO test_table (value) VALUES (?1)",
                [&format!("row_{}", i)],
            )
            .expect("insert row");
    }
    drop(_conn);

    let mut executor = QueryServiceImpl::new();
    executor.set_db_path(db_path).expect("set db path");

    let request = ExecuteQueryRequest {
        db_identifier: test_db_id.to_string(),
        query: "SELECT * FROM test_table".to_string(),
        parameters: std::collections::HashMap::new(),
        limit: 100,
        timeout_seconds: 30,
    };

    let _result = executor.execute_query(request).await;

    // Check row_count in event
    let events = event_bus.all_events().expect("get generic events");
    let test_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if let Event::QueryExecuted { db_identifier, .. } = e {
                db_identifier == test_db_id
            } else {
                false
            }
        })
        .collect();
    assert!(!test_events.is_empty());
    let event = test_events[0];

    match event {
        Event::QueryExecuted { row_count, .. } => {
            assert_eq!(row_count, &5i32, "Event should track 5 rows returned");
        }
        _ => panic!("Expected QueryExecuted event"),
    }
}
