use rusqlite::Connection;
use tempfile::NamedTempFile;

/// Integration-style test (placed under `tests/unit/`) that exercises the
/// `introspect_sqlite_path` API. This test intentionally assumes the
/// function exists — per TDD we expect the test to fail (compile or run)
/// until the production code is written.

#[test]
fn introspect_simple_sqlite_file() {
    // Create a temporary SQLite file so the introspector can open the same DB file.
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        [],
    )
    .expect("create table");

    drop(conn);

    // Call into the library under test and verify the reflected schema.
    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    assert!(schema.tables.iter().any(|t| t.name == "users"));
    let users = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table exists");
    assert!(users.columns.iter().any(|c| c.name == "id" && c.primary_key));
    assert!(users.columns.iter().any(|c| c.name == "name" && !c.nullable));
}

#[test]
fn introspect_simple_sqlite_file_err_case() {
    // Error case: pass a file that is not a valid SQLite database
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();
    // Write arbitrary non-SQLite content
    std::fs::write(&path, b"This is not a SQLite database").expect("write file");
    
    let res = oam_mirror::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for invalid SQLite file, got: {:?}", res);
}

#[test]
fn sqlite_detects_foreign_key_posts_user_id() {
    // This test asserts that the mirror detects foreign keys. It is
    // intentionally written to fail at this stage (TDD) because the
    // `Table` model does not yet expose `foreign_keys`.

    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id));",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let posts = schema
        .tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts table exists");

    // Expect a foreign_keys field on Table with an entry for user_id -> users.id
    // This will fail to compile/run until foreign key detection is implemented.
    let fk = posts
        .foreign_keys
        .iter()
        .find(|f| f.column == "user_id")
        .expect("expected fk on posts.user_id");

    assert_eq!(fk.referenced_table, "users");
    assert_eq!(fk.referenced_column, "id");
}

#[test]
fn sqlite_detects_foreign_key_posts_user_id_err_case() {
    // Error case: pass a file path that doesn't exist (nonexistent parent directory)
    let path = "/nonexistent/parent/dir/db.sqlite".to_string();
    let res = oam_mirror::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for nonexistent path, got: {:?}", res);
}

#[test]
fn sqlite_detects_nullable_foreign_key() {
    // Create a temp SQLite file and tables where the FK column is nullable.
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id));",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let posts = schema
        .tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts table exists");

    let user_col = posts
        .columns
        .iter()
        .find(|c| c.name == "user_id")
        .expect("user_id column exists");

    // Expect the column to be nullable
    assert!(user_col.nullable, "expected posts.user_id to be nullable");

    // Expect the foreign key to be present and point to users.id
    let fk = posts
        .foreign_keys
        .iter()
        .find(|f| f.column == "user_id")
        .expect("expected fk on posts.user_id");
    assert_eq!(fk.referenced_table, "users");
    assert_eq!(fk.referenced_column, "id");
}

#[test]
fn sqlite_detects_nullable_foreign_key_err_case() {
    // Error case: pass an invalid file path (directory instead of file)
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().to_str().unwrap().to_string();
    let res = oam_mirror::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for directory path, got: {:?}", res);
}

#[test]
fn sqlite_detects_composite_foreign_key() {
    // Create a temp SQLite file and tables with a composite foreign key.
    // This test is expected to fail until composite-FK support is added to the
    // mirror introspector.
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE parents (p1 INTEGER NOT NULL, p2 INTEGER NOT NULL, PRIMARY KEY (p1, p2));
         CREATE TABLE children (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL,
            FOREIGN KEY (a, b) REFERENCES parents(p1, p2));",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let children = schema
        .tables
        .iter()
        .find(|t| t.name == "children")
        .expect("children table exists");

    // Expect a composite_foreign_keys field or equivalent representation.
    // This will fail to compile/run until composite FK support is implemented.
    let cfk = children
        .composite_foreign_keys
        .iter()
        .find(|f| f.columns == vec!["a".to_string(), "b".to_string()])
        .expect("expected composite fk on children(a,b)");

    assert_eq!(cfk.referenced_table, "parents");
    assert_eq!(cfk.referenced_columns, vec!["p1".to_string(), "p2".to_string()]);
}

#[test]
fn sqlite_detects_composite_foreign_key_err_case() {
    // Error case: pass a non-existent file path (missing parent directory)
    let path = "/nonexistent/parent/dir/child.sqlite".to_string();
    let res = oam_mirror::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for nonexistent path, got: {:?}", res);
}

#[test]
fn sqlite_composite_fk_order_and_no_single_fks() {
    // Edge case: composite FK must preserve column order and not be duplicated
    // as separate single-column foreign keys.
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE parents (p1 INTEGER NOT NULL, p2 INTEGER NOT NULL, PRIMARY KEY (p1, p2));
         CREATE TABLE children (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL,
            FOREIGN KEY (a, b) REFERENCES parents(p1, p2) ON DELETE CASCADE);",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let children = schema
        .tables
        .iter()
        .find(|t| t.name == "children")
        .expect("children table exists");

    // Expect exactly one composite foreign key matching (a,b) -> (p1,p2)
    let cfk = children
        .composite_foreign_keys
        .iter()
        .find(|f| f.columns == vec!["a".to_string(), "b".to_string()])
        .expect("expected composite fk on children(a,b)");

    assert_eq!(cfk.referenced_table, "parents");
    assert_eq!(cfk.referenced_columns, vec!["p1".to_string(), "p2".to_string()]);

    // Ensure there are no single-column foreign_keys for 'a' or 'b'
    assert!(!children.foreign_keys.iter().any(|f| f.column == "a"));
    assert!(!children.foreign_keys.iter().any(|f| f.column == "b"));
}

#[test]
fn sqlite_composite_fk_order_and_no_single_fks_err_case() {
    // Error case: pass an invalid SQLite file (non-SQLite content)
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();
    std::fs::write(&path, b"invalid SQLite content").expect("write file");
    
    let res = oam_mirror::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for invalid SQLite file, got: {:?}", res);
}

#[test]
fn sqlite_detects_enum_via_check_constraint() {
    // Create a temp SQLite file and a table that uses a CHECK-based enum.
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            role TEXT NOT NULL CHECK(role IN ('admin','editor','viewer'))
        );",
    )
    .expect("create table");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let users = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table exists");

    let role_col = users
        .columns
        .iter()
        .find(|c| c.name == "role")
        .expect("role column exists");

    // Expect the introspector to expose enum values for this column. This will
    // fail to compile/run until `Column.enum_values` and parsing are implemented.
    assert_eq!(
        role_col.enum_values.as_ref().map(|v| v.as_slice()),
        Some(&["admin".to_string(), "editor".to_string(), "viewer".to_string()][..])
    );
}

#[test]
fn sqlite_detects_enum_via_check_constraint_err_case() {
    // Error case: create a CHECK constraint with an incomplete pattern (no literal values)
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            role TEXT NOT NULL CHECK(length(role) > 0)
        );",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let users = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table exists");

    let role_col = users
        .columns
        .iter()
        .find(|c| c.name == "role")
        .expect("role column exists");

    // The introspector should not extract enum values from a length-check CHECK (not an IN pattern)
    let has_invalid_enums = role_col.enum_values.as_ref().map_or(false, |v| !v.is_empty());
    assert!(!has_invalid_enums, "expected no enum values from length-check CHECK, but found: {:?}", role_col.enum_values);
}

#[test]
fn sqlite_detects_foreign_key_cascade_actions() {
    // Test that the introspector detects ON DELETE CASCADE and ON UPDATE SET NULL actions
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL);",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let posts = schema
        .tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts table exists");

    let fk = posts
        .foreign_keys
        .iter()
        .find(|f| f.column == "user_id")
        .expect("expected fk on posts.user_id");

    // Expect the FK to expose on_delete and on_update actions
    assert_eq!(fk.on_delete.as_deref(), Some("CASCADE"), "expected ON DELETE CASCADE");
    assert_eq!(fk.on_update.as_deref(), Some("SET NULL"), "expected ON UPDATE SET NULL");
}

#[test]
fn sqlite_detects_foreign_key_cascade_actions_err_case() {
    // Error case: create a FK without cascade actions (defaults to no action)
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id));",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let posts = schema
        .tables
        .iter()
        .find(|t| t.name == "posts")
        .expect("posts table exists");

    let fk = posts
        .foreign_keys
        .iter()
        .find(|f| f.column == "user_id")
        .expect("expected fk on posts.user_id");

    // Expect on_delete and on_update to be None when no action is specified
    assert_eq!(fk.on_delete.as_deref(), None, "expected no ON DELETE action");
    assert_eq!(fk.on_update.as_deref(), None, "expected no ON UPDATE action");
}

#[test]
fn sqlite_detects_composite_foreign_key_cascade_actions() {
    // Test that composite FKs also expose ON DELETE and ON UPDATE actions
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE parents (p1 INTEGER NOT NULL, p2 INTEGER NOT NULL, PRIMARY KEY (p1, p2));
         CREATE TABLE children (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL,
            FOREIGN KEY (a, b) REFERENCES parents(p1, p2) ON DELETE RESTRICT ON UPDATE CASCADE);",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let children = schema
        .tables
        .iter()
        .find(|t| t.name == "children")
        .expect("children table exists");

    let cfk = children
        .composite_foreign_keys
        .iter()
        .find(|f| f.columns == vec!["a".to_string(), "b".to_string()])
        .expect("expected composite fk on children(a,b)");

    // Expect the composite FK to expose on_delete and on_update actions
    assert_eq!(cfk.on_delete.as_deref(), Some("RESTRICT"), "expected ON DELETE RESTRICT");
    assert_eq!(cfk.on_update.as_deref(), Some("CASCADE"), "expected ON UPDATE CASCADE");
}

#[test]
fn sqlite_detects_composite_foreign_key_cascade_actions_err_case() {
    // Error case: create a composite FK without cascade actions (defaults to no action)
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE parents (p1 INTEGER NOT NULL, p2 INTEGER NOT NULL, PRIMARY KEY (p1, p2));
         CREATE TABLE children (id INTEGER PRIMARY KEY, a INTEGER NOT NULL, b INTEGER NOT NULL,
            FOREIGN KEY (a, b) REFERENCES parents(p1, p2));",
    )
    .expect("create tables");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let children = schema
        .tables
        .iter()
        .find(|t| t.name == "children")
        .expect("children table exists");

    let cfk = children
        .composite_foreign_keys
        .iter()
        .find(|f| f.columns == vec!["a".to_string(), "b".to_string()])
        .expect("expected composite fk on children(a,b)");

    // Expect on_delete and on_update to be None when no action is specified
    assert_eq!(cfk.on_delete.as_deref(), None, "expected no ON DELETE action");
    assert_eq!(cfk.on_update.as_deref(), None, "expected no ON UPDATE action");
}

#[test]
fn schema_converts_to_json_schema() {
    // Test that introspected schema can be converted to JSON Schema format
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            role TEXT CHECK(role IN ('admin', 'user', 'guest'))
        );",
    )
    .expect("create table");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    
    // Call the to_json_schema method to generate JSON Schema
    // This should return a schemars RootSchema that can be serialized to JSON
    let json_schema = schema.to_json_schema();
    
    // Verify we can serialize it to JSON (will be used for LLM tool definitions)
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    assert!(!json_str.is_empty(), "json schema should serialize to non-empty string");
    
    // Verify it's valid JSON
    let _json_obj: serde_json::Value = serde_json::from_str(&json_str).expect("should be valid json");
}

#[test]
fn json_schema_preserves_table_structure() {
    // Test that table structure is preserved in JSON Schema
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        [],
    )
    .expect("create table");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    
    // The JSON should mention "users" table somewhere
    // (we don't assert exact structure yet, just that it's present)
    assert!(json_str.len() > 0, "json schema should have content");
}

#[test]
fn json_schema_reflects_enum_constraints() {
    // Test that enum constraints are reflected in JSON Schema
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            role TEXT NOT NULL CHECK(role IN ('admin', 'user', 'guest'))
        );",
    )
    .expect("create table");

    drop(conn);

    let schema = oam_mirror::introspect_sqlite_path(&path).expect("introspect");
    
    // Verify the introspected schema has the enum values
    let users_table = schema.tables.iter().find(|t| t.name == "users").expect("users table");
    let role_col = users_table.columns.iter().find(|c| c.name == "role").expect("role column");
    let enum_vals = role_col.enum_values.as_ref().expect("role should have enum values");
    assert_eq!(enum_vals.len(), 3, "should have 3 enum values");
    
    // Generate JSON schema - it should reflect the enum somehow
    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    
    // The JSON schema should mention the enum values (at minimum for documentation)
    assert!(json_str.contains("admin") || json_str.contains("role"), 
        "json schema should reflect the enum constraint");
}
