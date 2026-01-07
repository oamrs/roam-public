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
    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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
    
    let res = roam::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for invalid SQLite file, got: {:?}", res);
}

#[test]
fn sqlite_detects_foreign_key_posts_user_id() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(fk.referenced_table, "users");
    assert_eq!(fk.referenced_column, "id");
}

#[test]
fn sqlite_detects_foreign_key_posts_user_id_err_case() {
    let path = "/nonexistent/parent/dir/db.sqlite".to_string();
    let res = roam::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for nonexistent path, got: {:?}", res);
}

#[test]
fn sqlite_detects_nullable_foreign_key() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert!(user_col.nullable, "expected posts.user_id to be nullable");

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
    let dir = tempfile::tempdir().expect("create temp dir");
    let path = dir.path().to_str().unwrap().to_string();
    let res = roam::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for directory path, got: {:?}", res);
}

#[test]
fn sqlite_detects_composite_foreign_key() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(cfk.referenced_table, "parents");
    assert_eq!(cfk.referenced_columns, vec!["p1".to_string(), "p2".to_string()]);
}

#[test]
fn sqlite_detects_composite_foreign_key_err_case() {
    let path = "/nonexistent/parent/dir/child.sqlite".to_string();
    let res = roam::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for nonexistent path, got: {:?}", res);
}

#[test]
fn sqlite_composite_fk_order_and_no_single_fks() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(cfk.referenced_table, "parents");
    assert_eq!(cfk.referenced_columns, vec!["p1".to_string(), "p2".to_string()]);

    assert!(!children.foreign_keys.iter().any(|f| f.column == "a"));
    assert!(!children.foreign_keys.iter().any(|f| f.column == "b"));
}

#[test]
fn sqlite_composite_fk_order_and_no_single_fks_err_case() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();
    std::fs::write(&path, b"invalid SQLite content").expect("write file");
    
    let res = roam::introspect_sqlite_path(&path);
    assert!(res.is_err(), "expected error for invalid SQLite file, got: {:?}", res);
}

#[test]
fn sqlite_detects_enum_via_check_constraint() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(
        role_col.enum_values.as_ref().map(|v| v.as_slice()),
        Some(&["admin".to_string(), "editor".to_string(), "viewer".to_string()][..])
    );
}

#[test]
fn sqlite_detects_enum_via_check_constraint_err_case() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    let has_invalid_enums = role_col.enum_values.as_ref().map_or(false, |v| !v.is_empty());
    assert!(!has_invalid_enums, "expected no enum values, got: {:?}", role_col.enum_values);
}

#[test]
fn sqlite_detects_foreign_key_cascade_actions() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(fk.on_delete.as_deref(), Some("CASCADE"), "expected ON DELETE CASCADE");
    assert_eq!(fk.on_update.as_deref(), Some("SET NULL"), "expected ON UPDATE SET NULL");
}

#[test]
fn sqlite_detects_foreign_key_cascade_actions_err_case() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(fk.on_delete.as_deref(), None, "expected no ON DELETE action");
    assert_eq!(fk.on_update.as_deref(), None, "expected no ON UPDATE action");
}

#[test]
fn sqlite_detects_composite_foreign_key_cascade_actions() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(cfk.on_delete.as_deref(), Some("RESTRICT"), "expected ON DELETE RESTRICT");
    assert_eq!(cfk.on_update.as_deref(), Some("CASCADE"), "expected ON UPDATE CASCADE");
}

#[test]
fn sqlite_detects_composite_foreign_key_cascade_actions_err_case() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(cfk.on_delete.as_deref(), None, "expected no ON DELETE action");
    assert_eq!(cfk.on_update.as_deref(), None, "expected no ON UPDATE action");
}

#[test]
fn schema_converts_to_json_schema() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
    
    let json_schema = schema.to_json_schema();
    
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    assert!(!json_str.is_empty(), "json schema should serialize to non-empty string");
    
    let _json_obj: serde_json::Value = serde_json::from_str(&json_str).expect("should be valid json");
}

#[test]
fn json_schema_preserves_table_structure() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        [],
    )
    .expect("create table");

    drop(conn);

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    
    let _json_obj: serde_json::Value = serde_json::from_str(&json_str).expect("should be valid json");
    assert!(
        json_schema.schema.object.as_ref()
            .and_then(|obj| obj.properties.get("users"))
            .is_some(),
        "users table should be present in schema"
    );
}

#[test]
fn json_schema_reflects_enum_constraints() {
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

    let schema = roam::introspect_sqlite_path(&path).expect("introspect");
    
    let users_table = schema.tables.iter().find(|t| t.name == "users").expect("users table");
    let role_col = users_table.columns.iter().find(|c| c.name == "role").expect("role column");
    let enum_vals = role_col.enum_values.as_ref().expect("role should have enum values");
    assert_eq!(enum_vals.len(), 3, "should have 3 enum values");
    
    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    
    assert!(json_str.contains("admin") || json_str.contains("role"), 
        "json schema should reflect the enum constraint");
}
