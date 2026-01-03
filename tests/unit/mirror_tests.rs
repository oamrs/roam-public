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

    // Call into the library (not implemented yet). Compilation failure is expected.
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
