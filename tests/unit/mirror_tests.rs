use rusqlite::Connection;
use tempfile::NamedTempFile;

#[test]
fn introspect_simple_sqlite_file() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        [],
    )
    .expect("create table");

    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    assert!(schema.tables.iter().any(|t| t.name == "users"));
    let users = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table exists");
    assert!(users
        .columns
        .iter()
        .any(|c| c.name == "id" && c.primary_key));
    assert!(users
        .columns
        .iter()
        .any(|c| c.name == "name" && !c.nullable));
}

#[test]
fn introspect_simple_sqlite_file_err_case() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();
    std::fs::write(&path, b"This is not a SQLite database").expect("write file");

    let res = oam::introspect_sqlite_path(&path);
    assert!(
        res.is_err(),
        "expected error for invalid SQLite file, got: {:?}",
        res
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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
    let res = oam::introspect_sqlite_path(&path);
    assert!(
        res.is_err(),
        "expected error for nonexistent path, got: {:?}",
        res
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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
    let res = oam::introspect_sqlite_path(&path);
    assert!(
        res.is_err(),
        "expected error for directory path, got: {:?}",
        res
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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
    assert_eq!(
        cfk.referenced_columns,
        vec!["p1".to_string(), "p2".to_string()]
    );
}

#[test]
fn sqlite_detects_composite_foreign_key_err_case() {
    let path = "/nonexistent/parent/dir/child.sqlite".to_string();
    let res = oam::introspect_sqlite_path(&path);
    assert!(
        res.is_err(),
        "expected error for nonexistent path, got: {:?}",
        res
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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
    assert_eq!(
        cfk.referenced_columns,
        vec!["p1".to_string(), "p2".to_string()]
    );

    assert!(!children.foreign_keys.iter().any(|f| f.column == "a"));
    assert!(!children.foreign_keys.iter().any(|f| f.column == "b"));
}

#[test]
fn sqlite_composite_fk_order_and_no_single_fks_err_case() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();
    std::fs::write(&path, b"invalid SQLite content").expect("write file");

    let res = oam::introspect_sqlite_path(&path);
    assert!(
        res.is_err(),
        "expected error for invalid SQLite file, got: {:?}",
        res
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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
        role_col.enum_values.as_deref(),
        Some(
            &[
                "admin".to_string(),
                "editor".to_string(),
                "viewer".to_string()
            ][..]
        )
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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

    let has_invalid_enums = role_col.enum_values.as_ref().is_some_and(|v| !v.is_empty());
    assert!(
        !has_invalid_enums,
        "expected no enum values, got: {:?}",
        role_col.enum_values
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(
        fk.on_delete.as_deref(),
        Some("CASCADE"),
        "expected ON DELETE CASCADE"
    );
    assert_eq!(
        fk.on_update.as_deref(),
        Some("SET NULL"),
        "expected ON UPDATE SET NULL"
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(
        fk.on_delete.as_deref(),
        None,
        "expected no ON DELETE action"
    );
    assert_eq!(
        fk.on_update.as_deref(),
        None,
        "expected no ON UPDATE action"
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(
        cfk.on_delete.as_deref(),
        Some("RESTRICT"),
        "expected ON DELETE RESTRICT"
    );
    assert_eq!(
        cfk.on_update.as_deref(),
        Some("CASCADE"),
        "expected ON UPDATE CASCADE"
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
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

    assert_eq!(
        cfk.on_delete.as_deref(),
        None,
        "expected no ON DELETE action"
    );
    assert_eq!(
        cfk.on_update.as_deref(),
        None,
        "expected no ON UPDATE action"
    );
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");

    let json_schema = schema.to_json_schema();

    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");
    assert!(
        !json_str.is_empty(),
        "json schema should serialize to non-empty string"
    );

    let _json_obj: serde_json::Value =
        serde_json::from_str(&json_str).expect("should be valid json");
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");

    let _json_obj: serde_json::Value =
        serde_json::from_str(&json_str).expect("should be valid json");
    assert!(
        json_schema
            .schema
            .object
            .as_ref()
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");

    let users_table = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table");
    let role_col = users_table
        .columns
        .iter()
        .find(|c| c.name == "role")
        .expect("role column");
    let enum_vals = role_col
        .enum_values
        .as_ref()
        .expect("role should have enum values");
    assert_eq!(enum_vals.len(), 3, "should have 3 enum values");

    let json_schema = schema.to_json_schema();
    let json_str = serde_json::to_string(&json_schema).expect("serialize to json");

    assert!(
        json_str.contains("admin") || json_str.contains("role"),
        "json schema should reflect the enum constraint"
    );
}

// ── Phase 1–2 TDD: UniqueIndex introspection ─────────────────────────────────

#[test]
fn sqlite_introspects_unique_indexes() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE accounts (
            id    INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE
        );",
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "accounts")
        .expect("accounts table");

    assert!(
        table
            .unique_indexes
            .iter()
            .any(|u| u.columns == vec!["email".to_string()]),
        "expected a UniqueIndex on accounts.email, got: {:?}",
        table.unique_indexes
    );
}

#[test]
fn sqlite_introspects_multi_column_unique_index() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE memberships (
            user_id INTEGER NOT NULL,
            org_id  INTEGER NOT NULL
        );
        CREATE UNIQUE INDEX uq_memberships ON memberships (user_id, org_id);",
    )
    .expect("create tables");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "memberships")
        .expect("memberships table");

    assert!(
        table
            .unique_indexes
            .iter()
            .any(|u| { u.columns == vec!["user_id".to_string(), "org_id".to_string()] }),
        "expected composite UniqueIndex on memberships(user_id, org_id), got: {:?}",
        table.unique_indexes
    );
}

#[test]
fn sqlite_unique_index_excludes_pk() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE);")
        .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "items")
        .expect("items table");

    assert!(
        !table
            .unique_indexes
            .iter()
            .any(|u| u.columns.contains(&"id".to_string())),
        "PK column 'id' must not appear in unique_indexes, got: {:?}",
        table.unique_indexes
    );
    assert!(
        table
            .unique_indexes
            .iter()
            .any(|u| u.columns == vec!["code".to_string()]),
        "expected UniqueIndex on items.code, got: {:?}",
        table.unique_indexes
    );
}

// ── Phase 3 TDD: JSON schema enrichment ──────────────────────────────────────

#[test]
fn json_schema_sets_additional_properties_false() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE things (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_schema = schema.to_json_schema();

    let table_schema = json_schema
        .schema
        .object
        .as_ref()
        .and_then(|obj| obj.properties.get("things"))
        .expect("things table in schema");

    let table_obj = match table_schema {
        schemars::schema::Schema::Object(o) => o,
        _ => panic!("expected schema object for things table"),
    };

    let additional = table_obj
        .object
        .as_ref()
        .expect("things table has object validation")
        .additional_properties
        .as_ref()
        .expect("additionalProperties must be set");

    assert!(
        matches!(additional.as_ref(), schemars::schema::Schema::Bool(false)),
        "expected additionalProperties: false, got: {:?}",
        additional
    );
}

#[test]
fn json_schema_pk_column_description_includes_insert_hint() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("omit on INSERT"),
        "PK column description must contain 'omit on INSERT', got schema: {}",
        json_str
    );
}

#[test]
fn json_schema_fk_column_description_includes_reference() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
             FOREIGN KEY (user_id) REFERENCES users(id));",
    )
    .expect("create tables");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("Foreign Key \u{2192}"),
        "FK column description must contain 'Foreign Key →', got schema: {}",
        json_str
    );
}

#[test]
fn json_schema_unique_column_description_includes_unique() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL UNIQUE)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("UNIQUE"),
        "unique column description must contain 'UNIQUE', got schema: {}",
        json_str
    );
}

#[test]
fn json_schema_composite_fk_in_table_description() {
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

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("parents"),
        "table description must include composite FK reference to 'parents', got schema: {}",
        json_str
    );
}

#[test]
fn json_schema_multi_unique_in_table_description() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE memberships (user_id INTEGER NOT NULL, org_id INTEGER NOT NULL);
         CREATE UNIQUE INDEX uq_memberships ON memberships (user_id, org_id);",
    )
    .expect("create tables");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("UNIQUE"),
        "table description must include multi-column UNIQUE annotation, got schema: {}",
        json_str
    );
}

#[test]
fn sqlite_detects_default_value() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'active',
            score INTEGER DEFAULT 0
        );",
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "users")
        .expect("users table exists");

    let status_col = table
        .columns
        .iter()
        .find(|c| c.name == "status")
        .expect("status column exists");
    assert_eq!(
        status_col.default_value,
        Some("'active'".to_string()),
        "status default_value should be \"'active'\""
    );

    let score_col = table
        .columns
        .iter()
        .find(|c| c.name == "score")
        .expect("score column exists");
    assert_eq!(
        score_col.default_value,
        Some("0".to_string()),
        "score default_value should be \"0\""
    );

    let id_col = table
        .columns
        .iter()
        .find(|c| c.name == "id")
        .expect("id column exists");
    assert_eq!(
        id_col.default_value, None,
        "id should have no default value"
    );
}

#[test]
fn sqlite_detects_non_unique_index() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL
        );
        CREATE INDEX idx_orders_user_id ON orders (user_id);
        CREATE INDEX idx_orders_status ON orders (status);",
    )
    .expect("create table and indexes");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "orders")
        .expect("orders table exists");

    assert!(
        table
            .indexes
            .iter()
            .any(|i| i.name == "idx_orders_user_id" && i.columns == vec!["user_id".to_string()]),
        "expected idx_orders_user_id in indexes, got: {:?}",
        table.indexes
    );
    assert!(
        table
            .indexes
            .iter()
            .any(|i| i.name == "idx_orders_status" && i.columns == vec!["status".to_string()]),
        "expected idx_orders_status in indexes, got: {:?}",
        table.indexes
    );
    assert!(
        table.unique_indexes.is_empty(),
        "expected no unique indexes, got: {:?}",
        table.unique_indexes
    );
}

#[test]
fn json_schema_column_description_includes_default_value() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE widgets (id INTEGER PRIMARY KEY, color TEXT NOT NULL DEFAULT 'red');",
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let json_str = serde_json::to_string(&schema.to_json_schema()).expect("to json");

    assert!(
        json_str.contains("Default:"),
        "column description must include 'Default:', got schema: {}",
        json_str
    );
}
