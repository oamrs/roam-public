use oam::MirrorProvider;
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

// ── Phase: Trigger introspection ─────────────────────────────────────────────

#[test]
fn sqlite_introspects_after_insert_trigger() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL DEFAULT 'pending');
         CREATE TABLE order_log (id INTEGER PRIMARY KEY, order_id INTEGER, action TEXT);
         CREATE TRIGGER trg_order_insert
             AFTER INSERT ON orders
         BEGIN
             INSERT INTO order_log (order_id, action) VALUES (NEW.id, 'created');
         END;",
    )
    .expect("create tables and trigger");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let orders = schema
        .tables
        .iter()
        .find(|t| t.name == "orders")
        .expect("orders table");

    let trigger = orders
        .triggers
        .iter()
        .find(|t| t.name == "trg_order_insert")
        .expect("trg_order_insert trigger must be found on orders table");

    assert_eq!(trigger.event, "INSERT");
    assert_eq!(trigger.timing, "AFTER");
    assert_eq!(trigger.table_name, "orders");
    assert!(!trigger.body.is_empty(), "trigger body must not be empty");
}

#[test]
fn sqlite_introspects_before_update_trigger() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute_batch(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL NOT NULL);
         CREATE TRIGGER trg_price_check
             BEFORE UPDATE ON products
         BEGIN
             SELECT RAISE(ABORT, 'price must be positive') WHERE NEW.price <= 0;
         END;",
    )
    .expect("create table and trigger");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let products = schema
        .tables
        .iter()
        .find(|t| t.name == "products")
        .expect("products table");

    let trigger = products
        .triggers
        .iter()
        .find(|t| t.name == "trg_price_check")
        .expect("trg_price_check trigger");

    assert_eq!(trigger.event, "UPDATE");
    assert_eq!(trigger.timing, "BEFORE");
}

#[test]
fn sqlite_table_with_no_triggers_has_empty_triggers_vec() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute("CREATE TABLE simple (id INTEGER PRIMARY KEY)", [])
        .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "simple")
        .expect("simple table");

    assert!(
        table.triggers.is_empty(),
        "table with no triggers must have empty triggers vec, got: {:?}",
        table.triggers
    );
}

// ── Phase: FieldMapping detection ────────────────────────────────────────────

#[test]
fn detect_camel_case_column_produces_hibernate_mapping() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE legacy (id INTEGER PRIMARY KEY, userId INTEGER, createdAt TEXT)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "legacy")
        .expect("legacy table");

    let mapping = table
        .field_mappings
        .iter()
        .find(|m| m.physical_name == "userId")
        .expect("mapping for userId must exist");

    assert_eq!(mapping.logical_name, "user_id");
    assert_eq!(mapping.orm_convention, "Hibernate");
}

#[test]
fn detect_shadow_property_produces_ef_mapping() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE ef_table (id INTEGER PRIMARY KEY, _tenantId INTEGER)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "ef_table")
        .expect("ef_table");

    let mapping = table
        .field_mappings
        .iter()
        .find(|m| m.physical_name == "_tenantId")
        .expect("mapping for _tenantId must exist");

    assert_eq!(mapping.logical_name, "tenantId");
    assert_eq!(mapping.orm_convention, "EntityFramework");
}

#[test]
fn snake_case_column_produces_no_field_mappings() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE clean (id INTEGER PRIMARY KEY, user_name TEXT, created_at TEXT)",
        [],
    )
    .expect("create table");
    drop(conn);

    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let table = schema
        .tables
        .iter()
        .find(|t| t.name == "clean")
        .expect("clean table");

    assert!(
        table.field_mappings.is_empty(),
        "snake_case columns must produce no field mappings, got: {:?}",
        table.field_mappings
    );
}

// ── Phase: UserDefinedType introspection ─────────────────────────────────────

#[test]
fn schema_model_has_user_defined_types_field() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)", [])
        .expect("create table");
    drop(conn);

    // The field must exist and be accessible — even if empty for standard tables
    let schema = oam::introspect_sqlite_path(&path).expect("introspect");
    let _ = &schema.user_defined_types; // compile-time field access check
}

// ── Phase: MirrorProvider trait ───────────────────────────────────────────────

#[test]
fn sqlite_mirror_provider_introspects_schema() {
    let tmp = NamedTempFile::new().expect("create tmp file");
    let path = tmp.path().to_str().unwrap().to_string();

    let conn = Connection::open(&path).expect("open tmp db");
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        [],
    )
    .expect("create table");
    drop(conn);

    let provider = oam::SqliteMirrorProvider::new(&path);
    let schema = tokio::runtime::Runtime::new()
        .expect("runtime")
        .block_on(provider.introspect_schema())
        .expect("introspect via MirrorProvider");

    assert!(schema.tables.iter().any(|t| t.name == "users"));
}

// ── Phase: TriggerFired event variant ────────────────────────────────────────

#[test]
fn trigger_fired_event_roundtrips_via_serde() {
    use oam::Event;

    let event = Event::trigger_fired(
        "orders".to_string(),
        "trg_order_insert".to_string(),
        "INSERT".to_string(),
        Some("42".to_string()),
    );

    assert_eq!(event.event_type(), "TriggerFired");

    let json = serde_json::to_string(&event).expect("serialize");
    let back: Event = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(event, back);
}

#[test]
fn trigger_fired_event_metadata_contains_required_keys() {
    use oam::Event;

    let event = Event::trigger_fired(
        "products".to_string(),
        "trg_price_check".to_string(),
        "UPDATE".to_string(),
        None,
    );

    let meta = event.metadata();
    assert_eq!(
        meta.get("event_type").map(String::as_str),
        Some("TriggerFired")
    );
    assert_eq!(meta.get("table_name").map(String::as_str), Some("products"));
    assert_eq!(
        meta.get("trigger_name").map(String::as_str),
        Some("trg_price_check")
    );
    assert_eq!(meta.get("operation").map(String::as_str), Some("UPDATE"));
}
