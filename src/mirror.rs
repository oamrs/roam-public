use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use schemars::schema::{
    InstanceType, Metadata, ObjectValidation, RootSchema, Schema, SchemaObject, SingleOrVec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Column {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub default_value: Option<String>,
    pub enum_values: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct UniqueIndex {
    /// Name of the unique index as recorded in the database
    pub name: String,
    /// Ordered list of column names covered by this index
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Index {
    /// Name of the index as recorded in the database
    pub name: String,
    /// Ordered list of column names covered by this index
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Trigger {
    pub name: String,
    pub event: String,
    pub timing: String,
    pub table_name: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct UserDefinedType {
    pub name: String,
    pub base_type: String,
    pub check_constraint: Option<String>,
    pub nullable: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct FieldMapping {
    pub logical_name: String,
    pub physical_name: String,
    pub orm_convention: String,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub composite_foreign_keys: Vec<CompositeForeignKey>,
    pub unique_indexes: Vec<UniqueIndex>,
    pub indexes: Vec<Index>,
    pub triggers: Vec<Trigger>,
    pub field_mappings: Vec<FieldMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SchemaModel {
    pub tables: Vec<Table>,
    pub user_defined_types: Vec<UserDefinedType>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct ForeignKey {
    /// column on this table
    pub column: String,
    /// referenced table name
    pub referenced_table: String,
    /// referenced column name
    pub referenced_column: String,
    /// ON DELETE action (e.g., "CASCADE", "SET NULL", "RESTRICT")
    pub on_delete: Option<String>,
    /// ON UPDATE action (e.g., "CASCADE", "SET NULL", "RESTRICT")
    pub on_update: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CompositeForeignKey {
    /// columns on this table (in order)
    pub columns: Vec<String>,
    /// referenced table name
    pub referenced_table: String,
    /// referenced columns (in order)
    pub referenced_columns: Vec<String>,
    /// ON DELETE action (e.g., "CASCADE", "SET NULL", "RESTRICT")
    pub on_delete: Option<String>,
    /// ON UPDATE action (e.g., "CASCADE", "SET NULL", "RESTRICT")
    pub on_update: Option<String>,
}

impl SchemaModel {
    /// Generates an LLM-ready JSON Schema representing the data structure of the database.
    /// This schema defines an Object where keys are Table Names and values are Objects
    /// representing a row in that table.
    pub fn to_json_schema(&self) -> RootSchema {
        let mut root = SchemaObject {
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
            ..Default::default()
        };
        for table in &self.tables {
            let (name, schema) = build_table_json_schema(table);
            root.object
                .get_or_insert_with(Default::default)
                .properties
                .insert(name, schema);
        }
        RootSchema {
            schema: root,
            ..Default::default()
        }
    }
}

fn column_instance_type(sql_type: &str) -> InstanceType {
    let upper = sql_type.to_uppercase();
    if upper.contains("INT") {
        InstanceType::Integer
    } else if upper.contains("REAL")
        || upper.contains("FLOAT")
        || upper.contains("DOUBLE")
        || upper.contains("NUMERIC")
    {
        InstanceType::Number
    } else if upper.contains("BOOL") {
        InstanceType::Boolean
    } else {
        InstanceType::String
    }
}

/// Returns true when the column is a SQLite rowid alias (sole INTEGER PRIMARY KEY).
/// Only rowid aliases are auto-generated; other PKs (TEXT, composite, etc.) must be
/// supplied by the caller.
fn is_rowid_alias(col: &Column, pk_count: usize) -> bool {
    col.primary_key && pk_count == 1 && col.sql_type.to_uppercase() == "INTEGER"
}

fn build_column_description(
    col: &Column,
    fk_map: &HashMap<&str, &ForeignKey>,
    single_unique_set: &HashSet<&str>,
    rowid_alias: bool,
) -> String {
    let mut desc = format!("SQL Type: {}", col.sql_type);
    if col.primary_key {
        if rowid_alias {
            desc.push_str(" | Primary Key — auto-generated; omit on INSERT");
        } else {
            desc.push_str(" | Primary Key");
        }
    }
    if let Some(fk) = fk_map.get(col.name.as_str()) {
        desc.push_str(&format!(
            " | Foreign Key → {}({})",
            fk.referenced_table, fk.referenced_column
        ));
        if let Some(ref action) = fk.on_delete {
            desc.push_str(&format!(", ON DELETE {}", action));
        }
        if let Some(ref action) = fk.on_update {
            desc.push_str(&format!(", ON UPDATE {}", action));
        }
    }
    if single_unique_set.contains(col.name.as_str()) {
        desc.push_str(" | UNIQUE — value must be unique across all rows");
    }
    if let Some(ref default) = col.default_value {
        desc.push_str(&format!(" | Default: {}", default));
    }
    if col.enum_values.is_some() {
        desc.push_str(" | Constrained by CHECK IN (enum values)");
    }
    desc
}

fn build_column_schema_obj(
    col: &Column,
    fk_map: &HashMap<&str, &ForeignKey>,
    single_unique_set: &HashSet<&str>,
    rowid_alias: bool,
) -> SchemaObject {
    let instance_type = column_instance_type(&col.sql_type);
    let mut schema_obj = SchemaObject {
        instance_type: Some(SingleOrVec::Single(Box::new(instance_type))),
        ..Default::default()
    };
    if let Some(enums) = &col.enum_values {
        schema_obj.enum_values = Some(
            enums
                .iter()
                .map(|v| serde_json::Value::String(v.clone()))
                .collect(),
        );
    }
    let desc = build_column_description(col, fk_map, single_unique_set, rowid_alias);
    schema_obj.metadata = Some(Box::new(Metadata {
        description: Some(desc),
        ..Default::default()
    }));
    schema_obj
}

fn build_table_json_schema(table: &Table) -> (String, Schema) {
    let fk_map: HashMap<&str, &ForeignKey> = table
        .foreign_keys
        .iter()
        .map(|fk| (fk.column.as_str(), fk))
        .collect();
    let single_unique_set: HashSet<&str> = table
        .unique_indexes
        .iter()
        .filter(|u| u.columns.len() == 1)
        .map(|u| u.columns[0].as_str())
        .collect();

    let pk_count = table.columns.iter().filter(|c| c.primary_key).count();

    let mut table_desc = format!("Table: {}", table.name);
    for u in &table.unique_indexes {
        if u.columns.len() > 1 {
            table_desc.push_str(&format!(
                " | UNIQUE ({}) — these columns together must be unique",
                u.columns.join(", ")
            ));
        }
    }
    for cfk in &table.composite_foreign_keys {
        table_desc.push_str(&format!(
            " | ({}) → {}({})",
            cfk.columns.join(", "),
            cfk.referenced_table,
            cfk.referenced_columns.join(", ")
        ));
    }

    let mut table_schema = SchemaObject {
        instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
        metadata: Some(Box::new(Metadata {
            description: Some(table_desc),
            ..Default::default()
        })),
        ..Default::default()
    };
    table_schema
        .object
        .get_or_insert_with(|| Box::new(ObjectValidation::default()))
        .additional_properties = Some(Box::new(Schema::Bool(false)));

    let mut required_cols = BTreeSet::new();
    for col in &table.columns {
        let rowid = is_rowid_alias(col, pk_count);
        let schema_obj = build_column_schema_obj(col, &fk_map, &single_unique_set, rowid);
        // Rowid-alias PKs are auto-generated and should not be in required.
        // All other non-nullable columns (including non-rowid PKs) must be provided.
        if !col.nullable && !rowid {
            required_cols.insert(col.name.clone());
        }
        table_schema
            .object
            .get_or_insert_with(|| Box::new(ObjectValidation::default()))
            .properties
            .insert(col.name.clone(), Schema::Object(schema_obj));
    }
    table_schema
        .object
        .get_or_insert_with(|| Box::new(ObjectValidation::default()))
        .required = required_cols;

    (table.name.clone(), Schema::Object(table_schema))
}

fn detect_enum_values(sql_text: &str, col_name: &str) -> Option<Vec<String>> {
    let esc = regex::escape(col_name);
    let pat = format!(r"(?i)CHECK\s*\(\s*{}\s+IN\s*\((?P<vals>[^)]+)\)\s*\)", esc);
    let re = Regex::new(&pat).ok()?;
    let caps = re.captures(sql_text)?;
    let vals = caps.name("vals")?;
    let list = vals
        .as_str()
        .split(',')
        .map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<String>>();
    if list.is_empty() {
        None
    } else {
        Some(list)
    }
}

fn introspect_columns(conn: &Connection, table: &str) -> Result<Vec<Column>> {
    let mut cols_stmt =
        conn.prepare(&format!("PRAGMA table_info({})", sqlite_quote_ident(table)))?;
    let cols = cols_stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let sql_type: String = row.get(2)?;
            let notnull: i32 = row.get(3)?;
            let default_value: Option<String> = row.get(4)?;
            let pk: i32 = row.get(5)?;
            Ok(Column {
                name,
                sql_type,
                nullable: notnull == 0,
                primary_key: pk != 0,
                default_value,
                enum_values: None,
            })
        })?
        .collect::<Result<Vec<Column>, _>>()?;

    let mut create_sql_stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?")?;
    let create_sql: Option<String> = create_sql_stmt
        .query_row([table], |row| row.get(0))
        .optional()?;

    match create_sql {
        None => Ok(cols),
        Some(sql_text) => {
            let enum_map: HashMap<String, Vec<String>> = cols
                .iter()
                .filter_map(|col| {
                    detect_enum_values(&sql_text, &col.name).map(|vals| (col.name.clone(), vals))
                })
                .collect();
            Ok(cols
                .into_iter()
                .map(|mut c| {
                    if let Some(vals) = enum_map.get(&c.name) {
                        c.enum_values = Some(vals.clone());
                    }
                    c
                })
                .collect())
        }
    }
}

fn introspect_foreign_keys(
    conn: &Connection,
    table: &str,
) -> Result<(Vec<ForeignKey>, Vec<CompositeForeignKey>)> {
    use std::collections::BTreeMap;
    let mut fk_stmt = conn.prepare(&format!(
        "PRAGMA foreign_key_list({})",
        sqlite_quote_ident(table)
    ))?;
    let fk_rows = fk_stmt
        .query_map([], |row| {
            let id: i64 = row.get(0)?;
            let seq: i64 = row.get(1)?;
            let referenced_table: String = row.get(2)?;
            let from_col: String = row.get(3)?;
            let to_col: String = row.get(4)?;
            let on_update: String = row.get(5)?;
            let on_delete: String = row.get(6)?;
            Ok((
                id,
                seq,
                referenced_table,
                from_col,
                to_col,
                on_update,
                on_delete,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut groups: BTreeMap<i64, Vec<(i64, String, String)>> = BTreeMap::new();
    let mut id_to_table: HashMap<i64, String> = HashMap::new();
    let mut id_to_on_delete: HashMap<i64, String> = HashMap::new();
    let mut id_to_on_update: HashMap<i64, String> = HashMap::new();
    for (id, seq, ref_table, from_col, to_col, on_update, on_delete) in fk_rows {
        groups.entry(id).or_default().push((seq, from_col, to_col));
        id_to_table.entry(id).or_insert(ref_table);
        id_to_on_delete.entry(id).or_insert(on_delete);
        id_to_on_update.entry(id).or_insert(on_update);
    }

    let mut single_fks: Vec<ForeignKey> = Vec::new();
    let mut composite_fks: Vec<CompositeForeignKey> = Vec::new();
    for (id, mut rows) in groups {
        rows.sort_by_key(|r| r.0);
        let ref_table = id_to_table.get(&id).cloned().unwrap_or_default();
        let on_delete = id_to_on_delete
            .get(&id)
            .cloned()
            .filter(|s| !s.is_empty() && s != "NO ACTION");
        let on_update = id_to_on_update
            .get(&id)
            .cloned()
            .filter(|s| !s.is_empty() && s != "NO ACTION");
        if rows.len() == 1 {
            let (_seq, from_col, to_col) = rows.into_iter().next().unwrap();
            single_fks.push(ForeignKey {
                column: from_col,
                referenced_table: ref_table,
                referenced_column: to_col,
                on_delete: on_delete.clone(),
                on_update: on_update.clone(),
            });
        } else {
            let columns: Vec<String> = rows.iter().map(|r| r.1.clone()).collect();
            let referenced_columns: Vec<String> = rows.iter().map(|r| r.2.clone()).collect();
            composite_fks.push(CompositeForeignKey {
                columns,
                referenced_table: ref_table,
                referenced_columns,
                on_delete: on_delete.clone(),
                on_update: on_update.clone(),
            });
        }
    }
    Ok((single_fks, composite_fks))
}

/// Wraps a SQLite identifier in double-quotes, escaping any embedded double-quotes.
/// Use this for all PRAGMA identifier arguments to handle names with special characters.
fn sqlite_quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn introspect_indexes(conn: &Connection, table: &str) -> Result<(Vec<UniqueIndex>, Vec<Index>)> {
    let mut idx_list_stmt =
        conn.prepare(&format!("PRAGMA index_list({})", sqlite_quote_ident(table)))?;
    let index_rows = idx_list_stmt
        .query_map([], |row| {
            let name: String = row.get(1)?;
            let unique: i32 = row.get(2)?;
            let origin: String = row.get(3)?;
            let partial: i32 = row.get(4)?;
            Ok((name, unique, origin, partial))
        })?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    let mut unique_indexes: Vec<UniqueIndex> = Vec::new();
    let mut indexes: Vec<Index> = Vec::new();
    for (idx_name, unique, origin, partial) in index_rows {
        if partial != 0 || origin == "pk" {
            continue;
        }
        let mut idx_info_stmt = conn.prepare(&format!(
            "PRAGMA index_info({})",
            sqlite_quote_ident(&idx_name)
        ))?;
        let mut col_rows = idx_info_stmt
            .query_map([], |row| {
                let seqno: i32 = row.get(0)?;
                let col_name: String = row.get(2)?;
                Ok((seqno, col_name))
            })?
            .collect::<Result<Vec<_>, rusqlite::Error>>()?;
        col_rows.sort_by_key(|r| r.0);
        let columns: Vec<String> = col_rows.into_iter().map(|r| r.1).collect();
        if columns.is_empty() {
            continue;
        }
        if unique == 1 {
            unique_indexes.push(UniqueIndex {
                name: idx_name,
                columns,
            });
        } else if origin == "c" {
            indexes.push(Index {
                name: idx_name,
                columns,
            });
        }
    }
    Ok((unique_indexes, indexes))
}

pub fn introspect_sqlite_path(path: &str) -> Result<SchemaModel> {
    let conn = Connection::open(path)?;
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    let tables = table_names
        .into_iter()
        .map(|t| {
            let columns = introspect_columns(&conn, &t)?;
            let (foreign_keys, composite_foreign_keys) = introspect_foreign_keys(&conn, &t)?;
            let (unique_indexes, indexes) = introspect_indexes(&conn, &t)?;
            let triggers = introspect_triggers(&conn, &t)?;
            let field_mappings = detect_field_mappings(&columns);
            Ok(Table {
                name: t,
                columns,
                foreign_keys,
                composite_foreign_keys,
                unique_indexes,
                indexes,
                triggers,
                field_mappings,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let user_defined_types = introspect_user_defined_types(&conn)?;

    Ok(SchemaModel {
        tables,
        user_defined_types,
    })
}

fn introspect_triggers(conn: &Connection, table: &str) -> Result<Vec<Trigger>> {
    let mut stmt =
        conn.prepare("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name = ?")?;
    let rows = stmt
        .query_map([table], |row| {
            let name: String = row.get(0)?;
            let body: Option<String> = row.get(1)?;
            Ok((name, body.unwrap_or_default()))
        })?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    let mut triggers = Vec::new();
    for (name, body) in rows {
        let (timing, event) = parse_trigger_timing_event(&body);
        triggers.push(Trigger {
            name,
            event,
            timing,
            table_name: table.to_string(),
            body,
        });
    }
    Ok(triggers)
}

fn parse_trigger_timing_event(ddl: &str) -> (String, String) {
    let upper = ddl.to_uppercase();
    let timing = if upper.contains("INSTEAD OF") {
        "INSTEAD OF"
    } else if upper.contains("BEFORE") {
        "BEFORE"
    } else {
        "AFTER"
    };
    let event = if upper.contains("INSERT") {
        "INSERT"
    } else if upper.contains("DELETE") {
        "DELETE"
    } else {
        "UPDATE"
    };
    (timing.to_string(), event.to_string())
}

fn introspect_user_defined_types(conn: &Connection) -> Result<Vec<UserDefinedType>> {
    let standard_affinities = [
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "MEDIUMINT",
        "BIGINT",
        "TEXT",
        "CHARACTER",
        "VARCHAR",
        "NCHAR",
        "NVARCHAR",
        "CLOB",
        "BLOB",
        "REAL",
        "DOUBLE",
        "FLOAT",
        "NUMERIC",
        "DECIMAL",
        "BOOLEAN",
        "DATE",
        "DATETIME",
    ];
    let sql_keywords = [
        "PRIMARY",
        "NOT",
        "NULL",
        "DEFAULT",
        "UNIQUE",
        "CHECK",
        "REFERENCES",
        "FOREIGN",
        "KEY",
        "CONSTRAINT",
        "INDEX",
        "CREATE",
        "TABLE",
        "VIEW",
        "TRIGGER",
        "ON",
        "AS",
        "END",
        "BEGIN",
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "WHERE",
        "NEW",
        "OLD",
        "RAISE",
        "ABORT",
    ];

    let mut stmt =
        conn.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL")?;
    let ddl_rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    let mut seen: HashSet<String> = HashSet::new();
    let mut udts: Vec<UserDefinedType> = Vec::new();

    // Parse column definitions: word followed by a non-keyword type token.
    // Compiled once outside the loop to satisfy clippy::regex_creation_in_loops.
    let col_type_re = Regex::new(r"(?i)\b([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]+)")
        .expect("valid regex");

    for sql in ddl_rows {
        for caps in col_type_re.captures_iter(&sql) {
            let type_token = caps[2].trim().to_uppercase();
            if standard_affinities.iter().any(|a| type_token == *a) {
                continue;
            }
            if sql_keywords.iter().any(|k| type_token == *k) {
                continue;
            }
            if type_token.len() < 2 || seen.contains(&type_token) {
                continue;
            }
            seen.insert(type_token.clone());
            udts.push(UserDefinedType {
                name: caps[2].trim().to_string(),
                base_type: sqlite_type_affinity(&type_token).to_string(),
                check_constraint: None,
                nullable: true,
                default_value: None,
            });
        }
    }
    Ok(udts)
}

fn sqlite_type_affinity(upper_type: &str) -> &'static str {
    if upper_type.contains("INT") {
        "INTEGER"
    } else if upper_type.contains("CHAR")
        || upper_type.contains("CLOB")
        || upper_type.contains("TEXT")
    {
        "TEXT"
    } else if upper_type.contains("BLOB") || upper_type.is_empty() {
        "BLOB"
    } else if upper_type.contains("REAL")
        || upper_type.contains("FLOA")
        || upper_type.contains("DOUB")
    {
        "REAL"
    } else {
        "NUMERIC"
    }
}

fn detect_field_mappings(columns: &[Column]) -> Vec<FieldMapping> {
    let mut mappings = Vec::new();
    for col in columns {
        let physical = &col.name;

        if looks_like_camel_case(physical) {
            mappings.push(FieldMapping {
                logical_name: camel_to_snake(physical),
                physical_name: physical.clone(),
                orm_convention: "Hibernate".to_string(),
                notes: Some("camelCase column mapped to snake_case property".to_string()),
            });
        } else if looks_like_pascal_case(physical) {
            mappings.push(FieldMapping {
                logical_name: camel_to_snake(physical),
                physical_name: physical.clone(),
                orm_convention: "EntityFramework".to_string(),
                notes: Some("PascalCase column mapped to snake_case property".to_string()),
            });
        }

        if let Some(stripped) = physical.strip_prefix('_') {
            if !stripped.is_empty() {
                mappings.push(FieldMapping {
                    logical_name: stripped.to_string(),
                    physical_name: physical.clone(),
                    orm_convention: "EntityFramework".to_string(),
                    notes: Some("EF Core shadow property convention".to_string()),
                });
            }
        }
    }
    mappings
}

fn looks_like_camel_case(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_lowercase() {
        return false;
    }
    bytes[1..].iter().any(|b| b.is_ascii_uppercase())
}

fn looks_like_pascal_case(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_uppercase() {
        return false;
    }
    bytes[1..].iter().any(|b| b.is_ascii_lowercase())
}

fn camel_to_snake(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.char_indices() {
        if ch.is_ascii_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}
