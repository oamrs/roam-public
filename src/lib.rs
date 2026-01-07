use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use schemars::schema::{
    InstanceType, Metadata, ObjectValidation, RootSchema, Schema, SchemaObject, SingleOrVec,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Column {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub enum_values: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub composite_foreign_keys: Vec<CompositeForeignKey>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SchemaModel {
    pub tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
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

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
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
        let mut root_properties = BTreeMap::new();

        for table in &self.tables {
            let mut col_properties = BTreeMap::new();
            let mut required_cols = BTreeSet::new();

            for col in &table.columns {
                // 1. Map SQL Types to JSON Schema Instance Types
                let sql_upper = col.sql_type.to_uppercase();
                let instance_type = if sql_upper.contains("INT") {
                    InstanceType::Integer
                } else if sql_upper.contains("REAL")
                    || sql_upper.contains("FLOAT")
                    || sql_upper.contains("DOUBLE")
                    || sql_upper.contains("NUMERIC")
                {
                    InstanceType::Number
                } else if sql_upper.contains("BOOL") {
                    InstanceType::Boolean
                } else {
                    // Default to String for TEXT, VARCHAR, DATE, BLOB, etc.
                    InstanceType::String
                };

                let mut schema_obj = SchemaObject {
                    instance_type: Some(SingleOrVec::Single(Box::new(instance_type))),
                    ..Default::default()
                };

                // 2. Inject Enums (Crucial for passing json_schema_reflects_enum_constraints)
                if let Some(enums) = &col.enum_values {
                    schema_obj.enum_values = Some(
                        enums
                            .iter()
                            .map(|v| serde_json::Value::String(v.clone()))
                            .collect(),
                    );
                }

                // 3. Add Metadata (Primary Key info, original SQL type)
                let mut desc = format!("SQL Type: {}", col.sql_type);
                if col.primary_key {
                    desc.push_str(" (Primary Key)");
                }
                schema_obj.metadata = Some(Box::new(Metadata {
                    description: Some(desc),
                    ..Default::default()
                }));

                // 4. Handle Nullability
                if !col.nullable {
                    required_cols.insert(col.name.clone());
                }

                col_properties.insert(col.name.clone(), Schema::Object(schema_obj));
            }

            // Create the Schema for the Table (representing a Row)
            let table_schema = SchemaObject {
                instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
                object: Some(Box::new(ObjectValidation {
                    properties: col_properties,
                    required: required_cols,
                    ..Default::default()
                })),
                metadata: Some(Box::new(Metadata {
                    description: Some(format!("Table: {}", table.name)),
                    ..Default::default()
                })),
                ..Default::default()
            };

            root_properties.insert(table.name.clone(), Schema::Object(table_schema));
        }

        // Wrap everything in a Root Object
        let root = SchemaObject {
            instance_type: Some(SingleOrVec::Single(Box::new(InstanceType::Object))),
            object: Some(Box::new(ObjectValidation {
                properties: root_properties,
                ..Default::default()
            })),
            ..Default::default()
        };

        RootSchema {
            schema: root,
            definitions: BTreeMap::new(),
            meta_schema: None,
        }
    }
}

// Helper: extract enum-like CHECK(...) values for a given column from CREATE SQL.
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

/// Minimal SQLite introspector. This is intentionally tiny and contains a
/// deliberate inversion bug in the `nullable` detection so the TDD test
/// will compile but fail for a different reason (assertion about nullability).
pub fn introspect_sqlite_path(path: &str) -> Result<SchemaModel> {
    let conn = Connection::open(path)?;

    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;

    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, rusqlite::Error>>()?;

    let mut tables: Vec<Table> = Vec::new();

    for t in table_names {
        let escaped_table_name = t.replace('\'', "''");
        let pragma_sql = format!("PRAGMA table_info('{}')", escaped_table_name);
        let mut cols_stmt = conn.prepare(&pragma_sql)?;
        let cols = cols_stmt
            .query_map([], |row| {
                let name: String = row.get(1)?;
                let sql_type: String = row.get(2)?;
                let notnull: i32 = row.get(3)?;
                let pk: i32 = row.get(5)?;
                Ok(Column {
                    name,
                    sql_type,
                    // Correct logic: `notnull` is 0 when column is nullable.
                    nullable: notnull == 0,
                    primary_key: pk != 0,
                    // enum detection not implemented yet
                    enum_values: None,
                })
            })?
            .collect::<Result<Vec<Column>, _>>()?;

        // Attempt to detect enum-like CHECK constraints from the CREATE TABLE SQL
        let mut create_sql_stmt =
            conn.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?")?;
        let create_sql: Option<String> = create_sql_stmt
            .query_row([t.as_str()], |row| row.get(0))
            .optional()?;

        // If we have CREATE SQL, try to find CHECK(column IN (...)) patterns
        let mut cols_with_enums: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        if let Some(sql_text) = create_sql {
            for col in cols.iter() {
                if let Some(list) = detect_enum_values(&sql_text, &col.name) {
                    cols_with_enums.insert(col.name.clone(), list);
                }
            }
        }

        // Attach enum values to columns where detected
        let cols = cols
            .into_iter()
            .map(|mut c| {
                if let Some(vals) = cols_with_enums.get(&c.name) {
                    c.enum_values = Some(vals.clone());
                }
                c
            })
            .collect::<Vec<Column>>();

        // Extract foreign keys for this table (SQLite PRAGMA foreign_key_list)
        // PRAGMA foreign_key_list returns one row per column in the FK with an `id`
        // grouping columns that belong to the same (possibly composite) FK.
        let mut fk_stmt = conn.prepare(&format!(
            "PRAGMA foreign_key_list('{}')",
            escaped_table_name
        ))?;
        let fk_rows = fk_stmt
            .query_map([], |row| {
                // Columns: id, seq, table, from, to, on_update, on_delete, match
                let id: i64 = row.get(0)?;
                let seq: i64 = row.get(1)?;
                let referenced_table: String = row.get(2)?;
                let from_col: String = row.get(3)?; // 'from'
                let to_col: String = row.get(4)?; // 'to'
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

        use std::collections::BTreeMap;
        let mut groups: BTreeMap<i64, Vec<(i64, String, String)>> = BTreeMap::new();
        let mut id_to_table: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();
        let mut id_to_on_delete: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();
        let mut id_to_on_update: std::collections::HashMap<i64, String> =
            std::collections::HashMap::new();
        for (id, seq, ref_table, from_col, to_col, on_update, on_delete) in fk_rows {
            groups.entry(id).or_default().push((seq, from_col, to_col));
            id_to_table.entry(id).or_insert(ref_table);
            id_to_on_delete.entry(id).or_insert(on_delete);
            id_to_on_update.entry(id).or_insert(on_update);
        }

        let mut single_fks: Vec<ForeignKey> = Vec::new();
        let mut composite_fks: Vec<CompositeForeignKey> = Vec::new();

        for (id, mut rows) in groups {
            // sort by seq to preserve column order
            rows.sort_by_key(|r| r.0);
            let ref_table = id_to_table.get(&id).cloned().unwrap_or_default();
            let on_delete_str = id_to_on_delete.get(&id).cloned();
            let on_update_str = id_to_on_update.get(&id).cloned();
            // Convert empty string or "NO ACTION" to None (SQLite returns "NO ACTION" as default)
            let on_delete = on_delete_str.filter(|s| !s.is_empty() && s != "NO ACTION");
            let on_update = on_update_str.filter(|s| !s.is_empty() && s != "NO ACTION");

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

        tables.push(Table {
            name: t,
            columns: cols,
            foreign_keys: single_fks,
            composite_foreign_keys: composite_fks,
        });
    }

    Ok(SchemaModel { tables })
}
