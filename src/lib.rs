use anyhow::Result;
use rusqlite::Connection;
use rusqlite::OptionalExtension;
use regex::Regex;
use schemars::schema::RootSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct CompositeForeignKey {
	/// columns on this table (in order)
	pub columns: Vec<String>,
	/// referenced table name
	pub referenced_table: String,
	/// referenced columns (in order)
	pub referenced_columns: Vec<String>,
}

impl SchemaModel {
	pub fn to_json_schema(&self) -> RootSchema {
		schemars::schema_for!(SchemaModel)
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
		let mut cols_stmt = conn.prepare(&format!("PRAGMA table_info('{}')", t))?;
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
		let mut create_sql_stmt = conn.prepare(
			"SELECT sql FROM sqlite_master WHERE type='table' AND name = ?",
		)?;
		let create_sql: Option<String> = create_sql_stmt
			.query_row([t.as_str()], |row| row.get(0))
			.optional()?;

		// If we have CREATE SQL, try to find CHECK(column IN (...)) patterns
		let mut cols_with_enums: std::collections::HashMap<String, Vec<String>> =
			std::collections::HashMap::new();
		if let Some(sql_text) = create_sql {
			for col in cols.iter() {
				let esc = regex::escape(&col.name);
				let pat = format!(r"(?i)CHECK\s*\(\s*{}\s+IN\s*\((?P<vals>[^)]+)\)\s*\)", esc);
				if let Ok(re) = Regex::new(&pat) {
					if let Some(caps) = re.captures(&sql_text) {
						if let Some(vals) = caps.name("vals") {
							let list = vals
								.as_str()
								.split(',')
								.map(|s| s.trim().trim_matches('\'').trim_matches('"').to_string())
								.collect::<Vec<String>>();
							if !list.is_empty() {
								cols_with_enums.insert(col.name.clone(), list);
							}
						}
					}
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
		let mut fk_stmt = conn.prepare(&format!("PRAGMA foreign_key_list('{}')", t))?;
		let fk_rows = fk_stmt
			.query_map([], |row| {
				// Columns: id, seq, table, from, to, on_update, on_delete, match
				let id: i64 = row.get(0)?;
				let seq: i64 = row.get(1)?;
				let referenced_table: String = row.get(2)?;
				let from_col: String = row.get(3)?; // 'from'
				let to_col: String = row.get(4)?; // 'to'
				Ok((id, seq, referenced_table, from_col, to_col))
			})?
			.collect::<Result<Vec<_>, _>>()?;

		use std::collections::BTreeMap;
		let mut groups: BTreeMap<i64, Vec<(i64, String, String)>> = BTreeMap::new();
		let mut id_to_table: std::collections::HashMap<i64, String> = std::collections::HashMap::new();
		for (id, seq, ref_table, from_col, to_col) in fk_rows {
			groups.entry(id).or_default().push((seq, from_col, to_col));
			id_to_table.entry(id).or_insert(ref_table);
		}

		let mut single_fks: Vec<ForeignKey> = Vec::new();
		let mut composite_fks: Vec<CompositeForeignKey> = Vec::new();

		for (id, mut rows) in groups {
			// sort by seq to preserve column order
			rows.sort_by_key(|r| r.0);
			let ref_table = id_to_table.get(&id).cloned().unwrap_or_default();
			if rows.len() == 1 {
				let (_seq, from_col, to_col) = rows.into_iter().next().unwrap();
				single_fks.push(ForeignKey {
					column: from_col,
					referenced_table: ref_table,
					referenced_column: to_col,
				});
			} else {
				let columns: Vec<String> = rows.iter().map(|r| r.1.clone()).collect();
				let referenced_columns: Vec<String> = rows.iter().map(|r| r.2.clone()).collect();
				composite_fks.push(CompositeForeignKey {
					columns,
					referenced_table: ref_table,
					referenced_columns,
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


