use anyhow::Result;
use rusqlite::Connection;
use schemars::schema::RootSchema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Column {
	pub name: String,
	pub sql_type: String,
	pub nullable: bool,
	pub primary_key: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct Table {
	pub name: String,
	pub columns: Vec<Column>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct SchemaModel {
	pub tables: Vec<Table>,
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
				})
			})?
			.collect::<Result<Vec<Column>, _>>()?;

		tables.push(Table { name: t, columns: cols });
	}

	Ok(SchemaModel { tables })
}


