# roam

The **roam** crate provides OAM (Object Agent Mapper) core functionality with submodules for different concerns:

- **mirror** - SeaORM Entity-to-Tool reflection. Extracts table and column metadata and emits a canonical `SchemaModel`.

## roam::mirror

Provides a SQLite introspector used to extract database metadata and emit LLM-safe tool definitions.

### Features

- **Table & Column Metadata**: Extracts all tables and columns with types, nullability, and primary key information
- **Foreign Key Support**: Detects both simple single-column and composite multi-column foreign keys with ON DELETE/UPDATE actions
- **Enum Detection**: Extracts enum-like constraints from CHECK constraints (e.g., `CHECK(role IN ('admin', 'user'))`)
- **JSON Schema Generation**: Converts database schema to JSON Schema for LLM consumption
- **Type Mapping**: Maps SQL types to JSON Schema instance types (INTEGER, REAL, TEXT, BLOB, BOOLEAN)

### Future Enhancements

- SeaORM entity introspection
- PostgreSQL support
- Additional constraint types
- Pydantic model generation

Run tests:

```bash
cd roam-public
cargo test
```
