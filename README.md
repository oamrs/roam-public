# roam

The **roam** crate provides OAM (Object Agent Mapper) core functionality with submodules for different concerns:

- **mirror** - SeaORM Entity-to-Tool reflection. Extracts table and column metadata and emits a canonical `SchemaModel`.

## roam::mirror

Provides a minimal SQLite introspector used to extract database metadata and emit LLM-safe tool definitions.

This is a prototype used for initial TDD cycles. It intentionally keeps logic small
—production code should expand to support SeaORM introspection, Postgres types,
foreign keys, enums, constraints, and JSON Schema/Pydantic generation for LLM tools.

Run tests:

```bash
cd roam-public
cargo test
```
