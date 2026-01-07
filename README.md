# oam-mirror

SeaORM Entity-to-Tool reflection. This crate provides a minimal SQLite introspector
used to extract tables and column metadata and emit a canonical `SchemaModel`.

This is a prototype used for initial TDD cycles. It intentionally keeps logic small
—production code should expand to support SeaORM introspection, Postgres types,
foreign keys, enums, constraints, and JSON Schema/Pydantic generation for LLM tools.

Run tests:

```bash
cd oam/mirror
cargo test
```
