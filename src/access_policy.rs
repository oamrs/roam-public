//! Row-level and column-level security enforcement for agent data access.
//!
//! ## Design
//!
//! Policies are stored in a backend-managed table and loaded at query time via an
//! [`AccessPolicyProvider`] implementation.  The library layer defines only the
//! *types* and the *enforcer*; concrete providers live in the service layer so
//! that `roam-public` remains infrastructure-agnostic.
//!
//! ## Row-Level Security (RLS)
//!
//! When a table has one or more row policies the enforcer wraps the original SQL in
//! a derived-table subquery:
//!
//! ```sql
//! SELECT * FROM (<original>) AS _roam_rls WHERE <filter_col> = '<filter_val>'
//! ```
//!
//! Multiple same-table policies across different roles are combined with `OR`
//! (most-permissive semantics — if *any* role would allow a row, it is visible).
//!
//! **JOIN fail-secure rule**: if the query contains a top-level `JOIN` keyword *and*
//! any table involved has a row policy, the query is denied.  Permitting JOINs
//! across row-filtered tables would require per-table subquery injection that is
//! both fragile and error-prone; we err on the side of safety.
//!
//! ## Column-Level Security (CLS)
//!
//! Policies carry a `denied_columns` list for each table.  The enforcer:
//!
//! * Rejects queries that explicitly name a denied column in their SELECT list.
//! * Rewrites `SELECT *` to enumerate only allowed columns when a schema model is
//!   provided; if no schema is available and denied columns exist for the queried
//!   table, the query is denied as a conservative fallback.
//!
//! TODO: add `allowed_columns` allowlist mode as an alternative to the current
//! denylist approach.  The enforcer interface is intentionally designed to make
//! this addition backward-compatible.

use crate::mirror::SchemaModel;
use crate::runtime_context::QueryRuntimeContext;
use async_trait::async_trait;
use std::sync::Arc;

// ── Policy data types ─────────────────────────────────────────────────────────

/// A single row-level predicate: rows in `table_name` are restricted to those
/// where `filter_column` equals `filter_value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPolicy {
    /// Unquoted, case-insensitive table name.
    pub table_name: String,
    /// Column to compare (must be a non-computed column).
    pub filter_column: String,
    /// Literal value to match.  Interpolated as a quoted SQL string literal.
    pub filter_value: String,
}

/// Column-level restriction for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPolicy {
    /// Unquoted, case-insensitive table name.
    pub table_name: String,
    /// Columns the requester must NOT access.
    pub denied_columns: Vec<String>,
    // TODO: denied_columns → denylist (current); add allowed_columns → allowlist mode
}

/// The complete policy set that applies to a single principal (role + org scope).
#[derive(Debug, Clone, Default)]
pub struct AccessPolicy {
    /// Role this policy was granted under (informational; used for logging).
    pub role: String,
    /// Organisation scope; `None` means the policy applies across all orgs.
    pub org_id: Option<String>,
    /// Zero or more row-level predicates.
    pub row_policies: Vec<RowPolicy>,
    /// Zero or more column-level restrictions.
    pub column_policies: Vec<ColumnPolicy>,
}

// ── Provider trait ────────────────────────────────────────────────────────────

/// Asynchronous source of access policies.
///
/// The library crate defines this trait; concrete implementations live in the
/// service layer (e.g. `DoltPolicyProvider` backed by a MariaDB/Dolt table).
#[async_trait]
pub trait AccessPolicyProvider: Send + Sync {
    /// Return all policies that apply to `roles` within `org_id`.
    ///
    /// The provider must never return an error for "no policies found" — that is
    /// represented as an empty `Vec`.  Errors are reserved for infrastructure
    /// failures (DB unreachable, etc.).
    async fn get_policies(
        &self,
        user_id: &str,
        org_id: Option<&str>,
        roles: &[String],
    ) -> Result<Vec<AccessPolicy>, String>;
}

// ── Enforcement outcome ───────────────────────────────────────────────────────

/// Result of running the enforcer against a single query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcementOutcome {
    /// No policies apply; continue with original SQL.
    Allow,
    /// The query was rewritten to enforce RLS/CLS.
    Rewrite {
        /// Replacement SQL to execute instead of the original.
        sql: String,
        /// Names of columns that were removed from a `SELECT *` expansion (CLS).
        redacted_columns: Vec<String>,
    },
    /// The query must be blocked entirely.
    Deny { reason: String },
}

// ── AccessEnforcer ────────────────────────────────────────────────────────────

/// Stateless enforcement engine that applies RLS and CLS rules fetched from an
/// [`AccessPolicyProvider`].
pub struct AccessEnforcer {
    provider: Arc<dyn AccessPolicyProvider>,
}

impl AccessEnforcer {
    pub fn new(provider: Arc<dyn AccessPolicyProvider>) -> Self {
        Self { provider }
    }

    /// Fetch applicable policies and enforce them against `query`.
    ///
    /// `schema` is optional.  When provided, `SELECT *` against a table with
    /// column restrictions is rewritten to exclude denied columns.  When absent,
    /// any `SELECT *` targeting a restricted table is denied conservatively.
    pub async fn enforce(
        &self,
        query: &str,
        runtime_context: &QueryRuntimeContext,
        schema: Option<&SchemaModel>,
    ) -> EnforcementOutcome {
        let user_id = match &runtime_context.user_id {
            Some(id) => id.as_str(),
            None => return EnforcementOutcome::Allow,
        };

        if runtime_context.grants.is_empty() {
            return EnforcementOutcome::Allow;
        }

        let org_id = runtime_context.organization_id.as_deref();

        let policies = match self
            .provider
            .get_policies(user_id, org_id, &runtime_context.grants)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return EnforcementOutcome::Deny {
                    reason: format!("Policy provider error: {e}"),
                }
            }
        };

        if policies.is_empty() {
            return EnforcementOutcome::Allow;
        }

        self.apply_policies(query, &policies, schema)
    }

    // ── Internal enforcement ──────────────────────────────────────────────────

    fn apply_policies(
        &self,
        query: &str,
        policies: &[AccessPolicy],
        schema: Option<&SchemaModel>,
    ) -> EnforcementOutcome {
        // Determine the primary table being queried.
        let table_name = match extract_query_table(query) {
            Some(t) => t,
            None => return EnforcementOutcome::Allow,
        };

        // ── Collect applicable row and column policies ─────────────────────
        let row_predicates: Vec<(&str, &str)> = policies
            .iter()
            .flat_map(|p| &p.row_policies)
            .filter(|rp| rp.table_name.eq_ignore_ascii_case(&table_name))
            .map(|rp| (rp.filter_column.as_str(), rp.filter_value.as_str()))
            .collect();

        let denied_columns: Vec<&str> = {
            // Most-permissive for CLS: a column is denied only when *all* applicable
            // policies deny it (intersection).  If even one role permits a column,
            // the column is accessible.
            let per_policy_denied: Vec<Vec<&str>> = policies
                .iter()
                .flat_map(|p| &p.column_policies)
                .filter(|cp| cp.table_name.eq_ignore_ascii_case(&table_name))
                .map(|cp| cp.denied_columns.iter().map(|s| s.as_str()).collect())
                .collect();

            if per_policy_denied.is_empty() {
                vec![]
            } else if per_policy_denied.len() == 1 {
                per_policy_denied.into_iter().next().unwrap()
            } else {
                // Intersection: only columns denied in ALL applicable policies are
                // truly denied (most-permissive semantics).
                let first = per_policy_denied[0].clone();
                first
                    .into_iter()
                    .filter(|col| per_policy_denied[1..].iter().all(|set| set.contains(col)))
                    .collect()
            }
        };

        // ── JOIN fail-secure check ────────────────────────────────────────
        if !row_predicates.is_empty() && query_has_join(query) {
            return EnforcementOutcome::Deny {
                reason: format!(
                    "Cross-table JOINs are not permitted when row-level policies \
                     apply to table '{table_name}'. Rewrite the query to access \
                     the table directly."
                ),
            };
        }

        // ── CLS: validate / rewrite SELECT clause ─────────────────────────
        let (cls_rewritten_sql, redacted_columns) =
            match apply_cls(query, &table_name, &denied_columns, schema) {
                Ok(result) => result,
                Err(reason) => return EnforcementOutcome::Deny { reason },
            };

        // ── RLS: wrap in subquery ─────────────────────────────────────────
        let final_sql = if row_predicates.is_empty() {
            cls_rewritten_sql
        } else {
            apply_rls(&cls_rewritten_sql, &row_predicates)
        };

        if final_sql == query && redacted_columns.is_empty() {
            EnforcementOutcome::Allow
        } else {
            EnforcementOutcome::Rewrite {
                sql: final_sql,
                redacted_columns,
            }
        }
    }
}

// ── SQL helpers ───────────────────────────────────────────────────────────────

/// Extract the primary FROM-clause table name (normalised to lowercase).
///
/// Returns `None` if the query has no top-level `FROM` clause (e.g. DDL).
pub fn extract_query_table(query: &str) -> Option<String> {
    use crate::executor::find_top_level_keyword_position;
    let from_pos = find_top_level_keyword_position(query, "FROM")?;
    let after_from = query[from_pos + 4..].trim_start();
    let raw = after_from
        .split(|c: char| c.is_whitespace() || c == ',' || c == ';' || c == ')')
        .next()
        .unwrap_or("")
        .trim_matches(|c| c == '"' || c == '`' || c == '[' || c == ']');
    if raw.is_empty() {
        None
    } else {
        let unqualified = raw.rsplit('.').next().unwrap_or(raw);
        Some(unqualified.to_lowercase())
    }
}

/// Returns `true` when the query contains a top-level `JOIN` keyword.
fn query_has_join(query: &str) -> bool {
    use crate::executor::find_top_level_keyword_position;
    find_top_level_keyword_position(query, "JOIN").is_some()
}

/// Wrap `sql` in a derived-table subquery and append a WHERE predicate from
/// `predicates`.  Multiple predicates are combined with `OR` (most-permissive).
fn apply_rls(sql: &str, predicates: &[(&str, &str)]) -> String {
    let where_clause = predicates
        .iter()
        .map(|(col, val)| {
            // Escape single quotes in the value to prevent SQL injection.
            let safe_val = val.replace('\'', "''");
            format!("{col} = '{safe_val}'")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    format!("SELECT * FROM ({sql}) AS _roam_rls WHERE {where_clause}")
}

/// Apply column-level security to `query`.
///
/// Returns `(possibly_rewritten_sql, redacted_column_names)` on success, or
/// `Err(denial_reason)` when the query must be blocked.
fn apply_cls(
    query: &str,
    table_name: &str,
    denied_columns: &[&str],
    schema: Option<&SchemaModel>,
) -> Result<(String, Vec<String>), String> {
    if denied_columns.is_empty() {
        return Ok((query.to_string(), vec![]));
    }

    // Find the SELECT...FROM span at the top level.
    let select_start = find_select_keyword(query);
    let from_pos = {
        use crate::executor::find_top_level_keyword_position;
        find_top_level_keyword_position(query, "FROM")
    };

    let (select_start, from_pos) = match (select_start, from_pos) {
        (Some(s), Some(f)) => (s, f),
        _ => return Ok((query.to_string(), vec![])),
    };

    // The column list lives between `SELECT` (+ offset 6) and `FROM`.
    let col_list_raw = query[select_start + 6..from_pos].trim();

    if col_list_raw == "*" {
        // SELECT * — rewrite to enumerate allowed columns.
        match schema {
            Some(s) => {
                let table = s
                    .tables
                    .iter()
                    .find(|t| t.name.eq_ignore_ascii_case(table_name));
                match table {
                    Some(t) => {
                        let (allowed, redacted): (Vec<_>, Vec<_>) =
                            t.columns.iter().map(|c| c.name.as_str()).partition(|col| {
                                !denied_columns.iter().any(|d| col.eq_ignore_ascii_case(d))
                            });

                        if allowed.is_empty() {
                            return Err(format!(
                                "All columns in table '{table_name}' are restricted by \
                                 column-level policy."
                            ));
                        }

                        let redacted_owned: Vec<String> =
                            redacted.iter().map(|s| s.to_string()).collect();
                        let col_list = allowed.join(", ");
                        // Reconstruct: keep everything from FROM onwards unchanged.
                        let rewritten = format!("SELECT {col_list} {}", &query[from_pos..]);
                        Ok((rewritten, redacted_owned))
                    }
                    None => {
                        // Table not found in schema model — pass through; the schema
                        // validation stage will catch the unknown table.
                        Ok((query.to_string(), vec![]))
                    }
                }
            }
            None => {
                // No schema available and denied columns exist: deny conservatively.
                Err(format!(
                    "Column-level policy applies to table '{table_name}' but no \
                     schema model is available to expand 'SELECT *'. Specify \
                     explicit columns in your query."
                ))
            }
        }
    } else {
        // Explicit column list — check for denied references.
        let selected: Vec<&str> = col_list_raw
            .split(',')
            .map(|c| {
                c.trim()
                    .trim_matches(|ch| ch == '"' || ch == '`' || ch == '[' || ch == ']')
            })
            .collect();

        let blocked: Vec<String> = selected
            .iter()
            .filter(|col| denied_columns.iter().any(|d| col.eq_ignore_ascii_case(d)))
            .map(|s| s.to_string())
            .collect();

        if blocked.is_empty() {
            Ok((query.to_string(), vec![]))
        } else {
            Err(format!(
                "Column(s) {} in table '{table_name}' are restricted by \
                 column-level policy.",
                blocked.join(", ")
            ))
        }
    }
}

/// Find the byte offset of the top-level `SELECT` keyword.
fn find_select_keyword(query: &str) -> Option<usize> {
    let upper = query.to_ascii_uppercase();
    let trimmed = upper.trim_start();
    let leading = query.len() - upper.len() + (upper.len() - trimmed.len());
    if trimmed.starts_with("SELECT") {
        Some(leading)
    } else {
        None
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── extract_query_table ──────────────────────────────────────────────────

    #[test]
    fn extracts_simple_table_name() {
        assert_eq!(
            extract_query_table("SELECT * FROM employees"),
            Some("employees".to_string())
        );
    }

    #[test]
    fn extracts_qualified_table_name() {
        assert_eq!(
            extract_query_table("SELECT id FROM public.employees WHERE id = 1"),
            Some("employees".to_string())
        );
    }

    #[test]
    fn returns_none_for_no_from() {
        assert_eq!(extract_query_table("SELECT 1"), None);
    }

    // ── query_has_join ───────────────────────────────────────────────────────

    #[test]
    fn detects_top_level_join() {
        assert!(query_has_join(
            "SELECT a.id FROM employees a JOIN departments d ON a.dept = d.id"
        ));
    }

    #[test]
    fn no_false_positive_join_in_subquery() {
        assert!(!query_has_join(
            "SELECT * FROM (SELECT a.id FROM emp a JOIN dept d ON a.d=d.id) AS sub"
        ));
    }

    // ── apply_rls ────────────────────────────────────────────────────────────

    #[test]
    fn wraps_single_predicate() {
        let sql = apply_rls("SELECT * FROM orders", &[("org_id", "acme")]);
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM orders) AS _roam_rls WHERE org_id = 'acme'"
        );
    }

    #[test]
    fn combines_multiple_predicates_with_or() {
        let sql = apply_rls(
            "SELECT * FROM orders",
            &[("org_id", "acme"), ("org_id", "beta")],
        );
        assert!(sql.contains("org_id = 'acme' OR org_id = 'beta'"));
    }

    #[test]
    fn escapes_single_quotes_in_rls_value() {
        let sql = apply_rls("SELECT * FROM t", &[("name", "o'reilly")]);
        assert!(sql.contains("name = 'o''reilly'"));
    }

    // ── apply_cls ─────────────────────────────────────────────────────────────

    #[test]
    fn passes_through_when_no_denied_columns() {
        let (sql, redacted) = apply_cls("SELECT * FROM employees", "employees", &[], None).unwrap();
        assert_eq!(sql, "SELECT * FROM employees");
        assert!(redacted.is_empty());
    }

    #[test]
    fn denies_explicit_denied_column() {
        let err = apply_cls(
            "SELECT salary FROM employees",
            "employees",
            &["salary"],
            None,
        )
        .unwrap_err();
        assert!(err.contains("salary"));
    }

    #[test]
    fn rejects_star_without_schema_when_columns_denied() {
        let err = apply_cls("SELECT * FROM employees", "employees", &["salary"], None).unwrap_err();
        assert!(err.contains("schema model is available"));
    }

    #[test]
    fn rewrites_star_with_schema() {
        use crate::mirror::{Column, SchemaModel, Table};
        let schema = SchemaModel {
            tables: vec![Table {
                name: "employees".to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        sql_type: "INTEGER".to_string(),
                        nullable: false,
                        primary_key: true,
                        default_value: None,
                        enum_values: None,
                    },
                    Column {
                        name: "name".to_string(),
                        sql_type: "TEXT".to_string(),
                        nullable: true,
                        primary_key: false,
                        default_value: None,
                        enum_values: None,
                    },
                    Column {
                        name: "salary".to_string(),
                        sql_type: "REAL".to_string(),
                        nullable: true,
                        primary_key: false,
                        default_value: None,
                        enum_values: None,
                    },
                ],
                ..Default::default()
            }],
            user_defined_types: vec![],
        };

        let (sql, redacted) = apply_cls(
            "SELECT * FROM employees",
            "employees",
            &["salary"],
            Some(&schema),
        )
        .unwrap();

        assert!(!sql.contains('*'));
        assert!(sql.contains("id"));
        assert!(sql.contains("name"));
        assert!(!sql.contains("salary"));
        assert_eq!(redacted, vec!["salary"]);
    }

    // ── AccessEnforcer integration ───────────────────────────────────────────

    struct AlwaysEmptyProvider;

    #[async_trait]
    impl AccessPolicyProvider for AlwaysEmptyProvider {
        async fn get_policies(
            &self,
            _user_id: &str,
            _org_id: Option<&str>,
            _roles: &[String],
        ) -> Result<Vec<AccessPolicy>, String> {
            Ok(vec![])
        }
    }

    struct FixedPolicyProvider {
        policies: Vec<AccessPolicy>,
    }

    #[async_trait]
    impl AccessPolicyProvider for FixedPolicyProvider {
        async fn get_policies(
            &self,
            _user_id: &str,
            _org_id: Option<&str>,
            _roles: &[String],
        ) -> Result<Vec<AccessPolicy>, String> {
            Ok(self.policies.clone())
        }
    }

    fn make_context(user_id: &str, grants: Vec<&str>) -> QueryRuntimeContext {
        QueryRuntimeContext {
            user_id: Some(user_id.to_string()),
            grants: grants.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn allow_when_no_policies() {
        let enforcer = AccessEnforcer::new(Arc::new(AlwaysEmptyProvider));
        let ctx = make_context("u1", vec!["viewer"]);
        let outcome = enforcer.enforce("SELECT * FROM t", &ctx, None).await;
        assert_eq!(outcome, EnforcementOutcome::Allow);
    }

    #[tokio::test]
    async fn allow_when_no_user_id() {
        let enforcer = AccessEnforcer::new(Arc::new(AlwaysEmptyProvider));
        let ctx = QueryRuntimeContext::default();
        let outcome = enforcer.enforce("SELECT * FROM t", &ctx, None).await;
        assert_eq!(outcome, EnforcementOutcome::Allow);
    }

    #[tokio::test]
    async fn rls_wraps_query() {
        let policies = vec![AccessPolicy {
            role: "viewer".to_string(),
            org_id: None,
            row_policies: vec![RowPolicy {
                table_name: "orders".to_string(),
                filter_column: "org_id".to_string(),
                filter_value: "acme".to_string(),
            }],
            column_policies: vec![],
        }];
        let enforcer = AccessEnforcer::new(Arc::new(FixedPolicyProvider { policies }));
        let ctx = make_context("u1", vec!["viewer"]);
        let outcome = enforcer.enforce("SELECT * FROM orders", &ctx, None).await;
        match outcome {
            EnforcementOutcome::Rewrite { sql, .. } => {
                assert!(sql.contains("_roam_rls"));
                assert!(sql.contains("org_id = 'acme'"));
            }
            other => panic!("Expected Rewrite, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rls_denies_join() {
        let policies = vec![AccessPolicy {
            role: "viewer".to_string(),
            org_id: None,
            row_policies: vec![RowPolicy {
                table_name: "orders".to_string(),
                filter_column: "org_id".to_string(),
                filter_value: "acme".to_string(),
            }],
            column_policies: vec![],
        }];
        let enforcer = AccessEnforcer::new(Arc::new(FixedPolicyProvider { policies }));
        let ctx = make_context("u1", vec!["viewer"]);
        let outcome = enforcer
            .enforce(
                "SELECT o.id FROM orders o JOIN items i ON o.id = i.order_id",
                &ctx,
                None,
            )
            .await;
        assert!(matches!(outcome, EnforcementOutcome::Deny { .. }));
    }

    #[tokio::test]
    async fn cls_denies_explicit_restricted_column() {
        let policies = vec![AccessPolicy {
            role: "viewer".to_string(),
            org_id: None,
            row_policies: vec![],
            column_policies: vec![ColumnPolicy {
                table_name: "employees".to_string(),
                denied_columns: vec!["salary".to_string()],
            }],
        }];
        let enforcer = AccessEnforcer::new(Arc::new(FixedPolicyProvider { policies }));
        let ctx = make_context("u1", vec!["viewer"]);
        let outcome = enforcer
            .enforce("SELECT salary FROM employees", &ctx, None)
            .await;
        assert!(matches!(outcome, EnforcementOutcome::Deny { .. }));
    }

    #[tokio::test]
    async fn most_permissive_cls_intersection() {
        // role_a denies [salary], role_b denies [salary, email]
        // intersection = [salary] — email is accessible via role_b
        let policies = vec![
            AccessPolicy {
                role: "role_a".to_string(),
                org_id: None,
                row_policies: vec![],
                column_policies: vec![ColumnPolicy {
                    table_name: "employees".to_string(),
                    denied_columns: vec!["salary".to_string()],
                }],
            },
            AccessPolicy {
                role: "role_b".to_string(),
                org_id: None,
                row_policies: vec![],
                column_policies: vec![ColumnPolicy {
                    table_name: "employees".to_string(),
                    denied_columns: vec!["salary".to_string(), "email".to_string()],
                }],
            },
        ];
        let enforcer = AccessEnforcer::new(Arc::new(FixedPolicyProvider { policies }));
        let ctx = make_context("u1", vec!["role_a", "role_b"]);

        // email should be accessible (only denied by role_b, not by role_a)
        let outcome_email = enforcer
            .enforce("SELECT email FROM employees", &ctx, None)
            .await;
        assert_eq!(outcome_email, EnforcementOutcome::Allow);

        // salary is denied by both roles → Deny
        let outcome_salary = enforcer
            .enforce("SELECT salary FROM employees", &ctx, None)
            .await;
        assert!(matches!(outcome_salary, EnforcementOutcome::Deny { .. }));
    }
}
