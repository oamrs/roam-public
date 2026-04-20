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

use crate::runtime_context::QueryRuntimeContext;
use async_trait::async_trait;

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
/// enterprise service layer.
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
        /// `true` when row-level predicates were applied (the query was wrapped
        /// in a derived-table subquery).  Use this flag instead of inspecting
        /// SQL text to trigger a `RowsFiltered` event.
        rls_applied: bool,
    },
    /// The query must be blocked entirely.
    Deny { reason: String },
}

// ── Enforcer hook ─────────────────────────────────────────────────────────────

/// Hook point for data-access enforcement implementations.
///
/// The concrete enforcer (SQL rewriting with RLS/CLS) lives in the
/// enterprise service layer and is **not** part of the open-source `oam` crate.
/// OSS consumers may implement this trait for custom enforcement logic.
#[async_trait]
pub trait DataAccessEnforcer: Send + Sync {
    /// Enforce access policies against `query` for the given runtime context.
    async fn enforce(
        &self,
        query: &str,
        runtime_context: &QueryRuntimeContext,
        schema: Option<&crate::mirror::SchemaModel>,
    ) -> EnforcementOutcome;
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
}
