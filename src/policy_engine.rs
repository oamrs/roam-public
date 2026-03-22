use serde::{Deserialize, Serialize};

const TRANSACTION_KEYWORDS: &[&str] = &[
    "BEGIN",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "TRANSACTION",
];
const DDL_KEYWORDS: &[&str] = &["CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME"];
const WRITE_KEYWORDS: &[&str] = &["INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE"];
const ADMIN_KEYWORDS: &[&str] = &[
    "ATTACH", "DETACH", "PRAGMA", "EXPLAIN", "VACUUM", "CALL", "GRANT", "REVOKE",
];
const SET_OPERATION_KEYWORDS: &[&str] = &["UNION", "INTERSECT", "EXCEPT"];
const STATEMENT_KEYWORDS: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "UPSERT",
    "REPLACE",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "BEGIN",
    "START",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "PRAGMA",
    "ATTACH",
    "DETACH",
    "EXPLAIN",
    "VACUUM",
    "CALL",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolIntent {
    ReadSelect,
    WriteInsert,
    WriteUpdate,
    WriteDelete,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizedSubqueryShape {
    pub table: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SubqueryPolicy {
    #[default]
    DenyAll,
    AllowListed(Vec<AuthorizedSubqueryShape>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolContract {
    pub name: String,
    pub intent: ToolIntent,
    pub subquery_policy: SubqueryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationContext {
    pub allowed_intents: Vec<ToolIntent>,
    pub grants: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyContext {
    pub tool: ToolContract,
    pub authorization: AuthorizationContext,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub allowed: bool,
    pub classification: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
struct Token {
    word: String,
    depth: usize,
}

#[derive(Debug, Default)]
struct LexAnalysis {
    tokens: Vec<Token>,
    has_line_comment: bool,
    has_block_comment: bool,
    has_semicolon: bool,
}

#[derive(Debug)]
struct SqlAnalysis {
    normalized_query: String,
    tokens: Vec<Token>,
    main_statement: Option<String>,
    has_nested_select: bool,
    nested_select_tables: Vec<String>,
    has_line_comment: bool,
    has_block_comment: bool,
    has_semicolon: bool,
}

impl AuthorizationContext {
    pub fn allows_intent(&self, intent: ToolIntent) -> bool {
        self.allowed_intents.contains(&intent)
    }
}

impl ToolContract {
    pub fn new(name: impl Into<String>, intent: ToolIntent) -> Self {
        Self {
            name: name.into(),
            intent,
            subquery_policy: SubqueryPolicy::DenyAll,
        }
    }
}

impl PolicyContext {
    pub fn for_intent(intent: ToolIntent) -> Self {
        Self {
            tool: ToolContract::new(default_tool_name(intent), intent),
            authorization: AuthorizationContext {
                allowed_intents: vec![intent],
                grants: Vec::new(),
            },
        }
    }
}

pub struct PolicyEngine;

impl PolicyEngine {
    pub fn evaluate(query: &str, intent: ToolIntent) -> PolicyDecision {
        let context = PolicyContext::for_intent(intent);
        Self::evaluate_with_context(query, &context)
    }

    pub fn evaluate_with_context(query: &str, context: &PolicyContext) -> PolicyDecision {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Self::deny("invalid", "Query is empty");
        }

        if !context.authorization.allows_intent(context.tool.intent) {
            return Self::deny(
                "unauthorized",
                &format!(
                    "Tool contract intent '{}' is not authorized by the supplied policy context",
                    classify_intent(context.tool.intent)
                ),
            );
        }

        let analysis = SqlAnalysis::analyze(trimmed);

        if analysis.has_line_comment {
            return Self::deny("invalid", "SQL line comments are not allowed");
        }

        if analysis.has_block_comment {
            return Self::deny("invalid", "SQL block comments are not allowed");
        }

        if analysis.has_semicolon {
            return Self::deny("invalid", "Semicolon-based command chaining is not allowed");
        }

        if let Some(reason) = Self::detect_injection_heuristics(&analysis.normalized_query) {
            return Self::deny("invalid", &reason);
        }

        match context.tool.intent {
            ToolIntent::ReadSelect => {
                Self::enforce_read_select(analysis, &context.tool.subquery_policy)
            }
            ToolIntent::WriteInsert | ToolIntent::WriteUpdate | ToolIntent::WriteDelete => {
                Self::deny(
                    "unsupported",
                    "Only read-select intent is currently supported by the public policy engine",
                )
            }
            ToolIntent::Admin => Self::deny(
                "unsupported",
                "Administrative SQL intents are not supported by the public policy engine",
            ),
        }
    }

    fn enforce_read_select(
        analysis: SqlAnalysis,
        subquery_policy: &SubqueryPolicy,
    ) -> PolicyDecision {
        if let Some(keyword) = Self::find_keyword(&analysis.tokens, TRANSACTION_KEYWORDS) {
            return Self::deny(
                "transaction",
                &format!(
                    "Transaction control is not allowed for read-select intent: {}",
                    keyword.to_lowercase()
                ),
            );
        }

        if let Some(keyword) = Self::find_keyword(&analysis.tokens, DDL_KEYWORDS) {
            return Self::deny(
                "ddl",
                &format!(
                    "DDL is not allowed for read-select intent: {}",
                    keyword.to_lowercase()
                ),
            );
        }

        if let Some(keyword) = Self::find_keyword(&analysis.tokens, WRITE_KEYWORDS) {
            return Self::deny(
                &format!("write-{}", keyword.to_lowercase()),
                &format!(
                    "DML write operation is not allowed for read-select intent: {}",
                    keyword.to_lowercase()
                ),
            );
        }

        if let Some(keyword) = Self::find_keyword(&analysis.tokens, ADMIN_KEYWORDS) {
            return Self::deny(
                "admin",
                &format!(
                    "Administrative SQL is not allowed for read-select intent: {}",
                    keyword.to_lowercase()
                ),
            );
        }

        if let Some(keyword) = Self::find_keyword(&analysis.tokens, SET_OPERATION_KEYWORDS) {
            return Self::deny(
                "set-operation",
                &format!(
                    "Set operations are not allowed for read-select intent: {}",
                    keyword.to_lowercase()
                ),
            );
        }

        if Self::find_top_level_keyword(&analysis.tokens, &["FROM"]).is_none() {
            return Self::deny("invalid", "SELECT statements must include a FROM clause");
        }

        if let Some(decision) = Self::evaluate_subquery_policy(&analysis, subquery_policy) {
            return decision;
        }

        match analysis.main_statement.as_deref() {
            Some("SELECT") => Self::allow("read-select"),
            Some(keyword) => Self::deny(
                &keyword.to_lowercase(),
                &format!(
                    "Tool intent read-select only allows SELECT statements, found {}",
                    keyword.to_lowercase()
                ),
            ),
            None => Self::deny("unknown", "Unable to classify SQL statement intent"),
        }
    }

    fn find_keyword(tokens: &[Token], keywords: &'static [&'static str]) -> Option<&'static str> {
        tokens.iter().find_map(|token| {
            keywords
                .iter()
                .copied()
                .find(|keyword| token.word == *keyword)
        })
    }

    fn find_top_level_keyword(
        tokens: &[Token],
        keywords: &'static [&'static str],
    ) -> Option<&'static str> {
        tokens
            .iter()
            .filter(|token| token.depth == 0)
            .find_map(|token| {
                keywords
                    .iter()
                    .copied()
                    .find(|keyword| token.word == *keyword)
            })
    }

    fn detect_injection_heuristics(normalized_query: &str) -> Option<String> {
        if normalized_query.contains(" OR '") && normalized_query.contains("'1'='1") {
            return Some("Suspected boolean-based SQL injection detected".to_string());
        }

        if normalized_query.contains(" OR \"") && normalized_query.contains("\"1\"=\"1") {
            return Some("Suspected boolean-based SQL injection detected".to_string());
        }

        if normalized_query.contains("SLEEP(") || normalized_query.contains("WAITFOR") {
            return Some("Time-based SQL injection pattern detected".to_string());
        }

        None
    }

    fn evaluate_subquery_policy(
        analysis: &SqlAnalysis,
        subquery_policy: &SubqueryPolicy,
    ) -> Option<PolicyDecision> {
        if !analysis.has_nested_select {
            return None;
        }

        match subquery_policy {
            SubqueryPolicy::DenyAll => Some(Self::deny(
                "subquery",
                "Nested subquery is not allowed for read-select intent",
            )),
            SubqueryPolicy::AllowListed(allowed_shapes) => {
                if analysis.nested_select_tables.is_empty() {
                    return Some(Self::deny(
                        "subquery",
                        "Nested subquery could not be classified against the tool allow-list",
                    ));
                }

                let unauthorized_table = analysis.nested_select_tables.iter().find(|table| {
                    !allowed_shapes
                        .iter()
                        .any(|shape| shape.table.eq_ignore_ascii_case(table))
                });

                unauthorized_table.map(|table| {
                    Self::deny(
                        "subquery",
                        &format!(
                            "Nested subquery references table '{}' which is not allow-listed for this tool",
                            table
                        ),
                    )
                })
            }
        }
    }

    fn allow(classification: &str) -> PolicyDecision {
        PolicyDecision {
            allowed: true,
            classification: classification.to_string(),
            reason: None,
        }
    }

    fn deny(classification: &str, reason: &str) -> PolicyDecision {
        PolicyDecision {
            allowed: false,
            classification: classification.to_string(),
            reason: Some(reason.to_string()),
        }
    }
}

impl SqlAnalysis {
    fn analyze(query: &str) -> Self {
        let lex = lex_sql(query);
        let main_statement = classify_main_statement(&lex.tokens).map(str::to_string);
        let has_nested_select = lex
            .tokens
            .iter()
            .any(|token| token.depth > 0 && token.word == "SELECT");
        let nested_select_tables = extract_nested_select_tables(&lex.tokens);

        Self {
            normalized_query: query.to_uppercase(),
            tokens: lex.tokens,
            main_statement,
            has_nested_select,
            nested_select_tables,
            has_line_comment: lex.has_line_comment,
            has_block_comment: lex.has_block_comment,
            has_semicolon: lex.has_semicolon,
        }
    }
}

fn classify_intent(intent: ToolIntent) -> &'static str {
    match intent {
        ToolIntent::ReadSelect => "read-select",
        ToolIntent::WriteInsert => "write-insert",
        ToolIntent::WriteUpdate => "write-update",
        ToolIntent::WriteDelete => "write-delete",
        ToolIntent::Admin => "admin",
    }
}

fn default_tool_name(intent: ToolIntent) -> &'static str {
    match intent {
        ToolIntent::ReadSelect => "default-read-select",
        ToolIntent::WriteInsert => "default-write-insert",
        ToolIntent::WriteUpdate => "default-write-update",
        ToolIntent::WriteDelete => "default-write-delete",
        ToolIntent::Admin => "default-admin",
    }
}

fn classify_main_statement(tokens: &[Token]) -> Option<&str> {
    tokens
        .iter()
        .filter(|token| token.depth == 0)
        .find_map(|token| {
            STATEMENT_KEYWORDS
                .iter()
                .copied()
                .find(|keyword| token.word == *keyword)
        })
}

fn extract_nested_select_tables(tokens: &[Token]) -> Vec<String> {
    let mut tables = Vec::new();

    for (index, token) in tokens.iter().enumerate() {
        let is_nested_relation_keyword =
            token.depth > 0 && (token.word == "FROM" || token.word == "JOIN");
        if !is_nested_relation_keyword {
            continue;
        }

        if let Some(table_name) = extract_relation_name(&tokens[index + 1..], token.depth) {
            if !tables.iter().any(|existing| existing == &table_name) {
                tables.push(table_name);
            }
        }
    }

    tables
}

fn extract_relation_name(tokens: &[Token], depth: usize) -> Option<String> {
    let mut relation_segments = Vec::new();
    let mut saw_relation_start = false;
    let mut expect_qualified_segment = false;

    for token in tokens {
        if token.depth != depth {
            if saw_relation_start {
                break;
            }
            continue;
        }

        if token.word == "." {
            if !saw_relation_start {
                continue;
            }

            expect_qualified_segment = true;
            continue;
        }

        if is_reserved_relation_token(&token.word) {
            if !saw_relation_start {
                continue;
            }
            break;
        }

        if !saw_relation_start || expect_qualified_segment {
            relation_segments.push(token.word.to_lowercase());
            saw_relation_start = true;
            expect_qualified_segment = false;
            continue;
        }

        break;
    }

    relation_segments.pop()
}

fn is_reserved_relation_token(token: &str) -> bool {
    matches!(
        token,
        "SELECT"
            | "WHERE"
            | "ON"
            | "GROUP"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "UNION"
            | "INTERSECT"
            | "EXCEPT"
    )
}

fn lex_sql(query: &str) -> LexAnalysis {
    let mut analysis = LexAnalysis::default();
    let chars: Vec<char> = query.chars().collect();
    let mut index = 0;
    let mut depth = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        let next = chars.get(index + 1).copied();

        if ch == '-' && next == Some('-') {
            analysis.has_line_comment = true;
            index = skip_line_comment(&chars, index + 2);
            continue;
        }

        if ch == '/' && next == Some('*') {
            analysis.has_block_comment = true;
            index = skip_block_comment(&chars, index + 2);
            continue;
        }

        if ch == '\'' {
            index = skip_single_quoted_literal(&chars, index + 1);
            continue;
        }

        if ch == '"' || ch == '`' {
            let (identifier, next_index) = read_quoted_identifier(&chars, index + 1, ch);
            push_identifier_token(&mut analysis, identifier, depth);
            index = next_index;
            continue;
        }

        if ch == '[' {
            let (identifier, next_index) = read_bracket_identifier(&chars, index + 1);
            push_identifier_token(&mut analysis, identifier, depth);
            index = next_index;
            continue;
        }

        match ch {
            ';' => {
                analysis.has_semicolon = true;
                index += 1;
            }
            '.' => {
                analysis.tokens.push(Token {
                    word: ".".to_string(),
                    depth,
                });
                index += 1;
            }
            '(' => {
                depth += 1;
                index += 1;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            _ if is_word_start(ch) => {
                let (word, next_index) = read_word(&chars, index);
                analysis.tokens.push(Token { word, depth });
                index = next_index;
            }
            _ => {
                index += 1;
            }
        }
    }

    analysis
}

fn skip_line_comment(chars: &[char], mut index: usize) -> usize {
    while index < chars.len() && chars[index] != '\n' {
        index += 1;
    }

    index
}

fn skip_block_comment(chars: &[char], mut index: usize) -> usize {
    while index + 1 < chars.len() {
        if chars[index] == '*' && chars[index + 1] == '/' {
            return index + 2;
        }
        index += 1;
    }

    chars.len()
}

fn skip_single_quoted_literal(chars: &[char], mut index: usize) -> usize {
    while index < chars.len() {
        if chars[index] == '\'' {
            if chars.get(index + 1) == Some(&'\'') {
                index += 2;
                continue;
            }

            return index + 1;
        }

        index += 1;
    }

    chars.len()
}

fn read_quoted_identifier(chars: &[char], mut index: usize, quote: char) -> (String, usize) {
    let mut identifier = String::new();

    while index < chars.len() {
        if chars[index] == quote {
            if chars.get(index + 1) == Some(&quote) {
                identifier.push(quote);
                index += 2;
                continue;
            }

            return (identifier, index + 1);
        }

        identifier.push(chars[index]);
        index += 1;
    }

    (identifier, chars.len())
}

fn read_bracket_identifier(chars: &[char], mut index: usize) -> (String, usize) {
    let mut identifier = String::new();

    while index < chars.len() {
        if chars[index] == ']' {
            if chars.get(index + 1) == Some(&']') {
                identifier.push(']');
                index += 2;
                continue;
            }

            return (identifier, index + 1);
        }

        identifier.push(chars[index]);
        index += 1;
    }

    (identifier, chars.len())
}

fn push_identifier_token(analysis: &mut LexAnalysis, identifier: String, depth: usize) {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return;
    }

    analysis.tokens.push(Token {
        word: trimmed.to_uppercase(),
        depth,
    });
}

fn is_word_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn read_word(chars: &[char], start: usize) -> (String, usize) {
    let mut index = start + 1;

    while index < chars.len() && (chars[index].is_ascii_alphanumeric() || chars[index] == '_') {
        index += 1;
    }

    let word = chars[start..index]
        .iter()
        .collect::<String>()
        .to_uppercase();
    (word, index)
}
