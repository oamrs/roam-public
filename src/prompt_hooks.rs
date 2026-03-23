use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptHookUpsertRequest {
    pub name: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub priority: i32,
    pub selector_key: Option<String>,
    pub markdown_template: String,
    pub matching_rules_yaml: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptHookResolveRequest {
    pub explicit_hook_id: Option<String>,
    pub explicit_selector_key: Option<String>,
    #[serde(default)]
    pub request_context: PromptHookRequestContext,
    #[serde(default)]
    pub schema_context: PromptHookSchemaContext,
    #[serde(default)]
    pub additional_variables: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptHookPreviewRequest {
    pub markdown_template: String,
    pub matching_rules_yaml: Option<String>,
    #[serde(default)]
    pub resolve_request: PromptHookResolveRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptHookRequestContext {
    pub user_id: Option<String>,
    pub organization_id: Option<String>,
    pub tool_name: Option<String>,
    pub session_id: Option<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub grants: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PromptHookSchemaContext {
    pub database_id: Option<String>,
    #[serde(default)]
    pub table_names: Vec<String>,
    #[serde(default)]
    pub domain_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct PromptHookMatchRules {
    #[serde(default)]
    pub request: PromptHookRequestMatchRules,
    #[serde(default)]
    pub schema: PromptHookSchemaMatchRules,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct PromptHookRequestMatchRules {
    #[serde(default)]
    pub organization_ids: Vec<String>,
    #[serde(default)]
    pub user_ids: Vec<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default)]
    pub grants: Vec<String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct PromptHookSchemaMatchRules {
    #[serde(default)]
    pub database_ids: Vec<String>,
    #[serde(default)]
    pub table_names: Vec<String>,
    #[serde(default)]
    pub domain_tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptHookMatchPreview {
    pub matches_context: bool,
    pub rendered_prompt: String,
    pub variables: BTreeMap<String, String>,
    pub parsed_rules: PromptHookMatchRules,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PromptHookResolution {
    pub selected_hook_id: String,
    pub selected_hook_name: String,
    pub rendered_prompt: String,
    pub selection_reason: String,
    pub matched_hook_ids: Vec<String>,
    pub variables: BTreeMap<String, String>,
}

const NO_MATCHING_PROMPT_HOOK_ERROR: &str =
    "No enabled prompt hook matched the supplied request and schema context";

pub trait PromptHookDefinition {
    fn id(&self) -> &str;
    fn name(&self) -> &str;
    fn enabled(&self) -> bool;
    fn priority(&self) -> i32;
    fn selector_key(&self) -> Option<&str>;
    fn markdown_template(&self) -> &str;
    fn matching_rules_yaml(&self) -> Option<&str>;
}

pub fn resolve_optional_prompt_hook<T>(
    hooks: &[T],
    request: &PromptHookResolveRequest,
) -> Result<Option<PromptHookResolution>, String>
where
    T: PromptHookDefinition,
{
    match resolve_prompt_hook(hooks, request) {
        Ok(resolution) => Ok(Some(resolution)),
        Err(error)
            if !has_explicit_prompt_hook_selection(request)
                && error == NO_MATCHING_PROMPT_HOOK_ERROR =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[async_trait]
pub trait PromptHookResolver: Send + Sync {
    async fn resolve(
        &self,
        request: &PromptHookResolveRequest,
    ) -> Result<Option<PromptHookResolution>, String>;
}

#[derive(Debug, Clone)]
pub struct StaticPromptHookResolver<T> {
    hooks: Vec<T>,
}

impl<T> StaticPromptHookResolver<T> {
    pub fn new(hooks: Vec<T>) -> Self {
        Self { hooks }
    }
}

#[async_trait]
impl<T> PromptHookResolver for StaticPromptHookResolver<T>
where
    T: PromptHookDefinition + Send + Sync,
{
    async fn resolve(
        &self,
        request: &PromptHookResolveRequest,
    ) -> Result<Option<PromptHookResolution>, String> {
        resolve_optional_prompt_hook(&self.hooks, request)
    }
}

pub fn validate_upsert_request(request: &PromptHookUpsertRequest) -> Result<(), String> {
    if request.name.trim().is_empty() {
        return Err("Prompt hook name is required".to_string());
    }

    if request.markdown_template.trim().is_empty() {
        return Err("Prompt hook markdown_template is required".to_string());
    }

    parse_matching_rules_yaml(request.matching_rules_yaml.as_deref())?;
    let variables = build_template_variables(&PromptHookResolveRequest::default());
    render_template(&request.markdown_template, &variables)?;
    Ok(())
}

pub fn parse_matching_rules_yaml(yaml: Option<&str>) -> Result<PromptHookMatchRules, String> {
    let Some(yaml) = yaml else {
        return Ok(PromptHookMatchRules::default());
    };

    let trimmed = yaml.trim();
    if trimmed.is_empty() {
        return Ok(PromptHookMatchRules::default());
    }

    serde_yaml::from_str(trimmed).map_err(|err| format!("Invalid matching_rules_yaml: {err}"))
}

pub fn preview_prompt_hook(
    request: &PromptHookPreviewRequest,
) -> Result<PromptHookMatchPreview, String> {
    let parsed_rules = parse_matching_rules_yaml(request.matching_rules_yaml.as_deref())?;
    let variables = build_template_variables(&request.resolve_request);
    let rendered_prompt = render_template(&request.markdown_template, &variables)?;
    let matches_context = matches_rules(&parsed_rules, &request.resolve_request);

    Ok(PromptHookMatchPreview {
        matches_context,
        rendered_prompt,
        variables,
        parsed_rules,
    })
}

pub fn resolve_prompt_hook<T: PromptHookDefinition>(
    hooks: &[T],
    request: &PromptHookResolveRequest,
) -> Result<PromptHookResolution, String> {
    let enabled_hooks: Vec<&T> = hooks.iter().filter(|hook| hook.enabled()).collect();

    if let Some(explicit_hook_id) = request.explicit_hook_id.as_deref() {
        let hook = enabled_hooks
            .iter()
            .find(|hook| hook.id() == explicit_hook_id)
            .ok_or_else(|| {
                format!("No enabled prompt hook found for explicit_hook_id '{explicit_hook_id}'")
            })?;

        return build_resolution(
            *hook,
            request,
            "explicit_hook_id".to_string(),
            vec![hook.id().to_string()],
        );
    }

    if let Some(explicit_selector_key) = request.explicit_selector_key.as_deref() {
        let hook = enabled_hooks
            .iter()
            .find(|hook| hook.selector_key() == Some(explicit_selector_key))
            .ok_or_else(|| format!("No enabled prompt hook found for explicit_selector_key '{explicit_selector_key}'"))?;

        return build_resolution(
            *hook,
            request,
            "explicit_selector_key".to_string(),
            vec![hook.id().to_string()],
        );
    }

    let mut matched = Vec::new();
    for hook in enabled_hooks {
        let rules = parse_matching_rules_yaml(hook.matching_rules_yaml())?;
        if matches_rules(&rules, request) {
            matched.push((hook, rules));
        }
    }

    if matched.is_empty() {
        return Err(NO_MATCHING_PROMPT_HOOK_ERROR.to_string());
    }

    matched.sort_by(|(left, _), (right, _)| {
        right
            .priority()
            .cmp(&left.priority())
            .then_with(|| left.name().cmp(right.name()))
    });

    if matched.len() > 1 && matched[0].0.priority() == matched[1].0.priority() {
        return Err(format!(
            "Prompt hook resolution is ambiguous between '{}' and '{}' at priority {}",
            matched[0].0.name(),
            matched[1].0.name(),
            matched[0].0.priority()
        ));
    }

    let matched_hook_ids = matched
        .iter()
        .map(|(hook, _)| hook.id().to_string())
        .collect::<Vec<_>>();

    build_resolution(
        matched[0].0,
        request,
        "priority_match".to_string(),
        matched_hook_ids,
    )
}

fn build_resolution<T: PromptHookDefinition>(
    hook: &T,
    request: &PromptHookResolveRequest,
    selection_reason: String,
    matched_hook_ids: Vec<String>,
) -> Result<PromptHookResolution, String> {
    let variables = build_template_variables(request);
    let rendered_prompt = render_template(hook.markdown_template(), &variables)?;

    Ok(PromptHookResolution {
        selected_hook_id: hook.id().to_string(),
        selected_hook_name: hook.name().to_string(),
        rendered_prompt,
        selection_reason,
        matched_hook_ids,
        variables,
    })
}

fn has_explicit_prompt_hook_selection(request: &PromptHookResolveRequest) -> bool {
    request.explicit_hook_id.is_some() || request.explicit_selector_key.is_some()
}

fn matches_rules(rules: &PromptHookMatchRules, request: &PromptHookResolveRequest) -> bool {
    matches_optional_value(
        &rules.request.organization_ids,
        request.request_context.organization_id.as_deref(),
    ) && matches_optional_value(
        &rules.request.user_ids,
        request.request_context.user_id.as_deref(),
    ) && matches_optional_value(
        &rules.request.tools,
        request.request_context.tool_name.as_deref(),
    ) && matches_optional_value(
        &rules.request.session_ids,
        request.request_context.session_id.as_deref(),
    ) && matches_all_map_entries(&rules.request.headers, &request.request_context.headers)
        && matches_any_values(&rules.request.grants, &request.request_context.grants)
        && matches_optional_value(
            &rules.schema.database_ids,
            request.schema_context.database_id.as_deref(),
        )
        && matches_any_values(
            &rules.schema.table_names,
            &request.schema_context.table_names,
        )
        && matches_any_values(
            &rules.schema.domain_tags,
            &request.schema_context.domain_tags,
        )
}

fn matches_optional_value(expected: &[String], actual: Option<&str>) -> bool {
    if expected.is_empty() {
        return true;
    }

    actual
        .map(|value| contains_case_insensitive(expected, value))
        .unwrap_or(false)
}

fn matches_any_values(expected: &[String], actual: &[String]) -> bool {
    if expected.is_empty() {
        return true;
    }

    actual
        .iter()
        .any(|value| contains_case_insensitive(expected, value))
}

fn matches_all_map_entries(
    expected: &BTreeMap<String, String>,
    actual: &BTreeMap<String, String>,
) -> bool {
    expected.iter().all(|(expected_key, expected_value)| {
        actual
            .iter()
            .find(|(actual_key, _)| actual_key.eq_ignore_ascii_case(expected_key))
            .map(|(_, actual_value)| actual_value.eq_ignore_ascii_case(expected_value))
            .unwrap_or(false)
    })
}

fn contains_case_insensitive(values: &[String], target: &str) -> bool {
    values
        .iter()
        .any(|value| value.eq_ignore_ascii_case(target))
}

pub fn build_template_variables(request: &PromptHookResolveRequest) -> BTreeMap<String, String> {
    let mut variables = BTreeMap::new();

    insert_optional(
        &mut variables,
        "user_id",
        request.request_context.user_id.as_deref(),
    );
    insert_optional(
        &mut variables,
        "organization_id",
        request.request_context.organization_id.as_deref(),
    );
    insert_optional(
        &mut variables,
        "tool_name",
        request.request_context.tool_name.as_deref(),
    );
    insert_optional(
        &mut variables,
        "session_id",
        request.request_context.session_id.as_deref(),
    );
    insert_optional(
        &mut variables,
        "schema_database_id",
        request.schema_context.database_id.as_deref(),
    );

    variables.insert(
        "schema_table_names_csv".to_string(),
        request.schema_context.table_names.join(", "),
    );
    variables.insert(
        "schema_domain_tags_csv".to_string(),
        request.schema_context.domain_tags.join(", "),
    );
    variables.insert(
        "grant_names_csv".to_string(),
        request.request_context.grants.join(", "),
    );

    for (key, value) in &request.request_context.headers {
        variables.insert(normalize_header_variable_name(key), value.clone());
    }

    for (key, value) in &request.additional_variables {
        variables.insert(key.clone(), value.clone());
    }

    variables
}

pub fn render_template(
    markdown_template: &str,
    variables: &BTreeMap<String, String>,
) -> Result<String, String> {
    let mut rendered = String::with_capacity(markdown_template.len());
    let mut cursor = 0;

    while let Some(start_offset) = markdown_template[cursor..].find("{{") {
        let start = cursor + start_offset;
        rendered.push_str(&markdown_template[cursor..start]);
        let after_open = start + 2;
        let end_offset = markdown_template[after_open..]
            .find("}}")
            .ok_or_else(|| "Unclosed template variable in markdown_template".to_string())?;
        let end = after_open + end_offset;
        let variable_name = markdown_template[after_open..end].trim();

        if variable_name.is_empty() {
            return Err("Template variables cannot be empty".to_string());
        }

        let value = variables
            .get(variable_name)
            .ok_or_else(|| format!("Unknown template variable '{{{{{variable_name}}}}}'"))?;
        rendered.push_str(value);
        cursor = end + 2;
    }

    rendered.push_str(&markdown_template[cursor..]);
    Ok(rendered)
}

fn insert_optional(target: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    target.insert(key.to_string(), value.unwrap_or_default().to_string());
}

fn normalize_header_variable_name(header_name: &str) -> String {
    let normalized = header_name
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    format!("header_{normalized}")
}
