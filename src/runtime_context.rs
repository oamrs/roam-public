use crate::policy_engine::{
    AuthorizationContext, PolicyContext, SubqueryPolicy, ToolContract, ToolIntent,
};
use crate::prompt_hooks::{
    PromptHookRequestContext, PromptHookResolution, PromptHookResolveRequest,
    PromptHookSchemaContext,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashMap;
use tonic::metadata::MetadataMap;

const SESSION_ID_HEADER: &str = "x-roam-session-id";
const USER_ID_HEADER: &str = "x-roam-user-id";
const ORGANIZATION_ID_HEADER: &str = "x-roam-organization-id";
const TOOL_NAME_HEADER: &str = "x-roam-tool-name";
const TOOL_INTENT_HEADER: &str = "x-roam-tool-intent";
const GRANTS_HEADER: &str = "x-roam-grants";
const PROMPT_HOOK_ID_HEADER: &str = "x-roam-prompt-hook-id";
const PROMPT_SELECTOR_KEY_HEADER: &str = "x-roam-prompt-selector-key";
const DOMAIN_TAGS_HEADER: &str = "x-roam-domain-tags";
const TABLE_NAMES_HEADER: &str = "x-roam-table-names";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRuntimeContext {
    pub session_id: Option<String>,
    pub agent_id: Option<String>,
    pub agent_version: Option<String>,
    pub schema_mode: Option<String>,
    pub user_id: Option<String>,
    pub organization_id: Option<String>,
    pub tool_name: Option<String>,
    pub tool_intent: Option<ToolIntent>,
    pub grants: Vec<String>,
    pub prompt_hook_id: Option<String>,
    pub prompt_selector_key: Option<String>,
    pub domain_tags: Vec<String>,
    pub table_names: Vec<String>,
}

impl QueryRuntimeContext {
    pub fn from_metadata(metadata: &MetadataMap) -> Self {
        Self {
            session_id: get_ascii_metadata(metadata, SESSION_ID_HEADER),
            agent_id: None,
            agent_version: None,
            schema_mode: None,
            user_id: get_ascii_metadata(metadata, USER_ID_HEADER),
            organization_id: get_ascii_metadata(metadata, ORGANIZATION_ID_HEADER),
            tool_name: get_ascii_metadata(metadata, TOOL_NAME_HEADER),
            tool_intent: get_ascii_metadata(metadata, TOOL_INTENT_HEADER)
                .and_then(|value| parse_tool_intent(&value)),
            grants: get_csv_metadata(metadata, GRANTS_HEADER),
            prompt_hook_id: get_ascii_metadata(metadata, PROMPT_HOOK_ID_HEADER),
            prompt_selector_key: get_ascii_metadata(metadata, PROMPT_SELECTOR_KEY_HEADER),
            domain_tags: get_csv_metadata(metadata, DOMAIN_TAGS_HEADER),
            table_names: get_csv_metadata(metadata, TABLE_NAMES_HEADER),
        }
    }

    pub fn has_values(&self) -> bool {
        self.session_id.is_some()
            || self.agent_id.is_some()
            || self.agent_version.is_some()
            || self.schema_mode.is_some()
            || self.user_id.is_some()
            || self.organization_id.is_some()
            || self.tool_name.is_some()
            || self.tool_intent.is_some()
            || !self.grants.is_empty()
            || self.prompt_hook_id.is_some()
            || self.prompt_selector_key.is_some()
            || !self.domain_tags.is_empty()
            || !self.table_names.is_empty()
    }

    pub fn policy_context(&self) -> Option<PolicyContext> {
        if self.tool_name.is_none() && self.tool_intent.is_none() && self.grants.is_empty() {
            return None;
        }

        let intent = self.tool_intent.unwrap_or(ToolIntent::ReadSelect);
        let tool_name = self
            .tool_name
            .clone()
            .unwrap_or_else(|| default_tool_name(intent).to_string());

        Some(PolicyContext {
            tool: ToolContract {
                name: tool_name,
                intent,
                subquery_policy: SubqueryPolicy::DenyAll,
            },
            authorization: AuthorizationContext {
                allowed_intents: vec![intent],
                grants: self.grants.clone(),
            },
        })
    }

    pub fn event_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        insert_optional(&mut metadata, "session_id", self.session_id.as_ref());
        insert_optional(&mut metadata, "agent_id", self.agent_id.as_ref());
        insert_optional(&mut metadata, "agent_version", self.agent_version.as_ref());
        insert_optional(&mut metadata, "schema_mode", self.schema_mode.as_ref());
        insert_optional(&mut metadata, "user_id", self.user_id.as_ref());
        insert_optional(
            &mut metadata,
            "organization_id",
            self.organization_id.as_ref(),
        );
        insert_optional(&mut metadata, "tool_name", self.tool_name.as_ref());
        insert_optional(
            &mut metadata,
            "tool_intent",
            self.tool_intent.as_ref().map(tool_intent_name),
        );
        insert_optional(
            &mut metadata,
            "prompt_hook_id",
            self.prompt_hook_id.as_ref(),
        );
        insert_optional(
            &mut metadata,
            "prompt_selector_key",
            self.prompt_selector_key.as_ref(),
        );

        if !self.grants.is_empty() {
            metadata.insert("grants".to_string(), self.grants.join(","));
        }
        if !self.domain_tags.is_empty() {
            metadata.insert("domain_tags".to_string(), self.domain_tags.join(","));
        }
        if !self.table_names.is_empty() {
            metadata.insert("table_names".to_string(), self.table_names.join(","));
        }

        metadata
    }

    pub fn event_metadata_with_prompt_hook_resolution(
        &self,
        resolution: Option<&PromptHookResolution>,
    ) -> HashMap<String, String> {
        let mut metadata = self.event_metadata();

        if let Some(resolution) = resolution {
            metadata.insert(
                "resolved_prompt_hook_id".to_string(),
                resolution.selected_hook_id.clone(),
            );
            metadata.insert(
                "resolved_prompt_hook_name".to_string(),
                resolution.selected_hook_name.clone(),
            );
            metadata.insert(
                "resolved_prompt_hook_selection_reason".to_string(),
                resolution.selection_reason.clone(),
            );
            metadata.insert(
                "resolved_prompt".to_string(),
                resolution.rendered_prompt.clone(),
            );
            if !resolution.matched_hook_ids.is_empty() {
                metadata.insert(
                    "resolved_prompt_hook_matched_ids".to_string(),
                    resolution.matched_hook_ids.join(","),
                );
            }
        }

        metadata
    }

    pub fn prompt_hook_resolve_request(
        &self,
        db_identifier: Option<&str>,
    ) -> PromptHookResolveRequest {
        let mut additional_variables = BTreeMap::new();

        insert_btree_optional(
            &mut additional_variables,
            "agent_id",
            self.agent_id.as_ref(),
        );
        insert_btree_optional(
            &mut additional_variables,
            "agent_version",
            self.agent_version.as_ref(),
        );
        insert_btree_optional(
            &mut additional_variables,
            "schema_mode",
            self.schema_mode.as_ref(),
        );
        insert_btree_optional(
            &mut additional_variables,
            "tool_intent",
            self.tool_intent.as_ref().map(tool_intent_name),
        );

        PromptHookResolveRequest {
            explicit_hook_id: self.prompt_hook_id.clone(),
            explicit_selector_key: self.prompt_selector_key.clone(),
            request_context: PromptHookRequestContext {
                user_id: self.user_id.clone(),
                organization_id: self.organization_id.clone(),
                tool_name: self.tool_name.clone(),
                session_id: self.session_id.clone(),
                headers: BTreeMap::new(),
                grants: self.grants.clone(),
            },
            schema_context: PromptHookSchemaContext {
                database_id: db_identifier.map(ToOwned::to_owned),
                table_names: self.table_names.clone(),
                domain_tags: self.domain_tags.clone(),
            },
            additional_variables,
        }
    }

    pub fn with_registered_agent(
        mut self,
        agent_id: &str,
        agent_version: &str,
        schema_mode: &str,
    ) -> Self {
        self.agent_id = Some(agent_id.to_string());
        self.agent_version = Some(agent_version.to_string());
        self.schema_mode = Some(schema_mode.to_string());
        self
    }
}

fn get_ascii_metadata(metadata: &MetadataMap, key: &str) -> Option<String> {
    metadata
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn get_csv_metadata(metadata: &MetadataMap, key: &str) -> Vec<String> {
    get_ascii_metadata(metadata, key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn parse_tool_intent(value: &str) -> Option<ToolIntent> {
    match value.trim().to_ascii_lowercase().as_str() {
        "readselect" | "read_select" | "read-select" | "read" => Some(ToolIntent::ReadSelect),
        "writeinsert" | "write_insert" | "write-insert" | "insert" => Some(ToolIntent::WriteInsert),
        "writeupdate" | "write_update" | "write-update" | "update" => Some(ToolIntent::WriteUpdate),
        "writedelete" | "write_delete" | "write-delete" | "delete" => Some(ToolIntent::WriteDelete),
        "admin" => Some(ToolIntent::Admin),
        _ => None,
    }
}

fn tool_intent_name(intent: &ToolIntent) -> &'static str {
    match intent {
        ToolIntent::ReadSelect => "read_select",
        ToolIntent::WriteInsert => "write_insert",
        ToolIntent::WriteUpdate => "write_update",
        ToolIntent::WriteDelete => "write_delete",
        ToolIntent::Admin => "admin",
    }
}

fn default_tool_name(intent: ToolIntent) -> &'static str {
    match intent {
        ToolIntent::ReadSelect => "sql.read_select",
        ToolIntent::WriteInsert => "sql.write_insert",
        ToolIntent::WriteUpdate => "sql.write_update",
        ToolIntent::WriteDelete => "sql.write_delete",
        ToolIntent::Admin => "sql.admin",
    }
}

fn insert_optional<S: Into<String>>(
    metadata: &mut HashMap<String, String>,
    key: &str,
    value: Option<S>,
) {
    if let Some(value) = value {
        metadata.insert(key.to_string(), value.into());
    }
}

fn insert_btree_optional<S: Into<String>>(
    metadata: &mut BTreeMap<String, String>,
    key: &str,
    value: Option<S>,
) {
    if let Some(value) = value {
        metadata.insert(key.to_string(), value.into());
    }
}
