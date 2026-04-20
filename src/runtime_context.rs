use crate::policy_engine::{
    AuthorizationContext, PolicyContext, SubqueryPolicy, ToolContract, ToolIntent,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tonic::metadata::MetadataMap;

const SESSION_ID_HEADER: &str = "x-roam-session-id";
const USER_ID_HEADER: &str = "x-roam-user-id";
const ORGANIZATION_ID_HEADER: &str = "x-roam-organization-id";
const TOOL_NAME_HEADER: &str = "x-roam-tool-name";
const TOOL_INTENT_HEADER: &str = "x-roam-tool-intent";
const GRANTS_HEADER: &str = "x-roam-grants";
const RUNTIME_AUGMENTATION_ID_HEADER: &str = "x-roam-runtime-augmentation-id";
const RUNTIME_AUGMENTATION_KEY_HEADER: &str = "x-roam-runtime-augmentation-key";
const DOMAIN_TAGS_HEADER: &str = "x-roam-domain-tags";
const TABLE_NAMES_HEADER: &str = "x-roam-table-names";
const PLAN_ID_HEADER: &str = "x-roam-plan-id";
const STEP_INDEX_HEADER: &str = "x-roam-step-index";

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
    pub runtime_augmentation_id: Option<String>,
    pub runtime_augmentation_key: Option<String>,
    pub domain_tags: Vec<String>,
    pub table_names: Vec<String>,
    /// Identifies the multi-step plan this query belongs to, if any.
    pub plan_id: Option<String>,
    /// Zero-based index of the step within the plan.
    pub step_index: Option<u32>,
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
            runtime_augmentation_id: get_ascii_metadata(metadata, RUNTIME_AUGMENTATION_ID_HEADER),
            runtime_augmentation_key: get_ascii_metadata(metadata, RUNTIME_AUGMENTATION_KEY_HEADER),
            domain_tags: get_csv_metadata(metadata, DOMAIN_TAGS_HEADER),
            table_names: get_csv_metadata(metadata, TABLE_NAMES_HEADER),
            plan_id: get_ascii_metadata(metadata, PLAN_ID_HEADER),
            step_index: get_ascii_metadata(metadata, STEP_INDEX_HEADER)
                .and_then(|v| v.trim().parse::<u32>().ok()),
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
            || self.runtime_augmentation_id.is_some()
            || self.runtime_augmentation_key.is_some()
            || !self.domain_tags.is_empty()
            || !self.table_names.is_empty()
            || self.plan_id.is_some()
            || self.step_index.is_some()
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
            "runtime_augmentation_id",
            self.runtime_augmentation_id.as_ref(),
        );
        insert_optional(
            &mut metadata,
            "runtime_augmentation_key",
            self.runtime_augmentation_key.as_ref(),
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
        insert_optional(&mut metadata, "plan_id", self.plan_id.as_ref());
        if let Some(idx) = self.step_index {
            metadata.insert("step_index".to_string(), idx.to_string());
        }

        metadata
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
