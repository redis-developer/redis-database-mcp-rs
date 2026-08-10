//! Optional RedisJSON document operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    CallToolResult, McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    ToolState, destructive_annotations, module_command, output_schema, read_annotations,
    write_annotations,
};
use crate::{AccessMode, RedisModule, RedisValue};

fn default_root_path() -> String {
    "$".to_string()
}

fn validate_path(path: &str) -> tower_mcp::Result<()> {
    if path.trim().is_empty() {
        Err(tower_mcp::Error::tool("path must not be empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonPathInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the document root.
    #[serde(default = "default_root_path")]
    path: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonGetOutput {
    key: String,
    path: String,
    exists: bool,
    value: Option<JsonValue>,
}

fn json_get_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_get")
        .title("Get Redis JSON")
        .description("Read the JSON value at a path. Requires RedisJSON; the default path is `$`.")
        .output_schema(output_schema::<JsonGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                validate_path(&input.path)?;
                let mut command = module_command(
                    "redis_json_get",
                    AccessMode::ReadOnly,
                    RedisModule::Json,
                    "JSON.GET",
                );
                command.arg(input.key.as_str()).arg(input.path.as_str());
                let raw: Option<Vec<u8>> = state.query(command, "JSON.GET failed").await?;
                let value = raw
                    .map(|raw| {
                        serde_json::from_slice(&raw).map_err(|error| {
                            tower_mcp::Error::tool(format!(
                                "JSON.GET returned invalid JSON for '{}': {error}",
                                input.key
                            ))
                        })
                    })
                    .transpose()?;
                CallToolResult::from_serialize(&JsonGetOutput {
                    key: input.key,
                    path: input.path,
                    exists: value.is_some(),
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonTypeOutput {
    key: String,
    path: String,
    exists: bool,
    types: Vec<String>,
}

fn json_types(value: RedisValue) -> tower_mcp::Result<Vec<String>> {
    fn one(value: RedisValue) -> tower_mcp::Result<String> {
        match value {
            RedisValue::BulkString(value) => String::from_utf8(value)
                .map_err(|_| tower_mcp::Error::tool("JSON.TYPE returned a non-UTF-8 type")),
            RedisValue::SimpleString(value) => Ok(value),
            other => Err(tower_mcp::Error::tool(format!(
                "JSON.TYPE returned an unexpected value: {other:?}"
            ))),
        }
    }

    match value {
        RedisValue::Nil => Ok(Vec::new()),
        RedisValue::Array(values) | RedisValue::Set(values) => values
            .into_iter()
            .filter(|value| !matches!(value, RedisValue::Nil))
            .map(one)
            .collect(),
        value => one(value).map(|value| vec![value]),
    }
}

fn json_type_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_type")
        .title("Get Redis JSON Type")
        .description(
            "Read JSON value types at a path. Requires RedisJSON; JSONPath may match multiple values.",
        )
        .output_schema(output_schema::<JsonTypeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                validate_path(&input.path)?;
                let mut command = module_command(
                    "redis_json_type",
                    AccessMode::ReadOnly,
                    RedisModule::Json,
                    "JSON.TYPE",
                );
                command.arg(input.key.as_str()).arg(input.path.as_str());
                let value = state.raw(command, "JSON.TYPE failed").await?;
                let types = json_types(value)?;
                CallToolResult::from_serialize(&JsonTypeOutput {
                    key: input.key,
                    path: input.path,
                    exists: !types.is_empty(),
                    types,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonSetInput {
    /// Redis key to create or update.
    key: String,
    /// JSONPath expression. Defaults to the document root.
    #[serde(default = "default_root_path")]
    path: String,
    /// JSON value. Pass structured JSON directly rather than a JSON-encoded string.
    value: JsonValue,
    /// Store only when the key or path does not exist.
    #[serde(default)]
    nx: bool,
    /// Store only when the key or path already exists.
    #[serde(default)]
    xx: bool,
}

impl JsonSetInput {
    fn validate(&self) -> tower_mcp::Result<()> {
        validate_path(&self.path)?;
        if self.nx && self.xx {
            Err(tower_mcp::Error::tool("nx and xx are mutually exclusive"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonSetOutput {
    key: String,
    path: String,
    stored: bool,
}

fn json_set_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_set")
        .title("Set Redis JSON")
        .description(
            "Set a structured JSON value at a path. Requires RedisJSON; NX and XX are optional conditions.",
        )
        .output_schema(output_schema::<JsonSetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonSetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_json_set")?;
                input.validate()?;
                let value = serde_json::to_vec(&input.value).map_err(|error| {
                    tower_mcp::Error::tool(format!("could not encode JSON value: {error}"))
                })?;
                let mut command = module_command(
                    "redis_json_set",
                    AccessMode::ReadWrite,
                    RedisModule::Json,
                    "JSON.SET",
                );
                command
                    .arg(input.key.as_str())
                    .arg(input.path.as_str())
                    .arg(value);
                if input.nx {
                    command.arg("NX");
                }
                if input.xx {
                    command.arg("XX");
                }
                let stored: Option<String> = state.query(command, "JSON.SET failed").await?;
                CallToolResult::from_serialize(&JsonSetOutput {
                    key: input.key,
                    path: input.path,
                    stored: stored.is_some(),
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonDelOutput {
    key: String,
    path: String,
    deleted: u64,
}

fn json_del_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_del")
        .title("Delete Redis JSON")
        .description(
            "Delete JSON values at a path. Deleting the root removes the key and requires full access.",
        )
        .output_schema(output_schema::<JsonDelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                state.require(AccessMode::Full, "redis_json_del")?;
                validate_path(&input.path)?;
                let mut command = module_command(
                    "redis_json_del",
                    AccessMode::Full,
                    RedisModule::Json,
                    "JSON.DEL",
                );
                command.arg(input.key.as_str()).arg(input.path.as_str());
                let deleted = state.query(command, "JSON.DEL failed").await?;
                CallToolResult::from_serialize(&JsonDelOutput {
                    key: input.key,
                    path: input.path,
                    deleted,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(json_get_tool(state.clone()));
    router.tool(json_type_tool(state))
}

pub(super) fn add_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(json_set_tool(state))
}

pub(super) fn add_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(json_del_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_set_rejects_conflicting_conditions() {
        let input = JsonSetInput {
            key: "doc:1".into(),
            path: "$".into(),
            value: serde_json::json!({"name": "Ada"}),
            nx: true,
            xx: true,
        };
        assert!(input.validate().is_err());
    }

    #[test]
    fn json_type_normalizes_legacy_and_jsonpath_shapes() {
        assert_eq!(
            json_types(RedisValue::BulkString(b"object".to_vec())).unwrap(),
            vec!["object"]
        );
        assert_eq!(
            json_types(RedisValue::Array(vec![
                RedisValue::BulkString(b"string".to_vec()),
                RedisValue::Nil,
            ]))
            .unwrap(),
            vec!["string"]
        );
    }
}
