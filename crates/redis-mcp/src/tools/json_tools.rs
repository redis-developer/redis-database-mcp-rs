//! Optional RedisJSON document operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    ToolState, destructive_annotations, module_command, output_schema, read_annotations,
    write_annotations,
};
use crate::{AccessMode, RedisModule, RedisValue};

const DEFAULT_POP_MAX_BYTES: usize = 64 * 1024;

fn default_root_path() -> String {
    "$".to_string()
}

fn default_pop_max_bytes() -> usize {
    DEFAULT_POP_MAX_BYTES
}

fn default_pop_index() -> i64 {
    -1
}

fn validate_path(path: &str) -> tower_mcp::Result<()> {
    if path.trim().is_empty() {
        Err(tower_mcp::Error::tool("path must not be empty"))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum JsonPathMode {
    Enhanced,
    Legacy,
}

fn path_mode(path: &str) -> JsonPathMode {
    if path == "$" || path.starts_with("$.") || path.starts_with("$[") {
        JsonPathMode::Enhanced
    } else {
        JsonPathMode::Legacy
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonPathInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
}

#[derive(Debug)]
struct JsonPathInspection {
    key_existed_before: bool,
    types: Vec<String>,
}

async fn json_key_exists(
    state: &ToolState,
    tool_name: &'static str,
    access: AccessMode,
    key: &str,
) -> tower_mcp::Result<bool> {
    let mut command = module_command(tool_name, access, RedisModule::Json, "EXISTS");
    command.arg(key);
    let exists: u64 = state
        .query(command, "Redis key existence check failed")
        .await?;
    Ok(exists != 0)
}

async fn inspect_json_path(
    state: &ToolState,
    tool_name: &'static str,
    access: AccessMode,
    key: &str,
    path: &str,
) -> tower_mcp::Result<JsonPathInspection> {
    let key_existed_before = json_key_exists(state, tool_name, access, key).await?;
    if !key_existed_before {
        return Ok(JsonPathInspection {
            key_existed_before,
            types: Vec::new(),
        });
    }

    let mut command = module_command(tool_name, access, RedisModule::Json, "JSON.TYPE");
    command.arg(key).arg(path);
    let value = state
        .raw(command, "JSON.TYPE path inspection failed")
        .await?;
    Ok(JsonPathInspection {
        key_existed_before,
        types: json_types(value)?,
    })
}

fn json_entries(value: &JsonValue) -> usize {
    match value {
        JsonValue::Array(values) => values.len() + values.iter().map(json_entries).sum::<usize>(),
        JsonValue::Object(values) => {
            values.len() + values.values().map(json_entries).sum::<usize>()
        }
        _ => 0,
    }
}

fn parsed_match_count(value: &JsonValue, mode: JsonPathMode) -> usize {
    match (mode, value) {
        (JsonPathMode::Enhanced, JsonValue::Array(values)) => values.len(),
        _ => 1,
    }
}

fn parse_json_bytes(raw: &[u8], command: &str, key: &str) -> tower_mcp::Result<JsonValue> {
    serde_json::from_slice(raw).map_err(|error| {
        tower_mcp::Error::tool(format!(
            "{command} returned invalid JSON for '{key}': {error}"
        ))
    })
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonGetOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    exists: bool,
    match_count: usize,
    value: Option<JsonValue>,
}

fn json_get_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_get")
        .title("Get Redis JSON")
        .description(
            "Read a structured JSON value at an enhanced or legacy path. Missing keys and missing paths are reported separately; the configured output budget is enforced.",
        )
        .output_schema(output_schema::<JsonGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                validate_path(&input.path)?;
                let mode = path_mode(&input.path);
                let mut command = module_command(
                    "redis_json_get",
                    AccessMode::ReadOnly,
                    RedisModule::Json,
                    "JSON.GET",
                );
                command.arg(input.key.as_str()).arg(input.path.as_str());
                let raw: Option<Vec<u8>> = state.query(command, "JSON.GET failed").await?;
                let value = raw
                    .as_deref()
                    .map(|raw| parse_json_bytes(raw, "JSON.GET", &input.key))
                    .transpose()?;
                let key_existed_before = if value.is_some() {
                    true
                } else {
                    json_key_exists(
                        &state,
                        "redis_json_get",
                        AccessMode::ReadOnly,
                        &input.key,
                    )
                    .await?
                };
                let match_count = value
                    .as_ref()
                    .map(|value| parsed_match_count(value, mode))
                    .unwrap_or(0);
                let entries = value.as_ref().map(json_entries).unwrap_or(0);
                state.output_collection(
                    &JsonGetOutput {
                        key: input.key,
                        path: input.path,
                        path_mode: mode,
                        key_existed_before,
                        exists: match_count != 0,
                        match_count,
                        value,
                    },
                    entries,
                    "Use a narrower JSONPath expression.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonTypeOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    exists: bool,
    match_count: usize,
    types: Vec<String>,
}

fn redis_string(value: RedisValue, command: &str) -> tower_mcp::Result<String> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => String::from_utf8(value)
            .map_err(|_| tower_mcp::Error::tool(format!("{command} returned non-UTF-8 text"))),
        RedisValue::SimpleString(value) => Ok(value),
        other => Err(tower_mcp::Error::tool(format!(
            "{command} returned an unexpected value: {other:?}"
        ))),
    }
}

fn json_types(value: RedisValue) -> tower_mcp::Result<Vec<String>> {
    match value {
        RedisValue::Nil => Ok(Vec::new()),
        RedisValue::Array(values) | RedisValue::Set(values) => values
            .into_iter()
            .filter(|value| !matches!(value, RedisValue::Nil))
            .map(|value| redis_string(value, "JSON.TYPE"))
            .collect(),
        value => redis_string(value, "JSON.TYPE").map(|value| vec![value]),
    }
}

fn json_type_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_type")
        .title("Get Redis JSON Type")
        .description(
            "Read JSON value types at an enhanced or legacy path. Missing keys and missing paths are reported separately.",
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
                let mode = path_mode(&input.path);
                let key_existed_before = if types.is_empty() {
                    json_key_exists(
                        &state,
                        "redis_json_type",
                        AccessMode::ReadOnly,
                        &input.key,
                    )
                    .await?
                } else {
                    true
                };
                let output = JsonTypeOutput {
                    key: input.key,
                    path: input.path,
                    path_mode: mode,
                    key_existed_before,
                    exists: !types.is_empty(),
                    match_count: types.len(),
                    types,
                };
                state.output_collection(
                    &output,
                    output.types.len(),
                    "Use a narrower JSONPath expression.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonMgetInput {
    /// Redis keys to read, in response order. On Redis Cluster every key must share a hash slot.
    keys: Vec<String>,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonMgetItem {
    key: String,
    key_existed_before: bool,
    exists: bool,
    match_count: usize,
    value: Option<JsonValue>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonMgetOutput {
    path: String,
    path_mode: JsonPathMode,
    values: Vec<JsonMgetItem>,
}

fn json_mget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_mget")
        .title("Get Multiple Redis JSON Values")
        .description(
            "Read one JSON path from 1 to the configured maximum number of keys. Results stay aligned with keys and distinguish missing keys from missing paths. Cluster keys must share a hash slot.",
        )
        .output_schema(output_schema::<JsonMgetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonMgetInput>| async move {
                validate_path(&input.path)?;
                state.validate_requested_entries(input.keys.len(), "keys length")?;
                let mode = path_mode(&input.path);
                let mut command = module_command(
                    "redis_json_mget",
                    AccessMode::ReadOnly,
                    RedisModule::Json,
                    "JSON.MGET",
                );
                for key in &input.keys {
                    command.arg(key.as_str());
                }
                command.arg(input.path.as_str());
                let raw: Vec<Option<Vec<u8>>> = state.query(command, "JSON.MGET failed").await?;
                if raw.len() != input.keys.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "JSON.MGET returned {} values for {} keys",
                        raw.len(),
                        input.keys.len()
                    )));
                }

                let mut entries = input.keys.len();
                let mut values = Vec::with_capacity(raw.len());
                for (key, raw) in input.keys.into_iter().zip(raw) {
                    let value = raw
                        .as_deref()
                        .map(|raw| parse_json_bytes(raw, "JSON.MGET", &key))
                        .transpose()?;
                    let key_existed_before = if value.is_some() {
                        true
                    } else {
                        json_key_exists(
                            &state,
                            "redis_json_mget",
                            AccessMode::ReadOnly,
                            &key,
                        )
                        .await?
                    };
                    let match_count = value
                        .as_ref()
                        .map(|value| parsed_match_count(value, mode))
                        .unwrap_or(0);
                    entries += value.as_ref().map(json_entries).unwrap_or(0);
                    values.push(JsonMgetItem {
                        key,
                        key_existed_before,
                        exists: match_count != 0,
                        match_count,
                        value,
                    });
                }
                state.output_collection(
                    &JsonMgetOutput {
                        path: input.path,
                        path_mode: mode,
                        values,
                    },
                    entries,
                    "Request fewer keys or use a narrower JSONPath expression.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonPathResultsOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    match_count: usize,
    /// JSON types before the operation, aligned with `values`.
    types: Vec<String>,
    /// One result per matched path. `null` means the matched value had the wrong type.
    values: Vec<Option<JsonValue>>,
}

fn redis_scalar_json(value: RedisValue, command: &str) -> tower_mcp::Result<Option<JsonValue>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::Integer(value) => Ok(Some(json!(value))),
        RedisValue::Double(value) => Ok(Some(json!(value))),
        RedisValue::Boolean(value) => Ok(Some(json!(value))),
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => {
            let parsed = serde_json::from_slice(&value).map_err(|error| {
                tower_mcp::Error::tool(format!("{command} returned invalid JSON: {error}"))
            })?;
            Ok((parsed != JsonValue::Null).then_some(parsed))
        }
        RedisValue::SimpleString(value) => {
            let parsed = serde_json::from_str(&value).map_err(|error| {
                tower_mcp::Error::tool(format!("{command} returned invalid JSON: {error}"))
            })?;
            Ok((parsed != JsonValue::Null).then_some(parsed))
        }
        other => Err(tower_mcp::Error::tool(format!(
            "{command} returned an unexpected scalar: {other:?}"
        ))),
    }
}

fn redis_path_results(
    value: RedisValue,
    mode: JsonPathMode,
    command: &str,
) -> tower_mcp::Result<Vec<Option<JsonValue>>> {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => values
            .into_iter()
            .map(|value| redis_scalar_json(value, command))
            .collect(),
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => {
            let parsed: JsonValue = serde_json::from_slice(&value).map_err(|error| {
                tower_mcp::Error::tool(format!("{command} returned invalid JSON: {error}"))
            })?;
            match (mode, parsed) {
                (JsonPathMode::Enhanced, JsonValue::Array(values)) => Ok(values
                    .into_iter()
                    .map(|value| (value != JsonValue::Null).then_some(value))
                    .collect()),
                (_, value) => Ok(vec![(value != JsonValue::Null).then_some(value)]),
            }
        }
        RedisValue::Nil => Ok(vec![None]),
        value => redis_scalar_json(value, command).map(|value| vec![value]),
    }
}

fn compatible_results(types: &[String], accepted: &[&str]) -> Vec<Option<JsonValue>> {
    debug_assert!(
        !types
            .iter()
            .any(|value_type| accepted.contains(&value_type.as_str()))
    );
    types.iter().map(|_| None).collect()
}

fn any_compatible(types: &[String], accepted: &[&str]) -> bool {
    types
        .iter()
        .any(|value_type| accepted.contains(&value_type.as_str()))
}

fn normalize_result_count(
    results: Vec<Option<JsonValue>>,
    inspection: &JsonPathInspection,
    command: &str,
) -> tower_mcp::Result<Vec<Option<JsonValue>>> {
    if results.len() != inspection.types.len() {
        Err(tower_mcp::Error::tool(format!(
            "{command} returned {} results for {} matched paths",
            results.len(),
            inspection.types.len()
        )))
    } else {
        Ok(results)
    }
}

#[derive(Clone, Copy)]
struct JsonPathCommandSpec {
    tool_name: &'static str,
    access: AccessMode,
    command_name: &'static str,
    accepted_types: &'static [&'static str],
}

impl JsonPathCommandSpec {
    const fn new(
        tool_name: &'static str,
        access: AccessMode,
        command_name: &'static str,
        accepted_types: &'static [&'static str],
    ) -> Self {
        Self {
            tool_name,
            access,
            command_name,
            accepted_types,
        }
    }
}

async fn integer_path_command(
    state: &ToolState,
    spec: JsonPathCommandSpec,
    key: String,
    path: String,
    arguments: impl IntoIterator<Item = Vec<u8>>,
) -> tower_mcp::Result<tower_mcp::CallToolResult> {
    validate_path(&path)?;
    let mode = path_mode(&path);
    let inspection = inspect_json_path(state, spec.tool_name, spec.access, &key, &path).await?;
    let values =
        if inspection.types.is_empty() || !any_compatible(&inspection.types, spec.accepted_types) {
            compatible_results(&inspection.types, spec.accepted_types)
        } else if matches!(mode, JsonPathMode::Legacy)
            && !spec.accepted_types.contains(&inspection.types[0].as_str())
        {
            vec![None]
        } else {
            let mut command = module_command(
                spec.tool_name,
                spec.access,
                RedisModule::Json,
                spec.command_name,
            );
            command.arg(key.as_str()).arg(path.as_str());
            for argument in arguments {
                command.arg(argument);
            }
            let raw = state
                .raw(command, &format!("{} failed", spec.command_name))
                .await?;
            normalize_result_count(
                redis_path_results(raw, mode, spec.command_name)?,
                &inspection,
                spec.command_name,
            )?
        };
    let output = JsonPathResultsOutput {
        key,
        path,
        path_mode: mode,
        key_existed_before: inspection.key_existed_before,
        match_count: inspection.types.len(),
        types: inspection.types,
        values,
    };
    state.output_collection(
        &output,
        output.match_count,
        "Use a narrower JSONPath expression.",
    )
}

fn json_strlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_strlen")
        .title("Get Redis JSON String Lengths")
        .description(
            "Return string lengths for every matching enhanced-path value, or the first legacy-path value. Wrong-type matches are explicit null results.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_strlen",
                        AccessMode::ReadOnly,
                        "JSON.STRLEN",
                        &["string"],
                    ),
                    input.key,
                    input.path,
                    [],
                )
                .await
            },
        )
        .build()
}

fn json_objlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_objlen")
        .title("Get Redis JSON Object Lengths")
        .description(
            "Return object key counts for matching enhanced or legacy paths. Wrong-type matches are explicit null results.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_objlen",
                        AccessMode::ReadOnly,
                        "JSON.OBJLEN",
                        &["object"],
                    ),
                    input.key,
                    input.path,
                    [],
                )
                .await
            },
        )
        .build()
}

fn json_arrlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_arrlen")
        .title("Get Redis JSON Array Lengths")
        .description(
            "Return array lengths for matching enhanced or legacy paths. Wrong-type matches are explicit null results.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_arrlen",
                        AccessMode::ReadOnly,
                        "JSON.ARRLEN",
                        &["array"],
                    ),
                    input.key,
                    input.path,
                    [],
                )
                .await
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonObjkeysOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    match_count: usize,
    types: Vec<String>,
    /// One object-key list per matched path. `null` means the match was not an object.
    keys: Vec<Option<Vec<String>>>,
}

fn json_object_keys(
    value: RedisValue,
    mode: JsonPathMode,
) -> tower_mcp::Result<Vec<Option<Vec<String>>>> {
    fn key_list(values: Vec<RedisValue>) -> tower_mcp::Result<Vec<String>> {
        values
            .into_iter()
            .map(|value| redis_string(value, "JSON.OBJKEYS"))
            .collect()
    }

    match (mode, value) {
        (JsonPathMode::Enhanced, RedisValue::Array(values)) => values
            .into_iter()
            .map(|value| match value {
                RedisValue::Nil => Ok(None),
                RedisValue::Array(values) | RedisValue::Set(values) => key_list(values).map(Some),
                other => Err(tower_mcp::Error::tool(format!(
                    "JSON.OBJKEYS returned an unexpected value: {other:?}"
                ))),
            })
            .collect(),
        (JsonPathMode::Legacy, RedisValue::Nil) => Ok(vec![None]),
        (JsonPathMode::Legacy, RedisValue::Array(values) | RedisValue::Set(values)) => {
            key_list(values).map(|values| vec![Some(values)])
        }
        (_, other) => Err(tower_mcp::Error::tool(format!(
            "JSON.OBJKEYS returned an unexpected value: {other:?}"
        ))),
    }
}

fn json_objkeys_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_objkeys")
        .title("Get Redis JSON Object Keys")
        .description(
            "Return object member names for matching paths. Results preserve enhanced-path match boundaries and enforce configured collection and byte budgets.",
        )
        .output_schema(output_schema::<JsonObjkeysOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                validate_path(&input.path)?;
                let mode = path_mode(&input.path);
                let inspection = inspect_json_path(
                    &state,
                    "redis_json_objkeys",
                    AccessMode::ReadOnly,
                    &input.key,
                    &input.path,
                )
                .await?;
                let keys = if inspection.types.is_empty()
                    || !any_compatible(&inspection.types, &["object"])
                {
                    inspection.types.iter().map(|_| None).collect()
                } else if matches!(mode, JsonPathMode::Legacy)
                    && inspection.types[0] != "object"
                {
                    vec![None]
                } else {
                    let mut command = module_command(
                        "redis_json_objkeys",
                        AccessMode::ReadOnly,
                        RedisModule::Json,
                        "JSON.OBJKEYS",
                    );
                    command.arg(input.key.as_str()).arg(input.path.as_str());
                    let raw = state.raw(command, "JSON.OBJKEYS failed").await?;
                    let keys = json_object_keys(raw, mode)?;
                    if keys.len() != inspection.types.len() {
                        return Err(tower_mcp::Error::tool(format!(
                            "JSON.OBJKEYS returned {} results for {} matched paths",
                            keys.len(),
                            inspection.types.len()
                        )));
                    }
                    keys
                };
                let entries = keys
                    .iter()
                    .filter_map(Option::as_ref)
                    .map(Vec::len)
                    .sum::<usize>();
                state.output_collection(
                    &JsonObjkeysOutput {
                        key: input.key,
                        path: input.path,
                        path_mode: mode,
                        key_existed_before: inspection.key_existed_before,
                        match_count: inspection.types.len(),
                        types: inspection.types,
                        keys,
                    },
                    entries,
                    "Use a narrower JSONPath expression; JSON.OBJKEYS has no cursor form.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonSetInput {
    /// Redis key to create or update.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
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
            "Set a structured JSON value at a path. NX and XX are mutually exclusive conditions; a false stored result is a conditional no-op.",
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
                state.output(&JsonSetOutput {
                    key: input.key,
                    path: input.path,
                    stored: stored.is_some(),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonNumberInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// Finite amount to add to every matched number.
    value: f64,
}

fn json_numincrby_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_numincrby")
        .title("Increment Redis JSON Numbers")
        .description(
            "Increment every numeric match by a finite value. Enhanced-path results stay aligned and wrong-type matches are explicit nulls.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonNumberInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_json_numincrby")?;
                if !input.value.is_finite() {
                    return Err(tower_mcp::Error::tool("value must be finite"));
                }
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_numincrby",
                        AccessMode::ReadWrite,
                        "JSON.NUMINCRBY",
                        &["integer", "number"],
                    ),
                    input.key,
                    input.path,
                    [input.value.to_string().into_bytes()],
                )
                .await
            },
        )
        .build()
}

fn json_toggle_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_toggle")
        .title("Toggle Redis JSON Booleans")
        .description(
            "Toggle Boolean values at matching enhanced or legacy paths. Wrong-type matches are explicit null results.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_json_toggle")?;
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_toggle",
                        AccessMode::ReadWrite,
                        "JSON.TOGGLE",
                        &["boolean"],
                    ),
                    input.key,
                    input.path,
                    [],
                )
                .await
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonArrayValuesInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// Structured JSON values to append or insert.
    values: Vec<JsonValue>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonArrayInsertInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// Position before which values are inserted. Negative indexes count from the end.
    index: i64,
    /// Structured JSON values to insert.
    values: Vec<JsonValue>,
}

fn encode_json_values(state: &ToolState, values: &[JsonValue]) -> tower_mcp::Result<Vec<Vec<u8>>> {
    state.validate_requested_entries(values.len(), "values length")?;
    values
        .iter()
        .map(|value| {
            serde_json::to_vec(value).map_err(|error| {
                tower_mcp::Error::tool(format!("could not encode JSON value: {error}"))
            })
        })
        .collect()
}

fn json_arrappend_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_arrappend")
        .title("Append Redis JSON Array Values")
        .description(
            "Append one or more structured JSON values to every matched array and return the new lengths.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonArrayValuesInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_json_arrappend")?;
                let values = encode_json_values(&state, &input.values)?;
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_arrappend",
                        AccessMode::ReadWrite,
                        "JSON.ARRAPPEND",
                        &["array"],
                    ),
                    input.key,
                    input.path,
                    values,
                )
                .await
            },
        )
        .build()
}

fn json_arrinsert_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_arrinsert")
        .title("Insert Redis JSON Array Values")
        .description(
            "Insert one or more structured JSON values before an index in every matched array and return the new lengths.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonArrayInsertInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_json_arrinsert")?;
                let mut values = vec![input.index.to_string().into_bytes()];
                values.extend(encode_json_values(&state, &input.values)?);
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_arrinsert",
                        AccessMode::ReadWrite,
                        "JSON.ARRINSERT",
                        &["array"],
                    ),
                    input.key,
                    input.path,
                    values,
                )
                .await
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonDelOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
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
                let key_existed_before = json_key_exists(
                    &state,
                    "redis_json_del",
                    AccessMode::Full,
                    &input.key,
                )
                .await?;
                let mode = path_mode(&input.path);
                let mut command = module_command(
                    "redis_json_del",
                    AccessMode::Full,
                    RedisModule::Json,
                    "JSON.DEL",
                );
                command.arg(input.key.as_str()).arg(input.path.as_str());
                let deleted = state.query(command, "JSON.DEL failed").await?;
                state.output(&JsonDelOutput {
                    key: input.key,
                    path: input.path,
                    path_mode: mode,
                    key_existed_before,
                    deleted,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonClearOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    match_count: usize,
    types: Vec<String>,
    cleared: u64,
}

fn json_clear_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_clear")
        .title("Clear Redis JSON Values")
        .description(
            "Clear matched arrays and objects and zero matched numbers. This destructive, idempotent operation requires full access.",
        )
        .output_schema(output_schema::<JsonClearOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonPathInput>| async move {
                state.require(AccessMode::Full, "redis_json_clear")?;
                validate_path(&input.path)?;
                let inspection = inspect_json_path(
                    &state,
                    "redis_json_clear",
                    AccessMode::Full,
                    &input.key,
                    &input.path,
                )
                .await?;
                let mode = path_mode(&input.path);
                let match_count = inspection.types.len();
                let cleared = if any_compatible(
                    &inspection.types,
                    &["array", "object", "integer", "number"],
                ) {
                    let mut command = module_command(
                        "redis_json_clear",
                        AccessMode::Full,
                        RedisModule::Json,
                        "JSON.CLEAR",
                    );
                    command.arg(input.key.as_str()).arg(input.path.as_str());
                    state.query(command, "JSON.CLEAR failed").await?
                } else {
                    0
                };
                state.output_collection(
                    &JsonClearOutput {
                        key: input.key,
                        path: input.path,
                        path_mode: mode,
                        key_existed_before: inspection.key_existed_before,
                        match_count,
                        types: inspection.types,
                        cleared,
                    },
                    match_count,
                    "Use a narrower JSONPath expression.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonArrpopInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// Element index to remove. Defaults to -1 (the last element).
    #[serde(default = "default_pop_index")]
    index: i64,
    /// Maximum encoded bytes retained for popped values. Larger payloads are omitted after mutation.
    #[serde(default = "default_pop_max_bytes")]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonArrpopOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    match_count: usize,
    types: Vec<String>,
    index: i64,
    popped: usize,
    returned_bytes: usize,
    values_omitted: bool,
    values: Option<Vec<Option<JsonValue>>>,
}

fn json_arrpop_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_arrpop")
        .title("Pop Redis JSON Array Values")
        .description(
            "Remove and optionally return one value from every matched array. Payloads over max_returned_bytes are omitted after mutation; full access is required.",
        )
        .output_schema(output_schema::<JsonArrpopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonArrpopInput>| async move {
                state.require(AccessMode::Full, "redis_json_arrpop")?;
                validate_path(&input.path)?;
                if input.max_returned_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_returned_bytes must be greater than zero",
                    ));
                }
                let mode = path_mode(&input.path);
                let inspection = inspect_json_path(
                    &state,
                    "redis_json_arrpop",
                    AccessMode::Full,
                    &input.key,
                    &input.path,
                )
                .await?;
                let values = if inspection.types.is_empty()
                    || !any_compatible(&inspection.types, &["array"])
                {
                    inspection.types.iter().map(|_| None).collect()
                } else if matches!(mode, JsonPathMode::Legacy)
                    && inspection.types[0] != "array"
                {
                    vec![None]
                } else {
                    let mut command = module_command(
                        "redis_json_arrpop",
                        AccessMode::Full,
                        RedisModule::Json,
                        "JSON.ARRPOP",
                    );
                    command
                        .arg(input.key.as_str())
                        .arg(input.path.as_str())
                        .arg(input.index.to_string());
                    let raw = state.raw(command, "JSON.ARRPOP failed").await?;
                    normalize_result_count(
                        redis_path_results(raw, mode, "JSON.ARRPOP")?,
                        &inspection,
                        "JSON.ARRPOP",
                    )?
                };
                let popped = values.iter().filter(|value| value.is_some()).count();
                let returned_bytes = serde_json::to_vec(&values)
                    .map_err(|error| tower_mcp::Error::tool(error.to_string()))?
                    .len();
                let max_returned_bytes = input
                    .max_returned_bytes
                    .min(state.max_output_bytes());
                let values_omitted = returned_bytes > max_returned_bytes;
                let match_count = inspection.types.len();
                state.output_collection(
                    &JsonArrpopOutput {
                        key: input.key,
                        path: input.path,
                        path_mode: mode,
                        key_existed_before: inspection.key_existed_before,
                        match_count,
                        types: inspection.types,
                        index: input.index,
                        popped,
                        returned_bytes,
                        values_omitted,
                        values: (!values_omitted).then_some(values),
                    },
                    match_count,
                    "Use a narrower JSONPath expression.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonArrtrimInput {
    /// Redis key containing the JSON document.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// Inclusive first array index to retain.
    start: i64,
    /// Inclusive last array index to retain.
    stop: i64,
}

fn json_arrtrim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_arrtrim")
        .title("Trim Redis JSON Arrays")
        .description(
            "Trim every matched array to an inclusive index range. This destructive operation requires full access and is not idempotent for arbitrary ranges.",
        )
        .output_schema(output_schema::<JsonPathResultsOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonArrtrimInput>| async move {
                state.require(AccessMode::Full, "redis_json_arrtrim")?;
                integer_path_command(
                    &state,
                    JsonPathCommandSpec::new(
                        "redis_json_arrtrim",
                        AccessMode::Full,
                        "JSON.ARRTRIM",
                        &["array"],
                    ),
                    input.key,
                    input.path,
                    [
                        input.start.to_string().into_bytes(),
                        input.stop.to_string().into_bytes(),
                    ],
                )
                .await
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonMergeInput {
    /// Redis key to create or update.
    key: String,
    /// JSONPath expression. Defaults to the enhanced document root `$`.
    #[serde(default = "default_root_path")]
    path: String,
    /// RFC 7396 merge patch. Pass structured JSON directly.
    value: JsonValue,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonMergeOutput {
    key: String,
    path: String,
    path_mode: JsonPathMode,
    key_existed_before: bool,
    merged: bool,
}

fn json_merge_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_json_merge")
        .title("Merge Redis JSON")
        .description(
            "Apply a structured RFC 7396 merge patch. Null object members can delete data, so this RedisJSON 2.6+ operation requires full access.",
        )
        .output_schema(output_schema::<JsonMergeOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<JsonMergeInput>| async move {
                state.require(AccessMode::Full, "redis_json_merge")?;
                validate_path(&input.path)?;
                let key_existed_before = json_key_exists(
                    &state,
                    "redis_json_merge",
                    AccessMode::Full,
                    &input.key,
                )
                .await?;
                if !key_existed_before && input.path != "$" {
                    return Err(tower_mcp::Error::tool(
                        "JSON.MERGE can create a missing key only at the root path `$`",
                    ));
                }
                let mode = path_mode(&input.path);
                let value = serde_json::to_vec(&input.value).map_err(|error| {
                    tower_mcp::Error::tool(format!("could not encode JSON merge patch: {error}"))
                })?;
                let mut command = module_command(
                    "redis_json_merge",
                    AccessMode::Full,
                    RedisModule::Json,
                    "JSON.MERGE",
                );
                command
                    .arg(input.key.as_str())
                    .arg(input.path.as_str())
                    .arg(value);
                let merged: String = state.query(command, "JSON.MERGE failed").await?;
                state.output(&JsonMergeOutput {
                    key: input.key,
                    path: input.path,
                    path_mode: mode,
                    key_existed_before,
                    merged: merged == "OK",
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(json_get_tool(state.clone()));
    router = router.tool(json_type_tool(state.clone()));
    router = router.tool(json_mget_tool(state.clone()));
    router = router.tool(json_strlen_tool(state.clone()));
    router = router.tool(json_objkeys_tool(state.clone()));
    router = router.tool(json_objlen_tool(state.clone()));
    router.tool(json_arrlen_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(json_set_tool(state.clone()));
    router = router.tool(json_numincrby_tool(state.clone()));
    router = router.tool(json_toggle_tool(state.clone()));
    router = router.tool(json_arrappend_tool(state.clone()));
    router.tool(json_arrinsert_tool(state))
}

pub(super) fn add_destructive_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(json_del_tool(state.clone()));
    router = router.tool(json_clear_tool(state.clone()));
    router = router.tool(json_arrpop_tool(state.clone()));
    router = router.tool(json_arrtrim_tool(state.clone()));
    router.tool(json_merge_tool(state))
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

    #[test]
    fn path_modes_follow_redisjson_syntax() {
        assert!(matches!(path_mode("$"), JsonPathMode::Enhanced));
        assert!(matches!(path_mode("$..name"), JsonPathMode::Enhanced));
        assert!(matches!(path_mode("$[0]"), JsonPathMode::Enhanced));
        assert!(matches!(path_mode(".name"), JsonPathMode::Legacy));
        assert!(matches!(path_mode("name"), JsonPathMode::Legacy));
    }

    #[test]
    fn json_entries_counts_nested_collection_members() {
        assert_eq!(json_entries(&serde_json::json!({"a": [1, {"b": 2}]})), 4);
    }
}
