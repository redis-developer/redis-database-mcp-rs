//! Curated Redis database MCP tools.

use std::{collections::BTreeMap, sync::Arc, time::Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use redis::{FromRedisValue, Value};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tower_mcp::{
    CallToolResult, McpRouter, ToolAnnotations, ToolBuilder,
    extract::{Json, State},
};

use crate::{AccessMode, RedisExecutor};

pub(crate) const READ_ONLY_TOOL_NAMES: &[&str] = &[
    "redis_ping",
    "redis_info",
    "redis_dbsize",
    "redis_scan",
    "redis_get",
    "redis_type",
    "redis_ttl",
];
pub(crate) const WRITE_TOOL_NAMES: &[&str] = &["redis_set"];
pub(crate) const DESTRUCTIVE_TOOL_NAMES: &[&str] = &["redis_del"];
pub(crate) const RAW_TOOL_NAME: &str = "redis_command";

#[derive(Clone)]
pub(crate) struct ToolState {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
}

impl ToolState {
    pub(crate) fn new(executor: Arc<dyn RedisExecutor>, access: AccessMode) -> Self {
        Self { executor, access }
    }

    async fn query<T: FromRedisValue>(
        &self,
        command: redis::Cmd,
        context: &str,
    ) -> tower_mcp::Result<T> {
        let value = self
            .executor
            .execute(command)
            .await
            .map_err(|error| tower_mcp::Error::tool(format!("{context}: {error}")))?;
        T::from_redis_value(value)
            .map_err(|error| tower_mcp::Error::tool(format!("{context}: {error}")))
    }

    async fn raw(&self, command: redis::Cmd, context: &str) -> tower_mcp::Result<Value> {
        self.executor
            .execute(command)
            .await
            .map_err(|error| tower_mcp::Error::tool(format!("{context}: {error}")))
    }

    fn require(&self, required: AccessMode, tool: &str) -> tower_mcp::Result<()> {
        if self.access.permits(required) {
            Ok(())
        } else {
            Err(tower_mcp::Error::tool(format!(
                "{tool} requires {required:?} access; this router is {:?}",
                self.access
            )))
        }
    }
}

pub(crate) fn add_read_only_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ping_tool(state.clone()));
    router = router.tool(info_tool(state.clone()));
    router = router.tool(dbsize_tool(state.clone()));
    router = router.tool(scan_tool(state.clone()));
    router = router.tool(get_tool(state.clone()));
    router = router.tool(type_tool(state.clone()));
    router.tool(ttl_tool(state))
}

pub(crate) fn add_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(set_tool(state))
}

pub(crate) fn add_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(del_tool(state))
}

pub(crate) fn add_raw_tool(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(raw_tool(state))
}

fn output_schema<T: JsonSchema>() -> JsonValue {
    schema_for!(T).to_value()
}

fn empty_input_schema() -> JsonValue {
    json!({
        "type": "object",
        "additionalProperties": false,
    })
}

fn read_annotations() -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: true,
        open_world_hint: true,
        ..ToolAnnotations::default()
    }
}

fn write_annotations(idempotent: bool) -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: false,
        destructive_hint: false,
        idempotent_hint: idempotent,
        open_world_hint: true,
        ..ToolAnnotations::default()
    }
}

fn destructive_annotations(idempotent: bool) -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: false,
        destructive_hint: true,
        idempotent_hint: idempotent,
        open_world_hint: true,
        ..ToolAnnotations::default()
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PingOutput {
    response: String,
    latency_ms: f64,
}

fn ping_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_ping")
        .title("Ping Redis")
        .description("Test the configured Redis connection and report round-trip latency.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<PingOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let started = Instant::now();
            let response: String = state.query(redis::cmd("PING"), "PING failed").await?;
            CallToolResult::from_serialize(&PingOutput {
                response,
                latency_ms: started.elapsed().as_secs_f64() * 1_000.0,
            })
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InfoInput {
    /// Optional INFO section, such as server, memory, stats, or replication.
    #[serde(default)]
    section: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InfoOutput {
    section: Option<String>,
    properties: BTreeMap<String, String>,
    raw: String,
}

fn info_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_info")
        .title("Redis Server Info")
        .description(
            "Read Redis server information and statistics, optionally for one INFO section.",
        )
        .output_schema(output_schema::<InfoOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<InfoInput>| async move {
                let mut command = redis::cmd("INFO");
                if let Some(section) = &input.section {
                    command.arg(section);
                }
                let raw: String = state.query(command, "INFO failed").await?;
                let properties = raw
                    .lines()
                    .filter(|line| !line.is_empty() && !line.starts_with('#'))
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect();
                CallToolResult::from_serialize(&InfoOutput {
                    section: input.section,
                    properties,
                    raw,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DbsizeOutput {
    key_count: u64,
}

fn dbsize_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_dbsize")
        .title("Redis Database Size")
        .description("Count keys in the selected Redis database.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<DbsizeOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let key_count = state.query(redis::cmd("DBSIZE"), "DBSIZE failed").await?;
            CallToolResult::from_serialize(&DbsizeOutput { key_count })
        })
        .build()
}

fn default_pattern() -> String {
    "*".to_string()
}

fn default_scan_count() -> usize {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScanInput {
    /// Cursor returned by the previous scan. Start with zero.
    #[serde(default)]
    cursor: u64,
    /// Glob-style key pattern.
    #[serde(default = "default_pattern")]
    pattern: String,
    /// Optional Redis key type filter.
    #[serde(default)]
    key_type: Option<String>,
    /// Approximate number of keys Redis should inspect in this page.
    #[serde(default = "default_scan_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScanOutput {
    cursor: u64,
    keys: Vec<String>,
    count: usize,
}

fn scan_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_scan")
        .title("Scan Redis Keys")
        .description("Read one non-blocking SCAN page. Pass the returned cursor to continue.")
        .output_schema(output_schema::<ScanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ScanInput>| async move {
                if !(1..=1000).contains(&input.count) {
                    return Err(tower_mcp::Error::tool("count must be between 1 and 1000"));
                }
                let mut command = redis::cmd("SCAN");
                command
                    .arg(input.cursor)
                    .arg("MATCH")
                    .arg(&input.pattern)
                    .arg("COUNT")
                    .arg(input.count);
                if let Some(key_type) = &input.key_type {
                    command.arg("TYPE").arg(key_type);
                }
                let (cursor, keys): (u64, Vec<Vec<u8>>) =
                    state.query(command, "SCAN failed").await?;
                let keys = keys.into_iter().map(display_bytes).collect::<Vec<_>>();
                CallToolResult::from_serialize(&ScanOutput {
                    cursor,
                    count: keys.len(),
                    keys,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyInput {
    /// Redis key.
    key: String,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ValueEncoding {
    Utf8,
    Base64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetOutput {
    key: String,
    exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

fn get_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_get")
        .title("Get Redis String")
        .description("Read a Redis string value. Binary data is returned as base64.")
        .output_schema(output_schema::<GetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = redis::cmd("GET");
                command.arg(&input.key);
                let value: Option<Vec<u8>> = state.query(command, "GET failed").await?;
                let (value, encoding) = match value {
                    Some(bytes) => {
                        let (value, encoding) = encode_bytes(bytes);
                        (Some(value), Some(encoding))
                    }
                    None => (None, None),
                };
                CallToolResult::from_serialize(&GetOutput {
                    key: input.key,
                    exists: value.is_some(),
                    value,
                    encoding,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TypeOutput {
    key: String,
    key_type: String,
}

fn type_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_type")
        .title("Redis Key Type")
        .description("Read the Redis data type of a key.")
        .output_schema(output_schema::<TypeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = redis::cmd("TYPE");
                command.arg(&input.key);
                let key_type = state.query(command, "TYPE failed").await?;
                CallToolResult::from_serialize(&TypeOutput {
                    key: input.key,
                    key_type,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TtlOutput {
    key: String,
    ttl_seconds: i64,
    exists: bool,
    persistent: bool,
}

fn ttl_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_ttl")
        .title("Redis Key TTL")
        .description("Read a key TTL in seconds, including whether it is missing or persistent.")
        .output_schema(output_schema::<TtlOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = redis::cmd("TTL");
                command.arg(&input.key);
                let ttl_seconds = state.query(command, "TTL failed").await?;
                CallToolResult::from_serialize(&TtlOutput {
                    key: input.key,
                    ttl_seconds,
                    exists: ttl_seconds != -2,
                    persistent: ttl_seconds == -1,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetInput {
    /// Redis key.
    key: String,
    /// UTF-8 value to store.
    value: String,
    /// Optional expiration in seconds.
    #[serde(default)]
    expires_in_seconds: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetOutput {
    key: String,
    stored: bool,
    expires_in_seconds: Option<u64>,
}

fn set_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_set")
        .title("Set Redis String")
        .description("Set a UTF-8 Redis string, optionally with an expiration.")
        .output_schema(output_schema::<SetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_set")?;
                let mut command = redis::cmd("SET");
                command.arg(&input.key).arg(&input.value);
                if let Some(seconds) = input.expires_in_seconds {
                    command.arg("EX").arg(seconds);
                }
                let response: String = state.query(command, "SET failed").await?;
                CallToolResult::from_serialize(&SetOutput {
                    key: input.key,
                    stored: response == "OK",
                    expires_in_seconds: input.expires_in_seconds,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DelInput {
    /// Redis keys to delete.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DelOutput {
    requested: usize,
    deleted: u64,
}

fn del_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_del")
        .title("Delete Redis Keys")
        .description("Permanently delete one or more Redis keys. Requires full access.")
        .output_schema(output_schema::<DelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DelInput>| async move {
                state.require(AccessMode::Full, "redis_del")?;
                if input.keys.is_empty() || input.keys.len() > 1000 {
                    return Err(tower_mcp::Error::tool(
                        "keys must contain between 1 and 1000 items",
                    ));
                }
                let requested = input.keys.len();
                let mut command = redis::cmd("DEL");
                command.arg(input.keys);
                let deleted = state.query(command, "DEL failed").await?;
                CallToolResult::from_serialize(&DelOutput { requested, deleted })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawCommandInput {
    /// Redis command name, without arguments.
    command: String,
    /// Command arguments in order.
    #[serde(default)]
    arguments: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawCommandOutput {
    command: String,
    value: JsonValue,
}

fn raw_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new(RAW_TOOL_NAME)
        .title("Run Redis Command")
        .description(
            "Run an explicitly enabled Redis command. Connection-state, streaming, transaction, and blocking commands are rejected.",
        )
        .output_schema(output_schema::<RawCommandOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<RawCommandInput>| async move {
                state.require(AccessMode::Full, RAW_TOOL_NAME)?;
                let command_name = validate_raw_command(&input.command, &input.arguments)?;
                let mut command = redis::cmd(&command_name);
                command.arg(input.arguments);
                let value = state.raw(command, "Redis command failed").await?;
                CallToolResult::from_serialize(&RawCommandOutput {
                    command: command_name,
                    value: redis_value_to_json(value),
                })
            },
        )
        .build()
}

fn validate_raw_command(command: &str, arguments: &[String]) -> tower_mcp::Result<String> {
    let command = command.trim().to_ascii_uppercase();
    if command.is_empty() || command.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(tower_mcp::Error::tool(
            "command must be one Redis command name without whitespace",
        ));
    }

    const REJECTED: &[&str] = &[
        "AUTH",
        "BLMOVE",
        "BLMPOP",
        "BLPOP",
        "BRPOP",
        "BRPOPLPUSH",
        "BZMPOP",
        "BZPOPMAX",
        "BZPOPMIN",
        "CLIENT",
        "EXEC",
        "HELLO",
        "MONITOR",
        "MULTI",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
        "QUIT",
        "SELECT",
        "SSUBSCRIBE",
        "SUBSCRIBE",
        "SUNSUBSCRIBE",
        "UNSUBSCRIBE",
        "UNWATCH",
        "WATCH",
    ];
    if REJECTED.contains(&command.as_str()) {
        return Err(tower_mcp::Error::tool(format!(
            "{command} is not supported by the request/response raw tool"
        )));
    }
    if matches!(command.as_str(), "XREAD" | "XREADGROUP")
        && arguments
            .iter()
            .any(|argument| argument.eq_ignore_ascii_case("BLOCK"))
    {
        return Err(tower_mcp::Error::tool(
            "blocking XREAD/XREADGROUP is not supported by the raw tool",
        ));
    }
    Ok(command)
}

fn encode_bytes(bytes: Vec<u8>) -> (String, ValueEncoding) {
    match String::from_utf8(bytes) {
        Ok(value) => (value, ValueEncoding::Utf8),
        Err(error) => (BASE64.encode(error.into_bytes()), ValueEncoding::Base64),
    }
}

fn display_bytes(bytes: Vec<u8>) -> String {
    let (value, encoding) = encode_bytes(bytes);
    match encoding {
        ValueEncoding::Utf8 => value,
        ValueEncoding::Base64 => format!("base64:{value}"),
    }
}

fn redis_value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Nil => JsonValue::Null,
        Value::Int(value) => json!(value),
        Value::BulkString(value) => {
            let (value, encoding) = encode_bytes(value);
            json!({ "value": value, "encoding": encoding })
        }
        Value::Array(values) | Value::Set(values) => {
            JsonValue::Array(values.into_iter().map(redis_value_to_json).collect())
        }
        Value::SimpleString(value) => json!(value),
        Value::Okay => json!("OK"),
        Value::Map(values) => JsonValue::Array(
            values
                .into_iter()
                .map(|(key, value)| {
                    json!({
                        "key": redis_value_to_json(key),
                        "value": redis_value_to_json(value),
                    })
                })
                .collect(),
        ),
        Value::Attribute { data, attributes } => json!({
            "data": redis_value_to_json(*data),
            "attributes": attributes
                .into_iter()
                .map(|(key, value)| json!({
                    "key": redis_value_to_json(key),
                    "value": redis_value_to_json(value),
                }))
                .collect::<Vec<_>>(),
        }),
        Value::Double(value) => json!(value),
        Value::Boolean(value) => json!(value),
        Value::VerbatimString { format, text } => {
            json!({ "format": format!("{format:?}"), "text": text })
        }
        Value::BigNumber(value) => json!(format!("{value:?}")),
        Value::Push { kind, data } => json!({
            "kind": format!("{kind:?}"),
            "data": data.into_iter().map(redis_value_to_json).collect::<Vec<_>>(),
        }),
        Value::ServerError(error) => json!({ "server_error": error.to_string() }),
        other => json!({ "unsupported": format!("{other:?}") }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_values_are_explicitly_encoded() {
        let json = redis_value_to_json(Value::BulkString(vec![0xff, 0x00]));
        assert_eq!(json["encoding"], "base64");
        assert_eq!(json["value"], "/wA=");
    }

    #[test]
    fn raw_commands_reject_connection_state_and_blocking_reads() {
        assert!(validate_raw_command("SELECT", &["1".into()]).is_err());
        assert!(validate_raw_command("xread", &["BLOCK".into(), "0".into()]).is_err());
        assert_eq!(
            validate_raw_command("get", &["key".into()]).ok().as_deref(),
            Some("GET")
        );
    }
}
