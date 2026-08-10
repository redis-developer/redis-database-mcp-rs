//! Curated Redis database MCP tools.

mod data_structures;
mod essentials;
mod json_tools;
mod search;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use redis::FromRedisValue;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tower_mcp::{
    CallToolResult, McpRouter, ToolAnnotations, ToolBuilder,
    extract::{Json, State},
};

use crate::{
    AccessMode, OutputBudget, RawCommandPolicy, RedisCommand, RedisExecutor, RedisModule,
    RedisValue, ToolBundle,
};

pub(crate) const RAW_TOOL_NAME: &str = "redis_command";
const OUTPUT_LIMIT_CODE: &str = "output_limit_exceeded";
const OUTPUT_LIMIT_META_KEY: &str = "io.redis.mcp/outputLimit";

#[derive(Clone)]
pub(crate) struct ToolState {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
    command_timeout: Duration,
    raw_command_policy: RawCommandPolicy,
    output_budget: OutputBudget,
}

impl ToolState {
    pub(crate) fn new(
        executor: Arc<dyn RedisExecutor>,
        access: AccessMode,
        command_timeout: Duration,
        raw_command_policy: RawCommandPolicy,
        output_budget: OutputBudget,
    ) -> Self {
        Self {
            executor,
            access,
            command_timeout,
            raw_command_policy,
            output_budget,
        }
    }

    fn max_collection_entries(&self) -> usize {
        self.output_budget.max_collection_entries()
    }

    fn validate_requested_entries(&self, requested: usize, name: &str) -> tower_mcp::Result<()> {
        let limit = self.max_collection_entries();
        if requested == 0 || requested > limit {
            Err(tower_mcp::Error::tool(format!(
                "{name} must be between 1 and the configured output limit of {limit} entries"
            )))
        } else {
            Ok(())
        }
    }

    fn output<T: Serialize>(&self, value: &T) -> tower_mcp::Result<CallToolResult> {
        let result = CallToolResult::from_serialize(value)?;
        let actual_bytes = serde_json::to_vec(&result)?.len();
        let max_bytes = self.output_budget.max_bytes();
        if actual_bytes > max_bytes {
            Ok(output_limit_result(
                "encoded_bytes",
                actual_bytes,
                max_bytes,
                "Request a smaller page, a narrower value, or a more specific diagnostic section.",
            ))
        } else {
            Ok(result)
        }
    }

    fn output_collection<T: Serialize>(
        &self,
        value: &T,
        entries: usize,
        guidance: &str,
    ) -> tower_mcp::Result<CallToolResult> {
        let limit = self.max_collection_entries();
        if entries > limit {
            Ok(output_limit_result(
                "collection_entries",
                entries,
                limit,
                guidance,
            ))
        } else {
            self.output(value)
        }
    }

    async fn query<T: FromRedisValue>(
        &self,
        command: RedisCommand,
        context: &str,
    ) -> tower_mcp::Result<T> {
        let value = self.execute(command, context).await?;
        let value = value
            .into_redis_rs()
            .map_err(|error| tower_mcp::Error::tool(format!("{context}: {error}")))?;
        T::from_redis_value(value)
            .map_err(|error| tower_mcp::Error::tool(format!("{context}: {error}")))
    }

    async fn raw(&self, command: RedisCommand, context: &str) -> tower_mcp::Result<RedisValue> {
        self.execute(command, context).await
    }

    async fn execute(&self, command: RedisCommand, context: &str) -> tower_mcp::Result<RedisValue> {
        let required_module = command.required_module();
        let command_name = command.name().to_string();
        match tokio::time::timeout(self.command_timeout, self.executor.execute(command)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let error =
                    error.classify_module_requirement(required_module, command_name.as_str());
                Err(tower_mcp::Error::tool(format!(
                    "{context} [{:?}]: {error}",
                    error.kind()
                )))
            }
            Err(_) => Err(tower_mcp::Error::tool(format!(
                "{context}: Redis command timed out after {} ms",
                self.command_timeout.as_millis()
            ))),
        }
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

fn output_limit_result(
    dimension: &'static str,
    actual: usize,
    limit: usize,
    guidance: &str,
) -> CallToolResult {
    let message = format!(
        "[{OUTPUT_LIMIT_CODE}] {dimension} result size {actual} exceeds configured limit {limit}. {guidance}"
    );
    let mut result = CallToolResult::error(message);
    result.meta = Some(json!({
        OUTPUT_LIMIT_META_KEY: {
            "code": OUTPUT_LIMIT_CODE,
            "dimension": dimension,
            "actual": actual,
            "limit": limit,
            "retryable": true,
            "guidance": guidance,
        },
    }));
    result
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PageContinuation {
    cursor: Option<u64>,
    start: Option<i64>,
    offset: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PageMetadata {
    requested: usize,
    returned: usize,
    complete: bool,
    continuation: Option<PageContinuation>,
}

impl PageMetadata {
    fn cursor(requested: usize, returned: usize, cursor: u64) -> Self {
        Self {
            requested,
            returned,
            complete: cursor == 0,
            continuation: (cursor != 0).then_some(PageContinuation {
                cursor: Some(cursor),
                start: None,
                offset: None,
            }),
        }
    }

    fn range(requested: usize, returned: usize, next_start: Option<i64>) -> Self {
        Self {
            requested,
            returned,
            complete: next_start.is_none(),
            continuation: next_start.map(|start| PageContinuation {
                cursor: None,
                start: Some(start),
                offset: None,
            }),
        }
    }

    fn offset(requested: usize, returned: usize, next_offset: Option<u64>) -> Self {
        Self {
            requested,
            returned,
            complete: next_offset.is_none(),
            continuation: next_offset.map(|offset| PageContinuation {
                cursor: None,
                start: None,
                offset: Some(offset),
            }),
        }
    }
}

fn command(
    tool_name: &'static str,
    required_access: AccessMode,
    command_name: &'static str,
) -> RedisCommand {
    RedisCommand::new(tool_name, required_access, command_name)
}

fn module_command(
    tool_name: &'static str,
    required_access: AccessMode,
    required_module: RedisModule,
    command_name: &'static str,
) -> RedisCommand {
    let mut command = RedisCommand::new(tool_name, required_access, command_name);
    command.require_module(required_module);
    command
}

pub(crate) fn add_read_only_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
    bundles: &BTreeSet<ToolBundle>,
) -> McpRouter {
    if bundles.contains(&ToolBundle::Essentials) {
        router = router.tool(ping_tool(state.clone()));
        router = router.tool(dbsize_tool(state.clone()));
        router = router.tool(scan_tool(state.clone()));
        router = router.tool(get_tool(state.clone()));
        router = router.tool(type_tool(state.clone()));
        router = router.tool(ttl_tool(state.clone()));
        router = essentials::add_read_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::DataStructures) {
        router = data_structures::add_read_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Json) {
        router = json_tools::add_read_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Search) {
        router = search::add_read_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Diagnostics) {
        router = router.tool(info_tool(state));
    }
    router
}

pub(crate) fn add_write_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
    bundles: &BTreeSet<ToolBundle>,
) -> McpRouter {
    if bundles.contains(&ToolBundle::Essentials) {
        router = router.tool(set_tool(state.clone()));
        router = essentials::add_write_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::DataStructures) {
        router = data_structures::add_write_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Json) {
        router = json_tools::add_write_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Search) {
        router = search::add_write_tools(router, state);
    }
    router
}

pub(crate) fn add_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
    bundles: &BTreeSet<ToolBundle>,
) -> McpRouter {
    if bundles.contains(&ToolBundle::Essentials) {
        router =
            essentials::add_destructive_tools(router.tool(del_tool(state.clone())), state.clone());
    }
    if bundles.contains(&ToolBundle::Json) {
        router = json_tools::add_destructive_tools(router, state.clone());
    }
    if bundles.contains(&ToolBundle::Search) {
        router = search::add_destructive_tools(router, state);
    }
    router
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
            let response: String = state
                .query(
                    command("redis_ping", AccessMode::ReadOnly, "PING"),
                    "PING failed",
                )
                .await?;
            state.output(&PingOutput {
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
                let mut command = command("redis_info", AccessMode::ReadOnly, "INFO");
                if let Some(section) = &input.section {
                    command.arg(section.as_str());
                }
                let raw: String = state.query(command, "INFO failed").await?;
                let properties = raw
                    .lines()
                    .filter(|line| !line.is_empty() && !line.starts_with('#'))
                    .filter_map(|line| line.split_once(':'))
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect();
                let output = InfoOutput {
                    section: input.section,
                    properties,
                    raw,
                };
                state.output_collection(
                    &output,
                    output.properties.len(),
                    "Request one specific INFO section.",
                )
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
            let key_count = state
                .query(
                    command("redis_dbsize", AccessMode::ReadOnly, "DBSIZE"),
                    "DBSIZE failed",
                )
                .await?;
            state.output(&DbsizeOutput { key_count })
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
    page: PageMetadata,
}

fn scan_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_scan")
        .title("Scan Redis Keys")
        .description(
            "Read one bounded non-blocking SCAN page. Pass page.continuation.cursor as cursor until page.complete is true.",
        )
        .output_schema(output_schema::<ScanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut command = command("redis_scan", AccessMode::ReadOnly, "SCAN");
                command
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                if let Some(key_type) = &input.key_type {
                    command.arg("TYPE").arg(key_type.as_str());
                }
                let (cursor, keys): (u64, Vec<Vec<u8>>) =
                    state.query(command, "SCAN failed").await?;
                let keys = keys.into_iter().map(display_bytes).collect::<Vec<_>>();
                let output = ScanOutput {
                    cursor,
                    count: keys.len(),
                    page: PageMetadata::cursor(input.count, keys.len(), cursor),
                    keys,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry SCAN with a smaller count and the same cursor.",
                )
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
                let mut command = command("redis_get", AccessMode::ReadOnly, "GET");
                command.arg(input.key.as_str());
                let value: Option<Vec<u8>> = state.query(command, "GET failed").await?;
                let (value, encoding) = match value {
                    Some(bytes) => {
                        let (value, encoding) = encode_bytes(bytes);
                        (Some(value), Some(encoding))
                    }
                    None => (None, None),
                };
                state.output(&GetOutput {
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
                let mut command = command("redis_type", AccessMode::ReadOnly, "TYPE");
                command.arg(input.key.as_str());
                let key_type = state.query(command, "TYPE failed").await?;
                state.output(&TypeOutput {
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
                let mut command = command("redis_ttl", AccessMode::ReadOnly, "TTL");
                command.arg(input.key.as_str());
                let ttl_seconds = state.query(command, "TTL failed").await?;
                state.output(&TtlOutput {
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
                let mut command = command("redis_set", AccessMode::ReadWrite, "SET");
                command.arg(input.key.as_str()).arg(input.value.as_str());
                if let Some(seconds) = input.expires_in_seconds {
                    command.arg("EX").arg(seconds.to_string());
                }
                let response: String = state.query(command, "SET failed").await?;
                state.output(&SetOutput {
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
                let mut command = command("redis_del", AccessMode::Full, "DEL");
                command.args(input.keys);
                let deleted = state.query(command, "DEL failed").await?;
                state.output(&DelOutput { requested, deleted })
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
            "Run a classified or explicitly unrestricted Redis request/response command. Connection-state, streaming, transaction, script, and blocking forms are rejected.",
        )
        .output_schema(output_schema::<RawCommandOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<RawCommandInput>| async move {
                state.require(AccessMode::Full, RAW_TOOL_NAME)?;
                let command_name = crate::raw::validate_command(
                    &input.command,
                    &input.arguments,
                    state.raw_command_policy,
                )
                .map_err(tower_mcp::Error::tool)?;
                let mut command = RedisCommand::new(
                    RAW_TOOL_NAME,
                    AccessMode::Full,
                    command_name.clone(),
                );
                command.args(input.arguments);
                let value = state.raw(command, "Redis command failed").await?;
                let entries = redis_value_collection_entries(&value);
                let output = RawCommandOutput {
                    command: command_name,
                    value: redis_value_to_json(value),
                };
                state.output_collection(
                    &output,
                    entries,
                    "Use a bounded command form with LIMIT, COUNT, or a cursor.",
                )
            },
        )
        .build()
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

fn redis_value_collection_entries(value: &RedisValue) -> usize {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => {
            values.iter().fold(values.len(), |count, value| {
                count.saturating_add(redis_value_collection_entries(value))
            })
        }
        RedisValue::Map(values) => values.iter().fold(values.len(), |count, (key, value)| {
            count
                .saturating_add(redis_value_collection_entries(key))
                .saturating_add(redis_value_collection_entries(value))
        }),
        RedisValue::Attribute { data, attributes } => attributes.iter().fold(
            redis_value_collection_entries(data).saturating_add(attributes.len()),
            |count, (key, value)| {
                count
                    .saturating_add(redis_value_collection_entries(key))
                    .saturating_add(redis_value_collection_entries(value))
            },
        ),
        RedisValue::Push { data, .. } => data.iter().fold(data.len(), |count, value| {
            count.saturating_add(redis_value_collection_entries(value))
        }),
        _ => 0,
    }
}

fn redis_value_to_json(value: RedisValue) -> JsonValue {
    match value {
        RedisValue::Nil => JsonValue::Null,
        RedisValue::Integer(value) => json!(value),
        RedisValue::BulkString(value) => {
            let (value, encoding) = encode_bytes(value);
            json!({ "value": value, "encoding": encoding })
        }
        RedisValue::Array(values) | RedisValue::Set(values) => {
            JsonValue::Array(values.into_iter().map(redis_value_to_json).collect())
        }
        RedisValue::SimpleString(value) => json!(value),
        RedisValue::Okay => json!("OK"),
        RedisValue::Map(values) => JsonValue::Array(
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
        RedisValue::Attribute { data, attributes } => json!({
            "data": redis_value_to_json(*data),
            "attributes": attributes
                .into_iter()
                .map(|(key, value)| json!({
                    "key": redis_value_to_json(key),
                    "value": redis_value_to_json(value),
                }))
                .collect::<Vec<_>>(),
        }),
        RedisValue::Double(value) => json!(value),
        RedisValue::Boolean(value) => json!(value),
        RedisValue::VerbatimString { format, text } => {
            json!({ "format": format, "text": text })
        }
        RedisValue::BigNumber(value) => {
            let (value, encoding) = encode_bytes(value);
            json!({ "value": value, "encoding": encoding })
        }
        RedisValue::Push { kind, data } => json!({
            "kind": kind,
            "data": data.into_iter().map(redis_value_to_json).collect::<Vec<_>>(),
        }),
        RedisValue::ServerError { code, message } => {
            json!({ "server_error": { "code": code, "message": message } })
        }
        RedisValue::Unsupported(value) => json!({ "unsupported": value }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_values_are_explicitly_encoded() {
        let json = redis_value_to_json(RedisValue::BulkString(vec![0xff, 0x00]));
        assert_eq!(json["encoding"], "base64");
        assert_eq!(json["value"], "/wA=");
    }
}
