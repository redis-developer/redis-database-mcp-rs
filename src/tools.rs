//! Curated Redis database MCP tools.

mod data_structures;
mod essentials;
mod json_tools;
mod search;
mod streams;

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
    AccessMode, NativeRedisInvocation, OutputBudget, RedisCommand, RedisInvocationEngine,
    RedisModule, RedisValue, RedisVersion, ToolBundle,
    invocation::{redis_value_collection_entries, redis_value_to_json},
};

pub(crate) const RAW_TOOL_NAME: &str = "redis_command";
const OUTPUT_LIMIT_CODE: &str = "output_limit_exceeded";
const OUTPUT_LIMIT_META_KEY: &str = "io.redis.mcp/outputLimit";
const DEFAULT_RETURNED_VALUE_MAX_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct ToolState {
    access: AccessMode,
    output_budget: OutputBudget,
    invocation_engine: RedisInvocationEngine,
}

impl ToolState {
    pub(crate) fn new(
        access: AccessMode,
        output_budget: OutputBudget,
        invocation_engine: RedisInvocationEngine,
    ) -> Self {
        Self {
            access,
            output_budget,
            invocation_engine,
        }
    }

    fn max_collection_entries(&self) -> usize {
        self.output_budget.max_collection_entries()
    }

    fn max_output_bytes(&self) -> usize {
        self.output_budget.max_bytes()
    }

    fn redis_version(&self) -> Option<RedisVersion> {
        self.invocation_engine.capabilities().redis_version()
    }

    fn module_version(&self, module: RedisModule) -> Option<RedisVersion> {
        self.invocation_engine
            .capabilities()
            .module(module)
            .version()
    }

    fn command_timeout(&self) -> Duration {
        self.invocation_engine.command_timeout()
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
        self.invocation_engine
            .execute_curated(command)
            .await
            .map_err(|error| {
                tower_mcp::Error::tool(format!("{context} [{:?}]: {error}", error.kind()))
            })
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
        router = streams::add_read_tools(router, state.clone());
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
        router = streams::add_write_tools(router, state.clone());
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
    if bundles.contains(&ToolBundle::DataStructures) {
        router = data_structures::add_destructive_tools(router, state.clone());
        router = streams::add_destructive_tools(router, state.clone());
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
pub(super) struct KeyInput {
    /// Redis key.
    pub(super) key: String,
    /// Encoding of `key`.
    #[serde(default)]
    pub(super) key_encoding: InputEncoding,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum ValueEncoding {
    Utf8,
    Base64,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum InputEncoding {
    #[default]
    Utf8,
    Base64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetOutput {
    key: String,
    key_encoding: InputEncoding,
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
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_get", AccessMode::ReadOnly, "GET");
                command.arg(key);
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
                    key_encoding: input.key_encoding,
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
    key_encoding: InputEncoding,
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
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_type", AccessMode::ReadOnly, "TYPE");
                command.arg(key);
                let key_type = state.query(command, "TYPE failed").await?;
                state.output(&TypeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
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
    key_encoding: InputEncoding,
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
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_ttl", AccessMode::ReadOnly, "TTL");
                command.arg(key);
                let ttl_seconds = state.query(command, "TTL failed").await?;
                state.output(&TtlOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
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
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Value to store.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
    /// Optional existence condition.
    #[serde(default)]
    condition: Option<SetCondition>,
    /// Return the previous value. `GET` with `NX` requires Redis 7.0 or newer.
    #[serde(default)]
    get: bool,
    /// Maximum previous-value bytes to return when `get` is true. Larger prior values are
    /// reported as omitted so the write result remains observable.
    #[serde(default = "default_returned_value_max_bytes")]
    #[schemars(range(min = 1))]
    max_previous_bytes: usize,
    /// Optional, mutually exclusive expiration behavior.
    #[serde(default)]
    expiration: Option<SetExpiration>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SetCondition {
    Nx,
    Xx,
}

impl SetCondition {
    fn redis_token(self) -> &'static str {
        match self {
            Self::Nx => "NX",
            Self::Xx => "XX",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum SetExpiration {
    Seconds(#[schemars(range(min = 1))] u64),
    Milliseconds(#[schemars(range(min = 1))] u64),
    UnixSeconds(#[schemars(range(min = 1))] u64),
    UnixMilliseconds(#[schemars(range(min = 1))] u64),
    KeepTtl,
}

impl SetExpiration {
    fn append_to(self, command: &mut RedisCommand) -> tower_mcp::Result<()> {
        let (token, value) = match self {
            Self::Seconds(value) => ("EX", Some(value)),
            Self::Milliseconds(value) => ("PX", Some(value)),
            Self::UnixSeconds(value) => ("EXAT", Some(value)),
            Self::UnixMilliseconds(value) => ("PXAT", Some(value)),
            Self::KeepTtl => ("KEEPTTL", None),
        };
        if value == Some(0) {
            return Err(tower_mcp::Error::tool(
                "expiration value must be greater than zero",
            ));
        }
        command.arg(token);
        if let Some(value) = value {
            command.arg(value.to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetOutput {
    key: String,
    key_encoding: InputEncoding,
    applied: bool,
    condition: Option<SetCondition>,
    expiration: Option<SetExpiration>,
    previous_value_requested: bool,
    previous_exists: Option<bool>,
    previous_value: Option<String>,
    previous_value_encoding: Option<ValueEncoding>,
    previous_value_bytes: Option<usize>,
    previous_value_omitted: bool,
}

fn default_returned_value_max_bytes() -> usize {
    DEFAULT_RETURNED_VALUE_MAX_BYTES
}

fn set_tool(state: Arc<ToolState>) -> tower_mcp::Tool {
    ToolBuilder::new("redis_set")
        .title("Set Redis String")
        .description(
            "Set a binary-safe Redis string with optional NX/XX, previous-value, and one typed expiration behavior.",
        )
        .output_schema(output_schema::<SetOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_set")?;
                if state.redis_version().is_some_and(|version| {
                    version < RedisVersion::new(6, 0, 0)
                        && matches!(input.expiration, Some(SetExpiration::KeepTtl))
                }) {
                    return Err(tower_mcp::Error::tool(
                        "SET with KEEPTTL requires Redis 6.0 or newer",
                    ));
                }
                if state.redis_version().is_some_and(|version| {
                    version < RedisVersion::new(6, 2, 0)
                        && (input.get
                            || matches!(
                                input.expiration,
                                Some(
                                    SetExpiration::UnixSeconds(_)
                                        | SetExpiration::UnixMilliseconds(_)
                                )
                            ))
                }) {
                    return Err(tower_mcp::Error::tool(
                        "SET with GET, EXAT, or PXAT requires Redis 6.2 or newer",
                    ));
                }
                if input.get
                    && matches!(input.condition, Some(SetCondition::Nx))
                    && state
                        .redis_version()
                        .is_some_and(|version| version < RedisVersion::new(7, 0, 0))
                {
                    return Err(tower_mcp::Error::tool(
                        "SET with GET and NX requires Redis 7.0 or newer",
                    ));
                }
                if input.get && input.max_previous_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_previous_bytes must be greater than zero",
                    ));
                }
                let max_previous_bytes =
                    input.max_previous_bytes.min(state.max_output_bytes());
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let value = decode_input(&input.value, input.value_encoding, "value")?;
                let mut command = command("redis_set", AccessMode::ReadWrite, "SET");
                command.arg(key).arg(value);
                if let Some(condition) = input.condition {
                    command.arg(condition.redis_token());
                }
                if input.get {
                    command.arg("GET");
                }
                if let Some(expiration) = input.expiration {
                    expiration.append_to(&mut command)?;
                }
                let response = state.raw(command, "SET failed").await?;
                let (
                    applied,
                    previous_exists,
                    previous_value,
                    previous_value_encoding,
                    previous_value_bytes,
                    previous_value_omitted,
                ) = if input.get {
                        let previous = optional_bytes(response, "SET GET")?;
                        let applied = match input.condition {
                            None => true,
                            Some(SetCondition::Nx) => previous.is_none(),
                            Some(SetCondition::Xx) => previous.is_some(),
                        };
                        let previous_exists = Some(previous.is_some());
                        let previous_value_bytes = previous.as_ref().map(Vec::len);
                        let previous_value_omitted = previous
                            .as_ref()
                            .is_some_and(|bytes| bytes.len() > max_previous_bytes);
                        let (previous_value, previous_value_encoding) = match previous {
                            Some(bytes) if !previous_value_omitted => {
                                let (value, encoding) = encode_bytes(bytes);
                                (Some(value), Some(encoding))
                            }
                            _ => (None, None),
                        };
                        (
                            applied,
                            previous_exists,
                            previous_value,
                            previous_value_encoding,
                            previous_value_bytes,
                            previous_value_omitted,
                        )
                    } else {
                        let applied = match response {
                            RedisValue::Okay => true,
                            RedisValue::SimpleString(value) if value.eq_ignore_ascii_case("OK") => {
                                true
                            }
                            RedisValue::Nil => false,
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "SET returned an unexpected reply: {other:?}"
                                )));
                            }
                        };
                        (applied, None, None, None, None, false)
                    };
                state.output(&SetOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    applied,
                    condition: input.condition,
                    expiration: input.expiration,
                    previous_value_requested: input.get,
                    previous_exists,
                    previous_value,
                    previous_value_encoding,
                    previous_value_bytes,
                    previous_value_omitted,
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
                let invocation = NativeRedisInvocation::new(input.command.trim().as_bytes())
                    .args(input.arguments.into_iter().map(String::into_bytes));
                let response = match state
                    .invocation_engine
                    .invoke_with_metadata(invocation)
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        if let Some(limit) = error.output_limit() {
                            return Ok(output_limit_result(
                                limit.dimension().as_str(),
                                limit.actual(),
                                limit.limit(),
                                "Use a bounded command form with LIMIT, COUNT, or a cursor.",
                            ));
                        }
                        return Err(tower_mcp::Error::tool(format!(
                            "Redis command failed [{:?}]: {error}",
                            error.kind()
                        )));
                    }
                };
                let (metadata, value) = response.into_parts();
                let entries = redis_value_collection_entries(&value);
                let output = RawCommandOutput {
                    command: metadata.name().to_string(),
                    value: redis_value_to_json(&value),
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

pub(super) fn decode_input(
    value: &str,
    encoding: InputEncoding,
    name: &str,
) -> tower_mcp::Result<Vec<u8>> {
    match encoding {
        InputEncoding::Utf8 => Ok(value.as_bytes().to_vec()),
        InputEncoding::Base64 => BASE64
            .decode(value)
            .map_err(|_| tower_mcp::Error::tool(format!("{name} is not valid standard base64"))),
    }
}

pub(super) fn optional_bytes(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Option<Vec<u8>>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::BulkString(value) => Ok(Some(value)),
        RedisValue::SimpleString(value) => Ok(Some(value.into_bytes())),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

pub(super) fn encode_bytes(bytes: Vec<u8>) -> (String, ValueEncoding) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_values_are_explicitly_encoded() {
        let json = redis_value_to_json(&RedisValue::BulkString(vec![0xff, 0x00]));
        assert_eq!(json["encoding"], "base64");
        assert_eq!(json["value"], "/wA=");
    }
}
