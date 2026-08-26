//! Bounded Lua scripting and Redis Functions operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, command, decode_input, destructive_annotations, output_limit_result,
    output_schema, read_annotations, redis_value_collection_entries, redis_value_to_json,
    write_annotations,
};
use crate::{AccessMode, RedisDeployment, RedisValue};

const DEFAULT_CLUSTER_NODE_LIMIT: usize = 32;
const MAX_CLUSTER_NODE_LIMIT: usize = 256;
const MAX_SCRIPT_BYTES: usize = 1024 * 1024;
const MAX_FUNCTION_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 64 * 1024;
const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_INVOCATION_BYTES: usize = 4 * 1024 * 1024;
const MAX_KEYS: usize = 1_000;
const MAX_ARGUMENTS: usize = 1_000;
const MAX_DIGESTS: usize = 1_000;
const MAX_NAME_BYTES: usize = 256;
const DEFAULT_DUMP_MAX_BYTES: usize = 64 * 1024;

type BinaryValues = Vec<Vec<u8>>;
type DecodedInvocation = (BinaryValues, BinaryValues);

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(eval_tool(state.clone(), true));
    router = router.tool(evalsha_tool(state.clone(), true));
    router = router.tool(fcall_tool(state.clone(), true));
    router.tool(script_exists_tool(state))
}

pub(super) fn add_full_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(eval_tool(state.clone(), false));
    router = router.tool(evalsha_tool(state.clone(), false));
    router = router.tool(fcall_tool(state.clone(), false));
    router = router.tool(script_load_tool(state.clone()));
    router = router.tool(script_flush_tool(state.clone()));
    router = router.tool(script_kill_tool(state.clone()));
    router = router.tool(function_list_tool(state.clone()));
    router = router.tool(function_stats_tool(state.clone()));
    router = router.tool(function_dump_tool(state.clone()));
    router = router.tool(function_load_tool(state.clone()));
    router = router.tool(function_restore_tool(state.clone()));
    router = router.tool(function_delete_tool(state.clone()));
    router = router.tool(function_flush_tool(state.clone()));
    router.tool(function_kill_tool(state))
}

fn default_cluster_node_limit() -> usize {
    DEFAULT_CLUSTER_NODE_LIMIT
}

fn default_dump_max_bytes() -> usize {
    DEFAULT_DUMP_MAX_BYTES
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedInput {
    /// UTF-8 text or standard base64, according to `encoding`.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

impl EncodedInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.value, self.encoding, name)
    }
}

fn validate_cluster_node_limit(limit: usize) -> tower_mcp::Result<()> {
    if limit == 0 || limit > MAX_CLUSTER_NODE_LIMIT {
        Err(tower_mcp::Error::tool(format!(
            "max_cluster_nodes must be between 1 and {MAX_CLUSTER_NODE_LIMIT}"
        )))
    } else {
        Ok(())
    }
}

fn validate_bytes(value: &[u8], max_bytes: usize, name: &str) -> tower_mcp::Result<()> {
    if value.len() > max_bytes {
        Err(tower_mcp::Error::tool(format!(
            "{name} is {} bytes; maximum is {max_bytes}",
            value.len()
        )))
    } else {
        Ok(())
    }
}

fn validate_name(value: &str, name: &str) -> tower_mcp::Result<()> {
    if value.is_empty() {
        return Err(tower_mcp::Error::tool(format!("{name} must not be empty")));
    }
    if value
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(tower_mcp::Error::tool(format!(
            "{name} must not contain whitespace or control characters"
        )));
    }
    validate_bytes(value.as_bytes(), MAX_NAME_BYTES, name)
}

fn validate_sha1(value: &str, name: &str) -> tower_mcp::Result<()> {
    if value.len() != 40 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Err(tower_mcp::Error::tool(format!(
            "{name} must be a 40-character hexadecimal SHA1 digest"
        )))
    } else {
        Ok(())
    }
}

fn decode_values(
    values: &[EncodedInput],
    max_items: usize,
    max_item_bytes: usize,
    name: &str,
) -> tower_mcp::Result<Vec<Vec<u8>>> {
    if values.len() > max_items {
        return Err(tower_mcp::Error::tool(format!(
            "{name} contains {} items; maximum is {max_items}",
            values.len()
        )));
    }
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let value = value.decode(&format!("{name}[{index}].value"))?;
            validate_bytes(&value, max_item_bytes, &format!("{name}[{index}]"))?;
            Ok(value)
        })
        .collect()
}

fn decode_invocation(
    keys: &[EncodedInput],
    arguments: &[EncodedInput],
) -> tower_mcp::Result<DecodedInvocation> {
    let keys = decode_values(keys, MAX_KEYS, MAX_KEY_BYTES, "keys")?;
    let arguments = decode_values(arguments, MAX_ARGUMENTS, MAX_ARGUMENT_BYTES, "arguments")?;
    let total_bytes = keys
        .iter()
        .chain(arguments.iter())
        .fold(0_usize, |total, value| total.saturating_add(value.len()));
    if total_bytes > MAX_TOTAL_INVOCATION_BYTES {
        return Err(tower_mcp::Error::tool(format!(
            "declared keys and arguments contain {total_bytes} bytes; maximum is {MAX_TOTAL_INVOCATION_BYTES}"
        )));
    }
    Ok((keys, arguments))
}

fn invocation_command(
    tool_name: &'static str,
    required_access: AccessMode,
    command_name: &'static str,
    subject: Vec<u8>,
    keys: &[Vec<u8>],
    arguments: &[Vec<u8>],
) -> crate::RedisCommand {
    let mut redis_command = command(tool_name, required_access, command_name);
    redis_command
        .arg(subject)
        .arg(keys.len().to_string())
        .args(keys.iter().cloned())
        .args(arguments.iter().cloned());
    redis_command
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterNodeReply {
    node: String,
    succeeded: bool,
    value: Option<JsonValue>,
    error_code: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterExecution {
    target: String,
    node_limit: usize,
    nodes_queried: usize,
    nodes_succeeded: usize,
    complete: bool,
    replies: Vec<ClusterNodeReply>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct OperationOutput {
    command: String,
    scope: String,
    key_count: usize,
    argument_count: usize,
    result: Option<JsonValue>,
    cluster: Option<ClusterExecution>,
}

fn operation_output(
    state: &ToolState,
    command_name: &str,
    value: RedisValue,
    key_count: usize,
    argument_count: usize,
    fanout: Option<(&str, usize)>,
) -> tower_mcp::Result<tower_mcp::CallToolResult> {
    let entries = redis_value_collection_entries(&value);
    let (scope, result, cluster) = match value {
        RedisValue::ClusterNodes(nodes) => {
            let (target, node_limit) = fanout.unwrap_or(("executor_selected", nodes.len()));
            if nodes.len() > node_limit {
                return Err(tower_mcp::Error::tool(format!(
                    "cluster node result size {} exceeds requested limit {node_limit}",
                    nodes.len()
                )));
            }
            let mut replies = Vec::with_capacity(nodes.len());
            let mut nodes_succeeded = 0_usize;
            for (node, value) in nodes {
                match value {
                    RedisValue::ServerError { code, .. } => replies.push(ClusterNodeReply {
                        node,
                        succeeded: false,
                        value: None,
                        error_code: Some(code),
                    }),
                    value => {
                        nodes_succeeded += 1;
                        replies.push(ClusterNodeReply {
                            node,
                            succeeded: true,
                            value: Some(redis_value_to_json(&value)),
                            error_code: None,
                        });
                    }
                }
            }
            if nodes_succeeded == 0 && !replies.is_empty() {
                let codes = replies
                    .iter()
                    .filter_map(|reply| reply.error_code.as_deref())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(tower_mcp::Error::tool(format!(
                    "{command_name} failed on every cluster node (codes: {codes})"
                )));
            }
            let nodes_queried = replies.len();
            (
                format!("cluster_{target}"),
                None,
                Some(ClusterExecution {
                    target: target.to_string(),
                    node_limit,
                    nodes_queried,
                    nodes_succeeded,
                    complete: nodes_succeeded == nodes_queried,
                    replies,
                }),
            )
        }
        value => (
            if state.deployment() == RedisDeployment::Cluster {
                "one_cluster_node".to_string()
            } else {
                "configured_target".to_string()
            },
            Some(redis_value_to_json(&value)),
            None,
        ),
    };
    state.output_collection(
        &OperationOutput {
            command: command_name.to_string(),
            scope,
            key_count,
            argument_count,
            result,
            cluster,
        },
        entries,
        "Narrow the function/script result or reduce the requested Cluster node limit.",
    )
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EvalInput {
    /// Binary-safe Lua source, limited to 1 MiB.
    script: EncodedInput,
    /// Every Redis key the script may access. Cluster keys must share one slot.
    #[serde(default)]
    keys: Vec<EncodedInput>,
    /// Binary-safe ARGV values supplied after the declared keys.
    #[serde(default)]
    arguments: Vec<EncodedInput>,
}

fn eval_tool(state: Arc<ToolState>, read_only: bool) -> Tool {
    let (tool_name, command_name, title, access) = if read_only {
        (
            "redis_eval_ro",
            "EVAL_RO",
            "Run Read-Only Redis Lua",
            AccessMode::ReadOnly,
        )
    } else {
        ("redis_eval", "EVAL", "Run Redis Lua", AccessMode::Full)
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(if read_only {
            "Run a bounded binary-safe Lua script through EVAL_RO with every key declared explicitly. Results obey global entry/byte budgets. The request timeout only bounds client waiting and does not guarantee server-side cancellation. Requires Redis 7+."
        } else {
            "Run a bounded binary-safe Lua script through EVAL with every key declared explicitly. The script can perform arbitrary writes or deletions, so full access is required. Results obey global entry/byte budgets. The request timeout only bounds client waiting and does not guarantee server-side cancellation."
        })
        .output_schema(output_schema::<OperationOutput>())
        .annotations(if read_only {
            read_annotations()
        } else {
            destructive_annotations(false)
        })
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<EvalInput>| async move {
                let script = input.script.decode("script.value")?;
                validate_bytes(&script, MAX_SCRIPT_BYTES, "script")?;
                let (keys, arguments) = decode_invocation(&input.keys, &input.arguments)?;
                let redis_command = invocation_command(
                    tool_name,
                    access,
                    command_name,
                    script,
                    &keys,
                    &arguments,
                );
                let value = state
                    .raw(redis_command, &format!("{command_name} failed"))
                    .await?;
                operation_output(
                    &state,
                    command_name,
                    value,
                    keys.len(),
                    arguments.len(),
                    None,
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EvalshaInput {
    /// SHA1 digest returned by SCRIPT LOAD.
    sha1: String,
    /// Every Redis key the cached script may access. Cluster keys must share one slot.
    #[serde(default)]
    keys: Vec<EncodedInput>,
    /// Binary-safe ARGV values supplied after the declared keys.
    #[serde(default)]
    arguments: Vec<EncodedInput>,
}

fn evalsha_tool(state: Arc<ToolState>, read_only: bool) -> Tool {
    let (tool_name, command_name, title, access) = if read_only {
        (
            "redis_evalsha_ro",
            "EVALSHA_RO",
            "Run Read-Only Cached Redis Lua",
            AccessMode::ReadOnly,
        )
    } else {
        (
            "redis_evalsha",
            "EVALSHA",
            "Run Cached Redis Lua",
            AccessMode::Full,
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(if read_only {
            "Run a cached read-only Lua script by SHA1 with explicit binary-safe keys and arguments. NOSCRIPT is returned as a stable tool failure; callers may load the source explicitly. Requires Redis 7+."
        } else {
            "Run a cached Lua script by SHA1 with explicit binary-safe keys and arguments. The script can perform arbitrary writes or deletions, so full access is required. NOSCRIPT is returned as a stable tool failure."
        })
        .output_schema(output_schema::<OperationOutput>())
        .annotations(if read_only {
            read_annotations()
        } else {
            destructive_annotations(false)
        })
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<EvalshaInput>| async move {
                validate_sha1(&input.sha1, "sha1")?;
                let (keys, arguments) = decode_invocation(&input.keys, &input.arguments)?;
                let redis_command = invocation_command(
                    tool_name,
                    access,
                    command_name,
                    input.sha1.to_ascii_lowercase().into_bytes(),
                    &keys,
                    &arguments,
                );
                let value = state
                    .raw(redis_command, &format!("{command_name} failed"))
                    .await?;
                operation_output(
                    &state,
                    command_name,
                    value,
                    keys.len(),
                    arguments.len(),
                    None,
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FcallInput {
    /// Registered Redis function name.
    function: String,
    /// Every Redis key the function may access. Cluster keys must share one slot.
    #[serde(default)]
    keys: Vec<EncodedInput>,
    /// Binary-safe arguments supplied after the declared keys.
    #[serde(default)]
    arguments: Vec<EncodedInput>,
}

fn fcall_tool(state: Arc<ToolState>, read_only: bool) -> Tool {
    let (tool_name, command_name, title, access) = if read_only {
        (
            "redis_fcall_ro",
            "FCALL_RO",
            "Run Read-Only Redis Function",
            AccessMode::ReadOnly,
        )
    } else {
        (
            "redis_fcall",
            "FCALL",
            "Run Redis Function",
            AccessMode::Full,
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(if read_only {
            "Run a registered Redis 7+ read-only function with every binary-safe key declared explicitly. Results obey global output budgets; request timeout does not guarantee server-side cancellation."
        } else {
            "Run a registered Redis 7+ function with every binary-safe key declared explicitly. The function can perform arbitrary writes or deletions, so full access is required. Results obey global output budgets; request timeout does not guarantee server-side cancellation."
        })
        .output_schema(output_schema::<OperationOutput>())
        .annotations(if read_only {
            read_annotations()
        } else {
            destructive_annotations(false)
        })
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<FcallInput>| async move {
                validate_name(&input.function, "function")?;
                let (keys, arguments) = decode_invocation(&input.keys, &input.arguments)?;
                let redis_command = invocation_command(
                    tool_name,
                    access,
                    command_name,
                    input.function.into_bytes(),
                    &keys,
                    &arguments,
                );
                let value = state
                    .raw(redis_command, &format!("{command_name} failed"))
                    .await?;
                operation_output(
                    &state,
                    command_name,
                    value,
                    keys.len(),
                    arguments.len(),
                    None,
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScriptExistsInput {
    /// One to 1000 SHA1 digests to inspect.
    #[schemars(length(min = 1, max = 1000))]
    sha1: Vec<String>,
    /// Maximum primary shards that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn script_exists_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_script_exists")
        .title("Inspect Redis Script Cache")
        .description("Check a bounded SHA1 list in every Redis Cluster primary script cache. Standalone returns one result; Cluster keeps per-primary replies and partial failures explicit.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ScriptExistsInput>| async move {
                if input.sha1.is_empty() || input.sha1.len() > MAX_DIGESTS {
                    return Err(tower_mcp::Error::tool(format!(
                        "sha1 must contain between 1 and {MAX_DIGESTS} digests"
                    )));
                }
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                for (index, digest) in input.sha1.iter().enumerate() {
                    validate_sha1(digest, &format!("sha1[{index}]"))?;
                }
                let mut redis_command = command(
                    "redis_script_exists",
                    AccessMode::ReadOnly,
                    "SCRIPT",
                );
                redis_command
                    .arg("EXISTS")
                    .args(input.sha1.iter().map(|sha1| sha1.to_ascii_lowercase()))
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "SCRIPT EXISTS failed").await?;
                operation_output(
                    &state,
                    "SCRIPT EXISTS",
                    value,
                    0,
                    input.sha1.len(),
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScriptLoadInput {
    /// Binary-safe Lua source, limited to 1 MiB.
    script: EncodedInput,
    /// Maximum nodes that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn script_load_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_script_load")
        .title("Load Redis Script Cache")
        .description("Load bounded binary-safe Lua source into every Redis Cluster node script cache, retaining per-node SHA1 replies and partial failures. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ScriptLoadInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let script = input.script.decode("script.value")?;
                validate_bytes(&script, MAX_SCRIPT_BYTES, "script")?;
                let mut redis_command = command("redis_script_load", AccessMode::Full, "SCRIPT");
                redis_command
                    .arg("LOAD")
                    .arg(script)
                    .aggregate_cluster_nodes(input.max_cluster_nodes);
                let value = state.raw(redis_command, "SCRIPT LOAD failed").await?;
                operation_output(
                    &state,
                    "SCRIPT LOAD",
                    value,
                    0,
                    1,
                    Some(("all_nodes", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FlushMode {
    #[default]
    Sync,
    Async,
}

impl FlushMode {
    const fn as_redis(self) -> &'static str {
        match self {
            Self::Sync => "SYNC",
            Self::Async => "ASYNC",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterFlushInput {
    /// Whether Redis performs the flush synchronously or asynchronously.
    #[serde(default)]
    mode: FlushMode,
    /// Maximum nodes that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn script_flush_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_script_flush")
        .title("Flush Redis Script Cache")
        .description("Permanently flush every Redis Cluster node script cache with an explicit SYNC or ASYNC mode. Partial node failures remain visible. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterFlushInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_script_flush", AccessMode::Full, "SCRIPT");
                redis_command.arg("FLUSH");
                let argument_count = match input.mode {
                    FlushMode::Sync => 0,
                    FlushMode::Async => {
                        if state
                            .redis_version()
                            .is_some_and(|version| version < crate::RedisVersion::new(6, 2, 0))
                        {
                            return Err(tower_mcp::Error::tool(
                                "SCRIPT FLUSH ASYNC requires Redis 6.2 or newer",
                            ));
                        }
                        redis_command.arg("ASYNC");
                        1
                    }
                };
                redis_command.aggregate_cluster_nodes(input.max_cluster_nodes);
                let value = state.raw(redis_command, "SCRIPT FLUSH failed").await?;
                operation_output(
                    &state,
                    "SCRIPT FLUSH",
                    value,
                    0,
                    argument_count,
                    Some(("all_nodes", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterOnlyInput {
    /// Maximum primary shards that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn script_kill_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_script_kill")
        .title("Kill Running Redis Scripts")
        .description("Attempt SCRIPT KILL on every Redis Cluster primary. Redis only kills scripts that have not performed writes; per-primary outcomes are explicit. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterOnlyInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_script_kill", AccessMode::Full, "SCRIPT");
                redis_command
                    .arg("KILL")
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "SCRIPT KILL failed").await?;
                operation_output(
                    &state,
                    "SCRIPT KILL",
                    value,
                    0,
                    0,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FunctionListInput {
    /// Optional Redis glob pattern for library names.
    #[serde(default)]
    library_name: Option<String>,
    /// Include library source code. This can substantially increase output.
    #[serde(default)]
    include_code: bool,
}

fn function_list_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_list")
        .title("List Redis Function Libraries")
        .description("Inspect Redis 7+ function libraries on one configured node with an optional library-name pattern and explicit source-code inclusion. Output is globally budgeted. Full access is required because source code may contain sensitive logic.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FunctionListInput>| async move {
                let mut redis_command = command("redis_function_list", AccessMode::Full, "FUNCTION");
                redis_command.arg("LIST");
                let mut argument_count = 0;
                if let Some(library_name) = input.library_name {
                    validate_name(&library_name, "library_name")?;
                    redis_command.arg("LIBRARYNAME").arg(library_name);
                    argument_count += 1;
                }
                if input.include_code {
                    redis_command.arg("WITHCODE");
                    argument_count += 1;
                }
                let value = state.raw(redis_command, "FUNCTION LIST failed").await?;
                operation_output(
                    &state,
                    "FUNCTION LIST",
                    value,
                    0,
                    argument_count,
                    None,
                )
            },
        )
        .build()
}

fn function_stats_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_stats")
        .title("Inspect Redis Function Runtime")
        .description("Read bounded Redis 7+ function runtime statistics from every Cluster primary with partial outcomes explicit. Full access is required because running-function metadata can expose operational details.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterOnlyInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_function_stats", AccessMode::Full, "FUNCTION");
                redis_command
                    .arg("STATS")
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION STATS failed").await?;
                operation_output(
                    &state,
                    "FUNCTION STATS",
                    value,
                    0,
                    0,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FunctionDumpInput {
    /// Maximum raw dump payload bytes accepted before returning an output-limit result.
    #[serde(default = "default_dump_max_bytes")]
    #[schemars(range(min = 1, max = 4194304))]
    max_bytes: usize,
}

fn function_dump_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_dump")
        .title("Dump Redis Function Libraries")
        .description("Read a binary-safe Redis 7+ function-library dump from one configured node. The caller supplies a raw payload ceiling and the encoded response also obeys the global output budget. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FunctionDumpInput>| async move {
                if input.max_bytes == 0 || input.max_bytes > MAX_FUNCTION_PAYLOAD_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "max_bytes must be between 1 and {MAX_FUNCTION_PAYLOAD_BYTES}"
                    )));
                }
                let mut redis_command = command("redis_function_dump", AccessMode::Full, "FUNCTION");
                redis_command.arg("DUMP");
                let value = state.raw(redis_command, "FUNCTION DUMP failed").await?;
                let raw_bytes = match &value {
                    RedisValue::BulkString(value) | RedisValue::BigNumber(value) => value.len(),
                    RedisValue::SimpleString(value) => value.len(),
                    other => {
                        return Err(tower_mcp::Error::tool(format!(
                            "FUNCTION DUMP returned an unexpected reply: {other:?}"
                        )));
                    }
                };
                if raw_bytes > input.max_bytes {
                    return Ok(output_limit_result(
                        "raw_bytes",
                        raw_bytes,
                        input.max_bytes,
                        "Increase max_bytes within the hard 4 MiB ceiling or narrow the deployed function libraries.",
                    ));
                }
                operation_output(&state, "FUNCTION DUMP", value, 0, 0, None)
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FunctionLoadInput {
    /// Binary-safe library source including its Redis shebang metadata.
    library_code: EncodedInput,
    /// Replace an existing library with the same name.
    #[serde(default)]
    replace: bool,
    /// Maximum primary shards that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn function_load_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_load")
        .title("Load Redis Function Library")
        .description("Load bounded binary-safe Redis 7+ function-library source on every Cluster primary. Replacement is explicit, partial outcomes remain visible, and full access is required.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FunctionLoadInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let library_code = input.library_code.decode("library_code.value")?;
                validate_bytes(
                    &library_code,
                    MAX_FUNCTION_PAYLOAD_BYTES,
                    "library_code",
                )?;
                let mut redis_command = command("redis_function_load", AccessMode::Full, "FUNCTION");
                redis_command.arg("LOAD");
                if input.replace {
                    redis_command.arg("REPLACE");
                }
                redis_command
                    .arg(library_code)
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION LOAD failed").await?;
                operation_output(
                    &state,
                    "FUNCTION LOAD",
                    value,
                    0,
                    1,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum RestorePolicy {
    #[default]
    Flush,
    Append,
    Replace,
}

impl RestorePolicy {
    const fn as_redis(self) -> &'static str {
        match self {
            Self::Flush => "FLUSH",
            Self::Append => "APPEND",
            Self::Replace => "REPLACE",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FunctionRestoreInput {
    /// Binary-safe payload returned by FUNCTION DUMP.
    payload: EncodedInput,
    /// Restore collision policy. `flush` removes all existing libraries first.
    #[serde(default)]
    policy: RestorePolicy,
    /// Maximum primary shards that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn function_restore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_restore")
        .title("Restore Redis Function Libraries")
        .description("Restore a bounded binary-safe Redis 7+ FUNCTION DUMP payload on every Cluster primary with an explicit flush/append/replace policy. Partial outcomes remain visible. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FunctionRestoreInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let payload = input.payload.decode("payload.value")?;
                validate_bytes(&payload, MAX_FUNCTION_PAYLOAD_BYTES, "payload")?;
                let mut redis_command = command("redis_function_restore", AccessMode::Full, "FUNCTION");
                redis_command
                    .arg("RESTORE")
                    .arg(payload)
                    .arg(input.policy.as_redis())
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION RESTORE failed").await?;
                operation_output(
                    &state,
                    "FUNCTION RESTORE",
                    value,
                    0,
                    1,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FunctionDeleteInput {
    /// Exact function-library name to delete.
    library_name: String,
    /// Maximum primary shards that may participate in Cluster fan-out.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn function_delete_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_delete")
        .title("Delete Redis Function Library")
        .description("Permanently delete one named Redis 7+ function library from every Cluster primary with partial outcomes explicit. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FunctionDeleteInput>| async move {
                validate_name(&input.library_name, "library_name")?;
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_function_delete", AccessMode::Full, "FUNCTION");
                redis_command
                    .arg("DELETE")
                    .arg(input.library_name)
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION DELETE failed").await?;
                operation_output(
                    &state,
                    "FUNCTION DELETE",
                    value,
                    0,
                    1,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

fn function_flush_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_flush")
        .title("Flush Redis Function Libraries")
        .description("Permanently flush every Redis 7+ function library from every Cluster primary with an explicit SYNC or ASYNC mode. Partial outcomes remain visible. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterFlushInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_function_flush", AccessMode::Full, "FUNCTION");
                redis_command
                    .arg("FLUSH")
                    .arg(input.mode.as_redis())
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION FLUSH failed").await?;
                operation_output(
                    &state,
                    "FUNCTION FLUSH",
                    value,
                    0,
                    1,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

fn function_kill_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_function_kill")
        .title("Kill Running Redis Functions")
        .description("Attempt FUNCTION KILL on every Redis Cluster primary. Redis only kills functions that have not performed writes; per-primary outcomes are explicit. Requires full access.")
        .output_schema(output_schema::<OperationOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterOnlyInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_function_kill", AccessMode::Full, "FUNCTION");
                redis_command
                    .arg("KILL")
                    .aggregate_cluster_primaries(input.max_cluster_nodes);
                let value = state.raw(redis_command, "FUNCTION KILL failed").await?;
                operation_output(
                    &state,
                    "FUNCTION KILL",
                    value,
                    0,
                    0,
                    Some(("primaries", input.max_cluster_nodes)),
                )
            },
        )
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_validation_is_strict_and_case_insensitive() {
        assert!(validate_sha1("0123456789abcdef0123456789ABCDEF01234567", "sha1").is_ok());
        assert!(validate_sha1("not-a-digest", "sha1").is_err());
        assert!(validate_sha1("g123456789abcdef0123456789abcdef01234567", "sha1").is_err());
    }

    #[test]
    fn invocation_command_declares_keys_before_binary_arguments() {
        let command = invocation_command(
            "redis_eval_ro",
            AccessMode::ReadOnly,
            "EVAL_RO",
            b"return ARGV[1]".to_vec(),
            &[b"key:{tenant}".to_vec()],
            &[vec![0xff, 0x00]],
        );
        assert_eq!(command.name(), "EVAL_RO");
        assert_eq!(
            command.arguments(),
            &[
                b"return ARGV[1]".to_vec(),
                b"1".to_vec(),
                b"key:{tenant}".to_vec(),
                vec![0xff, 0x00],
            ]
        );
    }

    #[test]
    fn input_limits_reject_oversized_items_and_totals() {
        let oversized = EncodedInput {
            value: "x".repeat(MAX_KEY_BYTES + 1),
            encoding: InputEncoding::Utf8,
        };
        assert!(decode_invocation(&[oversized], &[]).is_err());

        let too_many = vec![
            EncodedInput {
                value: String::new(),
                encoding: InputEncoding::Utf8,
            };
            MAX_ARGUMENTS + 1
        ];
        assert!(decode_invocation(&[], &too_many).is_err());
    }
}
