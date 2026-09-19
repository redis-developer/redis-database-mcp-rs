//! Bounded, redacted Redis diagnostics and server inspection.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use crate::response::FromRedisValue;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, PageMetadata, ToolState, ValueEncoding, command, decode_input, encode_bytes,
    output_schema, read_annotations,
};
use crate::{AccessMode, RedisDeployment, RedisValue, invocation::redis_value_to_json};

const DEFAULT_CLUSTER_NODE_LIMIT: usize = 32;
const MAX_CLUSTER_NODE_LIMIT: usize = 256;
const DEFAULT_CLIENT_LIMIT: usize = 100;
const DEFAULT_SLOWLOG_LIMIT: usize = 10;
const DEFAULT_LATENCY_LIMIT: usize = 100;
const DEFAULT_SCAN_COUNT: usize = 100;
const DEFAULT_HOTKEY_LIMIT: usize = 100;
const DEFAULT_HOTKEY_TOP: usize = 10;
const MAX_FILTER_BYTES: usize = 1024;
const MAX_EVENT_BYTES: usize = 256;
const MAX_HOTKEYS_PER_PAGE: usize = 256;

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(client_list_tool(state.clone()));
    router = router.tool(cluster_info_tool(state.clone()));
    router = router.tool(memory_stats_tool(state.clone()));
    router = router.tool(module_list_tool(state.clone()));
    router = router.tool(slowlog_tool(state.clone()));
    router = router.tool(latency_history_tool(state.clone()));
    router = router.tool(acl_whoami_tool(state.clone()));
    router = router.tool(health_check_tool(state.clone()));
    router = router.tool(connection_summary_tool(state.clone()));
    router = router.tool(keyspace_summary_tool(state.clone()));
    router = router.tool(memory_summary_tool(state.clone()));
    router = router.tool(key_summary_tool(state.clone()));
    router.tool(hotkeys_tool(state))
}

fn default_cluster_node_limit() -> usize {
    DEFAULT_CLUSTER_NODE_LIMIT
}

fn default_client_limit() -> usize {
    DEFAULT_CLIENT_LIMIT
}

fn default_slowlog_limit() -> usize {
    DEFAULT_SLOWLOG_LIMIT
}

fn default_latency_limit() -> usize {
    DEFAULT_LATENCY_LIMIT
}

fn default_scan_count() -> usize {
    DEFAULT_SCAN_COUNT
}

fn default_hotkey_limit() -> usize {
    DEFAULT_HOTKEY_LIMIT
}

fn default_hotkey_top() -> usize {
    DEFAULT_HOTKEY_TOP
}

fn default_pattern() -> String {
    "*".to_string()
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedValue {
    value: String,
    encoding: ValueEncoding,
}

impl EncodedValue {
    fn new(value: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(value);
        Self { value, encoding }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterNodeFailure {
    node: String,
    code: String,
    message_redacted: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterAggregation {
    node_limit: usize,
    nodes_queried: usize,
    nodes_succeeded: usize,
    complete: bool,
    node_addresses_redacted: bool,
    failures: Vec<ClusterNodeFailure>,
}

struct NodeReplies {
    values: Vec<(Option<String>, RedisValue)>,
    cluster: Option<ClusterAggregation>,
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

fn validate_limit(state: &ToolState, limit: usize, name: &str) -> tower_mcp::Result<()> {
    state.validate_requested_entries(limit, name)
}

fn validate_per_node_limit(
    state: &ToolState,
    limit: usize,
    max_cluster_nodes: usize,
    name: &str,
) -> tower_mcp::Result<()> {
    validate_limit(state, limit, name)?;
    if state.deployment() == RedisDeployment::Cluster {
        let total = limit.checked_mul(max_cluster_nodes).ok_or_else(|| {
            tower_mcp::Error::tool(format!("{name} multiplied by max_cluster_nodes overflowed"))
        })?;
        if total > state.max_collection_entries() {
            return Err(tower_mcp::Error::tool(format!(
                "{name} times max_cluster_nodes must not exceed the configured output limit of {} entries",
                state.max_collection_entries()
            )));
        }
    }
    Ok(())
}

fn validate_bytes(value: &str, max: usize, name: &str) -> tower_mcp::Result<()> {
    if value.len() > max {
        Err(tower_mcp::Error::tool(format!(
            "{name} is {} bytes; maximum is {max}",
            value.len()
        )))
    } else {
        Ok(())
    }
}

fn require_sensitive(state: &ToolState, requested: bool, tool: &str) -> tower_mcp::Result<()> {
    if requested {
        state.require(AccessMode::Full, tool)
    } else {
        Ok(())
    }
}

fn split_node_replies(
    value: RedisValue,
    node_limit: usize,
    include_node_addresses: bool,
) -> tower_mcp::Result<NodeReplies> {
    let RedisValue::ClusterNodes(nodes) = value else {
        return Ok(NodeReplies {
            values: vec![(None, value)],
            cluster: None,
        });
    };
    if nodes.len() > node_limit {
        return Err(tower_mcp::Error::tool(format!(
            "cluster node result size {} exceeds requested limit {node_limit}",
            nodes.len()
        )));
    }

    let nodes_queried = nodes.len();
    let mut values = Vec::with_capacity(nodes_queried);
    let mut failures = Vec::new();
    for (index, (address, value)) in nodes.into_iter().enumerate() {
        let node = if include_node_addresses {
            address
        } else {
            format!("node-{}", index + 1)
        };
        match value {
            RedisValue::ServerError { code, .. } => failures.push(ClusterNodeFailure {
                node,
                code,
                message_redacted: true,
            }),
            value => values.push((Some(node), value)),
        }
    }
    let nodes_succeeded = values.len();
    Ok(NodeReplies {
        values,
        cluster: Some(ClusterAggregation {
            node_limit,
            nodes_queried,
            nodes_succeeded,
            complete: failures.is_empty(),
            node_addresses_redacted: !include_node_addresses,
            failures,
        }),
    })
}

fn reply_bytes(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => Ok(value),
        RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
            Ok(value.into_bytes())
        }
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply type"
        ))),
    }
}

fn reply_string(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    String::from_utf8(reply_bytes(value, context)?).map_err(|_| {
        tower_mcp::Error::tool(format!("{context} returned non-UTF-8 diagnostic text"))
    })
}

fn reply_i64(value: RedisValue, context: &str) -> tower_mcp::Result<i64> {
    match value {
        RedisValue::Integer(value) => Ok(value),
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| tower_mcp::Error::tool(format!("{context} was not an integer"))),
        RedisValue::SimpleString(value) => value
            .parse()
            .map_err(|_| tower_mcp::Error::tool(format!("{context} was not an integer"))),
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected integer reply"
        ))),
    }
}

fn reply_u64(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    u64::try_from(reply_i64(value, context)?)
        .map_err(|_| tower_mcp::Error::tool(format!("{context} was negative")))
}

fn reply_pairs(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<(RedisValue, RedisValue)>> {
    match value {
        RedisValue::Map(values) => Ok(values),
        RedisValue::Array(values) => {
            if values.len() % 2 != 0 {
                return Err(tower_mcp::Error::tool(format!(
                    "{context} returned an odd number of field/value elements"
                )));
            }
            let mut values = values.into_iter();
            let mut pairs = Vec::new();
            while let Some(key) = values.next() {
                let value = values.next().expect("even response length");
                pairs.push((key, value));
            }
            Ok(pairs)
        }
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected map reply"
        ))),
    }
}

fn parse_text_properties(raw: &str) -> BTreeMap<String, String> {
    raw.lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

fn optional_u64(properties: &BTreeMap<String, String>, key: &str) -> Option<u64> {
    properties.get(key).and_then(|value| value.parse().ok())
}

fn optional_f64(properties: &BTreeMap<String, String>, key: &str) -> Option<f64> {
    properties.get(key).and_then(|value| value.parse().ok())
}

fn optional_bool(properties: &BTreeMap<String, String>, key: &str) -> Option<bool> {
    properties.get(key).and_then(|value| match value.as_str() {
        "0" | "no" => Some(false),
        "1" | "yes" => Some(true),
        _ => None,
    })
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInput {
    /// Maximum cluster nodes that may participate in one all-node inspection.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
    /// Return actual cluster node addresses. Requires Full access; defaults to pseudonymous labels.
    #[serde(default)]
    include_node_addresses: bool,
}

fn validate_cluster_input(
    state: &ToolState,
    input: &ClusterInput,
    tool: &str,
) -> tower_mcp::Result<()> {
    validate_cluster_node_limit(input.max_cluster_nodes)?;
    require_sensitive(state, input.include_node_addresses, tool)
}

fn clustered_command(
    tool_name: &'static str,
    command_name: &'static str,
    max_cluster_nodes: usize,
) -> crate::RedisCommand {
    let mut redis_command = command(tool_name, AccessMode::ReadOnly, command_name);
    redis_command.aggregate_cluster_nodes(max_cluster_nodes);
    redis_command
}

fn redacted_diagnostic_error(error: tower_mcp::Error, context: &str) -> tower_mcp::Error {
    let rendered = error.to_string();
    let category = [
        "[Authentication]",
        "[Authorization]",
        "[Timeout]",
        "[Connection]",
        "[InvalidRequest]",
        "[InvalidResponse]",
        "[CapabilityUnavailable]",
        "[ModuleUnavailable]",
        "[OutputLimit]",
        "[Server]",
        "[Other]",
    ]
    .into_iter()
    .find(|category| rendered.contains(category))
    .unwrap_or("[Other]");
    tower_mcp::Error::tool(format!(
        "{context} {category}: Redis diagnostic error details were redacted"
    ))
}

async fn diagnostic_raw(
    state: &ToolState,
    redis_command: crate::RedisCommand,
    context: &str,
) -> tower_mcp::Result<RedisValue> {
    state
        .raw(redis_command, context)
        .await
        .map_err(|error| redacted_diagnostic_error(error, context))
}

async fn diagnostic_query<T: FromRedisValue>(
    state: &ToolState,
    redis_command: crate::RedisCommand,
    context: &str,
) -> tower_mcp::Result<T> {
    state
        .query(redis_command, context)
        .await
        .map_err(|error| redacted_diagnostic_error(error, context))
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DiagnosticField {
    key: EncodedValue,
    value: JsonValue,
}

fn diagnostic_fields(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<DiagnosticField>> {
    reply_pairs(value, context)?
        .into_iter()
        .map(|(key, value)| {
            Ok(DiagnosticField {
                key: EncodedValue::new(reply_bytes(key, context)?),
                value: redis_value_to_json(&value),
            })
        })
        .collect()
}

fn field_name(field: &DiagnosticField) -> Option<&str> {
    matches!(field.key.encoding, ValueEncoding::Utf8).then_some(field.key.value.as_str())
}

fn json_u64(value: &JsonValue) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| {
            value
                .get("value")
                .and_then(JsonValue::as_str)
                .and_then(|value| value.parse().ok())
        })
}

fn json_f64(value: &JsonValue) -> Option<f64> {
    value.as_f64().or_else(|| {
        value
            .get("value")
            .and_then(JsonValue::as_str)
            .and_then(|value| value.parse().ok())
    })
}

fn field_u64(fields: &[DiagnosticField], name: &str) -> Option<u64> {
    fields
        .iter()
        .find(|field| field_name(field) == Some(name))
        .and_then(|field| json_u64(&field.value))
}

fn field_f64(fields: &[DiagnosticField], name: &str) -> Option<f64> {
    fields
        .iter()
        .find(|field| field_name(field) == Some(name))
        .and_then(|field| json_f64(&field.value))
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ClientKindFilter {
    Normal,
    Master,
    Replica,
    Pubsub,
}

impl ClientKindFilter {
    fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Normal => b"normal",
            Self::Master => b"master",
            Self::Replica => b"replica",
            Self::Pubsub => b"pubsub",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClientListInput {
    /// Filter by Redis client type.
    #[serde(default)]
    client_type: Option<ClientKindFilter>,
    /// Filter by selected database number.
    #[serde(default)]
    database: Option<i64>,
    /// Filter by exact ACL username (maximum 1024 bytes).
    #[serde(default)]
    username: Option<String>,
    /// Filter by exact client name (maximum 1024 bytes).
    #[serde(default)]
    name: Option<String>,
    /// Filter by a client-address prefix (maximum 1024 bytes).
    #[serde(default)]
    address_prefix: Option<String>,
    /// Minimum idle time in seconds.
    #[serde(default)]
    minimum_idle_seconds: Option<u64>,
    /// Maximum idle time in seconds.
    #[serde(default)]
    maximum_idle_seconds: Option<u64>,
    /// Maximum clients returned across the target.
    #[serde(default = "default_client_limit")]
    #[schemars(range(min = 1, max = 1000))]
    max_results: usize,
    /// Maximum cluster nodes that may participate.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
    /// Include addresses, names, usernames, library identity, unknown fields, and real node addresses. Requires Full access.
    #[serde(default)]
    include_sensitive: bool,
}

#[derive(Debug)]
struct ParsedClient {
    fields: BTreeMap<String, Vec<u8>>,
}

impl ParsedClient {
    fn bytes(&self, key: &str) -> Option<&[u8]> {
        self.fields.get(key).map(Vec::as_slice)
    }

    fn string(&self, key: &str) -> Option<String> {
        self.bytes(key)
            .and_then(|value| std::str::from_utf8(value).ok())
            .map(str::to_string)
    }

    fn i64(&self, key: &str) -> Option<i64> {
        self.bytes(key)
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse().ok())
    }

    fn u64(&self, key: &str) -> Option<u64> {
        self.i64(key).and_then(|value| u64::try_from(value).ok())
    }

    fn client_type(&self) -> &'static str {
        if let Some(client_type) = self.bytes("type") {
            return match client_type {
                b"master" => "master",
                b"replica" | b"slave" => "replica",
                b"pubsub" => "pubsub",
                _ => "normal",
            };
        }
        match self.bytes("flags") {
            Some(flags) if flags.contains(&b'M') => "master",
            Some(flags) if flags.contains(&b'S') => "replica",
            Some(flags) if flags.contains(&b'P') => "pubsub",
            _ => "normal",
        }
    }
}

fn parse_clients(value: RedisValue) -> tower_mcp::Result<Vec<ParsedClient>> {
    let bytes = reply_bytes(value, "CLIENT LIST")?;
    bytes
        .split(|byte| *byte == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut fields = BTreeMap::new();
            for pair in line.split(|byte| byte.is_ascii_whitespace()) {
                let Some(separator) = pair.iter().position(|byte| *byte == b'=') else {
                    continue;
                };
                let key = std::str::from_utf8(&pair[..separator]).map_err(|_| {
                    tower_mcp::Error::tool("CLIENT LIST returned a non-UTF-8 field name")
                })?;
                fields.insert(key.to_string(), pair[separator + 1..].to_vec());
            }
            Ok(ParsedClient { fields })
        })
        .collect()
}

fn client_matches(client: &ParsedClient, input: &ClientListInput) -> bool {
    input
        .client_type
        .is_none_or(|kind| client.client_type().as_bytes() == kind.as_bytes())
        && input
            .database
            .is_none_or(|database| client.i64("db") == Some(database))
        && input
            .username
            .as_ref()
            .is_none_or(|username| client.bytes("user") == Some(username.as_bytes()))
        && input
            .name
            .as_ref()
            .is_none_or(|name| client.bytes("name") == Some(name.as_bytes()))
        && input.address_prefix.as_ref().is_none_or(|prefix| {
            client
                .bytes("addr")
                .is_some_and(|address| address.starts_with(prefix.as_bytes()))
        })
        && input
            .minimum_idle_seconds
            .is_none_or(|minimum| client.u64("idle").is_some_and(|idle| idle >= minimum))
        && input
            .maximum_idle_seconds
            .is_none_or(|maximum| client.u64("idle").is_some_and(|idle| idle <= maximum))
}

const CLIENT_KNOWN_FIELDS: &[&str] = &[
    "id",
    "addr",
    "laddr",
    "fd",
    "name",
    "age",
    "idle",
    "flags",
    "db",
    "sub",
    "psub",
    "ssub",
    "multi",
    "qbuf",
    "qbuf-free",
    "argv-mem",
    "multi-mem",
    "rbs",
    "rbp",
    "obl",
    "oll",
    "omem",
    "tot-mem",
    "events",
    "cmd",
    "user",
    "redir",
    "resp",
    "lib-name",
    "lib-ver",
    "io-thread",
    "tot-net-in",
    "tot-net-out",
    "tot-cmds",
    "type",
];

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClientRecord {
    node: Option<String>,
    id: Option<u64>,
    client_type: Option<String>,
    database: Option<i64>,
    age_seconds: Option<u64>,
    idle_seconds: Option<u64>,
    flags: Option<String>,
    subscriptions: Option<u64>,
    pattern_subscriptions: Option<u64>,
    sharded_subscriptions: Option<u64>,
    command: Option<String>,
    response_protocol: Option<i64>,
    total_memory_bytes: Option<u64>,
    total_network_input_bytes: Option<u64>,
    total_network_output_bytes: Option<u64>,
    total_commands: Option<u64>,
    address: Option<EncodedValue>,
    local_address: Option<EncodedValue>,
    name: Option<EncodedValue>,
    username: Option<EncodedValue>,
    library_name: Option<EncodedValue>,
    library_version: Option<EncodedValue>,
    sensitive_fields_redacted: bool,
    unknown_field_count: usize,
    unknown_fields: Option<BTreeMap<String, EncodedValue>>,
}

fn client_record(
    node: Option<String>,
    mut client: ParsedClient,
    include_sensitive: bool,
) -> ClientRecord {
    let unknown_keys = client
        .fields
        .keys()
        .filter(|key| !CLIENT_KNOWN_FIELDS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let unknown_field_count = unknown_keys.len();
    let unknown_fields = include_sensitive.then(|| {
        unknown_keys
            .into_iter()
            .filter_map(|key| {
                client
                    .fields
                    .remove(&key)
                    .map(|value| (key, EncodedValue::new(value)))
            })
            .collect()
    });
    let sensitive = |key: &str, client: &ParsedClient| {
        include_sensitive
            .then(|| client.fields.get(key).cloned())
            .flatten()
            .map(EncodedValue::new)
    };
    ClientRecord {
        node,
        id: client.u64("id"),
        client_type: Some(client.client_type().to_string()),
        database: client.i64("db"),
        age_seconds: client.u64("age"),
        idle_seconds: client.u64("idle"),
        flags: client.string("flags"),
        subscriptions: client.u64("sub"),
        pattern_subscriptions: client.u64("psub"),
        sharded_subscriptions: client.u64("ssub"),
        command: client.string("cmd"),
        response_protocol: client.i64("resp"),
        total_memory_bytes: client.u64("tot-mem"),
        total_network_input_bytes: client.u64("tot-net-in"),
        total_network_output_bytes: client.u64("tot-net-out"),
        total_commands: client.u64("tot-cmds"),
        address: sensitive("addr", &client),
        local_address: sensitive("laddr", &client),
        name: sensitive("name", &client),
        username: sensitive("user", &client),
        library_name: sensitive("lib-name", &client),
        library_version: sensitive("lib-ver", &client),
        sensitive_fields_redacted: !include_sensitive,
        unknown_field_count,
        unknown_fields,
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClientListOutput {
    deployment: String,
    matched: usize,
    returned: usize,
    truncated: bool,
    clients: Vec<ClientRecord>,
    cluster: Option<ClusterAggregation>,
}

fn client_list_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_client_list")
        .title("List Redis Clients")
        .description(
            "List a bounded, filterable set of structured Redis clients. Addresses, names, usernames, library identity, unknown fields, and cluster node addresses are redacted unless explicitly requested under Full access.",
        )
        .output_schema(output_schema::<ClientListOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClientListInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                validate_limit(&state, input.max_results, "max_results")?;
                require_sensitive(&state, input.include_sensitive, "redis_client_list sensitive fields")?;
                for (name, value) in [
                    ("username", input.username.as_deref()),
                    ("name", input.name.as_deref()),
                    ("address_prefix", input.address_prefix.as_deref()),
                ] {
                    if let Some(value) = value {
                        validate_bytes(value, MAX_FILTER_BYTES, name)?;
                    }
                }
                if input.minimum_idle_seconds.zip(input.maximum_idle_seconds).is_some_and(
                    |(minimum, maximum)| minimum > maximum,
                ) {
                    return Err(tower_mcp::Error::tool(
                        "minimum_idle_seconds must not exceed maximum_idle_seconds",
                    ));
                }

                let mut redis_command = clustered_command(
                    "redis_client_list",
                    "CLIENT",
                    input.max_cluster_nodes,
                );
                redis_command.arg("LIST");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "CLIENT LIST failed").await?,
                    input.max_cluster_nodes,
                    input.include_sensitive,
                )?;
                let mut matched = 0_usize;
                let mut clients = Vec::new();
                for (node, value) in replies.values {
                    for client in parse_clients(value)? {
                        if !client_matches(&client, &input) {
                            continue;
                        }
                        matched = matched.saturating_add(1);
                        if clients.len() < input.max_results {
                            clients.push(client_record(
                                node.clone(),
                                client,
                                input.include_sensitive,
                            ));
                        }
                    }
                }
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let output = ClientListOutput {
                    deployment: deployment.as_str().to_string(),
                    matched,
                    returned: clients.len(),
                    truncated: matched > clients.len(),
                    clients,
                    cluster: replies.cluster,
                };
                state.output_collection(
                    &output,
                    output.returned,
                    "Retry CLIENT LIST with narrower filters or a smaller max_results.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInfoNode {
    node: Option<String>,
    state: Option<String>,
    slots_assigned: Option<u64>,
    slots_ok: Option<u64>,
    slots_pfail: Option<u64>,
    slots_fail: Option<u64>,
    known_nodes: Option<u64>,
    cluster_size: Option<u64>,
    current_epoch: Option<u64>,
    messages_sent: Option<u64>,
    messages_received: Option<u64>,
    properties: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInfoOutput {
    healthy_nodes: usize,
    unhealthy_nodes: usize,
    nodes: Vec<ClusterInfoNode>,
    cluster: Option<ClusterAggregation>,
}

fn cluster_info_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_cluster_info")
        .title("Inspect Redis Cluster")
        .description(
            "Read structured CLUSTER INFO summaries through a bounded all-node fan-out. Unknown fields are retained and partial node failures are explicit; real node addresses require Full access.",
        )
        .output_schema(output_schema::<ClusterInfoOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_cluster_info node addresses")?;
                let mut redis_command = clustered_command(
                    "redis_cluster_info",
                    "CLUSTER",
                    input.max_cluster_nodes,
                );
                redis_command.arg("INFO");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "CLUSTER INFO failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let mut healthy_nodes = 0;
                let mut unhealthy_nodes = 0;
                let mut nodes = Vec::new();
                for (node, value) in replies.values {
                    let properties = parse_text_properties(&reply_string(value, "CLUSTER INFO")?);
                    let state_value = properties.get("cluster_state").cloned();
                    if state_value.as_deref() == Some("ok") {
                        healthy_nodes += 1;
                    } else {
                        unhealthy_nodes += 1;
                    }
                    nodes.push(ClusterInfoNode {
                        node,
                        state: state_value,
                        slots_assigned: optional_u64(&properties, "cluster_slots_assigned"),
                        slots_ok: optional_u64(&properties, "cluster_slots_ok"),
                        slots_pfail: optional_u64(&properties, "cluster_slots_pfail"),
                        slots_fail: optional_u64(&properties, "cluster_slots_fail"),
                        known_nodes: optional_u64(&properties, "cluster_known_nodes"),
                        cluster_size: optional_u64(&properties, "cluster_size"),
                        current_epoch: optional_u64(&properties, "cluster_current_epoch"),
                        messages_sent: optional_u64(&properties, "cluster_stats_messages_sent"),
                        messages_received: optional_u64(
                            &properties,
                            "cluster_stats_messages_received",
                        ),
                        properties,
                    });
                }
                let entries = nodes
                    .iter()
                    .fold(nodes.len(), |total, node| total.saturating_add(node.properties.len()));
                let output = ClusterInfoOutput {
                    healthy_nodes,
                    unhealthy_nodes,
                    nodes,
                    cluster: replies.cluster,
                };
                state.output_collection(
                    &output,
                    entries,
                    "Use a smaller max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemoryStatsNode {
    node: Option<String>,
    peak_allocated_bytes: Option<u64>,
    total_allocated_bytes: Option<u64>,
    startup_allocated_bytes: Option<u64>,
    replication_backlog_bytes: Option<u64>,
    overhead_total_bytes: Option<u64>,
    keys_count: Option<u64>,
    bytes_per_key: Option<u64>,
    dataset_bytes: Option<u64>,
    dataset_percentage: Option<f64>,
    peak_percentage: Option<f64>,
    fragmentation_ratio: Option<f64>,
    fields: Vec<DiagnosticField>,
}

fn memory_stats_node(
    node: Option<String>,
    value: RedisValue,
) -> tower_mcp::Result<MemoryStatsNode> {
    let fields = diagnostic_fields(value, "MEMORY STATS")?;
    Ok(MemoryStatsNode {
        node,
        peak_allocated_bytes: field_u64(&fields, "peak.allocated"),
        total_allocated_bytes: field_u64(&fields, "total.allocated"),
        startup_allocated_bytes: field_u64(&fields, "startup.allocated"),
        replication_backlog_bytes: field_u64(&fields, "replication.backlog"),
        overhead_total_bytes: field_u64(&fields, "overhead.total"),
        keys_count: field_u64(&fields, "keys.count"),
        bytes_per_key: field_u64(&fields, "keys.bytes-per-key"),
        dataset_bytes: field_u64(&fields, "dataset.bytes"),
        dataset_percentage: field_f64(&fields, "dataset.percentage"),
        peak_percentage: field_f64(&fields, "peak.percentage"),
        fragmentation_ratio: field_f64(&fields, "fragmentation"),
        fields,
    })
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemoryStatsOutput {
    deployment: String,
    nodes: Vec<MemoryStatsNode>,
    cluster: Option<ClusterAggregation>,
}

async fn fetch_memory_stats(
    state: &ToolState,
    tool_name: &'static str,
    input: &ClusterInput,
) -> tower_mcp::Result<(Vec<MemoryStatsNode>, Option<ClusterAggregation>)> {
    let mut redis_command = clustered_command(tool_name, "MEMORY", input.max_cluster_nodes);
    redis_command.arg("STATS");
    let replies = split_node_replies(
        diagnostic_raw(state, redis_command, "MEMORY STATS failed").await?,
        input.max_cluster_nodes,
        input.include_node_addresses,
    )?;
    let nodes = replies
        .values
        .into_iter()
        .map(|(node, value)| memory_stats_node(node, value))
        .collect::<tower_mcp::Result<Vec<_>>>()?;
    Ok((nodes, replies.cluster))
}

fn memory_stats_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_memory_stats")
        .title("Inspect Redis Memory Statistics")
        .description(
            "Read structured MEMORY STATS values with known fields normalized and every unknown field retained in a binary-safe representation. Cluster fan-out is bounded and partial failures are explicit.",
        )
        .output_schema(output_schema::<MemoryStatsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_memory_stats node addresses")?;
                let (nodes, cluster) = fetch_memory_stats(&state, "redis_memory_stats", &input).await?;
                let entries = nodes
                    .iter()
                    .fold(nodes.len(), |total, node| total.saturating_add(node.fields.len()));
                let deployment = if cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                state.output_collection(
                    &MemoryStatsOutput {
                        deployment: deployment.as_str().to_string(),
                        nodes,
                        cluster,
                    },
                    entries,
                    "Use a smaller max_cluster_nodes value or a larger configured output budget.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModuleListInput {
    /// Maximum cluster nodes that may participate.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
    /// Include module paths, arguments, unknown fields, and real node addresses. Requires Full access.
    #[serde(default)]
    include_sensitive: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModuleRecord {
    node: Option<String>,
    name: Option<EncodedValue>,
    version: Option<i64>,
    path: Option<EncodedValue>,
    arguments: Option<JsonValue>,
    sensitive_fields_redacted: bool,
    unknown_field_count: usize,
    unknown_fields: Option<Vec<DiagnosticField>>,
}

fn module_record(
    node: Option<String>,
    value: RedisValue,
    include_sensitive: bool,
) -> tower_mcp::Result<ModuleRecord> {
    let mut name = None;
    let mut version = None;
    let mut path = None;
    let mut arguments = None;
    let mut unknown = Vec::new();
    for (key, value) in reply_pairs(value, "MODULE LIST entry")? {
        let key_bytes = reply_bytes(key, "MODULE LIST field")?;
        match key_bytes.as_slice() {
            b"name" => name = Some(EncodedValue::new(reply_bytes(value, "MODULE name")?)),
            b"ver" => version = Some(reply_i64(value, "MODULE version")?),
            b"path" if include_sensitive => {
                path = Some(EncodedValue::new(reply_bytes(value, "MODULE path")?));
            }
            b"args" if include_sensitive => arguments = Some(redis_value_to_json(&value)),
            b"path" | b"args" => {}
            _ => unknown.push(DiagnosticField {
                key: EncodedValue::new(key_bytes),
                value: redis_value_to_json(&value),
            }),
        }
    }
    let unknown_field_count = unknown.len();
    Ok(ModuleRecord {
        node,
        name,
        version,
        path,
        arguments,
        sensitive_fields_redacted: !include_sensitive,
        unknown_field_count,
        unknown_fields: include_sensitive.then_some(unknown),
    })
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ModuleListOutput {
    deployment: String,
    count: usize,
    modules: Vec<ModuleRecord>,
    cluster: Option<ClusterAggregation>,
}

fn module_list_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_module_list")
        .title("List Redis Modules")
        .description(
            "List loaded Redis modules with structured names and versions. Paths, arguments, unknown fields, and real cluster node addresses are redacted unless explicitly requested under Full access.",
        )
        .output_schema(output_schema::<ModuleListOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ModuleListInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                require_sensitive(&state, input.include_sensitive, "redis_module_list sensitive fields")?;
                let mut redis_command = clustered_command(
                    "redis_module_list",
                    "MODULE",
                    input.max_cluster_nodes,
                );
                redis_command.arg("LIST");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "MODULE LIST failed").await?,
                    input.max_cluster_nodes,
                    input.include_sensitive,
                )?;
                let mut modules = Vec::new();
                for (node, value) in replies.values {
                    let entries = match value {
                        RedisValue::Array(entries) | RedisValue::Set(entries) => entries,
                        _ => {
                            return Err(tower_mcp::Error::tool(
                                "MODULE LIST returned an unexpected collection reply",
                            ));
                        }
                    };
                    for entry in entries {
                        modules.push(module_record(node.clone(), entry, input.include_sensitive)?);
                    }
                }
                let count = modules.len();
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                state.output_collection(
                    &ModuleListOutput {
                        deployment: deployment.as_str().to_string(),
                        count,
                        modules,
                        cluster: replies.cluster,
                    },
                    count,
                    "Use a smaller max_cluster_nodes value or a larger configured output budget.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SlowlogInput {
    /// Maximum entries returned per Redis node.
    #[serde(default = "default_slowlog_limit")]
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
    /// Maximum cluster nodes that may participate.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
    /// Include command arguments. Requires Full access because commands may contain credentials or user data.
    #[serde(default)]
    include_arguments: bool,
    /// Include client addresses, names, future extra fields, and real node addresses. Requires Full access.
    #[serde(default)]
    include_sensitive: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SlowlogEntry {
    node: Option<String>,
    id: u64,
    timestamp_seconds: i64,
    duration_microseconds: u64,
    command: Option<EncodedValue>,
    argument_count: usize,
    arguments: Option<Vec<EncodedValue>>,
    client_address: Option<EncodedValue>,
    client_name: Option<EncodedValue>,
    extra_field_count: usize,
    extra_fields: Option<Vec<JsonValue>>,
    sensitive_fields_redacted: bool,
}

fn slowlog_entry(
    node: Option<String>,
    value: RedisValue,
    input: &SlowlogInput,
) -> tower_mcp::Result<SlowlogEntry> {
    let RedisValue::Array(mut values) = value else {
        return Err(tower_mcp::Error::tool(
            "SLOWLOG GET returned an unexpected entry reply",
        ));
    };
    if values.len() < 4 {
        return Err(tower_mcp::Error::tool(
            "SLOWLOG GET returned an incomplete entry",
        ));
    }
    let extra = values.split_off(4);
    let args = match values.pop().expect("four slowlog fields") {
        RedisValue::Array(args) => args,
        _ => {
            return Err(tower_mcp::Error::tool(
                "SLOWLOG GET returned invalid command arguments",
            ));
        }
    };
    let duration_microseconds = reply_u64(values.pop().expect("duration"), "SLOWLOG duration")?;
    let timestamp_seconds = reply_i64(values.pop().expect("timestamp"), "SLOWLOG timestamp")?;
    let id = reply_u64(values.pop().expect("id"), "SLOWLOG id")?;
    let command = args
        .first()
        .cloned()
        .map(|value| reply_bytes(value, "SLOWLOG command").map(EncodedValue::new))
        .transpose()?;
    let argument_count = args.len().saturating_sub(1);
    let arguments = input
        .include_arguments
        .then(|| {
            args.into_iter()
                .skip(1)
                .map(|value| reply_bytes(value, "SLOWLOG argument").map(EncodedValue::new))
                .collect::<tower_mcp::Result<Vec<_>>>()
        })
        .transpose()?;
    let mut extra = extra;
    let client_address = if input.include_sensitive && !extra.is_empty() {
        Some(EncodedValue::new(reply_bytes(
            extra.remove(0),
            "SLOWLOG client address",
        )?))
    } else {
        None
    };
    let client_name = if input.include_sensitive && !extra.is_empty() {
        Some(EncodedValue::new(reply_bytes(
            extra.remove(0),
            "SLOWLOG client name",
        )?))
    } else {
        None
    };
    let extra_field_count = if input.include_sensitive {
        extra.len()
    } else {
        // Redis 4+ appends address and name; count every future field without exposing it.
        extra.len()
    };
    let extra_fields = input
        .include_sensitive
        .then(|| extra.iter().map(redis_value_to_json).collect());
    Ok(SlowlogEntry {
        node,
        id,
        timestamp_seconds,
        duration_microseconds,
        command,
        argument_count,
        arguments,
        client_address,
        client_name,
        extra_field_count,
        extra_fields,
        sensitive_fields_redacted: !(input.include_sensitive && input.include_arguments),
    })
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SlowlogOutput {
    deployment: String,
    count: usize,
    entries: Vec<SlowlogEntry>,
    cluster: Option<ClusterAggregation>,
}

fn slowlog_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_slowlog")
        .title("Read Redis Slow Log")
        .description(
            "Read bounded structured SLOWLOG entries. Command arguments, client identity, future fields, and real cluster node addresses are redacted unless explicitly requested under Full access.",
        )
        .output_schema(output_schema::<SlowlogOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SlowlogInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                validate_per_node_limit(&state, input.limit, input.max_cluster_nodes, "limit")?;
                require_sensitive(
                    &state,
                    input.include_arguments || input.include_sensitive,
                    "redis_slowlog sensitive fields",
                )?;
                let mut redis_command = clustered_command(
                    "redis_slowlog",
                    "SLOWLOG",
                    input.max_cluster_nodes,
                );
                redis_command.arg("GET").arg(input.limit.to_string());
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "SLOWLOG GET failed").await?,
                    input.max_cluster_nodes,
                    input.include_sensitive,
                )?;
                let mut entries = Vec::new();
                for (node, value) in replies.values {
                    let RedisValue::Array(values) = value else {
                        return Err(tower_mcp::Error::tool(
                            "SLOWLOG GET returned an unexpected collection reply",
                        ));
                    };
                    for value in values {
                        entries.push(slowlog_entry(node.clone(), value, &input)?);
                    }
                }
                entries.sort_unstable_by(|left, right| right.id.cmp(&left.id));
                let count = entries.len();
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                state.output_collection(
                    &SlowlogOutput {
                        deployment: deployment.as_str().to_string(),
                        count,
                        entries,
                        cluster: replies.cluster,
                    },
                    count,
                    "Retry SLOWLOG with a smaller per-node limit or max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LatencyHistoryInput {
    /// Redis latency event, such as command or fast-command.
    event: String,
    /// Maximum samples returned per node.
    #[serde(default = "default_latency_limit")]
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
    /// Maximum cluster nodes that may participate.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
    /// Return real cluster node addresses. Requires Full access.
    #[serde(default)]
    include_node_addresses: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LatencySample {
    node: Option<String>,
    timestamp_seconds: i64,
    latency_milliseconds: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LatencyHistoryOutput {
    event: String,
    returned: usize,
    available: usize,
    truncated: bool,
    samples: Vec<LatencySample>,
    cluster: Option<ClusterAggregation>,
}

fn latency_history_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_latency_history")
        .title("Read Redis Latency History")
        .description(
            "Read bounded structured LATENCY HISTORY samples for one event. Cluster results expose completeness and pseudonymize node addresses by default.",
        )
        .output_schema(output_schema::<LatencyHistoryOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LatencyHistoryInput>| async move {
                validate_bytes(&input.event, MAX_EVENT_BYTES, "event")?;
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                validate_per_node_limit(&state, input.limit, input.max_cluster_nodes, "limit")?;
                require_sensitive(
                    &state,
                    input.include_node_addresses,
                    "redis_latency_history node addresses",
                )?;
                let mut redis_command = clustered_command(
                    "redis_latency_history",
                    "LATENCY",
                    input.max_cluster_nodes,
                );
                redis_command.arg("HISTORY").arg(input.event.as_bytes());
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "LATENCY HISTORY failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let mut samples = Vec::new();
                let mut available = 0_usize;
                for (node, value) in replies.values {
                    let RedisValue::Array(values) = value else {
                        return Err(tower_mcp::Error::tool(
                            "LATENCY HISTORY returned an unexpected collection reply",
                        ));
                    };
                    available = available.saturating_add(values.len());
                    for value in values.into_iter().take(input.limit) {
                        let RedisValue::Array(mut pair) = value else {
                            return Err(tower_mcp::Error::tool(
                                "LATENCY HISTORY returned an invalid sample",
                            ));
                        };
                        if pair.len() != 2 {
                            return Err(tower_mcp::Error::tool(
                                "LATENCY HISTORY returned an invalid sample length",
                            ));
                        }
                        let latency = reply_u64(pair.pop().expect("latency"), "LATENCY value")?;
                        let timestamp = reply_i64(pair.pop().expect("timestamp"), "LATENCY timestamp")?;
                        samples.push(LatencySample {
                            node: node.clone(),
                            timestamp_seconds: timestamp,
                            latency_milliseconds: latency,
                        });
                    }
                }
                samples.sort_unstable_by_key(|sample| sample.timestamp_seconds);
                let returned = samples.len();
                state.output_collection(
                    &LatencyHistoryOutput {
                        event: input.event,
                        returned,
                        available,
                        truncated: available > returned,
                        samples,
                        cluster: replies.cluster,
                    },
                    returned,
                    "Retry LATENCY HISTORY with a smaller limit or max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NodeIdentity {
    node: Option<String>,
    username: EncodedValue,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclWhoamiOutput {
    deployment: String,
    identities: Vec<NodeIdentity>,
    consistent: bool,
    cluster: Option<ClusterAggregation>,
}

fn acl_whoami_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_whoami")
        .title("Identify Redis ACL User")
        .description(
            "Return the current authenticated Redis username. Cluster execution is caller-bounded, reports partial failures, and pseudonymizes node addresses by default.",
        )
        .output_schema(output_schema::<AclWhoamiOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_acl_whoami node addresses")?;
                let mut redis_command = clustered_command(
                    "redis_acl_whoami",
                    "ACL",
                    input.max_cluster_nodes,
                );
                redis_command.arg("WHOAMI");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "ACL WHOAMI failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let identities = replies
                    .values
                    .into_iter()
                    .map(|(node, value)| {
                        Ok(NodeIdentity {
                            node,
                            username: EncodedValue::new(reply_bytes(value, "ACL WHOAMI")?),
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let usernames = identities
                    .iter()
                    .map(|identity| {
                        let encoding = match identity.username.encoding {
                            ValueEncoding::Utf8 => "utf8",
                            ValueEncoding::Base64 => "base64",
                        };
                        format!("{encoding}:{}", identity.username.value)
                    })
                    .collect::<BTreeSet<_>>();
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let count = identities.len();
                state.output_collection(
                    &AclWhoamiOutput {
                        deployment: deployment.as_str().to_string(),
                        identities,
                        consistent: usernames.len() <= 1,
                        cluster: replies.cluster,
                    },
                    count,
                    "Use a smaller max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HealthNode {
    node: Option<String>,
    status: String,
    issues: Vec<String>,
    redis_version: Option<String>,
    mode: Option<String>,
    role: Option<String>,
    uptime_seconds: Option<u64>,
    loading: Option<bool>,
    used_memory_bytes: Option<u64>,
    maxmemory_bytes: Option<u64>,
    memory_fragmentation_ratio: Option<f64>,
    connected_clients: Option<u64>,
    blocked_clients: Option<u64>,
    rejected_connections: Option<u64>,
    instantaneous_ops_per_second: Option<u64>,
    total_commands_processed: Option<u64>,
    total_keys: u64,
    expiring_keys: u64,
}

fn keyspace_totals(properties: &BTreeMap<String, String>) -> (u64, u64) {
    properties
        .iter()
        .filter(|(key, _)| key.starts_with("db") && key[2..].chars().all(|ch| ch.is_ascii_digit()))
        .fold((0_u64, 0_u64), |(keys, expires), (_, value)| {
            let fields = value
                .split(',')
                .filter_map(|field| field.split_once('='))
                .collect::<BTreeMap<_, _>>();
            (
                keys.saturating_add(
                    fields
                        .get("keys")
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                ),
                expires.saturating_add(
                    fields
                        .get("expires")
                        .and_then(|value| value.parse().ok())
                        .unwrap_or(0),
                ),
            )
        })
}

fn health_node(node: Option<String>, value: RedisValue) -> tower_mcp::Result<HealthNode> {
    let properties = parse_text_properties(&reply_string(value, "INFO ALL")?);
    let mut issues = Vec::new();
    if optional_bool(&properties, "loading") == Some(true) {
        issues.push("dataset_loading".to_string());
    }
    if properties
        .get("master_link_status")
        .is_some_and(|status| status == "down")
    {
        issues.push("replication_link_down".to_string());
    }
    if properties
        .get("cluster_state")
        .is_some_and(|state| state != "ok")
    {
        issues.push("cluster_state_not_ok".to_string());
    }
    if optional_u64(&properties, "rdb_last_bgsave_status") == Some(0)
        || properties
            .get("rdb_last_bgsave_status")
            .is_some_and(|status| status == "err")
    {
        issues.push("last_rdb_save_failed".to_string());
    }
    if properties
        .get("aof_last_bgrewrite_status")
        .is_some_and(|status| status == "err")
    {
        issues.push("last_aof_rewrite_failed".to_string());
    }
    let (total_keys, expiring_keys) = keyspace_totals(&properties);
    Ok(HealthNode {
        node,
        status: if issues.is_empty() { "ok" } else { "degraded" }.to_string(),
        issues,
        redis_version: properties.get("redis_version").cloned(),
        mode: properties.get("redis_mode").cloned(),
        role: properties.get("role").cloned(),
        uptime_seconds: optional_u64(&properties, "uptime_in_seconds"),
        loading: optional_bool(&properties, "loading"),
        used_memory_bytes: optional_u64(&properties, "used_memory"),
        maxmemory_bytes: optional_u64(&properties, "maxmemory"),
        memory_fragmentation_ratio: optional_f64(&properties, "mem_fragmentation_ratio"),
        connected_clients: optional_u64(&properties, "connected_clients"),
        blocked_clients: optional_u64(&properties, "blocked_clients"),
        rejected_connections: optional_u64(&properties, "rejected_connections"),
        instantaneous_ops_per_second: optional_u64(&properties, "instantaneous_ops_per_sec"),
        total_commands_processed: optional_u64(&properties, "total_commands_processed"),
        total_keys,
        expiring_keys,
    })
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HealthCheckOutput {
    deployment: String,
    status: String,
    elapsed_ms: f64,
    healthy_nodes: usize,
    degraded_nodes: usize,
    nodes: Vec<HealthNode>,
    cluster: Option<ClusterAggregation>,
}

fn health_check_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_health_check")
        .title("Check Redis Health")
        .description(
            "Derive a bounded structured health summary from INFO across the target, including persistence, replication, memory, workload, client, and keyspace signals. Partial cluster failures are explicit.",
        )
        .output_schema(output_schema::<HealthCheckOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_health_check node addresses")?;
                let mut redis_command = clustered_command(
                    "redis_health_check",
                    "INFO",
                    input.max_cluster_nodes,
                );
                redis_command.arg("ALL");
                let started = Instant::now();
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "INFO ALL failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let nodes = replies
                    .values
                    .into_iter()
                    .map(|(node, value)| health_node(node, value))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let healthy_nodes = nodes.iter().filter(|node| node.issues.is_empty()).count();
                let degraded_nodes = nodes.len().saturating_sub(healthy_nodes);
                let incomplete = replies.cluster.as_ref().is_some_and(|cluster| !cluster.complete);
                let status = if nodes.is_empty() {
                    "unavailable"
                } else if degraded_nodes > 0 || incomplete {
                    "degraded"
                } else {
                    "ok"
                };
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let entries = nodes
                    .iter()
                    .fold(nodes.len(), |total, node| total.saturating_add(node.issues.len()));
                state.output_collection(
                    &HealthCheckOutput {
                        deployment: deployment.as_str().to_string(),
                        status: status.to_string(),
                        elapsed_ms: started.elapsed().as_secs_f64() * 1_000.0,
                        healthy_nodes,
                        degraded_nodes,
                        nodes,
                        cluster: replies.cluster,
                    },
                    entries,
                    "Use a smaller max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConnectionNodeSummary {
    node: Option<String>,
    total: usize,
    idle_over_60_seconds: usize,
    blocked: usize,
    oldest_age_seconds: Option<u64>,
    client_types: BTreeMap<String, usize>,
    commands: BTreeMap<String, usize>,
    databases: BTreeMap<i64, usize>,
}

fn connection_node_summary(
    node: Option<String>,
    clients: Vec<ParsedClient>,
) -> ConnectionNodeSummary {
    let mut client_types = BTreeMap::new();
    let mut commands = BTreeMap::new();
    let mut databases = BTreeMap::new();
    let mut idle = 0;
    let mut blocked = 0;
    let mut oldest_age_seconds = None;
    for client in &clients {
        *client_types
            .entry(client.client_type().to_string())
            .or_insert(0) += 1;
        if let Some(command) = client.string("cmd") {
            *commands.entry(command).or_insert(0) += 1;
        }
        if let Some(database) = client.i64("db") {
            *databases.entry(database).or_insert(0) += 1;
        }
        idle += usize::from(client.u64("idle").is_some_and(|value| value > 60));
        blocked += usize::from(
            client
                .bytes("flags")
                .is_some_and(|flags| flags.contains(&b'b')),
        );
        if let Some(age) = client.u64("age") {
            oldest_age_seconds = Some(oldest_age_seconds.unwrap_or(0).max(age));
        }
    }
    ConnectionNodeSummary {
        node,
        total: clients.len(),
        idle_over_60_seconds: idle,
        blocked,
        oldest_age_seconds,
        client_types,
        commands,
        databases,
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConnectionSummaryOutput {
    deployment: String,
    total: usize,
    idle_over_60_seconds: usize,
    blocked: usize,
    oldest_age_seconds: Option<u64>,
    client_types: BTreeMap<String, usize>,
    commands: BTreeMap<String, usize>,
    databases: BTreeMap<i64, usize>,
    nodes: Vec<ConnectionNodeSummary>,
    client_identity_redacted: bool,
    cluster: Option<ClusterAggregation>,
}

fn add_counts<K: Ord + Clone>(target: &mut BTreeMap<K, usize>, source: &BTreeMap<K, usize>) {
    for (key, count) in source {
        *target.entry(key.clone()).or_insert(0) += count;
    }
}

fn connection_summary_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_connection_summary")
        .title("Summarize Redis Connections")
        .description(
            "Summarize client totals, types, commands, databases, idle connections, blocked connections, and oldest age without exposing client addresses, names, or usernames. Cluster node labels are pseudonymous by default.",
        )
        .output_schema(output_schema::<ConnectionSummaryOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_connection_summary node addresses")?;
                let mut redis_command = clustered_command(
                    "redis_connection_summary",
                    "CLIENT",
                    input.max_cluster_nodes,
                );
                redis_command.arg("LIST");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "CLIENT LIST failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let nodes = replies
                    .values
                    .into_iter()
                    .map(|(node, value)| parse_clients(value).map(|clients| connection_node_summary(node, clients)))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let mut total = 0_usize;
                let mut idle = 0_usize;
                let mut blocked = 0_usize;
                let mut oldest_age_seconds = None;
                let mut client_types = BTreeMap::new();
                let mut commands = BTreeMap::new();
                let mut databases = BTreeMap::new();
                for node in &nodes {
                    total = total.saturating_add(node.total);
                    idle = idle.saturating_add(node.idle_over_60_seconds);
                    blocked = blocked.saturating_add(node.blocked);
                    if let Some(age) = node.oldest_age_seconds {
                        oldest_age_seconds = Some(oldest_age_seconds.unwrap_or(0).max(age));
                    }
                    add_counts(&mut client_types, &node.client_types);
                    add_counts(&mut commands, &node.commands);
                    add_counts(&mut databases, &node.databases);
                }
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let entries = nodes.len()
                    .saturating_add(client_types.len())
                    .saturating_add(commands.len())
                    .saturating_add(databases.len());
                state.output_collection(
                    &ConnectionSummaryOutput {
                        deployment: deployment.as_str().to_string(),
                        total,
                        idle_over_60_seconds: idle,
                        blocked,
                        oldest_age_seconds,
                        client_types,
                        commands,
                        databases,
                        nodes,
                        client_identity_redacted: true,
                        cluster: replies.cluster,
                    },
                    entries,
                    "Use redis_client_list with filters for individual records or reduce max_cluster_nodes.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DatabaseSummary {
    database: String,
    keys: u64,
    expiring_keys: u64,
    average_ttl_ms: Option<u64>,
    subexpiry: Option<u64>,
    fields: BTreeMap<String, String>,
}

fn database_summary(database: String, value: &str) -> DatabaseSummary {
    let fields = value
        .split(',')
        .filter_map(|field| field.split_once('='))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<BTreeMap<_, _>>();
    DatabaseSummary {
        database,
        keys: fields
            .get("keys")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        expiring_keys: fields
            .get("expires")
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
        average_ttl_ms: fields.get("avg_ttl").and_then(|value| value.parse().ok()),
        subexpiry: fields.get("subexpiry").and_then(|value| value.parse().ok()),
        fields,
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyspaceNodeSummary {
    node: Option<String>,
    total_keys: u64,
    expiring_keys: u64,
    databases: Vec<DatabaseSummary>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyspaceSummaryOutput {
    deployment: String,
    total_keys: u64,
    expiring_keys: u64,
    nodes: Vec<KeyspaceNodeSummary>,
    cluster: Option<ClusterAggregation>,
}

fn keyspace_summary_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_keyspace_summary")
        .title("Summarize Redis Keyspace")
        .description(
            "Summarize per-database key counts, expirations, and average TTL from bounded INFO keyspace responses. Unknown database fields remain structured and cluster partial failures are explicit.",
        )
        .output_schema(output_schema::<KeyspaceSummaryOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_keyspace_summary node addresses")?;
                let mut redis_command = clustered_command(
                    "redis_keyspace_summary",
                    "INFO",
                    input.max_cluster_nodes,
                );
                redis_command.arg("keyspace");
                let replies = split_node_replies(
                    diagnostic_raw(&state, redis_command, "INFO keyspace failed").await?,
                    input.max_cluster_nodes,
                    input.include_node_addresses,
                )?;
                let mut nodes = Vec::new();
                let mut total_keys = 0_u64;
                let mut expiring_keys = 0_u64;
                for (node, value) in replies.values {
                    let properties = parse_text_properties(&reply_string(value, "INFO keyspace")?);
                    let databases = properties
                        .into_iter()
                        .filter(|(key, _)| {
                            key.starts_with("db")
                                && key[2..].chars().all(|character| character.is_ascii_digit())
                        })
                        .map(|(database, value)| database_summary(database, &value))
                        .collect::<Vec<_>>();
                    let node_keys = databases.iter().fold(0_u64, |total, database| {
                        total.saturating_add(database.keys)
                    });
                    let node_expiring = databases.iter().fold(0_u64, |total, database| {
                        total.saturating_add(database.expiring_keys)
                    });
                    total_keys = total_keys.saturating_add(node_keys);
                    expiring_keys = expiring_keys.saturating_add(node_expiring);
                    nodes.push(KeyspaceNodeSummary {
                        node,
                        total_keys: node_keys,
                        expiring_keys: node_expiring,
                        databases,
                    });
                }
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let entries = nodes.iter().fold(nodes.len(), |total, node| {
                    total.saturating_add(node.databases.len()).saturating_add(
                        node.databases
                            .iter()
                            .map(|database| database.fields.len())
                            .sum::<usize>(),
                    )
                });
                state.output_collection(
                    &KeyspaceSummaryOutput {
                        deployment: deployment.as_str().to_string(),
                        total_keys,
                        expiring_keys,
                        nodes,
                        cluster: replies.cluster,
                    },
                    entries,
                    "Use a smaller max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemoryNodeSummary {
    node: Option<String>,
    total_allocated_bytes: Option<u64>,
    peak_allocated_bytes: Option<u64>,
    overhead_bytes: Option<u64>,
    dataset_bytes: Option<u64>,
    dataset_percentage: Option<f64>,
    fragmentation_ratio: Option<f64>,
    bytes_per_key: Option<u64>,
    key_count: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemorySummaryOutput {
    deployment: String,
    total_allocated_bytes: u64,
    total_dataset_bytes: u64,
    total_overhead_bytes: u64,
    nodes: Vec<MemoryNodeSummary>,
    cluster: Option<ClusterAggregation>,
}

fn memory_summary_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_memory_summary")
        .title("Summarize Redis Memory")
        .description(
            "Derive a compact per-node and aggregate memory summary from MEMORY STATS while preserving bounded cluster completeness metadata.",
        )
        .output_schema(output_schema::<MemorySummaryOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
                validate_cluster_input(&state, &input, "redis_memory_summary node addresses")?;
                let (stats, cluster) = fetch_memory_stats(&state, "redis_memory_summary", &input).await?;
                let mut total_allocated_bytes = 0_u64;
                let mut total_dataset_bytes = 0_u64;
                let mut total_overhead_bytes = 0_u64;
                let nodes = stats
                    .into_iter()
                    .map(|stats| {
                        total_allocated_bytes = total_allocated_bytes
                            .saturating_add(stats.total_allocated_bytes.unwrap_or(0));
                        total_dataset_bytes = total_dataset_bytes
                            .saturating_add(stats.dataset_bytes.unwrap_or(0));
                        total_overhead_bytes = total_overhead_bytes
                            .saturating_add(stats.overhead_total_bytes.unwrap_or(0));
                        MemoryNodeSummary {
                            node: stats.node,
                            total_allocated_bytes: stats.total_allocated_bytes,
                            peak_allocated_bytes: stats.peak_allocated_bytes,
                            overhead_bytes: stats.overhead_total_bytes,
                            dataset_bytes: stats.dataset_bytes,
                            dataset_percentage: stats.dataset_percentage,
                            fragmentation_ratio: stats.fragmentation_ratio,
                            bytes_per_key: stats.bytes_per_key,
                            key_count: stats.keys_count,
                        }
                    })
                    .collect::<Vec<_>>();
                let deployment = if cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                state.output_collection(
                    &MemorySummaryOutput {
                        deployment: deployment.as_str().to_string(),
                        total_allocated_bytes,
                        total_dataset_bytes,
                        total_overhead_bytes,
                        nodes,
                        cluster,
                    },
                    1,
                    "Use a smaller max_cluster_nodes value.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeySummaryInput {
    /// Redis key, as UTF-8 or standard base64.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeySummaryOutput {
    key: EncodedValue,
    exists: bool,
    key_type: Option<String>,
    ttl_seconds: Option<i64>,
    persistent: bool,
    memory_bytes: Option<u64>,
    object_encoding: Option<EncodedValue>,
}

async fn run_key_summary(state: &ToolState, key: Vec<u8>) -> tower_mcp::Result<KeySummaryOutput> {
    let mut type_command = command("redis_key_summary", AccessMode::ReadOnly, "TYPE");
    type_command.arg(key.clone());
    let key_type: String = diagnostic_query(state, type_command, "TYPE failed").await?;
    if key_type == "none" {
        return Ok(KeySummaryOutput {
            key: EncodedValue::new(key),
            exists: false,
            key_type: None,
            ttl_seconds: None,
            persistent: false,
            memory_bytes: None,
            object_encoding: None,
        });
    }

    let mut ttl_command = command("redis_key_summary", AccessMode::ReadOnly, "TTL");
    ttl_command.arg(key.clone());
    let ttl: i64 = diagnostic_query(state, ttl_command, "TTL failed").await?;
    let mut memory_command = command("redis_key_summary", AccessMode::ReadOnly, "MEMORY");
    memory_command.arg("USAGE").arg(key.clone());
    let memory_bytes: Option<u64> =
        diagnostic_query(state, memory_command, "MEMORY USAGE failed").await?;
    let mut encoding_command = command("redis_key_summary", AccessMode::ReadOnly, "OBJECT");
    encoding_command.arg("ENCODING").arg(key.clone());
    let object_encoding = diagnostic_raw(state, encoding_command, "OBJECT ENCODING failed")
        .await
        .and_then(|value| match value {
            RedisValue::Nil => Ok(None),
            value => {
                reply_bytes(value, "OBJECT ENCODING").map(|value| Some(EncodedValue::new(value)))
            }
        })?;
    Ok(KeySummaryOutput {
        key: EncodedValue::new(key),
        exists: true,
        key_type: Some(key_type),
        ttl_seconds: (ttl >= 0).then_some(ttl),
        persistent: ttl == -1,
        memory_bytes,
        object_encoding,
    })
}

fn key_summary_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_key_summary")
        .title("Summarize Redis Key")
        .description(
            "Read a binary-safe structured key summary combining TYPE, TTL, MEMORY USAGE, and OBJECT ENCODING under one total workflow timeout.",
        )
        .output_schema(output_schema::<KeySummaryOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeySummaryInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let timeout = state.command_timeout();
                let output = tokio::time::timeout(timeout, run_key_summary(&state, key))
                    .await
                    .map_err(|_| {
                        tower_mcp::Error::tool(format!(
                            "redis_key_summary timed out after {} ms",
                            timeout.as_millis()
                        ))
                    })??;
                state.output(&output)
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HotkeysInput {
    /// Cursor returned by the previous call. Start with zero.
    #[serde(default)]
    cursor: u64,
    /// Glob-style key pattern, as UTF-8 or standard base64.
    #[serde(default = "default_pattern")]
    pattern: String,
    /// Encoding of `pattern`.
    #[serde(default)]
    pattern_encoding: InputEncoding,
    /// Approximate number of keys Redis should inspect in this one SCAN page.
    #[serde(default = "default_scan_count")]
    #[schemars(range(min = 1, max = 256))]
    count: usize,
    /// Hard maximum keys accepted from this SCAN page before any per-key inspection runs.
    #[serde(default = "default_hotkey_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_keys: usize,
    /// Number of largest sampled keys returned.
    #[serde(default = "default_hotkey_top")]
    #[schemars(range(min = 1, max = 256))]
    top: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HotkeyCandidate {
    rank: usize,
    key: EncodedValue,
    key_type: String,
    memory_bytes: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HotkeysOutput {
    sampled_keys: usize,
    measured_keys: usize,
    total_sampled_memory_bytes: u64,
    type_distribution: BTreeMap<String, usize>,
    candidates: Vec<HotkeyCandidate>,
    page: PageMetadata,
    selection_basis: String,
}

async fn run_hotkey_page(
    state: &ToolState,
    input: &HotkeysInput,
    pattern: Vec<u8>,
) -> tower_mcp::Result<HotkeysOutput> {
    let mut scan = command("redis_hotkeys", AccessMode::ReadOnly, "SCAN");
    scan.arg(input.cursor.to_string())
        .arg("MATCH")
        .arg(pattern)
        .arg("COUNT")
        .arg(input.count.to_string());
    let (cursor, keys): (u64, Vec<Vec<u8>>) = diagnostic_query(state, scan, "SCAN failed").await?;
    if keys.len() > input.max_keys {
        return Err(tower_mcp::Error::tool(format!(
            "SCAN returned {} keys, exceeding max_keys={}; retry the same cursor with a smaller count",
            keys.len(),
            input.max_keys
        )));
    }

    let sampled_keys = keys.len();
    let mut measured = Vec::new();
    let mut type_distribution = BTreeMap::new();
    let mut total_sampled_memory_bytes = 0_u64;
    for key in keys {
        let mut type_command = command("redis_hotkeys", AccessMode::ReadOnly, "TYPE");
        type_command.arg(key.clone());
        let key_type: String = diagnostic_query(state, type_command, "TYPE failed").await?;
        if key_type == "none" {
            continue;
        }
        let mut memory_command = command("redis_hotkeys", AccessMode::ReadOnly, "MEMORY");
        memory_command.arg("USAGE").arg(key.clone());
        let memory_bytes: Option<u64> =
            diagnostic_query(state, memory_command, "MEMORY USAGE failed").await?;
        *type_distribution.entry(key_type.clone()).or_insert(0) += 1;
        if let Some(memory_bytes) = memory_bytes {
            total_sampled_memory_bytes = total_sampled_memory_bytes.saturating_add(memory_bytes);
            measured.push((key, key_type, memory_bytes));
        }
    }
    measured
        .sort_unstable_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
    let measured_keys = measured.len();
    let candidates = measured
        .into_iter()
        .take(input.top)
        .enumerate()
        .map(|(index, (key, key_type, memory_bytes))| HotkeyCandidate {
            rank: index + 1,
            key: EncodedValue::new(key),
            key_type,
            memory_bytes,
        })
        .collect::<Vec<_>>();
    Ok(HotkeysOutput {
        sampled_keys,
        measured_keys,
        total_sampled_memory_bytes,
        type_distribution,
        candidates,
        page: PageMetadata::cursor(input.count, sampled_keys, cursor),
        selection_basis: "largest_memory_usage_in_one_explicit_scan_page".to_string(),
    })
}

fn hotkeys_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hotkeys")
        .title("Sample Large Redis Keys")
        .description(
            "Inspect exactly one explicit SCAN cursor page and rank that bounded sample by MEMORY USAGE. This never hides a full keyspace walk; continue with page.continuation.cursor. Standalone only.",
        )
        .output_schema(output_schema::<HotkeysOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HotkeysInput>| async move {
                if input.count == 0 || input.count > MAX_HOTKEYS_PER_PAGE {
                    return Err(tower_mcp::Error::tool(format!(
                        "count must be between 1 and {MAX_HOTKEYS_PER_PAGE}"
                    )));
                }
                if input.max_keys == 0 || input.max_keys > MAX_HOTKEYS_PER_PAGE {
                    return Err(tower_mcp::Error::tool(format!(
                        "max_keys must be between 1 and {MAX_HOTKEYS_PER_PAGE}"
                    )));
                }
                validate_limit(&state, input.max_keys, "max_keys")?;
                if input.top == 0 || input.top > input.max_keys {
                    return Err(tower_mcp::Error::tool(
                        "top must be between 1 and max_keys",
                    ));
                }
                let pattern = decode_input(&input.pattern, input.pattern_encoding, "pattern")?;
                validate_bytes(
                    std::str::from_utf8(&pattern).unwrap_or(""),
                    MAX_FILTER_BYTES,
                    "pattern",
                )?;
                if pattern.len() > MAX_FILTER_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "pattern is {} bytes; maximum is {MAX_FILTER_BYTES}",
                        pattern.len()
                    )));
                }
                let timeout = state.command_timeout();
                let output = tokio::time::timeout(timeout, run_hotkey_page(&state, &input, pattern))
                    .await
                    .map_err(|_| {
                        tower_mcp::Error::tool(format!(
                            "redis_hotkeys timed out after {} ms",
                            timeout.as_millis()
                        ))
                    })??;
                let entries = output
                    .candidates
                    .len()
                    .saturating_add(output.type_distribution.len());
                state.output_collection(
                    &output,
                    entries,
                    "Retry redis_hotkeys with a smaller count, max_keys, or top value.",
                )
            },
        )
        .build()
}
