//! Explicitly enabled, bounded Redis administration tools.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use tower_mcp::{
    CallToolResult, McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, command, decode_input, destructive_annotations,
    empty_input_schema, encode_bytes, output_schema, read_annotations,
};
use crate::{
    AccessMode, RedisValue,
    invocation::{redis_value_collection_entries, redis_value_to_json},
};

const DEFAULT_CLUSTER_NODE_LIMIT: usize = 32;
const MAX_CLUSTER_NODE_LIMIT: usize = 256;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_COMMAND_ARGUMENTS: usize = 32;
const MAX_ARGUMENT_BYTES: usize = 8 * 1024;

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(acl_categories_tool(state.clone()));
    router = router.tool(acl_users_tool(state.clone()));
    router = router.tool(acl_user_tool(state.clone()));
    router = router.tool(acl_rules_tool(state.clone()));
    router = router.tool(acl_dryrun_tool(state.clone()));
    router = router.tool(acl_log_tool(state.clone()));
    router = router.tool(backup_status_tool(state.clone()));
    router = router.tool(backup_files_tool(state.clone()));
    router = router.tool(cluster_inspect_tool(state.clone()));
    router = router.tool(cluster_slot_tool(state.clone()));
    router = router.tool(cluster_slot_stats_tool(state.clone()));
    router = router.tool(config_get_tool(state.clone()));
    router = router.tool(server_state_tool(state.clone()));
    router = router.tool(latency_overview_tool(state.clone()));
    router = router.tool(memory_diagnostics_tool(state.clone()));
    router = router.tool(slowlog_len_tool(state.clone()));
    router.tool(hotkeys_get_tool(state))
}

pub(super) fn add_full_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(acl_log_reset_tool(state.clone()));
    router = router.tool(client_control_tool(state.clone()));
    router = router.tool(config_set_tool(state.clone()));
    router = router.tool(config_resetstat_tool(state.clone()));
    router = router.tool(flush_tool(state.clone()));
    router = router.tool(hotkeys_control_tool(state.clone()));
    router = router.tool(latency_reset_tool(state.clone()));
    router = router.tool(memory_purge_tool(state.clone()));
    router = router.tool(slowlog_reset_tool(state.clone()));
    router.tool(swapdb_tool(state))
}

fn default_cluster_node_limit() -> usize {
    DEFAULT_CLUSTER_NODE_LIMIT
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

fn validate_cluster_input(state: &ToolState, limit: usize) -> tower_mcp::Result<()> {
    validate_cluster_node_limit(limit)?;
    state.validate_requested_entries(limit, "max_cluster_nodes")
}

fn validate_identifier(value: &str, name: &str) -> tower_mcp::Result<()> {
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    if value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(tower_mcp::Error::tool(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

fn validate_command_name(value: &str) -> tower_mcp::Result<()> {
    validate_identifier(value, "command")?;
    if value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        Ok(())
    } else {
        Err(tower_mcp::Error::tool(
            "command may contain only letters, digits, '.', '-', or '_'",
        ))
    }
}

fn redacted_admin_error(error: tower_mcp::Error, context: &str) -> tower_mcp::Error {
    let rendered = error.to_string();
    let category = [
        "Authentication",
        "Authorization",
        "Timeout",
        "Connection",
        "InvalidRequest",
        "InvalidResponse",
        "CapabilityUnavailable",
        "OutputLimit",
        "Server",
    ]
    .into_iter()
    .find(|category| rendered.contains(category))
    .unwrap_or("Other");
    tower_mcp::Error::tool(format!(
        "{context} [{category}]: Redis administration error details were redacted"
    ))
}

async fn admin_raw(
    state: &ToolState,
    redis_command: crate::RedisCommand,
    context: &str,
) -> tower_mcp::Result<RedisValue> {
    state
        .raw(redis_command, context)
        .await
        .map_err(|error| redacted_admin_error(error, context))
}

fn reply_bytes(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => Ok(value),
        RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
            Ok(value.into_bytes())
        }
        RedisValue::Okay => Ok(b"OK".to_vec()),
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected text reply"
        ))),
    }
}

fn reply_string(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    String::from_utf8(reply_bytes(value, context)?)
        .map_err(|_| tower_mcp::Error::tool(format!("{context} returned non-UTF-8 text")))
}

fn reply_i64(value: RedisValue, context: &str) -> tower_mcp::Result<i64> {
    match value {
        RedisValue::Integer(value) => Ok(value),
        other => reply_string(other, context)?
            .parse()
            .map_err(|_| tower_mcp::Error::tool(format!("{context} was not an integer"))),
    }
}

fn reply_values(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<RedisValue>> {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => Ok(values),
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected collection reply"
        ))),
    }
}

fn reply_pairs(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<(RedisValue, RedisValue)>> {
    match value {
        RedisValue::Map(values) => Ok(values),
        RedisValue::Array(values) if values.len() % 2 == 0 => {
            let mut values = values.into_iter();
            let mut pairs = Vec::new();
            while let Some(key) = values.next() {
                pairs.push((key, values.next().expect("even response length")));
            }
            Ok(pairs)
        }
        _ => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected map reply"
        ))),
    }
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
struct ClusterFailure {
    node: String,
    error_code: String,
    message_redacted: bool,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterSummary {
    node_limit: usize,
    nodes_queried: usize,
    nodes_succeeded: usize,
    complete: bool,
    node_addresses_redacted: bool,
    failures: Vec<ClusterFailure>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NodeValue {
    node: String,
    value: JsonValue,
}

fn pseudonymous_node_replies(
    value: RedisValue,
    node_limit: usize,
    sanitize: impl Fn(RedisValue) -> JsonValue,
) -> tower_mcp::Result<(Vec<NodeValue>, Option<ClusterSummary>)> {
    let RedisValue::ClusterNodes(mut nodes) = value else {
        return Ok((
            vec![NodeValue {
                node: "configured-node".to_string(),
                value: sanitize(value),
            }],
            None,
        ));
    };
    if nodes.len() > node_limit {
        return Err(tower_mcp::Error::tool(format!(
            "cluster response included {} nodes; requested maximum is {node_limit}",
            nodes.len()
        )));
    }
    nodes.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    let nodes_queried = nodes.len();
    let mut replies = Vec::new();
    let mut failures = Vec::new();
    for (index, (_, value)) in nodes.into_iter().enumerate() {
        let node = format!("node-{}", index + 1);
        match value {
            RedisValue::ServerError { code, .. } => failures.push(ClusterFailure {
                node,
                error_code: code,
                message_redacted: true,
            }),
            value => replies.push(NodeValue {
                node,
                value: sanitize(value),
            }),
        }
    }
    let nodes_succeeded = replies.len();
    Ok((
        replies,
        Some(ClusterSummary {
            node_limit,
            nodes_queried,
            nodes_succeeded,
            complete: failures.is_empty(),
            node_addresses_redacted: true,
            failures,
        }),
    ))
}

fn text_key(value: &RedisValue) -> Option<String> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => {
            String::from_utf8(value.clone()).ok()
        }
        RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
            Some(value.clone())
        }
        _ => None,
    }
}

fn sensitive_field(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "address"
            | "addr"
            | "endpoint"
            | "hostname"
            | "host"
            | "ip"
            | "path"
            | "port"
            | "tls-port"
            | "tls_port"
            | "password"
            | "passwords"
            | "secret"
            | "token"
            | "arguments"
            | "argv"
            | "client-info"
            | "last_error"
    )
}

fn sanitize_admin_value(value: RedisValue) -> JsonValue {
    match value {
        RedisValue::Map(values) => JsonValue::Array(
            values
                .into_iter()
                .map(|(key, value)| {
                    let redacted = text_key(&key).is_some_and(|key| sensitive_field(&key));
                    json!({
                        "key": redis_value_to_json(&key),
                        "value": if redacted { json!("[redacted]") } else { sanitize_admin_value(value) }
                    })
                })
                .collect(),
        ),
        RedisValue::Array(values)
            if values.len() % 2 == 0
                && values
                    .iter()
                    .step_by(2)
                    .all(|value| text_key(value).is_some()) =>
        {
            let mut values = values.into_iter();
            let mut result = Vec::new();
            while let Some(key) = values.next() {
                let value = values.next().expect("even response length");
                let redacted = text_key(&key).is_some_and(|key| sensitive_field(&key));
                result.push(json!({
                    "key": redis_value_to_json(&key),
                    "value": if redacted { json!("[redacted]") } else { sanitize_admin_value(value) }
                }));
            }
            JsonValue::Array(result)
        }
        RedisValue::Array(values) | RedisValue::Set(values) => {
            JsonValue::Array(values.into_iter().map(sanitize_admin_value).collect())
        }
        RedisValue::Attribute { data, attributes } => json!({
            "data": sanitize_admin_value(*data),
            "attributes": attributes.into_iter().map(|(key, value)| json!({
                "key": redis_value_to_json(&key),
                "value": if text_key(&key).is_some_and(|key| sensitive_field(&key)) {
                    json!("[redacted]")
                } else {
                    sanitize_admin_value(value)
                },
            })).collect::<Vec<_>>()
        }),
        other => redis_value_to_json(&other),
    }
}

fn sanitize_cluster_text(value: RedisValue) -> JsonValue {
    match value {
        RedisValue::BulkString(bytes) => match String::from_utf8(bytes) {
            Ok(text) => json!(redact_cluster_node_lines(&text)),
            Err(error) => sanitize_admin_value(RedisValue::BulkString(error.into_bytes())),
        },
        RedisValue::SimpleString(text) | RedisValue::VerbatimString { text, .. } => {
            json!(redact_cluster_node_lines(&text))
        }
        other => sanitize_admin_value(other),
    }
}

fn redact_cluster_node_lines(text: &str) -> String {
    text.lines()
        .map(|line| {
            let mut fields = line
                .split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>();
            if fields.len() >= 2 {
                fields[1] = "[address-redacted]".to_string();
            }
            fields.join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclCategoriesInput {
    /// Optional exact ACL category name without the leading `@`.
    #[serde(default)]
    category: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclCategoriesOutput {
    category: Option<String>,
    values: Vec<EncodedValue>,
}

fn acl_categories_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_categories")
        .title("Inspect Redis ACL Categories")
        .description("List Redis ACL categories or commands in one exact category. Requires Redis @admin +acl|cat permission; read-only and node-local.")
        .output_schema(output_schema::<AclCategoriesOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<AclCategoriesInput>| async move {
            let mut redis_command = command("redis_acl_categories", AccessMode::ReadOnly, "ACL");
            redis_command.arg("CAT");
            if let Some(category) = &input.category {
                validate_identifier(category, "category")?;
                redis_command.arg(category.as_bytes());
            }
            let values = reply_values(admin_raw(&state, redis_command, "ACL CAT failed").await?, "ACL CAT")?
                .into_iter()
                .map(|value| reply_bytes(value, "ACL CAT item").map(EncodedValue::new))
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            let count = values.len();
            state.output_collection(&AclCategoriesOutput { category: input.category, values }, count, "Request one exact ACL category.")
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclUsersOutput {
    users: Vec<EncodedValue>,
}

fn acl_users_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_users")
        .title("List Redis ACL Users")
        .description("List ACL usernames without rules or credentials. Requires Redis @admin +acl|users permission; read-only and node-local.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<AclUsersOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let mut redis_command = command("redis_acl_users", AccessMode::ReadOnly, "ACL");
            redis_command.arg("USERS");
            let users = reply_values(admin_raw(&state, redis_command, "ACL USERS failed").await?, "ACL USERS")?
                .into_iter()
                .map(|value| reply_bytes(value, "ACL USERS item").map(EncodedValue::new))
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            let count = users.len();
            state.output_collection(&AclUsersOutput { users }, count, "Reduce the number of ACL users on the target.")
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclUserInput {
    /// Exact Redis ACL username.
    username: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclUserOutput {
    username: String,
    exists: bool,
    flags: Vec<String>,
    password_hash_count: usize,
    key_pattern_count: usize,
    channel_pattern_count: usize,
    selector_count: usize,
    commands_redacted: bool,
    credentials_redacted: bool,
}

fn value_count(value: RedisValue) -> usize {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => values.len(),
        RedisValue::Nil => 0,
        _ => 1,
    }
}

fn acl_user_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_user")
        .title("Inspect One Redis ACL User")
        .description("Summarize one ACL user while always redacting password hashes, command rules, key patterns, and channel patterns. Requires Redis @admin +acl|getuser; read-only and node-local.")
        .output_schema(output_schema::<AclUserOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<AclUserInput>| async move {
            validate_identifier(&input.username, "username")?;
            let mut redis_command = command("redis_acl_user", AccessMode::ReadOnly, "ACL");
            redis_command.arg("GETUSER").arg(input.username.as_bytes());
            let value = admin_raw(&state, redis_command, "ACL GETUSER failed").await?;
            if matches!(value, RedisValue::Nil) {
                return state.output(&AclUserOutput {
                    username: input.username, exists: false, flags: Vec::new(), password_hash_count: 0,
                    key_pattern_count: 0, channel_pattern_count: 0, selector_count: 0,
                    commands_redacted: true, credentials_redacted: true,
                });
            }
            let mut flags = Vec::new();
            let mut password_hash_count = 0;
            let mut key_pattern_count = 0;
            let mut channel_pattern_count = 0;
            let mut selector_count = 0;
            for (key, value) in reply_pairs(value, "ACL GETUSER")? {
                match reply_string(key, "ACL GETUSER field")?.to_ascii_lowercase().as_str() {
                    "flags" => flags = reply_values(value, "ACL flags")?.into_iter()
                        .map(|value| reply_string(value, "ACL flag")).collect::<tower_mcp::Result<Vec<_>>>()?,
                    "passwords" => password_hash_count = value_count(value),
                    "keys" => key_pattern_count = value_count(value),
                    "channels" => channel_pattern_count = value_count(value),
                    "selectors" => selector_count = value_count(value),
                    _ => {}
                }
            }
            state.output(&AclUserOutput {
                username: input.username, exists: true, flags, password_hash_count,
                key_pattern_count, channel_pattern_count, selector_count,
                commands_redacted: true, credentials_redacted: true,
            })
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclRuleSummary {
    username: Option<String>,
    enabled: bool,
    no_password: bool,
    selector_count: usize,
    rules_redacted: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclRulesOutput {
    users: Vec<AclRuleSummary>,
}

fn acl_rules_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_rules")
        .title("Summarize Redis ACL Rules")
        .description("Summarize ACL LIST without returning password hashes, key/channel patterns, command rules, or future tokens. Requires Redis @admin +acl|list; read-only and node-local.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<AclRulesOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let mut redis_command = command("redis_acl_rules", AccessMode::ReadOnly, "ACL");
            redis_command.arg("LIST");
            let users = reply_values(admin_raw(&state, redis_command, "ACL LIST failed").await?, "ACL LIST")?
                .into_iter()
                .map(|value| {
                    let line = reply_string(value, "ACL LIST rule")?;
                    let tokens = line.split_whitespace().collect::<Vec<_>>();
                    let username = tokens.windows(2).find(|pair| pair[0] == "user").map(|pair| pair[1].to_string());
                    Ok(AclRuleSummary {
                        username,
                        enabled: tokens.contains(&"on"),
                        no_password: tokens.contains(&"nopass"),
                        selector_count: tokens.iter().filter(|token| token.starts_with('(')).count(),
                        rules_redacted: true,
                    })
                })
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            let count = users.len();
            state.output_collection(&AclRulesOutput { users }, count, "Reduce the number of ACL users on the target.")
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclDryRunInput {
    /// Exact ACL username whose permissions should be checked.
    username: String,
    /// Redis command name. This is checked, never executed.
    command: String,
    /// Bounded command arguments represented as strings or standard base64.
    #[serde(default)]
    arguments: Vec<EncodedInput>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedInput {
    value: String,
    #[serde(default)]
    encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StatusOutput {
    status: String,
}

fn acl_dryrun_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_dryrun")
        .title("Dry-run Redis ACL Permission")
        .description("Ask Redis whether an ACL user may run one bounded command without executing it. Command arguments are sent to Redis but are never returned or logged. Requires Redis @admin +acl|dryrun; read-only and node-local.")
        .output_schema(output_schema::<StatusOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<AclDryRunInput>| async move {
            validate_identifier(&input.username, "username")?;
            validate_command_name(&input.command)?;
            if input.arguments.len() > MAX_COMMAND_ARGUMENTS {
                return Err(tower_mcp::Error::tool(format!("arguments may contain at most {MAX_COMMAND_ARGUMENTS} values")));
            }
            let mut total = 0usize;
            let arguments = input.arguments.into_iter().map(|argument| {
                let value = decode_input(&argument.value, argument.encoding, "argument")?;
                total = total.saturating_add(value.len());
                Ok(value)
            }).collect::<tower_mcp::Result<Vec<_>>>()?;
            if total > MAX_ARGUMENT_BYTES {
                return Err(tower_mcp::Error::tool(format!("encoded arguments exceed the {MAX_ARGUMENT_BYTES}-byte request limit")));
            }
            let mut redis_command = command("redis_acl_dryrun", AccessMode::ReadOnly, "ACL");
            redis_command.arg("DRYRUN").arg(input.username.as_bytes()).arg(input.command.as_bytes()).args(arguments);
            let status = reply_string(admin_raw(&state, redis_command, "ACL DRYRUN failed").await?, "ACL DRYRUN")?;
            state.output(&StatusOutput { status })
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LimitInput {
    /// Maximum records requested from Redis.
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AclLogOutput {
    returned: usize,
    by_reason: BTreeMap<String, usize>,
    by_context: BTreeMap<String, usize>,
    identities_objects_and_arguments_redacted: bool,
}

fn acl_log_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_log")
        .title("Inspect Redis ACL Log")
        .description("Return bounded ACL denial counts grouped by reason and context. Usernames, client identity, keys/channels, commands, and arguments are always redacted. Requires Redis @admin +acl|log; read-only and node-local.")
        .output_schema(output_schema::<AclLogOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<LimitInput>| async move {
            state.validate_requested_entries(input.limit, "limit")?;
            let mut redis_command = command("redis_acl_log", AccessMode::ReadOnly, "ACL");
            redis_command.arg("LOG").arg(input.limit.to_string());
            let entries = reply_values(admin_raw(&state, redis_command, "ACL LOG failed").await?, "ACL LOG")?;
            let mut by_reason = BTreeMap::new();
            let mut by_context = BTreeMap::new();
            for entry in &entries {
                for (key, value) in reply_pairs(entry.clone(), "ACL LOG entry")? {
                    let key = reply_string(key, "ACL LOG field")?.to_ascii_lowercase();
                    if matches!(key.as_str(), "reason" | "context") {
                        let value = reply_string(value, "ACL LOG classification")?;
                        let target = if key == "reason" { &mut by_reason } else { &mut by_context };
                        *target.entry(value).or_insert(0) += 1;
                    }
                }
            }
            state.output_collection(&AclLogOutput {
                returned: entries.len(), by_reason, by_context,
                identities_objects_and_arguments_redacted: true,
            }, entries.len(), "Retry with a smaller ACL LOG limit.")
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConfirmationInput {
    /// Must be true to confirm this operationally destructive action.
    confirm: bool,
}

fn require_confirmation(confirm: bool, action: &str) -> tower_mcp::Result<()> {
    if confirm {
        Ok(())
    } else {
        Err(tower_mcp::Error::tool(format!(
            "set confirm=true to execute {action}"
        )))
    }
}

fn acl_log_reset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_acl_log_reset")
        .title("Reset Redis ACL Log")
        .description("Clear this node's ACL security log. Requires Full access and Redis @admin +acl|log; destructive, idempotent, and node-local.")
        .output_schema(output_schema::<StatusOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ConfirmationInput>| async move {
            require_confirmation(input.confirm, "ACL LOG RESET")?;
            let mut redis_command = command("redis_acl_log_reset", AccessMode::Full, "ACL");
            redis_command.arg("LOG").arg("RESET");
            let status = reply_string(admin_raw(&state, redis_command, "ACL LOG RESET failed").await?, "ACL LOG RESET")?;
            state.output(&StatusOutput { status })
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BackupStatusOutput {
    state: Option<String>,
    error_present: bool,
    start_time_unix_seconds: Option<i64>,
    end_time_unix_seconds: Option<i64>,
    unrecognized_fields_redacted: usize,
    error_and_future_field_values_redacted: bool,
}

fn backup_status_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_backup_status")
        .title("Inspect Redis Backup Status")
        .description("Inspect Redis 8 backup state with paths and future sensitive fields redacted. Requires Redis @admin +backup|status; read-only and node-local.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<BackupStatusOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let mut redis_command = command("redis_backup_status", AccessMode::ReadOnly, "BACKUP");
            redis_command.arg("STATUS");
            let raw = admin_raw(&state, redis_command, "BACKUP STATUS failed").await?;
            let mut output = BackupStatusOutput {
                state: None,
                error_present: false,
                start_time_unix_seconds: None,
                end_time_unix_seconds: None,
                unrecognized_fields_redacted: 0,
                error_and_future_field_values_redacted: true,
            };
            for (key, value) in reply_pairs(raw, "BACKUP STATUS")? {
                match reply_string(key, "BACKUP STATUS field")?.to_ascii_lowercase().as_str() {
                    "state" => output.state = Some(reply_string(value, "BACKUP STATUS state")?),
                    "error" => output.error_present = !reply_bytes(value, "BACKUP STATUS error")?.is_empty(),
                    "start_time" => output.start_time_unix_seconds = Some(reply_i64(value, "BACKUP STATUS start_time")?),
                    "end_time" => output.end_time_unix_seconds = Some(reply_i64(value, "BACKUP STATUS end_time")?),
                    _ => output.unrecognized_fields_redacted += 1,
                }
            }
            state.output(&output)
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BackupFilesOutput {
    entry_count: usize,
    paths_and_file_metadata_redacted: bool,
}

fn backup_files_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_backup_files")
        .title("Count Redis Backup Files")
        .description("Count Redis backup file records without exposing absolute paths or file metadata. Requires Redis @admin +backup|list; read-only and node-local.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<BackupFilesOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let mut redis_command = command("redis_backup_files", AccessMode::ReadOnly, "BACKUP");
            redis_command.arg("LIST");
            let value = admin_raw(&state, redis_command, "BACKUP LIST failed").await?;
            let entry_count = match value {
                RedisValue::Array(values) | RedisValue::Set(values) => values.len(),
                RedisValue::Map(values) => values.len(),
                RedisValue::Nil => 0,
                _ => 1,
            };
            state.output(&BackupFilesOutput { entry_count, paths_and_file_metadata_redacted: true })
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ClusterInspectKind {
    Nodes,
    Shards,
    Links,
    MyId,
    MyShardId,
    MigrationStatus,
}

impl ClusterInspectKind {
    fn subcommand(self) -> &'static str {
        match self {
            Self::Nodes => "NODES",
            Self::Shards => "SHARDS",
            Self::Links => "LINKS",
            Self::MyId => "MYID",
            Self::MyShardId => "MYSHARDID",
            Self::MigrationStatus => "MIGRATION",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInspectInput {
    operation: ClusterInspectKind,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInspectOutput {
    operation: String,
    replies: Vec<NodeValue>,
    cluster: Option<ClusterSummary>,
    addresses_redacted: bool,
}

fn cluster_inspect_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_cluster_inspect")
        .title("Inspect Redis Cluster Control Plane")
        .description("Inspect bounded Cluster nodes, shards, links, identities, or migration status. Requires Redis @admin +cluster; read-only, Cluster-only, and always redacts node addresses. Partial failures are returned per pseudonymous node.")
        .output_schema(output_schema::<ClusterInspectOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInspectInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let mut redis_command = command("redis_cluster_inspect", AccessMode::ReadOnly, "CLUSTER");
            redis_command.arg(input.operation.subcommand());
            if matches!(input.operation, ClusterInspectKind::MigrationStatus) { redis_command.arg("STATUS"); }
            redis_command.aggregate_cluster_nodes(input.max_cluster_nodes);
            let raw = admin_raw(&state, redis_command, "CLUSTER inspection failed").await?;
            let entries = redis_value_collection_entries(&raw);
            let (replies, cluster) = pseudonymous_node_replies(raw, input.max_cluster_nodes, |value| {
                if matches!(input.operation, ClusterInspectKind::Nodes) { sanitize_cluster_text(value) } else { sanitize_admin_value(value) }
            })?;
            state.output_collection(&ClusterInspectOutput {
                operation: input.operation.subcommand().to_ascii_lowercase(), replies, cluster,
                addresses_redacted: true,
            }, entries, "Retry with a narrower operation or smaller max_cluster_nodes value.")
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ClusterSlotKind {
    KeySlot,
    CountKeys,
    GetKeys,
    FailureReports,
    Replicas,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterSlotInput {
    operation: ClusterSlotKind,
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    slot: Option<u16>,
    #[serde(default)]
    node_id: Option<String>,
    #[serde(default)]
    count: Option<usize>,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterSlotOutput {
    operation: String,
    replies: Vec<NodeValue>,
    cluster: Option<ClusterSummary>,
    addresses_redacted: bool,
}

fn cluster_slot_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_cluster_slot")
        .title("Inspect Redis Cluster Slots")
        .description("Run one bounded Cluster slot, replica, or failure-report inspection. Slot-local counts and keys fan out to primaries; node-local reports fan out to all nodes. Requires Redis @admin +cluster; read-only and Cluster-only with pseudonymous partial failures.")
        .output_schema(output_schema::<ClusterSlotOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterSlotInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let mut redis_command = command("redis_cluster_slot", AccessMode::ReadOnly, "CLUSTER");
            let operation = match input.operation {
                ClusterSlotKind::KeySlot => {
                    let key = input.key.ok_or_else(|| tower_mcp::Error::tool("key is required for key_slot"))?;
                    redis_command.arg("KEYSLOT").arg(decode_input(&key, input.key_encoding, "key")?); "keyslot"
                }
                ClusterSlotKind::CountKeys => {
                    let slot = input.slot.ok_or_else(|| tower_mcp::Error::tool("slot is required for count_keys"))?;
                    if slot > 16_383 { return Err(tower_mcp::Error::tool("slot must be between 0 and 16383")); }
                    redis_command.arg("COUNTKEYSINSLOT").arg(slot.to_string())
                        .aggregate_cluster_primaries(input.max_cluster_nodes); "countkeysinslot"
                }
                ClusterSlotKind::GetKeys => {
                    let slot = input.slot.ok_or_else(|| tower_mcp::Error::tool("slot is required for get_keys"))?;
                    if slot > 16_383 { return Err(tower_mcp::Error::tool("slot must be between 0 and 16383")); }
                    let count = input.count.ok_or_else(|| tower_mcp::Error::tool("count is required for get_keys"))?;
                    state.validate_requested_entries(count, "count")?;
                    let total = count.checked_mul(input.max_cluster_nodes).ok_or_else(|| tower_mcp::Error::tool("count times max_cluster_nodes overflowed"))?;
                    state.validate_requested_entries(total, "count times max_cluster_nodes")?;
                    redis_command.arg("GETKEYSINSLOT").arg(slot.to_string()).arg(count.to_string())
                        .aggregate_cluster_primaries(input.max_cluster_nodes); "getkeysinslot"
                }
                ClusterSlotKind::FailureReports => {
                    let node_id = input.node_id.ok_or_else(|| tower_mcp::Error::tool("node_id is required for failure_reports"))?;
                    validate_identifier(&node_id, "node_id")?;
                    redis_command.arg("COUNT-FAILURE-REPORTS").arg(node_id.as_bytes())
                        .aggregate_cluster_nodes(input.max_cluster_nodes); "count-failure-reports"
                }
                ClusterSlotKind::Replicas => {
                    let node_id = input.node_id.ok_or_else(|| tower_mcp::Error::tool("node_id is required for replicas"))?;
                    validate_identifier(&node_id, "node_id")?;
                    redis_command.arg("REPLICAS").arg(node_id.as_bytes())
                        .aggregate_cluster_nodes(input.max_cluster_nodes); "replicas"
                }
            };
            let raw = admin_raw(&state, redis_command, "CLUSTER slot inspection failed").await?;
            let entries = redis_value_collection_entries(&raw);
            let (replies, cluster) = pseudonymous_node_replies(raw, input.max_cluster_nodes, |value| {
                if matches!(input.operation, ClusterSlotKind::Replicas) { sanitize_cluster_text(value) } else { sanitize_admin_value(value) }
            })?;
            state.output_collection(&ClusterSlotOutput {
                operation: operation.to_string(), replies, cluster, addresses_redacted: true,
            }, entries, "Retry GETKEYSINSLOT with a smaller count or max_cluster_nodes value.")
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterSlotStatsInput {
    #[schemars(range(min = 0, max = 16383))]
    start_slot: u16,
    #[schemars(range(min = 0, max = 16383))]
    end_slot: u16,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn cluster_slot_stats_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_cluster_slot_stats")
        .title("Inspect Redis Cluster Slot Statistics")
        .description("Read Redis 8.2 slot statistics for one bounded inclusive slot range across Cluster primaries. Requires Redis @slow +cluster|slot-stats; read-only, byte-budgeted, and address-redacted.")
        .output_schema(output_schema::<FanoutStatusOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterSlotStatsInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            if input.start_slot > 16_383 || input.end_slot > 16_383 || input.start_slot > input.end_slot {
                return Err(tower_mcp::Error::tool("slot range must satisfy 0 <= start_slot <= end_slot <= 16383"));
            }
            let requested_slots = usize::from(input.end_slot - input.start_slot) + 1;
            state.validate_requested_entries(requested_slots, "slot range")?;
            let total = requested_slots.checked_mul(input.max_cluster_nodes)
                .ok_or_else(|| tower_mcp::Error::tool("slot range times max_cluster_nodes overflowed"))?;
            state.validate_requested_entries(total, "slot range times max_cluster_nodes")?;
            let mut redis_command = command("redis_cluster_slot_stats", AccessMode::ReadOnly, "CLUSTER");
            redis_command.arg("SLOT-STATS").arg("SLOTSRANGE")
                .arg(input.start_slot.to_string()).arg(input.end_slot.to_string())
                .aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "CLUSTER SLOT-STATS failed").await
        })
        .build()
}

const SAFE_CONFIG_GET: &[&str] = &[
    "activedefrag",
    "appendfsync",
    "appendonly",
    "databases",
    "hz",
    "latency-monitor-threshold",
    "lazyfree-lazy-user-flush",
    "maxclients",
    "maxmemory",
    "maxmemory-policy",
    "save",
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "tcp-keepalive",
    "timeout",
];

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConfigGetInput {
    parameters: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConfigGetOutput {
    values: BTreeMap<String, EncodedValue>,
    allowlist_enforced: bool,
}

fn config_get_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_config_get")
        .title("Read Safe Redis Configuration")
        .description("Read exact non-secret operational configuration names from a fixed allowlist; glob patterns, paths, credentials, ACL files, and future fields are rejected. Requires Redis @admin +config|get; read-only and node-local.")
        .output_schema(output_schema::<ConfigGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ConfigGetInput>| async move {
            if input.parameters.is_empty() || input.parameters.len() > SAFE_CONFIG_GET.len() {
                return Err(tower_mcp::Error::tool(format!("parameters must contain between 1 and {} names", SAFE_CONFIG_GET.len())));
            }
            for parameter in &input.parameters {
                if !SAFE_CONFIG_GET.contains(&parameter.as_str()) {
                    return Err(tower_mcp::Error::tool(format!("configuration parameter {parameter:?} is not in the non-secret allowlist")));
                }
            }
            let mut redis_command = command("redis_config_get", AccessMode::ReadOnly, "CONFIG");
            redis_command.arg("GET").args(input.parameters.iter().map(String::as_bytes));
            let mut values = BTreeMap::new();
            for (key, value) in reply_pairs(admin_raw(&state, redis_command, "CONFIG GET failed").await?, "CONFIG GET")? {
                let key = reply_string(key, "CONFIG GET name")?;
                if input.parameters.contains(&key) {
                    values.insert(key, EncodedValue::new(reply_bytes(value, "CONFIG GET value")?));
                }
            }
            state.output_collection(&ConfigGetOutput { values, allowlist_enforced: true }, input.parameters.len(), "Request fewer configuration parameters.")
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum SafeConfigParameter {
    LatencyMonitorThreshold,
    SlowlogLogSlowerThan,
    SlowlogMaxLen,
    TcpKeepalive,
    Timeout,
}

impl SafeConfigParameter {
    fn name(self) -> &'static str {
        match self {
            Self::LatencyMonitorThreshold => "latency-monitor-threshold",
            Self::SlowlogLogSlowerThan => "slowlog-log-slower-than",
            Self::SlowlogMaxLen => "slowlog-max-len",
            Self::TcpKeepalive => "tcp-keepalive",
            Self::Timeout => "timeout",
        }
    }
    fn validate(self, value: i64) -> bool {
        match self {
            Self::SlowlogLogSlowerThan => (-1..=3_600_000_000).contains(&value),
            Self::SlowlogMaxLen => (0..=1_000_000).contains(&value),
            Self::LatencyMonitorThreshold => (0..=3_600_000).contains(&value),
            Self::TcpKeepalive => (0..=3_600).contains(&value),
            Self::Timeout => (0..=86_400).contains(&value),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConfigSetInput {
    parameter: SafeConfigParameter,
    value: i64,
    confirm_service_impact: bool,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FanoutStatusOutput {
    scope: String,
    replies: Vec<NodeValue>,
    cluster: Option<ClusterSummary>,
}

async fn fanout_status(
    state: &ToolState,
    redis_command: crate::RedisCommand,
    max_cluster_nodes: usize,
    context: &str,
) -> tower_mcp::Result<(FanoutStatusOutput, usize)> {
    let raw = admin_raw(state, redis_command, context).await?;
    let entries = redis_value_collection_entries(&raw);
    let (replies, cluster) =
        pseudonymous_node_replies(raw, max_cluster_nodes, sanitize_admin_value)?;
    Ok((
        FanoutStatusOutput {
            scope: if cluster.is_some() {
                "cluster"
            } else {
                "standalone"
            }
            .to_string(),
            replies,
            cluster,
        },
        entries,
    ))
}

async fn fanout_result(
    state: &ToolState,
    redis_command: crate::RedisCommand,
    max_cluster_nodes: usize,
    context: &str,
) -> tower_mcp::Result<CallToolResult> {
    let (output, entries) = fanout_status(state, redis_command, max_cluster_nodes, context).await?;
    state.output_collection(
        &output,
        entries,
        "Retry with a narrower operation, range, or max_cluster_nodes value.",
    )
}

fn config_set_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_config_set")
        .title("Set Safe Redis Operational Configuration")
        .description("Set one reversible numeric setting from a fixed allowlist across bounded Cluster nodes. Requires Full access and Redis @admin +config|set; may affect service behavior. Values are never echoed in errors.")
        .output_schema(output_schema::<FanoutStatusOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ConfigSetInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            require_confirmation(input.confirm_service_impact, "CONFIG SET")?;
            if !input.parameter.validate(input.value) { return Err(tower_mcp::Error::tool("value is outside the safe range for this parameter")); }
            let mut redis_command = command("redis_config_set", AccessMode::Full, "CONFIG");
            redis_command.arg("SET").arg(input.parameter.name()).arg(input.value.to_string()).aggregate_cluster_nodes(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "CONFIG SET failed").await
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterConfirmationInput {
    confirm: bool,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn config_resetstat_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_config_resetstat")
        .title("Reset Redis Server Statistics")
        .description("Reset INFO statistics on every bounded Cluster node. Requires Full access and Redis @admin +config|resetstat; destructive to observability history and idempotent.")
        .output_schema(output_schema::<FanoutStatusOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterConfirmationInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            require_confirmation(input.confirm, "CONFIG RESETSTAT")?;
            let mut redis_command = command("redis_config_resetstat", AccessMode::Full, "CONFIG");
            redis_command.arg("RESETSTAT").aggregate_cluster_nodes(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "CONFIG RESETSTAT failed").await
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ServerStateOutput {
    deployment: String,
    unix_time_seconds: i64,
    microseconds: i64,
    last_save_unix_seconds: i64,
    role: String,
    replication_details_redacted: bool,
}

fn server_state_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_server_state")
        .title("Read Redis Server State")
        .description("Read TIME, LASTSAVE, and ROLE as a compact server-state snapshot. Replication peer addresses and details are redacted. Requires Redis @fast +time +lastsave +role; read-only and node-local.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<ServerStateOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let time = reply_values(admin_raw(&state, command("redis_server_state", AccessMode::ReadOnly, "TIME"), "TIME failed").await?, "TIME")?;
            if time.len() != 2 { return Err(tower_mcp::Error::tool("TIME returned an unexpected reply")); }
            let unix_time_seconds = reply_i64(time[0].clone(), "TIME seconds")?;
            let microseconds = reply_i64(time[1].clone(), "TIME microseconds")?;
            let last_save_unix_seconds = reply_i64(admin_raw(&state, command("redis_server_state", AccessMode::ReadOnly, "LASTSAVE"), "LASTSAVE failed").await?, "LASTSAVE")?;
            let role_values = reply_values(admin_raw(&state, command("redis_server_state", AccessMode::ReadOnly, "ROLE"), "ROLE failed").await?, "ROLE")?;
            let role = role_values.into_iter().next().ok_or_else(|| tower_mcp::Error::tool("ROLE returned an empty reply")).and_then(|value| reply_string(value, "ROLE name"))?;
            state.output(&ServerStateOutput {
                deployment: state.deployment().as_str().to_string(), unix_time_seconds, microseconds,
                last_save_unix_seconds, role, replication_details_redacted: true,
            })
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum LatencyKind {
    Latest,
    Doctor,
    Graph,
    Histogram,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LatencyOverviewInput {
    operation: LatencyKind,
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    commands: Vec<String>,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn generic_fanout_output_schema() -> JsonValue {
    output_schema::<FanoutStatusOutput>()
}

fn latency_overview_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_latency_overview")
        .title("Inspect Redis Latency")
        .description("Inspect LATENCY LATEST, DOCTOR, GRAPH, or bounded HISTOGRAM results across Redis nodes. Requires Redis @admin +latency; read-only, byte-budgeted, and address-redacted.")
        .output_schema(generic_fanout_output_schema())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<LatencyOverviewInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            if input.commands.len() > MAX_COMMAND_ARGUMENTS { return Err(tower_mcp::Error::tool(format!("commands may contain at most {MAX_COMMAND_ARGUMENTS} names"))); }
            let mut redis_command = command("redis_latency_overview", AccessMode::ReadOnly, "LATENCY");
            match input.operation {
                LatencyKind::Latest => { redis_command.arg("LATEST"); }
                LatencyKind::Doctor => { redis_command.arg("DOCTOR"); }
                LatencyKind::Graph => {
                    let event = input.event.ok_or_else(|| tower_mcp::Error::tool("event is required for graph"))?;
                    validate_identifier(&event, "event")?; redis_command.arg("GRAPH").arg(event.as_bytes());
                }
                LatencyKind::Histogram => {
                    if input.commands.is_empty() {
                        return Err(tower_mcp::Error::tool("commands must contain at least one exact command name for histogram"));
                    }
                    let total = input.commands.len().checked_mul(input.max_cluster_nodes)
                        .ok_or_else(|| tower_mcp::Error::tool("commands times max_cluster_nodes overflowed"))?;
                    state.validate_requested_entries(total, "commands times max_cluster_nodes")?;
                    redis_command.arg("HISTOGRAM");
                    for command_name in &input.commands { validate_command_name(command_name)?; redis_command.arg(command_name.as_bytes()); }
                }
            }
            redis_command.aggregate_cluster_nodes(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "LATENCY inspection failed").await
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LatencyResetInput {
    #[serde(default)]
    events: Vec<String>,
    confirm: bool,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn latency_reset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_latency_reset")
        .title("Reset Redis Latency Events")
        .description("Reset all or selected latency-monitor events across bounded Redis nodes. Requires Full access and Redis @admin +latency|reset; destructive to observability history and idempotent.")
        .output_schema(generic_fanout_output_schema())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<LatencyResetInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?; require_confirmation(input.confirm, "LATENCY RESET")?;
            if input.events.len() > MAX_COMMAND_ARGUMENTS { return Err(tower_mcp::Error::tool(format!("events may contain at most {MAX_COMMAND_ARGUMENTS} names"))); }
            let mut redis_command = command("redis_latency_reset", AccessMode::Full, "LATENCY");
            redis_command.arg("RESET");
            for event in &input.events { validate_identifier(event, "event")?; redis_command.arg(event.as_bytes()); }
            redis_command.aggregate_cluster_nodes(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "LATENCY RESET failed").await
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum MemoryDiagnosticKind {
    Doctor,
    MallocStats,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemoryDiagnosticInput {
    operation: MemoryDiagnosticKind,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn memory_diagnostics_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_memory_diagnostics")
        .title("Inspect Redis Memory Diagnostics")
        .description("Read MEMORY DOCTOR or MALLOC-STATS once per bounded Cluster primary. Requires Redis @read +memory; read-only and byte-budgeted with pseudonymous node labels.")
        .output_schema(generic_fanout_output_schema())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<MemoryDiagnosticInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let mut redis_command = command("redis_memory_diagnostics", AccessMode::ReadOnly, "MEMORY");
            redis_command.arg(match input.operation { MemoryDiagnosticKind::Doctor => "DOCTOR", MemoryDiagnosticKind::MallocStats => "MALLOC-STATS" });
            redis_command.aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "MEMORY diagnostics failed").await
        })
        .build()
}

fn memory_purge_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_memory_purge")
        .title("Purge Redis Allocator Memory")
        .description("Ask every bounded Cluster primary's allocator to release reclaimable pages. Requires Full access and Redis @slow +memory|purge; may cause latency and is idempotent.")
        .output_schema(generic_fanout_output_schema())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterConfirmationInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?; require_confirmation(input.confirm, "MEMORY PURGE")?;
            let mut redis_command = command("redis_memory_purge", AccessMode::Full, "MEMORY");
            redis_command.arg("PURGE").aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "MEMORY PURGE failed").await
        })
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SlowlogLenOutput {
    replies: Vec<NodeValue>,
    cluster: Option<ClusterSummary>,
}

fn slowlog_len_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_slowlog_len")
        .title("Count Redis Slow Log Entries")
        .description("Count slow-log entries on every bounded Redis node without returning command arguments. Requires Redis @admin +slowlog|len; read-only and address-redacted.")
        .output_schema(output_schema::<SlowlogLenOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let mut redis_command = command("redis_slowlog_len", AccessMode::ReadOnly, "SLOWLOG");
            redis_command.arg("LEN").aggregate_cluster_nodes(input.max_cluster_nodes);
            let raw = admin_raw(&state, redis_command, "SLOWLOG LEN failed").await?;
            let entries = redis_value_collection_entries(&raw);
            let (replies, cluster) = pseudonymous_node_replies(raw, input.max_cluster_nodes, sanitize_admin_value)?;
            state.output_collection(&SlowlogLenOutput { replies, cluster }, entries, "Retry with a smaller max_cluster_nodes value.")
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterInput {
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn slowlog_reset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_slowlog_reset")
        .title("Reset Redis Slow Logs")
        .description("Clear slow-log history on every bounded Redis node. Requires Full access and Redis @admin +slowlog|reset; destructive to observability history and idempotent.")
        .output_schema(generic_fanout_output_schema())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterConfirmationInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?; require_confirmation(input.confirm, "SLOWLOG RESET")?;
            let mut redis_command = command("redis_slowlog_reset", AccessMode::Full, "SLOWLOG");
            redis_command.arg("RESET").aggregate_cluster_nodes(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "SLOWLOG RESET failed").await
        })
        .build()
}

fn hotkeys_get_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hotkeys_get")
        .title("Inspect Redis Hot Keys Tracking")
        .description("Read Redis 8.6 HOTKEYS tracking results from every bounded Cluster primary. Requires Redis @admin +hotkeys|get; read-only, byte-budgeted, and address-redacted.")
        .output_schema(generic_fanout_output_schema())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClusterInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let mut redis_command = command("redis_hotkeys_get", AccessMode::ReadOnly, "HOTKEYS");
            redis_command.arg("GET").aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "HOTKEYS GET failed").await
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HotkeysControlKind {
    Start,
    Stop,
    Reset,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HotkeysControlInput {
    operation: HotkeysControlKind,
    /// Required for START; bounds the server-global tracking session.
    #[serde(default)]
    #[schemars(range(min = 1, max = 3600))]
    duration_seconds: Option<u64>,
    /// Required for START; maximum hot keys retained per primary.
    #[serde(default)]
    #[schemars(range(min = 1, max = 1000))]
    count: Option<usize>,
    confirm_service_impact: bool,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn hotkeys_control_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hotkeys_control")
        .title("Control Redis Hot Keys Tracking")
        .description("Start a duration- and count-bounded Redis 8.6 HOTKEYS session, stop it, or reset its data on every bounded Cluster primary. Requires Full access and Redis @admin HOTKEYS permission; affects server-global tracking state.")
        .output_schema(generic_fanout_output_schema())
        .annotations(destructive_annotations(false))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<HotkeysControlInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?; require_confirmation(input.confirm_service_impact, "HOTKEYS control")?;
            let mut redis_command = command("redis_hotkeys_control", AccessMode::Full, "HOTKEYS");
            match input.operation {
                HotkeysControlKind::Start => {
                    let duration = input.duration_seconds.ok_or_else(|| tower_mcp::Error::tool("duration_seconds is required for start"))?;
                    if !(1..=3600).contains(&duration) { return Err(tower_mcp::Error::tool("duration_seconds must be between 1 and 3600")); }
                    let count = input.count.ok_or_else(|| tower_mcp::Error::tool("count is required for start"))?;
                    let total = count.checked_mul(input.max_cluster_nodes)
                        .ok_or_else(|| tower_mcp::Error::tool("count times max_cluster_nodes overflowed"))?;
                    state.validate_requested_entries(total, "count times max_cluster_nodes")?;
                    redis_command.arg("START").arg("COUNT").arg(count.to_string())
                        .arg("DURATION").arg(duration.to_string());
                }
                HotkeysControlKind::Stop => { redis_command.arg("STOP"); }
                HotkeysControlKind::Reset => { redis_command.arg("RESET"); }
            }
            redis_command.aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "HOTKEYS control failed").await
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ClientControlKind {
    Kill,
    UnblockError,
    UnblockTimeout,
    Unpause,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClientControlInput {
    operation: ClientControlKind,
    /// Exact CLIENT ID. Required for kill and unblock operations.
    #[serde(default)]
    id: Option<u64>,
    confirm_service_impact: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClientControlOutput {
    operation: String,
    affected: Option<i64>,
    status: Option<String>,
}

fn client_control_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_client_control")
        .title("Control One Redis Client")
        .description("Kill or unblock exactly one numeric CLIENT ID, or unpause clients on the configured standalone node. Broad filters are intentionally unavailable. Requires Full access and Redis @admin +client permissions; service-impacting and Standalone-only.")
        .output_schema(output_schema::<ClientControlOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<ClientControlInput>| async move {
            require_confirmation(input.confirm_service_impact, "CLIENT control")?;
            let mut redis_command = command("redis_client_control", AccessMode::Full, "CLIENT");
            let operation = match input.operation {
                ClientControlKind::Kill => { let id = input.id.ok_or_else(|| tower_mcp::Error::tool("id is required for kill"))?; redis_command.arg("KILL").arg("ID").arg(id.to_string()); "kill" }
                ClientControlKind::UnblockError => { let id = input.id.ok_or_else(|| tower_mcp::Error::tool("id is required for unblock_error"))?; redis_command.arg("UNBLOCK").arg(id.to_string()).arg("ERROR"); "unblock_error" }
                ClientControlKind::UnblockTimeout => { let id = input.id.ok_or_else(|| tower_mcp::Error::tool("id is required for unblock_timeout"))?; redis_command.arg("UNBLOCK").arg(id.to_string()).arg("TIMEOUT"); "unblock_timeout" }
                ClientControlKind::Unpause => { redis_command.arg("UNPAUSE"); "unpause" }
            };
            let raw = admin_raw(&state, redis_command, "CLIENT control failed").await?;
            let (affected, status) = match raw { RedisValue::Integer(value) => (Some(value), None), other => (None, Some(reply_string(other, "CLIENT control")?)) };
            state.output(&ClientControlOutput { operation: operation.to_string(), affected, status })
        })
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FlushScope {
    Database,
    All,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FlushMode {
    Sync,
    Async,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FlushInput {
    scope: FlushScope,
    mode: FlushMode,
    /// Must exactly equal FLUSHDB or FLUSHALL for the selected scope.
    confirmation: String,
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

fn flush_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_flush")
        .title("Flush Redis Data")
        .description("Delete all keys in the selected database or all databases across bounded Cluster primaries. Requires Full access and Redis @dangerous +flushdb/+flushall; irreversible, highly service-impacting, and explicitly confirmed.")
        .output_schema(generic_fanout_output_schema())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<FlushInput>| async move {
            validate_cluster_input(&state, input.max_cluster_nodes)?;
            let name = match input.scope { FlushScope::Database => "FLUSHDB", FlushScope::All => "FLUSHALL" };
            if input.confirmation != name { return Err(tower_mcp::Error::tool(format!("confirmation must exactly equal {name}"))); }
            let mut redis_command = command("redis_flush", AccessMode::Full, name);
            redis_command.arg(match input.mode { FlushMode::Sync => "SYNC", FlushMode::Async => "ASYNC" }).aggregate_cluster_primaries(input.max_cluster_nodes);
            fanout_result(&state, redis_command, input.max_cluster_nodes, "flush failed").await
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SwapDbInput {
    first_database: u32,
    second_database: u32,
    confirmation: String,
}

fn swapdb_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_swapdb")
        .title("Swap Redis Databases")
        .description("Atomically swap two distinct database numbers on a standalone Redis target. Requires Full access and Redis @keyspace +swapdb; destructive to logical key placement, service-impacting, and Standalone-only.")
        .output_schema(output_schema::<StatusOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(state, |State(state): State<Arc<ToolState>>, Json(input): Json<SwapDbInput>| async move {
            if input.first_database == input.second_database { return Err(tower_mcp::Error::tool("database numbers must be distinct")); }
            if input.confirmation != "SWAPDB" { return Err(tower_mcp::Error::tool("confirmation must exactly equal SWAPDB")); }
            let mut redis_command = command("redis_swapdb", AccessMode::Full, "SWAPDB");
            redis_command.arg(input.first_database.to_string()).arg(input.second_database.to_string());
            let status = reply_string(admin_raw(&state, redis_command, "SWAPDB failed").await?, "SWAPDB")?;
            state.output(&StatusOutput { status })
        })
        .build()
}
