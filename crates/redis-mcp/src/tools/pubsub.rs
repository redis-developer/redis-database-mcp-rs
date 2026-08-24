//! Bounded request/response Pub/Sub operations.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, command, decode_input, encode_bytes,
    output_limit_result, output_schema, read_annotations, write_annotations,
};
use crate::{AccessMode, RedisDeployment, RedisValue};

const DEFAULT_CHANNEL_LIMIT: usize = 100;
const DEFAULT_CLUSTER_NODE_LIMIT: usize = 32;
const MAX_CLUSTER_NODE_LIMIT: usize = 256;
const MAX_CHANNEL_ARGUMENTS: usize = 1_000;
const MAX_CHANNEL_BYTES: usize = 64 * 1024;
const MAX_PATTERN_BYTES: usize = 4 * 1024;
const MAX_PUBLISH_PAYLOAD_BYTES: usize = 1024 * 1024;

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(channels_tool(state.clone(), false));
    router = router.tool(numsub_tool(state.clone(), false));
    router = router.tool(numpat_tool(state.clone()));
    router = router.tool(channels_tool(state.clone(), true));
    router.tool(numsub_tool(state, true))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(publish_tool(state.clone(), false));
    router.tool(publish_tool(state, true))
}

fn default_channel_limit() -> usize {
    DEFAULT_CHANNEL_LIMIT
}

fn default_cluster_node_limit() -> usize {
    DEFAULT_CLUSTER_NODE_LIMIT
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

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedOutput {
    value: String,
    encoding: ValueEncoding,
}

impl From<Vec<u8>> for EncodedOutput {
    fn from(value: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(value);
        Self { value, encoding }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterNodeFailure {
    node: String,
    code: String,
    message: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClusterAggregation {
    node_limit: usize,
    nodes_queried: usize,
    nodes_succeeded: usize,
    complete: bool,
    failures: Vec<ClusterNodeFailure>,
}

struct Replies {
    values: Vec<RedisValue>,
    cluster: Option<ClusterAggregation>,
}

fn split_replies(value: RedisValue, node_limit: usize) -> tower_mcp::Result<Replies> {
    let RedisValue::ClusterNodes(nodes) = value else {
        return Ok(Replies {
            values: vec![value],
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
    for (node, value) in nodes {
        match value {
            RedisValue::ServerError { code, message } => failures.push(ClusterNodeFailure {
                node,
                code,
                message,
            }),
            value => values.push(value),
        }
    }
    failures.sort_unstable_by(|left, right| left.node.cmp(&right.node));
    if values.is_empty() && !failures.is_empty() {
        let codes = failures
            .iter()
            .map(|failure| format!("{}={}", failure.node, failure.code))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(tower_mcp::Error::tool(format!(
            "cluster Pub/Sub inspection failed on every node ({codes})"
        )));
    }
    Ok(Replies {
        cluster: Some(ClusterAggregation {
            node_limit,
            nodes_queried,
            nodes_succeeded: values.len(),
            complete: failures.is_empty(),
            failures,
        }),
        values,
    })
}

fn deployment_name(deployment: RedisDeployment) -> &'static str {
    deployment.as_str()
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

fn reply_bytes(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => Ok(value),
        RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
            Ok(value.into_bytes())
        }
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected channel value: {other:?}"
        ))),
    }
}

fn reply_count(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    match value {
        RedisValue::Integer(value) if value >= 0 => Ok(value as u64),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected count: {other:?}"
        ))),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PublishInput {
    /// Binary-safe channel name, limited to 64 KiB.
    channel: EncodedInput,
    /// Binary-safe message payload. Payloads are limited to 1 MiB.
    message: EncodedInput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PublishOutput {
    channel: EncodedOutput,
    receivers: u64,
    receiver_count_scope: String,
    delivery: String,
}

fn publish_tool(state: Arc<ToolState>, sharded: bool) -> Tool {
    let (name, title, command_name, delivery) = if sharded {
        (
            "redis_spublish",
            "Publish to Redis Shard Channel",
            "SPUBLISH",
            "sharded",
        )
    } else {
        (
            "redis_publish",
            "Publish to Redis Channel",
            "PUBLISH",
            "global",
        )
    };
    ToolBuilder::new(name)
        .title(title)
        .description(if sharded {
            "Publish one binary-safe message through Redis 7+ sharded Pub/Sub. In a cluster the channel is slot-routed; the receiver count is local to the routed node."
        } else {
            "Publish one binary-safe message through global Redis Pub/Sub. In a cluster delivery propagates cluster-wide while the receiver count is local to the routed node."
        })
        .output_schema(output_schema::<PublishOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<PublishInput>| async move {
                let channel = input.channel.decode("channel.value")?;
                let message = input.message.decode("message.value")?;
                validate_bytes(&channel, MAX_CHANNEL_BYTES, "channel")?;
                validate_bytes(&message, MAX_PUBLISH_PAYLOAD_BYTES, "message payload")?;
                let mut redis_command = command(name, AccessMode::ReadWrite, command_name);
                redis_command.arg(channel.clone()).arg(message);
                let receivers = reply_count(
                    state.raw(redis_command, &format!("{command_name} failed")).await?,
                    command_name,
                )?;
                state.output(&PublishOutput {
                    channel: channel.into(),
                    receivers,
                    receiver_count_scope: "executing_node".to_string(),
                    delivery: delivery.to_string(),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ChannelsInput {
    /// Optional binary-safe Redis glob pattern, limited to 4 KiB. Omit it to match every active channel.
    #[serde(default)]
    pattern: Option<EncodedInput>,
    /// Maximum distinct channels accepted in the result. Narrow the pattern when exceeded.
    #[serde(default = "default_channel_limit")]
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
    /// Maximum cluster nodes that may participate in one aggregation.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ChannelsOutput {
    scope: String,
    deployment: String,
    pattern: Option<EncodedOutput>,
    channels: Vec<EncodedOutput>,
    count: usize,
    limit: usize,
    cluster: Option<ClusterAggregation>,
}

fn channels_tool(state: Arc<ToolState>, sharded: bool) -> Tool {
    let (name, title, subcommand, scope) = if sharded {
        (
            "redis_pubsub_shardchannels",
            "List Active Redis Shard Channels",
            "SHARDCHANNELS",
            "sharded",
        )
    } else {
        (
            "redis_pubsub_channels",
            "List Active Redis Pub/Sub Channels",
            "CHANNELS",
            "global",
        )
    };
    ToolBuilder::new(name)
        .title(title)
        .description(if sharded {
            "List a bounded, sorted set of active Redis 7+ shard channels. Cluster results are deduplicated across a bounded all-node fan-out and expose partial node failures."
        } else {
            "List a bounded, sorted set of active global Pub/Sub channels. Cluster results are deduplicated across a bounded all-node fan-out and expose partial node failures."
        })
        .output_schema(output_schema::<ChannelsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<ChannelsInput>| async move {
                state.validate_requested_entries(input.limit, "limit")?;
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let pattern = input
                    .pattern
                    .as_ref()
                    .map(|pattern| pattern.decode("pattern.value"))
                    .transpose()?;
                if let Some(pattern) = &pattern {
                    validate_bytes(pattern, MAX_PATTERN_BYTES, "pattern")?;
                }
                let mut redis_command = command(name, AccessMode::ReadOnly, "PUBSUB");
                redis_command
                    .arg(subcommand)
                    .aggregate_cluster_nodes(input.max_cluster_nodes);
                if let Some(pattern) = &pattern {
                    redis_command.arg(pattern.clone());
                }
                let replies = split_replies(
                    state.raw(redis_command, "PUBSUB channel inspection failed").await?,
                    input.max_cluster_nodes,
                )?;
                let mut channels = BTreeMap::<Vec<u8>, ()>::new();
                for value in replies.values {
                    let values = match value {
                        RedisValue::Array(values) | RedisValue::Set(values) => values,
                        other => {
                            return Err(tower_mcp::Error::tool(format!(
                                "PUBSUB {subcommand} returned an unexpected reply: {other:?}"
                            )));
                        }
                    };
                    for value in values {
                        channels.insert(reply_bytes(value, subcommand)?, ());
                    }
                }
                if channels.len() > input.limit {
                    return Ok(output_limit_result(
                        "collection_entries",
                        channels.len(),
                        input.limit,
                        "Use a narrower Pub/Sub channel pattern or increase limit within the configured output budget.",
                    ));
                }
                let channels = channels
                    .into_keys()
                    .map(EncodedOutput::from)
                    .collect::<Vec<_>>();
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let output = ChannelsOutput {
                    scope: scope.to_string(),
                    deployment: deployment_name(deployment).to_string(),
                    pattern: pattern.map(EncodedOutput::from),
                    count: channels.len(),
                    limit: input.limit,
                    cluster: replies.cluster,
                    channels,
                };
                let output_entries = output.count.saturating_add(
                    output
                        .cluster
                        .as_ref()
                        .map_or(0, |cluster| cluster.failures.len()),
                );
                state.output_collection(
                    &output,
                    output_entries,
                    "Use a narrower Pub/Sub channel pattern or lower limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NumsubInput {
    /// Binary-safe channel names, each limited to 64 KiB. Duplicate decoded names are queried once.
    #[schemars(length(min = 1, max = 1000))]
    channels: Vec<EncodedInput>,
    /// Maximum cluster nodes that may participate in one aggregation.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubscriberCount {
    channel: EncodedOutput,
    subscribers: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NumsubOutput {
    scope: String,
    deployment: String,
    counts: Vec<SubscriberCount>,
    count: usize,
    cluster: Option<ClusterAggregation>,
}

fn count_pairs(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<(Vec<u8>, u64)>> {
    let values = match value {
        RedisValue::Array(values) => values,
        RedisValue::Map(values) => {
            return values
                .into_iter()
                .map(|(channel, count)| {
                    Ok((reply_bytes(channel, context)?, reply_count(count, context)?))
                })
                .collect();
        }
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an unexpected reply: {other:?}"
            )));
        }
    };
    if values.len() % 2 != 0 {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an odd number of channel/count elements"
        )));
    }
    values
        .chunks_exact(2)
        .map(|pair| {
            Ok((
                reply_bytes(pair[0].clone(), context)?,
                reply_count(pair[1].clone(), context)?,
            ))
        })
        .collect()
}

fn numsub_tool(state: Arc<ToolState>, sharded: bool) -> Tool {
    let (name, title, subcommand, scope) = if sharded {
        (
            "redis_pubsub_shardnumsub",
            "Count Redis Shard Channel Subscribers",
            "SHARDNUMSUB",
            "sharded",
        )
    } else {
        (
            "redis_pubsub_numsub",
            "Count Redis Pub/Sub Channel Subscribers",
            "NUMSUB",
            "global",
        )
    };
    ToolBuilder::new(name)
        .title(title)
        .description(if sharded {
            "Count subscribers for a bounded binary-safe list of Redis 7+ shard channels. Cluster counts are summed across a bounded all-node fan-out with partial failures exposed."
        } else {
            "Count exact-match subscribers for a bounded binary-safe channel list. Cluster counts are summed across a bounded all-node fan-out with partial failures exposed."
        })
        .output_schema(output_schema::<NumsubOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<NumsubInput>| async move {
                if input.channels.is_empty() || input.channels.len() > MAX_CHANNEL_ARGUMENTS {
                    return Err(tower_mcp::Error::tool(format!(
                        "channels must contain between 1 and {MAX_CHANNEL_ARGUMENTS} items"
                    )));
                }
                state.validate_requested_entries(input.channels.len(), "channels")?;
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut channels = BTreeMap::<Vec<u8>, ()>::new();
                for (index, channel) in input.channels.iter().enumerate() {
                    let channel = channel.decode(&format!("channels[{index}].value"))?;
                    validate_bytes(&channel, MAX_CHANNEL_BYTES, &format!("channels[{index}]"))?;
                    channels.insert(channel, ());
                }
                let mut redis_command = command(name, AccessMode::ReadOnly, "PUBSUB");
                redis_command
                    .arg(subcommand)
                    .args(channels.keys().cloned())
                    .aggregate_cluster_nodes(input.max_cluster_nodes);
                let replies = split_replies(
                    state.raw(redis_command, "PUBSUB subscriber inspection failed").await?,
                    input.max_cluster_nodes,
                )?;
                let mut counts = BTreeMap::<Vec<u8>, u64>::new();
                for value in replies.values {
                    for (channel, subscribers) in count_pairs(value, subcommand)? {
                        let total = counts.entry(channel).or_default();
                        *total = total.checked_add(subscribers).ok_or_else(|| {
                            tower_mcp::Error::tool(format!(
                                "PUBSUB {subcommand} subscriber total overflowed u64"
                            ))
                        })?;
                    }
                }
                for channel in channels.into_keys() {
                    counts.entry(channel).or_default();
                }
                let counts = counts
                    .into_iter()
                    .map(|(channel, subscribers)| SubscriberCount {
                        channel: channel.into(),
                        subscribers,
                    })
                    .collect::<Vec<_>>();
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let output = NumsubOutput {
                    scope: scope.to_string(),
                    deployment: deployment_name(deployment).to_string(),
                    count: counts.len(),
                    cluster: replies.cluster,
                    counts,
                };
                let output_entries = output.count.saturating_add(
                    output
                        .cluster
                        .as_ref()
                        .map_or(0, |cluster| cluster.failures.len()),
                );
                state.output_collection(
                    &output,
                    output_entries,
                    "Retry PUBSUB subscriber inspection with fewer channels.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NumpatInput {
    /// Maximum cluster nodes that may participate in one aggregation.
    #[serde(default = "default_cluster_node_limit")]
    #[schemars(range(min = 1, max = 256))]
    max_cluster_nodes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct NumpatOutput {
    deployment: String,
    pattern_count: u64,
    count_scope: String,
    cluster: Option<ClusterAggregation>,
}

fn numpat_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pubsub_numpat")
        .title("Count Redis Pub/Sub Patterns")
        .description(
            "Count unique active pattern subscriptions. Cluster counts are summed across a bounded all-node fan-out with partial failures exposed.",
        )
        .output_schema(output_schema::<NumpatOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<NumpatInput>| async move {
                validate_cluster_node_limit(input.max_cluster_nodes)?;
                let mut redis_command = command("redis_pubsub_numpat", AccessMode::ReadOnly, "PUBSUB");
                redis_command
                    .arg("NUMPAT")
                    .aggregate_cluster_nodes(input.max_cluster_nodes);
                let replies = split_replies(
                    state.raw(redis_command, "PUBSUB NUMPAT failed").await?,
                    input.max_cluster_nodes,
                )?;
                let mut pattern_count = 0_u64;
                for value in replies.values {
                    pattern_count = pattern_count
                        .checked_add(reply_count(value, "PUBSUB NUMPAT")?)
                        .ok_or_else(|| {
                            tower_mcp::Error::tool("PUBSUB NUMPAT total overflowed u64")
                        })?;
                }
                let deployment = if replies.cluster.is_some() {
                    RedisDeployment::Cluster
                } else {
                    state.deployment()
                };
                let output = NumpatOutput {
                    deployment: deployment_name(deployment).to_string(),
                    pattern_count,
                    count_scope: if deployment == RedisDeployment::Cluster {
                        "sum_of_node_unique_pattern_counts"
                    } else {
                        "target_unique_patterns"
                    }
                    .to_string(),
                    cluster: replies.cluster,
                };
                let output_entries = output
                    .cluster
                    .as_ref()
                    .map_or(0, |cluster| cluster.failures.len());
                state.output_collection(
                    &output,
                    output_entries,
                    "Retry Pub/Sub pattern inspection after resolving failed cluster nodes.",
                )
            },
        )
        .build()
}
