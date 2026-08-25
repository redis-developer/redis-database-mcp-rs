//! Owner-isolated, finite Pub/Sub subscription session tools.

use std::{sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolAnnotations, ToolBuilder,
    extract::{Context, Json, State},
};

use super::{InputEncoding, ToolState, ValueEncoding, decode_input, output_schema};
use crate::{
    PubSubMessage, PubSubReadRequest, PubSubSessionError, PubSubSessionOwner,
    PubSubSessionSnapshot, PubSubSubscription, PubSubSubscriptionKind,
};

const DEFAULT_READ_MESSAGES: usize = 100;
const MAX_READ_MESSAGES: usize = 1_000;
const DEFAULT_READ_BYTES: usize = 64 * 1024;
const MAX_READ_BYTES: usize = 1024 * 1024;
const DEFAULT_WAIT_MS: u64 = 0;
const MAX_WAIT_MS: u64 = 30_000;

pub(super) fn add_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(subscribe_tool(
        state.clone(),
        PubSubSubscriptionKind::Channel,
    ));
    router = router.tool(subscribe_tool(
        state.clone(),
        PubSubSubscriptionKind::Pattern,
    ));
    router = router.tool(subscribe_tool(
        state.clone(),
        PubSubSubscriptionKind::Sharded,
    ));
    router = router.tool(read_tool(state.clone()));
    router = router.tool(unsubscribe_tool(state.clone()));
    router.tool(close_tool(state))
}

fn stateful_read_annotations(idempotent: bool) -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: idempotent,
        open_world_hint: true,
        ..ToolAnnotations::default()
    }
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
        let (value, encoding) = encode_session_bytes(value);
        Self { value, encoding }
    }
}

fn encode_session_bytes(bytes: Vec<u8>) -> (String, ValueEncoding) {
    match String::from_utf8(bytes) {
        Ok(value)
            if value
                .chars()
                .all(|character| !character.is_control() && !matches!(character, '"' | '\\')) =>
        {
            (value, ValueEncoding::Utf8)
        }
        Ok(value) => (BASE64.encode(value.as_bytes()), ValueEncoding::Base64),
        Err(error) => (BASE64.encode(error.into_bytes()), ValueEncoding::Base64),
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubscribeInput {
    /// Binary-safe channels or patterns. Duplicate decoded values are subscribed once.
    #[schemars(length(min = 1, max = 100))]
    subscriptions: Vec<EncodedInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubscriptionOutput {
    kind: String,
    value: EncodedOutput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SessionOutput {
    session_id: String,
    subscriptions: Vec<SubscriptionOutput>,
    subscription_count: usize,
    buffered_messages: usize,
    max_buffered_messages: usize,
    max_message_bytes: usize,
    idle_timeout_ms: u64,
}

fn subscribe_tool(state: Arc<ToolState>, kind: PubSubSubscriptionKind) -> Tool {
    let (name, title, description) = match kind {
        PubSubSubscriptionKind::Channel => (
            "redis_subscribe",
            "Open Redis Channel Subscription",
            "Open an owner-isolated session on one or more binary-safe global Pub/Sub channels. The dedicated connection and message buffer are quota-bound and expire when idle.",
        ),
        PubSubSubscriptionKind::Pattern => (
            "redis_psubscribe",
            "Open Redis Pattern Subscription",
            "Open an owner-isolated session on one or more binary-safe Redis glob patterns. The dedicated connection and message buffer are quota-bound and expire when idle.",
        ),
        PubSubSubscriptionKind::Sharded => (
            "redis_ssubscribe",
            "Open Redis Shard Subscription",
            "Open an owner-isolated Redis 7+ sharded Pub/Sub session. Cluster subscriptions route by channel slot; the dedicated connection and message buffer are quota-bound.",
        ),
    };
    ToolBuilder::new(name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<SessionOutput>())
        .annotations(stateful_read_annotations(false))
        .extractor_handler(
            state,
            move |ctx: Context,
                  State(state): State<Arc<ToolState>>,
                  Json(input): Json<SubscribeInput>| async move {
                state.require_tool_capabilities(name)?;
                let subscriptions = decode_subscriptions(&input.subscriptions)?;
                state.validate_requested_entries(subscriptions.len(), "subscriptions")?;
                state.validate_session_binary_output(
                    subscriptions.iter().map(Vec::len).sum(),
                    subscriptions.len(),
                    "subscriptions",
                )?;
                let manager = state.pubsub_sessions()?;
                let owner = request_owner(&ctx)?;
                let snapshot = manager
                    .subscribe(&owner, kind, subscriptions)
                    .await
                    .map_err(session_error)?;
                let output = session_output(snapshot);
                state.output_collection(
                    &output,
                    output.subscription_count,
                    "Retry with fewer Pub/Sub subscriptions.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    /// Opaque session handle returned by a subscribe tool.
    session_id: String,
    /// Maximum messages removed from the session buffer.
    #[serde(default = "default_read_messages")]
    #[schemars(range(min = 1, max = 1000))]
    max_messages: usize,
    /// Maximum raw channel, pattern, and payload bytes removed in this read.
    #[serde(default = "default_read_bytes")]
    #[schemars(range(min = 1, max = 1048576))]
    max_bytes: usize,
    /// Finite wait for the first available message, in milliseconds.
    #[serde(default = "default_wait_ms")]
    #[schemars(range(min = 0, max = 30000))]
    wait_ms: u64,
}

fn default_read_messages() -> usize {
    DEFAULT_READ_MESSAGES
}

fn default_read_bytes() -> usize {
    DEFAULT_READ_BYTES
}

fn default_wait_ms() -> u64 {
    DEFAULT_WAIT_MS
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MessageOutput {
    sequence: u64,
    kind: String,
    channel: EncodedOutput,
    pattern: Option<EncodedOutput>,
    payload: EncodedOutput,
    age_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadOutput {
    session_id: String,
    messages: Vec<MessageOutput>,
    returned: usize,
    remaining_buffered: usize,
    timed_out: bool,
    dropped_buffer_full_total: u64,
    dropped_oversized_total: u64,
}

fn read_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pubsub_read")
        .title("Read Redis Pub/Sub Messages")
        .description(
            "Read and consume a bounded page from an owner-isolated Pub/Sub session. The wait is finite and request cancellation stops the read without consuming a message.",
        )
        .output_schema(output_schema::<ReadOutput>())
        .annotations(stateful_read_annotations(false))
        .extractor_handler(
            state,
            |ctx: Context,
             State(state): State<Arc<ToolState>>,
             Json(input): Json<ReadInput>| async move {
                state.require_tool_capabilities("redis_pubsub_read")?;
                if input.max_messages == 0 || input.max_messages > MAX_READ_MESSAGES {
                    return Err(tower_mcp::Error::tool(format!(
                        "max_messages must be between 1 and {MAX_READ_MESSAGES}"
                    )));
                }
                if input.max_bytes == 0 || input.max_bytes > MAX_READ_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "max_bytes must be between 1 and {MAX_READ_BYTES}"
                    )));
                }
                if input.wait_ms > MAX_WAIT_MS {
                    return Err(tower_mcp::Error::tool(format!(
                        "wait_ms must not exceed {MAX_WAIT_MS}"
                    )));
                }
                state.validate_requested_entries(input.max_messages, "max_messages")?;
                state.validate_session_binary_output(
                    input.max_bytes,
                    input.max_messages,
                    "max_bytes",
                )?;
                let manager = state.pubsub_sessions()?;
                let owner = request_owner(&ctx)?;
                let request = PubSubReadRequest {
                    max_messages: input.max_messages,
                    max_bytes: input.max_bytes,
                    wait: Duration::from_millis(input.wait_ms),
                };
                let result = {
                    let read = manager.read(&owner, &input.session_id, request);
                    tokio::pin!(read);
                    if ctx.is_cancelled() {
                        return Err(tower_mcp::Error::tool("Pub/Sub read was cancelled"));
                    }
                    tokio::select! {
                        biased;
                        result = &mut read => result.map_err(session_error)?,
                        () = ctx.cancelled() => {
                            return Err(tower_mcp::Error::tool("Pub/Sub read was cancelled"));
                        }
                    }
                };
                let messages = result
                    .messages
                    .into_iter()
                    .map(message_output)
                    .collect::<Vec<_>>();
                let output = ReadOutput {
                    session_id: input.session_id,
                    returned: messages.len(),
                    remaining_buffered: result.remaining_buffered,
                    timed_out: result.timed_out,
                    dropped_buffer_full_total: result.dropped_buffer_full_total,
                    dropped_oversized_total: result.dropped_oversized_total,
                    messages,
                };
                state.output_collection(
                    &output,
                    output.returned,
                    "Retry with smaller max_messages or max_bytes values.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SubscriptionKindInput {
    Channel,
    Pattern,
    Sharded,
}

impl From<SubscriptionKindInput> for PubSubSubscriptionKind {
    fn from(value: SubscriptionKindInput) -> Self {
        match value {
            SubscriptionKindInput::Channel => Self::Channel,
            SubscriptionKindInput::Pattern => Self::Pattern,
            SubscriptionKindInput::Sharded => Self::Sharded,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct UnsubscribeInput {
    /// Opaque session handle returned by a subscribe tool.
    session_id: String,
    /// Subscription command family to remove.
    kind: SubscriptionKindInput,
    /// Exact binary-safe subscription values to remove.
    #[schemars(length(min = 1, max = 100))]
    subscriptions: Vec<EncodedInput>,
}

fn unsubscribe_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pubsub_unsubscribe")
        .title("Unsubscribe Redis Pub/Sub Session")
        .description(
            "Remove exact channels or patterns from an owner-isolated session while leaving its dedicated connection open. Close the session explicitly when finished.",
        )
        .output_schema(output_schema::<SessionOutput>())
        .annotations(stateful_read_annotations(true))
        .extractor_handler(
            state,
            |ctx: Context,
             State(state): State<Arc<ToolState>>,
             Json(input): Json<UnsubscribeInput>| async move {
                state.require_tool_capabilities("redis_pubsub_unsubscribe")?;
                let kind = PubSubSubscriptionKind::from(input.kind);
                if kind == PubSubSubscriptionKind::Sharded {
                    state.require_tool_capabilities("redis_ssubscribe")?;
                }
                let subscriptions = decode_subscriptions(&input.subscriptions)?;
                state.validate_requested_entries(subscriptions.len(), "subscriptions")?;
                state.validate_session_binary_output(
                    subscriptions.iter().map(Vec::len).sum(),
                    subscriptions.len(),
                    "subscriptions",
                )?;
                let manager = state.pubsub_sessions()?;
                let owner = request_owner(&ctx)?;
                let snapshot = manager
                    .unsubscribe(
                        &owner,
                        &input.session_id,
                        kind,
                        subscriptions,
                    )
                    .await
                    .map_err(session_error)?;
                let output = session_output(snapshot);
                state.output_collection(
                    &output,
                    output.subscription_count,
                    "Retry with fewer Pub/Sub subscriptions.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CloseInput {
    /// Opaque session handle returned by a subscribe tool.
    session_id: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CloseOutput {
    session_id: String,
    closed: bool,
}

fn close_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_pubsub_close")
        .title("Close Redis Pub/Sub Session")
        .description(
            "Close one owner-isolated Pub/Sub session and release its dedicated Redis connection and buffered messages.",
        )
        .output_schema(output_schema::<CloseOutput>())
        .annotations(stateful_read_annotations(true))
        .extractor_handler(
            state,
            |ctx: Context,
             State(state): State<Arc<ToolState>>,
             Json(input): Json<CloseInput>| async move {
                state.require_tool_capabilities("redis_pubsub_close")?;
                let manager = state.pubsub_sessions()?;
                let owner = request_owner(&ctx)?;
                manager
                    .close(&owner, &input.session_id)
                    .await
                    .map_err(session_error)?;
                state.output(&CloseOutput {
                    session_id: input.session_id,
                    closed: true,
                })
            },
        )
        .build()
}

fn request_owner(ctx: &Context) -> tower_mcp::Result<PubSubSessionOwner> {
    ctx.extension::<PubSubSessionOwner>()
        .cloned()
        .ok_or_else(|| tower_mcp::Error::tool("Pub/Sub request owner is not configured"))
}

fn decode_subscriptions(values: &[EncodedInput]) -> tower_mcp::Result<Vec<Vec<u8>>> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| value.decode(&format!("subscriptions[{index}].value")))
        .collect()
}

fn session_output(snapshot: PubSubSessionSnapshot) -> SessionOutput {
    let subscriptions = snapshot
        .subscriptions
        .into_iter()
        .map(subscription_output)
        .collect::<Vec<_>>();
    SessionOutput {
        session_id: snapshot.session_id,
        subscription_count: subscriptions.len(),
        buffered_messages: snapshot.buffered_messages,
        max_buffered_messages: snapshot.max_buffered_messages,
        max_message_bytes: snapshot.max_message_bytes,
        idle_timeout_ms: duration_millis(snapshot.idle_timeout),
        subscriptions,
    }
}

fn subscription_output(subscription: PubSubSubscription) -> SubscriptionOutput {
    SubscriptionOutput {
        kind: subscription.kind.as_str().to_string(),
        value: subscription.value.into(),
    }
}

fn message_output(message: PubSubMessage) -> MessageOutput {
    MessageOutput {
        sequence: message.sequence,
        kind: message.kind.as_str().to_string(),
        channel: message.channel.into(),
        pattern: message.pattern.map(EncodedOutput::from),
        payload: message.payload.into(),
        age_ms: duration_millis(message.age),
    }
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn session_error(error: PubSubSessionError) -> tower_mcp::Error {
    tower_mcp::Error::tool(format!(
        "Pub/Sub session failed [{:?}]: {error}",
        error.kind()
    ))
}
