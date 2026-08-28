//! Owner-isolated, bounded MONITOR streaming session tools.

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolAnnotations, ToolBuilder,
    extract::{Context, Json, State},
};

use super::{ToolState, ValueEncoding, encode_bytes, output_schema};
use crate::{
    AccessMode, MonitorReadRequest, MonitorSessionOptions, MonitorSessionSnapshot,
    RedisSessionError, RedisSessionOwner,
};

const DEFAULT_READ_EVENTS: usize = 100;
const MAX_READ_EVENTS: usize = 1_000;
const DEFAULT_READ_BYTES: usize = 64 * 1024;
const MAX_READ_BYTES: usize = 1024 * 1024;
const DEFAULT_WAIT_MS: u64 = 0;
const MAX_WAIT_MS: u64 = 30_000;

pub(super) fn add_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(start_tool(state.clone()));
    router = router.tool(read_tool(state.clone()));
    router.tool(close_tool(state))
}

fn stateful_annotations(idempotent: bool) -> ToolAnnotations {
    ToolAnnotations {
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: idempotent,
        open_world_hint: true,
        ..ToolAnnotations::default()
    }
}

fn session_error(error: RedisSessionError) -> tower_mcp::Error {
    tower_mcp::Error::tool(format!("{error} [{:?}]", error.kind()))
}

fn request_owner(ctx: &Context) -> tower_mcp::Result<RedisSessionOwner> {
    ctx.extension::<RedisSessionOwner>()
        .cloned()
        .ok_or_else(|| tower_mcp::Error::tool("session request owner is not configured"))
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedOutput {
    value: String,
    encoding: ValueEncoding,
}

impl From<Vec<u8>> for EncodedOutput {
    fn from(bytes: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(bytes);
        Self { value, encoding }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StartInput {
    /// Capture binary-safe argument values instead of omitting them.
    /// Arguments routinely contain application data and credentials, so the
    /// default reports only command names and argument counts.
    #[serde(default)]
    include_arguments: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SessionOutput {
    session_id: String,
    include_arguments: bool,
    buffered_events: usize,
    max_buffered_events: usize,
    max_event_bytes: usize,
    idle_timeout_ms: u64,
}

impl From<MonitorSessionSnapshot> for SessionOutput {
    fn from(snapshot: MonitorSessionSnapshot) -> Self {
        Self {
            session_id: snapshot.session_id,
            include_arguments: snapshot.include_arguments,
            buffered_events: snapshot.buffered_events,
            max_buffered_events: snapshot.max_buffered_events,
            max_event_bytes: snapshot.max_event_bytes,
            idle_timeout_ms: snapshot.idle_timeout.as_millis() as u64,
        }
    }
}

fn start_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_monitor_start")
        .title("Open Redis MONITOR Session")
        .description(
            "Open an owner-isolated MONITOR stream on a dedicated connection. Events are captured into a bounded drop-oldest buffer with client addresses pseudonymized and argument values omitted unless explicitly requested. MONITOR measurably reduces server throughput; sessions are quota-bound, expire when idle, and must be closed explicitly. Standalone targets only.",
        )
        .output_schema(output_schema::<SessionOutput>())
        .annotations(stateful_annotations(false))
        .extractor_handler(
            state,
            |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<StartInput>| async move {
                state.require(AccessMode::Full, "redis_monitor_start")?;
                state.require_tool_capabilities("redis_monitor_start")?;
                let manager = state.monitor_sessions()?;
                let owner = request_owner(&ctx)?;
                let snapshot = manager
                    .start(
                        &owner,
                        MonitorSessionOptions::default().with_arguments(input.include_arguments),
                    )
                    .await
                    .map_err(session_error)?;
                state.output(&SessionOutput::from(snapshot))
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadInput {
    /// Opaque session handle returned by redis_monitor_start.
    session_id: String,
    /// Maximum events removed from the session buffer.
    #[serde(default = "default_read_events")]
    #[schemars(range(min = 1, max = 1000))]
    max_events: usize,
    /// Maximum raw command and argument bytes removed in this read.
    #[serde(default = "default_read_bytes")]
    #[schemars(range(min = 1, max = 1048576))]
    max_bytes: usize,
    /// Finite wait for the first available event, in milliseconds.
    #[serde(default = "default_wait_ms")]
    #[schemars(range(min = 0, max = 30000))]
    wait_ms: u64,
}

fn default_read_events() -> usize {
    DEFAULT_READ_EVENTS
}

fn default_read_bytes() -> usize {
    DEFAULT_READ_BYTES
}

fn default_wait_ms() -> u64 {
    DEFAULT_WAIT_MS
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EventOutput {
    sequence: u64,
    /// Server-reported unix timestamp with microseconds, verbatim.
    timestamp: String,
    database: i64,
    /// Stable per-session client pseudonym; real addresses are never exposed.
    client: String,
    command: EncodedOutput,
    argument_count: usize,
    /// Present only when the session captured argument values.
    arguments: Option<Vec<EncodedOutput>>,
    age_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadOutput {
    session_id: String,
    events: Vec<EventOutput>,
    returned: usize,
    remaining_buffered: usize,
    timed_out: bool,
    dropped_buffer_full_total: u64,
    dropped_oversized_total: u64,
    dropped_unparsed_total: u64,
    /// Whether the dedicated MONITOR connection has ended; buffered events
    /// remain readable until the session is closed or expires.
    disconnected: bool,
}

fn read_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_monitor_read")
        .title("Read Redis MONITOR Events")
        .description(
            "Read and consume a bounded page of redacted command observations from an owner-isolated MONITOR session. The wait is finite and request cancellation stops the read without consuming an event.",
        )
        .output_schema(output_schema::<ReadOutput>())
        .annotations(stateful_annotations(false))
        .extractor_handler(
            state,
            |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<ReadInput>| async move {
                state.require(AccessMode::Full, "redis_monitor_read")?;
                state.require_tool_capabilities("redis_monitor_read")?;
                let manager = state.monitor_sessions()?;
                let owner = request_owner(&ctx)?;
                let result = manager
                    .read(
                        &owner,
                        &input.session_id,
                        MonitorReadRequest::new(
                            input.max_events,
                            input.max_bytes,
                            Duration::from_millis(input.wait_ms),
                        ),
                    )
                    .await
                    .map_err(session_error)?;
                let returned = result.events.len();
                let output = ReadOutput {
                    session_id: input.session_id,
                    events: result
                        .events
                        .into_iter()
                        .map(|event| EventOutput {
                            sequence: event.sequence,
                            timestamp: event.timestamp,
                            database: event.database,
                            client: event.client,
                            command: event.command.into(),
                            argument_count: event.argument_count,
                            arguments: event.arguments.map(|arguments| {
                                arguments.into_iter().map(EncodedOutput::from).collect()
                            }),
                            age_ms: event.age.as_millis() as u64,
                        })
                        .collect(),
                    returned,
                    remaining_buffered: result.remaining_buffered,
                    timed_out: result.timed_out,
                    dropped_buffer_full_total: result.dropped_buffer_full_total,
                    dropped_oversized_total: result.dropped_oversized_total,
                    dropped_unparsed_total: result.dropped_unparsed_total,
                    disconnected: result.disconnected,
                };
                state.output_collection(&output, returned.max(1), "Retry with smaller max_events or max_bytes.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CloseInput {
    /// Opaque session handle returned by redis_monitor_start.
    session_id: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CloseOutput {
    session_id: String,
    closed: bool,
}

fn close_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_monitor_close")
        .title("Close Redis MONITOR Session")
        .description(
            "Close an owner-isolated MONITOR session, ending its dedicated connection and discarding buffered events.",
        )
        .output_schema(output_schema::<CloseOutput>())
        .annotations(stateful_annotations(true))
        .extractor_handler(
            state,
            |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<CloseInput>| async move {
                state.require(AccessMode::Full, "redis_monitor_close")?;
                state.require_tool_capabilities("redis_monitor_close")?;
                let manager = state.monitor_sessions()?;
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

// Compile-time guards: the schema bounds above must not drift silently.
const _: () = {
    assert!(DEFAULT_READ_EVENTS <= MAX_READ_EVENTS);
    assert!(DEFAULT_READ_BYTES <= MAX_READ_BYTES);
    assert!(DEFAULT_WAIT_MS < MAX_WAIT_MS);
};
