//! Durable Redis Streams-backed agent handoff tools.

use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    context::RequestContext,
    extract::{Context, Json, State},
    protocol::{ReadResourceResult, RequestOutcome},
    resource::ResourceTemplateBuilder,
};

use super::{
    ToolState, command, destructive_annotations, output_schema, read_annotations, write_annotations,
};
use crate::{
    AccessMode, CoordinationHandle, CoordinationPayload, CoordinationPrincipal, CoordinationStatus,
    RedisValue,
    coordination::{digest_hex, validate_capability},
};

const GROUP: &str = "redis-mcp-handoffs-v1";
const MAX_IDEMPOTENCY_BYTES: usize = 128;
const MAX_CORRELATION_BYTES: usize = 128;
const MAX_WORKER_BYTES: usize = 64;
const MAX_WAIT_MS: u64 = 5_000;
const DEFAULT_STATUS_EVENTS: usize = 20;
const MAX_STATUS_EVENTS: usize = 100;

const PUBLISH_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[4])
if existing then return {existing, '', 0} end
local group_result = redis.pcall('XGROUP', 'CREATE', KEYS[1], ARGV[1], '0', 'MKSTREAM')
if type(group_result) == 'table' and group_result.err and not string.find(group_result.err, 'BUSYGROUP') then
  return redis.error_reply(group_result.err)
end
local stream_id = redis.call('XADD', KEYS[1], '*',
  'handle', ARGV[2], 'payload', ARGV[3], 'metadata', ARGV[4], 'correlation_id', ARGV[5],
  'deadline_at_ms', ARGV[10], 'traceparent', ARGV[11])
redis.call('HSET', KEYS[2],
  'handle', ARGV[2], 'capability', ARGV[6], 'shard', ARGV[7],
  'status', 'published', 'publisher', ARGV[8], 'stream_id', stream_id,
  'payload', ARGV[3], 'metadata', ARGV[4], 'correlation_id', ARGV[5],
  'published_at_ms', ARGV[9], 'deadline_at_ms', ARGV[10], 'traceparent', ARGV[11],
  'attempts', '0')
redis.call('XADD', KEYS[3], '*', 'event', 'published', 'at_ms', ARGV[9])
redis.call('SET', KEYS[4], ARGV[2])
return {ARGV[2], stream_id, 1}
"#;

const RECORD_CLAIM_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return redis.error_reply('HANDOFF_NOT_FOUND') end
local status = redis.call('HGET', KEYS[1], 'status')
if status == 'completed' then return redis.error_reply('HANDOFF_COMPLETED') end
local stream_id = redis.call('HGET', KEYS[1], 'stream_id')
if stream_id ~= ARGV[5] then return redis.error_reply('HANDOFF_STREAM_MISMATCH') end
redis.call('HSET', KEYS[1], 'status', 'claimed', 'claimed_by', ARGV[1],
  'consumer', ARGV[2], 'claimed_at_ms', ARGV[3])
redis.call('XADD', KEYS[2], '*', 'event', ARGV[4], 'at_ms', ARGV[3])
return redis.call('HINCRBY', KEYS[1], 'attempts', 1)
"#;

const COMPLETE_SCRIPT: &str = r#"
local existing = redis.call('GET', KEYS[4])
if existing then return {1, redis.call('HGET', KEYS[2], 'status')} end
if redis.call('EXISTS', KEYS[2]) == 0 then return redis.error_reply('HANDOFF_NOT_FOUND') end
local status = redis.call('HGET', KEYS[2], 'status')
if status == 'completed' then return redis.error_reply('HANDOFF_ALREADY_COMPLETED') end
local owner = redis.call('HGET', KEYS[2], 'claimed_by')
if owner ~= ARGV[2] then return redis.error_reply('HANDOFF_NOT_OWNED') end
local stream_id = redis.call('HGET', KEYS[2], 'stream_id')
redis.call('HSET', KEYS[2], 'status', 'completed', 'result', ARGV[3],
  'completed_at_ms', ARGV[4])
redis.call('XADD', KEYS[3], '*', 'event', 'completed', 'at_ms', ARGV[4])
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], stream_id)
redis.call('SET', KEYS[4], ARGV[5])
return {acknowledged, 'completed'}
"#;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PublishInput {
    /// Queue/capability required to process this handoff.
    capability: String,
    /// Stable retry key scoped to the authenticated publisher.
    idempotency_key: String,
    payload: CoordinationPayload,
    #[serde(default)]
    metadata: BTreeMap<String, JsonValue>,
    #[serde(default)]
    correlation_id: Option<String>,
    /// Optional absolute Unix epoch deadline in milliseconds.
    #[serde(default)]
    deadline_at_ms: Option<u64>,
    /// Optional W3C traceparent propagated as opaque metadata.
    #[serde(default)]
    traceparent: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PublishOutput {
    handle: CoordinationHandle,
    shard: u16,
    stream_id: Option<String>,
    created: bool,
    resource_uri: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClaimInput {
    capability: String,
    shard: u16,
    /// Stable worker label; authenticated identity remains a separate server extension.
    worker: String,
    /// Finite long-poll duration. Zero performs a non-blocking claim.
    #[serde(default)]
    wait_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClaimOutput {
    claimed: bool,
    handoff: Option<ClaimedHandoff>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClaimedHandoff {
    handle: CoordinationHandle,
    capability: String,
    shard: u16,
    stream_id: String,
    payload: CoordinationPayload,
    metadata: BTreeMap<String, JsonValue>,
    correlation_id: Option<String>,
    deadline_at_ms: Option<u64>,
    traceparent: Option<String>,
    attempt: u64,
    resource_uri: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CompleteInput {
    handle: CoordinationHandle,
    /// Stable retry key scoped to the authenticated claimant.
    idempotency_key: String,
    result: CoordinationPayload,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CompleteOutput {
    handle: CoordinationHandle,
    status: String,
    acknowledged: bool,
    resource_uri: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StatusInput {
    handle: CoordinationHandle,
    #[serde(default = "default_status_events")]
    max_events: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StatusOutput {
    handle: CoordinationHandle,
    capability: String,
    shard: u16,
    status: CoordinationStatus,
    stream_id: String,
    correlation_id: Option<String>,
    payload: CoordinationPayload,
    metadata: BTreeMap<String, JsonValue>,
    result: Option<CoordinationPayload>,
    deadline_at_ms: Option<u64>,
    traceparent: Option<String>,
    attempts: u64,
    caller_is_publisher: bool,
    caller_is_claimant: bool,
    timeline: Vec<TimelineEvent>,
    resource_uri: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TimelineEvent {
    id: String,
    event: String,
    at_ms: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RecoverInput {
    capability: String,
    shard: u16,
    worker: String,
    /// Only entries idle for at least this duration are eligible.
    min_idle_ms: u64,
}

pub(super) fn add_read_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(status_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(publish_tool(state.clone()));
    router = router.tool(claim_tool(state.clone()));
    router.tool(complete_tool(state))
}

pub(super) fn add_full_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(recover_tool(state))
}

pub(super) fn add_resources(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    let template = ResourceTemplateBuilder::new(
        "redis-mcp://coordination/handoffs/{handle}",
    )
    .name("coordination-handoff-status")
    .title("Durable handoff status and timeline")
    .description("Principal-authorized durable state and the latest bounded timeline events for one handoff handle.")
    .mime_type("application/json")
    .argument("handle", Some("Opaque handoff handle returned by a coordination tool"), true)
    .mrtr_handler(move |ctx: RequestContext, uri: String, variables| {
        let state = state.clone();
        async move {
            let principal = ctx
                .extension::<CoordinationPrincipal>()
                .cloned()
                .ok_or_else(|| tower_mcp::Error::tool("durable coordination principal is not configured"))?;
            let handle = variables
                .get("handle")
                .cloned()
                .map(CoordinationHandle::from_stored)
                .ok_or_else(|| tower_mcp::Error::tool("handoff resource omitted its handle"))?;
            let status = read_status(
                &state,
                &principal,
                StatusInput {
                    handle,
                    max_events: DEFAULT_STATUS_EVENTS,
                },
            )
            .await?;
            let body = serde_json::to_string_pretty(&status)?;
            Ok(RequestOutcome::Complete(ReadResourceResult::text(uri, body)))
        }
    });
    router.resource_template(template)
}

fn resource_uri(handle: &CoordinationHandle) -> String {
    format!("redis-mcp://coordination/handoffs/{}", handle.as_str())
}

fn request_principal(ctx: &Context) -> tower_mcp::Result<CoordinationPrincipal> {
    ctx.extension::<CoordinationPrincipal>()
        .cloned()
        .ok_or_else(|| tower_mcp::Error::tool("durable coordination principal is not configured"))
}

fn default_status_events() -> usize {
    DEFAULT_STATUS_EVENTS
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn validate_bounded(name: &str, value: &str, max: usize) -> tower_mcp::Result<()> {
    if value.is_empty() || value.len() > max {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain 1..={max} bytes"
        )))
    } else {
        Ok(())
    }
}

fn validate_queue(state: &ToolState, capability: &str, shard: u16) -> tower_mcp::Result<()> {
    validate_capability(capability).map_err(tower_mcp::Error::tool)?;
    if shard >= state.coordination_config().shards() {
        return Err(tower_mcp::Error::tool(format!(
            "shard must be less than {}",
            state.coordination_config().shards()
        )));
    }
    Ok(())
}

fn worker_consumer(principal: &CoordinationPrincipal, worker: &str) -> tower_mcp::Result<String> {
    validate_bounded("worker", worker, MAX_WORKER_BYTES)?;
    if !worker
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(tower_mcp::Error::tool(
            "worker may contain only ASCII letters, digits, '-' or '_'",
        ));
    }
    Ok(format!(
        "{}:{}",
        &principal.digest()[..16],
        &digest_hex(worker.as_bytes())[..16]
    ))
}

fn publish_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_handoff_publish")
        .title("Publish Durable Redis Handoff")
        .description("Atomically publish one bounded, idempotent handoff into a sharded capability inbox and create its durable state and timeline.")
        .output_schema(output_schema::<PublishOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(state, |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<PublishInput>| async move {
            state.require(AccessMode::ReadWrite, "redis_handoff_publish")?;
            validate_capability(&input.capability).map_err(tower_mcp::Error::tool)?;
            validate_bounded("idempotency_key", &input.idempotency_key, MAX_IDEMPOTENCY_BYTES)?;
            if let Some(correlation) = &input.correlation_id {
                validate_bounded("correlation_id", correlation, MAX_CORRELATION_BYTES)?;
            }
            if let Some(traceparent) = &input.traceparent {
                validate_bounded("traceparent", traceparent, 256)?;
            }
            let published_at_ms = now_ms();
            if input.deadline_at_ms.is_some_and(|deadline| deadline <= published_at_ms) {
                return Err(tower_mcp::Error::tool("deadline_at_ms must be in the future"));
            }
            input.payload.validate(state.coordination_config().max_payload_bytes()).map_err(tower_mcp::Error::tool)?;
            let metadata_json = serde_json::to_string(&input.metadata)?;
            if metadata_json.len() > state.coordination_config().max_metadata_bytes() {
                return Err(tower_mcp::Error::tool(format!("encoded metadata is {} bytes; maximum is {}", metadata_json.len(), state.coordination_config().max_metadata_bytes())));
            }
            let principal = request_principal(&ctx)?;
            let shard = state.coordination_config().shard_for(&input.idempotency_key);
            let handle = CoordinationHandle::create(&input.capability, shard, &principal, &input.idempotency_key).map_err(tower_mcp::Error::tool)?;
            let parsed = handle.parse(state.coordination_config().shards()).map_err(tower_mcp::Error::tool)?;
            let keys = state.coordination_config().keys(&parsed);
            let payload_json = serde_json::to_string(&input.payload)?;
            let mut cmd = command("redis_handoff_publish", AccessMode::ReadWrite, "EVAL");
            cmd.arg(PUBLISH_SCRIPT).arg("4").arg(keys.inbox.as_str()).arg(keys.state.as_str()).arg(keys.events.as_str()).arg(keys.idempotency.as_str())
                .arg(GROUP).arg(handle.as_str()).arg(payload_json).arg(metadata_json)
                .arg(input.correlation_id.unwrap_or_default()).arg(input.capability.as_str()).arg(shard.to_string())
                .arg(principal.digest()).arg(published_at_ms.to_string())
                .arg(input.deadline_at_ms.map(|value| value.to_string()).unwrap_or_default())
                .arg(input.traceparent.unwrap_or_default());
            let values = array(state.raw(cmd, "publishing handoff failed").await?, "publish")?;
            if values.len() != 3 { return Err(tower_mcp::Error::tool("publish returned an invalid reply")); }
            let returned = CoordinationHandle::from_stored(text(values[0].clone(), "publish handle")?);
            let stream_id = text(values[1].clone(), "publish stream ID")?;
            let created = integer(values[2].clone(), "publish created")? == 1;
            let resource_uri = resource_uri(&returned);
            state.output(&PublishOutput { handle: returned, shard, stream_id: (!stream_id.is_empty()).then_some(stream_id), created, resource_uri })
        })
        .build()
}

fn claim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_handoff_claim")
        .title("Claim Durable Redis Handoff")
        .description("Claim at most one new handoff from one explicit capability shard. Poll shards independently so Redis Cluster never needs a cross-slot read.")
        .output_schema(output_schema::<ClaimOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(state, |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<ClaimInput>| async move {
            state.require(AccessMode::ReadWrite, "redis_handoff_claim")?;
            validate_queue(&state, &input.capability, input.shard)?;
            if input.wait_ms > MAX_WAIT_MS
                || std::time::Duration::from_millis(input.wait_ms) >= state.command_timeout()
            {
                return Err(tower_mcp::Error::tool(format!(
                    "wait_ms must be less than both {MAX_WAIT_MS} and the configured command timeout"
                )));
            }
            let principal = request_principal(&ctx)?;
            let consumer = worker_consumer(&principal, &input.worker)?;
            let probe = CoordinationHandle::create(&input.capability, input.shard, &principal, "claim-probe").map_err(tower_mcp::Error::tool)?;
            let parsed = probe.parse(state.coordination_config().shards()).map_err(tower_mcp::Error::tool)?;
            let inbox = state.coordination_config().keys(&parsed).inbox;
            let mut cmd = command("redis_handoff_claim", AccessMode::ReadWrite, "XREADGROUP");
            cmd.arg("GROUP").arg(GROUP).arg(consumer.as_str()).arg("COUNT").arg("1");
            if input.wait_ms > 0 { cmd.arg("BLOCK").arg(input.wait_ms.to_string()); }
            cmd.arg("STREAMS").arg(inbox.as_str()).arg(">");
            let value = state.raw(cmd, "claiming handoff failed").await?;
            let Some(entry) = first_stream_entry(value, "claim")? else {
                return state.output(&ClaimOutput { claimed: false, handoff: None });
            };
            let handoff = record_claim(
                &state,
                &principal,
                &consumer,
                entry,
                &input.capability,
                input.shard,
                false,
            )
            .await?;
            state.output(&ClaimOutput { claimed: true, handoff: Some(handoff) })
        })
        .build()
}

fn complete_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_handoff_complete")
        .title("Complete Durable Redis Handoff")
        .description("Atomically verify durable claimant ownership, store a bounded result, append the timeline, and acknowledge the inbox entry. Safe to retry with the same idempotency key.")
        .output_schema(output_schema::<CompleteOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(state, |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<CompleteInput>| async move {
            state.require(AccessMode::ReadWrite, "redis_handoff_complete")?;
            validate_bounded("idempotency_key", &input.idempotency_key, MAX_IDEMPOTENCY_BYTES)?;
            input.result.validate(state.coordination_config().max_payload_bytes()).map_err(tower_mcp::Error::tool)?;
            let principal = request_principal(&ctx)?;
            let parsed = input.handle.parse(state.coordination_config().shards()).map_err(tower_mcp::Error::tool)?;
            let keys = state.coordination_config().keys(&parsed);
            let completion_key = format!("{}:completion:{}:{}", keys.state, principal.digest(), digest_hex(input.idempotency_key.as_bytes()));
            let result_json = serde_json::to_string(&input.result)?;
            let mut cmd = command("redis_handoff_complete", AccessMode::ReadWrite, "EVAL");
            cmd.arg(COMPLETE_SCRIPT).arg("4").arg(keys.inbox.as_str()).arg(keys.state.as_str()).arg(keys.events.as_str()).arg(completion_key)
                .arg(GROUP).arg(principal.digest()).arg(result_json).arg(now_ms().to_string()).arg(input.handle.as_str());
            let values = array(state.raw(cmd, "completing handoff failed").await?, "complete")?;
            if values.len() != 2 { return Err(tower_mcp::Error::tool("complete returned an invalid reply")); }
            let acknowledged = integer(values[0].clone(), "complete acknowledgement")? == 1;
            let status = text(values[1].clone(), "complete status")?;
            let resource_uri = resource_uri(&input.handle);
            state.output(&CompleteOutput { handle: input.handle, status, acknowledged, resource_uri })
        })
        .build()
}

fn status_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_handoff_status")
        .title("Read Durable Redis Handoff Status")
        .description("Read bounded durable state and timeline for a handoff. Payloads are returned only to its publisher or current claimant.")
        .output_schema(output_schema::<StatusOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<StatusInput>| async move {
            state.require(AccessMode::ReadOnly, "redis_handoff_status")?;
            let principal = request_principal(&ctx)?;
            let status = read_status(&state, &principal, input).await?;
            let entries = status.timeline.len();
            state.output_collection(&status, entries, "Retry status with a smaller max_events value.")
        })
        .build()
}

async fn read_status(
    state: &ToolState,
    principal: &CoordinationPrincipal,
    input: StatusInput,
) -> tower_mcp::Result<StatusOutput> {
    if input.max_events == 0
        || input.max_events > MAX_STATUS_EVENTS
        || input.max_events > state.max_collection_entries()
    {
        return Err(tower_mcp::Error::tool(format!(
            "max_events must be between 1 and {}",
            MAX_STATUS_EVENTS.min(state.max_collection_entries())
        )));
    }
    let parsed = input
        .handle
        .parse(state.coordination_config().shards())
        .map_err(tower_mcp::Error::tool)?;
    let keys = state.coordination_config().keys(&parsed);
    let mut state_cmd = command("redis_handoff_status", AccessMode::ReadOnly, "HGETALL");
    state_cmd.arg(keys.state.as_str());
    let fields = pairs(
        state.raw(state_cmd, "reading handoff state failed").await?,
        "handoff state",
    )?;
    if fields.is_empty() {
        return Err(tower_mcp::Error::tool("handoff was not found"));
    }
    let digest = principal.digest();
    let caller_is_publisher = fields
        .get("publisher")
        .is_some_and(|value| value == &digest);
    let caller_is_claimant = fields
        .get("claimed_by")
        .is_some_and(|value| value == &digest);
    if !caller_is_publisher && !caller_is_claimant {
        return Err(tower_mcp::Error::tool(
            "handoff status is visible only to its publisher or current claimant",
        ));
    }
    let mut timeline_cmd = command("redis_handoff_status", AccessMode::ReadOnly, "XREVRANGE");
    timeline_cmd
        .arg(keys.events.as_str())
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(input.max_events.to_string());
    let mut timeline = parse_timeline(
        state
            .raw(timeline_cmd, "reading handoff timeline failed")
            .await?,
    )?;
    timeline.reverse();
    let payload = json_field::<CoordinationPayload>(&fields, "payload")?;
    let metadata = json_field::<BTreeMap<String, JsonValue>>(&fields, "metadata")?;
    let result = fields
        .get("result")
        .map(|value| serde_json::from_str(value))
        .transpose()
        .map_err(|error| tower_mcp::Error::tool(format!("invalid stored result: {error}")))?;
    let status =
        CoordinationStatus::parse(required(&fields, "status")?).map_err(tower_mcp::Error::tool)?;
    let deadline_at_ms = optional_u64(&fields, "deadline_at_ms")?;
    let attempts = required(&fields, "attempts")?
        .parse()
        .map_err(|_| tower_mcp::Error::tool("invalid stored attempt count"))?;
    let resource_uri = resource_uri(&input.handle);
    Ok(StatusOutput {
        handle: input.handle,
        capability: required(&fields, "capability")?.to_string(),
        shard: required(&fields, "shard")?
            .parse()
            .map_err(|_| tower_mcp::Error::tool("invalid stored shard"))?,
        status,
        stream_id: required(&fields, "stream_id")?.to_string(),
        correlation_id: optional_string(&fields, "correlation_id"),
        payload,
        metadata,
        result,
        deadline_at_ms,
        traceparent: optional_string(&fields, "traceparent"),
        attempts,
        caller_is_publisher,
        caller_is_claimant,
        timeline,
        resource_uri,
    })
}

fn recover_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_handoff_recover")
        .title("Recover Abandoned Redis Handoff")
        .description("Claim at most one pending handoff that exceeded the explicit idle threshold, then durably transfer ownership to the authenticated worker.")
        .output_schema(output_schema::<ClaimOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(state, |ctx: Context, State(state): State<Arc<ToolState>>, Json(input): Json<RecoverInput>| async move {
            state.require(AccessMode::Full, "redis_handoff_recover")?;
            validate_queue(&state, &input.capability, input.shard)?;
            if input.min_idle_ms == 0 { return Err(tower_mcp::Error::tool("min_idle_ms must be greater than zero")); }
            let principal = request_principal(&ctx)?;
            let consumer = worker_consumer(&principal, &input.worker)?;
            let probe = CoordinationHandle::create(&input.capability, input.shard, &principal, "recover-probe").map_err(tower_mcp::Error::tool)?;
            let parsed = probe.parse(state.coordination_config().shards()).map_err(tower_mcp::Error::tool)?;
            let inbox = state.coordination_config().keys(&parsed).inbox;
            let mut cmd = command("redis_handoff_recover", AccessMode::Full, "XAUTOCLAIM");
            cmd.arg(inbox.as_str()).arg(GROUP).arg(consumer.as_str()).arg(input.min_idle_ms.to_string()).arg("0-0").arg("COUNT").arg("1");
            let values = array(state.raw(cmd, "recovering handoff failed").await?, "recover")?;
            if values.len() < 2 { return Err(tower_mcp::Error::tool("recover returned an invalid reply")); }
            let mut entries = array(values[1].clone(), "recover entries")?;
            let Some(value) = entries.drain(..).next() else {
                return state.output(&ClaimOutput { claimed: false, handoff: None });
            };
            let entry = parse_entry(value, "recover")?;
            let handoff = record_claim(
                &state,
                &principal,
                &consumer,
                entry,
                &input.capability,
                input.shard,
                true,
            )
            .await?;
            state.output(&ClaimOutput { claimed: true, handoff: Some(handoff) })
        })
        .build()
}

#[derive(Debug)]
struct RawEntry {
    id: String,
    fields: BTreeMap<String, String>,
}

async fn record_claim(
    state: &ToolState,
    principal: &CoordinationPrincipal,
    consumer: &str,
    entry: RawEntry,
    expected_capability: &str,
    expected_shard: u16,
    recovered: bool,
) -> tower_mcp::Result<ClaimedHandoff> {
    let handle = CoordinationHandle::from_stored(required(&entry.fields, "handle")?.to_string());
    let parsed = handle
        .parse(state.coordination_config().shards())
        .map_err(tower_mcp::Error::tool)?;
    if parsed.capability != expected_capability || parsed.shard != expected_shard {
        return Err(tower_mcp::Error::tool(
            "handoff handle does not belong to the claimed capability shard",
        ));
    }
    let keys = state.coordination_config().keys(&parsed);
    let mut cmd = command(
        if recovered {
            "redis_handoff_recover"
        } else {
            "redis_handoff_claim"
        },
        if recovered {
            AccessMode::Full
        } else {
            AccessMode::ReadWrite
        },
        "EVAL",
    );
    cmd.arg(RECORD_CLAIM_SCRIPT)
        .arg("2")
        .arg(keys.state.as_str())
        .arg(keys.events.as_str())
        .arg(principal.digest())
        .arg(consumer)
        .arg(now_ms().to_string())
        .arg(if recovered { "recovered" } else { "claimed" })
        .arg(entry.id.as_str());
    let attempt = integer(
        state.raw(cmd, "recording handoff claim failed").await?,
        "claim attempt",
    )?
    .try_into()
    .map_err(|_| tower_mcp::Error::tool("claim attempt was negative"))?;
    let resource_uri = resource_uri(&handle);
    Ok(ClaimedHandoff {
        capability: parsed.capability,
        shard: parsed.shard,
        handle,
        stream_id: entry.id,
        payload: serde_json::from_str(required(&entry.fields, "payload")?)
            .map_err(|error| tower_mcp::Error::tool(format!("invalid handoff payload: {error}")))?,
        metadata: serde_json::from_str(required(&entry.fields, "metadata")?).map_err(|error| {
            tower_mcp::Error::tool(format!("invalid handoff metadata: {error}"))
        })?,
        correlation_id: entry
            .fields
            .get("correlation_id")
            .filter(|value| !value.is_empty())
            .cloned(),
        deadline_at_ms: entry
            .fields
            .get("deadline_at_ms")
            .filter(|value| !value.is_empty())
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| tower_mcp::Error::tool("invalid handoff deadline"))?,
        traceparent: entry
            .fields
            .get("traceparent")
            .filter(|value| !value.is_empty())
            .cloned(),
        attempt,
        resource_uri,
    })
}

fn first_stream_entry(value: RedisValue, context: &str) -> tower_mcp::Result<Option<RawEntry>> {
    let batches = match value {
        RedisValue::Nil => return Ok(None),
        RedisValue::Map(values) => values.into_iter().map(|(_, value)| value).collect(),
        RedisValue::Array(values) => values
            .into_iter()
            .map(|value| {
                let pair = array(value, context)?;
                pair.get(1).cloned().ok_or_else(|| {
                    tower_mcp::Error::tool(format!("{context} returned an invalid batch"))
                })
            })
            .collect::<tower_mcp::Result<Vec<_>>>()?,
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an unexpected reply: {other:?}"
            )));
        }
    };
    for batch in batches {
        let entries = array(batch, context)?;
        if let Some(entry) = entries.into_iter().next() {
            return parse_entry(entry, context).map(Some);
        }
    }
    Ok(None)
}

fn parse_entry(value: RedisValue, context: &str) -> tower_mcp::Result<RawEntry> {
    let values = array(value, context)?;
    if values.len() != 2 {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an invalid entry"
        )));
    }
    Ok(RawEntry {
        id: text(values[0].clone(), context)?,
        fields: pairs(values[1].clone(), context)?,
    })
}

fn parse_timeline(value: RedisValue) -> tower_mcp::Result<Vec<TimelineEvent>> {
    array(value, "timeline")?
        .into_iter()
        .map(|entry| {
            let entry = parse_entry(entry, "timeline")?;
            Ok(TimelineEvent {
                id: entry.id,
                event: required(&entry.fields, "event")?.to_string(),
                at_ms: required(&entry.fields, "at_ms")?
                    .parse()
                    .map_err(|_| tower_mcp::Error::tool("invalid timeline timestamp"))?,
            })
        })
        .collect()
}

fn array(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<RedisValue>> {
    match value {
        RedisValue::Array(values) => Ok(values),
        RedisValue::Set(values) => Ok(values),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} expected an array, got {other:?}"
        ))),
    }
}

fn text(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    match value {
        RedisValue::BulkString(value) => String::from_utf8(value)
            .map_err(|_| tower_mcp::Error::tool(format!("{context} returned non-UTF-8 data"))),
        RedisValue::SimpleString(value) => Ok(value),
        RedisValue::Okay => Ok("OK".to_string()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} expected text, got {other:?}"
        ))),
    }
}

fn integer(value: RedisValue, context: &str) -> tower_mcp::Result<i64> {
    match value {
        RedisValue::Integer(value) => Ok(value),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} expected an integer, got {other:?}"
        ))),
    }
}

fn pairs(value: RedisValue, context: &str) -> tower_mcp::Result<BTreeMap<String, String>> {
    let raw = match value {
        RedisValue::Map(values) => values,
        RedisValue::Array(values) => {
            if values.len() % 2 != 0 {
                return Err(tower_mcp::Error::tool(format!(
                    "{context} returned an odd field list"
                )));
            }
            let mut iter = values.into_iter();
            let mut pairs = Vec::new();
            while let Some(key) = iter.next() {
                pairs.push((key, iter.next().expect("length checked")));
            }
            pairs
        }
        RedisValue::Nil => return Ok(BTreeMap::new()),
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} expected fields, got {other:?}"
            )));
        }
    };
    raw.into_iter()
        .map(|(key, value)| Ok((text(key, context)?, text(value, context)?)))
        .collect()
}

fn required<'a>(fields: &'a BTreeMap<String, String>, name: &str) -> tower_mcp::Result<&'a str> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| tower_mcp::Error::tool(format!("stored handoff omitted {name}")))
}

fn optional_string(fields: &BTreeMap<String, String>, name: &str) -> Option<String> {
    fields.get(name).filter(|value| !value.is_empty()).cloned()
}

fn optional_u64(fields: &BTreeMap<String, String>, name: &str) -> tower_mcp::Result<Option<u64>> {
    fields
        .get(name)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse()
                .map_err(|_| tower_mcp::Error::tool(format!("invalid stored {name}")))
        })
        .transpose()
}

fn json_field<T: serde::de::DeserializeOwned>(
    fields: &BTreeMap<String, String>,
    name: &str,
) -> tower_mcp::Result<T> {
    serde_json::from_str(required(fields, name)?)
        .map_err(|error| tower_mcp::Error::tool(format!("invalid stored {name}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_resp2_stream_entry() {
        let value = RedisValue::Array(vec![RedisValue::Array(vec![
            RedisValue::BulkString(b"queue".to_vec()),
            RedisValue::Array(vec![RedisValue::Array(vec![
                RedisValue::BulkString(b"1-0".to_vec()),
                RedisValue::Array(vec![
                    RedisValue::BulkString(b"handle".to_vec()),
                    RedisValue::BulkString(b"h".to_vec()),
                ]),
            ])]),
        ])]);
        let entry = first_stream_entry(value, "test").unwrap().unwrap();
        assert_eq!(entry.id, "1-0");
        assert_eq!(entry.fields["handle"], "h");
    }
}
