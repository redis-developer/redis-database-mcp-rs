//! Finite blocking list/sorted-set operations and replication waits.
//!
//! Every tool issues one bounded blocking call on a dedicated connection
//! through [`crate::RedisBlockingEngine`]; a timeout is an explicit,
//! distinguishable result rather than an error.

use std::{sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, decode_input, destructive_annotations, encode_bytes,
    output_schema, read_annotations,
};
use crate::{
    AccessMode, RedisBlockingMoveAmount, RedisBlockingMoveOrdering, RedisListEnd, RedisSortedSetEnd,
};

const MAX_BLOCKING_KEYS: usize = 16;
const MAX_TIMEOUT_MS: u64 = 30_000;
const MAX_POP_COUNT: usize = 100;

pub(super) fn add_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(pop_list_tool(state.clone(), RedisListEnd::Left));
    router = router.tool(pop_list_tool(state.clone(), RedisListEnd::Right));
    router = router.tool(blmove_tool(state.clone()));
    router = router.tool(blmovem_tool(state.clone()));
    router = router.tool(blmpop_tool(state.clone()));
    router = router.tool(pop_sorted_set_tool(state.clone(), RedisSortedSetEnd::Min));
    router = router.tool(pop_sorted_set_tool(state.clone(), RedisSortedSetEnd::Max));
    router = router.tool(bzmpop_tool(state.clone()));
    router = router.tool(wait_tool(state.clone()));
    router.tool(waitaof_tool(state))
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
    fn from(bytes: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(bytes);
        Self { value, encoding }
    }
}

fn decode_keys(values: &[EncodedInput]) -> tower_mcp::Result<Vec<Vec<u8>>> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| value.decode(&format!("keys[{index}].value")))
        .collect()
}

fn blocking_error(error: crate::RedisError) -> tower_mcp::Error {
    tower_mcp::Error::tool(format!("{error} [{:?}]", error.kind()))
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ListEnd {
    Left,
    Right,
}

impl From<ListEnd> for RedisListEnd {
    fn from(end: ListEnd) -> Self {
        match end {
            ListEnd::Left => Self::Left,
            ListEnd::Right => Self::Right,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum MoveOrdering {
    OneByOne,
    Bulk,
}

impl From<MoveOrdering> for RedisBlockingMoveOrdering {
    fn from(ordering: MoveOrdering) -> Self {
        match ordering {
            MoveOrdering::OneByOne => Self::OneByOne,
            MoveOrdering::Bulk => Self::Bulk,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MoveAmount {
    UpTo {
        count: usize,
        ordering: MoveOrdering,
    },
    Exactly {
        count: usize,
        ordering: MoveOrdering,
    },
}

impl From<MoveAmount> for RedisBlockingMoveAmount {
    fn from(amount: MoveAmount) -> Self {
        match amount {
            MoveAmount::UpTo { count, ordering } => Self::UpTo {
                count,
                ordering: ordering.into(),
            },
            MoveAmount::Exactly { count, ordering } => Self::Exactly {
                count,
                ordering: ordering.into(),
            },
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PopListInput {
    /// Binary-safe keys checked in order; the first non-empty list answers.
    #[schemars(length(min = 1, max = 16))]
    keys: Vec<EncodedInput>,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PoppedElementOutput {
    key: EncodedOutput,
    element: EncodedOutput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PopListOutput {
    /// The popped key and element, absent when the wait timed out.
    popped: Option<PoppedElementOutput>,
    /// Whether the finite server-side wait elapsed without an element.
    timed_out: bool,
}

fn pop_list_tool(state: Arc<ToolState>, end: RedisListEnd) -> Tool {
    let (name, title, description) = match end {
        RedisListEnd::Left => (
            "redis_blpop",
            "Blocking Left Pop From Redis Lists",
            "Pop the head element from the first ready list on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; the popped element is removed and requires full access.",
        ),
        RedisListEnd::Right => (
            "redis_brpop",
            "Blocking Right Pop From Redis Lists",
            "Pop the tail element from the first ready list on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; the popped element is removed and requires full access.",
        ),
    };
    let tool_name = match end {
        RedisListEnd::Left => "redis_blpop",
        RedisListEnd::Right => "redis_brpop",
    };
    ToolBuilder::new(name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<PopListOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<PopListInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                state.require_tool_capabilities(tool_name)?;
                let keys = decode_keys(&input.keys)?;
                let popped = state
                    .blocking()?
                    .pop_list(keys, end, Duration::from_millis(input.timeout_ms))
                    .await
                    .map_err(blocking_error)?;
                state.output(&PopListOutput {
                    timed_out: popped.is_none(),
                    popped: popped.map(|popped| PoppedElementOutput {
                        key: popped.key.into(),
                        element: popped.element.into(),
                    }),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmoveInput {
    source: EncodedInput,
    destination: EncodedInput,
    from: ListEnd,
    to: ListEnd,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmoveOutput {
    /// The moved element, absent when the wait timed out.
    element: Option<EncodedOutput>,
    /// Whether the finite server-side wait elapsed without an element.
    timed_out: bool,
}

fn blmove_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_blmove")
        .title("Blocking Move Between Redis Lists")
        .description("Atomically move one element between same-slot lists on a dedicated connection, waiting a finite, server-capped time for the source to fill. A timeout is an explicit result; requires full access and Redis 6.2.")
        .output_schema(output_schema::<BlmoveOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BlmoveInput>| async move {
                state.require(AccessMode::Full, "redis_blmove")?;
                state.require_tool_capabilities("redis_blmove")?;
                let element = state
                    .blocking()?
                    .move_element(
                        input.source.decode("source")?,
                        input.destination.decode("destination")?,
                        input.from.into(),
                        input.to.into(),
                        Duration::from_millis(input.timeout_ms),
                    )
                    .await
                    .map_err(blocking_error)?;
                state.output(&BlmoveOutput {
                    timed_out: element.is_none(),
                    element: element.map(EncodedOutput::from),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmovemInput {
    source: EncodedInput,
    destination: EncodedInput,
    from: ListEnd,
    to: ListEnd,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
    /// Bounded element count; omitted moves exactly one element.
    #[serde(default)]
    amount: Option<MoveAmount>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmovemOutput {
    /// Moved elements in transfer order, absent when the wait timed out.
    elements: Option<Vec<EncodedOutput>>,
    /// Number of moved elements, zero when the wait timed out.
    moved: usize,
    /// Whether the finite server-side wait elapsed without any element.
    timed_out: bool,
}

fn blmovem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_blmovem")
        .title("Blocking Multi-Move Between Redis Lists")
        .description("Move up to or exactly a bounded number of elements between same-slot lists on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; requires full access and Redis 8.10.")
        .output_schema(output_schema::<BlmovemOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BlmovemInput>| async move {
                state.require(AccessMode::Full, "redis_blmovem")?;
                state.require_tool_capabilities("redis_blmovem")?;
                let elements = state
                    .blocking()?
                    .move_elements(
                        input.source.decode("source")?,
                        input.destination.decode("destination")?,
                        input.from.into(),
                        input.to.into(),
                        Duration::from_millis(input.timeout_ms),
                        input.amount.map(RedisBlockingMoveAmount::from),
                    )
                    .await
                    .map_err(blocking_error)?;
                let moved = elements.as_ref().map_or(0, Vec::len);
                let output = BlmovemOutput {
                    timed_out: elements.is_none(),
                    elements: elements
                        .map(|elements| elements.into_iter().map(EncodedOutput::from).collect()),
                    moved,
                };
                state.output_collection(&output, moved.max(1), "Retry with a smaller amount.count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmpopInput {
    /// Binary-safe keys checked in order; the first non-empty list answers.
    #[schemars(length(min = 1, max = 16))]
    keys: Vec<EncodedInput>,
    /// Which list end to pop from.
    end: ListEnd,
    /// Maximum elements popped from the answering key.
    #[schemars(range(min = 1, max = 100))]
    count: usize,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BlmpopOutput {
    /// The answering key, absent when the wait timed out.
    key: Option<EncodedOutput>,
    /// Popped elements in pop order, absent when the wait timed out.
    elements: Option<Vec<EncodedOutput>>,
    /// Number of popped elements, zero when the wait timed out.
    popped: usize,
    /// Whether the finite server-side wait elapsed without an element.
    timed_out: bool,
}

fn blmpop_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_blmpop")
        .title("Blocking Counted Pop From Redis Lists")
        .description("Pop a bounded batch from the first ready list on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; requires full access and Redis 7.0.")
        .output_schema(output_schema::<BlmpopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BlmpopInput>| async move {
                state.require(AccessMode::Full, "redis_blmpop")?;
                state.require_tool_capabilities("redis_blmpop")?;
                let keys = decode_keys(&input.keys)?;
                let result = state
                    .blocking()?
                    .pop_list_count(
                        keys,
                        input.end.into(),
                        input.count,
                        Duration::from_millis(input.timeout_ms),
                    )
                    .await
                    .map_err(blocking_error)?;
                let popped = result.as_ref().map_or(0, |result| result.elements.len());
                let output = match result {
                    Some(result) => BlmpopOutput {
                        key: Some(result.key.into()),
                        elements: Some(
                            result
                                .elements
                                .into_iter()
                                .map(EncodedOutput::from)
                                .collect(),
                        ),
                        popped,
                        timed_out: false,
                    },
                    None => BlmpopOutput {
                        key: None,
                        elements: None,
                        popped: 0,
                        timed_out: true,
                    },
                };
                state.output_collection(&output, popped.max(1), "Retry with a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PopSortedSetInput {
    /// Binary-safe keys checked in order; the first non-empty set answers.
    #[schemars(length(min = 1, max = 16))]
    keys: Vec<EncodedInput>,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScoredPopOutput {
    key: EncodedOutput,
    member: EncodedOutput,
    /// Exact decimal score string as returned by Redis.
    score: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PopSortedSetOutput {
    /// The popped member, absent when the wait timed out.
    popped: Option<ScoredPopOutput>,
    /// Whether the finite server-side wait elapsed without a member.
    timed_out: bool,
}

fn pop_sorted_set_tool(state: Arc<ToolState>, end: RedisSortedSetEnd) -> Tool {
    let (name, title, description) = match end {
        RedisSortedSetEnd::Min => (
            "redis_bzpopmin",
            "Blocking Minimum Pop From Redis Sorted Sets",
            "Pop the lowest-scored member from the first ready sorted set on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; requires full access.",
        ),
        RedisSortedSetEnd::Max => (
            "redis_bzpopmax",
            "Blocking Maximum Pop From Redis Sorted Sets",
            "Pop the highest-scored member from the first ready sorted set on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; requires full access.",
        ),
    };
    let tool_name = match end {
        RedisSortedSetEnd::Min => "redis_bzpopmin",
        RedisSortedSetEnd::Max => "redis_bzpopmax",
    };
    ToolBuilder::new(name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<PopSortedSetOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<PopSortedSetInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                state.require_tool_capabilities(tool_name)?;
                let keys = decode_keys(&input.keys)?;
                let popped = state
                    .blocking()?
                    .pop_sorted_set(keys, end, Duration::from_millis(input.timeout_ms))
                    .await
                    .map_err(blocking_error)?;
                state.output(&PopSortedSetOutput {
                    timed_out: popped.is_none(),
                    popped: popped.map(|popped| ScoredPopOutput {
                        key: popped.key.into(),
                        member: popped.member.into(),
                        score: popped.score,
                    }),
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SortedSetEnd {
    Min,
    Max,
}

impl From<SortedSetEnd> for RedisSortedSetEnd {
    fn from(end: SortedSetEnd) -> Self {
        match end {
            SortedSetEnd::Min => Self::Min,
            SortedSetEnd::Max => Self::Max,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BzmpopInput {
    /// Binary-safe keys checked in order; the first non-empty set answers.
    #[schemars(length(min = 1, max = 16))]
    keys: Vec<EncodedInput>,
    /// Which score end to pop from.
    end: SortedSetEnd,
    /// Maximum members popped from the answering key.
    #[schemars(range(min = 1, max = 100))]
    count: usize,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScoredMemberOutput {
    member: EncodedOutput,
    /// Exact decimal score string as returned by Redis.
    score: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BzmpopOutput {
    /// The answering key, absent when the wait timed out.
    key: Option<EncodedOutput>,
    /// Popped members in pop order, absent when the wait timed out.
    members: Option<Vec<ScoredMemberOutput>>,
    /// Number of popped members, zero when the wait timed out.
    popped: usize,
    /// Whether the finite server-side wait elapsed without a member.
    timed_out: bool,
}

fn bzmpop_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bzmpop")
        .title("Blocking Counted Pop From Redis Sorted Sets")
        .description("Pop a bounded scored batch from the first ready sorted set on a dedicated connection, waiting a finite, server-capped time. A timeout is an explicit result; requires full access and Redis 7.0.")
        .output_schema(output_schema::<BzmpopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BzmpopInput>| async move {
                state.require(AccessMode::Full, "redis_bzmpop")?;
                state.require_tool_capabilities("redis_bzmpop")?;
                let keys = decode_keys(&input.keys)?;
                let result = state
                    .blocking()?
                    .pop_sorted_set_count(
                        keys,
                        input.end.into(),
                        input.count,
                        Duration::from_millis(input.timeout_ms),
                    )
                    .await
                    .map_err(blocking_error)?;
                let popped = result.as_ref().map_or(0, |result| result.members.len());
                let output = match result {
                    Some(result) => BzmpopOutput {
                        key: Some(result.key.into()),
                        members: Some(
                            result
                                .members
                                .into_iter()
                                .map(|(member, score)| ScoredMemberOutput {
                                    member: member.into(),
                                    score,
                                })
                                .collect(),
                        ),
                        popped,
                        timed_out: false,
                    },
                    None => BzmpopOutput {
                        key: None,
                        members: None,
                        popped: 0,
                        timed_out: true,
                    },
                };
                state.output_collection(&output, popped.max(1), "Retry with a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WaitInput {
    /// Replica acknowledgements to wait for.
    #[schemars(range(min = 0, max = 1024))]
    replicas: usize,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WaitOutput {
    /// Replicas that acknowledged, as reported by Redis.
    acknowledged_replicas: u64,
    /// The requested replica count.
    requested_replicas: usize,
    /// Whether the requested count was reached before the timeout.
    requirement_met: bool,
}

fn wait_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_wait")
        .title("Wait For Redis Replica Acknowledgement")
        .description("Report how many replicas currently acknowledge this dedicated connection's write position, waiting a finite, server-capped time for the requested count. The call runs on a fresh connection with no prior writes, so it measures replica acknowledgement without implying durability for writes issued elsewhere.")
        .output_schema(output_schema::<WaitOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<WaitInput>| async move {
                state.require(AccessMode::Full, "redis_wait")?;
                state.require_tool_capabilities("redis_wait")?;
                let acknowledged = state
                    .blocking()?
                    .wait(input.replicas, Duration::from_millis(input.timeout_ms))
                    .await
                    .map_err(blocking_error)?;
                state.output(&WaitOutput {
                    acknowledged_replicas: acknowledged,
                    requested_replicas: input.replicas,
                    requirement_met: acknowledged >= input.replicas as u64,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WaitAofInput {
    /// Local AOF fsync acknowledgements to wait for (0 or 1).
    #[schemars(range(min = 0, max = 1))]
    local: usize,
    /// Replica AOF fsync acknowledgements to wait for.
    #[schemars(range(min = 0, max = 1024))]
    replicas: usize,
    /// Finite server-side wait in milliseconds. Indefinite blocking is not
    /// supported; the configured engine maximum also applies.
    #[schemars(range(min = 1, max = 30000))]
    timeout_ms: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WaitAofOutput {
    /// Local instances that fsynced the AOF, as reported by Redis.
    acknowledged_local: u64,
    /// Replicas that fsynced the AOF, as reported by Redis.
    acknowledged_replicas: u64,
    /// The requested local count.
    requested_local: usize,
    /// The requested replica count.
    requested_replicas: usize,
    /// Whether both requested counts were reached before the timeout.
    requirement_met: bool,
}

fn waitaof_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_waitaof")
        .title("Wait For Redis AOF Fsync Acknowledgement")
        .description("Report how many local and replica append-only files cover this dedicated connection's write position, waiting a finite, server-capped time for the requested counts. The call runs on a fresh connection with no prior writes, so it measures AOF acknowledgement without implying durability for writes issued elsewhere. Requires Redis 7.2 with AOF enabled.")
        .output_schema(output_schema::<WaitAofOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<WaitAofInput>| async move {
                state.require(AccessMode::Full, "redis_waitaof")?;
                state.require_tool_capabilities("redis_waitaof")?;
                let acknowledged = state
                    .blocking()?
                    .wait_aof(
                        input.local,
                        input.replicas,
                        Duration::from_millis(input.timeout_ms),
                    )
                    .await
                    .map_err(blocking_error)?;
                state.output(&WaitAofOutput {
                    acknowledged_local: acknowledged.local,
                    acknowledged_replicas: acknowledged.replicas,
                    requested_local: input.local,
                    requested_replicas: input.replicas,
                    requirement_met: acknowledged.local >= input.local as u64
                        && acknowledged.replicas >= input.replicas as u64,
                })
            },
        )
        .build()
}

// Compile-time guards: the schema bounds above must match the engine
// defaults so hosts see one consistent contract.
const _: () = {
    assert!(MAX_BLOCKING_KEYS == crate::DEFAULT_MAX_BLOCKING_KEYS);
    assert!(MAX_POP_COUNT == crate::DEFAULT_MAX_BLOCKING_COUNT);
    assert!(MAX_TIMEOUT_MS == crate::DEFAULT_MAX_BLOCKING_TIMEOUT.as_millis() as u64);
};
