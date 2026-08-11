//! Bounded, binary-safe Redis Streams and consumer-group tools.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, KeyInput, ToolState, ValueEncoding, command, decode_input,
    destructive_annotations, encode_bytes, output_schema, read_annotations, write_annotations,
};
use crate::{AccessMode, RedisValue, RedisVersion};

const DEFAULT_COUNT: usize = 100;
const MAX_ARGUMENT_ITEMS: usize = 1_000;
const DEFAULT_RETURNED_BYTES: usize = 64 * 1024;

fn default_count() -> usize {
    DEFAULT_COUNT
}

fn default_returned_bytes() -> usize {
    DEFAULT_RETURNED_BYTES
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamId {
    /// Millisecond timestamp component.
    milliseconds: u64,
    /// Sequence component within the millisecond.
    sequence: u64,
}

impl StreamId {
    fn token(self) -> String {
        format!("{}-{}", self.milliseconds, self.sequence)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RangeBound {
    NegativeInfinity,
    PositiveInfinity,
    Inclusive { id: StreamId },
    Exclusive { id: StreamId },
}

impl RangeBound {
    fn token(self) -> String {
        match self {
            Self::NegativeInfinity => "-".to_string(),
            Self::PositiveInfinity => "+".to_string(),
            Self::Inclusive { id } => id.token(),
            Self::Exclusive { id } => format!("({}", id.token()),
        }
    }
}

fn default_min_bound() -> RangeBound {
    RangeBound::NegativeInfinity
}

fn default_max_bound() -> RangeBound {
    RangeBound::PositiveInfinity
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ReadOffset {
    Latest,
    Explicit { id: StreamId },
}

impl ReadOffset {
    fn token(self) -> String {
        match self {
            Self::Latest => "$".to_string(),
            Self::Explicit { id } => id.token(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GroupReadOffset {
    New,
    Explicit { id: StreamId },
}

impl GroupReadOffset {
    fn token(self) -> String {
        match self {
            Self::New => ">".to_string(),
            Self::Explicit { id } => id.token(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GroupStartId {
    Beginning,
    #[default]
    Latest,
    Explicit {
        id: StreamId,
    },
}

impl GroupStartId {
    fn token(self) -> String {
        match self {
            Self::Beginning => "0-0".to_string(),
            Self::Latest => "$".to_string(),
            Self::Explicit { id } => id.token(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AddId {
    #[default]
    Auto,
    Explicit {
        id: StreamId,
    },
}

impl AddId {
    fn token(self) -> String {
        match self {
            Self::Auto => "*".to_string(),
            Self::Explicit { id } => id.token(),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BinaryInput {
    value: String,
    #[serde(default)]
    encoding: InputEncoding,
}

impl BinaryInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.value, self.encoding, name)
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BinaryOutput {
    value: String,
    encoding: ValueEncoding,
}

impl BinaryOutput {
    fn new(bytes: Vec<u8>) -> Self {
        let (value, encoding) = encode_bytes(bytes);
        Self { value, encoding }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamFieldInput {
    field: String,
    #[serde(default)]
    field_encoding: InputEncoding,
    value: String,
    #[serde(default)]
    value_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamFieldOutput {
    field: String,
    field_encoding: ValueEncoding,
    value: String,
    value_encoding: ValueEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamEntryOutput {
    id: String,
    fields: Vec<StreamFieldOutput>,
    field_count: usize,
    field_bytes: usize,
    fields_omitted: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamBatchOutput {
    key: String,
    key_encoding: ValueEncoding,
    entries: Vec<StreamEntryOutput>,
    count: usize,
    continuation_id: Option<String>,
}

fn decode_binary_name(input: &BinaryInput, name: &str) -> tower_mcp::Result<Vec<u8>> {
    let value = input.decode(name)?;
    if value.is_empty() {
        Err(tower_mcp::Error::tool(format!("{name} must not be empty")))
    } else {
        Ok(value)
    }
}

fn validate_argument_count(count: usize, name: &str) -> tower_mcp::Result<()> {
    if count == 0 || count > MAX_ARGUMENT_ITEMS {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_ARGUMENT_ITEMS} items"
        )))
    } else {
        Ok(())
    }
}

fn validate_block(state: &ToolState, block_ms: Option<u64>) -> tower_mcp::Result<()> {
    let Some(block_ms) = block_ms else {
        return Ok(());
    };
    if block_ms == 0 {
        return Err(tower_mcp::Error::tool(
            "block_ms must be greater than zero; indefinite blocking is not allowed",
        ));
    }
    let block = Duration::from_millis(block_ms);
    let timeout = state.command_timeout();
    if block >= timeout {
        return Err(tower_mcp::Error::tool(format!(
            "block_ms must be below the configured command timeout of {} ms",
            timeout.as_millis()
        )));
    }
    Ok(())
}

fn without_attributes(value: RedisValue) -> RedisValue {
    match value {
        RedisValue::Attribute { data, .. } => without_attributes(*data),
        other => other,
    }
}

fn redis_array(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<RedisValue>> {
    match without_attributes(value) {
        RedisValue::Array(values) | RedisValue::Set(values) => Ok(values),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

fn redis_bytes(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<u8>> {
    match without_attributes(value) {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => Ok(value),
        RedisValue::SimpleString(value) => Ok(value.into_bytes()),
        RedisValue::Okay => Ok(b"OK".to_vec()),
        RedisValue::Integer(value) => Ok(value.to_string().into_bytes()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected scalar: {other:?}"
        ))),
    }
}

fn redis_string(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    String::from_utf8(redis_bytes(value, context)?)
        .map_err(|_| tower_mcp::Error::tool(format!("{context} returned a non-UTF-8 identifier")))
}

fn redis_u64(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    match without_attributes(value) {
        RedisValue::Integer(value) => u64::try_from(value).map_err(|_| {
            tower_mcp::Error::tool(format!("{context} returned a negative integer: {value}"))
        }),
        value => redis_string(value, context)?
            .parse::<u64>()
            .map_err(|_| tower_mcp::Error::tool(format!("{context} returned a non-integer value"))),
    }
}

fn redis_i64(value: RedisValue, context: &str) -> tower_mcp::Result<i64> {
    match without_attributes(value) {
        RedisValue::Integer(value) => Ok(value),
        value => redis_string(value, context)?
            .parse::<i64>()
            .map_err(|_| tower_mcp::Error::tool(format!("{context} returned a non-integer value"))),
    }
}

fn redis_pairs(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<(RedisValue, RedisValue)>> {
    match without_attributes(value) {
        RedisValue::Map(values) => Ok(values),
        RedisValue::Array(values) => {
            if values.len() % 2 != 0 {
                return Err(tower_mcp::Error::tool(format!(
                    "{context} returned an odd-length key/value array"
                )));
            }
            let mut values = values.into_iter();
            let mut pairs = Vec::new();
            while let Some(key) = values.next() {
                pairs.push((key, values.next().expect("even length checked")));
            }
            Ok(pairs)
        }
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected key/value reply: {other:?}"
        ))),
    }
}

fn parse_entry(value: RedisValue, context: &str) -> tower_mcp::Result<StreamEntryOutput> {
    let mut values = redis_array(value, context)?;
    if values.len() != 2 {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned a stream entry with {} elements",
            values.len()
        )));
    }
    let fields = values.pop().expect("length checked");
    let id = redis_string(values.pop().expect("length checked"), context)?;
    let pairs = redis_pairs(fields, context)?;
    let field_count = pairs.len();
    let mut field_bytes = 0usize;
    let fields = pairs
        .into_iter()
        .map(|(field, value)| {
            let field = redis_bytes(field, context)?;
            let value = redis_bytes(value, context)?;
            field_bytes = field_bytes
                .saturating_add(field.len())
                .saturating_add(value.len());
            let (field, field_encoding) = encode_bytes(field);
            let (value, value_encoding) = encode_bytes(value);
            Ok(StreamFieldOutput {
                field,
                field_encoding,
                value,
                value_encoding,
            })
        })
        .collect::<tower_mcp::Result<Vec<_>>>()?;
    Ok(StreamEntryOutput {
        id,
        fields,
        field_count,
        field_bytes,
        fields_omitted: false,
    })
}

fn parse_entries(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<StreamEntryOutput>> {
    redis_array(value, context)?
        .into_iter()
        .map(|value| parse_entry(value, context))
        .collect()
}

fn parse_read_batches(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<StreamBatchOutput>> {
    let values = match without_attributes(value) {
        RedisValue::Nil => return Ok(Vec::new()),
        RedisValue::Map(values) => values,
        RedisValue::Array(values) => values
            .into_iter()
            .map(|value| {
                let mut pair = redis_array(value, context)?;
                if pair.len() != 2 {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an invalid stream batch"
                    )));
                }
                let entries = pair.pop().expect("length checked");
                let key = pair.pop().expect("length checked");
                Ok((key, entries))
            })
            .collect::<tower_mcp::Result<Vec<_>>>()?,
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an unexpected reply: {other:?}"
            )));
        }
    };
    values
        .into_iter()
        .map(|(key, entries)| {
            let key = BinaryOutput::new(redis_bytes(key, context)?);
            let entries = parse_entries(entries, context)?;
            let continuation_id = entries.last().map(|entry| entry.id.clone());
            Ok(StreamBatchOutput {
                key: key.value,
                key_encoding: key.encoding,
                count: entries.len(),
                entries,
                continuation_id,
            })
        })
        .collect()
}

fn parse_info_map(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<BTreeMap<String, RedisValue>> {
    redis_pairs(value, context)?
        .into_iter()
        .map(|(key, value)| Ok((redis_string(key, context)?, value)))
        .collect()
}

fn take_required(
    map: &mut BTreeMap<String, RedisValue>,
    key: &str,
    context: &str,
) -> tower_mcp::Result<RedisValue> {
    map.remove(key)
        .ok_or_else(|| tower_mcp::Error::tool(format!("{context} omitted {key}")))
}

fn take_optional_u64(
    map: &mut BTreeMap<String, RedisValue>,
    key: &str,
    context: &str,
) -> tower_mcp::Result<Option<u64>> {
    match map.remove(key) {
        None | Some(RedisValue::Nil) => Ok(None),
        Some(value) => redis_u64(value, context).map(Some),
    }
}

fn take_optional_i64(
    map: &mut BTreeMap<String, RedisValue>,
    key: &str,
    context: &str,
) -> tower_mcp::Result<Option<i64>> {
    match map.remove(key) {
        None | Some(RedisValue::Nil) => Ok(None),
        Some(value) => redis_i64(value, context).map(Some),
    }
}

fn take_optional_string(
    map: &mut BTreeMap<String, RedisValue>,
    key: &str,
    context: &str,
) -> tower_mcp::Result<Option<String>> {
    match map.remove(key) {
        None | Some(RedisValue::Nil) => Ok(None),
        Some(value) => redis_string(value, context).map(Some),
    }
}

fn stream_result_entries(batches: &[StreamBatchOutput]) -> usize {
    batches.iter().map(|batch| batch.entries.len()).sum()
}

fn stream_result_field_bytes(batches: &[StreamBatchOutput]) -> usize {
    batches
        .iter()
        .flat_map(|batch| &batch.entries)
        .map(|entry| entry.field_bytes)
        .sum()
}

fn omit_large_fields(
    batches: &mut [StreamBatchOutput],
    max_returned_bytes: usize,
) -> (usize, bool) {
    let field_bytes = stream_result_field_bytes(batches);
    let omit = field_bytes > max_returned_bytes;
    if omit {
        for entry in batches.iter_mut().flat_map(|batch| &mut batch.entries) {
            entry.fields.clear();
            entry.fields_omitted = true;
        }
    }
    (field_bytes, omit)
}

fn omit_large_entry_fields(
    entries: &mut [StreamEntryOutput],
    max_returned_bytes: usize,
) -> (usize, bool) {
    let field_bytes = entries.iter().map(|entry| entry.field_bytes).sum();
    let omit = field_bytes > max_returned_bytes;
    if omit {
        for entry in entries {
            entry.fields.clear();
            entry.fields_omitted = true;
        }
    }
    (field_bytes, omit)
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RangeInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default = "default_min_bound")]
    min: RangeBound,
    #[serde(default = "default_max_bound")]
    max: RangeBound,
    #[serde(default = "default_count")]
    count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamPage {
    requested: usize,
    returned: usize,
    complete: bool,
    continuation_id: Option<String>,
    continuation_exclusive: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RangeOutput {
    key: String,
    key_encoding: InputEncoding,
    direction: String,
    entries: Vec<StreamEntryOutput>,
    page: StreamPage,
}

fn range_tool(state: Arc<ToolState>, reverse: bool) -> Tool {
    let (tool_name, command_name, title, direction) = if reverse {
        (
            "redis_xrevrange",
            "XREVRANGE",
            "Read Redis Stream Reverse Range",
            "reverse",
        )
    } else {
        (
            "redis_xrange",
            "XRANGE",
            "Read Redis Stream Range",
            "forward",
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description("Read one bounded page of stream entries with binary-safe fields and an exclusive continuation ID.")
        .output_schema(output_schema::<RangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<RangeInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                if state.redis_version().is_some_and(|version| {
                    version < RedisVersion::new(6, 2, 0)
                        && (matches!(input.min, RangeBound::Exclusive { .. })
                            || matches!(input.max, RangeBound::Exclusive { .. }))
                }) {
                    return Err(tower_mcp::Error::tool(
                        "exclusive stream range bounds require Redis 6.2 or newer",
                    ));
                }
                let fetch = input.count.saturating_add(1);
                let mut cmd = command(tool_name, AccessMode::ReadOnly, command_name);
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if reverse {
                    cmd.arg(input.max.token()).arg(input.min.token());
                } else {
                    cmd.arg(input.min.token()).arg(input.max.token());
                }
                cmd.arg("COUNT").arg(fetch.to_string());
                let mut entries = parse_entries(state.raw(cmd, "stream range failed").await?, "stream range")?;
                let complete = entries.len() <= input.count;
                if !complete {
                    entries.pop();
                }
                let continuation_id = (!complete)
                    .then(|| entries.last().map(|entry| entry.id.clone()))
                    .flatten();
                let output = RangeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    direction: direction.to_string(),
                    page: StreamPage {
                        requested: input.count,
                        returned: entries.len(),
                        complete,
                        continuation_id,
                        continuation_exclusive: true,
                    },
                    entries,
                };
                state.output_collection(
                    &output,
                    output.entries.len(),
                    "Retry with a smaller count and the exclusive continuation ID.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XlenOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    length: u64,
}

fn xlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xlen")
        .title("Redis Stream Length")
        .description("Read a stream length and distinguish a missing key from an empty stream.")
        .output_schema(output_schema::<XlenOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut cmd = command("redis_xlen", AccessMode::ReadOnly, "XLEN");
                cmd.arg(key.clone());
                let length = state.query(cmd, "XLEN failed").await?;
                let exists = if length == 0 {
                    let mut exists = command("redis_xlen", AccessMode::ReadOnly, "EXISTS");
                    exists.arg(key);
                    state
                        .query::<u64>(exists, "EXISTS after XLEN failed")
                        .await?
                        != 0
                } else {
                    true
                };
                state.output(&XlenOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    length,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadTarget {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    offset: ReadOffset,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XreadInput {
    streams: Vec<ReadTarget>,
    #[serde(default = "default_count")]
    count: usize,
    #[serde(default)]
    block_ms: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XreadOutput {
    timed_out: bool,
    count: usize,
    streams: Vec<StreamBatchOutput>,
}

fn xread_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xread")
        .title("Read Redis Streams")
        .description("Read one finite, input-bounded page from one or more streams. Optional blocking must finish before the library command timeout.")
        .output_schema(output_schema::<XreadOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XreadInput>| async move {
                validate_argument_count(input.streams.len(), "streams")?;
                let total_limit = input.count.checked_mul(input.streams.len()).ok_or_else(|| tower_mcp::Error::tool("count times streams is too large"))?;
                state.validate_requested_entries(total_limit, "count times streams")?;
                validate_block(&state, input.block_ms)?;
                let mut cmd = command("redis_xread", AccessMode::ReadOnly, "XREAD");
                cmd.arg("COUNT").arg(input.count.to_string());
                if let Some(block_ms) = input.block_ms {
                    cmd.arg("BLOCK").arg(block_ms.to_string());
                }
                cmd.arg("STREAMS");
                for stream in &input.streams {
                    cmd.arg(decode_input(&stream.key, stream.key_encoding, "streams[].key")?);
                }
                for stream in &input.streams {
                    cmd.arg(stream.offset.token());
                }
                let value = state.raw(cmd, "XREAD failed").await?;
                let timed_out = matches!(without_attributes(value.clone()), RedisValue::Nil);
                let streams = parse_read_batches(value, "XREAD")?;
                let count = stream_result_entries(&streams);
                state.output_collection(
                    &XreadOutput { timed_out, count, streams },
                    count,
                    "Retry XREAD with fewer streams or a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XinfoStreamOutput {
    key: String,
    key_encoding: InputEncoding,
    length: u64,
    radix_tree_keys: u64,
    radix_tree_nodes: u64,
    last_generated_id: String,
    max_deleted_entry_id: Option<String>,
    entries_added: Option<u64>,
    recorded_first_entry_id: Option<String>,
    groups: u64,
    first_entry: Option<StreamEntryOutput>,
    last_entry: Option<StreamEntryOutput>,
}

fn optional_entry(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Option<StreamEntryOutput>> {
    match without_attributes(value) {
        RedisValue::Nil => Ok(None),
        value => parse_entry(value, context).map(Some),
    }
}

fn xinfo_stream_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xinfo_stream")
        .title("Inspect Redis Stream")
        .description(
            "Read structured stream metadata, including binary-safe first and last entries.",
        )
        .output_schema(output_schema::<XinfoStreamOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command("redis_xinfo_stream", AccessMode::ReadOnly, "XINFO");
                cmd.arg("STREAM")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?);
                let mut map =
                    parse_info_map(state.raw(cmd, "XINFO STREAM failed").await?, "XINFO STREAM")?;
                let output = XinfoStreamOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    length: redis_u64(
                        take_required(&mut map, "length", "XINFO STREAM")?,
                        "XINFO STREAM length",
                    )?,
                    radix_tree_keys: redis_u64(
                        take_required(&mut map, "radix-tree-keys", "XINFO STREAM")?,
                        "XINFO STREAM radix-tree-keys",
                    )?,
                    radix_tree_nodes: redis_u64(
                        take_required(&mut map, "radix-tree-nodes", "XINFO STREAM")?,
                        "XINFO STREAM radix-tree-nodes",
                    )?,
                    last_generated_id: redis_string(
                        take_required(&mut map, "last-generated-id", "XINFO STREAM")?,
                        "XINFO STREAM last-generated-id",
                    )?,
                    max_deleted_entry_id: take_optional_string(
                        &mut map,
                        "max-deleted-entry-id",
                        "XINFO STREAM",
                    )?,
                    entries_added: take_optional_u64(&mut map, "entries-added", "XINFO STREAM")?,
                    recorded_first_entry_id: take_optional_string(
                        &mut map,
                        "recorded-first-entry-id",
                        "XINFO STREAM",
                    )?,
                    groups: redis_u64(
                        take_required(&mut map, "groups", "XINFO STREAM")?,
                        "XINFO STREAM groups",
                    )?,
                    first_entry: optional_entry(
                        take_required(&mut map, "first-entry", "XINFO STREAM")?,
                        "XINFO STREAM first-entry",
                    )?,
                    last_entry: optional_entry(
                        take_required(&mut map, "last-entry", "XINFO STREAM")?,
                        "XINFO STREAM last-entry",
                    )?,
                };
                state.output(&output)
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GroupInfoOutput {
    name: String,
    name_encoding: ValueEncoding,
    consumers: u64,
    pending: u64,
    last_delivered_id: String,
    entries_read: Option<u64>,
    lag: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XinfoGroupsOutput {
    key: String,
    key_encoding: InputEncoding,
    count: usize,
    groups: Vec<GroupInfoOutput>,
}

fn parse_group_info(value: RedisValue) -> tower_mcp::Result<GroupInfoOutput> {
    let mut map = parse_info_map(value, "XINFO GROUPS")?;
    let name = BinaryOutput::new(redis_bytes(
        take_required(&mut map, "name", "XINFO GROUPS")?,
        "XINFO GROUPS name",
    )?);
    Ok(GroupInfoOutput {
        name: name.value,
        name_encoding: name.encoding,
        consumers: redis_u64(
            take_required(&mut map, "consumers", "XINFO GROUPS")?,
            "XINFO GROUPS consumers",
        )?,
        pending: redis_u64(
            take_required(&mut map, "pending", "XINFO GROUPS")?,
            "XINFO GROUPS pending",
        )?,
        last_delivered_id: redis_string(
            take_required(&mut map, "last-delivered-id", "XINFO GROUPS")?,
            "XINFO GROUPS last-delivered-id",
        )?,
        entries_read: take_optional_u64(&mut map, "entries-read", "XINFO GROUPS")?,
        lag: take_optional_u64(&mut map, "lag", "XINFO GROUPS")?,
    })
}

fn xinfo_groups_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xinfo_groups")
        .title("Inspect Redis Stream Groups")
        .description("Read structured consumer-group metadata for a stream.")
        .output_schema(output_schema::<XinfoGroupsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command("redis_xinfo_groups", AccessMode::ReadOnly, "XINFO");
                cmd.arg("GROUPS")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?);
                let groups =
                    redis_array(state.raw(cmd, "XINFO GROUPS failed").await?, "XINFO GROUPS")?
                        .into_iter()
                        .map(parse_group_info)
                        .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = XinfoGroupsOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    count: groups.len(),
                    groups,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "A stream has more groups than the configured output limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GroupKeyInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConsumerInfoOutput {
    name: String,
    name_encoding: ValueEncoding,
    pending: u64,
    idle_ms: u64,
    inactive_ms: Option<i64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XinfoConsumersOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    count: usize,
    consumers: Vec<ConsumerInfoOutput>,
}

fn parse_consumer_info(value: RedisValue) -> tower_mcp::Result<ConsumerInfoOutput> {
    let mut map = parse_info_map(value, "XINFO CONSUMERS")?;
    let name = BinaryOutput::new(redis_bytes(
        take_required(&mut map, "name", "XINFO CONSUMERS")?,
        "XINFO CONSUMERS name",
    )?);
    Ok(ConsumerInfoOutput {
        name: name.value,
        name_encoding: name.encoding,
        pending: redis_u64(
            take_required(&mut map, "pending", "XINFO CONSUMERS")?,
            "XINFO CONSUMERS pending",
        )?,
        idle_ms: redis_u64(
            take_required(&mut map, "idle", "XINFO CONSUMERS")?,
            "XINFO CONSUMERS idle",
        )?,
        inactive_ms: take_optional_i64(&mut map, "inactive", "XINFO CONSUMERS")?,
    })
}

fn xinfo_consumers_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xinfo_consumers")
        .title("Inspect Redis Stream Consumers")
        .description("Read structured consumer metadata for one stream group.")
        .output_schema(output_schema::<XinfoConsumersOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GroupKeyInput>| async move {
                let mut cmd = command("redis_xinfo_consumers", AccessMode::ReadOnly, "XINFO");
                cmd.arg("CONSUMERS")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?);
                let consumers = redis_array(
                    state.raw(cmd, "XINFO CONSUMERS failed").await?,
                    "XINFO CONSUMERS",
                )?
                .into_iter()
                .map(parse_consumer_info)
                .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = XinfoConsumersOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    group: input.group.value,
                    group_encoding: input.group.encoding,
                    count: consumers.len(),
                    consumers,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "A group has more consumers than the configured output limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PendingQuery {
    Summary,
    Entries {
        #[serde(default = "default_min_bound")]
        start: RangeBound,
        #[serde(default = "default_max_bound")]
        end: RangeBound,
        #[serde(default = "default_count")]
        count: usize,
    },
}

impl Default for PendingQuery {
    fn default() -> Self {
        Self::Summary
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XpendingInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    #[serde(default)]
    query: PendingQuery,
    #[serde(default)]
    consumer: Option<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PendingConsumerOutput {
    name: String,
    name_encoding: ValueEncoding,
    count: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PendingSummaryOutput {
    count: u64,
    smallest_id: Option<String>,
    greatest_id: Option<String>,
    consumers: Vec<PendingConsumerOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PendingEntryOutput {
    id: String,
    consumer: String,
    consumer_encoding: ValueEncoding,
    idle_ms: u64,
    deliveries: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XpendingOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    summary: Option<PendingSummaryOutput>,
    entries: Vec<PendingEntryOutput>,
    count: usize,
}

fn parse_nullable_id(value: RedisValue, context: &str) -> tower_mcp::Result<Option<String>> {
    match without_attributes(value) {
        RedisValue::Nil => Ok(None),
        value => redis_string(value, context).map(Some),
    }
}

fn parse_pending_summary(value: RedisValue) -> tower_mcp::Result<PendingSummaryOutput> {
    let mut values = redis_array(value, "XPENDING summary")?;
    if values.len() != 4 {
        return Err(tower_mcp::Error::tool(
            "XPENDING summary returned an invalid reply",
        ));
    }
    let consumers = redis_array(values.pop().expect("length checked"), "XPENDING consumers")?
        .into_iter()
        .map(|value| {
            let mut pair = redis_array(value, "XPENDING consumer")?;
            if pair.len() != 2 {
                return Err(tower_mcp::Error::tool(
                    "XPENDING consumer returned an invalid reply",
                ));
            }
            let count = redis_u64(
                pair.pop().expect("length checked"),
                "XPENDING consumer count",
            )?;
            let name = BinaryOutput::new(redis_bytes(
                pair.pop().expect("length checked"),
                "XPENDING consumer name",
            )?);
            Ok(PendingConsumerOutput {
                name: name.value,
                name_encoding: name.encoding,
                count,
            })
        })
        .collect::<tower_mcp::Result<Vec<_>>>()?;
    let greatest_id = parse_nullable_id(
        values.pop().expect("length checked"),
        "XPENDING greatest ID",
    )?;
    let smallest_id = parse_nullable_id(
        values.pop().expect("length checked"),
        "XPENDING smallest ID",
    )?;
    let count = redis_u64(values.pop().expect("length checked"), "XPENDING count")?;
    Ok(PendingSummaryOutput {
        count,
        smallest_id,
        greatest_id,
        consumers,
    })
}

fn parse_pending_entries(value: RedisValue) -> tower_mcp::Result<Vec<PendingEntryOutput>> {
    redis_array(value, "XPENDING entries")?
        .into_iter()
        .map(|value| {
            let mut values = redis_array(value, "XPENDING entry")?;
            if values.len() != 4 {
                return Err(tower_mcp::Error::tool(
                    "XPENDING entry returned an invalid reply",
                ));
            }
            let deliveries =
                redis_u64(values.pop().expect("length checked"), "XPENDING deliveries")?;
            let idle_ms = redis_u64(values.pop().expect("length checked"), "XPENDING idle")?;
            let consumer = BinaryOutput::new(redis_bytes(
                values.pop().expect("length checked"),
                "XPENDING consumer",
            )?);
            let id = redis_string(values.pop().expect("length checked"), "XPENDING ID")?;
            Ok(PendingEntryOutput {
                id,
                consumer: consumer.value,
                consumer_encoding: consumer.encoding,
                idle_ms,
                deliveries,
            })
        })
        .collect()
}

fn xpending_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xpending")
        .title("Inspect Redis Stream Pending Entries")
        .description("Read either the fixed XPENDING summary or one bounded pending-entry range.")
        .output_schema(output_schema::<XpendingOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XpendingInput>| async move {
                let mut cmd = command("redis_xpending", AccessMode::ReadOnly, "XPENDING");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?);
                let (summary, entries, count) = match input.query {
                    PendingQuery::Summary => {
                        if input.consumer.is_some() { return Err(tower_mcp::Error::tool("consumer is only valid for an entries query")); }
                        let summary = parse_pending_summary(state.raw(cmd, "XPENDING summary failed").await?)?;
                        let count = summary.consumers.len();
                        (Some(summary), Vec::new(), count)
                    }
                    PendingQuery::Entries { start, end, count } => {
                        state.validate_requested_entries(count, "query.count")?;
                        cmd.arg(start.token()).arg(end.token()).arg(count.to_string());
                        if let Some(consumer) = &input.consumer { cmd.arg(decode_binary_name(consumer, "consumer")?); }
                        let entries = parse_pending_entries(state.raw(cmd, "XPENDING entries failed").await?)?;
                        let count = entries.len();
                        (None, entries, count)
                    }
                };
                let output = XpendingOutput {
                    key: input.key, key_encoding: input.key_encoding,
                    group: input.group.value, group_encoding: input.group.encoding,
                    summary, entries, count,
                };
                state.output_collection(&output, output.count, "Retry XPENDING with a smaller count and the last returned ID as an exclusive start.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StreamTrim {
    MaxLen {
        threshold: u64,
        #[serde(default)]
        approximate: bool,
        #[serde(default)]
        limit: Option<u64>,
    },
    MinId {
        threshold: StreamId,
        #[serde(default)]
        approximate: bool,
        #[serde(default)]
        limit: Option<u64>,
    },
}

fn add_trim_arguments(
    cmd: &mut crate::RedisCommand,
    trim: &StreamTrim,
    redis_version: Option<RedisVersion>,
) -> tower_mcp::Result<()> {
    let (strategy, threshold, approximate, limit) = match trim {
        StreamTrim::MaxLen {
            threshold,
            approximate,
            limit,
        } => ("MAXLEN", threshold.to_string(), *approximate, *limit),
        StreamTrim::MinId {
            threshold,
            approximate,
            limit,
        } => ("MINID", threshold.token(), *approximate, *limit),
    };
    if limit.is_some() && !approximate {
        return Err(tower_mcp::Error::tool(
            "trim.limit is only valid with approximate=true",
        ));
    }
    let before_62 = redis_version.is_some_and(|version| version < RedisVersion::new(6, 2, 0));
    if before_62 && (matches!(trim, StreamTrim::MinId { .. }) || limit.is_some()) {
        return Err(tower_mcp::Error::tool(
            "MINID trimming and trim LIMIT require Redis 6.2 or newer",
        ));
    }
    cmd.arg(strategy);
    if approximate {
        cmd.arg("~");
    } else if !before_62 {
        cmd.arg("=");
    }
    cmd.arg(threshold);
    if let Some(limit) = limit {
        cmd.arg("LIMIT").arg(limit.to_string());
    }
    Ok(())
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XaddInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    id: AddId,
    fields: Vec<StreamFieldInput>,
    #[serde(default)]
    no_mkstream: bool,
    #[serde(default)]
    trim: Option<StreamTrim>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XaddOutput {
    key: String,
    key_encoding: InputEncoding,
    added: bool,
    id: Option<String>,
    field_count: usize,
}

fn xadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xadd")
        .title("Add Redis Stream Entry")
        .description("Append one binary-safe stream entry, optionally using bounded MAXLEN or MINID trimming.")
        .output_schema(output_schema::<XaddOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xadd")?;
                validate_argument_count(input.fields.len(), "fields")?;
                let mut cmd = command("redis_xadd", AccessMode::ReadWrite, "XADD");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if input.no_mkstream {
                    if state.redis_version().is_some_and(|version| version < RedisVersion::new(6, 2, 0)) {
                        return Err(tower_mcp::Error::tool("NOMKSTREAM requires Redis 6.2 or newer"));
                    }
                    cmd.arg("NOMKSTREAM");
                }
                if let Some(trim) = &input.trim { add_trim_arguments(&mut cmd, trim, state.redis_version())?; }
                cmd.arg(input.id.token());
                for field in &input.fields {
                    cmd.arg(decode_input(&field.field, field.field_encoding, "fields[].field")?)
                        .arg(decode_input(&field.value, field.value_encoding, "fields[].value")?);
                }
                let value = without_attributes(state.raw(cmd, "XADD failed").await?);
                let id = match value {
                    RedisValue::Nil => None,
                    value => Some(redis_string(value, "XADD ID")?),
                };
                state.output(&XaddOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    added: id.is_some(),
                    id,
                    field_count: input.fields.len(),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XgroupCreateInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    #[serde(default)]
    id: GroupStartId,
    #[serde(default)]
    mkstream: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GroupMutationOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    applied: bool,
}

fn xgroup_create_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xgroup_create")
        .title("Create Redis Stream Group")
        .description("Create a consumer group at an explicit start position, optionally creating the stream.")
        .output_schema(output_schema::<GroupMutationOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XgroupCreateInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xgroup_create")?;
                let mut cmd = command("redis_xgroup_create", AccessMode::ReadWrite, "XGROUP");
                cmd.arg("CREATE")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(input.id.token());
                if input.mkstream { cmd.arg("MKSTREAM"); }
                let reply = redis_string(state.raw(cmd, "XGROUP CREATE failed").await?, "XGROUP CREATE")?;
                state.output(&GroupMutationOutput {
                    key: input.key, key_encoding: input.key_encoding,
                    group: input.group.value, group_encoding: input.group.encoding,
                    applied: reply == "OK",
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XgroupSetidInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    id: GroupStartId,
}

fn xgroup_setid_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xgroup_setid")
        .title("Set Redis Stream Group ID")
        .description("Set the last-delivered position for an existing consumer group.")
        .output_schema(output_schema::<GroupMutationOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XgroupSetidInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xgroup_setid")?;
                let mut cmd = command("redis_xgroup_setid", AccessMode::ReadWrite, "XGROUP");
                cmd.arg("SETID")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(input.id.token());
                let reply =
                    redis_string(state.raw(cmd, "XGROUP SETID failed").await?, "XGROUP SETID")?;
                state.output(&GroupMutationOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    group: input.group.value,
                    group_encoding: input.group.encoding,
                    applied: reply == "OK",
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConsumerKeyInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    consumer: BinaryInput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ConsumerMutationOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    consumer: String,
    consumer_encoding: InputEncoding,
    created: bool,
}

fn xgroup_createconsumer_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xgroup_createconsumer")
        .title("Create Redis Stream Consumer")
        .description("Explicitly create one consumer in an existing stream group.")
        .output_schema(output_schema::<ConsumerMutationOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ConsumerKeyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xgroup_createconsumer")?;
                let mut cmd = command(
                    "redis_xgroup_createconsumer",
                    AccessMode::ReadWrite,
                    "XGROUP",
                );
                cmd.arg("CREATECONSUMER")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(decode_binary_name(&input.consumer, "consumer")?);
                let created = state
                    .query::<u64>(cmd, "XGROUP CREATECONSUMER failed")
                    .await?
                    != 0;
                state.output(&ConsumerMutationOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    group: input.group.value,
                    group_encoding: input.group.encoding,
                    consumer: input.consumer.value,
                    consumer_encoding: input.consumer.encoding,
                    created,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GroupReadTarget {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    offset: GroupReadOffset,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XreadgroupInput {
    group: BinaryInput,
    consumer: BinaryInput,
    streams: Vec<GroupReadTarget>,
    #[serde(default = "default_count")]
    count: usize,
    #[serde(default)]
    block_ms: Option<u64>,
    #[serde(default)]
    no_ack: bool,
    #[serde(default = "default_returned_bytes")]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XreadgroupOutput {
    group: String,
    group_encoding: InputEncoding,
    consumer: String,
    consumer_encoding: InputEncoding,
    timed_out: bool,
    count: usize,
    streams: Vec<StreamBatchOutput>,
    field_bytes: usize,
    fields_omitted: bool,
}

fn xreadgroup_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xreadgroup")
        .title("Read Redis Stream Group")
        .description("Read one finite consumer-group page. Large field payloads are omitted after success while entry IDs remain visible.")
        .output_schema(output_schema::<XreadgroupOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XreadgroupInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xreadgroup")?;
                validate_argument_count(input.streams.len(), "streams")?;
                let total_limit = input.count.checked_mul(input.streams.len()).ok_or_else(|| tower_mcp::Error::tool("count times streams is too large"))?;
                state.validate_requested_entries(total_limit, "count times streams")?;
                validate_block(&state, input.block_ms)?;
                if input.max_returned_bytes == 0 { return Err(tower_mcp::Error::tool("max_returned_bytes must be greater than zero")); }
                let max_returned_bytes = input.max_returned_bytes.min(state.max_output_bytes());
                let mut cmd = command("redis_xreadgroup", AccessMode::ReadWrite, "XREADGROUP");
                cmd.arg("GROUP")
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(decode_binary_name(&input.consumer, "consumer")?)
                    .arg("COUNT").arg(input.count.to_string());
                if let Some(block_ms) = input.block_ms { cmd.arg("BLOCK").arg(block_ms.to_string()); }
                if input.no_ack { cmd.arg("NOACK"); }
                cmd.arg("STREAMS");
                for stream in &input.streams { cmd.arg(decode_input(&stream.key, stream.key_encoding, "streams[].key")?); }
                for stream in &input.streams { cmd.arg(stream.offset.token()); }
                let value = state.raw(cmd, "XREADGROUP failed").await?;
                let timed_out = matches!(without_attributes(value.clone()), RedisValue::Nil);
                let mut streams = parse_read_batches(value, "XREADGROUP")?;
                let count = stream_result_entries(&streams);
                let (field_bytes, fields_omitted) = omit_large_fields(&mut streams, max_returned_bytes);
                state.output_collection(&XreadgroupOutput {
                    group: input.group.value, group_encoding: input.group.encoding,
                    consumer: input.consumer.value, consumer_encoding: input.consumer.encoding,
                    timed_out, count, streams, field_bytes, fields_omitted,
                }, count, "Retry XREADGROUP with fewer streams or a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XackInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    ids: Vec<StreamId>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XackOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    requested: usize,
    acknowledged: u64,
}

fn xack_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xack")
        .title("Acknowledge Redis Stream Entries")
        .description("Acknowledge an explicit bounded set of pending stream IDs.")
        .output_schema(output_schema::<XackOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XackInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xack")?;
                validate_argument_count(input.ids.len(), "ids")?;
                let mut cmd = command("redis_xack", AccessMode::ReadWrite, "XACK");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?);
                for id in &input.ids {
                    cmd.arg(id.token());
                }
                let acknowledged = state.query(cmd, "XACK failed").await?;
                state.output(&XackOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    group: input.group.value,
                    group_encoding: input.group.encoding,
                    requested: input.ids.len(),
                    acknowledged,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XclaimInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    consumer: BinaryInput,
    min_idle_time_ms: u64,
    ids: Vec<StreamId>,
    #[serde(default)]
    idle_ms: Option<u64>,
    #[serde(default)]
    time_unix_ms: Option<u64>,
    #[serde(default)]
    retry_count: Option<u64>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    just_id: bool,
    #[serde(default = "default_returned_bytes")]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ClaimOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    consumer: String,
    consumer_encoding: InputEncoding,
    count: usize,
    ids: Vec<String>,
    entries: Vec<StreamEntryOutput>,
    just_id: bool,
    field_bytes: usize,
    fields_omitted: bool,
}

fn parse_ids(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<String>> {
    redis_array(value, context)?
        .into_iter()
        .map(|value| redis_string(value, context))
        .collect()
}

fn xclaim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xclaim")
        .title("Claim Redis Stream Entries")
        .description("Claim an explicit bounded set of pending IDs. Supports JUSTID and preserves claimed IDs if returned fields exceed the payload ceiling.")
        .output_schema(output_schema::<ClaimOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XclaimInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xclaim")?;
                validate_argument_count(input.ids.len(), "ids")?;
                if input.idle_ms.is_some() && input.time_unix_ms.is_some() { return Err(tower_mcp::Error::tool("idle_ms and time_unix_ms are mutually exclusive")); }
                if input.max_returned_bytes == 0 { return Err(tower_mcp::Error::tool("max_returned_bytes must be greater than zero")); }
                let mut cmd = command("redis_xclaim", AccessMode::ReadWrite, "XCLAIM");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(decode_binary_name(&input.consumer, "consumer")?)
                    .arg(input.min_idle_time_ms.to_string());
                for id in &input.ids { cmd.arg(id.token()); }
                if let Some(idle) = input.idle_ms { cmd.arg("IDLE").arg(idle.to_string()); }
                if let Some(time) = input.time_unix_ms { cmd.arg("TIME").arg(time.to_string()); }
                if let Some(retry) = input.retry_count { cmd.arg("RETRYCOUNT").arg(retry.to_string()); }
                if input.force { cmd.arg("FORCE"); }
                if input.just_id { cmd.arg("JUSTID"); }
                let value = state.raw(cmd, "XCLAIM failed").await?;
                let (ids, mut entries) = if input.just_id {
                    (parse_ids(value, "XCLAIM JUSTID")?, Vec::new())
                } else {
                    let entries = parse_entries(value, "XCLAIM")?;
                    let ids = entries.iter().map(|entry| entry.id.clone()).collect();
                    (ids, entries)
                };
                let count = ids.len();
                let (field_bytes, fields_omitted) = omit_large_entry_fields(&mut entries, input.max_returned_bytes.min(state.max_output_bytes()));
                state.output_collection(&ClaimOutput {
                    key: input.key, key_encoding: input.key_encoding,
                    group: input.group.value, group_encoding: input.group.encoding,
                    consumer: input.consumer.value, consumer_encoding: input.consumer.encoding,
                    count, ids, entries, just_id: input.just_id, field_bytes, fields_omitted,
                }, count, "Retry XCLAIM with fewer IDs or JUSTID.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XautoclaimInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    consumer: BinaryInput,
    min_idle_time_ms: u64,
    start: StreamId,
    #[serde(default = "default_count")]
    count: usize,
    #[serde(default)]
    just_id: bool,
    #[serde(default = "default_returned_bytes")]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XautoclaimOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    consumer: String,
    consumer_encoding: InputEncoding,
    next_start_id: String,
    count: usize,
    ids: Vec<String>,
    entries: Vec<StreamEntryOutput>,
    deleted_ids: Vec<String>,
    just_id: bool,
    field_bytes: usize,
    fields_omitted: bool,
}

fn xautoclaim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xautoclaim")
        .title("Auto-Claim Redis Stream Entries")
        .description(
            "Claim one bounded pending-entry scan page and return the server continuation ID.",
        )
        .output_schema(output_schema::<XautoclaimOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XautoclaimInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_xautoclaim")?;
                let scanned = input.count.checked_mul(10).ok_or_else(|| {
                    tower_mcp::Error::tool("count times the Redis scan factor is too large")
                })?;
                state.validate_requested_entries(scanned, "count times the Redis scan factor")?;
                if input.max_returned_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_returned_bytes must be greater than zero",
                    ));
                }
                let mut cmd = command("redis_xautoclaim", AccessMode::ReadWrite, "XAUTOCLAIM");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(decode_binary_name(&input.consumer, "consumer")?)
                    .arg(input.min_idle_time_ms.to_string())
                    .arg(input.start.token())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                if input.just_id {
                    cmd.arg("JUSTID");
                }
                let mut values =
                    redis_array(state.raw(cmd, "XAUTOCLAIM failed").await?, "XAUTOCLAIM")?;
                if !(2..=3).contains(&values.len()) {
                    return Err(tower_mcp::Error::tool(
                        "XAUTOCLAIM returned an invalid reply",
                    ));
                }
                let deleted_ids = if values.len() == 3 {
                    parse_ids(
                        values.pop().expect("length checked"),
                        "XAUTOCLAIM deleted IDs",
                    )?
                } else {
                    Vec::new()
                };
                let claimed = values.pop().expect("length checked");
                let next_start_id = redis_string(
                    values.pop().expect("length checked"),
                    "XAUTOCLAIM next start ID",
                )?;
                let (ids, mut entries) = if input.just_id {
                    (parse_ids(claimed, "XAUTOCLAIM JUSTID")?, Vec::new())
                } else {
                    let entries = parse_entries(claimed, "XAUTOCLAIM")?;
                    let ids = entries.iter().map(|entry| entry.id.clone()).collect();
                    (ids, entries)
                };
                let count = ids.len();
                let output_entries = count.saturating_add(deleted_ids.len());
                let (field_bytes, fields_omitted) = omit_large_entry_fields(
                    &mut entries,
                    input.max_returned_bytes.min(state.max_output_bytes()),
                );
                state.output_collection(
                    &XautoclaimOutput {
                        key: input.key,
                        key_encoding: input.key_encoding,
                        group: input.group.value,
                        group_encoding: input.group.encoding,
                        consumer: input.consumer.value,
                        consumer_encoding: input.consumer.encoding,
                        next_start_id,
                        count,
                        ids,
                        entries,
                        deleted_ids,
                        just_id: input.just_id,
                        field_bytes,
                        fields_omitted,
                    },
                    output_entries,
                    "Retry XAUTOCLAIM with a smaller count or JUSTID.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XdelInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    ids: Vec<StreamId>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XdelOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    deleted: u64,
}

fn xdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xdel")
        .title("Delete Redis Stream Entries")
        .description(
            "Permanently delete an explicit bounded set of stream IDs. Requires full access.",
        )
        .output_schema(output_schema::<XdelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XdelInput>| async move {
                state.require(AccessMode::Full, "redis_xdel")?;
                validate_argument_count(input.ids.len(), "ids")?;
                let mut cmd = command("redis_xdel", AccessMode::Full, "XDEL");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for id in &input.ids {
                    cmd.arg(id.token());
                }
                let deleted = state.query(cmd, "XDEL failed").await?;
                state.output(&XdelOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested: input.ids.len(),
                    deleted,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XtrimInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    trim: StreamTrim,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XtrimOutput {
    key: String,
    key_encoding: InputEncoding,
    removed: u64,
    trim: StreamTrim,
}

fn xtrim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xtrim")
        .title("Trim Redis Stream")
        .description("Permanently trim a stream by MAXLEN or MINID. Requires full access.")
        .output_schema(output_schema::<XtrimOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XtrimInput>| async move {
                state.require(AccessMode::Full, "redis_xtrim")?;
                let mut cmd = command("redis_xtrim", AccessMode::Full, "XTRIM");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                add_trim_arguments(&mut cmd, &input.trim, state.redis_version())?;
                let removed = state.query(cmd, "XTRIM failed").await?;
                state.output(&XtrimOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    removed,
                    trim: input.trim,
                })
            },
        )
        .build()
}

fn xgroup_destroy_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xgroup_destroy")
        .title("Destroy Redis Stream Group")
        .description(
            "Permanently destroy a consumer group and its pending state. Requires full access.",
        )
        .output_schema(output_schema::<GroupMutationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GroupKeyInput>| async move {
                state.require(AccessMode::Full, "redis_xgroup_destroy")?;
                let mut cmd = command("redis_xgroup_destroy", AccessMode::Full, "XGROUP");
                cmd.arg("DESTROY")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?);
                let applied = state.query::<u64>(cmd, "XGROUP DESTROY failed").await? != 0;
                state.output(&GroupMutationOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    group: input.group.value,
                    group_encoding: input.group.encoding,
                    applied,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DeleteConsumerOutput {
    key: String,
    key_encoding: InputEncoding,
    group: String,
    group_encoding: InputEncoding,
    consumer: String,
    consumer_encoding: InputEncoding,
    pending_deleted: u64,
}

fn xgroup_delconsumer_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xgroup_delconsumer")
        .title("Delete Redis Stream Consumer")
        .description("Permanently delete a consumer and remove its pending entries from the group PEL. Requires full access.")
        .output_schema(output_schema::<DeleteConsumerOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ConsumerKeyInput>| async move {
                state.require(AccessMode::Full, "redis_xgroup_delconsumer")?;
                let mut cmd = command("redis_xgroup_delconsumer", AccessMode::Full, "XGROUP");
                cmd.arg("DELCONSUMER")
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_binary_name(&input.group, "group")?)
                    .arg(decode_binary_name(&input.consumer, "consumer")?);
                let pending_deleted = state.query(cmd, "XGROUP DELCONSUMER failed").await?;
                state.output(&DeleteConsumerOutput {
                    key: input.key, key_encoding: input.key_encoding,
                    group: input.group.value, group_encoding: input.group.encoding,
                    consumer: input.consumer.value, consumer_encoding: input.consumer.encoding,
                    pending_deleted,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(xlen_tool(state.clone()));
    router = router.tool(range_tool(state.clone(), false));
    router = router.tool(range_tool(state.clone(), true));
    router = router.tool(xread_tool(state.clone()));
    router = router.tool(xinfo_stream_tool(state.clone()));
    router = router.tool(xinfo_groups_tool(state.clone()));
    router = router.tool(xinfo_consumers_tool(state.clone()));
    router.tool(xpending_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(xadd_tool(state.clone()));
    router = router.tool(xgroup_create_tool(state.clone()));
    router = router.tool(xgroup_setid_tool(state.clone()));
    router = router.tool(xgroup_createconsumer_tool(state.clone()));
    router = router.tool(xreadgroup_tool(state.clone()));
    router = router.tool(xack_tool(state.clone()));
    router = router.tool(xclaim_tool(state.clone()));
    router.tool(xautoclaim_tool(state))
}

pub(super) fn add_destructive_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(xdel_tool(state.clone()));
    router = router.tool(xtrim_tool(state.clone()));
    router = router.tool(xgroup_destroy_tool(state.clone()));
    router.tool(xgroup_delconsumer_tool(state))
}
