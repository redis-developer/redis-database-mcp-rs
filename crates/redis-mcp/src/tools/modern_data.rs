//! Redis 8 native arrays, vector sets, and finite modern command deltas.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, KeyInput, ToolState, ValueEncoding, command, decode_input,
    destructive_annotations, encode_bytes, output_schema, read_annotations, write_annotations,
};
use crate::{
    AccessMode, RedisValue,
    invocation::{redis_value_collection_entries, redis_value_to_json},
};

const MAX_ARGUMENT_ITEMS: usize = 1_000;
const MAX_ARRAY_PREDICATES: usize = 250;
const MAX_ARRAY_REGEX_BYTES: usize = 2_048;
const MAX_VECTOR_DIMENSIONS: usize = 65_536;
const MAX_VECTOR_EXPLORATION_FACTOR: usize = 1_000_000;
const MIN_VECTOR_LINKS: usize = 4;
const MAX_VECTOR_LINKS: usize = 4_096;

fn validate_items(items: &[impl Sized], name: &str) -> tower_mcp::Result<()> {
    if items.is_empty() || items.len() > MAX_ARGUMENT_ITEMS {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_ARGUMENT_ITEMS} items"
        )))
    } else {
        Ok(())
    }
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

fn redis_integer(value: RedisValue, context: &str) -> tower_mcp::Result<i64> {
    match without_attributes(value) {
        RedisValue::Integer(value) => Ok(value),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected integer reply: {other:?}"
        ))),
    }
}

fn redis_boolean(value: RedisValue, context: &str) -> tower_mcp::Result<bool> {
    match without_attributes(value) {
        RedisValue::Boolean(value) => Ok(value),
        RedisValue::Integer(value) => Ok(value != 0),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected boolean reply: {other:?}"
        ))),
    }
}

fn redis_nonnegative(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    match without_attributes(value) {
        RedisValue::Integer(value) => u64::try_from(value).map_err(|_| {
            tower_mcp::Error::tool(format!(
                "{context} returned an unexpected negative integer: {value}"
            ))
        }),
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| {
                tower_mcp::Error::tool(format!("{context} returned an invalid unsigned integer"))
            }),
        RedisValue::SimpleString(value) => value.parse::<u64>().map_err(|_| {
            tower_mcp::Error::tool(format!("{context} returned an invalid unsigned integer"))
        }),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected unsigned integer reply: {other:?}"
        ))),
    }
}

fn redis_text(value: RedisValue, context: &str) -> tower_mcp::Result<String> {
    match without_attributes(value) {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => String::from_utf8(value)
            .map_err(|_| tower_mcp::Error::tool(format!("{context} returned non-UTF-8 text"))),
        RedisValue::SimpleString(value) => Ok(value),
        RedisValue::Integer(value) => Ok(value.to_string()),
        RedisValue::Double(value) if value.is_finite() => Ok(value.to_string()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected text reply: {other:?}"
        ))),
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BinaryInput {
    /// Binary-safe value.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

impl BinaryInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.value, self.encoding, name)
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
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

fn optional_binary(value: RedisValue, context: &str) -> tower_mcp::Result<Option<BinaryOutput>> {
    match without_attributes(value) {
        RedisValue::Nil => Ok(None),
        RedisValue::BulkString(value) => Ok(Some(BinaryOutput::new(value))),
        RedisValue::SimpleString(value) => Ok(Some(BinaryOutput::new(value.into_bytes()))),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected binary reply: {other:?}"
        ))),
    }
}

fn binary_array(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<Option<BinaryOutput>>> {
    redis_array(value, context)?
        .into_iter()
        .map(|value| optional_binary(value, context))
        .collect()
}

fn required_binary_array(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<BinaryOutput>> {
    binary_array(value, context)?
        .into_iter()
        .map(|value| {
            value.ok_or_else(|| {
                tower_mcp::Error::tool(format!("{context} returned an unexpected null element"))
            })
        })
        .collect()
}

fn string_key(input: &KeyInput) -> tower_mcp::Result<Vec<u8>> {
    decode_input(&input.key, input.key_encoding, "key")
}

fn inclusive_range_len(start: u64, end: u64, name: &str) -> tower_mcp::Result<usize> {
    validate_array_index(start, false, &format!("{name} start"))?;
    validate_array_index(end, false, &format!("{name} end"))?;
    let span = u128::from(start.abs_diff(end)) + 1;
    usize::try_from(span).map_err(|_| tower_mcp::Error::tool(format!("{name} range is too large")))
}

fn validate_array_index(index: u64, allow_max: bool, name: &str) -> tower_mcp::Result<()> {
    if index == u64::MAX && !allow_max {
        Err(tower_mcp::Error::tool(format!(
            "{name} must be between 0 and {}",
            u64::MAX - 1
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ArrayBound {
    Minimum,
    Index(u64),
    Maximum,
}

fn parse_array_bound(value: &str, name: &str) -> tower_mcp::Result<ArrayBound> {
    match value {
        "-" => Ok(ArrayBound::Minimum),
        "+" => Ok(ArrayBound::Maximum),
        value => {
            let index = value.parse::<u64>().map_err(|_| {
                tower_mcp::Error::tool(format!(
                    "{name} must be '-', '+', or an unsigned 64-bit array index"
                ))
            })?;
            validate_array_index(index, false, name)?;
            Ok(ArrayBound::Index(index))
        }
    }
}

fn redis_map(value: RedisValue, context: &str) -> tower_mcp::Result<BTreeMap<String, JsonValue>> {
    let pairs = match without_attributes(value) {
        RedisValue::Map(pairs) => pairs,
        RedisValue::Array(values) if values.len() % 2 == 0 => values
            .chunks_exact(2)
            .map(|pair| (pair[0].clone(), pair[1].clone()))
            .collect(),
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an unexpected map reply: {other:?}"
            )));
        }
    };
    pairs
        .into_iter()
        .map(|(key, value)| Ok((redis_text(key, context)?, redis_value_to_json(&value))))
        .collect()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
enum ExactDecimalInput {
    /// JSON number shorthand. Use a string when every decimal digit matters.
    Number(f64),
    /// Exact finite Redis decimal string.
    Exact(String),
}

impl ExactDecimalInput {
    fn token(&self, name: &str) -> tower_mcp::Result<String> {
        let token = match self {
            Self::Number(value) => value.to_string(),
            Self::Exact(value) => value.clone(),
        };
        let value = token.parse::<f64>().map_err(|_| {
            tower_mcp::Error::tool(format!("{name} must be a valid finite decimal"))
        })?;
        if !value.is_finite() {
            return Err(tower_mcp::Error::tool(format!(
                "{name} must be a finite decimal"
            )));
        }
        Ok(token)
    }
}

// Redis Array ------------------------------------------------------------

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayCountOutput {
    count: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayLengthOutput {
    length: u64,
}

fn array_count_tool(
    state: Arc<ToolState>,
    name: &'static str,
    title: &'static str,
    description: &'static str,
    redis_command: &'static str,
) -> Tool {
    ToolBuilder::new(name)
        .title(title)
        .description(description)
        .output_schema(if redis_command == "ARCOUNT" {
            output_schema::<ArrayCountOutput>()
        } else {
            output_schema::<ArrayLengthOutput>()
        })
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command(name, AccessMode::ReadOnly, redis_command);
                cmd.arg(string_key(&input)?);
                let value = redis_nonnegative(
                    state.raw(cmd, "Redis Array read failed").await?,
                    redis_command,
                )?;
                if redis_command == "ARCOUNT" {
                    state.output(&ArrayCountOutput { count: value })
                } else {
                    state.output(&ArrayLengthOutput { length: value })
                }
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayIndexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    index: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayGetOutput {
    index: u64,
    exists: bool,
    value: Option<BinaryOutput>,
}

fn arget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arget")
        .title("Get Redis Array Element")
        .description("Read one binary-safe Redis Array element by unsigned 64-bit index.")
        .output_schema(output_schema::<ArrayGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArrayIndexInput>| async move {
                validate_array_index(input.index, false, "index")?;
                let mut cmd = command("redis_arget", AccessMode::ReadOnly, "ARGET");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.index.to_string());
                let value = optional_binary(state.raw(cmd, "ARGET failed").await?, "ARGET")?;
                state.output(&ArrayGetOutput {
                    index: input.index,
                    exists: value.is_some(),
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayRangeInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    start: u64,
    end: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayRangeOutput {
    start: u64,
    end: u64,
    values: Vec<Option<BinaryOutput>>,
}

fn argetrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_argetrange")
        .title("Get Redis Array Range")
        .description("Read one explicitly bounded inclusive Redis Array range, preserving empty slots as null values.")
        .output_schema(output_schema::<ArrayRangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArrayRangeInput>| async move {
                let requested = inclusive_range_len(input.start, input.end, "array")?;
                state.validate_requested_entries(requested, "array range length")?;
                let mut cmd = command("redis_argetrange", AccessMode::ReadOnly, "ARGETRANGE");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.to_string())
                    .arg(input.end.to_string());
                let values = binary_array(state.raw(cmd, "ARGETRANGE failed").await?, "ARGETRANGE")?;
                state.output_collection(
                    &ArrayRangeOutput { start: input.start, end: input.end, values },
                    requested,
                    "Request a narrower Redis Array range.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ArrayPredicate {
    Exact { value: BinaryInput },
    Match { value: BinaryInput },
    Glob { pattern: String },
    Regex { pattern: String },
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum PredicateCombination {
    All,
    #[default]
    Any,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArgrepInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Inclusive textual start bound accepted by Redis, including special array bounds.
    start: String,
    /// Inclusive textual end bound accepted by Redis, including special array bounds.
    end: String,
    #[schemars(length(min = 1, max = 250))]
    predicates: Vec<ArrayPredicate>,
    #[serde(default)]
    combination: PredicateCombination,
    #[serde(default)]
    no_case: bool,
    #[serde(default)]
    with_values: bool,
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayEntryOutput {
    index: u64,
    value: BinaryOutput,
}

fn array_entries(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<ArrayEntryOutput>> {
    let values = redis_array(value, context)?;
    let pairs = if values.iter().all(|value| {
        matches!(
            without_attributes(value.clone()),
            RedisValue::Array(_) | RedisValue::Set(_)
        )
    }) {
        values
            .into_iter()
            .map(|entry| redis_array(entry, context))
            .collect::<tower_mcp::Result<Vec<_>>>()?
    } else {
        if values.len() % 2 != 0 {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an odd number of index/value elements"
            )));
        }
        values.chunks_exact(2).map(|pair| pair.to_vec()).collect()
    };
    pairs
        .into_iter()
        .map(|pair| {
            if pair.len() != 2 {
                return Err(tower_mcp::Error::tool(format!(
                    "{context} returned an invalid index/value pair"
                )));
            }
            let index = redis_nonnegative(pair[0].clone(), context)?;
            let value = optional_binary(pair[1].clone(), context)?.ok_or_else(|| {
                tower_mcp::Error::tool(format!("{context} returned a null array value"))
            })?;
            Ok(ArrayEntryOutput { index, value })
        })
        .collect()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArgrepOutput {
    count: usize,
    with_values: bool,
    indices: Vec<u64>,
    entries: Vec<ArrayEntryOutput>,
    complete: bool,
    next_start: Option<String>,
}

fn argrep_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_argrep")
        .title("Search Redis Array")
        .description("Search a Redis Array with typed exact, substring, glob, or regular-expression predicates and a mandatory result limit.")
        .output_schema(output_schema::<ArgrepOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArgrepInput>| async move {
                if input.predicates.is_empty() || input.predicates.len() > MAX_ARRAY_PREDICATES {
                    return Err(tower_mcp::Error::tool(format!(
                        "predicates must contain between 1 and {MAX_ARRAY_PREDICATES} items"
                    )));
                }
                for predicate in &input.predicates {
                    if let ArrayPredicate::Regex { pattern } = predicate
                        && (pattern.is_empty() || pattern.len() > MAX_ARRAY_REGEX_BYTES)
                    {
                        return Err(tower_mcp::Error::tool(format!(
                            "regular-expression predicates must contain between 1 and {MAX_ARRAY_REGEX_BYTES} bytes"
                        )));
                    }
                }
                state.validate_requested_entries(input.limit, "limit")?;
                let start_bound = parse_array_bound(&input.start, "start")?;
                let end_bound = parse_array_bound(&input.end, "end")?;
                let mut cmd = command("redis_argrep", AccessMode::ReadOnly, "ARGREP");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.as_bytes())
                    .arg(input.end.as_bytes());
                for predicate in &input.predicates {
                    match predicate {
                        ArrayPredicate::Exact { value } => {
                            cmd.arg("EXACT").arg(value.decode("predicates[].value")?);
                        }
                        ArrayPredicate::Match { value } => {
                            cmd.arg("MATCH").arg(value.decode("predicates[].value")?);
                        }
                        ArrayPredicate::Glob { pattern } => {
                            cmd.arg("GLOB").arg(pattern.as_bytes());
                        }
                        ArrayPredicate::Regex { pattern } => {
                            cmd.arg("RE").arg(pattern.as_bytes());
                        }
                    }
                }
                if input.predicates.len() > 1 {
                    cmd.arg(match input.combination {
                        PredicateCombination::All => "AND",
                        PredicateCombination::Any => "OR",
                    });
                }
                if input.no_case { cmd.arg("NOCASE"); }
                cmd.arg("LIMIT").arg(input.limit.to_string());
                if input.with_values { cmd.arg("WITHVALUES"); }
                let raw = state.raw(cmd, "ARGREP failed").await?;
                let (indices, entries) = if input.with_values {
                    (Vec::new(), array_entries(raw, "ARGREP")?)
                } else {
                    let indices = redis_array(raw, "ARGREP")?
                        .into_iter()
                        .map(|value| redis_nonnegative(value, "ARGREP"))
                        .collect::<tower_mcp::Result<Vec<_>>>()?;
                    (indices, Vec::new())
                };
                let count = indices.len() + entries.len();
                let last_index = indices
                    .last()
                    .copied()
                    .or_else(|| entries.last().map(|entry| entry.index));
                let descending = start_bound > end_bound;
                let next_start = if count == input.limit {
                    last_index.and_then(|index| {
                        if descending {
                            index.checked_sub(1)
                        } else {
                            index.checked_add(1).filter(|next| *next != u64::MAX)
                        }
                    }).map(|index| index.to_string())
                } else {
                    None
                };
                let complete = next_start.is_none();
                state.output_collection(
                    &ArgrepOutput { count, with_values: input.with_values, indices, entries, complete, next_start },
                    count,
                    "Retry ARGREP with a smaller limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArinfoInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    full: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MapOutput {
    exists: bool,
    fields: BTreeMap<String, JsonValue>,
}

fn arinfo_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arinfo")
        .title("Inspect Redis Array")
        .description("Return normalized Redis Array metadata. FULL adds bounded per-layout statistics, not element values.")
        .output_schema(output_schema::<MapOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArinfoInput>| async move {
                let mut cmd = command("redis_arinfo", AccessMode::ReadOnly, "ARINFO");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if input.full { cmd.arg("FULL"); }
                let fields = redis_map(state.raw(cmd, "ARINFO failed").await?, "ARINFO")?;
                state.output_collection(
                    &MapOutput {
                        exists: true,
                        fields,
                    },
                    11,
                    "Retry ARINFO without FULL.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArlastitemsInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    #[serde(default)]
    reverse: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BinaryValuesOutput {
    count: usize,
    values: Vec<Option<BinaryOutput>>,
}

fn arlastitems_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arlastitems")
        .title("Read Recent Redis Array Items")
        .description("Return a bounded number of the most recently inserted Redis Array elements, preserving empty positions as null.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArlastitemsInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut cmd = command("redis_arlastitems", AccessMode::ReadOnly, "ARLASTITEMS");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.count.to_string());
                if input.reverse { cmd.arg("REV"); }
                let values = binary_array(state.raw(cmd, "ARLASTITEMS failed").await?, "ARLASTITEMS")?;
                state.output_collection(
                    &BinaryValuesOutput { count: values.len(), values },
                    input.count,
                    "Retry ARLASTITEMS with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArmgetInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    indices: Vec<u64>,
}

fn armget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_armget")
        .title("Get Redis Array Elements")
        .description("Read an explicit bounded set of Redis Array indices in request order, preserving missing elements as null.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArmgetInput>| async move {
                validate_items(&input.indices, "indices")?;
                state.validate_requested_entries(input.indices.len(), "indices")?;
                for index in &input.indices {
                    validate_array_index(*index, false, "indices[]")?;
                }
                let mut cmd = command("redis_armget", AccessMode::ReadOnly, "ARMGET");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for index in &input.indices { cmd.arg(index.to_string()); }
                let values = binary_array(state.raw(cmd, "ARMGET failed").await?, "ARMGET")?;
                state.output_collection(
                    &BinaryValuesOutput { count: values.len(), values },
                    input.indices.len(),
                    "Retry ARMGET with fewer indices.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArnextOutput {
    exhausted: bool,
    next_index: Option<u64>,
}

fn arnext_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arnext")
        .title("Read Redis Array Insert Cursor")
        .description(
            "Return the next index ARINSERT would use, or report that the cursor is exhausted.",
        )
        .output_schema(output_schema::<ArnextOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command("redis_arnext", AccessMode::ReadOnly, "ARNEXT");
                cmd.arg(string_key(&input)?);
                let value = without_attributes(state.raw(cmd, "ARNEXT failed").await?);
                let next_index = match value {
                    RedisValue::Nil => None,
                    other => Some(redis_nonnegative(other, "ARNEXT")?),
                };
                state.output(&ArnextOutput {
                    exhausted: next_index.is_none(),
                    next_index,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum ArrayAggregate {
    Sum,
    Min,
    Max,
    And,
    Or,
    Xor,
    Match { value: BinaryInput },
    Used,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AropInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    start: u64,
    end: u64,
    operation: ArrayAggregate,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AropOutput {
    operation: ArrayAggregate,
    result: JsonValue,
}

fn arop_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arop")
        .title("Aggregate Redis Array Range")
        .description("Run one typed Redis Array aggregate over an inclusive range. Redis visits only allocated array positions where possible.")
        .output_schema(output_schema::<AropOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<AropInput>| async move {
                inclusive_range_len(input.start, input.end, "array")?;
                let mut cmd = command("redis_arop", AccessMode::ReadOnly, "AROP");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.to_string())
                    .arg(input.end.to_string());
                match &input.operation {
                    ArrayAggregate::Sum => { cmd.arg("SUM"); }
                    ArrayAggregate::Min => { cmd.arg("MIN"); }
                    ArrayAggregate::Max => { cmd.arg("MAX"); }
                    ArrayAggregate::And => { cmd.arg("AND"); }
                    ArrayAggregate::Or => { cmd.arg("OR"); }
                    ArrayAggregate::Xor => { cmd.arg("XOR"); }
                    ArrayAggregate::Match { value } => { cmd.arg("MATCH").arg(value.decode("operation.value")?); }
                    ArrayAggregate::Used => { cmd.arg("USED"); }
                }
                let raw = state.raw(cmd, "AROP failed").await?;
                state.output(&AropOutput { operation: input.operation, result: redis_value_to_json(&raw) })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArscanInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    start: u64,
    end: u64,
    #[schemars(range(min = 1, max = 1000))]
    limit: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArscanOutput {
    start: u64,
    end: u64,
    count: usize,
    entries: Vec<ArrayEntryOutput>,
    complete: bool,
    next_start: Option<u64>,
}

fn arscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arscan")
        .title("Scan Redis Array Range")
        .description("Read one bounded page of allocated Redis Array elements. Continue with next_start until complete is true.")
        .output_schema(output_schema::<ArscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArscanInput>| async move {
                inclusive_range_len(input.start, input.end, "array")?;
                state.validate_requested_entries(input.limit, "limit")?;
                let mut cmd = command("redis_arscan", AccessMode::ReadOnly, "ARSCAN");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.to_string())
                    .arg(input.end.to_string())
                    .arg("LIMIT")
                    .arg(input.limit.to_string());
                let entries = array_entries(state.raw(cmd, "ARSCAN failed").await?, "ARSCAN")?;
                let next_start = if entries.len() == input.limit {
                    entries.last().and_then(|entry| {
                        if input.start <= input.end {
                            entry.index.checked_add(1).filter(|next| *next <= input.end)
                        } else {
                            entry.index.checked_sub(1).filter(|next| *next >= input.end)
                        }
                    })
                } else { None };
                let complete = next_start.is_none();
                state.output_collection(
                    &ArscanOutput { start: input.start, end: input.end, count: entries.len(), entries, complete, next_start },
                    input.limit,
                    "Retry ARSCAN with a smaller limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayValuesInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    values: Vec<BinaryInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArraySetOutput {
    new_slots: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayDeleteOutput {
    deleted: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArraySeekOutput {
    set: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayLastIndexOutput {
    last_index: u64,
}

fn arinsert_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arinsert")
        .title("Insert Redis Array Values")
        .description("Insert a bounded sequence of binary-safe values at the array's current insertion cursor.")
        .output_schema(output_schema::<ArrayLastIndexOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArrayValuesInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_arinsert")?;
                validate_items(&input.values, "values")?;
                let mut cmd = command("redis_arinsert", AccessMode::ReadWrite, "ARINSERT");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, value) in input.values.iter().enumerate() { cmd.arg(value.decode(&format!("values[{index}]"))?); }
                let last_index = redis_nonnegative(state.raw(cmd, "ARINSERT failed").await?, "ARINSERT")?;
                state.output(&ArrayLastIndexOutput { last_index })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArrayPairInput {
    index: u64,
    value: BinaryInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArmsetInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    entries: Vec<ArrayPairInput>,
}

fn armset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_armset")
        .title("Set Redis Array Elements")
        .description("Set a bounded list of explicit index/value pairs in a Redis Array.")
        .output_schema(output_schema::<ArraySetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArmsetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_armset")?;
                validate_items(&input.entries, "entries")?;
                for entry in &input.entries {
                    validate_array_index(entry.index, false, "entries[].index")?;
                }
                let mut cmd = command("redis_armset", AccessMode::ReadWrite, "ARMSET");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for (index, entry) in input.entries.iter().enumerate() {
                    cmd.arg(entry.index.to_string())
                        .arg(entry.value.decode(&format!("entries[{index}].value"))?);
                }
                let new_slots =
                    redis_nonnegative(state.raw(cmd, "ARMSET failed").await?, "ARMSET")?;
                state.output(&ArraySetOutput { new_slots })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArringInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(range(min = 1))]
    size: u64,
    #[schemars(length(min = 1, max = 1000))]
    values: Vec<BinaryInput>,
}

fn arring_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arring")
        .title("Append Redis Array Ring")
        .description("Insert a bounded sequence into a Redis Array ring, resizing and truncating the ring to the requested positive size.")
        .output_schema(output_schema::<ArrayLastIndexOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArringInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_arring")?;
                if input.size == 0 { return Err(tower_mcp::Error::tool("size must be greater than zero")); }
                validate_items(&input.values, "values")?;
                let mut cmd = command("redis_arring", AccessMode::ReadWrite, "ARRING");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.size.to_string());
                for (index, value) in input.values.iter().enumerate() { cmd.arg(value.decode(&format!("values[{index}]"))?); }
                let last_index = redis_nonnegative(state.raw(cmd, "ARRING failed").await?, "ARRING")?;
                state.output(&ArrayLastIndexOutput { last_index })
            },
        )
        .build()
}

fn arseek_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arseek")
        .title("Set Redis Array Insert Cursor")
        .description(
            "Set the insertion cursor for a Redis Array to one explicit unsigned 64-bit index.",
        )
        .output_schema(output_schema::<ArraySeekOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArrayIndexInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_arseek")?;
                validate_array_index(input.index, true, "index")?;
                let mut cmd = command("redis_arseek", AccessMode::ReadWrite, "ARSEEK");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.index.to_string());
                let set = redis_boolean(state.raw(cmd, "ARSEEK failed").await?, "ARSEEK")?;
                state.output(&ArraySeekOutput { set })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArsetInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    index: u64,
    #[schemars(length(min = 1, max = 1000))]
    values: Vec<BinaryInput>,
}

fn arset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_arset")
        .title("Set Redis Array Range")
        .description("Set a bounded contiguous sequence of binary-safe Redis Array values starting at one index.")
        .output_schema(output_schema::<ArraySetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArsetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_arset")?;
                validate_items(&input.values, "values")?;
                validate_array_index(input.index, false, "index")?;
                let last_index = input
                    .index
                    .checked_add(input.values.len() as u64 - 1)
                    .filter(|index| *index != u64::MAX)
                    .ok_or_else(|| tower_mcp::Error::tool("index plus values exceeds the Redis Array index range"))?;
                validate_array_index(last_index, false, "last index")?;
                let mut cmd = command("redis_arset", AccessMode::ReadWrite, "ARSET");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.index.to_string());
                for (index, value) in input.values.iter().enumerate() { cmd.arg(value.decode(&format!("values[{index}]"))?); }
                let new_slots = redis_nonnegative(state.raw(cmd, "ARSET failed").await?, "ARSET")?;
                state.output(&ArraySetOutput { new_slots })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArdelInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    indices: Vec<u64>,
}

fn ardel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ardel")
        .title("Delete Redis Array Elements")
        .description("Permanently delete a bounded explicit set of Redis Array indices. Requires full access.")
        .output_schema(output_schema::<ArrayDeleteOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArdelInput>| async move {
                state.require(AccessMode::Full, "redis_ardel")?;
                validate_items(&input.indices, "indices")?;
                for index in &input.indices {
                    validate_array_index(*index, false, "indices[]")?;
                }
                let mut cmd = command("redis_ardel", AccessMode::Full, "ARDEL");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for index in &input.indices { cmd.arg(index.to_string()); }
                let deleted = redis_nonnegative(state.raw(cmd, "ARDEL failed").await?, "ARDEL")?;
                state.output(&ArrayDeleteOutput { deleted })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IndexRangeInput {
    start: u64,
    end: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArdelrangeInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    ranges: Vec<IndexRangeInput>,
}

fn ardelrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ardelrange")
        .title("Delete Redis Array Ranges")
        .description("Permanently delete one or more explicit inclusive Redis Array ranges. Requires full access.")
        .output_schema(output_schema::<ArrayDeleteOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ArdelrangeInput>| async move {
                state.require(AccessMode::Full, "redis_ardelrange")?;
                validate_items(&input.ranges, "ranges")?;
                let mut cmd = command("redis_ardelrange", AccessMode::Full, "ARDELRANGE");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for range in &input.ranges {
                    inclusive_range_len(range.start, range.end, "ranges[]")?;
                    cmd.arg(range.start.to_string()).arg(range.end.to_string());
                }
                let deleted = redis_nonnegative(state.raw(cmd, "ARDELRANGE failed").await?, "ARDELRANGE")?;
                state.output(&ArrayDeleteOutput { deleted })
            },
        )
        .build()
}

// Redis vector sets ------------------------------------------------------

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum VectorInput {
    Values {
        #[schemars(length(min = 1, max = 65536))]
        values: Vec<ExactDecimalInput>,
    },
    /// Little-endian packed IEEE-754 single-precision values.
    Fp32 { data: BinaryInput },
}

impl VectorInput {
    fn dimensions(&self) -> tower_mcp::Result<usize> {
        match self {
            Self::Values { values } => {
                if values.is_empty() || values.len() > MAX_VECTOR_DIMENSIONS {
                    Err(tower_mcp::Error::tool(format!(
                        "vector must contain between 1 and {MAX_VECTOR_DIMENSIONS} dimensions"
                    )))
                } else {
                    Ok(values.len())
                }
            }
            Self::Fp32 { data } => {
                let bytes = data.decode("vector.data")?;
                if bytes.is_empty() || bytes.len() % 4 != 0 {
                    return Err(tower_mcp::Error::tool(
                        "FP32 vector data must contain a non-empty multiple of four bytes",
                    ));
                }
                let dimensions = bytes.len() / 4;
                if dimensions > MAX_VECTOR_DIMENSIONS {
                    return Err(tower_mcp::Error::tool(format!(
                        "vector must contain at most {MAX_VECTOR_DIMENSIONS} dimensions"
                    )));
                }
                Ok(dimensions)
            }
        }
    }

    fn append(&self, cmd: &mut crate::RedisCommand) -> tower_mcp::Result<()> {
        match self {
            Self::Values { values } => {
                cmd.arg("VALUES").arg(values.len().to_string());
                for (index, value) in values.iter().enumerate() {
                    cmd.arg(value.token(&format!("vector.values[{index}]"))?);
                }
            }
            Self::Fp32 { data } => {
                cmd.arg("FP32").arg(data.decode("vector.data")?);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum VectorQuantization {
    NoQuantization,
    Q8,
    Binary,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VaddInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    vector: VectorInput,
    element: BinaryInput,
    #[serde(default)]
    reduce_dimensions: Option<usize>,
    #[serde(default)]
    check_and_set: bool,
    #[serde(default)]
    quantization: Option<VectorQuantization>,
    #[serde(default)]
    #[schemars(range(min = 1, max = 1000000))]
    build_exploration_factor: Option<usize>,
    #[serde(default)]
    attributes: Option<JsonValue>,
    #[serde(default)]
    #[schemars(range(min = 4, max = 4096))]
    num_links: Option<usize>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BooleanMutationOutput {
    changed: bool,
}

fn vadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vadd")
        .title("Add Redis Vector")
        .description("Add or update one binary-safe vector-set element using typed numeric values or a little-endian FP32 blob.")
        .output_schema(output_schema::<BooleanMutationOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_vadd")?;
                let dimensions = input.vector.dimensions()?;
                if let Some(reduce) = input.reduce_dimensions
                    && (reduce == 0 || reduce > dimensions)
                {
                    return Err(tower_mcp::Error::tool("reduce_dimensions must be between 1 and the source vector dimensions"));
                }
                let mut cmd = command("redis_vadd", AccessMode::ReadWrite, "VADD");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(reduce) = input.reduce_dimensions { cmd.arg("REDUCE").arg(reduce.to_string()); }
                input.vector.append(&mut cmd)?;
                cmd.arg(input.element.decode("element")?);
                if input.check_and_set { cmd.arg("CAS"); }
                if let Some(quantization) = input.quantization {
                    cmd.arg(match quantization {
                        VectorQuantization::NoQuantization => "NOQUANT",
                        VectorQuantization::Q8 => "Q8",
                        VectorQuantization::Binary => "BIN",
                    });
                }
                if let Some(factor) = input.build_exploration_factor {
                    if factor == 0 || factor > MAX_VECTOR_EXPLORATION_FACTOR { return Err(tower_mcp::Error::tool(format!("build_exploration_factor must be between 1 and {MAX_VECTOR_EXPLORATION_FACTOR}"))); }
                    cmd.arg("EF").arg(factor.to_string());
                }
                if let Some(attributes) = input.attributes {
                    cmd.arg("SETATTR").arg(serde_json::to_vec(&attributes).map_err(|error| tower_mcp::Error::tool(format!("attributes are not valid JSON: {error}")))?);
                }
                if let Some(num_links) = input.num_links {
                    if !(MIN_VECTOR_LINKS..=MAX_VECTOR_LINKS).contains(&num_links) { return Err(tower_mcp::Error::tool(format!("num_links must be between {MIN_VECTOR_LINKS} and {MAX_VECTOR_LINKS}"))); }
                    cmd.arg("M").arg(num_links.to_string());
                }
                let changed = redis_boolean(state.raw(cmd, "VADD failed").await?, "VADD")?;
                state.output(&BooleanMutationOutput { changed })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorCardinalityOutput {
    cardinality: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorDimensionsOutput {
    dimensions: u64,
}

fn vector_integer_tool(
    state: Arc<ToolState>,
    name: &'static str,
    title: &'static str,
    description: &'static str,
    redis_command: &'static str,
) -> Tool {
    ToolBuilder::new(name)
        .title(title)
        .description(description)
        .output_schema(if redis_command == "VCARD" {
            output_schema::<VectorCardinalityOutput>()
        } else {
            output_schema::<VectorDimensionsOutput>()
        })
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command(name, AccessMode::ReadOnly, redis_command);
                cmd.arg(string_key(&input)?);
                let value = redis_nonnegative(
                    state.raw(cmd, "vector-set read failed").await?,
                    redis_command,
                )?;
                if redis_command == "VCARD" {
                    state.output(&VectorCardinalityOutput { cardinality: value })
                } else {
                    state.output(&VectorDimensionsOutput { dimensions: value })
                }
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorElementInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    element: BinaryInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VembInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    element: BinaryInput,
    #[serde(default)]
    raw: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VembOutput {
    exists: bool,
    dimensions: usize,
    values: Vec<String>,
    raw: Option<BinaryOutput>,
}

fn vemb_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vemb")
        .title("Get Redis Vector Embedding")
        .description("Read one vector-set embedding as exact decimal tokens or a binary-safe packed FP32 blob.")
        .output_schema(output_schema::<VembOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VembInput>| async move {
                let mut cmd = command("redis_vemb", AccessMode::ReadOnly, "VEMB");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                if input.raw { cmd.arg("RAW"); }
                let raw_reply = without_attributes(state.raw(cmd, "VEMB failed").await?);
                if matches!(raw_reply, RedisValue::Nil) {
                    return state.output(&VembOutput { exists: false, dimensions: 0, values: Vec::new(), raw: None });
                }
                if input.raw {
                    let bytes = match raw_reply {
                        RedisValue::BulkString(bytes) => bytes,
                        RedisValue::SimpleString(value) => value.into_bytes(),
                        other => return Err(tower_mcp::Error::tool(format!("VEMB returned an unexpected raw reply: {other:?}"))),
                    };
                    if bytes.len() % 4 != 0 {
                        return Err(tower_mcp::Error::tool("VEMB returned a malformed packed FP32 vector"));
                    }
                    let dimensions = bytes.len() / 4;
                    return state.output(&VembOutput { exists: true, dimensions, values: Vec::new(), raw: Some(BinaryOutput::new(bytes)) });
                }
                let values = redis_array(raw_reply, "VEMB")?
                    .into_iter()
                    .map(|value| redis_text(value, "VEMB"))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let dimensions = values.len();
                state.output_collection(
                    &VembOutput { exists: true, dimensions, values, raw: None },
                    dimensions,
                    "Use raw=true or a smaller vector dimension.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct JsonAttributeOutput {
    exists: bool,
    attributes: Option<JsonValue>,
}

fn vgetattr_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vgetattr")
        .title("Get Redis Vector Attributes")
        .description("Read and parse the JSON attributes associated with one vector-set element.")
        .output_schema(output_schema::<JsonAttributeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VectorElementInput>| async move {
                let mut cmd = command("redis_vgetattr", AccessMode::ReadOnly, "VGETATTR");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                let value = without_attributes(state.raw(cmd, "VGETATTR failed").await?);
                if matches!(value, RedisValue::Nil) {
                    return state.output(&JsonAttributeOutput { exists: false, attributes: None });
                }
                let text = redis_text(value, "VGETATTR")?;
                if text.is_empty() {
                    return state.output(&JsonAttributeOutput { exists: true, attributes: None });
                }
                let attributes = serde_json::from_str(&text).map_err(|error| tower_mcp::Error::tool(format!("VGETATTR returned invalid JSON: {error}")))?;
                state.output(&JsonAttributeOutput { exists: true, attributes: Some(attributes) })
            },
        )
        .build()
}

fn vinfo_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vinfo")
        .title("Inspect Redis Vector Set")
        .description("Return normalized bounded metadata for one Redis vector set.")
        .output_schema(output_schema::<MapOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command("redis_vinfo", AccessMode::ReadOnly, "VINFO");
                cmd.arg(string_key(&input)?);
                let raw = without_attributes(state.raw(cmd, "VINFO failed").await?);
                if matches!(raw, RedisValue::Nil) {
                    return state.output(&MapOutput {
                        exists: false,
                        fields: BTreeMap::new(),
                    });
                }
                let fields = redis_map(raw, "VINFO")?;
                let count = fields.len();
                state.output_collection(
                    &MapOutput {
                        exists: true,
                        fields,
                    },
                    count,
                    "Vector metadata exceeded the configured output budget.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MembershipOutput {
    exists: bool,
}

fn vismember_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vismember")
        .title("Check Redis Vector Element")
        .description("Check whether one binary-safe element exists in a Redis vector set.")
        .output_schema(output_schema::<MembershipOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VectorElementInput>| async move {
                let mut cmd = command("redis_vismember", AccessMode::ReadOnly, "VISMEMBER");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                let exists = redis_boolean(state.raw(cmd, "VISMEMBER failed").await?, "VISMEMBER")?;
                state.output(&MembershipOutput { exists })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VlinksInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    element: BinaryInput,
    #[serde(default)]
    with_scores: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StructuredOutput {
    entries: usize,
    result: JsonValue,
}

fn vlinks_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vlinks")
        .title("Inspect Redis Vector Links")
        .description("Inspect the bounded HNSW neighbors of one vector-set element, optionally including scores.")
        .output_schema(output_schema::<StructuredOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VlinksInput>| async move {
                let mut cmd = command("redis_vlinks", AccessMode::ReadOnly, "VLINKS");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                if input.with_scores { cmd.arg("WITHSCORES"); }
                let raw = state.raw(cmd, "VLINKS failed").await?;
                let entries = redis_value_collection_entries(&raw);
                state.output_collection(
                    &StructuredOutput { entries, result: redis_value_to_json(&raw) },
                    entries,
                    "Reduce the vector set's HNSW link count or inspect a different element.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VrandmemberInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    count: Option<i64>,
}

fn vrandmember_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vrandmember")
        .title("Sample Redis Vector Members")
        .description("Return one member or a bounded positive/negative-count sample from a Redis vector set.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VrandmemberInput>| async move {
                if let Some(count) = input.count {
                    let requested = count.checked_abs().and_then(|value| usize::try_from(value).ok()).ok_or_else(|| tower_mcp::Error::tool("count magnitude is too large"))?;
                    state.validate_requested_entries(requested, "count")?;
                }
                let mut cmd = command("redis_vrandmember", AccessMode::ReadOnly, "VRANDMEMBER");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(count) = input.count { cmd.arg(count.to_string()); }
                let raw = without_attributes(state.raw(cmd, "VRANDMEMBER failed").await?);
                let values = if input.count.is_some() {
                    binary_array(raw, "VRANDMEMBER")?
                } else {
                    vec![optional_binary(raw, "VRANDMEMBER")?]
                };
                let count = values.iter().filter(|value| value.is_some()).count();
                state.output_collection(&BinaryValuesOutput { count, values }, count, "Retry VRANDMEMBER with a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VrangeInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    start: BinaryInput,
    end: BinaryInput,
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VrangeOutput {
    count: usize,
    values: Vec<BinaryOutput>,
    complete: bool,
    /// Exclusive binary-safe start bound for the next page.
    next_start: Option<BinaryOutput>,
}

fn vrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vrange")
        .title("Range Redis Vector Members")
        .description("Return a bounded lexicographical range of binary-safe vector-set members.")
        .output_schema(output_schema::<VrangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VrangeInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut cmd = command("redis_vrange", AccessMode::ReadOnly, "VRANGE");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.decode("start")?)
                    .arg(input.end.decode("end")?)
                    .arg(input.count.to_string());
                let values =
                    required_binary_array(state.raw(cmd, "VRANGE failed").await?, "VRANGE")?;
                let count = values.len();
                let next_start = if count == input.count {
                    values
                        .last()
                        .map(|last| {
                            let raw = decode_input(
                                &last.value,
                                match last.encoding {
                                    ValueEncoding::Utf8 => InputEncoding::Utf8,
                                    ValueEncoding::Base64 => InputEncoding::Base64,
                                },
                                "VRANGE last member",
                            )?;
                            let mut bound = Vec::with_capacity(raw.len() + 1);
                            bound.push(b'(');
                            bound.extend(raw);
                            Ok::<BinaryOutput, tower_mcp::Error>(BinaryOutput::new(bound))
                        })
                        .transpose()?
                } else {
                    None
                };
                let complete = next_start.is_none();
                state.output_collection(
                    &VrangeOutput {
                        count,
                        values,
                        complete,
                        next_start,
                    },
                    count,
                    "Retry VRANGE with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum VectorQuery {
    Element {
        element: BinaryInput,
    },
    Values {
        #[schemars(length(min = 1, max = 65536))]
        values: Vec<ExactDecimalInput>,
    },
    Fp32 {
        data: BinaryInput,
    },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VsimInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    query: VectorQuery,
    #[serde(default)]
    with_scores: bool,
    #[serde(default)]
    with_attributes: bool,
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    #[serde(default)]
    max_distance: Option<ExactDecimalInput>,
    #[serde(default)]
    #[schemars(range(min = 1, max = 1000000))]
    search_exploration_factor: Option<usize>,
    #[serde(default)]
    filter: Option<String>,
    #[serde(default)]
    max_filtering_effort: Option<usize>,
    #[serde(default)]
    exact_truth: bool,
    #[serde(default)]
    no_thread: bool,
}

fn vsim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vsim")
        .title("Search Redis Vector Set")
        .description("Run one bounded vector similarity search by element, numeric values, or packed FP32 data, with typed search controls.")
        .output_schema(output_schema::<StructuredOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VsimInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut cmd = command("redis_vsim", AccessMode::ReadOnly, "VSIM");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                match &input.query {
                    VectorQuery::Element { element } => { cmd.arg("ELE").arg(element.decode("query.element")?); }
                    VectorQuery::Values { values } => {
                        if values.is_empty() || values.len() > MAX_VECTOR_DIMENSIONS { return Err(tower_mcp::Error::tool(format!("query.values must contain between 1 and {MAX_VECTOR_DIMENSIONS} dimensions"))); }
                        cmd.arg("VALUES").arg(values.len().to_string());
                        for (index, value) in values.iter().enumerate() { cmd.arg(value.token(&format!("query.values[{index}]"))?); }
                    }
                    VectorQuery::Fp32 { data } => {
                        let bytes = data.decode("query.data")?;
                        if bytes.is_empty() || bytes.len() % 4 != 0 || bytes.len() / 4 > MAX_VECTOR_DIMENSIONS { return Err(tower_mcp::Error::tool("query.data must contain 1 to 65536 packed FP32 values")); }
                        cmd.arg("FP32").arg(bytes);
                    }
                }
                if input.with_scores { cmd.arg("WITHSCORES"); }
                if input.with_attributes { cmd.arg("WITHATTRIBS"); }
                cmd.arg("COUNT").arg(input.count.to_string());
                if let Some(distance) = &input.max_distance {
                    let token = distance.token("max_distance")?;
                    if token.parse::<f64>().is_ok_and(|distance| distance <= 0.0) {
                        return Err(tower_mcp::Error::tool("max_distance must be greater than zero"));
                    }
                    cmd.arg("EPSILON").arg(token);
                }
                if let Some(factor) = input.search_exploration_factor { if factor == 0 || factor > MAX_VECTOR_EXPLORATION_FACTOR { return Err(tower_mcp::Error::tool(format!("search_exploration_factor must be between 1 and {MAX_VECTOR_EXPLORATION_FACTOR}"))); } cmd.arg("EF").arg(factor.to_string()); }
                if let Some(filter) = &input.filter { cmd.arg("FILTER").arg(filter.as_bytes()); }
                if let Some(effort) = input.max_filtering_effort { if effort == 0 || effort > i64::MAX as usize { return Err(tower_mcp::Error::tool("max_filtering_effort must be between 1 and the signed 64-bit maximum")); } cmd.arg("FILTER-EF").arg(effort.to_string()); }
                if input.exact_truth { cmd.arg("TRUTH"); }
                if input.no_thread { cmd.arg("NOTHREAD"); }
                let raw = state.raw(cmd, "VSIM failed").await?;
                let entries = redis_value_collection_entries(&raw);
                state.output_collection(&StructuredOutput { entries, result: redis_value_to_json(&raw) }, entries, "Retry VSIM with a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VsetattrInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    element: BinaryInput,
    /// JSON attributes, or null to remove existing attributes.
    #[serde(default)]
    attributes: Option<JsonValue>,
}

fn vsetattr_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vsetattr")
        .title("Set Redis Vector Attributes")
        .description(
            "Replace or remove the JSON attributes associated with one vector-set element.",
        )
        .output_schema(output_schema::<BooleanMutationOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VsetattrInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_vsetattr")?;
                let mut cmd = command("redis_vsetattr", AccessMode::ReadWrite, "VSETATTR");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                match input.attributes {
                    Some(attributes) => {
                        cmd.arg(serde_json::to_vec(&attributes).map_err(|error| {
                            tower_mcp::Error::tool(format!(
                                "attributes are not valid JSON: {error}"
                            ))
                        })?);
                    }
                    None => {
                        cmd.arg(Vec::<u8>::new());
                    }
                }
                let changed = redis_boolean(state.raw(cmd, "VSETATTR failed").await?, "VSETATTR")?;
                state.output(&BooleanMutationOutput { changed })
            },
        )
        .build()
}

fn vrem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vrem")
        .title("Remove Redis Vector Element")
        .description("Permanently remove one binary-safe vector-set element. Requires full access.")
        .output_schema(output_schema::<BooleanMutationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VectorElementInput>| async move {
                state.require(AccessMode::Full, "redis_vrem")?;
                let mut cmd = command("redis_vrem", AccessMode::Full, "VREM");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.element.decode("element")?);
                let changed = redis_boolean(state.raw(cmd, "VREM failed").await?, "VREM")?;
                state.output(&BooleanMutationOutput { changed })
            },
        )
        .build()
}

// Modern string, hash, list, and Stream deltas --------------------------

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum DelexCondition {
    #[serde(rename = "value_equals")]
    ValueMatch { value: BinaryInput },
    #[serde(rename = "value_not_equals")]
    ValueMismatch { value: BinaryInput },
    #[serde(rename = "digest_equals")]
    DigestMatch { digest: String },
    #[serde(rename = "digest_not_equals")]
    DigestMismatch { digest: String },
}

fn validate_digest(digest: &str) -> tower_mcp::Result<()> {
    if digest.len() != 16 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Err(tower_mcp::Error::tool(
            "digest must contain exactly 16 hexadecimal characters",
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DelexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    condition: Option<DelexCondition>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DeletedOutput {
    deleted: bool,
}

fn delex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_delex")
        .title("Conditionally Delete Redis String")
        .description("Permanently delete a Redis string only when an optional binary value or XXH3 digest condition passes. Requires full access.")
        .output_schema(output_schema::<DeletedOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DelexInput>| async move {
                state.require(AccessMode::Full, "redis_delex")?;
                let mut cmd = command("redis_delex", AccessMode::Full, "DELEX");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(condition) = &input.condition {
                    match condition {
                        DelexCondition::ValueMatch { value } => { cmd.arg("IFEQ").arg(value.decode("condition.value")?); }
                        DelexCondition::ValueMismatch { value } => { cmd.arg("IFNE").arg(value.decode("condition.value")?); }
                        DelexCondition::DigestMatch { digest } => { validate_digest(digest)?; cmd.arg("IFDEQ").arg(digest.as_bytes()); }
                        DelexCondition::DigestMismatch { digest } => { validate_digest(digest)?; cmd.arg("IFDNE").arg(digest.as_bytes()); }
                    }
                }
                let deleted = redis_boolean(state.raw(cmd, "DELEX failed").await?, "DELEX")?;
                state.output(&DeletedOutput { deleted })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DigestOutput {
    exists: bool,
    digest: Option<String>,
    algorithm: &'static str,
}

fn digest_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_digest")
        .title("Digest Redis String")
        .description("Return the 16-character hexadecimal XXH3-64 digest of one Redis string without retrieving its value.")
        .output_schema(output_schema::<DigestOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut cmd = command("redis_digest", AccessMode::ReadOnly, "DIGEST");
                cmd.arg(string_key(&input)?);
                let value = without_attributes(state.raw(cmd, "DIGEST failed").await?);
                let digest = if matches!(value, RedisValue::Nil) { None } else { Some(redis_text(value, "DIGEST")?) };
                state.output(&DigestOutput { exists: digest.is_some(), digest, algorithm: "xxh3-64-hex" })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldsInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<BinaryInput>,
}

fn append_hash_fields(
    cmd: &mut crate::RedisCommand,
    fields: &[BinaryInput],
) -> tower_mcp::Result<()> {
    cmd.arg("FIELDS").arg(fields.len().to_string());
    for (index, field) in fields.iter().enumerate() {
        cmd.arg(field.decode(&format!("fields[{index}]"))?);
    }
    Ok(())
}

fn hgetdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hgetdel")
        .title("Get and Delete Redis Hash Fields")
        .description("Return and permanently delete an explicit bounded set of binary-safe hash fields. Requires full access.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldsInput>| async move {
                state.require(AccessMode::Full, "redis_hgetdel")?;
                validate_items(&input.fields, "fields")?;
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let mut cmd = command("redis_hgetdel", AccessMode::Full, "HGETDEL");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                append_hash_fields(&mut cmd, &input.fields)?;
                let values = binary_array(state.raw(cmd, "HGETDEL failed").await?, "HGETDEL")?;
                state.output_collection(&BinaryValuesOutput { count: values.len(), values }, input.fields.len(), "Retry HGETDEL with fewer fields.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum GetFieldExpiration {
    Seconds { value: u64 },
    Milliseconds { value: u64 },
    UnixSeconds { value: u64 },
    UnixMilliseconds { value: u64 },
    Persist,
}

fn append_get_field_expiration(
    cmd: &mut crate::RedisCommand,
    expiration: &GetFieldExpiration,
) -> tower_mcp::Result<()> {
    match expiration {
        GetFieldExpiration::Seconds { value } => {
            if *value == 0 {
                return Err(tower_mcp::Error::tool(
                    "expiration must be greater than zero",
                ));
            }
            cmd.arg("EX").arg(value.to_string());
        }
        GetFieldExpiration::Milliseconds { value } => {
            if *value == 0 {
                return Err(tower_mcp::Error::tool(
                    "expiration must be greater than zero",
                ));
            }
            cmd.arg("PX").arg(value.to_string());
        }
        GetFieldExpiration::UnixSeconds { value } => {
            cmd.arg("EXAT").arg(value.to_string());
        }
        GetFieldExpiration::UnixMilliseconds { value } => {
            cmd.arg("PXAT").arg(value.to_string());
        }
        GetFieldExpiration::Persist => {
            cmd.arg("PERSIST");
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HgetexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    expiration: Option<GetFieldExpiration>,
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<BinaryInput>,
}

fn hgetex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hgetex")
        .title("Get and Expire Redis Hash Fields")
        .description("Read an explicit bounded set of hash fields and optionally atomically change their expiration.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HgetexInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hgetex")?;
                validate_items(&input.fields, "fields")?;
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let mut cmd = command("redis_hgetex", AccessMode::ReadWrite, "HGETEX");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(expiration) = &input.expiration { append_get_field_expiration(&mut cmd, expiration)?; }
                append_hash_fields(&mut cmd, &input.fields)?;
                let values = binary_array(state.raw(cmd, "HGETEX failed").await?, "HGETEX")?;
                state.output_collection(&BinaryValuesOutput { count: values.len(), values }, input.fields.len(), "Retry HGETEX with fewer fields.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashSetCondition {
    FieldsMustNotExist,
    FieldsMustExist,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum SetExpiration {
    Seconds { value: u64 },
    Milliseconds { value: u64 },
    UnixSeconds { value: u64 },
    UnixMilliseconds { value: u64 },
    KeepTtl,
}

fn append_set_expiration(
    cmd: &mut crate::RedisCommand,
    expiration: &SetExpiration,
) -> tower_mcp::Result<()> {
    match expiration {
        SetExpiration::Seconds { value } => {
            if *value == 0 {
                return Err(tower_mcp::Error::tool(
                    "expiration must be greater than zero",
                ));
            }
            cmd.arg("EX").arg(value.to_string());
        }
        SetExpiration::Milliseconds { value } => {
            if *value == 0 {
                return Err(tower_mcp::Error::tool(
                    "expiration must be greater than zero",
                ));
            }
            cmd.arg("PX").arg(value.to_string());
        }
        SetExpiration::UnixSeconds { value } => {
            cmd.arg("EXAT").arg(value.to_string());
        }
        SetExpiration::UnixMilliseconds { value } => {
            cmd.arg("PXAT").arg(value.to_string());
        }
        SetExpiration::KeepTtl => {
            cmd.arg("KEEPTTL");
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldValueInput {
    field: BinaryInput,
    value: BinaryInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HsetexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    condition: Option<HashSetCondition>,
    #[serde(default)]
    expiration: Option<SetExpiration>,
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldValueInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetAppliedOutput {
    applied: bool,
}

fn hsetex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hsetex")
        .title("Set Expiring Redis Hash Fields")
        .description("Atomically set a bounded group of binary-safe hash fields with an optional field condition and shared expiration.")
        .output_schema(output_schema::<SetAppliedOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HsetexInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hsetex")?;
                validate_items(&input.fields, "fields")?;
                let mut cmd = command("redis_hsetex", AccessMode::ReadWrite, "HSETEX");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(condition) = input.condition { cmd.arg(match condition { HashSetCondition::FieldsMustNotExist => "FNX", HashSetCondition::FieldsMustExist => "FXX" }); }
                if let Some(expiration) = &input.expiration { append_set_expiration(&mut cmd, expiration)?; }
                cmd.arg("FIELDS").arg(input.fields.len().to_string());
                for (index, field) in input.fields.iter().enumerate() {
                    cmd.arg(field.field.decode(&format!("fields[{index}].field"))?)
                        .arg(field.value.decode(&format!("fields[{index}].value"))?);
                }
                let applied = redis_boolean(state.raw(cmd, "HSETEX failed").await?, "HSETEX")?;
                state.output(&SetAppliedOutput { applied })
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum IncrementValue {
    Integer { value: String },
    Float { value: ExactDecimalInput },
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    increment: Option<IncrementValue>,
    #[serde(default)]
    saturate: bool,
    #[serde(default)]
    lower_bound: Option<String>,
    #[serde(default)]
    upper_bound: Option<String>,
    #[serde(default)]
    expiration: Option<GetFieldExpiration>,
    #[serde(default)]
    expiration_only_if_missing: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrexOutput {
    value: String,
    actual_increment: String,
}

fn increx_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_increx")
        .title("Increment and Expire Redis String")
        .description("Atomically increment a Redis numeric string with optional bounds, saturation, and expiration controls.")
        .output_schema(output_schema::<IncrexOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<IncrexInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_increx")?;
                if input.expiration_only_if_missing && !matches!(input.expiration, Some(GetFieldExpiration::Seconds { .. } | GetFieldExpiration::Milliseconds { .. } | GetFieldExpiration::UnixSeconds { .. } | GetFieldExpiration::UnixMilliseconds { .. })) {
                    return Err(tower_mcp::Error::tool("expiration_only_if_missing requires a non-PERSIST expiration"));
                }
                let integer_mode = !matches!(input.increment, Some(IncrementValue::Float { .. }));
                let validate_bound = |value: &str, name: &str| -> tower_mcp::Result<()> {
                    if integer_mode { value.parse::<i64>().map(|_| ()).map_err(|_| tower_mcp::Error::tool(format!("{name} must be a signed 64-bit integer"))) }
                    else { ExactDecimalInput::Exact(value.to_string()).token(name).map(|_| ()) }
                };
                let mut cmd = command("redis_increx", AccessMode::ReadWrite, "INCREX");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(increment) = &input.increment {
                    match increment {
                        IncrementValue::Integer { value } => { value.parse::<i64>().map_err(|_| tower_mcp::Error::tool("increment.value must be a signed 64-bit integer"))?; cmd.arg("BYINT").arg(value.as_bytes()); }
                        IncrementValue::Float { value } => { cmd.arg("BYFLOAT").arg(value.token("increment.value")?); }
                    }
                }
                if input.saturate { cmd.arg("SATURATE"); }
                if let Some(bound) = &input.lower_bound { validate_bound(bound, "lower_bound")?; cmd.arg("LBOUND").arg(bound.as_bytes()); }
                if let Some(bound) = &input.upper_bound { validate_bound(bound, "upper_bound")?; cmd.arg("UBOUND").arg(bound.as_bytes()); }
                if let Some(expiration) = &input.expiration { append_get_field_expiration(&mut cmd, expiration)?; }
                if input.expiration_only_if_missing { cmd.arg("ENX"); }
                let values = redis_array(state.raw(cmd, "INCREX failed").await?, "INCREX")?;
                if values.len() != 2 { return Err(tower_mcp::Error::tool("INCREX returned an invalid result pair")); }
                state.output(&IncrexOutput { value: redis_text(values[0].clone(), "INCREX")?, actual_increment: redis_text(values[1].clone(), "INCREX")? })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SetCondition {
    OnlyIfMissing,
    OnlyIfExisting,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyValueInput {
    key: BinaryInput,
    value: BinaryInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MsetexInput {
    #[schemars(length(min = 1, max = 1000))]
    entries: Vec<KeyValueInput>,
    #[serde(default)]
    condition: Option<SetCondition>,
    #[serde(default)]
    expiration: Option<SetExpiration>,
}

fn msetex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_msetex")
        .title("Set Expiring Redis Strings")
        .description("Atomically set a bounded group of binary-safe strings with an optional all-keys condition and shared expiration. Cluster keys must share a slot.")
        .output_schema(output_schema::<SetAppliedOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<MsetexInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_msetex")?;
                validate_items(&input.entries, "entries")?;
                let mut cmd = command("redis_msetex", AccessMode::ReadWrite, "MSETEX");
                cmd.arg(input.entries.len().to_string());
                for (index, entry) in input.entries.iter().enumerate() {
                    cmd.arg(entry.key.decode(&format!("entries[{index}].key"))?)
                        .arg(entry.value.decode(&format!("entries[{index}].value"))?);
                }
                if let Some(condition) = input.condition { cmd.arg(match condition { SetCondition::OnlyIfMissing => "NX", SetCondition::OnlyIfExisting => "XX" }); }
                if let Some(expiration) = &input.expiration { append_set_expiration(&mut cmd, expiration)?; }
                let applied = redis_boolean(state.raw(cmd, "MSETEX failed").await?, "MSETEX")?;
                state.output(&SetAppliedOutput { applied })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ListEnd {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum MoveOrdering {
    OneByOne,
    Bulk,
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LmovemInput {
    source: BinaryInput,
    destination: BinaryInput,
    from: ListEnd,
    to: ListEnd,
    #[serde(default)]
    amount: Option<MoveAmount>,
}

fn lmovem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lmovem")
        .title("Move Redis List Elements")
        .description("Move one or a bounded batch of elements between same-slot Redis lists. This removes source elements and requires full access.")
        .output_schema(output_schema::<BinaryValuesOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LmovemInput>| async move {
                state.require(AccessMode::Full, "redis_lmovem")?;
                let mut cmd = command("redis_lmovem", AccessMode::Full, "LMOVEM");
                cmd.arg(input.source.decode("source")?)
                    .arg(input.destination.decode("destination")?)
                    .arg(match input.from { ListEnd::Left => "LEFT", ListEnd::Right => "RIGHT" })
                    .arg(match input.to { ListEnd::Left => "LEFT", ListEnd::Right => "RIGHT" });
                if let Some(amount) = input.amount {
                    let (token, count, ordering) = match amount {
                        MoveAmount::UpTo { count, ordering } => ("COUNT", count, ordering),
                        MoveAmount::Exactly { count, ordering } => ("EXACTLY", count, ordering),
                    };
                    state.validate_requested_entries(count, "amount.count")?;
                    cmd.arg(token).arg(count.to_string()).arg(match ordering { MoveOrdering::OneByOne => "OBO", MoveOrdering::Bulk => "BULK" });
                }
                let raw = without_attributes(state.raw(cmd, "LMOVEM failed").await?);
                let values = if matches!(raw, RedisValue::Nil) { Vec::new() } else { required_binary_array(raw, "LMOVEM")?.into_iter().map(Some).collect() };
                let count = values.len();
                state.output_collection(&BinaryValuesOutput { count, values }, count, "Retry LMOVEM with a smaller count.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum StreamReferencePolicy {
    KeepReferences,
    DeleteReferences,
    OnlyIfAcknowledged,
}

impl StreamReferencePolicy {
    fn token(self) -> &'static str {
        match self {
            Self::KeepReferences => "KEEPREF",
            Self::DeleteReferences => "DELREF",
            Self::OnlyIfAcknowledged => "ACKED",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamId {
    milliseconds: u64,
    sequence: u64,
}

impl StreamId {
    fn token(self) -> String {
        format!("{}-{}", self.milliseconds, self.sequence)
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct StreamDeleteOutput {
    requested: usize,
    results: Vec<i64>,
}

fn stream_results(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<i64>> {
    redis_array(value, context)?
        .into_iter()
        .map(|value| redis_integer(value, context))
        .collect()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XackdelInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    #[serde(default)]
    reference_policy: Option<StreamReferencePolicy>,
    #[schemars(length(min = 1, max = 1000))]
    ids: Vec<StreamId>,
}

fn xackdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xackdel")
        .title("Acknowledge and Delete Redis Stream Entries")
        .description("Acknowledge and permanently delete a bounded set of stream entries under an explicit reference policy. Requires full access.")
        .output_schema(output_schema::<StreamDeleteOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XackdelInput>| async move {
                state.require(AccessMode::Full, "redis_xackdel")?;
                validate_items(&input.ids, "ids")?;
                state.validate_requested_entries(input.ids.len(), "ids")?;
                let mut cmd = command("redis_xackdel", AccessMode::Full, "XACKDEL");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.group.decode("group")?);
                if let Some(policy) = input.reference_policy { cmd.arg(policy.token()); }
                cmd.arg("IDS").arg(input.ids.len().to_string());
                for id in &input.ids { cmd.arg(id.token()); }
                let results = stream_results(state.raw(cmd, "XACKDEL failed").await?, "XACKDEL")?;
                state.output_collection(&StreamDeleteOutput { requested: input.ids.len(), results }, input.ids.len(), "Retry XACKDEL with fewer IDs.")
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XdelexInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(default)]
    reference_policy: Option<StreamReferencePolicy>,
    #[schemars(length(min = 1, max = 1000))]
    ids: Vec<StreamId>,
}

fn xdelex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xdelex")
        .title("Delete Redis Stream Entries with References")
        .description("Permanently delete a bounded set of stream entries under an explicit consumer-group reference policy. Requires full access.")
        .output_schema(output_schema::<StreamDeleteOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XdelexInput>| async move {
                state.require(AccessMode::Full, "redis_xdelex")?;
                validate_items(&input.ids, "ids")?;
                state.validate_requested_entries(input.ids.len(), "ids")?;
                let mut cmd = command("redis_xdelex", AccessMode::Full, "XDELEX");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?);
                if let Some(policy) = input.reference_policy { cmd.arg(policy.token()); }
                cmd.arg("IDS").arg(input.ids.len().to_string());
                for id in &input.ids { cmd.arg(id.token()); }
                let results = stream_results(state.raw(cmd, "XDELEX failed").await?, "XDELEX")?;
                state.output_collection(&StreamDeleteOutput { requested: input.ids.len(), results }, input.ids.len(), "Retry XDELEX with fewer IDs.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum NackMode {
    Silent,
    Fail,
    Fatal,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XnackInput {
    key: String,
    #[serde(default)]
    key_encoding: InputEncoding,
    group: BinaryInput,
    mode: NackMode,
    #[schemars(length(min = 1, max = 1000))]
    ids: Vec<StreamId>,
    #[serde(default)]
    retry_count: Option<u64>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct XnackOutput {
    requested: usize,
    released: u64,
}

fn xnack_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_xnack")
        .title("Release Redis Stream Claims")
        .description("Release a bounded set of claimed messages back to a consumer group's pending-entry list. Requires full access.")
        .output_schema(output_schema::<XnackOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<XnackInput>| async move {
                state.require(AccessMode::Full, "redis_xnack")?;
                validate_items(&input.ids, "ids")?;
                let mut cmd = command("redis_xnack", AccessMode::Full, "XNACK");
                cmd.arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.group.decode("group")?)
                    .arg(match input.mode { NackMode::Silent => "SILENT", NackMode::Fail => "FAIL", NackMode::Fatal => "FATAL" })
                    .arg("IDS")
                    .arg(input.ids.len().to_string());
                for id in &input.ids { cmd.arg(id.token()); }
                if let Some(count) = input.retry_count { cmd.arg("RETRYCOUNT").arg(count.to_string()); }
                if input.force { cmd.arg("FORCE"); }
                let released = redis_nonnegative(state.raw(cmd, "XNACK failed").await?, "XNACK")?;
                state.output(&XnackOutput { requested: input.ids.len(), released })
            },
        )
        .build()
}

#[cfg(feature = "arrays")]
pub(super) fn add_array_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(array_count_tool(
        state.clone(),
        "redis_arcount",
        "Count Redis Array Elements",
        "Return the number of allocated (non-empty) elements in a Redis Array.",
        "ARCOUNT",
    ));
    router = router.tool(arget_tool(state.clone()));
    router = router.tool(argetrange_tool(state.clone()));
    router = router.tool(argrep_tool(state.clone()));
    router = router.tool(arinfo_tool(state.clone()));
    router = router.tool(arlastitems_tool(state.clone()));
    router = router.tool(array_count_tool(
        state.clone(),
        "redis_arlen",
        "Read Redis Array Length",
        "Return a Redis Array's logical length (highest index plus one).",
        "ARLEN",
    ));
    router = router.tool(armget_tool(state.clone()));
    router = router.tool(arnext_tool(state.clone()));
    router = router.tool(arop_tool(state.clone()));
    router = router.tool(arscan_tool(state.clone()));
    router
}

#[cfg(feature = "strings")]
pub(super) fn add_string_read_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(digest_tool(state))
}

#[cfg(feature = "vector-sets")]
pub(super) fn add_vector_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(vector_integer_tool(
        state.clone(),
        "redis_vcard",
        "Count Redis Vector Members",
        "Return the number of elements in a Redis vector set.",
        "VCARD",
    ));
    router = router.tool(vector_integer_tool(
        state.clone(),
        "redis_vdim",
        "Read Redis Vector Dimensions",
        "Return the vector dimension configured for a Redis vector set.",
        "VDIM",
    ));
    router = router.tool(vemb_tool(state.clone()));
    router = router.tool(vgetattr_tool(state.clone()));
    router = router.tool(vinfo_tool(state.clone()));
    router = router.tool(vismember_tool(state.clone()));
    router = router.tool(vlinks_tool(state.clone()));
    router = router.tool(vrandmember_tool(state.clone()));
    router = router.tool(vrange_tool(state.clone()));
    router.tool(vsim_tool(state))
}

#[cfg(feature = "arrays")]
pub(super) fn add_array_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(arinsert_tool(state.clone()));
    router = router.tool(armset_tool(state.clone()));
    router = router.tool(arring_tool(state.clone()));
    router = router.tool(arseek_tool(state.clone()));
    router = router.tool(arset_tool(state.clone()));
    router
}

#[cfg(feature = "vector-sets")]
pub(super) fn add_vector_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(vadd_tool(state.clone()));
    router = router.tool(vsetattr_tool(state.clone()));
    router
}

#[cfg(feature = "hashes")]
pub(super) fn add_hash_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hgetex_tool(state.clone()));
    router = router.tool(hsetex_tool(state.clone()));
    router
}

#[cfg(feature = "strings")]
pub(super) fn add_string_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(increx_tool(state.clone()));
    router.tool(msetex_tool(state))
}

#[cfg(feature = "arrays")]
pub(super) fn add_array_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(ardel_tool(state.clone()));
    router = router.tool(ardelrange_tool(state.clone()));
    router
}

#[cfg(feature = "strings")]
pub(super) fn add_string_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(delex_tool(state))
}

#[cfg(feature = "hashes")]
pub(super) fn add_hash_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(hgetdel_tool(state))
}

#[cfg(feature = "lists")]
pub(super) fn add_list_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(lmovem_tool(state))
}

#[cfg(feature = "vector-sets")]
pub(super) fn add_vector_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(vrem_tool(state))
}

#[cfg(feature = "streams")]
pub(super) fn add_stream_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(xackdel_tool(state.clone()));
    router = router.tool(xdelex_tool(state.clone()));
    router.tool(xnack_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_and_range_validation_is_bounded() {
        assert!(inclusive_range_len(0, 999, "range").is_ok());
        assert_eq!(inclusive_range_len(1, 0, "range").unwrap(), 2);
        assert!(inclusive_range_len(0, u64::MAX, "range").is_err());
        assert_eq!(
            redis_nonnegative(
                RedisValue::BigNumber((u64::MAX - 1).to_string().into_bytes()),
                "large array index"
            )
            .unwrap(),
            u64::MAX - 1
        );
        assert_eq!(
            parse_array_bound("+", "bound").unwrap(),
            ArrayBound::Maximum
        );
        assert!(parse_array_bound(&u64::MAX.to_string(), "bound").is_err());
        assert!(validate_digest("0123456789abcdef").is_ok());
        assert!(validate_digest("0123456789abcdeg").is_err());
        assert!(
            VectorInput::Fp32 {
                data: BinaryInput {
                    value: "AAAAAA==".into(),
                    encoding: InputEncoding::Base64
                }
            }
            .dimensions()
            .is_ok()
        );
        assert!(
            VectorInput::Fp32 {
                data: BinaryInput {
                    value: "AAA=".into(),
                    encoding: InputEncoding::Base64
                }
            }
            .dimensions()
            .is_err()
        );
    }
}
