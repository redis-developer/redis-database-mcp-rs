//! Curated hash, list, set, and sorted-set operations.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    PageMetadata, ToolState, ValueEncoding, command, output_schema, read_annotations,
    write_annotations,
};
use crate::AccessMode;

const MAX_ITEMS: usize = 1_000;
type BinaryPair = (Vec<u8>, Vec<u8>);

fn validate_items(items: &[impl Sized], name: &str) -> tower_mcp::Result<()> {
    if items.is_empty() || items.len() > MAX_ITEMS {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_ITEMS} items"
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedValue {
    value: String,
    encoding: ValueEncoding,
}

impl From<Vec<u8>> for EncodedValue {
    fn from(bytes: Vec<u8>) -> Self {
        let (value, encoding) = super::encode_bytes(bytes);
        Self { value, encoding }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldInput {
    /// Redis hash key.
    key: String,
    /// Hash field.
    field: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HgetOutput {
    key: String,
    field: String,
    exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

fn hget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hget")
        .title("Get Redis Hash Field")
        .description("Read one Redis hash field. Binary values are returned as base64.")
        .output_schema(output_schema::<HgetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldInput>| async move {
                let mut command = command("redis_hget", AccessMode::ReadOnly, "HGET");
                command.arg(input.key.as_str()).arg(input.field.as_str());
                let value: Option<Vec<u8>> = state.query(command, "HGET failed").await?;
                let (value, encoding) = match value {
                    Some(bytes) => {
                        let (value, encoding) = super::encode_bytes(bytes);
                        (Some(value), Some(encoding))
                    }
                    None => (None, None),
                };
                state.output(&HgetOutput {
                    key: input.key,
                    field: input.field,
                    exists: value.is_some(),
                    value,
                    encoding,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyInput {
    /// Redis key.
    key: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashEntry {
    field: String,
    field_encoding: ValueEncoding,
    value: String,
    value_encoding: ValueEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HgetallOutput {
    key: String,
    exists: bool,
    count: usize,
    entries: Vec<HashEntry>,
}

fn hgetall_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hgetall")
        .title("Get Redis Hash")
        .description(
            "Read all fields and values in a Redis hash with binary-safe encodings. The configured output budget is enforced; use redis_hscan for large hashes.",
        )
        .output_schema(output_schema::<HgetallOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = command("redis_hgetall", AccessMode::ReadOnly, "HGETALL");
                command.arg(input.key.as_str());
                let mut values: Vec<BinaryPair> =
                    state.query(command, "HGETALL failed").await?;
                values.sort_by(|left, right| left.0.cmp(&right.0));
                let entries = values
                    .into_iter()
                    .map(|(field, value)| {
                        let (field, field_encoding) = super::encode_bytes(field);
                        let (value, value_encoding) = super::encode_bytes(value);
                        HashEntry {
                            field,
                            field_encoding,
                            value,
                            value_encoding,
                        }
                    })
                    .collect::<Vec<_>>();
                let output = HgetallOutput {
                    key: input.key,
                    exists: !entries.is_empty(),
                    count: entries.len(),
                    entries,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Use redis_hscan to read the hash incrementally.",
                )
            },
        )
        .build()
}

fn default_pattern() -> String {
    "*".to_string()
}

fn default_scan_count() -> usize {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CollectionScanInput {
    /// Redis key containing the collection.
    key: String,
    /// Cursor returned by the previous page. Start with zero.
    #[serde(default)]
    cursor: u64,
    /// Glob-style field or member pattern.
    #[serde(default = "default_pattern")]
    pattern: String,
    /// Approximate number of fields or members Redis should inspect.
    #[serde(default = "default_scan_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HscanOutput {
    key: String,
    cursor: u64,
    count: usize,
    entries: Vec<HashEntry>,
    page: PageMetadata,
}

fn hscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hscan")
        .title("Scan Redis Hash")
        .description(
            "Read one bounded HSCAN page. Pass page.continuation.cursor as cursor until page.complete is true. Fields and values are binary-safe.",
        )
        .output_schema(output_schema::<HscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<CollectionScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut command = command("redis_hscan", AccessMode::ReadOnly, "HSCAN");
                command
                    .arg(input.key.as_str())
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                let (cursor, values): (u64, Vec<BinaryPair>) =
                    state.query(command, "HSCAN failed").await?;
                let entries = values
                    .into_iter()
                    .map(|(field, value)| {
                        let (field, field_encoding) = super::encode_bytes(field);
                        let (value, value_encoding) = super::encode_bytes(value);
                        HashEntry {
                            field,
                            field_encoding,
                            value,
                            value_encoding,
                        }
                    })
                    .collect::<Vec<_>>();
                let output = HscanOutput {
                    key: input.key,
                    cursor,
                    count: entries.len(),
                    page: PageMetadata::cursor(input.count, entries.len(), cursor),
                    entries,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry HSCAN with a smaller count and the same cursor.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SscanOutput {
    key: String,
    cursor: u64,
    count: usize,
    members: Vec<EncodedValue>,
    page: PageMetadata,
}

fn sscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sscan")
        .title("Scan Redis Set")
        .description(
            "Read one bounded SSCAN page. Pass page.continuation.cursor as cursor until page.complete is true. Members are binary-safe.",
        )
        .output_schema(output_schema::<SscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<CollectionScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut command = command("redis_sscan", AccessMode::ReadOnly, "SSCAN");
                command
                    .arg(input.key.as_str())
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                let (cursor, values): (u64, Vec<Vec<u8>>) =
                    state.query(command, "SSCAN failed").await?;
                let members = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = SscanOutput {
                    key: input.key,
                    cursor,
                    count: members.len(),
                    page: PageMetadata::cursor(input.count, members.len(), cursor),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry SSCAN with a smaller count and the same cursor.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZscanEntry {
    member: String,
    encoding: ValueEncoding,
    score: f64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZscanOutput {
    key: String,
    cursor: u64,
    count: usize,
    members: Vec<ZscanEntry>,
    page: PageMetadata,
}

fn zscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zscan")
        .title("Scan Redis Sorted Set")
        .description(
            "Read one bounded ZSCAN page. Pass page.continuation.cursor as cursor until page.complete is true. Members are binary-safe and include scores.",
        )
        .output_schema(output_schema::<ZscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<CollectionScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut command = command("redis_zscan", AccessMode::ReadOnly, "ZSCAN");
                command
                    .arg(input.key.as_str())
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                let (cursor, values): (u64, Vec<(Vec<u8>, f64)>) =
                    state.query(command, "ZSCAN failed").await?;
                let members = values
                    .into_iter()
                    .map(|(member, score)| {
                        let member = EncodedValue::from(member);
                        ZscanEntry {
                            member: member.value,
                            encoding: member.encoding,
                            score,
                        }
                    })
                    .collect::<Vec<_>>();
                let output = ZscanOutput {
                    key: input.key,
                    cursor,
                    count: members.len(),
                    page: PageMetadata::cursor(input.count, members.len(), cursor),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry ZSCAN with a smaller count and the same cursor.",
                )
            },
        )
        .build()
}

fn default_stop() -> i64 {
    99
}

fn validate_range(start: i64, stop: i64, limit: usize) -> tower_mcp::Result<usize> {
    if (start < 0) != (stop < 0) {
        return Err(tower_mcp::Error::tool(
            "start and stop must both be non-negative or both be negative so the response is bounded",
        ));
    }
    if stop < start {
        return Ok(0);
    }
    let requested = stop
        .checked_sub(start)
        .and_then(|span| span.checked_add(1))
        .and_then(|span| usize::try_from(span).ok())
        .ok_or_else(|| tower_mcp::Error::tool("requested range is too large"))?;
    if requested > limit {
        Err(tower_mcp::Error::tool(format!(
            "requested range contains {requested} entries; configured output limit is {limit}"
        )))
    } else {
        Ok(requested)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RangeInput {
    /// Redis key.
    key: String,
    /// Zero-based inclusive start index.
    #[serde(default)]
    start: i64,
    /// Inclusive stop index. Negative indexes address from the end.
    #[serde(default = "default_stop")]
    stop: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LrangeOutput {
    key: String,
    start: i64,
    stop: i64,
    count: usize,
    elements: Vec<EncodedValue>,
    page: PageMetadata,
}

fn lrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lrange")
        .title("Read Redis List Range")
        .description(
            "Read a bounded inclusive range from a Redis list. Defaults to ranks 0 through 99. Follow page.continuation.start until page.complete is true. Binary elements are base64.",
        )
        .output_schema(output_schema::<LrangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<RangeInput>| async move {
                let requested = validate_range(
                    input.start,
                    input.stop,
                    state.max_collection_entries(),
                )?;
                let fetch_stop = if input.start >= 0 && requested > 0 {
                    input.stop.checked_add(1).unwrap_or(input.stop)
                } else {
                    input.stop
                };
                let mut command = command("redis_lrange", AccessMode::ReadOnly, "LRANGE");
                command
                    .arg(input.key.as_str())
                    .arg(input.start.to_string())
                    .arg(fetch_stop.to_string());
                let mut values: Vec<Vec<u8>> = state.query(command, "LRANGE failed").await?;
                let has_more = input.start >= 0 && requested > 0 && values.len() > requested;
                values.truncate(requested);
                let elements = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let next_start = has_more.then(|| input.stop.saturating_add(1));
                let output = LrangeOutput {
                    key: input.key,
                    start: input.start,
                    stop: input.stop,
                    count: elements.len(),
                    page: PageMetadata::range(requested, elements.len(), next_start),
                    elements,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry LRANGE with a smaller start/stop span.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SmembersOutput {
    key: String,
    exists: bool,
    count: usize,
    members: Vec<EncodedValue>,
}

fn smembers_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_smembers")
        .title("Read Redis Set")
        .description(
            "Read all Redis set members in deterministic byte order. The configured output budget is enforced; use redis_sscan for large sets. Binary members are base64.",
        )
        .output_schema(output_schema::<SmembersOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = command("redis_smembers", AccessMode::ReadOnly, "SMEMBERS");
                command.arg(input.key.as_str());
                let mut values: Vec<Vec<u8>> = state.query(command, "SMEMBERS failed").await?;
                values.sort_unstable();
                let members = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = SmembersOutput {
                    key: input.key,
                    exists: !members.is_empty(),
                    count: members.len(),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Use redis_sscan to read the set incrementally.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeInput {
    /// Redis sorted-set key.
    key: String,
    /// Zero-based inclusive start rank.
    #[serde(default)]
    start: i64,
    /// Inclusive stop rank. Negative indexes address from the end.
    #[serde(default = "default_stop")]
    stop: i64,
    /// Include scores in the response.
    #[serde(default)]
    withscores: bool,
    /// Return highest scores first.
    #[serde(default)]
    rev: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeEntry {
    member: String,
    encoding: ValueEncoding,
    score: Option<f64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeOutput {
    key: String,
    start: i64,
    stop: i64,
    rev: bool,
    count: usize,
    members: Vec<ZrangeEntry>,
    page: PageMetadata,
}

fn zrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zrange")
        .title("Read Redis Sorted Set Range")
        .description(
            "Read a bounded rank range from a Redis sorted set, optionally with scores. Defaults to ranks 0 through 99. Follow page.continuation.start until page.complete is true.",
        )
        .output_schema(output_schema::<ZrangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZrangeInput>| async move {
                let requested = validate_range(
                    input.start,
                    input.stop,
                    state.max_collection_entries(),
                )?;
                let fetch_stop = if input.start >= 0 && requested > 0 {
                    input.stop.checked_add(1).unwrap_or(input.stop)
                } else {
                    input.stop
                };
                let mut command = command("redis_zrange", AccessMode::ReadOnly, "ZRANGE");
                command
                    .arg(input.key.as_str())
                    .arg(input.start.to_string())
                    .arg(fetch_stop.to_string());
                if input.rev {
                    command.arg("REV");
                }
                let mut members = if input.withscores {
                    command.arg("WITHSCORES");
                    let values: Vec<(Vec<u8>, f64)> = state.query(command, "ZRANGE failed").await?;
                    values
                        .into_iter()
                        .map(|(member, score)| {
                            let member = EncodedValue::from(member);
                            ZrangeEntry {
                                member: member.value,
                                encoding: member.encoding,
                                score: Some(score),
                            }
                        })
                        .collect::<Vec<_>>()
                } else {
                    let values: Vec<Vec<u8>> = state.query(command, "ZRANGE failed").await?;
                    values
                        .into_iter()
                        .map(|member| {
                            let member = EncodedValue::from(member);
                            ZrangeEntry {
                                member: member.value,
                                encoding: member.encoding,
                                score: None,
                            }
                        })
                        .collect::<Vec<_>>()
                };
                let has_more = input.start >= 0 && requested > 0 && members.len() > requested;
                members.truncate(requested);
                let next_start = has_more.then(|| input.stop.saturating_add(1));
                let output = ZrangeOutput {
                    key: input.key,
                    start: input.start,
                    stop: input.stop,
                    rev: input.rev,
                    count: members.len(),
                    page: PageMetadata::range(requested, members.len(), next_start),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry ZRANGE with a smaller start/stop span.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HsetInput {
    /// Redis hash key.
    key: String,
    /// Field-value pairs to set.
    #[schemars(length(min = 1, max = 1000))]
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HsetOutput {
    key: String,
    fields_set: usize,
    fields_added: u64,
}

fn hset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hset")
        .title("Set Redis Hash Fields")
        .description("Set between 1 and 1000 UTF-8 field-value pairs in a Redis hash.")
        .output_schema(output_schema::<HsetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HsetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hset")?;
                validate_items(&input.fields.iter().collect::<Vec<_>>(), "fields")?;
                let fields_set = input.fields.len();
                let mut command = command("redis_hset", AccessMode::ReadWrite, "HSET");
                command.arg(input.key.as_str());
                for (field, value) in input.fields {
                    command.arg(field).arg(value);
                }
                let fields_added = state.query(command, "HSET failed").await?;
                state.output(&HsetOutput {
                    key: input.key,
                    fields_set,
                    fields_added,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListPushInput {
    /// Redis list key.
    key: String,
    /// UTF-8 elements to push to the head in argument order.
    #[schemars(length(min = 1, max = 1000))]
    elements: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LpushOutput {
    key: String,
    pushed: usize,
    length: u64,
}

fn lpush_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lpush")
        .title("Push Redis List Elements")
        .description("Push between 1 and 1000 UTF-8 elements to the head of a Redis list.")
        .output_schema(output_schema::<LpushOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ListPushInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_lpush")?;
                validate_items(&input.elements, "elements")?;
                let pushed = input.elements.len();
                let mut command = command("redis_lpush", AccessMode::ReadWrite, "LPUSH");
                command.arg(input.key.as_str()).args(input.elements);
                let length = state.query(command, "LPUSH failed").await?;
                state.output(&LpushOutput {
                    key: input.key,
                    pushed,
                    length,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetAddInput {
    /// Redis set key.
    key: String,
    /// UTF-8 members to add.
    #[schemars(length(min = 1, max = 1000))]
    members: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SaddOutput {
    key: String,
    requested: usize,
    added: u64,
}

fn sadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sadd")
        .title("Add Redis Set Members")
        .description("Add between 1 and 1000 UTF-8 members to a Redis set.")
        .output_schema(output_schema::<SaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetAddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_sadd")?;
                validate_items(&input.members, "members")?;
                let requested = input.members.len();
                let mut command = command("redis_sadd", AccessMode::ReadWrite, "SADD");
                command.arg(input.key.as_str()).args(input.members);
                let added = state.query(command, "SADD failed").await?;
                state.output(&SaddOutput {
                    key: input.key,
                    requested,
                    added,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScoreMember {
    /// Finite score.
    score: f64,
    /// UTF-8 member.
    member: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZaddInput {
    /// Redis sorted-set key.
    key: String,
    /// Score-member pairs to add or update.
    #[schemars(length(min = 1, max = 1000))]
    members: Vec<ScoreMember>,
    /// Add new members only.
    #[serde(default)]
    nx: bool,
    /// Update existing members only.
    #[serde(default)]
    xx: bool,
    /// Update only when the new score is greater.
    #[serde(default)]
    gt: bool,
    /// Update only when the new score is less.
    #[serde(default)]
    lt: bool,
    /// Report both added and changed members.
    #[serde(default)]
    ch: bool,
}

impl ZaddInput {
    fn validate(&self) -> tower_mcp::Result<()> {
        validate_items(&self.members, "members")?;
        if self.nx && self.xx {
            return Err(tower_mcp::Error::tool("nx and xx are mutually exclusive"));
        }
        if self.gt && self.lt {
            return Err(tower_mcp::Error::tool("gt and lt are mutually exclusive"));
        }
        if self.nx && (self.gt || self.lt) {
            return Err(tower_mcp::Error::tool(
                "nx cannot be combined with gt or lt",
            ));
        }
        if self.members.iter().any(|member| !member.score.is_finite()) {
            return Err(tower_mcp::Error::tool("scores must be finite numbers"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZaddOutput {
    key: String,
    requested: usize,
    affected: u64,
    reports_changed: bool,
}

fn zadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zadd")
        .title("Add Redis Sorted Set Members")
        .description("Add or update scored UTF-8 members in a Redis sorted set.")
        .output_schema(output_schema::<ZaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_zadd")?;
                input.validate()?;
                let requested = input.members.len();
                let mut command = command("redis_zadd", AccessMode::ReadWrite, "ZADD");
                command.arg(input.key.as_str());
                if input.nx {
                    command.arg("NX");
                }
                if input.xx {
                    command.arg("XX");
                }
                if input.gt {
                    command.arg("GT");
                }
                if input.lt {
                    command.arg("LT");
                }
                if input.ch {
                    command.arg("CH");
                }
                for member in input.members {
                    command.arg(member.score.to_string()).arg(member.member);
                }
                let affected = state.query(command, "ZADD failed").await?;
                state.output(&ZaddOutput {
                    key: input.key,
                    requested,
                    affected,
                    reports_changed: input.ch,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hget_tool(state.clone()));
    router = router.tool(hgetall_tool(state.clone()));
    router = router.tool(hscan_tool(state.clone()));
    router = router.tool(lrange_tool(state.clone()));
    router = router.tool(smembers_tool(state.clone()));
    router = router.tool(sscan_tool(state.clone()));
    router = router.tool(zrange_tool(state.clone()));
    router.tool(zscan_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hset_tool(state.clone()));
    router = router.tool(lpush_tool(state.clone()));
    router = router.tool(sadd_tool(state.clone()));
    router.tool(zadd_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zadd_input() -> ZaddInput {
        ZaddInput {
            key: "leaders".into(),
            members: vec![ScoreMember {
                score: 1.0,
                member: "alice".into(),
            }],
            nx: false,
            xx: false,
            gt: false,
            lt: false,
            ch: false,
        }
    }

    #[test]
    fn zadd_rejects_conflicting_flags() {
        let mut input = zadd_input();
        input.nx = true;
        input.xx = true;
        assert!(input.validate().is_err());

        let mut input = zadd_input();
        input.gt = true;
        input.lt = true;
        assert!(input.validate().is_err());

        let mut input = zadd_input();
        input.nx = true;
        input.gt = true;
        assert!(input.validate().is_err());
    }
}
