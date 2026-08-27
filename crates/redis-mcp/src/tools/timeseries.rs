//! Optional RedisTimeSeries sample, metadata, and query operations.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    PageMetadata, ToolState, ValueEncoding, destructive_annotations, encode_bytes, module_command,
    output_schema, read_annotations, redis_value_to_json, write_annotations,
};
use crate::{AccessMode, RedisCommand, RedisModule, RedisValue, RedisVersion};

const MAX_TS_LABELS: usize = 64;
const MAX_TS_FILTERS: usize = 16;
const MAX_FILTER_BYTES: usize = 512;
const MAX_MADD_SAMPLES: usize = 100;
const MAX_TIMESTAMP_FILTERS: usize = 128;
const DEFAULT_RANGE_COUNT: usize = 100;
const MAX_RANGE_COUNT: usize = 1_000;
const MAX_TIMESTAMP_TOKEN_BYTES: usize = 20;

fn require_timeseries_version(
    state: &ToolState,
    minimum: RedisVersion,
    feature: &str,
) -> tower_mcp::Result<()> {
    if let Some(version) = state.module_version(RedisModule::TimeSeries)
        && version < minimum
    {
        return Err(tower_mcp::Error::tool(format!(
            "{feature} requires RedisTimeSeries {minimum} or newer; target reports {version}"
        )));
    }
    Ok(())
}

/// Validate one Redis time-series timestamp token without changing it.
///
/// RedisTimeSeries distinguishes exact integer milliseconds from the special
/// `*`, `-`, and `+` tokens, so inputs stay strings and are never coerced
/// through floating point.
fn validate_timestamp_token(token: &str, special: &[&str], name: &str) -> tower_mcp::Result<()> {
    if special.contains(&token) {
        return Ok(());
    }
    if token.is_empty()
        || token.len() > MAX_TIMESTAMP_TOKEN_BYTES
        || !token.bytes().all(|byte| byte.is_ascii_digit())
    {
        let mut accepted = special.join("`, `");
        if !accepted.is_empty() {
            accepted = format!(" or `{accepted}`");
        }
        return Err(tower_mcp::Error::tool(format!(
            "{name} must be an unsigned integer Unix millisecond timestamp{accepted}"
        )));
    }
    Ok(())
}

/// Render one finite sample value exactly as Redis will store it.
fn format_sample_value(value: f64, name: &str) -> tower_mcp::Result<String> {
    if !value.is_finite() {
        return Err(tower_mcp::Error::tool(format!(
            "{name} must be a finite number"
        )));
    }
    Ok(format!("{value}"))
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum TsEncoding {
    #[serde(rename = "COMPRESSED")]
    Compressed,
    #[serde(rename = "UNCOMPRESSED")]
    Uncompressed,
}

impl TsEncoding {
    fn as_str(self) -> &'static str {
        match self {
            Self::Compressed => "COMPRESSED",
            Self::Uncompressed => "UNCOMPRESSED",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum TsDuplicatePolicy {
    #[serde(rename = "BLOCK")]
    Block,
    #[serde(rename = "FIRST")]
    First,
    #[serde(rename = "LAST")]
    Last,
    #[serde(rename = "MIN")]
    Min,
    #[serde(rename = "MAX")]
    Max,
    #[serde(rename = "SUM")]
    Sum,
}

impl TsDuplicatePolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Block => "BLOCK",
            Self::First => "FIRST",
            Self::Last => "LAST",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Sum => "SUM",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum TsAggregation {
    #[serde(rename = "avg")]
    Avg,
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "range")]
    Range,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "first")]
    First,
    #[serde(rename = "last")]
    Last,
    #[serde(rename = "std.p")]
    StdP,
    #[serde(rename = "std.s")]
    StdS,
    #[serde(rename = "var.p")]
    VarP,
    #[serde(rename = "var.s")]
    VarS,
    #[serde(rename = "twa")]
    Twa,
}

impl TsAggregation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Range => "range",
            Self::Count => "count",
            Self::First => "first",
            Self::Last => "last",
            Self::StdP => "std.p",
            Self::StdS => "std.s",
            Self::VarP => "var.p",
            Self::VarS => "var.s",
            Self::Twa => "twa",
        }
    }

    fn minimum_version(self) -> Option<RedisVersion> {
        match self {
            Self::Twa => Some(RedisVersion::new(1, 8, 0)),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum TsBucketTimestamp {
    #[serde(rename = "start")]
    Start,
    #[serde(rename = "end")]
    End,
    #[serde(rename = "mid")]
    Mid,
}

impl TsBucketTimestamp {
    fn as_str(self) -> &'static str {
        match self {
            Self::Start => "-",
            Self::End => "+",
            Self::Mid => "~",
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsLabelInput {
    /// Label name.
    #[schemars(length(min = 1, max = 128))]
    name: String,
    /// Label value.
    #[schemars(length(max = 512))]
    value: String,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsIgnoreInput {
    /// Maximum timestamp difference for ignoring a near-duplicate insertion,
    /// in milliseconds.
    max_time_diff_ms: u64,
    /// Maximum absolute value difference for ignoring a near-duplicate
    /// insertion.
    max_value_diff: f64,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsSeriesOptionsInput {
    /// Sample retention in milliseconds. Zero keeps samples forever.
    #[serde(default)]
    retention_ms: Option<u64>,
    /// Chunk encoding for new chunks.
    #[serde(default)]
    encoding: Option<TsEncoding>,
    /// Initial chunk allocation in bytes (a multiple of 8, 48 to 1048576).
    #[serde(default)]
    #[schemars(range(min = 48, max = 1048576))]
    chunk_size: Option<u64>,
    /// Policy for samples that collide with an existing timestamp.
    #[serde(default)]
    duplicate_policy: Option<TsDuplicatePolicy>,
    /// Labels attached to the series for multi-series filtering.
    #[serde(default)]
    #[schemars(length(max = 64))]
    labels: Option<Vec<TsLabelInput>>,
    /// Ignore near-duplicate insertions within these deltas. Requires
    /// RedisTimeSeries 1.12 or newer.
    #[serde(default)]
    ignore: Option<TsIgnoreInput>,
}

impl TsSeriesOptionsInput {
    fn validate(&self, state: &ToolState) -> tower_mcp::Result<()> {
        if let Some(labels) = &self.labels
            && labels.len() > MAX_TS_LABELS
        {
            return Err(tower_mcp::Error::tool(format!(
                "labels must contain at most {MAX_TS_LABELS} entries"
            )));
        }
        if let Some(ignore) = &self.ignore {
            require_timeseries_version(state, RedisVersion::new(1, 12, 0), "IGNORE")?;
            if !ignore.max_value_diff.is_finite() || ignore.max_value_diff < 0.0 {
                return Err(tower_mcp::Error::tool(
                    "ignore.max_value_diff must be a finite non-negative number",
                ));
            }
        }
        Ok(())
    }

    /// Append the shared series configuration tokens in Redis argument order.
    ///
    /// `duplicate_token` selects `DUPLICATE_POLICY` for creation forms or
    /// `ON_DUPLICATE` for TS.ADD; `None` rejects a configured policy.
    fn apply(
        &self,
        command: &mut RedisCommand,
        duplicate_token: Option<&'static str>,
    ) -> tower_mcp::Result<()> {
        if let Some(retention_ms) = self.retention_ms {
            command.arg("RETENTION").arg(retention_ms.to_string());
        }
        if let Some(encoding) = self.encoding {
            command.arg("ENCODING").arg(encoding.as_str());
        }
        if let Some(chunk_size) = self.chunk_size {
            command.arg("CHUNK_SIZE").arg(chunk_size.to_string());
        }
        if let Some(policy) = self.duplicate_policy {
            let Some(token) = duplicate_token else {
                return Err(tower_mcp::Error::tool(
                    "duplicate_policy is not supported by this operation",
                ));
            };
            command.arg(token).arg(policy.as_str());
        }
        if let Some(ignore) = &self.ignore {
            command
                .arg("IGNORE")
                .arg(ignore.max_time_diff_ms.to_string())
                .arg(format!("{}", ignore.max_value_diff));
        }
        if let Some(labels) = &self.labels {
            command.arg("LABELS");
            for label in labels {
                command.arg(label.name.as_str()).arg(label.value.as_str());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsLabelOutput {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsSampleOutput {
    /// Exact Unix millisecond timestamp.
    timestamp: i64,
    /// Sample value as an IEEE 754 double.
    value: f64,
}

fn sample_value(value: &RedisValue, context: &str) -> tower_mcp::Result<f64> {
    match value {
        RedisValue::Double(value) => Ok(*value),
        RedisValue::Integer(value) => Ok(*value as f64),
        RedisValue::BulkString(bytes) => std::str::from_utf8(bytes)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                tower_mcp::Error::tool(format!("{context} returned a non-numeric sample value"))
            }),
        RedisValue::SimpleString(value) => value.parse().map_err(|_| {
            tower_mcp::Error::tool(format!("{context} returned a non-numeric sample value"))
        }),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected sample value: {other:?}"
        ))),
    }
}

fn parse_sample(value: &RedisValue, context: &str) -> tower_mcp::Result<TsSampleOutput> {
    let RedisValue::Array(entries) = value else {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected sample shape: {value:?}"
        )));
    };
    let (Some(RedisValue::Integer(timestamp)), Some(sample)) = (entries.first(), entries.get(1))
    else {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an incomplete sample"
        )));
    };
    Ok(TsSampleOutput {
        timestamp: *timestamp,
        value: sample_value(sample, context)?,
    })
}

fn parse_samples(value: &RedisValue, context: &str) -> tower_mcp::Result<Vec<TsSampleOutput>> {
    let RedisValue::Array(entries) = value else {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected samples shape: {value:?}"
        )));
    };
    entries
        .iter()
        .map(|entry| parse_sample(entry, context))
        .collect()
}

fn parse_optional_sample(
    value: &RedisValue,
    context: &str,
) -> tower_mcp::Result<Option<TsSampleOutput>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::Array(entries) if entries.is_empty() => Ok(None),
        // RESP2 returns the flat [timestamp, value] pair.
        RedisValue::Array(entries) if matches!(entries.first(), Some(RedisValue::Integer(_))) => {
            parse_sample(value, context).map(Some)
        }
        // Some reply forms nest one sample inside a single-entry array.
        RedisValue::Array(entries) if entries.len() == 1 => {
            parse_sample(&entries[0], context).map(Some)
        }
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected latest-sample shape: {other:?}"
        ))),
    }
}

fn label_text(value: &RedisValue, context: &str) -> tower_mcp::Result<String> {
    match value {
        RedisValue::BulkString(bytes) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        RedisValue::SimpleString(value) => Ok(value.clone()),
        RedisValue::VerbatimString { text, .. } => Ok(text.clone()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected label: {other:?}"
        ))),
    }
}

fn parse_labels(value: &RedisValue, context: &str) -> tower_mcp::Result<Vec<TsLabelOutput>> {
    match value {
        RedisValue::Nil => Ok(Vec::new()),
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(name, value)| {
                Ok(TsLabelOutput {
                    name: label_text(name, context)?,
                    value: label_text(value, context)?,
                })
            })
            .collect(),
        RedisValue::Array(entries) => entries
            .iter()
            .map(|entry| {
                let RedisValue::Array(pair) = entry else {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an unexpected label pair: {entry:?}"
                    )));
                };
                let (Some(name), Some(value)) = (pair.first(), pair.get(1)) else {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an incomplete label pair"
                    )));
                };
                Ok(TsLabelOutput {
                    name: label_text(name, context)?,
                    value: label_text(value, context)?,
                })
            })
            .collect(),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected label shape: {other:?}"
        ))),
    }
}

/// One multi-series reply entry normalized across RESP2 and RESP3 shapes.
struct TsSeriesEntry {
    name: String,
    labels: Vec<TsLabelOutput>,
    tail: RedisValue,
}

/// Normalize TS.MRANGE/TS.MREVRANGE/TS.MGET replies.
///
/// RESP2 returns an array of `[name, labels, tail]` triples; RESP3 returns a
/// map from series name to `[labels, .., tail]`, where reducer or source
/// metadata may appear between labels and the trailing samples.
fn parse_series_collection(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<Vec<TsSeriesEntry>> {
    fn from_parts(
        name: &RedisValue,
        parts: &[RedisValue],
        context: &str,
    ) -> tower_mcp::Result<TsSeriesEntry> {
        let name = label_text(name, context)?;
        let (Some(labels), Some(tail)) = (parts.first(), parts.last()) else {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an incomplete series entry for {name}"
            )));
        };
        Ok(TsSeriesEntry {
            name,
            labels: parse_labels(labels, context)?,
            tail: tail.clone(),
        })
    }

    match value {
        RedisValue::Nil => Ok(Vec::new()),
        RedisValue::Array(entries) => entries
            .iter()
            .map(|entry| {
                let RedisValue::Array(parts) = entry else {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an unexpected series entry: {entry:?}"
                    )));
                };
                let Some((name, tail_parts)) = parts.split_first() else {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an empty series entry"
                    )));
                };
                from_parts(name, tail_parts, context)
            })
            .collect(),
        RedisValue::Map(entries) => entries
            .iter()
            .map(|(name, value)| {
                let RedisValue::Array(parts) = value else {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned an unexpected series value: {value:?}"
                    )));
                };
                from_parts(name, parts, context)
            })
            .collect(),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

fn validate_filters(filters: &[String]) -> tower_mcp::Result<()> {
    if filters.is_empty() || filters.len() > MAX_TS_FILTERS {
        return Err(tower_mcp::Error::tool(format!(
            "filters must contain between 1 and {MAX_TS_FILTERS} label filter expressions"
        )));
    }
    for filter in filters {
        if filter.len() > MAX_FILTER_BYTES {
            return Err(tower_mcp::Error::tool(format!(
                "each filter expression is limited to {MAX_FILTER_BYTES} bytes"
            )));
        }
        if !filter.contains('=') {
            return Err(tower_mcp::Error::tool(format!(
                "filter `{filter}` must be a RedisTimeSeries label filter such as label=value, label!=value, label=(a,b), or label="
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsAggregationInput {
    /// Bucket aggregator applied server-side.
    aggregation: TsAggregation,
    /// Bucket duration in milliseconds.
    #[schemars(range(min = 1))]
    bucket_duration_ms: u64,
    /// Bucket alignment: `start`, `end`, or an exact millisecond timestamp.
    /// Requires RedisTimeSeries 1.6 or newer.
    #[serde(default)]
    align: Option<String>,
    /// Which bucket timestamp to report. Requires RedisTimeSeries 1.8 or
    /// newer.
    #[serde(default)]
    bucket_timestamp: Option<TsBucketTimestamp>,
    /// Report empty buckets inside the range. Requires RedisTimeSeries 1.8 or
    /// newer.
    #[serde(default)]
    empty: bool,
}

impl TsAggregationInput {
    fn validate(&self, state: &ToolState) -> tower_mcp::Result<()> {
        if let Some(minimum) = self.aggregation.minimum_version() {
            require_timeseries_version(state, minimum, self.aggregation.as_str())?;
        }
        if let Some(align) = &self.align {
            require_timeseries_version(state, RedisVersion::new(1, 6, 0), "ALIGN")?;
            validate_timestamp_token(align, &["start", "end", "-", "+"], "aggregation.align")?;
        }
        if self.bucket_timestamp.is_some() {
            require_timeseries_version(state, RedisVersion::new(1, 8, 0), "BUCKETTIMESTAMP")?;
        }
        if self.empty {
            require_timeseries_version(state, RedisVersion::new(1, 8, 0), "EMPTY")?;
        }
        Ok(())
    }

    fn apply(&self, command: &mut RedisCommand) {
        if let Some(align) = &self.align {
            command.arg("ALIGN").arg(align.as_str());
        }
        command
            .arg("AGGREGATION")
            .arg(self.aggregation.as_str())
            .arg(self.bucket_duration_ms.to_string());
        if let Some(bucket_timestamp) = self.bucket_timestamp {
            command
                .arg("BUCKETTIMESTAMP")
                .arg(bucket_timestamp.as_str());
        }
        if self.empty {
            command.arg("EMPTY");
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsValueFilterInput {
    /// Minimum accepted sample value, inclusive.
    minimum: f64,
    /// Maximum accepted sample value, inclusive.
    maximum: f64,
}

// --- TS.CREATE -------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsCreateInput {
    /// Time-series key.
    key: String,
    /// Optional series configuration.
    #[serde(default)]
    options: Option<TsSeriesOptionsInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsCreateOutput {
    key: String,
    created: bool,
}

fn ts_create_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_create")
        .title("Create Time Series")
        .description(
            "Create an empty RedisTimeSeries key with optional retention, encoding, chunk, duplicate-policy, label, and ignore configuration.",
        )
        .output_schema(output_schema::<TsCreateOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsCreateInput>| async move {
                let mut command = module_command(
                    "redis_ts_create",
                    AccessMode::ReadWrite,
                    RedisModule::TimeSeries,
                    "TS.CREATE",
                );
                command.arg(input.key.as_str());
                if let Some(options) = &input.options {
                    options.validate(&state)?;
                    options.apply(&mut command, Some("DUPLICATE_POLICY"))?;
                }
                let value = state.raw(command, "TS.CREATE failed").await?;
                if !matches!(value, RedisValue::Okay | RedisValue::SimpleString(_)) {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.CREATE returned an unexpected reply: {value:?}"
                    )));
                }
                state.output(&TsCreateOutput {
                    key: input.key,
                    created: true,
                })
            },
        )
        .build()
}

// --- TS.ALTER --------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsAlterInput {
    /// Time-series key.
    key: String,
    /// New sample retention in milliseconds. Shrinking retention trims
    /// samples older than the new window.
    #[serde(default)]
    retention_ms: Option<u64>,
    /// New chunk allocation for future chunks, in bytes.
    #[serde(default)]
    #[schemars(range(min = 48, max = 1048576))]
    chunk_size: Option<u64>,
    /// New duplicate-sample policy.
    #[serde(default)]
    duplicate_policy: Option<TsDuplicatePolicy>,
    /// Replacement label set. The existing labels are removed first; an empty
    /// list clears every label.
    #[serde(default)]
    #[schemars(length(max = 64))]
    labels: Option<Vec<TsLabelInput>>,
    /// New near-duplicate ignore deltas. Requires RedisTimeSeries 1.12.
    #[serde(default)]
    ignore: Option<TsIgnoreInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsAlterOutput {
    key: String,
    altered: bool,
}

fn ts_alter_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_alter")
        .title("Alter Time Series")
        .description(
            "Change retention, chunk size, duplicate policy, ignore deltas, or replace the whole label set of an existing time series. Requires full access because shrinking retention trims stored samples and the label set is replaced, not merged.",
        )
        .output_schema(output_schema::<TsAlterOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsAlterInput>| async move {
                state.require(AccessMode::Full, "redis_ts_alter")?;
                let options = TsSeriesOptionsInput {
                    retention_ms: input.retention_ms,
                    encoding: None,
                    chunk_size: input.chunk_size,
                    duplicate_policy: input.duplicate_policy,
                    labels: input.labels,
                    ignore: input.ignore,
                };
                options.validate(&state)?;
                let mut command = module_command(
                    "redis_ts_alter",
                    AccessMode::Full,
                    RedisModule::TimeSeries,
                    "TS.ALTER",
                );
                command.arg(input.key.as_str());
                options.apply(&mut command, Some("DUPLICATE_POLICY"))?;
                let value = state.raw(command, "TS.ALTER failed").await?;
                if !matches!(value, RedisValue::Okay | RedisValue::SimpleString(_)) {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.ALTER returned an unexpected reply: {value:?}"
                    )));
                }
                state.output(&TsAlterOutput {
                    key: input.key,
                    altered: true,
                })
            },
        )
        .build()
}

// --- TS.ADD ----------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsAddInput {
    /// Time-series key. The series is created on demand unless creation
    /// options say otherwise.
    key: String,
    /// Sample value.
    value: f64,
    /// Unix millisecond timestamp, or `*` for the server clock.
    #[serde(default)]
    timestamp: Option<String>,
    /// Creation options applied when the series does not exist yet, plus the
    /// ON_DUPLICATE override for this insertion.
    #[serde(default)]
    options: Option<TsSeriesOptionsInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsTimestampOutput {
    key: String,
    /// Exact Unix millisecond timestamp assigned by Redis.
    timestamp: i64,
}

fn ts_add_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_add")
        .title("Add Time-Series Sample")
        .description(
            "Append one sample to a time series, creating it on demand. Timestamps are exact integer milliseconds or `*` for the server clock.",
        )
        .output_schema(output_schema::<TsTimestampOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsAddInput>| async move {
                let timestamp = input.timestamp.as_deref().unwrap_or("*");
                validate_timestamp_token(timestamp, &["*"], "timestamp")?;
                let mut command = module_command(
                    "redis_ts_add",
                    AccessMode::ReadWrite,
                    RedisModule::TimeSeries,
                    "TS.ADD",
                );
                command
                    .arg(input.key.as_str())
                    .arg(timestamp)
                    .arg(format_sample_value(input.value, "value")?);
                if let Some(options) = &input.options {
                    options.validate(&state)?;
                    options.apply(&mut command, Some("ON_DUPLICATE"))?;
                }
                let timestamp: i64 = state.query(command, "TS.ADD failed").await?;
                state.output(&TsTimestampOutput {
                    key: input.key,
                    timestamp,
                })
            },
        )
        .build()
}

// --- TS.MADD ---------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMaddSampleInput {
    /// Time-series key. Every series must already exist.
    key: String,
    /// Sample value.
    value: f64,
    /// Unix millisecond timestamp, or `*` for the server clock.
    #[serde(default)]
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMaddInput {
    /// Samples appended in order. Results align one-to-one.
    #[schemars(length(min = 1, max = 100))]
    samples: Vec<TsMaddSampleInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMaddResult {
    index: usize,
    key: String,
    /// Assigned timestamp when this sample was accepted.
    timestamp: Option<i64>,
    /// Server error when this sample was rejected while the rest applied.
    error: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMaddOutput {
    requested: usize,
    accepted: usize,
    results: Vec<TsMaddResult>,
}

fn ts_madd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_madd")
        .title("Add Time-Series Samples")
        .description(
            "Append up to 100 samples across one or more existing series in one call. Per-sample failures are reported in-band while the remaining samples still apply; on Redis Cluster every key must hash to one slot.",
        )
        .output_schema(output_schema::<TsMaddOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsMaddInput>| async move {
                if input.samples.is_empty() || input.samples.len() > MAX_MADD_SAMPLES {
                    return Err(tower_mcp::Error::tool(format!(
                        "samples must contain between 1 and {MAX_MADD_SAMPLES} entries"
                    )));
                }
                state.validate_requested_entries(input.samples.len(), "samples")?;
                let mut command = module_command(
                    "redis_ts_madd",
                    AccessMode::ReadWrite,
                    RedisModule::TimeSeries,
                    "TS.MADD",
                );
                for (index, sample) in input.samples.iter().enumerate() {
                    let timestamp = sample.timestamp.as_deref().unwrap_or("*");
                    validate_timestamp_token(
                        timestamp,
                        &["*"],
                        &format!("samples[{index}].timestamp"),
                    )?;
                    command
                        .arg(sample.key.as_str())
                        .arg(timestamp)
                        .arg(format_sample_value(
                            sample.value,
                            &format!("samples[{index}].value"),
                        )?);
                }
                let value = state.raw(command, "TS.MADD failed").await?;
                let RedisValue::Array(entries) = value else {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.MADD returned an unexpected reply: {value:?}"
                    )));
                };
                if entries.len() != input.samples.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.MADD returned {} results for {} samples",
                        entries.len(),
                        input.samples.len()
                    )));
                }
                let mut accepted = 0_usize;
                let results = entries
                    .iter()
                    .enumerate()
                    .map(|(index, entry)| {
                        let key = input.samples[index].key.clone();
                        match entry {
                            RedisValue::Integer(timestamp) => {
                                accepted += 1;
                                Ok(TsMaddResult {
                                    index,
                                    key,
                                    timestamp: Some(*timestamp),
                                    error: None,
                                })
                            }
                            RedisValue::ServerError { code, message } => Ok(TsMaddResult {
                                index,
                                key,
                                timestamp: None,
                                error: Some(match message {
                                    Some(message) => format!("{code}: {message}"),
                                    None => code.clone(),
                                }),
                            }),
                            other => Err(tower_mcp::Error::tool(format!(
                                "TS.MADD returned an unexpected entry: {other:?}"
                            ))),
                        }
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = TsMaddOutput {
                    requested: results.len(),
                    accepted,
                    results,
                };
                state.output_collection(
                    &output,
                    output.requested,
                    "Send fewer samples per TS.MADD call.",
                )
            },
        )
        .build()
}

// --- TS.INCRBY / TS.DECRBY ---------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsCounterInput {
    /// Time-series key. The series is created on demand.
    key: String,
    /// Non-negative amount to add or subtract from the latest sample.
    value: f64,
    /// Explicit Unix millisecond timestamp for the adjusted sample, or `*`
    /// for the server clock.
    #[serde(default)]
    timestamp: Option<String>,
    /// Creation options applied when the series does not exist yet.
    #[serde(default)]
    options: Option<TsSeriesOptionsInput>,
}

fn ts_counter_tool(state: Arc<ToolState>, decrement: bool) -> Tool {
    let (tool_name, command_name, title, description) = if decrement {
        (
            "redis_ts_decrby",
            "TS.DECRBY",
            "Decrement Time-Series Counter",
            "Subtract a value from the latest sample of a counter-style time series, creating the series on demand.",
        )
    } else {
        (
            "redis_ts_incrby",
            "TS.INCRBY",
            "Increment Time-Series Counter",
            "Add a value to the latest sample of a counter-style time series, creating the series on demand.",
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<TsTimestampOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<TsCounterInput>| async move {
                let mut command = module_command(
                    if decrement {
                        "redis_ts_decrby"
                    } else {
                        "redis_ts_incrby"
                    },
                    AccessMode::ReadWrite,
                    RedisModule::TimeSeries,
                    command_name,
                );
                command
                    .arg(input.key.as_str())
                    .arg(format_sample_value(input.value, "value")?);
                if let Some(timestamp) = input.timestamp.as_deref() {
                    validate_timestamp_token(timestamp, &["*"], "timestamp")?;
                    command.arg("TIMESTAMP").arg(timestamp);
                }
                if let Some(options) = &input.options {
                    options.validate(&state)?;
                    options.apply(&mut command, None)?;
                }
                let timestamp: i64 = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                state.output(&TsTimestampOutput {
                    key: input.key,
                    timestamp,
                })
            },
        )
        .build()
}

// --- TS.DEL ----------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsDelInput {
    /// Time-series key.
    key: String,
    /// Inclusive start of the deleted range, in exact Unix milliseconds or
    /// `-` for the earliest sample.
    from_timestamp: String,
    /// Inclusive end of the deleted range, in exact Unix milliseconds or `+`
    /// for the latest sample.
    to_timestamp: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsDelOutput {
    key: String,
    deleted_samples: u64,
}

fn ts_del_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_del")
        .title("Delete Time-Series Samples")
        .description(
            "Permanently delete every sample inside an inclusive timestamp range. Requires full access and RedisTimeSeries 1.6 or newer.",
        )
        .output_schema(output_schema::<TsDelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsDelInput>| async move {
                state.require(AccessMode::Full, "redis_ts_del")?;
                state.require_tool_capabilities("redis_ts_del")?;
                validate_timestamp_token(&input.from_timestamp, &["-"], "from_timestamp")?;
                validate_timestamp_token(&input.to_timestamp, &["+"], "to_timestamp")?;
                let mut command = module_command(
                    "redis_ts_del",
                    AccessMode::Full,
                    RedisModule::TimeSeries,
                    "TS.DEL",
                );
                command
                    .arg(input.key.as_str())
                    .arg(input.from_timestamp.as_str())
                    .arg(input.to_timestamp.as_str());
                let deleted_samples: u64 = state.query(command, "TS.DEL failed").await?;
                state.output(&TsDelOutput {
                    key: input.key,
                    deleted_samples,
                })
            },
        )
        .build()
}

// --- TS.CREATERULE / TS.DELETERULE -------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsCreateRuleInput {
    /// Source series key.
    source_key: String,
    /// Destination series key. It must already exist; on Redis Cluster it
    /// must hash to the same slot as the source.
    destination_key: String,
    /// Bucket aggregator for the compaction.
    aggregation: TsAggregation,
    /// Compaction bucket duration in milliseconds.
    #[schemars(range(min = 1))]
    bucket_duration_ms: u64,
    /// Bucket alignment timestamp in milliseconds. Requires RedisTimeSeries
    /// 1.8 or newer.
    #[serde(default)]
    align_timestamp_ms: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsRuleOutput {
    source_key: String,
    destination_key: String,
    applied: bool,
}

fn ts_createrule_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_createrule")
        .title("Create Time-Series Compaction Rule")
        .description(
            "Attach a downsampling compaction rule from a source series into an existing destination series.",
        )
        .output_schema(output_schema::<TsRuleOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<TsCreateRuleInput>| async move {
                if let Some(minimum) = input.aggregation.minimum_version() {
                    require_timeseries_version(&state, minimum, input.aggregation.as_str())?;
                }
                if input.align_timestamp_ms.is_some() {
                    require_timeseries_version(
                        &state,
                        RedisVersion::new(1, 8, 0),
                        "alignTimestamp",
                    )?;
                }
                let mut command = module_command(
                    "redis_ts_createrule",
                    AccessMode::ReadWrite,
                    RedisModule::TimeSeries,
                    "TS.CREATERULE",
                );
                command
                    .arg(input.source_key.as_str())
                    .arg(input.destination_key.as_str())
                    .arg("AGGREGATION")
                    .arg(input.aggregation.as_str())
                    .arg(input.bucket_duration_ms.to_string());
                if let Some(align_timestamp_ms) = input.align_timestamp_ms {
                    command.arg(align_timestamp_ms.to_string());
                }
                let value = state.raw(command, "TS.CREATERULE failed").await?;
                if !matches!(value, RedisValue::Okay | RedisValue::SimpleString(_)) {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.CREATERULE returned an unexpected reply: {value:?}"
                    )));
                }
                state.output(&TsRuleOutput {
                    source_key: input.source_key,
                    destination_key: input.destination_key,
                    applied: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsDeleteRuleInput {
    /// Source series key.
    source_key: String,
    /// Destination series key of the removed rule.
    destination_key: String,
}

fn ts_deleterule_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_deleterule")
        .title("Delete Time-Series Compaction Rule")
        .description(
            "Remove the compaction rule between a source and destination series. Requires full access; the destination series and its samples are kept.",
        )
        .output_schema(output_schema::<TsRuleOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<TsDeleteRuleInput>| async move {
                state.require(AccessMode::Full, "redis_ts_deleterule")?;
                let mut command = module_command(
                    "redis_ts_deleterule",
                    AccessMode::Full,
                    RedisModule::TimeSeries,
                    "TS.DELETERULE",
                );
                command
                    .arg(input.source_key.as_str())
                    .arg(input.destination_key.as_str());
                let value = state.raw(command, "TS.DELETERULE failed").await?;
                if !matches!(value, RedisValue::Okay | RedisValue::SimpleString(_)) {
                    return Err(tower_mcp::Error::tool(format!(
                        "TS.DELETERULE returned an unexpected reply: {value:?}"
                    )));
                }
                state.output(&TsRuleOutput {
                    source_key: input.source_key,
                    destination_key: input.destination_key,
                    applied: true,
                })
            },
        )
        .build()
}

// --- TS.RANGE / TS.REVRANGE ---------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsRangeInput {
    /// Time-series key.
    key: String,
    /// Inclusive range start: exact Unix milliseconds or `-`.
    #[serde(default)]
    from_timestamp: Option<String>,
    /// Inclusive range end: exact Unix milliseconds or `+`.
    #[serde(default)]
    to_timestamp: Option<String>,
    /// Maximum samples or buckets returned by this page.
    #[serde(default)]
    #[schemars(range(min = 1, max = 1000))]
    count: Option<usize>,
    /// Optional server-side bucket aggregation.
    #[serde(default)]
    aggregation: Option<TsAggregationInput>,
    /// Return only samples with these exact timestamps.
    #[serde(default)]
    #[schemars(length(max = 128))]
    filter_by_timestamps: Option<Vec<i64>>,
    /// Return only samples inside this inclusive value range.
    #[serde(default)]
    filter_by_value: Option<TsValueFilterInput>,
    /// Report the latest possibly-open compaction bucket. Requires
    /// RedisTimeSeries 1.8 or newer.
    #[serde(default)]
    latest: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsRangeOutput {
    key: String,
    samples: Vec<TsSampleOutput>,
    page: PageMetadata,
}

fn apply_range_arguments(
    state: &ToolState,
    command: &mut RedisCommand,
    input: &TsRangeInput,
    count: usize,
) -> tower_mcp::Result<()> {
    let from = input.from_timestamp.as_deref().unwrap_or("-");
    let to = input.to_timestamp.as_deref().unwrap_or("+");
    validate_timestamp_token(from, &["-"], "from_timestamp")?;
    validate_timestamp_token(to, &["+"], "to_timestamp")?;
    command.arg(input.key.as_str()).arg(from).arg(to);
    if input.latest {
        require_timeseries_version(state, RedisVersion::new(1, 8, 0), "LATEST")?;
        command.arg("LATEST");
    }
    if let Some(timestamps) = &input.filter_by_timestamps {
        if timestamps.is_empty() || timestamps.len() > MAX_TIMESTAMP_FILTERS {
            return Err(tower_mcp::Error::tool(format!(
                "filter_by_timestamps must contain between 1 and {MAX_TIMESTAMP_FILTERS} timestamps"
            )));
        }
        command.arg("FILTER_BY_TS");
        for timestamp in timestamps {
            command.arg(timestamp.to_string());
        }
    }
    if let Some(filter) = &input.filter_by_value {
        command
            .arg("FILTER_BY_VALUE")
            .arg(format_sample_value(
                filter.minimum,
                "filter_by_value.minimum",
            )?)
            .arg(format_sample_value(
                filter.maximum,
                "filter_by_value.maximum",
            )?);
    }
    command.arg("COUNT").arg(count.to_string());
    if let Some(aggregation) = &input.aggregation {
        aggregation.validate(state)?;
        aggregation.apply(command);
    }
    Ok(())
}

fn ts_range_tool(state: Arc<ToolState>, reverse: bool) -> Tool {
    let (tool_name, command_name, title, description) = if reverse {
        (
            "redis_ts_revrange",
            "TS.REVRANGE",
            "Read Time-Series Range Descending",
            "Read a bounded page of samples or aggregation buckets from newest to oldest. Continuation metadata carries the next `to_timestamp` when more samples remain.",
        )
    } else {
        (
            "redis_ts_range",
            "TS.RANGE",
            "Read Time-Series Range",
            "Read a bounded page of samples or aggregation buckets from oldest to newest. Continuation metadata carries the next `from_timestamp` when more samples remain.",
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<TsRangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<TsRangeInput>| async move {
                let count = input
                    .count
                    .unwrap_or(DEFAULT_RANGE_COUNT)
                    .min(MAX_RANGE_COUNT);
                state.validate_requested_entries(count, "count")?;
                let mut command = module_command(
                    if reverse {
                        "redis_ts_revrange"
                    } else {
                        "redis_ts_range"
                    },
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    command_name,
                );
                apply_range_arguments(&state, &mut command, &input, count)?;
                let value = state
                    .raw(command, &format!("{command_name} failed"))
                    .await?;
                let samples = parse_samples(&value, command_name)?;
                let next_start = (samples.len() == count)
                    .then(|| samples.last())
                    .flatten()
                    .map(|sample| {
                        if reverse {
                            sample.timestamp.saturating_sub(1)
                        } else {
                            sample.timestamp.saturating_add(1)
                        }
                    });
                let output = TsRangeOutput {
                    key: input.key,
                    page: PageMetadata::range(count, samples.len(), next_start),
                    samples,
                };
                state.output_collection(
                    &output,
                    output.samples.len(),
                    "Request a smaller count or a narrower timestamp range.",
                )
            },
        )
        .build()
}

// --- TS.MRANGE / TS.MREVRANGE -------------------------------------------------

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum TsReducer {
    #[serde(rename = "avg")]
    Avg,
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "range")]
    Range,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "std.p")]
    StdP,
    #[serde(rename = "std.s")]
    StdS,
    #[serde(rename = "var.p")]
    VarP,
    #[serde(rename = "var.s")]
    VarS,
}

impl TsReducer {
    fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Range => "range",
            Self::Count => "count",
            Self::StdP => "std.p",
            Self::StdS => "std.s",
            Self::VarP => "var.p",
            Self::VarS => "var.s",
        }
    }

    fn minimum_version(self) -> RedisVersion {
        match self {
            Self::Sum | Self::Min | Self::Max => RedisVersion::new(1, 6, 0),
            _ => RedisVersion::new(1, 8, 0),
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsGroupByInput {
    /// Label whose values group the matched series.
    #[schemars(length(min = 1, max = 128))]
    label: String,
    /// Reducer applied across the series of each group.
    reducer: TsReducer,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMRangeInput {
    /// RedisTimeSeries label filter expressions, such as `sensor=1` or
    /// `area=(east,west)`. All expressions must match.
    #[schemars(length(min = 1, max = 16))]
    filters: Vec<String>,
    /// Inclusive range start: exact Unix milliseconds or `-`.
    #[serde(default)]
    from_timestamp: Option<String>,
    /// Inclusive range end: exact Unix milliseconds or `+`.
    #[serde(default)]
    to_timestamp: Option<String>,
    /// Maximum samples or buckets returned per series.
    #[serde(default)]
    #[schemars(range(min = 1, max = 1000))]
    count: Option<usize>,
    /// Optional server-side bucket aggregation.
    #[serde(default)]
    aggregation: Option<TsAggregationInput>,
    /// Return every label of each matched series.
    #[serde(default)]
    with_labels: bool,
    /// Return only these labels for each matched series.
    #[serde(default)]
    #[schemars(length(max = 16))]
    selected_labels: Option<Vec<String>>,
    /// Group matched series by one label and reduce each group to one series.
    /// Requires RedisTimeSeries 1.6 or newer.
    #[serde(default)]
    group_by: Option<TsGroupByInput>,
    /// Report the latest possibly-open compaction bucket per series.
    /// Requires RedisTimeSeries 1.8 or newer.
    #[serde(default)]
    latest: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsSeriesOutput {
    key: String,
    labels: Vec<TsLabelOutput>,
    samples: Vec<TsSampleOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMRangeOutput {
    series_count: usize,
    series: Vec<TsSeriesOutput>,
}

fn ts_mrange_tool(state: Arc<ToolState>, reverse: bool) -> Tool {
    let (tool_name, command_name, title, description) = if reverse {
        (
            "redis_ts_mrevrange",
            "TS.MREVRANGE",
            "Read Multi-Series Range Descending",
            "Read bounded descending sample pages from every series matching the label filters, optionally aggregated or grouped. On Redis Cluster this tool is unavailable because it would only observe one node.",
        )
    } else {
        (
            "redis_ts_mrange",
            "TS.MRANGE",
            "Read Multi-Series Range",
            "Read bounded ascending sample pages from every series matching the label filters, optionally aggregated or grouped. On Redis Cluster this tool is unavailable because it would only observe one node.",
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<TsMRangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<TsMRangeInput>| async move {
                let tool_name = if reverse {
                    "redis_ts_mrevrange"
                } else {
                    "redis_ts_mrange"
                };
                state.require_tool_capabilities(tool_name)?;
                validate_filters(&input.filters)?;
                if input.with_labels && input.selected_labels.is_some() {
                    return Err(tower_mcp::Error::tool(
                        "with_labels and selected_labels are mutually exclusive",
                    ));
                }
                let count = input.count.unwrap_or(DEFAULT_RANGE_COUNT).min(MAX_RANGE_COUNT);
                state.validate_requested_entries(count, "count")?;
                let from = input.from_timestamp.as_deref().unwrap_or("-");
                let to = input.to_timestamp.as_deref().unwrap_or("+");
                validate_timestamp_token(from, &["-"], "from_timestamp")?;
                validate_timestamp_token(to, &["+"], "to_timestamp")?;

                let mut command = module_command(
                    tool_name,
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    command_name,
                );
                command.arg(from).arg(to);
                if input.latest {
                    require_timeseries_version(&state, RedisVersion::new(1, 8, 0), "LATEST")?;
                    command.arg("LATEST");
                }
                command.arg("COUNT").arg(count.to_string());
                if let Some(aggregation) = &input.aggregation {
                    aggregation.validate(&state)?;
                    aggregation.apply(&mut command);
                }
                if input.with_labels {
                    command.arg("WITHLABELS");
                } else if let Some(selected) = &input.selected_labels {
                    if selected.is_empty() || selected.len() > MAX_TS_FILTERS {
                        return Err(tower_mcp::Error::tool(format!(
                            "selected_labels must contain between 1 and {MAX_TS_FILTERS} labels"
                        )));
                    }
                    command.arg("SELECTED_LABELS");
                    for label in selected {
                        command.arg(label.as_str());
                    }
                }
                command.arg("FILTER");
                for filter in &input.filters {
                    command.arg(filter.as_str());
                }
                if let Some(group_by) = &input.group_by {
                    require_timeseries_version(
                        &state,
                        group_by.reducer.minimum_version(),
                        "GROUPBY",
                    )?;
                    command
                        .arg("GROUPBY")
                        .arg(group_by.label.as_str())
                        .arg("REDUCE")
                        .arg(group_by.reducer.as_str());
                }
                let value = state
                    .raw(command, &format!("{command_name} failed"))
                    .await?;
                let series = parse_series_collection(value, command_name)?
                    .into_iter()
                    .map(|entry| {
                        Ok(TsSeriesOutput {
                            samples: parse_samples(&entry.tail, command_name)?,
                            key: entry.name,
                            labels: entry.labels,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let entries = series
                    .iter()
                    .map(|series| series.samples.len().saturating_add(series.labels.len()))
                    .fold(series.len(), usize::saturating_add);
                let output = TsMRangeOutput {
                    series_count: series.len(),
                    series,
                };
                state.output_collection(
                    &output,
                    entries,
                    "Narrow the label filters, lower the per-series count, or aggregate into buckets.",
                )
            },
        )
        .build()
}

// --- TS.GET / TS.MGET ---------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsGetInput {
    /// Time-series key.
    key: String,
    /// Report the latest possibly-open compaction bucket. Requires
    /// RedisTimeSeries 1.8 or newer.
    #[serde(default)]
    latest: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsGetOutput {
    key: String,
    /// Latest sample, or null when the series exists but is empty.
    sample: Option<TsSampleOutput>,
}

fn ts_get_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_get")
        .title("Read Latest Time-Series Sample")
        .description("Read the most recent sample of one time series.")
        .output_schema(output_schema::<TsGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsGetInput>| async move {
                let mut command = module_command(
                    "redis_ts_get",
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    "TS.GET",
                );
                command.arg(input.key.as_str());
                if input.latest {
                    require_timeseries_version(&state, RedisVersion::new(1, 8, 0), "LATEST")?;
                    command.arg("LATEST");
                }
                let value = state.raw(command, "TS.GET failed").await?;
                state.output(&TsGetOutput {
                    key: input.key,
                    sample: parse_optional_sample(&value, "TS.GET")?,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMGetInput {
    /// RedisTimeSeries label filter expressions. All expressions must match.
    #[schemars(length(min = 1, max = 16))]
    filters: Vec<String>,
    /// Return every label of each matched series.
    #[serde(default)]
    with_labels: bool,
    /// Return only these labels for each matched series.
    #[serde(default)]
    #[schemars(length(max = 16))]
    selected_labels: Option<Vec<String>>,
    /// Report the latest possibly-open compaction bucket per series.
    /// Requires RedisTimeSeries 1.8 or newer.
    #[serde(default)]
    latest: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMGetSeriesOutput {
    key: String,
    labels: Vec<TsLabelOutput>,
    sample: Option<TsSampleOutput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsMGetOutput {
    series_count: usize,
    series: Vec<TsMGetSeriesOutput>,
}

fn ts_mget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_mget")
        .title("Read Latest Multi-Series Samples")
        .description(
            "Read the most recent sample from every series matching the label filters. On Redis Cluster this tool is unavailable because it would only observe one node.",
        )
        .output_schema(output_schema::<TsMGetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsMGetInput>| async move {
                state.require_tool_capabilities("redis_ts_mget")?;
                validate_filters(&input.filters)?;
                if input.with_labels && input.selected_labels.is_some() {
                    return Err(tower_mcp::Error::tool(
                        "with_labels and selected_labels are mutually exclusive",
                    ));
                }
                let mut command = module_command(
                    "redis_ts_mget",
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    "TS.MGET",
                );
                if input.latest {
                    require_timeseries_version(&state, RedisVersion::new(1, 8, 0), "LATEST")?;
                    command.arg("LATEST");
                }
                if input.with_labels {
                    command.arg("WITHLABELS");
                } else if let Some(selected) = &input.selected_labels {
                    if selected.is_empty() || selected.len() > MAX_TS_FILTERS {
                        return Err(tower_mcp::Error::tool(format!(
                            "selected_labels must contain between 1 and {MAX_TS_FILTERS} labels"
                        )));
                    }
                    command.arg("SELECTED_LABELS");
                    for label in selected {
                        command.arg(label.as_str());
                    }
                }
                command.arg("FILTER");
                for filter in &input.filters {
                    command.arg(filter.as_str());
                }
                let value = state.raw(command, "TS.MGET failed").await?;
                let series = parse_series_collection(value, "TS.MGET")?
                    .into_iter()
                    .map(|entry| {
                        Ok(TsMGetSeriesOutput {
                            sample: parse_optional_sample(&entry.tail, "TS.MGET")?,
                            key: entry.name,
                            labels: entry.labels,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let entries = series
                    .iter()
                    .map(|series| series.labels.len().saturating_add(1))
                    .fold(series.len(), usize::saturating_add);
                let output = TsMGetOutput {
                    series_count: series.len(),
                    series,
                };
                state.output_collection(&output, entries, "Narrow the label filters.")
            },
        )
        .build()
}

// --- TS.INFO -------------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsInfoInput {
    /// Time-series key.
    key: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsInfoOutput {
    key: String,
    attributes: BTreeMap<String, JsonValue>,
}

fn ts_info_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_info")
        .title("Inspect Time Series")
        .description(
            "Read retention, chunk, duplicate-policy, label, source, and compaction-rule metadata for one time series.",
        )
        .output_schema(output_schema::<TsInfoOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<TsInfoInput>| async move {
                let mut command = module_command(
                    "redis_ts_info",
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    "TS.INFO",
                );
                command.arg(input.key.as_str());
                let value = state.raw(command, "TS.INFO failed").await?;
                let attributes = info_attributes(value)?;
                let output = TsInfoOutput {
                    key: input.key,
                    attributes,
                };
                state.output_collection(
                    &output,
                    output.attributes.len(),
                    "Use a larger configured entry budget; TS.INFO has no partial form.",
                )
            },
        )
        .build()
}

fn info_attributes(value: RedisValue) -> tower_mcp::Result<BTreeMap<String, JsonValue>> {
    let pairs: Vec<(RedisValue, RedisValue)> = match value {
        RedisValue::Map(entries) => entries,
        RedisValue::Array(entries) => entries
            .chunks(2)
            .map(|pair| {
                let mut pair = pair.iter();
                match (pair.next(), pair.next()) {
                    (Some(key), Some(value)) => Ok((key.clone(), value.clone())),
                    _ => Err(tower_mcp::Error::tool(
                        "TS.INFO returned an odd number of reply entries",
                    )),
                }
            })
            .collect::<tower_mcp::Result<Vec<_>>>()?,
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "TS.INFO returned an unexpected reply: {other:?}"
            )));
        }
    };
    Ok(pairs
        .into_iter()
        .map(|(key, value)| {
            let key = match label_text(&key, "TS.INFO") {
                Ok(key) => key,
                Err(_) => format!("{key:?}"),
            };
            (key, redis_value_to_json(&value))
        })
        .collect())
}

// --- TS.QUERYINDEX ---------------------------------------------------------------

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsQueryIndexInput {
    /// RedisTimeSeries label filter expressions. All expressions must match.
    #[schemars(length(min = 1, max = 16))]
    filters: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsKeyOutput {
    value: String,
    encoding: ValueEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TsQueryIndexOutput {
    key_count: usize,
    keys: Vec<TsKeyOutput>,
}

fn ts_queryindex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ts_queryindex")
        .title("Query Time-Series Index")
        .description(
            "List time-series keys whose labels match every filter expression. On Redis Cluster this tool is unavailable because it would only observe one node.",
        )
        .output_schema(output_schema::<TsQueryIndexOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<TsQueryIndexInput>| async move {
                state.require_tool_capabilities("redis_ts_queryindex")?;
                validate_filters(&input.filters)?;
                let mut command = module_command(
                    "redis_ts_queryindex",
                    AccessMode::ReadOnly,
                    RedisModule::TimeSeries,
                    "TS.QUERYINDEX",
                );
                for filter in &input.filters {
                    command.arg(filter.as_str());
                }
                let value = state.raw(command, "TS.QUERYINDEX failed").await?;
                let entries = match value {
                    RedisValue::Array(entries) | RedisValue::Set(entries) => entries,
                    other => {
                        return Err(tower_mcp::Error::tool(format!(
                            "TS.QUERYINDEX returned an unexpected reply: {other:?}"
                        )));
                    }
                };
                let keys = entries
                    .into_iter()
                    .map(|entry| match entry {
                        RedisValue::BulkString(bytes) => {
                            let (value, encoding) = encode_bytes(bytes);
                            Ok(TsKeyOutput { value, encoding })
                        }
                        RedisValue::SimpleString(value) => Ok(TsKeyOutput {
                            value,
                            encoding: ValueEncoding::Utf8,
                        }),
                        other => Err(tower_mcp::Error::tool(format!(
                            "TS.QUERYINDEX returned an unexpected key: {other:?}"
                        ))),
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = TsQueryIndexOutput {
                    key_count: keys.len(),
                    keys,
                };
                state.output_collection(
                    &output,
                    output.key_count,
                    "Narrow the label filters.",
                )
            },
        )
        .build()
}

// --- Registration ------------------------------------------------------------

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ts_get_tool(state.clone()));
    router = router.tool(ts_mget_tool(state.clone()));
    router = router.tool(ts_info_tool(state.clone()));
    router = router.tool(ts_queryindex_tool(state.clone()));
    router = router.tool(ts_range_tool(state.clone(), false));
    router = router.tool(ts_range_tool(state.clone(), true));
    router = router.tool(ts_mrange_tool(state.clone(), false));
    router.tool(ts_mrange_tool(state, true))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ts_create_tool(state.clone()));
    router = router.tool(ts_add_tool(state.clone()));
    router = router.tool(ts_madd_tool(state.clone()));
    router = router.tool(ts_counter_tool(state.clone(), false));
    router = router.tool(ts_counter_tool(state.clone(), true));
    router.tool(ts_createrule_tool(state))
}

pub(super) fn add_destructive_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ts_alter_tool(state.clone()));
    router = router.tool(ts_del_tool(state.clone()));
    router.tool(ts_deleterule_tool(state))
}
