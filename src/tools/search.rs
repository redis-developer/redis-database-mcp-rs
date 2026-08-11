//! Optional Redis Query Engine index and search operations.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    PageMetadata, ToolState, ValueEncoding, destructive_annotations, empty_input_schema,
    module_command, output_schema, read_annotations, redis_value_to_json, write_annotations,
};
use crate::{AccessMode, RedisModule, RedisValue};

const MAX_SEARCH_RESULTS: u64 = 100;
const MAX_SCHEMA_FIELDS: usize = 100;
const MAX_VECTOR_DIMENSIONS: usize = 16_384;
const DEFAULT_VECTOR_SCORE_ALIAS: &str = "vector_distance";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum VectorDataType {
    #[serde(rename = "FLOAT32")]
    Float32,
    #[serde(rename = "FLOAT64")]
    Float64,
}

impl VectorDataType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float64 => "FLOAT64",
        }
    }

    fn byte_width(self) -> usize {
        match self {
            Self::Float32 => size_of::<f32>(),
            Self::Float64 => size_of::<f64>(),
        }
    }

    fn encode(self, vector: &[f64]) -> tower_mcp::Result<Vec<u8>> {
        validate_vector_length(vector)?;
        let mut bytes = Vec::with_capacity(vector.len() * self.byte_width());
        for (index, value) in vector.iter().copied().enumerate() {
            if !value.is_finite() {
                return Err(tower_mcp::Error::tool(format!(
                    "vector[{index}] must be finite"
                )));
            }
            match self {
                Self::Float32 => {
                    let value = value as f32;
                    if !value.is_finite() {
                        return Err(tower_mcp::Error::tool(format!(
                            "vector[{index}] is outside the FLOAT32 range"
                        )));
                    }
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                Self::Float64 => bytes.extend_from_slice(&value.to_le_bytes()),
            }
        }
        Ok(bytes)
    }

    fn decode(self, bytes: &[u8]) -> tower_mcp::Result<Vec<f64>> {
        let width = self.byte_width();
        if bytes.is_empty() || !bytes.len().is_multiple_of(width) {
            return Err(tower_mcp::Error::tool(format!(
                "stored vector has {} bytes; {} requires a non-zero multiple of {width}",
                bytes.len(),
                self.as_str()
            )));
        }
        let dimensions = bytes.len() / width;
        if dimensions > MAX_VECTOR_DIMENSIONS {
            return Err(tower_mcp::Error::tool(format!(
                "stored vector has {dimensions} dimensions; maximum is {MAX_VECTOR_DIMENSIONS}"
            )));
        }
        bytes
            .chunks_exact(width)
            .enumerate()
            .map(|(index, bytes)| {
                let value = match self {
                    Self::Float32 => {
                        f32::from_le_bytes(bytes.try_into().expect("FLOAT32 chunk width")) as f64
                    }
                    Self::Float64 => {
                        f64::from_le_bytes(bytes.try_into().expect("FLOAT64 chunk width"))
                    }
                };
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(tower_mcp::Error::tool(format!(
                        "stored vector[{index}] is not finite"
                    )))
                }
            })
            .collect()
    }
}

fn validate_vector_length(vector: &[f64]) -> tower_mcp::Result<()> {
    if vector.is_empty() || vector.len() > MAX_VECTOR_DIMENSIONS {
        Err(tower_mcp::Error::tool(format!(
            "vector must contain between 1 and {MAX_VECTOR_DIMENSIONS} values"
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
enum VectorAlgorithm {
    #[serde(rename = "FLAT")]
    Flat,
    #[serde(rename = "HNSW")]
    Hnsw,
}

impl VectorAlgorithm {
    fn as_str(self) -> &'static str {
        match self {
            Self::Flat => "FLAT",
            Self::Hnsw => "HNSW",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorFieldOptions {
    /// Vector index algorithm: FLAT for exact search or HNSW for approximate search.
    algorithm: VectorAlgorithm,
    /// Element representation used in Redis: FLOAT32 or FLOAT64.
    data_type: VectorDataType,
    /// Number of elements in each vector.
    #[schemars(range(min = 1, max = 16384))]
    dimensions: usize,
    /// Distance metric: L2, IP, or COSINE.
    distance_metric: String,
    /// Optional initial index capacity.
    #[serde(default)]
    initial_capacity: Option<u64>,
    /// Optional FLAT allocation block size.
    #[serde(default)]
    block_size: Option<u64>,
    /// Optional HNSW maximum outgoing edges per node.
    #[serde(default)]
    m: Option<u64>,
    /// Optional HNSW construction candidate count.
    #[serde(default)]
    ef_construction: Option<u64>,
    /// Optional default HNSW runtime candidate count.
    #[serde(default)]
    ef_runtime: Option<u64>,
    /// Optional HNSW range-query relative distance factor.
    #[serde(default)]
    epsilon: Option<f64>,
}

impl VectorFieldOptions {
    fn validate(&self) -> tower_mcp::Result<String> {
        if self.dimensions == 0 || self.dimensions > MAX_VECTOR_DIMENSIONS {
            return Err(tower_mcp::Error::tool(format!(
                "dimensions must be between 1 and {MAX_VECTOR_DIMENSIONS}"
            )));
        }
        for (name, value) in [
            ("initial_capacity", self.initial_capacity),
            ("block_size", self.block_size),
            ("m", self.m),
            ("ef_construction", self.ef_construction),
            ("ef_runtime", self.ef_runtime),
        ] {
            if value == Some(0) {
                return Err(tower_mcp::Error::tool(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if self
            .epsilon
            .is_some_and(|value| !value.is_finite() || value < 0.0)
        {
            return Err(tower_mcp::Error::tool(
                "epsilon must be a finite number greater than or equal to zero",
            ));
        }
        match self.algorithm {
            VectorAlgorithm::Flat => {
                if self.m.is_some()
                    || self.ef_construction.is_some()
                    || self.ef_runtime.is_some()
                    || self.epsilon.is_some()
                {
                    return Err(tower_mcp::Error::tool(
                        "m, ef_construction, ef_runtime, and epsilon are valid only for HNSW",
                    ));
                }
            }
            VectorAlgorithm::Hnsw if self.block_size.is_some() => {
                return Err(tower_mcp::Error::tool("block_size is valid only for FLAT"));
            }
            VectorAlgorithm::Hnsw => {}
        }
        let metric = self.distance_metric.to_ascii_uppercase();
        if !matches!(metric.as_str(), "L2" | "IP" | "COSINE") {
            return Err(tower_mcp::Error::tool(
                "distance_metric must be L2, IP, or COSINE",
            ));
        }
        Ok(metric)
    }

    fn arguments(&self) -> tower_mcp::Result<Vec<String>> {
        let metric = self.validate()?;
        let mut arguments = vec![
            "TYPE".to_string(),
            self.data_type.as_str().to_string(),
            "DIM".to_string(),
            self.dimensions.to_string(),
            "DISTANCE_METRIC".to_string(),
            metric,
        ];
        for (name, value) in [
            ("INITIAL_CAP", self.initial_capacity),
            ("BLOCK_SIZE", self.block_size),
            ("M", self.m),
            ("EF_CONSTRUCTION", self.ef_construction),
            ("EF_RUNTIME", self.ef_runtime),
        ] {
            if let Some(value) = value {
                arguments.push(name.to_string());
                arguments.push(value.to_string());
            }
        }
        if let Some(value) = self.epsilon {
            arguments.push("EPSILON".to_string());
            arguments.push(value.to_string());
        }
        Ok(arguments)
    }
}

fn utf8(value: Vec<u8>, context: &str) -> tower_mcp::Result<String> {
    String::from_utf8(value)
        .map_err(|_| tower_mcp::Error::tool(format!("{context} returned non-UTF-8 text")))
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtListOutput {
    count: usize,
    indexes: Vec<String>,
}

fn ft_list_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_list")
        .title("List Redis Search Indexes")
        .description("List Redis Query Engine indexes. Requires the Search capability.")
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<FtListOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let command = module_command(
                "redis_ft_list",
                AccessMode::ReadOnly,
                RedisModule::Search,
                "FT._LIST",
            );
            let value = state.raw(command, "FT._LIST failed").await?;
            let values = match value {
                RedisValue::Array(values) | RedisValue::Set(values) => values,
                RedisValue::Nil => Vec::new(),
                value => vec![value],
            };
            let mut indexes = values
                .into_iter()
                .map(|value| match value {
                    RedisValue::BulkString(value) => utf8(value, "FT._LIST"),
                    RedisValue::SimpleString(value) => Ok(value),
                    other => Err(tower_mcp::Error::tool(format!(
                        "FT._LIST returned an unexpected index name: {other:?}"
                    ))),
                })
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            indexes.sort();
            let output = FtListOutput {
                count: indexes.len(),
                indexes,
            };
            state.output_collection(
                &output,
                output.count,
                "Use a larger configured entry budget or inspect a known index with redis_ft_info.",
            )
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IndexInput {
    /// Search index name.
    index: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtInfoOutput {
    index: String,
    attributes: BTreeMap<String, JsonValue>,
}

fn info_attributes(value: RedisValue) -> tower_mcp::Result<BTreeMap<String, JsonValue>> {
    let pairs = match value {
        RedisValue::Array(values) => {
            if values.len() % 2 != 0 {
                return Err(tower_mcp::Error::tool(
                    "FT.INFO returned an odd number of key/value elements",
                ));
            }
            let mut pairs = Vec::with_capacity(values.len() / 2);
            let mut values = values.into_iter();
            while let (Some(key), Some(value)) = (values.next(), values.next()) {
                pairs.push((key, value));
            }
            pairs
        }
        RedisValue::Map(pairs) => pairs,
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "FT.INFO returned an unexpected value: {other:?}"
            )));
        }
    };

    pairs
        .into_iter()
        .map(|(key, value)| {
            let key = match key {
                RedisValue::BulkString(key) => utf8(key, "FT.INFO field name")?,
                RedisValue::SimpleString(key) => key,
                other => {
                    return Err(tower_mcp::Error::tool(format!(
                        "FT.INFO returned an invalid field name: {other:?}"
                    )));
                }
            };
            Ok((key, redis_value_to_json(&value)))
        })
        .collect()
}

fn ft_info_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_info")
        .title("Inspect Redis Search Index")
        .description(
            "Read index schema, document counts, and indexing status. Requires the Search capability.",
        )
        .output_schema(output_schema::<FtInfoOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<IndexInput>| async move {
                let mut command = module_command(
                    "redis_ft_info",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.INFO",
                );
                command.arg(input.index.as_str());
                let value = state.raw(command, "FT.INFO failed").await?;
                let attributes = info_attributes(value)?;
                let output = FtInfoOutput {
                    index: input.index,
                    attributes,
                };
                state.output_collection(
                    &output,
                    output.attributes.len(),
                    "Use a larger configured entry budget; FT.INFO has no Redis cursor form.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtSearchInput {
    /// Search index name.
    index: String,
    /// Query expression. Use `*` to match all indexed documents.
    query: String,
    /// Result offset. Start with zero and follow page.continuation.offset.
    #[serde(default)]
    limit_offset: Option<u64>,
    /// Maximum results to return, bounded to 100 per call.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    limit_num: Option<u64>,
    /// Sortable field name.
    #[serde(default)]
    sortby: Option<String>,
    /// Sort order, ASC or DESC.
    #[serde(default)]
    sortby_order: Option<String>,
    /// Indexed fields to return.
    #[serde(default)]
    #[schemars(length(max = 100))]
    return_fields: Option<Vec<String>>,
    /// Return document identifiers without fields.
    #[serde(default)]
    nocontent: bool,
    /// Disable stemming for this query.
    #[serde(default)]
    verbatim: bool,
    /// Include match scores in the raw response sequence.
    #[serde(default)]
    withscores: bool,
}

impl FtSearchInput {
    fn validate(&self) -> tower_mcp::Result<()> {
        if self
            .limit_num
            .is_some_and(|limit| limit > MAX_SEARCH_RESULTS)
        {
            return Err(tower_mcp::Error::tool(format!(
                "limit_num must be at most {MAX_SEARCH_RESULTS}"
            )));
        }
        if self
            .return_fields
            .as_ref()
            .is_some_and(|fields| fields.len() > MAX_SCHEMA_FIELDS)
        {
            return Err(tower_mcp::Error::tool(format!(
                "return_fields must contain at most {MAX_SCHEMA_FIELDS} items"
            )));
        }
        if self.sortby.is_none() && self.sortby_order.is_some() {
            return Err(tower_mcp::Error::tool("sortby_order requires sortby"));
        }
        if let Some(order) = &self.sortby_order
            && !matches!(order.to_ascii_uppercase().as_str(), "ASC" | "DESC")
        {
            return Err(tower_mcp::Error::tool("sortby_order must be ASC or DESC"));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtSearchOutput {
    index: String,
    query: String,
    total: Option<u64>,
    response: JsonValue,
    limit_offset: u64,
    limit_num: u64,
    page: PageMetadata,
}

fn search_response(value: RedisValue) -> (Option<u64>, JsonValue) {
    match value {
        RedisValue::Array(mut values) if !values.is_empty() => {
            let total = match values.remove(0) {
                RedisValue::Integer(total) => u64::try_from(total).ok(),
                RedisValue::BulkString(total) => std::str::from_utf8(&total)
                    .ok()
                    .and_then(|total| total.parse().ok()),
                _ => None,
            };
            (
                total,
                JsonValue::Array(values.iter().map(redis_value_to_json).collect()),
            )
        }
        value => (None, redis_value_to_json(&value)),
    }
}

fn ft_search_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_search")
        .title("Search Redis Index")
        .description(
            "Run a bounded Redis Query Engine search. Pass page.continuation.offset as limit_offset until page.complete is true. Binary response values retain explicit encodings.",
        )
        .output_schema(output_schema::<FtSearchOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtSearchInput>| async move {
                input.validate()?;
                let limit_offset = input.limit_offset.unwrap_or(0);
                let limit_num = input.limit_num.unwrap_or(10);
                state.validate_requested_entries(limit_num as usize, "limit_num")?;
                let mut command = module_command(
                    "redis_ft_search",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.SEARCH",
                );
                command.arg(input.index.as_str()).arg(input.query.as_str());
                if input.nocontent {
                    command.arg("NOCONTENT");
                }
                if input.verbatim {
                    command.arg("VERBATIM");
                }
                if input.withscores {
                    command.arg("WITHSCORES");
                }
                if let Some(fields) = &input.return_fields {
                    command.arg("RETURN").arg(fields.len().to_string());
                    command.args(fields.iter().map(String::as_str));
                }
                if let Some(field) = &input.sortby {
                    command.arg("SORTBY").arg(field.as_str());
                    if let Some(order) = &input.sortby_order {
                        command.arg(order.to_ascii_uppercase());
                    }
                }
                command
                    .arg("LIMIT")
                    .arg(limit_offset.to_string())
                    .arg(limit_num.to_string());
                let value = state.raw(command, "FT.SEARCH failed").await?;
                let (total, response) = search_response(value);
                let returned = total
                    .map(|total| total.saturating_sub(limit_offset).min(limit_num) as usize)
                    .unwrap_or_else(|| {
                        response
                            .as_array()
                            .map_or(0, |values| values.len().min(limit_num as usize))
                    });
                let next_offset = total.and_then(|total| {
                    let next = limit_offset.saturating_add(returned as u64);
                    (next < total).then_some(next)
                });
                let output = FtSearchOutput {
                    index: input.index,
                    query: input.query,
                    total,
                    response,
                    limit_offset,
                    limit_num,
                    page: PageMetadata::offset(limit_num as usize, returned, next_offset),
                };
                state.output_collection(
                    &output,
                    returned,
                    "Retry FT.SEARCH with a smaller limit_num and page.continuation.offset.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashVectorInput {
    /// Redis hash key.
    key: String,
    /// Hash field containing the binary vector.
    field: String,
    /// Element representation used for the stored bytes.
    data_type: VectorDataType,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetHashVectorInput {
    /// Redis hash key.
    key: String,
    /// Hash field that will contain the binary vector.
    field: String,
    /// Element representation used for the stored bytes.
    data_type: VectorDataType,
    /// Numeric vector encoded directly into a binary-safe Redis argument.
    #[schemars(length(min = 1, max = 16384))]
    vector: Vec<f64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetHashVectorOutput {
    key: String,
    field: String,
    data_type: VectorDataType,
    dimensions: usize,
    bytes: usize,
    encoding: String,
    field_added: bool,
    stored: bool,
}

fn vector_set_hash_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vector_set_hash")
        .title("Store Redis Hash Vector")
        .description(
            "Encode a numeric FLOAT32 or FLOAT64 vector into deterministic little-endian bytes and store it in one Redis hash field.",
        )
        .output_schema(output_schema::<SetHashVectorOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetHashVectorInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_vector_set_hash")?;
                let bytes = input.data_type.encode(&input.vector)?;
                let byte_count = bytes.len();
                let mut command = module_command(
                    "redis_vector_set_hash",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "HSET",
                );
                command
                    .arg(input.key.as_str())
                    .arg(input.field.as_str())
                    .arg(bytes);
                let changed: i64 = state.query(command, "vector HSET failed").await?;
                state.output(&SetHashVectorOutput {
                    key: input.key,
                    field: input.field,
                    data_type: input.data_type,
                    dimensions: input.vector.len(),
                    bytes: byte_count,
                    encoding: "ieee754_little_endian".to_string(),
                    field_added: changed > 0,
                    stored: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetHashVectorOutput {
    key: String,
    field: String,
    exists: bool,
    data_type: VectorDataType,
    dimensions: usize,
    bytes: usize,
    encoding: String,
    vector: Option<Vec<f64>>,
}

fn vector_get_hash_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_vector_get_hash")
        .title("Read Redis Hash Vector")
        .description(
            "Decode one binary Redis hash field as a bounded little-endian FLOAT32 or FLOAT64 numeric vector.",
        )
        .output_schema(output_schema::<GetHashVectorOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashVectorInput>| async move {
                let mut command = module_command(
                    "redis_vector_get_hash",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "HGET",
                );
                command.arg(input.key.as_str()).arg(input.field.as_str());
                let bytes: Option<Vec<u8>> = state.query(command, "vector HGET failed").await?;
                let byte_count = bytes.as_ref().map_or(0, Vec::len);
                let vector = bytes
                    .as_deref()
                    .map(|bytes| input.data_type.decode(bytes))
                    .transpose()?;
                state.output(&GetHashVectorOutput {
                    key: input.key,
                    field: input.field,
                    exists: vector.is_some(),
                    data_type: input.data_type,
                    dimensions: vector.as_ref().map_or(0, Vec::len),
                    bytes: byte_count,
                    encoding: "ieee754_little_endian".to_string(),
                    vector,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorQueryInput {
    /// Search index name.
    index: String,
    /// Query-facing alias of the indexed VECTOR field.
    vector_field: String,
    /// Element representation expected by the index.
    data_type: VectorDataType,
    /// Numeric query vector. Its length must equal the index dimension.
    #[schemars(length(min = 1, max = 16384))]
    vector: Vec<f64>,
    /// Number of nearest neighbors considered, bounded to 100.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    top_k: Option<u64>,
    /// Offset within the top-k neighbors. Follow page.continuation.offset.
    #[serde(default)]
    limit_offset: Option<u64>,
    /// Maximum results returned by this page, bounded to 100.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    limit_num: Option<u64>,
    /// Additional fields to return with each document.
    #[serde(default)]
    #[schemars(length(max = 100))]
    return_fields: Vec<String>,
    /// Alias used for the computed distance field.
    #[serde(default)]
    score_alias: Option<String>,
    /// Per-query HNSW candidate count.
    #[serde(default)]
    ef_runtime: Option<u64>,
    /// Cluster shard over-fetch ratio between 0.1 and 1.0, with at most two decimals.
    #[serde(default)]
    shard_k_ratio: Option<f64>,
}

struct ValidatedVectorQuery {
    top_k: u64,
    limit_offset: u64,
    limit_num: u64,
    score_alias: String,
}

impl VectorQueryInput {
    fn validate(&self) -> tower_mcp::Result<ValidatedVectorQuery> {
        validate_query_identifier(&self.vector_field, "vector_field")?;
        validate_vector_length(&self.vector)?;
        let top_k = self.top_k.unwrap_or(10);
        if top_k == 0 || top_k > MAX_SEARCH_RESULTS {
            return Err(tower_mcp::Error::tool(format!(
                "top_k must be between 1 and {MAX_SEARCH_RESULTS}"
            )));
        }
        let limit_offset = self.limit_offset.unwrap_or(0);
        if limit_offset >= top_k {
            return Err(tower_mcp::Error::tool(
                "limit_offset must be smaller than top_k",
            ));
        }
        let limit_num = self
            .limit_num
            .unwrap_or_else(|| (top_k - limit_offset).min(10));
        if limit_num == 0 || limit_num > MAX_SEARCH_RESULTS {
            return Err(tower_mcp::Error::tool(format!(
                "limit_num must be between 1 and {MAX_SEARCH_RESULTS}"
            )));
        }
        if limit_offset.saturating_add(limit_num) > top_k {
            return Err(tower_mcp::Error::tool(
                "limit_offset + limit_num must not exceed top_k",
            ));
        }
        if self.return_fields.len() > MAX_SCHEMA_FIELDS {
            return Err(tower_mcp::Error::tool(format!(
                "return_fields must contain at most {MAX_SCHEMA_FIELDS} items"
            )));
        }
        let score_alias = self
            .score_alias
            .as_deref()
            .unwrap_or(DEFAULT_VECTOR_SCORE_ALIAS)
            .to_string();
        validate_query_identifier(&score_alias, "score_alias")?;
        let mut fields = BTreeSet::new();
        for field in &self.return_fields {
            if field.is_empty() {
                return Err(tower_mcp::Error::tool(
                    "return_fields must not contain empty names",
                ));
            }
            if field == &score_alias {
                return Err(tower_mcp::Error::tool(
                    "return_fields must not repeat score_alias",
                ));
            }
            if !fields.insert(field) {
                return Err(tower_mcp::Error::tool(
                    "return_fields must not contain duplicates",
                ));
            }
        }
        if self.ef_runtime == Some(0) {
            return Err(tower_mcp::Error::tool(
                "ef_runtime must be greater than zero",
            ));
        }
        if let Some(ratio) = self.shard_k_ratio {
            let hundredths = ratio * 100.0;
            if !ratio.is_finite()
                || !(0.1..=1.0).contains(&ratio)
                || (hundredths - hundredths.round()).abs() > f64::EPSILON * 100.0
            {
                return Err(tower_mcp::Error::tool(
                    "shard_k_ratio must be between 0.1 and 1.0 with at most two decimals",
                ));
            }
        }
        Ok(ValidatedVectorQuery {
            top_k,
            limit_offset,
            limit_num,
            score_alias,
        })
    }
}

fn validate_query_identifier(value: &str, name: &str) -> tower_mcp::Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain only ASCII letters, digits, and underscores"
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum HybridPolicy {
    Batches,
    AdHocBruteForce,
}

impl HybridPolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Batches => "BATCHES",
            Self::AdHocBruteForce => "ADHOC_BF",
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum GeoUnit {
    M,
    Km,
    Ft,
    Mi,
}

impl GeoUnit {
    fn as_str(self) -> &'static str {
        match self {
            Self::M => "m",
            Self::Km => "km",
            Self::Ft => "ft",
            Self::Mi => "mi",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum HybridFilter {
    /// Match one escaped literal text value.
    Text { field: String, value: String },
    /// Match any one of the escaped literal tag values.
    Tag {
        field: String,
        #[schemars(length(min = 1, max = 100))]
        values: Vec<String>,
    },
    /// Match a numeric range. Omitted endpoints mean negative or positive infinity.
    Numeric {
        field: String,
        #[serde(default)]
        minimum: Option<f64>,
        #[serde(default)]
        maximum: Option<f64>,
        #[serde(default)]
        minimum_exclusive: bool,
        #[serde(default)]
        maximum_exclusive: bool,
    },
    /// Match a point within a radius of a geographic coordinate.
    Geo {
        field: String,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    },
}

impl HybridFilter {
    fn query(&self) -> tower_mcp::Result<String> {
        match self {
            Self::Text { field, value } => {
                validate_query_identifier(field, "text filter field")?;
                if value.is_empty() {
                    return Err(tower_mcp::Error::tool(
                        "text filter value must not be empty",
                    ));
                }
                Ok(format!("@{field}:{}", escape_query_literal(value)))
            }
            Self::Tag { field, values } => {
                validate_query_identifier(field, "tag filter field")?;
                if values.is_empty() || values.len() > MAX_SCHEMA_FIELDS {
                    return Err(tower_mcp::Error::tool(format!(
                        "tag filter values must contain between 1 and {MAX_SCHEMA_FIELDS} items"
                    )));
                }
                if values.iter().any(String::is_empty) {
                    return Err(tower_mcp::Error::tool(
                        "tag filter values must not be empty",
                    ));
                }
                Ok(format!(
                    "@{field}:{{{}}}",
                    values
                        .iter()
                        .map(|value| escape_query_literal(value))
                        .collect::<Vec<_>>()
                        .join("|")
                ))
            }
            Self::Numeric {
                field,
                minimum,
                maximum,
                minimum_exclusive,
                maximum_exclusive,
            } => {
                validate_query_identifier(field, "numeric filter field")?;
                if minimum.is_none() && maximum.is_none() {
                    return Err(tower_mcp::Error::tool(
                        "numeric filter requires minimum, maximum, or both",
                    ));
                }
                if minimum.is_some_and(|value| !value.is_finite())
                    || maximum.is_some_and(|value| !value.is_finite())
                {
                    return Err(tower_mcp::Error::tool(
                        "numeric filter bounds must be finite",
                    ));
                }
                if let (Some(minimum), Some(maximum)) = (minimum, maximum)
                    && minimum > maximum
                {
                    return Err(tower_mcp::Error::tool(
                        "numeric filter minimum must not exceed maximum",
                    ));
                }
                let minimum = numeric_bound(*minimum, *minimum_exclusive, "-inf");
                let maximum = numeric_bound(*maximum, *maximum_exclusive, "+inf");
                Ok(format!("@{field}:[{minimum} {maximum}]"))
            }
            Self::Geo {
                field,
                longitude,
                latitude,
                radius,
                unit,
            } => {
                validate_query_identifier(field, "geo filter field")?;
                if !longitude.is_finite()
                    || !(-180.0..=180.0).contains(longitude)
                    || !latitude.is_finite()
                    || !(-90.0..=90.0).contains(latitude)
                    || !radius.is_finite()
                    || *radius <= 0.0
                {
                    return Err(tower_mcp::Error::tool(
                        "geo filter requires longitude -180..180, latitude -90..90, and a positive finite radius",
                    ));
                }
                Ok(format!(
                    "@{field}:[{longitude} {latitude} {radius} {}]",
                    unit.as_str()
                ))
            }
        }
    }
}

fn numeric_bound(value: Option<f64>, exclusive: bool, infinity: &str) -> String {
    match value {
        Some(value) if exclusive => format!("({value}"),
        Some(value) => value.to_string(),
        None => infinity.to_string(),
    }
}

fn escape_query_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_alphanumeric() || character == '_' {
            escaped.push(character);
        } else {
            escaped.push('\\');
            escaped.push(character);
        }
    }
    escaped
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtHybridSearchInput {
    #[serde(flatten)]
    vector_query: VectorQueryInput,
    /// Typed text, tag, numeric, and geo clauses combined with AND.
    #[schemars(length(min = 1, max = 100))]
    filters: Vec<HybridFilter>,
    /// Optional Query Engine hybrid execution policy.
    #[serde(default)]
    hybrid_policy: Option<HybridPolicy>,
    /// Optional batch size, valid only with hybrid_policy=batches.
    #[serde(default)]
    batch_size: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchFieldValue {
    name: String,
    name_encoding: ValueEncoding,
    value: JsonValue,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorSearchDocument {
    id: String,
    id_encoding: ValueEncoding,
    distance: f64,
    fields: Vec<SearchFieldValue>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct VectorSearchOutput {
    index: String,
    vector_field: String,
    data_type: VectorDataType,
    dimensions: usize,
    query: String,
    score_alias: String,
    total: u64,
    top_k: u64,
    limit_offset: u64,
    limit_num: u64,
    count: usize,
    results: Vec<VectorSearchDocument>,
    page: PageMetadata,
}

fn redis_text(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) => Ok(value),
        RedisValue::SimpleString(value) => Ok(value.into_bytes()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected text value: {other:?}"
        ))),
    }
}

fn redis_number(value: RedisValue, context: &str) -> tower_mcp::Result<f64> {
    let number = match value {
        RedisValue::Double(value) => value,
        RedisValue::Integer(value) => value as f64,
        RedisValue::BulkString(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| tower_mcp::Error::tool(format!("{context} was not numeric")))?,
        RedisValue::SimpleString(value) => value
            .parse()
            .map_err(|_| tower_mcp::Error::tool(format!("{context} was not numeric")))?,
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "{context} returned an unexpected value: {other:?}"
            )));
        }
    };
    if number.is_finite() {
        Ok(number)
    } else {
        Err(tower_mcp::Error::tool(format!("{context} was not finite")))
    }
}

fn vector_search_response(
    value: RedisValue,
    score_alias: &str,
) -> tower_mcp::Result<(u64, Vec<VectorSearchDocument>)> {
    let RedisValue::Array(values) = value else {
        return Err(tower_mcp::Error::tool(
            "FT.SEARCH vector query returned a non-array response",
        ));
    };
    let mut values = values.into_iter();
    let total = match values.next() {
        Some(RedisValue::Integer(total)) => u64::try_from(total)
            .map_err(|_| tower_mcp::Error::tool("FT.SEARCH returned a negative total"))?,
        Some(RedisValue::BulkString(total)) => std::str::from_utf8(&total)
            .ok()
            .and_then(|total| total.parse().ok())
            .ok_or_else(|| tower_mcp::Error::tool("FT.SEARCH returned an invalid total"))?,
        _ => {
            return Err(tower_mcp::Error::tool(
                "FT.SEARCH returned no numeric total",
            ));
        }
    };
    let remaining = values.collect::<Vec<_>>();
    if !remaining.len().is_multiple_of(2) {
        return Err(tower_mcp::Error::tool(
            "FT.SEARCH vector response did not contain document/field pairs",
        ));
    }
    let mut documents = Vec::with_capacity(remaining.len() / 2);
    let mut remaining = remaining.into_iter();
    while let (Some(id), Some(fields)) = (remaining.next(), remaining.next()) {
        let (id, id_encoding) = super::encode_bytes(redis_text(id, "FT.SEARCH document id")?);
        let pairs = match fields {
            RedisValue::Array(fields) => {
                if !fields.len().is_multiple_of(2) {
                    return Err(tower_mcp::Error::tool(
                        "FT.SEARCH returned an odd number of document field elements",
                    ));
                }
                let mut pairs = Vec::with_capacity(fields.len() / 2);
                let mut fields = fields.into_iter();
                while let (Some(name), Some(value)) = (fields.next(), fields.next()) {
                    pairs.push((name, value));
                }
                pairs
            }
            RedisValue::Map(fields) => fields,
            RedisValue::Nil => Vec::new(),
            other => {
                return Err(tower_mcp::Error::tool(format!(
                    "FT.SEARCH returned invalid document fields: {other:?}"
                )));
            }
        };
        let mut distance = None;
        let mut output_fields = Vec::with_capacity(pairs.len().saturating_sub(1));
        for (name, value) in pairs {
            let name = redis_text(name, "FT.SEARCH field name")?;
            if name == score_alias.as_bytes() {
                distance = Some(redis_number(value, "FT.SEARCH vector distance")?);
            } else {
                let (name, name_encoding) = super::encode_bytes(name);
                output_fields.push(SearchFieldValue {
                    name,
                    name_encoding,
                    value: redis_value_to_json(&value),
                });
            }
        }
        let distance = distance.ok_or_else(|| {
            tower_mcp::Error::tool(format!(
                "FT.SEARCH response omitted distance field '{score_alias}'"
            ))
        })?;
        documents.push(VectorSearchDocument {
            id,
            id_encoding,
            distance,
            fields: output_fields,
        });
    }
    Ok((total, documents))
}

async fn run_vector_search(
    state: &ToolState,
    tool_name: &'static str,
    input: VectorQueryInput,
    primary_filter: String,
    hybrid_options: Option<(HybridPolicy, Option<u64>)>,
) -> tower_mcp::Result<VectorSearchOutput> {
    let validated = input.validate()?;
    state.validate_requested_entries(validated.limit_num as usize, "limit_num")?;
    if let Some((policy, batch_size)) = hybrid_options {
        if batch_size == Some(0) {
            return Err(tower_mcp::Error::tool(
                "batch_size must be greater than zero",
            ));
        }
        if batch_size.is_some() && policy != HybridPolicy::Batches {
            return Err(tower_mcp::Error::tool(
                "batch_size requires hybrid_policy=batches",
            ));
        }
    }
    let bytes = input.data_type.encode(&input.vector)?;
    let mut vector_clause = format!("KNN {} @{} $BLOB", validated.top_k, input.vector_field);
    if let Some(ef_runtime) = input.ef_runtime {
        vector_clause.push_str(&format!(" EF_RUNTIME {ef_runtime}"));
    }
    if let Some(ratio) = input.shard_k_ratio {
        vector_clause.push_str(&format!(" $SHARD_K_RATIO {ratio}"));
    }
    if let Some((policy, batch_size)) = hybrid_options {
        vector_clause.push_str(&format!(" HYBRID_POLICY {}", policy.as_str()));
        if let Some(batch_size) = batch_size {
            vector_clause.push_str(&format!(" BATCH_SIZE {batch_size}"));
        }
    }
    vector_clause.push_str(&format!(" AS {}", validated.score_alias));
    let query = format!("{primary_filter}=>[{vector_clause}]");
    let mut command = module_command(
        tool_name,
        AccessMode::ReadOnly,
        RedisModule::Search,
        "FT.SEARCH",
    );
    command.arg(input.index.as_str()).arg(query.as_str());
    command
        .arg("RETURN")
        .arg((input.return_fields.len() + 1).to_string())
        .arg(validated.score_alias.as_str())
        .args(input.return_fields.iter().map(String::as_str));
    command
        .arg("SORTBY")
        .arg(validated.score_alias.as_str())
        .arg("ASC")
        .arg("LIMIT")
        .arg(validated.limit_offset.to_string())
        .arg(validated.limit_num.to_string())
        .arg("PARAMS")
        .arg("2")
        .arg("BLOB")
        .arg(bytes)
        .arg("DIALECT")
        .arg("2");
    let value = state.raw(command, "vector FT.SEARCH failed").await?;
    let (total, results) = vector_search_response(value, &validated.score_alias)?;
    let returned = results.len();
    let available = total.min(validated.top_k);
    let next = validated.limit_offset.saturating_add(returned as u64);
    let next_offset = (returned > 0 && next < available).then_some(next);
    Ok(VectorSearchOutput {
        index: input.index,
        vector_field: input.vector_field,
        data_type: input.data_type,
        dimensions: input.vector.len(),
        query,
        score_alias: validated.score_alias,
        total,
        top_k: validated.top_k,
        limit_offset: validated.limit_offset,
        limit_num: validated.limit_num,
        count: returned,
        results,
        page: PageMetadata::offset(validated.limit_num as usize, returned, next_offset),
    })
}

fn ft_vector_search_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_vector_search")
        .title("Search Redis Vectors")
        .description(
            "Run a binary-safe, bounded KNN query and return typed documents, distances, and continuation metadata.",
        )
        .output_schema(output_schema::<VectorSearchOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<VectorQueryInput>| async move {
                let output = run_vector_search(
                    &state,
                    "redis_ft_vector_search",
                    input,
                    "*".to_string(),
                    None,
                )
                .await?;
                state.output_collection(
                    &output,
                    output.results.len(),
                    "Retry vector search with a smaller limit_num and page.continuation.offset.",
                )
            },
        )
        .build()
}

fn ft_hybrid_search_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_hybrid_search")
        .title("Search Redis Vectors With Filters")
        .description(
            "Run bounded KNN search combined with typed, escaped text, tag, numeric, and geo filters.",
        )
        .output_schema(output_schema::<VectorSearchOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtHybridSearchInput>| async move {
                if input.filters.is_empty() || input.filters.len() > MAX_SCHEMA_FIELDS {
                    return Err(tower_mcp::Error::tool(format!(
                        "filters must contain between 1 and {MAX_SCHEMA_FIELDS} items"
                    )));
                }
                let clauses = input
                    .filters
                    .iter()
                    .map(HybridFilter::query)
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let primary_filter = format!("({})", clauses.join(" "));
                let policy = input.hybrid_policy.unwrap_or(HybridPolicy::Batches);
                let output = run_vector_search(
                    &state,
                    "redis_ft_hybrid_search",
                    input.vector_query,
                    primary_filter,
                    Some((policy, input.batch_size)),
                )
                .await?;
                state.output_collection(
                    &output,
                    output.results.len(),
                    "Retry hybrid search with a smaller limit_num and page.continuation.offset.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SearchField {
    /// Hash field name or JSONPath expression.
    name: String,
    /// Query-facing field alias, strongly recommended for JSON indexes.
    #[serde(default)]
    alias: Option<String>,
    /// TEXT, TAG, NUMERIC, GEO, or VECTOR.
    field_type: String,
    /// Required VECTOR algorithm, type, dimension, metric, and tuning options.
    #[serde(default)]
    vector: Option<VectorFieldOptions>,
    /// Make the field available to SORTBY.
    #[serde(default)]
    sortable: bool,
    /// Store the field without indexing it.
    #[serde(default)]
    noindex: bool,
    /// Disable stemming for a TEXT field.
    #[serde(default)]
    nostem: bool,
}

impl SearchField {
    fn normalized_type(&self) -> tower_mcp::Result<String> {
        let field_type = self.field_type.to_ascii_uppercase();
        if !matches!(
            field_type.as_str(),
            "TEXT" | "TAG" | "NUMERIC" | "GEO" | "VECTOR"
        ) {
            return Err(tower_mcp::Error::tool(format!(
                "unsupported field_type '{}'; expected TEXT, TAG, NUMERIC, GEO, or VECTOR",
                self.field_type
            )));
        }
        if self.nostem && field_type != "TEXT" {
            return Err(tower_mcp::Error::tool(
                "nostem is valid only for TEXT fields",
            ));
        }
        if field_type == "VECTOR" {
            let vector = self
                .vector
                .as_ref()
                .ok_or_else(|| tower_mcp::Error::tool("VECTOR fields require vector options"))?;
            vector.validate()?;
            if self.sortable || self.noindex || self.nostem {
                return Err(tower_mcp::Error::tool(
                    "sortable, noindex, and nostem are not valid for VECTOR fields",
                ));
            }
        } else if self.vector.is_some() {
            return Err(tower_mcp::Error::tool(
                "vector options require field_type VECTOR",
            ));
        }
        Ok(field_type)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCreateInput {
    /// Search index name.
    index: String,
    /// Data model to index: HASH (default) or JSON.
    #[serde(default)]
    on: Option<String>,
    /// Key prefixes to include. Empty means Redis's default prefix.
    #[serde(default)]
    #[schemars(length(max = 100))]
    prefixes: Vec<String>,
    /// Index field definitions, bounded to 100 fields.
    #[schemars(length(min = 1, max = 100))]
    schema: Vec<SearchField>,
}

impl FtCreateInput {
    fn normalized_on(&self) -> tower_mcp::Result<String> {
        let on = self.on.as_deref().unwrap_or("HASH").to_ascii_uppercase();
        if matches!(on.as_str(), "HASH" | "JSON") {
            Ok(on)
        } else {
            Err(tower_mcp::Error::tool("on must be HASH or JSON"))
        }
    }

    fn validate(&self) -> tower_mcp::Result<String> {
        if self.schema.is_empty() || self.schema.len() > MAX_SCHEMA_FIELDS {
            return Err(tower_mcp::Error::tool(format!(
                "schema must contain between 1 and {MAX_SCHEMA_FIELDS} fields"
            )));
        }
        if self.prefixes.len() > MAX_SCHEMA_FIELDS {
            return Err(tower_mcp::Error::tool(format!(
                "prefixes must contain at most {MAX_SCHEMA_FIELDS} items"
            )));
        }
        for field in &self.schema {
            field.normalized_type()?;
        }
        self.normalized_on()
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCreateOutput {
    index: String,
    on: String,
    fields: usize,
    vector_fields: usize,
    created: bool,
}

fn ft_create_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_create")
        .title("Create Redis Search Index")
        .description(
            "Create a HASH or JSON search index with TEXT, TAG, NUMERIC, GEO, or typed FLAT/HNSW VECTOR fields.",
        )
        .output_schema(output_schema::<FtCreateOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtCreateInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_ft_create")?;
                let on = input.validate()?;
                let mut command = module_command(
                    "redis_ft_create",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "FT.CREATE",
                );
                command.arg(input.index.as_str()).arg("ON").arg(on.as_str());
                if !input.prefixes.is_empty() {
                    command
                        .arg("PREFIX")
                        .arg(input.prefixes.len().to_string())
                        .args(input.prefixes.iter().map(String::as_str));
                }
                command.arg("SCHEMA");
                for field in &input.schema {
                    command.arg(field.name.as_str());
                    if let Some(alias) = &field.alias {
                        command.arg("AS").arg(alias.as_str());
                    }
                    let field_type = field.normalized_type()?;
                    command.arg(field_type.as_str());
                    if let Some(vector) = &field.vector {
                        let arguments = vector.arguments()?;
                        command
                            .arg(vector.algorithm.as_str())
                            .arg(arguments.len().to_string())
                            .args(arguments);
                    }
                    if field.sortable {
                        command.arg("SORTABLE");
                    }
                    if field.noindex {
                        command.arg("NOINDEX");
                    }
                    if field.nostem {
                        command.arg("NOSTEM");
                    }
                }
                let _: String = state.query(command, "FT.CREATE failed").await?;
                state.output(&FtCreateOutput {
                    index: input.index,
                    on,
                    fields: input.schema.len(),
                    vector_fields: input
                        .schema
                        .iter()
                        .filter(|field| field.vector.is_some())
                        .count(),
                    created: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtDropIndexInput {
    /// Search index name.
    index: String,
    /// Also delete all documents indexed by this index.
    #[serde(default)]
    delete_docs: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtDropIndexOutput {
    index: String,
    dropped: bool,
    documents_deleted: bool,
}

fn ft_dropindex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_dropindex")
        .title("Drop Redis Search Index")
        .description(
            "Drop a search index. Optionally delete indexed documents too; requires full access.",
        )
        .output_schema(output_schema::<FtDropIndexOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtDropIndexInput>| async move {
                state.require(AccessMode::Full, "redis_ft_dropindex")?;
                let mut command = module_command(
                    "redis_ft_dropindex",
                    AccessMode::Full,
                    RedisModule::Search,
                    "FT.DROPINDEX",
                );
                command.arg(input.index.as_str());
                if input.delete_docs {
                    command.arg("DD");
                }
                let _: String = state.query(command, "FT.DROPINDEX failed").await?;
                state.output(&FtDropIndexOutput {
                    index: input.index,
                    dropped: true,
                    documents_deleted: input.delete_docs,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ft_list_tool(state.clone()));
    router = router.tool(ft_info_tool(state.clone()));
    router = router.tool(ft_search_tool(state.clone()));
    router = router.tool(vector_get_hash_tool(state.clone()));
    router = router.tool(ft_vector_search_tool(state.clone()));
    router.tool(ft_hybrid_search_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ft_create_tool(state.clone()));
    router.tool(vector_set_hash_tool(state))
}

pub(super) fn add_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(ft_dropindex_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_limits_and_sorting_are_validated() {
        let mut input = FtSearchInput {
            index: "idx".into(),
            query: "*".into(),
            limit_offset: None,
            limit_num: Some(MAX_SEARCH_RESULTS + 1),
            sortby: None,
            sortby_order: None,
            return_fields: None,
            nocontent: false,
            verbatim: false,
            withscores: false,
        };
        assert!(input.validate().is_err());

        input.limit_num = Some(10);
        input.sortby_order = Some("DESC".into());
        assert!(input.validate().is_err());
    }

    #[test]
    fn search_field_validation_rejects_unsupported_shapes() {
        let mut field = SearchField {
            name: "title".into(),
            alias: None,
            field_type: "VECTOR".into(),
            vector: None,
            sortable: false,
            noindex: false,
            nostem: false,
        };
        assert!(field.normalized_type().is_err());
        field.field_type = "TAG".into();
        field.nostem = true;
        assert!(field.normalized_type().is_err());
    }

    #[test]
    fn vector_encoding_is_little_endian_and_round_trips() {
        let vector = [1.0, -2.5, 0.25];
        let bytes = VectorDataType::Float32.encode(&vector).unwrap();
        assert_eq!(&bytes[..4], &1.0_f32.to_le_bytes());
        assert_eq!(
            VectorDataType::Float32.decode(&bytes).unwrap(),
            vec![1.0, -2.5, 0.25]
        );
        assert!(VectorDataType::Float64.decode(&bytes).is_err());
        assert!(VectorDataType::Float32.encode(&[]).is_err());
        assert!(VectorDataType::Float32.encode(&[f64::MAX]).is_err());
    }

    #[test]
    fn vector_field_arguments_cover_flat_and_hnsw_tuning() {
        let flat = VectorFieldOptions {
            algorithm: VectorAlgorithm::Flat,
            data_type: VectorDataType::Float32,
            dimensions: 3,
            distance_metric: "cosine".into(),
            initial_capacity: Some(100),
            block_size: Some(10),
            m: None,
            ef_construction: None,
            ef_runtime: None,
            epsilon: None,
        };
        assert_eq!(
            flat.arguments().unwrap(),
            [
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "COSINE",
                "INITIAL_CAP",
                "100",
                "BLOCK_SIZE",
                "10",
            ]
        );

        let hnsw = VectorFieldOptions {
            algorithm: VectorAlgorithm::Hnsw,
            data_type: VectorDataType::Float64,
            dimensions: 4,
            distance_metric: "L2".into(),
            initial_capacity: None,
            block_size: None,
            m: Some(16),
            ef_construction: Some(200),
            ef_runtime: Some(20),
            epsilon: Some(0.01),
        };
        assert!(hnsw.arguments().unwrap().contains(&"EF_RUNTIME".into()));
    }

    #[test]
    fn hybrid_filters_escape_literals_and_validate_ranges() {
        let text = HybridFilter::Text {
            field: "title".into(),
            value: "hello @world".into(),
        };
        assert_eq!(text.query().unwrap(), "@title:hello\\ \\@world");

        let numeric = HybridFilter::Numeric {
            field: "price".into(),
            minimum: Some(10.0),
            maximum: Some(5.0),
            minimum_exclusive: false,
            maximum_exclusive: false,
        };
        assert!(numeric.query().is_err());
    }
}
