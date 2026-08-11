//! Curated hash, list, set, and sorted-set operations.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, PageMetadata, ToolState, ValueEncoding, command, decode_input,
    destructive_annotations, output_schema, read_annotations, write_annotations,
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

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Hash field.
    field: String,
    /// Encoding of `field`.
    #[serde(default)]
    field_encoding: InputEncoding,
}

impl HashFieldInput {
    fn decoded_key(&self) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.key, self.key_encoding, "key")
    }

    fn decoded_field(&self) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.field, self.field_encoding, "field")
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HgetOutput {
    key: String,
    key_encoding: InputEncoding,
    field: String,
    field_encoding: InputEncoding,
    hash_exists: bool,
    field_exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

async fn key_exists(
    state: &ToolState,
    tool_name: &'static str,
    required_access: AccessMode,
    key: Vec<u8>,
) -> tower_mcp::Result<bool> {
    let mut command = command(tool_name, required_access, "EXISTS");
    command.arg(key);
    let exists: u64 = state.query(command, "EXISTS failed").await?;
    Ok(exists != 0)
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
                let key = input.decoded_key()?;
                let field = input.decoded_field()?;
                let mut command = command("redis_hget", AccessMode::ReadOnly, "HGET");
                command.arg(key.clone()).arg(field);
                let value: Option<Vec<u8>> = state.query(command, "HGET failed").await?;
                let field_exists = value.is_some();
                let hash_exists = if field_exists {
                    true
                } else {
                    key_exists(&state, "redis_hget", AccessMode::ReadOnly, key).await?
                };
                let (value, encoding) = match value {
                    Some(bytes) => {
                        let (value, encoding) = super::encode_bytes(bytes);
                        (Some(value), Some(encoding))
                    }
                    None => (None, None),
                };
                state.output(&HgetOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    field: input.field,
                    field_encoding: input.field_encoding,
                    hash_exists,
                    field_exists,
                    value,
                    encoding,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashKeyInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
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
    key_encoding: InputEncoding,
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
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashKeyInput>| async move {
                let mut command = command("redis_hgetall", AccessMode::ReadOnly, "HGETALL");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
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
                    key_encoding: input.key_encoding,
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

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedHashFieldInput {
    /// Hash field.
    field: String,
    /// Encoding of `field`.
    #[serde(default)]
    field_encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum HashFieldSelector {
    /// UTF-8 hash field shorthand.
    Utf8(String),
    /// Explicitly encoded hash field.
    Encoded(EncodedHashFieldInput),
}

#[derive(Debug)]
struct DecodedHashField {
    field: String,
    field_encoding: InputEncoding,
    bytes: Vec<u8>,
}

impl HashFieldSelector {
    fn decode(self, index: usize) -> tower_mcp::Result<DecodedHashField> {
        match self {
            Self::Utf8(field) => Ok(DecodedHashField {
                bytes: field.as_bytes().to_vec(),
                field,
                field_encoding: InputEncoding::Utf8,
            }),
            Self::Encoded(field) => Ok(DecodedHashField {
                bytes: decode_input(
                    &field.field,
                    field.field_encoding,
                    &format!("fields[{index}].field"),
                )?,
                field: field.field,
                field_encoding: field.field_encoding,
            }),
        }
    }
}

fn decode_hash_fields(
    fields: Vec<HashFieldSelector>,
    reject_duplicates: bool,
) -> tower_mcp::Result<Vec<DecodedHashField>> {
    validate_items(&fields, "fields")?;
    let fields = fields
        .into_iter()
        .enumerate()
        .map(|(index, field)| field.decode(index))
        .collect::<tower_mcp::Result<Vec<_>>>()?;
    if reject_duplicates {
        let mut unique = BTreeSet::new();
        if fields
            .iter()
            .any(|field| !unique.insert(field.bytes.clone()))
        {
            return Err(tower_mcp::Error::tool(
                "fields must not contain duplicate decoded field names",
            ));
        }
    }
    Ok(fields)
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldsInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// One to 1000 fields. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldSelector>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HexistsOutput {
    key: String,
    key_encoding: InputEncoding,
    field: String,
    field_encoding: InputEncoding,
    hash_exists: bool,
    field_exists: bool,
}

fn hexists_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hexists")
        .title("Check Redis Hash Field")
        .description(
            "Check whether one binary-safe field exists and distinguish a missing hash from a missing field.",
        )
        .output_schema(output_schema::<HexistsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldInput>| async move {
                let key = input.decoded_key()?;
                let mut command = command("redis_hexists", AccessMode::ReadOnly, "HEXISTS");
                command.arg(key.clone()).arg(input.decoded_field()?);
                let field_exists: bool = state.query(command, "HEXISTS failed").await?;
                let hash_exists = if field_exists {
                    true
                } else {
                    key_exists(
                        &state,
                        "redis_hexists",
                        AccessMode::ReadOnly,
                        key,
                    )
                    .await?
                };
                state.output(&HexistsOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    field: input.field,
                    field_encoding: input.field_encoding,
                    hash_exists,
                    field_exists,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HlenOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    length: u64,
}

fn hlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hlen")
        .title("Read Redis Hash Length")
        .description("Return the number of fields in a binary-safe Redis hash key.")
        .output_schema(output_schema::<HlenOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashKeyInput>| async move {
                let mut command = command("redis_hlen", AccessMode::ReadOnly, "HLEN");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let length = state.query(command, "HLEN failed").await?;
                state.output(&HlenOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: length != 0,
                    length,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HmgetEntry {
    field: String,
    field_encoding: InputEncoding,
    exists: bool,
    value: Option<String>,
    value_encoding: Option<ValueEncoding>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HmgetOutput {
    key: String,
    key_encoding: InputEncoding,
    hash_exists: bool,
    count: usize,
    values: Vec<HmgetEntry>,
}

fn hmget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hmget")
        .title("Get Redis Hash Fields")
        .description(
            "Read one bounded, ordered list of binary-safe hash fields. Missing fields are null; empty values remain present.",
        )
        .output_schema(output_schema::<HmgetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldsInput>| async move {
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let fields = decode_hash_fields(input.fields, false)?;
                let mut command = command("redis_hmget", AccessMode::ReadOnly, "HMGET");
                command.arg(key.clone());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let values: Vec<Option<Vec<u8>>> = state.query(command, "HMGET failed").await?;
                if values.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "HMGET returned {} values for {} fields",
                        values.len(),
                        fields.len()
                    )));
                }
                let hash_exists = if values.iter().any(Option::is_some) {
                    true
                } else {
                    key_exists(&state, "redis_hmget", AccessMode::ReadOnly, key).await?
                };
                let values = fields
                    .into_iter()
                    .zip(values)
                    .map(|(field, value)| match value {
                        Some(bytes) => {
                            let (value, value_encoding) = super::encode_bytes(bytes);
                            HmgetEntry {
                                field: field.field,
                                field_encoding: field.field_encoding,
                                exists: true,
                                value: Some(value),
                                value_encoding: Some(value_encoding),
                            }
                        }
                        None => HmgetEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            exists: false,
                            value: None,
                            value_encoding: None,
                        },
                    })
                    .collect::<Vec<_>>();
                let output = HmgetOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    hash_exists,
                    count: values.len(),
                    values,
                };
                state.output_collection(&output, output.count, "Retry HMGET with fewer fields.")
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HstrlenOutput {
    key: String,
    key_encoding: InputEncoding,
    field: String,
    field_encoding: InputEncoding,
    hash_exists: bool,
    field_exists: bool,
    length_bytes: u64,
}

fn hstrlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hstrlen")
        .title("Read Redis Hash Value Length")
        .description(
            "Read one hash value length in bytes and distinguish an empty value, missing field, and missing hash.",
        )
        .output_schema(output_schema::<HstrlenOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldInput>| async move {
                let key = input.decoded_key()?;
                let field = input.decoded_field()?;
                let mut hstrlen_command =
                    command("redis_hstrlen", AccessMode::ReadOnly, "HSTRLEN");
                hstrlen_command.arg(key.clone()).arg(field.clone());
                let length_bytes = state.query(hstrlen_command, "HSTRLEN failed").await?;
                let field_exists = if length_bytes != 0 {
                    true
                } else {
                    let mut exists = command("redis_hstrlen", AccessMode::ReadOnly, "HEXISTS");
                    exists.arg(key.clone()).arg(field);
                    state.query(exists, "HEXISTS failed").await?
                };
                let hash_exists = if field_exists {
                    true
                } else {
                    key_exists(
                        &state,
                        "redis_hstrlen",
                        AccessMode::ReadOnly,
                        key,
                    )
                    .await?
                };
                state.output(&HstrlenOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    field: input.field,
                    field_encoding: input.field_encoding,
                    hash_exists,
                    field_exists,
                    length_bytes,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HkeysOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    count: usize,
    fields: Vec<EncodedValue>,
}

fn hkeys_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hkeys")
        .title("Read Redis Hash Fields")
        .description(
            "Read all hash field names in deterministic byte order. The configured output budget is enforced; use redis_hscan for large hashes.",
        )
        .output_schema(output_schema::<HkeysOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashKeyInput>| async move {
                let mut command = command("redis_hkeys", AccessMode::ReadOnly, "HKEYS");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let mut fields: Vec<Vec<u8>> = state.query(command, "HKEYS failed").await?;
                fields.sort_unstable();
                let fields = fields
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = HkeysOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: !fields.is_empty(),
                    count: fields.len(),
                    fields,
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

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HvalsOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    count: usize,
    values: Vec<EncodedValue>,
}

fn hvals_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hvals")
        .title("Read Redis Hash Values")
        .description(
            "Read all hash values in deterministic byte order. The configured output budget is enforced; use redis_hscan for large hashes.",
        )
        .output_schema(output_schema::<HvalsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashKeyInput>| async move {
                let mut command = command("redis_hvals", AccessMode::ReadOnly, "HVALS");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let mut values: Vec<Vec<u8>> = state.query(command, "HVALS failed").await?;
                values.sort_unstable();
                let values = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = HvalsOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: !values.is_empty(),
                    count: values.len(),
                    values,
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

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashTtlStatus {
    FieldMissing,
    Persistent,
    Expiring,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HttlEntry {
    field: String,
    field_encoding: InputEncoding,
    status: HashTtlStatus,
    ttl_seconds: Option<u64>,
    redis_code: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HttlOutput {
    key: String,
    key_encoding: InputEncoding,
    hash_exists: bool,
    count: usize,
    fields: Vec<HttlEntry>,
}

fn httl_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_httl")
        .title("Read Redis Hash Field TTLs")
        .description(
            "Read remaining field TTLs in seconds on Redis 7.4 or newer. Results preserve request order and distinguish missing and persistent fields.",
        )
        .output_schema(output_schema::<HttlOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldsInput>| async move {
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let fields = decode_hash_fields(input.fields, false)?;
                let mut command = command("redis_httl", AccessMode::ReadOnly, "HTTL");
                command.arg(key.clone()).arg("FIELDS").arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let ttls: Vec<i64> = state.query(command, "HTTL failed").await?;
                if ttls.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "HTTL returned {} values for {} fields",
                        ttls.len(),
                        fields.len()
                    )));
                }
                let hash_exists = if ttls.iter().any(|ttl| *ttl != -2) {
                    true
                } else {
                    key_exists(&state, "redis_httl", AccessMode::ReadOnly, key).await?
                };
                let fields = fields
                    .into_iter()
                    .zip(ttls)
                    .map(|(field, redis_code)| {
                        let (status, ttl_seconds) = match redis_code {
                            -2 => (HashTtlStatus::FieldMissing, None),
                            -1 => (HashTtlStatus::Persistent, None),
                            ttl if ttl >= 0 => (HashTtlStatus::Expiring, Some(ttl as u64)),
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "HTTL returned unexpected field status {other}"
                                )));
                            }
                        };
                        Ok(HttlEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            status,
                            ttl_seconds,
                            redis_code,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = HttlOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    hash_exists,
                    count: fields.len(),
                    fields,
                };
                state.output_collection(&output, output.count, "Retry HTTL with fewer fields.")
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
struct HashScanInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Cursor returned by the previous page. Start with zero.
    #[serde(default)]
    cursor: u64,
    /// Glob-style field pattern.
    #[serde(default = "default_pattern")]
    pattern: String,
    /// Approximate number of fields Redis should inspect.
    #[serde(default = "default_scan_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HscanOutput {
    key: String,
    key_encoding: InputEncoding,
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
             Json(input): Json<HashScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let mut command = command("redis_hscan", AccessMode::ReadOnly, "HSCAN");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
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
                    key_encoding: input.key_encoding,
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

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetKeyInput {
    /// Redis set key.
    key: String,
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
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetKeyInput>| async move {
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
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// UTF-8 field-value object. Use either `fields` or `entries`.
    #[serde(default)]
    #[schemars(extend("minProperties" = 1, "maxProperties" = 1000))]
    fields: Option<BTreeMap<String, String>>,
    /// Explicitly encoded field-value entries. Use either `fields` or `entries`.
    #[serde(default)]
    #[schemars(length(min = 1, max = 1000))]
    entries: Option<Vec<HashFieldValueInput>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashFieldValueInput {
    /// Hash field.
    field: String,
    /// Encoding of `field`.
    #[serde(default)]
    field_encoding: InputEncoding,
    /// Hash value.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
}

struct DecodedHashEntry {
    field: Vec<u8>,
    value: Vec<u8>,
}

impl HsetInput {
    fn decode_entries(&mut self) -> tower_mcp::Result<Vec<DecodedHashEntry>> {
        match (self.fields.take(), self.entries.take()) {
            (Some(fields), None) => {
                validate_items(&fields.iter().collect::<Vec<_>>(), "fields")?;
                Ok(fields
                    .into_iter()
                    .map(|(field, value)| DecodedHashEntry {
                        field: field.into_bytes(),
                        value: value.into_bytes(),
                    })
                    .collect())
            }
            (None, Some(entries)) => {
                validate_items(&entries, "entries")?;
                let entries = entries
                    .into_iter()
                    .enumerate()
                    .map(|(index, entry)| {
                        Ok(DecodedHashEntry {
                            field: decode_input(
                                &entry.field,
                                entry.field_encoding,
                                &format!("entries[{index}].field"),
                            )?,
                            value: decode_input(
                                &entry.value,
                                entry.value_encoding,
                                &format!("entries[{index}].value"),
                            )?,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let mut unique = BTreeSet::new();
                if entries
                    .iter()
                    .any(|entry| !unique.insert(entry.field.clone()))
                {
                    return Err(tower_mcp::Error::tool(
                        "entries must not contain duplicate decoded field names",
                    ));
                }
                Ok(entries)
            }
            (Some(_), Some(_)) => Err(tower_mcp::Error::tool(
                "provide exactly one of fields or entries, not both",
            )),
            (None, None) => Err(tower_mcp::Error::tool(
                "provide exactly one of fields or entries",
            )),
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HsetOutput {
    key: String,
    key_encoding: InputEncoding,
    fields_set: usize,
    fields_added: u64,
    fields_updated: u64,
}

fn hset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hset")
        .title("Set Redis Hash Fields")
        .description(
            "Set 1 to 1000 hash fields atomically in one HSET. The UTF-8 object form is deterministic; the binary entry form rejects duplicate decoded fields.",
        )
        .output_schema(output_schema::<HsetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(mut input): Json<HsetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hset")?;
                let entries = input.decode_entries()?;
                let fields_set = entries.len();
                let mut command = command("redis_hset", AccessMode::ReadWrite, "HSET");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for entry in entries {
                    command.arg(entry.field).arg(entry.value);
                }
                let fields_added: u64 = state.query(command, "HSET failed").await?;
                if fields_added > fields_set as u64 {
                    return Err(tower_mcp::Error::tool(format!(
                        "HSET reported {fields_added} added fields for {fields_set} inputs"
                    )));
                }
                state.output(&HsetOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    fields_set,
                    fields_added,
                    fields_updated: fields_set as u64 - fields_added,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HincrbyInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Hash field.
    field: String,
    /// Encoding of `field`.
    #[serde(default)]
    field_encoding: InputEncoding,
    /// Signed integer increment.
    increment: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HincrbyOutput {
    key: String,
    key_encoding: InputEncoding,
    field: String,
    field_encoding: InputEncoding,
    value: i64,
}

fn hincrby_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hincrby")
        .title("Increment Redis Hash Integer")
        .description(
            "Increment a binary-safe hash field by a signed integer and return the resulting value.",
        )
        .output_schema(output_schema::<HincrbyOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HincrbyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hincrby")?;
                let mut command = command("redis_hincrby", AccessMode::ReadWrite, "HINCRBY");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_input(
                        &input.field,
                        input.field_encoding,
                        "field",
                    )?)
                    .arg(input.increment.to_string());
                let value = state.query(command, "HINCRBY failed").await?;
                state.output(&HincrbyOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    field: input.field,
                    field_encoding: input.field_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HincrbyfloatInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Hash field.
    field: String,
    /// Encoding of `field`.
    #[serde(default)]
    field_encoding: InputEncoding,
    /// Finite floating-point increment.
    increment: f64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HincrbyfloatOutput {
    key: String,
    key_encoding: InputEncoding,
    field: String,
    field_encoding: InputEncoding,
    value: String,
}

fn hincrbyfloat_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hincrbyfloat")
        .title("Increment Redis Hash Decimal")
        .description(
            "Increment a binary-safe hash field by a finite decimal and return Redis's canonical decimal string.",
        )
        .output_schema(output_schema::<HincrbyfloatOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<HincrbyfloatInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hincrbyfloat")?;
                if !input.increment.is_finite() {
                    return Err(tower_mcp::Error::tool("increment must be finite"));
                }
                let mut command = command(
                    "redis_hincrbyfloat",
                    AccessMode::ReadWrite,
                    "HINCRBYFLOAT",
                );
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(decode_input(
                        &input.field,
                        input.field_encoding,
                        "field",
                    )?)
                    .arg(input.increment.to_string());
                let value = state.query(command, "HINCRBYFLOAT failed").await?;
                state.output(&HincrbyfloatOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    field: input.field,
                    field_encoding: input.field_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashExpireCondition {
    Nx,
    Xx,
    Gt,
    Lt,
}

impl HashExpireCondition {
    fn redis_token(self) -> &'static str {
        match self {
            Self::Nx => "NX",
            Self::Xx => "XX",
            Self::Gt => "GT",
            Self::Lt => "LT",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HexpireInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Positive relative expiration in seconds.
    #[schemars(range(min = 1))]
    seconds: i64,
    /// Optional NX, XX, GT, or LT condition.
    #[serde(default)]
    condition: Option<HashExpireCondition>,
    /// One to 1000 unique fields. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldSelector>,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashExpireStatus {
    FieldMissing,
    ConditionNotMet,
    ExpirationSet,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashExpireEntry {
    field: String,
    field_encoding: InputEncoding,
    status: HashExpireStatus,
    redis_code: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HexpireOutput {
    key: String,
    key_encoding: InputEncoding,
    seconds: i64,
    condition: Option<HashExpireCondition>,
    count: usize,
    expirations_set: usize,
    condition_not_met: usize,
    fields_missing: usize,
    fields: Vec<HashExpireEntry>,
}

fn hexpire_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hexpire")
        .title("Expire Redis Hash Fields")
        .description(
            "Set positive relative field expirations on Redis 7.4 or newer with a typed NX, XX, GT, or LT condition.",
        )
        .output_schema(output_schema::<HexpireOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HexpireInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hexpire")?;
                if input.seconds <= 0 {
                    return Err(tower_mcp::Error::tool(
                        "seconds must be greater than zero",
                    ));
                }
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let fields = decode_hash_fields(input.fields, true)?;
                let mut command = command("redis_hexpire", AccessMode::ReadWrite, "HEXPIRE");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.seconds.to_string());
                if let Some(condition) = input.condition {
                    command.arg(condition.redis_token());
                }
                command.arg("FIELDS").arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let results: Vec<i64> = state.query(command, "HEXPIRE failed").await?;
                if results.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "HEXPIRE returned {} values for {} fields",
                        results.len(),
                        fields.len()
                    )));
                }
                let mut expirations_set = 0;
                let mut condition_not_met = 0;
                let mut fields_missing = 0;
                let fields = fields
                    .into_iter()
                    .zip(results)
                    .map(|(field, redis_code)| {
                        let status = match redis_code {
                            -2 => {
                                fields_missing += 1;
                                HashExpireStatus::FieldMissing
                            }
                            0 => {
                                condition_not_met += 1;
                                HashExpireStatus::ConditionNotMet
                            }
                            1 => {
                                expirations_set += 1;
                                HashExpireStatus::ExpirationSet
                            }
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "HEXPIRE returned unexpected field status {other}"
                                )));
                            }
                        };
                        Ok(HashExpireEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            status,
                            redis_code,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = HexpireOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    seconds: input.seconds,
                    condition: input.condition,
                    count: fields.len(),
                    expirations_set,
                    condition_not_met,
                    fields_missing,
                    fields,
                };
                state.output_collection(&output, output.count, "Retry HEXPIRE with fewer fields.")
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashPersistStatus {
    FieldMissing,
    AlreadyPersistent,
    ExpirationRemoved,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashPersistEntry {
    field: String,
    field_encoding: InputEncoding,
    status: HashPersistStatus,
    redis_code: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HpersistOutput {
    key: String,
    key_encoding: InputEncoding,
    count: usize,
    expirations_removed: usize,
    already_persistent: usize,
    fields_missing: usize,
    fields: Vec<HashPersistEntry>,
}

fn hpersist_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hpersist")
        .title("Persist Redis Hash Fields")
        .description("Remove field expirations on Redis 7.4 or newer.")
        .output_schema(output_schema::<HpersistOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldsInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hpersist")?;
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let fields = decode_hash_fields(input.fields, true)?;
                let mut command = command("redis_hpersist", AccessMode::ReadWrite, "HPERSIST");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg("FIELDS")
                    .arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let results: Vec<i64> = state.query(command, "HPERSIST failed").await?;
                if results.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "HPERSIST returned {} values for {} fields",
                        results.len(),
                        fields.len()
                    )));
                }
                let mut expirations_removed = 0;
                let mut already_persistent = 0;
                let mut fields_missing = 0;
                let fields = fields
                    .into_iter()
                    .zip(results)
                    .map(|(field, redis_code)| {
                        let status = match redis_code {
                            -2 => {
                                fields_missing += 1;
                                HashPersistStatus::FieldMissing
                            }
                            -1 => {
                                already_persistent += 1;
                                HashPersistStatus::AlreadyPersistent
                            }
                            1 => {
                                expirations_removed += 1;
                                HashPersistStatus::ExpirationRemoved
                            }
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "HPERSIST returned unexpected field status {other}"
                                )));
                            }
                        };
                        Ok(HashPersistEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            status,
                            redis_code,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = HpersistOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    count: fields.len(),
                    expirations_removed,
                    already_persistent,
                    fields_missing,
                    fields,
                };
                state.output_collection(&output, output.count, "Retry HPERSIST with fewer fields.")
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HdelOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    deleted: u64,
}

fn hdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hdel")
        .title("Delete Redis Hash Fields")
        .description("Permanently delete 1 to 1000 unique hash fields. Requires full access.")
        .output_schema(output_schema::<HdelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HashFieldsInput>| async move {
                state.require(AccessMode::Full, "redis_hdel")?;
                let fields = decode_hash_fields(input.fields, true)?;
                let requested = fields.len();
                let mut command = command("redis_hdel", AccessMode::Full, "HDEL");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for field in fields {
                    command.arg(field.bytes);
                }
                let deleted = state.query(command, "HDEL failed").await?;
                state.output(&HdelOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    deleted,
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
    router = router.tool(hexists_tool(state.clone()));
    router = router.tool(hkeys_tool(state.clone()));
    router = router.tool(hlen_tool(state.clone()));
    router = router.tool(hmget_tool(state.clone()));
    router = router.tool(hscan_tool(state.clone()));
    router = router.tool(hstrlen_tool(state.clone()));
    router = router.tool(httl_tool(state.clone()));
    router = router.tool(hvals_tool(state.clone()));
    router = router.tool(lrange_tool(state.clone()));
    router = router.tool(smembers_tool(state.clone()));
    router = router.tool(sscan_tool(state.clone()));
    router = router.tool(zrange_tool(state.clone()));
    router.tool(zscan_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hset_tool(state.clone()));
    router = router.tool(hexpire_tool(state.clone()));
    router = router.tool(hincrby_tool(state.clone()));
    router = router.tool(hincrbyfloat_tool(state.clone()));
    router = router.tool(hpersist_tool(state.clone()));
    router = router.tool(lpush_tool(state.clone()));
    router = router.tool(sadd_tool(state.clone()));
    router.tool(zadd_tool(state))
}

pub(super) fn add_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(hdel_tool(state))
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
