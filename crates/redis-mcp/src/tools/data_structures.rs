//! Curated hash, list, set, and sorted-set operations.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
use crate::{AccessMode, RedisValue, RedisVersion};

const MAX_ITEMS: usize = 1_000;
const DEFAULT_RETURNED_COLLECTION_BYTES: usize = 64 * 1024;
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

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashTtlMode {
    #[default]
    RemainingSeconds,
    RemainingMilliseconds,
    UnixSeconds,
    UnixMilliseconds,
}

impl HashTtlMode {
    fn command(self) -> &'static str {
        match self {
            Self::RemainingSeconds => "HTTL",
            Self::RemainingMilliseconds => "HPTTL",
            Self::UnixSeconds => "HEXPIRETIME",
            Self::UnixMilliseconds => "HPEXPIRETIME",
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HttlEntry {
    field: String,
    field_encoding: InputEncoding,
    status: HashTtlStatus,
    value: Option<u64>,
    /// Compatibility field populated only for remaining-seconds mode.
    ttl_seconds: Option<u64>,
    redis_code: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HttlOutput {
    key: String,
    key_encoding: InputEncoding,
    hash_exists: bool,
    mode: HashTtlMode,
    count: usize,
    fields: Vec<HttlEntry>,
}

fn httl_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_httl")
        .title("Read Redis Hash Field TTLs")
        .description(
            "Read remaining or absolute hash-field expirations in seconds or milliseconds on Redis 7.4 or newer. Results preserve request order and distinguish missing and persistent fields.",
        )
        .output_schema(output_schema::<HttlOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HttlInput>| async move {
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let fields = decode_hash_fields(input.fields, false)?;
                let command_name = input.mode.command();
                let mut command = command("redis_httl", AccessMode::ReadOnly, command_name);
                command.arg(key.clone()).arg("FIELDS").arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let ttls: Vec<i64> = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                if ttls.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                            "{command_name} returned {} values for {} fields",
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
                        let (status, value) = match redis_code {
                            -2 => (HashTtlStatus::FieldMissing, None),
                            -1 => (HashTtlStatus::Persistent, None),
                            ttl if ttl >= 0 => (HashTtlStatus::Expiring, Some(ttl as u64)),
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "{command_name} returned unexpected field status {other}"
                                )));
                            }
                        };
                        Ok(HttlEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            status,
                            value,
                            ttl_seconds: if matches!(
                                input.mode,
                                HashTtlMode::RemainingSeconds
                            ) {
                                value
                            } else {
                                None
                            },
                            redis_code,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = HttlOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    hash_exists,
                    mode: input.mode,
                    count: fields.len(),
                    fields,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry the hash expiration inspection with fewer fields.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HttlInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Remaining or absolute time representation to return.
    #[serde(default)]
    mode: HashTtlMode,
    /// One to 1000 unique fields. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldSelector>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HrandfieldInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Omit for one unique field. Positive counts return unique fields; negative counts permit duplicates.
    #[serde(default)]
    #[schemars(range(min = -1000, max = 1000))]
    count: Option<i64>,
    /// Include the value paired with each returned field. Requires `count`.
    #[serde(default)]
    with_values: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HrandfieldEntry {
    field: String,
    field_encoding: ValueEncoding,
    value: Option<String>,
    value_encoding: Option<ValueEncoding>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HrandfieldOutput {
    key: String,
    key_encoding: InputEncoding,
    hash_exists: bool,
    requested_count: Option<i64>,
    duplicates_allowed: bool,
    with_values: bool,
    returned: usize,
    entries: Vec<HrandfieldEntry>,
}

fn hrandfield_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hrandfield")
        .title("Sample Redis Hash Fields")
        .description(
            "Sample one or a bounded count of binary-safe hash fields. Positive counts are unique, negative counts permit duplicates, and WITHVALUES requires an explicit count.",
        )
        .output_schema(output_schema::<HrandfieldOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HrandfieldInput>| async move {
                if input.with_values && input.count.is_none() {
                    return Err(tower_mcp::Error::tool(
                        "with_values requires an explicit count",
                    ));
                }
                if let Some(count) = input.count {
                    if count == 0 {
                        return Err(tower_mcp::Error::tool("count must not be zero"));
                    }
                    let requested = usize::try_from(count.unsigned_abs()).map_err(|_| {
                        tower_mcp::Error::tool("absolute count exceeds supported bounds")
                    })?;
                    state.validate_requested_entries(requested, "count")?;
                }
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_hrandfield", AccessMode::ReadOnly, "HRANDFIELD");
                command.arg(key.clone());
                let entries = match input.count {
                    None => {
                        let field: Option<Vec<u8>> =
                            state.query(command, "HRANDFIELD failed").await?;
                        field
                            .into_iter()
                            .map(|field| {
                                let (field, field_encoding) = super::encode_bytes(field);
                                HrandfieldEntry {
                                    field,
                                    field_encoding,
                                    value: None,
                                    value_encoding: None,
                                }
                            })
                            .collect::<Vec<_>>()
                    }
                    Some(count) if input.with_values => {
                        command.arg(count.to_string()).arg("WITHVALUES");
                        let pairs: Vec<BinaryPair> =
                            state.query(command, "HRANDFIELD WITHVALUES failed").await?;
                        pairs
                            .into_iter()
                            .map(|(field, value)| {
                                let (field, field_encoding) = super::encode_bytes(field);
                                let (value, value_encoding) = super::encode_bytes(value);
                                HrandfieldEntry {
                                    field,
                                    field_encoding,
                                    value: Some(value),
                                    value_encoding: Some(value_encoding),
                                }
                            })
                            .collect::<Vec<_>>()
                    }
                    Some(count) => {
                        command.arg(count.to_string());
                        let fields: Vec<Vec<u8>> =
                            state.query(command, "HRANDFIELD failed").await?;
                        fields
                            .into_iter()
                            .map(|field| {
                                let (field, field_encoding) = super::encode_bytes(field);
                                HrandfieldEntry {
                                    field,
                                    field_encoding,
                                    value: None,
                                    value_encoding: None,
                                }
                            })
                            .collect::<Vec<_>>()
                    }
                };
                let hash_exists = if entries.is_empty() {
                    key_exists(&state, "redis_hrandfield", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                let output = HrandfieldOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    hash_exists,
                    requested_count: input.count,
                    duplicates_allowed: input.count.is_some_and(|count| count < 0),
                    with_values: input.with_values,
                    returned: entries.len(),
                    entries,
                };
                state.output_collection(
                    &output,
                    output.returned,
                    "Retry HRANDFIELD with a smaller absolute count.",
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
struct SetScanInput {
    /// Redis set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Cursor returned by the previous page. Start with zero.
    #[serde(default)]
    cursor: u64,
    /// Glob-style member pattern. The pattern itself is UTF-8.
    #[serde(default = "default_pattern")]
    pattern: String,
    /// Approximate number of members Redis should inspect.
    #[serde(default = "default_scan_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SetResultOrdering {
    ByteSorted,
    ByteSortedWithinPage,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SscanOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    cursor: u64,
    count: usize,
    ordering: SetResultOrdering,
    members: Vec<EncodedValue>,
    page: PageMetadata,
}

fn sscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sscan")
        .title("Scan Redis Set")
        .description(
            "Read one bounded SSCAN page. Pass page.continuation.cursor as cursor until page.complete is true. Members are binary-safe and byte-sorted within each page for deterministic structured output; Redis sets and cursor traversal remain unordered and may repeat members.",
        )
        .output_schema(output_schema::<SscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<SetScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_sscan", AccessMode::ReadOnly, "SSCAN");
                command
                    .arg(key.clone())
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                let (cursor, values): (u64, Vec<Vec<u8>>) =
                    state.query(command, "SSCAN failed").await?;
                let exists = if values.is_empty() {
                    key_exists(&state, "redis_sscan", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                let mut values = values;
                values.sort_unstable();
                let members = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = SscanOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    cursor,
                    count: members.len(),
                    ordering: SetResultOrdering::ByteSortedWithinPage,
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
    score: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZscanOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    cursor: u64,
    count: usize,
    members: Vec<ZscanEntry>,
    page: PageMetadata,
}

fn zscan_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zscan")
        .title("Scan Redis Sorted Set")
        .description(
            "Read one bounded ZSCAN page. Pass page.continuation.cursor as cursor until page.complete is true. Keys and members are binary-safe, and scores are returned as canonical strings instead of JSON numbers.",
        )
        .output_schema(output_schema::<ZscanOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<SetScanInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_zscan", AccessMode::ReadOnly, "ZSCAN");
                command
                    .arg(key.clone())
                    .arg(input.cursor.to_string())
                    .arg("MATCH")
                    .arg(input.pattern.as_str())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                let value = state.raw(command, "ZSCAN failed").await?;
                let (cursor, values) = decode_zscan_response(value)?;
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
                let exists = if members.is_empty() {
                    key_exists(&state, "redis_zscan", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                let output = ZscanOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
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

fn redis_array(value: RedisValue, command_name: &str) -> tower_mcp::Result<Vec<RedisValue>> {
    match value {
        RedisValue::Array(values) => Ok(values),
        RedisValue::Attribute { data, .. } => redis_array(*data, command_name),
        other => Err(tower_mcp::Error::tool(format!(
            "{command_name} returned an unexpected response shape: {other:?}"
        ))),
    }
}

fn redis_bytes(value: RedisValue, command_name: &str) -> tower_mcp::Result<Vec<u8>> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => Ok(value),
        RedisValue::SimpleString(value) => Ok(value.into_bytes()),
        RedisValue::VerbatimString { text, .. } => Ok(text.into_bytes()),
        RedisValue::Attribute { data, .. } => redis_bytes(*data, command_name),
        other => Err(tower_mcp::Error::tool(format!(
            "{command_name} returned a non-binary member: {other:?}"
        ))),
    }
}

fn decode_score(value: RedisValue, command_name: &str) -> tower_mcp::Result<Option<String>> {
    match value {
        RedisValue::Nil => Ok(None),
        RedisValue::BulkString(bytes) | RedisValue::BigNumber(bytes) => {
            String::from_utf8(bytes).map(Some).map_err(|_| {
                tower_mcp::Error::tool(format!(
                    "{command_name} returned a score that was not valid UTF-8"
                ))
            })
        }
        RedisValue::SimpleString(value) => Ok(Some(value)),
        RedisValue::Double(value) => Ok(Some(value.to_string())),
        RedisValue::Integer(value) => Ok(Some(value.to_string())),
        RedisValue::Attribute { data, .. } => decode_score(*data, command_name),
        other => Err(tower_mcp::Error::tool(format!(
            "{command_name} returned an unexpected score: {other:?}"
        ))),
    }
}

type ScoredMember = (Vec<u8>, String);
type ZscanResponse = (u64, Vec<ScoredMember>);

fn decode_score_pairs(
    value: RedisValue,
    command_name: &str,
) -> tower_mcp::Result<Vec<ScoredMember>> {
    let values = redis_array(value, command_name)?;
    let pairs = if values
        .iter()
        .all(|value| matches!(value, RedisValue::Array(_)))
    {
        values
            .into_iter()
            .map(|value| {
                let mut pair = redis_array(value, command_name)?;
                if pair.len() != 2 {
                    return Err(tower_mcp::Error::tool(format!(
                        "{command_name} returned a score pair with {} values",
                        pair.len()
                    )));
                }
                let score = pair.pop().expect("score pair length checked");
                let member = pair.pop().expect("score pair length checked");
                Ok((
                    redis_bytes(member, command_name)?,
                    decode_score(score, command_name)?.ok_or_else(|| {
                        tower_mcp::Error::tool(format!(
                            "{command_name} returned a nil score for a member"
                        ))
                    })?,
                ))
            })
            .collect::<tower_mcp::Result<Vec<_>>>()?
    } else {
        if values.len() % 2 != 0 {
            return Err(tower_mcp::Error::tool(format!(
                "{command_name} returned an odd number of member/score values"
            )));
        }
        let mut pairs = Vec::with_capacity(values.len() / 2);
        let mut values = values.into_iter();
        while let Some(member) = values.next() {
            let score = values.next().expect("even score pair length checked");
            pairs.push((
                redis_bytes(member, command_name)?,
                decode_score(score, command_name)?.ok_or_else(|| {
                    tower_mcp::Error::tool(format!(
                        "{command_name} returned a nil score for a member"
                    ))
                })?,
            ));
        }
        pairs
    };
    Ok(pairs)
}

fn decode_zscan_response(value: RedisValue) -> tower_mcp::Result<ZscanResponse> {
    let mut response = redis_array(value, "ZSCAN")?;
    if response.len() != 2 {
        return Err(tower_mcp::Error::tool(format!(
            "ZSCAN returned {} top-level values instead of two",
            response.len()
        )));
    }
    let members = response.pop().expect("ZSCAN response length checked");
    let cursor = response.pop().expect("ZSCAN response length checked");
    let cursor = match cursor {
        RedisValue::BulkString(bytes) | RedisValue::BigNumber(bytes) => std::str::from_utf8(&bytes)
            .ok()
            .and_then(|value| value.parse().ok()),
        RedisValue::SimpleString(value) => value.parse().ok(),
        RedisValue::Integer(value) => u64::try_from(value).ok(),
        _ => None,
    }
    .ok_or_else(|| tower_mcp::Error::tool("ZSCAN returned an invalid cursor"))?;
    Ok((cursor, decode_score_pairs(members, "ZSCAN")?))
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
enum RedisDecimalInput {
    /// JSON number shorthand. Use a string when every decimal digit must be preserved.
    Number(f64),
    /// Exact finite Redis decimal string.
    Exact(String),
}

impl RedisDecimalInput {
    fn finite_token(&self, name: &str) -> tower_mcp::Result<String> {
        let token = match self {
            Self::Number(value) => value.to_string(),
            Self::Exact(value) => value.clone(),
        };
        let value = token.parse::<f64>().map_err(|_| {
            tower_mcp::Error::tool(format!("{name} must be a valid finite Redis decimal"))
        })?;
        if !value.is_finite() {
            return Err(tower_mcp::Error::tool(format!(
                "{name} must be a finite Redis decimal"
            )));
        }
        Ok(token)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ZscoreBound {
    /// Negative infinity (`-inf`).
    NegativeInfinity,
    /// Positive infinity (`+inf`).
    PositiveInfinity,
    /// Include this finite score.
    Inclusive { value: RedisDecimalInput },
    /// Exclude this finite score.
    Exclusive { value: RedisDecimalInput },
}

impl ZscoreBound {
    fn redis_token(&self, name: &str) -> tower_mcp::Result<String> {
        match self {
            Self::NegativeInfinity => Ok("-inf".into()),
            Self::PositiveInfinity => Ok("+inf".into()),
            Self::Inclusive { value } => value.finite_token(name),
            Self::Exclusive { value } => Ok(format!("({}", value.finite_token(name)?)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ZlexBound {
    /// Negative infinity (`-`).
    NegativeInfinity,
    /// Positive infinity (`+`).
    PositiveInfinity,
    /// Include this binary-safe member value.
    Inclusive {
        value: String,
        #[serde(default)]
        encoding: InputEncoding,
    },
    /// Exclude this binary-safe member value.
    Exclusive {
        value: String,
        #[serde(default)]
        encoding: InputEncoding,
    },
}

impl ZlexBound {
    fn redis_token(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        match self {
            Self::NegativeInfinity => Ok(vec![b'-']),
            Self::PositiveInfinity => Ok(vec![b'+']),
            Self::Inclusive { value, encoding } | Self::Exclusive { value, encoding } => {
                let mut token = vec![if matches!(self, Self::Inclusive { .. }) {
                    b'['
                } else {
                    b'('
                }];
                token.extend(decode_input(value, *encoding, name)?);
                Ok(token)
            }
        }
    }
}

fn default_zrange_limit() -> usize {
    100
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ZrangeSpec {
    /// Inclusive zero-based ranks. Negative ranks count from the end.
    Rank {
        #[serde(default)]
        start: i64,
        #[serde(default = "default_stop")]
        stop: i64,
    },
    /// Score bounds. Inputs remain in natural minimum/maximum order when `rev` is true.
    Score {
        min: ZscoreBound,
        max: ZscoreBound,
        /// Number of matching members to skip. Follow `page.continuation.offset`.
        #[serde(default)]
        offset: u64,
        /// Maximum members to return.
        #[serde(default = "default_zrange_limit")]
        #[schemars(range(min = 1, max = 1000))]
        limit: usize,
    },
    /// Binary lexicographic bounds. This is meaningful when members share the same score.
    Lex {
        min: ZlexBound,
        max: ZlexBound,
        /// Number of matching members to skip. Follow `page.continuation.offset`.
        #[serde(default)]
        offset: u64,
        /// Maximum members to return.
        #[serde(default = "default_zrange_limit")]
        #[schemars(range(min = 1, max = 1000))]
        limit: usize,
    },
}

impl Default for ZrangeSpec {
    fn default() -> Self {
        Self::Rank { start: 0, stop: 99 }
    }
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
            "requested range contains {requested} entries; configured output limit of {limit} entries would be exceeded"
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
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
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
    key_encoding: InputEncoding,
    exists: bool,
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
            "Read a bounded inclusive range from a Redis list. Negative indexes count from the tail; mixed-sign ranges are rejected because they can be unbounded. Defaults to indexes 0 through 99. Follow page.continuation.start until page.complete is true. Missing lists return exists=false and no elements. Binary keys and elements are supported.",
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
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.to_string())
                    .arg(fetch_stop.to_string());
                let mut values: Vec<Vec<u8>> = state.query(command, "LRANGE failed").await?;
                let has_more = input.start >= 0 && requested > 0 && values.len() > requested;
                values.truncate(requested);
                let elements = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let exists = if elements.is_empty() {
                    key_exists(
                        &state,
                        "redis_lrange",
                        AccessMode::ReadOnly,
                        decode_input(&input.key, input.key_encoding, "key")?,
                    )
                    .await?
                } else {
                    true
                };
                let next_start = has_more.then(|| input.stop.saturating_add(1));
                let output = LrangeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
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
    key_encoding: InputEncoding,
    exists: bool,
    count: usize,
    ordering: SetResultOrdering,
    members: Vec<EncodedValue>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetKeyInput {
    /// Redis set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
}

impl SetKeyInput {
    fn decoded_key(&self) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.key, self.key_encoding, "key")
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedSetMemberInput {
    /// Redis set member.
    member: String,
    /// Encoding of `member`.
    #[serde(default)]
    member_encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum SetMemberSelector {
    /// UTF-8 set member shorthand.
    Utf8(String),
    /// Explicitly encoded set member.
    Encoded(EncodedSetMemberInput),
}

#[derive(Debug)]
struct DecodedSetMember {
    member: String,
    member_encoding: InputEncoding,
    bytes: Vec<u8>,
}

impl SetMemberSelector {
    fn decode(self, index: usize) -> tower_mcp::Result<DecodedSetMember> {
        match self {
            Self::Utf8(member) => Ok(DecodedSetMember {
                bytes: member.as_bytes().to_vec(),
                member,
                member_encoding: InputEncoding::Utf8,
            }),
            Self::Encoded(member) => Ok(DecodedSetMember {
                bytes: decode_input(
                    &member.member,
                    member.member_encoding,
                    &format!("members[{index}].member"),
                )?,
                member: member.member,
                member_encoding: member.member_encoding,
            }),
        }
    }
}

fn decode_set_members(members: Vec<SetMemberSelector>) -> tower_mcp::Result<Vec<DecodedSetMember>> {
    validate_items(&members, "members")?;
    members
        .into_iter()
        .enumerate()
        .map(|(index, member)| member.decode(index))
        .collect()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetMemberInput {
    /// Redis set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Redis set member.
    member: String,
    /// Encoding of `member`.
    #[serde(default)]
    member_encoding: InputEncoding,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetMembersInput {
    /// Redis set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// One to 1000 members. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    members: Vec<SetMemberSelector>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedSetKeyInput {
    /// Redis set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum SetKeySelector {
    /// UTF-8 Redis set key shorthand.
    Utf8(String),
    /// Explicitly encoded Redis set key.
    Encoded(EncodedSetKeyInput),
}

impl SetKeySelector {
    fn decode(self, index: usize) -> tower_mcp::Result<Vec<u8>> {
        match self {
            Self::Utf8(key) => Ok(key.into_bytes()),
            Self::Encoded(key) => {
                decode_input(&key.key, key.key_encoding, &format!("keys[{index}].key"))
            }
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetAlgebraInput {
    /// One to 1000 set keys. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<SetKeySelector>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScardOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    cardinality: u64,
}

fn scard_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_scard")
        .title("Read Redis Set Cardinality")
        .description("Return the number of members in a binary-safe Redis set key. Missing sets have cardinality zero and exists=false.")
        .output_schema(output_schema::<ScardOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetKeyInput>| async move {
                let mut command = command("redis_scard", AccessMode::ReadOnly, "SCARD");
                command.arg(input.decoded_key()?);
                let cardinality = state.query(command, "SCARD failed").await?;
                state.output(&ScardOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: cardinality != 0,
                    cardinality,
                })
            },
        )
        .build()
}

fn smembers_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_smembers")
        .title("Read Redis Set")
        .description(
            "Read all Redis set members in deterministic byte order. Redis sets themselves are unordered. The configured output budget is enforced; use redis_sscan for large sets. Keys and members are binary-safe.",
        )
        .output_schema(output_schema::<SmembersOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetKeyInput>| async move {
                let mut command = command("redis_smembers", AccessMode::ReadOnly, "SMEMBERS");
                command.arg(input.decoded_key()?);
                let mut values: Vec<Vec<u8>> = state.query(command, "SMEMBERS failed").await?;
                values.sort_unstable();
                let members = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = SmembersOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: !members.is_empty(),
                    count: members.len(),
                    ordering: SetResultOrdering::ByteSorted,
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

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SismemberOutput {
    key: String,
    key_encoding: InputEncoding,
    member: String,
    member_encoding: InputEncoding,
    set_exists: bool,
    is_member: bool,
}

fn sismember_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sismember")
        .title("Check Redis Set Membership")
        .description(
            "Check one binary-safe set member and distinguish a missing set from a non-member.",
        )
        .output_schema(output_schema::<SismemberOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMemberInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_sismember", AccessMode::ReadOnly, "SISMEMBER");
                command.arg(key.clone()).arg(decode_input(
                    &input.member,
                    input.member_encoding,
                    "member",
                )?);
                let is_member = state.query(command, "SISMEMBER failed").await?;
                let set_exists = if is_member {
                    true
                } else {
                    key_exists(&state, "redis_sismember", AccessMode::ReadOnly, key).await?
                };
                state.output(&SismemberOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    member: input.member,
                    member_encoding: input.member_encoding,
                    set_exists,
                    is_member,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetMembershipEntry {
    member: String,
    member_encoding: InputEncoding,
    is_member: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SmismemberOutput {
    key: String,
    key_encoding: InputEncoding,
    set_exists: bool,
    count: usize,
    members: Vec<SetMembershipEntry>,
}

fn smismember_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_smismember")
        .title("Check Multiple Redis Set Members")
        .description(
            "Check 1 to 1000 binary-safe members in one Redis set. Results remain aligned one-to-one with the requested member order. Requires Redis 6.2 or newer.",
        )
        .output_schema(output_schema::<SmismemberOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMembersInput>| async move {
                state.validate_requested_entries(input.members.len(), "members")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let members = decode_set_members(input.members)?;
                let mut command =
                    command("redis_smismember", AccessMode::ReadOnly, "SMISMEMBER");
                command.arg(key.clone());
                for member in &members {
                    command.arg(member.bytes.clone());
                }
                let membership: Vec<bool> = state.query(command, "SMISMEMBER failed").await?;
                if membership.len() != members.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "SMISMEMBER returned {} results for {} members",
                        membership.len(),
                        members.len()
                    )));
                }
                let set_exists = if membership.iter().any(|is_member| *is_member) {
                    true
                } else {
                    key_exists(&state, "redis_smismember", AccessMode::ReadOnly, key).await?
                };
                let members = members
                    .into_iter()
                    .zip(membership)
                    .map(|(member, is_member)| SetMembershipEntry {
                        member: member.member,
                        member_encoding: member.member_encoding,
                        is_member,
                    })
                    .collect::<Vec<_>>();
                let output = SmismemberOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    set_exists,
                    count: members.len(),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry SMISMEMBER with fewer members.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SetAlgebraOperation {
    Difference,
    Intersection,
    Union,
}

impl SetAlgebraOperation {
    fn tool_name(self) -> &'static str {
        match self {
            Self::Difference => "redis_sdiff",
            Self::Intersection => "redis_sinter",
            Self::Union => "redis_sunion",
        }
    }

    fn command_name(self) -> &'static str {
        match self {
            Self::Difference => "SDIFF",
            Self::Intersection => "SINTER",
            Self::Union => "SUNION",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Difference => "Read Redis Set Difference",
            Self::Intersection => "Read Redis Set Intersection",
            Self::Union => "Read Redis Set Union",
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetAlgebraOutput {
    operation: SetAlgebraOperation,
    requested_keys: usize,
    count: usize,
    ordering: SetResultOrdering,
    members: Vec<EncodedValue>,
}

fn set_algebra_tool(state: Arc<ToolState>, operation: SetAlgebraOperation) -> Tool {
    let tool_name = operation.tool_name();
    let command_name = operation.command_name();
    ToolBuilder::new(tool_name)
        .title(operation.title())
        .description(format!(
            "Compute a binary-safe, output-budgeted Redis set {} across 1 to 1000 keys. Results are byte-sorted for deterministic structured output even though Redis sets are unordered. On Redis Cluster, every key must share a hash slot.",
            match operation {
                SetAlgebraOperation::Difference => "difference",
                SetAlgebraOperation::Intersection => "intersection",
                SetAlgebraOperation::Union => "union",
            }
        ))
        .output_schema(output_schema::<SetAlgebraOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<SetAlgebraInput>| async move {
                validate_items(&input.keys, "keys")?;
                let requested_keys = input.keys.len();
                let keys = input
                    .keys
                    .into_iter()
                    .enumerate()
                    .map(|(index, key)| key.decode(index))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let mut command = command(tool_name, AccessMode::ReadOnly, command_name);
                command.args(keys);
                let mut values: Vec<Vec<u8>> = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                values.sort_unstable();
                let members = values
                    .into_iter()
                    .map(EncodedValue::from)
                    .collect::<Vec<_>>();
                let output = SetAlgebraOutput {
                    operation,
                    requested_keys,
                    count: members.len(),
                    ordering: SetResultOrdering::ByteSorted,
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Reduce the key set or use SSCAN on source sets to perform bounded client-side algebra.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum CardinalityOperation {
    SetDifference,
    SetUnion,
    SortedSetIntersection,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CardinalityInput {
    /// One to 1000 source keys. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<SetKeySelector>,
    /// Stop counting after reaching this cardinality. Zero means unlimited.
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SunioncardInput {
    /// One to 1000 set keys. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<SetKeySelector>,
    /// Use Redis's HyperLogLog-based approximate union cardinality.
    #[serde(default)]
    approximate: bool,
    /// Stop counting after reaching this cardinality. Zero means unlimited.
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CardinalityOutput {
    operation: CardinalityOperation,
    requested_keys: usize,
    cardinality: u64,
    approximate: bool,
    limit: Option<u64>,
    limit_reached: bool,
    cluster_requires_same_slot: bool,
}

fn append_counted_keys(
    command: &mut crate::RedisCommand,
    keys: Vec<SetKeySelector>,
) -> tower_mcp::Result<usize> {
    validate_items(&keys, "keys")?;
    let requested_keys = keys.len();
    command.arg(requested_keys.to_string());
    for (index, key) in keys.into_iter().enumerate() {
        command.arg(key.decode(index)?);
    }
    Ok(requested_keys)
}

fn append_cardinality_limit(command: &mut crate::RedisCommand, limit: Option<u64>) {
    if let Some(limit) = limit {
        command.arg("LIMIT").arg(limit.to_string());
    }
}

fn cardinality_output(
    operation: CardinalityOperation,
    requested_keys: usize,
    cardinality: u64,
    approximate: bool,
    limit: Option<u64>,
) -> CardinalityOutput {
    CardinalityOutput {
        operation,
        requested_keys,
        cardinality,
        approximate,
        limit,
        limit_reached: limit.is_some_and(|limit| limit > 0 && cardinality >= limit),
        cluster_requires_same_slot: requested_keys > 1,
    }
}

fn sdiffcard_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sdiffcard")
        .title("Count Redis Set Difference")
        .description(
            "Count a binary-safe set difference across 1 to 1000 same-slot keys without materializing member payloads. An optional limit stops work once reached. Requires Redis 8.10 or newer.",
        )
        .output_schema(output_schema::<CardinalityOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<CardinalityInput>| async move {
                let mut command =
                    command("redis_sdiffcard", AccessMode::ReadOnly, "SDIFFCARD");
                let requested_keys = append_counted_keys(&mut command, input.keys)?;
                append_cardinality_limit(&mut command, input.limit);
                let cardinality = state.query(command, "SDIFFCARD failed").await?;
                state.output(&cardinality_output(
                    CardinalityOperation::SetDifference,
                    requested_keys,
                    cardinality,
                    false,
                    input.limit,
                ))
            },
        )
        .build()
}

fn sunioncard_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sunioncard")
        .title("Count Redis Set Union")
        .description(
            "Count a binary-safe set union across 1 to 1000 same-slot keys without returning member payloads. Exact and HyperLogLog-based approximate modes support an optional stopping limit. Requires Redis 8.10 or newer.",
        )
        .output_schema(output_schema::<CardinalityOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SunioncardInput>| async move {
                let mut command =
                    command("redis_sunioncard", AccessMode::ReadOnly, "SUNIONCARD");
                let requested_keys = append_counted_keys(&mut command, input.keys)?;
                if input.approximate {
                    command.arg("APPROX");
                }
                append_cardinality_limit(&mut command, input.limit);
                let cardinality = state.query(command, "SUNIONCARD failed").await?;
                state.output(&cardinality_output(
                    CardinalityOperation::SetUnion,
                    requested_keys,
                    cardinality,
                    input.approximate,
                    input.limit,
                ))
            },
        )
        .build()
}

fn zintercard_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zintercard")
        .title("Count Redis Sorted Set Intersection")
        .description(
            "Count the intersection of 1 to 1000 binary-safe, same-slot sorted-set keys without materializing members or scores. An optional limit stops work once reached. Requires Redis 7 or newer.",
        )
        .output_schema(output_schema::<CardinalityOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<CardinalityInput>| async move {
                let mut command =
                    command("redis_zintercard", AccessMode::ReadOnly, "ZINTERCARD");
                let requested_keys = append_counted_keys(&mut command, input.keys)?;
                append_cardinality_limit(&mut command, input.limit);
                let cardinality = state.query(command, "ZINTERCARD failed").await?;
                state.output(&cardinality_output(
                    CardinalityOperation::SortedSetIntersection,
                    requested_keys,
                    cardinality,
                    false,
                    input.limit,
                ))
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SetStoreOperation {
    Difference,
    Intersection,
    Union,
}

impl SetStoreOperation {
    fn tool_name(self) -> &'static str {
        match self {
            Self::Difference => "redis_sdiffstore",
            Self::Intersection => "redis_sinterstore",
            Self::Union => "redis_sunionstore",
        }
    }

    fn command_name(self) -> &'static str {
        match self {
            Self::Difference => "SDIFFSTORE",
            Self::Intersection => "SINTERSTORE",
            Self::Union => "SUNIONSTORE",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Difference => "Store Redis Set Difference",
            Self::Intersection => "Store Redis Set Intersection",
            Self::Union => "Store Redis Set Union",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetStoreInput {
    /// Destination set key that Redis overwrites.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    /// One to 1000 source keys. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<SetKeySelector>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetStoreOutput {
    operation: SetStoreOperation,
    destination: String,
    destination_encoding: InputEncoding,
    source_count: usize,
    destination_cardinality: u64,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn set_store_tool(state: Arc<ToolState>, operation: SetStoreOperation) -> Tool {
    let tool_name = operation.tool_name();
    let command_name = operation.command_name();
    ToolBuilder::new(tool_name)
        .title(operation.title())
        .description(format!(
            "Permanently overwrite a destination with the {} of 1 to 1000 binary-safe source sets. Returns only the resulting cardinality; every key must share a Redis Cluster slot. Requires full access.",
            match operation {
                SetStoreOperation::Difference => "difference",
                SetStoreOperation::Intersection => "intersection",
                SetStoreOperation::Union => "union",
            }
        ))
        .output_schema(output_schema::<SetStoreOutput>())
        .annotations(destructive_annotations(!matches!(
            operation,
            SetStoreOperation::Difference
        )))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<SetStoreInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                validate_items(&input.keys, "keys")?;
                let source_count = input.keys.len();
                let mut command = command(tool_name, AccessMode::Full, command_name);
                command.arg(decode_input(
                    &input.destination,
                    input.destination_encoding,
                    "destination",
                )?);
                for (index, key) in input.keys.into_iter().enumerate() {
                    command.arg(key.decode(index)?);
                }
                let destination_cardinality = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                state.output(&SetStoreOutput {
                    operation,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    source_count,
                    destination_cardinality,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ZsetAggregate {
    #[default]
    Sum,
    Min,
    Max,
    Count,
}

impl ZsetAggregate {
    fn redis_token(self) -> &'static str {
        match self {
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Count => "COUNT",
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WeightedZsetKeyInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Optional finite score multiplier. Omitted weights default to one.
    #[serde(default)]
    weight: Option<RedisDecimalInput>,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum WeightedZsetKeySelector {
    /// UTF-8 sorted-set key shorthand with the default weight of one.
    Utf8(String),
    /// Binary-safe key with an optional exact weight.
    Encoded(WeightedZsetKeyInput),
}

struct DecodedWeightedZsetKey {
    key: Vec<u8>,
    weight: Option<String>,
}

impl WeightedZsetKeySelector {
    fn decode(self, index: usize) -> tower_mcp::Result<DecodedWeightedZsetKey> {
        match self {
            Self::Utf8(key) => Ok(DecodedWeightedZsetKey {
                key: key.into_bytes(),
                weight: None,
            }),
            Self::Encoded(source) => Ok(DecodedWeightedZsetKey {
                key: decode_input(
                    &source.key,
                    source.key_encoding,
                    &format!("sources[{index}].key"),
                )?,
                weight: source
                    .weight
                    .as_ref()
                    .map(|weight| weight.finite_token(&format!("sources[{index}].weight")))
                    .transpose()?,
            }),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct WeightedZsetStoreInput {
    /// Destination sorted-set key that Redis overwrites.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    /// One to 1000 sorted-set sources, each with an optional finite score weight.
    #[schemars(length(min = 1, max = 1000))]
    sources: Vec<WeightedZsetKeySelector>,
    /// How scores from matching members are combined. `count` requires Redis 8.8+.
    #[serde(default)]
    aggregate: ZsetAggregate,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZdiffstoreInput {
    /// Destination sorted-set key that Redis overwrites.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    /// One to 1000 source sorted-set keys. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<SetKeySelector>,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ZsetStoreOperation {
    Difference,
    Intersection,
    Union,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZsetStoreOutput {
    operation: ZsetStoreOperation,
    destination: String,
    destination_encoding: InputEncoding,
    source_count: usize,
    weighted: bool,
    aggregate: Option<ZsetAggregate>,
    destination_cardinality: u64,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn zdiffstore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zdiffstore")
        .title("Store Redis Sorted Set Difference")
        .description(
            "Permanently overwrite a destination with the difference of 1 to 1000 binary-safe source sorted sets. Returns only the destination cardinality; every key must share a Redis Cluster slot. Requires Redis 6.2+ and full access.",
        )
        .output_schema(output_schema::<ZsetStoreOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZdiffstoreInput>| async move {
                state.require(AccessMode::Full, "redis_zdiffstore")?;
                let mut command = command("redis_zdiffstore", AccessMode::Full, "ZDIFFSTORE");
                command.arg(decode_input(
                    &input.destination,
                    input.destination_encoding,
                    "destination",
                )?);
                let source_count = append_counted_keys(&mut command, input.keys)?;
                let destination_cardinality = state.query(command, "ZDIFFSTORE failed").await?;
                state.output(&ZsetStoreOutput {
                    operation: ZsetStoreOperation::Difference,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    source_count,
                    weighted: false,
                    aggregate: None,
                    destination_cardinality,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

fn weighted_zset_store_tool(state: Arc<ToolState>, operation: ZsetStoreOperation) -> Tool {
    let (tool_name, command_name, title) = match operation {
        ZsetStoreOperation::Intersection => (
            "redis_zinterstore",
            "ZINTERSTORE",
            "Store Redis Sorted Set Intersection",
        ),
        ZsetStoreOperation::Union => (
            "redis_zunionstore",
            "ZUNIONSTORE",
            "Store Redis Sorted Set Union",
        ),
        ZsetStoreOperation::Difference => unreachable!("ZDIFFSTORE has no weights"),
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(format!(
            "Permanently overwrite a destination with the {} of 1 to 1000 binary-safe sorted sets. Sources support finite score weights and sum/min/max aggregation, plus count on Redis 8.8+. Returns only destination cardinality; every key must share a Redis Cluster slot. Requires full access.",
            match operation {
                ZsetStoreOperation::Intersection => "intersection",
                ZsetStoreOperation::Union => "union",
                ZsetStoreOperation::Difference => unreachable!(),
            }
        ))
        .output_schema(output_schema::<ZsetStoreOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<WeightedZsetStoreInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                validate_items(&input.sources, "sources")?;
                if matches!(input.aggregate, ZsetAggregate::Count)
                    && state
                        .redis_version()
                        .is_some_and(|version| version < RedisVersion::new(8, 8, 0))
                {
                    return Err(tower_mcp::Error::tool(
                        "sorted-set COUNT aggregation requires Redis 8.8 or newer",
                    ));
                }
                let source_count = input.sources.len();
                let sources = input
                    .sources
                    .into_iter()
                    .enumerate()
                    .map(|(index, source)| source.decode(index))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let weighted = sources.iter().any(|source| source.weight.is_some());
                let mut command = command(tool_name, AccessMode::Full, command_name);
                command
                    .arg(decode_input(
                        &input.destination,
                        input.destination_encoding,
                        "destination",
                    )?)
                    .arg(source_count.to_string());
                for source in &sources {
                    command.arg(source.key.clone());
                }
                if weighted {
                    command.arg("WEIGHTS");
                    for source in &sources {
                        command.arg(source.weight.as_deref().unwrap_or("1"));
                    }
                }
                command.arg("AGGREGATE").arg(input.aggregate.redis_token());
                let destination_cardinality = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                state.output(&ZsetStoreOutput {
                    operation,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    source_count,
                    weighted,
                    aggregate: Some(input.aggregate),
                    destination_cardinality,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Explicit rank, score, or lexicographic range. Defaults to ranks 0 through 99.
    #[serde(default)]
    range: ZrangeSpec,
    /// Include exact Redis score strings in the response.
    #[serde(default)]
    withscores: bool,
    /// Return highest scores or lexicographically greatest members first.
    #[serde(default)]
    rev: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeEntry {
    member: String,
    encoding: ValueEncoding,
    score: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangeOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    range: ZrangeSpec,
    rev: bool,
    withscores: bool,
    count: usize,
    members: Vec<ZrangeEntry>,
    page: PageMetadata,
}

enum ZrangePageKind {
    Rank { start: i64, stop: i64 },
    Offset { offset: u64 },
}

fn zrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zrange")
        .title("Read Redis Sorted Set Range")
        .description(
            "Read one bounded rank, score, or binary lexicographic range from a Redis sorted set. Inclusive, exclusive, and infinite bounds are explicit. `rev` changes output order without reversing natural min/max inputs. Follow page.continuation.start for rank pages or page.continuation.offset for score/lex pages. Scores are returned as canonical strings instead of JSON numbers.",
        )
        .output_schema(output_schema::<ZrangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZrangeInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_zrange", AccessMode::ReadOnly, "ZRANGE");
                command.arg(key.clone());
                let (requested, page_kind) = match &input.range {
                    ZrangeSpec::Rank { start, stop } => {
                        let requested =
                            validate_range(*start, *stop, state.max_collection_entries())?;
                        let fetch_stop = if *start >= 0 && requested > 0 {
                            stop.checked_add(1).unwrap_or(*stop)
                        } else {
                            *stop
                        };
                        command.arg(start.to_string()).arg(fetch_stop.to_string());
                        (requested, ZrangePageKind::Rank { start: *start, stop: *stop })
                    }
                    ZrangeSpec::Score {
                        min,
                        max,
                        offset,
                        limit,
                    } => {
                        state.validate_requested_entries(*limit, "range.limit")?;
                        let min = min.redis_token("range.min.value")?;
                        let max = max.redis_token("range.max.value")?;
                        if input.rev {
                            command.arg(max).arg(min);
                        } else {
                            command.arg(min).arg(max);
                        }
                        command
                            .arg("BYSCORE")
                            .arg("LIMIT")
                            .arg(offset.to_string())
                            .arg(limit.saturating_add(1).to_string());
                        (*limit, ZrangePageKind::Offset { offset: *offset })
                    }
                    ZrangeSpec::Lex {
                        min,
                        max,
                        offset,
                        limit,
                    } => {
                        state.validate_requested_entries(*limit, "range.limit")?;
                        let min = min.redis_token("range.min.value")?;
                        let max = max.redis_token("range.max.value")?;
                        if input.rev {
                            command.arg(max).arg(min);
                        } else {
                            command.arg(min).arg(max);
                        }
                        command
                            .arg("BYLEX")
                            .arg("LIMIT")
                            .arg(offset.to_string())
                            .arg(limit.saturating_add(1).to_string());
                        (*limit, ZrangePageKind::Offset { offset: *offset })
                    }
                };
                if input.rev {
                    command.arg("REV");
                }
                let mut members = if input.withscores {
                    command.arg("WITHSCORES");
                    let value = state.raw(command, "ZRANGE failed").await?;
                    let values = decode_score_pairs(value, "ZRANGE")?;
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
                let has_more = members.len() > requested;
                members.truncate(requested);
                let exists = if members.is_empty() {
                    key_exists(&state, "redis_zrange", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                let page = match page_kind {
                    ZrangePageKind::Rank { start, stop } => {
                        let next_start =
                            (has_more && start >= 0).then(|| stop.saturating_add(1));
                        PageMetadata::range(requested, members.len(), next_start)
                    }
                    ZrangePageKind::Offset { offset } => {
                        let next_offset =
                            has_more.then(|| offset.saturating_add(members.len() as u64));
                        PageMetadata::offset(requested, members.len(), next_offset)
                    }
                };
                let output = ZrangeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    range: input.range,
                    rev: input.rev,
                    withscores: input.withscores,
                    count: members.len(),
                    page,
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry ZRANGE with a smaller rank span or range.limit.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangestoreInput {
    /// Destination sorted-set key that Redis overwrites.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    /// Source sorted-set key.
    source: String,
    /// Encoding of `source`.
    #[serde(default)]
    source_encoding: InputEncoding,
    /// Explicit bounded rank, score, or lexicographic range. Defaults to ranks 0 through 99.
    #[serde(default)]
    range: ZrangeSpec,
    /// Store highest scores or lexicographically greatest members first.
    #[serde(default)]
    rev: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrangestoreOutput {
    destination: String,
    destination_encoding: InputEncoding,
    source: String,
    source_encoding: InputEncoding,
    range: ZrangeSpec,
    rev: bool,
    requested_maximum: usize,
    destination_cardinality: u64,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn zrangestore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zrangestore")
        .title("Store Redis Sorted Set Range")
        .description(
            "Permanently overwrite a destination with an explicitly bounded rank, score, or binary lexicographic range from one sorted set. Source and destination must share a Redis Cluster slot. Requires Redis 6.2+ and full access.",
        )
        .output_schema(output_schema::<ZrangestoreOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<ZrangestoreInput>| async move {
                state.require(AccessMode::Full, "redis_zrangestore")?;
                let mut command = command("redis_zrangestore", AccessMode::Full, "ZRANGESTORE");
                command
                    .arg(decode_input(
                        &input.destination,
                        input.destination_encoding,
                        "destination",
                    )?)
                    .arg(decode_input(
                        &input.source,
                        input.source_encoding,
                        "source",
                    )?);
                let requested_maximum = match &input.range {
                    ZrangeSpec::Rank { start, stop } => {
                        let requested =
                            validate_range(*start, *stop, state.max_collection_entries())?;
                        command.arg(start.to_string()).arg(stop.to_string());
                        requested
                    }
                    ZrangeSpec::Score {
                        min,
                        max,
                        offset,
                        limit,
                    } => {
                        state.validate_requested_entries(*limit, "range.limit")?;
                        let min = min.redis_token("range.min.value")?;
                        let max = max.redis_token("range.max.value")?;
                        if input.rev {
                            command.arg(max).arg(min);
                        } else {
                            command.arg(min).arg(max);
                        }
                        command
                            .arg("BYSCORE")
                            .arg("LIMIT")
                            .arg(offset.to_string())
                            .arg(limit.to_string());
                        *limit
                    }
                    ZrangeSpec::Lex {
                        min,
                        max,
                        offset,
                        limit,
                    } => {
                        state.validate_requested_entries(*limit, "range.limit")?;
                        let min = min.redis_token("range.min.value")?;
                        let max = max.redis_token("range.max.value")?;
                        if input.rev {
                            command.arg(max).arg(min);
                        } else {
                            command.arg(min).arg(max);
                        }
                        command
                            .arg("BYLEX")
                            .arg("LIMIT")
                            .arg(offset.to_string())
                            .arg(limit.to_string());
                        *limit
                    }
                };
                if input.rev {
                    command.arg("REV");
                }
                let destination_cardinality =
                    state.query(command, "ZRANGESTORE failed").await?;
                state.output(&ZrangestoreOutput {
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    source: input.source,
                    source_encoding: input.source_encoding,
                    range: input.range,
                    rev: input.rev,
                    requested_maximum,
                    destination_cardinality,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZcardOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    cardinality: u64,
}

fn zcard_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zcard")
        .title("Read Redis Sorted Set Cardinality")
        .description(
            "Return the number of members in a binary-safe Redis sorted-set key. Missing sorted sets have cardinality zero and exists=false.",
        )
        .output_schema(output_schema::<ZcardOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetKeyInput>| async move {
                let mut command = command("redis_zcard", AccessMode::ReadOnly, "ZCARD");
                command.arg(input.decoded_key()?);
                let cardinality = state.query(command, "ZCARD failed").await?;
                state.output(&ZcardOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: cardinality != 0,
                    cardinality,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZcountInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Lower score bound.
    min: ZscoreBound,
    /// Upper score bound.
    max: ZscoreBound,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZcountOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    min: ZscoreBound,
    max: ZscoreBound,
    count: u64,
}

fn zcount_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zcount")
        .title("Count Redis Sorted Set Score Range")
        .description(
            "Count members within explicit inclusive, exclusive, or infinite score bounds. Missing sorted sets return exists=false and count zero.",
        )
        .output_schema(output_schema::<ZcountOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZcountInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_zcount", AccessMode::ReadOnly, "ZCOUNT");
                command
                    .arg(key.clone())
                    .arg(input.min.redis_token("min.value")?)
                    .arg(input.max.redis_token("max.value")?);
                let count = state.query(command, "ZCOUNT failed").await?;
                let exists = if count == 0 {
                    key_exists(&state, "redis_zcount", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                state.output(&ZcountOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    min: input.min,
                    max: input.max,
                    count,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZscoreOutput {
    key: String,
    key_encoding: InputEncoding,
    member: String,
    member_encoding: InputEncoding,
    zset_exists: bool,
    member_exists: bool,
    score: Option<String>,
}

fn zscore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zscore")
        .title("Read Redis Sorted Set Score")
        .description(
            "Read one binary-safe member score as a canonical string instead of a JSON number and distinguish a missing sorted set from a missing member.",
        )
        .output_schema(output_schema::<ZscoreOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMemberInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_zscore", AccessMode::ReadOnly, "ZSCORE");
                command.arg(key.clone()).arg(decode_input(
                    &input.member,
                    input.member_encoding,
                    "member",
                )?);
                let score = decode_score(state.raw(command, "ZSCORE failed").await?, "ZSCORE")?;
                let member_exists = score.is_some();
                let zset_exists = if member_exists {
                    true
                } else {
                    key_exists(&state, "redis_zscore", AccessMode::ReadOnly, key).await?
                };
                state.output(&ZscoreOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    member: input.member,
                    member_encoding: input.member_encoding,
                    zset_exists,
                    member_exists,
                    score,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZmscoreEntry {
    member: String,
    member_encoding: InputEncoding,
    member_exists: bool,
    score: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZmscoreOutput {
    key: String,
    key_encoding: InputEncoding,
    zset_exists: bool,
    count: usize,
    members: Vec<ZmscoreEntry>,
}

fn zmscore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zmscore")
        .title("Read Multiple Redis Sorted Set Scores")
        .description(
            "Read 1 to 1000 binary-safe member scores as canonical strings instead of JSON numbers. Results remain aligned with request order and distinguish missing members from a missing sorted set. Requires Redis 6.2 or newer.",
        )
        .output_schema(output_schema::<ZmscoreOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMembersInput>| async move {
                state.validate_requested_entries(input.members.len(), "members")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let members = decode_set_members(input.members)?;
                let mut command = command("redis_zmscore", AccessMode::ReadOnly, "ZMSCORE");
                command.arg(key.clone());
                for member in &members {
                    command.arg(member.bytes.clone());
                }
                let scores = redis_array(state.raw(command, "ZMSCORE failed").await?, "ZMSCORE")?
                    .into_iter()
                    .map(|score| decode_score(score, "ZMSCORE"))
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                if scores.len() != members.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "ZMSCORE returned {} results for {} members",
                        scores.len(),
                        members.len()
                    )));
                }
                let zset_exists = if scores.iter().any(Option::is_some) {
                    true
                } else {
                    key_exists(&state, "redis_zmscore", AccessMode::ReadOnly, key).await?
                };
                let members = members
                    .into_iter()
                    .zip(scores)
                    .map(|(member, score)| -> tower_mcp::Result<_> {
                        let member_exists = score.is_some();
                        Ok(ZmscoreEntry {
                            member: member.member,
                            member_encoding: member.member_encoding,
                            member_exists,
                            score,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = ZmscoreOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    zset_exists,
                    count: members.len(),
                    members,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry ZMSCORE with fewer members.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZrankOutput {
    key: String,
    key_encoding: InputEncoding,
    member: String,
    member_encoding: InputEncoding,
    zset_exists: bool,
    member_exists: bool,
    reverse: bool,
    rank: Option<u64>,
}

fn zrank_tool(state: Arc<ToolState>, reverse: bool) -> Tool {
    let (tool_name, command_name, title) = if reverse {
        (
            "redis_zrevrank",
            "ZREVRANK",
            "Read Redis Sorted Set Reverse Rank",
        )
    } else {
        ("redis_zrank", "ZRANK", "Read Redis Sorted Set Rank")
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(if reverse {
            "Read a binary-safe member's zero-based rank from highest to lowest score and distinguish a missing sorted set from a missing member."
        } else {
            "Read a binary-safe member's zero-based rank from lowest to highest score and distinguish a missing sorted set from a missing member."
        })
        .output_schema(output_schema::<ZrankOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<SetMemberInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command(tool_name, AccessMode::ReadOnly, command_name);
                command.arg(key.clone()).arg(decode_input(
                    &input.member,
                    input.member_encoding,
                    "member",
                )?);
                let rank: Option<u64> = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                let member_exists = rank.is_some();
                let zset_exists = if member_exists {
                    true
                } else {
                    key_exists(&state, tool_name, AccessMode::ReadOnly, key).await?
                };
                state.output(&ZrankOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    member: input.member,
                    member_encoding: input.member_encoding,
                    zset_exists,
                    member_exists,
                    reverse,
                    rank,
                })
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
    /// Positive expiration value interpreted according to `mode`.
    #[serde(default)]
    #[schemars(range(min = 1))]
    expiration: Option<i64>,
    /// Compatibility shorthand for a relative expiration in seconds.
    #[serde(default)]
    #[schemars(range(min = 1))]
    seconds: Option<i64>,
    /// Relative/absolute and second/millisecond interpretation.
    #[serde(default)]
    mode: HashExpirationMode,
    /// Optional NX, XX, GT, or LT condition.
    #[serde(default)]
    condition: Option<HashExpireCondition>,
    /// One to 1000 unique fields. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldSelector>,
}

impl HexpireInput {
    fn expiration(&self) -> tower_mcp::Result<i64> {
        match (self.expiration, self.seconds) {
            (Some(value), None) => Ok(value),
            (None, Some(value)) if matches!(self.mode, HashExpirationMode::RelativeSeconds) => {
                Ok(value)
            }
            (None, Some(_)) => Err(tower_mcp::Error::tool(
                "seconds shorthand can only be used with relative_seconds mode",
            )),
            (Some(_), Some(_)) => Err(tower_mcp::Error::tool(
                "provide expiration or the seconds compatibility shorthand, not both",
            )),
            (None, None) => Err(tower_mcp::Error::tool(
                "provide expiration or the seconds compatibility shorthand",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashExpirationMode {
    #[default]
    RelativeSeconds,
    RelativeMilliseconds,
    UnixSeconds,
    UnixMilliseconds,
}

impl HashExpirationMode {
    fn command(self) -> &'static str {
        match self {
            Self::RelativeSeconds => "HEXPIRE",
            Self::RelativeMilliseconds => "HPEXPIRE",
            Self::UnixSeconds => "HEXPIREAT",
            Self::UnixMilliseconds => "HPEXPIREAT",
        }
    }

    fn validate(self, value: i64) -> tower_mcp::Result<()> {
        if value <= 0 {
            return Err(tower_mcp::Error::tool(
                "expiration must be greater than zero",
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| tower_mcp::Error::tool("system clock is before the Unix epoch"))?;
        let future = match self {
            Self::RelativeSeconds | Self::RelativeMilliseconds => true,
            Self::UnixSeconds => u64::try_from(value).is_ok_and(|value| value > now.as_secs()),
            Self::UnixMilliseconds => {
                u64::try_from(value).is_ok_and(|value| u128::from(value) > now.as_millis())
            }
        };
        if future {
            Ok(())
        } else {
            Err(tower_mcp::Error::tool(
                "absolute expiration must be in the future; use redis_hdel for immediate deletion",
            ))
        }
    }

    fn immediate_deletion_value(self) -> i64 {
        0
    }
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
    expiration: i64,
    mode: HashExpirationMode,
    /// Compatibility field populated only for relative-seconds mode.
    seconds: Option<i64>,
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
            "Set positive relative or future absolute hash-field expirations in seconds or milliseconds on Redis 7.4 or newer. Past absolute timestamps are rejected; use redis_hdel for immediate deletion.",
        )
        .output_schema(output_schema::<HexpireOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<HexpireInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_hexpire")?;
                let expiration = input.expiration()?;
                input.mode.validate(expiration)?;
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let fields = decode_hash_fields(input.fields, true)?;
                let command_name = input.mode.command();
                let mut command = command("redis_hexpire", AccessMode::ReadWrite, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(expiration.to_string());
                if let Some(condition) = input.condition {
                    command.arg(condition.redis_token());
                }
                command.arg("FIELDS").arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let results: Vec<i64> = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                if results.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "{command_name} returned {} values for {} fields",
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
                                    "{command_name} returned unexpected field status {other}"
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
                    expiration,
                    mode: input.mode,
                    seconds: matches!(input.mode, HashExpirationMode::RelativeSeconds)
                        .then_some(expiration),
                    condition: input.condition,
                    count: fields.len(),
                    expirations_set,
                    condition_not_met,
                    fields_missing,
                    fields,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry the hash expiration update with fewer fields.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HexpireDeleteInput {
    /// Redis hash key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Expiration command family to use for the immediate deletion.
    #[serde(default)]
    mode: HashExpirationMode,
    /// Optional NX, XX, GT, or LT condition applied by Redis before deletion.
    #[serde(default)]
    condition: Option<HashExpireCondition>,
    /// One to 1000 unique fields. Strings are UTF-8 shorthand; objects can select base64.
    #[schemars(length(min = 1, max = 1000))]
    fields: Vec<HashFieldSelector>,
}

#[derive(Debug, Clone, Copy, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum HashExpireDeleteStatus {
    FieldMissing,
    ConditionNotMet,
    Deleted,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HashExpireDeleteEntry {
    field: String,
    field_encoding: InputEncoding,
    status: HashExpireDeleteStatus,
    redis_code: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct HexpireDeleteOutput {
    key: String,
    key_encoding: InputEncoding,
    mode: HashExpirationMode,
    condition: Option<HashExpireCondition>,
    count: usize,
    deleted: usize,
    condition_not_met: usize,
    fields_missing: usize,
    fields: Vec<HashExpireDeleteEntry>,
}

fn hexpire_delete_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_hexpire_delete")
        .title("Delete Redis Hash Fields by Expiration")
        .description(
            "Immediately and conditionally delete hash fields through a selected Redis 7.4+ HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT form. This exposes the commands' destructive past-expiration semantics separately from ordinary expiration updates and requires full access.",
        )
        .output_schema(output_schema::<HexpireDeleteOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<HexpireDeleteInput>| async move {
                state.require(AccessMode::Full, "redis_hexpire_delete")?;
                state.validate_requested_entries(input.fields.len(), "fields")?;
                let fields = decode_hash_fields(input.fields, true)?;
                let command_name = input.mode.command();
                let mut command = command("redis_hexpire_delete", AccessMode::Full, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.mode.immediate_deletion_value().to_string());
                if let Some(condition) = input.condition {
                    command.arg(condition.redis_token());
                }
                command.arg("FIELDS").arg(fields.len().to_string());
                for field in &fields {
                    command.arg(field.bytes.clone());
                }
                let results: Vec<i64> = state
                    .query(command, &format!("{command_name} immediate deletion failed"))
                    .await?;
                if results.len() != fields.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "{command_name} returned {} values for {} fields",
                        results.len(),
                        fields.len()
                    )));
                }
                let mut deleted = 0;
                let mut condition_not_met = 0;
                let mut fields_missing = 0;
                let fields = fields
                    .into_iter()
                    .zip(results)
                    .map(|(field, redis_code)| {
                        let status = match redis_code {
                            -2 => {
                                fields_missing += 1;
                                HashExpireDeleteStatus::FieldMissing
                            }
                            0 => {
                                condition_not_met += 1;
                                HashExpireDeleteStatus::ConditionNotMet
                            }
                            2 => {
                                deleted += 1;
                                HashExpireDeleteStatus::Deleted
                            }
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "{command_name} returned unexpected immediate-deletion status {other}"
                                )));
                            }
                        };
                        Ok(HashExpireDeleteEntry {
                            field: field.field,
                            field_encoding: field.field_encoding,
                            status,
                            redis_code,
                        })
                    })
                    .collect::<tower_mcp::Result<Vec<_>>>()?;
                let output = HexpireDeleteOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    mode: input.mode,
                    condition: input.condition,
                    count: fields.len(),
                    deleted,
                    condition_not_met,
                    fields_missing,
                    fields,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry the conditional hash-field deletion with fewer fields.",
                )
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

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedListElementInput {
    /// List element.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum ListElementSelector {
    /// UTF-8 list element shorthand.
    Utf8(String),
    /// Explicitly encoded list element.
    Encoded(EncodedListElementInput),
}

impl ListElementSelector {
    fn decode(self, index: usize) -> tower_mcp::Result<Vec<u8>> {
        match self {
            Self::Utf8(value) => Ok(value.into_bytes()),
            Self::Encoded(value) => decode_input(
                &value.value,
                value.value_encoding,
                &format!("elements[{index}].value"),
            ),
        }
    }
}

fn decode_list_elements(elements: Vec<ListElementSelector>) -> tower_mcp::Result<Vec<Vec<u8>>> {
    validate_items(&elements, "elements")?;
    elements
        .into_iter()
        .enumerate()
        .map(|(index, element)| element.decode(index))
        .collect()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListKeyInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListPushInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// UTF-8 shorthand or explicitly encoded elements to push in argument order.
    #[schemars(length(min = 1, max = 1000))]
    elements: Vec<ListElementSelector>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListPushOutput {
    key: String,
    key_encoding: InputEncoding,
    pushed: usize,
    length: u64,
}

fn list_push_tool(state: Arc<ToolState>, left: bool) -> Tool {
    let (tool_name, command_name, title, description) = if left {
        (
            "redis_lpush",
            "LPUSH",
            "Push Redis List Head",
            "Push between 1 and 1000 binary-safe elements to a Redis list head. Redis processes elements in argument order, so the last argument becomes the new head.",
        )
    } else {
        (
            "redis_rpush",
            "RPUSH",
            "Push Redis List Tail",
            "Push between 1 and 1000 binary-safe elements to a Redis list tail. The first argument is closest to the prior tail and the last argument becomes the new tail.",
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<ListPushOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<ListPushInput>| async move {
                state.require(AccessMode::ReadWrite, tool_name)?;
                let elements = decode_list_elements(input.elements)?;
                let pushed = elements.len();
                let mut command = command(tool_name, AccessMode::ReadWrite, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .args(elements);
                let length = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                state.output(&ListPushOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    pushed,
                    length,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LlenOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    length: u64,
}

fn llen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_llen")
        .title("Read Redis List Length")
        .description(
            "Read a Redis list length. Because Redis removes empty lists, length zero means the key does not exist.",
        )
        .output_schema(output_schema::<LlenOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ListKeyInput>| async move {
                let mut command = command("redis_llen", AccessMode::ReadOnly, "LLEN");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let length = state.query(command, "LLEN failed").await?;
                state.output(&LlenOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: length != 0,
                    length,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LindexInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Zero-based index. Negative indexes count from the tail, with -1 selecting the last element.
    index: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LindexOutput {
    key: String,
    key_encoding: InputEncoding,
    list_exists: bool,
    element_exists: bool,
    index: i64,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

fn lindex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lindex")
        .title("Read Redis List Element")
        .description(
            "Read one binary-safe list element by zero-based index. Negative indexes count from the tail. Missing lists and out-of-range indexes are distinguished in the result.",
        )
        .output_schema(output_schema::<LindexOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LindexInput>| async move {
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_lindex", AccessMode::ReadOnly, "LINDEX");
                command.arg(key.clone()).arg(input.index.to_string());
                let value: Option<Vec<u8>> = state.query(command, "LINDEX failed").await?;
                let element_exists = value.is_some();
                let list_exists = if element_exists {
                    true
                } else {
                    key_exists(&state, "redis_lindex", AccessMode::ReadOnly, key).await?
                };
                let (value, encoding) = match value {
                    Some(value) => {
                        let value = EncodedValue::from(value);
                        (Some(value.value), Some(value.encoding))
                    }
                    None => (None, None),
                };
                state.output(&LindexOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    list_exists,
                    element_exists,
                    index: input.index,
                    value,
                    encoding,
                })
            },
        )
        .build()
}

fn default_one() -> usize {
    1
}

fn default_list_returned_bytes() -> usize {
    DEFAULT_RETURNED_COLLECTION_BYTES
}

fn default_rank() -> i64 {
    1
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LposInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// List element to find.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
    /// Match rank. Positive ranks search from the head and negative ranks from the tail; zero is invalid.
    #[serde(default = "default_rank")]
    rank: i64,
    /// Maximum number of matching positions to return.
    #[serde(default = "default_one")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Maximum number of list elements to scan. Zero or omission means no scan limit.
    max_len: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LposOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    rank: i64,
    requested: usize,
    count: usize,
    positions: Vec<i64>,
}

fn lpos_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lpos")
        .title("Find Redis List Elements")
        .description(
            "Find a bounded number of matching list-element positions. Positive ranks search from the head, negative ranks from the tail, and zero is invalid. COUNT is always sent so the response is an explicit array; missing lists return exists=false and no positions.",
        )
        .output_schema(output_schema::<LposOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LposInput>| async move {
                state.validate_requested_entries(input.count, "count")?;
                if input.rank == 0 {
                    return Err(tower_mcp::Error::tool("rank must not be zero"));
                }
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_lpos", AccessMode::ReadOnly, "LPOS");
                command
                    .arg(key.clone())
                    .arg(decode_input(
                        &input.value,
                        input.value_encoding,
                        "value",
                    )?)
                    .arg("RANK")
                    .arg(input.rank.to_string())
                    .arg("COUNT")
                    .arg(input.count.to_string());
                if let Some(max_len) = input.max_len {
                    command.arg("MAXLEN").arg(max_len.to_string());
                }
                let positions: Vec<i64> = state.query(command, "LPOS failed").await?;
                let exists = if positions.is_empty() {
                    key_exists(&state, "redis_lpos", AccessMode::ReadOnly, key).await?
                } else {
                    true
                };
                let output = LposOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    rank: input.rank,
                    requested: input.count,
                    count: positions.len(),
                    positions,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Retry LPOS with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListPopInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Maximum number of elements to remove and return.
    #[serde(default = "default_one")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Maximum aggregate bytes of popped elements to include. Larger results are reported as omitted so the mutation remains observable.
    #[serde(default = "default_list_returned_bytes")]
    #[schemars(range(min = 1))]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListPopOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    popped: usize,
    found: bool,
    element_bytes: usize,
    elements_omitted: bool,
    elements: Vec<EncodedValue>,
}

fn list_pop_tool(state: Arc<ToolState>, left: bool) -> Tool {
    let (tool_name, command_name, title, edge) = if left {
        ("redis_lpop", "LPOP", "Pop Redis List Head", "head")
    } else {
        ("redis_rpop", "RPOP", "Pop Redis List Tail", "tail")
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(format!(
            "Remove and return between 1 and 1000 binary-safe elements from the list {edge}. This is non-blocking; a missing or exhausted list returns found=false and no elements. Oversized returned values are explicitly omitted after the mutation. Requires full access and Redis 6.2 or newer."
        ))
        .output_schema(output_schema::<ListPopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<ListPopInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                state.validate_requested_entries(input.count, "count")?;
                if input.max_returned_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_returned_bytes must be greater than zero",
                    ));
                }
                let max_returned_bytes =
                    input.max_returned_bytes.min(state.max_output_bytes());
                let mut command = command(tool_name, AccessMode::Full, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.count.to_string());
                let elements: Option<Vec<Vec<u8>>> = state
                    .query(command, &format!("{command_name} failed"))
                    .await?;
                let elements = elements.unwrap_or_default();
                let popped = elements.len();
                let element_bytes = elements.iter().try_fold(0_usize, |total, element| {
                    total.checked_add(element.len()).ok_or_else(|| {
                        tower_mcp::Error::tool("popped element byte count overflowed")
                    })
                })?;
                let elements_omitted = element_bytes > max_returned_bytes;
                let elements = if elements_omitted {
                    Vec::new()
                } else {
                    elements
                        .into_iter()
                        .map(EncodedValue::from)
                        .collect::<Vec<_>>()
                };
                let output = ListPopOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested: input.count,
                    popped,
                    found: popped != 0,
                    element_bytes,
                    elements_omitted,
                    elements,
                };
                state.output_collection(
                    &output,
                    output.elements.len(),
                    "Retry the pop with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LremInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Occurrences to remove: positive from the head, negative from the tail, or zero for all matches.
    count: i64,
    /// List element to remove.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LremOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: i64,
    removed: u64,
}

fn lrem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lrem")
        .title("Remove Redis List Elements")
        .description(
            "Remove matching binary-safe list elements. Positive counts remove from the head, negative counts from the tail, and zero removes every match. Requires full access.",
        )
        .output_schema(output_schema::<LremOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LremInput>| async move {
                state.require(AccessMode::Full, "redis_lrem")?;
                let mut command = command("redis_lrem", AccessMode::Full, "LREM");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.count.to_string())
                    .arg(decode_input(
                        &input.value,
                        input.value_encoding,
                        "value",
                    )?);
                let removed = state.query(command, "LREM failed").await?;
                state.output(&LremOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested: input.count,
                    removed,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LsetInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Zero-based index to replace. Negative indexes count from the tail.
    index: i64,
    /// Replacement value.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LsetOutput {
    key: String,
    key_encoding: InputEncoding,
    index: i64,
    replaced: bool,
}

fn lset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lset")
        .title("Replace Redis List Element")
        .description(
            "Replace one list element by index. Negative indexes count from the tail. Missing lists and out-of-range indexes are Redis errors. Requires full access.",
        )
        .output_schema(output_schema::<LsetOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LsetInput>| async move {
                state.require(AccessMode::Full, "redis_lset")?;
                let mut command = command("redis_lset", AccessMode::Full, "LSET");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.index.to_string())
                    .arg(decode_input(
                        &input.value,
                        input.value_encoding,
                        "value",
                    )?);
                let _: String = state.query(command, "LSET failed").await?;
                state.output(&LsetOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    index: input.index,
                    replaced: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LtrimInput {
    /// Redis list key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Inclusive start index. Negative indexes count from the tail.
    start: i64,
    /// Inclusive stop index. Negative indexes count from the tail.
    stop: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LtrimOutput {
    key: String,
    key_encoding: InputEncoding,
    start: i64,
    stop: i64,
    exists: bool,
    trimmed: bool,
}

fn ltrim_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ltrim")
        .title("Trim Redis List")
        .description(
            "Keep only the inclusive list range from start through stop. Negative indexes count from the tail. If the range is empty, Redis deletes the key. Requires full access.",
        )
        .output_schema(output_schema::<LtrimOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LtrimInput>| async move {
                state.require(AccessMode::Full, "redis_ltrim")?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_ltrim", AccessMode::Full, "LTRIM");
                command
                    .arg(key.clone())
                    .arg(input.start.to_string())
                    .arg(input.stop.to_string());
                let _: String = state.query(command, "LTRIM failed").await?;
                let exists = key_exists(&state, "redis_ltrim", AccessMode::Full, key).await?;
                state.output(&LtrimOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    start: input.start,
                    stop: input.stop,
                    exists,
                    trimmed: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum ListSide {
    Left,
    Right,
}

impl ListSide {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LmoveInput {
    /// Source Redis list key.
    source: String,
    /// Encoding of `source`.
    #[serde(default)]
    source_encoding: InputEncoding,
    /// Destination Redis list key.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    /// Source edge from which to remove the element.
    from: ListSide,
    /// Destination edge at which to insert the element.
    to: ListSide,
    /// Maximum moved-value bytes to include. Larger values are reported as omitted so the move remains observable.
    #[serde(default = "default_list_returned_bytes")]
    #[schemars(range(min = 1))]
    max_value_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct LmoveOutput {
    source: String,
    source_encoding: InputEncoding,
    destination: String,
    destination_encoding: InputEncoding,
    from: ListSide,
    to: ListSide,
    moved: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
    value_bytes: Option<usize>,
    value_omitted: bool,
}

fn lmove_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_lmove")
        .title("Move Redis List Element")
        .description(
            "Atomically move one binary-safe element between list edges without blocking. A missing or exhausted source returns moved=false. Oversized returned values are explicitly omitted after the move. On Redis Cluster, source and destination must share a hash slot. Requires full access and Redis 6.2 or newer.",
        )
        .output_schema(output_schema::<LmoveOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<LmoveInput>| async move {
                state.require(AccessMode::Full, "redis_lmove")?;
                if input.max_value_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_value_bytes must be greater than zero",
                    ));
                }
                let max_value_bytes = input.max_value_bytes.min(state.max_output_bytes());
                let mut command = command("redis_lmove", AccessMode::Full, "LMOVE");
                command
                    .arg(decode_input(
                        &input.source,
                        input.source_encoding,
                        "source",
                    )?)
                    .arg(decode_input(
                        &input.destination,
                        input.destination_encoding,
                        "destination",
                    )?)
                    .arg(input.from.as_str())
                    .arg(input.to.as_str());
                let value: Option<Vec<u8>> = state.query(command, "LMOVE failed").await?;
                let moved = value.is_some();
                let value_bytes = value.as_ref().map(Vec::len);
                let value_omitted = value
                    .as_ref()
                    .is_some_and(|value| value.len() > max_value_bytes);
                let (value, encoding) = match value {
                    Some(value) if !value_omitted => {
                        let value = EncodedValue::from(value);
                        (Some(value.value), Some(value.encoding))
                    }
                    _ => (None, None),
                };
                state.output(&LmoveOutput {
                    source: input.source,
                    source_encoding: input.source_encoding,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    from: input.from,
                    to: input.to,
                    moved,
                    value,
                    encoding,
                    value_bytes,
                    value_omitted,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SaddOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    added: u64,
}

fn sadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sadd")
        .title("Add Redis Set Members")
        .description("Add between 1 and 1000 binary-safe members to a Redis set.")
        .output_schema(output_schema::<SaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMembersInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_sadd")?;
                let members = decode_set_members(input.members)?;
                let requested = members.len();
                let mut command = command("redis_sadd", AccessMode::ReadWrite, "SADD");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for member in members {
                    command.arg(member.bytes);
                }
                let added = state.query(command, "SADD failed").await?;
                state.output(&SaddOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    added,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SremOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    removed: u64,
}

fn srem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_srem")
        .title("Remove Redis Set Members")
        .description(
            "Permanently remove 1 to 1000 binary-safe members from a Redis set. Requires full access.",
        )
        .output_schema(output_schema::<SremOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMembersInput>| async move {
                state.require(AccessMode::Full, "redis_srem")?;
                let members = decode_set_members(input.members)?;
                let requested = members.len();
                let mut command = command("redis_srem", AccessMode::Full, "SREM");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for member in members {
                    command.arg(member.bytes);
                }
                let removed = state.query(command, "SREM failed").await?;
                state.output(&SremOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    removed,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ScoreMember {
    /// Finite score. Strings preserve every decimal digit sent to Redis.
    score: RedisDecimalInput,
    /// Binary-safe member. Strings are UTF-8 shorthand; objects can select base64.
    member: SetMemberSelector,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZaddInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
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
        for (index, member) in self.members.iter().enumerate() {
            member
                .score
                .finite_token(&format!("members[{index}].score"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZaddOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    affected: u64,
    reports_changed: bool,
}

fn zadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zadd")
        .title("Add Redis Sorted Set Members")
        .description(
            "Add or update 1 to 1000 binary-safe sorted-set members. Scores may be JSON numbers or exact finite decimal strings.",
        )
        .output_schema(output_schema::<ZaddOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZaddInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_zadd")?;
                input.validate()?;
                let requested = input.members.len();
                let mut command = command("redis_zadd", AccessMode::ReadWrite, "ZADD");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
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
                for (index, member) in input.members.into_iter().enumerate() {
                    command
                        .arg(
                            member
                                .score
                                .finite_token(&format!("members[{index}].score"))?,
                        )
                        .arg(member.member.decode(index)?.bytes);
                }
                let affected = state.query(command, "ZADD failed").await?;
                state.output(&ZaddOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    affected,
                    reports_changed: input.ch,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZincrbyInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Binary-safe member.
    member: String,
    /// Encoding of `member`.
    #[serde(default)]
    member_encoding: InputEncoding,
    /// Finite increment. Strings preserve every decimal digit sent to Redis.
    increment: RedisDecimalInput,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZincrbyOutput {
    key: String,
    key_encoding: InputEncoding,
    member: String,
    member_encoding: InputEncoding,
    score: String,
}

fn zincrby_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zincrby")
        .title("Increment Redis Sorted Set Score")
        .description(
            "Increment one binary-safe sorted-set member by an exact finite decimal and return Redis's canonical score string.",
        )
        .output_schema(output_schema::<ZincrbyOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ZincrbyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_zincrby")?;
                let mut command = command("redis_zincrby", AccessMode::ReadWrite, "ZINCRBY");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.increment.finite_token("increment")?)
                    .arg(decode_input(
                        &input.member,
                        input.member_encoding,
                        "member",
                    )?);
                let score = decode_score(
                    state.raw(command, "ZINCRBY failed").await?,
                    "ZINCRBY",
                )?
                .ok_or_else(|| tower_mcp::Error::tool("ZINCRBY returned a nil score"))?;
                state.output(&ZincrbyOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    member: input.member,
                    member_encoding: input.member_encoding,
                    score,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZremOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    removed: u64,
}

fn zrem_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zrem")
        .title("Remove Redis Sorted Set Members")
        .description(
            "Permanently remove 1 to 1000 binary-safe members from a Redis sorted set. Requires full access.",
        )
        .output_schema(output_schema::<ZremOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetMembersInput>| async move {
                state.require(AccessMode::Full, "redis_zrem")?;
                let members = decode_set_members(input.members)?;
                let requested = members.len();
                let mut command = command("redis_zrem", AccessMode::Full, "ZREM");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                for member in members {
                    command.arg(member.bytes);
                }
                let removed = state.query(command, "ZREM failed").await?;
                state.output(&ZremOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested,
                    removed,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy)]
enum ZpopDirection {
    Min,
    Max,
}

impl ZpopDirection {
    fn tool_name(self) -> &'static str {
        match self {
            Self::Min => "redis_zpopmin",
            Self::Max => "redis_zpopmax",
        }
    }

    fn command_name(self) -> &'static str {
        match self {
            Self::Min => "ZPOPMIN",
            Self::Max => "ZPOPMAX",
        }
    }
}

fn default_pop_count() -> usize {
    1
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZpopInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Maximum members to remove and return.
    #[serde(default = "default_pop_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Maximum aggregate member bytes to include. Larger results are reported as omitted so the mutation remains observable.
    #[serde(default = "default_list_returned_bytes")]
    #[schemars(range(min = 1))]
    max_returned_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZpopOutput {
    key: String,
    key_encoding: InputEncoding,
    requested: usize,
    count: usize,
    member_bytes: usize,
    members_omitted: bool,
    members: Vec<ZrangeEntry>,
}

fn zpop_tool(state: Arc<ToolState>, direction: ZpopDirection) -> Tool {
    let tool_name = direction.tool_name();
    let command_name = direction.command_name();
    ToolBuilder::new(tool_name)
        .title(match direction {
            ZpopDirection::Min => "Pop Lowest Redis Sorted Set Members",
            ZpopDirection::Max => "Pop Highest Redis Sorted Set Members",
        })
        .description(format!(
            "Permanently remove and return up to 1000 binary-safe members with the {} scores. Scores are returned as canonical strings instead of JSON numbers. Oversized returned members are explicitly omitted after the mutation. Requires full access.",
            match direction {
                ZpopDirection::Min => "lowest",
                ZpopDirection::Max => "highest",
            }
        ))
        .output_schema(output_schema::<ZpopOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<ZpopInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                state.validate_requested_entries(input.count, "count")?;
                if input.max_returned_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_returned_bytes must be greater than zero",
                    ));
                }
                let max_returned_bytes =
                    input.max_returned_bytes.min(state.max_output_bytes());
                let mut command = command(tool_name, AccessMode::Full, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.count.to_string());
                let value = state
                    .raw(command, &format!("{command_name} failed"))
                    .await?;
                let values = decode_score_pairs(value, command_name)?;
                let count = values.len();
                let member_bytes = values.iter().try_fold(0_usize, |total, (member, _)| {
                    total.checked_add(member.len()).ok_or_else(|| {
                        tower_mcp::Error::tool("popped sorted-set member byte count overflowed")
                    })
                })?;
                let members_omitted = member_bytes > max_returned_bytes;
                let members = if members_omitted {
                    Vec::new()
                } else {
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
                };
                let output = ZpopOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    requested: input.count,
                    count,
                    member_bytes,
                    members_omitted,
                    members,
                };
                state.output_collection(
                    &output,
                    output.members.len(),
                    "Retry the sorted-set pop with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZremrangebyscoreInput {
    /// Redis sorted-set key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Lower score bound.
    min: ZscoreBound,
    /// Upper score bound.
    max: ZscoreBound,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ZremrangebyscoreOutput {
    key: String,
    key_encoding: InputEncoding,
    min: ZscoreBound,
    max: ZscoreBound,
    removed: u64,
}

fn zremrangebyscore_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_zremrangebyscore")
        .title("Remove Redis Sorted Set Score Range")
        .description(
            "Permanently remove every member within explicit inclusive, exclusive, or infinite score bounds. Requires full access.",
        )
        .output_schema(output_schema::<ZremrangebyscoreOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<ZremrangebyscoreInput>| async move {
                state.require(AccessMode::Full, "redis_zremrangebyscore")?;
                let mut command = command(
                    "redis_zremrangebyscore",
                    AccessMode::Full,
                    "ZREMRANGEBYSCORE",
                );
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.min.redis_token("min.value")?)
                    .arg(input.max.redis_token("max.value")?);
                let removed = state
                    .query(command, "ZREMRANGEBYSCORE failed")
                    .await?;
                state.output(&ZremrangebyscoreOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    min: input.min,
                    max: input.max,
                    removed,
                })
            },
        )
        .build()
}

#[cfg(feature = "hashes")]
pub(super) fn add_hash_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hget_tool(state.clone()));
    router = router.tool(hgetall_tool(state.clone()));
    router = router.tool(hexists_tool(state.clone()));
    router = router.tool(hkeys_tool(state.clone()));
    router = router.tool(hlen_tool(state.clone()));
    router = router.tool(hmget_tool(state.clone()));
    router = router.tool(hrandfield_tool(state.clone()));
    router = router.tool(hscan_tool(state.clone()));
    router = router.tool(hstrlen_tool(state.clone()));
    router = router.tool(httl_tool(state.clone()));
    router = router.tool(hvals_tool(state.clone()));
    router
}

#[cfg(feature = "lists")]
pub(super) fn add_list_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(lindex_tool(state.clone()));
    router = router.tool(llen_tool(state.clone()));
    router = router.tool(lpos_tool(state.clone()));
    router = router.tool(lrange_tool(state.clone()));
    router
}

#[cfg(feature = "sets")]
pub(super) fn add_set_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(scard_tool(state.clone()));
    router = router.tool(sdiffcard_tool(state.clone()));
    router = router.tool(set_algebra_tool(
        state.clone(),
        SetAlgebraOperation::Difference,
    ));
    router = router.tool(set_algebra_tool(
        state.clone(),
        SetAlgebraOperation::Intersection,
    ));
    router = router.tool(sismember_tool(state.clone()));
    router = router.tool(smembers_tool(state.clone()));
    router = router.tool(smismember_tool(state.clone()));
    router = router.tool(sscan_tool(state.clone()));
    router = router.tool(sunioncard_tool(state.clone()));
    router = router.tool(set_algebra_tool(state.clone(), SetAlgebraOperation::Union));
    router
}

#[cfg(feature = "sorted-sets")]
pub(super) fn add_sorted_set_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(zcard_tool(state.clone()));
    router = router.tool(zcount_tool(state.clone()));
    router = router.tool(zintercard_tool(state.clone()));
    router = router.tool(zmscore_tool(state.clone()));
    router = router.tool(zrange_tool(state.clone()));
    router = router.tool(zrank_tool(state.clone(), false));
    router = router.tool(zrank_tool(state.clone(), true));
    router = router.tool(zscore_tool(state.clone()));
    router.tool(zscan_tool(state))
}

#[cfg(feature = "hashes")]
pub(super) fn add_hash_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(hset_tool(state.clone()));
    router = router.tool(hexpire_tool(state.clone()));
    router = router.tool(hincrby_tool(state.clone()));
    router = router.tool(hincrbyfloat_tool(state.clone()));
    router = router.tool(hpersist_tool(state.clone()));
    router
}

#[cfg(feature = "lists")]
pub(super) fn add_list_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(list_push_tool(state.clone(), true));
    router = router.tool(list_push_tool(state.clone(), false));
    router
}

#[cfg(feature = "sets")]
pub(super) fn add_set_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(sadd_tool(state))
}

#[cfg(feature = "sorted-sets")]
pub(super) fn add_sorted_set_write_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(zadd_tool(state.clone()));
    router.tool(zincrby_tool(state))
}

#[cfg(feature = "hashes")]
pub(super) fn add_hash_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router
        .tool(hexpire_delete_tool(state.clone()))
        .tool(hdel_tool(state))
}

#[cfg(feature = "lists")]
pub(super) fn add_list_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(list_pop_tool(state.clone(), true));
    router = router.tool(lmove_tool(state.clone()));
    router = router.tool(lrem_tool(state.clone()));
    router = router.tool(lset_tool(state.clone()));
    router = router.tool(ltrim_tool(state.clone()));
    router = router.tool(list_pop_tool(state.clone(), false));
    router
}

#[cfg(feature = "sets")]
pub(super) fn add_set_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router
        .tool(set_store_tool(state.clone(), SetStoreOperation::Difference))
        .tool(set_store_tool(
            state.clone(),
            SetStoreOperation::Intersection,
        ))
        .tool(srem_tool(state.clone()))
        .tool(set_store_tool(state, SetStoreOperation::Union))
}

#[cfg(feature = "sorted-sets")]
pub(super) fn add_sorted_set_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(zdiffstore_tool(state.clone()));
    router = router.tool(weighted_zset_store_tool(
        state.clone(),
        ZsetStoreOperation::Intersection,
    ));
    router = router.tool(zpop_tool(state.clone(), ZpopDirection::Max));
    router = router.tool(zpop_tool(state.clone(), ZpopDirection::Min));
    router = router.tool(zrangestore_tool(state.clone()));
    router = router.tool(zrem_tool(state.clone()));
    router = router.tool(zremrangebyscore_tool(state.clone()));
    router.tool(weighted_zset_store_tool(state, ZsetStoreOperation::Union))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zadd_input() -> ZaddInput {
        ZaddInput {
            key: "leaders".into(),
            key_encoding: InputEncoding::Utf8,
            members: vec![ScoreMember {
                score: RedisDecimalInput::Number(1.0),
                member: SetMemberSelector::Utf8("alice".into()),
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
