//! Curated key and string operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, command, decode_input, destructive_annotations,
    encode_bytes, optional_bytes, output_schema, read_annotations, write_annotations,
};
use crate::{AccessMode, RedisCommand, RedisDeployment, RedisValue};

const MAX_ITEMS: usize = 1_000;
const MAX_RANGE_BYTES: u64 = 64 * 1024;
const MAX_STRING_EXTENT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_RESTORE_PAYLOAD_BYTES: usize = 256 * 1024;
const DEFAULT_DUMP_MAX_BYTES: usize = 64 * 1024;
const DEFAULT_RETURNED_VALUE_MAX_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeysInput {
    /// Redis keys. Between 1 and 1000 keys are accepted per call.
    #[schemars(length(min = 1, max = 1000))]
    keys: Vec<String>,
}

fn validate_items(items: &[impl Sized], name: &str) -> tower_mcp::Result<()> {
    if items.is_empty() || items.len() > MAX_ITEMS {
        Err(tower_mcp::Error::tool(format!(
            "{name} must contain between 1 and {MAX_ITEMS} items"
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BinaryKeyInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
}

fn binary_key(input: &BinaryKeyInput) -> tower_mcp::Result<Vec<u8>> {
    decode_input(&input.key, input.key_encoding, "key")
}

fn require_ok(value: RedisValue, context: &str) -> tower_mcp::Result<()> {
    match value {
        RedisValue::Okay => Ok(()),
        RedisValue::SimpleString(value) if value.eq_ignore_ascii_case("OK") => Ok(()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected reply: {other:?}"
        ))),
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ExistsOutput {
    requested: usize,
    existing: u64,
    all_exist: bool,
}

fn exists_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_exists")
        .title("Check Redis Keys")
        .description("Count how many of the requested Redis keys currently exist.")
        .output_schema(output_schema::<ExistsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeysInput>| async move {
                validate_items(&input.keys, "keys")?;
                let requested = input.keys.len();
                let mut command = command("redis_exists", AccessMode::ReadOnly, "EXISTS");
                for key in &input.keys {
                    command.arg(key.as_str());
                }
                let existing = state.query(command, "EXISTS failed").await?;
                state.output(&ExistsOutput {
                    requested,
                    existing,
                    all_exist: existing == requested as u64,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MgetEntry {
    key: String,
    exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MgetOutput {
    values: Vec<MgetEntry>,
    count: usize,
}

fn mget_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_mget")
        .title("Get Multiple Redis Strings")
        .description("Read multiple Redis string values atomically. Binary values are base64.")
        .output_schema(output_schema::<MgetOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeysInput>| async move {
                validate_items(&input.keys, "keys")?;
                let mut command = command("redis_mget", AccessMode::ReadOnly, "MGET");
                for key in &input.keys {
                    command.arg(key.as_str());
                }
                let values: Vec<Option<Vec<u8>>> = state.query(command, "MGET failed").await?;
                if values.len() != input.keys.len() {
                    return Err(tower_mcp::Error::tool(format!(
                        "MGET returned {} values for {} keys",
                        values.len(),
                        input.keys.len()
                    )));
                }
                let values = input
                    .keys
                    .into_iter()
                    .zip(values)
                    .map(|(key, value)| match value {
                        Some(bytes) => {
                            let (value, encoding) = super::encode_bytes(bytes);
                            MgetEntry {
                                key,
                                exists: true,
                                value: Some(value),
                                encoding: Some(encoding),
                            }
                        }
                        None => MgetEntry {
                            key,
                            exists: false,
                            value: None,
                            encoding: None,
                        },
                    })
                    .collect::<Vec<_>>();
                let output = MgetOutput {
                    count: values.len(),
                    values,
                };
                state.output_collection(&output, output.count, "Retry MGET with fewer keys.")
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
struct StrlenOutput {
    key: String,
    length_bytes: u64,
}

fn strlen_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_strlen")
        .title("Redis String Length")
        .description("Read a Redis string length in bytes. Missing keys have length zero.")
        .output_schema(output_schema::<StrlenOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = command("redis_strlen", AccessMode::ReadOnly, "STRLEN");
                command.arg(input.key.as_str());
                let length_bytes = state.query(command, "STRLEN failed").await?;
                state.output(&StrlenOutput {
                    key: input.key,
                    length_bytes,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MemoryUsageOutput {
    key: String,
    exists: bool,
    bytes: Option<u64>,
}

fn memory_usage_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_memory_usage")
        .title("Redis Key Memory Usage")
        .description("Read the number of bytes a Redis key and its value require in memory.")
        .output_schema(output_schema::<MemoryUsageOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                let mut command = command("redis_memory_usage", AccessMode::ReadOnly, "MEMORY");
                command.arg("USAGE").arg(input.key.as_str());
                let bytes: Option<u64> = state.query(command, "MEMORY USAGE failed").await?;
                state.output(&MemoryUsageOutput {
                    key: input.key,
                    exists: bytes.is_some(),
                    bytes,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RandomKeyOutput {
    key: Option<String>,
    encoding: Option<ValueEncoding>,
}

fn randomkey_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_randomkey")
        .title("Random Redis Key")
        .description("Return one random key, or null when the selected database is empty.")
        .input_schema(super::empty_input_schema())
        .output_schema(output_schema::<RandomKeyOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let key: Option<Vec<u8>> = state
                .query(
                    command("redis_randomkey", AccessMode::ReadOnly, "RANDOMKEY"),
                    "RANDOMKEY failed",
                )
                .await?;
            let (key, encoding) = match key {
                Some(bytes) => {
                    let (key, encoding) = super::encode_bytes(bytes);
                    (Some(key), Some(encoding))
                }
                None => (None, None),
            };
            state.output(&RandomKeyOutput { key, encoding })
        })
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ExpireInput {
    /// Redis key.
    key: String,
    /// Positive expiration in seconds.
    #[schemars(range(min = 1))]
    seconds: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ExpireOutput {
    key: String,
    seconds: u64,
    applied: bool,
}

fn expire_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_expire")
        .title("Expire Redis Key")
        .description(
            "Set a positive key expiration in seconds. Missing keys are reported without error.",
        )
        .output_schema(output_schema::<ExpireOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ExpireInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_expire")?;
                if input.seconds == 0 {
                    return Err(tower_mcp::Error::tool("seconds must be greater than zero"));
                }
                let mut command = command("redis_expire", AccessMode::ReadWrite, "EXPIRE");
                command
                    .arg(input.key.as_str())
                    .arg(input.seconds.to_string());
                let applied = state.query(command, "EXPIRE failed").await?;
                state.output(&ExpireOutput {
                    key: input.key,
                    seconds: input.seconds,
                    applied,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PersistOutput {
    key: String,
    applied: bool,
}

fn persist_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_persist")
        .title("Persist Redis Key")
        .description("Remove a key expiration. Missing or already-persistent keys report false.")
        .output_schema(output_schema::<PersistOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_persist")?;
                let mut command = command("redis_persist", AccessMode::ReadWrite, "PERSIST");
                command.arg(input.key.as_str());
                let applied = state.query(command, "PERSIST failed").await?;
                state.output(&PersistOutput {
                    key: input.key,
                    applied,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct KeyValuePair {
    /// Redis key.
    key: String,
    /// UTF-8 value.
    value: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MsetInput {
    /// Key-value pairs to store atomically.
    #[schemars(length(min = 1, max = 1000))]
    entries: Vec<KeyValuePair>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct MsetOutput {
    stored: usize,
}

fn mset_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_mset")
        .title("Set Multiple Redis Strings")
        .description("Atomically store between 1 and 1000 UTF-8 key-value pairs.")
        .output_schema(output_schema::<MsetOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<MsetInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_mset")?;
                validate_items(&input.entries, "entries")?;
                let stored = input.entries.len();
                let mut command = command("redis_mset", AccessMode::ReadWrite, "MSET");
                for entry in input.entries {
                    command.arg(entry.key).arg(entry.value);
                }
                let _: String = state.query(command, "MSET failed").await?;
                state.output(&MsetOutput { stored })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrOutput {
    key: String,
    key_encoding: InputEncoding,
    value: i64,
}

fn incr_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_incr")
        .title("Increment Redis Integer")
        .description("Increment a Redis integer string by one and return the new value.")
        .output_schema(output_schema::<IncrOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BinaryKeyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_incr")?;
                let mut command = command("redis_incr", AccessMode::ReadWrite, "INCR");
                command.arg(binary_key(&input)?);
                let value = state.query(command, "INCR failed").await?;
                state.output(&IncrOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AppendInput {
    /// Redis key.
    key: String,
    /// UTF-8 value to append.
    value: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AppendOutput {
    key: String,
    length_bytes: u64,
}

fn append_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_append")
        .title("Append Redis String")
        .description("Append a UTF-8 value to a Redis string and return its new byte length.")
        .output_schema(output_schema::<AppendOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<AppendInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_append")?;
                let mut command = command("redis_append", AccessMode::ReadWrite, "APPEND");
                command.arg(input.key.as_str()).arg(input.value);
                let length_bytes = state.query(command, "APPEND failed").await?;
                state.output(&AppendOutput {
                    key: input.key,
                    length_bytes,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BoundedStringValueOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
    value_bytes: Option<usize>,
    value_omitted: bool,
}

fn default_returned_value_max_bytes() -> usize {
    DEFAULT_RETURNED_VALUE_MAX_BYTES
}

fn validate_returned_value_limit(
    state: &ToolState,
    requested: usize,
    name: &str,
) -> tower_mcp::Result<usize> {
    if requested == 0 {
        Err(tower_mcp::Error::tool(format!(
            "{name} must be greater than zero"
        )))
    } else {
        Ok(requested.min(state.max_output_bytes()))
    }
}

fn bounded_encoded_optional(
    value: Option<Vec<u8>>,
    max_bytes: usize,
) -> (
    bool,
    Option<String>,
    Option<ValueEncoding>,
    Option<usize>,
    bool,
) {
    let exists = value.is_some();
    let value_bytes = value.as_ref().map(Vec::len);
    let value_omitted = value.as_ref().is_some_and(|value| value.len() > max_bytes);
    let (value, encoding) = match value {
        Some(value) if !value_omitted => {
            let (value, encoding) = encode_bytes(value);
            (Some(value), Some(encoding))
        }
        _ => (None, None),
    };
    (exists, value, encoding, value_bytes, value_omitted)
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetDelInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Maximum deleted-value bytes to include in the result. Larger values are reported as
    /// omitted so deletion remains observable.
    #[serde(default = "default_returned_value_max_bytes")]
    #[schemars(range(min = 1))]
    max_value_bytes: usize,
}

fn getdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_getdel")
        .title("Get and Delete Redis String")
        .description(
            "Atomically return and delete a binary-safe Redis string. Requires full access.",
        )
        .output_schema(output_schema::<BoundedStringValueOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GetDelInput>| async move {
                state.require(AccessMode::Full, "redis_getdel")?;
                let max_value_bytes = validate_returned_value_limit(
                    &state,
                    input.max_value_bytes,
                    "max_value_bytes",
                )?;
                let mut command = command("redis_getdel", AccessMode::Full, "GETDEL");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let value = optional_bytes(state.raw(command, "GETDEL failed").await?, "GETDEL")?;
                let (exists, value, encoding, value_bytes, value_omitted) =
                    bounded_encoded_optional(value, max_value_bytes);
                state.output(&BoundedStringValueOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    value,
                    encoding,
                    value_bytes,
                    value_omitted,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum GetExExpiration {
    Seconds(#[schemars(range(min = 1))] u64),
    Milliseconds(#[schemars(range(min = 1))] u64),
    UnixSeconds(#[schemars(range(min = 1))] u64),
    UnixMilliseconds(#[schemars(range(min = 1))] u64),
    Persist,
}

impl GetExExpiration {
    fn append_to(self, command: &mut RedisCommand) -> tower_mcp::Result<()> {
        let (token, value) = match self {
            Self::Seconds(value) => ("EX", Some(value)),
            Self::Milliseconds(value) => ("PX", Some(value)),
            Self::UnixSeconds(value) => ("EXAT", Some(value)),
            Self::UnixMilliseconds(value) => ("PXAT", Some(value)),
            Self::Persist => ("PERSIST", None),
        };
        if value == Some(0) {
            return Err(tower_mcp::Error::tool(
                "expiration value must be greater than zero",
            ));
        }
        command.arg(token);
        if let Some(value) = value {
            command.arg(value.to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetExInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Expiration change to apply atomically with the read.
    expiration: GetExExpiration,
    /// Maximum value bytes to include in the result. Larger values are reported as omitted so
    /// the expiration change remains observable.
    #[serde(default = "default_returned_value_max_bytes")]
    #[schemars(range(min = 1))]
    max_value_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetExOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    value: Option<String>,
    encoding: Option<ValueEncoding>,
    value_bytes: Option<usize>,
    value_omitted: bool,
    expiration: GetExExpiration,
}

fn getex_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_getex")
        .title("Get Redis String and Change Expiration")
        .description(
            "Atomically read a binary-safe Redis string and apply exactly one expiration behavior.",
        )
        .output_schema(output_schema::<GetExOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GetExInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_getex")?;
                let max_value_bytes = validate_returned_value_limit(
                    &state,
                    input.max_value_bytes,
                    "max_value_bytes",
                )?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut command = command("redis_getex", AccessMode::ReadWrite, "GETEX");
                command.arg(key);
                input.expiration.append_to(&mut command)?;
                let value = optional_bytes(state.raw(command, "GETEX failed").await?, "GETEX")?;
                let (exists, value, encoding, value_bytes, value_omitted) =
                    bounded_encoded_optional(value, max_value_bytes);
                state.output(&GetExOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists,
                    value,
                    encoding,
                    value_bytes,
                    value_omitted,
                    expiration: input.expiration,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetRangeInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Inclusive non-negative byte offset.
    start: u64,
    /// Inclusive non-negative byte offset, at most 65536 bytes after `start`.
    end: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetRangeOutput {
    key: String,
    key_encoding: InputEncoding,
    start: u64,
    end: u64,
    bytes: usize,
    value: String,
    encoding: ValueEncoding,
}

fn getrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_getrange")
        .title("Get Redis String Byte Range")
        .description(
            "Read an inclusive, non-negative Redis string byte range of at most 65536 bytes. Binary data is returned as base64.",
        )
        .output_schema(output_schema::<GetRangeOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<GetRangeInput>| async move {
                if input.end < input.start {
                    return Err(tower_mcp::Error::tool(
                        "end must be greater than or equal to start",
                    ));
                }
                let requested = input
                    .end
                    .checked_sub(input.start)
                    .and_then(|span| span.checked_add(1))
                    .ok_or_else(|| tower_mcp::Error::tool("range length overflow"))?;
                if requested > MAX_RANGE_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "range must contain at most {MAX_RANGE_BYTES} bytes"
                    )));
                }
                let mut command = command("redis_getrange", AccessMode::ReadOnly, "GETRANGE");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.start.to_string())
                    .arg(input.end.to_string());
                let bytes: Vec<u8> = state.query(command, "GETRANGE failed").await?;
                let byte_count = bytes.len();
                let (value, encoding) = encode_bytes(bytes);
                state.output(&GetRangeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    start: input.start,
                    end: input.end,
                    bytes: byte_count,
                    value,
                    encoding,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetRangeInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Non-negative byte offset, with the resulting string capped at 16 MiB.
    offset: u64,
    /// Bytes to write.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    value_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetRangeOutput {
    key: String,
    key_encoding: InputEncoding,
    offset: u64,
    written_bytes: usize,
    length_bytes: u64,
}

fn setrange_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_setrange")
        .title("Set Redis String Byte Range")
        .description(
            "Write at most 65536 binary-safe bytes at a bounded offset, with a 16 MiB maximum resulting extent.",
        )
        .output_schema(output_schema::<SetRangeOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SetRangeInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_setrange")?;
                let value = decode_input(&input.value, input.value_encoding, "value")?;
                if value.len() > MAX_RANGE_BYTES as usize {
                    return Err(tower_mcp::Error::tool(format!(
                        "value must contain at most {MAX_RANGE_BYTES} decoded bytes"
                    )));
                }
                let extent = input
                    .offset
                    .checked_add(value.len() as u64)
                    .ok_or_else(|| tower_mcp::Error::tool("offset and value length overflow"))?;
                if extent > MAX_STRING_EXTENT_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "offset plus decoded value length must not exceed {MAX_STRING_EXTENT_BYTES} bytes"
                    )));
                }
                let written_bytes = value.len();
                let mut command = command("redis_setrange", AccessMode::ReadWrite, "SETRANGE");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.offset.to_string())
                    .arg(value);
                let length_bytes = state.query(command, "SETRANGE failed").await?;
                state.output(&SetRangeOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    offset: input.offset,
                    written_bytes,
                    length_bytes,
                })
            },
        )
        .build()
}

fn integer_mutation_tool(
    state: Arc<ToolState>,
    tool_name: &'static str,
    title: &'static str,
    description: &'static str,
    command_name: &'static str,
) -> Tool {
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<IncrOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<BinaryKeyInput>| async move {
                state.require(AccessMode::ReadWrite, tool_name)?;
                let mut command = command(tool_name, AccessMode::ReadWrite, command_name);
                command.arg(binary_key(&input)?);
                let value = state.query(command, &format!("{command_name} failed")).await?;
                state.output(&IncrOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IntegerByInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Signed amount accepted by Redis.
    amount: i64,
}

fn integer_by_tool(
    state: Arc<ToolState>,
    tool_name: &'static str,
    title: &'static str,
    description: &'static str,
    command_name: &'static str,
) -> Tool {
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<IncrOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<IntegerByInput>| async move {
                state.require(AccessMode::ReadWrite, tool_name)?;
                let mut command = command(tool_name, AccessMode::ReadWrite, command_name);
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.amount.to_string());
                let value = state.query(command, &format!("{command_name} failed")).await?;
                state.output(&IncrOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrByFloatInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Finite increment.
    amount: f64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrByFloatOutput {
    key: String,
    key_encoding: InputEncoding,
    value: String,
}

fn incrbyfloat_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_incrbyfloat")
        .title("Increment Redis Float")
        .description(
            "Increment a Redis numeric string by a finite float and return Redis's canonical decimal string.",
        )
        .output_schema(output_schema::<IncrByFloatOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<IncrByFloatInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_incrbyfloat")?;
                if !input.amount.is_finite() {
                    return Err(tower_mcp::Error::tool("amount must be finite"));
                }
                let mut command = command(
                    "redis_incrbyfloat",
                    AccessMode::ReadWrite,
                    "INCRBYFLOAT",
                );
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.amount.to_string());
                let value: Vec<u8> = state.query(command, "INCRBYFLOAT failed").await?;
                let value = String::from_utf8(value).map_err(|_| {
                    tower_mcp::Error::tool("INCRBYFLOAT returned a non-UTF-8 decimal")
                })?;
                state.output(&IncrByFloatOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TwoKeysInput {
    /// Source Redis key.
    source: String,
    /// Encoding of `source`.
    #[serde(default)]
    source_encoding: InputEncoding,
    /// Destination Redis key.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CopyOutput {
    source: String,
    source_encoding: InputEncoding,
    destination: String,
    destination_encoding: InputEncoding,
    copied: bool,
    overwrite_allowed: bool,
}

fn copy_tool(state: Arc<ToolState>, replace: bool) -> Tool {
    let (tool_name, title, description, required_access, annotations) = if replace {
        (
            "redis_copy_replace",
            "Copy and Replace Redis Key",
            "Copy a key in the selected database, overwriting the destination. Requires full access; Cluster keys must share a slot.",
            AccessMode::Full,
            destructive_annotations(true),
        )
    } else {
        (
            "redis_copy",
            "Copy Redis Key",
            "Copy a key in the selected database only when the destination is absent. Cluster keys must share a slot.",
            AccessMode::ReadWrite,
            write_annotations(true),
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<CopyOutput>())
        .annotations(annotations)
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<TwoKeysInput>| async move {
                state.require(required_access, tool_name)?;
                let mut command = command(tool_name, required_access, "COPY");
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
                    )?);
                if replace {
                    command.arg("REPLACE");
                }
                let copied = state.query(command, "COPY failed").await?;
                state.output(&CopyOutput {
                    source: input.source,
                    source_encoding: input.source_encoding,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    copied,
                    overwrite_allowed: replace,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TouchOutput {
    requested: usize,
    touched: u64,
}

fn touch_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_touch")
        .title("Touch Redis Keys")
        .description("Update the last-access time for between 1 and 1000 Redis keys.")
        .output_schema(output_schema::<TouchOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeysInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_touch")?;
                validate_items(&input.keys, "keys")?;
                let requested = input.keys.len();
                let mut command = command("redis_touch", AccessMode::ReadWrite, "TOUCH");
                command.args(input.keys);
                let touched = state.query(command, "TOUCH failed").await?;
                state.output(&TouchOutput { requested, touched })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RenameOutput {
    source: String,
    source_encoding: InputEncoding,
    destination: String,
    destination_encoding: InputEncoding,
    renamed: bool,
    overwrite_allowed: bool,
}

fn rename_tool(state: Arc<ToolState>, only_if_absent: bool) -> Tool {
    let (tool_name, title, description, command_name, idempotent) = if only_if_absent {
        (
            "redis_renamenx",
            "Rename Redis Key If Destination Is Absent",
            "Rename a key only when the destination is absent. Requires full access; Cluster keys must share a slot.",
            "RENAMENX",
            true,
        )
    } else {
        (
            "redis_rename",
            "Rename Redis Key",
            "Rename a key and overwrite an existing destination. Requires full access; Cluster keys must share a slot.",
            "RENAME",
            false,
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<RenameOutput>())
        .annotations(destructive_annotations(idempotent))
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<TwoKeysInput>| async move {
                state.require(AccessMode::Full, tool_name)?;
                let mut command = command(tool_name, AccessMode::Full, command_name);
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
                    )?);
                let response = state
                    .raw(command, &format!("{command_name} failed"))
                    .await?;
                let renamed = if only_if_absent {
                    match response {
                        RedisValue::Integer(value) => value == 1,
                        RedisValue::Boolean(value) => value,
                        other => {
                            return Err(tower_mcp::Error::tool(format!(
                                "RENAMENX returned an unexpected reply: {other:?}"
                            )));
                        }
                    }
                } else {
                    require_ok(response, "RENAME")?;
                    true
                };
                state.output(&RenameOutput {
                    source: input.source,
                    source_encoding: input.source_encoding,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    renamed,
                    overwrite_allowed: !only_if_absent,
                })
            },
        )
        .build()
}

fn default_dump_max_bytes() -> usize {
    DEFAULT_DUMP_MAX_BYTES
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DumpInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Maximum serialized payload bytes accepted by this call.
    #[serde(default = "default_dump_max_bytes")]
    #[schemars(range(min = 1))]
    max_bytes: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DumpOutput {
    key: String,
    key_encoding: InputEncoding,
    exists: bool,
    payload_base64: Option<String>,
    payload_bytes: usize,
}

fn dump_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_dump")
        .title("Dump Redis Key")
        .description(
            "Return a Redis serialized payload as base64 only when it fits both the requested and configured output budgets.",
        )
        .output_schema(output_schema::<DumpOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DumpInput>| async move {
                if input.max_bytes == 0 {
                    return Err(tower_mcp::Error::tool(
                        "max_bytes must be greater than zero",
                    ));
                }
                let max_bytes = input.max_bytes.min(state.max_output_bytes());
                let mut command = command("redis_dump", AccessMode::ReadOnly, "DUMP");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                let payload = optional_bytes(state.raw(command, "DUMP failed").await?, "DUMP")?;
                if payload
                    .as_ref()
                    .is_some_and(|payload| payload.len() > max_bytes)
                {
                    return Err(tower_mcp::Error::tool(format!(
                        "DUMP payload exceeds requested max_bytes of {}",
                        max_bytes
                    )));
                }
                let payload_bytes = payload.as_ref().map_or(0, Vec::len);
                state.output(&DumpOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    exists: payload.is_some(),
                    payload_base64: payload.map(|payload| {
                        base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            payload,
                        )
                    }),
                    payload_bytes,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RestoreInput {
    /// Destination Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Base64-encoded payload produced by DUMP.
    #[schemars(length(max = 349528))]
    payload_base64: String,
    /// TTL in milliseconds; zero creates a persistent key.
    #[serde(default)]
    ttl_milliseconds: u64,
    /// Interpret `ttl_milliseconds` as an absolute Unix timestamp.
    #[serde(default)]
    absolute_ttl: bool,
    /// Optional idle time in positive seconds.
    #[serde(default)]
    #[schemars(range(min = 1))]
    idle_time_seconds: Option<u64>,
    /// Optional LFU frequency from 0 through 255.
    #[serde(default)]
    frequency: Option<u8>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RestoreOutput {
    key: String,
    key_encoding: InputEncoding,
    restored: bool,
    overwrite_allowed: bool,
    payload_bytes: usize,
    ttl_milliseconds: u64,
    absolute_ttl: bool,
}

fn restore_tool(state: Arc<ToolState>, replace: bool) -> Tool {
    let (tool_name, title, description, required_access, annotations) = if replace {
        (
            "redis_restore_replace",
            "Restore and Replace Redis Key",
            "Restore a bounded DUMP payload and overwrite an existing destination. Requires full access.",
            AccessMode::Full,
            destructive_annotations(false),
        )
    } else {
        (
            "redis_restore",
            "Restore Redis Key",
            "Restore a bounded DUMP payload only when the destination key is absent.",
            AccessMode::ReadWrite,
            write_annotations(true),
        )
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<RestoreOutput>())
        .annotations(annotations)
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<RestoreInput>| async move {
                state.require(required_access, tool_name)?;
                if input.idle_time_seconds == Some(0) {
                    return Err(tower_mcp::Error::tool(
                        "idle_time_seconds must be greater than zero when provided",
                    ));
                }
                let payload = base64::Engine::decode(
                    &base64::engine::general_purpose::STANDARD,
                    &input.payload_base64,
                )
                .map_err(|_| {
                    tower_mcp::Error::tool("payload_base64 is not valid standard base64")
                })?;
                if payload.len() > MAX_RESTORE_PAYLOAD_BYTES {
                    return Err(tower_mcp::Error::tool(format!(
                        "decoded payload must not exceed {MAX_RESTORE_PAYLOAD_BYTES} bytes"
                    )));
                }
                let payload_bytes = payload.len();
                let mut command = command(tool_name, required_access, "RESTORE");
                command
                    .arg(decode_input(&input.key, input.key_encoding, "key")?)
                    .arg(input.ttl_milliseconds.to_string())
                    .arg(payload);
                if replace {
                    command.arg("REPLACE");
                }
                if input.absolute_ttl {
                    command.arg("ABSTTL");
                }
                if let Some(idle_time_seconds) = input.idle_time_seconds {
                    command.arg("IDLETIME").arg(idle_time_seconds.to_string());
                }
                if let Some(frequency) = input.frequency {
                    command.arg("FREQ").arg(frequency.to_string());
                }
                require_ok(state.raw(command, "RESTORE failed").await?, "RESTORE")?;
                state.output(&RestoreOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    restored: true,
                    overwrite_allowed: replace,
                    payload_bytes,
                    ttl_milliseconds: input.ttl_milliseconds,
                    absolute_ttl: input.absolute_ttl,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ObjectOperation {
    Encoding,
    Frequency,
    IdleTime,
    Refcount,
}

impl ObjectOperation {
    fn redis_token(self) -> &'static str {
        match self {
            Self::Encoding => "ENCODING",
            Self::Frequency => "FREQ",
            Self::IdleTime => "IDLETIME",
            Self::Refcount => "REFCOUNT",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ObjectInspectInput {
    /// Redis key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// One bounded OBJECT inspection; HELP is intentionally unavailable.
    operation: ObjectOperation,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ObjectInspectOutput {
    key: String,
    key_encoding: InputEncoding,
    operation: ObjectOperation,
    exists: bool,
    encoding: Option<String>,
    value: Option<u64>,
}

fn object_inspect_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_object_inspect")
        .title("Inspect Redis Object")
        .description(
            "Read one safe OBJECT property: encoding, frequency, idle time, or reference count. OBJECT HELP is excluded.",
        )
        .output_schema(output_schema::<ObjectInspectOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<ObjectInspectInput>| async move {
                let mut command = command("redis_object_inspect", AccessMode::ReadOnly, "OBJECT");
                command
                    .arg(input.operation.redis_token())
                    .arg(decode_input(&input.key, input.key_encoding, "key")?);
                let response = state.raw(command, "OBJECT inspection failed").await?;
                let (exists, encoding, value) = match (input.operation, response) {
                    (_, RedisValue::Nil) => (false, None, None),
                    (ObjectOperation::Encoding, RedisValue::BulkString(value)) => {
                        let encoding = String::from_utf8(value).map_err(|_| {
                            tower_mcp::Error::tool("OBJECT ENCODING returned non-UTF-8 data")
                        })?;
                        (true, Some(encoding), None)
                    }
                    (ObjectOperation::Encoding, RedisValue::SimpleString(value)) => {
                        (true, Some(value), None)
                    }
                    (_, RedisValue::Integer(value)) if value >= 0 => {
                        (true, None, Some(value as u64))
                    }
                    (operation, other) => {
                        return Err(tower_mcp::Error::tool(format!(
                            "OBJECT {operation:?} returned an unexpected reply: {other:?}"
                        )));
                    }
                };
                state.output(&ObjectInspectOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    operation: input.operation,
                    exists,
                    encoding,
                    value,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct UnlinkOutput {
    requested: usize,
    unlinked: u64,
}

fn unlink_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_unlink")
        .title("Asynchronously Delete Redis Keys")
        .description("Asynchronously unlink one or more Redis keys. Requires full access.")
        .output_schema(output_schema::<UnlinkOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeysInput>| async move {
                state.require(AccessMode::Full, "redis_unlink")?;
                validate_items(&input.keys, "keys")?;
                let requested = input.keys.len();
                let mut command = command("redis_unlink", AccessMode::Full, "UNLINK");
                command.args(input.keys);
                let unlinked = state.query(command, "UNLINK failed").await?;
                state.output(&UnlinkOutput {
                    requested,
                    unlinked,
                })
            },
        )
        .build()
}

const MAX_SORT_PATTERNS: usize = 64;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedSortPattern {
    /// Redis SORT BY/GET pattern.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(untagged)]
enum SortPatternSelector {
    Utf8(String),
    Encoded(EncodedSortPattern),
}

impl SortPatternSelector {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        let value = match self {
            Self::Utf8(value) => Ok(value.as_bytes().to_vec()),
            Self::Encoded(value) => decode_input(&value.value, value.encoding, name),
        }?;
        if value.is_empty() {
            return Err(tower_mcp::Error::tool(format!("{name} must not be empty")));
        }
        Ok(value)
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SortOrder {
    #[default]
    Ascending,
    Descending,
}

impl SortOrder {
    fn redis_token(self) -> &'static str {
        match self {
            Self::Ascending => "ASC",
            Self::Descending => "DESC",
        }
    }
}

fn default_sort_count() -> usize {
    100
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortOptions {
    /// Optional external-key BY pattern. Rejected on Redis Cluster because the expanded keys cannot be prevalidated.
    #[serde(default)]
    by: Option<SortPatternSelector>,
    /// Optional external-key or `#` GET patterns.
    #[serde(default)]
    #[schemars(length(max = 64))]
    get: Vec<SortPatternSelector>,
    /// Zero-based result offset.
    #[serde(default)]
    offset: u64,
    /// Maximum source elements to sort and return/store.
    #[serde(default = "default_sort_count")]
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Sort ascending or descending.
    #[serde(default)]
    order: SortOrder,
    /// Sort lexicographically instead of parsing numeric values.
    #[serde(default)]
    alpha: bool,
}

impl SortOptions {
    fn validate(&self, state: &ToolState) -> tower_mcp::Result<()> {
        state.validate_requested_entries(self.count, "count")?;
        if self.get.len() > MAX_SORT_PATTERNS {
            return Err(tower_mcp::Error::tool(format!(
                "get must contain at most {MAX_SORT_PATTERNS} patterns"
            )));
        }
        let multiplier = self.get.len().max(1);
        let returned = self.count.checked_mul(multiplier).ok_or_else(|| {
            tower_mcp::Error::tool("count multiplied by GET patterns exceeds supported bounds")
        })?;
        state.validate_requested_entries(returned, "maximum returned values")?;
        if state.deployment() == RedisDeployment::Cluster {
            let external_by = self
                .by
                .as_ref()
                .map(|pattern| pattern.decode("by"))
                .transpose()?
                .is_some_and(|pattern| !pattern.eq_ignore_ascii_case(b"nosort"));
            let external_get = self
                .get
                .iter()
                .enumerate()
                .map(|(index, pattern)| pattern.decode(&format!("get[{index}]")))
                .collect::<tower_mcp::Result<Vec<_>>>()?
                .into_iter()
                .any(|pattern| pattern != b"#");
            if external_by || external_get {
                return Err(tower_mcp::Error::tool(
                    "SORT external-key BY/GET patterns are unavailable on Redis Cluster because expanded keys cannot be proven to share the source slot; BY nosort and GET # remain available",
                ));
            }
        }
        Ok(())
    }

    fn append_to(&self, command: &mut RedisCommand) -> tower_mcp::Result<()> {
        if let Some(by) = &self.by {
            command.arg("BY").arg(by.decode("by")?);
        }
        command
            .arg("LIMIT")
            .arg(self.offset.to_string())
            .arg(self.count.to_string());
        for (index, pattern) in self.get.iter().enumerate() {
            command
                .arg("GET")
                .arg(pattern.decode(&format!("get[{index}]"))?);
        }
        command.arg(self.order.redis_token());
        if self.alpha {
            command.arg("ALPHA");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortInput {
    /// List, set, or sorted-set source key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    #[serde(flatten)]
    options: SortOptions,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortValue {
    value: Option<String>,
    encoding: Option<ValueEncoding>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortOutput {
    key: String,
    key_encoding: InputEncoding,
    source_exists: bool,
    offset: u64,
    requested_count: usize,
    get_pattern_count: usize,
    returned: usize,
    values: Vec<SortValue>,
}

fn sort_read_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sort")
        .title("Sort Redis Collection")
        .description(
            "Read one explicitly bounded SORT_RO window from a list, set, or sorted set. BY/GET external-key patterns are rejected on Cluster.",
        )
        .output_schema(output_schema::<SortOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SortInput>| async move {
                input.options.validate(&state)?;
                let key = decode_input(&input.key, input.key_encoding, "key")?;
                let mut sort_command = command("redis_sort", AccessMode::ReadOnly, "SORT_RO");
                sort_command.arg(key.clone());
                input.options.append_to(&mut sort_command)?;
                let values: Vec<Option<Vec<u8>>> =
                    state.query(sort_command, "SORT_RO failed").await?;
                let source_exists = if values.is_empty() {
                    let mut exists = command("redis_sort", AccessMode::ReadOnly, "EXISTS");
                    exists.arg(key);
                    state.query::<u64>(exists, "EXISTS failed").await? != 0
                } else {
                    true
                };
                let values = values
                    .into_iter()
                    .map(|value| match value {
                        Some(value) => {
                            let (value, encoding) = encode_bytes(value);
                            SortValue {
                                value: Some(value),
                                encoding: Some(encoding),
                            }
                        }
                        None => SortValue {
                            value: None,
                            encoding: None,
                        },
                    })
                    .collect::<Vec<_>>();
                let output = SortOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    source_exists,
                    offset: input.options.offset,
                    requested_count: input.options.count,
                    get_pattern_count: input.options.get.len(),
                    returned: values.len(),
                    values,
                };
                state.output_collection(
                    &output,
                    output.returned,
                    "Retry SORT with a smaller count or fewer GET patterns.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortStoreInput {
    /// List, set, or sorted-set source key.
    key: String,
    /// Encoding of `key`.
    #[serde(default)]
    key_encoding: InputEncoding,
    /// Destination list key overwritten by SORT STORE.
    destination: String,
    /// Encoding of `destination`.
    #[serde(default)]
    destination_encoding: InputEncoding,
    #[serde(flatten)]
    options: SortOptions,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SortStoreOutput {
    key: String,
    key_encoding: InputEncoding,
    destination: String,
    destination_encoding: InputEncoding,
    requested_count: usize,
    stored: u64,
    destination_overwritten: bool,
    cluster_requires_same_slot: bool,
}

fn sort_store_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_sort_store")
        .title("Sort and Store Redis Collection")
        .description(
            "Sort one explicitly bounded window and overwrite a destination list. Source and destination must share a Cluster slot; BY/GET external-key patterns are rejected on Cluster.",
        )
        .output_schema(output_schema::<SortStoreOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<SortStoreInput>| async move {
                state.require(AccessMode::Full, "redis_sort_store")?;
                input.options.validate(&state)?;
                let mut command = command("redis_sort_store", AccessMode::Full, "SORT");
                command.arg(decode_input(&input.key, input.key_encoding, "key")?);
                input.options.append_to(&mut command)?;
                command.arg("STORE").arg(decode_input(
                    &input.destination,
                    input.destination_encoding,
                    "destination",
                )?);
                let stored = state.query(command, "SORT STORE failed").await?;
                state.output(&SortStoreOutput {
                    key: input.key,
                    key_encoding: input.key_encoding,
                    destination: input.destination,
                    destination_encoding: input.destination_encoding,
                    requested_count: input.options.count,
                    stored,
                    destination_overwritten: true,
                    cluster_requires_same_slot: true,
                })
            },
        )
        .build()
}

#[cfg(feature = "keyspace")]
pub(super) fn add_keyspace_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(exists_tool(state.clone()));
    router = router.tool(memory_usage_tool(state.clone()));
    router = router.tool(randomkey_tool(state.clone()));
    router = router.tool(dump_tool(state.clone()));
    router = router.tool(sort_read_tool(state.clone()));
    router.tool(object_inspect_tool(state))
}

#[cfg(feature = "strings")]
pub(super) fn add_string_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(mget_tool(state.clone()));
    router = router.tool(strlen_tool(state.clone()));
    router.tool(getrange_tool(state))
}

#[cfg(feature = "keyspace")]
pub(super) fn add_keyspace_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(expire_tool(state.clone()));
    router = router.tool(persist_tool(state.clone()));
    router = router.tool(copy_tool(state.clone(), false));
    router = router.tool(touch_tool(state.clone()));
    router.tool(restore_tool(state, false))
}

#[cfg(feature = "strings")]
pub(super) fn add_string_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(mset_tool(state.clone()));
    router = router.tool(incr_tool(state.clone()));
    router = router.tool(append_tool(state.clone()));
    router = router.tool(getex_tool(state.clone()));
    router = router.tool(setrange_tool(state.clone()));
    router = router.tool(integer_mutation_tool(
        state.clone(),
        "redis_decr",
        "Decrement Redis Integer",
        "Decrement a Redis integer string by one and return the new value.",
        "DECR",
    ));
    router = router.tool(integer_by_tool(
        state.clone(),
        "redis_decrby",
        "Decrement Redis Integer by Amount",
        "Decrement a Redis integer string by a signed amount and return the new value.",
        "DECRBY",
    ));
    router = router.tool(integer_by_tool(
        state.clone(),
        "redis_incrby",
        "Increment Redis Integer by Amount",
        "Increment a Redis integer string by a signed amount and return the new value.",
        "INCRBY",
    ));
    router = router.tool(incrbyfloat_tool(state.clone()));
    router
}

#[cfg(feature = "keyspace")]
pub(super) fn add_keyspace_destructive_tools(
    mut router: McpRouter,
    state: Arc<ToolState>,
) -> McpRouter {
    router = router.tool(unlink_tool(state.clone()));
    router = router.tool(copy_tool(state.clone(), true));
    router = router.tool(rename_tool(state.clone(), false));
    router = router.tool(rename_tool(state.clone(), true));
    router = router.tool(sort_store_tool(state.clone()));
    router.tool(restore_tool(state, true))
}

#[cfg(feature = "strings")]
pub(super) fn add_string_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(getdel_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_inputs_are_enforced_beyond_json_schema() {
        let empty: [String; 0] = [];
        assert!(validate_items(&empty, "keys").is_err());
        assert!(validate_items(&vec![String::new(); MAX_ITEMS + 1], "keys").is_err());
        assert!(validate_items(&[String::new()], "keys").is_ok());
    }
}
