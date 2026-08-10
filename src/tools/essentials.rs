//! Curated key and string operations.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tower_mcp::{
    CallToolResult, McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    ToolState, ValueEncoding, command, destructive_annotations, output_schema, read_annotations,
    write_annotations,
};
use crate::AccessMode;

const MAX_ITEMS: usize = 1_000;

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
                CallToolResult::from_serialize(&ExistsOutput {
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
                CallToolResult::from_serialize(&MgetOutput {
                    count: values.len(),
                    values,
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
                CallToolResult::from_serialize(&StrlenOutput {
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
                CallToolResult::from_serialize(&MemoryUsageOutput {
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
            CallToolResult::from_serialize(&RandomKeyOutput { key, encoding })
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
        .annotations(write_annotations(true))
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
                CallToolResult::from_serialize(&ExpireOutput {
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
                CallToolResult::from_serialize(&PersistOutput {
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
                CallToolResult::from_serialize(&MsetOutput { stored })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IncrOutput {
    key: String,
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
            |State(state): State<Arc<ToolState>>, Json(input): Json<KeyInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_incr")?;
                let mut command = command("redis_incr", AccessMode::ReadWrite, "INCR");
                command.arg(input.key.as_str());
                let value = state.query(command, "INCR failed").await?;
                CallToolResult::from_serialize(&IncrOutput {
                    key: input.key,
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
                CallToolResult::from_serialize(&AppendOutput {
                    key: input.key,
                    length_bytes,
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
                CallToolResult::from_serialize(&UnlinkOutput {
                    requested,
                    unlinked,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(exists_tool(state.clone()));
    router = router.tool(mget_tool(state.clone()));
    router = router.tool(strlen_tool(state.clone()));
    router = router.tool(memory_usage_tool(state.clone()));
    router.tool(randomkey_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(expire_tool(state.clone()));
    router = router.tool(persist_tool(state.clone()));
    router = router.tool(mset_tool(state.clone()));
    router = router.tool(incr_tool(state.clone()));
    router.tool(append_tool(state))
}

pub(super) fn add_destructive_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(unlink_tool(state))
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
