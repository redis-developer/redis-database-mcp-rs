//! Governed Redis argv invocation tiers for Redis-syntax MCP clients.
//!
//! These tools expose the native invocation policy over MCP so a dual-dialect
//! CLI can execute `GET foo` through the same classification, access,
//! capability, timeout, redaction, and output-budget policies as the curated
//! `redis_get` tool, without importing a second execution backend. Every tier
//! keeps honest MCP annotations: the read-only tool refuses anything
//! classified above read-only instead of advertising safety it cannot prove.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, decode_input, empty_input_schema, output_limit_result, output_schema,
    read_annotations, write_annotations,
};
use crate::{
    AccessMode, NativeRedisInvocation,
    invocation::{redis_value_collection_entries, redis_value_to_json},
    raw::native_command_inventory,
};

const READONLY_TOOL: &str = "redis_command_readonly";
const WRITE_TOOL: &str = "redis_command_write";

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArgvArgumentInput {
    /// UTF-8 text or standard base64, according to `encoding`.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArgvCommandInput {
    /// Redis command name, without arguments.
    #[schemars(length(min = 1, max = 128))]
    command: String,
    /// Binary-safe command arguments in wire order.
    #[serde(default)]
    #[schemars(length(max = 1024))]
    arguments: Vec<ArgvArgumentInput>,
}

impl ArgvCommandInput {
    fn invocation(&self) -> tower_mcp::Result<NativeRedisInvocation> {
        let mut invocation = NativeRedisInvocation::new(self.command.trim().as_bytes().to_vec());
        for (index, argument) in self.arguments.iter().enumerate() {
            invocation = invocation.arg(decode_input(
                &argument.value,
                argument.encoding,
                &format!("arguments[{index}]"),
            )?);
        }
        Ok(invocation)
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ArgvCommandOutput {
    /// Normalized uppercase Redis command name.
    command: String,
    /// Access tier the classified command required.
    required_access: String,
    /// Whether this library version explicitly classified the command.
    classified: bool,
    value: JsonValue,
}

fn tier_for(access: AccessMode) -> &'static str {
    match access {
        AccessMode::ReadOnly => READONLY_TOOL,
        AccessMode::ReadWrite => WRITE_TOOL,
        AccessMode::Full => "redis_command",
    }
}

fn argv_tool(state: Arc<ToolState>, cap: AccessMode) -> Tool {
    let (tool_name, title, description) = match cap {
        AccessMode::ReadOnly => (
            READONLY_TOOL,
            "Run Read-Only Redis Command",
            "Run one classified read-only Redis request/response command from pre-tokenized binary-safe argv. Commands classified as writes or destructive operations are rejected here so this tool's read-only contract stays honest.",
        ),
        _ => (
            WRITE_TOOL,
            "Run Read-Write Redis Command",
            "Run one classified read-only or ordinary-write Redis request/response command from pre-tokenized binary-safe argv. Destructive and unclassified commands are rejected here; they require the full-access redis_command escape hatch.",
        ),
    };
    let annotations = if cap == AccessMode::ReadOnly {
        read_annotations()
    } else {
        write_annotations(false)
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<ArgvCommandOutput>())
        .annotations(annotations)
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>,
                  Json(input): Json<ArgvCommandInput>| async move {
                let invocation = input.invocation()?;
                let metadata = state.invocation_engine.classify(&invocation).map_err(
                    |error| {
                        tower_mcp::Error::tool(format!(
                            "command classification failed [{:?}]: {error}",
                            error.kind()
                        ))
                    },
                )?;
                if metadata.required_access() > cap {
                    return Err(tower_mcp::Error::tool(format!(
                        "[COMMAND_EXCEEDS_TOOL_ACCESS] {} requires {} access; use {} instead",
                        metadata.name(),
                        metadata.required_access(),
                        tier_for(metadata.required_access()),
                    )));
                }
                let response = match state.invocation_engine.invoke_with_metadata(invocation).await
                {
                    Ok(response) => response,
                    Err(error) => {
                        if let Some(limit) = error.output_limit() {
                            return Ok(output_limit_result(
                                limit.dimension().as_str(),
                                limit.actual(),
                                limit.limit(),
                                "Use a bounded command form with LIMIT, COUNT, or a cursor.",
                            ));
                        }
                        return Err(tower_mcp::Error::tool(format!(
                            "Redis command failed [{:?}]: {error}",
                            error.kind()
                        )));
                    }
                };
                let (metadata, value) = response.into_parts();
                let entries = redis_value_collection_entries(&value);
                let output = ArgvCommandOutput {
                    command: metadata.name().to_string(),
                    required_access: metadata.required_access().as_str().to_string(),
                    classified: metadata.is_classified(),
                    value: redis_value_to_json(&value),
                };
                state.output_collection(
                    &output,
                    entries,
                    "Use a bounded command form with LIMIT, COUNT, or a cursor.",
                )
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct CommandMetadataOutput {
    /// Normalized uppercase Redis command name.
    command: String,
    /// Whether this argv can execute through the governed invocation surface.
    supported: bool,
    /// Whether this library version explicitly classified the command.
    classified: Option<bool>,
    /// Access tier required by the classified command form.
    required_access: Option<String>,
    /// Invocation tool that serves the required access tier.
    invocation_tool: Option<String>,
    /// Whether this router's configured access level permits the command.
    permitted_here: Option<bool>,
    minimum_redis_version: Option<String>,
    required_module: Option<String>,
    minimum_module_version: Option<String>,
    /// Stable reason code when the command is unsupported.
    blocked_code: Option<String>,
    /// Human-readable reason when the command is unsupported.
    blocked_reason: Option<String>,
}

fn metadata_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_command_metadata")
        .title("Preview Redis Command Policy")
        .description(
            "Classify one pre-tokenized Redis command without executing it: the required access tier, matching invocation tool, capability requirements, and whether this router would permit it. Hard-blocked and unclassified commands report stable reason codes instead of failing.",
        )
        .output_schema(output_schema::<CommandMetadataOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<ArgvCommandInput>| async move {
                let invocation = input.invocation()?;
                let output = match state.invocation_engine.classify(&invocation) {
                    Ok(metadata) => CommandMetadataOutput {
                        command: metadata.name().to_string(),
                        supported: true,
                        classified: Some(metadata.is_classified()),
                        required_access: Some(metadata.required_access().as_str().to_string()),
                        invocation_tool: Some(
                            tier_for(metadata.required_access()).to_string(),
                        ),
                        permitted_here: Some(
                            state
                                .invocation_engine
                                .access()
                                .permits(metadata.required_access()),
                        ),
                        minimum_redis_version: metadata
                            .minimum_redis_version()
                            .map(|version| version.to_string()),
                        required_module: metadata
                            .required_module()
                            .map(|module| module.as_str().to_string()),
                        minimum_module_version: metadata
                            .minimum_module_version()
                            .map(|version| version.to_string()),
                        blocked_code: None,
                        blocked_reason: None,
                    },
                    Err(error) => CommandMetadataOutput {
                        command: input.command.trim().to_ascii_uppercase(),
                        supported: false,
                        classified: None,
                        required_access: None,
                        invocation_tool: None,
                        permitted_here: None,
                        minimum_redis_version: None,
                        required_module: None,
                        minimum_module_version: None,
                        blocked_code: error.code().map(str::to_string),
                        blocked_reason: Some(error.message().to_string()),
                    },
                };
                state.output(&output)
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InventoryCommandOutput {
    command: String,
    /// Base access tier for the argument-independent command form.
    required_access: String,
    /// Whether specific arguments escalate the base tier, such as COPY
    /// REPLACE or non-positive HEXPIRE.
    access_may_escalate: bool,
    minimum_redis_version: Option<String>,
    required_module: Option<String>,
    minimum_module_version: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct InventoryOutput {
    command_count: usize,
    /// Classified native commands in sorted order. Argument-dependent forms
    /// such as XGROUP subcommands classify only with their arguments and are
    /// previewed through redis_command_metadata instead.
    commands: Vec<InventoryCommandOutput>,
}

fn inventory_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_command_inventory")
        .title("List Classified Redis Commands")
        .description(
            "List every Redis command this library version classifies for governed argv invocation, with base access tiers and capability requirements, for host completion and preview.",
        )
        .input_schema(empty_input_schema())
        .output_schema(output_schema::<InventoryOutput>())
        .annotations(read_annotations())
        .extractor_handler(state, |State(state): State<Arc<ToolState>>| async move {
            let commands = native_command_inventory()
                .into_iter()
                .map(|entry| InventoryCommandOutput {
                    command: entry.name.to_string(),
                    required_access: entry.access.as_str().to_string(),
                    access_may_escalate: entry.access_may_escalate,
                    minimum_redis_version: entry
                        .minimum_redis_version
                        .map(|version| version.to_string()),
                    required_module: entry
                        .required_module
                        .map(|module| module.as_str().to_string()),
                    minimum_module_version: entry
                        .minimum_module_version
                        .map(|version| version.to_string()),
                })
                .collect::<Vec<_>>();
            let output = InventoryOutput {
                command_count: commands.len(),
                commands,
            };
            state.output_collection(
                &output,
                output.command_count,
                "Raise the configured collection-entry budget to list the full inventory.",
            )
        })
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(argv_tool(state.clone(), AccessMode::ReadOnly));
    router = router.tool(metadata_tool(state.clone()));
    router.tool(inventory_tool(state))
}

pub(super) fn add_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(argv_tool(state, AccessMode::ReadWrite))
}
