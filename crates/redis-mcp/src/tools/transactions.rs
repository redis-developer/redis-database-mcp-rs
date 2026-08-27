//! Bounded atomic MULTI/EXEC transaction tool.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{InputEncoding, ToolState, decode_input, destructive_annotations, output_schema};
use crate::{
    AccessMode, NativeRedisInvocation, RedisTransactionOutcome, RedisTransactionRequest,
    invocation::{redis_value_collection_entries, redis_value_to_json},
    transactions::TRANSACTION_TOOL_NAME,
};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionArgumentInput {
    /// UTF-8 text or standard base64, according to `encoding`.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

impl TransactionArgumentInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.value, self.encoding, name)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionCommandInput {
    /// Redis command name, without arguments.
    command: String,
    /// Binary-safe command arguments in wire order.
    #[serde(default)]
    arguments: Vec<TransactionArgumentInput>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionInput {
    /// Commands queued between MULTI and EXEC, in execution order. Every
    /// command passes the same classification policy as redis_command.
    #[schemars(length(min = 1, max = 64))]
    commands: Vec<TransactionCommandInput>,
    /// Optional binary-safe keys to WATCH before the transaction. If any
    /// watched key changes before EXEC, the transaction aborts.
    #[serde(default)]
    #[schemars(length(max = 16))]
    watch: Vec<TransactionArgumentInput>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionCommandResult {
    index: usize,
    command: String,
    value: JsonValue,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionFailure {
    index: Option<usize>,
    command: Option<String>,
    code: String,
    message: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransactionOutput {
    /// `committed`, `aborted` (a watched key changed), or `rejected` (the
    /// server refused queued commands and nothing executed).
    status: String,
    watched: usize,
    command_count: usize,
    /// Per-command results aligned with the request when committed. An entry
    /// may carry a server_error object when that command failed at runtime.
    results: Option<Vec<TransactionCommandResult>>,
    /// Queue-time rejections when the transaction was rejected.
    failures: Option<Vec<TransactionFailure>>,
}

pub(super) fn transaction_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new(TRANSACTION_TOOL_NAME)
        .title("Run Atomic Redis Transaction")
        .description(
            "Run one bounded command list atomically inside MULTI/EXEC on a dedicated connection, optionally watching keys for conflicting writes. Nested commands pass the same classification policy as redis_command; transaction, session, blocking, script, and admin forms stay rejected. The outcome is explicit: committed with aligned per-command results, aborted by a watched-key conflict, or rejected at queue time with nothing executed.",
        )
        .output_schema(output_schema::<TransactionOutput>())
        .annotations(destructive_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>,
             Json(input): Json<TransactionInput>| async move {
                state.require(AccessMode::Full, TRANSACTION_TOOL_NAME)?;
                state.require_tool_capabilities(TRANSACTION_TOOL_NAME)?;
                let engine = state.transactions()?.clone();

                let mut request = RedisTransactionRequest::new();
                for (index, key) in input.watch.iter().enumerate() {
                    request = request.watch(key.decode(&format!("watch[{index}]"))?);
                }
                let command_names = input
                    .commands
                    .iter()
                    .map(|command| command.command.trim().to_ascii_uppercase())
                    .collect::<Vec<_>>();
                for (index, command) in input.commands.iter().enumerate() {
                    let mut invocation =
                        NativeRedisInvocation::new(command.command.trim().as_bytes().to_vec());
                    for (position, argument) in command.arguments.iter().enumerate() {
                        invocation = invocation.arg(
                            argument.decode(&format!("commands[{index}].arguments[{position}]"))?,
                        );
                    }
                    request = request.command(invocation);
                }
                let watched = input.watch.len();
                let command_count = input.commands.len();

                let outcome = match engine.invoke(request).await {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        if let Some(limit) = error.output_limit() {
                            return Ok(super::output_limit_result(
                                limit.dimension().as_str(),
                                limit.actual(),
                                limit.limit(),
                                "Return fewer or smaller values from the transaction.",
                            ));
                        }
                        return Err(tower_mcp::Error::tool(format!(
                            "redis_transaction failed [{:?}]: {error}",
                            error.kind()
                        )));
                    }
                };

                let output = match outcome {
                    RedisTransactionOutcome::Committed { results } => {
                        let entries = results
                            .iter()
                            .map(redis_value_collection_entries)
                            .fold(results.len(), usize::saturating_add);
                        let results = results
                            .iter()
                            .enumerate()
                            .map(|(index, value)| TransactionCommandResult {
                                index,
                                command: command_names[index].clone(),
                                value: redis_value_to_json(value),
                            })
                            .collect();
                        let output = TransactionOutput {
                            status: "committed".to_string(),
                            watched,
                            command_count,
                            results: Some(results),
                            failures: None,
                        };
                        return state.output_collection(
                            &output,
                            entries,
                            "Return fewer or smaller values from the transaction.",
                        );
                    }
                    RedisTransactionOutcome::Aborted => TransactionOutput {
                        status: "aborted".to_string(),
                        watched,
                        command_count,
                        results: None,
                        failures: None,
                    },
                    RedisTransactionOutcome::Rejected { failures } => TransactionOutput {
                        status: "rejected".to_string(),
                        watched,
                        command_count,
                        results: None,
                        failures: Some(
                            failures
                                .iter()
                                .map(|failure| TransactionFailure {
                                    index: failure.index(),
                                    command: failure
                                        .index()
                                        .and_then(|index| command_names.get(index).cloned()),
                                    code: failure.code().to_string(),
                                    message: failure.message().map(str::to_string),
                                })
                                .collect(),
                        ),
                    },
                };
                state.output(&output)
            },
        )
        .build()
}
