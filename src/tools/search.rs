//! Optional Redis Query Engine index and search operations.

use std::{collections::BTreeMap, sync::Arc};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    PageMetadata, ToolState, destructive_annotations, empty_input_schema, module_command,
    output_schema, read_annotations, redis_value_to_json, write_annotations,
};
use crate::{AccessMode, RedisModule, RedisValue};

const MAX_SEARCH_RESULTS: u64 = 100;
const MAX_SCHEMA_FIELDS: usize = 100;

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
struct SearchField {
    /// Hash field name or JSONPath expression.
    name: String,
    /// Query-facing field alias, strongly recommended for JSON indexes.
    #[serde(default)]
    alias: Option<String>,
    /// TEXT, TAG, NUMERIC, or GEO.
    field_type: String,
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
        if !matches!(field_type.as_str(), "TEXT" | "TAG" | "NUMERIC" | "GEO") {
            return Err(tower_mcp::Error::tool(format!(
                "unsupported field_type '{}'; expected TEXT, TAG, NUMERIC, or GEO",
                self.field_type
            )));
        }
        if self.nostem && field_type != "TEXT" {
            return Err(tower_mcp::Error::tool(
                "nostem is valid only for TEXT fields",
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
    created: bool,
}

fn ft_create_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_create")
        .title("Create Redis Search Index")
        .description("Create a HASH or JSON search index with TEXT, TAG, NUMERIC, or GEO fields.")
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
                    command.arg(field.normalized_type()?);
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
    router.tool(ft_search_tool(state))
}

pub(super) fn add_write_tools(router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router.tool(ft_create_tool(state))
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
            sortable: false,
            noindex: false,
            nostem: false,
        };
        assert!(field.normalized_type().is_err());
        field.field_type = "TAG".into();
        field.nostem = true;
        assert!(field.normalized_type().is_err());
    }
}
