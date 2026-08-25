//! Extended Redis Query Engine command families.

use super::*;

const MAX_AGGREGATE_STAGES: usize = 100;
const MAX_REDUCER_ARGUMENTS: usize = 16;

fn stateful_read_annotations() -> tower_mcp::ToolAnnotations {
    tower_mcp::ToolAnnotations {
        read_only_hint: true,
        destructive_hint: false,
        idempotent_hint: false,
        open_world_hint: true,
        ..tower_mcp::ToolAnnotations::default()
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EncodedSearchValue {
    value: String,
    encoding: ValueEncoding,
}

fn encoded_value(value: RedisValue, context: &str) -> tower_mcp::Result<EncodedSearchValue> {
    let (value, encoding) = super::super::encode_bytes(redis_text(value, context)?);
    Ok(EncodedSearchValue { value, encoding })
}

fn unsigned_integer(value: RedisValue, context: &str) -> tower_mcp::Result<u64> {
    match value {
        RedisValue::Integer(value) => u64::try_from(value)
            .map_err(|_| tower_mcp::Error::tool(format!("{context} returned a negative count"))),
        RedisValue::BulkString(value) => std::str::from_utf8(&value)
            .ok()
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| tower_mcp::Error::tool(format!("{context} returned an invalid count"))),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected count: {other:?}"
        ))),
    }
}

fn text_collection(value: RedisValue, context: &str) -> tower_mcp::Result<Vec<EncodedSearchValue>> {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => values
            .into_iter()
            .map(|value| encoded_value(value, context))
            .collect(),
        RedisValue::Nil => Ok(Vec::new()),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unexpected collection: {other:?}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum AggregateReducerFunction {
    Count,
    CountDistinct,
    CountDistinctish,
    Sum,
    Min,
    Max,
    Avg,
    Stddev,
    Quantile,
    ToList,
    FirstValue,
    RandomSample,
}

impl AggregateReducerFunction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::CountDistinct => "COUNT_DISTINCT",
            Self::CountDistinctish => "COUNT_DISTINCTISH",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::Stddev => "STDDEV",
            Self::Quantile => "QUANTILE",
            Self::ToList => "TOLIST",
            Self::FirstValue => "FIRST_VALUE",
            Self::RandomSample => "RANDOM_SAMPLE",
        }
    }

    fn validates(self, arguments: usize) -> bool {
        match self {
            Self::Count => arguments == 0,
            Self::CountDistinct
            | Self::CountDistinctish
            | Self::Sum
            | Self::Min
            | Self::Max
            | Self::Avg
            | Self::Stddev
            | Self::ToList => arguments == 1,
            Self::Quantile | Self::RandomSample => arguments == 2,
            Self::FirstValue => (1..=5).contains(&arguments),
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AggregateReducer {
    function: AggregateReducerFunction,
    /// Function arguments, normally aggregate properties such as `@price`.
    #[serde(default)]
    #[schemars(length(max = 16))]
    arguments: Vec<String>,
    /// Stable output field name for this reducer.
    #[serde(default)]
    alias: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AggregateSortField {
    property: String,
    order: SortOrder,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum AggregateStage {
    /// Group by zero or more properties and apply typed reducers.
    GroupBy {
        #[serde(default)]
        #[schemars(length(max = 100))]
        properties: Vec<String>,
        #[serde(default)]
        #[schemars(length(max = 100))]
        reducers: Vec<AggregateReducer>,
    },
    /// Sort rows by one or more properties.
    SortBy {
        #[schemars(length(min = 1, max = 100))]
        fields: Vec<AggregateSortField>,
        #[serde(default)]
        max: Option<u64>,
    },
    /// Compute a new field from an aggregate expression.
    Apply { expression: String, alias: String },
    /// Filter rows using an aggregate expression.
    Filter { expression: String },
    /// Bound an intermediate pipeline stage. A final bounded LIMIT is always added too.
    Limit {
        #[serde(default)]
        offset: u64,
        #[schemars(range(min = 1, max = 100))]
        count: u64,
    },
}

impl AggregateStage {
    fn validate(&self) -> tower_mcp::Result<()> {
        match self {
            Self::GroupBy {
                properties,
                reducers,
            } => {
                if properties.len() > MAX_SCHEMA_FIELDS || reducers.len() > MAX_SCHEMA_FIELDS {
                    return Err(tower_mcp::Error::tool(format!(
                        "GROUPBY properties and reducers are each limited to {MAX_SCHEMA_FIELDS} items"
                    )));
                }
                for reducer in reducers {
                    if reducer.arguments.len() > MAX_REDUCER_ARGUMENTS
                        || !reducer.function.validates(reducer.arguments.len())
                    {
                        return Err(tower_mcp::Error::tool(format!(
                            "{} received an invalid number of arguments",
                            reducer.function.as_str()
                        )));
                    }
                }
            }
            Self::SortBy { fields, max } => {
                if fields.is_empty() || fields.len() > MAX_SCHEMA_FIELDS {
                    return Err(tower_mcp::Error::tool(format!(
                        "SORTBY fields must contain between 1 and {MAX_SCHEMA_FIELDS} items"
                    )));
                }
                if *max == Some(0) {
                    return Err(tower_mcp::Error::tool(
                        "SORTBY max must be greater than zero",
                    ));
                }
            }
            Self::Apply { expression, alias } => {
                if expression.is_empty() || alias.is_empty() {
                    return Err(tower_mcp::Error::tool(
                        "APPLY expression and alias must be non-empty",
                    ));
                }
            }
            Self::Filter { expression } if expression.is_empty() => {
                return Err(tower_mcp::Error::tool(
                    "FILTER expression must be non-empty",
                ));
            }
            Self::Limit { count, .. } if *count == 0 || *count > MAX_SEARCH_RESULTS => {
                return Err(tower_mcp::Error::tool(format!(
                    "pipeline LIMIT count must be between 1 and {MAX_SEARCH_RESULTS}"
                )));
            }
            _ => {}
        }
        Ok(())
    }

    fn append(&self, command: &mut crate::RedisCommand) {
        match self {
            Self::GroupBy {
                properties,
                reducers,
            } => {
                command.arg("GROUPBY").arg(properties.len().to_string());
                command.args(properties.iter().map(String::as_str));
                for reducer in reducers {
                    command
                        .arg("REDUCE")
                        .arg(reducer.function.as_str())
                        .arg(reducer.arguments.len().to_string())
                        .args(reducer.arguments.iter().map(String::as_str));
                    if let Some(alias) = &reducer.alias {
                        command.arg("AS").arg(alias.as_str());
                    }
                }
            }
            Self::SortBy { fields, max } => {
                command.arg("SORTBY").arg((fields.len() * 2).to_string());
                for field in fields {
                    command
                        .arg(field.property.as_str())
                        .arg(field.order.as_str());
                }
                if let Some(max) = max {
                    command.arg("MAX").arg(max.to_string());
                }
            }
            Self::Apply { expression, alias } => {
                command
                    .arg("APPLY")
                    .arg(expression.as_str())
                    .arg("AS")
                    .arg(alias.as_str());
            }
            Self::Filter { expression } => {
                command.arg("FILTER").arg(expression.as_str());
            }
            Self::Limit { offset, count } => {
                command
                    .arg("LIMIT")
                    .arg(offset.to_string())
                    .arg(count.to_string());
            }
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AggregateCursorOptions {
    /// Rows requested per FT.CURSOR page.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    count: Option<u64>,
    /// Cursor idle expiry in milliseconds.
    #[serde(default)]
    max_idle_ms: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtAggregateInput {
    index: String,
    query: String,
    /// Load these document properties before processing the pipeline.
    #[serde(default)]
    #[schemars(length(max = 100))]
    load_fields: Vec<String>,
    /// Load every document property. Mutually exclusive with load_fields.
    #[serde(default)]
    load_all: bool,
    #[serde(default)]
    verbatim: bool,
    /// Server-side execution timeout in milliseconds.
    #[serde(default)]
    timeout_ms: Option<u64>,
    /// Ordered aggregate stages.
    #[serde(default)]
    #[schemars(length(max = 100))]
    stages: Vec<AggregateStage>,
    /// Final result offset; a final LIMIT is always emitted.
    #[serde(default)]
    limit_offset: Option<u64>,
    /// Final result count, bounded to 100.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    limit_num: Option<u64>,
    #[serde(default)]
    #[schemars(length(max = 100))]
    params: Vec<SearchParameter>,
    #[serde(default)]
    dialect: Option<u64>,
    /// Create a server cursor and return its continuation identifier.
    #[serde(default)]
    cursor: Option<AggregateCursorOptions>,
}

impl FtAggregateInput {
    fn validate(&self) -> tower_mcp::Result<u64> {
        if self.load_all && !self.load_fields.is_empty() {
            return Err(tower_mcp::Error::tool(
                "load_all and load_fields are mutually exclusive",
            ));
        }
        if self.stages.len() > MAX_AGGREGATE_STAGES {
            return Err(tower_mcp::Error::tool(format!(
                "stages must contain at most {MAX_AGGREGATE_STAGES} items"
            )));
        }
        for stage in &self.stages {
            stage.validate()?;
        }
        if self.timeout_ms == Some(0) {
            return Err(tower_mcp::Error::tool(
                "timeout_ms must be greater than zero",
            ));
        }
        if self.params.len() > MAX_SEARCH_PARAMETERS {
            return Err(tower_mcp::Error::tool(format!(
                "params must contain at most {MAX_SEARCH_PARAMETERS} items"
            )));
        }
        let mut names = BTreeSet::new();
        if self
            .params
            .iter()
            .any(|parameter| parameter.name.is_empty() || !names.insert(&parameter.name))
        {
            return Err(tower_mcp::Error::tool(
                "parameter names must be non-empty and unique",
            ));
        }
        if !self.params.is_empty() && self.dialect.is_none_or(|dialect| dialect < 2) {
            return Err(tower_mcp::Error::tool("params require dialect 2 or newer"));
        }
        if self.dialect == Some(0) {
            return Err(tower_mcp::Error::tool("dialect must be greater than zero"));
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.count == Some(0) || cursor.max_idle_ms == Some(0))
        {
            return Err(tower_mcp::Error::tool(
                "cursor count and max_idle_ms must be greater than zero",
            ));
        }
        let limit = self.limit_num.unwrap_or(10);
        if limit == 0 || limit > MAX_SEARCH_RESULTS {
            return Err(tower_mcp::Error::tool(format!(
                "limit_num must be between 1 and {MAX_SEARCH_RESULTS}"
            )));
        }
        Ok(limit)
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AggregateRow {
    fields: Vec<SearchFieldValue>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtAggregateOutput {
    index: String,
    query: String,
    total: u64,
    count: usize,
    rows: Vec<AggregateRow>,
    cursor_id: u64,
    page: PageMetadata,
}

fn aggregate_payload(
    value: RedisValue,
    context: &str,
) -> tower_mcp::Result<(u64, Vec<AggregateRow>)> {
    match value {
        RedisValue::Array(mut values) => {
            if values.is_empty() {
                return Err(tower_mcp::Error::tool(format!(
                    "{context} omitted its row count"
                )));
            }
            let total = unsigned_integer(values.remove(0), context)?;
            let rows = values
                .into_iter()
                .map(|value| {
                    Ok(AggregateRow {
                        fields: search_field_pairs(value, context)?,
                    })
                })
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            Ok((total, rows))
        }
        RedisValue::Map(mut pairs) => {
            let total = unsigned_integer(
                take_search_map_value(&mut pairs, "total_results").ok_or_else(|| {
                    tower_mcp::Error::tool(format!(
                        "{context} omitted its RESP3 total_results field"
                    ))
                })?,
                context,
            )?;
            let results = match take_search_map_value(&mut pairs, "results") {
                Some(RedisValue::Array(results)) => results,
                None if total == 0 => Vec::new(),
                Some(other) => {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} returned invalid RESP3 results: {other:?}"
                    )));
                }
                None => {
                    return Err(tower_mcp::Error::tool(format!(
                        "{context} omitted its RESP3 results field"
                    )));
                }
            };
            let rows = results
                .into_iter()
                .map(|result| {
                    let mut result = match result {
                        RedisValue::Map(result) => result,
                        RedisValue::Attribute { data, .. } => match *data {
                            RedisValue::Map(result) => result,
                            other => {
                                return Err(tower_mcp::Error::tool(format!(
                                    "{context} returned an invalid attributed row: {other:?}"
                                )));
                            }
                        },
                        other => {
                            return Err(tower_mcp::Error::tool(format!(
                                "{context} returned an invalid RESP3 row: {other:?}"
                            )));
                        }
                    };
                    Ok(AggregateRow {
                        fields: search_field_pairs(
                            take_search_map_value(&mut result, "extra_attributes")
                                .unwrap_or(RedisValue::Nil),
                            context,
                        )?,
                    })
                })
                .collect::<tower_mcp::Result<Vec<_>>>()?;
            Ok((total, rows))
        }
        RedisValue::Attribute { data, .. } => aggregate_payload(*data, context),
        other => Err(tower_mcp::Error::tool(format!(
            "{context} returned an unsupported result page: {other:?}"
        ))),
    }
}

fn aggregate_response(
    value: RedisValue,
    with_cursor: bool,
    context: &str,
) -> tower_mcp::Result<(u64, Vec<AggregateRow>, u64)> {
    if !with_cursor {
        let (total, rows) = aggregate_payload(value, context)?;
        return Ok((total, rows, 0));
    }
    let RedisValue::Array(mut values) = value else {
        return Err(tower_mcp::Error::tool(format!(
            "{context} returned an invalid cursor response"
        )));
    };
    if values.len() != 2 {
        return Err(tower_mcp::Error::tool(format!(
            "{context} cursor response must contain a page and cursor id"
        )));
    }
    let cursor = unsigned_integer(values.pop().expect("cursor id"), context)?;
    let (total, rows) = aggregate_payload(values.pop().expect("cursor page"), context)?;
    Ok((total, rows, cursor))
}

fn ft_aggregate_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_aggregate")
        .title("Aggregate Redis Search Results")
        .description(
            "Run a typed, bounded FT.AGGREGATE pipeline. GROUPBY reducers, sorting, expressions, parameters, and cursor continuation are explicit schema fields.",
        )
        .output_schema(output_schema::<FtAggregateOutput>())
        .annotations(stateful_read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtAggregateInput>| async move {
                let limit_num = input.validate()?;
                if input.dialect.is_some() {
                    require_search_version(
                        &state,
                        crate::RedisVersion::new(2, 4, 3),
                        "FT.AGGREGATE DIALECT",
                    )?;
                }
                state.validate_requested_entries(limit_num as usize, "limit_num")?;
                let mut command = module_command(
                    "redis_ft_aggregate",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.AGGREGATE",
                );
                command.arg(input.index.as_str()).arg(input.query.as_str());
                if input.verbatim {
                    command.arg("VERBATIM");
                }
                if input.load_all {
                    command.arg("LOAD").arg("*");
                } else if !input.load_fields.is_empty() {
                    command
                        .arg("LOAD")
                        .arg(input.load_fields.len().to_string())
                        .args(input.load_fields.iter().map(String::as_str));
                }
                if let Some(timeout) = input.timeout_ms {
                    command.arg("TIMEOUT").arg(timeout.to_string());
                }
                for stage in &input.stages {
                    stage.append(&mut command);
                }
                let limit_offset = input.limit_offset.unwrap_or(0);
                command
                    .arg("LIMIT")
                    .arg(limit_offset.to_string())
                    .arg(limit_num.to_string());
                if !input.params.is_empty() {
                    command
                        .arg("PARAMS")
                        .arg((input.params.len() * 2).to_string());
                    for parameter in &input.params {
                        command
                            .arg(parameter.name.as_str())
                            .arg(parameter.value.as_str());
                    }
                }
                if let Some(dialect) = input.dialect {
                    command.arg("DIALECT").arg(dialect.to_string());
                }
                if let Some(cursor) = &input.cursor {
                    command.arg("WITHCURSOR");
                    command
                        .arg("COUNT")
                        .arg(cursor.count.unwrap_or(limit_num).to_string());
                    if let Some(max_idle_ms) = cursor.max_idle_ms {
                        command.arg("MAXIDLE").arg(max_idle_ms.to_string());
                    }
                }
                let value = state.raw(command, "FT.AGGREGATE failed").await?;
                let (total, rows, cursor_id) =
                    aggregate_response(value, input.cursor.is_some(), "FT.AGGREGATE")?;
                let count = rows.len();
                let output = FtAggregateOutput {
                    index: input.index,
                    query: input.query,
                    total,
                    count,
                    rows,
                    cursor_id,
                    page: PageMetadata::cursor(limit_num as usize, count, cursor_id),
                };
                state.output_collection(
                    &output,
                    count,
                    "Retry FT.AGGREGATE with a smaller limit_num or continue the returned cursor.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCursorReadInput {
    index: String,
    cursor_id: u64,
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    count: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCursorReadOutput {
    index: String,
    total: u64,
    count: usize,
    rows: Vec<AggregateRow>,
    cursor_id: u64,
    page: PageMetadata,
}

fn ft_cursor_read_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_cursor_read")
        .title("Continue Redis Search Aggregation Cursor")
        .description("Read one bounded page from an FT.AGGREGATE cursor.")
        .output_schema(output_schema::<FtCursorReadOutput>())
        .annotations(stateful_read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtCursorReadInput>| async move {
                if input.cursor_id == 0 {
                    return Err(tower_mcp::Error::tool(
                        "cursor_id must be greater than zero",
                    ));
                }
                let count = input.count.unwrap_or(10);
                state.validate_requested_entries(count as usize, "count")?;
                let mut command = module_command(
                    "redis_ft_cursor_read",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.CURSOR",
                );
                command
                    .arg("READ")
                    .arg(input.index.as_str())
                    .arg(input.cursor_id.to_string())
                    .arg("COUNT")
                    .arg(count.to_string());
                let value = state.raw(command, "FT.CURSOR READ failed").await?;
                let (total, rows, cursor_id) = aggregate_response(value, true, "FT.CURSOR READ")?;
                let returned = rows.len();
                let output = FtCursorReadOutput {
                    index: input.index,
                    total,
                    count: returned,
                    rows,
                    cursor_id,
                    page: PageMetadata::cursor(count as usize, returned, cursor_id),
                };
                state.output_collection(
                    &output,
                    returned,
                    "Retry FT.CURSOR READ with a smaller count.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCursorDelInput {
    index: String,
    cursor_id: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtCursorDelOutput {
    index: String,
    cursor_id: u64,
    deleted: bool,
}

fn ft_cursor_del_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_cursor_del")
        .title("Delete Redis Search Aggregation Cursor")
        .description("Release an FT.AGGREGATE cursor before its idle expiry.")
        .output_schema(output_schema::<FtCursorDelOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtCursorDelInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_ft_cursor_del")?;
                if input.cursor_id == 0 {
                    return Err(tower_mcp::Error::tool(
                        "cursor_id must be greater than zero",
                    ));
                }
                let mut command = module_command(
                    "redis_ft_cursor_del",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "FT.CURSOR",
                );
                command
                    .arg("DEL")
                    .arg(input.index.as_str())
                    .arg(input.cursor_id.to_string());
                let _: String = state.query(command, "FT.CURSOR DEL failed").await?;
                state.output(&FtCursorDelOutput {
                    index: input.index,
                    cursor_id: input.cursor_id,
                    deleted: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtExplainInput {
    index: String,
    query: String,
    #[serde(default)]
    dialect: Option<u64>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtExplainOutput {
    index: String,
    query: String,
    dialect: Option<u64>,
    plan: String,
}

fn ft_explain_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_explain")
        .title("Explain Redis Search Query")
        .description("Return the Query Engine execution plan for a search query.")
        .output_schema(output_schema::<FtExplainOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtExplainInput>| async move {
                if input.dialect == Some(0) {
                    return Err(tower_mcp::Error::tool("dialect must be greater than zero"));
                }
                if input.dialect.is_some() {
                    require_search_version(
                        &state,
                        crate::RedisVersion::new(2, 4, 3),
                        "FT.EXPLAIN DIALECT",
                    )?;
                }
                let mut command = module_command(
                    "redis_ft_explain",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.EXPLAIN",
                );
                command.arg(input.index.as_str()).arg(input.query.as_str());
                if let Some(dialect) = input.dialect {
                    command.arg("DIALECT").arg(dialect.to_string());
                }
                let plan: String = state.query(command, "FT.EXPLAIN failed").await?;
                state.output(&FtExplainOutput {
                    index: input.index,
                    query: input.query,
                    dialect: input.dialect,
                    plan,
                })
            },
        )
        .build()
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum ProfileCommand {
    Search,
    Aggregate,
}

impl ProfileCommand {
    fn as_str(self) -> &'static str {
        match self {
            Self::Search => "SEARCH",
            Self::Aggregate => "AGGREGATE",
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtProfileInput {
    index: String,
    command: ProfileCommand,
    query: String,
    /// Limit profile detail as well as query results.
    #[serde(default = "default_true")]
    limited: bool,
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    limit_num: Option<u64>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtProfileOutput {
    index: String,
    command: ProfileCommand,
    query: String,
    limited: bool,
    results: JsonValue,
    profile: JsonValue,
}

fn profile_sections(value: RedisValue) -> tower_mcp::Result<(JsonValue, JsonValue)> {
    match value {
        RedisValue::Array(mut parts) => {
            if parts.len() != 2 {
                return Err(tower_mcp::Error::tool(
                    "FT.PROFILE response must contain results and profile sections",
                ));
            }
            let profile = redis_value_to_json(&parts.pop().expect("profile section"));
            let results = redis_value_to_json(&parts.pop().expect("result section"));
            Ok((results, profile))
        }
        RedisValue::Map(mut parts) => {
            let profile = take_search_map_value(&mut parts, "profile").ok_or_else(|| {
                tower_mcp::Error::tool("FT.PROFILE RESP3 response omitted its profile field")
            })?;
            Ok((
                redis_value_to_json(&RedisValue::Map(parts)),
                redis_value_to_json(&profile),
            ))
        }
        RedisValue::Attribute { data, .. } => profile_sections(*data),
        other => Err(tower_mcp::Error::tool(format!(
            "FT.PROFILE returned an unsupported response: {other:?}"
        ))),
    }
}

fn ft_profile_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_profile")
        .title("Profile Redis Search Query")
        .description(
            "Profile a bounded SEARCH or AGGREGATE query and return separate result and execution-profile sections.",
        )
        .output_schema(output_schema::<FtProfileOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtProfileInput>| async move {
                let limit = input.limit_num.unwrap_or(10);
                state.validate_requested_entries(limit as usize, "limit_num")?;
                let mut command = module_command(
                    "redis_ft_profile",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.PROFILE",
                );
                command.arg(input.index.as_str()).arg(input.command.as_str());
                if input.limited {
                    command.arg("LIMITED");
                }
                command.arg("QUERY").arg(input.query.as_str());
                if matches!(input.command, ProfileCommand::Search) {
                    command.arg("NOCONTENT");
                }
                command.arg("LIMIT").arg("0").arg(limit.to_string());
                let value = state.raw(command, "FT.PROFILE failed").await?;
                let (results, profile) = profile_sections(value)?;
                state.output_collection(
                    &FtProfileOutput {
                        index: input.index,
                        command: input.command,
                        query: input.query,
                        limited: input.limited,
                        results,
                        profile,
                    },
                    limit as usize,
                    "Retry FT.PROFILE with a smaller limit_num and limited=true.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtTagvalsInput {
    index: String,
    field: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtTagvalsOutput {
    index: String,
    field: String,
    deprecated: bool,
    count: usize,
    values: Vec<EncodedSearchValue>,
}

fn ft_tagvals_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_tagvals")
        .title("List Redis Search Tag Values")
        .description(
            "Return all distinct indexed TAG values. FT.TAGVALS is deprecated, potentially expensive, and whole-result budget guarded.",
        )
        .output_schema(output_schema::<FtTagvalsOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtTagvalsInput>| async move {
                let mut command = module_command(
                    "redis_ft_tagvals",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.TAGVALS",
                );
                command.arg(input.index.as_str()).arg(input.field.as_str());
                let mut values = text_collection(state.raw(command, "FT.TAGVALS failed").await?, "FT.TAGVALS")?;
                values.sort_by(|left, right| left.value.cmp(&right.value));
                let output = FtTagvalsOutput {
                    index: input.index,
                    field: input.field,
                    deprecated: true,
                    count: values.len(),
                    values,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Use a larger configured entry budget or query known tag values directly; FT.TAGVALS has no cursor form.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DictionaryInput {
    dict: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DictionaryTermsInput {
    dict: String,
    #[schemars(length(min = 1, max = 100))]
    terms: Vec<String>,
}

impl DictionaryTermsInput {
    fn validate(&self) -> tower_mcp::Result<()> {
        if self.terms.is_empty() || self.terms.len() > MAX_SCHEMA_FIELDS {
            Err(tower_mcp::Error::tool(format!(
                "terms must contain between 1 and {MAX_SCHEMA_FIELDS} items"
            )))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DictionaryMutationOutput {
    dict: String,
    requested: usize,
    changed: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DictionaryDumpOutput {
    dict: String,
    count: usize,
    terms: Vec<EncodedSearchValue>,
}

fn ft_dictdump_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_dictdump")
        .title("Dump Redis Search Dictionary")
        .description("Return every term in a Query Engine dictionary with explicit value encoding.")
        .output_schema(output_schema::<DictionaryDumpOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DictionaryInput>| async move {
                let mut command = module_command(
                    "redis_ft_dictdump",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.DICTDUMP",
                );
                command.arg(input.dict.as_str());
                let mut terms = text_collection(
                    state.raw(command, "FT.DICTDUMP failed").await?,
                    "FT.DICTDUMP",
                )?;
                terms.sort_by(|left, right| left.value.cmp(&right.value));
                let output = DictionaryDumpOutput {
                    dict: input.dict,
                    count: terms.len(),
                    terms,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Use a larger configured entry budget; FT.DICTDUMP has no cursor form.",
                )
            },
        )
        .build()
}

fn ft_dictadd_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_dictadd")
        .title("Add Redis Search Dictionary Terms")
        .description("Add one or more terms to a Query Engine dictionary.")
        .output_schema(output_schema::<DictionaryMutationOutput>())
        .annotations(write_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DictionaryTermsInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_ft_dictadd")?;
                input.validate()?;
                let mut command = module_command(
                    "redis_ft_dictadd",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "FT.DICTADD",
                );
                command.arg(input.dict.as_str()).args(input.terms.iter().map(String::as_str));
                let changed = unsigned_integer(state.raw(command, "FT.DICTADD failed").await?, "FT.DICTADD")?;
                state.output(&DictionaryMutationOutput {
                    dict: input.dict,
                    requested: input.terms.len(),
                    changed,
                })
            },
        )
        .build()
}

fn ft_dictdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_dictdel")
        .title("Delete Redis Search Dictionary Terms")
        .description("Remove one or more terms from a Query Engine dictionary; requires full access.")
        .output_schema(output_schema::<DictionaryMutationOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<DictionaryTermsInput>| async move {
                state.require(AccessMode::Full, "redis_ft_dictdel")?;
                input.validate()?;
                let mut command = module_command(
                    "redis_ft_dictdel",
                    AccessMode::Full,
                    RedisModule::Search,
                    "FT.DICTDEL",
                );
                command.arg(input.dict.as_str()).args(input.terms.iter().map(String::as_str));
                let changed = unsigned_integer(state.raw(command, "FT.DICTDEL failed").await?, "FT.DICTDEL")?;
                state.output(&DictionaryMutationOutput {
                    dict: input.dict,
                    requested: input.terms.len(),
                    changed,
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtSynupdateInput {
    index: String,
    group_id: String,
    #[serde(default)]
    skip_initial_scan: bool,
    #[schemars(length(min = 1, max = 100))]
    terms: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtSynupdateOutput {
    index: String,
    group_id: String,
    terms: usize,
    skip_initial_scan: bool,
    updated: bool,
}

fn ft_synupdate_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_synupdate")
        .title("Update Redis Search Synonym Group")
        .description("Create or extend an index synonym group with bounded terms.")
        .output_schema(output_schema::<FtSynupdateOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtSynupdateInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_ft_synupdate")?;
                if input.terms.is_empty() || input.terms.len() > MAX_SCHEMA_FIELDS {
                    return Err(tower_mcp::Error::tool(format!(
                        "terms must contain between 1 and {MAX_SCHEMA_FIELDS} items"
                    )));
                }
                let mut command = module_command(
                    "redis_ft_synupdate",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "FT.SYNUPDATE",
                );
                command
                    .arg(input.index.as_str())
                    .arg(input.group_id.as_str());
                if input.skip_initial_scan {
                    command.arg("SKIPINITIALSCAN");
                }
                command.args(input.terms.iter().map(String::as_str));
                let _: String = state.query(command, "FT.SYNUPDATE failed").await?;
                state.output(&FtSynupdateOutput {
                    index: input.index,
                    group_id: input.group_id,
                    terms: input.terms.len(),
                    skip_initial_scan: input.skip_initial_scan,
                    updated: true,
                })
            },
        )
        .build()
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SynonymEntry {
    term: EncodedSearchValue,
    group_ids: Vec<EncodedSearchValue>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtSyndumpOutput {
    index: String,
    count: usize,
    entries: Vec<SynonymEntry>,
}

fn synonym_entries(value: RedisValue) -> tower_mcp::Result<Vec<SynonymEntry>> {
    let pairs = match value {
        RedisValue::Map(pairs) => pairs,
        RedisValue::Array(values) => {
            if !values.len().is_multiple_of(2) {
                return Err(tower_mcp::Error::tool(
                    "FT.SYNDUMP returned an odd number of elements",
                ));
            }
            values
                .chunks_exact(2)
                .map(|pair| (pair[0].clone(), pair[1].clone()))
                .collect()
        }
        RedisValue::Nil => Vec::new(),
        other => {
            return Err(tower_mcp::Error::tool(format!(
                "FT.SYNDUMP returned an unexpected response: {other:?}"
            )));
        }
    };
    let mut entries = pairs
        .into_iter()
        .map(|(term, groups)| {
            Ok(SynonymEntry {
                term: encoded_value(term, "FT.SYNDUMP term")?,
                group_ids: text_collection(groups, "FT.SYNDUMP group ids")?,
            })
        })
        .collect::<tower_mcp::Result<Vec<_>>>()?;
    entries.sort_by(|left, right| left.term.value.cmp(&right.term.value));
    Ok(entries)
}

fn ft_syndump_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_syndump")
        .title("Dump Redis Search Synonyms")
        .description("Return structured term-to-group synonym mappings for an index.")
        .output_schema(output_schema::<FtSyndumpOutput>())
        .annotations(read_annotations())
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<IndexInput>| async move {
                let mut command = module_command(
                    "redis_ft_syndump",
                    AccessMode::ReadOnly,
                    RedisModule::Search,
                    "FT.SYNDUMP",
                );
                command.arg(input.index.as_str());
                let entries = synonym_entries(state.raw(command, "FT.SYNDUMP failed").await?)?;
                let output = FtSyndumpOutput {
                    index: input.index,
                    count: entries.len(),
                    entries,
                };
                state.output_collection(
                    &output,
                    output.count,
                    "Use a larger configured entry budget; FT.SYNDUMP has no cursor form.",
                )
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AliasTargetInput {
    alias: String,
    index: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AliasInput {
    alias: String,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AliasOutput {
    alias: String,
    index: Option<String>,
    action: String,
}

fn alias_target_tool(
    state: Arc<ToolState>,
    tool_name: &'static str,
    redis_command: &'static str,
    title: &'static str,
    description: &'static str,
    access: AccessMode,
) -> Tool {
    let annotations = if access == AccessMode::Full {
        destructive_annotations(false)
    } else {
        write_annotations(false)
    };
    ToolBuilder::new(tool_name)
        .title(title)
        .description(description)
        .output_schema(output_schema::<AliasOutput>())
        .annotations(annotations)
        .extractor_handler(
            state,
            move |State(state): State<Arc<ToolState>>, Json(input): Json<AliasTargetInput>| async move {
                state.require(access, tool_name)?;
                let mut command = module_command(tool_name, access, RedisModule::Search, redis_command);
                command.arg(input.alias.as_str()).arg(input.index.as_str());
                let _: String = state.query(command, &format!("{redis_command} failed")).await?;
                state.output(&AliasOutput {
                    alias: input.alias,
                    index: Some(input.index),
                    action: if redis_command == "FT.ALIASADD" { "added" } else { "updated" }.into(),
                })
            },
        )
        .build()
}

fn ft_aliasdel_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_aliasdel")
        .title("Delete Redis Search Index Alias")
        .description("Delete an index alias; requires full access.")
        .output_schema(output_schema::<AliasOutput>())
        .annotations(destructive_annotations(true))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<AliasInput>| async move {
                state.require(AccessMode::Full, "redis_ft_aliasdel")?;
                let mut command = module_command(
                    "redis_ft_aliasdel",
                    AccessMode::Full,
                    RedisModule::Search,
                    "FT.ALIASDEL",
                );
                command.arg(input.alias.as_str());
                let _: String = state.query(command, "FT.ALIASDEL failed").await?;
                state.output(&AliasOutput {
                    alias: input.alias,
                    index: None,
                    action: "deleted".into(),
                })
            },
        )
        .build()
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtAlterInput {
    index: String,
    #[serde(default)]
    skip_initial_scan: bool,
    field: SearchField,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct FtAlterOutput {
    index: String,
    field: String,
    field_type: String,
    skip_initial_scan: bool,
    added: bool,
}

fn ft_alter_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_ft_alter")
        .title("Alter Redis Search Index")
        .description("Add one typed field to an existing Query Engine index.")
        .output_schema(output_schema::<FtAlterOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<FtAlterInput>| async move {
                state.require(AccessMode::ReadWrite, "redis_ft_alter")?;
                let field_type = input.field.normalized_type()?;
                if input.field.vector.is_some() {
                    require_search_version(
                        &state,
                        crate::RedisVersion::new(2, 4, 0),
                        "VECTOR index fields",
                    )?;
                }
                let mut command = module_command(
                    "redis_ft_alter",
                    AccessMode::ReadWrite,
                    RedisModule::Search,
                    "FT.ALTER",
                );
                command.arg(input.index.as_str());
                if input.skip_initial_scan {
                    command.arg("SKIPINITIALSCAN");
                }
                command.arg("SCHEMA").arg("ADD");
                append_search_field(&mut command, &input.field)?;
                let _: String = state.query(command, "FT.ALTER failed").await?;
                state.output(&FtAlterOutput {
                    index: input.index,
                    field: input.field.name,
                    field_type,
                    skip_initial_scan: input.skip_initial_scan,
                    added: true,
                })
            },
        )
        .build()
}

pub(super) fn add_read_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ft_aggregate_tool(state.clone()));
    router = router.tool(ft_cursor_read_tool(state.clone()));
    router = router.tool(ft_explain_tool(state.clone()));
    router = router.tool(ft_profile_tool(state.clone()));
    router = router.tool(ft_tagvals_tool(state.clone()));
    router = router.tool(ft_dictdump_tool(state.clone()));
    router.tool(ft_syndump_tool(state))
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(ft_cursor_del_tool(state.clone()));
    router = router.tool(ft_alter_tool(state.clone()));
    router = router.tool(ft_synupdate_tool(state.clone()));
    router = router.tool(ft_dictadd_tool(state.clone()));
    router.tool(alias_target_tool(
        state,
        "redis_ft_aliasadd",
        "FT.ALIASADD",
        "Add Redis Search Index Alias",
        "Create a new alias for a Query Engine index.",
        AccessMode::ReadWrite,
    ))
}

pub(super) fn add_destructive_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(alias_target_tool(
        state.clone(),
        "redis_ft_aliasupdate",
        "FT.ALIASUPDATE",
        "Update Redis Search Index Alias",
        "Repoint an existing index alias; requires full access because the old target is replaced.",
        AccessMode::Full,
    ));
    router = router.tool(ft_aliasdel_tool(state.clone()));
    router.tool(ft_dictdel_tool(state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resp3_aggregate_and_profile_envelopes_are_normalized() {
        let aggregate = RedisValue::Map(vec![
            (
                RedisValue::SimpleString("total_results".into()),
                RedisValue::Integer(1),
            ),
            (
                RedisValue::SimpleString("results".into()),
                RedisValue::Array(vec![RedisValue::Map(vec![(
                    RedisValue::SimpleString("extra_attributes".into()),
                    RedisValue::Map(vec![(
                        RedisValue::BulkString(b"count".to_vec()),
                        RedisValue::BulkString(b"1".to_vec()),
                    )]),
                )])]),
            ),
        ]);
        let (total, rows) = aggregate_payload(aggregate, "FT.AGGREGATE").unwrap();
        assert_eq!(total, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields[0].name, "count");

        let profile = RedisValue::Map(vec![
            (
                RedisValue::SimpleString("results".into()),
                RedisValue::Array(Vec::new()),
            ),
            (
                RedisValue::SimpleString("profile".into()),
                RedisValue::Map(vec![(
                    RedisValue::SimpleString("Total profile time".into()),
                    RedisValue::Double(0.1),
                )]),
            ),
        ]);
        let (results, profile) = profile_sections(profile).unwrap();
        assert!(results.is_array());
        assert!(profile.is_array());
    }
}
