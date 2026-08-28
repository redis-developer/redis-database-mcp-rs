//! Bounded bulk load and deterministic seed tools.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tower_mcp::{
    McpRouter, Tool, ToolBuilder,
    extract::{Json, State},
};

use super::{
    InputEncoding, ToolState, ValueEncoding, decode_input, encode_bytes, output_schema,
    write_annotations,
};
use crate::{
    BulkErrorHandling, BulkLoadOptions, BulkLoadReport, BulkLoadRequest, BulkRecord,
    BulkRecordValue, BulkSeedField, BulkSeedRequest, BulkSeedTemplate, BulkSeedValue,
    generate_seed_records,
};

const MAX_SEED_SAMPLE_KEYS: usize = 10;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkBytesInput {
    /// UTF-8 text or standard base64, according to `encoding`.
    value: String,
    /// Encoding of `value`.
    #[serde(default)]
    encoding: InputEncoding,
}

impl BulkBytesInput {
    fn decode(&self, name: &str) -> tower_mcp::Result<Vec<u8>> {
        decode_input(&self.value, self.encoding, name)
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkHashFieldInput {
    name: BulkBytesInput,
    value: BulkBytesInput,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkScoredMemberInput {
    member: BulkBytesInput,
    score: f64,
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkVectorElementInput {
    element: BulkBytesInput,
    #[schemars(length(min = 1, max = 16384))]
    vector: Vec<f64>,
}

/// One structured record value. Exactly one variant is present.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BulkValueInput {
    /// SET, with any expiration applied atomically in the same command.
    String { value: BulkBytesInput },
    /// HSET field/value pairs merged into the hash.
    Hash {
        #[schemars(length(min = 1, max = 128))]
        fields: Vec<BulkHashFieldInput>,
    },
    /// RPUSH elements appended in order.
    List {
        #[schemars(length(min = 1, max = 128))]
        elements: Vec<BulkBytesInput>,
    },
    /// SADD members merged into the set.
    Set {
        #[schemars(length(min = 1, max = 128))]
        members: Vec<BulkBytesInput>,
    },
    /// ZADD member/score pairs merged into the sorted set.
    SortedSet {
        #[schemars(length(min = 1, max = 128))]
        members: Vec<BulkScoredMemberInput>,
    },
    /// JSON.SET $, replacing the whole document. Requires RedisJSON.
    Json { value: JsonValue },
    /// One VADD per element merged into the vector set. Requires Redis 8.
    VectorSet {
        #[schemars(length(min = 1, max = 128))]
        elements: Vec<BulkVectorElementInput>,
    },
}

impl BulkValueInput {
    fn decode(&self, index: usize) -> tower_mcp::Result<BulkRecordValue> {
        let context = |part: &str| format!("records[{index}].{part}");
        Ok(match self {
            Self::String { value } => BulkRecordValue::String {
                value: value.decode(&context("string.value"))?,
            },
            Self::Hash { fields } => BulkRecordValue::Hash {
                fields: fields
                    .iter()
                    .map(|field| {
                        Ok((
                            field.name.decode(&context("hash.fields.name"))?,
                            field.value.decode(&context("hash.fields.value"))?,
                        ))
                    })
                    .collect::<tower_mcp::Result<_>>()?,
            },
            Self::List { elements } => BulkRecordValue::List {
                elements: elements
                    .iter()
                    .map(|element| element.decode(&context("list.elements")))
                    .collect::<tower_mcp::Result<_>>()?,
            },
            Self::Set { members } => BulkRecordValue::Set {
                members: members
                    .iter()
                    .map(|member| member.decode(&context("set.members")))
                    .collect::<tower_mcp::Result<_>>()?,
            },
            Self::SortedSet { members } => BulkRecordValue::SortedSet {
                members: members
                    .iter()
                    .map(|member| {
                        Ok((
                            member
                                .member
                                .decode(&context("sorted_set.members.member"))?,
                            member.score,
                        ))
                    })
                    .collect::<tower_mcp::Result<_>>()?,
            },
            Self::Json { value } => BulkRecordValue::Json {
                value: value.clone(),
            },
            Self::VectorSet { elements } => BulkRecordValue::VectorSet {
                elements: elements
                    .iter()
                    .map(|element| {
                        Ok((
                            element
                                .element
                                .decode(&context("vector_set.elements.element"))?,
                            element.vector.clone(),
                        ))
                    })
                    .collect::<tower_mcp::Result<_>>()?,
            },
        })
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkRecordInput {
    /// Redis key receiving this record.
    key: BulkBytesInput,
    /// Structured value loaded into the key.
    value: BulkValueInput,
    /// Expire the key this many milliseconds after loading. Non-string types
    /// issue a separate PEXPIRE that is not atomic with the data write.
    #[serde(default)]
    expire_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum BulkErrorHandlingInput {
    /// Finish the current batch, then skip every remaining record.
    #[default]
    Stop,
    /// Keep loading the remaining records and report every failure.
    Continue,
}

impl From<BulkErrorHandlingInput> for BulkErrorHandling {
    fn from(value: BulkErrorHandlingInput) -> Self {
        match value {
            BulkErrorHandlingInput::Stop => Self::StopOnError,
            BulkErrorHandlingInput::Continue => Self::ContinueOnError,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkLoadInput {
    /// Records loaded in order across sequential bounded batches.
    #[schemars(length(min = 1, max = 1000))]
    records: Vec<BulkRecordInput>,
    /// Records per sequential batch.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    batch_size: Option<usize>,
    /// Concurrent commands in flight inside one batch.
    #[serde(default)]
    #[schemars(range(min = 1, max = 8))]
    concurrency: Option<usize>,
    /// Reaction to a failed record.
    #[serde(default)]
    on_error: BulkErrorHandlingInput,
    /// Validate and plan every record without writing anything to Redis.
    #[serde(default)]
    dry_run: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkKeyOutput {
    value: String,
    encoding: ValueEncoding,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkFailureOutput {
    /// Zero-based record position, absent for workflow-level failures such as
    /// the deadline elapsing.
    index: Option<usize>,
    key: Option<BulkKeyOutput>,
    code: Option<String>,
    message: String,
    /// True when earlier commands of this record already applied, so the key
    /// may hold partial data.
    partially_applied: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkBatchOutput {
    batch: usize,
    records: usize,
    applied: usize,
    failed: usize,
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkReportOutput {
    requested: usize,
    attempted: usize,
    applied: usize,
    failed: usize,
    skipped: usize,
    total_commands: usize,
    dry_run: bool,
    complete: bool,
    deadline_exceeded: bool,
    duration_ms: u64,
    batches: Vec<BulkBatchOutput>,
    /// Bounded representative failures in record order.
    failures: Vec<BulkFailureOutput>,
    failures_truncated: bool,
    /// First generated keys, for seed previews.
    sample_keys: Option<Vec<String>>,
}

fn render_report(report: BulkLoadReport, sample_keys: Option<Vec<String>>) -> BulkReportOutput {
    BulkReportOutput {
        requested: report.requested,
        attempted: report.attempted,
        applied: report.applied,
        failed: report.failed,
        skipped: report.skipped,
        total_commands: report.total_commands,
        dry_run: report.dry_run,
        complete: report.complete,
        deadline_exceeded: report.deadline_exceeded,
        duration_ms: u64::try_from(report.duration.as_millis()).unwrap_or(u64::MAX),
        batches: report
            .batches
            .iter()
            .map(|batch| BulkBatchOutput {
                batch: batch.batch,
                records: batch.records,
                applied: batch.applied,
                failed: batch.failed,
            })
            .collect(),
        failures: report
            .failures
            .into_iter()
            .map(|failure| {
                let index = (failure.index() != usize::MAX).then_some(failure.index());
                let key = (!failure.key().is_empty()).then(|| {
                    let (value, encoding) = encode_bytes(failure.key().to_vec());
                    BulkKeyOutput { value, encoding }
                });
                BulkFailureOutput {
                    index,
                    key,
                    code: failure.code().map(str::to_string),
                    message: failure.message().to_string(),
                    partially_applied: failure.partially_applied(),
                }
            })
            .collect(),
        failures_truncated: report.failures_truncated,
        sample_keys,
    }
}

fn load_options(
    batch_size: Option<usize>,
    concurrency: Option<usize>,
    on_error: BulkErrorHandlingInput,
    dry_run: bool,
) -> BulkLoadOptions {
    let mut options = BulkLoadOptions::default();
    if let Some(batch_size) = batch_size {
        options.batch_size = batch_size;
    }
    if let Some(concurrency) = concurrency {
        options.concurrency = concurrency;
    }
    options.error_handling = on_error.into();
    options.dry_run = dry_run;
    options
}

fn bulk_error(error: crate::RedisError) -> tower_mcp::Error {
    tower_mcp::Error::tool(format!(
        "bulk workflow failed [{:?}]: {error}",
        error.kind()
    ))
}

fn bulk_load_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bulk_load")
        .title("Bulk Load Redis Records")
        .description(
            "Load bounded structured records (strings, hashes, lists, sets, sorted sets, JSON documents, and vector sets, with optional expirations) through sequential batches of bounded concurrent commands. Every record is validated before anything executes; stop-on-error and continue-on-error are explicit; dry_run plans without writing. Multi-command records are not atomic and report partial application.",
        )
        .output_schema(output_schema::<BulkReportOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BulkLoadInput>| async move {
                let mut records = Vec::with_capacity(input.records.len());
                for (index, record) in input.records.iter().enumerate() {
                    let key = record.key.decode(&format!("records[{index}].key"))?;
                    let mut bulk_record = BulkRecord::new(key, record.value.decode(index)?);
                    if let Some(expire_ms) = record.expire_ms {
                        bulk_record = bulk_record.with_expire_ms(expire_ms);
                    }
                    records.push(bulk_record);
                }
                let request = BulkLoadRequest::new().records(records).options(load_options(
                    input.batch_size,
                    input.concurrency,
                    input.on_error,
                    input.dry_run,
                ));
                let report = state
                    .bulk_engine()
                    .load(request)
                    .await
                    .map_err(bulk_error)?;
                let output = render_report(report, None);
                let entries = output.batches.len().saturating_add(output.failures.len());
                state.output_collection(
                    &output,
                    entries,
                    "Raise batch_size so fewer batch summaries are reported.",
                )
            },
        )
        .build()
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SeedValueInput {
    /// The same value for every record.
    Constant { value: String },
    /// A deterministic lowercase alphanumeric token of this length.
    Token {
        #[schemars(range(min = 1, max = 64))]
        length: usize,
    },
    /// A deterministic integer in the inclusive range.
    Integer { minimum: i64, maximum: i64 },
    /// A deterministic float in the range.
    Float { minimum: f64, maximum: f64 },
    /// A deterministic choice from the listed values.
    Choice {
        #[schemars(length(min = 1, max = 64))]
        values: Vec<String>,
    },
    /// `start + index` for reproducible identifiers.
    Sequence {
        #[serde(default)]
        start: u64,
    },
}

impl From<&SeedValueInput> for BulkSeedValue {
    fn from(value: &SeedValueInput) -> Self {
        match value {
            SeedValueInput::Constant { value } => Self::Constant {
                value: value.clone(),
            },
            SeedValueInput::Token { length } => Self::Token { length: *length },
            SeedValueInput::Integer { minimum, maximum } => Self::Integer {
                minimum: *minimum,
                maximum: *maximum,
            },
            SeedValueInput::Float { minimum, maximum } => Self::Float {
                minimum: *minimum,
                maximum: *maximum,
            },
            SeedValueInput::Choice { values } => Self::Choice {
                values: values.clone(),
            },
            SeedValueInput::Sequence { start } => Self::Sequence { start: *start },
        }
    }
}

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SeedFieldInput {
    /// Field name.
    #[schemars(length(min = 1, max = 128))]
    name: String,
    value: SeedValueInput,
}

impl From<&SeedFieldInput> for BulkSeedField {
    fn from(field: &SeedFieldInput) -> Self {
        Self {
            name: field.name.clone(),
            value: (&field.value).into(),
        }
    }
}

/// The generated shape of every seeded record. Exactly one variant is present.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SeedTemplateInput {
    String {
        value: SeedValueInput,
    },
    Hash {
        #[schemars(length(min = 1, max = 64))]
        fields: Vec<SeedFieldInput>,
    },
    List {
        element: SeedValueInput,
        #[schemars(range(min = 1, max = 128))]
        elements: usize,
    },
    Set {
        member: SeedValueInput,
        #[schemars(range(min = 1, max = 128))]
        members: usize,
    },
    SortedSet {
        member: SeedValueInput,
        /// Must generate numeric values.
        score: SeedValueInput,
        #[schemars(range(min = 1, max = 128))]
        members: usize,
    },
    /// A flat JSON object of generated fields. Requires RedisJSON.
    Json {
        #[schemars(length(min = 1, max = 64))]
        fields: Vec<SeedFieldInput>,
    },
}

impl From<&SeedTemplateInput> for BulkSeedTemplate {
    fn from(template: &SeedTemplateInput) -> Self {
        match template {
            SeedTemplateInput::String { value } => Self::String {
                value: value.into(),
            },
            SeedTemplateInput::Hash { fields } => Self::Hash {
                fields: fields.iter().map(Into::into).collect(),
            },
            SeedTemplateInput::List { element, elements } => Self::List {
                element: element.into(),
                elements: *elements,
            },
            SeedTemplateInput::Set { member, members } => Self::Set {
                member: member.into(),
                members: *members,
            },
            SeedTemplateInput::SortedSet {
                member,
                score,
                members,
            } => Self::SortedSet {
                member: member.into(),
                score: score.into(),
                members: *members,
            },
            SeedTemplateInput::Json { fields } => Self::Json {
                fields: fields.iter().map(Into::into).collect(),
            },
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct BulkSeedInput {
    /// Explicit deterministic seed. The same seed, schema, and library
    /// version always generate the same dataset.
    seed: u64,
    /// Records generated as `{key_prefix}{index}`.
    #[schemars(range(min = 1, max = 1000))]
    count: usize,
    /// Prefix for every generated key.
    #[schemars(length(min = 1, max = 512))]
    key_prefix: String,
    /// The generated shape of every record.
    template: SeedTemplateInput,
    /// Expire every generated key this many milliseconds after loading.
    #[serde(default)]
    expire_ms: Option<u64>,
    /// Records per sequential batch.
    #[serde(default)]
    #[schemars(range(min = 1, max = 100))]
    batch_size: Option<usize>,
    /// Concurrent commands in flight inside one batch.
    #[serde(default)]
    #[schemars(range(min = 1, max = 8))]
    concurrency: Option<usize>,
    /// Reaction to a failed record.
    #[serde(default)]
    on_error: BulkErrorHandlingInput,
    /// Generate and validate the dataset without writing anything to Redis.
    #[serde(default)]
    dry_run: bool,
}

fn bulk_seed_tool(state: Arc<ToolState>) -> Tool {
    ToolBuilder::new("redis_bulk_seed")
        .title("Seed Deterministic Redis Records")
        .description(
            "Deterministically generate a bounded test dataset from an explicit seed and schema, then load it through the bounded bulk workflow. The same seed, schema, and library version always produce the same records; dry_run generates and validates without writing.",
        )
        .output_schema(output_schema::<BulkReportOutput>())
        .annotations(write_annotations(false))
        .extractor_handler(
            state,
            |State(state): State<Arc<ToolState>>, Json(input): Json<BulkSeedInput>| async move {
                let request = BulkSeedRequest {
                    seed: input.seed,
                    count: input.count,
                    key_prefix: input.key_prefix,
                    template: (&input.template).into(),
                    expire_ms: input.expire_ms,
                    options: load_options(
                        input.batch_size,
                        input.concurrency,
                        input.on_error,
                        input.dry_run,
                    ),
                };
                let sample_keys = generate_seed_records(&request)
                    .map_err(bulk_error)?
                    .iter()
                    .take(MAX_SEED_SAMPLE_KEYS)
                    .map(|record| String::from_utf8_lossy(record.key()).into_owned())
                    .collect::<Vec<_>>();
                let report = state
                    .bulk_engine()
                    .seed(request)
                    .await
                    .map_err(bulk_error)?;
                let output = render_report(report, Some(sample_keys));
                let entries = output.batches.len().saturating_add(output.failures.len());
                state.output_collection(
                    &output,
                    entries,
                    "Raise batch_size so fewer batch summaries are reported.",
                )
            },
        )
        .build()
}

pub(super) fn add_write_tools(mut router: McpRouter, state: Arc<ToolState>) -> McpRouter {
    router = router.tool(bulk_load_tool(state.clone()));
    router.tool(bulk_seed_tool(state))
}
