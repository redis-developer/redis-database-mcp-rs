//! Bounded bulk load and deterministic seed workflows.
//!
//! The bulk surface loads structured, binary-safe records through the shared
//! invocation policy with explicit batch, concurrency, byte, duration, and
//! result-summary bounds. It never bypasses [`crate::RedisExecutor`]: every
//! record becomes ordinary curated commands, so access policy, timeouts,
//! telemetry metadata, and Cluster routing apply per command. Multi-command
//! records (collections, separate expirations) are not atomic and report
//! partial application explicitly.

use std::{sync::Arc, time::Duration};

use tokio::{task::JoinSet, time::Instant};

use crate::{AccessMode, RedisCommand, RedisError, RedisErrorKind, RedisInvocationEngine};

pub(crate) const BULK_LOAD_TOOL_NAME: &str = "redis_bulk_load";
pub(crate) const BULK_SEED_TOOL_NAME: &str = "redis_bulk_seed";

/// Default maximum records accepted by one bulk request.
pub const DEFAULT_MAX_BULK_RECORDS: usize = 1_000;
/// Default records per sequential batch.
pub const DEFAULT_BULK_BATCH_SIZE: usize = 50;
/// Default maximum records per sequential batch.
pub const DEFAULT_MAX_BULK_BATCH_SIZE: usize = 100;
/// Default concurrent commands in flight inside one batch.
pub const DEFAULT_BULK_CONCURRENCY: usize = 4;
/// Default maximum concurrent commands in flight inside one batch.
pub const DEFAULT_MAX_BULK_CONCURRENCY: usize = 8;
/// Default maximum total request bytes across keys, fields, and values.
pub const DEFAULT_MAX_BULK_INPUT_BYTES: usize = 4 * 1024 * 1024;
/// Default maximum wall-clock duration of one bulk request.
pub const DEFAULT_MAX_BULK_DURATION: Duration = Duration::from_secs(60);
/// Default maximum representative failures included in one report.
pub const DEFAULT_MAX_BULK_REPORTED_FAILURES: usize = 10;
/// Default maximum per-batch summaries included in one report.
pub const DEFAULT_MAX_BULK_BATCH_SUMMARIES: usize = 200;

const MAX_BULK_KEY_BYTES: usize = 64 * 1024;
const MAX_COLLECTION_ENTRIES_PER_RECORD: usize = 128;
const MAX_VECTOR_DIMENSIONS: usize = 16_384;
const MAX_SEED_TOKEN_LENGTH: usize = 64;
const MAX_SEED_CHOICES: usize = 64;
const MAX_SEED_FIELDS: usize = 64;

fn invalid(message: impl Into<String>, code: &str) -> RedisError {
    RedisError::new(RedisErrorKind::InvalidRequest, message).with_code(code.to_string())
}

/// Bounds applied to every bulk request before it reaches Redis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBulkLimits {
    max_records: usize,
    max_batch_size: usize,
    max_concurrency: usize,
    max_input_bytes: usize,
    max_duration: Duration,
    max_reported_failures: usize,
    max_batch_summaries: usize,
}

impl Default for RedisBulkLimits {
    fn default() -> Self {
        Self {
            max_records: DEFAULT_MAX_BULK_RECORDS,
            max_batch_size: DEFAULT_MAX_BULK_BATCH_SIZE,
            max_concurrency: DEFAULT_MAX_BULK_CONCURRENCY,
            max_input_bytes: DEFAULT_MAX_BULK_INPUT_BYTES,
            max_duration: DEFAULT_MAX_BULK_DURATION,
            max_reported_failures: DEFAULT_MAX_BULK_REPORTED_FAILURES,
            max_batch_summaries: DEFAULT_MAX_BULK_BATCH_SUMMARIES,
        }
    }
}

impl RedisBulkLimits {
    pub const fn max_records(self) -> usize {
        self.max_records
    }

    pub const fn max_batch_size(self) -> usize {
        self.max_batch_size
    }

    pub const fn max_concurrency(self) -> usize {
        self.max_concurrency
    }

    pub const fn max_input_bytes(self) -> usize {
        self.max_input_bytes
    }

    pub const fn max_duration(self) -> Duration {
        self.max_duration
    }

    pub const fn max_reported_failures(self) -> usize {
        self.max_reported_failures
    }

    pub const fn max_batch_summaries(self) -> usize {
        self.max_batch_summaries
    }

    pub const fn with_max_records(mut self, value: usize) -> Self {
        self.max_records = value;
        self
    }

    pub const fn with_max_batch_size(mut self, value: usize) -> Self {
        self.max_batch_size = value;
        self
    }

    pub const fn with_max_concurrency(mut self, value: usize) -> Self {
        self.max_concurrency = value;
        self
    }

    pub const fn with_max_input_bytes(mut self, value: usize) -> Self {
        self.max_input_bytes = value;
        self
    }

    pub const fn with_max_duration(mut self, value: Duration) -> Self {
        self.max_duration = value;
        self
    }

    pub const fn with_max_reported_failures(mut self, value: usize) -> Self {
        self.max_reported_failures = value;
        self
    }

    pub const fn with_max_batch_summaries(mut self, value: usize) -> Self {
        self.max_batch_summaries = value;
        self
    }

    fn validate(self) -> Result<Self, RedisError> {
        if self.max_records == 0
            || self.max_batch_size == 0
            || self.max_concurrency == 0
            || self.max_input_bytes == 0
            || self.max_duration.is_zero()
            || self.max_reported_failures == 0
            || self.max_batch_summaries == 0
        {
            return Err(invalid(
                "bulk limits must all be greater than zero",
                "INVALID_BULK_LIMITS",
            ));
        }
        Ok(self)
    }
}

/// One binary-safe structured value loaded into one Redis key.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum BulkRecordValue {
    /// `SET`, with any expiration applied atomically in the same command.
    String { value: Vec<u8> },
    /// `HSET` field/value pairs merged into the hash.
    Hash { fields: Vec<(Vec<u8>, Vec<u8>)> },
    /// `RPUSH` elements appended in order.
    List { elements: Vec<Vec<u8>> },
    /// `SADD` members merged into the set.
    Set { members: Vec<Vec<u8>> },
    /// `ZADD` member/score pairs merged into the sorted set.
    SortedSet { members: Vec<(Vec<u8>, f64)> },
    /// `JSON.SET $`, replacing the whole document. Requires RedisJSON.
    Json { value: serde_json::Value },
    /// One `VADD VALUES` per element merged into the vector set.
    VectorSet { elements: Vec<(Vec<u8>, Vec<f64>)> },
}

/// One bounded record: a key, one structured value, and an optional
/// expiration.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkRecord {
    key: Vec<u8>,
    value: BulkRecordValue,
    expire_ms: Option<u64>,
}

impl BulkRecord {
    pub fn new(key: impl Into<Vec<u8>>, value: BulkRecordValue) -> Self {
        Self {
            key: key.into(),
            value,
            expire_ms: None,
        }
    }

    /// Expire the key this many milliseconds after loading. Strings apply
    /// the expiration atomically inside SET; every other type issues a
    /// separate PEXPIRE and is therefore not atomic with its data write.
    pub fn with_expire_ms(mut self, expire_ms: u64) -> Self {
        self.expire_ms = Some(expire_ms);
        self
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &BulkRecordValue {
        &self.value
    }

    pub fn expire_ms(&self) -> Option<u64> {
        self.expire_ms
    }
}

/// How the workflow reacts to a failed record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BulkErrorHandling {
    /// Finish the current batch, then skip every remaining record.
    #[default]
    StopOnError,
    /// Keep loading the remaining records and report every failure.
    ContinueOnError,
}

/// Per-request execution options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct BulkLoadOptions {
    pub batch_size: usize,
    pub concurrency: usize,
    pub error_handling: BulkErrorHandling,
    /// Validate and plan every record without sending anything to Redis.
    pub dry_run: bool,
}

impl Default for BulkLoadOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BULK_BATCH_SIZE,
            concurrency: DEFAULT_BULK_CONCURRENCY,
            error_handling: BulkErrorHandling::default(),
            dry_run: false,
        }
    }
}

/// One bounded bulk load request.
#[derive(Debug, Clone, Default)]
pub struct BulkLoadRequest {
    records: Vec<BulkRecord>,
    options: BulkLoadOptions,
}

impl BulkLoadRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(mut self, record: BulkRecord) -> Self {
        self.records.push(record);
        self
    }

    pub fn records(mut self, records: impl IntoIterator<Item = BulkRecord>) -> Self {
        self.records.extend(records);
        self
    }

    pub fn options(mut self, options: BulkLoadOptions) -> Self {
        self.options = options;
        self
    }

    pub fn record_list(&self) -> &[BulkRecord] {
        &self.records
    }
}

/// One record the workflow could not fully apply.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct BulkRecordFailure {
    index: usize,
    key: Vec<u8>,
    code: Option<String>,
    message: String,
    /// True when earlier commands of this multi-command record already
    /// applied, so the key may hold partial data.
    partially_applied: bool,
}

impl BulkRecordFailure {
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn partially_applied(&self) -> bool {
        self.partially_applied
    }
}

/// Applied/failed counts for one sequential batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct BulkBatchSummary {
    pub batch: usize,
    pub records: usize,
    pub applied: usize,
    pub failed: usize,
}

/// Compact machine-readable outcome of one bulk request.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct BulkLoadReport {
    pub requested: usize,
    pub attempted: usize,
    pub applied: usize,
    pub failed: usize,
    /// Records never attempted because an earlier failure or the deadline
    /// stopped the workflow.
    pub skipped: usize,
    pub total_commands: usize,
    pub dry_run: bool,
    /// False when a stop-on-error failure or the deadline ended the workflow
    /// before every record was attempted.
    pub complete: bool,
    /// True when the configured maximum duration elapsed mid-workflow. The
    /// outcome of commands in flight at the deadline is unknown; the workflow
    /// never retries them.
    pub deadline_exceeded: bool,
    pub duration: Duration,
    pub batches: Vec<BulkBatchSummary>,
    /// Bounded representative failures in record order.
    pub failures: Vec<BulkRecordFailure>,
    /// True when more failures occurred than the report retains.
    pub failures_truncated: bool,
}

struct RecordPlan {
    index: usize,
    key: Vec<u8>,
    commands: Vec<RedisCommand>,
}

/// (record index, key, per-record outcome with a partial-application flag).
type RecordOutcome = (usize, Vec<u8>, Result<(), (RedisError, bool)>);

fn format_seed_float(value: f64) -> String {
    format!("{value}")
}

fn plan_record(
    tool_name: &'static str,
    index: usize,
    record: &BulkRecord,
) -> Result<RecordPlan, RedisError> {
    let context =
        |message: String, code: &str| invalid(format!("records[{index}]: {message}"), code);
    if record.key.is_empty() || record.key.len() > MAX_BULK_KEY_BYTES {
        return Err(context(
            format!("keys must contain between 1 and {MAX_BULK_KEY_BYTES} bytes"),
            "INVALID_BULK_RECORD",
        ));
    }
    if let Some(expire_ms) = record.expire_ms
        && expire_ms == 0
    {
        return Err(context(
            "expire_ms must be greater than zero".to_string(),
            "INVALID_BULK_RECORD",
        ));
    }
    let mut commands = Vec::new();
    let mut expire_pending = record.expire_ms;
    match &record.value {
        BulkRecordValue::String { value } => {
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "SET");
            command.arg(record.key.clone()).arg(value.clone());
            if let Some(expire_ms) = expire_pending.take() {
                command.arg("PX").arg(expire_ms.to_string());
            }
            commands.push(command);
        }
        BulkRecordValue::Hash { fields } => {
            if fields.is_empty() || fields.len() > MAX_COLLECTION_ENTRIES_PER_RECORD {
                return Err(context(
                    format!(
                        "hash records must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} fields"
                    ),
                    "INVALID_BULK_RECORD",
                ));
            }
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "HSET");
            command.arg(record.key.clone());
            for (field, value) in fields {
                if field.is_empty() {
                    return Err(context(
                        "hash field names must not be empty".to_string(),
                        "INVALID_BULK_RECORD",
                    ));
                }
                command.arg(field.clone()).arg(value.clone());
            }
            commands.push(command);
        }
        BulkRecordValue::List { elements } => {
            if elements.is_empty() || elements.len() > MAX_COLLECTION_ENTRIES_PER_RECORD {
                return Err(context(
                    format!(
                        "list records must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} elements"
                    ),
                    "INVALID_BULK_RECORD",
                ));
            }
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "RPUSH");
            command.arg(record.key.clone());
            for element in elements {
                command.arg(element.clone());
            }
            commands.push(command);
        }
        BulkRecordValue::Set { members } => {
            if members.is_empty() || members.len() > MAX_COLLECTION_ENTRIES_PER_RECORD {
                return Err(context(
                    format!(
                        "set records must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} members"
                    ),
                    "INVALID_BULK_RECORD",
                ));
            }
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "SADD");
            command.arg(record.key.clone());
            for member in members {
                command.arg(member.clone());
            }
            commands.push(command);
        }
        BulkRecordValue::SortedSet { members } => {
            if members.is_empty() || members.len() > MAX_COLLECTION_ENTRIES_PER_RECORD {
                return Err(context(
                    format!(
                        "sorted-set records must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} members"
                    ),
                    "INVALID_BULK_RECORD",
                ));
            }
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "ZADD");
            command.arg(record.key.clone());
            for (member, score) in members {
                if !score.is_finite() {
                    return Err(context(
                        "sorted-set scores must be finite numbers".to_string(),
                        "INVALID_BULK_RECORD",
                    ));
                }
                command.arg(format_seed_float(*score)).arg(member.clone());
            }
            commands.push(command);
        }
        BulkRecordValue::Json { value } => {
            let payload = serde_json::to_vec(value).map_err(|error| {
                context(
                    format!("JSON records must serialize: {error}"),
                    "INVALID_BULK_RECORD",
                )
            })?;
            let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "JSON.SET");
            command.require_module(crate::RedisModule::Json);
            command.arg(record.key.clone()).arg("$").arg(payload);
            commands.push(command);
        }
        BulkRecordValue::VectorSet { elements } => {
            if elements.is_empty() || elements.len() > MAX_COLLECTION_ENTRIES_PER_RECORD {
                return Err(context(
                    format!(
                        "vector-set records must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} elements"
                    ),
                    "INVALID_BULK_RECORD",
                ));
            }
            for (element, vector) in elements {
                if element.is_empty() {
                    return Err(context(
                        "vector-set element names must not be empty".to_string(),
                        "INVALID_BULK_RECORD",
                    ));
                }
                if vector.is_empty() || vector.len() > MAX_VECTOR_DIMENSIONS {
                    return Err(context(
                        format!(
                            "vector-set vectors must contain between 1 and {MAX_VECTOR_DIMENSIONS} dimensions"
                        ),
                        "INVALID_BULK_RECORD",
                    ));
                }
                if vector.iter().any(|value| !value.is_finite()) {
                    return Err(context(
                        "vector-set values must be finite numbers".to_string(),
                        "INVALID_BULK_RECORD",
                    ));
                }
                let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "VADD");
                command
                    .arg(record.key.clone())
                    .arg("VALUES")
                    .arg(vector.len().to_string());
                for value in vector {
                    command.arg(format_seed_float(*value));
                }
                command.arg(element.clone());
                commands.push(command);
            }
        }
    }
    if let Some(expire_ms) = expire_pending {
        let mut command = RedisCommand::new(tool_name, AccessMode::ReadWrite, "PEXPIRE");
        command.arg(record.key.clone()).arg(expire_ms.to_string());
        commands.push(command);
    }
    Ok(RecordPlan {
        index,
        key: record.key.clone(),
        commands,
    })
}

fn request_bytes(records: &[BulkRecord]) -> usize {
    records.iter().fold(0_usize, |total, record| {
        let value_bytes = match &record.value {
            BulkRecordValue::String { value } => value.len(),
            BulkRecordValue::Hash { fields } => fields
                .iter()
                .map(|(field, value)| field.len() + value.len())
                .sum(),
            BulkRecordValue::List { elements } => elements.iter().map(Vec::len).sum(),
            BulkRecordValue::Set { members } => members.iter().map(Vec::len).sum(),
            BulkRecordValue::SortedSet { members } => {
                members.iter().map(|(member, _)| member.len() + 8).sum()
            }
            BulkRecordValue::Json { value } => serde_json::to_vec(value)
                .map(|payload| payload.len())
                .unwrap_or(usize::MAX / 4),
            BulkRecordValue::VectorSet { elements } => elements
                .iter()
                .map(|(element, vector)| element.len() + vector.len() * 8)
                .sum(),
        };
        total
            .saturating_add(record.key.len())
            .saturating_add(value_bytes)
    })
}

/// Public service that applies bulk policy before invoking the shared
/// executor with bounded concurrency.
#[derive(Clone)]
pub struct RedisBulkEngine {
    invocation: RedisInvocationEngine,
    limits: RedisBulkLimits,
}

impl RedisBulkEngine {
    /// Pair the shared invocation policy with the default bulk bounds.
    pub fn new(invocation: RedisInvocationEngine) -> Self {
        Self {
            invocation,
            limits: RedisBulkLimits::default(),
        }
    }

    /// Replace the bulk bounds. Invalid limits fail each request.
    pub fn with_limits(mut self, limits: RedisBulkLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn limits(&self) -> RedisBulkLimits {
        self.limits
    }

    /// Validate, bound, and execute one bulk load.
    pub async fn load(&self, request: BulkLoadRequest) -> Result<BulkLoadReport, RedisError> {
        self.load_as(BULK_LOAD_TOOL_NAME, request).await
    }

    async fn load_as(
        &self,
        tool_name: &'static str,
        request: BulkLoadRequest,
    ) -> Result<BulkLoadReport, RedisError> {
        let limits = self.limits.validate()?;
        self.invocation
            .require_access(AccessMode::ReadWrite, tool_name)?;
        let options = request.options;
        if request.records.is_empty() || request.records.len() > limits.max_records() {
            return Err(invalid(
                format!(
                    "bulk requests must contain between 1 and {} records",
                    limits.max_records()
                ),
                "BULK_RECORD_LIMIT_EXCEEDED",
            ));
        }
        if options.batch_size == 0 || options.batch_size > limits.max_batch_size() {
            return Err(invalid(
                format!(
                    "batch_size must be between 1 and {}",
                    limits.max_batch_size()
                ),
                "INVALID_BULK_OPTIONS",
            ));
        }
        if options.concurrency == 0 || options.concurrency > limits.max_concurrency() {
            return Err(invalid(
                format!(
                    "concurrency must be between 1 and {}",
                    limits.max_concurrency()
                ),
                "INVALID_BULK_OPTIONS",
            ));
        }
        let batch_count = request.records.len().div_ceil(options.batch_size);
        if batch_count > limits.max_batch_summaries() {
            return Err(invalid(
                format!(
                    "this request would run {batch_count} batches; raise batch_size so at most {} batches are needed",
                    limits.max_batch_summaries()
                ),
                "INVALID_BULK_OPTIONS",
            ));
        }
        let input_bytes = request_bytes(&request.records);
        if input_bytes > limits.max_input_bytes() {
            return Err(invalid(
                format!(
                    "bulk request size {input_bytes} bytes exceeds configured limit {}",
                    limits.max_input_bytes()
                ),
                "BULK_INPUT_LIMIT_EXCEEDED",
            ));
        }

        // Every record is validated and planned before anything executes, so
        // a malformed record fails the whole request instead of a prefix of
        // it silently applying.
        let mut plans = Vec::with_capacity(request.records.len());
        for (index, record) in request.records.iter().enumerate() {
            plans.push(plan_record(tool_name, index, record)?);
        }
        let total_commands = plans.iter().map(|plan| plan.commands.len()).sum();

        let requested = plans.len();
        if options.dry_run {
            return Ok(BulkLoadReport {
                requested,
                attempted: 0,
                applied: 0,
                failed: 0,
                skipped: 0,
                total_commands,
                dry_run: true,
                complete: true,
                deadline_exceeded: false,
                duration: Duration::ZERO,
                batches: Vec::new(),
                failures: Vec::new(),
                failures_truncated: false,
            });
        }

        let started = Instant::now();
        let deadline = started + limits.max_duration();
        let invocation = Arc::new(self.invocation.clone());
        let mut pending: std::collections::VecDeque<RecordPlan> = plans.into();
        let mut batches = Vec::new();
        let mut failures: Vec<BulkRecordFailure> = Vec::new();
        let mut failures_truncated = false;
        let mut attempted = 0_usize;
        let mut applied = 0_usize;
        let mut failed = 0_usize;
        let mut stop = false;
        let mut deadline_exceeded = false;
        let mut batch_number = 0_usize;

        while !pending.is_empty() && !stop && !deadline_exceeded {
            batch_number += 1;
            let mut batch_left = pending.len().min(options.batch_size);
            let mut batch_spawned = 0_usize;
            let mut batch_applied = 0_usize;
            let mut batch_failed = 0_usize;
            let mut running: JoinSet<RecordOutcome> = JoinSet::new();
            loop {
                while running.len() < options.concurrency
                    && batch_left > 0
                    && !stop
                    && !deadline_exceeded
                {
                    let plan = pending.pop_front().expect("batch_left tracks pending");
                    batch_left -= 1;
                    batch_spawned += 1;
                    attempted += 1;
                    let invocation = invocation.clone();
                    running.spawn(async move {
                        for (position, command) in plan.commands.into_iter().enumerate() {
                            if let Err(error) = invocation.execute_curated(command).await {
                                return (plan.index, plan.key, Err((error, position > 0)));
                            }
                        }
                        (plan.index, plan.key, Ok(()))
                    });
                }
                if running.is_empty() {
                    break;
                }
                match tokio::time::timeout_at(deadline, running.join_next()).await {
                    Err(_) => {
                        // Commands in flight at the deadline have unknown
                        // outcomes; abandon them without retrying.
                        deadline_exceeded = true;
                        let unknown = running.len();
                        running.abort_all();
                        batch_failed += unknown;
                        failed += unknown;
                        if failures.len() < limits.max_reported_failures() {
                            failures.push(BulkRecordFailure {
                                index: usize::MAX,
                                key: Vec::new(),
                                code: Some("BULK_DEADLINE_EXCEEDED".to_string()),
                                message: format!(
                                    "the bulk deadline of {} ms elapsed with {unknown} commands in flight; their outcomes are unknown and were not retried",
                                    limits.max_duration().as_millis()
                                ),
                                partially_applied: true,
                            });
                        } else {
                            failures_truncated = true;
                        }
                        break;
                    }
                    Ok(None) => break,
                    Ok(Some(joined)) => {
                        let (index, key, outcome) = joined.expect("bulk record tasks never panic");
                        match outcome {
                            Ok(()) => {
                                applied += 1;
                                batch_applied += 1;
                            }
                            Err((error, partially_applied)) => {
                                failed += 1;
                                batch_failed += 1;
                                if failures.len() < limits.max_reported_failures() {
                                    failures.push(BulkRecordFailure {
                                        index,
                                        key,
                                        code: error.code().map(str::to_string),
                                        message: format!("[{:?}] {error}", error.kind()),
                                        partially_applied,
                                    });
                                } else {
                                    failures_truncated = true;
                                }
                                if options.error_handling == BulkErrorHandling::StopOnError {
                                    stop = true;
                                }
                            }
                        }
                    }
                }
            }
            batches.push(BulkBatchSummary {
                batch: batch_number,
                records: batch_spawned,
                applied: batch_applied,
                failed: batch_failed,
            });
        }
        let skipped = requested - attempted;
        Ok(BulkLoadReport {
            requested,
            attempted,
            applied,
            failed,
            skipped,
            total_commands,
            dry_run: false,
            complete: !stop && !deadline_exceeded && skipped == 0,
            deadline_exceeded,
            duration: started.elapsed(),
            batches,
            failures,
            failures_truncated,
        })
    }

    /// Deterministically generate records and load them.
    pub async fn seed(&self, request: BulkSeedRequest) -> Result<BulkLoadReport, RedisError> {
        let records = generate_seed_records(&request)?;
        self.load_as(
            BULK_SEED_TOOL_NAME,
            BulkLoadRequest::new()
                .records(records)
                .options(request.options),
        )
        .await
    }
}

// --- Deterministic seed generation ------------------------------------------

/// One deterministic value generator leaf.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum BulkSeedValue {
    /// The same value for every record.
    Constant { value: String },
    /// A deterministic lowercase alphanumeric token of this length.
    Token { length: usize },
    /// A deterministic integer in the inclusive range.
    Integer { minimum: i64, maximum: i64 },
    /// A deterministic float in the half-open range.
    Float { minimum: f64, maximum: f64 },
    /// A deterministic choice from the listed values.
    Choice { values: Vec<String> },
    /// `start + index`, where the index counts generated values.
    Sequence { start: u64 },
}

/// One named generated field.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkSeedField {
    pub name: String,
    pub value: BulkSeedValue,
}

/// The generated shape of every seeded record.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum BulkSeedTemplate {
    String {
        value: BulkSeedValue,
    },
    Hash {
        fields: Vec<BulkSeedField>,
    },
    List {
        element: BulkSeedValue,
        elements: usize,
    },
    Set {
        member: BulkSeedValue,
        members: usize,
    },
    SortedSet {
        member: BulkSeedValue,
        score: BulkSeedValue,
        members: usize,
    },
    /// A flat JSON object of generated fields. Requires RedisJSON.
    Json {
        fields: Vec<BulkSeedField>,
    },
}

/// One deterministic seed request: an explicit seed, a key prefix, a count,
/// and a record template.
#[derive(Debug, Clone, PartialEq)]
pub struct BulkSeedRequest {
    pub seed: u64,
    pub count: usize,
    pub key_prefix: String,
    pub template: BulkSeedTemplate,
    pub expire_ms: Option<u64>,
    pub options: BulkLoadOptions,
}

/// SplitMix64: a small, fixed PRNG so identical seed requests generate
/// identical datasets across runs and library versions.
struct SeedStream(u64);

impl SeedStream {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn bounded(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

fn validate_seed_value(value: &BulkSeedValue, context: &str) -> Result<(), RedisError> {
    match value {
        BulkSeedValue::Constant { .. } | BulkSeedValue::Sequence { .. } => Ok(()),
        BulkSeedValue::Token { length } => {
            if *length == 0 || *length > MAX_SEED_TOKEN_LENGTH {
                return Err(invalid(
                    format!(
                        "{context}: token length must be between 1 and {MAX_SEED_TOKEN_LENGTH}"
                    ),
                    "INVALID_SEED_TEMPLATE",
                ));
            }
            Ok(())
        }
        BulkSeedValue::Integer { minimum, maximum } => {
            if minimum > maximum {
                return Err(invalid(
                    format!("{context}: integer minimum must not exceed maximum"),
                    "INVALID_SEED_TEMPLATE",
                ));
            }
            Ok(())
        }
        BulkSeedValue::Float { minimum, maximum } => {
            if !minimum.is_finite() || !maximum.is_finite() || minimum > maximum {
                return Err(invalid(
                    format!("{context}: float bounds must be finite with minimum <= maximum"),
                    "INVALID_SEED_TEMPLATE",
                ));
            }
            Ok(())
        }
        BulkSeedValue::Choice { values } => {
            if values.is_empty() || values.len() > MAX_SEED_CHOICES {
                return Err(invalid(
                    format!(
                        "{context}: choice generators must list between 1 and {MAX_SEED_CHOICES} values"
                    ),
                    "INVALID_SEED_TEMPLATE",
                ));
            }
            Ok(())
        }
    }
}

fn generate_value(value: &BulkSeedValue, stream: &mut SeedStream, index: u64) -> String {
    match value {
        BulkSeedValue::Constant { value } => value.clone(),
        BulkSeedValue::Token { length } => {
            const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
            (0..*length)
                .map(|_| ALPHABET[stream.bounded(ALPHABET.len() as u64) as usize] as char)
                .collect()
        }
        BulkSeedValue::Integer { minimum, maximum } => {
            let span = maximum.abs_diff(*minimum).saturating_add(1);
            let offset = stream.bounded(span);
            minimum.wrapping_add(offset as i64).to_string()
        }
        BulkSeedValue::Float { minimum, maximum } => {
            let fraction = (stream.next_u64() >> 11) as f64 / (1_u64 << 53) as f64;
            format_seed_float(minimum + (maximum - minimum) * fraction)
        }
        BulkSeedValue::Choice { values } => {
            values[stream.bounded(values.len() as u64) as usize].clone()
        }
        BulkSeedValue::Sequence { start } => start.wrapping_add(index).to_string(),
    }
}

fn validate_seed_fields(fields: &[BulkSeedField], context: &str) -> Result<(), RedisError> {
    if fields.is_empty() || fields.len() > MAX_SEED_FIELDS {
        return Err(invalid(
            format!("{context}: templates must contain between 1 and {MAX_SEED_FIELDS} fields"),
            "INVALID_SEED_TEMPLATE",
        ));
    }
    for field in fields {
        if field.name.is_empty() {
            return Err(invalid(
                format!("{context}: field names must not be empty"),
                "INVALID_SEED_TEMPLATE",
            ));
        }
        validate_seed_value(&field.value, context)?;
    }
    Ok(())
}

fn validate_collection_size(size: usize, context: &str) -> Result<(), RedisError> {
    if size == 0 || size > MAX_COLLECTION_ENTRIES_PER_RECORD {
        return Err(invalid(
            format!(
                "{context}: generated collections must contain between 1 and {MAX_COLLECTION_ENTRIES_PER_RECORD} entries"
            ),
            "INVALID_SEED_TEMPLATE",
        ));
    }
    Ok(())
}

/// Deterministically expand a seed request into loadable records.
pub fn generate_seed_records(request: &BulkSeedRequest) -> Result<Vec<BulkRecord>, RedisError> {
    if request.count == 0 {
        return Err(invalid(
            "seed requests must generate at least one record",
            "INVALID_SEED_TEMPLATE",
        ));
    }
    match &request.template {
        BulkSeedTemplate::String { value } => validate_seed_value(value, "template.string")?,
        BulkSeedTemplate::Hash { fields } => validate_seed_fields(fields, "template.hash")?,
        BulkSeedTemplate::List { element, elements } => {
            validate_seed_value(element, "template.list")?;
            validate_collection_size(*elements, "template.list")?;
        }
        BulkSeedTemplate::Set { member, members } => {
            validate_seed_value(member, "template.set")?;
            validate_collection_size(*members, "template.set")?;
        }
        BulkSeedTemplate::SortedSet {
            member,
            score,
            members,
        } => {
            validate_seed_value(member, "template.sorted_set")?;
            validate_seed_value(score, "template.sorted_set.score")?;
            if matches!(
                score,
                BulkSeedValue::Token { .. } | BulkSeedValue::Choice { .. }
            ) {
                return Err(invalid(
                    "template.sorted_set.score must generate numeric values",
                    "INVALID_SEED_TEMPLATE",
                ));
            }
            validate_collection_size(*members, "template.sorted_set")?;
        }
        BulkSeedTemplate::Json { fields } => validate_seed_fields(fields, "template.json")?,
    }

    let mut stream = SeedStream(request.seed);
    let mut records = Vec::with_capacity(request.count);
    for index in 0..request.count {
        let key = format!("{}{index}", request.key_prefix).into_bytes();
        let value = match &request.template {
            BulkSeedTemplate::String { value } => BulkRecordValue::String {
                value: generate_value(value, &mut stream, index as u64).into_bytes(),
            },
            BulkSeedTemplate::Hash { fields } => BulkRecordValue::Hash {
                fields: fields
                    .iter()
                    .map(|field| {
                        (
                            field.name.clone().into_bytes(),
                            generate_value(&field.value, &mut stream, index as u64).into_bytes(),
                        )
                    })
                    .collect(),
            },
            BulkSeedTemplate::List { element, elements } => BulkRecordValue::List {
                elements: (0..*elements)
                    .map(|position| {
                        generate_value(element, &mut stream, position as u64).into_bytes()
                    })
                    .collect(),
            },
            BulkSeedTemplate::Set { member, members } => BulkRecordValue::Set {
                members: (0..*members)
                    .map(|position| {
                        // Suffix with the position so generated members stay
                        // distinct even for narrow generators.
                        let mut value =
                            generate_value(member, &mut stream, position as u64).into_bytes();
                        value.extend_from_slice(format!(":{position}").as_bytes());
                        value
                    })
                    .collect(),
            },
            BulkSeedTemplate::SortedSet {
                member,
                score,
                members,
            } => BulkRecordValue::SortedSet {
                members: (0..*members)
                    .map(|position| {
                        let mut name =
                            generate_value(member, &mut stream, position as u64).into_bytes();
                        name.extend_from_slice(format!(":{position}").as_bytes());
                        let score = generate_value(score, &mut stream, position as u64)
                            .parse::<f64>()
                            .unwrap_or(position as f64);
                        (name, score)
                    })
                    .collect(),
            },
            BulkSeedTemplate::Json { fields } => {
                let mut object = serde_json::Map::new();
                for field in fields {
                    let rendered = generate_value(&field.value, &mut stream, index as u64);
                    let value = match &field.value {
                        BulkSeedValue::Integer { .. } | BulkSeedValue::Sequence { .. } => rendered
                            .parse::<i64>()
                            .map(serde_json::Value::from)
                            .unwrap_or(serde_json::Value::String(rendered)),
                        BulkSeedValue::Float { .. } => rendered
                            .parse::<f64>()
                            .map(serde_json::Value::from)
                            .unwrap_or(serde_json::Value::String(rendered)),
                        _ => serde_json::Value::String(rendered),
                    };
                    object.insert(field.name.clone(), value);
                }
                BulkRecordValue::Json {
                    value: serde_json::Value::Object(object),
                }
            }
        };
        let mut record = BulkRecord::new(key, value);
        if let Some(expire_ms) = request.expire_ms {
            record = record.with_expire_ms(expire_ms);
        }
        records.push(record);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::{RawCommandPolicy, RedisExecutor, RedisValue};

    #[derive(Default)]
    struct RecordingExecutor {
        commands: Mutex<Vec<(String, Vec<Vec<u8>>)>>,
        fail_keys: Vec<Vec<u8>>,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
    }

    #[async_trait]
    impl RedisExecutor for RecordingExecutor {
        async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
            let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_in_flight.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(2)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            self.commands
                .lock()
                .expect("record commands")
                .push((command.name().to_string(), command.arguments().to_vec()));
            if let Some(key) = command.arguments().first()
                && self.fail_keys.iter().any(|fail| fail == key)
            {
                return Err(
                    RedisError::new(RedisErrorKind::Server, "WRONGTYPE simulated failure")
                        .with_code("WRONGTYPE"),
                );
            }
            Ok(RedisValue::Okay)
        }
    }

    fn engine(executor: Arc<RecordingExecutor>) -> RedisBulkEngine {
        RedisBulkEngine::new(
            RedisInvocationEngine::builder(executor)
                .access(AccessMode::ReadWrite)
                .raw_command_policy(RawCommandPolicy::Disabled)
                .build(),
        )
    }

    fn record(key: &str, value: &str) -> BulkRecord {
        BulkRecord::new(
            key,
            BulkRecordValue::String {
                value: value.into(),
            },
        )
    }

    #[tokio::test]
    async fn bulk_load_applies_records_with_bounded_concurrency() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = engine(executor.clone());
        let request = BulkLoadRequest::new()
            .records((0..20).map(|index| record(&format!("bulk:{index}"), "value")))
            .options(BulkLoadOptions {
                batch_size: 10,
                concurrency: 3,
                ..BulkLoadOptions::default()
            });
        let report = engine.load(request).await.expect("bulk load");
        assert_eq!(report.requested, 20);
        assert_eq!(report.applied, 20);
        assert_eq!(report.failed, 0);
        assert_eq!(report.skipped, 0);
        assert!(report.complete);
        assert_eq!(report.batches.len(), 2);
        assert_eq!(report.total_commands, 20);
        assert_eq!(
            executor.commands.lock().expect("recorded").len(),
            20,
            "every record executes exactly one command"
        );
        assert!(
            executor.max_in_flight.load(Ordering::SeqCst) <= 3,
            "concurrency stays bounded"
        );
    }

    #[tokio::test]
    async fn dry_run_validates_and_plans_without_executing() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = engine(executor.clone());
        let request = BulkLoadRequest::new()
            .record(record("bulk:dry", "value").with_expire_ms(1_000))
            .record(BulkRecord::new(
                "bulk:hash",
                BulkRecordValue::Hash {
                    fields: vec![(b"field".to_vec(), b"value".to_vec())],
                },
            ))
            .options(BulkLoadOptions {
                dry_run: true,
                ..BulkLoadOptions::default()
            });
        let report = engine.load(request).await.expect("dry run");
        assert!(report.dry_run);
        assert_eq!(report.requested, 2);
        assert_eq!(report.attempted, 0);
        // String expirations ride SET PX; the hash record is one HSET.
        assert_eq!(report.total_commands, 2);
        assert!(executor.commands.lock().expect("recorded").is_empty());

        let invalid_record = BulkLoadRequest::new()
            .record(BulkRecord::new(
                "",
                BulkRecordValue::String {
                    value: b"x".to_vec(),
                },
            ))
            .options(BulkLoadOptions {
                dry_run: true,
                ..BulkLoadOptions::default()
            });
        let error = engine.load(invalid_record).await.expect_err("invalid key");
        assert_eq!(error.code(), Some("INVALID_BULK_RECORD"));
    }

    #[tokio::test]
    async fn stop_on_error_skips_later_batches_and_reports_identity() {
        let executor = Arc::new(RecordingExecutor {
            fail_keys: vec![b"bulk:2".to_vec()],
            ..RecordingExecutor::default()
        });
        let engine = engine(executor.clone());
        let request = BulkLoadRequest::new()
            .records((0..9).map(|index| record(&format!("bulk:{index}"), "value")))
            .options(BulkLoadOptions {
                batch_size: 3,
                concurrency: 1,
                ..BulkLoadOptions::default()
            });
        let report = engine.load(request).await.expect("stop on error");
        assert!(!report.complete);
        assert_eq!(report.failed, 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].index(), 2);
        assert_eq!(report.failures[0].key(), b"bulk:2");
        assert_eq!(report.failures[0].code(), Some("WRONGTYPE"));
        assert!(!report.failures[0].partially_applied());
        assert!(report.skipped >= 6, "later batches are skipped");
        assert_eq!(
            report.requested,
            report.attempted + report.skipped,
            "accounting stays exact"
        );
    }

    #[tokio::test]
    async fn continue_on_error_attempts_everything() {
        let executor = Arc::new(RecordingExecutor {
            fail_keys: vec![b"bulk:1".to_vec(), b"bulk:4".to_vec()],
            ..RecordingExecutor::default()
        });
        let engine = engine(executor.clone());
        let request = BulkLoadRequest::new()
            .records((0..6).map(|index| record(&format!("bulk:{index}"), "value")))
            .options(BulkLoadOptions {
                batch_size: 2,
                concurrency: 2,
                error_handling: BulkErrorHandling::ContinueOnError,
                ..BulkLoadOptions::default()
            });
        let report = engine.load(request).await.expect("continue on error");
        assert!(report.complete);
        assert_eq!(report.attempted, 6);
        assert_eq!(report.applied, 4);
        assert_eq!(report.failed, 2);
        assert_eq!(report.skipped, 0);
    }

    #[tokio::test]
    async fn read_only_engines_reject_bulk_writes() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = RedisBulkEngine::new(
            RedisInvocationEngine::builder(executor)
                .access(AccessMode::ReadOnly)
                .build(),
        );
        let error = engine
            .load(BulkLoadRequest::new().record(record("bulk:denied", "value")))
            .await
            .expect_err("read-only engines cannot bulk load");
        assert_eq!(error.kind(), RedisErrorKind::Authorization);
    }

    #[tokio::test]
    async fn expirations_ride_set_or_follow_as_pexpire() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = engine(executor.clone());
        let request = BulkLoadRequest::new()
            .record(record("bulk:string", "value").with_expire_ms(5_000))
            .record(
                BulkRecord::new(
                    "bulk:list",
                    BulkRecordValue::List {
                        elements: vec![b"one".to_vec(), b"two".to_vec()],
                    },
                )
                .with_expire_ms(5_000),
            );
        let report = engine.load(request).await.expect("expiring load");
        assert_eq!(report.applied, 2);
        assert_eq!(report.total_commands, 3);
        let commands = executor.commands.lock().expect("recorded");
        let set = commands
            .iter()
            .find(|(name, _)| name == "SET")
            .expect("SET command");
        assert!(set.1.iter().any(|argument| argument == b"PX"));
        assert!(commands.iter().any(|(name, _)| name == "PEXPIRE"));
    }

    #[tokio::test]
    async fn deadlines_stop_the_workflow_without_retrying() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = engine(executor)
            .with_limits(RedisBulkLimits::default().with_max_duration(Duration::from_millis(1)));
        let request = BulkLoadRequest::new()
            .records((0..50).map(|index| record(&format!("bulk:{index}"), "value")))
            .options(BulkLoadOptions {
                batch_size: 10,
                concurrency: 1,
                ..BulkLoadOptions::default()
            });
        let report = engine.load(request).await.expect("deadline load");
        assert!(report.deadline_exceeded);
        assert!(!report.complete);
        assert!(report.skipped > 0);
        assert!(
            report
                .failures
                .iter()
                .any(|failure| failure.code() == Some("BULK_DEADLINE_EXCEEDED")),
            "{report:?}"
        );
    }

    #[test]
    fn seed_generation_is_deterministic_and_bounded() {
        let request = BulkSeedRequest {
            seed: 42,
            count: 25,
            key_prefix: "seed:user:".to_string(),
            template: BulkSeedTemplate::Hash {
                fields: vec![
                    BulkSeedField {
                        name: "name".to_string(),
                        value: BulkSeedValue::Token { length: 8 },
                    },
                    BulkSeedField {
                        name: "age".to_string(),
                        value: BulkSeedValue::Integer {
                            minimum: 18,
                            maximum: 99,
                        },
                    },
                    BulkSeedField {
                        name: "tier".to_string(),
                        value: BulkSeedValue::Choice {
                            values: vec!["free".to_string(), "pro".to_string()],
                        },
                    },
                    BulkSeedField {
                        name: "id".to_string(),
                        value: BulkSeedValue::Sequence { start: 1_000 },
                    },
                ],
            },
            expire_ms: None,
            options: BulkLoadOptions::default(),
        };
        let first = generate_seed_records(&request).expect("generate");
        let second = generate_seed_records(&request).expect("generate again");
        assert_eq!(first, second, "same seed and schema must be identical");
        assert_eq!(first.len(), 25);
        assert_eq!(first[0].key(), b"seed:user:0");
        assert_eq!(first[24].key(), b"seed:user:24");
        let BulkRecordValue::Hash { fields } = first[7].value() else {
            panic!("hash template generates hashes");
        };
        assert_eq!(fields.len(), 4);
        let age = fields
            .iter()
            .find(|(name, _)| name == b"age")
            .expect("age field");
        let age: i64 = std::str::from_utf8(&age.1)
            .expect("utf8 age")
            .parse()
            .expect("numeric age");
        assert!((18..=99).contains(&age));
        let id = fields.iter().find(|(name, _)| name == b"id").expect("id");
        assert_eq!(id.1, b"1007");

        let different = generate_seed_records(&BulkSeedRequest {
            seed: 43,
            ..request.clone()
        })
        .expect("different seed");
        assert_ne!(first, different, "different seeds must differ");

        let invalid_template = BulkSeedRequest {
            template: BulkSeedTemplate::SortedSet {
                member: BulkSeedValue::Token { length: 4 },
                score: BulkSeedValue::Token { length: 4 },
                members: 4,
            },
            ..request
        };
        assert_eq!(
            generate_seed_records(&invalid_template).unwrap_err().code(),
            Some("INVALID_SEED_TEMPLATE")
        );
    }

    #[tokio::test]
    async fn request_bounds_fail_closed() {
        let executor = Arc::new(RecordingExecutor::default());
        let engine = engine(executor).with_limits(
            RedisBulkLimits::default()
                .with_max_records(2)
                .with_max_input_bytes(64),
        );
        let too_many = BulkLoadRequest::new()
            .records((0..3).map(|index| record(&format!("bulk:{index}"), "v")));
        assert_eq!(
            engine.load(too_many).await.unwrap_err().code(),
            Some("BULK_RECORD_LIMIT_EXCEEDED")
        );
        let too_big = BulkLoadRequest::new().record(record("bulk:big", &"x".repeat(128)));
        assert_eq!(
            engine.load(too_big).await.unwrap_err().code(),
            Some("BULK_INPUT_LIMIT_EXCEEDED")
        );
        let bad_options = BulkLoadRequest::new()
            .record(record("bulk:0", "v"))
            .options(BulkLoadOptions {
                concurrency: 100,
                ..BulkLoadOptions::default()
            });
        assert_eq!(
            engine.load(bad_options).await.unwrap_err().code(),
            Some("INVALID_BULK_OPTIONS")
        );
    }
}
