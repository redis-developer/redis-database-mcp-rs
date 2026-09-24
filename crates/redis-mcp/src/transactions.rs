//! Bounded, policy-governed atomic Redis transaction execution.
//!
//! This module provides one-shot MULTI/EXEC transactions with optional WATCH
//! keys. Connection-stateful transaction commands never appear as unrelated
//! MCP calls: one bounded command list executes atomically on one dedicated
//! connection, and every nested command passes the shared native invocation
//! policy first.

use std::{fmt, sync::Arc, time::Duration};

use crate::transport::{Target, connect_cluster};
use async_trait::async_trait;
use redis_tower::commands::RawCommand;
use redis_tower_core::{Command, Frame};
use tokio::sync::Semaphore;

use crate::{
    AccessMode, NativeRedisInvocation, RedisCommand, RedisDeployment, RedisError, RedisErrorKind,
    RedisInvocationEngine, RedisValue,
    executor::{validate_cluster_command_slots, validate_same_cluster_slot},
};

pub(crate) const TRANSACTION_TOOL_NAME: &str = "redis_transaction";

/// Default maximum number of commands in one transaction.
pub const DEFAULT_MAX_TRANSACTION_COMMANDS: usize = 64;
/// Default maximum number of WATCH keys in one transaction.
pub const DEFAULT_MAX_TRANSACTION_WATCH_KEYS: usize = 16;
/// Default maximum total request bytes across watch keys, command names, and
/// arguments.
pub const DEFAULT_MAX_TRANSACTION_REQUEST_BYTES: usize = 1024 * 1024;
/// Default maximum total duration of one transaction, including connection
/// setup, WATCH, and EXEC.
pub const DEFAULT_MAX_TRANSACTION_DURATION: Duration = Duration::from_secs(30);
/// Default maximum concurrent transactions held by one direct adapter.
pub const DEFAULT_MAX_CONCURRENT_TRANSACTIONS: usize = 8;

/// Bounds applied to every transaction before it reaches an executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisTransactionLimits {
    max_commands: usize,
    max_watch_keys: usize,
    max_request_bytes: usize,
    max_duration: Duration,
}

impl Default for RedisTransactionLimits {
    fn default() -> Self {
        Self {
            max_commands: DEFAULT_MAX_TRANSACTION_COMMANDS,
            max_watch_keys: DEFAULT_MAX_TRANSACTION_WATCH_KEYS,
            max_request_bytes: DEFAULT_MAX_TRANSACTION_REQUEST_BYTES,
            max_duration: DEFAULT_MAX_TRANSACTION_DURATION,
        }
    }
}

impl RedisTransactionLimits {
    pub const fn max_commands(self) -> usize {
        self.max_commands
    }

    pub const fn max_watch_keys(self) -> usize {
        self.max_watch_keys
    }

    pub const fn max_request_bytes(self) -> usize {
        self.max_request_bytes
    }

    pub const fn max_duration(self) -> Duration {
        self.max_duration
    }

    pub const fn with_max_commands(mut self, value: usize) -> Self {
        self.max_commands = value;
        self
    }

    pub const fn with_max_watch_keys(mut self, value: usize) -> Self {
        self.max_watch_keys = value;
        self
    }

    pub const fn with_max_request_bytes(mut self, value: usize) -> Self {
        self.max_request_bytes = value;
        self
    }

    pub const fn with_max_duration(mut self, value: Duration) -> Self {
        self.max_duration = value;
        self
    }

    pub(crate) fn validate(self) -> Result<Self, RedisError> {
        if self.max_commands == 0 || self.max_request_bytes == 0 || self.max_duration.is_zero() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "transaction command, byte, and duration limits must be greater than zero",
            )
            .with_code("INVALID_TRANSACTION_LIMITS"));
        }
        Ok(self)
    }
}

/// One bounded transaction: optional watched keys plus an ordered command list.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RedisTransactionRequest {
    watch: Vec<Vec<u8>>,
    commands: Vec<NativeRedisInvocation>,
}

impl RedisTransactionRequest {
    pub fn new() -> Self {
        Self::default()
    }

    /// Watch one binary-safe key for external modification before EXEC.
    pub fn watch(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.watch.push(key.into());
        self
    }

    /// Watch binary-safe keys in order.
    pub fn watch_keys<I, T>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Vec<u8>>,
    {
        self.watch.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Append one nested command in execution order.
    pub fn command(mut self, invocation: NativeRedisInvocation) -> Self {
        self.commands.push(invocation);
        self
    }

    /// Append nested commands in execution order.
    pub fn commands(
        mut self,
        invocations: impl IntoIterator<Item = NativeRedisInvocation>,
    ) -> Self {
        self.commands.extend(invocations);
        self
    }

    pub fn watched_keys(&self) -> &[Vec<u8>] {
        &self.watch
    }

    pub fn command_list(&self) -> &[NativeRedisInvocation] {
        &self.commands
    }
}

impl fmt::Debug for RedisTransactionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisTransactionRequest")
            .field("watch_count", &self.watch.len())
            .field("command_count", &self.commands.len())
            .finish()
    }
}

/// A transaction that already passed classification, access, capability, and
/// bound checks. Executors run it verbatim on one dedicated connection.
#[derive(Clone)]
pub struct RedisPreparedTransaction {
    watch: Vec<Vec<u8>>,
    commands: Vec<RedisCommand>,
    required_access: AccessMode,
}

impl RedisPreparedTransaction {
    pub(crate) fn new(
        watch: Vec<Vec<u8>>,
        commands: Vec<RedisCommand>,
        required_access: AccessMode,
    ) -> Self {
        Self {
            watch,
            commands,
            required_access,
        }
    }

    /// Binary-safe keys to WATCH before MULTI, in request order.
    pub fn watched_keys(&self) -> &[Vec<u8>] {
        &self.watch
    }

    /// Validated commands to queue between MULTI and EXEC, in request order.
    pub fn commands(&self) -> &[RedisCommand] {
        &self.commands
    }

    /// Maximum access level required by any nested command.
    pub fn required_access(&self) -> AccessMode {
        self.required_access
    }
}

impl fmt::Debug for RedisPreparedTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisPreparedTransaction")
            .field("watch_count", &self.watch.len())
            .field("command_count", &self.commands.len())
            .field("required_access", &self.required_access)
            .finish()
    }
}

/// One command the server rejected while the transaction was being built.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisTransactionCommandFailure {
    index: Option<usize>,
    code: String,
    message: Option<String>,
}

impl RedisTransactionCommandFailure {
    pub(crate) fn new(index: Option<usize>, code: String, message: Option<String>) -> Self {
        Self {
            index,
            code,
            message,
        }
    }

    /// Zero-based position of the rejected command in the request, when the
    /// server reported one.
    pub fn index(&self) -> Option<usize> {
        self.index
    }

    /// Stable server error code, such as `NOPERM` or `ERR`.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Server-provided detail, when available.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

/// Explicit result of one atomic transaction attempt.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RedisTransactionOutcome {
    /// EXEC ran. Results align one-to-one with the submitted commands; an
    /// entry may be a [`RedisValue::ServerError`] when that command failed at
    /// runtime while the rest of the transaction still executed.
    Committed { results: Vec<RedisValue> },
    /// EXEC returned nil because a watched key changed before execution.
    /// Nothing executed.
    Aborted,
    /// The server rejected one or more commands while they were queued, so
    /// EXEC aborted the whole transaction. Nothing executed.
    Rejected {
        failures: Vec<RedisTransactionCommandFailure>,
    },
}

/// Executes one prepared transaction atomically for the library.
///
/// Implementations must run WATCH, MULTI, every queued command, and EXEC on
/// one dedicated connection that no other request shares, and must not replay
/// the transaction after EXEC may have been delivered. Dropping the returned
/// future must abandon the connection rather than leak MULTI or WATCH state
/// into a pool.
#[async_trait]
pub trait RedisTransactionExecutor: Send + Sync + 'static {
    async fn execute_transaction(
        &self,
        transaction: RedisPreparedTransaction,
    ) -> Result<RedisTransactionOutcome, RedisError>;
}

#[async_trait]
impl<T> RedisTransactionExecutor for Arc<T>
where
    T: RedisTransactionExecutor + ?Sized,
{
    async fn execute_transaction(
        &self,
        transaction: RedisPreparedTransaction,
    ) -> Result<RedisTransactionOutcome, RedisError> {
        self.as_ref().execute_transaction(transaction).await
    }
}

/// Public service that applies transaction policy before invoking a
/// transaction executor.
#[derive(Clone)]
pub struct RedisTransactionEngine {
    invocation: RedisInvocationEngine,
    executor: Arc<dyn RedisTransactionExecutor>,
    limits: RedisTransactionLimits,
}

impl RedisTransactionEngine {
    /// Pair the shared invocation policy with a host transaction executor.
    pub fn new(invocation: RedisInvocationEngine, executor: impl RedisTransactionExecutor) -> Self {
        Self::from_shared(invocation, Arc::new(executor))
    }

    /// Pair the shared invocation policy with a shared executor trait object.
    pub fn from_shared(
        invocation: RedisInvocationEngine,
        executor: Arc<dyn RedisTransactionExecutor>,
    ) -> Self {
        Self {
            invocation,
            executor,
            limits: RedisTransactionLimits::default(),
        }
    }

    /// Replace the transaction bounds. Invalid limits fail each invocation.
    pub fn with_limits(mut self, limits: RedisTransactionLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn limits(&self) -> RedisTransactionLimits {
        self.limits
    }

    /// Classify and validate a transaction without executing it.
    ///
    /// Every nested command passes the same policy as a native invocation:
    /// raw-policy classification with its hard boundaries, per-command access
    /// tiers, and capability checks. The whole transaction then requires the
    /// maximum access of any nested command.
    pub fn classify(
        &self,
        request: &RedisTransactionRequest,
    ) -> Result<RedisPreparedTransaction, RedisError> {
        let limits = self.limits.validate()?;
        if request.commands.is_empty() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "a transaction requires at least one command",
            )
            .with_code("EMPTY_TRANSACTION"));
        }
        if request.commands.len() > limits.max_commands() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "transaction command count {} exceeds configured limit {}",
                    request.commands.len(),
                    limits.max_commands()
                ),
            )
            .with_code("TRANSACTION_COMMAND_LIMIT_EXCEEDED"));
        }
        if request.watch.len() > limits.max_watch_keys() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "transaction watch key count {} exceeds configured limit {}",
                    request.watch.len(),
                    limits.max_watch_keys()
                ),
            )
            .with_code("TRANSACTION_WATCH_LIMIT_EXCEEDED"));
        }
        let request_bytes = request
            .watch
            .iter()
            .map(Vec::len)
            .chain(request.commands.iter().map(|invocation| {
                invocation.command().len()
                    + invocation.arguments().iter().map(Vec::len).sum::<usize>()
            }))
            .fold(0_usize, usize::saturating_add);
        if request_bytes > limits.max_request_bytes() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "transaction request size {request_bytes} bytes exceeds configured limit {}",
                    limits.max_request_bytes()
                ),
            )
            .with_code("TRANSACTION_REQUEST_LIMIT_EXCEEDED"));
        }

        let cluster = self.invocation.capabilities().deployment() == RedisDeployment::Cluster;
        let mut commands = Vec::with_capacity(request.commands.len());
        let mut required_access = AccessMode::ReadOnly;
        for invocation in &request.commands {
            let metadata = self.invocation.classify(invocation)?;
            self.invocation.require_capabilities(&metadata)?;
            required_access = required_access.max(metadata.required_access());
            let mut command = RedisCommand::new(
                TRANSACTION_TOOL_NAME,
                metadata.required_access(),
                metadata.name().to_string(),
            );
            if let Some(module) = metadata.required_module() {
                command.require_module(module);
            }
            command.args(invocation.arguments().iter().cloned());
            if cluster {
                validate_cluster_command_slots(&command)?;
            }
            commands.push(command);
        }
        if cluster {
            validate_same_cluster_slot(
                request.watch.iter().map(Vec::as_slice),
                "watched keys must hash to the same Redis Cluster slot",
            )?;
        }
        self.invocation
            .require_access(required_access, TRANSACTION_TOOL_NAME)?;
        Ok(RedisPreparedTransaction::new(
            request.watch.clone(),
            commands,
            required_access,
        ))
    }

    /// Classify, execute, and bound one atomic transaction.
    ///
    /// The whole attempt, including connection setup and EXEC, must finish
    /// within the configured maximum duration. A timeout or connection loss
    /// after EXEC was sent leaves the outcome unknown; the engine reports the
    /// failure and never replays a possibly committed transaction.
    pub async fn invoke(
        &self,
        request: RedisTransactionRequest,
    ) -> Result<RedisTransactionOutcome, RedisError> {
        let prepared = self.classify(&request)?;
        let outcome = match tokio::time::timeout(
            self.limits.max_duration(),
            self.executor.execute_transaction(prepared),
        )
        .await
        {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                return Err(error.redact_for_command(TRANSACTION_TOOL_NAME));
            }
            Err(_) => {
                return Err(RedisError::new(
                    RedisErrorKind::Timeout,
                    format!(
                        "redis_transaction timed out after {} ms; the outcome is unknown and committed writes are never replayed",
                        self.limits.max_duration().as_millis()
                    ),
                )
                .with_code("TRANSACTION_TIMEOUT"));
            }
        };
        match outcome {
            RedisTransactionOutcome::Committed { results } => {
                let combined = RedisValue::Array(results);
                self.invocation.require_output_budget(&combined)?;
                let RedisValue::Array(results) = combined else {
                    unreachable!("transaction results stay an array");
                };
                Ok(RedisTransactionOutcome::Committed { results })
            }
            other => Ok(other),
        }
    }
}

enum DirectTransactionTarget {
    Standalone(Target),
    Cluster(Vec<Target>),
}

/// Direct redis-tower transaction executor for fixed standalone or Cluster
/// targets.
///
/// Every transaction runs on a freshly dialed dedicated connection that is
/// dropped afterwards, so MULTI and WATCH state can never leak into pooled
/// connections or across MCP calls. Transactions are never replayed after a server or transport error because
/// a second pipeline could commit the writes twice.
pub struct DirectRedisTransactions {
    target: DirectTransactionTarget,
    concurrency: Arc<Semaphore>,
}

impl fmt::Debug for DirectRedisTransactions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectRedisTransactions")
            .field(
                "target",
                &match self.target {
                    DirectTransactionTarget::Standalone(_) => "standalone",
                    DirectTransactionTarget::Cluster(_) => "cluster",
                },
            )
            .finish()
    }
}

impl DirectRedisTransactions {
    /// Prepare an executor for a fixed standalone Redis URL.
    pub fn standalone(url: &str) -> Result<Self, RedisError> {
        Ok(Self {
            target: DirectTransactionTarget::Standalone(Target::parse(url)?),
            concurrency: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)),
        })
    }

    /// Prepare a Cluster executor using validated discovery seed URLs.
    pub fn cluster<I, S>(seed_urls: I) -> Result<Self, RedisError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let targets = seed_urls
            .into_iter()
            .map(|url| Target::parse(url.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if targets.is_empty() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "at least one Redis Cluster seed URL is required",
            )
            .with_code("EMPTY_CLUSTER_SEEDS"));
        }
        for target in &targets {
            target.cluster_builder()?;
        }
        Ok(Self {
            target: DirectTransactionTarget::Cluster(targets),
            concurrency: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_TRANSACTIONS)),
        })
    }

    /// Bound how many transactions may hold dedicated connections at once.
    pub fn with_max_concurrent_transactions(mut self, value: usize) -> Result<Self, RedisError> {
        if value == 0 {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "maximum concurrent transactions must be greater than zero",
            )
            .with_code("INVALID_TRANSACTION_CONCURRENCY"));
        }
        self.concurrency = Arc::new(Semaphore::new(value));
        Ok(self)
    }
}

fn raw_frame(name: &str, arguments: &[Vec<u8>]) -> Frame {
    let mut command = RawCommand::new(name);
    for argument in arguments {
        command = command.arg(argument.clone());
    }
    command.to_frame()
}

fn check_watch_reply(value: RedisValue) -> Result<(), RedisError> {
    match value {
        RedisValue::Okay => Ok(()),
        RedisValue::SimpleString(value) if value.eq_ignore_ascii_case("OK") => Ok(()),
        RedisValue::ServerError { code, message } => Err(RedisError::new(
            if code == "NOPERM" {
                RedisErrorKind::Authorization
            } else {
                RedisErrorKind::Server
            },
            format!(
                "WATCH was rejected before the transaction started: {}",
                message.unwrap_or_else(|| code.clone())
            ),
        )
        .with_code("TRANSACTION_WATCH_FAILED")),
        other => Err(RedisError::new(
            RedisErrorKind::InvalidResponse,
            format!("WATCH returned an unexpected reply: {other:?}"),
        )
        .with_code("TRANSACTION_WATCH_FAILED")),
    }
}

/// Mark errors that can occur after EXEC left the client, where the commit
/// state on the server is unknowable.
fn mark_unknown_outcome(error: RedisError) -> RedisError {
    if matches!(
        error.kind(),
        RedisErrorKind::Connection | RedisErrorKind::Timeout
    ) {
        RedisError::new(
            error.kind(),
            "the transaction connection failed; the outcome is unknown and committed writes are never replayed",
        )
        .with_code("TRANSACTION_OUTCOME_UNKNOWN")
    } else {
        error
    }
}

fn interpret_exec_reply(
    result: Result<Vec<RedisValue>, RedisError>,
    command_count: usize,
) -> Result<RedisTransactionOutcome, RedisError> {
    let mut values = result.map_err(mark_unknown_outcome)?;
    if values.len() != 1 {
        return Err(RedisError::new(
            RedisErrorKind::InvalidResponse,
            format!("EXEC returned {} replies instead of one", values.len()),
        )
        .with_code("INVALID_TRANSACTION_RESPONSE"));
    }
    match values.pop().expect("one EXEC reply") {
        RedisValue::Nil => Ok(RedisTransactionOutcome::Aborted),
        RedisValue::Array(items) => {
            if items.len() != command_count {
                return Err(RedisError::new(
                    RedisErrorKind::InvalidResponse,
                    format!(
                        "EXEC returned {} results for {command_count} commands",
                        items.len()
                    ),
                )
                .with_code("INVALID_TRANSACTION_RESPONSE"));
            }
            Ok(RedisTransactionOutcome::Committed { results: items })
        }
        RedisValue::ServerError { code, message } if code == "EXECABORT" => {
            Ok(RedisTransactionOutcome::Rejected {
                failures: vec![RedisTransactionCommandFailure::new(None, code, message)],
            })
        }
        RedisValue::ServerError { code, message } => Err(RedisError::new(
            RedisErrorKind::Server,
            message.unwrap_or_else(|| "EXEC failed".into()),
        )
        .with_code(code)),
        other => Err(RedisError::new(
            RedisErrorKind::InvalidResponse,
            format!("EXEC returned an unexpected reply: {other:?}"),
        )
        .with_code("INVALID_TRANSACTION_RESPONSE")),
    }
}

#[async_trait]
impl RedisTransactionExecutor for DirectRedisTransactions {
    async fn execute_transaction(
        &self,
        transaction: RedisPreparedTransaction,
    ) -> Result<RedisTransactionOutcome, RedisError> {
        let _permit = self.concurrency.acquire().await.map_err(|_| {
            RedisError::new(
                RedisErrorKind::Other,
                "the transaction executor is shutting down",
            )
        })?;
        let commands = transaction.commands();
        let command_frames = commands
            .iter()
            .map(|command| raw_frame(command.name(), command.arguments()))
            .collect::<Vec<_>>();
        let watch = (!transaction.watched_keys().is_empty())
            .then(|| raw_frame("WATCH", transaction.watched_keys()));
        // Keep the Cluster owner alive while the dedicated connection is used.
        let cluster;
        let mut connection = match &self.target {
            DirectTransactionTarget::Standalone(target) => target.connect().await?,
            DirectTransactionTarget::Cluster(targets) => {
                let mut all_frames = command_frames.clone();
                all_frames.extend(watch.iter().cloned());
                let slot = redis_tower_cluster::key_extractor::common_slot(&all_frames).map_err(
                    |error| {
                        RedisError::new(RedisErrorKind::InvalidRequest, error.to_string())
                            .with_code("CROSSSLOT")
                    },
                )?;
                cluster = connect_cluster(targets).await?;
                let topology = cluster.topology().await;
                let node = match slot {
                    Some(slot) => topology.master_for_slot(slot).cloned(),
                    None => topology.master_addrs().first().map(|node| (*node).clone()),
                }
                .ok_or_else(|| {
                    RedisError::new(
                        RedisErrorKind::Connection,
                        "Cluster has no transaction owner",
                    )
                })?;
                cluster
                    .connect_to_node(node)
                    .await
                    .map_err(RedisError::from)?
            }
        };
        if let Some(watch) = watch {
            let mut replies = connection
                .execute_pipeline(vec![watch])
                .await
                .map_err(RedisError::from)?;
            check_watch_reply(RedisValue::from(replies.remove(0)))?;
        }
        // Confirm MULTI before sending writes. If ACLs reject MULTI, no
        // queued command may accidentally execute outside the transaction.
        let multi = connection
            .execute(RawCommand::new("MULTI"))
            .await
            .map_err(RedisError::from)?;
        check_watch_reply(RedisValue::from(multi)).map_err(|_| {
            RedisError::new(RedisErrorKind::Server, "MULTI was rejected")
                .with_code("TRANSACTION_MULTI_FAILED")
        })?;
        // Keep all QUEUED/error replies and EXEC to preserve rejection indices.
        let mut frames = command_frames;
        frames.push(raw_frame("EXEC", &[]));
        let replies = connection
            .execute_pipeline(frames)
            .await
            .map_err(RedisError::from)
            .map_err(mark_unknown_outcome)?;
        let mut replies = replies.into_iter().map(RedisValue::from);
        let mut failures = Vec::new();
        for index in 0..commands.len() {
            match replies.next().expect("one queued reply per command") {
                RedisValue::ServerError { code, message } => failures.push(
                    RedisTransactionCommandFailure::new(Some(index), code, message),
                ),
                RedisValue::SimpleString(value) if value == "QUEUED" => {}
                _ => {
                    return Err(RedisError::new(
                        RedisErrorKind::InvalidResponse,
                        "transaction command was not queued",
                    )
                    .with_code("INVALID_TRANSACTION_RESPONSE"));
                }
            }
        }
        let outcome = interpret_exec_reply(Ok(replies.collect()), commands.len())?;
        if matches!(outcome, RedisTransactionOutcome::Rejected { .. }) && !failures.is_empty() {
            return Ok(RedisTransactionOutcome::Rejected { failures });
        }
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{OutputBudget, RawCommandPolicy, RedisExecutor};

    struct RejectingExecutor;

    #[async_trait]
    impl RedisExecutor for RejectingExecutor {
        async fn execute(&self, _command: RedisCommand) -> Result<RedisValue, RedisError> {
            Err(RedisError::new(
                RedisErrorKind::Other,
                "single-command execution is unused in transaction tests",
            ))
        }
    }

    struct RecordingTransactionExecutor {
        prepared: Mutex<Option<RedisPreparedTransaction>>,
        outcome: RedisTransactionOutcome,
    }

    impl RecordingTransactionExecutor {
        fn committed(results: Vec<RedisValue>) -> Arc<Self> {
            Arc::new(Self {
                prepared: Mutex::new(None),
                outcome: RedisTransactionOutcome::Committed { results },
            })
        }
    }

    #[async_trait]
    impl RedisTransactionExecutor for RecordingTransactionExecutor {
        async fn execute_transaction(
            &self,
            transaction: RedisPreparedTransaction,
        ) -> Result<RedisTransactionOutcome, RedisError> {
            *self.prepared.lock().expect("record transaction") = Some(transaction);
            Ok(self.outcome.clone())
        }
    }

    fn engine(
        executor: Arc<RecordingTransactionExecutor>,
        access: AccessMode,
        policy: RawCommandPolicy,
    ) -> RedisTransactionEngine {
        let invocation = RedisInvocationEngine::builder(RejectingExecutor)
            .access(access)
            .raw_command_policy(policy)
            .build();
        RedisTransactionEngine::from_shared(invocation, executor)
    }

    fn get_set_request() -> RedisTransactionRequest {
        RedisTransactionRequest::new()
            .watch("inventory:{tenant}:count")
            .command(NativeRedisInvocation::new("GET").arg("inventory:{tenant}:count"))
            .command(
                NativeRedisInvocation::new("SET")
                    .arg("inventory:{tenant}:count")
                    .arg("7"),
            )
    }

    #[tokio::test]
    async fn transactions_require_the_maximum_nested_access() {
        let executor =
            RecordingTransactionExecutor::committed(vec![RedisValue::Nil, RedisValue::Okay]);
        let engine = engine(
            executor.clone(),
            AccessMode::Full,
            RawCommandPolicy::Classified,
        );
        let outcome = engine
            .invoke(get_set_request())
            .await
            .expect("mixed transaction");
        assert!(matches!(
            outcome,
            RedisTransactionOutcome::Committed { results } if results.len() == 2
        ));
        let prepared = executor
            .prepared
            .lock()
            .expect("recorded transaction")
            .clone()
            .expect("executor received the transaction");
        assert_eq!(prepared.required_access(), AccessMode::ReadWrite);
        assert_eq!(prepared.watched_keys().len(), 1);
        assert_eq!(prepared.commands().len(), 2);
        assert_eq!(prepared.commands()[0].name(), "GET");
        assert_eq!(prepared.commands()[0].tool_name(), "redis_transaction");
        assert_eq!(
            prepared.commands()[0].required_access(),
            AccessMode::ReadOnly
        );
        assert_eq!(
            prepared.commands()[1].required_access(),
            AccessMode::ReadWrite
        );
    }

    #[tokio::test]
    async fn read_only_engines_reject_nested_writes() {
        let executor = RecordingTransactionExecutor::committed(Vec::new());
        let engine = engine(executor, AccessMode::ReadOnly, RawCommandPolicy::Classified);
        let error = engine
            .invoke(get_set_request())
            .await
            .expect_err("read-only engine must reject SET");
        assert_eq!(error.kind(), RedisErrorKind::Authorization);
        assert_eq!(error.code(), Some("COMMAND_ACCESS_DENIED"));
    }

    #[tokio::test]
    async fn disabled_raw_policy_rejects_every_transaction() {
        let executor = RecordingTransactionExecutor::committed(Vec::new());
        let engine = engine(executor, AccessMode::Full, RawCommandPolicy::Disabled);
        let error = engine
            .invoke(get_set_request())
            .await
            .expect_err("disabled policy must fail closed");
        assert_eq!(error.code(), Some("RAW_COMMANDS_DISABLED"));
    }

    #[tokio::test]
    async fn transaction_hard_boundaries_cannot_be_bypassed() {
        for (command, code) in [
            ("MULTI", "TRANSACTION_COMMAND_UNSUPPORTED"),
            ("EXEC", "TRANSACTION_COMMAND_UNSUPPORTED"),
            ("WATCH", "TRANSACTION_COMMAND_UNSUPPORTED"),
            ("SUBSCRIBE", "SUBSCRIPTION_COMMAND_UNSUPPORTED"),
            ("BLPOP", "BLOCKING_COMMAND_UNSUPPORTED"),
            ("EVAL", "SCRIPT_COMMAND_UNSUPPORTED"),
            ("FLUSHALL", "ADMIN_COMMAND_UNSUPPORTED"),
            ("SHUTDOWN", "SERVER_LIFECYCLE_COMMAND_UNSUPPORTED"),
        ] {
            let executor = RecordingTransactionExecutor::committed(Vec::new());
            let engine = engine(executor, AccessMode::Full, RawCommandPolicy::Unrestricted);
            let request = RedisTransactionRequest::new()
                .command(NativeRedisInvocation::new("GET").arg("key"))
                .command(NativeRedisInvocation::new(command));
            let error = engine
                .invoke(request)
                .await
                .expect_err("hard boundaries must stay blocked inside transactions");
            assert_eq!(error.code(), Some(code), "{command}");
            assert_eq!(error.kind(), RedisErrorKind::InvalidRequest, "{command}");
        }
    }

    #[tokio::test]
    async fn transaction_bounds_fail_closed() {
        let executor = RecordingTransactionExecutor::committed(Vec::new());
        let engine = engine(executor, AccessMode::Full, RawCommandPolicy::Classified).with_limits(
            RedisTransactionLimits::default()
                .with_max_commands(1)
                .with_max_watch_keys(1)
                .with_max_request_bytes(16),
        );

        let empty = RedisTransactionRequest::new();
        assert_eq!(
            engine.invoke(empty).await.unwrap_err().code(),
            Some("EMPTY_TRANSACTION")
        );

        let too_many_commands = RedisTransactionRequest::new()
            .command(NativeRedisInvocation::new("GET").arg("a"))
            .command(NativeRedisInvocation::new("GET").arg("b"));
        assert_eq!(
            engine.invoke(too_many_commands).await.unwrap_err().code(),
            Some("TRANSACTION_COMMAND_LIMIT_EXCEEDED")
        );

        let too_many_watch_keys = RedisTransactionRequest::new()
            .watch_keys(["a", "b"])
            .command(NativeRedisInvocation::new("GET").arg("a"));
        assert_eq!(
            engine.invoke(too_many_watch_keys).await.unwrap_err().code(),
            Some("TRANSACTION_WATCH_LIMIT_EXCEEDED")
        );

        let oversized = RedisTransactionRequest::new()
            .command(NativeRedisInvocation::new("GET").arg("a".repeat(64)));
        assert_eq!(
            engine.invoke(oversized).await.unwrap_err().code(),
            Some("TRANSACTION_REQUEST_LIMIT_EXCEEDED")
        );
    }

    #[tokio::test]
    async fn committed_results_share_the_output_budget() {
        let oversized = RedisValue::BulkString(vec![b'x'; 4096]);
        let executor = RecordingTransactionExecutor::committed(vec![oversized]);
        let invocation = RedisInvocationEngine::builder(RejectingExecutor)
            .access(AccessMode::Full)
            .raw_command_policy(RawCommandPolicy::Classified)
            .output_budget(OutputBudget::new(512, 10))
            .build();
        let engine = RedisTransactionEngine::from_shared(invocation, executor);
        let request =
            RedisTransactionRequest::new().command(NativeRedisInvocation::new("GET").arg("key"));
        let error = engine
            .invoke(request)
            .await
            .expect_err("oversized results must fail the shared budget");
        assert_eq!(error.kind(), RedisErrorKind::OutputLimit);
        assert!(error.output_limit().is_some());
    }

    #[tokio::test]
    async fn slow_executors_report_an_unknown_outcome_timeout() {
        struct SlowExecutor;

        #[async_trait]
        impl RedisTransactionExecutor for SlowExecutor {
            async fn execute_transaction(
                &self,
                _transaction: RedisPreparedTransaction,
            ) -> Result<RedisTransactionOutcome, RedisError> {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(RedisTransactionOutcome::Aborted)
            }
        }

        let invocation = RedisInvocationEngine::builder(RejectingExecutor)
            .access(AccessMode::Full)
            .raw_command_policy(RawCommandPolicy::Classified)
            .build();
        let engine = RedisTransactionEngine::new(invocation, SlowExecutor).with_limits(
            RedisTransactionLimits::default().with_max_duration(Duration::from_millis(20)),
        );
        let request =
            RedisTransactionRequest::new().command(NativeRedisInvocation::new("GET").arg("key"));
        let error = engine.invoke(request).await.expect_err("timeout");
        assert_eq!(error.kind(), RedisErrorKind::Timeout);
        assert_eq!(error.code(), Some("TRANSACTION_TIMEOUT"));
    }

    #[test]
    fn cluster_targets_validate_watch_and_command_slots_before_dialing() {
        let executor = RecordingTransactionExecutor::committed(Vec::new());
        let invocation = RedisInvocationEngine::builder(RejectingExecutor)
            .access(AccessMode::Full)
            .raw_command_policy(RawCommandPolicy::Classified)
            .capabilities(
                crate::RedisCapabilities::unknown().with_deployment(RedisDeployment::Cluster),
            )
            .build();
        let engine = RedisTransactionEngine::from_shared(invocation, executor);

        let cross_slot_watch = RedisTransactionRequest::new()
            .watch_keys(["order:{a}:1", "order:{b}:1"])
            .command(NativeRedisInvocation::new("GET").arg("order:{a}:1"));
        assert_eq!(
            engine.classify(&cross_slot_watch).unwrap_err().code(),
            Some("CROSSSLOT")
        );

        let cross_slot_store = RedisTransactionRequest::new().command(
            NativeRedisInvocation::new("SUNIONSTORE")
                .arg("set:{a}:destination")
                .arg("set:{b}:source"),
        );
        assert_eq!(
            engine.classify(&cross_slot_store).unwrap_err().code(),
            Some("CROSSSLOT")
        );
    }

    #[test]
    fn exec_replies_map_to_explicit_outcomes() {
        assert_eq!(
            interpret_exec_reply(Ok(vec![RedisValue::Nil]), 2).expect("aborted"),
            RedisTransactionOutcome::Aborted
        );
        let committed = interpret_exec_reply(
            Ok(vec![RedisValue::Array(vec![
                RedisValue::Okay,
                RedisValue::Integer(7),
            ])]),
            2,
        )
        .expect("committed");
        assert_eq!(
            committed,
            RedisTransactionOutcome::Committed {
                results: vec![RedisValue::Okay, RedisValue::Integer(7)],
            }
        );
        let misaligned =
            interpret_exec_reply(Ok(vec![RedisValue::Array(vec![RedisValue::Okay])]), 2)
                .expect_err("misaligned results");
        assert_eq!(misaligned.code(), Some("INVALID_TRANSACTION_RESPONSE"));
    }

    #[test]
    fn connection_failures_are_marked_outcome_unknown() {
        let error = mark_unknown_outcome(RedisError::new(
            RedisErrorKind::Connection,
            "connection reset by peer while awaiting EXEC",
        ));
        assert_eq!(error.code(), Some("TRANSACTION_OUTCOME_UNKNOWN"));
        assert_eq!(error.kind(), RedisErrorKind::Connection);

        let invalid = mark_unknown_outcome(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "cross slot",
        ));
        assert_eq!(invalid.code(), None);
    }

    #[test]
    fn direct_adapters_validate_their_configuration() {
        assert_eq!(
            DirectRedisTransactions::cluster(Vec::<String>::new())
                .unwrap_err()
                .code(),
            Some("EMPTY_CLUSTER_SEEDS")
        );
        let adapter = DirectRedisTransactions::standalone("redis://127.0.0.1:6379")
            .expect("standalone adapter");
        assert_eq!(
            adapter
                .with_max_concurrent_transactions(0)
                .unwrap_err()
                .code(),
            Some("INVALID_TRANSACTION_CONCURRENCY")
        );
    }
}
