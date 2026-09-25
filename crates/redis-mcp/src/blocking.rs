//! Bounded, finite blocking list/sorted-set operations and replication waits.
//!
//! Every call runs on a freshly dialed dedicated connection so a blocking
//! wait can never stall the shared executor, a pooled connection, or another
//! MCP request. Waits are finite by construction: callers must declare a
//! positive timeout, the engine caps it at a configured maximum, and the
//! client-side deadline always exceeds the server-side timeout so the server
//! stays authoritative about whether an element was consumed. A call whose
//! connection fails after the command was sent reports an unknown outcome
//! instead of retrying, because a replayed blocking pop could consume a
//! second element.

use std::{fmt, sync::Arc, time::Duration};

use crate::transport::Target;
use async_trait::async_trait;
use redis_tower::commands::RawCommand;
use tokio::sync::Semaphore;

use crate::{
    AccessMode,
    executor::{RedisCommand, RedisError, RedisErrorKind, RedisValue},
};

/// Default maximum server-side timeout for one blocking call.
pub const DEFAULT_MAX_BLOCKING_TIMEOUT: Duration = Duration::from_secs(30);
/// Default maximum number of keys accepted by one multi-key blocking call.
pub const DEFAULT_MAX_BLOCKING_KEYS: usize = 16;
/// Default maximum element count accepted by counted pop and move forms.
pub const DEFAULT_MAX_BLOCKING_COUNT: usize = 100;
/// Default maximum number of concurrent dedicated blocking connections.
pub const DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS: usize = 16;

/// Additional client-side allowance beyond the server timeout, covering
/// connection setup and response transfer before a call is abandoned.
const CLIENT_DEADLINE_MARGIN: Duration = Duration::from_secs(5);

/// Bounds enforced by [`RedisBlockingEngine`] before any connection is dialed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBlockingLimits {
    max_timeout: Duration,
    max_keys: usize,
    max_count: usize,
}

impl Default for RedisBlockingLimits {
    fn default() -> Self {
        Self {
            max_timeout: DEFAULT_MAX_BLOCKING_TIMEOUT,
            max_keys: DEFAULT_MAX_BLOCKING_KEYS,
            max_count: DEFAULT_MAX_BLOCKING_COUNT,
        }
    }
}

impl RedisBlockingLimits {
    /// Maximum server-side timeout for one blocking call.
    pub const fn max_timeout(self) -> Duration {
        self.max_timeout
    }

    /// Maximum number of keys accepted by one multi-key blocking call.
    pub const fn max_keys(self) -> usize {
        self.max_keys
    }

    /// Maximum element count accepted by counted pop and move forms.
    pub const fn max_count(self) -> usize {
        self.max_count
    }

    /// Replace the maximum server-side timeout.
    pub const fn with_max_timeout(mut self, value: Duration) -> Self {
        self.max_timeout = value;
        self
    }

    /// Replace the maximum key count.
    pub const fn with_max_keys(mut self, value: usize) -> Self {
        self.max_keys = value;
        self
    }

    /// Replace the maximum element count.
    pub const fn with_max_count(mut self, value: usize) -> Self {
        self.max_count = value;
        self
    }

    pub(crate) fn validate(self) -> Result<Self, RedisError> {
        if self.max_timeout.is_zero() || self.max_keys == 0 || self.max_count == 0 {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "blocking limits must all be greater than zero",
            )
            .with_code("INVALID_BLOCKING_LIMITS"));
        }
        Ok(self)
    }
}

/// List end selected by a blocking pop or move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisListEnd {
    Left,
    Right,
}

impl RedisListEnd {
    const fn token(self) -> &'static str {
        match self {
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
        }
    }
}

/// Sorted-set end selected by a blocking pop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisSortedSetEnd {
    Min,
    Max,
}

/// Element count semantics for the Redis 8.10 BLMOVEM form, mirroring the
/// non-blocking LMOVEM contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisBlockingMoveAmount {
    /// Move up to `count` elements.
    UpTo {
        count: usize,
        ordering: RedisBlockingMoveOrdering,
    },
    /// Move exactly `count` elements or block until they exist.
    Exactly {
        count: usize,
        ordering: RedisBlockingMoveOrdering,
    },
}

impl RedisBlockingMoveAmount {
    const fn count(self) -> usize {
        match self {
            Self::UpTo { count, .. } | Self::Exactly { count, .. } => count,
        }
    }
}

/// Element transfer ordering for BLMOVEM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisBlockingMoveOrdering {
    /// Move elements one by one, preserving per-element rotation semantics.
    OneByOne,
    /// Move elements as one bulk block.
    Bulk,
}

impl RedisBlockingMoveOrdering {
    const fn token(self) -> &'static str {
        match self {
            Self::OneByOne => "OBO",
            Self::Bulk => "BULK",
        }
    }
}

/// One element popped from the first ready list key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBlockingListPop {
    /// The key that produced the element.
    pub key: Vec<u8>,
    /// The popped element.
    pub element: Vec<u8>,
}

/// A bounded batch popped from the first ready list key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBlockingListMultiPop {
    /// The key that produced the elements.
    pub key: Vec<u8>,
    /// The popped elements in pop order.
    pub elements: Vec<Vec<u8>>,
}

/// One scored member popped from the first ready sorted-set key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBlockingScoredPop {
    /// The key that produced the member.
    pub key: Vec<u8>,
    /// The popped member.
    pub member: Vec<u8>,
    /// The member score as an exact decimal string.
    pub score: String,
}

/// A bounded scored batch popped from the first ready sorted-set key.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisBlockingScoredMultiPop {
    /// The key that produced the members.
    pub key: Vec<u8>,
    /// Popped members with exact decimal score strings, in pop order.
    pub members: Vec<(Vec<u8>, String)>,
}

/// WAITAOF acknowledgement counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct RedisWaitAofAcknowledged {
    /// Local Redis instances that fsynced the AOF.
    pub local: u64,
    /// Replicas that fsynced the AOF.
    pub replicas: u64,
}

/// One validated blocking call ready for a dedicated connection.
///
/// The engine performs every bound check; executors run the command verbatim
/// under `client_deadline` and must abandon the dedicated connection instead
/// of retrying, because a replayed blocking pop could consume a second
/// element.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RedisPreparedBlockingCall {
    command: RedisCommand,
    server_timeout: Duration,
    client_deadline: Duration,
}

impl RedisPreparedBlockingCall {
    /// The prepared command and arguments.
    pub fn command(&self) -> &RedisCommand {
        &self.command
    }

    /// The server-side blocking timeout encoded into the command.
    pub fn server_timeout(&self) -> Duration {
        self.server_timeout
    }

    /// The total client-side deadline, always greater than the server
    /// timeout, after which the call must be abandoned with an unknown
    /// outcome.
    pub fn client_deadline(&self) -> Duration {
        self.client_deadline
    }
}

/// Executes one prepared blocking call for the library.
///
/// Implementations must run the command on a dedicated connection that no
/// other request shares, enforce `client_deadline`, and never replay the
/// command after it may have been delivered. Dropping the returned future
/// must abandon the dedicated connection rather than return it to a pool.
#[async_trait]
pub trait RedisBlockingExecutor: Send + Sync + 'static {
    async fn execute_blocking(
        &self,
        call: RedisPreparedBlockingCall,
    ) -> Result<RedisValue, RedisError>;
}

#[async_trait]
impl<T> RedisBlockingExecutor for Arc<T>
where
    T: RedisBlockingExecutor + ?Sized,
{
    async fn execute_blocking(
        &self,
        call: RedisPreparedBlockingCall,
    ) -> Result<RedisValue, RedisError> {
        self.as_ref().execute_blocking(call).await
    }
}

/// Public service that applies blocking-call policy before invoking a
/// blocking executor.
#[derive(Clone)]
pub struct RedisBlockingEngine {
    executor: Arc<dyn RedisBlockingExecutor>,
    limits: RedisBlockingLimits,
}

impl fmt::Debug for RedisBlockingEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisBlockingEngine")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl RedisBlockingEngine {
    /// Wrap a blocking executor with the default limits.
    pub fn new(executor: impl RedisBlockingExecutor) -> Self {
        Self {
            executor: Arc::new(executor),
            limits: RedisBlockingLimits::default(),
        }
    }

    /// Wrap a shared blocking executor trait object.
    pub fn from_shared(executor: Arc<dyn RedisBlockingExecutor>) -> Self {
        Self {
            executor,
            limits: RedisBlockingLimits::default(),
        }
    }

    /// Replace the default limits.
    pub fn with_limits(mut self, limits: RedisBlockingLimits) -> Self {
        self.limits = limits;
        self
    }

    /// The limits applied to every call.
    pub fn limits(&self) -> RedisBlockingLimits {
        self.limits
    }

    /// Read at most one new consumer-group entry on a dedicated connection.
    ///
    /// This is the bounded primitive used by durable coordination claims. It
    /// deliberately keeps `XREADGROUP BLOCK` off the shared multiplexed
    /// executor, and it never replays a command after it may have reached
    /// Redis.
    #[cfg(feature = "coordination")]
    pub(crate) async fn read_group_one(
        &self,
        group: &str,
        consumer: &str,
        stream: &[u8],
        timeout: Duration,
    ) -> Result<RedisValue, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        let mut command =
            RedisCommand::new("redis_handoff_claim", AccessMode::ReadWrite, "XREADGROUP");
        command
            .arg("GROUP")
            .arg(group)
            .arg(consumer)
            .arg("COUNT")
            .arg("1")
            .arg("BLOCK")
            .arg(timeout.as_millis().to_string())
            .arg("STREAMS")
            .arg(stream)
            .arg(">");
        self.execute(command, timeout).await
    }

    /// BLPOP or BRPOP: pop one element from the first ready key.
    ///
    /// Returns `None` when the server timeout elapsed without an element.
    pub async fn pop_list(
        &self,
        keys: Vec<Vec<u8>>,
        end: RedisListEnd,
        timeout: Duration,
    ) -> Result<Option<RedisBlockingListPop>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        self.validate_keys(keys.len())?;
        let name = match end {
            RedisListEnd::Left => "BLPOP",
            RedisListEnd::Right => "BRPOP",
        };
        let mut command = RedisCommand::new("redis_blpop", AccessMode::Full, name);
        command.args(keys).arg(timeout_seconds(timeout));
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::Array(values) if values.len() == 2 => {
                let mut values = values.into_iter();
                Ok(Some(RedisBlockingListPop {
                    key: required_bytes(values.next().expect("checked length"), name)?,
                    element: required_bytes(values.next().expect("checked length"), name)?,
                }))
            }
            other => Err(unexpected_reply(name, &other)),
        }
    }

    /// BLMOVE: atomically move one element between lists, blocking until the
    /// source is non-empty.
    ///
    /// Returns `None` when the server timeout elapsed without an element.
    pub async fn move_element(
        &self,
        source: Vec<u8>,
        destination: Vec<u8>,
        from: RedisListEnd,
        to: RedisListEnd,
        timeout: Duration,
    ) -> Result<Option<Vec<u8>>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        let mut command = RedisCommand::new("redis_blmove", AccessMode::Full, "BLMOVE");
        command
            .arg(source)
            .arg(destination)
            .arg(from.token())
            .arg(to.token())
            .arg(timeout_seconds(timeout));
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::BulkString(element) => Ok(Some(element)),
            other => Err(unexpected_reply("BLMOVE", &other)),
        }
    }

    /// BLMOVEM (Redis 8.10): move up to or exactly `amount` elements between
    /// lists, blocking until the request can be satisfied.
    ///
    /// Returns `None` when the server timeout elapsed without any element.
    pub async fn move_elements(
        &self,
        source: Vec<u8>,
        destination: Vec<u8>,
        from: RedisListEnd,
        to: RedisListEnd,
        timeout: Duration,
        amount: Option<RedisBlockingMoveAmount>,
    ) -> Result<Option<Vec<Vec<u8>>>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        if let Some(amount) = amount {
            self.validate_count(amount.count())?;
        }
        let mut command = RedisCommand::new("redis_blmovem", AccessMode::Full, "BLMOVEM");
        command
            .arg(source)
            .arg(destination)
            .arg(from.token())
            .arg(to.token())
            .arg(timeout_seconds(timeout));
        if let Some(amount) = amount {
            let (token, count, ordering) = match amount {
                RedisBlockingMoveAmount::UpTo { count, ordering } => ("COUNT", count, ordering),
                RedisBlockingMoveAmount::Exactly { count, ordering } => {
                    ("EXACTLY", count, ordering)
                }
            };
            command
                .arg(token)
                .arg(count.to_string())
                .arg(ordering.token());
        }
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::Array(values) => Ok(Some(
                values
                    .into_iter()
                    .map(|value| required_bytes(value, "BLMOVEM"))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            other => Err(unexpected_reply("BLMOVEM", &other)),
        }
    }

    /// BLMPOP (Redis 7.0): pop up to `count` elements from the first ready
    /// key.
    ///
    /// Returns `None` when the server timeout elapsed without an element.
    pub async fn pop_list_count(
        &self,
        keys: Vec<Vec<u8>>,
        end: RedisListEnd,
        count: usize,
        timeout: Duration,
    ) -> Result<Option<RedisBlockingListMultiPop>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        self.validate_keys(keys.len())?;
        self.validate_count(count)?;
        let mut command = RedisCommand::new("redis_blmpop", AccessMode::Full, "BLMPOP");
        command
            .arg(timeout_seconds(timeout))
            .arg(keys.len().to_string())
            .args(keys)
            .arg(end.token())
            .arg("COUNT")
            .arg(count.to_string());
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::Array(values) if values.len() == 2 => {
                let mut values = values.into_iter();
                let key = required_bytes(values.next().expect("checked length"), "BLMPOP")?;
                let elements = required_array(values.next().expect("checked length"), "BLMPOP")?
                    .into_iter()
                    .map(|value| required_bytes(value, "BLMPOP"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some(RedisBlockingListMultiPop { key, elements }))
            }
            other => Err(unexpected_reply("BLMPOP", &other)),
        }
    }

    /// BZPOPMIN or BZPOPMAX: pop one scored member from the first ready key.
    ///
    /// Returns `None` when the server timeout elapsed without a member.
    pub async fn pop_sorted_set(
        &self,
        keys: Vec<Vec<u8>>,
        end: RedisSortedSetEnd,
        timeout: Duration,
    ) -> Result<Option<RedisBlockingScoredPop>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        self.validate_keys(keys.len())?;
        let name = match end {
            RedisSortedSetEnd::Min => "BZPOPMIN",
            RedisSortedSetEnd::Max => "BZPOPMAX",
        };
        let mut command = RedisCommand::new("redis_bzpop", AccessMode::Full, name);
        command.args(keys).arg(timeout_seconds(timeout));
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::Array(values) if values.len() == 3 => {
                let mut values = values.into_iter();
                Ok(Some(RedisBlockingScoredPop {
                    key: required_bytes(values.next().expect("checked length"), name)?,
                    member: required_bytes(values.next().expect("checked length"), name)?,
                    score: score_string(values.next().expect("checked length"), name)?,
                }))
            }
            other => Err(unexpected_reply(name, &other)),
        }
    }

    /// BZMPOP (Redis 7.0): pop up to `count` scored members from the first
    /// ready key.
    ///
    /// Returns `None` when the server timeout elapsed without a member.
    pub async fn pop_sorted_set_count(
        &self,
        keys: Vec<Vec<u8>>,
        end: RedisSortedSetEnd,
        count: usize,
        timeout: Duration,
    ) -> Result<Option<RedisBlockingScoredMultiPop>, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        self.validate_keys(keys.len())?;
        self.validate_count(count)?;
        let end_token = match end {
            RedisSortedSetEnd::Min => "MIN",
            RedisSortedSetEnd::Max => "MAX",
        };
        let mut command = RedisCommand::new("redis_bzmpop", AccessMode::Full, "BZMPOP");
        command
            .arg(timeout_seconds(timeout))
            .arg(keys.len().to_string())
            .args(keys)
            .arg(end_token)
            .arg("COUNT")
            .arg(count.to_string());
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Nil => Ok(None),
            RedisValue::Array(values) if values.len() == 2 => {
                let mut values = values.into_iter();
                let key = required_bytes(values.next().expect("checked length"), "BZMPOP")?;
                let members = required_array(values.next().expect("checked length"), "BZMPOP")?
                    .into_iter()
                    .map(|pair| scored_member(pair, "BZMPOP"))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Some(RedisBlockingScoredMultiPop { key, members }))
            }
            other => Err(unexpected_reply("BZMPOP", &other)),
        }
    }

    /// WAIT: block until `replicas` replicas acknowledged all writes
    /// previously sent on this dedicated connection, or the timeout elapsed.
    ///
    /// The call runs on a fresh connection with no prior writes, so the
    /// achieved count reports currently acknowledged replicas without
    /// implying durability for writes issued on other connections.
    pub async fn wait(&self, replicas: usize, timeout: Duration) -> Result<u64, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        let mut command = RedisCommand::new("redis_wait", AccessMode::Full, "WAIT");
        command
            .arg(replicas.to_string())
            .arg(timeout.as_millis().to_string());
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Integer(achieved) if achieved >= 0 => Ok(achieved as u64),
            other => Err(unexpected_reply("WAIT", &other)),
        }
    }

    /// WAITAOF (Redis 7.2): block until the requested local and replica AOF
    /// fsync counts are achieved, or the timeout elapsed.
    pub async fn wait_aof(
        &self,
        local: usize,
        replicas: usize,
        timeout: Duration,
    ) -> Result<RedisWaitAofAcknowledged, RedisError> {
        let timeout = self.validate_timeout(timeout)?;
        let mut command = RedisCommand::new("redis_waitaof", AccessMode::Full, "WAITAOF");
        command
            .arg(local.to_string())
            .arg(replicas.to_string())
            .arg(timeout.as_millis().to_string());
        let reply = self.execute(command, timeout).await?;
        match without_attributes(reply) {
            RedisValue::Array(values) if values.len() == 2 => {
                let mut counts = values.into_iter().map(|value| match value {
                    RedisValue::Integer(count) if count >= 0 => Ok(count as u64),
                    other => Err(unexpected_reply("WAITAOF", &other)),
                });
                Ok(RedisWaitAofAcknowledged {
                    local: counts.next().expect("checked length")?,
                    replicas: counts.next().expect("checked length")?,
                })
            }
            other => Err(unexpected_reply("WAITAOF", &other)),
        }
    }

    async fn execute(
        &self,
        command: RedisCommand,
        server_timeout: Duration,
    ) -> Result<RedisValue, RedisError> {
        let call = RedisPreparedBlockingCall {
            command,
            server_timeout,
            client_deadline: server_timeout.saturating_add(CLIENT_DEADLINE_MARGIN),
        };
        self.executor.execute_blocking(call).await
    }

    fn validate_timeout(&self, timeout: Duration) -> Result<Duration, RedisError> {
        if timeout.is_zero() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "a blocking call requires a positive finite timeout; indefinite blocking is not supported",
            )
            .with_code("INVALID_BLOCKING_TIMEOUT"));
        }
        let max = self.limits.max_timeout;
        if timeout > max {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "the requested blocking timeout of {}ms exceeds the configured maximum of {}ms",
                    timeout.as_millis(),
                    max.as_millis()
                ),
            )
            .with_code("BLOCKING_TIMEOUT_EXCEEDED"));
        }
        Ok(timeout)
    }

    fn validate_keys(&self, requested: usize) -> Result<(), RedisError> {
        if requested == 0 || requested > self.limits.max_keys {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "a blocking call accepts between 1 and {} keys; {requested} were provided",
                    self.limits.max_keys
                ),
            )
            .with_code("INVALID_BLOCKING_KEYS"));
        }
        Ok(())
    }

    fn validate_count(&self, requested: usize) -> Result<(), RedisError> {
        if requested == 0 || requested > self.limits.max_count {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "a counted blocking call accepts between 1 and {} elements; {requested} were requested",
                    self.limits.max_count
                ),
            )
            .with_code("INVALID_BLOCKING_COUNT"));
        }
        Ok(())
    }
}

/// Render a Redis blocking timeout as decimal seconds.
///
/// Redis 6.0+ accepts fractional timeouts for every command this module
/// issues; milliseconds keep the MCP contract integer-typed.
fn timeout_seconds(timeout: Duration) -> String {
    let millis = timeout.as_millis();
    if millis.is_multiple_of(1_000) {
        (millis / 1_000).to_string()
    } else {
        let rendered = format!("{}.{:03}", millis / 1_000, millis % 1_000);
        rendered.trim_end_matches('0').to_string()
    }
}

fn without_attributes(value: RedisValue) -> RedisValue {
    match value {
        RedisValue::Attribute { data, .. } => without_attributes(*data),
        other => other,
    }
}

fn required_bytes(value: RedisValue, command: &str) -> Result<Vec<u8>, RedisError> {
    match value {
        RedisValue::BulkString(bytes) => Ok(bytes),
        RedisValue::SimpleString(text) => Ok(text.into_bytes()),
        other => Err(unexpected_reply(command, &other)),
    }
}

fn required_array(value: RedisValue, command: &str) -> Result<Vec<RedisValue>, RedisError> {
    match value {
        RedisValue::Array(values) => Ok(values),
        other => Err(unexpected_reply(command, &other)),
    }
}

fn scored_member(value: RedisValue, command: &str) -> Result<(Vec<u8>, String), RedisError> {
    let mut pair = required_array(value, command)?;
    if pair.len() != 2 {
        return Err(RedisError::new(
            RedisErrorKind::InvalidResponse,
            format!("{command} returned a scored member without exactly two entries"),
        )
        .with_code("INVALID_BLOCKING_RESPONSE"));
    }
    let score = score_string(pair.pop().expect("checked length"), command)?;
    let member = required_bytes(pair.pop().expect("checked length"), command)?;
    Ok((member, score))
}

/// Convert a RESP2 bulk-string or RESP3 double score into an exact decimal
/// string. Bulk strings are preserved verbatim; doubles use the shortest
/// round-trip rendering.
fn score_string(value: RedisValue, command: &str) -> Result<String, RedisError> {
    match value {
        RedisValue::BulkString(bytes) => String::from_utf8(bytes).map_err(|_| {
            RedisError::new(
                RedisErrorKind::InvalidResponse,
                format!("{command} returned a non-UTF-8 score"),
            )
            .with_code("INVALID_BLOCKING_RESPONSE")
        }),
        RedisValue::SimpleString(text) => Ok(text),
        RedisValue::Double(score) => Ok(if score == f64::INFINITY {
            "inf".to_string()
        } else if score == f64::NEG_INFINITY {
            "-inf".to_string()
        } else {
            score.to_string()
        }),
        other => Err(unexpected_reply(command, &other)),
    }
}

fn unexpected_reply(command: &str, value: &RedisValue) -> RedisError {
    match value {
        RedisValue::ServerError { code, message } => RedisError::new(
            RedisErrorKind::Server,
            format!(
                "{command} failed: {}",
                message.as_deref().unwrap_or("no server detail")
            ),
        )
        .with_code(code.clone()),
        other => RedisError::new(
            RedisErrorKind::InvalidResponse,
            format!("{command} returned an unexpected reply shape: {other:?}"),
        )
        .with_code("INVALID_BLOCKING_RESPONSE"),
    }
}

enum DirectBlockingTarget {
    Standalone(Target),
    Cluster(Vec<Target>),
}

/// Direct redis-tower blocking executor for fixed standalone or Cluster targets.
///
/// Every call dials a fresh dedicated connection that is dropped afterwards,
/// so a blocking wait can never occupy a pooled or multiplexed connection.
/// Transport failures are never replayed because a second blocking pop could
/// consume another element. Cluster routing follows the owning primary and
/// rejects cross-slot key lists before dispatch.
pub struct DirectRedisBlocking {
    target: DirectBlockingTarget,
    concurrency: Arc<Semaphore>,
}

impl fmt::Debug for DirectRedisBlocking {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectRedisBlocking")
            .field(
                "target",
                &match self.target {
                    DirectBlockingTarget::Standalone(_) => "standalone",
                    DirectBlockingTarget::Cluster(_) => "cluster",
                },
            )
            .finish()
    }
}

impl DirectRedisBlocking {
    /// Prepare an executor for a fixed standalone Redis URL.
    pub fn standalone(url: &str) -> Result<Self, RedisError> {
        Ok(Self {
            target: DirectBlockingTarget::Standalone(Target::parse(url)?),
            concurrency: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS)),
        })
    }

    /// Prepare an executor for a Redis Cluster through one or more seed URLs.
    pub fn cluster<I, S>(seed_urls: I) -> Result<Self, RedisError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let seed_urls = seed_urls
            .into_iter()
            .map(|url| Target::parse(url.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        if seed_urls.is_empty() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "at least one Redis Cluster seed URL is required",
            )
            .with_code("EMPTY_CLUSTER_SEEDS"));
        }
        for url in &seed_urls {
            url.cluster_builder()?;
        }
        Ok(Self {
            target: DirectBlockingTarget::Cluster(seed_urls),
            concurrency: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS)),
        })
    }

    /// Bound how many blocking calls may hold dedicated connections at once.
    pub fn with_max_concurrent_calls(mut self, value: usize) -> Result<Self, RedisError> {
        if value == 0 {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "maximum concurrent blocking calls must be greater than zero",
            )
            .with_code("INVALID_BLOCKING_CONCURRENCY"));
        }
        self.concurrency = Arc::new(Semaphore::new(value));
        Ok(self)
    }
}

fn blocking_command(command: &RedisCommand) -> RawCommand {
    let mut cmd = RawCommand::new(command.name());
    for argument in command.arguments() {
        cmd = cmd.arg(argument.clone());
    }
    cmd
}

fn deadline_exceeded(command: &RedisCommand, deadline: Duration) -> RedisError {
    RedisError::new(
        RedisErrorKind::Timeout,
        format!(
            "{} exceeded the {}ms client deadline; the outcome is unknown and a delivered element is not replayed",
            command.name(),
            deadline.as_millis()
        ),
    )
    .with_code("BLOCKING_OUTCOME_UNKNOWN")
}

#[async_trait]
impl RedisBlockingExecutor for DirectRedisBlocking {
    async fn execute_blocking(
        &self,
        call: RedisPreparedBlockingCall,
    ) -> Result<RedisValue, RedisError> {
        let _permit = self.concurrency.acquire().await.map_err(|_| {
            RedisError::new(
                RedisErrorKind::Other,
                "the blocking executor is shutting down",
            )
        })?;
        let deadline = call.client_deadline();
        let attempt = async {
            let frame = match &self.target {
                DirectBlockingTarget::Standalone(url) => {
                    let mut connection = url.connect().await?;
                    connection
                        .execute(blocking_command(call.command()))
                        .await
                        .map_err(RedisError::from)?
                }
                DirectBlockingTarget::Cluster(seeds) => {
                    crate::executor::validate_cluster_command_slots(call.command())?;
                    let mut connected = None;
                    let mut last_error = None;
                    // Only discovery may try another seed. Once the command is
                    // submitted, a transport failure is returned without replay.
                    for url in seeds {
                        match url.exclusive_cluster().await {
                            Ok(connection) => {
                                connected = Some(connection);
                                break;
                            }
                            Err(error) => last_error = Some(error),
                        }
                    }
                    let mut connection =
                        connected.ok_or_else(|| last_error.expect("nonempty seeds"))?;
                    connection
                        .execute(blocking_command(call.command()))
                        .await
                        .map_err(RedisError::from)?
                }
            };
            let value = RedisValue::from(frame);
            if let RedisValue::ServerError { code, message } = value {
                let message = message
                    .map(|message| format!("{code} {message}"))
                    .unwrap_or(code);
                return Err(crate::response::server_error(&message));
            }
            Ok(value)
        };
        match tokio::time::timeout(deadline, attempt).await {
            Ok(result) => result,
            Err(_) => Err(deadline_exceeded(call.command(), deadline)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct FakeBlockingExecutor {
        calls: Mutex<Vec<RedisPreparedBlockingCall>>,
        replies: Mutex<Vec<Result<RedisValue, RedisError>>>,
    }

    impl FakeBlockingExecutor {
        fn returning(replies: Vec<Result<RedisValue, RedisError>>) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                replies: Mutex::new(replies),
            })
        }

        fn recorded(&self) -> Vec<RedisPreparedBlockingCall> {
            self.calls.lock().expect("record lock").clone()
        }
    }

    #[async_trait]
    impl RedisBlockingExecutor for FakeBlockingExecutor {
        async fn execute_blocking(
            &self,
            call: RedisPreparedBlockingCall,
        ) -> Result<RedisValue, RedisError> {
            self.calls.lock().expect("record lock").push(call);
            self.replies.lock().expect("reply lock").remove(0)
        }
    }

    fn engine(
        replies: Vec<Result<RedisValue, RedisError>>,
    ) -> (RedisBlockingEngine, Arc<FakeBlockingExecutor>) {
        let executor = FakeBlockingExecutor::returning(replies);
        (
            RedisBlockingEngine::from_shared(executor.clone() as Arc<dyn RedisBlockingExecutor>),
            executor,
        )
    }

    fn argv(call: &RedisPreparedBlockingCall) -> Vec<String> {
        std::iter::once(call.command().name().to_string())
            .chain(
                call.command()
                    .arguments()
                    .iter()
                    .map(|argument| String::from_utf8_lossy(argument).to_string()),
            )
            .collect()
    }

    #[tokio::test]
    async fn blpop_builds_argv_and_decodes_pop() {
        let (engine, executor) = engine(vec![Ok(RedisValue::Array(vec![
            RedisValue::BulkString(b"queue".to_vec()),
            RedisValue::BulkString(b"job-1".to_vec()),
        ]))]);
        let popped = engine
            .pop_list(
                vec![b"queue".to_vec(), b"fallback".to_vec()],
                RedisListEnd::Left,
                Duration::from_millis(1_500),
            )
            .await
            .expect("BLPOP succeeds")
            .expect("an element was ready");
        assert_eq!(popped.key, b"queue");
        assert_eq!(popped.element, b"job-1");
        let calls = executor.recorded();
        assert_eq!(argv(&calls[0]), vec!["BLPOP", "queue", "fallback", "1.5"]);
        assert_eq!(calls[0].server_timeout(), Duration::from_millis(1_500));
        assert!(calls[0].client_deadline() > calls[0].server_timeout());
    }

    #[tokio::test]
    async fn brpop_timeout_returns_none() {
        let (engine, executor) = engine(vec![Ok(RedisValue::Nil)]);
        let popped = engine
            .pop_list(
                vec![b"queue".to_vec()],
                RedisListEnd::Right,
                Duration::from_secs(1),
            )
            .await
            .expect("BRPOP succeeds");
        assert!(popped.is_none());
        assert_eq!(argv(&executor.recorded()[0]), vec!["BRPOP", "queue", "1"]);
    }

    #[tokio::test]
    async fn zero_and_excessive_timeouts_are_rejected_before_execution() {
        let (engine, executor) = engine(vec![]);
        let zero = engine
            .pop_list(vec![b"queue".to_vec()], RedisListEnd::Left, Duration::ZERO)
            .await
            .expect_err("zero timeout is indefinite blocking");
        assert_eq!(zero.code(), Some("INVALID_BLOCKING_TIMEOUT"));
        let excessive = engine
            .pop_list(
                vec![b"queue".to_vec()],
                RedisListEnd::Left,
                DEFAULT_MAX_BLOCKING_TIMEOUT + Duration::from_secs(1),
            )
            .await
            .expect_err("timeout above the cap is rejected");
        assert_eq!(excessive.code(), Some("BLOCKING_TIMEOUT_EXCEEDED"));
        assert!(executor.recorded().is_empty());
    }

    #[tokio::test]
    async fn key_and_count_bounds_are_enforced() {
        let (engine, executor) = engine(vec![]);
        let too_many = engine
            .pop_list(
                vec![b"key".to_vec(); DEFAULT_MAX_BLOCKING_KEYS + 1],
                RedisListEnd::Left,
                Duration::from_secs(1),
            )
            .await
            .expect_err("too many keys");
        assert_eq!(too_many.code(), Some("INVALID_BLOCKING_KEYS"));
        let zero_count = engine
            .pop_list_count(
                vec![b"key".to_vec()],
                RedisListEnd::Left,
                0,
                Duration::from_secs(1),
            )
            .await
            .expect_err("zero count");
        assert_eq!(zero_count.code(), Some("INVALID_BLOCKING_COUNT"));
        assert!(executor.recorded().is_empty());
    }

    #[tokio::test]
    async fn blmove_and_blmovem_build_argv() {
        let (engine, executor) = engine(vec![
            Ok(RedisValue::BulkString(b"job".to_vec())),
            Ok(RedisValue::Array(vec![
                RedisValue::BulkString(b"a".to_vec()),
                RedisValue::BulkString(b"b".to_vec()),
            ])),
        ]);
        let moved = engine
            .move_element(
                b"source".to_vec(),
                b"destination".to_vec(),
                RedisListEnd::Left,
                RedisListEnd::Right,
                Duration::from_secs(2),
            )
            .await
            .expect("BLMOVE succeeds")
            .expect("an element moved");
        assert_eq!(moved, b"job");
        let batch = engine
            .move_elements(
                b"source".to_vec(),
                b"destination".to_vec(),
                RedisListEnd::Left,
                RedisListEnd::Left,
                Duration::from_secs(2),
                Some(RedisBlockingMoveAmount::Exactly {
                    count: 2,
                    ordering: RedisBlockingMoveOrdering::Bulk,
                }),
            )
            .await
            .expect("BLMOVEM succeeds")
            .expect("elements moved");
        assert_eq!(batch, vec![b"a".to_vec(), b"b".to_vec()]);
        let calls = executor.recorded();
        assert_eq!(
            argv(&calls[0]),
            vec!["BLMOVE", "source", "destination", "LEFT", "RIGHT", "2"]
        );
        assert_eq!(
            argv(&calls[1]),
            vec![
                "BLMOVEM",
                "source",
                "destination",
                "LEFT",
                "LEFT",
                "2",
                "EXACTLY",
                "2",
                "BULK"
            ]
        );
    }

    #[tokio::test]
    async fn blmpop_and_bzmpop_decode_first_ready_key_batches() {
        let (engine, executor) = engine(vec![
            Ok(RedisValue::Array(vec![
                RedisValue::BulkString(b"queue".to_vec()),
                RedisValue::Array(vec![RedisValue::BulkString(b"one".to_vec())]),
            ])),
            Ok(RedisValue::Array(vec![
                RedisValue::BulkString(b"board".to_vec()),
                RedisValue::Array(vec![RedisValue::Array(vec![
                    RedisValue::BulkString(b"member".to_vec()),
                    RedisValue::Double(1.5),
                ])]),
            ])),
        ]);
        let list = engine
            .pop_list_count(
                vec![b"queue".to_vec()],
                RedisListEnd::Left,
                5,
                Duration::from_secs(1),
            )
            .await
            .expect("BLMPOP succeeds")
            .expect("elements popped");
        assert_eq!(list.key, b"queue");
        assert_eq!(list.elements, vec![b"one".to_vec()]);
        let scored = engine
            .pop_sorted_set_count(
                vec![b"board".to_vec()],
                RedisSortedSetEnd::Max,
                5,
                Duration::from_secs(1),
            )
            .await
            .expect("BZMPOP succeeds")
            .expect("members popped");
        assert_eq!(scored.key, b"board");
        assert_eq!(
            scored.members,
            vec![(b"member".to_vec(), "1.5".to_string())]
        );
        let calls = executor.recorded();
        assert_eq!(
            argv(&calls[0]),
            vec!["BLMPOP", "1", "1", "queue", "LEFT", "COUNT", "5"]
        );
        assert_eq!(
            argv(&calls[1]),
            vec!["BZMPOP", "1", "1", "board", "MAX", "COUNT", "5"]
        );
    }

    #[tokio::test]
    async fn bzpop_preserves_resp2_score_strings() {
        let (engine, executor) = engine(vec![Ok(RedisValue::Array(vec![
            RedisValue::BulkString(b"board".to_vec()),
            RedisValue::BulkString(b"member".to_vec()),
            RedisValue::BulkString(b"2.5000000000000001".to_vec()),
        ]))]);
        let popped = engine
            .pop_sorted_set(
                vec![b"board".to_vec()],
                RedisSortedSetEnd::Min,
                Duration::from_secs(1),
            )
            .await
            .expect("BZPOPMIN succeeds")
            .expect("a member popped");
        assert_eq!(popped.score, "2.5000000000000001");
        assert_eq!(
            argv(&executor.recorded()[0]),
            vec!["BZPOPMIN", "board", "1"]
        );
    }

    #[tokio::test]
    async fn wait_and_waitaof_report_achieved_counts() {
        let (engine, executor) = engine(vec![
            Ok(RedisValue::Integer(1)),
            Ok(RedisValue::Array(vec![
                RedisValue::Integer(1),
                RedisValue::Integer(0),
            ])),
        ]);
        let achieved = engine
            .wait(2, Duration::from_millis(250))
            .await
            .expect("WAIT succeeds");
        assert_eq!(achieved, 1);
        let aof = engine
            .wait_aof(1, 0, Duration::from_millis(250))
            .await
            .expect("WAITAOF succeeds");
        assert_eq!(aof.local, 1);
        assert_eq!(aof.replicas, 0);
        let calls = executor.recorded();
        assert_eq!(argv(&calls[0]), vec!["WAIT", "2", "250"]);
        assert_eq!(argv(&calls[1]), vec!["WAITAOF", "1", "0", "250"]);
    }

    #[tokio::test]
    async fn server_errors_keep_stable_codes() {
        let (engine, _) = engine(vec![Ok(RedisValue::ServerError {
            code: "WRONGTYPE".to_string(),
            message: Some("Operation against a key holding the wrong kind of value".to_string()),
        })]);
        let error = engine
            .pop_list(
                vec![b"queue".to_vec()],
                RedisListEnd::Left,
                Duration::from_secs(1),
            )
            .await
            .expect_err("server error propagates");
        assert_eq!(error.kind(), RedisErrorKind::Server);
        assert_eq!(error.code(), Some("WRONGTYPE"));
    }

    #[test]
    fn timeout_seconds_renders_exact_decimals() {
        assert_eq!(timeout_seconds(Duration::from_secs(30)), "30");
        assert_eq!(timeout_seconds(Duration::from_millis(1_500)), "1.5");
        assert_eq!(timeout_seconds(Duration::from_millis(50)), "0.05");
        assert_eq!(timeout_seconds(Duration::from_millis(1_001)), "1.001");
    }

    #[test]
    fn limits_reject_zero_values() {
        let error = RedisBlockingLimits::default()
            .with_max_keys(0)
            .validate()
            .expect_err("zero key limit");
        assert_eq!(error.code(), Some("INVALID_BLOCKING_LIMITS"));
    }
}
