//! Redis command execution abstractions.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use crate::transport::{Target, connect_cluster};
use async_trait::async_trait;
#[cfg(test)]
use redis::{ErrorKind as RedisRsErrorKind, ServerErrorKind as RedisRsServerErrorKind};
use redis_tower_cluster::{MultiplexedClusterClient, slot_for_key};

use redis_tower::{
    MultiplexedClient,
    auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig},
    reconnect::ReconnectConfig,
};
use redis_tower_core::{Command, Frame};

use crate::{
    AccessMode, DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT, RedisCapabilities, RedisDeployment,
    RedisModule, RedisOutputLimit, RedisOutputLimitDimension, capabilities::discover_capabilities,
};

/// A Redis command prepared by one of this crate's tools.
///
/// The command name and arguments are exposed as byte-oriented, crate-owned
/// data so host adapters do not need to use the same Redis client library. The
/// originating tool and required access level are included for host telemetry,
/// auditing, and routing decisions. The library has already enforced its own
/// access policy before a mutating command reaches the executor.
#[derive(Clone, PartialEq, Eq)]
pub struct RedisCommand {
    tool_name: &'static str,
    required_access: AccessMode,
    required_module: Option<RedisModule>,
    name: String,
    arguments: Vec<Vec<u8>>,
    cluster_node_limit: Option<usize>,
    cluster_fanout: Option<RedisClusterFanout>,
}

/// Explicit Redis Cluster fan-out target requested by a curated tool.
///
/// Custom executors can inspect this hint to preserve the direct adapter's
/// bounded node-local semantics instead of silently collapsing replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisClusterFanout {
    /// Execute once on every discovered primary and replica node.
    AllNodes,
    /// Execute once on every discovered primary shard.
    Primaries,
}

impl RedisCommand {
    pub(crate) fn new(
        tool_name: &'static str,
        required_access: AccessMode,
        name: impl Into<String>,
    ) -> Self {
        Self {
            tool_name,
            required_access,
            required_module: None,
            name: name.into(),
            arguments: Vec::new(),
            cluster_node_limit: None,
            cluster_fanout: None,
        }
    }

    pub(crate) fn arg(&mut self, argument: impl Into<Vec<u8>>) -> &mut Self {
        self.arguments.push(argument.into());
        self
    }

    pub(crate) fn args<I, T>(&mut self, arguments: I) -> &mut Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Vec<u8>>,
    {
        self.arguments.extend(arguments.into_iter().map(Into::into));
        self
    }

    pub(crate) fn require_module(&mut self, module: RedisModule) -> &mut Self {
        self.required_module = Some(module);
        self
    }

    /// Request one raw response from every Redis Cluster node, rejecting the
    /// aggregation when the discovered topology exceeds `max_nodes`.
    ///
    /// Direct standalone adapters ignore this hint. Custom executors can use
    /// the public getter to provide the same bounded cluster semantics.
    pub(crate) fn aggregate_cluster_nodes(&mut self, max_nodes: usize) -> &mut Self {
        self.cluster_node_limit = Some(max_nodes);
        self.cluster_fanout = Some(RedisClusterFanout::AllNodes);
        self
    }

    /// Request one raw response from every Redis Cluster primary, rejecting
    /// the aggregation when the discovered topology exceeds `max_nodes`.
    pub(crate) fn aggregate_cluster_primaries(&mut self, max_nodes: usize) -> &mut Self {
        self.cluster_node_limit = Some(max_nodes);
        self.cluster_fanout = Some(RedisClusterFanout::Primaries);
        self
    }

    /// Name of the MCP tool that produced this command.
    pub fn tool_name(&self) -> &'static str {
        self.tool_name
    }

    /// Minimum library access mode required by the originating tool.
    pub fn required_access(&self) -> AccessMode {
        self.required_access
    }

    /// Optional Redis capability required to execute this command.
    pub fn required_module(&self) -> Option<RedisModule> {
        self.required_module
    }

    /// Uppercase Redis command name without arguments.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Binary-safe command arguments in wire order.
    pub fn arguments(&self) -> &[Vec<u8>] {
        &self.arguments
    }

    /// Maximum cluster nodes the originating tool permits for an explicit
    /// all-node aggregation, when one was requested.
    pub fn cluster_node_limit(&self) -> Option<usize> {
        self.cluster_node_limit
    }

    /// Node set requested for an explicitly bounded Cluster fan-out.
    pub fn cluster_fanout(&self) -> Option<RedisClusterFanout> {
        self.cluster_fanout
    }
}

impl fmt::Debug for RedisCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisCommand")
            .field("tool_name", &self.tool_name)
            .field("required_access", &self.required_access)
            .field("required_module", &self.required_module)
            .field("name", &self.name)
            .field("argument_count", &self.arguments.len())
            .field("cluster_node_limit", &self.cluster_node_limit)
            .field("cluster_fanout", &self.cluster_fanout)
            .finish()
    }
}

/// A crate-owned representation of RESP2 and RESP3 values.
///
/// Byte strings remain binary-safe. Map and attribute entries retain their
/// original order because Redis responses do not require map keys to be JSON
/// strings.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RedisValue {
    Nil,
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<Self>),
    SimpleString(String),
    Okay,
    Map(Vec<(Self, Self)>),
    Attribute {
        data: Box<Self>,
        attributes: Vec<(Self, Self)>,
    },
    Set(Vec<Self>),
    Double(f64),
    Boolean(bool),
    VerbatimString {
        format: String,
        text: String,
    },
    BigNumber(Vec<u8>),
    Push {
        kind: String,
        data: Vec<Self>,
    },
    /// Raw, address-tagged replies from an explicitly bounded Redis Cluster
    /// fan-out. Entries are sorted by node address by the direct adapter.
    ClusterNodes(Vec<(String, Self)>),
    ServerError {
        code: String,
        message: Option<String>,
    },
    /// Forward-compatible representation for a value not understood by this
    /// version of the library's bundled Redis client.
    Unsupported(String),
}

// Temporary public compatibility for the legacy cluster/session adapters and
// external integration fixtures; integration builds do not enable cfg(test).
#[cfg(test)]
impl From<redis::Value> for RedisValue {
    fn from(value: redis::Value) -> Self {
        match value {
            redis::Value::Nil => Self::Nil,
            redis::Value::Int(value) => Self::Integer(value),
            redis::Value::BulkString(value) => Self::BulkString(value),
            redis::Value::Array(values) => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            redis::Value::SimpleString(value) => Self::SimpleString(value),
            redis::Value::Okay => Self::Okay,
            redis::Value::Map(values) => Self::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            ),
            redis::Value::Attribute { data, attributes } => Self::Attribute {
                data: Box::new(Self::from(*data)),
                attributes: attributes
                    .into_iter()
                    .map(|(key, value)| (Self::from(key), Self::from(value)))
                    .collect(),
            },
            redis::Value::Set(values) => Self::Set(values.into_iter().map(Self::from).collect()),
            redis::Value::Double(value) => Self::Double(value),
            redis::Value::Boolean(value) => Self::Boolean(value),
            redis::Value::VerbatimString { format, text } => Self::VerbatimString {
                format: format.to_string(),
                text,
            },
            redis::Value::BigNumber(value) => Self::BigNumber(value),
            redis::Value::Push { kind, data } => Self::Push {
                kind: kind.to_string(),
                data: data.into_iter().map(Self::from).collect(),
            },
            redis::Value::ServerError(error) => Self::ServerError {
                code: error.code().to_string(),
                message: error.details().map(str::to_string),
            },
            other => Self::Unsupported(format!("{other:?}")),
        }
    }
}

#[cfg(test)]
impl RedisValue {
    pub(crate) fn into_redis_rs(self) -> Result<redis::Value, RedisError> {
        match self {
            Self::Nil => Ok(redis::Value::Nil),
            Self::Integer(value) => Ok(redis::Value::Int(value)),
            Self::BulkString(value) => Ok(redis::Value::BulkString(value)),
            Self::Array(values) => values
                .into_iter()
                .map(Self::into_redis_rs)
                .collect::<Result<_, _>>()
                .map(redis::Value::Array),
            Self::SimpleString(value) => Ok(redis::Value::SimpleString(value)),
            Self::Okay => Ok(redis::Value::Okay),
            Self::Map(values) => values
                .into_iter()
                .map(|(key, value)| Ok((key.into_redis_rs()?, value.into_redis_rs()?)))
                .collect::<Result<_, RedisError>>()
                .map(redis::Value::Map),
            Self::Attribute { data, attributes } => Ok(redis::Value::Attribute {
                data: Box::new(data.into_redis_rs()?),
                attributes: attributes
                    .into_iter()
                    .map(|(key, value)| Ok((key.into_redis_rs()?, value.into_redis_rs()?)))
                    .collect::<Result<_, RedisError>>()?,
            }),
            Self::Set(values) => values
                .into_iter()
                .map(Self::into_redis_rs)
                .collect::<Result<_, _>>()
                .map(redis::Value::Set),
            Self::Double(value) => Ok(redis::Value::Double(value)),
            Self::Boolean(value) => Ok(redis::Value::Boolean(value)),
            Self::VerbatimString { format, text } => Ok(redis::Value::VerbatimString {
                format: match format.as_str() {
                    "mkd" => redis::VerbatimFormat::Markdown,
                    "txt" => redis::VerbatimFormat::Text,
                    _ => redis::VerbatimFormat::Unknown(format),
                },
                text,
            }),
            Self::BigNumber(value) => Ok(redis::Value::BigNumber(value)),
            // Typed request/response tools do not consume push values. Preserve
            // the data for generic conversion while avoiding a version-coupled
            // PushKind reconstruction.
            Self::Push { data, .. } => data
                .into_iter()
                .map(Self::into_redis_rs)
                .collect::<Result<_, _>>()
                .map(redis::Value::Array),
            Self::ClusterNodes(values) => values
                .into_iter()
                .map(|(node, value)| {
                    Ok((
                        redis::Value::BulkString(node.into_bytes()),
                        value.into_redis_rs()?,
                    ))
                })
                .collect::<Result<_, RedisError>>()
                .map(redis::Value::Map),
            Self::ServerError { code, message } => Err(RedisError::new(
                RedisErrorKind::Server,
                match message {
                    Some(message) => format!("{code}: {message}"),
                    None => code,
                },
            )),
            Self::Unsupported(value) => Err(RedisError::new(
                RedisErrorKind::InvalidResponse,
                format!("unsupported Redis response: {value}"),
            )),
        }
    }
}

/// Stable error categories available to host adapters and MCP handlers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisErrorKind {
    Authentication,
    Authorization,
    Timeout,
    Connection,
    InvalidRequest,
    InvalidResponse,
    CapabilityUnavailable,
    ModuleUnavailable,
    /// The response exceeded a configured native or MCP output budget.
    OutputLimit,
    Server,
    Other,
}

/// A Redis execution failure with a stable category independent of redis-rs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisError {
    kind: RedisErrorKind,
    message: String,
    code: Option<String>,
    output_limit: Option<RedisOutputLimit>,
}

impl RedisError {
    /// Create an executor error that can be returned by a custom host adapter.
    pub fn new(kind: RedisErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
            output_limit: None,
        }
    }

    /// Attach a stable server or adapter-specific error code.
    pub fn with_code(mut self, code: impl Into<String>) -> Self {
        self.code = Some(code.into());
        self
    }

    pub fn kind(&self) -> RedisErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn code(&self) -> Option<&str> {
        self.code.as_deref()
    }

    /// Machine-readable output-limit details, when this is an output-limit
    /// failure.
    pub fn output_limit(&self) -> Option<RedisOutputLimit> {
        self.output_limit
    }

    pub(crate) fn exceeded_output_limit(
        dimension: RedisOutputLimitDimension,
        actual: usize,
        limit: usize,
    ) -> Self {
        Self {
            kind: RedisErrorKind::OutputLimit,
            message: format!(
                "{} result size {actual} exceeds configured limit {limit}",
                dimension.as_str()
            ),
            code: Some("OUTPUT_LIMIT_EXCEEDED".to_string()),
            output_limit: Some(RedisOutputLimit {
                dimension,
                actual,
                limit,
            }),
        }
    }

    pub(crate) fn redact_for_command(self, command_name: &str) -> Self {
        Self {
            kind: self.kind,
            message: format!("{command_name} failed; executor details were redacted"),
            code: self.code,
            output_limit: self.output_limit,
        }
    }

    pub(crate) fn classify_module_requirement(
        self,
        module: Option<RedisModule>,
        command_name: &str,
    ) -> Self {
        let Some(module) = module else {
            return self;
        };
        if self.kind != RedisErrorKind::Server
            || !self
                .message
                .to_ascii_lowercase()
                .contains("unknown command")
        {
            return self;
        }
        Self::new(
            RedisErrorKind::ModuleUnavailable,
            format!(
                "{} is unavailable or does not support {command_name}",
                module.display_name()
            ),
        )
        .with_code("MODULE_UNAVAILABLE")
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = &self.code {
            write!(formatter, "{code}: {}", self.message)
        } else {
            formatter.write_str(&self.message)
        }
    }
}

impl Error for RedisError {}

#[cfg(test)]
impl From<redis::RedisError> for RedisError {
    fn from(error: redis::RedisError) -> Self {
        let kind = if error.is_timeout() {
            RedisErrorKind::Timeout
        } else {
            match error.kind() {
                RedisRsErrorKind::AuthenticationFailed => RedisErrorKind::Authentication,
                RedisRsErrorKind::Server(RedisRsServerErrorKind::NoPerm) => {
                    RedisErrorKind::Authorization
                }
                RedisRsErrorKind::Server(RedisRsServerErrorKind::CrossSlot) => {
                    RedisErrorKind::InvalidRequest
                }
                RedisRsErrorKind::Io
                | RedisRsErrorKind::MasterNameNotFoundBySentinel
                | RedisRsErrorKind::NoValidReplicasFoundBySentinel
                | RedisRsErrorKind::EmptySentinelList
                | RedisRsErrorKind::ClusterConnectionNotFound => RedisErrorKind::Connection,
                RedisRsErrorKind::InvalidClientConfig | RedisRsErrorKind::Client => {
                    RedisErrorKind::InvalidRequest
                }
                RedisRsErrorKind::Parse
                | RedisRsErrorKind::UnexpectedReturnType
                | RedisRsErrorKind::RESP3NotSupported => RedisErrorKind::InvalidResponse,
                RedisRsErrorKind::Server(_) | RedisRsErrorKind::Extension => RedisErrorKind::Server,
                _ => RedisErrorKind::Other,
            }
        };
        let code = error.code().map(str::to_string);
        let mut converted = Self::new(kind, error.to_string());
        converted.code = code;
        converted
    }
}

/// Executes one Redis command for an MCP tool.
///
/// Hosts such as redisctl can supply profile-aware routing, credentials,
/// telemetry, or their own connection lifecycle without exposing those
/// concerns in tool schemas or depending on this crate's Redis client library.
#[async_trait]
pub trait RedisExecutor: Send + Sync + 'static {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError>;
}

#[async_trait]
impl<T> RedisExecutor for Arc<T>
where
    T: RedisExecutor + ?Sized,
{
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        self.as_ref().execute(command).await
    }
}

// The raw response boundary intentionally rejects only top-level server errors.
// Nested errors (notably TS.MADD entries) remain ordinary RedisValue data.
impl Command for RedisCommand {
    type Response = RedisValue;

    fn to_frame(&self) -> Frame {
        let mut parts = Vec::with_capacity(self.arguments.len() + 1);
        parts.push(Frame::BulkString(Some(self.name.clone().into())));
        parts.extend(
            self.arguments
                .iter()
                .cloned()
                .map(|arg| Frame::BulkString(Some(arg.into()))),
        );
        Frame::Array(Some(parts))
    }

    fn parse_response(&self, frame: Frame) -> Result<RedisValue, redis_tower_core::RedisError> {
        match frame {
            Frame::Error(error) | Frame::BlobError(error) => Err(
                redis_tower_core::RedisError::Redis(String::from_utf8_lossy(&error).into_owned()),
            ),
            value => Ok(value.into()),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A fixed Redis target backed by redis-tower's reconnecting multiplexed client.
#[derive(Clone)]
pub struct DirectRedis {
    connection: MultiplexedClient,
}

impl DirectRedis {
    /// Connect to a fixed Redis URL, with a ten-second connection setup bound.
    ///
    /// Reconnection replays URL authentication, database and protocol settings.
    /// Each setup attempt (including TLS and authentication) is bounded. Ordinary
    /// commands have no transport response timeout; the library invocation
    /// deadline remains authoritative. In-flight writes are not blindly retried.
    pub async fn connect(url: &str) -> Result<Self, RedisError> {
        Self::connect_with_setup(url, crate::ConnectionSetup::default()).await
    }

    /// Connect with replayable, connection-local setup for every physical socket.
    ///
    /// The caller owns the chosen client identity. Setup failures close the
    /// socket and fail connection establishment; names are not logged in errors.
    pub async fn connect_with_setup(
        url: &str,
        setup: crate::ConnectionSetup,
    ) -> Result<Self, RedisError> {
        let reconnect = ReconnectConfig::default().connect_timeout(Duration::from_secs(10));
        let connection = MultiplexedClient::from_factory(
            crate::response::connection_factory(url)?.with_connection_setup(setup),
            AutoPipelineConfig {
                response_timeout: None,
                ..AutoPipelineConfig::default()
            },
            AutoPipelineReconnectConfig::new(reconnect),
        )
        .await
        .map_err(RedisError::from)?;
        Ok(Self { connection })
    }

    /// Wrap an existing redis-tower multiplexed client.
    ///
    /// The caller controls its setup bounds, reconnection and pipeline policy.
    /// Use a factory-backed client for reconnection; clients constructed from a
    /// single connection do not reconnect. Avoid response timeouts shorter than
    /// the library command deadline.
    pub fn from_multiplexed_client(connection: MultiplexedClient) -> Self {
        Self { connection }
    }

    /// Discover the target's Redis version, deployment mode, known modules,
    /// and commands used by this library under a bounded total timeout.
    pub async fn discover_capabilities(&self) -> Result<RedisCapabilities, RedisError> {
        self.discover_capabilities_with_timeout(DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT)
            .await
    }

    /// Discover target capabilities with a caller-selected total timeout.
    pub async fn discover_capabilities_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<RedisCapabilities, RedisError> {
        discover_capabilities(self, timeout, RedisDeployment::Standalone).await
    }
}

#[async_trait]
impl RedisExecutor for DirectRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let required_module = command.required_module();
        let name = command.name().to_owned();
        self.connection
            .execute(command)
            .await
            .map_err(RedisError::from)
            .map_err(|error| error.classify_module_requirement(required_module, &name))
    }
}

/// A fixed Redis Cluster target backed by redis-tower's multiplexed router.
///
/// The adapter discovers topology from one or more seed URLs and handles
/// `MOVED`/`ASK` redirections, topology refreshes, and supported multi-slot
/// commands. Cluster selection remains server configuration and never appears
/// in MCP tool schemas.
#[derive(Clone)]
pub struct DirectRedisCluster {
    connection: MultiplexedClusterClient,
}

impl DirectRedisCluster {
    /// Connect to a Redis Cluster through one or more seed URLs.
    ///
    /// All seeds must use compatible authentication, TLS, and RESP settings.
    /// Supplying multiple nodes improves initial discovery when one seed is
    /// unavailable.
    pub async fn connect<I, S>(seed_urls: I) -> Result<Self, RedisError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::connect_with_setup(seed_urls, crate::ConnectionSetup::default()).await
    }

    /// Connect with one replayable setup policy for all Cluster sockets.
    ///
    /// The policy is retained for topology discovery, redirects, replacement
    /// connections, and dedicated node sockets. Every seed shares it.
    pub async fn connect_with_setup<I, S>(
        seed_urls: I,
        setup: crate::ConnectionSetup,
    ) -> Result<Self, RedisError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let targets = seed_urls
            .into_iter()
            .map(|url| {
                Target::parse(url.as_ref())
                    .map(|target| target.with_connection_setup(setup.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let connection = connect_cluster(&targets).await?;
        Ok(Self { connection })
    }

    /// Wrap an existing redis-tower Cluster client. Its authentication,
    /// protocol, routing, and lifecycle settings remain caller-owned.
    pub fn from_multiplexed_client(connection: MultiplexedClusterClient) -> Self {
        Self { connection }
    }

    /// Discover capabilities through the cluster adapter and mark the target
    /// deployment as Redis Cluster.
    pub async fn discover_capabilities(&self) -> Result<RedisCapabilities, RedisError> {
        self.discover_capabilities_with_timeout(DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT)
            .await
    }

    /// Discover cluster capabilities with a caller-selected total timeout.
    pub async fn discover_capabilities_with_timeout(
        &self,
        timeout: Duration,
    ) -> Result<RedisCapabilities, RedisError> {
        discover_capabilities(self, timeout, RedisDeployment::Cluster).await
    }
}

fn counted_cluster_keys(
    command: &RedisCommand,
    count_index: usize,
    first_key_index: usize,
) -> Result<&[Vec<u8>], RedisError> {
    let count = command
        .arguments()
        .get(count_index)
        .and_then(|count| std::str::from_utf8(count).ok())
        .and_then(|count| count.parse::<usize>().ok())
        .filter(|count| *count > 0)
        .ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!("{} contained an invalid key count", command.name()),
            )
        })?;
    command
        .arguments()
        .get(first_key_index..first_key_index.saturating_add(count))
        .ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "{} key count exceeded the supplied key arguments",
                    command.name()
                ),
            )
        })
}

fn declared_script_keys(command: &RedisCommand) -> Result<&[Vec<u8>], RedisError> {
    let count = command
        .arguments()
        .get(1)
        .and_then(|count| std::str::from_utf8(count).ok())
        .and_then(|count| count.parse::<usize>().ok())
        .ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!("{} contained an invalid key count", command.name()),
            )
        })?;
    command
        .arguments()
        .get(2..2_usize.saturating_add(count))
        .ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                format!(
                    "{} key count exceeded the supplied key arguments",
                    command.name()
                ),
            )
        })
}

pub(crate) fn validate_same_cluster_slot<'a>(
    keys: impl IntoIterator<Item = &'a [u8]>,
    context: &str,
) -> Result<(), RedisError> {
    let mut keys = keys.into_iter();
    let Some(first) = keys.next() else {
        return Ok(());
    };
    let first = slot_for_key(first);
    if keys.any(|key| slot_for_key(key) != first) {
        Err(
            RedisError::new(RedisErrorKind::InvalidRequest, context.to_string())
                .with_code("CROSSSLOT"),
        )
    } else {
        Ok(())
    }
}

pub(crate) fn validate_cluster_command_slots(command: &RedisCommand) -> Result<(), RedisError> {
    let route = |key: &[u8]| slot_for_key(key);
    let command_name = command.name().to_ascii_uppercase();
    match command_name.as_str() {
        "EVAL" | "EVALSHA" | "EVALSHA_RO" | "EVAL_RO" | "FCALL" | "FCALL_RO" => {
            let keys = declared_script_keys(command)?;
            validate_same_cluster_slot(
                keys.iter().map(Vec::as_slice),
                "declared script keys must hash to the same Redis Cluster slot",
            )?;
        }
        "SDIFF" | "SINTER" | "SUNION" => validate_same_cluster_slot(
            command.arguments().iter().map(Vec::as_slice),
            "source keys must hash to the same Redis Cluster slot",
        )?,
        "SDIFFCARD" | "SINTERCARD" | "SUNIONCARD" | "ZDIFF" | "ZINTER" | "ZINTERCARD"
        | "ZUNION" => {
            let keys = counted_cluster_keys(command, 0, 1)?;
            validate_same_cluster_slot(
                keys.iter().map(Vec::as_slice),
                "source keys must hash to the same Redis Cluster slot",
            )?;
        }
        "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => validate_same_cluster_slot(
            command.arguments().iter().map(Vec::as_slice),
            "destination and source keys must hash to the same Redis Cluster slot",
        )?,
        "TS.CREATERULE" | "TS.DELETERULE" if command.arguments().len() >= 2 => {
            validate_same_cluster_slot(
                command.arguments()[..2].iter().map(Vec::as_slice),
                "source and destination series must hash to the same Redis Cluster slot",
            )?
        }
        "TS.MADD" => validate_same_cluster_slot(
            command.arguments().iter().step_by(3).map(Vec::as_slice),
            "TS.MADD keys must hash to the same Redis Cluster slot",
        )?,
        "ZDIFFSTORE" | "ZINTERSTORE" | "ZUNIONSTORE" => {
            let sources = counted_cluster_keys(command, 1, 2)?;
            let destination = command.arguments().first().ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    format!("{} is missing its destination key", command.name()),
                )
            })?;
            validate_same_cluster_slot(
                std::iter::once(destination.as_slice()).chain(sources.iter().map(Vec::as_slice)),
                "destination and source keys must hash to the same Redis Cluster slot",
            )?;
        }
        "ZRANGESTORE" if command.arguments().len() >= 2 => validate_same_cluster_slot(
            command.arguments()[..2].iter().map(Vec::as_slice),
            "destination and source in ZRANGESTORE must hash to the same Redis Cluster slot",
        )?,
        _ => {}
    }
    if command.name().eq_ignore_ascii_case("JSON.MGET") && command.arguments().len() >= 3 {
        let keys = &command.arguments()[..command.arguments().len() - 1];
        let first = route(&keys[0]);
        if keys.iter().skip(1).any(|key| route(key) != first) {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "keys in JSON.MGET must hash to the same Redis Cluster slot",
            )
            .with_code("CROSSSLOT"));
        }
    }
    if command.name().eq_ignore_ascii_case("LMOVEM")
        && command.arguments().len() >= 2
        && route(&command.arguments()[0]) != route(&command.arguments()[1])
    {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "source and destination in LMOVEM must hash to the same Redis Cluster slot",
        )
        .with_code("CROSSSLOT"));
    }
    if command.name().eq_ignore_ascii_case("MSETEX") && !command.arguments().is_empty() {
        let count = std::str::from_utf8(&command.arguments()[0])
            .ok()
            .and_then(|count| count.parse::<usize>().ok())
            .filter(|count| *count > 0)
            .ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    "MSETEX contained an invalid key count",
                )
            })?;
        let pair_end = count
            .checked_mul(2)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    "MSETEX key count exceeded the supported argument range",
                )
            })?;
        let pair_arguments = command.arguments().get(1..pair_end).ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                "MSETEX key count exceeded the supplied key/value arguments",
            )
        })?;
        if let Some(first_key) = pair_arguments.first() {
            let first = route(first_key);
            if pair_arguments
                .iter()
                .step_by(2)
                .skip(1)
                .any(|key| route(key) != first)
            {
                return Err(RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    "keys in MSETEX must hash to the same Redis Cluster slot for atomic execution",
                )
                .with_code("CROSSSLOT"));
            }
        }
    }
    if command.name().eq_ignore_ascii_case("SORT")
        && !command.arguments().is_empty()
        && let Some(store_position) = command
            .arguments()
            .iter()
            .position(|argument| argument.eq_ignore_ascii_case(b"STORE"))
    {
        let destination = command.arguments().get(store_position + 1).ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                "SORT STORE is missing its destination key",
            )
        })?;
        if route(&command.arguments()[0]) != route(destination) {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "source and destination in SORT STORE must hash to the same Redis Cluster slot",
            )
            .with_code("CROSSSLOT"));
        }
    }
    if command.name().eq_ignore_ascii_case("SORT") || command.name().eq_ignore_ascii_case("SORT_RO")
    {
        let mut arguments = command.arguments().iter().skip(1);
        while let Some(argument) = arguments.next() {
            if argument.eq_ignore_ascii_case(b"BY") {
                let pattern = arguments.next().ok_or_else(|| {
                    RedisError::new(
                        RedisErrorKind::InvalidRequest,
                        "SORT BY is missing its pattern",
                    )
                })?;
                if !pattern.eq_ignore_ascii_case(b"nosort") {
                    return Err(RedisError::new(
                        RedisErrorKind::InvalidRequest,
                        "SORT external-key BY patterns are unavailable on Redis Cluster",
                    )
                    .with_code("CLUSTER_SORT_EXTERNAL_KEYS_UNSUPPORTED"));
                }
            } else if argument.eq_ignore_ascii_case(b"GET") {
                let pattern = arguments.next().ok_or_else(|| {
                    RedisError::new(
                        RedisErrorKind::InvalidRequest,
                        "SORT GET is missing its pattern",
                    )
                })?;
                if pattern != b"#" {
                    return Err(RedisError::new(
                        RedisErrorKind::InvalidRequest,
                        "SORT external-key GET patterns are unavailable on Redis Cluster",
                    )
                    .with_code("CLUSTER_SORT_EXTERNAL_KEYS_UNSUPPORTED"));
                }
            }
        }
    }
    if matches!(
        command.name().to_ascii_uppercase().as_str(),
        "FT.ALIASADD" | "FT.ALIASUPDATE"
    ) && command.arguments().len() >= 2
        && route(&command.arguments()[0]) != route(&command.arguments()[1])
    {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "the alias and index must hash to the same Redis Cluster slot",
        )
        .with_code("CROSSSLOT"));
    }
    if command.name().eq_ignore_ascii_case("FT.CREATE") && !command.arguments().is_empty() {
        let Some(prefix_position) = command
            .arguments()
            .iter()
            .position(|argument| argument.eq_ignore_ascii_case(b"PREFIX"))
        else {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "FT.CREATE on Redis Cluster requires explicit prefixes sharing the index hash slot",
            )
            .with_code("CLUSTER_SEARCH_PREFIX_REQUIRED"));
        };
        let count = command
            .arguments()
            .get(prefix_position + 1)
            .and_then(|count| std::str::from_utf8(count).ok())
            .and_then(|count| count.parse::<usize>().ok())
            .ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    "FT.CREATE contained an invalid PREFIX count",
                )
            })?;
        let prefixes = command
            .arguments()
            .get(prefix_position + 2..prefix_position + 2 + count)
            .ok_or_else(|| {
                RedisError::new(
                    RedisErrorKind::InvalidRequest,
                    "FT.CREATE PREFIX count exceeded the supplied arguments",
                )
            })?;
        let index_route = route(&command.arguments()[0]);
        if prefixes.iter().any(|prefix| route(prefix) != index_route) {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "FT.CREATE prefixes must hash to the same Redis Cluster slot as the index",
            )
            .with_code("CROSSSLOT"));
        }
    }
    Ok(())
}

#[cfg(test)]
fn cluster_routing_key(command: &RedisCommand) -> Option<&[u8]> {
    // Exercise the actual dependency router in the existing golden routing tests.
    let frame = command.to_frame();
    let key = redis_tower_cluster::key_extractor::extract_key(&frame)?;
    command
        .arguments()
        .iter()
        .find(|argument| argument.as_slice() == key)
        .map(Vec::as_slice)
}

async fn execute_cluster_command(
    connection: &MultiplexedClusterClient,
    command: RedisCommand,
) -> Result<RedisValue, RedisError> {
    let required_module = command.required_module();
    let command_name = command.name().to_owned();
    let execute = async {
        if let Some(max_nodes) = command.cluster_node_limit() {
            let topology = connection.topology().await;
            let mut nodes = topology
                .master_addrs()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            if command
                .cluster_fanout()
                .unwrap_or(RedisClusterFanout::AllNodes)
                == RedisClusterFanout::AllNodes
            {
                nodes.extend(topology.replica_addrs().into_iter().cloned());
            }
            nodes.sort_by_key(|node| node.addr_string());
            nodes.dedup();
            // Reject the entire operation before dispatching any node command.
            if nodes.len() > max_nodes {
                return Err(RedisError::new(
                    RedisErrorKind::OutputLimit,
                    format!(
                        "cluster node result size {} exceeds requested limit {max_nodes}",
                        nodes.len()
                    ),
                )
                .with_code("CLUSTER_NODE_LIMIT_EXCEEDED"));
            }
            let mut responses = Vec::with_capacity(nodes.len());
            for node in nodes {
                let mut dedicated = connection
                    .connect_to_node(node.clone())
                    .await
                    .map_err(RedisError::from)?;
                // Keep node-local server errors in-band for the existing
                // completeness/error reporting policy; transport errors fail
                // the whole operation rather than imply partial completeness.
                let mut frames = dedicated
                    .execute_pipeline(vec![command.to_frame()])
                    .await
                    .map_err(RedisError::from)?;
                responses.push((node.addr_string(), RedisValue::from(frames.remove(0))));
            }
            return Ok(RedisValue::ClusterNodes(responses));
        }
        match command.name().to_ascii_uppercase().as_str() {
            "MGET" if !command.arguments().is_empty() => {
                let values = connection
                    .mget_split(command.arguments())
                    .await
                    .map_err(RedisError::from)?;
                Ok(RedisValue::Array(
                    values
                        .into_iter()
                        .map(|value| {
                            value.map_or(RedisValue::Nil, |value| {
                                RedisValue::BulkString(value.to_vec())
                            })
                        })
                        .collect(),
                ))
            }
            "MSET"
                if !command.arguments().is_empty()
                    && command.arguments().len().is_multiple_of(2) =>
            {
                connection
                    .mset_split(
                        command
                            .arguments()
                            .chunks_exact(2)
                            .map(|pair| (&pair[0], &pair[1])),
                    )
                    .await
                    .map_err(RedisError::from)?;
                Ok(RedisValue::Okay)
            }
            "DEL" if !command.arguments().is_empty() => connection
                .del_split(command.arguments())
                .await
                .map(RedisValue::Integer)
                .map_err(RedisError::from),
            "UNLINK" | "EXISTS" | "TOUCH" if !command.arguments().is_empty() => {
                let mut groups = std::collections::BTreeMap::<u16, Vec<Vec<u8>>>::new();
                for key in command.arguments() {
                    groups
                        .entry(slot_for_key(key))
                        .or_default()
                        .push(key.clone());
                }
                let mut total = 0_i64;
                for arguments in groups.into_values() {
                    let mut group = command.clone();
                    group.arguments = arguments;
                    match connection.execute(group).await.map_err(RedisError::from)? {
                        RedisValue::Integer(count) => {
                            total = total.checked_add(count).ok_or_else(|| {
                                RedisError::new(
                                    RedisErrorKind::InvalidResponse,
                                    "Cluster count overflow",
                                )
                            })?
                        }
                        _ => {
                            return Err(RedisError::new(
                                RedisErrorKind::InvalidResponse,
                                "Cluster count reply was not an integer",
                            ));
                        }
                    }
                }
                Ok(RedisValue::Integer(total))
            }
            _ => connection.execute(command).await.map_err(RedisError::from),
        }
    };
    execute
        .await
        .map_err(|error| error.classify_module_requirement(required_module, &command_name))
}

#[async_trait]
impl RedisExecutor for DirectRedisCluster {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        validate_cluster_command_slots(&command)?;
        execute_cluster_command(&self.connection, command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    mod core_transport {
        use super::*;
        use redis_server_wrapper::{RedisServer, RedisServerHandle};

        async fn server() -> Option<(RedisServerHandle, tempfile::TempDir, String)> {
            let directory = tempfile::tempdir().unwrap();
            let server = RedisServer::new()
                .auto_port()
                .bind("127.0.0.1")
                .dir(directory.path())
                .no_stack_modules()
                .start()
                .await;
            let server = match server {
                Ok(server) => server,
                Err(redis_server_wrapper::Error::BinaryNotFound { binary }) => {
                    eprintln!("skipping local transport test: {binary} is not on PATH");
                    return None;
                }
                Err(error) => panic!("start isolated Redis: {error}"),
            };
            let url = format!("redis://127.0.0.1:{}/", server.port());
            Some((server, directory, url))
        }

        fn command(name: &str, args: &[&[u8]]) -> RedisCommand {
            let mut command = RedisCommand::new("transport_test", AccessMode::Full, name);
            command.args(args.iter().copied());
            command
        }

        #[tokio::test]
        async fn binary_null_nested_errors_and_protocols() {
            let Some((_server, _directory, url)) = server().await else {
                return;
            };
            for protocol in ["resp2", "resp3"] {
                let client = DirectRedis::connect(&format!("{url}?protocol={protocol}"))
                    .await
                    .unwrap();
                client
                    .execute(command("SET", &[b"binary", &[0xff, 0, 0x80]]))
                    .await
                    .unwrap();
                assert_eq!(
                    client.execute(command("GET", &[b"binary"])).await.unwrap(),
                    RedisValue::BulkString(vec![0xff, 0, 0x80])
                );
                assert_eq!(
                    client.execute(command("GET", &[b"missing"])).await.unwrap(),
                    RedisValue::Nil
                );
                let result = client
                    .execute(command(
                        "EVAL",
                        &[
                            b"return {7, redis.error_reply('ERR per-entry failure')}",
                            b"0",
                        ],
                    ))
                    .await
                    .unwrap();
                assert!(
                    matches!(result, RedisValue::Array(ref values) if values.len() == 2 && matches!(values[1], RedisValue::ServerError { .. }))
                );
                let error = client
                    .execute(command(
                        "EVAL",
                        &[b"return redis.error_reply('ERR top-level failure')", b"0"],
                    ))
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), RedisErrorKind::Server);
                assert_eq!(error.code(), Some("ERR"));
                let error = client
                    .execute(command("HGET", &[b"binary", b"field"]))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), Some("WRONGTYPE"));
            }
        }

        #[tokio::test]
        async fn ordinary_command_can_exceed_half_a_second() {
            let Some((_server, _directory, url)) = server().await else {
                return;
            };
            let client = DirectRedis::connect(&url).await.unwrap();
            let started = std::time::Instant::now();
            let value = tokio::time::timeout(
                Duration::from_secs(3),
                client.execute(command("BLPOP", &[b"empty", b"0.7"])),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(value, RedisValue::Nil);
            assert!(started.elapsed() >= Duration::from_millis(600));
        }

        #[tokio::test]
        async fn reconnects_after_server_closes_connection() {
            let Some((_server, _directory, url)) = server().await else {
                return;
            };
            let client = DirectRedis::connect(&url).await.unwrap();
            let RedisValue::Integer(id) =
                client.execute(command("CLIENT", &[b"ID"])).await.unwrap()
            else {
                panic!("expected client ID");
            };
            let admin = redis::Client::open(url).unwrap();
            let mut admin = admin.get_multiplexed_async_connection().await.unwrap();
            let killed: i64 = redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id)
                .query_async(&mut admin)
                .await
                .unwrap();
            assert_eq!(killed, 1);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(RedisValue::Integer(new_id)) =
                        client.execute(command("CLIENT", &[b"ID"])).await
                    {
                        assert_ne!(id, new_id);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .unwrap();
        }

        #[tokio::test]
        async fn client_name_setup_survives_reconnect() {
            let Some((_server, _directory, url)) = server().await else {
                return;
            };
            let name = b"redis-mcp-reconnect-test";
            let client = DirectRedis::connect_with_setup(
                &url,
                crate::ConnectionSetup::new().with_client_name(name),
            )
            .await
            .expect("connect with client name");
            assert_eq!(
                client
                    .execute(command("CLIENT", &[b"GETNAME"]))
                    .await
                    .unwrap(),
                RedisValue::BulkString(name.to_vec())
            );
            let RedisValue::Integer(id) =
                client.execute(command("CLIENT", &[b"ID"])).await.unwrap()
            else {
                panic!("expected client ID");
            };
            let admin = redis::Client::open(url).unwrap();
            let mut admin = admin.get_multiplexed_async_connection().await.unwrap();
            let killed: i64 = redis::cmd("CLIENT")
                .arg("KILL")
                .arg("ID")
                .arg(id)
                .query_async(&mut admin)
                .await
                .unwrap();
            assert_eq!(killed, 1);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Ok(RedisValue::Integer(new_id)) =
                        client.execute(command("CLIENT", &[b"ID"])).await
                        && new_id != id
                    {
                        assert_eq!(
                            client
                                .execute(command("CLIENT", &[b"GETNAME"]))
                                .await
                                .unwrap(),
                            RedisValue::BulkString(name.to_vec())
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("reconnect and replay client name");
        }

        #[tokio::test]
        async fn setup_including_handshake_is_bounded() {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (_socket, _) = listener.accept().await.unwrap();
                std::future::pending::<()>().await;
            });
            let result = tokio::time::timeout(
                Duration::from_secs(12),
                DirectRedis::connect(&format!("redis://{address}/")),
            )
            .await;
            peer.abort();
            let error = match result.expect("setup must finish within ten-second bound") {
                Ok(_) => panic!("silent server unexpectedly connected"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), RedisErrorKind::Timeout);
        }
    }

    #[test]
    fn command_debug_redacts_arguments() {
        let mut command = RedisCommand::new("redis_set", AccessMode::ReadWrite, "SET");
        command.arg("secret-key").arg("secret-value");
        let debug = format!("{command:?}");
        assert!(debug.contains("argument_count: 2"));
        assert!(!debug.contains("secret-key"));
        assert!(!debug.contains("secret-value"));
    }

    #[test]
    fn command_debug_exposes_only_safe_cluster_aggregation_metadata() {
        let mut command =
            RedisCommand::new("redis_pubsub_channels", AccessMode::ReadOnly, "PUBSUB");
        command
            .arg("CHANNELS")
            .arg("secret-pattern")
            .aggregate_cluster_nodes(8);
        let debug = format!("{command:?}");
        assert!(debug.contains("cluster_node_limit: Some(8)"));
        assert!(!debug.contains("secret-pattern"));
    }

    #[test]
    fn command_arguments_are_encoded_as_single_binary_safe_arguments() {
        let mut command = RedisCommand::new("redis_set", AccessMode::ReadWrite, "SET");
        command.arg("key").arg(vec![0xff, 0x00]);
        assert_eq!(
            command.to_frame(),
            Frame::Array(Some(vec![
                Frame::BulkString(Some(b"SET"[..].into())),
                Frame::BulkString(Some(b"key"[..].into())),
                Frame::BulkString(Some(vec![0xff, 0].into())),
            ]))
        );
    }

    #[test]
    fn redis_json_mget_cluster_keys_must_share_a_slot() {
        let mut same_slot = RedisCommand::new("redis_json_mget", AccessMode::ReadOnly, "JSON.MGET");
        same_slot
            .arg("doc:{tenant}:1")
            .arg("doc:{tenant}:2")
            .arg("$");
        assert!(validate_cluster_command_slots(&same_slot).is_ok());

        let mut cross_slot =
            RedisCommand::new("redis_json_mget", AccessMode::ReadOnly, "JSON.MGET");
        cross_slot
            .arg("doc:{tenant-a}:1")
            .arg("doc:{tenant-b}:2")
            .arg("$");
        let error = validate_cluster_command_slots(&cross_slot).unwrap_err();
        assert_eq!(error.kind(), RedisErrorKind::InvalidRequest);
        assert_eq!(error.code(), Some("CROSSSLOT"));
    }

    #[test]
    fn scripting_cluster_routing_uses_declared_keys_and_rejects_cross_slot_calls() {
        for command_name in [
            "EVAL",
            "EVALSHA",
            "EVAL_RO",
            "EVALSHA_RO",
            "FCALL",
            "FCALL_RO",
        ] {
            let mut command =
                RedisCommand::new("redis_scripting_test", AccessMode::Full, command_name);
            command
                .arg("subject")
                .arg("2")
                .arg("key:{tenant}:1")
                .arg("key:{tenant}:2")
                .arg("argument");
            assert!(
                validate_cluster_command_slots(&command).is_ok(),
                "{command_name}"
            );
            assert_eq!(
                cluster_routing_key(&command),
                Some(b"key:{tenant}:1".as_slice()),
                "{command_name}"
            );

            command.arguments[3] = b"key:{other}:2".to_vec();
            assert_eq!(
                validate_cluster_command_slots(&command).unwrap_err().code(),
                Some("CROSSSLOT"),
                "{command_name}"
            );
        }

        let mut keyless = RedisCommand::new("redis_eval_ro", AccessMode::ReadOnly, "EVAL_RO");
        keyless.arg("return ARGV[1]").arg("0").arg("not-a-key");
        assert!(validate_cluster_command_slots(&keyless).is_ok());
        assert_eq!(cluster_routing_key(&keyless), None);
    }

    #[test]
    fn scripting_cluster_key_counts_fail_closed_when_malformed() {
        let mut invalid_count = RedisCommand::new("redis_eval", AccessMode::Full, "EVAL");
        invalid_count.arg("return 1").arg("not-a-number");
        assert_eq!(
            validate_cluster_command_slots(&invalid_count)
                .unwrap_err()
                .kind(),
            RedisErrorKind::InvalidRequest
        );

        let mut missing_key = RedisCommand::new("redis_fcall", AccessMode::Full, "FCALL");
        missing_key.arg("lookup").arg("2").arg("only-one-key");
        assert_eq!(
            validate_cluster_command_slots(&missing_key)
                .unwrap_err()
                .kind(),
            RedisErrorKind::InvalidRequest
        );
    }

    #[test]
    fn modern_multi_key_commands_require_one_atomic_cluster_slot() {
        let mut lmovem = RedisCommand::new("redis_lmovem", AccessMode::Full, "LMOVEM");
        lmovem
            .arg("list:{tenant}:source")
            .arg("list:{tenant}:destination")
            .arg("LEFT")
            .arg("RIGHT");
        assert!(validate_cluster_command_slots(&lmovem).is_ok());

        let mut cross_slot = lmovem.clone();
        cross_slot.arguments[1] = b"list:{other}:destination".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&cross_slot)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );

        let mut msetex = RedisCommand::new("redis_msetex", AccessMode::ReadWrite, "MSETEX");
        msetex
            .arg("2")
            .arg("key:{tenant}:1")
            .arg("one")
            .arg("key:{tenant}:2")
            .arg("two")
            .arg("EX")
            .arg("30");
        assert!(validate_cluster_command_slots(&msetex).is_ok());
        assert_eq!(
            cluster_routing_key(&msetex),
            Some(b"key:{tenant}:1".as_slice())
        );

        msetex.arguments[3] = b"key:{other}:2".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&msetex).unwrap_err().code(),
            Some("CROSSSLOT")
        );

        let mut invalid = RedisCommand::new("redis_command", AccessMode::Full, "MSETEX");
        invalid.arg(usize::MAX.to_string());
        assert_eq!(
            validate_cluster_command_slots(&invalid).unwrap_err().kind(),
            RedisErrorKind::InvalidRequest
        );
    }

    #[test]
    fn set_and_sorted_set_algebra_validate_every_cluster_key() {
        let mut cardinality =
            RedisCommand::new("redis_sdiffcard", AccessMode::ReadOnly, "SDIFFCARD");
        cardinality
            .arg("2")
            .arg("set:{tenant}:left")
            .arg("set:{tenant}:right")
            .arg("LIMIT")
            .arg("10");
        assert!(validate_cluster_command_slots(&cardinality).is_ok());
        assert_eq!(
            cluster_routing_key(&cardinality),
            Some(b"set:{tenant}:left".as_slice())
        );
        cardinality.arguments[2] = b"set:{other}:right".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&cardinality)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );

        let mut set_store = RedisCommand::new("redis_sunionstore", AccessMode::Full, "SUNIONSTORE");
        set_store
            .arg("set:{tenant}:destination")
            .arg("set:{tenant}:left")
            .arg("set:{tenant}:right");
        assert!(validate_cluster_command_slots(&set_store).is_ok());
        set_store.arguments[2] = b"set:{other}:right".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&set_store)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );

        let mut weighted_store =
            RedisCommand::new("redis_zinterstore", AccessMode::Full, "ZINTERSTORE");
        weighted_store
            .arg("zset:{tenant}:destination")
            .arg("2")
            .arg("zset:{tenant}:left")
            .arg("zset:{tenant}:right")
            .arg("WEIGHTS")
            .arg("2")
            .arg("3")
            .arg("AGGREGATE")
            .arg("MAX");
        assert!(validate_cluster_command_slots(&weighted_store).is_ok());
        assert_eq!(
            cluster_routing_key(&weighted_store),
            Some(b"zset:{tenant}:destination".as_slice())
        );
        weighted_store.arguments[3] = b"zset:{other}:right".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&weighted_store)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );

        let mut range_store =
            RedisCommand::new("redis_zrangestore", AccessMode::Full, "ZRANGESTORE");
        range_store
            .arg("zset:{tenant}:destination")
            .arg("zset:{tenant}:source")
            .arg("0")
            .arg("9");
        assert!(validate_cluster_command_slots(&range_store).is_ok());
        range_store.arguments[1] = b"zset:{other}:source".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&range_store)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );
    }

    #[test]
    fn sort_cluster_routing_allows_local_patterns_and_rejects_external_keys() {
        let mut store = RedisCommand::new("redis_sort_store", AccessMode::Full, "SORT");
        store
            .arg("list:{tenant}:source")
            .arg("BY")
            .arg("nosort")
            .arg("LIMIT")
            .arg("0")
            .arg("10")
            .arg("GET")
            .arg("#")
            .arg("STORE")
            .arg("list:{tenant}:sorted");
        assert!(validate_cluster_command_slots(&store).is_ok());
        assert_eq!(
            cluster_routing_key(&store),
            Some(b"list:{tenant}:source".as_slice())
        );

        let mut cross_slot = store.clone();
        *cross_slot.arguments.last_mut().expect("STORE destination") =
            b"list:{other}:sorted".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&cross_slot)
                .unwrap_err()
                .code(),
            Some("CROSSSLOT")
        );

        let mut external_by = RedisCommand::new("redis_sort", AccessMode::ReadOnly, "SORT_RO");
        external_by
            .arg("list:{tenant}")
            .arg("BY")
            .arg("weight:*->score");
        assert_eq!(
            validate_cluster_command_slots(&external_by)
                .unwrap_err()
                .code(),
            Some("CLUSTER_SORT_EXTERNAL_KEYS_UNSUPPORTED")
        );

        let mut external_get = RedisCommand::new("redis_sort", AccessMode::ReadOnly, "SORT_RO");
        external_get
            .arg("list:{tenant}")
            .arg("GET")
            .arg("object:*->name");
        assert_eq!(
            validate_cluster_command_slots(&external_get)
                .unwrap_err()
                .code(),
            Some("CLUSTER_SORT_EXTERNAL_KEYS_UNSUPPORTED")
        );
    }

    #[test]
    fn search_cluster_indexes_aliases_and_prefixes_share_one_slot() {
        let mut create = RedisCommand::new("redis_ft_create", AccessMode::ReadWrite, "FT.CREATE");
        create
            .require_module(RedisModule::Search)
            .arg("idx:{tenant}")
            .arg("PREFIX")
            .arg("1")
            .arg("doc:{tenant}:")
            .arg("SCHEMA")
            .arg("title")
            .arg("TEXT");
        assert!(validate_cluster_command_slots(&create).is_ok());

        let mut missing_prefix =
            RedisCommand::new("redis_ft_create", AccessMode::ReadWrite, "FT.CREATE");
        missing_prefix
            .require_module(RedisModule::Search)
            .arg("idx:{tenant}")
            .arg("SCHEMA")
            .arg("title")
            .arg("TEXT");
        assert_eq!(
            validate_cluster_command_slots(&missing_prefix)
                .unwrap_err()
                .code(),
            Some("CLUSTER_SEARCH_PREFIX_REQUIRED")
        );

        let mut alias =
            RedisCommand::new("redis_ft_aliasadd", AccessMode::ReadWrite, "FT.ALIASADD");
        alias
            .require_module(RedisModule::Search)
            .arg("alias:{tenant}")
            .arg("idx:{other}");
        assert_eq!(
            validate_cluster_command_slots(&alias).unwrap_err().code(),
            Some("CROSSSLOT")
        );
    }

    #[test]
    fn search_cluster_cursor_routes_by_index_not_subcommand() {
        let mut cursor =
            RedisCommand::new("redis_ft_cursor_read", AccessMode::ReadOnly, "FT.CURSOR");
        cursor
            .require_module(RedisModule::Search)
            .arg("READ")
            .arg("idx:{tenant}")
            .arg("7");
        assert_eq!(
            cluster_routing_key(&cursor),
            Some(b"idx:{tenant}".as_slice())
        );
    }

    #[test]
    fn timeseries_cluster_commands_route_by_key_and_enforce_same_slot_rules() {
        let mut range = RedisCommand::new("redis_ts_range", AccessMode::ReadOnly, "TS.RANGE");
        range
            .require_module(RedisModule::TimeSeries)
            .arg("ts:{tenant}:temperature")
            .arg("-")
            .arg("+");
        assert!(validate_cluster_command_slots(&range).is_ok());
        assert_eq!(
            cluster_routing_key(&range),
            Some(b"ts:{tenant}:temperature".as_slice())
        );

        // Keyless multi-series queries observe one node and must not pretend
        // to have a key route.
        let mut mrange = RedisCommand::new("redis_ts_mrange", AccessMode::ReadOnly, "TS.MRANGE");
        mrange
            .require_module(RedisModule::TimeSeries)
            .arg("-")
            .arg("+")
            .arg("FILTER")
            .arg("sensor=1");
        assert_eq!(cluster_routing_key(&mrange), None);

        let mut madd = RedisCommand::new("redis_ts_madd", AccessMode::ReadWrite, "TS.MADD");
        madd.require_module(RedisModule::TimeSeries)
            .arg("ts:{tenant}:a")
            .arg("1")
            .arg("1.5")
            .arg("ts:{tenant}:b")
            .arg("2")
            .arg("2.5");
        assert!(validate_cluster_command_slots(&madd).is_ok());
        assert_eq!(
            cluster_routing_key(&madd),
            Some(b"ts:{tenant}:a".as_slice())
        );
        madd.arguments[3] = b"ts:{other}:b".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&madd).unwrap_err().code(),
            Some("CROSSSLOT")
        );

        let mut rule = RedisCommand::new(
            "redis_ts_createrule",
            AccessMode::ReadWrite,
            "TS.CREATERULE",
        );
        rule.require_module(RedisModule::TimeSeries)
            .arg("ts:{tenant}:source")
            .arg("ts:{tenant}:hourly")
            .arg("AGGREGATION")
            .arg("avg")
            .arg("3600000");
        assert!(validate_cluster_command_slots(&rule).is_ok());
        rule.arguments[1] = b"ts:{other}:hourly".to_vec();
        assert_eq!(
            validate_cluster_command_slots(&rule).unwrap_err().code(),
            Some("CROSSSLOT")
        );
    }

    #[test]
    fn resp_values_round_trip_without_public_redis_types() {
        let value = RedisValue::Map(vec![(
            RedisValue::BulkString(vec![0xff]),
            RedisValue::Array(vec![RedisValue::Integer(7), RedisValue::Boolean(true)]),
        )]);
        let redis_value = value.clone().into_redis_rs().expect("convert to redis-rs");
        assert_eq!(RedisValue::from(redis_value), value);
    }

    #[test]
    fn cluster_node_values_convert_without_exposing_redis_rs_publicly() {
        let value = RedisValue::ClusterNodes(vec![(
            "127.0.0.1:6379".to_string(),
            RedisValue::Array(vec![RedisValue::BulkString(b"events".to_vec())]),
        )]);
        let converted = value.into_redis_rs().expect("convert cluster replies");
        assert!(matches!(converted, redis::Value::Map(entries) if entries.len() == 1));
    }

    #[test]
    fn redis_errors_have_stable_categories() {
        let error = redis::RedisError::from((
            RedisRsErrorKind::AuthenticationFailed,
            "authentication failed",
        ));
        assert_eq!(
            RedisError::from(error).kind(),
            RedisErrorKind::Authentication
        );
    }

    #[test]
    fn cross_slot_errors_are_invalid_requests_with_stable_codes() {
        let error = redis::RedisError::from((
            RedisRsErrorKind::Server(RedisRsServerErrorKind::CrossSlot),
            "keys hash to different slots",
        ));
        let error = RedisError::from(error);
        assert_eq!(error.kind(), RedisErrorKind::InvalidRequest);
        assert_eq!(error.code(), Some("CROSSSLOT"));
    }

    #[tokio::test]
    async fn cluster_executor_rejects_an_empty_seed_list() {
        let error = match DirectRedisCluster::connect(Vec::<String>::new()).await {
            Ok(_) => panic!("empty cluster seeds unexpectedly connected"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), RedisErrorKind::InvalidRequest);
        assert_eq!(error.code(), Some("EMPTY_CLUSTER_SEEDS"));
    }

    #[test]
    fn unknown_module_commands_have_a_stable_category() {
        let error = RedisError::new(
            RedisErrorKind::Server,
            "ERR unknown command 'JSON.GET', with args beginning with: 'doc'",
        )
        .classify_module_requirement(Some(RedisModule::Json), "JSON.GET");
        assert_eq!(error.kind(), RedisErrorKind::ModuleUnavailable);
        assert_eq!(error.code(), Some("MODULE_UNAVAILABLE"));
        assert!(error.message().contains("RedisJSON"));
        assert!(!error.message().contains("doc"));
    }
}
