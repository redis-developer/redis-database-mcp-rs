//! Redis command execution abstractions.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::{
    ErrorKind as RedisRsErrorKind, ServerErrorKind as RedisRsServerErrorKind,
    aio::{ConnectionLike, ConnectionManager},
    cluster::ClusterClient,
    cluster_async::ClusterConnection,
};

use crate::{
    AccessMode, DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT, RedisCapabilities, RedisDeployment,
    RedisModule, RedisOutputLimit, RedisOutputLimitDimension, capabilities::discover_capabilities,
};

/// A Redis command prepared by one of this crate's tools.
///
/// The command name and arguments are exposed as byte-oriented, crate-owned
/// data so host adapters do not need to use the same redis-rs version. The
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
    ServerError {
        code: String,
        message: Option<String>,
    },
    /// Forward-compatible representation for a value not understood by this
    /// version of the library's bundled Redis client.
    Unsupported(String),
}

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
/// concerns in tool schemas or depending on this crate's redis-rs version.
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

async fn execute_redis_command(
    mut connection: impl ConnectionLike + Send,
    command: RedisCommand,
) -> Result<RedisValue, RedisError> {
    let required_module = command.required_module();
    let command_name = command.name().to_string();
    let mut redis_command = redis::cmd(command.name());
    for argument in command.arguments() {
        redis_command.arg(argument);
    }
    let value: redis::Value = redis_command
        .query_async(&mut connection)
        .await
        .map_err(RedisError::from)
        .map_err(|error| {
            error.classify_module_requirement(required_module, command_name.as_str())
        })?;
    Ok(RedisValue::from(value))
}

/// A fixed Redis target backed by redis-rs' reconnecting connection manager.
#[derive(Clone)]
pub struct DirectRedis {
    connection: ConnectionManager,
}

impl DirectRedis {
    /// Connect to a fixed Redis URL.
    pub async fn connect(url: &str) -> Result<Self, RedisError> {
        let client = redis::Client::open(url).map_err(RedisError::from)?;
        let connection = ConnectionManager::new(client)
            .await
            .map_err(RedisError::from)?;
        Ok(Self { connection })
    }

    /// Wrap an existing redis-rs connection manager.
    ///
    /// This convenience method intentionally lives on the redis-rs-backed
    /// adapter. Implementing `RedisExecutor` itself does not require redis-rs.
    pub fn from_connection_manager(connection: ConnectionManager) -> Self {
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
        execute_redis_command(self.connection.clone(), command).await
    }
}

/// A fixed Redis Cluster target backed by redis-rs' async cluster router.
///
/// The adapter discovers topology from one or more seed URLs and handles
/// `MOVED`/`ASK` redirections, topology refreshes, and supported multi-slot
/// commands. Cluster selection remains server configuration and never appears
/// in MCP tool schemas.
#[derive(Clone)]
pub struct DirectRedisCluster {
    connection: ClusterConnection,
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
        let seed_urls = seed_urls
            .into_iter()
            .map(|url| url.as_ref().to_string())
            .collect::<Vec<_>>();
        if seed_urls.is_empty() {
            return Err(RedisError::new(
                RedisErrorKind::InvalidRequest,
                "at least one Redis Cluster seed URL is required",
            )
            .with_code("EMPTY_CLUSTER_SEEDS"));
        }
        let client = ClusterClient::new(seed_urls).map_err(RedisError::from)?;
        let connection = client
            .get_async_connection()
            .await
            .map_err(RedisError::from)?;
        Ok(Self { connection })
    }

    /// Wrap an existing redis-rs async cluster connection.
    ///
    /// This convenience method intentionally lives on the redis-rs-backed
    /// adapter. Implementing [`RedisExecutor`] itself does not require
    /// redis-rs.
    pub fn from_cluster_connection(connection: ClusterConnection) -> Self {
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

fn validate_cluster_command_slots(command: &RedisCommand) -> Result<(), RedisError> {
    if !command.name().eq_ignore_ascii_case("JSON.MGET") || command.arguments().len() < 3 {
        return Ok(());
    }

    let keys = &command.arguments()[..command.arguments().len() - 1];
    let first =
        redis::cluster_routing::Route::with_key(&keys[0], redis::cluster_routing::SlotAddr::Master);
    if keys.iter().skip(1).any(|key| {
        redis::cluster_routing::Route::with_key(key, redis::cluster_routing::SlotAddr::Master)
            != first
    }) {
        Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "keys in JSON.MGET must hash to the same Redis Cluster slot",
        )
        .with_code("CROSSSLOT"))
    } else {
        Ok(())
    }
}

#[async_trait]
impl RedisExecutor for DirectRedisCluster {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        validate_cluster_command_slots(&command)?;
        execute_redis_command(self.connection.clone(), command).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn command_arguments_are_encoded_as_single_binary_safe_arguments() {
        let mut command = RedisCommand::new("redis_set", AccessMode::ReadWrite, "SET");
        command.arg("key").arg(vec![0xff, 0x00]);
        let mut redis_command = redis::cmd(command.name());
        for argument in command.arguments() {
            redis_command.arg(argument);
        }
        assert_eq!(
            redis_command.get_packed_command(),
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$2\r\n\xff\0\r\n"
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
    fn resp_values_round_trip_without_public_redis_types() {
        let value = RedisValue::Map(vec![(
            RedisValue::BulkString(vec![0xff]),
            RedisValue::Array(vec![RedisValue::Integer(7), RedisValue::Boolean(true)]),
        )]);
        let redis_value = value.clone().into_redis_rs().expect("convert to redis-rs");
        assert_eq!(RedisValue::from(redis_value), value);
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
