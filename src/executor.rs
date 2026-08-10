//! Redis command execution abstractions.

use std::{error::Error, fmt};

use async_trait::async_trait;
use redis::{
    ErrorKind as RedisRsErrorKind, ServerErrorKind as RedisRsServerErrorKind,
    aio::ConnectionManager,
};

use crate::AccessMode;

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

    /// Name of the MCP tool that produced this command.
    pub fn tool_name(&self) -> &'static str {
        self.tool_name
    }

    /// Minimum library access mode required by the originating tool.
    pub fn required_access(&self) -> AccessMode {
        self.required_access
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
    Server,
    Other,
}

/// A Redis execution failure with a stable category independent of redis-rs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisError {
    kind: RedisErrorKind,
    message: String,
    code: Option<String>,
}

impl RedisError {
    /// Create an executor error that can be returned by a custom host adapter.
    pub fn new(kind: RedisErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code: None,
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
}

#[async_trait]
impl RedisExecutor for DirectRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        let mut redis_command = redis::cmd(command.name());
        for argument in command.arguments() {
            redis_command.arg(argument);
        }
        let mut connection = self.connection.clone();
        let value: redis::Value = redis_command
            .query_async(&mut connection)
            .await
            .map_err(RedisError::from)?;
        Ok(RedisValue::from(value))
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
}
