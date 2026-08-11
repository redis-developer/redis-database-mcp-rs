//! Governed native Redis command invocation for non-MCP frontends.

use std::{fmt, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value as JsonValue, json};

use crate::{
    AccessMode, CapabilityStatus, DEFAULT_COMMAND_TIMEOUT, OutputBudget, RawCommandPolicy,
    RedisCapabilities, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisModule,
    RedisValue, RedisVersion,
};

const NATIVE_TOOL_NAME: &str = "redis_native_invocation";

/// A pre-tokenized, binary-safe Redis command invocation.
#[derive(Clone, PartialEq, Eq)]
pub struct NativeRedisInvocation {
    command: Vec<u8>,
    arguments: Vec<Vec<u8>>,
}

impl NativeRedisInvocation {
    /// Construct an invocation from a command token. Validation happens when
    /// the invocation is classified or executed.
    pub fn new(command: impl Into<Vec<u8>>) -> Self {
        Self {
            command: command.into(),
            arguments: Vec::new(),
        }
    }

    /// Construct an invocation from a complete pre-tokenized argv sequence.
    pub fn from_argv<I, T>(argv: I) -> Result<Self, RedisError>
    where
        I: IntoIterator<Item = T>,
        T: Into<Vec<u8>>,
    {
        let mut argv = argv.into_iter();
        let command = argv.next().ok_or_else(|| {
            RedisError::new(
                RedisErrorKind::InvalidRequest,
                "native Redis argv must contain a command token",
            )
            .with_code("EMPTY_REDIS_ARGV")
        })?;
        Ok(Self {
            command: command.into(),
            arguments: argv.map(Into::into).collect(),
        })
    }

    /// Append one binary-safe command argument.
    pub fn arg(mut self, argument: impl Into<Vec<u8>>) -> Self {
        self.arguments.push(argument.into());
        self
    }

    /// Append binary-safe command arguments in wire order.
    pub fn args<I, T>(mut self, arguments: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Vec<u8>>,
    {
        self.arguments.extend(arguments.into_iter().map(Into::into));
        self
    }

    pub fn command(&self) -> &[u8] {
        &self.command
    }

    pub fn arguments(&self) -> &[Vec<u8>] {
        &self.arguments
    }

    pub fn argv(&self) -> impl Iterator<Item = &[u8]> {
        std::iter::once(self.command.as_slice()).chain(self.arguments.iter().map(Vec::as_slice))
    }
}

impl fmt::Debug for NativeRedisInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeRedisInvocation")
            .field("command_bytes", &self.command.len())
            .field("argument_count", &self.arguments.len())
            .finish()
    }
}

/// Policy metadata determined before a native command reaches Redis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeCommandMetadata {
    name: String,
    required_access: AccessMode,
    classified: bool,
    minimum_redis_version: Option<RedisVersion>,
    required_module: Option<RedisModule>,
    minimum_module_version: Option<RedisVersion>,
}

impl NativeCommandMetadata {
    pub(crate) fn new(
        name: String,
        required_access: AccessMode,
        classified: bool,
        minimum_redis_version: Option<RedisVersion>,
        required_module: Option<RedisModule>,
        minimum_module_version: Option<RedisVersion>,
    ) -> Self {
        Self {
            name,
            required_access,
            classified,
            minimum_redis_version,
            required_module,
            minimum_module_version,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn required_access(&self) -> AccessMode {
        self.required_access
    }

    pub fn is_classified(&self) -> bool {
        self.classified
    }

    pub fn minimum_redis_version(&self) -> Option<RedisVersion> {
        self.minimum_redis_version
    }

    pub fn required_module(&self) -> Option<RedisModule> {
        self.required_module
    }

    pub fn minimum_module_version(&self) -> Option<RedisVersion> {
        self.minimum_module_version
    }
}

/// A governed native response together with the classification used to run it.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeRedisResponse {
    metadata: NativeCommandMetadata,
    value: RedisValue,
}

impl NativeRedisResponse {
    pub fn metadata(&self) -> &NativeCommandMetadata {
        &self.metadata
    }

    pub fn value(&self) -> &RedisValue {
        &self.value
    }

    pub fn into_value(self) -> RedisValue {
        self.value
    }

    pub fn into_parts(self) -> (NativeCommandMetadata, RedisValue) {
        (self.metadata, self.value)
    }
}

/// Output-limit dimension attached to a native invocation error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisOutputLimitDimension {
    EncodedBytes,
    CollectionEntries,
}

impl RedisOutputLimitDimension {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EncodedBytes => "encoded_bytes",
            Self::CollectionEntries => "collection_entries",
        }
    }
}

/// Machine-readable native invocation output-limit details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RedisOutputLimit {
    pub(crate) dimension: RedisOutputLimitDimension,
    pub(crate) actual: usize,
    pub(crate) limit: usize,
}

impl RedisOutputLimit {
    pub const fn dimension(self) -> RedisOutputLimitDimension {
        self.dimension
    }

    pub const fn actual(self) -> usize {
        self.actual
    }

    pub const fn limit(self) -> usize {
        self.limit
    }
}

/// Public service that applies Redis command policy before invoking an executor.
#[derive(Clone)]
pub struct RedisInvocationEngine {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
    raw_command_policy: RawCommandPolicy,
    command_timeout: Duration,
    output_budget: OutputBudget,
    capabilities: Arc<RedisCapabilities>,
}

impl RedisInvocationEngine {
    pub fn builder(executor: impl RedisExecutor) -> RedisInvocationEngineBuilder {
        RedisInvocationEngineBuilder {
            executor: Arc::new(executor),
            access: AccessMode::ReadOnly,
            raw_command_policy: RawCommandPolicy::Disabled,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            output_budget: OutputBudget::default(),
            capabilities: RedisCapabilities::unknown(),
        }
    }

    pub(crate) fn from_shared(
        executor: Arc<dyn RedisExecutor>,
        access: AccessMode,
        raw_command_policy: RawCommandPolicy,
        command_timeout: Duration,
        output_budget: OutputBudget,
        capabilities: Arc<RedisCapabilities>,
    ) -> Self {
        Self {
            executor,
            access,
            raw_command_policy,
            command_timeout,
            output_budget,
            capabilities,
        }
    }

    pub fn access(&self) -> AccessMode {
        self.access
    }

    pub fn raw_command_policy(&self) -> RawCommandPolicy {
        self.raw_command_policy
    }

    pub fn command_timeout(&self) -> Duration {
        self.command_timeout
    }

    pub fn output_budget(&self) -> OutputBudget {
        self.output_budget
    }

    pub fn capabilities(&self) -> &RedisCapabilities {
        &self.capabilities
    }

    /// Classify and validate an invocation without executing it.
    pub fn classify(
        &self,
        invocation: &NativeRedisInvocation,
    ) -> Result<NativeCommandMetadata, RedisError> {
        crate::raw::classify_command(
            invocation.command(),
            invocation.arguments(),
            self.raw_command_policy,
        )
    }

    /// Execute a governed invocation and return only its crate-owned RESP value.
    pub async fn invoke(
        &self,
        invocation: NativeRedisInvocation,
    ) -> Result<RedisValue, RedisError> {
        self.invoke_with_metadata(invocation)
            .await
            .map(NativeRedisResponse::into_value)
    }

    /// Execute a governed invocation and retain its classification metadata.
    pub async fn invoke_with_metadata(
        &self,
        invocation: NativeRedisInvocation,
    ) -> Result<NativeRedisResponse, RedisError> {
        let metadata = self.classify(&invocation)?;
        self.require_access(metadata.required_access(), metadata.name())?;
        self.require_capabilities(&metadata)?;

        let mut command = RedisCommand::new(
            NATIVE_TOOL_NAME,
            metadata.required_access(),
            metadata.name().to_string(),
        );
        if let Some(module) = metadata.required_module() {
            command.require_module(module);
        }
        command.args(invocation.arguments);
        let value = self.execute(command, true).await?;
        self.require_output_budget(&value)?;
        Ok(NativeRedisResponse { metadata, value })
    }

    pub(crate) async fn execute_curated(
        &self,
        command: RedisCommand,
    ) -> Result<RedisValue, RedisError> {
        self.require_access(command.required_access(), command.name())?;
        if let Some(metadata) = crate::tool_catalog()
            .iter()
            .find(|metadata| metadata.name == command.tool_name())
        {
            self.capabilities.check_tool(*metadata)?;
        }
        self.execute(command, false).await
    }

    fn require_access(&self, required: AccessMode, command: &str) -> Result<(), RedisError> {
        if self.access.permits(required) {
            Ok(())
        } else {
            Err(RedisError::new(
                RedisErrorKind::Authorization,
                format!(
                    "{command} requires {required} access; invocation engine is {}",
                    self.access
                ),
            )
            .with_code("COMMAND_ACCESS_DENIED"))
        }
    }

    fn require_capabilities(&self, metadata: &NativeCommandMetadata) -> Result<(), RedisError> {
        if self.capabilities.command(metadata.name()) == CapabilityStatus::Unavailable {
            return Err(RedisError::new(
                RedisErrorKind::CapabilityUnavailable,
                format!(
                    "Redis command {} is unavailable on the configured target",
                    metadata.name()
                ),
            )
            .with_code("COMMAND_UNAVAILABLE"));
        }
        if let Some(minimum) = metadata.minimum_redis_version()
            && let Some(actual) = self.capabilities.redis_version()
            && actual < minimum
        {
            return Err(RedisError::new(
                RedisErrorKind::CapabilityUnavailable,
                format!(
                    "{} requires Redis {minimum} or newer; target reports {actual}",
                    metadata.name()
                ),
            )
            .with_code("REDIS_VERSION_UNAVAILABLE"));
        }
        if let Some(module) = metadata.required_module() {
            let capability = self.capabilities.module(module);
            if capability.status() == CapabilityStatus::Unavailable {
                return Err(RedisError::new(
                    RedisErrorKind::ModuleUnavailable,
                    format!("{module} is unavailable on the configured Redis target"),
                )
                .with_code("MODULE_UNAVAILABLE"));
            }
            if let Some(minimum) = metadata.minimum_module_version()
                && let Some(actual) = capability.version()
                && actual < minimum
            {
                return Err(RedisError::new(
                    RedisErrorKind::ModuleUnavailable,
                    format!(
                        "{} requires {module} {minimum} or newer; target reports {actual}",
                        metadata.name()
                    ),
                )
                .with_code("MODULE_VERSION_UNAVAILABLE"));
            }
        }
        Ok(())
    }

    async fn execute(
        &self,
        command: RedisCommand,
        redact_executor_message: bool,
    ) -> Result<RedisValue, RedisError> {
        let required_module = command.required_module();
        let command_name = command.name().to_string();
        match tokio::time::timeout(self.command_timeout, self.executor.execute(command)).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                let error =
                    error.classify_module_requirement(required_module, command_name.as_str());
                if redact_executor_message {
                    Err(error.redact_for_command(&command_name))
                } else {
                    Err(error)
                }
            }
            Err(_) => Err(RedisError::new(
                RedisErrorKind::Timeout,
                format!(
                    "{command_name} timed out after {} ms",
                    self.command_timeout.as_millis()
                ),
            )
            .with_code("COMMAND_TIMEOUT")),
        }
    }

    fn require_output_budget(&self, value: &RedisValue) -> Result<(), RedisError> {
        let entries = redis_value_collection_entries(value);
        let entry_limit = self.output_budget.max_collection_entries();
        if entries > entry_limit {
            return Err(RedisError::exceeded_output_limit(
                RedisOutputLimitDimension::CollectionEntries,
                entries,
                entry_limit,
            ));
        }
        let encoded_bytes = serde_json::to_vec(&redis_value_to_json(value))
            .map_err(|error| RedisError::new(RedisErrorKind::InvalidResponse, error.to_string()))?
            .len();
        let byte_limit = self.output_budget.max_bytes();
        if encoded_bytes > byte_limit {
            return Err(RedisError::exceeded_output_limit(
                RedisOutputLimitDimension::EncodedBytes,
                encoded_bytes,
                byte_limit,
            ));
        }
        Ok(())
    }
}

/// Builder for a governed native Redis invocation service.
pub struct RedisInvocationEngineBuilder {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
    raw_command_policy: RawCommandPolicy,
    command_timeout: Duration,
    output_budget: OutputBudget,
    capabilities: RedisCapabilities,
}

impl RedisInvocationEngineBuilder {
    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn raw_command_policy(mut self, policy: RawCommandPolicy) -> Self {
        self.raw_command_policy = policy;
        self
    }

    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    pub fn output_budget(mut self, output_budget: OutputBudget) -> Self {
        self.output_budget = output_budget;
        self
    }

    pub fn capabilities(mut self, capabilities: RedisCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn build(self) -> RedisInvocationEngine {
        self.try_build()
            .expect("Redis invocation engine configuration should be valid")
    }

    pub fn try_build(self) -> Result<RedisInvocationEngine, RedisInvocationEngineBuildError> {
        if self.command_timeout.is_zero() {
            return Err(RedisInvocationEngineBuildError::ZeroCommandTimeout);
        }
        if self.output_budget.max_bytes() == 0 {
            return Err(RedisInvocationEngineBuildError::ZeroOutputBytes);
        }
        if self.output_budget.max_collection_entries() == 0 {
            return Err(RedisInvocationEngineBuildError::ZeroOutputEntries);
        }
        Ok(RedisInvocationEngine::from_shared(
            self.executor,
            self.access,
            self.raw_command_policy,
            self.command_timeout,
            self.output_budget,
            Arc::new(self.capabilities),
        ))
    }
}

/// Invalid native invocation engine configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisInvocationEngineBuildError {
    ZeroCommandTimeout,
    ZeroOutputBytes,
    ZeroOutputEntries,
}

impl fmt::Display for RedisInvocationEngineBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCommandTimeout => {
                formatter.write_str("command timeout must be greater than zero")
            }
            Self::ZeroOutputBytes => {
                formatter.write_str("maximum output bytes must be greater than zero")
            }
            Self::ZeroOutputEntries => {
                formatter.write_str("maximum output collection entries must be greater than zero")
            }
        }
    }
}

impl std::error::Error for RedisInvocationEngineBuildError {}

pub(crate) fn redis_value_collection_entries(value: &RedisValue) -> usize {
    match value {
        RedisValue::Array(values) | RedisValue::Set(values) => {
            values.iter().fold(values.len(), |count, value| {
                count.saturating_add(redis_value_collection_entries(value))
            })
        }
        RedisValue::Map(values) => values.iter().fold(values.len(), |count, (key, value)| {
            count
                .saturating_add(redis_value_collection_entries(key))
                .saturating_add(redis_value_collection_entries(value))
        }),
        RedisValue::Attribute { data, attributes } => attributes.iter().fold(
            redis_value_collection_entries(data).saturating_add(attributes.len()),
            |count, (key, value)| {
                count
                    .saturating_add(redis_value_collection_entries(key))
                    .saturating_add(redis_value_collection_entries(value))
            },
        ),
        RedisValue::Push { data, .. } => data.iter().fold(data.len(), |count, value| {
            count.saturating_add(redis_value_collection_entries(value))
        }),
        _ => 0,
    }
}

pub(crate) fn redis_value_to_json(value: &RedisValue) -> JsonValue {
    match value {
        RedisValue::Nil => JsonValue::Null,
        RedisValue::Integer(value) => json!(value),
        RedisValue::BulkString(value) => match std::str::from_utf8(value) {
            Ok(value) => json!({ "value": value, "encoding": "utf8" }),
            Err(_) => json!({ "value": BASE64.encode(value), "encoding": "base64" }),
        },
        RedisValue::Array(values) | RedisValue::Set(values) => {
            JsonValue::Array(values.iter().map(redis_value_to_json).collect())
        }
        RedisValue::SimpleString(value) => json!(value),
        RedisValue::Okay => json!("OK"),
        RedisValue::Map(values) => JsonValue::Array(
            values
                .iter()
                .map(|(key, value)| {
                    json!({
                        "key": redis_value_to_json(key),
                        "value": redis_value_to_json(value),
                    })
                })
                .collect(),
        ),
        RedisValue::Attribute { data, attributes } => json!({
            "data": redis_value_to_json(data),
            "attributes": attributes
                .iter()
                .map(|(key, value)| json!({
                    "key": redis_value_to_json(key),
                    "value": redis_value_to_json(value),
                }))
                .collect::<Vec<_>>(),
        }),
        RedisValue::Double(value) => json!(value),
        RedisValue::Boolean(value) => json!(value),
        RedisValue::VerbatimString { format, text } => json!({ "format": format, "text": text }),
        RedisValue::BigNumber(value) => match std::str::from_utf8(value) {
            Ok(value) => json!({ "value": value, "encoding": "utf8" }),
            Err(_) => json!({ "value": BASE64.encode(value), "encoding": "base64" }),
        },
        RedisValue::Push { kind, data } => json!({
            "kind": kind,
            "data": data.iter().map(redis_value_to_json).collect::<Vec<_>>(),
        }),
        RedisValue::ServerError { code, message } => {
            json!({ "server_error": { "code": code, "message": message } })
        }
        RedisValue::Unsupported(value) => json!({ "unsupported": value }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_debug_redacts_every_token() {
        let invocation = NativeRedisInvocation::new("SET")
            .arg("secret-key")
            .arg(vec![0xff, 0x00]);
        let debug = format!("{invocation:?}");
        assert!(debug.contains("argument_count: 2"));
        assert!(!debug.contains("SET"));
        assert!(!debug.contains("secret-key"));
    }

    #[test]
    fn argv_requires_a_command_and_remains_binary_safe() {
        assert!(NativeRedisInvocation::from_argv(Vec::<Vec<u8>>::new()).is_err());
        let invocation = NativeRedisInvocation::from_argv([b"ECHO".to_vec(), vec![0xff, 0x00]])
            .expect("binary argv");
        assert_eq!(invocation.command(), b"ECHO");
        assert_eq!(invocation.arguments(), &[vec![0xff, 0x00]]);
    }

    #[test]
    fn binary_values_use_explicit_canonical_json_encoding() {
        let json = redis_value_to_json(&RedisValue::BulkString(vec![0xff, 0x00]));
        assert_eq!(json["encoding"], "base64");
        assert_eq!(json["value"], "/wA=");
    }
}
