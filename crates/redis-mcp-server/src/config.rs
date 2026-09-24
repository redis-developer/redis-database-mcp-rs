//! Server configuration from CLI arguments, environment variables, and an
//! explicit TOML file.
//!
//! One schema, three sources, one precedence: CLI arguments override
//! environment variables override the TOML file override built-in defaults.
//! The file is only ever loaded explicitly (`--config` or
//! `REDIS_MCP_CONFIG`), unknown TOML keys are rejected, and every non-secret
//! setting is reachable from every source. The HTTP Bearer token is
//! environment-only. Resolution is pure — the environment is
//! injected as a lookup function — so precedence is unit-testable per
//! setting.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use clap::{Parser, ValueEnum};
use redis_mcp::{
    AccessMode, DEFAULT_COMMAND_TIMEOUT, DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS,
    DEFAULT_MAX_CONCURRENT_TRANSACTIONS, DEFAULT_MAX_OUTPUT_BYTES, DEFAULT_MAX_OUTPUT_ENTRIES,
    MonitorSessionLimits, PubSubSessionLimits, RawCommandPolicy, RedisBlockingLimits,
    RedisBulkLimits, RedisDocsOptions, RedisTransactionLimits, ToolBundle, UnavailableToolPolicy,
};
use serde::Deserialize;

/// Environment lookup injected into resolution so tests never mutate global
/// process state.
pub(crate) type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CliAccessMode {
    ReadOnly,
    ReadWrite,
    Full,
}

impl From<CliAccessMode> for AccessMode {
    fn from(value: CliAccessMode) -> Self {
        match value {
            CliAccessMode::ReadOnly => Self::ReadOnly,
            CliAccessMode::ReadWrite => Self::ReadWrite,
            CliAccessMode::Full => Self::Full,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CliRawPolicy {
    Disabled,
    Classified,
    Unrestricted,
}

impl From<CliRawPolicy> for RawCommandPolicy {
    fn from(value: CliRawPolicy) -> Self {
        match value {
            CliRawPolicy::Disabled => Self::Disabled,
            CliRawPolicy::Classified => Self::Classified,
            CliRawPolicy::Unrestricted => Self::Unrestricted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CliOptionalBundle {
    Admin,
    Bulk,
    Invocation,
    Json,
    Search,
    Scripting,
    Timeseries,
}

impl From<CliOptionalBundle> for ToolBundle {
    fn from(value: CliOptionalBundle) -> Self {
        match value {
            CliOptionalBundle::Admin => Self::Admin,
            CliOptionalBundle::Bulk => Self::Bulk,
            CliOptionalBundle::Invocation => Self::Invocation,
            CliOptionalBundle::Json => Self::Json,
            CliOptionalBundle::Search => Self::Search,
            CliOptionalBundle::Scripting => Self::Scripting,
            CliOptionalBundle::Timeseries => Self::TimeSeries,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CliUnavailableTools {
    Advertise,
    Hide,
}

impl From<CliUnavailableTools> for UnavailableToolPolicy {
    fn from(value: CliUnavailableTools) -> Self {
        match value {
            CliUnavailableTools::Advertise => Self::Advertise,
            CliUnavailableTools::Hide => Self::Hide,
        }
    }
}

const PUBSUB_HEADING: &str = "Pub/Sub session limits";
const MONITOR_HEADING: &str = "MONITOR session limits";
const BLOCKING_HEADING: &str = "Blocking call limits";
const TRANSACTION_HEADING: &str = "Transaction limits";
const BULK_HEADING: &str = "Bulk workflow limits";
const DOCS_HEADING: &str = "Command documentation";
const HTTP_HEADING: &str = "Streamable HTTP transport";

pub(crate) const DEFAULT_HTTP_BIND: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080);
pub(crate) const DEFAULT_HTTP_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const DEFAULT_HTTP_MAX_CONCURRENCY: usize = 64;
pub(crate) const DEFAULT_HTTP_MAX_SESSIONS: usize = 256;
pub(crate) const DEFAULT_HTTP_SESSION_TTL: Duration = Duration::from_secs(30 * 60);
pub(crate) const DEFAULT_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub(crate) const DEFAULT_HTTP_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum CliTransport {
    Stdio,
    Http,
}

#[derive(Debug, Parser)]
#[command(name = "redis-mcp-server", version, about)]
pub(crate) struct Args {
    /// Path to a TOML configuration file. CLI arguments and environment
    /// variables override its values.
    #[arg(long)]
    pub(crate) config: Option<std::path::PathBuf>,

    /// Fixed standalone Redis target. Defaults to redis://127.0.0.1:6379.
    #[arg(long, conflicts_with = "cluster_urls")]
    pub(crate) url: Option<String>,

    /// Redis Cluster seed URL. Repeat for multiple seeds; conflicts with --url.
    #[arg(long = "cluster-url", value_delimiter = ',', conflicts_with = "url")]
    pub(crate) cluster_urls: Vec<String>,

    /// Maximum side-effect level to expose. Defaults to read-only.
    #[arg(long, value_enum)]
    pub(crate) access: Option<CliAccessMode>,

    /// Enable classified native command execution: the full-access
    /// redis_command tool and, with --enable-bundle invocation, the tiered
    /// governed argv tools at the configured access level.
    #[arg(long)]
    pub(crate) raw: bool,

    /// Expose unclassified request/response commands too. Requires full access.
    #[arg(long, conflicts_with = "raw")]
    pub(crate) raw_unrestricted: bool,

    /// Expose bounded atomic redis_transaction execution. Requires an enabled
    /// raw command policy.
    #[arg(long)]
    pub(crate) transactions: bool,

    /// Add an optional tool bundle to the curated defaults.
    #[arg(long = "enable-bundle", value_enum)]
    pub(crate) optional_bundles: Vec<CliOptionalBundle>,

    /// How to treat tools the discovered target cannot serve.
    #[arg(long, value_enum)]
    pub(crate) unavailable_tools: Option<CliUnavailableTools>,

    /// Skip Redis capability discovery at startup and advertise the full
    /// selected catalog without version or module awareness.
    #[arg(long)]
    pub(crate) no_discovery: bool,

    /// Maximum encoded bytes for one MCP tool result.
    #[arg(long)]
    pub(crate) max_output_bytes: Option<usize>,

    /// Maximum collection entries in one MCP tool result.
    #[arg(long)]
    pub(crate) max_output_entries: Option<usize>,

    /// Upper bound for one Redis command, in milliseconds.
    #[arg(long)]
    pub(crate) command_timeout_ms: Option<u64>,

    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_sessions: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_sessions_per_owner: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_subscriptions_per_session: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_buffered_messages: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_message_bytes: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_read_bytes: Option<usize>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_max_read_duration_ms: Option<u64>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_idle_timeout_ms: Option<u64>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_cleanup_interval_ms: Option<u64>,
    #[arg(long, help_heading = PUBSUB_HEADING)]
    pub(crate) pubsub_operation_timeout_ms: Option<u64>,

    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_sessions: Option<usize>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_sessions_per_owner: Option<usize>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_buffered_events: Option<usize>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_event_bytes: Option<usize>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_read_bytes: Option<usize>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_max_read_duration_ms: Option<u64>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_idle_timeout_ms: Option<u64>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_cleanup_interval_ms: Option<u64>,
    #[arg(long, help_heading = MONITOR_HEADING)]
    pub(crate) monitor_operation_timeout_ms: Option<u64>,

    #[arg(long, help_heading = BLOCKING_HEADING)]
    pub(crate) blocking_max_timeout_ms: Option<u64>,
    #[arg(long, help_heading = BLOCKING_HEADING)]
    pub(crate) blocking_max_keys: Option<usize>,
    #[arg(long, help_heading = BLOCKING_HEADING)]
    pub(crate) blocking_max_count: Option<usize>,
    #[arg(long, help_heading = BLOCKING_HEADING)]
    pub(crate) blocking_max_concurrent_calls: Option<usize>,

    #[arg(long, help_heading = TRANSACTION_HEADING)]
    pub(crate) transaction_max_commands: Option<usize>,
    #[arg(long, help_heading = TRANSACTION_HEADING)]
    pub(crate) transaction_max_watch_keys: Option<usize>,
    #[arg(long, help_heading = TRANSACTION_HEADING)]
    pub(crate) transaction_max_request_bytes: Option<usize>,
    #[arg(long, help_heading = TRANSACTION_HEADING)]
    pub(crate) transaction_max_duration_ms: Option<u64>,
    #[arg(long, help_heading = TRANSACTION_HEADING)]
    pub(crate) transaction_max_concurrent: Option<usize>,

    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_records: Option<usize>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_batch_size: Option<usize>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_concurrency: Option<usize>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_input_bytes: Option<usize>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_duration_ms: Option<u64>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_reported_failures: Option<usize>,
    #[arg(long, help_heading = BULK_HEADING)]
    pub(crate) bulk_max_batch_summaries: Option<usize>,

    /// Serve official Redis command documentation as passthrough resources.
    /// Introduces outbound HTTPS to raw.githubusercontent.com at the pinned
    /// revision; nothing is fetched without this opt-in.
    #[arg(long, help_heading = DOCS_HEADING)]
    pub(crate) enable_docs: bool,
    /// redis/docs commit documentation is served from.
    #[arg(long, help_heading = DOCS_HEADING)]
    pub(crate) docs_pin: Option<String>,
    /// Maximum bytes for one documentation page.
    #[arg(long, help_heading = DOCS_HEADING)]
    pub(crate) docs_max_bytes: Option<usize>,
    /// Fetched pages kept in the bounded in-memory cache.
    #[arg(long, help_heading = DOCS_HEADING)]
    pub(crate) docs_cache_entries: Option<usize>,
    /// Ceiling for one documentation fetch, in milliseconds.
    #[arg(long, help_heading = DOCS_HEADING)]
    pub(crate) docs_fetch_timeout_ms: Option<u64>,

    /// Serve MCP over stdio (the default transport).
    #[arg(long, conflicts_with = "http")]
    pub(crate) stdio: bool,

    /// Serve Streamable HTTP at this socket address instead of stdio.
    #[arg(long, value_name = "ADDRESS", conflicts_with = "stdio", help_heading = HTTP_HEADING)]
    pub(crate) http: Option<SocketAddr>,
    /// Explicitly acknowledge binding HTTP to a non-loopback address.
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_allow_remote: bool,
    /// Allowed HTTP Host value. Repeat or comma-separate for multiple hosts.
    #[arg(
        long = "http-allowed-host",
        value_delimiter = ',',
        help_heading = HTTP_HEADING
    )]
    pub(crate) http_allowed_hosts: Vec<String>,
    /// Allowed browser Origin. Repeat or comma-separate for multiple origins.
    #[arg(
        long = "http-allowed-origin",
        value_delimiter = ',',
        help_heading = HTTP_HEADING
    )]
    pub(crate) http_allowed_origins: Vec<String>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_max_body_bytes: Option<usize>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_max_concurrency: Option<usize>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_max_sessions: Option<usize>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_session_ttl_ms: Option<u64>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_request_timeout_ms: Option<u64>,
    #[arg(long, help_heading = HTTP_HEADING)]
    pub(crate) http_drain_timeout_ms: Option<u64>,
}

/// The explicit TOML file schema. Every field is optional; unknown keys are
/// rejected so typos fail loudly at startup instead of silently applying
/// defaults.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileConfig {
    pub(crate) target: FileTarget,
    pub(crate) server: FileServer,
    pub(crate) output: FileOutput,
    pub(crate) timeouts: FileTimeouts,
    pub(crate) docs: FileDocs,
    pub(crate) http: FileHttp,
    pub(crate) limits: FileLimits,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileTarget {
    pub(crate) url: Option<String>,
    pub(crate) cluster_urls: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileServer {
    pub(crate) transport: Option<CliTransport>,
    pub(crate) access: Option<CliAccessMode>,
    pub(crate) raw: Option<CliRawPolicy>,
    pub(crate) transactions: Option<bool>,
    pub(crate) bundles: Option<Vec<CliOptionalBundle>>,
    pub(crate) unavailable_tools: Option<CliUnavailableTools>,
    pub(crate) discover_capabilities: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileHttp {
    pub(crate) bind: Option<SocketAddr>,
    pub(crate) allow_remote: Option<bool>,
    pub(crate) allowed_hosts: Option<Vec<String>>,
    pub(crate) allowed_origins: Option<Vec<String>>,
    pub(crate) max_body_bytes: Option<usize>,
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) max_sessions: Option<usize>,
    pub(crate) session_ttl_ms: Option<u64>,
    pub(crate) request_timeout_ms: Option<u64>,
    pub(crate) drain_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileOutput {
    pub(crate) max_bytes: Option<usize>,
    pub(crate) max_entries: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileTimeouts {
    pub(crate) command_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileDocs {
    pub(crate) enabled: Option<bool>,
    pub(crate) pin: Option<String>,
    pub(crate) max_doc_bytes: Option<usize>,
    pub(crate) cache_entries: Option<usize>,
    pub(crate) fetch_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileLimits {
    pub(crate) pubsub: FilePubSubLimits,
    pub(crate) monitor: FileMonitorLimits,
    pub(crate) blocking: FileBlockingLimits,
    pub(crate) transactions: FileTransactionLimits,
    pub(crate) bulk: FileBulkLimits,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FilePubSubLimits {
    pub(crate) max_sessions: Option<usize>,
    pub(crate) max_sessions_per_owner: Option<usize>,
    pub(crate) max_subscriptions_per_session: Option<usize>,
    pub(crate) max_buffered_messages: Option<usize>,
    pub(crate) max_message_bytes: Option<usize>,
    pub(crate) max_read_bytes: Option<usize>,
    pub(crate) max_read_duration_ms: Option<u64>,
    pub(crate) idle_timeout_ms: Option<u64>,
    pub(crate) cleanup_interval_ms: Option<u64>,
    pub(crate) operation_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileMonitorLimits {
    pub(crate) max_sessions: Option<usize>,
    pub(crate) max_sessions_per_owner: Option<usize>,
    pub(crate) max_buffered_events: Option<usize>,
    pub(crate) max_event_bytes: Option<usize>,
    pub(crate) max_read_bytes: Option<usize>,
    pub(crate) max_read_duration_ms: Option<u64>,
    pub(crate) idle_timeout_ms: Option<u64>,
    pub(crate) cleanup_interval_ms: Option<u64>,
    pub(crate) operation_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileBlockingLimits {
    pub(crate) max_timeout_ms: Option<u64>,
    pub(crate) max_keys: Option<usize>,
    pub(crate) max_count: Option<usize>,
    pub(crate) max_concurrent_calls: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileTransactionLimits {
    pub(crate) max_commands: Option<usize>,
    pub(crate) max_watch_keys: Option<usize>,
    pub(crate) max_request_bytes: Option<usize>,
    pub(crate) max_duration_ms: Option<u64>,
    pub(crate) max_concurrent: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct FileBulkLimits {
    pub(crate) max_records: Option<usize>,
    pub(crate) max_batch_size: Option<usize>,
    pub(crate) max_concurrency: Option<usize>,
    pub(crate) max_input_bytes: Option<usize>,
    pub(crate) max_duration_ms: Option<u64>,
    pub(crate) max_reported_failures: Option<usize>,
    pub(crate) max_batch_summaries: Option<usize>,
}

/// The configured Redis target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerTarget {
    Standalone(String),
    Cluster(Vec<String>),
}

/// Secret material is accepted only through the environment and is always
/// redacted from diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct SecretString(Arc<str>);

impl SecretString {
    pub(crate) fn new(value: String) -> Option<Self> {
        (!value.trim().is_empty()).then(|| Self(Arc::from(value)))
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HttpConfig {
    pub(crate) bind: SocketAddr,
    pub(crate) allow_remote: bool,
    pub(crate) bearer_token: Option<SecretString>,
    pub(crate) allowed_hosts: Vec<String>,
    pub(crate) allowed_origins: Vec<String>,
    pub(crate) max_body_bytes: usize,
    pub(crate) max_concurrency: usize,
    pub(crate) max_sessions: usize,
    pub(crate) session_ttl: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) drain_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerTransport {
    Stdio,
    Http(HttpConfig),
}

/// Fully resolved, validated server configuration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ServerConfig {
    pub(crate) transport: ServerTransport,
    pub(crate) target: ServerTarget,
    pub(crate) access: AccessMode,
    pub(crate) raw_policy: RawCommandPolicy,
    pub(crate) transactions: bool,
    pub(crate) bundles: Vec<ToolBundle>,
    pub(crate) unavailable_tools: UnavailableToolPolicy,
    pub(crate) discover_capabilities: bool,
    pub(crate) max_output_bytes: usize,
    pub(crate) max_output_entries: usize,
    pub(crate) command_timeout: Duration,
    pub(crate) pubsub_limits: PubSubSessionLimits,
    pub(crate) monitor_limits: MonitorSessionLimits,
    pub(crate) blocking_limits: RedisBlockingLimits,
    pub(crate) blocking_max_concurrent_calls: usize,
    pub(crate) transaction_limits: RedisTransactionLimits,
    pub(crate) transaction_max_concurrent: usize,
    pub(crate) bulk_limits: RedisBulkLimits,
    pub(crate) docs_enabled: bool,
    pub(crate) docs_options: RedisDocsOptions,
}

/// Actionable configuration failure. Messages never contain credential
/// material; targets are described by presence, not value.
#[derive(Debug)]
pub(crate) enum ConfigError {
    UnreadableFile(std::path::PathBuf, std::io::Error),
    InvalidFile(std::path::PathBuf, toml::de::Error),
    InvalidEnv(&'static str, String),
    ConflictingTargets,
    UnrestrictedRawRequiresFullAccess,
    TransactionsRequireRawCommands,
    InvalidDocsOptions,
    InvalidHttpOptions(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnreadableFile(path, error) => {
                write!(formatter, "cannot read config file {}: {error}", path.display())
            }
            Self::InvalidFile(path, error) => {
                write!(formatter, "invalid config file {}: {error}", path.display())
            }
            Self::InvalidEnv(name, message) => {
                write!(formatter, "invalid environment variable {name}: {message}")
            }
            Self::ConflictingTargets => formatter.write_str(
                "configure either a standalone url or cluster urls, not both (check --url/--cluster-url, REDIS_URL/REDIS_CLUSTER_URLS, and [target] in the config file)",
            ),
            Self::UnrestrictedRawRequiresFullAccess => {
                formatter.write_str("unrestricted raw command execution requires full access")
            }
            Self::TransactionsRequireRawCommands => formatter.write_str(
                "transactions require an enabled raw command policy (--raw or --raw-unrestricted)",
            ),
            Self::InvalidDocsOptions => formatter.write_str(
                "enabled documentation serving requires a well-formed revision and non-zero byte, cache, and timeout bounds",
            ),
            Self::InvalidHttpOptions(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Load the TOML file selected by `--config` or `REDIS_MCP_CONFIG`.
pub(crate) fn load_file(
    args: &Args,
    env: EnvLookup<'_>,
) -> Result<Option<FileConfig>, ConfigError> {
    let path = args
        .config
        .clone()
        .or_else(|| env("REDIS_MCP_CONFIG").map(std::path::PathBuf::from));
    let Some(path) = path else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(&path)
        .map_err(|error| ConfigError::UnreadableFile(path.clone(), error))?;
    let parsed = toml::from_str::<FileConfig>(&raw)
        .map_err(|error| ConfigError::InvalidFile(path, error))?;
    Ok(Some(parsed))
}

fn env_parsed<T: std::str::FromStr>(
    env: EnvLookup<'_>,
    name: &'static str,
) -> Result<Option<T>, ConfigError> {
    match env(name) {
        None => Ok(None),
        Some(value) => value.trim().parse::<T>().map(Some).map_err(|_| {
            ConfigError::InvalidEnv(name, format!("cannot parse {value:?} as a number"))
        }),
    }
}

fn env_bool(env: EnvLookup<'_>, name: &'static str) -> Result<Option<bool>, ConfigError> {
    match env(name) {
        None => Ok(None),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(Some(true)),
            "false" | "0" | "no" | "off" => Ok(Some(false)),
            other => Err(ConfigError::InvalidEnv(
                name,
                format!("expected a boolean, found {other:?}"),
            )),
        },
    }
}

fn env_enum<T: ValueEnum>(
    env: EnvLookup<'_>,
    name: &'static str,
) -> Result<Option<T>, ConfigError> {
    match env(name) {
        None => Ok(None),
        Some(value) => T::from_str(value.trim(), true).map(Some).map_err(|_| {
            ConfigError::InvalidEnv(
                name,
                format!(
                    "expected one of [{}], found {value:?}",
                    T::value_variants()
                        .iter()
                        .filter_map(|variant| variant.to_possible_value())
                        .map(|possible| possible.get_name().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        }),
    }
}

fn env_list<T: ValueEnum>(
    env: EnvLookup<'_>,
    name: &'static str,
) -> Result<Option<Vec<T>>, ConfigError> {
    match env(name) {
        None => Ok(None),
        Some(value) => value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                T::from_str(entry, true)
                    .map_err(|_| ConfigError::InvalidEnv(name, format!("unknown entry {entry:?}")))
            })
            .collect::<Result<Vec<_>, _>>()
            .map(Some),
    }
}

fn env_string_list(env: EnvLookup<'_>, name: &'static str) -> Option<Vec<String>> {
    env(name).map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect()
    })
}

fn pick<T>(cli: Option<T>, env: Option<T>, file: Option<T>, default: T) -> T {
    cli.or(env).or(file).unwrap_or(default)
}

/// Resolve one validated configuration from the three sources.
pub(crate) fn resolve(
    args: &Args,
    file: Option<&FileConfig>,
    env: EnvLookup<'_>,
) -> Result<ServerConfig, ConfigError> {
    let defaults = FileConfig::default();
    let file = file.unwrap_or(&defaults);

    let env_http_bind = env_parsed::<SocketAddr>(env, "REDIS_MCP_HTTP_BIND")?;
    let cli_transport = if args.http.is_some() {
        Some(CliTransport::Http)
    } else if args.stdio {
        Some(CliTransport::Stdio)
    } else {
        None
    };
    let env_transport = env_enum::<CliTransport>(env, "REDIS_MCP_TRANSPORT")?
        .or_else(|| env_http_bind.is_some().then_some(CliTransport::Http));
    let file_transport = file
        .server
        .transport
        .or_else(|| file.http.bind.is_some().then_some(CliTransport::Http));
    let transport_kind = pick(
        cli_transport,
        env_transport,
        file_transport,
        CliTransport::Stdio,
    );
    let http = HttpConfig {
        bind: pick(args.http, env_http_bind, file.http.bind, DEFAULT_HTTP_BIND),
        allow_remote: pick(
            args.http_allow_remote.then_some(true),
            env_bool(env, "REDIS_MCP_HTTP_ALLOW_REMOTE")?,
            file.http.allow_remote,
            false,
        ),
        // Secrets deliberately do not have CLI or TOML forms: command-line
        // arguments are process-visible and config files are commonly checked
        // in. Operators provide the token directly through the environment.
        bearer_token: env("REDIS_MCP_HTTP_BEARER_TOKEN").and_then(SecretString::new),
        allowed_hosts: pick(
            (!args.http_allowed_hosts.is_empty()).then(|| args.http_allowed_hosts.clone()),
            env_string_list(env, "REDIS_MCP_HTTP_ALLOWED_HOSTS"),
            file.http.allowed_hosts.clone(),
            Vec::new(),
        ),
        allowed_origins: pick(
            (!args.http_allowed_origins.is_empty()).then(|| args.http_allowed_origins.clone()),
            env_string_list(env, "REDIS_MCP_HTTP_ALLOWED_ORIGINS"),
            file.http.allowed_origins.clone(),
            Vec::new(),
        ),
        max_body_bytes: pick(
            args.http_max_body_bytes,
            env_parsed(env, "REDIS_MCP_HTTP_MAX_BODY_BYTES")?,
            file.http.max_body_bytes,
            DEFAULT_HTTP_MAX_BODY_BYTES,
        ),
        max_concurrency: pick(
            args.http_max_concurrency,
            env_parsed(env, "REDIS_MCP_HTTP_MAX_CONCURRENCY")?,
            file.http.max_concurrency,
            DEFAULT_HTTP_MAX_CONCURRENCY,
        ),
        max_sessions: pick(
            args.http_max_sessions,
            env_parsed(env, "REDIS_MCP_HTTP_MAX_SESSIONS")?,
            file.http.max_sessions,
            DEFAULT_HTTP_MAX_SESSIONS,
        ),
        session_ttl: millis(pick(
            args.http_session_ttl_ms,
            env_parsed(env, "REDIS_MCP_HTTP_SESSION_TTL_MS")?,
            file.http.session_ttl_ms,
            DEFAULT_HTTP_SESSION_TTL.as_millis() as u64,
        )),
        request_timeout: millis(pick(
            args.http_request_timeout_ms,
            env_parsed(env, "REDIS_MCP_HTTP_REQUEST_TIMEOUT_MS")?,
            file.http.request_timeout_ms,
            DEFAULT_HTTP_REQUEST_TIMEOUT.as_millis() as u64,
        )),
        drain_timeout: millis(pick(
            args.http_drain_timeout_ms,
            env_parsed(env, "REDIS_MCP_HTTP_DRAIN_TIMEOUT_MS")?,
            file.http.drain_timeout_ms,
            DEFAULT_HTTP_DRAIN_TIMEOUT.as_millis() as u64,
        )),
    };
    let transport = match transport_kind {
        CliTransport::Stdio => ServerTransport::Stdio,
        CliTransport::Http => ServerTransport::Http(http),
    };

    // Target: within each source url and cluster urls are mutually
    // exclusive (clap enforces the CLI); across sources the higher-precedence
    // source wins entirely so a CLI --url overrides a file cluster target.
    let cli_target = if !args.cluster_urls.is_empty() {
        Some(ServerTarget::Cluster(args.cluster_urls.clone()))
    } else {
        args.url.clone().map(ServerTarget::Standalone)
    };
    let env_url = env("REDIS_URL").filter(|value| !value.trim().is_empty());
    let env_cluster = env("REDIS_CLUSTER_URLS").map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_string)
            .collect::<Vec<_>>()
    });
    let env_target = match (env_url, env_cluster.filter(|urls| !urls.is_empty())) {
        (Some(_), Some(_)) => return Err(ConfigError::ConflictingTargets),
        (Some(url), None) => Some(ServerTarget::Standalone(url)),
        (None, Some(urls)) => Some(ServerTarget::Cluster(urls)),
        (None, None) => None,
    };
    let file_target = match (
        file.target.url.clone(),
        file.target
            .cluster_urls
            .clone()
            .filter(|urls| !urls.is_empty()),
    ) {
        (Some(_), Some(_)) => return Err(ConfigError::ConflictingTargets),
        (Some(url), None) => Some(ServerTarget::Standalone(url)),
        (None, Some(urls)) => Some(ServerTarget::Cluster(urls)),
        (None, None) => None,
    };
    let target = pick(
        cli_target,
        env_target,
        file_target,
        ServerTarget::Standalone("redis://127.0.0.1:6379".to_string()),
    );

    let access: AccessMode = pick(
        args.access,
        env_enum::<CliAccessMode>(env, "REDIS_MCP_ACCESS")?,
        file.server.access,
        CliAccessMode::ReadOnly,
    )
    .into();

    let cli_raw = if args.raw_unrestricted {
        Some(CliRawPolicy::Unrestricted)
    } else if args.raw {
        Some(CliRawPolicy::Classified)
    } else {
        None
    };
    let raw_policy: RawCommandPolicy = pick(
        cli_raw,
        env_enum::<CliRawPolicy>(env, "REDIS_MCP_RAW")?,
        file.server.raw,
        CliRawPolicy::Disabled,
    )
    .into();

    let transactions = pick(
        args.transactions.then_some(true),
        env_bool(env, "REDIS_MCP_TRANSACTIONS")?,
        file.server.transactions,
        false,
    );

    let bundles = pick(
        (!args.optional_bundles.is_empty()).then(|| args.optional_bundles.clone()),
        env_list::<CliOptionalBundle>(env, "REDIS_MCP_BUNDLES")?,
        file.server.bundles.clone(),
        Vec::new(),
    )
    .into_iter()
    .map(ToolBundle::from)
    .collect();

    let unavailable_tools: UnavailableToolPolicy = pick(
        args.unavailable_tools,
        env_enum::<CliUnavailableTools>(env, "REDIS_MCP_UNAVAILABLE_TOOLS")?,
        file.server.unavailable_tools,
        CliUnavailableTools::Advertise,
    )
    .into();

    let discover_capabilities = pick(
        args.no_discovery.then_some(false),
        env_bool(env, "REDIS_MCP_DISCOVER_CAPABILITIES")?,
        file.server.discover_capabilities,
        true,
    );

    let max_output_bytes = pick(
        args.max_output_bytes,
        env_parsed(env, "REDIS_MCP_MAX_OUTPUT_BYTES")?,
        file.output.max_bytes,
        DEFAULT_MAX_OUTPUT_BYTES,
    );
    let max_output_entries = pick(
        args.max_output_entries,
        env_parsed(env, "REDIS_MCP_MAX_OUTPUT_ENTRIES")?,
        file.output.max_entries,
        DEFAULT_MAX_OUTPUT_ENTRIES,
    );
    let command_timeout = Duration::from_millis(pick(
        args.command_timeout_ms,
        env_parsed(env, "REDIS_MCP_COMMAND_TIMEOUT_MS")?,
        file.timeouts.command_ms,
        DEFAULT_COMMAND_TIMEOUT.as_millis() as u64,
    ));

    let defaults_pubsub = PubSubSessionLimits::default();
    let pubsub_limits = PubSubSessionLimits::default()
        .with_max_sessions(pick(
            args.pubsub_max_sessions,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_SESSIONS")?,
            file.limits.pubsub.max_sessions,
            defaults_pubsub.max_sessions(),
        ))
        .with_max_sessions_per_owner(pick(
            args.pubsub_max_sessions_per_owner,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_SESSIONS_PER_OWNER")?,
            file.limits.pubsub.max_sessions_per_owner,
            defaults_pubsub.max_sessions_per_owner(),
        ))
        .with_max_subscriptions_per_session(pick(
            args.pubsub_max_subscriptions_per_session,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_SUBSCRIPTIONS_PER_SESSION")?,
            file.limits.pubsub.max_subscriptions_per_session,
            defaults_pubsub.max_subscriptions_per_session(),
        ))
        .with_max_buffered_messages(pick(
            args.pubsub_max_buffered_messages,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_BUFFERED_MESSAGES")?,
            file.limits.pubsub.max_buffered_messages,
            defaults_pubsub.max_buffered_messages(),
        ))
        .with_max_message_bytes(pick(
            args.pubsub_max_message_bytes,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_MESSAGE_BYTES")?,
            file.limits.pubsub.max_message_bytes,
            defaults_pubsub.max_message_bytes(),
        ))
        .with_max_read_bytes(pick(
            args.pubsub_max_read_bytes,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_READ_BYTES")?,
            file.limits.pubsub.max_read_bytes,
            defaults_pubsub.max_read_bytes(),
        ))
        .with_max_read_duration(millis(pick(
            args.pubsub_max_read_duration_ms,
            env_parsed(env, "REDIS_MCP_PUBSUB_MAX_READ_DURATION_MS")?,
            file.limits.pubsub.max_read_duration_ms,
            defaults_pubsub.max_read_duration().as_millis() as u64,
        )))
        .with_idle_timeout(millis(pick(
            args.pubsub_idle_timeout_ms,
            env_parsed(env, "REDIS_MCP_PUBSUB_IDLE_TIMEOUT_MS")?,
            file.limits.pubsub.idle_timeout_ms,
            defaults_pubsub.idle_timeout().as_millis() as u64,
        )))
        .with_cleanup_interval(millis(pick(
            args.pubsub_cleanup_interval_ms,
            env_parsed(env, "REDIS_MCP_PUBSUB_CLEANUP_INTERVAL_MS")?,
            file.limits.pubsub.cleanup_interval_ms,
            defaults_pubsub.cleanup_interval().as_millis() as u64,
        )))
        .with_operation_timeout(millis(pick(
            args.pubsub_operation_timeout_ms,
            env_parsed(env, "REDIS_MCP_PUBSUB_OPERATION_TIMEOUT_MS")?,
            file.limits.pubsub.operation_timeout_ms,
            defaults_pubsub.operation_timeout().as_millis() as u64,
        )));

    let defaults_monitor = MonitorSessionLimits::default();
    let monitor_limits = MonitorSessionLimits::default()
        .with_max_sessions(pick(
            args.monitor_max_sessions,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_SESSIONS")?,
            file.limits.monitor.max_sessions,
            defaults_monitor.max_sessions(),
        ))
        .with_max_sessions_per_owner(pick(
            args.monitor_max_sessions_per_owner,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_SESSIONS_PER_OWNER")?,
            file.limits.monitor.max_sessions_per_owner,
            defaults_monitor.max_sessions_per_owner(),
        ))
        .with_max_buffered_events(pick(
            args.monitor_max_buffered_events,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_BUFFERED_EVENTS")?,
            file.limits.monitor.max_buffered_events,
            defaults_monitor.max_buffered_events(),
        ))
        .with_max_event_bytes(pick(
            args.monitor_max_event_bytes,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_EVENT_BYTES")?,
            file.limits.monitor.max_event_bytes,
            defaults_monitor.max_event_bytes(),
        ))
        .with_max_read_bytes(pick(
            args.monitor_max_read_bytes,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_READ_BYTES")?,
            file.limits.monitor.max_read_bytes,
            defaults_monitor.max_read_bytes(),
        ))
        .with_max_read_duration(millis(pick(
            args.monitor_max_read_duration_ms,
            env_parsed(env, "REDIS_MCP_MONITOR_MAX_READ_DURATION_MS")?,
            file.limits.monitor.max_read_duration_ms,
            defaults_monitor.max_read_duration().as_millis() as u64,
        )))
        .with_idle_timeout(millis(pick(
            args.monitor_idle_timeout_ms,
            env_parsed(env, "REDIS_MCP_MONITOR_IDLE_TIMEOUT_MS")?,
            file.limits.monitor.idle_timeout_ms,
            defaults_monitor.idle_timeout().as_millis() as u64,
        )))
        .with_cleanup_interval(millis(pick(
            args.monitor_cleanup_interval_ms,
            env_parsed(env, "REDIS_MCP_MONITOR_CLEANUP_INTERVAL_MS")?,
            file.limits.monitor.cleanup_interval_ms,
            defaults_monitor.cleanup_interval().as_millis() as u64,
        )))
        .with_operation_timeout(millis(pick(
            args.monitor_operation_timeout_ms,
            env_parsed(env, "REDIS_MCP_MONITOR_OPERATION_TIMEOUT_MS")?,
            file.limits.monitor.operation_timeout_ms,
            defaults_monitor.operation_timeout().as_millis() as u64,
        )));

    let defaults_blocking = RedisBlockingLimits::default();
    let blocking_limits = RedisBlockingLimits::default()
        .with_max_timeout(millis(pick(
            args.blocking_max_timeout_ms,
            env_parsed(env, "REDIS_MCP_BLOCKING_MAX_TIMEOUT_MS")?,
            file.limits.blocking.max_timeout_ms,
            defaults_blocking.max_timeout().as_millis() as u64,
        )))
        .with_max_keys(pick(
            args.blocking_max_keys,
            env_parsed(env, "REDIS_MCP_BLOCKING_MAX_KEYS")?,
            file.limits.blocking.max_keys,
            defaults_blocking.max_keys(),
        ))
        .with_max_count(pick(
            args.blocking_max_count,
            env_parsed(env, "REDIS_MCP_BLOCKING_MAX_COUNT")?,
            file.limits.blocking.max_count,
            defaults_blocking.max_count(),
        ));
    let blocking_max_concurrent_calls = pick(
        args.blocking_max_concurrent_calls,
        env_parsed(env, "REDIS_MCP_BLOCKING_MAX_CONCURRENT_CALLS")?,
        file.limits.blocking.max_concurrent_calls,
        DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS,
    );

    let defaults_transactions = RedisTransactionLimits::default();
    let transaction_limits = RedisTransactionLimits::default()
        .with_max_commands(pick(
            args.transaction_max_commands,
            env_parsed(env, "REDIS_MCP_TRANSACTION_MAX_COMMANDS")?,
            file.limits.transactions.max_commands,
            defaults_transactions.max_commands(),
        ))
        .with_max_watch_keys(pick(
            args.transaction_max_watch_keys,
            env_parsed(env, "REDIS_MCP_TRANSACTION_MAX_WATCH_KEYS")?,
            file.limits.transactions.max_watch_keys,
            defaults_transactions.max_watch_keys(),
        ))
        .with_max_request_bytes(pick(
            args.transaction_max_request_bytes,
            env_parsed(env, "REDIS_MCP_TRANSACTION_MAX_REQUEST_BYTES")?,
            file.limits.transactions.max_request_bytes,
            defaults_transactions.max_request_bytes(),
        ))
        .with_max_duration(millis(pick(
            args.transaction_max_duration_ms,
            env_parsed(env, "REDIS_MCP_TRANSACTION_MAX_DURATION_MS")?,
            file.limits.transactions.max_duration_ms,
            defaults_transactions.max_duration().as_millis() as u64,
        )));
    let transaction_max_concurrent = pick(
        args.transaction_max_concurrent,
        env_parsed(env, "REDIS_MCP_TRANSACTION_MAX_CONCURRENT")?,
        file.limits.transactions.max_concurrent,
        DEFAULT_MAX_CONCURRENT_TRANSACTIONS,
    );

    let defaults_bulk = RedisBulkLimits::default();
    let bulk_limits = RedisBulkLimits::default()
        .with_max_records(pick(
            args.bulk_max_records,
            env_parsed(env, "REDIS_MCP_BULK_MAX_RECORDS")?,
            file.limits.bulk.max_records,
            defaults_bulk.max_records(),
        ))
        .with_max_batch_size(pick(
            args.bulk_max_batch_size,
            env_parsed(env, "REDIS_MCP_BULK_MAX_BATCH_SIZE")?,
            file.limits.bulk.max_batch_size,
            defaults_bulk.max_batch_size(),
        ))
        .with_max_concurrency(pick(
            args.bulk_max_concurrency,
            env_parsed(env, "REDIS_MCP_BULK_MAX_CONCURRENCY")?,
            file.limits.bulk.max_concurrency,
            defaults_bulk.max_concurrency(),
        ))
        .with_max_input_bytes(pick(
            args.bulk_max_input_bytes,
            env_parsed(env, "REDIS_MCP_BULK_MAX_INPUT_BYTES")?,
            file.limits.bulk.max_input_bytes,
            defaults_bulk.max_input_bytes(),
        ))
        .with_max_duration(millis(pick(
            args.bulk_max_duration_ms,
            env_parsed(env, "REDIS_MCP_BULK_MAX_DURATION_MS")?,
            file.limits.bulk.max_duration_ms,
            defaults_bulk.max_duration().as_millis() as u64,
        )))
        .with_max_reported_failures(pick(
            args.bulk_max_reported_failures,
            env_parsed(env, "REDIS_MCP_BULK_MAX_REPORTED_FAILURES")?,
            file.limits.bulk.max_reported_failures,
            defaults_bulk.max_reported_failures(),
        ))
        .with_max_batch_summaries(pick(
            args.bulk_max_batch_summaries,
            env_parsed(env, "REDIS_MCP_BULK_MAX_BATCH_SUMMARIES")?,
            file.limits.bulk.max_batch_summaries,
            defaults_bulk.max_batch_summaries(),
        ));

    let docs_enabled = pick(
        args.enable_docs.then_some(true),
        env_bool(env, "REDIS_MCP_DOCS_ENABLED")?,
        file.docs.enabled,
        false,
    );
    let defaults_docs = RedisDocsOptions::default();
    let docs_options = RedisDocsOptions::default()
        .with_pin(pick(
            args.docs_pin.clone(),
            env("REDIS_MCP_DOCS_PIN").filter(|value| !value.trim().is_empty()),
            file.docs.pin.clone(),
            defaults_docs.pin().to_string(),
        ))
        .with_max_doc_bytes(pick(
            args.docs_max_bytes,
            env_parsed(env, "REDIS_MCP_DOCS_MAX_BYTES")?,
            file.docs.max_doc_bytes,
            defaults_docs.max_doc_bytes(),
        ))
        .with_cache_entries(pick(
            args.docs_cache_entries,
            env_parsed(env, "REDIS_MCP_DOCS_CACHE_ENTRIES")?,
            file.docs.cache_entries,
            defaults_docs.cache_entries(),
        ))
        .with_fetch_timeout(millis(pick(
            args.docs_fetch_timeout_ms,
            env_parsed(env, "REDIS_MCP_DOCS_FETCH_TIMEOUT_MS")?,
            file.docs.fetch_timeout_ms,
            defaults_docs.fetch_timeout().as_millis() as u64,
        )));

    let config = ServerConfig {
        transport,
        target,
        access,
        raw_policy,
        transactions,
        bundles,
        unavailable_tools,
        discover_capabilities,
        max_output_bytes,
        max_output_entries,
        command_timeout,
        pubsub_limits,
        monitor_limits,
        blocking_limits,
        blocking_max_concurrent_calls,
        transaction_limits,
        transaction_max_concurrent,
        bulk_limits,
        docs_enabled,
        docs_options,
    };
    validate(&config)?;
    Ok(config)
}

const fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}

fn validate(config: &ServerConfig) -> Result<(), ConfigError> {
    if config.raw_policy == RawCommandPolicy::Unrestricted && config.access != AccessMode::Full {
        return Err(ConfigError::UnrestrictedRawRequiresFullAccess);
    }
    if config.transactions && config.raw_policy == RawCommandPolicy::Disabled {
        return Err(ConfigError::TransactionsRequireRawCommands);
    }
    if config.docs_enabled && config.docs_options.validate().is_err() {
        return Err(ConfigError::InvalidDocsOptions);
    }
    if let ServerTransport::Http(http) = &config.transport {
        if http.max_body_bytes == 0
            || http.max_concurrency == 0
            || http.max_sessions == 0
            || http.session_ttl.is_zero()
            || http.request_timeout.is_zero()
            || http.drain_timeout.is_zero()
        {
            return Err(ConfigError::InvalidHttpOptions(
                "HTTP body, concurrency, session, TTL, request-timeout, and drain-timeout limits must all be greater than zero",
            ));
        }
        if http.allowed_hosts.iter().any(|host| host.trim().is_empty())
            || http
                .allowed_origins
                .iter()
                .any(|origin| origin.trim().is_empty())
        {
            return Err(ConfigError::InvalidHttpOptions(
                "HTTP host and origin allowlist entries must not be empty",
            ));
        }
        if !http.bind.ip().is_loopback() {
            if !http.allow_remote {
                return Err(ConfigError::InvalidHttpOptions(
                    "binding HTTP to a non-loopback address requires --http-allow-remote (or REDIS_MCP_HTTP_ALLOW_REMOTE=true)",
                ));
            }
            if http.bearer_token.is_none() {
                return Err(ConfigError::InvalidHttpOptions(
                    "binding HTTP to a non-loopback address requires REDIS_MCP_HTTP_BEARER_TOKEN",
                ));
            }
            if http.allowed_hosts.is_empty() {
                return Err(ConfigError::InvalidHttpOptions(
                    "binding HTTP to a non-loopback address requires an explicit HTTP Host allowlist",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn env_of(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn args(argv: &[&str]) -> Args {
        Args::try_parse_from(std::iter::once("redis-mcp-server").chain(argv.iter().copied()))
            .expect("parse test arguments")
    }

    #[test]
    fn defaults_resolve_without_any_source() {
        let config = resolve(&args(&["--stdio"]), None, &no_env).expect("resolve defaults");
        assert_eq!(
            config.target,
            ServerTarget::Standalone("redis://127.0.0.1:6379".to_string())
        );
        assert_eq!(config.access, AccessMode::ReadOnly);
        assert_eq!(config.transport, ServerTransport::Stdio);
        assert_eq!(config.raw_policy, RawCommandPolicy::Disabled);
        assert!(!config.transactions);
        assert!(config.discover_capabilities);
        assert_eq!(config.max_output_bytes, DEFAULT_MAX_OUTPUT_BYTES);
        assert_eq!(config.command_timeout, DEFAULT_COMMAND_TIMEOUT);
        assert_eq!(config.pubsub_limits, PubSubSessionLimits::default());
        assert_eq!(config.monitor_limits, MonitorSessionLimits::default());
        assert_eq!(config.blocking_limits, RedisBlockingLimits::default());
    }

    #[test]
    fn cli_overrides_env_overrides_file_per_setting() {
        let file: FileConfig = toml::from_str(
            r#"
            [target]
            url = "redis://file:6379"
            [server]
            access = "full"
            [output]
            max_bytes = 1000
            [limits.pubsub]
            max_sessions = 1
            [limits.blocking]
            max_keys = 2
            [limits.transactions]
            max_commands = 3
            [limits.bulk]
            max_records = 4
            [limits.monitor]
            max_sessions = 2
            "#,
        )
        .expect("parse layered file");
        let env = env_of(&[
            ("REDIS_URL", "redis://env:6379"),
            ("REDIS_MCP_ACCESS", "read-write"),
            ("REDIS_MCP_MAX_OUTPUT_BYTES", "2000"),
            ("REDIS_MCP_PUBSUB_MAX_SESSIONS", "10"),
        ]);
        let lookup = |name: &str| env.get(name).cloned();

        // File only.
        let file_only = resolve(&args(&["--stdio"]), Some(&file), &no_env).expect("file layer");
        assert_eq!(
            file_only.target,
            ServerTarget::Standalone("redis://file:6379".to_string())
        );
        assert_eq!(file_only.access, AccessMode::Full);
        assert_eq!(file_only.max_output_bytes, 1000);
        assert_eq!(file_only.pubsub_limits.max_sessions(), 1);
        assert_eq!(file_only.blocking_limits.max_keys(), 2);
        assert_eq!(file_only.transaction_limits.max_commands(), 3);
        assert_eq!(file_only.bulk_limits.max_records(), 4);
        assert_eq!(file_only.monitor_limits.max_sessions(), 2);

        // Env beats file.
        let env_over_file = resolve(&args(&["--stdio"]), Some(&file), &lookup).expect("env layer");
        assert_eq!(
            env_over_file.target,
            ServerTarget::Standalone("redis://env:6379".to_string())
        );
        assert_eq!(env_over_file.access, AccessMode::ReadWrite);
        assert_eq!(env_over_file.max_output_bytes, 2000);
        assert_eq!(env_over_file.pubsub_limits.max_sessions(), 10);
        // Untouched by env: file still applies.
        assert_eq!(env_over_file.blocking_limits.max_keys(), 2);

        // CLI beats both.
        let cli_over_all = resolve(
            &args(&[
                "--stdio",
                "--url",
                "redis://cli:6379",
                "--access",
                "read-only",
                "--max-output-bytes",
                "3000",
                "--pubsub-max-sessions",
                "20",
            ]),
            Some(&file),
            &lookup,
        )
        .expect("cli layer");
        assert_eq!(
            cli_over_all.target,
            ServerTarget::Standalone("redis://cli:6379".to_string())
        );
        assert_eq!(cli_over_all.access, AccessMode::ReadOnly);
        assert_eq!(cli_over_all.max_output_bytes, 3000);
        assert_eq!(cli_over_all.pubsub_limits.max_sessions(), 20);
    }

    #[test]
    fn unknown_toml_keys_are_rejected() {
        let error = toml::from_str::<FileConfig>("[server]\naccess = \"full\"\ntypo_key = 1\n")
            .expect_err("unknown key must fail");
        assert!(error.to_string().contains("typo_key"), "{error}");
    }

    #[test]
    fn invalid_env_values_are_actionable() {
        let env = env_of(&[("REDIS_MCP_ACCESS", "root")]);
        let lookup = |name: &str| env.get(name).cloned();
        let error = resolve(&args(&["--stdio"]), None, &lookup).expect_err("bad enum");
        let message = error.to_string();
        assert!(message.contains("REDIS_MCP_ACCESS"), "{message}");
        assert!(message.contains("read-only"), "{message}");

        let env = env_of(&[("REDIS_MCP_TRANSACTIONS", "maybe")]);
        let lookup = |name: &str| env.get(name).cloned();
        let error = resolve(&args(&["--stdio"]), None, &lookup).expect_err("bad bool");
        assert!(error.to_string().contains("REDIS_MCP_TRANSACTIONS"));
    }

    #[test]
    fn http_is_loopback_by_default_and_can_be_selected_from_each_source() {
        let cli = resolve(&args(&["--http", "127.0.0.1:9000"]), None, &no_env)
            .expect("loopback HTTP from CLI");
        let ServerTransport::Http(cli_http) = cli.transport else {
            panic!("CLI must select HTTP");
        };
        assert_eq!(cli_http.bind, "127.0.0.1:9000".parse().unwrap());
        assert!(cli_http.bearer_token.is_none());

        let env = env_of(&[("REDIS_MCP_TRANSPORT", "http")]);
        let lookup = |name: &str| env.get(name).cloned();
        let from_env = resolve(&args(&[]), None, &lookup).expect("HTTP from env");
        assert!(matches!(from_env.transport, ServerTransport::Http(_)));

        let file: FileConfig =
            toml::from_str("[server]\ntransport = \"http\"\n").expect("HTTP file");
        let from_file = resolve(&args(&[]), Some(&file), &no_env).expect("HTTP from file");
        assert!(matches!(from_file.transport, ServerTransport::Http(_)));
    }

    #[test]
    fn remote_http_requires_acknowledgement_auth_and_host_allowlist() {
        let remote = ["--http", "0.0.0.0:8080"];
        let error = resolve(&args(&remote), None, &no_env).expect_err("needs acknowledgement");
        assert!(error.to_string().contains("--http-allow-remote"));

        let error = resolve(
            &args(&["--http", "0.0.0.0:8080", "--http-allow-remote"]),
            None,
            &no_env,
        )
        .expect_err("needs authentication");
        assert!(error.to_string().contains("REDIS_MCP_HTTP_BEARER_TOKEN"));

        let env = env_of(&[("REDIS_MCP_HTTP_BEARER_TOKEN", "do-not-print-this")]);
        let lookup = |name: &str| env.get(name).cloned();
        let error = resolve(
            &args(&["--http", "0.0.0.0:8080", "--http-allow-remote"]),
            None,
            &lookup,
        )
        .expect_err("needs hosts");
        assert!(error.to_string().contains("Host allowlist"));

        let config = resolve(
            &args(&[
                "--http",
                "0.0.0.0:8080",
                "--http-allow-remote",
                "--http-allowed-host",
                "redis.example:8080",
            ]),
            None,
            &lookup,
        )
        .expect("guarded remote HTTP");
        let debug = format!("{config:?}");
        assert!(!debug.contains("do-not-print-this"), "{debug}");
        assert!(debug.contains("[REDACTED]"), "{debug}");
    }

    #[test]
    fn http_limits_reject_zero() {
        let error = resolve(
            &args(&["--http", "127.0.0.1:8080", "--http-max-concurrency", "0"]),
            None,
            &no_env,
        )
        .expect_err("zero concurrency");
        assert!(matches!(error, ConfigError::InvalidHttpOptions(_)));
    }

    #[test]
    fn cross_source_target_conflicts_fail_closed() {
        let env = env_of(&[
            ("REDIS_URL", "redis://env:6379"),
            ("REDIS_CLUSTER_URLS", "redis://a:7000,redis://b:7000"),
        ]);
        let lookup = |name: &str| env.get(name).cloned();
        let error = resolve(&args(&["--stdio"]), None, &lookup).expect_err("conflicting env");
        assert!(matches!(error, ConfigError::ConflictingTargets));

        let file: FileConfig = toml::from_str(
            "[target]\nurl = \"redis://x:6379\"\ncluster_urls = [\"redis://y:7000\"]\n",
        )
        .expect("parse conflicting file");
        let error = resolve(&args(&["--stdio"]), Some(&file), &no_env).expect_err("file conflict");
        assert!(matches!(error, ConfigError::ConflictingTargets));

        // A higher-precedence source replaces the target wholesale instead of
        // conflicting with a lower one.
        let file: FileConfig =
            toml::from_str("[target]\ncluster_urls = [\"redis://y:7000\"]\n").expect("file");
        let resolved = resolve(
            &args(&["--stdio", "--url", "redis://cli:6379"]),
            Some(&file),
            &no_env,
        )
        .expect("cli replaces file target");
        assert_eq!(
            resolved.target,
            ServerTarget::Standalone("redis://cli:6379".to_string())
        );
    }

    #[test]
    fn policy_invariants_hold_across_sources() {
        let error = resolve(&args(&["--stdio", "--raw-unrestricted"]), None, &no_env)
            .expect_err("unrestricted needs full");
        assert!(matches!(
            error,
            ConfigError::UnrestrictedRawRequiresFullAccess
        ));

        let file: FileConfig =
            toml::from_str("[server]\ntransactions = true\n").expect("parse transactions file");
        let error =
            resolve(&args(&["--stdio"]), Some(&file), &no_env).expect_err("transactions need raw");
        assert!(matches!(error, ConfigError::TransactionsRequireRawCommands));

        // Satisfied across sources: raw from env, transactions from file.
        let env = env_of(&[("REDIS_MCP_RAW", "classified")]);
        let lookup = |name: &str| env.get(name).cloned();
        let resolved =
            resolve(&args(&["--stdio"]), Some(&file), &lookup).expect("raw via env satisfies");
        assert!(resolved.transactions);
        assert_eq!(resolved.raw_policy, RawCommandPolicy::Classified);
    }

    #[test]
    fn bundles_merge_by_precedence_not_union() {
        let file: FileConfig =
            toml::from_str("[server]\nbundles = [\"admin\", \"bulk\"]\n").expect("bundle file");
        let from_file = resolve(&args(&["--stdio"]), Some(&file), &no_env).expect("file bundles");
        assert_eq!(from_file.bundles, vec![ToolBundle::Admin, ToolBundle::Bulk]);
        let from_cli = resolve(
            &args(&["--stdio", "--enable-bundle", "search"]),
            Some(&file),
            &no_env,
        )
        .expect("cli bundles");
        assert_eq!(from_cli.bundles, vec![ToolBundle::Search]);
    }

    #[test]
    fn cli_flags_keep_their_existing_shapes() {
        let parsed = args(&[
            "--access",
            "full",
            "--raw",
            "--transactions",
            "--enable-bundle",
            "scripting",
            "--enable-bundle",
            "timeseries",
            "--stdio",
        ]);
        assert!(parsed.raw);
        assert!(parsed.transactions);
        assert_eq!(parsed.optional_bundles.len(), 2);
        let config = resolve(&parsed, None, &no_env).expect("resolve compat flags");
        assert_eq!(config.access, AccessMode::Full);
        assert_eq!(config.raw_policy, RawCommandPolicy::Classified);
        assert!(config.transactions);
        assert_eq!(
            config.bundles,
            vec![ToolBundle::Scripting, ToolBundle::TimeSeries]
        );
    }

    #[test]
    fn docs_serving_is_opt_in_and_layered() {
        let disabled = resolve(&args(&["--stdio"]), None, &no_env).expect("resolve default docs");
        assert!(!disabled.docs_enabled);

        let file: FileConfig =
            toml::from_str("[docs]\nenabled = true\npin = \"abc123\"\n").expect("docs file");
        let from_file = resolve(&args(&["--stdio"]), Some(&file), &no_env).expect("file docs");
        assert!(from_file.docs_enabled);
        assert_eq!(from_file.docs_options.pin(), "abc123");

        let env = env_of(&[("REDIS_MCP_DOCS_PIN", "def456")]);
        let lookup = |name: &str| env.get(name).cloned();
        let env_pin = resolve(&args(&["--stdio"]), Some(&file), &lookup).expect("env docs pin");
        assert_eq!(env_pin.docs_options.pin(), "def456");

        let cli_pin = resolve(
            &args(&["--stdio", "--enable-docs", "--docs-pin", "cli789"]),
            Some(&file),
            &lookup,
        )
        .expect("cli docs pin");
        assert!(cli_pin.docs_enabled);
        assert_eq!(cli_pin.docs_options.pin(), "cli789");
    }

    #[test]
    fn enabled_docs_reject_invalid_options_during_resolution() {
        let zero_bytes = resolve(
            &args(&["--stdio", "--enable-docs", "--docs-max-bytes", "0"]),
            None,
            &no_env,
        )
        .expect_err("zero docs byte ceiling");
        assert!(matches!(zero_bytes, ConfigError::InvalidDocsOptions));

        let path_pin = resolve(
            &args(&["--stdio", "--enable-docs", "--docs-pin", "../main"]),
            None,
            &no_env,
        )
        .expect_err("path-like docs revision");
        assert!(matches!(path_pin, ConfigError::InvalidDocsOptions));
    }

    #[test]
    fn no_discovery_flag_and_env_disable_discovery() {
        let disabled = resolve(&args(&["--stdio", "--no-discovery"]), None, &no_env)
            .expect("resolve no-discovery");
        assert!(!disabled.discover_capabilities);
        let env = env_of(&[("REDIS_MCP_DISCOVER_CAPABILITIES", "false")]);
        let lookup = |name: &str| env.get(name).cloned();
        let via_env = resolve(&args(&["--stdio"]), None, &lookup).expect("resolve env discovery");
        assert!(!via_env.discover_capabilities);
    }

    #[test]
    fn example_config_file_is_complete_and_matches_the_defaults() {
        let example: FileConfig = toml::from_str(include_str!("../redis-mcp.example.toml"))
            .expect("the shipped example config must parse");

        // Every documented key resolves to exactly the built-in defaults, so
        // the example never teaches wrong numbers.
        let from_example =
            resolve(&args(&["--stdio"]), Some(&example), &no_env).expect("resolve example");
        let from_defaults = resolve(&args(&["--stdio"]), None, &no_env).expect("resolve defaults");
        assert_eq!(from_example, from_defaults);

        // Every setting is present, so the example is a complete reference.
        // cluster_urls stays commented out because it conflicts with url.
        let presence = [
            ("target.url", example.target.url.is_some()),
            ("server.access", example.server.access.is_some()),
            ("server.transport", example.server.transport.is_some()),
            ("server.raw", example.server.raw.is_some()),
            ("server.transactions", example.server.transactions.is_some()),
            ("server.bundles", example.server.bundles.is_some()),
            (
                "server.unavailable_tools",
                example.server.unavailable_tools.is_some(),
            ),
            (
                "server.discover_capabilities",
                example.server.discover_capabilities.is_some(),
            ),
            ("output.max_bytes", example.output.max_bytes.is_some()),
            ("output.max_entries", example.output.max_entries.is_some()),
            ("timeouts.command_ms", example.timeouts.command_ms.is_some()),
            (
                "http.*",
                example.http.bind.is_some()
                    && example.http.allow_remote.is_some()
                    && example.http.allowed_hosts.is_some()
                    && example.http.allowed_origins.is_some()
                    && example.http.max_body_bytes.is_some()
                    && example.http.max_concurrency.is_some()
                    && example.http.max_sessions.is_some()
                    && example.http.session_ttl_ms.is_some()
                    && example.http.request_timeout_ms.is_some()
                    && example.http.drain_timeout_ms.is_some(),
            ),
            (
                "docs.*",
                example.docs.enabled.is_some()
                    && example.docs.pin.is_some()
                    && example.docs.max_doc_bytes.is_some()
                    && example.docs.cache_entries.is_some()
                    && example.docs.fetch_timeout_ms.is_some(),
            ),
            (
                "limits.pubsub.*",
                example.limits.pubsub.max_sessions.is_some()
                    && example.limits.pubsub.max_sessions_per_owner.is_some()
                    && example
                        .limits
                        .pubsub
                        .max_subscriptions_per_session
                        .is_some()
                    && example.limits.pubsub.max_buffered_messages.is_some()
                    && example.limits.pubsub.max_message_bytes.is_some()
                    && example.limits.pubsub.max_read_bytes.is_some()
                    && example.limits.pubsub.max_read_duration_ms.is_some()
                    && example.limits.pubsub.idle_timeout_ms.is_some()
                    && example.limits.pubsub.cleanup_interval_ms.is_some()
                    && example.limits.pubsub.operation_timeout_ms.is_some(),
            ),
            (
                "limits.monitor.*",
                example.limits.monitor.max_sessions.is_some()
                    && example.limits.monitor.max_sessions_per_owner.is_some()
                    && example.limits.monitor.max_buffered_events.is_some()
                    && example.limits.monitor.max_event_bytes.is_some()
                    && example.limits.monitor.max_read_bytes.is_some()
                    && example.limits.monitor.max_read_duration_ms.is_some()
                    && example.limits.monitor.idle_timeout_ms.is_some()
                    && example.limits.monitor.cleanup_interval_ms.is_some()
                    && example.limits.monitor.operation_timeout_ms.is_some(),
            ),
            (
                "limits.blocking.*",
                example.limits.blocking.max_timeout_ms.is_some()
                    && example.limits.blocking.max_keys.is_some()
                    && example.limits.blocking.max_count.is_some()
                    && example.limits.blocking.max_concurrent_calls.is_some(),
            ),
            (
                "limits.transactions.*",
                example.limits.transactions.max_commands.is_some()
                    && example.limits.transactions.max_watch_keys.is_some()
                    && example.limits.transactions.max_request_bytes.is_some()
                    && example.limits.transactions.max_duration_ms.is_some()
                    && example.limits.transactions.max_concurrent.is_some(),
            ),
            (
                "limits.bulk.*",
                example.limits.bulk.max_records.is_some()
                    && example.limits.bulk.max_batch_size.is_some()
                    && example.limits.bulk.max_concurrency.is_some()
                    && example.limits.bulk.max_input_bytes.is_some()
                    && example.limits.bulk.max_duration_ms.is_some()
                    && example.limits.bulk.max_reported_failures.is_some()
                    && example.limits.bulk.max_batch_summaries.is_some(),
            ),
        ];
        for (name, present) in presence {
            assert!(present, "example config is missing {name}");
        }
    }
}
