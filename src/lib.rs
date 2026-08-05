//! Composable MCP tools for Redis databases.
//!
//! redis-mcp owns the database tool schemas, structured results, access
//! policy, and Redis command execution boundary. It deliberately does not own
//! transport selection, redisctl profiles, Cloud or Enterprise APIs, or a CLI
//! user interface.

#![forbid(unsafe_code)]

mod access;
mod catalog;
mod executor;
mod raw;
mod tools;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub use access::AccessMode;
pub use catalog::{ToolBundle, ToolMetadata, tool_catalog};
pub use executor::{
    DirectRedis, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisValue,
};
pub use raw::RawCommandPolicy;
use tower_mcp::McpRouter;

/// Default upper bound for one Redis command executed by a tool.
pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

/// Entry point for building a composable Redis database MCP router.
pub struct RedisMcp;

impl RedisMcp {
    /// Start a router builder around a host-supplied command executor.
    pub fn builder(executor: impl RedisExecutor) -> RedisMcpBuilder {
        RedisMcpBuilder {
            executor: Arc::new(executor),
            access: AccessMode::ReadOnly,
            bundles: ToolBundle::DEFAULTS.iter().copied().collect(),
            raw_command_policy: RawCommandPolicy::Disabled,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            server_name: "redis-mcp".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Builder for a Redis database MCP router.
pub struct RedisMcpBuilder {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
    bundles: BTreeSet<ToolBundle>,
    raw_command_policy: RawCommandPolicy,
    command_timeout: Duration,
    server_name: String,
    server_version: String,
}

impl RedisMcpBuilder {
    /// Set the maximum side-effect level exposed by the router.
    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    /// Replace the enabled non-raw tool bundles.
    ///
    /// Raw command execution remains governed by its separate opt-in even
    /// though its catalog metadata belongs to [`ToolBundle::Raw`].
    pub fn bundles(mut self, bundles: impl IntoIterator<Item = ToolBundle>) -> Self {
        self.bundles = bundles.into_iter().collect();
        self
    }

    /// Add one non-raw tool bundle to the current selection.
    pub fn bundle(mut self, bundle: ToolBundle) -> Self {
        self.bundles.insert(bundle);
        self
    }

    /// Enable the redis_command escape hatch.
    ///
    /// `true` selects [`RawCommandPolicy::Classified`], which fails closed for
    /// command names this library has not reviewed. Use
    /// [`RedisMcpBuilder::raw_command_policy`] for the stronger unrestricted
    /// opt-in.
    pub fn raw_commands(mut self, enabled: bool) -> Self {
        self.raw_command_policy = if enabled {
            RawCommandPolicy::Classified
        } else {
            RawCommandPolicy::Disabled
        };
        self
    }

    /// Configure the raw Redis command escape hatch explicitly.
    pub fn raw_command_policy(mut self, policy: RawCommandPolicy) -> Self {
        self.raw_command_policy = policy;
        self
    }

    /// Set the maximum time allowed for any one Redis command.
    ///
    /// The timeout is enforced around the host executor future and therefore
    /// also applies to custom executors. A zero duration is rejected by
    /// [`RedisMcpBuilder::try_build`].
    pub fn command_timeout(mut self, timeout: Duration) -> Self {
        self.command_timeout = timeout;
        self
    }

    /// Override the server identity advertised during MCP initialization.
    pub fn server_info(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.server_name = name.into();
        self.server_version = version.into();
        self
    }

    /// Build a transport-independent Tower MCP router.
    pub fn build(self) -> McpRouter {
        self.try_build()
            .expect("RedisMcp builder configuration should be valid")
    }

    /// Build a transport-independent Tower MCP router with validation.
    pub fn try_build(self) -> Result<McpRouter, RedisMcpBuildError> {
        if self.command_timeout.is_zero() {
            return Err(RedisMcpBuildError::ZeroCommandTimeout);
        }
        if self.raw_command_policy.is_enabled() && self.access != AccessMode::Full {
            return Err(RedisMcpBuildError::RawCommandsRequireFullAccess);
        }
        let state = Arc::new(tools::ToolState::new(
            self.executor,
            self.access,
            self.command_timeout,
            self.raw_command_policy,
        ));
        let mut router = McpRouter::new().server_info(self.server_name, self.server_version);
        router = tools::add_read_only_tools(router, state.clone(), &self.bundles);
        if self.access.permits(AccessMode::ReadWrite) {
            router = tools::add_write_tools(router, state.clone(), &self.bundles);
        }
        if self.access.permits(AccessMode::Full) {
            router = tools::add_destructive_tools(router, state.clone(), &self.bundles);
            if self.raw_command_policy.is_enabled() {
                router = tools::add_raw_tool(router, state);
            }
        }
        Ok(router)
    }
}

/// Invalid Redis MCP router configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RedisMcpBuildError {
    /// A zero timeout would make every command fail immediately.
    ZeroCommandTimeout,
    /// Raw commands are an escape hatch and require full access in addition to
    /// their separate policy opt-in.
    RawCommandsRequireFullAccess,
}

impl std::fmt::Display for RedisMcpBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroCommandTimeout => {
                formatter.write_str("command timeout must be greater than zero")
            }
            Self::RawCommandsRequireFullAccess => {
                formatter.write_str("raw command execution requires full access")
            }
        }
    }
}

impl std::error::Error for RedisMcpBuildError {}

/// Stable tool names in the curated default surface.
pub fn tool_names(access: AccessMode, raw_commands: bool) -> Vec<&'static str> {
    tool_names_for(access, ToolBundle::DEFAULTS.iter().copied(), raw_commands)
}

/// Tool names exposed for an explicit access and bundle selection.
pub fn tool_names_for(
    access: AccessMode,
    bundles: impl IntoIterator<Item = ToolBundle>,
    raw_commands: bool,
) -> Vec<&'static str> {
    let bundles = bundles.into_iter().collect::<Vec<_>>();
    catalog::selected_tool_names(access, &bundles, raw_commands)
}
