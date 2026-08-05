//! Composable MCP tools for Redis databases.
//!
//! redis-mcp owns the database tool schemas, structured results, access
//! policy, and Redis command execution boundary. It deliberately does not own
//! transport selection, redisctl profiles, Cloud or Enterprise APIs, or a CLI
//! user interface.

#![forbid(unsafe_code)]

mod access;
mod executor;
mod tools;

use std::sync::Arc;

pub use access::AccessMode;
pub use executor::{DirectRedis, RedisExecutor};
use tower_mcp::McpRouter;

/// Entry point for building a composable Redis database MCP router.
pub struct RedisMcp;

impl RedisMcp {
    /// Start a router builder around a host-supplied command executor.
    pub fn builder(executor: impl RedisExecutor) -> RedisMcpBuilder {
        RedisMcpBuilder {
            executor: Arc::new(executor),
            access: AccessMode::ReadOnly,
            raw_commands: false,
            server_name: "redis-mcp".to_string(),
            server_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Builder for a Redis database MCP router.
pub struct RedisMcpBuilder {
    executor: Arc<dyn RedisExecutor>,
    access: AccessMode,
    raw_commands: bool,
    server_name: String,
    server_version: String,
}

impl RedisMcpBuilder {
    /// Set the maximum side-effect level exposed by the router.
    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    /// Enable the redis_command escape hatch.
    ///
    /// The tool is only exposed when access is also AccessMode::Full.
    pub fn raw_commands(mut self, enabled: bool) -> Self {
        self.raw_commands = enabled;
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
        let state = Arc::new(tools::ToolState::new(self.executor, self.access));
        let mut router = McpRouter::new().server_info(self.server_name, self.server_version);
        router = tools::add_read_only_tools(router, state.clone());
        if self.access.permits(AccessMode::ReadWrite) {
            router = tools::add_write_tools(router, state.clone());
        }
        if self.access.permits(AccessMode::Full) {
            router = tools::add_destructive_tools(router, state.clone());
            if self.raw_commands {
                router = tools::add_raw_tool(router, state);
            }
        }
        router
    }
}

/// Stable tool names in the initial curated surface.
pub fn tool_names(access: AccessMode, raw_commands: bool) -> Vec<&'static str> {
    let mut names = tools::READ_ONLY_TOOL_NAMES.to_vec();
    if access.permits(AccessMode::ReadWrite) {
        names.extend_from_slice(tools::WRITE_TOOL_NAMES);
    }
    if access.permits(AccessMode::Full) {
        names.extend_from_slice(tools::DESTRUCTIVE_TOOL_NAMES);
        if raw_commands {
            names.push(tools::RAW_TOOL_NAME);
        }
    }
    names.sort_unstable();
    names
}
