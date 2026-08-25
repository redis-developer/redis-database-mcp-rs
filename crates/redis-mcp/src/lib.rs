//! Composable MCP tools for Redis databases.
//!
//! redis-mcp owns the database tool schemas, structured results, access
//! policy, and Redis command execution boundary. It deliberately does not own
//! transport selection, redisctl profiles, Cloud or Enterprise APIs, or a CLI
//! user interface.

#![forbid(unsafe_code)]

mod access;
mod capabilities;
mod catalog;
mod executor;
mod invocation;
mod output;
mod pubsub_sessions;
mod raw;
mod tools;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub use access::AccessMode;
pub use capabilities::{
    CapabilityStatus, DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT, RedisCapabilities, RedisDeployment,
    RedisModuleCapability, RedisVersion, RedisVersionParseError, UnavailableToolPolicy,
};
pub use catalog::{
    RedisModule, ToolBundle, ToolCapabilityRequirements, ToolDeploymentRequirement, ToolMetadata,
    ToolOutputPolicy, tool_catalog,
};
pub use executor::{
    DirectRedis, DirectRedisCluster, RedisCommand, RedisError, RedisErrorKind, RedisExecutor,
    RedisValue,
};
pub use invocation::{
    NativeCommandMetadata, NativeRedisInvocation, NativeRedisResponse, RedisInvocationEngine,
    RedisInvocationEngineBuildError, RedisInvocationEngineBuilder, RedisOutputLimit,
    RedisOutputLimitDimension,
};
pub use output::{DEFAULT_MAX_OUTPUT_BYTES, DEFAULT_MAX_OUTPUT_ENTRIES, OutputBudget};
pub use pubsub_sessions::{
    DEFAULT_MAX_PUBSUB_BUFFERED_MESSAGES, DEFAULT_MAX_PUBSUB_MESSAGE_BYTES,
    DEFAULT_MAX_PUBSUB_READ_BYTES, DEFAULT_MAX_PUBSUB_READ_DURATION, DEFAULT_MAX_PUBSUB_SESSIONS,
    DEFAULT_MAX_PUBSUB_SESSIONS_PER_OWNER, DEFAULT_MAX_PUBSUB_SUBSCRIPTIONS,
    DEFAULT_PUBSUB_CLEANUP_INTERVAL, DEFAULT_PUBSUB_IDLE_TIMEOUT, DEFAULT_PUBSUB_OPERATION_TIMEOUT,
    DirectRedisPubSubSessionManager, PubSubMessage, PubSubReadRequest, PubSubReadResult,
    PubSubSessionError, PubSubSessionErrorKind, PubSubSessionLimits, PubSubSessionManager,
    PubSubSessionOwner, PubSubSessionSnapshot, PubSubSubscription, PubSubSubscriptionKind,
};
pub use raw::RawCommandPolicy;
use tower_mcp::{CapabilityFilter, Filterable, McpRouter, Tool};

struct PubSubOwnerCleanup {
    manager: Arc<dyn PubSubSessionManager>,
    owner: PubSubSessionOwner,
}

impl Drop for PubSubOwnerCleanup {
    fn drop(&mut self) {
        let manager = self.manager.clone();
        let owner = self.owner.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                manager.close_owner(&owner).await;
            });
        }
    }
}

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
            output_budget: OutputBudget::default(),
            pubsub_sessions: None,
            capabilities: RedisCapabilities::unknown(),
            unavailable_tool_policy: UnavailableToolPolicy::Advertise,
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
    output_budget: OutputBudget,
    pubsub_sessions: Option<Arc<dyn PubSubSessionManager>>,
    capabilities: RedisCapabilities,
    unavailable_tool_policy: UnavailableToolPolicy,
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

    /// Set hard limits for encoded MCP responses and returned collection
    /// entries.
    ///
    /// The byte limit measures the complete serialized tool result, including
    /// both structured content and its MCP text rendering. Both limits must be
    /// greater than zero.
    pub fn output_budget(mut self, output_budget: OutputBudget) -> Self {
        self.output_budget = output_budget;
        self
    }

    /// Enable the owner-isolated Pub/Sub session bundle with a host-supplied
    /// lifecycle manager.
    ///
    /// The manager is independent of [`RedisExecutor`], because subscription
    /// sessions require dedicated connections and a longer lifecycle than one
    /// request/response command. This method also enables
    /// [`ToolBundle::Sessions`].
    pub fn pubsub_sessions(mut self, manager: impl PubSubSessionManager) -> Self {
        self.pubsub_sessions = Some(Arc::new(manager));
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Enable Pub/Sub sessions with a shared manager trait object.
    pub fn shared_pubsub_sessions(mut self, manager: Arc<dyn PubSubSessionManager>) -> Self {
        self.pubsub_sessions = Some(manager);
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Supply a precomputed Redis capability snapshot.
    ///
    /// Custom executors can construct this snapshot without depending on
    /// redis-rs. Capabilities omitted from the snapshot remain unknown and are
    /// allowed through for backward compatibility.
    pub fn capabilities(mut self, capabilities: RedisCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Select whether known-unavailable tools remain visible with stable
    /// errors or are omitted from the MCP command surface.
    pub fn unavailable_tool_policy(mut self, policy: UnavailableToolPolicy) -> Self {
        self.unavailable_tool_policy = policy;
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
        if self.output_budget.max_bytes() == 0 {
            return Err(RedisMcpBuildError::ZeroOutputBytes);
        }
        if self.output_budget.max_collection_entries() == 0 {
            return Err(RedisMcpBuildError::ZeroOutputEntries);
        }
        if self.raw_command_policy.is_enabled() && self.access != AccessMode::Full {
            return Err(RedisMcpBuildError::RawCommandsRequireFullAccess);
        }
        if self.bundles.contains(&ToolBundle::Sessions) && self.pubsub_sessions.is_none() {
            return Err(RedisMcpBuildError::SessionsRequireManager);
        }
        let capabilities = Arc::new(self.capabilities);
        let invocation_engine = RedisInvocationEngine::from_shared(
            self.executor,
            self.access,
            self.raw_command_policy,
            self.command_timeout,
            self.output_budget,
            capabilities.clone(),
        );
        let pubsub_owner = (self.bundles.contains(&ToolBundle::Sessions)
            && self.pubsub_sessions.is_some())
        .then(PubSubSessionOwner::random);
        let state = Arc::new(tools::ToolState::new(
            self.access,
            self.output_budget,
            invocation_engine,
            self.pubsub_sessions,
        ));
        let mut router = McpRouter::new().server_info(self.server_name, self.server_version);
        if let (Some(manager), Some(owner)) = (&state.pubsub_sessions, pubsub_owner) {
            router = router
                .with_extension(owner.clone())
                .with_extension(Arc::new(PubSubOwnerCleanup {
                    manager: manager.clone(),
                    owner,
                }));
        }
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
        if self.unavailable_tool_policy == UnavailableToolPolicy::Hide {
            router = router.tool_filter(CapabilityFilter::new(move |_session, tool: &Tool| {
                tool_catalog()
                    .iter()
                    .find(|metadata| metadata.name == tool.name())
                    .is_none_or(|metadata| {
                        capabilities.tool_status(*metadata) != CapabilityStatus::Unavailable
                    })
            }));
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
    /// A zero byte budget cannot represent a valid MCP tool result.
    ZeroOutputBytes,
    /// A zero entry budget cannot represent a collection page.
    ZeroOutputEntries,
    /// Raw commands are an escape hatch and require full access in addition to
    /// their separate policy opt-in.
    RawCommandsRequireFullAccess,
    /// Stateful Pub/Sub tools require an explicit lifecycle manager.
    SessionsRequireManager,
}

impl std::fmt::Display for RedisMcpBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            Self::RawCommandsRequireFullAccess => {
                formatter.write_str("raw command execution requires full access")
            }
            Self::SessionsRequireManager => {
                formatter.write_str("the sessions bundle requires a Pub/Sub session manager")
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

/// Tool names exposed for a selection after applying known target
/// capabilities and an availability policy.
pub fn tool_names_for_capabilities(
    access: AccessMode,
    bundles: impl IntoIterator<Item = ToolBundle>,
    raw_commands: bool,
    capabilities: &RedisCapabilities,
    policy: UnavailableToolPolicy,
) -> Vec<&'static str> {
    let mut names = tool_names_for(access, bundles, raw_commands);
    if policy == UnavailableToolPolicy::Hide {
        names.retain(|name| {
            tool_catalog()
                .iter()
                .find(|metadata| metadata.name == *name)
                .is_none_or(|metadata| {
                    capabilities.tool_status(*metadata) != CapabilityStatus::Unavailable
                })
        });
    }
    names
}
