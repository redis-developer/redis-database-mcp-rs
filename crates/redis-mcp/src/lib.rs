//! Composable MCP tools for Redis databases.
//!
//! redis-mcp owns the database tool schemas, structured results, access
//! policy, and Redis command execution boundary. It deliberately does not own
//! transport selection, redisctl profiles, Cloud or Enterprise APIs, or a CLI
//! user interface.

#![forbid(unsafe_code)]
#![cfg_attr(not(feature = "full"), allow(dead_code, unused_mut, unused_variables))]

mod access;
mod blocking;
mod bulk;
mod capabilities;
mod catalog;
#[cfg(feature = "coordination")]
mod coordination;
mod docs;
mod executor;
pub mod families;
#[cfg(feature = "guidance")]
mod guidance;
mod invocation;
mod monitor;
mod output;
mod pubsub_sessions;
mod raw;
mod response;
mod tools;
mod transactions;
mod transport;

use std::{collections::BTreeSet, sync::Arc, time::Duration};

pub use access::AccessMode;
pub use blocking::{
    DEFAULT_MAX_BLOCKING_COUNT, DEFAULT_MAX_BLOCKING_KEYS, DEFAULT_MAX_BLOCKING_TIMEOUT,
    DEFAULT_MAX_CONCURRENT_BLOCKING_CALLS, DirectRedisBlocking, RedisBlockingEngine,
    RedisBlockingExecutor, RedisBlockingLimits, RedisBlockingListMultiPop, RedisBlockingListPop,
    RedisBlockingMoveAmount, RedisBlockingMoveOrdering, RedisBlockingScoredMultiPop,
    RedisBlockingScoredPop, RedisListEnd, RedisPreparedBlockingCall, RedisSortedSetEnd,
    RedisWaitAofAcknowledged,
};
pub use bulk::{
    BulkBatchSummary, BulkErrorHandling, BulkLoadOptions, BulkLoadReport, BulkLoadRequest,
    BulkRecord, BulkRecordFailure, BulkRecordValue, BulkSeedField, BulkSeedRequest,
    BulkSeedTemplate, BulkSeedValue, DEFAULT_BULK_BATCH_SIZE, DEFAULT_BULK_CONCURRENCY,
    DEFAULT_MAX_BULK_BATCH_SIZE, DEFAULT_MAX_BULK_BATCH_SUMMARIES, DEFAULT_MAX_BULK_CONCURRENCY,
    DEFAULT_MAX_BULK_DURATION, DEFAULT_MAX_BULK_INPUT_BYTES, DEFAULT_MAX_BULK_RECORDS,
    DEFAULT_MAX_BULK_REPORTED_FAILURES, RedisBulkEngine, RedisBulkLimits, generate_seed_records,
};
pub use capabilities::{
    CapabilityStatus, DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT, RedisCapabilities, RedisDeployment,
    RedisModuleCapability, RedisVersion, RedisVersionParseError, UnavailableToolPolicy,
};
pub use catalog::{
    RedisModule, ToolBundle, ToolCapabilityRequirements, ToolDeploymentRequirement, ToolMetadata,
    ToolOutputPolicy, tool_catalog,
};
#[cfg(feature = "coordination")]
pub use coordination::{
    COORDINATION_GUIDE_URI, CoordinationConfig, CoordinationHandle, CoordinationPayload,
    CoordinationPrincipal, CoordinationStatus, DEFAULT_COORDINATION_MAX_METADATA_BYTES,
    DEFAULT_COORDINATION_MAX_PAYLOAD_BYTES, DEFAULT_COORDINATION_SHARDS,
};
pub use docs::{
    DEFAULT_DOC_CACHE_ENTRIES, DEFAULT_DOC_FETCH_TIMEOUT, DEFAULT_MAX_DOC_BYTES,
    DEFAULT_REDIS_DOCS_PIN, REDIS_DOCS_URI_TEMPLATE, RedisDocsFetcher, RedisDocsOptions,
};
pub use executor::{
    DirectRedis, DirectRedisCluster, RedisClusterFanout, RedisCommand, RedisError, RedisErrorKind,
    RedisExecutor, RedisValue,
};
pub use families::{ToolFamily, compiled_tool_families};
#[cfg(feature = "guidance")]
pub use guidance::{GUIDANCE_DOCS, GuidanceDoc, MAX_GUIDANCE_DOC_BYTES};
pub use invocation::{
    NativeCommandMetadata, NativeRedisInvocation, NativeRedisResponse, RedisInvocationEngine,
    RedisInvocationEngineBuildError, RedisInvocationEngineBuilder, RedisOutputLimit,
    RedisOutputLimitDimension,
};
pub use monitor::{
    DEFAULT_MAX_MONITOR_BUFFERED_EVENTS, DEFAULT_MAX_MONITOR_EVENT_BYTES,
    DEFAULT_MAX_MONITOR_READ_BYTES, DEFAULT_MAX_MONITOR_READ_DURATION,
    DEFAULT_MAX_MONITOR_SESSIONS, DEFAULT_MAX_MONITOR_SESSIONS_PER_OWNER,
    DEFAULT_MONITOR_CLEANUP_INTERVAL, DEFAULT_MONITOR_IDLE_TIMEOUT,
    DEFAULT_MONITOR_OPERATION_TIMEOUT, DirectRedisMonitorSessions, MonitorEvent,
    MonitorReadRequest, MonitorReadResult, MonitorSessionLimits, MonitorSessionManager,
    MonitorSessionOptions, MonitorSessionSnapshot, RedisSessionError, RedisSessionErrorKind,
    RedisSessionOwner,
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
pub use transactions::{
    DEFAULT_MAX_CONCURRENT_TRANSACTIONS, DEFAULT_MAX_TRANSACTION_COMMANDS,
    DEFAULT_MAX_TRANSACTION_DURATION, DEFAULT_MAX_TRANSACTION_REQUEST_BYTES,
    DEFAULT_MAX_TRANSACTION_WATCH_KEYS, DirectRedisTransactions, RedisPreparedTransaction,
    RedisTransactionCommandFailure, RedisTransactionEngine, RedisTransactionExecutor,
    RedisTransactionLimits, RedisTransactionOutcome, RedisTransactionRequest,
};

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

struct MonitorOwnerCleanup {
    manager: Arc<dyn MonitorSessionManager>,
    owner: PubSubSessionOwner,
}

impl Drop for MonitorOwnerCleanup {
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
            families: None,
            raw_command_policy: RawCommandPolicy::Disabled,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            output_budget: OutputBudget::default(),
            pubsub_sessions: None,
            transactions: None,
            transaction_limits: RedisTransactionLimits::default(),
            bulk_limits: RedisBulkLimits::default(),
            #[cfg(feature = "coordination")]
            coordination_config: CoordinationConfig::default(),
            blocking: None,
            blocking_limits: RedisBlockingLimits::default(),
            monitor_sessions: None,
            docs_fetcher: None,
            docs_options: RedisDocsOptions::default(),
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
    families: Option<BTreeSet<ToolFamily>>,
    raw_command_policy: RawCommandPolicy,
    command_timeout: Duration,
    output_budget: OutputBudget,
    pubsub_sessions: Option<Arc<dyn PubSubSessionManager>>,
    transactions: Option<Arc<dyn RedisTransactionExecutor>>,
    transaction_limits: RedisTransactionLimits,
    bulk_limits: RedisBulkLimits,
    #[cfg(feature = "coordination")]
    coordination_config: CoordinationConfig,
    blocking: Option<Arc<dyn RedisBlockingExecutor>>,
    blocking_limits: RedisBlockingLimits,
    monitor_sessions: Option<Arc<dyn MonitorSessionManager>>,
    docs_fetcher: Option<Arc<dyn RedisDocsFetcher>>,
    docs_options: RedisDocsOptions,
    capabilities: RedisCapabilities,
    unavailable_tool_policy: UnavailableToolPolicy,
    server_name: String,
    server_version: String,
}

impl RedisMcpBuilder {
    /// Replace the durable handoff namespace, sharding, and payload bounds.
    #[cfg(feature = "coordination")]
    pub fn coordination_config(mut self, config: CoordinationConfig) -> Self {
        self.coordination_config = config;
        self
    }

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
        self.families = None;
        self
    }

    /// Add one non-raw tool bundle to the current selection.
    pub fn bundle(mut self, bundle: ToolBundle) -> Self {
        self.bundles.insert(bundle);
        self
    }

    /// Replace the enabled Redis command families.
    ///
    /// This switches from the compatibility bundle assembly path to precise
    /// family selection and clears all bundles. Cross-cutting surfaces such as
    /// diagnostics and sessions can then be added explicitly with
    /// [`RedisMcpBuilder::bundle`]. Every selected family must also be enabled
    /// by its additive Cargo feature.
    pub fn families(mut self, families: impl IntoIterator<Item = ToolFamily>) -> Self {
        self.families = Some(families.into_iter().collect());
        self.bundles.clear();
        self
    }

    /// Add one Redis command family to a precise family selection.
    ///
    /// The first call switches away from the compatibility bundle defaults
    /// and starts an otherwise empty selection.
    pub fn family(mut self, family: ToolFamily) -> Self {
        if self.families.is_none() {
            self.families = Some(BTreeSet::new());
            self.bundles.clear();
        }
        self.families
            .as_mut()
            .expect("family selection was initialized")
            .insert(family);
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

    /// Enable finite blocking operations with a host-supplied dedicated
    /// connection executor.
    ///
    /// The executor is independent of [`RedisExecutor`], because a blocking
    /// wait must hold one dedicated connection per call rather than a pooled
    /// request/response command. This method also enables
    /// [`ToolBundle::Sessions`].
    pub fn blocking(mut self, executor: impl RedisBlockingExecutor) -> Self {
        self.blocking = Some(Arc::new(executor));
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Enable finite blocking operations with a shared executor trait object.
    pub fn shared_blocking(mut self, executor: Arc<dyn RedisBlockingExecutor>) -> Self {
        self.blocking = Some(executor);
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Replace the default blocking-call bounds.
    pub fn blocking_limits(mut self, limits: RedisBlockingLimits) -> Self {
        self.blocking_limits = limits;
        self
    }

    /// Enable owner-isolated MONITOR sessions with a host-supplied lifecycle
    /// manager.
    ///
    /// The manager is independent of [`RedisExecutor`], because MONITOR
    /// converts a dedicated connection into an indefinite server-push stream.
    /// This method also enables [`ToolBundle::Sessions`].
    pub fn monitor_sessions(mut self, manager: impl MonitorSessionManager) -> Self {
        self.monitor_sessions = Some(Arc::new(manager));
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Enable MONITOR sessions with a shared manager trait object.
    pub fn shared_monitor_sessions(mut self, manager: Arc<dyn MonitorSessionManager>) -> Self {
        self.monitor_sessions = Some(manager);
        self.bundles.insert(ToolBundle::Sessions);
        self
    }

    /// Enable the bounded atomic transaction tool with a host-supplied
    /// transaction executor.
    ///
    /// The executor is independent of [`RedisExecutor`], because MULTI, WATCH,
    /// and EXEC require one dedicated connection per call rather than a pooled
    /// request/response command. Transactions execute argv-shaped nested
    /// commands, so they additionally require the raw command policy and full
    /// access, exactly like the `redis_command` escape hatch. This method also
    /// enables [`ToolBundle::Transactions`].
    pub fn transactions(mut self, executor: impl RedisTransactionExecutor) -> Self {
        self.transactions = Some(Arc::new(executor));
        self.bundles.insert(ToolBundle::Transactions);
        self
    }

    /// Enable transactions with a shared executor trait object.
    pub fn shared_transactions(mut self, executor: Arc<dyn RedisTransactionExecutor>) -> Self {
        self.transactions = Some(executor);
        self.bundles.insert(ToolBundle::Transactions);
        self
    }

    /// Replace the default transaction bounds.
    pub fn transaction_limits(mut self, limits: RedisTransactionLimits) -> Self {
        self.transaction_limits = limits;
        self
    }

    /// Serve official Redis command documentation as passthrough resources
    /// through a host-supplied fetcher.
    ///
    /// Documentation reads introduce network egress the base surface never
    /// performs, so they are enabled only by configuring a fetcher. The
    /// library owns the URI template, pinned-inventory validation, bounds,
    /// caching, and attribution; the fetcher owns the transport.
    pub fn docs_fetcher(mut self, fetcher: impl RedisDocsFetcher) -> Self {
        self.docs_fetcher = Some(Arc::new(fetcher));
        self
    }

    /// Serve documentation through a shared fetcher trait object.
    pub fn shared_docs_fetcher(mut self, fetcher: Arc<dyn RedisDocsFetcher>) -> Self {
        self.docs_fetcher = Some(fetcher);
        self
    }

    /// Replace the default documentation pin and bounds.
    pub fn docs_options(mut self, options: RedisDocsOptions) -> Self {
        self.docs_options = options;
        self
    }

    /// Replace the default bulk workflow bounds.
    pub fn bulk_limits(mut self, limits: RedisBulkLimits) -> Self {
        self.bulk_limits = limits;
        self
    }

    /// Supply a precomputed Redis capability snapshot.
    ///
    /// Custom executors can construct this snapshot without depending on
    /// the bundled Redis client. Capabilities omitted from the snapshot remain unknown and are
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
        if self.raw_command_policy == RawCommandPolicy::Unrestricted
            && self.access != AccessMode::Full
        {
            return Err(RedisMcpBuildError::RawCommandsRequireFullAccess);
        }
        let invocation_enabled = self.bundles.contains(&ToolBundle::Invocation);
        if invocation_enabled && !self.raw_command_policy.is_enabled() {
            return Err(RedisMcpBuildError::InvocationRequiresRawCommands);
        }
        let sessions_enabled =
            cfg!(feature = "sessions") && self.bundles.contains(&ToolBundle::Sessions);
        if sessions_enabled
            && self.pubsub_sessions.is_none()
            && self.blocking.is_none()
            && self.monitor_sessions.is_none()
        {
            return Err(RedisMcpBuildError::SessionsRequireManager);
        }
        if sessions_enabled && self.blocking.is_some() && self.blocking_limits.validate().is_err() {
            return Err(RedisMcpBuildError::InvalidBlockingLimits);
        }
        if self.docs_fetcher.is_some() && self.docs_options.validate().is_err() {
            return Err(RedisMcpBuildError::InvalidDocsOptions);
        }
        #[cfg(feature = "coordination")]
        let coordination_enabled = self.bundles.contains(&ToolBundle::Coordination);
        #[cfg(feature = "coordination")]
        if coordination_enabled {
            self.coordination_config
                .validate()
                .map_err(|_| RedisMcpBuildError::InvalidCoordinationConfig)?;
            if self.blocking.is_some() && self.blocking_limits.validate().is_err() {
                return Err(RedisMcpBuildError::InvalidBlockingLimits);
            }
        }
        let transactions_enabled =
            cfg!(feature = "transactions") && self.bundles.contains(&ToolBundle::Transactions);
        if transactions_enabled {
            if self.transactions.is_none() {
                return Err(RedisMcpBuildError::TransactionsRequireExecutor);
            }
            if !self.raw_command_policy.is_enabled() || !self.access.permits(AccessMode::Full) {
                return Err(RedisMcpBuildError::TransactionsRequireRawCommands);
            }
            if self.transaction_limits.validate().is_err() {
                return Err(RedisMcpBuildError::InvalidTransactionLimits);
            }
        }
        if let Some(family) = self
            .families
            .as_ref()
            .and_then(|families| families.iter().find(|family| !family.is_compiled()))
        {
            return Err(RedisMcpBuildError::FamilyNotCompiled(*family));
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
        let session_owner = (sessions_enabled
            && (self.pubsub_sessions.is_some()
                || self.blocking.is_some()
                || self.monitor_sessions.is_some()))
        .then(PubSubSessionOwner::random);
        let transaction_engine = transactions_enabled
            .then_some(self.transactions)
            .flatten()
            .map(|executor| {
                RedisTransactionEngine::from_shared(invocation_engine.clone(), executor)
                    .with_limits(self.transaction_limits)
            });
        let blocking_engine = (sessions_enabled
            || cfg!(feature = "coordination") && self.bundles.contains(&ToolBundle::Coordination))
        .then_some(self.blocking)
        .flatten()
        .map(|executor| {
            RedisBlockingEngine::from_shared(executor).with_limits(self.blocking_limits)
        });
        let monitor_sessions = sessions_enabled.then_some(self.monitor_sessions).flatten();
        #[cfg(feature = "coordination")]
        let coordination_principal = self
            .bundles
            .contains(&ToolBundle::Coordination)
            .then(CoordinationPrincipal::random);
        let state = Arc::new(tools::ToolState::new(
            self.access,
            self.output_budget,
            invocation_engine,
            self.pubsub_sessions,
            transaction_engine,
            self.bulk_limits,
            blocking_engine,
            monitor_sessions,
            #[cfg(feature = "coordination")]
            self.coordination_config,
        ));
        let mut router = McpRouter::new().server_info(self.server_name, self.server_version);
        if let Some(owner) = session_owner {
            router = router.with_extension(owner.clone());
            if let Some(manager) = &state.pubsub_sessions {
                router = router.with_extension(Arc::new(PubSubOwnerCleanup {
                    manager: manager.clone(),
                    owner: owner.clone(),
                }));
            }
            if let Some(manager) = &state.monitor_sessions {
                router = router.with_extension(Arc::new(MonitorOwnerCleanup {
                    manager: manager.clone(),
                    owner,
                }));
            }
        }
        #[cfg(feature = "coordination")]
        if let Some(principal) = coordination_principal {
            router = router.with_extension(principal);
        }
        router = tools::add_read_only_tools(
            router,
            state.clone(),
            &self.bundles,
            self.families.as_ref(),
        );
        if invocation_enabled {
            router = tools::add_invocation_read_tools(router, state.clone());
        }
        if self.access.permits(AccessMode::ReadWrite) {
            router = tools::add_write_tools(
                router,
                state.clone(),
                &self.bundles,
                self.families.as_ref(),
            );
            if invocation_enabled {
                router = tools::add_invocation_write_tools(router, state.clone());
            }
        }
        if self.access.permits(AccessMode::Full) {
            router = tools::add_destructive_tools(
                router,
                state.clone(),
                &self.bundles,
                self.families.as_ref(),
            );
            #[cfg(feature = "transactions")]
            if transactions_enabled {
                router = tools::add_transaction_tool(router, state.clone());
            }
            if self.raw_command_policy.is_enabled() {
                router = tools::add_raw_tool(router, state.clone());
            }
        }
        #[cfg(feature = "guidance")]
        if self.bundles.contains(&ToolBundle::Guidance) {
            router = guidance::add_guidance(router, &capabilities);
        }
        #[cfg(feature = "coordination")]
        if self.bundles.contains(&ToolBundle::Coordination) {
            router = coordination::add_resources(router);
            router = tools::add_coordination_resources(router, state);
        }
        if let Some(fetcher) = self.docs_fetcher {
            router = docs::add_docs(router, fetcher, self.docs_options);
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
    /// Unrestricted raw command execution permits unknown Full-tier commands
    /// and therefore requires full access in addition to its policy opt-in.
    RawCommandsRequireFullAccess,
    /// Governed argv invocation tools classify through the raw command policy
    /// and require it to be enabled.
    InvocationRequiresRawCommands,
    /// Stateful session tools require at least one explicit lifecycle
    /// backend: a Pub/Sub session manager, a blocking executor, or a MONITOR
    /// session manager.
    SessionsRequireManager,
    /// Blocking timeout, key, and count bounds must be non-zero.
    InvalidBlockingLimits,
    /// Documentation reads require a well-formed pin and non-zero bounds.
    InvalidDocsOptions,
    /// Durable coordination requires a valid namespace and non-zero bounds.
    InvalidCoordinationConfig,
    /// Atomic transactions require an explicit dedicated-connection executor.
    TransactionsRequireExecutor,
    /// Transactions execute argv-shaped nested commands and therefore require
    /// the raw command policy in addition to their executor.
    TransactionsRequireRawCommands,
    /// Transaction command, byte, and duration bounds must be non-zero.
    InvalidTransactionLimits,
    /// A precise runtime family selection referenced a handler family omitted
    /// from this crate's Cargo feature set.
    FamilyNotCompiled(ToolFamily),
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
                formatter.write_str("unrestricted raw command execution requires full access")
            }
            Self::InvocationRequiresRawCommands => {
                formatter.write_str("the invocation bundle requires an enabled raw command policy")
            }
            Self::SessionsRequireManager => formatter.write_str(
                "the sessions bundle requires a Pub/Sub session manager, a blocking executor, or a MONITOR session manager",
            ),
            Self::InvalidBlockingLimits => {
                formatter.write_str("blocking limits must all be greater than zero")
            }
            Self::InvalidDocsOptions => formatter.write_str(
                "documentation options require a well-formed pin and non-zero bounds",
            ),
            Self::InvalidCoordinationConfig => formatter.write_str(
                "coordination options require a safe namespace and non-zero shard and byte bounds",
            ),
            Self::TransactionsRequireExecutor => {
                formatter.write_str("the transactions bundle requires a transaction executor")
            }
            Self::TransactionsRequireRawCommands => formatter.write_str(
                "the transactions bundle requires an enabled raw command policy and full access",
            ),
            Self::InvalidTransactionLimits => {
                formatter.write_str("transaction limits must all be greater than zero")
            }
            Self::FamilyNotCompiled(family) => write!(
                formatter,
                "the {family} family requires the `{}` Cargo feature",
                family.feature_name()
            ),
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

/// Tool names exposed for a precise Redis family selection.
///
/// This helper covers data families only. Cross-cutting bundles and the raw
/// command escape hatch remain separate policy choices.
pub fn tool_names_for_families(
    access: AccessMode,
    families: impl IntoIterator<Item = ToolFamily>,
) -> Vec<&'static str> {
    let families = families.into_iter().collect::<Vec<_>>();
    catalog::selected_family_tool_names(access, &families)
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
