//! One-shot assembly of the configured Redis MCP runtime.
//!
//! Every transport serves the same assembled router: the executor, session
//! managers, capability snapshot, and Tower-MCP router are constructed here
//! exactly once from a resolved [`ServerConfig`], and shutdown releases the
//! same session resources regardless of how the router was served.

use std::sync::Arc;

use redis_mcp::{
    DirectRedis, DirectRedisBlocking, DirectRedisCluster, DirectRedisMonitorSessions,
    DirectRedisPubSubSessionManager, DirectRedisTransactions, MonitorSessionManager,
    PubSubSessionManager, RedisCapabilities, RedisExecutor, RedisMcp, RedisMcpBuilder,
};
use tower_mcp::McpRouter;
use tracing::warn;

use crate::config::{ServerConfig, ServerTarget};

/// The assembled server runtime: one router plus the session managers whose
/// lifecycles outlive individual requests.
pub(crate) struct ServerRuntime {
    pub(crate) router: McpRouter,
    pub(crate) topology: &'static str,
    pub(crate) sessions: ServerSessions,
}

/// The session managers a transport must shut down when it stops serving.
pub(crate) struct ServerSessions {
    pubsub: Arc<dyn PubSubSessionManager>,
    monitor: Option<Arc<dyn MonitorSessionManager>>,
}

impl ServerSessions {
    /// Close every owner-scoped and global session.
    pub(crate) async fn shutdown(&self) {
        self.pubsub.shutdown().await;
        if let Some(monitor) = &self.monitor {
            monitor.shutdown().await;
        }
    }
}

impl ServerRuntime {
    /// Build the executor, session managers, capability snapshot, and router
    /// from a resolved configuration.
    ///
    /// Failure messages describe the failing component and never echo
    /// connection URLs, which may embed credentials.
    pub(crate) async fn build(config: &ServerConfig) -> Result<Self, tower_mcp::BoxError> {
        // A bundle explicitly requested at runtime must exist in this binary;
        // silently serving a smaller surface would read as success. Checked
        // before any connection is dialed so slim misconfiguration fails
        // instantly and offline.
        for bundle in &config.bundles {
            let compiled = redis_mcp::tool_catalog()
                .iter()
                .any(|tool| tool.bundle == *bundle && tool.is_compiled());
            if !compiled {
                return Err(format!(
                    "the {bundle} bundle is not compiled into this binary; rebuild with the `{bundle}` Cargo feature (or without --no-default-features)"
                )
                .into());
            }
        }
        #[cfg(not(feature = "docs"))]
        if config.docs_enabled {
            return Err(
                "documentation serving is not compiled into this binary; rebuild with the `docs` Cargo feature (or without --no-default-features)"
                    .into(),
            );
        }
        match &config.target {
            ServerTarget::Cluster(urls) => {
                let executor = DirectRedisCluster::connect(urls).await.map_err(|error| {
                    format!("cannot connect to the Redis Cluster target: {error}")
                })?;
                let capabilities = discover(config, &executor, "cluster").await;
                let pubsub_sessions: Arc<dyn PubSubSessionManager> = Arc::new(
                    DirectRedisPubSubSessionManager::cluster(urls, config.pubsub_limits)
                        .map_err(|error| format!("cannot prepare Pub/Sub sessions: {error}"))?,
                );
                let blocking = DirectRedisBlocking::cluster(urls)
                    .map_err(|error| format!("cannot prepare blocking calls: {error}"))?
                    .with_max_concurrent_calls(config.blocking_max_concurrent_calls)
                    .map_err(|error| format!("invalid blocking concurrency: {error}"))?;
                let transactions = config
                    .transactions
                    .then(|| {
                        DirectRedisTransactions::cluster(urls).and_then(|transactions| {
                            transactions
                                .with_max_concurrent_transactions(config.transaction_max_concurrent)
                        })
                    })
                    .transpose()
                    .map_err(|error| format!("cannot prepare transactions: {error}"))?;
                let router = Self::assemble(
                    config,
                    executor,
                    capabilities,
                    pubsub_sessions.clone(),
                    blocking,
                    // MONITOR streams are node-local; the fixed-target server
                    // does not select a Cluster node to observe.
                    None,
                    transactions,
                )?;
                Ok(Self {
                    router,
                    topology: "cluster",
                    sessions: ServerSessions {
                        pubsub: pubsub_sessions,
                        monitor: None,
                    },
                })
            }
            ServerTarget::Standalone(url) => {
                let executor = DirectRedis::connect(url)
                    .await
                    .map_err(|error| format!("cannot connect to the Redis target: {error}"))?;
                let capabilities = discover(config, &executor, "standalone").await;
                let pubsub_sessions: Arc<dyn PubSubSessionManager> = Arc::new(
                    DirectRedisPubSubSessionManager::standalone(url, config.pubsub_limits)
                        .map_err(|error| format!("cannot prepare Pub/Sub sessions: {error}"))?,
                );
                let blocking = DirectRedisBlocking::standalone(url)
                    .map_err(|error| format!("cannot prepare blocking calls: {error}"))?
                    .with_max_concurrent_calls(config.blocking_max_concurrent_calls)
                    .map_err(|error| format!("invalid blocking concurrency: {error}"))?;
                let monitor_sessions: Arc<dyn MonitorSessionManager> = Arc::new(
                    DirectRedisMonitorSessions::standalone(url, config.monitor_limits)
                        .map_err(|error| format!("cannot prepare MONITOR sessions: {error}"))?,
                );
                let transactions = config
                    .transactions
                    .then(|| {
                        DirectRedisTransactions::standalone(url).and_then(|transactions| {
                            transactions
                                .with_max_concurrent_transactions(config.transaction_max_concurrent)
                        })
                    })
                    .transpose()
                    .map_err(|error| format!("cannot prepare transactions: {error}"))?;
                let router = Self::assemble(
                    config,
                    executor,
                    capabilities,
                    pubsub_sessions.clone(),
                    blocking,
                    Some(monitor_sessions.clone()),
                    transactions,
                )?;
                Ok(Self {
                    router,
                    topology: "standalone",
                    sessions: ServerSessions {
                        pubsub: pubsub_sessions,
                        monitor: Some(monitor_sessions),
                    },
                })
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn assemble(
        config: &ServerConfig,
        executor: impl RedisExecutor,
        capabilities: Option<RedisCapabilities>,
        pubsub_sessions: Arc<dyn PubSubSessionManager>,
        blocking: DirectRedisBlocking,
        monitor_sessions: Option<Arc<dyn MonitorSessionManager>>,
        transactions: Option<DirectRedisTransactions>,
    ) -> Result<McpRouter, tower_mcp::BoxError> {
        let mut builder: RedisMcpBuilder = RedisMcp::builder(executor)
            .access(config.access)
            .raw_command_policy(config.raw_policy)
            .output_budget(redis_mcp::OutputBudget::new(
                config.max_output_bytes,
                config.max_output_entries,
            ))
            .command_timeout(config.command_timeout)
            .unavailable_tool_policy(config.unavailable_tools)
            .shared_pubsub_sessions(pubsub_sessions)
            .blocking(blocking)
            .blocking_limits(config.blocking_limits)
            .bulk_limits(config.bulk_limits)
            .server_info("redis-mcp-server", env!("CARGO_PKG_VERSION"));
        if let Some(capabilities) = capabilities {
            builder = builder.capabilities(capabilities);
        }
        if let Some(monitor_sessions) = monitor_sessions {
            builder = builder.shared_monitor_sessions(monitor_sessions);
        }
        if let Some(transactions) = transactions {
            builder = builder
                .transactions(transactions)
                .transaction_limits(config.transaction_limits);
        }
        #[cfg(feature = "docs")]
        if config.docs_enabled {
            builder = builder
                .docs_fetcher(crate::docs::HttpDocsFetcher::new()?)
                .docs_options(config.docs_options.clone());
        }
        for bundle in &config.bundles {
            builder = builder.bundle(*bundle);
        }
        builder
            .try_build()
            .map_err(|error| format!("invalid router configuration: {error}").into())
    }
}

/// Discover the target's capabilities unless discovery is disabled.
///
/// A reachable target whose ACL blocks introspection should not prevent
/// startup, so discovery failures degrade to an unknown snapshot with a
/// warning instead of exiting.
async fn discover<E>(
    config: &ServerConfig,
    executor: &E,
    topology: &'static str,
) -> Option<RedisCapabilities>
where
    E: Discover,
{
    if !config.discover_capabilities {
        return None;
    }
    match executor.discover().await {
        Ok(capabilities) => Some(capabilities),
        Err(error) => {
            warn!(
                topology,
                %error,
                "capability discovery failed; advertising the selected catalog without version awareness"
            );
            None
        }
    }
}

/// Uniform discovery over the two direct executor types.
trait Discover {
    fn discover(
        &self,
    ) -> impl std::future::Future<Output = Result<RedisCapabilities, redis_mcp::RedisError>>;
}

impl Discover for DirectRedis {
    async fn discover(&self) -> Result<RedisCapabilities, redis_mcp::RedisError> {
        self.discover_capabilities().await
    }
}

impl Discover for DirectRedisCluster {
    async fn discover(&self) -> Result<RedisCapabilities, redis_mcp::RedisError> {
        self.discover_capabilities().await
    }
}
