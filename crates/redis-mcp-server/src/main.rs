#![forbid(unsafe_code)]

use clap::{Parser, ValueEnum};
use std::sync::Arc;

use redis_mcp::{
    AccessMode, DirectRedis, DirectRedisCluster, DirectRedisPubSubSessionManager,
    PubSubSessionLimits, PubSubSessionManager, RawCommandPolicy, RedisExecutor, RedisMcp,
    ToolBundle,
};
use tower_mcp::{McpRouter, ProtocolSupport, StdioTransport};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliAccessMode {
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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CliOptionalBundle {
    Admin,
    Json,
    Search,
    Scripting,
}

impl From<CliOptionalBundle> for ToolBundle {
    fn from(value: CliOptionalBundle) -> Self {
        match value {
            CliOptionalBundle::Admin => Self::Admin,
            CliOptionalBundle::Json => Self::Json,
            CliOptionalBundle::Search => Self::Search,
            CliOptionalBundle::Scripting => Self::Scripting,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "redis-mcp-server", version, about)]
struct Args {
    /// Fixed standalone Redis target. Defaults to redis://127.0.0.1:6379.
    #[arg(long, env = "REDIS_URL", conflicts_with = "cluster_urls")]
    url: Option<String>,

    /// Redis Cluster seed URL. Repeat for multiple seeds; conflicts with --url.
    #[arg(
        long = "cluster-url",
        env = "REDIS_CLUSTER_URLS",
        value_delimiter = ',',
        conflicts_with = "url"
    )]
    cluster_urls: Vec<String>,

    /// Maximum side-effect level to expose.
    #[arg(long, value_enum, default_value = "read-only")]
    access: CliAccessMode,

    /// Expose classified redis_command operations. Requires --access full.
    #[arg(long)]
    raw: bool,

    /// Expose unclassified request/response commands too. Requires --access full.
    #[arg(long, conflicts_with = "raw")]
    raw_unrestricted: bool,

    /// Add an optional tool bundle to the curated defaults.
    #[arg(long = "enable-bundle", value_enum)]
    optional_bundles: Vec<CliOptionalBundle>,

    /// Explicit transport marker for MCP client configurations. Stdio is always used.
    #[arg(long)]
    stdio: bool,
}

fn build_router(
    executor: impl RedisExecutor,
    access: AccessMode,
    raw_command_policy: RawCommandPolicy,
    optional_bundles: &[CliOptionalBundle],
    pubsub_sessions: Arc<dyn PubSubSessionManager>,
) -> McpRouter {
    let mut builder = RedisMcp::builder(executor)
        .access(access)
        .raw_command_policy(raw_command_policy)
        .shared_pubsub_sessions(pubsub_sessions)
        .server_info("redis-mcp-server", env!("CARGO_PKG_VERSION"));
    for bundle in optional_bundles {
        builder = builder.bundle((*bundle).into());
    }
    builder.build()
}

#[tokio::main]
async fn main() -> Result<(), tower_mcp::BoxError> {
    let args = Args::parse();
    let access = AccessMode::from(args.access);
    let raw_command_policy = if args.raw_unrestricted {
        RawCommandPolicy::Unrestricted
    } else if args.raw {
        RawCommandPolicy::Classified
    } else {
        RawCommandPolicy::Disabled
    };
    if raw_command_policy != RawCommandPolicy::Disabled && access != AccessMode::Full {
        return Err("--raw and --raw-unrestricted require --access full".into());
    }

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();

    let cluster_mode = !args.cluster_urls.is_empty();
    let (router, pubsub_sessions): (McpRouter, Arc<dyn PubSubSessionManager>) = if cluster_mode {
        let executor = DirectRedisCluster::connect(&args.cluster_urls).await?;
        let sessions = Arc::new(DirectRedisPubSubSessionManager::cluster(
            &args.cluster_urls,
            PubSubSessionLimits::default(),
        )?);
        (
            build_router(
                executor,
                access,
                raw_command_policy,
                &args.optional_bundles,
                sessions.clone(),
            ),
            sessions,
        )
    } else {
        let url = args.url.as_deref().unwrap_or("redis://127.0.0.1:6379");
        let executor = DirectRedis::connect(url).await?;
        let sessions = Arc::new(DirectRedisPubSubSessionManager::standalone(
            url,
            PubSubSessionLimits::default(),
        )?);
        (
            build_router(
                executor,
                access,
                raw_command_policy,
                &args.optional_bundles,
                sessions.clone(),
            ),
            sessions,
        )
    };

    let topology = if cluster_mode {
        "cluster"
    } else {
        "standalone"
    };
    info!(
        ?access,
        ?raw_command_policy,
        topology,
        "Redis MCP server ready"
    );
    let protocols = ProtocolSupport::try_new(["2025-11-25", "2026-07-28"])?;
    let mut transport = StdioTransport::new(router).protocol_support(protocols);
    let handle = transport.handle();
    tokio::spawn(async move {
        handle.stopping().await;
        pubsub_sessions.shutdown().await;
    });
    transport.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scripting_bundle_is_selectable_from_the_server_cli() {
        let args = Args::try_parse_from([
            "redis-mcp-server",
            "--access",
            "full",
            "--enable-bundle",
            "scripting",
            "--stdio",
        ])
        .expect("parse scripting server arguments");

        assert_eq!(args.optional_bundles.len(), 1);
        assert!(matches!(
            ToolBundle::from(args.optional_bundles[0]),
            ToolBundle::Scripting
        ));
    }

    #[test]
    fn admin_bundle_is_selectable_from_the_server_cli() {
        let args = Args::try_parse_from([
            "redis-mcp-server",
            "--access",
            "full",
            "--enable-bundle",
            "admin",
            "--stdio",
        ])
        .expect("parse admin server arguments");

        assert_eq!(args.optional_bundles.len(), 1);
        assert!(matches!(
            ToolBundle::from(args.optional_bundles[0]),
            ToolBundle::Admin
        ));
    }
}
