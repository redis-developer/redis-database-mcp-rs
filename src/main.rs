#![forbid(unsafe_code)]

use clap::{Parser, ValueEnum};
use redis_mcp::{AccessMode, DirectRedis, RawCommandPolicy, RedisMcp};
use tower_mcp::{ProtocolSupport, StdioTransport};
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

#[derive(Debug, Parser)]
#[command(name = "redis-mcp-server", version, about)]
struct Args {
    /// Fixed Redis target. The URL is server configuration and is never exposed as a tool argument.
    #[arg(long, env = "REDIS_URL", default_value = "redis://127.0.0.1:6379")]
    url: String,

    /// Maximum side-effect level to expose.
    #[arg(long, value_enum, default_value = "read-only")]
    access: CliAccessMode,

    /// Expose classified redis_command operations. Requires --access full.
    #[arg(long)]
    raw: bool,

    /// Expose unclassified request/response commands too. Requires --access full.
    #[arg(long, conflicts_with = "raw")]
    raw_unrestricted: bool,

    /// Explicit transport marker for MCP client configurations. Stdio is always used.
    #[arg(long)]
    stdio: bool,
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

    let executor = DirectRedis::connect(&args.url).await?;
    let router = RedisMcp::builder(executor)
        .access(access)
        .raw_command_policy(raw_command_policy)
        .server_info("redis-mcp-server", env!("CARGO_PKG_VERSION"))
        .build();

    info!(?access, ?raw_command_policy, "Redis MCP server ready");
    let protocols = ProtocolSupport::try_new(["2025-11-25", "2026-07-28"])?;
    StdioTransport::new(router)
        .protocol_support(protocols)
        .run()
        .await?;
    Ok(())
}
