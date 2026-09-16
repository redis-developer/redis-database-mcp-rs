#![forbid(unsafe_code)]

mod config;
#[cfg(feature = "docs")]
mod docs;
mod runtime;

use clap::Parser;
use tower_mcp::{ProtocolSupport, StdioTransport};
use tracing::info;
use tracing_subscriber::EnvFilter;

use config::Args;
use runtime::ServerRuntime;

#[tokio::main]
async fn main() -> Result<(), tower_mcp::BoxError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();

    let args = Args::parse();
    let env = |name: &str| std::env::var(name).ok();
    let file = config::load_file(&args, &env)?;
    let resolved = config::resolve(&args, file.as_ref(), &env)?;

    let runtime = ServerRuntime::build(&resolved).await?;
    info!(
        access = ?resolved.access,
        raw_command_policy = ?resolved.raw_policy,
        topology = runtime.topology,
        "Redis MCP server ready"
    );

    let protocols = ProtocolSupport::try_new(["2025-11-25", "2026-07-28"])?;
    let mut transport = StdioTransport::new(runtime.router).protocol_support(protocols);
    let handle = transport.handle();
    let sessions = runtime.sessions;
    tokio::spawn(async move {
        handle.stopping().await;
        sessions.shutdown().await;
    });
    transport.run().await?;
    Ok(())
}
