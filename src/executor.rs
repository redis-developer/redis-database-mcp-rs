//! Redis command execution abstractions.

use async_trait::async_trait;
use redis::aio::ConnectionManager;

/// Executes one Redis command for an MCP tool.
///
/// The trait is intentionally smaller than a connection pool API. Hosts such as
/// redisctl can supply profile-aware routing, credentials, telemetry, or their
/// own connection lifecycle without exposing those concerns in tool schemas.
#[async_trait]
pub trait RedisExecutor: Send + Sync + 'static {
    /// Execute a command and return its RESP value.
    async fn execute(&self, command: redis::Cmd) -> redis::RedisResult<redis::Value>;
}

/// A fixed Redis target backed by redis-rs' reconnecting connection manager.
#[derive(Clone)]
pub struct DirectRedis {
    connection: ConnectionManager,
}

impl DirectRedis {
    /// Connect to a fixed Redis URL.
    pub async fn connect(url: &str) -> redis::RedisResult<Self> {
        let client = redis::Client::open(url)?;
        let connection = ConnectionManager::new(client).await?;
        Ok(Self { connection })
    }

    /// Wrap an existing redis-rs connection manager.
    pub fn from_connection_manager(connection: ConnectionManager) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl RedisExecutor for DirectRedis {
    async fn execute(&self, command: redis::Cmd) -> redis::RedisResult<redis::Value> {
        let mut connection = self.connection.clone();
        command.query_async(&mut connection).await
    }
}
