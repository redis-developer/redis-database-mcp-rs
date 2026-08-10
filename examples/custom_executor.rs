//! Compile-time example of a host adapter that does not use redis-rs types.

use std::sync::Arc;

use async_trait::async_trait;
use redis_mcp::{
    AccessMode, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisMcp, RedisValue,
};

/// A host-owned connection abstraction. In redisctl this layer can resolve a
/// profile, use its connection cache, and record audit/telemetry data.
#[async_trait]
trait HostConnection: Send + Sync {
    async fn send(
        &self,
        command_name: &str,
        arguments: &[Vec<u8>],
    ) -> Result<RedisValue, RedisError>;
}

#[derive(Clone)]
struct ProfileAwareExecutor {
    selected_profile: Arc<str>,
    connection: Arc<dyn HostConnection>,
}

#[async_trait]
impl RedisExecutor for ProfileAwareExecutor {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        // `tool_name` and `required_access` are available for host audit and
        // policy records. Command arguments are intentionally not Debug-printed.
        let _audit_context = (
            self.selected_profile.as_ref(),
            command.tool_name(),
            command.required_access(),
        );
        self.connection
            .send(command.name(), command.arguments())
            .await
    }
}

struct ExampleConnection;

#[async_trait]
impl HostConnection for ExampleConnection {
    async fn send(
        &self,
        command_name: &str,
        _arguments: &[Vec<u8>],
    ) -> Result<RedisValue, RedisError> {
        match command_name {
            "PING" => Ok(RedisValue::SimpleString("PONG".to_string())),
            _ => Err(RedisError::new(
                RedisErrorKind::Other,
                "example connection only implements PING",
            )),
        }
    }
}

fn main() {
    // The host resolves its profile before constructing the adapter. Profile
    // names and credential-bearing URLs never enter the Redis tool schemas.
    let executor = ProfileAwareExecutor {
        selected_profile: Arc::from("production-readonly"),
        connection: Arc::new(ExampleConnection),
    };
    let router = RedisMcp::builder(executor)
        .access(AccessMode::ReadOnly)
        .build();

    // A host can serve or merge the Tower-MCP router using its chosen transport.
    let _ = router;
}
