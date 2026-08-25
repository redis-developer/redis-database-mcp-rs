//! Compile-time example of a host adapter that does not use redis-rs types.

use std::sync::Arc;

use async_trait::async_trait;
use redis_mcp::{
    AccessMode, RedisCapabilities, RedisCommand, RedisDeployment, RedisError, RedisErrorKind,
    RedisExecutor, RedisMcp, RedisValue, RedisVersion, families,
};
use tower_mcp::McpRouter;

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
        // Tool, access, and module requirements are available for host audit,
        // routing, and policy records. Arguments are intentionally not logged.
        let _audit_context = (
            self.selected_profile.as_ref(),
            command.tool_name(),
            command.required_access(),
            command.required_module(),
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
    // This authoritative information is host-owned and uses only redis-mcp
    // types. Omitted facts remain unknown rather than unavailable.
    let capabilities = RedisCapabilities::unknown()
        .with_redis_version(RedisVersion::new(7, 4, 0))
        .with_deployment(RedisDeployment::Standalone)
        .with_command_inventory(["PING"]);
    let redis = RedisMcp::builder(executor)
        .access(AccessMode::ReadOnly)
        .family(families::keyspace::FAMILY)
        .capabilities(capabilities)
        .build();

    // redisctl can keep its own identity and other product routers while the
    // selected Redis families remain one shared library-owned fragment.
    let router = McpRouter::new()
        .server_info("redisctl", env!("CARGO_PKG_VERSION"))
        .merge(redis);

    // A host serves the composed Tower-MCP router using its chosen transport.
    let _ = router;
}
