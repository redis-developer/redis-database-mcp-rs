use async_trait::async_trait;
use redis_mcp::{
    AccessMode, ConnectionSetup, DirectRedis, DirectRedisCluster, RedisCommand, RedisError,
    RedisErrorKind, RedisExecutor, RedisMcp, RedisValue, ToolFamily,
};

#[cfg(not(any(
    feature = "minimal",
    feature = "library-default",
    feature = "library-full"
)))]
compile_error!("select one release-consumer feature");

#[derive(Clone, Copy)]
struct StubRedis;

#[async_trait]
impl RedisExecutor for StubRedis {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        Err(RedisError::new(
            RedisErrorKind::Other,
            format!("{} is not executed by the release consumer", command.name()),
        ))
    }
}

fn main() {
    let router = RedisMcp::builder(StubRedis)
        .access(AccessMode::ReadOnly)
        .family(ToolFamily::Strings)
        .build();
    drop(router);
}

// Compile the public setup seam from an external crate. The release gate does
// not connect to Redis or depend on a running server.
#[allow(dead_code)]
async fn direct_connection_setup_api() -> Result<(), RedisError> {
    let setup = ConnectionSetup::new().with_client_name("redis-mcp-consumer");
    let _standalone = DirectRedis::connect_with_setup("redis://127.0.0.1:6379", setup.clone()).await?;
    let _cluster = DirectRedisCluster::connect_with_setup(["redis://127.0.0.1:7000"], setup).await?;
    Ok(())
}
