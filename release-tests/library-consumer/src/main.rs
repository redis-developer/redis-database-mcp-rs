use async_trait::async_trait;
use redis_mcp::{
    AccessMode, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisMcp, RedisValue,
    ToolFamily,
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
