use async_trait::async_trait;
use redis_mcp::{
    AccessMode, RedisCommand, RedisDeployment, RedisError, RedisErrorKind, RedisExecutor,
    RedisMcp, RedisValue, ToolFamily, validate_redis_target_url,
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
    validate_redis_target_url(
        "redis://127.0.0.1:1/",
        RedisDeployment::Standalone,
    )
    .expect("offline standalone target validation");
    assert_eq!(
        validate_redis_target_url("redis://127.0.0.1:1/1", RedisDeployment::Cluster)
            .expect_err("Cluster rejects nonzero database")
            .code(),
        Some("CLUSTER_DATABASE_UNSUPPORTED")
    );
    let router = RedisMcp::builder(StubRedis)
        .access(AccessMode::ReadOnly)
        .family(ToolFamily::Strings)
        .build();
    drop(router);
}
