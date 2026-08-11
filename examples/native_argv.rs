use redis_mcp::{
    AccessMode, DirectRedis, NativeRedisInvocation, RawCommandPolicy, RedisError,
    RedisInvocationEngine, RedisValue,
};

/// Adapt already-tokenized Redis argv into the governed library service.
///
/// Tokenization, quoting, history, completion, and rendering belong to the
/// calling CLI or REPL. Each token remains binary-safe at this boundary.
async fn invoke_argv(
    engine: &RedisInvocationEngine,
    argv: Vec<Vec<u8>>,
) -> Result<RedisValue, RedisError> {
    engine.invoke(NativeRedisInvocation::from_argv(argv)?).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let executor = DirectRedis::connect(&url).await?;
    let engine = RedisInvocationEngine::builder(executor)
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();

    // A real frontend supplies this vector after applying its own Redis-style
    // tokenization. The library neither reparses nor assumes UTF-8 arguments.
    let value = invoke_argv(
        &engine,
        vec![b"ECHO".to_vec(), b"hello from native argv".to_vec()],
    )
    .await?;

    println!("{value:?}");
    Ok(())
}
