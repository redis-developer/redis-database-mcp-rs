# redis-mcp

Composable, Tower-native MCP tools for Redis databases.

This repository is an intentionally narrow extraction target for the Redis
database tools currently hosted by redisctl-mcp. The library returns an
McpRouter; applications own transport, configuration, identity, and policy
outside the Redis database boundary.

The repository is named redis-database-mcp-rs to distinguish this surface from
Redis Cloud and Redis Enterprise APIs. The Rust package is simply redis-mcp.

## Spike scope

The initial curated surface is deliberately small:

- read-only: redis_ping, redis_info, redis_dbsize, redis_scan, redis_get,
  redis_type, redis_ttl
- read-write: redis_set
- full: redis_del
- explicit full-access escape hatch: redis_command

Every successful tool result includes MCP structuredContent and an output
schema. A fixed Redis target is configured once by the server; arbitrary URLs
are not accepted in tool calls.

See [the spike decision record](docs/spike.md) for the tested architecture,
REPL findings, and redisctl migration sequence.

## Run the standalone server

    cargo run --bin redis-mcp-server -- \
      --url redis://127.0.0.1:6379 \
      --access read-write \
      --stdio

Use it with any stdio MCP client. With
[mcp-repl](https://github.com/joshrotenberg/mcp-repl):

    mcp-repl -- redis-mcp-server \
      --url redis://127.0.0.1:6379 \
      --access read-write \
      --stdio

Inside the REPL:

    redis_ping
    redis_set key=greeting value=hello
    redis_get key=greeting
    redis_scan pattern=gre* count=20

Raw commands require two independent opt-ins:

    redis-mcp-server --access full --raw --stdio

## Embed the router

    use redis_mcp::{AccessMode, DirectRedis, RedisMcp};

    # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
    let router = RedisMcp::builder(redis)
        .access(AccessMode::ReadWrite)
        .build();

    // Serve or merge router in the host application.
    # let _ = router;
    # Ok(())
    # }

Hosts with their own connection lifecycle implement RedisExecutor. That is the
intended seam for redisctl profiles, connection caching, audit context, and
future cluster-aware routing.

## Non-goals for this spike

- Cloud or Enterprise REST APIs
- redisctl profiles or per-tool target URLs
- transactions, Pub/Sub, MONITOR, or other streaming/session-oriented commands
- recreating the complete redisctl database catalog before the REPL experience
  is evaluated

## License

MIT or Apache-2.0, at your option.
