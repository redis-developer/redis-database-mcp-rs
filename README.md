# redis-mcp

Composable, Tower-native MCP tools for Redis databases.

This repository is the standalone home for Redis database MCP tools. The
library returns an `McpRouter`; applications own transport, configuration,
identity, target selection, and product policy outside the Redis database
boundary.

The repository is named redis-database-mcp-rs to distinguish this surface from
Redis Cloud and Redis Enterprise APIs. The Rust package is simply redis-mcp.

## Curated default

The standalone default exposes 29 broadly useful tools:

- read-only essentials: `redis_ping`, `redis_dbsize`, `redis_scan`,
  `redis_get`, `redis_type`, `redis_ttl`, `redis_exists`, `redis_mget`,
  `redis_strlen`, `redis_memory_usage`, `redis_randomkey`
- read-write essentials: `redis_set`, `redis_expire`, `redis_persist`,
  `redis_mset`, `redis_incr`, `redis_append`
- full-access essentials: `redis_del`, `redis_unlink`
- data structures: `redis_hget`, `redis_hgetall`, `redis_hset`,
  `redis_lrange`, `redis_lpush`, `redis_smembers`, `redis_sadd`,
  `redis_zrange`, `redis_zadd`
- diagnostics: `redis_info`
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

Classified raw commands require full access plus their own opt-in. Unknown
commands fail closed:

    redis-mcp-server --access full --raw --stdio

An intentionally stronger flag permits unclassified request/response commands
while retaining hard blocks for session, streaming, transaction, replication,
script, and indefinite-blocking forms:

    redis-mcp-server --access full --raw-unrestricted --stdio

## Embed the router

    use std::time::Duration;
    use redis_mcp::{AccessMode, DirectRedis, RedisMcp, ToolBundle};

    # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
    let router = RedisMcp::builder(redis)
        .access(AccessMode::ReadWrite)
        .bundles([ToolBundle::Essentials, ToolBundle::Diagnostics])
        .command_timeout(Duration::from_secs(10))
        .build();

    // Serve or merge router in the host application.
    # let _ = router;
    # Ok(())
    # }

Hosts with their own connection lifecycle implement `RedisExecutor` using
crate-owned `RedisCommand`, `RedisValue`, and `RedisError` types. They do not
need to share this crate's redis-rs dependency line. Commands include the
originating tool and required access level for host telemetry and audit
records. See [the custom executor example](examples/custom_executor.rs).

The curated default enables the `essentials`, `data_structures`, and
`diagnostics` bundles. The public taxonomy also reserves `search`, `admin`,
`bulk`, and `raw` for deliberate composition as the catalog grows. Raw
execution is always controlled by its separate policy rather than bundle
selection alone.

See [the architecture decisions](docs/architecture.md) for the intentional
Tower-MCP boundary and fixed-target model, and the
[redisctl compatibility inventory](docs/redisctl-compatibility.md) for the
132-tool read-only baseline and known contract differences.

## Compatibility and testing

CI runs the complete suite on Redis 8.8 and the live router/stdio contract on
every currently supported Redis Open Source series: 6.2, 7.2, 7.4, 8.0, 8.2,
8.4, 8.6, and 8.8. Live tests exercise both RESP2 and RESP3, the 29-tool curated
catalog, binary and nil responses, and the real `redis-mcp-server` stdio process.
The version list follows the
[Redis Open Source version-management table](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/).

## Non-goals

- Cloud or Enterprise REST APIs
- redisctl profiles or per-tool target URLs
- transactions, Pub/Sub, MONITOR, or other streaming/session-oriented commands
- recreating the complete redisctl database catalog before the REPL experience
  is evaluated

## License

MIT or Apache-2.0, at your option.
