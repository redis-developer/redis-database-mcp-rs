# redis-mcp

`redis-mcp` is the transport-independent library for the Redis database MCP
surface. It provides composable Tower-MCP routers, typed Redis command and
result contracts, access policy, capability filtering, bounded output, and
standalone/Cluster adapters built on redis-tower.

Applications own transport, target selection, authenticated identity, and
product policy. Use the companion `redis-mcp-server` package when a ready-made
stdio or Streamable HTTP host is preferable.

## Install

```toml
[dependencies]
redis-mcp = "0.1"
```

The default feature set builds the complete library. A smaller host can select
only the command families it embeds:

```toml
[dependencies]
redis-mcp = { version = "0.1", default-features = false, features = ["keyspace", "strings", "hashes"] }
```

## Compose a router

```rust,ignore
use redis_mcp::{AccessMode, DirectRedis, RedisMcp, ToolFamily};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
let router = RedisMcp::builder(redis)
    .access(AccessMode::ReadWrite)
    .families([ToolFamily::Keyspace, ToolFamily::Strings, ToolFamily::Hashes])
    .build();

// Merge `router` into the host's McpRouter, then select a transport there.
# let _ = router;
# Ok(())
# }
```

The `custom_executor` example shows the seam for redisctl or another host that
already owns its Redis connection lifecycle. The `native_argv` example shows
the governed Redis-command interface for CLI/REPL frontends.

For a host whose router must be built before Redis is reachable, validate the
fixed target synchronously and connect lazily inside its `RedisExecutor`:

```rust,ignore
use redis_mcp::{RedisDeployment, validate_redis_target_url};

let url = "redis://127.0.0.1:6379/0";
validate_redis_target_url(url, RedisDeployment::Cluster)?;
// Keep the chosen URL in host-owned configuration; the executor connects on
// its first command, not while the MCP router is assembled.
# Ok::<(), redis_mcp::RedisError>(())
```

Validation uses the direct adapters' parser without network access. A Cluster
target must use database 0 and cannot be a Unix socket. TCP/TLS URLs put
credentials in the URL authority and a database number in the path; query
credentials/database values are rejected rather than silently ignored. Error
messages and debug output never include the input URL or its secrets.

## Cargo features

All features are additive. Compile-time inclusion and runtime exposure remain
separate: a family must be compiled before it can be selected, while access
mode, capability discovery, and bundle policy still determine the advertised
surface.

- Families: `keyspace`, `strings`, `hashes`, `lists`, `sets`, `sorted-sets`,
  `streams`, `bitmaps`, `arrays`, `hyperloglog`, `geospatial`, `vector-sets`,
  `pubsub`, `scripting`, `json`, `search`, and `timeseries`.
- Cross-cutting surfaces: `diagnostics`, `sessions`, `transactions`, `admin`,
  `bulk`, `coordination`, and `guidance`.
- `all-families` enables every family.
- `full` enables every family and cross-cutting surface; it is the default.

Stateful Pub/Sub and MONITOR sessions require a host-provided stable owner and
lifecycle. Transactions and blocking operations use dedicated connections.
Redis ACLs remain the authorization boundary; MCP access modes and annotations
are defense in depth.

The opt-in `coordination` bundle provides a Redis Streams-backed durable
handoff protocol with publish, claim, MRTR approval, complete, status, and
recovery tools. Its keys are sharded and hash-tagged for Redis Cluster, and its
principal is a host extension separate from transient MCP transport sessions.

## Documentation and support

The repository documentation covers the
[initial release boundary](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/initial-release.md),
[architecture](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/architecture.md),
and [official Redis command coverage](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/redis-command-coverage.md).

The supported Redis, module, MCP, and transport matrix is maintained in the
initial release document. The checked-in catalog and coverage snapshots are
release gates rather than informal documentation.

## License

MIT or Apache-2.0, at your option.
