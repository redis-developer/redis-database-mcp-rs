# redis-mcp

`redis-mcp` is the transport-independent library for the Redis database MCP
surface. It provides composable Tower-MCP routers, typed Redis command and
result contracts, access policy, capability filtering, bounded output, and
standalone/Cluster adapters built on redis-tower.

Applications own transport, target selection, authenticated identity, and
product policy. Use the companion `redis-mcp-server` package when a ready-made
stdio or Streamable HTTP host is preferable.

## Use from GitHub (current channel)

The repository is public, but `redis-mcp` has not been published to crates.io.
Pin a reviewed commit SHA rather than a moving branch or tag:

```toml
[dependencies]
redis-mcp = { git = "https://github.com/redis-developer/redis-database-mcp-rs", rev = "<reviewed-commit-sha>" }
```

The default feature set builds the complete library. A smaller host can select
only the command families it embeds:

```toml
[dependencies]
redis-mcp = { git = "https://github.com/redis-developer/redis-database-mcp-rs", rev = "<reviewed-commit-sha>", default-features = false, features = ["keyspace", "strings", "hashes"] }
```

The version-only `redis-mcp = "0.1"` form is for a future crates.io release,
not the current GitHub-only channel. See the repository's
[release process](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/releasing.md).

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
