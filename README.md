# redis-mcp

Composable, Tower-native MCP tools for Redis databases.

This repository is the standalone home for Redis database MCP tools. The
library returns an `McpRouter`; applications own transport, configuration,
identity, target selection, and product policy outside the Redis database
boundary.

The repository is named redis-database-mcp-rs to distinguish this surface from
Redis Cloud and Redis Enterprise APIs. The Rust package is simply redis-mcp.

## Curated default

The standalone default exposes 109 broadly useful tools:

- read-only essentials: `redis_ping`, `redis_dbsize`, `redis_scan`,
  `redis_get`, `redis_type`, `redis_ttl`, `redis_exists`, `redis_mget`,
  `redis_strlen`, `redis_memory_usage`, `redis_randomkey`, `redis_getrange`,
  `redis_dump`, `redis_object_inspect`
- read-write essentials: `redis_set`, `redis_expire`, `redis_persist`,
  `redis_mset`, `redis_incr`, `redis_append`, `redis_getex`, `redis_setrange`,
  `redis_decr`, `redis_decrby`, `redis_incrby`, `redis_incrbyfloat`,
  `redis_copy`, `redis_touch`, `redis_restore`
- full-access essentials: `redis_del`, `redis_unlink`, `redis_getdel`,
  `redis_copy_replace`, `redis_rename`, `redis_renamenx`,
  `redis_restore_replace`
- data structures: `redis_hget`, `redis_hgetall`, `redis_hexists`,
  `redis_hkeys`, `redis_hlen`, `redis_hmget`, `redis_hstrlen`, `redis_httl`,
  `redis_hvals`, `redis_hscan`, `redis_hset`, `redis_hincrby`,
  `redis_hincrbyfloat`, `redis_hexpire`, `redis_hpersist`, `redis_hdel`,
  `redis_lindex`, `redis_llen`, `redis_lpos`, `redis_lrange`, `redis_lpush`,
  `redis_rpush`, `redis_lpop`, `redis_rpop`, `redis_lmove`, `redis_lrem`,
  `redis_lset`, `redis_ltrim`, `redis_scard`, `redis_sdiff`, `redis_sinter`,
  `redis_sismember`, `redis_smembers`, `redis_smismember`, `redis_sscan`,
  `redis_sunion`, `redis_sadd`, `redis_srem`, `redis_zcard`, `redis_zcount`,
  `redis_zmscore`, `redis_zrange`, `redis_zrank`, `redis_zrevrank`,
  `redis_zscan`, `redis_zscore`, `redis_zadd`, `redis_zincrby`,
  `redis_zpopmin`, `redis_zpopmax`, `redis_zrem`,
  `redis_zremrangebyscore`, `redis_xlen`, `redis_xrange`,
  `redis_xrevrange`, `redis_xread`, `redis_xinfo_stream`,
  `redis_xinfo_groups`, `redis_xinfo_consumers`, `redis_xpending`,
  `redis_xadd`, `redis_xgroup_create`, `redis_xgroup_setid`,
  `redis_xgroup_createconsumer`, `redis_xreadgroup`, `redis_xack`,
  `redis_xclaim`, `redis_xautoclaim`, `redis_xdel`, `redis_xtrim`,
  `redis_xgroup_destroy`, `redis_xgroup_delconsumer`
- diagnostics: `redis_info`
- optional RedisJSON family: `redis_json_get`, `redis_json_type`,
  `redis_json_mget`, `redis_json_strlen`, `redis_json_objkeys`,
  `redis_json_objlen`, `redis_json_arrlen`, `redis_json_set`,
  `redis_json_numincrby`, `redis_json_toggle`, `redis_json_arrappend`,
  `redis_json_arrinsert`, `redis_json_del`, `redis_json_clear`,
  `redis_json_arrpop`, `redis_json_arrtrim`, `redis_json_merge`
- optional Search lifecycle: `redis_ft_list`, `redis_ft_info`,
  `redis_ft_search`, `redis_ft_create`, `redis_ft_dropindex`,
  `redis_vector_get_hash`, `redis_vector_set_hash`,
  `redis_ft_vector_search`, `redis_ft_hybrid_search`
- explicit full-access escape hatch: `redis_command`

Every successful tool result includes MCP structuredContent and an output
schema. Results are limited by default to 256 KiB for the complete encoded MCP
result and 1,000 collection entries. Oversized results return a stable
`output_limit_exceeded` reason with machine-readable
`io.redis.mcp/outputLimit` metadata; hashes, sets, sorted sets, streams, ranges, and
Search also expose typed continuation metadata. A fixed Redis target is
configured once by the server; arbitrary URLs are not accepted in tool calls.
Side-effectful value-returning commands (`SET GET`, `GETEX`, `GETDEL`, counted
list and sorted-set pops, `LMOVE`, `XREADGROUP`, `XCLAIM`, `XAUTOCLAIM`, and
`JSON.ARRPOP`) accept explicit byte caps and report
oversized returned values as omitted while preserving the mutation outcome.

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

For Redis Cluster, provide one or more seed URLs instead of `--url`. Multiple
seeds improve initial discovery when a node is unavailable:

    redis-mcp-server \
      --cluster-url redis://127.0.0.1:7000 \
      --cluster-url redis://127.0.0.1:7001 \
      --cluster-url redis://127.0.0.1:7002 \
      --access read-write \
      --stdio

`REDIS_CLUSTER_URLS` accepts the same seeds as a comma-separated list and
conflicts with the standalone `REDIS_URL`. The target remains fixed for the
life of the server and is never exposed in tool inputs. Normal Redis Cluster
slot rules still apply: supported multi-key commands such as `MGET`, `MSET`,
and `DEL` are split across slots by the adapter, while commands that require
all keys in one slot (for example `RENAME`, `LMOVE`, and set algebra) return a
stable `CROSSSLOT` invalid-request error.

Every curated sorted-set operation is currently single-key and follows normal
Cluster routing. Multi-key union/intersection tools are deliberately deferred
until their same-slot or explicit fan-out contract can be defined without
implying transparent cluster-wide aggregation.

Classified raw commands require full access plus their own opt-in. Unknown
commands fail closed:

    redis-mcp-server --access full --raw --stdio

An intentionally stronger flag permits unclassified request/response commands
while retaining hard blocks for session, streaming, transaction, replication,
script, and indefinite-blocking forms:

    redis-mcp-server --access full --raw-unrestricted --stdio

RedisJSON and Search are explicit additions to the curated defaults. The JSON
bundle exposes 17 structured tools spanning reads, typed mutations, arrays,
objects, deletion, clearing, and RFC 7396 merge. Enhanced JSONPath (`$`) is the
default; callers can explicitly select legacy paths where RedisJSON has
different reply semantics. The configured Redis target must provide the
corresponding capability:

    redis-mcp-server --access full \
      --enable-bundle json \
      --enable-bundle search \
      --stdio

## Embed the router

    use std::time::Duration;
    use redis_mcp::{
        AccessMode, DirectRedis, OutputBudget, RedisMcp, ToolBundle,
        UnavailableToolPolicy,
    };

    # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
    let capabilities = redis.discover_capabilities().await?;
    let router = RedisMcp::builder(redis)
        .access(AccessMode::ReadWrite)
        .bundles([ToolBundle::Essentials, ToolBundle::Diagnostics])
        .command_timeout(Duration::from_secs(10))
        .output_budget(OutputBudget::new(512 * 1024, 2_000))
        .capabilities(capabilities)
        .unavailable_tool_policy(UnavailableToolPolicy::Hide)
        .build();

    // Serve or merge router in the host application.
    # let _ = router;
    # Ok(())
    # }

For a fixed Redis Cluster target, use the cluster-aware convenience adapter;
the router and tool contracts are otherwise identical:

    use redis_mcp::{AccessMode, DirectRedisCluster, RedisMcp};

    # async fn cluster_example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedisCluster::connect([
        "redis://127.0.0.1:7000",
        "redis://127.0.0.1:7001",
        "redis://127.0.0.1:7002",
    ]).await?;
    let router = RedisMcp::builder(redis)
        .access(AccessMode::ReadWrite)
        .build();

    # let _ = router;
    # Ok(())
    # }

Hosts with their own connection lifecycle implement `RedisExecutor` using
crate-owned `RedisCommand`, `RedisValue`, and `RedisError` types. They do not
need to share this crate's redis-rs dependency line. Commands include the
originating tool, required access level, and any required Redis module for host
telemetry, capability routing, and audit records. See
[the custom executor example](examples/custom_executor.rs).

## Embed Redis-style argv

CLI and REPL frontends can use the same governed execution boundary without
constructing MCP JSON arguments:

    use redis_mcp::{
        AccessMode, DirectRedis, NativeRedisInvocation, RawCommandPolicy,
        RedisInvocationEngine,
    };

    # async fn native_example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
    let engine = RedisInvocationEngine::builder(redis)
        .access(AccessMode::ReadWrite)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();
    let value = engine
        .invoke(NativeRedisInvocation::from_argv([
            b"SET".to_vec(),
            b"greeting".to_vec(),
            b"hello".to_vec(),
        ])?)
        .await?;
    # let _ = value;
    # Ok(())
    # }

The invocation stays binary-safe and shares command classification, access,
capability checks, timeout, redaction, error categories, and result budgets
with `redis_command`. Tokenization, quoting, history, completion, rendering,
and session-oriented commands remain frontend concerns. See the complete
[pre-tokenized argv example](examples/native_argv.rs).

`RedisCapabilities` is crate-owned too. A host can supply an authoritative or
partial snapshot containing Redis and module versions, deployment mode, and
per-command availability without sharing the library's redis-rs dependency.
`with_command_inventory` and `with_module_inventory` make omitted catalog
requirements explicitly unavailable; the individual `with_command` and
`with_module` methods support partial knowledge.
`DirectRedis` and `DirectRedisCluster` provide bounded asynchronous discovery
for hosts that use the bundled adapters. Unknown facts remain permissive for
custom-executor compatibility. Known-unavailable tools are advertised with a
stable capability error by default; `UnavailableToolPolicy::Hide` removes them
from `tools/list` instead. Every catalog entry exposes its minimum Redis/module
versions and required command names.

The current catalog marks `redis_info`, `redis_dbsize`, `redis_scan`, and
`redis_randomkey` as standalone-only because redis-rs otherwise routes them to
one cluster node or returns a fan-out shape without the database-wide
aggregation their contracts imply. A discovered cluster snapshot therefore
hides or rejects those tools instead of silently reporting one node as the
whole database.

The curated default enables the `essentials`, `data_structures`, and
`diagnostics` bundles. The module-backed `json` and `search` bundles are
available only through deliberate composition; `admin`, `bulk`, and `raw` are
reserved for further catalog growth. Raw execution is always controlled by its
separate policy rather than bundle selection alone.

See [the architecture decisions](docs/architecture.md) for the intentional
Tower-MCP boundary and fixed-target model, and the
[redisctl compatibility inventory](docs/redisctl-compatibility.md) for the
132-tool read-only baseline and known contract differences. The
[command-surface scorecard](docs/surface-comparison.md) pins both redisctl and
`redis/mcp-redis`, maps every baseline tool, and defines the objective gate for
the library surface roadmap.

## Compatibility and testing

On Unix, `cargo test --all-features` starts isolated Redis processes through
[`redis-server-wrapper`](https://github.com/joshrotenberg/redis-server-wrapper)
when `REDIS_URL` is not set. `redis-server` and `redis-cli` must be on `PATH`;
when either binary is unavailable, the live cases print an explicit skip reason.
Set `REDIS_URL` to test an already-running target instead—the external target
always takes precedence and is never stopped by the suite.

The Redis Stack lifecycle test similarly uses `REDIS_STACK_URL` when supplied.
Otherwise, the wrapper auto-detects a local Redis Stack installation and loads
its Search and RedisJSON modules into an isolated server. A plain Redis target
is also exercised to keep missing-module errors stable and actionable. Stack
coverage includes the complete RedisJSON family, JSONPath and legacy-path
semantics, nil and wrong-type results, output bounds, ACL key patterns, FLOAT32
HASH and FLOAT64 JSON vectors, FLAT and HNSW indexes, binary-safe hash round
trips, KNN pagination, and typed text/tag/numeric/geo hybrid filters.

CI runs the complete suite on Redis 8.8 and the live router/stdio contract on
every currently supported Redis Open Source series: 6.2, 7.2, 7.4, 8.0, 8.2,
8.4, 8.6, and 8.8. Live tests exercise both RESP2 and RESP3, the 109-tool curated
catalog, binary and nil responses, conditional and absolute expiration,
bounded serialization/restore, complete bounded list semantics, typed
hash-field expiration, binary-safe membership, budgeted set algebra, complete
bounded sorted-set semantics, complete Streams and consumer-group workflows,
finite blocking reads, ACL failures, bounded connection loss and
recovery, and the real `redis-mcp-server` stdio process. A separate job pins
the official
`redis/redis-stack-server:7.4.0-v8` image and runs the JSON/Search lifecycle.
Dedicated three-master cluster jobs run on Redis 6.2 and 8.8 and exercise
redirection, multi-slot aggregation, same-slot copy/rename/list movement and
set algebra, single- and same-slot multi-stream reads, stable cross-slot failures, and the cluster-configured stdio
server. The version list follows the
[Redis Open Source version-management table](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/).

## Non-goals

- Cloud or Enterprise REST APIs
- redisctl profiles or per-tool target URLs
- transactions, Pub/Sub, MONITOR, or other streaming/session-oriented commands
- terminal tokenization, history, completion, result rendering, or an
  application-specific CLI/REPL frontend

## License

MIT or Apache-2.0, at your option.
