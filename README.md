# redis-mcp

Composable, Tower-native MCP tools for Redis databases.

This repository is the standalone home for Redis database MCP tools. The
library returns an `McpRouter`; applications own transport, configuration,
identity, target selection, and product policy outside the Redis database
boundary.

The repository is named redis-database-mcp-rs to distinguish this surface from
Redis Cloud and Redis Enterprise APIs. It is a Cargo workspace with two focused
packages:

- `crates/redis-mcp`: the reusable, transport-independent Redis MCP library;
- `crates/redis-mcp-server`: the thin standalone server and transport host.

User-facing CLI and REPL behavior remains outside the initial release boundary.
For now, `mcp-repl` dynamically derives both interactive and one-shot commands
from the server's MCP surface, which lets the library contract drive agent and
human workflows without duplicating command definitions.

## Library-first family composition

The default `redis-mcp` Cargo feature set compiles the full library surface so
existing applications remain compatible. Smaller consumers can disable default
features and enable only the Redis families they embed:

    redis-mcp = { version = "0.1", default-features = false, features = ["strings", "hashes"] }

The additive family features are `keyspace`, `strings`, `hashes`, `lists`,
`sets`, `sorted-sets`, `streams`, `bitmaps`, `arrays`, `hyperloglog`,
`geospatial`, `vector-sets`, `pubsub`, `scripting`, `json`, `search`, and
`timeseries`. The `diagnostics`, `sessions`, `transactions`, and `admin`
features compile their corresponding cross-cutting bundles. `all-families` enables every command
family, while `full` also enables diagnostics, sessions, transactions, and
guarded administration.

Compile-time inclusion and runtime exposure are separate. `families(...)`
replaces the compatibility bundle defaults with a precise family selection;
access mode and capability filtering still apply afterward. All selected
families are mounted in one builder pass and therefore share one executor,
capability snapshot, output budget, and session lifecycle:

```rust,ignore
use redis_mcp::{AccessMode, RedisMcp, families};
use tower_mcp::McpRouter;

let redis = RedisMcp::builder(executor)
    .access(AccessMode::ReadWrite)
    .families([
        families::strings::FAMILY,
        families::hashes::FAMILY,
    ])
    .build();

let app = McpRouter::new()
    .server_info("redisctl", "1.0")
    .merge(redis);
```

The existing `bundles(...)` API remains the convenient compatibility and
standalone-server assembly path. See
[`custom_executor.rs`](crates/redis-mcp/examples/custom_executor.rs) for the
intended redisctl-style adapter and router merge boundary.

## Curated default

The curated default catalog contains 201 broadly useful tools across all
access levels. A running server exposes the subset permitted by its configured
access mode (113 tools at read-only on the current full build):

- read-only essentials: `redis_ping`, `redis_dbsize`, `redis_scan`,
  `redis_get`, `redis_type`, `redis_ttl`, `redis_exists`, `redis_mget`,
  `redis_strlen`, `redis_memory_usage`, `redis_randomkey`, `redis_getrange`,
  `redis_dump`, `redis_object_inspect`, `redis_sort`, `redis_pubsub_channels`,
  `redis_pubsub_numsub`, `redis_pubsub_numpat`,
  `redis_pubsub_shardchannels`, `redis_pubsub_shardnumsub`
- read-write essentials: `redis_set`, `redis_expire`, `redis_persist`,
  `redis_mset`, `redis_incr`, `redis_append`, `redis_getex`, `redis_setrange`,
  `redis_decr`, `redis_decrby`, `redis_incrby`, `redis_incrbyfloat`,
  `redis_copy`, `redis_touch`, `redis_restore`, `redis_publish`,
  `redis_spublish`
- full-access essentials: `redis_del`, `redis_unlink`, `redis_getdel`,
  `redis_copy_replace`, `redis_rename`, `redis_renamenx`,
  `redis_restore_replace`, `redis_sort_store`
- data structures: `redis_hget`, `redis_hgetall`, `redis_hexists`,
  `redis_hkeys`, `redis_hlen`, `redis_hmget`, `redis_hstrlen`, `redis_hrandfield`, `redis_httl`,
  `redis_hvals`, `redis_hscan`, `redis_hset`, `redis_hincrby`,
  `redis_hincrbyfloat`, `redis_hexpire`, `redis_hpersist`,
  `redis_hexpire_delete`, `redis_hdel`,
  `redis_lindex`, `redis_llen`, `redis_lpos`, `redis_lrange`, `redis_lpush`,
  `redis_rpush`, `redis_lpop`, `redis_rpop`, `redis_lmove`, `redis_lrem`,
  `redis_lset`, `redis_ltrim`, `redis_scard`, `redis_sdiff`,
  `redis_sdiffcard`, `redis_sdiffstore`, `redis_sinter`,
  `redis_sinterstore`, `redis_sismember`, `redis_smembers`,
  `redis_smismember`, `redis_sscan`, `redis_sunion`, `redis_sunioncard`,
  `redis_sunionstore`, `redis_sadd`, `redis_srem`, `redis_zcard`,
  `redis_zcount`, `redis_zdiffstore`, `redis_zintercard`,
  `redis_zinterstore`, `redis_zmscore`, `redis_zrange`,
  `redis_zrangestore`, `redis_zrank`, `redis_zrevrank`, `redis_zscan`,
  `redis_zscore`, `redis_zunionstore`, `redis_zadd`, `redis_zincrby`,
  `redis_zpopmin`, `redis_zpopmax`, `redis_zrem`, `redis_zremrangebyscore`,
  `redis_getbit`, `redis_setbit`,
  `redis_bitcount`, `redis_bitpos`, `redis_bitfield_ro`, `redis_bitfield`,
  `redis_bitop`, `redis_geoadd`, `redis_geodist`, `redis_geohash`,
  `redis_geopos`, `redis_geosearch`, `redis_geosearchstore`, `redis_pfadd`,
  `redis_pfcount`, `redis_pfmerge`, `redis_xlen`, `redis_xrange`,
  `redis_xrevrange`, `redis_xread`, `redis_xinfo_stream`,
  `redis_xinfo_groups`, `redis_xinfo_consumers`, `redis_xpending`,
  `redis_xadd`, `redis_xgroup_create`, `redis_xgroup_setid`,
  `redis_xgroup_createconsumer`, `redis_xreadgroup`, `redis_xack`,
  `redis_xclaim`, `redis_xautoclaim`, `redis_xdel`, `redis_xtrim`,
  `redis_xgroup_destroy`, `redis_xgroup_delconsumer`, `redis_arcount`,
  `redis_ardel`, `redis_ardelrange`, `redis_arget`, `redis_argetrange`,
  `redis_argrep`, `redis_arinfo`, `redis_arinsert`, `redis_arlastitems`,
  `redis_arlen`, `redis_armget`, `redis_armset`, `redis_arnext`, `redis_arop`,
  `redis_arring`, `redis_arscan`, `redis_arseek`, `redis_arset`, `redis_vadd`,
  `redis_vcard`, `redis_vdim`, `redis_vemb`, `redis_vgetattr`, `redis_vinfo`,
  `redis_vismember`, `redis_vlinks`, `redis_vrandmember`, `redis_vrange`,
  `redis_vrem`, `redis_vsetattr`, `redis_vsim`, `redis_delex`, `redis_digest`,
  `redis_hgetdel`, `redis_hgetex`, `redis_hsetex`, `redis_increx`,
  `redis_lmovem`, `redis_msetex`, `redis_xackdel`, `redis_xdelex`,
  `redis_xnack`
- diagnostics: `redis_info`, `redis_client_list`, `redis_cluster_info`,
  `redis_memory_stats`, `redis_module_list`, `redis_slowlog`,
  `redis_latency_history`, `redis_acl_whoami`, `redis_health_check`,
  `redis_connection_summary`, `redis_keyspace_summary`,
  `redis_memory_summary`, `redis_key_summary`, `redis_hotkeys`
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
- optional RedisTimeSeries family: `redis_ts_create`, `redis_ts_alter`,
  `redis_ts_add`, `redis_ts_madd`, `redis_ts_incrby`, `redis_ts_decrby`,
  `redis_ts_del`, `redis_ts_createrule`, `redis_ts_deleterule`,
  `redis_ts_range`, `redis_ts_revrange`, `redis_ts_mrange`,
  `redis_ts_mrevrange`, `redis_ts_get`, `redis_ts_mget`, `redis_ts_info`,
  `redis_ts_queryindex`
- optional owner-isolated Pub/Sub sessions: `redis_subscribe`,
  `redis_psubscribe`, `redis_ssubscribe`, `redis_pubsub_read`,
  `redis_pubsub_unsubscribe`, `redis_pubsub_close`
- optional finite blocking calls and replication waits: `redis_blpop`,
  `redis_brpop`, `redis_blmove`, `redis_blmovem`, `redis_blmpop`,
  `redis_bzpopmin`, `redis_bzpopmax`, `redis_bzmpop`, `redis_wait`,
  `redis_waitaof`
- optional owner-isolated MONITOR sessions: `redis_monitor_start`,
  `redis_monitor_read`, `redis_monitor_close`
- optional scripting family: `redis_eval`, `redis_eval_ro`, `redis_evalsha`,
  `redis_evalsha_ro`, `redis_fcall`, `redis_fcall_ro`, `redis_script_exists`,
  `redis_script_load`, `redis_script_flush`, `redis_script_kill`,
  `redis_function_list`, `redis_function_stats`, `redis_function_dump`,
  `redis_function_load`, `redis_function_restore`, `redis_function_delete`,
  `redis_function_flush`, `redis_function_kill`
- optional guarded administration: redacted ACL, backup, Cluster,
  configuration, server-state, latency, memory, slow-log, and hot-key
  inspection plus separately Full-gated client, configuration, flush, reset,
  purge, hot-key, database, and Redis 8.10 backup lifecycle controls
- optional bounded atomic transactions: `redis_transaction`
- optional governed argv invocation tiers: `redis_command_readonly`,
  `redis_command_write`, `redis_command_metadata`, `redis_command_inventory`
- optional bounded bulk workflows: `redis_bulk_load`, `redis_bulk_seed`
- explicit full-access escape hatch: `redis_command`

The reusable router keeps the stateful `sessions` bundle opt-in because its
lifecycle belongs to the embedding host. The included `redis-mcp-server`
provides the built-in DirectRedis Pub/Sub, blocking, and MONITOR backends
automatically, so its ordinary stdio surface contains the 201 curated defaults
plus these session and lifecycle tools.

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
Pub/Sub publication and inspection are binary-safe too. Channel enumeration
requires a result limit; cluster inspection requires a node limit, deduplicates
or sums node-local replies deterministically, and reports partial node failures.
`SPUBLISH` and shard inspection are capability-gated to Redis 7.0 or newer.
The non-default `scripting` family preserves binary keys, arguments, sources,
and dump payloads; requires declared same-slot keys on Cluster; and separates
read-only execution from arbitrary-code and lifecycle operations. Script and
function results share the global output budgets. A request timeout bounds how
long the MCP call waits, but cannot promise that Redis stopped server-side
execution; explicit kill tools retain Redis's own write-safety limitations.

The non-default `timeseries` family covers the complete pinned
RedisTimeSeries 1.12.6 command surface with typed contracts. Timestamps stay
exact integer milliseconds (or the explicit `*`, `-`, and `+` tokens) and
values stay IEEE 754 doubles, so nothing is coerced lossily. Labels, filters,
bulk samples, and page counts are bounded; single-series ranges return typed
continuation metadata; multi-series queries share the encoded output budgets.
`TS.MADD` keeps per-sample failures aligned in-band while the remaining
samples apply. Alteration, sample deletion, and rule removal are classified as
destructive because retention shrinks trim samples and the label set is
replaced rather than merged. Options newer than the connected module version
(LATEST, EMPTY, ALIGN, GROUPBY, IGNORE, and the newer aggregators) fail closed
before execution when capabilities are known. On Redis Cluster, keyed tools
route by key and `TS.MADD`/`TS.CREATERULE` require same-slot keys, while
`TS.MGET`, `TS.MRANGE`, `TS.MREVRANGE`, and `TS.QUERYINDEX` are
standalone-only because OSS Cluster offers no database-wide TimeSeries
coordinator.

The non-default `bulk` bundle loads bounded structured records — strings,
hashes, lists, sets, sorted sets, JSON documents, and vector sets, with
optional expirations — and deterministic seed datasets generated from an
explicit seed and schema. Every record is validated before anything executes;
records then load through sequential batches of bounded concurrent commands
that never bypass the executor, access policy, timeouts, or Cluster routing.
Batch size, record count, input bytes, concurrency, wall-clock duration, and
result summaries are all bounded; stop-on-error and continue-on-error are
explicit; reports return compact per-batch counts plus bounded representative
failures that keep record identity, instead of echoing every record. Dry runs
write nothing. Multi-command records (collections with expirations, multiple
vector elements) are not atomic and report partial application explicitly.
The same seed, schema, and library version always regenerate the same
dataset. File and URL ingestion deliberately stay host concerns outside the
library boundary.

The non-default `transactions` bundle provides one-shot atomic MULTI/EXEC
execution without exposing connection-stateful transaction commands as
unrelated MCP calls. One bounded command list, with optional WATCH keys, runs
on one freshly dialed dedicated connection that is dropped afterwards, so
transaction state can never leak across MCP calls or pooled connections. Every
nested command passes the same classification policy as `redis_command`
(transactions therefore require the raw opt-in and full access), the whole
transaction requires the maximum access of any nested command, and command
count, watch keys, request bytes, total duration, and result budgets are all
bounded. Outcomes are explicit: `committed` with per-command result alignment
(runtime failures stay in-band per entry), `aborted` when a watched key
changed, or `rejected` with per-command queue-time failures when the server
refused the transaction and nothing executed. Connection loss or timeout after
EXEC may leave the outcome unknown; the library reports that explicitly and
never replays a possibly committed transaction. On Redis Cluster all keys must
hash to one slot: watched keys are validated client-side and pin the pipeline
to their slot's node, while cross-slot command lists return the stable
`CROSSSLOT` error.

The `sessions` bundle also covers Redis operations that block or stream.
Finite blocking pops, moves, and replication waits each run on one freshly
dialed dedicated connection with a required, server-capped timeout, so a wait
can never stall the shared executor and indefinite blocking is impossible; a
server-side timeout is an explicit, distinguishable result, and a delivered
element is never replayed after a connection failure. MONITOR becomes an
owner-isolated session over a dedicated connection with a bounded drop-oldest
event buffer: client addresses are pseudonymized, argument values are omitted
unless the session explicitly opts in, unparseable lines are dropped rather
than partially exposed, reads are finite and cancellable, and idle sessions
are reaped. The Redis 8.10 backup lifecycle joins the admin bundle as guarded
`start`, `seal`, `abort`, and `cleanup` transitions that pair with the
existing status and file-count inspections; abort and cleanup require
explicit confirmation, and results never contain filesystem paths.

The MCP surface is more than tools. The default `guidance` bundle ships
curated Redis expertise as MCP resources and prompts: seven versioned guides
(`redis-mcp://guidance/…`) covering data modeling, memory tuning, latency
diagnosis, Search index design, Cluster key design, expiration strategies,
and batching-versus-transactions; live descriptions of the running server
(`redis-mcp://catalog` for every tool with its access tier and capability
requirements, `redis-mcp://capabilities` for the configured Redis version,
deployment, and modules); and five guided workflow prompts
(`redis_diagnose_latency`, `redis_review_memory`, `redis_plan_data_model`,
`redis_design_search_index`, `redis_plan_bulk_load`) whose instructions
reference exact tool names. Content is compiled in, bounded, snapshot-tested,
and every referenced tool name is validated against the catalog, so guidance
can never drift from the surface it describes.

Hosts can additionally serve the official Redis command documentation as
passthrough resources: configuring a `RedisDocsFetcher` on the builder
registers the `redis-mcp://docs/commands/{command}` template, which conveys
`content/commands/*.md` from a pinned redis/docs revision at read time with
front matter rendered into a typed header, per-page byte ceilings, a bounded
cache, fetch timeouts, and CC BY-NC-SA attribution appended. Unknown
commands fail closed against the classified command inventory without
fetching, and the same inventory drives offline `completion/complete`
suggestions for the template's `{command}` variable. Because documentation
reads introduce network egress the base surface never performs, nothing is
fetched — and no template is advertised — unless a host explicitly supplies
the fetcher.

See [the spike decision record](docs/spike.md) for the tested architecture,
REPL findings, and redisctl migration sequence.

## Run the standalone server

    cargo run -p redis-mcp-server -- \
      --url redis://127.0.0.1:6379 \
      --access read-write \
      --stdio

Use it with any stdio MCP client. With
[mcp-repl](https://github.com/joshrotenberg/mcp-repl):

    mcp-repl -- redis-mcp-server \
      --url redis://127.0.0.1:6379 \
      --access full \
      --enable-bundle admin \
      --enable-bundle scripting \
      --stdio

The same generated command surface is available non-interactively through
`mcp-repl --exec`, making it the interim one-shot CLI as well as the REPL. A
future Redis-specific frontend can build on a reusable `mcp-repl` core after
the generated experience has exposed which specialized layers are worthwhile.
See [the verified mcp-repl recipes](docs/mcp-repl-recipes.md) for interactive,
NDJSON, schema-contract, pagination, and session workflows.

Inside the REPL:

    redis_ping
    redis_set key=greeting value=hello
    redis_get key=greeting
    redis_scan pattern=gre* count=20
    call redis_subscribe {"subscriptions":[{"value":"events"}]}
    call redis_publish {"channel":{"value":"events"},"message":{"value":"hello"}}
    redis_pubsub_read session_id=ps_<opaque-handle> wait_ms=1000

### Configuration sources and precedence

Every server setting is reachable from three sources with one precedence:
CLI arguments override environment variables override an explicit TOML file
override built-in defaults. The file is only read when selected with
`--config <path>` or `REDIS_MCP_CONFIG`, and unknown keys in it fail startup
rather than silently applying defaults.

[`redis-mcp.example.toml`](crates/redis-mcp-server/redis-mcp.example.toml)
documents every key at its default value, and a test keeps it complete and
correct. Environment variables follow the flag names (`REDIS_MCP_ACCESS`,
`REDIS_MCP_RAW`, `REDIS_MCP_MAX_OUTPUT_BYTES`,
`REDIS_MCP_PUBSUB_MAX_SESSIONS`, …); the target keeps its historical
`REDIS_URL` / `REDIS_CLUSTER_URLS` names. Beyond the target and policy
flags, the configurable surface covers output budgets, the per-command
timeout, capability discovery (`--no-discovery` to skip), the
unavailable-tool policy, and every Pub/Sub, MONITOR, blocking, transaction,
bulk, and documentation limit.

Official command documentation is a separate opt-in because reads perform
outbound HTTPS. `--enable-docs` (or `[docs] enabled = true`) advertises
`redis-mcp://docs/commands/{command}` and fetches only validated command pages
from `raw.githubusercontent.com/redis/docs` at the configured revision. The
client does not follow redirects, streams into the configured byte ceiling,
uses a bounded in-memory cache, and is also bounded by the library fetch
timeout. The default full binary compiles this capability but leaves it off;
slim builds must include the `docs` feature explicitly.

    redis-mcp-server --url redis://127.0.0.1:6379 \
      --enable-docs --stdio

The server discovers the target's version, deployment, and modules at
startup so version- and module-gated tools preflight accurately; discovery
failures (for example an ACL that blocks introspection) log a warning and
fall back to advertising the selected catalog without version awareness.

### Slim server builds

The binary ships with everything by default and is compile-time tunable to
exactly the surface a deployment wants: the server crate mirrors every
library Cargo feature as a passthrough, so `--no-default-features` plus an
explicit feature list builds a smaller binary whose catalog contains only
the selected families and bundles. Requesting an uncompiled bundle at
runtime (`--enable-bundle json` on a binary built without `json`) fails at
startup with an actionable error before any connection is dialed — a slim
binary never silently serves less than it was asked for.

Two measured recipes (release profile, macOS arm64; the default full binary
is 24.2 MiB):

    # Read-only cache inspector: keys, strings, hashes, diagnostics. 14.4 MiB.
    cargo build --release -p redis-mcp-server --no-default-features \
      --features keyspace,strings,hashes,diagnostics

    # JSON + Search document server. 14.3 MiB.
    cargo build --release -p redis-mcp-server --no-default-features \
      --features keyspace,strings,json,search

    # Add the opt-in documentation host to any slim surface.
    cargo build --release -p redis-mcp-server --no-default-features \
      --features keyspace,strings,docs

CI compiles representative slim combinations of both crates and verifies the
uncompiled-bundle rejection, so slim builds cannot rot.

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
all keys in one slot (for example `RENAME`, `LMOVE`, `LMOVEM`, `MSETEX`, set
algebra, `BITOP`, `GEOSEARCHSTORE`, `PFCOUNT`, and `PFMERGE`) return a stable `CROSSSLOT`
invalid-request error.

Every curated sorted-set operation is currently single-key and follows normal
Cluster routing. Multi-key union/intersection tools are deliberately deferred
until their same-slot or explicit fan-out contract can be defined without
implying transparent cluster-wide aggregation.

The full-access `redis_command` escape hatch requires full access plus the
classified raw opt-in. Unknown commands fail closed:

    redis-mcp-server --access full --raw --stdio

An intentionally stronger flag permits unclassified request/response commands
while retaining hard blocks for session, streaming, transaction, replication,
script/function, and indefinite-blocking forms:

    redis-mcp-server --access full --raw-unrestricted --stdio

Redis-syntax clients can enable the governed argv invocation tiers instead.
The same classified policy then executes read-only commands at read-only
access and ordinary writes at read-write access, with honest per-tier MCP
annotations, while destructive and unknown forms still require the full-access
escape hatch. The metadata and inventory tools give CLIs access/effect
previews and completion data from the same policy table:

    redis-mcp-server --access read-only --raw \
      --enable-bundle invocation \
      --stdio

Bounded atomic `redis_transaction` execution builds on the same classification
policy and therefore requires one of the raw flags:

    redis-mcp-server --access full --raw --transactions --stdio

Administration, Scripting, RedisJSON, Search, and TimeSeries are explicit
additions to the curated defaults. The `admin` bundle remains off even under Full access; it
uses fixed non-secret configuration allowlists, confirmation fields, bounded
Cluster fan-out, pseudonymous nodes, and redacted partial failures. The JSON
bundle exposes 17 structured tools spanning reads, typed
mutations, arrays, objects, deletion, clearing, and RFC 7396 merge. Enhanced
JSONPath (`$`) is the default; callers can explicitly select legacy paths where
RedisJSON has different reply semantics. The configured Redis target must
provide the corresponding capability:

    redis-mcp-server --access full \
      --enable-bundle admin \
      --enable-bundle scripting \
      --enable-bundle json \
      --enable-bundle search \
      --enable-bundle timeseries \
      --stdio

## Embed the router

    use std::time::Duration;
    use redis_mcp::{
        AccessMode, DirectRedis, DirectRedisPubSubSessionManager, OutputBudget,
        PubSubSessionLimits, RedisMcp, ToolBundle, UnavailableToolPolicy,
    };

    # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let redis = DirectRedis::connect("redis://127.0.0.1:6379").await?;
    let capabilities = redis.discover_capabilities().await?;
    let sessions = DirectRedisPubSubSessionManager::standalone(
        "redis://127.0.0.1:6379",
        PubSubSessionLimits::default(),
    )?;
    let router = RedisMcp::builder(redis)
        .access(AccessMode::ReadWrite)
        .bundles([ToolBundle::Essentials, ToolBundle::Diagnostics])
        .command_timeout(Duration::from_secs(10))
        .output_budget(OutputBudget::new(512 * 1024, 2_000))
        .capabilities(capabilities)
        .unavailable_tool_policy(UnavailableToolPolicy::Hide)
        .pubsub_sessions(sessions)
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

The bundled adapters use redis-tower. Ordinary requests share reconnecting
multiplexed clients; blocking operations, transactions, Pub/Sub, and MONITOR
use dedicated connections. Connection setup is bounded at ten seconds, while
command, blocking, transaction, and session deadlines remain authoritative.
There is no hidden 500 ms response timeout. Delivered writes are not replayed
after an ambiguous transport failure.

Redis URL `?protocol=resp2` and `?protocol=resp3` options remain supported;
ordinary adapters retain the RESP2 default. Binary Pub/Sub names and payloads
remain bytes through subscription, delivery, and resubscription. Cluster shard
subscriptions use one dedicated connection per distinct hash slot in a bounded
session. The dependency migration preserves MCP tool schemas and access rules.

The migration currently uses an immutable redis-tower Git revision containing
required Cluster and binary Pub/Sub APIs. Registry packaging remains blocked
until those APIs are released. RESP3 attribute-prefixed wire replies currently
fail with an explicit protocol error; this prevents reply misalignment while
full attribute attachment is being decided. Host-supplied `RedisValue::Attribute`
values continue to work.

For hosts wrapping an existing client, use
`DirectRedis::from_multiplexed_client` or
`DirectRedisCluster::from_multiplexed_client`. These replace the old redis-rs
connection constructors. Blocking and transaction adapters accept target URLs
and open a fresh connection per operation. The redis-rs dependency is now used
only by tests as an independent seeding client and decoding oracle.

Hosts with their own connection lifecycle implement `RedisExecutor` using
crate-owned `RedisCommand`, `RedisValue`, and `RedisError` types. They do not
need to share this crate's redis-tower dependency line. Commands include the
originating tool, required access level, and any required Redis module for host
telemetry, capability routing, and audit records. See
[the custom executor example](crates/redis-mcp/examples/custom_executor.rs).

Subscription sessions deliberately use a separate `PubSubSessionManager`
boundary: every live session owns a dedicated Redis connection and outlives a
single command future. `DirectRedisPubSubSessionManager` supports fixed
standalone and Cluster targets. Custom hosts can implement the public trait,
inject a stable `PubSubSessionOwner` request extension for each authenticated
client or principal, and call `close_owner` when that host session ends. The
builder supplies and cleans up a random owner automatically for one-client
routers such as stdio.

Atomic transactions use the analogous `RedisTransactionExecutor` boundary,
because MULTI, WATCH, and EXEC require one dedicated connection per call
rather than a pooled request/response command. `DirectRedisTransactions`
supports fixed standalone and Cluster targets, dials a fresh connection per
transaction, and never replays a possibly committed EXEC. Enable the bundle
with `.transactions(...)` alongside an enabled raw command policy, and tune
bounds with `.transaction_limits(...)`. Library consumers outside MCP pair the
same policy with `RedisTransactionEngine`, which classifies every nested
command, enforces the transaction bounds, and returns the explicit
committed/aborted/rejected outcome.

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
[pre-tokenized argv example](crates/redis-mcp/examples/native_argv.rs).

Out-of-process MCP clients get the same boundary through the opt-in
`invocation` bundle. `redis_command_readonly` and `redis_command_write`
execute classified commands at their matching access tiers from binary-safe
argv, so `GET foo` traverses exactly the policies of `redis_get key=foo`
without enabling the full-access escape hatch. `redis_command_metadata`
previews one command's tier, capability requirements, and matching tool — or
its stable blocked/unclassified reason — without executing it, and
`redis_command_inventory` lists the classified command surface for completion.
Connection-stateful forms stay mapped to their dedicated session, transaction,
and workflow tools.

`RedisCapabilities` is crate-owned too. A host can supply an authoritative or
partial snapshot containing Redis and module versions, deployment mode, and
per-command availability without sharing the library's Redis client dependency.
`with_command_inventory` and `with_module_inventory` make omitted catalog
requirements explicitly unavailable; the individual `with_command` and
`with_module` methods support partial knowledge.
`DirectRedis` and `DirectRedisCluster` provide bounded asynchronous discovery
for hosts that use the bundled adapters. Unknown facts remain permissive for
custom-executor compatibility. Known-unavailable tools are advertised with a
stable capability error by default; `UnavailableToolPolicy::Hide` removes them
from `tools/list` instead. Every catalog entry exposes its minimum Redis/module
versions and required command names.

The current catalog marks `redis_info`, `redis_dbsize`, `redis_scan`,
`redis_randomkey`, `redis_hotkeys`, and `redis_ft_list` as standalone-only because
their contracts require database-wide aggregation that these tools do not yet
implement for Cluster. A discovered cluster
snapshot therefore hides or rejects those tools instead of silently reporting
one node as the whole database.

The curated default enables the `essentials`, `data_structures`, and
`diagnostics` bundles. The module-backed `json` and `search` bundles and the
guarded `admin` bundle are available only through deliberate composition. The
`sessions` bundle is enabled by supplying its manager; `bulk` remains reserved
for further catalog growth. Raw execution is always controlled by its separate
policy rather than bundle selection alone, and unrestricted raw invocation
cannot bypass guarded administration tools.

See [the architecture decisions](docs/architecture.md) for the intentional
Tower-MCP boundary and fixed-target model, and the
[redisctl compatibility inventory](docs/redisctl-compatibility.md) for the
132-tool read-only baseline and known contract differences. The
[command-surface scorecard](docs/surface-comparison.md) pins both redisctl and
`redis/mcp-redis`, maps every baseline tool, and defines the objective gate for
the earlier competitor-parity gate. The
[official Redis command ledger](docs/redis-command-coverage.md) pins Redis
8.10.1 itself and is the command-completeness source of truth.

## Compatibility and testing

On Unix, `cargo test --workspace --all-features` starts isolated Redis processes
through
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
trips, KNN pagination, and typed text/tag/numeric/geo hybrid filters. It also
covers the complete redisctl Search baseline: structured advanced search,
typed aggregate pipelines and cursor lifecycle, aliases, schema alteration,
explain/profile, dictionaries, synonyms, deprecated tag values, binary fields,
output limits, ACL command restrictions, module versions, and same-slot
three-node Cluster routing.

CI runs the complete suite on Redis 8.8 and the live router/stdio contract on
every currently supported Redis Open Source series: 6.2, 7.2, 7.4, 8.0, 8.2,
8.4, 8.6, 8.8, and 8.10.1. Standalone and Cluster jobs cover the latest pin,
and a separate job regenerates the official command metadata from the pinned
Redis image. Live tests exercise both RESP2 and RESP3, the 201-tool curated
catalog, binary and nil responses, conditional and absolute expiration,
bounded serialization/restore, complete bounded list semantics, typed
hash-field expiration, binary-safe membership, budgeted set algebra, complete
bounded sorted-set semantics, typed bitmap/bitfield, geospatial, and
HyperLogLog semantics, complete Streams and consumer-group workflows, Redis 8
vector sets, Redis Arrays, and the finite modern core deltas through Redis 8.10,
finite blocking reads, binary-safe Pub/Sub publication and inspection,
owner-isolated subscription sessions, bounded buffers and reads, cancellation,
idle cleanup, reconnect/resubscription, bounded and redacted standalone and
Cluster diagnostics, ACL failures, bounded connection loss
and recovery, and the real `redis-mcp-server` stdio process. A separate job pins
the official
`redis/redis-stack-server:7.4.0-v8` image and runs the JSON/Search lifecycle.
Dedicated three-master cluster jobs run on Redis 6.2, 8.8, and 8.10.1 and exercise
redirection, multi-slot aggregation, bounded all-node Pub/Sub inspection,
slot-routed publication, global and sharded subscription sessions, same-slot
copy/rename/list movement and set algebra, Redis 8 Array/vector routing and
modern atomic same-slot contracts,
single- and same-slot multi-stream reads, stable cross-slot failures, and the
cluster-configured stdio server. The version list follows the
[Redis Open Source version-management table](https://redis.io/docs/latest/operate/oss_and_stack/install/version-mgmt/).

## Non-goals

- Cloud or Enterprise REST APIs
- redisctl profiles or per-tool target URLs
- unbounded or connection-stateful operations without a finite
  session/workflow contract
- terminal tokenization, history, completion, result rendering, or an
  application-specific CLI/REPL frontend

## License

MIT or Apache-2.0, at your option.
