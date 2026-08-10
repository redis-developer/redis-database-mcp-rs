# Library boundary decisions

Date: 2026-08-05

Status: accepted for the 0.1 foundation

## Decision summary

`redis-mcp` owns Redis tool contracts, tool behavior, access classification,
structured results, and the command-execution boundary. It exposes a
transport-independent Tower-MCP router and a redis-rs-backed fixed-target
executor, but hosts can implement the executor without depending on redis-rs.

The default server remains fixed-target. Redis URLs, redisctl profile names,
credentials, and target selection do not appear in the default tool schemas.

## Host execution boundary

`RedisExecutor` receives a crate-owned `RedisCommand` and returns a crate-owned
`RedisValue` or `RedisError`.

- `RedisCommand` exposes the originating tool, required access level, uppercase
  Redis command name, and binary-safe arguments. Its `Debug` implementation
  reports only the argument count so values and credentials are not logged.
- `RedisValue` represents RESP2 and RESP3 values without exposing redis-rs
  types. Binary strings remain bytes; maps and attributes retain entry order.
- `RedisErrorKind` gives adapters and tool handlers stable authentication,
  authorization, timeout, connection, request, response, server, and fallback
  categories.
- `DirectRedis` is the standalone convenience adapter that converts these
  types to and from redis-rs 1.5 and uses its reconnecting connection manager.
- `DirectRedisCluster` is the fixed-cluster convenience adapter. It discovers
  topology from configured seed URLs and delegates redirection, topology
  refresh, and supported multi-slot command splitting to redis-rs.
- Their `from_connection_manager` and `from_cluster_connection` constructors
  are intentionally redis-rs-specific, but implementing `RedisExecutor` is not.

Redis Cluster `CROSSSLOT` failures map to `RedisErrorKind::InvalidRequest` and
retain the stable `CROSSSLOT` code. Other server-side cluster failures retain
their Redis codes under the stable error categories. A custom host adapter can
make different routing decisions while preserving the same crate-owned result
surface.

Every executor future is bounded by the router's command timeout (30 seconds by
default). A custom executor can apply a shorter transport or command timeout,
but cannot bypass the library's outer bound.

## Tower-MCP is an intentional public dependency

`RedisMcpBuilder::build` returns `tower_mcp::McpRouter`. This coupling is
intentional: merging the Redis router with another host router is the primary
in-process composition surface, and hiding the type behind a wrapper would not
remove the version requirement at the merge point.

A Tower-MCP upgrade that prevents a consumer from merging routers is therefore
a public compatibility change for this crate and must be called out in release
notes. redisctl's current Tower-MCP 0.8.2 line cannot merge this crate's 0.19
router; redisctl must move to the compatible line before integration. This
repository does not carry a compatibility copy of Tower-MCP types.

## Target selection and the future redisctl adapter

The default composition model binds one executor to one router. The standalone
binary accepts either one standalone URL or one or more Redis Cluster seed URLs;
both are server configuration and neither appears in tool schemas. A host can
instead resolve a profile, cluster, or other connection policy before
constructing its executor, as shown in `examples/custom_executor.rs`. The
executor receives tool and access metadata for host-side telemetry and audit
records.

redisctl currently exposes optional `url` and `profile` on each database tool.
Those fields are deliberately absent here:

- credential-bearing URLs in generic tool calls weaken the standalone server's
  fixed-target/SSRF boundary;
- profile names are redisctl application concepts rather than Redis tool
  concepts;
- accepting fields that the fixed-target server ignores would create a
  misleading contract.

The future redisctl integration must make one of two choices explicitly:

1. bind the Redis MCP router to a server/session-selected profile and retire
   per-call target fields; or
2. add an opt-in, host-supplied target-selector extension to this crate before
   migration, with opaque selector values interpreted only by the host.

Until that choice is made, only the fixed/default-profile adapter is considered
straightforward. The current per-call profile/URL surface is recorded as known
compatibility pressure rather than being smuggled into the executor trait or
the standalone schemas.

## Bundles and access

Bundles answer which coherent capabilities a host wants; access mode answers
which side effects that host permits. These decisions are orthogonal.

The public taxonomy is `essentials`, `data_structures`, `json`, `search`,
`diagnostics`, `admin`, `bulk`, and `raw`. The curated default enables
`essentials`, `data_structures`, and `diagnostics`, totaling 32 tools. The
module-backed `json` and `search` bundles are explicitly selected so a default
router never advertises capabilities that its Redis target may not provide.
Empty bundles are reserved for coherent catalog growth and do not expose
placeholder tools.

Module requirements are part of both catalog metadata and each crate-owned
`RedisCommand`. A host adapter can inspect the requirement for routing or
telemetry. If Redis reports an unknown command for a module-backed tool, the
library maps it to `RedisErrorKind::ModuleUnavailable` with the capability and
command name, but without echoing command arguments. This covers both a missing
module and an installed version too old to provide the command. Other module
errors, such as a missing index or malformed query, remain ordinary server
errors.

Raw commands remain a separate opt-in even though their metadata belongs to the
`raw` bundle. They require full access and one of two enabled policies:

- `Classified` permits only command names reviewed as bounded
  request/response operations and fails closed for unknown names.
- `Unrestricted` permits unknown request/response commands, while retaining
  hard blocks for authentication/connection state, transactions, streaming,
  subscriptions, replication handshakes, unbounded scripts, and blocking
forms.

## Output budgets and continuation contracts

`OutputBudget` is host policy applied after each typed tool has built its
result. The byte ceiling measures the complete serialized MCP
`CallToolResult`, including structured content, text rendering, and base64
expansion. The entry ceiling applies to typed collection results and recursively
to collection-shaped raw RESP values. Defaults are 256 KiB and 1,000 entries;
zero limits are rejected at router construction.

Oversized success candidates are replaced by an MCP error result with stable
`io.redis.mcp/outputLimit` metadata containing the `output_limit_exceeded` code,
dimension, actual size, configured limit, retryability, and narrowing guidance.
Error details live in MCP `_meta` rather than `structuredContent`, so they do not
conflict with the tool's advertised success `outputSchema`. The library never
emits partial JSON. Every catalog entry also exposes its dominant
`ToolOutputPolicy` so hosts can audit whether a tool is intrinsically bounded,
budget guarded, or cursor-, range-, or offset-paginated.

`redis_scan`, `redis_hscan`, `redis_sscan`, and `redis_zscan` continue with a
Redis cursor. `redis_lrange` and `redis_zrange` default to ranks 0 through 99
and continue with a start index. `redis_ft_search` always emits a LIMIT clause
and continues with an offset. Whole-collection reads remain available for small
values, but `redis_hgetall` and `redis_smembers` direct oversized callers to the
corresponding scan tool.

Redis ACLs remain the ultimate authorization boundary. Bundle selection, access
mode, annotations, raw policy, and timeouts are defense-in-depth and product
contract layers; none is presented as a replacement for ACLs.

## Contract change discipline

`tests/snapshots/curated_catalog.json` records each implemented tool's name,
bundle, access tier, required module, raw opt-in, description, input schema,
output schema, annotations, and representative structured result. Any
deliberate public contract change updates that snapshot in the same review.
