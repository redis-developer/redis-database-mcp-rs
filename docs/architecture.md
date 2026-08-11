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
  authorization, timeout, connection, request, response, capability, module,
  server, and fallback categories.
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

## Governed native invocation boundary

`RedisInvocationEngine` is the library seam for Redis-style CLI and REPL
frontends. It accepts a pre-tokenized, binary-safe `NativeRedisInvocation` and
returns the same crate-owned `RedisValue` or `RedisError` used by MCP tools.
Frontends retain ownership of tokenization, quoting, history, completion,
display formatting, and interactive sessions.

`RedisExecutor` is also implemented for `Arc<T>` where `T` is an executor, so
a combined product can give one shared host adapter to both `RedisMcp` and
`RedisInvocationEngine` without adding `Clone` to the executor contract.

The engine is not a shortcut around MCP policy. Before execution it:

- normalizes and classifies the command name;
- enforces `ReadOnly`, `ReadWrite`, or `Full` access for the classified command
  form;
- applies `Disabled`, fail-closed `Classified`, or explicitly stronger
  `Unrestricted` raw-command policy;
- rejects connection-state, transaction, subscription, streaming,
  replication-handshake, script, and blocking forms that need dedicated
  session APIs;
- checks known command, Redis-version, and module capabilities;
- applies the same outer executor timeout and Redis error taxonomy as curated
  tools;
- redacts custom-executor details at the native boundary while retaining the
  stable error kind and code; and
- recursively measures RESP collections and canonical JSON bytes against the
  configured `OutputBudget`.

Unknown commands in `Unrestricted` mode still require `Full` access. Known
forms expose `NativeCommandMetadata` so a frontend can report the normalized
name, access classification, and version/module requirements without
reimplementing library policy. The MCP `redis_command` handler delegates to
this engine and then applies its complete-`CallToolResult` byte check, so native
and MCP consumers share execution policy while retaining their appropriate
result envelopes. See `examples/native_argv.rs`.

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
`essentials`, `data_structures`, and `diagnostics`, totaling 78 tools. The
module-backed `json` and `search` bundles are explicitly selected so a default
router never advertises capabilities that its Redis target may not provide.
Empty bundles are reserved for coherent catalog growth and do not expose
placeholder tools.

Every catalog entry has `ToolCapabilityRequirements`: required commands,
optional minimum Redis and module versions, and the required module. Module
requirements also travel on each crate-owned `RedisCommand` for host routing
and telemetry.

`RedisCapabilities` is a crate-owned, partially known snapshot. It records the
Redis version, standalone or cluster deployment, module presence and versions,
and command availability. Custom hosts can supply it directly without using
redis-rs. Missing facts remain `Unknown`; this is deliberately permissive so
existing custom executors continue to work. `DirectRedis` and
`DirectRedisCluster` can discover a snapshot asynchronously under one bounded
total timeout using INFO, MODULE LIST, and COMMAND INFO. Cluster INFO and module
responses are reduced conservatively: the oldest node version and capabilities
present on every reported node determine availability.

Known incompatibilities are checked before executing a tool. The default
`UnavailableToolPolicy::Advertise` keeps a stable command surface and returns
`CapabilityUnavailable` or `ModuleUnavailable` with stable error codes.
`UnavailableToolPolicy::Hide` applies the same catalog evaluation to MCP
discovery and rejects direct calls to filtered tools. A Redis unknown-command
response for a module-backed tool remains a redacted fallback classification
when discovery was not available. Other module errors, such as a missing index
or malformed query, remain ordinary server errors.

Deployment requirements are catalog data too. `redis_info`, `redis_dbsize`,
`redis_scan`, and `redis_randomkey` are currently standalone-only: the cluster
adapter cannot yet aggregate their node-local or fan-out responses into the
database-wide result those contracts promise. Known cluster snapshots make
that limitation explicit instead of returning an arbitrary node's answer.

Raw commands remain a separate opt-in even though their metadata belongs to the
`raw` bundle. The MCP tool requires full access; direct native invocations are
authorized per classified command. Both use one of two enabled policies:

- `Classified` permits only command names reviewed as bounded
  request/response operations and fails closed for unknown names.
- `Unrestricted` permits unknown request/response commands, while retaining
  hard blocks for authentication/connection state, transactions, streaming,
  subscriptions, replication handshakes, unbounded scripts, and blocking
  forms.

## Output budgets and continuation contracts

`OutputBudget` is host policy applied after each typed tool has built its
result. For MCP, the byte ceiling measures the complete serialized
`CallToolResult`, including structured content, text rendering, and base64
expansion. The entry ceiling applies to typed collection results and recursively
to collection-shaped raw RESP values. Native invocation measures the canonical
JSON representation of its crate-owned RESP value before returning it. Defaults
are 256 KiB and 1,000 entries; zero limits are rejected at construction.

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
values, but `redis_hgetall`, `redis_hkeys`, and `redis_hvals` direct oversized
callers to `redis_hscan`, while `redis_smembers` directs them to `redis_sscan`.
Bounded multi-field hash reads such as `redis_hmget` and `redis_httl` reject
requests above the configured entry ceiling before execution.

List searches and counted pops also reject counts above the configured entry
ceiling before execution. Pop and `LMOVE` results have per-call byte caps;
when a committed mutation returns an oversized value, the result preserves the
element count, byte count, and mutation outcome while explicitly marking the
payload omitted. Blocking list commands are not exposed as ordinary tools.

Set membership inputs are bounded to the configured entry ceiling and retain
one result per requested member in request order. Whole-set and algebra results
are byte-sorted for deterministic structured output, then checked against both
output ceilings; `redis_sscan` remains the cursor alternative for large source
sets. Algebra keys are capped at 1,000, stay binary-safe for custom executors,
and preserve Redis Cluster's native same-slot requirement. Direct cluster
adapters therefore return stable `CROSSSLOT` errors, while a custom executor
may implement a different routing policy behind the same crate-owned command.

`SDIFFSTORE`, `SINTERSTORE`, and `SUNIONSTORE` are deliberately not curated
tools. Although their integer replies are small, the destination write can
materialize and overwrite an unbounded set, so the MCP output budget does not
bound their effect. They remain reachable only through the explicitly stronger
full-access unrestricted invocation policy rather than being advertised as
ordinary bounded set operations.

Redis ACLs remain the ultimate authorization boundary. Bundle selection, access
mode, annotations, raw policy, and timeouts are defense-in-depth and product
contract layers; none is presented as a replacement for ACLs.

## Contract change discipline

`tests/snapshots/curated_catalog.json` records each implemented tool's name,
bundle, access tier, deployment requirement, Redis/module version and command
requirements, raw opt-in, description, input schema, output schema,
annotations, and representative structured result. Any deliberate public
contract change updates that snapshot in the same review.
