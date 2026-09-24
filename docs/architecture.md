# Library boundary decisions

Date: 2026-08-05

Status: accepted for the 0.1 foundation

## Decision summary

`redis-mcp` owns Redis tool contracts, tool behavior, access classification,
structured results, and the command-execution boundary. It exposes a
transport-independent Tower-MCP router and redis-tower-backed fixed-target
executors. Hosts can implement the executor without depending on redis-tower.

The default server remains fixed-target. Redis URLs, redisctl profile names,
credentials, and target selection do not appear in the default tool schemas.

## Host execution boundary

`RedisExecutor` receives a crate-owned `RedisCommand` and returns a crate-owned
`RedisValue` or `RedisError`.

- `RedisCommand` exposes the originating tool, required access level, uppercase
  Redis command name, and binary-safe arguments. Its `Debug` implementation
  reports only the argument count so values and credentials are not logged.
- `RedisValue` represents RESP2 and RESP3 values without exposing dependency-owned
  types. Binary strings remain bytes; maps and attributes retain entry order.
- `RedisErrorKind` gives adapters and tool handlers stable authentication,
  authorization, timeout, connection, request, response, capability, module,
  server, and fallback categories.
- `DirectRedis` converts crate-owned commands and replies using redis-tower's
  reconnecting `MultiplexedClient` and a URL connection factory.
- `DirectRedisCluster` uses `MultiplexedClusterClient` for slot routing,
  redirection and topology refresh, with bounded seed fallback. Explicit
  node fan-out snapshots and deduplicates the requested node roles, checks
  the node limit before dispatch, and uses authenticated dedicated sockets.
  MGET, MSET, DEL, UNLINK, EXISTS, and TOUCH retain split-by-slot behavior.
- Both expose `from_multiplexed_client` for hosts that already own a
  redis-tower client. The public `RedisExecutor` boundary remains independent
  of any Redis client library.
- Setup attempts have a ten-second total bound. Ordinary transport response
  timeouts are disabled so the invocation deadline remains authoritative;
  dedicated blocking, transaction and session operations keep their own
  finite bounds. Reconnect does not imply replay of an in-flight write.

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
result envelopes. See `crates/redis-mcp/examples/native_argv.rs`.

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
constructing its executor, as shown in
`crates/redis-mcp/examples/custom_executor.rs`. The
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

## Families, bundles, and access

Cargo features answer which Redis family handlers a host compiles, runtime
families or bundles answer which coherent capabilities it advertises, and
access mode answers which side effects it permits. These decisions are
orthogonal.

The public family taxonomy is `keyspace`, `strings`, `hashes`, `lists`, `sets`,
`sorted-sets`, `streams`, `bitmaps`, `arrays`, `hyperloglog`, `geospatial`,
`vector-sets`, `pubsub`, `scripting`, `json`, `search`, and `timeseries`. Each
is an additive Cargo feature and a `ToolFamily` runtime marker. `all-families`
is the command-family aggregate; `full` adds the cross-cutting `diagnostics`,
`sessions`, `transactions`, and `admin` features.
Default features select `full` to preserve the complete library build; runtime
bundle selection still keeps administration out of the default server catalog.
A size-sensitive host can use `default-features = false` and name only the
families and cross-cutting features it embeds.

`RedisMcpBuilder::families` switches to precise family selection and clears the
legacy bundle defaults. Selecting a family omitted at compile time is a build
error rather than a silently incomplete router. The builder assembles every
selected family around one `ToolState`, so executor ownership, capability
discovery, access policy, budgets, and sessions are never duplicated. Its
result is a normal Tower-MCP router fragment; redisctl can retain its own server
identity and product routers by merging the Redis fragment into its root
`McpRouter`.

Bundles remain the high-level compatibility and cross-cutting assembly API.
They answer which broad capabilities a host wants while preserving the
standalone server's existing contract.

The public taxonomy is `essentials`, `data_structures`, `json`, `search`,
`timeseries`, `diagnostics`, `sessions`, `transactions`, `scripting`, `admin`,
`bulk`, and `raw`. The curated default enables `essentials`,
`data_structures`, and `diagnostics`, totaling 201 tools. The module-backed
`json`, `search`, and `timeseries` bundles are explicitly selected so a
default router never advertises capabilities that its Redis target may not
provide.
The stateful `sessions` bundle is enabled only by supplying a lifecycle manager.
The `scripting` family/bundle is also opt-in at runtime because even its
read-only forms execute server-side code; full access additionally exposes
arbitrary-write execution and script/function lifecycle operations.
The `admin` bundle is independently opt-in at runtime. Read access adds
redacted operational inspection; Full access additionally adds confirmed
control operations. Bounded Cluster fan-out exposes pseudonymous per-node
results and partial failures, configuration reads use a fixed non-secret
allowlist, and raw invocation hard-blocks this surface so it cannot bypass the
typed policy. Empty bundles are reserved for coherent catalog growth and do
not expose placeholder tools. The bundled stdio executable installs the
DirectRedis session manager itself and therefore exposes 207 tools before
admin, module, scripting, or raw additions.

The `timeseries` family follows the same module pattern as `json` and
`search`, against its own pinned RedisTimeSeries 1.12.6 inventory and
disposition ledger. Timestamps stay exact integer milliseconds or the explicit
`*`, `-`, and `+` tokens, values stay finite IEEE 754 doubles, and RESP2 and
RESP3 reply shapes normalize to one structured contract. Alteration is
classified as destructive because retention shrinks trim stored samples and
`TS.ALTER LABELS` replaces the label set. Options newer than the discovered
module version (LATEST, EMPTY, ALIGN, GROUPBY, IGNORE, the TWA aggregator, and
the newer group reducers) fail closed before execution. On Cluster, keyed
tools route by their first key, `TS.MADD` and `TS.CREATERULE` require
same-slot keys, and the keyless multi-series queries are standalone-only
because OSS Cluster provides no database-wide TimeSeries coordinator and a
node-local answer would misrepresent the database.

The Data Structures bundle includes the current Redis 8 core data model rather
than freezing the contract at older Redis releases. Redis 8.0 introduces typed
vector-set and expiring-hash operations; 8.2 adds vector membership and
reference-aware Stream deletion; 8.4 adds conditional string deletion,
digests, expiring multi-set, and stateless vector ranges; 8.8 adds the complete
Redis Array family, bounded numeric increment, and Stream negative
acknowledgement; 8.10 adds multi-element list movement. Capability discovery
hides or rejects every tool below its minimum version or when its command is
absent. Array indices preserve Redis's unsigned 64-bit domain, binary values
carry explicit encodings, and all variable inputs and results are bounded.
ARGREP, ARSCAN, and VRANGE return typed continuation fields.

Connection-local `HIMPORT` migration state, destructive node-local
`TRIMSLOTS`, and internal `XCFGSET`, `XIDMPRECORD`, and `XSETID` operations are
deliberately not advertised as ordinary database tools. MSETEX and LMOVEM keep
their atomic same-slot contract in Cluster and fail with the stable
`CROSSSLOT` classification before execution when their keys differ.

Every catalog entry has `ToolCapabilityRequirements`: required commands,
optional minimum Redis and module versions, and the required module. Module
requirements also travel on each crate-owned `RedisCommand` for host routing
and telemetry.

`RedisCapabilities` is a crate-owned, partially known snapshot. It records the
Redis version, standalone or cluster deployment, module presence and versions,
and command availability. Custom hosts can supply it directly without using
the Redis client. Missing facts remain `Unknown`; this is deliberately permissive
so existing custom executors continue to work. `DirectRedis` and
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
`redis_scan`, `redis_randomkey`, `redis_hotkeys`, and `redis_ft_list` are currently
standalone-only: the cluster adapter cannot yet aggregate their node-local or
fan-out responses into the database-wide result those contracts promise. Known
cluster snapshots make that limitation explicit instead of returning an
arbitrary node's answer.

The remaining diagnostics are cluster-aware. Each all-node inspection carries
a caller-selected node ceiling, pseudonymizes node addresses unless Full access
explicitly enables them, and reports partial failures structurally. CLIENT
LIST, MODULE LIST, and SLOWLOG redact identity, paths, arguments, and future
unknown fields by default. Their explicit sensitive switches require Full
access, while diagnostic server errors retain stable categories but discard
server-supplied details. `redis_hotkeys` deliberately analyzes one SCAN cursor
page and returns its continuation instead of hiding a full keyspace traversal.

The RedisJSON bundle contains 17 structured tools. Enhanced JSONPath is the
default, while legacy paths remain an explicit mode because RedisJSON changes
reply shape between the two. Variable JSON results are measured by encoded
bytes and nested entries; destructive deletion, clearing, array pop/trim, and
merge require full access. `JSON.MGET` retains Redis Cluster's native same-slot
contract. The direct cluster adapter validates its key slots before execution
because module commands need explicit validation of their multi-key contracts;
cross-slot requests therefore fail with the stable `CROSSSLOT` invalid-request classification instead of reaching one arbitrary
node.

The Search bundle contains 24 structured tools. It covers the complete
redisctl Query Engine baseline, typed HASH/JSON vector indexing and search,
and explicit aggregate cursor read/delete operations. Search and aggregate
pages are capped at 100 rows; structured document and row values preserve
UTF-8/base64 encodings. Whole-result index, dictionary, synonym, profile,
explain, and deprecated tag-value reads are budget guarded. Alias replacement,
alias/dictionary deletion, index deletion, and cursor cleanup are separately
classified from additive schema, synonym, dictionary, alias, and vector writes.
Known module versions preflight whole-tool requirements and dialect/vector
options add conditional version checks. Same-slot index names, prefixes, and
documents are live-tested through the cluster adapter without promising
cross-node Search aggregation.

The Essentials bundle also contains seven request/response Pub/Sub tools.
`redis_publish` and Redis 7+ `redis_spublish` accept binary-safe channels and
payloads, cap payloads at 1 MiB, and report the receiver count with its scope.
Global publication propagates cluster-wide, while the Redis reply counts only
subscribers connected to the routed node; sharded publication is routed by the
channel slot and has the same node-local count caveat.

The five inspection tools cover `PUBSUB CHANNELS`, `NUMSUB`, `NUMPAT`,
`SHARDCHANNELS`, and `SHARDNUMSUB`. Channel lists have explicit caller and
global output limits, count queries have bounded binary-safe inputs, and every
cluster fan-out has an explicit `max_cluster_nodes` ceiling. The direct cluster
adapter returns sorted address-tagged node replies instead of accepting
an opaque client aggregate. Handlers then byte-sort and deduplicate channel
names, sum subscriber and pattern counts, and expose server-side node failures
alongside an explicit completeness flag. Transport failures fail the whole
request rather than presenting partial data as complete. Custom executors can
read `RedisCommand::cluster_node_limit` plus `RedisCommand::cluster_fanout` and
return `RedisValue::ClusterNodes` to preserve the same behavior. Long-lived
subscription sessions remain a separate lifecycle surface in their own
optional bundle.

The optional Scripting family/bundle contains 18 Lua and Redis Functions tools.
EVAL/EVALSHA and FCALL inputs carry explicit binary-safe key and argument
arrays; the direct Cluster adapter routes by the first declared key and rejects
different slots before execution. Redis 7+ `_RO` variants are the only
execution tools available to a read-only router. The ordinary forms can run
arbitrary writes or deletions and therefore require full access.

Script-cache inspection fans out to primaries, cache load/flush fans out to all
nodes, function lifecycle and statistics fan out to primaries, and FUNCTION
LIST/DUMP explicitly report one-node scope. Every fan-out has a caller ceiling,
returns address-tagged replies with a completeness flag, and suppresses server
error messages from partial results. Custom executors distinguish the target
through `RedisClusterFanout::Primaries` and `RedisClusterFanout::AllNodes`.
Sources, library code, dump/restore payloads, keys, arguments, and dynamic
results all have hard byte or collection ceilings. The shared request timeout
bounds client waiting; dropping the future does not establish that Redis
stopped script execution. SCRIPT/FUNCTION KILL preserve Redis's rule that an
execution which already wrote cannot be killed safely. `SCRIPT DEBUG` remains
excluded.

The optional Sessions bundle contains six long-lived Pub/Sub operations:
`redis_subscribe`, `redis_psubscribe`, Redis 7+ `redis_ssubscribe`,
`redis_pubsub_read`, `redis_pubsub_unsubscribe`, and `redis_pubsub_close`.
Subscription creation accepts bounded binary-safe channels or patterns and
returns an opaque 128-bit random handle. Reads remove a bounded number of
messages under both raw-byte and encoded MCP output ceilings, wait only for a
finite duration, preserve channel, pattern, payload, sequence, and age, and
report buffer-full and oversized-message drop totals.

`PubSubSessionManager` is a public host boundary rather than part of
`RedisExecutor`. One command executor connection cannot safely represent a
subscription that outlives a request. The DirectRedis manager accordingly owns
one bounded message queue per session and dedicated binary Pub/Sub transports.
Standalone and regular Cluster sessions use one connection; sharded Cluster
sessions group channels by hash slot and use one connection per group, bounded
by the subscription limit. Subscription pushes never share ordinary command
workers. redis-tower replays all confirmed binary subscriptions on reconnect;
sharded sessions follow observed slot ownership changes. Message delivery
remains at-most-once across reconnect gaps. Session teardown aborts its message
pumps and closes their sockets. Sentinel and host-specific failover policies
require a custom manager.

Every manager operation receives a `PubSubSessionOwner`. Lookup uses the owner
and handle together and returns the same not-found result for missing, guessed,
or foreign handles. The builder installs a random default owner and teardown
guard for one-client routers such as stdio. A multi-client HTTP or WebSocket
host must bridge a stable owner extension from its authenticated session or
principal into each request and call `close_owner` when that host session ends;
per-request extensions override the default. The DirectRedis manager also
enforces global and per-owner session quotas, subscriptions per session,
buffered messages, accepted message bytes, read bytes and duration, operation
timeouts, and idle lifetime. A background weak-reference reaper closes stale
connections without waiting for another subscription request, while manager
shutdown rejects creation, drains every session, and wakes pending reads.

Finite ordinary MCP tool calls are intentional here. A cancelled
`redis_pubsub_read` future does not consume a queued message, and finite polling
works with clients that do not implement the evolving MCP task surface. MCP
tasks would add a second lifecycle without improving Redis queue ownership or
delivery semantics, so this version does not require them. The library likewise
does not translate Redis pushes into transport-specific MCP notifications.

Raw commands remain a separate opt-in even though their metadata belongs to the
`raw` bundle. The MCP tool requires full access; direct native invocations are
authorized per classified command. Both use one of two enabled policies:

- `Classified` permits only command names reviewed as bounded
  request/response operations and fails closed for unknown names.
- `Unrestricted` permits unknown request/response commands, while retaining
  hard blocks for authentication/connection state, transactions, streaming,
  subscriptions, replication handshakes, script/function execution and
  lifecycle, guarded administration, and blocking forms. Scripting and
  administration stay behind their dedicated typed surfaces even when
  unrestricted native invocation is enabled.

The opt-in `invocation` bundle exposes the same classified policy to
Redis-syntax MCP clients as graduated tiers instead of one full-access hatch.
`redis_command_readonly` executes only commands classified read-only and
therefore keeps honest read-only MCP annotations; `redis_command_write` adds
ordinary classified writes at read-write access; destructive and unclassified
commands remain exclusive to the full-access `redis_command` tool. A tier
mismatch fails before execution with a stable reason naming the correct tool.
`redis_command_metadata` classifies one argv without executing it — including
stable blocked and unclassified reason codes — and `redis_command_inventory`
lists the argument-independent classified surface with base tiers, escalation
flags, and capability requirements so a CLI can complete and preview commands
from the single shared policy table. Because the classified policy now serves
sub-full tiers, only the unrestricted policy requires full access at build
time; every request is still authorized per classified command.

The opt-in `bulk` bundle activates two write-gated workflows built on the
same execution boundary: `redis_bulk_load` for bounded structured records and
`redis_bulk_seed` for deterministic datasets generated from an explicit seed
and schema by a fixed SplitMix64 stream. Both validate and plan every record
before anything executes, then run sequential batches of bounded concurrent
`RedisExecutor` commands, so access policy, timeouts, telemetry metadata, and
Cluster routing hold per command and no Redis pipeline or background work is
created. Reports stay compact: totals, per-batch counts, and bounded
representative failures that preserve record identity, with explicit
stop/continue-on-error, partial-application flags for non-atomic
multi-command records, and unknown-outcome accounting when the configured
deadline elapses mid-flight. Dry runs perform no writes, and file or URL
ingestion remains a host concern outside the library contract.

Atomic transactions extend that policy rather than bypassing it. The
`transactions` bundle exposes one `redis_transaction` tool that executes a
bounded command list inside MULTI/EXEC with optional WATCH keys, and the
library exposes the same capability to non-MCP frontends through
`RedisTransactionEngine`. Every nested command is classified exactly like a
native invocation, so the hard boundaries above cannot be smuggled into a
transaction body, per-command capability checks still apply, and the whole
transaction requires the maximum access of any nested command. Because the
body is argv-shaped generic execution, the bundle additionally requires an
enabled raw command policy and full access.

Execution crosses the public `RedisTransactionExecutor` boundary, which is
separate from `RedisExecutor` for the same reason Pub/Sub sessions have their
own manager: MULTI and WATCH are connection state. `DirectRedisTransactions`
dials one dedicated connection per transaction and drops it afterwards, so
state never leaks into pooled connections or across MCP calls, and a cancelled
call abandons its connection instead of leaking a half-built MULTI. WATCH is
issued as its own fail-closed round trip before the atomic pipeline; a watch
that cannot be established never reaches MULTI. On Cluster, watched keys must
hash to one slot, the pipeline is pinned to that slot's node so EXEC observes
the WATCH state, and the per-transaction cluster client disables request
retries because a replayed pipeline could commit twice. Command count, watch
keys, request bytes, and total duration are bounded ahead of execution, and
committed results share the global output budgets. Outcomes are explicit:
committed results align one-to-one with the submitted commands and keep
runtime failures in-band per entry, a watched-key conflict reports `aborted`,
and queue-time rejections report `rejected` with per-command server codes
while executing nothing. Connection loss or timeout after EXEC left the client
reports `TRANSACTION_OUTCOME_UNKNOWN` semantics; the library never replays a
possibly committed transaction.

Blocking and streaming commands get the same dedicated-connection treatment
instead of being forced into ordinary request/response calls. The `sessions`
bundle's finite blocking tools (`BLPOP`/`BRPOP`, `BLMOVE`, the Redis 8.10
`BLMOVEM`, `BLMPOP`/`BZMPOP`, `BZPOPMIN`/`BZPOPMAX`, `WAIT`, and `WAITAOF`)
run through `RedisBlockingEngine` over the public `RedisBlockingExecutor`
boundary: every call requires a positive, engine-capped timeout, dials one
fresh connection under a client deadline that always exceeds the server
timeout, and never replays a command after it may have been delivered,
because a replayed blocking pop could consume a second element.
`DirectRedisBlocking` bounds concurrent dedicated connections with a
semaphore and disables Cluster request retries; multi-key routing follows the
first key while the server rejects cross-slot lists. A server-side timeout is
a typed result, not an error, and WAIT/WAITAOF report achieved counts on a
fresh connection without implying durability for writes issued elsewhere.

MONITOR reuses the owner-isolated session shape through the
`MonitorSessionManager` boundary and `DirectRedisMonitorSessions`: an opaque
handle owns one dedicated monitoring connection whose events are parsed and
redacted at capture time — client addresses become stable per-session
pseudonyms, argument values are omitted unless the session opted in at start,
and lines that do not parse are counted and dropped rather than partially
exposed. Buffers drop oldest with explicit counters, reads are finite and
cancellable, per-owner and global quotas are far below the Pub/Sub ceilings
because MONITOR reduces server throughput, and idle or disconnected sessions
are reaped. The Redis 8.10 backup lifecycle lands in the admin bundle as four
guarded transitions built on the shared executor: `redis_backup_start`,
`redis_backup_seal`, and the confirmation-gated `redis_backup_abort` and
`redis_backup_cleanup` acknowledge server-global state changes, pair with the
finite status and file-count inspections, and never return filesystem paths.

## Guidance prompts and resources

Tools define what a client can run; the `guidance` bundle defines what it
should do. It contributes no tools: it registers MCP resources and prompts on
the same router, rides in the curated default surface, and compiles out with
the `guidance` Cargo feature.

Static knowledge is compiled into the library from `crates/redis-mcp/guidance`
and served as `redis-mcp://guidance/{slug}` markdown resources, so content
versions with the crate, never fetches externally, and stays bounded by an
enforced per-document ceiling. Two live resources describe the actual
configured surface: `redis-mcp://catalog` renders every curated tool with its
bundle, access tier, raw opt-in, and capability requirements, and
`redis-mcp://capabilities` renders the capability snapshot the router was
built with, in the same shape tools preflight against. Prompts are guided
workflows — latency diagnosis, memory review, data-model design, Search index
design, and bounded bulk loading — whose generated instructions name exact
tools and guidance URIs.

Drift is checked in both directions: unit tests validate that every
`redis_*` name mentioned anywhere in guidance content or rendered prompts
exists in the compiled catalog and that cross-references between guides
resolve, while the contract suite snapshots the advertised prompt and
resource catalog exactly like the tool catalog and verifies that routers
without the bundle advertise neither.

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
Redis cursor. `redis_lrange` and rank-mode `redis_zrange` default to ranks 0
through 99 and continue with a start index; score- and lex-mode `redis_zrange`
continue with an offset. `redis_xrange` and `redis_xrevrange` fetch one bounded
page and continue from the last returned stream ID using an exclusive bound.
`redis_ft_search` always emits a LIMIT clause and continues with an offset.
`redis_ft_aggregate` can create a bounded server cursor, and
`redis_ft_cursor_read` continues one bounded page at a time. Whole-collection
reads remain available for small values, but `redis_hgetall`, `redis_hkeys`,
and `redis_hvals` direct oversized callers to `redis_hscan`, while
`redis_smembers` directs them to `redis_sscan`. Bounded multi-field reads such
as `redis_hmget`, `redis_httl`, and
`redis_zmscore` reject requests above the configured entry ceiling before
execution.

List searches and counted list or sorted-set pops also reject counts above the
configured entry ceiling before execution. Pop and `LMOVE` results have
per-call byte caps; when a committed mutation returns an oversized value, the
result preserves the element count, byte count, and mutation outcome while
explicitly marking the payload omitted. Blocking list commands are not exposed
as ordinary tools.

Set membership inputs are bounded to the configured entry ceiling and retain
one result per requested member in request order. Whole-set and algebra results
are byte-sorted for deterministic structured output, then checked against both
output ceilings; `redis_sscan` remains the cursor alternative for large source
sets. Algebra keys are capped at 1,000, stay binary-safe for custom executors,
and preserve Redis Cluster's native same-slot requirement. Direct cluster
adapters therefore return stable `CROSSSLOT` errors, while a custom executor
may implement a different routing policy behind the same crate-owned command.

Sorted-set keys and members are binary-safe, and scores are accepted as JSON
numbers or finite decimal strings while score results remain strings instead of
being coerced through JSON numbers. `redis_zrange` uses a tagged rank, score, or
lexicographic range so incompatible modes cannot be combined; score and lex
ranges have explicit inclusive, exclusive, and infinite bounds plus bounded
offset pagination. Multi-member score reads and destructive pops are rejected
before execution when their requested cardinality exceeds the entry ceiling;
oversized popped members are explicitly omitted after reporting the committed
count and aggregate member bytes.

All curated sorted-set operations are single-key. Future union/intersection
tools must define their same-slot or explicit fan-out behavior before entering
the catalog; the current surface makes no cluster-wide aggregation promise.

Bitmap and bitfield tools use explicit zero-based offsets, signed/unsigned
widths, indexed or absolute addressing, and per-increment overflow modes.
Read offsets retain Redis's full 32-bit range, while `SETBIT` and mutating
`BITFIELD` operations cap one-call string growth at 16 MiB. Exact bitfield
integers are returned as decimal strings. `BITOP` is full access because it
overwrites a destination; its byte-count reply describes result size rather
than bounding the mutation.

Geospatial inputs accept JSON-number shorthand or exact finite decimal strings,
with Redis longitude and latitude bounds checked before execution. `GEOSEARCH`
always emits a caller-bounded `COUNT` and returns binary-safe members together
with exact distance, integer geohash, and coordinate strings.
`GEOSEARCHSTORE` is full access and explicitly overwrites at most the requested
number of destination members. HyperLogLog tools likewise label counts as
approximate, distinguish empty-key initialization and `PFADD` register changes
from exact novelty, and treat `PFMERGE` as a destination overwrite. `BITOP`, `GEOSEARCHSTORE`,
multi-key `PFCOUNT`, and `PFMERGE` preserve Redis Cluster's native same-slot
contract and return stable `CROSSSLOT` errors otherwise.

Streams use structured IDs rather than overloaded strings for entry creation,
range bounds, group start positions, and read offsets. Fields, values, group
names, and consumer names are binary-safe. `XREAD` and `XREADGROUP` require a
finite per-stream count; optional blocking must be positive and strictly below
the library command timeout, so `BLOCK 0` cannot enter the curated path.
`XAUTOCLAIM` also preflights Redis's ten-times-count scan factor against the
entry ceiling. Group reads and claims accept a returned-field byte cap and
retain IDs, counts, and the committed state change when field payloads are
omitted.

Single-stream operations route normally in Cluster. Multi-stream reads retain
Redis's native slot contract: same-slot keys are supported, while cross-slot
requests return the stable `CROSSSLOT` invalid-request classification. The
library does not fan out consumer-group state across slots.

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

`crates/redis-mcp/tests/snapshots/curated_catalog.json` records each implemented
tool's name, bundle, access tier, deployment requirement, Redis/module version
and command requirements, raw opt-in, description, input schema, output schema,
annotations, and representative structured result. Any deliberate public
contract change updates that snapshot in the same review.

`crates/redis-mcp/tests/fixtures/redis-commands-8.10.1.json` separately pins the
official Redis command metadata extracted from `COMMAND` and `COMMAND DOCS`.
`crates/redis-mcp/tests/fixtures/redis-command-coverage.json` maps every
definition to typed/composed catalog evidence, classified native invocation, a
dedicated session/workflow, an explicit backlog issue, or a documented
exclusion. See `docs/redis-command-coverage.md` for the enforced invariants and
update process.
