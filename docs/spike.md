# Standalone server and REPL spike

Date: 2026-08-05

## Decision

Proceed with redis-database-mcp-rs as the standalone home for Redis database
MCP tools. Publish the Rust library as redis-mcp and keep redisctl as its
reference and primary consumer.

The repository owns:

- Redis database tool names, descriptions, input schemas, output schemas, and
  structured results
- the read-only, read-write, and full access model
- a small Redis command-execution interface for host integration
- a fixed-target standalone stdio server
- contract and live Redis tests

It does not own:

- redisctl profiles, configuration, credential storage, transport selection,
  audit policy, or Cloud and Enterprise APIs
- an interactive terminal UI
- per-tool Redis URLs
- session-oriented Redis features such as transactions, Pub/Sub, MONITOR, or
  indefinite blocking commands

## What the spike tested

The initial surface contains seven reads, one ordinary write, one destructive
operation, and an independently enabled raw-command escape hatch.

The router was exercised through:

1. an in-process Tower-MCP client for catalog, schema, annotation, access, and
   structured-result contracts;
2. a real stdio client/server process boundary against Redis 8.8;
3. mcp-repl 0.2 using tool discovery, describe, ping, set, get, and scan.

Representative REPL commands:

    redis_ping
    redis_set key=greeting value=hello expiration='{"type":"seconds","value":60}'
    redis_get key=greeting
    redis_scan pattern=gre* count=20

The typed command style is usable in the generic REPL. A future redl experiment
may still add a native Redis command dialect, but that is no longer a
prerequisite for extracting the server surface.

## Design findings

### Fixed targets are the safe default

The standalone server accepts its Redis URL once, as process configuration.
Tool schemas do not accept arbitrary URLs. This avoids turning a generally
reachable MCP server into an arbitrary Redis network client.

redisctl can implement RedisExecutor with its own profile and connection
lifecycle. Compatibility for redisctl's current per-tool profile fields should
be handled by its adapter or a deliberate selectable-target extension, not by
weakening the standalone default.

### Access and visibility are one contract

Read-only routers do not advertise writes. Read-write routers add ordinary
writes but not destructive operations. Full routers add destructive tools.
The raw command tool requires both full access and a separate opt-in.

Mutation handlers also check access at execution time. Catalog filtering and
handler authorization therefore cannot drift independently.

### Raw commands are not a streaming API

The raw tool rejects connection-state, transaction, Pub/Sub, MONITOR, and
blocking command forms. Those operations need a separate session or streaming
design instead of being forced through a request/response tool.

### Structured results are worth preserving

Every successful tool returns structuredContent and declares outputSchema.
Binary string values are base64 encoded with an explicit encoding field.
Cursor-based scan returns the next cursor rather than hiding an unbounded
server traversal behind one call.

## Next implementation slice

1. Create compatibility snapshots for the existing redisctl database tools.
2. Migrate redisctl and this crate to the same Tower-MCP 0.19 and redis-rs 1.5
   dependency lines.
3. Move the existing tools in coherent bundles, preserving names and input
   compatibility while upgrading outputs deliberately.
4. Implement the redisctl RedisExecutor adapter and keep redisctl policy,
   profiles, audit, and transport in redisctl.
5. Run the extracted contract suite against supported Redis and Redis Stack
   versions in CI.

Transactions, Pub/Sub, MONITOR, blocking operations, and a native Redis syntax
front end remain follow-up experiments rather than extraction blockers.

## 2026-08-10 follow-up

The native frontend seam is now implemented as `RedisInvocationEngine`. It
accepts pre-tokenized binary argv and deliberately stops before terminal
syntax or rendering; `redis_command` delegates to the same policy-preserving
service. Session-oriented operations remain separate future work.

## 2026-08-11 hash-surface follow-up

The curated default now includes a complete bounded hash command family. Its
contracts preserve binary fields and values, distinguish nil from empty data,
separate read-write mutations from full-access deletion, and capability-gate
field expiration to Redis 7.4 or newer. The surface is exercised through
RESP2, RESP3, restricted ACLs, output-budget failures, and remote Cluster slots.

## 2026-08-11 list-surface follow-up

The curated default now includes the complete non-blocking list command family:
binary-safe pushes, indexes, lengths, bounded ranges and position searches,
counted pops, removal, replacement, trimming, and atomic movement. Destructive
operations require full access, Redis 6.0/6.2 features are capability-gated,
oversized values preserve mutation outcomes through explicit omission metadata,
and `LMOVE` retains Redis Cluster's same-slot rule. Blocking list commands stay
outside ordinary request/response tools.

## 2026-08-11 set-surface follow-up

The curated default now includes binary-safe cardinality, single and ordered
multi-member checks, full-access removal, cursor scans, and deterministic
budget-guarded difference/intersection/union results. Missing sets stay
distinct from absent members, SMISMEMBER is capability-gated to Redis 6.2, and
the suite exercises RESP2/RESP3, ACLs, large outputs, custom executor argv, and
same-slot versus CROSSSLOT Cluster behavior. The destructive `*STORE` variants
are not advertised as bounded tools because the output budget cannot constrain
their destination cardinality or overwrite effect.
