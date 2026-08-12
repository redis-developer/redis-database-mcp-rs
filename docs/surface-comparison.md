# Redis MCP command-surface scorecard

Date: 2026-08-11

This scorecard defines the command-surface finish line for the
[library command-surface roadmap](https://github.com/redis-developer/redis-database-mcp-rs/issues/14).
It compares capabilities and contract quality rather than treating a larger
tool count as success.

The machine-readable source is
[`surface-comparison.json`](surface-comparison.json). The
`surface_comparison` integration test verifies that every pinned baseline tool
is mapped exactly once and that every implemented library tool remains in the
matrix.

## Pinned baselines

| Surface | Revision | Release | Tools |
| --- | --- | ---: | ---: |
| [`redis/mcp-redis`](https://github.com/redis/mcp-redis) | `5945b0b5b098c9a1882075a161a6a58f23de81ed` | 0.5.1 | 53 |
| [`redisctl`](https://github.com/redis/redisctl) | `955f4b18f4266c332bc640cada67d125d23edde8` | read-only inventory | 132 |
| `redis-mcp` | current catalog | 0.1 development line | 136 |

The redisctl baseline is a compatibility and breadth reference, not a promise
to copy application-specific profile fields, aliases, or weak contracts.

## Current capability coverage

| Baseline | Implemented | Planned | Superseded | Excluded |
| --- | ---: | ---: | ---: | ---: |
| `redis/mcp-redis` | 45 | 6 | 1 | 1 |
| redisctl | 94 | 32 | 1 | 5 |

The dispositions mean:

- **implemented**: a first-class structured library tool exists;
- **planned**: the capability belongs here and has a tracked issue;
- **superseded**: the baseline behavior has a safer or more composable
  replacement;
- **excluded**: the behavior is outside the Redis database library boundary or
  should not be a first-class tool.

One tool name is not necessarily one capability. `redis_ft_info`, for example,
subsumes both `get_index_info` and `get_indexed_keys_number`. Conversely, name
overlap does not imply equivalent quality. The library now adds three bounded
cursor tools absent from both pinned inventories: `redis_hscan`, `redis_sscan`,
and `redis_zscan`.

### Remaining `redis/mcp-redis` gaps

| Capability | Competitor tools | Library issue |
| --- | --- | ---: |
| bounded Pub/Sub | `publish` | #29 |
| subscription sessions | `subscribe`, `psubscribe`, `read_messages`, `unsubscribe` | #30 |
| diagnostics | `client_list` | #31 |

`scan_all_keys` is intentionally superseded by cursor-based `redis_scan`; the
library will not disguise a full keyspace materialization as a safe scan.
`search_redis_documents` is deliberately excluded because external
documentation search is useful but is neither a Redis database command nor a
no-egress operation.

Vector and hybrid search now lead the pinned competitor surface: one typed
index schema covers HASH and JSON with FLAT or HNSW, numeric vectors are encoded
without a process-global mode, HASH vectors have explicit binary-safe helpers,
and focused KNN and hybrid tools return documents, distances, counts, and
bounded continuation metadata. Hybrid callers use escaped text, tag, numeric,
and geo clauses rather than interpolating a raw filter expression.

## Contract-quality comparison

Scores use a checked-in four-level rubric:

- 0: absent or actively misleading;
- 1: partial, inconsistent, or substantially unbounded;
- 2: complete and dependable for the selected surface;
- 3: differentiated with enforced contracts and adversarial coverage.

| Dimension | `redis-mcp` | `redis/mcp-redis` | Remaining target |
| --- | ---: | ---: | --- |
| schemas | 3 | 2 | Keep input and output contracts snapshotted. |
| structured results | 3 | 1 | Keep success and error semantics independent of prose. |
| error semantics | 3 | 1 | Preserve stable categories, Redis codes, and redaction. |
| annotations and access | 3 | 0 | Keep discovery and handler enforcement derived from one policy. |
| output bounds | 3 | 1 | Preserve centralized byte limits, typed continuations, and adversarial boundary coverage. |
| binary safety | 3 | 0 | Preserve explicit UTF-8/base64 values. |
| cluster behavior | 3 | 1 | Specify slot, fan-out, aggregation, and partial failures per tool. |
| capability/version awareness | 3 | 1 | Preserve bounded direct discovery, partial custom snapshots, catalog requirements, and hide/advertise policy. |
| live compatibility testing | 3 | 1 | Keep real MCP calls across Redis, Stack, and cluster CI. |
| embedding and host policy | 3 | 0 | Keep redisctl and REPL policy outside tool definitions. |

The aggregate is currently 30/30 versus 8/30, with the library leading in
all ten dimensions. The aggregate is descriptive, not the completion
test: a high score cannot compensate for a missing strategic capability or an
unbounded default tool.

The embedding score now includes the public `RedisInvocationEngine`: a
pre-tokenized Redis-style frontend can invoke binary argv directly while
retaining the same access classification, raw policy, capability checks,
timeout, redaction, error taxonomy, and response budgets as the MCP
`redis_command` path. Terminal parsing and presentation remain outside the
database library.

Keys and strings also lead the pinned surface. Binary-safe SET models NX/XX,
GET, and exactly one of EX/PX/EXAT/PXAT/KEEPTTL; ranges and serialized payloads
are bounded; side-effectful returned values can be omitted with their byte
length and outcome preserved; and copy, rename, and restore expose separate
overwrite-capable tools at full access. Safe OBJECT inspection is a typed
choice and intentionally provides no unbounded HELP form. Same-slot Cluster
operations are live-tested alongside stable CROSSSLOT failures.

Hashes now materially exceed both pinned surfaces. The library covers complete
binary-safe reads, bounded ordered HMGET and multi-field HSET, integer and
decimal increments, deterministic whole-hash reads with HSCAN fallback, and
full-access deletion. Results preserve empty values while distinguishing a
missing field from a missing hash. Redis 7.4 field expiration is capability
gated and exposes typed HEXPIRE, HTTL, and HPERSIST per-field statuses. Contract
tests pin exact binary argv and schemas; live tests cover RESP2/RESP3, ACLs,
large hashes, wrong types, version gates, and remote Cluster slots.

Lists now do as well. The library covers binary-safe head and tail pushes,
indexed reads, length, bounded position searches and ranges, counted
non-blocking pops, removal, replacement, trimming, and atomic edge-to-edge
movement. Results distinguish missing lists, out-of-range indexes, empty
values, and empty result arrays. Destructive operations require full access;
Redis 6.0/6.2 features are capability-gated; oversized pop and move payloads
are explicitly omitted without losing the committed outcome; and `LMOVE`
preserves the native same-slot Cluster contract. Blocking commands remain
outside the ordinary MCP tool surface.

Sets now form another strict superset. The library covers binary-safe add and
full-access removal, cardinality, single and request-aligned multi-member
checks, deterministic whole-set reads, bounded cursor scans, and byte-sorted
difference/intersection/union results. Missing sets and absent members remain
distinct, SMISMEMBER is gated to Redis 6.2, and live tests cover RESP2/RESP3,
ACLs, binary data, large-result budgets, same-slot Cluster algebra, and stable
CROSSSLOT failures. The destructive `*STORE` forms are intentionally not
curated because a small integer response cannot bound their destination
cardinality or overwrite effect; they require the explicit full-access
unrestricted invocation path.

Sorted sets are now another strict superset. The library covers binary-safe
members, exact finite decimal inputs, canonical string scores, cardinality,
single and request-aligned multi-score reads, forward and reverse rank, score
increments, bounded cursor scans, full-access removal and pops, and one tagged
rank/score/lex range contract with explicit bounds and continuation metadata.
Redis 5.0/6.2 features are capability-gated; live tests cover RESP2/RESP3,
ACLs, large requests, exact binary argv, missing keys and members, and remote
Cluster slots. All curated operations are single-key. Multi-key
union/intersection remains deferred until its same-slot or explicit fan-out
contract is defined.

Streams and consumer groups are now a strict superset of both pinned surfaces.
The library covers XADD/XDEL/XLEN, forward and reverse bounded ranges, finite
multi-stream reads, structured XINFO and XPENDING, the complete ordinary
XGROUP lifecycle, XREADGROUP, XACK, XCLAIM, and resumable XAUTOCLAIM. IDs and
special offsets are typed; fields, values, groups, and consumers are
binary-safe; `BLOCK 0` is impossible; and group reads or claims retain the
committed IDs when oversized fields are omitted. Live tests cover RESP2/RESP3,
nil and NOGROUP states, ACLs, finite blocking timeouts, Redis version shapes,
remote Cluster slots, same-slot multi-stream reads, and stable CROSSSLOT
failures.

RedisJSON is now a strict superset of the redisctl JSON family. Seventeen tools
cover structured reads, conditional writes, numeric and boolean mutation,
object inspection, array lifecycle operations, deletion, clearing, and RFC
7396 merge. Enhanced JSONPath and legacy paths are explicit modes; results
preserve missing-key, missing-path, wrong-type, and request-aligned nil states.
Structured JSON inputs avoid double encoding, every variable result is bounded,
and a committed oversized `JSON.ARRPOP` value can be omitted without hiding the
mutation. Destructive operations require full access, module versions are
capability-gated, ACL key patterns are live-tested, and deprecated
`JSON.NUMMULTBY` is omitted in favor of `JSON.NUMINCRBY`.

### Output-policy audit

Every successful result is measured as a complete encoded MCP
`CallToolResult`, including its structured content and text rendering. Binary
values are measured after base64 expansion. The configured entry ceiling is
also applied to collection results and raw RESP collections.

| Policy | Tools |
| --- | --- |
| cursor paginated | `redis_scan`, `redis_hscan`, `redis_sscan`, `redis_zscan` |
| range paginated | `redis_lrange`, `redis_zrange`, `redis_xrange`, `redis_xrevrange` |
| offset paginated | `redis_ft_search`, `redis_ft_vector_search`, `redis_ft_hybrid_search` |
| budget guarded | `redis_command`, `redis_dump`, `redis_ft_info`, `redis_ft_list`, `redis_get`, `redis_getdel`, `redis_getex`, `redis_getrange`, `redis_hget`, `redis_hgetall`, `redis_hkeys`, `redis_hmget`, `redis_hvals`, `redis_info`, `redis_json_arrappend`, `redis_json_arrinsert`, `redis_json_arrlen`, `redis_json_arrpop`, `redis_json_arrtrim`, `redis_json_get`, `redis_json_mget`, `redis_json_numincrby`, `redis_json_objkeys`, `redis_json_objlen`, `redis_json_strlen`, `redis_json_toggle`, `redis_json_type`, `redis_mget`, `redis_randomkey`, `redis_sdiff`, `redis_set`, `redis_sinter`, `redis_smembers`, `redis_smismember`, `redis_sunion`, `redis_vector_get_hash`, `redis_xinfo_consumers`, `redis_xinfo_groups`, `redis_xinfo_stream`, `redis_xpending`, `redis_zmscore` |
| intrinsically bounded | `redis_append`, `redis_copy`, `redis_copy_replace`, `redis_dbsize`, `redis_decr`, `redis_decrby`, `redis_del`, `redis_exists`, `redis_expire`, `redis_ft_create`, `redis_ft_dropindex`, `redis_hdel`, `redis_hexists`, `redis_hexpire`, `redis_hincrby`, `redis_hincrbyfloat`, `redis_hlen`, `redis_hpersist`, `redis_hset`, `redis_hstrlen`, `redis_httl`, `redis_incr`, `redis_incrby`, `redis_incrbyfloat`, `redis_json_clear`, `redis_json_del`, `redis_json_merge`, `redis_json_set`, `redis_lindex`, `redis_llen`, `redis_lmove`, `redis_lpop`, `redis_lpos`, `redis_lpush`, `redis_lrem`, `redis_lset`, `redis_ltrim`, `redis_memory_usage`, `redis_mset`, `redis_object_inspect`, `redis_persist`, `redis_ping`, `redis_rename`, `redis_renamenx`, `redis_restore`, `redis_restore_replace`, `redis_rpop`, `redis_rpush`, `redis_sadd`, `redis_scard`, `redis_setrange`, `redis_sismember`, `redis_srem`, `redis_strlen`, `redis_touch`, `redis_ttl`, `redis_type`, `redis_unlink`, `redis_vector_set_hash`, `redis_xack`, `redis_xadd`, `redis_xautoclaim`, `redis_xclaim`, `redis_xdel`, `redis_xgroup_create`, `redis_xgroup_createconsumer`, `redis_xgroup_delconsumer`, `redis_xgroup_destroy`, `redis_xgroup_setid`, `redis_xlen`, `redis_xread`, `redis_xreadgroup`, `redis_xtrim`, `redis_zadd`, `redis_zcard`, `redis_zcount`, `redis_zincrby`, `redis_zpopmax`, `redis_zpopmin`, `redis_zrank`, `redis_zrem`, `redis_zremrangebyscore`, `redis_zrevrank`, `redis_zscore` |

Budget-guarded whole-collection reads fail with an `output_limit_exceeded`
reason and machine-readable `io.redis.mcp/outputLimit` metadata instead of
emitting partial JSON. `redis_hgetall`, `redis_hkeys`, `redis_hvals`, and
`redis_smembers` point callers to their cursor alternatives. INFO can be
narrowed by section, Search continues by offset, list and rank ranges continue
by start, score and lex ranges continue by offset, and cursor tools continue
using the returned Redis cursor. `SET GET`, `GETEX`,
`GETDEL`, counted list and sorted-set pops, `LMOVE`, and `JSON.ARRPOP`
additionally accept
per-call byte caps so a committed side effect can return an explicit omitted
value and byte count instead of losing the mutation outcome to an output error.

## Objective completion gate

The roadmap is complete only when all of these are true:

1. Every pinned `redis/mcp-redis` tool is implemented, superseded, or excluded
   with a concrete rationale. No planned competitor mapping remains.
2. Every implemented library tool appears exactly once in the matrix.
3. Every contract dimension scores at least 2, the library trails the
   competitor in none, and it leads in at least six.
4. `vector.search`, `streams.consumer_groups`, and `pubsub.sessions` are
   implemented.
5. The checked-in `current_gate.met` value agrees with the gate calculated by
   the test.

The gate is currently **not met**. Six roadmap blockers remain after completing
the Streams and RedisJSON families.

## Updating the scorecard

When either surface changes:

1. pin the new upstream revision and release;
2. update the baseline tool list and capability mapping together;
3. add a reason and issue for every planned capability;
4. update contract scores only with source or test evidence;
5. run `cargo test --test surface_comparison` and review the JSON diff.

The comparison never fetches upstream state during CI. Reviews receive a
deterministic diff rather than a result that changes when another repository
moves.
