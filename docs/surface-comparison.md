# Redis MCP command-surface scorecard

Date: 2026-08-10

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
| `redis-mcp` | current catalog | 0.1 development line | 42 |

The redisctl baseline is a compatibility and breadth reference, not a promise
to copy application-specific profile fields, aliases, or weak contracts.

## Current capability coverage

| Baseline | Implemented | Planned | Superseded | Excluded |
| --- | ---: | ---: | ---: | ---: |
| `redis/mcp-redis` | 23 | 28 | 1 | 1 |
| redisctl | 39 | 87 | 1 | 5 |

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
| keys and strings | `rename` | #18 |
| hashes | `hdel`, `hexists` | #19 |
| lists | `llen`, `lpop`, `lrem`, `rpop`, `rpush` | #20 |
| sets | `srem` | #21 |
| sorted sets | `zrem` | #22 |
| Streams and consumer groups | `xack`, `xadd`, `xdel`, `xgroup_create`, `xgroup_destroy`, `xrange`, `xreadgroup` | #23 |
| vector and hybrid search | `create_vector_index_hash`, vector read/write helpers, `vector_search_hash`, `hybrid_search` | #26 |
| bounded Pub/Sub | `publish` | #29 |
| subscription sessions | `subscribe`, `psubscribe`, `read_messages`, `unsubscribe` | #30 |
| diagnostics | `client_list` | #31 |

`scan_all_keys` is intentionally superseded by cursor-based `redis_scan`; the
library will not disguise a full keyspace materialization as a safe scan.
`search_redis_documents` is deliberately excluded because external
documentation search is useful but is neither a Redis database command nor a
no-egress operation.

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
| capability/version awareness | 1 | 1 | Complete #17. |
| live compatibility testing | 3 | 1 | Keep real MCP calls across Redis, Stack, and cluster CI. |
| embedding and host policy | 3 | 0 | Keep redisctl and REPL policy outside tool definitions. |

The aggregate is currently 28/30 versus 8/30, with the library leading in
nine of ten dimensions. The aggregate is descriptive, not the completion
test: a high score cannot compensate for a missing strategic capability or an
unbounded default tool.

### Output-policy audit

Every successful result is measured as a complete encoded MCP
`CallToolResult`, including its structured content and text rendering. Binary
values are measured after base64 expansion. The configured entry ceiling is
also applied to collection results and raw RESP collections.

| Policy | Tools |
| --- | --- |
| cursor paginated | `redis_scan`, `redis_hscan`, `redis_sscan`, `redis_zscan` |
| range paginated | `redis_lrange`, `redis_zrange` |
| offset paginated | `redis_ft_search` |
| budget guarded | `redis_info`, `redis_get`, `redis_mget`, `redis_randomkey`, `redis_hget`, `redis_hgetall`, `redis_smembers`, `redis_json_get`, `redis_json_type`, `redis_ft_list`, `redis_ft_info`, `redis_command` |
| intrinsically bounded | `redis_ping`, `redis_dbsize`, `redis_type`, `redis_ttl`, `redis_exists`, `redis_strlen`, `redis_memory_usage`, `redis_set`, `redis_expire`, `redis_persist`, `redis_mset`, `redis_incr`, `redis_append`, `redis_hset`, `redis_lpush`, `redis_sadd`, `redis_zadd`, `redis_json_set`, `redis_ft_create`, `redis_del`, `redis_unlink`, `redis_json_del`, `redis_ft_dropindex` |

Budget-guarded whole-collection reads fail with an `output_limit_exceeded`
reason and machine-readable `io.redis.mcp/outputLimit` metadata instead of
emitting partial JSON. `redis_hgetall` and `redis_smembers` point callers to
their cursor alternatives. INFO can be narrowed by section, Search continues
by offset, ranges continue by start, and cursor tools continue using the
returned Redis cursor.

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

The gate is currently **not met**. That is expected: this file establishes the
baseline from which backlog work is measured.

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
