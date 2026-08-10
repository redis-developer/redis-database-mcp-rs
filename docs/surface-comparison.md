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
| `redis-mcp` | current catalog | 0.1 development line | 39 |

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
overlap does not imply equivalent quality: current whole-collection tools still
carry issue #16 until uniform response budgets land.

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
| output bounds | 1 | 1 | Complete #16. |
| binary safety | 3 | 0 | Preserve explicit UTF-8/base64 values. |
| cluster behavior | 3 | 1 | Specify slot, fan-out, aggregation, and partial failures per tool. |
| capability/version awareness | 1 | 1 | Complete #17. |
| live compatibility testing | 3 | 1 | Keep real MCP calls across Redis, Stack, and cluster CI. |
| embedding and host policy | 3 | 0 | Keep redisctl and REPL policy outside tool definitions. |

The aggregate is currently 26/30 versus 8/30, with the library leading in
eight of ten dimensions. The aggregate is descriptive, not the completion
test: a high score cannot compensate for a missing strategic capability or an
unbounded default tool.

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
