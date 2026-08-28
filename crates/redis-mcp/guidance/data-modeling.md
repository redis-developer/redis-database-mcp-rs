# Redis data modeling

How to choose keys and structures for a workload served through this MCP
surface. Every recommendation names the tools that implement it.

## Key design

- Build keys from stable, colon-delimited segments: `app:entity:id`
  (`shop:order:1001`). Segments make keyspace analysis possible later
  (`redis_scan` with `match`, `redis_keyspace_summary`, `redis_hotkeys`).
- Keep keys short but self-describing. Keys are stored per entry; a 100-byte
  key on 100 million entries is 10 GB of key overhead alone. Check real
  per-key cost with `redis_memory_usage`.
- Never encode volatile data (timestamps, counters) into keys you will need
  to look up again. Put volatility in the value or the score.
- Binary keys are legal. Every tool here accepts `encoding: "base64"` for
  keys and values; prefer UTF-8 keys unless the source data is binary.
- One logical entity per key. If two values are always read together, put
  them in one hash rather than two string keys: `redis_hset` +
  `redis_hmget` beats two `redis_get` calls and halves the round trips.

## Choosing a structure

| Need | Structure | Core tools |
| --- | --- | --- |
| Opaque value, counter, flag | string | `redis_set`, `redis_get`, `redis_incr`, `redis_incrby` |
| Object with named fields | hash | `redis_hset`, `redis_hmget`, `redis_hgetall`, `redis_hrandfield` |
| Nested/queryable document | JSON | `redis_json_set`, `redis_json_get`, `redis_json_merge` |
| Queue, recent-N list, timeline | list | `redis_lpush`, `redis_rpop`, `redis_lrange`, `redis_ltrim` |
| Uniqueness, membership, tags | set | `redis_sadd`, `redis_sismember`, `redis_sinter` |
| Ranking, priority, time index | sorted set | `redis_zadd`, `redis_zrange`, `redis_zrangestore` |
| Append-only events, consumers | stream | `redis_xadd`, `redis_xreadgroup`, `redis_xack` |
| Approximate distinct count | HyperLogLog | `redis_pfadd`, `redis_pfcount` |
| Bit flags per offset | bitmap | `redis_setbit`, `redis_bitcount`, `redis_bitfield` |
| Coordinates and radius search | geospatial | `redis_geoadd`, `redis_geosearch` |
| Embedding similarity | vector set (Redis 8) | `redis_vadd`, `redis_vsim` |

Guidelines that repeatedly matter:

- Hash versus JSON: hashes are flat field/value maps with the lowest memory
  overhead and per-field expiration from Redis 7.4 (`redis_hexpire`,
  `redis_httl`). JSON (RedisJSON module) supports nesting, typed paths, and
  Search indexing over paths; it costs more memory. If you never need nested
  paths, use a hash.
- Hash versus many string keys: prefer one hash per entity. Small hashes use
  a compact listpack encoding; thousands of separate string keys pay per-key
  overhead and pollute the keyspace. Verify encoding with
  `redis_object_inspect`.
- Sorted set scores are IEEE 754 doubles. Integers stay exact only up to
  2^53; for larger identifiers keep the identifier in the member and the
  ordering timestamp in the score. Tools return scores as exact decimal
  strings.
- Streams versus lists for queues: lists (`redis_lpush` + `redis_brpop`) are
  simple single-consumer queues; streams add consumer groups, pending-entry
  tracking, and replay (`redis_xreadgroup`, `redis_xpending`,
  `redis_xautoclaim`). If a message must survive a crashed consumer, use a
  stream.
- Counters: `redis_incr` on a string is atomic and cheap. Per-field counters
  in one hash (`redis_hincrby`) group related counters and expire together.

## Reads should be bounded from the start

Model so that no read ever needs the whole structure:

- Cap list growth at write time with `redis_ltrim` (a timeline that only
  ever needs 1,000 entries should never hold 1,000,000).
- Cap streams with `redis_xtrim` or `redis_xadd`'s trim options.
- Read collections in pages: `redis_hscan`, `redis_sscan`, `redis_zscan`,
  and `redis_scan` expose one explicit cursor page per call; `redis_lrange`
  and `redis_zrange` take explicit bounds and return typed continuations.
- If `redis_hgetall` or `redis_smembers` hits the output budget, that is the
  signal the structure has outgrown whole-value reads — switch the access
  path to cursor pages, or split the structure.

## Redis Cluster changes key design

Multi-key operations require all keys in one hash slot. Choose hash tags at
modeling time, not migration time: `shop:{order:1001}:items` and
`shop:{order:1001}:status` share slot `{order:1001}` and can be used together
in `redis_sinterstore`, `redis_transaction`, or `redis_blmove`. See the
cluster key design guide (`redis-mcp://guidance/cluster-key-design`) before
sharding anything.

## Verify a model empirically

1. Load a representative dataset with `redis_bulk_load`, or generate a
   deterministic synthetic one with `redis_bulk_seed`.
2. Measure per-key memory with `redis_memory_usage` and keyspace shape with
   `redis_keyspace_summary` and `redis_key_summary`.
3. Check encodings with `redis_object_inspect`: listpack/intset encodings
   confirm the model stays in compact representations.
4. Exercise the real read paths and watch `redis_slowlog` and
   `redis_latency_history` for scans that should have been indexes.
