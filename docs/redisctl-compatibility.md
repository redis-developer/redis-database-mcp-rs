# redisctl database-tool compatibility inventory

Date: 2026-08-12

Read-only source: redisctl commit
`955f4b18f4266c332bc640cada67d125d23edde8`

No redisctl files were changed while producing this inventory.

The cross-project [command-surface scorecard](surface-comparison.md) maps this
baseline and the pinned `redis/mcp-redis` catalog to implemented, planned,
superseded, or excluded library capabilities. Its machine-readable source and
test keep this inventory connected to the live `redis-mcp` catalog.

## Catalog baseline

The redisctl database router declares 132 unique tool names across nine
submodules:

| redisctl submodule | Count | Tool names |
| --- | ---: | --- |
| aliases | 4 | `redis_alias_delete`, `redis_alias_list`, `redis_alias_run`, `redis_alias_set` |
| bulk | 2 | `redis_bulk_load`, `redis_seed` |
| diagnostics | 4 | `redis_connection_summary`, `redis_health_check`, `redis_hotkeys`, `redis_key_summary` |
| JSON | 16 | `redis_json_arrappend`, `redis_json_arrinsert`, `redis_json_arrlen`, `redis_json_arrpop`, `redis_json_arrtrim`, `redis_json_clear`, `redis_json_del`, `redis_json_get`, `redis_json_mget`, `redis_json_numincrby`, `redis_json_objkeys`, `redis_json_objlen`, `redis_json_set`, `redis_json_strlen`, `redis_json_toggle`, `redis_json_type` |
| keys | 31 | `redis_append`, `redis_copy`, `redis_decr`, `redis_del`, `redis_dump`, `redis_exists`, `redis_expire`, `redis_get`, `redis_getrange`, `redis_incr`, `redis_keys`, `redis_memory_usage`, `redis_mget`, `redis_mset`, `redis_object_encoding`, `redis_object_freq`, `redis_object_help`, `redis_object_idletime`, `redis_persist`, `redis_randomkey`, `redis_rename`, `redis_restore`, `redis_scan`, `redis_set`, `redis_setnx`, `redis_setrange`, `redis_strlen`, `redis_touch`, `redis_ttl`, `redis_type`, `redis_unlink` |
| raw | 1 | `redis_command` |
| search | 18 | `redis_ft_aggregate`, `redis_ft_aliasadd`, `redis_ft_aliasdel`, `redis_ft_aliasupdate`, `redis_ft_alter`, `redis_ft_create`, `redis_ft_dictadd`, `redis_ft_dictdel`, `redis_ft_dictdump`, `redis_ft_dropindex`, `redis_ft_explain`, `redis_ft_info`, `redis_ft_list`, `redis_ft_profile`, `redis_ft_search`, `redis_ft_syndump`, `redis_ft_synupdate`, `redis_ft_tagvals` |
| server | 14 | `redis_acl_list`, `redis_acl_whoami`, `redis_client_list`, `redis_cluster_info`, `redis_config_get`, `redis_config_set`, `redis_dbsize`, `redis_flushdb`, `redis_info`, `redis_latency_history`, `redis_memory_stats`, `redis_module_list`, `redis_ping`, `redis_slowlog` |
| structures | 42 | `redis_hdel`, `redis_hexists`, `redis_hexpire`, `redis_hget`, `redis_hgetall`, `redis_hincrby`, `redis_hkeys`, `redis_hlen`, `redis_hmget`, `redis_hset`, `redis_hvals`, `redis_lindex`, `redis_llen`, `redis_lpop`, `redis_lpush`, `redis_lrange`, `redis_pubsub_channels`, `redis_pubsub_numsub`, `redis_rpop`, `redis_rpush`, `redis_sadd`, `redis_scard`, `redis_sdiff`, `redis_sinter`, `redis_sismember`, `redis_smembers`, `redis_srem`, `redis_sunion`, `redis_xadd`, `redis_xinfo_stream`, `redis_xlen`, `redis_xrange`, `redis_xtrim`, `redis_zadd`, `redis_zcard`, `redis_zcount`, `redis_zrange`, `redis_zrangebyscore`, `redis_zrank`, `redis_zrem`, `redis_zremrangebyscore`, `redis_zscore` |

The source contains about 5,500 Redis tool implementation lines. Redis and
Redis Stack behavior is covered primarily by `tests/redis_tools.rs` and
`tests/redis_stack_tools.rs`, totaling about 2,700 lines at the baseline.

## Current overlap

The library implements 115 names from the baseline: 80 default tools, 34
optional RedisJSON/Search tools, and the separately enabled raw tool. One typed
`redis_object_inspect` additionally covers three redisctl OBJECT tools without
copying their names. Matching a name does not imply an identical contract:

| Tool | Input compatibility | Intentional library behavior |
| --- | --- | --- |
| `redis_ping` | Redis arguments match; redisctl also injects `url` and `profile`. | Structured response and measured latency instead of prose. |
| `redis_info` | `section` matches; redisctl also injects `url` and `profile`. | Parsed properties plus the raw INFO response. |
| `redis_client_list` | Redis-domain filters replace redisctl's unfiltered call; target fields remain host-owned. | Structured records have a total result ceiling. Addresses, names, usernames, library identity, unknown fields, and real cluster node addresses require Full access. |
| `redis_cluster_info` | Target fields are omitted; `max_cluster_nodes` bounds fan-out. | Parses known CLUSTER INFO metrics, retains unknown fields, pseudonymizes node addresses, and reports partial failures. |
| `redis_slowlog` | `limit` corresponds to redisctl's `count`; cluster and disclosure ceilings are explicit. | Arguments and client identity are redacted by default because slowlog entries can contain credentials and user data. |
| `redis_memory_stats`, `redis_module_list`, `redis_latency_history`, `redis_acl_whoami` | Redis-domain inputs correspond; target fields are omitted and cluster tools add node bounds. | Results are structured, forward-compatible, binary-safe, output-bounded, and explicit about partial cluster failures. Module paths/arguments require Full access. |
| `redis_health_check`, `redis_connection_summary`, `redis_key_summary` | Redis-domain intent is preserved without target fields. | Structured summaries run under total workflow bounds; connection identity is never included in aggregate output and key names remain binary-safe. |
| `redis_hotkeys` | Pattern and sample controls correspond, but the library uses an explicit Redis cursor page. | Exactly one SCAN page is analyzed and returned with continuation metadata; the tool never hides a full keyspace walk. |
| `redis_keyspace_summary`, `redis_memory_summary` | New library summaries with no direct redisctl name. | Compact, cluster-aware derived views complement the raw structured INFO keyspace and MEMORY STATS tools. |
| `redis_dbsize` | Redis arguments match; redisctl also injects target fields. | Structured unsigned key count. |
| `redis_scan` | Pattern and type filter correspond, but redisctl accepts `limit` and loops to accumulate results. | One bounded cursor page with `cursor` and `count`; callers explicitly continue. This is a deliberate contract change. |
| `redis_get` | `key` matches; redisctl also injects target fields. | Nil is explicit and binary values are base64 rather than lossy UTF-8/prose. |
| `redis_type` | `key` matches; redisctl also injects target fields. | Structured key/type result. |
| `redis_ttl` | `key` matches; redisctl also injects target fields. | Structured TTL plus `exists` and `persistent` flags. |
| `redis_set` | `key` and `value` match; binary encodings and typed `condition`/`expiration` objects are intentional schema extensions. | NX/XX, GET, EX/PX/EXAT/PXAT/KEEPTTL are mutually valid choices. Results distinguish applied/no-op, prior existence, nil, binary encoding, and explicitly omitted oversized prior values. |
| `redis_pubsub_channels` | The pattern corresponds; the library makes it a binary-safe value and requires a bounded result limit plus a cluster-node ceiling. | Results are byte-sorted and deduplicated. Cluster fan-out reports completeness and node failures instead of implying that one node is the whole cluster. |
| `redis_pubsub_numsub` | Channel names correspond; the library uses bounded binary-safe channel objects and a cluster-node ceiling. | Returns structured per-channel counts, deduplicates requested names, and sums node-local counts deterministically in Cluster. |
| `redis_del` | `keys` matches; redisctl also injects target fields. | Enforces 1–1000 keys and returns requested/deleted counts. |
| `redis_command` | redisctl uses `args`, `dry_run`, `url`, and `profile`; the library uses `arguments` and fixed-target configuration. | Structured RESP output delegates to the public binary-safe `RedisInvocationEngine`, which centralizes access classification, raw policy, timeout, capabilities, redaction, and output budgets. This is intentionally not wire-compatible today. |

The expanded overlap also includes `redis_exists`, `redis_mget`,
`redis_strlen`, `redis_memory_usage`, `redis_randomkey`, `redis_expire`,
`redis_persist`, `redis_mset`, `redis_incr`, `redis_append`, `redis_unlink`,
`redis_copy`, `redis_decr`, `redis_dump`, `redis_getrange`, `redis_rename`,
`redis_restore`, `redis_setrange`, `redis_touch`,
`redis_hdel`, `redis_hexists`, `redis_hexpire`, `redis_hget`, `redis_hgetall`,
`redis_hincrby`, `redis_hkeys`, `redis_hlen`, `redis_hmget`, `redis_hset`,
`redis_hvals`, `redis_lindex`, `redis_llen`, `redis_lpop`, `redis_lrange`,
`redis_lpush`, `redis_rpop`, `redis_rpush`, `redis_smembers`, `redis_sadd`,
`redis_scard`, `redis_sdiff`, `redis_sinter`, `redis_sismember`, `redis_srem`,
`redis_sunion`, `redis_zcard`, `redis_zcount`, `redis_zrank`, `redis_zrem`,
`redis_zremrangebyscore`, `redis_zscore`, `redis_zrange`, `redis_zadd`,
`redis_xadd`, `redis_xinfo_stream`, `redis_xlen`, `redis_xrange`, and
`redis_xtrim`.

The library additionally exposes `redis_hscan`, `redis_hincrbyfloat`,
`redis_hpersist`, `redis_hstrlen`, `redis_httl`, `redis_lmove`, `redis_lpos`,
`redis_lrem`, `redis_lset`, `redis_ltrim`, `redis_smismember`, `redis_sscan`,
`redis_zincrby`, `redis_zmscore`, `redis_zpopmax`, `redis_zpopmin`,
`redis_zrevrank`, `redis_zscan`, `redis_xrevrange`, `redis_xread`,
`redis_xinfo_groups`, `redis_xinfo_consumers`, `redis_xpending`,
`redis_xgroup_create`, `redis_xgroup_setid`, `redis_xgroup_createconsumer`,
`redis_xreadgroup`, `redis_xack`, `redis_xclaim`, `redis_xautoclaim`,
`redis_xdel`, `redis_xgroup_destroy`, and `redis_xgroup_delconsumer`.
Request/response Pub/Sub additionally includes binary-safe `redis_publish`,
Redis 7+ `redis_spublish`, `redis_pubsub_numpat`,
`redis_pubsub_shardchannels`, and `redis_pubsub_shardnumsub`, all with typed
receiver/count results and explicit cluster semantics.
The separate `sessions` bundle adds owner-isolated global, pattern, and sharded
subscriptions, finite bounded reads, exact unsubscribe, and explicit close.
Those tools have no redisctl baseline names; they are a library lifecycle
surface backed by a host-supplied manager rather than ordinary command
execution.
These are command-surface improvements rather than redisctl name overlap. Hash
reads distinguish a missing hash, missing field, and empty value;
field-expiration tools are capability-gated to Redis 7.4 or newer and return
typed per-field statuses; scan tools return one bounded Redis cursor page with
  typed continuation metadata.
- Streams use explicit IDs and special offsets, preserve binary fields and
  group/consumer names, bound every range/read/claim page, forbid indefinite
  blocking, and expose the complete ordinary consumer-group lifecycle. Group
  reads and claims retain committed IDs when returned fields exceed their
  per-call byte cap; same-slot multi-stream Cluster reads are supported and
  cross-slot reads fail explicitly.

- Redis-domain input names remain compatible where practical (`keys`,
  `entries`, `fields`, `elements`, `members`, and the ZADD flags). ZRANGE uses
  one tagged rank/score/lex range object instead of copying mutually ambiguous
  top-level flags; that single tool also supersedes redisctl's separate
  `redis_zrangebyscore` name.
- Every collection input is bounded to 1–1000 items in both JSON Schema and
  handler validation.
- `redis_expire` accepts only positive seconds at the read-write tier; Redis's
  delete-on-nonpositive behavior belongs behind full access instead.
- `redis_copy` and `redis_restore` never overwrite. Their overwrite-capable
  forms are separate full-access tools; rename operations are full access
  because they remove the source and may overwrite the destination.
- `redis_dump` and `redis_restore` cap serialized payloads; OBJECT inspection
  is one typed operation over encoding, frequency, idle time, or reference
  count, and intentionally omits `OBJECT HELP`.
- `GETRANGE` accepts only bounded non-negative ranges, and `SETRANGE` limits
  both write size and resulting sparse extent.
- Collection reads use explicit UTF-8/base64 encodings and deterministic order
  where Redis itself is unordered (hash fields and set members).
- Multi-field HSET and HMGET preserve binary data and request order, enforce
  configured entry ceilings, and expose added/updated or present/missing status
  without collapsing nil and empty values.
- Hash deletion is full-access and destructive; increments and field-expiration
  mutations are read-write and independently ACL-enforced by Redis.
- List reads, pushes, searches, and counted pops are bounded and binary-safe.
  Missing lists, out-of-range indexes, empty element values, and empty result
  arrays remain distinct. Removal, pop, replacement, trim, and movement require
  full access; `LMOVE` documents and tests its same-slot Cluster contract, and
  blocking list commands remain outside ordinary request/response tools.
- Set keys and members are binary-safe. Single and ordered multi-member checks
  distinguish a missing set from absent members; whole-set and algebra results
  are deterministically byte-sorted and output-budgeted; SREM is full-access;
  and multi-key algebra documents and tests Redis Cluster's same-slot rule.
  The destructive `*STORE` variants are not curated because output limits do
  not bound the cardinality or overwrite effect of their destination writes.
- Sorted-set keys and members are binary-safe, exact decimal string inputs avoid
  JSON-number coercion, and score outputs remain canonical strings. Reads
  distinguish missing keys and members; multi-score results stay aligned with
  request order; rank, score, and lex ranges are explicitly tagged and bounded;
  pops and removals require full access. The current family is single-key, so
  normal Cluster routing is live-tested without implying multi-key fan-out.
- Outputs are structured rather than preserving redisctl's prose rendering.

The optional module-backed overlap adds all 16 redisctl JSON names:
`redis_json_arrappend`, `redis_json_arrinsert`, `redis_json_arrlen`,
`redis_json_arrpop`, `redis_json_arrtrim`, `redis_json_clear`, `redis_json_del`,
`redis_json_get`, `redis_json_mget`, `redis_json_numincrby`,
`redis_json_objkeys`, `redis_json_objlen`, `redis_json_set`,
`redis_json_strlen`, `redis_json_toggle`, and `redis_json_type`. The library's
`redis_json_merge` is a deliberate additional JSON capability. Search adds
all 18 redisctl Search names: `redis_ft_aggregate`, `redis_ft_aliasadd`,
`redis_ft_aliasdel`, `redis_ft_aliasupdate`, `redis_ft_alter`,
`redis_ft_create`, `redis_ft_dictadd`, `redis_ft_dictdel`,
`redis_ft_dictdump`, `redis_ft_dropindex`, `redis_ft_explain`,
`redis_ft_info`, `redis_ft_list`, `redis_ft_profile`, `redis_ft_search`,
`redis_ft_syndump`, `redis_ft_synupdate`, and `redis_ft_tagvals`. The Search
bundle additionally provides binary-safe vector helpers and typed vector and
hybrid search, plus explicit `redis_ft_cursor_read` and
`redis_ft_cursor_del` lifecycle tools.

- Tool names and Redis-domain field names remain aligned where practical.
- `redis_json_set.value` accepts structured JSON directly. redisctl accepts a
  string containing another layer of JSON; the library intentionally removes
  that double encoding.
- JSON tools default to enhanced JSONPath and accept an explicit legacy mode
  where reply shape differs. Reads preserve aligned nil and wrong-type results,
  distinguish missing keys from missing paths, and bound both encoded bytes and
  nested JSON entries.
- Read-write operations accept structured JSON values, preserve conditional SET
  no-ops, and report matched paths or array outcomes. Deletion, clearing, array
  pop/trim, and RFC 7396 merge require full access; `redis_json_arrpop` can omit
  an oversized committed value without obscuring the pop outcome.
- RedisJSON 2.0 gates the enhanced-path family and RedisJSON 2.6 gates MERGE.
  Deprecated `JSON.NUMMULTBY` is intentionally omitted in favor of
  `JSON.NUMINCRBY`.
- Search pagination is bounded to 100 results per call, always emits LIMIT, and
  returns a typed continuation offset. Responses expose structured binary-safe
  documents and retain the protocol sequence for compatibility. Sorting,
  scores and explanations, legacy numeric/geo filters, key/field restrictions,
  highlighting, summarization, language, scorer, expander, timeout, parameters,
  and dialect selection are explicit schema fields.
- Aggregation uses ordered typed GROUPBY/reducer, SORTBY, APPLY, FILTER, and
  LIMIT stages. Result rows are structured and binary-safe; optional server
  cursors have bounded read pages and explicit early deletion. Explain/profile,
  aliases, schema alteration, dictionaries, synonyms, and the deprecated
  whole-result `FT.TAGVALS` surface all have structured results, output
  ceilings, access annotations, command/version requirements, ACL coverage,
  and live standalone and same-slot Cluster tests.
- `redis_ft_create` supports HASH and JSON indexes with TEXT, TAG, NUMERIC,
  GEO, and typed FLAT/HNSW VECTOR fields. Focused vector tools encode numeric
  arrays as binary-safe FLOAT32/FLOAT64 values, return structured distances and
  documents, and compose KNN with escaped text, tag, numeric, and geo filters.
  redisctl's `if_exists=drop` shortcut remains out of scope because implicit
  index deletion needs clearer destructive-access semantics.
- Redis and module minimum versions plus required commands travel in catalog
  metadata. Module requirements also travel in `RedisCommand`. Hosts can supply
  a crate-owned capability snapshot or use bounded direct-adapter discovery,
  then choose stable advertised errors or hide known-incompatible tools.

## Bundle mapping direction

- `essentials`: broadly useful server, key, and request/response Pub/Sub tools
- `data_structures`: native hashes, lists, sets, sorted sets, and streams
- `json`: explicitly selected RedisJSON operations
- `search`: Redis Query Engine (`FT.*`) operations and module/version behavior
- `diagnostics`: health, connection, latency, memory, and safe server inspection
- `sessions`: owner-isolated, quota-bound Pub/Sub subscription lifecycles
- `admin`: ACL/configuration and destructive server administration
- `bulk`: bounded bulk load and seed workflows
- `raw`: explicitly opted-in command execution

Alias management is not automatically assigned to the Redis library: its
storage, lifecycle, and product semantics must be evaluated separately.
Pub/Sub publication and inspection are bounded request/response tools, while
subscription sessions use their own explicit manager and owner boundary.

## Compatibility rules for later catalog growth

1. Preserve redisctl names when the tool concept remains the same.
2. Preserve Redis-domain inputs when they are sound; do not copy `url` or
   `profile` into fixed-target schemas.
3. Record deliberate input changes, especially bounded pagination replacing
   hidden full scans.
4. Prefer structured, binary-safe outputs over redisctl's preformatted prose.
5. Snapshot names, schemas, annotations, access tier, bundle, and representative
   structured output in this repository before considering redisctl migration.
6. Treat module absence, version constraints, ACL errors, cluster routing, and
   RESP shape differences as contract cases rather than incidental errors.
