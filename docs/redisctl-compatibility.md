# redisctl database-tool compatibility inventory

Date: 2026-08-05

Read-only source: redisctl commit
`955f4b18f4266c332bc640cada67d125d23edde8`

No redisctl files were changed while producing this inventory.

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

The initial library implements ten names from the baseline. Matching a name
does not imply an identical contract:

| Tool | Input compatibility | Intentional library behavior |
| --- | --- | --- |
| `redis_ping` | Redis arguments match; redisctl also injects `url` and `profile`. | Structured response and measured latency instead of prose. |
| `redis_info` | `section` matches; redisctl also injects `url` and `profile`. | Parsed properties plus the raw INFO response. |
| `redis_dbsize` | Redis arguments match; redisctl also injects target fields. | Structured unsigned key count. |
| `redis_scan` | Pattern and type filter correspond, but redisctl accepts `limit` and loops to accumulate results. | One bounded cursor page with `cursor` and `count`; callers explicitly continue. This is a deliberate contract change. |
| `redis_get` | `key` matches; redisctl also injects target fields. | Nil is explicit and binary values are base64 rather than lossy UTF-8/prose. |
| `redis_type` | `key` matches; redisctl also injects target fields. | Structured key/type result. |
| `redis_ttl` | `key` matches; redisctl also injects target fields. | Structured TTL plus `exists` and `persistent` flags. |
| `redis_set` | `key` and `value` match. redisctl uses `ex`, `px`, `nx`, and `xx`; the initial library only has `expires_in_seconds`. | Structured result. Full conditional/expiry compatibility must be decided before catalog migration. |
| `redis_del` | `keys` matches; redisctl also injects target fields. | Enforces 1–1000 keys and returns requested/deleted counts. |
| `redis_command` | redisctl uses `args`, `dry_run`, `url`, and `profile`; the library uses `arguments` and fixed-target configuration. | Structured RESP output, a library timeout, classified fail-closed mode, and a separate unrestricted opt-in. This is intentionally not wire-compatible today. |

## Bundle mapping direction

- `essentials`: the broadly useful subset of redisctl `server` and `keys`
- `data_structures`: hashes, lists, sets, sorted sets, streams, and selected
  Redis JSON operations
- `search`: Redis Query Engine (`FT.*`) operations and module/version behavior
- `diagnostics`: health, connection, latency, memory, and safe server inspection
- `admin`: ACL/configuration and destructive server administration
- `bulk`: bounded bulk load and seed workflows
- `raw`: explicitly opted-in command execution

Alias management is not automatically assigned to the Redis library: its
storage, lifecycle, and product semantics must be evaluated separately. Pub/Sub
inspection commands may be ordinary bounded diagnostics, but subscription
sessions remain outside request/response tools.

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
