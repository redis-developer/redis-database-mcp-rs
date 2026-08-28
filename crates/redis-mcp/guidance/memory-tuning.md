# Redis memory tuning

How to read memory pressure through this MCP surface and what to change.
Inspection tools are read-only; configuration changes go through the guarded
admin bundle and require explicit confirmation.

## Measure before changing anything

- `redis_memory_summary` — used memory, peak, fragmentation ratio, and
  eviction counters in one bounded read.
- `redis_memory_stats` — the full MEMORY STATS breakdown: dataset versus
  overhead, per-database dictionary costs, client buffers, replication
  backlog.
- `redis_keyspace_summary` — keys and expirations per database; a large gap
  between keys and keys-with-TTL often explains unbounded growth.
- `redis_key_summary` and `redis_memory_usage` — type, TTL, encoding, and
  byte cost of one key; sample the biggest suspects.
- `redis_hotkeys` and `redis_hotkeys_get` (Redis 8.6+) — find the keys that
  dominate traffic; hot and large are different problems.
- `redis_object_inspect` — encoding and idle time for one key.

## Interpret the three big numbers

- `used_memory` versus `maxmemory`: the eviction headroom. With no
  `maxmemory`, Redis grows until the OS intervenes — the kernel OOM killer
  ends the process, which is an availability incident, not an eviction.
- `mem_fragmentation_ratio` (RSS / used): around 1.0–1.5 is healthy. Well
  above that after mass deletion means the allocator is holding freed pages:
  consider `redis_memory_purge` (jemalloc builds) or activedefrag. Below 1.0
  means the OS has swapped Redis memory — treat as an incident; latency will
  follow.
- `evicted_keys` climbing means `maxmemory` is undersized for the working
  set or the policy evicts the wrong keys.

## Choose an eviction policy deliberately

Read the current policy with `redis_config_get`; change it with
`redis_config_set` (Full access plus `confirm_service_impact`).

- `noeviction` (default): writes fail at the limit. Correct for databases
  that must never silently lose data; demands real capacity planning.
- `allkeys-lru`: pure cache with no TTL discipline. Any key can vanish.
- `volatile-lru` / `volatile-ttl`: evicts only keys that have expirations —
  the usual choice when persistent state and cache share one database. If
  nothing has a TTL, this degrades to `noeviction`.
- `allkeys-lfu` / `volatile-lfu` (Redis 4+): frequency beats recency when
  periodic scans would poison an LRU.
- `volatile-random` / `allkeys-random`: cheapest bookkeeping; acceptable
  when access is genuinely uniform, which is rare.

LRU and LFU are sampled approximations, tunable via `maxmemory-samples`;
they are good, not exact.

## Reduce the dataset itself

- Give cacheable keys TTLs at write time: `redis_set` expiration options,
  `redis_expire`, `redis_getex`, and per-hash-field TTLs on Redis 7.4+
  (`redis_hexpire`). The expiration guide
  (`redis-mcp://guidance/expiration-strategies`) covers semantics.
- Keep collections inside their compact-encoding thresholds (listpack,
  intset). One 10,000-field hash costs far more per field than ten
  1,000-field hashes below the threshold; verify with
  `redis_object_inspect` and `redis_memory_usage`.
- Shorten keys and field names on high-cardinality data; the savings
  multiply by key count.
- Prefer hashes over JSON documents when fields are flat; prefer bitmaps
  (`redis_setbit`) and HyperLogLog (`redis_pfadd`) over sets when the
  question is "which offsets" or "how many distinct", not "which members".
- Delete in bulk with `redis_unlink` rather than `redis_del`: reclamation
  happens on a background thread instead of blocking the event loop.

## Memory pressure that is not the dataset

`redis_memory_stats` separates these:

- Client output buffers: one slow consumer of huge replies can hold
  gigabytes; find it with `redis_client_list` and, if necessary, end it with
  `redis_client_control`.
- Replication backlog and replica output buffers grow with write volume and
  disconnected replicas.
- MONITOR and keyspace-notification consumers add per-client overhead; this
  library's own MONITOR sessions are bounded and reaped for exactly that
  reason.

## Version notes

- Redis 4.0+: `redis_memory_usage`, `redis_memory_stats`,
  `redis_memory_purge`, LFU policies, `redis_unlink`.
- Redis 7.4+: hash-field expiration (`redis_hexpire`, `redis_httl`,
  `redis_hpersist`).
- Redis 8.6+: server-maintained hot-key tracking (`redis_hotkeys_control`,
  `redis_hotkeys_get`).
