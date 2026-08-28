# Redis latency diagnosis

A working order for finding out why Redis is slow, using this MCP surface.
Run the steps in sequence; each one either finds the cause or eliminates a
class of causes.

## 1. Establish that Redis itself is slow

`redis_ping` reports measured round-trip latency, and `redis_health_check`
summarizes reachability, role, persistence status, and load in one call. If
ping latency is fine while the application is slow, look at the application's
connection pooling, serialization, and network path before touching Redis.

## 2. Read what the server has already recorded

- `redis_slowlog` — commands that exceeded `slowlog-log-slower-than`
  (server-side execution time only, excluding network). Repeated entries for
  the same command shape are the highest-signal finding in most incidents.
- `redis_latency_history` and `redis_latency_overview` — spike events Redis
  attributes to internal causes: `fork` (persistence), `command`,
  `aof-write`, expiration cycles.
- `redis_info` — check `instantaneous_ops_per_sec`, `connected_clients`,
  `blocked_clients`, and rejected connections for saturation.
- Reset baselines deliberately with `redis_slowlog_reset` and
  `redis_latency_reset` (Full access) so the next window is attributable.

## 3. Match the finding to its usual cause

- Slowlog full of `KEYS`, unbounded `SMEMBERS`/`HGETALL`/`LRANGE`, or
  `SORT`: O(N) reads over large structures on the single main thread. Find
  the large keys with `redis_key_summary` / `redis_memory_usage`, then move
  the access path to cursor pages (`redis_scan`, `redis_hscan`,
  `redis_sscan`, `redis_zscan`) or bounded ranges. Everything queued behind
  a 500 ms command waits 500 ms.
- `fork` latency events: RDB saves and AOF rewrites fork the process;
  copy-on-write cost scales with dataset size and write rate.
  `redis_server_state` shows persistence state and last-save results. Tune
  save points, move heavy persistence to replicas, or accept the spikes.
- `aof-write` events: fsync stalls on slow disks. `appendfsync everysec` is
  the usual compromise; confirm the disk itself with the host's metrics.
- Expiration bursts: mass-expiring keys in one moment (same TTL written to
  millions of keys) makes the active-expire cycle expensive. Spread TTLs
  with jitter at write time.
- One key dominating traffic: confirm with `redis_hotkeys` (one bounded
  SCAN page ranked by memory) or, on Redis 8.6+, server-side tracking via
  `redis_hotkeys_control` / `redis_hotkeys_get`. Fixes are application-side:
  local caching, key splitting, or read replicas.
- Many blocked clients: `blocked_clients` in `redis_info` counts connections
  parked in blocking pops. This library's blocking tools (`redis_blpop`,
  `redis_blmove`) hold their own dedicated connections with capped timeouts,
  so they never stall other tool calls — but every blocked connection is
  still a connection the server tracks.
- Swapping: `mem_fragmentation_ratio` below 1.0 in `redis_memory_summary`
  means the OS paged Redis out; see the memory guide
  (`redis-mcp://guidance/memory-tuning`).

## 4. Observe live traffic only if you still need to

A bounded MONITOR session shows exactly what commands arrive, at what rate,
from how many distinct clients:

1. `redis_monitor_start` — argument values are omitted by default and
   client addresses are pseudonymized; opt into argument capture only when
   the payloads themselves are the question.
2. `redis_monitor_read` — finite, bounded pages with drop counters.
3. `redis_monitor_close` — always close; MONITOR measurably reduces server
   throughput while attached, which is why sessions here are quota-bound
   and idle-reaped. Standalone targets only; on Cluster, attach to one
   node's URL through a dedicated manager.

## 5. Structural fixes worth knowing

- Batch round trips: `redis_mget`/`redis_mset` replace N string round trips;
  `redis_transaction` groups writes atomically on one dedicated connection;
  the pipeline-versus-transaction guide
  (`redis-mcp://guidance/pipeline-vs-transaction`) covers the trade.
- Replace polling loops with blocking pops (`redis_brpop`) or Pub/Sub
  sessions (`redis_subscribe`, `redis_pubsub_read`): the server pushes
  instead of the client spinning.
- On Cluster, latency on one shard only (visible via `redis_cluster_info`
  and per-node `redis_connection_summary` fan-out) usually means a slot
  imbalance from hash-tag concentration — see
  `redis-mcp://guidance/cluster-key-design`.
