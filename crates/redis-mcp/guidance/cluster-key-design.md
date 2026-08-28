# Redis Cluster key design

Redis Cluster shards the keyspace into 16,384 hash slots by CRC16 of the
key. Every multi-key operation must find all its keys in one slot, so key
design decides which operations remain possible.

## Hash tags are the design tool

If a key contains `{...}`, only the bytes inside the first brace pair are
hashed. `shop:{user:42}:cart` and `shop:{user:42}:orders` land in the same
slot; `shop:user:42:cart` and `shop:user:42:orders` almost certainly do not.

Design rule: pick the co-access boundary (usually a user, tenant, session,
or order), put exactly that identifier inside the braces on every key that
must interact, and keep everything else outside the braces so slot
distribution stays wide.

Inspect placement with `redis_cluster_slot` (slot-level key counts and
sampled keys) and cluster health with `redis_cluster_info` and
`redis_cluster_inspect` (Redis 8.4+); per-slot traffic statistics come from
`redis_cluster_slot_stats` on Redis 8.2+.

## What same-slot buys you

These operations work on Cluster exactly when all keys share a slot, and
return the server's stable `CROSSSLOT` error when they do not:

- multi-key reads and writes: `redis_mget`, `redis_mset`, `redis_exists`,
  `redis_del`, `redis_unlink`
- set/sorted-set algebra and stores: `redis_sinter`, `redis_sunionstore`,
  `redis_zinterstore`, `redis_zrangestore`
- key movement and stores: `redis_rename`, `redis_copy`, `redis_lmove`,
  `redis_geosearchstore`, `redis_sort_store`
- atomic transactions: `redis_transaction` validates watched keys
  client-side and pins the MULTI/EXEC pipeline to the slot's node
- scripts and functions: `redis_eval` / `redis_fcall` declare their keys and
  are same-slot validated
- finite blocking calls: `redis_blpop`, `redis_blmove`, `redis_blmpop` route
  by their first key on dedicated connections

The `CROSSSLOT` error is a design signal, not a retry candidate: either the
keys belong together (fix the tags) or the operation should be decomposed.

## Anti-patterns

- One tag for everything: putting `{app}` in every key collapses the entire
  dataset into one slot on one shard — a single-node bottleneck wearing a
  cluster costume. Watch for slot concentration with
  `redis_cluster_slot_stats` and shard imbalance in `redis_cluster_inspect`.
- Tagging by accident: braces occurring naturally in keys (serialized JSON
  fragments, template leftovers) silently narrow hashing to whatever sits
  in the first brace pair.
- Designing single-key today, multi-key tomorrow: adding hash tags later
  changes every key's slot, which means migrating data. Decide co-access
  groups before production data exists.

## Operations that change meaning on Cluster

- Keyspace-wide commands run per node. `redis_scan` walks the answering
  node; `redis_dbsize` and `redis_info` describe one node. Diagnostics
  tools with explicit fan-out (`redis_connection_summary`,
  `redis_health_check`, `redis_memory_summary`) aggregate across nodes with
  bounded node counts and report partial failures per pseudonymous node.
- Node-scoped lifecycles stay node-scoped: MONITOR sessions, `redis_wait` /
  `redis_waitaof`, backup lifecycle transitions, and RedisTimeSeries
  multi-series queries are standalone/node-scoped in this library rather
  than pretending database-wide semantics.
- Pub/Sub: classic pub/sub broadcasts cluster-wide; Redis 7+ sharded
  channels (`redis_spublish`, `redis_ssubscribe`) route by channel slot and
  scale with the cluster instead of flooding the bus.
- Scripts and multi-key writes are routed by this library with retries
  disabled where a replay could double-apply (transactions, blocking pops);
  a MOVED response during topology changes surfaces as a structured error
  to be retried deliberately.

## Verifying a design

1. Seed representative keys with `redis_bulk_seed` (deterministic) or
   `redis_bulk_load`; records route per key and failures keep record
   identity.
2. Confirm slot spread: `redis_cluster_slot_stats` (8.2+) for hot slots,
   `redis_cluster_slot` for suspicious ones.
3. Run the intended multi-key operations against tagged keys and confirm no
   `CROSSSLOT` errors surface.
4. Fan out `redis_health_check` and compare per-node memory and ops to
   catch imbalance early.
