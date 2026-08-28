# Redis expiration strategies

How to make data disappear on schedule without surprises, using this MCP
surface.

## The mechanics that shape every strategy

Redis expires keys two ways: lazily when a key is touched after its
deadline, and actively via a background cycle that samples volatile keys.
Consequences:

- Expired-but-unsampled keys still occupy memory until visited or sampled;
  `redis_keyspace_summary` reports keys and expirations per database.
- Writing the same TTL to millions of keys creates an expiration burst that
  shows up in `redis_latency_history` as expire-cycle spikes. Add jitter
  (for a 1 hour TTL, write 3300–3900 seconds).
- A TTL is per key. Overwriting a value with `redis_set` discards any
  existing TTL unless the call uses `keepttl`; plain SET is the classic
  accidental-immortality bug. `redis_getex` reads and adjusts expiration in
  one atomic step; `redis_getdel` reads and removes.

## Setting and inspecting TTLs

- At write time: `redis_set` supports `ex`/`px`/`exat`/`pxat`/`keepttl` as
  typed options — one atomic write with its deadline.
- After the fact: `redis_expire` (positive relative seconds; this surface
  rejects non-positive values rather than hiding delete semantics),
  `redis_persist` to remove a deadline.
- Inspection: `redis_ttl` distinguishes missing keys, persistent keys, and
  live deadlines; `redis_key_summary` includes TTL alongside type, encoding,
  and size.
- Absolute versus relative: prefer absolute (`exat`/`pxat`) when several
  writers race to set the same deadline — relative TTLs re-extend on every
  write.

## Hash-field expiration (Redis 7.4+)

Fields inside one hash can carry their own deadlines:

- `redis_hexpire` — relative or absolute, second or millisecond precision,
  with conditional flags (only-if-no-TTL, only-if-longer, ...).
- `redis_httl` — per-field remaining lifetimes, request-aligned.
- `redis_hpersist` — remove field deadlines.
- `redis_hexpire_delete` covers the delete-on-nonpositive form explicitly.
- `redis_hgetex` / `redis_hsetex` (Redis 8) read or write fields while
  adjusting field TTLs atomically.

This replaces the old pattern of one string key per session attribute purely
for TTL reasons: one `session:{id}` hash, per-field deadlines, one keyspace
entry. On earlier versions the tools report a version capability error
rather than misbehaving.

## Patterns that work

- Cache-aside: write with `redis_set` + `ex` + jitter; read misses fall
  through to the source. Pair with `volatile-lru`/`volatile-ttl` eviction so
  memory pressure evicts only cache entries (see
  `redis-mcp://guidance/memory-tuning`).
- Sessions: one hash per session; refresh the key TTL on activity with
  `redis_expire`; put shorter deadlines on sensitive fields with
  `redis_hexpire` (7.4+).
- Rate limiting: `redis_incr` then set the window TTL only when the counter
  is 1 (the increment result tells you); or use `redis_increx` (Redis 8.8+)
  which increments with expiration semantics in one command.
- Locks and leases: `redis_set` with `nx` + `px` is the lease acquisition;
  never write a lock without a deadline — a crashed holder otherwise leaks
  the lock forever. Release by checking fencing data before `redis_del`, or
  atomically via `redis_transaction` with a watched key or `redis_eval`.
- Time-indexed cleanup without TTLs: sorted sets with timestamp scores
  (`redis_zadd`, then periodic `redis_zremrangebyscore` below a cutoff)
  give range-controlled retirement where TTLs are too blunt — for example
  "keep the last 30 days" over shared structures.
- Streams and lists don't expire entries individually: cap them at write
  time (`redis_xtrim`, `redis_xadd` trim options, `redis_ltrim`).

## Pitfalls checklist

- SET without `keepttl` on a key that had a TTL → immortal key. Audit with
  `redis_scan` + `redis_ttl` sampling.
- Same TTL constant everywhere → synchronized expiration storm; jitter it.
- TTL as the only cleanup for unbounded structures → hashes/sets grow for
  the whole TTL window; bound size too.
- Relying on expiration order → expiration is approximate in time; never a
  scheduler. For ordered work, use a sorted set or stream and consume
  explicitly (`redis_zpopmin`, `redis_bzpopmin`, `redis_xreadgroup`).
- Replicas expire logically with the primary; wildly skewed clocks between
  a primary and its host can make absolute deadlines misleading — prefer
  server-relative TTLs unless clocks are managed.
